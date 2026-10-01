package com.example.saga.order;

import com.example.saga.messages.SagaTopics;
import com.example.saga.messages.commands.CancelOrderCommand;
import com.example.saga.messages.commands.CompleteOrderCommand;
import com.example.saga.messages.events.OrderCancelledEvent;
import com.example.saga.messages.events.OrderCompletedEvent;
import com.example.saga.messages.events.OrderCreatedEvent;
import com.example.saga.order.domain.OrderRepository;
import com.example.saga.order.domain.OrderStatus;
import com.example.saga.testsupport.KafkaTestClient;
import com.example.saga.testsupport.SagaIntegrationTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SagaIntegrationTest
@AutoConfigureMockMvc
class OrderServiceIntegrationTest {

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper json;
    @Autowired KafkaTestClient kafka;
    @Autowired OrderRepository orders;

    private JsonNode placeOrder(String body, String idempotencyKey) throws Exception {
        MockHttpServletRequestBuilder request = post("/orders").contentType(MediaType.APPLICATION_JSON).content(body);
        if (idempotencyKey != null) {
            request.header("Idempotency-Key", idempotencyKey);
        }
        String response = mvc.perform(request)
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", org.hamcrest.Matchers.containsString("/orders/")))
                .andReturn().getResponse().getContentAsString();
        return json.readTree(response);
    }

    private JsonNode placeOrder() throws Exception {
        return placeOrder("{\"productId\":\"product-100\",\"quantity\":2,\"amount\":250.00}", null);
    }

    // --- REST API -------------------------------------------------------------------------------

    @Test
    void placingAnOrderStoresItPendingAndStartsTheSaga() throws Exception {
        JsonNode order = placeOrder();
        UUID sagaId = UUID.fromString(order.get("sagaId").asText());
        UUID orderId = UUID.fromString(order.get("orderId").asText());

        assertThat(order.get("status").asText()).isEqualTo("PENDING");
        OrderCreatedEvent event = kafka.expectOne(SagaTopics.ORDER_CREATED, sagaId, OrderCreatedEvent.class);
        assertThat(event.orderId()).isEqualTo(orderId);
        assertThat(event.productId()).isEqualTo("product-100");
        assertThat(event.quantity()).isEqualTo(2);
        assertThat(event.amount()).isEqualByComparingTo("250.00");
    }

    @Test
    void retriedPostWithSameIdempotencyKeyReturnsTheSameOrderAndStartsOneSaga() throws Exception {
        String key = UUID.randomUUID().toString();
        String body = "{\"productId\":\"product-100\",\"quantity\":1,\"amount\":10}";
        JsonNode first = placeOrder(body, key);
        JsonNode second = placeOrder(body, key);

        assertThat(second.get("orderId")).isEqualTo(first.get("orderId"));
        UUID sagaId = UUID.fromString(first.get("sagaId").asText());
        assertThat(kafka.recordsFor(SagaTopics.ORDER_CREATED, sagaId, Duration.ofSeconds(3))).hasSize(1);
    }

    @Test
    void invalidRequestsAreRejected() throws Exception {
        for (String body : new String[]{
                "{\"productId\":\"\",\"quantity\":1,\"amount\":10}",
                "{\"productId\":\"p\",\"quantity\":0,\"amount\":10}",
                "{\"productId\":\"p\",\"quantity\":1,\"amount\":-5}",
                "{\"productId\":\"p\",\"quantity\":1,\"amount\":1.234}",
                "{\"productId\":\"p\",\"quantity\":1}"}) {
            mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                    .andExpect(status().isBadRequest());
        }
    }

    @Test
    void ordersCanBeReadBackIndividuallyAndListed() throws Exception {
        JsonNode order = placeOrder();
        String orderId = order.get("orderId").asText();

        mvc.perform(get("/orders/{id}", orderId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value(orderId))
                .andExpect(jsonPath("$.status").value("PENDING"));
        mvc.perform(get("/orders/{id}", UUID.randomUUID())).andExpect(status().isNotFound());
        mvc.perform(get("/orders").param("limit", "1000"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.orderId == '" + orderId + "')]").exists());
    }

    // --- commands from the orchestrator ---------------------------------------------------------

    @Test
    void completeOrderCommandCompletesTheOrderAndReplies() throws Exception {
        JsonNode order = placeOrder();
        UUID sagaId = UUID.fromString(order.get("sagaId").asText());
        UUID orderId = UUID.fromString(order.get("orderId").asText());

        kafka.send(SagaTopics.COMPLETE_ORDER, new CompleteOrderCommand(sagaId, orderId));

        assertThat(kafka.expectOne(SagaTopics.ORDER_COMPLETED, sagaId, OrderCompletedEvent.class).orderId()).isEqualTo(orderId);
        assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.COMPLETED);
    }

    @Test
    void cancelOrderCommandCancelsWithReason() throws Exception {
        JsonNode order = placeOrder();
        UUID sagaId = UUID.fromString(order.get("sagaId").asText());
        UUID orderId = UUID.fromString(order.get("orderId").asText());

        kafka.send(SagaTopics.CANCEL_ORDER, new CancelOrderCommand(sagaId, orderId, "payment declined"));

        assertThat(kafka.expectOne(SagaTopics.ORDER_CANCELLED, sagaId, OrderCancelledEvent.class).reason())
                .isEqualTo("payment declined");
        var cancelled = orders.findById(orderId).orElseThrow();
        assertThat(cancelled.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(cancelled.getFailureReason()).isEqualTo("payment declined");
    }

    @Test
    void redeliveredCommandIsProcessedOnceButAResentCommandIsAnsweredAgain() throws Exception {
        JsonNode order = placeOrder();
        UUID sagaId = UUID.fromString(order.get("sagaId").asText());
        UUID orderId = UUID.fromString(order.get("orderId").asText());
        var command = new CompleteOrderCommand(sagaId, orderId);

        String messageId = kafka.send(SagaTopics.COMPLETE_ORDER, command);
        kafka.send(SagaTopics.COMPLETE_ORDER, command, messageId);       // broker redelivery: ignored by inbox
        assertThat(kafka.recordsFor(SagaTopics.ORDER_COMPLETED, sagaId, Duration.ofSeconds(3))).hasSize(1);

        kafka.send(SagaTopics.COMPLETE_ORDER, command);                  // orchestrator re-send: idempotent reply
        await().untilAsserted(() ->
                assertThat(kafka.recordsFor(SagaTopics.ORDER_COMPLETED, sagaId, Duration.ofMillis(500))).hasSize(2));
    }

    @Test
    void contradictoryCommandIsDeadLetteredWithoutRetries() throws Exception {
        JsonNode order = placeOrder();
        UUID sagaId = UUID.fromString(order.get("sagaId").asText());
        UUID orderId = UUID.fromString(order.get("orderId").asText());
        kafka.send(SagaTopics.CANCEL_ORDER, new CancelOrderCommand(sagaId, orderId, "declined"));
        kafka.expectOne(SagaTopics.ORDER_CANCELLED, sagaId, OrderCancelledEvent.class);

        kafka.send(SagaTopics.COMPLETE_ORDER, new CompleteOrderCommand(sagaId, orderId));

        kafka.expectOne(SagaTopics.COMPLETE_ORDER + "-dlt", sagaId, CompleteOrderCommand.class);
        assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void commandForUnknownOrderIsDeadLettered() {
        UUID sagaId = UUID.randomUUID();
        kafka.send(SagaTopics.CANCEL_ORDER, new CancelOrderCommand(sagaId, UUID.randomUUID(), "x"));
        kafka.expectOne(SagaTopics.CANCEL_ORDER + "-dlt", sagaId, CancelOrderCommand.class);
    }
}
