package com.example.saga.orchestrator;

import com.example.saga.messages.SagaTopics;
import com.example.saga.messages.commands.*;
import com.example.saga.messages.events.*;
import com.example.saga.orchestrator.domain.SagaInstance;
import com.example.saga.orchestrator.domain.SagaOrchestrator;
import com.example.saga.orchestrator.domain.SagaRepository;
import com.example.saga.orchestrator.domain.SagaStatus;
import com.example.saga.orchestrator.domain.SagaTimeoutWatcher;
import com.example.saga.testsupport.KafkaTestClient;
import com.example.saga.testsupport.SagaIntegrationTest;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** The test plays order, payment and inventory services and checks the orchestrator's commands and state. */
@SagaIntegrationTest
@AutoConfigureMockMvc
class SagaOrchestratorIntegrationTest {

    @Autowired KafkaTestClient kafka;
    @Autowired SagaRepository sagas;
    @Autowired SagaOrchestrator orchestrator;
    @Autowired SagaTimeoutWatcher watcher;
    @Autowired JdbcTemplate jdbc;
    @Autowired MeterRegistry meters;
    @Autowired MockMvc mvc;

    private final UUID sagaId = UUID.randomUUID();
    private final UUID orderId = UUID.randomUUID();
    private final UUID paymentId = UUID.randomUUID();

    private void startSaga(int quantity, String amount) {
        kafka.send(SagaTopics.ORDER_CREATED, new OrderCreatedEvent(sagaId, orderId, "product-100", quantity, new BigDecimal(amount)));
    }

    private SagaStatus awaitStatus(SagaStatus expected) {
        return await().until(() -> sagas.findById(sagaId).map(SagaInstance::getStatus).orElse(null), expected::equals);
    }

    private double finished(String outcome) {
        var counter = meters.find("saga.finished").tag("outcome", outcome).counter();
        return counter == null ? 0 : counter.count();
    }

    @Test
    void happyPath() {
        double completedBefore = finished("COMPLETED");
        startSaga(2, "250.00");

        ProcessPaymentCommand pay = kafka.expectOne(SagaTopics.PROCESS_PAYMENT, sagaId, ProcessPaymentCommand.class);
        assertThat(pay).isEqualTo(new ProcessPaymentCommand(sagaId, orderId, new BigDecimal("250.00")));

        kafka.send(SagaTopics.PAYMENT_COMPLETED, new PaymentCompletedEvent(sagaId, orderId, paymentId));
        assertThat(kafka.expectOne(SagaTopics.RESERVE_INVENTORY, sagaId, ReserveInventoryCommand.class))
                .isEqualTo(new ReserveInventoryCommand(sagaId, orderId, "product-100", 2));

        UUID reservationId = UUID.randomUUID();
        kafka.send(SagaTopics.INVENTORY_RESERVED, new InventoryReservedEvent(sagaId, orderId, reservationId));
        assertThat(kafka.expectOne(SagaTopics.COMPLETE_ORDER, sagaId, CompleteOrderCommand.class))
                .isEqualTo(new CompleteOrderCommand(sagaId, orderId));

        kafka.send(SagaTopics.ORDER_COMPLETED, new OrderCompletedEvent(sagaId, orderId));
        awaitStatus(SagaStatus.COMPLETED);

        SagaInstance saga = sagas.findById(sagaId).orElseThrow();
        assertThat(saga.getPaymentId()).isEqualTo(paymentId);
        assertThat(saga.getReservationId()).isEqualTo(reservationId);
        assertThat(finished("COMPLETED")).isEqualTo(completedBefore + 1);
    }

    @Test
    void paymentFailureCancelsTheOrderWithoutCompensation() {
        startSaga(2, "1500");
        kafka.expectOne(SagaTopics.PROCESS_PAYMENT, sagaId, ProcessPaymentCommand.class);

        kafka.send(SagaTopics.PAYMENT_FAILED, new PaymentFailedEvent(sagaId, orderId, "limit exceeded"));
        assertThat(kafka.expectOne(SagaTopics.CANCEL_ORDER, sagaId, CancelOrderCommand.class).reason())
                .isEqualTo("limit exceeded");
        assertThat(kafka.recordsFor(SagaTopics.REFUND_PAYMENT, sagaId, Duration.ZERO)).isEmpty();

        kafka.send(SagaTopics.ORDER_CANCELLED, new OrderCancelledEvent(sagaId, orderId, "limit exceeded"));
        awaitStatus(SagaStatus.FAILED);
    }

    @Test
    void inventoryFailureRefundsThePaymentThenCancels() {
        startSaga(10, "300");
        kafka.expectOne(SagaTopics.PROCESS_PAYMENT, sagaId, ProcessPaymentCommand.class);
        kafka.send(SagaTopics.PAYMENT_COMPLETED, new PaymentCompletedEvent(sagaId, orderId, paymentId));
        kafka.expectOne(SagaTopics.RESERVE_INVENTORY, sagaId, ReserveInventoryCommand.class);

        kafka.send(SagaTopics.INVENTORY_FAILED, new InventoryFailedEvent(sagaId, orderId, "out of stock"));
        RefundPaymentCommand refund = kafka.expectOne(SagaTopics.REFUND_PAYMENT, sagaId, RefundPaymentCommand.class);
        assertThat(refund.paymentId()).isEqualTo(paymentId);
        assertThat(refund.reason()).contains("out of stock");
        assertThat(kafka.recordsFor(SagaTopics.CANCEL_ORDER, sagaId, Duration.ZERO)).isEmpty(); // refund first

        kafka.send(SagaTopics.PAYMENT_REFUNDED, new PaymentRefundedEvent(sagaId, orderId, paymentId));
        kafka.expectOne(SagaTopics.CANCEL_ORDER, sagaId, CancelOrderCommand.class);

        kafka.send(SagaTopics.ORDER_CANCELLED, new OrderCancelledEvent(sagaId, orderId, "out of stock"));
        awaitStatus(SagaStatus.COMPENSATED);
    }

    @Test
    void duplicateOrderCreatedStartsOneSaga() {
        startSaga(1, "10");
        startSaga(1, "10");

        assertThat(kafka.recordsFor(SagaTopics.PROCESS_PAYMENT, sagaId, Duration.ofSeconds(3))).hasSize(1);
    }

    @Test
    void lateAndDuplicateEventsDoNotMoveTheSaga() {
        startSaga(1, "10");
        kafka.expectOne(SagaTopics.PROCESS_PAYMENT, sagaId, ProcessPaymentCommand.class);
        kafka.send(SagaTopics.PAYMENT_COMPLETED, new PaymentCompletedEvent(sagaId, orderId, paymentId));
        awaitStatus(SagaStatus.INVENTORY_RESERVING);

        kafka.send(SagaTopics.PAYMENT_COMPLETED, new PaymentCompletedEvent(sagaId, orderId, UUID.randomUUID()));
        kafka.send(SagaTopics.PAYMENT_FAILED, new PaymentFailedEvent(sagaId, orderId, "late"));
        kafka.send(SagaTopics.ORDER_COMPLETED, new OrderCompletedEvent(sagaId, orderId));

        assertThat(kafka.recordsFor(SagaTopics.RESERVE_INVENTORY, sagaId, Duration.ofSeconds(3))).hasSize(1);
        SagaInstance saga = sagas.findById(sagaId).orElseThrow();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.INVENTORY_RESERVING);
        assertThat(saga.getPaymentId()).isEqualTo(paymentId);
    }

    @Test
    void eventForUnknownSagaIsDeadLettered() {
        kafka.send(SagaTopics.PAYMENT_COMPLETED, new PaymentCompletedEvent(sagaId, orderId, paymentId));
        kafka.expectOne(SagaTopics.PAYMENT_COMPLETED + "-dlt", sagaId, PaymentCompletedEvent.class);
    }

    @Test
    void watcherResendsAStuckStepAndThenCompensates() {
        startSaga(1, "10");
        kafka.expectOne(SagaTopics.PROCESS_PAYMENT, sagaId, ProcessPaymentCommand.class);

        for (int i = 0; i < 4; i++) {
            jdbc.update("update saga_instance set updated_at = now() - interval '2 hours' where saga_id = ?", sagaId);
            watcher.scan();
        }

        assertThat(kafka.recordsFor(SagaTopics.PROCESS_PAYMENT, sagaId, Duration.ofSeconds(1))).hasSize(4); // 1 + 3 retries
        assertThat(kafka.expectOne(SagaTopics.REFUND_PAYMENT, sagaId, RefundPaymentCommand.class).reason())
                .contains("Timed out in PAYMENT_PROCESSING");
        assertThat(sagas.findById(sagaId).orElseThrow().getStatus()).isEqualTo(SagaStatus.COMPENSATING);
    }

    @Test
    void timeoutOfAFinishedSagaIsANoOp() {
        startSaga(1, "1500");
        kafka.expectOne(SagaTopics.PROCESS_PAYMENT, sagaId, ProcessPaymentCommand.class);
        kafka.send(SagaTopics.PAYMENT_FAILED, new PaymentFailedEvent(sagaId, orderId, "declined"));
        kafka.expectOne(SagaTopics.CANCEL_ORDER, sagaId, CancelOrderCommand.class);
        kafka.send(SagaTopics.ORDER_CANCELLED, new OrderCancelledEvent(sagaId, orderId, "declined"));
        awaitStatus(SagaStatus.FAILED);

        orchestrator.handleTimeout(sagaId);

        assertThat(sagas.findById(sagaId).orElseThrow().getStepTimeouts()).isZero();
        assertThat(kafka.recordsFor(SagaTopics.CANCEL_ORDER, sagaId, Duration.ofSeconds(1))).hasSize(1);
    }

    @Test
    void sagasAreExposedOverRest() throws Exception {
        startSaga(3, "99.99");
        awaitStatus(SagaStatus.PAYMENT_PROCESSING);

        mvc.perform(get("/sagas/{id}", sagaId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value(orderId.toString()))
                .andExpect(jsonPath("$.status").value("PAYMENT_PROCESSING"))
                .andExpect(jsonPath("$.quantity").value(3));
        mvc.perform(get("/sagas/{id}", UUID.randomUUID())).andExpect(status().isNotFound());
        mvc.perform(get("/sagas").param("limit", "1000"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.sagaId == '" + sagaId + "')]").exists());
    }
}
