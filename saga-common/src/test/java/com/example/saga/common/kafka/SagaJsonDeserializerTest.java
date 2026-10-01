package com.example.saga.common.kafka;

import com.example.saga.messages.SagaHeaders;
import com.example.saga.messages.events.OrderCreatedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SagaJsonDeserializerTest {

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void resolvesPayloadTypeFromLogicalTypeHeader() throws Exception {
        var event = new OrderCreatedEvent(UUID.randomUUID(), UUID.randomUUID(), "product-100", 2, new BigDecimal("250.00"));
        var headers = new RecordHeaders().add(SagaHeaders.MESSAGE_TYPE, "OrderCreated.v1".getBytes(StandardCharsets.UTF_8));

        Object value = SagaKafkaConfiguration.sagaJsonDeserializer(objectMapper)
                .deserialize("saga.order.created", headers, objectMapper.writeValueAsBytes(event));

        assertThat(value).isEqualTo(event);
    }

    @Test
    void rejectsClassNamesThatAreNotRegisteredTypes() throws Exception {
        var headers = new RecordHeaders().add(SagaHeaders.MESSAGE_TYPE, "org.springframework.context.support.ClassPathXmlApplicationContext".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> SagaKafkaConfiguration.sagaJsonDeserializer(objectMapper)
                .deserialize("saga.order.created", headers, "{}".getBytes(StandardCharsets.UTF_8)))
                .hasMessageContaining("not in the trusted packages");
    }
}
