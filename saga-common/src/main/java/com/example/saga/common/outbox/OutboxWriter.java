package com.example.saga.common.outbox;

import com.example.saga.messages.MessageTypes;
import com.example.saga.messages.SagaMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Records an outgoing message in the caller's database transaction. The message reaches Kafka
 * only if the business change commits; {@link OutboxRelay} then publishes it (at-least-once).
 */
public class OutboxWriter {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public OutboxWriter(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    /** MANDATORY: writing to the outbox outside a business transaction defeats its purpose. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void send(String topic, SagaMessage message) {
        jdbcTemplate.update("""
                        insert into outbox_message (id, aggregate_id, topic, message_key, message_type, payload)
                        values (?, ?, ?, ?, ?, ?::jsonb)
                        """,
                UUID.randomUUID(),
                message.sagaId(),
                topic,
                message.sagaId().toString(),
                MessageTypes.nameOf(message.getClass()),
                toJson(message));
    }

    private String toJson(SagaMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize " + message, e);
        }
    }
}
