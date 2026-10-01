package com.example.saga.common.inbox;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Idempotent-consumer guard. The insert runs in the listener's transaction: if the handler fails,
 * the marker rolls back too and the redelivered message is processed again.
 */
public class InboxGuard {

    private final JdbcTemplate jdbcTemplate;
    private final String consumer;

    public InboxGuard(JdbcTemplate jdbcTemplate, String consumer) {
        this.jdbcTemplate = jdbcTemplate;
        this.consumer = consumer;
    }

    /** @return true the first time this message id is seen, false for a duplicate delivery. */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean firstDelivery(String messageId) {
        return jdbcTemplate.update(
                "insert into processed_message (consumer, message_id) values (?, ?) on conflict do nothing",
                consumer, messageId) == 1;
    }
}
