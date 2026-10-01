package com.example.saga.common.outbox;

import com.example.saga.common.SagaProperties;
import com.example.saga.messages.SagaHeaders;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Polling publisher for the transactional outbox.
 * <p>
 * {@code FOR UPDATE SKIP LOCKED} lets several instances of a service relay concurrently without
 * publishing the same row twice. If the Kafka send fails, the transaction rolls back and the batch
 * is retried on the next tick, so delivery is at-least-once and consumers must be idempotent.
 */
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TransactionTemplate transactionTemplate;
    private final SagaProperties.Outbox properties;

    public OutboxRelay(JdbcTemplate jdbcTemplate, KafkaTemplate<String, String> kafkaTemplate,
                       TransactionTemplate transactionTemplate, SagaProperties.Outbox properties) {
        this.jdbcTemplate = jdbcTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.transactionTemplate = transactionTemplate;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${saga.outbox.poll-interval-ms:250}")
    public void relay() {
        try {
            Integer published;
            do {
                published = transactionTemplate.execute(status -> publishBatch());
            } while (published != null && published == properties.batchSize());
        } catch (RuntimeException e) {
            log.warn("Outbox relay failed, will retry on next tick: {}", e.getMessage());
        }
    }

    @Scheduled(cron = "${saga.outbox.purge-cron:0 0 * * * *}")
    public void purgePublished() {
        Instant cutoff = Instant.now().minus(properties.retention());
        int deleted = jdbcTemplate.update(
                "delete from outbox_message where published_at < ?", Timestamp.from(cutoff));
        if (deleted > 0) {
            log.info("Purged {} published outbox messages older than {}", deleted, cutoff);
        }
    }

    private int publishBatch() {
        List<OutboxRow> rows = jdbcTemplate.query("""
                        select id, topic, message_key, message_type, payload::text as payload
                        from outbox_message
                        where published_at is null
                        order by seq
                        limit ?
                        for update skip locked
                        """,
                (rs, i) -> new OutboxRow(
                        rs.getObject("id", UUID.class),
                        rs.getString("topic"),
                        rs.getString("message_key"),
                        rs.getString("message_type"),
                        rs.getString("payload")),
                properties.batchSize());
        if (rows.isEmpty()) {
            return 0;
        }

        // Send the whole batch, then wait: the idempotent producer keeps per-partition order.
        List<CompletableFuture<SendResult<String, String>>> sends = rows.stream()
                .map(row -> kafkaTemplate.send(toRecord(row)))
                .toList();
        CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new))
                .orTimeout(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .join();

        jdbcTemplate.batchUpdate("update outbox_message set published_at = now() where id = ?",
                rows.stream().map(row -> new Object[]{row.id()}).toList());
        log.debug("Relayed {} outbox messages", rows.size());
        return rows.size();
    }

    private static ProducerRecord<String, String> toRecord(OutboxRow row) {
        ProducerRecord<String, String> record = new ProducerRecord<>(row.topic(), row.key(), row.payload());
        record.headers()
                .add(SagaHeaders.MESSAGE_ID, row.id().toString().getBytes(StandardCharsets.UTF_8))
                .add(SagaHeaders.MESSAGE_TYPE, row.type().getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private record OutboxRow(UUID id, String topic, String key, String type, String payload) {
    }
}
