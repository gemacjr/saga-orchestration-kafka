package com.example.saga.common.it;

import com.example.saga.common.inbox.InboxGuard;
import com.example.saga.common.outbox.OutboxRelay;
import com.example.saga.common.outbox.OutboxWriter;
import com.example.saga.messages.SagaHeaders;
import com.example.saga.messages.SagaTopics;
import com.example.saga.messages.events.OrderCreatedEvent;
import com.example.saga.messages.events.PaymentCompletedEvent;
import com.example.saga.testsupport.KafkaTestClient;
import com.example.saga.testsupport.SagaIntegrationTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.example.saga.testsupport.KafkaTestClient.header;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@SagaIntegrationTest
class SagaCommonIntegrationTest {

    @Autowired OutboxWriter outbox;
    @Autowired OutboxRelay relay;
    @Autowired InboxGuard inbox;
    @Autowired JdbcTemplate jdbc;
    @Autowired TransactionTemplate tx;
    @Autowired KafkaTestClient kafka;
    @Autowired TestSagaApplication.ProbeListener probe;

    private static OrderCreatedEvent orderCreated(String productId) {
        return new OrderCreatedEvent(UUID.randomUUID(), UUID.randomUUID(), productId, 1, BigDecimal.TEN);
    }

    // --- outbox ---------------------------------------------------------------------------------

    @Test
    void outboxRequiresAnExistingTransaction() {
        var event = new PaymentCompletedEvent(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        assertThatThrownBy(() -> outbox.send(SagaTopics.PAYMENT_COMPLETED, event))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    void committedMessageIsRelayedWithIdAndLogicalTypeHeaders() {
        var event = new PaymentCompletedEvent(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        tx.executeWithoutResult(s -> outbox.send(SagaTopics.PAYMENT_COMPLETED, event));

        ConsumerRecord<String, String> record =
                kafka.awaitRecords(SagaTopics.PAYMENT_COMPLETED, event.sagaId().toString(), 1, Duration.ofSeconds(20)).get(0);

        assertThat(kafka.read(record, PaymentCompletedEvent.class)).isEqualTo(event);
        assertThat(header(record, SagaHeaders.MESSAGE_TYPE)).isEqualTo("PaymentCompleted.v1");
        assertThat(header(record, SagaHeaders.MESSAGE_ID)).isNotBlank();
        await().untilAsserted(() -> assertThat(jdbc.queryForObject(
                "select published_at is not null from outbox_message where id = ?::uuid",
                Boolean.class, header(record, SagaHeaders.MESSAGE_ID))).isTrue());
    }

    @Test
    void rolledBackMessageIsNeverPublished() {
        var event = new PaymentCompletedEvent(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        tx.executeWithoutResult(s -> {
            outbox.send(SagaTopics.PAYMENT_COMPLETED, event);
            s.setRollbackOnly();
        });

        assertThat(kafka.recordsFor(SagaTopics.PAYMENT_COMPLETED, event.sagaId(), Duration.ofSeconds(2))).isEmpty();
        assertThat(jdbc.queryForObject("select count(*) from outbox_message where aggregate_id = ?",
                Integer.class, event.sagaId())).isZero();
    }

    @Test
    void messagesOfOneSagaArePublishedInOrder() {
        UUID sagaId = UUID.randomUUID();
        List<UUID> paymentIds = List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        tx.executeWithoutResult(s -> paymentIds.forEach(p ->
                outbox.send(SagaTopics.PAYMENT_COMPLETED, new PaymentCompletedEvent(sagaId, UUID.randomUUID(), p))));

        var records = kafka.awaitRecords(SagaTopics.PAYMENT_COMPLETED, sagaId.toString(), 3, Duration.ofSeconds(20));
        assertThat(records).extracting(r -> kafka.read(r, PaymentCompletedEvent.class).paymentId())
                .containsExactlyElementsOf(paymentIds);
    }

    @Test
    void purgeRemovesOnlyPublishedMessagesPastRetention() {
        UUID oldId = UUID.randomUUID();
        UUID recentId = UUID.randomUUID();
        UUID pendingId = UUID.randomUUID();
        insertOutboxRow(oldId, "now() - interval '30 days'");
        insertOutboxRow(recentId, "now()");
        insertOutboxRow(pendingId, null);
        // keep the relay from publishing the pending row during this test
        jdbc.update("update outbox_message set topic = 'not-a-topic' where id = ?", pendingId);

        relay.purgePublished();

        assertThat(jdbc.queryForList("select id from outbox_message where id in (?, ?, ?)", UUID.class,
                oldId, recentId, pendingId)).containsExactlyInAnyOrder(recentId, pendingId);
        jdbc.update("delete from outbox_message where id = ?", pendingId);
    }

    private void insertOutboxRow(UUID id, String publishedAtSql) {
        jdbc.update("insert into outbox_message (id, aggregate_id, topic, message_key, message_type, payload, published_at) "
                + "values (?, ?, 'not-a-topic', 'k', 'PaymentCompleted.v1', '{}'::jsonb, " + publishedAtSql + ")", id, UUID.randomUUID());
    }

    // --- inbox ----------------------------------------------------------------------------------

    @Test
    void inboxAcceptsAMessageIdOnlyOnce() {
        String messageId = UUID.randomUUID().toString();
        assertThat(firstDelivery(messageId)).isTrue();
        assertThat(firstDelivery(messageId)).isFalse();
    }

    @Test
    void inboxMarkerRollsBackWithTheHandler() {
        String messageId = UUID.randomUUID().toString();
        tx.executeWithoutResult(s -> {
            inbox.firstDelivery(messageId);
            s.setRollbackOnly();
        });
        assertThat(firstDelivery(messageId)).isTrue();
    }

    private boolean firstDelivery(String messageId) {
        return Boolean.TRUE.equals(tx.execute(s -> inbox.firstDelivery(messageId)));
    }

    @Test
    void inboxRequiresAnExistingTransaction() {
        assertThatThrownBy(() -> inbox.firstDelivery("x")).isInstanceOf(IllegalTransactionStateException.class);
    }

    // --- consumer error handling ----------------------------------------------------------------

    @Test
    void transientFailureIsRetriedUntilItSucceeds() {
        String messageId = kafka.send(SagaTopics.ORDER_CREATED, orderCreated("flaky-1"));

        await().untilAsserted(() -> assertThat(probe.handled).containsEntry("flaky-1", messageId));
        assertThat(probe.attempts.get("flaky-1")).isEqualTo(2);
    }

    @Test
    void persistentFailureIsDeadLetteredAfterRetries() {
        OrderCreatedEvent event = orderCreated("always-failing");
        kafka.send(SagaTopics.ORDER_CREATED, event);

        ConsumerRecord<String, String> dead = kafka.awaitRecords(SagaTopics.ORDER_CREATED + "-dlt",
                event.sagaId().toString(), 1, Duration.ofSeconds(20)).get(0);

        assertThat(kafka.read(dead, OrderCreatedEvent.class)).isEqualTo(event);
        assertThat(header(dead, "kafka_dlt-exception-cause-fqcn")).isEqualTo(IllegalStateException.class.getName());
        assertThat(probe.attempts.get("always-failing")).isEqualTo(3); // 1 + max-retries(2)
    }

    @Test
    void nonRetryableFailureGoesStraightToTheDlt() {
        OrderCreatedEvent event = orderCreated("non-retryable");
        kafka.send(SagaTopics.ORDER_CREATED, event);

        kafka.awaitRecords(SagaTopics.ORDER_CREATED + "-dlt", event.sagaId().toString(), 1, Duration.ofSeconds(20));
        assertThat(probe.attempts.get("non-retryable")).isEqualTo(1);
    }

    @Test
    void poisonPillIsDeadLetteredWithItsOriginalBytes() {
        String key = UUID.randomUUID().toString();
        kafka.sendRaw(SagaTopics.ORDER_CREATED, key, "{not json",
                Map.of(SagaHeaders.MESSAGE_TYPE, "OrderCreated.v1", SagaHeaders.MESSAGE_ID, key));

        ConsumerRecord<String, String> dead =
                kafka.awaitRecords(SagaTopics.ORDER_CREATED + "-dlt", key, 1, Duration.ofSeconds(20)).get(0);

        assertThat(dead.value()).isEqualTo("{not json");
        assertThat(header(dead, "kafka_dlt-exception-fqcn")).contains("DeserializationException");
    }

    @Test
    void unknownMessageTypeIsDeadLettered() {
        String key = UUID.randomUUID().toString();
        kafka.sendRaw(SagaTopics.ORDER_CREATED, key, "{}",
                Map.of(SagaHeaders.MESSAGE_TYPE, "com.attacker.Gadget", SagaHeaders.MESSAGE_ID, key));

        kafka.awaitRecords(SagaTopics.ORDER_CREATED + "-dlt", key, 1, Duration.ofSeconds(20));
    }
}
