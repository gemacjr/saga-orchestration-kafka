package com.example.saga.common.outbox;

import com.example.saga.common.SagaProperties;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Failure paths of the relay that are awkward to provoke against a real broker. */
class OutboxRelayTest {

    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final PlatformTransactionManager txManager = mock(PlatformTransactionManager.class);

    private OutboxRelay relay(int batchSize, Duration sendTimeout) {
        return new OutboxRelay(jdbc, kafka, new TransactionTemplate(txManager),
                new SagaProperties.Outbox(batchSize, sendTimeout, Duration.ofDays(7)));
    }

    @SuppressWarnings("unchecked")
    private void outboxReturns(int rows) throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getObject("id", UUID.class)).thenAnswer(i -> UUID.randomUUID());
        when(rs.getString(anyString())).thenReturn("x");
        when(jdbc.query(anyString(), any(RowMapper.class), anyInt())).thenAnswer(inv -> {
            RowMapper<Object> mapper = inv.getArgument(1);
            return java.util.stream.IntStream.range(0, rows).mapToObj(i -> {
                try {
                    return mapper.mapRow(rs, i);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }).toList();
        }).thenReturn(List.of());
    }

    @Test
    void emptyOutboxSendsNothing() throws Exception {
        outboxReturns(0);
        relay(10, Duration.ofSeconds(1)).relay();
        verifyNoInteractions(kafka);
        verify(jdbc, never()).batchUpdate(anyString(), anyList());
    }

    @Test
    void fullBatchTriggersAnotherPollInTheSameTick() throws Exception {
        outboxReturns(2);
        when(kafka.send(any(org.apache.kafka.clients.producer.ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        relay(2, Duration.ofSeconds(1)).relay();

        verify(jdbc, times(2)).query(anyString(), any(RowMapper.class), anyInt());
        verify(jdbc, times(1)).batchUpdate(anyString(), anyList());
    }

    @Test
    void failedSendLeavesRowsUnpublishedAndRollsBack() throws Exception {
        outboxReturns(1);
        when(kafka.send(any(org.apache.kafka.clients.producer.ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        relay(10, Duration.ofSeconds(1)).relay(); // must not throw: the scheduler keeps running

        verify(jdbc, never()).batchUpdate(anyString(), anyList());
        verify(txManager).rollback(any());
    }

    @Test
    void sendTimeoutIsTreatedAsFailure() throws Exception {
        outboxReturns(1);
        when(kafka.send(any(org.apache.kafka.clients.producer.ProducerRecord.class)))
                .thenReturn(new CompletableFuture<>()); // never completes

        relay(10, Duration.ofMillis(50)).relay();

        verify(jdbc, never()).batchUpdate(anyString(), anyList());
    }

    @Test
    void purgeOnlyLogsWhenRowsWereDeleted() {
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(0, 3);
        OutboxRelay relay = relay(10, Duration.ofSeconds(1));
        relay.purgePublished();
        relay.purgePublished();
        verify(jdbc, times(2)).update(startsWith("delete from outbox_message"), any(Object[].class));
    }
}
