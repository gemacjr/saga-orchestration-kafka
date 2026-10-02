package com.example.saga.orchestrator.domain;

import com.example.saga.common.outbox.OutboxWriter;
import com.example.saga.messages.SagaTopics;
import com.example.saga.messages.commands.CompleteOrderCommand;
import com.example.saga.messages.commands.ProcessPaymentCommand;
import com.example.saga.messages.commands.RefundPaymentCommand;
import com.example.saga.orchestrator.OrchestratorProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class SagaTimeoutPolicyTest {

    private final OutboxWriter outbox = mock(OutboxWriter.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final SagaOrchestrator orchestrator = new SagaOrchestrator(
            mock(SagaRepository.class), outbox, new OrchestratorProperties(Duration.ofSeconds(30), 3), meters);

    private final SagaInstance saga = SagaInstance.start(
            UUID.randomUUID(), UUID.randomUUID(), "product-100", 2, new BigDecimal("250.00"));

    private void timeOut(int times) {
        for (int i = 0; i < times; i++) {
            saga.recordStepTimeout();
            orchestrator.onStepTimeout(saga);
        }
    }

    @Test
    void resendsCurrentCommandWithinRetryBudget() {
        timeOut(3);
        verify(outbox, times(3)).send(eq(SagaTopics.PROCESS_PAYMENT), any(ProcessPaymentCommand.class));
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.PAYMENT_PROCESSING);
    }

    @Test
    void compensatesOnceRetriesAreExhausted() {
        timeOut(4);
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
        assertThat(saga.getFailureReason()).isEqualTo("Timed out in PAYMENT_PROCESSING");
        verify(outbox).send(eq(SagaTopics.REFUND_PAYMENT), any(RefundPaymentCommand.class));
    }

    @Test
    void keepsResendingAndFlagsWhenCompensationIsNoLongerPossible() {
        saga.paymentCompleted(UUID.randomUUID());
        saga.inventoryReserved(UUID.randomUUID());
        timeOut(4);
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.ORDER_COMPLETING);
        verify(outbox, times(4)).send(eq(SagaTopics.COMPLETE_ORDER), any(CompleteOrderCommand.class));
        assertThat(meters.counter("saga.stuck", "status", "ORDER_COMPLETING").count()).isEqualTo(1.0);
    }
}
