package com.example.saga.orchestrator.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class SagaInstanceTest {

    private final SagaInstance saga = SagaInstance.start(
            UUID.randomUUID(), UUID.randomUUID(), "product-100", 2, new BigDecimal("250.00"));

    @Test
    void happyPathCompletes() {
        assertThat(saga.paymentCompleted(UUID.randomUUID())).isTrue();
        assertThat(saga.inventoryReserved(UUID.randomUUID())).isTrue();
        assertThat(saga.orderCompleted()).isTrue();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.COMPLETED);
    }

    @Test
    void paymentFailureCancelsOrderAndEndsFailed() {
        assertThat(saga.paymentFailed("declined")).isTrue();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.ORDER_CANCELLING);
        assertThat(saga.orderCancelled()).isTrue();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED);
    }

    @Test
    void inventoryFailureRefundsThenCancelsAndEndsCompensated() {
        saga.paymentCompleted(UUID.randomUUID());
        assertThat(saga.inventoryFailed("out of stock")).isTrue();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
        assertThat(saga.paymentRefunded()).isTrue();
        assertThat(saga.orderCancelled()).isTrue();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
        assertThat(saga.getFailureReason()).isEqualTo("out of stock");
    }

    @Test
    void duplicateAndLateEventsAreIgnored() {
        UUID paymentId = UUID.randomUUID();
        saga.paymentCompleted(paymentId);
        assertThat(saga.paymentCompleted(UUID.randomUUID())).isFalse();
        assertThat(saga.paymentFailed("late")).isFalse();
        assertThat(saga.orderCompleted()).isFalse();
        assertThat(saga.getPaymentId()).isEqualTo(paymentId);
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.INVENTORY_RESERVING);
    }

    @Test
    void compensationIsNotAllowedOnceOrderCompletionWasRequested() {
        saga.paymentCompleted(UUID.randomUUID());
        saga.inventoryReserved(UUID.randomUUID());
        assertThat(saga.startCompensation("timeout")).isFalse();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.ORDER_COMPLETING);
    }

    @Test
    void transitionResetsStepTimeoutCounter() {
        saga.recordStepTimeout();
        saga.recordStepTimeout();
        assertThat(saga.getStepTimeouts()).isEqualTo(2);
        saga.paymentCompleted(UUID.randomUUID());
        assertThat(saga.getStepTimeouts()).isZero();
    }
}
