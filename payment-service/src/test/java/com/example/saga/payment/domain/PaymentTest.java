package com.example.saga.payment.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentTest {

    private final UUID sagaId = UUID.randomUUID();
    private final UUID orderId = UUID.randomUUID();

    @Test
    void completedPaymentCanBeRefundedOnce() {
        Payment payment = Payment.completed(sagaId, orderId, BigDecimal.TEN);
        payment.refund("inventory failed");
        payment.refund("again");

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.REFUNDED);
        assertThat(payment.getReason()).isEqualTo("inventory failed");
        assertThat(payment.getSagaId()).isEqualTo(sagaId);
        assertThat(payment.getOrderId()).isEqualTo(orderId);
        assertThat(payment.getAmount()).isEqualByComparingTo(BigDecimal.TEN);
    }

    @Test
    void refundingAFailedOrVoidedPaymentChangesNothing() {
        Payment failed = Payment.failed(sagaId, orderId, BigDecimal.TEN, "declined");
        failed.refund("compensation");
        assertThat(failed.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(failed.getReason()).isEqualTo("declined");

        Payment voided = Payment.voided(sagaId, orderId, "timeout");
        voided.refund("compensation");
        assertThat(voided.getStatus()).isEqualTo(PaymentStatus.VOIDED);
        assertThat(voided.getAmount()).isNull();
    }
}
