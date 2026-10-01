package com.example.saga.payment;

import com.example.saga.messages.SagaTopics;
import com.example.saga.messages.commands.ProcessPaymentCommand;
import com.example.saga.messages.commands.RefundPaymentCommand;
import com.example.saga.messages.events.PaymentCompletedEvent;
import com.example.saga.messages.events.PaymentFailedEvent;
import com.example.saga.messages.events.PaymentRefundedEvent;
import com.example.saga.payment.domain.Payment;
import com.example.saga.payment.domain.PaymentRepository;
import com.example.saga.payment.domain.PaymentStatus;
import com.example.saga.testsupport.KafkaTestClient;
import com.example.saga.testsupport.SagaIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SagaIntegrationTest
@AutoConfigureMockMvc
class PaymentServiceIntegrationTest {

    @Autowired KafkaTestClient kafka;
    @Autowired PaymentRepository payments;
    @Autowired MockMvc mvc;

    private final UUID sagaId = UUID.randomUUID();
    private final UUID orderId = UUID.randomUUID();

    private Payment awaitPayment() {
        return await().until(() -> payments.findBySagaId(sagaId).orElse(null), p -> p != null);
    }

    private ProcessPaymentCommand process(String amount) {
        return new ProcessPaymentCommand(sagaId, orderId, new BigDecimal(amount));
    }

    @Test
    void amountWithinLimitIsCharged() {
        kafka.send(SagaTopics.PROCESS_PAYMENT, process("1000.00"));  // boundary: limit itself is allowed

        PaymentCompletedEvent event = kafka.expectOne(SagaTopics.PAYMENT_COMPLETED, sagaId, PaymentCompletedEvent.class);
        Payment payment = awaitPayment();
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(event.paymentId()).isEqualTo(payment.getId());
        assertThat(event.orderId()).isEqualTo(orderId);
    }

    @Test
    void amountAboveLimitIsDeclined() {
        kafka.send(SagaTopics.PROCESS_PAYMENT, process("1000.01"));

        PaymentFailedEvent event = kafka.expectOne(SagaTopics.PAYMENT_FAILED, sagaId, PaymentFailedEvent.class);
        assertThat(event.reason()).contains("exceeds limit 1000");
        assertThat(awaitPayment().getStatus()).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void resentProcessCommandNeverChargesTwiceAndReplaysTheOutcome() {
        kafka.send(SagaTopics.PROCESS_PAYMENT, process("50"));
        kafka.send(SagaTopics.PROCESS_PAYMENT, process("50"));

        await().untilAsserted(() ->
                assertThat(kafka.recordsFor(SagaTopics.PAYMENT_COMPLETED, sagaId, Duration.ofMillis(500))).hasSize(2));
        assertThat(kafka.recordsFor(SagaTopics.PAYMENT_COMPLETED, sagaId, Duration.ZERO))
                .extracting(r -> kafka.read(r, PaymentCompletedEvent.class).paymentId())
                .containsOnly(awaitPayment().getId());
        assertThat(payments.findAll()).filteredOn(p -> p.getSagaId().equals(sagaId)).hasSize(1);
    }

    @Test
    void redeliveredMessageIsIgnoredByTheInbox() {
        String messageId = kafka.send(SagaTopics.PROCESS_PAYMENT, process("50"));
        kafka.send(SagaTopics.PROCESS_PAYMENT, process("50"), messageId);

        assertThat(kafka.recordsFor(SagaTopics.PAYMENT_COMPLETED, sagaId, Duration.ofSeconds(3))).hasSize(1);
    }

    @Test
    void completedPaymentIsRefunded() {
        kafka.send(SagaTopics.PROCESS_PAYMENT, process("300"));
        UUID paymentId = kafka.expectOne(SagaTopics.PAYMENT_COMPLETED, sagaId, PaymentCompletedEvent.class).paymentId();

        kafka.send(SagaTopics.REFUND_PAYMENT, new RefundPaymentCommand(sagaId, orderId, paymentId, "inventory failed"));

        assertThat(kafka.expectOne(SagaTopics.PAYMENT_REFUNDED, sagaId, PaymentRefundedEvent.class).paymentId())
                .isEqualTo(paymentId);
        await().untilAsserted(() -> assertThat(payments.findById(paymentId).orElseThrow().getStatus())
                .isEqualTo(PaymentStatus.REFUNDED));
    }

    @Test
    void refundBeforeChargeVoidsThePaymentAndBlocksALateCharge() {
        kafka.send(SagaTopics.REFUND_PAYMENT, new RefundPaymentCommand(sagaId, orderId, null, "timed out"));
        kafka.expectOne(SagaTopics.PAYMENT_REFUNDED, sagaId, PaymentRefundedEvent.class);
        assertThat(awaitPayment().getStatus()).isEqualTo(PaymentStatus.VOIDED);

        kafka.send(SagaTopics.PROCESS_PAYMENT, process("50"));  // the delayed original command

        assertThat(kafka.expectOne(SagaTopics.PAYMENT_FAILED, sagaId, PaymentFailedEvent.class).reason())
                .isEqualTo("timed out");
        assertThat(kafka.recordsFor(SagaTopics.PAYMENT_COMPLETED, sagaId, Duration.ofSeconds(1))).isEmpty();
        assertThat(payments.findBySagaId(sagaId).orElseThrow().getStatus()).isEqualTo(PaymentStatus.VOIDED);
    }

    @Test
    void refundOfDeclinedPaymentConfirmsWithoutChangingIt() {
        kafka.send(SagaTopics.PROCESS_PAYMENT, process("5000"));
        kafka.expectOne(SagaTopics.PAYMENT_FAILED, sagaId, PaymentFailedEvent.class);

        kafka.send(SagaTopics.REFUND_PAYMENT, new RefundPaymentCommand(sagaId, orderId, null, "compensation"));

        kafka.expectOne(SagaTopics.PAYMENT_REFUNDED, sagaId, PaymentRefundedEvent.class);
        assertThat(payments.findBySagaId(sagaId).orElseThrow().getStatus()).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void lateProcessCommandAfterRefundIsIgnored() {
        kafka.send(SagaTopics.PROCESS_PAYMENT, process("20"));
        UUID paymentId = kafka.expectOne(SagaTopics.PAYMENT_COMPLETED, sagaId, PaymentCompletedEvent.class).paymentId();
        kafka.send(SagaTopics.REFUND_PAYMENT, new RefundPaymentCommand(sagaId, orderId, paymentId, "x"));
        kafka.expectOne(SagaTopics.PAYMENT_REFUNDED, sagaId, PaymentRefundedEvent.class);

        kafka.send(SagaTopics.PROCESS_PAYMENT, process("20"));

        assertThat(kafka.recordsFor(SagaTopics.PAYMENT_COMPLETED, sagaId, Duration.ofSeconds(2))).hasSize(1);
        assertThat(kafka.recordsFor(SagaTopics.PAYMENT_FAILED, sagaId, Duration.ZERO)).isEmpty();
    }

    @Test
    void paymentsAreListed() throws Exception {
        kafka.send(SagaTopics.PROCESS_PAYMENT, process("75"));
        Payment payment = awaitPayment();

        mvc.perform(get("/payments").param("limit", "1000"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.paymentId == '" + payment.getId() + "')].status").value("COMPLETED"));
    }
}
