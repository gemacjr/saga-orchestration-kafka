package com.example.saga.payment.domain;

import com.example.saga.common.outbox.OutboxWriter;
import com.example.saga.messages.SagaTopics;
import com.example.saga.messages.commands.ProcessPaymentCommand;
import com.example.saga.messages.commands.RefundPaymentCommand;
import com.example.saga.messages.events.PaymentCompletedEvent;
import com.example.saga.messages.events.PaymentFailedEvent;
import com.example.saga.messages.events.PaymentRefundedEvent;
import com.example.saga.payment.PaymentProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * One payment per saga (unique saga_id). Every command first looks up the existing payment, so a
 * repeated command replays the recorded outcome instead of charging twice.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository payments;
    private final OutboxWriter outbox;
    private final PaymentProperties properties;

    public PaymentService(PaymentRepository payments, OutboxWriter outbox, PaymentProperties properties) {
        this.payments = payments;
        this.outbox = outbox;
        this.properties = properties;
    }

    @Transactional
    public void process(ProcessPaymentCommand command) {
        Payment payment = payments.findBySagaId(command.sagaId())
                .orElseGet(() -> payments.save(charge(command)));

        switch (payment.getStatus()) {
            case COMPLETED -> outbox.send(SagaTopics.PAYMENT_COMPLETED,
                    new PaymentCompletedEvent(command.sagaId(), command.orderId(), payment.getId()));
            case FAILED, VOIDED -> outbox.send(SagaTopics.PAYMENT_FAILED,
                    new PaymentFailedEvent(command.sagaId(), command.orderId(), payment.getReason()));
            case REFUNDED -> log.info("Ignoring ProcessPayment for already refunded saga {}", command.sagaId());
        }
    }

    @Transactional
    public void refund(RefundPaymentCommand command) {
        Payment payment = payments.findBySagaId(command.sagaId())
                .orElseGet(() -> payments.save(Payment.voided(command.sagaId(), command.orderId(), command.reason())));
        payment.refund(command.reason());
        outbox.send(SagaTopics.PAYMENT_REFUNDED,
                new PaymentRefundedEvent(command.sagaId(), command.orderId(), payment.getId()));
    }

    /** Stand-in for a payment provider call. Demo rule from the article: amount above the limit is declined. */
    private Payment charge(ProcessPaymentCommand command) {
        if (command.amount().compareTo(properties.maxAmount()) > 0) {
            return Payment.failed(command.sagaId(), command.orderId(), command.amount(),
                    "Amount " + command.amount() + " exceeds limit " + properties.maxAmount());
        }
        return Payment.completed(command.sagaId(), command.orderId(), command.amount());
    }
}
