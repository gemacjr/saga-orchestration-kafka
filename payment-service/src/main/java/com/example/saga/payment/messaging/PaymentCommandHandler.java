package com.example.saga.payment.messaging;

import com.example.saga.common.inbox.InboxGuard;
import com.example.saga.messages.SagaHeaders;
import com.example.saga.messages.SagaTopics;
import com.example.saga.messages.commands.ProcessPaymentCommand;
import com.example.saga.messages.commands.RefundPaymentCommand;
import com.example.saga.payment.domain.PaymentService;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@KafkaListener(topics = {SagaTopics.PROCESS_PAYMENT, SagaTopics.REFUND_PAYMENT})
public class PaymentCommandHandler {

    private final InboxGuard inbox;
    private final PaymentService paymentService;

    public PaymentCommandHandler(InboxGuard inbox, PaymentService paymentService) {
        this.inbox = inbox;
        this.paymentService = paymentService;
    }

    @KafkaHandler
    @Transactional
    public void on(ProcessPaymentCommand command, @Header(SagaHeaders.MESSAGE_ID) String messageId) {
        if (inbox.firstDelivery(messageId)) {
            paymentService.process(command);
        }
    }

    @KafkaHandler
    @Transactional
    public void on(RefundPaymentCommand command, @Header(SagaHeaders.MESSAGE_ID) String messageId) {
        if (inbox.firstDelivery(messageId)) {
            paymentService.refund(command);
        }
    }
}
