package com.example.saga.orchestrator.messaging;

import com.example.saga.common.inbox.InboxGuard;
import com.example.saga.messages.SagaHeaders;
import com.example.saga.messages.SagaTopics;
import com.example.saga.messages.events.*;
import com.example.saga.orchestrator.domain.SagaInstance;
import com.example.saga.orchestrator.domain.SagaOrchestrator;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/** Maps each participant event onto a saga transition. Inbox check and state change share one transaction. */
@Component
@KafkaListener(topics = {
        SagaTopics.ORDER_CREATED, SagaTopics.ORDER_COMPLETED, SagaTopics.ORDER_CANCELLED,
        SagaTopics.PAYMENT_COMPLETED, SagaTopics.PAYMENT_FAILED, SagaTopics.PAYMENT_REFUNDED,
        SagaTopics.INVENTORY_RESERVED, SagaTopics.INVENTORY_FAILED})
public class SagaEventHandler {

    private final InboxGuard inbox;
    private final SagaOrchestrator orchestrator;

    public SagaEventHandler(InboxGuard inbox, SagaOrchestrator orchestrator) {
        this.inbox = inbox;
        this.orchestrator = orchestrator;
    }

    @KafkaHandler
    @Transactional
    public void on(OrderCreatedEvent e, @Header(SagaHeaders.MESSAGE_ID) String messageId) {
        if (inbox.firstDelivery(messageId)) {
            orchestrator.start(e);
        }
    }

    @KafkaHandler
    @Transactional
    public void on(PaymentCompletedEvent e, @Header(SagaHeaders.MESSAGE_ID) String messageId) {
        if (inbox.firstDelivery(messageId)) {
            orchestrator.advance(e.sagaId(), "PaymentCompleted", s -> s.paymentCompleted(e.paymentId()));
        }
    }

    @KafkaHandler
    @Transactional
    public void on(PaymentFailedEvent e, @Header(SagaHeaders.MESSAGE_ID) String messageId) {
        if (inbox.firstDelivery(messageId)) {
            orchestrator.advance(e.sagaId(), "PaymentFailed", s -> s.paymentFailed(e.reason()));
        }
    }

    @KafkaHandler
    @Transactional
    public void on(InventoryReservedEvent e, @Header(SagaHeaders.MESSAGE_ID) String messageId) {
        if (inbox.firstDelivery(messageId)) {
            orchestrator.advance(e.sagaId(), "InventoryReserved", s -> s.inventoryReserved(e.reservationId()));
        }
    }

    @KafkaHandler
    @Transactional
    public void on(InventoryFailedEvent e, @Header(SagaHeaders.MESSAGE_ID) String messageId) {
        if (inbox.firstDelivery(messageId)) {
            orchestrator.advance(e.sagaId(), "InventoryFailed", s -> s.inventoryFailed(e.reason()));
        }
    }

    @KafkaHandler
    @Transactional
    public void on(PaymentRefundedEvent e, @Header(SagaHeaders.MESSAGE_ID) String messageId) {
        if (inbox.firstDelivery(messageId)) {
            orchestrator.advance(e.sagaId(), "PaymentRefunded", SagaInstance::paymentRefunded);
        }
    }

    @KafkaHandler
    @Transactional
    public void on(OrderCompletedEvent e, @Header(SagaHeaders.MESSAGE_ID) String messageId) {
        if (inbox.firstDelivery(messageId)) {
            orchestrator.advance(e.sagaId(), "OrderCompleted", SagaInstance::orderCompleted);
        }
    }

    @KafkaHandler
    @Transactional
    public void on(OrderCancelledEvent e, @Header(SagaHeaders.MESSAGE_ID) String messageId) {
        if (inbox.firstDelivery(messageId)) {
            orchestrator.advance(e.sagaId(), "OrderCancelled", SagaInstance::orderCancelled);
        }
    }
}
