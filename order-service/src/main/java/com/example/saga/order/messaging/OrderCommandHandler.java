package com.example.saga.order.messaging;

import com.example.saga.common.inbox.InboxGuard;
import com.example.saga.messages.SagaHeaders;
import com.example.saga.messages.SagaTopics;
import com.example.saga.messages.commands.CancelOrderCommand;
import com.example.saga.messages.commands.CompleteOrderCommand;
import com.example.saga.order.domain.OrderService;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Executes the orchestrator's commands. A redelivered message (same message id) is skipped by the
 * inbox; a re-sent command (new message id) is absorbed by the idempotent domain transitions and the
 * reply is published again, which is harmless for the orchestrator.
 */
@Component
@KafkaListener(topics = {SagaTopics.COMPLETE_ORDER, SagaTopics.CANCEL_ORDER})
public class OrderCommandHandler {

    private final InboxGuard inbox;
    private final OrderService orderService;

    public OrderCommandHandler(InboxGuard inbox, OrderService orderService) {
        this.inbox = inbox;
        this.orderService = orderService;
    }

    @KafkaHandler
    @Transactional
    public void on(CompleteOrderCommand command, @Header(SagaHeaders.MESSAGE_ID) String messageId) {
        if (inbox.firstDelivery(messageId)) {
            orderService.complete(command.orderId());
        }
    }

    @KafkaHandler
    @Transactional
    public void on(CancelOrderCommand command, @Header(SagaHeaders.MESSAGE_ID) String messageId) {
        if (inbox.firstDelivery(messageId)) {
            orderService.cancel(command.orderId(), command.reason());
        }
    }
}
