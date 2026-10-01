package com.example.saga.order.domain;

import com.example.saga.common.NonRetryableSagaException;
import com.example.saga.common.outbox.OutboxWriter;
import com.example.saga.messages.SagaTopics;
import com.example.saga.messages.events.OrderCancelledEvent;
import com.example.saga.messages.events.OrderCompletedEvent;
import com.example.saga.messages.events.OrderCreatedEvent;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orders;
    private final OutboxWriter outbox;

    public OrderService(OrderRepository orders, OutboxWriter outbox) {
        this.orders = orders;
        this.outbox = outbox;
    }

    /** Saves the PENDING order and the OrderCreated event atomically; the saga starts once the outbox is relayed. */
    @Transactional
    public Order placeOrder(String productId, int quantity, BigDecimal amount, String idempotencyKey) {
        if (idempotencyKey != null) {
            var existing = orders.findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                return existing.get();
            }
        }
        Order order = orders.save(Order.place(productId, quantity, amount, idempotencyKey));
        outbox.send(SagaTopics.ORDER_CREATED, new OrderCreatedEvent(
                order.getSagaId(), order.getId(), productId, quantity, amount));
        return order;
    }

    @Transactional
    public void complete(UUID orderId) {
        Order order = load(orderId);
        order.complete();
        outbox.send(SagaTopics.ORDER_COMPLETED, new OrderCompletedEvent(order.getSagaId(), order.getId()));
    }

    @Transactional
    public void cancel(UUID orderId, String reason) {
        Order order = load(orderId);
        order.cancel(reason);
        outbox.send(SagaTopics.ORDER_CANCELLED, new OrderCancelledEvent(order.getSagaId(), order.getId(), reason));
    }

    private Order load(UUID orderId) {
        return orders.findById(orderId)
                .orElseThrow(() -> new NonRetryableSagaException("Unknown order " + orderId));
    }
}
