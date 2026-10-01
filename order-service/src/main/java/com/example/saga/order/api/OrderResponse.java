package com.example.saga.order.api;

import com.example.saga.order.domain.Order;
import com.example.saga.order.domain.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderResponse(UUID orderId, UUID sagaId, String productId, int quantity, BigDecimal amount,
                            OrderStatus status, String failureReason, Instant createdAt, Instant updatedAt) {

    static OrderResponse from(Order o) {
        return new OrderResponse(o.getId(), o.getSagaId(), o.getProductId(), o.getQuantity(), o.getAmount(),
                o.getStatus(), o.getFailureReason(), o.getCreatedAt(), o.getUpdatedAt());
    }
}
