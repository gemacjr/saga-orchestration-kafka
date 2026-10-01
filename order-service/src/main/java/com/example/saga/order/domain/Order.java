package com.example.saga.order.domain;

import com.example.saga.common.NonRetryableSagaException;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    private UUID id;

    @Column(nullable = false, updatable = false)
    private UUID sagaId;

    @Column(nullable = false, updatable = false)
    private String productId;

    private int quantity;

    @Column(nullable = false, updatable = false)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;

    private String failureReason;

    @Column(updatable = false)
    private String idempotencyKey;

    @Version
    private long version;

    @CreationTimestamp
    private Instant createdAt;

    @UpdateTimestamp
    private Instant updatedAt;

    protected Order() {
    }

    public static Order place(String productId, int quantity, BigDecimal amount, String idempotencyKey) {
        Order order = new Order();
        order.id = UUID.randomUUID();
        order.sagaId = UUID.randomUUID();
        order.productId = productId;
        order.quantity = quantity;
        order.amount = amount;
        order.idempotencyKey = idempotencyKey;
        order.status = OrderStatus.PENDING;
        return order;
    }

    /** Idempotent: completing a completed order is a no-op. */
    public void complete() {
        if (status == OrderStatus.COMPLETED) {
            return;
        }
        requirePending("complete");
        status = OrderStatus.COMPLETED;
    }

    /** Idempotent: cancelling a cancelled order is a no-op. */
    public void cancel(String reason) {
        if (status == OrderStatus.CANCELLED) {
            return;
        }
        requirePending("cancel");
        status = OrderStatus.CANCELLED;
        failureReason = reason;
    }

    private void requirePending(String action) {
        if (status != OrderStatus.PENDING) {
            throw new NonRetryableSagaException("Cannot " + action + " order " + id + " in status " + status);
        }
    }

    public UUID getId() { return id; }
    public UUID getSagaId() { return sagaId; }
    public String getProductId() { return productId; }
    public int getQuantity() { return quantity; }
    public BigDecimal getAmount() { return amount; }
    public OrderStatus getStatus() { return status; }
    public String getFailureReason() { return failureReason; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
