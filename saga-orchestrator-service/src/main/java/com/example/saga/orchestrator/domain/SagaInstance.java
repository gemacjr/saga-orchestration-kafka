package com.example.saga.orchestrator.domain;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Persistent saga state machine. Every transition method is guarded by the expected current state and
 * returns false (no change) for duplicate, late or out-of-order events, so the orchestrator only issues
 * the next command when the saga actually advanced. {@code @Version} rejects concurrent updates.
 *
 * <pre>
 * PAYMENT_PROCESSING --paymentCompleted--> INVENTORY_RESERVING --inventoryReserved--> ORDER_COMPLETING --orderCompleted--> COMPLETED
 *        |                                         |
 *   paymentFailed                           inventoryFailed
 *        v                                         v
 * ORDER_CANCELLING <------paymentRefunded------ COMPENSATING
 *        |
 *   orderCancelled --> FAILED (nothing to undo) | COMPENSATED (payment was refunded)
 * </pre>
 */
@Entity
@Table(name = "saga_instance")
public class SagaInstance {

    @Id
    private UUID sagaId;

    @Column(nullable = false, updatable = false)
    private UUID orderId;

    @Column(nullable = false, updatable = false)
    private String productId;

    private int quantity;

    @Column(nullable = false, updatable = false)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private SagaStatus status;

    private UUID paymentId;

    private UUID reservationId;

    private String failureReason;

    /** True once a completed step had to be undone; decides FAILED vs COMPENSATED at the end. */
    private boolean compensating;

    /** Number of times the current step timed out; reset on every transition. */
    private int stepTimeouts;

    @Version
    private long version;

    @CreationTimestamp
    private Instant createdAt;

    @UpdateTimestamp
    private Instant updatedAt;

    protected SagaInstance() {
    }

    public static SagaInstance start(UUID sagaId, UUID orderId, String productId, int quantity, BigDecimal amount) {
        SagaInstance saga = new SagaInstance();
        saga.sagaId = sagaId;
        saga.orderId = orderId;
        saga.productId = productId;
        saga.quantity = quantity;
        saga.amount = amount;
        saga.status = SagaStatus.PAYMENT_PROCESSING;
        return saga;
    }

    public boolean paymentCompleted(UUID paymentId) {
        if (status != SagaStatus.PAYMENT_PROCESSING) {
            return false;
        }
        this.paymentId = paymentId;
        return moveTo(SagaStatus.INVENTORY_RESERVING);
    }

    public boolean paymentFailed(String reason) {
        if (status != SagaStatus.PAYMENT_PROCESSING) {
            return false;
        }
        this.failureReason = reason;
        return moveTo(SagaStatus.ORDER_CANCELLING);
    }

    public boolean inventoryReserved(UUID reservationId) {
        if (status != SagaStatus.INVENTORY_RESERVING) {
            return false;
        }
        this.reservationId = reservationId;
        return moveTo(SagaStatus.ORDER_COMPLETING);
    }

    public boolean inventoryFailed(String reason) {
        if (status != SagaStatus.INVENTORY_RESERVING) {
            return false;
        }
        return startCompensation(reason);
    }

    public boolean paymentRefunded() {
        if (status != SagaStatus.COMPENSATING) {
            return false;
        }
        return moveTo(SagaStatus.ORDER_CANCELLING);
    }

    public boolean orderCompleted() {
        if (status != SagaStatus.ORDER_COMPLETING) {
            return false;
        }
        return moveTo(SagaStatus.COMPLETED);
    }

    public boolean orderCancelled() {
        if (status != SagaStatus.ORDER_CANCELLING) {
            return false;
        }
        return moveTo(compensating ? SagaStatus.COMPENSATED : SagaStatus.FAILED);
    }

    /**
     * Undo the payment (refund, or void it if it never happened) and then cancel the order.
     * Only possible before the order-completion command went out.
     */
    public boolean startCompensation(String reason) {
        if (status != SagaStatus.PAYMENT_PROCESSING && status != SagaStatus.INVENTORY_RESERVING) {
            return false;
        }
        this.failureReason = reason;
        this.compensating = true;
        return moveTo(SagaStatus.COMPENSATING);
    }

    /** Called by the timeout watcher; also bumps updated_at so the saga is re-checked one timeout later. */
    public void recordStepTimeout() {
        stepTimeouts++;
    }

    private boolean moveTo(SagaStatus next) {
        this.status = next;
        this.stepTimeouts = 0;
        return true;
    }

    public UUID getSagaId() { return sagaId; }
    public UUID getOrderId() { return orderId; }
    public String getProductId() { return productId; }
    public int getQuantity() { return quantity; }
    public BigDecimal getAmount() { return amount; }
    public SagaStatus getStatus() { return status; }
    public UUID getPaymentId() { return paymentId; }
    public UUID getReservationId() { return reservationId; }
    public String getFailureReason() { return failureReason; }
    public boolean isCompensating() { return compensating; }
    public int getStepTimeouts() { return stepTimeouts; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
