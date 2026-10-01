package com.example.saga.payment.domain;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payments")
public class Payment {

    @Id
    private UUID id;

    @Column(nullable = false, updatable = false)
    private UUID sagaId;

    @Column(nullable = false, updatable = false)
    private UUID orderId;

    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PaymentStatus status;

    private String reason;

    @Version
    private long version;

    @CreationTimestamp
    private Instant createdAt;

    @UpdateTimestamp
    private Instant updatedAt;

    protected Payment() {
    }

    private Payment(UUID sagaId, UUID orderId, BigDecimal amount, PaymentStatus status, String reason) {
        this.id = UUID.randomUUID();
        this.sagaId = sagaId;
        this.orderId = orderId;
        this.amount = amount;
        this.status = status;
        this.reason = reason;
    }

    public static Payment completed(UUID sagaId, UUID orderId, BigDecimal amount) {
        return new Payment(sagaId, orderId, amount, PaymentStatus.COMPLETED, null);
    }

    public static Payment failed(UUID sagaId, UUID orderId, BigDecimal amount, String reason) {
        return new Payment(sagaId, orderId, amount, PaymentStatus.FAILED, reason);
    }

    public static Payment voided(UUID sagaId, UUID orderId, String reason) {
        return new Payment(sagaId, orderId, null, PaymentStatus.VOIDED, reason);
    }

    /** Compensating action. Refunding anything other than a completed payment changes nothing. */
    public void refund(String reason) {
        if (status == PaymentStatus.COMPLETED) {
            status = PaymentStatus.REFUNDED;
            this.reason = reason;
        }
    }

    public UUID getId() { return id; }
    public UUID getSagaId() { return sagaId; }
    public UUID getOrderId() { return orderId; }
    public BigDecimal getAmount() { return amount; }
    public PaymentStatus getStatus() { return status; }
    public String getReason() { return reason; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
