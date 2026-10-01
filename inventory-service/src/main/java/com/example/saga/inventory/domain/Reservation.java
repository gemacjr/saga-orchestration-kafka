package com.example.saga.inventory.domain;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "reservations")
public class Reservation {

    @Id
    private UUID id;

    @Column(nullable = false, updatable = false)
    private UUID sagaId;

    @Column(nullable = false, updatable = false)
    private UUID orderId;

    @Column(nullable = false, updatable = false)
    private String productId;

    private int quantity;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ReservationStatus status;

    private String reason;

    @Version
    private long version;

    @CreationTimestamp
    private Instant createdAt;

    @UpdateTimestamp
    private Instant updatedAt;

    protected Reservation() {
    }

    private Reservation(UUID sagaId, UUID orderId, String productId, int quantity,
                        ReservationStatus status, String reason) {
        this.id = UUID.randomUUID();
        this.sagaId = sagaId;
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.status = status;
        this.reason = reason;
    }

    public static Reservation reserved(UUID sagaId, UUID orderId, String productId, int quantity) {
        return new Reservation(sagaId, orderId, productId, quantity, ReservationStatus.RESERVED, null);
    }

    public static Reservation failed(UUID sagaId, UUID orderId, String productId, int quantity, String reason) {
        return new Reservation(sagaId, orderId, productId, quantity, ReservationStatus.FAILED, reason);
    }

    public UUID getId() { return id; }
    public UUID getSagaId() { return sagaId; }
    public UUID getOrderId() { return orderId; }
    public String getProductId() { return productId; }
    public int getQuantity() { return quantity; }
    public ReservationStatus getStatus() { return status; }
    public String getReason() { return reason; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
