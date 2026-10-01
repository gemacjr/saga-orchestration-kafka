package com.example.saga.payment.domain;

public enum PaymentStatus {
    COMPLETED,
    FAILED,
    REFUNDED,
    /** Refund arrived before the charge (e.g. saga timed out): blocks a late ProcessPayment from charging. */
    VOIDED
}
