package com.example.saga.messages.events;

import com.example.saga.messages.SagaMessage;

import java.util.UUID;

public record PaymentCompletedEvent(UUID sagaId, UUID orderId, UUID paymentId) implements SagaMessage {
}
