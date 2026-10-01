package com.example.saga.messages.events;

import com.example.saga.messages.SagaMessage;

import java.util.UUID;

public record PaymentFailedEvent(UUID sagaId, UUID orderId, String reason) implements SagaMessage {
}
