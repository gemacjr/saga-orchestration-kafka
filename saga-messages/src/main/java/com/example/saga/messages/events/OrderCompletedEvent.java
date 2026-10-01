package com.example.saga.messages.events;

import com.example.saga.messages.SagaMessage;

import java.util.UUID;

public record OrderCompletedEvent(UUID sagaId, UUID orderId) implements SagaMessage {
}
