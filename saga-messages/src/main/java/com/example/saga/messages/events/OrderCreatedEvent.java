package com.example.saga.messages.events;

import com.example.saga.messages.SagaMessage;

import java.math.BigDecimal;
import java.util.UUID;

public record OrderCreatedEvent(UUID sagaId, UUID orderId, String productId, int quantity, BigDecimal amount) implements SagaMessage {
}
