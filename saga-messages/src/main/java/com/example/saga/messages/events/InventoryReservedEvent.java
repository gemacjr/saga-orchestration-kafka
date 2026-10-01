package com.example.saga.messages.events;

import com.example.saga.messages.SagaMessage;

import java.util.UUID;

public record InventoryReservedEvent(UUID sagaId, UUID orderId, UUID reservationId) implements SagaMessage {
}
