package com.example.saga.messages.commands;

import com.example.saga.messages.SagaMessage;

import java.util.UUID;

public record ReserveInventoryCommand(UUID sagaId, UUID orderId, String productId, int quantity) implements SagaMessage {
}
