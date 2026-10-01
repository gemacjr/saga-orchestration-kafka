package com.example.saga.messages.commands;

import com.example.saga.messages.SagaMessage;

import java.util.UUID;

public record CompleteOrderCommand(UUID sagaId, UUID orderId) implements SagaMessage {
}
