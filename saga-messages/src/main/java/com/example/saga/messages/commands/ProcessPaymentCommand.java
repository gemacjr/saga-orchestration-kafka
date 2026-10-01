package com.example.saga.messages.commands;

import com.example.saga.messages.SagaMessage;

import java.math.BigDecimal;
import java.util.UUID;

public record ProcessPaymentCommand(UUID sagaId, UUID orderId, BigDecimal amount) implements SagaMessage {
}
