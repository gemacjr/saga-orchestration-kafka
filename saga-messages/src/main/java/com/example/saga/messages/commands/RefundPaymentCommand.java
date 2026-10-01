package com.example.saga.messages.commands;

import com.example.saga.messages.SagaMessage;

import java.util.UUID;

public record RefundPaymentCommand(UUID sagaId, UUID orderId, UUID paymentId, String reason) implements SagaMessage {
}
