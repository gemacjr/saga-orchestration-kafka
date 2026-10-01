package com.example.saga.messages;

import com.example.saga.messages.commands.*;
import com.example.saga.messages.events.*;

import java.util.Map;

/**
 * Registry of logical, versioned type names. The wire format carries these names instead of
 * Java class names, so packages can be refactored and a v2 schema can be introduced side by side.
 */
public final class MessageTypes {

    private static final Map<String, Class<? extends SagaMessage>> TYPES = Map.ofEntries(
            Map.entry("OrderCreated.v1", OrderCreatedEvent.class),
            Map.entry("OrderCompleted.v1", OrderCompletedEvent.class),
            Map.entry("OrderCancelled.v1", OrderCancelledEvent.class),
            Map.entry("PaymentCompleted.v1", PaymentCompletedEvent.class),
            Map.entry("PaymentFailed.v1", PaymentFailedEvent.class),
            Map.entry("PaymentRefunded.v1", PaymentRefundedEvent.class),
            Map.entry("InventoryReserved.v1", InventoryReservedEvent.class),
            Map.entry("InventoryFailed.v1", InventoryFailedEvent.class),
            Map.entry("ProcessPayment.v1", ProcessPaymentCommand.class),
            Map.entry("RefundPayment.v1", RefundPaymentCommand.class),
            Map.entry("ReserveInventory.v1", ReserveInventoryCommand.class),
            Map.entry("CompleteOrder.v1", CompleteOrderCommand.class),
            Map.entry("CancelOrder.v1", CancelOrderCommand.class));

    public static Map<String, Class<? extends SagaMessage>> all() {
        return TYPES;
    }

    public static String nameOf(Class<?> type) {
        return TYPES.entrySet().stream()
                .filter(e -> e.getValue().equals(type))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unregistered message type: " + type.getName()));
    }

    private MessageTypes() {
    }
}
