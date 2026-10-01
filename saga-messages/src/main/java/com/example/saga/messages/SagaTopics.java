package com.example.saga.messages;

import java.util.List;

public final class SagaTopics {

    // Events (something happened)
    public static final String ORDER_CREATED = "saga.order.created";
    public static final String ORDER_COMPLETED = "saga.order.completed";
    public static final String ORDER_CANCELLED = "saga.order.cancelled";
    public static final String PAYMENT_COMPLETED = "saga.payment.completed";
    public static final String PAYMENT_FAILED = "saga.payment.failed";
    public static final String PAYMENT_REFUNDED = "saga.payment.refunded";
    public static final String INVENTORY_RESERVED = "saga.inventory.reserved";
    public static final String INVENTORY_FAILED = "saga.inventory.failed";

    // Commands (do something)
    public static final String PROCESS_PAYMENT = "saga.payment.process";
    public static final String REFUND_PAYMENT = "saga.payment.refund";
    public static final String RESERVE_INVENTORY = "saga.inventory.reserve";
    public static final String COMPLETE_ORDER = "saga.order.complete";
    public static final String CANCEL_ORDER = "saga.order.cancel";

    /** Suffix used by Spring Kafka's DeadLetterPublishingRecoverer. */
    public static final String DLT_SUFFIX = "-dlt";

    public static final List<String> ALL = List.of(
            ORDER_CREATED, ORDER_COMPLETED, ORDER_CANCELLED,
            PAYMENT_COMPLETED, PAYMENT_FAILED, PAYMENT_REFUNDED,
            INVENTORY_RESERVED, INVENTORY_FAILED,
            PROCESS_PAYMENT, REFUND_PAYMENT, RESERVE_INVENTORY, COMPLETE_ORDER, CANCEL_ORDER);

    private SagaTopics() {
    }
}
