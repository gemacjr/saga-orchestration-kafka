package com.example.saga.messages;

public final class SagaHeaders {

    /** Unique id of a single message (the outbox row id). Consumers de-duplicate on it. */
    public static final String MESSAGE_ID = "saga_message_id";

    /** Logical message type, see {@link MessageTypes}. Same header name Spring Kafka's JsonDeserializer reads. */
    public static final String MESSAGE_TYPE = "__TypeId__";

    private SagaHeaders() {
    }
}
