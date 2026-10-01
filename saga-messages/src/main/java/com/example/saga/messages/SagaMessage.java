package com.example.saga.messages;

import java.util.UUID;

/** Every command and event carries the saga id; it is also the Kafka key, so all messages of one saga stay ordered on one partition. */
public interface SagaMessage {
    UUID sagaId();
}
