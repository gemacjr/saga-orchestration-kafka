package com.example.saga.common;

/** A message that can never succeed (broken invariant, unknown aggregate). Skips retries and goes straight to the DLT. */
public class NonRetryableSagaException extends RuntimeException {

    public NonRetryableSagaException(String message) {
        super(message);
    }
}
