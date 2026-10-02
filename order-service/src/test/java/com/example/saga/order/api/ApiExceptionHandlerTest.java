package com.example.saga.order.api;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ApiExceptionHandlerTest {

    @Test
    void duplicateIdempotencyKeyRaceMapsTo409() {
        var problem = new ApiExceptionHandler().conflict();
        assertThat(problem.getStatus()).isEqualTo(409);
        assertThat(problem.getDetail()).contains("Idempotency-Key");
    }
}
