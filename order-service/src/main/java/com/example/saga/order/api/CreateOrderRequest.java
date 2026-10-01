package com.example.saga.order.api;

import jakarta.validation.constraints.*;

import java.math.BigDecimal;

public record CreateOrderRequest(
        @NotBlank @Size(max = 100) String productId,
        @Positive @Max(10_000) int quantity,
        @NotNull @Positive @Digits(integer = 17, fraction = 2) BigDecimal amount) {
}
