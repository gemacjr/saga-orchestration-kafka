package com.example.saga.payment;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;

/** Bound from SSM Parameter Store: /saga/payment-service/payment.max-amount */
@Validated
@ConfigurationProperties(prefix = "payment")
public record PaymentProperties(@NotNull BigDecimal maxAmount) {
}
