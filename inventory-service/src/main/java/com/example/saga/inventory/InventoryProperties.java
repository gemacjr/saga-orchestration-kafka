package com.example.saga.inventory;

import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/** Bound from SSM Parameter Store: /saga/inventory-service/inventory.max-quantity-per-order */
@Validated
@ConfigurationProperties(prefix = "inventory")
public record InventoryProperties(@Positive int maxQuantityPerOrder) {
}
