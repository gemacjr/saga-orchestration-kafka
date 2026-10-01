package com.example.saga.order.domain;

import com.example.saga.common.NonRetryableSagaException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderTest {

    private final Order order = Order.place("product-100", 2, new BigDecimal("250.00"), "key-1");

    @Test
    void newOrderIsPendingWithItsOwnSagaId() {
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.getId()).isNotNull();
        assertThat(order.getSagaId()).isNotNull().isNotEqualTo(order.getId());
        assertThat(order.getProductId()).isEqualTo("product-100");
        assertThat(order.getQuantity()).isEqualTo(2);
        assertThat(order.getAmount()).isEqualByComparingTo("250.00");
    }

    @Test
    void completeIsIdempotent() {
        order.complete();
        order.complete();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.COMPLETED);
    }

    @Test
    void cancelIsIdempotentAndKeepsFirstReason() {
        order.cancel("payment declined");
        order.cancel("other");
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getFailureReason()).isEqualTo("payment declined");
    }

    @Test
    void completedOrderCannotBeCancelled() {
        order.complete();
        assertThatThrownBy(() -> order.cancel("late"))
                .isInstanceOf(NonRetryableSagaException.class)
                .hasMessageContaining("Cannot cancel");
    }

    @Test
    void cancelledOrderCannotBeCompleted() {
        order.cancel("declined");
        assertThatThrownBy(order::complete)
                .isInstanceOf(NonRetryableSagaException.class)
                .hasMessageContaining("Cannot complete");
    }
}
