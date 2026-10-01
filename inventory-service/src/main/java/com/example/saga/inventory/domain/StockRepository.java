package com.example.saga.inventory.domain;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class StockRepository {

    private final JdbcTemplate jdbcTemplate;

    public StockRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Check-and-decrement in a single statement: no read-modify-write race between concurrent
     * reservations, and no lock held across application code.
     */
    public boolean tryDecrement(String productId, int quantity) {
        return jdbcTemplate.update(
                "update product_stock set available = available - ? where product_id = ? and available >= ?",
                quantity, productId, quantity) == 1;
    }
}
