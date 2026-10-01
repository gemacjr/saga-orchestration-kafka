package com.example.saga.orchestrator.domain;

public enum SagaStatus {
    PAYMENT_PROCESSING,
    INVENTORY_RESERVING,
    ORDER_COMPLETING,
    COMPENSATING,
    ORDER_CANCELLING,
    COMPLETED,
    COMPENSATED,
    FAILED;

    public static final java.util.Set<SagaStatus> TERMINAL = java.util.EnumSet.of(COMPLETED, COMPENSATED, FAILED);

    public boolean isTerminal() {
        return TERMINAL.contains(this);
    }
}
