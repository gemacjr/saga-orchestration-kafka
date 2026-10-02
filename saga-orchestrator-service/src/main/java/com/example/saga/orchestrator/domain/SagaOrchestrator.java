package com.example.saga.orchestrator.domain;

import com.example.saga.common.NonRetryableSagaException;
import com.example.saga.common.outbox.OutboxWriter;
import com.example.saga.messages.SagaTopics;
import com.example.saga.messages.commands.*;
import com.example.saga.messages.events.OrderCreatedEvent;
import com.example.saga.orchestrator.OrchestratorProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;
import java.util.function.Predicate;

/**
 * Workflow logic only: which command follows which outcome. Business rules (limits, stock) stay
 * inside the participants. Callers provide the transaction (listener or timeout watcher).
 */
@Service
public class SagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

    private final SagaRepository sagas;
    private final OutboxWriter outbox;
    private final OrchestratorProperties properties;
    private final MeterRegistry meterRegistry;

    public SagaOrchestrator(SagaRepository sagas, OutboxWriter outbox, OrchestratorProperties properties,
                            MeterRegistry meterRegistry) {
        this.sagas = sagas;
        this.outbox = outbox;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
    }

    @Transactional
    public void start(OrderCreatedEvent event) {
        if (sagas.existsById(event.sagaId())) {
            log.info("Saga {} already started", event.sagaId());
            return;
        }
        SagaInstance saga = sagas.save(SagaInstance.start(
                event.sagaId(), event.orderId(), event.productId(), event.quantity(), event.amount()));
        dispatch(saga);
    }

    /** Applies a guarded transition; only a saga that actually advanced issues its next command. */
    @Transactional
    public void advance(UUID sagaId, String event, Predicate<SagaInstance> transition) {
        SagaInstance saga = sagas.findById(sagaId)
                .orElseThrow(() -> new NonRetryableSagaException("Unknown saga " + sagaId));
        SagaStatus before = saga.getStatus();
        if (!transition.test(saga)) {
            log.info("Saga {} ignored {} in status {} (duplicate or late)", sagaId, event, before);
            return;
        }
        log.info("Saga {} {} -> {} on {}", sagaId, before, saga.getStatus(), event);
        dispatch(saga);
    }

    /** Called by {@link SagaTimeoutWatcher} for a saga whose current step has not answered in time. */
    @Transactional
    public void handleTimeout(UUID sagaId) {
        SagaInstance saga = sagas.findById(sagaId).orElseThrow();
        if (saga.getStatus().isTerminal()) {
            return;
        }
        saga.recordStepTimeout();
        onStepTimeout(saga);
    }

    /**
     * Policy for a step that timed out. {@code saga.getStepTimeouts()} counts the timeouts of the current step.
     * Tools available: {@code dispatch(saga)} re-sends the current step's command (all participants are idempotent),
     * {@code saga.startCompensation(reason)} rolls back (only legal in PAYMENT_PROCESSING / INVENTORY_RESERVING),
     * {@code properties.maxStepRetries()} is the configured retry budget.
     */
    void onStepTimeout(SagaInstance saga) {
        if (saga.getStepTimeouts() <= properties.maxStepRetries()) {
            log.warn("Saga {} timed out in {}, re-sending command (attempt {}/{})",
                    saga.getSagaId(), saga.getStatus(), saga.getStepTimeouts(), properties.maxStepRetries());
            dispatch(saga);
            return;
        }
        SagaStatus stuckIn = saga.getStatus();
        if (saga.startCompensation("Timed out in " + stuckIn)) {
            log.warn("Saga {} exhausted retries in {}, compensating", saga.getSagaId(), stuckIn);
            dispatch(saga);
            return;
        }
        // Past the point of no return (order completion or compensation in flight): these steps must
        // eventually succeed, so keep re-sending and alert an operator.
        log.error("Saga {} stuck in {} after {} timeouts and cannot be compensated; re-sending, needs attention",
                saga.getSagaId(), stuckIn, saga.getStepTimeouts());
        meterRegistry.counter("saga.stuck", "status", stuckIn.name()).increment();
        dispatch(saga);
    }

    /** The current state fully determines the next command. Terminal states only record the outcome. */
    void dispatch(SagaInstance saga) {
        UUID id = saga.getSagaId();
        switch (saga.getStatus()) {
            case PAYMENT_PROCESSING -> outbox.send(SagaTopics.PROCESS_PAYMENT,
                    new ProcessPaymentCommand(id, saga.getOrderId(), saga.getAmount()));
            case INVENTORY_RESERVING -> outbox.send(SagaTopics.RESERVE_INVENTORY,
                    new ReserveInventoryCommand(id, saga.getOrderId(), saga.getProductId(), saga.getQuantity()));
            case ORDER_COMPLETING -> outbox.send(SagaTopics.COMPLETE_ORDER,
                    new CompleteOrderCommand(id, saga.getOrderId()));
            case COMPENSATING -> outbox.send(SagaTopics.REFUND_PAYMENT,
                    new RefundPaymentCommand(id, saga.getOrderId(), saga.getPaymentId(),
                            "Compensation: " + saga.getFailureReason()));
            case ORDER_CANCELLING -> outbox.send(SagaTopics.CANCEL_ORDER,
                    new CancelOrderCommand(id, saga.getOrderId(), saga.getFailureReason()));
            case COMPLETED, COMPENSATED, FAILED ->
                    meterRegistry.counter("saga.finished", "outcome", saga.getStatus().name()).increment();
        }
    }
}
