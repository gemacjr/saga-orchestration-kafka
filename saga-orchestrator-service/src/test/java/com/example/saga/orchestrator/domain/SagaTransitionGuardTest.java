package com.example.saga.orchestrator.domain;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Exhaustive guard check: every transition is accepted only from its source state(s), never from any other. */
class SagaTransitionGuardTest {

    /** Event name -> (transition, states it is allowed from). */
    private static final Map<String, Map.Entry<Predicate<SagaInstance>, EnumSet<SagaStatus>>> TRANSITIONS = Map.of(
            "paymentCompleted", Map.entry(s -> s.paymentCompleted(UUID.randomUUID()), EnumSet.of(SagaStatus.PAYMENT_PROCESSING)),
            "paymentFailed", Map.entry(s -> s.paymentFailed("x"), EnumSet.of(SagaStatus.PAYMENT_PROCESSING)),
            "inventoryReserved", Map.entry(s -> s.inventoryReserved(UUID.randomUUID()), EnumSet.of(SagaStatus.INVENTORY_RESERVING)),
            "inventoryFailed", Map.entry(s -> s.inventoryFailed("x"), EnumSet.of(SagaStatus.INVENTORY_RESERVING)),
            "paymentRefunded", Map.entry(SagaInstance::paymentRefunded, EnumSet.of(SagaStatus.COMPENSATING)),
            "orderCompleted", Map.entry(SagaInstance::orderCompleted, EnumSet.of(SagaStatus.ORDER_COMPLETING)),
            "orderCancelled", Map.entry(SagaInstance::orderCancelled, EnumSet.of(SagaStatus.ORDER_CANCELLING)),
            "startCompensation", Map.entry(s -> s.startCompensation("x"),
                    EnumSet.of(SagaStatus.PAYMENT_PROCESSING, SagaStatus.INVENTORY_RESERVING)));

    /** A path of real transitions that leads to each status. */
    private static final Map<SagaStatus, Consumer<SagaInstance>> REACH = Map.of(
            SagaStatus.PAYMENT_PROCESSING, s -> { },
            SagaStatus.INVENTORY_RESERVING, s -> s.paymentCompleted(UUID.randomUUID()),
            SagaStatus.ORDER_COMPLETING, s -> { s.paymentCompleted(UUID.randomUUID()); s.inventoryReserved(UUID.randomUUID()); },
            SagaStatus.COMPLETED, s -> { s.paymentCompleted(UUID.randomUUID()); s.inventoryReserved(UUID.randomUUID()); s.orderCompleted(); },
            SagaStatus.COMPENSATING, s -> { s.paymentCompleted(UUID.randomUUID()); s.inventoryFailed("x"); },
            SagaStatus.ORDER_CANCELLING, s -> s.paymentFailed("x"),
            SagaStatus.FAILED, s -> { s.paymentFailed("x"); s.orderCancelled(); },
            SagaStatus.COMPENSATED, s -> { s.paymentCompleted(UUID.randomUUID()); s.inventoryFailed("x"); s.paymentRefunded(); s.orderCancelled(); });

    static Stream<Arguments> everyTransitionFromEveryState() {
        return TRANSITIONS.keySet().stream().flatMap(event ->
                Arrays.stream(SagaStatus.values()).map(status -> Arguments.of(event, status)));
    }

    @ParameterizedTest(name = "{0} from {1}")
    @MethodSource("everyTransitionFromEveryState")
    void transitionIsGuardedByCurrentState(String event, SagaStatus from) {
        SagaInstance saga = SagaInstance.start(UUID.randomUUID(), UUID.randomUUID(), "p", 1, BigDecimal.ONE);
        REACH.get(from).accept(saga);
        assertThat(saga.getStatus()).isEqualTo(from);

        var transition = TRANSITIONS.get(event);
        boolean allowed = transition.getValue().contains(from);

        assertThat(transition.getKey().test(saga)).isEqualTo(allowed);
        if (!allowed) {
            assertThat(saga.getStatus()).as("rejected transition must not change state").isEqualTo(from);
        } else {
            assertThat(saga.getStatus()).isNotEqualTo(from);
        }
    }
}
