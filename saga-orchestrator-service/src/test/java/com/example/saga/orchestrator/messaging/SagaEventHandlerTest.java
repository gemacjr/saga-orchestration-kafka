package com.example.saga.orchestrator.messaging;

import com.example.saga.common.inbox.InboxGuard;
import com.example.saga.messages.events.*;
import com.example.saga.orchestrator.domain.SagaOrchestrator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/** Every handler must consult the inbox: a redelivered message never reaches the orchestrator. */
class SagaEventHandlerTest {

    private final InboxGuard inbox = mock(InboxGuard.class);
    private final SagaOrchestrator orchestrator = mock(SagaOrchestrator.class);
    private final SagaEventHandler handler = new SagaEventHandler(inbox, orchestrator);
    private final UUID s = UUID.randomUUID();
    private final UUID o = UUID.randomUUID();

    @Test
    void redeliveredEventsAreDroppedByEveryHandler() {
        when(inbox.firstDelivery(anyString())).thenReturn(false);

        handler.on(new OrderCreatedEvent(s, o, "p", 1, BigDecimal.ONE), "m1");
        handler.on(new PaymentCompletedEvent(s, o, UUID.randomUUID()), "m2");
        handler.on(new PaymentFailedEvent(s, o, "r"), "m3");
        handler.on(new InventoryReservedEvent(s, o, UUID.randomUUID()), "m4");
        handler.on(new InventoryFailedEvent(s, o, "r"), "m5");
        handler.on(new PaymentRefundedEvent(s, o, UUID.randomUUID()), "m6");
        handler.on(new OrderCompletedEvent(s, o), "m7");
        handler.on(new OrderCancelledEvent(s, o, "r"), "m8");

        verify(inbox, times(8)).firstDelivery(anyString());
        verifyNoInteractions(orchestrator);
    }
}
