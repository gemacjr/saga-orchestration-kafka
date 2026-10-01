package com.example.saga.order;

import com.example.saga.common.inbox.InboxGuard;
import com.example.saga.messages.commands.CancelOrderCommand;
import com.example.saga.messages.commands.CompleteOrderCommand;
import com.example.saga.order.domain.OrderService;
import com.example.saga.order.messaging.OrderCommandHandler;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class OrderCommandHandlerTest {

    @Test
    void redeliveredCommandsAreDroppedByEveryHandler() {
        InboxGuard inbox = mock(InboxGuard.class);
        OrderService orders = mock(OrderService.class);
        when(inbox.firstDelivery(anyString())).thenReturn(false);
        var handler = new OrderCommandHandler(inbox, orders);

        handler.on(new CompleteOrderCommand(UUID.randomUUID(), UUID.randomUUID()), "m1");
        handler.on(new CancelOrderCommand(UUID.randomUUID(), UUID.randomUUID(), "r"), "m2");

        verifyNoInteractions(orders);
    }
}
