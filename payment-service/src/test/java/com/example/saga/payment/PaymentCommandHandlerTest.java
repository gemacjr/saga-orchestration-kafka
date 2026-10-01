package com.example.saga.payment;

import com.example.saga.common.inbox.InboxGuard;
import com.example.saga.messages.commands.ProcessPaymentCommand;
import com.example.saga.messages.commands.RefundPaymentCommand;
import com.example.saga.payment.domain.PaymentService;
import com.example.saga.payment.messaging.PaymentCommandHandler;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class PaymentCommandHandlerTest {

    @Test
    void redeliveredCommandsAreDroppedByEveryHandler() {
        InboxGuard inbox = mock(InboxGuard.class);
        PaymentService payments = mock(PaymentService.class);
        when(inbox.firstDelivery(anyString())).thenReturn(false);
        var handler = new PaymentCommandHandler(inbox, payments);

        handler.on(new ProcessPaymentCommand(UUID.randomUUID(), UUID.randomUUID(), BigDecimal.ONE), "m1");
        handler.on(new RefundPaymentCommand(UUID.randomUUID(), UUID.randomUUID(), null, "r"), "m2");

        verifyNoInteractions(payments);
    }
}
