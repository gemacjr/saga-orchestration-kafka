package com.example.saga.inventory.messaging;

import com.example.saga.common.inbox.InboxGuard;
import com.example.saga.inventory.domain.InventoryService;
import com.example.saga.messages.SagaHeaders;
import com.example.saga.messages.SagaTopics;
import com.example.saga.messages.commands.ReserveInventoryCommand;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class InventoryCommandHandler {

    private final InboxGuard inbox;
    private final InventoryService inventoryService;

    public InventoryCommandHandler(InboxGuard inbox, InventoryService inventoryService) {
        this.inbox = inbox;
        this.inventoryService = inventoryService;
    }

    @KafkaListener(topics = SagaTopics.RESERVE_INVENTORY)
    @Transactional
    public void on(ReserveInventoryCommand command, @Header(SagaHeaders.MESSAGE_ID) String messageId) {
        if (inbox.firstDelivery(messageId)) {
            inventoryService.reserve(command);
        }
    }
}
