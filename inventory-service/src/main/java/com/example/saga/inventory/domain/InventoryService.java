package com.example.saga.inventory.domain;

import com.example.saga.common.outbox.OutboxWriter;
import com.example.saga.inventory.InventoryProperties;
import com.example.saga.messages.SagaTopics;
import com.example.saga.messages.commands.ReserveInventoryCommand;
import com.example.saga.messages.events.InventoryFailedEvent;
import com.example.saga.messages.events.InventoryReservedEvent;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InventoryService {

    private final ReservationRepository reservations;
    private final StockRepository stock;
    private final OutboxWriter outbox;
    private final InventoryProperties properties;

    public InventoryService(ReservationRepository reservations, StockRepository stock,
                            OutboxWriter outbox, InventoryProperties properties) {
        this.reservations = reservations;
        this.stock = stock;
        this.outbox = outbox;
        this.properties = properties;
    }

    /** One reservation per saga: a repeated command replays the recorded outcome. */
    @Transactional
    public void reserve(ReserveInventoryCommand command) {
        Reservation reservation = reservations.findBySagaId(command.sagaId())
                .orElseGet(() -> reservations.save(tryReserve(command)));

        switch (reservation.getStatus()) {
            case RESERVED -> outbox.send(SagaTopics.INVENTORY_RESERVED,
                    new InventoryReservedEvent(command.sagaId(), command.orderId(), reservation.getId()));
            case FAILED -> outbox.send(SagaTopics.INVENTORY_FAILED,
                    new InventoryFailedEvent(command.sagaId(), command.orderId(), reservation.getReason()));
        }
    }

    private Reservation tryReserve(ReserveInventoryCommand c) {
        if (c.quantity() > properties.maxQuantityPerOrder()) {
            return Reservation.failed(c.sagaId(), c.orderId(), c.productId(), c.quantity(),
                    "Quantity " + c.quantity() + " exceeds per-order limit " + properties.maxQuantityPerOrder());
        }
        if (!stock.tryDecrement(c.productId(), c.quantity())) {
            return Reservation.failed(c.sagaId(), c.orderId(), c.productId(), c.quantity(),
                    "Insufficient stock for " + c.productId());
        }
        return Reservation.reserved(c.sagaId(), c.orderId(), c.productId(), c.quantity());
    }
}
