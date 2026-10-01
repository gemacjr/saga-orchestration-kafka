package com.example.saga.inventory;

import com.example.saga.inventory.domain.InventoryService;
import com.example.saga.inventory.domain.Reservation;
import com.example.saga.inventory.domain.ReservationRepository;
import com.example.saga.inventory.domain.ReservationStatus;
import com.example.saga.messages.SagaTopics;
import com.example.saga.messages.commands.ReserveInventoryCommand;
import com.example.saga.messages.events.InventoryFailedEvent;
import com.example.saga.messages.events.InventoryReservedEvent;
import com.example.saga.testsupport.KafkaTestClient;
import com.example.saga.testsupport.SagaIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SagaIntegrationTest
@AutoConfigureMockMvc
class InventoryServiceIntegrationTest {

    @Autowired KafkaTestClient kafka;
    @Autowired ReservationRepository reservations;
    @Autowired InventoryService inventoryService;
    @Autowired JdbcTemplate jdbc;
    @Autowired MockMvc mvc;

    private final UUID sagaId = UUID.randomUUID();
    private final UUID orderId = UUID.randomUUID();

    private String newProduct(int stock) {
        String productId = "p-" + UUID.randomUUID();
        jdbc.update("insert into product_stock (product_id, available) values (?, ?)", productId, stock);
        return productId;
    }

    private int stockOf(String productId) {
        return jdbc.queryForObject("select available from product_stock where product_id = ?", Integer.class, productId);
    }

    private Reservation awaitReservation() {
        return await().until(() -> reservations.findBySagaId(sagaId).orElse(null), r -> r != null);
    }

    @Test
    void reservesStockAndReplies() {
        String product = newProduct(10);
        kafka.send(SagaTopics.RESERVE_INVENTORY, new ReserveInventoryCommand(sagaId, orderId, product, 5)); // limit itself

        InventoryReservedEvent event = kafka.expectOne(SagaTopics.INVENTORY_RESERVED, sagaId, InventoryReservedEvent.class);
        Reservation reservation = awaitReservation();
        assertThat(event.reservationId()).isEqualTo(reservation.getId());
        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.RESERVED);
        assertThat(stockOf(product)).isEqualTo(5);
    }

    @Test
    void quantityAbovePerOrderLimitFailsWithoutTouchingStock() {
        String product = newProduct(100);
        kafka.send(SagaTopics.RESERVE_INVENTORY, new ReserveInventoryCommand(sagaId, orderId, product, 6));

        assertThat(kafka.expectOne(SagaTopics.INVENTORY_FAILED, sagaId, InventoryFailedEvent.class).reason())
                .contains("exceeds per-order limit 5");
        assertThat(awaitReservation().getStatus()).isEqualTo(ReservationStatus.FAILED);
        assertThat(stockOf(product)).isEqualTo(100);
    }

    @Test
    void insufficientStockFails() {
        String product = newProduct(1);
        kafka.send(SagaTopics.RESERVE_INVENTORY, new ReserveInventoryCommand(sagaId, orderId, product, 2));

        assertThat(kafka.expectOne(SagaTopics.INVENTORY_FAILED, sagaId, InventoryFailedEvent.class).reason())
                .contains("Insufficient stock");
        assertThat(stockOf(product)).isEqualTo(1);
    }

    @Test
    void unknownProductFails() {
        kafka.send(SagaTopics.RESERVE_INVENTORY, new ReserveInventoryCommand(sagaId, orderId, "no-such-product", 1));
        kafka.expectOne(SagaTopics.INVENTORY_FAILED, sagaId, InventoryFailedEvent.class);
    }

    @Test
    void resentCommandReservesOnceAndReplaysTheOutcome() {
        String product = newProduct(10);
        var command = new ReserveInventoryCommand(sagaId, orderId, product, 3);
        String messageId = kafka.send(SagaTopics.RESERVE_INVENTORY, command);
        kafka.send(SagaTopics.RESERVE_INVENTORY, command, messageId); // redelivery: dropped by inbox
        kafka.send(SagaTopics.RESERVE_INVENTORY, command);            // re-send: replayed

        await().untilAsserted(() ->
                assertThat(kafka.recordsFor(SagaTopics.INVENTORY_RESERVED, sagaId, Duration.ofMillis(500))).hasSize(2));
        assertThat(stockOf(product)).isEqualTo(7);
    }

    @Test
    void concurrentReservationsNeverOversell() throws Exception {
        String product = newProduct(10);
        List<UUID> sagas = IntStream.range(0, 25).mapToObj(i -> UUID.randomUUID()).toList();

        var pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> futures = sagas.stream()
                    .map(s -> pool.submit(() -> inventoryService.reserve(new ReserveInventoryCommand(s, UUID.randomUUID(), product, 1))))
                    .<Future<?>>map(f -> f)
                    .toList();
            for (Future<?> f : futures) {
                f.get();
            }
        } finally {
            pool.shutdownNow();
        }

        var outcomes = sagas.stream().map(s -> reservations.findBySagaId(s).orElseThrow().getStatus()).toList();
        assertThat(outcomes).filteredOn(ReservationStatus.RESERVED::equals).hasSize(10);
        assertThat(outcomes).filteredOn(ReservationStatus.FAILED::equals).hasSize(15);
        assertThat(stockOf(product)).isZero();
    }

    @Test
    void reservationsAreListed() throws Exception {
        String product = newProduct(10);
        kafka.send(SagaTopics.RESERVE_INVENTORY, new ReserveInventoryCommand(sagaId, orderId, product, 1));
        Reservation reservation = awaitReservation();

        mvc.perform(get("/reservations").param("limit", "1000"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.reservationId == '" + reservation.getId() + "')].productId").value(product));
    }
}
