package com.example.saga.inventory.api;

import com.example.saga.inventory.domain.Reservation;
import com.example.saga.inventory.domain.ReservationRepository;
import com.example.saga.inventory.domain.ReservationStatus;
import org.springframework.data.domain.PageRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/reservations")
public class ReservationController {

    private final ReservationRepository reservations;

    public ReservationController(ReservationRepository reservations) {
        this.reservations = reservations;
    }

    @GetMapping
    public List<ReservationResponse> list(@RequestParam(defaultValue = "50") int limit) {
        return reservations.findAllByOrderByCreatedAtDesc(PageRequest.ofSize(Math.min(limit, 500)))
                .stream().map(ReservationResponse::from).toList();
    }

    public record ReservationResponse(UUID reservationId, UUID sagaId, UUID orderId, String productId, int quantity,
                                      ReservationStatus status, String reason, Instant createdAt) {
        static ReservationResponse from(Reservation r) {
            return new ReservationResponse(r.getId(), r.getSagaId(), r.getOrderId(), r.getProductId(),
                    r.getQuantity(), r.getStatus(), r.getReason(), r.getCreatedAt());
        }
    }
}
