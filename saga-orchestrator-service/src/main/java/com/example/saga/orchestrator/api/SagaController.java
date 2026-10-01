package com.example.saga.orchestrator.api;

import com.example.saga.orchestrator.domain.SagaInstance;
import com.example.saga.orchestrator.domain.SagaRepository;
import com.example.saga.orchestrator.domain.SagaStatus;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/sagas")
public class SagaController {

    private final SagaRepository sagas;

    public SagaController(SagaRepository sagas) {
        this.sagas = sagas;
    }

    @GetMapping
    public List<SagaResponse> list(@RequestParam(defaultValue = "50") int limit) {
        return sagas.findAllByOrderByCreatedAtDesc(PageRequest.ofSize(Math.min(limit, 500)))
                .stream().map(SagaResponse::from).toList();
    }

    @GetMapping("/{sagaId}")
    public ResponseEntity<SagaResponse> get(@PathVariable UUID sagaId) {
        return ResponseEntity.of(sagas.findById(sagaId).map(SagaResponse::from));
    }

    public record SagaResponse(UUID sagaId, UUID orderId, String productId, int quantity, BigDecimal amount,
                               SagaStatus status, UUID paymentId, UUID reservationId, String failureReason,
                               int stepTimeouts, Instant createdAt, Instant updatedAt) {
        static SagaResponse from(SagaInstance s) {
            return new SagaResponse(s.getSagaId(), s.getOrderId(), s.getProductId(), s.getQuantity(), s.getAmount(),
                    s.getStatus(), s.getPaymentId(), s.getReservationId(), s.getFailureReason(),
                    s.getStepTimeouts(), s.getCreatedAt(), s.getUpdatedAt());
        }
    }
}
