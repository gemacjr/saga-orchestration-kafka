package com.example.saga.payment.api;

import com.example.saga.payment.domain.Payment;
import com.example.saga.payment.domain.PaymentRepository;
import com.example.saga.payment.domain.PaymentStatus;
import org.springframework.data.domain.PageRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final PaymentRepository payments;

    public PaymentController(PaymentRepository payments) {
        this.payments = payments;
    }

    @GetMapping
    public List<PaymentResponse> list(@RequestParam(defaultValue = "50") int limit) {
        return payments.findAllByOrderByCreatedAtDesc(PageRequest.ofSize(Math.min(limit, 500)))
                .stream().map(PaymentResponse::from).toList();
    }

    public record PaymentResponse(UUID paymentId, UUID sagaId, UUID orderId, BigDecimal amount,
                                  PaymentStatus status, String reason, Instant createdAt, Instant updatedAt) {
        static PaymentResponse from(Payment p) {
            return new PaymentResponse(p.getId(), p.getSagaId(), p.getOrderId(), p.getAmount(),
                    p.getStatus(), p.getReason(), p.getCreatedAt(), p.getUpdatedAt());
        }
    }
}
