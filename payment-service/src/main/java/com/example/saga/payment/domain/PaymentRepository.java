package com.example.saga.payment.domain;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    Optional<Payment> findBySagaId(UUID sagaId);

    List<Payment> findAllByOrderByCreatedAtDesc(Pageable pageable);
}
