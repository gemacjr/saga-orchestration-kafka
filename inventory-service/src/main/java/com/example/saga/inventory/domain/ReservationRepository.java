package com.example.saga.inventory.domain;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ReservationRepository extends JpaRepository<Reservation, UUID> {

    Optional<Reservation> findBySagaId(UUID sagaId);

    List<Reservation> findAllByOrderByCreatedAtDesc(Pageable pageable);
}
