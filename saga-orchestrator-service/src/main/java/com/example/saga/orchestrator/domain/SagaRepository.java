package com.example.saga.orchestrator.domain;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface SagaRepository extends JpaRepository<SagaInstance, UUID> {

    List<SagaInstance> findAllByOrderByCreatedAtDesc(Pageable pageable);

    @Query("select s.sagaId from SagaInstance s where s.status not in :terminal and s.updatedAt < :before order by s.updatedAt")
    List<UUID> findStuck(Collection<SagaStatus> terminal, Instant before, Pageable pageable);
}
