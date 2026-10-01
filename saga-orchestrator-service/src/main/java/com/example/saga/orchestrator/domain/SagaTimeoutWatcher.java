package com.example.saga.orchestrator.domain;

import com.example.saga.orchestrator.OrchestratorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/** Finds sagas whose current step has not progressed within the step timeout. */
@Component
public class SagaTimeoutWatcher {

    private static final Logger log = LoggerFactory.getLogger(SagaTimeoutWatcher.class);

    private final SagaRepository sagas;
    private final SagaOrchestrator orchestrator;
    private final OrchestratorProperties properties;

    public SagaTimeoutWatcher(SagaRepository sagas, SagaOrchestrator orchestrator, OrchestratorProperties properties) {
        this.sagas = sagas;
        this.orchestrator = orchestrator;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${saga.orchestrator.timeout-scan-interval-ms:5000}")
    public void scan() {
        Instant before = Instant.now().minus(properties.stepTimeout());
        for (UUID sagaId : sagas.findStuck(SagaStatus.TERMINAL, before, PageRequest.ofSize(100))) {
            try {
                orchestrator.handleTimeout(sagaId); // own transaction per saga
            } catch (ObjectOptimisticLockingFailureException e) {
                log.debug("Saga {} advanced concurrently, skipping timeout", sagaId);
            } catch (RuntimeException e) {
                log.error("Timeout handling failed for saga {}", sagaId, e);
            }
        }
    }
}
