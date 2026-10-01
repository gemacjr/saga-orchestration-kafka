package com.example.saga.orchestrator.domain;

import com.example.saga.orchestrator.OrchestratorProperties;
import org.junit.jupiter.api.Test;
import org.springframework.orm.ObjectOptimisticLockingFailureException;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SagaTimeoutWatcherTest {

    private final SagaRepository sagas = mock(SagaRepository.class);
    private final SagaOrchestrator orchestrator = mock(SagaOrchestrator.class);
    private final SagaTimeoutWatcher watcher =
            new SagaTimeoutWatcher(sagas, orchestrator, new OrchestratorProperties(Duration.ofSeconds(30), 3));

    @Test
    void oneFailingSagaDoesNotStopTheOthers() {
        UUID conflicting = UUID.randomUUID();
        UUID broken = UUID.randomUUID();
        UUID healthy = UUID.randomUUID();
        when(sagas.findStuck(any(), any(), any())).thenReturn(List.of(conflicting, broken, healthy));
        doThrow(new ObjectOptimisticLockingFailureException(SagaInstance.class, conflicting))
                .when(orchestrator).handleTimeout(conflicting);
        doThrow(new IllegalStateException("db hiccup")).when(orchestrator).handleTimeout(broken);

        watcher.scan();

        verify(orchestrator).handleTimeout(healthy);
    }
}
