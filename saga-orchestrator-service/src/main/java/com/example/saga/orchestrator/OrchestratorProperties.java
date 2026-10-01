package com.example.saga.orchestrator;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/** Bound from SSM Parameter Store: /saga/saga-orchestrator-service/saga.orchestrator.* */
@Validated
@ConfigurationProperties(prefix = "saga.orchestrator")
public record OrchestratorProperties(@NotNull Duration stepTimeout, @Positive int maxStepRetries) {
}
