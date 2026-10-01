package com.example.saga.common;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class SagaPropertiesTest {

    @Test
    void unsetPropertiesFallBackToSafeDefaults() {
        SagaProperties properties = new SagaProperties(null, null);

        assertThat(properties.kafka().topics().create()).isFalse();
        assertThat(properties.kafka().topics().partitions()).isEqualTo(3);
        assertThat(properties.kafka().topics().replicas()).isEqualTo((short) 1);
        assertThat(properties.kafka().retry().maxRetries()).isEqualTo(4);
        assertThat(properties.kafka().retry().initialInterval()).isEqualTo(Duration.ofMillis(500));
        assertThat(properties.kafka().retry().maxInterval()).isEqualTo(Duration.ofSeconds(10));
        assertThat(properties.outbox().batchSize()).isEqualTo(100);
        assertThat(properties.outbox().sendTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(properties.outbox().retention()).isEqualTo(Duration.ofDays(7));
    }

    @Test
    void explicitValuesAreKept() {
        var properties = new SagaProperties(
                new SagaProperties.Kafka(new SagaProperties.Topics(true, 6, (short) 3),
                        new SagaProperties.Retry(9, Duration.ofSeconds(1), Duration.ofMinutes(1))),
                new SagaProperties.Outbox(10, Duration.ofSeconds(2), Duration.ofDays(1)));

        assertThat(properties.kafka().topics()).isEqualTo(new SagaProperties.Topics(true, 6, (short) 3));
        assertThat(properties.kafka().retry().maxRetries()).isEqualTo(9);
        assertThat(properties.outbox().batchSize()).isEqualTo(10);
    }
}
