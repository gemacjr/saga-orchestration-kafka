package com.example.saga.common;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "saga")
public record SagaProperties(Kafka kafka, Outbox outbox) {

    public SagaProperties {
        kafka = kafka != null ? kafka : new Kafka(null, null);
        outbox = outbox != null ? outbox : new Outbox(0, null, null);
    }

    public record Kafka(Topics topics, Retry retry) {
        public Kafka {
            topics = topics != null ? topics : new Topics(false, 0, (short) 0);
            retry = retry != null ? retry : new Retry(0, null, null);
        }
    }

    /** Topic auto-creation is a local convenience; in prod topics are owned by infrastructure-as-code. */
    public record Topics(boolean create, int partitions, short replicas) {
        public Topics {
            partitions = partitions > 0 ? partitions : 3;
            replicas = replicas > 0 ? replicas : 1;
        }
    }

    public record Retry(int maxRetries, Duration initialInterval, Duration maxInterval) {
        public Retry {
            maxRetries = maxRetries > 0 ? maxRetries : 4;
            initialInterval = initialInterval != null ? initialInterval : Duration.ofMillis(500);
            maxInterval = maxInterval != null ? maxInterval : Duration.ofSeconds(10);
        }
    }

    public record Outbox(int batchSize, Duration sendTimeout, Duration retention) {
        public Outbox {
            batchSize = batchSize > 0 ? batchSize : 100;
            sendTimeout = sendTimeout != null ? sendTimeout : Duration.ofSeconds(10);
            retention = retention != null ? retention : Duration.ofDays(7);
        }
    }
}
