package com.example.saga.testsupport;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/** Postgres + Kafka wired into Spring Boot via @ServiceConnection; shared through the test context cache. */
@TestConfiguration(proxyBeanMethods = false)
public class SagaTestContainers {

    public static final String POSTGRES_IMAGE = "postgres:17-alpine";
    public static final String KAFKA_IMAGE = "apache/kafka:3.9.1";

    @Bean
    @ServiceConnection
    PostgreSQLContainer<?> postgres() {
        return new PostgreSQLContainer<>(POSTGRES_IMAGE);
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        return new KafkaContainer(KAFKA_IMAGE);
    }

    @Bean
    KafkaTestClient kafkaTestClient(KafkaContainer kafka) {
        return new KafkaTestClient(kafka.getBootstrapServers());
    }
}
