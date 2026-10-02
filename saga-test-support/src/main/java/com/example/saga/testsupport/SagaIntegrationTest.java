package com.example.saga.testsupport;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.lang.annotation.*;

/** Full application context against real Postgres + Kafka containers, using the {@code test} profile. */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@SpringBootTest
@ActiveProfiles("test")
@Import(SagaTestContainers.class)
public @interface SagaIntegrationTest {
}
