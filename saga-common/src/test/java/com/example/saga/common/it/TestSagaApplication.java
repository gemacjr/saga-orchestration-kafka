package com.example.saga.common.it;

import com.example.saga.common.NonRetryableSagaException;
import com.example.saga.messages.SagaHeaders;
import com.example.saga.messages.SagaTopics;
import com.example.saga.messages.events.OrderCreatedEvent;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Minimal app that consumes with the saga-common Kafka setup, to exercise retries and dead-lettering. */
@SpringBootApplication
public class TestSagaApplication {

    @Component
    public static class ProbeListener {

        public final Map<String, Integer> attempts = new ConcurrentHashMap<>();
        public final Map<String, String> handled = new ConcurrentHashMap<>();

        @KafkaListener(topics = SagaTopics.ORDER_CREATED)
        public void on(OrderCreatedEvent event, @Header(SagaHeaders.MESSAGE_ID) String messageId) {
            int attempt = attempts.merge(event.productId(), 1, Integer::sum);
            switch (event.productId()) {
                case "non-retryable" -> throw new NonRetryableSagaException("invariant broken");
                case "always-failing" -> throw new IllegalStateException("still down");
                default -> {
                    if (event.productId().startsWith("flaky") && attempt < 2) {
                        throw new IllegalStateException("transient");
                    }
                    handled.put(event.productId(), messageId);
                }
            }
        }
    }
}
