package com.example.saga.messages;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Every message on the wire must survive a JSON round trip unchanged and expose its saga id. */
class MessageJsonContractTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    static Stream<Class<? extends SagaMessage>> messageTypes() {
        return MessageTypes.all().values().stream();
    }

    @ParameterizedTest
    @MethodSource("messageTypes")
    void roundTripsThroughJson(Class<? extends SagaMessage> type) throws Exception {
        SagaMessage message = sample(type);

        SagaMessage copy = JSON.readValue(JSON.writeValueAsString(message), type);

        assertThat(copy).isEqualTo(message).hasSameHashCodeAs(message);
        assertThat(copy.sagaId()).isNotNull();
        assertThat(copy.toString()).startsWith(type.getSimpleName());
    }

    private static SagaMessage sample(Class<? extends SagaMessage> type) throws Exception {
        RecordComponent[] components = type.getRecordComponents();
        Object[] args = Arrays.stream(components).map(c -> sampleValue(c.getType())).toArray();
        Class<?>[] types = Arrays.stream(components).map(RecordComponent::getType).toArray(Class<?>[]::new);
        return type.getDeclaredConstructor(types).newInstance(args);
    }

    private static Object sampleValue(Class<?> type) {
        if (type == UUID.class) return UUID.randomUUID();
        if (type == String.class) return "value";
        if (type == int.class) return 3;
        if (type == BigDecimal.class) return new BigDecimal("123.45");
        throw new IllegalArgumentException("Add a sample for " + type);
    }
}
