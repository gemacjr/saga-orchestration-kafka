package com.example.saga.messages;

import com.example.saga.messages.events.OrderCreatedEvent;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageTypesTest {

    @Test
    void everyRegisteredTypeRoundTripsThroughItsLogicalName() {
        MessageTypes.all().forEach((name, type) -> assertThat(MessageTypes.nameOf(type)).isEqualTo(name));
    }

    @Test
    void logicalNamesAreVersionedAndUnique() {
        assertThat(MessageTypes.all().keySet()).allMatch(name -> name.matches("[A-Z][A-Za-z]+\\.v\\d+"));
        assertThat(new HashSet<>(MessageTypes.all().values())).hasSameSizeAs(MessageTypes.all().values());
    }

    @Test
    void everySagaMessageRecordIsRegistered() {
        assertThat(MessageTypes.all()).hasSize(13);
        assertThat(MessageTypes.nameOf(OrderCreatedEvent.class)).isEqualTo("OrderCreated.v1");
    }

    @Test
    void unregisteredTypeIsRejected() {
        record Rogue(UUID sagaId) implements SagaMessage {
        }
        assertThatThrownBy(() -> MessageTypes.nameOf(Rogue.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unregistered");
    }

    @Test
    void topicsAreDistinctOneTopicPerMessageType() {
        List<String> topics = SagaTopics.ALL;
        assertThat(new HashSet<>(topics)).hasSize(topics.size());
        assertThat(topics).hasSameSizeAs(MessageTypes.all().keySet()).allMatch(t -> t.startsWith("saga."));
    }
}
