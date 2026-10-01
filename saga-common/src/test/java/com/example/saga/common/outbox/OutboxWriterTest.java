package com.example.saga.common.outbox;

import com.example.saga.messages.events.OrderCompletedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxWriterTest {

    @Test
    void serializationFailureIsSurfaced() throws Exception {
        ObjectMapper objectMapper = mock(ObjectMapper.class);
        when(objectMapper.writeValueAsString(any())).thenThrow(new JsonProcessingException("boom") { });

        var writer = new OutboxWriter(mock(JdbcTemplate.class), objectMapper);

        assertThatThrownBy(() -> writer.send("t", new OrderCompletedEvent(UUID.randomUUID(), UUID.randomUUID())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot serialize");
    }
}
