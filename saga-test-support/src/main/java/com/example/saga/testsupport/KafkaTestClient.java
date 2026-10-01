package com.example.saga.testsupport;

import com.example.saga.messages.MessageTypes;
import com.example.saga.messages.SagaHeaders;
import com.example.saga.messages.SagaMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Plays the other saga participants in integration tests: publishes messages exactly like OutboxRelay does
 * (message id + logical type headers) and reads what the service under test published.
 */
public class KafkaTestClient implements AutoCloseable {

    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final String bootstrapServers;
    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
    private final KafkaProducer<String, String> producer;

    public KafkaTestClient(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        this.producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
    }

    /** Sends with a fresh message id; returns the id so a test can redeliver the very same message. */
    public String send(String topic, SagaMessage message) {
        String messageId = UUID.randomUUID().toString();
        send(topic, message, messageId);
        return messageId;
    }

    public void send(String topic, SagaMessage message, String messageId) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic, message.sagaId().toString(), objectMapper.writeValueAsString(message));
            record.headers()
                    .add(SagaHeaders.MESSAGE_ID, messageId.getBytes(StandardCharsets.UTF_8))
                    .add(SagaHeaders.MESSAGE_TYPE, MessageTypes.nameOf(message.getClass()).getBytes(StandardCharsets.UTF_8));
            producer.send(record).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Sends a raw value with arbitrary headers (poison pills, missing headers...). */
    public void sendRaw(String topic, String key, String value, Map<String, String> headers) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        headers.forEach((k, v) -> record.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
        try {
            producer.send(record).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Waits for the single message with key {@code sagaId} on {@code topic} and maps it to {@code type}. */
    public <T> T expectOne(String topic, UUID sagaId, Class<T> type) {
        List<ConsumerRecord<String, String>> records = awaitRecords(topic, sagaId.toString(), 1, DEFAULT_TIMEOUT);
        return read(records.get(0), type);
    }

    /** All records currently on {@code topic} for {@code sagaId}, after waiting {@code settle} for stragglers. */
    public List<ConsumerRecord<String, String>> recordsFor(String topic, UUID sagaId, Duration settle) {
        return awaitRecords(topic, sagaId.toString(), Integer.MAX_VALUE, settle);
    }

    public List<ConsumerRecord<String, String>> awaitRecords(String topic, String key, int count, Duration timeout) {
        try (KafkaConsumer<String, String> consumer = newConsumer()) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic, Duration.ofSeconds(10)).stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            // Always read everything already on the topic, then keep waiting until the deadline for more
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            List<ConsumerRecord<String, String>> matches = new ArrayList<>();
            Instant deadline = Instant.now().plus(timeout);
            while (matches.size() < count && (Instant.now().isBefore(deadline) || !caughtUp(consumer, endOffsets))) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(200))) {
                    if (key.equals(r.key())) {
                        matches.add(r);
                    }
                }
            }
            if (count != Integer.MAX_VALUE && matches.size() < count) {
                throw new AssertionError("Expected " + count + " record(s) with key " + key + " on " + topic
                        + " within " + timeout + " but found " + matches.size());
            }
            return matches;
        }
    }

    private static boolean caughtUp(KafkaConsumer<?, ?> consumer, Map<TopicPartition, Long> endOffsets) {
        return endOffsets.entrySet().stream().allMatch(e -> consumer.position(e.getKey()) >= e.getValue());
    }

    public <T> T read(ConsumerRecord<String, String> record, Class<T> type) {
        try {
            return objectMapper.readValue(record.value(), type);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private KafkaConsumer<String, String> newConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<>(props);
    }

    @Override
    public void close() {
        producer.close();
    }
}
