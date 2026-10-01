package com.example.saga.common.kafka;

import com.example.saga.common.NonRetryableSagaException;
import com.example.saga.common.SagaProperties;
import com.example.saga.messages.MessageTypes;
import com.example.saga.messages.SagaTopics;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

@Configuration(proxyBeanMethods = false)
public class SagaKafkaConfiguration {

    /**
     * Values are deserialized by the logical type header ({@code OrderCreated.v1}), never by class name
     * from the wire. ErrorHandlingDeserializer turns poison pills into DLT records instead of an endless loop.
     */
    @Bean
    DefaultKafkaConsumerFactoryCustomizer sagaValueDeserializer(ObjectMapper objectMapper) {
        return factory -> {
            @SuppressWarnings({"unchecked", "rawtypes"})
            DefaultKafkaConsumerFactory<Object, Object> typed = (DefaultKafkaConsumerFactory) factory;
            typed.setValueDeserializerSupplier(() -> new ErrorHandlingDeserializer<>(sagaJsonDeserializer(objectMapper)));
        };
    }

    static JsonDeserializer<Object> sagaJsonDeserializer(ObjectMapper objectMapper) {
        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
        typeMapper.setIdClassMapping(new HashMap<>(MessageTypes.all()));
        typeMapper.addTrustedPackages("com.example.saga.messages.commands", "com.example.saga.messages.events");
        // A hand-built mapper defaults to INFERRED, which makes JsonDeserializer ignore the type header
        typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.TYPE_ID);

        JsonDeserializer<Object> json = new JsonDeserializer<>(objectMapper);
        json.setTypeMapper(typeMapper);
        return json;
    }

    /** Retries transient failures with exponential backoff, then parks the record on {@code <topic>-dlt}. */
    @Bean
    CommonErrorHandler sagaErrorHandler(ProducerFactory<?, ?> producerFactory, SagaProperties sagaProperties) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                deadLetterTemplate(producerFactory),
                // partition -1: let the key choose, so DLTs need not mirror the source partition count
                (record, ex) -> new TopicPartition(record.topic() + SagaTopics.DLT_SUFFIX, -1));

        SagaProperties.Retry retry = sagaProperties.kafka().retry();
        ExponentialBackOff backOff = new ExponentialBackOff(retry.initialInterval().toMillis(), 2.0);
        backOff.setMaxInterval(retry.maxInterval().toMillis());
        backOff.setMaxAttempts(retry.maxRetries());

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(NonRetryableSagaException.class);
        return handler;
    }

    @Bean
    @ConditionalOnProperty(prefix = "saga.kafka.topics", name = "create", havingValue = "true")
    KafkaAdmin.NewTopics sagaTopics(SagaProperties sagaProperties) {
        SagaProperties.Topics topics = sagaProperties.kafka().topics();
        return new KafkaAdmin.NewTopics(SagaTopics.ALL.stream()
                .flatMap(topic -> Stream.of(topic, topic + SagaTopics.DLT_SUFFIX))
                .map(topic -> new NewTopic(topic, topics.partitions(), topics.replicas()))
                .toArray(NewTopic[]::new));
    }

    /**
     * Dead letters are either raw bytes (deserialization failed) or the already deserialized payload
     * (handler failed), so the DLT producer picks a serializer by value type. It copies the configuration of
     * Boot's producer factory, so it gets the same connection (Boot ConnectionDetails, MSK IAM in prod).
     */
    private static KafkaTemplate<Object, Object> deadLetterTemplate(ProducerFactory<?, ?> producerFactory) {
        Map<Class<?>, Serializer<?>> serializers = new LinkedHashMap<>();
        serializers.put(byte[].class, new ByteArraySerializer());
        serializers.put(Object.class, new JsonSerializer<>().noTypeInfo());
        DefaultKafkaProducerFactory<Object, Object> factory = new DefaultKafkaProducerFactory<>(
                producerFactory.getConfigurationProperties(),
                stringKeys(),
                new DelegatingByTypeSerializer(serializers, true));
        return new KafkaTemplate<>(factory);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Serializer<Object> stringKeys() {
        return (Serializer) new StringSerializer();
    }
}
