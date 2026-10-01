package com.example.saga.common;

import com.example.saga.common.inbox.InboxGuard;
import com.example.saga.common.kafka.SagaKafkaConfiguration;
import com.example.saga.common.outbox.OutboxRelay;
import com.example.saga.common.outbox.OutboxWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.support.TransactionTemplate;

@AutoConfiguration(after = {JdbcTemplateAutoConfiguration.class, KafkaAutoConfiguration.class})
@EnableScheduling
@EnableConfigurationProperties(SagaProperties.class)
@Import(SagaKafkaConfiguration.class)
public class SagaCommonAutoConfiguration {

    @Bean
    OutboxWriter outboxWriter(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        return new OutboxWriter(jdbcTemplate, objectMapper);
    }

    @Bean
    OutboxRelay outboxRelay(JdbcTemplate jdbcTemplate, KafkaTemplate<String, String> kafkaTemplate,
                            TransactionTemplate transactionTemplate, SagaProperties properties) {
        return new OutboxRelay(jdbcTemplate, kafkaTemplate, transactionTemplate, properties.outbox());
    }

    @Bean
    InboxGuard inboxGuard(JdbcTemplate jdbcTemplate, @Value("${spring.application.name}") String consumerName) {
        return new InboxGuard(jdbcTemplate, consumerName);
    }
}
