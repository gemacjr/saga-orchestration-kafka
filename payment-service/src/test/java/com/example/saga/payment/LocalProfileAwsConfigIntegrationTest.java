package com.example.saga.payment;

import com.example.saga.testsupport.SagaTestContainers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.ParameterType;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Boots the service with the real {@code local} profile (application-local.yml) against LocalStack, proving the
 * Secrets Manager + SSM Parameter Store import that prod relies on: DB credentials and location and the business
 * limit all come from AWS, nothing from the test.
 */
class LocalProfileAwsConfigIntegrationTest {

    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:4.9"))
            .withServices(LocalStackContainer.Service.SECRETSMANAGER, LocalStackContainer.Service.SSM);
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(SagaTestContainers.POSTGRES_IMAGE);
    static final KafkaContainer KAFKA = new KafkaContainer(SagaTestContainers.KAFKA_IMAGE);

    static SsmClient ssm;
    static SecretsManagerClient secrets;

    @BeforeAll
    static void startInfrastructure() {
        LOCALSTACK.start();
        POSTGRES.start();
        KAFKA.start();

        var credentials = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()));
        ssm = SsmClient.builder().endpointOverride(LOCALSTACK.getEndpoint())
                .credentialsProvider(credentials).region(Region.of(LOCALSTACK.getRegion())).build();
        secrets = SecretsManagerClient.builder().endpointOverride(LOCALSTACK.getEndpoint())
                .credentialsProvider(credentials).region(Region.of(LOCALSTACK.getRegion())).build();

        param("/saga/shared/saga.kafka.retry.max-retries", "2");
        param("/saga/payment-service/db.host", POSTGRES.getHost());
        param("/saga/payment-service/db.port", String.valueOf(POSTGRES.getFirstMappedPort()));
        param("/saga/payment-service/db.name", POSTGRES.getDatabaseName());
        param("/saga/payment-service/payment.max-amount", "42.50");
    }

    @AfterAll
    static void stopInfrastructure() {
        KAFKA.stop();
        POSTGRES.stop();
        LOCALSTACK.stop();
    }

    private static void param(String name, String value) {
        ssm.putParameter(b -> b.name(name).value(value).type(ParameterType.STRING).overwrite(true));
    }

    private static ConfigurableApplicationContext startPaymentService() {
        return new SpringApplicationBuilder(PaymentServiceApplication.class)
                .profiles("local")
                .run("--AWS_ENDPOINT_URL=" + LOCALSTACK.getEndpoint(),
                        "--KAFKA_BOOTSTRAP_SERVERS=" + KAFKA.getBootstrapServers(),
                        "--server.port=0");
    }

    @Test
    void localProfileLoadsCredentialsAndSettingsFromAwsAndFailsFastWithoutThem() {
        // Without the DB secret the service must refuse to start rather than run half-configured
        assertThatThrownBy(LocalProfileAwsConfigIntegrationTest::startPaymentService)
                .hasStackTraceContaining("saga/payment-service/db");

        secrets.createSecret(b -> b.name("saga/payment-service/db").secretString(
                "{\"username\":\"" + POSTGRES.getUsername() + "\",\"password\":\"" + POSTGRES.getPassword() + "\"}"));

        try (ConfigurableApplicationContext context = startPaymentService()) {
            var env = context.getEnvironment();
            assertThat(env.getProperty("db.username")).isEqualTo(POSTGRES.getUsername());
            assertThat(env.getProperty("db.host")).isEqualTo(POSTGRES.getHost());
            assertThat(env.getProperty("saga.kafka.retry.max-retries")).isEqualTo("2");
            assertThat(context.getBean(PaymentProperties.class).maxAmount()).isEqualByComparingTo(new BigDecimal("42.50"));
            // Flyway ran through the AWS-provided connection
            assertThat(context.getBean(JdbcTemplate.class)
                    .queryForObject("select count(*) from payments", Integer.class)).isZero();
        }
    }
}
