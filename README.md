# Orchestration-Based Saga — Spring Boot 3.5 + Kafka + AWS

An implementation of [Orchestration-Based Saga Pattern with Spring Boot and Kafka](https://erkndmrl.medium.com/orchestration-based-saga-pattern-with-spring-boot-and-kafka-6a02f50a8d49).
It includes the article's "Production Considerations" and uses AWS-managed configuration:
**LocalStack locally, real AWS in prod**.

```
Client ─POST /orders─▶ Order ─OrderCreated─▶ Orchestrator ─ProcessPayment─▶ Payment
                                               ▲   │ ◀──PaymentCompleted/Failed──┘
                                               │   ├─ReserveInventory─▶ Inventory
                                               │   │ ◀──InventoryReserved/Failed──┘
                                               │   ├─RefundPayment (compensation)─▶ Payment
                                               │   └─CompleteOrder / CancelOrder─▶ Order
                                               └──────OrderCompleted/Cancelled───────┘
```

| Module | Port | Owns |
|---|---|---|
| `saga-messages` | – | Commands, events, topic names, versioned type registry (plain Java) |
| `saga-common` | – | Auto-configuration: outbox, inbox, Kafka retry/DLT, AWS config + MSK IAM deps |
| `order-service` | 8090 | `orders` DB, starts the saga, Complete/CancelOrder |
| `payment-service` | 8091 | `payments` DB, Process/RefundPayment |
| `inventory-service` | 8092 | `inventory` DB (stock + reservations), ReserveInventory |
| `saga-orchestrator-service` | 8093 | `sagas` DB, the state machine, timeouts |

## Run locally

```bash
mvn clean package
docker compose up --build          # Kafka, Postgres, LocalStack, Kafka UI (http://localhost:8088) + 4 services
./scripts/e2e.sh                   # the article's three scenarios, asserted against all four services
```

Port conflicts: `ORDER_SERVICE_PORT=18090 docker compose up` (also `PAYMENT_SERVICE_PORT`,
`INVENTORY_SERVICE_PORT`, `ORCHESTRATOR_PORT`). Pass the same variables to `e2e.sh`.

To run services from the IDE, start only the infrastructure with `docker compose up -d kafka postgres localstack kafka-ui`
and run any `*Application` class. The default profile is `local`.

| Scenario | Request | Order | Payment | Reservation | Saga |
|---|---|---|---|---|---|
| Happy path | qty 2, amount 250 | COMPLETED | COMPLETED | RESERVED | COMPLETED |
| Payment fails | amount > 1000 | CANCELLED | FAILED | – | FAILED |
| Compensation | qty > 5 | CANCELLED | REFUNDED | FAILED | COMPENSATED |

Inspect: `GET :8090/orders`, `:8091/payments`, `:8092/reservations`, `:8093/sagas[/{id}]`, `/actuator/prometheus`.
Send `Idempotency-Key: <uuid>` on `POST /orders` to make client retries safe.

## Configuration: LocalStack locally, AWS in prod

Every service loads its configuration through Spring Cloud AWS `spring.config.import`. The import is the
same in both profiles; only the endpoint and credentials differ.

| What | Where | Example |
|---|---|---|
| DB credentials | Secrets Manager `saga/<service>/db` → `db.username`, `db.password` | `{"username":"…","password":"…"}` |
| DB location | SSM `/saga/<service>/db.host`, `db.name` | `/saga/payment-service/db.host` |
| Business rules | SSM `/saga/<service>/…` | `payment.max-amount=1000`, `inventory.max-quantity-per-order=5` |
| Shared settings | SSM `/saga/shared/…` | `saga.kafka.retry.max-retries`, prod: `spring.kafka.bootstrap-servers` |

- **`local` profile** (`application-local.yml`): endpoint `http://localhost:4566`, static `test`/`test` credentials.
  `infra/localstack/seed-aws.sh` seeds LocalStack on startup with exactly the layout above. Kafka topics are auto-declared.
- **`prod` profile** (`application-prod.yml`, `SPRING_PROFILES_ACTIVE=prod`):
  - **Credentials**: no endpoint or keys are configured. The services use the default AWS provider chain
    (ECS task role, EKS IRSA/Pod Identity, EC2 profile) and read the region from `AWS_REGION`.
  - **Database**: RDS PostgreSQL with `sslmode=require`. Set `DB_SECRET_ID` to point at an RDS-managed master secret (`rds!db-…`).
  - **Kafka**: Amazon MSK over `SASL_SSL` with `AWS_MSK_IAM`. Put the bootstrap servers in `/saga/shared/spring.kafka.bootstrap-servers`.
  - **Topics**: `saga.kafka.topics.create=false`. Provision every `saga.*` topic and its `-dlt` twin in your IaC
    (RF 3, `min.insync.replicas=2`).
  - **IAM**: grant least privilege per service, see `infra/aws/service-iam-policy.json`.
  - **Logging**: structured ECS JSON logs.

To seed the prod parameters, run the same script against real AWS: `AWS_CLI=aws SEED_DB_HOST=<rds-endpoint> infra/localstack/seed-aws.sh`.
Prefer IaC, though, and **don't** use the demo secrets.

## Best practices beyond the article

| Concern | Implementation |
|---|---|
| Dual write (DB + Kafka) | **Transactional outbox** (`OutboxWriter`, `Propagation.MANDATORY`) + polling `OutboxRelay` using `FOR UPDATE SKIP LOCKED` (safe with N replicas), batch send, purge after retention |
| At-least-once delivery | **Inbox** table (`InboxGuard`): `(consumer, message_id)` insert in the handler's transaction; plus idempotent domain transitions (one payment/reservation per saga, re-send replays the recorded outcome) |
| Out-of-order / late events | Guarded transitions in `SagaInstance`: an event that doesn't match the current state is ignored |
| Concurrent saga updates | `@Version` optimistic locking; conflicts roll back and are retried by the error handler |
| Poison pills, transient errors | `ErrorHandlingDeserializer`, exponential backoff, then `<topic>-dlt`; `NonRetryableSagaException` skips retries |
| Schema evolution / security | Wire type = logical name (`OrderCreated.v1`), not a Java class; unknown classes are rejected |
| Ordering | Saga id is the Kafka key, so all messages of one saga share a partition |
| Unresponsive participants | `SagaTimeoutWatcher` finds stuck sagas (`saga.orchestrator.step-timeout`). `onStepTimeout` re-sends the command up to `max-step-retries`, then compensates; steps past the point of no return keep re-sending and increment `saga.stuck` for alerting |
| Refund before charge race | Payment records a `VOIDED` tombstone so a late `ProcessPayment` cannot charge |
| Stock races | Single-statement conditional `UPDATE … WHERE available >= ?` |
| Schema management | Flyway per service (+ shared outbox/inbox migration from `saga-common`), `ddl-auto=validate` |
| Ops | Actuator probes, Prometheus metrics (`saga.finished{outcome}`), Kafka observations, graceful shutdown, non-root layered image |

Replaying a dead letter: fix the cause, then copy the record from `<topic>-dlt` back to `<topic>` (Kafka UI → Produce).
The inbox ensures that an already applied message stays a no-op.

## Tests

`mvn verify` runs about 160 tests and **fails the build if any module drops below 95% line / 85% branch coverage** (JaCoCo).
It needs Docker for Testcontainers. The aggregated report is at `coverage-report/target/site/jacoco-aggregate/index.html`.

| Layer | What it proves |
|---|---|
| Unit: `SagaInstanceTest`, `SagaTransitionGuardTest` | Every transition, tried from every state: allowed only from its source state, never changes state otherwise |
| Unit: `SagaTimeoutPolicyTest`, `SagaTimeoutWatcherTest` | Retry → compensate → stuck-alert policy; one failing saga doesn't stop the timeout scan |
| Unit: domain + handler tests | Idempotent order/payment transitions; every Kafka handler consults the inbox |
| Contract: `MessageJsonContractTest`, `SagaJsonDeserializerTest` | Every message round-trips through JSON; logical type headers resolve; gadget classes are rejected |
| Integration: `SagaCommonIntegrationTest` (Postgres + Kafka) | Outbox commit/rollback/order/purge, inbox, retry-then-succeed, retries-then-DLT, non-retryable → DLT, poison pill → DLT with original bytes |
| Integration: one per service (Postgres + Kafka) | The test plays the other saga participants: commands in, DB state + reply events out, duplicates, late events, DLT, REST API |
| Integration: `InventoryServiceIntegrationTest` | 25 concurrent reservations on 10 units of stock reserve exactly 10 (no oversell) |
| Integration: `PaymentServiceIntegrationTest` | Refund before charge leaves a `VOIDED` record, and the late charge is blocked |
| AWS: `LocalProfileAwsConfigIntegrationTest` (LocalStack) | The real `local` profile loads DB credentials from Secrets Manager plus DB location and the business limit from SSM. A missing secret fails startup. |
| System: `scripts/e2e.sh` | The article's three scenarios against the full docker compose stack |
