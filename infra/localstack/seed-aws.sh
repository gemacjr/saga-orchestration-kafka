#!/bin/bash
# Seeds LocalStack with the same Secrets Manager / SSM layout the services read in prod:
#   Secrets Manager  saga/<service>/db          {"username": "...", "password": "..."}
#   SSM              /saga/<service>/<property>  plain Spring property names
#   SSM              /saga/shared/<property>     settings shared by all services
# Runs automatically from /etc/localstack/init/ready.d when LocalStack is ready.
set -euo pipefail

AWS="${AWS_CLI:-awslocal}"
DB_HOST="${SEED_DB_HOST:-localhost}"

secret() { # name json
  $AWS secretsmanager create-secret --name "$1" --secret-string "$2" >/dev/null 2>&1 \
    || $AWS secretsmanager put-secret-value --secret-id "$1" --secret-string "$2" >/dev/null
}
param() { # name value [type]
  $AWS ssm put-parameter --name "$1" --value "$2" --type "${3:-String}" --overwrite >/dev/null
}

db() { # service database user password
  secret "saga/$1/db" "{\"username\":\"$3\",\"password\":\"$4\"}"
  param "/saga/$1/db.host" "$DB_HOST"
  param "/saga/$1/db.name" "$2"
}

db order-service             orders    order_svc     order_pw
db payment-service           payments  payment_svc   payment_pw
db inventory-service         inventory inventory_svc inventory_pw
db saga-orchestrator-service sagas     saga_svc      saga_pw

# Shared Kafka consumer retry policy
param /saga/shared/saga.kafka.retry.max-retries 4

# Business rules from the article, now runtime configuration
param /saga/payment-service/payment.max-amount 1000
param /saga/inventory-service/inventory.max-quantity-per-order 5

# Orchestrator timeouts
param /saga/saga-orchestrator-service/saga.orchestrator.step-timeout 30s
param /saga/saga-orchestrator-service/saga.orchestrator.max-step-retries 3

echo "Seeded saga secrets and parameters"
