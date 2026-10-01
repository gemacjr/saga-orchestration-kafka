#!/usr/bin/env bash
# Runs the article's three scenarios against a running stack and asserts the final state of every service.
#   ./scripts/e2e.sh            (ports overridable: ORDER_URL, PAYMENT_URL, INVENTORY_URL, SAGA_URL)
set -euo pipefail

ORDER_URL=${ORDER_URL:-http://localhost:${ORDER_SERVICE_PORT:-8090}}
PAYMENT_URL=${PAYMENT_URL:-http://localhost:${PAYMENT_SERVICE_PORT:-8091}}
INVENTORY_URL=${INVENTORY_URL:-http://localhost:${INVENTORY_SERVICE_PORT:-8092}}
SAGA_URL=${SAGA_URL:-http://localhost:${ORCHESTRATOR_PORT:-8093}}
failures=0

field() { # url jq-filter
  curl -sf "$1" | jq -r "$2"
}

run() { # name productId quantity amount expectedOrder expectedPayment expectedReservation expectedSaga
  local name=$1 key
  key=$(uuidgen)
  local order
  order=$(curl -sf -X POST "$ORDER_URL/orders" -H 'Content-Type: application/json' -H "Idempotency-Key: $key" \
    -d "{\"productId\":\"$2\",\"quantity\":$3,\"amount\":$4}")
  local orderId sagaId
  orderId=$(jq -r .orderId <<<"$order")
  sagaId=$(jq -r .sagaId <<<"$order")

  # A retried POST with the same Idempotency-Key must return the same order, not start a second saga
  local replayId
  replayId=$(curl -sf -X POST "$ORDER_URL/orders" -H 'Content-Type: application/json' -H "Idempotency-Key: $key" \
    -d "{\"productId\":\"$2\",\"quantity\":$3,\"amount\":$4}" | jq -r .orderId)

  local saga=""
  for _ in $(seq 1 60); do
    saga=$(field "$SAGA_URL/sagas/$sagaId" .status 2>/dev/null || true)
    [[ $saga =~ ^(COMPLETED|COMPENSATED|FAILED)$ ]] && break
    sleep 0.5
  done

  local o p r
  o=$(field "$ORDER_URL/orders/$orderId" .status)
  p=$(field "$PAYMENT_URL/payments?limit=500" ".[] | select(.sagaId==\"$sagaId\") | .status")
  r=$(field "$INVENTORY_URL/reservations?limit=500" ".[] | select(.sagaId==\"$sagaId\") | .status")
  : "${p:=-}" "${r:=-}"

  local actual="order=$o payment=$p reservation=$r saga=$saga"
  local expected="order=$5 payment=$6 reservation=$7 saga=$8"
  if [[ $actual == "$expected" && $replayId == "$orderId" ]]; then
    printf '  PASS  %-28s %s\n' "$name" "$actual"
  else
    printf '  FAIL  %-28s expected [%s] got [%s] replay=%s\n' "$name" "$expected" "$actual" "$replayId"
    failures=$((failures + 1))
  fi
}

for url in "$ORDER_URL" "$PAYMENT_URL" "$INVENTORY_URL" "$SAGA_URL"; do
  for _ in $(seq 1 60); do
    curl -sf "$url/actuator/health/readiness" >/dev/null && break
    sleep 2
  done
  curl -sf "$url/actuator/health/readiness" >/dev/null || { echo "Not ready: $url"; exit 1; }
done

echo "Saga end-to-end scenarios"
run "happy path"                 product-100 2  250.00  COMPLETED COMPLETED RESERVED COMPLETED
run "payment failure"            product-200 2  1500.00 CANCELLED FAILED    -        FAILED
run "inventory -> compensation"  product-300 10 300.00  CANCELLED REFUNDED  FAILED   COMPENSATED

exit $failures
