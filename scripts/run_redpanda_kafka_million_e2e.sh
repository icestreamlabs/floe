#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-floe-redpanda-e2e}"
REDPANDA_IMAGE="${REDPANDA_IMAGE:-docker.redpanda.com/redpandadata/redpanda:latest}"
BROKERS="${BROKERS:-127.0.0.1:9092}"
FLOE_TEST_RELEASE="${FLOE_TEST_RELEASE:-0}"

cleanup() {
  docker rm -fv "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}

trap cleanup EXIT
cleanup

echo "Pulling Redpanda image: ${REDPANDA_IMAGE}"
docker pull "${REDPANDA_IMAGE}" >/dev/null

echo "Starting Redpanda container: ${CONTAINER_NAME}"
docker run -d \
  --name "${CONTAINER_NAME}" \
  -p 9092:9092 \
  "${REDPANDA_IMAGE}" \
  redpanda start \
    --overprovisioned \
    --smp 1 \
    --memory 1G \
    --reserve-memory 0M \
    --node-id 0 \
    --check=false \
    --kafka-addr PLAINTEXT://0.0.0.0:9092 \
    --advertise-kafka-addr PLAINTEXT://127.0.0.1:9092 >/dev/null

echo "Waiting for Redpanda to become ready..."
ready=0
for _ in $(seq 1 90); do
  if docker exec "${CONTAINER_NAME}" rpk cluster info >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [[ "${ready}" -ne 1 ]]; then
  echo "Redpanda did not become ready in time."
  docker logs "${CONTAINER_NAME}" || true
  exit 1
fi

docker exec "${CONTAINER_NAME}" rpk cluster info

echo "Running Floe million-row Kafka source+sink e2e test"
TEST_ARGS=(-p floe-node --test redpanda_kafka_million_e2e -- --ignored --nocapture)
if [[ "${FLOE_TEST_RELEASE}" == "1" ]]; then
  TEST_ARGS=(--release "${TEST_ARGS[@]}")
fi

FLOE_REDPANDA_BROKERS="${BROKERS}" \
cargo test "${TEST_ARGS[@]}"
