#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-floe-postgres-cdc-binary-e2e}"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:16}"
POSTGRES_PORT="${POSTGRES_PORT:-55433}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-postgres}"
FLOE_TEST_RELEASE="${FLOE_TEST_RELEASE:-0}"

cleanup() {
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}

trap cleanup EXIT
cleanup

echo "Pulling Postgres image: ${POSTGRES_IMAGE}"
docker pull "${POSTGRES_IMAGE}" >/dev/null

echo "Starting logical-replication Postgres container: ${CONTAINER_NAME}"
docker run -d \
  --name "${CONTAINER_NAME}" \
  -e POSTGRES_USER="${POSTGRES_USER}" \
  -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
  -e POSTGRES_DB="${POSTGRES_DB}" \
  -p "${POSTGRES_PORT}:5432" \
  "${POSTGRES_IMAGE}" \
  postgres \
    -c wal_level=logical \
    -c max_replication_slots=16 \
    -c max_wal_senders=16 \
    -c max_slot_wal_keep_size=1024MB >/dev/null

echo "Waiting for Postgres to become ready..."
ready=0
for _ in $(seq 1 90); do
  if docker exec "${CONTAINER_NAME}" pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [[ "${ready}" -ne 1 ]]; then
  echo "Postgres did not become ready in time."
  docker logs "${CONTAINER_NAME}" || true
  exit 1
fi

docker exec "${CONTAINER_NAME}" psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" \
  -c "SHOW wal_level;" \
  -c "SHOW max_replication_slots;" \
  -c "SHOW max_wal_senders;"

echo "Running Floe binary native Postgres CDC e2e tests"
TEST_ARGS=(-p floe-node --test ga_acceptance postgres_cdc -- --ignored --nocapture --test-threads=1)
if [[ "${FLOE_TEST_RELEASE}" == "1" ]]; then
  TEST_ARGS=(--release "${TEST_ARGS[@]}")
fi

FLOE_ACCEPTANCE_PG_DSN="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:${POSTGRES_PORT}/${POSTGRES_DB}" \
cargo test "${TEST_ARGS[@]}"
