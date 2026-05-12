#!/usr/bin/env bash
set -euo pipefail

POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-floe-cdc-bench-postgres}"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:16}"
POSTGRES_PORT="${POSTGRES_PORT:-55434}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-postgres}"

REDPANDA_CONTAINER="${REDPANDA_CONTAINER:-floe-cdc-bench-redpanda}"
REDPANDA_IMAGE="${REDPANDA_IMAGE:-docker.redpanda.com/redpandadata/redpanda:latest}"
REDPANDA_PORT="${REDPANDA_PORT:-19092}"
BROKERS="${BROKERS:-127.0.0.1:${REDPANDA_PORT}}"

ROWS="${ROWS:-100000}"
TOPIC="${TOPIC:-floe_cdc_bench_orders}"
SLOT="${SLOT:-floe_cdc_bench_slot}"
PUBLICATION="${PUBLICATION:-floe_cdc_bench_pub}"
PIPELINE_FORMAT="${PIPELINE_FORMAT:-debezium-json}"
ARROW_IPC_ROWS_PER_RECORD="${ARROW_IPC_ROWS_PER_RECORD:-8192}"
FLOE_PG_PORT="${FLOE_PG_PORT:-16432}"
TIMEOUT_SECS="${TIMEOUT_SECS:-900}"
BUILD_RELEASE="${BUILD_RELEASE:-1}"
KEEP_CONTAINERS="${KEEP_CONTAINERS:-0}"

RUN_ID="$(date +%Y%m%dT%H%M%S)"
ARTIFACT_DIR="${ARTIFACT_DIR:-target/cdc_bench/${RUN_ID}}"
CONFIG_PATH="${ARTIFACT_DIR}/empty_config.json"
SQL_PATH="${ARTIFACT_DIR}/program.sql"
NODE_STDOUT="${ARTIFACT_DIR}/floe-node.stdout.log"
NODE_STDERR="${ARTIFACT_DIR}/floe-node.stderr.log"
COUNTER_LOG="${ARTIFACT_DIR}/kafka-counter.log"

mkdir -p "${ARTIFACT_DIR}"
printf '{}\n' >"${CONFIG_PATH}"

node_pid=""

cleanup() {
  if [[ -n "${node_pid}" ]] && kill -0 "${node_pid}" >/dev/null 2>&1; then
    kill -INT "${node_pid}" >/dev/null 2>&1 || true
    wait "${node_pid}" >/dev/null 2>&1 || true
  fi
  if [[ "${KEEP_CONTAINERS}" != "1" ]]; then
    docker rm -f "${POSTGRES_CONTAINER}" >/dev/null 2>&1 || true
    docker rm -f "${REDPANDA_CONTAINER}" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

wait_for_postgres() {
  local ready=0
  for _ in $(seq 1 90); do
    if docker exec "${POSTGRES_CONTAINER}" pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 1
  done
  if [[ "${ready}" -ne 1 ]]; then
    echo "Postgres did not become ready in time." >&2
    docker logs "${POSTGRES_CONTAINER}" >&2 || true
    exit 1
  fi
}

wait_for_redpanda() {
  local ready=0
  for _ in $(seq 1 90); do
    if docker exec "${REDPANDA_CONTAINER}" rpk cluster info >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 1
  done
  if [[ "${ready}" -ne 1 ]]; then
    echo "Redpanda did not become ready in time." >&2
    docker logs "${REDPANDA_CONTAINER}" >&2 || true
    exit 1
  fi
}

require_cmd docker
require_cmd cargo

docker rm -f "${POSTGRES_CONTAINER}" "${REDPANDA_CONTAINER}" >/dev/null 2>&1 || true

echo "artifact_dir=${ARTIFACT_DIR}"
echo "rows=${ROWS}"
echo "brokers=${BROKERS}"
echo "topic=${TOPIC}"
echo "pipeline_format=${PIPELINE_FORMAT}"

echo "Pulling images..."
docker pull "${POSTGRES_IMAGE}" >/dev/null
docker pull "${REDPANDA_IMAGE}" >/dev/null

echo "Starting Postgres ${POSTGRES_IMAGE} on port ${POSTGRES_PORT}"
docker run -d \
  --name "${POSTGRES_CONTAINER}" \
  -e POSTGRES_USER="${POSTGRES_USER}" \
  -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
  -e POSTGRES_DB="${POSTGRES_DB}" \
  -p "${POSTGRES_PORT}:5432" \
  "${POSTGRES_IMAGE}" \
  postgres \
    -c wal_level=logical \
    -c max_replication_slots=16 \
    -c max_wal_senders=16 \
    -c max_slot_wal_keep_size=4096MB >/dev/null
wait_for_postgres

echo "Starting Redpanda ${REDPANDA_IMAGE} on port ${REDPANDA_PORT}"
docker run -d \
  --name "${REDPANDA_CONTAINER}" \
  -p "${REDPANDA_PORT}:9092" \
  "${REDPANDA_IMAGE}" \
  redpanda start \
    --overprovisioned \
    --smp 1 \
    --memory 2G \
    --reserve-memory 0M \
    --node-id 0 \
    --check=false \
    --kafka-addr PLAINTEXT://0.0.0.0:9092 \
    --advertise-kafka-addr "PLAINTEXT://127.0.0.1:${REDPANDA_PORT}" >/dev/null
wait_for_redpanda

echo "Creating Kafka topic ${TOPIC}"
docker exec "${REDPANDA_CONTAINER}" rpk topic create "${TOPIC}" -p 1 -r 1 >/dev/null 2>&1 || true

echo "Loading Postgres snapshot table with ${ROWS} rows"
load_started_ns="$(date +%s%N)"
docker exec -i "${POSTGRES_CONTAINER}" psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null <<SQL
DROP PUBLICATION IF EXISTS ${PUBLICATION};
DROP TABLE IF EXISTS public.orders;
CREATE TABLE public.orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  amount BIGINT NOT NULL,
  status TEXT,
  created_at BIGINT NOT NULL
);
INSERT INTO public.orders
SELECT
  gs::BIGINT AS id,
  (gs % 100000)::BIGINT AS customer_id,
  (100 + (gs % 10000))::BIGINT AS amount,
  CASE WHEN gs % 3 = 0 THEN 'paid' WHEN gs % 3 = 1 THEN 'open' ELSE 'cancelled' END AS status,
  (1700000000000 + gs)::BIGINT AS created_at
FROM generate_series(1, ${ROWS}) AS gs;
SQL
load_finished_ns="$(date +%s%N)"
load_seconds="$(awk "BEGIN { printf \"%.3f\", (${load_finished_ns} - ${load_started_ns}) / 1000000000 }")"
echo "timing.postgres_load_seconds=${load_seconds}"

if [[ "${BUILD_RELEASE}" == "1" ]]; then
  echo "Building release binaries"
  cargo build --release -p floe-node -p floe-benchmarks --bins >/dev/null
  FLOE_BIN="target/release/floe-node"
  COUNTER_BIN="target/release/postgres_cdc_kafka_counter"
else
  echo "Building debug binaries"
  cargo build -p floe-node -p floe-benchmarks --bins >/dev/null
  FLOE_BIN="target/debug/floe-node"
  COUNTER_BIN="target/debug/postgres_cdc_kafka_counter"
fi

PG_DSN="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:${POSTGRES_PORT}/${POSTGRES_DB}"
cat >"${SQL_PATH}" <<SQL
CREATE SOURCE pg_main WITH (
  connector = 'postgres-cdc',
  connection = '${PG_DSN}',
  slot.name = '${SLOT}',
  publication.name = '${PUBLICATION}'
);
CREATE REPLICATION PIPELINE pg_orders_to_kafka
FROM pg_main TABLE 'public.orders'
INTO KAFKA WITH (
  brokers = '${BROKERS}',
  topic = '${TOPIC}',
  format = '${PIPELINE_FORMAT}',
  delivery = 'at-least-once',
  tombstones = false,
  transaction_metadata = false
);
SQL

normalized_pipeline_format="${PIPELINE_FORMAT//-/_}"
case "${normalized_pipeline_format}" in
  debezium_json)
    expected_messages="${ROWS}"
    ;;
  arrow_ipc)
    expected_messages="$(( (ROWS + ARROW_IPC_ROWS_PER_RECORD - 1) / ARROW_IPC_ROWS_PER_RECORD ))"
    ;;
  *)
    echo "unsupported PIPELINE_FORMAT=${PIPELINE_FORMAT}" >&2
    exit 1
    ;;
esac
echo "expected_kafka_messages=${expected_messages}"

echo "Starting Floe node"
node_started_ns="$(date +%s%N)"
FLOE_DATA_DIR="${ARTIFACT_DIR}/floe-data" \
FLOE_PG_ADDR="127.0.0.1:${FLOE_PG_PORT}" \
"${FLOE_BIN}" run \
  --config "${CONFIG_PATH}" \
  --mv-query "$(cat "${SQL_PATH}")" \
  --slatedb-await-durable=false \
  --ingest-batch-size 16384 \
  --ingest-batch-per-source 16384 \
  --ingest-batch-per-connector 16384 \
  >"${NODE_STDOUT}" 2>"${NODE_STDERR}" &
node_pid="$!"

echo "Counting CDC records from Kafka"
"${COUNTER_BIN}" \
  --brokers "${BROKERS}" \
  --topic "${TOPIC}" \
  --expected "${expected_messages}" \
  --timeout-secs "${TIMEOUT_SECS}" | tee "${COUNTER_LOG}"
node_finished_ns="$(date +%s%N)"

end_to_end_seconds="$(awk "BEGIN { printf \"%.3f\", (${node_finished_ns} - ${node_started_ns}) / 1000000000 }")"
end_to_end_rows_per_second="$(awk "BEGIN { printf \"%.0f\", ${ROWS} / ${end_to_end_seconds} }")"

{
  echo "benchmark.rows=${ROWS}"
  echo "benchmark.pipeline_format=${PIPELINE_FORMAT}"
  echo "benchmark.expected_kafka_messages=${expected_messages}"
  echo "benchmark.end_to_end_seconds=${end_to_end_seconds}"
  echo "benchmark.end_to_end_rows_per_second=${end_to_end_rows_per_second}"
  echo "benchmark.artifact_dir=${ARTIFACT_DIR}"
  echo "benchmark.node_stdout=${NODE_STDOUT}"
  echo "benchmark.node_stderr=${NODE_STDERR}"
  echo "benchmark.counter_log=${COUNTER_LOG}"
} | tee "${ARTIFACT_DIR}/summary.env"

echo "Stopping Floe node"
kill -INT "${node_pid}" >/dev/null 2>&1 || true
wait "${node_pid}" >/dev/null 2>&1 || true
node_pid=""

echo "CDC benchmark complete."
