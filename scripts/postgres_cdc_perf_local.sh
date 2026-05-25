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
REDPANDA_KAFKA_BATCH_MAX_BYTES="${REDPANDA_KAFKA_BATCH_MAX_BYTES:-10485760}"
REDPANDA_TOPIC_MAX_MESSAGE_BYTES="${REDPANDA_TOPIC_MAX_MESSAGE_BYTES:-10485760}"
BROKERS="${BROKERS:-127.0.0.1:${REDPANDA_PORT}}"

ROWS="${ROWS:-100000}"
DATASET="${DATASET:-synthetic-orders}"
BENCH_MODE="${BENCH_MODE:-snapshot}"
TARGET="${TARGET:-kafka}"
TOPIC="${TOPIC:-floe_cdc_bench_orders}"
SLOT="${SLOT:-floe_cdc_bench_slot}"
PUBLICATION="${PUBLICATION:-floe_cdc_bench_pub}"
PIPELINE_FORMAT="${PIPELINE_FORMAT:-floe-json}"
DURABLE_REPLICATION_BUFFER="${DURABLE_REPLICATION_BUFFER:-true}"
BUFFER_MAX_PENDING_BYTES="${BUFFER_MAX_PENDING_BYTES:-}"
BUFFER_MAX_PENDING_RECORDS="${BUFFER_MAX_PENDING_RECORDS:-}"
BUFFER_MAX_PENDING_OBJECTS="${BUFFER_MAX_PENDING_OBJECTS:-}"
BUFFER_MAX_PENDING_AGE_MS="${BUFFER_MAX_PENDING_AGE_MS:-}"
ARROW_IPC_ROWS_PER_RECORD="${ARROW_IPC_ROWS_PER_RECORD:-16384}"
ARROW_IPC_COMPRESSION="${ARROW_IPC_COMPRESSION:-none}"
KAFKA_METADATA_HEADERS="${KAFKA_METADATA_HEADERS:-false}"
LIVE_WRITE_CHUNK_ROWS="${LIVE_WRITE_CHUNK_ROWS:-0}"
LIVE_WRITE_SLEEP_MS="${LIVE_WRITE_SLEEP_MS:-0}"
FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH="${FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH:-16384}"
FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS="${FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS:-1}"
FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS="${FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS:-1}"
FLOE_PG_PORT="${FLOE_PG_PORT:-16432}"
FLOE_ADMIN_PORT="${FLOE_ADMIN_PORT:-18080}"
TIMEOUT_SECS="${TIMEOUT_SECS:-900}"
BUILD_RELEASE="${BUILD_RELEASE:-1}"
KEEP_CONTAINERS="${KEEP_CONTAINERS:-0}"
TPCH_SCALE_FACTOR="${TPCH_SCALE_FACTOR:-0.01}"
TPCHGEN_BIN="${TPCHGEN_BIN:-tpchgen-cli}"
SLATEDB_FLUSH_INTERVAL_MS="${FLOE_SLATEDB_FLUSH_INTERVAL_MS:-500}"

RUN_ID="$(date +%Y%m%dT%H%M%S)"
ARTIFACT_DIR="${ARTIFACT_DIR:-target/cdc_bench/${RUN_ID}}"
TPCH_DATA_DIR="${TPCH_DATA_DIR:-${ARTIFACT_DIR}/tpch}"
CONFIG_PATH="${ARTIFACT_DIR}/empty_config.json"
SQL_PATH="${ARTIFACT_DIR}/program.sql"
NODE_STDOUT="${ARTIFACT_DIR}/floe-node.stdout.log"
NODE_STDERR="${ARTIFACT_DIR}/floe-node.stderr.log"
NODE_RESOURCE_LOG="${ARTIFACT_DIR}/floe-node.resources.log"
COUNTER_LOG="${ARTIFACT_DIR}/kafka-counter.log"
REPRODUCE_LOG="${ARTIFACT_DIR}/reproduce.sh"
SYSTEM_LOG="${ARTIFACT_DIR}/system.txt"
POSTGRES_SETTINGS_LOG="${ARTIFACT_DIR}/postgres-settings.txt"
KAFKA_TOPIC_LOG="${ARTIFACT_DIR}/kafka-topic.txt"
POSTGRES_SLOT_LOG="${ARTIFACT_DIR}/postgres-slot.log"
DOCKER_STATS_LOG="${ARTIFACT_DIR}/docker-stats.log"
SUMMARY_ENV="${ARTIFACT_DIR}/summary.env"
SUMMARY_JSON="${ARTIFACT_DIR}/summary.json"
SUMMARY_MD="${ARTIFACT_DIR}/summary.md"

mkdir -p "${ARTIFACT_DIR}"
TARGET_NORMALIZED="${TARGET,,}"
TARGET_NORMALIZED="${TARGET_NORMALIZED//-/_}"
case "${TARGET_NORMALIZED}" in
  kafka|postgres) ;;
  *)
    echo "unsupported TARGET=${TARGET} (expected kafka|postgres)" >&2
    exit 1
    ;;
esac
if [[ "${TARGET_NORMALIZED}" == "postgres" ]]; then
  normalized_pipeline_format="${PIPELINE_FORMAT,,}"
  normalized_pipeline_format="${normalized_pipeline_format//-/_}"
  case "${normalized_pipeline_format}" in
    floe_json|compact_json) ;;
    *)
      echo "TARGET=postgres currently requires PIPELINE_FORMAT=floe-json" >&2
      exit 1
      ;;
  esac
fi
case "${ARROW_IPC_COMPRESSION}" in
  ""|none|off|false|0)
    ARROW_IPC_COMPRESSION_JSON="null"
    ;;
  lz4|lz4_frame|lz4-frame)
    ARROW_IPC_COMPRESSION_JSON='"lz4_frame"'
    ;;
  *)
    echo "unsupported ARROW_IPC_COMPRESSION=${ARROW_IPC_COMPRESSION}; expected none or lz4_frame" >&2
    exit 1
    ;;
esac
cat >"${CONFIG_PATH}" <<JSON
{
  "runtime": {
    "pgwire_addr": "127.0.0.1:${FLOE_PG_PORT}",
    "admin_port": ${FLOE_ADMIN_PORT}
  },
  "storage": {
    "data_dir": "${ARTIFACT_DIR}/floe-data"
  },
  "replication": {
    "encoding": {
      "arrow_ipc_rows_per_record": ${ARROW_IPC_ROWS_PER_RECORD},
      "arrow_ipc_compression": ${ARROW_IPC_COMPRESSION_JSON},
      "kafka_metadata_headers": ${KAFKA_METADATA_HEADERS}
    }
  },
  "postgres_cdc": {
    "snapshot": {
      "rows_per_batch": ${FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH},
      "max_workers": ${FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS},
      "intra_table_chunks": ${FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS}
    }
  }
}
JSON

node_pid=""
node_process_group=0

stop_node() {
  if [[ -z "${node_pid}" ]]; then
    return
  fi
  if ! kill -0 "${node_pid}" >/dev/null 2>&1; then
    wait "${node_pid}" >/dev/null 2>&1 || true
    node_pid=""
    node_process_group=0
    return
  fi

  if [[ "${node_process_group}" == "1" ]]; then
    kill -INT -- "-${node_pid}" >/dev/null 2>&1 || true
  else
    kill -INT "${node_pid}" >/dev/null 2>&1 || true
  fi

  for _ in $(seq 1 20); do
    if ! kill -0 "${node_pid}" >/dev/null 2>&1; then
      wait "${node_pid}" >/dev/null 2>&1 || true
      node_pid=""
      node_process_group=0
      return
    fi
    sleep 0.5
  done

  if [[ "${node_process_group}" == "1" ]]; then
    kill -TERM -- "-${node_pid}" >/dev/null 2>&1 || true
  else
    kill -TERM "${node_pid}" >/dev/null 2>&1 || true
  fi
  wait "${node_pid}" >/dev/null 2>&1 || true
  node_pid=""
  node_process_group=0
}

cleanup() {
  stop_node || true
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

write_reproduce_command() {
  {
    echo "#!/usr/bin/env bash"
    echo "set -euo pipefail"
    printf 'ARTIFACT_DIR=%q \\\n' "${ARTIFACT_DIR}"
    printf 'ROWS=%q \\\n' "${ROWS}"
    printf 'DATASET=%q \\\n' "${DATASET}"
    printf 'TPCH_SCALE_FACTOR=%q \\\n' "${TPCH_SCALE_FACTOR}"
    printf 'BENCH_MODE=%q \\\n' "${BENCH_MODE}"
    printf 'TARGET=%q \\\n' "${TARGET}"
    printf 'TOPIC=%q \\\n' "${TOPIC}"
    printf 'PIPELINE_FORMAT=%q \\\n' "${PIPELINE_FORMAT}"
    printf 'DURABLE_REPLICATION_BUFFER=%q \\\n' "${DURABLE_REPLICATION_BUFFER}"
    printf 'BUFFER_MAX_PENDING_BYTES=%q \\\n' "${BUFFER_MAX_PENDING_BYTES}"
    printf 'BUFFER_MAX_PENDING_RECORDS=%q \\\n' "${BUFFER_MAX_PENDING_RECORDS}"
    printf 'BUFFER_MAX_PENDING_OBJECTS=%q \\\n' "${BUFFER_MAX_PENDING_OBJECTS}"
    printf 'BUFFER_MAX_PENDING_AGE_MS=%q \\\n' "${BUFFER_MAX_PENDING_AGE_MS}"
    printf 'ARROW_IPC_ROWS_PER_RECORD=%q \\\n' "${ARROW_IPC_ROWS_PER_RECORD}"
    printf 'ARROW_IPC_COMPRESSION=%q \\\n' "${ARROW_IPC_COMPRESSION}"
    printf 'KAFKA_METADATA_HEADERS=%q \\\n' "${KAFKA_METADATA_HEADERS}"
    printf 'LIVE_WRITE_CHUNK_ROWS=%q \\\n' "${LIVE_WRITE_CHUNK_ROWS}"
    printf 'LIVE_WRITE_SLEEP_MS=%q \\\n' "${LIVE_WRITE_SLEEP_MS}"
    printf 'FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH=%q \\\n' "${FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH}"
    printf 'FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS=%q \\\n' "${FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS}"
    printf 'FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS=%q \\\n' "${FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS}"
    printf 'FLOE_PG_PORT=%q \\\n' "${FLOE_PG_PORT}"
    printf 'FLOE_ADMIN_PORT=%q \\\n' "${FLOE_ADMIN_PORT}"
    printf 'FLOE_SLATEDB_FLUSH_INTERVAL_MS=%q \\\n' "${SLATEDB_FLUSH_INTERVAL_MS}"
    printf 'TIMEOUT_SECS=%q \\\n' "${TIMEOUT_SECS}"
    printf 'BUILD_RELEASE=%q \\\n' "${BUILD_RELEASE}"
    printf 'scripts/postgres_cdc_perf_local.sh\n'
  } >"${REPRODUCE_LOG}"
  chmod +x "${REPRODUCE_LOG}"
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

wait_for_postgres_slot_active() {
  local ready=0
  for _ in $(seq 1 120); do
    if [[ "$(docker exec "${POSTGRES_CONTAINER}" psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COALESCE((SELECT active FROM pg_replication_slots WHERE slot_name = '${SLOT}'), false)")" == "t" ]]; then
      ready=1
      break
    fi
    sleep 1
  done
  if [[ "${ready}" -ne 1 ]]; then
    echo "Postgres CDC replication slot ${SLOT} did not become active in time." >&2
    docker exec "${POSTGRES_CONTAINER}" psql -x -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots" >&2 || true
    exit 1
  fi
}

write_system_context() {
  {
    echo "benchmark.timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "benchmark.git_commit=$(git rev-parse HEAD 2>/dev/null || true)"
    echo "benchmark.git_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
    echo "benchmark.host_uname=$(uname -a)"
    echo "benchmark.cargo=$(cargo --version 2>/dev/null || true)"
    echo "benchmark.rustc=$(rustc --version 2>/dev/null || true)"
    echo
    if command -v lscpu >/dev/null 2>&1; then
      lscpu
      echo
    fi
    if command -v free >/dev/null 2>&1; then
      free -h
      echo
    fi
    docker version
  } >"${SYSTEM_LOG}" 2>&1 || true
}

write_postgres_settings() {
  docker exec "${POSTGRES_CONTAINER}" psql -x -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "
    SELECT name, setting, unit
    FROM pg_settings
    WHERE name IN (
      'wal_level',
      'max_replication_slots',
      'max_wal_senders',
      'max_slot_wal_keep_size',
      'shared_buffers',
      'work_mem',
      'maintenance_work_mem',
      'effective_cache_size',
      'synchronous_commit'
    )
    ORDER BY name;
  " >"${POSTGRES_SETTINGS_LOG}" 2>&1 || true
}

write_kafka_topic_info() {
  : >"${KAFKA_TOPIC_LOG}"
  for topic in "${topics[@]}"; do
    {
      echo "topic=${topic}"
      docker exec "${REDPANDA_CONTAINER}" rpk topic describe "${topic}"
      echo
    } >>"${KAFKA_TOPIC_LOG}" 2>&1 || true
  done
}

write_postgres_slot_info() {
  docker exec "${POSTGRES_CONTAINER}" psql -x -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "
    SELECT
      slot_name,
      active,
      restart_lsn,
      confirmed_flush_lsn,
      pg_current_wal_lsn() AS current_wal_lsn,
      pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::BIGINT AS confirmed_lag_bytes,
      pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::BIGINT AS restart_lag_bytes
    FROM pg_replication_slots
    WHERE slot_name = '${SLOT}';
  " >"${POSTGRES_SLOT_LOG}" 2>&1 || true
}

write_docker_stats() {
  local stats_containers=("${POSTGRES_CONTAINER}")
  if [[ "${TARGET_NORMALIZED}" == "kafka" ]]; then
    stats_containers+=("${REDPANDA_CONTAINER}")
  fi
  docker stats --no-stream --format 'container={{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}} net={{.NetIO}} block={{.BlockIO}} pids={{.PIDs}}' \
    "${stats_containers[@]}" >"${DOCKER_STATS_LOG}" 2>&1 || true
  if [[ -n "${node_pid}" ]] && kill -0 "${node_pid}" >/dev/null 2>&1; then
    local observed_pid="${node_pid}"
    if command -v pgrep >/dev/null 2>&1; then
      observed_pid="$(pgrep -P "${node_pid}" -n floe-node 2>/dev/null || printf '%s' "${node_pid}")"
    fi
    {
      echo
      ps -p "${observed_pid}" -o pid=,pcpu=,pmem=,rss=,vsz=,etime=,command=
    } >>"${DOCKER_STATS_LOG}" 2>&1 || true
  fi
}

postgres_target_table_for_upstream() {
  local upstream="$1"
  local schema="${upstream%.*}"
  local table="${upstream##*.}"
  if [[ "${schema}" == "${upstream}" ]]; then
    schema="public"
    table="${upstream}"
  fi
  printf '%s.%s_sink' "${schema}" "${table}"
}

create_postgres_sink_tables() {
  if [[ "${TARGET_NORMALIZED}" != "postgres" ]]; then
    return
  fi
  for idx in "${!upstream_tables[@]}"; do
    local upstream="${upstream_tables[$idx]}"
    local target="${target_tables[$idx]}"
    docker exec -i "${POSTGRES_CONTAINER}" psql \
      -v ON_ERROR_STOP=1 \
      -U "${POSTGRES_USER}" \
      -d "${POSTGRES_DB}" >/dev/null <<SQL
DROP TABLE IF EXISTS ${target};
CREATE TABLE ${target} (LIKE ${upstream} INCLUDING ALL);
SQL
  done
}

postgres_sink_total_rows() {
  local total=0
  for table in "${target_tables[@]}"; do
    local count
    count="$(docker exec "${POSTGRES_CONTAINER}" psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM ${table}")"
    total=$((total + count))
  done
  echo "${total}"
}

postgres_sink_updated_rows() {
  if [[ "${DATASET}" != "synthetic-orders" ]]; then
    echo "0"
    return
  fi
  docker exec "${POSTGRES_CONTAINER}" psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM public.orders_sink WHERE status = 'updated'"
}

wait_for_postgres_sink() {
  local expected_rows="$1"
  local expected_updated_rows="$2"
  local deadline=$((SECONDS + TIMEOUT_SECS))
  local total_rows=0
  local updated_rows=0
  while (( SECONDS < deadline )); do
    total_rows="$(postgres_sink_total_rows)"
    if (( expected_updated_rows > 0 )); then
      updated_rows="$(postgres_sink_updated_rows)"
    fi
    if (( total_rows >= expected_rows && updated_rows >= expected_updated_rows )); then
      sink_observed_rows="${total_rows}"
      postgres_sink_updated_rows_observed="${updated_rows}"
      return 0
    fi
    sleep 0.2
  done
  sink_observed_rows="${total_rows}"
  postgres_sink_updated_rows_observed="${updated_rows}"
  echo "Postgres sink observed ${total_rows} rows and ${updated_rows} updated rows; expected ${expected_rows} rows and ${expected_updated_rows} updated rows before timeout" >&2
  return 1
}

copy_pipe_delimited_file() {
  local table="$1"
  local file="$2"
  sed 's/|$//' "${file}" | docker exec -i "${POSTGRES_CONTAINER}" psql \
    -v ON_ERROR_STOP=1 \
    -U "${POSTGRES_USER}" \
    -d "${POSTGRES_DB}" \
    -c "\\copy ${table} FROM STDIN WITH (FORMAT csv, DELIMITER '|', QUOTE E'\\b', ESCAPE E'\\b')" >/dev/null
}

prepare_tpch_data_dir() {
  mkdir -p "${TPCH_DATA_DIR}"
  for table in "$@"; do
    rm -f "${TPCH_DATA_DIR}/${table}.tbl"
  done
}

sleep_live_write_pause() {
  if (( LIVE_WRITE_SLEEP_MS <= 0 )); then
    return
  fi
  sleep "$(awk "BEGIN { printf \"%.3f\", ${LIVE_WRITE_SLEEP_MS} / 1000 }")"
}

load_synthetic_orders_dataset() {
  local initial_rows="$1"
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
FROM generate_series(1, ${initial_rows}) AS gs;
SQL
}

load_tpch_lineitem_flat_dataset() {
  if [[ "${BENCH_MODE}" != "snapshot" ]]; then
    echo "DATASET=tpch-lineitem-flat currently supports BENCH_MODE=snapshot only" >&2
    exit 1
  fi
  require_cmd "${TPCHGEN_BIN}"
  prepare_tpch_data_dir lineitem
  "${TPCHGEN_BIN}" \
    --scale-factor "${TPCH_SCALE_FACTOR}" \
    --tables lineitem \
    --format tbl \
    --output-dir "${TPCH_DATA_DIR}" >/dev/null

  docker exec -i "${POSTGRES_CONTAINER}" psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null <<SQL
DROP PUBLICATION IF EXISTS ${PUBLICATION};
DROP TABLE IF EXISTS public.lineitem_flat_stage;
DROP TABLE IF EXISTS public.lineitem_flat;
CREATE TABLE public.lineitem_flat_stage (
  l_orderkey TEXT NOT NULL,
  l_partkey TEXT NOT NULL,
  l_suppkey TEXT NOT NULL,
  l_linenumber TEXT NOT NULL,
  l_quantity TEXT NOT NULL,
  l_extendedprice TEXT NOT NULL,
  l_discount TEXT NOT NULL,
  l_tax TEXT NOT NULL,
  l_returnflag TEXT NOT NULL,
  l_linestatus TEXT NOT NULL,
  l_shipdate TEXT NOT NULL,
  l_commitdate TEXT NOT NULL,
  l_receiptdate TEXT NOT NULL,
  l_shipinstruct TEXT NOT NULL,
  l_shipmode TEXT NOT NULL,
  l_comment TEXT NOT NULL
);
SQL

  copy_pipe_delimited_file public.lineitem_flat_stage "${TPCH_DATA_DIR}/lineitem.tbl"

  docker exec -i "${POSTGRES_CONTAINER}" psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null <<SQL
CREATE TABLE public.lineitem_flat (
  l_orderkey BIGINT NOT NULL,
  l_partkey BIGINT NOT NULL,
  l_suppkey BIGINT NOT NULL,
  l_linenumber BIGINT NOT NULL,
  l_quantity BIGINT NOT NULL,
  l_extendedprice_cents BIGINT NOT NULL,
  l_discount_bps BIGINT NOT NULL,
  l_tax_bps BIGINT NOT NULL,
  l_returnflag TEXT NOT NULL,
  l_linestatus TEXT NOT NULL,
  l_shipdate_days BIGINT NOT NULL,
  l_commitdate_days BIGINT NOT NULL,
  l_receiptdate_days BIGINT NOT NULL,
  l_shipinstruct TEXT NOT NULL,
  l_shipmode TEXT NOT NULL,
  l_comment TEXT NOT NULL,
  PRIMARY KEY (l_orderkey, l_linenumber)
);
INSERT INTO public.lineitem_flat
SELECT
  l_orderkey::BIGINT,
  l_partkey::BIGINT,
  l_suppkey::BIGINT,
  l_linenumber::BIGINT,
  ROUND(l_quantity::NUMERIC)::BIGINT,
  ROUND(l_extendedprice::NUMERIC * 100)::BIGINT,
  ROUND(l_discount::NUMERIC * 10000)::BIGINT,
  ROUND(l_tax::NUMERIC * 10000)::BIGINT,
  l_returnflag,
  l_linestatus,
  (l_shipdate::DATE - DATE '1970-01-01')::BIGINT,
  (l_commitdate::DATE - DATE '1970-01-01')::BIGINT,
  (l_receiptdate::DATE - DATE '1970-01-01')::BIGINT,
  l_shipinstruct,
  l_shipmode,
  l_comment
FROM public.lineitem_flat_stage;
DROP TABLE public.lineitem_flat_stage;
SQL
}

load_tpch_lineitem_dataset() {
  if [[ "${BENCH_MODE}" != "snapshot" ]]; then
    echo "DATASET=tpch-lineitem currently supports BENCH_MODE=snapshot only" >&2
    exit 1
  fi
  require_cmd "${TPCHGEN_BIN}"
  prepare_tpch_data_dir lineitem
  "${TPCHGEN_BIN}" \
    --scale-factor "${TPCH_SCALE_FACTOR}" \
    --tables lineitem \
    --format tbl \
    --output-dir "${TPCH_DATA_DIR}" >/dev/null

  docker exec -i "${POSTGRES_CONTAINER}" psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null <<SQL
DROP PUBLICATION IF EXISTS ${PUBLICATION};
DROP TABLE IF EXISTS public.lineitem;
CREATE TABLE public.lineitem (
  l_orderkey BIGINT NOT NULL,
  l_partkey BIGINT NOT NULL,
  l_suppkey BIGINT NOT NULL,
  l_linenumber BIGINT NOT NULL,
  l_quantity NUMERIC(15,2) NOT NULL,
  l_extendedprice NUMERIC(15,2) NOT NULL,
  l_discount NUMERIC(15,2) NOT NULL,
  l_tax NUMERIC(15,2) NOT NULL,
  l_returnflag CHAR(1) NOT NULL,
  l_linestatus CHAR(1) NOT NULL,
  l_shipdate DATE NOT NULL,
  l_commitdate DATE NOT NULL,
  l_receiptdate DATE NOT NULL,
  l_shipinstruct CHAR(25) NOT NULL,
  l_shipmode CHAR(10) NOT NULL,
  l_comment VARCHAR(44) NOT NULL,
  PRIMARY KEY (l_orderkey, l_linenumber)
);
SQL

  copy_pipe_delimited_file public.lineitem "${TPCH_DATA_DIR}/lineitem.tbl"
}

create_tpch_top2_tables() {
  docker exec -i "${POSTGRES_CONTAINER}" psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null <<SQL
DROP PUBLICATION IF EXISTS ${PUBLICATION};
DROP TABLE IF EXISTS public.lineitem;
DROP TABLE IF EXISTS public.orders;

CREATE TABLE public.orders (
  o_orderkey BIGINT PRIMARY KEY,
  o_custkey BIGINT NOT NULL,
  o_orderstatus CHAR(1) NOT NULL,
  o_totalprice NUMERIC(15,2) NOT NULL,
  o_orderdate DATE NOT NULL,
  o_orderpriority CHAR(15) NOT NULL,
  o_clerk CHAR(15) NOT NULL,
  o_shippriority BIGINT NOT NULL,
  o_comment VARCHAR(79) NOT NULL
);

CREATE TABLE public.lineitem (
  l_orderkey BIGINT NOT NULL,
  l_partkey BIGINT NOT NULL,
  l_suppkey BIGINT NOT NULL,
  l_linenumber BIGINT NOT NULL,
  l_quantity NUMERIC(15,2) NOT NULL,
  l_extendedprice NUMERIC(15,2) NOT NULL,
  l_discount NUMERIC(15,2) NOT NULL,
  l_tax NUMERIC(15,2) NOT NULL,
  l_returnflag CHAR(1) NOT NULL,
  l_linestatus CHAR(1) NOT NULL,
  l_shipdate DATE NOT NULL,
  l_commitdate DATE NOT NULL,
  l_receiptdate DATE NOT NULL,
  l_shipinstruct CHAR(25) NOT NULL,
  l_shipmode CHAR(10) NOT NULL,
  l_comment VARCHAR(44) NOT NULL,
  PRIMARY KEY (l_orderkey, l_linenumber)
);
SQL
}

load_tpch_top2_dataset() {
  if [[ "${BENCH_MODE}" == "snapshot_live_update" ]]; then
    echo "DATASET=tpch-top2 currently supports BENCH_MODE=snapshot or live_insert" >&2
    exit 1
  fi

  create_tpch_top2_tables
  if [[ "${BENCH_MODE}" == "live_insert" ]]; then
    return
  fi

  require_cmd "${TPCHGEN_BIN}"
  prepare_tpch_data_dir orders lineitem
  "${TPCHGEN_BIN}" \
    --scale-factor "${TPCH_SCALE_FACTOR}" \
    --tables orders,lineitem \
    --format tbl \
    --output-dir "${TPCH_DATA_DIR}" >/dev/null

  copy_pipe_delimited_file public.orders "${TPCH_DATA_DIR}/orders.tbl"
  copy_pipe_delimited_file public.lineitem "${TPCH_DATA_DIR}/lineitem.tbl"
}

load_tpch_all_dataset() {
  if [[ "${BENCH_MODE}" != "snapshot" ]]; then
    echo "DATASET=tpch-all currently supports BENCH_MODE=snapshot only" >&2
    exit 1
  fi
  require_cmd "${TPCHGEN_BIN}"
  prepare_tpch_data_dir region nation supplier customer part partsupp orders lineitem
  "${TPCHGEN_BIN}" \
    --scale-factor "${TPCH_SCALE_FACTOR}" \
    --format tbl \
    --output-dir "${TPCH_DATA_DIR}" >/dev/null

  docker exec -i "${POSTGRES_CONTAINER}" psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null <<SQL
DROP PUBLICATION IF EXISTS ${PUBLICATION};
DROP TABLE IF EXISTS public.lineitem;
DROP TABLE IF EXISTS public.orders;
DROP TABLE IF EXISTS public.partsupp;
DROP TABLE IF EXISTS public.part;
DROP TABLE IF EXISTS public.customer;
DROP TABLE IF EXISTS public.supplier;
DROP TABLE IF EXISTS public.nation;
DROP TABLE IF EXISTS public.region;

CREATE TABLE public.region (
  r_regionkey BIGINT PRIMARY KEY,
  r_name CHAR(25) NOT NULL,
  r_comment VARCHAR(152) NOT NULL
);

CREATE TABLE public.nation (
  n_nationkey BIGINT PRIMARY KEY,
  n_name CHAR(25) NOT NULL,
  n_regionkey BIGINT NOT NULL,
  n_comment VARCHAR(152) NOT NULL
);

CREATE TABLE public.supplier (
  s_suppkey BIGINT PRIMARY KEY,
  s_name CHAR(25) NOT NULL,
  s_address VARCHAR(40) NOT NULL,
  s_nationkey BIGINT NOT NULL,
  s_phone CHAR(15) NOT NULL,
  s_acctbal NUMERIC(15,2) NOT NULL,
  s_comment VARCHAR(101) NOT NULL
);

CREATE TABLE public.customer (
  c_custkey BIGINT PRIMARY KEY,
  c_name VARCHAR(25) NOT NULL,
  c_address VARCHAR(40) NOT NULL,
  c_nationkey BIGINT NOT NULL,
  c_phone CHAR(15) NOT NULL,
  c_acctbal NUMERIC(15,2) NOT NULL,
  c_mktsegment CHAR(10) NOT NULL,
  c_comment VARCHAR(117) NOT NULL
);

CREATE TABLE public.part (
  p_partkey BIGINT PRIMARY KEY,
  p_name VARCHAR(55) NOT NULL,
  p_mfgr CHAR(25) NOT NULL,
  p_brand CHAR(10) NOT NULL,
  p_type VARCHAR(25) NOT NULL,
  p_size BIGINT NOT NULL,
  p_container CHAR(10) NOT NULL,
  p_retailprice NUMERIC(15,2) NOT NULL,
  p_comment VARCHAR(23) NOT NULL
);

CREATE TABLE public.partsupp (
  ps_partkey BIGINT NOT NULL,
  ps_suppkey BIGINT NOT NULL,
  ps_availqty BIGINT NOT NULL,
  ps_supplycost NUMERIC(15,2) NOT NULL,
  ps_comment VARCHAR(199) NOT NULL,
  PRIMARY KEY (ps_partkey, ps_suppkey)
);

CREATE TABLE public.orders (
  o_orderkey BIGINT PRIMARY KEY,
  o_custkey BIGINT NOT NULL,
  o_orderstatus CHAR(1) NOT NULL,
  o_totalprice NUMERIC(15,2) NOT NULL,
  o_orderdate DATE NOT NULL,
  o_orderpriority CHAR(15) NOT NULL,
  o_clerk CHAR(15) NOT NULL,
  o_shippriority BIGINT NOT NULL,
  o_comment VARCHAR(79) NOT NULL
);

CREATE TABLE public.lineitem (
  l_orderkey BIGINT NOT NULL,
  l_partkey BIGINT NOT NULL,
  l_suppkey BIGINT NOT NULL,
  l_linenumber BIGINT NOT NULL,
  l_quantity NUMERIC(15,2) NOT NULL,
  l_extendedprice NUMERIC(15,2) NOT NULL,
  l_discount NUMERIC(15,2) NOT NULL,
  l_tax NUMERIC(15,2) NOT NULL,
  l_returnflag CHAR(1) NOT NULL,
  l_linestatus CHAR(1) NOT NULL,
  l_shipdate DATE NOT NULL,
  l_commitdate DATE NOT NULL,
  l_receiptdate DATE NOT NULL,
  l_shipinstruct CHAR(25) NOT NULL,
  l_shipmode CHAR(10) NOT NULL,
  l_comment VARCHAR(44) NOT NULL,
  PRIMARY KEY (l_orderkey, l_linenumber)
);
SQL

  copy_pipe_delimited_file public.region "${TPCH_DATA_DIR}/region.tbl"
  copy_pipe_delimited_file public.nation "${TPCH_DATA_DIR}/nation.tbl"
  copy_pipe_delimited_file public.supplier "${TPCH_DATA_DIR}/supplier.tbl"
  copy_pipe_delimited_file public.customer "${TPCH_DATA_DIR}/customer.tbl"
  copy_pipe_delimited_file public.part "${TPCH_DATA_DIR}/part.tbl"
  copy_pipe_delimited_file public.partsupp "${TPCH_DATA_DIR}/partsupp.tbl"
  copy_pipe_delimited_file public.orders "${TPCH_DATA_DIR}/orders.tbl"
  copy_pipe_delimited_file public.lineitem "${TPCH_DATA_DIR}/lineitem.tbl"
}

tpch_top2_chunk_orders() {
  local chunk_rows="$1"
  local orders=$(((chunk_rows + 4) / 5))
  if (( orders > chunk_rows )); then
    orders="${chunk_rows}"
  fi
  echo "${orders}"
}

write_live_inserts() {
  local total="$1"
  local chunk="${LIVE_WRITE_CHUNK_ROWS}"
  if (( chunk <= 0 || chunk > total )); then
    chunk="${total}"
  fi
  local start=1
  while (( start <= total )); do
    local end=$((start + chunk - 1))
    if (( end > total )); then
      end="${total}"
    fi
    docker exec -i "${POSTGRES_CONTAINER}" psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null <<SQL
INSERT INTO public.orders
SELECT
  gs::BIGINT AS id,
  (gs % 100000)::BIGINT AS customer_id,
  (100 + (gs % 10000))::BIGINT AS amount,
  CASE WHEN gs % 3 = 0 THEN 'paid' WHEN gs % 3 = 1 THEN 'open' ELSE 'cancelled' END AS status,
  (1700000000000 + gs)::BIGINT AS created_at
FROM generate_series(${start}, ${end}) AS gs;
SQL
    start=$((end + 1))
    if (( start <= total )); then
      sleep_live_write_pause
    fi
  done
}

write_live_tpch_top2_inserts() {
  local total="$1"
  local chunk="${LIVE_WRITE_CHUNK_ROWS}"
  if (( chunk <= 0 || chunk > total )); then
    chunk="${total}"
  fi
  local remaining="${total}"
  local next_order_key=1
  local next_lineitem_idx=1
  while (( remaining > 0 )); do
    local chunk_rows="${chunk}"
    if (( chunk_rows > remaining )); then
      chunk_rows="${remaining}"
    fi
    local order_rows
    order_rows="$(tpch_top2_chunk_orders "${chunk_rows}")"
    local lineitem_rows=$((chunk_rows - order_rows))
    local order_start="${next_order_key}"
    local order_end=$((order_start + order_rows - 1))
    local lineitem_start="${next_lineitem_idx}"
    local lineitem_end=$((lineitem_start + lineitem_rows - 1))

    docker exec -i "${POSTGRES_CONTAINER}" psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null <<SQL
BEGIN;
INSERT INTO public.orders
SELECT
  gs::BIGINT AS o_orderkey,
  ((gs % 150000) + 1)::BIGINT AS o_custkey,
  'O'::CHAR(1) AS o_orderstatus,
  ((100000 + (gs % 100000))::NUMERIC / 100)::NUMERIC(15,2) AS o_totalprice,
  (DATE '1992-01-01' + ((gs % 2500)::INT))::DATE AS o_orderdate,
  '5-LOW'::CHAR(15) AS o_orderpriority,
  ('Clerk#' || LPAD((gs % 1000)::TEXT, 9, '0'))::CHAR(15) AS o_clerk,
  0::BIGINT AS o_shippriority,
  ('live order ' || gs)::VARCHAR(79) AS o_comment
FROM generate_series(${order_start}, ${order_end}) AS gs;

INSERT INTO public.lineitem
SELECT
  (((gs - 1) / 4) + 1)::BIGINT AS l_orderkey,
  ((gs % 200000) + 1)::BIGINT AS l_partkey,
  ((gs % 10000) + 1)::BIGINT AS l_suppkey,
  (((gs - 1) % 4) + 1)::BIGINT AS l_linenumber,
  ((1 + (gs % 50))::NUMERIC)::NUMERIC(15,2) AS l_quantity,
  ((10000 + (gs % 100000))::NUMERIC / 100)::NUMERIC(15,2) AS l_extendedprice,
  ((gs % 10)::NUMERIC / 100)::NUMERIC(15,2) AS l_discount,
  ((gs % 8)::NUMERIC / 100)::NUMERIC(15,2) AS l_tax,
  'N'::CHAR(1) AS l_returnflag,
  'O'::CHAR(1) AS l_linestatus,
  (DATE '1992-01-01' + ((gs % 2500)::INT))::DATE AS l_shipdate,
  (DATE '1992-01-02' + ((gs % 2500)::INT))::DATE AS l_commitdate,
  (DATE '1992-01-03' + ((gs % 2500)::INT))::DATE AS l_receiptdate,
  'DELIVER IN PERSON'::CHAR(25) AS l_shipinstruct,
  'AIR'::CHAR(10) AS l_shipmode,
  ('live lineitem ' || gs)::VARCHAR(44) AS l_comment
FROM generate_series(${lineitem_start}, ${lineitem_end}) AS gs;
COMMIT;
SQL

    next_order_key=$((order_end + 1))
    next_lineitem_idx=$((lineitem_end + 1))
    remaining=$((remaining - chunk_rows))
    if (( remaining > 0 )); then
      sleep_live_write_pause
    fi
  done
}

write_live_updates() {
  local total="$1"
  local chunk="${LIVE_WRITE_CHUNK_ROWS}"
  if (( chunk <= 0 || chunk > total )); then
    chunk="${total}"
  fi
  local start=1
  while (( start <= total )); do
    local end=$((start + chunk - 1))
    if (( end > total )); then
      end="${total}"
    fi
    docker exec -i "${POSTGRES_CONTAINER}" psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null <<SQL
UPDATE public.orders
SET amount = amount + 1,
    status = 'updated'
WHERE id BETWEEN ${start} AND ${end};
SQL
    start=$((end + 1))
    if (( start <= total )); then
      sleep_live_write_pause
    fi
  done
}

expected_insert_messages() {
  local rows="$1"
  local normalized_format="${PIPELINE_FORMAT//-/_}"
  case "${normalized_format}" in
    debezium_json)
      echo "${rows}"
      ;;
    floe_json|compact_json)
      echo "${rows}"
      ;;
    arrow_ipc)
      echo "$(( (rows + ARROW_IPC_ROWS_PER_RECORD - 1) / ARROW_IPC_ROWS_PER_RECORD ))"
      ;;
    *)
      echo "unsupported PIPELINE_FORMAT=${PIPELINE_FORMAT}" >&2
      exit 1
      ;;
  esac
}

expected_insert_messages_for_chunks() {
  local rows="$1"
  local chunk="$2"
  if (( chunk <= 0 || chunk >= rows )); then
    expected_insert_messages "${rows}"
    return
  fi
  local full_chunks=$((rows / chunk))
  local remainder=$((rows % chunk))
  local per_full_chunk
  per_full_chunk="$(expected_insert_messages "${chunk}")"
  local total=$((full_chunks * per_full_chunk))
  if (( remainder > 0 )); then
    total=$((total + $(expected_insert_messages "${remainder}")))
  fi
  echo "${total}"
}

expected_tpch_top2_live_insert_messages() {
  local rows="$1"
  local chunk="${2}"
  if (( chunk <= 0 || chunk > rows )); then
    chunk="${rows}"
  fi
  local remaining="${rows}"
  local total=0
  while (( remaining > 0 )); do
    local chunk_rows="${chunk}"
    if (( chunk_rows > remaining )); then
      chunk_rows="${remaining}"
    fi
    local order_rows
    order_rows="$(tpch_top2_chunk_orders "${chunk_rows}")"
    local lineitem_rows=$((chunk_rows - order_rows))
    total=$((total + $(expected_insert_messages "${order_rows}")))
    if (( lineitem_rows > 0 )); then
      total=$((total + $(expected_insert_messages "${lineitem_rows}")))
    fi
    remaining=$((remaining - chunk_rows))
  done
  echo "${total}"
}

expected_update_messages() {
  local rows="$1"
  local normalized_format="${PIPELINE_FORMAT//-/_}"
  case "${normalized_format}" in
    debezium_json)
      echo "${rows}"
      ;;
    floe_json|compact_json)
      echo "${rows}"
      ;;
    arrow_ipc)
      echo "$(( (rows * 2 + ARROW_IPC_ROWS_PER_RECORD - 1) / ARROW_IPC_ROWS_PER_RECORD ))"
      ;;
    *)
      echo "unsupported PIPELINE_FORMAT=${PIPELINE_FORMAT}" >&2
      exit 1
      ;;
  esac
}

expected_update_messages_for_chunks() {
  local rows="$1"
  local chunk="$2"
  if (( chunk <= 0 || chunk >= rows )); then
    expected_update_messages "${rows}"
    return
  fi
  local full_chunks=$((rows / chunk))
  local remainder=$((rows % chunk))
  local per_full_chunk
  per_full_chunk="$(expected_update_messages "${chunk}")"
  local total=$((full_chunks * per_full_chunk))
  if (( remainder > 0 )); then
    total=$((total + $(expected_update_messages "${remainder}")))
  fi
  echo "${total}"
}

write_replication_pipeline_sql() {
  local idx="$1"
  {
    echo "CREATE REPLICATION PIPELINE ${pipeline_names[$idx]}"
    echo "FROM pg_main TABLE '${upstream_tables[$idx]}'"
    if [[ "${TARGET_NORMALIZED}" == "kafka" ]]; then
      echo "INTO KAFKA WITH ("
      echo "  brokers = '${BROKERS}',"
      echo "  topic = '${topics[$idx]}',"
    else
      echo "INTO POSTGRES WITH ("
      echo "  connection = '${PG_DSN}',"
      echo "  table = '${target_tables[$idx]}',"
    fi
    echo "  format = '${PIPELINE_FORMAT}',"
    echo "  durable_buffer = ${DURABLE_REPLICATION_BUFFER},"
    if [[ -n "${BUFFER_MAX_PENDING_BYTES}" ]]; then
      echo "  buffer.max_pending_bytes = ${BUFFER_MAX_PENDING_BYTES},"
    fi
    if [[ -n "${BUFFER_MAX_PENDING_RECORDS}" ]]; then
      echo "  buffer.max_pending_records = ${BUFFER_MAX_PENDING_RECORDS},"
    fi
    if [[ -n "${BUFFER_MAX_PENDING_OBJECTS}" ]]; then
      echo "  buffer.max_pending_objects = ${BUFFER_MAX_PENDING_OBJECTS},"
    fi
    if [[ -n "${BUFFER_MAX_PENDING_AGE_MS}" ]]; then
      echo "  buffer.max_pending_age_ms = ${BUFFER_MAX_PENDING_AGE_MS},"
    fi
    echo "  tombstones = false,"
    echo "  transaction_metadata = false"
    echo ");"
  } >>"${SQL_PATH}"
}

require_cmd docker
require_cmd cargo
require_cmd jq

upstream_tables=()
pipeline_names=()
topics=()
target_tables=()
table_row_counts=()

case "${DATASET}" in
  synthetic-orders)
    source_table="orders"
    upstream_table="public.orders"
    pipeline_name="pg_orders_to_kafka"
    upstream_tables=("public.orders")
    pipeline_names=("pg_orders_to_kafka")
    topics=("${TOPIC}")
    ;;
  tpch-lineitem-flat)
    source_table="lineitem_flat"
    upstream_table="public.lineitem_flat"
    pipeline_name="pg_lineitem_flat_to_kafka"
    if [[ "${TOPIC}" == "floe_cdc_bench_orders" ]]; then
      TOPIC="floe_cdc_bench_lineitem_flat"
    fi
    upstream_tables=("public.lineitem_flat")
    pipeline_names=("pg_lineitem_flat_to_kafka")
    topics=("${TOPIC}")
    ;;
  tpch-lineitem)
    source_table="lineitem"
    upstream_table="public.lineitem"
    pipeline_name="pg_lineitem_to_kafka"
    if [[ "${TOPIC}" == "floe_cdc_bench_orders" ]]; then
      TOPIC="floe_cdc_bench_lineitem"
    fi
    upstream_tables=("public.lineitem")
    pipeline_names=("pg_lineitem_to_kafka")
    topics=("${TOPIC}")
    ;;
  tpch-top2)
    source_table="orders,lineitem"
    upstream_table="public.orders,public.lineitem"
    pipeline_name="pg_orders_to_kafka,pg_lineitem_to_kafka"
    if [[ "${TOPIC}" == "floe_cdc_bench_orders" ]]; then
      TOPIC="floe_cdc_bench_tpch_top2"
    fi
    upstream_tables=(public.orders public.lineitem)
    pipeline_names=(pg_orders_to_kafka pg_lineitem_to_kafka)
    topics=("${TOPIC}_orders" "${TOPIC}_lineitem")
    ;;
  tpch-all)
    source_table="region,nation,supplier,customer,part,partsupp,orders,lineitem"
    upstream_table="public.region,public.nation,public.supplier,public.customer,public.part,public.partsupp,public.orders,public.lineitem"
    pipeline_name="pg_region_to_kafka,pg_nation_to_kafka,pg_supplier_to_kafka,pg_customer_to_kafka,pg_part_to_kafka,pg_partsupp_to_kafka,pg_orders_to_kafka,pg_lineitem_to_kafka"
    if [[ "${TOPIC}" == "floe_cdc_bench_orders" ]]; then
      TOPIC="floe_cdc_bench_tpch"
    fi
    upstream_tables=(public.region public.nation public.supplier public.customer public.part public.partsupp public.orders public.lineitem)
    pipeline_names=(pg_region_to_kafka pg_nation_to_kafka pg_supplier_to_kafka pg_customer_to_kafka pg_part_to_kafka pg_partsupp_to_kafka pg_orders_to_kafka pg_lineitem_to_kafka)
    topics=("${TOPIC}_region" "${TOPIC}_nation" "${TOPIC}_supplier" "${TOPIC}_customer" "${TOPIC}_part" "${TOPIC}_partsupp" "${TOPIC}_orders" "${TOPIC}_lineitem")
    ;;
  *)
    echo "unsupported DATASET=${DATASET} (expected synthetic-orders|tpch-lineitem-flat|tpch-lineitem|tpch-top2|tpch-all)" >&2
    exit 1
    ;;
esac
for idx in "${!upstream_tables[@]}"; do
  target_tables+=("$(postgres_target_table_for_upstream "${upstream_tables[$idx]}")")
  if [[ "${TARGET_NORMALIZED}" == "postgres" ]]; then
    pipeline_names[$idx]="${pipeline_names[$idx]/_to_kafka/_to_postgres}"
  fi
done
if [[ "${TARGET_NORMALIZED}" == "kafka" ]]; then
  topic_list="$(IFS=,; echo "${topics[*]}")"
else
  topic_list=""
fi
target_table_list="$(IFS=,; echo "${target_tables[*]}")"

tables_json="$(
  printf '%s\n' "${upstream_tables[@]}" \
    | jq -Rsc 'split("\n") | map(select(length > 0))'
)"
topics_json="$(
  if [[ -n "${topic_list}" ]]; then
    tr ',' '\n' <<<"${topic_list}"
  fi \
    | jq -Rsc 'split("\n") | map(select(length > 0))'
)"
target_tables_json="$(
  printf '%s\n' "${target_tables[@]}" \
    | jq -Rsc 'split("\n") | map(select(length > 0))'
)"

docker rm -f "${POSTGRES_CONTAINER}" "${REDPANDA_CONTAINER}" >/dev/null 2>&1 || true

echo "artifact_dir=${ARTIFACT_DIR}"
echo "rows=${ROWS}"
echo "dataset=${DATASET}"
echo "tpch_scale_factor=${TPCH_SCALE_FACTOR}"
echo "bench_mode=${BENCH_MODE}"
echo "target=${TARGET_NORMALIZED}"
echo "brokers=${BROKERS}"
echo "topic=${TOPIC}"
echo "topics=${topic_list}"
echo "postgres_sink_tables=${target_table_list}"
echo "pipeline_format=${PIPELINE_FORMAT}"
echo "durable_replication_buffer=${DURABLE_REPLICATION_BUFFER}"
echo "buffer_max_pending_bytes=${BUFFER_MAX_PENDING_BYTES:-unset}"
echo "buffer_max_pending_records=${BUFFER_MAX_PENDING_RECORDS:-unset}"
echo "buffer_max_pending_objects=${BUFFER_MAX_PENDING_OBJECTS:-unset}"
echo "buffer_max_pending_age_ms=${BUFFER_MAX_PENDING_AGE_MS:-unset}"
echo "arrow_ipc_compression=${ARROW_IPC_COMPRESSION}"
echo "kafka_metadata_headers=${KAFKA_METADATA_HEADERS}"
echo "redpanda_kafka_batch_max_bytes=${REDPANDA_KAFKA_BATCH_MAX_BYTES}"
echo "redpanda_topic_max_message_bytes=${REDPANDA_TOPIC_MAX_MESSAGE_BYTES}"
echo "live_write_chunk_rows=${LIVE_WRITE_CHUNK_ROWS}"
echo "live_write_sleep_ms=${LIVE_WRITE_SLEEP_MS}"
echo "postgres_snapshot_rows_per_batch=${FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH}"
echo "postgres_snapshot_max_workers=${FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS}"
echo "postgres_snapshot_intra_table_chunks=${FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS}"
echo "floe_pg_port=${FLOE_PG_PORT}"
echo "floe_admin_port=${FLOE_ADMIN_PORT}"
echo "slatedb_flush_interval_ms=${SLATEDB_FLUSH_INTERVAL_MS}"
write_reproduce_command

echo "Pulling images..."
docker pull "${POSTGRES_IMAGE}" >/dev/null
if [[ "${TARGET_NORMALIZED}" == "kafka" ]]; then
  docker pull "${REDPANDA_IMAGE}" >/dev/null
fi

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
write_system_context

if [[ "${TARGET_NORMALIZED}" == "kafka" ]]; then
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
      --set "redpanda.kafka_batch_max_bytes=${REDPANDA_KAFKA_BATCH_MAX_BYTES}" \
      --kafka-addr PLAINTEXT://0.0.0.0:9092 \
      --advertise-kafka-addr "PLAINTEXT://127.0.0.1:${REDPANDA_PORT}" >/dev/null
  wait_for_redpanda

  echo "Creating Kafka topics ${topic_list}"
  for topic in "${topics[@]}"; do
    if ! docker exec "${REDPANDA_CONTAINER}" rpk topic create "${topic}" -p 1 -r 1 \
      -c "max.message.bytes=${REDPANDA_TOPIC_MAX_MESSAGE_BYTES}" >/dev/null 2>&1; then
      docker exec "${REDPANDA_CONTAINER}" rpk topic create "${topic}" -p 1 -r 1 >/dev/null 2>&1 || true
    fi
    docker exec "${REDPANDA_CONTAINER}" rpk topic alter-config "${topic}" \
      --set "max.message.bytes=${REDPANDA_TOPIC_MAX_MESSAGE_BYTES}" >/dev/null 2>&1 || true
  done
  write_kafka_topic_info
else
  : >"${KAFKA_TOPIC_LOG}"
fi

case "${BENCH_MODE}" in
  snapshot|live_insert|snapshot_live_update) ;;
  *)
    echo "unsupported BENCH_MODE=${BENCH_MODE} (expected snapshot|live_insert|snapshot_live_update)" >&2
    exit 1
    ;;
esac

if [[ "${BENCH_MODE}" == "live_insert" ]]; then
  initial_rows=0
  live_insert_rows="${ROWS}"
  live_update_rows=0
else
  initial_rows="${ROWS}"
  live_insert_rows=0
  if [[ "${BENCH_MODE}" == "snapshot_live_update" ]]; then
    live_update_rows="${ROWS}"
  else
    live_update_rows=0
  fi
fi

echo "Loading Postgres dataset ${DATASET} with ${initial_rows} requested initial rows"
load_started_ns="$(date +%s%N)"
case "${DATASET}" in
  synthetic-orders)
    load_synthetic_orders_dataset "${initial_rows}"
    table_row_counts=("${initial_rows}")
    ;;
  tpch-lineitem-flat)
    load_tpch_lineitem_flat_dataset
    initial_rows="$(docker exec "${POSTGRES_CONTAINER}" psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM public.lineitem_flat")"
    table_row_counts=("${initial_rows}")
    ;;
  tpch-lineitem)
    load_tpch_lineitem_dataset
    initial_rows="$(docker exec "${POSTGRES_CONTAINER}" psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM public.lineitem")"
    table_row_counts=("${initial_rows}")
    ;;
  tpch-top2)
    load_tpch_top2_dataset
    if [[ "${BENCH_MODE}" == "live_insert" ]]; then
      table_row_counts=(0 0)
    else
      mapfile -t table_row_counts < <(docker exec "${POSTGRES_CONTAINER}" psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "
        SELECT row_count
        FROM (
          VALUES
            (1, (SELECT COUNT(*)::BIGINT FROM public.orders)),
            (2, (SELECT COUNT(*)::BIGINT FROM public.lineitem))
        ) AS counts(table_order, row_count)
        ORDER BY table_order;
      ")
      initial_rows=0
      for row_count in "${table_row_counts[@]}"; do
        initial_rows=$((initial_rows + row_count))
      done
    fi
    ;;
  tpch-all)
    load_tpch_all_dataset
    mapfile -t table_row_counts < <(docker exec "${POSTGRES_CONTAINER}" psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "
      SELECT row_count
      FROM (
        VALUES
          (1, (SELECT COUNT(*)::BIGINT FROM public.region)),
          (2, (SELECT COUNT(*)::BIGINT FROM public.nation)),
          (3, (SELECT COUNT(*)::BIGINT FROM public.supplier)),
          (4, (SELECT COUNT(*)::BIGINT FROM public.customer)),
          (5, (SELECT COUNT(*)::BIGINT FROM public.part)),
          (6, (SELECT COUNT(*)::BIGINT FROM public.partsupp)),
          (7, (SELECT COUNT(*)::BIGINT FROM public.orders)),
          (8, (SELECT COUNT(*)::BIGINT FROM public.lineitem))
      ) AS counts(table_order, row_count)
      ORDER BY table_order;
    ")
    initial_rows=0
    for row_count in "${table_row_counts[@]}"; do
      initial_rows=$((initial_rows + row_count))
    done
    ;;
esac
load_finished_ns="$(date +%s%N)"
load_seconds="$(awk "BEGIN { printf \"%.3f\", (${load_finished_ns} - ${load_started_ns}) / 1000000000 }")"
echo "timing.postgres_load_seconds=${load_seconds}"
write_postgres_settings
create_postgres_sink_tables
source_rows=$((initial_rows + live_insert_rows + live_update_rows))

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
SQL
for idx in "${!pipeline_names[@]}"; do
  write_replication_pipeline_sql "${idx}"
done

expected_messages=0
for row_count in "${table_row_counts[@]}"; do
  if (( row_count > 0 )); then
    expected_messages=$((expected_messages + $(expected_insert_messages "${row_count}")))
  fi
done
if (( live_insert_rows > 0 )); then
  if [[ "${DATASET}" == "tpch-top2" ]]; then
    expected_messages=$((expected_messages + $(expected_tpch_top2_live_insert_messages "${live_insert_rows}" "${LIVE_WRITE_CHUNK_ROWS}")))
  else
    expected_messages=$((expected_messages + $(expected_insert_messages_for_chunks "${live_insert_rows}" "${LIVE_WRITE_CHUNK_ROWS}")))
  fi
fi
if (( live_update_rows > 0 )); then
  expected_messages=$((expected_messages + $(expected_update_messages_for_chunks "${live_update_rows}" "${LIVE_WRITE_CHUNK_ROWS}")))
fi
echo "expected_kafka_messages=${expected_messages}"
expected_kafka_messages_report="${expected_messages}"
expected_sink_rows=""
expected_postgres_updated_rows=0
if [[ "${TARGET_NORMALIZED}" == "postgres" ]]; then
  expected_kafka_messages_report=""
  expected_sink_rows=$((initial_rows + live_insert_rows))
  if [[ "${DATASET}" == "synthetic-orders" && "${BENCH_MODE}" == "snapshot_live_update" ]]; then
    expected_postgres_updated_rows="${live_update_rows}"
  fi
  echo "expected_postgres_sink_rows=${expected_sink_rows}"
  echo "expected_postgres_sink_updated_rows=${expected_postgres_updated_rows}"
fi

SLATEDB_ARGS=(
  --slatedb-await-durable=false
  --slatedb-flush-interval-ms "${SLATEDB_FLUSH_INTERVAL_MS}"
)

echo "Starting Floe node"
node_started_ns="$(date +%s%N)"
if command -v setsid >/dev/null 2>&1 && command -v /usr/bin/time >/dev/null 2>&1; then
	node_process_group=1
	setsid /usr/bin/time -v -o "${NODE_RESOURCE_LOG}" \
	  "${FLOE_BIN}" run \
      --config "${CONFIG_PATH}" \
      --mv-query "$(cat "${SQL_PATH}")" \
      "${SLATEDB_ARGS[@]}" \
      --ingest-batch-size 16384 \
      --ingest-batch-per-source 16384 \
      --ingest-batch-per-connector 16384 \
      >"${NODE_STDOUT}" 2>"${NODE_STDERR}" &
elif command -v setsid >/dev/null 2>&1; then
	node_process_group=1
	setsid \
	  "${FLOE_BIN}" run \
      --config "${CONFIG_PATH}" \
      --mv-query "$(cat "${SQL_PATH}")" \
      "${SLATEDB_ARGS[@]}" \
      --ingest-batch-size 16384 \
      --ingest-batch-per-source 16384 \
      --ingest-batch-per-connector 16384 \
      >"${NODE_STDOUT}" 2>"${NODE_STDERR}" &
else
	node_process_group=0
	"${FLOE_BIN}" run \
    --config "${CONFIG_PATH}" \
    --mv-query "$(cat "${SQL_PATH}")" \
    "${SLATEDB_ARGS[@]}" \
    --ingest-batch-size 16384 \
    --ingest-batch-per-source 16384 \
    --ingest-batch-per-connector 16384 \
    >"${NODE_STDOUT}" 2>"${NODE_STDERR}" &
fi
node_pid="$!"

counter_started_ns=""
counter_pid=""
sink_wait_started_ns=""
if [[ "${TARGET_NORMALIZED}" == "kafka" ]]; then
  echo "Counting CDC records from Kafka"
  counter_started_ns="$(date +%s%N)"
  "${COUNTER_BIN}" \
    --brokers "${BROKERS}" \
    --topics "${topic_list}" \
    --expected "${expected_messages}" \
    --timeout-secs "${TIMEOUT_SECS}" >"${COUNTER_LOG}" 2>&1 &
  counter_pid="$!"
else
  : >"${COUNTER_LOG}"
  sink_wait_started_ns="$(date +%s%N)"
fi

live_write_seconds="0.000"
if [[ "${BENCH_MODE}" == "live_insert" ]]; then
  echo "Waiting for Postgres CDC replication slot ${SLOT} to become active"
  wait_for_postgres_slot_active
  echo "Writing ${live_insert_rows} live insert rows"
  live_started_ns="$(date +%s%N)"
  if [[ "${DATASET}" == "tpch-top2" ]]; then
    write_live_tpch_top2_inserts "${live_insert_rows}"
  else
    write_live_inserts "${live_insert_rows}"
  fi
  live_finished_ns="$(date +%s%N)"
  live_write_seconds="$(awk "BEGIN { printf \"%.3f\", (${live_finished_ns} - ${live_started_ns}) / 1000000000 }")"
elif [[ "${BENCH_MODE}" == "snapshot_live_update" ]]; then
  echo "Waiting for Postgres CDC replication slot ${SLOT} to become active"
  wait_for_postgres_slot_active
  echo "Writing ${live_update_rows} live update rows"
  live_started_ns="$(date +%s%N)"
  write_live_updates "${live_update_rows}"
  live_finished_ns="$(date +%s%N)"
  live_write_seconds="$(awk "BEGIN { printf \"%.3f\", (${live_finished_ns} - ${live_started_ns}) / 1000000000 }")"
fi

sink_wait_seconds=""
sink_rows_per_second=""
sink_observed_rows=""
postgres_sink_updated_rows_observed=""
if [[ "${TARGET_NORMALIZED}" == "kafka" ]]; then
  if ! wait "${counter_pid}"; then
    cat "${COUNTER_LOG}" >&2 || true
    exit 1
  fi
  cat "${COUNTER_LOG}"
else
  echo "Waiting for Postgres sink tables ${target_table_list}"
  if ! wait_for_postgres_sink "${expected_sink_rows}" "${expected_postgres_updated_rows}"; then
    tail -80 "${NODE_STDERR}" >&2 || true
    exit 1
  fi
  sink_wait_finished_ns="$(date +%s%N)"
  sink_wait_seconds="$(awk "BEGIN { printf \"%.3f\", (${sink_wait_finished_ns} - ${sink_wait_started_ns}) / 1000000000 }")"
  sink_rows_per_second="$(awk "BEGIN { printf \"%.0f\", ${source_rows} / (${sink_wait_seconds} > 0.001 ? ${sink_wait_seconds} : 0.001) }")"
fi
node_finished_ns="$(date +%s%N)"
write_postgres_slot_info
write_docker_stats

end_to_end_seconds="$(awk "BEGIN { printf \"%.3f\", (${node_finished_ns} - ${node_started_ns}) / 1000000000 }")"
if [[ -n "${counter_started_ns}" ]]; then
  counter_seconds="$(awk "BEGIN { printf \"%.3f\", (${node_finished_ns} - ${counter_started_ns}) / 1000000000 }")"
else
  counter_seconds=""
fi
end_to_end_rows_per_second="$(awk "BEGIN { printf \"%.0f\", ${source_rows} / ${end_to_end_seconds} }")"
kafka_counter_wall_seconds="$(awk -F= '$1 == "cdc_counter.wall_seconds" { print $2; exit }' "${COUNTER_LOG}")"
kafka_pre_stream_wait_seconds="$(awk -F= '$1 == "cdc_counter.pre_stream_wait_seconds" { print $2; exit }' "${COUNTER_LOG}")"
kafka_stream_seconds="$(awk -F= '$1 == "cdc_counter.stream_seconds" { print $2; exit }' "${COUNTER_LOG}")"
kafka_post_stream_wait_seconds="$(awk -F= '$1 == "cdc_counter.post_stream_wait_seconds" { print $2; exit }' "${COUNTER_LOG}")"
kafka_stream_rows_per_second="$(awk -F= '$1 == "cdc_counter.stream_rows_per_second" { print $2; exit }' "${COUNTER_LOG}")"
kafka_stream_mb_per_second="$(awk -F= '$1 == "cdc_counter.stream_mb_per_second" { print $2; exit }' "${COUNTER_LOG}")"
observed_kafka_messages="$(awk -F= '$1 == "cdc_counter.observed_messages" { print $2; exit }' "${COUNTER_LOG}")"
kafka_key_bytes="$(awk -F= '$1 == "cdc_counter.key_bytes" { print $2; exit }' "${COUNTER_LOG}")"
kafka_value_bytes="$(awk -F= '$1 == "cdc_counter.value_bytes" { print $2; exit }' "${COUNTER_LOG}")"
kafka_total_bytes="$(awk -F= '$1 == "cdc_counter.total_bytes" { print $2; exit }' "${COUNTER_LOG}")"
kafka_wall_mb_per_second="$(awk -F= '$1 == "cdc_counter.wall_mb_per_second" { print $2; exit }' "${COUNTER_LOG}")"
consumer_wall_source_rows_per_second=""
kafka_stream_source_rows_per_second=""
harness_overhead_percent=""
message_multiplier=""
postgres_load_rows_per_second=""
postgres_live_write_rows_per_second=""
if [[ -n "${kafka_stream_seconds}" ]]; then
  kafka_stream_source_rows_per_second="$(awk "BEGIN { printf \"%.0f\", ${source_rows} / ${kafka_stream_seconds} }")"
  harness_overhead_seconds="$(awk "BEGIN { value = ${end_to_end_seconds} - ${kafka_stream_seconds}; if (value < 0) value = 0; printf \"%.3f\", value }")"
  harness_overhead_percent="$(awk "BEGIN { printf \"%.1f\", (${harness_overhead_seconds} / ${end_to_end_seconds}) * 100 }")"
else
  harness_overhead_seconds=""
fi
if [[ -n "${kafka_counter_wall_seconds}" ]]; then
  consumer_wall_source_rows_per_second="$(awk "BEGIN { printf \"%.0f\", ${source_rows} / ${kafka_counter_wall_seconds} }")"
fi
if (( source_rows > 0 )); then
  message_multiplier="$(awk "BEGIN { printf \"%.3f\", ${expected_messages} / ${source_rows} }")"
fi
if (( initial_rows > 0 )); then
  postgres_load_rows_per_second="$(awk "BEGIN { printf \"%.0f\", ${initial_rows} / (${load_seconds} > 0.001 ? ${load_seconds} : 0.001) }")"
fi
live_rows=$((live_insert_rows + live_update_rows))
if (( live_rows > 0 )); then
  postgres_live_write_rows_per_second="$(awk "BEGIN { printf \"%.0f\", ${live_rows} / (${live_write_seconds} > 0.001 ? ${live_write_seconds} : 0.001) }")"
fi
target_observation_seconds=""
target_observed_records=""
target_observed_records_per_second=""
case "${TARGET_NORMALIZED}" in
  kafka)
    target_observation_seconds="${kafka_counter_wall_seconds}"
    target_observed_records="${observed_kafka_messages}"
    target_observed_records_per_second="${consumer_wall_source_rows_per_second}"
    ;;
  postgres)
    target_observation_seconds="${sink_wait_seconds}"
    target_observed_records="${sink_observed_rows}"
    target_observed_records_per_second="${sink_rows_per_second}"
    ;;
esac

row_counts_json="$(
  for idx in "${!upstream_tables[@]}"; do
    printf '%s=%s\n' "${upstream_tables[$idx]}" "${table_row_counts[$idx]:-0}"
  done \
    | jq -Rsc '
        split("\n")
        | map(select(length > 0))
        | map(capture("(?<table>[^=]+)=(?<rows>.*)") | .rows |= tonumber)
      '
)"

write_summary_json() {
  jq -n \
    --arg run_id "${RUN_ID}" \
    --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg git_commit "$(git rev-parse HEAD 2>/dev/null || true)" \
    --arg git_branch "$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)" \
    --arg build_profile "$([[ "${BUILD_RELEASE}" == "1" ]] && printf release || printf debug)" \
    --arg dataset "${DATASET}" \
    --arg tpch_scale_factor "${TPCH_SCALE_FACTOR}" \
    --arg rows "${ROWS}" \
    --arg mode "${BENCH_MODE}" \
    --arg source_table "${source_table}" \
    --arg upstream_table "${upstream_table}" \
    --argjson upstream_tables "${tables_json}" \
    --arg target "${TARGET_NORMALIZED}" \
    --argjson kafka_topics "${topics_json}" \
    --argjson postgres_sink_tables "${target_tables_json}" \
    --arg pipeline_format "${PIPELINE_FORMAT}" \
    --arg durable_replication_buffer "${DURABLE_REPLICATION_BUFFER}" \
    --arg buffer_max_pending_bytes "${BUFFER_MAX_PENDING_BYTES}" \
    --arg buffer_max_pending_records "${BUFFER_MAX_PENDING_RECORDS}" \
    --arg buffer_max_pending_objects "${BUFFER_MAX_PENDING_OBJECTS}" \
    --arg buffer_max_pending_age_ms "${BUFFER_MAX_PENDING_AGE_MS}" \
    --arg arrow_ipc_rows_per_record "${ARROW_IPC_ROWS_PER_RECORD}" \
    --arg arrow_ipc_compression "${ARROW_IPC_COMPRESSION}" \
    --arg kafka_metadata_headers "${KAFKA_METADATA_HEADERS}" \
    --arg postgres_snapshot_rows_per_batch "${FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH}" \
    --arg postgres_snapshot_max_workers "${FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS}" \
    --arg postgres_snapshot_intra_table_chunks "${FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS}" \
    --arg floe_pg_port "${FLOE_PG_PORT}" \
    --arg floe_admin_port "${FLOE_ADMIN_PORT}" \
    --arg redpanda_kafka_batch_max_bytes "${REDPANDA_KAFKA_BATCH_MAX_BYTES}" \
    --arg redpanda_topic_max_message_bytes "${REDPANDA_TOPIC_MAX_MESSAGE_BYTES}" \
    --arg live_write_chunk_rows "${LIVE_WRITE_CHUNK_ROWS}" \
    --arg live_write_sleep_ms "${LIVE_WRITE_SLEEP_MS}" \
    --arg slatedb_flush_interval_ms "${SLATEDB_FLUSH_INTERVAL_MS}" \
    --arg initial_rows "${initial_rows}" \
    --arg live_insert_rows "${live_insert_rows}" \
    --arg live_update_rows "${live_update_rows}" \
    --arg source_rows "${source_rows}" \
    --arg expected_kafka_messages "${expected_kafka_messages_report}" \
    --arg observed_kafka_messages "${observed_kafka_messages}" \
    --arg expected_sink_rows "${expected_sink_rows}" \
    --arg sink_observed_rows "${sink_observed_rows}" \
    --arg expected_postgres_updated_rows "${expected_postgres_updated_rows}" \
    --arg postgres_sink_updated_rows_observed "${postgres_sink_updated_rows_observed}" \
    --arg message_multiplier "${message_multiplier}" \
    --argjson table_row_counts "${row_counts_json}" \
    --arg postgres_load_seconds "${load_seconds}" \
    --arg postgres_live_write_seconds "${live_write_seconds}" \
    --arg end_to_end_seconds "${end_to_end_seconds}" \
    --arg counter_seconds "${counter_seconds}" \
    --arg kafka_counter_wall_seconds "${kafka_counter_wall_seconds}" \
    --arg kafka_pre_stream_wait_seconds "${kafka_pre_stream_wait_seconds}" \
    --arg kafka_stream_seconds "${kafka_stream_seconds}" \
    --arg kafka_post_stream_wait_seconds "${kafka_post_stream_wait_seconds}" \
    --arg sink_wait_seconds "${sink_wait_seconds}" \
    --arg target_observation_seconds "${target_observation_seconds}" \
    --arg harness_overhead_seconds "${harness_overhead_seconds}" \
    --arg end_to_end_rows_per_second "${end_to_end_rows_per_second}" \
    --arg kafka_stream_rows_per_second "${kafka_stream_rows_per_second}" \
    --arg kafka_stream_source_rows_per_second "${kafka_stream_source_rows_per_second}" \
    --arg consumer_wall_source_rows_per_second "${consumer_wall_source_rows_per_second}" \
    --arg postgres_load_rows_per_second "${postgres_load_rows_per_second}" \
    --arg postgres_live_write_rows_per_second "${postgres_live_write_rows_per_second}" \
    --arg sink_rows_per_second "${sink_rows_per_second}" \
    --arg target_observed_records_per_second "${target_observed_records_per_second}" \
    --arg kafka_key_bytes "${kafka_key_bytes}" \
    --arg kafka_value_bytes "${kafka_value_bytes}" \
    --arg kafka_total_bytes "${kafka_total_bytes}" \
    --arg kafka_stream_mb_per_second "${kafka_stream_mb_per_second}" \
    --arg kafka_wall_mb_per_second "${kafka_wall_mb_per_second}" \
    --arg harness_overhead_percent "${harness_overhead_percent}" \
    --arg artifact_dir "${ARTIFACT_DIR}" \
    --arg node_stdout "${NODE_STDOUT}" \
    --arg node_stderr "${NODE_STDERR}" \
    --arg node_resource_log "${NODE_RESOURCE_LOG}" \
    --arg counter_log "${COUNTER_LOG}" \
    --arg reproduce_log "${REPRODUCE_LOG}" \
    --arg system_log "${SYSTEM_LOG}" \
    --arg postgres_settings_log "${POSTGRES_SETTINGS_LOG}" \
    --arg postgres_slot_log "${POSTGRES_SLOT_LOG}" \
    --arg kafka_topic_log "${KAFKA_TOPIC_LOG}" \
    --arg docker_stats_log "${DOCKER_STATS_LOG}" \
    '
    def maybe_num($value): if $value == "" then null else ($value | tonumber) end;
    def maybe_bool($value):
      if $value == "true" or $value == "1" then true
      elif $value == "false" or $value == "0" then false
      elif $value == "" then null
      else $value
      end;

    {
      schema_version: 1,
      run: {
        id: $run_id,
        timestamp: $timestamp,
        git_commit: $git_commit,
        git_branch: $git_branch,
        build_profile: $build_profile,
        artifact_dir: $artifact_dir
      },
      scenario: {
        dataset: $dataset,
        tpch_scale_factor: maybe_num($tpch_scale_factor),
        requested_rows: maybe_num($rows),
        mode: $mode,
        source_table: $source_table,
        upstream_table: $upstream_table,
        upstream_tables: $upstream_tables,
        target: {
          kind: $target,
          kafka_topics: $kafka_topics,
          postgres_tables: $postgres_sink_tables
        },
        pipeline_format: $pipeline_format,
        durable_replication_buffer: maybe_bool($durable_replication_buffer),
        buffer: {
          max_pending_bytes: maybe_num($buffer_max_pending_bytes),
          max_pending_records: maybe_num($buffer_max_pending_records),
          max_pending_objects: maybe_num($buffer_max_pending_objects),
          max_pending_age_ms: maybe_num($buffer_max_pending_age_ms)
        },
        encoding: {
          arrow_ipc_rows_per_record: maybe_num($arrow_ipc_rows_per_record),
          arrow_ipc_compression: $arrow_ipc_compression,
          kafka_metadata_headers: maybe_bool($kafka_metadata_headers)
        },
        postgres_snapshot: {
          rows_per_batch: maybe_num($postgres_snapshot_rows_per_batch),
          max_workers: maybe_num($postgres_snapshot_max_workers),
          intra_table_chunks: maybe_num($postgres_snapshot_intra_table_chunks)
        },
        floe_ports: {
          pgwire: maybe_num($floe_pg_port),
          admin: maybe_num($floe_admin_port)
        },
        redpanda: {
          kafka_batch_max_bytes: maybe_num($redpanda_kafka_batch_max_bytes),
          topic_max_message_bytes: maybe_num($redpanda_topic_max_message_bytes)
        },
        live_write: {
          chunk_rows: maybe_num($live_write_chunk_rows),
          sleep_ms: maybe_num($live_write_sleep_ms)
        },
        slatedb: {
          flush_interval_ms: maybe_num($slatedb_flush_interval_ms)
        }
      },
      counts: {
        table_rows: $table_row_counts,
        initial_rows: maybe_num($initial_rows),
        live_insert_rows: maybe_num($live_insert_rows),
        live_update_rows: maybe_num($live_update_rows),
        source_rows: maybe_num($source_rows),
        expected_kafka_messages: maybe_num($expected_kafka_messages),
        observed_kafka_messages: maybe_num($observed_kafka_messages),
        expected_sink_rows: maybe_num($expected_sink_rows),
        observed_sink_rows: maybe_num($sink_observed_rows),
        expected_postgres_updated_rows: maybe_num($expected_postgres_updated_rows),
        observed_postgres_updated_rows: maybe_num($postgres_sink_updated_rows_observed),
        message_multiplier: maybe_num($message_multiplier)
      },
      timings_seconds: {
        postgres_load: maybe_num($postgres_load_seconds),
        postgres_live_write: maybe_num($postgres_live_write_seconds),
        end_to_end: maybe_num($end_to_end_seconds),
        kafka_counter_wall: maybe_num($kafka_counter_wall_seconds),
        kafka_counter_process: maybe_num($counter_seconds),
        kafka_pre_stream_wait: maybe_num($kafka_pre_stream_wait_seconds),
        kafka_stream: maybe_num($kafka_stream_seconds),
        kafka_post_stream_wait: maybe_num($kafka_post_stream_wait_seconds),
        sink_wait: maybe_num($sink_wait_seconds),
        target_observation: maybe_num($target_observation_seconds),
        harness_overhead: maybe_num($harness_overhead_seconds)
      },
      rates: {
        end_to_end_source_rows_per_second: maybe_num($end_to_end_rows_per_second),
        kafka_stream_messages_per_second: maybe_num($kafka_stream_rows_per_second),
        kafka_stream_source_rows_per_second: maybe_num($kafka_stream_source_rows_per_second),
        consumer_wall_source_rows_per_second: maybe_num($consumer_wall_source_rows_per_second),
        postgres_load_rows_per_second: maybe_num($postgres_load_rows_per_second),
        postgres_live_write_rows_per_second: maybe_num($postgres_live_write_rows_per_second),
        sink_rows_per_second: maybe_num($sink_rows_per_second),
        target_observed_records_per_second: maybe_num($target_observed_records_per_second),
        kafka_stream_mb_per_second: maybe_num($kafka_stream_mb_per_second),
        kafka_wall_mb_per_second: maybe_num($kafka_wall_mb_per_second),
        harness_overhead_percent: maybe_num($harness_overhead_percent)
      },
      bytes: {
        kafka_key_bytes: maybe_num($kafka_key_bytes),
        kafka_value_bytes: maybe_num($kafka_value_bytes),
        kafka_total_bytes: maybe_num($kafka_total_bytes)
      },
      artifacts: {
        summary_env: ($artifact_dir + "/summary.env"),
        summary_json: ($artifact_dir + "/summary.json"),
        summary_md: ($artifact_dir + "/summary.md"),
        node_stdout: $node_stdout,
        node_stderr: $node_stderr,
        node_resource_log: $node_resource_log,
        counter_log: $counter_log,
        reproduce_log: $reproduce_log,
        system_log: $system_log,
        postgres_settings_log: $postgres_settings_log,
        postgres_slot_log: $postgres_slot_log,
        kafka_topic_log: $kafka_topic_log,
        docker_stats_log: $docker_stats_log
      }
    }
    ' >"${SUMMARY_JSON}"
}

write_summary_markdown() {
  cat >"${SUMMARY_MD}" <<MD
# Postgres CDC Benchmark

Run: \`${RUN_ID}\`

Dataset: \`${DATASET}\`

Mode: \`${BENCH_MODE}\`

Target: \`${TARGET_NORMALIZED}\`

Format: \`${PIPELINE_FORMAT}\`

Durable replication buffer: \`${DURABLE_REPLICATION_BUFFER}\`

Artifact directory: \`${ARTIFACT_DIR}\`

| Metric | Value |
| --- | ---: |
| Source rows | ${source_rows} |
| Expected Kafka messages | ${expected_kafka_messages_report:-} |
| Observed Kafka messages | ${observed_kafka_messages:-} |
| Expected Postgres sink rows | ${expected_sink_rows:-} |
| Observed Postgres sink rows | ${sink_observed_rows:-} |
| Observed Postgres updated rows | ${postgres_sink_updated_rows_observed:-} |
| End-to-end seconds | ${end_to_end_seconds} |
| End-to-end source rows/s | ${end_to_end_rows_per_second} |
| Target observation seconds | ${target_observation_seconds:-} |
| Target observed records/s | ${target_observed_records_per_second:-} |
| Kafka stream seconds | ${kafka_stream_seconds:-} |
| Kafka stream messages/s | ${kafka_stream_rows_per_second:-} |
| Kafka stream source rows/s | ${kafka_stream_source_rows_per_second:-} |
| Consumer wall source rows/s | ${consumer_wall_source_rows_per_second:-} |
| Harness overhead seconds | ${harness_overhead_seconds:-} |
| Harness overhead percent | ${harness_overhead_percent:-} |
| Postgres load seconds | ${load_seconds} |
| Postgres load rows/s | ${postgres_load_rows_per_second:-} |
| Postgres live write seconds | ${live_write_seconds} |
| Postgres live write rows/s | ${postgres_live_write_rows_per_second:-} |
| Postgres sink wait seconds | ${sink_wait_seconds:-} |
| Postgres sink rows/s | ${sink_rows_per_second:-} |
| Kafka total bytes | ${kafka_total_bytes:-} |
| Kafka stream MB/s | ${kafka_stream_mb_per_second:-} |

Machine-readable report: \`${SUMMARY_JSON}\`
MD
}

{
  echo "benchmark.dataset=${DATASET}"
  echo "benchmark.tpch_scale_factor=${TPCH_SCALE_FACTOR}"
  echo "benchmark.rows=${ROWS}"
  echo "benchmark.source_table=${source_table}"
  echo "benchmark.upstream_table=${upstream_table}"
  echo "benchmark.target=${TARGET_NORMALIZED}"
  echo "benchmark.kafka_topics=${topic_list}"
  echo "benchmark.postgres_sink_tables=${target_table_list}"
  echo "benchmark.mode=${BENCH_MODE}"
  echo "benchmark.initial_rows=${initial_rows}"
  echo "benchmark.live_insert_rows=${live_insert_rows}"
  echo "benchmark.live_update_rows=${live_update_rows}"
  echo "benchmark.source_rows=${source_rows}"
  echo "benchmark.pipeline_format=${PIPELINE_FORMAT}"
  echo "benchmark.durable_replication_buffer=${DURABLE_REPLICATION_BUFFER}"
  echo "benchmark.buffer_max_pending_bytes=${BUFFER_MAX_PENDING_BYTES}"
  echo "benchmark.buffer_max_pending_records=${BUFFER_MAX_PENDING_RECORDS}"
  echo "benchmark.buffer_max_pending_objects=${BUFFER_MAX_PENDING_OBJECTS}"
  echo "benchmark.buffer_max_pending_age_ms=${BUFFER_MAX_PENDING_AGE_MS}"
  echo "benchmark.arrow_ipc_rows_per_record=${ARROW_IPC_ROWS_PER_RECORD}"
  echo "benchmark.arrow_ipc_compression=${ARROW_IPC_COMPRESSION}"
  echo "benchmark.kafka_metadata_headers=${KAFKA_METADATA_HEADERS}"
  echo "benchmark.postgres_snapshot_rows_per_batch=${FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH}"
  echo "benchmark.postgres_snapshot_max_workers=${FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS}"
  echo "benchmark.postgres_snapshot_intra_table_chunks=${FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS}"
  echo "benchmark.floe_pg_port=${FLOE_PG_PORT}"
  echo "benchmark.floe_admin_port=${FLOE_ADMIN_PORT}"
  echo "benchmark.redpanda_kafka_batch_max_bytes=${REDPANDA_KAFKA_BATCH_MAX_BYTES}"
  echo "benchmark.redpanda_topic_max_message_bytes=${REDPANDA_TOPIC_MAX_MESSAGE_BYTES}"
  echo "benchmark.live_write_chunk_rows=${LIVE_WRITE_CHUNK_ROWS}"
  echo "benchmark.live_write_sleep_ms=${LIVE_WRITE_SLEEP_MS}"
  echo "benchmark.slatedb_flush_interval_ms=${SLATEDB_FLUSH_INTERVAL_MS}"
  echo "benchmark.expected_kafka_messages=${expected_kafka_messages_report}"
  echo "benchmark.observed_kafka_messages=${observed_kafka_messages}"
  echo "benchmark.expected_postgres_sink_rows=${expected_sink_rows}"
  echo "benchmark.observed_postgres_sink_rows=${sink_observed_rows}"
  echo "benchmark.expected_postgres_sink_updated_rows=${expected_postgres_updated_rows}"
  echo "benchmark.observed_postgres_sink_updated_rows=${postgres_sink_updated_rows_observed}"
  echo "benchmark.postgres_load_seconds=${load_seconds}"
  echo "benchmark.postgres_live_write_seconds=${live_write_seconds}"
  echo "benchmark.end_to_end_seconds=${end_to_end_seconds}"
  echo "benchmark.counter_seconds=${counter_seconds}"
  echo "benchmark.end_to_end_rows_per_second=${end_to_end_rows_per_second}"
  echo "benchmark.kafka_counter_wall_seconds=${kafka_counter_wall_seconds}"
  echo "benchmark.kafka_pre_stream_wait_seconds=${kafka_pre_stream_wait_seconds}"
  echo "benchmark.kafka_stream_seconds=${kafka_stream_seconds}"
  echo "benchmark.kafka_post_stream_wait_seconds=${kafka_post_stream_wait_seconds}"
  echo "benchmark.postgres_sink_wait_seconds=${sink_wait_seconds}"
  echo "benchmark.target_observation_seconds=${target_observation_seconds}"
  echo "benchmark.kafka_stream_rows_per_second=${kafka_stream_rows_per_second}"
  echo "benchmark.kafka_stream_source_rows_per_second=${kafka_stream_source_rows_per_second}"
  echo "benchmark.kafka_stream_mb_per_second=${kafka_stream_mb_per_second}"
  echo "benchmark.kafka_key_bytes=${kafka_key_bytes}"
  echo "benchmark.kafka_value_bytes=${kafka_value_bytes}"
  echo "benchmark.kafka_total_bytes=${kafka_total_bytes}"
  echo "benchmark.kafka_wall_mb_per_second=${kafka_wall_mb_per_second}"
  echo "benchmark.harness_overhead_seconds=${harness_overhead_seconds}"
  echo "benchmark.harness_overhead_percent=${harness_overhead_percent}"
  echo "benchmark.consumer_wall_source_rows_per_second=${consumer_wall_source_rows_per_second}"
  echo "benchmark.message_multiplier=${message_multiplier}"
  echo "benchmark.postgres_load_rows_per_second=${postgres_load_rows_per_second}"
  echo "benchmark.postgres_live_write_rows_per_second=${postgres_live_write_rows_per_second}"
  echo "benchmark.postgres_sink_rows_per_second=${sink_rows_per_second}"
  echo "benchmark.target_observed_records_per_second=${target_observed_records_per_second}"
  echo "benchmark.artifact_dir=${ARTIFACT_DIR}"
  echo "benchmark.node_stdout=${NODE_STDOUT}"
  echo "benchmark.node_stderr=${NODE_STDERR}"
  echo "benchmark.node_resource_log=${NODE_RESOURCE_LOG}"
  echo "benchmark.counter_log=${COUNTER_LOG}"
  echo "benchmark.reproduce_log=${REPRODUCE_LOG}"
  echo "benchmark.system_log=${SYSTEM_LOG}"
  echo "benchmark.postgres_settings_log=${POSTGRES_SETTINGS_LOG}"
  echo "benchmark.postgres_slot_log=${POSTGRES_SLOT_LOG}"
  echo "benchmark.kafka_topic_log=${KAFKA_TOPIC_LOG}"
  echo "benchmark.docker_stats_log=${DOCKER_STATS_LOG}"
  echo "benchmark.summary_json=${SUMMARY_JSON}"
  echo "benchmark.summary_md=${SUMMARY_MD}"
} | tee "${SUMMARY_ENV}"

write_summary_json
write_summary_markdown

echo "Stopping Floe node"
stop_node

echo "CDC benchmark complete."
echo "summary_json=${SUMMARY_JSON}"
echo "summary_md=${SUMMARY_MD}"
