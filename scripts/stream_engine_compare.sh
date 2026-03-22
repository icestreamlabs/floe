#!/usr/bin/env bash
set -euo pipefail

ENGINE="${1:-all}"
BENCH_QUERY="${2:-${BENCH_QUERY:-filter_projection}}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-${REPO_ROOT}/target/third_party_engine_benchmarks}"
RUN_ID="$(date +%s%3N)"
NETWORK_NAME="${NETWORK_NAME:-floe-stream-bench-net}"

ROWS="${ROWS:-1000000}"
JOIN_AUCTION_ROWS="${JOIN_AUCTION_ROWS:-10000}"
EXPECTED_ROWS=""
INPUT_ROWS_TOTAL=""
QUERY_DESCRIPTION=""
QUERY_RESULT_RELATION="benchmark_result"
QUERY_COUNT_RELATION="benchmark_result_count"
POLL_INTERVAL_MS="${POLL_INTERVAL_MS:-250}"
POLL_TIMEOUT_MS="${POLL_TIMEOUT_MS:-150000}"
BROKER_PORT="${BROKER_PORT:-19092}"
BROKER_ADDR="127.0.0.1:${BROKER_PORT}"
BROKER_ADDR_FROM_CONTAINER="${REDPANDA_CONTAINER:-floe-stream-bench-redpanda}:9092"

REDPANDA_CONTAINER="${REDPANDA_CONTAINER:-floe-stream-bench-redpanda}"
REDPANDA_IMAGE="${REDPANDA_IMAGE:-docker.redpanda.com/redpandadata/redpanda:latest}"

MATERIALIZE_CONTAINER="${MATERIALIZE_CONTAINER:-floe-stream-bench-materialize}"
MATERIALIZE_IMAGE="${MATERIALIZE_IMAGE:-materialize/materialized:v26.14.1}"
MATERIALIZE_SQL_PORT="${MATERIALIZE_SQL_PORT:-16875}"
MATERIALIZE_CLUSTER_SIZE="${MATERIALIZE_CLUSTER_SIZE:-25cc}"
MATERIALIZE_BEST_EFFORT_IN_MEMORY="${MATERIALIZE_BEST_EFFORT_IN_MEMORY:-1}"

RISINGWAVE_CONTAINER="${RISINGWAVE_CONTAINER:-floe-stream-bench-risingwave}"
RISINGWAVE_IMAGE="${RISINGWAVE_IMAGE:-risingwavelabs/risingwave:latest}"
RISINGWAVE_SQL_PORT="${RISINGWAVE_SQL_PORT:-14566}"
RISINGWAVE_IN_MEMORY="${RISINGWAVE_IN_MEMORY:-1}"

FELDERA_CONTAINER="${FELDERA_CONTAINER:-floe-stream-bench-feldera}"
FELDERA_IMAGE="${FELDERA_IMAGE:-ghcr.io/feldera/pipeline-manager:latest}"
FELDERA_HTTP_PORT="${FELDERA_HTTP_PORT:-18080}"
FELDERA_WORKERS="${FELDERA_WORKERS:-4}"
FELDERA_BEST_EFFORT_IN_MEMORY="${FELDERA_BEST_EFFORT_IN_MEMORY:-1}"
FELDERA_MIN_STORAGE_BYTES="${FELDERA_MIN_STORAGE_BYTES:-1099511627776}"
FELDERA_MIN_STEP_STORAGE_BYTES="${FELDERA_MIN_STEP_STORAGE_BYTES:-1099511627776}"
FELDERA_COMPLETION_MODE="${FELDERA_COMPLETION_MODE:-count}"
KAFKA_LATENCY_FETCH_PROFILE="${KAFKA_LATENCY_FETCH_PROFILE:-1}"
KAFKA_FETCH_WAIT_MAX_MS="${KAFKA_FETCH_WAIT_MAX_MS:-1}"
KAFKA_FETCH_QUEUE_BACKOFF_MS="${KAFKA_FETCH_QUEUE_BACKOFF_MS:-1}"
KAFKA_FETCH_MIN_BYTES="${KAFKA_FETCH_MIN_BYTES:-1}"

FLOE_PG_PORT="${FLOE_PG_PORT:-16432}"
FLOE_KAFKA_GROUP_ID_PREFIX="${FLOE_KAFKA_GROUP_ID_PREFIX:-floe-stream-bench}"
FLOE_KAFKA_POLL_MS="${FLOE_KAFKA_POLL_MS:-10}"
FLOE_KAFKA_MAX_MESSAGES_PER_TICK="${FLOE_KAFKA_MAX_MESSAGES_PER_TICK:-16384}"
FLOE_INGEST_QUEUE_CAPACITY="${FLOE_INGEST_QUEUE_CAPACITY:-262144}"
FLOE_INGEST_BATCH_SIZE="${FLOE_INGEST_BATCH_SIZE:-16384}"
FLOE_INGEST_BATCH_PER_SOURCE="${FLOE_INGEST_BATCH_PER_SOURCE:-16384}"
FLOE_INGEST_BATCH_PER_CONNECTOR="${FLOE_INGEST_BATCH_PER_CONNECTOR:-16384}"
FLOE_MV_RETAIN_LAST="${FLOE_MV_RETAIN_LAST:-256}"
FLOE_L0_SST_BYTES="${FLOE_L0_SST_BYTES:-1073741824}"
FLOE_MAX_UNFLUSHED_BYTES="${FLOE_MAX_UNFLUSHED_BYTES:-8589934592}"

KEEP_CONTAINERS="${KEEP_CONTAINERS:-0}"

RESULTS_FILE="${ARTIFACT_ROOT}/${RUN_ID}/summary.md"
mkdir -p "${ARTIFACT_ROOT}/${RUN_ID}"
POLL_ATTEMPTS=$(((POLL_TIMEOUT_MS + POLL_INTERVAL_MS - 1) / POLL_INTERVAL_MS))
FLOE_NODE_PID=""

log() {
  printf '[stream-engine-compare] %s\n' "$*"
}

env_enabled() {
  case "${1}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

die() {
  printf '[stream-engine-compare] ERROR: %s\n' "$*" >&2
  exit 1
}

compute_filter_projection_expected_rows() {
  local rows="$1"
  local full_cycles remainder
  full_cycles=$((rows / 10000))
  remainder=$((rows % 10000))
  printf '%s\n' $((full_cycles * 5000 + (remainder < 5000 ? remainder : 5000)))
}

configure_benchmark_profile() {
  case "${BENCH_QUERY}" in
    filter_projection)
      QUERY_DESCRIPTION="bid filter + projection"
      INPUT_ROWS_TOTAL="${ROWS}"
      EXPECTED_ROWS="$(compute_filter_projection_expected_rows "${ROWS}")"
      ;;
    join)
      if (( JOIN_AUCTION_ROWS != 10000 )); then
        die "join benchmark currently requires JOIN_AUCTION_ROWS=10000 to match the deterministic bid auction id distribution"
      fi
      QUERY_DESCRIPTION="bid/auction inner join + auction-side category filter"
      INPUT_ROWS_TOTAL=$((ROWS + JOIN_AUCTION_ROWS))
      EXPECTED_ROWS=$((ROWS / 10))
      ;;
    *)
      die "unknown BENCH_QUERY '${BENCH_QUERY}' (expected filter_projection|join)"
      ;;
  esac
}

cleanup() {
  stop_floe_process
  if [[ "${KEEP_CONTAINERS}" == "1" ]]; then
    return
  fi
  docker rm -f "${MATERIALIZE_CONTAINER}" >/dev/null 2>&1 || true
  docker rm -f "${RISINGWAVE_CONTAINER}" >/dev/null 2>&1 || true
  docker rm -f "${FELDERA_CONTAINER}" >/dev/null 2>&1 || true
  docker rm -f "${REDPANDA_CONTAINER}" >/dev/null 2>&1 || true
  docker network rm "${NETWORK_NAME}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

sleep_ms() {
  local millis="$1"
  sleep "$(awk "BEGIN { printf \"%.3f\", ${millis} / 1000 }")"
}

run_psql() {
  local port="$1"
  local user="$2"
  local db="$3"
  local sql="$4"
  PGPASSWORD="" psql -h 127.0.0.1 -p "${port}" -U "${user}" -d "${db}" -v ON_ERROR_STOP=1 -Atqc "${sql}"
}

run_psql_file() {
  local port="$1"
  local user="$2"
  local db="$3"
  local file="$4"
  PGPASSWORD="" psql -h 127.0.0.1 -p "${port}" -U "${user}" -d "${db}" -v ON_ERROR_STOP=1 -f "${file}"
}

wait_for_pg() {
  local port="$1"
  local user="$2"
  local db="$3"
  local label="$4"
  for _ in $(seq 1 90); do
    if PGPASSWORD="" psql -h 127.0.0.1 -p "${port}" -U "${user}" -d "${db}" -Atqc "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  die "${label} did not become ready on port ${port}"
}

wait_for_http_ok() {
  local url="$1"
  local label="$2"
  for _ in $(seq 1 90); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  die "${label} did not become ready at ${url}"
}

build_producer() {
  log "building kafka benchmark producer"
  cargo build -p floe-benchmarks --bin kafka_million_bid_producer --release >/dev/null
}

build_floe_node() {
  log "building floe-node release binary"
  cargo build -p floe-node --release >/dev/null
}

ensure_network() {
  if docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1; then
    return 0
  fi
  docker network create "${NETWORK_NAME}" >/dev/null
}

ensure_redpanda() {
  if docker ps --format '{{.Names}}' | grep -Fx "${REDPANDA_CONTAINER}" >/dev/null 2>&1; then
    if docker image inspect "${REDPANDA_IMAGE}" >/dev/null 2>&1; then
      capture_image_metadata "${REDPANDA_IMAGE}" "${ARTIFACT_ROOT}/${RUN_ID}/redpanda_image_metadata.json"
    fi
    return 0
  fi

  ensure_network
  log "starting Redpanda ${REDPANDA_CONTAINER}"
  docker rm -f "${REDPANDA_CONTAINER}" >/dev/null 2>&1 || true
  docker pull "${REDPANDA_IMAGE}" >/dev/null
  capture_image_metadata "${REDPANDA_IMAGE}" "${ARTIFACT_ROOT}/${RUN_ID}/redpanda_image_metadata.json"
  docker run -d \
    --name "${REDPANDA_CONTAINER}" \
    --network "${NETWORK_NAME}" \
    -p "${BROKER_PORT}:19092" \
    "${REDPANDA_IMAGE}" \
    redpanda start \
      --overprovisioned \
      --smp 1 \
      --memory 1G \
      --reserve-memory 0M \
      --node-id 0 \
      --check=false \
      --kafka-addr "internal://0.0.0.0:9092,external://0.0.0.0:19092" \
      --advertise-kafka-addr "internal://${REDPANDA_CONTAINER}:9092,external://127.0.0.1:${BROKER_PORT}" >/dev/null

  for _ in $(seq 1 90); do
    if docker exec "${REDPANDA_CONTAINER}" rpk cluster info >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  docker logs "${REDPANDA_CONTAINER}" || true
  die "Redpanda did not become ready"
}

reset_topic() {
  local topic="$1"
  docker exec "${REDPANDA_CONTAINER}" rpk topic delete "${topic}" >/dev/null 2>&1 || true
  docker exec "${REDPANDA_CONTAINER}" rpk topic create "${topic}" -p 1 -r 1 >/dev/null
}

produce_topic() {
  local topic="$1"
  local dataset="$2"
  local rows="$3"
  local start_ms end_ms
  start_ms="$(date +%s%3N)"
  "${REPO_ROOT}/target/release/kafka_million_bid_producer" \
    --brokers "${BROKER_ADDR}" \
    --topic "${topic}" \
    --dataset "${dataset}" \
    --rows "${rows}"
  end_ms="$(date +%s%3N)"
  PRODUCE_MS=$((PRODUCE_MS + end_ms - start_ms))
}

produce_query_inputs() {
  PRODUCE_MS=0
  case "${BENCH_QUERY}" in
    filter_projection)
      local bid_topic="$1"
      produce_topic "${bid_topic}" bid "${ROWS}"
      ;;
    join)
      local bid_topic="$1"
      local auction_topic="$2"
      produce_topic "${auction_topic}" auction "${JOIN_AUCTION_ROWS}"
      produce_topic "${bid_topic}" bid "${ROWS}"
      ;;
    *)
      die "produce_query_inputs called with unsupported BENCH_QUERY '${BENCH_QUERY}'"
      ;;
  esac
}

write_result() {
  local engine="$1"
  local artifact_dir="$2"
  local total_ms="$3"
  local produce_ms="$4"
  local post_ms="$5"
  local rows_per_sec="$6"
  local completion_signal="$7"

  jq -n \
    --arg engine "${engine}" \
    --arg artifact_dir "${artifact_dir}" \
    --arg completion_signal "${completion_signal}" \
    --arg bench_query "${BENCH_QUERY}" \
    --arg query_description "${QUERY_DESCRIPTION}" \
    --argjson rows "${INPUT_ROWS_TOTAL}" \
    --argjson primary_rows "${ROWS}" \
    --argjson join_auction_rows "${JOIN_AUCTION_ROWS}" \
    --argjson expected_rows "${EXPECTED_ROWS}" \
    --argjson total_ms "${total_ms}" \
    --argjson produce_ms "${produce_ms}" \
    --argjson post_produce_wait_ms "${post_ms}" \
    --argjson input_rows_per_sec "${rows_per_sec}" \
    '{
      engine: $engine,
      benchmark_query: $bench_query,
      benchmark_query_description: $query_description,
      rows: $rows,
      primary_rows: $primary_rows,
      join_auction_rows: $join_auction_rows,
      expected_rows: $expected_rows,
      timing: {
        total_ms: $total_ms,
        produce_ms: $produce_ms,
        post_produce_wait_ms: $post_produce_wait_ms
      },
      throughput: {
        input_rows_per_sec: $input_rows_per_sec
      },
      measurement: {
        completion_signal: $completion_signal
      },
      artifact_dir: $artifact_dir
    }' > "${artifact_dir}/summary.json"
}

append_summary_row() {
  local engine="$1"
  local total_ms="$2"
  local produce_ms="$3"
  local post_ms="$4"
  local rows_per_sec="$5"
  printf '| %s | %.3f | %.3f | %.3f | %s |\n' \
    "${engine}" \
    "$(awk "BEGIN { print ${total_ms}/1000 }")" \
    "$(awk "BEGIN { print ${produce_ms}/1000 }")" \
    "$(awk "BEGIN { print ${post_ms}/1000 }")" \
    "${rows_per_sec}" >> "${RESULTS_FILE}"
}

poll_pg_count() {
  local port="$1"
  local user="$2"
  local db="$3"
  local sql="$4"
  local label="$5"
  local count
  local start_ms now_ms
  start_ms="$(date +%s%3N)"
  for _ in $(seq 1 "${POLL_ATTEMPTS}"); do
    count="$(PGPASSWORD="" psql -h 127.0.0.1 -p "${port}" -U "${user}" -d "${db}" -Atqc "${sql}" 2>/dev/null | tr -d '[:space:]')"
    if [[ -n "${count}" ]] && [[ "${count}" =~ ^[0-9]+$ ]] && (( count >= EXPECTED_ROWS )); then
      now_ms="$(date +%s%3N)"
      POST_PRODUCE_WAIT_MS=$((now_ms - start_ms))
      return 0
    fi
    sleep_ms "${POLL_INTERVAL_MS}"
  done
  die "${label} did not reach count ${EXPECTED_ROWS}"
}

poll_feldera_program_success() {
  local pipeline="$1"
  local status
  for _ in $(seq 1 240); do
    status="$(curl -fsS "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}" | jq -r '.program_status')"
    case "${status}" in
      Success) return 0 ;;
      SqlError|RustError|SystemError)
        curl -fsS "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}" | jq .
        die "Feldera program failed with status ${status}"
        ;;
    esac
    sleep 2
  done
  die "Feldera program did not compile successfully"
}

poll_feldera_running() {
  local pipeline="$1"
  local status
  for _ in $(seq 1 120); do
    status="$(curl -fsS "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}" | jq -r '.deployment_status')"
    if [[ "${status}" == "Running" ]]; then
      return 0
    fi
    sleep 1
  done
  die "Feldera pipeline did not reach Running"
}

poll_feldera_completion() {
  case "${FELDERA_COMPLETION_MODE}" in
    count)
      poll_feldera_count_query "$1"
      ;;
    completed_records)
      poll_feldera_completed_records "$1"
      ;;
    *)
      die "unsupported FELDERA_COMPLETION_MODE '${FELDERA_COMPLETION_MODE}' (expected count|completed_records)"
      ;;
  esac
}

poll_feldera_completed_records() {
  local pipeline="$1"
  local response input_count completed_count
  local start_ms now_ms
  start_ms="$(date +%s%3N)"
  for _ in $(seq 1 "${POLL_ATTEMPTS}"); do
    response="$(curl -fsS "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}/stats" 2>/dev/null || true)"
    input_count="$(printf '%s' "${response}" | jq -r '.global_metrics.total_input_records // empty' 2>/dev/null | tr -d '[:space:]' || true)"
    completed_count="$(printf '%s' "${response}" | jq -r '.global_metrics.total_completed_records // empty' 2>/dev/null | tr -d '[:space:]' || true)"
    if [[ -n "${input_count}" ]] && [[ -n "${completed_count}" ]] \
      && [[ "${input_count}" =~ ^[0-9]+$ ]] && [[ "${completed_count}" =~ ^[0-9]+$ ]] \
      && (( input_count >= INPUT_ROWS_TOTAL )) && (( completed_count >= INPUT_ROWS_TOTAL )); then
      now_ms="$(date +%s%3N)"
      POST_PRODUCE_WAIT_MS=$((now_ms - start_ms))
      return 0
    fi
    sleep_ms "${POLL_INTERVAL_MS}"
  done
  die "Feldera pipeline did not complete ${INPUT_ROWS_TOTAL} rows"
}

poll_feldera_count_query() {
  local pipeline="$1"
  local response count
  local start_ms now_ms
  start_ms="$(date +%s%3N)"
  for _ in $(seq 1 "${POLL_ATTEMPTS}"); do
    response="$(curl -fsS --get \
      "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}/query" \
      --data-urlencode "sql=SELECT ROW_COUNT FROM ${QUERY_COUNT_RELATION}" \
      --data-urlencode "format=json" 2>/dev/null || true)"
    count="$(printf '%s' "${response}" | jq -sr 'if length > 0 then (.[0].ROW_COUNT // .[0].row_count // empty) else empty end' 2>/dev/null | tr -d '[:space:]' || true)"
    if [[ -n "${count}" ]] && [[ "${count}" =~ ^[0-9]+$ ]] && (( count >= EXPECTED_ROWS )); then
      now_ms="$(date +%s%3N)"
      POST_PRODUCE_WAIT_MS=$((now_ms - start_ms))
      return 0
    fi
    sleep_ms "${POLL_INTERVAL_MS}"
  done
  die "Feldera count view did not reach ${EXPECTED_ROWS} rows"
}

write_run_context() {
  jq -n \
    --arg run_id "${RUN_ID}" \
    --arg network "${NETWORK_NAME}" \
    --arg broker_addr "${BROKER_ADDR}" \
    --arg broker_addr_from_container "${BROKER_ADDR_FROM_CONTAINER}" \
    --arg redpanda_image "${REDPANDA_IMAGE}" \
    --arg materialize_image "${MATERIALIZE_IMAGE}" \
    --arg risingwave_image "${RISINGWAVE_IMAGE}" \
    --arg feldera_image "${FELDERA_IMAGE}" \
    --arg bench_query "${BENCH_QUERY}" \
    --arg query_description "${QUERY_DESCRIPTION}" \
    --arg git_commit "$(git rev-parse HEAD)" \
    --arg git_branch "$(git branch --show-current 2>/dev/null || true)" \
    --arg rustc_version "$(rustc -V)" \
    --argjson rows "${INPUT_ROWS_TOTAL}" \
    --argjson primary_rows "${ROWS}" \
    --argjson join_auction_rows "${JOIN_AUCTION_ROWS}" \
    --argjson expected_rows "${EXPECTED_ROWS}" \
    --argjson poll_interval_ms "${POLL_INTERVAL_MS}" \
    --argjson poll_timeout_ms "${POLL_TIMEOUT_MS}" \
    '{
      run_id: $run_id,
      benchmark_query: $bench_query,
      benchmark_query_description: $query_description,
      rows: $rows,
      primary_rows: $primary_rows,
      join_auction_rows: $join_auction_rows,
      expected_rows: $expected_rows,
      polling: {
        interval_ms: $poll_interval_ms,
        timeout_ms: $poll_timeout_ms
      },
      kafka: {
        broker_addr: $broker_addr,
        broker_addr_from_container: $broker_addr_from_container
      },
      images: {
        redpanda: $redpanda_image,
        materialize: $materialize_image,
        risingwave: $risingwave_image,
        feldera: $feldera_image
      },
      floe: {
        git_commit: $git_commit,
        git_branch: $git_branch,
        rustc_version: $rustc_version
      }
    }' > "${ARTIFACT_ROOT}/${RUN_ID}/run_context.json"
}

capture_image_metadata() {
  local image_ref="$1"
  local output_path="$2"
  docker image inspect "${image_ref}" | jq '.[0] | {
    id: .Id,
    repo_tags: .RepoTags,
    repo_digests: .RepoDigests,
    created: .Created,
    architecture: .Architecture,
    os: .Os
  }' > "${output_path}"
}

capture_floe_metadata() {
  local output_path="$1"
  jq -n \
    --arg binary "${REPO_ROOT}/target/release/floe-node" \
    --arg git_commit "$(git rev-parse HEAD)" \
    --arg git_branch "$(git branch --show-current 2>/dev/null || true)" \
    --arg rustc_version "$(rustc -V)" \
    --argjson pg_port "${FLOE_PG_PORT}" \
    --argjson kafka_poll_ms "${FLOE_KAFKA_POLL_MS}" \
    --argjson kafka_max_messages_per_tick "${FLOE_KAFKA_MAX_MESSAGES_PER_TICK}" \
    --argjson ingest_queue_capacity "${FLOE_INGEST_QUEUE_CAPACITY}" \
    --argjson ingest_batch_size "${FLOE_INGEST_BATCH_SIZE}" \
    --argjson ingest_batch_per_source "${FLOE_INGEST_BATCH_PER_SOURCE}" \
    --argjson ingest_batch_per_connector "${FLOE_INGEST_BATCH_PER_CONNECTOR}" \
    --argjson mv_retain_last "${FLOE_MV_RETAIN_LAST}" \
    --argjson slatedb_l0_sst_bytes "${FLOE_L0_SST_BYTES}" \
    --argjson slatedb_max_unflushed_bytes "${FLOE_MAX_UNFLUSHED_BYTES}" \
    '{
      binary: $binary,
      git_commit: $git_commit,
      git_branch: $git_branch,
      rustc_version: $rustc_version,
      pg_port: $pg_port,
      kafka: {
        poll_ms: $kafka_poll_ms,
        max_messages_per_tick: $kafka_max_messages_per_tick
      },
      runtime: {
        ingest_queue_capacity: $ingest_queue_capacity,
        ingest_batch_size: $ingest_batch_size,
        ingest_batch_per_source: $ingest_batch_per_source,
        ingest_batch_per_connector: $ingest_batch_per_connector,
        mv_retain_last: $mv_retain_last
      },
      storage: {
        slatedb_l0_sst_bytes: $slatedb_l0_sst_bytes,
        slatedb_max_unflushed_bytes: $slatedb_max_unflushed_bytes
      }
    }' > "${output_path}"
}

stop_floe_process() {
  if [[ -z "${FLOE_NODE_PID}" ]]; then
    return
  fi
  if kill -0 "${FLOE_NODE_PID}" >/dev/null 2>&1; then
    kill -INT "${FLOE_NODE_PID}" >/dev/null 2>&1 || true
    wait "${FLOE_NODE_PID}" >/dev/null 2>&1 || true
  fi
  FLOE_NODE_PID=""
}

wait_for_floe_pg() {
  local artifact_dir="$1"
  local stderr_path="${artifact_dir}/floe-node.stderr.log"
  for _ in $(seq 1 180); do
    if ! kill -0 "${FLOE_NODE_PID}" >/dev/null 2>&1; then
      tail -n 120 "${stderr_path}" >&2 || true
      die "Floe process exited before pgwire became ready"
    fi
    if PGPASSWORD="" psql -h 127.0.0.1 -p "${FLOE_PG_PORT}" -U postgres -d postgres -Atqc "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  tail -n 120 "${stderr_path}" >&2 || true
  die "Floe did not become ready on port ${FLOE_PG_PORT}"
}

materialize_benchmark() {
  local artifact_dir="${ARTIFACT_ROOT}/${RUN_ID}/materialize"
  local bid_topic="materialize_bids_${RUN_ID}"
  local auction_topic="materialize_auctions_${RUN_ID}"
  local object_mode="durable_mvs"
  mkdir -p "${artifact_dir}"

  docker rm -f "${MATERIALIZE_CONTAINER}" >/dev/null 2>&1 || true
  log "starting Materialize emulator"
  docker pull "${MATERIALIZE_IMAGE}" >/dev/null
  capture_image_metadata "${MATERIALIZE_IMAGE}" "${artifact_dir}/image_metadata.json"
  docker run -d \
    --name "${MATERIALIZE_CONTAINER}" \
    --network "${NETWORK_NAME}" \
    -p "${MATERIALIZE_SQL_PORT}:6875" \
    "${MATERIALIZE_IMAGE}" >/dev/null

  wait_for_pg "${MATERIALIZE_SQL_PORT}" materialize materialize "Materialize"
  reset_topic "${bid_topic}"
  if [[ "${BENCH_QUERY}" == "join" ]]; then
    reset_topic "${auction_topic}"
  fi

  if env_enabled "${MATERIALIZE_BEST_EFFORT_IN_MEMORY}"; then
    object_mode="indexed_views"
    case "${BENCH_QUERY}" in
      filter_projection)
        cat > "${artifact_dir}/setup.sql" <<SQL
DROP INDEX IF EXISTS ${QUERY_COUNT_RELATION}_primary_idx CASCADE;
DROP INDEX IF EXISTS ${QUERY_RESULT_RELATION}_primary_idx CASCADE;
DROP VIEW IF EXISTS ${QUERY_COUNT_RELATION} CASCADE;
DROP VIEW IF EXISTS ${QUERY_RESULT_RELATION} CASCADE;
DROP SOURCE IF EXISTS bids_source CASCADE;
DROP CONNECTION IF EXISTS kafka_conn CASCADE;
DROP CLUSTER IF EXISTS bench CASCADE;
CREATE CLUSTER bench SIZE '${MATERIALIZE_CLUSTER_SIZE}';
SET cluster = bench;
CREATE CONNECTION kafka_conn TO KAFKA (
  BROKER '${BROKER_ADDR_FROM_CONTAINER}',
  SECURITY PROTOCOL PLAINTEXT
);
CREATE SOURCE bids_source
FROM KAFKA CONNECTION kafka_conn (TOPIC '${bid_topic}')
FORMAT JSON ENVELOPE NONE;
CREATE VIEW ${QUERY_RESULT_RELATION} AS
SELECT
  (data->>'auction')::bigint AS auction,
  (data->>'bidder')::bigint AS bidder,
  (data->>'price')::bigint AS projected_price
FROM bids_source
WHERE (data->>'auction')::bigint <= 5000;
CREATE DEFAULT INDEX ON ${QUERY_RESULT_RELATION};
CREATE VIEW ${QUERY_COUNT_RELATION} AS
SELECT COUNT(*)::bigint AS row_count FROM ${QUERY_RESULT_RELATION};
CREATE DEFAULT INDEX ON ${QUERY_COUNT_RELATION};
SQL
        ;;
      join)
        cat > "${artifact_dir}/setup.sql" <<SQL
DROP INDEX IF EXISTS ${QUERY_COUNT_RELATION}_primary_idx CASCADE;
DROP INDEX IF EXISTS ${QUERY_RESULT_RELATION}_primary_idx CASCADE;
DROP VIEW IF EXISTS ${QUERY_COUNT_RELATION} CASCADE;
DROP VIEW IF EXISTS ${QUERY_RESULT_RELATION} CASCADE;
DROP SOURCE IF EXISTS bids_source CASCADE;
DROP SOURCE IF EXISTS auctions_source CASCADE;
DROP CONNECTION IF EXISTS kafka_conn CASCADE;
DROP CLUSTER IF EXISTS bench CASCADE;
CREATE CLUSTER bench SIZE '${MATERIALIZE_CLUSTER_SIZE}';
SET cluster = bench;
CREATE CONNECTION kafka_conn TO KAFKA (
  BROKER '${BROKER_ADDR_FROM_CONTAINER}',
  SECURITY PROTOCOL PLAINTEXT
);
CREATE SOURCE bids_source
FROM KAFKA CONNECTION kafka_conn (TOPIC '${bid_topic}')
FORMAT JSON ENVELOPE NONE;
CREATE SOURCE auctions_source
FROM KAFKA CONNECTION kafka_conn (TOPIC '${auction_topic}')
FORMAT JSON ENVELOPE NONE;
CREATE VIEW ${QUERY_RESULT_RELATION} AS
SELECT
  (b.data->>'auction')::bigint AS auction,
  (b.data->>'bidder')::bigint AS bidder,
  (b.data->>'price')::bigint AS projected_price,
  (a.data->>'seller')::bigint AS seller
FROM bids_source AS b
JOIN auctions_source AS a
  ON (b.data->>'auction')::bigint = (a.data->>'id')::bigint
WHERE (a.data->>'category')::bigint = 10;
CREATE DEFAULT INDEX ON ${QUERY_RESULT_RELATION};
CREATE VIEW ${QUERY_COUNT_RELATION} AS
SELECT COUNT(*)::bigint AS row_count FROM ${QUERY_RESULT_RELATION};
CREATE DEFAULT INDEX ON ${QUERY_COUNT_RELATION};
SQL
        ;;
    esac
  else
    case "${BENCH_QUERY}" in
      filter_projection)
        cat > "${artifact_dir}/setup.sql" <<SQL
DROP MATERIALIZED VIEW IF EXISTS ${QUERY_COUNT_RELATION} CASCADE;
DROP MATERIALIZED VIEW IF EXISTS ${QUERY_RESULT_RELATION} CASCADE;
DROP SOURCE IF EXISTS bids_source CASCADE;
DROP CONNECTION IF EXISTS kafka_conn CASCADE;
DROP CLUSTER IF EXISTS bench CASCADE;
CREATE CLUSTER bench SIZE '${MATERIALIZE_CLUSTER_SIZE}';
SET cluster = bench;
CREATE CONNECTION kafka_conn TO KAFKA (
  BROKER '${BROKER_ADDR_FROM_CONTAINER}',
  SECURITY PROTOCOL PLAINTEXT
);
CREATE SOURCE bids_source
FROM KAFKA CONNECTION kafka_conn (TOPIC '${bid_topic}')
FORMAT JSON ENVELOPE NONE;
CREATE MATERIALIZED VIEW ${QUERY_RESULT_RELATION} AS
SELECT
  (data->>'auction')::bigint AS auction,
  (data->>'bidder')::bigint AS bidder,
  (data->>'price')::bigint AS projected_price
FROM bids_source
WHERE (data->>'auction')::bigint <= 5000;
CREATE MATERIALIZED VIEW ${QUERY_COUNT_RELATION} AS
SELECT COUNT(*)::bigint AS row_count FROM ${QUERY_RESULT_RELATION};
SQL
        ;;
      join)
        cat > "${artifact_dir}/setup.sql" <<SQL
DROP MATERIALIZED VIEW IF EXISTS ${QUERY_COUNT_RELATION} CASCADE;
DROP MATERIALIZED VIEW IF EXISTS ${QUERY_RESULT_RELATION} CASCADE;
DROP SOURCE IF EXISTS bids_source CASCADE;
DROP SOURCE IF EXISTS auctions_source CASCADE;
DROP CONNECTION IF EXISTS kafka_conn CASCADE;
DROP CLUSTER IF EXISTS bench CASCADE;
CREATE CLUSTER bench SIZE '${MATERIALIZE_CLUSTER_SIZE}';
SET cluster = bench;
CREATE CONNECTION kafka_conn TO KAFKA (
  BROKER '${BROKER_ADDR_FROM_CONTAINER}',
  SECURITY PROTOCOL PLAINTEXT
);
CREATE SOURCE bids_source
FROM KAFKA CONNECTION kafka_conn (TOPIC '${bid_topic}')
FORMAT JSON ENVELOPE NONE;
CREATE SOURCE auctions_source
FROM KAFKA CONNECTION kafka_conn (TOPIC '${auction_topic}')
FORMAT JSON ENVELOPE NONE;
CREATE MATERIALIZED VIEW ${QUERY_RESULT_RELATION} AS
SELECT
  (b.data->>'auction')::bigint AS auction,
  (b.data->>'bidder')::bigint AS bidder,
  (b.data->>'price')::bigint AS projected_price,
  (a.data->>'seller')::bigint AS seller
FROM bids_source AS b
JOIN auctions_source AS a
  ON (b.data->>'auction')::bigint = (a.data->>'id')::bigint
WHERE (a.data->>'category')::bigint = 10;
CREATE MATERIALIZED VIEW ${QUERY_COUNT_RELATION} AS
SELECT COUNT(*)::bigint AS row_count FROM ${QUERY_RESULT_RELATION};
SQL
        ;;
    esac
  fi

  run_psql_file "${MATERIALIZE_SQL_PORT}" materialize materialize "${artifact_dir}/setup.sql"

  local start_ms end_ms total_ms rows_per_sec
  start_ms="$(date +%s%3N)"
  if [[ "${BENCH_QUERY}" == "join" ]]; then
    produce_query_inputs "${bid_topic}" "${auction_topic}"
  else
    produce_query_inputs "${bid_topic}"
  fi
  poll_pg_count "${MATERIALIZE_SQL_PORT}" materialize materialize "SELECT row_count FROM ${QUERY_COUNT_RELATION}" "Materialize"
  end_ms="$(date +%s%3N)"
  total_ms=$((end_ms - start_ms))
  rows_per_sec=$((INPUT_ROWS_TOTAL * 1000 / total_ms))

  printf '%s\n' "${object_mode}" > "${artifact_dir}/mode.txt"
  write_result materialize "${artifact_dir}" "${total_ms}" "${PRODUCE_MS}" "${POST_PRODUCE_WAIT_MS}" "${rows_per_sec}" "count_view_pgwire"
  append_summary_row materialize "${total_ms}" "${PRODUCE_MS}" "${POST_PRODUCE_WAIT_MS}" "${rows_per_sec}"
}

risingwave_benchmark() {
  local artifact_dir="${ARTIFACT_ROOT}/${RUN_ID}/risingwave"
  local bid_topic="risingwave_bids_${RUN_ID}"
  local auction_topic="risingwave_auctions_${RUN_ID}"
  local kafka_fetch_profile="default"
  mkdir -p "${artifact_dir}"

  docker rm -f "${RISINGWAVE_CONTAINER}" >/dev/null 2>&1 || true
  log "starting RisingWave single-node container"
  docker pull "${RISINGWAVE_IMAGE}" >/dev/null
  capture_image_metadata "${RISINGWAVE_IMAGE}" "${artifact_dir}/image_metadata.json"
  if env_enabled "${RISINGWAVE_IN_MEMORY}"; then
    docker run -d \
      --name "${RISINGWAVE_CONTAINER}" \
      --network "${NETWORK_NAME}" \
      -p "${RISINGWAVE_SQL_PORT}:4566" \
      "${RISINGWAVE_IMAGE}" \
      single_node --in-memory >/dev/null
  else
    docker run -d \
      --name "${RISINGWAVE_CONTAINER}" \
      --network "${NETWORK_NAME}" \
      -p "${RISINGWAVE_SQL_PORT}:4566" \
      "${RISINGWAVE_IMAGE}" \
      single_node >/dev/null
  fi

  wait_for_pg "${RISINGWAVE_SQL_PORT}" root dev "RisingWave"
  reset_topic "${bid_topic}"
  if [[ "${BENCH_QUERY}" == "join" ]]; then
    reset_topic "${auction_topic}"
  fi

  if env_enabled "${KAFKA_LATENCY_FETCH_PROFILE}"; then
    kafka_fetch_profile="latency"
    case "${BENCH_QUERY}" in
      filter_projection)
        cat > "${artifact_dir}/setup.sql" <<SQL
DROP MATERIALIZED VIEW IF EXISTS ${QUERY_COUNT_RELATION};
DROP MATERIALIZED VIEW IF EXISTS ${QUERY_RESULT_RELATION};
DROP SOURCE IF EXISTS bids_source;
CREATE SOURCE bids_source (
  auction BIGINT,
  bidder BIGINT,
  price BIGINT,
  channel VARCHAR,
  url VARCHAR,
  date_time BIGINT,
  extra VARCHAR
)
WITH (
  connector = 'kafka',
  topic = '${bid_topic}',
  properties.bootstrap.server = '${BROKER_ADDR_FROM_CONTAINER}',
  scan.startup.mode = 'earliest',
  properties.fetch.wait.max.ms = '${KAFKA_FETCH_WAIT_MAX_MS}',
  properties.fetch.queue.backoff.ms = '${KAFKA_FETCH_QUEUE_BACKOFF_MS}',
  properties.fetch.min.bytes = '${KAFKA_FETCH_MIN_BYTES}'
)
FORMAT PLAIN ENCODE JSON;
CREATE MATERIALIZED VIEW ${QUERY_RESULT_RELATION} AS
SELECT auction, bidder, price AS projected_price
FROM bids_source
WHERE auction <= 5000;
CREATE MATERIALIZED VIEW ${QUERY_COUNT_RELATION} AS
SELECT COUNT(*)::BIGINT AS row_count FROM ${QUERY_RESULT_RELATION};
SQL
        ;;
      join)
        cat > "${artifact_dir}/setup.sql" <<SQL
DROP MATERIALIZED VIEW IF EXISTS ${QUERY_COUNT_RELATION};
DROP MATERIALIZED VIEW IF EXISTS ${QUERY_RESULT_RELATION};
DROP SOURCE IF EXISTS bids_source;
DROP SOURCE IF EXISTS auctions_source;
CREATE SOURCE bids_source (
  auction BIGINT,
  bidder BIGINT,
  price BIGINT,
  channel VARCHAR,
  url VARCHAR,
  date_time BIGINT,
  extra VARCHAR
)
WITH (
  connector = 'kafka',
  topic = '${bid_topic}',
  properties.bootstrap.server = '${BROKER_ADDR_FROM_CONTAINER}',
  scan.startup.mode = 'earliest',
  properties.fetch.wait.max.ms = '${KAFKA_FETCH_WAIT_MAX_MS}',
  properties.fetch.queue.backoff.ms = '${KAFKA_FETCH_QUEUE_BACKOFF_MS}',
  properties.fetch.min.bytes = '${KAFKA_FETCH_MIN_BYTES}'
)
FORMAT PLAIN ENCODE JSON;
CREATE SOURCE auctions_source (
  id BIGINT,
  item_name VARCHAR,
  description VARCHAR,
  initial_bid BIGINT,
  reserve BIGINT,
  seller BIGINT,
  category BIGINT,
  expires BIGINT,
  date_time BIGINT,
  extra VARCHAR
)
WITH (
  connector = 'kafka',
  topic = '${auction_topic}',
  properties.bootstrap.server = '${BROKER_ADDR_FROM_CONTAINER}',
  scan.startup.mode = 'earliest',
  properties.fetch.wait.max.ms = '${KAFKA_FETCH_WAIT_MAX_MS}',
  properties.fetch.queue.backoff.ms = '${KAFKA_FETCH_QUEUE_BACKOFF_MS}',
  properties.fetch.min.bytes = '${KAFKA_FETCH_MIN_BYTES}'
)
FORMAT PLAIN ENCODE JSON;
CREATE MATERIALIZED VIEW ${QUERY_RESULT_RELATION} AS
SELECT b.auction, b.bidder, b.price AS projected_price, a.seller
FROM bids_source AS b
JOIN auctions_source AS a
  ON b.auction = a.id
WHERE a.category = 10;
CREATE MATERIALIZED VIEW ${QUERY_COUNT_RELATION} AS
SELECT COUNT(*)::BIGINT AS row_count FROM ${QUERY_RESULT_RELATION};
SQL
        ;;
    esac
  else
    case "${BENCH_QUERY}" in
      filter_projection)
        cat > "${artifact_dir}/setup.sql" <<SQL
DROP MATERIALIZED VIEW IF EXISTS ${QUERY_COUNT_RELATION};
DROP MATERIALIZED VIEW IF EXISTS ${QUERY_RESULT_RELATION};
DROP SOURCE IF EXISTS bids_source;
CREATE SOURCE bids_source (
  auction BIGINT,
  bidder BIGINT,
  price BIGINT,
  channel VARCHAR,
  url VARCHAR,
  date_time BIGINT,
  extra VARCHAR
)
WITH (
  connector = 'kafka',
  topic = '${bid_topic}',
  properties.bootstrap.server = '${BROKER_ADDR_FROM_CONTAINER}',
  scan.startup.mode = 'earliest'
)
FORMAT PLAIN ENCODE JSON;
CREATE MATERIALIZED VIEW ${QUERY_RESULT_RELATION} AS
SELECT auction, bidder, price AS projected_price
FROM bids_source
WHERE auction <= 5000;
CREATE MATERIALIZED VIEW ${QUERY_COUNT_RELATION} AS
SELECT COUNT(*)::BIGINT AS row_count FROM ${QUERY_RESULT_RELATION};
SQL
        ;;
      join)
        cat > "${artifact_dir}/setup.sql" <<SQL
DROP MATERIALIZED VIEW IF EXISTS ${QUERY_COUNT_RELATION};
DROP MATERIALIZED VIEW IF EXISTS ${QUERY_RESULT_RELATION};
DROP SOURCE IF EXISTS bids_source;
DROP SOURCE IF EXISTS auctions_source;
CREATE SOURCE bids_source (
  auction BIGINT,
  bidder BIGINT,
  price BIGINT,
  channel VARCHAR,
  url VARCHAR,
  date_time BIGINT,
  extra VARCHAR
)
WITH (
  connector = 'kafka',
  topic = '${bid_topic}',
  properties.bootstrap.server = '${BROKER_ADDR_FROM_CONTAINER}',
  scan.startup.mode = 'earliest'
)
FORMAT PLAIN ENCODE JSON;
CREATE SOURCE auctions_source (
  id BIGINT,
  item_name VARCHAR,
  description VARCHAR,
  initial_bid BIGINT,
  reserve BIGINT,
  seller BIGINT,
  category BIGINT,
  expires BIGINT,
  date_time BIGINT,
  extra VARCHAR
)
WITH (
  connector = 'kafka',
  topic = '${auction_topic}',
  properties.bootstrap.server = '${BROKER_ADDR_FROM_CONTAINER}',
  scan.startup.mode = 'earliest'
)
FORMAT PLAIN ENCODE JSON;
CREATE MATERIALIZED VIEW ${QUERY_RESULT_RELATION} AS
SELECT b.auction, b.bidder, b.price AS projected_price, a.seller
FROM bids_source AS b
JOIN auctions_source AS a
  ON b.auction = a.id
WHERE a.category = 10;
CREATE MATERIALIZED VIEW ${QUERY_COUNT_RELATION} AS
SELECT COUNT(*)::BIGINT AS row_count FROM ${QUERY_RESULT_RELATION};
SQL
        ;;
    esac
  fi

  run_psql_file "${RISINGWAVE_SQL_PORT}" root dev "${artifact_dir}/setup.sql"

  local start_ms end_ms total_ms rows_per_sec
  start_ms="$(date +%s%3N)"
  if [[ "${BENCH_QUERY}" == "join" ]]; then
    produce_query_inputs "${bid_topic}" "${auction_topic}"
  else
    produce_query_inputs "${bid_topic}"
  fi
  poll_pg_count "${RISINGWAVE_SQL_PORT}" root dev "SELECT row_count FROM ${QUERY_COUNT_RELATION}" "RisingWave"
  end_ms="$(date +%s%3N)"
  total_ms=$((end_ms - start_ms))
  rows_per_sec=$((INPUT_ROWS_TOTAL * 1000 / total_ms))

  printf '%s\n' "$(env_enabled "${RISINGWAVE_IN_MEMORY}" && printf 'true' || printf 'false')" \
    > "${artifact_dir}/in_memory.txt"
  printf '%s\n' "${kafka_fetch_profile}" > "${artifact_dir}/kafka_fetch_profile.txt"
  write_result risingwave "${artifact_dir}" "${total_ms}" "${PRODUCE_MS}" "${POST_PRODUCE_WAIT_MS}" "${rows_per_sec}" "count_view_pgwire"
  append_summary_row risingwave "${total_ms}" "${PRODUCE_MS}" "${POST_PRODUCE_WAIT_MS}" "${rows_per_sec}"
}

feldera_benchmark() {
  local artifact_dir="${ARTIFACT_ROOT}/${RUN_ID}/feldera"
  local bid_topic="feldera_bids_${RUN_ID}"
  local auction_topic="feldera_auctions_${RUN_ID}"
  local pipeline="stream_bench_${RUN_ID}"
  local kafka_fetch_profile="default"
  mkdir -p "${artifact_dir}"

  docker rm -f "${FELDERA_CONTAINER}" >/dev/null 2>&1 || true
  log "starting Feldera pipeline-manager container"
  docker pull "${FELDERA_IMAGE}" >/dev/null
  capture_image_metadata "${FELDERA_IMAGE}" "${artifact_dir}/image_metadata.json"
  docker run -d \
    --name "${FELDERA_CONTAINER}" \
    --network "${NETWORK_NAME}" \
    -p "${FELDERA_HTTP_PORT}:8080" \
    "${FELDERA_IMAGE}" >/dev/null

  wait_for_http_ok "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines" "Feldera"
  reset_topic "${bid_topic}"
  if [[ "${BENCH_QUERY}" == "join" ]]; then
    reset_topic "${auction_topic}"
  fi

  if env_enabled "${KAFKA_LATENCY_FETCH_PROFILE}"; then
    kafka_fetch_profile="latency"
    case "${BENCH_QUERY}" in
      filter_projection)
        cat > "${artifact_dir}/program.sql" <<SQL
CREATE TABLE bids_source (
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    channel VARCHAR,
    url VARCHAR,
    date_time BIGINT,
    extra VARCHAR
) WITH (
    'connectors' = '[{
      "transport": {
        "name": "kafka_input",
        "config": {
          "topic": "${bid_topic}",
          "start_from": "earliest",
          "bootstrap.servers": "${BROKER_ADDR_FROM_CONTAINER}",
          "fetch.wait.max.ms": "${KAFKA_FETCH_WAIT_MAX_MS}",
          "fetch.queue.backoff.ms": "${KAFKA_FETCH_QUEUE_BACKOFF_MS}",
          "fetch.min.bytes": "${KAFKA_FETCH_MIN_BYTES}"
        }
      },
      "format": {
        "name": "json",
        "config": {
          "update_format": "raw",
          "array": false
        }
      }
    }]'
);

CREATE MATERIALIZED VIEW ${QUERY_RESULT_RELATION} AS
SELECT auction, bidder, price AS projected_price
FROM bids_source
WHERE auction <= 5000;

CREATE MATERIALIZED VIEW ${QUERY_COUNT_RELATION} AS
SELECT COUNT(*) AS ROW_COUNT FROM ${QUERY_RESULT_RELATION};
SQL
        ;;
      join)
        cat > "${artifact_dir}/program.sql" <<SQL
CREATE TABLE bids_source (
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    channel VARCHAR,
    url VARCHAR,
    date_time BIGINT,
    extra VARCHAR
) WITH (
    'connectors' = '[{
      "transport": {
        "name": "kafka_input",
        "config": {
          "topic": "${bid_topic}",
          "start_from": "earliest",
          "bootstrap.servers": "${BROKER_ADDR_FROM_CONTAINER}",
          "fetch.wait.max.ms": "${KAFKA_FETCH_WAIT_MAX_MS}",
          "fetch.queue.backoff.ms": "${KAFKA_FETCH_QUEUE_BACKOFF_MS}",
          "fetch.min.bytes": "${KAFKA_FETCH_MIN_BYTES}"
        }
      },
      "format": {
        "name": "json",
        "config": {
          "update_format": "raw",
          "array": false
        }
      }
    }]'
);

CREATE TABLE auctions_source (
    id BIGINT,
    item_name VARCHAR,
    description VARCHAR,
    initial_bid BIGINT,
    reserve BIGINT,
    seller BIGINT,
    category BIGINT,
    expires BIGINT,
    date_time BIGINT,
    extra VARCHAR
) WITH (
    'connectors' = '[{
      "transport": {
        "name": "kafka_input",
        "config": {
          "topic": "${auction_topic}",
          "start_from": "earliest",
          "bootstrap.servers": "${BROKER_ADDR_FROM_CONTAINER}",
          "fetch.wait.max.ms": "${KAFKA_FETCH_WAIT_MAX_MS}",
          "fetch.queue.backoff.ms": "${KAFKA_FETCH_QUEUE_BACKOFF_MS}",
          "fetch.min.bytes": "${KAFKA_FETCH_MIN_BYTES}"
        }
      },
      "format": {
        "name": "json",
        "config": {
          "update_format": "raw",
          "array": false
        }
      }
    }]'
);

CREATE MATERIALIZED VIEW ${QUERY_RESULT_RELATION} AS
SELECT b.auction, b.bidder, b.price AS projected_price, a.seller
FROM bids_source AS b
JOIN auctions_source AS a ON b.auction = a.id
WHERE a.category = 10;

CREATE MATERIALIZED VIEW ${QUERY_COUNT_RELATION} AS
SELECT COUNT(*) AS ROW_COUNT FROM ${QUERY_RESULT_RELATION};
SQL
        ;;
    esac
  else
    case "${BENCH_QUERY}" in
      filter_projection)
        cat > "${artifact_dir}/program.sql" <<SQL
CREATE TABLE bids_source (
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    channel VARCHAR,
    url VARCHAR,
    date_time BIGINT,
    extra VARCHAR
) WITH (
    'connectors' = '[{
      "transport": {
        "name": "kafka_input",
        "config": {
          "topic": "${bid_topic}",
          "start_from": "earliest",
          "bootstrap.servers": "${BROKER_ADDR_FROM_CONTAINER}"
        }
      },
      "format": {
        "name": "json",
        "config": {
          "update_format": "raw",
          "array": false
        }
      }
    }]'
);

CREATE MATERIALIZED VIEW ${QUERY_RESULT_RELATION} AS
SELECT auction, bidder, price AS projected_price
FROM bids_source
WHERE auction <= 5000;

CREATE MATERIALIZED VIEW ${QUERY_COUNT_RELATION} AS
SELECT COUNT(*) AS ROW_COUNT FROM ${QUERY_RESULT_RELATION};
SQL
        ;;
      join)
        cat > "${artifact_dir}/program.sql" <<SQL
CREATE TABLE bids_source (
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    channel VARCHAR,
    url VARCHAR,
    date_time BIGINT,
    extra VARCHAR
) WITH (
    'connectors' = '[{
      "transport": {
        "name": "kafka_input",
        "config": {
          "topic": "${bid_topic}",
          "start_from": "earliest",
          "bootstrap.servers": "${BROKER_ADDR_FROM_CONTAINER}"
        }
      },
      "format": {
        "name": "json",
        "config": {
          "update_format": "raw",
          "array": false
        }
      }
    }]'
);

CREATE TABLE auctions_source (
    id BIGINT,
    item_name VARCHAR,
    description VARCHAR,
    initial_bid BIGINT,
    reserve BIGINT,
    seller BIGINT,
    category BIGINT,
    expires BIGINT,
    date_time BIGINT,
    extra VARCHAR
) WITH (
    'connectors' = '[{
      "transport": {
        "name": "kafka_input",
        "config": {
          "topic": "${auction_topic}",
          "start_from": "earliest",
          "bootstrap.servers": "${BROKER_ADDR_FROM_CONTAINER}"
        }
      },
      "format": {
        "name": "json",
        "config": {
          "update_format": "raw",
          "array": false
        }
      }
    }]'
);

CREATE MATERIALIZED VIEW ${QUERY_RESULT_RELATION} AS
SELECT b.auction, b.bidder, b.price AS projected_price, a.seller
FROM bids_source AS b
JOIN auctions_source AS a ON b.auction = a.id
WHERE a.category = 10;

CREATE MATERIALIZED VIEW ${QUERY_COUNT_RELATION} AS
SELECT COUNT(*) AS ROW_COUNT FROM ${QUERY_RESULT_RELATION};
SQL
        ;;
    esac
  fi

  if env_enabled "${FELDERA_BEST_EFFORT_IN_MEMORY}"; then
    curl -fsS -X PUT "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}" \
      -H 'Content-Type: application/json' \
      -d "$(jq -Rsn \
        --rawfile code "${artifact_dir}/program.sql" \
        --arg name "${pipeline}" \
        --argjson workers "${FELDERA_WORKERS}" \
        --argjson min_storage_bytes "${FELDERA_MIN_STORAGE_BYTES}" \
        --argjson min_step_storage_bytes "${FELDERA_MIN_STEP_STORAGE_BYTES}" \
        '{name: $name, description: "Floe stream engine comparison benchmark", runtime_config: {workers: $workers, storage: {min_storage_bytes: $min_storage_bytes, min_step_storage_bytes: $min_step_storage_bytes}}, program_config: {}, program_code: $code}')" \
        > "${artifact_dir}/pipeline_create.json"
  else
    curl -fsS -X PUT "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}" \
      -H 'Content-Type: application/json' \
      -d "$(jq -Rsn \
        --rawfile code "${artifact_dir}/program.sql" \
        --arg name "${pipeline}" \
        --argjson workers "${FELDERA_WORKERS}" \
        '{name: $name, description: "Floe stream engine comparison benchmark", runtime_config: {workers: $workers}, program_config: {}, program_code: $code}')" \
        > "${artifact_dir}/pipeline_create.json"
  fi

  poll_feldera_program_success "${pipeline}"

  curl -fsS -X POST "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}/start" >/dev/null
  poll_feldera_running "${pipeline}"

  local start_ms end_ms total_ms rows_per_sec
  start_ms="$(date +%s%3N)"
  if [[ "${BENCH_QUERY}" == "join" ]]; then
    produce_query_inputs "${bid_topic}" "${auction_topic}"
  else
    produce_query_inputs "${bid_topic}"
  fi
  poll_feldera_completion "${pipeline}"
  end_ms="$(date +%s%3N)"
  total_ms=$((end_ms - start_ms))
  rows_per_sec=$((INPUT_ROWS_TOTAL * 1000 / total_ms))

  if env_enabled "${FELDERA_BEST_EFFORT_IN_MEMORY}"; then
    jq -n \
      --argjson min_storage_bytes "${FELDERA_MIN_STORAGE_BYTES}" \
      --argjson min_step_storage_bytes "${FELDERA_MIN_STEP_STORAGE_BYTES}" \
      --arg kafka_fetch_profile "${kafka_fetch_profile}" \
      '{best_effort_in_memory: true, min_storage_bytes: $min_storage_bytes, min_step_storage_bytes: $min_step_storage_bytes, kafka_fetch_profile: $kafka_fetch_profile}' \
      > "${artifact_dir}/runtime_storage_mode.json"
  else
    jq -n --arg kafka_fetch_profile "${kafka_fetch_profile}" \
      '{best_effort_in_memory: false, kafka_fetch_profile: $kafka_fetch_profile}' \
      > "${artifact_dir}/runtime_storage_mode.json"
  fi
  write_result feldera "${artifact_dir}" "${total_ms}" "${PRODUCE_MS}" "${POST_PRODUCE_WAIT_MS}" "${rows_per_sec}" \
    "$(if [[ "${FELDERA_COMPLETION_MODE}" == "count" ]]; then printf 'count_view_adhoc_query'; else printf 'completed_records_stats'; fi)"
  append_summary_row feldera "${total_ms}" "${PRODUCE_MS}" "${POST_PRODUCE_WAIT_MS}" "${rows_per_sec}"
}

floe_benchmark() {
  local artifact_dir="${ARTIFACT_ROOT}/${RUN_ID}/floe"
  local bid_topic="floe_bids_${RUN_ID}"
  local auction_topic="floe_auctions_${RUN_ID}"
  local bid_group_id="${FLOE_KAFKA_GROUP_ID_PREFIX}_${RUN_ID}_bids"
  local auction_group_id="${FLOE_KAFKA_GROUP_ID_PREFIX}_${RUN_ID}_auctions"
  local config_path="${artifact_dir}/floe_config.json"
  local mv_program
  mkdir -p "${artifact_dir}"

  stop_floe_process
  reset_topic "${bid_topic}"
  if [[ "${BENCH_QUERY}" == "join" ]]; then
    reset_topic "${auction_topic}"
  fi
  capture_floe_metadata "${artifact_dir}/binary_metadata.json"

  case "${BENCH_QUERY}" in
    filter_projection)
      jq -n \
        --arg brokers "${BROKER_ADDR}" \
        --arg topic "${bid_topic}" \
        --arg group_id "${bid_group_id}" \
        --argjson kafka_poll_ms "${FLOE_KAFKA_POLL_MS}" \
        --argjson kafka_max_messages_per_tick "${FLOE_KAFKA_MAX_MESSAGES_PER_TICK}" \
        --argjson ingest_queue_capacity "${FLOE_INGEST_QUEUE_CAPACITY}" \
        --argjson ingest_batch_size "${FLOE_INGEST_BATCH_SIZE}" \
        --argjson ingest_batch_per_source "${FLOE_INGEST_BATCH_PER_SOURCE}" \
        --argjson ingest_batch_per_connector "${FLOE_INGEST_BATCH_PER_CONNECTOR}" \
        --argjson mv_retain_last "${FLOE_MV_RETAIN_LAST}" \
        '{
          connectors: [
            {
              type: "kafka",
              brokers: $brokers,
              topics: [$topic],
              group_id: $group_id,
              default_source: "nexmark_bid",
              poll_ms: $kafka_poll_ms,
              max_messages_per_tick: $kafka_max_messages_per_tick
            }
          ],
          runtime: {
            ingest_queue_capacity: $ingest_queue_capacity,
            ingest_batch_size: $ingest_batch_size,
            ingest_batch_per_source: $ingest_batch_per_source,
            ingest_batch_per_connector: $ingest_batch_per_connector,
            mv_retain_last: $mv_retain_last
          },
          storage: {
            await_durable: false
          }
        }' > "${config_path}"
      mv_program="CREATE MATERIALIZED VIEW ${QUERY_RESULT_RELATION} AS SELECT auction, bidder, price AS projected_price FROM nexmark_bid WHERE auction <= 5000;"
      ;;
    join)
      jq -n \
        --arg brokers "${BROKER_ADDR}" \
        --arg bid_topic "${bid_topic}" \
        --arg auction_topic "${auction_topic}" \
        --arg bid_group_id "${bid_group_id}" \
        --arg auction_group_id "${auction_group_id}" \
        --argjson kafka_poll_ms "${FLOE_KAFKA_POLL_MS}" \
        --argjson kafka_max_messages_per_tick "${FLOE_KAFKA_MAX_MESSAGES_PER_TICK}" \
        --argjson ingest_queue_capacity "${FLOE_INGEST_QUEUE_CAPACITY}" \
        --argjson ingest_batch_size "${FLOE_INGEST_BATCH_SIZE}" \
        --argjson ingest_batch_per_source "${FLOE_INGEST_BATCH_PER_SOURCE}" \
        --argjson ingest_batch_per_connector "${FLOE_INGEST_BATCH_PER_CONNECTOR}" \
        --argjson mv_retain_last "${FLOE_MV_RETAIN_LAST}" \
        '{
          connectors: [
            {
              type: "kafka",
              brokers: $brokers,
              topics: [$bid_topic],
              group_id: $bid_group_id,
              default_source: "nexmark_bid",
              poll_ms: $kafka_poll_ms,
              max_messages_per_tick: $kafka_max_messages_per_tick
            },
            {
              type: "kafka",
              brokers: $brokers,
              topics: [$auction_topic],
              group_id: $auction_group_id,
              default_source: "nexmark_auction",
              poll_ms: $kafka_poll_ms,
              max_messages_per_tick: $kafka_max_messages_per_tick
            }
          ],
          runtime: {
            ingest_queue_capacity: $ingest_queue_capacity,
            ingest_batch_size: $ingest_batch_size,
            ingest_batch_per_source: $ingest_batch_per_source,
            ingest_batch_per_connector: $ingest_batch_per_connector,
            mv_retain_last: $mv_retain_last
          },
          storage: {
            await_durable: false
          }
        }' > "${config_path}"
      mv_program="CREATE MATERIALIZED VIEW ${QUERY_RESULT_RELATION} AS SELECT b.auction, b.bidder, b.price AS projected_price, a.seller FROM nexmark_bid AS b JOIN nexmark_auction AS a ON b.auction = a.id WHERE a.category = 10;"
      ;;
  esac

  log "starting Floe native benchmark process"
  FLOE_PG_ADDR="127.0.0.1:${FLOE_PG_PORT}" \
    FLOE_ADMIN_PORT=0 \
    "${REPO_ROOT}/target/release/floe-node" run \
    --slatedb-await-durable false \
    --slatedb-l0-sst-bytes "${FLOE_L0_SST_BYTES}" \
    --slatedb-max-unflushed-bytes "${FLOE_MAX_UNFLUSHED_BYTES}" \
    --config "${config_path}" \
    --mv-query "${mv_program}" \
    > "${artifact_dir}/floe-node.stdout.log" \
    2> "${artifact_dir}/floe-node.stderr.log" &
  FLOE_NODE_PID=$!

  wait_for_floe_pg "${artifact_dir}"

  local start_ms end_ms total_ms rows_per_sec
  start_ms="$(date +%s%3N)"
  if [[ "${BENCH_QUERY}" == "join" ]]; then
    produce_query_inputs "${bid_topic}" "${auction_topic}"
  else
    produce_query_inputs "${bid_topic}"
  fi
  poll_pg_count "${FLOE_PG_PORT}" postgres postgres "SELECT COUNT(*)::BIGINT FROM ${QUERY_RESULT_RELATION}" "Floe"
  end_ms="$(date +%s%3N)"
  total_ms=$((end_ms - start_ms))
  rows_per_sec=$((INPUT_ROWS_TOTAL * 1000 / total_ms))

  stop_floe_process
  write_result floe "${artifact_dir}" "${total_ms}" "${PRODUCE_MS}" "${POST_PRODUCE_WAIT_MS}" "${rows_per_sec}" "count_query_pgwire"
  append_summary_row floe "${total_ms}" "${PRODUCE_MS}" "${POST_PRODUCE_WAIT_MS}" "${rows_per_sec}"
}

main() {
  mkdir -p "${ARTIFACT_ROOT}/${RUN_ID}"
  configure_benchmark_profile

  cat > "${RESULTS_FILE}" <<EOF
# Stream Engine Benchmark Summary

Query: \`${BENCH_QUERY}\` (${QUERY_DESCRIPTION})
Total input rows: \`${INPUT_ROWS_TOTAL}\`
Expected output rows: \`${EXPECTED_ROWS}\`

| Engine | Ingest Complete (s) | Produce (s) | Post-Produce Wait (s) | Input Rows/s |
| --- | ---: | ---: | ---: | ---: |
EOF

  ensure_redpanda
  build_producer
  write_run_context

  if [[ "${ENGINE}" == "floe" || "${ENGINE}" == "all" ]]; then
    build_floe_node
  fi

  case "${ENGINE}" in
    floe)
      floe_benchmark
      ;;
    materialize)
      materialize_benchmark
      ;;
    risingwave)
      risingwave_benchmark
      ;;
    feldera)
      feldera_benchmark
      ;;
    all)
      floe_benchmark
      materialize_benchmark
      risingwave_benchmark
      feldera_benchmark
      ;;
    *)
      die "unknown engine '${ENGINE}' (expected floe|materialize|risingwave|feldera|all)"
      ;;
  esac

  log "results written to ${RESULTS_FILE}"
  cat "${RESULTS_FILE}"
}

main "$@"
