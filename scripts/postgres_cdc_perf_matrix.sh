#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

RUN_ID="$(date +%Y%m%dT%H%M%S)"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-${REPO_ROOT}/target/cdc_bench_matrix/${RUN_ID}}"
ROWS_LIST="${ROWS_LIST:-1000 100000 1000000}"
DATASET="${DATASET:-synthetic-orders}"
TPCH_SCALE_FACTOR="${TPCH_SCALE_FACTOR:-0.01}"
PIPELINE_FORMATS="${PIPELINE_FORMATS:-floe-json debezium-json arrow-ipc}"
if [[ -z "${BENCH_MODES+x}" ]]; then
  case "${DATASET}" in
    tpch-all)
      BENCH_MODES="snapshot"
      ;;
    tpch-top2)
      BENCH_MODES="snapshot live_insert"
      ;;
    *)
      BENCH_MODES="snapshot live_insert snapshot_live_update"
      ;;
  esac
else
  BENCH_MODES="${BENCH_MODES:-snapshot live_insert snapshot_live_update}"
fi
TIMEOUT_SECS="${TIMEOUT_SECS:-900}"
BUILD_RELEASE="${BUILD_RELEASE:-1}"
LIVE_WRITE_CHUNK_ROWS="${LIVE_WRITE_CHUNK_ROWS:-0}"
LIVE_WRITE_SLEEP_MS="${LIVE_WRITE_SLEEP_MS:-0}"
BUFFER_MAX_PENDING_BYTES="${BUFFER_MAX_PENDING_BYTES:-}"
BUFFER_MAX_PENDING_RECORDS="${BUFFER_MAX_PENDING_RECORDS:-}"
BUFFER_MAX_PENDING_OBJECTS="${BUFFER_MAX_PENDING_OBJECTS:-}"
BUFFER_MAX_PENDING_AGE_MS="${BUFFER_MAX_PENDING_AGE_MS:-}"
KAFKA_METADATA_HEADERS="${KAFKA_METADATA_HEADERS:-false}"
FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH="${FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH:-16384}"
FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS="${FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS:-1}"
FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS="${FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS:-1}"
STOP_ON_FAIL="${STOP_ON_FAIL:-0}"

SUMMARY_CSV="${ARTIFACT_ROOT}/summary.csv"
SUMMARY_MD="${ARTIFACT_ROOT}/summary.md"
REPRODUCE_LOG="${ARTIFACT_ROOT}/reproduce.sh"

mkdir -p "${ARTIFACT_ROOT}"

log() {
  printf '[postgres-cdc-matrix] %s\n' "$*"
}

env_value() {
  local file="$1"
  local key="$2"
  if [[ ! -f "${file}" ]]; then
    return 0
  fi
  awk -F= -v key="${key}" '$1 == key { print substr($0, length(key) + 2); exit }' "${file}"
}

write_headers() {
  cat >"${SUMMARY_CSV}" <<CSV
status,mode,format,rows,source_rows,expected_messages,observed_messages,end_to_end_seconds,end_to_end_source_rows_per_second,kafka_stream_seconds,kafka_stream_messages_per_second,kafka_stream_source_rows_per_second,consumer_wall_source_rows_per_second,kafka_pre_stream_wait_seconds,harness_overhead_seconds,harness_overhead_percent,message_multiplier,postgres_load_rows_per_second,postgres_live_write_rows_per_second,total_bytes,wall_mb_per_second,stream_mb_per_second,artifact_dir
CSV
  cat >"${SUMMARY_MD}" <<MD
# Postgres CDC Benchmark Matrix

Run: \`${RUN_ID}\`

Rows list: \`${ROWS_LIST}\`

Dataset: \`${DATASET}\`

TPC-H scale factor: \`${TPCH_SCALE_FACTOR}\`

Formats: \`${PIPELINE_FORMATS}\`

Modes: \`${BENCH_MODES}\`

Live write chunk rows: \`${LIVE_WRITE_CHUNK_ROWS}\`

Live write sleep ms: \`${LIVE_WRITE_SLEEP_MS}\`

Buffer max pending bytes: \`${BUFFER_MAX_PENDING_BYTES:-unset}\`

Buffer max pending records: \`${BUFFER_MAX_PENDING_RECORDS:-unset}\`

Buffer max pending objects: \`${BUFFER_MAX_PENDING_OBJECTS:-unset}\`

Buffer max pending age ms: \`${BUFFER_MAX_PENDING_AGE_MS:-unset}\`

Kafka metadata headers: \`${KAFKA_METADATA_HEADERS}\`

Postgres snapshot rows per batch: \`${FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH}\`

Postgres snapshot max workers: \`${FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS}\`

Postgres snapshot intra-table chunks: \`${FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS}\`

| Status | Mode | Format | Rows | Source Rows | Expected Msgs | Observed Msgs | End-to-End (s) | E2E Source Rows/s | Kafka Stream (s) | Stream Msgs/s | Stream Source Rows/s | Consumer Wall Source Rows/s | Pre-Stream Wait (s) | Harness Overhead (s) | Harness Overhead % | Msg Multiplier | PG Load Rows/s | PG Live Write Rows/s | Total Bytes | Wall MB/s | Stream MB/s | Artifacts |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
MD
}

write_reproduce_command() {
  {
    echo "#!/usr/bin/env bash"
    echo "set -euo pipefail"
    printf 'ARTIFACT_ROOT=%q \\\n' "${ARTIFACT_ROOT}"
    printf 'ROWS_LIST=%q \\\n' "${ROWS_LIST}"
    printf 'DATASET=%q \\\n' "${DATASET}"
    printf 'TPCH_SCALE_FACTOR=%q \\\n' "${TPCH_SCALE_FACTOR}"
    printf 'PIPELINE_FORMATS=%q \\\n' "${PIPELINE_FORMATS}"
    printf 'BENCH_MODES=%q \\\n' "${BENCH_MODES}"
    printf 'TIMEOUT_SECS=%q \\\n' "${TIMEOUT_SECS}"
    printf 'BUILD_RELEASE=%q \\\n' "${BUILD_RELEASE}"
    printf 'LIVE_WRITE_CHUNK_ROWS=%q \\\n' "${LIVE_WRITE_CHUNK_ROWS}"
    printf 'LIVE_WRITE_SLEEP_MS=%q \\\n' "${LIVE_WRITE_SLEEP_MS}"
    printf 'BUFFER_MAX_PENDING_BYTES=%q \\\n' "${BUFFER_MAX_PENDING_BYTES}"
    printf 'BUFFER_MAX_PENDING_RECORDS=%q \\\n' "${BUFFER_MAX_PENDING_RECORDS}"
    printf 'BUFFER_MAX_PENDING_OBJECTS=%q \\\n' "${BUFFER_MAX_PENDING_OBJECTS}"
    printf 'BUFFER_MAX_PENDING_AGE_MS=%q \\\n' "${BUFFER_MAX_PENDING_AGE_MS}"
    printf 'KAFKA_METADATA_HEADERS=%q \\\n' "${KAFKA_METADATA_HEADERS}"
    printf 'FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH=%q \\\n' "${FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH}"
    printf 'FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS=%q \\\n' "${FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS}"
    printf 'FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS=%q \\\n' "${FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS}"
    printf 'STOP_ON_FAIL=%q \\\n' "${STOP_ON_FAIL}"
    printf 'scripts/postgres_cdc_perf_matrix.sh\n'
  } >"${REPRODUCE_LOG}"
  chmod +x "${REPRODUCE_LOG}"
}

append_result() {
  local status="$1"
  local run_dir="$2"
  local mode="$3"
  local format="$4"
  local rows="$5"
  local summary="${run_dir}/summary.env"
  local counter="${run_dir}/kafka-counter.log"

  local source_rows expected observed seconds rows_per_second stream_seconds stream_rows_per_second stream_source_rows_per_second consumer_wall_source_rows_per_second pre_stream_wait harness_overhead harness_overhead_percent message_multiplier postgres_load_rows_per_second postgres_live_write_rows_per_second total_bytes wall_mb_per_second stream_mb_per_second
  source_rows="$(env_value "${summary}" benchmark.source_rows)"
  expected="$(env_value "${summary}" benchmark.expected_kafka_messages)"
  observed="$(env_value "${counter}" cdc_counter.observed_messages)"
  seconds="$(env_value "${summary}" benchmark.end_to_end_seconds)"
  rows_per_second="$(env_value "${summary}" benchmark.end_to_end_rows_per_second)"
  stream_seconds="$(env_value "${summary}" benchmark.kafka_stream_seconds)"
  stream_rows_per_second="$(env_value "${summary}" benchmark.kafka_stream_rows_per_second)"
  stream_source_rows_per_second="$(env_value "${summary}" benchmark.kafka_stream_source_rows_per_second)"
  consumer_wall_source_rows_per_second="$(env_value "${summary}" benchmark.consumer_wall_source_rows_per_second)"
  pre_stream_wait="$(env_value "${summary}" benchmark.kafka_pre_stream_wait_seconds)"
  harness_overhead="$(env_value "${summary}" benchmark.harness_overhead_seconds)"
  harness_overhead_percent="$(env_value "${summary}" benchmark.harness_overhead_percent)"
  message_multiplier="$(env_value "${summary}" benchmark.message_multiplier)"
  postgres_load_rows_per_second="$(env_value "${summary}" benchmark.postgres_load_rows_per_second)"
  postgres_live_write_rows_per_second="$(env_value "${summary}" benchmark.postgres_live_write_rows_per_second)"
  total_bytes="$(env_value "${counter}" cdc_counter.total_bytes)"
  wall_mb_per_second="$(env_value "${counter}" cdc_counter.wall_mb_per_second)"
  stream_mb_per_second="$(env_value "${summary}" benchmark.kafka_stream_mb_per_second)"

  source_rows="${source_rows:-}"
  expected="${expected:-}"
  observed="${observed:-}"
  seconds="${seconds:-}"
  rows_per_second="${rows_per_second:-}"
  stream_seconds="${stream_seconds:-}"
  stream_rows_per_second="${stream_rows_per_second:-}"
  stream_source_rows_per_second="${stream_source_rows_per_second:-}"
  consumer_wall_source_rows_per_second="${consumer_wall_source_rows_per_second:-}"
  pre_stream_wait="${pre_stream_wait:-}"
  harness_overhead="${harness_overhead:-}"
  harness_overhead_percent="${harness_overhead_percent:-}"
  message_multiplier="${message_multiplier:-}"
  postgres_load_rows_per_second="${postgres_load_rows_per_second:-}"
  postgres_live_write_rows_per_second="${postgres_live_write_rows_per_second:-}"
  total_bytes="${total_bytes:-}"
  wall_mb_per_second="${wall_mb_per_second:-}"
  stream_mb_per_second="${stream_mb_per_second:-}"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "${status}" \
    "${mode}" \
    "${format}" \
    "${rows}" \
    "${source_rows}" \
    "${expected}" \
    "${observed}" \
    "${seconds}" \
    "${rows_per_second}" \
    "${stream_seconds}" \
    "${stream_rows_per_second}" \
    "${stream_source_rows_per_second}" \
    "${consumer_wall_source_rows_per_second}" \
    "${pre_stream_wait}" \
    "${harness_overhead}" \
    "${harness_overhead_percent}" \
    "${message_multiplier}" \
    "${postgres_load_rows_per_second}" \
    "${postgres_live_write_rows_per_second}" \
    "${total_bytes}" \
    "${wall_mb_per_second}" \
    "${stream_mb_per_second}" \
    "${run_dir}" >>"${SUMMARY_CSV}"

  printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | `%s` |\n' \
    "${status}" \
    "${mode}" \
    "${format}" \
    "${rows}" \
    "${source_rows:-n/a}" \
    "${expected:-n/a}" \
    "${observed:-n/a}" \
    "${seconds:-n/a}" \
    "${rows_per_second:-n/a}" \
    "${stream_seconds:-n/a}" \
    "${stream_rows_per_second:-n/a}" \
    "${stream_source_rows_per_second:-n/a}" \
    "${consumer_wall_source_rows_per_second:-n/a}" \
    "${pre_stream_wait:-n/a}" \
    "${harness_overhead:-n/a}" \
    "${harness_overhead_percent:-n/a}" \
    "${message_multiplier:-n/a}" \
    "${postgres_load_rows_per_second:-n/a}" \
    "${postgres_live_write_rows_per_second:-n/a}" \
    "${total_bytes:-n/a}" \
    "${wall_mb_per_second:-n/a}" \
    "${stream_mb_per_second:-n/a}" \
    "${run_dir}" >>"${SUMMARY_MD}"
}

write_headers
write_reproduce_command

for mode in ${BENCH_MODES}; do
  for format in ${PIPELINE_FORMATS}; do
    for rows in ${ROWS_LIST}; do
      run_dir="${ARTIFACT_ROOT}/${mode}/${format}/${rows}"
      mkdir -p "${run_dir}"
      log "running mode=${mode} format=${format} rows=${rows}"
      if (
        cd "${REPO_ROOT}"
        ARTIFACT_DIR="${run_dir}" \
        DATASET="${DATASET}" \
        TPCH_SCALE_FACTOR="${TPCH_SCALE_FACTOR}" \
        BENCH_MODE="${mode}" \
        PIPELINE_FORMAT="${format}" \
        ROWS="${rows}" \
        TIMEOUT_SECS="${TIMEOUT_SECS}" \
        BUILD_RELEASE="${BUILD_RELEASE}" \
        LIVE_WRITE_CHUNK_ROWS="${LIVE_WRITE_CHUNK_ROWS}" \
        LIVE_WRITE_SLEEP_MS="${LIVE_WRITE_SLEEP_MS}" \
        BUFFER_MAX_PENDING_BYTES="${BUFFER_MAX_PENDING_BYTES}" \
        BUFFER_MAX_PENDING_RECORDS="${BUFFER_MAX_PENDING_RECORDS}" \
        BUFFER_MAX_PENDING_OBJECTS="${BUFFER_MAX_PENDING_OBJECTS}" \
        BUFFER_MAX_PENDING_AGE_MS="${BUFFER_MAX_PENDING_AGE_MS}" \
        KAFKA_METADATA_HEADERS="${KAFKA_METADATA_HEADERS}" \
        FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH="${FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH}" \
        FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS="${FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS}" \
        FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS="${FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS}" \
        scripts/postgres_cdc_perf_local.sh
      ) >"${run_dir}/matrix-run.log" 2>&1; then
        append_result "ok" "${run_dir}" "${mode}" "${format}" "${rows}"
      else
        append_result "failed" "${run_dir}" "${mode}" "${format}" "${rows}"
        log "failed; see ${run_dir}/matrix-run.log"
        if [[ "${STOP_ON_FAIL}" == "1" ]]; then
          exit 1
        fi
      fi
    done
  done
done

log "summary written to ${SUMMARY_MD}"
cat "${SUMMARY_MD}"
