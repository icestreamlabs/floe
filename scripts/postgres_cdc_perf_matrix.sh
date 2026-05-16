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
if [[ -z "${BENCH_MODES+x}" && "${DATASET}" == "tpch-all" ]]; then
  BENCH_MODES="snapshot"
else
  BENCH_MODES="${BENCH_MODES:-snapshot live_insert snapshot_live_update}"
fi
TIMEOUT_SECS="${TIMEOUT_SECS:-900}"
BUILD_RELEASE="${BUILD_RELEASE:-1}"
LIVE_WRITE_CHUNK_ROWS="${LIVE_WRITE_CHUNK_ROWS:-0}"
LIVE_WRITE_SLEEP_MS="${LIVE_WRITE_SLEEP_MS:-0}"
BUFFER_MAX_PENDING_BYTES="${BUFFER_MAX_PENDING_BYTES:-}"
BUFFER_MAX_PENDING_AGE_MS="${BUFFER_MAX_PENDING_AGE_MS:-}"
FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH="${FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH:-16384}"
FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS="${FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS:-1}"
FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS="${FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS:-1}"
STOP_ON_FAIL="${STOP_ON_FAIL:-0}"

SUMMARY_CSV="${ARTIFACT_ROOT}/summary.csv"
SUMMARY_MD="${ARTIFACT_ROOT}/summary.md"

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
status,mode,format,rows,source_rows,expected_messages,observed_messages,end_to_end_seconds,end_to_end_rows_per_second,total_bytes,wall_mb_per_second,artifact_dir
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

Buffer max pending age ms: \`${BUFFER_MAX_PENDING_AGE_MS:-unset}\`

Postgres snapshot rows per batch: \`${FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH}\`

Postgres snapshot max workers: \`${FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS}\`

Postgres snapshot intra-table chunks: \`${FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS}\`

| Status | Mode | Format | Rows | Source Rows | Expected Msgs | Observed Msgs | End-to-End (s) | Source Rows/s | Total Bytes | Wall MB/s | Artifacts |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
MD
}

append_result() {
  local status="$1"
  local run_dir="$2"
  local mode="$3"
  local format="$4"
  local rows="$5"
  local summary="${run_dir}/summary.env"
  local counter="${run_dir}/kafka-counter.log"

  local source_rows expected observed seconds rows_per_second total_bytes mb_per_second
  source_rows="$(env_value "${summary}" benchmark.source_rows)"
  expected="$(env_value "${summary}" benchmark.expected_kafka_messages)"
  observed="$(env_value "${counter}" cdc_counter.observed_messages)"
  seconds="$(env_value "${summary}" benchmark.end_to_end_seconds)"
  rows_per_second="$(env_value "${summary}" benchmark.end_to_end_rows_per_second)"
  total_bytes="$(env_value "${counter}" cdc_counter.total_bytes)"
  mb_per_second="$(env_value "${counter}" cdc_counter.wall_mb_per_second)"

  source_rows="${source_rows:-}"
  expected="${expected:-}"
  observed="${observed:-}"
  seconds="${seconds:-}"
  rows_per_second="${rows_per_second:-}"
  total_bytes="${total_bytes:-}"
  mb_per_second="${mb_per_second:-}"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "${status}" \
    "${mode}" \
    "${format}" \
    "${rows}" \
    "${source_rows}" \
    "${expected}" \
    "${observed}" \
    "${seconds}" \
    "${rows_per_second}" \
    "${total_bytes}" \
    "${mb_per_second}" \
    "${run_dir}" >>"${SUMMARY_CSV}"

  printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | `%s` |\n' \
    "${status}" \
    "${mode}" \
    "${format}" \
    "${rows}" \
    "${source_rows:-n/a}" \
    "${expected:-n/a}" \
    "${observed:-n/a}" \
    "${seconds:-n/a}" \
    "${rows_per_second:-n/a}" \
    "${total_bytes:-n/a}" \
    "${mb_per_second:-n/a}" \
    "${run_dir}" >>"${SUMMARY_MD}"
}

write_headers

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
        BUFFER_MAX_PENDING_AGE_MS="${BUFFER_MAX_PENDING_AGE_MS}" \
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
