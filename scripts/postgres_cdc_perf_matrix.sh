#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

RUN_ID="$(date +%Y%m%dT%H%M%S)"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-${REPO_ROOT}/target/cdc_bench_matrix/${RUN_ID}}"
ROWS_LIST="${ROWS_LIST:-1000 100000 1000000}"
DATASET="${DATASET:-synthetic-orders}"
TPCH_SCALE_FACTOR="${TPCH_SCALE_FACTOR:-0.01}"
TARGETS="${TARGETS:-kafka}"
PIPELINE_FORMATS="${PIPELINE_FORMATS:-}"
DURABLE_REPLICATION_BUFFER="${DURABLE_REPLICATION_BUFFER:-true}"
DURABLE_REPLICATION_BUFFERS="${DURABLE_REPLICATION_BUFFERS:-${DURABLE_REPLICATION_BUFFER}}"
BENCH_MODES_EXPLICIT=0
if [[ -n "${BENCH_MODES+x}" && -n "${BENCH_MODES}" ]]; then
  BENCH_MODES_EXPLICIT=1
else
  BENCH_MODES=""
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
FLOE_PG_PORT="${FLOE_PG_PORT:-16432}"
FLOE_ADMIN_PORT="${FLOE_ADMIN_PORT:-18080}"
STOP_ON_FAIL="${STOP_ON_FAIL:-0}"

SUMMARY_CSV="${ARTIFACT_ROOT}/summary.csv"
SUMMARY_MD="${ARTIFACT_ROOT}/summary.md"
SUMMARY_JSONL="${ARTIFACT_ROOT}/summary.jsonl"
SUMMARY_JSON="${ARTIFACT_ROOT}/summary.json"
REPRODUCE_LOG="${ARTIFACT_ROOT}/reproduce.sh"

mkdir -p "${ARTIFACT_ROOT}"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required" >&2
  exit 1
fi

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

formats_for_target() {
  local target="$1"
  if [[ -n "${PIPELINE_FORMATS}" ]]; then
    printf '%s\n' "${PIPELINE_FORMATS}"
    return
  fi
  case "${target}" in
    kafka)
      printf '%s\n' "floe-json debezium-json arrow-ipc"
      ;;
    postgres)
      printf '%s\n' "floe-json"
      ;;
    *)
      echo "unsupported TARGETS entry '${target}'" >&2
      exit 1
      ;;
  esac
}

modes_for_target() {
  local target="$1"
  if [[ "${BENCH_MODES_EXPLICIT}" == "1" ]]; then
    printf '%s\n' "${BENCH_MODES}"
    return
  fi
  case "${DATASET}" in
    tpch-all|tpch-lineitem|tpch-lineitem-flat)
      printf '%s\n' "snapshot"
      ;;
    tpch-top2)
      printf '%s\n' "snapshot live_insert"
      ;;
    *)
      if [[ "${target}" == "postgres" ]]; then
        printf '%s\n' "snapshot live_insert"
      else
        printf '%s\n' "snapshot live_insert snapshot_live_update"
      fi
      ;;
  esac
}

write_headers() {
  cat >"${SUMMARY_CSV}" <<CSV
status,target,durable_buffer,mode,format,rows,source_rows,expected_messages,observed_messages,expected_postgres_sink_rows,observed_postgres_sink_rows,end_to_end_seconds,end_to_end_source_rows_per_second,target_observation_seconds,target_observed_records_per_second,kafka_stream_seconds,kafka_stream_messages_per_second,kafka_stream_source_rows_per_second,consumer_wall_source_rows_per_second,kafka_pre_stream_wait_seconds,postgres_sink_wait_seconds,postgres_sink_rows_per_second,harness_overhead_seconds,harness_overhead_percent,message_multiplier,postgres_load_rows_per_second,postgres_live_write_rows_per_second,total_bytes,wall_mb_per_second,stream_mb_per_second,artifact_dir
CSV
  : >"${SUMMARY_JSONL}"
  cat >"${SUMMARY_MD}" <<MD
# Postgres CDC Benchmark Matrix

Run: \`${RUN_ID}\`

Rows list: \`${ROWS_LIST}\`

Dataset: \`${DATASET}\`

TPC-H scale factor: \`${TPCH_SCALE_FACTOR}\`

Targets: \`${TARGETS}\`

Formats: \`${PIPELINE_FORMATS:-auto}\`

Modes: \`${BENCH_MODES:-auto}\`

Durable buffer modes: \`${DURABLE_REPLICATION_BUFFERS}\`

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

Floe pgwire port: \`${FLOE_PG_PORT}\`

Floe admin port: \`${FLOE_ADMIN_PORT}\`

| Status | Target | Durable Buffer | Mode | Format | Rows | Source Rows | Expected Msgs | Observed Msgs | Expected PG Sink Rows | Observed PG Sink Rows | End-to-End (s) | E2E Source Rows/s | Target Observation (s) | Target Records/s | Kafka Stream (s) | Stream Msgs/s | Stream Source Rows/s | Consumer Wall Source Rows/s | Pre-Stream Wait (s) | PG Sink Wait (s) | PG Sink Rows/s | Harness Overhead (s) | Harness Overhead % | Msg Multiplier | PG Load Rows/s | PG Live Write Rows/s | Total Bytes | Wall MB/s | Stream MB/s | Artifacts |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
MD
}

write_json_summary() {
  jq -s \
    --arg run_id "${RUN_ID}" \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg git_commit "$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || true)" \
    --arg git_branch "$(git -C "${REPO_ROOT}" rev-parse --abbrev-ref HEAD 2>/dev/null || true)" \
    --arg rows_list "${ROWS_LIST}" \
    --arg targets "${TARGETS}" \
    --arg durable_replication_buffers "${DURABLE_REPLICATION_BUFFERS}" \
    --arg dataset "${DATASET}" \
    --arg tpch_scale_factor "${TPCH_SCALE_FACTOR}" \
    --arg pipeline_formats "${PIPELINE_FORMATS:-auto}" \
    --arg bench_modes "${BENCH_MODES:-auto}" \
    --arg timeout_secs "${TIMEOUT_SECS}" \
    --arg build_release "${BUILD_RELEASE}" \
    --arg live_write_chunk_rows "${LIVE_WRITE_CHUNK_ROWS}" \
    --arg live_write_sleep_ms "${LIVE_WRITE_SLEEP_MS}" \
    --arg buffer_max_pending_bytes "${BUFFER_MAX_PENDING_BYTES}" \
    --arg buffer_max_pending_records "${BUFFER_MAX_PENDING_RECORDS}" \
    --arg buffer_max_pending_objects "${BUFFER_MAX_PENDING_OBJECTS}" \
    --arg buffer_max_pending_age_ms "${BUFFER_MAX_PENDING_AGE_MS}" \
    --arg kafka_metadata_headers "${KAFKA_METADATA_HEADERS}" \
    --arg postgres_snapshot_rows_per_batch "${FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH}" \
    --arg postgres_snapshot_max_workers "${FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS}" \
    --arg postgres_snapshot_intra_table_chunks "${FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS}" \
    --arg floe_pg_port "${FLOE_PG_PORT}" \
    --arg floe_admin_port "${FLOE_ADMIN_PORT}" \
    --arg artifact_root "${ARTIFACT_ROOT}" \
    --arg summary_csv "${SUMMARY_CSV}" \
    --arg summary_md "${SUMMARY_MD}" \
    --arg summary_jsonl "${SUMMARY_JSONL}" \
    '
    def words($value): $value | split(" ") | map(select(length > 0));
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
        generated_at: $generated_at,
        git_commit: $git_commit,
        git_branch: $git_branch,
        artifact_root: $artifact_root
      },
      config: {
        rows_list: words($rows_list) | map(tonumber),
        targets: words($targets),
        durable_replication_buffers: words($durable_replication_buffers) | map(maybe_bool(.)),
        dataset: $dataset,
        tpch_scale_factor: maybe_num($tpch_scale_factor),
        pipeline_formats: (if $pipeline_formats == "auto" then "auto" else words($pipeline_formats) end),
        bench_modes: (if $bench_modes == "auto" then "auto" else words($bench_modes) end),
        timeout_secs: maybe_num($timeout_secs),
        build_release: maybe_bool($build_release),
        live_write_chunk_rows: maybe_num($live_write_chunk_rows),
        live_write_sleep_ms: maybe_num($live_write_sleep_ms),
        buffer: {
          max_pending_bytes: maybe_num($buffer_max_pending_bytes),
          max_pending_records: maybe_num($buffer_max_pending_records),
          max_pending_objects: maybe_num($buffer_max_pending_objects),
          max_pending_age_ms: maybe_num($buffer_max_pending_age_ms)
        },
        kafka_metadata_headers: maybe_bool($kafka_metadata_headers),
        postgres_snapshot: {
          rows_per_batch: maybe_num($postgres_snapshot_rows_per_batch),
          max_workers: maybe_num($postgres_snapshot_max_workers),
          intra_table_chunks: maybe_num($postgres_snapshot_intra_table_chunks)
        },
        floe_ports: {
          pgwire: maybe_num($floe_pg_port),
          admin: maybe_num($floe_admin_port)
        }
      },
      artifacts: {
        summary_csv: $summary_csv,
        summary_md: $summary_md,
        summary_jsonl: $summary_jsonl
      },
      results: .
    }
    ' "${SUMMARY_JSONL}" >"${SUMMARY_JSON}"
}

write_reproduce_command() {
  {
    echo "#!/usr/bin/env bash"
    echo "set -euo pipefail"
    printf 'ARTIFACT_ROOT=%q \\\n' "${ARTIFACT_ROOT}"
    printf 'ROWS_LIST=%q \\\n' "${ROWS_LIST}"
    printf 'DATASET=%q \\\n' "${DATASET}"
    printf 'TPCH_SCALE_FACTOR=%q \\\n' "${TPCH_SCALE_FACTOR}"
    printf 'TARGETS=%q \\\n' "${TARGETS}"
    printf 'PIPELINE_FORMATS=%q \\\n' "${PIPELINE_FORMATS}"
    printf 'DURABLE_REPLICATION_BUFFERS=%q \\\n' "${DURABLE_REPLICATION_BUFFERS}"
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
    printf 'FLOE_PG_PORT=%q \\\n' "${FLOE_PG_PORT}"
    printf 'FLOE_ADMIN_PORT=%q \\\n' "${FLOE_ADMIN_PORT}"
    printf 'STOP_ON_FAIL=%q \\\n' "${STOP_ON_FAIL}"
    printf 'scripts/postgres_cdc_perf_matrix.sh\n'
  } >"${REPRODUCE_LOG}"
  chmod +x "${REPRODUCE_LOG}"
}

append_result() {
  local status="$1"
  local run_dir="$2"
  local target="$3"
  local durable_buffer="$4"
  local mode="$5"
  local format="$6"
  local rows="$7"
  local summary="${run_dir}/summary.env"

  local source_rows expected observed expected_sink observed_sink seconds rows_per_second target_observation_seconds target_observed_records_per_second stream_seconds stream_rows_per_second stream_source_rows_per_second consumer_wall_source_rows_per_second pre_stream_wait sink_wait_seconds sink_rows_per_second harness_overhead harness_overhead_percent message_multiplier postgres_load_rows_per_second postgres_live_write_rows_per_second total_bytes wall_mb_per_second stream_mb_per_second
  source_rows="$(env_value "${summary}" benchmark.source_rows)"
  expected="$(env_value "${summary}" benchmark.expected_kafka_messages)"
  observed="$(env_value "${summary}" benchmark.observed_kafka_messages)"
  expected_sink="$(env_value "${summary}" benchmark.expected_postgres_sink_rows)"
  observed_sink="$(env_value "${summary}" benchmark.observed_postgres_sink_rows)"
  seconds="$(env_value "${summary}" benchmark.end_to_end_seconds)"
  rows_per_second="$(env_value "${summary}" benchmark.end_to_end_rows_per_second)"
  target_observation_seconds="$(env_value "${summary}" benchmark.target_observation_seconds)"
  target_observed_records_per_second="$(env_value "${summary}" benchmark.target_observed_records_per_second)"
  stream_seconds="$(env_value "${summary}" benchmark.kafka_stream_seconds)"
  stream_rows_per_second="$(env_value "${summary}" benchmark.kafka_stream_rows_per_second)"
  stream_source_rows_per_second="$(env_value "${summary}" benchmark.kafka_stream_source_rows_per_second)"
  consumer_wall_source_rows_per_second="$(env_value "${summary}" benchmark.consumer_wall_source_rows_per_second)"
  pre_stream_wait="$(env_value "${summary}" benchmark.kafka_pre_stream_wait_seconds)"
  sink_wait_seconds="$(env_value "${summary}" benchmark.postgres_sink_wait_seconds)"
  sink_rows_per_second="$(env_value "${summary}" benchmark.postgres_sink_rows_per_second)"
  harness_overhead="$(env_value "${summary}" benchmark.harness_overhead_seconds)"
  harness_overhead_percent="$(env_value "${summary}" benchmark.harness_overhead_percent)"
  message_multiplier="$(env_value "${summary}" benchmark.message_multiplier)"
  postgres_load_rows_per_second="$(env_value "${summary}" benchmark.postgres_load_rows_per_second)"
  postgres_live_write_rows_per_second="$(env_value "${summary}" benchmark.postgres_live_write_rows_per_second)"
  total_bytes="$(env_value "${summary}" benchmark.kafka_total_bytes)"
  wall_mb_per_second="$(env_value "${summary}" benchmark.kafka_wall_mb_per_second)"
  stream_mb_per_second="$(env_value "${summary}" benchmark.kafka_stream_mb_per_second)"

  source_rows="${source_rows:-}"
  expected="${expected:-}"
  observed="${observed:-}"
  expected_sink="${expected_sink:-}"
  observed_sink="${observed_sink:-}"
  seconds="${seconds:-}"
  rows_per_second="${rows_per_second:-}"
  target_observation_seconds="${target_observation_seconds:-}"
  target_observed_records_per_second="${target_observed_records_per_second:-}"
  stream_seconds="${stream_seconds:-}"
  stream_rows_per_second="${stream_rows_per_second:-}"
  stream_source_rows_per_second="${stream_source_rows_per_second:-}"
  consumer_wall_source_rows_per_second="${consumer_wall_source_rows_per_second:-}"
  pre_stream_wait="${pre_stream_wait:-}"
  sink_wait_seconds="${sink_wait_seconds:-}"
  sink_rows_per_second="${sink_rows_per_second:-}"
  harness_overhead="${harness_overhead:-}"
  harness_overhead_percent="${harness_overhead_percent:-}"
  message_multiplier="${message_multiplier:-}"
  postgres_load_rows_per_second="${postgres_load_rows_per_second:-}"
  postgres_live_write_rows_per_second="${postgres_live_write_rows_per_second:-}"
  total_bytes="${total_bytes:-}"
  wall_mb_per_second="${wall_mb_per_second:-}"
  stream_mb_per_second="${stream_mb_per_second:-}"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "${status}" \
    "${target}" \
    "${durable_buffer}" \
    "${mode}" \
    "${format}" \
    "${rows}" \
    "${source_rows}" \
    "${expected}" \
    "${observed}" \
    "${expected_sink}" \
    "${observed_sink}" \
    "${seconds}" \
    "${rows_per_second}" \
    "${target_observation_seconds}" \
    "${target_observed_records_per_second}" \
    "${stream_seconds}" \
    "${stream_rows_per_second}" \
    "${stream_source_rows_per_second}" \
    "${consumer_wall_source_rows_per_second}" \
    "${pre_stream_wait}" \
    "${sink_wait_seconds}" \
    "${sink_rows_per_second}" \
    "${harness_overhead}" \
    "${harness_overhead_percent}" \
    "${message_multiplier}" \
    "${postgres_load_rows_per_second}" \
    "${postgres_live_write_rows_per_second}" \
    "${total_bytes}" \
    "${wall_mb_per_second}" \
    "${stream_mb_per_second}" \
    "${run_dir}" >>"${SUMMARY_CSV}"

  printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | `%s` |\n' \
    "${status}" \
    "${target}" \
    "${durable_buffer}" \
    "${mode}" \
    "${format}" \
    "${rows}" \
    "${source_rows:-n/a}" \
    "${expected:-n/a}" \
    "${observed:-n/a}" \
    "${expected_sink:-n/a}" \
    "${observed_sink:-n/a}" \
    "${seconds:-n/a}" \
    "${rows_per_second:-n/a}" \
    "${target_observation_seconds:-n/a}" \
    "${target_observed_records_per_second:-n/a}" \
    "${stream_seconds:-n/a}" \
    "${stream_rows_per_second:-n/a}" \
    "${stream_source_rows_per_second:-n/a}" \
    "${consumer_wall_source_rows_per_second:-n/a}" \
    "${pre_stream_wait:-n/a}" \
    "${sink_wait_seconds:-n/a}" \
    "${sink_rows_per_second:-n/a}" \
    "${harness_overhead:-n/a}" \
    "${harness_overhead_percent:-n/a}" \
    "${message_multiplier:-n/a}" \
    "${postgres_load_rows_per_second:-n/a}" \
    "${postgres_live_write_rows_per_second:-n/a}" \
    "${total_bytes:-n/a}" \
    "${wall_mb_per_second:-n/a}" \
    "${stream_mb_per_second:-n/a}" \
    "${run_dir}" >>"${SUMMARY_MD}"

  local run_summary_json="${run_dir}/summary.json"
  if [[ -f "${run_summary_json}" ]] && jq empty "${run_summary_json}" >/dev/null 2>&1; then
    jq -c \
      --arg status "${status}" \
      --arg target "${target}" \
      --arg durable_buffer "${durable_buffer}" \
      --arg mode "${mode}" \
      --arg format "${format}" \
      --arg rows "${rows}" \
      --arg artifact_dir "${run_dir}" \
      '
      . + {
        matrix: {
          status: $status,
          target: $target,
          durable_buffer: ($durable_buffer == "true"),
          mode: $mode,
          pipeline_format: $format,
          requested_rows: ($rows | tonumber),
          artifact_dir: $artifact_dir
        }
      }
      ' "${run_summary_json}" >>"${SUMMARY_JSONL}"
  else
    jq -nc \
      --arg status "${status}" \
      --arg target "${target}" \
      --arg durable_buffer "${durable_buffer}" \
      --arg mode "${mode}" \
      --arg format "${format}" \
      --arg rows "${rows}" \
      --arg artifact_dir "${run_dir}" \
      '{
        schema_version: 1,
        matrix: {
          status: $status,
          target: $target,
          durable_buffer: ($durable_buffer == "true"),
          mode: $mode,
          pipeline_format: $format,
          requested_rows: ($rows | tonumber),
          artifact_dir: $artifact_dir
        },
        error: {
          summary_json_missing: true
        }
      }' >>"${SUMMARY_JSONL}"
  fi
}

write_headers
write_reproduce_command

for target in ${TARGETS}; do
  target="${target,,}"
  target="${target//-/_}"
  target_formats="$(formats_for_target "${target}")"
  target_modes="$(modes_for_target "${target}")"
  for durable_buffer in ${DURABLE_REPLICATION_BUFFERS}; do
    for mode in ${target_modes}; do
      for format in ${target_formats}; do
        for rows in ${ROWS_LIST}; do
          run_dir="${ARTIFACT_ROOT}/${target}/durable-${durable_buffer}/${mode}/${format}/${rows}"
      mkdir -p "${run_dir}"
      log "running target=${target} durable_buffer=${durable_buffer} mode=${mode} format=${format} rows=${rows}"
      if (
        cd "${REPO_ROOT}"
        ARTIFACT_DIR="${run_dir}" \
        DATASET="${DATASET}" \
        TPCH_SCALE_FACTOR="${TPCH_SCALE_FACTOR}" \
        TARGET="${target}" \
        DURABLE_REPLICATION_BUFFER="${durable_buffer}" \
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
        FLOE_PG_PORT="${FLOE_PG_PORT}" \
        FLOE_ADMIN_PORT="${FLOE_ADMIN_PORT}" \
        scripts/postgres_cdc_perf_local.sh
      ) >"${run_dir}/matrix-run.log" 2>&1; then
        append_result "ok" "${run_dir}" "${target}" "${durable_buffer}" "${mode}" "${format}" "${rows}"
      else
        append_result "failed" "${run_dir}" "${target}" "${durable_buffer}" "${mode}" "${format}" "${rows}"
        log "failed; see ${run_dir}/matrix-run.log"
        if [[ "${STOP_ON_FAIL}" == "1" ]]; then
          exit 1
        fi
      fi
    done
  done
done
done
done

write_json_summary

log "summary written to ${SUMMARY_MD}"
log "json summary written to ${SUMMARY_JSON}"
cat "${SUMMARY_MD}"
