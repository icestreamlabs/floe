#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PROFILE="${PROFILE:-smoke}"
RUN_ID="$(date +%Y%m%dT%H%M%S)"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-${REPO_ROOT}/target/cdc_bench_guardrails/${PROFILE}/${RUN_ID}}"

default_env() {
  local name="$1"
  local value="$2"
  if [[ -z "${!name+x}" || -z "${!name}" ]]; then
    printf -v "${name}" '%s' "${value}"
  fi
  export "${name}"
}

case "${PROFILE}" in
  smoke)
    default_env ROWS_LIST "1000"
    default_env TARGETS "kafka postgres"
    default_env DURABLE_REPLICATION_BUFFERS "true false"
    default_env BENCH_MODES "snapshot"
    default_env BUILD_RELEASE "0"
    default_env TIMEOUT_SECS "600"
    ;;
  baseline)
    default_env ROWS_LIST "100000 1000000"
    default_env TARGETS "kafka postgres"
    default_env DURABLE_REPLICATION_BUFFERS "true false"
    default_env BENCH_MODES "snapshot live_insert snapshot_live_update"
    default_env BUILD_RELEASE "1"
    default_env TIMEOUT_SECS "1800"
    ;;
  soak)
    default_env ROWS_LIST "1000000"
    default_env TARGETS "kafka postgres"
    default_env DURABLE_REPLICATION_BUFFERS "true"
    default_env BENCH_MODES "live_insert snapshot_live_update"
    default_env LIVE_WRITE_CHUNK_ROWS "10000"
    default_env LIVE_WRITE_SLEEP_MS "250"
    default_env BUILD_RELEASE "1"
    default_env STOP_ON_FAIL "1"
    default_env TIMEOUT_SECS "3600"
    ;;
  postgres-sink)
    default_env ROWS_LIST "100000 1000000"
    default_env TARGETS "postgres"
    default_env DURABLE_REPLICATION_BUFFERS "true false"
    default_env BENCH_MODES "snapshot snapshot_live_update"
    default_env BUILD_RELEASE "1"
    default_env TIMEOUT_SECS "1800"
    ;;
  *)
    echo "unsupported PROFILE=${PROFILE} (expected smoke|baseline|soak|postgres-sink)" >&2
    exit 1
    ;;
esac

export ARTIFACT_ROOT

mkdir -p "${ARTIFACT_ROOT}"
cat >"${ARTIFACT_ROOT}/guardrail.env" <<ENV
profile=${PROFILE}
artifact_root=${ARTIFACT_ROOT}
git_commit=$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || true)
git_branch=$(git -C "${REPO_ROOT}" rev-parse --abbrev-ref HEAD 2>/dev/null || true)
rows_list=${ROWS_LIST}
targets=${TARGETS}
durable_replication_buffers=${DURABLE_REPLICATION_BUFFERS}
bench_modes=${BENCH_MODES:-auto}
build_release=${BUILD_RELEASE}
timeout_secs=${TIMEOUT_SECS}
live_write_chunk_rows=${LIVE_WRITE_CHUNK_ROWS:-0}
live_write_sleep_ms=${LIVE_WRITE_SLEEP_MS:-0}
ENV

echo "profile=${PROFILE}"
echo "artifact_root=${ARTIFACT_ROOT}"
echo "rows_list=${ROWS_LIST}"
echo "targets=${TARGETS}"
echo "durable_replication_buffers=${DURABLE_REPLICATION_BUFFERS}"
echo "bench_modes=${BENCH_MODES:-auto}"

exec "${REPO_ROOT}/scripts/postgres_cdc_perf_matrix.sh"
