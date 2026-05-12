#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

failures=0

check_absent() {
  local label="$1"
  shift
  local pattern="$1"
  shift

  local output
  if output="$(rg -n "${pattern}" "$@" 2>/dev/null)"; then
    printf 'CDC vectorization audit failed: %s\n%s\n' "${label}" "${output}" >&2
    failures=$((failures + 1))
  fi
}

check_absent \
  "row-wise CDC delta encoder fallback is not allowed" \
  "encode_cdc_table_deltas_rowwise|rowwise_encode" \
  crates/floe-cdc \
  crates/floe-cdc-core \
  crates/floe-cdc-pg \
  crates/floe-node \
  crates/floe-node-core \
  crates/floe-benchmarks

check_absent \
  "CDC runtime paths must encode source rows through Arrow batches, not encode_row_values" \
  "\\.encode_row_values\\(" \
  crates/floe-cdc \
  crates/floe-cdc-core \
  crates/floe-cdc-pg \
  crates/floe-node \
  crates/floe-node-core \
  crates/floe-benchmarks

if (( failures > 0 )); then
  exit 1
fi

printf 'CDC vectorization audit passed.\n'
