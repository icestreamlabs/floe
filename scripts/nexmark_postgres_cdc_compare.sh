#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"
if [[ "${FLOE_HARNESS_RELEASE:-0}" == "1" ]]; then
  exec cargo run -p floe-benchmarks --release --bin nexmark_postgres_cdc_compare -- "$@"
else
  exec cargo run -p floe-benchmarks --bin nexmark_postgres_cdc_compare -- "$@"
fi
