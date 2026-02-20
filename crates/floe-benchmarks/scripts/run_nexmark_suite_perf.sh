#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$repo_root"

report_date="$(date +%F)"
out_file="reports/SPRINT_0005_PERF_${report_date}.csv"

cargo run -p floe-benchmarks --bin nexmark_suite_perf > "$out_file"

echo "wrote $out_file"
