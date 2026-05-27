# Postgres CDC Performance Handoff

Date: 2026-05-25

Branch: `perf/postgres-cdc-reporting`

Primary issue: https://github.com/icestreamlabs/floe/issues/2

## Current Git State

The branch has two local commits on top of `main`:

- `03d508c perf: add cdc benchmark JSON reporting`
- `bcf68e9 perf: add cdc sink target benchmark modes`

Known untracked local files:

- `DBSP_FELDERA_PARITY_PLAN.md`
- `POSTGRES_CDC_PERF_HANDOFF.md`

Do not assume these untracked files should be committed without checking with the user.

## Issues Created

- #1 Clarify and refactor SourceEvent as append-ingest row input
- #2 Build repeatable Postgres CDC performance harness and parity reporting
- #3 Harden Postgres CDC conformance coverage against Debezium/RisingWave semantics
- #4 Tighten durable CDC buffer caps, cleanup, and backpressure semantics
- #5 Harden Postgres sink delivery, retries, and dead-letter recovery

## What Was Done

### JSON Reporting

`scripts/postgres_cdc_perf_local.sh` now writes:

- `summary.env`
- `summary.json`
- `summary.md`

`scripts/postgres_cdc_perf_matrix.sh` now writes:

- `summary.csv`
- `summary.md`
- `summary.jsonl`
- `summary.json`

The JSON includes run metadata, git commit/branch, dataset, target, format, durability mode, source row counts, sink/Kafka observations, timing boundaries, rates, bytes, and artifact paths.

### Target-Aware CDC Benchmark Harness

`scripts/postgres_cdc_perf_local.sh` now supports:

- `TARGET=kafka`, the original Kafka sink path.
- `TARGET=postgres`, a Postgres sink path.

For `TARGET=postgres`, the harness:

- skips Redpanda startup,
- creates empty sink tables with `CREATE TABLE sink (LIKE source INCLUDING ALL)`,
- emits `CREATE REPLICATION PIPELINE ... INTO POSTGRES`,
- waits until expected rows are visible in the sink,
- reports Postgres sink wait time and sink rows/s.

Postgres target currently requires:

- `PIPELINE_FORMAT=floe-json`

### Matrix Enhancements

`scripts/postgres_cdc_perf_matrix.sh` now supports:

- `TARGETS='kafka postgres'`
- `DURABLE_REPLICATION_BUFFERS='true false'`
- target-specific default formats:
  - Kafka: `floe-json debezium-json arrow-ipc`
  - Postgres: `floe-json`
- target-specific default modes:
  - Kafka synthetic dataset: `snapshot live_insert snapshot_live_update`
  - Postgres synthetic dataset: `snapshot live_insert`

Explicit `BENCH_MODES` still overrides auto mode selection.

## Validation Already Run

Static/syntax checks:

```bash
bash -n scripts/postgres_cdc_perf_local.sh
bash -n scripts/postgres_cdc_perf_matrix.sh
git diff --check
cargo check -p floe-benchmarks --bin postgres_cdc_kafka_counter
```

Smoke runs:

```bash
ROWS=10 DATASET=synthetic-orders BENCH_MODE=snapshot PIPELINE_FORMAT=floe-json \
  DURABLE_REPLICATION_BUFFER=false BUILD_RELEASE=0 TIMEOUT_SECS=180 \
  ARTIFACT_DIR=target/cdc_bench_smoke/json-report-3 \
  scripts/postgres_cdc_perf_local.sh
```

```bash
ROWS_LIST=10 DATASET=synthetic-orders BENCH_MODES=snapshot \
  TARGETS='kafka postgres' PIPELINE_FORMATS=floe-json \
  DURABLE_REPLICATION_BUFFER=false BUILD_RELEASE=0 TIMEOUT_SECS=180 \
  ARTIFACT_ROOT=target/cdc_bench_matrix_smoke/targets \
  scripts/postgres_cdc_perf_matrix.sh
```

```bash
ROWS=10 DATASET=synthetic-orders BENCH_MODE=snapshot TARGET=postgres \
  PIPELINE_FORMAT=floe-json DURABLE_REPLICATION_BUFFER=false BUILD_RELEASE=0 \
  TIMEOUT_SECS=180 ARTIFACT_DIR=target/cdc_bench_smoke/postgres-target \
  scripts/postgres_cdc_perf_local.sh
```

```bash
ROWS=5 DATASET=synthetic-orders BENCH_MODE=live_insert TARGET=postgres \
  PIPELINE_FORMAT=floe-json DURABLE_REPLICATION_BUFFER=false BUILD_RELEASE=0 \
  TIMEOUT_SECS=60 LIVE_WRITE_CHUNK_ROWS=5 \
  ARTIFACT_DIR=target/cdc_bench_smoke/postgres-target-live-insert \
  scripts/postgres_cdc_perf_local.sh
```

```bash
ROWS_LIST=5 DATASET=synthetic-orders TARGETS=postgres PIPELINE_FORMATS=floe-json \
  DURABLE_REPLICATION_BUFFERS=false BUILD_RELEASE=0 TIMEOUT_SECS=120 \
  ARTIFACT_ROOT=target/cdc_bench_matrix_smoke/postgres-durable-list \
  scripts/postgres_cdc_perf_matrix.sh
```

All of the above passed.

## Important Finding

`TARGET=postgres` with `BENCH_MODE=snapshot_live_update` did not complete in the tiny smoke run.

Observed behavior:

- source table had 5 rows with `status = 'updated'`,
- sink table had 5 rows but 0 updated rows,
- replication slot stayed active,
- confirmed flush LSN did not move past the snapshot LSN.

The stopped smoke command was:

```bash
ROWS=5 DATASET=synthetic-orders BENCH_MODE=snapshot_live_update TARGET=postgres \
  PIPELINE_FORMAT=floe-json DURABLE_REPLICATION_BUFFER=false BUILD_RELEASE=0 \
  TIMEOUT_SECS=180 LIVE_WRITE_CHUNK_ROWS=5 \
  ARTIFACT_DIR=target/cdc_bench_smoke/postgres-target-update \
  scripts/postgres_cdc_perf_local.sh
```

Because `live_insert` works for `TARGET=postgres`, this may be an update-path issue, a replication handoff issue, or a benchmark setup issue. I avoided hiding it by excluding `snapshot_live_update` from Postgres auto modes, while still allowing explicit update runs via `BENCH_MODES=snapshot_live_update`.

This likely belongs under issue #5 if it turns out to be a Postgres sink/update delivery bug.

## Suggested Next Steps

1. Push the branch if the user wants the current work shared:

```bash
git push -u origin perf/postgres-cdc-reporting
```

2. Decide whether to merge this harness/reporting work now or continue on the branch.

3. Run a real small baseline matrix:

```bash
ROWS_LIST='1000 10000' \
TARGETS='kafka postgres' \
DURABLE_REPLICATION_BUFFERS='false true' \
DATASET=synthetic-orders \
BUILD_RELEASE=1 \
TIMEOUT_SECS=300 \
scripts/postgres_cdc_perf_matrix.sh
```

4. Add better scenario presets or a wrapper for the issue #2 primary cases:

- Kafka top-level JSON
- Kafka Debezium JSON
- Kafka durable buffer on/off
- Postgres sink snapshot/live insert
- TPC-H lineitem or top2 shape

5. Consider adding a no-op/counting sink path if engine-only CDC cost needs to be isolated from Kafka/Postgres sink cost.

6. Investigate the Postgres target `snapshot_live_update` finding before advertising update throughput for Postgres sink benchmarks.

## Notes For The Next Agent

- Keep benchmark changes measurement-oriented; avoid benchmark-only special casing.
- Do not use Arrow IPC Kafka payloads for public apples-to-apples claims unless clearly marked internal/experimental.
- Object-storage-friendly buffer defaults matter; do not tune flush intervals down to tiny local-only values.
- `docs/` is ignored/untracked in this repo. Avoid editing docs unless the user asks.
- `DBSP_FELDERA_PARITY_PLAN.md` is intentionally untracked.
