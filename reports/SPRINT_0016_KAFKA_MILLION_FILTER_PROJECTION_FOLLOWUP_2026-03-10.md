# SPRINT_0016 Kafka Million Filter/Projection Follow-up (2026-03-10)

## Scope

- Query/test: `redpanda_kafka_million_filter_projection_nosink_row_e2e`
- Verify mode: `FLOE_E2E_NO_SINK_VERIFY=count_only`
- Baseline target: improve no-sink filter/projection throughput without regressions.

Command used:

```bash
FLOE_E2E_NO_SINK_VERIFY=count_only \
cargo test -p floe-node --release \
  redpanda_kafka_million_filter_projection_nosink_row_e2e \
  -- --ignored --nocapture
```

## Merged Improvement

- Branch: `perf/step15-output-raw-intern`
- Commit merged to `master`: `9831c8e`
- Commit title: `perf: optimize dictionary intern for unique filter_map batches`
- Files:
  - `crates/dbsp-storage/src/storage/dictionary/core.rs`
  - `crates/dbsp-runtime/src/filter_map.rs`

### What changed

- Added a unique-batch dictionary intern API (`intern_many_values_unique`) to avoid duplicate-detection bookkeeping in the output write path when the batch is already key-consolidated.
- Reused precomputed hash in reserve calls to remove extra hash work.
- Switched filter_map output delta staging to the new unique-batch intern path.

## A/B Results (same session)

### With `9831c8e` patch

- Run 1: `75,133` rows/s (`ingest_complete_s=13.310`)
- Run 2: `77,522` rows/s (`ingest_complete_s=12.900`)
- Run 3: `77,906` rows/s (`ingest_complete_s=12.836`)
- Median: `77,522` rows/s

### Without patch (stashed/reverted for A/B)

- Run 1: `76,891` rows/s (`ingest_complete_s=13.005`)
- Run 2: `74,735` rows/s (`ingest_complete_s=13.381`)
- Run 3: `73,901` rows/s (`ingest_complete_s=13.532`)
- Median: `74,735` rows/s

### Net

- Throughput gain: `+2,787` rows/s (`+3.73%`)
- Ingest completion median improvement: about `0.48s` faster

## Follow-up Attempts (not merged)

### 1) Dictionary lock primitive swap (`parking_lot::Mutex`)

- Branch: `perf/step16-dict-parking-lot`
- Result runs: `74,631`, `76,246`, `76,733` rows/s
- Median: `76,246` rows/s
- Decision: **rejected** (below merged step15 median in this validation window)

### 2) Alternate fresh-dictionary hash-seen fast path

- Branch: `perf/step17-unique-fresh-hash-path`
- Result runs: `74,747`, `74,272`, `76,191` rows/s
- Median: `74,747` rows/s
- Decision: **rejected** (regression)

### 3) Kafka connector event clone elimination

- Temporary change tested via stash A/B.
- No stable win in repeated runs.
- Decision: **not merged**

## Final State

- `master` contains only the winning optimization from `9831c8e`.
- Rejected follow-up changes were reverted and not merged.
