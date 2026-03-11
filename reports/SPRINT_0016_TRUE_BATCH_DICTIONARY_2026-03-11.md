# Sprint 16 - True Batch Dictionary Experiment (2026-03-11)

## Goal
Prototype a "true batch dictionary" path for 1M Kafka filter+projection no-sink throughput.

## Branch
`perf/true-batch-dictionary-arrow`

## What Changed
1. `floe-executor` vectorized source-map path now consolidates projected row keys with Arrow `BinaryDictionaryBuilder` (batch dictionary) and returns `Vec<(key, weight)>` deltas instead of a per-row `HashMap` update loop.
2. `dbsp-runtime` filter-map batch path now accepts consolidated vector deltas and applies them directly.
3. `dbsp-storage` dictionary gained owned unique-batch intern (`intern_many_values_unique_owned`) and an owned reserve path to avoid extra encoded-key cloning during unique batch inserts.
4. Added dictionary test coverage for the new owned unique-batch intern API.

## Validation
- `cargo check -p dbsp-storage -p dbsp-runtime -p floe-executor -p floe-node`
- `cargo test -p dbsp-storage storage::dictionary::tests::interns_owned_unique_batch_without_cloning_keys`

## Benchmark Command
`FLOE_E2E_NO_SINK_VERIFY=count_only cargo test -p floe-node --release redpanda_kafka_million_filter_projection_nosink_row_e2e -- --ignored --nocapture`

## Results
### Experiment Branch (`perf/true-batch-dictionary-arrow`)
- run1: 70,547 rows/s (`ingest_complete_s=14.175`)
- run2: 73,187 rows/s (`ingest_complete_s=13.664`)
- run3: 72,796 rows/s (`ingest_complete_s=13.737`)
- median: **72,796 rows/s**

### Master Baseline (`50756c2`)
- run1: 71,521 rows/s (`ingest_complete_s=13.982`)
- run2: 73,416 rows/s (`ingest_complete_s=13.621`)
- run3: 72,911 rows/s (`ingest_complete_s=13.715`)
- median: **72,911 rows/s**

## Conclusion
This prototype is effectively neutral to slightly regressive on this benchmark (median delta: **-115 rows/s**, about **-0.16%**). Do **not** merge this branch into `master`.

## Follow-up
Keep this branch as a reference experiment only.
