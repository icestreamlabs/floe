# Sprint 16 - Phase 1 transient unary chain hypothesis check (2026-03-11)

## Hypothesis

Bypassing persisted intermediate `select/project/passthrough` layers for the 1M no-sink filter/projection flow should materially increase throughput.

## Phase 1 implementation

- Added an optional transient delta transform path in MV materialization:
  - `crates/floe-executor/src/dbsp_graph_builder/materialize.rs`
- Added planner-side detection of unary chains and wiring to materialize directly from the durable boundary:
  - `crates/floe-executor/src/dbsp_graph_builder/builder.rs`
- Enabled for:
  - root materialization (no explicit sink node)
  - sink materialization

## Validation command

```bash
RUST_LOG=info FLOE_E2E_NO_SINK_VERIFY=count_only \
  cargo test -p floe-node --release \
  redpanda_kafka_million_filter_projection_nosink_row_e2e \
  -- --ignored --nocapture
```

## Results (Phase 1 enabled)

| Run | Throughput input rows/s | Ingest complete s | Log marker |
|---|---:|---:|---|
| run3 | 68,913 | 14.511 | `using transient unary chain for root materialization` |
| run4 | 76,054 | 13.148 | `using transient unary chain for root materialization` |
| run5 | 72,800 | 13.736 | `using transient unary chain for root materialization` |
| run6 | 70,360 | 14.213 | `using transient unary chain for root materialization` |

Aggregate over run3-6:

- mean: 72,032 input rows/s
- median: 71,580 input rows/s
- min/max: 68,913 / 76,054 input rows/s

## Conclusion

Phase 1 is functionally active and correct, but it does **not** produce a dramatic step-function increase in throughput for this workload. The result is neutral-to-modest improvement depending on run variance, which supports the view that additional structural costs remain outside this single bypass.
