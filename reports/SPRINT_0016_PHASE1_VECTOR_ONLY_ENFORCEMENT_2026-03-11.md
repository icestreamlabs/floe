# Sprint 16 - Vectorized-only enforcement for unary/filter-projection paths (2026-03-11)

## Summary

This change removes scalar fallback from unary source-map execution and from transient unary-chain materialization.

Enforced behavior:

- `filter`, `map`, and `filter_map` compile paths are vectorized-only.
- transient unary-chain optimization is vectorized-only.
- `FLOE_VECTORIZED_FILTER_MAP` toggle has been removed from executor code.

## Key code changes

- Shared vectorized evaluator moved into dedicated module:
  - `crates/floe-executor/src/dbsp_graph_builder/vectorized_filter_project.rs`
- Scalar fallback removed from:
  - `crates/floe-executor/src/dbsp_graph_builder/compile/source_map_phase.rs`
- Transient unary chain now applies vectorized evaluator batches end-to-end:
  - `crates/floe-executor/src/dbsp_graph_builder/builder.rs`

## Validation

### 1) Build/test compile checks

Commands:

```bash
cargo check -p floe-node
cargo test -p floe-executor --no-run
```

Result:

- both succeeded

### 2) Normal release run

Command:

```bash
RUST_LOG=info FLOE_E2E_NO_SINK_VERIFY=count_only \
  cargo test -p floe-node --release \
  redpanda_kafka_million_filter_projection_nosink_row_e2e \
  -- --ignored --nocapture
```

Result samples:

- `throughput.no_sink.input_rows_per_sec=77183`
- `throughput.no_sink.input_rows_per_sec=69128`
- transient path active in node log:
  - `using transient unary chain for root materialization`

Log artifacts:

- `reports/SPRINT_0016_PHASE1_TRANSIENT_UNARY_CHAIN_VECTORIZED_ONLY_COUNTONLY_RELEASE_run2_2026-03-11.log`
- `reports/SPRINT_0016_PHASE1_TRANSIENT_UNARY_CHAIN_VECTORIZED_ONLY_COUNTONLY_RELEASE_run3_2026-03-11.log`

## Conclusion

The filter/projection unary path is vectorized-only by construction, including the transient materialization optimization, and the old env-var escape hatch has been removed.
