# Sprint 16 - Phase 2/3 persistence-boundary optimization (2026-03-12)

## Scope

This change moves transient optimization from an ad-hoc unary matcher to a planner-driven persistence policy and segment builder.

Implemented:

1. PersistencePolicy layer in planning/runtime build.
2. Segment compilation between durable boundaries for eligible stateless unary chains.
3. Transient batch channel represented as transform-over-delta-batch in materialization path.
4. Recovery/correctness unchanged: durable boundaries remain source/stateful/MV handles.
5. Planner-driven behavior (not query-specific), with simple cost gating.

## Code changes

- Added policy module:
  - `crates/floe-executor/src/dbsp_graph_builder/persistence_policy.rs`
- Wired builder to policy-driven segments:
  - `crates/floe-executor/src/dbsp_graph_builder/builder.rs`
- Registered policy module:
  - `crates/floe-executor/src/dbsp_graph_builder/mod.rs`
- Reduced transient-path overhead in MV apply:
  - `crates/floe-executor/src/dbsp_graph_builder/materialize.rs`

## Behavior details

- Node persistence classification:
  - transient: `Select`, `Project`, `Passthrough`
  - durable: `Source`, `Join`, `Aggregate`, `WindowAggregate`, `TopN`, `Union`, `Distinct`, `Sink`
- Builder now computes transient segments from a terminal input back to a durable boundary.
- Segment selection is cost-gated:
  - `FLOE_TRANSIENT_SEGMENT_MAX_NODES` (default `32`)
  - `FLOE_TRANSIENT_SEGMENT_MIN_SCORE` (default `1`)
- Segment execution remains vectorized via `VectorizedFilterProjectEvaluator` chain.
- MV apply path no longer does an extra pre-merge `HashMap` consolidation step before `add_deltas`; overlay consolidation handles it.

## Validation

Build/compile checks:

```bash
cargo check -p floe-node
cargo test -p floe-executor --no-run
```

Both passed.

1M no-sink count-only release runs:

- run1: `74328` rows/s
  - `reports/SPRINT_0016_PHASE23_PERSISTENCE_POLICY_SEGMENTS_COUNTONLY_RELEASE_run1_2026-03-12.log`
- run2: `69154` rows/s
  - `reports/SPRINT_0016_PHASE23_PERSISTENCE_POLICY_SEGMENTS_COUNTONLY_RELEASE_run2_2026-03-12.log`
- run3: `75062` rows/s
  - `reports/SPRINT_0016_PHASE23_PERSISTENCE_POLICY_SEGMENTS_COUNTONLY_RELEASE_run3_2026-03-12.log`
- run4: `74740` rows/s
  - `reports/SPRINT_0016_PHASE23_PERSISTENCE_POLICY_SEGMENTS_COUNTONLY_RELEASE_run4_2026-03-12.log`

Aggregate (run1-4):

- mean: `73321` rows/s
- median: `74534` rows/s

Recent pre-change reference:

- `70938` rows/s
  - `reports/SPRINT_0016_PHASE1_TRANSIENT_UNARY_CHAIN_VECTORIZED_ONLY_COUNTONLY_RELEASE_post_toggle_removal_2026-03-11.log`

## Conclusion

Phase 2/3 implementation is complete, policy-driven, and non-regressive in the sampled runs. Mean throughput improved relative to the recent pre-change reference while preserving durable-boundary recovery semantics.
