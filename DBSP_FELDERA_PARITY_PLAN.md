# DBSP Feldera-Parity Incrementality Plan

This plan is about raising Floe's implementation to the level where it can make
claims as strong as Feldera's reference DBSP implementation. It is not a plan to
document the current state more carefully. The target is an implementation,
test, and observability program that makes operational incrementality an
enforced invariant of the DBSP stack.

## Target Claim

Floe should be able to claim:

> For supported incremental SQL plans, steady-state foreground work per logical
> tick is proportional to the input delta, produced output delta, and indexed
> state slices addressed by changed keys. It is not proportional to total
> historical input size or total materialized state size, except where the SQL
> shape genuinely requires broader work. Any broader work is part of the plan's
> declared work profile, is measured separately, and is visible to the user and
> tests.

This is intentionally stronger than "Floe implements the DBSP semantic equation"
or "Floe has benchmarks that look incremental." The semantic equation proves the
meaning of the output. It does not prove the runtime avoids scanning history.

## Feldera Bar

Feldera's DBSP implementation supports this level of claim through several
implementation patterns:

- Incremental operators consume delta batches plus traces/arrangements, not full
  collections.
- Join is implemented as delta streams joined against traces of the opposite
  side. See
  `/home/jlerche/programming_projects/github.com/feldera/feldera/crates/dbsp/src/operator/dynamic/join.rs:657`.
- Aggregate uses the delta only to find affected keys and uses traces to compute
  updated values for those keys. See
  `/home/jlerche/programming_projects/github.com/feldera/feldera/crates/dbsp/src/operator/dynamic/aggregate.rs:764`.
- Distinct explicitly computes from the support of the delta. See
  `/home/jlerche/programming_projects/github.com/feldera/feldera/crates/dbsp/src/operator/dynamic/distinct.rs:402`.
- Non-incremental work is named and isolated through a `non_incremental`
  subcircuit API that warns it is inefficient and intended for testing/debugging.
  See
  `/home/jlerche/programming_projects/github.com/feldera/feldera/crates/dbsp/src/operator/non_incremental.rs:229`.
- Runtime metadata exposes logical work counters such as input records, state
  records, join-side records, computed output records, spine batch counts, cache
  stats, and invocation/runtime counters. See
  `/home/jlerche/programming_projects/github.com/feldera/feldera/crates/dbsp/src/circuit/metadata.rs:66`.
- Trace/spine storage documents the key caveat that lookup and iteration cost
  depends on batch count unless compaction keeps read amplification bounded. See
  `/home/jlerche/programming_projects/github.com/feldera/feldera/crates/dbsp/src/trace/spine_async.rs:1`.

Floe should treat these as the minimum bar for implementation discipline and
evidence, not as a requirement to copy Feldera's public API shape. Floe is not a
general-purpose DBSP library; it is a database whose execution engine exists to
maintain streaming materialized views. That means Floe does not need an exposed
`non_incremental` subcircuit API. It does need an equivalent internal ability to
describe, measure, test, and explain the work profile of each plan.

## Current Floe Position

Floe already has useful pieces, but they are not sufficient for Feldera-level
claims.

- `crates/dbsp-semantic` defines the paper-facing semantic layer and tests
  `QDelta = D o up-arrow(Q) o I`. This is necessary but not an operational
  work proof.
- `docs/dbsp_paper_semantics_audit.md` tracks denotational coverage, not
  history-independent runtime work.
- `docs/incremental_state_semantics.md` documents the join delta formula:
  `Delta(A join B) = DeltaA join B + A join DeltaB + DeltaA join DeltaB`.
- `crates/dbsp-runtime/src/operators/join/op.rs:1034` implements the same
  high-level delta join shape.
- `crates/dbsp-runtime/src/metrics.rs` has latency and persistence metrics, but
  not enough logical work counters to prove that a tick did not scan unrelated
  history.
- Some paths are explicitly suspicious from an incrementality-proof perspective:
  cache materialization in
  `crates/dbsp-runtime/src/operators/incremental_aggregate.rs:343`, full entry
  listing in
  `crates/dbsp-runtime/src/collections/arrow_indexed_batch_zset.rs:767`, and
  per-key prefix scans in
  `crates/dbsp-runtime/src/collections/arrow_indexed_batch_zset.rs:862`.

The goal is to close those gaps, not to relabel them.

## Definition Of Done

Floe reaches Feldera-parity for DBSP incrementality when all of the following
are true:

1. Every production DBSP operator and materialized-view plan has an explicit
   work profile: delta-local, keyed-state, affected-slice, bounded maintenance,
   cold-start/recovery, or full-state work.
2. Every work profile has a written contract that states exactly what it may
   touch per tick and which SQL shapes can produce it.
3. The planner/circuit layer prevents accidental hidden full-state work. It may
   produce mixed plans when the SQL requires them, but the mixed work envelope
   must be visible and measured.
4. Runtime counters can show, per operator and per tick, how much delta, state,
   index, cache, compaction, and output work was performed.
5. Regression tests vary total history while holding the delta and affected
   state slice fixed, and assert logical work remains flat.
6. Regression tests vary affected key fanout/group size and assert work grows
   with that slice, not unrelated state.
7. Cold-start, recovery, cache rebuild, snapshot, and compaction work are
   separately measured and never hidden inside the steady-state foreground
   incrementality claim.
8. The public README/API claim is only strengthened after the above gates pass.

## Phase 1: Operator And Plan Work Profiles

Add first-class work-profile metadata to the DBSP plan/operator model. This is
not a Feldera-style public `non_incremental` marker. It is a Floe-specific
contract that describes the work an MV can perform while being maintained.

Required work profiles:

- `DeltaLocal`: work is bounded by the input delta and output delta.
- `KeyedStateLookup`: work is bounded by the delta, output, and keyed lookups
  into maintained state.
- `AffectedSlice`: work may scan all rows in affected groups, windows, ranges,
  or join-key fanout, but not unrelated state.
- `BoundedMaintenance`: work is not directly caused by the user delta, but is
  bounded by configured compaction, retention, or snapshot policy and measured
  separately.
- `ColdStartOrRecovery`: may scan stored state to rebuild caches or descriptors,
  but is outside the steady-state tick claim.
- `FullStateWork`: may scan full integrated state or materialize whole inputs.
  This is allowed only when it is part of the declared plan work profile, never
  as an accidental implementation detail hidden inside an otherwise incremental
  operator.

Implementation targets:

- Add work-profile metadata to `crates/dbsp-circuit` nodes and runtime
  operators.
- Mirror Feldera's discipline from `INonIncremental`/`IIncremental` in spirit,
  adapted to Floe's database-specific plan model rather than copied as a public
  API.
- Teach `crates/dbsp-planner` and `crates/floe-executor` to compute a
  materialized view's aggregate work profile from its operators.
- Add plan validation that rejects unprofiled work and rejects hidden full-state
  scans inside operators that claim `DeltaLocal`, `KeyedStateLookup`, or
  `AffectedSlice`.
- Add an explicit escape hatch for tests/reference recompute paths so full
  recomputation remains available without being confused with production
  incrementality.

Acceptance tests:

- Planner validation fails when a production MV plan contains unprofiled work.
- Planner validation fails when an operator's implementation performs broader
  work than its declared profile allows.
- A production MV plan may contain `FullStateWork` only when the plan-level work
  profile exposes it.
- Existing reference recompute tests still work through a clearly named testing
  path.
- A machine-readable operator and plan work-profile inventory can be generated
  and reviewed.

## Phase 2: Operator Work Contracts

For every production operator and plan-level work profile, write a work contract
next to the implementation or in a central contract document.

Each contract must specify:

- Input delta representation.
- State representation and indexing assumptions.
- Per-tick state touched.
- Output lower bound or fanout behavior.
- Whether the operator can force broader work for particular SQL shapes.
- Cache assumptions.
- Persistence and compaction behavior.
- Cold/recovery exceptions.
- Metrics that prove the contract at runtime.

Initial operator contracts to cover:

- Filter/project/map: batch-native delta-only transformation.
- Union/consolidate: delta-only transformation plus per-tick consolidation.
- Join: `DeltaA join B`, `A join DeltaB`, and `DeltaA join DeltaB` using keyed
  state indexes; work may grow with changed-key fanout and output size.
- Semijoin/antijoin/range/as-of joins: same standard as join, with explicit
  range/window fanout bounds.
- Distinct: work over changed values and their old weights, not all values.
- Additive aggregates: update per-group state from changed rows.
- Nonlinear aggregates such as min/max: either maintain enough auxiliary state
  to avoid full group scans, or profile group rescans as affected-slice work and
  measure group size.
- TopN/Top1/window operators: define whether they maintain indexed per-window or
  per-group state, and where output/fanout lower bounds apply.
- Materialized view application and delta emission: distinguish output-delta
  application from full snapshot materialization.

Acceptance tests:

- Each operator has a contract before it can be considered part of a production
  MV work profile.
- Each contract names the exact counters that will be asserted in Phase 4.

## Phase 3: Runtime Logical Work Counters

Add Feldera-style logical work metrics. Wall-clock benchmarks are not enough,
because storage cache warmth and machine noise can hide history scans.

Minimum per-operator, per-tick counters:

- `input_delta_rows`
- `input_delta_batches`
- `output_delta_rows`
- `output_delta_batches`
- `state_lookup_keys`
- `state_lookup_rows`
- `state_scan_rows`
- `state_full_scan_count`
- `index_segments_examined`
- `index_postings_examined`
- `cache_hits`
- `cache_misses`
- `cache_rebuild_rows`
- `compaction_input_rows`
- `compaction_output_rows`
- `snapshot_rows`
- `persisted_rows`
- `persisted_keys`

Join-specific counters:

- `left_delta_rows`
- `right_delta_rows`
- `left_changed_keys`
- `right_changed_keys`
- `left_state_rows_examined`
- `right_state_rows_examined`
- `delta_delta_rows_examined`
- `join_output_rows`

Aggregate-specific counters:

- `changed_groups`
- `group_state_rows_examined`
- `aggregate_state_rows_updated`
- `distinct_aux_rows_examined`
- `extrema_rebuild_rows`

Storage/index counters:

- Per-key read amplification.
- Number of segments or batches consulted for a lookup.
- Number of full namespace scans.
- Number of rows decoded from storage.
- Number of rows discarded by consolidation.

Implementation targets:

- Extend `crates/dbsp-runtime/src/metrics.rs` beyond latency histograms.
- Add a lightweight in-process test collector so regression tests can assert
  counters without scraping Prometheus.
- Emit structured trace spans for full scans, cache rebuilds, compaction, and
  snapshot materialization.
- Ensure maintenance counters are separate from foreground operator counters.

Acceptance tests:

- Tests can inspect counters deterministically for one logical tick.
- A full state scan increments `state_full_scan_count`.
- Per-key persisted index lookup reports segment/read amplification.
- Cache rebuilds are visible as rebuild work, not operator delta work.

## Phase 4: History-Invariance Regression Suite

Create a new test suite whose purpose is operational incrementality, not output
correctness. Correctness is already covered elsewhere; these tests should fail
when unrelated history increases steady-state foreground work.

Test pattern:

1. Build historical state sizes: for example 1k, 100k, and 1M rows.
2. Keep the next input delta fixed.
3. Keep affected key fanout/group size fixed unless the test intentionally
   varies it.
4. Run one or more steady-state ticks.
5. Assert output correctness.
6. Assert logical work counters stay within a fixed envelope across historical
   sizes.

Required scenarios:

- Filter/project over large history with fixed delta.
- Union/consolidate with fixed delta.
- Equality join with fixed changed keys and fixed opposite-side fanout.
- Equality join with increasing changed-key fanout, proving work grows with
  fanout rather than unrelated keys.
- Semijoin/antijoin with fixed changed keys.
- Distinct with fixed changed values.
- Count/sum aggregates with fixed changed groups.
- Min/max aggregates with fixed changed group size and with intentionally
  increasing group size.
- TopN/Top1 with fixed changed group/window size.
- Windowed aggregates with fixed affected windows.
- MV application with fixed output delta and large existing MV state.
- Recovery/cold-cache case, separately profiled, proving rebuild work is
  measured and not attributed to steady-state incrementality.

Acceptance criteria:

- For fixed affected slices, counters do not grow with unrelated history.
- For fanout/group/window tests, counters grow only with the affected slice.
- Any operator that cannot pass is either fixed or assigned a broader work
  profile such as `FullStateWork` or `ColdStartOrRecovery`. It may still be
  usable in a mixed MV plan, but the plan cannot be described as purely
  delta-local/keyed-state incremental.

## Phase 5: Trace, Index, And Compaction Parity

Feldera's trace/spine layer makes the cost model explicit: arrangements support
delta insertion and keyed lookup, while compaction bounds the cost of too many
batches. Floe needs the equivalent discipline for its versioned ZSets and
Arrow-indexed state.

Work items:

- Define the trace/index abstraction Floe is willing to claim publicly.
- Make the per-key lookup cost model explicit for `ArrowIndexedBatchZSet`.
- Bound or measure segment/read amplification for
  `segment_refs_for_key`.
- Ensure compaction policy has an operational SLA, not only a storage-cleanup
  purpose.
- Separate foreground tick work from background/maintenance compaction.
- Add tests where historical segment count grows while logical state and delta
  stay fixed.
- Fail or warn when read amplification exceeds the configured incrementality
  envelope.

Acceptance criteria:

- Per-key lookup does not silently degrade with unbounded segment history.
- Compaction/read-amplification counters are visible in tests and metrics.
- Incrementality tests can distinguish "many rows for affected key" from "many
  historical segments for unrelated keys."

## Phase 6: Cache And Recovery Semantics

Floe can have cache rebuilds and recovery scans, but they cannot be hidden inside
the steady-state claim.

Work items:

- Audit all lazy materialization paths, starting with
  `IncrementalAggregateOperator::ensure_state_cache`.
- Decide per path whether to eliminate the rebuild, make it replayable from a
  compact auxiliary state, or profile it as `ColdStartOrRecovery`.
- Add a runtime mode for tests that forces cold caches so rebuild counters are
  exercised.
- Ensure hot steady-state tests run after warmup and assert zero rebuild rows.
- Ensure recovery tests assert rebuild rows separately from foreground delta
  rows.

Acceptance criteria:

- No production steady-state tick can unexpectedly materialize full integrated
  state without incrementing a cold/rebuild counter.
- Strong incrementality tests fail if cache rebuild work appears in the
  foreground operator budget.

## Phase 7: Planner And SQL Surface Gates

The strong claim is made about supported materialized-view plans, so the planner
must know when a SQL shape has a pure incremental work profile and when it
requires mixed work. Floe should prefer incremental implementations, but it does
not need to reject every SQL shape that requires broader work.

Work items:

- Attach operator work profiles to physical plan construction.
- Compute the plan-level work profile from its operators.
- Reject unprofiled plans before execution.
- Reject plans that violate configured work-profile policy. For example, a
  deployment may choose to reject `FullStateWork`, while a development or
  explicit compatibility mode may allow it.
- Include the reason in the plan validation error when a plan is rejected.
- Add an explain/debug view that shows the DBSP operator work profile and work
  contract for each plan node.
- Keep reference/full-recompute paths available only under explicit test or
  debugging modes.

Acceptance criteria:

- SQL cannot accidentally execute through a broader-work runtime path while
  still being described as a pure incremental MV.
- Mixed-work MVs are allowed only when the plan-level profile exposes the
  broader work and counters measure it.
- Plan validation tests cover every work-profile exception.

## Phase 8: Benchmarks And Public Evidence

Once counters and regression gates exist, benchmarks become useful evidence
instead of the primary proof.

Benchmark requirements:

- Keep the current 1M Kafka filter/projection no-sink benchmark as a throughput
  guardrail.
- Add microbenchmarks that vary history size independently of delta size.
- Add microbenchmarks that vary affected fanout/group/window size.
- Add benchmark output that records logical counters alongside throughput.
- Compare selected scenarios with Feldera where possible, but use logical
  counters as the core Floe proof.

Acceptance criteria:

- Benchmark reports include both throughput and logical work counters.
- A performance gain that increases hidden state scans is considered a
  regression unless the operator contract permits it.

## Phase 9: Strengthen The Public Claim

Only after Phases 1-8 pass should Floe strengthen public wording in README,
crate docs, or product docs.

The public claim should include:

- The supported operator/SQL surface.
- The steady-state incrementality contract for pure incremental plans.
- The mixed-work contract for SQL shapes that require affected-slice or
  full-state work.
- The output/fanout caveat.
- The affected-group caveat for aggregates and windows.
- The explicit exclusion of cold start, recovery, snapshotting, and compaction
  from foreground steady-state work.
- The names of tests/benchmarks/counters that back the claim.

Do not claim "every SQL query is O(input delta)." That is not true for Feldera
or DBSP in general. The correct strong claim is that Floe can compute and expose
the work profile of a materialized view, that pure incremental plans avoid work
proportional to unrelated historical state, and that mixed-work plans make any
broader work explicit instead of hiding it behind an incremental label.

## Recommended Milestones

1. **Inventory milestone**: all production operators and MV plan shapes have
   work profiles; current known broader-work cases are listed.
2. **Metrics milestone**: deterministic per-tick logical work collector exists.
3. **Join parity milestone**: equality join passes history-invariance tests and
   exposes Feldera-comparable counters.
4. **Aggregate parity milestone**: aggregate/distinct/top operators pass
   affected-slice tests or are explicitly gated.
5. **Storage parity milestone**: indexed state lookup and compaction have
   bounded/measured read amplification.
6. **Planner gate milestone**: hidden full-state work cannot enter production
   MV plans; mixed-work plans expose their profile.
7. **Public claim milestone**: README/crate docs are updated only after all
   parity gates pass.

## Immediate Next Actions

1. Build the operator inventory from `crates/dbsp-runtime/src/operators`,
   `crates/dbsp-planner`, and `crates/floe-executor`.
2. Add the work-profile enum and planner propagation path.
3. Add the in-process logical work collector.
4. Implement the first history-invariance test for equality join.
5. Expand the same test pattern across aggregate, distinct, TopN, and MV
   application paths.
