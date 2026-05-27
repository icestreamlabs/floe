# DBSP Incrementality Evidence

This pass replaces descriptive incrementality claims with executable counters and history-invariance checks. The central contract is `LogicalWorkSnapshot`: operators report the foreground logical work they performed for the last tick, and tests assert that fixed deltas stay flat as unrelated history grows.

## Covered Contracts

- Join and semijoin/antijoin: fixed changed keys do not scale with unrelated opposite-side history; affected fanout remains visible through state rows examined and output rows.
- Filter/project, union, and MV sink: work is bounded by the current delta and emitted delta, with no full-state scans.
- Distinct and aggregate paths: changed values/groups drive state lookups; unrelated groups do not increase steady-tick work.
- Count, count-distinct, and incremental aggregates: changed groups and distinct auxiliary checks are counted explicitly.
- Top1, TopN, and window aggregates: changed partitions/windows and affected slice rows are counted explicitly.
- Storage lookup paths: indexed lookups report lookup keys, returned rows, segments examined, postings examined, and cache hits/misses.
- Cold-start/recovery-sensitive paths: cache rebuild rows and full-scan counts are part of the same snapshot, so steady-state tests can assert they remain zero.
- Planned materialized views: each published MV version records a `LogicalWorkSnapshot`, which lets SQL-level tests assert emitted/apply work without reaching into private operator state.
- Retractions and negative weights: planned MVs, MV sink application, TAIL output, and runtime operator tests include delete/update-style deltas.
- Batch boundaries and consolidation: equivalent logical changes converge whether applied in one batch or split across ticks, and canceling weights do not leave visible zero-weight rows.
- Source and MV boundaries: source-journal replay records delta-local MV work, and unrelated source deltas do not advance unaffected MVs.
- Maintenance boundaries: compaction/reopen tests preserve trace semantics across retractions and zero-weight consolidation.
- User-visible output: TAIL emits steady-state deltas, including retractions, instead of re-emitting full MV snapshots.

## Issue Coverage

- #26, #27, #30: planned SQL/MV tests assert per-version MV logical work from source deltas through MV apply.
- #28, #36: restart, replay, compaction, and reopen tests distinguish recovery/maintenance semantics from hot steady-state work.
- #29, #31: the benchmark emits logical counters alongside timing so instrumentation cost and hidden full scans are visible.
- #32: the validation command set below is the focused pre-merge proof suite.
- #33: unsupported SQL shapes are rejected by the planner/validator; supported planned paths now have executable evidence at runtime or MV boundaries.
- #34, #35, #37, #38: retractions, batch-boundary invariance, multi-source/MV boundaries, and TAIL delta semantics have dedicated tests.

## Benchmark Evidence

The Criterion benchmark `incrementality_evidence` varies unrelated history independently from affected join fanout and prints logical work snapshots alongside timing output:

```bash
cargo bench -p dbsp --bench incrementality_evidence
```

The emitted `incrementality_evidence ...` lines include:

- `input_delta_rows`
- `right_state_rows_examined`
- `output_delta_rows`
- `state_lookup_keys`
- `index_segments_examined`
- `index_postings_examined`
- `state_full_scan_count`
- `cache_rebuild_rows`

Expected reading:

- `history_fixed_fanout` should keep lookup/output work flat when history grows from 1k to 10k with fixed fanout.
- `fanout_fixed_history` should grow with affected fanout while unrelated history is held fixed.
- `state_full_scan_count` and `cache_rebuild_rows` should stay zero for the measured steady tick.

## Validation Run

Focused proof-suite command:

```bash
scripts/dbsp_incrementality_proof.sh
```

Expanded command set used by the script:

```bash
cargo test -p dbsp-runtime operators::
cargo test -p dbsp-runtime stream::tests::core::compaction::
cargo test -p dbsp-runtime collections::arrow_indexed_batch_zset::tests::
cargo test -p floe-executor operators::mv_sink::tests::
cargo test -p floe-executor tail::tests::
cargo test -p floe-executor --test dbsp_graph_builder
cargo test -p floe-executor --test plan_validation
cargo bench -p dbsp --bench incrementality_evidence --no-run
```

Additional broad validation commands run for this pass:

```bash
cargo test -p dbsp-runtime stream::tests::core::pipeline::
cargo test -p dbsp-runtime filter_map::tests::
cargo clippy -p dbsp-runtime -p floe-executor -- -D warnings
cargo check --workspace
cargo test --workspace --no-run
git diff --check
```
