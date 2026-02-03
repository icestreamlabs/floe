# Incremental State Update Semantics

This document defines the state update semantics used by the DBSP runtime during
incremental execution, with a focus on joins and versioned ZSet compaction.

## State Storage Model

State is stored as **append-only versions** of ZSets. Each tick writes a new
version that references the previous version as a base. Periodic compaction
creates a new compacted version and releases older segments.

## Atomic Step Semantics

Each tick is treated as an atomic step:

1. **Compute output delta** from the *previous* integrated state and the
   current deltas.
2. **Apply deltas to state and indexes** (integrated state, join indexes).
3. **Persist output delta** as the result of the step.

This ordering ensures that a tick's output reflects `(A, B, ΔA, ΔB)` where
`A` and `B` are the states **before** applying the current deltas. State
mutations never influence the output of the same tick.

## Join Delta Semantics

Incremental joins follow the standard DBSP delta formula:

```
Δ(A ⋈ B) = (ΔA ⋈ B) + (A ⋈ ΔB) + (ΔA ⋈ ΔB)
```

- Each term is short-circuited when its delta input is empty.
- Output deltas are consolidated per tick by summing weights and dropping zeros.

The join operator in `crates/dbsp-runtime/src/operators/join/op.rs` computes the
full output delta *before* mutating the integrated states or indexes.

## Compaction Cadence and Triggers

Versioned ZSet compaction is controlled by `CompactionPolicy` and runs after a
successful version update during a stream flush.

- **Cadence**: compaction is evaluated on every flush that writes a new version
  (see `crates/dbsp-runtime/src/stream/zset_stream/core.rs`).
- **Trigger**: compaction runs when either condition is met
  (`CompactionPolicy::should_compact` in
  `crates/dbsp-runtime/src/collections/zset/versioned/state.rs`):
  - `version_count >= max_chain_len`
  - `segment_count >= max_segments`

Default policy:

- `max_chain_len = 32`
- `max_segments = 256`

Compaction can be disabled via `CompactionPolicy::disabled()`.
