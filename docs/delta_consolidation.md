# Delta Consolidation

This document describes how Floe consolidates delta output batches per tick
and how the system can evolve from row-based consolidation to key-based
consolidation.

## Current behavior (MVP)

Delta batches are consolidated per tick by grouping on all row columns plus any
optional `__key` column, then summing `__weight`:

- `GROUP BY` all non-`__weight` columns
- `SUM(__weight)` produces the consolidated weight
- Rows with `__weight == 0` are dropped

This is implemented in `crates/floe-executor/src/delta_consolidation.rs` via
`DeltaConsolidator`.

## Key-based consolidation (planned)

When the optional `__key` column is present, it represents a stable encoding of
primary key columns. This allows a faster consolidation path that groups only
on `__key` while aggregating the payload columns (currently using `MIN` as a
stable pick). The assumption is that non-key columns are functionally
dependent on `__key` (i.e., the primary key uniquely identifies the row), so
any stable aggregate produces the same payload.

Migration path:

1. Produce `__key` in all vectorized delta batches.
2. Switch `DeltaConsolidator` to `ConsolidationMode::ByKey` once primary key
   uniqueness is guaranteed.
3. (Future) Replace the payload aggregates with a keyed lookup, so the
   consolidation only groups `__key` + `__weight` and rehydrates payload
   columns from state.

Until step 2, the system continues to consolidate by grouping all columns to
preserve full multiset semantics.
