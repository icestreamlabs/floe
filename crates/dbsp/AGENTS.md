# DBSP Crate Agent Guide

This file is guidance for coding agents working in `crates/dbsp`.

## What This Crate Actually Is
`dbsp` is a **facade crate**. It primarily re-exports APIs from sibling crates:

- `dbsp-runtime` (stream runtime, operators, handle/ZSet mechanics)
- `dbsp-storage` (key-value abstractions, dictionary, manifests, segments, GC)
- `dbsp-circuit` (circuit/plan node types)
- `dbsp-planner` (DataFusion -> DBSP plan translation)

The facade itself is mostly `src/lib.rs` plus Criterion benches in `benches/`.

## Where To Make Changes
If behavior changes are needed, edit the owning crate first:

- Runtime stream semantics and maintained columnar primitives: `crates/dbsp-runtime/src/`
- Persistent storage format/keyspace/manifest/GC: `crates/dbsp-storage/src/storage/`
- Circuit node definitions and expression/schema types: `crates/dbsp-circuit/src/circuit/`
- DataFusion planning translation: `crates/dbsp-planner/src/planner/`
- Facade exports and public API surface: `crates/dbsp/src/lib.rs`

Do not add major runtime logic directly to `crates/dbsp/src/lib.rs`.

## Public API Surface in This Crate
`crates/dbsp/src/lib.rs` currently:

- re-exports planner + circuit node/types (`CircuitPlan`, `DbspNodeKind`, expressions, schema/types),
- re-exports runtime modules (`stream`, `collections`, `handles`, and the maintained columnar count operator),
- re-exports storage namespace via `dbsp_storage::storage`.

When modifying exports, check downstream usage in:
- `crates/floe-executor`
- `crates/floe-node`
- `crates/floe-server`

## Core Invariants to Preserve
These invariants are implemented in runtime/storage crates and must stay consistent when changing DBSP behavior.

1. Handle identity is stable and lightweight.
- `ZSetHandle` is `{ ns, version }`.
- `StreamHandle` is `{ ns, frontier }`.
- Stored stream rows should reference handles, not embedded collection payloads.

2. Stream flushes are intent-guarded.
- Stream flush writes pending defaults/data/state and an intent key, then clears intent after successful batch write.
- Committed frontier should only advance after durable flush sequence completes.

3. ZSet stream flush can emit both snapshot and delta handles.
- `ZSetStream::flush_with_delta()` returns `(snapshot_handle, delta_handle)`.
- Even empty overlays advance stream time consistently.

4. Zero-weight entries are pruned.
- ZSet and delta paths remove or ignore zero-weight entries to avoid state bloat and incorrect identity checks.

5. Dictionary IDs are stable for persisted keys.
- Dictionary intern/resolve behavior must remain deterministic and robust to partial writes/recovery.

6. Versioned manifests define layering.
- Version chains are represented by manifest + segments; retention/compaction/GC must preserve reachable versions.

## Testing and Validation
For facade-only changes (re-exports/docs):
- `cargo test -p dbsp`

For runtime/storage behavior changes:
- `cargo test -p dbsp-runtime stream::tests::core::`
- `cargo test -p dbsp-runtime collections::columnar_indexed_zset::tests::`
- `cargo test -p dbsp-storage storage::dictionary::tests::`

For wider integration confidence:
- `cargo test --workspace --no-run`
- `cargo test -p floe-executor`

Formatting/lints:
- `cargo fmt --all`
- `cargo clippy --workspace -- -D warnings`

## Benchmarks
The `dbsp` crate owns Criterion benches for the maintained handle and
materialization paths:
- `benches/delta_emission.rs`
- `benches/materialization_metrics.rs`

Useful commands:
- `cargo bench -p dbsp --no-run`
Benchmarks use in-memory object store + SlateDB and exercise maintained handle
and materialization paths.

## Reference Context
For semantic cross-checking, compare with Feldera when needed:
`/home/jlerche/programming_projects/github.com/feldera/feldera`
