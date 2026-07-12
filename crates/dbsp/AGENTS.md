# DBSP Runtime and Storage Guidelines

## Scope

This subtree contains Floe's operational DBSP facade, runtime streams/operators, and SlateDB-backed storage. DataFusion logical plans are compiled by `floe-executor`; there is no separate shadow circuit planner.

## Crates

- `dbsp`: narrow facade over the active runtime and storage APIs.
- `dbsp-runtime`: streams, handles, columnar ZSets, maintained operators, and operator-state restore.
- `dbsp-storage`: dictionaries, manifests, segments, keyspaces, compaction, and GC.

Keep facade exports narrow and backed by production callers. Do not add generic paper-model abstractions unless the production executor consumes them.

## Validation

- `cargo test -p dbsp-runtime stream::tests::core::`
- `cargo test -p dbsp-storage storage::dictionary::tests::`
- `cargo test -p dbsp-runtime collections::columnar_indexed_zset::tests::`
- `cargo test -p dbsp --benches --no-run`

When changing persistence behavior, inspect both `dbsp-runtime` and `dbsp-storage`, and preserve serialized formats unless all readers/writers and recovery tests are updated together.
