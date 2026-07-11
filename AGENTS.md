# Repository Guidelines

## About
Floe is a single-node streaming SQL database built around a DBSP runtime, DataFusion planning, and SlateDB-backed state.

Feldera is a similar streaming SQL database with a reference DBSP implementation.
Local reference repo: `/home/jlerche/programming_projects/github.com/feldera/feldera`.

## Workspace Layout
Floe is a Rust **workspace** (not a single binary crate). The workspace currently contains 13 crates under `crates/`.

- Default member: `crates/floe-node`.
- Running `cargo run` from repo root targets `floe-node` by default.
- Build artifacts go to `target/` and should not be committed.

### Crate Map
- `crates/floe-node`: CLI entrypoint and runtime orchestration (`run` and `tail` commands).
- `crates/floe-node-core`: connectors, source registry, MV planning helpers.
- `crates/floe-executor`: DBSP graph building, execution pipeline, MV runtime.
- `crates/floe-server`: pgwire server, TAIL protocol execution.
- `crates/floe-storage`: Floe-level catalog/state persistence.
- `crates/floe-core`: shared domain types (catalog/source/row abstractions).
- `crates/floe-sql-parser`: parser for Floe SQL statements.
- `crates/dbsp`: DBSP facade crate re-exporting planner/circuit/runtime/storage APIs.
- `crates/dbsp-runtime`: stream runtime, operators, handle-based ZSet mechanics.
- `crates/dbsp-storage`: DBSP storage primitives (dictionary, manifests, segments, GC).
- `crates/dbsp-circuit`: DBSP logical plan/circuit types.
- `crates/dbsp-planner`: DataFusion -> DBSP circuit translation.
- `crates/floe-benchmarks`: benchmark binaries and Criterion benches.

## Build, Run, and Test Commands
- `cargo check --workspace`: fastest workspace compile sanity check.
- `cargo test --workspace --no-run`: compile all tests without executing them.
- `cargo test --workspace`: run all tests (slow; includes end-to-end suites).
- `cargo run -p floe-node -- run --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"`: run node.
- `cargo run -p floe-node -- tail --mv mv`: tail an MV via pgwire client.
- `cargo fmt --all`: format all crates.
- `cargo clippy --workspace -- -D warnings`: strict lint pass across workspace.

## Targeted Test Shortcuts
Use focused tests during normal development; run full workspace tests before large merges.

- DBSP stream/runtime core: `cargo test -p dbsp-runtime stream::tests::core::`
- DBSP dictionary/storage: `cargo test -p dbsp-storage storage::dictionary::tests::`
- DBSP columnar index path: `cargo test -p dbsp-runtime collections::columnar_indexed_zset::tests::`
- Executor plan validation: `cargo test -p floe-executor --test plan_validation`
- End-to-end node flows: `cargo test -p floe-node --test production_smoke`
- Ignored integration tests (e.g., Kafka): `cargo test --workspace -- --ignored`

## Coding Style and Conventions
- Rust edition is `2024` (workspace-wide).
- Use `rustfmt` defaults and idiomatic naming:
  - modules/functions/variables: `snake_case`
  - types/traits: `UpperCamelCase`
  - constants: `SCREAMING_SNAKE_CASE`
- Keep modules focused; prefer narrow public APIs and local `pub(crate)` visibility where possible.
- Avoid introducing keyspace or serialized-format changes without updating all readers/writers in the DBSP stack.

## Current Performance Direction
- Treat the current hot-path goal as preserving and improving the `~220k input rows/s` 1M Kafka filter/projection no-sink benchmark on `master`, with the medium-term target of pushing Floe as close to `300k input rows/s` as possible on that workload.
- The comparison bar is no longer the older single-worker Materialize emulator run. Use the persisted cross-engine harness in `scripts/stream_engine_compare.sh` with:
  - RisingWave in-memory mode
  - Feldera best-effort no-spill storage mode
  - equivalent low-latency Kafka consumer fetch settings where the engine supports them
- Performance work should assume the external comparison set can reach roughly the high-200k to high-300k rows/s range on this logical workload when Kafka consumer latency settings are aligned.
- The preferred durability model is:
  - durable source-batch journal as the synchronous commit boundary
  - overlay-backed materialized views for eligible plans
  - asynchronous, off-thread MV snapshotting
  - tunable snapshot policy, not per-tick MV flushes
- Do not reintroduce per-tick durable MV flushes or other foreground persistence fanout on eligible fast paths unless there is a correctness requirement that cannot be met another way.
- Keep Kafka ingest latency-sensitive. Avoid changes that add avoidable fetch backoff, poll delay, or batch-boundary stalls in the connector path.
- Keep the Kafka consumer path aligned with the current low-latency fetch profile unless there is a measured reason to change it:
  - `fetch.wait.max.ms = 1`
  - `fetch.queue.backoff.ms = 1`
  - `fetch.min.bytes = 1`

## Vectorization Policy
- Full vectorized execution is the default expectation across planning, ingest, DBSP operators, and MV application.
- Row-wise or scalar hot-path execution is forbidden unless it is strictly necessary for correctness or for a feature that does not yet have a viable vectorized implementation.
- If a row-wise fallback is introduced or retained, document why it is unavoidable and keep it off the steady-state hot path whenever possible.
- New work should prefer batch-native representations and transformations over per-row `Vec`/`HashMap` rebuilds, per-row encode/decode loops, or repeated full materialization.
- When touching filter/project/map-style paths, preserve vectorized execution end-to-end unless there is an explicit, measured reason not to.

## Practical Development Notes
- If a change touches DBSP persistence behavior, inspect both runtime and storage crates:
  - runtime stream/handle behavior in `crates/dbsp-runtime/src/`
  - manifest/keyspace/GC behavior in `crates/dbsp-storage/src/storage/`
- The reports archive has been consolidated into `reports/REPORTS_SUMMARY.md`; benchmarks and harnesses live in `crates/floe-benchmarks/` and `crates/dbsp/benches/`.
- If a change touches the current fast path, prefer validating with the 1M Kafka no-sink filter/projection benchmark in `crates/floe-node/tests/redpanda_kafka_million_filter_projection_nosink_e2e.rs`.
- If a change touches MV durability or recovery, validate both steady-state throughput and restart/crash semantics.
- There is no CI workflow file in this repo today; local validation commands above are the source of truth.

## Commit and PR Guidelines
- Prefer Conventional Commits (`feat:`, `fix:`, `refactor:`, `test:`, `docs:`).
- Keep PRs scoped; include:
  - what changed,
  - why it changed,
  - exact validation commands executed,
  - relevant logs/screenshots for user-visible behavior changes.
- If follow-up refactors are discovered mid-task, land the minimum safe scope first and open follow-ups.
