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
- DBSP operator join path: `cargo test -p dbsp-runtime operators::join::tests::`
- Executor plan validation: `cargo test -p floe-executor --test plan_validation`
- End-to-end node flows: `cargo test -p floe-node --test end_to_end_mv`
- Ignored integration tests (e.g., Kafka): `cargo test --workspace -- --ignored`

## Coding Style and Conventions
- Rust edition is `2024` (workspace-wide).
- Use `rustfmt` defaults and idiomatic naming:
  - modules/functions/variables: `snake_case`
  - types/traits: `UpperCamelCase`
  - constants: `SCREAMING_SNAKE_CASE`
- Keep modules focused; prefer narrow public APIs and local `pub(crate)` visibility where possible.
- Avoid introducing keyspace or serialized-format changes without updating all readers/writers in the DBSP stack.

## Practical Development Notes
- If a change touches DBSP persistence behavior, inspect both runtime and storage crates:
  - runtime stream/handle behavior in `crates/dbsp-runtime/src/`
  - manifest/keyspace/GC behavior in `crates/dbsp-storage/src/storage/`
- Performance and sprint logs live in `reports/`; benchmarks and harnesses live in `crates/floe-benchmarks/` and `crates/dbsp/benches/`.
- There is no CI workflow file in this repo today; local validation commands above are the source of truth.

## Commit and PR Guidelines
- Prefer Conventional Commits (`feat:`, `fix:`, `refactor:`, `test:`, `docs:`).
- Keep PRs scoped; include:
  - what changed,
  - why it changed,
  - exact validation commands executed,
  - relevant logs/screenshots for user-visible behavior changes.
- If follow-up refactors are discovered mid-task, land the minimum safe scope first and open follow-ups.
