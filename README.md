# Floe

Floe is a single-node streaming SQL database built on a DBSP runtime, DataFusion
planning, and SlateDB-backed state. It ingests events from connectors, builds
materialized views, and serves results over a pgwire-compatible endpoint.

## Quickstart

Build:

```bash
cargo build
```

Run the node with the built-in Nexmark generator and a single materialized view:

```bash
cargo run -- run --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

Tail the view over pgwire (defaults to 127.0.0.1:6432):

```bash
cargo run -- tail --mv mv
```

Optional inputs:

- File ingest (newline-delimited JSON, use "-" for stdin):
  `cargo run -- run --input-file /path/to/events.json --input-source nexmark_bid`
- Kafka ingest:
  `cargo run -- run --kafka-brokers localhost:9092 --kafka-topics nexmark_bid`
- Connector config file (TOML/YAML/JSON):
  `cargo run -- run --config /path/to/connectors.toml`
- Validate config + SQL planning without startup side effects:
  `cargo run -- run --config /path/to/connectors.toml --dry-run`

Runtime configuration:

- `--pgwire-addr` / `runtime.pgwire_addr` to change the pgwire bind address
  (default `127.0.0.1:6432`)
- `--data-dir` / `storage.data_dir` to persist SlateDB state (default
  in-memory)
- `--admin-port` / `runtime.admin_port` to change the admin HTTP port
- `--slatedb-config` / `storage.slatedb_config` to load a SlateDB settings file
  (TOML/YAML/JSON)

Observability:

- Floe always runs an admin server exposing `/healthz`, `/readyz`, and
  `/metrics`.
- Admin host defaults to `--http-host`; admin port defaults to `8081`
  (`--admin-port` or `runtime.admin_port` overrides).
- `/healthz` reports process liveness.
- `/readyz` reports process + executor + storage + runtime readiness.
- If HTTP ingest is enabled, `/healthz`, `/readyz`, and `/metrics` are also
  available on the ingest server.
- Default tracing schema (span names + fields) for correlation:
  - `ingest_decode`: `epoch`, `raw_batch_size`, `decoded_rows`, `latency_ms`
  - `connector_tick`: `epoch`, `watermark`, `tick_latency_ms`
  - `dbsp_write`: `graph_id`, `view`, `namespace`, `version`, `latency_ms`
  - `tail_emit`: `mv`, `version`, `mode`, `rows`
  Correlation: `connector_tick.epoch` aligns with `dbsp_write.version` for MV
  updates produced by that ingest tick.
- Key Prometheus counters:
  - `floe_ingest_ticks_total{result=...}`
  - `floe_source_offset_lag{source=...,partition=...}`
  - `floe_mv_freshness_seconds{view=...}`
  - `floe_runtime_errors_total{component=...}`

### Storage tuning (SlateDB)

Floe uses SlateDB defaults unless configured. You can tune flush, compaction, and
cache settings via:

- CLI overrides (examples):
  - `--slatedb-flush-interval-ms 250`
  - `--slatedb-compaction-max-sst-bytes 268435456`
  - `--slatedb-compaction-max-concurrent 2`
  - `--slatedb-await-durable`
  - `--slatedb-cache-dir /tmp/floe-slate-cache --slatedb-cache-max-bytes 1073741824`
- A settings file: `--slatedb-config /path/to/SlateDb.toml` or
  `storage.slatedb_config = "/path/to/SlateDb.toml"`
- Environment variables can still be used for SlateDB internals when explicitly
  requested with `--slatedb-env-prefix` or `storage.slatedb_env_prefix`.

Defaults (SlateDB 0.8.2):

- Flush interval: 100ms
- L0 SST size: 64 MiB; max unflushed bytes: 1 GiB
- Compaction: poll interval 5s, max SST size 256 MiB, max concurrent 4
- Object store cache: disabled unless a cache dir is set; default cache size
  16 GiB and part size 4 MiB

Tradeoffs:

- Flush interval: lower values reduce commit latency and speed recovery but
  increase object store PUT frequency; higher values reduce cost but increase
  WAL size and visibility lag.
- Compaction: larger SST targets and lower concurrency reduce compaction IO,
  while smaller targets or higher concurrency reduce read amplification at the
  cost of more background work.
- Cache: enabling the object store cache speeds reads at the cost of local disk
  usage; larger caches help scan-heavy workloads.

## Architecture Overview

1. Append-style connectors emit `AppendIngestEvent` payloads (file, Kafka, or generator).
2. Events are decoded into typed rows via `SourceRowDecoder`.
3. Outer streams feed the DBSP runtime built from DataFusion logical plans.
4. Materialized views are managed in the executor and exposed via pgwire TAIL.
5. State is stored in SlateDB (in-memory by default, filesystem when configured).

## Workspace Modules and Ownership

- `crates/floe-node`: CLI entrypoint and orchestration for connectors + execution.
- `crates/floe-node-core`: connector implementations, MV planner, and generator.
- `crates/floe-executor`: DataFusion plan validation, DBSP graph building, and MV runtime.
- `crates/floe-server`: pgwire protocol server and TAIL query execution.
- `crates/floe-storage`: SlateDB catalog integration and persistence glue.
- `crates/floe-core`: shared types (catalog, source definitions, row values).
- `crates/floe-sql-parser`: SQL parsing for Floe-specific statements (MV/SINK/TAIL).
- `crates/dbsp`: core DBSP APIs and types (align semantics with Feldera).
- `crates/dbsp-circuit`: circuit representation and planning utilities.
- `crates/dbsp-planner`: DataFusion to DBSP plan translation.
- `crates/dbsp-runtime`: runtime operators, streams, and state mechanics.
- `crates/dbsp-storage`: DBSP storage utilities.

## Development

Common commands:

```bash
cargo build
cargo test
cargo fmt
cargo clippy -- -D warnings
```

Repo hygiene checker:

```bash
bash scripts/repo_hygiene.sh
```

Policy details and exception process:

- `reports/REPO_HYGIENE_POLICY.md`

Additional operational documentation:

- `docs/production_readiness.md`: production exit checklist and quick operator runbook.
- `docs/runtime_config.md`: config-first schema, precedence rules, and examples.
- `docs/storage_data_directory.md`: `--data-dir` behavior and safe reset procedure.
- `docs/operator_runbook.md`: production-like startup/restart/troubleshooting guide.
- `docs/ga_contract.md`: connector/sink delivery guarantees and GA limitations.
- `docs/supported_features.md`: explicit SQL/connectors/sinks support matrix.
- `docs/local_deploy.md`: local compose deploy (Kafka + Postgres + Floe).
- Canonical full validation sequence: `scripts/validate_workspace.sh`.
