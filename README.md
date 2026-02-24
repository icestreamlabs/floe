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

- `FLOE_PG_ADDR` to change the pgwire bind address (default `127.0.0.1:6432`)
- `FLOE_DATA_DIR` to persist SlateDB state (default in-memory)
- `FLOE_SLATEDB_CONFIG` to load a SlateDB settings file (TOML/YAML/JSON)
- `FLOE_SLATEDB_ENV_PREFIX` to change the SlateDB env prefix (default `SLATEDB_`)

Observability:

- When the HTTP ingest server is enabled, it also exposes `/metrics` and
  `/healthz` on the same host/port.
- `/healthz` returns `200` only when:
  - the process is running,
  - the executor loop is still alive,
  - storage initialization is healthy.
- Default tracing schema (span names + fields) for correlation:
  - `ingest_decode`: `epoch`, `raw_batch_size`, `decoded_rows`, `latency_ms`
  - `connector_tick`: `epoch`, `watermark`, `tick_latency_ms`
  - `dbsp_write`: `graph_id`, `view`, `namespace`, `version`, `latency_ms`
  - `tail_emit`: `mv`, `version`, `mode`, `rows`
  Correlation: `connector_tick.epoch` aligns with `dbsp_write.version` for MV
  updates produced by that ingest tick.
- Key Prometheus counters:
  - `floe_ingest_ticks_total{result=...}`
  - `floe_mv_updates_total`
  - `floe_tail_rows_total`
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
- A settings file: `--slatedb-config /path/to/SlateDb.toml` (or `FLOE_SLATEDB_CONFIG`)
- Environment variables: `SLATEDB_FLUSH_INTERVAL=250ms` (prefix configurable via
  `FLOE_SLATEDB_ENV_PREFIX`)

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

1. Connectors emit `SourceEvent` payloads (file, Kafka, or generator).
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

Additional operational documentation:

- `docs/production_readiness.md`: production exit checklist and quick operator runbook.
- `docs/runtime_config.md`: config-first schema, precedence rules, and examples.
- `docs/storage_data_directory.md`: `FLOE_DATA_DIR` behavior and safe reset procedure.
- `docs/operator_runbook.md`: production-like startup/restart/troubleshooting guide.
- Canonical full validation sequence: `scripts/validate_workspace.sh`.
