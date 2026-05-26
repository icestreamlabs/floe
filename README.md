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
- Postgres CDC ingest, configured through SQL or config files, uses native
  logical replication with `pgoutput`.
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
  - `floe_postgres_cdc_source_lag_bytes{source=...,slot=...}`
  - `floe_postgres_cdc_table_lag_bytes{source=...,slot=...,table=...}`
  - `floe_cdc_buffer_pending_records{pipeline=...}`
  - `floe_cdc_buffer_pending_bytes{pipeline=...}`
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
2. Native Postgres CDC uses transaction/change batches and replication pipelines
   instead of the append-ingest event path.
3. Events and CDC changes are decoded into typed rows via planner/runtime
   source definitions.
4. Outer streams feed the DBSP runtime built from DataFusion logical plans.
5. Materialized views are managed in the executor and exposed via pgwire TAIL.
6. State is stored in SlateDB (in-memory by default, filesystem when configured).

## Postgres CDC Alpha Surface

Postgres CDC is an alpha feature for single-node deployments. It supports:

- `CREATE SOURCE ... WITH (connector = 'postgres-cdc', connection = ...,
  slot.name = ..., publication.name = ...)` using native `pgoutput` logical
  replication.
- `CREATE TABLE ... FROM <source> TABLE '<schema.table>'` bindings for CDC
  source tables.
- Materialized views over CDC-backed tables, including snapshot backfill, WAL
  handoff, inserts, updates, deletes, joins, and aggregates.
- `CREATE REPLICATION PIPELINE ... INTO KAFKA` with `floe-json`,
  `debezium-json`, or experimental `arrow-ipc` payloads.
- `CREATE REPLICATION PIPELINE ... INTO POSTGRES` with `floe-json` payloads.
- Optional durable replication buffers with bounded pending bytes, records,
  transactions, and age limits.
- Postgres materialized-view sinks in `upsert` and `append_only` modes.
- Operator inspection under `/ops/cdc/replication` and DLQ list, inspect,
  retry, batch retry, and discard endpoints under `/ops/cdc/replication/dlq`.
  The older `/debug/cdc/replication` paths remain engineering aliases.

Current requirements and limitations:

- The Postgres publication and logical replication slot are auto-created by
  default when possible. Use `publication.create = false` or
  `slot.create = false` to require manually managed objects.
- CDC tables need primary-key metadata for update/delete and target upsert
  semantics.
- Schema evolution has an explicit alpha contract:
  - `schema_evolution = 'fail_fast'` rejects any observed schema change.
  - `ignore_compatible` and `apply_compatible_additions` allow nullable,
    non-key columns appended to the upstream table and continue projecting rows
    into the catalog schema used by materialized views and replication sinks.
  - Drop/reorder/type/primary-key/replica-identity changes fail closed.
  - Non-key nullability/default changes are not carried in `pgoutput` relation
    metadata; Floe enforces the catalog schema on rows and fails if an upstream
    change starts emitting incompatible NULLs.
  - Postgres replication targets are checked for required columns, compatible
    types/nullability, extra required target columns, and a unique index matching
    the CDC primary key before rows are applied.
- Common scalar types are covered; arrays, enums/domains, intervals, and range
  types remain deferred.
- Source/target HA failover, reconciliation/drift checks, richer operator CLI
  UX, and larger published performance baselines are follow-up product work.
- Arrow IPC Kafka output is internal/experimental and should not be used for
  public apples-to-apples format claims without calling that out.

Postgres CDC operator endpoints:

- `GET /ops/cdc/replication` lists pipelines with source lag, target state,
  durable buffer stats, DLQ summary, replay state, and latest target error.
- `GET /ops/cdc/replication/dlq?pipeline=...&status=pending&limit=100&offset=0`
  lists DLQ entries with bounded pagination and optional status filtering.
- `GET /ops/cdc/replication/dlq/{pipeline}/{dlq_id}` inspects a single DLQ
  entry.
- `POST /ops/cdc/replication/dlq/{pipeline}/{dlq_id}/retry` retries one DLQ
  entry. The JSON body may include `{ "reason": "...", "operator": "..." }`.
- `POST /ops/cdc/replication/dlq/retry?pipeline=...&limit=100` retries a
  bounded batch of pending DLQ entries. The JSON body may include
  `{ "reason": "...", "operator": "..." }`.
- `POST /ops/cdc/replication/dlq/{pipeline}/{dlq_id}/discard` discards one DLQ
  entry. The JSON body must include `reason` and may include `operator`.

Operator output avoids connector connection strings and does not return raw DLQ
payload bytes; DLQ metadata may include payload object keys, formats, and byte
counts for recovery/audit workflows.

Postgres CDC type compatibility:

| Postgres type family | Floe CDC representation | Source decode | Kafka JSON | Postgres target |
| --- | --- | --- | --- | --- |
| `bool` | `Bool` | supported | boolean | supported |
| `int2`, `int4`, `int8` | `Int64` | supported | number | supported |
| `text`, `varchar`, `bpchar`, `name` | `Utf8` | supported | string | supported |
| `uuid` | `Utf8` canonical string | supported | string | supported with `uuid` target cast |
| `json`, `jsonb` | `Utf8` JSON text | supported | string | supported with `json`/`jsonb` target cast |
| `bytea` | `Utf8` Postgres bytea text (`\\x...`) | supported | string | supported with `bytea` target cast |
| `date` | `DateDays` | supported | days since epoch | supported |
| `timestamp`, `timestamptz` | `TimestampMillis` | supported | epoch milliseconds | supported |
| `numeric(p,s)` with `p <= 38` | `Decimal128` | supported | decimal string | supported |
| unconstrained `numeric` / larger precision | `Numeric` string | supported | string | supported |
| `float4`, `float8` | none | rejected | n/a | n/a |
| arrays, ranges, multiranges | none | rejected/deferred | n/a | n/a |
| enums/domains | none | rejected/deferred | n/a | n/a |
| `time`, `timetz`, `interval` | none | rejected/deferred | n/a | n/a |

Useful validation entry points:

- `scripts/run_postgres_cdc_pgoutput_e2e.sh`
- `scripts/run_postgres_cdc_binary_e2e.sh`
- `cargo test -p floe-cdc-pg --test postgres_pgoutput_e2e`
- `FLOE_ACCEPTANCE_PG_DSN=... cargo test -p floe-node --test ga_acceptance -- --ignored`
- `scripts/postgres_cdc_perf_local.sh`
- `scripts/postgres_cdc_perf_matrix.sh`

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

Canonical full validation sequence:

```bash
bash scripts/validate_workspace.sh
```

This repository currently tracks README and scripts as the committed operational
surface. The `docs/` directory is ignored by `.gitignore`, so local files there
are not part of the published repository unless that policy changes.
