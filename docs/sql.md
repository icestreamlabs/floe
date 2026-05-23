# SQL Support and Operational Semantics

This document describes the SQL subset Floe supports today and the runtime
semantics for materialized views and TAIL.

## Supported Statements

- `CREATE MATERIALIZED VIEW [IF NOT EXISTS] <name> [WITH (<options>)] AS <select>`
- `CREATE TABLE <name> (<columns...>, PRIMARY KEY (...))`
- `CREATE SINK <sink_name> FROM <mv_name> WITH (<options>)`
- `TAIL <mv_name> [WITH SNAPSHOT] [AS OF <version>]`
- `SUBSCRIBE <mv_name> [WITH SNAPSHOT] [AS OF <version>]`
- `SELECT ... FROM <materialized_view>` (read-only queries via pgwire)

## SQL Program Parsing

- Floe accepts SQL program text with multiple semicolon-separated statements.
- Statement order is preserved and processed in-order.
- `--mv-query` accepts SQL programs containing:
  - `CREATE TABLE`
  - `CREATE MATERIALIZED VIEW`
  - `CREATE SINK`
- `TAIL` and `SUBSCRIBE` remain query-time statements and are not valid in
  `--mv-query`.

## Materialized View Definition Rules

- A single `CREATE MATERIALIZED VIEW` statement must be syntactically valid.
- `WITH` clauses are accepted in materialized view definitions, but the
  options are currently ignored.
- Identifiers can be double-quoted to preserve case or include special
  characters (`"MyView"`).
- The logical plan must compile to supported nodes (for example:
  Source/Select/Project/Join/Aggregate/WindowAggregate/TopN/Union/Distinct/Passthrough/Sink).
  Unsupported nodes are rejected at plan validation time.
- Joins are **inner equi-joins** on column references only.
  Non-column join expressions are rejected.
- NULL semantics:
  - `NULL = NULL` evaluates to unknown and is treated as `false` in `WHERE`.
  - Join keys containing NULL do not match.
- Supported scalar types in expressions: `INT64`, `BOOL`, `UTF8`, and
  `TIMESTAMP(MILLISECOND)`.
- `LIKE` only supports a single prefix or suffix `%` wildcard (no substring).
- `SELECT DISTINCT` and `UNION DISTINCT` are supported.

## CREATE SINK Syntax

Syntax:

```sql
CREATE SINK <sink_name>
FROM <mv_name>
WITH (
  connector = '<kafka|file|http|postgres>',
  ... connector/reliability options ...
)
```

Connector options:

- Kafka: `brokers`, `topic`
- File: `path`, optional `append`
- HTTP: `url`, optional `batch_size`
- Postgres: `connection`, `table`, optional `mode`, `primary_key`

Common tail options:

- `with_snapshot` (bool)
- `as_of` (int64)

Reliability options:

- `batch_rows` (flush threshold by row count)
- `batch_bytes` (flush threshold by serialized payload bytes)
- `queue_capacity` (bounded sink queue size)
- `retry_max_attempts` (Kafka/HTTP/Postgres)
- `retry_base_ms` (Kafka/HTTP/Postgres)
- `retry_max_backoff_ms` (Kafka/HTTP/Postgres)

Sink execution behavior:

- Flush occurs when a threshold is hit, on each tail tick boundary, and on shutdown.
- Postgres applies each MV version in one transaction using temporary COPY
  staging tables, then bulk delete/upsert or append SQL.
- Kafka/HTTP emission retries use bounded exponential backoff.
- On permanent Kafka/HTTP failures, sink execution stops and the sink task exits with error.
- Bounded sink queues apply backpressure when consumers fall behind.

## Materialized View Query Semantics

- Materialized views are read-only. `INSERT` and `CREATE TABLE` are rejected.
- Every materialized view exposes a reserved column `__mv_version` (Int64).
  Use it to query a point-in-time snapshot:
  - `SELECT ... FROM mv WHERE __mv_version = 42`
  - Only equality filters on `__mv_version` are recognized for as-of reads.

## Window Semantics

- `TUMBLE` / `HOP` assign rows to windows using the event-time expression in the
  window spec.
- Rows with event-time earlier than `(watermark - allowed_lateness)` are
  dropped. The SQL planner currently defaults `allowed_lateness` to `0`.

## Nexmark Support Matrix

The canonical Sprint 0005 Nexmark suite (`q0-q9`, `q12-q22`) is guarded by
`crates/floe-node-core/tests/nexmark_query_coverage.rs`.

Current status:
- All canonical queries pass logical planning, circuit planning, and runtime
  graph validation in the coverage harness.
- `q13` currently uses a processing-time + regular join SQL shape in Floe
  canonical coverage. Temporal lookup syntax (`FOR SYSTEM_TIME AS OF`) remains
  documented as unsupported below.

## Nexmark-Specific Limitations

Unsupported constructs with concrete errors:

- Temporal joins using `FOR SYSTEM_TIME AS OF`
  - Example parser error:
    - `failed to parse materialized view statement: sql parser error: Expected: one of UPDATE or SHARE, found: SYSTEM_TIME`
- Full `DATE_FORMAT` token parity is not yet guaranteed beyond tokens required
  by the Nexmark suite (`yyyy`, `MM`, `dd`, `HH`, `mm`, `ss`).
- `REGEXP_EXTRACT` returns `NULL` for invalid patterns (some systems raise
  errors instead).
- `SPLIT_INDEX` returns `NULL` for negative indices and empty delimiters.

## Version and Time Semantics

- Floe uses a single global logical epoch (monotonic `i64`) for ingestion.
- Each time sources are ticked, the current epoch is advanced by 1 and any
  materialized views update to that version.
- `__mv_version` corresponds to this global epoch.
- Floe tracks a global **event-time watermark** in milliseconds based on the
  latest decoded source timestamps (the first `TIMESTAMP(MILLISECOND)` column in
  each ingested row).
- `__time` reports the current event-time watermark (microseconds since Unix
  epoch) when available. If no event timestamps have been observed yet, it
  falls back to the wall-clock commit time.

## TAIL and SUBSCRIBE Semantics

`TAIL` and `SUBSCRIBE` stream **delta updates** (row-level diffs) for each
version in ascending version order. They share the same snapshot/as-of
semantics and output shape. `SUBSCRIBE` is the preferred pgwire spelling for
new clients; `TAIL` remains supported for compatibility with existing Floe
tools.

Syntax:

```
TAIL <mv_name> [WITH SNAPSHOT] [AS OF <version>]
SUBSCRIBE <mv_name> [WITH SNAPSHOT] [AS OF <version>]
```

Behavior:

- `WITH SNAPSHOT` emits the snapshot for the requested version immediately,
  encoded as inserts (`__op = 1`).
  - If `AS OF` is provided, that exact version is used.
  - Otherwise, the latest available version is used.
- Without `WITH SNAPSHOT`, streaming starts **after** the current version.
- If `AS OF` is provided without `WITH SNAPSHOT`, the stream starts after that
  version.
- Subsequent versions emit **only the delta** (insert/delete rows) rather than
  full snapshots.

Output columns (in order):

1) `__mv_version` (Int64) - version for the emitted snapshot
2) `__op` (Int16) - `1` for inserts, `-1` for deletes (updates appear
   as a delete + insert pair)
3) `__time` (Timestamp, UTC) - event-time watermark for the version (microseconds since Unix epoch), or commit time if no watermark is available
4) User-defined columns from the materialized view

Row order within a version is not guaranteed; versions are emitted in order.

The node CLI exposes both forms:

```bash
floe-node tail --mv mv_orders --with-snapshot
floe-node subscribe --mv mv_orders --with-snapshot
```

The admin HTTP server also exposes the single registered materialized view as
Server-Sent Events:

```bash
curl -N 'http://127.0.0.1:8080/mv?with_snapshot=true'
```

Each SSE message has event type `mv_change` and JSON data containing `mv`,
`version`, `diff`, `time`, and `row`.


## Schema Evolution

- Schema evolution is not supported. Changing source or materialized view schemas
  requires recreating the view and re-ingesting data.

## Restart and Recovery

- Materialized view state and schema are persisted in SlateDB.
- On restart, views can be re-registered and queried without re-ingesting
  historical data.
- The latest persisted `__mv_version` is used as the current version on
  restart.
