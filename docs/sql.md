---
layout: default
title: SQL Reference
description: Floe SQL support and streaming semantics.
permalink: /sql/
---

# SQL Support and Operational Semantics

This document describes the SQL subset Floe supports today and the runtime
semantics for materialized views and SUBSCRIBE.

## Supported Statements

- `CREATE SOURCE <name> WITH (<options>)`
- `CREATE MATERIALIZED VIEW [IF NOT EXISTS] <name> [WITH (<options>)] AS <select>`
- `CREATE TABLE <name> (<columns...>, PRIMARY KEY (...))`
- `CREATE TABLE <name> (<columns...>) FROM <source> TABLE '<schema.table>'`
- `CREATE SINK <sink_name> FROM <mv_name> WITH (<options>)`
- `CREATE REPLICATION PIPELINE <name> FROM <source> TABLE '<schema.table>' INTO <target> WITH (<options>)`
- `SUBSCRIBE <mv_name> [WITH SNAPSHOT] [AS OF <version>]`
- `COPY (SUBSCRIBE <mv_name> [WITH SNAPSHOT] [AS OF <version>]) TO STDOUT`
- `SELECT ... FROM <materialized_view>` (read-only queries via pgwire)

## SQL Program Parsing

- Floe accepts SQL program text with multiple semicolon-separated statements.
- Statement order is preserved and processed in-order.
- `--mv-query` accepts SQL programs containing:
  - `CREATE SOURCE`
  - `CREATE TABLE`
  - `CREATE MATERIALIZED VIEW`
  - `CREATE SINK`
  - `CREATE REPLICATION PIPELINE`
- `SUBSCRIBE` and `COPY (SUBSCRIBE ...) TO STDOUT` remain query-time statements
  and are not valid in `--mv-query`.
- Config-file `materialized_views[].query` values contain the SELECT query body,
  not the full `CREATE MATERIALIZED VIEW` statement.
- The runtime currently accepts at most one materialized view per process.

## CREATE SOURCE

`CREATE SOURCE` currently supports the native Postgres CDC connector:

```sql
CREATE SOURCE pg_main WITH (
  connector = 'postgres-cdc',
  connection = 'postgres://postgres:postgres@localhost/postgres',
  slot.name = 'floe_slot',
  publication.name = 'floe_pub',
  include_schema_in_source = true,
  schema.evolution = 'ignore-compatible',
  slot.create = false,
  publication.create = true
);
```

Connection options can also be supplied as parts:

```sql
CREATE SOURCE pg_main WITH (
  type = 'postgres_cdc',
  hostname = 'localhost',
  port = '5432',
  username = 'postgres',
  password = 'postgres',
  database.name = 'postgres',
  slot = 'floe_slot'
);
```

Supported Postgres CDC source options:

- `connection`, `connection_string`, `dsn`, or `url`
- `hostname`/`host`, `port`, `username`/`user`, `password`, `database.name`/`database`/`dbname`
- `slot.name` or `slot`
- `publication.name` or `publication`
- `include_schema_in_source`
- `schema.evolution`: `fail_fast`, `ignore_compatible`, or `apply_compatible_additions`
- `slot.create`, `slot.auto_create`, or `auto_create_slot`
- `publication.create`, `publication.auto_create`, or `auto_create_publication`

Slot and publication auto-creation default to `true`.

## CREATE TABLE

Tables must declare at least one column and exactly one primary key column:

```sql
CREATE TABLE bids (
  id BIGINT PRIMARY KEY,
  price NUMERIC(15,2) NOT NULL,
  channel TEXT,
  shipdate DATE
);
```

Source-backed tables bind a Floe table to an upstream CDC table:

```sql
CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  amount BIGINT NOT NULL,
  status TEXT
) FROM pg_main TABLE 'public.orders';
```

Supported table column types:

- Integer: `INT`, `INTEGER`, `BIGINT`, `INT8`, `INT64`
- Boolean: `BOOL`, `BOOLEAN`
- Text: `TEXT`, `VARCHAR`, `CHAR`, `CHARACTER`, `STRING`
- Time: `TIMESTAMP`, `DATETIME`, `TIMESTAMP_NTZ`
- Date: `DATE`, `DATE32`
- Numeric: `NUMERIC`, `NUMERIC(p)`, `NUMERIC(p,s)`, `DECIMAL` aliases up to Decimal128 precision 38

Unsupported `CREATE TABLE` forms include `IF NOT EXISTS`, CTAS, `LIKE`, and
`CLONE`.

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
  `TIMESTAMP(MILLISECOND)`. Table declarations also cover date and exact
  numeric types as listed above.
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

- Kafka: `brokers`, `topic`, optional `format`, `key_columns`
- File: `path`, optional `append`
- HTTP: `url`, optional `batch_size`
- Postgres: `connection`, `table`, optional `mode`, `primary_key`

Common changelog options:

- `with_snapshot` (bool)
- `as_of` (int64)

Reliability options:

- `batch_rows` (flush threshold by row count)
- `batch_bytes` (flush threshold by serialized payload bytes)
- `queue_capacity` (bounded sink queue size)
- `retry_max_attempts` (Kafka/HTTP/Postgres)
- `retry_base_ms` (Kafka/HTTP/Postgres)
- `retry_max_backoff_ms` (Kafka/HTTP/Postgres)
- `transactional_id`, `checkpoint_topic`, `checkpoint_partition` (Kafka config-file sinks)

Kafka sink formats are `json` and `debezium_json`. Debezium Kafka sinks require
`key_columns`. Postgres sink modes are `upsert` and `append_only`; `upsert`
requires `primary_key`.

Sink execution behavior:

- Flush occurs when a threshold is hit, on each MV version boundary, and on shutdown.
- Postgres applies each MV version in one transaction using temporary COPY
  staging tables, then bulk delete/upsert or append SQL.
- Kafka/HTTP emission retries use bounded exponential backoff.
- On permanent Kafka/HTTP failures, sink execution stops and the sink task exits with error.
- Bounded sink queues apply backpressure when consumers fall behind.

## CREATE REPLICATION PIPELINE

Replication pipelines forward native CDC changes from a Postgres CDC source to a
target without going through an MV changelog:

```sql
CREATE REPLICATION PIPELINE pg_orders_to_kafka
FROM pg_main TABLE 'public.orders'
INTO KAFKA WITH (
  brokers = 'localhost:9092',
  topic = 'orders_cdc',
  format = 'debezium-json',
  durable_buffer = true,
  buffer.max_pending_bytes = 1048576,
  buffer.max_pending_records = 100000,
  buffer.max_pending_objects = 64,
  buffer.max_pending_age_ms = 60000,
  tombstones = true,
  transaction_metadata = true,
  error.policy = 'dead-letter-and-continue',
  error.max_retries = 3
);
```

Postgres targets are also supported:

```sql
CREATE REPLICATION PIPELINE pg_orders_to_postgres
FROM pg_main TABLE public.orders
INTO POSTGRES WITH (
  connection = 'postgres://postgres:postgres@localhost/postgres',
  table = 'public.orders_copy'
);
```

Supported replication options:

- Targets: `KAFKA` with `brokers` and `topic`; `POSTGRES` with `connection` and `table`
- Formats: `floe-json`/`compact_json`, `debezium-json`, `arrow-ipc`
- Buffering: `durable_buffer` defaults to `true`; set it to `false` for no buffer
- Buffer caps: `buffer.max_pending_bytes`, `buffer.max_pending_records`,
  `buffer.max_pending_transactions`/`buffer.max_pending_objects`,
  `buffer.max_pending_age_ms`
- Deletion metadata: `emit_tombstones`, `tombstones`, or `delete.tombstones`
- Transaction metadata: `include_transaction_metadata` or `transaction_metadata`
- Error policy: `error.policy`/`error_policy` and `error.max_retries`/`error_max_retries`

The default format is `floe_json`, the default buffer mode is durable, and the
default error policy is retry-with-backoff.

## Materialized View Query Semantics

- Materialized views are read-only over pgwire. Runtime query statements such
  as `INSERT` and `CREATE TABLE` are rejected by the pgwire endpoint.
- Every materialized view exposes a reserved column `__mv_version` (Int64).
  Use it to query a point-in-time snapshot:
  - `SELECT ... FROM mv WHERE __mv_version = 42`
  - Only equality filters on `__mv_version` are recognized for as-of reads.

## Window Semantics

- `TUMBLE` / `HOP` assign rows to windows using the event-time expression in the
  window spec.
- Rows with event-time earlier than `(watermark - allowed_lateness)` are
  dropped. The SQL planner currently defaults `allowed_lateness` to `0`.
- SQL `ASOF JOIN` is supported for latest-prior lookup joins over Int64 or
  timestamp keys. `FOR SYSTEM_TIME AS OF` compatibility syntax is not currently
  supported.

## Nexmark Support Matrix

The canonical Sprint 0005 Nexmark suite (`q0-q9`, `q12-q22`) is guarded by
`crates/floe-node-core/tests/nexmark_query_coverage.rs`.

Current status:
- All canonical queries pass logical planning, circuit planning, and runtime
  graph validation in the coverage harness.
- SQL `ASOF JOIN` is available in Floe. RisingWave-compatible
  `FOR SYSTEM_TIME AS OF` syntax is listed below.

## Nexmark-Specific Limits

Current limits with concrete errors:

- Temporal joins using `FOR SYSTEM_TIME AS OF` syntax
  - Example parser error:
    - `failed to parse materialized view statement: sql parser error: Expected: one of UPDATE or SHARE, found: SYSTEM_TIME`
- `DATE_FORMAT` currently covers the tokens required
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

## SUBSCRIBE Semantics

`SUBSCRIBE` streams **delta updates** (row-level diffs) for each version in
ascending version order.

Syntax:

```
SUBSCRIBE <mv_name> [WITH SNAPSHOT] [AS OF <version>]
COPY (SUBSCRIBE <mv_name> [WITH SNAPSHOT] [AS OF <version>]) TO STDOUT
```

Behavior:

- `WITH SNAPSHOT` emits the snapshot for the requested version immediately,
  encoded as inserts (`floe_diff = 1`).
  - If `AS OF` is provided, that exact version is used.
  - Otherwise, the latest available version is used.
- Without `WITH SNAPSHOT`, streaming starts **after** the current version.
- If `AS OF` is provided without `WITH SNAPSHOT`, the stream starts after that
  version.
- Subsequent versions emit **only the delta** (insert/delete rows) rather than
  full snapshots.

Output columns (in order):

1. `floe_version` (Int64) - version for the emitted snapshot
2. `floe_diff` (Int64) - `1` for inserts, `-1` for deletes (updates appear
   as a delete + insert pair)
3. `floe_time` (Timestamp, UTC) - event-time watermark for the version
   (microseconds since Unix epoch), or commit time if no watermark is available
4. User-defined columns from the materialized view

Row order within a version is not guaranteed; versions are emitted in order.

Bare `SUBSCRIBE` streams pgwire data rows for client libraries that consume an
unbounded query result directly. Interactive `psql` should use the PostgreSQL
COPY protocol form so rows are emitted as they arrive:

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -c "COPY (SUBSCRIBE mv_orders WITH SNAPSHOT) TO STDOUT"
```

The COPY form emits the same columns in PostgreSQL text COPY format: tab
separated fields, `\N` for NULLs, and standard COPY text escapes.

The admin HTTP server also exposes the single registered materialized view as
Server-Sent Events:

```bash
curl -N 'http://127.0.0.1:8081/mv?with_snapshot=true'
```

Each SSE message has event type `mv_change` and JSON data containing `mv`,
`version`, `diff`, `time`, and `row`.


## Schema Evolution

- General materialized-view schema evolution is not supported. Changing source
  or materialized-view schemas usually requires recreating the view and
  re-ingesting data.
- Postgres CDC source schema evolution can be configured with
  `schema.evolution` / `schema_evolution_policy`:
  - `fail_fast` rejects observed schema changes.
  - `ignore_compatible` and `apply_compatible_additions` allow nullable,
    non-key columns appended to the upstream table while Floe continues using
    the catalog schema.
  - Drop, reorder, type, primary-key, and replica-identity changes fail closed.

## Restart and Recovery

- Materialized view state and schema are persisted in SlateDB.
- On restart, views can be re-registered and queried without re-ingesting
  historical data.
- The latest persisted `__mv_version` is used as the current version on
  restart.
- Durable CDC replication buffers and checkpoints are stored in SlateDB when
  durable buffering is enabled.
