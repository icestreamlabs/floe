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

- `CREATE MATERIALIZED VIEW [IF NOT EXISTS] <name> [WITH (<options>)] AS <select>`
- `CREATE TABLE <name> (<columns...>, PRIMARY KEY (...))`
- `CREATE SINK <sink_name> FROM <mv_name> WITH (<options>)`
- `SUBSCRIBE <mv_name> [WITH SNAPSHOT] [AS OF <version>]`
- `COPY (SUBSCRIBE <mv_name> [WITH SNAPSHOT] [AS OF <version>]) TO STDOUT`
- `SELECT ... FROM <materialized_view>` (read-only queries via pgwire)

## SQL Program Parsing

- Floe accepts SQL program text with multiple semicolon-separated statements.
- Statement order is preserved and processed in-order.
- `--mv-query` accepts SQL programs containing:
  - `CREATE TABLE`
  - `CREATE MATERIALIZED VIEW`
  - `CREATE SINK`
- `SUBSCRIBE` and `COPY (SUBSCRIBE ...) TO STDOUT` remain query-time statements
  and are not valid in `--mv-query`.

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

Sink execution behavior:

- Flush occurs when a threshold is hit, on each MV version boundary, and on shutdown.
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
- SQL `ASOF JOIN` is supported for latest-prior lookup joins over Int64 or
  timestamp keys. `FOR SYSTEM_TIME AS OF` compatibility syntax is not yet
  supported.

## Nexmark Support Matrix

The canonical Sprint 0005 Nexmark suite (`q0-q9`, `q12-q22`) is guarded by
`crates/floe-node-core/tests/nexmark_query_coverage.rs`.

Current status:
- All canonical queries pass logical planning, circuit planning, and runtime
  graph validation in the coverage harness.
- SQL `ASOF JOIN` is available in Floe. RisingWave-compatible
  `FOR SYSTEM_TIME AS OF` syntax remains documented as unsupported below.

## Nexmark-Specific Limitations

Unsupported constructs with concrete errors:

- Temporal joins using `FOR SYSTEM_TIME AS OF` syntax
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

1) `floe_version` (Int64) - version for the emitted snapshot
2) `floe_diff` (Int64) - `1` for inserts, `-1` for deletes (updates appear
   as a delete + insert pair)
3) `floe_time` (Timestamp, UTC) - event-time watermark for the version (microseconds since Unix epoch), or commit time if no watermark is available
4) User-defined columns from the materialized view

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
