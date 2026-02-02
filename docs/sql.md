# SQL Support and Operational Semantics

This document describes the SQL subset Floe supports today and the runtime
semantics for materialized views and TAIL.

## Supported Statements

- `CREATE MATERIALIZED VIEW [IF NOT EXISTS] <name> [WITH (<options>)] AS <select>`
- `TAIL <mv_name> [WITH SNAPSHOT] [AS OF <version>]`
- `SELECT ... FROM <materialized_view>` (read-only queries via pgwire)

## Materialized View Definition Rules

- Exactly one statement; multiple statements are rejected.
- `WITH` clauses are accepted in materialized view definitions, but the
  options are currently ignored.
- Identifiers can be double-quoted to preserve case or include special
  characters (`"MyView"`).
- The logical plan must compile to supported nodes (for example:
  Source/Select/Project/Join/Aggregate/WindowAggregate/TopN/Union/Passthrough/Sink).
  Unsupported nodes are rejected at plan validation time.
- Joins are **inner equi-joins** on column references only.
  Non-column join expressions are rejected.
- NULL semantics:
  - `NULL = NULL` evaluates to unknown and is treated as `false` in `WHERE`.
  - Join keys containing NULL do not match.
- Supported scalar types in expressions: `INT64`, `BOOL`, `UTF8`, and
  `TIMESTAMP(MILLISECOND)`.
- `LIKE` only supports a single prefix or suffix `%` wildcard (no substring).

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

## TAIL Semantics

`TAIL` streams **delta updates** (row-level diffs) for each version
in ascending version order.

Syntax:

```
TAIL <mv_name> [WITH SNAPSHOT] [AS OF <version>]
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


## Schema Evolution

- Schema evolution is not supported. Changing source or materialized view schemas
  requires recreating the view and re-ingesting data.

## Restart and Recovery

- Materialized view state and schema are persisted in SlateDB.
- On restart, views can be re-registered and queried without re-ingesting
  historical data.
- The latest persisted `__mv_version` is used as the current version on
  restart.
