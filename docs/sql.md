# SQL Support and Operational Semantics

This document describes the SQL subset Floe supports today and the runtime
semantics for materialized views and TAIL.

## Supported Statements

- `CREATE MATERIALIZED VIEW <name> AS <select>`
- `TAIL <mv_name> [WITH SNAPSHOT] [AS OF <version>]`
- `SELECT ... FROM <materialized_view>` (read-only queries via pgwire)

## Materialized View Definition Rules

- Exactly one statement; multiple statements are rejected.
- `WITH` clauses are rejected in materialized view definitions.
- The logical plan must compile to Source/Select/Project/Join/Sink nodes only.
  Queries that introduce aggregates, window aggregates, `TOP N`/`LIMIT`,
  `UNION`, or passthrough nodes are rejected.
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

## Version and Time Semantics

- Floe uses a single global logical epoch (monotonic `i64`) for ingestion.
- Each time sources are ticked, the current epoch is advanced by 1 and any
  materialized views update to that version.
- `__mv_version` corresponds to this global epoch.

## TAIL Semantics

`TAIL` streams **full snapshots** of the materialized view at each version
in ascending version order.

Syntax:

```
TAIL <mv_name> [WITH SNAPSHOT] [AS OF <version>]
```

Behavior:

- `WITH SNAPSHOT` emits the snapshot for the requested version immediately.
  - If `AS OF` is provided, that exact version is used.
  - Otherwise, the latest available version is used.
- Without `WITH SNAPSHOT`, streaming starts **after** the current version.
- If `AS OF` is provided without `WITH SNAPSHOT`, the stream starts after that
  version.

Output columns (in order):

1) `__mv_version` (Int64) - version for the emitted snapshot
2) `__op` (Int16) - currently always `1` (full snapshot rows)
3) `__time` (Timestamp, UTC) - currently `NULL`
4) User-defined columns from the materialized view

Row order within a version is not guaranteed; versions are emitted in order.

## Restart and Recovery

- Materialized view state and schema are persisted in SlateDB.
- On restart, views can be re-registered and queried without re-ingesting
  historical data.
- The latest persisted `__mv_version` is used as the current version on
  restart.
