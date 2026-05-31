---
layout: default
title: Support Matrix
description: Current Floe feature support and limits.
permalink: /support/
---

# Support matrix

This page summarizes what Floe supports today and where current limits apply.
Floe is a single-node system; distributed runtime, cluster scheduling, and
multi-node architecture are not planned.

## Supported today

| Area | Support |
| --- | --- |
| Runtime | Single-node DBSP runtime with vectorized execution as the default. |
| Planning | DataFusion logical planning with DBSP plan validation. |
| Storage | SlateDB-backed catalog, materialized-view state, checkpoints, and CDC buffers. |
| SQL programs | Startup SQL with `CREATE SOURCE`, `CREATE TABLE`, `CREATE MATERIALIZED VIEW`, `CREATE SINK`, and `CREATE REPLICATION PIPELINE`. |
| Query endpoint | pgwire `SELECT` over materialized views. |
| Changelog endpoint | `SUBSCRIBE` for pgwire clients and `COPY (SUBSCRIBE ...) TO STDOUT` for psql. |
| Ingest | Nexmark generator, file, Kafka, HTTP, object store, and native Postgres CDC. |
| Sinks | Kafka, file, HTTP, and Postgres materialized-view sinks. |
| CDC replication | Postgres CDC replication pipelines into Kafka or Postgres. |
| Observability | `/healthz`, `/readyz`, `/metrics`, tracing fields, and CDC ops endpoints. |

## Available with limits

| Area | Status |
| --- | --- |
| Arrow IPC CDC pipeline output | Available for evaluation. Encoding details may change before stable support. |
| Postgres CDC schema evolution | Compatible nullable-column additions are supported. Incompatible changes fail closed. |
| Runtime tuning | SlateDB, batch, sink, and compaction knobs are available. Defaults and validation coverage will continue to improve. |

## Planned

| Area | Target |
| --- | --- |
| Runtime SQL DDL over pgwire | Planned next. Startup SQL/config is supported today. |
| Broader psql introspection and EXPLAIN | Planned next. |
| Auth, TLS, backup/restore, and upgrade validation | Planned for the first stable release. |
| `FOR SYSTEM_TIME AS OF` compatibility syntax and advanced window semantics | Planned after the first stable release. |
| Remaining Postgres type families | Planned after the first stable release. |
| User-defined functions | Planned later. |
| Full time travel | Planned later. |

## Current limits

- pgwire currently serves queries and changelog streams for materialized views;
  runtime DDL over pgwire is planned.
- Postgres CDC requires primary-key metadata for update/delete and target upsert semantics.
- Arrays, enums/domains, intervals, and range types are not currently supported.
- Source or materialized-view schema changes outside the documented CDC-compatible cases require recreating the view and re-ingesting data.
- Automatic failover discovery is not currently available. Use stable DNS/proxy
  endpoints for Postgres source and sink failover.

## Validation checklist

Changes should pass at least:

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace --no-run
git diff --check
```

Feature-specific changes should also run the targeted validation commands for
the affected area.
