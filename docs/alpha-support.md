---
layout: default
title: Alpha Support
description: Floe alpha support matrix and known limitations.
permalink: /alpha-support/
---

# Alpha support

This matrix describes the intended alpha contract for Floe. Floe is a single-node system; distributed runtime, cluster scheduling, and multi-node architecture are not part of the roadmap.

## Supported

| Area | Alpha support |
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

## Experimental

| Area | Status |
| --- | --- |
| Arrow IPC CDC pipeline output | Available for internal experimentation; not yet a public compatibility claim. |
| Postgres CDC schema evolution | Limited policies exist for compatible additions. Incompatible changes fail closed. |
| Runtime tuning | SlateDB, batch, sink, and compaction knobs exist, but release gates are still being formalized. |

## Deferred

| Area | Target |
| --- | --- |
| Runtime SQL DDL over pgwire | Beta. Startup SQL/config is supported today. |
| Broader psql introspection and EXPLAIN | Beta. |
| Auth, TLS, backup/restore, and upgrade validation | 1.0 GA. |
| `FOR SYSTEM_TIME AS OF` compatibility syntax and advanced window semantics | 2.0. |
| Remaining Postgres type families | 2.0. |
| User-defined functions | 3.0. |
| Full time travel | 3.0. |

## Known limitations

- pgwire is currently a query/read endpoint for materialized views; runtime DDL is a Beta item.
- Postgres CDC requires primary-key metadata for update/delete and target upsert semantics.
- Arrays, enums/domains, intervals, and range types remain deferred.
- Source or materialized-view schema changes outside the documented CDC-compatible cases require recreating the view and re-ingesting data.
- Full HA discovery is not part of alpha. Use stable DNS/proxy endpoints for Postgres source and sink failover.

## Release checklist

Alpha release candidates should at minimum pass:

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace --no-run
git diff --check
```

Feature-specific changes should also run the targeted commands listed in the repository `AGENTS.md`.
