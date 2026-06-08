---
layout: default
title: Roadmap
description: Floe release roadmap.
permalink: /roadmap/
---

# Roadmap

Floe will remain a single-node streaming SQL engine. The roadmap intentionally
excludes distributed runtime, cluster scheduling, and multi-node architecture.

## Current main

Current `main` includes:

- Single-node runtime with DataFusion planning and vectorized DBSP execution.
- One materialized view per process, served over pgwire and admin HTTP SSE.
- `COPY (SUBSCRIBE ...) TO STDOUT` changelog streaming.
- Config-first startup with TOML/YAML/JSON.
- Generator, file, HTTP, Kafka, object-store, and native Postgres CDC inputs.
- Kafka, file, HTTP, and Postgres MV sinks.
- CDC replication pipelines to Kafka and Postgres with durable buffers and DLQ operations.
- Admin health, readiness, metrics, watermarks, CDC operations, reconcile, and storage flush endpoints.

## Near term

- Keep hosted docs, quickstart examples, and validation checklist current.
- Broaden end-to-end client regression coverage for `COPY (SUBSCRIBE ...) TO STDOUT`.
- Improve config examples for Postgres CDC and sink-heavy deployments.

Milestone: [Alpha](https://github.com/icestreamlabs/floe/milestone/1)

## Beta

- Runtime SQL DDL over pgwire.
- Broader source, sink, replication, and EXPLAIN introspection.
- `floe-node ops` CLI for single-node admin workflows.
- Client compatibility matrix for psql, libpq, JDBC, and tokio-postgres.
- Expanded Postgres CDC failover and recovery runbooks.

Milestone: [Beta](https://github.com/icestreamlabs/floe/milestone/2)

## 1.0 GA

- pgwire authentication, TLS, and production credential handling.
- Single-node backup, restore, and upgrade validation.
- Online schema and catalog evolution contracts.
- Release validation for freshness, throughput, recovery, and vectorization.
- Stable compatibility contracts for sink formats and CDC replication formats.

Milestone: [1.0 GA](https://github.com/icestreamlabs/floe/milestone/3)

## 2.0

- `FOR SYSTEM_TIME AS OF` compatibility syntax and advanced window semantics.
- Broader data type coverage and stable Arrow IPC format support.
- Single-node resource governance and admission control.

Milestone: [2.0](https://github.com/icestreamlabs/floe/milestone/4)

## 3.0

- User-defined functions.
- Full time travel and historical query semantics.

Milestone: [3.0](https://github.com/icestreamlabs/floe/milestone/5)
