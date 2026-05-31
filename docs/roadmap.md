---
layout: default
title: Roadmap
description: Floe release roadmap.
permalink: /roadmap/
---

# Roadmap

Floe will remain a single-node streaming SQL engine. The roadmap intentionally
excludes distributed runtime, cluster scheduling, and multi-node architecture.

## Alpha

- Keep the public docs, quickstart, and validation checklist current.
- Add automated psql `COPY (SUBSCRIBE ...) TO STDOUT` regression coverage.

Milestone: [Alpha](https://github.com/icestreamlabs/floe/milestone/1)

## Beta

- Runtime SQL DDL over pgwire.
- Source, sink, replication, and EXPLAIN introspection.
- `floe-node ops` CLI for single-node admin workflows.
- Client compatibility matrix for psql, libpq, JDBC, and tokio-postgres.

Milestone: [Beta](https://github.com/icestreamlabs/floe/milestone/2)

## 1.0 GA

- pgwire authentication, TLS, and production credential handling.
- Single-node backup, restore, and upgrade validation.
- Online schema and catalog evolution contracts.
- Release validation for freshness, throughput, recovery, and vectorization.

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
