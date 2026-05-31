---
layout: default
title: Operations
description: Single-node Floe operations and observability.
permalink: /operations/
---

# Operations

Floe runs as one process. Operational controls focus on one node: pgwire, admin HTTP, SlateDB state, connector health, CDC lag, sink lag, and materialized-view freshness.

## Admin endpoints

| Endpoint | Purpose |
| --- | --- |
| `/healthz` | Process liveness. |
| `/readyz` | Process, executor, storage, and runtime readiness. |
| `/metrics` | Prometheus metrics. |
| `/ops/cdc/replication` | CDC replication pipeline status, source lag, target state, durable buffer stats, and DLQ summary. |
| `/ops/cdc/replication/dlq` | DLQ list, inspect, retry, batch retry, and discard workflows. |

## Key metrics

| Metric | Purpose |
| --- | --- |
| `floe_ingest_ticks_total` | Ingest tick outcomes. |
| `floe_source_offset_lag` | Source offset lag by source and partition. |
| `floe_mv_freshness_seconds` | Materialized-view freshness. |
| `floe_checkpoint_age_seconds` | Time since latest committed tick checkpoint. |
| `floe_postgres_cdc_source_lag_bytes` | Postgres source WAL lag. |
| `floe_postgres_cdc_table_lag_bytes` | Postgres CDC table lag. |
| `floe_cdc_buffer_pending_records` | Pending CDC buffer records. |
| `floe_sink_queue_depth` | Queued sink work. |
| `floe_sink_version_lag` | Sink lag by MV version. |
| `floe_runtime_errors_total` | Runtime error counts by component. |

## Persistence

By default Floe uses in-memory SlateDB state. Use `--data-dir` for filesystem-backed state:

```bash
cargo run -- run \
  --data-dir /var/lib/floe \
  --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

Object-store-backed SlateDB can be configured from environment variables:

```bash
cargo run -- run \
  --object-store-from-env \
  --slatedb-name floe \
  --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

## SlateDB tuning

Common knobs:

- `--slatedb-flush-interval-ms`
- `--slatedb-l0-sst-bytes`
- `--slatedb-compaction-max-sst-bytes`
- `--slatedb-compaction-max-concurrent`
- `--slatedb-await-durable`
- `--slatedb-cache-dir`
- `--slatedb-cache-max-bytes`

Lower flush intervals can reduce commit latency and recovery time at the cost of more writes. Larger compaction targets reduce compaction overhead but can increase read amplification.

## CDC operations

Postgres CDC operator endpoints report:

- source connection and reconnect state
- source and table lag
- durable buffer occupancy
- target state
- DLQ entry counts and retry/discard state

The current Alpha API is HTTP-first. A `floe-node ops` CLI wrapper is tracked for Beta.
