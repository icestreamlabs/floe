---
layout: default
title: Operations
description: Single-node Floe operations and observability.
permalink: /operations/
---

# Operations

Floe runs as one process. Operational controls focus on one node: pgwire,
admin HTTP, SlateDB state, connector health, watermarks, CDC lag, sink lag, and
materialized-view freshness.

## Admin Server

The admin server is always started unless the process exits during validation.
It binds to the runtime HTTP host, with port `8081` by default. Use
`--admin-port` or `runtime.admin_port` to change the port.

| Endpoint | Method | Purpose |
| --- | --- | --- |
| `/healthz` | GET | Process liveness. |
| `/readyz` | GET | Process, executor, storage, and runtime readiness. |
| `/metrics` | GET | Prometheus metrics. |
| `/debug/watermarks` | GET | Global and per-source event-time watermark state. |
| `/mv` | GET | Server-Sent Events stream for MV changelog output. |
| `/ops/storage/flush` | POST | Force a SlateDB flush and return elapsed time. |
| `/ops/cdc/replication` | GET | CDC source and replication pipeline status. |
| `/ops/cdc/replication/dlq` | GET | List CDC replication DLQ entries. |
| `/ops/cdc/replication/dlq/retry` | POST | Retry a bounded batch of pending DLQ entries. |
| `/ops/cdc/replication/dlq/{pipeline}/{dlq_id}` | GET | Inspect one DLQ entry. |
| `/ops/cdc/replication/dlq/{pipeline}/{dlq_id}/retry` | POST | Retry one DLQ entry. |
| `/ops/cdc/replication/dlq/{pipeline}/{dlq_id}/discard` | POST | Discard one DLQ entry. |
| `/ops/cdc/replication/{pipeline}/reconcile` | POST | Compare a Postgres source table to a Postgres replication target. |

The older `/debug/cdc/replication` and `/debug/cdc/replication/dlq` paths remain
accepted for compatibility.

If HTTP ingest is enabled, the ingest server also exposes `/healthz`,
`/readyz`, `/debug/watermarks`, and `/metrics` alongside `POST /ingest`.

## Materialized-View SSE

The admin HTTP server exposes MV changes as Server-Sent Events:

```bash
curl -N 'http://127.0.0.1:8081/mv?mv=mv_bid&with_snapshot=true'
```

When exactly one MV is registered, `mv` can be omitted. Each SSE message has
event type `mv_change` and JSON data containing `mv`, `version`, `diff`, `time`,
and `row`.

## Key Metrics

| Metric | Purpose |
| --- | --- |
| `floe_ingest_ticks_total` | Ingest tick outcomes. |
| `floe_source_offset_lag` | Source offset lag by source and partition. |
| `floe_mv_freshness_seconds` | Materialized-view freshness. |
| `floe_checkpoint_age_seconds` | Time since latest committed tick checkpoint. |
| `floe_runtime_errors_total` | Runtime error counts by component. |
| `floe_sink_queue_depth` | Queued sink work. |
| `floe_sink_version_lag` | Sink lag by MV version. |
| `floe_sink_failures_total` | Sink delivery failures. |
| `floe_sink_retries_total` | Sink delivery retries. |
| `floe_postgres_cdc_upstream_lsn` | Latest observed upstream Postgres LSN. |
| `floe_postgres_cdc_durable_lsn` | Durable Postgres CDC commit LSN. |
| `floe_postgres_cdc_source_lag_bytes` | Source WAL lag in bytes. |
| `floe_postgres_cdc_source_connected` | Whether the CDC stream is connected. |
| `floe_postgres_cdc_reconnects_total` | CDC reconnect attempts by result. |
| `floe_postgres_cdc_table_lag_bytes` | Per-table CDC lag. |
| `floe_postgres_cdc_schema_evolution_events_total` | Observed CDC schema-evolution events. |
| `floe_postgres_cdc_snapshot_concurrency_target` | Current adaptive snapshot target worker count. |
| `floe_postgres_cdc_snapshot_wal_buffer_fill_percent` | Snapshot WAL handoff buffer fill. |
| `floe_cdc_buffer_pending_records` | Pending records in durable CDC replication buffers. |
| `floe_cdc_buffer_pending_bytes` | Approximate pending CDC buffer payload bytes. |
| `floe_cdc_buffer_source_backpressure_active` | CDC source backpressure from buffer limits. |
| `floe_cdc_replication_replaying` | Whether a replication pipeline is replaying buffered records. |
| `floe_cdc_replication_target_error` | Whether the latest target delivery failed. |
| `floe_cdc_replication_target_write_records_total` | CDC replication records delivered or attempted. |
| `floe_cdc_replication_target_write_latency_ms` | CDC replication target write latency. |
| `floe_cdc_replication_dlq_entries` | DLQ entries by pipeline and status. |

## Persistence

By default Floe uses in-memory SlateDB state. Use `--data-dir` for
filesystem-backed state:

```bash
cargo run -p floe-node -- run \
  --data-dir /var/lib/floe \
  --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

Object-store-backed SlateDB can be configured from environment variables:

```bash
cargo run -p floe-node -- run \
  --object-store-from-env \
  --slatedb-name floe \
  --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

Related config fields:

```toml
[storage]
data_dir = "/var/lib/floe"
await_durable = true
source_journal = "auto"
slatedb_close_timeout_ms = 1000
```

When `object_store_from_env = true`, do not also set `data_dir`.

## SlateDB Tuning

Common CLI knobs:

- `--slatedb-config`
- `--slatedb-env-prefix`
- `--slatedb-flush-interval-ms`
- `--slatedb-l0-sst-bytes`
- `--slatedb-max-wal-flushes-before-l0-flush`
- `--slatedb-l0-max-ssts`
- `--slatedb-l0-max-ssts-per-key`
- `--slatedb-max-unflushed-bytes`
- `--slatedb-compaction-max-sst-bytes`
- `--slatedb-compaction-max-concurrent`
- `--slatedb-await-durable`
- `--slatedb-cache-dir`
- `--slatedb-cache-max-bytes`
- `--slatedb-cache-part-bytes`
- `--slatedb-cache-puts`
- `--slatedb-cache-max-open-file-handles`
- `--slatedb-close-timeout-ms`

Lower flush intervals can reduce commit latency and recovery time at the cost
of more writes. Larger compaction targets reduce compaction overhead but can
increase read amplification. Enabling the object-store cache improves read-heavy
paths at the cost of local disk usage.

## Runtime State Retention

Common MV/ZSet knobs:

- `--mv-retain-last`
- `--zset-compaction-max-chain-len`
- `--zset-compaction-max-segments`
- `--zset-compaction-backoff-ticks`
- `--zset-compaction-max-concurrent-jobs`
- `--zset-gc-grace-period-ms`

Maintenance startup flags:

- `--maintenance-paused`
- `--maintenance-inspect-namespace`
- `--maintenance-compact-namespace`
- `--maintenance-gc-namespace`

Equivalent config sections are `[runtime]`, `[storage]`, and `[maintenance]`.

## CDC Operations

`GET /ops/cdc/replication` reports:

- Postgres source connection and reconnect state.
- Source and per-table WAL lag.
- Durable buffer occupancy and backpressure state.
- Target state, replay state, and latest target error.
- DLQ summary by pipeline.

DLQ operations:

```bash
curl 'http://127.0.0.1:8081/ops/cdc/replication/dlq?pipeline=orders_pipe&status=pending&limit=100&offset=0'

curl 'http://127.0.0.1:8081/ops/cdc/replication/dlq/orders_pipe/entry-1'

curl -X POST 'http://127.0.0.1:8081/ops/cdc/replication/dlq/orders_pipe/entry-1/retry' \
  -H 'content-type: application/json' \
  -d '{"reason":"target recovered","operator":"ops"}'

curl -X POST 'http://127.0.0.1:8081/ops/cdc/replication/dlq/orders_pipe/entry-1/discard' \
  -H 'content-type: application/json' \
  -d '{"reason":"duplicate downstream row","operator":"ops"}'
```

Reconcile a Postgres replication target:

```bash
curl -X POST 'http://127.0.0.1:8081/ops/cdc/replication/orders_pipe/reconcile?max_rows=100000'
```

Reconciliation reports `ok`, `drift`, `bounded`,
`pending_target_delivery`, or `unsupported_target`. It does not run an unbounded
`COUNT(*)` unless `full_scan=true` is passed explicitly.

Operator output avoids connector connection strings and does not return raw DLQ
payload bytes. DLQ metadata can include payload object keys, formats, and byte
counts for audit and recovery workflows.

An `floe-node ops` CLI wrapper is planned; HTTP endpoints are the operator API
today.
