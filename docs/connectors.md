---
layout: default
title: Connectors and Sinks
description: Floe connectors, sink behavior, and delivery settings.
permalink: /connectors/
---

# Connectors and Sinks

Floe can run with CLI-created connectors or a config file. CLI startup is useful
for quick local runs. Config files are the preferred path for multiple
connectors, sinks, Postgres CDC, and production-style settings.

Supported input connectors:

- `generator`: built-in Nexmark `nexmark_person`, `nexmark_auction`, and `nexmark_bid` data.
- `file`: newline-delimited JSON from a local file or stdin.
- `http`: `POST /ingest` JSON ingestion.
- `kafka`: JSON or Debezium JSON messages from Kafka topics.
- `object_store`: newline-delimited JSON from an object-store prefix.
- `postgres_cdc`: native logical replication with the `pgoutput` plugin.

Supported sinks:

- `kafka`: MV changelog rows to a Kafka topic.
- `file`: MV changelog rows to JSONL.
- `http`: MV changelog batches to an HTTP endpoint.
- `postgres`: MV changelog rows into a Postgres target table.

Postgres CDC replication pipelines can also forward source table changes
directly to Kafka or Postgres without materializing an MV changelog first.

## Configuration Files

`--config` accepts TOML, YAML, and JSON. Unknown fields are rejected.

```toml
[[connectors]]
type = "kafka"
name = "bids"
brokers = "localhost:9092"
topics = ["nexmark_bid"]
group_id = "floe"
default_source = "nexmark_bid"
poll_ms = 100
max_messages_per_tick = 256
format = "floe_json"

[[materialized_views]]
name = "mv_bid"
query = "SELECT auction, bidder, price FROM nexmark_bid WHERE price >= 100"

[[sinks]]
type = "file"
name = "mv_bid_file"
mv = "mv_bid"
path = "/tmp/mv_bid.jsonl"
with_snapshot = true

[runtime]
pgwire_addr = "127.0.0.1:6432"
admin_port = 8081
ingest_batch_size = 256
mv_retain_last = 1

[storage]
data_dir = "/tmp/floe-data"
source_journal = "auto"
```

Precedence rules:

- If `--config` is present, connector creation flags such as `--http-port`,
  `--kafka-brokers`, `--kafka-topics`, and `--input-file` are ignored.
- Runtime and storage flags still apply after config defaults.
- Existing persisted catalog definitions load first.
- Config connectors, materialized views, and sinks load next.
- SQL definitions from `--mv-query` are applied after config parsing.

`--mv-query` can also define Postgres CDC sources, source-backed tables, sinks,
and replication pipelines with SQL at startup. See the [SQL reference]({{ site.baseurl }}/sql/)
for the full startup-SQL boundary. Runtime DDL over pgwire is not supported yet.

## Append JSON Payloads

File and HTTP ingest use Floe JSON objects. A payload can be wrapped:

```json
{"source":"nexmark_bid","data":{"auction":1,"bidder":42,"price":100,"channel":"web","url":"u","date_time":0,"extra":""}}
```

Or it can be an unwrapped row when a default source is configured:

```json
{"auction":1,"bidder":42,"price":100,"channel":"web","url":"u","date_time":0,"extra":""}
```

Kafka `floe_json` values can be a wrapped object, an unwrapped row, or an array
of either form. If an unwrapped Kafka row has no configured `default_source`,
Floe uses the Kafka topic name as the source.

The Kafka connector applies low-latency fetch settings:

- `fetch.wait.max.ms = 1`
- `fetch.queue.backoff.ms = 1`
- `fetch.min.bytes = 1`
- `enable.auto.offset.store = false`

## Connector Reference

### Generator

CLI:

```bash
cargo run -p floe-node -- run \
  --events-per-second 100 \
  --max-events 10000 \
  --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

Config:

```toml
[[connectors]]
type = "generator"
events_per_second = 100.0
max_events = 10000
```

### File

CLI:

```bash
cargo run -p floe-node -- run \
  --input-file /path/to/events.jsonl \
  --input-source nexmark_bid \
  --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

Config:

```toml
[[connectors]]
type = "file"
path = "/path/to/events.jsonl"
default_source = "nexmark_bid"
```

Use `-` as the path to read from stdin.

### HTTP

CLI:

```bash
cargo run -p floe-node -- run \
  --http-port 8080 \
  --http-source nexmark_bid \
  --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

Config:

```toml
[[connectors]]
type = "http"
host = "127.0.0.1"
port = 8080
default_source = "nexmark_bid"
```

Requests go to `POST /ingest`. Optional query parameter `source` overrides the
configured default source:

```bash
curl -X POST 'http://127.0.0.1:8080/ingest?source=nexmark_bid' \
  -H 'content-type: application/json' \
  -d '{"auction":1,"bidder":42,"price":100,"channel":"web","url":"u","date_time":0,"extra":""}'
```

### Kafka

CLI:

```bash
cargo run -p floe-node -- run \
  --kafka-brokers localhost:9092 \
  --kafka-topics nexmark_bid \
  --kafka-default-source nexmark_bid \
  --kafka-poll-ms 100 \
  --kafka-max-messages 256 \
  --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

Config:

```toml
[[connectors]]
type = "kafka"
brokers = "localhost:9092"
topics = ["nexmark_bid"]
group_id = "floe"
default_source = "nexmark_bid"
poll_ms = 100
max_messages_per_tick = 256
format = "floe_json"
```

Kafka input formats:

- `floe_json`: Floe wrapped/unwrapped JSON. This is the default.
- `debezium_json`: Debezium-style change envelopes.

### Object Store

The object-store connector reads newline-delimited JSON under a URL prefix such
as `s3://bucket/prefix/`.

```toml
[[connectors]]
type = "object_store"
url = "s3://my-bucket/events/nexmark/"
default_source = "nexmark_bid"
```

### Postgres CDC

The Postgres CDC connector uses native logical replication with `pgoutput`.
It can auto-create the publication and slot when the configured user has enough
privileges.

```toml
[[connectors]]
type = "postgres_cdc"
name = "pg_main"
connection = "postgres://user:password@localhost:5432/db"
slot = "floe_slot"
publication = "floe_publication"
include_tables = ["public.orders", "public.customers"]
include_schema_in_source = true
schema_evolution_policy = "ignore_compatible"
auto_create_slot = true
auto_create_publication = true
```

CDC-backed tables are declared in SQL:

```sql
CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  amount BIGINT NOT NULL,
  status TEXT
) FROM pg_main TABLE 'public.orders';
```

Postgres CDC settings:

```toml
[postgres_cdc.snapshot]
rows_per_batch = 16384
max_workers = 1
intra_table_chunks = 1
adaptive_concurrency = true
min_workers = 1
wal_buffer_high_watermark_percent = 75
wal_buffer_low_watermark_percent = 25
slow_scan_ms = 30000
controller_interval_ms = 500

[postgres_cdc.reconnect]
max_reconnects = 10
retry_base_ms = 1000
retry_max_backoff_ms = 30000
```

Postgres CDC limits:

- CDC tables need primary-key metadata for updates, deletes, and upsert targets.
- Common scalar types are covered; arrays, enums/domains, intervals, and range
  types are not currently supported.
- Compatible appended nullable non-key columns can be ignored or applied
  according to `schema_evolution_policy`; incompatible changes fail closed.
- Automatic failover discovery is not available. Use stable DNS/proxy endpoints
  and compatible logical slots/publications after promotion.

## Sink Reference

Sinks consume MV changelog output. Common changelog options are `with_snapshot`
and `as_of`.

Reliability and throughput options:

- `batch_rows`
- `batch_bytes`
- `queue_capacity`
- `retry_max_attempts`
- `retry_base_ms`
- `retry_max_backoff_ms`

Kafka sinks:

```toml
[[sinks]]
type = "kafka"
name = "mv_bid_kafka"
brokers = "localhost:9092"
topic = "mv_bid"
mv = "mv_bid"
format = "json"
with_snapshot = true
batch_rows = 1000
```

Kafka sink formats are `json` and `debezium_json`. Debezium sinks require
`key_columns`. Config-file Kafka sinks also support `transactional_id`,
`checkpoint_topic`, and `checkpoint_partition`.

File sinks:

```toml
[[sinks]]
type = "file"
name = "mv_bid_file"
path = "/tmp/mv_bid.jsonl"
mv = "mv_bid"
append = true
with_snapshot = true
```

HTTP sinks:

```toml
[[sinks]]
type = "http"
name = "mv_bid_http"
url = "http://127.0.0.1:9000/mv_bid"
mv = "mv_bid"
batch_size = 100
with_snapshot = true
```

Postgres sinks:

```toml
[[sinks]]
type = "postgres"
name = "mv_bid_pg"
connection = "postgres://postgres:postgres@localhost/postgres"
table = "public.mv_bid"
mv = "mv_bid"
mode = "upsert"
primary_key = ["auction"]
with_snapshot = true
```

Postgres sink modes:

- `upsert`: negative diffs delete by key and positive diffs upsert in one transaction.
- `append_only`: inserts positive diffs and fails if the MV emits a negative diff.

Execution semantics:

- Kafka, file, and HTTP sinks flush on thresholds, MV version boundaries, and shutdown.
- Postgres applies each MV version in one transaction.
- Kafka and HTTP sinks retry transient failures with bounded exponential backoff.
- Permanent failures are recorded and stop the sink task.
- Bounded queues apply backpressure when consumers fall behind.

## CDC Replication Pipelines

Replication pipelines forward Postgres CDC source-table changes directly to
Kafka or Postgres targets:

```sql
CREATE REPLICATION PIPELINE pg_orders_to_kafka
FROM pg_main TABLE 'public.orders'
INTO KAFKA WITH (
  brokers = 'localhost:9092',
  topic = 'orders_cdc',
  format = 'debezium-json',
  durable_buffer = true,
  buffer.max_pending_bytes = 1048576,
  error.policy = 'dead-letter-and-continue'
);
```

Durable buffers are enabled by default and are bounded by the global
`[replication.buffer_limits]` settings plus any per-pipeline caps.

```toml
[replication.buffer_limits]
max_pending_bytes = 10737418240
max_pending_records = 0
max_pending_transactions = 0
max_pending_age_ms = 0

[replication.buffer_cleanup]
delivered_retention_ms = 5000
orphan_retention_ms = 60000
cleanup_interval_ms = 5000

[replication.kafka]
message_max_bytes = 10485760
acks = "1"
enable_idempotence = false
linger_ms = 1

[replication.encoding]
arrow_ipc_rows_per_record = 16384
snapshot_batches_per_chunk = 1
arrow_ipc_compression = "lz4_frame"
kafka_metadata_headers = false
```
