---
layout: default
title: Connectors and Sinks
description: Floe connectors, sink behavior, and delivery settings.
permalink: /connectors/
---

# Connectors and Sinks

This page describes the connector and sink behavior Floe exposes today.

## Lifecycle

Connectors follow a simple lifecycle:

- init: allocate resources and validate configuration.
- tick: emit zero or more source events for one logical cycle.
- shutdown: release resources and finish gracefully.

The runtime drives `tick` at the connector's declared interval and stops when the
connector reports `Finished` or the runtime is cancelled.

Connector lifecycle behavior:

- pre-tick commit notification: connectors can receive commit decisions
  before polling (for example, Kafka offsets and Postgres CDC LSN advancement).
- post-checkpoint barrier: commit notifications are sent only after tick state
  and checkpoint writes are durable.

## Event Emission

- Connectors send events through the shared source-event channel.
- Events are expected to be self-contained JSON objects with fields matching the
  corresponding `SourceDefinition`.
- A connector should skip emitting events with missing required fields rather
  than sending partial records.
- Connectors should attach resume metadata whenever
  available (partition/offset/LSN/cursor) and may attach `event_time_ms` when
  source-native event time is known.

## Batching Expectations

- The runtime batches events per source before writing to DBSP outer streams.
- Connectors do not need to implement batching themselves; emitting one event
  per `tick` is acceptable.
- If a connector naturally produces batches (e.g., polling a file or stream),
  it can emit multiple events per `tick` as long as they share the same source
  schema.

## File Connector Format

The file connector reads newline-delimited JSON. Each line must be either:

- An object containing `source` and `data` fields, where `data` is the row payload.
- A row payload object, when a default source name is configured.

Examples:

```json
{"source":"nexmark_bid","data":{"auction":1,"bidder":42,"price":100,"channel":"web","url":"u","date_time":0,"extra":""}}
```

```json
{"auction":1,"bidder":42,"price":100,"channel":"web","url":"u","date_time":0,"extra":""}
```

## HTTP Ingest Endpoint

When enabled, Floe exposes `POST /ingest` to accept JSON payloads. The body can be:

- An object with `source` and `data` fields.
- A row payload object when a default source is configured.
- An array of either of the above.

Optional query parameter `source` overrides the configured default source.

Examples:

```bash
curl -X POST http://127.0.0.1:8080/ingest \
  -H 'content-type: application/json' \
  -d '{"source":"nexmark_bid","data":{"auction":1,"bidder":42,"price":100,"channel":"web","url":"u","date_time":0,"extra":""}}'
```

```bash
curl -X POST 'http://127.0.0.1:8080/ingest?source=nexmark_bid' \
  -H 'content-type: application/json' \
  -d '{"auction":1,"bidder":42,"price":100,"channel":"web","url":"u","date_time":0,"extra":""}'
```

## Kafka Connector

When enabled, Floe consumes JSON payloads from Kafka topics. Each message value can be:

- An object with `source` and `data` fields.
- A row payload object, which uses the configured default source if provided.
- A row payload object with no default source, which falls back to the Kafka topic name.
- An array of either of the above.

Examples (message value):

```json
{"source":"nexmark_bid","data":{"auction":1,"bidder":42,"price":100,"channel":"web","url":"u","date_time":0,"extra":""}}
```

```json
{"auction":1,"bidder":42,"price":100,"channel":"web","url":"u","date_time":0,"extra":""}
```

Enable it with `--kafka-brokers` and `--kafka-topics`. Optional flags:
`--kafka-group-id`, `--kafka-default-source`, `--kafka-poll-ms`, and
`--kafka-max-messages`.

## Connector Configuration Files

Floe can load connector and sink definitions from a config file:

- `--config path/to/connectors.toml`
- Supported formats: TOML, YAML, JSON
- Configuration can also include `materialized_views`, `runtime`, `storage`,
  and `maintenance` sections.

Example (TOML):

```toml
[[connectors]]
type = "generator"
events_per_second = 50.0
max_events = 10000

[[connectors]]
type = "kafka"
brokers = "localhost:9092"
topics = ["nexmark_bid"]
group_id = "floe"
default_source = "nexmark_bid"

[[connectors]]
type = "http"
host = "127.0.0.1"
port = 8080
default_source = "nexmark_bid"

[[sinks]]
type = "file"
mv = "mv_bid_passthrough"
path = "/tmp/mv_bid.jsonl"
with_snapshot = true
```

### Merge and Precedence Rules

When multiple inputs are provided, Floe applies deterministic precedence:

1. Connector and sink definitions in `--config` are the base runtime input.
2. `CREATE MATERIALIZED VIEW` and `CREATE SINK` statements from `--mv-query`
   are applied after config parsing.
3. Existing persisted materialized views are loaded first, then config/SQL
   updates are applied in process startup order.

Operational notes:

- If `--config` is present, connector creation flags (`--http-port`,
  `--kafka-brokers`, `--kafka-topics`, `--input-file`) are ignored.
- Runtime/storage knobs (for example `--slatedb-*`, `--zset-*`,
  `--mv-retain-last`) still apply when `--config` is used.

Connector config fields are exposed for introspection (for example,
`connector.kafka.brokers` and `connector.generator.events_per_second`).

Validation rules:

- Required string fields must be non-empty (for example `brokers`, `slot`, `url`).
- Numeric rate/limit fields must be positive when provided.
- Connector-specific required collections (for example Kafka `topics`) must be
  non-empty.

## Object Store Connector

The object store connector reads newline-delimited JSON from an object store
prefix (S3-compatible via `s3://` URLs).

Example (TOML):

```toml
[[connectors]]
type = "object_store"
url = "s3://my-bucket/events/nexmark/"
default_source = "nexmark_bid"
```

The connector lists all objects under the prefix and ingests each line as an
event payload.

## Postgres CDC Connector

The Postgres CDC connector uses native logical replication with the built-in
`pgoutput` plugin. It reads from an existing logical replication slot and
publication, and reports the applied LSN only after Floe's durable tick-commit
barrier. CDC-backed tables use the native CDC table runtime so inserts, updates,
and deletes can be reflected in materialized views and replication pipelines.

Example (TOML):

```toml
[[connectors]]
type = "postgres_cdc"
connection = "postgres://user:password@localhost:5432/db"
slot = "floe_slot"
publication = "floe_publication"
include_tables = ["nexmark_bid", "nexmark_auction"]
```

## Sink Connectors

Sinks stream materialized view changelog output to external systems. Each sink
specifies the materialized view name (`mv`) and optional stream parameters
(`with_snapshot`, `as_of`).

Supported sinks:

- Kafka (`type = "kafka"`) writes JSON rows to a topic.
- File (`type = "file"`) appends JSONL rows to a file.
- HTTP (`type = "http"`) POSTs JSON batches to a URL.
- Postgres (`type = "postgres"`) writes MV changes into a target table.

Postgres sinks support two modes:

- `mode = "upsert"` requires `primary_key = [...]`. Negative diffs delete by
  key, then positive diffs upsert rows in the same transaction.
- `mode = "append_only"` inserts positive diffs and fails if the MV emits a
  negative diff.

Reliability and throughput options:

- `batch_rows` (Kafka/File/HTTP): flush when buffered row count reaches threshold.
- `batch_bytes` (Kafka/File/HTTP): flush when buffered serialized bytes reach
  threshold.
- `queue_capacity` (Kafka/File/HTTP): bounded in-memory queue size between the
  changelog producer and sink worker.
- `retry_max_attempts` (Kafka/HTTP/Postgres): max delivery attempts before
  permanent failure.
- `retry_base_ms` (Kafka/HTTP/Postgres): base delay for exponential backoff.
- `retry_max_backoff_ms` (Kafka/HTTP/Postgres): max delay cap for backoff.

Execution semantics:

- Kafka/File/HTTP rows are flushed on threshold, MV version boundary, and shutdown.
- Postgres applies each MV version in one transaction and checkpoints after
  commit. The sink uses temporary text staging tables loaded by
  `COPY FROM STDIN`, then applies a bulk delete/upsert or append statement.
- Kafka and HTTP sinks retry transient failures with bounded exponential backoff.
- Permanent failures are recorded and stop the sink task.
- Backpressure is applied via bounded queues for queued sinks and directly by
  the Postgres commit path for Postgres sinks.
- Metrics exported per sink:
  - `floe_sink_queue_depth{sink=...}`
  - `floe_sink_version_lag{sink=...}`
  - `floe_sink_failures_total{sink=...,transport=...}`
  - `floe_sink_retries_total{sink=...,transport=...}`

See the [operations page]({{ site.baseurl }}/operations/) for health checks,
metrics, and CDC operator endpoints.
