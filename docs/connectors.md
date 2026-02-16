# Connector Contracts

This document describes the expectations for connector implementations used by Floe.

## Lifecycle

Connectors implement a simple lifecycle:

- init: allocate resources and validate configuration.
- tick: emit zero or more `SourceEvent` records for one logical cycle.
- shutdown: release resources and finish gracefully.

The runtime drives `tick` at the connector's declared interval and stops when the
connector reports `Finished` or the runtime is cancelled.

## Event Emission

- Connectors send events through the shared `SourceEventSender`.
- Events are expected to be self-contained JSON objects with fields matching the
  corresponding `SourceDefinition`.
- A connector should skip emitting events with missing required fields rather
  than sending partial records.

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

Connector config fields are mapped into `SourceDefinition` properties for
introspection (e.g., `connector.kafka.brokers`, `connector.generator.events_per_second`).

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

The Postgres CDC connector polls a logical replication slot using
`pg_logical_slot_get_changes` with the `wal2json` output plugin. Only insert
and update events are emitted (delete events are ignored).

Example (TOML):

```toml
[[connectors]]
type = "postgres_cdc"
connection = "postgres://user:password@localhost:5432/db"
slot = "floe_slot"
poll_ms = 1000
max_changes = 500
include_tables = ["nexmark_bid", "nexmark_auction"]
```

## Sink Connectors

Sinks stream materialized view output (TAIL semantics) to external systems.
Each sink specifies the materialized view name (`mv`) and optional tail
parameters (`with_snapshot`, `as_of`).

Supported sinks:

- Kafka (`type = "kafka"`) writes JSON rows to a topic.
- File (`type = "file"`) appends JSONL rows to a file.
- HTTP (`type = "http"`) POSTs JSON batches to a URL.

Reliability and throughput options:

- `batch_rows` (all sinks): flush when buffered row count reaches threshold.
- `batch_bytes` (all sinks): flush when buffered serialized bytes reach threshold.
- `queue_capacity` (all sinks): bounded in-memory queue size between TAIL producer and sink worker.
- `retry_max_attempts` (Kafka/HTTP): max delivery attempts before permanent failure.
- `retry_base_ms` (Kafka/HTTP): base delay for exponential backoff.
- `retry_max_backoff_ms` (Kafka/HTTP): max delay cap for backoff.

Execution semantics:

- Rows are flushed on threshold, tail tick boundary, and shutdown.
- Kafka and HTTP sinks retry transient failures with bounded exponential backoff.
- Permanent failures are recorded and stop the sink task.
- Backpressure is applied naturally via bounded queues when sink workers lag.
- Metrics exported per sink:
  - `floe_sink_queue_depth{sink=...}`
  - `floe_sink_version_lag{sink=...}`
  - `floe_sink_failures_total{sink=...,transport=...}`
