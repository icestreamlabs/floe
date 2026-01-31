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
