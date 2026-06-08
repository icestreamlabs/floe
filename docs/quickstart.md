---
layout: default
title: Quickstart
description: Run Floe locally and subscribe to a materialized-view changelog.
permalink: /quickstart/
---

# Quickstart

From the repository root, build the default `floe-node` workspace member:

```bash
cargo build -p floe-node
```

Run a single-node runtime with the built-in Nexmark generator and one materialized view:

```bash
cargo run -p floe-node -- run \
  --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

Floe supports one materialized view per process today. The default pgwire endpoint is `127.0.0.1:6432`.

Subscribe to the view changelog with `psql`:

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -c "COPY (SUBSCRIBE mv WITH SNAPSHOT) TO STDOUT"
```

Query the materialized view:

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -c "SELECT COUNT(*) FROM mv"
```

Check the admin endpoint:

```bash
curl http://127.0.0.1:8081/readyz
```

## Optional input sources

Read newline-delimited JSON from a file:

```bash
cargo run -p floe-node -- run \
  --input-file /path/to/events.jsonl \
  --input-source nexmark_bid \
  --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

Read from Kafka:

```bash
cargo run -p floe-node -- run \
  --kafka-brokers localhost:9092 \
  --kafka-topics nexmark_bid \
  --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

Enable HTTP ingest:

```bash
cargo run -p floe-node -- run \
  --http-port 8080 \
  --http-source nexmark_bid \
  --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

Then post one row:

```bash
curl -X POST http://127.0.0.1:8080/ingest \
  -H 'content-type: application/json' \
  -d '{"auction":1,"bidder":42,"price":100,"channel":"web","url":"u","date_time":0,"extra":""}'
```

## Config-first startup

Create a config file when a run needs multiple connectors, sinks, runtime settings, or Postgres CDC settings:

```toml
[[connectors]]
type = "generator"
events_per_second = 100.0

[[materialized_views]]
name = "mv_bid"
query = "SELECT auction, bidder, price FROM nexmark_bid WHERE price >= 100"

[runtime]
pgwire_addr = "127.0.0.1:6432"
admin_port = 8081

[storage]
data_dir = "/tmp/floe-data"
```

Load it:

```bash
cargo run -p floe-node -- run --config /path/to/floe.toml
```

Validate configuration and SQL planning without starting connectors or servers:

```bash
cargo run -p floe-node -- run --config /path/to/floe.toml --dry-run
```

## Useful runtime flags

| Flag | Purpose |
| --- | --- |
| `--config` | Load TOML, YAML, or JSON configuration. |
| `--dry-run` | Validate config and SQL planning without starting connectors or servers. |
| `--events-per-second` / `--max-events` | Tune the built-in Nexmark generator. |
| `--pgwire-addr` | Change the pgwire bind address. Default: `127.0.0.1:6432`. |
| `--admin-port` | Change the admin HTTP port. Default: `8081`. |
| `--data-dir` | Persist SlateDB state under a filesystem directory. |
| `--object-store-from-env` / `--slatedb-name` | Use object-store-backed SlateDB state from environment variables. |
| `--slatedb-config` | Load SlateDB settings from TOML/YAML/JSON. |
| `--disable-pgwire` | Run without the pgwire endpoint. |
| `--ingest-batch-size` / `--ingest-batch-per-source` / `--ingest-batch-per-connector` | Tune append-ingest batching. |
| `--subscribe-channel-capacity` / `--subscribe-max-catchup-versions` | Tune pgwire `SUBSCRIBE` catch-up behavior. |

## Next steps

- Read the [SQL reference]({{ site.baseurl }}/sql/).
- Configure [connectors and sinks]({{ site.baseurl }}/connectors/).
- Check the [roadmap]({{ site.baseurl }}/roadmap/).
