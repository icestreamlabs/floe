---
layout: default
title: Quickstart
description: Run Floe locally and subscribe to a materialized-view changelog.
permalink: /quickstart/
---

# Quickstart

Build Floe:

```bash
cargo build
```

Run a single-node runtime with the built-in Nexmark generator and one materialized view:

```bash
cargo run -- run --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

Subscribe to the view changelog with psql:

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -c "COPY (SUBSCRIBE mv WITH SNAPSHOT) TO STDOUT"
```

Query the materialized view:

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -c "SELECT COUNT(*) FROM mv"
```

## Optional input sources

Read newline-delimited JSON from a file:

```bash
cargo run -- run \
  --input-file /path/to/events.jsonl \
  --input-source nexmark_bid \
  --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

Read from Kafka:

```bash
cargo run -- run \
  --kafka-brokers localhost:9092 \
  --kafka-topics nexmark_bid \
  --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

Load configuration from a file:

```bash
cargo run -- run --config /path/to/floe.toml
```

Validate configuration and SQL planning without starting connectors or servers:

```bash
cargo run -- run --config /path/to/floe.toml --dry-run
```

## Useful runtime flags

| Flag | Purpose |
| --- | --- |
| `--pgwire-addr` | Change the pgwire bind address. Default: `127.0.0.1:6432`. |
| `--admin-port` | Change the admin HTTP port. Default: `8081`. |
| `--data-dir` | Persist SlateDB state under a filesystem directory. |
| `--slatedb-config` | Load SlateDB settings from TOML/YAML/JSON. |
| `--disable-pgwire` | Run without the pgwire endpoint. |

## Next steps

- Review the [support matrix]({{ site.baseurl }}/support/).
- Read the [SQL reference]({{ site.baseurl }}/sql/).
- Configure [connectors and sinks]({{ site.baseurl }}/connectors/).
