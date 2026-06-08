# Floe

Floe is a single-node streaming SQL database built on a DBSP runtime, DataFusion
planning, and SlateDB-backed state. It ingests events from connectors, builds
materialized views, and serves results over a pgwire-compatible endpoint.

## Quickstart

Build:

```bash
cargo build
```

Run the node with the built-in Nexmark generator and a single materialized view:

```bash
cargo run -- run --mv-query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_bid"
```

Subscribe to the view changelog over pgwire (defaults to 127.0.0.1:6432):

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -c "COPY (SUBSCRIBE mv WITH SNAPSHOT) TO STDOUT"
```

