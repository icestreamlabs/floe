# Floe SQL Parser

Minimal SQL helpers for Floe. This crate intentionally implements only the
parsing primitives needed by Floe runtime/CLI flows:

- `CREATE MATERIALIZED VIEW`
- `CREATE SINK`
- `SUBSCRIBE`
- Multi-statement SQL program parsing with ordered statements

The initial implementation references the grammar used by RisingWave's SQL
parser. Many thanks to the RisingWave Labs team for publishing their work under
the Apache 2.0 license.
