# Floe SQL Parser

Minimal SQL helpers for Floe. This crate intentionally implements only the
parsing primitives we need for the CLI (currently `CREATE MATERIALIZED VIEW`).

The initial implementation references the grammar used by RisingWave's SQL
parser. Many thanks to the RisingWave Labs team for publishing their work under
the Apache 2.0 license.
