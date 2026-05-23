# Postgres CDC Conformance Notes

This is a working checklist against the public Supermetal and RisingWave CDC
surface. It is intentionally implementation-oriented rather than marketing
copy.

References:

- Supermetal architecture:
  <https://docs.supermetal.io/docs/main/concepts/architecture/>
- Supermetal Postgres source:
  <https://docs.supermetal.io/docs/main/sources/pg/>
- Supermetal Postgres target:
  <https://docs.supermetal.io/docs/main/targets/postgres/>
- Supermetal local/object buffer docs:
  <https://docs.supermetal.io/docs/main/buffer/local/>
- RisingWave Postgres CDC:
  <https://docs.risingwave.com/ingestion/sources/postgresql/pg-cdc>
- RisingWave Postgres sink:
  <https://docs.risingwave.com/integrations/destinations/postgresql>
- RisingWave Debezium CDC architecture blog:
  <https://risingwave.com/blog/why-we-didnt-rewrite-debezium-in-rust/>

## Implemented

- Native Postgres CDC source using `pgoutput`, replication slots, and
  publications.
- Shared-source shape: `CREATE SOURCE ... connector = 'postgres-cdc'` plus
  `CREATE TABLE ... FROM <source> TABLE '<schema.table>'`.
- Materialized views over CDC tables.
- Durable CDC table path for non-passthrough materialized views.
- CDC passthrough/replication pipeline path with optional durable object-backed
  buffer.
- Transaction-aware LSN progress reporting after Floe's durable commit barrier.
- Initial snapshot with exported snapshot consistency, WAL handoff, table
  backfill, and adaptive parallelism controls.
- Primary-key metadata round trip for CDC table definitions.
- TOAST unchanged-value completion from materialized CDC table state.
- Schema evolution policy plumbing for fail-fast, compatible-ignore, and
  compatible-addition handling.
- Debezium JSON envelope encoder shared by replication pipelines and Kafka MV
  sinks.
- Postgres MV sink with append-only and upsert modes. Upsert requires
  `primary_key`, matching RisingWave's documented sink behavior.
- Postgres MV sink now stages each MV version through temp tables loaded by
  `COPY FROM STDIN`, then applies one bulk delete/upsert or append transaction.
- `TAIL`, pgwire `SUBSCRIBE`, and HTTP SSE `/mv` expose MV changelog output.

## Deliberate Differences

- Floe uses a Rust-native pgoutput decoder instead of embedding Debezium. The
  tradeoff is performance/control now, with a higher compatibility burden over
  time.
- Floe currently expects the Postgres replication slot and publication to exist.
  RisingWave can create them automatically by default.
- Floe supports one materialized view per process today. Multi-MV orchestration
  is a future product/runtime concern.
- Floe's durable CDC buffer is a bounded staging buffer for target lag and
  outages, not a permanent history store. CDC table state and checkpoints are
  separate from buffer payload retention.

## Remaining Gaps

- Automatic publication and replication slot creation with an explicit opt-out.
- HA/failover handling for Postgres cluster writer endpoint changes.
- Broader Postgres type coverage:
  - arrays,
  - binary/bytea,
  - JSON/JSONB extension metadata,
  - UUID/network/enums/domains,
  - time/timetz/interval/range/multirange policy.
- Binary COPY for Postgres target writes. Floe currently uses text `COPY` into
  temp staging tables and typed SQL casts, which is much better than row-wise
  writes but still leaves room for the Supermetal-style target path.
- More explicit partitioned-table publication guidance and tests.
- External Debezium compatibility conformance tests for exact envelope fields
  and tombstone/update/delete edge cases.
- Stress tests for schema evolution history growth and memory backpressure.

## Near-Term Priority

1. Add automatic slot/publication creation behind explicit configuration.
2. Extend Postgres source/target type coverage where it directly affects common
   CDC replication workloads.
3. Add HA reconnect tests around logical replication resume from the last
   durable LSN.
4. Add Debezium envelope golden fixtures that compare Floe output to Debezium
   for inserts, updates, deletes, snapshots, tombstones, and transaction
   metadata.
5. Explore binary COPY for the Postgres MV sink once the text COPY staging path
   has baseline benchmarks.
