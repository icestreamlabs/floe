use super::*;

pub(super) fn selected_queries(selector: &str) -> Result<Vec<String>> {
    if selector == "all" || selector == "nexmark_all" {
        return Ok(CANONICAL_NEXMARK_QUERY_IDS
            .iter()
            .map(|id| (*id).to_string())
            .collect());
    }
    if CANONICAL_NEXMARK_QUERY_IDS.contains(&selector) {
        return Ok(vec![selector.to_string()]);
    }
    bail!("unknown query selector '{selector}' (expected all|nexmark_all|q0..q22 canonical IDs)")
}

pub(super) fn required_sources_for_query(query_id: &str) -> Vec<Source> {
    match query_id {
        "q3" => vec![Source::Auction, Source::Person],
        "q4" | "q6" | "q9" | "q13" | "q20" => vec![Source::Bid, Source::Auction],
        "q8" => vec![Source::Person],
        _ => vec![Source::Bid],
    }
}

pub(super) fn relation_specs_for_sources(
    config: &Config,
    sources: &[Source],
    relation_prefix: &str,
) -> Vec<RelationSpec> {
    sources
        .iter()
        .map(|source| RelationSpec {
            relation: format!("{}_{}", relation_prefix, source.label()),
            target: config.rows_for_source(*source),
        })
        .collect()
}

pub(super) fn expected_result_rows_for_query(config: &Config, query_id: &str) -> Option<u64> {
    let bid_rows = config.bid_rows;
    let auction_rows = config.auction_rows;
    let person_rows = config.person_rows;
    match query_id {
        "q0" | "q1" | "q18" | "q21" | "q22" => Some(bid_rows),
        "q2" => {
            let full_cycles = bid_rows / 10_000;
            let rem = bid_rows % 10_000;
            Some(full_cycles * 81 + rem / 123)
        }
        "q3" => {
            let mut matches = 0;
            let mut id = 10;
            while id <= auction_rows && id <= person_rows {
                let rem = id % 6;
                if rem == 0 || rem == 1 || rem == 2 {
                    matches += 1;
                }
                id += 10;
            }
            Some(matches)
        }
        "q4" => {
            let auctions_with_bids = bid_rows.min(10_000);
            let joined_auctions = auction_rows.min(auctions_with_bids);
            Some(if joined_auctions < 10 {
                joined_auctions
            } else {
                10
            })
        }
        "q5" => Some(bid_rows * 5),
        "q6" | "q9" => Some(auction_rows.min(bid_rows.min(10_000))),
        "q7" => {
            if bid_rows == 0 {
                Some(0)
            } else {
                Some(bid_rows / 10_000 + 1)
            }
        }
        "q8" => Some(person_rows),
        "q12" => Some(bid_rows),
        "q13" => {
            let full_cycles = bid_rows / 10_000;
            let rem = bid_rows % 10_000;
            if auction_rows == 0 {
                Some(0)
            } else if auction_rows >= 10_000 {
                Some(bid_rows)
            } else {
                Some(full_cycles * auction_rows + rem.min(auction_rows))
            }
        }
        "q14" => Some(0),
        "q15" => Some(u64::from(bid_rows > 0)),
        "q16" => Some(if bid_rows < 5 { bid_rows } else { 5 }),
        "q17" => Some(if bid_rows < 10_000 { bid_rows } else { 10_000 }),
        "q19" => {
            let full_cycles = bid_rows / 10_000;
            let rem = bid_rows % 10_000;
            let top_q = full_cycles.min(10);
            let top_q1 = (full_cycles + 1).min(10);
            Some(rem * top_q1 + (10_000 - rem) * top_q)
        }
        "q20" => {
            let full_cycles = bid_rows / 10_000;
            let rem = bid_rows % 10_000;
            let mut total = 0;
            let mut id = 10;
            while id <= auction_rows && id <= 10_000 {
                total += full_cycles;
                if id <= rem {
                    total += 1;
                }
                id += 10;
            }
            Some(total)
        }
        _ => None,
    }
}

pub(super) fn query_sql_portable(query_id: &str) -> Option<String> {
    nexmark_query_sql(query_id, NexmarkSqlDialect::Portable)
}

pub(super) fn query_sql_for_engine(engine: Engine, query_id: &str) -> Option<String> {
    match engine {
        Engine::RisingWave | Engine::Feldera | Engine::Materialize => query_sql_portable(query_id),
        Engine::Floe => query_sql_floe(query_id),
    }
}

pub(super) fn query_sql_floe(query_id: &str) -> Option<String> {
    nexmark_query_sql(query_id, NexmarkSqlDialect::FloeKafka)
}

pub(super) fn write_materialize_setup_sql(
    config: &Config,
    query_id: &str,
    sources: &[Source],
    topics: &Topics,
    artifact_dir: &Path,
) -> Result<PathBuf> {
    let query_text = query_sql_for_engine(Engine::Materialize, query_id)
        .with_context(|| format!("query SQL for {query_id}"))?;
    let use_indexed_views = config.materialize_best_effort_in_memory;
    let mut sql = String::new();
    sql.push_str(&format!(
        r#"SET cluster = bench;
DROP INDEX IF EXISTS benchmark_ingest_bid_primary_idx CASCADE;
DROP INDEX IF EXISTS benchmark_ingest_auction_primary_idx CASCADE;
DROP INDEX IF EXISTS benchmark_ingest_person_primary_idx CASCADE;
DROP INDEX IF EXISTS benchmark_result_primary_idx CASCADE;
DROP VIEW IF EXISTS bid CASCADE;
DROP VIEW IF EXISTS auction CASCADE;
DROP VIEW IF EXISTS person CASCADE;
DROP SOURCE IF EXISTS bids_source CASCADE;
DROP SOURCE IF EXISTS auctions_source CASCADE;
DROP SOURCE IF EXISTS persons_source CASCADE;
DROP CONNECTION IF EXISTS kafka_conn CASCADE;
CREATE CONNECTION kafka_conn TO KAFKA (
  BROKER '{}',
  SECURITY PROTOCOL PLAINTEXT
);
"#,
        config.broker_addr_from_container
    ));
    if use_indexed_views {
        sql.push_str(
            "DROP VIEW IF EXISTS benchmark_ingest_bid CASCADE;\nDROP VIEW IF EXISTS benchmark_ingest_auction CASCADE;\nDROP VIEW IF EXISTS benchmark_ingest_person CASCADE;\nDROP VIEW IF EXISTS benchmark_result CASCADE;\n",
        );
    } else {
        sql.push_str(
            "DROP MATERIALIZED VIEW IF EXISTS benchmark_ingest_bid CASCADE;\nDROP MATERIALIZED VIEW IF EXISTS benchmark_ingest_auction CASCADE;\nDROP MATERIALIZED VIEW IF EXISTS benchmark_ingest_person CASCADE;\nDROP MATERIALIZED VIEW IF EXISTS benchmark_result CASCADE;\n",
        );
    }

    if sources.contains(&Source::Bid) {
        sql.push_str(&format!(
            r#"CREATE SOURCE bids_source
FROM KAFKA CONNECTION kafka_conn (TOPIC '{}')
FORMAT JSON ENVELOPE NONE;
CREATE VIEW bid AS
SELECT
  (data->>'auction')::bigint AS auction,
  (data->>'bidder')::bigint AS bidder,
  (data->>'price')::bigint AS price,
  (data->>'channel')::text AS channel,
  (data->>'url')::text AS url,
  (data->>'date_time')::bigint AS "dateTime",
  (data->>'extra')::text AS extra
FROM bids_source;
"#,
            topics.bid
        ));
        append_count_view(
            &mut sql,
            "benchmark_ingest_bid",
            "bids_source",
            use_indexed_views,
        );
    }
    if sources.contains(&Source::Auction) {
        sql.push_str(&format!(
            r#"CREATE SOURCE auctions_source
FROM KAFKA CONNECTION kafka_conn (TOPIC '{}')
FORMAT JSON ENVELOPE NONE;
CREATE VIEW auction AS
SELECT
  (data->>'id')::bigint AS id,
  (data->>'item_name')::text AS "itemName",
  (data->>'description')::text AS description,
  (data->>'initial_bid')::bigint AS "initialBid",
  (data->>'reserve')::bigint AS reserve,
  (data->>'date_time')::bigint AS "dateTime",
  (data->>'expires')::bigint AS expires,
  (data->>'seller')::bigint AS seller,
  (data->>'category')::bigint AS category,
  (data->>'extra')::text AS extra
FROM auctions_source;
"#,
            topics.auction
        ));
        append_count_view(
            &mut sql,
            "benchmark_ingest_auction",
            "auctions_source",
            use_indexed_views,
        );
    }
    if sources.contains(&Source::Person) {
        sql.push_str(&format!(
            r#"CREATE SOURCE persons_source
FROM KAFKA CONNECTION kafka_conn (TOPIC '{}')
FORMAT JSON ENVELOPE NONE;
CREATE VIEW person AS
SELECT
  (data->>'id')::bigint AS id,
  (data->>'name')::text AS name,
  (data->>'city')::text AS city,
  (data->>'state')::text AS state,
  (data->>'date_time')::bigint AS "dateTime",
  (data->>'extra')::text AS extra
FROM persons_source;
"#,
            topics.person
        ));
        append_count_view(
            &mut sql,
            "benchmark_ingest_person",
            "persons_source",
            use_indexed_views,
        );
    }
    if use_indexed_views {
        sql.push_str(&format!(
            "CREATE VIEW benchmark_result AS\n{query_text};\nCREATE DEFAULT INDEX ON benchmark_result;\n"
        ));
    } else {
        sql.push_str(&format!(
            "CREATE MATERIALIZED VIEW benchmark_result AS\n{query_text};\n"
        ));
    }
    let path = artifact_dir.join("setup.sql");
    fs::write(&path, sql)?;
    Ok(path)
}

pub(super) fn append_count_view(sql: &mut String, view: &str, source: &str, indexed_view: bool) {
    if indexed_view {
        sql.push_str(&format!(
            "CREATE VIEW {view} AS\nSELECT COUNT(*)::bigint AS row_count FROM {source};\nCREATE DEFAULT INDEX ON {view};\n"
        ));
    } else {
        sql.push_str(&format!(
            "CREATE MATERIALIZED VIEW {view} AS\nSELECT COUNT(*)::bigint AS row_count FROM {source};\n"
        ));
    }
}

pub(super) fn write_risingwave_setup_sql(
    config: &Config,
    query_id: &str,
    sources: &[Source],
    topics: &Topics,
    artifact_dir: &Path,
) -> Result<PathBuf> {
    let query_text = query_sql_for_engine(Engine::RisingWave, query_id)
        .with_context(|| format!("query SQL for {query_id}"))?;
    let fetch_opts = if config.kafka_latency_fetch_profile {
        format!(
            "\n  ,properties.fetch.wait.max.ms = '{}'\n  ,properties.fetch.queue.backoff.ms = '{}'\n  ,properties.fetch.min.bytes = '{}'",
            config.kafka_fetch_wait_max_ms,
            config.kafka_fetch_queue_backoff_ms,
            config.kafka_fetch_min_bytes
        )
    } else {
        String::new()
    };
    let mut sql = String::from(
        "DROP MATERIALIZED VIEW IF EXISTS benchmark_ingest_bid;\nDROP MATERIALIZED VIEW IF EXISTS benchmark_ingest_auction;\nDROP MATERIALIZED VIEW IF EXISTS benchmark_ingest_person;\nDROP MATERIALIZED VIEW IF EXISTS benchmark_result;\nDROP MATERIALIZED VIEW IF EXISTS bid;\nDROP MATERIALIZED VIEW IF EXISTS auction;\nDROP MATERIALIZED VIEW IF EXISTS person;\nDROP SOURCE IF EXISTS bids_source;\nDROP SOURCE IF EXISTS auctions_source;\nDROP SOURCE IF EXISTS persons_source;\n",
    );
    if sources.contains(&Source::Bid) {
        sql.push_str(&format!(
            r#"CREATE SOURCE bids_source (
  auction BIGINT,
  bidder BIGINT,
  price BIGINT,
  channel VARCHAR,
  url VARCHAR,
  date_time BIGINT,
  extra VARCHAR
)
WITH (
  connector = 'kafka',
  topic = '{}',
  properties.bootstrap.server = '{}',
  scan.startup.mode = 'earliest'{}
)
FORMAT PLAIN ENCODE JSON;
CREATE MATERIALIZED VIEW bid AS
SELECT auction, bidder, price, channel, url, date_time AS "dateTime", extra
FROM bids_source;
CREATE MATERIALIZED VIEW benchmark_ingest_bid AS
SELECT COUNT(*)::BIGINT AS row_count FROM bids_source;
"#,
            topics.bid, config.broker_addr_from_container, fetch_opts
        ));
    }
    if sources.contains(&Source::Auction) {
        sql.push_str(&format!(
            r#"CREATE SOURCE auctions_source (
  id BIGINT,
  item_name VARCHAR,
  description VARCHAR,
  initial_bid BIGINT,
  reserve BIGINT,
  seller BIGINT,
  category BIGINT,
  expires BIGINT,
  date_time BIGINT,
  extra VARCHAR
)
WITH (
  connector = 'kafka',
  topic = '{}',
  properties.bootstrap.server = '{}',
  scan.startup.mode = 'earliest'{}
)
FORMAT PLAIN ENCODE JSON;
CREATE MATERIALIZED VIEW auction AS
SELECT id, item_name AS "itemName", description, initial_bid AS "initialBid", reserve, date_time AS "dateTime", expires, seller, category, extra
FROM auctions_source;
CREATE MATERIALIZED VIEW benchmark_ingest_auction AS
SELECT COUNT(*)::BIGINT AS row_count FROM auctions_source;
"#,
            topics.auction, config.broker_addr_from_container, fetch_opts
        ));
    }
    if sources.contains(&Source::Person) {
        sql.push_str(&format!(
            r#"CREATE SOURCE persons_source (
  id BIGINT,
  name VARCHAR,
  email_address VARCHAR,
  credit_card VARCHAR,
  city VARCHAR,
  state VARCHAR,
  date_time BIGINT,
  extra VARCHAR
)
WITH (
  connector = 'kafka',
  topic = '{}',
  properties.bootstrap.server = '{}',
  scan.startup.mode = 'earliest'{}
)
FORMAT PLAIN ENCODE JSON;
CREATE MATERIALIZED VIEW person AS
SELECT id, name, city, state, date_time AS "dateTime", extra
FROM persons_source;
CREATE MATERIALIZED VIEW benchmark_ingest_person AS
SELECT COUNT(*)::BIGINT AS row_count FROM persons_source;
"#,
            topics.person, config.broker_addr_from_container, fetch_opts
        ));
    }
    sql.push_str(&format!(
        "CREATE MATERIALIZED VIEW benchmark_result AS\n{query_text};\n"
    ));
    let path = artifact_dir.join("setup.sql");
    fs::write(&path, sql)?;
    Ok(path)
}

pub(super) fn feldera_program_sql(
    config: &Config,
    query_id: &str,
    sources: &[Source],
    topics: &Topics,
) -> Result<String> {
    let query_text = query_sql_for_engine(Engine::Feldera, query_id)
        .with_context(|| format!("query SQL for {query_id}"))?;
    let fetch_json = if config.kafka_latency_fetch_profile {
        format!(
            r#",
          "fetch.wait.max.ms": "{}",
          "fetch.queue.backoff.ms": "{}",
          "fetch.min.bytes": "{}""#,
            config.kafka_fetch_wait_max_ms,
            config.kafka_fetch_queue_backoff_ms,
            config.kafka_fetch_min_bytes
        )
    } else {
        String::new()
    };
    let mut sql = String::new();
    if sources.contains(&Source::Bid) {
        sql.push_str(&format!(
            r#"CREATE TABLE bids_source (
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    channel VARCHAR,
    url VARCHAR,
    date_time BIGINT,
    extra VARCHAR
) WITH (
    'connectors' = '[{{
      "name": "bids_in",
      "transport": {{
        "name": "kafka_input",
        "config": {{
          "topic": "{}",
          "start_from": "earliest",
          "bootstrap.servers": "{}"{}
        }}
      }},
      "format": {{
        "name": "json",
        "config": {{
          "update_format": "raw",
          "array": false
        }}
      }}
    }}]'
);

CREATE MATERIALIZED VIEW bid AS
SELECT auction, bidder, price, channel, url, date_time AS "dateTime", extra
FROM bids_source;

CREATE MATERIALIZED VIEW benchmark_ingest_bid AS
SELECT COUNT(*) AS row_count FROM bids_source;

"#,
            topics.bid, config.broker_addr_from_container, fetch_json
        ));
    }
    if sources.contains(&Source::Auction) {
        sql.push_str(&format!(
            r#"CREATE TABLE auctions_source (
    id BIGINT,
    item_name VARCHAR,
    description VARCHAR,
    initial_bid BIGINT,
    reserve BIGINT,
    seller BIGINT,
    category BIGINT,
    expires BIGINT,
    date_time BIGINT,
    extra VARCHAR
) WITH (
    'connectors' = '[{{
      "name": "auctions_in",
      "transport": {{
        "name": "kafka_input",
        "config": {{
          "topic": "{}",
          "start_from": "earliest",
          "bootstrap.servers": "{}"{}
        }}
      }},
      "format": {{
        "name": "json",
        "config": {{
          "update_format": "raw",
          "array": false
        }}
      }}
    }}]'
);

CREATE MATERIALIZED VIEW auction AS
SELECT id, item_name AS "itemName", description, initial_bid AS "initialBid", reserve, date_time AS "dateTime", expires, seller, category, extra
FROM auctions_source;

CREATE MATERIALIZED VIEW benchmark_ingest_auction AS
SELECT COUNT(*) AS row_count FROM auctions_source;

"#,
            topics.auction, config.broker_addr_from_container, fetch_json
        ));
    }
    if sources.contains(&Source::Person) {
        sql.push_str(&format!(
            r#"CREATE TABLE persons_source (
    id BIGINT,
    name VARCHAR,
    email_address VARCHAR,
    credit_card VARCHAR,
    city VARCHAR,
    state VARCHAR,
    date_time BIGINT,
    extra VARCHAR
) WITH (
    'connectors' = '[{{
      "name": "persons_in",
      "transport": {{
        "name": "kafka_input",
        "config": {{
          "topic": "{}",
          "start_from": "earliest",
          "bootstrap.servers": "{}"{}
        }}
      }},
      "format": {{
        "name": "json",
        "config": {{
          "update_format": "raw",
          "array": false
        }}
      }}
    }}]'
);

CREATE MATERIALIZED VIEW person AS
SELECT id, name, city, state, date_time AS "dateTime", extra
FROM persons_source;

CREATE MATERIALIZED VIEW benchmark_ingest_person AS
SELECT COUNT(*) AS row_count FROM persons_source;

"#,
            topics.person, config.broker_addr_from_container, fetch_json
        ));
    }
    sql.push_str(&format!(
        "CREATE MATERIALIZED VIEW benchmark_result AS\n{query_text};\n"
    ));
    Ok(sql)
}

pub(super) fn floe_config_json(config: &Config) -> serde_json::Value {
    json!({
        "runtime": {
            "ingest_queue_capacity": config.floe_ingest_queue_capacity,
            "ingest_batch_size": config.floe_ingest_batch_size,
            "ingest_batch_per_source": config.floe_ingest_batch_per_source,
            "ingest_batch_per_connector": config.floe_ingest_batch_per_connector,
            "mv_retain_last": config.floe_mv_retain_last,
            "mv_flush": {
                "enabled": config.floe_mv_flush_enabled,
                "max_pending_deltas": if config.floe_mv_flush_max_pending_deltas > 0 {
                    json!(config.floe_mv_flush_max_pending_deltas)
                } else {
                    serde_json::Value::Null
                },
                "max_delay_ms": if config.floe_mv_flush_max_delay_ms > 0 {
                    json!(config.floe_mv_flush_max_delay_ms)
                } else {
                    serde_json::Value::Null
                },
                "flush_on_catchup_boundary": config.floe_mv_flush_on_catchup_boundary,
            }
        },
        "storage": {
            "await_durable": config.floe_slatedb_await_durable == "true",
            "source_journal": config.floe_source_journal,
        }
    })
}

pub(super) fn floe_program_sql(
    config: &Config,
    query_id: &str,
    sources: &[Source],
    topics: &Topics,
    groups: &Groups,
) -> Result<String> {
    let query_text =
        query_sql_floe(query_id).with_context(|| format!("Floe query SQL for {query_id}"))?;
    Ok(format!(
        "{}CREATE MATERIALIZED VIEW benchmark_result AS\n{query_text};\n",
        floe_source_setup_sql(config, sources, topics, groups)
    ))
}

pub(super) fn floe_validation_program_sql(
    config: &Config,
    query_id: &str,
    sources: &[Source],
    topics: &Topics,
    groups: &Groups,
) -> Result<String> {
    let expected_query = floe_expected_query_text_for_source_tables(query_id, sources)?;
    Ok(format!(
        "{}CREATE MATERIALIZED VIEW benchmark_result AS\n{expected_query};\n",
        floe_source_setup_sql(config, sources, topics, groups)
    ))
}

fn floe_source_setup_sql(
    config: &Config,
    sources: &[Source],
    topics: &Topics,
    groups: &Groups,
) -> String {
    let mut sql = String::new();
    for source in sources {
        sql.push_str(&floe_kafka_source_sql(
            config,
            *source,
            topics.for_source(*source),
            groups.for_source(*source),
        ));
        sql.push('\n');
    }
    sql
}

fn floe_kafka_source_sql(config: &Config, source: Source, topic: &str, group_id: &str) -> String {
    let columns = source
        .floe_columns()
        .iter()
        .map(|(name, typ)| format!("  {name} {typ}"))
        .chain(
            (!source.floe_primary_key().is_empty())
                .then(|| format!("  PRIMARY KEY ({})", source.floe_primary_key().join(", "))),
        )
        .collect::<Vec<_>>()
        .join(",\n");
    format!(
        "CREATE SOURCE {} (\n{columns}\n)\nWITH (\n  connector = 'kafka',\n  brokers = '{}',\n  topic = '{topic}',\n  group_id = '{group_id}',\n  poll_ms = {},\n  max_messages_per_tick = {}\n)\nFORMAT PLAIN ENCODE JSON;\n",
        source.floe_source(),
        config.broker_addr,
        config.floe_kafka_poll_ms,
        config.floe_kafka_max_messages_per_tick
    )
}

pub(super) fn floe_expected_query_text_for_source_tables(
    query_id: &str,
    sources: &[Source],
) -> Result<String> {
    match query_id {
        "q5" | "q7" | "q8" | "q12" => {
            let query_text = query_sql_portable(query_id)
                .with_context(|| format!("portable query SQL for {query_id}"))?;
            Ok(wrap_query_with_source_ctes(&query_text, sources, true))
        }
        "q13" => {
            let query_text = query_sql_portable(query_id)
                .with_context(|| format!("portable query SQL for {query_id}"))?;
            Ok(wrap_query_with_source_ctes(&query_text, sources, false))
        }
        _ => {
            let query_text = query_sql_floe(query_id)
                .with_context(|| format!("Floe query SQL for {query_id}"))?;
            Ok(query_text.to_string())
        }
    }
}

pub(super) fn wrap_query_with_source_ctes(
    query_text: &str,
    sources: &[Source],
    cast_time_to_bigint: bool,
) -> String {
    let mut ctes = Vec::new();
    if sources.contains(&Source::Bid) {
        let date_expr = if cast_time_to_bigint {
            r#"CAST(date_time AS BIGINT) AS "dateTime""#
        } else {
            r#"date_time AS "dateTime""#
        };
        ctes.push(format!(
            r#"bid AS (SELECT auction, bidder, price, channel, url, {date_expr}, extra FROM nexmark_bid)"#
        ));
    }
    if sources.contains(&Source::Auction) {
        let date_expr = if cast_time_to_bigint {
            r#"CAST(date_time AS BIGINT) AS "dateTime""#
        } else {
            r#"date_time AS "dateTime""#
        };
        ctes.push(format!(
            r#"auction AS (SELECT id, item_name AS "itemName", description, initial_bid AS "initialBid", reserve, {date_expr}, expires, seller, category, extra FROM nexmark_auction)"#
        ));
    }
    if sources.contains(&Source::Person) {
        let date_expr = if cast_time_to_bigint {
            r#"CAST(date_time AS BIGINT) AS "dateTime""#
        } else {
            r#"date_time AS "dateTime""#
        };
        ctes.push(format!(
            r#"person AS (SELECT id, name, city, state, {date_expr}, extra FROM nexmark_person)"#
        ));
    }
    if ctes.is_empty() {
        query_text.to_string()
    } else {
        format!("WITH {} {query_text}", ctes.join(", "))
    }
}
