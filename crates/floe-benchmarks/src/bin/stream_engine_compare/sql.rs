use super::{BenchQuery, Config, QUERY_COUNT_RELATION, QUERY_RESULT_RELATION};

pub(super) fn materialize_sql(config: &Config, bid_topic: &str, auction_topic: &str) -> String {
    let result_kind = if config.materialize_best_effort_in_memory {
        "VIEW"
    } else {
        "MATERIALIZED VIEW"
    };
    let default_indexes = if config.materialize_best_effort_in_memory {
        format!(
            "CREATE DEFAULT INDEX ON {QUERY_RESULT_RELATION};\nCREATE VIEW {QUERY_COUNT_RELATION} AS\nSELECT COUNT(*)::bigint AS row_count FROM {QUERY_RESULT_RELATION};\nCREATE DEFAULT INDEX ON {QUERY_COUNT_RELATION};"
        )
    } else {
        format!(
            "CREATE MATERIALIZED VIEW {QUERY_COUNT_RELATION} AS\nSELECT COUNT(*)::bigint AS row_count FROM {QUERY_RESULT_RELATION};"
        )
    };
    let source_sql = match config.bench_query {
        BenchQuery::FilterProjection => format!(
            "CREATE SOURCE bids_source\nFROM KAFKA CONNECTION kafka_conn (TOPIC '{bid_topic}')\nFORMAT JSON ENVELOPE NONE;"
        ),
        BenchQuery::Join => format!(
            "CREATE SOURCE bids_source\nFROM KAFKA CONNECTION kafka_conn (TOPIC '{bid_topic}')\nFORMAT JSON ENVELOPE NONE;\nCREATE SOURCE auctions_source\nFROM KAFKA CONNECTION kafka_conn (TOPIC '{auction_topic}')\nFORMAT JSON ENVELOPE NONE;"
        ),
    };
    let query_sql = match config.bench_query {
        BenchQuery::FilterProjection => format!(
            "CREATE {result_kind} {QUERY_RESULT_RELATION} AS\nSELECT\n  (data->>'auction')::bigint AS auction,\n  (data->>'bidder')::bigint AS bidder,\n  (data->>'price')::bigint AS projected_price\nFROM bids_source\nWHERE (data->>'auction')::bigint <= 5000;"
        ),
        BenchQuery::Join => format!(
            "CREATE {result_kind} {QUERY_RESULT_RELATION} AS\nSELECT\n  (b.data->>'auction')::bigint AS auction,\n  (b.data->>'bidder')::bigint AS bidder,\n  (b.data->>'price')::bigint AS projected_price,\n  (a.data->>'seller')::bigint AS seller\nFROM bids_source AS b\nJOIN auctions_source AS a\n  ON (b.data->>'auction')::bigint = (a.data->>'id')::bigint\nWHERE (a.data->>'category')::bigint = 10;"
        ),
    };
    format!(
        "DROP MATERIALIZED VIEW IF EXISTS {QUERY_COUNT_RELATION} CASCADE;\nDROP MATERIALIZED VIEW IF EXISTS {QUERY_RESULT_RELATION} CASCADE;\nDROP VIEW IF EXISTS {QUERY_COUNT_RELATION} CASCADE;\nDROP VIEW IF EXISTS {QUERY_RESULT_RELATION} CASCADE;\nDROP SOURCE IF EXISTS bids_source CASCADE;\nDROP SOURCE IF EXISTS auctions_source CASCADE;\nDROP CONNECTION IF EXISTS kafka_conn CASCADE;\nDROP CLUSTER IF EXISTS bench CASCADE;\nCREATE CLUSTER bench SIZE '{}';\nSET cluster = bench;\nCREATE CONNECTION kafka_conn TO KAFKA (\n  BROKER '{}',\n  SECURITY PROTOCOL PLAINTEXT\n);\n{source_sql}\n{query_sql}\n{default_indexes}\n",
        config.materialize_cluster_size, config.broker_addr_from_container
    )
}

pub(super) fn risingwave_sql(config: &Config, bid_topic: &str, auction_topic: &str) -> String {
    let latency_props = if config.kafka_latency_fetch_profile {
        format!(
            ",\n  properties.fetch.wait.max.ms = '{}',\n  properties.fetch.queue.backoff.ms = '{}',\n  properties.fetch.min.bytes = '{}'",
            config.kafka_fetch_wait_max_ms,
            config.kafka_fetch_queue_backoff_ms,
            config.kafka_fetch_min_bytes
        )
    } else {
        String::new()
    };
    let bid_source = format!(
        "CREATE SOURCE bids_source (\n  auction BIGINT,\n  bidder BIGINT,\n  price BIGINT,\n  channel VARCHAR,\n  url VARCHAR,\n  date_time BIGINT,\n  extra VARCHAR\n)\nWITH (\n  connector = 'kafka',\n  topic = '{bid_topic}',\n  properties.bootstrap.server = '{}',\n  scan.startup.mode = 'earliest'{latency_props}\n)\nFORMAT PLAIN ENCODE JSON;",
        config.broker_addr_from_container
    );
    let auction_source = format!(
        "CREATE SOURCE auctions_source (\n  id BIGINT,\n  item_name VARCHAR,\n  description VARCHAR,\n  initial_bid BIGINT,\n  reserve BIGINT,\n  seller BIGINT,\n  category BIGINT,\n  expires BIGINT,\n  date_time BIGINT,\n  extra VARCHAR\n)\nWITH (\n  connector = 'kafka',\n  topic = '{auction_topic}',\n  properties.bootstrap.server = '{}',\n  scan.startup.mode = 'earliest'{latency_props}\n)\nFORMAT PLAIN ENCODE JSON;",
        config.broker_addr_from_container
    );
    let query_sql = match config.bench_query {
        BenchQuery::FilterProjection => format!(
            "{bid_source}\nCREATE MATERIALIZED VIEW {QUERY_RESULT_RELATION} AS\nSELECT auction, bidder, price AS projected_price\nFROM bids_source\nWHERE auction <= 5000;\nCREATE MATERIALIZED VIEW {QUERY_COUNT_RELATION} AS\nSELECT COUNT(*)::BIGINT AS row_count FROM {QUERY_RESULT_RELATION};"
        ),
        BenchQuery::Join => format!(
            "{bid_source}\n{auction_source}\nCREATE MATERIALIZED VIEW {QUERY_RESULT_RELATION} AS\nSELECT b.auction, b.bidder, b.price AS projected_price, a.seller\nFROM bids_source AS b\nJOIN auctions_source AS a\n  ON b.auction = a.id\nWHERE a.category = 10;\nCREATE MATERIALIZED VIEW {QUERY_COUNT_RELATION} AS\nSELECT COUNT(*)::BIGINT AS row_count FROM {QUERY_RESULT_RELATION};"
        ),
    };
    format!(
        "DROP MATERIALIZED VIEW IF EXISTS {QUERY_COUNT_RELATION};\nDROP MATERIALIZED VIEW IF EXISTS {QUERY_RESULT_RELATION};\nDROP SOURCE IF EXISTS bids_source;\nDROP SOURCE IF EXISTS auctions_source;\n{query_sql}\n"
    )
}

pub(super) fn feldera_sql(config: &Config, bid_topic: &str, auction_topic: &str) -> String {
    let fetch_props = if config.kafka_latency_fetch_profile {
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
    let bid_source = feldera_source(
        "bids_source",
        &[
            ("auction", "BIGINT"),
            ("bidder", "BIGINT"),
            ("price", "BIGINT"),
            ("channel", "VARCHAR"),
            ("url", "VARCHAR"),
            ("date_time", "BIGINT"),
            ("extra", "VARCHAR"),
        ],
        bid_topic,
        &config.broker_addr_from_container,
        &fetch_props,
    );
    let auction_source = feldera_source(
        "auctions_source",
        &[
            ("id", "BIGINT"),
            ("item_name", "VARCHAR"),
            ("description", "VARCHAR"),
            ("initial_bid", "BIGINT"),
            ("reserve", "BIGINT"),
            ("seller", "BIGINT"),
            ("category", "BIGINT"),
            ("expires", "BIGINT"),
            ("date_time", "BIGINT"),
            ("extra", "VARCHAR"),
        ],
        auction_topic,
        &config.broker_addr_from_container,
        &fetch_props,
    );
    match config.bench_query {
        BenchQuery::FilterProjection => format!(
            "{bid_source}\nCREATE MATERIALIZED VIEW {QUERY_RESULT_RELATION} AS\nSELECT auction, bidder, price AS projected_price\nFROM bids_source\nWHERE auction <= 5000;\n\nCREATE MATERIALIZED VIEW {QUERY_COUNT_RELATION} AS\nSELECT COUNT(*) AS ROW_COUNT FROM {QUERY_RESULT_RELATION};\n"
        ),
        BenchQuery::Join => format!(
            "{bid_source}\n{auction_source}\nCREATE MATERIALIZED VIEW {QUERY_RESULT_RELATION} AS\nSELECT b.auction, b.bidder, b.price AS projected_price, a.seller\nFROM bids_source AS b\nJOIN auctions_source AS a ON b.auction = a.id\nWHERE a.category = 10;\n\nCREATE MATERIALIZED VIEW {QUERY_COUNT_RELATION} AS\nSELECT COUNT(*) AS ROW_COUNT FROM {QUERY_RESULT_RELATION};\n"
        ),
    }
}

fn feldera_source(
    name: &str,
    columns: &[(&str, &str)],
    topic: &str,
    brokers: &str,
    fetch_props: &str,
) -> String {
    let columns = columns
        .iter()
        .map(|(name, typ)| format!("    {name} {typ}"))
        .collect::<Vec<_>>()
        .join(",\n");
    format!(
        r#"CREATE TABLE {name} (
{columns}
) WITH (
    'connectors' = '[{{
      "transport": {{
        "name": "kafka_input",
        "config": {{
          "topic": "{topic}",
          "start_from": "earliest",
          "bootstrap.servers": "{brokers}"{fetch_props}
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
"#
    )
}
