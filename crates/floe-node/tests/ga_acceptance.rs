use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[path = "support/ports.rs"]
mod ports;

use anyhow::{Context, Result, bail};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use ports::find_unused_port;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::TcpListener as TokioTcpListener;
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_postgres::NoTls;

const BID_MV_SQL: &str = "CREATE MATERIALIZED VIEW mv_acceptance_bid AS \
     SELECT auction, bidder, price FROM nexmark_bid";
const JOIN_MV_SQL: &str = "CREATE MATERIALIZED VIEW mv_acceptance_join AS \
     SELECT b.auction, b.bidder, b.price, a.seller \
     FROM nexmark_bid b JOIN nexmark_auction a ON b.auction = a.id";

#[tokio::test]
async fn http_ingest_to_mv_to_http_sink_acceptance() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let ingest_port = find_unused_port()?;
    let sink_port = find_unused_port()?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("http_http_sink.json");
    let ingest_addr = format!("http://127.0.0.1:{ingest_port}");
    let sink_url = format!("http://127.0.0.1:{sink_port}/collect");

    let (sink_tx, mut sink_rx) = mpsc::channel::<Value>(16);
    let sink_server = spawn_sink_server(sink_port, sink_tx).await?;

    let config = json!({
        "connectors": [
            {
                "type": "http",
                "host": "127.0.0.1",
                "port": ingest_port,
                "default_source": "nexmark_bid"
            }
        ],
        "sinks": [
            {
                "type": "http",
                "name": "http_acceptance",
                "mv": "mv_acceptance_bid",
                "url": sink_url,
                "with_snapshot": true,
                "batch_rows": 1,
                "batch_bytes": 1048576,
                "queue_capacity": 64
            }
        ]
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write acceptance config")?;

    let mut child = spawn_node_with_args(&config_path, &data_dir, 0, Some(BID_MV_SQL), &[]).await?;

    let test_result = async {
        wait_for_healthz(&ingest_addr).await?;
        post_bid(&ingest_addr, 101, 7001, 999).await?;

        let payload = timeout(Duration::from_secs(10), sink_rx.recv())
            .await
            .context("timed out waiting for sink payload")?
            .context("sink receiver closed")?;
        let rows = payload_to_rows(payload);
        let matched = rows.iter().any(|row| {
            row.get("auction").and_then(Value::as_i64) == Some(101)
                && row.get("bidder").and_then(Value::as_i64) == Some(7001)
                && row.get("price").and_then(Value::as_i64) == Some(999)
        });
        if !matched {
            bail!("http sink payload did not include expected row");
        }
        Ok(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    sink_server.abort();
    let _ = sink_server.await;
    test_result
}

#[tokio::test]
#[ignore = "requires Kafka broker; set FLOE_ACCEPTANCE_KAFKA_BROKERS (and optionally FLOE_ACCEPTANCE_KAFKA_TOPIC_PREFIX)"]
async fn kafka_to_mv_to_pgwire_acceptance() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let pg_port = find_unused_port()?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("kafka_pgwire_acceptance.json");
    let brokers = std::env::var("FLOE_ACCEPTANCE_KAFKA_BROKERS")
        .context("set FLOE_ACCEPTANCE_KAFKA_BROKERS for kafka acceptance")?;
    let topic_prefix = std::env::var("FLOE_ACCEPTANCE_KAFKA_TOPIC_PREFIX")
        .unwrap_or_else(|_| "floe_acceptance".to_string());
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let topic = format!("{topic_prefix}_{run_id}");
    let group_id = format!("floe-acceptance-{run_id}");

    let config = json!({
        "connectors": [
            {
                "type": "kafka",
                "brokers": brokers,
                "topics": [topic],
                "group_id": group_id,
                "default_source": "nexmark_bid",
                "poll_ms": 100,
                "max_messages_per_tick": 64
            }
        ]
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write kafka acceptance config")?;

    let mut child = spawn_node(&config_path, &data_dir, pg_port, Some(BID_MV_SQL)).await?;

    let test_result = async {
        sleep(Duration::from_millis(400)).await;
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .context("create kafka producer")?;
        let payload = json!({
            "source": "nexmark_bid",
            "data": {
                "auction": 202,
                "bidder": 7002,
                "price": 1999,
                "channel": "web",
                "url": "http://example.com",
                "date_time": 1_700_000_202_i64,
                "extra": "kafka_acceptance"
            }
        })
        .to_string();
        let record = FutureRecord::<(), _>::to(&topic).payload(&payload);
        producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(err, _)| err)
            .context("produce kafka acceptance message")?;

        wait_for_auction_count_at_least(pg_port, 202, 1).await?;
        Ok(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    test_result
}

#[tokio::test]
#[ignore = "requires Kafka broker; set FLOE_ACCEPTANCE_KAFKA_BROKERS (and optionally FLOE_ACCEPTANCE_KAFKA_TOPIC_PREFIX)"]
async fn kafka_restart_rebuilds_transient_join_from_replayable_topic() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let pg_port = find_unused_port()?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("kafka_restart_join.json");
    let brokers = std::env::var("FLOE_ACCEPTANCE_KAFKA_BROKERS")
        .context("set FLOE_ACCEPTANCE_KAFKA_BROKERS for kafka acceptance")?;
    let topic_prefix = std::env::var("FLOE_ACCEPTANCE_KAFKA_TOPIC_PREFIX")
        .unwrap_or_else(|_| "floe_acceptance".to_string());
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let topic = format!("{topic_prefix}_restart_{run_id}");
    let group_id = format!("floe-acceptance-restart-{run_id}");

    create_kafka_topic(&brokers, &topic).await?;

    let config = json!({
        "connectors": [
            {
                "type": "kafka",
                "brokers": brokers,
                "topics": [topic],
                "group_id": group_id,
                "poll_ms": 25,
                "max_messages_per_tick": 64
            }
        ],
        "runtime": {
            "mv_snapshot": {
                "max_pending_rows": 1,
                "max_pending_batches": 1,
                "max_delay_ms": 100
            }
        }
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write kafka restart config")?;

    let mut first = spawn_node(&config_path, &data_dir, pg_port, Some(JOIN_MV_SQL)).await?;
    let test_result = async {
        sleep(Duration::from_millis(500)).await;
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .context("create kafka producer")?;
        produce_auction(&producer, &topic, 501, 9001).await?;
        produce_bid(&producer, &topic, 501, 8001, 1234).await?;
        wait_for_join_count_at_least(pg_port, 501, 1).await?;
        sleep(Duration::from_millis(500)).await;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    stop_child(&mut first, "INT").await;
    test_result?;

    let mut restarted = spawn_node(&config_path, &data_dir, pg_port, Some(JOIN_MV_SQL)).await?;
    let restart_result = async {
        wait_for_join_count_at_least(pg_port, 501, 1).await?;
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .context("create kafka producer")?;
        produce_bid(&producer, &topic, 501, 8002, 4321).await?;
        wait_for_join_count_at_least(pg_port, 501, 2).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    stop_child(&mut restarted, "INT").await;
    restart_result?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_to_mv_to_file_sink_acceptance() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let table = "nexmark_bid";
    let slot = format!("floe_acceptance_{run_id}");
    let publication = format!("floe_acceptance_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let sink_path = temp_dir.path().join("cdc_sink.jsonl");
    let config_path = temp_dir.path().join("cdc_file_sink_acceptance.json");

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for acceptance setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres acceptance setup connection closed");
        }
    });

    client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {publication};
             DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (
               auction BIGINT NOT NULL,
               bidder BIGINT NOT NULL,
               price BIGINT NOT NULL,
               channel TEXT,
               url TEXT,
               date_time BIGINT NOT NULL,
               extra TEXT
             );
             CREATE PUBLICATION {publication} FOR TABLE {table};"
        ))
        .await
        .context("prepare cdc acceptance table")?;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;
    client
        .query_one(
            "SELECT * FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
        .context("create pgoutput replication slot")?;

    let config = json!({
        "connectors": [
            {
                "type": "postgres_cdc",
                "connection": dsn,
                "slot": slot,
                "publication": publication,
                "include_tables": [table]
            }
        ],
        "sinks": [
            {
                "type": "file",
                "mv": "mv_acceptance_bid",
                "path": sink_path,
                "with_snapshot": true,
                "append": true
            }
        ]
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write cdc acceptance config")?;

    let mut child = spawn_node_with_args(&config_path, &data_dir, 0, Some(BID_MV_SQL), &[]).await?;

    let test_result = async {
        sleep(Duration::from_millis(500)).await;
        client
            .execute(
                &format!(
                    "INSERT INTO {table} \
                     (auction, bidder, price, channel, url, date_time, extra) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7)"
                ),
                &[
                    &901_i64,
                    &7001_i64,
                    &500_i64,
                    &"web".to_string(),
                    &"http://example.com".to_string(),
                    &1_700_000_901_i64,
                    &"cdc_acceptance".to_string(),
                ],
            )
            .await
            .context("insert cdc acceptance row")?;
        wait_for_rows_matching(&sink_path, |value| {
            value.get("auction").and_then(Value::as_i64) == Some(901)
                && value.get("bidder").and_then(Value::as_i64) == Some(7001)
                && value.get("price").and_then(Value::as_i64) == Some(500)
        })
        .await?;
        Ok(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;
    let _ = client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication};"))
        .await;
    let _ = client
        .batch_execute(&format!("DROP TABLE IF EXISTS {table};"))
        .await;
    test_result
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_table_mv_update_delete_acceptance() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let table = "nexmark_bid".to_string();
    let mv_name = format!("mv_floe_cdc_bid_{run_id}");
    let slot = format!("floe_acceptance_native_{run_id}");
    let publication = format!("floe_acceptance_native_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let sink_path = temp_dir.path().join("cdc_native_sink.jsonl");
    let config_path = temp_dir.path().join("cdc_native_file_sink_acceptance.json");

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for native cdc acceptance setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres native cdc acceptance setup connection closed");
        }
    });

    client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {publication};
             DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (
               auction BIGINT PRIMARY KEY,
               bidder BIGINT NOT NULL,
               price BIGINT NOT NULL,
               channel TEXT,
               url TEXT,
               date_time BIGINT NOT NULL,
               extra TEXT
             );
             CREATE PUBLICATION {publication} FOR TABLE {table};"
        ))
        .await
        .context("prepare native cdc acceptance table")?;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;

    let config = json!({
        "connectors": [
            {
                "type": "postgres_cdc",
                "connection": dsn,
                "slot": slot,
                "publication": publication,
                "include_tables": [table]
            }
        ],
        "sinks": [
            {
                "type": "file",
                "mv": mv_name,
                "path": sink_path,
                "with_snapshot": true,
                "append": true
            }
        ]
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write native cdc acceptance config")?;

    let sql = format!(
        "CREATE TABLE {table} (
            auction BIGINT PRIMARY KEY,
            bidder BIGINT NOT NULL,
            price BIGINT NOT NULL,
            channel TEXT,
            url TEXT,
            date_time BIGINT NOT NULL,
            extra TEXT
         );
         CREATE MATERIALIZED VIEW {mv_name} AS SELECT auction, bidder, price FROM {table}"
    );
    let mut child = spawn_node_with_args(&config_path, &data_dir, 0, Some(&sql), &[]).await?;

    let test_result = async {
        sleep(Duration::from_millis(500)).await;
        client
            .execute(
                &format!(
                    "INSERT INTO {table} \
                     (auction, bidder, price, channel, url, date_time, extra) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7)"
                ),
                &[
                    &1_i64,
                    &10_i64,
                    &100_i64,
                    &"web",
                    &"http://example.com",
                    &1_700_000_001_i64,
                    &"open",
                ],
            )
            .await
            .context("insert native cdc row")?;
        wait_for_rows_matching(&sink_path, |value| {
            value.get("__op").and_then(Value::as_i64) == Some(1)
                && value.get("auction").and_then(Value::as_i64) == Some(1)
                && value.get("bidder").and_then(Value::as_i64) == Some(10)
                && value.get("price").and_then(Value::as_i64) == Some(100)
        })
        .await?;

        client
            .execute(
                &format!("UPDATE {table} SET price = $1, extra = $2 WHERE auction = $3"),
                &[&150_i64, &"paid", &1_i64],
            )
            .await
            .context("update native cdc row")?;
        wait_for_rows_matching(&sink_path, |value| {
            value.get("__op").and_then(Value::as_i64) == Some(-1)
                && value.get("auction").and_then(Value::as_i64) == Some(1)
                && value.get("price").and_then(Value::as_i64) == Some(100)
        })
        .await?;
        wait_for_rows_matching(&sink_path, |value| {
            value.get("__op").and_then(Value::as_i64) == Some(1)
                && value.get("auction").and_then(Value::as_i64) == Some(1)
                && value.get("price").and_then(Value::as_i64) == Some(150)
        })
        .await?;

        client
            .execute(
                &format!("DELETE FROM {table} WHERE auction = $1"),
                &[&1_i64],
            )
            .await
            .context("delete native cdc row")?;
        wait_for_rows_matching(&sink_path, |value| {
            value.get("__op").and_then(Value::as_i64) == Some(-1)
                && value.get("auction").and_then(Value::as_i64) == Some(1)
                && value.get("price").and_then(Value::as_i64) == Some(150)
        })
        .await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;
    let _ = client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication};"))
        .await;
    let _ = client
        .batch_execute(&format!("DROP TABLE IF EXISTS {table};"))
        .await;
    test_result
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_mv_to_postgres_sink_acceptance() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let escaped_dsn = sql_string_literal(&dsn);
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let source_name = format!("pg_sink_source_{run_id}");
    let source_table = format!("floe_mv_sink_orders_{run_id}");
    let target_table = format!("floe_mv_sink_target_{run_id}");
    let mv_name = format!("mv_pg_sink_{run_id}");
    let sink_name = format!("pg_sink_{run_id}");
    let slot = format!("floe_mv_sink_{run_id}");
    let publication = format!("floe_mv_sink_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("postgres_mv_sink_acceptance.json");
    std::fs::write(&config_path, "{}").context("write empty acceptance config")?;

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for MV sink acceptance setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres MV sink acceptance setup connection closed");
        }
    });

    cleanup_postgres_sink_acceptance(&client, &publication, &slot, &source_table, &target_table)
        .await;
    client
        .batch_execute(&format!(
            "CREATE TABLE {source_table} (
               id BIGINT PRIMARY KEY,
               amount BIGINT NOT NULL,
               note TEXT
             );
             CREATE TABLE {target_table} (
               id BIGINT PRIMARY KEY,
               amount BIGINT NOT NULL,
               note TEXT
             );
             CREATE PUBLICATION {publication} FOR TABLE {source_table};"
        ))
        .await
        .context("prepare Postgres MV sink acceptance tables")?;

    let sql = format!(
        "CREATE SOURCE {source_name} WITH (
            connector = 'postgres-cdc',
            connection = '{escaped_dsn}',
            slot.name = '{slot}',
            publication.name = '{publication}'
         );
         CREATE TABLE {source_table} (
            id BIGINT PRIMARY KEY,
            amount BIGINT NOT NULL,
            note TEXT
         ) FROM {source_name} TABLE 'public.{source_table}';
         CREATE MATERIALIZED VIEW {mv_name} AS
         SELECT id, amount, note FROM {source_table};
         CREATE SINK {sink_name} FROM {mv_name} WITH (
            connector = 'postgres',
            connection = '{escaped_dsn}',
            table = 'public.{target_table}',
            mode = 'upsert',
            primary_key = 'id',
            with_snapshot = true
         );"
    );
    let mut child = spawn_node_with_args(&config_path, &data_dir, 0, Some(&sql), &[]).await?;

    let test_result = async {
        sleep(Duration::from_millis(500)).await;
        client
            .execute(
                &format!("INSERT INTO {source_table} (id, amount, note) VALUES ($1, $2, $3)"),
                &[&1_i64, &100_i64, &"open"],
            )
            .await
            .context("insert source row for Postgres MV sink")?;
        wait_for_postgres_sink_row(&client, &target_table, 1, 100, Some("open")).await?;

        client
            .execute(
                &format!("UPDATE {source_table} SET amount = $1, note = $2 WHERE id = $3"),
                &[&175_i64, &"paid", &1_i64],
            )
            .await
            .context("update source row for Postgres MV sink")?;
        wait_for_postgres_sink_row(&client, &target_table, 1, 175, Some("paid")).await?;

        client
            .execute(
                &format!("DELETE FROM {source_table} WHERE id = $1"),
                &[&1_i64],
            )
            .await
            .context("delete source row for Postgres MV sink")?;
        wait_for_postgres_sink_absent(&client, &target_table, 1).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    cleanup_postgres_sink_acceptance(&client, &publication, &slot, &source_table, &target_table)
        .await;
    test_result
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_table_restart_resumes_from_committed_lsn() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let pg_port = find_unused_port()?;
    let table = "nexmark_bid".to_string();
    let mv_name = format!("mv_floe_cdc_restart_{run_id}");
    let slot = format!("floe_acceptance_restart_{run_id}");
    let publication = format!("floe_acceptance_restart_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("cdc_restart_acceptance.json");

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for native cdc restart setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres native cdc restart setup connection closed");
        }
    });

    client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {publication};
             DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (
               auction BIGINT PRIMARY KEY,
               bidder BIGINT NOT NULL,
               price BIGINT NOT NULL,
               channel TEXT,
               url TEXT,
               date_time BIGINT NOT NULL,
               extra TEXT
             );
             CREATE PUBLICATION {publication} FOR TABLE {table};"
        ))
        .await
        .context("prepare native cdc restart table")?;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;

    let config = json!({
        "connectors": [
            {
                "type": "postgres_cdc",
                "connection": dsn,
                "slot": slot,
                "publication": publication,
                "include_tables": [table]
            }
        ]
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write native cdc restart config")?;

    let sql = format!(
        "CREATE TABLE {table} (
            auction BIGINT PRIMARY KEY,
            bidder BIGINT NOT NULL,
            price BIGINT NOT NULL,
            channel TEXT,
            url TEXT,
            date_time BIGINT NOT NULL,
            extra TEXT
         );
         CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name} AS
         SELECT auction, bidder, price FROM {table}"
    );
    let mut first = spawn_node(&config_path, &data_dir, pg_port, Some(&sql)).await?;

    let first_result = async {
        sleep(Duration::from_millis(500)).await;
        client
            .execute(
                &format!(
                    "INSERT INTO {table} \
                     (auction, bidder, price, channel, url, date_time, extra) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7)"
                ),
                &[
                    &11_i64,
                    &20_i64,
                    &100_i64,
                    &"web",
                    &"http://example.com",
                    &1_700_000_011_i64,
                    &"before_restart",
                ],
            )
            .await
            .context("insert native cdc restart row")?;
        wait_for_mv_price_count_at_least(pg_port, &mv_name, 11, 100, 1).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    stop_child(&mut first, "INT").await;
    first_result?;

    let mut restarted = spawn_node(&config_path, &data_dir, pg_port, Some(&sql)).await?;
    let restart_result = async {
        wait_for_mv_price_count_at_least(pg_port, &mv_name, 11, 100, 1).await?;
        assert_eq!(
            query_mv_price_count(pg_port, &mv_name, 11, 100).await?,
            1,
            "restarted CDC MV should expose the committed pre-restart row once"
        );

        client
            .execute(
                &format!("UPDATE {table} SET price = $1, extra = $2 WHERE auction = $3"),
                &[&175_i64, &"after_restart", &11_i64],
            )
            .await
            .context("update native cdc row after restart")?;
        wait_for_mv_price_count_at_least(pg_port, &mv_name, 11, 175, 1).await?;
        assert_eq!(
            query_mv_price_count(pg_port, &mv_name, 11, 100).await?,
            0,
            "post-restart CDC update should retract the old MV row"
        );
        assert_eq!(
            query_mv_price_count(pg_port, &mv_name, 11, 175).await?,
            1,
            "post-restart CDC update should insert the new MV row once"
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;
    stop_child(&mut restarted, "INT").await;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;
    let _ = client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication};"))
        .await;
    let _ = client
        .batch_execute(&format!("DROP TABLE IF EXISTS {table};"))
        .await;
    restart_result
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_shared_source_transaction_feeds_join_mv() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let pg_port = find_unused_port()?;
    let bid_table = "nexmark_bid".to_string();
    let auction_table = "nexmark_auction".to_string();
    let bid_mv = format!("mv_floe_cdc_shared_bid_{run_id}");
    let auction_mv = format!("mv_floe_cdc_shared_auction_{run_id}");
    let join_mv = format!("mv_floe_cdc_shared_join_{run_id}");
    let slot = format!("floe_acceptance_join_{run_id}");
    let publication = format!("floe_acceptance_join_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("cdc_join_acceptance.json");

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for shared-source cdc setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres shared-source cdc setup connection closed");
        }
    });

    client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {publication};
             DROP TABLE IF EXISTS {bid_table};
             DROP TABLE IF EXISTS {auction_table};
             CREATE TABLE {auction_table} (
               id BIGINT PRIMARY KEY,
               seller BIGINT NOT NULL,
               category BIGINT NOT NULL,
               initial_bid BIGINT NOT NULL,
               reserve BIGINT NOT NULL,
               item_name TEXT,
               description TEXT,
               expires BIGINT NOT NULL,
               date_time BIGINT NOT NULL,
               extra TEXT
             );
             CREATE TABLE {bid_table} (
               auction BIGINT PRIMARY KEY,
               bidder BIGINT NOT NULL,
               price BIGINT NOT NULL,
               channel TEXT,
               url TEXT,
               date_time BIGINT NOT NULL,
               extra TEXT
             );
             CREATE PUBLICATION {publication} FOR TABLE {auction_table}, {bid_table};"
        ))
        .await
        .context("prepare shared-source cdc tables")?;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;

    let config = json!({
        "connectors": [
            {
                "type": "postgres_cdc",
                "connection": dsn,
                "slot": slot,
                "publication": publication,
                "include_tables": [auction_table, bid_table]
            }
        ]
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write shared-source cdc config")?;

    let sql = format!(
        "CREATE TABLE {auction_table} (
            id BIGINT PRIMARY KEY,
            seller BIGINT NOT NULL,
            category BIGINT NOT NULL,
            initial_bid BIGINT NOT NULL,
            reserve BIGINT NOT NULL,
            item_name TEXT,
            description TEXT,
            expires BIGINT NOT NULL,
            date_time BIGINT NOT NULL,
            extra TEXT
         );
         CREATE TABLE {bid_table} (
            auction BIGINT PRIMARY KEY,
            bidder BIGINT NOT NULL,
            price BIGINT NOT NULL,
            channel TEXT,
            url TEXT,
            date_time BIGINT NOT NULL,
            extra TEXT
         );
         CREATE MATERIALIZED VIEW IF NOT EXISTS {bid_mv} AS
         SELECT auction, bidder, price FROM {bid_table};
         CREATE MATERIALIZED VIEW IF NOT EXISTS {auction_mv} AS
         SELECT id, seller FROM {auction_table};
         CREATE MATERIALIZED VIEW IF NOT EXISTS {join_mv} AS
         SELECT b.auction, b.bidder, b.price, a.seller
         FROM {bid_table} AS b JOIN {auction_table} AS a ON b.auction = a.id"
    );
    let mut child = spawn_node(&config_path, &data_dir, pg_port, Some(&sql)).await?;

    let test_result = async {
        sleep(Duration::from_millis(500)).await;
        client
            .batch_execute(&format!(
                "BEGIN;
                 INSERT INTO {auction_table}
                   (id, seller, category, initial_bid, reserve, item_name, description, expires, date_time, extra)
                   VALUES (21, 9001, 17, 100, 500, 'item', 'description', 1700100021, 1700000021, 'auction');
                 INSERT INTO {bid_table}
                   (auction, bidder, price, channel, url, date_time, extra)
                   VALUES (21, 42, 650, 'web', 'http://example.com', 1700000021, 'bid');
                 COMMIT;"
            ))
            .await
            .context("commit shared-source cdc transaction")?;
        wait_for_mv_price_count_at_least(pg_port, &bid_mv, 21, 650, 1).await?;
        wait_for_auction_seller_count_at_least(pg_port, &auction_mv, 21, 9001, 1).await?;
        wait_for_join_mv_count_at_least(pg_port, &join_mv, 21, 42, 9001, 1).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;
    let _ = client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication};"))
        .await;
    let _ = client
        .batch_execute(&format!("DROP TABLE IF EXISTS {bid_table};"))
        .await;
    let _ = client
        .batch_execute(&format!("DROP TABLE IF EXISTS {auction_table};"))
        .await;
    test_result
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_shared_source_snapshot_converges_to_wal_stream() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let pg_port = find_unused_port()?;
    let admin_port = find_unused_port()?;
    let bid_table = format!("nexmark_bid_shared_snapshot_{run_id}");
    let auction_table = format!("nexmark_auction_shared_snapshot_{run_id}");
    let source_name = format!("pg_shared_snapshot_{run_id}");
    let join_mv = format!("mv_floe_cdc_shared_snapshot_join_{run_id}");
    let slot = format!("floe_acceptance_shared_snapshot_{run_id}");
    let publication = format!("floe_acceptance_shared_snapshot_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("empty.json");
    std::fs::write(&config_path, "{}").context("write empty config")?;

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for shared-source snapshot setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres shared-source snapshot setup connection closed");
        }
    });

    client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {publication};
             DROP TABLE IF EXISTS {bid_table};
             DROP TABLE IF EXISTS {auction_table};
             CREATE TABLE {auction_table} (
               id BIGINT PRIMARY KEY,
               seller BIGINT NOT NULL,
               category BIGINT NOT NULL,
               initial_bid BIGINT NOT NULL,
               reserve BIGINT NOT NULL,
               item_name TEXT,
               description TEXT,
               expires BIGINT NOT NULL,
               date_time BIGINT NOT NULL,
               extra TEXT
             );
             CREATE TABLE {bid_table} (
               auction BIGINT PRIMARY KEY,
               bidder BIGINT NOT NULL,
               price BIGINT NOT NULL,
               channel TEXT,
               url TEXT,
               date_time BIGINT NOT NULL,
               extra TEXT
             );
             INSERT INTO {auction_table}
               (id, seller, category, initial_bid, reserve, item_name, description, expires, date_time, extra)
               VALUES (81, 9801, 17, 100, 500, 'snapshot item', 'description', 1700100081, 1700000081, 'auction_snapshot');
             INSERT INTO {bid_table}
               (auction, bidder, price, channel, url, date_time, extra)
               VALUES (81, 781, 881, 'web', 'http://example.com', 1700000081, 'bid_snapshot');"
        ))
        .await
        .context("prepare shared-source snapshot cdc tables")?;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;

    let sql = format!(
        "CREATE SOURCE {source_name} WITH (
            connector = 'postgres-cdc',
            connection = '{dsn}',
            slot.name = '{slot}',
            publication.name = '{publication}'
         );
         CREATE TABLE {auction_table} (
            id BIGINT PRIMARY KEY,
            seller BIGINT NOT NULL,
            category BIGINT NOT NULL,
            initial_bid BIGINT NOT NULL,
            reserve BIGINT NOT NULL,
            item_name TEXT,
            description TEXT,
            expires BIGINT NOT NULL,
            date_time BIGINT NOT NULL,
            extra TEXT
         ) FROM {source_name} TABLE 'public.{auction_table}';
         CREATE TABLE {bid_table} (
            auction BIGINT PRIMARY KEY,
            bidder BIGINT NOT NULL,
            price BIGINT NOT NULL,
            channel TEXT,
            url TEXT,
            date_time BIGINT NOT NULL,
            extra TEXT
         ) FROM {source_name} TABLE 'public.{bid_table}';
         CREATE MATERIALIZED VIEW IF NOT EXISTS {join_mv} AS
         SELECT b.auction, b.bidder, b.price, a.seller
         FROM {bid_table} AS b JOIN {auction_table} AS a ON b.auction = a.id"
    );
    let admin_args = vec!["--admin-port".to_string(), admin_port.to_string()];
    let mut child =
        spawn_node_with_args(&config_path, &data_dir, pg_port, Some(&sql), &admin_args).await?;

    let test_result = async {
        wait_for_join_mv_count_at_least(pg_port, &join_mv, 81, 781, 9801, 1).await?;
        wait_for_admin_metrics_contains(
            admin_port,
            &format!(
                "floe_postgres_cdc_source_lag_bytes{{slot=\"{slot}\",source=\"{source_name}\""
            ),
        )
        .await?;
        wait_for_admin_metrics_contains(
            admin_port,
            &format!(
                "floe_postgres_cdc_table_last_applied_lsn{{slot=\"{slot}\",source=\"{source_name}\",table=\"{bid_table}\""
            ),
        )
        .await?;

        client
            .batch_execute(&format!(
                "BEGIN;
                 INSERT INTO {auction_table}
                   (id, seller, category, initial_bid, reserve, item_name, description, expires, date_time, extra)
                   VALUES (82, 9802, 17, 100, 500, 'wal item', 'description', 1700100082, 1700000082, 'auction_wal');
                 INSERT INTO {bid_table}
                   (auction, bidder, price, channel, url, date_time, extra)
                   VALUES (82, 782, 882, 'web', 'http://example.com', 1700000082, 'bid_wal');
                 COMMIT;"
            ))
            .await
            .context("commit shared-source cdc transaction after snapshot")?;
        wait_for_join_mv_count_at_least(pg_port, &join_mv, 82, 782, 9802, 1).await?;
        wait_for_admin_metrics_contains(
            admin_port,
            &format!(
                "floe_postgres_cdc_table_lag_bytes{{slot=\"{slot}\",source=\"{source_name}\",table=\"{auction_table}\""
            ),
        )
        .await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;
    let _ = client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication};"))
        .await;
    let _ = client
        .batch_execute(&format!("DROP TABLE IF EXISTS {bid_table};"))
        .await;
    let _ = client
        .batch_execute(&format!("DROP TABLE IF EXISTS {auction_table};"))
        .await;
    test_result
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_sql_source_table_mv_acceptance() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let pg_port = find_unused_port()?;
    let table = "nexmark_bid".to_string();
    let source_name = format!("pg_sql_{run_id}");
    let mv_name = format!("mv_floe_cdc_sql_{run_id}");
    let slot = format!("floe_acceptance_sql_{run_id}");
    let publication = format!("floe_acceptance_sql_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("empty.json");
    std::fs::write(&config_path, "{}").context("write empty config")?;

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for SQL CDC acceptance setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres SQL CDC acceptance setup connection closed");
        }
    });

    client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {publication};
             DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (
               auction BIGINT PRIMARY KEY,
               bidder BIGINT NOT NULL,
               price BIGINT NOT NULL,
               channel TEXT,
               url TEXT,
               date_time BIGINT NOT NULL,
               extra TEXT
             );
             CREATE PUBLICATION {publication} FOR TABLE {table};"
        ))
        .await
        .context("prepare SQL CDC acceptance table")?;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;

    let sql = format!(
        "CREATE SOURCE {source_name} WITH (
            connector = 'postgres-cdc',
            connection = '{dsn}',
            slot.name = '{slot}',
            publication.name = '{publication}'
         );
         CREATE TABLE {table} (
            auction BIGINT PRIMARY KEY,
            bidder BIGINT NOT NULL,
            price BIGINT NOT NULL,
            channel TEXT,
            url TEXT,
            date_time BIGINT NOT NULL,
            extra TEXT
         ) FROM {source_name} TABLE 'public.{table}';
         CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name} AS
         SELECT auction, bidder, price FROM {table}"
    );
    let mut child = spawn_node(&config_path, &data_dir, pg_port, Some(&sql)).await?;

    let test_result = async {
        sleep(Duration::from_millis(500)).await;
        client
            .execute(
                &format!(
                    "INSERT INTO {table} \
                     (auction, bidder, price, channel, url, date_time, extra) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7)"
                ),
                &[
                    &41_i64,
                    &910_i64,
                    &123_i64,
                    &"web",
                    &"http://example.com",
                    &1_700_000_041_i64,
                    &"sql_surface",
                ],
            )
            .await
            .context("insert SQL CDC row")?;
        wait_for_mv_price_count_at_least(pg_port, &mv_name, 41, 123, 1).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;
    let _ = client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication};"))
        .await;
    let _ = client
        .batch_execute(&format!("DROP TABLE IF EXISTS {table};"))
        .await;
    test_result
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_sql_source_table_snapshot_backfill_acceptance() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let pg_port = find_unused_port()?;
    let table = format!("nexmark_bid_snapshot_{run_id}");
    let source_name = format!("pg_sql_snapshot_{run_id}");
    let mv_name = format!("mv_floe_cdc_sql_snapshot_{run_id}");
    let slot = format!("floe_acceptance_snapshot_{run_id}");
    let publication = format!("floe_acceptance_snapshot_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("empty.json");
    std::fs::write(&config_path, "{}").context("write empty config")?;

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for SQL CDC snapshot setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres SQL CDC snapshot setup connection closed");
        }
    });

    client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {publication};
             DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (
               auction BIGINT PRIMARY KEY,
               bidder BIGINT NOT NULL,
               price BIGINT NOT NULL,
               channel TEXT,
               url TEXT,
               date_time BIGINT NOT NULL,
               extra TEXT
             );
             INSERT INTO {table}
               (auction, bidder, price, channel, url, date_time, extra)
               VALUES (71, 971, 701, 'web', 'http://example.com', 1700000071, 'snapshot');"
        ))
        .await
        .context("prepare SQL CDC snapshot acceptance table")?;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;

    let sql = format!(
        "CREATE SOURCE {source_name} WITH (
            connector = 'postgres-cdc',
            connection = '{dsn}',
            slot.name = '{slot}',
            publication.name = '{publication}'
         );
         CREATE TABLE {table} (
            auction BIGINT PRIMARY KEY,
            bidder BIGINT NOT NULL,
            price BIGINT NOT NULL,
            channel TEXT,
            url TEXT,
            date_time BIGINT NOT NULL,
            extra TEXT
         ) FROM {source_name} TABLE 'public.{table}';
         CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name} AS
         SELECT auction, bidder, price FROM {table}"
    );
    let mut child = spawn_node(&config_path, &data_dir, pg_port, Some(&sql)).await?;

    let test_result = async {
        wait_for_mv_price_count_at_least(pg_port, &mv_name, 71, 701, 1).await?;

        client
            .execute(
                &format!(
                    "INSERT INTO {table} \
                     (auction, bidder, price, channel, url, date_time, extra) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7)"
                ),
                &[
                    &72_i64,
                    &972_i64,
                    &702_i64,
                    &"web",
                    &"http://example.com",
                    &1_700_000_072_i64,
                    &"wal_after_snapshot",
                ],
            )
            .await
            .context("insert SQL CDC row after snapshot")?;
        wait_for_mv_price_count_at_least(pg_port, &mv_name, 72, 702, 1).await?;

        client
            .execute(
                &format!("UPDATE {table} SET price = $1, extra = $2 WHERE auction = $3"),
                &[&731_i64, &"snapshot_updated", &71_i64],
            )
            .await
            .context("update SQL CDC snapshot row")?;
        wait_for_mv_price_count_at_least(pg_port, &mv_name, 71, 731, 1).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;
    let _ = client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication};"))
        .await;
    let _ = client
        .batch_execute(&format!("DROP TABLE IF EXISTS {table};"))
        .await;
    test_result
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_table_aggregate_update_delete_acceptance() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let pg_port = find_unused_port()?;
    let table = "nexmark_bid".to_string();
    let mv_name = format!("mv_floe_cdc_aggregate_{run_id}");
    let slot = format!("floe_acceptance_aggregate_{run_id}");
    let publication = format!("floe_acceptance_aggregate_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("cdc_aggregate_acceptance.json");

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for aggregate cdc setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres aggregate cdc setup connection closed");
        }
    });

    client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {publication};
             DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (
               auction BIGINT PRIMARY KEY,
               bidder BIGINT NOT NULL,
               price BIGINT NOT NULL,
               channel TEXT,
               url TEXT,
               date_time BIGINT NOT NULL,
               extra TEXT
             );
             CREATE PUBLICATION {publication} FOR TABLE {table};"
        ))
        .await
        .context("prepare aggregate cdc table")?;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;

    let config = json!({
        "connectors": [
            {
                "type": "postgres_cdc",
                "connection": dsn,
                "slot": slot,
                "publication": publication,
                "include_tables": [table]
            }
        ]
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write aggregate cdc config")?;

    let sql = format!(
        "CREATE TABLE {table} (
            auction BIGINT PRIMARY KEY,
            bidder BIGINT NOT NULL,
            price BIGINT NOT NULL,
            channel TEXT,
            url TEXT,
            date_time BIGINT NOT NULL,
            extra TEXT
         );
         CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name} AS
         SELECT bidder, COUNT(*) AS bid_count, SUM(price) AS total_price
         FROM {table}
         GROUP BY bidder"
    );
    let mut child = spawn_node(&config_path, &data_dir, pg_port, Some(&sql)).await?;

    let test_result = async {
        sleep(Duration::from_millis(500)).await;
        client
            .batch_execute(&format!(
                "BEGIN;
                 INSERT INTO {table}
                   (auction, bidder, price, channel, url, date_time, extra)
                   VALUES (31, 900, 100, 'web', 'http://example.com', 1700000031, 'first');
                 INSERT INTO {table}
                   (auction, bidder, price, channel, url, date_time, extra)
                   VALUES (32, 900, 200, 'web', 'http://example.com', 1700000032, 'second');
                 COMMIT;"
            ))
            .await
            .context("commit aggregate cdc inserts")?;
        wait_for_bidder_aggregate(pg_port, &mv_name, 900, 2, 300).await?;

        client
            .execute(
                &format!("UPDATE {table} SET price = $1, extra = $2 WHERE auction = $3"),
                &[&150_i64, &"updated", &31_i64],
            )
            .await
            .context("update aggregate cdc row")?;
        wait_for_bidder_aggregate(pg_port, &mv_name, 900, 2, 350).await?;

        client
            .execute(
                &format!("DELETE FROM {table} WHERE auction = $1"),
                &[&32_i64],
            )
            .await
            .context("delete aggregate cdc row")?;
        wait_for_bidder_aggregate(pg_port, &mv_name, 900, 1, 150).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;
    let _ = client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication};"))
        .await;
    let _ = client
        .batch_execute(&format!("DROP TABLE IF EXISTS {table};"))
        .await;
    test_result
}

#[derive(Clone)]
struct SinkCaptureState {
    sender: mpsc::Sender<Value>,
}

async fn sink_collect(
    State(state): State<SinkCaptureState>,
    Json(payload): Json<Value>,
) -> StatusCode {
    if state.sender.send(payload).await.is_err() {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    StatusCode::OK
}

async fn spawn_sink_server(
    port: u16,
    sender: mpsc::Sender<Value>,
) -> Result<tokio::task::JoinHandle<()>> {
    let state = SinkCaptureState { sender };
    let app = Router::new()
        .route("/collect", post(sink_collect))
        .with_state(state);
    let listener = TokioTcpListener::bind(("127.0.0.1", port))
        .await
        .with_context(|| format!("bind sink receiver on port {port}"))?;
    Ok(tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            tracing::error!(error = %err, "sink receiver server exited");
        }
    }))
}

fn payload_to_rows(payload: Value) -> Vec<Value> {
    match payload {
        Value::Array(rows) => rows,
        row => vec![row],
    }
}

async fn spawn_node(
    config_path: &Path,
    data_dir: &Path,
    pg_port: u16,
    mv_sql: Option<&str>,
) -> Result<Child> {
    spawn_node_with_args(config_path, data_dir, pg_port, mv_sql, &[]).await
}

async fn spawn_node_with_args(
    config_path: &Path,
    data_dir: &Path,
    pg_port: u16,
    mv_sql: Option<&str>,
    extra_args: &[String],
) -> Result<Child> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_floe-node"));
    cmd.arg("run");
    if pg_port > 0 {
        cmd.arg("--pgwire-addr").arg(format!("127.0.0.1:{pg_port}"));
    } else {
        cmd.arg("--disable-pgwire");
    }
    cmd.arg("--data-dir")
        .arg(data_dir)
        .arg("--config")
        .arg(config_path.to_string_lossy().to_string())
        .stdout(if std::env::var_os("FLOE_TEST_NODE_STDERR").is_some() {
            Stdio::inherit()
        } else {
            Stdio::null()
        })
        .stderr(if std::env::var_os("FLOE_TEST_NODE_STDERR").is_some() {
            Stdio::inherit()
        } else {
            Stdio::null()
        });
    if !extra_args.iter().any(|arg| arg == "--admin-port") {
        cmd.arg("--admin-port").arg("0");
    }
    for arg in extra_args {
        cmd.arg(arg);
    }
    if let Some(sql) = mv_sql {
        cmd.arg("--mv-query").arg(sql);
    }
    cmd.spawn().context("spawn floe-node")
}

async fn stop_child(child: &mut Child, signal: &str) {
    if let Some(pid) = child.id() {
        let _ = std::process::Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(pid.to_string())
            .status();
    }
    let _ = child.wait().await;
}

async fn wait_for_healthz(addr: &str) -> Result<()> {
    let client = reqwest::Client::new();
    for attempt in 0..60 {
        match client.get(format!("{addr}/healthz")).send().await {
            Ok(response) if response.status() == StatusCode::OK => return Ok(()),
            Ok(_) | Err(_) if attempt < 59 => sleep(Duration::from_millis(100)).await,
            Ok(response) => bail!("healthz returned {}", response.status()),
            Err(err) => bail!("healthz never became ready: {err}"),
        }
    }
    unreachable!("loop either returns success or bails")
}

async fn wait_for_admin_metrics_contains(admin_port: u16, needle: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{admin_port}/metrics");
    for _ in 0..120 {
        match client.get(&url).send().await {
            Ok(response) if response.status() == StatusCode::OK => {
                let body = response.text().await.context("read admin metrics body")?;
                if body.contains(needle) {
                    return Ok(());
                }
            }
            Ok(_) | Err(_) => {}
        }
        sleep(Duration::from_millis(100)).await;
    }
    bail!("timed out waiting for admin metrics to contain {needle}");
}

async fn post_bid(addr: &str, auction: i64, bidder: i64, price: i64) -> Result<()> {
    let payload = json!({
        "source": "nexmark_bid",
        "data": {
            "auction": auction,
            "bidder": bidder,
            "price": price,
            "channel": "web",
            "url": "http://example.com",
            "date_time": 1_700_000_000_i64 + auction,
            "extra": "acceptance"
        }
    });
    let response = reqwest::Client::new()
        .post(format!("{addr}/ingest"))
        .json(&payload)
        .send()
        .await
        .context("post acceptance bid payload")?;
    if response.status() != StatusCode::OK {
        bail!("ingest returned {}", response.status());
    }
    Ok(())
}

async fn create_kafka_topic(brokers: &str, topic: &str) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .context("create kafka admin client")?;
    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    let results = admin
        .create_topics(&[new_topic], &AdminOptions::new())
        .await
        .context("create kafka restart topic")?;
    for result in results {
        result
            .map(|_| ())
            .map_err(|(topic, err)| anyhow::anyhow!("create kafka topic {topic}: {err}"))?;
    }
    Ok(())
}

async fn produce_auction(
    producer: &FutureProducer,
    topic: &str,
    id: i64,
    seller: i64,
) -> Result<()> {
    let payload = json!({
        "source": "nexmark_auction",
        "data": {
            "id": id,
            "seller": seller,
            "category": 17,
            "initial_bid": 100,
            "reserve": 500,
            "item_name": "restart-test",
            "description": "restart-test",
            "expires": 1_700_100_000_i64 + id,
            "date_time": 1_700_000_000_i64 + id,
            "extra": "kafka_restart"
        }
    })
    .to_string();
    let record = FutureRecord::<(), _>::to(topic).payload(&payload);
    producer
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(err, _)| err)
        .context("produce kafka auction")?;
    Ok(())
}

async fn produce_bid(
    producer: &FutureProducer,
    topic: &str,
    auction: i64,
    bidder: i64,
    price: i64,
) -> Result<()> {
    let payload = json!({
        "source": "nexmark_bid",
        "data": {
            "auction": auction,
            "bidder": bidder,
            "price": price,
            "channel": "web",
            "url": "http://example.com",
            "date_time": 1_700_000_000_i64 + auction,
            "extra": "kafka_restart"
        }
    })
    .to_string();
    let record = FutureRecord::<(), _>::to(topic).payload(&payload);
    producer
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(err, _)| err)
        .context("produce kafka bid")?;
    Ok(())
}

async fn wait_for_auction_count_at_least(
    pg_port: u16,
    auction: i64,
    min_count: i64,
) -> Result<i64> {
    for _ in 0..80 {
        match query_auction_count(pg_port, auction).await {
            Ok(count) if count >= min_count => return Ok(count),
            Ok(_) | Err(_) => sleep(Duration::from_millis(100)).await,
        }
    }
    bail!("timed out waiting for mv_acceptance_bid count >= {min_count} for auction={auction}");
}

async fn wait_for_join_count_at_least(pg_port: u16, auction: i64, min_count: i64) -> Result<i64> {
    for _ in 0..120 {
        match query_join_count(pg_port, auction).await {
            Ok(count) if count >= min_count => return Ok(count),
            Ok(_) | Err(_) => sleep(Duration::from_millis(100)).await,
        }
    }
    bail!("timed out waiting for mv_acceptance_join count >= {min_count} for auction={auction}");
}

async fn query_auction_count(pg_port: u16, auction: i64) -> Result<i64> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        NoTls,
    )
    .await
    .context("connect to pgwire")?;
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM mv_acceptance_bid WHERE auction = $1",
            &[&auction],
        )
        .await
        .context("query acceptance mv count")?;
    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(row.get::<_, i64>(0))
}

async fn query_join_count(pg_port: u16, auction: i64) -> Result<i64> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        NoTls,
    )
    .await
    .context("connect to pgwire")?;
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM mv_acceptance_join WHERE auction = $1",
            &[&auction],
        )
        .await
        .context("query acceptance join mv count")?;
    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(row.get::<_, i64>(0))
}

async fn wait_for_mv_price_count_at_least(
    pg_port: u16,
    mv_name: &str,
    auction: i64,
    price: i64,
    min_count: i64,
) -> Result<i64> {
    for _ in 0..120 {
        match query_mv_price_count(pg_port, mv_name, auction, price).await {
            Ok(count) if count >= min_count => return Ok(count),
            Ok(_) | Err(_) => sleep(Duration::from_millis(100)).await,
        }
    }
    bail!(
        "timed out waiting for {mv_name} count >= {min_count} for auction={auction}, price={price}"
    );
}

async fn query_mv_price_count(
    pg_port: u16,
    mv_name: &str,
    auction: i64,
    price: i64,
) -> Result<i64> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        NoTls,
    )
    .await
    .context("connect to pgwire")?;
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let query = format!("SELECT COUNT(*) FROM {mv_name} WHERE auction = $1 AND price = $2");
    let row = client
        .query_one(&query, &[&auction, &price])
        .await
        .with_context(|| format!("query {mv_name} count"))?;
    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(row.get::<_, i64>(0))
}

async fn wait_for_auction_seller_count_at_least(
    pg_port: u16,
    mv_name: &str,
    id: i64,
    seller: i64,
    min_count: i64,
) -> Result<i64> {
    for _ in 0..120 {
        match query_auction_seller_count(pg_port, mv_name, id, seller).await {
            Ok(count) if count >= min_count => return Ok(count),
            Ok(_) | Err(_) => sleep(Duration::from_millis(100)).await,
        }
    }
    bail!("timed out waiting for {mv_name} count >= {min_count} for id={id}, seller={seller}");
}

async fn query_auction_seller_count(
    pg_port: u16,
    mv_name: &str,
    id: i64,
    seller: i64,
) -> Result<i64> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        NoTls,
    )
    .await
    .context("connect to pgwire")?;
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let query = format!("SELECT COUNT(*) FROM {mv_name} WHERE id = $1 AND seller = $2");
    let row = client
        .query_one(&query, &[&id, &seller])
        .await
        .with_context(|| format!("query {mv_name} auction count"))?;
    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(row.get::<_, i64>(0))
}

async fn wait_for_join_mv_count_at_least(
    pg_port: u16,
    mv_name: &str,
    auction: i64,
    bidder: i64,
    seller: i64,
    min_count: i64,
) -> Result<i64> {
    for _ in 0..120 {
        match query_join_mv_count(pg_port, mv_name, auction, bidder, seller).await {
            Ok(count) if count >= min_count => return Ok(count),
            Ok(_) | Err(_) => sleep(Duration::from_millis(100)).await,
        }
    }
    bail!(
        "timed out waiting for {mv_name} count >= {min_count} for auction={auction}, bidder={bidder}, seller={seller}"
    );
}

async fn query_join_mv_count(
    pg_port: u16,
    mv_name: &str,
    auction: i64,
    bidder: i64,
    seller: i64,
) -> Result<i64> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        NoTls,
    )
    .await
    .context("connect to pgwire")?;
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let query = format!(
        "SELECT COUNT(*) FROM {mv_name} WHERE auction = $1 AND bidder = $2 AND seller = $3"
    );
    let row = client
        .query_one(&query, &[&auction, &bidder, &seller])
        .await
        .with_context(|| format!("query {mv_name} join count"))?;
    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(row.get::<_, i64>(0))
}

async fn wait_for_bidder_aggregate(
    pg_port: u16,
    mv_name: &str,
    bidder: i64,
    expected_count: i64,
    expected_total: i64,
) -> Result<(i64, i64)> {
    for _ in 0..120 {
        match query_bidder_aggregate(pg_port, mv_name, bidder).await {
            Ok(Some((bid_count, total_price)))
                if bid_count == expected_count && total_price == expected_total =>
            {
                return Ok((bid_count, total_price));
            }
            Ok(_) | Err(_) => sleep(Duration::from_millis(100)).await,
        }
    }
    bail!(
        "timed out waiting for {mv_name} aggregate bidder={bidder} count={expected_count} total={expected_total}"
    );
}

async fn query_bidder_aggregate(
    pg_port: u16,
    mv_name: &str,
    bidder: i64,
) -> Result<Option<(i64, i64)>> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        NoTls,
    )
    .await
    .context("connect to pgwire")?;
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let query = format!("SELECT bid_count, total_price FROM {mv_name} WHERE bidder = $1");
    let rows = client
        .query(&query, &[&bidder])
        .await
        .with_context(|| format!("query {mv_name} aggregate"))?;
    let aggregate = match rows.as_slice() {
        [] => None,
        [row] => {
            let bid_count = row.try_get::<_, i64>(0).context("decode bid_count")?;
            let total_price = row.try_get::<_, i64>(1).context("decode total_price")?;
            Some((bid_count, total_price))
        }
        rows => bail!(
            "{mv_name} returned {} aggregate rows for bidder={bidder}",
            rows.len()
        ),
    };
    drop(client);
    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(aggregate)
}

fn sql_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

async fn cleanup_postgres_sink_acceptance(
    client: &tokio_postgres::Client,
    publication: &str,
    slot: &str,
    source_table: &str,
    target_table: &str,
) {
    let _ = client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication};"))
        .await;
    let _ = client
        .execute(
            "SELECT pg_drop_replication_slot($1)
             WHERE EXISTS (
               SELECT 1
               FROM pg_replication_slots
               WHERE slot_name = $1
             )",
            &[&slot],
        )
        .await;
    let _ = client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {target_table};
             DROP TABLE IF EXISTS {source_table};"
        ))
        .await;
}

async fn wait_for_postgres_sink_row(
    client: &tokio_postgres::Client,
    table: &str,
    id: i64,
    amount: i64,
    note: Option<&str>,
) -> Result<()> {
    let mut last_seen = None;
    for _ in 0..120 {
        let rows = client
            .query(
                &format!("SELECT amount, note FROM {table} WHERE id = $1"),
                &[&id],
            )
            .await
            .with_context(|| format!("query Postgres sink table {table}"))?;
        match rows.as_slice() {
            [row] => {
                let row_amount = row.try_get::<_, i64>(0).context("decode sink amount")?;
                let row_note = row
                    .try_get::<_, Option<String>>(1)
                    .context("decode sink note")?;
                if row_amount == amount && row_note.as_deref() == note {
                    return Ok(());
                }
                last_seen = Some(format!("amount={row_amount}, note={row_note:?}"));
            }
            rows => {
                last_seen = Some(format!("{} rows", rows.len()));
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    bail!(
        "timed out waiting for Postgres sink table {table} id={id} amount={amount} note={note:?}; last seen: {:?}",
        last_seen
    )
}

async fn wait_for_postgres_sink_absent(
    client: &tokio_postgres::Client,
    table: &str,
    id: i64,
) -> Result<()> {
    for _ in 0..120 {
        let row = client
            .query_one(
                &format!("SELECT COUNT(*)::BIGINT FROM {table} WHERE id = $1"),
                &[&id],
            )
            .await
            .with_context(|| format!("query Postgres sink table {table} absence"))?;
        let count = row.try_get::<_, i64>(0).context("decode sink count")?;
        if count == 0 {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }
    bail!("timed out waiting for Postgres sink table {table} id={id} to be absent")
}

async fn wait_for_rows_matching(
    path: &Path,
    predicate: impl Fn(&Value) -> bool,
) -> Result<Vec<Value>> {
    for _ in 0..120 {
        let rows = read_rows(path).await?;
        if rows.iter().any(&predicate) {
            return Ok(rows);
        }
        sleep(Duration::from_millis(100)).await;
    }
    bail!("predicate did not match rows in {}", path.to_string_lossy())
}

async fn read_rows(path: &Path) -> Result<Vec<Value>> {
    let contents = match tokio::fs::read_to_string(path).await {
        Ok(value) => value,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    let mut rows = Vec::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        rows.push(serde_json::from_str::<Value>(line).context("parse jsonl row")?);
    }
    Ok(rows)
}
