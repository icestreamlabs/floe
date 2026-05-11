use std::net::TcpListener;
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
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

    let mut child = spawn_node_with_env(
        &config_path,
        &data_dir,
        0,
        Some(BID_MV_SQL),
        &[("FLOE_DISABLE_PGWIRE".to_string(), "1".to_string())],
    )
    .await?;

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

    let mut child = spawn_node_with_env(
        &config_path,
        &data_dir,
        0,
        Some(BID_MV_SQL),
        &[("FLOE_DISABLE_PGWIRE".to_string(), "1".to_string())],
    )
    .await?;

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
    spawn_node_with_env(config_path, data_dir, pg_port, mv_sql, &[]).await
}

async fn spawn_node_with_env(
    config_path: &Path,
    data_dir: &Path,
    pg_port: u16,
    mv_sql: Option<&str>,
    extra_env: &[(String, String)],
) -> Result<Child> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_floe-node"));
    if pg_port > 0 {
        cmd.env("FLOE_PG_ADDR", format!("127.0.0.1:{pg_port}"));
    } else {
        cmd.env("FLOE_DISABLE_PGWIRE", "1");
    }
    cmd.env("FLOE_DATA_DIR", data_dir)
        .env("FLOE_ADMIN_PORT", "0")
        .arg("run")
        .arg("--config")
        .arg(config_path.to_string_lossy().to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (key, value) in extra_env {
        cmd.env(key, value);
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

fn find_unused_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("bind ephemeral port")?;
    Ok(listener.local_addr().context("read ephemeral port")?.port())
}
