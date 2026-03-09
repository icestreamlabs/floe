use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use futures::TryStreamExt;
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokio::time::sleep;
use tokio_postgres::{NoTls, SimpleQueryMessage};

const MV_NAME: &str = "mv_kafka_redpanda_million";

// High-coverage row-wise query for 1M-row throughput:
// projection, filter, arithmetic, CASE, LOWER, REGEXP_EXTRACT, SPLIT_INDEX, DATE_FORMAT.
const MV_SQL: &str = r#"
CREATE MATERIALIZED VIEW mv_kafka_redpanda_million AS
SELECT
  auction,
  bidder,
  price * 89 / 100 AS normalized_price,
  CASE
    WHEN lower(channel) = 'apple' THEN lower(channel)
    WHEN lower(channel) = 'google' THEN lower(channel)
    WHEN lower(channel) = 'facebook' THEN lower(channel)
    WHEN lower(channel) = 'baidu' THEN lower(channel)
    ELSE REGEXP_EXTRACT(channel, '(web)', 1)
  END AS channel_id,
  SPLIT_INDEX(url, '/', 3) AS dir1,
  DATE_FORMAT(date_time, 'yyyy-MM-dd') AS day
FROM nexmark_bid
WHERE price >= 0
"#;

const TOTAL_ROWS: usize = 1_000_000;
const SAMPLE_ROW_COUNT: usize = 20;
const CHECKSUM_MOD: i128 = 2_305_843_009_213_693_951;
const BASE_TS_MS: i64 = 1_700_000_000_000;
const DAY_UTC: &str = "2023-11-14";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpectedRow {
    auction: i64,
    bidder: i64,
    normalized_price: i64,
    channel_id: String,
    dir1: String,
    day: String,
}

#[derive(Debug, Clone, Default)]
struct Metrics {
    row_count: i64,
    sum_normalized_price: i128,
    checksum: i128,
}

impl Metrics {
    fn apply(&mut self, row: &ExpectedRow, op: i64) {
        self.row_count += op;
        self.sum_normalized_price += i128::from(op) * i128::from(row.normalized_price);
        self.checksum =
            (self.checksum + i128::from(op) * row_checksum(row)).rem_euclid(CHECKSUM_MOD);
    }
}

#[derive(Debug, Clone, Default)]
struct ExpectedDataset {
    generated_rows: usize,
    metrics: Metrics,
    sample_rows_by_bidder: BTreeMap<i64, ExpectedRow>,
}

#[tokio::test]
#[ignore = "requires Kafka/Redpanda and processes a 1,000,000-row dataset"]
async fn redpanda_kafka_million_row_e2e() -> Result<()> {
    let brokers =
        std::env::var("FLOE_REDPANDA_BROKERS").unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let artifacts_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/e2e_artifacts")
        .join(format!("redpanda_million_{run_id}"));
    std::fs::create_dir_all(&artifacts_dir).context("create artifact dir")?;

    let dataset_path = artifacts_dir.join("dataset_1m.jsonl");
    let config_path = artifacts_dir.join("node_config.json");
    let stdout_log_path = artifacts_dir.join("floe-node.stdout.log");
    let stderr_log_path = artifacts_dir.join("floe-node.stderr.log");
    let pg_port = find_unused_port()?;
    let input_topic = format!("floe_redpanda_in_{run_id}");
    let output_topic = format!("floe_redpanda_out_{run_id}");
    let group_id = format!("floe-redpanda-e2e-{run_id}");
    let mv_max_pending_deltas = std::env::var("FLOE_E2E_MV_MAX_PENDING_DELTAS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0);
    let mv_max_delay_ms = std::env::var("FLOE_E2E_MV_MAX_DELAY_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0);
    let mv_flush_enabled = mv_max_pending_deltas.is_some() || mv_max_delay_ms.is_some();
    let connector_max_messages_per_tick = std::env::var("FLOE_E2E_CONNECTOR_MAX_MESSAGES_PER_TICK")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16_384);
    let sink_batch_rows = std::env::var("FLOE_E2E_SINK_BATCH_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16_384);
    let ingest_batch_size = std::env::var("FLOE_E2E_INGEST_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16_384);
    let ingest_batch_per_source = std::env::var("FLOE_E2E_INGEST_BATCH_PER_SOURCE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16_384);
    let ingest_batch_per_connector = std::env::var("FLOE_E2E_INGEST_BATCH_PER_CONNECTOR")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16_384);
    let slatedb_flush_interval_ms = std::env::var("FLOE_E2E_SLATEDB_FLUSH_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok());

    eprintln!("artifacts_dir={}", artifacts_dir.display());
    eprintln!("dataset_path={}", dataset_path.display());
    eprintln!("brokers={brokers} input_topic={input_topic} output_topic={output_topic}");

    let expected = {
        let dataset_path = dataset_path.clone();
        tokio::task::spawn_blocking(move || generate_dataset_file(&dataset_path))
            .await
            .context("join dataset generation task")??
    };
    if expected.generated_rows != TOTAL_ROWS {
        bail!(
            "dataset generator wrote {} rows, expected {}",
            expected.generated_rows,
            TOTAL_ROWS
        );
    }

    let config = serde_json::json!({
        "connectors": [
            {
                "type": "kafka",
                "brokers": brokers,
                "topics": [input_topic],
                "group_id": group_id,
                "poll_ms": 10,
                "max_messages_per_tick": connector_max_messages_per_tick
            }
        ],
        "sinks": [
            {
                "type": "kafka",
                "name": "kafka_sink_million",
                "brokers": brokers,
                "topic": output_topic,
                "mv": MV_NAME,
                "with_snapshot": false,
                "batch_rows": sink_batch_rows,
                "batch_bytes": 16777216,
                "queue_capacity": 65536,
                "retry_max_attempts": 8,
                "retry_base_ms": 50,
                "retry_max_backoff_ms": 1000
            }
        ],
        "storage": {
            "await_durable": false
        },
        "runtime": {
            "mv_flush": {
                "enabled": mv_flush_enabled,
                "max_pending_deltas": mv_max_pending_deltas,
                "max_delay_ms": mv_max_delay_ms
            }
        }
    });
    eprintln!("runtime.mv_flush.enabled={mv_flush_enabled}");
    if let Some(max_pending_deltas) = mv_max_pending_deltas {
        eprintln!("runtime.mv_flush.max_pending_deltas={max_pending_deltas}");
    }
    if let Some(max_delay_ms) = mv_max_delay_ms {
        eprintln!("runtime.mv_flush.max_delay_ms={max_delay_ms}");
    }
    eprintln!("connector.max_messages_per_tick={connector_max_messages_per_tick}");
    eprintln!("sink.batch_rows={sink_batch_rows}");
    eprintln!("run.ingest_batch_size={ingest_batch_size}");
    eprintln!("run.ingest_batch_per_source={ingest_batch_per_source}");
    eprintln!("run.ingest_batch_per_connector={ingest_batch_per_connector}");
    if let Some(flush_interval_ms) = slatedb_flush_interval_ms {
        eprintln!("run.slatedb_flush_interval_ms={flush_interval_ms}");
    }
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write node config")?;

    let mut child = spawn_node(
        &config_path,
        pg_port,
        Some(MV_SQL),
        &stdout_log_path,
        &stderr_log_path,
        ingest_batch_size,
        ingest_batch_per_source,
        ingest_batch_per_connector,
        slatedb_flush_interval_ms,
    )
    .await?;

    let test_result = async {
        wait_for_pgwire(pg_port, &mut child, &stderr_log_path).await?;

        let (pgwire_ready_tx, pgwire_ready_rx) = oneshot::channel();
        let expected_for_pgwire = expected.clone();
        let pgwire_task = tokio::spawn(async move {
            verify_pgwire_tail_metrics(
                pg_port,
                expected_for_pgwire,
                Duration::from_secs(1800),
                pgwire_ready_tx,
            )
            .await
        });
        pgwire_ready_rx
            .await
            .context("wait for pgwire tail consumer readiness")?;

        let produce_started = Instant::now();
        {
            let dataset_path = dataset_path.clone();
            let brokers = brokers.clone();
            let input_topic = input_topic.clone();
            tokio::task::spawn_blocking(move || {
                produce_dataset_file(&dataset_path, &brokers, &input_topic)
            })
            .await
            .context("join kafka producer task")??;
        }
        eprintln!(
            "kafka production completed in {:?}",
            produce_started.elapsed()
        );

        let observed_sink = {
            let brokers = brokers.clone();
            let output_topic = output_topic.clone();
            let expected = expected.clone();
            tokio::task::spawn_blocking(move || {
                consume_sink_metrics(
                    &brokers,
                    &output_topic,
                    expected.metrics.row_count,
                    Duration::from_secs(1800),
                )
            })
            .await
            .context("join sink consumer task")??
        };

        assert_eq!(
            observed_sink.row_count, expected.metrics.row_count,
            "sink row count mismatch"
        );
        assert_eq!(
            observed_sink.sum_normalized_price, expected.metrics.sum_normalized_price,
            "sink normalized_price sum mismatch"
        );
        assert_eq!(
            observed_sink.checksum, expected.metrics.checksum,
            "sink checksum mismatch"
        );

        pgwire_task
            .await
            .context("join pgwire tail consumer task")??;

        eprintln!(
            "verified rows={} sum_normalized_price={} checksum={}",
            expected.metrics.row_count,
            expected.metrics.sum_normalized_price,
            expected.metrics.checksum
        );

        Ok(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    test_result
}

fn generate_dataset_file(path: &Path) -> Result<ExpectedDataset> {
    let file =
        File::create(path).with_context(|| format!("create dataset file {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
    let mut expected = ExpectedDataset::default();
    let sample_indices = compute_sample_indices(TOTAL_ROWS, SAMPLE_ROW_COUNT);

    for bid_idx in 1..=TOTAL_ROWS {
        let bid_idx_i64 = i64::try_from(bid_idx).unwrap_or_default();
        let auction = i64::try_from((bid_idx - 1) % 10_000 + 1).unwrap_or_default();
        let bidder = 10_000_i64 + bid_idx_i64;
        let price = 1_000_i64 + (bid_idx_i64 % 50_000);
        let channel = match bid_idx % 5 {
            0 => "web",
            1 => "apple",
            2 => "google",
            3 => "facebook",
            _ => "baidu",
        };
        let dir1 = format!("dir{}", auction % 11);
        let url = if channel == "web" {
            format!(
                "https://example.com/{dir1}/item/{bid_idx}?q=1&channel_id={}",
                bid_idx % 97
            )
        } else {
            format!("https://example.com/{dir1}/item/{bid_idx}?q=1")
        };

        writeln!(
            writer,
            "{{\"source\":\"nexmark_bid\",\"data\":{{\"auction\":{auction},\"bidder\":{bidder},\"price\":{price},\"channel\":\"{channel}\",\"url\":\"{url}\",\"date_time\":{},\"extra\":\"bid_extra_{bid_idx}\"}}}}",
            BASE_TS_MS + bid_idx_i64,
        )
        .context("write bid row")?;
        expected.generated_rows += 1;

        let channel_id = channel.to_string();
        let row = ExpectedRow {
            auction,
            bidder,
            normalized_price: price * 89 / 100,
            channel_id,
            dir1,
            day: DAY_UTC.to_string(),
        };
        expected.metrics.apply(&row, 1);
        if sample_indices.contains(&bid_idx) {
            expected.sample_rows_by_bidder.insert(row.bidder, row);
        }
    }

    if expected.sample_rows_by_bidder.len() != sample_indices.len() {
        bail!(
            "captured {} sample rows, expected {}",
            expected.sample_rows_by_bidder.len(),
            sample_indices.len()
        );
    }

    writer.flush().context("flush dataset writer")?;
    Ok(expected)
}

fn compute_sample_indices(total_rows: usize, sample_count: usize) -> BTreeSet<usize> {
    let mut out = BTreeSet::new();
    if total_rows == 0 || sample_count == 0 {
        return out;
    }
    if sample_count == 1 {
        out.insert(total_rows);
        return out;
    }

    let denominator = sample_count - 1;
    for i in 0..sample_count {
        let idx = 1 + (i * (total_rows - 1)) / denominator;
        out.insert(idx);
    }
    out
}

fn produce_dataset_file(dataset_path: &Path, brokers: &str, topic: &str) -> Result<()> {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("queue.buffering.max.messages", "200000")
        .set("queue.buffering.max.kbytes", "524288")
        .create()
        .context("create kafka producer")?;

    let file = File::open(dataset_path)
        .with_context(|| format!("open dataset file {}", dataset_path.display()))?;
    let reader = BufReader::with_capacity(8 * 1024 * 1024, file);

    let mut produced = 0usize;
    for line in reader.lines() {
        let line = line.context("read dataset line")?;
        loop {
            match producer.send(BaseRecord::<(), _>::to(topic).payload(&line)) {
                Ok(_) => break,
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _record)) => {
                    producer.poll(Duration::from_millis(50));
                }
                Err((err, _record)) => {
                    return Err(err).context("produce kafka message");
                }
            }
        }

        produced += 1;
        if produced % 10_000 == 0 {
            producer.poll(Duration::from_millis(0));
        }
        if produced % 100_000 == 0 {
            eprintln!("produced {produced} rows to topic={topic}");
        }
    }

    producer
        .flush(Duration::from_secs(120))
        .context("flush kafka producer")?;

    if produced != TOTAL_ROWS {
        bail!("produced {produced} rows, expected {TOTAL_ROWS}");
    }

    Ok(())
}

fn consume_sink_metrics(
    brokers: &str,
    topic: &str,
    expected_rows: i64,
    timeout: Duration,
) -> Result<Metrics> {
    let group_id = format!(
        "floe-redpanda-sink-verify-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", &group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("create kafka consumer")?;
    consumer
        .subscribe(&[topic])
        .with_context(|| format!("subscribe sink topic {topic}"))?;

    let mut metrics = Metrics::default();
    let start = Instant::now();
    let mut last_message_at = Instant::now();
    let mut messages_seen = 0usize;

    while start.elapsed() < timeout {
        match consumer.poll(Duration::from_millis(250)) {
            Some(Ok(message)) => {
                let Some(payload) = message.payload() else {
                    continue;
                };
                let value: Value = serde_json::from_slice(payload).context("parse sink json")?;
                let op = value
                    .get("__op")
                    .and_then(Value::as_i64)
                    .context("sink row missing __op")?;
                if op != 1 {
                    bail!("unexpected sink __op={op}; expected insert-only output");
                }

                let row = row_from_json(&value)?;
                metrics.apply(&row, op);
                messages_seen += 1;
                last_message_at = Instant::now();

                if messages_seen % 100_000 == 0 {
                    eprintln!("consumed {messages_seen} sink rows from topic={topic}");
                }
            }
            Some(Err(KafkaError::MessageConsumption(
                RDKafkaErrorCode::UnknownTopicOrPartition,
            ))) => {
                std::thread::sleep(Duration::from_millis(250));
            }
            Some(Err(err)) => return Err(err).context("poll sink topic"),
            None => {
                if metrics.row_count >= expected_rows
                    && last_message_at.elapsed() >= Duration::from_secs(3)
                {
                    break;
                }
            }
        }
    }

    if metrics.row_count != expected_rows {
        bail!(
            "sink did not reach expected row count: observed={}, expected={expected_rows}",
            metrics.row_count
        );
    }

    Ok(metrics)
}

async fn verify_pgwire_tail_metrics(
    pg_port: u16,
    expected: ExpectedDataset,
    timeout: Duration,
    ready_tx: oneshot::Sender<()>,
) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        NoTls,
    )
    .await
    .context("connect to pgwire")?;
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    let tail_sql = format!("TAIL {MV_NAME}");
    let mut stream = Box::pin(
        client
            .simple_query_raw(&tail_sql)
            .await
            .context("start pgwire tail")?,
    );
    let _ = ready_tx.send(());

    let mut observed_samples: BTreeMap<i64, ExpectedRow> = BTreeMap::new();
    let start = Instant::now();
    let mut tail_rows_seen: i64 = 0;

    while start.elapsed() < timeout {
        match tokio::time::timeout(Duration::from_millis(250), stream.try_next()).await {
            Ok(Ok(Some(SimpleQueryMessage::Row(row)))) => {
                let Some(op_raw) = row.get(1) else {
                    continue;
                };
                let op: i16 = op_raw.parse().context("parse pgwire __op as i16")?;
                if op != 1 {
                    bail!("unexpected pgwire tail __op={op}; expected insert-only output");
                }
                tail_rows_seen += 1;

                let bidder: i64 = row
                    .get(4)
                    .context("pgwire tail row missing bidder")?
                    .parse()
                    .context("parse pgwire bidder as i64")?;

                if expected.sample_rows_by_bidder.contains_key(&bidder)
                    && !observed_samples.contains_key(&bidder)
                {
                    let actual = ExpectedRow {
                        auction: row
                            .get(3)
                            .context("pgwire tail row missing auction")?
                            .parse()
                            .context("parse pgwire auction as i64")?,
                        bidder,
                        normalized_price: row
                            .get(5)
                            .context("pgwire tail row missing normalized_price")?
                            .parse()
                            .context("parse pgwire normalized_price as i64")?,
                        channel_id: row
                            .get(6)
                            .context("pgwire tail row missing channel_id")?
                            .to_string(),
                        dir1: row
                            .get(7)
                            .context("pgwire tail row missing dir1")?
                            .to_string(),
                        day: row
                            .get(8)
                            .context("pgwire tail row missing day")?
                            .to_string(),
                    };
                    observed_samples.insert(bidder, actual);
                    eprintln!(
                        "captured pgwire sample bidder={} ({}/{})",
                        bidder,
                        observed_samples.len(),
                        expected.sample_rows_by_bidder.len()
                    );
                    if observed_samples.len() == expected.sample_rows_by_bidder.len() {
                        break;
                    }
                }
                if tail_rows_seen % 100_000 == 0 {
                    eprintln!("consumed {tail_rows_seen} pgwire tail rows from mv={MV_NAME}");
                }
            }
            Ok(Ok(Some(SimpleQueryMessage::RowDescription(_))))
            | Ok(Ok(Some(SimpleQueryMessage::CommandComplete(_)))) => {}
            Ok(Ok(Some(_))) => {}
            Ok(Ok(None)) => break,
            Ok(Err(err)) => return Err(err).context("read pgwire tail row"),
            Err(_) => {}
        }
    }

    if observed_samples.len() != expected.sample_rows_by_bidder.len() {
        let missing: Vec<i64> = expected
            .sample_rows_by_bidder
            .keys()
            .filter(|bidder| !observed_samples.contains_key(*bidder))
            .copied()
            .collect();
        bail!(
            "pgwire tail sample row count mismatch: observed={}, expected={}, tail_rows_seen={}, missing_bidders={missing:?}",
            observed_samples.len(),
            expected.sample_rows_by_bidder.len(),
            tail_rows_seen
        );
    }
    for (bidder, expected_row) in &expected.sample_rows_by_bidder {
        let actual = observed_samples
            .get(bidder)
            .with_context(|| format!("missing pgwire tail sample for bidder={bidder}"))?;
        if actual != expected_row {
            bail!(
                "pgwire tail sample mismatch for bidder={}: actual={actual:?}, expected={expected_row:?}",
                bidder
            );
        }
    }

    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(())
}

async fn spawn_node(
    config_path: &Path,
    pg_port: u16,
    mv_sql: Option<&str>,
    stdout_log_path: &Path,
    stderr_log_path: &Path,
    ingest_batch_size: usize,
    ingest_batch_per_source: usize,
    ingest_batch_per_connector: usize,
    slatedb_flush_interval_ms: Option<u64>,
) -> Result<Child> {
    let stdout_log = File::create(stdout_log_path)
        .with_context(|| format!("create {}", stdout_log_path.display()))?;
    let stderr_log = File::create(stderr_log_path)
        .with_context(|| format!("create {}", stderr_log_path.display()))?;

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_floe-node"));
    cmd.env("FLOE_PG_ADDR", format!("127.0.0.1:{pg_port}"))
        .env("FLOE_ADMIN_PORT", "0")
        .arg("run")
        .arg("--ingest-queue-capacity")
        .arg("262144")
        .arg("--ingest-batch-size")
        .arg(ingest_batch_size.to_string())
        .arg("--ingest-batch-per-source")
        .arg(ingest_batch_per_source.to_string())
        .arg("--ingest-batch-per-connector")
        .arg(ingest_batch_per_connector.to_string())
        .arg("--slatedb-l0-sst-bytes")
        .arg("1073741824")
        .arg("--slatedb-max-unflushed-bytes")
        .arg("8589934592")
        .arg("--mv-retain-last")
        .arg("256")
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::from(stdout_log))
        .stderr(Stdio::from(stderr_log));
    if let Some(flush_interval_ms) = slatedb_flush_interval_ms {
        cmd.arg("--slatedb-flush-interval-ms")
            .arg(flush_interval_ms.to_string());
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

async fn wait_for_pgwire(pg_port: u16, child: &mut Child, stderr_log_path: &Path) -> Result<()> {
    let addr = format!("127.0.0.1:{pg_port}");
    for attempt in 0..120 {
        if let Some(status) = child.try_wait().context("poll floe-node process status")? {
            let stderr_tail = read_log_tail(stderr_log_path, 120).unwrap_or_else(|_| {
                format!("failed to read stderr log {}", stderr_log_path.display())
            });
            bail!(
                "floe-node exited before pgwire became ready (status={status}); stderr tail:\n{stderr_tail}"
            );
        }
        match TcpStream::connect(&addr).await {
            Ok(stream) => {
                drop(stream);
                return Ok(());
            }
            Err(err) if attempt < 119 => {
                if attempt % 20 == 0 {
                    eprintln!("waiting for pgwire at {addr}: {err}");
                }
                sleep(Duration::from_millis(250)).await;
            }
            Err(err) => bail!("pgwire listener at {addr} never became ready: {err}"),
        }
    }
    unreachable!("loop returns or bails")
}

fn row_from_json(value: &Value) -> Result<ExpectedRow> {
    let object = value
        .as_object()
        .context("sink payload must be an object")?;

    Ok(ExpectedRow {
        auction: read_i64(object, "auction")?,
        bidder: read_i64(object, "bidder")?,
        normalized_price: read_i64(object, "normalized_price")?,
        channel_id: read_string(object, "channel_id")?,
        dir1: read_string(object, "dir1")?,
        day: read_string(object, "day")?,
    })
}

fn read_i64(map: &serde_json::Map<String, Value>, key: &str) -> Result<i64> {
    map.get(key)
        .and_then(Value::as_i64)
        .with_context(|| format!("missing int64 field '{key}'"))
}

fn read_string(map: &serde_json::Map<String, Value>, key: &str) -> Result<String> {
    map.get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .with_context(|| format!("missing string field '{key}'"))
}

fn row_checksum(row: &ExpectedRow) -> i128 {
    let mut acc = 17_i128;
    acc = mix(acc, i128::from(row.auction));
    acc = mix(acc, i128::from(row.bidder));
    acc = mix(acc, i128::from(row.normalized_price));
    acc = mix_string(acc, &row.channel_id);
    acc = mix_string(acc, &row.dir1);
    acc = mix_string(acc, &row.day);
    acc
}

fn mix(acc: i128, value: i128) -> i128 {
    (acc * 1_000_003 + value + 97).rem_euclid(CHECKSUM_MOD)
}

fn mix_string(mut acc: i128, value: &str) -> i128 {
    for byte in value.as_bytes() {
        acc = mix(acc, i128::from(*byte));
    }
    mix(acc, 31)
}

fn find_unused_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("bind ephemeral port")?;
    Ok(listener.local_addr().context("read ephemeral port")?.port())
}

fn read_log_tail(path: &Path, max_lines: usize) -> Result<String> {
    let contents =
        std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let lines = contents.lines().collect::<Vec<_>>();
    let start = lines.len().saturating_sub(max_lines);
    Ok(lines[start..].join("\n"))
}
