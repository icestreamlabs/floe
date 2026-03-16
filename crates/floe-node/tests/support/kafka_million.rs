#![allow(dead_code)]

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
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokio::time::sleep;
use tokio_postgres::{NoTls, SimpleQueryMessage};

pub(crate) const BID_ROW_COUNT: usize = 1_000_000;
pub(crate) const JOIN_AUCTION_ROW_COUNT: usize = 10_000;
const DEFAULT_SAMPLE_ROW_COUNT: usize = 20;
const CHECKSUM_MOD: i128 = 2_305_843_009_213_693_951;
const BASE_TS_MS: i64 = 1_700_000_000_000;
const DAY_UTC: &str = "2023-11-14";
const DEFAULT_NO_SINK_END_COUNT_SETTLE_MS: u64 = 0;
const DEFAULT_NO_SINK_END_COUNT_POLL_MS: u64 = 250;

#[derive(Debug, Clone, Copy)]
pub(crate) enum FieldKind {
    Int64,
    String,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FieldSpec {
    pub(crate) name: &'static str,
    pub(crate) kind: FieldKind,
}

impl FieldSpec {
    pub(crate) const fn int64(name: &'static str) -> Self {
        Self {
            name,
            kind: FieldKind::Int64,
        }
    }

    pub(crate) const fn string(name: &'static str) -> Self {
        Self {
            name,
            kind: FieldKind::String,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExpectedValue {
    Int64(i64),
    String(String),
}

pub(crate) fn int64(value: i64) -> ExpectedValue {
    ExpectedValue::Int64(value)
}

pub(crate) fn string(value: impl Into<String>) -> ExpectedValue {
    ExpectedValue::String(value.into())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExpectedRow {
    values: Vec<ExpectedValue>,
}

impl ExpectedRow {
    pub(crate) fn new(values: Vec<ExpectedValue>) -> Self {
        Self { values }
    }
}

#[derive(Debug, Clone, Default)]
struct Metrics {
    row_count: i64,
    checksum: i128,
}

impl Metrics {
    fn apply(&mut self, row: &ExpectedRow, op: i64) {
        self.row_count += op;
        self.checksum =
            (self.checksum + i128::from(op) * row_checksum(row)).rem_euclid(CHECKSUM_MOD);
    }
}

#[derive(Debug, Clone, Default)]
struct ExpectedDataset {
    generated_rows: usize,
    metrics: Metrics,
    sample_rows_by_key: BTreeMap<String, ExpectedRow>,
}

#[derive(Debug, Clone, Copy)]
struct NoSinkVerificationTiming {
    pgwire_connect: Duration,
    wait_for_count: Duration,
    wait_for_count_for_throughput: Duration,
    sample_query: Duration,
    total: Duration,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum SampleSelection {
    FirstN(usize),
    EvenlySpaced(usize),
}

impl Default for SampleSelection {
    fn default() -> Self {
        Self::FirstN(DEFAULT_SAMPLE_ROW_COUNT)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MillionQuerySpec {
    pub(crate) mv_name: &'static str,
    pub(crate) mv_sql: &'static str,
    pub(crate) output_fields: &'static [FieldSpec],
    pub(crate) input_row_count: usize,
    pub(crate) dataset: MillionDatasetKind,
    pub(crate) sample_selection: SampleSelection,
    pub(crate) sample_match_field: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum MillionDatasetKind {
    BidOnly {
        project: fn(&BidInput) -> Option<ExpectedRow>,
    },
    BidAuctionJoin {
        auction_rows: usize,
        project: fn(&BidInput, &AuctionInput) -> Option<ExpectedRow>,
    },
}

#[derive(Debug, Clone, Copy)]
enum SinkMode {
    WithKafkaSink,
    NoSink,
}

#[derive(Debug, Clone, Copy)]
enum TailVerifyMode {
    SamplesOnly,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum NoSinkVerifyMode {
    Full,
    CountOnly,
    CountAtEndOnly,
}

impl NoSinkVerifyMode {
    fn from_env() -> Self {
        match std::env::var("FLOE_E2E_NO_SINK_VERIFY")
            .unwrap_or_else(|_| "full".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "count_only" | "count-only" | "count" => Self::CountOnly,
            "count_end_only" | "count-end-only" | "count_end" | "count-end" | "end_count"
            | "end-count" => Self::CountAtEndOnly,
            _ => Self::Full,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BidInput {
    pub(crate) auction: i64,
    pub(crate) bidder: i64,
    pub(crate) price: i64,
    pub(crate) channel: &'static str,
    pub(crate) dir1: String,
    pub(crate) url: String,
    pub(crate) date_time_ms: i64,
}

impl BidInput {
    fn from_bid_idx(bid_idx: usize) -> Self {
        let bid_idx_i64 = i64::try_from(bid_idx).unwrap_or_default();
        let auction = i64::try_from((bid_idx - 1) % JOIN_AUCTION_ROW_COUNT + 1).unwrap_or_default();
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
        Self {
            auction,
            bidder,
            price,
            channel,
            dir1,
            url,
            date_time_ms: BASE_TS_MS + bid_idx_i64,
        }
    }

    fn write_json_line(&self, writer: &mut BufWriter<File>, bid_idx: usize) -> Result<()> {
        writeln!(
            writer,
            "{{\"source\":\"nexmark_bid\",\"data\":{{\"auction\":{},\"bidder\":{},\"price\":{},\"channel\":\"{}\",\"url\":\"{}\",\"date_time\":{},\"extra\":\"bid_extra_{}\"}}}}",
            self.auction,
            self.bidder,
            self.price,
            self.channel,
            self.url,
            self.date_time_ms,
            bid_idx,
        )
        .context("write bid row")?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AuctionInput {
    pub(crate) id: i64,
    pub(crate) seller: i64,
    pub(crate) category: i64,
    pub(crate) initial_bid: i64,
    pub(crate) reserve: i64,
    pub(crate) item_name: String,
    pub(crate) description: String,
    pub(crate) expires_ms: i64,
    pub(crate) date_time_ms: i64,
}

impl AuctionInput {
    fn from_auction_idx(auction_idx: usize) -> Self {
        let auction_idx_i64 = i64::try_from(auction_idx).unwrap_or_default();
        let category = 10 + i64::try_from((auction_idx - 1) % 10).unwrap_or_default();
        let initial_bid = 500 + (auction_idx_i64 % 1_000);
        let reserve = initial_bid + 100;
        Self {
            id: auction_idx_i64,
            seller: 20_000 + auction_idx_i64,
            category,
            initial_bid,
            reserve,
            item_name: format!("item_{auction_idx}"),
            description: format!("auction_desc_{auction_idx}"),
            expires_ms: BASE_TS_MS + 3_600_000 + auction_idx_i64,
            date_time_ms: BASE_TS_MS - 60_000 + auction_idx_i64,
        }
    }

    fn write_json_line(&self, writer: &mut BufWriter<File>, auction_idx: usize) -> Result<()> {
        writeln!(
            writer,
            "{{\"source\":\"nexmark_auction\",\"data\":{{\"id\":{},\"item_name\":\"{}\",\"description\":\"{}\",\"initial_bid\":{},\"reserve\":{},\"seller\":{},\"category\":{},\"expires\":{},\"date_time\":{},\"extra\":\"auction_extra_{}\"}}}}",
            self.id,
            self.item_name,
            self.description,
            self.initial_bid,
            self.reserve,
            self.seller,
            self.category,
            self.expires_ms,
            self.date_time_ms,
            auction_idx,
        )
        .context("write auction row")?;
        Ok(())
    }
}

pub(crate) async fn run_redpanda_kafka_million_test(spec: MillionQuerySpec) -> Result<()> {
    run_redpanda_kafka_million_test_impl(spec, SinkMode::WithKafkaSink, None).await
}

pub(crate) async fn run_redpanda_kafka_million_no_sink_test(spec: MillionQuerySpec) -> Result<()> {
    run_redpanda_kafka_million_test_impl(spec, SinkMode::NoSink, None).await
}

pub(crate) async fn run_redpanda_kafka_million_no_sink_test_with_verify_mode(
    spec: MillionQuerySpec,
    verify_mode: NoSinkVerifyMode,
) -> Result<()> {
    run_redpanda_kafka_million_test_impl(spec, SinkMode::NoSink, Some(verify_mode)).await
}

async fn run_redpanda_kafka_million_test_impl(
    spec: MillionQuerySpec,
    sink_mode: SinkMode,
    no_sink_verify_mode_override: Option<NoSinkVerifyMode>,
) -> Result<()> {
    let brokers =
        std::env::var("FLOE_REDPANDA_BROKERS").unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let artifacts_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/e2e_artifacts")
        .join(format!("{}_{}", spec.mv_name, run_id));
    std::fs::create_dir_all(&artifacts_dir).context("create artifact dir")?;

    let dataset_path = artifacts_dir.join("dataset.jsonl");
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
    let no_sink_verify_mode =
        no_sink_verify_mode_override.unwrap_or_else(NoSinkVerifyMode::from_env);
    let no_sink_end_count_settle_ms = std::env::var("FLOE_E2E_NO_SINK_END_COUNT_SETTLE_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_NO_SINK_END_COUNT_SETTLE_MS);
    let no_sink_end_count_poll_ms = std::env::var("FLOE_E2E_NO_SINK_END_COUNT_POLL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_NO_SINK_END_COUNT_POLL_MS);
    let build_samples = !matches!(
        (sink_mode, no_sink_verify_mode),
        (
            SinkMode::NoSink,
            NoSinkVerifyMode::CountOnly | NoSinkVerifyMode::CountAtEndOnly
        )
    );

    eprintln!("artifacts_dir={}", artifacts_dir.display());
    eprintln!("dataset_path={}", dataset_path.display());
    eprintln!(
        "brokers={brokers} input_topic={input_topic} output_topic={output_topic} sink_mode={sink_mode:?}"
    );

    if matches!(sink_mode, SinkMode::NoSink) {
        eprintln!("verify.no_sink_mode={no_sink_verify_mode:?}");
        if matches!(no_sink_verify_mode, NoSinkVerifyMode::CountAtEndOnly) {
            eprintln!("verify.no_sink_end_count_settle_ms={no_sink_end_count_settle_ms}");
            eprintln!("verify.no_sink_end_count_poll_ms={no_sink_end_count_poll_ms}");
        }
    }

    let expected = {
        let dataset_generation_started = Instant::now();
        let dataset_path = dataset_path.clone();
        let expected = tokio::task::spawn_blocking(move || {
            generate_dataset_file(&dataset_path, spec, build_samples)
        })
        .await
        .context("join dataset generation task")??;
        eprintln!(
            "timing.dataset_generation_s={:.3}",
            dataset_generation_started.elapsed().as_secs_f64()
        );
        expected
    };
    if expected.generated_rows != spec.input_row_count {
        bail!(
            "dataset generator wrote {} rows, expected {}",
            expected.generated_rows,
            spec.input_row_count
        );
    }

    let sinks = match sink_mode {
        SinkMode::WithKafkaSink => vec![serde_json::json!({
            "type": "kafka",
            "name": "kafka_sink_million",
            "brokers": brokers.clone(),
            "topic": output_topic.clone(),
            "mv": spec.mv_name,
            "with_snapshot": false,
            "batch_rows": sink_batch_rows,
            "batch_bytes": 16777216,
            "queue_capacity": 65536,
            "retry_max_attempts": 8,
            "retry_base_ms": 50,
            "retry_max_backoff_ms": 1000
        })],
        SinkMode::NoSink => Vec::new(),
    };

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
        "sinks": sinks,
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
    if matches!(sink_mode, SinkMode::WithKafkaSink) {
        eprintln!("sink.batch_rows={sink_batch_rows}");
    }
    eprintln!("run.ingest_batch_size={ingest_batch_size}");
    eprintln!("run.ingest_batch_per_source={ingest_batch_per_source}");
    eprintln!("run.ingest_batch_per_connector={ingest_batch_per_connector}");
    if let Some(flush_interval_ms) = slatedb_flush_interval_ms {
        eprintln!("run.slatedb_flush_interval_ms={flush_interval_ms}");
    }
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write node config")?;

    ensure_topic_exists(&brokers, &input_topic).await?;
    if matches!(sink_mode, SinkMode::WithKafkaSink) {
        ensure_topic_exists(&brokers, &output_topic).await?;
    }

    let node_spawn_started = Instant::now();
    let mut child = spawn_node(
        &config_path,
        pg_port,
        spec.mv_sql,
        &stdout_log_path,
        &stderr_log_path,
        ingest_batch_size,
        ingest_batch_per_source,
        ingest_batch_per_connector,
        slatedb_flush_interval_ms,
    )
    .await?;
    eprintln!(
        "timing.node.spawn_s={:.3}",
        node_spawn_started.elapsed().as_secs_f64()
    );

    let test_result = async {
        let pgwire_ready_started = Instant::now();
        wait_for_pgwire(pg_port, &mut child, &stderr_log_path).await?;
        eprintln!(
            "timing.node.pgwire_ready_s={:.3} (post_spawn_wait_s={:.3})",
            node_spawn_started.elapsed().as_secs_f64(),
            pgwire_ready_started.elapsed().as_secs_f64()
        );
        let execution_started = Instant::now();

        match sink_mode {
            SinkMode::WithKafkaSink => {
                let (pgwire_ready_tx, pgwire_ready_rx) = oneshot::channel();
                let expected_for_pgwire = expected.clone();
                let pgwire_task = tokio::spawn(async move {
                    verify_pgwire_tail(
                        pg_port,
                        spec.mv_name,
                        spec.output_fields,
                        spec.sample_match_field,
                        expected_for_pgwire,
                        TailVerifyMode::SamplesOnly,
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
                        produce_dataset_file(
                            &dataset_path,
                            &brokers,
                            &input_topic,
                            spec.input_row_count,
                        )
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
                    let expected_row_count = expected.metrics.row_count;
                    let output_fields = spec.output_fields;
                    tokio::task::spawn_blocking(move || {
                        consume_sink_metrics(
                            &brokers,
                            &output_topic,
                            output_fields,
                            expected_row_count,
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
                    observed_sink.checksum, expected.metrics.checksum,
                    "sink checksum mismatch"
                );

                pgwire_task
                    .await
                    .context("join pgwire tail consumer task")??;
            }
            SinkMode::NoSink => {
                let produce_started = Instant::now();
                {
                    let dataset_path = dataset_path.clone();
                    let brokers = brokers.clone();
                    let input_topic = input_topic.clone();
                    tokio::task::spawn_blocking(move || {
                        produce_dataset_file(
                            &dataset_path,
                            &brokers,
                            &input_topic,
                            spec.input_row_count,
                        )
                    })
                    .await
                    .context("join kafka producer task")??;
                }
                let produce_elapsed = produce_started.elapsed();
                eprintln!(
                    "kafka production completed in {:?}",
                    produce_elapsed
                );

                let verify_timing = verify_mv_snapshot_count_and_samples(
                    pg_port,
                    spec.mv_name,
                    spec.output_fields,
                    spec.sample_match_field,
                    expected.clone(),
                    Duration::from_secs(1800),
                    no_sink_verify_mode,
                    Duration::from_millis(no_sink_end_count_settle_ms),
                    Duration::from_millis(no_sink_end_count_poll_ms),
                )
                .await?;

                let ingest_completion =
                    produce_elapsed + verify_timing.wait_for_count_for_throughput;
                let input_rows_per_sec =
                    safe_rows_per_sec(spec.input_row_count as f64, ingest_completion.as_secs_f64());
                let output_rows_per_sec = safe_rows_per_sec(
                    expected.metrics.row_count.max(0) as f64,
                    ingest_completion.as_secs_f64(),
                );
                eprintln!(
                    "timing.no_sink.ingest_complete_s={:.3} (produce_s={:.3}, post_produce_wait_s={:.3}, post_produce_wait_for_throughput_s={:.3})",
                    ingest_completion.as_secs_f64(),
                    produce_elapsed.as_secs_f64(),
                    verify_timing.wait_for_count.as_secs_f64(),
                    verify_timing.wait_for_count_for_throughput.as_secs_f64()
                );
                eprintln!(
                    "timing.no_sink.pgwire_connect_s={:.3}",
                    verify_timing.pgwire_connect.as_secs_f64()
                );
                eprintln!(
                    "timing.no_sink.verification_s={:.3} (sample_query_s={:.3})",
                    verify_timing.total.as_secs_f64(),
                    verify_timing.sample_query.as_secs_f64()
                );
                eprintln!(
                    "throughput.no_sink.input_rows_per_sec={:.0} output_rows_per_sec={:.0}",
                    input_rows_per_sec, output_rows_per_sec
                );
            }
        }

        eprintln!(
            "timing.execution.total_s={:.3}",
            execution_started.elapsed().as_secs_f64()
        );
        eprintln!(
            "verified rows={} checksum={}",
            expected.metrics.row_count, expected.metrics.checksum
        );

        Ok(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    test_result
}

async fn ensure_topic_exists(brokers: &str, topic: &str) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .context("create kafka admin client")?;
    let results = admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(5))),
        )
        .await
        .with_context(|| format!("create topic {topic}"))?;
    for result in results {
        match result {
            Ok(created_topic) if created_topic == topic => {}
            Ok(created_topic) => {
                bail!("unexpected topic creation result for '{created_topic}', expected '{topic}'")
            }
            Err((existing_topic, RDKafkaErrorCode::TopicAlreadyExists))
                if existing_topic == topic => {}
            Err((failed_topic, code)) => {
                bail!("failed to create topic '{failed_topic}': {code}")
            }
        }
    }
    Ok(())
}

fn generate_dataset_file(
    path: &Path,
    spec: MillionQuerySpec,
    build_samples: bool,
) -> Result<ExpectedDataset> {
    let file =
        File::create(path).with_context(|| format!("create dataset file {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
    let mut expected = ExpectedDataset::default();
    let output_rows = match spec.dataset {
        MillionDatasetKind::BidOnly { project } => {
            let mut output_rows = 0usize;
            for bid_idx in 1..=BID_ROW_COUNT {
                let input = BidInput::from_bid_idx(bid_idx);
                input.write_json_line(&mut writer, bid_idx)?;
                expected.generated_rows += 1;
                if let Some(row) = project(&input) {
                    expected.metrics.apply(&row, 1);
                    output_rows += 1;
                }
            }
            output_rows
        }
        MillionDatasetKind::BidAuctionJoin {
            auction_rows,
            project,
        } => {
            if auction_rows < JOIN_AUCTION_ROW_COUNT {
                bail!(
                    "join dataset requires at least {} auction rows, got {}",
                    JOIN_AUCTION_ROW_COUNT,
                    auction_rows
                );
            }
            let mut auctions = Vec::with_capacity(auction_rows);
            for auction_idx in 1..=auction_rows {
                let auction = AuctionInput::from_auction_idx(auction_idx);
                auction.write_json_line(&mut writer, auction_idx)?;
                expected.generated_rows += 1;
                auctions.push(auction);
            }

            let mut output_rows = 0usize;
            for bid_idx in 1..=BID_ROW_COUNT {
                let input = BidInput::from_bid_idx(bid_idx);
                input.write_json_line(&mut writer, bid_idx)?;
                expected.generated_rows += 1;
                let auction = auctions
                    .get((input.auction - 1).max(0) as usize)
                    .with_context(|| {
                        format!("missing auction row for join key {}", input.auction)
                    })?;
                if let Some(row) = project(&input, auction) {
                    expected.metrics.apply(&row, 1);
                    output_rows += 1;
                }
            }
            output_rows
        }
    };

    writer.flush().context("flush dataset writer")?;

    if !build_samples {
        return Ok(expected);
    }

    let sample_ordinals = compute_sample_ordinals(output_rows, spec.sample_selection);
    if sample_ordinals.is_empty() {
        return Ok(expected);
    }
    let sample_field_idx = sample_field_index(spec.output_fields, spec.sample_match_field)?;

    match spec.dataset {
        MillionDatasetKind::BidOnly { project } => {
            let mut output_ordinal = 0usize;
            for bid_idx in 1..=BID_ROW_COUNT {
                let input = BidInput::from_bid_idx(bid_idx);
                let Some(row) = project(&input) else {
                    continue;
                };
                output_ordinal += 1;
                maybe_record_sample_row(
                    &mut expected,
                    &sample_ordinals,
                    &mut output_ordinal,
                    sample_field_idx,
                    spec.sample_match_field,
                    row,
                )?;
                if expected.sample_rows_by_key.len() == sample_ordinals.len() {
                    break;
                }
            }
        }
        MillionDatasetKind::BidAuctionJoin {
            auction_rows,
            project,
        } => {
            let auctions: Vec<_> = (1..=auction_rows)
                .map(AuctionInput::from_auction_idx)
                .collect();
            let mut output_ordinal = 0usize;
            for bid_idx in 1..=BID_ROW_COUNT {
                let input = BidInput::from_bid_idx(bid_idx);
                let auction = auctions
                    .get((input.auction - 1).max(0) as usize)
                    .with_context(|| {
                        format!("missing auction row for join key {}", input.auction)
                    })?;
                let Some(row) = project(&input, auction) else {
                    continue;
                };
                output_ordinal += 1;
                maybe_record_sample_row(
                    &mut expected,
                    &sample_ordinals,
                    &mut output_ordinal,
                    sample_field_idx,
                    spec.sample_match_field,
                    row,
                )?;
                if expected.sample_rows_by_key.len() == sample_ordinals.len() {
                    break;
                }
            }
        }
    }

    if expected.sample_rows_by_key.len() != sample_ordinals.len() {
        bail!(
            "captured {} sample rows, expected {}",
            expected.sample_rows_by_key.len(),
            sample_ordinals.len()
        );
    }

    Ok(expected)
}

fn compute_sample_ordinals(total_rows: usize, selection: SampleSelection) -> BTreeSet<usize> {
    let mut out = BTreeSet::new();
    if total_rows == 0 {
        return out;
    }

    match selection {
        SampleSelection::FirstN(count) => {
            let end = count.min(total_rows);
            for idx in 1..=end {
                out.insert(idx);
            }
        }
        SampleSelection::EvenlySpaced(sample_count) => {
            if sample_count == 0 {
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
        }
    }

    out
}

fn maybe_record_sample_row(
    expected: &mut ExpectedDataset,
    sample_ordinals: &BTreeSet<usize>,
    output_ordinal: &mut usize,
    sample_field_idx: usize,
    sample_match_field: &str,
    row: ExpectedRow,
) -> Result<()> {
    if !sample_ordinals.contains(output_ordinal) {
        return Ok(());
    }

    let key = expected_value_key(row.values.get(sample_field_idx).with_context(|| {
        format!(
            "sample field index {} out of bounds for field '{}'",
            sample_field_idx, sample_match_field
        )
    })?);
    if expected
        .sample_rows_by_key
        .insert(key.clone(), row)
        .is_some()
    {
        bail!(
            "duplicate sample key '{key}' for field '{}'; choose a unique sample_match_field",
            sample_match_field
        );
    }
    Ok(())
}

fn produce_dataset_file(
    dataset_path: &Path,
    brokers: &str,
    topic: &str,
    expected_rows: usize,
) -> Result<()> {
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

    if produced != expected_rows {
        bail!("produced {produced} rows, expected {expected_rows}");
    }

    Ok(())
}

fn consume_sink_metrics(
    brokers: &str,
    topic: &str,
    output_fields: &[FieldSpec],
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

                let row = row_from_json(&value, output_fields)?;
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

async fn verify_pgwire_tail(
    pg_port: u16,
    mv_name: &str,
    output_fields: &'static [FieldSpec],
    sample_match_field: &'static str,
    expected: ExpectedDataset,
    verify_mode: TailVerifyMode,
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

    let tail_sql = format!("TAIL {mv_name}");
    let mut stream = Box::pin(
        client
            .simple_query_raw(&tail_sql)
            .await
            .context("start pgwire tail")?,
    );
    let _ = ready_tx.send(());

    let sample_field_idx = sample_field_index(output_fields, sample_match_field)?;
    let pgwire_value_idx = sample_field_idx + 3;
    let mut observed_samples: BTreeMap<String, ExpectedRow> = BTreeMap::new();
    let start = Instant::now();
    let mut tail_rows_seen: usize = 0;
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

                let Some(sample_key) = row.get(pgwire_value_idx) else {
                    continue;
                };
                if expected.sample_rows_by_key.contains_key(sample_key)
                    && !observed_samples.contains_key(sample_key)
                {
                    let actual = row_from_pgwire(&row, output_fields)?;
                    observed_samples.insert(sample_key.to_string(), actual);
                    eprintln!(
                        "captured pgwire sample key={} ({}/{})",
                        sample_key,
                        observed_samples.len(),
                        expected.sample_rows_by_key.len()
                    );
                    if observed_samples.len() == expected.sample_rows_by_key.len() {
                        break;
                    }
                }
                if tail_rows_seen % 100_000 == 0 {
                    eprintln!("consumed {tail_rows_seen} pgwire tail rows from mv={mv_name}");
                }
                if matches!(verify_mode, TailVerifyMode::SamplesOnly)
                    && observed_samples.len() == expected.sample_rows_by_key.len()
                {
                    break;
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

    if observed_samples.len() != expected.sample_rows_by_key.len() {
        let missing: Vec<String> = expected
            .sample_rows_by_key
            .keys()
            .filter(|key| !observed_samples.contains_key(*key))
            .cloned()
            .collect();
        bail!(
            "pgwire tail sample row count mismatch: observed={}, expected={}, tail_rows_seen={}, missing_keys={missing:?}",
            observed_samples.len(),
            expected.sample_rows_by_key.len(),
            tail_rows_seen
        );
    }
    for (key, expected_row) in &expected.sample_rows_by_key {
        let actual = observed_samples
            .get(key)
            .with_context(|| format!("missing pgwire tail sample for key={key}"))?;
        if actual != expected_row {
            bail!(
                "pgwire tail sample mismatch for key={}: actual={actual:?}, expected={expected_row:?}",
                key
            );
        }
    }

    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(())
}

async fn verify_mv_snapshot_count_and_samples(
    pg_port: u16,
    mv_name: &str,
    output_fields: &'static [FieldSpec],
    sample_match_field: &'static str,
    expected: ExpectedDataset,
    timeout: Duration,
    verify_mode: NoSinkVerifyMode,
    end_count_settle: Duration,
    end_count_poll: Duration,
) -> Result<NoSinkVerificationTiming> {
    let verify_started = Instant::now();
    let pgwire_connect_started = Instant::now();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        NoTls,
    )
    .await
    .context("connect to pgwire for no-sink verification")?;
    let pgwire_connect = pgwire_connect_started.elapsed();
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    let expected_rows = usize::try_from(expected.metrics.row_count)
        .context("expected row count must be non-negative and fit usize")?;
    let count_wait_started = Instant::now();
    let (settle_before_poll, poll_interval) =
        if matches!(verify_mode, NoSinkVerifyMode::CountAtEndOnly) {
            (end_count_settle, end_count_poll)
        } else {
            (Duration::ZERO, Duration::from_millis(250))
        };
    if !settle_before_poll.is_zero() {
        sleep(settle_before_poll).await;
    }
    let progress_targets = count_progress_targets(expected_rows);
    let mut first_nonzero_logged = false;
    let mut next_progress_idx = 0usize;
    let mut last_progress_rows = 0usize;
    let mut last_observed_rows = 0usize;
    let mut last_observed_elapsed = Duration::ZERO;
    loop {
        let observed_rows = query_mv_count(&client, mv_name).await?;
        log_count_progress(
            observed_rows,
            &count_wait_started,
            &mut first_nonzero_logged,
            &progress_targets,
            &mut next_progress_idx,
            &mut last_progress_rows,
            &mut last_observed_rows,
            &mut last_observed_elapsed,
        );
        if observed_rows == expected_rows {
            break;
        }
        if observed_rows > expected_rows {
            bail!(
                "mv row count exceeded expected: observed={}, expected={}",
                observed_rows,
                expected_rows
            );
        }
        if count_wait_started.elapsed() >= timeout {
            bail!(
                "mv row count did not reach expected within timeout: observed={}, expected={}",
                observed_rows,
                expected_rows
            );
        }
        sleep(poll_interval).await;
    }
    let wait_for_count = count_wait_started.elapsed();
    let wait_for_count_for_throughput = wait_for_count.saturating_sub(settle_before_poll);

    let sample_query_started = Instant::now();
    if matches!(verify_mode, NoSinkVerifyMode::Full) {
        let sample_field_idx = sample_field_index(output_fields, sample_match_field)?;
        let mut observed_samples: BTreeMap<String, ExpectedRow> = BTreeMap::new();
        if !expected.sample_rows_by_key.is_empty() {
            let sample_field_kind = output_fields
                .get(sample_field_idx)
                .map(|field| field.kind)
                .with_context(|| {
                    format!("sample field index {} out of bounds", sample_field_idx)
                })?;
            let sample_in_list =
                build_sql_in_list(expected.sample_rows_by_key.keys(), sample_field_kind)
                    .context("build sample IN list")?;
            let select_fields = output_fields
                .iter()
                .map(|field| field.name)
                .collect::<Vec<_>>()
                .join(", ");
            let sample_sql = format!(
                "SELECT {select_fields} FROM {mv_name} WHERE {sample_match_field} IN ({sample_in_list})"
            );
            let messages = client
                .simple_query(&sample_sql)
                .await
                .with_context(|| format!("query sample rows from {mv_name}"))?;
            for message in messages {
                if let SimpleQueryMessage::Row(row) = message {
                    let parsed = row_from_query_row(&row, output_fields)?;
                    let key = expected_value_key(
                        parsed.values.get(sample_field_idx).with_context(|| {
                            format!(
                                "sample field index {} out of bounds while parsing query row",
                                sample_field_idx
                            )
                        })?,
                    );
                    observed_samples.insert(key, parsed);
                }
            }
        }

        if observed_samples.len() != expected.sample_rows_by_key.len() {
            let missing: Vec<String> = expected
                .sample_rows_by_key
                .keys()
                .filter(|key| !observed_samples.contains_key(*key))
                .cloned()
                .collect();
            bail!(
                "sample row count mismatch after no-sink verification: observed={}, expected={}, missing_keys={missing:?}",
                observed_samples.len(),
                expected.sample_rows_by_key.len()
            );
        }
        for (key, expected_row) in &expected.sample_rows_by_key {
            let actual = observed_samples
                .get(key)
                .with_context(|| format!("missing sample row for key={key}"))?;
            if actual != expected_row {
                bail!(
                    "sample mismatch for key={}: actual={actual:?}, expected={expected_row:?}",
                    key
                );
            }
        }
    }
    let sample_query = sample_query_started.elapsed();

    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(NoSinkVerificationTiming {
        pgwire_connect,
        wait_for_count,
        wait_for_count_for_throughput,
        sample_query,
        total: verify_started.elapsed(),
    })
}

fn count_progress_targets(expected_rows: usize) -> [(&'static str, usize); 6] {
    [
        ("10pct", expected_rows / 10),
        ("25pct", expected_rows / 4),
        ("50pct", expected_rows / 2),
        ("75pct", expected_rows.saturating_mul(3) / 4),
        ("90pct", expected_rows.saturating_mul(9) / 10),
        ("100pct", expected_rows),
    ]
}

fn log_count_progress(
    observed_rows: usize,
    count_wait_started: &Instant,
    first_nonzero_logged: &mut bool,
    progress_targets: &[(&'static str, usize)],
    next_progress_idx: &mut usize,
    last_progress_rows: &mut usize,
    last_observed_rows: &mut usize,
    last_observed_elapsed: &mut Duration,
) {
    let elapsed = count_wait_started.elapsed();
    let poll_rows = observed_rows.saturating_sub(*last_observed_rows);
    let poll_elapsed = elapsed.saturating_sub(*last_observed_elapsed);
    let poll_rows_per_sec = safe_rows_per_sec(poll_rows as f64, poll_elapsed.as_secs_f64());
    if !*first_nonzero_logged && observed_rows > 0 {
        *first_nonzero_logged = true;
        eprintln!(
            "timing.no_sink.count_progress.first_nonzero_s={:.3} rows={observed_rows}",
            elapsed.as_secs_f64()
        );
    }
    while let Some((label, target_rows)) = progress_targets.get(*next_progress_idx) {
        if observed_rows < *target_rows {
            break;
        }
        let interval_rows = target_rows.saturating_sub(*last_progress_rows);
        eprintln!(
            "timing.no_sink.count_progress.{label}_s={:.3} observed_rows={} interval_rows={} poll_rows_per_sec={:.0} cumulative_rows_per_sec={:.0}",
            elapsed.as_secs_f64(),
            observed_rows,
            interval_rows,
            poll_rows_per_sec,
            safe_rows_per_sec(*target_rows as f64, elapsed.as_secs_f64())
        );
        *last_progress_rows = *target_rows;
        *next_progress_idx += 1;
    }
    *last_observed_rows = observed_rows;
    *last_observed_elapsed = elapsed;
}

async fn query_mv_count(client: &tokio_postgres::Client, mv_name: &str) -> Result<usize> {
    let sql = format!("SELECT COUNT(*) AS row_count FROM {mv_name}");
    let messages = client
        .simple_query(&sql)
        .await
        .with_context(|| format!("query row count for {mv_name}"))?;
    for message in messages {
        if let SimpleQueryMessage::Row(row) = message {
            let raw = row.get(0).context("COUNT(*) query missing first column")?;
            let count = raw
                .parse::<usize>()
                .with_context(|| format!("parse COUNT(*) result '{raw}' as usize"))?;
            return Ok(count);
        }
    }
    bail!("COUNT(*) query returned no rows for {mv_name}")
}

fn build_sql_in_list<'a, I>(keys: I, field_kind: FieldKind) -> Result<String>
where
    I: Iterator<Item = &'a String>,
{
    let mut values = Vec::new();
    for key in keys {
        let value = match field_kind {
            FieldKind::Int64 => {
                let parsed = key
                    .parse::<i64>()
                    .with_context(|| format!("parse sample key '{key}' as i64"))?;
                parsed.to_string()
            }
            FieldKind::String => format!("'{}'", key.replace('\'', "''")),
        };
        values.push(value);
    }
    if values.is_empty() {
        bail!("sample key set is empty");
    }
    Ok(values.join(", "))
}

async fn spawn_node(
    config_path: &Path,
    pg_port: u16,
    mv_sql: &str,
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
        .arg("--mv-query")
        .arg(mv_sql)
        .stdout(Stdio::from(stdout_log))
        .stderr(Stdio::from(stderr_log));
    if let Some(flush_interval_ms) = slatedb_flush_interval_ms {
        cmd.arg("--slatedb-flush-interval-ms")
            .arg(flush_interval_ms.to_string());
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

fn row_from_json(value: &Value, output_fields: &[FieldSpec]) -> Result<ExpectedRow> {
    let object = value
        .as_object()
        .context("sink payload must be an object")?;
    let mut values = Vec::with_capacity(output_fields.len());
    for field in output_fields {
        let value = match field.kind {
            FieldKind::Int64 => ExpectedValue::Int64(
                object
                    .get(field.name)
                    .and_then(Value::as_i64)
                    .with_context(|| format!("missing int64 field '{}'", field.name))?,
            ),
            FieldKind::String => ExpectedValue::String(
                object
                    .get(field.name)
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
                    .with_context(|| format!("missing string field '{}'", field.name))?,
            ),
        };
        values.push(value);
    }
    Ok(ExpectedRow::new(values))
}

fn row_from_pgwire(
    row: &tokio_postgres::SimpleQueryRow,
    output_fields: &[FieldSpec],
) -> Result<ExpectedRow> {
    row_from_query_row_at_offset(row, output_fields, 3)
}

fn row_from_query_row(
    row: &tokio_postgres::SimpleQueryRow,
    output_fields: &[FieldSpec],
) -> Result<ExpectedRow> {
    row_from_query_row_at_offset(row, output_fields, 0)
}

fn row_from_query_row_at_offset(
    row: &tokio_postgres::SimpleQueryRow,
    output_fields: &[FieldSpec],
    base_offset: usize,
) -> Result<ExpectedRow> {
    let mut values = Vec::with_capacity(output_fields.len());
    for (idx, field) in output_fields.iter().enumerate() {
        let value_idx = idx + base_offset;
        let raw = row
            .get(value_idx)
            .with_context(|| format!("pgwire tail row missing {}", field.name))?;
        let value = match field.kind {
            FieldKind::Int64 => ExpectedValue::Int64(
                raw.parse()
                    .with_context(|| format!("parse pgwire {} as i64", field.name))?,
            ),
            FieldKind::String => ExpectedValue::String(raw.to_string()),
        };
        values.push(value);
    }
    Ok(ExpectedRow::new(values))
}

fn row_checksum(row: &ExpectedRow) -> i128 {
    let mut acc = 17_i128;
    for value in &row.values {
        acc = match value {
            ExpectedValue::Int64(value) => mix(acc, i128::from(*value)),
            ExpectedValue::String(value) => mix_string(acc, value),
        };
    }
    acc
}

fn safe_rows_per_sec(rows: f64, seconds: f64) -> f64 {
    if seconds <= f64::EPSILON {
        0.0
    } else {
        rows / seconds
    }
}

fn sample_field_index(output_fields: &[FieldSpec], sample_match_field: &str) -> Result<usize> {
    output_fields
        .iter()
        .position(|field| field.name == sample_match_field)
        .with_context(|| {
            format!(
                "sample_match_field '{}' not found in output schema",
                sample_match_field
            )
        })
}

fn expected_value_key(value: &ExpectedValue) -> String {
    match value {
        ExpectedValue::Int64(value) => value.to_string(),
        ExpectedValue::String(value) => value.clone(),
    }
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

pub(crate) fn day_string() -> &'static str {
    DAY_UTC
}
