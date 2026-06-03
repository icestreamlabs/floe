#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[path = "ports.rs"]
mod ports;

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
use tokio::time::interval;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use ports::find_unused_port;

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

struct SubscribeVerification {
    pg_port: u16,
    mv_name: &'static str,
    output_fields: &'static [FieldSpec],
    sample_match_field: &'static str,
    expected: ExpectedDataset,
    verify_mode: SubscribeVerifyMode,
    timeout: Duration,
    ready_tx: oneshot::Sender<()>,
}

struct NoSinkVerification {
    pg_port: u16,
    mv_name: &'static str,
    output_fields: &'static [FieldSpec],
    sample_match_field: &'static str,
    expected: ExpectedDataset,
    timeout: Duration,
    verify_mode: NoSinkVerifyMode,
    end_count_settle: Duration,
    end_count_poll: Duration,
}

struct CountProgressLogger {
    count_wait_started: Instant,
    first_nonzero_logged: bool,
    progress_targets: [(&'static str, usize); 6],
    next_progress_idx: usize,
    last_progress_rows: usize,
    last_observed_rows: usize,
    last_observed_elapsed: Duration,
}

impl CountProgressLogger {
    fn new(count_wait_started: Instant, expected_rows: usize) -> Self {
        Self {
            count_wait_started,
            first_nonzero_logged: false,
            progress_targets: count_progress_targets(expected_rows),
            next_progress_idx: 0,
            last_progress_rows: 0,
            last_observed_rows: 0,
            last_observed_elapsed: Duration::ZERO,
        }
    }

    fn log(&mut self, observed_rows: usize) {
        let elapsed = self.count_wait_started.elapsed();
        let poll_rows = observed_rows.saturating_sub(self.last_observed_rows);
        let poll_elapsed = elapsed.saturating_sub(self.last_observed_elapsed);
        let poll_rows_per_sec = safe_rows_per_sec(poll_rows as f64, poll_elapsed.as_secs_f64());
        if !self.first_nonzero_logged && observed_rows > 0 {
            self.first_nonzero_logged = true;
            eprintln!(
                "timing.no_sink.count_progress.first_nonzero_s={:.3} rows={observed_rows}",
                elapsed.as_secs_f64()
            );
        }
        while let Some((label, target_rows)) = self.progress_targets.get(self.next_progress_idx) {
            if observed_rows < *target_rows {
                break;
            }
            let interval_rows = target_rows.saturating_sub(self.last_progress_rows);
            eprintln!(
                "timing.no_sink.count_progress.{label}_s={:.3} observed_rows={} interval_rows={} poll_rows_per_sec={:.0} cumulative_rows_per_sec={:.0}",
                elapsed.as_secs_f64(),
                observed_rows,
                interval_rows,
                poll_rows_per_sec,
                safe_rows_per_sec(*target_rows as f64, elapsed.as_secs_f64())
            );
            self.last_progress_rows = *target_rows;
            self.next_progress_idx += 1;
        }
        self.last_observed_rows = observed_rows;
        self.last_observed_elapsed = elapsed;
    }
}

struct NodeSpawnConfig<'a> {
    config_path: &'a Path,
    pg_port: u16,
    mv_sql: &'a str,
    stdout_log_path: &'a Path,
    stderr_log_path: &'a Path,
    ingest_batch_size: usize,
    ingest_batch_per_source: usize,
    ingest_batch_per_connector: usize,
    slatedb_flush_interval_ms: Option<u64>,
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
enum SubscribeVerifyMode {
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

#[path = "kafka_million/dataset.rs"]
mod dataset;
#[path = "kafka_million/process_rows.rs"]
mod process_rows;
#[path = "kafka_million/run_impl.rs"]
mod run_impl;
#[path = "kafka_million/verify.rs"]
mod verify;

use dataset::*;
use process_rows::*;
use run_impl::*;
use verify::*;

pub(crate) fn day_string() -> &'static str {
    process_rows::day_string()
}
