use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail, ensure};
use chrono::{DateTime, Utc};
use serde_json::json;
use sha2::{Digest, Sha256};

const CANONICAL_NEXMARK_QUERY_IDS: &[&str] = &[
    "q0", "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q12", "q13", "q14", "q15", "q16",
    "q17", "q18", "q19", "q20", "q21", "q22",
];
const DEFAULT_FLOE_NEXMARK_BATCH_ROWS: u64 = 8_192;
const DEFAULT_FLOE_KAFKA_POLL_MS: u64 = 1;
const DEFAULT_FLOE_SOURCE_JOURNAL: &str = "auto";
const NEXMARK_BASE_TS_MS: i64 = 1_700_000_000_000;
const NEXMARK_BID_AUCTION_CARDINALITY: u64 = 10_000;

#[path = "nexmark_cross_engine_compare/commands.rs"]
mod commands;
#[path = "nexmark_cross_engine_compare/fingerprints.rs"]
mod fingerprints;
#[path = "nexmark_cross_engine_compare/harness_engines.rs"]
mod harness_engines;
#[path = "nexmark_cross_engine_compare/harness_feldera_summary.rs"]
mod harness_feldera_summary;
#[path = "nexmark_cross_engine_compare/harness_floe.rs"]
mod harness_floe;
#[path = "nexmark_cross_engine_compare/harness_postgres.rs"]
mod harness_postgres;
#[path = "nexmark_cross_engine_compare/harness_setup.rs"]
mod harness_setup;
#[path = "nexmark_cross_engine_compare/queries.rs"]
mod queries;
#[cfg(test)]
#[path = "nexmark_cross_engine_compare/tests.rs"]
mod tests;

use self::commands::*;
use self::fingerprints::*;
use self::queries::*;

fn wait_before_retry(deadline: Instant, interval: Duration) -> bool {
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        return false;
    };
    if remaining.is_zero() {
        return false;
    }
    thread::park_timeout(interval.min(remaining));
    deadline > Instant::now()
}

fn main() -> Result<()> {
    let config = Config::from_env_and_args()?;
    let mut harness = Harness::new(config)?;
    harness.run()
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Engine {
    Floe,
    Materialize,
    RisingWave,
    Feldera,
}

impl Engine {
    fn parse(raw: &str) -> Result<EngineSelector> {
        let raw = raw.trim();
        if raw == "all" {
            return Ok(EngineSelector::new(Engine::all()));
        }
        let mut engines = Vec::new();
        for part in raw.split(',') {
            let part = part.trim();
            if part.is_empty() {
                bail!("empty engine in selector '{raw}'");
            }
            let engine = match part {
                "floe" => Self::Floe,
                "materialize" => Self::Materialize,
                "risingwave" => Self::RisingWave,
                "feldera" => Self::Feldera,
                "all" => bail!("'all' cannot be combined with other engines in '{raw}'"),
                other => bail!(
                    "unknown engine '{other}' (expected comma-separated floe|materialize|risingwave|feldera or all)"
                ),
            };
            if !engines.contains(&engine) {
                engines.push(engine);
            }
        }
        if engines.is_empty() {
            bail!("empty engine selector");
        }
        Ok(EngineSelector::new(engines))
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Floe => "floe",
            Self::Materialize => "materialize",
            Self::RisingWave => "risingwave",
            Self::Feldera => "feldera",
        }
    }

    fn all() -> [Self; 4] {
        [
            Self::Floe,
            Self::Materialize,
            Self::RisingWave,
            Self::Feldera,
        ]
    }
}

#[derive(Debug, Clone)]
struct EngineSelector {
    engines: Vec<Engine>,
}

impl EngineSelector {
    fn new<I>(engines: I) -> Self
    where
        I: IntoIterator<Item = Engine>,
    {
        Self {
            engines: engines.into_iter().collect(),
        }
    }

    fn selected(&self) -> Vec<Engine> {
        self.engines.clone()
    }

    fn contains(&self, engine: Engine) -> bool {
        self.engines.contains(&engine)
    }

    fn as_str(&self) -> String {
        self.engines
            .iter()
            .map(|engine| engine.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
enum Source {
    Bid,
    Auction,
    Person,
}

impl Source {
    fn label(self) -> &'static str {
        match self {
            Self::Bid => "bid",
            Self::Auction => "auction",
            Self::Person => "person",
        }
    }

    fn floe_source(self) -> &'static str {
        match self {
            Self::Bid => "nexmark_bid",
            Self::Auction => "nexmark_auction",
            Self::Person => "nexmark_person",
        }
    }
}

#[derive(Debug, Clone)]
struct Config {
    engine_selector: EngineSelector,
    query_selector: String,
    queries: Vec<String>,
    repo_root: PathBuf,
    run_id: String,
    run_dir: PathBuf,
    network_name: String,
    bid_rows: u64,
    auction_rows: u64,
    person_rows: u64,
    poll_interval: Duration,
    poll_timeout: Duration,
    pg_query_timeout_seconds: u64,
    pg_content_query_timeout_seconds: u64,
    broker_port: u16,
    broker_addr: String,
    broker_addr_from_container: String,
    redpanda_container: String,
    redpanda_image: String,
    materialize_container: String,
    materialize_image: String,
    materialize_sql_port: u16,
    materialize_cluster_size: String,
    materialize_best_effort_in_memory: bool,
    risingwave_container: String,
    risingwave_image: String,
    risingwave_sql_port: u16,
    risingwave_in_memory: bool,
    feldera_container: String,
    feldera_image: String,
    feldera_http_port: u16,
    feldera_workers: u64,
    feldera_best_effort_in_memory: bool,
    feldera_min_storage_bytes: u64,
    feldera_min_step_storage_bytes: u64,
    kafka_latency_fetch_profile: bool,
    kafka_fetch_wait_max_ms: u64,
    kafka_fetch_queue_backoff_ms: u64,
    kafka_fetch_min_bytes: u64,
    floe_pg_port: u16,
    floe_kafka_group_id_prefix: String,
    floe_kafka_poll_ms: u64,
    floe_kafka_max_messages_per_tick: u64,
    floe_ingest_queue_capacity: u64,
    floe_ingest_batch_size: u64,
    floe_ingest_batch_per_source: u64,
    floe_ingest_batch_per_connector: u64,
    floe_mv_retain_last: u64,
    floe_mv_flush_enabled: bool,
    floe_mv_flush_max_pending_deltas: u64,
    floe_mv_flush_max_delay_ms: u64,
    floe_mv_flush_on_catchup_boundary: bool,
    floe_l0_sst_bytes: u64,
    floe_max_unflushed_bytes: u64,
    floe_slatedb_await_durable: String,
    floe_source_journal: String,
    floe_object_store_db_name_prefix: Option<String>,
    floe_object_store_db_name: Option<String>,
    floe_admin_http_port: u16,
    floe_state_settle_after_catchup: bool,
    floe_state_settle_required: bool,
    floe_state_settle_timeout_seconds: u64,
    floe_aws_request_timeout: String,
    floe_require_object_store: bool,
    cloud_provider: Option<String>,
    keep_containers: bool,
    strict_result_correctness: bool,
    strict_result_content_check: bool,
    strict_content_retry_attempts: u64,
    strict_content_retry_delay_seconds: u64,
}

impl Config {
    fn from_env_and_args() -> Result<Self> {
        let mut args = env::args().skip(1);
        let engine_arg = args.next().unwrap_or_else(|| "all".to_string());
        if engine_arg == "-h" || engine_arg == "--help" {
            print_usage();
            std::process::exit(0);
        }
        let query_selector = args.next().unwrap_or_else(|| "all".to_string());
        if let Some(extra) = args.next() {
            bail!("unexpected argument '{extra}'");
        }

        let engine_selector = Engine::parse(&engine_arg)?;
        let queries = selected_queries(&query_selector)?;
        let repo_root = repo_root()?;
        let artifact_root = env_path("ARTIFACT_ROOT")
            .unwrap_or_else(|| repo_root.join("target/third_party_engine_benchmarks_nexmark"));
        let run_id = current_millis()?.to_string();
        let run_dir = artifact_root.join(&run_id);
        let redpanda_container = env_string("REDPANDA_CONTAINER", "floe-stream-bench-redpanda");
        let broker_port = env_parse("BROKER_PORT", 19092)?;

        Ok(Self {
            engine_selector,
            query_selector,
            queries,
            repo_root,
            run_id,
            run_dir,
            network_name: env_string("NETWORK_NAME", "floe-stream-bench-net"),
            bid_rows: env_parse("BID_ROWS", 1_000_000)?,
            auction_rows: env_parse("AUCTION_ROWS", 10_000)?,
            person_rows: env_parse("PERSON_ROWS", 10_000)?,
            poll_interval: Duration::from_millis(env_parse("POLL_INTERVAL_MS", 250)?),
            poll_timeout: Duration::from_millis(env_parse("POLL_TIMEOUT_MS", 600_000)?),
            pg_query_timeout_seconds: env_parse("PG_QUERY_TIMEOUT_SECONDS", 5)?,
            pg_content_query_timeout_seconds: env_parse("PG_CONTENT_QUERY_TIMEOUT_SECONDS", 120)?,
            broker_port,
            broker_addr: format!("127.0.0.1:{broker_port}"),
            broker_addr_from_container: format!("{redpanda_container}:9092"),
            redpanda_container,
            redpanda_image: env_string(
                "REDPANDA_IMAGE",
                "docker.redpanda.com/redpandadata/redpanda:latest",
            ),
            materialize_container: env_string(
                "MATERIALIZE_CONTAINER",
                "floe-stream-bench-materialize",
            ),
            materialize_image: env_string("MATERIALIZE_IMAGE", "materialize/materialized:v26.14.1"),
            materialize_sql_port: env_parse("MATERIALIZE_SQL_PORT", 16875)?,
            materialize_cluster_size: env_string("MATERIALIZE_CLUSTER_SIZE", "25cc"),
            materialize_best_effort_in_memory: env_bool("MATERIALIZE_BEST_EFFORT_IN_MEMORY", true),
            risingwave_container: env_string(
                "RISINGWAVE_CONTAINER",
                "floe-stream-bench-risingwave",
            ),
            risingwave_image: env_string("RISINGWAVE_IMAGE", "risingwavelabs/risingwave:latest"),
            risingwave_sql_port: env_parse("RISINGWAVE_SQL_PORT", 14566)?,
            risingwave_in_memory: env_bool("RISINGWAVE_IN_MEMORY", true),
            feldera_container: env_string("FELDERA_CONTAINER", "floe-stream-bench-feldera"),
            feldera_image: env_string("FELDERA_IMAGE", "ghcr.io/feldera/pipeline-manager:latest"),
            feldera_http_port: env_parse("FELDERA_HTTP_PORT", 18080)?,
            feldera_workers: env_parse("FELDERA_WORKERS", 4)?,
            feldera_best_effort_in_memory: env_bool("FELDERA_BEST_EFFORT_IN_MEMORY", true),
            feldera_min_storage_bytes: env_parse(
                "FELDERA_MIN_STORAGE_BYTES",
                1_099_511_627_776u64,
            )?,
            feldera_min_step_storage_bytes: env_parse(
                "FELDERA_MIN_STEP_STORAGE_BYTES",
                1_099_511_627_776u64,
            )?,
            kafka_latency_fetch_profile: env_bool("KAFKA_LATENCY_FETCH_PROFILE", true),
            kafka_fetch_wait_max_ms: env_parse("KAFKA_FETCH_WAIT_MAX_MS", 1)?,
            kafka_fetch_queue_backoff_ms: env_parse("KAFKA_FETCH_QUEUE_BACKOFF_MS", 1)?,
            kafka_fetch_min_bytes: env_parse("KAFKA_FETCH_MIN_BYTES", 1)?,
            floe_pg_port: env_parse("FLOE_PG_PORT", 16432)?,
            floe_kafka_group_id_prefix: env_string(
                "FLOE_KAFKA_GROUP_ID_PREFIX",
                "floe-stream-bench",
            ),
            floe_kafka_poll_ms: env_parse("FLOE_KAFKA_POLL_MS", DEFAULT_FLOE_KAFKA_POLL_MS)?,
            floe_kafka_max_messages_per_tick: env_parse(
                "FLOE_KAFKA_MAX_MESSAGES_PER_TICK",
                DEFAULT_FLOE_NEXMARK_BATCH_ROWS,
            )?,
            floe_ingest_queue_capacity: env_parse("FLOE_INGEST_QUEUE_CAPACITY", 262_144)?,
            floe_ingest_batch_size: env_parse(
                "FLOE_INGEST_BATCH_SIZE",
                DEFAULT_FLOE_NEXMARK_BATCH_ROWS,
            )?,
            floe_ingest_batch_per_source: env_parse(
                "FLOE_INGEST_BATCH_PER_SOURCE",
                DEFAULT_FLOE_NEXMARK_BATCH_ROWS,
            )?,
            floe_ingest_batch_per_connector: env_parse(
                "FLOE_INGEST_BATCH_PER_CONNECTOR",
                DEFAULT_FLOE_NEXMARK_BATCH_ROWS,
            )?,
            floe_mv_retain_last: env_parse("FLOE_MV_RETAIN_LAST", 256)?,
            floe_mv_flush_enabled: env_bool("FLOE_MV_FLUSH_ENABLED", false),
            floe_mv_flush_max_pending_deltas: env_parse("FLOE_MV_FLUSH_MAX_PENDING_DELTAS", 0)?,
            floe_mv_flush_max_delay_ms: env_parse("FLOE_MV_FLUSH_MAX_DELAY_MS", 0)?,
            floe_mv_flush_on_catchup_boundary: env_bool("FLOE_MV_FLUSH_ON_CATCHUP_BOUNDARY", true),
            floe_l0_sst_bytes: env_parse("FLOE_L0_SST_BYTES", 1_073_741_824)?,
            floe_max_unflushed_bytes: env_parse("FLOE_MAX_UNFLUSHED_BYTES", 8_589_934_592u64)?,
            floe_slatedb_await_durable: env_string("FLOE_SLATEDB_AWAIT_DURABLE", "false"),
            floe_source_journal: env_string("FLOE_SOURCE_JOURNAL", DEFAULT_FLOE_SOURCE_JOURNAL),
            floe_object_store_db_name_prefix: env_nonempty("FLOE_OBJECT_STORE_DB_NAME_PREFIX"),
            floe_object_store_db_name: env_nonempty("FLOE_OBJECT_STORE_DB_NAME"),
            floe_admin_http_port: env_parse("FLOE_ADMIN_HTTP_PORT", 0)?,
            floe_state_settle_after_catchup: env_bool("FLOE_STATE_SETTLE_AFTER_CATCHUP", false),
            floe_state_settle_required: env_bool("FLOE_STATE_SETTLE_REQUIRED", false),
            floe_state_settle_timeout_seconds: env_parse("FLOE_STATE_SETTLE_TIMEOUT_SECONDS", 300)?,
            floe_aws_request_timeout: env_string("FLOE_AWS_REQUEST_TIMEOUT", "180 seconds"),
            floe_require_object_store: env_bool("FLOE_REQUIRE_OBJECT_STORE", false),
            cloud_provider: env_nonempty("CLOUD_PROVIDER"),
            keep_containers: env_bool("KEEP_CONTAINERS", false),
            strict_result_correctness: env_bool("STRICT_RESULT_CORRECTNESS", true),
            strict_result_content_check: env_bool("STRICT_RESULT_CONTENT_CHECK", true),
            strict_content_retry_attempts: env_parse("STRICT_CONTENT_RETRY_ATTEMPTS", 24)?,
            strict_content_retry_delay_seconds: env_parse("STRICT_CONTENT_RETRY_DELAY_SECONDS", 5)?,
        })
    }

    fn rows_for_source(&self, source: Source) -> u64 {
        match source {
            Source::Bid => self.bid_rows,
            Source::Auction => self.auction_rows,
            Source::Person => self.person_rows,
        }
    }

    fn input_rows_total(&self, sources: &[Source]) -> u64 {
        sources
            .iter()
            .map(|source| self.rows_for_source(*source))
            .sum()
    }

    fn results_file(&self) -> PathBuf {
        self.run_dir.join("summary.md")
    }

    fn results_jsonl(&self) -> PathBuf {
        self.run_dir.join("results.jsonl")
    }

    fn target_release_binary(&self, name: &str) -> PathBuf {
        self.repo_root.join("target/release").join(name)
    }

    fn floe_slatedb_name_for_query(&self, query_id: &str) -> Option<String> {
        self.floe_object_store_db_name_prefix
            .as_ref()
            .map(|prefix| format!("{prefix}-{query_id}"))
            .or_else(|| self.floe_object_store_db_name.clone())
    }
}

struct Harness {
    config: Config,
    floe_child: Option<Child>,
}

struct Topics {
    bid: String,
    auction: String,
    person: String,
}

impl Topics {
    fn for_source(&self, source: Source) -> &str {
        match source {
            Source::Bid => &self.bid,
            Source::Auction => &self.auction,
            Source::Person => &self.person,
        }
    }
}

#[derive(Debug, Clone)]
struct Groups {
    bid: String,
    auction: String,
    person: String,
}

impl Groups {
    fn for_source(&self, source: Source) -> &str {
        match source {
            Source::Bid => &self.bid,
            Source::Auction => &self.auction,
            Source::Person => &self.person,
        }
    }
}

#[derive(Clone, Copy)]
struct PgTarget<'a> {
    port: u16,
    user: &'a str,
    db: &'a str,
}

struct PgTimedQuery<'a> {
    engine: Engine,
    query_id: &'a str,
    artifact_dir: &'a Path,
    sources: &'a [Source],
    topics: &'a Topics,
    target: PgTarget<'a>,
    notes_prefix: &'a str,
}

struct FloeValidationSpec<'a> {
    query_id: &'a str,
    artifact_dir: &'a Path,
    sources: &'a [Source],
    topics: &'a Topics,
    groups: &'a Groups,
    main_slatedb_name: Option<&'a str>,
    expected_result_rows: u64,
}

#[derive(Debug)]
struct RelationSpec {
    relation: String,
    target: u64,
}

#[derive(Debug)]
struct GroupStatus {
    current: u64,
    end: u64,
    lag: u64,
}

struct SummaryRow<'a> {
    engine: Engine,
    query_id: &'a str,
    status: &'a str,
    source_catchup_ms: Option<u128>,
    result_ready_ms: Option<u128>,
    produce_ms: Option<u128>,
    source_post_ms: Option<u128>,
    result_post_ms: Option<u128>,
    input_rows: u64,
    result_rows: Option<u64>,
    notes: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ContentFingerprint {
    row_count: u64,
    hash: String,
}

impl ContentFingerprint {
    fn short_hash(&self) -> &str {
        let end = self.hash.len().min(16);
        &self.hash[..end]
    }
}

#[derive(Default)]
struct HotspotStats {
    count: u64,
    share_sum: f64,
    max_total_ms: u64,
}

impl HotspotStats {
    fn avg_share(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.share_sum / self.count as f64
        }
    }
}
