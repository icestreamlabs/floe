use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, bail};

use floe_benchmarks::harness_common::*;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum Engine {
    Floe,
    Materialize,
    RisingWave,
    Feldera,
}

impl Engine {
    fn parse(raw: &str) -> Result<EngineSelector> {
        match raw {
            "floe" => Ok(EngineSelector::One(Self::Floe)),
            "materialize" => Ok(EngineSelector::One(Self::Materialize)),
            "risingwave" => Ok(EngineSelector::One(Self::RisingWave)),
            "feldera" => Ok(EngineSelector::One(Self::Feldera)),
            "all" => Ok(EngineSelector::All),
            other => {
                bail!("unknown engine '{other}' (expected floe|materialize|risingwave|feldera|all)")
            }
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

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Floe => "floe",
            Self::Materialize => "materialize",
            Self::RisingWave => "risingwave",
            Self::Feldera => "feldera",
        }
    }
}

#[derive(Debug, Clone)]
pub(super) enum EngineSelector {
    One(Engine),
    All,
}

impl EngineSelector {
    pub(super) fn selected(&self) -> Vec<Engine> {
        match self {
            Self::One(engine) => vec![*engine],
            Self::All => Engine::all().to_vec(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum BenchQuery {
    FilterProjection,
    Join,
}

impl BenchQuery {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "filter_projection" => Ok(Self::FilterProjection),
            "join" => Ok(Self::Join),
            other => bail!("unknown BENCH_QUERY '{other}' (expected filter_projection|join)"),
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::FilterProjection => "filter_projection",
            Self::Join => "join",
        }
    }

    pub(super) fn description(self) -> &'static str {
        match self {
            Self::FilterProjection => "bid filter + projection",
            Self::Join => "bid/auction inner join + auction-side category filter",
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Config {
    pub(super) engine_selector: EngineSelector,
    pub(super) bench_query: BenchQuery,
    pub(super) repo_root: PathBuf,
    pub(super) run_id: String,
    pub(super) run_dir: PathBuf,
    pub(super) network_name: String,
    pub(super) rows: u64,
    pub(super) join_auction_rows: u64,
    pub(super) expected_rows: u64,
    pub(super) input_rows_total: u64,
    pub(super) poll_interval: Duration,
    pub(super) poll_timeout: Duration,
    pub(super) broker_port: u16,
    pub(super) broker_addr: String,
    pub(super) broker_addr_from_container: String,
    pub(super) redpanda_container: String,
    pub(super) redpanda_image: String,
    pub(super) materialize_container: String,
    pub(super) materialize_image: String,
    pub(super) materialize_sql_port: u16,
    pub(super) materialize_cluster_size: String,
    pub(super) materialize_best_effort_in_memory: bool,
    pub(super) risingwave_container: String,
    pub(super) risingwave_image: String,
    pub(super) risingwave_sql_port: u16,
    pub(super) risingwave_in_memory: bool,
    pub(super) feldera_container: String,
    pub(super) feldera_image: String,
    pub(super) feldera_http_port: u16,
    pub(super) feldera_workers: u64,
    pub(super) feldera_best_effort_in_memory: bool,
    pub(super) feldera_min_storage_bytes: u64,
    pub(super) feldera_min_step_storage_bytes: u64,
    pub(super) feldera_completion_mode: String,
    pub(super) kafka_latency_fetch_profile: bool,
    pub(super) kafka_fetch_wait_max_ms: u64,
    pub(super) kafka_fetch_queue_backoff_ms: u64,
    pub(super) kafka_fetch_min_bytes: u64,
    pub(super) floe_pg_port: u16,
    pub(super) floe_kafka_group_id_prefix: String,
    pub(super) floe_kafka_poll_ms: u64,
    pub(super) floe_kafka_max_messages_per_tick: u64,
    pub(super) floe_ingest_queue_capacity: u64,
    pub(super) floe_ingest_batch_size: u64,
    pub(super) floe_ingest_batch_per_source: u64,
    pub(super) floe_ingest_batch_per_connector: u64,
    pub(super) floe_mv_retain_last: u64,
    pub(super) floe_l0_sst_bytes: u64,
    pub(super) floe_max_unflushed_bytes: u64,
    pub(super) keep_containers: bool,
}

impl Config {
    pub(super) fn from_env_and_args() -> Result<Self> {
        let mut args = std::env::args().skip(1);
        let engine_arg = args.next().unwrap_or_else(|| "all".to_string());
        if engine_arg == "-h" || engine_arg == "--help" {
            super::print_usage();
            std::process::exit(0);
        }
        let query_arg = args
            .next()
            .unwrap_or_else(|| env_string("BENCH_QUERY", "filter_projection"));
        if let Some(extra) = args.next() {
            bail!("unexpected argument '{extra}'");
        }

        let engine_selector = Engine::parse(&engine_arg)?;
        let bench_query = BenchQuery::parse(&query_arg)?;
        let rows = env_parse("ROWS", 1_000_000)?;
        let join_auction_rows = env_parse("JOIN_AUCTION_ROWS", 10_000)?;
        if bench_query == BenchQuery::Join && join_auction_rows != 10_000 {
            bail!("join benchmark currently requires JOIN_AUCTION_ROWS=10000");
        }
        let input_rows_total = match bench_query {
            BenchQuery::FilterProjection => rows,
            BenchQuery::Join => rows + join_auction_rows,
        };
        let expected_rows = match bench_query {
            BenchQuery::FilterProjection => filter_projection_expected_rows(rows),
            BenchQuery::Join => rows / 10,
        };
        let repo_root = repo_root()?;
        let artifact_root = env_path("ARTIFACT_ROOT")
            .unwrap_or_else(|| repo_root.join("target/third_party_engine_benchmarks"));
        let run_id = current_millis()?.to_string();
        let run_dir = artifact_root.join(&run_id);
        let redpanda_container = env_string("REDPANDA_CONTAINER", "floe-stream-bench-redpanda");
        let broker_port = env_parse("BROKER_PORT", 19092)?;

        Ok(Self {
            engine_selector,
            bench_query,
            repo_root,
            run_id,
            run_dir,
            network_name: env_string("NETWORK_NAME", "floe-stream-bench-net"),
            rows,
            join_auction_rows,
            expected_rows,
            input_rows_total,
            poll_interval: Duration::from_millis(env_parse("POLL_INTERVAL_MS", 250)?),
            poll_timeout: Duration::from_millis(env_parse("POLL_TIMEOUT_MS", 150_000)?),
            broker_port,
            broker_addr: format!("127.0.0.1:{broker_port}"),
            broker_addr_from_container: env_string(
                "BROKER_ADDR_FROM_CONTAINER",
                &format!("{redpanda_container}:9092"),
            ),
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
            feldera_completion_mode: env_string("FELDERA_COMPLETION_MODE", "count"),
            kafka_latency_fetch_profile: env_bool("KAFKA_LATENCY_FETCH_PROFILE", true),
            kafka_fetch_wait_max_ms: env_parse("KAFKA_FETCH_WAIT_MAX_MS", 1)?,
            kafka_fetch_queue_backoff_ms: env_parse("KAFKA_FETCH_QUEUE_BACKOFF_MS", 1)?,
            kafka_fetch_min_bytes: env_parse("KAFKA_FETCH_MIN_BYTES", 1)?,
            floe_pg_port: env_parse("FLOE_PG_PORT", 16432)?,
            floe_kafka_group_id_prefix: env_string(
                "FLOE_KAFKA_GROUP_ID_PREFIX",
                "floe-stream-bench",
            ),
            floe_kafka_poll_ms: env_parse("FLOE_KAFKA_POLL_MS", 10)?,
            floe_kafka_max_messages_per_tick: env_parse(
                "FLOE_KAFKA_MAX_MESSAGES_PER_TICK",
                16_384,
            )?,
            floe_ingest_queue_capacity: env_parse("FLOE_INGEST_QUEUE_CAPACITY", 262_144)?,
            floe_ingest_batch_size: env_parse("FLOE_INGEST_BATCH_SIZE", 16_384)?,
            floe_ingest_batch_per_source: env_parse("FLOE_INGEST_BATCH_PER_SOURCE", 16_384)?,
            floe_ingest_batch_per_connector: env_parse("FLOE_INGEST_BATCH_PER_CONNECTOR", 16_384)?,
            floe_mv_retain_last: env_parse("FLOE_MV_RETAIN_LAST", 256)?,
            floe_l0_sst_bytes: env_parse("FLOE_L0_SST_BYTES", 1_073_741_824)?,
            floe_max_unflushed_bytes: env_parse("FLOE_MAX_UNFLUSHED_BYTES", 8_589_934_592u64)?,
            keep_containers: env_bool("KEEP_CONTAINERS", false),
        })
    }

    pub(super) fn results_file(&self) -> PathBuf {
        self.run_dir.join("summary.md")
    }

    pub(super) fn release_binary(&self, name: &str) -> PathBuf {
        self.repo_root.join("target/release").join(name)
    }
}

fn filter_projection_expected_rows(rows: u64) -> u64 {
    let full_cycles = rows / 10_000;
    let remainder = rows % 10_000;
    full_cycles * 5_000 + remainder.min(5_000)
}
