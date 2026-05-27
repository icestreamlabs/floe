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
const DEFAULT_FLOE_SOURCE_JOURNAL: &str = "auto";
const NEXMARK_BASE_TS_MS: i64 = 1_700_000_000_000;
const NEXMARK_BID_AUCTION_CARDINALITY: u64 = 10_000;

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
enum EngineSelector {
    One(Engine),
    All,
}

impl EngineSelector {
    fn selected(&self) -> Vec<Engine> {
        match self {
            Self::One(engine) => vec![*engine],
            Self::All => Engine::all().to_vec(),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::One(engine) => engine.as_str(),
            Self::All => "all",
        }
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
            floe_kafka_poll_ms: env_parse("FLOE_KAFKA_POLL_MS", 10)?,
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

impl Harness {
    fn new(config: Config) -> Result<Self> {
        fs::create_dir_all(&config.run_dir)
            .with_context(|| format!("create run dir {}", config.run_dir.display()))?;
        Ok(Self {
            config,
            floe_child: None,
        })
    }

    fn run(&mut self) -> Result<()> {
        self.ensure_command("docker")?;
        self.ensure_command("psql")?;
        self.ensure_command("curl")?;
        self.validate_correctness_input_shape()?;
        self.write_summary_header()?;
        self.ensure_redpanda()?;
        self.build_producer()?;
        if matches!(
            self.config.engine_selector,
            EngineSelector::One(Engine::Floe) | EngineSelector::All
        ) {
            self.build_floe_node()?;
        }
        self.capture_run_context()?;

        for engine in self.config.engine_selector.selected() {
            self.run_engine_suite(engine)?;
        }

        log(format!(
            "results written to {}",
            self.config.results_file().display()
        ));
        let summary = fs::read_to_string(self.config.results_file()).context("read summary")?;
        print!("{summary}");
        Ok(())
    }

    fn validate_correctness_input_shape(&self) -> Result<()> {
        if self.config.strict_result_correctness && self.config.auction_rows > 10_000 {
            bail!(
                "STRICT_RESULT_CORRECTNESS requires AUCTION_ROWS <= 10000 (current: {})",
                self.config.auction_rows
            );
        }
        Ok(())
    }

    fn write_summary_header(&self) -> Result<()> {
        let mut file = File::create(self.config.results_file()).context("create summary")?;
        writeln!(file, "# Nexmark Cross-Engine Benchmark Summary")?;
        writeln!(file)?;
        writeln!(file, "Run: `{}`", self.config.run_id)?;
        writeln!(
            file,
            "Engine selector: `{}`",
            self.config.engine_selector.as_str()
        )?;
        writeln!(file, "Query selector: `{}`", self.config.query_selector)?;
        writeln!(
            file,
            "Dataset rows: bid=`{}`, auction=`{}`, person=`{}`",
            self.config.bid_rows, self.config.auction_rows, self.config.person_rows
        )?;
        writeln!(file)?;
        writeln!(
            file,
            "| Engine | Query | Status | Source Catchup (s) | Result Ready (s) | Produce (s) | Source Post-Produce Wait (s) | Result Post-Produce Wait (s) | Source Rows/s | Result Ready Rows/s | Input Rows | Result Rows | Notes |"
        )?;
        writeln!(
            file,
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"
        )?;
        File::create(self.config.results_jsonl()).context("create results jsonl")?;
        let queries_path = self.config.run_dir.join("queries.txt");
        fs::write(queries_path, self.config.queries.join("\n") + "\n").context("write queries")?;
        Ok(())
    }

    fn ensure_command(&self, command: &str) -> Result<()> {
        let status = Command::new("sh")
            .arg("-c")
            .arg(format!("command -v {command} >/dev/null 2>&1"))
            .status()
            .with_context(|| format!("check command {command}"))?;
        ensure!(status.success(), "{command} is required");
        Ok(())
    }

    fn ensure_network(&self) -> Result<()> {
        if command_success(
            "docker",
            ["network", "inspect", &self.config.network_name],
            None,
        )? {
            return Ok(());
        }
        run_status(
            "docker",
            ["network", "create", &self.config.network_name],
            None,
        )
        .context("create docker network")?;
        Ok(())
    }

    fn ensure_redpanda(&self) -> Result<()> {
        if self.container_running(&self.config.redpanda_container)? {
            return Ok(());
        }

        self.ensure_network()?;
        log(format!(
            "starting Redpanda {}",
            self.config.redpanda_container
        ));
        let _ = run_status(
            "docker",
            ["rm", "-f", &self.config.redpanda_container],
            None,
        );
        run_status("docker", ["pull", &self.config.redpanda_image], None)
            .context("pull redpanda image")?;
        run_status(
            "docker",
            [
                "run",
                "-d",
                "--name",
                &self.config.redpanda_container,
                "--network",
                &self.config.network_name,
                "-p",
                &format!("{}:19092", self.config.broker_port),
                &self.config.redpanda_image,
                "redpanda",
                "start",
                "--overprovisioned",
                "--smp",
                "1",
                "--memory",
                "1G",
                "--reserve-memory",
                "0M",
                "--node-id",
                "0",
                "--check=false",
                "--kafka-addr",
                "internal://0.0.0.0:9092,external://0.0.0.0:19092",
                "--advertise-kafka-addr",
                &format!(
                    "internal://{}:9092,external://127.0.0.1:{}",
                    self.config.redpanda_container, self.config.broker_port
                ),
            ],
            None,
        )
        .context("start redpanda")?;

        for _ in 0..90 {
            if command_success(
                "docker",
                [
                    "exec",
                    &self.config.redpanda_container,
                    "rpk",
                    "cluster",
                    "info",
                ],
                None,
            )? {
                return Ok(());
            }
            thread::sleep(Duration::from_secs(1));
        }

        let logs = run_capture("docker", ["logs", &self.config.redpanda_container], None)
            .unwrap_or_default();
        eprintln!("{logs}");
        bail!("Redpanda did not become ready")
    }

    fn container_running(&self, name: &str) -> Result<bool> {
        let out = run_capture("docker", ["ps", "--format", "{{.Names}}"], None)?;
        Ok(out.lines().any(|line| line == name))
    }

    fn build_producer(&self) -> Result<()> {
        log("building kafka benchmark producer");
        run_status(
            "cargo",
            [
                "build",
                "-p",
                "floe-benchmarks",
                "--bin",
                "kafka_million_bid_producer",
                "--release",
            ],
            Some(&self.config.repo_root),
        )
        .context("build kafka benchmark producer")
    }

    fn build_floe_node(&self) -> Result<()> {
        log("building floe-node release binary");
        run_status(
            "cargo",
            ["build", "-p", "floe-node", "--release"],
            Some(&self.config.repo_root),
        )
        .context("build floe-node")
    }

    fn capture_run_context(&self) -> Result<()> {
        let context = json!({
            "run_id": self.config.run_id,
            "engine": self.config.engine_selector.as_str(),
            "query_selector": self.config.query_selector,
            "dataset_rows": {
                "bid": self.config.bid_rows,
                "auction": self.config.auction_rows,
                "person": self.config.person_rows,
            },
            "kafka": {
                "broker_addr": self.config.broker_addr,
                "broker_addr_from_container": self.config.broker_addr_from_container,
            },
            "images": {
                "redpanda": self.config.redpanda_image,
                "materialize": self.config.materialize_image,
                "risingwave": self.config.risingwave_image,
                "feldera": self.config.feldera_image,
            },
            "floe": {
                "git_commit": run_capture("git", ["rev-parse", "HEAD"], Some(&self.config.repo_root)).unwrap_or_default().trim(),
                "git_branch": run_capture("git", ["branch", "--show-current"], Some(&self.config.repo_root)).unwrap_or_default().trim(),
                "rustc_version": run_capture("rustc", ["-V"], Some(&self.config.repo_root)).unwrap_or_default().trim(),
                "binary": "target/release/floe-node",
            },
            "correctness": {
                "strict_result_correctness": self.config.strict_result_correctness,
                "strict_result_content_check": self.config.strict_result_content_check,
                "content_check_mode": if self.config.strict_result_content_check {
                    "engine_local_expected_query"
                } else {
                    "exact_row_counts"
                },
            }
        });
        let path = self.config.run_dir.join("run_context.json");
        fs::write(path, serde_json::to_string_pretty(&context)?).context("write run context")
    }

    fn run_engine_suite(&mut self, engine: Engine) -> Result<()> {
        let engine_run_dir = self.config.run_dir.join(engine.as_str());
        fs::create_dir_all(&engine_run_dir)
            .with_context(|| format!("create engine artifact dir {}", engine_run_dir.display()))?;

        match engine {
            Engine::Materialize => {
                log("starting Materialize container");
                if let Err(err) = self.start_materialize() {
                    self.record_start_failures(engine, &format!("engine_start_failed: {err}"))?;
                    return Ok(());
                }
            }
            Engine::RisingWave => {
                log("starting RisingWave container");
                if let Err(err) = self.start_risingwave() {
                    self.record_start_failures(engine, &format!("engine_start_failed: {err}"))?;
                    return Ok(());
                }
            }
            Engine::Feldera => {
                log("starting Feldera container");
                if let Err(err) = self.start_feldera() {
                    self.record_start_failures(engine, &format!("engine_start_failed: {err}"))?;
                    return Ok(());
                }
            }
            Engine::Floe => {}
        }

        let queries = self.config.queries.clone();
        for query_id in queries {
            let query_artifact_dir = engine_run_dir.join(&query_id);
            let sources = required_sources_for_query(&query_id);
            let topics = self.producer_topics_for_query(engine, &query_id);

            for source in &sources {
                self.reset_topic(topics.for_source(*source))?;
            }

            let input_rows = self.config.input_rows_total(&sources);
            log(format!(
                "running {} {} (sources: {}, input_rows: {})",
                engine.as_str(),
                query_id,
                source_labels(&sources),
                input_rows
            ));

            let result = match engine {
                Engine::Floe => {
                    self.run_floe_query(&query_id, &query_artifact_dir, &sources, &topics)
                }
                Engine::Materialize => {
                    self.run_materialize_query(&query_id, &query_artifact_dir, &sources, &topics)
                }
                Engine::RisingWave => {
                    self.run_risingwave_query(&query_id, &query_artifact_dir, &sources, &topics)
                }
                Engine::Feldera => {
                    self.run_feldera_query(&query_id, &query_artifact_dir, &sources, &topics)
                }
            };

            if let Err(err) = result {
                self.record_failure(
                    engine,
                    &query_id,
                    &format!(
                        "setup_or_completion_failed (see {}): {err}",
                        query_artifact_dir.display()
                    ),
                    input_rows,
                )?;
            }
        }

        match engine {
            Engine::Materialize => self.stop_container(&self.config.materialize_container),
            Engine::RisingWave => self.stop_container(&self.config.risingwave_container),
            Engine::Feldera => self.stop_container(&self.config.feldera_container),
            Engine::Floe => {}
        }

        Ok(())
    }

    fn record_start_failures(&self, engine: Engine, notes: &str) -> Result<()> {
        for query_id in &self.config.queries {
            let sources = required_sources_for_query(query_id);
            let input_rows = self.config.input_rows_total(&sources);
            self.record_failure(engine, query_id, notes, input_rows)?;
        }
        Ok(())
    }

    fn producer_topics_for_query(&self, engine: Engine, query_id: &str) -> Topics {
        Topics {
            bid: format!(
                "{}_{}_{}_bids",
                engine.as_str(),
                query_id,
                self.config.run_id
            ),
            auction: format!(
                "{}_{}_{}_auctions",
                engine.as_str(),
                query_id,
                self.config.run_id
            ),
            person: format!(
                "{}_{}_{}_persons",
                engine.as_str(),
                query_id,
                self.config.run_id
            ),
        }
    }

    fn reset_topic(&self, topic: &str) -> Result<()> {
        let _ = run_status(
            "docker",
            [
                "exec",
                &self.config.redpanda_container,
                "rpk",
                "topic",
                "delete",
                topic,
            ],
            None,
        );
        run_status(
            "docker",
            [
                "exec",
                &self.config.redpanda_container,
                "rpk",
                "topic",
                "create",
                topic,
                "-p",
                "1",
                "-r",
                "1",
            ],
            None,
        )
        .with_context(|| format!("create topic {topic}"))
    }

    fn produce_for_sources(&self, sources: &[Source], topics: &Topics) -> Result<u128> {
        let mut produce_ms = 0;
        for source in [Source::Auction, Source::Person, Source::Bid] {
            if sources.contains(&source) {
                produce_ms += self.produce_topic(
                    topics.for_source(source),
                    source.label(),
                    self.config.rows_for_source(source),
                )?;
            }
        }
        Ok(produce_ms)
    }

    fn produce_topic(&self, topic: &str, dataset: &str, rows: u64) -> Result<u128> {
        let start = Instant::now();
        run_status(
            self.config
                .target_release_binary("kafka_million_bid_producer")
                .as_os_str(),
            [
                "--brokers",
                &self.config.broker_addr,
                "--topic",
                topic,
                "--dataset",
                dataset,
                "--rows",
                &rows.to_string(),
            ],
            Some(&self.config.repo_root),
        )
        .with_context(|| format!("produce {rows} {dataset} rows to {topic}"))?;
        Ok(start.elapsed().as_millis())
    }

    fn start_materialize(&self) -> Result<()> {
        self.stop_container(&self.config.materialize_container);
        run_status("docker", ["pull", &self.config.materialize_image], None)
            .context("pull materialize image")?;
        run_status(
            "docker",
            [
                "run",
                "-d",
                "--name",
                &self.config.materialize_container,
                "--network",
                &self.config.network_name,
                "-p",
                &format!("{}:6875", self.config.materialize_sql_port),
                &self.config.materialize_image,
            ],
            None,
        )
        .context("start materialize")?;

        self.wait_for_pg(
            self.config.materialize_sql_port,
            "materialize",
            "materialize",
        )
        .context("wait for materialize pgwire")?;
        self.pg_exec(
            self.config.materialize_sql_port,
            "materialize",
            "materialize",
            &format!(
                "DROP CLUSTER IF EXISTS bench CASCADE; CREATE CLUSTER bench SIZE '{}'",
                self.config.materialize_cluster_size
            ),
            None,
        )
        .context("create materialize bench cluster")
    }

    fn start_risingwave(&self) -> Result<()> {
        self.stop_container(&self.config.risingwave_container);
        run_status("docker", ["pull", &self.config.risingwave_image], None)
            .context("pull risingwave image")?;
        let mut args = vec![
            "run".to_string(),
            "-d".to_string(),
            "--name".to_string(),
            self.config.risingwave_container.clone(),
            "--network".to_string(),
            self.config.network_name.clone(),
            "-p".to_string(),
            format!("{}:4566", self.config.risingwave_sql_port),
            self.config.risingwave_image.clone(),
            "single_node".to_string(),
        ];
        if self.config.risingwave_in_memory {
            args.push("--in-memory".to_string());
        }
        run_status_vec("docker", &args, None).context("start risingwave")?;
        self.wait_for_pg(self.config.risingwave_sql_port, "root", "dev")
            .context("wait for risingwave pgwire")
    }

    fn start_feldera(&self) -> Result<()> {
        self.stop_container(&self.config.feldera_container);
        run_status("docker", ["pull", &self.config.feldera_image], None)
            .context("pull feldera image")?;
        run_status(
            "docker",
            [
                "run",
                "-d",
                "--name",
                &self.config.feldera_container,
                "--network",
                &self.config.network_name,
                "-p",
                &format!("{}:8080", self.config.feldera_http_port),
                &self.config.feldera_image,
            ],
            None,
        )
        .context("start feldera")?;

        let url = format!(
            "http://127.0.0.1:{}/v0/pipelines",
            self.config.feldera_http_port
        );
        for _ in 0..90 {
            if command_success("curl", ["-fsS", &url], None)? {
                return Ok(());
            }
            thread::sleep(Duration::from_secs(1));
        }
        bail!("Feldera HTTP API did not become ready")
    }

    fn run_materialize_query(
        &self,
        query_id: &str,
        artifact_dir: &Path,
        sources: &[Source],
        topics: &Topics,
    ) -> Result<()> {
        fs::create_dir_all(artifact_dir)?;
        let setup_sql =
            write_materialize_setup_sql(&self.config, query_id, sources, topics, artifact_dir)?;
        self.psql_file(
            self.config.materialize_sql_port,
            "materialize",
            "materialize",
            &setup_sql,
            artifact_dir,
            "setup",
        )
        .context("run materialize setup")?;
        self.run_pg_timed_query(PgTimedQuery {
            engine: Engine::Materialize,
            query_id,
            artifact_dir,
            sources,
            topics,
            target: PgTarget {
                port: self.config.materialize_sql_port,
                user: "materialize",
                db: "materialize",
            },
            notes_prefix: "count_views_pgwire",
        })
    }

    fn run_risingwave_query(
        &self,
        query_id: &str,
        artifact_dir: &Path,
        sources: &[Source],
        topics: &Topics,
    ) -> Result<()> {
        fs::create_dir_all(artifact_dir)?;
        let setup_sql =
            write_risingwave_setup_sql(&self.config, query_id, sources, topics, artifact_dir)?;
        self.psql_file(
            self.config.risingwave_sql_port,
            "root",
            "dev",
            &setup_sql,
            artifact_dir,
            "setup",
        )
        .context("run risingwave setup")?;
        self.run_pg_timed_query(PgTimedQuery {
            engine: Engine::RisingWave,
            query_id,
            artifact_dir,
            sources,
            topics,
            target: PgTarget {
                port: self.config.risingwave_sql_port,
                user: "root",
                db: "dev",
            },
            notes_prefix: "count_views_pgwire",
        })
    }

    fn run_pg_timed_query(&self, spec: PgTimedQuery<'_>) -> Result<()> {
        let PgTimedQuery {
            engine,
            query_id,
            artifact_dir,
            sources,
            topics,
            target,
            notes_prefix,
        } = spec;
        let specs = relation_specs_for_sources(&self.config, sources, "benchmark_ingest");
        let input_rows = self.config.input_rows_total(sources);
        let expected_result_rows = expected_result_rows_for_query(&self.config, query_id)
            .with_context(|| format!("missing expected result rows for query {query_id}"))?;

        let start = Instant::now();
        let produce_ms = self.produce_for_sources(sources, topics)?;
        self.poll_pg_source_counts(target, &specs)
            .context("source count poll failed")?;
        let source_catchup_ms = start.elapsed().as_millis();
        let source_post_ms = source_catchup_ms.saturating_sub(produce_ms);

        self.poll_pg_result_rows_equals(target, expected_result_rows, "benchmark_result")
            .context("result row poll failed")?;
        let result_ready_ms = start.elapsed().as_millis();
        let result_post_ms = result_ready_ms.saturating_sub(produce_ms);
        let result_rows = self
            .fetch_pg_scalar(target, "SELECT COUNT(*)::BIGINT FROM benchmark_result")
            .unwrap_or_default()
            .parse::<u64>()
            .unwrap_or(0);

        if result_rows != expected_result_rows {
            fs::write(
                artifact_dir.join("correctness.error"),
                format!(
                    "expected_result_rows={expected_result_rows}\nobserved_result_rows={result_rows}\nquery_id={query_id}\n"
                ),
            )?;
            bail!("result row mismatch: expected {expected_result_rows}, observed {result_rows}");
        }

        let mut content_hash_note = String::new();
        if self.config.strict_result_content_check {
            let observed = self.compute_pg_result_content_hash(
                target,
                artifact_dir,
                &artifact_dir.join("benchmark_result.stderr.log"),
            )?;
            let expected_query_text = query_sql_for_engine(engine, query_id)
                .with_context(|| format!("expected query SQL for {query_id}"))?;
            let expected = self.compute_pg_query_content_fingerprint(
                target,
                artifact_dir,
                "expected_result",
                expected_query_text,
                &artifact_dir.join("expected_result.stderr.log"),
            )?;
            verify_result_content_hash(engine, query_id, &observed, &expected, artifact_dir)?;
            content_hash_note = format!(";content_sha256={}", observed.short_hash());
        }

        self.append_summary_row(SummaryRow {
            engine,
            query_id,
            status: "ok",
            source_catchup_ms: Some(source_catchup_ms),
            result_ready_ms: Some(result_ready_ms),
            produce_ms: Some(produce_ms),
            source_post_ms: Some(source_post_ms),
            result_post_ms: Some(result_post_ms),
            input_rows,
            result_rows: Some(result_rows),
            notes: format!(
                "{notes_prefix};correctness_exact_rows={expected_result_rows}{content_hash_note}"
            ),
        })
    }

    fn run_feldera_query(
        &self,
        query_id: &str,
        artifact_dir: &Path,
        sources: &[Source],
        topics: &Topics,
    ) -> Result<()> {
        fs::create_dir_all(artifact_dir)?;
        let pipeline = format!("nexmark_{}_{}", query_id, self.config.run_id);
        let program_path = artifact_dir.join("program.sql");
        fs::write(
            &program_path,
            feldera_program_sql(&self.config, query_id, sources, topics)?,
        )
        .context("write feldera program")?;

        let pipeline_url = format!(
            "http://127.0.0.1:{}/v0/pipelines/{}",
            self.config.feldera_http_port, pipeline
        );
        let _ = run_status("curl", ["-fsS", "-X", "DELETE", &pipeline_url], None);

        let program_code = fs::read_to_string(&program_path)?;
        let payload = if self.config.feldera_best_effort_in_memory {
            json!({
                "name": pipeline,
                "description": "Nexmark cross-engine benchmark",
                "runtime_config": {
                    "workers": self.config.feldera_workers,
                    "storage": {
                        "min_storage_bytes": self.config.feldera_min_storage_bytes,
                        "min_step_storage_bytes": self.config.feldera_min_step_storage_bytes,
                    }
                },
                "program_config": {},
                "program_code": program_code,
            })
        } else {
            json!({
                "name": pipeline,
                "description": "Nexmark cross-engine benchmark",
                "runtime_config": { "workers": self.config.feldera_workers },
                "program_config": {},
                "program_code": program_code,
            })
        };
        let payload_path = artifact_dir.join("pipeline_create_payload.json");
        fs::write(&payload_path, serde_json::to_vec_pretty(&payload)?)?;
        self.curl_json_file(
            "PUT",
            &pipeline_url,
            &payload_path,
            artifact_dir,
            "pipeline_create",
        )
        .context("create feldera pipeline")?;
        self.poll_feldera_program_success(&pipeline)?;

        run_status(
            "curl",
            ["-fsS", "-X", "POST", &format!("{pipeline_url}/start")],
            None,
        )
        .context("start feldera pipeline")?;
        self.poll_feldera_running(&pipeline)?;

        let specs = relation_specs_for_sources(&self.config, sources, "benchmark_ingest");
        let input_rows = self.config.input_rows_total(sources);
        let expected_result_rows = expected_result_rows_for_query(&self.config, query_id)
            .with_context(|| format!("missing expected result rows for query {query_id}"))?;

        let start = Instant::now();
        let produce_ms = self.produce_for_sources(sources, topics)?;
        self.poll_feldera_source_counts(&pipeline, &specs)?;
        let source_catchup_ms = start.elapsed().as_millis();
        let source_post_ms = source_catchup_ms.saturating_sub(produce_ms);

        self.poll_feldera_result_rows_equals(&pipeline, expected_result_rows)?;
        let result_ready_ms = start.elapsed().as_millis();
        let result_post_ms = result_ready_ms.saturating_sub(produce_ms);
        let result_rows = self
            .feldera_query_row_count(
                &pipeline,
                "SELECT COUNT(*) AS row_count FROM benchmark_result",
            )
            .unwrap_or(0);

        let mut content_hash_note = String::new();
        if self.config.strict_result_content_check {
            let observed = self.compute_feldera_query_content_fingerprint(
                &pipeline,
                artifact_dir,
                "benchmark_result",
                "SELECT * FROM benchmark_result",
            )?;
            let expected_query_text = query_sql_for_engine(Engine::Feldera, query_id)
                .with_context(|| format!("expected query SQL for {query_id}"))?;
            let expected = self.compute_feldera_query_content_fingerprint(
                &pipeline,
                artifact_dir,
                "expected_result",
                expected_query_text,
            )?;
            verify_result_content_hash(
                Engine::Feldera,
                query_id,
                &observed,
                &expected,
                artifact_dir,
            )?;
            content_hash_note = format!(";content_sha256={}", observed.short_hash());
        }

        let mut notes = "count_views_adhoc_query".to_string();
        if self.config.feldera_best_effort_in_memory {
            notes = "count_views_adhoc_query_best_effort_in_memory".to_string();
        }
        notes.push_str(&format!(
            ";correctness_exact_rows={expected_result_rows}{content_hash_note}"
        ));
        self.append_summary_row(SummaryRow {
            engine: Engine::Feldera,
            query_id,
            status: "ok",
            source_catchup_ms: Some(source_catchup_ms),
            result_ready_ms: Some(result_ready_ms),
            produce_ms: Some(produce_ms),
            source_post_ms: Some(source_post_ms),
            result_post_ms: Some(result_post_ms),
            input_rows,
            result_rows: Some(result_rows),
            notes,
        })?;

        let _ = run_status(
            "curl",
            ["-fsS", "-X", "POST", &format!("{pipeline_url}/shutdown")],
            None,
        );
        let _ = run_status("curl", ["-fsS", "-X", "DELETE", &pipeline_url], None);
        Ok(())
    }

    fn run_floe_query(
        &mut self,
        query_id: &str,
        artifact_dir: &Path,
        sources: &[Source],
        topics: &Topics,
    ) -> Result<()> {
        fs::create_dir_all(artifact_dir)?;
        let bid_group_id = format!(
            "{}_{}_{}_bid",
            self.config.floe_kafka_group_id_prefix, self.config.run_id, query_id
        );
        let auction_group_id = format!(
            "{}_{}_{}_auction",
            self.config.floe_kafka_group_id_prefix, self.config.run_id, query_id
        );
        let person_group_id = format!(
            "{}_{}_{}_person",
            self.config.floe_kafka_group_id_prefix, self.config.run_id, query_id
        );
        let groups = Groups {
            bid: bid_group_id,
            auction: auction_group_id,
            person: person_group_id,
        };

        let config_path = artifact_dir.join("floe_config.json");
        fs::write(
            &config_path,
            serde_json::to_vec_pretty(&floe_config_json(&self.config, sources, topics, &groups))?,
        )
        .context("write floe config")?;
        let program_path = artifact_dir.join("program.sql");
        let program_sql = floe_program_sql(query_id, sources)?;
        fs::write(&program_path, &program_sql).context("write floe program")?;

        let main_slatedb_name = self.config.floe_slatedb_name_for_query(query_id);
        if let Some(name) = &main_slatedb_name {
            fs::write(artifact_dir.join("slatedb_name.txt"), name)?;
        }

        self.stop_floe_process();
        let _ = run_status("pkill", ["-f", "/target/release/floe-node run"], None);
        self.start_floe_node(
            artifact_dir,
            &config_path,
            &program_sql.replace('\n', " "),
            main_slatedb_name.as_deref(),
            self.config.floe_admin_http_port,
        )?;
        self.wait_for_floe_pg(artifact_dir)?;
        self.verify_floe_storage_mode_if_requested(artifact_dir)?;

        let input_rows = self.config.input_rows_total(sources);
        let expected_result_rows = expected_result_rows_for_query(&self.config, query_id)
            .with_context(|| format!("missing expected result rows for query {query_id}"))?;
        let start = Instant::now();
        let produce_ms = self.produce_for_sources(sources, topics)?;
        self.poll_floe_query_completion(sources, &groups, topics)?;
        let source_catchup_ms = start.elapsed().as_millis();
        let source_post_ms = source_catchup_ms.saturating_sub(produce_ms);

        let target = PgTarget {
            port: self.config.floe_pg_port,
            user: "postgres",
            db: "postgres",
        };
        self.poll_pg_result_rows_equals(target, expected_result_rows, "benchmark_result")?;
        let result_ready_ms = start.elapsed().as_millis();
        let result_post_ms = result_ready_ms.saturating_sub(produce_ms);
        let result_rows = self
            .fetch_pg_scalar(target, "SELECT COUNT(*)::BIGINT FROM benchmark_result")
            .unwrap_or_default()
            .parse::<u64>()
            .unwrap_or(0);
        if result_rows != expected_result_rows {
            fs::write(
                artifact_dir.join("correctness.error"),
                format!(
                    "expected_result_rows={expected_result_rows}\nobserved_result_rows={result_rows}\nquery_id={query_id}\n"
                ),
            )?;
            bail!("result row mismatch: expected {expected_result_rows}, observed {result_rows}");
        }

        let mut content_hash_note = String::new();
        if self.config.strict_result_content_check {
            self.settle_floe_state_if_requested(artifact_dir)?;
            let offline_expected =
                self.floe_offline_expected_content_fingerprint(query_id, sources, artifact_dir)?;
            let observed = if let Some(expected) = offline_expected.as_ref() {
                self.retry_floe_result_content_hash_until_expected(target, artifact_dir, expected)?
            } else {
                self.poll_pg_relation_max_mv_version_stable(target, "benchmark_result", 8)?;
                self.retry_floe_result_content_hash(target, artifact_dir)?
            };
            let expected = if let Some(expected) = offline_expected {
                expected
            } else {
                self.stop_floe_process();
                self.run_floe_validation_for_content(FloeValidationSpec {
                    query_id,
                    artifact_dir,
                    sources,
                    topics,
                    groups: &groups,
                    main_slatedb_name: main_slatedb_name.as_deref(),
                    expected_result_rows,
                })?
            };
            verify_result_content_hash(Engine::Floe, query_id, &observed, &expected, artifact_dir)?;
            content_hash_note = format!(";content_sha256={}", observed.short_hash());
        }

        if !self.config.strict_result_content_check {
            self.settle_floe_state_if_requested(artifact_dir)?;
        }
        self.stop_floe_process();
        let hotspot_note = self
            .summarize_floe_hotspots(artifact_dir)
            .unwrap_or_default();
        let mut notes = format!(
            "source_catchup_kafka_group_offsets;correctness_exact_rows={expected_result_rows}{content_hash_note}"
        );
        if !hotspot_note.is_empty() {
            notes.push(';');
            notes.push_str(&hotspot_note);
        }
        self.append_summary_row(SummaryRow {
            engine: Engine::Floe,
            query_id,
            status: "ok",
            source_catchup_ms: Some(source_catchup_ms),
            result_ready_ms: Some(result_ready_ms),
            produce_ms: Some(produce_ms),
            source_post_ms: Some(source_post_ms),
            result_post_ms: Some(result_post_ms),
            input_rows,
            result_rows: Some(result_rows),
            notes,
        })
    }

    fn start_floe_node(
        &mut self,
        artifact_dir: &Path,
        config_path: &Path,
        program_sql: &str,
        slatedb_name: Option<&str>,
        admin_port: u16,
    ) -> Result<()> {
        let stdout = File::create(artifact_dir.join("floe-node.stdout.log"))?;
        let stderr = File::create(artifact_dir.join("floe-node.stderr.log"))?;
        let mut command = Command::new(self.config.target_release_binary("floe-node"));
        command
            .arg("run")
            .arg("--pgwire-addr")
            .arg(format!("127.0.0.1:{}", self.config.floe_pg_port))
            .arg("--admin-port")
            .arg(admin_port.to_string());

        if self.config.cloud_provider.is_some() {
            command.arg("--object-store-from-env");
            if let Some(name) = slatedb_name {
                command.arg("--slatedb-name").arg(name);
            }
        }
        if self.config.cloud_provider.as_deref() == Some("aws")
            && env::var_os("AWS_TIMEOUT").is_none()
        {
            command.env("AWS_TIMEOUT", &self.config.floe_aws_request_timeout);
        }

        command
            .arg("--slatedb-await-durable")
            .arg(&self.config.floe_slatedb_await_durable)
            .arg("--slatedb-l0-sst-bytes")
            .arg(self.config.floe_l0_sst_bytes.to_string())
            .arg("--slatedb-max-unflushed-bytes")
            .arg(self.config.floe_max_unflushed_bytes.to_string())
            .arg("--config")
            .arg(config_path)
            .arg("--mv-query")
            .arg(program_sql)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        self.floe_child = Some(command.spawn().context("start floe-node")?);
        Ok(())
    }

    fn wait_for_floe_pg(&mut self, artifact_dir: &Path) -> Result<()> {
        for _ in 0..180 {
            if let Some(child) = self.floe_child.as_mut()
                && let Some(status) = child.try_wait().context("poll floe-node")?
            {
                print_tail(artifact_dir.join("floe-node.stderr.log"), 120);
                bail!("floe-node exited before pgwire became ready: {status}");
            }
            if self
                .fetch_pg_scalar(
                    PgTarget {
                        port: self.config.floe_pg_port,
                        user: "postgres",
                        db: "postgres",
                    },
                    "SELECT 1",
                )
                .ok()
                .as_deref()
                == Some("1")
            {
                return Ok(());
            }
            thread::sleep(Duration::from_secs(1));
        }
        print_tail(artifact_dir.join("floe-node.stderr.log"), 120);
        bail!("floe pgwire did not become ready")
    }

    fn verify_floe_storage_mode_if_requested(&self, artifact_dir: &Path) -> Result<()> {
        if !self.config.floe_require_object_store {
            return Ok(());
        }
        let stdout_path = artifact_dir.join("floe-node.stdout.log");
        let stdout = fs::read_to_string(&stdout_path).unwrap_or_default();
        if stdout.contains("opening SlateDB database [path=in-memory") {
            fs::write(
                artifact_dir.join("storage_mode.error"),
                "floe_started_with_in_memory_storage_but_FLOE_REQUIRE_OBJECT_STORE_is_enabled\n",
            )?;
            bail!("FLOE_REQUIRE_OBJECT_STORE requested but Floe used in-memory storage");
        }
        if self.config.cloud_provider.is_none() {
            fs::write(
                artifact_dir.join("storage_mode.error"),
                "FLOE_REQUIRE_OBJECT_STORE_enabled_but_CLOUD_PROVIDER_is_unset\n",
            )?;
            bail!("FLOE_REQUIRE_OBJECT_STORE requested but CLOUD_PROVIDER is unset");
        }
        Ok(())
    }

    fn settle_floe_state_if_requested(&self, artifact_dir: &Path) -> Result<()> {
        if !self.config.floe_state_settle_after_catchup {
            return Ok(());
        }
        if self.config.floe_admin_http_port == 0 {
            fs::write(
                artifact_dir.join("state_settle.error"),
                "state_settle_requested_but_FLOE_ADMIN_HTTP_PORT_is_0\n",
            )?;
            if self.config.floe_state_settle_required {
                bail!("FLOE_STATE_SETTLE_AFTER_CATCHUP requested but FLOE_ADMIN_HTTP_PORT=0");
            }
            return Ok(());
        }

        let response_path = artifact_dir.join("state_settle.json");
        let stderr_path = artifact_dir.join("state_settle.stderr.log");
        let stdout = File::create(&response_path)?;
        let stderr = File::create(&stderr_path)?;
        let start = Instant::now();
        let status = Command::new("timeout")
            .arg(format!(
                "{}s",
                self.config.floe_state_settle_timeout_seconds
            ))
            .arg("curl")
            .arg("-fsS")
            .arg("-X")
            .arg("POST")
            .arg(format!(
                "http://127.0.0.1:{}/debug/storage/flush",
                self.config.floe_admin_http_port
            ))
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .status()
            .context("settle Floe state")?;
        if !status.success() {
            fs::write(
                artifact_dir.join("state_settle.error"),
                "state_settle_failed_or_timed_out\n",
            )?;
            if self.config.floe_state_settle_required {
                bail!("Floe state settle failed with {status}");
            }
            return Ok(());
        }
        let response = fs::read_to_string(&response_path)
            .ok()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
            .unwrap_or(serde_json::Value::Null);
        let summary = json!({
            "settle_elapsed_ms": start.elapsed().as_millis(),
            "response": response,
        });
        fs::write(
            artifact_dir.join("state_settle_summary.json"),
            serde_json::to_vec_pretty(&summary)?,
        )?;
        Ok(())
    }

    fn poll_pg_relation_max_mv_version_stable(
        &self,
        target: PgTarget<'_>,
        relation: &str,
        stable_polls_required: u64,
    ) -> Result<()> {
        let start = Instant::now();
        let mut previous = None;
        let mut stable_polls = 0;
        loop {
            if start.elapsed() >= self.config.poll_timeout {
                bail!("{relation} __mv_version did not become stable before timeout");
            }
            let sql = format!("SELECT COALESCE(MAX(__mv_version)::BIGINT, 0) FROM {relation}");
            let current = self
                .fetch_pg_scalar(target, &sql)
                .ok()
                .and_then(|raw| raw.parse::<u64>().ok());
            match current {
                Some(current) if previous == Some(current) => stable_polls += 1,
                Some(current) => {
                    previous = Some(current);
                    stable_polls = 1;
                }
                None => {
                    previous = None;
                    stable_polls = 0;
                }
            }
            if stable_polls >= stable_polls_required {
                return Ok(());
            }
            thread::sleep(self.config.poll_interval);
        }
    }

    fn retry_floe_result_content_hash(
        &self,
        target: PgTarget<'_>,
        artifact_dir: &Path,
    ) -> Result<ContentFingerprint> {
        let attempts = self.config.strict_content_retry_attempts.max(1);
        let delay = Duration::from_secs(self.config.strict_content_retry_delay_seconds);
        let mut last_error = None;
        for attempt in 1..=attempts {
            match self.compute_floe_result_content_hash(
                target,
                artifact_dir,
                &artifact_dir.join("benchmark_result.stderr.log"),
                "benchmark_result",
                "benchmark_result",
            ) {
                Ok(fingerprint) => return Ok(fingerprint),
                Err(err) => last_error = Some(err),
            }
            if attempt < attempts && !delay.is_zero() {
                thread::sleep(delay);
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("failed to compute Floe content hash")))
    }

    fn retry_floe_result_content_hash_until_expected(
        &self,
        target: PgTarget<'_>,
        artifact_dir: &Path,
        expected: &ContentFingerprint,
    ) -> Result<ContentFingerprint> {
        let attempts = self.config.strict_content_retry_attempts.max(1);
        let delay = Duration::from_secs(self.config.strict_content_retry_delay_seconds);
        let mut last = None;
        for attempt in 1..=attempts {
            self.poll_pg_relation_max_mv_version_stable(target, "benchmark_result", 8)?;
            let observed = self.compute_floe_result_content_hash(
                target,
                artifact_dir,
                &artifact_dir.join("benchmark_result.stderr.log"),
                "benchmark_result",
                "benchmark_result",
            )?;
            if observed == *expected {
                return Ok(observed);
            }
            last = Some(observed);
            if attempt < attempts && !delay.is_zero() {
                thread::sleep(delay);
            }
        }
        Ok(last.unwrap_or_else(|| ContentFingerprint {
            row_count: 0,
            hash: String::new(),
        }))
    }

    fn floe_offline_expected_content_fingerprint(
        &self,
        query_id: &str,
        sources: &[Source],
        artifact_dir: &Path,
    ) -> Result<Option<ContentFingerprint>> {
        let fingerprint = match (query_id, sources) {
            ("q5", [Source::Bid]) => deterministic_nexmark_q5_fingerprint(self.config.bid_rows),
            ("q14", [Source::Bid]) => fingerprint_lines(Vec::new()),
            ("q15", [Source::Bid]) => deterministic_nexmark_q15_fingerprint(self.config.bid_rows),
            ("q16", [Source::Bid]) => deterministic_nexmark_q16_fingerprint(self.config.bid_rows),
            ("q17", [Source::Bid]) => deterministic_nexmark_q17_fingerprint(self.config.bid_rows),
            _ => return Ok(None),
        };
        fs::write(
            artifact_dir.join("expected_result.offline.txt"),
            format!(
                "oracle=deterministic_nexmark_{query_id}\nbid_rows={}\nresult_rows={}\ncontent_sha256={}\n",
                self.config.bid_rows, fingerprint.row_count, fingerprint.hash
            ),
        )?;
        Ok(Some(fingerprint))
    }

    fn run_floe_validation_for_content(
        &mut self,
        spec: FloeValidationSpec<'_>,
    ) -> Result<ContentFingerprint> {
        let FloeValidationSpec {
            query_id,
            artifact_dir,
            sources,
            topics,
            groups,
            main_slatedb_name,
            expected_result_rows,
        } = spec;
        let validation_dir = artifact_dir.join("validation");
        fs::create_dir_all(&validation_dir)?;
        let validation_groups = Groups {
            bid: format!("{}_validation", groups.bid),
            auction: format!("{}_validation", groups.auction),
            person: format!("{}_validation", groups.person),
        };
        let validation_config_path = validation_dir.join("floe_config.json");
        let mut validation_config =
            floe_config_json(&self.config, sources, topics, &validation_groups);
        validation_config["storage"]["source_journal"] = json!("full");
        fs::write(
            &validation_config_path,
            serde_json::to_vec_pretty(&validation_config)?,
        )?;
        let expected_query = floe_expected_query_text_for_source_tables(query_id, sources)?;
        let validation_program =
            format!("CREATE MATERIALIZED VIEW benchmark_result AS\n{expected_query};\n");
        let validation_program_path = validation_dir.join("program.sql");
        fs::write(&validation_program_path, &validation_program)?;

        let validation_slatedb_name = if self.config.cloud_provider.is_some() {
            main_slatedb_name.map(|name| format!("{name}-validation"))
        } else {
            None
        };
        self.start_floe_node(
            &validation_dir,
            &validation_config_path,
            &validation_program.replace('\n', " "),
            validation_slatedb_name.as_deref(),
            0,
        )?;
        self.wait_for_floe_pg(&validation_dir)?;
        self.poll_floe_query_completion(sources, &validation_groups, topics)?;

        let target = PgTarget {
            port: self.config.floe_pg_port,
            user: "postgres",
            db: "postgres",
        };
        self.poll_pg_result_rows_equals(target, expected_result_rows, "benchmark_result")?;
        self.poll_pg_relation_max_mv_version_stable(target, "benchmark_result", 8)?;
        let expected = self.compute_floe_result_content_hash(
            target,
            &validation_dir,
            &validation_dir.join("expected_result.stderr.log"),
            "benchmark_result",
            "expected_result",
        );
        self.stop_floe_process();
        expected
    }

    fn wait_for_pg(&self, port: u16, user: &str, db: &str) -> Result<()> {
        let target = PgTarget { port, user, db };
        for _ in 0..90 {
            if self.fetch_pg_scalar(target, "SELECT 1").ok().as_deref() == Some("1") {
                return Ok(());
            }
            thread::sleep(Duration::from_secs(1));
        }
        bail!("pgwire did not become ready on port {port}")
    }

    fn pg_exec(
        &self,
        port: u16,
        user: &str,
        db: &str,
        sql: &str,
        cwd: Option<&Path>,
    ) -> Result<()> {
        let mut command = Command::new("psql");
        command
            .env("PGPASSWORD", "")
            .arg("-h")
            .arg("127.0.0.1")
            .arg("-p")
            .arg(port.to_string())
            .arg("-U")
            .arg(user)
            .arg("-d")
            .arg(db)
            .arg("-v")
            .arg("ON_ERROR_STOP=1")
            .arg("-Atqc")
            .arg(sql)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        let status = command.status().context("run psql")?;
        ensure!(status.success(), "psql command failed with {status}");
        Ok(())
    }

    fn psql_file(
        &self,
        port: u16,
        user: &str,
        db: &str,
        path: &Path,
        artifact_dir: &Path,
        label: &str,
    ) -> Result<()> {
        let stdout = File::create(artifact_dir.join(format!("{label}.stdout.log")))?;
        let stderr = File::create(artifact_dir.join(format!("{label}.stderr.log")))?;
        let status = Command::new("psql")
            .env("PGPASSWORD", "")
            .arg("-h")
            .arg("127.0.0.1")
            .arg("-p")
            .arg(port.to_string())
            .arg("-U")
            .arg(user)
            .arg("-d")
            .arg(db)
            .arg("-v")
            .arg("ON_ERROR_STOP=1")
            .arg("-f")
            .arg(path)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .status()
            .context("run psql file")?;
        ensure!(status.success(), "psql file failed with {status}");
        Ok(())
    }

    fn fetch_pg_scalar(&self, target: PgTarget<'_>, sql: &str) -> Result<String> {
        let output = Command::new("timeout")
            .arg(format!("{}s", self.config.pg_query_timeout_seconds))
            .arg("psql")
            .env("PGPASSWORD", "")
            .arg("-h")
            .arg("127.0.0.1")
            .arg("-p")
            .arg(target.port.to_string())
            .arg("-U")
            .arg(target.user)
            .arg("-d")
            .arg(target.db)
            .arg("-Atqc")
            .arg(sql)
            .output()
            .context("run pg scalar query")?;
        if !output.status.success() {
            bail!("pg scalar query failed with {}", output.status);
        }
        Ok(String::from_utf8_lossy(&output.stdout)
            .trim()
            .replace(char::is_whitespace, ""))
    }

    fn compute_pg_result_content_hash(
        &self,
        target: PgTarget<'_>,
        artifact_dir: &Path,
        stderr_path: &Path,
    ) -> Result<ContentFingerprint> {
        let projection =
            self.compute_pg_relation_projection(target, "benchmark_result", "public")?;
        let query_sql = if projection.is_empty() {
            "SELECT * FROM benchmark_result".to_string()
        } else {
            format!("SELECT {projection} FROM benchmark_result")
        };
        self.compute_pg_query_content_fingerprint(
            target,
            artifact_dir,
            "benchmark_result",
            &query_sql,
            stderr_path,
        )
    }

    fn compute_floe_result_content_hash(
        &self,
        target: PgTarget<'_>,
        artifact_dir: &Path,
        stderr_path: &Path,
        relation: &str,
        label: &str,
    ) -> Result<ContentFingerprint> {
        let projection = self.compute_floe_normalized_projection_for_relation(
            target, relation, "public", relation,
        )?;
        let query_sql = if projection.is_empty() {
            format!("SELECT * FROM {relation}")
        } else {
            format!("SELECT {projection} FROM {relation}")
        };
        self.compute_pg_query_content_fingerprint(
            target,
            artifact_dir,
            label,
            &query_sql,
            stderr_path,
        )
    }

    fn compute_pg_query_content_fingerprint(
        &self,
        target: PgTarget<'_>,
        artifact_dir: &Path,
        label: &str,
        sql: &str,
        stderr_path: &Path,
    ) -> Result<ContentFingerprint> {
        let rows_file = artifact_dir.join(format!("{label}.rows.tsv"));
        let stdout = File::create(&rows_file)?;
        let stderr = File::create(stderr_path)?;
        let status = Command::new("timeout")
            .arg(format!("{}s", self.config.pg_content_query_timeout_seconds))
            .arg("psql")
            .env("PGPASSWORD", "")
            .arg("-h")
            .arg("127.0.0.1")
            .arg("-p")
            .arg(target.port.to_string())
            .arg("-U")
            .arg(target.user)
            .arg("-d")
            .arg(target.db)
            .arg("-P")
            .arg("null=\\N")
            .arg("-At")
            .arg("-F")
            .arg("\t")
            .arg("-c")
            .arg(sql)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .status()
            .context("run content query")?;
        ensure!(status.success(), "content query failed with {status}");
        fingerprint_file_lines(&rows_file)
    }

    fn compute_pg_relation_projection(
        &self,
        target: PgTarget<'_>,
        relation: &str,
        schema: &str,
    ) -> Result<String> {
        validate_identifier(relation)?;
        validate_identifier(schema)?;
        let sql = format!(
            "WITH chosen_schema AS (
                SELECT table_schema
                FROM information_schema.columns
                WHERE table_name = '{}'
                  AND table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY
                  CASE WHEN table_schema = '{}' THEN 1 ELSE 0 END DESC,
                  table_schema
                LIMIT 1
              )
              SELECT c.column_name
              FROM information_schema.columns c
              JOIN chosen_schema s
                ON c.table_schema = s.table_schema
              WHERE c.table_name = '{}'
              ORDER BY c.ordinal_position",
            escape_sql_literal(relation),
            escape_sql_literal(schema),
            escape_sql_literal(relation)
        );
        let output = self.fetch_pg_table(target, &sql, self.config.pg_query_timeout_seconds)?;
        let columns = output
            .lines()
            .map(str::trim)
            .filter(|column| !column.is_empty() && *column != "__mv_version")
            .map(quote_identifier)
            .collect::<Vec<_>>();
        Ok(columns.join(", "))
    }

    fn compute_floe_normalized_projection_for_relation(
        &self,
        target: PgTarget<'_>,
        relation: &str,
        schema: &str,
        relation_alias: &str,
    ) -> Result<String> {
        validate_identifier(relation)?;
        validate_identifier(schema)?;
        validate_identifier(relation_alias)?;
        let sql = format!(
            "WITH chosen_schema AS (
                SELECT table_schema
                FROM information_schema.columns
                WHERE table_name = '{}'
                  AND table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY
                  CASE WHEN table_schema = '{}' THEN 1 ELSE 0 END DESC,
                  table_schema
                LIMIT 1
              )
              SELECT c.column_name, c.data_type
              FROM information_schema.columns c
              JOIN chosen_schema s
                ON c.table_schema = s.table_schema
              WHERE c.table_name = '{}'
              ORDER BY c.ordinal_position",
            escape_sql_literal(relation),
            escape_sql_literal(schema),
            escape_sql_literal(relation)
        );
        let output = self.fetch_pg_table(target, &sql, self.config.pg_query_timeout_seconds)?;
        let mut projection = Vec::new();
        for line in output.lines() {
            let mut parts = line.split('\t');
            let Some(column_name) = parts.next() else {
                continue;
            };
            if column_name.is_empty() || column_name == "__mv_version" {
                continue;
            }
            let data_type = parts.next().unwrap_or_default();
            let column_ref = format!(
                "{}.{}",
                quote_identifier(relation_alias),
                quote_identifier(column_name)
            );
            let expr = match data_type {
                "int64" | "utf8" | "timestamp(ms)" | "bool" | "binary" | "uint64" | "null" => {
                    column_ref
                }
                _ => format!("CAST({column_ref} AS VARCHAR)"),
            };
            projection.push(expr);
        }
        Ok(projection.join(", "))
    }

    fn fetch_pg_table(
        &self,
        target: PgTarget<'_>,
        sql: &str,
        timeout_seconds: u64,
    ) -> Result<String> {
        let output = Command::new("timeout")
            .arg(format!("{timeout_seconds}s"))
            .arg("psql")
            .env("PGPASSWORD", "")
            .arg("-h")
            .arg("127.0.0.1")
            .arg("-p")
            .arg(target.port.to_string())
            .arg("-U")
            .arg(target.user)
            .arg("-d")
            .arg(target.db)
            .arg("-At")
            .arg("-F")
            .arg("\t")
            .arg("-c")
            .arg(sql)
            .output()
            .context("run pg table query")?;
        if !output.status.success() {
            bail!("pg table query failed with {}", output.status);
        }
        Ok(String::from_utf8_lossy(&output.stdout)
            .trim_end_matches('\n')
            .to_string())
    }

    fn poll_pg_source_counts(&self, target: PgTarget<'_>, specs: &[RelationSpec]) -> Result<()> {
        let start = Instant::now();
        loop {
            if start.elapsed() >= self.config.poll_timeout {
                bail!("source counts did not reach targets before timeout");
            }
            let mut ready = true;
            for spec in specs {
                let sql = format!("SELECT row_count FROM {}", spec.relation);
                let count = self
                    .fetch_pg_scalar(target, &sql)
                    .ok()
                    .and_then(|raw| raw.parse::<u64>().ok())
                    .unwrap_or(0);
                if count < spec.target {
                    ready = false;
                    break;
                }
            }
            if ready {
                return Ok(());
            }
            thread::sleep(self.config.poll_interval);
        }
    }

    fn poll_pg_result_rows_equals(
        &self,
        target: PgTarget<'_>,
        expected_rows: u64,
        relation: &str,
    ) -> Result<()> {
        let start = Instant::now();
        loop {
            if start.elapsed() >= self.config.poll_timeout {
                bail!("result rows did not reach {expected_rows} before timeout");
            }
            let sql = format!("SELECT COUNT(*)::BIGINT FROM {relation}");
            let rows = self
                .fetch_pg_scalar(target, &sql)
                .ok()
                .and_then(|raw| raw.parse::<u64>().ok());
            if rows == Some(expected_rows) {
                return Ok(());
            }
            thread::sleep(self.config.poll_interval);
        }
    }

    fn poll_floe_query_completion(
        &self,
        sources: &[Source],
        groups: &Groups,
        topics: &Topics,
    ) -> Result<()> {
        let start = Instant::now();
        loop {
            if start.elapsed() >= self.config.poll_timeout {
                bail!("Floe Kafka consumer groups did not catch up before timeout");
            }
            let mut ready = true;
            for source in sources {
                let group_id = groups.for_source(*source);
                let topic = topics.for_source(*source);
                let target_rows = self.config.rows_for_source(*source);
                let status = self.kafka_group_topic_status(group_id, topic);
                match status {
                    Ok(group)
                        if group.current >= target_rows
                            && group.end >= target_rows
                            && group.lag == 0 => {}
                    _ => {
                        ready = false;
                        break;
                    }
                }
            }
            if ready {
                return Ok(());
            }
            thread::sleep(self.config.poll_interval);
        }
    }

    fn kafka_group_topic_status(&self, group_id: &str, topic: &str) -> Result<GroupStatus> {
        let output = run_capture(
            "docker",
            [
                "exec",
                &self.config.redpanda_container,
                "rpk",
                "group",
                "describe",
                group_id,
            ],
            None,
        )?;
        for line in output.lines() {
            let columns = line.split_whitespace().collect::<Vec<_>>();
            if columns.first() == Some(&topic) && columns.len() >= 6 {
                return Ok(GroupStatus {
                    current: columns[2].parse().unwrap_or(0),
                    end: columns[4].parse().unwrap_or(0),
                    lag: columns[5].parse().unwrap_or(u64::MAX),
                });
            }
        }
        bail!("topic {topic} not found in group {group_id}")
    }

    fn curl_json_file(
        &self,
        method: &str,
        url: &str,
        payload_path: &Path,
        artifact_dir: &Path,
        label: &str,
    ) -> Result<()> {
        let stdout = File::create(artifact_dir.join(format!("{label}.json")))?;
        let stderr = File::create(artifact_dir.join(format!("{label}.stderr.log")))?;
        let status = Command::new("curl")
            .arg("-fsS")
            .arg("-X")
            .arg(method)
            .arg(url)
            .arg("-H")
            .arg("Content-Type: application/json")
            .arg("--data-binary")
            .arg(format!("@{}", payload_path.display()))
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .status()
            .context("run curl json")?;
        ensure!(status.success(), "curl failed with {status}");
        Ok(())
    }

    fn feldera_query(&self, pipeline: &str, sql: &str) -> Result<serde_json::Value> {
        let url = format!(
            "http://127.0.0.1:{}/v0/pipelines/{pipeline}/query",
            self.config.feldera_http_port
        );
        let output = Command::new("curl")
            .arg("-fsS")
            .arg("--get")
            .arg(url)
            .arg("--data-urlencode")
            .arg(format!("sql={sql}"))
            .arg("--data-urlencode")
            .arg("format=json")
            .output()
            .context("query feldera")?;
        if !output.status.success() {
            bail!("Feldera query failed with {}", output.status);
        }
        parse_feldera_json_stream(&output.stdout).context("parse Feldera query JSON")
    }

    fn feldera_query_row_count(&self, pipeline: &str, sql: &str) -> Result<u64> {
        let value = self.feldera_query(pipeline, sql)?;
        parse_row_count_value(&value)
            .ok_or_else(|| anyhow!("Feldera query response missing row_count"))
    }

    fn compute_feldera_query_content_fingerprint(
        &self,
        pipeline: &str,
        artifact_dir: &Path,
        label: &str,
        sql: &str,
    ) -> Result<ContentFingerprint> {
        let value = self.feldera_query(pipeline, sql)?;
        let rows = value
            .as_array()
            .ok_or_else(|| anyhow!("Feldera query response was not an array"))?;
        let rows_json_file = artifact_dir.join(format!("{label}.rows.json"));
        fs::write(&rows_json_file, serde_json::to_vec_pretty(&value)?)?;
        let rows_jsonl_file = artifact_dir.join(format!("{label}.rows.jsonl"));
        let mut lines = Vec::with_capacity(rows.len());
        for row in rows {
            lines.push(canonical_json_line(row)?);
        }
        let rows_jsonl = if lines.is_empty() {
            String::new()
        } else {
            lines.join("\n") + "\n"
        };
        fs::write(&rows_jsonl_file, rows_jsonl)?;
        Ok(fingerprint_lines(lines))
    }

    fn poll_feldera_program_success(&self, pipeline: &str) -> Result<()> {
        for _ in 0..240 {
            let status = self.feldera_pipeline_field(pipeline, "program_status")?;
            match status.as_str() {
                "Success" => return Ok(()),
                "SqlError" | "RustError" | "SystemError" => {
                    bail!("Feldera program failed with status {status}");
                }
                _ => thread::sleep(Duration::from_secs(2)),
            }
        }
        bail!("Feldera program did not compile before timeout")
    }

    fn poll_feldera_running(&self, pipeline: &str) -> Result<()> {
        for _ in 0..120 {
            let status = self.feldera_pipeline_field(pipeline, "deployment_status")?;
            if status == "Running" {
                return Ok(());
            }
            thread::sleep(Duration::from_secs(1));
        }
        bail!("Feldera pipeline did not reach Running before timeout")
    }

    fn feldera_pipeline_field(&self, pipeline: &str, field: &str) -> Result<String> {
        let url = format!(
            "http://127.0.0.1:{}/v0/pipelines/{pipeline}",
            self.config.feldera_http_port
        );
        let output = run_capture("curl", ["-fsS", &url], None)?;
        let value: serde_json::Value = serde_json::from_str(&output)?;
        Ok(value
            .get(field)
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string())
    }

    fn poll_feldera_source_counts(&self, pipeline: &str, specs: &[RelationSpec]) -> Result<()> {
        let start = Instant::now();
        loop {
            if start.elapsed() >= self.config.poll_timeout {
                bail!("Feldera source counts did not reach targets before timeout");
            }
            let mut ready = true;
            for spec in specs {
                let sql = format!("SELECT row_count FROM {}", spec.relation);
                let count = self.feldera_query_row_count(pipeline, &sql).unwrap_or(0);
                if count < spec.target {
                    ready = false;
                    break;
                }
            }
            if ready {
                return Ok(());
            }
            thread::sleep(self.config.poll_interval);
        }
    }

    fn poll_feldera_result_rows_equals(&self, pipeline: &str, expected_rows: u64) -> Result<()> {
        let start = Instant::now();
        loop {
            if start.elapsed() >= self.config.poll_timeout {
                bail!("Feldera result rows did not reach {expected_rows} before timeout");
            }
            let rows = self
                .feldera_query_row_count(
                    pipeline,
                    "SELECT COUNT(*) AS row_count FROM benchmark_result",
                )
                .ok();
            if rows == Some(expected_rows) {
                return Ok(());
            }
            thread::sleep(self.config.poll_interval);
        }
    }

    fn append_summary_row(&self, row: SummaryRow<'_>) -> Result<()> {
        let source_rows_per_sec = row
            .source_catchup_ms
            .filter(|ms| *ms > 0)
            .map(|ms| row.input_rows as u128 * 1000 / ms)
            .unwrap_or(0);
        let result_rows_per_sec = row
            .result_ready_ms
            .filter(|ms| *ms > 0)
            .map(|ms| row.input_rows as u128 * 1000 / ms)
            .unwrap_or(0);

        let mut summary = OpenOptions::new()
            .append(true)
            .open(self.config.results_file())
            .context("open summary")?;
        writeln!(
            summary,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            row.engine.as_str(),
            row.query_id,
            row.status,
            seconds_cell(row.source_catchup_ms),
            seconds_cell(row.result_ready_ms),
            seconds_cell(row.produce_ms),
            seconds_cell(row.source_post_ms),
            seconds_cell(row.result_post_ms),
            if row.source_catchup_ms.is_some() {
                source_rows_per_sec.to_string()
            } else {
                "n/a".to_string()
            },
            if row.result_ready_ms.is_some() {
                result_rows_per_sec.to_string()
            } else {
                "n/a".to_string()
            },
            row.input_rows,
            row.result_rows
                .map(|rows| rows.to_string())
                .unwrap_or_else(|| "n/a".to_string()),
            row.notes,
        )?;

        let json = json!({
            "engine": row.engine.as_str(),
            "query_id": row.query_id,
            "status": row.status,
            "timing": {
                "source_catchup_ms": row.source_catchup_ms.unwrap_or(0),
                "result_ready_ms": row.result_ready_ms.unwrap_or(0),
                "produce_ms": row.produce_ms.unwrap_or(0),
                "source_post_produce_wait_ms": row.source_post_ms.unwrap_or(0),
                "result_post_produce_wait_ms": row.result_post_ms.unwrap_or(0),
            },
            "throughput": {
                "source_catchup_input_rows_per_sec": source_rows_per_sec,
                "result_ready_input_rows_per_sec": result_rows_per_sec,
                "input_rows_per_sec": source_rows_per_sec,
            },
            "rows": {
                "input_rows": row.input_rows,
                "result_rows": row.result_rows.unwrap_or(0),
            },
            "notes": row.notes,
        });
        let mut jsonl = OpenOptions::new()
            .append(true)
            .open(self.config.results_jsonl())
            .context("open results jsonl")?;
        writeln!(jsonl, "{}", serde_json::to_string(&json)?)?;
        Ok(())
    }

    fn record_failure(
        &self,
        engine: Engine,
        query_id: &str,
        notes: &str,
        input_rows: u64,
    ) -> Result<()> {
        self.append_summary_row(SummaryRow {
            engine,
            query_id,
            status: "failed",
            source_catchup_ms: None,
            result_ready_ms: None,
            produce_ms: None,
            source_post_ms: None,
            result_post_ms: None,
            input_rows,
            result_rows: None,
            notes: notes.to_string(),
        })
    }

    fn summarize_floe_hotspots(&self, artifact_dir: &Path) -> Result<String> {
        let mut text = String::new();
        for name in ["floe-node.stdout.log", "floe-node.stderr.log"] {
            let path = artifact_dir.join(name);
            if let Ok(content) = fs::read_to_string(path) {
                text.push_str(&content);
                text.push('\n');
            }
        }

        let mut stats: BTreeMap<String, HotspotStats> = BTreeMap::new();
        for line in text.lines() {
            if !line.contains("materialized view optimization hotspot") {
                continue;
            }
            let path = token_value(line, "path=").unwrap_or_default();
            let phase = token_value(line, "hotspot_phase=").unwrap_or_default();
            if path.is_empty() || phase.is_empty() {
                continue;
            }
            let share = token_value(line, "hotspot_phase_share=")
                .and_then(|raw| raw.parse::<f64>().ok())
                .unwrap_or(0.0);
            let total = token_value(line, "total_ms=")
                .and_then(|raw| raw.parse::<u64>().ok())
                .unwrap_or(0);
            let key = format!("{path}:{phase}");
            let entry = stats.entry(key).or_default();
            entry.count += 1;
            entry.share_sum += share;
            entry.max_total_ms = entry.max_total_ms.max(total);
        }

        if stats.is_empty() {
            return Ok(String::new());
        }

        let mut rows = stats.into_iter().collect::<Vec<_>>();
        rows.sort_by(|(_, left), (_, right)| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| right.avg_share().total_cmp(&left.avg_share()))
        });
        let mut report = String::new();
        for (key, stat) in &rows {
            report.push_str(&format!(
                "{key} count={} avg_share={:.3} max_total_ms={}\n",
                stat.count,
                stat.avg_share(),
                stat.max_total_ms
            ));
        }
        fs::write(artifact_dir.join("floe_optimization_hotspots.txt"), report)?;
        let (top_key, top_stat) = &rows[0];
        Ok(format!(
            "hotspot={}(avg_share={:.3})",
            top_key,
            top_stat.avg_share()
        ))
    }

    fn stop_container(&self, container: &str) {
        let _ = run_status("docker", ["rm", "-f", container], None);
    }

    fn stop_floe_process(&mut self) {
        if let Some(mut child) = self.floe_child.take() {
            let pid = child.id().to_string();
            let _ = run_status("kill", ["-INT", &pid], None);
            for _ in 0..50 {
                if child.try_wait().ok().flatten().is_some() {
                    return;
                }
                thread::sleep(Duration::from_millis(100));
            }
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = run_status("pkill", ["-f", "/target/release/floe-node run"], None);
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.stop_floe_process();
        if self.config.keep_containers {
            return;
        }
        self.stop_container(&self.config.materialize_container);
        self.stop_container(&self.config.risingwave_container);
        self.stop_container(&self.config.feldera_container);
        self.stop_container(&self.config.redpanda_container);
        let _ = run_status("docker", ["network", "rm", &self.config.network_name], None);
    }
}

#[derive(Debug, Clone)]
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

fn selected_queries(selector: &str) -> Result<Vec<String>> {
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

fn required_sources_for_query(query_id: &str) -> Vec<Source> {
    match query_id {
        "q3" => vec![Source::Auction, Source::Person],
        "q4" | "q6" | "q9" | "q13" | "q20" => vec![Source::Bid, Source::Auction],
        "q8" => vec![Source::Person],
        _ => vec![Source::Bid],
    }
}

fn relation_specs_for_sources(
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

fn expected_result_rows_for_query(config: &Config, query_id: &str) -> Option<u64> {
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

fn query_sql(query_id: &str) -> Option<&'static str> {
    Some(match query_id {
        "q0" => r#"SELECT auction, bidder, price, channel, url, "dateTime", extra FROM bid"#,
        "q1" => {
            r#"SELECT auction, bidder, price * 89 / 100 AS converted_price, "dateTime", extra FROM bid"#
        }
        "q2" => r#"SELECT auction, price FROM bid WHERE auction % 123 = 0"#,
        "q3" => {
            r#"SELECT p.name, p.city, p.state, a.id FROM auction AS a JOIN person AS p ON a.seller = p.id WHERE a.category = 10 AND p.state IN ('or', 'id', 'ca')"#
        }
        "q4" => {
            r#"SELECT category, AVG(max) FROM (SELECT MAX(b.price) AS max, a.category FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires GROUP BY a.id, a.category) per_auction GROUP BY category"#
        }
        "q5" => {
            r#"SELECT auction, COUNT(*) AS num FROM bid GROUP BY auction, HOP("dateTime", 2000, 10000)"#
        }
        "q6" => {
            r#"SELECT seller, AVG(price) AS moving_avg_price FROM (SELECT a.seller, b.price, b."dateTime", ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC, b."dateTime" ASC, b.bidder ASC, b.channel ASC, b.url ASC, b.extra ASC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires) ranked WHERE rownum <= 1 GROUP BY seller"#
        }
        "q7" => r#"SELECT MAX(price) AS maxprice FROM bid GROUP BY TUMBLE("dateTime", 10000)"#,
        "q8" => {
            r#"SELECT id, name, COUNT(*) AS person_count FROM person GROUP BY id, name, TUMBLE("dateTime", 10000)"#
        }
        "q9" => {
            r#"SELECT id, "itemName", description, "initialBid", reserve, "dateTime", expires, seller, category, extra, auction, bidder, price, "bidTime", "bidExtra" FROM (SELECT a.id, a."itemName", a.description, a."initialBid", a.reserve, a."dateTime", a.expires, a.seller, a.category, a.extra, b.auction, b.bidder, b.price, b."dateTime" AS "bidTime", b.extra AS "bidExtra", ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b."dateTime" ASC, b.bidder ASC, b.extra ASC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires) ranked WHERE rownum <= 1"#
        }
        "q12" => {
            r#"SELECT bidder, COUNT(*) AS bid_count FROM bid GROUP BY bidder, TUMBLE("dateTime", 10000)"#
        }
        "q13" => {
            r#"SELECT b.auction, b.bidder, b.price, b."dateTime", a.seller AS value FROM (SELECT *, PROCTIME() AS p_time FROM bid) b JOIN auction AS a ON b.auction = a.id WHERE b.auction % 10000 = a.id % 10000"#
        }
        "q14" => {
            r#"SELECT auction, bidder, price * 908 / 1000 AS price, CASE WHEN HOUR("dateTime") >= 8 AND HOUR("dateTime") <= 18 THEN 'dayTime' WHEN HOUR("dateTime") <= 6 OR HOUR("dateTime") >= 20 THEN 'nightTime' ELSE 'otherTime' END AS bid_time_type, "dateTime", extra, COUNT_CHAR(extra, 'c') AS c_counts FROM bid WHERE price * 908 / 1000 > 1000000 AND price * 908 / 1000 < 50000000"#
        }
        "q15" => {
            r#"SELECT DATE_FORMAT("dateTime", 'yyyy-MM-dd') AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY DATE_FORMAT("dateTime", 'yyyy-MM-dd')"#
        }
        "q16" => {
            r#"SELECT channel, DATE_FORMAT("dateTime", 'yyyy-MM-dd') AS day, MAX(DATE_FORMAT("dateTime", 'HH:mm')) AS minute, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY channel, DATE_FORMAT("dateTime", 'yyyy-MM-dd')"#
        }
        "q17" => {
            r#"SELECT auction, DATE_FORMAT("dateTime", 'yyyy-MM-dd') AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, MIN(price) AS min_price, MAX(price) AS max_price, AVG(price) AS avg_price, SUM(price) AS sum_price FROM bid GROUP BY auction, DATE_FORMAT("dateTime", 'yyyy-MM-dd')"#
        }
        "q18" => {
            r#"SELECT auction, bidder, price, channel, url, "dateTime", extra FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY "dateTime" DESC, price DESC, channel ASC, url ASC, extra ASC) AS rank_number FROM bid) dedup WHERE rank_number <= 1"#
        }
        "q19" => {
            r#"SELECT auction, bidder, price, channel, url, "dateTime", extra FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC, "dateTime" ASC, bidder ASC, channel ASC, url ASC, extra ASC) AS rank_number FROM bid) ranked WHERE rank_number <= 10"#
        }
        "q20" => {
            r#"SELECT b.auction, b.bidder, b.price, b.channel, b.url, b."dateTime", b.extra, a."itemName", a.description, a."initialBid", a.reserve, a."dateTime" AS auction_time, a.expires, a.seller, a.category, a.extra AS auction_extra FROM bid AS b JOIN auction AS a ON b.auction = a.id WHERE a.category = 10"#
        }
        "q21" => {
            r#"SELECT auction, bidder, price, channel, CASE WHEN lower(channel) = 'apple' THEN '0' WHEN lower(channel) = 'google' THEN '1' WHEN lower(channel) = 'facebook' THEN '2' WHEN lower(channel) = 'baidu' THEN '3' ELSE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) END AS channel_id FROM bid WHERE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) IS NOT NULL OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')"#
        }
        "q22" => {
            r#"SELECT auction, bidder, price, channel, SPLIT_INDEX(url, '/', 3) AS dir1, SPLIT_INDEX(url, '/', 4) AS dir2, SPLIT_INDEX(url, '/', 5) AS dir3 FROM bid"#
        }
        _ => return None,
    })
}

fn query_sql_portable(query_id: &str) -> Option<&'static str> {
    Some(match query_id {
        "q5" => {
            r#"SELECT auction, COUNT(*) AS num
FROM (
  SELECT auction, (("dateTime" / 2000) * 2000 - 0) AS hop_start FROM bid
  UNION ALL
  SELECT auction, (("dateTime" / 2000) * 2000 - 2000) AS hop_start FROM bid
  UNION ALL
  SELECT auction, (("dateTime" / 2000) * 2000 - 4000) AS hop_start FROM bid
  UNION ALL
  SELECT auction, (("dateTime" / 2000) * 2000 - 6000) AS hop_start FROM bid
  UNION ALL
  SELECT auction, (("dateTime" / 2000) * 2000 - 8000) AS hop_start FROM bid
) expanded
GROUP BY auction, hop_start"#
        }
        "q7" => r#"SELECT MAX(price) AS maxprice FROM bid GROUP BY ("dateTime" / 10000)"#,
        "q8" => {
            r#"SELECT id, name, COUNT(*) AS person_count FROM person GROUP BY id, name, ("dateTime" / 10000)"#
        }
        "q12" => {
            r#"SELECT bidder, COUNT(*) AS bid_count FROM bid GROUP BY bidder, ("dateTime" / 10000)"#
        }
        "q13" => {
            r#"SELECT b.auction, b.bidder, b.price, b."dateTime", a.seller AS value FROM bid AS b JOIN auction AS a ON b.auction = a.id WHERE b.auction % 10000 = a.id % 10000"#
        }
        "q14" => {
            r#"SELECT auction, bidder, price * 908 / 1000 AS price, CASE WHEN (("dateTime" / 3600000) % 24) >= 8 AND (("dateTime" / 3600000) % 24) <= 18 THEN 'dayTime' WHEN (("dateTime" / 3600000) % 24) <= 6 OR (("dateTime" / 3600000) % 24) >= 20 THEN 'nightTime' ELSE 'otherTime' END AS bid_time_type, "dateTime", extra, LENGTH(extra) - LENGTH(REPLACE(extra, 'c', '')) AS c_counts FROM bid WHERE price * 908 / 1000 > 1000000 AND price * 908 / 1000 < 50000000"#
        }
        "q15" => {
            r#"SELECT ("dateTime" / 86400000) AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY ("dateTime" / 86400000)"#
        }
        "q16" => {
            r#"SELECT channel, ("dateTime" / 86400000) AS day, MAX((("dateTime" / 60000) % 1440)) AS minute, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY channel, ("dateTime" / 86400000)"#
        }
        "q17" => {
            r#"SELECT auction, ("dateTime" / 86400000) AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, MIN(price) AS min_price, MAX(price) AS max_price, AVG(price) AS avg_price, SUM(price) AS sum_price FROM bid GROUP BY auction, ("dateTime" / 86400000)"#
        }
        "q21" => {
            r#"SELECT auction, bidder, price, channel, CASE WHEN lower(channel) = 'apple' THEN '0' WHEN lower(channel) = 'google' THEN '1' WHEN lower(channel) = 'facebook' THEN '2' WHEN lower(channel) = 'baidu' THEN '3' ELSE NULLIF(SPLIT_PART(SPLIT_PART(url, 'channel_id=', 2), '&', 1), '') END AS channel_id FROM bid WHERE NULLIF(SPLIT_PART(SPLIT_PART(url, 'channel_id=', 2), '&', 1), '') IS NOT NULL OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')"#
        }
        "q22" => {
            r#"SELECT auction, bidder, price, channel, SPLIT_PART(url, '/', 4) AS dir1, SPLIT_PART(url, '/', 5) AS dir2, SPLIT_PART(url, '/', 6) AS dir3 FROM bid"#
        }
        _ => query_sql(query_id)?,
    })
}

fn query_sql_for_engine(engine: Engine, query_id: &str) -> Option<&'static str> {
    match engine {
        Engine::RisingWave | Engine::Feldera | Engine::Materialize => query_sql_portable(query_id),
        Engine::Floe => query_sql_floe(query_id),
    }
}

fn query_sql_floe(query_id: &str) -> Option<&'static str> {
    Some(match query_id {
        "q0" => {
            r#"SELECT auction, bidder, price, channel, url, date_time AS "dateTime", extra FROM nexmark_bid"#
        }
        "q1" => {
            r#"SELECT auction, bidder, price * 89 / 100 AS converted_price, date_time AS "dateTime", extra FROM nexmark_bid"#
        }
        "q2" => r#"SELECT auction, price FROM nexmark_bid WHERE auction % 123 = 0"#,
        "q3" => {
            r#"SELECT p.name, p.city, p.state, a.id FROM nexmark_auction AS a JOIN nexmark_person AS p ON a.seller = p.id WHERE a.category = 10 AND p.state IN ('or', 'id', 'ca')"#
        }
        "q4" => {
            r#"SELECT category, CAST(AVG(max) AS BIGINT) AS avg_price FROM (SELECT MAX(b.price) AS max, a.category FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction WHERE b.date_time BETWEEN a.date_time AND a.expires GROUP BY a.id, a.category) per_auction GROUP BY category"#
        }
        "q5" => {
            r#"SELECT auction, COUNT(*) AS num FROM nexmark_bid GROUP BY auction, HOP(date_time, 2000, 10000)"#
        }
        "q6" => {
            r#"SELECT seller, CAST(AVG(price) AS BIGINT) AS moving_avg_price FROM (SELECT a.seller, b.price, b.date_time, ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC, b.date_time ASC, b.bidder ASC, b.channel ASC, b.url ASC, b.extra ASC) AS rownum FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction WHERE b.date_time BETWEEN a.date_time AND a.expires) ranked WHERE rownum <= 1 GROUP BY seller"#
        }
        "q7" => {
            r#"SELECT MAX(price) AS maxprice FROM nexmark_bid GROUP BY TUMBLE(date_time, 10000)"#
        }
        "q8" => {
            r#"SELECT id, name, COUNT(*) AS person_count FROM nexmark_person GROUP BY id, name, TUMBLE(date_time, 10000)"#
        }
        "q9" => {
            r#"SELECT id, "itemName", description, "initialBid", reserve, "dateTime", expires, seller, category, extra, auction, bidder, price, "bidTime", "bidExtra" FROM (SELECT a.id, a.item_name AS "itemName", a.description, a.initial_bid AS "initialBid", a.reserve, a.auction_time AS "dateTime", a.expires, a.seller, a.category, a.auction_extra AS extra, b.auction, b.bidder, b.price, b.bid_time AS "bidTime", b.bid_extra AS "bidExtra", ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b.bid_time ASC, b.bidder ASC, b.bid_extra ASC) AS rownum FROM (SELECT id, item_name, description, initial_bid, reserve, date_time AS auction_time, expires, seller, category, extra AS auction_extra FROM nexmark_auction) a JOIN (SELECT auction, bidder, price, date_time AS bid_time, extra AS bid_extra FROM nexmark_bid) b ON a.id = b.auction WHERE b.bid_time BETWEEN a.auction_time AND a.expires) ranked WHERE rownum <= 1"#
        }
        "q12" => {
            r#"SELECT bidder, COUNT(*) AS bid_count FROM nexmark_bid GROUP BY bidder, TUMBLE(date_time, 10000)"#
        }
        "q13" => {
            r#"SELECT b.auction, b.bidder, b.price, b.date_time AS "dateTime", a.seller AS value FROM (SELECT *, PROCTIME() AS p_time FROM nexmark_bid) b JOIN nexmark_auction AS a ON b.auction = a.id WHERE b.auction % 10000 = a.id % 10000"#
        }
        "q14" => {
            r#"SELECT auction, bidder, price * 908 / 1000 AS price, CASE WHEN HOUR(date_time) >= 8 AND HOUR(date_time) <= 18 THEN 'dayTime' WHEN HOUR(date_time) <= 6 OR HOUR(date_time) >= 20 THEN 'nightTime' ELSE 'otherTime' END AS bid_time_type, date_time AS "dateTime", extra, COUNT_CHAR(extra, 'c') AS c_counts FROM nexmark_bid WHERE price * 908 / 1000 > 1000000 AND price * 908 / 1000 < 50000000"#
        }
        "q15" => {
            r#"SELECT DATE_FORMAT(date_time, 'yyyy-MM-dd') AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM nexmark_bid GROUP BY DATE_FORMAT(date_time, 'yyyy-MM-dd')"#
        }
        "q16" => {
            r#"SELECT channel, DATE_FORMAT(date_time, 'yyyy-MM-dd') AS day, MAX(DATE_FORMAT(date_time, 'HH:mm')) AS minute, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM nexmark_bid GROUP BY channel, DATE_FORMAT(date_time, 'yyyy-MM-dd')"#
        }
        "q17" => {
            r#"SELECT auction, DATE_FORMAT(date_time, 'yyyy-MM-dd') AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, MIN(price) AS min_price, MAX(price) AS max_price, CAST(AVG(price) AS BIGINT) AS avg_price, SUM(price) AS sum_price FROM nexmark_bid GROUP BY auction, DATE_FORMAT(date_time, 'yyyy-MM-dd')"#
        }
        "q18" => {
            r#"SELECT auction, bidder, price, channel, url, "dateTime", extra FROM (SELECT auction, bidder, price, channel, url, date_time AS "dateTime", extra, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY date_time DESC, price DESC, channel ASC, url ASC, extra ASC) AS rank_number FROM nexmark_bid) dedup WHERE rank_number <= 1"#
        }
        "q19" => {
            r#"SELECT auction, bidder, price, channel, url, "dateTime", extra FROM (SELECT auction, bidder, price, channel, url, date_time AS "dateTime", extra, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC, date_time ASC, bidder ASC, channel ASC, url ASC, extra ASC) AS rank_number FROM nexmark_bid) ranked WHERE rank_number <= 10"#
        }
        "q20" => {
            r#"SELECT b.auction, b.bidder, b.price, b.channel, b.url, b.date_time AS "dateTime", b.extra, a.item_name AS "itemName", a.description, a.initial_bid AS "initialBid", a.reserve, a.date_time AS auction_time, a.expires, a.seller, a.category, a.extra AS auction_extra FROM nexmark_bid AS b JOIN nexmark_auction AS a ON b.auction = a.id WHERE a.category = 10"#
        }
        "q21" => {
            r#"SELECT auction, bidder, price, channel, CASE WHEN lower(channel) = 'apple' THEN '0' WHEN lower(channel) = 'google' THEN '1' WHEN lower(channel) = 'facebook' THEN '2' WHEN lower(channel) = 'baidu' THEN '3' ELSE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) END AS channel_id FROM nexmark_bid WHERE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) IS NOT NULL OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')"#
        }
        "q22" => {
            r#"SELECT auction, bidder, price, channel, SPLIT_INDEX(url, '/', 3) AS dir1, SPLIT_INDEX(url, '/', 4) AS dir2, SPLIT_INDEX(url, '/', 5) AS dir3 FROM nexmark_bid"#
        }
        _ => return None,
    })
}

fn write_materialize_setup_sql(
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

fn append_count_view(sql: &mut String, view: &str, source: &str, indexed_view: bool) {
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

fn write_risingwave_setup_sql(
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

fn feldera_program_sql(
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

fn floe_config_json(
    config: &Config,
    sources: &[Source],
    topics: &Topics,
    groups: &Groups,
) -> serde_json::Value {
    let mut connectors = Vec::new();
    for source in sources {
        connectors.push(json!({
            "type": "kafka",
            "brokers": config.broker_addr,
            "topics": [topics.for_source(*source)],
            "group_id": groups.for_source(*source),
            "default_source": source.floe_source(),
            "poll_ms": config.floe_kafka_poll_ms,
            "max_messages_per_tick": config.floe_kafka_max_messages_per_tick,
        }));
    }
    json!({
        "connectors": connectors,
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

fn floe_program_sql(query_id: &str, _sources: &[Source]) -> Result<String> {
    let query_text =
        query_sql_floe(query_id).with_context(|| format!("Floe query SQL for {query_id}"))?;
    Ok(format!(
        "CREATE MATERIALIZED VIEW benchmark_result AS\n{query_text};\n"
    ))
}

fn floe_expected_query_text_for_source_tables(
    query_id: &str,
    sources: &[Source],
) -> Result<String> {
    match query_id {
        "q5" | "q7" | "q8" | "q12" => {
            let query_text = query_sql_portable(query_id)
                .with_context(|| format!("portable query SQL for {query_id}"))?;
            Ok(wrap_query_with_source_ctes(query_text, sources, true))
        }
        "q13" => {
            let query_text = query_sql_portable(query_id)
                .with_context(|| format!("portable query SQL for {query_id}"))?;
            Ok(wrap_query_with_source_ctes(query_text, sources, false))
        }
        _ => {
            let query_text = query_sql_floe(query_id)
                .with_context(|| format!("Floe query SQL for {query_id}"))?;
            Ok(query_text.to_string())
        }
    }
}

fn wrap_query_with_source_ctes(
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

fn parse_row_count_value(value: &serde_json::Value) -> Option<u64> {
    if let Some(rows) = value.as_array() {
        return parse_row_count_row(rows.first()?);
    }
    parse_row_count_row(value)
}

fn parse_row_count_row(row: &serde_json::Value) -> Option<u64> {
    row.get("ROW_COUNT")
        .or_else(|| row.get("row_count"))
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
}

fn verify_result_content_hash(
    engine: Engine,
    query_id: &str,
    observed: &ContentFingerprint,
    expected: &ContentFingerprint,
    artifact_dir: &Path,
) -> Result<()> {
    let report = format!(
        "engine={}\nquery_id={}\nobserved_result_rows={}\nobserved_content_sha256={}\nexpected_result_rows={}\nexpected_content_sha256={}\n",
        engine.as_str(),
        query_id,
        observed.row_count,
        observed.hash,
        expected.row_count,
        expected.hash
    );
    fs::write(artifact_dir.join("content_hash.txt"), report)?;
    if observed.row_count != expected.row_count || observed.hash != expected.hash {
        fs::write(
            artifact_dir.join("correctness.error"),
            format!(
                "expected_result_rows={}\nexpected_content_sha256={}\nobserved_result_rows={}\nobserved_content_sha256={}\nquery_id={}\nengine={}\n",
                expected.row_count,
                expected.hash,
                observed.row_count,
                observed.hash,
                query_id,
                engine.as_str()
            ),
        )?;
        bail!("content hash mismatch for {} {query_id}", engine.as_str());
    }
    Ok(())
}

fn fingerprint_file_lines(path: &Path) -> Result<ContentFingerprint> {
    let content = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let lines = content.lines().map(ToOwned::to_owned).collect::<Vec<_>>();
    Ok(fingerprint_lines(lines))
}

fn fingerprint_lines(mut lines: Vec<String>) -> ContentFingerprint {
    lines.sort();
    let mut hasher = Sha256::new();
    for line in &lines {
        hasher.update(line.as_bytes());
        hasher.update(b"\n");
    }
    ContentFingerprint {
        row_count: lines.len() as u64,
        hash: hex::encode(hasher.finalize()),
    }
}

fn deterministic_nexmark_q5_fingerprint(bid_rows: u64) -> ContentFingerprint {
    let full_cycles = bid_rows / NEXMARK_BID_AUCTION_CARDINALITY;
    let remainder = bid_rows % NEXMARK_BID_AUCTION_CARDINALITY;
    let mut lines = (1..=NEXMARK_BID_AUCTION_CARDINALITY)
        .map(|auction| {
            let bids_for_auction = full_cycles + u64::from(auction <= remainder);
            (format!("{auction}\t1"), bids_for_auction * 5)
        })
        .filter(|(_, repetitions)| *repetitions > 0)
        .collect::<Vec<_>>();
    lines.sort_by(|left, right| left.0.cmp(&right.0));

    let mut hasher = Sha256::new();
    let mut row_count = 0_u64;
    for (line, repetitions) in lines {
        for _ in 0..repetitions {
            hasher.update(line.as_bytes());
            hasher.update(b"\n");
        }
        row_count += repetitions;
    }

    ContentFingerprint {
        row_count,
        hash: hex::encode(hasher.finalize()),
    }
}

fn deterministic_nexmark_q15_fingerprint(bid_rows: u64) -> ContentFingerprint {
    #[derive(Default)]
    struct Stats {
        total_bids: u64,
        rank1_bids: u64,
        rank2_bids: u64,
        rank3_bids: u64,
        total_auctions: BTreeSet<i64>,
        rank1_auctions: BTreeSet<i64>,
        rank2_auctions: BTreeSet<i64>,
        rank3_auctions: BTreeSet<i64>,
    }

    let mut stats_by_day = BTreeMap::<String, Stats>::new();
    for bid_idx in 1..=bid_rows {
        let row = deterministic_bid_row(bid_idx);
        let stats = stats_by_day.entry(row.day).or_default();
        stats.total_bids += 1;
        stats.total_auctions.insert(row.auction);
        match price_rank(row.price) {
            1 => {
                stats.rank1_bids += 1;
                stats.rank1_auctions.insert(row.auction);
            }
            2 => {
                stats.rank2_bids += 1;
                stats.rank2_auctions.insert(row.auction);
            }
            3 => {
                stats.rank3_bids += 1;
                stats.rank3_auctions.insert(row.auction);
            }
            _ => unreachable!("validated price rank"),
        }
    }

    fingerprint_lines(
        stats_by_day
            .into_iter()
            .map(|(day, stats)| {
                format!(
                    "{day}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    stats.total_bids,
                    stats.rank1_bids,
                    stats.rank2_bids,
                    stats.rank3_bids,
                    stats.total_bids,
                    stats.rank1_bids,
                    stats.rank2_bids,
                    stats.rank3_bids,
                    stats.total_auctions.len(),
                    stats.rank1_auctions.len(),
                    stats.rank2_auctions.len(),
                    stats.rank3_auctions.len()
                )
            })
            .collect(),
    )
}

fn deterministic_nexmark_q16_fingerprint(bid_rows: u64) -> ContentFingerprint {
    #[derive(Default)]
    struct Stats {
        max_minute: Option<String>,
        total_bids: u64,
        rank1_bids: u64,
        rank2_bids: u64,
        rank3_bids: u64,
        total_auctions: BTreeSet<i64>,
        rank1_auctions: BTreeSet<i64>,
        rank2_auctions: BTreeSet<i64>,
        rank3_auctions: BTreeSet<i64>,
    }

    let mut stats_by_group = BTreeMap::<(String, String), Stats>::new();
    for bid_idx in 1..=bid_rows {
        let row = deterministic_bid_row(bid_idx);
        let stats = stats_by_group
            .entry((row.channel.to_string(), row.day))
            .or_default();
        stats.max_minute = Some(match stats.max_minute.take() {
            Some(existing) => existing.max(row.minute),
            None => row.minute,
        });
        stats.total_bids += 1;
        stats.total_auctions.insert(row.auction);
        match price_rank(row.price) {
            1 => {
                stats.rank1_bids += 1;
                stats.rank1_auctions.insert(row.auction);
            }
            2 => {
                stats.rank2_bids += 1;
                stats.rank2_auctions.insert(row.auction);
            }
            3 => {
                stats.rank3_bids += 1;
                stats.rank3_auctions.insert(row.auction);
            }
            _ => unreachable!("validated price rank"),
        }
    }

    fingerprint_lines(
        stats_by_group
            .into_iter()
            .map(|((channel, day), stats)| {
                let minute = stats.max_minute.unwrap_or_default();
                format!(
                    "{channel}\t{day}\t{minute}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    stats.total_bids,
                    stats.rank1_bids,
                    stats.rank2_bids,
                    stats.rank3_bids,
                    stats.total_bids,
                    stats.rank1_bids,
                    stats.rank2_bids,
                    stats.rank3_bids,
                    stats.total_auctions.len(),
                    stats.rank1_auctions.len(),
                    stats.rank2_auctions.len(),
                    stats.rank3_auctions.len()
                )
            })
            .collect(),
    )
}

fn deterministic_nexmark_q17_fingerprint(bid_rows: u64) -> ContentFingerprint {
    #[derive(Default)]
    struct Stats {
        total_bids: u64,
        rank1_bids: u64,
        rank2_bids: u64,
        rank3_bids: u64,
        min_price: Option<i64>,
        max_price: Option<i64>,
        sum_price: i64,
    }

    let mut stats_by_group = BTreeMap::<(i64, String), Stats>::new();
    for bid_idx in 1..=bid_rows {
        let row = deterministic_bid_row(bid_idx);
        let stats = stats_by_group.entry((row.auction, row.day)).or_default();
        stats.total_bids += 1;
        match price_rank(row.price) {
            1 => stats.rank1_bids += 1,
            2 => stats.rank2_bids += 1,
            3 => stats.rank3_bids += 1,
            _ => unreachable!("validated price rank"),
        }
        stats.min_price = Some(
            stats
                .min_price
                .map_or(row.price, |value| value.min(row.price)),
        );
        stats.max_price = Some(
            stats
                .max_price
                .map_or(row.price, |value| value.max(row.price)),
        );
        stats.sum_price += row.price;
    }

    fingerprint_lines(
        stats_by_group
            .into_iter()
            .map(|((auction, day), stats)| {
                let avg_price = if stats.total_bids == 0 {
                    0
                } else {
                    stats.sum_price / i64::try_from(stats.total_bids).unwrap_or(1)
                };
                format!(
                    "{auction}\t{day}\t{}\t{}\t{}\t{}\t{}\t{}\t{avg_price}\t{}",
                    stats.total_bids,
                    stats.rank1_bids,
                    stats.rank2_bids,
                    stats.rank3_bids,
                    stats.min_price.unwrap_or_default(),
                    stats.max_price.unwrap_or_default(),
                    stats.sum_price
                )
            })
            .collect(),
    )
}

struct DeterministicBidRow {
    auction: i64,
    price: i64,
    channel: &'static str,
    day: String,
    minute: String,
}

fn deterministic_bid_row(bid_idx: u64) -> DeterministicBidRow {
    let bid_idx_i64 = i64::try_from(bid_idx).unwrap_or(i64::MAX);
    let auction =
        i64::try_from((bid_idx - 1) % NEXMARK_BID_AUCTION_CARDINALITY + 1).unwrap_or(i64::MAX);
    let price = 1_000 + (bid_idx_i64 % 50_000);
    let channel = match bid_idx % 5 {
        0 => "web",
        1 => "apple",
        2 => "google",
        3 => "facebook",
        _ => "baidu",
    };
    let timestamp = DateTime::<Utc>::from_timestamp_millis(NEXMARK_BASE_TS_MS + bid_idx_i64)
        .expect("deterministic Nexmark timestamp is in range");
    DeterministicBidRow {
        auction,
        price,
        channel,
        day: timestamp.format("%Y-%m-%d").to_string(),
        minute: timestamp.format("%H:%M").to_string(),
    }
}

fn price_rank(price: i64) -> u8 {
    if price < 10_000 {
        1
    } else if price < 1_000_000 {
        2
    } else {
        3
    }
}

fn canonical_json_line(value: &serde_json::Value) -> Result<String> {
    let canonical = canonical_json_value(value);
    serde_json::to_string(&canonical).context("serialize canonical JSON row")
}

fn canonical_json_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(map) => {
            let mut sorted = serde_json::Map::new();
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                sorted.insert(key.clone(), canonical_json_value(&map[key]));
            }
            serde_json::Value::Object(sorted)
        }
        other => other.clone(),
    }
}

fn parse_feldera_json_stream(bytes: &[u8]) -> Result<serde_json::Value> {
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) {
        return Ok(value);
    }

    let text = String::from_utf8_lossy(bytes);
    let mut rows = Vec::new();
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        rows.push(
            serde_json::from_str::<serde_json::Value>(line)
                .with_context(|| format!("parse Feldera JSON line: {line}"))?,
        );
    }
    Ok(serde_json::Value::Array(rows))
}

fn command_success<I, S>(program: impl AsRef<OsStr>, args: I, cwd: Option<&Path>) -> Result<bool>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let status = command(program, args, cwd)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("run command")?;
    Ok(status.success())
}

fn run_status<I, S>(program: impl AsRef<OsStr>, args: I, cwd: Option<&Path>) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let status = command(program, args, cwd)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("run command")?;
    ensure_status(status)
}

fn run_status_vec(program: impl AsRef<OsStr>, args: &[String], cwd: Option<&Path>) -> Result<()> {
    let status = command(program, args, cwd)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("run command")?;
    ensure_status(status)
}

fn run_capture<I, S>(program: impl AsRef<OsStr>, args: I, cwd: Option<&Path>) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = command(program, args, cwd)
        .output()
        .context("run command")?;
    if !output.status.success() {
        bail!("command failed with {}", output.status);
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn command<I, S>(program: impl AsRef<OsStr>, args: I, cwd: Option<&Path>) -> Command
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = Command::new(program);
    command.args(args);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    command
}

fn ensure_status(status: ExitStatus) -> Result<()> {
    ensure!(status.success(), "command failed with {status}");
    Ok(())
}

fn env_string(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_nonempty(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

fn env_path(name: &str) -> Option<PathBuf> {
    env_nonempty(name).map(PathBuf::from)
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.as_str(),
                "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
            )
        })
        .unwrap_or(default)
}

fn env_parse<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match env::var(name) {
        Ok(value) => value
            .parse::<T>()
            .map_err(|err| anyhow!("parse {name}={value}: {err}")),
        Err(_) => Ok(default),
    }
}

fn repo_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow!("cannot derive repo root from CARGO_MANIFEST_DIR"))
}

fn current_millis() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before UNIX_EPOCH")?
        .as_millis())
}

fn source_labels(sources: &[Source]) -> String {
    sources
        .iter()
        .map(|source| source.label())
        .collect::<Vec<_>>()
        .join(" ")
}

fn validate_identifier(identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        bail!("empty SQL identifier");
    };
    ensure!(
        first == '_' || first.is_ascii_alphabetic(),
        "invalid SQL identifier '{identifier}'"
    );
    ensure!(
        chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()),
        "invalid SQL identifier '{identifier}'"
    );
    Ok(())
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn seconds_cell(ms: Option<u128>) -> String {
    ms.map(|ms| format!("{:.3}", ms as f64 / 1000.0))
        .unwrap_or_else(|| "n/a".to_string())
}

fn log(message: impl AsRef<str>) {
    println!("[nexmark-cross-engine] {}", message.as_ref());
}

fn token_value(line: &str, prefix: &str) -> Option<String> {
    for token in line.split_whitespace() {
        if let Some(value) = token.strip_prefix(prefix) {
            return Some(
                value
                    .trim_matches(|ch: char| {
                        !(ch.is_ascii_alphanumeric()
                            || ch == '_'
                            || ch == '.'
                            || ch == ':'
                            || ch == '-')
                    })
                    .to_string(),
            );
        }
    }
    None
}

fn print_tail(path: PathBuf, lines: usize) {
    if let Ok(content) = fs::read_to_string(path) {
        let tail = content.lines().rev().take(lines).collect::<Vec<_>>();
        for line in tail.into_iter().rev() {
            eprintln!("{line}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn explicit_q5_fingerprint(bid_rows: u64) -> ContentFingerprint {
        let mut lines = Vec::new();
        for bid_idx in 1..=bid_rows {
            let auction = (bid_idx - 1) % NEXMARK_BID_AUCTION_CARDINALITY + 1;
            for _ in 0..5 {
                lines.push(format!("{auction}\t1"));
            }
        }
        fingerprint_lines(lines)
    }

    #[test]
    fn deterministic_q5_fingerprint_matches_explicit_rows() {
        for bid_rows in [0, 1, 2, 10_000, 10_001] {
            assert_eq!(
                deterministic_nexmark_q5_fingerprint(bid_rows),
                explicit_q5_fingerprint(bid_rows)
            );
        }
    }

    #[test]
    fn floe_validation_queries_use_supported_floe_surface_for_string_queries() {
        for query_id in ["q14", "q15", "q16", "q17", "q21", "q22"] {
            let query = floe_expected_query_text_for_source_tables(query_id, &[Source::Bid])
                .expect("validation query");
            let lower = query.to_ascii_lowercase();
            assert!(!lower.contains("substr("), "{query_id}: {query}");
            assert!(!lower.contains("split_part("), "{query_id}: {query}");
        }
    }
}

fn print_usage() {
    println!(
        "Usage: nexmark_cross_engine_compare [floe|materialize|risingwave|feldera|all] [all|nexmark_all|q0..q22]"
    );
}
