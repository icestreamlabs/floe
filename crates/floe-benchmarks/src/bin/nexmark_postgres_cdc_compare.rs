use std::collections::BTreeMap;
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail, ensure};
use serde_json::json;
use sha2::{Digest, Sha256};

#[path = "harness_common/mod.rs"]
mod harness_common;

use self::harness_common::{
    configure_process_group, terminate_child_process_group,
    terminate_stale_floe_nodes_on_pgwire_port,
};

const CANONICAL_NEXMARK_QUERY_IDS: &[&str] = &[
    "q0", "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q12", "q13", "q14", "q15", "q16",
    "q17", "q18", "q19", "q20", "q21", "q22",
];
const DEFAULT_LIVE_CDC_OPS: u64 = 1_000_000;
const DEFAULT_BID_ROWS: u64 = 1_000_000;
const DEFAULT_AUCTION_ROWS: u64 = 10_000;
const DEFAULT_PERSON_ROWS: u64 = 10_000;
const DEFAULT_SLOT_CATCHUP_MAX_LAG_BYTES: i64 = 16 * 1024 * 1024;
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
    RisingWave,
}

impl Engine {
    fn parse_selector(raw: &str) -> Result<Vec<Self>> {
        let raw = raw.trim();
        if raw == "all" {
            return Ok(vec![Self::Floe, Self::RisingWave]);
        }
        let mut engines = Vec::new();
        for part in raw.split(',') {
            let part = part.trim();
            let engine = match part {
                "floe" => Self::Floe,
                "risingwave" => Self::RisingWave,
                "all" => bail!("'all' cannot be combined with other engines in '{raw}'"),
                other => bail!("unknown engine '{other}' (expected floe|risingwave|all)"),
            };
            if !engines.contains(&engine) {
                engines.push(engine);
            }
        }
        ensure!(!engines.is_empty(), "empty engine selector");
        Ok(engines)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Floe => "floe",
            Self::RisingWave => "risingwave",
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

    fn upstream_table(self) -> &'static str {
        match self {
            Self::Bid => "public.nexmark_bid",
            Self::Auction => "public.nexmark_auction",
            Self::Person => "public.nexmark_person",
        }
    }
}

#[derive(Debug, Clone)]
struct Config {
    engine_selector: String,
    engines: Vec<Engine>,
    query_selector: String,
    queries: Vec<String>,
    repo_root: PathBuf,
    run_id: String,
    run_dir: PathBuf,
    network_name: String,
    postgres_container_prefix: String,
    postgres_image: String,
    postgres_port: u16,
    postgres_user: String,
    postgres_password: String,
    postgres_db: String,
    risingwave_container: String,
    risingwave_image: String,
    risingwave_sql_port: u16,
    risingwave_in_memory: bool,
    floe_pg_port: u16,
    floe_admin_port: u16,
    floe_ingest_batch_size: u64,
    floe_ingest_batch_per_source: u64,
    floe_ingest_batch_per_connector: u64,
    floe_slatedb_await_durable: String,
    floe_slatedb_flush_interval_ms: u64,
    floe_l0_sst_bytes: u64,
    floe_max_unflushed_bytes: u64,
    floe_object_store_db_name_prefix: Option<String>,
    cloud_provider: Option<String>,
    live_cdc_ops: u64,
    slot_catchup_max_lag_bytes: i64,
    bid_initial_rows: u64,
    auction_initial_rows: u64,
    person_initial_rows: u64,
    live_write_chunk_rows: u64,
    poll_interval: Duration,
    poll_timeout: Duration,
    pg_query_timeout_seconds: u64,
    pg_content_query_timeout_seconds: u64,
    snapshot_rows_per_batch: u64,
    snapshot_max_workers: u64,
    snapshot_intra_table_chunks: u64,
    build_release: bool,
    keep_containers: bool,
    strict_content_check: bool,
}

impl Config {
    fn from_env_and_args() -> Result<Self> {
        let mut args = env::args().skip(1);
        let engine_selector = args.next().unwrap_or_else(|| "all".to_string());
        if engine_selector == "-h" || engine_selector == "--help" {
            print_usage();
            std::process::exit(0);
        }
        let query_selector = args.next().unwrap_or_else(|| "all".to_string());
        if let Some(extra) = args.next() {
            bail!("unexpected argument '{extra}'");
        }

        let engines = Engine::parse_selector(&engine_selector)?;
        let queries = selected_queries(&query_selector)?;
        let repo_root = repo_root()?;
        let artifact_root = env_path("ARTIFACT_ROOT")
            .unwrap_or_else(|| repo_root.join("target/postgres_cdc_nexmark_compare"));
        let run_id = current_millis()?.to_string();
        let run_dir = artifact_root.join(&run_id);

        Ok(Self {
            engine_selector,
            engines,
            query_selector,
            queries,
            repo_root,
            run_id,
            run_dir,
            network_name: env_string("NETWORK_NAME", "floe-postgres-cdc-nexmark-net"),
            postgres_container_prefix: env_string(
                "POSTGRES_CONTAINER_PREFIX",
                "floe-cdc-nexmark-postgres",
            ),
            postgres_image: env_string("POSTGRES_IMAGE", "postgres:16"),
            postgres_port: env_parse("POSTGRES_PORT", 55434)?,
            postgres_user: env_string("POSTGRES_USER", "postgres"),
            postgres_password: env_string("POSTGRES_PASSWORD", "postgres"),
            postgres_db: env_string("POSTGRES_DB", "postgres"),
            risingwave_container: env_string("RISINGWAVE_CONTAINER", "floe-cdc-nexmark-risingwave"),
            risingwave_image: env_string("RISINGWAVE_IMAGE", "risingwavelabs/risingwave:latest"),
            risingwave_sql_port: env_parse("RISINGWAVE_SQL_PORT", 14566)?,
            risingwave_in_memory: env_bool("RISINGWAVE_IN_MEMORY", true),
            floe_pg_port: env_parse("FLOE_PG_PORT", 16432)?,
            floe_admin_port: env_parse("FLOE_ADMIN_PORT", 18080)?,
            floe_ingest_batch_size: env_parse("FLOE_INGEST_BATCH_SIZE", 16_384)?,
            floe_ingest_batch_per_source: env_parse("FLOE_INGEST_BATCH_PER_SOURCE", 16_384)?,
            floe_ingest_batch_per_connector: env_parse("FLOE_INGEST_BATCH_PER_CONNECTOR", 16_384)?,
            floe_slatedb_await_durable: env_string("FLOE_SLATEDB_AWAIT_DURABLE", "false"),
            floe_slatedb_flush_interval_ms: env_parse("FLOE_SLATEDB_FLUSH_INTERVAL_MS", 500)?,
            floe_l0_sst_bytes: env_parse("FLOE_L0_SST_BYTES", 1_073_741_824)?,
            floe_max_unflushed_bytes: env_parse("FLOE_MAX_UNFLUSHED_BYTES", 8_589_934_592u64)?,
            floe_object_store_db_name_prefix: env_nonempty("FLOE_OBJECT_STORE_DB_NAME_PREFIX"),
            cloud_provider: env_nonempty("CLOUD_PROVIDER"),
            live_cdc_ops: env_parse("CDC_OPS", DEFAULT_LIVE_CDC_OPS)?,
            slot_catchup_max_lag_bytes: env_parse(
                "CDC_SLOT_CATCHUP_MAX_LAG_BYTES",
                DEFAULT_SLOT_CATCHUP_MAX_LAG_BYTES,
            )?,
            bid_initial_rows: env_parse("BID_INITIAL_ROWS", DEFAULT_BID_ROWS)?,
            auction_initial_rows: env_parse("AUCTION_INITIAL_ROWS", DEFAULT_AUCTION_ROWS)?,
            person_initial_rows: env_parse("PERSON_INITIAL_ROWS", DEFAULT_PERSON_ROWS)?,
            live_write_chunk_rows: env_parse("LIVE_WRITE_CHUNK_ROWS", 16_384)?,
            poll_interval: Duration::from_millis(env_parse("POLL_INTERVAL_MS", 500)?),
            poll_timeout: Duration::from_millis(env_parse("POLL_TIMEOUT_MS", 900_000)?),
            pg_query_timeout_seconds: env_parse("PG_QUERY_TIMEOUT_SECONDS", 10)?,
            pg_content_query_timeout_seconds: env_parse("PG_CONTENT_QUERY_TIMEOUT_SECONDS", 300)?,
            snapshot_rows_per_batch: env_parse(
                "FLOE_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH",
                16_384,
            )?,
            snapshot_max_workers: env_parse("FLOE_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS", 1)?,
            snapshot_intra_table_chunks: env_parse(
                "FLOE_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS",
                1,
            )?,
            build_release: env_bool("BUILD_RELEASE", true),
            keep_containers: env_bool("KEEP_CONTAINERS", false),
            strict_content_check: env_bool("STRICT_CONTENT_CHECK", true),
        })
    }

    fn configured_initial_rows(&self, source: Source) -> u64 {
        match source {
            Source::Bid => self.bid_initial_rows,
            Source::Auction => self.auction_initial_rows,
            Source::Person => self.person_initial_rows,
        }
    }

    fn source_pg_target(&self) -> PgTarget<'_> {
        PgTarget {
            port: self.postgres_port,
            user: &self.postgres_user,
            password: &self.postgres_password,
            db: &self.postgres_db,
        }
    }

    fn source_dsn_for_host(&self) -> String {
        format!(
            "postgres://{}:{}@127.0.0.1:{}/{}",
            self.postgres_user, self.postgres_password, self.postgres_port, self.postgres_db
        )
    }

    fn target_binary(&self, name: &str) -> PathBuf {
        let profile = if self.build_release {
            "release"
        } else {
            "debug"
        };
        self.repo_root.join("target").join(profile).join(name)
    }

    fn results_file(&self) -> PathBuf {
        self.run_dir.join("summary.md")
    }

    fn results_jsonl(&self) -> PathBuf {
        self.run_dir.join("results.jsonl")
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
        self.ensure_network()?;
        self.write_summary_header()?;
        self.capture_run_context()?;
        if self.config.engines.contains(&Engine::Floe) {
            self.build_floe_node()?;
        }

        let engines = self.config.engines.clone();
        for engine in engines {
            if engine == Engine::RisingWave {
                log("starting RisingWave container");
                if let Err(err) = self.start_risingwave() {
                    self.record_start_failures(engine, &format!("engine_start_failed: {err}"))?;
                    continue;
                }
            }
            self.run_engine_suite(engine)?;
            if engine == Engine::RisingWave {
                self.stop_container(&self.config.risingwave_container);
            }
        }

        log(format!(
            "results written to {}",
            self.config.results_file().display()
        ));
        let summary = fs::read_to_string(self.config.results_file()).context("read summary")?;
        print!("{summary}");
        Ok(())
    }

    fn run_engine_suite(&mut self, engine: Engine) -> Result<()> {
        let engine_dir = self.config.run_dir.join(engine.as_str());
        fs::create_dir_all(&engine_dir)?;

        let queries = self.config.queries.clone();
        for query_id in queries {
            let sources = required_sources_for_query(&query_id);
            let profile = WorkloadProfile::for_query(&self.config, &sources);
            let artifact_dir = engine_dir.join(&query_id);
            fs::create_dir_all(&artifact_dir)?;
            let input_rows = profile.live_ops_total();
            log(format!(
                "running {} {} (sources: {}, live_cdc_ops: {})",
                engine.as_str(),
                query_id,
                source_labels(&sources),
                input_rows
            ));

            let result = match engine {
                Engine::Floe => self.run_floe_query(&query_id, &sources, &profile, &artifact_dir),
                Engine::RisingWave => {
                    self.run_risingwave_query(&query_id, &sources, &profile, &artifact_dir)
                }
            };
            if let Err(err) = result {
                self.record_failure(
                    engine,
                    &query_id,
                    &format!("setup_or_completion_failed: {err}"),
                    input_rows,
                    &artifact_dir,
                )?;
                self.stop_floe_process();
                self.stop_postgres_for(engine, &query_id);
            }
        }
        Ok(())
    }

    fn run_floe_query(
        &mut self,
        query_id: &str,
        sources: &[Source],
        profile: &WorkloadProfile,
        artifact_dir: &Path,
    ) -> Result<()> {
        let postgres_container = self.postgres_container_name(Engine::Floe, query_id);
        let slot = slot_name(&self.config.run_id, Engine::Floe, query_id);
        let publication = publication_name(&self.config.run_id, Engine::Floe, query_id);
        self.prepare_postgres(
            &postgres_container,
            &publication,
            sources,
            profile,
            artifact_dir,
        )?;

        let program_sql = floe_program_sql(
            &self.config.source_dsn_for_host(),
            &slot,
            &publication,
            sources,
            query_id,
        )?;
        fs::write(artifact_dir.join("program.sql"), &program_sql)?;
        let config_json = floe_config_json(&self.config, artifact_dir);
        let config_path = artifact_dir.join("floe_config.json");
        fs::write(&config_path, serde_json::to_vec_pretty(&config_json)?)?;

        self.stop_floe_process();
        self.kill_stale_floe_nodes();
        self.start_floe_node(&config_path, &program_sql, artifact_dir, query_id)?;
        self.wait_for_floe_pg(artifact_dir)?;

        let target = PgTarget {
            port: self.config.floe_pg_port,
            user: "postgres",
            password: "",
            db: "postgres",
        };
        self.run_cdc_measurement(
            Engine::Floe,
            query_id,
            sources,
            profile,
            target,
            artifact_dir,
        )?;
        self.stop_floe_process();
        self.stop_postgres_for(Engine::Floe, query_id);
        Ok(())
    }

    fn run_risingwave_query(
        &mut self,
        query_id: &str,
        sources: &[Source],
        profile: &WorkloadProfile,
        artifact_dir: &Path,
    ) -> Result<()> {
        let postgres_container = self.postgres_container_name(Engine::RisingWave, query_id);
        let slot = slot_name(&self.config.run_id, Engine::RisingWave, query_id);
        let publication = publication_name(&self.config.run_id, Engine::RisingWave, query_id);
        self.prepare_postgres(
            &postgres_container,
            &publication,
            sources,
            profile,
            artifact_dir,
        )?;

        let setup_sql = risingwave_setup_sql(
            &self.config,
            &postgres_container,
            &slot,
            &publication,
            sources,
            query_id,
        )?;
        let setup_path = artifact_dir.join("setup.sql");
        fs::write(&setup_path, setup_sql)?;
        self.psql_file(
            PgTarget {
                port: self.config.risingwave_sql_port,
                user: "root",
                password: "",
                db: "dev",
            },
            &setup_path,
            artifact_dir,
            "setup",
        )
        .context("run RisingWave setup")?;

        let target = PgTarget {
            port: self.config.risingwave_sql_port,
            user: "root",
            password: "",
            db: "dev",
        };
        self.run_cdc_measurement(
            Engine::RisingWave,
            query_id,
            sources,
            profile,
            target,
            artifact_dir,
        )?;
        self.stop_postgres_for(Engine::RisingWave, query_id);
        Ok(())
    }

    fn run_cdc_measurement(
        &self,
        engine: Engine,
        query_id: &str,
        sources: &[Source],
        profile: &WorkloadProfile,
        engine_target: PgTarget<'_>,
        artifact_dir: &Path,
    ) -> Result<()> {
        let source_target = self.config.source_pg_target();
        let expected_baseline_sql = expected_sql_for_engine(engine, query_id)
            .with_context(|| format!("expected SQL for {engine:?} {query_id}"))?;
        let baseline = self.compute_expected_fingerprint(
            source_target,
            expected_baseline_sql,
            artifact_dir,
            "baseline_expected",
        )?;
        let baseline_start = Instant::now();
        let baseline_observed = self.poll_engine_result_until_expected(
            engine_target,
            &baseline,
            artifact_dir,
            "baseline",
        )?;
        let baseline_ready_ms = baseline_start.elapsed().as_millis();
        fs::write(
            artifact_dir.join("baseline_content_hash.txt"),
            format!(
                "expected_rows={}\nexpected_sha256={}\nobserved_rows={}\nobserved_sha256={}\n",
                baseline.row_count,
                baseline.hash,
                baseline_observed.row_count,
                baseline_observed.hash
            ),
        )?;

        self.wait_for_postgres_slot_active(query_id, engine, artifact_dir)?;
        let live_started = Instant::now();
        let live_write_ms = self.write_live_mutations(sources, profile, artifact_dir)?;
        let slot_catchup_ms =
            self.wait_for_postgres_slot_caught_up(query_id, engine, artifact_dir)?;
        let final_expected = self.compute_expected_fingerprint(
            source_target,
            expected_baseline_sql,
            artifact_dir,
            "final_expected",
        )?;
        let final_observed = self.poll_engine_result_until_expected(
            engine_target,
            &final_expected,
            artifact_dir,
            "final",
        )?;
        let result_ready_ms = live_started.elapsed().as_millis();
        let result_post_write_ms = result_ready_ms.saturating_sub(live_write_ms);
        let rows_per_second = rate(profile.live_ops_total(), result_ready_ms);
        let result_rows = final_observed.row_count;
        self.append_summary_row(SummaryRow {
            engine,
            query_id,
            status: "ok",
            baseline_ready_ms: Some(baseline_ready_ms),
            live_write_ms: Some(live_write_ms),
            result_ready_ms: Some(result_ready_ms),
            result_post_write_ms: Some(result_post_write_ms),
            rows_per_second: Some(rows_per_second),
            input_rows: profile.live_ops_total(),
            result_rows: Some(result_rows),
            notes: format!(
                "cdc_updates_deletes_inserts;baseline_rows={};final_content_sha256={};slot_catchup_ms={slot_catchup_ms};{}",
                baseline.row_count,
                final_observed.short_hash(),
                profile.notes()
            ),
        })?;
        Ok(())
    }

    fn prepare_postgres(
        &self,
        container: &str,
        publication: &str,
        sources: &[Source],
        profile: &WorkloadProfile,
        artifact_dir: &Path,
    ) -> Result<()> {
        self.start_postgres(container)?;
        self.wait_for_postgres(container)?;
        let setup_sql = postgres_setup_sql(publication, sources, profile);
        let setup_path = artifact_dir.join("postgres_setup.sql");
        fs::write(&setup_path, &setup_sql)?;
        self.psql_file(
            self.config.source_pg_target(),
            &setup_path,
            artifact_dir,
            "postgres_setup",
        )
        .context("load Postgres Nexmark CDC dataset")?;
        Ok(())
    }

    fn start_postgres(&self, container: &str) -> Result<()> {
        self.stop_container(container);
        log(format!(
            "starting Postgres {} as {} on port {}",
            self.config.postgres_image, container, self.config.postgres_port
        ));
        run_status("docker", ["pull", &self.config.postgres_image], None)
            .context("pull Postgres image")?;
        run_status(
            "docker",
            [
                "run",
                "-d",
                "--name",
                container,
                "--network",
                &self.config.network_name,
                "-e",
                &format!("POSTGRES_USER={}", self.config.postgres_user),
                "-e",
                &format!("POSTGRES_PASSWORD={}", self.config.postgres_password),
                "-e",
                &format!("POSTGRES_DB={}", self.config.postgres_db),
                "-p",
                &format!("{}:5432", self.config.postgres_port),
                &self.config.postgres_image,
                "postgres",
                "-c",
                "wal_level=logical",
                "-c",
                "max_replication_slots=32",
                "-c",
                "max_wal_senders=32",
                "-c",
                "max_slot_wal_keep_size=8192MB",
            ],
            None,
        )
        .context("start Postgres")
    }

    fn wait_for_postgres(&self, container: &str) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(90);
        while Instant::now() < deadline {
            if command_success(
                "docker",
                [
                    "exec",
                    container,
                    "pg_isready",
                    "-U",
                    &self.config.postgres_user,
                    "-d",
                    &self.config.postgres_db,
                ],
                None,
            )? {
                return Ok(());
            }
            wait_before_retry(deadline, Duration::from_secs(1));
        }
        let logs = run_capture("docker", ["logs", container], None).unwrap_or_default();
        eprintln!("{logs}");
        bail!("Postgres did not become ready")
    }

    fn start_risingwave(&self) -> Result<()> {
        self.stop_container(&self.config.risingwave_container);
        run_status("docker", ["pull", &self.config.risingwave_image], None)
            .context("pull RisingWave image")?;
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
        run_status_vec("docker", &args, None).context("start RisingWave")?;
        self.wait_for_pg(PgTarget {
            port: self.config.risingwave_sql_port,
            user: "root",
            password: "",
            db: "dev",
        })
        .context("wait for RisingWave pgwire")
    }

    fn start_floe_node(
        &mut self,
        config_path: &Path,
        program_sql: &str,
        artifact_dir: &Path,
        query_id: &str,
    ) -> Result<()> {
        let stdout = File::create(artifact_dir.join("floe-node.stdout.log"))?;
        let stderr = File::create(artifact_dir.join("floe-node.stderr.log"))?;
        let mut command = Command::new(self.config.target_binary("floe-node"));
        configure_process_group(&mut command);
        command
            .arg("run")
            .arg("--pgwire-addr")
            .arg(format!("127.0.0.1:{}", self.config.floe_pg_port))
            .arg("--admin-port")
            .arg(self.config.floe_admin_port.to_string());
        if self.config.cloud_provider.is_some() {
            command.arg("--object-store-from-env");
            if let Some(prefix) = &self.config.floe_object_store_db_name_prefix {
                command
                    .arg("--slatedb-name")
                    .arg(format!("{prefix}-{query_id}"));
            }
        }
        command
            .arg("--slatedb-await-durable")
            .arg(&self.config.floe_slatedb_await_durable)
            .arg("--slatedb-flush-interval-ms")
            .arg(self.config.floe_slatedb_flush_interval_ms.to_string())
            .arg("--slatedb-l0-sst-bytes")
            .arg(self.config.floe_l0_sst_bytes.to_string())
            .arg("--slatedb-max-unflushed-bytes")
            .arg(self.config.floe_max_unflushed_bytes.to_string())
            .arg("--ingest-batch-size")
            .arg(self.config.floe_ingest_batch_size.to_string())
            .arg("--ingest-batch-per-source")
            .arg(self.config.floe_ingest_batch_per_source.to_string())
            .arg("--ingest-batch-per-connector")
            .arg(self.config.floe_ingest_batch_per_connector.to_string())
            .arg("--config")
            .arg(config_path)
            .arg("--mv-query")
            .arg(program_sql.replace('\n', " "))
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        self.floe_child = Some(command.spawn().context("start floe-node")?);
        Ok(())
    }

    fn wait_for_floe_pg(&mut self, artifact_dir: &Path) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(180);
        while Instant::now() < deadline {
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
                        password: "",
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
            wait_before_retry(deadline, Duration::from_secs(1));
        }
        print_tail(artifact_dir.join("floe-node.stderr.log"), 120);
        bail!("floe pgwire did not become ready")
    }

    fn stop_floe_process(&mut self) {
        if let Some(mut child) = self.floe_child.take() {
            terminate_child_process_group(&mut child, Duration::from_secs(5));
        }
    }

    fn kill_stale_floe_nodes(&self) {
        terminate_stale_floe_nodes_on_pgwire_port(self.config.floe_pg_port, Duration::from_secs(5));
    }

    fn wait_for_postgres_slot_active(
        &self,
        query_id: &str,
        engine: Engine,
        artifact_dir: &Path,
    ) -> Result<()> {
        let slot = slot_name(&self.config.run_id, engine, query_id);
        let sql = format!(
            "SELECT COALESCE((SELECT active FROM pg_replication_slots WHERE slot_name = '{}'), false)",
            escape_sql_literal(&slot)
        );
        let deadline = Instant::now() + Duration::from_secs(180);
        while Instant::now() < deadline {
            if self
                .fetch_pg_scalar(self.config.source_pg_target(), &sql)
                .ok()
                .as_deref()
                == Some("t")
            {
                return Ok(());
            }
            wait_before_retry(deadline, Duration::from_secs(1));
        }
        let slots = self
            .fetch_pg_table(
                self.config.source_pg_target(),
                "SELECT slot_name, active, plugin, confirmed_flush_lsn FROM pg_replication_slots ORDER BY slot_name",
                self.config.pg_query_timeout_seconds,
            )
            .unwrap_or_default();
        fs::write(artifact_dir.join("postgres_slots.txt"), slots)?;
        bail!("Postgres CDC slot {slot} did not become active")
    }

    fn wait_for_postgres_slot_caught_up(
        &self,
        query_id: &str,
        engine: Engine,
        artifact_dir: &Path,
    ) -> Result<u128> {
        let started = Instant::now();
        let slot = slot_name(&self.config.run_id, engine, query_id);
        let sql = format!(
            "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::BIGINT, confirmed_flush_lsn::TEXT, pg_current_wal_lsn()::TEXT FROM pg_replication_slots WHERE slot_name = '{}'",
            escape_sql_literal(&slot)
        );
        let deadline = Instant::now() + self.config.poll_timeout;
        let mut last = String::new();
        while Instant::now() < deadline {
            let output = self
                .fetch_pg_table(
                    self.config.source_pg_target(),
                    &sql,
                    self.config.pg_query_timeout_seconds,
                )
                .unwrap_or_default();
            last = output.clone();
            let lag = output
                .split('\t')
                .next()
                .and_then(|value| value.parse::<i64>().ok());
            if let Some(lag) = lag
                && lag <= self.config.slot_catchup_max_lag_bytes
            {
                fs::write(
                    artifact_dir.join("slot_catchup.txt"),
                    format!(
                        "slot={slot}\nlag_bytes={lag}\nelapsed_ms={}\nraw={output}\n",
                        started.elapsed().as_millis()
                    ),
                )?;
                return Ok(started.elapsed().as_millis());
            }
            wait_before_retry(deadline, self.config.poll_interval);
        }
        fs::write(
            artifact_dir.join("slot_catchup.error"),
            format!(
                "slot={slot}\nmax_lag_bytes={}\nlast={last}\n",
                self.config.slot_catchup_max_lag_bytes
            ),
        )?;
        bail!("Postgres CDC slot {slot} did not catch up before timeout")
    }

    fn write_live_mutations(
        &self,
        sources: &[Source],
        profile: &WorkloadProfile,
        artifact_dir: &Path,
    ) -> Result<u128> {
        let started = Instant::now();
        for source in sources {
            let Some(plan) = profile.sources.get(source) else {
                continue;
            };
            self.write_source_mutation_phase(*source, plan, MutationKind::Update, artifact_dir)?;
            self.write_source_mutation_phase(*source, plan, MutationKind::Delete, artifact_dir)?;
            self.write_source_mutation_phase(*source, plan, MutationKind::Insert, artifact_dir)?;
        }
        Ok(started.elapsed().as_millis())
    }

    fn write_source_mutation_phase(
        &self,
        source: Source,
        plan: &SourceWorkload,
        kind: MutationKind,
        artifact_dir: &Path,
    ) -> Result<()> {
        let total = match kind {
            MutationKind::Update => plan.updates,
            MutationKind::Delete => plan.deletes,
            MutationKind::Insert => plan.inserts,
        };
        if total == 0 {
            return Ok(());
        }
        let chunk = if self.config.live_write_chunk_rows == 0 {
            total
        } else {
            self.config.live_write_chunk_rows.min(total)
        };
        let mut offset = 0;
        while offset < total {
            let count = (total - offset).min(chunk);
            let sql = match kind {
                MutationKind::Update => mutation_update_sql(source, offset + 1, count),
                MutationKind::Delete => mutation_delete_sql(source, plan, offset + 1, count),
                MutationKind::Insert => mutation_insert_sql(
                    source,
                    plan,
                    offset + 1,
                    count,
                    profile_auction_keyspace(plan),
                ),
            };
            let label = format!(
                "postgres_mutation_{}_{}_{}",
                source.label(),
                kind.label(),
                offset / chunk
            );
            let sql_path = artifact_dir.join(format!("{label}.sql"));
            fs::write(&sql_path, &sql)?;
            self.psql_file(
                self.config.source_pg_target(),
                &sql_path,
                artifact_dir,
                &label,
            )
            .with_context(|| format!("apply {kind:?} mutations for {}", source.label()))?;
            offset += count;
        }
        Ok(())
    }

    fn compute_expected_fingerprint(
        &self,
        target: PgTarget<'_>,
        sql: &str,
        artifact_dir: &Path,
        label: &str,
    ) -> Result<ContentFingerprint> {
        self.compute_query_fingerprint(
            target,
            &format!("SELECT * FROM ({sql}) AS __expected_result"),
            artifact_dir,
            label,
        )
    }

    fn poll_engine_result_until_expected(
        &self,
        target: PgTarget<'_>,
        expected: &ContentFingerprint,
        artifact_dir: &Path,
        phase: &str,
    ) -> Result<ContentFingerprint> {
        let deadline = Instant::now() + self.config.poll_timeout;
        let mut attempt = 0_u64;
        loop {
            let count = self
                .fetch_pg_scalar(target, "SELECT COUNT(*)::BIGINT FROM benchmark_result")
                .ok()
                .and_then(|value| value.parse::<u64>().ok());
            if count == Some(expected.row_count) {
                let observed = self.compute_result_fingerprint(
                    target,
                    artifact_dir,
                    &format!("{phase}_observed_attempt_{attempt}"),
                )?;
                if !self.config.strict_content_check || &observed == expected {
                    return Ok(observed);
                }
                fs::write(
                    artifact_dir.join(format!("{phase}_last_mismatch.txt")),
                    format!(
                        "expected_rows={}\nexpected_sha256={}\nobserved_rows={}\nobserved_sha256={}\n",
                        expected.row_count, expected.hash, observed.row_count, observed.hash
                    ),
                )?;
            }
            if Instant::now() >= deadline {
                let final_observed = self
                    .compute_result_fingerprint(
                        target,
                        artifact_dir,
                        &format!("{phase}_observed_timeout"),
                    )
                    .ok();
                let observed_note = final_observed
                    .as_ref()
                    .map(|fp| {
                        format!(
                            "observed_rows={}\nobserved_sha256={}\n",
                            fp.row_count, fp.hash
                        )
                    })
                    .unwrap_or_else(|| "observed_unavailable=true\n".to_string());
                fs::write(
                    artifact_dir.join(format!("{phase}_correctness.error")),
                    format!(
                        "expected_rows={}\nexpected_sha256={}\n{}",
                        expected.row_count, expected.hash, observed_note
                    ),
                )?;
                bail!("{phase} result did not match expected content before timeout");
            }
            attempt += 1;
            wait_before_retry(deadline, self.config.poll_interval);
        }
    }

    fn compute_result_fingerprint(
        &self,
        target: PgTarget<'_>,
        artifact_dir: &Path,
        label: &str,
    ) -> Result<ContentFingerprint> {
        let projection = self
            .compute_relation_projection(target, "benchmark_result", "public")
            .context("compute benchmark_result projection")?;
        let sql = if projection.is_empty() {
            "SELECT * FROM benchmark_result".to_string()
        } else {
            format!("SELECT {projection} FROM benchmark_result")
        };
        self.compute_query_fingerprint(target, &sql, artifact_dir, label)
    }

    fn compute_query_fingerprint(
        &self,
        target: PgTarget<'_>,
        sql: &str,
        artifact_dir: &Path,
        label: &str,
    ) -> Result<ContentFingerprint> {
        let rows_path = artifact_dir.join(format!("{label}.rows.tsv"));
        let stderr_path = artifact_dir.join(format!("{label}.stderr.log"));
        let stdout = File::create(&rows_path)?;
        let stderr = File::create(&stderr_path)?;
        let status = Command::new("timeout")
            .arg(format!("{}s", self.config.pg_content_query_timeout_seconds))
            .arg("psql")
            .env("PGPASSWORD", target.password)
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
        fingerprint_file_lines(&rows_path)
    }

    fn compute_relation_projection(
        &self,
        target: PgTarget<'_>,
        relation: &str,
        preferred_schema: &str,
    ) -> Result<String> {
        validate_identifier(relation)?;
        validate_identifier(preferred_schema)?;
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
            escape_sql_literal(preferred_schema),
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

    fn wait_for_pg(&self, target: PgTarget<'_>) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(120);
        while Instant::now() < deadline {
            if self.fetch_pg_scalar(target, "SELECT 1").ok().as_deref() == Some("1") {
                return Ok(());
            }
            wait_before_retry(deadline, Duration::from_secs(1));
        }
        bail!("pgwire did not become ready on port {}", target.port)
    }

    fn psql_file(
        &self,
        target: PgTarget<'_>,
        path: &Path,
        artifact_dir: &Path,
        label: &str,
    ) -> Result<()> {
        let stdout = File::create(artifact_dir.join(format!("{label}.stdout.log")))?;
        let stderr = File::create(artifact_dir.join(format!("{label}.stderr.log")))?;
        let status = Command::new("psql")
            .env("PGPASSWORD", target.password)
            .arg("-h")
            .arg("127.0.0.1")
            .arg("-p")
            .arg(target.port.to_string())
            .arg("-U")
            .arg(target.user)
            .arg("-d")
            .arg(target.db)
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
            .env("PGPASSWORD", target.password)
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

    fn fetch_pg_table(
        &self,
        target: PgTarget<'_>,
        sql: &str,
        timeout_seconds: u64,
    ) -> Result<String> {
        let output = Command::new("timeout")
            .arg(format!("{timeout_seconds}s"))
            .arg("psql")
            .env("PGPASSWORD", target.password)
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

    fn ensure_command(&self, command_name: &str) -> Result<()> {
        let status = Command::new("sh")
            .arg("-c")
            .arg(format!("command -v {command_name} >/dev/null 2>&1"))
            .status()
            .with_context(|| format!("check command {command_name}"))?;
        ensure!(status.success(), "{command_name} is required");
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
        .context("create docker network")
    }

    fn build_floe_node(&self) -> Result<()> {
        log("building floe-node");
        let mut args = vec!["build", "-p", "floe-node"];
        if self.config.build_release {
            args.push("--release");
        }
        run_status("cargo", args, Some(&self.config.repo_root)).context("build floe-node")
    }

    fn capture_run_context(&self) -> Result<()> {
        let context = json!({
            "run_id": self.config.run_id,
            "engine_selector": self.config.engine_selector,
            "query_selector": self.config.query_selector,
            "live_cdc_ops": self.config.live_cdc_ops,
            "slot_catchup_max_lag_bytes": self.config.slot_catchup_max_lag_bytes,
            "initial_rows": {
                "bid": self.config.bid_initial_rows,
                "auction": self.config.auction_initial_rows,
                "person": self.config.person_initial_rows,
            },
            "postgres": {
                "image": self.config.postgres_image,
                "port": self.config.postgres_port,
            },
            "risingwave": {
                "image": self.config.risingwave_image,
                "port": self.config.risingwave_sql_port,
                "in_memory": self.config.risingwave_in_memory,
            },
            "floe": {
                "git_commit": run_capture("git", ["rev-parse", "HEAD"], Some(&self.config.repo_root)).unwrap_or_default().trim(),
                "git_branch": run_capture("git", ["branch", "--show-current"], Some(&self.config.repo_root)).unwrap_or_default().trim(),
                "binary": self.config.target_binary("floe-node").display().to_string(),
                "pg_port": self.config.floe_pg_port,
                "admin_port": self.config.floe_admin_port,
            },
            "correctness": {
                "strict_content_check": self.config.strict_content_check,
                "expected_source": "postgres_final_state",
            }
        });
        fs::write(
            self.config.run_dir.join("run_context.json"),
            serde_json::to_vec_pretty(&context)?,
        )
        .context("write run context")
    }

    fn write_summary_header(&self) -> Result<()> {
        let mut file = File::create(self.config.results_file())?;
        writeln!(file, "# Nexmark Postgres CDC Compare")?;
        writeln!(file)?;
        writeln!(file, "Run: `{}`", self.config.run_id)?;
        writeln!(file, "Engines: `{}`", self.config.engine_selector)?;
        writeln!(file, "Queries: `{}`", self.config.query_selector)?;
        writeln!(file, "Live CDC ops/query: `{}`", self.config.live_cdc_ops)?;
        writeln!(file)?;
        writeln!(
            file,
            "| Engine | Query | Status | Baseline Ready (s) | Live Write (s) | Result Ready (s) | Post-Write Wait (s) | CDC Rows/s | CDC Ops | Result Rows | Notes |"
        )?;
        writeln!(
            file,
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"
        )?;
        File::create(self.config.results_jsonl())?;
        Ok(())
    }

    fn append_summary_row(&self, row: SummaryRow<'_>) -> Result<()> {
        let mut file = OpenOptions::new()
            .append(true)
            .open(self.config.results_file())?;
        writeln!(
            file,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            row.engine.as_str(),
            row.query_id,
            row.status,
            seconds_cell(row.baseline_ready_ms),
            seconds_cell(row.live_write_ms),
            seconds_cell(row.result_ready_ms),
            seconds_cell(row.result_post_write_ms),
            row.rows_per_second
                .map(|value| format!("{value:.0}"))
                .unwrap_or_else(|| "n/a".to_string()),
            row.input_rows,
            row.result_rows
                .map(|value| value.to_string())
                .unwrap_or_else(|| "n/a".to_string()),
            row.notes
        )?;
        let json_line = json!({
            "engine": row.engine.as_str(),
            "query_id": row.query_id,
            "status": row.status,
            "baseline_ready_ms": row.baseline_ready_ms,
            "live_write_ms": row.live_write_ms,
            "result_ready_ms": row.result_ready_ms,
            "result_post_write_ms": row.result_post_write_ms,
            "cdc_rows_per_second": row.rows_per_second,
            "input_rows": row.input_rows,
            "result_rows": row.result_rows,
            "notes": row.notes,
        });
        let mut jsonl = OpenOptions::new()
            .append(true)
            .open(self.config.results_jsonl())?;
        writeln!(jsonl, "{}", serde_json::to_string(&json_line)?)?;
        Ok(())
    }

    fn record_start_failures(&self, engine: Engine, notes: &str) -> Result<()> {
        for query_id in &self.config.queries {
            let sources = required_sources_for_query(query_id);
            let profile = WorkloadProfile::for_query(&self.config, &sources);
            self.append_summary_row(SummaryRow {
                engine,
                query_id,
                status: "failed",
                baseline_ready_ms: None,
                live_write_ms: None,
                result_ready_ms: None,
                result_post_write_ms: None,
                rows_per_second: None,
                input_rows: profile.live_ops_total(),
                result_rows: None,
                notes: notes.to_string(),
            })?;
        }
        Ok(())
    }

    fn record_failure(
        &self,
        engine: Engine,
        query_id: &str,
        notes: &str,
        input_rows: u64,
        artifact_dir: &Path,
    ) -> Result<()> {
        fs::write(artifact_dir.join("failure.txt"), notes)?;
        self.append_summary_row(SummaryRow {
            engine,
            query_id,
            status: "failed",
            baseline_ready_ms: None,
            live_write_ms: None,
            result_ready_ms: None,
            result_post_write_ms: None,
            rows_per_second: None,
            input_rows,
            result_rows: None,
            notes: notes.to_string(),
        })
    }

    fn postgres_container_name(&self, engine: Engine, query_id: &str) -> String {
        format!(
            "{}-{}-{}",
            self.config.postgres_container_prefix,
            engine.as_str(),
            query_id
        )
    }

    fn stop_postgres_for(&self, engine: Engine, query_id: &str) {
        self.stop_container(&self.postgres_container_name(engine, query_id));
    }

    fn stop_container(&self, name: &str) {
        if !self.config.keep_containers {
            let _ = run_status("docker", ["rm", "-fv", name], None);
        }
    }
}

#[derive(Clone, Copy)]
struct PgTarget<'a> {
    port: u16,
    user: &'a str,
    password: &'a str,
    db: &'a str,
}

struct SummaryRow<'a> {
    engine: Engine,
    query_id: &'a str,
    status: &'a str,
    baseline_ready_ms: Option<u128>,
    live_write_ms: Option<u128>,
    result_ready_ms: Option<u128>,
    result_post_write_ms: Option<u128>,
    rows_per_second: Option<f64>,
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

#[derive(Debug, Clone)]
struct WorkloadProfile {
    sources: BTreeMap<Source, SourceWorkload>,
}

impl WorkloadProfile {
    fn for_query(config: &Config, sources: &[Source]) -> Self {
        let mut weighted = Vec::new();
        let has_bid = sources.contains(&Source::Bid);
        for source in sources {
            let weight = if has_bid && *source == Source::Bid {
                8
            } else {
                1
            };
            weighted.push((*source, weight));
        }
        let total_weight: u64 = weighted.iter().map(|(_, weight)| *weight).sum();
        let mut remaining = config.live_cdc_ops;
        let mut result = BTreeMap::new();
        for (idx, (source, weight)) in weighted.iter().enumerate() {
            let ops = if idx + 1 == weighted.len() {
                remaining
            } else {
                let share = config.live_cdc_ops.saturating_mul(*weight) / total_weight;
                remaining = remaining.saturating_sub(share);
                share
            };
            let updates = ops / 2;
            let deletes = ops / 4;
            let inserts = ops.saturating_sub(updates + deletes);
            let initial_rows = config
                .configured_initial_rows(*source)
                .max(updates + deletes)
                .max(1);
            result.insert(
                *source,
                SourceWorkload {
                    initial_rows,
                    live_ops: ops,
                    updates,
                    deletes,
                    inserts,
                    auction_keyspace: 1,
                    person_keyspace: 1,
                },
            );
        }

        let auction_rows_after = result
            .get(&Source::Auction)
            .map(|plan| plan.initial_rows + plan.inserts)
            .unwrap_or(NEXMARK_BID_AUCTION_CARDINALITY)
            .max(1);
        let person_rows_after = result
            .get(&Source::Person)
            .map(|plan| plan.initial_rows + plan.inserts)
            .unwrap_or(DEFAULT_PERSON_ROWS)
            .max(1);
        for plan in result.values_mut() {
            plan.auction_keyspace = auction_rows_after;
            plan.person_keyspace = person_rows_after;
        }
        Self { sources: result }
    }

    fn live_ops_total(&self) -> u64 {
        self.sources.values().map(|plan| plan.live_ops).sum()
    }

    fn initial_rows(&self, source: Source) -> u64 {
        self.sources
            .get(&source)
            .map(|plan| plan.initial_rows)
            .unwrap_or(0)
    }

    fn notes(&self) -> String {
        self.sources
            .iter()
            .map(|(source, plan)| {
                format!(
                    "{}_initial={};{}_updates={};{}_deletes={};{}_inserts={}",
                    source.label(),
                    plan.initial_rows,
                    source.label(),
                    plan.updates,
                    source.label(),
                    plan.deletes,
                    source.label(),
                    plan.inserts
                )
            })
            .collect::<Vec<_>>()
            .join(";")
    }
}

#[derive(Debug, Clone)]
struct SourceWorkload {
    initial_rows: u64,
    live_ops: u64,
    updates: u64,
    deletes: u64,
    inserts: u64,
    auction_keyspace: u64,
    person_keyspace: u64,
}

#[derive(Debug, Clone, Copy)]
enum MutationKind {
    Update,
    Delete,
    Insert,
}

impl MutationKind {
    fn label(self) -> &'static str {
        match self {
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Insert => "insert",
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

fn floe_program_sql(
    dsn: &str,
    slot: &str,
    publication: &str,
    sources: &[Source],
    query_id: &str,
) -> Result<String> {
    let query = query_sql_floe(query_id).with_context(|| format!("Floe SQL for {query_id}"))?;
    let mut sql = format!(
        "CREATE SOURCE pg_main WITH (
  connector = 'postgres-cdc',
  connection = '{}',
  slot.name = '{}',
  publication.name = '{}'
);
",
        escape_sql_literal(dsn),
        escape_sql_literal(slot),
        escape_sql_literal(publication)
    );
    for source in sources {
        sql.push_str(&floe_cdc_table_sql(*source));
    }
    sql.push_str(&format!(
        "CREATE MATERIALIZED VIEW benchmark_result AS\n{query};\n"
    ));
    Ok(sql)
}

fn floe_cdc_table_sql(source: Source) -> String {
    match source {
        Source::Bid => "CREATE TABLE nexmark_bid (
  id BIGINT PRIMARY KEY,
  auction BIGINT NOT NULL,
  bidder BIGINT NOT NULL,
  price BIGINT NOT NULL,
  channel TEXT,
  url TEXT,
  date_time BIGINT NOT NULL,
  extra TEXT
) FROM pg_main TABLE 'public.nexmark_bid';
"
        .to_string(),
        Source::Auction => "CREATE TABLE nexmark_auction (
  id BIGINT PRIMARY KEY,
  item_name TEXT,
  description TEXT,
  initial_bid BIGINT NOT NULL,
  reserve BIGINT NOT NULL,
  date_time BIGINT NOT NULL,
  expires BIGINT NOT NULL,
  seller BIGINT NOT NULL,
  category BIGINT NOT NULL,
  extra TEXT
) FROM pg_main TABLE 'public.nexmark_auction';
"
        .to_string(),
        Source::Person => "CREATE TABLE nexmark_person (
  id BIGINT PRIMARY KEY,
  name TEXT,
  email_address TEXT,
  credit_card TEXT,
  city TEXT,
  state TEXT,
  date_time BIGINT NOT NULL,
  extra TEXT
) FROM pg_main TABLE 'public.nexmark_person';
"
        .to_string(),
    }
}

fn risingwave_setup_sql(
    config: &Config,
    postgres_container: &str,
    slot: &str,
    publication: &str,
    sources: &[Source],
    query_id: &str,
) -> Result<String> {
    let query =
        query_sql_risingwave(query_id).with_context(|| format!("RisingWave SQL for {query_id}"))?;
    let mut sql = String::from(
        "DROP MATERIALIZED VIEW IF EXISTS benchmark_result;
DROP MATERIALIZED VIEW IF EXISTS bid;
DROP MATERIALIZED VIEW IF EXISTS auction;
DROP MATERIALIZED VIEW IF EXISTS person;
DROP TABLE IF EXISTS nexmark_bid;
DROP TABLE IF EXISTS nexmark_auction;
DROP TABLE IF EXISTS nexmark_person;
DROP SOURCE IF EXISTS pg_main;
",
    );
    sql.push_str(&format!(
        "CREATE SOURCE pg_main WITH (
  connector = 'postgres-cdc',
  hostname = '{}',
  port = '5432',
  username = '{}',
  password = '{}',
  database.name = '{}',
  schema.name = 'public',
  slot.name = '{}',
  publication.name = '{}'
);
",
        escape_sql_literal(postgres_container),
        escape_sql_literal(&config.postgres_user),
        escape_sql_literal(&config.postgres_password),
        escape_sql_literal(&config.postgres_db),
        escape_sql_literal(slot),
        escape_sql_literal(publication)
    ));
    for source in sources {
        sql.push_str(&risingwave_cdc_table_sql(*source));
        sql.push_str(&risingwave_view_sql(*source));
    }
    sql.push_str(&format!(
        "CREATE MATERIALIZED VIEW benchmark_result AS\n{query};\n"
    ));
    Ok(sql)
}

fn risingwave_cdc_table_sql(source: Source) -> String {
    match source {
        Source::Bid => "CREATE TABLE nexmark_bid (
  id BIGINT PRIMARY KEY,
  auction BIGINT,
  bidder BIGINT,
  price BIGINT,
  channel VARCHAR,
  url VARCHAR,
  date_time BIGINT,
  extra VARCHAR
) WITH (
  snapshot = 'true'
) FROM pg_main TABLE 'public.nexmark_bid';
"
        .to_string(),
        Source::Auction => "CREATE TABLE nexmark_auction (
  id BIGINT PRIMARY KEY,
  item_name VARCHAR,
  description VARCHAR,
  initial_bid BIGINT,
  reserve BIGINT,
  date_time BIGINT,
  expires BIGINT,
  seller BIGINT,
  category BIGINT,
  extra VARCHAR
) WITH (
  snapshot = 'true'
) FROM pg_main TABLE 'public.nexmark_auction';
"
        .to_string(),
        Source::Person => "CREATE TABLE nexmark_person (
  id BIGINT PRIMARY KEY,
  name VARCHAR,
  email_address VARCHAR,
  credit_card VARCHAR,
  city VARCHAR,
  state VARCHAR,
  date_time BIGINT,
  extra VARCHAR
) WITH (
  snapshot = 'true'
) FROM pg_main TABLE 'public.nexmark_person';
"
        .to_string(),
    }
}

fn risingwave_view_sql(source: Source) -> String {
    match source {
        Source::Bid => {
            "CREATE MATERIALIZED VIEW bid AS
SELECT auction, bidder, price, channel, url, date_time AS \"dateTime\", extra
FROM nexmark_bid;
"
            .to_string()
        }
        Source::Auction => {
            "CREATE MATERIALIZED VIEW auction AS
SELECT id, item_name AS \"itemName\", description, initial_bid AS \"initialBid\", reserve, date_time AS \"dateTime\", expires, seller, category, extra
FROM nexmark_auction;
"
            .to_string()
        }
        Source::Person => {
            "CREATE MATERIALIZED VIEW person AS
SELECT id, name, city, state, date_time AS \"dateTime\", extra
FROM nexmark_person;
"
            .to_string()
        }
    }
}

fn postgres_setup_sql(publication: &str, sources: &[Source], profile: &WorkloadProfile) -> String {
    let mut sql = String::new();
    sql.push_str(&format!(
        "DROP PUBLICATION IF EXISTS {publication};
DROP VIEW IF EXISTS public.bid;
DROP VIEW IF EXISTS public.auction;
DROP VIEW IF EXISTS public.person;
DROP TABLE IF EXISTS public.nexmark_bid;
DROP TABLE IF EXISTS public.nexmark_auction;
DROP TABLE IF EXISTS public.nexmark_person;
"
    ));
    for source in sources {
        sql.push_str(postgres_table_schema_sql(*source));
    }
    for source in sources {
        sql.push_str(&postgres_insert_initial_sql(
            *source,
            profile.initial_rows(*source),
            profile,
        ));
    }
    for source in sources {
        sql.push_str(postgres_expected_view_sql(*source));
    }
    let tables = sources
        .iter()
        .map(|source| source.upstream_table())
        .collect::<Vec<_>>()
        .join(", ");
    sql.push_str(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {tables} WITH (publish = 'insert, update, delete');
"
    ));
    sql
}

fn postgres_table_schema_sql(source: Source) -> &'static str {
    match source {
        Source::Bid => {
            "CREATE TABLE public.nexmark_bid (
  id BIGINT PRIMARY KEY,
  auction BIGINT NOT NULL,
  bidder BIGINT NOT NULL,
  price BIGINT NOT NULL,
  channel TEXT NOT NULL,
  url TEXT NOT NULL,
  date_time BIGINT NOT NULL,
  extra TEXT NOT NULL
);
"
        }
        Source::Auction => {
            "CREATE TABLE public.nexmark_auction (
  id BIGINT PRIMARY KEY,
  item_name TEXT NOT NULL,
  description TEXT NOT NULL,
  initial_bid BIGINT NOT NULL,
  reserve BIGINT NOT NULL,
  date_time BIGINT NOT NULL,
  expires BIGINT NOT NULL,
  seller BIGINT NOT NULL,
  category BIGINT NOT NULL,
  extra TEXT NOT NULL
);
"
        }
        Source::Person => {
            "CREATE TABLE public.nexmark_person (
  id BIGINT PRIMARY KEY,
  name TEXT NOT NULL,
  email_address TEXT NOT NULL,
  credit_card TEXT NOT NULL,
  city TEXT NOT NULL,
  state TEXT NOT NULL,
  date_time BIGINT NOT NULL,
  extra TEXT NOT NULL
);
"
        }
    }
}

fn postgres_insert_initial_sql(source: Source, rows: u64, profile: &WorkloadProfile) -> String {
    match source {
        Source::Bid => bid_insert_select_sql(
            1,
            rows,
            profile_auction_keyspace_for(profile),
            profile_person_keyspace_for(profile),
        ),
        Source::Auction => auction_insert_select_sql(1, rows, profile_person_keyspace_for(profile)),
        Source::Person => person_insert_select_sql(1, rows),
    }
}

fn postgres_expected_view_sql(source: Source) -> &'static str {
    match source {
        Source::Bid => {
            "CREATE VIEW public.bid AS
SELECT auction, bidder, price, channel, url, date_time AS \"dateTime\", extra
FROM public.nexmark_bid;
"
        }
        Source::Auction => {
            "CREATE VIEW public.auction AS
SELECT id, item_name AS \"itemName\", description, initial_bid AS \"initialBid\", reserve, date_time AS \"dateTime\", expires, seller, category, extra
FROM public.nexmark_auction;
"
        }
        Source::Person => {
            "CREATE VIEW public.person AS
SELECT id, name, city, state, date_time AS \"dateTime\", extra
FROM public.nexmark_person;
"
        }
    }
}

fn mutation_update_sql(source: Source, offset: u64, count: u64) -> String {
    let start = offset;
    let end = offset + count - 1;
    match source {
        Source::Bid => format!(
            "UPDATE public.nexmark_bid
SET price = price + 17,
    channel = CASE WHEN id % 4 = 0 THEN 'apple' WHEN id % 4 = 1 THEN 'google' WHEN id % 4 = 2 THEN 'facebook' ELSE 'baidu' END,
    url = 'https://cdc.example.com/watch/channel_id=' || ((id + 7) % 100)::TEXT || '/u/' || id::TEXT,
    date_time = date_time + 1000,
    extra = extra || '_updated'
WHERE id BETWEEN {start} AND {end};
"
        ),
        Source::Auction => format!(
            "UPDATE public.nexmark_auction
SET reserve = reserve + 31,
    category = CASE WHEN category = 20 THEN 1 ELSE category + 1 END,
    expires = expires + 1000,
    extra = extra || '_updated'
WHERE id BETWEEN {start} AND {end};
"
        ),
        Source::Person => format!(
            "UPDATE public.nexmark_person
SET state = CASE WHEN id % 3 = 0 THEN 'or' WHEN id % 3 = 1 THEN 'id' ELSE 'ca' END,
    city = 'updated_city_' || (id % 100)::TEXT,
    extra = extra || '_updated'
WHERE id BETWEEN {start} AND {end};
"
        ),
    }
}

fn mutation_delete_sql(source: Source, plan: &SourceWorkload, offset: u64, count: u64) -> String {
    let start = plan.updates + offset;
    let end = plan.updates + offset + count - 1;
    format!(
        "DELETE FROM {}
WHERE id BETWEEN {start} AND {end};
",
        source.upstream_table()
    )
}

fn mutation_insert_sql(
    source: Source,
    plan: &SourceWorkload,
    offset: u64,
    count: u64,
    auction_keyspace: u64,
) -> String {
    let start = plan.initial_rows + offset;
    let end = plan.initial_rows + offset + count - 1;
    match source {
        Source::Bid => bid_insert_select_sql(start, end, auction_keyspace, plan.person_keyspace),
        Source::Auction => auction_insert_select_sql(start, end, plan.person_keyspace),
        Source::Person => person_insert_select_sql(start, end),
    }
}

fn bid_insert_select_sql(
    start: u64,
    end: u64,
    auction_keyspace: u64,
    person_keyspace: u64,
) -> String {
    format!(
        "INSERT INTO public.nexmark_bid (id, auction, bidder, price, channel, url, date_time, extra)
SELECT
  gs::BIGINT AS id,
  (((gs - 1) % {auction_keyspace}) + 1)::BIGINT AS auction,
  (((gs - 1) % {person_keyspace}) + 1)::BIGINT AS bidder,
  (1000 + ((gs * 17) % 2000000))::BIGINT AS price,
  CASE WHEN gs % 5 = 0 THEN 'apple' WHEN gs % 5 = 1 THEN 'google' WHEN gs % 5 = 2 THEN 'facebook' WHEN gs % 5 = 3 THEN 'baidu' ELSE 'web' END AS channel,
  'https://nexmark.example.com/auction/' || (((gs - 1) % {auction_keyspace}) + 1)::TEXT || '/bid/' || gs::TEXT || '?channel_id=' || (gs % 100)::TEXT AS url,
  ({NEXMARK_BASE_TS_MS} + gs)::BIGINT AS date_time,
  'bid_extra_ccc_' || gs::TEXT AS extra
FROM generate_series({start}, {end}) AS gs;
"
    )
}

fn auction_insert_select_sql(start: u64, end: u64, person_keyspace: u64) -> String {
    format!(
        "INSERT INTO public.nexmark_auction (id, item_name, description, initial_bid, reserve, date_time, expires, seller, category, extra)
SELECT
  gs::BIGINT AS id,
  'item_' || gs::TEXT AS item_name,
  'auction description ' || gs::TEXT AS description,
  (100 + (gs % 10000))::BIGINT AS initial_bid,
  (1000 + (gs % 100000))::BIGINT AS reserve,
  ({NEXMARK_BASE_TS_MS} + gs)::BIGINT AS date_time,
  ({NEXMARK_BASE_TS_MS} + gs + 86400000)::BIGINT AS expires,
  (((gs - 1) % {person_keyspace}) + 1)::BIGINT AS seller,
  (((gs - 1) % 20) + 1)::BIGINT AS category,
  'auction_extra_' || gs::TEXT AS extra
FROM generate_series({start}, {end}) AS gs;
"
    )
}

fn person_insert_select_sql(start: u64, end: u64) -> String {
    format!(
        "INSERT INTO public.nexmark_person (id, name, email_address, credit_card, city, state, date_time, extra)
SELECT
  gs::BIGINT AS id,
  'person_' || gs::TEXT AS name,
  'person_' || gs::TEXT || '@example.com' AS email_address,
  '411111111111' || LPAD((gs % 10000)::TEXT, 4, '0') AS credit_card,
  'city_' || (gs % 100)::TEXT AS city,
  CASE WHEN gs % 6 = 0 THEN 'or' WHEN gs % 6 = 1 THEN 'id' WHEN gs % 6 = 2 THEN 'ca' WHEN gs % 6 = 3 THEN 'wa' WHEN gs % 6 = 4 THEN 'ny' ELSE 'tx' END AS state,
  ({NEXMARK_BASE_TS_MS} + gs)::BIGINT AS date_time,
  'person_extra_' || gs::TEXT AS extra
FROM generate_series({start}, {end}) AS gs;
"
    )
}

fn profile_auction_keyspace_for(profile: &WorkloadProfile) -> u64 {
    profile
        .sources
        .get(&Source::Bid)
        .or_else(|| profile.sources.values().next())
        .map(|plan| plan.auction_keyspace)
        .unwrap_or(NEXMARK_BID_AUCTION_CARDINALITY)
        .max(1)
}

fn profile_person_keyspace_for(profile: &WorkloadProfile) -> u64 {
    profile
        .sources
        .get(&Source::Bid)
        .or_else(|| profile.sources.values().next())
        .map(|plan| plan.person_keyspace)
        .unwrap_or(DEFAULT_PERSON_ROWS)
        .max(1)
}

fn profile_auction_keyspace(plan: &SourceWorkload) -> u64 {
    plan.auction_keyspace.max(1)
}

fn floe_config_json(config: &Config, artifact_dir: &Path) -> serde_json::Value {
    json!({
        "runtime": {
            "pgwire_addr": format!("127.0.0.1:{}", config.floe_pg_port),
            "admin_port": config.floe_admin_port
        },
        "storage": {
            "data_dir": artifact_dir.join("floe-data").display().to_string()
        },
        "postgres_cdc": {
            "snapshot": {
                "rows_per_batch": config.snapshot_rows_per_batch,
                "max_workers": config.snapshot_max_workers,
                "intra_table_chunks": config.snapshot_intra_table_chunks
            }
        }
    })
}

fn query_sql_risingwave(query_id: &str) -> Option<&'static str> {
    query_sql_portable(query_id)
}

fn expected_sql_for_engine(engine: Engine, query_id: &str) -> Option<&'static str> {
    match engine {
        Engine::Floe => query_sql_expected_for_floe(query_id),
        Engine::RisingWave => query_sql_portable(query_id),
    }
}

fn query_sql_portable(query_id: &str) -> Option<&'static str> {
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
            r#"SELECT category, CAST(AVG(max) AS BIGINT) AS avg_price FROM (SELECT MAX(b.price) AS max, a.category FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires GROUP BY a.id, a.category) per_auction GROUP BY category"#
        }
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
        "q6" => {
            r#"SELECT seller, CAST(AVG(price) AS BIGINT) AS moving_avg_price FROM (SELECT a.seller, b.price, b."dateTime", ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC, b."dateTime" ASC, b.bidder ASC, b.channel ASC, b.url ASC, b.extra ASC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires) ranked WHERE rownum <= 1 GROUP BY seller"#
        }
        "q7" => r#"SELECT MAX(price) AS maxprice FROM bid GROUP BY ("dateTime" / 10000)"#,
        "q8" => {
            r#"SELECT id, name, COUNT(*) AS person_count FROM person GROUP BY id, name, ("dateTime" / 10000)"#
        }
        "q9" => {
            r#"SELECT id, "itemName", description, "initialBid", reserve, "dateTime", expires, seller, category, extra, auction, bidder, price, "bidTime", "bidExtra" FROM (SELECT a.id, a."itemName", a.description, a."initialBid", a.reserve, a."dateTime", a.expires, a.seller, a.category, a.extra, b.auction, b.bidder, b.price, b."dateTime" AS "bidTime", b.extra AS "bidExtra", ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b."dateTime" ASC, b.bidder ASC, b.extra ASC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires) ranked WHERE rownum <= 1"#
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
            r#"SELECT ("dateTime" / 86400000) AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY ("dateTime" / 86400000)"#
        }
        "q16" => {
            r#"SELECT channel, ("dateTime" / 86400000) AS day, MAX((("dateTime" / 60000) % 1440)) AS minute, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY channel, ("dateTime" / 86400000)"#
        }
        "q17" => {
            r#"SELECT auction, ("dateTime" / 86400000) AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, MIN(price) AS min_price, MAX(price) AS max_price, CAST(AVG(price) AS BIGINT) AS avg_price, SUM(price) AS sum_price FROM bid GROUP BY auction, ("dateTime" / 86400000)"#
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
            r#"SELECT auction, bidder, price, channel, CASE WHEN lower(channel) = 'apple' THEN '0' WHEN lower(channel) = 'google' THEN '1' WHEN lower(channel) = 'facebook' THEN '2' WHEN lower(channel) = 'baidu' THEN '3' ELSE NULLIF(SPLIT_PART(SPLIT_PART(url, 'channel_id=', 2), '&', 1), '') END AS channel_id FROM bid WHERE NULLIF(SPLIT_PART(SPLIT_PART(url, 'channel_id=', 2), '&', 1), '') IS NOT NULL OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')"#
        }
        "q22" => {
            r#"SELECT auction, bidder, price, channel, SPLIT_PART(url, '/', 4) AS dir1, SPLIT_PART(url, '/', 5) AS dir2, SPLIT_PART(url, '/', 6) AS dir3 FROM bid"#
        }
        _ => return None,
    })
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
            r#"SELECT auction, COUNT(*) AS num
FROM (
  SELECT auction, ((date_time / 2000) * 2000 - 0) AS hop_start FROM nexmark_bid
  UNION ALL
  SELECT auction, ((date_time / 2000) * 2000 - 2000) AS hop_start FROM nexmark_bid
  UNION ALL
  SELECT auction, ((date_time / 2000) * 2000 - 4000) AS hop_start FROM nexmark_bid
  UNION ALL
  SELECT auction, ((date_time / 2000) * 2000 - 6000) AS hop_start FROM nexmark_bid
  UNION ALL
  SELECT auction, ((date_time / 2000) * 2000 - 8000) AS hop_start FROM nexmark_bid
) expanded
GROUP BY auction, hop_start"#
        }
        "q6" => {
            r#"SELECT seller, CAST(AVG(price) AS BIGINT) AS moving_avg_price FROM (SELECT a.seller, b.price, b.date_time, ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC, b.date_time ASC, b.bidder ASC, b.channel ASC, b.url ASC, b.extra ASC) AS rownum FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction WHERE b.date_time BETWEEN a.date_time AND a.expires) ranked WHERE rownum <= 1 GROUP BY seller"#
        }
        "q7" => r#"SELECT MAX(price) AS maxprice FROM nexmark_bid GROUP BY (date_time / 10000)"#,
        "q8" => {
            r#"SELECT id, name, COUNT(*) AS person_count FROM nexmark_person GROUP BY id, name, (date_time / 10000)"#
        }
        "q9" => {
            r#"SELECT id, "itemName", description, "initialBid", reserve, "dateTime", expires, seller, category, extra, auction, bidder, price, "bidTime", "bidExtra" FROM (SELECT a.id, a.item_name AS "itemName", a.description, a.initial_bid AS "initialBid", a.reserve, a.auction_time AS "dateTime", a.expires, a.seller, a.category, a.auction_extra AS extra, b.auction, b.bidder, b.price, b.bid_time AS "bidTime", b.bid_extra AS "bidExtra", ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b.bid_time ASC, b.bidder ASC, b.bid_extra ASC) AS rownum FROM (SELECT id, item_name, description, initial_bid, reserve, date_time AS auction_time, expires, seller, category, extra AS auction_extra FROM nexmark_auction) a JOIN (SELECT auction, bidder, price, date_time AS bid_time, extra AS bid_extra FROM nexmark_bid) b ON a.id = b.auction WHERE b.bid_time BETWEEN a.auction_time AND a.expires) ranked WHERE rownum <= 1"#
        }
        "q12" => {
            r#"SELECT bidder, COUNT(*) AS bid_count FROM nexmark_bid GROUP BY bidder, (date_time / 10000)"#
        }
        "q13" => {
            r#"SELECT b.auction, b.bidder, b.price, b.date_time AS "dateTime", a.seller AS value FROM (SELECT *, PROCTIME() AS p_time FROM nexmark_bid) b JOIN nexmark_auction AS a ON b.auction = a.id WHERE b.auction % 10000 = a.id % 10000"#
        }
        "q14" => {
            r#"SELECT auction, bidder, price * 908 / 1000 AS price, CASE WHEN ((date_time / 3600000) % 24) >= 8 AND ((date_time / 3600000) % 24) <= 18 THEN 'dayTime' WHEN ((date_time / 3600000) % 24) <= 6 OR ((date_time / 3600000) % 24) >= 20 THEN 'nightTime' ELSE 'otherTime' END AS bid_time_type, date_time AS "dateTime", extra, COUNT_CHAR(extra, 'c') AS c_counts FROM nexmark_bid WHERE price * 908 / 1000 > 1000000 AND price * 908 / 1000 < 50000000"#
        }
        "q15" => {
            r#"SELECT (date_time / 86400000) AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM nexmark_bid GROUP BY (date_time / 86400000)"#
        }
        "q16" => {
            r#"SELECT channel, (date_time / 86400000) AS day, MAX(((date_time / 60000) % 1440)) AS minute, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM nexmark_bid GROUP BY channel, (date_time / 86400000)"#
        }
        "q17" => {
            r#"SELECT auction, (date_time / 86400000) AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, MIN(price) AS min_price, MAX(price) AS max_price, CAST(AVG(price) AS BIGINT) AS avg_price, SUM(price) AS sum_price FROM nexmark_bid GROUP BY auction, (date_time / 86400000)"#
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
            r#"SELECT auction, bidder, price, channel, CASE WHEN lower(channel) = 'apple' THEN '0' WHEN lower(channel) = 'google' THEN '1' WHEN lower(channel) = 'facebook' THEN '2' WHEN lower(channel) = 'baidu' THEN '3' ELSE REGEXP_EXTRACT(url, 'channel_id=([^&]*)', 1) END AS channel_id FROM nexmark_bid WHERE REGEXP_EXTRACT(url, 'channel_id=([^&]*)', 1) IS NOT NULL OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')"#
        }
        "q22" => {
            r#"SELECT auction, bidder, price, channel, SPLIT_INDEX(url, '/', 3) AS dir1, SPLIT_INDEX(url, '/', 4) AS dir2, SPLIT_INDEX(url, '/', 5) AS dir3 FROM nexmark_bid"#
        }
        _ => return None,
    })
}

fn query_sql_expected_for_floe(query_id: &str) -> Option<&'static str> {
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
            r#"SELECT category, CAST(FLOOR(AVG(max)) AS BIGINT) AS avg_price FROM (SELECT MAX(b.price) AS max, a.category FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires GROUP BY a.id, a.category) per_auction GROUP BY category"#
        }
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
        "q6" => {
            r#"SELECT seller, CAST(FLOOR(AVG(price)) AS BIGINT) AS moving_avg_price FROM (SELECT a.seller, b.price, b."dateTime", ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC, b."dateTime" ASC, b.bidder ASC, b.channel ASC, b.url ASC, b.extra ASC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires) ranked WHERE rownum <= 1 GROUP BY seller"#
        }
        "q7" => r#"SELECT MAX(price) AS maxprice FROM bid GROUP BY ("dateTime" / 10000)"#,
        "q8" => {
            r#"SELECT id, name, COUNT(*) AS person_count FROM person GROUP BY id, name, ("dateTime" / 10000)"#
        }
        "q9" => {
            r#"SELECT id, "itemName", description, "initialBid", reserve, "dateTime", expires, seller, category, extra, auction, bidder, price, "bidTime", "bidExtra" FROM (SELECT a.id, a."itemName", a.description, a."initialBid", a.reserve, a."dateTime", a.expires, a.seller, a.category, a.extra, b.auction, b.bidder, b.price, b."dateTime" AS "bidTime", b.extra AS "bidExtra", ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b."dateTime" ASC, b.bidder ASC, b.extra ASC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires) ranked WHERE rownum <= 1"#
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
            r#"SELECT ("dateTime" / 86400000) AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY ("dateTime" / 86400000)"#
        }
        "q16" => {
            r#"SELECT channel, ("dateTime" / 86400000) AS day, MAX((("dateTime" / 60000) % 1440)) AS minute, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY channel, ("dateTime" / 86400000)"#
        }
        "q17" => {
            r#"SELECT auction, ("dateTime" / 86400000) AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, MIN(price) AS min_price, MAX(price) AS max_price, CAST(FLOOR(AVG(price)) AS BIGINT) AS avg_price, SUM(price) AS sum_price FROM bid GROUP BY auction, ("dateTime" / 86400000)"#
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
            r#"SELECT auction, bidder, price, channel, CASE WHEN lower(channel) = 'apple' THEN '0' WHEN lower(channel) = 'google' THEN '1' WHEN lower(channel) = 'facebook' THEN '2' WHEN lower(channel) = 'baidu' THEN '3' ELSE substring(url from 'channel_id=([^&]*)') END AS channel_id FROM bid WHERE substring(url from 'channel_id=([^&]*)') IS NOT NULL OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')"#
        }
        "q22" => {
            r#"SELECT auction, bidder, price, channel, SPLIT_PART(url, '/', 4) AS dir1, SPLIT_PART(url, '/', 5) AS dir2, SPLIT_PART(url, '/', 6) AS dir3 FROM bid"#
        }
        _ => return None,
    })
}

fn fingerprint_file_lines(path: &Path) -> Result<ContentFingerprint> {
    let content = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut lines = content.lines().map(ToOwned::to_owned).collect::<Vec<_>>();
    lines.sort();
    let mut hasher = Sha256::new();
    for line in &lines {
        hasher.update(line.as_bytes());
        hasher.update(b"\n");
    }
    Ok(ContentFingerprint {
        row_count: lines.len() as u64,
        hash: hex::encode(hasher.finalize()),
    })
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

fn slot_name(run_id: &str, engine: Engine, query_id: &str) -> String {
    format!("floe_cdc_{}_{}_{}", engine.as_str(), query_id, run_id)
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect::<String>()
        .to_ascii_lowercase()
}

fn publication_name(run_id: &str, engine: Engine, query_id: &str) -> String {
    format!("floe_cdc_pub_{}_{}_{}", engine.as_str(), query_id, run_id)
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect::<String>()
        .to_ascii_lowercase()
}

fn repo_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow!("cannot derive repo root from CARGO_MANIFEST_DIR"))
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

fn current_millis() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before UNIX_EPOCH")?
        .as_millis())
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

fn ensure_status(status: ExitStatus) -> Result<()> {
    ensure!(status.success(), "command failed with {status}");
    Ok(())
}

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

fn seconds_cell(ms: Option<u128>) -> String {
    ms.map(|ms| format!("{:.3}", ms as f64 / 1000.0))
        .unwrap_or_else(|| "n/a".to_string())
}

fn rate(rows: u64, ms: u128) -> f64 {
    if ms == 0 {
        return rows as f64;
    }
    rows as f64 / (ms as f64 / 1000.0)
}

fn log(message: impl AsRef<str>) {
    println!("[nexmark-postgres-cdc] {}", message.as_ref());
}

fn print_tail(path: impl AsRef<Path>, lines: usize) {
    if let Ok(content) = fs::read_to_string(path) {
        let tail = content.lines().rev().take(lines).collect::<Vec<_>>();
        for line in tail.into_iter().rev() {
            eprintln!("{line}");
        }
    }
}

fn print_usage() {
    println!(
        "Usage: nexmark_postgres_cdc_compare [floe|risingwave|floe,risingwave|all] [all|nexmark_all|q0..q22]"
    );
    println!(
        "Environment: CDC_OPS=1000000 LIVE_WRITE_CHUNK_ROWS=16384 CDC_SLOT_CATCHUP_MAX_LAG_BYTES=16777216 STRICT_CONTENT_CHECK=true"
    );
}
