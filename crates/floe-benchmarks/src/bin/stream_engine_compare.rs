use std::fs::{self, File};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use serde_json::json;

#[path = "stream_engine_compare/config.rs"]
mod config;
#[path = "harness_common/mod.rs"]
mod harness_common;
#[path = "stream_engine_compare/report.rs"]
mod report;
#[path = "stream_engine_compare/sql.rs"]
mod sql;

use config::{BenchQuery, Config, Engine};
use harness_common::*;
use report::{
    EngineResult, capture_floe_metadata, capture_image_metadata, write_result, write_run_context,
    write_summary_header,
};
use sql::{feldera_sql, materialize_sql, risingwave_sql};

const QUERY_RESULT_RELATION: &str = "benchmark_result";
const QUERY_COUNT_RELATION: &str = "benchmark_result_count";

fn main() -> Result<()> {
    let config = Config::from_env_and_args()?;
    let mut harness = Harness::new(config)?;
    harness.run()
}

struct Harness {
    config: Config,
    floe_child: Option<Child>,
}

struct PgBenchmarkSpec<'a> {
    port: u16,
    user: &'a str,
    db: &'a str,
    bid_topic: &'a str,
    auction_topic: Option<&'a str>,
    count_sql: &'a str,
    label: &'a str,
}

impl Harness {
    fn new(config: Config) -> Result<Self> {
        fs::create_dir_all(&config.run_dir)
            .with_context(|| format!("create {}", config.run_dir.display()))?;
        Ok(Self {
            config,
            floe_child: None,
        })
    }

    fn run(&mut self) -> Result<()> {
        write_summary_header(&self.config)?;
        self.ensure_redpanda()?;
        self.build_producer()?;
        write_run_context(&self.config)?;
        if self
            .config
            .engine_selector
            .selected()
            .contains(&Engine::Floe)
        {
            self.build_floe_node()?;
        }
        for engine in self.config.engine_selector.selected() {
            match engine {
                Engine::Floe => self.floe_benchmark()?,
                Engine::Materialize => self.materialize_benchmark()?,
                Engine::RisingWave => self.risingwave_benchmark()?,
                Engine::Feldera => self.feldera_benchmark()?,
            }
        }
        log(format!(
            "results written to {}",
            self.config.results_file().display()
        ));
        println!("{}", fs::read_to_string(self.config.results_file())?);
        Ok(())
    }

    fn ensure_redpanda(&self) -> Result<()> {
        let running = run_capture(
            "docker",
            ["ps", "--format", "{{.Names}}"],
            Some(&self.config.repo_root),
        )
        .unwrap_or_default()
        .lines()
        .any(|line| line == self.config.redpanda_container);
        if running {
            capture_image_metadata(
                &self.config,
                &self.config.redpanda_image,
                &self.config.run_dir.join("redpanda_image_metadata.json"),
            );
            return Ok(());
        }

        self.ensure_network()?;
        self.docker_rm_force(&[&self.config.redpanda_container]);
        log(format!(
            "starting Redpanda {}",
            self.config.redpanda_container
        ));
        run_status(
            "docker",
            ["pull", self.config.redpanda_image.as_str()],
            Some(&self.config.repo_root),
        )?;
        capture_image_metadata(
            &self.config,
            &self.config.redpanda_image,
            &self.config.run_dir.join("redpanda_image_metadata.json"),
        );
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
            Some(&self.config.repo_root),
        )?;
        let ready = wait_until(Duration::from_secs(90), Duration::from_secs(1), || {
            command_success(
                "docker",
                [
                    "exec",
                    &self.config.redpanda_container,
                    "rpk",
                    "cluster",
                    "info",
                ],
                Some(&self.config.repo_root),
            )
        })?;
        if ready {
            Ok(())
        } else {
            let _ = Command::new("docker")
                .args(["logs", &self.config.redpanda_container])
                .status();
            bail!("Redpanda did not become ready")
        }
    }

    fn ensure_network(&self) -> Result<()> {
        if command_success(
            "docker",
            ["network", "inspect", &self.config.network_name],
            Some(&self.config.repo_root),
        )? {
            return Ok(());
        }
        run_status(
            "docker",
            ["network", "create", &self.config.network_name],
            Some(&self.config.repo_root),
        )
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
    }

    fn build_floe_node(&self) -> Result<()> {
        log("building floe-node release binary");
        run_status(
            "cargo",
            ["build", "-p", "floe-node", "--release"],
            Some(&self.config.repo_root),
        )
    }

    fn reset_topic(&self, topic: &str) -> Result<()> {
        let _ = Command::new("docker")
            .args([
                "exec",
                &self.config.redpanda_container,
                "rpk",
                "topic",
                "delete",
                topic,
            ])
            .status();
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
            Some(&self.config.repo_root),
        )
    }

    fn produce_topic(&self, topic: &str, dataset: &str, rows: u64) -> Result<u128> {
        let start = current_millis()?;
        run_status(
            self.config.release_binary("kafka_million_bid_producer"),
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
        )?;
        Ok(current_millis()? - start)
    }

    fn produce_query_inputs(&self, bid_topic: &str, auction_topic: Option<&str>) -> Result<u128> {
        match self.config.bench_query {
            BenchQuery::FilterProjection => self.produce_topic(bid_topic, "bid", self.config.rows),
            BenchQuery::Join => {
                let auction_topic = auction_topic.context("join query requires auction topic")?;
                let auction_ms =
                    self.produce_topic(auction_topic, "auction", self.config.join_auction_rows)?;
                let bid_ms = self.produce_topic(bid_topic, "bid", self.config.rows)?;
                Ok(auction_ms + bid_ms)
            }
        }
    }

    fn materialize_benchmark(&mut self) -> Result<()> {
        let artifact_dir = self.config.run_dir.join("materialize");
        fs::create_dir_all(&artifact_dir)?;
        let bid_topic = format!("materialize_bids_{}", self.config.run_id);
        let auction_topic = format!("materialize_auctions_{}", self.config.run_id);
        self.docker_rm_force(&[&self.config.materialize_container]);
        log("starting Materialize emulator");
        run_status(
            "docker",
            ["pull", &self.config.materialize_image],
            Some(&self.config.repo_root),
        )?;
        capture_image_metadata(
            &self.config,
            &self.config.materialize_image,
            &artifact_dir.join("image_metadata.json"),
        );
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
            Some(&self.config.repo_root),
        )?;
        self.wait_for_pg(
            self.config.materialize_sql_port,
            "materialize",
            "materialize",
            "Materialize",
        )?;
        self.reset_topics(&bid_topic, &auction_topic)?;
        let mode = if self.config.materialize_best_effort_in_memory {
            "indexed_views"
        } else {
            "durable_mvs"
        };
        let setup = materialize_sql(&self.config, &bid_topic, &auction_topic);
        let setup_path = artifact_dir.join("setup.sql");
        write_file(&setup_path, setup)?;
        self.run_psql_file(
            self.config.materialize_sql_port,
            "materialize",
            "materialize",
            &setup_path,
        )?;
        let count_sql = format!("SELECT row_count FROM {QUERY_COUNT_RELATION}");
        let (total_ms, produce_ms, post_ms, rows_per_sec) =
            self.timed_pg_benchmark(PgBenchmarkSpec {
                port: self.config.materialize_sql_port,
                user: "materialize",
                db: "materialize",
                bid_topic: &bid_topic,
                auction_topic: Some(&auction_topic),
                count_sql: &count_sql,
                label: "Materialize",
            })?;
        write_file(artifact_dir.join("mode.txt"), mode)?;
        write_result(
            &self.config,
            EngineResult {
                engine: Engine::Materialize,
                artifact_dir: &artifact_dir,
                total_ms,
                produce_ms,
                post_ms,
                rows_per_sec,
                completion_signal: "count_view_pgwire",
            },
        )
    }

    fn risingwave_benchmark(&mut self) -> Result<()> {
        let artifact_dir = self.config.run_dir.join("risingwave");
        fs::create_dir_all(&artifact_dir)?;
        let bid_topic = format!("risingwave_bids_{}", self.config.run_id);
        let auction_topic = format!("risingwave_auctions_{}", self.config.run_id);
        self.docker_rm_force(&[&self.config.risingwave_container]);
        log("starting RisingWave single-node container");
        run_status(
            "docker",
            ["pull", &self.config.risingwave_image],
            Some(&self.config.repo_root),
        )?;
        capture_image_metadata(
            &self.config,
            &self.config.risingwave_image,
            &artifact_dir.join("image_metadata.json"),
        );
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
        run_status("docker", &args, Some(&self.config.repo_root))?;
        self.wait_for_pg(self.config.risingwave_sql_port, "root", "dev", "RisingWave")?;
        self.reset_topics(&bid_topic, &auction_topic)?;
        let setup = risingwave_sql(&self.config, &bid_topic, &auction_topic);
        let setup_path = artifact_dir.join("setup.sql");
        write_file(&setup_path, setup)?;
        self.run_psql_file(self.config.risingwave_sql_port, "root", "dev", &setup_path)?;
        let count_sql = format!("SELECT row_count FROM {QUERY_COUNT_RELATION}");
        let (total_ms, produce_ms, post_ms, rows_per_sec) =
            self.timed_pg_benchmark(PgBenchmarkSpec {
                port: self.config.risingwave_sql_port,
                user: "root",
                db: "dev",
                bid_topic: &bid_topic,
                auction_topic: Some(&auction_topic),
                count_sql: &count_sql,
                label: "RisingWave",
            })?;
        write_file(
            artifact_dir.join("in_memory.txt"),
            self.config.risingwave_in_memory.to_string(),
        )?;
        write_file(
            artifact_dir.join("kafka_fetch_profile.txt"),
            if self.config.kafka_latency_fetch_profile {
                "latency"
            } else {
                "default"
            },
        )?;
        write_result(
            &self.config,
            EngineResult {
                engine: Engine::RisingWave,
                artifact_dir: &artifact_dir,
                total_ms,
                produce_ms,
                post_ms,
                rows_per_sec,
                completion_signal: "count_view_pgwire",
            },
        )
    }

    fn feldera_benchmark(&mut self) -> Result<()> {
        let artifact_dir = self.config.run_dir.join("feldera");
        fs::create_dir_all(&artifact_dir)?;
        let bid_topic = format!("feldera_bids_{}", self.config.run_id);
        let auction_topic = format!("feldera_auctions_{}", self.config.run_id);
        let pipeline = format!("stream_bench_{}", self.config.run_id);
        self.docker_rm_force(&[&self.config.feldera_container]);
        log("starting Feldera pipeline-manager container");
        run_status(
            "docker",
            ["pull", &self.config.feldera_image],
            Some(&self.config.repo_root),
        )?;
        capture_image_metadata(
            &self.config,
            &self.config.feldera_image,
            &artifact_dir.join("image_metadata.json"),
        );
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
            Some(&self.config.repo_root),
        )?;
        self.wait_for_http_ok(
            &format!(
                "http://127.0.0.1:{}/v0/pipelines",
                self.config.feldera_http_port
            ),
            "Feldera",
        )?;
        self.reset_topics(&bid_topic, &auction_topic)?;
        let program = feldera_sql(&self.config, &bid_topic, &auction_topic);
        let program_path = artifact_dir.join("program.sql");
        write_file(&program_path, &program)?;
        let payload = if self.config.feldera_best_effort_in_memory {
            json!({
                "name": pipeline,
                "description": "Floe stream engine comparison benchmark",
                "runtime_config": {
                    "workers": self.config.feldera_workers,
                    "storage": {
                        "min_storage_bytes": self.config.feldera_min_storage_bytes,
                        "min_step_storage_bytes": self.config.feldera_min_step_storage_bytes
                    }
                },
                "program_config": {},
                "program_code": program
            })
        } else {
            json!({
                "name": pipeline,
                "description": "Floe stream engine comparison benchmark",
                "runtime_config": {"workers": self.config.feldera_workers},
                "program_config": {},
                "program_code": program
            })
        };
        let create = run_capture(
            "curl",
            [
                "-fsS",
                "-X",
                "PUT",
                &format!(
                    "http://127.0.0.1:{}/v0/pipelines/{pipeline}",
                    self.config.feldera_http_port
                ),
                "-H",
                "Content-Type: application/json",
                "-d",
                &payload.to_string(),
            ],
            Some(&self.config.repo_root),
        )?;
        write_file(artifact_dir.join("pipeline_create.json"), create)?;
        self.poll_feldera_program_success(&pipeline)?;
        run_status(
            "curl",
            [
                "-fsS",
                "-X",
                "POST",
                &format!(
                    "http://127.0.0.1:{}/v0/pipelines/{pipeline}/start",
                    self.config.feldera_http_port
                ),
            ],
            Some(&self.config.repo_root),
        )?;
        self.poll_feldera_running(&pipeline)?;
        let start = current_millis()?;
        let produce_ms = self.produce_query_inputs(&bid_topic, Some(&auction_topic))?;
        let post_start = current_millis()?;
        self.poll_feldera_completion(&pipeline)?;
        let post_ms = current_millis()? - post_start;
        let total_ms = current_millis()? - start;
        let rows_per_sec = rows_per_second(self.config.input_rows_total, total_ms);
        write_file(
            artifact_dir.join("runtime_storage_mode.json"),
            serde_json::to_vec_pretty(&json!({
                "best_effort_in_memory": self.config.feldera_best_effort_in_memory,
                "min_storage_bytes": self.config.feldera_best_effort_in_memory.then_some(self.config.feldera_min_storage_bytes),
                "min_step_storage_bytes": self.config.feldera_best_effort_in_memory.then_some(self.config.feldera_min_step_storage_bytes),
                "kafka_fetch_profile": if self.config.kafka_latency_fetch_profile { "latency" } else { "default" }
            }))?,
        )?;
        let signal = if self.config.feldera_completion_mode == "count" {
            "count_view_adhoc_query"
        } else {
            "completed_records_stats"
        };
        write_result(
            &self.config,
            EngineResult {
                engine: Engine::Feldera,
                artifact_dir: &artifact_dir,
                total_ms,
                produce_ms,
                post_ms,
                rows_per_sec,
                completion_signal: signal,
            },
        )
    }

    fn floe_benchmark(&mut self) -> Result<()> {
        let artifact_dir = self.config.run_dir.join("floe");
        fs::create_dir_all(&artifact_dir)?;
        let bid_topic = format!("floe_bids_{}", self.config.run_id);
        let auction_topic = format!("floe_auctions_{}", self.config.run_id);
        self.stop_floe_process();
        self.reset_topics(&bid_topic, &auction_topic)?;
        capture_floe_metadata(&self.config, &artifact_dir.join("binary_metadata.json"))?;
        let config_path = artifact_dir.join("floe_config.json");
        let mv_program = self.write_floe_config(&config_path, &bid_topic, &auction_topic)?;
        log("starting Floe native benchmark process");
        let stdout = File::create(artifact_dir.join("floe-node.stdout.log"))?;
        let stderr = File::create(artifact_dir.join("floe-node.stderr.log"))?;
        let pgwire_addr = format!("127.0.0.1:{}", self.config.floe_pg_port);
        let l0_sst_bytes = self.config.floe_l0_sst_bytes.to_string();
        let max_unflushed_bytes = self.config.floe_max_unflushed_bytes.to_string();
        let config_path = config_path.to_str().context("config path is not UTF-8")?;
        let mut command = Command::new(self.config.release_binary("floe-node"));
        configure_process_group(&mut command);
        let child = command
            .args([
                "run",
                "--pgwire-addr",
                &pgwire_addr,
                "--admin-port",
                "0",
                "--slatedb-await-durable",
                "false",
                "--slatedb-l0-sst-bytes",
                &l0_sst_bytes,
                "--slatedb-max-unflushed-bytes",
                &max_unflushed_bytes,
                "--config",
                config_path,
                "--mv-query",
                &mv_program,
            ])
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .context("spawn floe-node")?;
        self.floe_child = Some(child);
        self.wait_for_floe_pg(&artifact_dir)?;
        let count_sql = format!("SELECT COUNT(*)::BIGINT FROM {QUERY_RESULT_RELATION}");
        let (total_ms, produce_ms, post_ms, rows_per_sec) =
            self.timed_pg_benchmark(PgBenchmarkSpec {
                port: self.config.floe_pg_port,
                user: "postgres",
                db: "postgres",
                bid_topic: &bid_topic,
                auction_topic: Some(&auction_topic),
                count_sql: &count_sql,
                label: "Floe",
            })?;
        self.stop_floe_process();
        write_result(
            &self.config,
            EngineResult {
                engine: Engine::Floe,
                artifact_dir: &artifact_dir,
                total_ms,
                produce_ms,
                post_ms,
                rows_per_sec,
                completion_signal: "count_query_pgwire",
            },
        )
    }

    fn timed_pg_benchmark(&self, spec: PgBenchmarkSpec<'_>) -> Result<(u128, u128, u128, u64)> {
        let start = current_millis()?;
        let produce_ms = self.produce_query_inputs(spec.bid_topic, spec.auction_topic)?;
        let post_start = current_millis()?;
        self.poll_pg_count(spec.port, spec.user, spec.db, spec.count_sql, spec.label)?;
        let post_ms = current_millis()? - post_start;
        let total_ms = current_millis()? - start;
        Ok((
            total_ms,
            produce_ms,
            post_ms,
            rows_per_second(self.config.input_rows_total, total_ms),
        ))
    }

    fn reset_topics(&self, bid_topic: &str, auction_topic: &str) -> Result<()> {
        self.reset_topic(bid_topic)?;
        if self.config.bench_query == BenchQuery::Join {
            self.reset_topic(auction_topic)?;
        }
        Ok(())
    }

    fn wait_for_pg(&self, port: u16, user: &str, db: &str, label: &str) -> Result<()> {
        let ready = wait_until(Duration::from_secs(90), Duration::from_secs(1), || {
            Ok(self
                .run_psql(port, user, db, "SELECT 1")
                .is_ok_and(|out| out.trim() == "1"))
        })?;
        ensure!(ready, "{label} did not become ready on port {port}");
        Ok(())
    }

    fn wait_for_http_ok(&self, url: &str, label: &str) -> Result<()> {
        let ready = wait_until(Duration::from_secs(90), Duration::from_secs(1), || {
            command_success("curl", ["-fsS", url], Some(&self.config.repo_root))
        })?;
        ensure!(ready, "{label} did not become ready at {url}");
        Ok(())
    }

    fn wait_for_floe_pg(&mut self, artifact_dir: &Path) -> Result<()> {
        let stderr_path = artifact_dir.join("floe-node.stderr.log");
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            if let Some(child) = &mut self.floe_child
                && let Some(status) = child.try_wait()?
            {
                print_tail(&stderr_path, 120);
                bail!("Floe process exited before pgwire became ready: {status}");
            }
            if self
                .run_psql(self.config.floe_pg_port, "postgres", "postgres", "SELECT 1")
                .is_ok_and(|out| out.trim() == "1")
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                print_tail(&stderr_path, 120);
                bail!(
                    "Floe did not become ready on port {}",
                    self.config.floe_pg_port
                );
            }
            std::thread::park_timeout(Duration::from_secs(1));
        }
    }

    fn poll_pg_count(&self, port: u16, user: &str, db: &str, sql: &str, label: &str) -> Result<()> {
        let ready = wait_until(self.config.poll_timeout, self.config.poll_interval, || {
            let count = self
                .run_psql(port, user, db, sql)
                .unwrap_or_default()
                .trim()
                .parse::<u64>()
                .unwrap_or(0);
            Ok(count >= self.config.expected_rows)
        })?;
        ensure!(
            ready,
            "{label} did not reach count {}",
            self.config.expected_rows
        );
        Ok(())
    }

    fn run_psql(&self, port: u16, user: &str, db: &str, sql: &str) -> Result<String> {
        let output = Command::new("psql")
            .args([
                "-h",
                "127.0.0.1",
                "-p",
                &port.to_string(),
                "-U",
                user,
                "-d",
                db,
                "-v",
                "ON_ERROR_STOP=1",
                "-Atqc",
                sql,
            ])
            .env("PGPASSWORD", "")
            .output()
            .context("run psql")?;
        ensure!(
            output.status.success(),
            "psql failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    fn run_psql_file(&self, port: u16, user: &str, db: &str, file: &Path) -> Result<()> {
        let status = Command::new("psql")
            .args([
                "-h",
                "127.0.0.1",
                "-p",
                &port.to_string(),
                "-U",
                user,
                "-d",
                db,
                "-v",
                "ON_ERROR_STOP=1",
                "-f",
                file.to_str().context("SQL path is not UTF-8")?,
            ])
            .env("PGPASSWORD", "")
            .status()
            .context("run psql file")?;
        ensure_status(status)
    }

    fn poll_feldera_program_success(&self, pipeline: &str) -> Result<()> {
        let ready = wait_until(Duration::from_secs(480), Duration::from_secs(2), || {
            let response = run_capture(
                "curl",
                [
                    "-fsS",
                    &format!(
                        "http://127.0.0.1:{}/v0/pipelines/{pipeline}",
                        self.config.feldera_http_port
                    ),
                ],
                Some(&self.config.repo_root),
            )
            .unwrap_or_default();
            let status = serde_json::from_str::<serde_json::Value>(&response)
                .ok()
                .and_then(|value| value["program_status"].as_str().map(str::to_string));
            if matches!(
                status.as_deref(),
                Some("SqlError" | "RustError" | "SystemError")
            ) {
                bail!("Feldera program failed with status {}", status.unwrap());
            }
            Ok(status.as_deref() == Some("Success"))
        })?;
        ensure!(ready, "Feldera program did not compile successfully");
        Ok(())
    }

    fn poll_feldera_running(&self, pipeline: &str) -> Result<()> {
        let ready = wait_until(Duration::from_secs(120), Duration::from_secs(1), || {
            let response = run_capture(
                "curl",
                [
                    "-fsS",
                    &format!(
                        "http://127.0.0.1:{}/v0/pipelines/{pipeline}",
                        self.config.feldera_http_port
                    ),
                ],
                Some(&self.config.repo_root),
            )
            .unwrap_or_default();
            let status = serde_json::from_str::<serde_json::Value>(&response)
                .ok()
                .and_then(|value| value["deployment_status"].as_str().map(str::to_string));
            Ok(status.as_deref() == Some("Running"))
        })?;
        ensure!(ready, "Feldera pipeline did not reach Running");
        Ok(())
    }

    fn poll_feldera_completion(&self, pipeline: &str) -> Result<()> {
        match self.config.feldera_completion_mode.as_str() {
            "count" => self.poll_feldera_count_query(pipeline),
            "completed_records" => self.poll_feldera_completed_records(pipeline),
            other => bail!("unsupported FELDERA_COMPLETION_MODE '{other}'"),
        }
    }

    fn poll_feldera_count_query(&self, pipeline: &str) -> Result<()> {
        let ready = wait_until(self.config.poll_timeout, self.config.poll_interval, || {
            let response = run_capture(
                "curl",
                [
                    "-fsS",
                    "--get",
                    &format!(
                        "http://127.0.0.1:{}/v0/pipelines/{pipeline}/query",
                        self.config.feldera_http_port
                    ),
                    "--data-urlencode",
                    &format!("sql=SELECT ROW_COUNT FROM {QUERY_COUNT_RELATION}"),
                    "--data-urlencode",
                    "format=json",
                ],
                Some(&self.config.repo_root),
            )
            .unwrap_or_default();
            let count = serde_json::from_str::<serde_json::Value>(&response)
                .ok()
                .and_then(|value| value.as_array().and_then(|rows| rows.first().cloned()))
                .and_then(|row| {
                    row.get("ROW_COUNT")
                        .or_else(|| row.get("row_count"))
                        .and_then(serde_json::Value::as_u64)
                })
                .unwrap_or(0);
            Ok(count >= self.config.expected_rows)
        })?;
        ensure!(
            ready,
            "Feldera count view did not reach {}",
            self.config.expected_rows
        );
        Ok(())
    }

    fn poll_feldera_completed_records(&self, pipeline: &str) -> Result<()> {
        let ready = wait_until(self.config.poll_timeout, self.config.poll_interval, || {
            let response = run_capture(
                "curl",
                [
                    "-fsS",
                    &format!(
                        "http://127.0.0.1:{}/v0/pipelines/{pipeline}/stats",
                        self.config.feldera_http_port
                    ),
                ],
                Some(&self.config.repo_root),
            )
            .unwrap_or_default();
            let metrics = serde_json::from_str::<serde_json::Value>(&response).unwrap_or_default();
            let input = metrics["global_metrics"]["total_input_records"]
                .as_u64()
                .unwrap_or(0);
            let completed = metrics["global_metrics"]["total_completed_records"]
                .as_u64()
                .unwrap_or(0);
            Ok(input >= self.config.input_rows_total && completed >= self.config.input_rows_total)
        })?;
        ensure!(
            ready,
            "Feldera pipeline did not complete {} rows",
            self.config.input_rows_total
        );
        Ok(())
    }

    fn write_floe_config(
        &self,
        config_path: &Path,
        bid_topic: &str,
        auction_topic: &str,
    ) -> Result<String> {
        let mut connectors = vec![json!({
            "type": "kafka",
            "brokers": self.config.broker_addr,
            "topics": [bid_topic],
            "group_id": format!("{}_{}_bids", self.config.floe_kafka_group_id_prefix, self.config.run_id),
            "default_source": "nexmark_bid",
            "poll_ms": self.config.floe_kafka_poll_ms,
            "max_messages_per_tick": self.config.floe_kafka_max_messages_per_tick
        })];
        if self.config.bench_query == BenchQuery::Join {
            connectors.push(json!({
                "type": "kafka",
                "brokers": self.config.broker_addr,
                "topics": [auction_topic],
                "group_id": format!("{}_{}_auctions", self.config.floe_kafka_group_id_prefix, self.config.run_id),
                "default_source": "nexmark_auction",
                "poll_ms": self.config.floe_kafka_poll_ms,
                "max_messages_per_tick": self.config.floe_kafka_max_messages_per_tick
            }));
        }
        write_file(
            config_path,
            serde_json::to_vec_pretty(&json!({
                "connectors": connectors,
                "runtime": {
                    "ingest_queue_capacity": self.config.floe_ingest_queue_capacity,
                    "ingest_batch_size": self.config.floe_ingest_batch_size,
                    "ingest_batch_per_source": self.config.floe_ingest_batch_per_source,
                    "ingest_batch_per_connector": self.config.floe_ingest_batch_per_connector,
                    "mv_retain_last": self.config.floe_mv_retain_last
                },
                "storage": {"await_durable": false}
            }))?,
        )?;
        Ok(match self.config.bench_query {
            BenchQuery::FilterProjection => format!(
                "CREATE MATERIALIZED VIEW {QUERY_RESULT_RELATION} AS SELECT auction, bidder, price AS projected_price FROM nexmark_bid WHERE auction <= 5000;"
            ),
            BenchQuery::Join => format!(
                "CREATE MATERIALIZED VIEW {QUERY_RESULT_RELATION} AS SELECT b.auction, b.bidder, b.price AS projected_price, a.seller FROM nexmark_bid AS b JOIN nexmark_auction AS a ON b.auction = a.id WHERE a.category = 10;"
            ),
        })
    }

    fn docker_rm_force(&self, names: &[&str]) {
        for name in names {
            let _ = Command::new("docker").args(["rm", "-f", name]).status();
        }
    }

    fn stop_floe_process(&mut self) {
        if let Some(mut child) = self.floe_child.take() {
            terminate_child_process_group(&mut child, Duration::from_secs(10));
        }
    }

    fn cleanup(&mut self) {
        self.stop_floe_process();
        if self.config.keep_containers {
            return;
        }
        self.docker_rm_force(&[
            &self.config.materialize_container,
            &self.config.risingwave_container,
            &self.config.feldera_container,
            &self.config.redpanda_container,
        ]);
        let _ = Command::new("docker")
            .args(["network", "rm", &self.config.network_name])
            .status();
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.cleanup();
    }
}

fn rows_per_second(rows: u64, ms: u128) -> u64 {
    if ms == 0 {
        rows
    } else {
        (u128::from(rows) * 1000 / ms) as u64
    }
}

fn log(message: impl AsRef<str>) {
    println!("[stream-engine-compare] {}", message.as_ref());
}

fn print_usage() {
    println!(
        "Usage: stream_engine_compare [floe|materialize|risingwave|feldera|all] [filter_projection|join]"
    );
}
