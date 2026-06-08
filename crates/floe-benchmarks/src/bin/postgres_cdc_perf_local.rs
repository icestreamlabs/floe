use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};

#[path = "postgres_cdc_perf_local/config.rs"]
mod config;
#[path = "postgres_cdc_perf_local/datasets.rs"]
mod datasets;
#[path = "harness_common/mod.rs"]
mod harness_common;
#[path = "postgres_cdc_perf_local/report.rs"]
mod report;

use config::{BenchMode, Config, Dataset, TargetKind};
use datasets::*;
use harness_common::*;
use report::*;

fn main() -> Result<()> {
    let config = Config::from_env()?;
    let plan = dataset_plan(&config)?;
    let artifacts = ArtifactPaths::new(&config);
    let mut harness = Harness::new(config, plan, artifacts)?;
    harness.run()
}

struct Harness {
    config: Config,
    plan: DatasetPlan,
    artifacts: ArtifactPaths,
    node_child: Option<Child>,
}

impl Harness {
    fn new(config: Config, plan: DatasetPlan, artifacts: ArtifactPaths) -> Result<Self> {
        fs::create_dir_all(&config.artifact_dir)
            .with_context(|| format!("create {}", config.artifact_dir.display()))?;
        Ok(Self {
            config,
            plan,
            artifacts,
            node_child: None,
        })
    }

    fn run(&mut self) -> Result<()> {
        validate_dataset_mode(&self.config)?;
        self.require_commands()?;
        write_file(
            &self.config.config_path,
            serde_json::to_vec_pretty(&self.config.floe_config_json())?,
        )?;
        write_reproduce_command(&self.config, &self.artifacts)?;
        self.cleanup_containers();
        self.print_scenario();
        self.pull_images()?;
        self.start_postgres()?;
        self.wait_for_postgres()?;
        self.write_system_context();
        if self.config.target == TargetKind::Kafka {
            self.start_redpanda()?;
            self.create_kafka_topics()?;
            self.write_kafka_topic_info();
        } else {
            write_file(&self.artifacts.kafka_topic_log, "")?;
        }

        let load_plan = LoadPlan::for_config(&self.config);
        let load_started = Instant::now();
        let mut table_row_counts = self.load_dataset(&load_plan)?;
        let postgres_load_seconds = load_started.elapsed().as_secs_f64();
        self.write_postgres_settings();
        self.create_postgres_sink_tables()?;
        let source_rows = load_plan.source_rows();
        self.build_binaries()?;
        write_file(
            &self.config.sql_path,
            replication_sql(&self.config, &self.plan),
        )?;

        let (expected_kafka_messages, expected_sink_rows, expected_updated_rows) =
            self.expected_observation_counts(&load_plan, &table_row_counts)?;
        let node_started = Instant::now();
        self.start_floe_node()?;
        let counter_started = expected_kafka_messages.map(|_| Instant::now());
        let mut counter = self.start_counter(expected_kafka_messages)?;
        let sink_wait_started = Instant::now();
        let live_write_seconds = self.write_live_changes(&load_plan)?;

        let mut sink_wait_seconds = None;
        let mut observed_sink_rows = None;
        let mut observed_updated_rows = None;
        if self.config.target == TargetKind::Kafka {
            if let Some(counter) = counter.as_mut() {
                let status = counter.wait().context("wait for Kafka counter")?;
                if !status.success() {
                    print_tail(&self.artifacts.counter_log, 120);
                    bail!("Kafka counter failed with {status}");
                }
            }
        } else {
            let expected_rows = expected_sink_rows.context("postgres target expected rows")?;
            let (observed, updated) =
                self.wait_for_postgres_sink(expected_rows, expected_updated_rows)?;
            sink_wait_seconds = Some(sink_wait_started.elapsed().as_secs_f64());
            observed_sink_rows = Some(observed);
            observed_updated_rows = Some(updated);
        }
        let end_to_end_seconds = node_started.elapsed().as_secs_f64();
        self.capture_floe_observability();
        self.write_postgres_slot_info();
        self.write_docker_stats();
        self.stop_node();

        if self.config.dataset == Dataset::TpchTop2
            && self.config.bench_mode == BenchMode::LiveInsert
        {
            table_row_counts = vec![0, 0];
        }
        let counter_metrics = CounterMetrics::from_file(&self.artifacts.counter_log);
        let observed_kafka_messages = counter_metrics
            .get("cdc_counter.observed_messages")
            .map(str::to_string);
        let counter_seconds = counter_started.map(|started| started.elapsed().as_secs_f64());
        let summary = RunSummary {
            initial_rows: load_plan.initial_rows,
            live_insert_rows: load_plan.live_insert_rows,
            live_update_rows: load_plan.live_update_rows,
            source_rows,
            table_row_counts,
            expected_kafka_messages,
            observed_kafka_messages,
            expected_sink_rows,
            observed_sink_rows,
            expected_postgres_updated_rows: expected_updated_rows,
            observed_postgres_updated_rows: observed_updated_rows,
            postgres_load_seconds,
            live_write_seconds,
            end_to_end_seconds,
            sink_wait_seconds,
            counter_seconds,
            counter_metrics,
            artifact_paths: self.artifacts.clone(),
        };
        write_summary_env(&self.config, &self.plan, &summary)?;
        write_summary_files(&self.config, &self.plan, &load_plan, &summary)?;
        println!("CDC benchmark complete.");
        println!("summary_json={}", self.config.summary_json.display());
        println!("summary_md={}", self.config.summary_md.display());
        Ok(())
    }

    fn require_commands(&self) -> Result<()> {
        for command_name in ["docker", "cargo", "curl"] {
            ensure!(
                command_success("which", [command_name], Some(&self.config.repo_root))?,
                "missing required command: {command_name}"
            );
        }
        Ok(())
    }

    fn print_scenario(&self) {
        for (key, value) in [
            (
                "artifact_dir",
                self.config.artifact_dir.display().to_string(),
            ),
            ("rows", self.config.rows.to_string()),
            ("dataset", self.config.dataset.as_str().to_string()),
            ("tpch_scale_factor", self.config.tpch_scale_factor.clone()),
            ("bench_mode", self.config.bench_mode.as_str().to_string()),
            ("target", self.config.target.as_str().to_string()),
            ("brokers", self.config.brokers.clone()),
            ("topics", self.plan.topic_list()),
            ("postgres_sink_tables", self.plan.target_table_list()),
            ("pipeline_format", self.config.pipeline_format.clone()),
            (
                "durable_replication_buffer",
                self.config.durable_replication_buffer.to_string(),
            ),
        ] {
            println!("{key}={value}");
        }
    }

    fn pull_images(&self) -> Result<()> {
        println!("Pulling images...");
        run_status(
            "docker",
            ["pull", &self.config.postgres_image],
            Some(&self.config.repo_root),
        )?;
        if self.config.target == TargetKind::Kafka {
            run_status(
                "docker",
                ["pull", &self.config.redpanda_image],
                Some(&self.config.repo_root),
            )?;
        }
        Ok(())
    }

    fn start_postgres(&self) -> Result<()> {
        println!(
            "Starting Postgres {} on port {}",
            self.config.postgres_image, self.config.postgres_port
        );
        run_status(
            "docker",
            [
                "run",
                "-d",
                "--name",
                &self.config.postgres_container,
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
                "max_replication_slots=16",
                "-c",
                "max_wal_senders=16",
                "-c",
                "max_slot_wal_keep_size=4096MB",
            ],
            Some(&self.config.repo_root),
        )
    }

    fn wait_for_postgres(&self) -> Result<()> {
        let ready = wait_until(Duration::from_secs(90), Duration::from_secs(1), || {
            command_success(
                "docker",
                [
                    "exec",
                    &self.config.postgres_container,
                    "pg_isready",
                    "-U",
                    &self.config.postgres_user,
                    "-d",
                    &self.config.postgres_db,
                ],
                Some(&self.config.repo_root),
            )
        })?;
        if ready {
            Ok(())
        } else {
            let _ = Command::new("docker")
                .args(["logs", &self.config.postgres_container])
                .status();
            bail!("Postgres did not become ready in time")
        }
    }

    fn start_redpanda(&self) -> Result<()> {
        println!(
            "Starting Redpanda {} on port {}",
            self.config.redpanda_image, self.config.redpanda_port
        );
        run_status(
            "docker",
            [
                "run",
                "-d",
                "--name",
                &self.config.redpanda_container,
                "-p",
                &format!("{}:9092", self.config.redpanda_port),
                &self.config.redpanda_image,
                "redpanda",
                "start",
                "--overprovisioned",
                "--smp",
                "1",
                "--memory",
                "2G",
                "--reserve-memory",
                "0M",
                "--node-id",
                "0",
                "--check=false",
                "--set",
                &format!(
                    "redpanda.kafka_batch_max_bytes={}",
                    self.config.redpanda_kafka_batch_max_bytes
                ),
                "--kafka-addr",
                "PLAINTEXT://0.0.0.0:9092",
                "--advertise-kafka-addr",
                &format!("PLAINTEXT://127.0.0.1:{}", self.config.redpanda_port),
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
        ensure!(ready, "Redpanda did not become ready in time");
        Ok(())
    }

    fn create_kafka_topics(&self) -> Result<()> {
        println!("Creating Kafka topics {}", self.plan.topic_list());
        for topic in &self.plan.topics {
            let create = Command::new("docker")
                .args([
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
                    "-c",
                    &format!(
                        "max.message.bytes={}",
                        self.config.redpanda_topic_max_message_bytes
                    ),
                ])
                .status();
            if !create.is_ok_and(|status| status.success()) {
                let _ = Command::new("docker")
                    .args([
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
                    ])
                    .status();
            }
            let _ = Command::new("docker")
                .args([
                    "exec",
                    &self.config.redpanda_container,
                    "rpk",
                    "topic",
                    "alter-config",
                    topic,
                    "--set",
                    &format!(
                        "max.message.bytes={}",
                        self.config.redpanda_topic_max_message_bytes
                    ),
                ])
                .status();
        }
        Ok(())
    }

    fn load_dataset(&self, load: &LoadPlan) -> Result<Vec<u64>> {
        println!(
            "Loading Postgres dataset {} with {} requested initial rows",
            self.config.dataset.as_str(),
            load.initial_rows
        );
        match self.config.dataset {
            Dataset::SyntheticOrders => {
                self.docker_psql(&synthetic_orders_sql(
                    load.initial_rows,
                    &self.config.publication,
                ))?;
                Ok(vec![load.initial_rows])
            }
            Dataset::TpchLineitemFlat => {
                self.run_tpchgen(&["--tables", "lineitem"])?;
                self.docker_psql(&lineitem_flat_stage_sql(&self.config.publication))?;
                self.copy_pipe_delimited_file(
                    "public.lineitem_flat_stage",
                    &self.config.tpch_data_dir.join("lineitem.tbl"),
                )?;
                self.docker_psql(lineitem_flat_finish_sql())?;
                Ok(vec![self.count_table("public.lineitem_flat")?])
            }
            Dataset::TpchLineitem => {
                self.run_tpchgen(&["--tables", "lineitem"])?;
                self.docker_psql(&lineitem_schema_sql(&self.config.publication))?;
                self.copy_pipe_delimited_file(
                    "public.lineitem",
                    &self.config.tpch_data_dir.join("lineitem.tbl"),
                )?;
                Ok(vec![self.count_table("public.lineitem")?])
            }
            Dataset::TpchTop2 => {
                self.docker_psql(&tpch_top2_schema_sql(&self.config.publication))?;
                if self.config.bench_mode == BenchMode::LiveInsert {
                    return Ok(vec![0, 0]);
                }
                self.run_tpchgen(&["--tables", "orders,lineitem"])?;
                self.copy_pipe_delimited_file(
                    "public.orders",
                    &self.config.tpch_data_dir.join("orders.tbl"),
                )?;
                self.copy_pipe_delimited_file(
                    "public.lineitem",
                    &self.config.tpch_data_dir.join("lineitem.tbl"),
                )?;
                Ok(vec![
                    self.count_table("public.orders")?,
                    self.count_table("public.lineitem")?,
                ])
            }
            Dataset::TpchAll => {
                self.run_tpchgen(&[])?;
                self.docker_psql(&tpch_all_schema_sql(&self.config.publication))?;
                for table in [
                    "region", "nation", "supplier", "customer", "part", "partsupp", "orders",
                    "lineitem",
                ] {
                    self.copy_pipe_delimited_file(
                        &format!("public.{table}"),
                        &self.config.tpch_data_dir.join(format!("{table}.tbl")),
                    )?;
                }
                self.plan
                    .upstream_tables
                    .iter()
                    .map(|table| self.count_table(table))
                    .collect()
            }
        }
    }

    fn run_tpchgen(&self, extra_args: &[&str]) -> Result<()> {
        ensure!(
            command_success(
                "which",
                [&self.config.tpchgen_bin],
                Some(&self.config.repo_root)
            )?,
            "missing required command: {}",
            self.config.tpchgen_bin
        );
        fs::create_dir_all(&self.config.tpch_data_dir)?;
        for entry in fs::read_dir(&self.config.tpch_data_dir)? {
            let entry = entry?;
            if entry.path().extension().is_some_and(|ext| ext == "tbl") {
                fs::remove_file(entry.path())?;
            }
        }
        let mut args = vec![
            "--scale-factor".to_string(),
            self.config.tpch_scale_factor.clone(),
            "--format".to_string(),
            "tbl".to_string(),
            "--output-dir".to_string(),
            self.config.tpch_data_dir.display().to_string(),
        ];
        args.extend(extra_args.iter().map(|arg| arg.to_string()));
        run_status(
            &self.config.tpchgen_bin,
            &args,
            Some(&self.config.repo_root),
        )
    }

    fn copy_pipe_delimited_file(&self, table: &str, path: &Path) -> Result<()> {
        let mut child = Command::new("docker")
            .args([
                "exec",
                "-i",
                &self.config.postgres_container,
                "psql",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                &self.config.postgres_user,
                "-d",
                &self.config.postgres_db,
                "-c",
                &format!(
                    "\\copy {table} FROM STDIN WITH (FORMAT csv, DELIMITER '|', QUOTE E'\\b', ESCAPE E'\\b')"
                ),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .context("spawn psql copy")?;
        let mut stdin = child.stdin.take().context("open psql copy stdin")?;
        for line in BufReader::new(File::open(path)?).lines() {
            let line = line?;
            writeln!(stdin, "{}", line.strip_suffix('|').unwrap_or(&line))?;
        }
        drop(stdin);
        ensure_status(child.wait().context("wait for psql copy")?)
    }

    fn create_postgres_sink_tables(&self) -> Result<()> {
        if self.config.target != TargetKind::Postgres {
            return Ok(());
        }
        for (upstream, target) in self
            .plan
            .upstream_tables
            .iter()
            .zip(&self.plan.target_tables)
        {
            self.docker_psql(&create_postgres_sink_table_sql(upstream, target))?;
        }
        Ok(())
    }

    fn build_binaries(&self) -> Result<()> {
        println!("Building {} binaries", self.config.profile());
        let mut args = vec![
            "build",
            "-p",
            "floe-node",
            "-p",
            "floe-benchmarks",
            "--bins",
        ];
        if self.config.build_release {
            args.insert(1, "--release");
        }
        run_status("cargo", args, Some(&self.config.repo_root))
    }

    fn expected_observation_counts(
        &self,
        load: &LoadPlan,
        table_row_counts: &[u64],
    ) -> Result<(Option<u64>, Option<u64>, u64)> {
        if self.config.target == TargetKind::Postgres {
            let expected_updated =
                if self.config.dataset == Dataset::SyntheticOrders && load.live_update_rows > 0 {
                    load.live_update_rows
                } else {
                    0
                };
            return Ok((
                None,
                Some(load.initial_rows + load.live_insert_rows),
                expected_updated,
            ));
        }
        let mut expected = 0;
        for row_count in table_row_counts {
            if *row_count > 0 {
                expected += expected_insert_messages(*row_count, &self.config)?;
            }
        }
        if load.live_insert_rows > 0 {
            expected += if self.config.dataset == Dataset::TpchTop2 {
                expected_tpch_top2_live_insert_messages(
                    load.live_insert_rows,
                    self.config.live_write_chunk_rows,
                    &self.config,
                )?
            } else {
                expected_messages_for_chunks(
                    load.live_insert_rows,
                    self.config.live_write_chunk_rows,
                    &self.config,
                    false,
                )?
            };
        }
        if load.live_update_rows > 0 {
            expected += expected_messages_for_chunks(
                load.live_update_rows,
                self.config.live_write_chunk_rows,
                &self.config,
                true,
            )?;
        }
        println!("expected_kafka_messages={expected}");
        Ok((Some(expected), None, 0))
    }

    fn start_floe_node(&mut self) -> Result<()> {
        println!("Starting Floe node");
        let stdout = File::create(&self.artifacts.node_stdout)?;
        let stderr = File::create(&self.artifacts.node_stderr)?;
        let sql = fs::read_to_string(&self.config.sql_path)?;
        let config_path = self
            .config
            .config_path
            .to_str()
            .context("config path not UTF-8")?;
        let flush_interval_ms = self.config.slatedb_flush_interval_ms.to_string();
        let mut command = if Path::new("/usr/bin/time").exists() {
            let mut command = Command::new("/usr/bin/time");
            command
                .args(["-v", "-o"])
                .arg(&self.artifacts.node_resource_log)
                .arg(self.config.target_binary("floe-node"));
            command
        } else {
            write_file(
                &self.artifacts.node_resource_log,
                "resource collection unavailable: /usr/bin/time not found\n",
            )?;
            Command::new(self.config.target_binary("floe-node"))
        };
        configure_process_group(&mut command);
        let child = command
            .args([
                "run",
                "--config",
                config_path,
                "--mv-query",
                &sql,
                "--slatedb-await-durable=false",
                "--slatedb-flush-interval-ms",
                &flush_interval_ms,
                "--ingest-batch-size",
                "16384",
                "--ingest-batch-per-source",
                "16384",
                "--ingest-batch-per-connector",
                "16384",
            ])
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .context("spawn floe-node")?;
        self.node_child = Some(child);
        Ok(())
    }

    fn start_counter(&self, expected: Option<u64>) -> Result<Option<Child>> {
        let Some(expected) = expected else {
            write_file(&self.artifacts.counter_log, "")?;
            return Ok(None);
        };
        println!("Counting CDC records from Kafka");
        let stdout = File::create(&self.artifacts.counter_log)?;
        let stderr = stdout.try_clone()?;
        let child = Command::new(self.config.target_binary("postgres_cdc_kafka_counter"))
            .args([
                "--brokers",
                &self.config.brokers,
                "--topics",
                &self.plan.topic_list(),
                "--expected",
                &expected.to_string(),
                "--timeout-secs",
                &self.config.timeout_secs.to_string(),
            ])
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .context("spawn Kafka counter")?;
        Ok(Some(child))
    }

    fn write_live_changes(&self, load: &LoadPlan) -> Result<f64> {
        let started = Instant::now();
        match self.config.bench_mode {
            BenchMode::LiveInsert => {
                self.wait_for_postgres_slot_active()?;
                if self.config.dataset == Dataset::TpchTop2 {
                    self.write_live_tpch_top2_inserts(load.live_insert_rows)?;
                } else {
                    self.write_live_inserts(load.live_insert_rows)?;
                }
            }
            BenchMode::SnapshotLiveUpdate => {
                self.wait_for_postgres_slot_active()?;
                self.write_live_updates(load.live_update_rows)?;
            }
            BenchMode::Snapshot => {}
        }
        Ok(started.elapsed().as_secs_f64())
    }

    fn write_live_inserts(&self, total: u64) -> Result<()> {
        self.write_chunked(total, synthetic_live_insert_sql)
    }

    fn write_live_updates(&self, total: u64) -> Result<()> {
        self.write_chunked(total, synthetic_live_update_sql)
    }

    fn write_chunked<F>(&self, total: u64, mut sql_for_range: F) -> Result<()>
    where
        F: FnMut(u64, u64) -> String,
    {
        let chunk = if self.config.live_write_chunk_rows == 0
            || self.config.live_write_chunk_rows > total
        {
            total
        } else {
            self.config.live_write_chunk_rows
        };
        let mut start = 1;
        while start <= total {
            let end = (start + chunk - 1).min(total);
            self.docker_psql(&sql_for_range(start, end))?;
            start = end + 1;
            self.sleep_live_write_pause();
        }
        Ok(())
    }

    fn write_live_tpch_top2_inserts(&self, total: u64) -> Result<()> {
        let chunk = if self.config.live_write_chunk_rows == 0
            || self.config.live_write_chunk_rows > total
        {
            total
        } else {
            self.config.live_write_chunk_rows
        };
        let mut remaining = total;
        let mut next_order_key = 1;
        let mut next_lineitem_idx = 1;
        while remaining > 0 {
            let chunk_rows = chunk.min(remaining);
            let order_rows = tpch_top2_chunk_orders(chunk_rows);
            let lineitem_rows = chunk_rows - order_rows;
            let order_start = next_order_key;
            let order_end = order_start + order_rows - 1;
            let lineitem_start = next_lineitem_idx;
            let lineitem_end = lineitem_start + lineitem_rows.saturating_sub(1);
            self.docker_psql(&tpch_top2_live_insert_sql(
                order_start,
                order_end,
                lineitem_start,
                lineitem_end,
            ))?;
            next_order_key = order_end + 1;
            next_lineitem_idx = lineitem_end + 1;
            remaining -= chunk_rows;
            self.sleep_live_write_pause();
        }
        Ok(())
    }

    fn sleep_live_write_pause(&self) {
        if self.config.live_write_sleep_ms > 0 {
            std::thread::park_timeout(Duration::from_millis(self.config.live_write_sleep_ms));
        }
    }

    fn wait_for_postgres_slot_active(&self) -> Result<()> {
        let ready = wait_until(Duration::from_secs(120), Duration::from_secs(1), || {
            let active = self.docker_psql_capture(&format!(
                "SELECT COALESCE((SELECT active FROM pg_replication_slots WHERE slot_name = '{}'), false)",
                self.config.slot
            ))?;
            Ok(active.trim() == "t")
        })?;
        ensure!(
            ready,
            "Postgres CDC replication slot {} did not become active in time",
            self.config.slot
        );
        Ok(())
    }

    fn wait_for_postgres_sink(
        &self,
        expected_rows: u64,
        expected_updated: u64,
    ) -> Result<(u64, u64)> {
        println!(
            "Waiting for Postgres sink tables {}",
            self.plan.target_table_list()
        );
        let deadline = Instant::now() + Duration::from_secs(self.config.timeout_secs);
        let mut total_rows = 0;
        let mut updated_rows = 0;
        while Instant::now() < deadline {
            total_rows = self.postgres_sink_total_rows()?;
            if expected_updated > 0 {
                updated_rows = self.postgres_sink_updated_rows()?;
            }
            if total_rows >= expected_rows && updated_rows >= expected_updated {
                return Ok((total_rows, updated_rows));
            }
            std::thread::park_timeout(Duration::from_millis(200));
        }
        print_tail(&self.artifacts.node_stderr, 80);
        bail!(
            "Postgres sink observed {total_rows} rows and {updated_rows} updated rows; expected {expected_rows} rows and {expected_updated} updated rows"
        )
    }

    fn postgres_sink_total_rows(&self) -> Result<u64> {
        let mut total = 0;
        for table in &self.plan.target_tables {
            total += self.count_table(table)?;
        }
        Ok(total)
    }

    fn postgres_sink_updated_rows(&self) -> Result<u64> {
        if self.config.dataset != Dataset::SyntheticOrders {
            return Ok(0);
        }
        self.docker_psql_capture("SELECT COUNT(*) FROM public.orders_sink WHERE status = 'updated'")
            .and_then(|value| parse_u64(value.trim()))
    }

    fn count_table(&self, table: &str) -> Result<u64> {
        self.docker_psql_capture(&format!("SELECT COUNT(*) FROM {table}"))
            .and_then(|value| parse_u64(value.trim()))
    }

    fn docker_psql(&self, sql: &str) -> Result<String> {
        self.docker_psql_args(sql, false)
    }

    fn docker_psql_capture(&self, sql: &str) -> Result<String> {
        self.docker_psql_args(sql, true)
    }

    fn docker_psql_args(&self, sql: &str, capture: bool) -> Result<String> {
        let args = [
            "exec",
            "-i",
            &self.config.postgres_container,
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            &self.config.postgres_user,
            "-d",
            &self.config.postgres_db,
            "-Atqc",
            sql,
        ];
        if capture {
            run_capture("docker", args, Some(&self.config.repo_root))
        } else {
            run_capture("docker", args, Some(&self.config.repo_root)).map(|_| String::new())
        }
    }

    fn write_system_context(&self) {
        let mut content = String::new();
        content.push_str(&format!(
            "benchmark.timestamp={}\n",
            chrono::Utc::now().to_rfc3339()
        ));
        content.push_str(&format!(
            "benchmark.git_commit={}\n",
            run_capture("git", ["rev-parse", "HEAD"], Some(&self.config.repo_root))
                .unwrap_or_default()
        ));
        for command in [
            ("cargo", vec!["--version"]),
            ("rustc", vec!["--version"]),
            ("docker", vec!["version"]),
        ] {
            content.push_str(
                &run_capture(command.0, command.1, Some(&self.config.repo_root))
                    .unwrap_or_default(),
            );
            content.push('\n');
        }
        let _ = write_file(&self.artifacts.system_log, content);
    }

    fn write_postgres_settings(&self) {
        let sql = "SELECT name, setting, unit FROM pg_settings WHERE name IN ('wal_level','max_replication_slots','max_wal_senders','max_slot_wal_keep_size','shared_buffers','work_mem','maintenance_work_mem','effective_cache_size','synchronous_commit') ORDER BY name;";
        let content = self.docker_psql_capture(sql).unwrap_or_default();
        let _ = write_file(&self.artifacts.postgres_settings_log, content);
    }

    fn write_kafka_topic_info(&self) {
        let mut content = String::new();
        for topic in &self.plan.topics {
            content.push_str(&format!("topic={topic}\n"));
            content.push_str(
                &run_capture(
                    "docker",
                    [
                        "exec",
                        &self.config.redpanda_container,
                        "rpk",
                        "topic",
                        "describe",
                        topic,
                    ],
                    Some(&self.config.repo_root),
                )
                .unwrap_or_default(),
            );
            content.push('\n');
        }
        let _ = write_file(&self.artifacts.kafka_topic_log, content);
    }

    fn write_postgres_slot_info(&self) {
        let sql = format!(
            "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, pg_current_wal_lsn() AS current_wal_lsn FROM pg_replication_slots WHERE slot_name = '{}';",
            self.config.slot
        );
        let _ = write_file(
            &self.artifacts.postgres_slot_log,
            self.docker_psql_capture(&sql).unwrap_or_default(),
        );
    }

    fn write_docker_stats(&self) {
        let mut containers = vec![self.config.postgres_container.clone()];
        if self.config.target == TargetKind::Kafka {
            containers.push(self.config.redpanda_container.clone());
        }
        let mut args = vec![
            "stats".to_string(),
            "--no-stream".to_string(),
            "--format".to_string(),
            "container={{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}} net={{.NetIO}} block={{.BlockIO}} pids={{.PIDs}}".to_string(),
        ];
        args.extend(containers);
        let _ = write_file(
            &self.artifacts.docker_stats_log,
            run_capture("docker", &args, Some(&self.config.repo_root)).unwrap_or_default(),
        );
    }

    fn capture_floe_observability(&self) {
        let metrics = run_capture(
            "curl",
            [
                "-fsS",
                "--max-time",
                "5",
                &format!("http://127.0.0.1:{}/metrics", self.config.floe_admin_port),
            ],
            Some(&self.config.repo_root),
        )
        .unwrap_or_default();
        let _ = write_file(&self.artifacts.floe_metrics_log, metrics);
        let debug = run_capture(
            "curl",
            [
                "-fsS",
                "--max-time",
                "5",
                &format!(
                    "http://127.0.0.1:{}/debug/cdc/replication",
                    self.config.floe_admin_port
                ),
            ],
            Some(&self.config.repo_root),
        )
        .unwrap_or_else(|_| "{}".to_string());
        let pretty = serde_json::from_str::<serde_json::Value>(&debug)
            .ok()
            .and_then(|value| serde_json::to_string_pretty(&value).ok())
            .unwrap_or_else(|| "{}".to_string());
        let _ = write_file(&self.artifacts.cdc_replication_debug_json, pretty);
    }

    fn stop_node(&mut self) {
        if let Some(mut child) = self.node_child.take() {
            terminate_child_process_group(&mut child, Duration::from_secs(10));
        }
    }

    fn cleanup_containers(&self) {
        let _ = Command::new("docker")
            .args([
                "rm",
                "-f",
                &self.config.postgres_container,
                &self.config.redpanda_container,
            ])
            .status();
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.stop_node();
        if !self.config.keep_containers {
            self.cleanup_containers();
        }
    }
}

fn parse_u64(value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .with_context(|| format!("parse integer '{value}'"))
}
