use super::*;

impl Harness {
    pub(super) fn new(config: Config) -> Result<Self> {
        fs::create_dir_all(&config.run_dir)
            .with_context(|| format!("create run dir {}", config.run_dir.display()))?;
        Ok(Self {
            config,
            floe_child: None,
        })
    }

    pub(super) fn run(&mut self) -> Result<()> {
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

    pub(super) fn validate_correctness_input_shape(&self) -> Result<()> {
        if self.config.strict_result_correctness && self.config.auction_rows > 10_000 {
            bail!(
                "STRICT_RESULT_CORRECTNESS requires AUCTION_ROWS <= 10000 (current: {})",
                self.config.auction_rows
            );
        }
        Ok(())
    }

    pub(super) fn write_summary_header(&self) -> Result<()> {
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

    pub(super) fn ensure_command(&self, command: &str) -> Result<()> {
        let status = Command::new("sh")
            .arg("-c")
            .arg(format!("command -v {command} >/dev/null 2>&1"))
            .status()
            .with_context(|| format!("check command {command}"))?;
        ensure!(status.success(), "{command} is required");
        Ok(())
    }

    pub(super) fn ensure_network(&self) -> Result<()> {
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

    pub(super) fn ensure_redpanda(&self) -> Result<()> {
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

        let deadline = Instant::now() + Duration::from_secs(90);
        while Instant::now() < deadline {
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
            wait_before_retry(deadline, Duration::from_secs(1));
        }

        let logs = run_capture("docker", ["logs", &self.config.redpanda_container], None)
            .unwrap_or_default();
        eprintln!("{logs}");
        bail!("Redpanda did not become ready")
    }

    pub(super) fn container_running(&self, name: &str) -> Result<bool> {
        let out = run_capture("docker", ["ps", "--format", "{{.Names}}"], None)?;
        Ok(out.lines().any(|line| line == name))
    }

    pub(super) fn build_producer(&self) -> Result<()> {
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

    pub(super) fn build_floe_node(&self) -> Result<()> {
        log("building floe-node release binary");
        run_status(
            "cargo",
            ["build", "-p", "floe-node", "--release"],
            Some(&self.config.repo_root),
        )
        .context("build floe-node")
    }

    pub(super) fn capture_run_context(&self) -> Result<()> {
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

    pub(super) fn run_engine_suite(&mut self, engine: Engine) -> Result<()> {
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

    pub(super) fn record_start_failures(&self, engine: Engine, notes: &str) -> Result<()> {
        for query_id in &self.config.queries {
            let sources = required_sources_for_query(query_id);
            let input_rows = self.config.input_rows_total(&sources);
            self.record_failure(engine, query_id, notes, input_rows)?;
        }
        Ok(())
    }

    pub(super) fn producer_topics_for_query(&self, engine: Engine, query_id: &str) -> Topics {
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

    pub(super) fn reset_topic(&self, topic: &str) -> Result<()> {
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
}
