use super::*;

impl Harness {
    pub(super) fn produce_for_sources(&self, sources: &[Source], topics: &Topics) -> Result<u128> {
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

    pub(super) fn produce_topic(&self, topic: &str, dataset: &str, rows: u64) -> Result<u128> {
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

    pub(super) fn start_materialize(&self) -> Result<()> {
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

    pub(super) fn start_risingwave(&self) -> Result<()> {
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

    pub(super) fn start_feldera(&self) -> Result<()> {
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
        let deadline = Instant::now() + Duration::from_secs(90);
        while Instant::now() < deadline {
            if command_success("curl", ["-fsS", &url], None)? {
                return Ok(());
            }
            wait_before_retry(deadline, Duration::from_secs(1));
        }
        bail!("Feldera HTTP API did not become ready")
    }

    pub(super) fn run_materialize_query(
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

    pub(super) fn run_risingwave_query(
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

    pub(super) fn run_pg_timed_query(&self, spec: PgTimedQuery<'_>) -> Result<()> {
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
                &expected_query_text,
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

    pub(super) fn run_feldera_query(
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
                &expected_query_text,
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
}
