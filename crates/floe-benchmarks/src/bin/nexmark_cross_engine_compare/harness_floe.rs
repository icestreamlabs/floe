use super::*;

impl Harness {
    pub(super) fn run_floe_query(
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

    pub(super) fn start_floe_node(
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

    pub(super) fn wait_for_floe_pg(&mut self, artifact_dir: &Path) -> Result<()> {
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

    pub(super) fn verify_floe_storage_mode_if_requested(&self, artifact_dir: &Path) -> Result<()> {
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

    pub(super) fn settle_floe_state_if_requested(&self, artifact_dir: &Path) -> Result<()> {
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

    pub(super) fn poll_pg_relation_max_mv_version_stable(
        &self,
        target: PgTarget<'_>,
        relation: &str,
        stable_polls_required: u64,
    ) -> Result<()> {
        let deadline = Instant::now() + self.config.poll_timeout;
        let mut previous = None;
        let mut stable_polls = 0;
        loop {
            if Instant::now() >= deadline {
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
            wait_before_retry(deadline, self.config.poll_interval);
        }
    }

    pub(super) fn retry_floe_result_content_hash(
        &self,
        target: PgTarget<'_>,
        artifact_dir: &Path,
    ) -> Result<ContentFingerprint> {
        let attempts = self.config.strict_content_retry_attempts.max(1);
        let delay = Duration::from_secs(self.config.strict_content_retry_delay_seconds);
        let deadline = Instant::now() + self.config.poll_timeout;
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
            if attempt < attempts && !delay.is_zero() && !wait_before_retry(deadline, delay) {
                break;
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("failed to compute Floe content hash")))
    }

    pub(super) fn retry_floe_result_content_hash_until_expected(
        &self,
        target: PgTarget<'_>,
        artifact_dir: &Path,
        expected: &ContentFingerprint,
    ) -> Result<ContentFingerprint> {
        let attempts = self.config.strict_content_retry_attempts.max(1);
        let delay = Duration::from_secs(self.config.strict_content_retry_delay_seconds);
        let deadline = Instant::now() + self.config.poll_timeout;
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
            if attempt < attempts && !delay.is_zero() && !wait_before_retry(deadline, delay) {
                break;
            }
        }
        Ok(last.unwrap_or_else(|| ContentFingerprint {
            row_count: 0,
            hash: String::new(),
        }))
    }

    pub(super) fn floe_offline_expected_content_fingerprint(
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

    pub(super) fn run_floe_validation_for_content(
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
}
