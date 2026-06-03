use super::*;

impl Harness {
    pub(super) fn curl_json_file(
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

    pub(super) fn feldera_query(&self, pipeline: &str, sql: &str) -> Result<serde_json::Value> {
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

    pub(super) fn feldera_query_row_count(&self, pipeline: &str, sql: &str) -> Result<u64> {
        let value = self.feldera_query(pipeline, sql)?;
        parse_row_count_value(&value)
            .ok_or_else(|| anyhow!("Feldera query response missing row_count"))
    }

    pub(super) fn compute_feldera_query_content_fingerprint(
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

    pub(super) fn poll_feldera_program_success(&self, pipeline: &str) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(480);
        while Instant::now() < deadline {
            let status = self.feldera_pipeline_field(pipeline, "program_status")?;
            match status.as_str() {
                "Success" => return Ok(()),
                "SqlError" | "RustError" | "SystemError" => {
                    bail!("Feldera program failed with status {status}");
                }
                _ => {
                    wait_before_retry(deadline, Duration::from_secs(2));
                }
            }
        }
        bail!("Feldera program did not compile before timeout")
    }

    pub(super) fn poll_feldera_running(&self, pipeline: &str) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(120);
        while Instant::now() < deadline {
            let status = self.feldera_pipeline_field(pipeline, "deployment_status")?;
            if status == "Running" {
                return Ok(());
            }
            wait_before_retry(deadline, Duration::from_secs(1));
        }
        bail!("Feldera pipeline did not reach Running before timeout")
    }

    pub(super) fn feldera_pipeline_field(&self, pipeline: &str, field: &str) -> Result<String> {
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

    pub(super) fn poll_feldera_source_counts(
        &self,
        pipeline: &str,
        specs: &[RelationSpec],
    ) -> Result<()> {
        let deadline = Instant::now() + self.config.poll_timeout;
        loop {
            if Instant::now() >= deadline {
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
            wait_before_retry(deadline, self.config.poll_interval);
        }
    }

    pub(super) fn poll_feldera_result_rows_equals(
        &self,
        pipeline: &str,
        expected_rows: u64,
    ) -> Result<()> {
        let deadline = Instant::now() + self.config.poll_timeout;
        loop {
            if Instant::now() >= deadline {
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
            wait_before_retry(deadline, self.config.poll_interval);
        }
    }

    pub(super) fn append_summary_row(&self, row: SummaryRow<'_>) -> Result<()> {
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

    pub(super) fn record_failure(
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

    pub(super) fn summarize_floe_hotspots(&self, artifact_dir: &Path) -> Result<String> {
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

    pub(super) fn stop_container(&self, container: &str) {
        let _ = run_status("docker", ["rm", "-f", container], None);
    }

    pub(super) fn stop_floe_process(&mut self) {
        if let Some(mut child) = self.floe_child.take() {
            let pid = child.id().to_string();
            let _ = run_status("kill", ["-INT", &pid], None);
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if child.try_wait().ok().flatten().is_some() {
                    return;
                }
                wait_before_retry(deadline, Duration::from_millis(100));
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
