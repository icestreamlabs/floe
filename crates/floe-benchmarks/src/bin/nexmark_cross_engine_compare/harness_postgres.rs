use super::*;

impl Harness {
    pub(super) fn wait_for_pg(&self, port: u16, user: &str, db: &str) -> Result<()> {
        let target = PgTarget { port, user, db };
        for _ in 0..90 {
            if self.fetch_pg_scalar(target, "SELECT 1").ok().as_deref() == Some("1") {
                return Ok(());
            }
            thread::sleep(Duration::from_secs(1));
        }
        bail!("pgwire did not become ready on port {port}")
    }

    pub(super) fn pg_exec(
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

    pub(super) fn psql_file(
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

    pub(super) fn fetch_pg_scalar(&self, target: PgTarget<'_>, sql: &str) -> Result<String> {
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

    pub(super) fn compute_pg_result_content_hash(
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

    pub(super) fn compute_floe_result_content_hash(
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

    pub(super) fn compute_pg_query_content_fingerprint(
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

    pub(super) fn compute_pg_relation_projection(
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

    pub(super) fn compute_floe_normalized_projection_for_relation(
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

    pub(super) fn fetch_pg_table(
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

    pub(super) fn poll_pg_source_counts(
        &self,
        target: PgTarget<'_>,
        specs: &[RelationSpec],
    ) -> Result<()> {
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

    pub(super) fn poll_pg_result_rows_equals(
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

    pub(super) fn poll_floe_query_completion(
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

    pub(super) fn kafka_group_topic_status(
        &self,
        group_id: &str,
        topic: &str,
    ) -> Result<GroupStatus> {
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
}
