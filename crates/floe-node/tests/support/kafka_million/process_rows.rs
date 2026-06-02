use super::*;

pub(super) async fn spawn_node(config: NodeSpawnConfig<'_>) -> Result<Child> {
    let NodeSpawnConfig {
        config_path,
        pg_port,
        mv_sql,
        stdout_log_path,
        stderr_log_path,
        ingest_batch_size,
        ingest_batch_per_source,
        ingest_batch_per_connector,
        slatedb_flush_interval_ms,
    } = config;
    let stdout_log = File::create(stdout_log_path)
        .with_context(|| format!("create {}", stdout_log_path.display()))?;
    let stderr_log = File::create(stderr_log_path)
        .with_context(|| format!("create {}", stderr_log_path.display()))?;

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_floe-node"));
    cmd.arg("run")
        .arg("--pgwire-addr")
        .arg(format!("127.0.0.1:{pg_port}"))
        .arg("--admin-port")
        .arg("0")
        .arg("--ingest-queue-capacity")
        .arg("262144")
        .arg("--ingest-batch-size")
        .arg(ingest_batch_size.to_string())
        .arg("--ingest-batch-per-source")
        .arg(ingest_batch_per_source.to_string())
        .arg("--ingest-batch-per-connector")
        .arg(ingest_batch_per_connector.to_string())
        .arg("--slatedb-l0-sst-bytes")
        .arg("1073741824")
        .arg("--slatedb-max-unflushed-bytes")
        .arg("8589934592")
        .arg("--mv-retain-last")
        .arg("256")
        .arg("--config")
        .arg(config_path)
        .arg("--mv-query")
        .arg(mv_sql)
        .stdout(Stdio::from(stdout_log))
        .stderr(Stdio::from(stderr_log));
    if let Some(flush_interval_ms) = slatedb_flush_interval_ms {
        cmd.arg("--slatedb-flush-interval-ms")
            .arg(flush_interval_ms.to_string());
    }
    cmd.spawn().context("spawn floe-node")
}

pub(super) async fn stop_child(child: &mut Child, signal: &str) {
    if let Some(pid) = child.id() {
        let _ = std::process::Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(pid.to_string())
            .status();
    }
    let _ = child.wait().await;
}

pub(super) async fn wait_for_pgwire(
    pg_port: u16,
    child: &mut Child,
    stderr_log_path: &Path,
) -> Result<()> {
    let addr = format!("127.0.0.1:{pg_port}");
    for attempt in 0..120 {
        if let Some(status) = child.try_wait().context("poll floe-node process status")? {
            let stderr_tail = read_log_tail(stderr_log_path, 120).unwrap_or_else(|_| {
                format!("failed to read stderr log {}", stderr_log_path.display())
            });
            bail!(
                "floe-node exited before pgwire became ready (status={status}); stderr tail:\n{stderr_tail}"
            );
        }
        match TcpStream::connect(&addr).await {
            Ok(stream) => {
                drop(stream);
                return Ok(());
            }
            Err(err) if attempt < 119 => {
                if attempt % 20 == 0 {
                    eprintln!("waiting for pgwire at {addr}: {err}");
                }
                sleep(Duration::from_millis(250)).await;
            }
            Err(err) => bail!("pgwire listener at {addr} never became ready: {err}"),
        }
    }
    unreachable!("loop returns or bails")
}

pub(super) fn row_from_json(value: &Value, output_fields: &[FieldSpec]) -> Result<ExpectedRow> {
    let object = value
        .as_object()
        .context("sink payload must be an object")?;
    let mut values = Vec::with_capacity(output_fields.len());
    for field in output_fields {
        let value = match field.kind {
            FieldKind::Int64 => ExpectedValue::Int64(
                object
                    .get(field.name)
                    .and_then(Value::as_i64)
                    .with_context(|| format!("missing int64 field '{}'", field.name))?,
            ),
            FieldKind::String => ExpectedValue::String(
                object
                    .get(field.name)
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
                    .with_context(|| format!("missing string field '{}'", field.name))?,
            ),
        };
        values.push(value);
    }
    Ok(ExpectedRow::new(values))
}

pub(super) fn row_from_pgwire(
    row: &tokio_postgres::SimpleQueryRow,
    output_fields: &[FieldSpec],
) -> Result<ExpectedRow> {
    row_from_query_row_at_offset(row, output_fields, 3)
}

pub(super) fn row_from_query_row(
    row: &tokio_postgres::SimpleQueryRow,
    output_fields: &[FieldSpec],
) -> Result<ExpectedRow> {
    row_from_query_row_at_offset(row, output_fields, 0)
}

pub(super) fn row_from_query_row_at_offset(
    row: &tokio_postgres::SimpleQueryRow,
    output_fields: &[FieldSpec],
    base_offset: usize,
) -> Result<ExpectedRow> {
    let mut values = Vec::with_capacity(output_fields.len());
    for (idx, field) in output_fields.iter().enumerate() {
        let value_idx = idx + base_offset;
        let raw = row
            .get(value_idx)
            .with_context(|| format!("pgwire subscribe row missing {}", field.name))?;
        let value = match field.kind {
            FieldKind::Int64 => ExpectedValue::Int64(
                raw.parse()
                    .with_context(|| format!("parse pgwire {} as i64", field.name))?,
            ),
            FieldKind::String => ExpectedValue::String(raw.to_string()),
        };
        values.push(value);
    }
    Ok(ExpectedRow::new(values))
}

pub(super) fn row_checksum(row: &ExpectedRow) -> i128 {
    let mut acc = 17_i128;
    for value in &row.values {
        acc = match value {
            ExpectedValue::Int64(value) => mix(acc, i128::from(*value)),
            ExpectedValue::String(value) => mix_string(acc, value),
        };
    }
    acc
}

pub(super) fn safe_rows_per_sec(rows: f64, seconds: f64) -> f64 {
    if seconds <= f64::EPSILON {
        0.0
    } else {
        rows / seconds
    }
}

pub(super) fn sample_field_index(
    output_fields: &[FieldSpec],
    sample_match_field: &str,
) -> Result<usize> {
    output_fields
        .iter()
        .position(|field| field.name == sample_match_field)
        .with_context(|| {
            format!(
                "sample_match_field '{}' not found in output schema",
                sample_match_field
            )
        })
}

pub(super) fn expected_value_key(value: &ExpectedValue) -> String {
    match value {
        ExpectedValue::Int64(value) => value.to_string(),
        ExpectedValue::String(value) => value.clone(),
    }
}

pub(super) fn mix(acc: i128, value: i128) -> i128 {
    (acc * 1_000_003 + value + 97).rem_euclid(CHECKSUM_MOD)
}

pub(super) fn mix_string(mut acc: i128, value: &str) -> i128 {
    for byte in value.as_bytes() {
        acc = mix(acc, i128::from(*byte));
    }
    mix(acc, 31)
}

pub(super) fn read_log_tail(path: &Path, max_lines: usize) -> Result<String> {
    let contents =
        std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let lines = contents.lines().collect::<Vec<_>>();
    let start = lines.len().saturating_sub(max_lines);
    Ok(lines[start..].join("\n"))
}

pub(super) fn day_string() -> &'static str {
    DAY_UTC
}
