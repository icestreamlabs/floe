use super::*;

pub(super) fn consume_sink_metrics(
    brokers: &str,
    topic: &str,
    output_fields: &[FieldSpec],
    expected_rows: i64,
    timeout: Duration,
) -> Result<Metrics> {
    let group_id = format!(
        "floe-redpanda-sink-verify-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", &group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("create kafka consumer")?;
    consumer
        .subscribe(&[topic])
        .with_context(|| format!("subscribe sink topic {topic}"))?;

    let mut metrics = Metrics::default();
    let start = Instant::now();
    let mut last_message_at = Instant::now();
    let mut messages_seen = 0usize;

    while start.elapsed() < timeout {
        match consumer.poll(Duration::from_millis(250)) {
            Some(Ok(message)) => {
                let Some(payload) = message.payload() else {
                    continue;
                };
                let value: Value = serde_json::from_slice(payload).context("parse sink json")?;
                let op = value
                    .get("__op")
                    .and_then(Value::as_i64)
                    .context("sink row missing __op")?;
                if op != 1 {
                    bail!("unexpected sink __op={op}; expected insert-only output");
                }

                let row = row_from_json(&value, output_fields)?;
                metrics.apply(&row, op);
                messages_seen += 1;
                last_message_at = Instant::now();

                if messages_seen.is_multiple_of(100_000) {
                    eprintln!("consumed {messages_seen} sink rows from topic={topic}");
                }
            }
            Some(Err(KafkaError::MessageConsumption(
                RDKafkaErrorCode::UnknownTopicOrPartition,
            ))) => {
                std::thread::sleep(Duration::from_millis(250));
            }
            Some(Err(err)) => return Err(err).context("poll sink topic"),
            None => {
                if metrics.row_count >= expected_rows
                    && last_message_at.elapsed() >= Duration::from_secs(3)
                {
                    break;
                }
            }
        }
    }

    if metrics.row_count != expected_rows {
        bail!(
            "sink did not reach expected row count: observed={}, expected={expected_rows}",
            metrics.row_count
        );
    }

    Ok(metrics)
}

pub(super) async fn verify_pgwire_subscribe(config: SubscribeVerification) -> Result<()> {
    let SubscribeVerification {
        pg_port,
        mv_name,
        output_fields,
        sample_match_field,
        expected,
        verify_mode,
        timeout,
        ready_tx,
    } = config;
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        NoTls,
    )
    .await
    .context("connect to pgwire")?;
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    let subscribe_sql = format!("SUBSCRIBE {mv_name}");
    let mut stream = Box::pin(
        client
            .simple_query_raw(&subscribe_sql)
            .await
            .context("start pgwire subscribe")?,
    );
    let _ = ready_tx.send(());

    let sample_field_idx = sample_field_index(output_fields, sample_match_field)?;
    let pgwire_value_idx = sample_field_idx + 3;
    let mut observed_samples: BTreeMap<String, ExpectedRow> = BTreeMap::new();
    let start = Instant::now();
    let mut subscribe_rows_seen: usize = 0;
    while start.elapsed() < timeout {
        match tokio::time::timeout(Duration::from_millis(250), stream.try_next()).await {
            Ok(Ok(Some(SimpleQueryMessage::Row(row)))) => {
                let Some(op_raw) = row.get(1) else {
                    continue;
                };
                let op: i64 = op_raw.parse().context("parse pgwire floe_diff as i64")?;
                if op != 1 {
                    bail!(
                        "unexpected pgwire subscribe floe_diff={op}; expected insert-only output"
                    );
                }
                subscribe_rows_seen += 1;

                let Some(sample_key) = row.get(pgwire_value_idx) else {
                    continue;
                };
                if expected.sample_rows_by_key.contains_key(sample_key)
                    && !observed_samples.contains_key(sample_key)
                {
                    let actual = row_from_pgwire(&row, output_fields)?;
                    observed_samples.insert(sample_key.to_string(), actual);
                    eprintln!(
                        "captured pgwire sample key={} ({}/{})",
                        sample_key,
                        observed_samples.len(),
                        expected.sample_rows_by_key.len()
                    );
                    if observed_samples.len() == expected.sample_rows_by_key.len() {
                        break;
                    }
                }
                if subscribe_rows_seen.is_multiple_of(100_000) {
                    eprintln!(
                        "consumed {subscribe_rows_seen} pgwire subscribe rows from mv={mv_name}"
                    );
                }
                if matches!(verify_mode, SubscribeVerifyMode::SamplesOnly)
                    && observed_samples.len() == expected.sample_rows_by_key.len()
                {
                    break;
                }
            }
            Ok(Ok(Some(SimpleQueryMessage::RowDescription(_))))
            | Ok(Ok(Some(SimpleQueryMessage::CommandComplete(_)))) => {}
            Ok(Ok(Some(_))) => {}
            Ok(Ok(None)) => break,
            Ok(Err(err)) => return Err(err).context("read pgwire subscribe row"),
            Err(_) => {}
        }
    }

    if observed_samples.len() != expected.sample_rows_by_key.len() {
        let missing: Vec<String> = expected
            .sample_rows_by_key
            .keys()
            .filter(|key| !observed_samples.contains_key(*key))
            .cloned()
            .collect();
        bail!(
            "pgwire subscribe sample row count mismatch: observed={}, expected={}, subscribe_rows_seen={}, missing_keys={missing:?}",
            observed_samples.len(),
            expected.sample_rows_by_key.len(),
            subscribe_rows_seen
        );
    }
    for (key, expected_row) in &expected.sample_rows_by_key {
        let actual = observed_samples
            .get(key)
            .with_context(|| format!("missing pgwire subscribe sample for key={key}"))?;
        if actual != expected_row {
            bail!(
                "pgwire subscribe sample mismatch for key={}: actual={actual:?}, expected={expected_row:?}",
                key
            );
        }
    }

    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(())
}

pub(super) async fn verify_mv_snapshot_count_and_samples(
    config: NoSinkVerification,
) -> Result<NoSinkVerificationTiming> {
    let NoSinkVerification {
        pg_port,
        mv_name,
        output_fields,
        sample_match_field,
        expected,
        timeout,
        verify_mode,
        end_count_settle,
        end_count_poll,
    } = config;
    let verify_started = Instant::now();
    let pgwire_connect_started = Instant::now();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        NoTls,
    )
    .await
    .context("connect to pgwire for no-sink verification")?;
    let pgwire_connect = pgwire_connect_started.elapsed();
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    let expected_rows = usize::try_from(expected.metrics.row_count)
        .context("expected row count must be non-negative and fit usize")?;
    let count_wait_started = Instant::now();
    let (settle_before_poll, poll_interval) =
        if matches!(verify_mode, NoSinkVerifyMode::CountAtEndOnly) {
            (end_count_settle, end_count_poll)
        } else {
            (Duration::ZERO, Duration::from_millis(250))
        };
    if !settle_before_poll.is_zero() {
        sleep(settle_before_poll).await;
    }
    let mut progress_logger = CountProgressLogger::new(count_wait_started, expected_rows);
    loop {
        let observed_rows = query_mv_count(&client, mv_name).await?;
        progress_logger.log(observed_rows);
        if observed_rows == expected_rows {
            break;
        }
        if observed_rows > expected_rows {
            bail!(
                "mv row count exceeded expected: observed={}, expected={}",
                observed_rows,
                expected_rows
            );
        }
        if count_wait_started.elapsed() >= timeout {
            bail!(
                "mv row count did not reach expected within timeout: observed={}, expected={}",
                observed_rows,
                expected_rows
            );
        }
        sleep(poll_interval).await;
    }
    let wait_for_count = count_wait_started.elapsed();
    let wait_for_count_for_throughput = wait_for_count.saturating_sub(settle_before_poll);

    let sample_query_started = Instant::now();
    if matches!(verify_mode, NoSinkVerifyMode::Full) {
        let sample_field_idx = sample_field_index(output_fields, sample_match_field)?;
        let mut observed_samples: BTreeMap<String, ExpectedRow> = BTreeMap::new();
        if !expected.sample_rows_by_key.is_empty() {
            let sample_field_kind = output_fields
                .get(sample_field_idx)
                .map(|field| field.kind)
                .with_context(|| {
                    format!("sample field index {} out of bounds", sample_field_idx)
                })?;
            let sample_in_list =
                build_sql_in_list(expected.sample_rows_by_key.keys(), sample_field_kind)
                    .context("build sample IN list")?;
            let select_fields = output_fields
                .iter()
                .map(|field| field.name)
                .collect::<Vec<_>>()
                .join(", ");
            let sample_sql = format!(
                "SELECT {select_fields} FROM {mv_name} WHERE {sample_match_field} IN ({sample_in_list})"
            );
            let messages = client
                .simple_query(&sample_sql)
                .await
                .with_context(|| format!("query sample rows from {mv_name}"))?;
            for message in messages {
                if let SimpleQueryMessage::Row(row) = message {
                    let parsed = row_from_query_row(&row, output_fields)?;
                    let key = expected_value_key(
                        parsed.values.get(sample_field_idx).with_context(|| {
                            format!(
                                "sample field index {} out of bounds while parsing query row",
                                sample_field_idx
                            )
                        })?,
                    );
                    observed_samples.insert(key, parsed);
                }
            }
        }

        if observed_samples.len() != expected.sample_rows_by_key.len() {
            let missing: Vec<String> = expected
                .sample_rows_by_key
                .keys()
                .filter(|key| !observed_samples.contains_key(*key))
                .cloned()
                .collect();
            bail!(
                "sample row count mismatch after no-sink verification: observed={}, expected={}, missing_keys={missing:?}",
                observed_samples.len(),
                expected.sample_rows_by_key.len()
            );
        }
        for (key, expected_row) in &expected.sample_rows_by_key {
            let actual = observed_samples
                .get(key)
                .with_context(|| format!("missing sample row for key={key}"))?;
            if actual != expected_row {
                bail!(
                    "sample mismatch for key={}: actual={actual:?}, expected={expected_row:?}",
                    key
                );
            }
        }
    }
    let sample_query = sample_query_started.elapsed();

    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(NoSinkVerificationTiming {
        pgwire_connect,
        wait_for_count,
        wait_for_count_for_throughput,
        sample_query,
        total: verify_started.elapsed(),
    })
}

pub(super) fn count_progress_targets(expected_rows: usize) -> [(&'static str, usize); 6] {
    [
        ("10pct", expected_rows / 10),
        ("25pct", expected_rows / 4),
        ("50pct", expected_rows / 2),
        ("75pct", expected_rows.saturating_mul(3) / 4),
        ("90pct", expected_rows.saturating_mul(9) / 10),
        ("100pct", expected_rows),
    ]
}

pub(super) async fn query_mv_count(
    client: &tokio_postgres::Client,
    mv_name: &str,
) -> Result<usize> {
    let sql = format!("SELECT COUNT(*) AS row_count FROM {mv_name}");
    let messages = client
        .simple_query(&sql)
        .await
        .with_context(|| format!("query row count for {mv_name}"))?;
    for message in messages {
        if let SimpleQueryMessage::Row(row) = message {
            let raw = row.get(0).context("COUNT(*) query missing first column")?;
            let count = raw
                .parse::<usize>()
                .with_context(|| format!("parse COUNT(*) result '{raw}' as usize"))?;
            return Ok(count);
        }
    }
    bail!("COUNT(*) query returned no rows for {mv_name}")
}

pub(super) fn build_sql_in_list<'a, I>(keys: I, field_kind: FieldKind) -> Result<String>
where
    I: Iterator<Item = &'a String>,
{
    let mut values = Vec::new();
    for key in keys {
        let value = match field_kind {
            FieldKind::Int64 => {
                let parsed = key
                    .parse::<i64>()
                    .with_context(|| format!("parse sample key '{key}' as i64"))?;
                parsed.to_string()
            }
            FieldKind::String => format!("'{}'", key.replace('\'', "''")),
        };
        values.push(value);
    }
    if values.is_empty() {
        bail!("sample key set is empty");
    }
    Ok(values.join(", "))
}
