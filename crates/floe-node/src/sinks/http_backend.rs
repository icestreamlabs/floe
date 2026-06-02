use super::*;

pub(super) async fn run_http_sink(
    sink_name: &str,
    registry: Arc<MaterializedViewRegistry>,
    cancel: CancellationToken,
    url: &str,
    mv: &str,
    with_snapshot: bool,
    as_of: Option<i64>,
    queue_capacity: usize,
    batch_policy: BatchPolicy,
    retry_policy: RetryPolicy,
    checkpoint_tx: Option<SinkCheckpointSender>,
) -> Result<()> {
    if queue_capacity == 0 {
        bail!("sink queue_capacity must be greater than zero");
    }

    let client = Client::new();
    let stream = execute_mv_changelog(
        registry.as_ref(),
        MvChangelogParams {
            mv_name: mv.to_string(),
            with_snapshot,
            as_of,
        },
        cancel,
    )
    .await?;

    let (tx, rx) = mpsc::channel(queue_capacity);
    let tracker = SinkQueueTracker::new(sink_name);
    let producer_task = tokio::spawn(stream_changelog_into_queue(
        stream,
        tx,
        Arc::clone(&tracker),
    ));
    let consumer_result = run_http_worker(
        sink_name,
        mv,
        &client,
        url,
        rx,
        tracker,
        batch_policy,
        retry_policy,
        checkpoint_tx,
    )
    .await;
    let producer_result = producer_task
        .await
        .context("join sink queue producer task")
        .and_then(|result| result);

    consumer_result?;
    producer_result?;
    Ok(())
}

pub(super) async fn run_http_worker(
    sink_name: &str,
    mv_name: &str,
    client: &Client,
    url: &str,
    mut rx: mpsc::Receiver<SinkEvent>,
    tracker: Arc<SinkQueueTracker>,
    batch_policy: BatchPolicy,
    retry_policy: RetryPolicy,
    checkpoint_tx: Option<SinkCheckpointSender>,
) -> Result<()> {
    let mut buffer = Vec::new();
    let mut buffer_bytes = 0usize;

    while let Some(event) = rx.recv().await {
        match event {
            SinkEvent::Rows(rows) => {
                tracker.on_dequeue_many(rows.len());
                for row in rows {
                    buffer_bytes += row.byte_len;
                    buffer.push(row);
                    if batch_policy.should_flush(buffer.len(), buffer_bytes) {
                        flush_http_buffer(
                            sink_name,
                            mv_name,
                            client,
                            url,
                            &mut buffer,
                            &mut buffer_bytes,
                            retry_policy,
                            &tracker,
                            None,
                            &checkpoint_tx,
                        )
                        .await?;
                    }
                }
            }
            SinkEvent::Flush { version } => {
                tracker.on_dequeue();
                flush_http_buffer(
                    sink_name,
                    mv_name,
                    client,
                    url,
                    &mut buffer,
                    &mut buffer_bytes,
                    retry_policy,
                    &tracker,
                    Some(version),
                    &checkpoint_tx,
                )
                .await?;
            }
        }
    }

    flush_http_buffer(
        sink_name,
        mv_name,
        client,
        url,
        &mut buffer,
        &mut buffer_bytes,
        retry_policy,
        &tracker,
        None,
        &checkpoint_tx,
    )
    .await?;
    Ok(())
}

pub(super) async fn flush_http_buffer(
    sink_name: &str,
    mv_name: &str,
    client: &Client,
    url: &str,
    buffer: &mut Vec<SinkRecord>,
    buffer_bytes: &mut usize,
    retry_policy: RetryPolicy,
    tracker: &SinkQueueTracker,
    flush_version: Option<i64>,
    checkpoint_tx: &Option<SinkCheckpointSender>,
) -> Result<()> {
    if buffer.is_empty() {
        if let Some(version) = flush_version {
            tracker.on_flushed(version);
            publish_sink_cursor(
                checkpoint_tx,
                SinkCursor {
                    sink: sink_name.to_string(),
                    mv_name: mv_name.to_string(),
                    last_emitted_mv_version: version,
                    row_index: None,
                },
            )
            .await?;
        }
        return Ok(());
    }

    post_http_batch_with_retry(sink_name, client, url, buffer, retry_policy).await?;
    let mut flushed_version = flush_version.unwrap_or(-1);
    for row in buffer.iter() {
        flushed_version = flushed_version.max(row.version);
    }
    buffer.clear();
    *buffer_bytes = 0;
    if flushed_version >= 0 {
        tracker.on_flushed(flushed_version);
        publish_sink_cursor(
            checkpoint_tx,
            SinkCursor {
                sink: sink_name.to_string(),
                mv_name: mv_name.to_string(),
                last_emitted_mv_version: flushed_version,
                row_index: None,
            },
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn post_http_batch_with_retry(
    sink_name: &str,
    client: &Client,
    url: &str,
    batch: &[SinkRecord],
    retry_policy: RetryPolicy,
) -> Result<()> {
    let payload = if batch.len() == 1 {
        batch[0].json.clone()
    } else {
        serde_json::Value::Array(batch.iter().map(|row| row.json.clone()).collect())
    };
    let (idempotency_key, idempotency_key_list) = build_http_idempotency_keys(batch);

    for attempt in 0..retry_policy.max_attempts {
        let result = client
            .post(url)
            .header("Idempotency-Key", &idempotency_key)
            .header("X-Floe-Idempotency-Keys", &idempotency_key_list)
            .json(&payload)
            .send()
            .await
            .context("post sink batch")
            .and_then(|response| response.error_for_status().context("sink http error"));

        match result {
            Ok(_) => return Ok(()),
            Err(err) => {
                if attempt + 1 == retry_policy.max_attempts {
                    metrics::inc_sink_failure(sink_name, "http");
                    return Err(anyhow!("http sink request failed after retries: {err}"));
                }
                metrics::inc_sink_retry(sink_name, "http");
                tokio::time::sleep(retry_policy.backoff_for_failure(attempt)).await;
            }
        }
    }
    unreachable!("retry loop should return or fail");
}

pub(super) fn build_http_idempotency_keys(batch: &[SinkRecord]) -> (String, String) {
    let key_parts: Vec<String> = batch
        .iter()
        .map(|row| format!("{}:{}", row.version, row.row_idx))
        .collect();
    let first_key = key_parts.first().cloned().unwrap_or_default();
    let last_key = key_parts.last().cloned().unwrap_or_default();
    let idempotency_key = if key_parts.len() == 1 {
        first_key
    } else {
        format!("batch:{first_key}..{last_key}")
    };
    (idempotency_key, key_parts.join(","))
}
