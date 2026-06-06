use super::*;

pub(super) struct HttpSinkConfig<'a> {
    pub(super) sink_name: &'a str,
    pub(super) changelog: ChangelogSourceConfig<'a>,
    pub(super) url: &'a str,
    pub(super) queue_capacity: usize,
    pub(super) batch_policy: BatchPolicy,
    pub(super) retry_policy: RetryPolicy,
    pub(super) checkpoint_tx: Option<SinkCheckpointSender>,
}

pub(super) async fn run_http_sink(config: HttpSinkConfig<'_>) -> Result<()> {
    if config.queue_capacity == 0 {
        bail!("sink queue_capacity must be greater than zero");
    }

    let client = Client::new();
    let stream = execute_mv_changelog(
        config.changelog.registry.as_ref(),
        config.changelog.params(),
        config.changelog.cancel.clone(),
    )
    .await?;

    let (tx, rx) = mpsc::channel(config.queue_capacity);
    let tracker = SinkQueueTracker::new(config.sink_name);
    let producer_task = tokio::spawn(stream_changelog_into_queue(
        stream,
        tx,
        Arc::clone(&tracker),
    ));
    let consumer_result = run_http_worker(HttpWorkerConfig {
        sink_name: config.sink_name,
        mv_name: config.changelog.mv,
        client: &client,
        url: config.url,
        rx,
        tracker,
        batch_policy: config.batch_policy,
        retry_policy: config.retry_policy,
        checkpoint_tx: config.checkpoint_tx,
        cancel: config.changelog.cancel.clone(),
    })
    .await;
    let producer_result = producer_task
        .await
        .context("join sink queue producer task")
        .and_then(|result| result);

    consumer_result?;
    producer_result?;
    Ok(())
}

pub(super) struct HttpWorkerConfig<'a> {
    pub(super) sink_name: &'a str,
    pub(super) mv_name: &'a str,
    pub(super) client: &'a Client,
    pub(super) url: &'a str,
    pub(super) rx: mpsc::Receiver<SinkEvent>,
    pub(super) tracker: Arc<SinkQueueTracker>,
    pub(super) batch_policy: BatchPolicy,
    pub(super) retry_policy: RetryPolicy,
    pub(super) checkpoint_tx: Option<SinkCheckpointSender>,
    pub(super) cancel: CancellationToken,
}

pub(super) async fn run_http_worker(config: HttpWorkerConfig<'_>) -> Result<()> {
    let backend = HttpSinkBackend {
        sink_name: config.sink_name,
        mv_name: config.mv_name,
        client: config.client,
        url: config.url,
        retry_policy: config.retry_policy,
        tracker: Arc::clone(&config.tracker),
        checkpoint_tx: config.checkpoint_tx,
        cancel: config.cancel,
    };
    run_buffered_sink_worker(config.rx, config.tracker, config.batch_policy, backend).await
}

struct HttpSinkBackend<'a> {
    sink_name: &'a str,
    mv_name: &'a str,
    client: &'a Client,
    url: &'a str,
    retry_policy: RetryPolicy,
    tracker: Arc<SinkQueueTracker>,
    checkpoint_tx: Option<SinkCheckpointSender>,
    cancel: CancellationToken,
}

impl BufferedSinkBackend for HttpSinkBackend<'_> {
    async fn flush(
        &mut self,
        buffer: &mut Vec<SinkRecord>,
        buffer_bytes: &mut usize,
        flush_version: Option<i64>,
    ) -> Result<()> {
        flush_http_buffer(
            HttpFlushContext {
                sink_name: self.sink_name,
                mv_name: self.mv_name,
                client: self.client,
                url: self.url,
                retry_policy: self.retry_policy,
                tracker: &self.tracker,
                checkpoint_tx: &self.checkpoint_tx,
                cancel: &self.cancel,
            },
            buffer,
            buffer_bytes,
            flush_version,
        )
        .await
    }
}

pub(super) struct HttpFlushContext<'a> {
    sink_name: &'a str,
    mv_name: &'a str,
    client: &'a Client,
    url: &'a str,
    retry_policy: RetryPolicy,
    tracker: &'a SinkQueueTracker,
    checkpoint_tx: &'a Option<SinkCheckpointSender>,
    cancel: &'a CancellationToken,
}

async fn flush_http_buffer(
    context: HttpFlushContext<'_>,
    buffer: &mut Vec<SinkRecord>,
    buffer_bytes: &mut usize,
    flush_version: Option<i64>,
) -> Result<()> {
    if buffer.is_empty() {
        if let Some(version) = flush_version {
            context.tracker.on_flushed(version);
            publish_sink_cursor(
                context.checkpoint_tx,
                SinkCursor {
                    sink: context.sink_name.to_string(),
                    mv_name: context.mv_name.to_string(),
                    last_emitted_mv_version: version,
                    row_index: None,
                },
            )
            .await?;
        }
        return Ok(());
    }

    post_http_batch_with_retry(
        context.sink_name,
        context.client,
        context.url,
        buffer,
        context.retry_policy,
        context.cancel,
    )
    .await?;
    let mut flushed_version = flush_version.unwrap_or(-1);
    for row in buffer.iter() {
        flushed_version = flushed_version.max(row.version);
    }
    buffer.clear();
    *buffer_bytes = 0;
    if flushed_version >= 0 {
        context.tracker.on_flushed(flushed_version);
        publish_sink_cursor(
            context.checkpoint_tx,
            SinkCursor {
                sink: context.sink_name.to_string(),
                mv_name: context.mv_name.to_string(),
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
    cancel: &CancellationToken,
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
                wait_for_sink_retry_backoff(retry_policy.backoff_for_failure(attempt), cancel)
                    .await?;
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
