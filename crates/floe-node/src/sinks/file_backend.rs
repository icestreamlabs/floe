use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_file_sink(
    sink_name: &str,
    registry: Arc<MaterializedViewRegistry>,
    cancel: CancellationToken,
    path: &str,
    mv: &str,
    with_snapshot: bool,
    as_of: Option<i64>,
    append: bool,
    queue_capacity: usize,
    batch_policy: BatchPolicy,
    checkpoint_tx: Option<SinkCheckpointSender>,
) -> Result<()> {
    if queue_capacity == 0 {
        bail!("sink queue_capacity must be greater than zero");
    }

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
    let consumer_result = run_file_worker(
        sink_name,
        mv,
        path,
        append,
        rx,
        tracker,
        batch_policy,
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

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_file_worker(
    sink_name: &str,
    mv_name: &str,
    path: &str,
    append: bool,
    rx: mpsc::Receiver<SinkEvent>,
    tracker: Arc<SinkQueueTracker>,
    batch_policy: BatchPolicy,
    checkpoint_tx: Option<SinkCheckpointSender>,
) -> Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .truncate(!append)
        .open(path)
        .await
        .with_context(|| format!("open sink file {path}"))?;

    let backend = FileSinkBackend {
        sink_name,
        mv_name,
        file,
        tracker: Arc::clone(&tracker),
        checkpoint_tx,
    };
    run_buffered_sink_worker(rx, tracker, batch_policy, backend).await
}

struct FileSinkBackend<'a> {
    sink_name: &'a str,
    mv_name: &'a str,
    file: tokio::fs::File,
    tracker: Arc<SinkQueueTracker>,
    checkpoint_tx: Option<SinkCheckpointSender>,
}

impl BufferedSinkBackend for FileSinkBackend<'_> {
    async fn flush(
        &mut self,
        buffer: &mut Vec<SinkRecord>,
        buffer_bytes: &mut usize,
        flush_version: Option<i64>,
    ) -> Result<()> {
        let flushed_version = buffer
            .iter()
            .map(|entry| entry.version)
            .max()
            .unwrap_or_else(|| flush_version.unwrap_or(-1));
        flush_file_buffer(
            &mut self.file,
            buffer,
            buffer_bytes,
            &self.tracker,
            flush_version,
        )
        .await?;
        if flushed_version >= 0 {
            publish_sink_cursor(
                &self.checkpoint_tx,
                SinkCursor {
                    sink: self.sink_name.to_string(),
                    mv_name: self.mv_name.to_string(),
                    last_emitted_mv_version: flushed_version,
                    row_index: None,
                },
            )
            .await?;
        }
        Ok(())
    }
}

pub(super) async fn flush_file_buffer(
    file: &mut tokio::fs::File,
    buffer: &mut Vec<SinkRecord>,
    buffer_bytes: &mut usize,
    tracker: &SinkQueueTracker,
    flush_version: Option<i64>,
) -> Result<()> {
    let mut flushed_version = flush_version.unwrap_or(-1);
    for row in buffer.iter() {
        file.write_all(row.payload.as_bytes()).await?;
        file.write_all(b"\n").await?;
        flushed_version = flushed_version.max(row.version);
    }
    file.flush().await?;
    buffer.clear();
    *buffer_bytes = 0;
    if flushed_version >= 0 {
        tracker.on_flushed(flushed_version);
    }
    Ok(())
}
