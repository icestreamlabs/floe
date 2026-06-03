use super::*;

pub(super) struct FileSinkConfig<'a> {
    pub(super) sink_name: &'a str,
    pub(super) changelog: ChangelogSourceConfig<'a>,
    pub(super) path: &'a str,
    pub(super) append: bool,
    pub(super) queue_capacity: usize,
    pub(super) batch_policy: BatchPolicy,
    pub(super) checkpoint_tx: Option<SinkCheckpointSender>,
}

pub(super) async fn run_file_sink(config: FileSinkConfig<'_>) -> Result<()> {
    if config.queue_capacity == 0 {
        bail!("sink queue_capacity must be greater than zero");
    }

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
    let consumer_result = run_file_worker(FileWorkerConfig {
        sink_name: config.sink_name,
        mv_name: config.changelog.mv,
        path: config.path,
        append: config.append,
        rx,
        tracker,
        batch_policy: config.batch_policy,
        checkpoint_tx: config.checkpoint_tx,
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

pub(super) struct FileWorkerConfig<'a> {
    pub(super) sink_name: &'a str,
    pub(super) mv_name: &'a str,
    pub(super) path: &'a str,
    pub(super) append: bool,
    pub(super) rx: mpsc::Receiver<SinkEvent>,
    pub(super) tracker: Arc<SinkQueueTracker>,
    pub(super) batch_policy: BatchPolicy,
    pub(super) checkpoint_tx: Option<SinkCheckpointSender>,
}

pub(super) async fn run_file_worker(config: FileWorkerConfig<'_>) -> Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(config.append)
        .truncate(!config.append)
        .open(config.path)
        .await
        .with_context(|| format!("open sink file {}", config.path))?;

    let backend = FileSinkBackend {
        sink_name: config.sink_name,
        mv_name: config.mv_name,
        file,
        tracker: Arc::clone(&config.tracker),
        checkpoint_tx: config.checkpoint_tx,
    };
    run_buffered_sink_worker(config.rx, config.tracker, config.batch_policy, backend).await
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
