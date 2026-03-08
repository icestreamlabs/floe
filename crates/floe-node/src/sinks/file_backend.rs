use super::*;

pub(super) async fn run_file_sink(
    sink_name: &str,
    query: &FloeQueryContext,
    registry: Arc<MaterializedViewRegistry>,
    cancel: CancellationToken,
    path: &str,
    mv: &str,
    with_snapshot: bool,
    as_of: Option<i64>,
    append: bool,
    effectively_once: bool,
    queue_capacity: usize,
    batch_policy: BatchPolicy,
    checkpoint_tx: Option<mpsc::UnboundedSender<SinkCursor>>,
) -> Result<()> {
    if queue_capacity == 0 {
        bail!("sink queue_capacity must be greater than zero");
    }

    let stream = execute_tail(
        &query.session(),
        registry.as_ref(),
        TailParams {
            mv_name: mv.to_string(),
            with_snapshot,
            as_of,
        },
        cancel,
    )
    .await?;

    let (tx, rx) = mpsc::channel(queue_capacity);
    let tracker = SinkQueueTracker::new(sink_name);
    let producer_task = tokio::spawn(stream_tail_into_queue(stream, tx, Arc::clone(&tracker)));
    let consumer_result = if effectively_once {
        run_file_worker_effectively_once(sink_name, mv, path, rx, tracker, checkpoint_tx).await
    } else {
        run_file_worker(
            sink_name,
            mv,
            path,
            append,
            rx,
            tracker,
            batch_policy,
            checkpoint_tx,
        )
        .await
    };
    let producer_result = producer_task
        .await
        .context("join sink queue producer task")
        .and_then(|result| result);

    consumer_result?;
    producer_result?;
    Ok(())
}

pub(super) async fn run_file_worker(
    sink_name: &str,
    mv_name: &str,
    path: &str,
    append: bool,
    mut rx: mpsc::Receiver<SinkEvent>,
    tracker: Arc<SinkQueueTracker>,
    batch_policy: BatchPolicy,
    checkpoint_tx: Option<mpsc::UnboundedSender<SinkCursor>>,
) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .truncate(!append)
        .open(path)
        .await
        .with_context(|| format!("open sink file {path}"))?;

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
                        let flushed_version =
                            buffer.iter().map(|entry| entry.version).max().unwrap_or(-1);
                        flush_file_buffer(
                            &mut file,
                            &mut buffer,
                            &mut buffer_bytes,
                            &tracker,
                            None,
                        )
                        .await?;
                        if flushed_version >= 0 {
                            publish_sink_cursor(
                                &checkpoint_tx,
                                SinkCursor {
                                    sink: sink_name.to_string(),
                                    mv_name: mv_name.to_string(),
                                    last_emitted_mv_version: flushed_version,
                                    row_index: None,
                                },
                            );
                        }
                    }
                }
            }
            SinkEvent::Flush { version } => {
                tracker.on_dequeue();
                flush_file_buffer(
                    &mut file,
                    &mut buffer,
                    &mut buffer_bytes,
                    &tracker,
                    Some(version),
                )
                .await?;
                publish_sink_cursor(
                    &checkpoint_tx,
                    SinkCursor {
                        sink: sink_name.to_string(),
                        mv_name: mv_name.to_string(),
                        last_emitted_mv_version: version,
                        row_index: None,
                    },
                );
            }
        }
    }

    let final_version = buffer.iter().map(|entry| entry.version).max().unwrap_or(-1);
    flush_file_buffer(&mut file, &mut buffer, &mut buffer_bytes, &tracker, None).await?;
    if final_version >= 0 {
        publish_sink_cursor(
            &checkpoint_tx,
            SinkCursor {
                sink: sink_name.to_string(),
                mv_name: mv_name.to_string(),
                last_emitted_mv_version: final_version,
                row_index: None,
            },
        );
    }
    Ok(())
}

pub(super) async fn run_file_worker_effectively_once(
    sink_name: &str,
    mv_name: &str,
    path: &str,
    mut rx: mpsc::Receiver<SinkEvent>,
    tracker: Arc<SinkQueueTracker>,
    checkpoint_tx: Option<mpsc::UnboundedSender<SinkCursor>>,
) -> Result<()> {
    let mut pending = Vec::new();

    while let Some(event) = rx.recv().await {
        match event {
            SinkEvent::Rows(rows) => {
                tracker.on_dequeue_many(rows.len());
                pending.extend(rows);
            }
            SinkEvent::Flush { version } => {
                tracker.on_dequeue();
                let mut rows = Vec::new();
                let mut retained = Vec::new();
                for row in pending.drain(..) {
                    if row.version <= version {
                        rows.push(row);
                    } else {
                        retained.push(row);
                    }
                }
                pending = retained;
                write_versioned_file_batch(path, version, &rows).await?;
                tracker.on_flushed(version);
                publish_sink_cursor(
                    &checkpoint_tx,
                    SinkCursor {
                        sink: sink_name.to_string(),
                        mv_name: mv_name.to_string(),
                        last_emitted_mv_version: version,
                        row_index: None,
                    },
                );
            }
        }
    }

    if !pending.is_empty() {
        let final_version = pending.iter().map(|row| row.version).max().unwrap_or(-1);
        if final_version >= 0 {
            write_versioned_file_batch(path, final_version, &pending).await?;
            tracker.on_flushed(final_version);
            publish_sink_cursor(
                &checkpoint_tx,
                SinkCursor {
                    sink: sink_name.to_string(),
                    mv_name: mv_name.to_string(),
                    last_emitted_mv_version: final_version,
                    row_index: None,
                },
            );
        }
    }
    Ok(())
}

pub(super) async fn write_versioned_file_batch(
    path: &str,
    version: i64,
    rows: &[SinkRecord],
) -> Result<()> {
    let data_path = format!("{path}.v{version}.jsonl");
    if fs::try_exists(&data_path).await.unwrap_or(false) {
        return Ok(());
    }

    let tmp_path = format!("{data_path}.pending");
    let mut tmp = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)
        .await
        .with_context(|| format!("open pending sink file {}", tmp_path))?;
    for row in rows {
        tmp.write_all(row.payload.as_bytes()).await?;
        tmp.write_all(b"\n").await?;
    }
    tmp.flush().await?;
    tmp.sync_all().await?;
    drop(tmp);
    fs::rename(&tmp_path, &data_path)
        .await
        .with_context(|| format!("commit versioned sink file {}", data_path))?;

    let manifest_path = format!("{path}.manifest");
    let mut manifest = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&manifest_path)
        .await
        .with_context(|| format!("open sink manifest {}", manifest_path))?;
    let manifest_entry = serde_json::json!({
        "version": version,
        "file": data_path,
        "rows": rows.len(),
        "committed_at_unix_ms": current_unix_time_ms(),
    });
    let line = serde_json::to_string(&manifest_entry).context("serialize sink manifest entry")?;
    manifest.write_all(line.as_bytes()).await?;
    manifest.write_all(b"\n").await?;
    manifest.flush().await?;
    manifest.sync_all().await?;

    let commit_tmp_path = format!("{path}.commit.pending");
    let commit_path = format!("{path}.commit");
    fs::write(&commit_tmp_path, version.to_string())
        .await
        .with_context(|| format!("write sink commit marker {}", commit_tmp_path))?;
    fs::rename(&commit_tmp_path, &commit_path)
        .await
        .with_context(|| format!("commit sink marker {}", commit_path))?;
    Ok(())
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
