use super::*;

pub(super) trait BufferedSinkBackend {
    async fn flush(
        &mut self,
        buffer: &mut Vec<SinkRecord>,
        buffer_bytes: &mut usize,
        flush_version: Option<i64>,
    ) -> Result<()>;
}

pub(super) async fn run_buffered_sink_worker<B>(
    mut rx: mpsc::Receiver<SinkEvent>,
    tracker: Arc<SinkQueueTracker>,
    batch_policy: BatchPolicy,
    mut backend: B,
) -> Result<()>
where
    B: BufferedSinkBackend,
{
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
                        backend.flush(&mut buffer, &mut buffer_bytes, None).await?;
                    }
                }
            }
            SinkEvent::Flush { version } => {
                tracker.on_dequeue();
                backend
                    .flush(&mut buffer, &mut buffer_bytes, Some(version))
                    .await?;
            }
        }
    }

    backend.flush(&mut buffer, &mut buffer_bytes, None).await
}
