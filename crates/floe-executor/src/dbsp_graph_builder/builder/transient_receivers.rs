use super::*;

pub(super) fn build_transient_source_receiver(
    graph_id: &str,
    task_label: impl Into<String>,
    upstream: TransientSourceHandleStream,
    input_transform: Arc<DeltaTransformFn>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
) -> TransientMaterializeReceiver {
    let mut upstream_rx = upstream.subscribe();
    let (tx, rx) =
        mpsc::channel::<TransientMaterializeBatch>(TRANSIENT_MATERIALIZE_CHANNEL_CAPACITY);
    let graph_id = graph_id.to_string();
    let task_label = task_label.into();
    let task_events = task_events.clone();
    let cancel = cancel.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                maybe_batch = upstream_rx.recv() => {
                    let Some(batch) = maybe_batch else {
                        break;
                    };
                    let input_deltas = match input_transform(Arc::clone(&batch.deltas)).await {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if tx.send(TransientMaterializeBatch {
                        version: batch.version,
                        deltas: Arc::new(input_deltas),
                        deltas_consolidated: false,
                    }).await.is_err() {
                        break;
                    }
                }
            }
        }
    });
    rx
}

pub(super) fn build_transient_transform_receiver(
    graph_id: &str,
    task_label: impl Into<String>,
    mut upstream: TransientMaterializeReceiver,
    transform: Arc<DeltaTransformFn>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
) -> TransientMaterializeReceiver {
    let (tx, rx) =
        mpsc::channel::<TransientMaterializeBatch>(TRANSIENT_MATERIALIZE_CHANNEL_CAPACITY);
    let graph_id = graph_id.to_string();
    let task_label = task_label.into();
    let task_events = task_events.clone();
    let cancel = cancel.clone();
    let debug_transient_join = tracing::enabled!(tracing::Level::DEBUG);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                maybe_batch = upstream.recv() => {
                    let Some(batch) = maybe_batch else {
                        break;
                    };
                    let output_deltas = match transform(Arc::clone(&batch.deltas)).await {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if debug_transient_join {
                        tracing::debug!(
                            graph_id = %graph_id,
                            task = %task_label,
                            version = batch.version,
                            rows = output_deltas.len(),
                            "transient transform output"
                        );
                    }
                    if tx.send(TransientMaterializeBatch {
                        version: batch.version,
                        deltas: Arc::new(output_deltas),
                        deltas_consolidated: false,
                    }).await.is_err() {
                        break;
                    }
                }
            }
        }
    });
    rx
}
