use super::*;

pub(super) fn build_transient_source_receiver(
    graph_id: &str,
    task_label: impl Into<String>,
    upstream: TransientSourceHandleStream,
    input_transform: Arc<DeltaTransformFn>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
) -> mpsc::UnboundedReceiver<TransientMaterializeBatch> {
    let mut upstream_rx = upstream.subscribe();
    let (tx, rx) = mpsc::unbounded_channel::<TransientMaterializeBatch>();
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
                    let input_deltas = match input_transform(batch.deltas.as_ref()) {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if tx.send(TransientMaterializeBatch {
                        version: batch.version,
                        deltas: Arc::new(input_deltas),
                    }).is_err() {
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
    mut upstream: mpsc::UnboundedReceiver<TransientMaterializeBatch>,
    transform: Arc<DeltaTransformFn>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
) -> mpsc::UnboundedReceiver<TransientMaterializeBatch> {
    let (tx, rx) = mpsc::unbounded_channel::<TransientMaterializeBatch>();
    let graph_id = graph_id.to_string();
    let task_label = task_label.into();
    let task_events = task_events.clone();
    let cancel = cancel.clone();
    let debug_transient_join = std::env::var_os("FLOE_DEBUG_TRANSIENT_JOIN").is_some();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                maybe_batch = upstream.recv() => {
                    let Some(batch) = maybe_batch else {
                        break;
                    };
                    let output_deltas = match transform(batch.deltas.as_ref()) {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if debug_transient_join {
                        eprintln!(
                            "transient-transform-output graph_id={} task={} version={} rows={}",
                            graph_id,
                            task_label,
                            batch.version,
                            output_deltas.len()
                        );
                    }
                    if tx.send(TransientMaterializeBatch {
                        version: batch.version,
                        deltas: Arc::new(output_deltas),
                    }).is_err() {
                        break;
                    }
                }
            }
        }
    });
    rx
}
