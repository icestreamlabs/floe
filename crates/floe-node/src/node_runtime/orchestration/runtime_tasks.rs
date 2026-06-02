use super::*;

pub(super) fn spawn_graph_task_monitor(
    runtime_cancel: CancellationToken,
    runtime_failure: Arc<StdMutex<Option<String>>>,
    mut task_event_rx: mpsc::Receiver<GraphTaskError>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = runtime_cancel.cancelled() => break,
                maybe_event = task_event_rx.recv() => {
                    let Some(event) = maybe_event else {
                        break;
                    };
                    tracing::error!(
                        graph_id = %event.graph_id,
                        task = %event.task,
                        error = %event.error,
                        error_chain = %format!("{:#}", event.error),
                        "graph background task failed"
                    );
                    record_runtime_failure(
                        &runtime_failure,
                        format!(
                            "graph background task failed (graph='{}', task='{}'): {}",
                            event.graph_id, event.task, event.error
                        ),
                    );
                    runtime_cancel.cancel();
                }
            }
        }
    })
}

pub(super) fn spawn_cancellation_propagation(
    runtime_cancel: CancellationToken,
    ingest_cancel: CancellationToken,
    sink_cancel: CancellationToken,
    service_cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        runtime_cancel.cancelled().await;
        ingest_cancel.cancel();
        sink_cancel.cancel();
        service_cancel.cancel();
    })
}
