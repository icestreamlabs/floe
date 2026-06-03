use super::*;

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
