use super::*;

pub(super) struct RuntimeShutdownContext {
    pub(super) runtime_cancel: CancellationToken,
    pub(super) shutdown_signal: CancellationToken,
    pub(super) ingest_cancel: CancellationToken,
    pub(super) sink_cancel: CancellationToken,
    pub(super) service_cancel: CancellationToken,
    pub(super) task_event_tx: mpsc::Sender<GraphTaskError>,
    pub(super) connector_handles: Vec<JoinHandle<()>>,
    pub(super) sink_handles: Vec<JoinHandle<()>>,
    pub(super) admin_handle: JoinHandle<()>,
    pub(super) cdc_replication_debug_handle: JoinHandle<()>,
    pub(super) executor_handle: JoinHandle<()>,
    pub(super) task_monitor: JoinHandle<()>,
    pub(super) server_handle: JoinHandle<anyhow::Result<()>>,
    pub(super) signal_handle: JoinHandle<()>,
    pub(super) cancellation_propagation_handle: JoinHandle<()>,
    pub(super) query: FloeQueryContext,
    pub(super) mv_registry: Arc<MaterializedViewRegistry>,
    pub(super) outer_registry: Arc<Mutex<OuterStreamRegistry>>,
    pub(super) db: Arc<slatedb::Db>,
    pub(super) slatedb_close_timeout_ms: Option<u64>,
    pub(super) runtime_failure: Arc<StdMutex<Option<String>>>,
}

pub(super) async fn shutdown_runtime(context: RuntimeShutdownContext) -> anyhow::Result<()> {
    let RuntimeShutdownContext {
        runtime_cancel,
        shutdown_signal,
        ingest_cancel,
        sink_cancel,
        service_cancel,
        task_event_tx,
        connector_handles,
        sink_handles,
        admin_handle,
        cdc_replication_debug_handle,
        executor_handle,
        task_monitor,
        server_handle,
        signal_handle,
        cancellation_propagation_handle,
        query,
        mv_registry,
        outer_registry,
        db,
        slatedb_close_timeout_ms,
        runtime_failure,
    } = context;

    tokio::select! {
        _ = runtime_cancel.cancelled() => {}
        _ = shutdown_signal.cancelled() => {}
    }
    let graceful_shutdown = shutdown_signal.is_cancelled() && !runtime_cancel.is_cancelled();
    let mut executor_handle = Some(executor_handle);
    if graceful_shutdown {
        ingest_cancel.cancel();
        if let Some(handle) = executor_handle.take()
            && let Err(err) = handle.await
            && !err.is_cancelled()
        {
            tracing::error!(
                error = %err,
                "executor task joined with error during graceful shutdown"
            );
        }
    }
    sink_cancel.cancel();
    service_cancel.cancel();
    runtime_cancel.cancel();
    ingest_cancel.cancel();
    drop(task_event_tx);

    for handle in connector_handles {
        if let Err(err) = handle.await
            && !err.is_cancelled()
        {
            tracing::error!(error = %err, "connector task joined with error");
        }
    }

    for handle in sink_handles {
        if let Err(err) = handle.await
            && !err.is_cancelled()
        {
            tracing::error!(error = %err, "sink task joined with error");
        }
    }

    if let Err(err) = admin_handle.await
        && !err.is_cancelled()
    {
        tracing::error!(error = %err, "admin HTTP server task joined with error");
    }

    if let Err(err) = cdc_replication_debug_handle.await
        && !err.is_cancelled()
    {
        tracing::error!(error = %err, "CDC replication debug task joined with error");
    }

    if let Some(handle) = executor_handle.take()
        && let Err(err) = handle.await
        && !err.is_cancelled()
    {
        tracing::error!(error = %err, "executor task joined with error");
    }

    if let Err(err) = task_monitor.await
        && !err.is_cancelled()
    {
        tracing::error!(error = %err, "graph monitor task joined with error");
    }

    let server_result = match server_handle.await {
        Ok(result) => result,
        Err(err) if err.is_cancelled() => Ok(()),
        Err(err) => Err(anyhow!("pgwire server task join error: {err}")),
    };
    if let Err(err) = signal_handle.await
        && !err.is_cancelled()
    {
        tracing::error!(error = %err, "signal task joined with error");
    }
    if let Err(err) = cancellation_propagation_handle.await
        && !err.is_cancelled()
    {
        tracing::error!(error = %err, "cancellation propagation task joined with error");
    }

    drop(query);
    drop(mv_registry);
    drop(outer_registry);

    let close_timeout =
        Duration::from_millis(slatedb_close_timeout_ms.unwrap_or(DEFAULT_SLATEDB_CLOSE_TIMEOUT_MS));
    let close_result = match tokio::time::timeout(close_timeout, db.close()).await {
        Ok(result) => result.map_err(anyhow::Error::new),
        Err(_) => {
            tracing::warn!(
                timeout_ms = close_timeout.as_millis() as u64,
                "timed out closing SlateDB; continuing shutdown"
            );
            Ok(())
        }
    };

    let recorded_failure = match runtime_failure.lock() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => {
            tracing::warn!("runtime failure lock was poisoned during shutdown");
            poisoned.into_inner().clone()
        }
    };
    if let Some(message) = recorded_failure {
        return Err(anyhow!(message));
    }

    close_result?;

    server_result
}
