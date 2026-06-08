use super::*;

pub(super) fn spawn_signal_handler(
    signal_cancel: CancellationToken,
    signal_ingest_cancel: CancellationToken,
    signal_shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};

            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(err) => {
                    tracing::error!(error = %err, "failed to listen for SIGTERM");
                    signal_cancel.cancel();
                    return;
                }
            };

            tokio::select! {
                signal = tokio::signal::ctrl_c() => {
                    match signal {
                        Ok(()) => tracing::info!("shutdown signal received"),
                        Err(err) => tracing::error!(error = %err, "failed to listen for Ctrl-C"),
                    }
                    signal_ingest_cancel.cancel();
                    signal_shutdown.cancel();
                }
                _ = sigterm.recv() => {
                    tracing::info!("SIGTERM received");
                    signal_ingest_cancel.cancel();
                    signal_shutdown.cancel();
                }
                _ = signal_cancel.cancelled() => {}
            }
        }
        #[cfg(not(unix))]
        {
            tokio::select! {
                signal = tokio::signal::ctrl_c() => {
                    match signal {
                        Ok(()) => tracing::info!("shutdown signal received"),
                        Err(err) => tracing::error!(error = %err, "failed to listen for Ctrl-C"),
                    }
                    signal_ingest_cancel.cancel();
                    signal_shutdown.cancel();
                }
                _ = signal_cancel.cancelled() => {}
            }
        }
    })
}

pub(super) struct PgwireServerSpawn {
    pub(super) query: FloeQueryContext,
    pub(super) mv_registry: Arc<MaterializedViewRegistry>,
    pub(super) server_cancel: CancellationToken,
    pub(super) runtime_cancel_for_server: CancellationToken,
    pub(super) failure_for_server: Arc<StdMutex<Option<String>>>,
    pub(super) enabled: bool,
    pub(super) address: String,
    pub(super) runtime_config: server::ServerRuntimeConfig,
}

pub(super) fn spawn_pgwire_server(request: PgwireServerSpawn) -> JoinHandle<anyhow::Result<()>> {
    let PgwireServerSpawn {
        query,
        mv_registry,
        server_cancel,
        runtime_cancel_for_server,
        failure_for_server,
        enabled,
        address,
        runtime_config,
    } = request;
    if !enabled {
        tracing::warn!("pgwire server disabled by configuration");
        tokio::spawn(async move {
            server_cancel.cancelled().await;
            Ok(())
        })
    } else {
        tokio::spawn(async move {
            let result = server::run_with_shutdown_on_with_config(
                address,
                query,
                mv_registry,
                server_cancel.clone(),
                runtime_config,
            )
            .await;
            if let Err(err) = &result {
                record_runtime_failure(&failure_for_server, format!("pgwire server failed: {err}"));
                runtime_cancel_for_server.cancel();
            }
            result
        })
    }
}
