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

pub(super) fn spawn_pgwire_server(
    query: FloeQueryContext,
    mv_registry: Arc<MaterializedViewRegistry>,
    server_cancel: CancellationToken,
    runtime_cancel_for_server: CancellationToken,
    failure_for_server: Arc<StdMutex<Option<String>>>,
) -> JoinHandle<anyhow::Result<()>> {
    let disable_pgwire = std::env::var("FLOE_DISABLE_PGWIRE")
        .ok()
        .map(|value| {
            value == "1" || value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("yes")
        })
        .unwrap_or(false);
    if disable_pgwire {
        tracing::warn!("pgwire server disabled by FLOE_DISABLE_PGWIRE");
        tokio::spawn(async move {
            server_cancel.cancelled().await;
            Ok(())
        })
    } else {
        tokio::spawn(async move {
            let result = server::run_with_shutdown(query, mv_registry, server_cancel.clone()).await;
            if let Err(err) = &result {
                record_runtime_failure(&failure_for_server, format!("pgwire server failed: {err}"));
                runtime_cancel_for_server.cancel();
            }
            result
        })
    }
}
