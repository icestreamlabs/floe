mod execution;
mod management;
mod protocol;
mod sql;
mod tail;
mod types;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::{FloeQueryContext, MaterializedViewRegistry};
use floe_storage::SlateCatalog;
use pgwire::error::{ErrorInfo, PgWireError};
use pgwire::tokio::process_socket;
use slatedb::config::Settings;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use execution::FloeServerState;
use protocol::FloeServerFactory;

const LISTEN_ENV: &str = "FLOE_PG_ADDR";
const DATA_ENV: &str = "FLOE_DATA_DIR";

pub async fn init_storage(settings: Option<Settings>) -> Result<Arc<SlateCatalog>> {
    match std::env::var(DATA_ENV) {
        Ok(dir) => {
            let path = PathBuf::from(dir);
            SlateCatalog::with_filesystem_with_settings(path, settings)
                .await
                .map(Arc::new)
                .context("failed to initialise SlateDB filesystem catalog")
        }
        Err(_) => SlateCatalog::in_memory_with_settings(settings)
            .await
            .map(Arc::new)
            .context("failed to initialise SlateDB in-memory catalog"),
    }
}

pub async fn run(
    query: FloeQueryContext,
    materialized_views: Arc<MaterializedViewRegistry>,
) -> Result<()> {
    let shutdown = CancellationToken::new();
    let signal_shutdown = shutdown.clone();
    let signal_task = tokio::spawn(async move {
        tokio::select! {
            signal = signal::ctrl_c() => {
                match signal {
                    Ok(()) => {
                        tracing::info!("shutdown signal received, closing pgwire listener");
                    }
                    Err(err) => {
                        tracing::error!(error = %err, "failed to listen for shutdown signal");
                    }
                }
                signal_shutdown.cancel();
            }
            _ = signal_shutdown.cancelled() => {}
        }
    });

    let result = run_with_shutdown(query, materialized_views, shutdown.clone()).await;
    shutdown.cancel();
    if let Err(err) = signal_task.await
        && !err.is_cancelled()
    {
        tracing::warn!(error = %err, "pgwire signal task joined with error");
    }
    result
}

pub async fn run_with_shutdown(
    query: FloeQueryContext,
    materialized_views: Arc<MaterializedViewRegistry>,
    shutdown: CancellationToken,
) -> Result<()> {
    let db = query.storage().db();
    let bridge = DbspBridge::new(db).await?;
    let state = Arc::new(FloeServerState::new(query, materialized_views, bridge));
    let factory = Arc::new(FloeServerFactory::new(state));

    let address = std::env::var(LISTEN_ENV).unwrap_or_else(|_| "127.0.0.1:6432".to_string());
    let listener = TcpListener::bind(&address)
        .await
        .with_context(|| format!("failed to bind pgwire listener at {address}"))?;
    tracing::info!(address = %address, "Floe pgwire endpoint listening");
    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (socket, peer) = accept_result?;
                let handlers = factory.clone();
                connections.spawn(async move {
                    if let Err(err) = process_socket(socket, None, handlers).await {
                        tracing::warn!(
                            peer = ?peer,
                            error = %err,
                            "connection terminated with error"
                        );
                    }
                });
            }
            _ = shutdown.cancelled() => {
                tracing::info!("shutdown requested, closing pgwire listener");
                break;
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(err)) = joined
                    && !err.is_cancelled()
                {
                    tracing::warn!(error = %err, "connection task joined with error");
                }
            }
        }
    }

    connections.abort_all();
    while let Some(joined) = connections.join_next().await {
        if let Err(err) = joined
            && !err.is_cancelled()
        {
            tracing::warn!(error = %err, "connection task joined with error during shutdown");
        }
    }

    Ok(())
}

pub(crate) fn user_error(message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        "XX000".into(),
        message.into(),
    )))
}
