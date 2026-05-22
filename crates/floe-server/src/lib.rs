mod catalog_shim;
mod execution;
mod management;
mod protocol;
mod sql;
mod subscribe;
mod tail;
mod types;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::tail::TailExecutionConfig;
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

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:6432";

#[derive(Clone, Copy, Debug, Default)]
pub struct ServerRuntimeConfig {
    pub tail: TailExecutionConfig,
}

#[derive(Clone, Debug, Default)]
pub struct ServerStorageConfig {
    pub data_dir: Option<PathBuf>,
    pub object_store_from_env: bool,
    pub object_store_env_file: Option<String>,
    pub slatedb_name: Option<String>,
}

impl ServerStorageConfig {
    pub fn in_memory() -> Self {
        Self::default()
    }
}

pub async fn init_storage(
    config: ServerStorageConfig,
    settings: Option<Settings>,
) -> Result<Arc<SlateCatalog>> {
    if config.object_store_from_env {
        let object_store = slatedb::admin::load_object_store_from_env(config.object_store_env_file)
            .map_err(|err| anyhow::anyhow!("{err}"))
            .context("failed to initialise SlateDB object store from configured environment")?;
        let db_name = config.slatedb_name.unwrap_or_else(|| "floe".to_string());
        return SlateCatalog::with_object_store_with_settings(db_name, object_store, settings)
            .await
            .map(Arc::new)
            .context("failed to initialise SlateDB object-store catalog");
    }

    match config.data_dir {
        Some(path) => SlateCatalog::with_filesystem_with_settings(path, settings)
            .await
            .map(Arc::new)
            .context("failed to initialise SlateDB filesystem catalog"),
        None => SlateCatalog::in_memory_with_settings(settings)
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
    run_with_shutdown_on(DEFAULT_LISTEN_ADDR, query, materialized_views, shutdown).await
}

pub async fn run_with_shutdown_on(
    address: impl Into<String>,
    query: FloeQueryContext,
    materialized_views: Arc<MaterializedViewRegistry>,
    shutdown: CancellationToken,
) -> Result<()> {
    run_with_shutdown_on_with_config(
        address,
        query,
        materialized_views,
        shutdown,
        ServerRuntimeConfig::default(),
    )
    .await
}

pub async fn run_with_shutdown_on_with_config(
    address: impl Into<String>,
    query: FloeQueryContext,
    materialized_views: Arc<MaterializedViewRegistry>,
    shutdown: CancellationToken,
    runtime_config: ServerRuntimeConfig,
) -> Result<()> {
    let db = query.storage().db();
    let bridge = DbspBridge::new(db).await?;
    let state = Arc::new(FloeServerState::new_with_config(
        query,
        materialized_views,
        bridge,
        runtime_config,
    ));
    let factory = Arc::new(FloeServerFactory::new(state));

    let address = address.into();
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
    internal_error(message)
}

pub(crate) fn user_error_with_code(code: &'static str, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        code.into(),
        message.into(),
    )))
}

pub(crate) fn parse_error(message: impl Into<String>) -> PgWireError {
    user_error_with_code("42601", message)
}

pub(crate) fn feature_not_supported_error(message: impl Into<String>) -> PgWireError {
    user_error_with_code("0A000", message)
}

pub(crate) fn undefined_table_error(message: impl Into<String>) -> PgWireError {
    user_error_with_code("42P01", message)
}

pub(crate) fn internal_error(message: impl Into<String>) -> PgWireError {
    user_error_with_code("XX000", message)
}

pub(crate) fn planner_error(message: impl Into<String>) -> PgWireError {
    let message = message.into();
    let normalized = message.to_ascii_lowercase();
    if normalized.contains("table")
        && (normalized.contains("not found")
            || normalized.contains("doesn't exist")
            || normalized.contains("does not exist"))
    {
        return undefined_table_error(message);
    }
    internal_error(message)
}
