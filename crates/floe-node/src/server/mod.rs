mod execution;
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
use tokio::net::TcpListener;
use tokio::signal;

use execution::FloeServerState;
use protocol::FloeServerFactory;

const LISTEN_ENV: &str = "FLOE_PG_ADDR";
const DATA_ENV: &str = "FLOE_DATA_DIR";

pub async fn init_storage() -> Result<Arc<SlateCatalog>> {
    match std::env::var(DATA_ENV) {
        Ok(dir) => {
            let path = PathBuf::from(dir);
            SlateCatalog::with_filesystem(path)
                .await
                .map(Arc::new)
                .context("failed to initialise SlateDB filesystem catalog")
        }
        Err(_) => SlateCatalog::in_memory()
            .await
            .map(Arc::new)
            .context("failed to initialise SlateDB in-memory catalog"),
    }
}

pub async fn run(
    query: FloeQueryContext,
    materialized_views: Arc<MaterializedViewRegistry>,
) -> Result<()> {
    let db = query.storage().db();
    let bridge = DbspBridge::new(db).await?;
    let state = Arc::new(FloeServerState::new(query, materialized_views, bridge));
    let factory = Arc::new(FloeServerFactory::new(state));

    let address = std::env::var(LISTEN_ENV).unwrap_or_else(|_| "127.0.0.1:6432".to_string());
    let listener = TcpListener::bind(&address)
        .await
        .with_context(|| format!("failed to bind pgwire listener at {address}"))?;
    println!("Floe pgwire endpoint listening on {address}");

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (socket, peer) = accept_result?;
                let handlers = factory.clone();
                tokio::spawn(async move {
                    if let Err(err) = process_socket(socket, None, handlers).await {
                        eprintln!("connection {peer:?} terminated with error: {err}");
                    }
                });
            }
            signal = signal::ctrl_c() => {
                match signal {
                    Ok(()) => {
                        println!("Shutdown signal received, closing pgwire listener");
                    }
                    Err(err) => {
                        eprintln!("Failed to listen for shutdown signal: {err}");
                    }
                }
                break;
            }
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
