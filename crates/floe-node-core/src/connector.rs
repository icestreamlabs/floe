use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::source::SourceEventSender;
use floe_core::source::SourceDefinition;

/// Context shared with connector implementations for event emission.
#[derive(Clone)]
pub struct ConnectorContext {
    sender: SourceEventSender,
}

impl ConnectorContext {
    pub fn new(sender: SourceEventSender) -> Self {
        Self { sender }
    }

    pub fn sender(&self) -> &SourceEventSender {
        &self.sender
    }
}

/// Outcome of a single connector tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorTick {
    /// Events were emitted during this tick.
    Emitted(usize),
    /// No events were emitted, but the connector remains active.
    Idle,
    /// Connector has finished and should stop ticking.
    Finished,
}

/// Connector lifecycle contract.
#[async_trait]
pub trait Connector: Send {
    fn name(&self) -> &str;
    fn definitions(&self) -> &[SourceDefinition];
    fn tick_interval(&self) -> Duration;

    async fn init(&mut self, ctx: &ConnectorContext) -> Result<()>;
    async fn tick(&mut self, ctx: &ConnectorContext) -> Result<ConnectorTick>;
    async fn shutdown(&mut self) -> Result<()>;
}

/// Drive a connector through init/tick/shutdown until cancellation or completion.
pub async fn run_connector<C: Connector>(
    connector: &mut C,
    ctx: &ConnectorContext,
    cancel: CancellationToken,
) -> Result<()> {
    connector.init(ctx).await?;
    let interval = connector.tick_interval();
    let mut error: Option<anyhow::Error> = None;

    if interval.is_zero() {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                result = connector.tick(ctx) => {
                    match result {
                        Ok(ConnectorTick::Finished) => break,
                        Ok(_) => {
                            tokio::task::yield_now().await;
                        }
                        Err(err) => {
                            error = Some(err);
                            break;
                        }
                    }
                }
            }
        }
    } else {
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    match connector.tick(ctx).await {
                        Ok(ConnectorTick::Finished) => break,
                        Ok(_) => {}
                        Err(err) => {
                            error = Some(err);
                            break;
                        }
                    }
                }
            }
        }
    }

    connector.shutdown().await?;
    if let Some(err) = error {
        return Err(err);
    }
    Ok(())
}
