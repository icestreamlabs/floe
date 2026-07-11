use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::source::{
    AppendIngestEventBatch, AppendIngestEventSender, KafkaArrowIngestBatch, KafkaRawIngestBatch,
    send_batch, send_event, send_kafka_arrow_batch, send_kafka_raw_batch,
};
use floe_core::source::{AppendIngestEvent, SourceDefinition};

const MAX_PENDING_EVENTS_BEFORE_YIELD: usize = 65_536;
const MAX_CONSECUTIVE_EMITTED_TICKS_BEFORE_YIELD: usize = 8;

/// Context shared with connector implementations for event emission.
#[derive(Clone)]
pub struct ConnectorContext {
    sender: AppendIngestEventSender,
}

impl ConnectorContext {
    pub fn new(sender: AppendIngestEventSender) -> Self {
        Self { sender }
    }

    pub fn sender(&self) -> &AppendIngestEventSender {
        &self.sender
    }

    pub fn pending_events(&self) -> usize {
        self.sender.pending_events()
    }

    pub async fn send_event(
        &self,
        event: AppendIngestEvent,
    ) -> std::result::Result<(), tokio::sync::mpsc::error::SendError<AppendIngestEventBatch>> {
        send_event(&self.sender, event).await
    }

    pub async fn send_batch(
        &self,
        events: AppendIngestEventBatch,
    ) -> std::result::Result<(), tokio::sync::mpsc::error::SendError<AppendIngestEventBatch>> {
        send_batch(&self.sender, events).await
    }

    pub async fn send_kafka_raw_batch(
        &self,
        batch: KafkaRawIngestBatch,
    ) -> std::result::Result<(), tokio::sync::mpsc::error::SendError<KafkaRawIngestBatch>> {
        send_kafka_raw_batch(&self.sender, batch).await
    }

    pub async fn send_kafka_arrow_batch(
        &self,
        batch: KafkaArrowIngestBatch,
    ) -> std::result::Result<(), tokio::sync::mpsc::error::SendError<KafkaArrowIngestBatch>> {
        send_kafka_arrow_batch(&self.sender, batch).await
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
    let mut consecutive_emitted_ticks = 0usize;

    if interval.is_zero() {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                result = connector.tick(ctx) => {
                    match result {
                        Ok(ConnectorTick::Finished) => break,
                        Ok(ConnectorTick::Emitted(_)) => {
                            consecutive_emitted_ticks = consecutive_emitted_ticks.saturating_add(1);
                            if consecutive_emitted_ticks >= MAX_CONSECUTIVE_EMITTED_TICKS_BEFORE_YIELD
                                || ctx.pending_events() >= MAX_PENDING_EVENTS_BEFORE_YIELD
                            {
                                consecutive_emitted_ticks = 0;
                                tokio::task::yield_now().await;
                            }
                        }
                        Ok(ConnectorTick::Idle) => {
                            consecutive_emitted_ticks = 0;
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
