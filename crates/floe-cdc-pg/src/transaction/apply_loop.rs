use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use floe_cdc::CdcTableStore;
use floe_cdc_core::CdcSourceId;

use crate::{
    PostgresCdcConfig, PostgresLsn, PostgresReplicationClient, PostgresReplicationEvent,
    config_with_stored_cdc_checkpoint,
};

use super::applier::PostgresCdcEventApplier;

#[async_trait]
pub trait PostgresReplicationStream {
    async fn recv_event(&mut self) -> Result<Option<PostgresReplicationEvent>>;
    fn update_applied_lsn(&mut self, lsn: PostgresLsn);
}

#[async_trait]
impl PostgresReplicationStream for PostgresReplicationClient {
    async fn recv_event(&mut self) -> Result<Option<PostgresReplicationEvent>> {
        self.recv().await
    }

    fn update_applied_lsn(&mut self, lsn: PostgresLsn) {
        PostgresReplicationClient::update_applied_lsn(self, lsn);
    }
}

pub async fn run_postgres_cdc_apply_loop<C>(
    client: &mut C,
    applier: &mut PostgresCdcEventApplier,
) -> Result<()>
where
    C: PostgresReplicationStream + Send,
{
    while let Some(event) = client.recv_event().await? {
        let outcome = applier.accept_event(event).await?;
        if let Some(feedback_lsn) = outcome.feedback_lsn() {
            client.update_applied_lsn(feedback_lsn);
        }
    }
    Ok(())
}

#[async_trait]
pub trait PostgresReplicationClientFactory {
    type Stream: PostgresReplicationStream + Send;

    async fn connect(&self, config: &PostgresCdcConfig) -> Result<Self::Stream>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PgWireReplicationClientFactory;

#[async_trait]
impl PostgresReplicationClientFactory for PgWireReplicationClientFactory {
    type Stream = PostgresReplicationClient;

    async fn connect(&self, config: &PostgresCdcConfig) -> Result<Self::Stream> {
        PostgresReplicationClient::connect(config).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostgresCdcReconnectPolicy {
    max_reconnects: usize,
    retry_delay: Duration,
}

impl PostgresCdcReconnectPolicy {
    pub fn new(max_reconnects: usize, retry_delay: Duration) -> Self {
        Self {
            max_reconnects,
            retry_delay,
        }
    }

    pub fn max_reconnects(&self) -> usize {
        self.max_reconnects
    }

    pub fn retry_delay(&self) -> Duration {
        self.retry_delay
    }
}

impl Default for PostgresCdcReconnectPolicy {
    fn default() -> Self {
        Self {
            max_reconnects: 10,
            retry_delay: Duration::from_secs(1),
        }
    }
}

pub async fn run_postgres_cdc_apply_loop_with_reconnect<F>(
    base_config: PostgresCdcConfig,
    source_id: &CdcSourceId,
    table_store: &CdcTableStore,
    applier: &mut PostgresCdcEventApplier,
    factory: &F,
    policy: PostgresCdcReconnectPolicy,
) -> Result<()>
where
    F: PostgresReplicationClientFactory + Sync,
{
    let mut reconnects = 0usize;
    loop {
        let config =
            config_with_stored_cdc_checkpoint(base_config.clone(), table_store, source_id).await?;
        let mut client = factory.connect(&config).await.with_context(|| {
            format!(
                "connect Postgres CDC replication stream from LSN {:?}",
                config.start_lsn()
            )
        })?;

        match run_postgres_cdc_apply_loop(&mut client, applier).await {
            Ok(()) => return Ok(()),
            Err(err) if reconnects < policy.max_reconnects => {
                reconnects += 1;
                applier.reset_stream_state();
                tracing::warn!(
                    error = %err,
                    reconnects,
                    max_reconnects = policy.max_reconnects,
                    retry_delay_ms = policy.retry_delay.as_millis() as u64,
                    start_lsn = ?config.start_lsn(),
                    "Postgres CDC stream failed; reconnecting from durable checkpoint"
                );
                wait_for_postgres_cdc_apply_reconnect(policy.retry_delay).await;
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "Postgres CDC stream failed after {} reconnect attempt(s)",
                        reconnects
                    )
                });
            }
        }
    }
}

async fn wait_for_postgres_cdc_apply_reconnect(delay: Duration) {
    if delay.is_zero() {
        return;
    }
    tokio::time::sleep(delay).await;
}
