use std::time::Duration;

use anyhow::{Result, ensure};
use floe_cdc::CdcTableStore;
use floe_cdc_core::{CdcCheckpoint, CdcSourceId, CdcSourcePosition, CdcTransactionId};
use pgwire_replication::{ReplicationConfig, TlsConfig};
use serde::{Deserialize, Serialize};

use crate::lsn::PostgresLsn;

const DEFAULT_POSTGRES_PORT: u16 = 5432;
const DEFAULT_STATUS_INTERVAL_MS: u64 = 1_000;
const DEFAULT_IDLE_WAKEUP_INTERVAL_MS: u64 = 10_000;
const DEFAULT_BUFFER_EVENTS: usize = 8_192;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostgresCdcConfig {
    host: String,
    #[serde(default = "default_postgres_port")]
    port: u16,
    user: String,
    password: String,
    database: String,
    slot: String,
    publication: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    start_lsn: Option<PostgresLsn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stop_lsn: Option<PostgresLsn>,
    #[serde(default = "default_status_interval_ms")]
    status_interval_ms: u64,
    #[serde(default = "default_idle_wakeup_interval_ms")]
    idle_wakeup_interval_ms: u64,
    #[serde(default = "default_buffer_events")]
    buffer_events: usize,
}

impl PostgresCdcConfig {
    pub fn new(
        host: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
        database: impl Into<String>,
        slot: impl Into<String>,
        publication: impl Into<String>,
    ) -> Result<Self> {
        let config = Self {
            host: host.into(),
            port: DEFAULT_POSTGRES_PORT,
            user: user.into(),
            password: password.into(),
            database: database.into(),
            slot: slot.into(),
            publication: publication.into(),
            start_lsn: None,
            stop_lsn: None,
            status_interval_ms: DEFAULT_STATUS_INTERVAL_MS,
            idle_wakeup_interval_ms: DEFAULT_IDLE_WAKEUP_INTERVAL_MS,
            buffer_events: DEFAULT_BUFFER_EVENTS,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn slot(&self) -> &str {
        &self.slot
    }

    pub fn publication(&self) -> &str {
        &self.publication
    }

    pub fn start_lsn(&self) -> Option<PostgresLsn> {
        self.start_lsn
    }

    pub fn stop_lsn(&self) -> Option<PostgresLsn> {
        self.stop_lsn
    }

    pub fn status_interval(&self) -> Duration {
        Duration::from_millis(self.status_interval_ms)
    }

    pub fn idle_wakeup_interval(&self) -> Duration {
        Duration::from_millis(self.idle_wakeup_interval_ms)
    }

    pub fn buffer_events(&self) -> usize {
        self.buffer_events
    }

    pub fn with_port(mut self, port: u16) -> Result<Self> {
        self.port = port;
        self.validate()?;
        Ok(self)
    }

    pub fn with_start_lsn(mut self, start_lsn: PostgresLsn) -> Self {
        self.start_lsn = Some(start_lsn);
        self
    }

    pub fn with_start_position(mut self, position: &CdcSourcePosition) -> Result<Self> {
        self.start_lsn = Some(PostgresLsn::from_source_position(position)?);
        self.validate()?;
        Ok(self)
    }

    pub fn with_start_checkpoint(mut self, checkpoint: &CdcCheckpoint) -> Result<Self> {
        self.start_lsn = Some(PostgresLsn::from_source_position(checkpoint.position())?);
        self.validate()?;
        Ok(self)
    }

    pub fn with_stop_lsn(mut self, stop_lsn: PostgresLsn) -> Self {
        self.stop_lsn = Some(stop_lsn);
        self
    }

    pub fn with_status_interval(mut self, interval: Duration) -> Result<Self> {
        self.status_interval_ms = millis(interval);
        self.validate()?;
        Ok(self)
    }

    pub fn with_idle_wakeup_interval(mut self, interval: Duration) -> Result<Self> {
        self.idle_wakeup_interval_ms = millis(interval);
        self.validate()?;
        Ok(self)
    }

    pub fn with_buffer_events(mut self, buffer_events: usize) -> Result<Self> {
        self.buffer_events = buffer_events;
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.host.trim().is_empty(),
            "Postgres CDC host cannot be empty"
        );
        ensure!(self.port > 0, "Postgres CDC port must be positive");
        ensure!(
            !self.user.trim().is_empty(),
            "Postgres CDC user cannot be empty"
        );
        ensure!(
            !self.database.trim().is_empty(),
            "Postgres CDC database cannot be empty"
        );
        ensure!(
            !self.slot.trim().is_empty(),
            "Postgres CDC slot cannot be empty"
        );
        ensure!(
            !self.publication.trim().is_empty(),
            "Postgres CDC publication cannot be empty"
        );
        ensure!(
            self.status_interval_ms > 0,
            "Postgres CDC status interval must be positive"
        );
        ensure!(
            self.idle_wakeup_interval_ms > 0,
            "Postgres CDC idle wakeup interval must be positive"
        );
        ensure!(
            self.buffer_events > 0,
            "Postgres CDC event buffer must be positive"
        );
        if let (Some(start_lsn), Some(stop_lsn)) = (self.start_lsn, self.stop_lsn) {
            ensure!(
                stop_lsn >= start_lsn,
                "Postgres CDC stop LSN must be greater than or equal to start LSN"
            );
        }
        Ok(())
    }

    pub fn to_replication_config(&self) -> Result<ReplicationConfig> {
        self.validate()?;
        Ok(ReplicationConfig {
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            password: self.password.clone(),
            database: self.database.clone(),
            tls: TlsConfig::disabled(),
            slot: self.slot.clone(),
            publication: self.publication.clone(),
            start_lsn: self.start_lsn.unwrap_or(PostgresLsn::ZERO).into(),
            stop_at_lsn: self.stop_lsn.map(Into::into),
            status_interval: self.status_interval(),
            idle_wakeup_interval: self.idle_wakeup_interval(),
            buffer_events: self.buffer_events,
        })
    }
}

pub async fn config_with_stored_cdc_checkpoint(
    config: PostgresCdcConfig,
    table_store: &CdcTableStore,
    source_id: &CdcSourceId,
) -> Result<PostgresCdcConfig> {
    let Some(checkpoint) = table_store.load_checkpoint(source_id).await? else {
        tracing::debug!(
            source = %source_id.as_str(),
            slot = %config.slot(),
            configured_start_lsn = ?config.start_lsn(),
            "Postgres CDC has no durable checkpoint; using configured start LSN"
        );
        return Ok(config);
    };
    let slot = config.slot().to_string();
    let configured_start_lsn = config.start_lsn();
    let resumed = config.with_start_checkpoint(&checkpoint)?;
    tracing::info!(
        source = %source_id.as_str(),
        slot = %slot,
        configured_start_lsn = ?configured_start_lsn,
        durable_start_lsn = ?resumed.start_lsn(),
        checkpoint_position = ?checkpoint.position(),
        checkpoint_transaction_id = checkpoint.transaction_id().map(CdcTransactionId::as_str),
        schema_versions = checkpoint.schema_versions().len(),
        "Postgres CDC configured to resume from durable checkpoint"
    );
    Ok(resumed)
}

fn default_postgres_port() -> u16 {
    DEFAULT_POSTGRES_PORT
}

fn default_status_interval_ms() -> u64 {
    DEFAULT_STATUS_INTERVAL_MS
}

fn default_idle_wakeup_interval_ms() -> u64 {
    DEFAULT_IDLE_WAKEUP_INTERVAL_MS
}

fn default_buffer_events() -> usize {
    DEFAULT_BUFFER_EVENTS
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
