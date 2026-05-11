use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use floe_cdc_core::CdcSourcePosition;
use pgwire_replication::{
    Lsn as PgWireLsn, ReplicationClient, ReplicationConfig,
    ReplicationEvent as PgWireReplicationEvent, TlsConfig,
};
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize, Serializer};

mod pgoutput;
mod transaction;
pub use pgoutput::*;
pub use transaction::*;

const DEFAULT_POSTGRES_PORT: u16 = 5432;
const DEFAULT_STATUS_INTERVAL_MS: u64 = 1_000;
const DEFAULT_IDLE_WAKEUP_INTERVAL_MS: u64 = 10_000;
const DEFAULT_BUFFER_EVENTS: usize = 8_192;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PostgresLsn(u64);

impl PostgresLsn {
    pub const ZERO: Self = Self(0);

    pub fn parse(value: &str) -> Result<Self> {
        PgWireLsn::parse(value)
            .map(Self::from)
            .with_context(|| format!("parse Postgres LSN '{value}'"))
    }

    pub fn from_u64(value: u64) -> Self {
        Self(value)
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub fn to_pg_string(self) -> String {
        PgWireLsn::from(self).to_pg_string()
    }

    pub fn to_source_position(self) -> Result<CdcSourcePosition> {
        CdcSourcePosition::postgres(self.to_pg_string(), None)
    }

    pub fn from_source_position(position: &CdcSourcePosition) -> Result<Self> {
        let CdcSourcePosition::Postgres { commit_lsn, .. } = position else {
            bail!("expected Postgres CDC source position, got {position:?}");
        };
        Self::parse(commit_lsn)
    }
}

impl fmt::Display for PostgresLsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        PgWireLsn::from(*self).fmt(f)
    }
}

impl FromStr for PostgresLsn {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl From<PgWireLsn> for PostgresLsn {
    fn from(value: PgWireLsn) -> Self {
        Self(value.as_u64())
    }
}

impl From<PostgresLsn> for PgWireLsn {
    fn from(value: PostgresLsn) -> Self {
        PgWireLsn::from_u64(value.as_u64())
    }
}

impl Serialize for PostgresLsn {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_pg_string())
    }
}

impl<'de> Deserialize<'de> for PostgresLsn {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(de::Error::custom)
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresReplicationEvent {
    KeepAlive {
        wal_end: PostgresLsn,
        reply_requested: bool,
        server_time_micros: i64,
    },
    Begin {
        final_lsn: PostgresLsn,
        xid: u32,
        commit_time_micros: i64,
    },
    XLogData {
        wal_start: PostgresLsn,
        wal_end: PostgresLsn,
        server_time_micros: i64,
        data: Bytes,
    },
    Commit {
        lsn: PostgresLsn,
        end_lsn: PostgresLsn,
        commit_time_micros: i64,
    },
    Message {
        transactional: bool,
        lsn: PostgresLsn,
        prefix: String,
        content: Bytes,
    },
    StoppedAt {
        reached: PostgresLsn,
    },
}

impl From<PgWireReplicationEvent> for PostgresReplicationEvent {
    fn from(event: PgWireReplicationEvent) -> Self {
        match event {
            PgWireReplicationEvent::KeepAlive {
                wal_end,
                reply_requested,
                server_time_micros,
            } => Self::KeepAlive {
                wal_end: wal_end.into(),
                reply_requested,
                server_time_micros,
            },
            PgWireReplicationEvent::Begin {
                final_lsn,
                xid,
                commit_time_micros,
            } => Self::Begin {
                final_lsn: final_lsn.into(),
                xid,
                commit_time_micros,
            },
            PgWireReplicationEvent::XLogData {
                wal_start,
                wal_end,
                server_time_micros,
                data,
            } => Self::XLogData {
                wal_start: wal_start.into(),
                wal_end: wal_end.into(),
                server_time_micros,
                data,
            },
            PgWireReplicationEvent::Commit {
                lsn,
                end_lsn,
                commit_time_micros,
            } => Self::Commit {
                lsn: lsn.into(),
                end_lsn: end_lsn.into(),
                commit_time_micros,
            },
            PgWireReplicationEvent::Message {
                transactional,
                lsn,
                prefix,
                content,
            } => Self::Message {
                transactional,
                lsn: lsn.into(),
                prefix,
                content,
            },
            PgWireReplicationEvent::StoppedAt { reached } => Self::StoppedAt {
                reached: reached.into(),
            },
        }
    }
}

pub struct PostgresReplicationClient {
    inner: ReplicationClient,
}

impl PostgresReplicationClient {
    pub async fn connect(config: &PostgresCdcConfig) -> Result<Self> {
        let config = config.to_replication_config()?;
        let inner = ReplicationClient::connect(config)
            .await
            .context("connect Postgres logical replication client")?;
        Ok(Self { inner })
    }

    pub async fn recv(&mut self) -> Result<Option<PostgresReplicationEvent>> {
        self.inner
            .recv()
            .await
            .context("receive Postgres logical replication event")
            .map(|event| event.map(Into::into))
    }

    pub fn update_applied_lsn(&self, lsn: PostgresLsn) {
        self.inner.update_applied_lsn(lsn.into());
    }

    pub fn stop(&self) {
        self.inner.stop();
    }

    pub fn is_running(&self) -> bool {
        self.inner.is_running()
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        self.inner
            .shutdown()
            .await
            .context("shutdown Postgres logical replication client")
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_lsn_parses_formats_and_serializes_as_pg_lsn() {
        let lsn = PostgresLsn::parse("16/B374D848").expect("parse lsn");
        assert_eq!(lsn.to_string(), "16/B374D848");
        assert_eq!(PostgresLsn::from_u64(lsn.as_u64()), lsn);
        assert!(!lsn.is_zero());

        let encoded = serde_json::to_string(&lsn).expect("serialize lsn");
        assert_eq!(encoded, r#""16/B374D848""#);
        let decoded: PostgresLsn = serde_json::from_str(&encoded).expect("decode lsn");
        assert_eq!(decoded, lsn);
        assert!(PostgresLsn::parse("not-a-lsn").is_err());
    }

    #[test]
    fn postgres_lsn_converts_to_cdc_source_position() {
        let position = PostgresLsn::parse("0/16B6C50")
            .expect("parse lsn")
            .to_source_position()
            .expect("source position");
        assert_eq!(
            position,
            CdcSourcePosition::Postgres {
                commit_lsn: "0/16B6C50".to_string(),
                event_lsn: None
            }
        );
    }

    #[test]
    fn config_validates_required_fields_and_maps_to_pgwire_config() {
        assert!(PostgresCdcConfig::new("", "floe", "", "app", "slot", "pub").is_err());
        assert!(PostgresCdcConfig::new("localhost", "", "", "app", "slot", "pub").is_err());
        assert!(PostgresCdcConfig::new("localhost", "floe", "", "", "slot", "pub").is_err());
        assert!(PostgresCdcConfig::new("localhost", "floe", "", "app", "", "pub").is_err());
        assert!(PostgresCdcConfig::new("localhost", "floe", "", "app", "slot", "").is_err());

        let start = PostgresLsn::parse("0/10").expect("start lsn");
        let stop = PostgresLsn::parse("0/20").expect("stop lsn");
        let config = PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
            .expect("config")
            .with_port(15432)
            .expect("port")
            .with_start_lsn(start)
            .with_stop_lsn(stop)
            .with_status_interval(Duration::from_millis(250))
            .expect("status interval")
            .with_idle_wakeup_interval(Duration::from_millis(500))
            .expect("idle interval")
            .with_buffer_events(64)
            .expect("buffer size");

        let pgwire = config.to_replication_config().expect("pgwire config");
        assert_eq!(pgwire.host, "localhost");
        assert_eq!(pgwire.port, 15432);
        assert_eq!(pgwire.user, "floe");
        assert_eq!(pgwire.password, "secret");
        assert_eq!(pgwire.database, "app");
        assert_eq!(pgwire.slot, "slot");
        assert_eq!(pgwire.publication, "pub");
        assert_eq!(PostgresLsn::from(pgwire.start_lsn), start);
        assert_eq!(pgwire.stop_at_lsn.map(PostgresLsn::from), Some(stop));
        assert_eq!(pgwire.status_interval, Duration::from_millis(250));
        assert_eq!(pgwire.idle_wakeup_interval, Duration::from_millis(500));
        assert_eq!(pgwire.buffer_events, 64);
    }

    #[test]
    fn config_rejects_invalid_replay_bounds_and_tunables() {
        let start = PostgresLsn::parse("0/20").expect("start lsn");
        let stop = PostgresLsn::parse("0/10").expect("stop lsn");
        assert!(
            PostgresCdcConfig::new("localhost", "floe", "", "app", "slot", "pub")
                .expect("config")
                .with_start_lsn(start)
                .with_stop_lsn(stop)
                .validate()
                .is_err()
        );
        assert!(
            PostgresCdcConfig::new("localhost", "floe", "", "app", "slot", "pub")
                .expect("config")
                .with_status_interval(Duration::ZERO)
                .is_err()
        );
        assert!(
            PostgresCdcConfig::new("localhost", "floe", "", "app", "slot", "pub")
                .expect("config")
                .with_buffer_events(0)
                .is_err()
        );
    }

    #[test]
    fn events_map_from_pgwire_without_copying_bytes() {
        let data = Bytes::from_static(b"pgoutput");
        let event = PgWireReplicationEvent::XLogData {
            wal_start: PgWireLsn::from_u64(1),
            wal_end: PgWireLsn::from_u64(2),
            server_time_micros: 3,
            data: data.clone(),
        };
        assert_eq!(
            PostgresReplicationEvent::from(event),
            PostgresReplicationEvent::XLogData {
                wal_start: PostgresLsn::from_u64(1),
                wal_end: PostgresLsn::from_u64(2),
                server_time_micros: 3,
                data
            }
        );
    }
}
