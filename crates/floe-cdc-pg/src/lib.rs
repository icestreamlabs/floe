use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use floe_cdc::CdcTableStore;
use floe_cdc_core::{CdcCheckpoint, CdcSourceId, CdcSourcePosition, CdcTransactionId};
use pgwire_replication::auth::ScramClient;
use pgwire_replication::protocol::framing::{
    read_backend_message, write_password_message, write_query, write_startup_message,
};
use pgwire_replication::protocol::{parse_auth_request, parse_error_response};
use pgwire_replication::{
    Lsn as PgWireLsn, PgWireError, ReplicationClient, ReplicationConfig,
    ReplicationEvent as PgWireReplicationEvent, TlsConfig,
};
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize, Serializer};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

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

#[derive(Debug)]
pub struct PostgresExportedSlotSnapshot {
    slot_name: String,
    consistent_lsn: PostgresLsn,
    snapshot_name: String,
    output_plugin: String,
    _stream: TcpStream,
}

impl PostgresExportedSlotSnapshot {
    pub fn slot_name(&self) -> &str {
        &self.slot_name
    }

    pub fn consistent_lsn(&self) -> PostgresLsn {
        self.consistent_lsn
    }

    pub fn snapshot_name(&self) -> &str {
        &self.snapshot_name
    }

    pub fn output_plugin(&self) -> &str {
        &self.output_plugin
    }
}

pub async fn create_pgoutput_slot_with_exported_snapshot(
    config: &PostgresCdcConfig,
) -> Result<PostgresExportedSlotSnapshot> {
    validate_replication_slot_name(config.slot())?;
    let mut stream = TcpStream::connect((config.host(), config.port()))
        .await
        .with_context(|| {
            format!(
                "connect Postgres replication control plane at {}:{}",
                config.host(),
                config.port()
            )
        })?;
    stream
        .set_nodelay(true)
        .context("configure Postgres replication control TCP_NODELAY")?;

    let startup_params = [
        ("user", config.user()),
        ("database", config.database()),
        ("replication", "database"),
        ("client_encoding", "UTF8"),
        ("application_name", "floe-cdc-snapshot"),
    ];
    write_startup_message(&mut stream, 196608, &startup_params)
        .await
        .context("send Postgres replication startup message")?;
    authenticate_replication_control_stream(&mut stream, config)
        .await
        .context("authenticate Postgres replication control connection")?;

    let command = format!(
        "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput EXPORT_SNAPSHOT",
        config.slot()
    );
    write_query(&mut stream, &command)
        .await
        .with_context(|| format!("create Postgres pgoutput slot '{}'", config.slot()))?;

    let mut data_row = None;
    loop {
        let message = read_backend_message(&mut stream).await.with_context(|| {
            format!(
                "read Postgres CREATE_REPLICATION_SLOT response for '{}'",
                config.slot()
            )
        })?;
        match message.tag {
            b'D' => data_row = Some(parse_simple_data_row(&message.payload)?),
            b'E' => bail!(
                "Postgres failed to create pgoutput slot '{}': {}",
                config.slot(),
                parse_error_response(&message.payload)
            ),
            b'C' | b'T' | b'N' | b'S' | b'K' => {}
            b'Z' => break,
            _ => {}
        }
    }

    let values = data_row.ok_or_else(|| {
        anyhow::anyhow!(
            "Postgres CREATE_REPLICATION_SLOT for '{}' returned no data row",
            config.slot()
        )
    })?;
    ensure!(
        values.len() >= 4,
        "Postgres CREATE_REPLICATION_SLOT returned {} columns, expected at least 4",
        values.len()
    );
    let slot_name = required_data_row_value(&values, 0, "slot_name")?;
    let consistent_lsn = required_data_row_value(&values, 1, "consistent_point")?;
    let snapshot_name = required_data_row_value(&values, 2, "snapshot_name")?;
    let output_plugin = required_data_row_value(&values, 3, "output_plugin")?;
    ensure!(
        slot_name == config.slot(),
        "Postgres created logical slot '{slot_name}', expected '{}'",
        config.slot()
    );
    ensure!(
        output_plugin == "pgoutput",
        "Postgres created logical slot '{}' with output plugin '{output_plugin}', expected pgoutput",
        config.slot()
    );
    let consistent_lsn = PostgresLsn::parse(&consistent_lsn)?;

    Ok(PostgresExportedSlotSnapshot {
        slot_name,
        consistent_lsn,
        snapshot_name,
        output_plugin,
        _stream: stream,
    })
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

fn validate_replication_slot_name(slot: &str) -> Result<()> {
    ensure!(!slot.is_empty(), "Postgres CDC slot cannot be empty");
    ensure!(
        slot.bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "Postgres CDC slot '{}' can only contain lowercase ASCII letters, digits, and underscores",
        slot
    );
    Ok(())
}

async fn authenticate_replication_control_stream<S>(
    stream: &mut S,
    config: &PostgresCdcConfig,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let message = read_backend_message(stream).await?;
        match message.tag {
            b'R' => {
                let (code, data) = parse_auth_request(&message.payload)?;
                handle_replication_control_auth_request(stream, config, code, data).await?;
            }
            b'E' => bail!(
                "Postgres replication authentication failed: {}",
                parse_error_response(&message.payload)
            ),
            b'S' | b'K' => {}
            b'Z' => return Ok(()),
            _ => {}
        }
    }
}

async fn handle_replication_control_auth_request<S>(
    stream: &mut S,
    config: &PostgresCdcConfig,
    code: i32,
    data: &[u8],
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match code {
        0 => Ok(()),
        3 => {
            let mut payload = Vec::from(config.password().as_bytes());
            payload.push(0);
            write_password_message(stream, &payload).await?;
            Ok(())
        }
        10 => authenticate_replication_control_scram(stream, config, data).await,
        _ => Err(PgWireError::Auth(format!("unsupported auth method code: {code}")).into()),
    }
}

async fn authenticate_replication_control_scram<S>(
    stream: &mut S,
    config: &PostgresCdcConfig,
    mechanisms_data: &[u8],
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mechanisms = parse_sasl_mechanisms(mechanisms_data);
    ensure!(
        mechanisms
            .iter()
            .any(|mechanism| mechanism == "SCRAM-SHA-256"),
        "Postgres server does not offer SCRAM-SHA-256 authentication; available mechanisms: {:?}",
        mechanisms
    );

    let scram = ScramClient::new(config.user());
    let mut initial_response = Vec::new();
    initial_response.extend_from_slice(b"SCRAM-SHA-256\0");
    initial_response.extend_from_slice(&(scram.client_first.len() as i32).to_be_bytes());
    initial_response.extend_from_slice(scram.client_first.as_bytes());
    write_password_message(stream, &initial_response).await?;

    let server_first = read_auth_data(stream, 11).await?;
    let server_first = String::from_utf8_lossy(&server_first);
    let (client_final, auth_message, salted_password) =
        scram.client_final(config.password(), &server_first)?;
    write_password_message(stream, client_final.as_bytes()).await?;

    let server_final = read_auth_data(stream, 12).await?;
    let server_final = String::from_utf8_lossy(&server_final);
    ScramClient::verify_server_final(&server_final, &salted_password, &auth_message)?;
    Ok(())
}

async fn read_auth_data<S>(stream: &mut S, expected_code: i32) -> Result<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let message = read_backend_message(stream).await?;
        match message.tag {
            b'R' => {
                let (code, data) = parse_auth_request(&message.payload)?;
                ensure!(
                    code == expected_code,
                    "unexpected Postgres authentication code {code}, expected {expected_code}"
                );
                return Ok(data.to_vec());
            }
            b'E' => bail!(
                "Postgres authentication failed: {}",
                parse_error_response(&message.payload)
            ),
            _ => {}
        }
    }
}

fn parse_sasl_mechanisms(data: &[u8]) -> Vec<String> {
    let mut mechanisms = Vec::new();
    let mut remaining = data;
    while !remaining.is_empty() {
        let Some(pos) = remaining.iter().position(|&byte| byte == 0) else {
            break;
        };
        if pos == 0 {
            break;
        }
        mechanisms.push(String::from_utf8_lossy(&remaining[..pos]).to_string());
        remaining = &remaining[pos + 1..];
    }
    mechanisms
}

fn parse_simple_data_row(payload: &[u8]) -> Result<Vec<Option<String>>> {
    let mut remaining = payload;
    let column_count = take_i16(&mut remaining)? as usize;
    let mut values = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let len = take_i32(&mut remaining)?;
        if len == -1 {
            values.push(None);
            continue;
        }
        ensure!(len >= 0, "Postgres data row field length cannot be {len}");
        let len = len as usize;
        ensure!(
            remaining.len() >= len,
            "Postgres data row field is truncated: need {len} bytes, have {}",
            remaining.len()
        );
        let value = std::str::from_utf8(&remaining[..len])
            .context("decode Postgres data row field as UTF-8")?
            .to_string();
        remaining = &remaining[len..];
        values.push(Some(value));
    }
    ensure!(
        remaining.is_empty(),
        "Postgres data row has {} trailing bytes",
        remaining.len()
    );
    Ok(values)
}

fn required_data_row_value(values: &[Option<String>], idx: usize, name: &str) -> Result<String> {
    values
        .get(idx)
        .and_then(Clone::clone)
        .ok_or_else(|| anyhow::anyhow!("Postgres CREATE_REPLICATION_SLOT returned NULL {name}"))
}

fn take_i16(input: &mut &[u8]) -> Result<i16> {
    ensure!(
        input.len() >= 2,
        "Postgres data row is truncated while reading i16"
    );
    let value = i16::from_be_bytes([input[0], input[1]]);
    *input = &input[2..];
    Ok(value)
}

fn take_i32(input: &mut &[u8]) -> Result<i32> {
    ensure!(
        input.len() >= 4,
        "Postgres data row is truncated while reading i32"
    );
    let value = i32::from_be_bytes([input[0], input[1], input[2], input[3]]);
    *input = &input[4..];
    Ok(value)
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use dbsp_storage::storage::{KeyValueTable, SlateTable};
    use floe_cdc_core::{
        CdcChange, CdcColumn, CdcPrimaryKey, CdcRow, CdcTableId, CdcTableSchema, CdcTransactionId,
        ChangeBatch, TransactionBatch, UpstreamTableRef,
    };
    use floe_core::RowValue;
    use floe_core::catalog::ColumnType;
    use object_store::memory::InMemory;
    use slatedb::Db;

    async fn test_store(name: &str) -> CdcTableStore {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, object_store).await.expect("open SlateDB"));
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        CdcTableStore::new(table)
    }

    fn orders_schema() -> CdcTableSchema {
        CdcTableSchema::new(
            CdcTableId::new("orders").expect("table id"),
            UpstreamTableRef::new("public", "orders").expect("upstream"),
            vec![
                CdcColumn::new("id", ColumnType::Int64, false).expect("id column"),
                CdcColumn::new("status", ColumnType::Utf8, true).expect("status column"),
            ],
            CdcPrimaryKey::new(["id"]).expect("primary key"),
        )
        .expect("schema")
    }

    fn checkpoint_transaction(source_id: CdcSourceId, position: &str) -> TransactionBatch {
        let schema = orders_schema();
        TransactionBatch::new(
            source_id,
            Some(CdcTransactionId::new(format!("tx-{position}")).expect("txid")),
            None,
            CdcSourcePosition::postgres(position, None).expect("position"),
            vec![
                ChangeBatch::new(
                    schema.table_id().clone(),
                    vec![CdcChange::Insert {
                        row: CdcRow::new([
                            Some(RowValue::Int64(1)),
                            Some(RowValue::Utf8("open".to_string())),
                        ])
                        .expect("row"),
                    }],
                )
                .expect("change batch"),
            ],
        )
        .expect("transaction")
    }

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
    fn exported_slot_response_helpers_parse_data_row_and_validate_slot_name() {
        let mut row = Vec::new();
        row.extend_from_slice(&4_i16.to_be_bytes());
        put_data_row_text(&mut row, "slot_a");
        put_data_row_text(&mut row, "0/16B6C50");
        put_data_row_text(&mut row, "00000003-00000010-1");
        put_data_row_text(&mut row, "pgoutput");

        let values = parse_simple_data_row(&row).expect("parse data row");
        assert_eq!(
            values,
            vec![
                Some("slot_a".to_string()),
                Some("0/16B6C50".to_string()),
                Some("00000003-00000010-1".to_string()),
                Some("pgoutput".to_string())
            ]
        );
        validate_replication_slot_name("slot_a_123").expect("valid slot");
        assert!(validate_replication_slot_name("Slot-A").is_err());
    }

    #[test]
    fn config_can_resume_from_cdc_checkpoint() {
        let checkpoint = CdcCheckpoint::new(
            CdcSourceId::new("pg_main").expect("source id"),
            CdcSourcePosition::postgres("0/80", None).expect("position"),
            None,
        );
        let config = PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
            .expect("config")
            .with_start_checkpoint(&checkpoint)
            .expect("resume from checkpoint");
        assert_eq!(config.start_lsn(), Some(PostgresLsn::from_u64(0x80)));
        assert_eq!(
            PostgresLsn::from(config.to_replication_config().expect("pgwire").start_lsn),
            PostgresLsn::from_u64(0x80)
        );
    }

    #[tokio::test]
    async fn stored_checkpoint_configures_start_lsn() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let table_store = test_store("pg-cdc-config-checkpoint").await;
        let schema = orders_schema();
        table_store
            .apply_transaction(
                &HashMap::from([(schema.table_id().clone(), schema)]),
                &checkpoint_transaction(source_id.clone(), "0/90"),
            )
            .await
            .expect("apply checkpoint transaction");

        let config = PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
            .expect("config");
        let resumed = config_with_stored_cdc_checkpoint(config, &table_store, &source_id)
            .await
            .expect("resume config");
        assert_eq!(resumed.start_lsn(), Some(PostgresLsn::from_u64(0x90)));

        let no_checkpoint_source = CdcSourceId::new("pg_other").expect("source id");
        let unchanged = config_with_stored_cdc_checkpoint(
            PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
                .expect("config"),
            &table_store,
            &no_checkpoint_source,
        )
        .await
        .expect("no checkpoint config");
        assert_eq!(unchanged.start_lsn(), None);
    }

    fn put_data_row_text(out: &mut Vec<u8>, value: &str) {
        out.extend_from_slice(&(value.len() as i32).to_be_bytes());
        out.extend_from_slice(value.as_bytes());
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
