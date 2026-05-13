use std::collections::HashSet;
use std::str;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail, ensure};
use floe_cdc_core::{CdcChange, CdcRow};
use floe_cdc_pg::{
    PgOutputCdcChange, PgOutputDecoder, PgOutputRelation, PostgresCdcConfig, PostgresLsn,
    PostgresReplicationClient, PostgresReplicationEvent,
};
use floe_core::RowValue;
use floe_core::source::{SourceDefinition, SourceEvent, SourceResumeToken};
use serde_json::Value;
use tokio::sync::watch;
use tokio_postgres::config::Host;
use tokio_util::sync::CancellationToken;

use crate::connector::{Connector, ConnectorContext, ConnectorTick, run_connector};
use crate::source::SourceEventSender;

const DEFAULT_POSTGRES_PUBLICATION: &str = "floe_publication";

#[derive(Debug, Clone)]
pub struct PostgresCdcConnectorConfig {
    pub connection_string: String,
    pub slot: String,
    pub publication: String,
    pub include_tables: Option<Vec<String>>,
    pub include_schema_in_source: bool,
    pub commit_lsn_rx: Option<watch::Receiver<PostgresCdcCommit>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresSlotCommit {
    pub slot: String,
    pub lsn: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PostgresCdcCommit {
    pub tick_id: u64,
    pub slots: Vec<PostgresSlotCommit>,
}

pub struct PostgresCdcConnector {
    config: PostgresCdcConnectorConfig,
    definitions: Vec<SourceDefinition>,
    replication: Option<PostgresReplicationClient>,
    decoder: PgOutputDecoder,
    include_tables: Option<HashSet<String>>,
    staged_events: Vec<SourceEvent>,
    current_txid: Option<u64>,
    last_committed_tick_id: u64,
}

impl PostgresCdcConnector {
    pub fn new(
        config: PostgresCdcConnectorConfig,
        definitions: Vec<SourceDefinition>,
    ) -> Result<Self> {
        ensure!(
            !config.connection_string.trim().is_empty(),
            "postgres connection string must not be empty"
        );
        ensure!(
            !config.slot.trim().is_empty(),
            "postgres slot must not be empty"
        );
        ensure!(
            !config.publication.trim().is_empty(),
            "postgres publication must not be empty"
        );
        let include_tables = config
            .include_tables
            .as_ref()
            .map(|tables| tables.iter().cloned().collect());
        Ok(Self {
            config,
            definitions,
            replication: None,
            decoder: PgOutputDecoder::new(),
            include_tables,
            staged_events: Vec::new(),
            current_txid: None,
            last_committed_tick_id: 0,
        })
    }

    pub async fn run(config: PostgresCdcConnectorConfig, sender: SourceEventSender) -> Result<()> {
        let mut connector = PostgresCdcConnector::new(config, Vec::new())?;
        let ctx = ConnectorContext::new(sender);
        run_connector(&mut connector, &ctx, CancellationToken::new()).await
    }
}

#[async_trait::async_trait]
impl Connector for PostgresCdcConnector {
    fn name(&self) -> &str {
        "postgres_cdc"
    }

    fn definitions(&self) -> &[SourceDefinition] {
        &self.definitions
    }

    fn tick_interval(&self) -> Duration {
        Duration::ZERO
    }

    async fn init(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        let start_lsn = stored_slot_start_lsn(&self.config.connection_string, &self.config.slot)
            .await
            .with_context(|| {
                format!(
                    "load Postgres logical slot '{}' start LSN",
                    self.config.slot
                )
            })?;
        let replication_config = replication_config_from_connection_string(
            &self.config.connection_string,
            &self.config.slot,
            &self.config.publication,
            start_lsn,
        )?;
        self.replication = Some(
            PostgresReplicationClient::connect(&replication_config)
                .await
                .context("connect native Postgres pgoutput replication client")?,
        );
        Ok(())
    }

    async fn tick(&mut self, ctx: &ConnectorContext) -> Result<ConnectorTick> {
        self.commit_lsn_if_requested().await?;

        loop {
            let event = self
                .replication
                .as_mut()
                .context("postgres cdc connector is not initialized")?
                .recv()
                .await
                .context("receive native Postgres pgoutput event")?;
            let Some(event) = event else {
                return Ok(ConnectorTick::Finished);
            };

            match event {
                PostgresReplicationEvent::Begin { xid, .. } => {
                    self.current_txid = Some(u64::from(xid));
                    self.staged_events.clear();
                }
                PostgresReplicationEvent::XLogData { data, .. } => {
                    for change in self.decoder.decode_cdc_changes(data)? {
                        if let Some(event) = self.change_to_source_event(change)? {
                            self.staged_events.push(event);
                        }
                    }
                }
                PostgresReplicationEvent::Commit { end_lsn, .. } => {
                    return self.commit_staged_events(ctx, end_lsn).await;
                }
                PostgresReplicationEvent::KeepAlive { .. }
                | PostgresReplicationEvent::Message { .. } => {
                    return Ok(ConnectorTick::Idle);
                }
                PostgresReplicationEvent::StoppedAt { .. } => {
                    return Ok(ConnectorTick::Finished);
                }
            }
        }
    }

    async fn shutdown(&mut self) -> Result<()> {
        if let Some(mut replication) = self.replication.take() {
            replication.stop();
            let _ = replication.shutdown().await;
        }
        Ok(())
    }
}

impl PostgresCdcConnector {
    async fn commit_staged_events(
        &mut self,
        ctx: &ConnectorContext,
        commit_lsn: PostgresLsn,
    ) -> Result<ConnectorTick> {
        let txid = self.current_txid.take();
        if self.staged_events.is_empty() {
            return Ok(ConnectorTick::Idle);
        }

        let lsn = commit_lsn.to_pg_string();
        let events = self
            .staged_events
            .drain(..)
            .map(|event| {
                event.with_resume_token(SourceResumeToken::PostgresCdc {
                    slot: Some(self.config.slot.clone()),
                    lsn: lsn.clone(),
                    txid,
                })
            })
            .collect::<Vec<_>>();
        let emitted = events.len();
        ctx.send_batch(events)
            .await
            .context("failed to enqueue postgres cdc batch")?;
        Ok(ConnectorTick::Emitted(emitted))
    }

    fn change_to_source_event(&self, change: PgOutputCdcChange) -> Result<Option<SourceEvent>> {
        let source = source_name_for_relation(change.relation(), &self.config);
        if let Some(allowed) = self.include_tables.as_ref()
            && !allowed.contains(&source)
            && !allowed.contains(change.relation().name())
        {
            return Ok(None);
        }

        match change.change() {
            CdcChange::Insert { row } => Ok(Some(SourceEvent::new(
                source,
                cdc_row_to_json(change.relation(), row)?,
            ))),
            CdcChange::Update { after, .. } => Ok(Some(SourceEvent::new(
                source,
                cdc_row_to_json(change.relation(), after)?,
            ))),
            CdcChange::Delete { .. } => {
                tracing::warn!(
                    table = %source,
                    "native Postgres CDC delete skipped by the legacy SourceEvent bridge; CDC table support will handle deletes"
                );
                Ok(None)
            }
            CdcChange::Truncate => {
                tracing::warn!(
                    table = %source,
                    "native Postgres CDC truncate skipped by the legacy SourceEvent bridge; CDC table support will handle truncates"
                );
                Ok(None)
            }
        }
    }

    async fn commit_lsn_if_requested(&mut self) -> Result<()> {
        let Some(replication) = self.replication.as_mut() else {
            return Ok(());
        };
        let Some(receiver) = self.config.commit_lsn_rx.as_mut() else {
            return Ok(());
        };

        let mut latest_commit = None;
        while receiver.has_changed().unwrap_or(false) {
            latest_commit = Some(receiver.borrow_and_update().clone());
        }
        let Some(commit) = latest_commit else {
            return Ok(());
        };
        if commit.tick_id <= self.last_committed_tick_id {
            return Ok(());
        }

        let Some(target_lsn) = commit
            .slots
            .iter()
            .find(|entry| entry.slot == self.config.slot)
            .map(|entry| entry.lsn.as_str())
        else {
            self.last_committed_tick_id = commit.tick_id;
            return Ok(());
        };

        replication.update_applied_lsn(PostgresLsn::parse(target_lsn)?);
        self.last_committed_tick_id = commit.tick_id;
        Ok(())
    }
}

fn source_name_for_relation(
    relation: &PgOutputRelation,
    config: &PostgresCdcConnectorConfig,
) -> String {
    if config.include_schema_in_source {
        format!("{}.{}", relation.namespace(), relation.name())
    } else {
        relation.name().to_string()
    }
}

fn cdc_row_to_json(relation: &PgOutputRelation, row: &CdcRow) -> Result<Value> {
    ensure!(
        relation.columns().len() == row.values().len(),
        "pgoutput relation '{}' column count {} does not match row length {}",
        relation.name(),
        relation.columns().len(),
        row.values().len()
    );

    let mut payload = serde_json::Map::with_capacity(relation.columns().len());
    for (column, value) in relation.columns().iter().zip(row.values()) {
        payload.insert(column.name().to_string(), row_value_to_json(value));
    }
    Ok(Value::Object(payload))
}

fn row_value_to_json(value: &Option<RowValue>) -> Value {
    match value {
        Some(RowValue::Int64(value)) => Value::from(*value),
        Some(RowValue::Bool(value)) => Value::from(*value),
        Some(RowValue::Utf8(value)) => Value::from(value.clone()),
        Some(RowValue::TimestampMillis(value)) => Value::from(*value),
        Some(RowValue::DateDays(value)) => Value::from(*value),
        Some(RowValue::Numeric(value)) => Value::from(value.clone()),
        None => Value::Null,
    }
}

pub async fn stored_slot_start_lsn(connection_string: &str, slot: &str) -> Result<PostgresLsn> {
    let (client, connection) = tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
        .await
        .context("connect Postgres control plane for native CDC")?;
    let connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::debug!(error = %err, "Postgres native CDC control connection closed");
        }
    });

    let row = client
        .query_opt(
            "SELECT confirmed_flush_lsn::text, restart_lsn::text
             FROM pg_replication_slots
             WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .context("query pg_replication_slots for native CDC start LSN")?
        .ok_or_else(|| {
            anyhow!(
                "Postgres logical replication slot '{slot}' does not exist; create it with pg_create_logical_replication_slot(..., 'pgoutput')"
            )
        })?;
    let confirmed: Option<String> = row.get(0);
    let restart: Option<String> = row.get(1);
    drop(client);
    connection_task.abort();

    let lsn = confirmed
        .or(restart)
        .ok_or_else(|| anyhow!("Postgres logical replication slot '{slot}' has no start LSN"))?;
    PostgresLsn::parse(&lsn)
}

pub fn replication_config_from_connection_string(
    connection_string: &str,
    slot: &str,
    publication: &str,
    start_lsn: PostgresLsn,
) -> Result<PostgresCdcConfig> {
    let config = connection_string
        .parse::<tokio_postgres::Config>()
        .with_context(|| format!("parse Postgres connection string '{connection_string}'"))?;
    let host = match config.get_hosts().first() {
        Some(Host::Tcp(host)) => host.clone(),
        Some(Host::Unix(_)) => bail!("native Postgres CDC requires a TCP host"),
        None => "localhost".to_string(),
    };
    let port = config.get_ports().first().copied().unwrap_or(5432);
    let user = config
        .get_user()
        .ok_or_else(|| anyhow!("native Postgres CDC connection string must include user"))?
        .to_string();
    let database = config
        .get_dbname()
        .map(str::to_string)
        .unwrap_or_else(|| user.clone());
    let password = config
        .get_password()
        .map(str::from_utf8)
        .transpose()
        .context("native Postgres CDC password must be valid UTF-8")?
        .unwrap_or_default()
        .to_string();

    PostgresCdcConfig::new(host, user, password, database, slot, publication)?
        .with_port(port)?
        .with_start_lsn(start_lsn)
        .with_status_interval(Duration::from_millis(100))
}

pub fn default_postgres_publication() -> String {
    DEFAULT_POSTGRES_PUBLICATION.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use floe_cdc_core::{CdcColumn, CdcPrimaryKey, CdcTableId, CdcTableSchema, UpstreamTableRef};
    use floe_core::catalog::ColumnType;

    fn base_config() -> PostgresCdcConnectorConfig {
        PostgresCdcConnectorConfig {
            connection_string: "postgres://floe:secret@localhost:5432/postgres".to_string(),
            slot: "floe_slot".to_string(),
            publication: DEFAULT_POSTGRES_PUBLICATION.to_string(),
            include_tables: None,
            include_schema_in_source: false,
            commit_lsn_rx: None,
        }
    }

    #[test]
    fn constructor_validates_required_fields() {
        let mut config = base_config();
        config.connection_string = " ".to_string();
        assert!(PostgresCdcConnector::new(config.clone(), Vec::new()).is_err());

        config = base_config();
        config.slot = " ".to_string();
        assert!(PostgresCdcConnector::new(config.clone(), Vec::new()).is_err());

        config = base_config();
        config.publication = " ".to_string();
        assert!(PostgresCdcConnector::new(config.clone(), Vec::new()).is_err());
    }

    #[test]
    fn parses_replication_config_from_postgres_url() {
        let config = replication_config_from_connection_string(
            "postgres://floe:secret@127.0.0.1:55432/app",
            "slot",
            "publication",
            PostgresLsn::from_u64(0x50),
        )
        .expect("parse config");

        assert_eq!(config.host(), "127.0.0.1");
        assert_eq!(config.port(), 55432);
        assert_eq!(config.user(), "floe");
        assert_eq!(config.password(), "secret");
        assert_eq!(config.database(), "app");
        assert_eq!(config.slot(), "slot");
        assert_eq!(config.publication(), "publication");
        assert_eq!(config.start_lsn(), Some(PostgresLsn::from_u64(0x50)));
    }

    #[test]
    fn cdc_rows_convert_to_json_payloads() {
        let relation = CdcTableSchema::new(
            CdcTableId::new("orders").expect("table id"),
            UpstreamTableRef::new("public", "orders").expect("upstream"),
            vec![
                CdcColumn::new("id", ColumnType::Int64, false).expect("id"),
                CdcColumn::new("paid", ColumnType::Bool, false).expect("paid"),
                CdcColumn::new("note", ColumnType::Utf8, true).expect("note"),
            ],
            CdcPrimaryKey::new(["id"]).expect("primary key"),
        )
        .expect("schema");
        let row =
            CdcRow::new([Some(RowValue::Int64(7)), Some(RowValue::Bool(true)), None]).expect("row");
        let payload = serde_json::json!({
            "id": 7,
            "paid": true,
            "note": null,
        });

        assert_eq!(
            schema_row_to_json_for_test(&relation, &row).expect("json"),
            payload
        );
    }

    fn schema_row_to_json_for_test(schema: &CdcTableSchema, row: &CdcRow) -> Result<Value> {
        let mut payload = serde_json::Map::with_capacity(schema.columns().len());
        for (column, value) in schema.columns().iter().zip(row.values()) {
            payload.insert(column.name().to_string(), row_value_to_json(value));
        }
        Ok(Value::Object(payload))
    }
}
