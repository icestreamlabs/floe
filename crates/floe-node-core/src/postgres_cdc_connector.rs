use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};
use tokio_util::sync::CancellationToken;

use crate::connector::{Connector, ConnectorContext, ConnectorTick, run_connector};
use crate::source::SourceEventSender;
use floe_core::source::{SourceDefinition, SourceEvent};

#[derive(Debug, Clone)]
pub struct PostgresCdcConnectorConfig {
    pub connection_string: String,
    pub slot: String,
    pub poll_interval: Duration,
    pub max_changes: usize,
    pub default_schema: String,
    pub include_tables: Option<Vec<String>>,
    pub include_schema_in_source: bool,
}

pub struct PostgresCdcConnector {
    config: PostgresCdcConnectorConfig,
    definitions: Vec<SourceDefinition>,
    client: Option<Client>,
    connection_task: Option<JoinHandle<()>>,
    include_tables: Option<HashSet<String>>,
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
            config.max_changes > 0,
            "postgres max_changes must be positive"
        );
        let include_tables = config
            .include_tables
            .as_ref()
            .map(|tables| tables.iter().cloned().collect());
        Ok(Self {
            config,
            definitions,
            client: None,
            connection_task: None,
            include_tables,
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
        self.config.poll_interval
    }

    async fn init(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        let (client, connection) = tokio_postgres::connect(&self.config.connection_string, NoTls)
            .await
            .context("connect to postgres")?;
        let handle = tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::error!(error = %err, "postgres cdc connection closed");
            }
        });
        self.client = Some(client);
        self.connection_task = Some(handle);
        Ok(())
    }

    async fn tick(&mut self, ctx: &ConnectorContext) -> Result<ConnectorTick> {
        let client = self
            .client
            .as_ref()
            .context("postgres cdc connector is not initialized")?;
        let rows = client
            .query(
                "SELECT data FROM pg_logical_slot_get_changes($1, NULL, $2)",
                &[&self.config.slot, &(self.config.max_changes as i64)],
            )
            .await
            .context("fetch logical slot changes")?;

        let mut emitted = 0usize;
        for row in rows {
            let payload: String = row.try_get(0).context("read logical slot payload")?;
            let events = match parse_wal2json_payload(
                &payload,
                &self.config,
                self.include_tables.as_ref(),
            ) {
                Ok(events) => events,
                Err(err) => {
                    tracing::warn!(error = %err, "failed to parse wal2json payload");
                    continue;
                }
            };
            for event in events {
                ctx.sender()
                    .send(event)
                    .await
                    .context("failed to enqueue postgres cdc event")?;
                emitted = emitted.saturating_add(1);
            }
        }

        if emitted > 0 {
            Ok(ConnectorTick::Emitted(emitted))
        } else {
            Ok(ConnectorTick::Idle)
        }
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.client = None;
        if let Some(task) = self.connection_task.take() {
            task.abort();
        }
        Ok(())
    }
}

fn parse_wal2json_payload(
    payload: &str,
    config: &PostgresCdcConnectorConfig,
    include_tables: Option<&HashSet<String>>,
) -> Result<Vec<SourceEvent>> {
    let value: Value = serde_json::from_str(payload).context("decode wal2json payload")?;
    let changes = value
        .get("change")
        .and_then(Value::as_array)
        .context("wal2json payload missing change array")?;
    let mut events = Vec::new();

    for change in changes {
        let kind = change
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !matches!(kind, "insert" | "update") {
            continue;
        }
        let table = change
            .get("table")
            .and_then(Value::as_str)
            .context("wal2json change missing table name")?;
        let schema = change
            .get("schema")
            .and_then(Value::as_str)
            .unwrap_or(config.default_schema.as_str());
        let source = if config.include_schema_in_source {
            format!("{schema}.{table}")
        } else {
            table.to_string()
        };
        if let Some(allowed) = include_tables
            && !allowed.contains(&source)
            && !allowed.contains(table)
        {
            continue;
        }

        let names = change
            .get("columnnames")
            .and_then(Value::as_array)
            .context("wal2json change missing columnnames")?;
        let values = change
            .get("columnvalues")
            .and_then(Value::as_array)
            .context("wal2json change missing columnvalues")?;
        ensure!(
            names.len() == values.len(),
            "wal2json column name/value length mismatch"
        );

        let mut payload_object = serde_json::Map::with_capacity(names.len());
        for (name, value) in names.iter().zip(values.iter()) {
            let key = name
                .as_str()
                .context("wal2json column name must be a string")?;
            payload_object.insert(key.to_string(), value.clone());
        }

        events.push(SourceEvent::new(source, Value::Object(payload_object)));
    }

    Ok(events)
}
