use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use floe_core::source::{AppendIngestEvent, SourceDefinition};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceCheckpoint {
    pub connector: String,
    pub source: String,
    pub partition: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SinkCheckpoint {
    pub connector: String,
    pub sink: String,
    pub mv_name: String,
    pub last_emitted_mv_version: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_index: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorMetricKind {
    Emit,
    Retry,
    Error,
    Lag,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectorMetricEvent {
    pub connector: String,
    pub kind: ConnectorMetricKind,
    pub value: f64,
    #[serde(default)]
    pub labels: Vec<(String, String)>,
}

pub trait ConnectorMetrics: Send + Sync {
    fn record(&self, event: ConnectorMetricEvent);
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum JsonChangeOp {
    Upsert,
    Delete,
    Snapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonChangeEnvelope {
    pub op: JsonChangeOp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct ConnectorContext {
    pub connector_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorTick {
    Emitted(usize),
    Idle,
    Finished,
}

#[async_trait]
pub trait ConnectorLifecycle: Send {
    fn name(&self) -> &str;

    async fn init(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait SourceConnector: ConnectorLifecycle {
    fn source_definitions(&self) -> &[SourceDefinition];

    async fn tick(&mut self, ctx: &ConnectorContext) -> Result<ConnectorTick>;

    async fn checkpoint(&self) -> Result<Vec<SourceCheckpoint>> {
        Ok(Vec::new())
    }
}

#[async_trait]
pub trait SinkConnector: ConnectorLifecycle {
    async fn send(&mut self, batch: &[serde_json::Value], ctx: &ConnectorContext) -> Result<()>;

    async fn load_checkpoint(&self) -> Result<Option<SinkCheckpoint>> {
        Ok(None)
    }

    async fn persist_checkpoint(&mut self, _cursor: &SinkCheckpoint) -> Result<()> {
        Ok(())
    }
}

pub trait SchemaMapper: Send + Sync {
    fn map_source_schema(&self, definition: &SourceDefinition) -> Result<SourceDefinition>;

    fn map_sink_row(
        &self,
        mv_name: &str,
        version: i64,
        row_index: u64,
        row: &serde_json::Value,
    ) -> Result<serde_json::Value>;
}

pub trait AppendIngestEventDecoder: Send + Sync {
    fn decode(
        &self,
        payload: &[u8],
        default_source: Option<&str>,
    ) -> Result<Vec<AppendIngestEvent>>;
}
