use std::collections::BTreeMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context as TaskContext, Poll};

use anyhow::{Context, Result, ensure};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::http::header;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use floe_executor::MaterializedViewRegistry;
use floe_executor::mv_changelog::{
    MvChangelogBatch, MvChangelogParams, MvChangelogStream, execute_mv_changelog,
};
use floe_storage::{ReplicationPipelineDlqEntry, ReplicationPipelineDlqStatus, SlateCatalog};
use futures::Stream;
use prometheus::{Encoder, TextEncoder};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use slatedb::Db;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::node_runtime::{ReplicationPipelineReconciliationOptions, ReplicationPipelineRuntime};
use floe_node_core::source::{
    AppendIngestEvent, AppendIngestEventSender, send_batch_with_commit_ack,
};

const DEFAULT_CDC_REPLICATION_DLQ_BATCH_RETRY_LIMIT: usize = 100;
const DEFAULT_CDC_REPLICATION_DLQ_LIST_LIMIT: usize = 100;
const DEFAULT_CDC_REPLICATION_RECONCILE_MAX_ROWS: usize = 100_000;
const MAX_CDC_REPLICATION_DLQ_BATCH_RETRY_LIMIT: usize = 1_000;
const MAX_CDC_REPLICATION_DLQ_LIST_LIMIT: usize = 1_000;
const MAX_CDC_REPLICATION_RECONCILE_MAX_ROWS: usize = 10_000_000;

#[derive(Debug, Clone)]
pub struct HttpIngestConfig {
    pub host: String,
    pub port: u16,
    pub default_source: Option<String>,
    pub health: Option<HttpIngestHealth>,
}

#[derive(Clone)]
pub struct HttpAdminConfig {
    pub host: String,
    pub port: u16,
    pub health: HttpIngestHealth,
    pub storage_db: Option<Arc<Db>>,
    pub storage_catalog: Option<Arc<SlateCatalog>>,
    pub replication_runtime: Option<Arc<ReplicationPipelineRuntime>>,
    pub materialized_views: Option<Arc<MaterializedViewRegistry>>,
}

#[derive(Debug, Clone)]
pub struct HttpIngestHealth {
    pub executor_running: Arc<AtomicBool>,
    pub storage_reachable: Arc<AtomicBool>,
    pub runtime_ready: Arc<AtomicBool>,
    pub watermark_debug: Option<Arc<RwLock<WatermarkDebugState>>>,
    pub cdc_replication_debug: Option<Arc<RwLock<CdcReplicationDebugState>>>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct WatermarkDebugSourceState {
    pub source: String,
    pub watermark_ms: i64,
    pub idle: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct WatermarkDebugState {
    pub global_watermark_ms: Option<i64>,
    pub policy: String,
    pub updated_at_unix_ms: u64,
    pub sources: Vec<WatermarkDebugSourceState>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct CdcReplicationDebugState {
    pub updated_at_unix_ms: u64,
    pub refresh_error: Option<String>,
    pub postgres_sources: Vec<PostgresCdcDebugSourceState>,
    pub pipelines: Vec<CdcReplicationDebugPipelineState>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PostgresCdcDebugSourceState {
    pub source: String,
    pub slot: Option<String>,
    pub schema_evolution_policy: String,
    pub connected: bool,
    pub reconnect_attempts: u64,
    pub upstream_lsn: Option<String>,
    pub upstream_lsn_bytes: Option<u64>,
    pub durable_lsn: Option<String>,
    pub durable_lsn_bytes: Option<u64>,
    pub source_lag_bytes: Option<u64>,
    pub last_error: Option<String>,
    pub latest_schema_evolution: Option<PostgresCdcSchemaEvolutionDebugState>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PostgresCdcSchemaEvolutionDebugState {
    pub table: String,
    pub upstream_table: String,
    pub policy: String,
    pub outcome: String,
    pub added_columns: Vec<String>,
    pub reason: Option<String>,
    pub catalog_schema_version: u64,
    pub observed_schema_version: u64,
    pub observed_at_unix_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct CdcReplicationDebugPipelineState {
    pub pipeline: String,
    pub source: String,
    pub schema_evolution_policy: String,
    pub error_policy: String,
    pub target_kind: String,
    pub checkpoint_position: Option<String>,
    pub checkpoint_lsn_bytes: Option<u64>,
    pub checkpoint_lag_bytes: Option<u64>,
    pub checkpoint_transaction_id: Option<String>,
    pub target_state: BTreeMap<String, String>,
    pub pending_transactions: usize,
    pub pending_objects: usize,
    pub pending_records: usize,
    pub pending_bytes: usize,
    pub oldest_pending_age_ms: Option<u64>,
    pub dlq_pending_entries: usize,
    pub dlq_replayed_entries: usize,
    pub dlq_discarded_entries: usize,
    pub oldest_dlq_pending_age_ms: Option<u64>,
    pub missing_payload_objects: usize,
    pub orphan_payload_objects: usize,
    pub orphan_payload_bytes: usize,
    pub replaying: bool,
    pub source_backpressure_active: bool,
    pub last_error: Option<String>,
}

#[derive(Clone)]
struct HttpIngestState {
    sender: AppendIngestEventSender,
    default_source: Option<String>,
    cancel: CancellationToken,
    health: Option<HttpIngestHealth>,
}

#[derive(Clone)]
struct HttpAdminState {
    cancel: CancellationToken,
    health: HttpIngestHealth,
    storage_db: Option<Arc<Db>>,
    storage_catalog: Option<Arc<SlateCatalog>>,
    replication_runtime: Option<Arc<ReplicationPipelineRuntime>>,
    materialized_views: Option<Arc<MaterializedViewRegistry>>,
}

#[derive(Deserialize)]
struct IngestQuery {
    source: Option<String>,
}

#[derive(Deserialize)]
struct SubscribeQuery {
    with_snapshot: Option<bool>,
    as_of: Option<i64>,
}

#[derive(Deserialize)]
struct CdcReplicationDlqListQuery {
    pipeline: String,
    status: Option<ReplicationPipelineDlqStatus>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Deserialize)]
struct CdcReplicationDlqActionRequest {
    reason: Option<String>,
    operator: Option<String>,
}

#[derive(Deserialize)]
struct CdcReplicationDlqBatchRetryQuery {
    pipeline: String,
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct CdcReplicationReconcileQuery {
    max_rows: Option<usize>,
    full_scan: Option<bool>,
}

#[derive(Serialize)]
struct CdcReplicationDlqListResponse {
    pipeline: String,
    status: Option<ReplicationPipelineDlqStatus>,
    offset: usize,
    limit: usize,
    total_matching: usize,
    count: usize,
    oldest_pending_age_ms: Option<u64>,
    entries: Vec<ReplicationPipelineDlqEntry>,
}

#[derive(Serialize)]
struct CdcReplicationDlqEntryResponse {
    entry: ReplicationPipelineDlqEntry,
}

#[path = "http_ingest/cdc_admin.rs"]
mod cdc_admin;
#[path = "http_ingest/server.rs"]
mod server;
#[path = "http_ingest/sse_json.rs"]
mod sse_json;
#[cfg(test)]
#[path = "http_ingest/tests.rs"]
mod tests;

pub use self::server::{run_admin_server, run_http_ingest};

use self::cdc_admin::*;
use self::sse_json::*;
