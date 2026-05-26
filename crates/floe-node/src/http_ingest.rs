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
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::TimeUnit;
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

use crate::node_runtime::ReplicationPipelineRuntime;
use floe_node_core::source::{
    AppendIngestEvent, AppendIngestEventSender, send_batch_with_commit_ack,
};

const DEFAULT_CDC_REPLICATION_DLQ_BATCH_RETRY_LIMIT: usize = 100;
const DEFAULT_CDC_REPLICATION_DLQ_LIST_LIMIT: usize = 100;
const MAX_CDC_REPLICATION_DLQ_BATCH_RETRY_LIMIT: usize = 1_000;
const MAX_CDC_REPLICATION_DLQ_LIST_LIMIT: usize = 1_000;

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
    pub upstream_lsn: Option<String>,
    pub upstream_lsn_bytes: Option<u64>,
    pub durable_lsn: Option<String>,
    pub durable_lsn_bytes: Option<u64>,
    pub source_lag_bytes: Option<u64>,
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

pub async fn run_http_ingest(
    config: HttpIngestConfig,
    sender: AppendIngestEventSender,
    cancel: CancellationToken,
) -> Result<()> {
    let state = HttpIngestState {
        sender,
        default_source: config.default_source,
        cancel: cancel.clone(),
        health: config.health,
    };
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/debug/watermarks", get(debug_watermarks_ingest))
        .route("/ingest", post(ingest))
        .route("/metrics", get(metrics))
        .with_state(state);
    let addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind http ingest {addr}"))?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel.cancelled().await;
        })
        .await
        .context("run http ingest server")?;
    Ok(())
}

pub async fn run_admin_server(config: HttpAdminConfig, cancel: CancellationToken) -> Result<()> {
    let state = HttpAdminState {
        cancel: cancel.clone(),
        health: config.health,
        storage_db: config.storage_db,
        storage_catalog: config.storage_catalog,
        replication_runtime: config.replication_runtime,
        materialized_views: config.materialized_views,
    };
    let app = Router::new()
        .route("/healthz", get(admin_healthz))
        .route("/readyz", get(admin_readyz))
        .route("/debug/watermarks", get(debug_watermarks_admin))
        .route("/debug/cdc/replication", get(debug_cdc_replication_admin))
        .route("/ops/cdc/replication", get(debug_cdc_replication_admin))
        .route(
            "/debug/cdc/replication/dlq",
            get(debug_cdc_replication_dlq_list_admin),
        )
        .route(
            "/ops/cdc/replication/dlq",
            get(debug_cdc_replication_dlq_list_admin),
        )
        .route(
            "/debug/cdc/replication/dlq/retry",
            post(debug_cdc_replication_dlq_retry_batch_admin),
        )
        .route(
            "/ops/cdc/replication/dlq/retry",
            post(debug_cdc_replication_dlq_retry_batch_admin),
        )
        .route(
            "/debug/cdc/replication/dlq/:pipeline/:dlq_id",
            get(debug_cdc_replication_dlq_entry_admin),
        )
        .route(
            "/ops/cdc/replication/dlq/:pipeline/:dlq_id",
            get(debug_cdc_replication_dlq_entry_admin),
        )
        .route(
            "/debug/cdc/replication/dlq/:pipeline/:dlq_id/discard",
            post(debug_cdc_replication_dlq_discard_admin),
        )
        .route(
            "/ops/cdc/replication/dlq/:pipeline/:dlq_id/discard",
            post(debug_cdc_replication_dlq_discard_admin),
        )
        .route(
            "/debug/cdc/replication/dlq/:pipeline/:dlq_id/retry",
            post(debug_cdc_replication_dlq_retry_admin),
        )
        .route(
            "/ops/cdc/replication/dlq/:pipeline/:dlq_id/retry",
            post(debug_cdc_replication_dlq_retry_admin),
        )
        .route("/debug/storage/flush", post(debug_storage_flush_admin))
        .route("/mv", get(subscribe_sse_admin))
        .route("/metrics", get(metrics))
        .with_state(state);
    let addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind admin http server {addr}"))?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel.cancelled().await;
        })
        .await
        .context("run admin http server")?;
    Ok(())
}

async fn ingest(
    State(state): State<HttpIngestState>,
    Query(query): Query<IngestQuery>,
    Json(payload): Json<Value>,
) -> Result<StatusCode, (StatusCode, String)> {
    let default_source = query.source.as_deref().or(state.default_source.as_deref());
    let events = parse_events(payload, default_source).map_err(|err| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid event payload: {err}"),
        )
    })?;

    let commit_ack = send_batch_with_commit_ack(&state.sender, events)
        .await
        .map_err(|err| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("ingest channel closed: {err}"),
            )
        })?;

    commit_ack
        .await
        .map_err(|err| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("ingest commit ack closed: {err}"),
            )
        })?
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err))?;

    Ok(StatusCode::OK)
}

async fn healthz(State(state): State<HttpIngestState>) -> impl IntoResponse {
    let process_alive = !state.cancel.is_cancelled();
    let status = if process_alive {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(serde_json::json!({
            "process_alive": process_alive,
        })),
    )
}

async fn readyz(State(state): State<HttpIngestState>) -> impl IntoResponse {
    let process_alive = !state.cancel.is_cancelled();
    let executor_alive = state
        .health
        .as_ref()
        .map(|health| health.executor_running.load(Ordering::Relaxed))
        .unwrap_or(true);
    let storage_reachable = state
        .health
        .as_ref()
        .map(|health| health.storage_reachable.load(Ordering::Relaxed))
        .unwrap_or(true);
    let runtime_ready = state
        .health
        .as_ref()
        .map(|health| health.runtime_ready.load(Ordering::Relaxed))
        .unwrap_or(true);

    let status = if process_alive && executor_alive && storage_reachable && runtime_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(serde_json::json!({
            "process_alive": process_alive,
            "executor_alive": executor_alive,
            "storage_reachable": storage_reachable,
            "runtime_ready": runtime_ready,
        })),
    )
}

async fn admin_healthz(State(state): State<HttpAdminState>) -> impl IntoResponse {
    let process_alive = !state.cancel.is_cancelled();
    let status = if process_alive {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(serde_json::json!({
            "process_alive": process_alive,
        })),
    )
}

async fn admin_readyz(State(state): State<HttpAdminState>) -> impl IntoResponse {
    let process_alive = !state.cancel.is_cancelled();
    let executor_alive = state.health.executor_running.load(Ordering::Relaxed);
    let storage_reachable = state.health.storage_reachable.load(Ordering::Relaxed);
    let runtime_ready = state.health.runtime_ready.load(Ordering::Relaxed);
    let status = if process_alive && executor_alive && storage_reachable && runtime_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(serde_json::json!({
            "process_alive": process_alive,
            "executor_alive": executor_alive,
            "storage_reachable": storage_reachable,
            "runtime_ready": runtime_ready,
        })),
    )
}

async fn debug_watermarks_ingest(State(state): State<HttpIngestState>) -> impl IntoResponse {
    let Some(health) = state.health else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "watermark debug state unavailable"})),
        )
            .into_response();
    };
    let Some(shared) = health.watermark_debug else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "watermark debug state unavailable"})),
        )
            .into_response();
    };
    let snapshot = shared.read().await.clone();
    (StatusCode::OK, Json(snapshot)).into_response()
}

async fn debug_watermarks_admin(State(state): State<HttpAdminState>) -> impl IntoResponse {
    let Some(shared) = &state.health.watermark_debug else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "watermark debug state unavailable"})),
        )
            .into_response();
    };
    let snapshot = shared.read().await.clone();
    (StatusCode::OK, Json(snapshot)).into_response()
}

async fn debug_cdc_replication_admin(State(state): State<HttpAdminState>) -> impl IntoResponse {
    let Some(shared) = &state.health.cdc_replication_debug else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "CDC replication debug state unavailable"})),
        )
            .into_response();
    };
    let snapshot = shared.read().await.clone();
    (StatusCode::OK, Json(snapshot)).into_response()
}

async fn debug_cdc_replication_dlq_list_admin(
    State(state): State<HttpAdminState>,
    Query(query): Query<CdcReplicationDlqListQuery>,
) -> impl IntoResponse {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_CDC_REPLICATION_DLQ_LIST_LIMIT);
    if limit == 0 || limit > MAX_CDC_REPLICATION_DLQ_LIST_LIMIT {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!(
                    "list limit must be between 1 and {}",
                    MAX_CDC_REPLICATION_DLQ_LIST_LIMIT
                ),
            })),
        )
            .into_response();
    }
    let offset = query.offset.unwrap_or(0);
    let Some(storage) = &state.storage_catalog else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "storage catalog unavailable"})),
        )
            .into_response();
    };
    let mut entries = match storage
        .replication_pipeline_dlq_entries(&query.pipeline)
        .await
    {
        Ok(entries) => entries,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": err.to_string()})),
            )
                .into_response();
        }
    };
    if let Some(status) = query.status {
        entries.retain(|entry| entry.status() == status);
    }
    entries.sort_by(|left, right| {
        left.created_at_unix_ms()
            .cmp(&right.created_at_unix_ms())
            .then_with(|| left.dlq_id().cmp(right.dlq_id()))
    });
    let oldest_pending_age_ms = entries
        .iter()
        .filter(|entry| entry.status() == ReplicationPipelineDlqStatus::Pending)
        .map(|entry| current_unix_time_ms().saturating_sub(entry.created_at_unix_ms()))
        .min();
    let total_matching = entries.len();
    let entries = entries
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    let response = CdcReplicationDlqListResponse {
        pipeline: query.pipeline,
        status: query.status,
        offset,
        limit,
        total_matching,
        count: entries.len(),
        oldest_pending_age_ms,
        entries,
    };
    (StatusCode::OK, Json(response)).into_response()
}

async fn debug_cdc_replication_dlq_entry_admin(
    State(state): State<HttpAdminState>,
    Path((pipeline, dlq_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let Some(storage) = &state.storage_catalog else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "storage catalog unavailable"})),
        )
            .into_response();
    };
    match storage
        .replication_pipeline_dlq_entry(&pipeline, &dlq_id)
        .await
    {
        Ok(Some(entry)) => (
            StatusCode::OK,
            Json(CdcReplicationDlqEntryResponse { entry }),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "DLQ entry not found"})),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": err.to_string()})),
        )
            .into_response(),
    }
}

async fn debug_cdc_replication_dlq_discard_admin(
    State(state): State<HttpAdminState>,
    Path((pipeline, dlq_id)): Path<(String, String)>,
    payload: Option<Json<CdcReplicationDlqActionRequest>>,
) -> impl IntoResponse {
    let Some(storage) = &state.storage_catalog else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "storage catalog unavailable"})),
        )
            .into_response();
    };
    let reason = payload
        .map(|Json(payload)| payload)
        .and_then(cdc_replication_action_reason);
    let Some(reason) = reason else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "discard reason is required"})),
        )
            .into_response();
    };
    match storage
        .update_replication_pipeline_dlq_entry_status_with_reason(
            &pipeline,
            &dlq_id,
            ReplicationPipelineDlqStatus::Discarded,
            Some(reason),
            current_unix_time_ms(),
        )
        .await
    {
        Ok(Some(entry)) => (
            StatusCode::OK,
            Json(CdcReplicationDlqEntryResponse { entry }),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "DLQ entry not found"})),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": err.to_string()})),
        )
            .into_response(),
    }
}

async fn debug_cdc_replication_dlq_retry_admin(
    State(state): State<HttpAdminState>,
    Path((pipeline, dlq_id)): Path<(String, String)>,
    payload: Option<Json<CdcReplicationDlqActionRequest>>,
) -> impl IntoResponse {
    let Some(storage) = &state.storage_catalog else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "storage catalog unavailable"})),
        )
            .into_response();
    };
    let Some(runtime) = &state.replication_runtime else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "CDC replication runtime unavailable"})),
        )
            .into_response();
    };
    let reason = payload
        .map(|Json(payload)| payload)
        .and_then(cdc_replication_action_reason);
    match runtime
        .retry_dlq_entry_with_reason(storage, &pipeline, &dlq_id, reason)
        .await
    {
        Ok(Some(entry)) => (
            StatusCode::OK,
            Json(CdcReplicationDlqEntryResponse { entry }),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "DLQ entry or pipeline not found"})),
        )
            .into_response(),
        Err(err) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": err.to_string()})),
        )
            .into_response(),
    }
}

async fn debug_cdc_replication_dlq_retry_batch_admin(
    State(state): State<HttpAdminState>,
    Query(query): Query<CdcReplicationDlqBatchRetryQuery>,
    payload: Option<Json<CdcReplicationDlqActionRequest>>,
) -> impl IntoResponse {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_CDC_REPLICATION_DLQ_BATCH_RETRY_LIMIT);
    if limit == 0 || limit > MAX_CDC_REPLICATION_DLQ_BATCH_RETRY_LIMIT {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!(
                    "retry limit must be between 1 and {}",
                    MAX_CDC_REPLICATION_DLQ_BATCH_RETRY_LIMIT
                ),
            })),
        )
            .into_response();
    }
    let Some(storage) = &state.storage_catalog else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "storage catalog unavailable"})),
        )
            .into_response();
    };
    let Some(runtime) = &state.replication_runtime else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "CDC replication runtime unavailable"})),
        )
            .into_response();
    };
    let reason = payload
        .map(|Json(payload)| payload)
        .and_then(cdc_replication_action_reason);
    match runtime
        .retry_pending_dlq_entries_with_reason(storage, &query.pipeline, limit, reason)
        .await
    {
        Ok(Some(outcome)) => (StatusCode::OK, Json(outcome)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "pipeline not found"})),
        )
            .into_response(),
        Err(err) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": err.to_string()})),
        )
            .into_response(),
    }
}

fn cdc_replication_action_reason(request: CdcReplicationDlqActionRequest) -> Option<String> {
    let reason = request.reason?.trim().to_string();
    if reason.is_empty() {
        return None;
    }
    let Some(operator) = request.operator.map(|operator| operator.trim().to_string()) else {
        return Some(reason);
    };
    if operator.is_empty() {
        Some(reason)
    } else {
        Some(format!("{reason} (operator: {operator})"))
    }
}

async fn debug_storage_flush_admin(State(state): State<HttpAdminState>) -> impl IntoResponse {
    let Some(db) = &state.storage_db else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "storage unavailable"})),
        )
            .into_response();
    };
    let started_at = std::time::Instant::now();
    match db.flush().await {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "flushed": true,
                "elapsed_ms": started_at.elapsed().as_millis() as u64,
            })),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "flushed": false,
                "error": err.to_string(),
                "elapsed_ms": started_at.elapsed().as_millis() as u64,
            })),
        )
            .into_response(),
    }
}

async fn subscribe_sse_admin(
    State(state): State<HttpAdminState>,
    Query(query): Query<SubscribeQuery>,
) -> Response {
    let Some(registry) = &state.materialized_views else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "materialized view registry unavailable"})),
        )
            .into_response();
    };
    if !state.health.runtime_ready.load(Ordering::Relaxed) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "runtime not ready"})),
        )
            .into_response();
    }
    let Some(handle) = registry.handles().into_iter().next() else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "no materialized view registered"})),
        )
            .into_response();
    };
    let mv = handle.name().to_string();

    let cancel = state.cancel.child_token();
    let stream = match execute_mv_changelog(
        registry.as_ref(),
        MvChangelogParams {
            mv_name: mv.clone(),
            with_snapshot: query.with_snapshot.unwrap_or(false),
            as_of: query.as_of,
        },
        cancel.clone(),
    )
    .await
    {
        Ok(stream) => stream,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": err.to_string()})),
            )
                .into_response();
        }
    };

    Sse::new(MvSseStream::new(mv, stream, cancel))
        .keep_alive(KeepAlive::default())
        .into_response()
}

async fn metrics() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    if encoder.encode(&metric_families, &mut buffer).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let mut response = Response::new(Body::from(buffer));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; version=0.0.4"),
    );
    response
}

fn current_unix_time_ms() -> u64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

struct MvSseStream {
    mv_name: String,
    stream: MvChangelogStream,
    cancel: CancellationToken,
    current_batch: Option<MvChangelogBatch>,
    next_row: usize,
    done: bool,
}

impl MvSseStream {
    fn new(mv_name: String, stream: MvChangelogStream, cancel: CancellationToken) -> Self {
        Self {
            mv_name,
            stream,
            cancel,
            current_batch: None,
            next_row: 0,
            done: false,
        }
    }
}

impl Drop for MvSseStream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl Stream for MvSseStream {
    type Item = std::result::Result<Event, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            if let Some(batch) = self.current_batch.as_ref() {
                if self.next_row < batch.batch.num_rows() {
                    let event = encode_sse_changelog_event(&self.mv_name, batch, self.next_row)
                        .unwrap_or_else(error_sse_event);
                    self.next_row += 1;
                    return Poll::Ready(Some(Ok(event)));
                }
                self.current_batch = None;
                self.next_row = 0;
            }

            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    if batch.batch.num_rows() == 0 {
                        continue;
                    }
                    self.current_batch = Some(batch);
                    self.next_row = 0;
                }
                Poll::Ready(Some(Err(err))) => {
                    self.done = true;
                    return Poll::Ready(Some(Ok(error_sse_event(err))));
                }
                Poll::Ready(None) => {
                    self.done = true;
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn encode_sse_changelog_event(
    mv_name: &str,
    batch: &MvChangelogBatch,
    row_idx: usize,
) -> Result<Event> {
    let row = changelog_row_to_json(batch, row_idx)?;
    let data = serde_json::json!({
        "mv": mv_name,
        "version": batch.version,
        "diff": batch.diffs.get(row_idx).copied().unwrap_or(0),
        "time": batch.version_time,
        "row": row,
    });
    Ok(Event::default()
        .event("mv_change")
        .id(format!("{}:{row_idx}", batch.version))
        .json_data(data)?)
}

fn error_sse_event(err: impl std::fmt::Display) -> Event {
    Event::default()
        .event("error")
        .data(serde_json::json!({"error": err.to_string()}).to_string())
}

fn changelog_row_to_json(batch: &MvChangelogBatch, row_idx: usize) -> Result<serde_json::Value> {
    let schema = batch.batch.schema();
    let mut object = serde_json::Map::new();
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array = batch.batch.column(col_idx);
        object.insert(field.name().clone(), array_value_to_json(array, row_idx)?);
    }
    Ok(serde_json::Value::Object(object))
}

fn array_value_to_json(array: &ArrayRef, row_idx: usize) -> Result<serde_json::Value> {
    if array.is_null(row_idx) {
        return Ok(serde_json::Value::Null);
    }
    if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Date32Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Decimal128Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx).to_string()));
    }
    match array.data_type() {
        datafusion::arrow::datatypes::DataType::Timestamp(TimeUnit::Second, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .context("timestamp second array has incompatible type")?;
            Ok(serde_json::Value::from(values.value(row_idx)))
        }
        datafusion::arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .context("timestamp millisecond array has incompatible type")?;
            Ok(serde_json::Value::from(values.value(row_idx)))
        }
        datafusion::arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .context("timestamp microsecond array has incompatible type")?;
            Ok(serde_json::Value::from(values.value(row_idx)))
        }
        datafusion::arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .context("timestamp nanosecond array has incompatible type")?;
            Ok(serde_json::Value::from(values.value(row_idx)))
        }
        other => Ok(serde_json::Value::from(format!(
            "<unsupported Arrow type {other:?}>"
        ))),
    }
}

fn parse_events(value: Value, default_source: Option<&str>) -> Result<Vec<AppendIngestEvent>> {
    match value {
        Value::Array(items) => {
            if items.is_empty() {
                ensure!(false, "event array must not be empty");
            }
            let mut events = Vec::with_capacity(items.len());
            for item in items {
                events.push(parse_event(item, default_source)?);
            }
            Ok(events)
        }
        other => Ok(vec![parse_event(other, default_source)?]),
    }
}

fn parse_event(value: Value, default_source: Option<&str>) -> Result<AppendIngestEvent> {
    let object = value
        .as_object()
        .context("event payload must be a JSON object")?;

    if let (Some(source), Some(payload)) = (object.get("source"), object.get("data")) {
        let source = source.as_str().context("event source must be a string")?;
        ensure!(payload.is_object(), "event payload must be an object");
        return Ok(AppendIngestEvent::new(source, payload.clone()));
    }

    let source = default_source.context("event payload missing source")?;
    Ok(AppendIngestEvent::new(source, value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, header};
    use serde_json::json;
    use tokio::sync::mpsc;
    use tower::util::ServiceExt;

    #[test]
    fn parse_events_accepts_source_wrapped_payload() {
        let value = json!({"source": "nexmark_bid", "data": {"auction": 1}});
        let events = parse_events(value, None).expect("parse events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source(), "nexmark_bid");
    }

    #[test]
    fn parse_events_uses_default_source() {
        let value = json!({"auction": 1});
        let events = parse_events(value, Some("nexmark_bid")).expect("parse events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source(), "nexmark_bid");
    }

    #[tokio::test]
    async fn http_ingest_accepts_events() {
        let (tx, mut rx) = mpsc::channel::<Vec<AppendIngestEvent>>(4);
        let state = HttpIngestState {
            sender: AppendIngestEventSender::Direct {
                sender: tx,
                pending: Default::default(),
            },
            default_source: Some("nexmark_bid".to_string()),
            cancel: CancellationToken::new(),
            health: None,
        };
        let app = Router::new()
            .route("/ingest", post(ingest))
            .with_state(state);

        let payload = json!({"auction": 1});
        let request = Request::builder()
            .method("POST")
            .uri("/ingest")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .expect("request");
        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let batch = rx.recv().await.expect("batch");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].source(), "nexmark_bid");
    }

    #[tokio::test]
    async fn http_ingest_waits_for_commit_ack() {
        let (tx, mut rx) = mpsc::channel(4);
        let state = HttpIngestState {
            sender: AppendIngestEventSender::Routed {
                connector_id: 0,
                sender: tx,
                pending: Default::default(),
            },
            default_source: Some("nexmark_bid".to_string()),
            cancel: CancellationToken::new(),
            health: None,
        };
        let app = Router::new()
            .route("/ingest", post(ingest))
            .with_state(state);

        let payload = json!({"auction": 1});
        let request = Request::builder()
            .method("POST")
            .uri("/ingest")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .expect("request");
        let response_task = tokio::spawn(async move { app.oneshot(request).await });

        let batch = rx.recv().await.expect("batch");
        assert_eq!(batch.events.len(), 1);
        assert!(!response_task.is_finished());
        batch
            .commit_ack
            .expect("commit ack")
            .record_committed()
            .await;

        let response = response_task
            .await
            .expect("response task")
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn healthz_reports_unavailable_when_executor_stops() {
        let (tx, _rx) = mpsc::channel(1);
        let state = HttpIngestState {
            sender: AppendIngestEventSender::Direct {
                sender: tx,
                pending: Default::default(),
            },
            default_source: Some("nexmark_bid".to_string()),
            cancel: CancellationToken::new(),
            health: Some(HttpIngestHealth {
                executor_running: Arc::new(AtomicBool::new(false)),
                storage_reachable: Arc::new(AtomicBool::new(true)),
                runtime_ready: Arc::new(AtomicBool::new(true)),
                watermark_debug: None,
                cdc_replication_debug: None,
            }),
        };
        let app = Router::new()
            .route("/healthz", get(healthz))
            .route("/readyz", get(readyz))
            .with_state(state);
        let request = Request::builder()
            .method("GET")
            .uri("/healthz")
            .body(Body::empty())
            .expect("request");
        let response = app.clone().oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let request = Request::builder()
            .method("GET")
            .uri("/readyz")
            .body(Body::empty())
            .expect("request");
        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn debug_watermarks_returns_snapshot() {
        let (tx, _rx) = mpsc::channel(1);
        let snapshot = Arc::new(RwLock::new(WatermarkDebugState {
            global_watermark_ms: Some(42),
            policy: "min_active_sources".to_string(),
            updated_at_unix_ms: 7,
            sources: vec![WatermarkDebugSourceState {
                source: "s1".to_string(),
                watermark_ms: 42,
                idle: false,
            }],
        }));
        let state = HttpIngestState {
            sender: AppendIngestEventSender::Direct {
                sender: tx,
                pending: Default::default(),
            },
            default_source: Some("nexmark_bid".to_string()),
            cancel: CancellationToken::new(),
            health: Some(HttpIngestHealth {
                executor_running: Arc::new(AtomicBool::new(true)),
                storage_reachable: Arc::new(AtomicBool::new(true)),
                runtime_ready: Arc::new(AtomicBool::new(true)),
                watermark_debug: Some(snapshot),
                cdc_replication_debug: None,
            }),
        };
        let app = Router::new()
            .route("/debug/watermarks", get(debug_watermarks_ingest))
            .with_state(state);
        let request = Request::builder()
            .method("GET")
            .uri("/debug/watermarks")
            .body(Body::empty())
            .expect("request");
        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn debug_cdc_replication_admin_returns_snapshot() {
        let snapshot = Arc::new(RwLock::new(CdcReplicationDebugState {
            updated_at_unix_ms: 9,
            refresh_error: None,
            postgres_sources: vec![PostgresCdcDebugSourceState {
                source: "pg_main".to_string(),
                slot: Some("slot_main".to_string()),
                schema_evolution_policy: "ignore_compatible".to_string(),
                upstream_lsn: Some("0/16B6C80".to_string()),
                upstream_lsn_bytes: Some(23_817_344),
                durable_lsn: Some("0/16B6C50".to_string()),
                durable_lsn_bytes: Some(23_817_296),
                source_lag_bytes: Some(48),
                latest_schema_evolution: Some(PostgresCdcSchemaEvolutionDebugState {
                    table: "orders".to_string(),
                    upstream_table: "public.orders".to_string(),
                    policy: "ignore_compatible".to_string(),
                    outcome: "compatible_addition".to_string(),
                    added_columns: vec!["note".to_string()],
                    reason: None,
                    catalog_schema_version: 1,
                    observed_schema_version: 2,
                    observed_at_unix_ms: 8,
                }),
            }],
            pipelines: vec![CdcReplicationDebugPipelineState {
                pipeline: "orders_pipe".to_string(),
                source: "pg_main".to_string(),
                schema_evolution_policy: "ignore_compatible".to_string(),
                error_policy: "retry_with_backoff".to_string(),
                target_kind: "kafka".to_string(),
                checkpoint_position: Some("pg/0/16B6C50".to_string()),
                checkpoint_lsn_bytes: Some(23_817_296),
                checkpoint_lag_bytes: Some(48),
                checkpoint_transaction_id: Some("pg-xid-77".to_string()),
                target_state: BTreeMap::from([(
                    "target.delivery.status".to_string(),
                    "pending".to_string(),
                )]),
                pending_transactions: 1,
                pending_objects: 1,
                pending_records: 2,
                pending_bytes: 3,
                oldest_pending_age_ms: Some(4),
                dlq_pending_entries: 5,
                dlq_replayed_entries: 6,
                dlq_discarded_entries: 7,
                oldest_dlq_pending_age_ms: Some(8),
                missing_payload_objects: 0,
                orphan_payload_objects: 0,
                orphan_payload_bytes: 0,
                replaying: true,
                source_backpressure_active: true,
                last_error: Some("kafka unavailable".to_string()),
            }],
        }));
        let state = HttpAdminState {
            cancel: CancellationToken::new(),
            health: HttpIngestHealth {
                executor_running: Arc::new(AtomicBool::new(true)),
                storage_reachable: Arc::new(AtomicBool::new(true)),
                runtime_ready: Arc::new(AtomicBool::new(true)),
                watermark_debug: None,
                cdc_replication_debug: Some(snapshot),
            },
            storage_db: None,
            storage_catalog: None,
            replication_runtime: None,
            materialized_views: None,
        };
        let app = Router::new()
            .route("/debug/cdc/replication", get(debug_cdc_replication_admin))
            .route("/ops/cdc/replication", get(debug_cdc_replication_admin))
            .with_state(state);
        let request = Request::builder()
            .method("GET")
            .uri("/ops/cdc/replication")
            .body(Body::empty())
            .expect("request");
        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body = String::from_utf8(body.to_vec()).expect("utf8 body");
        assert!(body.contains("orders_pipe"));
        assert!(!body.contains("postgres://"));
        assert!(!body.contains("connection"));
    }

    #[tokio::test]
    async fn admin_cdc_replication_dlq_lists_inspects_and_discards_entries() {
        let storage = SlateCatalog::in_memory().await.expect("storage");
        let dlq_entry = ReplicationPipelineDlqEntry::new(
            "orders_pipe",
            "entry-1",
            "pg_main",
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).expect("position"),
            Some(floe_cdc_core::CdcTransactionId::new("pg-xid-1").expect("transaction")),
            "postgres_delivery",
            "permission denied",
            1,
            Some("payloads/entry-1.bin".to_string()),
            Some("kafka_records".to_string()),
            128,
            BTreeMap::from([(
                "target.delivery.status".to_string(),
                "dead_lettered".to_string(),
            )]),
            current_unix_time_ms(),
        )
        .expect("dlq entry");
        storage
            .put_replication_pipeline_dlq_entry(dlq_entry)
            .await
            .expect("persist dlq entry");
        let second_entry = ReplicationPipelineDlqEntry::new(
            "orders_pipe",
            "entry-2",
            "pg_main",
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C60", None).expect("position"),
            Some(floe_cdc_core::CdcTransactionId::new("pg-xid-2").expect("transaction")),
            "postgres_delivery",
            "permission denied",
            1,
            Some("payloads/entry-2.bin".to_string()),
            Some("kafka_records".to_string()),
            128,
            BTreeMap::from([(
                "target.delivery.status".to_string(),
                "dead_lettered".to_string(),
            )]),
            current_unix_time_ms().saturating_add(1),
        )
        .expect("second dlq entry");
        storage
            .put_replication_pipeline_dlq_entry(second_entry)
            .await
            .expect("persist second dlq entry");
        let state = HttpAdminState {
            cancel: CancellationToken::new(),
            health: HttpIngestHealth {
                executor_running: Arc::new(AtomicBool::new(true)),
                storage_reachable: Arc::new(AtomicBool::new(true)),
                runtime_ready: Arc::new(AtomicBool::new(true)),
                watermark_debug: None,
                cdc_replication_debug: None,
            },
            storage_db: None,
            storage_catalog: Some(Arc::new(storage.clone())),
            replication_runtime: None,
            materialized_views: None,
        };
        let app = Router::new()
            .route(
                "/debug/cdc/replication/dlq",
                get(debug_cdc_replication_dlq_list_admin),
            )
            .route(
                "/ops/cdc/replication/dlq",
                get(debug_cdc_replication_dlq_list_admin),
            )
            .route(
                "/debug/cdc/replication/dlq/retry",
                post(debug_cdc_replication_dlq_retry_batch_admin),
            )
            .route(
                "/ops/cdc/replication/dlq/retry",
                post(debug_cdc_replication_dlq_retry_batch_admin),
            )
            .route(
                "/debug/cdc/replication/dlq/:pipeline/:dlq_id",
                get(debug_cdc_replication_dlq_entry_admin),
            )
            .route(
                "/ops/cdc/replication/dlq/:pipeline/:dlq_id",
                get(debug_cdc_replication_dlq_entry_admin),
            )
            .route(
                "/debug/cdc/replication/dlq/:pipeline/:dlq_id/discard",
                post(debug_cdc_replication_dlq_discard_admin),
            )
            .route(
                "/ops/cdc/replication/dlq/:pipeline/:dlq_id/discard",
                post(debug_cdc_replication_dlq_discard_admin),
            )
            .route(
                "/ops/cdc/replication/dlq/:pipeline/:dlq_id/retry",
                post(debug_cdc_replication_dlq_retry_admin),
            )
            .with_state(state);

        let request = Request::builder()
            .method("GET")
            .uri("/ops/cdc/replication/dlq?pipeline=orders_pipe&status=pending&offset=1&limit=1")
            .body(Body::empty())
            .expect("request");
        let response = app.clone().oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("json body");
        assert_eq!(value["offset"], 1);
        assert_eq!(value["limit"], 1);
        assert_eq!(value["total_matching"], 2);
        assert_eq!(value["count"], 1);
        assert_eq!(value["entries"][0]["dlq_id"], "entry-2");

        let request = Request::builder()
            .method("GET")
            .uri("/ops/cdc/replication/dlq/orders_pipe/entry-1")
            .body(Body::empty())
            .expect("request");
        let response = app.clone().oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let request = Request::builder()
            .method("POST")
            .uri("/ops/cdc/replication/dlq/retry?pipeline=orders_pipe&limit=0")
            .body(Body::empty())
            .expect("request");
        let response = app.clone().oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let request = Request::builder()
            .method("POST")
            .uri("/ops/cdc/replication/dlq/orders_pipe/entry-1/retry")
            .body(Body::empty())
            .expect("request");
        let response = app.clone().oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let payload = json!({
            "reason": "operator confirmed duplicate",
            "operator": "ops@example.com"
        });
        let request = Request::builder()
            .method("POST")
            .uri("/ops/cdc/replication/dlq/orders_pipe/entry-1/discard")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .expect("request");
        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let discarded = storage
            .replication_pipeline_dlq_entry("orders_pipe", "entry-1")
            .await
            .expect("load entry")
            .expect("entry exists");
        assert_eq!(discarded.status(), ReplicationPipelineDlqStatus::Discarded);
        assert_eq!(
            discarded.status_reason(),
            Some("operator confirmed duplicate (operator: ops@example.com)")
        );
    }

    #[tokio::test]
    async fn admin_mv_endpoint_selects_single_registered_mv() {
        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.register("mv_one");
        let state = HttpAdminState {
            cancel: CancellationToken::new(),
            health: HttpIngestHealth {
                executor_running: Arc::new(AtomicBool::new(true)),
                storage_reachable: Arc::new(AtomicBool::new(true)),
                runtime_ready: Arc::new(AtomicBool::new(true)),
                watermark_debug: None,
                cdc_replication_debug: None,
            },
            storage_db: None,
            storage_catalog: None,
            replication_runtime: None,
            materialized_views: Some(registry),
        };
        let app = Router::new()
            .route("/mv", get(subscribe_sse_admin))
            .with_state(state);

        let request = Request::builder()
            .method("GET")
            .uri("/mv")
            .body(Body::empty())
            .expect("request");
        let response = app.oneshot(request).await.expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
