use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result, ensure};
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use prometheus::{Encoder, TextEncoder};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use slatedb::Db;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use floe_node_core::source::{SourceEvent, SourceEventSender, send_batch_with_commit_ack};

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
}

#[derive(Debug, Clone)]
pub struct HttpIngestHealth {
    pub executor_running: Arc<AtomicBool>,
    pub storage_reachable: Arc<AtomicBool>,
    pub runtime_ready: Arc<AtomicBool>,
    pub watermark_debug: Option<Arc<RwLock<WatermarkDebugState>>>,
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

#[derive(Clone)]
struct HttpIngestState {
    sender: SourceEventSender,
    default_source: Option<String>,
    cancel: CancellationToken,
    health: Option<HttpIngestHealth>,
}

#[derive(Clone)]
struct HttpAdminState {
    cancel: CancellationToken,
    health: HttpIngestHealth,
    storage_db: Option<Arc<Db>>,
}

#[derive(Deserialize)]
struct IngestQuery {
    source: Option<String>,
}

pub async fn run_http_ingest(
    config: HttpIngestConfig,
    sender: SourceEventSender,
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
    };
    let app = Router::new()
        .route("/healthz", get(admin_healthz))
        .route("/readyz", get(admin_readyz))
        .route("/debug/watermarks", get(debug_watermarks_admin))
        .route("/debug/storage/flush", post(debug_storage_flush_admin))
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

fn parse_events(value: Value, default_source: Option<&str>) -> Result<Vec<SourceEvent>> {
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

fn parse_event(value: Value, default_source: Option<&str>) -> Result<SourceEvent> {
    let object = value
        .as_object()
        .context("event payload must be a JSON object")?;

    if let (Some(source), Some(payload)) = (object.get("source"), object.get("data")) {
        let source = source.as_str().context("event source must be a string")?;
        ensure!(payload.is_object(), "event payload must be an object");
        return Ok(SourceEvent::new(source, payload.clone()));
    }

    let source = default_source.context("event payload missing source")?;
    Ok(SourceEvent::new(source, value))
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
        let (tx, mut rx) = mpsc::channel::<Vec<SourceEvent>>(4);
        let state = HttpIngestState {
            sender: SourceEventSender::Direct {
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
            sender: SourceEventSender::Routed {
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
            sender: SourceEventSender::Direct {
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
            sender: SourceEventSender::Direct {
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
}
