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
use serde::Deserialize;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use floe_node_core::source::{SourceEvent, SourceEventSender};

#[derive(Debug, Clone)]
pub struct HttpIngestConfig {
    pub host: String,
    pub port: u16,
    pub default_source: Option<String>,
    pub health: Option<HttpIngestHealth>,
}

#[derive(Debug, Clone)]
pub struct HttpAdminConfig {
    pub host: String,
    pub port: u16,
    pub health: HttpIngestHealth,
}

#[derive(Debug, Clone)]
pub struct HttpIngestHealth {
    pub executor_running: Arc<AtomicBool>,
    pub storage_reachable: Arc<AtomicBool>,
    pub runtime_ready: Arc<AtomicBool>,
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
    };
    let app = Router::new()
        .route("/healthz", get(admin_healthz))
        .route("/readyz", get(admin_readyz))
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

    for event in events {
        state.sender.send(event).await.map_err(|err| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("ingest channel closed: {err}"),
            )
        })?;
    }

    Ok(StatusCode::ACCEPTED)
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
        let (tx, mut rx) = mpsc::channel(4);
        let state = HttpIngestState {
            sender: tx,
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
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let event = rx.recv().await.expect("event");
        assert_eq!(event.source(), "nexmark_bid");
    }

    #[tokio::test]
    async fn healthz_reports_unavailable_when_executor_stops() {
        let (tx, _rx) = mpsc::channel(1);
        let state = HttpIngestState {
            sender: tx,
            default_source: Some("nexmark_bid".to_string()),
            cancel: CancellationToken::new(),
            health: Some(HttpIngestHealth {
                executor_running: Arc::new(AtomicBool::new(false)),
                storage_reachable: Arc::new(AtomicBool::new(true)),
                runtime_ready: Arc::new(AtomicBool::new(true)),
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
}
