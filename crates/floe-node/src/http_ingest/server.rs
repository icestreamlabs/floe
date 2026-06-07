use super::*;

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
            "/ops/cdc/replication/dlq/retry",
            post(ops_cdc_replication_dlq_retry_batch_admin),
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
            "/ops/cdc/replication/dlq/:pipeline/:dlq_id/discard",
            post(ops_cdc_replication_dlq_discard_admin),
        )
        .route(
            "/ops/cdc/replication/dlq/:pipeline/:dlq_id/retry",
            post(ops_cdc_replication_dlq_retry_admin),
        )
        .route(
            "/ops/cdc/replication/:pipeline/reconcile",
            post(ops_cdc_replication_reconcile_admin),
        )
        .route("/ops/storage/flush", post(ops_storage_flush_admin))
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

pub(super) async fn ingest(
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

pub(super) async fn healthz(State(state): State<HttpIngestState>) -> impl IntoResponse {
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

pub(super) async fn readyz(State(state): State<HttpIngestState>) -> impl IntoResponse {
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

pub(super) async fn admin_healthz(State(state): State<HttpAdminState>) -> impl IntoResponse {
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

pub(super) async fn admin_readyz(State(state): State<HttpAdminState>) -> impl IntoResponse {
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

pub(super) async fn debug_watermarks_ingest(
    State(state): State<HttpIngestState>,
) -> impl IntoResponse {
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

pub(super) async fn debug_watermarks_admin(
    State(state): State<HttpAdminState>,
) -> impl IntoResponse {
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
