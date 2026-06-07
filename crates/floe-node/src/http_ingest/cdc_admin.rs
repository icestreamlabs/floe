use super::*;

pub(super) async fn debug_cdc_replication_admin(
    State(state): State<HttpAdminState>,
) -> impl IntoResponse {
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

pub(super) async fn debug_cdc_replication_dlq_list_admin(
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

pub(super) async fn debug_cdc_replication_dlq_entry_admin(
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

pub(super) async fn ops_cdc_replication_dlq_discard_admin(
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

pub(super) async fn ops_cdc_replication_dlq_retry_admin(
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

pub(super) async fn ops_cdc_replication_dlq_retry_batch_admin(
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

pub(super) async fn ops_cdc_replication_reconcile_admin(
    State(state): State<HttpAdminState>,
    Path(pipeline): Path<String>,
    Query(query): Query<CdcReplicationReconcileQuery>,
) -> impl IntoResponse {
    let max_rows = query
        .max_rows
        .unwrap_or(DEFAULT_CDC_REPLICATION_RECONCILE_MAX_ROWS);
    if max_rows == 0 || max_rows > MAX_CDC_REPLICATION_RECONCILE_MAX_ROWS {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!(
                    "reconcile max_rows must be between 1 and {}",
                    MAX_CDC_REPLICATION_RECONCILE_MAX_ROWS
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
    match runtime
        .reconcile_pipeline(
            storage,
            &pipeline,
            ReplicationPipelineReconciliationOptions {
                max_rows,
                full_scan: query.full_scan.unwrap_or(false),
            },
        )
        .await
    {
        Ok(Some(report)) => (StatusCode::OK, Json(report)).into_response(),
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

pub(super) fn cdc_replication_action_reason(
    request: CdcReplicationDlqActionRequest,
) -> Option<String> {
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
