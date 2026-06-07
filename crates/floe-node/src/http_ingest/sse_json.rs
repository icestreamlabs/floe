use super::*;

pub(super) async fn ops_storage_flush_admin(
    State(state): State<HttpAdminState>,
) -> impl IntoResponse {
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

pub(super) async fn subscribe_sse_admin(
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
    let handle = match query.mv.as_deref() {
        Some(mv_name) => match registry.get(mv_name) {
            Some(handle) => handle,
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("materialized view '{mv_name}' not found")})),
                )
                    .into_response();
            }
        },
        None => {
            let handles = registry.handles();
            match handles.len() {
                0 => {
                    return (
                        StatusCode::NOT_FOUND,
                        Json(serde_json::json!({"error": "no materialized view registered"})),
                    )
                        .into_response();
                }
                1 => handles.into_iter().next().expect("one MV handle"),
                _ => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({
                            "error": "mv query parameter is required when multiple materialized views are registered"
                        })),
                    )
                    .into_response();
                }
            }
        }
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

pub(super) async fn metrics() -> impl IntoResponse {
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

pub(super) fn current_unix_time_ms() -> u64 {
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

pub(super) fn encode_sse_changelog_event(
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

pub(super) fn error_sse_event(err: impl std::fmt::Display) -> Event {
    Event::default()
        .event("error")
        .data(serde_json::json!({"error": err.to_string()}).to_string())
}

pub(super) fn changelog_row_to_json(
    batch: &MvChangelogBatch,
    row_idx: usize,
) -> Result<serde_json::Value> {
    let schema = batch.batch.schema();
    crate::arrow_json::record_batch_row_to_json(&batch.batch, row_idx, &schema)
}

pub(super) fn parse_events(
    value: Value,
    default_source: Option<&str>,
) -> Result<Vec<AppendIngestEvent>> {
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

pub(super) fn parse_event(value: Value, default_source: Option<&str>) -> Result<AppendIngestEvent> {
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
