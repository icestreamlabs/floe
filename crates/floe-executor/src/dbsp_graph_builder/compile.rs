use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, anyhow};
use dbsp::circuit::plan::DbspProjectExpr;
use dbsp::handles::ZSetHandle;
use dbsp::stream::{DeltaHandleStream, StreamCursor};
use dbsp::stream::runtime::RuntimeErrorHandler;
use dbsp::{
    DbspFilter, DbspJoin, DbspJoinNode, DbspMap, DbspProjectNode, DbspSelectNode,
    DbspSourceNode,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::dbsp_bridge::DbspBridge;
use crate::encoding::{decode_projected_row_key, encode_projected_row_key};
use crate::task_events::{GraphTaskSender, report_graph_task_error};

use super::builder::DbspGraphBuilder;
use super::eval::{eval_expression, eval_predicate, eval_projection, resolve_join_key_indices};

impl DbspGraphBuilder {
    pub(super) async fn compile_source(
        &self,
        source: &DbspSourceNode,
        outer_streams: &HashMap<String, DeltaHandleStream>,
    ) -> Result<DeltaHandleStream> {
        tracing::info!(
            source = %source.table.name,
            "attaching DBSP source node to outer stream"
        );
        let snapshot_stream = outer_streams
            .get(source.table.name)
            .cloned()
            .with_context(|| anyhow!("source '{}' has no handle stream", source.table.name))?;
        Ok(snapshot_stream)
    }

    pub(super) async fn compile_filter(
        &mut self,
        node: &DbspSelectNode,
        upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let predicate = node.predicate().clone();
        let schema = Arc::clone(node.output_schema());
        let graph_id = self.graph_id().to_string();
        let task_events = task_events.clone();
        let task_label = format!("filter:{graph_id}");
        let error_graph_id = graph_id.clone();
        let error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&task_events, &error_graph_id, task_label.clone(), err);
        });
        let log_graph_id = graph_id.clone();
        let filter_pred = move |bytes: &Vec<u8>| -> bool {
            let row = match decode_projected_row_key(bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %log_graph_id,
                        error = %err,
                        "failed to decode filter row"
                    );
                    return false;
                }
            };
            match eval_predicate(&predicate, &row, schema.as_ref()) {
                Ok(result) => result,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %log_graph_id,
                        error = %err,
                        "failed to evaluate filter predicate"
                    );
                    false
                }
            }
        };
        let filter = DbspFilter::new::<Vec<u8>, _>(&upstream, filter_pred, Some(error_handler))
            .await
            .context("initialize DBSP filter")?;
        Ok(filter.stream())
    }

    pub(super) async fn compile_map(
        &mut self,
        node: &DbspProjectNode,
        upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let expressions: Arc<Vec<DbspProjectExpr>> = Arc::new(node.expressions().to_vec());
        let schema = Arc::clone(node.input_schema());
        let graph_id = self.graph_id().to_string();
        let task_events = task_events.clone();
        let task_label = format!("map:{graph_id}");
        let error_graph_id = graph_id.clone();
        let error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&task_events, &error_graph_id, task_label.clone(), err);
        });
        let log_graph_id = graph_id.clone();
        let projector = move |bytes: &Vec<u8>| -> Vec<u8> {
            let row = match decode_projected_row_key(bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %log_graph_id,
                        error = %err,
                        "failed to decode projection row"
                    );
                    return Vec::new();
                }
            };
            let projected = match eval_projection(expressions.as_ref(), &row, schema.as_ref()) {
                Ok(projected) => projected,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %log_graph_id,
                        error = %err,
                        "failed to evaluate projection"
                    );
                    return Vec::new();
                }
            };
            match encode_projected_row_key(&projected) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %log_graph_id,
                        error = %err,
                        "failed to encode projected row"
                    );
                    Vec::new()
                }
            }
        };
        let map = DbspMap::new::<Vec<u8>, Vec<u8>, _>(&upstream, projector, Some(error_handler))
            .await
            .context("initialize DBSP map")?;
        Ok(map.stream())
    }

    pub(super) async fn compile_join(
        &mut self,
        node: &DbspJoinNode,
        left: DeltaHandleStream,
        right: DeltaHandleStream,
        cancel: &CancellationToken,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let left_schema = Arc::clone(&node.left_schema);
        let right_schema = Arc::clone(&node.right_schema);
        let residual = node.residual.clone();
        let output_schema = Arc::clone(&node.output_schema);
        let graph_id = self.graph_id().to_string();
        let join_events = task_events.clone();
        let join_graph_id = graph_id.clone();
        let join_label = format!("join:{graph_id}");
        let join_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&join_events, &join_graph_id, join_label.clone(), err);
        });

        let left_log_limit = Arc::new(AtomicUsize::new(3));
        let right_log_limit = Arc::new(AtomicUsize::new(3));

        let mut left_cursor = StreamCursor::new(left.stream());
        let mut right_cursor = StreamCursor::new(right.stream());
        if let Ok((ts, handle)) = left_cursor.snapshot().await
            && left_log_limit.fetch_sub(1, Ordering::Relaxed) > 0
        {
            tracing::debug!(
                graph_id = %graph_id,
                ts,
                handle_version = handle.version,
                schema_width = left_schema.len(),
                "join left snapshot"
            );
            log_handle_rows("left snapshot", &handle, &self.bridge).await?;
        }
        if let Ok((ts, handle)) = right_cursor.snapshot().await
            && right_log_limit.fetch_sub(1, Ordering::Relaxed) > 0
        {
            tracing::debug!(
                graph_id = %graph_id,
                ts,
                handle_version = handle.version,
                schema_width = right_schema.len(),
                "join right snapshot"
            );
            log_handle_rows("right snapshot", &handle, &self.bridge).await?;
        }
        let left_log_limit_clone = Arc::clone(&left_log_limit);
        let left_schema_clone = Arc::clone(&left_schema);
        let bridge_clone = Arc::clone(&self.bridge);
        let left_task_events = task_events.clone();
        let left_graph_id = graph_id.clone();
        let left_task_label = "join-left-logger".to_string();
        let cancel_left = cancel.clone();
        tokio::spawn(async move {
            let mut cursor = left_cursor;
            loop {
                tokio::select! {
                    _ = cancel_left.cancelled() => break,
                    result = cursor.next() => {
                        let (ts, handle) = match result {
                            Ok(next) => next,
                            Err(err) => {
                                report_graph_task_error(
                                    &left_task_events,
                                    &left_graph_id,
                                    left_task_label.clone(),
                                    anyhow!("join left handle stream closed: {err}"),
                                );
                                break;
                            }
                        };
                        if left_log_limit_clone.fetch_sub(1, Ordering::Relaxed) > 0 {
                            tracing::debug!(
                                graph_id = %left_graph_id,
                                ts,
                                handle_version = handle.version,
                                schema_width = left_schema_clone.len(),
                                "join left handle"
                            );
                            if let Err(err) = log_handle_rows("left handle", &handle, &bridge_clone).await {
                                report_graph_task_error(
                                    &left_task_events,
                                    &left_graph_id,
                                    left_task_label.clone(),
                                    anyhow!("failed to log left handle rows: {err}"),
                                );
                                break;
                            }
                        }
                    }
                }
            }
        });
        let right_log_limit_clone = Arc::clone(&right_log_limit);
        let right_schema_clone = Arc::clone(&right_schema);
        let bridge_clone = Arc::clone(&self.bridge);
        let right_task_events = task_events.clone();
        let right_graph_id = graph_id.clone();
        let right_task_label = "join-right-logger".to_string();
        let cancel_right = cancel.clone();
        tokio::spawn(async move {
            let mut cursor = right_cursor;
            loop {
                tokio::select! {
                    _ = cancel_right.cancelled() => break,
                    result = cursor.next() => {
                        let (ts, handle) = match result {
                            Ok(next) => next,
                            Err(err) => {
                                report_graph_task_error(
                                    &right_task_events,
                                    &right_graph_id,
                                    right_task_label.clone(),
                                    anyhow!("join right handle stream closed: {err}"),
                                );
                                break;
                            }
                        };
                        if right_log_limit_clone.fetch_sub(1, Ordering::Relaxed) > 0 {
                            tracing::debug!(
                                graph_id = %right_graph_id,
                                ts,
                                handle_version = handle.version,
                                schema_width = right_schema_clone.len(),
                                "join right handle"
                            );
                            if let Err(err) = log_handle_rows("right handle", &handle, &bridge_clone).await
                            {
                                report_graph_task_error(
                                    &right_task_events,
                                    &right_graph_id,
                                    right_task_label.clone(),
                                    anyhow!("failed to log right handle rows: {err}"),
                                );
                                break;
                            }
                        }
                    }
                }
            }
        });

        let key_indices =
            resolve_join_key_indices(&node.keys, left_schema.as_ref(), right_schema.as_ref())
                .context("resolve join key indices")?;
        let key_indices = Arc::new(key_indices);
        let left_key_indices = Arc::clone(&key_indices);
        let right_key_indices = Arc::clone(&key_indices);
        let left_graph_id = graph_id.clone();
        let right_graph_id = graph_id.clone();
        let predicate_graph_id = graph_id.clone();
        let projector_graph_id = graph_id.clone();

        let left_key = move |left_bytes: &Vec<u8>| -> Option<Vec<u8>> {
            let left_row = match decode_projected_row_key(left_bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %left_graph_id,
                        error = %err,
                        "failed to decode join left key"
                    );
                    return None;
                }
            };
            let mut key_columns = Vec::with_capacity(left_key_indices.len());
            for (li, _) in left_key_indices.iter() {
                let value = match left_row.get(*li) {
                    Some(value) => value.clone(),
                    None => {
                        tracing::warn!(
                            graph_id = %left_graph_id,
                            index = *li,
                            "join left key index out of bounds"
                        );
                        return None;
                    }
                };
                if value.is_null() {
                    return None;
                }
                key_columns.push(value);
            }
            match encode_projected_row_key(&key_columns) {
                Ok(encoded) => Some(encoded),
                Err(err) => {
                    tracing::warn!(
                        graph_id = %left_graph_id,
                        error = %err,
                        "failed to encode join left key"
                    );
                    None
                }
            }
        };

        let right_key = move |right_bytes: &Vec<u8>| -> Option<Vec<u8>> {
            let right_row = match decode_projected_row_key(right_bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %right_graph_id,
                        error = %err,
                        "failed to decode join right key"
                    );
                    return None;
                }
            };
            let mut key_columns = Vec::with_capacity(right_key_indices.len());
            for (_, ri) in right_key_indices.iter() {
                let value = match right_row.get(*ri) {
                    Some(value) => value.clone(),
                    None => {
                        tracing::warn!(
                            graph_id = %right_graph_id,
                            index = *ri,
                            "join right key index out of bounds"
                        );
                        return None;
                    }
                };
                if value.is_null() {
                    return None;
                }
                key_columns.push(value);
            }
            match encode_projected_row_key(&key_columns) {
                Ok(encoded) => Some(encoded),
                Err(err) => {
                    tracing::warn!(
                        graph_id = %right_graph_id,
                        error = %err,
                        "failed to encode join right key"
                    );
                    None
                }
            }
        };

        let predicate = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> bool {
            let Some(expr) = residual.as_ref() else {
                return true;
            };
            let left_row = match decode_projected_row_key(left_bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %predicate_graph_id,
                        error = %err,
                        "failed to decode join left row"
                    );
                    return false;
                }
            };
            let right_row = match decode_projected_row_key(right_bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %predicate_graph_id,
                        error = %err,
                        "failed to decode join right row"
                    );
                    return false;
                }
            };
            let mut combined = Vec::with_capacity(left_row.len() + right_row.len());
            combined.extend(left_row.into_iter());
            combined.extend(right_row.into_iter());
            match eval_expression(expr, &combined, output_schema.as_ref()) {
                Ok(result) => result,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %predicate_graph_id,
                        error = %err,
                        "failed to evaluate join residual"
                    );
                    false
                }
            }
        };

        let projector = move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
            let mut combined = match decode_projected_row_key(left_bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %projector_graph_id,
                        error = %err,
                        "failed to decode join projection left row"
                    );
                    return Vec::new();
                }
            };
            let right_row = match decode_projected_row_key(right_bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %projector_graph_id,
                        error = %err,
                        "failed to decode join projection right row"
                    );
                    return Vec::new();
                }
            };
            combined.extend(right_row);
            match encode_projected_row_key(&combined) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %projector_graph_id,
                        error = %err,
                        "failed to encode join projection row"
                    );
                    Vec::new()
                }
            }
        };

        let join = DbspJoin::new::<Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, _, _, _, _>(
            &left,
            &right,
            left_key,
            right_key,
            predicate,
            projector,
            Some(join_error_handler),
        )
        .await
        .context("initialize DBSP join")?;
        // Log the first output handle, if any, to verify join activity.
        let mut join_cursor = StreamCursor::new(join.stream().stream());
        if let Ok((ts, handle)) = join_cursor.snapshot().await {
            tracing::debug!(
                graph_id = %graph_id,
                ts,
                handle_version = handle.version,
                "join output snapshot"
            );
            log_handle_rows("join output snapshot", &handle, &self.bridge).await?;
        }
        Ok(join.stream())
    }
}

async fn log_handle_rows(
    label: &str,
    handle: &ZSetHandle,
    bridge: &Arc<Mutex<DbspBridge>>,
) -> Result<()> {
    let mut guard = bridge.lock().await;
    let handle_view = guard
        .handle_view_for(&handle.ns, handle.version)
        .await
        .context("open handle view for logging")?;
    let materialized = handle_view.materialize().await?;
    let total = materialized.len();
    let mut rows = Vec::new();
    for (row, diff) in materialized.into_iter().take(3) {
        let decoded = decode_projected_row_key(&row);
        rows.push((decoded, diff));
    }
    tracing::debug!(
        label,
        row_count = total,
        first_rows = ?rows,
        "handle rows"
    );
    Ok(())
}
