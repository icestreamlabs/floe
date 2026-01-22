use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, anyhow};
use datafusion::scalar::ScalarValue;
use dbsp::circuit::plan::DbspProjectExpr;
use dbsp::handles::ZSetHandle;
use dbsp::stream::{DeltaHandleStream, StreamCursor};
use dbsp::stream::runtime::RuntimeErrorHandler;
use dbsp::{
    DbspAggregate, DbspAggregateFunction, DbspAggregateNode, DbspFilter, DbspJoin, DbspJoinNode,
    DbspMap, DbspProjectNode, DbspScalarType, DbspSelectNode, DbspSourceNode,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::dbsp_bridge::DbspBridge;
use crate::encoding::{decode_projected_row_key, encode_projected_row_key};
use crate::task_events::{GraphTaskSender, report_graph_task_error};

use super::builder::DbspGraphBuilder;
use super::eval::{
    eval_expression, eval_predicate, eval_projection, eval_scalar_expression,
    resolve_join_key_indices,
};

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

    pub(super) async fn compile_aggregate(
        &mut self,
        node: &DbspAggregateNode,
        upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let input_schema = Arc::clone(node.input_schema());
        let group_keys = node.group_keys().to_vec();
        let aggregates = node.aggregates().to_vec();
        let graph_id = self.graph_id().to_string();
        let aggregate_events = task_events.clone();
        let aggregate_label = format!("aggregate:{graph_id}");
        let aggregate_graph_id = graph_id.clone();
        let aggregate_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &aggregate_events,
                &aggregate_graph_id,
                aggregate_label.clone(),
                err,
            );
        });

        let key_schema = Arc::clone(&input_schema);
        let key_graph_id = graph_id.clone();
        let key_extractor = move |bytes: &Vec<u8>| -> Option<Vec<u8>> {
            let row = match decode_projected_row_key(bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %key_graph_id,
                        error = %err,
                        "failed to decode aggregate row for group key"
                    );
                    return None;
                }
            };
            let mut key_values = Vec::with_capacity(group_keys.len());
            for key_expr in &group_keys {
                let value = match eval_scalar_expression(
                    key_expr.expression(),
                    &row,
                    key_schema.as_ref(),
                ) {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %key_graph_id,
                            error = %err,
                            "failed to evaluate aggregate group key expression"
                        );
                        return None;
                    }
                };
                key_values.push(value);
            }
            match encode_projected_row_key(&key_values) {
                Ok(encoded) => Some(encoded),
                Err(err) => {
                    tracing::warn!(
                        graph_id = %key_graph_id,
                        error = %err,
                        "failed to encode aggregate group key"
                    );
                    None
                }
            }
        };

        let agg_schema = Arc::clone(&input_schema);
        let agg_graph_id = graph_id.clone();
        let aggregator = move |_key: &Vec<u8>, values: &[(Vec<u8>, i64)]| -> Option<Vec<u8>> {
            if values.is_empty() {
                return None;
            }
            let mut decoded = Vec::with_capacity(values.len());
            for (value, weight) in values {
                if *weight == 0 {
                    continue;
                }
                match decode_projected_row_key(value) {
                    Ok(row) => decoded.push((row, *weight)),
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %agg_graph_id,
                            error = %err,
                            "failed to decode aggregate input row"
                        );
                    }
                }
            }
            if decoded.is_empty() {
                return None;
            }

            let mut outputs = Vec::with_capacity(aggregates.len());
            for agg in &aggregates {
                let output = match agg.function() {
                    DbspAggregateFunction::Count => {
                        let mut count = 0i64;
                        match agg.expression() {
                            Some(expr) => {
                                for (row, weight) in &decoded {
                                    match eval_scalar_expression(expr, row, agg_schema.as_ref()) {
                                        Ok(value) => {
                                            if !value.is_null() {
                                                count += *weight;
                                            }
                                        }
                                        Err(err) => {
                                            tracing::warn!(
                                                graph_id = %agg_graph_id,
                                                error = %err,
                                                "failed to evaluate count expression"
                                            );
                                        }
                                    }
                                }
                            }
                            None => {
                                for (_, weight) in &decoded {
                                    count += *weight;
                                }
                            }
                        }
                        ScalarValue::Int64(Some(count))
                    }
                    DbspAggregateFunction::Sum => {
                        if let Some(expr) = agg.expression() {
                            let mut sum = 0i64;
                            let mut has_value = false;
                            for (row, weight) in &decoded {
                                match eval_scalar_expression(expr, row, agg_schema.as_ref()) {
                                    Ok(value) => {
                                        if let Some(number) = scalar_to_i64(&value) {
                                            sum += number * *weight;
                                            has_value = true;
                                        }
                                    }
                                    Err(err) => {
                                        tracing::warn!(
                                            graph_id = %agg_graph_id,
                                            error = %err,
                                            "failed to evaluate sum expression"
                                        );
                                    }
                                }
                            }
                            if has_value {
                                scalar_from_i64(sum, agg.output_type())
                            } else {
                                ScalarValue::Null
                            }
                        } else {
                            ScalarValue::Null
                        }
                    }
                    DbspAggregateFunction::Avg => {
                        if let Some(expr) = agg.expression() {
                            let mut sum = 0i64;
                            let mut count = 0i64;
                            for (row, weight) in &decoded {
                                match eval_scalar_expression(expr, row, agg_schema.as_ref()) {
                                    Ok(value) => {
                                        if let Some(number) = scalar_to_i64(&value) {
                                            sum += number * *weight;
                                            count += *weight;
                                        }
                                    }
                                    Err(err) => {
                                        tracing::warn!(
                                            graph_id = %agg_graph_id,
                                            error = %err,
                                            "failed to evaluate avg expression"
                                        );
                                    }
                                }
                            }
                            if count != 0 {
                                ScalarValue::Int64(Some(sum / count))
                            } else {
                                ScalarValue::Null
                            }
                        } else {
                            ScalarValue::Null
                        }
                    }
                    DbspAggregateFunction::Min => {
                        if let Some(expr) = agg.expression() {
                            let mut current: Option<i64> = None;
                            for (row, weight) in &decoded {
                                if *weight == 0 {
                                    continue;
                                }
                                match eval_scalar_expression(expr, row, agg_schema.as_ref()) {
                                    Ok(value) => {
                                        if let Some(number) = scalar_to_i64(&value) {
                                            current = Some(match current {
                                                Some(existing) => existing.min(number),
                                                None => number,
                                            });
                                        }
                                    }
                                    Err(err) => {
                                        tracing::warn!(
                                            graph_id = %agg_graph_id,
                                            error = %err,
                                            "failed to evaluate min expression"
                                        );
                                    }
                                }
                            }
                            current
                                .map(|value| scalar_from_i64(value, agg.output_type()))
                                .unwrap_or(ScalarValue::Null)
                        } else {
                            ScalarValue::Null
                        }
                    }
                    DbspAggregateFunction::Max => {
                        if let Some(expr) = agg.expression() {
                            let mut current: Option<i64> = None;
                            for (row, weight) in &decoded {
                                if *weight == 0 {
                                    continue;
                                }
                                match eval_scalar_expression(expr, row, agg_schema.as_ref()) {
                                    Ok(value) => {
                                        if let Some(number) = scalar_to_i64(&value) {
                                            current = Some(match current {
                                                Some(existing) => existing.max(number),
                                                None => number,
                                            });
                                        }
                                    }
                                    Err(err) => {
                                        tracing::warn!(
                                            graph_id = %agg_graph_id,
                                            error = %err,
                                            "failed to evaluate max expression"
                                        );
                                    }
                                }
                            }
                            current
                                .map(|value| scalar_from_i64(value, agg.output_type()))
                                .unwrap_or(ScalarValue::Null)
                        } else {
                            ScalarValue::Null
                        }
                    }
                };
                outputs.push(output);
            }

            match encode_projected_row_key(&outputs) {
                Ok(encoded) => Some(encoded),
                Err(err) => {
                    tracing::warn!(
                        graph_id = %agg_graph_id,
                        error = %err,
                        "failed to encode aggregate output"
                    );
                    None
                }
            }
        };

        let aggregate_spec = dbsp::operators::aggregate::AggregateSpec::new(
            format!("aggregate_{graph_id}"),
            aggregator,
        );

        let aggregate = DbspAggregate::new::<Vec<u8>, Vec<u8>, Vec<u8>, _>(
            &upstream,
            key_extractor,
            aggregate_spec,
            Some(aggregate_error_handler),
        )
        .await
        .context("initialize DBSP aggregate")?;

        let project_events = task_events.clone();
        let project_label = format!("aggregate-project:{graph_id}");
        let project_graph_id = graph_id.clone();
        let project_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &project_events,
                &project_graph_id,
                project_label.clone(),
                err,
            );
        });
        let project_graph_id = graph_id.clone();
        let projector = move |pair: &(Vec<u8>, Vec<u8>)| -> Vec<u8> {
            let mut key_values = match decode_projected_row_key(&pair.0) {
                Ok(values) => values,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to decode aggregate group key"
                    );
                    return Vec::new();
                }
            };
            let aggregate_values = match decode_projected_row_key(&pair.1) {
                Ok(values) => values,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to decode aggregate values"
                    );
                    return Vec::new();
                }
            };
            key_values.extend(aggregate_values);
            match encode_projected_row_key(&key_values) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to encode aggregate row"
                    );
                    Vec::new()
                }
            }
        };

        let mapped = DbspMap::new::<(Vec<u8>, Vec<u8>), Vec<u8>, _>(
            &aggregate.stream(),
            projector,
            Some(project_error_handler),
        )
        .await
        .context("initialize aggregate output map")?;

        Ok(mapped.stream())
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

fn scalar_to_i64(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::TimestampMillisecond(Some(v), _) => Some(*v),
        _ => None,
    }
}

fn scalar_from_i64(value: i64, output_type: &DbspScalarType) -> ScalarValue {
    match output_type {
        DbspScalarType::Int64 => ScalarValue::Int64(Some(value)),
        DbspScalarType::TimestampMillis => ScalarValue::TimestampMillisecond(Some(value), None),
        _ => ScalarValue::Int64(Some(value)),
    }
}
