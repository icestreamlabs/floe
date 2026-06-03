use super::incremental_eval::{
    direct_column_index, encode_count_values, encode_incremental_aggregate_values,
    encode_window_bounds, expression_lookup_key,
};
use super::shared::{ExpressionColumnMap, project_encoded_delta_batch};
use super::*;
use crate::encoding::concat_encoded_rows;
use datafusion::common::Column;
use datafusion::logical_expr::Expr;
use std::collections::HashSet;

type EncodedPairDelta = ((Vec<u8>, Vec<u8>), i64);
type CountValuesDelta = ((Vec<u8>, Vec<i64>), i64);
type WindowCountValuesDelta = ((WindowKey<Vec<u8>>, Vec<i64>), i64);
type WindowAggregateValuesDelta = ((WindowKey<Vec<u8>>, Vec<dbsp::AggregateValue>), i64);
type WindowEncodedValueDelta = ((WindowKey<Vec<u8>>, Vec<u8>), i64);
type WindowCountStarDelta = ((WindowKey<Vec<u8>>, i64), i64);
type AggregateValuesDelta = ((Vec<u8>, Vec<dbsp::AggregateValue>), i64);

impl DbspGraphBuilder {
    pub(super) async fn precompute_aggregate_window_expressions(
        &mut self,
        upstream: DeltaHandleStream,
        input_schema: Arc<RowSchema>,
        expressions: &[dbsp::DbspExpression],
        task_events: &GraphTaskSender,
        alias_prefix: &str,
    ) -> Result<(DeltaHandleStream, Arc<RowSchema>, Arc<ExpressionColumnMap>)> {
        let mut seen = HashSet::new();
        let mut non_direct_expressions = Vec::new();
        for expr in expressions {
            if direct_column_index(expr, input_schema.as_ref()).is_some() {
                continue;
            }
            let key = expression_lookup_key(expr.expr());
            if seen.insert(key.clone()) {
                non_direct_expressions.push((key, expr.expr().clone()));
            }
        }
        if non_direct_expressions.is_empty() {
            return Ok((upstream, input_schema, Arc::new(HashMap::new())));
        }

        let mut items = Vec::with_capacity(input_schema.len() + non_direct_expressions.len());
        for field in input_schema.fields() {
            items.push(dbsp::circuit::plan::ProjectItem {
                expr: Expr::Column(Column::new_unqualified(field.name.clone())),
                alias: Some(field.name.clone()),
            });
        }

        let mut expression_columns = HashMap::with_capacity(non_direct_expressions.len());
        let mut next_index = input_schema.len();
        for (index, (key, expr)) in non_direct_expressions.into_iter().enumerate() {
            let alias = format!("__floe_{alias_prefix}_expr_{index}");
            items.push(dbsp::circuit::plan::ProjectItem {
                expr,
                alias: Some(alias),
            });
            expression_columns.insert(key, next_index);
            next_index += 1;
        }

        let precompute = dbsp::DbspProjectNode::try_new(Arc::clone(&input_schema), items)
            .with_context(|| format!("build {alias_prefix} expression precompute projection"))?;
        let precompute_schema = Arc::clone(precompute.output_schema());
        let precomputed = self
            .compile_map(&precompute, upstream, task_events)
            .await
            .with_context(|| format!("initialize {alias_prefix} expression precompute map"))?;

        Ok((precomputed, precompute_schema, Arc::new(expression_columns)))
    }

    pub(super) async fn map_aggregate_output(
        &self,
        graph_id: &str,
        aggregate_stream: &DeltaHandleStream,
        task_events: &GraphTaskSender,
        label_prefix: &str,
    ) -> Result<DeltaHandleStream> {
        let project_events = task_events.clone();
        let project_label = format!("{label_prefix}:{graph_id}");
        let project_graph_id = graph_id.to_string();
        let project_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &project_events,
                &project_graph_id,
                project_label.clone(),
                err,
            );
        });
        let project_graph_id = graph_id.to_string();
        let projector = move |pair: &(Vec<u8>, Vec<u8>)| -> Vec<u8> {
            match concat_encoded_rows(&pair.0, &pair.1) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to concatenate aggregate row segments"
                    );
                    Vec::new()
                }
            }
        };

        let transform =
            move |delta_values: &[EncodedPairDelta]| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                Ok(project_encoded_delta_batch(delta_values, &projector))
            };

        let mapped = DbspFilterMap::new_batch::<(Vec<u8>, Vec<u8>), Vec<u8>, _>(
            aggregate_stream,
            transform,
            Some(project_error_handler),
        )
        .await
        .context("initialize aggregate output map")?;
        Ok(mapped.stream())
    }

    pub(super) async fn map_count_aggregate_output(
        &self,
        graph_id: &str,
        aggregate_stream: &DeltaHandleStream,
        task_events: &GraphTaskSender,
        label_prefix: &str,
    ) -> Result<DeltaHandleStream> {
        let project_events = task_events.clone();
        let project_label = format!("{label_prefix}:{graph_id}");
        let project_graph_id = graph_id.to_string();
        let project_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &project_events,
                &project_graph_id,
                project_label.clone(),
                err,
            );
        });
        let project_graph_id = graph_id.to_string();
        let projector = move |pair: &(Vec<u8>, Vec<i64>)| -> Vec<u8> {
            let encoded_count_values = match encode_count_values(&pair.1) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to encode count aggregate values"
                    );
                    return Vec::new();
                }
            };
            match concat_encoded_rows(&pair.0, &encoded_count_values) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to concatenate count aggregate row segments"
                    );
                    Vec::new()
                }
            }
        };

        let transform =
            move |delta_values: &[CountValuesDelta]| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                Ok(project_encoded_delta_batch(delta_values, &projector))
            };

        let mapped = DbspFilterMap::new_batch::<(Vec<u8>, Vec<i64>), Vec<u8>, _>(
            aggregate_stream,
            transform,
            Some(project_error_handler),
        )
        .await
        .context("initialize count aggregate output map")?;
        Ok(mapped.stream())
    }

    pub(super) async fn map_window_count_aggregate_output(
        &self,
        graph_id: &str,
        aggregate_stream: &DeltaHandleStream,
        task_events: &GraphTaskSender,
        label_prefix: &str,
    ) -> Result<DeltaHandleStream> {
        let project_events = task_events.clone();
        let project_label = format!("{label_prefix}:{graph_id}");
        let project_graph_id = graph_id.to_string();
        let project_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &project_events,
                &project_graph_id,
                project_label.clone(),
                err,
            );
        });
        let project_graph_id = graph_id.to_string();
        let projector = move |pair: &(WindowKey<Vec<u8>>, Vec<i64>)| -> Vec<u8> {
            let encoded_window_bounds = match encode_window_bounds(pair.0.start, pair.0.end) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to encode window aggregate bounds"
                    );
                    return Vec::new();
                }
            };
            let with_key = match concat_encoded_rows(&encoded_window_bounds, &pair.0.key) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to concatenate window aggregate bounds and key"
                    );
                    return Vec::new();
                }
            };
            let encoded_count_values = match encode_count_values(&pair.1) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to encode count aggregate values"
                    );
                    return Vec::new();
                }
            };
            match concat_encoded_rows(&with_key, &encoded_count_values) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to concatenate window aggregate output values"
                    );
                    Vec::new()
                }
            }
        };

        let transform =
            move |delta_values: &[WindowCountValuesDelta]| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                Ok(project_encoded_delta_batch(delta_values, &projector))
            };

        let mapped = DbspFilterMap::new_batch::<(WindowKey<Vec<u8>>, Vec<i64>), Vec<u8>, _>(
            aggregate_stream,
            transform,
            Some(project_error_handler),
        )
        .await
        .context("initialize window count aggregate output map")?;
        Ok(mapped.stream())
    }

    pub(super) async fn map_window_incremental_aggregate_output(
        &self,
        graph_id: &str,
        aggregate_stream: &DeltaHandleStream,
        task_events: &GraphTaskSender,
        label_prefix: &str,
    ) -> Result<DeltaHandleStream> {
        let project_events = task_events.clone();
        let project_label = format!("{label_prefix}:{graph_id}");
        let project_graph_id = graph_id.to_string();
        let project_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &project_events,
                &project_graph_id,
                project_label.clone(),
                err,
            );
        });
        let project_graph_id = graph_id.to_string();
        let projector = move |pair: &(WindowKey<Vec<u8>>, Vec<dbsp::AggregateValue>)| -> Vec<u8> {
            let encoded_window_bounds = match encode_window_bounds(pair.0.start, pair.0.end) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to encode window aggregate bounds"
                    );
                    return Vec::new();
                }
            };
            let with_key = match concat_encoded_rows(&encoded_window_bounds, &pair.0.key) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to concatenate window aggregate bounds and key"
                    );
                    return Vec::new();
                }
            };
            let encoded_aggregate_values = match encode_incremental_aggregate_values(&pair.1) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to encode window incremental aggregate values"
                    );
                    return Vec::new();
                }
            };
            match concat_encoded_rows(&with_key, &encoded_aggregate_values) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to concatenate window aggregate output values"
                    );
                    Vec::new()
                }
            }
        };

        let transform = move |delta_values: &[WindowAggregateValuesDelta]|
              -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
            Ok(project_encoded_delta_batch(delta_values, &projector))
        };

        let mapped = DbspFilterMap::new_batch::<
            (WindowKey<Vec<u8>>, Vec<dbsp::AggregateValue>),
            Vec<u8>,
            _,
        >(aggregate_stream, transform, Some(project_error_handler))
        .await
        .context("initialize window incremental aggregate output map")?;
        Ok(mapped.stream())
    }

    pub(super) async fn map_window_aggregate_output(
        &self,
        graph_id: &str,
        aggregate_stream: &DeltaHandleStream,
        task_events: &GraphTaskSender,
        label_prefix: &str,
    ) -> Result<DeltaHandleStream> {
        let project_events = task_events.clone();
        let project_label = format!("{label_prefix}:{graph_id}");
        let project_graph_id = graph_id.to_string();
        let project_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &project_events,
                &project_graph_id,
                project_label.clone(),
                err,
            );
        });
        let project_graph_id = graph_id.to_string();
        let projector = move |pair: &(WindowKey<Vec<u8>>, Vec<u8>)| -> Vec<u8> {
            let encoded_window_bounds = match encode_window_bounds(pair.0.start, pair.0.end) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to encode window aggregate bounds"
                    );
                    return Vec::new();
                }
            };
            let with_key = match concat_encoded_rows(&encoded_window_bounds, &pair.0.key) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to concatenate window aggregate bounds and key"
                    );
                    return Vec::new();
                }
            };
            match concat_encoded_rows(&with_key, &pair.1) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to concatenate window aggregate output values"
                    );
                    Vec::new()
                }
            }
        };

        let transform =
            move |delta_values: &[WindowEncodedValueDelta]| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                Ok(project_encoded_delta_batch(delta_values, &projector))
            };

        let mapped = DbspFilterMap::new_batch::<(WindowKey<Vec<u8>>, Vec<u8>), Vec<u8>, _>(
            aggregate_stream,
            transform,
            Some(project_error_handler),
        )
        .await
        .context("initialize window aggregate output map")?;
        Ok(mapped.stream())
    }

    pub(super) async fn map_window_count_star_aggregate_output(
        &self,
        graph_id: &str,
        aggregate_stream: &DeltaHandleStream,
        task_events: &GraphTaskSender,
        label_prefix: &str,
    ) -> Result<DeltaHandleStream> {
        let project_events = task_events.clone();
        let project_label = format!("{label_prefix}:{graph_id}");
        let project_graph_id = graph_id.to_string();
        let project_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &project_events,
                &project_graph_id,
                project_label.clone(),
                err,
            );
        });
        let project_graph_id = graph_id.to_string();
        let projector = move |pair: &(WindowKey<Vec<u8>>, i64)| -> Vec<u8> {
            let encoded_window_bounds = match encode_window_bounds(pair.0.start, pair.0.end) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to encode window aggregate bounds"
                    );
                    return Vec::new();
                }
            };
            let with_key = match concat_encoded_rows(&encoded_window_bounds, &pair.0.key) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to concatenate window aggregate bounds and key"
                    );
                    return Vec::new();
                }
            };
            let encoded_count_values = match encode_count_values(std::slice::from_ref(&pair.1)) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to encode count aggregate value"
                    );
                    return Vec::new();
                }
            };
            match concat_encoded_rows(&with_key, &encoded_count_values) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to concatenate window aggregate output values"
                    );
                    Vec::new()
                }
            }
        };

        let transform =
            move |delta_values: &[WindowCountStarDelta]| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                Ok(project_encoded_delta_batch(delta_values, &projector))
            };

        let mapped = DbspFilterMap::new_batch::<(WindowKey<Vec<u8>>, i64), Vec<u8>, _>(
            aggregate_stream,
            transform,
            Some(project_error_handler),
        )
        .await
        .context("initialize window count-star aggregate output map")?;
        Ok(mapped.stream())
    }

    pub(super) async fn map_incremental_aggregate_output(
        &self,
        graph_id: &str,
        aggregate_stream: &DeltaHandleStream,
        task_events: &GraphTaskSender,
        label_prefix: &str,
    ) -> Result<DeltaHandleStream> {
        let project_events = task_events.clone();
        let project_label = format!("{label_prefix}:{graph_id}");
        let project_graph_id = graph_id.to_string();
        let project_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(
                &project_events,
                &project_graph_id,
                project_label.clone(),
                err,
            );
        });
        let project_graph_id = graph_id.to_string();
        let projector = move |pair: &(Vec<u8>, Vec<dbsp::AggregateValue>)| -> Vec<u8> {
            let encoded_aggregate_values = match encode_incremental_aggregate_values(&pair.1) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to encode incremental aggregate values"
                    );
                    return Vec::new();
                }
            };
            match concat_encoded_rows(&pair.0, &encoded_aggregate_values) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to concatenate incremental aggregate row segments"
                    );
                    Vec::new()
                }
            }
        };

        let transform =
            move |delta_values: &[AggregateValuesDelta]| -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
                Ok(project_encoded_delta_batch(delta_values, &projector))
            };

        let mapped = DbspFilterMap::new_batch::<(Vec<u8>, Vec<dbsp::AggregateValue>), Vec<u8>, _>(
            aggregate_stream,
            transform,
            Some(project_error_handler),
        )
        .await
        .context("initialize incremental aggregate output map")?;
        Ok(mapped.stream())
    }
}
