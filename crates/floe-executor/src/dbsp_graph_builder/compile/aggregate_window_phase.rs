use super::*;
use crate::encoding::{concat_encoded_rows, extract_encoded_row_columns};
use datafusion::common::Column;
use datafusion::logical_expr::Expr;
use std::collections::{BTreeSet, HashMap, HashSet};

type ExpressionColumnMap = HashMap<String, usize>;

#[derive(Clone)]
struct CountEvalLayout {
    filters: Vec<dbsp::DbspExpression>,
    filter_direct_columns: Vec<Option<usize>>,
    expressions: Vec<dbsp::DbspExpression>,
    expression_direct_columns: Vec<Option<usize>>,
    required_input_columns: Vec<usize>,
    plans: Vec<CountEvalPlan>,
}

#[derive(Clone, Copy)]
struct CountEvalPlan {
    filter_index: Option<usize>,
    expr_index: Option<usize>,
}

impl DbspGraphBuilder {
    pub(crate) async fn compile_aggregate(
        &mut self,
        node: &DbspAggregateNode,
        mut upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let input_schema = Arc::clone(node.input_schema());
        let group_keys = node.group_keys().to_vec();
        let aggregates = node.aggregates().to_vec();
        let mut precompute_expressions = Vec::new();
        precompute_expressions.extend(group_keys.iter().map(|key| key.expression().clone()));
        for agg in &aggregates {
            if let Some(filter) = agg.filter() {
                precompute_expressions.push(filter.clone());
            }
            if let Some(expr) = agg.expression() {
                precompute_expressions.push(expr.clone());
            }
        }
        let (precomputed_upstream, eval_schema, expression_columns) = self
            .precompute_aggregate_window_expressions(
                upstream,
                Arc::clone(&input_schema),
                &precompute_expressions,
                task_events,
                "aggregate",
            )
            .await?;
        upstream = precomputed_upstream;
        let direct_group_key_columns = Arc::new(
            direct_group_key_columns(
                &group_keys,
                eval_schema.as_ref(),
                expression_columns.as_ref(),
            )
            .ok_or_else(|| anyhow!("failed to resolve vectorized aggregate group key columns"))?,
        );
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

        if aggregates
            .iter()
            .all(|agg| agg.function() == &DbspAggregateFunction::Count)
        {
            let slot_kinds = build_count_aggregate_slot_kinds(&aggregates);
            let row_evaluator = build_count_row_evaluator(
                Arc::clone(&eval_schema),
                group_keys.clone(),
                aggregates.clone(),
                Arc::clone(&expression_columns),
                graph_id.clone(),
                "aggregate",
            );

            let count_aggregate = DbspCountAggregate::new::<Vec<u8>, Vec<u8>, Vec<u8>, _>(
                &upstream,
                row_evaluator,
                slot_kinds,
                Some(aggregate_error_handler),
            )
            .await
            .context("initialize DBSP count aggregate")?;

            let mapped = self
                .map_count_aggregate_output(
                    &graph_id,
                    &count_aggregate.stream(),
                    task_events,
                    "aggregate-project",
                )
                .await?;
            return Ok(mapped.stream());
        }

        if let Some(slot_kinds) = build_incremental_aggregate_slot_kinds(&aggregates) {
            let row_evaluator = build_incremental_aggregate_row_evaluator(
                Arc::clone(&eval_schema),
                group_keys.clone(),
                aggregates.clone(),
                Arc::clone(&expression_columns),
                graph_id.clone(),
                "aggregate",
            );

            let incremental_aggregate = dbsp::DbspIncrementalAggregate::new::<Vec<u8>, Vec<u8>, _>(
                &upstream,
                row_evaluator,
                slot_kinds,
                Some(aggregate_error_handler),
            )
            .await
            .context("initialize DBSP incremental aggregate")?;

            let mapped = self
                .map_incremental_aggregate_output(
                    &graph_id,
                    &incremental_aggregate.stream(),
                    task_events,
                    "aggregate-project",
                )
                .await?;
            return Ok(mapped.stream());
        }

        let key_columns = Arc::clone(&direct_group_key_columns);
        let key_graph_id = graph_id.clone();
        let key_extractor = move |bytes: &Vec<u8>| -> Option<Vec<u8>> {
            match extract_encoded_row_columns(bytes, key_columns.as_ref(), false) {
                Ok(selected) => selected,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %key_graph_id,
                        error = %err,
                        "failed to extract aggregate group key columns"
                    );
                    None
                }
            }
        };

        let agg_schema = Arc::clone(&eval_schema);
        let agg_graph_id = graph_id.clone();
        let agg_layout = Arc::new(build_count_eval_layout(
            &aggregates,
            eval_schema.as_ref(),
            expression_columns.as_ref(),
        ));
        let aggregator = move |_key: &Vec<u8>, values: &[(Vec<u8>, i64)]| -> Option<Vec<u8>> {
            if values.is_empty() {
                return None;
            }
            let outputs = evaluate_aggregate_values_from_encoded(
                agg_layout.as_ref(),
                &aggregates,
                values,
                agg_schema.as_ref(),
                &agg_graph_id,
                "aggregate",
            );
            if outputs.is_empty() {
                return None;
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

        let mapped = self
            .map_aggregate_output(
                &graph_id,
                &aggregate.stream(),
                task_events,
                "aggregate-project",
            )
            .await?;

        Ok(mapped.stream())
    }

    pub(crate) async fn compile_window_aggregate(
        &mut self,
        node: &DbspWindowAggregateNode,
        mut upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let aggregate = &node.aggregate;
        let input_schema = Arc::clone(aggregate.input_schema());
        let group_keys = aggregate.group_keys().to_vec();
        let aggregates = aggregate.aggregates().to_vec();
        let time_expression = node.window.time_expression.clone();
        let mut precompute_expressions = Vec::new();
        precompute_expressions.extend(group_keys.iter().map(|key| key.expression().clone()));
        precompute_expressions.push(time_expression.clone());
        for agg in &aggregates {
            if let Some(filter) = agg.filter() {
                precompute_expressions.push(filter.clone());
            }
            if let Some(expr) = agg.expression() {
                precompute_expressions.push(expr.clone());
            }
        }
        let (precomputed_upstream, eval_schema, expression_columns) = self
            .precompute_aggregate_window_expressions(
                upstream,
                Arc::clone(&input_schema),
                &precompute_expressions,
                task_events,
                "window_aggregate",
            )
            .await?;
        upstream = precomputed_upstream;
        let direct_group_key_columns = Arc::new(
            direct_group_key_columns(
                &group_keys,
                eval_schema.as_ref(),
                expression_columns.as_ref(),
            )
            .ok_or_else(|| {
                anyhow!("failed to resolve vectorized window aggregate group key columns")
            })?,
        );
        let (window_size, window_slide) = match &node.window.policy {
            DbspWindowPolicy::Tumbling { size_ms } => (*size_ms, *size_ms),
            DbspWindowPolicy::Hopping { size_ms, slide_ms } => (*size_ms, *slide_ms),
            DbspWindowPolicy::Session { gap_ms } => (*gap_ms, *gap_ms),
        };
        let allowed_lateness_ms = node.window.allowed_lateness_ms;

        let graph_id = self.graph_id().to_string();
        let window_events = task_events.clone();
        let window_label = format!("window-aggregate:{graph_id}");
        let window_graph_id = graph_id.clone();
        let window_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&window_events, &window_graph_id, window_label.clone(), err);
        });

        let key_columns = Arc::clone(&direct_group_key_columns);
        let key_graph_id = graph_id.clone();
        let key_extractor = move |bytes: &Vec<u8>| -> Option<Vec<u8>> {
            match extract_encoded_row_columns(bytes, key_columns.as_ref(), false) {
                Ok(selected) => selected,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %key_graph_id,
                        error = %err,
                        "failed to extract window aggregate group key columns"
                    );
                    None
                }
            }
        };

        let time_graph_id = graph_id.clone();
        let direct_time_column = resolved_expression_column_index(
            &time_expression,
            eval_schema.as_ref(),
            expression_columns.as_ref(),
        )
        .ok_or_else(|| anyhow!("failed to resolve vectorized window aggregate time column"))?;
        let time_extractor = move |bytes: &Vec<u8>| -> Option<i64> {
            let selected = match extract_encoded_row_columns(bytes, &[direct_time_column], false) {
                Ok(Some(selected)) => selected,
                Ok(None) => return None,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %time_graph_id,
                        error = %err,
                        "failed to extract window aggregate time column"
                    );
                    return None;
                }
            };
            let values = match decode_projected_row_key(&selected) {
                Ok(values) => values,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %time_graph_id,
                        error = %err,
                        "failed to decode extracted window aggregate time column"
                    );
                    return None;
                }
            };
            values.first().and_then(scalar_to_i64)
        };

        let agg_schema = Arc::clone(&eval_schema);
        let agg_graph_id = graph_id.clone();
        let agg_layout = Arc::new(build_count_eval_layout(
            &aggregates,
            eval_schema.as_ref(),
            expression_columns.as_ref(),
        ));
        let aggregator = move |_key: &Vec<u8>, values: &[(Vec<u8>, i64)]| -> Option<Vec<u8>> {
            if values.is_empty() {
                return None;
            }
            let outputs = evaluate_aggregate_values_from_encoded(
                agg_layout.as_ref(),
                &aggregates,
                values,
                agg_schema.as_ref(),
                &agg_graph_id,
                "window aggregate",
            );
            if outputs.is_empty() {
                return None;
            }

            match encode_projected_row_key(&outputs) {
                Ok(encoded) => Some(encoded),
                Err(err) => {
                    tracing::warn!(
                        graph_id = %agg_graph_id,
                        error = %err,
                        "failed to encode window aggregate output"
                    );
                    None
                }
            }
        };

        let watermark = Arc::clone(&self.watermark);
        let window_aggregate = DbspWindowAggregate::new::<Vec<u8>, Vec<u8>, Vec<u8>, _, _, _>(
            &upstream,
            key_extractor,
            aggregator,
            time_extractor,
            window_size,
            window_slide,
            allowed_lateness_ms,
            watermark,
            Some(window_error_handler),
        )
        .await
        .context("initialize DBSP window aggregate")?;

        let project_events = task_events.clone();
        let project_label = format!("window-aggregate-project:{graph_id}");
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
        let projector = move |pair: &(WindowKey<Vec<u8>>, Vec<u8>)| -> Vec<u8> {
            let window_bounds = [
                ScalarValue::TimestampMillisecond(Some(pair.0.start), None),
                ScalarValue::TimestampMillisecond(Some(pair.0.end), None),
            ];
            let encoded_window_bounds = match encode_projected_row_key(&window_bounds) {
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

        let mapped = DbspMap::new::<(WindowKey<Vec<u8>>, Vec<u8>), Vec<u8>, _>(
            &window_aggregate.stream(),
            projector,
            Some(project_error_handler),
        )
        .await
        .context("initialize window aggregate output map")?;

        Ok(mapped.stream())
    }
}

impl DbspGraphBuilder {
    async fn precompute_aggregate_window_expressions(
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

    async fn map_aggregate_output(
        &self,
        graph_id: &str,
        aggregate_stream: &DeltaHandleStream,
        task_events: &GraphTaskSender,
        label_prefix: &str,
    ) -> Result<DbspMap> {
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

        DbspMap::new::<(Vec<u8>, Vec<u8>), Vec<u8>, _>(
            aggregate_stream,
            projector,
            Some(project_error_handler),
        )
        .await
        .context("initialize aggregate output map")
    }

    async fn map_count_aggregate_output(
        &self,
        graph_id: &str,
        aggregate_stream: &DeltaHandleStream,
        task_events: &GraphTaskSender,
        label_prefix: &str,
    ) -> Result<DbspMap> {
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
            let count_values = pair
                .1
                .iter()
                .map(|value| ScalarValue::Int64(Some(*value)))
                .collect::<Vec<_>>();
            let encoded_count_values = match encode_projected_row_key(&count_values) {
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

        DbspMap::new::<(Vec<u8>, Vec<i64>), Vec<u8>, _>(
            aggregate_stream,
            projector,
            Some(project_error_handler),
        )
        .await
        .context("initialize count aggregate output map")
    }

    async fn map_incremental_aggregate_output(
        &self,
        graph_id: &str,
        aggregate_stream: &DeltaHandleStream,
        task_events: &GraphTaskSender,
        label_prefix: &str,
    ) -> Result<DbspMap> {
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
            let aggregate_values = pair
                .1
                .iter()
                .map(scalar_from_incremental_aggregate_value)
                .collect::<Vec<_>>();
            let encoded_aggregate_values = match encode_projected_row_key(&aggregate_values) {
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

        DbspMap::new::<(Vec<u8>, Vec<dbsp::AggregateValue>), Vec<u8>, _>(
            aggregate_stream,
            projector,
            Some(project_error_handler),
        )
        .await
        .context("initialize incremental aggregate output map")
    }
}

pub(crate) fn build_count_aggregate_slot_kinds(
    aggregates: &[DbspAggregateExpr],
) -> Vec<dbsp::CountAggregateSlotKind> {
    aggregates
        .iter()
        .map(|agg| {
            if agg.distinct() {
                dbsp::CountAggregateSlotKind::Distinct
            } else {
                dbsp::CountAggregateSlotKind::Linear
            }
        })
        .collect()
}

pub(crate) fn build_count_row_evaluator(
    input_schema: Arc<RowSchema>,
    group_keys: Vec<dbsp::circuit::plan::GroupKeyExpr>,
    aggregates: Vec<DbspAggregateExpr>,
    expression_columns: Arc<ExpressionColumnMap>,
    graph_id: String,
    context: &'static str,
) -> impl Fn(&Vec<u8>) -> Option<dbsp::CountAggregateRow<Vec<u8>, Vec<u8>>> + Send + Sync + 'static
{
    let layout = Arc::new(build_count_eval_layout(
        &aggregates,
        input_schema.as_ref(),
        expression_columns.as_ref(),
    ));
    let direct_group_key_columns = direct_group_key_columns(
        &group_keys,
        input_schema.as_ref(),
        expression_columns.as_ref(),
    )
    .map(Arc::new);
    let slot_eval_needs_row = !layout.required_input_columns.is_empty();
    let eval_required_columns =
        slot_eval_needs_row.then(|| Arc::new(layout.required_input_columns.clone()));
    move |bytes: &Vec<u8>| -> Option<dbsp::CountAggregateRow<Vec<u8>, Vec<u8>>> {
        let row = if slot_eval_needs_row {
            match decode_sparse_row_for_columns(
                bytes,
                eval_required_columns
                    .as_ref()
                    .expect("required columns should be present when decoding rows")
                    .as_ref(),
                input_schema.len(),
            ) {
                Ok(row) => Some(row),
                Err(err) => {
                    tracing::warn!(
                        graph_id = %graph_id,
                        error = %err,
                        "failed to decode aggregate row for count aggregate"
                    );
                    return None;
                }
            }
        } else {
            None
        };

        let Some(indices) = direct_group_key_columns.as_ref() else {
            tracing::warn!(
                graph_id = %graph_id,
                "failed to resolve vectorized count aggregate group key columns"
            );
            return None;
        };
        let encoded_key = match extract_encoded_row_columns(bytes, indices.as_ref(), false) {
            Ok(Some(encoded_key)) => encoded_key,
            Ok(None) => return None,
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to extract count aggregate group key columns"
                );
                return None;
            }
        };

        let row_for_slot_eval = if slot_eval_needs_row {
            row.as_deref().expect("decoded row should be present")
        } else {
            &[]
        };
        let counts = evaluate_count_row_values(
            layout.as_ref(),
            &aggregates,
            row_for_slot_eval,
            input_schema.as_ref(),
            &graph_id,
            context,
        );
        Some(dbsp::CountAggregateRow {
            key: encoded_key,
            slots: counts,
        })
    }
}

fn build_count_eval_layout(
    aggregates: &[DbspAggregateExpr],
    schema: &RowSchema,
    expression_columns: &ExpressionColumnMap,
) -> CountEvalLayout {
    let mut filters = Vec::new();
    let mut filter_direct_columns = Vec::new();
    let mut expressions = Vec::new();
    let mut expression_direct_columns = Vec::new();
    let mut required_input_columns = BTreeSet::new();
    let mut plans = Vec::with_capacity(aggregates.len());

    for agg in aggregates {
        let filter_index = agg.filter().map(|filter| {
            if let Some(existing) = filters
                .iter()
                .position(|existing: &dbsp::DbspExpression| existing.expr() == filter.expr())
            {
                existing
            } else {
                filters.push(filter.clone());
                let column = resolved_expression_column_index(filter, schema, expression_columns);
                if let Some(column_idx) = column {
                    required_input_columns.insert(column_idx);
                }
                filter_direct_columns.push(column);
                filters.len() - 1
            }
        });
        let expr_index = agg.expression().map(|expr| {
            if let Some(existing) = expressions
                .iter()
                .position(|existing: &dbsp::DbspExpression| existing.expr() == expr.expr())
            {
                existing
            } else {
                expressions.push(expr.clone());
                let column = resolved_expression_column_index(expr, schema, expression_columns);
                if let Some(column_idx) = column {
                    required_input_columns.insert(column_idx);
                }
                expression_direct_columns.push(column);
                expressions.len() - 1
            }
        });
        plans.push(CountEvalPlan {
            filter_index,
            expr_index,
        });
    }

    CountEvalLayout {
        filters,
        filter_direct_columns,
        expressions,
        expression_direct_columns,
        required_input_columns: required_input_columns.into_iter().collect(),
        plans,
    }
}

fn evaluate_count_row_values(
    layout: &CountEvalLayout,
    aggregates: &[DbspAggregateExpr],
    row: &[ScalarValue],
    _schema: &RowSchema,
    graph_id: &str,
    context: &str,
) -> Vec<dbsp::CountAggregateSlotUpdate<Vec<u8>>> {
    let mut filter_results = vec![false; layout.filters.len()];
    for (index, filter) in layout.filters.iter().enumerate() {
        if let Some(column_idx) = layout.filter_direct_columns[index] {
            let value = row.get(column_idx).unwrap_or(&ScalarValue::Null);
            filter_results[index] = match crate::expression::scalar_to_bool(value) {
                Ok(include) => include,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %graph_id,
                        error = %err,
                        "failed to evaluate {context} direct FILTER column"
                    );
                    false
                }
            };
        } else {
            tracing::warn!(
                graph_id = %graph_id,
                expression = ?filter.expr(),
                "unresolved {context} FILTER expression without vectorized precompute column"
            );
            filter_results[index] = false;
        }
    }

    let mut expression_values = vec![ScalarValue::Null; layout.expressions.len()];
    let mut expression_valid = vec![false; layout.expressions.len()];
    for (index, expr) in layout.expressions.iter().enumerate() {
        if let Some(column_idx) = layout.expression_direct_columns[index] {
            if let Some(value) = row.get(column_idx) {
                expression_values[index] = value.clone();
                expression_valid[index] = true;
            }
        } else {
            tracing::warn!(
                graph_id = %graph_id,
                expression = ?expr.expr(),
                "unresolved {context} aggregate expression without vectorized precompute column"
            );
        }
    }

    aggregates
        .iter()
        .zip(layout.plans.iter())
        .map(|(agg, plan)| {
            if let Some(filter_index) = plan.filter_index
                && !filter_results[filter_index]
            {
                return if agg.distinct() {
                    dbsp::CountAggregateSlotUpdate::Distinct(None)
                } else {
                    dbsp::CountAggregateSlotUpdate::Linear(0)
                };
            }
            match plan.expr_index {
                Some(expr_index) => {
                    if expression_valid[expr_index] && !expression_values[expr_index].is_null() {
                        if agg.distinct() {
                            let encoded = encode_projected_row_key(std::slice::from_ref(
                                &expression_values[expr_index],
                            ))
                            .map(Some)
                            .unwrap_or_else(|err| {
                                tracing::warn!(
                                    graph_id = %graph_id,
                                    error = %err,
                                    "failed to encode count aggregate DISTINCT value"
                                );
                                None
                            });
                            dbsp::CountAggregateSlotUpdate::Distinct(encoded)
                        } else {
                            dbsp::CountAggregateSlotUpdate::Linear(1)
                        }
                    } else {
                        if agg.distinct() {
                            dbsp::CountAggregateSlotUpdate::Distinct(None)
                        } else {
                            dbsp::CountAggregateSlotUpdate::Linear(0)
                        }
                    }
                }
                None => dbsp::CountAggregateSlotUpdate::Linear(1),
            }
        })
        .collect()
}

fn evaluate_aggregate_values_from_encoded(
    layout: &CountEvalLayout,
    aggregates: &[DbspAggregateExpr],
    values: &[(Vec<u8>, i64)],
    schema: &RowSchema,
    graph_id: &str,
    context: &str,
) -> Vec<ScalarValue> {
    if aggregates.is_empty() {
        return Vec::new();
    }

    let mut accumulators = Vec::with_capacity(aggregates.len());
    for agg in aggregates {
        accumulators.push(match agg.function() {
            DbspAggregateFunction::Count if agg.distinct() => AggregateAccumulator::CountDistinct {
                weights: HashMap::new(),
            },
            DbspAggregateFunction::Count => AggregateAccumulator::Count { count: 0 },
            DbspAggregateFunction::Sum => AggregateAccumulator::Sum {
                sum: 0,
                has_value: false,
            },
            DbspAggregateFunction::Avg => AggregateAccumulator::Avg { sum: 0, count: 0 },
            DbspAggregateFunction::Min => AggregateAccumulator::Min { current: None },
            DbspAggregateFunction::Max => AggregateAccumulator::Max { current: None },
        });
    }

    let mut filter_results = vec![false; layout.filters.len()];
    let mut expression_values = vec![ScalarValue::Null; layout.expressions.len()];
    let mut expression_valid = vec![false; layout.expressions.len()];
    let mut decoded_row_count = 0usize;

    for (value, weight) in values {
        if *weight == 0 {
            continue;
        }
        let row = match decode_sparse_row_for_columns(
            value,
            layout.required_input_columns.as_slice(),
            schema.len(),
        ) {
            Ok(row) => row,
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to decode {context} input row"
                );
                continue;
            }
        };
        decoded_row_count = decoded_row_count.saturating_add(1);

        for (index, filter) in layout.filters.iter().enumerate() {
            if let Some(column_idx) = layout.filter_direct_columns[index] {
                let value = row.get(column_idx).unwrap_or(&ScalarValue::Null);
                filter_results[index] = match crate::expression::scalar_to_bool(value) {
                    Ok(include) => include,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %graph_id,
                            error = %err,
                            "failed to evaluate {context} direct FILTER column"
                        );
                        false
                    }
                };
            } else {
                tracing::warn!(
                    graph_id = %graph_id,
                    expression = ?filter.expr(),
                    "unresolved {context} FILTER expression without vectorized precompute column"
                );
                filter_results[index] = false;
            }
        }

        expression_valid.fill(false);
        for (index, expr) in layout.expressions.iter().enumerate() {
            if let Some(column_idx) = layout.expression_direct_columns[index] {
                if let Some(value) = row.get(column_idx) {
                    expression_values[index] = value.clone();
                    expression_valid[index] = true;
                }
            } else {
                tracing::warn!(
                    graph_id = %graph_id,
                    expression = ?expr.expr(),
                    "unresolved {context} aggregate expression without vectorized precompute column"
                );
            }
        }

        for ((_, plan), accumulator) in aggregates
            .iter()
            .zip(layout.plans.iter())
            .zip(accumulators.iter_mut())
        {
            if let Some(filter_index) = plan.filter_index
                && !filter_results[filter_index]
            {
                continue;
            }

            match accumulator {
                AggregateAccumulator::CountDistinct { weights } => {
                    let Some(expr_index) = plan.expr_index else {
                        continue;
                    };
                    if !expression_valid[expr_index] {
                        continue;
                    }
                    let expr_value = expression_values[expr_index].clone();
                    if expr_value.is_null() {
                        continue;
                    }
                    let entry = weights.entry(expr_value.clone()).or_insert(0);
                    *entry += *weight;
                    if *entry == 0 {
                        weights.remove(&expr_value);
                    }
                }
                AggregateAccumulator::Count { count } => match plan.expr_index {
                    Some(expr_index) => {
                        if expression_valid[expr_index] && !expression_values[expr_index].is_null()
                        {
                            *count += *weight;
                        }
                    }
                    None => *count += *weight,
                },
                AggregateAccumulator::Sum { sum, has_value } => {
                    let Some(expr_index) = plan.expr_index else {
                        continue;
                    };
                    if !expression_valid[expr_index] {
                        continue;
                    }
                    if let Some(number) = scalar_to_i64(&expression_values[expr_index]) {
                        *sum += number * *weight;
                        *has_value = true;
                    }
                }
                AggregateAccumulator::Avg { sum, count } => {
                    let Some(expr_index) = plan.expr_index else {
                        continue;
                    };
                    if !expression_valid[expr_index] {
                        continue;
                    }
                    if let Some(number) = scalar_to_i64(&expression_values[expr_index]) {
                        *sum += number * *weight;
                        *count += *weight;
                    }
                }
                AggregateAccumulator::Min { current } => {
                    let Some(expr_index) = plan.expr_index else {
                        continue;
                    };
                    if !expression_valid[expr_index] {
                        continue;
                    }
                    let expr_value = expression_values[expr_index].clone();
                    if expr_value.is_null() {
                        continue;
                    }
                    let next = match current.take() {
                        Some(existing) => match compare_scalar_values(&expr_value, &existing) {
                            Some(std::cmp::Ordering::Less) => expr_value,
                            Some(_) | None => existing,
                        },
                        None => expr_value,
                    };
                    *current = Some(next);
                }
                AggregateAccumulator::Max { current } => {
                    let Some(expr_index) = plan.expr_index else {
                        continue;
                    };
                    if !expression_valid[expr_index] {
                        continue;
                    }
                    let expr_value = expression_values[expr_index].clone();
                    if expr_value.is_null() {
                        continue;
                    }
                    let next = match current.take() {
                        Some(existing) => match compare_scalar_values(&expr_value, &existing) {
                            Some(std::cmp::Ordering::Greater) => expr_value,
                            Some(_) | None => existing,
                        },
                        None => expr_value,
                    };
                    *current = Some(next);
                }
            }
        }
    }

    if decoded_row_count == 0 {
        return Vec::new();
    }

    aggregates
        .iter()
        .zip(accumulators)
        .map(|(agg, accumulator)| match accumulator {
            AggregateAccumulator::CountDistinct { weights } => ScalarValue::Int64(Some(
                weights.values().filter(|weight| **weight > 0).count() as i64,
            )),
            AggregateAccumulator::Count { count } => ScalarValue::Int64(Some(count)),
            AggregateAccumulator::Sum { sum, has_value } => {
                if has_value {
                    scalar_from_i64(sum, agg.output_type())
                } else {
                    ScalarValue::Null
                }
            }
            AggregateAccumulator::Avg { sum, count } => {
                if count != 0 {
                    ScalarValue::Int64(Some(sum / count))
                } else {
                    ScalarValue::Null
                }
            }
            AggregateAccumulator::Min { current } | AggregateAccumulator::Max { current } => {
                current.unwrap_or(ScalarValue::Null)
            }
        })
        .collect()
}

pub(crate) fn build_incremental_aggregate_slot_kinds(
    aggregates: &[DbspAggregateExpr],
) -> Option<Vec<dbsp::IncrementalAggregateSlotKind>> {
    let mut slot_kinds = Vec::with_capacity(aggregates.len());
    for agg in aggregates {
        let kind = match agg.function() {
            DbspAggregateFunction::Count if agg.distinct() => {
                dbsp::IncrementalAggregateSlotKind::CountDistinct
            }
            DbspAggregateFunction::Count => dbsp::IncrementalAggregateSlotKind::Count,
            DbspAggregateFunction::Sum => dbsp::IncrementalAggregateSlotKind::Sum(
                aggregate_value_type_from_dbsp_type(agg.output_type())?,
            ),
            DbspAggregateFunction::Avg => dbsp::IncrementalAggregateSlotKind::Avg,
            DbspAggregateFunction::Min => dbsp::IncrementalAggregateSlotKind::Min(
                aggregate_value_type_from_dbsp_type(agg.output_type())?,
            ),
            DbspAggregateFunction::Max => dbsp::IncrementalAggregateSlotKind::Max(
                aggregate_value_type_from_dbsp_type(agg.output_type())?,
            ),
        };
        slot_kinds.push(kind);
    }
    Some(slot_kinds)
}

pub(crate) fn build_incremental_aggregate_row_evaluator(
    input_schema: Arc<RowSchema>,
    group_keys: Vec<dbsp::circuit::plan::GroupKeyExpr>,
    aggregates: Vec<DbspAggregateExpr>,
    expression_columns: Arc<ExpressionColumnMap>,
    graph_id: String,
    context: &'static str,
) -> impl Fn(&Vec<u8>) -> Option<dbsp::IncrementalAggregateRow<Vec<u8>>> + Send + Sync + 'static {
    let layout = Arc::new(build_count_eval_layout(
        &aggregates,
        input_schema.as_ref(),
        expression_columns.as_ref(),
    ));
    let direct_group_key_columns = direct_group_key_columns(
        &group_keys,
        input_schema.as_ref(),
        expression_columns.as_ref(),
    )
    .map(Arc::new);
    let slot_eval_needs_row = !layout.required_input_columns.is_empty();
    let eval_required_columns =
        slot_eval_needs_row.then(|| Arc::new(layout.required_input_columns.clone()));
    move |bytes: &Vec<u8>| -> Option<dbsp::IncrementalAggregateRow<Vec<u8>>> {
        let row = if slot_eval_needs_row {
            match decode_sparse_row_for_columns(
                bytes,
                eval_required_columns
                    .as_ref()
                    .expect("required columns should be present when decoding rows")
                    .as_ref(),
                input_schema.len(),
            ) {
                Ok(row) => Some(row),
                Err(err) => {
                    tracing::warn!(
                        graph_id = %graph_id,
                        error = %err,
                        "failed to decode aggregate row for incremental aggregate"
                    );
                    return None;
                }
            }
        } else {
            None
        };

        let Some(indices) = direct_group_key_columns.as_ref() else {
            tracing::warn!(
                graph_id = %graph_id,
                "failed to resolve vectorized incremental aggregate group key columns"
            );
            return None;
        };
        let encoded_key = match extract_encoded_row_columns(bytes, indices.as_ref(), false) {
            Ok(Some(encoded_key)) => encoded_key,
            Ok(None) => return None,
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to extract incremental aggregate group key columns"
                );
                return None;
            }
        };

        let row_for_slot_eval = if slot_eval_needs_row {
            row.as_deref().expect("decoded row should be present")
        } else {
            &[]
        };
        let slots = evaluate_incremental_aggregate_row_values(
            layout.as_ref(),
            &aggregates,
            row_for_slot_eval,
            input_schema.as_ref(),
            &graph_id,
            context,
        );
        Some(dbsp::IncrementalAggregateRow {
            key: encoded_key,
            slots,
        })
    }
}

fn evaluate_incremental_aggregate_row_values(
    layout: &CountEvalLayout,
    aggregates: &[DbspAggregateExpr],
    row: &[ScalarValue],
    _schema: &RowSchema,
    graph_id: &str,
    context: &str,
) -> Vec<dbsp::IncrementalAggregateSlotUpdate> {
    let mut filter_results = vec![false; layout.filters.len()];
    for (index, filter) in layout.filters.iter().enumerate() {
        if let Some(column_idx) = layout.filter_direct_columns[index] {
            let value = row.get(column_idx).unwrap_or(&ScalarValue::Null);
            filter_results[index] = match crate::expression::scalar_to_bool(value) {
                Ok(include) => include,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %graph_id,
                        error = %err,
                        "failed to evaluate {context} direct FILTER column"
                    );
                    false
                }
            };
        } else {
            tracing::warn!(
                graph_id = %graph_id,
                expression = ?filter.expr(),
                "unresolved {context} FILTER expression without vectorized precompute column"
            );
            filter_results[index] = false;
        }
    }

    let mut expression_values = vec![ScalarValue::Null; layout.expressions.len()];
    let mut expression_valid = vec![false; layout.expressions.len()];
    for (index, expr) in layout.expressions.iter().enumerate() {
        if let Some(column_idx) = layout.expression_direct_columns[index] {
            if let Some(value) = row.get(column_idx) {
                expression_values[index] = value.clone();
                expression_valid[index] = true;
            }
        } else {
            tracing::warn!(
                graph_id = %graph_id,
                expression = ?expr.expr(),
                "unresolved {context} aggregate expression without vectorized precompute column"
            );
        }
    }

    aggregates
        .iter()
        .zip(layout.plans.iter())
        .map(|(agg, plan)| {
            if let Some(filter_index) = plan.filter_index
                && !filter_results[filter_index]
            {
                return match agg.function() {
                    DbspAggregateFunction::Count if !agg.distinct() => {
                        dbsp::IncrementalAggregateSlotUpdate::Count(0)
                    }
                    _ => dbsp::IncrementalAggregateSlotUpdate::Value(None),
                };
            }

            match agg.function() {
                DbspAggregateFunction::Count if !agg.distinct() => match plan.expr_index {
                    Some(expr_index) => {
                        if expression_valid[expr_index] && !expression_values[expr_index].is_null()
                        {
                            dbsp::IncrementalAggregateSlotUpdate::Count(1)
                        } else {
                            dbsp::IncrementalAggregateSlotUpdate::Count(0)
                        }
                    }
                    None => dbsp::IncrementalAggregateSlotUpdate::Count(1),
                },
                _ => match plan.expr_index {
                    Some(expr_index) if expression_valid[expr_index] => {
                        dbsp::IncrementalAggregateSlotUpdate::Value(
                            incremental_aggregate_value_from_scalar(
                                &expression_values[expr_index],
                                graph_id,
                                context,
                            ),
                        )
                    }
                    _ => dbsp::IncrementalAggregateSlotUpdate::Value(None),
                },
            }
        })
        .collect()
}

fn aggregate_value_type_from_dbsp_type(
    value_type: &DbspScalarType,
) -> Option<dbsp::AggregateValueType> {
    match value_type {
        DbspScalarType::Int64 => Some(dbsp::AggregateValueType::Int64),
        DbspScalarType::TimestampMillis => Some(dbsp::AggregateValueType::TimestampMillis),
        DbspScalarType::Utf8 => Some(dbsp::AggregateValueType::Utf8),
        DbspScalarType::Bool => None,
    }
}

fn incremental_aggregate_value_from_scalar(
    value: &ScalarValue,
    graph_id: &str,
    context: &str,
) -> Option<dbsp::AggregateValue> {
    match value {
        ScalarValue::Int64(Some(value)) => Some(dbsp::AggregateValue::Int64(*value)),
        ScalarValue::TimestampMillisecond(Some(value), _) => {
            Some(dbsp::AggregateValue::TimestampMillis(*value))
        }
        ScalarValue::Utf8(Some(value)) => Some(dbsp::AggregateValue::Utf8(value.clone())),
        ScalarValue::Int64(None)
        | ScalarValue::TimestampMillisecond(None, _)
        | ScalarValue::Utf8(None)
        | ScalarValue::Null => None,
        other => {
            tracing::warn!(
                graph_id = %graph_id,
                value = ?other,
                "unsupported {context} aggregate value for incremental aggregate"
            );
            None
        }
    }
}

pub(crate) fn scalar_from_incremental_aggregate_value(value: &dbsp::AggregateValue) -> ScalarValue {
    match value {
        dbsp::AggregateValue::Null(value_type) => match value_type {
            dbsp::AggregateValueType::Int64 => ScalarValue::Int64(None),
            dbsp::AggregateValueType::TimestampMillis => {
                ScalarValue::TimestampMillisecond(None, None)
            }
            dbsp::AggregateValueType::Utf8 => ScalarValue::Utf8(None),
        },
        dbsp::AggregateValue::Int64(value) => ScalarValue::Int64(Some(*value)),
        dbsp::AggregateValue::TimestampMillis(value) => {
            ScalarValue::TimestampMillisecond(Some(*value), None)
        }
        dbsp::AggregateValue::Utf8(value) => ScalarValue::Utf8(Some(value.clone())),
    }
}

fn direct_group_key_columns(
    group_keys: &[dbsp::circuit::plan::GroupKeyExpr],
    schema: &RowSchema,
    expression_columns: &ExpressionColumnMap,
) -> Option<Vec<usize>> {
    group_keys
        .iter()
        .map(|key_expr| {
            resolved_expression_column_index(key_expr.expression(), schema, expression_columns)
        })
        .collect()
}

fn direct_column_index(
    expr: &dbsp::circuit::plan::DbspExpression,
    schema: &RowSchema,
) -> Option<usize> {
    match expr.expr() {
        Expr::Alias(alias) => direct_column_index_expression(alias.expr.as_ref(), schema),
        other => direct_column_index_expression(other, schema),
    }
}

fn direct_column_index_expression(expr: &Expr, schema: &RowSchema) -> Option<usize> {
    match expr {
        Expr::Column(column) => resolve_direct_column(schema, column),
        Expr::Alias(alias) => direct_column_index_expression(alias.expr.as_ref(), schema),
        _ => None,
    }
}

fn resolve_direct_column(schema: &RowSchema, column: &Column) -> Option<usize> {
    let qualified = column.flat_name();
    schema
        .field_index(&qualified)
        .or_else(|| schema.field_index(&column.name))
}

fn resolved_expression_column_index(
    expr: &dbsp::circuit::plan::DbspExpression,
    schema: &RowSchema,
    expression_columns: &ExpressionColumnMap,
) -> Option<usize> {
    direct_column_index(expr, schema).or_else(|| {
        expression_columns
            .get(&expression_lookup_key(expr.expr()))
            .copied()
    })
}

fn expression_lookup_key(expr: &Expr) -> String {
    match expr {
        Expr::Alias(alias) => expression_lookup_key(alias.expr.as_ref()),
        other => other.to_string(),
    }
}

fn decode_sparse_row_for_columns(
    encoded: &[u8],
    columns: &[usize],
    row_width: usize,
) -> Result<Vec<ScalarValue>> {
    if columns.is_empty() {
        return Ok(vec![ScalarValue::Null; row_width]);
    }
    let selected = extract_encoded_row_columns(encoded, columns, false)?
        .ok_or_else(|| anyhow!("sparse row extraction unexpectedly returned null"))?;
    let values = decode_projected_row_key(&selected)?;
    if values.len() != columns.len() {
        return Err(anyhow!(
            "sparse row extraction expected {} columns but decoded {}",
            columns.len(),
            values.len()
        ));
    }
    let mut row = vec![ScalarValue::Null; row_width];
    for (slot, column_idx) in columns.iter().copied().enumerate() {
        row[column_idx] = values[slot].clone();
    }
    Ok(row)
}
