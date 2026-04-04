use super::*;
use crate::encoding::extract_encoded_row_columns;
use datafusion::common::Column;
use datafusion::logical_expr::Expr;

#[derive(Clone)]
struct CountEvalLayout {
    filters: Vec<dbsp::DbspExpression>,
    expressions: Vec<dbsp::DbspExpression>,
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
        upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let input_schema = Arc::clone(node.input_schema());
        let group_keys = node.group_keys().to_vec();
        let direct_group_key_columns =
            direct_group_key_columns(&group_keys, input_schema.as_ref()).map(Arc::new);
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

        if aggregates
            .iter()
            .all(|agg| agg.function() == &DbspAggregateFunction::Count)
        {
            let slot_kinds = build_count_aggregate_slot_kinds(&aggregates);
            let row_evaluator = build_count_row_evaluator(
                Arc::clone(&input_schema),
                group_keys.clone(),
                aggregates.clone(),
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
                Arc::clone(&input_schema),
                group_keys.clone(),
                aggregates.clone(),
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

        let key_schema = Arc::clone(&input_schema);
        let key_columns = direct_group_key_columns;
        let key_graph_id = graph_id.clone();
        let key_extractor = move |bytes: &Vec<u8>| -> Option<Vec<u8>> {
            if let Some(indices) = key_columns.as_ref() {
                return match extract_encoded_row_columns(bytes, indices.as_ref(), false) {
                    Ok(selected) => selected,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %key_graph_id,
                            error = %err,
                            "failed to extract aggregate group key columns"
                        );
                        None
                    }
                };
            }
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

            let outputs = evaluate_aggregate_values(
                &aggregates,
                &decoded,
                agg_schema.as_ref(),
                &agg_graph_id,
                "aggregate",
            );

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
        upstream: DeltaHandleStream,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let aggregate = &node.aggregate;
        let input_schema = Arc::clone(aggregate.input_schema());
        let group_keys = aggregate.group_keys().to_vec();
        let direct_group_key_columns =
            direct_group_key_columns(&group_keys, input_schema.as_ref()).map(Arc::new);
        let aggregates = aggregate.aggregates().to_vec();
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

        let key_schema = Arc::clone(&input_schema);
        let key_columns = direct_group_key_columns;
        let key_graph_id = graph_id.clone();
        let key_extractor = move |bytes: &Vec<u8>| -> Option<Vec<u8>> {
            if let Some(indices) = key_columns.as_ref() {
                return match extract_encoded_row_columns(bytes, indices.as_ref(), false) {
                    Ok(selected) => selected,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %key_graph_id,
                            error = %err,
                            "failed to extract window aggregate group key columns"
                        );
                        None
                    }
                };
            }
            let row = match decode_projected_row_key(bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %key_graph_id,
                        error = %err,
                        "failed to decode window aggregate row for group key"
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
                            "failed to evaluate window aggregate group key expression"
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
                        "failed to encode window aggregate group key"
                    );
                    None
                }
            }
        };

        let time_schema = Arc::clone(&input_schema);
        let time_graph_id = graph_id.clone();
        let time_expression = node.window.time_expression.clone();
        let direct_time_column =
            direct_column_index(&time_expression, input_schema.as_ref()).map(|index| [index]);
        let time_extractor = move |bytes: &Vec<u8>| -> Option<i64> {
            if let Some(index) = direct_time_column.as_ref() {
                let selected = match extract_encoded_row_columns(bytes, index, false) {
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
                return values.first().and_then(scalar_to_i64);
            }
            let row = match decode_projected_row_key(bytes) {
                Ok(row) => row,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %time_graph_id,
                        error = %err,
                        "failed to decode window aggregate row for time expression"
                    );
                    return None;
                }
            };
            let value = match eval_scalar_expression(&time_expression, &row, time_schema.as_ref()) {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %time_graph_id,
                        error = %err,
                        "failed to evaluate window aggregate time expression"
                    );
                    return None;
                }
            };
            scalar_to_i64(&value)
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
                            "failed to decode window aggregate input row"
                        );
                    }
                }
            }
            if decoded.is_empty() {
                return None;
            }

            let outputs = evaluate_aggregate_values(
                &aggregates,
                &decoded,
                agg_schema.as_ref(),
                &agg_graph_id,
                "window aggregate",
            );

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
            let mut key_values = match decode_projected_row_key(&pair.0.key) {
                Ok(values) => values,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to decode window aggregate group key"
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
                        "failed to decode window aggregate values"
                    );
                    return Vec::new();
                }
            };
            let mut output = Vec::with_capacity(2 + key_values.len() + aggregate_values.len());
            output.push(ScalarValue::TimestampMillisecond(Some(pair.0.start), None));
            output.push(ScalarValue::TimestampMillisecond(Some(pair.0.end), None));
            output.append(&mut key_values);
            output.extend(aggregate_values);
            match encode_projected_row_key(&output) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %project_graph_id,
                        error = %err,
                        "failed to encode window aggregate row"
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
            key_values.extend(pair.1.iter().map(|value| ScalarValue::Int64(Some(*value))));
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
            key_values.extend(pair.1.iter().map(scalar_from_incremental_aggregate_value));
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
    graph_id: String,
    context: &'static str,
) -> impl Fn(&Vec<u8>) -> Option<dbsp::CountAggregateRow<Vec<u8>, Vec<u8>>> + Send + Sync + 'static
{
    let layout = Arc::new(build_count_eval_layout(&aggregates));
    let direct_group_key_columns =
        direct_group_key_columns(&group_keys, input_schema.as_ref()).map(Arc::new);
    move |bytes: &Vec<u8>| -> Option<dbsp::CountAggregateRow<Vec<u8>, Vec<u8>>> {
        let row = match decode_projected_row_key(bytes) {
            Ok(row) => row,
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to decode aggregate row for count aggregate"
                );
                return None;
            }
        };

        let encoded_key = if let Some(indices) = direct_group_key_columns.as_ref() {
            match extract_encoded_row_columns(bytes, indices.as_ref(), false) {
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
            }
        } else {
            let mut key_values = Vec::with_capacity(group_keys.len());
            for key_expr in &group_keys {
                let value = match eval_scalar_expression(
                    key_expr.expression(),
                    &row,
                    input_schema.as_ref(),
                ) {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %graph_id,
                            error = %err,
                            "failed to evaluate count aggregate group key expression"
                        );
                        return None;
                    }
                };
                key_values.push(value);
            }
            match encode_projected_row_key(&key_values) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %graph_id,
                        error = %err,
                        "failed to encode count aggregate group key"
                    );
                    return None;
                }
            }
        };

        let counts = evaluate_count_row_values(
            layout.as_ref(),
            &aggregates,
            &row,
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

fn build_count_eval_layout(aggregates: &[DbspAggregateExpr]) -> CountEvalLayout {
    let mut filters = Vec::new();
    let mut expressions = Vec::new();
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
        expressions,
        plans,
    }
}

fn evaluate_count_row_values(
    layout: &CountEvalLayout,
    aggregates: &[DbspAggregateExpr],
    row: &[ScalarValue],
    schema: &RowSchema,
    graph_id: &str,
    context: &str,
) -> Vec<dbsp::CountAggregateSlotUpdate<Vec<u8>>> {
    let mut filter_results = vec![false; layout.filters.len()];
    for (index, filter) in layout.filters.iter().enumerate() {
        filter_results[index] = match eval_expression(filter, row, schema) {
            Ok(include) => include,
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to evaluate {context} FILTER expression"
                );
                false
            }
        };
    }

    let mut expression_values = vec![ScalarValue::Null; layout.expressions.len()];
    let mut expression_valid = vec![false; layout.expressions.len()];
    for (index, expr) in layout.expressions.iter().enumerate() {
        match eval_scalar_expression(expr, row, schema) {
            Ok(value) => {
                expression_values[index] = value;
                expression_valid[index] = true;
            }
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to evaluate {context} aggregate expression"
                );
            }
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
    graph_id: String,
    context: &'static str,
) -> impl Fn(&Vec<u8>) -> Option<dbsp::IncrementalAggregateRow<Vec<u8>>> + Send + Sync + 'static {
    let layout = Arc::new(build_count_eval_layout(&aggregates));
    let direct_group_key_columns =
        direct_group_key_columns(&group_keys, input_schema.as_ref()).map(Arc::new);
    move |bytes: &Vec<u8>| -> Option<dbsp::IncrementalAggregateRow<Vec<u8>>> {
        let row = match decode_projected_row_key(bytes) {
            Ok(row) => row,
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to decode aggregate row for incremental aggregate"
                );
                return None;
            }
        };

        let encoded_key = if let Some(indices) = direct_group_key_columns.as_ref() {
            match extract_encoded_row_columns(bytes, indices.as_ref(), false) {
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
            }
        } else {
            let mut key_values = Vec::with_capacity(group_keys.len());
            for key_expr in &group_keys {
                let value = match eval_scalar_expression(
                    key_expr.expression(),
                    &row,
                    input_schema.as_ref(),
                ) {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %graph_id,
                            error = %err,
                            "failed to evaluate incremental aggregate group key expression"
                        );
                        return None;
                    }
                };
                key_values.push(value);
            }
            match encode_projected_row_key(&key_values) {
                Ok(encoded) => encoded,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %graph_id,
                        error = %err,
                        "failed to encode incremental aggregate group key"
                    );
                    return None;
                }
            }
        };

        let slots = evaluate_incremental_aggregate_row_values(
            layout.as_ref(),
            &aggregates,
            &row,
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
    schema: &RowSchema,
    graph_id: &str,
    context: &str,
) -> Vec<dbsp::IncrementalAggregateSlotUpdate> {
    let mut filter_results = vec![false; layout.filters.len()];
    for (index, filter) in layout.filters.iter().enumerate() {
        filter_results[index] = match eval_expression(filter, row, schema) {
            Ok(include) => include,
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to evaluate {context} FILTER expression"
                );
                false
            }
        };
    }

    let mut expression_values = vec![ScalarValue::Null; layout.expressions.len()];
    let mut expression_valid = vec![false; layout.expressions.len()];
    for (index, expr) in layout.expressions.iter().enumerate() {
        match eval_scalar_expression(expr, row, schema) {
            Ok(value) => {
                expression_values[index] = value;
                expression_valid[index] = true;
            }
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to evaluate {context} aggregate expression"
                );
            }
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
) -> Option<Vec<usize>> {
    group_keys
        .iter()
        .map(|key_expr| direct_column_index(key_expr.expression(), schema))
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
