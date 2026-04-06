use super::*;
use crate::encoding::{
    EncodedRowScalar, concat_encoded_rows, extract_encoded_row_columns,
    extract_encoded_row_i64_like_column, extract_encoded_row_scalars,
};
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
    required_input_positions: HashMap<usize, usize>,
    plans: Vec<CountEvalPlan>,
}

#[derive(Clone, Copy)]
struct CountEvalPlan {
    filter_index: Option<usize>,
    expr_index: Option<usize>,
}

enum EncodedAggregateAccumulator {
    Count {
        count: i64,
    },
    CountDistinct {
        weights: HashMap<EncodedRowScalar, i64>,
    },
    Sum {
        sum: i64,
        has_value: bool,
    },
    Avg {
        sum: i64,
        count: i64,
    },
    Min {
        current: Option<EncodedRowScalar>,
    },
    Max {
        current: Option<EncodedRowScalar>,
    },
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
            match encode_aggregate_values_from_encoded(
                agg_layout.as_ref(),
                &aggregates,
                values,
                &agg_graph_id,
                "aggregate",
            ) {
                Ok(Some(encoded)) => Some(encoded),
                Ok(None) => None,
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
            match extract_encoded_row_i64_like_column(bytes, direct_time_column) {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %time_graph_id,
                        error = %err,
                        "failed to extract window aggregate time column"
                    );
                    None
                }
            }
        };

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
            match encode_aggregate_values_from_encoded(
                agg_layout.as_ref(),
                &aggregates,
                values,
                &agg_graph_id,
                "window aggregate",
            ) {
                Ok(Some(encoded)) => Some(encoded),
                Ok(None) => None,
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
    move |bytes: &Vec<u8>| -> Option<dbsp::CountAggregateRow<Vec<u8>, Vec<u8>>> {
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

        let counts = evaluate_count_row_values(
            layout.as_ref(),
            &aggregates,
            bytes,
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

    let required_input_columns = required_input_columns.into_iter().collect::<Vec<_>>();
    let required_input_positions = required_input_columns
        .iter()
        .copied()
        .enumerate()
        .map(|(slot, column)| (column, slot))
        .collect::<HashMap<_, _>>();

    CountEvalLayout {
        filters,
        filter_direct_columns,
        expressions,
        expression_direct_columns,
        required_input_columns,
        required_input_positions,
        plans,
    }
}

fn evaluate_count_row_values(
    layout: &CountEvalLayout,
    aggregates: &[DbspAggregateExpr],
    row_bytes: &[u8],
    _schema: &RowSchema,
    graph_id: &str,
    context: &str,
) -> Vec<dbsp::CountAggregateSlotUpdate<Vec<u8>>> {
    let decoded =
        match extract_encoded_row_scalars(row_bytes, layout.required_input_columns.as_slice()) {
            Ok(decoded) => decoded,
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to decode {context} count aggregate row inputs"
                );
                return aggregates
                    .iter()
                    .map(|agg| {
                        if agg.distinct() {
                            dbsp::CountAggregateSlotUpdate::Distinct(None)
                        } else {
                            dbsp::CountAggregateSlotUpdate::Linear(0)
                        }
                    })
                    .collect();
            }
        };

    let mut filter_results = vec![false; layout.filters.len()];
    for (index, filter) in layout.filters.iter().enumerate() {
        if let Some(column_idx) = layout.filter_direct_columns[index] {
            let decoded_idx = layout.required_input_positions.get(&column_idx).copied();
            let value = decoded_idx
                .and_then(|slot| decoded.get(slot))
                .and_then(|scalar| scalar.as_ref());
            filter_results[index] = match bool_from_encoded_scalar(value) {
                Ok(include) => include,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %graph_id,
                        error = %err,
                        "failed to evaluate {context} direct FILTER column"
                    );
                    false
                }
            }
        } else {
            tracing::warn!(
                graph_id = %graph_id,
                expression = ?filter.expr(),
                "unresolved {context} FILTER expression without vectorized precompute column"
            );
            filter_results[index] = false;
        }
    }

    let mut expression_values = vec![None; layout.expressions.len()];
    let mut expression_valid = vec![false; layout.expressions.len()];
    for (index, expr) in layout.expressions.iter().enumerate() {
        if let Some(column_idx) = layout.expression_direct_columns[index] {
            if let Some(decoded_idx) = layout.required_input_positions.get(&column_idx).copied() {
                expression_values[index] = decoded.get(decoded_idx).cloned().flatten();
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
                    if expression_valid[expr_index] && expression_values[expr_index].is_some() {
                        if agg.distinct() {
                            let encoded =
                                expression_values[expr_index].as_ref().and_then(|value| {
                                    encode_single_encoded_scalar_key(value)
                                        .map(Some)
                                        .unwrap_or_else(|err| {
                                            tracing::warn!(
                                                graph_id = %graph_id,
                                                error = %err,
                                                "failed to encode count aggregate DISTINCT value"
                                            );
                                            None
                                        })
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

fn encode_aggregate_values_from_encoded(
    layout: &CountEvalLayout,
    aggregates: &[DbspAggregateExpr],
    values: &[(Vec<u8>, i64)],
    graph_id: &str,
    context: &str,
) -> Result<Option<Vec<u8>>> {
    if aggregates.is_empty() {
        return Ok(None);
    }

    let mut accumulators = Vec::with_capacity(aggregates.len());
    for agg in aggregates {
        accumulators.push(match agg.function() {
            DbspAggregateFunction::Count if agg.distinct() => {
                EncodedAggregateAccumulator::CountDistinct {
                    weights: HashMap::new(),
                }
            }
            DbspAggregateFunction::Count => EncodedAggregateAccumulator::Count { count: 0 },
            DbspAggregateFunction::Sum => EncodedAggregateAccumulator::Sum {
                sum: 0,
                has_value: false,
            },
            DbspAggregateFunction::Avg => EncodedAggregateAccumulator::Avg { sum: 0, count: 0 },
            DbspAggregateFunction::Min => EncodedAggregateAccumulator::Min { current: None },
            DbspAggregateFunction::Max => EncodedAggregateAccumulator::Max { current: None },
        });
    }

    let mut filter_results = vec![false; layout.filters.len()];
    let mut expression_values = vec![None; layout.expressions.len()];
    let mut expression_valid = vec![false; layout.expressions.len()];
    let mut decoded_row_count = 0usize;

    for (value, weight) in values {
        if *weight == 0 {
            continue;
        }
        let decoded =
            match extract_encoded_row_scalars(value, layout.required_input_columns.as_slice()) {
                Ok(decoded) => decoded,
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
                let decoded_idx = layout.required_input_positions.get(&column_idx).copied();
                let value = decoded_idx
                    .and_then(|slot| decoded.get(slot))
                    .and_then(|scalar| scalar.as_ref());
                filter_results[index] = match bool_from_encoded_scalar(value) {
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
                if let Some(decoded_idx) = layout.required_input_positions.get(&column_idx).copied()
                {
                    expression_values[index] = decoded.get(decoded_idx).cloned().flatten();
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
                EncodedAggregateAccumulator::CountDistinct { weights } => {
                    let Some(expr_index) = plan.expr_index else {
                        continue;
                    };
                    if !expression_valid[expr_index] {
                        continue;
                    }
                    let Some(expr_value) = expression_values[expr_index].clone() else {
                        continue;
                    };
                    let entry = weights.entry(expr_value.clone()).or_insert(0);
                    *entry += *weight;
                    if *entry == 0 {
                        weights.remove(&expr_value);
                    }
                }
                EncodedAggregateAccumulator::Count { count } => match plan.expr_index {
                    Some(expr_index) => {
                        if expression_valid[expr_index] && expression_values[expr_index].is_some() {
                            *count += *weight;
                        }
                    }
                    None => *count += *weight,
                },
                EncodedAggregateAccumulator::Sum { sum, has_value } => {
                    let Some(expr_index) = plan.expr_index else {
                        continue;
                    };
                    if !expression_valid[expr_index] {
                        continue;
                    }
                    if let Some(number) =
                        i64_from_encoded_scalar(expression_values[expr_index].as_ref())
                    {
                        *sum += number * *weight;
                        *has_value = true;
                    }
                }
                EncodedAggregateAccumulator::Avg { sum, count } => {
                    let Some(expr_index) = plan.expr_index else {
                        continue;
                    };
                    if !expression_valid[expr_index] {
                        continue;
                    }
                    if let Some(number) =
                        i64_from_encoded_scalar(expression_values[expr_index].as_ref())
                    {
                        *sum += number * *weight;
                        *count += *weight;
                    }
                }
                EncodedAggregateAccumulator::Min { current } => {
                    let Some(expr_index) = plan.expr_index else {
                        continue;
                    };
                    if !expression_valid[expr_index] {
                        continue;
                    }
                    let Some(expr_value) = expression_values[expr_index].clone() else {
                        continue;
                    };
                    let next = match current.take() {
                        Some(existing) => match compare_encoded_scalars(&expr_value, &existing) {
                            Some(std::cmp::Ordering::Less) => expr_value,
                            Some(_) | None => existing,
                        },
                        None => expr_value,
                    };
                    *current = Some(next);
                }
                EncodedAggregateAccumulator::Max { current } => {
                    let Some(expr_index) = plan.expr_index else {
                        continue;
                    };
                    if !expression_valid[expr_index] {
                        continue;
                    }
                    let Some(expr_value) = expression_values[expr_index].clone() else {
                        continue;
                    };
                    let next = match current.take() {
                        Some(existing) => match compare_encoded_scalars(&expr_value, &existing) {
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
        return Ok(None);
    }

    let count =
        u32::try_from(aggregates.len()).map_err(|_| anyhow!("too many columns in MV key"))?;
    let mut encoded = Vec::with_capacity(4 + (aggregates.len() * 9));
    encoded.extend_from_slice(&count.to_le_bytes());
    for (agg, accumulator) in aggregates.iter().zip(accumulators.into_iter()) {
        match accumulator {
            EncodedAggregateAccumulator::CountDistinct { weights } => {
                append_encoded_i64(
                    weights.values().filter(|weight| **weight > 0).count() as i64,
                    &mut encoded,
                );
            }
            EncodedAggregateAccumulator::Count { count } => {
                append_encoded_i64(count, &mut encoded);
            }
            EncodedAggregateAccumulator::Sum { sum, has_value } => {
                if has_value {
                    append_encoded_sum_like_value(sum, agg.output_type(), &mut encoded)?;
                } else {
                    append_untyped_null(&mut encoded);
                }
            }
            EncodedAggregateAccumulator::Avg { sum, count } => {
                if count != 0 {
                    append_encoded_i64(sum / count, &mut encoded);
                } else {
                    append_untyped_null(&mut encoded);
                }
            }
            EncodedAggregateAccumulator::Min { current }
            | EncodedAggregateAccumulator::Max { current } => {
                if let Some(value) = current.as_ref() {
                    append_encoded_scalar(value, &mut encoded)?;
                } else {
                    append_untyped_null(&mut encoded);
                }
            }
        }
    }
    Ok(Some(encoded))
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
    move |bytes: &Vec<u8>| -> Option<dbsp::IncrementalAggregateRow<Vec<u8>>> {
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

        let slots = evaluate_incremental_aggregate_row_values(
            layout.as_ref(),
            &aggregates,
            bytes,
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
    row_bytes: &[u8],
    _schema: &RowSchema,
    graph_id: &str,
    context: &str,
) -> Vec<dbsp::IncrementalAggregateSlotUpdate> {
    let decoded =
        match extract_encoded_row_scalars(row_bytes, layout.required_input_columns.as_slice()) {
            Ok(decoded) => decoded,
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to decode {context} incremental aggregate row inputs"
                );
                return aggregates
                    .iter()
                    .map(|agg| match agg.function() {
                        DbspAggregateFunction::Count if !agg.distinct() => {
                            dbsp::IncrementalAggregateSlotUpdate::Count(0)
                        }
                        _ => dbsp::IncrementalAggregateSlotUpdate::Value(None),
                    })
                    .collect();
            }
        };

    let mut filter_results = vec![false; layout.filters.len()];
    for (index, filter) in layout.filters.iter().enumerate() {
        if let Some(column_idx) = layout.filter_direct_columns[index] {
            let decoded_idx = layout.required_input_positions.get(&column_idx).copied();
            let value = decoded_idx
                .and_then(|slot| decoded.get(slot))
                .and_then(|scalar| scalar.as_ref());
            filter_results[index] = match bool_from_encoded_scalar(value) {
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

    let mut expression_values = vec![None; layout.expressions.len()];
    let mut expression_valid = vec![false; layout.expressions.len()];
    for (index, expr) in layout.expressions.iter().enumerate() {
        if let Some(column_idx) = layout.expression_direct_columns[index] {
            if let Some(decoded_idx) = layout.required_input_positions.get(&column_idx).copied() {
                expression_values[index] = decoded.get(decoded_idx).cloned().flatten();
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
                        if expression_valid[expr_index] && expression_values[expr_index].is_some() {
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
                            incremental_aggregate_value_from_encoded_scalar(
                                expression_values[expr_index].as_ref(),
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

fn bool_from_encoded_scalar(value: Option<&EncodedRowScalar>) -> Result<bool> {
    match value {
        Some(EncodedRowScalar::Bool(flag)) => Ok(*flag),
        None => Ok(false),
        Some(other) => Err(anyhow!("expected boolean value, found {other:?}")),
    }
}

fn i64_from_encoded_scalar(value: Option<&EncodedRowScalar>) -> Option<i64> {
    match value {
        Some(EncodedRowScalar::Int64(value) | EncodedRowScalar::TimestampMillis(value)) => {
            Some(*value)
        }
        _ => None,
    }
}

fn compare_encoded_scalars(
    left: &EncodedRowScalar,
    right: &EncodedRowScalar,
) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (EncodedRowScalar::Int64(l), EncodedRowScalar::Int64(r)) => Some(l.cmp(r)),
        (EncodedRowScalar::TimestampMillis(l), EncodedRowScalar::TimestampMillis(r)) => {
            Some(l.cmp(r))
        }
        (EncodedRowScalar::Utf8(l), EncodedRowScalar::Utf8(r)) => Some(l.cmp(r)),
        (EncodedRowScalar::Bool(l), EncodedRowScalar::Bool(r)) => Some(l.cmp(r)),
        _ => None,
    }
}

fn append_encoded_scalar(value: &EncodedRowScalar, encoded: &mut Vec<u8>) -> Result<()> {
    match value {
        EncodedRowScalar::Int64(value) => {
            append_encoded_i64(*value, encoded);
        }
        EncodedRowScalar::Utf8(value) => {
            encoded.push(0x02);
            let bytes = value.as_bytes();
            let len = u32::try_from(bytes.len())
                .map_err(|_| anyhow!("utf8 value too large for MV key"))?;
            encoded.extend_from_slice(&len.to_le_bytes());
            encoded.extend_from_slice(bytes);
        }
        EncodedRowScalar::TimestampMillis(value) => {
            append_encoded_timestamp(*value, encoded);
        }
        EncodedRowScalar::Bool(value) => {
            encoded.push(0x04);
            encoded.push(if *value { 1 } else { 0 });
        }
    }
    Ok(())
}

fn encode_single_encoded_scalar_key(value: &EncodedRowScalar) -> Result<Vec<u8>> {
    let mut encoded = Vec::with_capacity(13);
    encoded.extend_from_slice(&1_u32.to_le_bytes());
    append_encoded_scalar(value, &mut encoded)?;
    Ok(encoded)
}

fn append_encoded_i64(value: i64, encoded: &mut Vec<u8>) {
    encoded.push(0x01);
    encoded.extend_from_slice(&value.to_le_bytes());
}

fn append_encoded_timestamp(value: i64, encoded: &mut Vec<u8>) {
    encoded.push(0x03);
    encoded.extend_from_slice(&value.to_le_bytes());
}

fn append_untyped_null(encoded: &mut Vec<u8>) {
    encoded.push(0x00);
}

fn encode_window_bounds(start: i64, end: i64) -> Result<Vec<u8>> {
    let mut encoded = Vec::with_capacity(4 + 2 * 9);
    encoded.extend_from_slice(&2_u32.to_le_bytes());
    append_encoded_timestamp(start, &mut encoded);
    append_encoded_timestamp(end, &mut encoded);
    Ok(encoded)
}

fn encode_count_values(values: &[i64]) -> Result<Vec<u8>> {
    let count = u32::try_from(values.len()).map_err(|_| anyhow!("too many columns in MV key"))?;
    let mut encoded = Vec::with_capacity(4 + (values.len() * 9));
    encoded.extend_from_slice(&count.to_le_bytes());
    for value in values {
        append_encoded_i64(*value, &mut encoded);
    }
    Ok(encoded)
}

fn encode_incremental_aggregate_values(values: &[dbsp::AggregateValue]) -> Result<Vec<u8>> {
    let count = u32::try_from(values.len()).map_err(|_| anyhow!("too many columns in MV key"))?;
    let mut encoded = Vec::with_capacity(4 + (values.len() * 9));
    encoded.extend_from_slice(&count.to_le_bytes());
    for value in values {
        match value {
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::Int64) => encoded.push(0x05),
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::TimestampMillis) => {
                encoded.push(0x07);
            }
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::Utf8) => encoded.push(0x06),
            dbsp::AggregateValue::Int64(value) => append_encoded_i64(*value, &mut encoded),
            dbsp::AggregateValue::TimestampMillis(value) => {
                append_encoded_timestamp(*value, &mut encoded);
            }
            dbsp::AggregateValue::Utf8(value) => {
                encoded.push(0x02);
                let bytes = value.as_bytes();
                let len = u32::try_from(bytes.len())
                    .map_err(|_| anyhow!("utf8 value too large for MV key"))?;
                encoded.extend_from_slice(&len.to_le_bytes());
                encoded.extend_from_slice(bytes);
            }
        }
    }
    Ok(encoded)
}

fn append_encoded_sum_like_value(
    value: i64,
    output_type: &DbspScalarType,
    encoded: &mut Vec<u8>,
) -> Result<()> {
    match output_type {
        DbspScalarType::Int64 => append_encoded_i64(value, encoded),
        DbspScalarType::TimestampMillis => append_encoded_timestamp(value, encoded),
        DbspScalarType::Utf8 | DbspScalarType::Bool => {
            return Err(anyhow!(
                "unsupported aggregate SUM output type for encoded output: {output_type:?}"
            ));
        }
    }
    Ok(())
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

fn incremental_aggregate_value_from_encoded_scalar(
    value: Option<&EncodedRowScalar>,
    graph_id: &str,
    context: &str,
) -> Option<dbsp::AggregateValue> {
    match value {
        Some(EncodedRowScalar::Int64(value)) => Some(dbsp::AggregateValue::Int64(*value)),
        Some(EncodedRowScalar::TimestampMillis(value)) => {
            Some(dbsp::AggregateValue::TimestampMillis(*value))
        }
        Some(EncodedRowScalar::Utf8(value)) => Some(dbsp::AggregateValue::Utf8(value.clone())),
        None => None,
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
