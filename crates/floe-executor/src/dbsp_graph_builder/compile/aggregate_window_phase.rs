use super::*;
use crate::delta_batch::{DeltaBatchBuffer, DeltaBatchConfig};
#[cfg(test)]
use crate::encoding::extract_encoded_row_scalars;
use crate::encoding::{EncodedRowScalar, concat_encoded_rows};
use crate::vectorized_keys::VectorizedEncodedKeyExtractor;
use anyhow::ensure;
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Column;
use datafusion::logical_expr::Expr;
use std::collections::{BTreeSet, HashMap, HashSet};

type ExpressionColumnMap = HashMap<String, usize>;

fn project_encoded_delta_batch<K>(
    delta_values: &[(K, i64)],
    projector: impl Fn(&K) -> Vec<u8>,
) -> Vec<(Vec<u8>, i64)> {
    let mut projected = HashMap::<Vec<u8>, i64>::new();
    for (key, weight) in delta_values {
        if *weight == 0 {
            continue;
        }
        let encoded = projector(key);
        if encoded.is_empty() {
            continue;
        }
        let entry = projected.entry(encoded.clone()).or_insert(0);
        *entry += *weight;
        if *entry == 0 {
            projected.remove(&encoded);
        }
    }
    projected.into_iter().collect()
}

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

fn count_eval_record_batch(
    layout: &CountEvalLayout,
    input_schema: &RowSchema,
    rows: impl IntoIterator<Item = (Vec<u8>, i64)>,
) -> Result<Option<RecordBatch>> {
    let arrow_schema = input_schema.to_arrow_schema();
    let eval_schema = projected_arrow_schema(&arrow_schema, &layout.required_input_columns)?;
    let input_columns = Arc::<[usize]>::from(layout.required_input_columns.clone());
    let mut buffer = DeltaBatchBuffer::new_projected(
        eval_schema,
        input_columns,
        false,
        DeltaBatchConfig {
            max_rows: usize::MAX,
            max_bytes: usize::MAX,
        },
    )
    .context("create vectorized aggregate input buffer")?;
    for (row, weight) in rows {
        let _ = buffer
            .push(row, weight, None)
            .context("decode vectorized aggregate input row")?;
    }
    buffer
        .flush_manual()
        .context("flush vectorized aggregate input batch")
}

fn projected_arrow_schema(
    input_schema: &datafusion::arrow::datatypes::SchemaRef,
    columns: &[usize],
) -> Result<datafusion::arrow::datatypes::SchemaRef> {
    let fields = columns
        .iter()
        .map(|idx| {
            input_schema
                .fields()
                .get(*idx)
                .map(|field| (**field).clone())
                .ok_or_else(|| {
                    anyhow!(
                        "aggregate input column {idx} is out of bounds for schema width {}",
                        input_schema.fields().len()
                    )
                })
        })
        .collect::<Result<Vec<Field>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

enum EncodedAggregateAccumulator {
    Count {
        count: i64,
    },
    CountDistinct {
        weights: HashMap<EncodedRowScalar, i64>,
    },
    Sum {
        sum: i128,
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
        node_idx: usize,
        node: &DbspAggregateNode,
        mut upstream: DeltaHandleStream,
        append_only_input: bool,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let input_schema = Arc::clone(node.input_schema());
        let state_namespace = self.operator_state_namespace(node_idx, "aggregate");
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
            let row_evaluator = build_count_batch_row_evaluator(
                Arc::clone(&eval_schema),
                group_keys.clone(),
                aggregates.clone(),
                Arc::clone(&expression_columns),
                graph_id.clone(),
                "aggregate",
            );

            let count_aggregate =
                DbspCountAggregate::new_batch_with_state_namespace_and_append_only_input::<
                    Vec<u8>,
                    Vec<u8>,
                    Vec<u8>,
                    _,
                >(
                    &upstream,
                    Some(state_namespace),
                    row_evaluator,
                    slot_kinds,
                    append_only_input,
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
            return Ok(mapped);
        }

        if let Some(slot_kinds) = build_incremental_aggregate_slot_kinds(&aggregates) {
            let row_evaluator = build_incremental_aggregate_batch_row_evaluator(
                Arc::clone(&eval_schema),
                group_keys.clone(),
                aggregates.clone(),
                Arc::clone(&expression_columns),
                graph_id.clone(),
                "aggregate",
            );

            let incremental_aggregate =
                dbsp::DbspIncrementalAggregate::new_batch_with_state_namespace_and_append_only_input::<
                    Vec<u8>,
                    Vec<u8>,
                    _,
                >(
                    &upstream,
                    Some(state_namespace),
                    row_evaluator,
                    slot_kinds,
                    append_only_input,
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
            return Ok(mapped);
        }

        let key_extractor = Arc::new(
            VectorizedEncodedKeyExtractor::new(
                eval_schema.to_arrow_schema(),
                Arc::clone(&direct_group_key_columns),
            )
            .context("initialize vectorized aggregate key extractor")?,
        );
        let key_graph_id = graph_id.clone();
        let key_error_handler = Arc::clone(&aggregate_error_handler);
        let key_extractor = move |delta_values: &[(Vec<u8>, i64)]| match key_extractor
            .extract_keyed_deltas(delta_values)
        {
            Ok(keyed) => keyed,
            Err(err) => {
                tracing::warn!(
                    graph_id = %key_graph_id,
                    error = %err,
                    "failed to evaluate vectorized aggregate group keys"
                );
                report_operator_closure_error(
                    &key_error_handler,
                    "failed to evaluate vectorized aggregate group keys",
                    err,
                );
                Vec::new()
            }
        };

        let agg_graph_id = graph_id.clone();
        let agg_error_handler = Arc::clone(&aggregate_error_handler);
        let agg_layout = Arc::new(build_count_eval_layout(
            &aggregates,
            eval_schema.as_ref(),
            expression_columns.as_ref(),
        ));
        let agg_input_schema = Arc::clone(&eval_schema);
        let aggregator = move |_key: &Vec<u8>, values: &[(Vec<u8>, i64)]| -> Option<Vec<u8>> {
            if values.is_empty() {
                return None;
            }
            match encode_aggregate_values_from_encoded(
                agg_layout.as_ref(),
                &aggregates,
                agg_input_schema.as_ref(),
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
                    report_operator_closure_error(
                        &agg_error_handler,
                        "failed to encode aggregate output",
                        err,
                    );
                    None
                }
            }
        };

        let aggregate_spec = dbsp::operators::aggregate::AggregateSpec::new(
            format!("aggregate_{graph_id}"),
            aggregator,
        );

        let aggregate =
            DbspAggregate::new_batch_with_state_namespace::<Vec<u8>, Vec<u8>, Vec<u8>, _>(
                &upstream,
                Some(state_namespace),
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

        Ok(mapped)
    }

    pub(crate) async fn compile_window_aggregate(
        &mut self,
        node_idx: usize,
        node: &DbspWindowAggregateNode,
        mut upstream: DeltaHandleStream,
        append_only_input: bool,
        task_events: &GraphTaskSender,
    ) -> Result<DeltaHandleStream> {
        let aggregate = &node.aggregate;
        let state_namespace = self.operator_state_namespace(node_idx, "window_aggregate");
        let input_schema = Arc::clone(aggregate.input_schema());
        let group_keys = aggregate.group_keys().to_vec();
        let aggregates = aggregate.aggregates().to_vec();
        let simple_count_star = is_simple_count_star_aggregate(&aggregates);
        let time_expression = node.window.time_expression.clone();
        let mut precompute_expressions = Vec::new();
        precompute_expressions.extend(group_keys.iter().map(|key| key.expression().clone()));
        precompute_expressions.push(time_expression.clone());
        for agg in &aggregates {
            if let Some(filter) = agg.filter() {
                precompute_expressions.push(filter.clone());
            }
            if !simple_count_star && let Some(expr) = agg.expression() {
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
        let allowed_lateness_ms = node.window.allowed_lateness_ms;
        let graph_id = self.graph_id().to_string();
        let window_events = task_events.clone();
        let window_label = format!("window-aggregate:{graph_id}");
        let window_graph_id = graph_id.clone();
        let window_error_handler: RuntimeErrorHandler = Arc::new(move |err| {
            report_graph_task_error(&window_events, &window_graph_id, window_label.clone(), err);
        });
        let direct_time_column = resolved_expression_column_index(
            &time_expression,
            eval_schema.as_ref(),
            expression_columns.as_ref(),
        )
        .ok_or_else(|| anyhow!("failed to resolve vectorized window aggregate time column"))?;
        let watermark = Arc::clone(&self.watermark);
        let vectorized_window_extractor = Arc::new(
            VectorizedEncodedKeyExtractor::new(
                eval_schema.to_arrow_schema(),
                Arc::clone(&direct_group_key_columns),
            )
            .context("initialize vectorized window aggregate key extractor")?,
        );

        if let DbspWindowPolicy::Session { gap_ms } = &node.window.policy {
            tracing::info!(
                graph_id = %graph_id,
                "using session window aggregate path"
            );
            let key_extractor = Arc::clone(&vectorized_window_extractor);
            let row_graph_id = graph_id.clone();
            let row_error_handler = Arc::clone(&window_error_handler);
            let row_extractor = move |delta_values: &[(Vec<u8>, i64)]| match key_extractor
                .extract_keyed_time_deltas(delta_values, direct_time_column)
            {
                Ok(extracted) => extracted,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %row_graph_id,
                        error = %err,
                        "failed to evaluate vectorized session window aggregate keys"
                    );
                    report_operator_closure_error(
                        &row_error_handler,
                        "failed to evaluate vectorized session window aggregate keys",
                        err,
                    );
                    Vec::new()
                }
            };

            let agg_graph_id = graph_id.clone();
            let agg_error_handler = Arc::clone(&window_error_handler);
            let agg_layout = Arc::new(build_count_eval_layout(
                &aggregates,
                eval_schema.as_ref(),
                expression_columns.as_ref(),
            ));
            let agg_input_schema = Arc::clone(&eval_schema);
            let aggregator = move |_key: &Vec<u8>, values: &[(Vec<u8>, i64)]| -> Option<Vec<u8>> {
                if values.is_empty() {
                    return None;
                }
                match encode_aggregate_values_from_encoded(
                    agg_layout.as_ref(),
                    &aggregates,
                    agg_input_schema.as_ref(),
                    values,
                    &agg_graph_id,
                    "session window aggregate",
                ) {
                    Ok(Some(encoded)) => Some(encoded),
                    Ok(None) => None,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %agg_graph_id,
                            error = %err,
                            "failed to encode session window aggregate output"
                        );
                        report_operator_closure_error(
                            &agg_error_handler,
                            "failed to encode session window aggregate output",
                            err,
                        );
                        None
                    }
                }
            };

            let session_aggregate =
                dbsp::DbspSessionWindowAggregate::new_batch_with_state_namespace::<
                    Vec<u8>,
                    Vec<u8>,
                    Vec<u8>,
                    _,
                    _,
                >(
                    &upstream,
                    Some(state_namespace),
                    row_extractor,
                    aggregator,
                    *gap_ms,
                    allowed_lateness_ms,
                    watermark,
                    Some(window_error_handler),
                )
                .await
                .context("initialize DBSP session window aggregate")?;

            let mapped = self
                .map_window_aggregate_output(
                    &graph_id,
                    &session_aggregate.stream(),
                    task_events,
                    "window-aggregate-project",
                )
                .await?;
            return Ok(mapped);
        }

        let (window_size, window_slide) = match &node.window.policy {
            DbspWindowPolicy::Tumbling { size_ms } => (*size_ms, *size_ms),
            DbspWindowPolicy::Hopping { size_ms, slide_ms } => (*size_ms, *slide_ms),
            DbspWindowPolicy::Session { .. } => unreachable!("handled above"),
        };

        if simple_count_star {
            tracing::info!(
                graph_id = %graph_id,
                "using window count-star fast path"
            );
            let key_extractor = Arc::clone(&vectorized_window_extractor);
            let row_graph_id = graph_id.clone();
            let row_error_handler = Arc::clone(&window_error_handler);
            let row_extractor = move |delta_values: &[(Vec<u8>, i64)]| match key_extractor
                .extract_keyed_time_deltas(delta_values, direct_time_column)
            {
                Ok(extracted) => extracted,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %row_graph_id,
                        error = %err,
                        "failed to evaluate vectorized window count-star keys"
                    );
                    report_operator_closure_error(
                        &row_error_handler,
                        "failed to evaluate vectorized window count-star keys",
                        err,
                    );
                    Vec::new()
                }
            };
            let window_count_star_aggregate =
                dbsp::DbspWindowCountStarAggregate::new_batch_with_state_namespace::<
                    Vec<u8>,
                    Vec<u8>,
                    _,
                >(
                    &upstream,
                    Some(state_namespace),
                    row_extractor,
                    window_size,
                    window_slide,
                    allowed_lateness_ms,
                    watermark,
                    Some(window_error_handler),
                )
                .await
                .context("initialize DBSP window count-star aggregate")?;

            let mapped = self
                .map_window_count_star_aggregate_output(
                    &graph_id,
                    &window_count_star_aggregate.stream(),
                    task_events,
                    "window-aggregate-project",
                )
                .await?;
            return Ok(mapped);
        }

        if aggregates
            .iter()
            .all(|agg| agg.function() == &DbspAggregateFunction::Count)
        {
            let key_extractor = Arc::clone(&vectorized_window_extractor);
            let window_graph_id = graph_id.clone();
            let window_error_handler_for_keys = Arc::clone(&window_error_handler);
            let window_extractor = move |delta_values: &[(Vec<u8>, i64)]| match key_extractor
                .extract_keyed_time_deltas(delta_values, direct_time_column)
            {
                Ok(extracted) => extracted,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %window_graph_id,
                        error = %err,
                        "failed to evaluate vectorized window count aggregate keys"
                    );
                    report_operator_closure_error(
                        &window_error_handler_for_keys,
                        "failed to evaluate vectorized window count aggregate keys",
                        err,
                    );
                    Vec::new()
                }
            };

            let slot_kinds = build_count_aggregate_slot_kinds(&aggregates);
            let row_evaluator = build_window_count_batch_row_evaluator(
                Arc::clone(&eval_schema),
                aggregates.clone(),
                Arc::clone(&expression_columns),
                graph_id.clone(),
                "window aggregate",
            );
            let window_count_aggregate =
                dbsp::DbspWindowCountAggregate::new_batch_with_state_namespace::<
                    Vec<u8>,
                    Vec<u8>,
                    Vec<u8>,
                    _,
                    _,
                >(
                    &upstream,
                    Some(state_namespace),
                    window_extractor,
                    row_evaluator,
                    slot_kinds,
                    window_size,
                    window_slide,
                    allowed_lateness_ms,
                    watermark,
                    Some(Arc::clone(&window_error_handler)),
                )
                .await
                .context("initialize DBSP window count aggregate")?;

            let mapped = self
                .map_window_count_aggregate_output(
                    &graph_id,
                    &window_count_aggregate.stream(),
                    task_events,
                    "window-aggregate-project",
                )
                .await?;
            return Ok(mapped);
        }

        if let Some(slot_kinds) = build_incremental_aggregate_slot_kinds(&aggregates) {
            let key_extractor = Arc::clone(&vectorized_window_extractor);
            let window_graph_id = graph_id.clone();
            let window_error_handler_for_keys = Arc::clone(&window_error_handler);
            let window_extractor = move |delta_values: &[(Vec<u8>, i64)]| match key_extractor
                .extract_keyed_time_deltas(delta_values, direct_time_column)
            {
                Ok(extracted) => extracted,
                Err(err) => {
                    tracing::warn!(
                        graph_id = %window_graph_id,
                        error = %err,
                        "failed to evaluate vectorized window incremental aggregate keys"
                    );
                    report_operator_closure_error(
                        &window_error_handler_for_keys,
                        "failed to evaluate vectorized window incremental aggregate keys",
                        err,
                    );
                    Vec::new()
                }
            };

            let row_evaluator = build_window_incremental_aggregate_batch_row_evaluator(
                Arc::clone(&eval_schema),
                aggregates.clone(),
                Arc::clone(&expression_columns),
                graph_id.clone(),
                "window aggregate",
            );
            let window_incremental_aggregate =
                dbsp::DbspWindowIncrementalAggregate::new_batch_with_state_namespace_and_append_only_input::<
                    Vec<u8>,
                    Vec<u8>,
                    _,
                    _,
                >(
                    &upstream,
                    Some(state_namespace),
                    window_extractor,
                    row_evaluator,
                    slot_kinds,
                    window_size,
                    window_slide,
                    allowed_lateness_ms,
                    watermark,
                    append_only_input,
                    Some(Arc::clone(&window_error_handler)),
                )
                .await
                .context("initialize DBSP window incremental aggregate")?;

            let mapped = self
                .map_window_incremental_aggregate_output(
                    &graph_id,
                    &window_incremental_aggregate.stream(),
                    task_events,
                    "window-aggregate-project",
                )
                .await?;
            return Ok(mapped);
        }

        let key_extractor = Arc::clone(&vectorized_window_extractor);
        let key_graph_id = graph_id.clone();
        let key_error_handler = Arc::clone(&window_error_handler);
        let window_extractor = move |delta_values: &[(Vec<u8>, i64)]| match key_extractor
            .extract_keyed_time_deltas(delta_values, direct_time_column)
        {
            Ok(extracted) => extracted,
            Err(err) => {
                tracing::warn!(
                    graph_id = %key_graph_id,
                    error = %err,
                    "failed to evaluate vectorized window aggregate keys"
                );
                report_operator_closure_error(
                    &key_error_handler,
                    "failed to evaluate vectorized window aggregate keys",
                    err,
                );
                Vec::new()
            }
        };

        let agg_graph_id = graph_id.clone();
        let agg_error_handler = Arc::clone(&window_error_handler);
        let agg_layout = Arc::new(build_count_eval_layout(
            &aggregates,
            eval_schema.as_ref(),
            expression_columns.as_ref(),
        ));
        let agg_input_schema = Arc::clone(&eval_schema);
        let aggregator = move |_key: &Vec<u8>, values: &[(Vec<u8>, i64)]| -> Option<Vec<u8>> {
            if values.is_empty() {
                return None;
            }
            match encode_aggregate_values_from_encoded(
                agg_layout.as_ref(),
                &aggregates,
                agg_input_schema.as_ref(),
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
                    report_operator_closure_error(
                        &agg_error_handler,
                        "failed to encode window aggregate output",
                        err,
                    );
                    None
                }
            }
        };

        let window_aggregate = DbspWindowAggregate::new_with_state_namespace_and_batch_extractor::<
            Vec<u8>,
            Vec<u8>,
            Vec<u8>,
            _,
            _,
        >(
            &upstream,
            Some(state_namespace),
            window_extractor,
            aggregator,
            window_size,
            window_slide,
            allowed_lateness_ms,
            watermark,
            Some(window_error_handler),
        )
        .await
        .context("initialize DBSP window aggregate")?;

        self.map_window_aggregate_output(
            &graph_id,
            &window_aggregate.stream(),
            task_events,
            "window-aggregate-project",
        )
        .await
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

        let transform = move |delta_values: &[((Vec<u8>, Vec<u8>), i64)]|
              -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
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

    async fn map_count_aggregate_output(
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

        let transform = move |delta_values: &[((Vec<u8>, Vec<i64>), i64)]|
              -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
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

    async fn map_window_count_aggregate_output(
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

        let transform = move |delta_values: &[((WindowKey<Vec<u8>>, Vec<i64>), i64)]|
              -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
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

    async fn map_window_incremental_aggregate_output(
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

        let transform = move |delta_values: &[(
            (WindowKey<Vec<u8>>, Vec<dbsp::AggregateValue>),
            i64,
        )]|
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

    async fn map_window_aggregate_output(
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

        let transform = move |delta_values: &[((WindowKey<Vec<u8>>, Vec<u8>), i64)]|
              -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
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

    async fn map_window_count_star_aggregate_output(
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

        let transform = move |delta_values: &[((WindowKey<Vec<u8>>, i64), i64)]|
              -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
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

    async fn map_incremental_aggregate_output(
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

        let transform = move |delta_values: &[((Vec<u8>, Vec<dbsp::AggregateValue>), i64)]|
              -> anyhow::Result<Vec<(Vec<u8>, i64)>> {
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

fn is_simple_count_star_aggregate(aggregates: &[DbspAggregateExpr]) -> bool {
    aggregates.len() == 1 && aggregates.iter().all(is_unconditional_count_aggregate)
}

fn is_unconditional_count_aggregate(agg: &DbspAggregateExpr) -> bool {
    agg.function() == &DbspAggregateFunction::Count
        && !agg.distinct()
        && agg.filter().is_none()
        && agg.expression().is_none_or(|expr| match expr.expr() {
            Expr::Literal(value, _) => !value.is_null(),
            _ => false,
        })
}

pub(crate) fn build_window_count_batch_row_evaluator(
    input_schema: Arc<RowSchema>,
    aggregates: Vec<DbspAggregateExpr>,
    expression_columns: Arc<ExpressionColumnMap>,
    graph_id: String,
    context: &'static str,
) -> impl Fn(
    &[(dbsp::WindowCountInput<Vec<u8>, Vec<u8>>, i64)],
) -> Vec<(
    dbsp::CountAggregateRow<dbsp::WindowKey<Vec<u8>>, Vec<u8>>,
    i64,
)> + Send
+ Sync
+ 'static {
    let layout = Arc::new(build_count_eval_layout(
        &aggregates,
        input_schema.as_ref(),
        expression_columns.as_ref(),
    ));
    let vectorized_aggregates = aggregates.clone();
    move |delta_values: &[(dbsp::WindowCountInput<Vec<u8>, Vec<u8>>, i64)]| {
        let input_rows = delta_values
            .iter()
            .map(|(row, weight)| (row.value.clone(), *weight))
            .collect::<Vec<_>>();
        if let Some(slots_by_row) = evaluate_count_batch_row_values(
            layout.as_ref(),
            &vectorized_aggregates,
            input_schema.as_ref(),
            &input_rows,
            &graph_id,
            context,
        ) {
            return delta_values
                .iter()
                .zip(slots_by_row)
                .map(|((row, weight), slots)| {
                    (
                        dbsp::CountAggregateRow {
                            key: row.window_key.clone(),
                            slots,
                        },
                        *weight,
                    )
                })
                .collect();
        }
        Vec::new()
    }
}

pub(crate) fn build_count_batch_row_evaluator(
    input_schema: Arc<RowSchema>,
    group_keys: Vec<dbsp::circuit::plan::GroupKeyExpr>,
    aggregates: Vec<DbspAggregateExpr>,
    expression_columns: Arc<ExpressionColumnMap>,
    graph_id: String,
    context: &'static str,
) -> impl Fn(&[(Vec<u8>, i64)]) -> Vec<(dbsp::CountAggregateRow<Vec<u8>, Vec<u8>>, i64)>
+ Send
+ Sync
+ 'static {
    let layout = Arc::new(build_count_eval_layout(
        &aggregates,
        input_schema.as_ref(),
        expression_columns.as_ref(),
    ));
    let vectorized_key_extractor = direct_group_key_columns(
        &group_keys,
        input_schema.as_ref(),
        expression_columns.as_ref(),
    )
    .and_then(|columns| {
        VectorizedEncodedKeyExtractor::new(input_schema.to_arrow_schema(), Arc::new(columns)).ok()
    })
    .map(Arc::new);
    let vectorized_input_schema = Arc::clone(&input_schema);
    let vectorized_aggregates = aggregates.clone();
    let vectorized_graph_id = graph_id.clone();
    move |delta_values: &[(Vec<u8>, i64)]| {
        if let Some(key_extractor) = vectorized_key_extractor.as_ref() {
            match key_extractor.extract_keyed_deltas(delta_values) {
                Ok(keyed) => {
                    let input_rows = keyed
                        .iter()
                        .map(|(_, bytes, weight)| (bytes.clone(), *weight))
                        .collect::<Vec<_>>();
                    if let Some(slots_by_row) = evaluate_count_batch_row_values(
                        layout.as_ref(),
                        &vectorized_aggregates,
                        vectorized_input_schema.as_ref(),
                        &input_rows,
                        &vectorized_graph_id,
                        context,
                    ) {
                        return keyed
                            .into_iter()
                            .zip(slots_by_row)
                            .map(|((encoded_key, _bytes, weight), slots)| {
                                (
                                    dbsp::CountAggregateRow {
                                        key: encoded_key,
                                        slots,
                                    },
                                    weight,
                                )
                            })
                            .collect();
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        graph_id = %vectorized_graph_id,
                        error = %err,
                        "failed to evaluate vectorized count aggregate group keys"
                    );
                }
            }
        }
        Vec::new()
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

fn evaluate_count_batch_row_values(
    layout: &CountEvalLayout,
    aggregates: &[DbspAggregateExpr],
    input_schema: &RowSchema,
    rows: &[(Vec<u8>, i64)],
    graph_id: &str,
    context: &str,
) -> Option<Vec<Vec<dbsp::CountAggregateSlotUpdate<Vec<u8>>>>> {
    if rows.is_empty() {
        return Some(Vec::new());
    }
    let batch = match count_eval_record_batch(layout, input_schema, rows.iter().cloned()) {
        Ok(Some(batch)) => batch,
        Ok(None) => return Some(Vec::new()),
        Err(err) => {
            tracing::warn!(
                graph_id = %graph_id,
                error = %err,
                "failed to build vectorized {context} count aggregate input batch"
            );
            return None;
        }
    };
    if batch.num_rows() != rows.len() {
        tracing::warn!(
            graph_id = %graph_id,
            expected_rows = rows.len(),
            actual_rows = batch.num_rows(),
            "vectorized {context} count aggregate input batch row count mismatch"
        );
        return None;
    }
    let mut evaluated = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        evaluated.push(evaluate_count_arrow_row_values(
            layout, aggregates, &batch, row_idx, graph_id, context,
        ));
    }
    Some(evaluated)
}

fn evaluate_count_arrow_row_values(
    layout: &CountEvalLayout,
    aggregates: &[DbspAggregateExpr],
    batch: &RecordBatch,
    row_idx: usize,
    graph_id: &str,
    context: &str,
) -> Vec<dbsp::CountAggregateSlotUpdate<Vec<u8>>> {
    let mut filter_results = vec![false; layout.filters.len()];
    for (index, filter) in layout.filters.iter().enumerate() {
        if let Some(column_idx) = layout.filter_direct_columns[index] {
            let Some(decoded_idx) = layout.required_input_positions.get(&column_idx).copied()
            else {
                continue;
            };
            filter_results[index] =
                match bool_from_arrow_array(batch.column(decoded_idx).as_ref(), row_idx) {
                    Ok(include) => include,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %graph_id,
                            error = %err,
                            "failed to evaluate vectorized {context} direct FILTER column"
                        );
                        false
                    }
                };
        } else {
            tracing::warn!(
                graph_id = %graph_id,
                expression = ?filter.expr(),
                "unresolved vectorized {context} FILTER expression without precompute column"
            );
        }
    }

    let mut expression_values = vec![None; layout.expressions.len()];
    let mut expression_valid = vec![false; layout.expressions.len()];
    for (index, expr) in layout.expressions.iter().enumerate() {
        if let Some(column_idx) = layout.expression_direct_columns[index] {
            let Some(decoded_idx) = layout.required_input_positions.get(&column_idx).copied()
            else {
                continue;
            };
            match encoded_scalar_from_arrow_array(batch.column(decoded_idx).as_ref(), row_idx) {
                Ok(value) => {
                    expression_values[index] = value;
                    expression_valid[index] = true;
                }
                Err(err) => {
                    tracing::warn!(
                        graph_id = %graph_id,
                        error = %err,
                        "failed to read vectorized {context} aggregate expression column"
                    );
                }
            }
        } else {
            tracing::warn!(
                graph_id = %graph_id,
                expression = ?expr.expr(),
                "unresolved vectorized {context} aggregate expression without precompute column"
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
                            let encoded = expression_values[expr_index]
                                .as_ref()
                                .and_then(|value| match encode_single_encoded_scalar_key(value) {
                                    Ok(encoded) => Some(encoded),
                                    Err(err) => {
                                        tracing::warn!(
                                            graph_id = %graph_id,
                                            error = %err,
                                            "failed to encode vectorized count aggregate DISTINCT value"
                                        );
                                        None
                                    }
                                });
                            dbsp::CountAggregateSlotUpdate::Distinct(encoded)
                        } else {
                            dbsp::CountAggregateSlotUpdate::Linear(1)
                        }
                    } else if agg.distinct() {
                        dbsp::CountAggregateSlotUpdate::Distinct(None)
                    } else {
                        dbsp::CountAggregateSlotUpdate::Linear(0)
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
    input_schema: &RowSchema,
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

    let batch = match count_eval_record_batch(
        layout,
        input_schema,
        values
            .iter()
            .map(|(value, weight)| (value.clone(), *weight)),
    ) {
        Ok(Some(batch)) => batch,
        Ok(None) => return Ok(None),
        Err(err) => {
            tracing::warn!(
                graph_id = %graph_id,
                error = %err,
                "failed to build vectorized {context} aggregate input batch"
            );
            return Ok(None);
        }
    };
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let weight_array = batch
        .column(batch.num_columns().saturating_sub(1))
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("expected vectorized aggregate weight column"))?;

    let mut filter_results = vec![false; layout.filters.len()];
    let mut expression_values = vec![None; layout.expressions.len()];
    let mut expression_valid = vec![false; layout.expressions.len()];

    for row_idx in 0..batch.num_rows() {
        let weight = weight_array.value(row_idx);
        if weight == 0 {
            continue;
        }

        for (index, filter) in layout.filters.iter().enumerate() {
            if let Some(column_idx) = layout.filter_direct_columns[index] {
                let Some(decoded_idx) = layout.required_input_positions.get(&column_idx).copied()
                else {
                    filter_results[index] = false;
                    continue;
                };
                filter_results[index] =
                    match bool_from_arrow_array(batch.column(decoded_idx).as_ref(), row_idx) {
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
                let Some(decoded_idx) = layout.required_input_positions.get(&column_idx).copied()
                else {
                    continue;
                };
                match encoded_scalar_from_arrow_array(batch.column(decoded_idx).as_ref(), row_idx) {
                    Ok(value) => {
                        expression_values[index] = value;
                        expression_valid[index] = true;
                    }
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %graph_id,
                            error = %err,
                            "failed to read vectorized {context} aggregate expression column"
                        );
                    }
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
                    *entry += weight;
                    if *entry == 0 {
                        weights.remove(&expr_value);
                    }
                }
                EncodedAggregateAccumulator::Count { count } => match plan.expr_index {
                    Some(expr_index) => {
                        if expression_valid[expr_index] && expression_values[expr_index].is_some() {
                            *count += weight;
                        }
                    }
                    None => *count += weight,
                },
                EncodedAggregateAccumulator::Sum { sum, has_value } => {
                    let Some(expr_index) = plan.expr_index else {
                        continue;
                    };
                    if !expression_valid[expr_index] {
                        continue;
                    }
                    if let Some(number) =
                        sum_numeric_from_encoded_scalar(expression_values[expr_index].as_ref())
                    {
                        *sum = checked_sum_add(*sum, checked_weighted_sum_delta(number, weight)?)?;
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
                        *sum += number * weight;
                        *count += weight;
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
                aggregate_numeric_value_type_from_dbsp_type(agg.output_type())?,
            ),
            DbspAggregateFunction::Avg => dbsp::IncrementalAggregateSlotKind::Avg,
            DbspAggregateFunction::Min => dbsp::IncrementalAggregateSlotKind::Min(
                aggregate_ordered_value_type_from_dbsp_type(agg.output_type())?,
            ),
            DbspAggregateFunction::Max => dbsp::IncrementalAggregateSlotKind::Max(
                aggregate_ordered_value_type_from_dbsp_type(agg.output_type())?,
            ),
        };
        slot_kinds.push(kind);
    }
    Some(slot_kinds)
}

pub(crate) fn build_incremental_aggregate_batch_row_evaluator(
    input_schema: Arc<RowSchema>,
    group_keys: Vec<dbsp::circuit::plan::GroupKeyExpr>,
    aggregates: Vec<DbspAggregateExpr>,
    expression_columns: Arc<ExpressionColumnMap>,
    graph_id: String,
    context: &'static str,
) -> impl Fn(&[(Vec<u8>, i64)]) -> Vec<(Vec<u8>, dbsp::IncrementalAggregateRow<Vec<u8>>, i64)>
+ Send
+ Sync
+ 'static {
    let layout = Arc::new(build_count_eval_layout(
        &aggregates,
        input_schema.as_ref(),
        expression_columns.as_ref(),
    ));
    let vectorized_key_extractor = direct_group_key_columns(
        &group_keys,
        input_schema.as_ref(),
        expression_columns.as_ref(),
    )
    .and_then(|columns| {
        VectorizedEncodedKeyExtractor::new(input_schema.to_arrow_schema(), Arc::new(columns)).ok()
    })
    .map(Arc::new);
    let vectorized_input_schema = Arc::clone(&input_schema);
    let vectorized_aggregates = aggregates.clone();
    let vectorized_graph_id = graph_id.clone();
    move |delta_values: &[(Vec<u8>, i64)]| {
        if let Some(key_extractor) = vectorized_key_extractor.as_ref() {
            match key_extractor.extract_keyed_deltas(delta_values) {
                Ok(keyed) => {
                    let input_rows = keyed
                        .iter()
                        .map(|(_, bytes, weight)| (bytes.clone(), *weight))
                        .collect::<Vec<_>>();
                    if let Some(slots_by_row) = evaluate_incremental_aggregate_batch_row_values(
                        layout.as_ref(),
                        &vectorized_aggregates,
                        vectorized_input_schema.as_ref(),
                        &input_rows,
                        &vectorized_graph_id,
                        context,
                    ) {
                        return keyed
                            .into_iter()
                            .zip(slots_by_row)
                            .map(|((encoded_key, bytes, weight), slots)| {
                                let row = dbsp::IncrementalAggregateRow {
                                    key: encoded_key,
                                    slots,
                                };
                                (bytes, row, weight)
                            })
                            .collect();
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        graph_id = %vectorized_graph_id,
                        error = %err,
                        "failed to evaluate vectorized incremental aggregate group keys"
                    );
                }
            }
        }
        Vec::new()
    }
}

pub(crate) fn build_window_incremental_aggregate_batch_row_evaluator(
    input_schema: Arc<RowSchema>,
    aggregates: Vec<DbspAggregateExpr>,
    expression_columns: Arc<ExpressionColumnMap>,
    graph_id: String,
    context: &'static str,
) -> impl Fn(
    &[(dbsp::WindowIncrementalInput<Vec<u8>, Vec<u8>>, i64)],
) -> Vec<(
    dbsp::WindowIncrementalInput<Vec<u8>, Vec<u8>>,
    dbsp::IncrementalAggregateRow<dbsp::WindowKey<Vec<u8>>>,
    i64,
)> + Send
+ Sync
+ 'static {
    let layout = Arc::new(build_count_eval_layout(
        &aggregates,
        input_schema.as_ref(),
        expression_columns.as_ref(),
    ));
    move |delta_values: &[(dbsp::WindowIncrementalInput<Vec<u8>, Vec<u8>>, i64)]| {
        let input_rows = delta_values
            .iter()
            .map(|(row, weight)| (row.value.clone(), *weight))
            .collect::<Vec<_>>();
        if let Some(slots_by_row) = evaluate_incremental_aggregate_batch_row_values(
            layout.as_ref(),
            &aggregates,
            input_schema.as_ref(),
            &input_rows,
            &graph_id,
            context,
        ) {
            return delta_values
                .iter()
                .zip(slots_by_row)
                .map(|((row, weight), slots)| {
                    let aggregate_row = dbsp::IncrementalAggregateRow {
                        key: row.window_key.clone(),
                        slots,
                    };
                    (row.clone(), aggregate_row, *weight)
                })
                .collect();
        }
        Vec::new()
    }
}

#[derive(Clone)]
pub(crate) struct PrekeyedIncrementalAggregateBatchEvaluator {
    layout: Arc<CountEvalLayout>,
    input_schema: Arc<RowSchema>,
    aggregates: Vec<DbspAggregateExpr>,
    graph_id: String,
    context: &'static str,
}

pub(crate) fn build_prekeyed_incremental_aggregate_batch_evaluator(
    input_schema: Arc<RowSchema>,
    aggregates: Vec<DbspAggregateExpr>,
    expression_columns: Arc<ExpressionColumnMap>,
    graph_id: String,
    context: &'static str,
) -> PrekeyedIncrementalAggregateBatchEvaluator {
    PrekeyedIncrementalAggregateBatchEvaluator {
        layout: Arc::new(build_count_eval_layout(
            &aggregates,
            input_schema.as_ref(),
            expression_columns.as_ref(),
        )),
        input_schema,
        aggregates,
        graph_id,
        context,
    }
}

impl PrekeyedIncrementalAggregateBatchEvaluator {
    pub(crate) fn required_input_columns(&self) -> &[usize] {
        &self.layout.required_input_columns
    }

    pub(crate) fn evaluate_batch_row(
        &self,
        batch: &RecordBatch,
        input_positions: &HashMap<usize, usize>,
        row_idx: usize,
    ) -> Vec<dbsp::IncrementalAggregateSlotUpdate> {
        evaluate_incremental_aggregate_arrow_row_values(
            self.layout.as_ref(),
            &self.aggregates,
            batch,
            input_positions,
            row_idx,
            &self.graph_id,
            self.context,
        )
    }

    pub(crate) fn evaluate_deltas(
        &self,
        delta_values: &[((Vec<u8>, Vec<u8>), i64)],
    ) -> Vec<(
        (Vec<u8>, Vec<u8>),
        dbsp::IncrementalAggregateRow<Vec<u8>>,
        i64,
    )> {
        let input_rows = delta_values
            .iter()
            .map(|(pair, weight)| (pair.1.clone(), *weight))
            .collect::<Vec<_>>();
        if let Some(slots_by_row) = evaluate_incremental_aggregate_batch_row_values(
            self.layout.as_ref(),
            &self.aggregates,
            self.input_schema.as_ref(),
            &input_rows,
            &self.graph_id,
            self.context,
        ) {
            return delta_values
                .iter()
                .zip(slots_by_row)
                .map(|((pair, weight), slots)| {
                    let row = dbsp::IncrementalAggregateRow {
                        key: pair.0.clone(),
                        slots,
                    };
                    (pair.clone(), row, *weight)
                })
                .collect();
        }
        Vec::new()
    }
}

fn evaluate_incremental_aggregate_batch_row_values(
    layout: &CountEvalLayout,
    aggregates: &[DbspAggregateExpr],
    input_schema: &RowSchema,
    rows: &[(Vec<u8>, i64)],
    graph_id: &str,
    context: &str,
) -> Option<Vec<Vec<dbsp::IncrementalAggregateSlotUpdate>>> {
    if rows.is_empty() {
        return Some(Vec::new());
    }
    let batch = match count_eval_record_batch(layout, input_schema, rows.iter().cloned()) {
        Ok(Some(batch)) => batch,
        Ok(None) => return Some(Vec::new()),
        Err(err) => {
            tracing::warn!(
                graph_id = %graph_id,
                error = %err,
                "failed to build vectorized {context} incremental aggregate input batch"
            );
            return None;
        }
    };
    if batch.num_rows() != rows.len() {
        tracing::warn!(
            graph_id = %graph_id,
            expected_rows = rows.len(),
            actual_rows = batch.num_rows(),
            "vectorized {context} incremental aggregate input batch row count mismatch"
        );
        return None;
    }
    let mut evaluated = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        evaluated.push(evaluate_incremental_aggregate_arrow_row_values(
            layout,
            aggregates,
            &batch,
            &layout.required_input_positions,
            row_idx,
            graph_id,
            context,
        ));
    }
    Some(evaluated)
}

fn evaluate_incremental_aggregate_arrow_row_values(
    layout: &CountEvalLayout,
    aggregates: &[DbspAggregateExpr],
    batch: &RecordBatch,
    input_positions: &HashMap<usize, usize>,
    row_idx: usize,
    graph_id: &str,
    context: &str,
) -> Vec<dbsp::IncrementalAggregateSlotUpdate> {
    let mut filter_results = vec![false; layout.filters.len()];
    for (index, filter) in layout.filters.iter().enumerate() {
        if let Some(column_idx) = layout.filter_direct_columns[index] {
            let Some(decoded_idx) = input_positions.get(&column_idx).copied() else {
                continue;
            };
            filter_results[index] =
                match bool_from_arrow_array(batch.column(decoded_idx).as_ref(), row_idx) {
                    Ok(include) => include,
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %graph_id,
                            error = %err,
                            "failed to evaluate vectorized {context} direct FILTER column"
                        );
                        false
                    }
                };
        } else {
            tracing::warn!(
                graph_id = %graph_id,
                expression = ?filter.expr(),
                "unresolved vectorized {context} FILTER expression without precompute column"
            );
        }
    }

    let mut expression_values = vec![None; layout.expressions.len()];
    let mut expression_valid = vec![false; layout.expressions.len()];
    for (index, expr) in layout.expressions.iter().enumerate() {
        if let Some(column_idx) = layout.expression_direct_columns[index] {
            let Some(decoded_idx) = input_positions.get(&column_idx).copied() else {
                continue;
            };
            match encoded_scalar_from_arrow_array(batch.column(decoded_idx).as_ref(), row_idx) {
                Ok(value) => {
                    expression_values[index] = value;
                    expression_valid[index] = true;
                }
                Err(err) => {
                    tracing::warn!(
                        graph_id = %graph_id,
                        error = %err,
                        "failed to read vectorized {context} incremental aggregate expression column"
                    );
                }
            }
        } else {
            tracing::warn!(
                graph_id = %graph_id,
                expression = ?expr.expr(),
                "unresolved vectorized {context} aggregate expression without precompute column"
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

#[cfg(test)]
fn bool_from_encoded_scalar(value: Option<&EncodedRowScalar>) -> Result<bool> {
    match value {
        Some(EncodedRowScalar::Bool(flag)) => Ok(*flag),
        None => Ok(false),
        Some(other) => Err(anyhow!("expected boolean value, found {other:?}")),
    }
}

fn bool_from_arrow_array(array: &dyn Array, row_idx: usize) -> Result<bool> {
    if array.is_null(row_idx) {
        return Ok(false);
    }
    let values = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| anyhow!("expected Boolean aggregate filter column"))?;
    Ok(values.value(row_idx))
}

fn encoded_scalar_from_arrow_array(
    array: &dyn Array,
    row_idx: usize,
) -> Result<Option<EncodedRowScalar>> {
    if array.is_null(row_idx) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("expected Int64 aggregate column"))?;
            Ok(Some(EncodedRowScalar::Int64(values.value(row_idx))))
        }
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("expected Utf8 aggregate column"))?;
            Ok(Some(EncodedRowScalar::Utf8(
                values.value(row_idx).to_string(),
            )))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| anyhow!("expected TimestampMillisecond aggregate column"))?;
            Ok(Some(EncodedRowScalar::TimestampMillis(
                values.value(row_idx),
            )))
        }
        DataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("expected Boolean aggregate column"))?;
            Ok(Some(EncodedRowScalar::Bool(values.value(row_idx))))
        }
        DataType::Date32 => {
            let values = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| anyhow!("expected Date32 aggregate column"))?;
            Ok(Some(EncodedRowScalar::DateDays(values.value(row_idx))))
        }
        DataType::Decimal128(_, _) => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| anyhow!("expected Decimal128 aggregate column"))?;
            Ok(Some(EncodedRowScalar::Decimal128(values.value(row_idx))))
        }
        other => Err(anyhow!(
            "unsupported vectorized aggregate column type {other:?}"
        )),
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

fn sum_numeric_from_encoded_scalar(value: Option<&EncodedRowScalar>) -> Option<i128> {
    match value {
        Some(EncodedRowScalar::Int64(value) | EncodedRowScalar::TimestampMillis(value)) => {
            Some(i128::from(*value))
        }
        Some(EncodedRowScalar::Decimal128(value)) => Some(*value),
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
        (EncodedRowScalar::DateDays(l), EncodedRowScalar::DateDays(r)) => Some(l.cmp(r)),
        (EncodedRowScalar::Decimal128(l), EncodedRowScalar::Decimal128(r)) => Some(l.cmp(r)),
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
        EncodedRowScalar::DateDays(value) => {
            encoded.push(0x09);
            encoded.extend_from_slice(&value.to_le_bytes());
        }
        EncodedRowScalar::Decimal128(value) => {
            encoded.push(0x0B);
            encoded.extend_from_slice(&value.to_le_bytes());
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
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::DateDays) => encoded.push(0x0A),
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::Decimal128 { .. }) => {
                encoded.push(0x0C);
            }
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
            dbsp::AggregateValue::DateDays(value) => {
                encoded.push(0x09);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            dbsp::AggregateValue::Decimal128(value) => {
                encoded.push(0x0B);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
        }
    }
    Ok(encoded)
}

fn append_encoded_sum_like_value(
    value: i128,
    output_type: &DbspScalarType,
    encoded: &mut Vec<u8>,
) -> Result<()> {
    match output_type {
        DbspScalarType::Int64 => append_encoded_i64(
            i64::try_from(value).context("aggregate Int64 SUM overflow")?,
            encoded,
        ),
        DbspScalarType::TimestampMillis => append_encoded_timestamp(
            i64::try_from(value).context("aggregate TimestampMillis SUM overflow")?,
            encoded,
        ),
        DbspScalarType::Decimal128 { precision, .. } => {
            ensure_decimal_sum_fits_precision(value, *precision)?;
            encoded.push(0x0B);
            encoded.extend_from_slice(&value.to_le_bytes());
        }
        DbspScalarType::Utf8 | DbspScalarType::Bool | DbspScalarType::DateDays => {
            return Err(anyhow!(
                "unsupported aggregate SUM output type for encoded output: {output_type:?}"
            ));
        }
    }
    Ok(())
}

fn checked_weighted_sum_delta(value: i128, weight: i64) -> Result<i128> {
    value
        .checked_mul(i128::from(weight))
        .ok_or_else(|| anyhow!("aggregate SUM overflow while applying input weight"))
}

fn checked_sum_add(left: i128, right: i128) -> Result<i128> {
    left.checked_add(right)
        .ok_or_else(|| anyhow!("aggregate SUM overflow"))
}

fn ensure_decimal_sum_fits_precision(value: i128, precision: u8) -> Result<()> {
    let max_abs = 10_i128
        .checked_pow(u32::from(precision))
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| anyhow!("invalid Decimal128 precision {precision}"))?;
    let abs = value
        .checked_abs()
        .ok_or_else(|| anyhow!("Decimal128 SUM overflow"))?;
    ensure!(
        abs <= max_abs,
        "Decimal128 SUM overflow: value {value} exceeds precision {precision}"
    );
    Ok(())
}

fn aggregate_numeric_value_type_from_dbsp_type(
    value_type: &DbspScalarType,
) -> Option<dbsp::AggregateValueType> {
    match value_type {
        DbspScalarType::Int64 => Some(dbsp::AggregateValueType::Int64),
        DbspScalarType::TimestampMillis => Some(dbsp::AggregateValueType::TimestampMillis),
        DbspScalarType::Decimal128 { precision, scale } => {
            Some(dbsp::AggregateValueType::Decimal128 {
                precision: *precision,
                scale: *scale,
            })
        }
        DbspScalarType::Utf8 | DbspScalarType::Bool | DbspScalarType::DateDays => None,
    }
}

fn aggregate_ordered_value_type_from_dbsp_type(
    value_type: &DbspScalarType,
) -> Option<dbsp::AggregateValueType> {
    match value_type {
        DbspScalarType::Int64 => Some(dbsp::AggregateValueType::Int64),
        DbspScalarType::TimestampMillis => Some(dbsp::AggregateValueType::TimestampMillis),
        DbspScalarType::Utf8 => Some(dbsp::AggregateValueType::Utf8),
        DbspScalarType::DateDays => Some(dbsp::AggregateValueType::DateDays),
        DbspScalarType::Decimal128 { precision, scale } => {
            Some(dbsp::AggregateValueType::Decimal128 {
                precision: *precision,
                scale: *scale,
            })
        }
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
        Some(EncodedRowScalar::DateDays(value)) => Some(dbsp::AggregateValue::DateDays(*value)),
        Some(EncodedRowScalar::Decimal128(value)) => Some(dbsp::AggregateValue::Decimal128(*value)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::col;
    use dbsp::circuit::schema::Field;

    fn schema(fields: Vec<(&str, DbspScalarType)>) -> Arc<RowSchema> {
        let fields = fields
            .into_iter()
            .map(|(name, ty)| Field::new(name, ty, true))
            .collect();
        RowSchema::try_new(fields).expect("schema")
    }

    fn encode_test_row(columns: &[Option<EncodedRowScalar>]) -> Vec<u8> {
        let mut encoded = Vec::new();
        let count = u32::try_from(columns.len()).expect("column count fits u32");
        encoded.extend_from_slice(&count.to_le_bytes());
        for value in columns {
            match value {
                None => encoded.push(0x00),
                Some(EncodedRowScalar::Int64(value)) => {
                    encoded.push(0x01);
                    encoded.extend_from_slice(&value.to_le_bytes());
                }
                Some(EncodedRowScalar::Utf8(value)) => {
                    encoded.push(0x02);
                    let bytes = value.as_bytes();
                    let len = u32::try_from(bytes.len()).expect("utf8 length fits u32");
                    encoded.extend_from_slice(&len.to_le_bytes());
                    encoded.extend_from_slice(bytes);
                }
                Some(EncodedRowScalar::TimestampMillis(value)) => {
                    encoded.push(0x03);
                    encoded.extend_from_slice(&value.to_le_bytes());
                }
                Some(EncodedRowScalar::Bool(value)) => {
                    encoded.push(0x04);
                    encoded.push(if *value { 1 } else { 0 });
                }
                Some(EncodedRowScalar::DateDays(value)) => {
                    encoded.push(0x09);
                    encoded.extend_from_slice(&value.to_le_bytes());
                }
                Some(EncodedRowScalar::Decimal128(value)) => {
                    encoded.push(0x0B);
                    encoded.extend_from_slice(&value.to_le_bytes());
                }
            }
        }
        encoded
    }

    #[test]
    fn count_slot_kinds_and_count_star_detection() {
        let input_schema = schema(vec![
            ("price", DbspScalarType::Int64),
            ("bidder", DbspScalarType::Int64),
        ]);
        let aggregate = DbspAggregateNode::try_new(
            Arc::clone(&input_schema),
            vec![],
            vec![
                (
                    DbspAggregateFunction::Count,
                    None,
                    None,
                    false,
                    Some("count_star".to_string()),
                ),
                (
                    DbspAggregateFunction::Count,
                    Some(col("bidder")),
                    None,
                    true,
                    Some("count_distinct_bidder".to_string()),
                ),
            ],
        )
        .expect("aggregate node");

        let slot_kinds = build_count_aggregate_slot_kinds(aggregate.aggregates());
        assert!(matches!(
            slot_kinds[0],
            dbsp::CountAggregateSlotKind::Linear
        ));
        assert!(matches!(
            slot_kinds[1],
            dbsp::CountAggregateSlotKind::Distinct
        ));

        assert!(is_simple_count_star_aggregate(&[
            aggregate.aggregates()[0].clone()
        ]));
        assert!(!is_simple_count_star_aggregate(aggregate.aggregates()));
        assert!(is_unconditional_count_aggregate(&aggregate.aggregates()[0]));
        assert!(!is_unconditional_count_aggregate(
            &aggregate.aggregates()[1]
        ));
    }

    #[test]
    fn count_and_incremental_evaluators_decode_rows() {
        let input_schema = schema(vec![
            ("price", DbspScalarType::Int64),
            ("bidder", DbspScalarType::Int64),
            ("label", DbspScalarType::Utf8),
        ]);
        let aggregate = DbspAggregateNode::try_new(
            Arc::clone(&input_schema),
            vec![(col("bidder"), None)],
            vec![
                (
                    DbspAggregateFunction::Count,
                    None,
                    None,
                    false,
                    Some("total".to_string()),
                ),
                (
                    DbspAggregateFunction::Count,
                    Some(col("bidder")),
                    None,
                    false,
                    Some("nonnull_bidder".to_string()),
                ),
                (
                    DbspAggregateFunction::Count,
                    Some(col("bidder")),
                    None,
                    true,
                    Some("cheap_distinct_bidder".to_string()),
                ),
                (
                    DbspAggregateFunction::Sum,
                    Some(col("price")),
                    None,
                    false,
                    Some("sum_price".to_string()),
                ),
                (
                    DbspAggregateFunction::Max,
                    Some(col("label")),
                    None,
                    false,
                    Some("max_label".to_string()),
                ),
            ],
        )
        .expect("aggregate node");

        let expression_columns = Arc::new(HashMap::new());
        let count_eval = build_count_batch_row_evaluator(
            Arc::clone(&input_schema),
            aggregate.group_keys().to_vec(),
            aggregate.aggregates().to_vec(),
            Arc::clone(&expression_columns),
            "test".to_string(),
            "aggregate",
        );
        let incr_eval = build_incremental_aggregate_batch_row_evaluator(
            Arc::clone(&input_schema),
            aggregate.group_keys().to_vec(),
            aggregate.aggregates().to_vec(),
            expression_columns,
            "test".to_string(),
            "aggregate",
        );

        let row = encode_test_row(&[
            Some(EncodedRowScalar::Int64(50)),
            Some(EncodedRowScalar::Int64(42)),
            Some(EncodedRowScalar::Utf8("alpha".to_string())),
        ]);
        let count_rows = count_eval(&[(row.clone(), 1)]);
        let count_row = &count_rows.first().expect("count row").0;
        assert_eq!(
            extract_encoded_row_scalars(&count_row.key, &[0]).expect("decode key"),
            vec![Some(EncodedRowScalar::Int64(42))]
        );
        assert!(matches!(
            &count_row.slots[0],
            dbsp::CountAggregateSlotUpdate::Linear(1)
        ));
        assert!(matches!(
            &count_row.slots[1],
            dbsp::CountAggregateSlotUpdate::Linear(1)
        ));
        match &count_row.slots[2] {
            dbsp::CountAggregateSlotUpdate::Distinct(Some(encoded)) => {
                assert_eq!(
                    extract_encoded_row_scalars(encoded, &[0]).expect("decode distinct"),
                    vec![Some(EncodedRowScalar::Int64(42))]
                );
            }
            other => panic!("expected distinct encoded value, found {other:?}"),
        }

        let incr_rows = incr_eval(&[(row.clone(), 1)]);
        let incr_row = &incr_rows.first().expect("incremental row").1;
        assert!(matches!(
            &incr_row.slots[0],
            dbsp::IncrementalAggregateSlotUpdate::Count(1)
        ));
        assert!(matches!(
            &incr_row.slots[3],
            dbsp::IncrementalAggregateSlotUpdate::Value(Some(dbsp::AggregateValue::Int64(50)))
        ));
        assert!(matches!(
            &incr_row.slots[4],
            dbsp::IncrementalAggregateSlotUpdate::Value(Some(dbsp::AggregateValue::Utf8(value)))
                if value == "alpha"
        ));

        let filtered_row = encode_test_row(&[
            Some(EncodedRowScalar::Int64(200)),
            Some(EncodedRowScalar::Int64(7)),
            Some(EncodedRowScalar::Utf8("beta".to_string())),
        ]);
        let filtered_rows = count_eval(&[(filtered_row, 1)]);
        let count_row = &filtered_rows.first().expect("count row").0;
        match &count_row.slots[2] {
            dbsp::CountAggregateSlotUpdate::Distinct(Some(encoded)) => {
                assert_eq!(
                    extract_encoded_row_scalars(encoded, &[0]).expect("decode distinct"),
                    vec![Some(EncodedRowScalar::Int64(7))]
                );
            }
            other => panic!("expected distinct encoded value, found {other:?}"),
        }
    }

    #[test]
    fn incremental_slot_kinds_and_encoding_helpers_work() {
        let input_schema = schema(vec![
            ("price", DbspScalarType::Int64),
            ("label", DbspScalarType::Utf8),
        ]);
        let aggregate = DbspAggregateNode::try_new(
            Arc::clone(&input_schema),
            vec![],
            vec![
                (
                    DbspAggregateFunction::Count,
                    None,
                    None,
                    false,
                    Some("count".to_string()),
                ),
                (
                    DbspAggregateFunction::Count,
                    Some(col("label")),
                    None,
                    true,
                    Some("distinct_label".to_string()),
                ),
                (
                    DbspAggregateFunction::Sum,
                    Some(col("price")),
                    None,
                    false,
                    Some("sum_price".to_string()),
                ),
                (
                    DbspAggregateFunction::Avg,
                    Some(col("price")),
                    None,
                    false,
                    Some("avg_price".to_string()),
                ),
                (
                    DbspAggregateFunction::Max,
                    Some(col("label")),
                    None,
                    false,
                    Some("max_label".to_string()),
                ),
            ],
        )
        .expect("aggregate node");

        let slot_kinds = build_incremental_aggregate_slot_kinds(aggregate.aggregates())
            .expect("incremental slot kinds");
        assert!(matches!(
            slot_kinds[0],
            dbsp::IncrementalAggregateSlotKind::Count
        ));
        assert!(matches!(
            slot_kinds[1],
            dbsp::IncrementalAggregateSlotKind::CountDistinct
        ));
        assert!(matches!(
            slot_kinds[2],
            dbsp::IncrementalAggregateSlotKind::Sum(dbsp::AggregateValueType::Int64)
        ));
        assert!(matches!(
            slot_kinds[3],
            dbsp::IncrementalAggregateSlotKind::Avg
        ));
        assert!(matches!(
            slot_kinds[4],
            dbsp::IncrementalAggregateSlotKind::Max(dbsp::AggregateValueType::Utf8)
        ));

        assert_eq!(
            aggregate_ordered_value_type_from_dbsp_type(&DbspScalarType::Bool),
            None
        );

        let typed_input_schema = schema(vec![
            ("shipdate", DbspScalarType::DateDays),
            (
                "amount",
                DbspScalarType::Decimal128 {
                    precision: 18,
                    scale: 2,
                },
            ),
        ]);
        let typed_aggregate = DbspAggregateNode::try_new(
            Arc::clone(&typed_input_schema),
            vec![],
            vec![
                (
                    DbspAggregateFunction::Min,
                    Some(col("shipdate")),
                    None,
                    false,
                    Some("min_shipdate".to_string()),
                ),
                (
                    DbspAggregateFunction::Max,
                    Some(col("amount")),
                    None,
                    false,
                    Some("max_amount".to_string()),
                ),
            ],
        )
        .expect("typed aggregate node");
        let typed_slot_kinds = build_incremental_aggregate_slot_kinds(typed_aggregate.aggregates())
            .expect("typed incremental slot kinds");
        assert!(matches!(
            typed_slot_kinds[0],
            dbsp::IncrementalAggregateSlotKind::Min(dbsp::AggregateValueType::DateDays)
        ));
        assert!(matches!(
            typed_slot_kinds[1],
            dbsp::IncrementalAggregateSlotKind::Max(dbsp::AggregateValueType::Decimal128 {
                precision: 18,
                scale: 2,
            })
        ));
        assert_eq!(
            aggregate_numeric_value_type_from_dbsp_type(&DbspScalarType::Decimal128 {
                precision: 18,
                scale: 2,
            }),
            Some(dbsp::AggregateValueType::Decimal128 {
                precision: 18,
                scale: 2,
            })
        );

        let encoded_bounds = encode_window_bounds(10, 20).expect("encode bounds");
        let decoded_bounds =
            extract_encoded_row_scalars(&encoded_bounds, &[0, 1]).expect("decode bounds");
        assert_eq!(
            decoded_bounds,
            vec![
                Some(EncodedRowScalar::TimestampMillis(10)),
                Some(EncodedRowScalar::TimestampMillis(20))
            ]
        );

        let encoded_counts = encode_count_values(&[1, 2, 3]).expect("encode count values");
        let decoded_counts =
            extract_encoded_row_scalars(&encoded_counts, &[0, 1, 2]).expect("decode counts");
        assert_eq!(
            decoded_counts,
            vec![
                Some(EncodedRowScalar::Int64(1)),
                Some(EncodedRowScalar::Int64(2)),
                Some(EncodedRowScalar::Int64(3))
            ]
        );

        let encoded_incremental = encode_incremental_aggregate_values(&[
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::Int64),
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::TimestampMillis),
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::Utf8),
            dbsp::AggregateValue::Int64(9),
            dbsp::AggregateValue::TimestampMillis(11),
            dbsp::AggregateValue::Utf8("x".to_string()),
        ])
        .expect("encode incremental values");
        let decoded_incremental =
            extract_encoded_row_scalars(&encoded_incremental, &[0, 1, 2, 3, 4, 5])
                .expect("decode incremental values");
        assert_eq!(
            decoded_incremental,
            vec![
                None,
                None,
                None,
                Some(EncodedRowScalar::Int64(9)),
                Some(EncodedRowScalar::TimestampMillis(11)),
                Some(EncodedRowScalar::Utf8("x".to_string())),
            ]
        );

        let mut encoded = Vec::new();
        append_encoded_sum_like_value(7, &DbspScalarType::Int64, &mut encoded)
            .expect("append int sum");
        assert_eq!(
            extract_encoded_row_scalars(&[1_u32.to_le_bytes().as_slice(), &encoded].concat(), &[0])
                .expect("decode sum"),
            vec![Some(EncodedRowScalar::Int64(7))]
        );
        assert!(append_encoded_sum_like_value(1, &DbspScalarType::Utf8, &mut Vec::new()).is_err());
    }

    #[test]
    fn scalar_helpers_and_column_resolution_behave_as_expected() {
        assert!(bool_from_encoded_scalar(Some(&EncodedRowScalar::Bool(true))).expect("bool value"));
        assert!(!bool_from_encoded_scalar(None).expect("null bool"));
        assert!(bool_from_encoded_scalar(Some(&EncodedRowScalar::Int64(1))).is_err());

        assert_eq!(
            i64_from_encoded_scalar(Some(&EncodedRowScalar::TimestampMillis(5))),
            Some(5)
        );
        assert_eq!(
            i64_from_encoded_scalar(Some(&EncodedRowScalar::Utf8("x".to_string()))),
            None
        );

        assert_eq!(
            compare_encoded_scalars(&EncodedRowScalar::Int64(1), &EncodedRowScalar::Int64(2)),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            compare_encoded_scalars(
                &EncodedRowScalar::Utf8("a".to_string()),
                &EncodedRowScalar::Utf8("a".to_string())
            ),
            Some(std::cmp::Ordering::Equal)
        );
        assert_eq!(
            compare_encoded_scalars(
                &EncodedRowScalar::Int64(1),
                &EncodedRowScalar::Utf8("a".to_string())
            ),
            None
        );

        let mut scalar = Vec::new();
        append_encoded_scalar(&EncodedRowScalar::Bool(true), &mut scalar).expect("append bool");
        assert_eq!(scalar, vec![0x04, 0x01]);

        let encoded_key = encode_single_encoded_scalar_key(&EncodedRowScalar::Int64(9))
            .expect("encode single scalar key");
        assert_eq!(
            extract_encoded_row_scalars(&encoded_key, &[0]).expect("decode scalar key"),
            vec![Some(EncodedRowScalar::Int64(9))]
        );

        let input_schema = schema(vec![
            ("price", DbspScalarType::Int64),
            ("bidder", DbspScalarType::Int64),
        ]);
        let aggregate = DbspAggregateNode::try_new(
            Arc::clone(&input_schema),
            vec![(col("bidder"), None)],
            vec![(
                DbspAggregateFunction::Count,
                Some(col("price")),
                None,
                false,
                Some("count_price".to_string()),
            )],
        )
        .expect("aggregate node");

        let key_columns = direct_group_key_columns(
            aggregate.group_keys(),
            input_schema.as_ref(),
            &HashMap::new(),
        )
        .expect("direct group key columns");
        assert_eq!(key_columns, vec![1]);

        let key_expr = &aggregate.group_keys()[0];
        assert_eq!(
            direct_column_index(key_expr.expression(), input_schema.as_ref()),
            Some(1)
        );
        assert_eq!(
            resolved_expression_column_index(
                key_expr.expression(),
                input_schema.as_ref(),
                &HashMap::new()
            ),
            Some(1)
        );

        let aliased_expr = dbsp::DbspExpression::analyze(
            datafusion::logical_expr::Expr::Alias(datafusion::logical_expr::expr::Alias::new(
                col("price"),
                None::<String>,
                "p".to_string(),
            )),
            Arc::clone(&input_schema),
        )
        .expect("analyze alias expression");
        assert_eq!(
            direct_column_index(&aliased_expr, input_schema.as_ref()),
            Some(0)
        );

        assert_eq!(expression_lookup_key(aliased_expr.expr()), "price");

        assert_eq!(
            incremental_aggregate_value_from_encoded_scalar(
                Some(&EncodedRowScalar::Int64(7)),
                "graph",
                "ctx"
            ),
            Some(dbsp::AggregateValue::Int64(7))
        );
        assert_eq!(
            incremental_aggregate_value_from_encoded_scalar(
                Some(&EncodedRowScalar::Bool(true)),
                "graph",
                "ctx"
            ),
            None
        );
    }
}

#[cfg(test)]
mod aggregate_window_helper_tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};
    use dbsp::circuit::schema::Field;

    fn schema(fields: Vec<(&str, DbspScalarType)>) -> Arc<RowSchema> {
        RowSchema::try_new(
            fields
                .into_iter()
                .map(|(name, data_type)| Field::new(name, data_type, true))
                .collect(),
        )
        .expect("schema")
    }

    fn encode_row(columns: &[Option<EncodedRowScalar>]) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&(columns.len() as u32).to_le_bytes());
        for column in columns {
            match column {
                None => encoded.push(0x00),
                Some(EncodedRowScalar::Int64(value)) => {
                    encoded.push(0x01);
                    encoded.extend_from_slice(&value.to_le_bytes());
                }
                Some(EncodedRowScalar::Utf8(value)) => {
                    encoded.push(0x02);
                    let bytes = value.as_bytes();
                    encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    encoded.extend_from_slice(bytes);
                }
                Some(EncodedRowScalar::TimestampMillis(value)) => {
                    encoded.push(0x03);
                    encoded.extend_from_slice(&value.to_le_bytes());
                }
                Some(EncodedRowScalar::Bool(value)) => {
                    encoded.push(0x04);
                    encoded.push(if *value { 1 } else { 0 });
                }
                Some(EncodedRowScalar::DateDays(value)) => {
                    encoded.push(0x09);
                    encoded.extend_from_slice(&value.to_le_bytes());
                }
                Some(EncodedRowScalar::Decimal128(value)) => {
                    encoded.push(0x0B);
                    encoded.extend_from_slice(&value.to_le_bytes());
                }
            }
        }
        encoded
    }

    #[test]
    fn encode_aggregate_values_supports_count_sum_avg_min_max() {
        let input_schema = schema(vec![
            ("price", DbspScalarType::Int64),
            ("label", DbspScalarType::Utf8),
            ("flag", DbspScalarType::Bool),
        ]);
        let aggregate = DbspAggregateNode::try_new(
            Arc::clone(&input_schema),
            vec![],
            vec![
                (
                    DbspAggregateFunction::Count,
                    None,
                    None,
                    false,
                    Some("count_star".to_string()),
                ),
                (
                    DbspAggregateFunction::Count,
                    Some(col("price")),
                    None,
                    false,
                    Some("count_price".to_string()),
                ),
                (
                    DbspAggregateFunction::Count,
                    Some(col("price")),
                    Some(col("flag")),
                    true,
                    Some("count_distinct_price".to_string()),
                ),
                (
                    DbspAggregateFunction::Sum,
                    Some(col("price")),
                    None,
                    false,
                    Some("sum_price".to_string()),
                ),
                (
                    DbspAggregateFunction::Avg,
                    Some(col("price")),
                    None,
                    false,
                    Some("avg_price".to_string()),
                ),
                (
                    DbspAggregateFunction::Min,
                    Some(col("label")),
                    None,
                    false,
                    Some("min_label".to_string()),
                ),
                (
                    DbspAggregateFunction::Max,
                    Some(col("label")),
                    None,
                    false,
                    Some("max_label".to_string()),
                ),
            ],
        )
        .expect("aggregate");

        let layout = build_count_eval_layout(
            aggregate.aggregates(),
            input_schema.as_ref(),
            &HashMap::new(),
        );

        let values = vec![
            (
                encode_row(&[
                    Some(EncodedRowScalar::Int64(10)),
                    Some(EncodedRowScalar::Utf8("b".to_string())),
                    Some(EncodedRowScalar::Bool(true)),
                ]),
                1,
            ),
            (
                encode_row(&[
                    Some(EncodedRowScalar::Int64(30)),
                    Some(EncodedRowScalar::Utf8("a".to_string())),
                    Some(EncodedRowScalar::Bool(true)),
                ]),
                1,
            ),
            (
                encode_row(&[
                    Some(EncodedRowScalar::Int64(10)),
                    Some(EncodedRowScalar::Utf8("c".to_string())),
                    Some(EncodedRowScalar::Bool(false)),
                ]),
                1,
            ),
            (
                encode_row(&[None, None, Some(EncodedRowScalar::Bool(true))]),
                1,
            ),
        ];

        let encoded = encode_aggregate_values_from_encoded(
            &layout,
            aggregate.aggregates(),
            input_schema.as_ref(),
            &values,
            "test",
            "aggregate",
        )
        .expect("encode aggregate values")
        .expect("non-empty aggregate output");

        assert_eq!(
            extract_encoded_row_scalars(&encoded, &[0, 1, 2, 3, 4, 5, 6]).expect("decode output"),
            vec![
                Some(EncodedRowScalar::Int64(4)),
                Some(EncodedRowScalar::Int64(3)),
                Some(EncodedRowScalar::Int64(2)),
                Some(EncodedRowScalar::Int64(50)),
                Some(EncodedRowScalar::Int64(16)),
                Some(EncodedRowScalar::Utf8("a".to_string())),
                Some(EncodedRowScalar::Utf8("c".to_string())),
            ]
        );
    }

    #[test]
    fn encode_aggregate_values_supports_decimal_sum() {
        let input_schema = schema(vec![(
            "amount",
            DbspScalarType::Decimal128 {
                precision: 18,
                scale: 2,
            },
        )]);
        let aggregate = DbspAggregateNode::try_new(
            Arc::clone(&input_schema),
            vec![],
            vec![(
                DbspAggregateFunction::Sum,
                Some(col("amount")),
                None,
                false,
                Some("sum_amount".to_string()),
            )],
        )
        .expect("decimal aggregate");
        let layout = build_count_eval_layout(
            aggregate.aggregates(),
            input_schema.as_ref(),
            &HashMap::new(),
        );
        let values = vec![
            (encode_row(&[Some(EncodedRowScalar::Decimal128(1234))]), 1),
            (encode_row(&[Some(EncodedRowScalar::Decimal128(200))]), 2),
            (encode_row(&[None]), 1),
        ];

        let encoded = encode_aggregate_values_from_encoded(
            &layout,
            aggregate.aggregates(),
            input_schema.as_ref(),
            &values,
            "test",
            "aggregate",
        )
        .expect("encode decimal aggregate values")
        .expect("non-empty decimal aggregate output");
        assert_eq!(
            extract_encoded_row_scalars(&encoded, &[0]).expect("decode decimal sum"),
            vec![Some(EncodedRowScalar::Decimal128(1634))]
        );

        let mut encoded_decimal = Vec::new();
        append_encoded_sum_like_value(
            9999,
            &DbspScalarType::Decimal128 {
                precision: 4,
                scale: 2,
            },
            &mut encoded_decimal,
        )
        .expect("append decimal sum");
        assert_eq!(
            extract_encoded_row_scalars(
                &[1_u32.to_le_bytes().as_slice(), &encoded_decimal].concat(),
                &[0]
            )
            .expect("decode appended decimal sum"),
            vec![Some(EncodedRowScalar::Decimal128(9999))]
        );
        assert!(
            append_encoded_sum_like_value(
                10_000,
                &DbspScalarType::Decimal128 {
                    precision: 4,
                    scale: 2,
                },
                &mut Vec::new(),
            )
            .is_err()
        );
    }

    #[test]
    fn unresolved_filter_and_expression_paths_fall_back_to_zero_updates() {
        let input_schema = schema(vec![("price", DbspScalarType::Int64)]);
        let aggregate = DbspAggregateNode::try_new(
            Arc::clone(&input_schema),
            vec![],
            vec![
                (
                    DbspAggregateFunction::Count,
                    None,
                    Some(col("price").gt(lit(20_i64))),
                    false,
                    Some("filtered_count_star".to_string()),
                ),
                (
                    DbspAggregateFunction::Count,
                    Some(col("price") + lit(1_i64)),
                    None,
                    false,
                    Some("count_unresolved_expr".to_string()),
                ),
                (
                    DbspAggregateFunction::Count,
                    Some(col("price") + lit(1_i64)),
                    None,
                    true,
                    Some("count_distinct_unresolved_expr".to_string()),
                ),
            ],
        )
        .expect("aggregate");

        let layout = build_count_eval_layout(
            aggregate.aggregates(),
            input_schema.as_ref(),
            &HashMap::new(),
        );
        let row = encode_row(&[Some(EncodedRowScalar::Int64(30))]);

        let slot_updates = evaluate_count_batch_row_values(
            &layout,
            aggregate.aggregates(),
            input_schema.as_ref(),
            &[(row.clone(), 1)],
            "test",
            "aggregate",
        )
        .expect("batch slot updates")
        .remove(0);
        assert!(matches!(
            &slot_updates[0],
            dbsp::CountAggregateSlotUpdate::Linear(0)
        ));
        assert!(matches!(
            &slot_updates[1],
            dbsp::CountAggregateSlotUpdate::Linear(0)
        ));
        assert!(matches!(
            &slot_updates[2],
            dbsp::CountAggregateSlotUpdate::Distinct(None)
        ));

        let encoded = encode_aggregate_values_from_encoded(
            &layout,
            aggregate.aggregates(),
            input_schema.as_ref(),
            &[(row, 1)],
            "test",
            "aggregate",
        )
        .expect("encode unresolved output")
        .expect("resolved encoded output");
        assert_eq!(
            extract_encoded_row_scalars(&encoded, &[0, 1, 2]).expect("decode unresolved output"),
            vec![
                Some(EncodedRowScalar::Int64(0)),
                Some(EncodedRowScalar::Int64(0)),
                Some(EncodedRowScalar::Int64(0)),
            ]
        );
    }

    #[test]
    fn decode_failures_and_empty_inputs_return_none_or_default_slots() {
        let input_schema = schema(vec![("price", DbspScalarType::Int64)]);
        let aggregate = DbspAggregateNode::try_new(
            Arc::clone(&input_schema),
            vec![],
            vec![
                (
                    DbspAggregateFunction::Count,
                    None,
                    None,
                    false,
                    Some("count_star".to_string()),
                ),
                (
                    DbspAggregateFunction::Count,
                    Some(col("price")),
                    None,
                    true,
                    Some("count_distinct_price".to_string()),
                ),
            ],
        )
        .expect("aggregate");

        let layout = build_count_eval_layout(
            aggregate.aggregates(),
            input_schema.as_ref(),
            &HashMap::new(),
        );

        let invalid_row = vec![0x01_u8];
        let slots = evaluate_count_batch_row_values(
            &layout,
            aggregate.aggregates(),
            input_schema.as_ref(),
            &[(invalid_row.clone(), 1)],
            "test",
            "aggregate",
        )
        .unwrap_or_default()
        .into_iter()
        .next()
        .unwrap_or_else(|| {
            aggregate
                .aggregates()
                .iter()
                .map(|agg| {
                    if agg.distinct() {
                        dbsp::CountAggregateSlotUpdate::Distinct(None)
                    } else {
                        dbsp::CountAggregateSlotUpdate::Linear(0)
                    }
                })
                .collect()
        });
        assert!(matches!(
            slots[0],
            dbsp::CountAggregateSlotUpdate::Linear(0)
        ));
        assert!(matches!(
            slots[1],
            dbsp::CountAggregateSlotUpdate::Distinct(None)
        ));

        assert!(
            encode_aggregate_values_from_encoded(
                &layout,
                aggregate.aggregates(),
                input_schema.as_ref(),
                &[(invalid_row, 1)],
                "test",
                "aggregate",
            )
            .expect("encode invalid rows")
            .is_none()
        );

        assert!(
            encode_aggregate_values_from_encoded(
                &layout,
                aggregate.aggregates(),
                input_schema.as_ref(),
                &[],
                "test",
                "aggregate",
            )
            .expect("encode empty rows")
            .is_none()
        );

        assert!(
            encode_aggregate_values_from_encoded(
                &layout,
                &[],
                input_schema.as_ref(),
                &[(encode_row(&[Some(EncodedRowScalar::Int64(1))]), 1)],
                "test",
                "aggregate",
            )
            .expect("encode empty aggregates")
            .is_none()
        );
    }
}
