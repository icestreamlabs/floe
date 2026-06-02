use super::incremental_eval::{
    append_encoded_i64, append_encoded_scalar, append_encoded_sum_like_value, append_untyped_null,
    bool_from_arrow_array, checked_sum_add, checked_weighted_sum_delta, compare_encoded_scalars,
    direct_group_key_columns, encode_single_encoded_scalar_key, encoded_scalar_from_arrow_array,
    i64_from_encoded_scalar, resolved_expression_column_index, sum_numeric_from_encoded_scalar,
};
use super::shared::{
    CountEvalLayout, CountEvalPlan, EncodedAggregateAccumulator, ExpressionColumnMap,
    count_eval_record_batch,
};
use super::*;
use crate::vectorized_keys::VectorizedEncodedKeyExtractor;
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::Expr;
use std::collections::BTreeSet;

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

pub(super) fn is_simple_count_star_aggregate(aggregates: &[DbspAggregateExpr]) -> bool {
    aggregates.len() == 1 && aggregates.iter().all(is_unconditional_count_aggregate)
}

pub(super) fn is_unconditional_count_aggregate(agg: &DbspAggregateExpr) -> bool {
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

pub(super) fn build_count_eval_layout(
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

pub(super) fn evaluate_count_batch_row_values(
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

pub(super) fn evaluate_count_arrow_row_values(
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

pub(super) fn encode_aggregate_values_from_encoded(
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
