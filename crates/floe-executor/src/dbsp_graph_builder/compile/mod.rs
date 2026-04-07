use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, anyhow};
#[cfg(test)]
use datafusion::common::Column;
#[cfg(test)]
use datafusion::logical_expr::Expr;
use dbsp::circuit::plan::{DbspAggregateExpr, DbspProjectExpr};
use dbsp::handles::ZSetHandle;
use dbsp::operators::semijoin::SemiJoinMode;
use dbsp::stream::runtime::RuntimeErrorHandler;
use dbsp::stream::{DeltaHandleStream, StreamCursor};
use dbsp::{
    DbspAggregate, DbspAggregateFunction, DbspAggregateNode, DbspCountAggregate, DbspDistinct,
    DbspDistinctNode, DbspFilterMap, DbspJoin, DbspJoinNode, DbspJoinType, DbspMap,
    DbspProjectNode, DbspScalarType, DbspSelectNode, DbspSemiJoin, DbspSourceNode, DbspTopN,
    DbspTopNNode, DbspUnion, DbspUnionNode, DbspWindowAggregate, DbspWindowAggregateNode,
    DbspWindowPolicy, RowSchema, WindowKey,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::dbsp_bridge::DbspBridge;
#[cfg(test)]
use crate::dbsp_graph_builder::vectorized_filter_project::VectorizedFilterProjectEvaluator;
#[cfg(test)]
use crate::encoding::EncodedRowScalar;
use crate::encoding::decode_all_encoded_row_scalars_into;
use crate::task_events::{GraphTaskSender, report_graph_task_error};

use super::builder::DbspGraphBuilder;
#[derive(Clone, Debug, Eq, PartialEq)]
struct TopNSortSpec {
    ascending: bool,
    nulls_first: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TopNValue {
    Null,
    Int64(i64),
    Timestamp(i64),
    Utf8(String),
    Bool(bool),
}

impl Ord for TopNValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use TopNValue::*;
        let rank = |value: &TopNValue| -> u8 {
            match value {
                Null => 0,
                Int64(_) => 1,
                Timestamp(_) => 2,
                Utf8(_) => 3,
                Bool(_) => 4,
            }
        };

        let left_rank = rank(self);
        let right_rank = rank(other);
        if left_rank != right_rank {
            return left_rank.cmp(&right_rank);
        }

        match (self, other) {
            (Null, Null) => std::cmp::Ordering::Equal,
            (Int64(a), Int64(b)) => a.cmp(b),
            (Timestamp(a), Timestamp(b)) => a.cmp(b),
            (Utf8(a), Utf8(b)) => a.cmp(b),
            (Bool(a), Bool(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        }
    }
}

impl PartialOrd for TopNValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TopNKey {
    specs: Arc<Vec<TopNSortSpec>>,
    values: Vec<TopNValue>,
    tie_breaker: Vec<u8>,
}

impl TopNKey {
    fn new(specs: Arc<Vec<TopNSortSpec>>, values: Vec<TopNValue>, tie_breaker: Vec<u8>) -> Self {
        Self {
            specs,
            values,
            tie_breaker,
        }
    }
}

impl Ord for TopNKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for (idx, spec) in self.specs.iter().enumerate() {
            let left = self.values.get(idx);
            let right = other.values.get(idx);
            let (left, right) = match (left, right) {
                (Some(left), Some(right)) => (left, right),
                _ => continue,
            };

            let cmp = match (left, right) {
                (TopNValue::Null, TopNValue::Null) => std::cmp::Ordering::Equal,
                (TopNValue::Null, _) => {
                    if spec.nulls_first {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    }
                }
                (_, TopNValue::Null) => {
                    if spec.nulls_first {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                }
                _ => {
                    let cmp = left.cmp(right);
                    if spec.ascending { cmp } else { cmp.reverse() }
                }
            };

            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }

        self.tie_breaker.cmp(&other.tie_breaker)
    }
}

impl PartialOrd for TopNKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

mod aggregate_window_phase;
mod join_phase;
mod set_ops_topn_phase;
mod source_map_phase;

pub(crate) use aggregate_window_phase::{
    build_count_aggregate_slot_kinds, build_count_row_evaluator,
    build_incremental_aggregate_row_evaluator, build_incremental_aggregate_slot_kinds,
};

async fn log_handle_rows(
    label: &str,
    handle: &ZSetHandle,
    bridge: &Arc<Mutex<DbspBridge>>,
) -> Result<()> {
    if !tracing::event_enabled!(tracing::Level::DEBUG) {
        return Ok(());
    }
    let mut guard = bridge.lock().await;
    let handle_view = guard
        .handle_view_for(&handle.ns, handle.version)
        .await
        .context("open handle view for logging")?;
    let materialized = handle_view.materialize().await?;
    let total = materialized.len();
    let mut rows = Vec::new();
    let mut decode_scratch = Vec::new();
    for (row, diff) in materialized.into_iter().take(3) {
        let decoded = decode_all_encoded_row_scalars_into(&row, &mut decode_scratch)
            .map(|_| decode_scratch.clone());
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

#[cfg(test)]
fn encoded_scalar_to_i64(value: Option<&EncodedRowScalar>) -> Option<i64> {
    match value {
        Some(EncodedRowScalar::Int64(v) | EncodedRowScalar::TimestampMillis(v)) => Some(*v),
        _ => None,
    }
}

#[cfg(test)]
fn compare_encoded_scalar_values(
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

#[cfg(test)]
fn evaluate_aggregate_value(
    agg: &DbspAggregateExpr,
    encoded_rows: &[(Vec<u8>, i64)],
    schema: &RowSchema,
    graph_id: &str,
    context: &str,
) -> Option<EncodedRowScalar> {
    evaluate_aggregate_values(
        std::slice::from_ref(agg),
        encoded_rows,
        schema,
        graph_id,
        context,
    )
    .into_iter()
    .next()
    .unwrap_or(None)
}

#[cfg(test)]
#[derive(Clone, Copy)]
struct AggregateEvalPlan<'a> {
    agg: &'a DbspAggregateExpr,
    filter_index: Option<usize>,
    expr_index: Option<usize>,
}

#[cfg(test)]
enum AggregateAccumulator {
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

#[cfg(test)]
fn aggregate_eval_direct_column_index(
    expr: &dbsp::DbspExpression,
    schema: &RowSchema,
) -> Option<usize> {
    match expr.expr() {
        Expr::Alias(alias) => aggregate_eval_direct_column_index_expr(alias.expr.as_ref(), schema),
        other => aggregate_eval_direct_column_index_expr(other, schema),
    }
}

#[cfg(test)]
fn aggregate_eval_direct_column_index_expr(expr: &Expr, schema: &RowSchema) -> Option<usize> {
    match expr {
        Expr::Column(column) => {
            let qualified = column.flat_name();
            schema
                .field_index(&qualified)
                .or_else(|| schema.field_index(&column.name))
        }
        Expr::Alias(alias) => aggregate_eval_direct_column_index_expr(alias.expr.as_ref(), schema),
        _ => None,
    }
}

#[cfg(test)]
fn aggregate_eval_expr_key(expr: &Expr) -> String {
    match expr {
        Expr::Alias(alias) => aggregate_eval_expr_key(alias.expr.as_ref()),
        other => other.to_string(),
    }
}

#[cfg(test)]
fn evaluate_aggregate_values(
    aggregates: &[DbspAggregateExpr],
    encoded_rows: &[(Vec<u8>, i64)],
    schema: &RowSchema,
    graph_id: &str,
    context: &str,
) -> Vec<Option<EncodedRowScalar>> {
    if aggregates.is_empty() {
        return Vec::new();
    }
    let mut encoded = encoded_rows.to_vec();

    let input_schema = Arc::new(schema.clone());
    let mut expr_column_map = HashMap::new();
    let mut unique_non_direct = Vec::new();
    let mut seen_non_direct = HashMap::new();
    for agg in aggregates {
        if let Some(filter) = agg.filter()
            && aggregate_eval_direct_column_index(filter, schema).is_none()
        {
            let key = aggregate_eval_expr_key(filter.expr());
            if !seen_non_direct.contains_key(&key) {
                unique_non_direct.push((key.clone(), filter.expr().clone()));
                seen_non_direct.insert(key, ());
            }
        }
        if let Some(expr) = agg.expression()
            && aggregate_eval_direct_column_index(expr, schema).is_none()
        {
            let key = aggregate_eval_expr_key(expr.expr());
            if !seen_non_direct.contains_key(&key) {
                unique_non_direct.push((key.clone(), expr.expr().clone()));
                seen_non_direct.insert(key, ());
            }
        }
    }

    let mut eval_schema = Arc::clone(&input_schema);
    if !unique_non_direct.is_empty() {
        let mut items = Vec::with_capacity(input_schema.len() + unique_non_direct.len());
        for field in input_schema.fields() {
            items.push(dbsp::circuit::plan::ProjectItem {
                expr: Expr::Column(Column::new_unqualified(field.name.clone())),
                alias: Some(field.name.clone()),
            });
        }
        let mut next_index = input_schema.len();
        for (index, (key, expr)) in unique_non_direct.into_iter().enumerate() {
            let alias = format!("__floe_test_agg_expr_{index}");
            items.push(dbsp::circuit::plan::ProjectItem {
                expr,
                alias: Some(alias),
            });
            expr_column_map.insert(key, next_index);
            next_index += 1;
        }
        let project = match DbspProjectNode::try_new(Arc::clone(&input_schema), items) {
            Ok(project) => project,
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to build {context} test aggregate projection"
                );
                return vec![None; aggregates.len()];
            }
        };
        eval_schema = Arc::clone(project.output_schema());
        let evaluator = match VectorizedFilterProjectEvaluator::for_map(
            project.expressions(),
            Arc::clone(&input_schema),
        ) {
            Ok(evaluator) => evaluator,
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to initialize {context} test aggregate evaluator"
                );
                return vec![None; aggregates.len()];
            }
        };
        encoded = match evaluator.transform_delta(graph_id, encoded) {
            Ok(delta) => delta,
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to apply {context} test aggregate projection"
                );
                return vec![None; aggregates.len()];
            }
        };
    }

    let mut filters: Vec<&dbsp::DbspExpression> = Vec::new();
    let mut filter_columns = Vec::new();
    let mut expressions: Vec<&dbsp::DbspExpression> = Vec::new();
    let mut expression_columns = Vec::new();
    let mut plans = Vec::with_capacity(aggregates.len());
    let mut accumulators = Vec::with_capacity(aggregates.len());

    for agg in aggregates {
        let filter_index = agg.filter().map(|filter| {
            if let Some(existing) = filters
                .iter()
                .position(|existing: &&_| existing.expr() == filter.expr())
            {
                existing
            } else {
                filters.push(filter);
                let column = aggregate_eval_direct_column_index(filter, eval_schema.as_ref())
                    .or_else(|| {
                        expr_column_map
                            .get(&aggregate_eval_expr_key(filter.expr()))
                            .copied()
                    });
                filter_columns.push(column);
                filters.len() - 1
            }
        });
        let expr_index = agg.expression().map(|expr| {
            if let Some(existing) = expressions
                .iter()
                .position(|existing: &&_| existing.expr() == expr.expr())
            {
                existing
            } else {
                expressions.push(expr);
                let column = aggregate_eval_direct_column_index(expr, eval_schema.as_ref())
                    .or_else(|| {
                        expr_column_map
                            .get(&aggregate_eval_expr_key(expr.expr()))
                            .copied()
                    });
                expression_columns.push(column);
                expressions.len() - 1
            }
        });
        plans.push(AggregateEvalPlan {
            agg,
            filter_index,
            expr_index,
        });
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

    let mut filter_results = vec![false; filters.len()];
    let mut expression_values = vec![None; expressions.len()];
    let mut expression_valid = vec![false; expressions.len()];
    let mut decode_scratch = Vec::new();

    for (encoded_row, weight) in encoded {
        if weight == 0 {
            continue;
        }
        let row = match decode_all_encoded_row_scalars_into(&encoded_row, &mut decode_scratch) {
            Ok(()) => &decode_scratch,
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to decode {context} test aggregate row"
                );
                continue;
            }
        };

        for (index, filter) in filters.iter().enumerate() {
            if let Some(column_idx) = filter_columns[index] {
                let value = row.get(column_idx).and_then(|scalar| scalar.as_ref());
                filter_results[index] = match value {
                    Some(EncodedRowScalar::Bool(include)) => *include,
                    None => false,
                    Some(other) => {
                        tracing::warn!(
                            graph_id = %graph_id,
                            value = ?other,
                            "failed to evaluate {context} FILTER expression"
                        );
                        false
                    }
                };
            } else {
                tracing::warn!(
                    graph_id = %graph_id,
                    expression = ?filter.expr(),
                    "missing precomputed FILTER column in {context} test aggregate evaluation"
                );
                filter_results[index] = false;
            }
        }

        expression_valid.fill(false);
        for (index, expr) in expressions.iter().enumerate() {
            if let Some(column_idx) = expression_columns[index] {
                if let Some(value) = row.get(column_idx) {
                    expression_values[index] = value.clone();
                    expression_valid[index] = true;
                }
            } else {
                tracing::warn!(
                    graph_id = %graph_id,
                    expression = ?expr.expr(),
                    "missing precomputed aggregate expression column in {context} test evaluation"
                );
            }
        }

        for (plan, accumulator) in plans.iter().zip(accumulators.iter_mut()) {
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
                    let Some(value) = expression_values[expr_index].clone() else {
                        continue;
                    };
                    let entry = weights.entry(value.clone()).or_insert(0);
                    *entry += weight;
                    if *entry == 0 {
                        weights.remove(&value);
                    }
                }
                AggregateAccumulator::Count { count } => match plan.expr_index {
                    Some(expr_index) => {
                        if expression_valid[expr_index] && expression_values[expr_index].is_some() {
                            *count += weight;
                        }
                    }
                    None => *count += weight,
                },
                AggregateAccumulator::Sum { sum, has_value } => {
                    let Some(expr_index) = plan.expr_index else {
                        continue;
                    };
                    if !expression_valid[expr_index] {
                        continue;
                    }
                    if let Some(number) =
                        encoded_scalar_to_i64(expression_values[expr_index].as_ref())
                    {
                        *sum += number * weight;
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
                    if let Some(number) =
                        encoded_scalar_to_i64(expression_values[expr_index].as_ref())
                    {
                        *sum += number * weight;
                        *count += weight;
                    }
                }
                AggregateAccumulator::Min { current } => {
                    let Some(expr_index) = plan.expr_index else {
                        continue;
                    };
                    if !expression_valid[expr_index] {
                        continue;
                    }
                    let Some(value) = expression_values[expr_index].clone() else {
                        continue;
                    };
                    let next = match current.take() {
                        Some(existing) => match compare_encoded_scalar_values(&value, &existing) {
                            Some(std::cmp::Ordering::Less) => value,
                            Some(_) | None => existing,
                        },
                        None => value,
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
                    let Some(value) = expression_values[expr_index].clone() else {
                        continue;
                    };
                    let next = match current.take() {
                        Some(existing) => match compare_encoded_scalar_values(&value, &existing) {
                            Some(std::cmp::Ordering::Greater) => value,
                            Some(_) | None => existing,
                        },
                        None => value,
                    };
                    *current = Some(next);
                }
            }
        }
    }

    plans
        .iter()
        .zip(accumulators.into_iter())
        .map(|(plan, accumulator)| match accumulator {
            AggregateAccumulator::CountDistinct { weights } => Some(EncodedRowScalar::Int64(
                weights.values().filter(|weight| **weight > 0).count() as i64,
            )),
            AggregateAccumulator::Count { count } => Some(EncodedRowScalar::Int64(count)),
            AggregateAccumulator::Sum { sum, has_value } => {
                if has_value {
                    Some(scalar_from_i64(sum, plan.agg.output_type()))
                } else {
                    None
                }
            }
            AggregateAccumulator::Avg { sum, count } => {
                if count != 0 {
                    Some(EncodedRowScalar::Int64(sum / count))
                } else {
                    None
                }
            }
            AggregateAccumulator::Min { current } | AggregateAccumulator::Max { current } => {
                current
            }
        })
        .collect()
}

#[cfg(test)]
fn scalar_from_i64(value: i64, output_type: &DbspScalarType) -> EncodedRowScalar {
    match output_type {
        DbspScalarType::Int64 => EncodedRowScalar::Int64(value),
        DbspScalarType::TimestampMillis => EncodedRowScalar::TimestampMillis(value),
        _ => EncodedRowScalar::Int64(value),
    }
}

#[cfg(test)]
mod tests {
    use super::{EncodedRowScalar, evaluate_aggregate_value, evaluate_aggregate_values};
    use datafusion::logical_expr::{col, lit};
    use dbsp::circuit::schema::{Field, RowSchema};
    use dbsp::circuit::types::DbspScalarType;
    use dbsp::{DbspAggregateFunction, DbspAggregateNode};
    use std::sync::Arc;

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
            }
        }
        encoded
    }

    #[test]
    fn aggregate_eval_supports_filtered_distinct_count() {
        let input_schema = schema(vec![
            ("price", DbspScalarType::Int64),
            ("bidder", DbspScalarType::Int64),
        ]);
        let aggregate = DbspAggregateNode::try_new(
            Arc::clone(&input_schema),
            vec![],
            vec![(
                DbspAggregateFunction::Count,
                Some(col("bidder")),
                Some(col("price").lt(lit(100_i64))),
                true,
                Some("filtered_distinct_bidders".to_string()),
            )],
        )
        .expect("aggregate node");
        let expr = &aggregate.aggregates()[0];

        let encoded_rows = vec![
            (
                encode_test_row(&[
                    Some(EncodedRowScalar::Int64(10)),
                    Some(EncodedRowScalar::Int64(1)),
                ]),
                2,
            ),
            (
                encode_test_row(&[
                    Some(EncodedRowScalar::Int64(10)),
                    Some(EncodedRowScalar::Int64(2)),
                ]),
                1,
            ),
            (
                encode_test_row(&[
                    Some(EncodedRowScalar::Int64(200)),
                    Some(EncodedRowScalar::Int64(3)),
                ]),
                1,
            ),
        ];
        let value = evaluate_aggregate_value(
            expr,
            &encoded_rows,
            input_schema.as_ref(),
            "test",
            "aggregate",
        );
        assert_eq!(value, Some(EncodedRowScalar::Int64(2)));

        let encoded_rows = vec![
            (
                encode_test_row(&[
                    Some(EncodedRowScalar::Int64(10)),
                    Some(EncodedRowScalar::Int64(1)),
                ]),
                1,
            ),
            (
                encode_test_row(&[
                    Some(EncodedRowScalar::Int64(10)),
                    Some(EncodedRowScalar::Int64(2)),
                ]),
                -1,
            ),
        ];
        let value = evaluate_aggregate_value(
            expr,
            &encoded_rows,
            input_schema.as_ref(),
            "test",
            "aggregate",
        );
        assert_eq!(value, Some(EncodedRowScalar::Int64(1)));
    }

    #[test]
    fn aggregate_eval_supports_utf8_max() {
        let input_schema = schema(vec![("label", DbspScalarType::Utf8)]);
        let aggregate = DbspAggregateNode::try_new(
            Arc::clone(&input_schema),
            vec![],
            vec![(
                DbspAggregateFunction::Max,
                Some(col("label")),
                None,
                false,
                Some("max_label".to_string()),
            )],
        )
        .expect("aggregate node");
        let expr = &aggregate.aggregates()[0];
        let encoded_rows = vec![
            (
                encode_test_row(&[Some(EncodedRowScalar::Utf8("alpha".to_string()))]),
                1,
            ),
            (
                encode_test_row(&[Some(EncodedRowScalar::Utf8("zeta".to_string()))]),
                1,
            ),
        ];
        let value = evaluate_aggregate_value(
            expr,
            &encoded_rows,
            input_schema.as_ref(),
            "test",
            "aggregate",
        );
        assert_eq!(value, Some(EncodedRowScalar::Utf8("zeta".to_string())));
    }

    #[test]
    fn aggregate_eval_multi_pass_matches_single_pass() {
        let input_schema = schema(vec![
            ("price", DbspScalarType::Int64),
            ("bidder", DbspScalarType::Int64),
            ("auction", DbspScalarType::Int64),
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
                    Some("total_bids".to_string()),
                ),
                (
                    DbspAggregateFunction::Count,
                    Some(col("bidder")),
                    None,
                    true,
                    Some("total_bidders".to_string()),
                ),
                (
                    DbspAggregateFunction::Count,
                    Some(col("auction")),
                    Some(col("price").lt(lit(100_i64))),
                    true,
                    Some("cheap_auctions".to_string()),
                ),
                (
                    DbspAggregateFunction::Max,
                    Some(col("price")),
                    None,
                    false,
                    Some("max_price".to_string()),
                ),
            ],
        )
        .expect("aggregate node");
        let encoded_rows = vec![
            (
                encode_test_row(&[
                    Some(EncodedRowScalar::Int64(10)),
                    Some(EncodedRowScalar::Int64(1)),
                    Some(EncodedRowScalar::Int64(100)),
                ]),
                2,
            ),
            (
                encode_test_row(&[
                    Some(EncodedRowScalar::Int64(250)),
                    Some(EncodedRowScalar::Int64(2)),
                    Some(EncodedRowScalar::Int64(100)),
                ]),
                1,
            ),
            (
                encode_test_row(&[
                    Some(EncodedRowScalar::Int64(80)),
                    Some(EncodedRowScalar::Int64(1)),
                    Some(EncodedRowScalar::Int64(200)),
                ]),
                1,
            ),
        ];

        let multi = evaluate_aggregate_values(
            aggregate.aggregates(),
            &encoded_rows,
            input_schema.as_ref(),
            "test",
            "aggregate",
        );
        let single = aggregate
            .aggregates()
            .iter()
            .map(|expr| {
                evaluate_aggregate_value(
                    expr,
                    &encoded_rows,
                    input_schema.as_ref(),
                    "test",
                    "aggregate",
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(multi, single);
    }
}
