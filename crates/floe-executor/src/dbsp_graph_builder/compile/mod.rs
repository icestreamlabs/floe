use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, anyhow};
#[cfg(test)]
use datafusion::common::Column;
#[cfg(test)]
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;
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
use crate::encoding::decode_projected_row_key;
use crate::encoding::{EncodedRowScalar, encode_projected_row_key, extract_encoded_row_scalars};
use crate::task_events::{GraphTaskSender, report_graph_task_error};

use super::builder::DbspGraphBuilder;
#[cfg(test)]
use dbsp::DbspPredicate;
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
    scalar_from_incremental_aggregate_value,
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
    for (row, diff) in materialized.into_iter().take(3) {
        let decoded = decode_all_encoded_row_scalars(&row);
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

fn decode_all_encoded_row_scalars(bytes: &[u8]) -> Result<Vec<Option<EncodedRowScalar>>> {
    if bytes.len() < 4 {
        return Err(anyhow!("encoded key too short"));
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let indices = (0..count).collect::<Vec<_>>();
    extract_encoded_row_scalars(bytes, indices.as_slice())
}

fn null_scalar_for_dbsp_type(data_type: &DbspScalarType) -> ScalarValue {
    match data_type {
        DbspScalarType::Int64 => ScalarValue::Int64(None),
        DbspScalarType::Utf8 => ScalarValue::Utf8(None),
        DbspScalarType::TimestampMillis => ScalarValue::TimestampMillisecond(None, None),
        DbspScalarType::Bool => ScalarValue::Boolean(None),
    }
}

#[cfg(test)]
fn scalar_to_i64(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::TimestampMillisecond(Some(v), _) => Some(*v),
        _ => None,
    }
}

#[cfg(test)]
fn compare_scalar_values(left: &ScalarValue, right: &ScalarValue) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => Some(l.cmp(r)),
        (
            ScalarValue::TimestampMillisecond(Some(l), _),
            ScalarValue::TimestampMillisecond(Some(r), _),
        ) => Some(l.cmp(r)),
        (ScalarValue::Utf8(Some(l)), ScalarValue::Utf8(Some(r))) => Some(l.cmp(r)),
        (ScalarValue::Boolean(Some(l)), ScalarValue::Boolean(Some(r))) => Some(l.cmp(r)),
        _ => None,
    }
}

#[cfg(test)]
fn evaluate_aggregate_value(
    agg: &DbspAggregateExpr,
    decoded: &[(Vec<ScalarValue>, i64)],
    schema: &RowSchema,
    graph_id: &str,
    context: &str,
) -> ScalarValue {
    evaluate_aggregate_values(
        std::slice::from_ref(agg),
        decoded,
        schema,
        graph_id,
        context,
    )
    .into_iter()
    .next()
    .unwrap_or(ScalarValue::Null)
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
    Count { count: i64 },
    CountDistinct { weights: HashMap<ScalarValue, i64> },
    Sum { sum: i64, has_value: bool },
    Avg { sum: i64, count: i64 },
    Min { current: Option<ScalarValue> },
    Max { current: Option<ScalarValue> },
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
    decoded: &[(Vec<ScalarValue>, i64)],
    schema: &RowSchema,
    graph_id: &str,
    context: &str,
) -> Vec<ScalarValue> {
    if aggregates.is_empty() {
        return Vec::new();
    }

    let mut encoded = Vec::with_capacity(decoded.len());
    for (row, weight) in decoded {
        match encode_projected_row_key(row) {
            Ok(encoded_row) => encoded.push((encoded_row, *weight)),
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to encode {context} test input row"
                );
            }
        }
    }

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
                return vec![ScalarValue::Null; aggregates.len()];
            }
        };
        eval_schema = Arc::clone(project.output_schema());
        let predicate = match DbspPredicate::try_new(
            Expr::Literal(ScalarValue::Boolean(Some(true)), None),
            Arc::clone(&input_schema),
        ) {
            Ok(predicate) => predicate,
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to build {context} test aggregate predicate"
                );
                return vec![ScalarValue::Null; aggregates.len()];
            }
        };
        let evaluator = match VectorizedFilterProjectEvaluator::for_filter_map(
            &predicate,
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
                return vec![ScalarValue::Null; aggregates.len()];
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
                return vec![ScalarValue::Null; aggregates.len()];
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
    let mut expression_values = vec![ScalarValue::Null; expressions.len()];
    let mut expression_valid = vec![false; expressions.len()];

    for (encoded_row, weight) in encoded {
        if weight == 0 {
            continue;
        }
        let row = match decode_projected_row_key(&encoded_row) {
            Ok(row) => row,
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
                let value = row.get(column_idx).unwrap_or(&ScalarValue::Null);
                filter_results[index] = match crate::expression::scalar_to_bool(value) {
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
                    let value = expression_values[expr_index].clone();
                    if value.is_null() {
                        continue;
                    }
                    let entry = weights.entry(value.clone()).or_insert(0);
                    *entry += weight;
                    if *entry == 0 {
                        weights.remove(&value);
                    }
                }
                AggregateAccumulator::Count { count } => match plan.expr_index {
                    Some(expr_index) => {
                        if expression_valid[expr_index] && !expression_values[expr_index].is_null()
                        {
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
                    if let Some(number) = scalar_to_i64(&expression_values[expr_index]) {
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
                    if let Some(number) = scalar_to_i64(&expression_values[expr_index]) {
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
                    let value = expression_values[expr_index].clone();
                    if value.is_null() {
                        continue;
                    }
                    let next = match current.take() {
                        Some(existing) => match compare_scalar_values(&value, &existing) {
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
                    let value = expression_values[expr_index].clone();
                    if value.is_null() {
                        continue;
                    }
                    let next = match current.take() {
                        Some(existing) => match compare_scalar_values(&value, &existing) {
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
            AggregateAccumulator::CountDistinct { weights } => ScalarValue::Int64(Some(
                weights.values().filter(|weight| **weight > 0).count() as i64,
            )),
            AggregateAccumulator::Count { count } => ScalarValue::Int64(Some(count)),
            AggregateAccumulator::Sum { sum, has_value } => {
                if has_value {
                    scalar_from_i64(sum, plan.agg.output_type())
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

#[cfg(test)]
fn scalar_from_i64(value: i64, output_type: &DbspScalarType) -> ScalarValue {
    match output_type {
        DbspScalarType::Int64 => ScalarValue::Int64(Some(value)),
        DbspScalarType::TimestampMillis => ScalarValue::TimestampMillisecond(Some(value), None),
        _ => ScalarValue::Int64(Some(value)),
    }
}

#[cfg(test)]
mod tests {
    use super::{evaluate_aggregate_value, evaluate_aggregate_values};
    use datafusion::logical_expr::{col, lit};
    use datafusion::scalar::ScalarValue;
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

        let decoded = vec![
            (
                vec![ScalarValue::Int64(Some(10)), ScalarValue::Int64(Some(1))],
                2,
            ),
            (
                vec![ScalarValue::Int64(Some(10)), ScalarValue::Int64(Some(2))],
                1,
            ),
            (
                vec![ScalarValue::Int64(Some(200)), ScalarValue::Int64(Some(3))],
                1,
            ),
        ];
        let value =
            evaluate_aggregate_value(expr, &decoded, input_schema.as_ref(), "test", "aggregate");
        assert_eq!(value, ScalarValue::Int64(Some(2)));

        let decoded = vec![
            (
                vec![ScalarValue::Int64(Some(10)), ScalarValue::Int64(Some(1))],
                1,
            ),
            (
                vec![ScalarValue::Int64(Some(10)), ScalarValue::Int64(Some(2))],
                -1,
            ),
        ];
        let value =
            evaluate_aggregate_value(expr, &decoded, input_schema.as_ref(), "test", "aggregate");
        assert_eq!(value, ScalarValue::Int64(Some(1)));
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
        let decoded = vec![
            (vec![ScalarValue::Utf8(Some("alpha".to_string()))], 1),
            (vec![ScalarValue::Utf8(Some("zeta".to_string()))], 1),
        ];
        let value =
            evaluate_aggregate_value(expr, &decoded, input_schema.as_ref(), "test", "aggregate");
        assert_eq!(value, ScalarValue::Utf8(Some("zeta".to_string())));
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
        let decoded = vec![
            (
                vec![
                    ScalarValue::Int64(Some(10)),
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Int64(Some(100)),
                ],
                2,
            ),
            (
                vec![
                    ScalarValue::Int64(Some(250)),
                    ScalarValue::Int64(Some(2)),
                    ScalarValue::Int64(Some(100)),
                ],
                1,
            ),
            (
                vec![
                    ScalarValue::Int64(Some(80)),
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Int64(Some(200)),
                ],
                1,
            ),
        ];

        let multi = evaluate_aggregate_values(
            aggregate.aggregates(),
            &decoded,
            input_schema.as_ref(),
            "test",
            "aggregate",
        );
        let single = aggregate
            .aggregates()
            .iter()
            .map(|expr| {
                evaluate_aggregate_value(expr, &decoded, input_schema.as_ref(), "test", "aggregate")
            })
            .collect::<Vec<_>>();
        assert_eq!(multi, single);
    }
}
