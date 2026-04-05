use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, anyhow};
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
use crate::encoding::{decode_projected_row_key, encode_projected_row_key};
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

impl TopNValue {
    fn from_scalar(value: &ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Int64(Some(v)) => Ok(Self::Int64(*v)),
            ScalarValue::Int64(None) => Ok(Self::Null),
            ScalarValue::TimestampMillisecond(Some(v), _) => Ok(Self::Timestamp(*v)),
            ScalarValue::TimestampMillisecond(None, _) => Ok(Self::Null),
            ScalarValue::Utf8(Some(v)) => Ok(Self::Utf8(v.clone())),
            ScalarValue::Utf8(None) => Ok(Self::Null),
            ScalarValue::Boolean(Some(v)) => Ok(Self::Bool(*v)),
            ScalarValue::Boolean(None) | ScalarValue::Null => Ok(Self::Null),
            other => Err(anyhow!("unsupported sort value {other:?}")),
        }
    }
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

fn null_scalar_for_dbsp_type(data_type: &DbspScalarType) -> ScalarValue {
    match data_type {
        DbspScalarType::Int64 => ScalarValue::Int64(None),
        DbspScalarType::Utf8 => ScalarValue::Utf8(None),
        DbspScalarType::TimestampMillis => ScalarValue::TimestampMillisecond(None, None),
        DbspScalarType::Bool => ScalarValue::Boolean(None),
    }
}

fn scalar_to_i64(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::TimestampMillisecond(Some(v), _) => Some(*v),
        _ => None,
    }
}

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

enum AggregateAccumulator {
    Count { count: i64 },
    CountDistinct { weights: HashMap<ScalarValue, i64> },
    Sum { sum: i64, has_value: bool },
    Avg { sum: i64, count: i64 },
    Min { current: Option<ScalarValue> },
    Max { current: Option<ScalarValue> },
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

    let mut filters: Vec<&dbsp::DbspExpression> = Vec::new();
    let mut expressions: Vec<&dbsp::DbspExpression> = Vec::new();
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

    for (row, weight) in decoded {
        if *weight == 0 {
            continue;
        }

        for (index, filter) in filters.iter().enumerate() {
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

        expression_valid.fill(false);
        for (index, expr) in expressions.iter().enumerate() {
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
                    *entry += *weight;
                    if *entry == 0 {
                        weights.remove(&value);
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
