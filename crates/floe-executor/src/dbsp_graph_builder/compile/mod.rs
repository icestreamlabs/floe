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
    DbspAggregate, DbspAggregateFunction, DbspAggregateNode, DbspDistinct, DbspDistinctNode,
    DbspFilterMap, DbspJoin, DbspJoinNode, DbspJoinType, DbspMap, DbspProjectNode, DbspScalarType,
    DbspSelectNode, DbspSemiJoin, DbspSourceNode, DbspTopN, DbspTopNNode, DbspUnion, DbspUnionNode,
    DbspWindowAggregate, DbspWindowAggregateNode, DbspWindowPolicy, RowSchema, WindowKey,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::dbsp_bridge::DbspBridge;
use crate::encoding::{decode_projected_row_key, encode_projected_row_key};
use crate::task_events::{GraphTaskSender, report_graph_task_error};

use super::builder::DbspGraphBuilder;
use super::eval::{eval_expression, eval_scalar_expression};

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

fn evaluate_aggregate_value(
    agg: &DbspAggregateExpr,
    decoded: &[(Vec<ScalarValue>, i64)],
    schema: &RowSchema,
    graph_id: &str,
    context: &str,
) -> ScalarValue {
    let mut filtered_rows = Vec::with_capacity(decoded.len());
    for (row, weight) in decoded {
        if *weight == 0 {
            continue;
        }
        if !row_passes_aggregate_filter(agg, row, schema, graph_id, context) {
            continue;
        }
        filtered_rows.push((row.as_slice(), *weight));
    }

    match agg.function() {
        DbspAggregateFunction::Count => {
            if agg.distinct() {
                let Some(expr) = agg.expression() else {
                    return ScalarValue::Int64(Some(0));
                };
                let mut distinct_weights: HashMap<ScalarValue, i64> = HashMap::new();
                for (row, weight) in &filtered_rows {
                    match eval_scalar_expression(expr, row, schema) {
                        Ok(value) => {
                            if value.is_null() {
                                continue;
                            }
                            let entry = distinct_weights.entry(value.clone()).or_insert(0);
                            *entry += *weight;
                            if *entry == 0 {
                                distinct_weights.remove(&value);
                            }
                        }
                        Err(err) => {
                            tracing::warn!(
                                graph_id = %graph_id,
                                error = %err,
                                "failed to evaluate {context} COUNT(DISTINCT) expression"
                            );
                        }
                    }
                }
                let count = distinct_weights
                    .values()
                    .filter(|weight| **weight > 0)
                    .count() as i64;
                ScalarValue::Int64(Some(count))
            } else {
                let mut count = 0i64;
                match agg.expression() {
                    Some(expr) => {
                        for (row, weight) in &filtered_rows {
                            match eval_scalar_expression(expr, row, schema) {
                                Ok(value) => {
                                    if !value.is_null() {
                                        count += *weight;
                                    }
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        graph_id = %graph_id,
                                        error = %err,
                                        "failed to evaluate {context} count expression"
                                    );
                                }
                            }
                        }
                    }
                    None => {
                        for (_, weight) in &filtered_rows {
                            count += *weight;
                        }
                    }
                }
                ScalarValue::Int64(Some(count))
            }
        }
        DbspAggregateFunction::Sum => {
            let Some(expr) = agg.expression() else {
                return ScalarValue::Null;
            };
            let mut sum = 0i64;
            let mut has_value = false;
            for (row, weight) in &filtered_rows {
                match eval_scalar_expression(expr, row, schema) {
                    Ok(value) => {
                        if let Some(number) = scalar_to_i64(&value) {
                            sum += number * *weight;
                            has_value = true;
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %graph_id,
                            error = %err,
                            "failed to evaluate {context} sum expression"
                        );
                    }
                }
            }
            if has_value {
                scalar_from_i64(sum, agg.output_type())
            } else {
                ScalarValue::Null
            }
        }
        DbspAggregateFunction::Avg => {
            let Some(expr) = agg.expression() else {
                return ScalarValue::Null;
            };
            let mut sum = 0i64;
            let mut count = 0i64;
            for (row, weight) in &filtered_rows {
                match eval_scalar_expression(expr, row, schema) {
                    Ok(value) => {
                        if let Some(number) = scalar_to_i64(&value) {
                            sum += number * *weight;
                            count += *weight;
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %graph_id,
                            error = %err,
                            "failed to evaluate {context} avg expression"
                        );
                    }
                }
            }
            if count != 0 {
                ScalarValue::Int64(Some(sum / count))
            } else {
                ScalarValue::Null
            }
        }
        DbspAggregateFunction::Min => {
            let Some(expr) = agg.expression() else {
                return ScalarValue::Null;
            };
            let mut current: Option<ScalarValue> = None;
            for (row, _weight) in &filtered_rows {
                match eval_scalar_expression(expr, row, schema) {
                    Ok(value) => {
                        if value.is_null() {
                            continue;
                        }
                        current = Some(match current {
                            Some(existing) => match compare_scalar_values(&value, &existing) {
                                Some(std::cmp::Ordering::Less) => value,
                                Some(_) | None => existing,
                            },
                            None => value,
                        });
                    }
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %graph_id,
                            error = %err,
                            "failed to evaluate {context} min expression"
                        );
                    }
                }
            }
            current.unwrap_or(ScalarValue::Null)
        }
        DbspAggregateFunction::Max => {
            let Some(expr) = agg.expression() else {
                return ScalarValue::Null;
            };
            let mut current: Option<ScalarValue> = None;
            for (row, _weight) in &filtered_rows {
                match eval_scalar_expression(expr, row, schema) {
                    Ok(value) => {
                        if value.is_null() {
                            continue;
                        }
                        current = Some(match current {
                            Some(existing) => match compare_scalar_values(&value, &existing) {
                                Some(std::cmp::Ordering::Greater) => value,
                                Some(_) | None => existing,
                            },
                            None => value,
                        });
                    }
                    Err(err) => {
                        tracing::warn!(
                            graph_id = %graph_id,
                            error = %err,
                            "failed to evaluate {context} max expression"
                        );
                    }
                }
            }
            current.unwrap_or(ScalarValue::Null)
        }
    }
}

fn row_passes_aggregate_filter(
    agg: &DbspAggregateExpr,
    row: &[ScalarValue],
    schema: &RowSchema,
    graph_id: &str,
    context: &str,
) -> bool {
    let Some(filter) = agg.filter() else {
        return true;
    };
    match eval_expression(filter, row, schema) {
        Ok(include) => include,
        Err(err) => {
            tracing::warn!(
                graph_id = %graph_id,
                error = %err,
                "failed to evaluate {context} FILTER expression"
            );
            false
        }
    }
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
    use super::evaluate_aggregate_value;
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
}
