use super::count_eval::build_count_eval_layout;
use super::shared::{CountEvalLayout, ExpressionColumnMap, count_eval_record_batch};
use super::*;
use crate::encoding::EncodedRowScalar;
use crate::vectorized_keys::VectorizedEncodedKeyExtractor;
use anyhow::ensure;
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Column;
use datafusion::logical_expr::Expr;

type IncrementalAggregateOutputDelta = (Vec<u8>, dbsp::IncrementalAggregateRow<Vec<u8>>, i64);
type WindowIncrementalInputDelta = (dbsp::WindowIncrementalInput<Vec<u8>, Vec<u8>>, i64);
type WindowIncrementalOutputDelta = (
    dbsp::WindowIncrementalInput<Vec<u8>, Vec<u8>>,
    dbsp::IncrementalAggregateRow<dbsp::WindowKey<Vec<u8>>>,
    i64,
);
type PrekeyedIncrementalInputDelta = ((Vec<u8>, Vec<u8>), i64);
type PrekeyedIncrementalOutputDelta = (
    (Vec<u8>, Vec<u8>),
    dbsp::IncrementalAggregateRow<Vec<u8>>,
    i64,
);

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
) -> impl Fn(&[(Vec<u8>, i64)]) -> Vec<IncrementalAggregateOutputDelta> + Send + Sync + 'static {
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
) -> impl Fn(&[WindowIncrementalInputDelta]) -> Vec<WindowIncrementalOutputDelta> + Send + Sync + 'static
{
    let layout = Arc::new(build_count_eval_layout(
        &aggregates,
        input_schema.as_ref(),
        expression_columns.as_ref(),
    ));
    move |delta_values: &[WindowIncrementalInputDelta]| {
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
        delta_values: &[PrekeyedIncrementalInputDelta],
    ) -> Vec<PrekeyedIncrementalOutputDelta> {
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

pub(super) fn evaluate_incremental_aggregate_batch_row_values(
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

pub(super) fn evaluate_incremental_aggregate_arrow_row_values(
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
pub(super) fn bool_from_encoded_scalar(value: Option<&EncodedRowScalar>) -> Result<bool> {
    match value {
        Some(EncodedRowScalar::Bool(flag)) => Ok(*flag),
        None => Ok(false),
        Some(other) => Err(anyhow!("expected boolean value, found {other:?}")),
    }
}

pub(super) fn bool_from_arrow_array(array: &dyn Array, row_idx: usize) -> Result<bool> {
    if array.is_null(row_idx) {
        return Ok(false);
    }
    let values = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| anyhow!("expected Boolean aggregate filter column"))?;
    Ok(values.value(row_idx))
}

pub(super) fn encoded_scalar_from_arrow_array(
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

pub(super) fn i64_from_encoded_scalar(value: Option<&EncodedRowScalar>) -> Option<i64> {
    match value {
        Some(EncodedRowScalar::Int64(value) | EncodedRowScalar::TimestampMillis(value)) => {
            Some(*value)
        }
        _ => None,
    }
}

pub(super) fn sum_numeric_from_encoded_scalar(value: Option<&EncodedRowScalar>) -> Option<i128> {
    match value {
        Some(EncodedRowScalar::Int64(value) | EncodedRowScalar::TimestampMillis(value)) => {
            Some(i128::from(*value))
        }
        Some(EncodedRowScalar::Decimal128(value)) => Some(*value),
        _ => None,
    }
}

pub(super) fn compare_encoded_scalars(
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

pub(super) fn append_encoded_scalar(value: &EncodedRowScalar, encoded: &mut Vec<u8>) -> Result<()> {
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

pub(super) fn encode_single_encoded_scalar_key(value: &EncodedRowScalar) -> Result<Vec<u8>> {
    let mut encoded = Vec::with_capacity(13);
    encoded.extend_from_slice(&1_u32.to_le_bytes());
    append_encoded_scalar(value, &mut encoded)?;
    Ok(encoded)
}

pub(super) fn append_encoded_i64(value: i64, encoded: &mut Vec<u8>) {
    encoded.push(0x01);
    encoded.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn append_encoded_timestamp(value: i64, encoded: &mut Vec<u8>) {
    encoded.push(0x03);
    encoded.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn append_untyped_null(encoded: &mut Vec<u8>) {
    encoded.push(0x00);
}

pub(super) fn encode_window_bounds(start: i64, end: i64) -> Result<Vec<u8>> {
    let mut encoded = Vec::with_capacity(4 + 2 * 9);
    encoded.extend_from_slice(&2_u32.to_le_bytes());
    append_encoded_timestamp(start, &mut encoded);
    append_encoded_timestamp(end, &mut encoded);
    Ok(encoded)
}

pub(super) fn encode_count_values(values: &[i64]) -> Result<Vec<u8>> {
    let count = u32::try_from(values.len()).map_err(|_| anyhow!("too many columns in MV key"))?;
    let mut encoded = Vec::with_capacity(4 + (values.len() * 9));
    encoded.extend_from_slice(&count.to_le_bytes());
    for value in values {
        append_encoded_i64(*value, &mut encoded);
    }
    Ok(encoded)
}

pub(super) fn encode_incremental_aggregate_values(
    values: &[dbsp::AggregateValue],
) -> Result<Vec<u8>> {
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

pub(super) fn append_encoded_sum_like_value(
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

pub(super) fn checked_weighted_sum_delta(value: i128, weight: i64) -> Result<i128> {
    value
        .checked_mul(i128::from(weight))
        .ok_or_else(|| anyhow!("aggregate SUM overflow while applying input weight"))
}

pub(super) fn checked_sum_add(left: i128, right: i128) -> Result<i128> {
    left.checked_add(right)
        .ok_or_else(|| anyhow!("aggregate SUM overflow"))
}

pub(super) fn ensure_decimal_sum_fits_precision(value: i128, precision: u8) -> Result<()> {
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

pub(super) fn aggregate_numeric_value_type_from_dbsp_type(
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

pub(super) fn aggregate_ordered_value_type_from_dbsp_type(
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

pub(super) fn incremental_aggregate_value_from_encoded_scalar(
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

pub(super) fn direct_group_key_columns(
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

pub(super) fn direct_column_index(
    expr: &dbsp::circuit::plan::DbspExpression,
    schema: &RowSchema,
) -> Option<usize> {
    match expr.expr() {
        Expr::Alias(alias) => direct_column_index_expression(alias.expr.as_ref(), schema),
        other => direct_column_index_expression(other, schema),
    }
}

pub(super) fn direct_column_index_expression(expr: &Expr, schema: &RowSchema) -> Option<usize> {
    match expr {
        Expr::Column(column) => resolve_direct_column(schema, column),
        Expr::Alias(alias) => direct_column_index_expression(alias.expr.as_ref(), schema),
        _ => None,
    }
}

pub(super) fn resolve_direct_column(schema: &RowSchema, column: &Column) -> Option<usize> {
    let qualified = column.flat_name();
    schema
        .field_index(&qualified)
        .or_else(|| schema.field_index(&column.name))
}

pub(super) fn resolved_expression_column_index(
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

pub(super) fn expression_lookup_key(expr: &Expr) -> String {
    match expr {
        Expr::Alias(alias) => expression_lookup_key(alias.expr.as_ref()),
        other => other.to_string(),
    }
}
