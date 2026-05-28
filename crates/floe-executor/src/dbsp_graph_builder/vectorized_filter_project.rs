use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use datafusion::arrow::array::builder::BinaryDictionaryBuilder;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int64Array,
    Int64Builder, StringArray, StringBuilder, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Column;
use datafusion::common::Result as DataFusionResult;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr_fn::SimpleScalarUDF;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionImplementation, ScalarUDF, Volatility,
};
use datafusion::prelude::{SessionContext, col};
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::circuit::plan::DbspProjectExpr;
use dbsp::{DbspPredicate, RowSchema};
use regex::Regex;

use crate::delta_batch::{DeltaBatchBuffer, DeltaBatchConfig};

#[derive(Clone)]
pub(crate) struct VectorizedFilterProjectEvaluator {
    eval_input_schema: datafusion::arrow::datatypes::SchemaRef,
    eval_input_columns: Arc<[usize]>,
    predicate_expr: Option<Expr>,
    projection_exprs: Arc<Vec<(Expr, String)>>,
    mode: VectorizedFilterProjectMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VectorizedFilterProjectMode {
    FilterPassthrough,
    Project,
}

impl VectorizedFilterProjectEvaluator {
    pub(crate) fn for_filter_map(
        predicate: &DbspPredicate,
        projections: &[DbspProjectExpr],
        input_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        Self::new(
            input_schema,
            Some(predicate),
            Some(projections),
            VectorizedFilterProjectMode::Project,
        )
    }

    pub(crate) fn for_filter(
        predicate: &DbspPredicate,
        input_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        let required_columns =
            required_encoded_input_columns(Some(predicate), None, &input_schema)?;
        let input_schema = input_schema.to_arrow_schema();
        let eval_input_schema = projected_arrow_schema(&input_schema, &required_columns)?;
        Ok(Self {
            eval_input_schema,
            eval_input_columns: Arc::from(required_columns),
            predicate_expr: Some(normalize_floe_udfs(predicate.expression().expr().clone())?),
            projection_exprs: Arc::new(Vec::new()),
            mode: VectorizedFilterProjectMode::FilterPassthrough,
        })
    }

    pub(crate) fn for_map(
        projections: &[DbspProjectExpr],
        input_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        Self::new(
            input_schema,
            None,
            Some(projections),
            VectorizedFilterProjectMode::Project,
        )
    }

    fn new(
        input_schema: Arc<RowSchema>,
        predicate: Option<&DbspPredicate>,
        projections: Option<&[DbspProjectExpr]>,
        mode: VectorizedFilterProjectMode,
    ) -> Result<Self> {
        let required_columns =
            required_encoded_input_columns(predicate, projections, &input_schema)?;
        let input_schema = input_schema.to_arrow_schema();
        let eval_input_schema = projected_arrow_schema(&input_schema, &required_columns)?;
        let projection_exprs = projections
            .map(projection_exprs)
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            eval_input_schema,
            eval_input_columns: Arc::from(required_columns),
            predicate_expr: predicate
                .map(|predicate| normalize_floe_udfs(predicate.expression().expr().clone()))
                .transpose()?,
            projection_exprs,
            mode,
        })
    }

    pub(crate) async fn transform_delta_arrow(
        &self,
        graph_id: &str,
        delta_values: Arc<Vec<(Vec<u8>, i64)>>,
    ) -> Result<Vec<(Vec<u8>, i64)>> {
        if delta_values.is_empty() {
            return Ok(Vec::new());
        }

        let mut buffer = DeltaBatchBuffer::new_projected(
            Arc::clone(&self.eval_input_schema),
            Arc::clone(&self.eval_input_columns),
            false,
            DeltaBatchConfig {
                max_rows: usize::MAX,
                max_bytes: usize::MAX,
            },
        )
        .context("create vectorized filter/project input delta buffer")?;
        let mut staged_rows = Vec::with_capacity(delta_values.len());
        for (row, weight) in delta_values.iter() {
            if *weight == 0 {
                continue;
            }
            let _ = buffer
                .push(row.clone(), *weight, None)
                .with_context(|| format!("decode input row for vectorized graph '{graph_id}'"))?;
            staged_rows.push((row.clone(), *weight));
        }
        let Some(input_batch) = buffer
            .flush_manual()
            .context("flush vectorized filter/project input delta batch")?
        else {
            return Ok(Vec::new());
        };

        let ctx = SessionContext::new();
        let delta_schema = input_batch.schema();
        let table = MemTable::try_new(delta_schema, vec![vec![input_batch]])
            .context("create DataFusion delta table for vectorized filter/project")?;
        ctx.register_table("__floe_delta", Arc::new(table))
            .context("register vectorized filter/project delta table")?;
        let mut df = ctx
            .table("__floe_delta")
            .await
            .context("open vectorized filter/project delta table")?;
        if self.mode == VectorizedFilterProjectMode::FilterPassthrough {
            return self
                .filter_passthrough(df, staged_rows)
                .await
                .context("apply vectorized filter predicate");
        }
        if let Some(predicate) = self.predicate_expr.as_ref() {
            df = df
                .filter(predicate.clone())
                .context("apply vectorized filter predicate")?;
        }

        let mut select_exprs = Vec::with_capacity(self.projection_exprs.len() + 1);
        for (expr, alias) in self.projection_exprs.iter() {
            select_exprs.push(expr.clone().alias(alias));
        }
        select_exprs.push(col(WEIGHT_COLUMN_NAME));
        let batches = df
            .select(select_exprs)
            .context("apply vectorized projection")?
            .collect()
            .await
            .context("collect vectorized filter/project output")?;
        encode_arrow_delta_batches(batches, self.projection_exprs.len())
            .context("encode vectorized filter/project output delta")
    }

    async fn filter_passthrough(
        &self,
        df: datafusion::dataframe::DataFrame,
        staged_rows: Vec<(Vec<u8>, i64)>,
    ) -> Result<Vec<(Vec<u8>, i64)>> {
        let Some(predicate) = self.predicate_expr.as_ref() else {
            return consolidate_encoded_delta_batch(staged_rows);
        };
        let batches = df
            .select(vec![
                predicate.clone().alias("__floe_predicate"),
                col(WEIGHT_COLUMN_NAME),
            ])
            .context("evaluate vectorized filter predicate")?
            .collect()
            .await
            .context("collect vectorized filter predicate")?;

        let mut staged_idx = 0usize;
        let mut selected = Vec::new();
        for batch in batches {
            if batch.num_columns() != 2 {
                return Err(anyhow!(
                    "vectorized filter predicate batch has {} columns but expected predicate plus weight",
                    batch.num_columns()
                ));
            }
            let predicate_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("vectorized filter predicate was not Boolean"))?;
            let weight_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("vectorized filter weight column was not Int64"))?;
            for row_idx in 0..batch.num_rows() {
                let (encoded, _) = staged_rows.get(staged_idx).ok_or_else(|| {
                    anyhow!(
                        "vectorized filter returned more rows than the staged input delta batch"
                    )
                })?;
                staged_idx += 1;
                let selected_row =
                    !predicate_array.is_null(row_idx) && predicate_array.value(row_idx);
                let weight = weight_array.value(row_idx);
                if selected_row && weight != 0 {
                    selected.push((encoded.clone(), weight));
                }
            }
        }
        if staged_idx != staged_rows.len() {
            return Err(anyhow!(
                "vectorized filter returned {} rows for {} staged input rows",
                staged_idx,
                staged_rows.len()
            ));
        }
        consolidate_encoded_delta_batch(selected)
    }

    #[cfg(test)]
    pub(crate) fn transform_delta(
        &self,
        graph_id: &str,
        delta_values: &[(Vec<u8>, i64)],
    ) -> Result<Vec<(Vec<u8>, i64)>> {
        futures::executor::block_on(
            self.transform_delta_arrow(graph_id, Arc::new(delta_values.to_vec())),
        )
    }
}

fn projection_exprs(projections: &[DbspProjectExpr]) -> Result<Arc<Vec<(Expr, String)>>> {
    projections
        .iter()
        .map(|projection| {
            Ok((
                normalize_floe_udfs(projection.expression().expr().clone())?,
                projection.alias().to_string(),
            ))
        })
        .collect::<Result<Vec<_>>>()
        .map(Arc::new)
}

fn normalize_floe_udfs(expr: Expr) -> Result<Expr> {
    Ok(expr
        .transform(|expr| {
            if let Expr::ScalarFunction(mut scalar) = expr {
                if let Some(udf) = floe_vectorized_udf(scalar.name()) {
                    scalar.func = udf;
                    Ok(Transformed::yes(Expr::ScalarFunction(scalar)))
                } else {
                    Ok(Transformed::no(Expr::ScalarFunction(scalar)))
                }
            } else {
                Ok(Transformed::no(expr))
            }
        })
        .map_err(anyhow::Error::from)?
        .data)
}

fn floe_vectorized_udf(name: &str) -> Option<Arc<ScalarUDF>> {
    let ts = DataType::Timestamp(TimeUnit::Millisecond, None);
    let udf = match name.to_ascii_lowercase().as_str() {
        "proctime" => ScalarUDF::from(SimpleScalarUDF::new(
            "proctime",
            vec![],
            ts,
            Volatility::Volatile,
            Arc::new(
                |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
                    Ok(null_ts_value(udf_batch_len(args)))
                },
            ),
        )),
        "hour" => ScalarUDF::from(SimpleScalarUDF::new(
            "hour",
            vec![ts],
            DataType::Int64,
            Volatility::Immutable,
            hour_udf(),
        )),
        "date_format" => ScalarUDF::from(SimpleScalarUDF::new(
            "date_format",
            vec![
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Utf8,
            ],
            DataType::Utf8,
            Volatility::Immutable,
            date_format_udf(),
        )),
        "regexp_extract" => ScalarUDF::from(SimpleScalarUDF::new(
            "regexp_extract",
            vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
            DataType::Utf8,
            Volatility::Immutable,
            regexp_extract_udf(),
        )),
        "split_index" => ScalarUDF::from(SimpleScalarUDF::new(
            "split_index",
            vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
            DataType::Utf8,
            Volatility::Immutable,
            split_index_udf(),
        )),
        "count_char" => ScalarUDF::from(SimpleScalarUDF::new(
            "count_char",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Int64,
            Volatility::Immutable,
            count_char_udf(),
        )),
        _ => return None,
    };
    Some(Arc::new(udf))
}

fn udf_batch_len(args: &[ColumnarValue]) -> usize {
    args.iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.len()),
            ColumnarValue::Scalar(_) => None,
        })
        .unwrap_or(1)
}

fn null_ts_value(len: usize) -> ColumnarValue {
    let array: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![None::<i64>; len]));
    ColumnarValue::Array(array)
}

fn null_utf8_value(len: usize) -> ColumnarValue {
    let array: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; len]));
    ColumnarValue::Array(array)
}

fn null_i64_value(len: usize) -> ColumnarValue {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![None::<i64>; len]));
    ColumnarValue::Array(array)
}

fn date_format_udf() -> ScalarFunctionImplementation {
    Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let ts = args
                .first()
                .cloned()
                .unwrap_or_else(|| null_ts_value(len))
                .into_array(len)?;
            let fmt = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| null_utf8_value(len))
                .into_array(len)?;
            let (Some(ts), Some(fmt)) = (
                ts.as_any().downcast_ref::<TimestampMillisecondArray>(),
                fmt.as_any().downcast_ref::<StringArray>(),
            ) else {
                return Ok(null_utf8_value(len));
            };

            let mut out = StringBuilder::new();
            for row_idx in 0..len {
                if ts.is_null(row_idx) || fmt.is_null(row_idx) {
                    out.append_null();
                    continue;
                }
                let Some(dt) = chrono::DateTime::<Utc>::from_timestamp_millis(ts.value(row_idx))
                else {
                    out.append_null();
                    continue;
                };
                let pattern = fmt
                    .value(row_idx)
                    .replace("yyyy", "%Y")
                    .replace("MM", "%m")
                    .replace("dd", "%d")
                    .replace("HH", "%H")
                    .replace("mm", "%M")
                    .replace("ss", "%S");
                out.append_value(dt.format(&pattern).to_string());
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish())))
        },
    )
}

fn regexp_extract_udf() -> ScalarFunctionImplementation {
    Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let text = args
                .first()
                .cloned()
                .unwrap_or_else(|| null_utf8_value(len))
                .into_array(len)?;
            let pattern = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| null_utf8_value(len))
                .into_array(len)?;
            let group = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| null_i64_value(len))
                .into_array(len)?;
            let (Some(text), Some(pattern), Some(group)) = (
                text.as_any().downcast_ref::<StringArray>(),
                pattern.as_any().downcast_ref::<StringArray>(),
                group.as_any().downcast_ref::<Int64Array>(),
            ) else {
                return Ok(null_utf8_value(len));
            };

            let mut cache: HashMap<String, Option<Regex>> = HashMap::new();
            let mut out = StringBuilder::new();
            for row_idx in 0..len {
                if text.is_null(row_idx) || pattern.is_null(row_idx) || group.is_null(row_idx) {
                    out.append_null();
                    continue;
                }
                let group_idx = group.value(row_idx);
                if group_idx < 0 {
                    out.append_null();
                    continue;
                }
                let pattern_text = pattern.value(row_idx);
                let regex = cache
                    .entry(pattern_text.to_string())
                    .or_insert_with(|| Regex::new(pattern_text).ok());
                let Some(regex) = regex.as_ref() else {
                    out.append_null();
                    continue;
                };
                let Some(captures) = regex.captures(text.value(row_idx)) else {
                    out.append_null();
                    continue;
                };
                let Some(matched) = captures.get(group_idx as usize) else {
                    out.append_null();
                    continue;
                };
                out.append_value(matched.as_str());
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish())))
        },
    )
}

fn split_index_udf() -> ScalarFunctionImplementation {
    Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let text = args
                .first()
                .cloned()
                .unwrap_or_else(|| null_utf8_value(len))
                .into_array(len)?;
            let delimiter = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| null_utf8_value(len))
                .into_array(len)?;
            let index = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| null_i64_value(len))
                .into_array(len)?;
            let (Some(text), Some(delimiter), Some(index)) = (
                text.as_any().downcast_ref::<StringArray>(),
                delimiter.as_any().downcast_ref::<StringArray>(),
                index.as_any().downcast_ref::<Int64Array>(),
            ) else {
                return Ok(null_utf8_value(len));
            };

            let mut out = StringBuilder::new();
            for row_idx in 0..len {
                if text.is_null(row_idx) || delimiter.is_null(row_idx) || index.is_null(row_idx) {
                    out.append_null();
                    continue;
                }
                match split_index_value(
                    text.value(row_idx),
                    delimiter.value(row_idx),
                    index.value(row_idx),
                ) {
                    Some(value) => out.append_value(value),
                    None => out.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish())))
        },
    )
}

fn hour_udf() -> ScalarFunctionImplementation {
    Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let ts = args
                .first()
                .cloned()
                .unwrap_or_else(|| null_ts_value(len))
                .into_array(len)?;
            let Some(ts) = ts.as_any().downcast_ref::<TimestampMillisecondArray>() else {
                return Ok(null_i64_value(len));
            };
            let mut out = Int64Builder::with_capacity(len);
            for row_idx in 0..len {
                if ts.is_null(row_idx) {
                    out.append_null();
                } else {
                    let hour = ts.value(row_idx).div_euclid(3_600_000).rem_euclid(24);
                    out.append_value(hour);
                }
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish())))
        },
    )
}

fn count_char_udf() -> ScalarFunctionImplementation {
    Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let text = args
                .first()
                .cloned()
                .unwrap_or_else(|| null_utf8_value(len))
                .into_array(len)?;
            let needle = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| null_utf8_value(len))
                .into_array(len)?;
            let (Some(text), Some(needle)) = (
                text.as_any().downcast_ref::<StringArray>(),
                needle.as_any().downcast_ref::<StringArray>(),
            ) else {
                return Ok(null_i64_value(len));
            };

            let mut out = Int64Builder::with_capacity(len);
            for row_idx in 0..len {
                if text.is_null(row_idx) || needle.is_null(row_idx) {
                    out.append_null();
                    continue;
                }
                let token = needle.value(row_idx);
                let count = if token.is_empty() {
                    0
                } else {
                    i64::try_from(text.value(row_idx).matches(token).count()).unwrap_or(i64::MAX)
                };
                out.append_value(count);
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish())))
        },
    )
}

fn split_index_value(text: &str, delimiter: &str, index: i64) -> Option<String> {
    if index < 0 {
        return None;
    }
    if delimiter.is_empty() {
        return text.chars().nth(index as usize).map(|ch| ch.to_string());
    }
    text.split(delimiter)
        .nth(index as usize)
        .map(|part| part.to_string())
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
                        "vectorized input column {idx} is out of bounds for schema width {}",
                        input_schema.fields().len()
                    )
                })
        })
        .collect::<Result<Vec<Field>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn encode_arrow_delta_batches(
    batches: Vec<RecordBatch>,
    payload_width: usize,
) -> Result<Vec<(Vec<u8>, i64)>> {
    let mut staged = Vec::new();
    for batch in batches {
        if batch.num_columns() != payload_width + 1 {
            return Err(anyhow!(
                "vectorized output batch has {} columns but expected {} payload columns plus weight",
                batch.num_columns(),
                payload_width
            ));
        }
        let weight_array = batch
            .column(payload_width)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow!("vectorized output weight column was not Int64"))?;
        for row_idx in 0..batch.num_rows() {
            let weight = weight_array.value(row_idx);
            if weight == 0 {
                continue;
            }
            let row = encode_arrow_payload_row(&batch, payload_width, row_idx)?;
            staged.push((row, weight));
        }
    }
    consolidate_encoded_delta_batch(staged)
}

fn encode_arrow_payload_row(
    batch: &RecordBatch,
    payload_width: usize,
    row_idx: usize,
) -> Result<Vec<u8>> {
    let count = u32::try_from(payload_width).context("too many vectorized output columns")?;
    let mut encoded = Vec::with_capacity(4 + payload_width.saturating_mul(16));
    encoded.extend_from_slice(&count.to_le_bytes());
    for column_idx in 0..payload_width {
        append_arrow_value(batch.column(column_idx).as_ref(), row_idx, &mut encoded)?;
    }
    Ok(encoded)
}

fn append_arrow_value(array: &dyn Array, row_idx: usize, encoded: &mut Vec<u8>) -> Result<()> {
    match array.data_type() {
        DataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("expected Int64 array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x05);
            } else {
                encoded.push(0x01);
                encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
            }
        }
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("expected Utf8 array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x06);
            } else {
                encoded.push(0x02);
                let bytes = values.value(row_idx).as_bytes();
                let len = u32::try_from(bytes.len()).context("utf8 value too large for MV key")?;
                encoded.extend_from_slice(&len.to_le_bytes());
                encoded.extend_from_slice(bytes);
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| anyhow!("expected TimestampMillisecond array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x07);
            } else {
                encoded.push(0x03);
                encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
            }
        }
        DataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("expected Boolean array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x08);
            } else {
                encoded.push(0x04);
                encoded.push(u8::from(values.value(row_idx)));
            }
        }
        DataType::Date32 => {
            let values = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| anyhow!("expected Date32 array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x0A);
            } else {
                encoded.push(0x09);
                encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
            }
        }
        DataType::Decimal128(_, _) => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| anyhow!("expected Decimal128 array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x0C);
            } else {
                encoded.push(0x0B);
                encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
            }
        }
        other => {
            return Err(anyhow!(
                "unsupported vectorized output Arrow type for encoded boundary: {other:?}"
            ));
        }
    }
    Ok(())
}

pub(crate) fn required_encoded_input_columns(
    predicate: Option<&DbspPredicate>,
    projections: Option<&[DbspProjectExpr]>,
    input_schema: &RowSchema,
) -> Result<Vec<usize>> {
    let mut columns = BTreeSet::new();
    if let Some(predicate) = predicate {
        add_expr_input_columns(predicate.expression().expr(), input_schema, &mut columns)?;
    }
    if let Some(projections) = projections {
        if let Some(indices) = column_projection_indices(projections, input_schema) {
            columns.extend(indices);
        } else {
            for projection in projections {
                add_expr_input_columns(projection.expression().expr(), input_schema, &mut columns)?;
            }
        }
    }
    Ok(columns.into_iter().collect())
}

fn column_projection_indices(
    projections: &[DbspProjectExpr],
    input_schema: &RowSchema,
) -> Option<Vec<usize>> {
    projections
        .iter()
        .map(|projection| {
            let column = match projection.expression().expr() {
                Expr::Column(column) => column,
                Expr::Alias(alias) => match alias.expr.as_ref() {
                    Expr::Column(column) => column,
                    _ => return None,
                },
                _ => return None,
            };
            resolve_input_schema_column_index(input_schema, column)
        })
        .collect::<Option<Vec<_>>>()
}

fn add_expr_input_columns(
    expr: &Expr,
    input_schema: &RowSchema,
    columns: &mut BTreeSet<usize>,
) -> Result<()> {
    for column in expr.column_refs() {
        let index = resolve_input_schema_column_index(input_schema, column).ok_or_else(|| {
            anyhow!(
                "column '{}' was not found in vectorized input schema",
                column.flat_name()
            )
        })?;
        columns.insert(index);
    }
    Ok(())
}

fn resolve_input_schema_column_index(input_schema: &RowSchema, column: &Column) -> Option<usize> {
    let qualified = column.flat_name();
    input_schema
        .field_index(&qualified)
        .or_else(|| input_schema.field_index(column.name.as_str()))
}

fn consolidate_encoded_delta_batch(
    delta_values: Vec<(Vec<u8>, i64)>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    if delta_values.is_empty() {
        return Ok(Vec::new());
    }

    let mut builder = BinaryDictionaryBuilder::<Int32Type>::new();
    let mut deltas_by_dictionary_index: Vec<i64> = Vec::new();

    for (encoded, diff) in delta_values {
        if diff == 0 {
            continue;
        }
        let dict_index = builder
            .append(encoded.as_slice())
            .context("append encoded row into Arrow batch dictionary")?;
        let dict_index =
            usize::try_from(dict_index).context("Arrow dictionary index must be non-negative")?;
        if dict_index >= deltas_by_dictionary_index.len() {
            deltas_by_dictionary_index.resize(dict_index + 1, 0);
        }
        deltas_by_dictionary_index[dict_index] += diff;
    }

    if deltas_by_dictionary_index.is_empty() {
        return Ok(Vec::new());
    }

    let dictionary = builder.finish();
    let values = dictionary
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| anyhow!("vectorized dictionary values were not binary"))?;

    let mut output = Vec::with_capacity(deltas_by_dictionary_index.len());
    for (idx, diff) in deltas_by_dictionary_index.into_iter().enumerate() {
        if diff == 0 {
            continue;
        }
        output.push((values.value(idx).to_vec(), diff));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BooleanArray, Int64Array};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::prelude::lit;
    use dbsp::circuit::{DbspScalarType, Field as DbspField};
    use dbsp::{DbspProjectNode, ProjectItem};

    fn pruned_filter_schema() -> Arc<RowSchema> {
        RowSchema::try_new(vec![
            DbspField::new("id", DbspScalarType::Int64, false),
            DbspField::new("url", DbspScalarType::Utf8, false),
            DbspField::new("price", DbspScalarType::Int64, false),
        ])
        .expect("schema")
    }

    fn pruned_row_with_elided_url(id: i64, price: i64) -> Vec<u8> {
        let mut row = Vec::new();
        row.extend_from_slice(&(3_u32).to_le_bytes());
        row.push(0x01);
        row.extend_from_slice(&id.to_le_bytes());
        row.push(0x06);
        row.push(0x01);
        row.extend_from_slice(&price.to_le_bytes());
        row
    }

    #[test]
    fn consolidate_skips_zero_weight_and_sums_duplicates() {
        let row_a = vec![1, 2, 3];
        let row_b = vec![4, 5, 6];

        let output = consolidate_encoded_delta_batch(vec![
            (row_a.clone(), 1),
            (row_a, -1),
            (row_b.clone(), 2),
            (row_b.clone(), 0),
            (row_b.clone(), 3),
        ])
        .expect("consolidate");

        assert_eq!(output, vec![(row_b, 5)]);
    }

    #[test]
    fn encodes_arrow_output_batch_with_payload_and_weight() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Boolean, true),
                Field::new(WEIGHT_COLUMN_NAME, DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![Some(10), Some(10), Some(20)])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(true),
                    Some(false),
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 0])),
            ],
        )
        .expect("batch");

        let output = encode_arrow_delta_batches(vec![batch], 2).expect("encode");
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].1, 3);
    }

    #[test]
    fn filter_passthrough_decodes_only_predicate_columns() {
        let schema = pruned_filter_schema();
        let predicate = DbspPredicate::try_new(col("price").gt(lit(10_i64)), Arc::clone(&schema))
            .expect("predicate");
        let evaluator =
            VectorizedFilterProjectEvaluator::for_filter(&predicate, schema).expect("evaluator");
        let selected = pruned_row_with_elided_url(1, 15);
        let rejected = pruned_row_with_elided_url(2, 5);

        let output = evaluator
            .transform_delta("pruned-filter", &[(selected.clone(), 1), (rejected, 1)])
            .expect("transform");

        assert_eq!(output, vec![(selected, 1)]);
    }

    #[test]
    fn projection_decodes_only_referenced_columns() {
        let schema = pruned_filter_schema();
        let project = DbspProjectNode::try_new(
            Arc::clone(&schema),
            vec![ProjectItem {
                expr: col("price") + lit(1_i64),
                alias: Some("next_price".to_string()),
            }],
        )
        .expect("project");
        let evaluator = VectorizedFilterProjectEvaluator::for_map(project.expressions(), schema)
            .expect("evaluator");
        let row = pruned_row_with_elided_url(1, 15);

        let output = evaluator
            .transform_delta("pruned-project", &[(row, 1)])
            .expect("transform");
        let values = crate::encoding::extract_encoded_row_scalars(&output[0].0, &[0])
            .expect("decode output");

        assert_eq!(
            values,
            vec![Some(crate::encoding::EncodedRowScalar::Int64(16))]
        );
        assert_eq!(output[0].1, 1);
    }
}
