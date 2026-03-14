use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use datafusion::arrow::array::builder::BinaryDictionaryBuilder;
use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, BooleanArray};
use datafusion::arrow::datatypes::{DataType, Int32Type, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;
use dbsp::circuit::plan::DbspProjectExpr;
use dbsp::{DbspPredicate, RowSchema};

use crate::encoding::encode_projected_row_key;

#[derive(Clone)]
pub(crate) struct VectorizedFilterProjectEvaluator {
    input_schema: datafusion::arrow::datatypes::SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    projection_plan: ProjectionPlan,
    decoded_input_slots: Arc<Vec<Option<usize>>>,
    decoded_input_count: usize,
}

impl VectorizedFilterProjectEvaluator {
    pub(crate) fn for_filter_map(
        predicate: &DbspPredicate,
        projections: &[DbspProjectExpr],
        input_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        let column_projection = column_projection_indices(projections, input_schema.as_ref());
        let decoded_input_columns = required_input_columns(
            Some(predicate),
            Some(projections),
            input_schema.as_ref(),
            column_projection.is_some(),
        )?;
        let decoded_input_count = decoded_input_columns.len();
        let decoded_input_slots = Arc::new(build_decoded_input_slots(
            input_schema.len(),
            &decoded_input_columns,
        ));
        let input_schema = input_schema.to_arrow_schema();
        let df_schema = DFSchema::try_from(input_schema.as_ref().clone())
            .context("build DataFusion schema for vectorized filter_map")?;
        let ctx = SessionContext::new();
        let predicate = ctx
            .create_physical_expr(predicate.expression().expr().clone(), &df_schema)
            .context("compile vectorized predicate expression")?;
        let projection_plan = if let Some(indices) = column_projection {
            ProjectionPlan::column_indices(indices, input_schema.fields().len())
        } else {
            let projections = projections
                .iter()
                .map(|expr| {
                    ctx.create_physical_expr(expr.expression().expr().clone(), &df_schema)
                        .context("compile vectorized projection expression")
                })
                .collect::<Result<Vec<_>>>()?;
            ProjectionPlan::Physical(Arc::new(projections))
        };
        Ok(Self {
            input_schema,
            predicate: Some(predicate),
            projection_plan,
            decoded_input_slots,
            decoded_input_count,
        })
    }

    pub(crate) fn for_filter(
        predicate: &DbspPredicate,
        input_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        let projections = (0..input_schema.len()).collect::<Vec<_>>();
        let decoded_input_columns =
            required_input_columns(Some(predicate), None, input_schema.as_ref(), true)?;
        let decoded_input_count = decoded_input_columns.len();
        let decoded_input_slots = Arc::new(build_decoded_input_slots(
            input_schema.len(),
            &decoded_input_columns,
        ));
        let input_schema = input_schema.to_arrow_schema();
        let input_width = input_schema.fields().len();
        let df_schema = DFSchema::try_from(input_schema.as_ref().clone())
            .context("build DataFusion schema for vectorized filter")?;
        let ctx = SessionContext::new();
        let predicate = ctx
            .create_physical_expr(predicate.expression().expr().clone(), &df_schema)
            .context("compile vectorized filter predicate expression")?;
        Ok(Self {
            input_schema,
            predicate: Some(predicate),
            projection_plan: ProjectionPlan::column_indices(projections, input_width),
            decoded_input_slots,
            decoded_input_count,
        })
    }

    pub(crate) fn for_map(
        projections: &[DbspProjectExpr],
        input_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        let column_projection = column_projection_indices(projections, input_schema.as_ref());
        let decoded_input_columns = required_input_columns(
            None,
            Some(projections),
            input_schema.as_ref(),
            column_projection.is_some(),
        )?;
        let decoded_input_count = decoded_input_columns.len();
        let decoded_input_slots = Arc::new(build_decoded_input_slots(
            input_schema.len(),
            &decoded_input_columns,
        ));
        let input_schema = input_schema.to_arrow_schema();
        let df_schema = DFSchema::try_from(input_schema.as_ref().clone())
            .context("build DataFusion schema for vectorized map")?;
        let ctx = SessionContext::new();
        let projection_plan = if let Some(indices) = column_projection {
            ProjectionPlan::column_indices(indices, input_schema.fields().len())
        } else {
            let projections = projections
                .iter()
                .map(|expr| {
                    ctx.create_physical_expr(expr.expression().expr().clone(), &df_schema)
                        .context("compile vectorized map projection expression")
                })
                .collect::<Result<Vec<_>>>()?;
            ProjectionPlan::Physical(Arc::new(projections))
        };
        Ok(Self {
            input_schema,
            predicate: None,
            projection_plan,
            decoded_input_slots,
            decoded_input_count,
        })
    }

    pub(crate) fn transform_delta(
        &self,
        graph_id: &str,
        delta_values: Vec<(Vec<u8>, i64)>,
    ) -> Result<Vec<(Vec<u8>, i64)>> {
        if delta_values.is_empty() {
            return Ok(Vec::new());
        }
        let identity_projection = self
            .projection_plan
            .is_identity(self.input_schema.fields().len());
        if self.predicate.is_none() && identity_projection {
            return consolidate_encoded_delta_batch(delta_values);
        }

        let mut prepared = self.prepare_input(graph_id, delta_values)?;
        if prepared.encoded_rows.is_empty() {
            return Ok(Vec::new());
        }
        let selected = if let Some(batch) = prepared.batch.as_ref() {
            self.selected_indices(batch)?
        } else {
            (0..prepared.encoded_rows.len()).collect()
        };

        match &self.projection_plan {
            ProjectionPlan::ColumnIndices { indices, .. } => {
                let mut staged = Vec::with_capacity(selected.len());
                if identity_projection {
                    for idx in selected {
                        let diff = prepared.weights.get(idx).copied().unwrap_or(0);
                        if diff == 0 {
                            continue;
                        }
                        let Some(encoded) = prepared.encoded_rows.get(idx).cloned() else {
                            continue;
                        };
                        staged.push((encoded, diff));
                    }
                    return consolidate_encoded_delta_batch(staged);
                }
                let Some(projected_ranges) = prepared.projected_ranges.as_ref() else {
                    return Ok(Vec::new());
                };
                for idx in selected {
                    let diff = prepared.weights.get(idx).copied().unwrap_or(0);
                    if diff == 0 {
                        continue;
                    }
                    let Some(encoded) = prepared.encoded_rows.get(idx) else {
                        continue;
                    };
                    let Some(ranges) = projected_ranges.get(idx) else {
                        continue;
                    };
                    let encoded = project_encoded_row(encoded, ranges, indices.len())?;
                    staged.push((encoded, diff));
                }
                consolidate_encoded_delta_batch(staged)
            }
            ProjectionPlan::Physical(projections) => {
                let batch = prepared.batch.take().ok_or_else(|| {
                    anyhow!("vectorized projection batch was unexpectedly missing")
                })?;
                let projection_arrays = projections
                    .iter()
                    .enumerate()
                    .map(|(idx, expr)| {
                        expr.evaluate(&batch)
                            .with_context(|| {
                                format!("evaluate vectorized projection column {idx}")
                            })?
                            .into_array(batch.num_rows())
                            .with_context(|| {
                                format!("materialize vectorized projection column {idx}")
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let mut staged = Vec::with_capacity(selected.len());
                for idx in selected {
                    let diff = prepared.weights.get(idx).copied().unwrap_or(0);
                    if diff == 0 {
                        continue;
                    }
                    let mut projected_row = Vec::with_capacity(projection_arrays.len());
                    for array in &projection_arrays {
                        projected_row.push(ScalarValue::try_from_array(array, idx)?);
                    }
                    let encoded = encode_projected_row_key(&projected_row)?;
                    staged.push((encoded, diff));
                }
                consolidate_encoded_delta_batch(staged)
            }
        }
    }

    fn prepare_input(
        &self,
        graph_id: &str,
        delta_values: Vec<(Vec<u8>, i64)>,
    ) -> Result<PreparedEncodedInput> {
        let needs_batch = self.predicate.is_some() || self.projection_plan.requires_batch();
        let capture_projection_ranges = self
            .projection_plan
            .needs_projection_ranges(self.input_schema.fields().len());
        let mut decoded_columns =
            vec![Vec::with_capacity(delta_values.len()); self.decoded_input_count];
        let mut encoded_rows = Vec::with_capacity(delta_values.len());
        let mut weights = Vec::with_capacity(delta_values.len());
        let mut projected_ranges =
            capture_projection_ranges.then(|| Vec::with_capacity(delta_values.len()));
        for (encoded, weight) in delta_values {
            if weight == 0 {
                continue;
            }
            match self.decode_row(&encoded, capture_projection_ranges) {
                Ok(decoded) => {
                    for (slot, value) in decoded.decoded_values.into_iter().enumerate() {
                        decoded_columns[slot].push(value);
                    }
                    if let Some(all_ranges) = projected_ranges.as_mut()
                        && let Some(ranges) = decoded.projected_ranges
                    {
                        all_ranges.push(ranges);
                    }
                    encoded_rows.push(encoded);
                    weights.push(weight);
                }
                Err(err) => {
                    tracing::warn!(
                        graph_id = %graph_id,
                        error = %err,
                        "failed to decode vectorized filter_map row"
                    );
                }
            }
        }
        let batch = if needs_batch && !encoded_rows.is_empty() {
            Some(build_sparse_input_batch(
                &self.input_schema,
                self.decoded_input_slots.as_ref(),
                decoded_columns,
                encoded_rows.len(),
            )?)
        } else {
            None
        };
        Ok(PreparedEncodedInput {
            batch,
            encoded_rows,
            weights,
            projected_ranges,
        })
    }

    fn decode_row(
        &self,
        encoded: &[u8],
        capture_projection_ranges: bool,
    ) -> Result<DecodedEncodedRow> {
        if encoded.len() < 4 {
            return Err(anyhow!("encoded key too short"));
        }
        let input_width = self.input_schema.fields().len();
        let count = u32::from_le_bytes(encoded[0..4].try_into().unwrap()) as usize;
        if count != input_width {
            return Err(anyhow!(
                "encoded row has {count} columns but schema has {input_width}"
            ));
        }
        let mut cursor = 4usize;
        let mut decoded_values = vec![ScalarValue::Null; self.decoded_input_count];
        let mut projected_ranges = if capture_projection_ranges {
            Some(vec![0..0; self.projection_plan.output_width()])
        } else {
            None
        };
        let projection_positions = self.projection_plan.output_positions_by_input();
        for input_idx in 0..count {
            let start = cursor;
            let end = encoded_field_end(encoded, start)?;
            if let Some(slot) = self.decoded_input_slots[input_idx] {
                decoded_values[slot] = decode_encoded_field(&encoded[start..end])?;
            }
            if let (Some(ranges), Some(positions)) =
                (projected_ranges.as_mut(), projection_positions)
            {
                for output_idx in &positions[input_idx] {
                    ranges[*output_idx] = start..end;
                }
            }
            cursor = end;
        }
        if cursor != encoded.len() {
            return Err(anyhow!("encoded row had trailing bytes"));
        }
        if let Some(ranges) = projected_ranges.as_ref()
            && ranges.iter().any(Range::is_empty)
        {
            return Err(anyhow!(
                "projected encoded row was missing one or more columns"
            ));
        }
        Ok(DecodedEncodedRow {
            decoded_values,
            projected_ranges,
        })
    }

    fn selected_indices(&self, batch: &RecordBatch) -> Result<Vec<usize>> {
        let mut selected = Vec::with_capacity(batch.num_rows());
        if let Some(predicate) = &self.predicate {
            let predicate = predicate
                .evaluate(batch)
                .context("evaluate vectorized filter_map predicate")?
                .into_array(batch.num_rows())
                .context("materialize vectorized predicate result")?;
            let bool_array = predicate
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("vectorized predicate did not evaluate to boolean"))?;
            for idx in 0..bool_array.len() {
                if bool_array.is_valid(idx) && bool_array.value(idx) {
                    selected.push(idx);
                }
            }
        } else {
            for idx in 0..batch.num_rows() {
                selected.push(idx);
            }
        }
        Ok(selected)
    }
}

#[derive(Clone)]
enum ProjectionPlan {
    ColumnIndices {
        indices: Arc<Vec<usize>>,
        output_positions_by_input: Arc<Vec<Vec<usize>>>,
    },
    Physical(Arc<Vec<Arc<dyn PhysicalExpr>>>),
}

impl ProjectionPlan {
    fn column_indices(indices: Vec<usize>, input_width: usize) -> Self {
        let mut output_positions_by_input = vec![Vec::new(); input_width];
        for (output_idx, input_idx) in indices.iter().copied().enumerate() {
            output_positions_by_input[input_idx].push(output_idx);
        }
        Self::ColumnIndices {
            indices: Arc::new(indices),
            output_positions_by_input: Arc::new(output_positions_by_input),
        }
    }

    fn is_identity(&self, input_width: usize) -> bool {
        match self {
            Self::ColumnIndices { indices, .. } => {
                indices.len() == input_width
                    && indices.iter().enumerate().all(|(idx, col)| idx == *col)
            }
            Self::Physical(_) => false,
        }
    }

    fn output_width(&self) -> usize {
        match self {
            Self::ColumnIndices { indices, .. } => indices.len(),
            Self::Physical(projections) => projections.len(),
        }
    }

    fn output_positions_by_input(&self) -> Option<&Vec<Vec<usize>>> {
        match self {
            Self::ColumnIndices {
                output_positions_by_input,
                ..
            } => Some(output_positions_by_input.as_ref()),
            Self::Physical(_) => None,
        }
    }

    fn needs_projection_ranges(&self, input_width: usize) -> bool {
        matches!(self, Self::ColumnIndices { .. }) && !self.is_identity(input_width)
    }

    fn requires_batch(&self) -> bool {
        matches!(self, Self::Physical(_))
    }
}

fn column_projection_indices(
    projections: &[DbspProjectExpr],
    input_schema: &RowSchema,
) -> Option<Vec<usize>> {
    projections
        .iter()
        .map(|projection| {
            let column_name = match projection.expression().expr() {
                datafusion::logical_expr::Expr::Column(column) => column.name.as_str(),
                datafusion::logical_expr::Expr::Alias(alias) => match alias.expr.as_ref() {
                    datafusion::logical_expr::Expr::Column(column) => column.name.as_str(),
                    _ => return None,
                },
                _ => return None,
            };
            input_schema.field_index(column_name)
        })
        .collect::<Option<Vec<_>>>()
}

fn required_input_columns(
    predicate: Option<&DbspPredicate>,
    projections: Option<&[DbspProjectExpr]>,
    input_schema: &RowSchema,
    projection_is_columnar: bool,
) -> Result<Vec<usize>> {
    let mut columns = BTreeSet::new();
    if let Some(predicate) = predicate {
        add_expr_input_columns(predicate.expression().expr(), input_schema, &mut columns)?;
    }
    if !projection_is_columnar && let Some(projections) = projections {
        for projection in projections {
            add_expr_input_columns(projection.expression().expr(), input_schema, &mut columns)?;
        }
    }
    Ok(columns.into_iter().collect())
}

fn add_expr_input_columns(
    expr: &Expr,
    input_schema: &RowSchema,
    columns: &mut BTreeSet<usize>,
) -> Result<()> {
    for column in expr.column_refs() {
        let index = input_schema
            .field_index(column.name.as_str())
            .ok_or_else(|| {
                anyhow!(
                    "column '{}' was not found in vectorized input schema",
                    column.name
                )
            })?;
        columns.insert(index);
    }
    Ok(())
}

fn build_decoded_input_slots(input_width: usize, required_columns: &[usize]) -> Vec<Option<usize>> {
    let mut slots = vec![None; input_width];
    for (slot, column_idx) in required_columns.iter().copied().enumerate() {
        slots[column_idx] = Some(slot);
    }
    slots
}

fn build_sparse_input_batch(
    schema: &datafusion::arrow::datatypes::SchemaRef,
    decoded_input_slots: &[Option<usize>],
    mut decoded_columns: Vec<Vec<ScalarValue>>,
    row_count: usize,
) -> Result<RecordBatch> {
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            if let Some(slot) = decoded_input_slots[idx] {
                ScalarValue::iter_to_array(std::mem::take(&mut decoded_columns[slot]))
                    .with_context(|| format!("build vectorized input column {idx}"))
            } else {
                placeholder_scalar(field.data_type())?
                    .to_array_of_size(row_count)
                    .context("build placeholder vectorized input column")
            }
        })
        .collect::<Result<_>>()?;
    RecordBatch::try_new(Arc::clone(schema), arrays).context("build vectorized input batch")
}

fn placeholder_scalar(data_type: &DataType) -> Result<ScalarValue> {
    match data_type {
        DataType::Int64 => Ok(ScalarValue::Int64(Some(0))),
        DataType::Utf8 => Ok(ScalarValue::Utf8(Some(String::new()))),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Ok(ScalarValue::TimestampMillisecond(Some(0), None))
        }
        DataType::Boolean => Ok(ScalarValue::Boolean(Some(false))),
        DataType::Null => Ok(ScalarValue::Null),
        other => Err(anyhow!(
            "unsupported placeholder type in vectorized input batch: {other:?}"
        )),
    }
}

fn encoded_field_end(bytes: &[u8], start: usize) -> Result<usize> {
    let tag = *bytes
        .get(start)
        .ok_or_else(|| anyhow!("unexpected end of key while decoding tag"))?;
    let payload = start + 1;
    match tag {
        0x00 | 0x05 | 0x06 | 0x07 | 0x08 => Ok(payload),
        0x01 | 0x03 => {
            let end = payload + 8;
            bytes
                .get(payload..end)
                .ok_or_else(|| anyhow!("truncated fixed-width scalar"))?;
            Ok(end)
        }
        0x02 => {
            let len_bytes = bytes
                .get(payload..payload + 4)
                .ok_or_else(|| anyhow!("truncated string length"))?;
            let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            let end = payload + 4 + len;
            bytes
                .get(payload + 4..end)
                .ok_or_else(|| anyhow!("truncated string payload"))?;
            Ok(end)
        }
        0x04 => {
            let end = payload + 1;
            bytes
                .get(payload..end)
                .ok_or_else(|| anyhow!("missing boolean payload"))?;
            Ok(end)
        }
        _ => Err(anyhow!("unknown column tag {tag:#x} in MV key")),
    }
}

fn decode_encoded_field(field: &[u8]) -> Result<ScalarValue> {
    let tag = *field
        .first()
        .ok_or_else(|| anyhow!("encoded field must contain a tag"))?;
    match tag {
        0x00 => Ok(ScalarValue::Null),
        0x01 => {
            let chunk = field.get(1..9).ok_or_else(|| anyhow!("truncated int64"))?;
            Ok(ScalarValue::Int64(Some(i64::from_le_bytes(
                chunk.try_into().unwrap(),
            ))))
        }
        0x02 => {
            let len_bytes = field
                .get(1..5)
                .ok_or_else(|| anyhow!("truncated string length"))?;
            let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            let chunk = field
                .get(5..5 + len)
                .ok_or_else(|| anyhow!("truncated string payload"))?;
            let text =
                std::str::from_utf8(chunk).map_err(|err| anyhow!("utf8 decode error: {err}"))?;
            Ok(ScalarValue::Utf8(Some(text.to_string())))
        }
        0x03 => {
            let chunk = field
                .get(1..9)
                .ok_or_else(|| anyhow!("truncated timestamp"))?;
            Ok(ScalarValue::TimestampMillisecond(
                Some(i64::from_le_bytes(chunk.try_into().unwrap())),
                None,
            ))
        }
        0x04 => {
            let flag = *field
                .get(1)
                .ok_or_else(|| anyhow!("missing boolean payload"))?;
            Ok(ScalarValue::Boolean(Some(flag != 0)))
        }
        0x05 => Ok(ScalarValue::Int64(None)),
        0x06 => Ok(ScalarValue::Utf8(None)),
        0x07 => Ok(ScalarValue::TimestampMillisecond(None, None)),
        0x08 => Ok(ScalarValue::Boolean(None)),
        _ => Err(anyhow!("unknown column tag {tag:#x} in MV key")),
    }
}

fn project_encoded_row(encoded: &[u8], ranges: &[Range<usize>], width: usize) -> Result<Vec<u8>> {
    let count = u32::try_from(width).context("too many columns in projected encoded row")?;
    let payload_len = ranges.iter().map(|range| range.len()).sum::<usize>();
    let mut projected = Vec::with_capacity(4 + payload_len);
    projected.extend_from_slice(&count.to_le_bytes());
    for range in ranges {
        projected.extend_from_slice(
            encoded
                .get(range.clone())
                .ok_or_else(|| anyhow!("projected encoded row slice was out of bounds"))?,
        );
    }
    Ok(projected)
}

struct PreparedEncodedInput {
    batch: Option<RecordBatch>,
    encoded_rows: Vec<Vec<u8>>,
    weights: Vec<i64>,
    projected_ranges: Option<Vec<Vec<Range<usize>>>>,
}

struct DecodedEncodedRow {
    decoded_values: Vec<ScalarValue>,
    projected_ranges: Option<Vec<Range<usize>>>,
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
