use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use datafusion::arrow::array::builder::BinaryDictionaryBuilder;
use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Int64Array, StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Int32Type, TimeUnit};
use datafusion::common::{Column, DFSchema};
use datafusion::logical_expr::{Expr, ExprSchemable, Operator};
use dbsp::circuit::plan::DbspProjectExpr;
use dbsp::{DbspPredicate, RowSchema};
use regex::Regex;

use crate::encoding::{EncodedRowScalar, extract_encoded_row_i64_like_column};

#[derive(Clone)]
pub(crate) struct VectorizedFilterProjectEvaluator {
    input_schema: datafusion::arrow::datatypes::SchemaRef,
    predicate: Option<PredicatePlan>,
    projection_plan: ProjectionPlan,
    encoded_fast_path: Option<EncodedFilterProjectFastPath>,
    encoded_predicate: Option<EncodedPredicatePlan>,
    predicate_input_layout: DecodedInputLayout,
    projection_input_layout: DecodedInputLayout,
}

#[derive(Clone)]
enum PredicatePlan {
    Compiled(Arc<CompiledExpr>),
}

#[derive(Clone)]
struct EncodedFilterProjectFastPath {
    predicate: SimpleEncodedPredicate,
    projection_width: usize,
    output_positions_by_input: Arc<Vec<Vec<usize>>>,
    max_required_input_index: usize,
    input_width: usize,
}

#[derive(Clone)]
enum EncodedPredicatePlan {
    Simple(SimpleEncodedPredicate),
    I64Compare {
        left: EncodedI64Expr,
        op: Operator,
        right: EncodedI64Expr,
    },
    ConjunctiveRange {
        expr: EncodedI64Expr,
        low: EncodedI64Expr,
        low_op: Operator,
        high: EncodedI64Expr,
        high_op: Operator,
    },
}

#[derive(Clone)]
enum EncodedI64Expr {
    Column {
        index: usize,
        field_type: EncodedFieldType,
    },
    Literal(i64),
    Negative(Arc<EncodedI64Expr>),
    Binary {
        op: Operator,
        left: Arc<EncodedI64Expr>,
        right: Arc<EncodedI64Expr>,
    },
}

#[derive(Clone, Copy)]
struct SimpleEncodedPredicate {
    column_index: usize,
    op: Operator,
    literal: i64,
    field_type: EncodedFieldType,
}

#[derive(Clone, Copy)]
enum EncodedFieldType {
    Int64,
    TimestampMillis,
}

#[derive(Clone)]
struct DecodedInputLayout {
    slots: Arc<Vec<Option<usize>>>,
    value_types: Arc<Vec<CompiledValueType>>,
    count: usize,
}

impl VectorizedFilterProjectEvaluator {
    pub(crate) fn for_filter_map(
        predicate: &DbspPredicate,
        projections: &[DbspProjectExpr],
        input_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        let column_projection = column_projection_indices(projections, input_schema.as_ref());
        let predicate_input_columns =
            required_input_columns(Some(predicate), None, input_schema.as_ref(), true)?;
        let predicate_input_layout =
            build_decoded_input_layout(input_schema.as_ref(), &predicate_input_columns)?;
        let projection_input_columns = required_input_columns(
            None,
            Some(projections),
            input_schema.as_ref(),
            column_projection.is_some(),
        )?;
        let projection_input_layout =
            build_decoded_input_layout(input_schema.as_ref(), &projection_input_columns)?;
        let input_schema = input_schema.to_arrow_schema();
        let df_schema = DFSchema::try_from(input_schema.as_ref().clone())
            .context("build DataFusion schema for vectorized filter_map")?;
        let predicate = if let Some(compiled) = CompiledExpr::try_compile(
            predicate.expression().expr(),
            &df_schema,
            input_schema.as_ref(),
        )? {
            PredicatePlan::Compiled(Arc::new(compiled))
        } else {
            return Err(anyhow!(
                "unsupported vectorized filter_map predicate expression: {:?}",
                predicate.expression().expr()
            ));
        };
        let projection_plan = if let Some(indices) = column_projection {
            ProjectionPlan::column_indices(indices, input_schema.fields().len())
        } else if let Some(compiled) = projections
            .iter()
            .map(|expr| {
                CompiledExpr::try_compile(
                    expr.expression().expr(),
                    &df_schema,
                    input_schema.as_ref(),
                )
            })
            .collect::<Result<Option<Vec<_>>>>()?
        {
            ProjectionPlan::Compiled(Arc::new(compiled))
        } else {
            return Err(anyhow!(
                "unsupported vectorized filter_map projection expression(s): {:?}",
                projections
                    .iter()
                    .map(|expr| expr.expression().expr())
                    .collect::<Vec<_>>()
            ));
        };
        let encoded_predicate = build_encoded_predicate(Some(&predicate), input_schema.as_ref());
        let encoded_fast_path =
            build_encoded_fast_path(Some(&predicate), &projection_plan, input_schema.as_ref());
        Ok(Self {
            input_schema,
            predicate: Some(predicate),
            projection_plan,
            encoded_fast_path,
            encoded_predicate,
            predicate_input_layout,
            projection_input_layout,
        })
    }

    pub(crate) fn for_filter(
        predicate: &DbspPredicate,
        input_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        let projections = (0..input_schema.len()).collect::<Vec<_>>();
        let predicate_input_columns =
            required_input_columns(Some(predicate), None, input_schema.as_ref(), true)?;
        let predicate_input_layout =
            build_decoded_input_layout(input_schema.as_ref(), &predicate_input_columns)?;
        let projection_input_layout = build_decoded_input_layout(input_schema.as_ref(), &[])?;
        let input_schema = input_schema.to_arrow_schema();
        let input_width = input_schema.fields().len();
        let df_schema = DFSchema::try_from(input_schema.as_ref().clone())
            .context("build DataFusion schema for vectorized filter")?;
        let predicate = if let Some(compiled) = CompiledExpr::try_compile(
            predicate.expression().expr(),
            &df_schema,
            input_schema.as_ref(),
        )? {
            PredicatePlan::Compiled(Arc::new(compiled))
        } else {
            return Err(anyhow!(
                "unsupported vectorized filter predicate expression: {:?}",
                predicate.expression().expr()
            ));
        };
        let projection_plan = ProjectionPlan::column_indices(projections, input_width);
        let encoded_predicate = build_encoded_predicate(Some(&predicate), input_schema.as_ref());
        let encoded_fast_path =
            build_encoded_fast_path(Some(&predicate), &projection_plan, input_schema.as_ref());
        Ok(Self {
            input_schema,
            predicate: Some(predicate),
            projection_plan,
            encoded_fast_path,
            encoded_predicate,
            predicate_input_layout,
            projection_input_layout,
        })
    }

    pub(crate) fn for_map(
        projections: &[DbspProjectExpr],
        input_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        let column_projection = column_projection_indices(projections, input_schema.as_ref());
        let predicate_input_layout = build_decoded_input_layout(input_schema.as_ref(), &[])?;
        let projection_input_columns = required_input_columns(
            None,
            Some(projections),
            input_schema.as_ref(),
            column_projection.is_some(),
        )?;
        let projection_input_layout =
            build_decoded_input_layout(input_schema.as_ref(), &projection_input_columns)?;
        let input_schema = input_schema.to_arrow_schema();
        let df_schema = DFSchema::try_from(input_schema.as_ref().clone())
            .context("build DataFusion schema for vectorized map")?;
        let projection_plan = if let Some(indices) = column_projection {
            ProjectionPlan::column_indices(indices, input_schema.fields().len())
        } else if let Some(compiled) = projections
            .iter()
            .map(|expr| {
                CompiledExpr::try_compile(
                    expr.expression().expr(),
                    &df_schema,
                    input_schema.as_ref(),
                )
            })
            .collect::<Result<Option<Vec<_>>>>()?
        {
            ProjectionPlan::Compiled(Arc::new(compiled))
        } else {
            return Err(anyhow!(
                "unsupported vectorized map projection expression(s): {:?}",
                projections
                    .iter()
                    .map(|expr| expr.expression().expr())
                    .collect::<Vec<_>>()
            ));
        };
        Ok(Self {
            input_schema,
            predicate: None,
            projection_plan,
            encoded_fast_path: None,
            encoded_predicate: None,
            predicate_input_layout,
            projection_input_layout,
        })
    }

    pub(crate) fn transform_delta(
        &self,
        graph_id: &str,
        delta_values: &[(Vec<u8>, i64)],
    ) -> Result<Vec<(Vec<u8>, i64)>> {
        if delta_values.is_empty() {
            return Ok(Vec::new());
        }
        let identity_projection = self
            .projection_plan
            .is_identity(self.input_schema.fields().len());
        if self.predicate.is_none() && identity_projection {
            return consolidate_encoded_delta_batch(delta_values.to_vec());
        }
        if let Some(fast_path) = self.encoded_fast_path.as_ref() {
            return fast_path.transform_delta(delta_values);
        }
        let selected_input = if self.predicate.is_some() {
            if let Some(encoded_predicate) = self.encoded_predicate.as_ref() {
                let selected = encoded_predicate.select_delta(delta_values)?;
                if selected.is_empty() {
                    return Ok(Vec::new());
                }
                if identity_projection {
                    return consolidate_encoded_delta_batch(selected);
                }
                selected
            } else {
                let prepared = self.prepare_input_with_layout(
                    graph_id,
                    delta_values,
                    &self.predicate_input_layout,
                    self.predicate_requires_compiled_batch(),
                    false,
                )?;
                if prepared.encoded_rows.is_empty() {
                    return Ok(Vec::new());
                }
                let selected = self.selected_indices(&prepared)?;
                if selected.is_empty() {
                    return Ok(Vec::new());
                }
                if identity_projection {
                    let mut staged = Vec::with_capacity(selected.len());
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
                build_selected_delta_values(&prepared, &selected)
            }
        } else {
            delta_values
                .iter()
                .filter(|(_, diff)| *diff != 0)
                .cloned()
                .collect::<Vec<_>>()
        };
        if selected_input.is_empty() {
            return Ok(Vec::new());
        }

        let prepared = self.prepare_input_with_layout(
            graph_id,
            selected_input.as_slice(),
            &self.projection_input_layout,
            self.projection_plan.requires_compiled_batch(),
            self.projection_plan
                .needs_projection_ranges(self.input_schema.fields().len()),
        )?;

        match &self.projection_plan {
            ProjectionPlan::ColumnIndices { indices, .. } => {
                let mut staged = Vec::with_capacity(prepared.encoded_rows.len());
                let Some(projected_ranges) = prepared.projected_ranges.as_ref() else {
                    return Ok(Vec::new());
                };
                for idx in 0..prepared.encoded_rows.len() {
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
            ProjectionPlan::Compiled(projections) => {
                let batch = prepared
                    .compiled_batch
                    .as_ref()
                    .ok_or_else(|| anyhow!("compiled projection batch was unexpectedly missing"))?;
                let projection_columns = projections
                    .iter()
                    .enumerate()
                    .map(|(idx, expr)| {
                        expr.evaluate(batch)
                            .with_context(|| format!("evaluate compiled projection column {idx}"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let mut staged = Vec::with_capacity(prepared.encoded_rows.len());
                for idx in 0..prepared.encoded_rows.len() {
                    let diff = prepared.weights.get(idx).copied().unwrap_or(0);
                    if diff == 0 {
                        continue;
                    }
                    let encoded = encode_compiled_projection_row(&projection_columns, idx)?;
                    staged.push((encoded, diff));
                }
                consolidate_encoded_delta_batch(staged)
            }
        }
    }

    fn prepare_input_with_layout(
        &self,
        graph_id: &str,
        delta_values: &[(Vec<u8>, i64)],
        layout: &DecodedInputLayout,
        needs_compiled_batch: bool,
        capture_projection_ranges: bool,
    ) -> Result<PreparedEncodedInput> {
        let mut compiled_columns = needs_compiled_batch
            .then(|| vec![Vec::with_capacity(delta_values.len()); layout.count]);
        let mut encoded_rows = Vec::with_capacity(delta_values.len());
        let mut weights = Vec::with_capacity(delta_values.len());
        let mut projected_ranges =
            capture_projection_ranges.then(|| Vec::with_capacity(delta_values.len()));
        for (encoded, weight) in delta_values.iter() {
            if *weight == 0 {
                continue;
            }
            match self.decode_row(
                encoded,
                layout,
                capture_projection_ranges,
                needs_compiled_batch,
            ) {
                Ok(decoded) => {
                    if let (Some(all_columns), Some(row_values)) =
                        (compiled_columns.as_mut(), decoded.compiled_values)
                    {
                        for (slot, value) in row_values.into_iter().enumerate() {
                            all_columns[slot].push(value);
                        }
                    }
                    if let Some(all_ranges) = projected_ranges.as_mut()
                        && let Some(ranges) = decoded.projected_ranges
                    {
                        all_ranges.push(ranges);
                    }
                    encoded_rows.push(encoded.clone());
                    weights.push(*weight);
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
        let compiled_batch = if needs_compiled_batch && !encoded_rows.is_empty() {
            Some(build_compiled_input_batch(
                self.input_schema.fields().len(),
                layout.slots.as_ref(),
                compiled_columns.unwrap_or_default(),
                encoded_rows.len(),
            ))
        } else {
            None
        };
        Ok(PreparedEncodedInput {
            compiled_batch,
            encoded_rows,
            weights,
            projected_ranges,
        })
    }

    fn decode_row(
        &self,
        encoded: &[u8],
        layout: &DecodedInputLayout,
        capture_projection_ranges: bool,
        decode_compiled_values: bool,
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
        let mut compiled_values = decode_compiled_values.then(|| {
            layout
                .value_types
                .iter()
                .copied()
                .map(CompiledValue::null)
                .collect::<Vec<_>>()
        });
        let mut projected_ranges = if capture_projection_ranges {
            Some(vec![0..0; self.projection_plan.output_width()])
        } else {
            None
        };
        let projection_positions = self.projection_plan.output_positions_by_input();
        for input_idx in 0..count {
            let start = cursor;
            let end = encoded_field_end(encoded, start)?;
            if let Some(slot) = layout.slots[input_idx] {
                let decoded_scalar = if compiled_values.is_some() {
                    Some(decode_encoded_field_as_encoded_scalar(
                        &encoded[start..end],
                        layout.value_types[slot],
                    )?)
                } else {
                    None
                };
                if let Some(values) = compiled_values.as_mut() {
                    values[slot] = compiled_value_from_encoded_scalar(
                        decoded_scalar.as_ref().and_then(Option::as_ref),
                        layout.value_types[slot],
                    )?;
                }
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
            compiled_values,
            projected_ranges,
        })
    }

    fn selected_indices(&self, prepared: &PreparedEncodedInput) -> Result<Vec<usize>> {
        let mut selected = Vec::with_capacity(prepared.encoded_rows.len());
        match (&self.predicate, prepared.compiled_batch.as_ref()) {
            (Some(PredicatePlan::Compiled(predicate)), Some(batch)) => {
                let predicate = predicate
                    .evaluate(batch)
                    .context("evaluate compiled filter_map predicate")?;
                for (idx, value) in predicate.iter().enumerate() {
                    if value.predicate_truth()? {
                        selected.push(idx);
                    }
                }
            }
            (None, _) => {
                for idx in 0..prepared.encoded_rows.len() {
                    selected.push(idx);
                }
            }
            (Some(PredicatePlan::Compiled(_)), None) => {
                return Err(anyhow!("compiled predicate batch was unexpectedly missing"));
            }
        }
        Ok(selected)
    }

    fn predicate_requires_compiled_batch(&self) -> bool {
        matches!(self.predicate, Some(PredicatePlan::Compiled(_)))
    }
}

#[derive(Clone)]
enum ProjectionPlan {
    ColumnIndices {
        indices: Arc<Vec<usize>>,
        output_positions_by_input: Arc<Vec<Vec<usize>>>,
    },
    Compiled(Arc<Vec<CompiledExpr>>),
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
            Self::Compiled(_) => false,
        }
    }

    fn output_width(&self) -> usize {
        match self {
            Self::ColumnIndices { indices, .. } => indices.len(),
            Self::Compiled(projections) => projections.len(),
        }
    }

    fn output_positions_by_input(&self) -> Option<&Vec<Vec<usize>>> {
        match self {
            Self::ColumnIndices {
                output_positions_by_input,
                ..
            } => Some(output_positions_by_input.as_ref()),
            Self::Compiled(_) => None,
        }
    }

    fn needs_projection_ranges(&self, input_width: usize) -> bool {
        matches!(self, Self::ColumnIndices { .. }) && !self.is_identity(input_width)
    }

    fn requires_compiled_batch(&self) -> bool {
        matches!(self, Self::Compiled(_))
    }
}

fn build_encoded_fast_path(
    predicate: Option<&PredicatePlan>,
    projection_plan: &ProjectionPlan,
    input_schema: &datafusion::arrow::datatypes::Schema,
) -> Option<EncodedFilterProjectFastPath> {
    let PredicatePlan::Compiled(predicate) = predicate?;
    let ProjectionPlan::ColumnIndices {
        indices,
        output_positions_by_input,
    } = projection_plan
    else {
        return None;
    };
    let predicate = SimpleEncodedPredicate::try_from_compiled(predicate.as_ref(), input_schema)?;
    let max_projection_index = indices.iter().copied().max().unwrap_or(0);
    Some(EncodedFilterProjectFastPath {
        predicate,
        projection_width: indices.len(),
        output_positions_by_input: Arc::clone(output_positions_by_input),
        max_required_input_index: max_projection_index.max(predicate.column_index),
        input_width: input_schema.fields().len(),
    })
}

fn build_encoded_predicate(
    predicate: Option<&PredicatePlan>,
    input_schema: &datafusion::arrow::datatypes::Schema,
) -> Option<EncodedPredicatePlan> {
    let PredicatePlan::Compiled(predicate) = predicate?;
    EncodedPredicatePlan::try_from_compiled(predicate.as_ref(), input_schema)
}

impl EncodedFilterProjectFastPath {
    fn transform_delta(&self, delta_values: &[(Vec<u8>, i64)]) -> Result<Vec<(Vec<u8>, i64)>> {
        if delta_values.is_empty() {
            return Ok(Vec::new());
        }
        let mut staged = Vec::with_capacity(delta_values.len());
        for (encoded, diff) in delta_values.iter() {
            if *diff == 0 {
                continue;
            }
            if let Some(projected) = self.transform_row(encoded)? {
                staged.push((projected, *diff));
            }
        }
        consolidate_encoded_delta_batch(staged)
    }

    fn transform_row(&self, encoded: &[u8]) -> Result<Option<Vec<u8>>> {
        if encoded.len() < 4 {
            return Err(anyhow!("encoded key too short"));
        }
        let count = u32::from_le_bytes(encoded[0..4].try_into().unwrap()) as usize;
        if count != self.input_width {
            return Err(anyhow!(
                "encoded row has {count} columns but schema has {}",
                self.input_width
            ));
        }

        let mut cursor = 4usize;
        let mut predicate_range = None;
        let mut projection_ranges = vec![0..0; self.projection_width];
        for input_idx in 0..count {
            let start = cursor;
            let end = encoded_field_end(encoded, start)?;
            if input_idx == self.predicate.column_index {
                predicate_range = Some(start..end);
            }
            for output_idx in &self.output_positions_by_input[input_idx] {
                projection_ranges[*output_idx] = start..end;
            }
            cursor = end;
            if input_idx >= self.max_required_input_index
                && predicate_range.is_some()
                && projection_ranges.iter().all(|range| !range.is_empty())
            {
                break;
            }
        }

        let predicate_range =
            predicate_range.ok_or_else(|| anyhow!("encoded predicate column was missing"))?;
        if !self.predicate.matches(
            encoded
                .get(predicate_range)
                .ok_or_else(|| anyhow!("encoded predicate slice was out of bounds"))?,
        )? {
            return Ok(None);
        }
        if projection_ranges.iter().any(Range::is_empty) {
            return Err(anyhow!(
                "encoded projection row was missing one or more columns"
            ));
        }
        Ok(Some(project_encoded_row(
            encoded,
            &projection_ranges,
            self.projection_width,
        )?))
    }
}

impl EncodedPredicatePlan {
    fn try_from_compiled(
        expr: &CompiledExpr,
        input_schema: &datafusion::arrow::datatypes::Schema,
    ) -> Option<Self> {
        if let Some(predicate) = SimpleEncodedPredicate::try_from_compiled(expr, input_schema) {
            return Some(Self::Simple(predicate));
        }
        match expr {
            CompiledExpr::Binary { op, left, right }
                if matches!(
                    op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                ) =>
            {
                Some(Self::I64Compare {
                    left: EncodedI64Expr::try_from_compiled(left.as_ref(), input_schema)?,
                    op: *op,
                    right: EncodedI64Expr::try_from_compiled(right.as_ref(), input_schema)?,
                })
            }
            CompiledExpr::ConjunctiveRange {
                expr,
                low,
                low_op,
                high,
                high_op,
            } => Some(Self::ConjunctiveRange {
                expr: EncodedI64Expr::try_from_compiled(expr.as_ref(), input_schema)?,
                low: EncodedI64Expr::try_from_compiled(low.as_ref(), input_schema)?,
                low_op: *low_op,
                high: EncodedI64Expr::try_from_compiled(high.as_ref(), input_schema)?,
                high_op: *high_op,
            }),
            _ => None,
        }
    }

    fn select_delta(&self, delta_values: &[(Vec<u8>, i64)]) -> Result<Vec<(Vec<u8>, i64)>> {
        let mut selected = Vec::with_capacity(delta_values.len());
        for (encoded, diff) in delta_values {
            if *diff == 0 {
                continue;
            }
            if self.matches_row(encoded)? {
                selected.push((encoded.clone(), *diff));
            }
        }
        Ok(selected)
    }

    fn matches_row(&self, encoded: &[u8]) -> Result<bool> {
        match self {
            Self::Simple(predicate) => predicate.matches_encoded_row(encoded),
            Self::I64Compare { left, op, right } => {
                let left = left.evaluate_row(encoded)?;
                let right = right.evaluate_row(encoded)?;
                Ok(match (left, right) {
                    (Some(left), Some(right)) => match op {
                        Operator::Eq => left == right,
                        Operator::NotEq => left != right,
                        Operator::Lt => left < right,
                        Operator::LtEq => left <= right,
                        Operator::Gt => left > right,
                        Operator::GtEq => left >= right,
                        _ => unreachable!("validated encoded predicate operator"),
                    },
                    _ => false,
                })
            }
            Self::ConjunctiveRange {
                expr,
                low,
                low_op,
                high,
                high_op,
            } => {
                let value = expr.evaluate_row(encoded)?;
                let low = low.evaluate_row(encoded)?;
                let high = high.evaluate_row(encoded)?;
                Ok(match (value, low, high) {
                    (Some(value), Some(low), Some(high)) => {
                        compare_i64(value, low, *low_op)? && compare_i64(value, high, *high_op)?
                    }
                    _ => false,
                })
            }
        }
    }
}

impl EncodedI64Expr {
    fn try_from_compiled(
        expr: &CompiledExpr,
        input_schema: &datafusion::arrow::datatypes::Schema,
    ) -> Option<Self> {
        match expr {
            CompiledExpr::Column { index } => {
                let field_type = match input_schema.field(*index).data_type() {
                    DataType::Int64 => EncodedFieldType::Int64,
                    DataType::Timestamp(TimeUnit::Millisecond, _) => {
                        EncodedFieldType::TimestampMillis
                    }
                    _ => return None,
                };
                Some(Self::Column {
                    index: *index,
                    field_type,
                })
            }
            CompiledExpr::Literal { value } => match value {
                CompiledValue::Int64(Some(value)) | CompiledValue::TimestampMillis(Some(value)) => {
                    Some(Self::Literal(*value))
                }
                _ => None,
            },
            CompiledExpr::Negative(inner) => Some(Self::Negative(Arc::new(
                Self::try_from_compiled(inner.as_ref(), input_schema)?,
            ))),
            CompiledExpr::Binary { op, left, right }
                if matches!(
                    op,
                    Operator::Plus
                        | Operator::Minus
                        | Operator::Multiply
                        | Operator::Divide
                        | Operator::Modulo
                ) =>
            {
                Some(Self::Binary {
                    op: *op,
                    left: Arc::new(Self::try_from_compiled(left.as_ref(), input_schema)?),
                    right: Arc::new(Self::try_from_compiled(right.as_ref(), input_schema)?),
                })
            }
            _ => None,
        }
    }

    fn evaluate_row(&self, encoded: &[u8]) -> Result<Option<i64>> {
        match self {
            Self::Column { index, field_type } => {
                let value = extract_encoded_row_i64_like_column(encoded, *index)?;
                match (field_type, value) {
                    (EncodedFieldType::Int64, value)
                    | (EncodedFieldType::TimestampMillis, value) => Ok(value),
                }
            }
            Self::Literal(value) => Ok(Some(*value)),
            Self::Negative(inner) => Ok(inner.evaluate_row(encoded)?.map(|value| -value)),
            Self::Binary { op, left, right } => {
                let Some(left) = left.evaluate_row(encoded)? else {
                    return Ok(None);
                };
                let Some(right) = right.evaluate_row(encoded)? else {
                    return Ok(None);
                };
                let value = match op {
                    Operator::Plus => left + right,
                    Operator::Minus => left - right,
                    Operator::Multiply => left * right,
                    Operator::Divide => {
                        if right == 0 {
                            return Err(anyhow!("division by zero in encoded predicate"));
                        }
                        left / right
                    }
                    Operator::Modulo => {
                        if right == 0 {
                            return Err(anyhow!("modulo by zero in encoded predicate"));
                        }
                        left % right
                    }
                    _ => unreachable!("validated encoded arithmetic operator"),
                };
                Ok(Some(value))
            }
        }
    }
}

impl SimpleEncodedPredicate {
    fn try_from_compiled(
        expr: &CompiledExpr,
        input_schema: &datafusion::arrow::datatypes::Schema,
    ) -> Option<Self> {
        let CompiledExpr::Binary { op, left, right } = expr else {
            return None;
        };
        if !matches!(
            op,
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
        ) {
            return None;
        }
        if let Some(predicate) =
            Self::column_literal(left.as_ref(), right.as_ref(), *op, input_schema)
        {
            return Some(predicate);
        }
        Self::column_literal(
            right.as_ref(),
            left.as_ref(),
            invert_comparison_operator(*op)?,
            input_schema,
        )
    }

    fn column_literal(
        column_expr: &CompiledExpr,
        literal_expr: &CompiledExpr,
        op: Operator,
        input_schema: &datafusion::arrow::datatypes::Schema,
    ) -> Option<Self> {
        let CompiledExpr::Column { index } = column_expr else {
            return None;
        };
        let CompiledExpr::Literal { value } = literal_expr else {
            return None;
        };
        let field_type = match input_schema.field(*index).data_type() {
            DataType::Int64 => EncodedFieldType::Int64,
            DataType::Timestamp(TimeUnit::Millisecond, _) => EncodedFieldType::TimestampMillis,
            _ => return None,
        };
        let literal = match value {
            CompiledValue::Int64(Some(value)) => *value,
            CompiledValue::TimestampMillis(Some(value)) => *value,
            _ => return None,
        };
        Some(Self {
            column_index: *index,
            op,
            literal,
            field_type,
        })
    }

    fn matches(&self, field: &[u8]) -> Result<bool> {
        let value = match (self.field_type, field.first().copied()) {
            (EncodedFieldType::Int64, Some(0x01))
            | (EncodedFieldType::TimestampMillis, Some(0x03)) => {
                let chunk = field
                    .get(1..9)
                    .ok_or_else(|| anyhow!("truncated encoded predicate field"))?;
                Some(i64::from_le_bytes(chunk.try_into().unwrap()))
            }
            (EncodedFieldType::Int64, Some(0x05))
            | (EncodedFieldType::TimestampMillis, Some(0x07)) => None,
            _ => {
                return Err(anyhow!(
                    "encoded predicate field did not match the expected scalar type"
                ));
            }
        };
        let Some(value) = value else {
            return Ok(false);
        };
        Ok(match self.op {
            Operator::Eq => value == self.literal,
            Operator::NotEq => value != self.literal,
            Operator::Lt => value < self.literal,
            Operator::LtEq => value <= self.literal,
            Operator::Gt => value > self.literal,
            Operator::GtEq => value >= self.literal,
            _ => unreachable!("validated encoded predicate operator"),
        })
    }

    fn matches_encoded_row(&self, encoded: &[u8]) -> Result<bool> {
        let value = extract_encoded_row_i64_like_column(encoded, self.column_index)?;
        let Some(value) = value else {
            return Ok(false);
        };
        Ok(match self.op {
            Operator::Eq => value == self.literal,
            Operator::NotEq => value != self.literal,
            Operator::Lt => value < self.literal,
            Operator::LtEq => value <= self.literal,
            Operator::Gt => value > self.literal,
            Operator::GtEq => value >= self.literal,
            _ => unreachable!("validated encoded predicate operator"),
        })
    }
}

fn invert_comparison_operator(op: Operator) -> Option<Operator> {
    match op {
        Operator::Eq | Operator::NotEq => Some(op),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        _ => None,
    }
}

fn compare_i64(left: i64, right: i64, op: Operator) -> Result<bool> {
    Ok(match op {
        Operator::Eq => left == right,
        Operator::NotEq => left != right,
        Operator::Lt => left < right,
        Operator::LtEq => left <= right,
        Operator::Gt => left > right,
        Operator::GtEq => left >= right,
        _ => return Err(anyhow!("unsupported encoded comparison operator {op:?}")),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompiledValueType {
    Int64,
    Utf8,
    TimestampMillis,
    Bool,
}

impl CompiledValueType {
    fn try_from_arrow(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Int64 => Some(Self::Int64),
            DataType::Utf8 => Some(Self::Utf8),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Some(Self::TimestampMillis),
            DataType::Boolean => Some(Self::Bool),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CompiledValue {
    Int64(Option<i64>),
    Utf8(Option<String>),
    TimestampMillis(Option<i64>),
    Bool(Option<bool>),
}

impl CompiledValue {
    fn null(data_type: CompiledValueType) -> Self {
        match data_type {
            CompiledValueType::Int64 => Self::Int64(None),
            CompiledValueType::Utf8 => Self::Utf8(None),
            CompiledValueType::TimestampMillis => Self::TimestampMillis(None),
            CompiledValueType::Bool => Self::Bool(None),
        }
    }

    fn from_array(array: &dyn Array, data_type: CompiledValueType) -> Result<Self> {
        match data_type {
            CompiledValueType::Int64 => {
                let values = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    anyhow!(
                        "unsupported literal array {:?} for compiled type {:?}",
                        array.data_type(),
                        data_type
                    )
                })?;
                Ok(Self::Int64((!values.is_null(0)).then(|| values.value(0))))
            }
            CompiledValueType::Utf8 => {
                let values = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        anyhow!(
                            "unsupported literal array {:?} for compiled type {:?}",
                            array.data_type(),
                            data_type
                        )
                    })?;
                Ok(Self::Utf8(
                    (!values.is_null(0)).then(|| values.value(0).to_string()),
                ))
            }
            CompiledValueType::TimestampMillis => {
                let values = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        anyhow!(
                            "unsupported literal array {:?} for compiled type {:?}",
                            array.data_type(),
                            data_type
                        )
                    })?;
                Ok(Self::TimestampMillis(
                    (!values.is_null(0)).then(|| values.value(0)),
                ))
            }
            CompiledValueType::Bool => {
                let values = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        anyhow!(
                            "unsupported literal array {:?} for compiled type {:?}",
                            array.data_type(),
                            data_type
                        )
                    })?;
                Ok(Self::Bool((!values.is_null(0)).then(|| values.value(0))))
            }
        }
    }

    fn is_null(&self) -> bool {
        matches!(
            self,
            Self::Int64(None) | Self::Utf8(None) | Self::TimestampMillis(None) | Self::Bool(None)
        )
    }

    fn predicate_truth(&self) -> Result<bool> {
        match self {
            Self::Bool(Some(value)) => Ok(*value),
            Self::Bool(None) => Ok(false),
            other => Err(anyhow!(
                "compiled predicate did not evaluate to boolean: {other:?}"
            )),
        }
    }

    fn as_bool_opt(&self) -> Result<Option<bool>> {
        match self {
            Self::Bool(value) => Ok(*value),
            other => Err(anyhow!("expected boolean value, found {other:?}")),
        }
    }

    fn as_i64_opt(&self, context: &str) -> Result<Option<i64>> {
        match self {
            Self::Int64(value) => Ok(*value),
            other => Err(anyhow!("{context} expects Int64, found {other:?}")),
        }
    }

    fn equals(&self, other: &Self) -> Result<Option<bool>> {
        if self.is_null() || other.is_null() {
            return Ok(None);
        }
        let equal = match (self, other) {
            (Self::Int64(Some(left)), Self::Int64(Some(right))) => left == right,
            (Self::Utf8(Some(left)), Self::Utf8(Some(right))) => left == right,
            (Self::TimestampMillis(Some(left)), Self::TimestampMillis(Some(right))) => {
                left == right
            }
            (Self::Bool(Some(left)), Self::Bool(Some(right))) => left == right,
            _ => {
                return Err(anyhow!(
                    "mismatched compiled comparison operands: {self:?} vs {other:?}"
                ));
            }
        };
        Ok(Some(equal))
    }

    fn compare(&self, other: &Self) -> Result<Option<Ordering>> {
        if self.is_null() || other.is_null() {
            return Ok(None);
        }
        let ordering = match (self, other) {
            (Self::Int64(Some(left)), Self::Int64(Some(right))) => left.cmp(right),
            (Self::Utf8(Some(left)), Self::Utf8(Some(right))) => left.cmp(right),
            (Self::TimestampMillis(Some(left)), Self::TimestampMillis(Some(right))) => {
                left.cmp(right)
            }
            (Self::Bool(Some(left)), Self::Bool(Some(right))) => left.cmp(right),
            _ => {
                return Err(anyhow!(
                    "mismatched compiled comparison operands: {self:?} vs {other:?}"
                ));
            }
        };
        Ok(Some(ordering))
    }
}

type CompiledColumn = Arc<Vec<CompiledValue>>;

struct CompiledInputBatch {
    columns: Vec<Option<CompiledColumn>>,
    row_count: usize,
}

impl CompiledInputBatch {
    fn column(&self, index: usize) -> Result<CompiledColumn> {
        self.columns
            .get(index)
            .and_then(|column| column.as_ref())
            .cloned()
            .ok_or_else(|| anyhow!("compiled input column {index} was unexpectedly missing"))
    }
}

#[derive(Clone, PartialEq, Eq)]
struct CompiledCaseArm {
    when: Arc<CompiledExpr>,
    then: Arc<CompiledExpr>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CompiledScalarFunction {
    Hour,
    CountChar,
    DateFormat,
    Lower,
    Proctime,
    RegexpExtract,
    SplitIndex,
}

impl CompiledScalarFunction {
    fn from_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("hour") {
            Some(Self::Hour)
        } else if name.eq_ignore_ascii_case("count_char") {
            Some(Self::CountChar)
        } else if name.eq_ignore_ascii_case("date_format") {
            Some(Self::DateFormat)
        } else if name.eq_ignore_ascii_case("lower") {
            Some(Self::Lower)
        } else if name.eq_ignore_ascii_case("proctime") {
            Some(Self::Proctime)
        } else if name.eq_ignore_ascii_case("regexp_extract") {
            Some(Self::RegexpExtract)
        } else if name.eq_ignore_ascii_case("split_index") {
            Some(Self::SplitIndex)
        } else {
            None
        }
    }

    fn arity(&self) -> usize {
        match self {
            Self::Hour => 1,
            Self::CountChar => 2,
            Self::DateFormat => 2,
            Self::Lower => 1,
            Self::Proctime => 0,
            Self::RegexpExtract => 3,
            Self::SplitIndex => 3,
        }
    }

    fn evaluate(&self, args: &[CompiledColumn], row_count: usize) -> Result<CompiledColumn> {
        match self {
            Self::Hour => {
                let ts = args
                    .first()
                    .ok_or_else(|| anyhow!("compiled hour expected one argument"))?;
                let mut output = Vec::with_capacity(row_count);
                for row_idx in 0..row_count {
                    let value = ts
                        .get(row_idx)
                        .ok_or_else(|| anyhow!("compiled hour row {row_idx} was missing"))?;
                    let hour = match value {
                        CompiledValue::TimestampMillis(Some(millis)) => {
                            Some(millis.div_euclid(3_600_000).rem_euclid(24))
                        }
                        CompiledValue::TimestampMillis(None) => None,
                        other => {
                            return Err(anyhow!(
                                "compiled hour expects timestamp(ms), found {other:?}"
                            ));
                        }
                    };
                    output.push(CompiledValue::Int64(hour));
                }
                Ok(Arc::new(output))
            }
            Self::Proctime => Ok(Arc::new(vec![
                CompiledValue::TimestampMillis(None);
                row_count
            ])),
            Self::CountChar => {
                let text = args
                    .first()
                    .ok_or_else(|| anyhow!("compiled count_char expected two arguments"))?;
                let needle = args
                    .get(1)
                    .ok_or_else(|| anyhow!("compiled count_char expected two arguments"))?;
                let mut output = Vec::with_capacity(row_count);
                for row_idx in 0..row_count {
                    let text = text
                        .get(row_idx)
                        .ok_or_else(|| anyhow!("compiled count_char row {row_idx} was missing"))?;
                    let needle = needle.get(row_idx).ok_or_else(|| {
                        anyhow!("compiled count_char row {row_idx} was missing from needle")
                    })?;
                    let count = match (text, needle) {
                        (CompiledValue::Utf8(Some(text)), CompiledValue::Utf8(Some(needle))) => {
                            Some(if needle.is_empty() {
                                0
                            } else {
                                i64::try_from(text.matches(needle).count()).unwrap_or(i64::MAX)
                            })
                        }
                        (CompiledValue::Utf8(None), _) | (_, CompiledValue::Utf8(None)) => None,
                        _ => {
                            return Err(anyhow!(
                                "compiled count_char expects Utf8 operands: {text:?} vs {needle:?}"
                            ));
                        }
                    };
                    output.push(CompiledValue::Int64(count));
                }
                Ok(Arc::new(output))
            }
            Self::DateFormat => {
                let ts = args
                    .first()
                    .ok_or_else(|| anyhow!("compiled date_format expected two arguments"))?;
                let fmt = args
                    .get(1)
                    .ok_or_else(|| anyhow!("compiled date_format expected two arguments"))?;
                let mut output = Vec::with_capacity(row_count);
                for row_idx in 0..row_count {
                    let ts = ts
                        .get(row_idx)
                        .ok_or_else(|| anyhow!("compiled date_format row {row_idx} was missing"))?;
                    let fmt = fmt.get(row_idx).ok_or_else(|| {
                        anyhow!("compiled date_format row {row_idx} was missing from format")
                    })?;
                    let value = match (ts, fmt) {
                        (
                            CompiledValue::TimestampMillis(Some(millis)),
                            CompiledValue::Utf8(Some(pattern)),
                        ) => chrono::DateTime::<Utc>::from_timestamp_millis(*millis).map(|dt| {
                            dt.format(&translate_date_format_pattern(pattern))
                                .to_string()
                        }),
                        (CompiledValue::TimestampMillis(None), _)
                        | (_, CompiledValue::Utf8(None)) => None,
                        _ => {
                            return Err(anyhow!(
                                "compiled date_format expects TimestampMillis and Utf8 operands: {ts:?} vs {fmt:?}"
                            ));
                        }
                    };
                    output.push(CompiledValue::Utf8(value));
                }
                Ok(Arc::new(output))
            }
            Self::Lower => {
                let text = args
                    .first()
                    .ok_or_else(|| anyhow!("compiled lower expected one argument"))?;
                let mut output = Vec::with_capacity(row_count);
                for row_idx in 0..row_count {
                    let text = text
                        .get(row_idx)
                        .ok_or_else(|| anyhow!("compiled lower row {row_idx} was missing"))?;
                    let value = match text {
                        CompiledValue::Utf8(Some(text)) => Some(text.to_lowercase()),
                        CompiledValue::Utf8(None) => None,
                        other => {
                            return Err(anyhow!("compiled lower expects Utf8, found {other:?}"));
                        }
                    };
                    output.push(CompiledValue::Utf8(value));
                }
                Ok(Arc::new(output))
            }
            Self::RegexpExtract => {
                let text = args
                    .first()
                    .ok_or_else(|| anyhow!("compiled regexp_extract expected three arguments"))?;
                let pattern = args
                    .get(1)
                    .ok_or_else(|| anyhow!("compiled regexp_extract expected three arguments"))?;
                let group = args
                    .get(2)
                    .ok_or_else(|| anyhow!("compiled regexp_extract expected three arguments"))?;
                let mut cache: HashMap<String, Option<Regex>> = HashMap::new();
                let mut output = Vec::with_capacity(row_count);
                for row_idx in 0..row_count {
                    let text = text.get(row_idx).ok_or_else(|| {
                        anyhow!("compiled regexp_extract row {row_idx} was missing")
                    })?;
                    let pattern = pattern.get(row_idx).ok_or_else(|| {
                        anyhow!("compiled regexp_extract row {row_idx} was missing from pattern")
                    })?;
                    let group = group.get(row_idx).ok_or_else(|| {
                        anyhow!("compiled regexp_extract row {row_idx} was missing from group")
                    })?;
                    let value = match (text, pattern, group) {
                        (
                            CompiledValue::Utf8(Some(text)),
                            CompiledValue::Utf8(Some(pattern)),
                            CompiledValue::Int64(Some(group)),
                        ) if *group >= 0 => {
                            let regex = cache
                                .entry(pattern.clone())
                                .or_insert_with(|| Regex::new(pattern).ok());
                            regex
                                .as_ref()
                                .and_then(|regex| regex.captures(text))
                                .and_then(|captures| captures.get(*group as usize))
                                .map(|matched| matched.as_str().to_string())
                        }
                        (
                            CompiledValue::Utf8(Some(_)),
                            CompiledValue::Utf8(Some(_)),
                            CompiledValue::Int64(Some(_)),
                        ) => None,
                        (CompiledValue::Utf8(None), _, _)
                        | (_, CompiledValue::Utf8(None), _)
                        | (_, _, CompiledValue::Int64(None)) => None,
                        _ => {
                            return Err(anyhow!(
                                "compiled regexp_extract expects Utf8, Utf8, Int64 operands: {text:?}, {pattern:?}, {group:?}"
                            ));
                        }
                    };
                    output.push(CompiledValue::Utf8(value));
                }
                Ok(Arc::new(output))
            }
            Self::SplitIndex => {
                let text = args
                    .first()
                    .ok_or_else(|| anyhow!("compiled split_index expected three arguments"))?;
                let delimiter = args
                    .get(1)
                    .ok_or_else(|| anyhow!("compiled split_index expected three arguments"))?;
                let index = args
                    .get(2)
                    .ok_or_else(|| anyhow!("compiled split_index expected three arguments"))?;
                let mut output = Vec::with_capacity(row_count);
                for row_idx in 0..row_count {
                    let text = text
                        .get(row_idx)
                        .ok_or_else(|| anyhow!("compiled split_index row {row_idx} was missing"))?;
                    let delimiter = delimiter.get(row_idx).ok_or_else(|| {
                        anyhow!("compiled split_index row {row_idx} was missing from delimiter")
                    })?;
                    let index = index.get(row_idx).ok_or_else(|| {
                        anyhow!("compiled split_index row {row_idx} was missing from index")
                    })?;
                    let value = match (text, delimiter, index) {
                        (
                            CompiledValue::Utf8(Some(text)),
                            CompiledValue::Utf8(Some(delimiter)),
                            CompiledValue::Int64(Some(index)),
                        ) => split_index_value(text, delimiter, *index),
                        (CompiledValue::Utf8(None), _, _)
                        | (_, CompiledValue::Utf8(None), _)
                        | (_, _, CompiledValue::Int64(None)) => None,
                        _ => {
                            return Err(anyhow!(
                                "compiled split_index expects Utf8, Utf8, Int64 operands: {text:?}, {delimiter:?}, {index:?}"
                            ));
                        }
                    };
                    output.push(CompiledValue::Utf8(value));
                }
                Ok(Arc::new(output))
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
enum CompiledExpr {
    Column {
        index: usize,
    },
    Literal {
        value: CompiledValue,
    },
    Binary {
        op: Operator,
        left: Arc<CompiledExpr>,
        right: Arc<CompiledExpr>,
    },
    Not(Arc<CompiledExpr>),
    Negative(Arc<CompiledExpr>),
    IsNull(Arc<CompiledExpr>),
    IsNotNull(Arc<CompiledExpr>),
    IsTrue(Arc<CompiledExpr>),
    IsNotTrue(Arc<CompiledExpr>),
    IsFalse(Arc<CompiledExpr>),
    IsNotFalse(Arc<CompiledExpr>),
    IsUnknown(Arc<CompiledExpr>),
    IsNotUnknown(Arc<CompiledExpr>),
    Between {
        expr: Arc<CompiledExpr>,
        low: Arc<CompiledExpr>,
        high: Arc<CompiledExpr>,
        negated: bool,
    },
    InList {
        expr: Arc<CompiledExpr>,
        list: Arc<Vec<CompiledExpr>>,
        negated: bool,
    },
    ConjunctiveRange {
        expr: Arc<CompiledExpr>,
        low: Arc<CompiledExpr>,
        low_op: Operator,
        high: Arc<CompiledExpr>,
        high_op: Operator,
    },
    Case {
        expr: Option<Arc<CompiledExpr>>,
        when_then_expr: Arc<Vec<CompiledCaseArm>>,
        else_expr: Option<Arc<CompiledExpr>>,
        result_type: CompiledValueType,
    },
    Cast {
        expr: Arc<CompiledExpr>,
        target_type: CompiledValueType,
    },
    ScalarFunction {
        function: CompiledScalarFunction,
        args: Arc<Vec<CompiledExpr>>,
    },
}

impl CompiledExpr {
    fn try_compile(
        expr: &Expr,
        df_schema: &DFSchema,
        input_schema: &datafusion::arrow::datatypes::Schema,
    ) -> Result<Option<Self>> {
        match expr {
            Expr::Alias(alias) => Self::try_compile(alias.expr.as_ref(), df_schema, input_schema),
            Expr::Cast(cast) => {
                let Some(target_type) = CompiledValueType::try_from_arrow(&cast.data_type) else {
                    return Ok(None);
                };
                let Some(expr) = Self::try_compile(cast.expr.as_ref(), df_schema, input_schema)?
                else {
                    return Ok(None);
                };
                Ok(Some(Self::Cast {
                    expr: Arc::new(expr),
                    target_type,
                }))
            }
            Expr::Column(column) => Ok(Some(Self::Column {
                index: resolve_compiled_column_index(input_schema, column)?,
            })),
            Expr::Literal(value, _) => {
                let Some(data_type) = CompiledValueType::try_from_arrow(&expr.get_type(df_schema)?)
                else {
                    return Ok(None);
                };
                let literal_array = value.to_array()?;
                Ok(Some(Self::Literal {
                    value: CompiledValue::from_array(literal_array.as_ref(), data_type)?,
                }))
            }
            Expr::BinaryExpr(binary) => {
                if binary.op == Operator::And
                    && let Some(compiled) =
                        Self::try_compile_conjunctive_range(binary, df_schema, input_schema)?
                {
                    return Ok(Some(compiled));
                }
                let supported = matches!(
                    binary.op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                        | Operator::And
                        | Operator::Or
                        | Operator::Plus
                        | Operator::Minus
                        | Operator::Multiply
                        | Operator::Divide
                        | Operator::Modulo
                        | Operator::StringConcat
                );
                if !supported {
                    return Ok(None);
                }
                let Some(left) = Self::try_compile(binary.left.as_ref(), df_schema, input_schema)?
                else {
                    return Ok(None);
                };
                let Some(right) =
                    Self::try_compile(binary.right.as_ref(), df_schema, input_schema)?
                else {
                    return Ok(None);
                };
                Ok(Some(Self::Binary {
                    op: binary.op,
                    left: Arc::new(left),
                    right: Arc::new(right),
                }))
            }
            Expr::Not(inner) => Self::try_compile(inner.as_ref(), df_schema, input_schema)
                .map(|expr| expr.map(|expr| Self::Not(Arc::new(expr)))),
            Expr::Negative(inner) => Self::try_compile(inner.as_ref(), df_schema, input_schema)
                .map(|expr| expr.map(|expr| Self::Negative(Arc::new(expr)))),
            Expr::IsNull(inner) => Self::try_compile(inner.as_ref(), df_schema, input_schema)
                .map(|expr| expr.map(|expr| Self::IsNull(Arc::new(expr)))),
            Expr::IsNotNull(inner) => Self::try_compile(inner.as_ref(), df_schema, input_schema)
                .map(|expr| expr.map(|expr| Self::IsNotNull(Arc::new(expr)))),
            Expr::IsTrue(inner) => Self::try_compile(inner.as_ref(), df_schema, input_schema)
                .map(|expr| expr.map(|expr| Self::IsTrue(Arc::new(expr)))),
            Expr::IsNotTrue(inner) => Self::try_compile(inner.as_ref(), df_schema, input_schema)
                .map(|expr| expr.map(|expr| Self::IsNotTrue(Arc::new(expr)))),
            Expr::IsFalse(inner) => Self::try_compile(inner.as_ref(), df_schema, input_schema)
                .map(|expr| expr.map(|expr| Self::IsFalse(Arc::new(expr)))),
            Expr::IsNotFalse(inner) => Self::try_compile(inner.as_ref(), df_schema, input_schema)
                .map(|expr| expr.map(|expr| Self::IsNotFalse(Arc::new(expr)))),
            Expr::IsUnknown(inner) => Self::try_compile(inner.as_ref(), df_schema, input_schema)
                .map(|expr| expr.map(|expr| Self::IsUnknown(Arc::new(expr)))),
            Expr::IsNotUnknown(inner) => Self::try_compile(inner.as_ref(), df_schema, input_schema)
                .map(|expr| expr.map(|expr| Self::IsNotUnknown(Arc::new(expr)))),
            Expr::Between(between) => {
                let Some(expr) = Self::try_compile(between.expr.as_ref(), df_schema, input_schema)?
                else {
                    return Ok(None);
                };
                let Some(low) = Self::try_compile(between.low.as_ref(), df_schema, input_schema)?
                else {
                    return Ok(None);
                };
                let Some(high) = Self::try_compile(between.high.as_ref(), df_schema, input_schema)?
                else {
                    return Ok(None);
                };
                Ok(Some(Self::Between {
                    expr: Arc::new(expr),
                    low: Arc::new(low),
                    high: Arc::new(high),
                    negated: between.negated,
                }))
            }
            Expr::InList(in_list) => {
                let Some(expr) = Self::try_compile(in_list.expr.as_ref(), df_schema, input_schema)?
                else {
                    return Ok(None);
                };
                let Some(list) = in_list
                    .list
                    .iter()
                    .map(|item| Self::try_compile(item, df_schema, input_schema))
                    .collect::<Result<Option<Vec<_>>>>()?
                else {
                    return Ok(None);
                };
                Ok(Some(Self::InList {
                    expr: Arc::new(expr),
                    list: Arc::new(list),
                    negated: in_list.negated,
                }))
            }
            Expr::Case(case) => {
                let base_expr = match case.expr.as_ref() {
                    Some(expr) => {
                        let Some(compiled) =
                            Self::try_compile(expr.as_ref(), df_schema, input_schema)?
                        else {
                            return Ok(None);
                        };
                        Some(Arc::new(compiled))
                    }
                    None => None,
                };
                let mut when_then_expr = Vec::with_capacity(case.when_then_expr.len());
                for (when, then) in &case.when_then_expr {
                    let Some(when) = Self::try_compile(when.as_ref(), df_schema, input_schema)?
                    else {
                        return Ok(None);
                    };
                    let Some(then) = Self::try_compile(then.as_ref(), df_schema, input_schema)?
                    else {
                        return Ok(None);
                    };
                    when_then_expr.push(CompiledCaseArm {
                        when: Arc::new(when),
                        then: Arc::new(then),
                    });
                }
                let else_expr = match case.else_expr.as_ref() {
                    Some(expr) => {
                        let Some(compiled) =
                            Self::try_compile(expr.as_ref(), df_schema, input_schema)?
                        else {
                            return Ok(None);
                        };
                        Some(Arc::new(compiled))
                    }
                    None => None,
                };
                let Some(result_type) =
                    CompiledValueType::try_from_arrow(&expr.get_type(df_schema)?)
                else {
                    return Ok(None);
                };
                Ok(Some(Self::Case {
                    expr: base_expr,
                    when_then_expr: Arc::new(when_then_expr),
                    else_expr,
                    result_type,
                }))
            }
            Expr::ScalarFunction(function) => {
                let Some(function_name) = CompiledScalarFunction::from_name(function.name()) else {
                    return Ok(None);
                };
                if function.args.len() != function_name.arity() {
                    return Ok(None);
                }
                let Some(args) = function
                    .args
                    .iter()
                    .map(|arg| Self::try_compile(arg, df_schema, input_schema))
                    .collect::<Result<Option<Vec<_>>>>()?
                else {
                    return Ok(None);
                };
                Ok(Some(Self::ScalarFunction {
                    function: function_name,
                    args: Arc::new(args),
                }))
            }
            _ => Ok(None),
        }
    }

    fn try_compile_conjunctive_range(
        binary: &datafusion::logical_expr::BinaryExpr,
        df_schema: &DFSchema,
        input_schema: &datafusion::arrow::datatypes::Schema,
    ) -> Result<Option<Self>> {
        let Some(left) = Self::try_compile(binary.left.as_ref(), df_schema, input_schema)? else {
            return Ok(None);
        };
        let Some(right) = Self::try_compile(binary.right.as_ref(), df_schema, input_schema)? else {
            return Ok(None);
        };
        let Some((left_expr, left_bound, left_op)) = extract_range_comparison(left) else {
            return Ok(None);
        };
        let Some((right_expr, right_bound, right_op)) = extract_range_comparison(right) else {
            return Ok(None);
        };
        if left_expr != right_expr {
            return Ok(None);
        }
        let (low, low_op, high, high_op) = match (left_op, right_op) {
            (Operator::Gt | Operator::GtEq, Operator::Lt | Operator::LtEq) => {
                (left_bound, left_op, right_bound, right_op)
            }
            (Operator::Lt | Operator::LtEq, Operator::Gt | Operator::GtEq) => {
                (right_bound, right_op, left_bound, left_op)
            }
            _ => return Ok(None),
        };
        Ok(Some(Self::ConjunctiveRange {
            expr: Arc::new(left_expr),
            low: Arc::new(low),
            low_op,
            high: Arc::new(high),
            high_op,
        }))
    }

    fn evaluate(&self, batch: &CompiledInputBatch) -> Result<CompiledColumn> {
        match self {
            Self::Column { index } => batch.column(*index),
            Self::Literal { value } => Ok(Arc::new(vec![value.clone(); batch.row_count])),
            Self::Binary { op, left, right } => {
                let left = left.evaluate(batch)?;
                let right = right.evaluate(batch)?;
                eval_compiled_binary(*op, left.as_ref(), right.as_ref())
            }
            Self::Not(inner) => {
                let values = inner.evaluate(batch)?;
                Ok(Arc::new(
                    values
                        .iter()
                        .map(|value| {
                            Ok(CompiledValue::Bool(value.as_bool_opt()?.map(|flag| !flag)))
                        })
                        .collect::<Result<Vec<_>>>()?,
                ))
            }
            Self::Negative(inner) => {
                let values = inner.evaluate(batch)?;
                Ok(Arc::new(
                    values
                        .iter()
                        .map(|value| {
                            Ok(CompiledValue::Int64(
                                value.as_i64_opt("compiled negation")?.map(|number| -number),
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?,
                ))
            }
            Self::IsNull(inner) | Self::IsUnknown(inner) => {
                let values = inner.evaluate(batch)?;
                Ok(Arc::new(
                    values
                        .iter()
                        .map(|value| CompiledValue::Bool(Some(value.is_null())))
                        .collect(),
                ))
            }
            Self::IsNotNull(inner) | Self::IsNotUnknown(inner) => {
                let values = inner.evaluate(batch)?;
                Ok(Arc::new(
                    values
                        .iter()
                        .map(|value| CompiledValue::Bool(Some(!value.is_null())))
                        .collect(),
                ))
            }
            Self::IsTrue(inner) => evaluate_truthy(inner, batch, Some(true)),
            Self::IsNotTrue(inner) => evaluate_not_truthy(inner, batch, Some(true)),
            Self::IsFalse(inner) => evaluate_truthy(inner, batch, Some(false)),
            Self::IsNotFalse(inner) => evaluate_not_truthy(inner, batch, Some(false)),
            Self::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let expr = expr.evaluate(batch)?;
                let low = low.evaluate(batch)?;
                let high = high.evaluate(batch)?;
                let mut output = Vec::with_capacity(batch.row_count);
                for ((value, low), high) in expr.iter().zip(low.iter()).zip(high.iter()) {
                    let lower = compare_compiled_values(value, low, Operator::GtEq)?;
                    let upper = compare_compiled_values(value, high, Operator::LtEq)?;
                    let value =
                        and_bool_opt(lower, upper).map(|flag| if *negated { !flag } else { flag });
                    output.push(CompiledValue::Bool(value));
                }
                Ok(Arc::new(output))
            }
            Self::InList {
                expr,
                list,
                negated,
            } => {
                let expr = expr.evaluate(batch)?;
                let list = list
                    .iter()
                    .map(|item| item.evaluate(batch))
                    .collect::<Result<Vec<_>>>()?;
                let mut output = Vec::with_capacity(batch.row_count);
                for row_idx in 0..batch.row_count {
                    let value = expr
                        .get(row_idx)
                        .ok_or_else(|| anyhow!("compiled in-list row {row_idx} was missing"))?;
                    if value.is_null() {
                        output.push(CompiledValue::Bool(None));
                        continue;
                    }
                    let mut saw_null = false;
                    let mut matched = false;
                    for item in &list {
                        let item = item.get(row_idx).ok_or_else(|| {
                            anyhow!("compiled in-list row {row_idx} was missing from list item")
                        })?;
                        match value.equals(item)? {
                            Some(true) => {
                                matched = true;
                                break;
                            }
                            Some(false) => {}
                            None => saw_null = true,
                        }
                    }
                    let result = if matched {
                        Some(!negated)
                    } else if saw_null {
                        None
                    } else {
                        Some(*negated)
                    };
                    output.push(CompiledValue::Bool(result));
                }
                Ok(Arc::new(output))
            }
            Self::ConjunctiveRange {
                expr,
                low,
                low_op,
                high,
                high_op,
            } => {
                let expr = expr.evaluate(batch)?;
                let low = low.evaluate(batch)?;
                let high = high.evaluate(batch)?;
                let mut output = Vec::with_capacity(batch.row_count);
                for row_idx in 0..batch.row_count {
                    let value = expr.get(row_idx).ok_or_else(|| {
                        anyhow!("compiled conjunctive range row {row_idx} was missing")
                    })?;
                    let low = low.get(row_idx).ok_or_else(|| {
                        anyhow!("compiled conjunctive range row {row_idx} was missing from low")
                    })?;
                    let high = high.get(row_idx).ok_or_else(|| {
                        anyhow!("compiled conjunctive range row {row_idx} was missing from high")
                    })?;
                    let lower = compare_compiled_values(value, low, *low_op)?;
                    let upper = compare_compiled_values(value, high, *high_op)?;
                    output.push(CompiledValue::Bool(and_bool_opt(lower, upper)));
                }
                Ok(Arc::new(output))
            }
            Self::Case {
                expr,
                when_then_expr,
                else_expr,
                result_type,
            } => {
                let expr = expr.as_ref().map(|expr| expr.evaluate(batch)).transpose()?;
                let when_then_expr = when_then_expr
                    .iter()
                    .enumerate()
                    .map(|(idx, arm)| {
                        Ok::<_, anyhow::Error>((
                            arm.when.evaluate(batch).with_context(|| {
                                format!("evaluate compiled case when arm {idx}")
                            })?,
                            arm.then.evaluate(batch).with_context(|| {
                                format!("evaluate compiled case then arm {idx}")
                            })?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let else_expr = else_expr
                    .as_ref()
                    .map(|expr| expr.evaluate(batch))
                    .transpose()?;
                let mut output = Vec::with_capacity(batch.row_count);
                for row_idx in 0..batch.row_count {
                    let matched = if let Some(expr) = expr.as_ref() {
                        let expr_value = expr.get(row_idx).ok_or_else(|| {
                            anyhow!("compiled case row {row_idx} was missing from base expression")
                        })?;
                        let mut matched = None;
                        for (when, then) in &when_then_expr {
                            let when_value = when.get(row_idx).ok_or_else(|| {
                                anyhow!("compiled case row {row_idx} was missing from when arm")
                            })?;
                            if expr_value.equals(when_value)? == Some(true) {
                                matched = Some(
                                    then.get(row_idx)
                                        .ok_or_else(|| {
                                            anyhow!(
                                                "compiled case row {row_idx} was missing from then arm"
                                            )
                                        })?
                                        .clone(),
                                );
                                break;
                            }
                        }
                        matched
                    } else {
                        let mut matched = None;
                        for (when, then) in &when_then_expr {
                            let when_value = when.get(row_idx).ok_or_else(|| {
                                anyhow!("compiled case row {row_idx} was missing from when arm")
                            })?;
                            if when_value.predicate_truth()? {
                                matched = Some(
                                    then.get(row_idx)
                                        .ok_or_else(|| {
                                            anyhow!(
                                                "compiled case row {row_idx} was missing from then arm"
                                            )
                                        })?
                                        .clone(),
                                );
                                break;
                            }
                        }
                        matched
                    };
                    if let Some(value) = matched {
                        output.push(value);
                    } else if let Some(else_expr) = else_expr.as_ref() {
                        output.push(
                            else_expr
                                .get(row_idx)
                                .ok_or_else(|| {
                                    anyhow!("compiled case row {row_idx} was missing from else arm")
                                })?
                                .clone(),
                        );
                    } else {
                        output.push(CompiledValue::null(*result_type));
                    }
                }
                Ok(Arc::new(output))
            }
            Self::Cast { expr, target_type } => {
                let values = expr.evaluate(batch)?;
                Ok(Arc::new(
                    values
                        .iter()
                        .map(|value| cast_compiled_value(value, *target_type))
                        .collect::<Result<Vec<_>>>()?,
                ))
            }
            Self::ScalarFunction { function, args } => {
                let args = args
                    .iter()
                    .enumerate()
                    .map(|(idx, arg)| {
                        arg.evaluate(batch)
                            .with_context(|| format!("evaluate compiled scalar argument {idx}"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                function.evaluate(&args, batch.row_count)
            }
        }
    }
}

fn evaluate_truthy(
    expr: &CompiledExpr,
    batch: &CompiledInputBatch,
    expected: Option<bool>,
) -> Result<CompiledColumn> {
    let values = expr.evaluate(batch)?;
    Ok(Arc::new(
        values
            .iter()
            .map(|value| Ok(CompiledValue::Bool(Some(value.as_bool_opt()? == expected))))
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn evaluate_not_truthy(
    expr: &CompiledExpr,
    batch: &CompiledInputBatch,
    expected: Option<bool>,
) -> Result<CompiledColumn> {
    let values = expr.evaluate(batch)?;
    Ok(Arc::new(
        values
            .iter()
            .map(|value| {
                let result = match value.as_bool_opt()? {
                    value if value == expected => false,
                    _ => true,
                };
                Ok(CompiledValue::Bool(Some(result)))
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn eval_compiled_binary(
    op: Operator,
    left: &[CompiledValue],
    right: &[CompiledValue],
) -> Result<CompiledColumn> {
    if left.len() != right.len() {
        return Err(anyhow!(
            "compiled binary expression length mismatch: {} vs {}",
            left.len(),
            right.len()
        ));
    }
    Ok(Arc::new(
        left.iter()
            .zip(right.iter())
            .map(|(left, right)| eval_compiled_binary_value(op, left, right))
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn cast_compiled_value(
    value: &CompiledValue,
    target_type: CompiledValueType,
) -> Result<CompiledValue> {
    if value.is_null() {
        return Ok(CompiledValue::null(target_type));
    }
    match (value, target_type) {
        (CompiledValue::Int64(Some(value)), CompiledValueType::Int64) => {
            Ok(CompiledValue::Int64(Some(*value)))
        }
        (CompiledValue::Int64(Some(value)), CompiledValueType::TimestampMillis) => {
            Ok(CompiledValue::TimestampMillis(Some(*value)))
        }
        (CompiledValue::Int64(Some(value)), CompiledValueType::Utf8) => {
            Ok(CompiledValue::Utf8(Some(value.to_string())))
        }
        (CompiledValue::TimestampMillis(Some(value)), CompiledValueType::TimestampMillis) => {
            Ok(CompiledValue::TimestampMillis(Some(*value)))
        }
        (CompiledValue::TimestampMillis(Some(value)), CompiledValueType::Int64) => {
            Ok(CompiledValue::Int64(Some(*value)))
        }
        (CompiledValue::TimestampMillis(Some(value)), CompiledValueType::Utf8) => {
            Ok(CompiledValue::Utf8(Some(value.to_string())))
        }
        (CompiledValue::Utf8(Some(value)), CompiledValueType::Utf8) => {
            Ok(CompiledValue::Utf8(Some(value.clone())))
        }
        (CompiledValue::Utf8(Some(value)), CompiledValueType::Int64) => value
            .parse::<i64>()
            .map(|value| CompiledValue::Int64(Some(value)))
            .map_err(|err| anyhow!("failed to cast Utf8 to Int64: {err}")),
        (CompiledValue::Bool(Some(value)), CompiledValueType::Bool) => {
            Ok(CompiledValue::Bool(Some(*value)))
        }
        (CompiledValue::Bool(Some(value)), CompiledValueType::Utf8) => {
            Ok(CompiledValue::Utf8(Some(value.to_string())))
        }
        (other, target_type) => Err(anyhow!(
            "unsupported compiled cast from {other:?} to {target_type:?}"
        )),
    }
}

fn eval_compiled_binary_value(
    op: Operator,
    left: &CompiledValue,
    right: &CompiledValue,
) -> Result<CompiledValue> {
    match op {
        Operator::Eq => Ok(CompiledValue::Bool(left.equals(right)?)),
        Operator::NotEq => Ok(CompiledValue::Bool(left.equals(right)?.map(|value| !value))),
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => Ok(CompiledValue::Bool(
            compare_compiled_values(left, right, op)?,
        )),
        Operator::And => {
            let lhs = left.as_bool_opt()?;
            let rhs = right.as_bool_opt()?;
            Ok(CompiledValue::Bool(and_bool_opt(lhs, rhs)))
        }
        Operator::Or => {
            let lhs = left.as_bool_opt()?;
            let rhs = right.as_bool_opt()?;
            Ok(CompiledValue::Bool(or_bool_opt(lhs, rhs)))
        }
        Operator::Plus
        | Operator::Minus
        | Operator::Multiply
        | Operator::Divide
        | Operator::Modulo => {
            let Some(lhs) = left.as_i64_opt("compiled arithmetic")? else {
                return Ok(CompiledValue::Int64(None));
            };
            let Some(rhs) = right.as_i64_opt("compiled arithmetic")? else {
                return Ok(CompiledValue::Int64(None));
            };
            let value = match op {
                Operator::Plus => lhs + rhs,
                Operator::Minus => lhs - rhs,
                Operator::Multiply => lhs * rhs,
                Operator::Divide => {
                    if rhs == 0 {
                        return Err(anyhow!("division by zero in compiled expression"));
                    }
                    lhs / rhs
                }
                Operator::Modulo => {
                    if rhs == 0 {
                        return Err(anyhow!("modulo by zero in compiled expression"));
                    }
                    lhs % rhs
                }
                _ => unreachable!(),
            };
            Ok(CompiledValue::Int64(Some(value)))
        }
        Operator::StringConcat => match (left, right) {
            (CompiledValue::Utf8(Some(left)), CompiledValue::Utf8(Some(right))) => {
                Ok(CompiledValue::Utf8(Some(format!("{left}{right}"))))
            }
            (CompiledValue::Utf8(None), _) | (_, CompiledValue::Utf8(None)) => {
                Ok(CompiledValue::Utf8(None))
            }
            _ => Err(anyhow!(
                "compiled string concat expects Utf8 operands: {left:?} vs {right:?}"
            )),
        },
        _ => Err(anyhow!("unsupported compiled binary operator {op:?}")),
    }
}

fn compare_compiled_values(
    left: &CompiledValue,
    right: &CompiledValue,
    op: Operator,
) -> Result<Option<bool>> {
    let Some(ordering) = left.compare(right)? else {
        return Ok(None);
    };
    let result = match op {
        Operator::Lt => ordering.is_lt(),
        Operator::LtEq => ordering.is_le(),
        Operator::Gt => ordering.is_gt(),
        Operator::GtEq => ordering.is_ge(),
        _ => return Err(anyhow!("unsupported compiled comparison operator {op:?}")),
    };
    Ok(Some(result))
}

fn extract_range_comparison(expr: CompiledExpr) -> Option<(CompiledExpr, CompiledExpr, Operator)> {
    let CompiledExpr::Binary { op, left, right } = expr else {
        return None;
    };
    if !matches!(
        op,
        Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
    ) {
        return None;
    }
    Some((left.as_ref().clone(), right.as_ref().clone(), op))
}

fn and_bool_opt(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(false), _) => Some(false),
        (Some(true), other) => other,
        (None, Some(false)) => Some(false),
        (None, Some(true)) => None,
        (None, None) => None,
    }
}

fn or_bool_opt(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) => Some(true),
        (Some(false), other) => other,
        (None, Some(true)) => Some(true),
        (None, Some(false)) => None,
        (None, None) => None,
    }
}

fn translate_date_format_pattern(pattern: &str) -> String {
    pattern
        .replace("yyyy", "%Y")
        .replace("MM", "%m")
        .replace("dd", "%d")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("ss", "%S")
}

fn split_index_value(text: &str, delimiter: &str, index: i64) -> Option<String> {
    if index < 0 || delimiter.is_empty() {
        return None;
    }
    text.split(delimiter)
        .nth(index as usize)
        .map(str::to_string)
}

fn resolve_compiled_column_index(
    input_schema: &datafusion::arrow::datatypes::Schema,
    column: &Column,
) -> Result<usize> {
    let qualified = column.flat_name();
    input_schema
        .fields()
        .iter()
        .enumerate()
        .find_map(|(idx, field)| {
            (field.name() == &qualified || field.name() == &column.name).then_some(idx)
        })
        .ok_or_else(|| {
            anyhow!(
                "column {} not found in vectorized input schema",
                column.name
            )
        })
}

fn column_projection_indices(
    projections: &[DbspProjectExpr],
    input_schema: &RowSchema,
) -> Option<Vec<usize>> {
    projections
        .iter()
        .map(|projection| {
            let column = match projection.expression().expr() {
                datafusion::logical_expr::Expr::Column(column) => column,
                datafusion::logical_expr::Expr::Alias(alias) => match alias.expr.as_ref() {
                    datafusion::logical_expr::Expr::Column(column) => column,
                    _ => return None,
                },
                _ => return None,
            };
            resolve_input_schema_column_index(input_schema, column)
        })
        .collect::<Option<Vec<_>>>()
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

fn build_decoded_input_slots(input_width: usize, required_columns: &[usize]) -> Vec<Option<usize>> {
    let mut slots = vec![None; input_width];
    for (slot, column_idx) in required_columns.iter().copied().enumerate() {
        slots[column_idx] = Some(slot);
    }
    slots
}

fn build_decoded_input_layout(
    input_schema: &RowSchema,
    required_columns: &[usize],
) -> Result<DecodedInputLayout> {
    Ok(DecodedInputLayout {
        slots: Arc::new(build_decoded_input_slots(
            input_schema.len(),
            required_columns,
        )),
        value_types: Arc::new(build_decoded_input_value_types(
            input_schema,
            required_columns,
        )?),
        count: required_columns.len(),
    })
}

fn build_decoded_input_value_types(
    input_schema: &RowSchema,
    required_columns: &[usize],
) -> Result<Vec<CompiledValueType>> {
    required_columns
        .iter()
        .map(|column_idx| {
            let field = input_schema
                .field(*column_idx)
                .ok_or_else(|| anyhow!("vectorized input column {column_idx} was out of bounds"))?;
            Ok(match field.data_type {
                dbsp::circuit::types::DbspScalarType::Int64 => CompiledValueType::Int64,
                dbsp::circuit::types::DbspScalarType::Utf8 => CompiledValueType::Utf8,
                dbsp::circuit::types::DbspScalarType::TimestampMillis => {
                    CompiledValueType::TimestampMillis
                }
                dbsp::circuit::types::DbspScalarType::Bool => CompiledValueType::Bool,
            })
        })
        .collect()
}

fn build_selected_delta_values(
    prepared: &PreparedEncodedInput,
    selected: &[usize],
) -> Vec<(Vec<u8>, i64)> {
    let mut rows = Vec::with_capacity(selected.len());
    for idx in selected {
        let diff = prepared.weights.get(*idx).copied().unwrap_or(0);
        if diff == 0 {
            continue;
        }
        let Some(encoded) = prepared.encoded_rows.get(*idx).cloned() else {
            continue;
        };
        rows.push((encoded, diff));
    }
    rows
}

fn build_compiled_input_batch(
    input_width: usize,
    decoded_input_slots: &[Option<usize>],
    mut decoded_columns: Vec<Vec<CompiledValue>>,
    row_count: usize,
) -> CompiledInputBatch {
    let mut columns = vec![None; input_width];
    for (input_idx, slot) in decoded_input_slots.iter().copied().enumerate() {
        if let Some(slot) = slot {
            columns[input_idx] = Some(Arc::new(std::mem::take(&mut decoded_columns[slot])));
        }
    }
    CompiledInputBatch { columns, row_count }
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

fn decode_encoded_field_as_encoded_scalar(
    field: &[u8],
    data_type: CompiledValueType,
) -> Result<Option<EncodedRowScalar>> {
    let tag = *field
        .first()
        .ok_or_else(|| anyhow!("encoded field must contain a tag"))?;
    match (data_type, tag) {
        (_, 0x00) => Ok(None),
        (CompiledValueType::Int64, 0x01) => {
            let chunk = field.get(1..9).ok_or_else(|| anyhow!("truncated int64"))?;
            Ok(Some(EncodedRowScalar::Int64(i64::from_le_bytes(
                chunk.try_into().unwrap(),
            ))))
        }
        (CompiledValueType::Utf8, 0x02) => {
            let len_bytes = field
                .get(1..5)
                .ok_or_else(|| anyhow!("truncated string length"))?;
            let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            let chunk = field
                .get(5..5 + len)
                .ok_or_else(|| anyhow!("truncated string payload"))?;
            let text =
                std::str::from_utf8(chunk).map_err(|err| anyhow!("utf8 decode error: {err}"))?;
            Ok(Some(EncodedRowScalar::Utf8(text.to_string())))
        }
        (CompiledValueType::TimestampMillis, 0x03) => {
            let chunk = field
                .get(1..9)
                .ok_or_else(|| anyhow!("truncated timestamp"))?;
            Ok(Some(EncodedRowScalar::TimestampMillis(i64::from_le_bytes(
                chunk.try_into().unwrap(),
            ))))
        }
        (CompiledValueType::Bool, 0x04) => {
            let flag = *field
                .get(1)
                .ok_or_else(|| anyhow!("missing boolean payload"))?;
            Ok(Some(EncodedRowScalar::Bool(flag != 0)))
        }
        (CompiledValueType::Int64, 0x05)
        | (CompiledValueType::Utf8, 0x06)
        | (CompiledValueType::TimestampMillis, 0x07)
        | (CompiledValueType::Bool, 0x08) => Ok(None),
        _ => Err(anyhow!(
            "encoded field tag {tag:#x} did not match compiled type {data_type:?}"
        )),
    }
}

fn compiled_value_from_encoded_scalar(
    value: Option<&EncodedRowScalar>,
    data_type: CompiledValueType,
) -> Result<CompiledValue> {
    match (data_type, value) {
        (_, None) => Ok(CompiledValue::null(data_type)),
        (CompiledValueType::Int64, Some(EncodedRowScalar::Int64(value))) => {
            Ok(CompiledValue::Int64(Some(*value)))
        }
        (CompiledValueType::Utf8, Some(EncodedRowScalar::Utf8(value))) => {
            Ok(CompiledValue::Utf8(Some(value.clone())))
        }
        (CompiledValueType::TimestampMillis, Some(EncodedRowScalar::TimestampMillis(value))) => {
            Ok(CompiledValue::TimestampMillis(Some(*value)))
        }
        (CompiledValueType::Bool, Some(EncodedRowScalar::Bool(value))) => {
            Ok(CompiledValue::Bool(Some(*value)))
        }
        (_, Some(other)) => Err(anyhow!(
            "encoded scalar {other:?} did not match compiled type {data_type:?}"
        )),
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
    compiled_batch: Option<CompiledInputBatch>,
    encoded_rows: Vec<Vec<u8>>,
    weights: Vec<i64>,
    projected_ranges: Option<Vec<Vec<Range<usize>>>>,
}

struct DecodedEncodedRow {
    compiled_values: Option<Vec<CompiledValue>>,
    projected_ranges: Option<Vec<Range<usize>>>,
}

fn encode_compiled_projection_row(columns: &[CompiledColumn], row_idx: usize) -> Result<Vec<u8>> {
    let count = u32::try_from(columns.len()).map_err(|_| anyhow!("too many columns in MV key"))?;
    let mut encoded = Vec::with_capacity(4 + (columns.len() * 9));
    encoded.extend_from_slice(&count.to_le_bytes());
    for column in columns {
        let value = column
            .get(row_idx)
            .ok_or_else(|| anyhow!("compiled projection row {row_idx} was missing"))?;
        encode_compiled_value(value, &mut encoded)?;
    }
    Ok(encoded)
}

fn encode_compiled_value(value: &CompiledValue, encoded: &mut Vec<u8>) -> Result<()> {
    match value {
        CompiledValue::Int64(Some(v)) => {
            encoded.push(0x01);
            encoded.extend_from_slice(&v.to_le_bytes());
        }
        CompiledValue::Int64(None) => encoded.push(0x05),
        CompiledValue::Utf8(Some(text)) => {
            encoded.push(0x02);
            let bytes = text.as_bytes();
            let len = u32::try_from(bytes.len())
                .map_err(|_| anyhow!("utf8 value too large for MV key"))?;
            encoded.extend_from_slice(&len.to_le_bytes());
            encoded.extend_from_slice(bytes);
        }
        CompiledValue::Utf8(None) => encoded.push(0x06),
        CompiledValue::TimestampMillis(Some(v)) => {
            encoded.push(0x03);
            encoded.extend_from_slice(&v.to_le_bytes());
        }
        CompiledValue::TimestampMillis(None) => encoded.push(0x07),
        CompiledValue::Bool(Some(flag)) => {
            encoded.push(0x04);
            encoded.push(if *flag { 1 } else { 0 });
        }
        CompiledValue::Bool(None) => encoded.push(0x08),
    }
    Ok(())
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
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::common::{Column, ScalarValue};
    use datafusion::error::Result as DataFusionResult;
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::logical_expr::{
        BinaryExpr, Case, ScalarFunctionImplementation, ScalarUDF, Volatility, create_udf,
    };
    use datafusion::physical_plan::ColumnarValue;

    fn encode_test_row(columns: &[Option<EncodedRowScalar>]) -> Vec<u8> {
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
                    let bytes = value.as_bytes();
                    encoded.push(0x02);
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
            }
        }
        encoded
    }

    fn null_i64_value(len: usize) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(Int64Array::from(vec![None::<i64>; len])))
    }

    fn create_test_udf(
        name: &str,
        arg_types: Vec<DataType>,
        return_type: DataType,
    ) -> Arc<ScalarUDF> {
        let return_type_for_impl = return_type.clone();
        let implementation: ScalarFunctionImplementation = Arc::new(
            move |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
                let len = args
                    .iter()
                    .find_map(|arg| match arg {
                        ColumnarValue::Array(array) => Some(array.len()),
                        ColumnarValue::Scalar(_) => None,
                    })
                    .unwrap_or(1);
                Ok(match return_type_for_impl {
                    DataType::Int64 => null_i64_value(len),
                    _ => unreachable!("test UDF only supports Int64 output"),
                })
            },
        );
        Arc::new(create_udf(
            name,
            arg_types,
            return_type,
            Volatility::Immutable,
            implementation,
        ))
    }

    #[test]
    fn compiles_q14_case_count_char_and_range_predicate_expressions() {
        let input_schema = Schema::new(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("price", DataType::Int64, true),
            Field::new(
                "date_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("extra", DataType::Utf8, true),
        ]);
        let df_schema = DFSchema::try_from(input_schema.clone()).expect("df schema");
        let hour_udf = create_test_udf(
            "hour",
            vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
            DataType::Int64,
        );
        let count_char_udf = create_test_udf(
            "count_char",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Int64,
        );

        let hour_expr = |column_name: &str| {
            Expr::ScalarFunction(ScalarFunction::new_udf(
                Arc::clone(&hour_udf),
                vec![Expr::Column(Column::from_name(column_name))],
            ))
        };
        let price_expr = || {
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(Expr::Column(Column::from_name("price"))),
                    Operator::Multiply,
                    Box::new(Expr::Literal(ScalarValue::Int64(Some(908)), None)),
                ))),
                Operator::Divide,
                Box::new(Expr::Literal(ScalarValue::Int64(Some(1000)), None)),
            ))
        };
        let int_lit = |value| Expr::Literal(ScalarValue::Int64(Some(value)), None);
        let str_lit = |value: &str| Expr::Literal(ScalarValue::Utf8(Some(value.to_string())), None);

        let q14_case = Expr::Case(Case::new(
            None,
            vec![
                (
                    Box::new(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(hour_expr("date_time")),
                            Operator::GtEq,
                            Box::new(int_lit(8)),
                        ))),
                        Operator::And,
                        Box::new(Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(hour_expr("date_time")),
                            Operator::LtEq,
                            Box::new(int_lit(18)),
                        ))),
                    ))),
                    Box::new(str_lit("dayTime")),
                ),
                (
                    Box::new(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(hour_expr("date_time")),
                            Operator::LtEq,
                            Box::new(int_lit(6)),
                        ))),
                        Operator::Or,
                        Box::new(Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(hour_expr("date_time")),
                            Operator::GtEq,
                            Box::new(int_lit(20)),
                        ))),
                    ))),
                    Box::new(str_lit("nightTime")),
                ),
            ],
            Some(Box::new(str_lit("otherTime"))),
        ));
        let count_char = Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::clone(&count_char_udf),
            vec![
                Expr::Column(Column::from_name("extra")),
                Expr::Literal(ScalarValue::Utf8(Some("c".to_string())), None),
            ],
        ));
        let q14_predicate = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(price_expr()),
                Operator::Gt,
                Box::new(int_lit(1_000_000)),
            ))),
            Operator::And,
            Box::new(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(price_expr()),
                Operator::Lt,
                Box::new(int_lit(50_000_000)),
            ))),
        ));

        assert!(
            CompiledExpr::try_compile(&q14_case, &df_schema, &input_schema)
                .expect("compile q14 case")
                .is_some()
        );
        assert!(
            CompiledExpr::try_compile(&count_char, &df_schema, &input_schema)
                .expect("compile q14 count_char")
                .is_some()
        );
        assert!(matches!(
            CompiledExpr::try_compile(&q14_predicate, &df_schema, &input_schema)
                .expect("compile q14 predicate"),
            Some(CompiledExpr::ConjunctiveRange { .. })
        ));
    }

    #[test]
    fn encoded_predicate_plan_supports_q14_conjunctive_range_filter() {
        let input_schema = Schema::new(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("price", DataType::Int64, true),
            Field::new(
                "date_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("extra", DataType::Utf8, true),
        ]);
        let df_schema = DFSchema::try_from(input_schema.clone()).expect("df schema");
        let int_lit = |value| Expr::Literal(ScalarValue::Int64(Some(value)), None);
        let price_expr = || {
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(Expr::Column(Column::from_name("price"))),
                    Operator::Multiply,
                    Box::new(Expr::Literal(ScalarValue::Int64(Some(908)), None)),
                ))),
                Operator::Divide,
                Box::new(Expr::Literal(ScalarValue::Int64(Some(1000)), None)),
            ))
        };
        let q14_predicate = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(price_expr()),
                Operator::Gt,
                Box::new(int_lit(1_000_000)),
            ))),
            Operator::And,
            Box::new(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(price_expr()),
                Operator::Lt,
                Box::new(int_lit(50_000_000)),
            ))),
        ));
        let compiled = CompiledExpr::try_compile(&q14_predicate, &df_schema, &input_schema)
            .expect("compile q14 predicate")
            .expect("compiled q14 predicate");
        let encoded_predicate = EncodedPredicatePlan::try_from_compiled(&compiled, &input_schema)
            .expect("build encoded q14 predicate");

        let passing = encode_test_row(&[
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(2)),
            Some(EncodedRowScalar::Int64(2_000_000)),
            Some(EncodedRowScalar::TimestampMillis(0)),
            Some(EncodedRowScalar::Utf8("ccc".to_string())),
        ]);
        let failing = encode_test_row(&[
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(2)),
            Some(EncodedRowScalar::Int64(500_000)),
            Some(EncodedRowScalar::TimestampMillis(0)),
            Some(EncodedRowScalar::Utf8("ccc".to_string())),
        ]);

        assert!(
            encoded_predicate
                .matches_row(passing.as_slice())
                .expect("evaluate passing row")
        );
        assert!(
            !encoded_predicate
                .matches_row(failing.as_slice())
                .expect("evaluate failing row")
        );
    }
}

#[cfg(test)]
mod helper_tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema};
    use dbsp::circuit::schema::Field as DbspField;
    use dbsp::circuit::types::DbspScalarType;

    fn dbsp_schema() -> Arc<RowSchema> {
        RowSchema::try_new(vec![
            DbspField::new("a", DbspScalarType::Int64, true),
            DbspField::new("b", DbspScalarType::Utf8, true),
            DbspField::new("c", DbspScalarType::TimestampMillis, true),
            DbspField::new("d", DbspScalarType::Bool, true),
        ])
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
                    let bytes = value.as_bytes();
                    encoded.push(0x02);
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
            }
        }
        encoded
    }

    #[test]
    fn comparison_and_boolean_helpers_cover_edges() {
        assert_eq!(invert_comparison_operator(Operator::Eq), Some(Operator::Eq));
        assert_eq!(invert_comparison_operator(Operator::Lt), Some(Operator::Gt));
        assert_eq!(
            invert_comparison_operator(Operator::GtEq),
            Some(Operator::LtEq)
        );
        assert_eq!(invert_comparison_operator(Operator::And), None);

        assert_eq!(compare_i64(10, 10, Operator::Eq).expect("eq"), true);
        assert_eq!(compare_i64(10, 11, Operator::Lt).expect("lt"), true);
        assert_eq!(compare_i64(11, 10, Operator::Gt).expect("gt"), true);
        assert!(compare_i64(1, 2, Operator::And).is_err());

        assert_eq!(and_bool_opt(Some(true), Some(false)), Some(false));
        assert_eq!(and_bool_opt(Some(true), None), None);
        assert_eq!(and_bool_opt(None, Some(false)), Some(false));

        assert_eq!(or_bool_opt(Some(false), Some(true)), Some(true));
        assert_eq!(or_bool_opt(Some(false), None), None);
        assert_eq!(or_bool_opt(None, Some(true)), Some(true));
    }

    #[test]
    fn eval_compiled_binary_covers_arithmetic_and_concat_paths() {
        let left = vec![CompiledValue::Int64(Some(7)), CompiledValue::Int64(None)];
        let right = vec![CompiledValue::Int64(Some(2)), CompiledValue::Int64(Some(3))];

        let plus = eval_compiled_binary(Operator::Plus, &left, &right).expect("plus");
        assert_eq!(
            plus.as_ref(),
            &vec![CompiledValue::Int64(Some(9)), CompiledValue::Int64(None)]
        );

        assert!(eval_compiled_binary(Operator::Eq, &left[..1], &right).is_err());
        assert!(
            eval_compiled_binary_value(
                Operator::Divide,
                &CompiledValue::Int64(Some(9)),
                &CompiledValue::Int64(Some(0))
            )
            .is_err()
        );

        assert_eq!(
            eval_compiled_binary_value(
                Operator::StringConcat,
                &CompiledValue::Utf8(Some("a".to_string())),
                &CompiledValue::Utf8(Some("b".to_string()))
            )
            .expect("concat"),
            CompiledValue::Utf8(Some("ab".to_string()))
        );

        assert_eq!(
            eval_compiled_binary_value(
                Operator::StringConcat,
                &CompiledValue::Utf8(None),
                &CompiledValue::Utf8(Some("x".to_string()))
            )
            .expect("concat null"),
            CompiledValue::Utf8(None)
        );
    }

    #[test]
    fn encoded_field_decode_and_projection_helpers_round_trip() {
        let encoded = encode_row(&[
            Some(EncodedRowScalar::Int64(10)),
            Some(EncodedRowScalar::Utf8("x".to_string())),
            Some(EncodedRowScalar::Bool(true)),
            Some(EncodedRowScalar::TimestampMillis(50)),
        ]);

        let mut cursor = 4usize;
        let mut ranges = Vec::new();
        for _ in 0..4 {
            let end = encoded_field_end(&encoded, cursor).expect("field end");
            ranges.push(cursor..end);
            cursor = end;
        }
        assert_eq!(cursor, encoded.len());

        assert_eq!(
            decode_encoded_field_as_encoded_scalar(
                &encoded[ranges[0].clone()],
                CompiledValueType::Int64
            )
            .expect("decode int"),
            Some(EncodedRowScalar::Int64(10))
        );
        assert_eq!(
            decode_encoded_field_as_encoded_scalar(
                &encoded[ranges[1].clone()],
                CompiledValueType::Utf8
            )
            .expect("decode utf8"),
            Some(EncodedRowScalar::Utf8("x".to_string()))
        );

        let projected = project_encoded_row(
            &encoded,
            &[ranges[2].clone(), ranges[0].clone(), ranges[3].clone()],
            3,
        )
        .expect("project");
        assert_eq!(
            crate::encoding::extract_encoded_row_scalars(&projected, &[0, 1, 2])
                .expect("decode projected"),
            vec![
                Some(EncodedRowScalar::Bool(true)),
                Some(EncodedRowScalar::Int64(10)),
                Some(EncodedRowScalar::TimestampMillis(50)),
            ]
        );

        assert!(encoded_field_end(&[0x09], 0).is_err());
        assert!(project_encoded_row(&encoded, &[999..1000], 1).is_err());
    }

    #[test]
    fn layout_and_compiled_batch_helpers_cover_sparse_decode() {
        let schema = dbsp_schema();
        let required = vec![3usize, 0usize];
        let slots = build_decoded_input_slots(schema.len(), &required);
        assert_eq!(slots, vec![Some(1), None, None, Some(0)]);

        let layout = build_decoded_input_layout(schema.as_ref(), &required).expect("layout");
        assert_eq!(layout.count, 2);
        assert_eq!(
            layout.value_types.as_ref(),
            &[CompiledValueType::Bool, CompiledValueType::Int64]
        );

        let batch = build_compiled_input_batch(
            schema.len(),
            &slots,
            vec![
                vec![CompiledValue::Bool(Some(true)), CompiledValue::Bool(None)],
                vec![
                    CompiledValue::Int64(Some(10)),
                    CompiledValue::Int64(Some(20)),
                ],
            ],
            2,
        );

        assert_eq!(
            batch.column(0).expect("a column").as_ref(),
            &[
                CompiledValue::Int64(Some(10)),
                CompiledValue::Int64(Some(20)),
            ]
        );
        assert!(batch.column(1).is_err());
        assert_eq!(
            batch.column(3).expect("d column").as_ref(),
            &[CompiledValue::Bool(Some(true)), CompiledValue::Bool(None)]
        );

        assert!(build_decoded_input_value_types(schema.as_ref(), &[99]).is_err());
    }

    #[test]
    fn compiled_projection_encoder_round_trip() {
        assert_eq!(
            compiled_value_from_encoded_scalar(
                Some(&EncodedRowScalar::Int64(3)),
                CompiledValueType::Int64
            )
            .expect("compiled int"),
            CompiledValue::Int64(Some(3))
        );
        assert!(
            compiled_value_from_encoded_scalar(
                Some(&EncodedRowScalar::Utf8("x".to_string())),
                CompiledValueType::Int64,
            )
            .is_err()
        );

        let compiled_columns: Vec<CompiledColumn> = vec![
            Arc::new(vec![
                CompiledValue::Int64(Some(11)),
                CompiledValue::Int64(None),
            ]),
            Arc::new(vec![
                CompiledValue::Utf8(Some("z".to_string())),
                CompiledValue::Utf8(None),
            ]),
            Arc::new(vec![
                CompiledValue::TimestampMillis(Some(77)),
                CompiledValue::TimestampMillis(None),
            ]),
            Arc::new(vec![
                CompiledValue::Bool(Some(true)),
                CompiledValue::Bool(None),
            ]),
        ];
        let encoded0 = encode_compiled_projection_row(&compiled_columns, 0).expect("row 0");
        let encoded1 = encode_compiled_projection_row(&compiled_columns, 1).expect("row 1");

        assert_eq!(
            crate::encoding::extract_encoded_row_scalars(&encoded0, &[0, 1, 2, 3])
                .expect("decode compiled row 0"),
            vec![
                Some(EncodedRowScalar::Int64(11)),
                Some(EncodedRowScalar::Utf8("z".to_string())),
                Some(EncodedRowScalar::TimestampMillis(77)),
                Some(EncodedRowScalar::Bool(true)),
            ]
        );
        assert_eq!(
            crate::encoding::extract_encoded_row_scalars(&encoded1, &[0, 1, 2, 3])
                .expect("decode compiled row 1"),
            vec![None, None, None, None]
        );
    }

    #[test]
    fn selection_and_consolidation_helpers_skip_zero_weights() {
        let row_a = encode_row(&[Some(EncodedRowScalar::Int64(1))]);
        let row_b = encode_row(&[Some(EncodedRowScalar::Int64(2))]);

        let prepared = PreparedEncodedInput {
            compiled_batch: None,
            encoded_rows: vec![row_a.clone(), row_b.clone()],
            weights: vec![0, 3],
            projected_ranges: None,
        };
        assert_eq!(
            build_selected_delta_values(&prepared, &[0, 1, 2]),
            vec![(row_b.clone(), 3)]
        );

        let consolidated = consolidate_encoded_delta_batch(vec![
            (row_a.clone(), 1),
            (row_a, -1),
            (row_b.clone(), 2),
            (row_b.clone(), 3),
            (row_b.clone(), -1),
            (row_b.clone(), 0),
        ])
        .expect("consolidate");
        assert_eq!(consolidated, vec![(row_b, 4)]);
    }

    #[test]
    fn column_resolution_helpers_match_name_and_qualified_name() {
        let arrow_schema = Schema::new(vec![
            ArrowField::new("a", DataType::Int64, true),
            ArrowField::new("t.b", DataType::Int64, true),
        ]);
        assert_eq!(
            resolve_compiled_column_index(&arrow_schema, &Column::from_name("a")).expect("a"),
            0
        );
        assert_eq!(
            resolve_compiled_column_index(&arrow_schema, &Column::from_qualified_name("t.b"))
                .expect("qualified b"),
            1
        );
        assert!(
            resolve_compiled_column_index(&arrow_schema, &Column::from_name("missing")).is_err()
        );

        let schema = dbsp_schema();
        assert_eq!(
            resolve_input_schema_column_index(schema.as_ref(), &Column::from_name("a")),
            Some(0)
        );
        assert_eq!(
            resolve_input_schema_column_index(schema.as_ref(), &Column::from_name("missing")),
            None
        );
    }

    #[test]
    fn compiled_input_batch_builder_places_columns_by_input_slot() {
        let batch = build_compiled_input_batch(
            4,
            &[Some(1), None, Some(0), None],
            vec![
                vec![CompiledValue::Bool(Some(true)), CompiledValue::Bool(None)],
                vec![
                    CompiledValue::Int64(Some(9)),
                    CompiledValue::Int64(Some(10)),
                ],
            ],
            2,
        );

        assert_eq!(batch.row_count, 2);
        assert!(batch.columns[1].is_none());
        assert!(batch.columns[3].is_none());

        assert_eq!(
            batch.columns[0].as_ref().expect("col0").as_ref(),
            &vec![
                CompiledValue::Int64(Some(9)),
                CompiledValue::Int64(Some(10))
            ]
        );
        assert_eq!(
            batch.columns[2].as_ref().expect("col2").as_ref(),
            &vec![CompiledValue::Bool(Some(true)), CompiledValue::Bool(None)]
        );
    }
}
