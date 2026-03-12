use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use datafusion::arrow::array::builder::BinaryDictionaryBuilder;
use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, BooleanArray};
use datafusion::arrow::datatypes::Int32Type;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::execution::context::SessionContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;
use dbsp::circuit::plan::DbspProjectExpr;
use dbsp::{DbspPredicate, RowSchema};

use crate::encoding::{decode_projected_row_key, encode_projected_row_key};

#[derive(Clone)]
pub(crate) struct VectorizedFilterProjectEvaluator {
    input_schema: datafusion::arrow::datatypes::SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    projection_plan: ProjectionPlan,
}

impl VectorizedFilterProjectEvaluator {
    pub(crate) fn for_filter_map(
        predicate: &DbspPredicate,
        projections: &[DbspProjectExpr],
        input_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        let column_projection = column_projection_indices(projections, input_schema.as_ref());
        let input_schema = input_schema.to_arrow_schema();
        let df_schema = DFSchema::try_from(input_schema.as_ref().clone())
            .context("build DataFusion schema for vectorized filter_map")?;
        let ctx = SessionContext::new();
        let predicate = ctx
            .create_physical_expr(predicate.expression().expr().clone(), &df_schema)
            .context("compile vectorized predicate expression")?;
        let projection_plan = if let Some(indices) = column_projection {
            ProjectionPlan::ColumnIndices(Arc::new(indices))
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
        })
    }

    pub(crate) fn for_filter(
        predicate: &DbspPredicate,
        input_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        let projections = (0..input_schema.len()).collect::<Vec<_>>();
        let input_schema = input_schema.to_arrow_schema();
        let df_schema = DFSchema::try_from(input_schema.as_ref().clone())
            .context("build DataFusion schema for vectorized filter")?;
        let ctx = SessionContext::new();
        let predicate = ctx
            .create_physical_expr(predicate.expression().expr().clone(), &df_schema)
            .context("compile vectorized filter predicate expression")?;
        Ok(Self {
            input_schema,
            predicate: Some(predicate),
            projection_plan: ProjectionPlan::ColumnIndices(Arc::new(projections)),
        })
    }

    pub(crate) fn for_map(
        projections: &[DbspProjectExpr],
        input_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        let column_projection = column_projection_indices(projections, input_schema.as_ref());
        let input_schema = input_schema.to_arrow_schema();
        let df_schema = DFSchema::try_from(input_schema.as_ref().clone())
            .context("build DataFusion schema for vectorized map")?;
        let ctx = SessionContext::new();
        let projection_plan = if let Some(indices) = column_projection {
            ProjectionPlan::ColumnIndices(Arc::new(indices))
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

        let mut rows = Vec::with_capacity(delta_values.len());
        let mut weights = Vec::with_capacity(delta_values.len());
        let mut encoded_rows = identity_projection.then(|| Vec::with_capacity(delta_values.len()));
        for (encoded, weight) in delta_values {
            if weight == 0 {
                continue;
            }
            match decode_projected_row_key(&encoded) {
                Ok(row) => {
                    rows.push(row);
                    weights.push(weight);
                    if let Some(encoded_rows) = encoded_rows.as_mut() {
                        encoded_rows.push(encoded);
                    }
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
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        match &self.projection_plan {
            ProjectionPlan::ColumnIndices(indices) => {
                let batch = rows_to_record_batch_ref(&self.input_schema, &rows)?;
                let selected = self.selected_indices(&batch)?;
                if selected.is_empty() {
                    return Ok(Vec::new());
                }
                let mut staged = Vec::with_capacity(selected.len());
                if identity_projection {
                    let Some(encoded_rows) = encoded_rows.as_ref() else {
                        return Ok(Vec::new());
                    };
                    for idx in selected {
                        let diff = weights.get(idx).copied().unwrap_or(0);
                        if diff == 0 {
                            continue;
                        }
                        let Some(encoded) = encoded_rows.get(idx).cloned() else {
                            continue;
                        };
                        staged.push((encoded, diff));
                    }
                    return consolidate_encoded_delta_batch(staged);
                }
                for idx in selected {
                    let diff = weights.get(idx).copied().unwrap_or(0);
                    if diff == 0 {
                        continue;
                    }
                    let Some(row) = rows.get(idx) else {
                        continue;
                    };
                    let projected_row = indices
                        .iter()
                        .map(|column_idx| row[*column_idx].clone())
                        .collect::<Vec<_>>();
                    let encoded = encode_projected_row_key(&projected_row)?;
                    staged.push((encoded, diff));
                }
                consolidate_encoded_delta_batch(staged)
            }
            ProjectionPlan::Physical(projections) => {
                let batch = rows_to_record_batch_owned(&self.input_schema, rows)?;
                let selected = self.selected_indices(&batch)?;
                if selected.is_empty() {
                    return Ok(Vec::new());
                }
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
                    let diff = weights.get(idx).copied().unwrap_or(0);
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
    ColumnIndices(Arc<Vec<usize>>),
    Physical(Arc<Vec<Arc<dyn PhysicalExpr>>>),
}

impl ProjectionPlan {
    fn is_identity(&self, input_width: usize) -> bool {
        match self {
            Self::ColumnIndices(indices) => {
                indices.len() == input_width
                    && indices.iter().enumerate().all(|(idx, col)| idx == *col)
            }
            Self::Physical(_) => false,
        }
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

fn rows_to_record_batch_ref(
    schema: &datafusion::arrow::datatypes::SchemaRef,
    rows: &[Vec<ScalarValue>],
) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    let width = schema.fields().len();
    let mut columns = vec![Vec::with_capacity(rows.len()); width];
    for row in rows {
        if row.len() != width {
            return Err(anyhow!(
                "row has {} columns but schema has {}",
                row.len(),
                width
            ));
        }
        for (idx, value) in row.into_iter().enumerate() {
            columns[idx].push(value.clone());
        }
    }
    let arrays = columns
        .into_iter()
        .enumerate()
        .map(|(idx, values)| {
            ScalarValue::iter_to_array(values)
                .with_context(|| format!("build vectorized input column {idx}"))
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::clone(schema), arrays).context("build vectorized input batch")
}

fn rows_to_record_batch_owned(
    schema: &datafusion::arrow::datatypes::SchemaRef,
    rows: Vec<Vec<ScalarValue>>,
) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    let width = schema.fields().len();
    let mut columns = vec![Vec::with_capacity(rows.len()); width];
    for row in rows {
        if row.len() != width {
            return Err(anyhow!(
                "row has {} columns but schema has {}",
                row.len(),
                width
            ));
        }
        for (idx, value) in row.into_iter().enumerate() {
            columns[idx].push(value);
        }
    }
    let arrays: Vec<ArrayRef> = columns
        .into_iter()
        .enumerate()
        .map(|(idx, values)| {
            ScalarValue::iter_to_array(values)
                .with_context(|| format!("build vectorized input column {idx}"))
        })
        .collect::<Result<_>>()?;
    RecordBatch::try_new(Arc::clone(schema), arrays).context("build vectorized input batch")
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

pub(crate) fn vectorized_filter_map_enabled() -> bool {
    std::env::var("FLOE_VECTORIZED_FILTER_MAP")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "FALSE" | "False"))
        .unwrap_or(true)
}
