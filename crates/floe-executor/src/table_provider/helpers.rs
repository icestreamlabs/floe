use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::scalar::ScalarValue;

use crate::stream_types::{Diff, Row};

use super::MV_VERSION_COLUMN;

pub(super) fn build_i64_batches(
    rows: Vec<Vec<i64>>,
    schema: SchemaRef,
) -> Result<Vec<RecordBatch>> {
    if rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }

    let column_count = schema.fields().len();
    let mut columns: Vec<Vec<i64>> = vec![Vec::with_capacity(rows.len()); column_count];

    for row in rows {
        for (idx, value) in row.into_iter().enumerate() {
            if let Some(column) = columns.get_mut(idx) {
                column.push(value);
            } else {
                return Err(anyhow!("row contains unexpected column index {idx}"));
            }
        }
    }

    let arrays: Vec<ArrayRef> = columns
        .into_iter()
        .map(|col| Arc::new(Int64Array::from(col)) as ArrayRef)
        .collect();

    let batch = RecordBatch::try_new(schema, arrays).map_err(anyhow::Error::from)?;
    Ok(vec![batch])
}

pub(super) fn to_datafusion_error(err: anyhow::Error) -> DataFusionError {
    DataFusionError::Execution(err.to_string())
}

pub(super) fn append_row_with_diff(rows: &mut Vec<Row>, row: Row, diff: Diff) -> DFResult<()> {
    if diff < 0 {
        return Err(DataFusionError::Execution(format!(
            "materialized view snapshot contains negative diff {diff}"
        )));
    }
    for _ in 0..diff {
        rows.push(row.clone());
    }
    Ok(())
}

pub(super) fn build_scalar_batches(rows: Vec<Row>, schema: SchemaRef) -> Result<Vec<RecordBatch>> {
    if rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }

    let column_count = schema.fields().len();
    let mut columns: Vec<Vec<ScalarValue>> = vec![Vec::with_capacity(rows.len()); column_count];

    for row in rows {
        if row.len() != column_count {
            return Err(anyhow!(
                "row has {} columns but schema has {}",
                row.len(),
                column_count
            ));
        }
        for (idx, value) in row.into_iter().enumerate() {
            columns[idx].push(value);
        }
    }

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(column_count);
    for (idx, column) in columns.into_iter().enumerate() {
        let array = ScalarValue::iter_to_array(column.into_iter())
            .with_context(|| format!("convert column {idx} to array"))?;
        arrays.push(array);
    }

    let batch = RecordBatch::try_new(schema, arrays).map_err(anyhow::Error::from)?;
    Ok(vec![batch])
}

pub(super) fn append_mv_version_field(schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| (**field).clone())
        .collect();
    fields.push(Field::new(MV_VERSION_COLUMN, DataType::UInt64, false));
    Arc::new(Schema::new(fields))
}
