use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::scalar::ScalarValue;

use crate::encoding::decode_projected_row_key;
use crate::stream_types::Row;

use super::MV_VERSION_COLUMN;

const SCAN_BATCH_ROW_LIMIT: usize = 1024;

pub(super) fn to_datafusion_error(err: anyhow::Error) -> DataFusionError {
    DataFusionError::Execution(err.to_string())
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

pub(super) fn project_schema(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DFResult<(SchemaRef, Vec<usize>)> {
    let indices = projection
        .cloned()
        .unwrap_or_else(|| (0..schema.fields().len()).collect());
    let mut fields = Vec::with_capacity(indices.len());
    for index in &indices {
        let Some(field) = schema.fields().get(*index) else {
            return Err(DataFusionError::Execution(format!(
                "projection index {index} out of bounds for schema with {} columns",
                schema.fields().len()
            )));
        };
        fields.push((**field).clone());
    }
    Ok((Arc::new(Schema::new(fields)), indices))
}

pub(super) fn build_batches_from_encoded_snapshot<F>(
    snapshot: std::collections::HashMap<Vec<u8>, i64>,
    schema: SchemaRef,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
    mv_version: Option<u64>,
    row_filter: F,
) -> DFResult<(SchemaRef, Vec<RecordBatch>)>
where
    F: Fn(&Row) -> bool,
{
    let (projected_schema, projected_indices) = project_schema(&schema, projection)?;
    let mv_version_index = schema
        .fields()
        .iter()
        .position(|field| field.name() == MV_VERSION_COLUMN);
    let decoded_row_len = mv_version_index.unwrap_or(schema.fields().len());
    let mut batches = Vec::new();
    let zero_column_projection = projected_indices.is_empty();
    let mut columns: Vec<Vec<ScalarValue>> =
        vec![Vec::with_capacity(SCAN_BATCH_ROW_LIMIT); projected_indices.len()];
    let mut rows_in_batch = 0usize;
    let mut total_rows = 0usize;
    'snapshot: for (key, diff) in snapshot {
        if diff < 0 {
            return Err(DataFusionError::Execution(format!(
                "snapshot contains negative diff {diff}"
            )));
        }
        if diff == 0 {
            continue;
        }
        let decoded = decode_projected_row_key(&key)
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        if decoded.len() != decoded_row_len {
            return Err(DataFusionError::Execution(format!(
                "decoded row has {} columns but expected {}",
                decoded.len(),
                decoded_row_len
            )));
        }
        if !row_filter(&decoded) {
            continue;
        }
        for _ in 0..diff {
            if let Some(limit) = limit
                && total_rows >= limit
            {
                break 'snapshot;
            }
            if zero_column_projection {
                total_rows += 1;
                continue;
            }
            for (column_idx, source_idx) in projected_indices.iter().enumerate() {
                let value = if Some(*source_idx) == mv_version_index {
                    ScalarValue::UInt64(Some(mv_version.unwrap_or(0)))
                } else {
                    decoded.get(*source_idx).cloned().ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "row does not contain projected column index {source_idx}"
                        ))
                    })?
                };
                columns[column_idx].push(value);
            }
            rows_in_batch += 1;
            total_rows += 1;
            if rows_in_batch >= SCAN_BATCH_ROW_LIMIT {
                flush_columns_to_batch(&mut columns, Arc::clone(&projected_schema), &mut batches)?;
                rows_in_batch = 0;
            }
        }
    }
    if zero_column_projection {
        let options = RecordBatchOptions::new().with_row_count(Some(total_rows));
        let batch =
            RecordBatch::try_new_with_options(Arc::clone(&projected_schema), vec![], &options)
                .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        batches.push(batch);
        return Ok((projected_schema, batches));
    }

    flush_columns_to_batch(&mut columns, Arc::clone(&projected_schema), &mut batches)?;
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(Arc::clone(&projected_schema)));
    }
    Ok((projected_schema, batches))
}

pub(super) fn build_constant_projection_batches(
    schema: SchemaRef,
    value: ScalarValue,
    row_count: usize,
) -> DFResult<Vec<RecordBatch>> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        let batch = RecordBatch::try_new_with_options(Arc::clone(&schema), vec![], &options)
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        return Ok(vec![batch]);
    }

    if row_count == 0 {
        let arrays: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|_| {
                value
                    .clone()
                    .to_array_of_size(0)
                    .map_err(|err| DataFusionError::Execution(err.to_string()))
            })
            .collect::<DFResult<_>>()?;
        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        return Ok(vec![batch]);
    }

    let mut batches = Vec::new();
    let mut remaining = row_count;
    while remaining > 0 {
        let batch_rows = remaining.min(SCAN_BATCH_ROW_LIMIT);
        let arrays: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|_| {
                value
                    .clone()
                    .to_array_of_size(batch_rows)
                    .map_err(|err| DataFusionError::Execution(err.to_string()))
            })
            .collect::<DFResult<_>>()?;
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        batches.push(batch);
        remaining -= batch_rows;
    }
    Ok(batches)
}

fn flush_columns_to_batch(
    columns: &mut [Vec<ScalarValue>],
    schema: SchemaRef,
    batches: &mut Vec<RecordBatch>,
) -> DFResult<()> {
    if columns.is_empty() || columns[0].is_empty() {
        return Ok(());
    }
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for column in columns.iter_mut() {
        let values = std::mem::take(column);
        let array = ScalarValue::iter_to_array(values.into_iter())
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        arrays.push(array);
    }
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|err| DataFusionError::Execution(err.to_string()))?;
    batches.push(batch);
    Ok(())
}
