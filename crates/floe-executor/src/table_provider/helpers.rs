use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::scalar::ScalarValue;

use crate::encoding::extract_encoded_row_scalars;
use crate::scalar_array_builder::ScalarColumnBuilder;

use super::MV_VERSION_COLUMN;

const SCAN_BATCH_ROW_LIMIT: usize = 1024;

pub(super) fn to_datafusion_error(err: anyhow::Error) -> DataFusionError {
    DataFusionError::Execution(err.to_string())
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

pub(super) fn build_batches_from_encoded_snapshot(
    snapshot: std::collections::HashMap<Vec<u8>, i64>,
    schema: SchemaRef,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
    mv_version: Option<u64>,
) -> DFResult<(SchemaRef, Vec<RecordBatch>)> {
    let (projected_schema, projected_indices) = project_schema(&schema, projection)?;
    let mv_version_index = schema
        .fields()
        .iter()
        .position(|field| field.name() == MV_VERSION_COLUMN);
    let mut batches = Vec::new();
    let zero_column_projection = projected_indices.is_empty();
    let mut builders = projected_schema
        .fields()
        .iter()
        .map(|field| {
            ScalarColumnBuilder::new(field.data_type(), SCAN_BATCH_ROW_LIMIT)
                .map_err(|err| DataFusionError::Execution(err.to_string()))
        })
        .collect::<DFResult<Vec<_>>>()?;
    let mut rows_in_batch = 0usize;
    let mut total_rows = 0usize;
    let mut projection_source_indices = projected_indices
        .iter()
        .copied()
        .filter(|source_idx| Some(*source_idx) != mv_version_index)
        .collect::<Vec<_>>();
    projection_source_indices.sort_unstable();
    projection_source_indices.dedup();
    let projection_source_positions = projection_source_indices
        .iter()
        .copied()
        .enumerate()
        .map(|(slot, source_idx)| (source_idx, slot))
        .collect::<std::collections::HashMap<_, _>>();
    'snapshot: for (key, diff) in snapshot {
        if diff < 0 {
            return Err(DataFusionError::Execution(format!(
                "snapshot contains negative diff {diff}"
            )));
        }
        if diff == 0 {
            continue;
        }
        let projected_values = if !projection_source_indices.is_empty() {
            Some(
                extract_encoded_row_scalars(&key, projection_source_indices.as_slice())
                    .map_err(|err| DataFusionError::Execution(err.to_string()))?,
            )
        } else {
            None
        };
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
                    let projected_slot = projection_source_positions
                        .get(source_idx)
                        .copied()
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "projection source column index {source_idx} was not decoded"
                            ))
                        })?;
                    let encoded_value = projected_values
                        .as_ref()
                        .and_then(|values| values.get(projected_slot))
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "row does not contain projected column index {source_idx}"
                            ))
                        })?;
                    builders[column_idx]
                        .append_encoded_scalar(encoded_value.as_ref())
                        .map_err(|err| DataFusionError::Execution(err.to_string()))?;
                    continue;
                };
                builders[column_idx]
                    .append(&value)
                    .map_err(|err| DataFusionError::Execution(err.to_string()))?;
            }
            rows_in_batch += 1;
            total_rows += 1;
            if rows_in_batch >= SCAN_BATCH_ROW_LIMIT {
                flush_builders_to_batch(
                    builders.as_mut_slice(),
                    rows_in_batch,
                    Arc::clone(&projected_schema),
                    &mut batches,
                )?;
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

    flush_builders_to_batch(
        builders.as_mut_slice(),
        rows_in_batch,
        Arc::clone(&projected_schema),
        &mut batches,
    )?;
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

fn flush_builders_to_batch(
    builders: &mut [ScalarColumnBuilder],
    row_count: usize,
    schema: SchemaRef,
    batches: &mut Vec<RecordBatch>,
) -> DFResult<()> {
    if row_count == 0 {
        return Ok(());
    }
    let arrays = builders
        .iter_mut()
        .map(ScalarColumnBuilder::finish_array)
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|err| DataFusionError::Execution(err.to_string()))?;
    batches.push(batch);
    Ok(())
}
