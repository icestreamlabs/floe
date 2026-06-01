use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, UInt64Array, UInt64Builder};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::error::{DataFusionError, Result as DFResult};

use crate::encoded_batch::{
    VirtualU64Column, append_virtual_u64_field, build_expanded_batches_from_encoded_rows,
    project_schema as project_arrow_schema,
};

use super::MV_VERSION_COLUMN;

const SCAN_BATCH_ROW_LIMIT: usize = 1024;

pub(super) fn to_datafusion_error(err: anyhow::Error) -> DataFusionError {
    DataFusionError::Execution(err.to_string())
}

pub(super) fn append_mv_version_field(schema: &SchemaRef) -> SchemaRef {
    append_virtual_u64_field(schema, MV_VERSION_COLUMN)
}

pub(super) fn project_schema(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DFResult<(SchemaRef, Vec<usize>)> {
    project_arrow_schema(schema, projection).map_err(to_datafusion_error)
}

pub(super) fn build_batches_from_encoded_snapshot<I>(
    snapshot: I,
    schema: SchemaRef,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
    mv_version: Option<u64>,
) -> DFResult<(SchemaRef, Vec<RecordBatch>)>
where
    I: IntoIterator<Item = (Vec<u8>, i64)>,
{
    let virtual_version = mv_version.map(|value| VirtualU64Column {
        name: MV_VERSION_COLUMN,
        value,
    });
    let (projected_schema, batches) = build_expanded_batches_from_encoded_rows(
        snapshot,
        schema,
        projection,
        limit,
        virtual_version,
    )
    .map_err(to_datafusion_error)?;
    Ok((
        projected_schema,
        batches.into_iter().map(|batch| batch.batch).collect(),
    ))
}

pub(super) fn build_batches_from_arrow_snapshot(
    snapshot: Arc<Vec<RecordBatch>>,
    schema: SchemaRef,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
    mv_version: u64,
) -> DFResult<(SchemaRef, Vec<RecordBatch>)> {
    let (projected_schema, projected_indices) = project_schema(&schema, projection)?;
    let mv_version_index = schema
        .fields()
        .iter()
        .position(|field| field.name() == MV_VERSION_COLUMN);
    let zero_column_projection = projected_indices.is_empty();
    let mut batches = Vec::new();
    let mut total_rows = 0usize;

    for batch in snapshot.iter() {
        if batch.num_rows() == 0 {
            continue;
        }
        let remaining_rows = limit
            .map(|limit| limit.saturating_sub(total_rows))
            .unwrap_or(usize::MAX);
        if remaining_rows == 0 {
            break;
        }
        let batch = if remaining_rows < batch.num_rows() {
            batch.slice(0, remaining_rows)
        } else {
            batch.clone()
        };
        let row_count = batch.num_rows();
        total_rows = total_rows.saturating_add(row_count);

        if zero_column_projection {
            continue;
        }

        let mut columns = Vec::with_capacity(projected_indices.len());
        for source_idx in &projected_indices {
            if Some(*source_idx) == mv_version_index {
                columns.push(Arc::new(UInt64Array::from_value(mv_version, row_count)) as ArrayRef);
            } else {
                let Some(column) = batch.columns().get(*source_idx) else {
                    return Err(DataFusionError::Execution(format!(
                        "projection source column index {source_idx} out of bounds for Arrow snapshot"
                    )));
                };
                columns.push(Arc::clone(column));
            }
        }
        batches.push(
            RecordBatch::try_new(Arc::clone(&projected_schema), columns)
                .map_err(|err| DataFusionError::Execution(err.to_string()))?,
        );
    }

    if zero_column_projection {
        let options = RecordBatchOptions::new().with_row_count(Some(total_rows));
        let batch =
            RecordBatch::try_new_with_options(Arc::clone(&projected_schema), vec![], &options)
                .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        return Ok((projected_schema, vec![batch]));
    }
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(Arc::clone(&projected_schema)));
    }
    Ok((projected_schema, batches))
}

pub(super) fn build_constant_u64_projection_batches(
    schema: SchemaRef,
    value: u64,
    row_count: usize,
) -> DFResult<Vec<RecordBatch>> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        let batch = RecordBatch::try_new_with_options(Arc::clone(&schema), vec![], &options)
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        return Ok(vec![batch]);
    }

    for field in schema.fields() {
        if field.data_type() != &DataType::UInt64 {
            return Err(DataFusionError::Execution(format!(
                "constant u64 projection requires UInt64 fields, found {:?}",
                field.data_type()
            )));
        }
    }

    if row_count == 0 {
        let array: ArrayRef = Arc::new(UInt64Builder::with_capacity(0).finish());
        let arrays: Vec<ArrayRef> = vec![array; schema.fields().len()];
        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        return Ok(vec![batch]);
    }

    let mut batches = Vec::new();
    let mut remaining = row_count;
    while remaining > 0 {
        let batch_rows = remaining.min(SCAN_BATCH_ROW_LIMIT);
        let mut builder = UInt64Builder::with_capacity(batch_rows);
        for _ in 0..batch_rows {
            builder.append_value(value);
        }
        let array: ArrayRef = Arc::new(builder.finish());
        let arrays: Vec<ArrayRef> = vec![array; schema.fields().len()];
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        batches.push(batch);
        remaining -= batch_rows;
    }
    Ok(batches)
}
