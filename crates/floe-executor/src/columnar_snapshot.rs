use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use dbsp::collections::ColumnarZSet;

use crate::scalar_array_builder::ScalarColumnBuilder;

const COLUMNAR_SCAN_BATCH_ROW_LIMIT: usize = 4096;

pub(crate) fn columnar_zset_weight_sum(zset: &ColumnarZSet) -> Result<i64> {
    let mut sum = 0_i64;
    for batch in zset.batches() {
        let weights = batch
            .column(zset.value_column_count())
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("columnar zset weight column must be Int64"))?;
        for row_idx in 0..weights.len() {
            if weights.is_null(row_idx) {
                anyhow::bail!("columnar zset weight cannot be NULL");
            }
            sum = sum.saturating_add(weights.value(row_idx));
        }
    }
    Ok(sum)
}

pub(crate) fn columnar_zset_positive_row_count(zset: &ColumnarZSet) -> Result<usize> {
    let mut row_count = 0usize;
    for batch in zset.batches() {
        let weights = batch
            .column(zset.value_column_count())
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("columnar zset weight column must be Int64"))?;
        for row_idx in 0..weights.len() {
            if weights.is_null(row_idx) {
                anyhow::bail!("columnar zset weight cannot be NULL");
            }
            let weight = weights.value(row_idx);
            if weight < 0 {
                anyhow::bail!("columnar zset materialized snapshot contains negative weight");
            }
            row_count = row_count.saturating_add(
                usize::try_from(weight).context("columnar zset row weight exceeds usize")?,
            );
        }
    }
    Ok(row_count)
}

pub(crate) fn columnar_zset_to_arrow_snapshot(
    zset: &ColumnarZSet,
    schema: SchemaRef,
    limit: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    let mut output = Vec::new();
    let mut builders = snapshot_output_builders(&schema)?;
    let mut buffered_rows = 0usize;
    let mut emitted_rows = 0usize;
    let max_rows = limit.unwrap_or(usize::MAX);

    'batches: for batch in zset.batches() {
        if batch.num_rows() == 0 {
            continue;
        }
        let weights = batch
            .column(zset.value_column_count())
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("columnar zset weight column must be Int64"))?;
        for row_idx in 0..batch.num_rows() {
            if weights.is_null(row_idx) {
                anyhow::bail!("columnar zset weight cannot be NULL");
            }
            let weight = weights.value(row_idx);
            if weight < 0 {
                anyhow::bail!("columnar zset materialized snapshot contains negative weight");
            }
            let repeat =
                usize::try_from(weight).context("columnar zset row weight exceeds usize")?;
            for _ in 0..repeat {
                if emitted_rows == max_rows {
                    break 'batches;
                }
                append_snapshot_row(&mut builders, batch, row_idx, zset.value_column_count())?;
                buffered_rows = buffered_rows.saturating_add(1);
                emitted_rows = emitted_rows.saturating_add(1);
                if buffered_rows == COLUMNAR_SCAN_BATCH_ROW_LIMIT {
                    output.push(finish_snapshot_batch(&schema, &mut builders)?);
                    buffered_rows = 0;
                }
            }
        }
    }

    if buffered_rows > 0 {
        output.push(finish_snapshot_batch(&schema, &mut builders)?);
    }
    if output.is_empty() {
        output.push(RecordBatch::new_empty(schema));
    }
    Ok(output)
}

fn snapshot_output_builders(schema: &SchemaRef) -> Result<Vec<ScalarColumnBuilder>> {
    schema
        .fields()
        .iter()
        .map(|field| ScalarColumnBuilder::new(field.data_type(), COLUMNAR_SCAN_BATCH_ROW_LIMIT))
        .collect()
}

fn append_snapshot_row(
    builders: &mut [ScalarColumnBuilder],
    batch: &RecordBatch,
    row_idx: usize,
    value_column_count: usize,
) -> Result<()> {
    for column_idx in 0..value_column_count {
        builders[column_idx].append_array_value(batch.column(column_idx).as_ref(), row_idx)?;
    }
    Ok(())
}

fn finish_snapshot_batch(
    schema: &SchemaRef,
    builders: &mut [ScalarColumnBuilder],
) -> Result<RecordBatch> {
    let arrays = builders
        .iter_mut()
        .map(ScalarColumnBuilder::finish_array)
        .collect::<Vec<_>>();
    Ok(RecordBatch::try_new(Arc::clone(schema), arrays)?)
}
