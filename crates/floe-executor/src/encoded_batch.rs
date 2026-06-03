use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;

use crate::delta_batch::{DeltaBatchBuffer, DeltaBatchConfig};
use crate::encoding::{EncodedRowScalar, decode_all_encoded_row_scalars_into};
use crate::scalar_array_builder::ScalarColumnBuilder;

const ENCODED_ARROW_BATCH_ROW_LIMIT: usize = 4096;

pub(crate) fn encoded_snapshot_row_count(snapshot: &HashMap<Vec<u8>, i64>) -> usize {
    snapshot
        .values()
        .filter_map(|diff| usize::try_from((*diff).max(0)).ok())
        .sum()
}

pub(crate) fn encoded_snapshot_to_arrow_batches(
    snapshot: &HashMap<Vec<u8>, i64>,
    schema: SchemaRef,
    limit: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    let mut builders = EncodedSnapshotBatchBuilder::new(Arc::clone(&schema))?;
    let mut remaining = limit.unwrap_or(usize::MAX);
    let mut decoded = Vec::new();

    for (row, diff) in snapshot {
        if remaining == 0 {
            break;
        }
        let row_count = usize::try_from((*diff).max(0))
            .unwrap_or(usize::MAX)
            .min(remaining);
        if row_count == 0 {
            continue;
        }
        decode_all_encoded_row_scalars_into(row, &mut decoded)?;
        builders.push_repeated(&decoded, row_count, &mut batches)?;
        remaining = remaining.saturating_sub(row_count);
    }

    if let Some(batch) = builders.finish()? {
        batches.push(batch);
    }
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(schema));
    }
    Ok(batches)
}

pub(crate) fn encoded_deltas_to_weighted_arrow_batches(
    deltas: &[(Vec<u8>, i64)],
    schema: SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let mut buffer = DeltaBatchBuffer::new(schema, false, DeltaBatchConfig::default())?;
    let mut batches = Vec::new();
    for (row, diff) in deltas {
        if let Some(batch) = buffer.push_ref(row, *diff, None)? {
            batches.push(batch);
        }
    }
    if let Some(batch) = buffer.flush_manual()? {
        batches.push(batch);
    }
    Ok(batches)
}

struct EncodedSnapshotBatchBuilder {
    schema: SchemaRef,
    columns: Vec<ScalarColumnBuilder>,
    rows: usize,
}

impl EncodedSnapshotBatchBuilder {
    fn new(schema: SchemaRef) -> Result<Self> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), ENCODED_ARROW_BATCH_ROW_LIMIT))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            schema,
            columns,
            rows: 0,
        })
    }

    fn push_repeated(
        &mut self,
        decoded: &[Option<EncodedRowScalar>],
        mut row_count: usize,
        batches: &mut Vec<RecordBatch>,
    ) -> Result<()> {
        if decoded.len() != self.schema.fields().len() {
            return Err(anyhow!(
                "encoded row has {} columns but schema has {}",
                decoded.len(),
                self.schema.fields().len()
            ));
        }
        while row_count > 0 {
            let chunk_rows = row_count.min(ENCODED_ARROW_BATCH_ROW_LIMIT - self.rows);
            for (builder, value) in self.columns.iter_mut().zip(decoded.iter()) {
                builder.append_encoded_scalar_repeated(value.as_ref(), chunk_rows)?;
            }
            self.rows += chunk_rows;
            row_count -= chunk_rows;
            if self.rows == ENCODED_ARROW_BATCH_ROW_LIMIT
                && let Some(batch) = self.finish()?
            {
                batches.push(batch);
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.rows == 0 {
            return Ok(None);
        }
        let arrays = self
            .columns
            .iter_mut()
            .map(ScalarColumnBuilder::finish_array)
            .collect::<Vec<_>>();
        self.rows = 0;
        Ok(Some(RecordBatch::try_new(
            Arc::clone(&self.schema),
            arrays,
        )?))
    }
}
