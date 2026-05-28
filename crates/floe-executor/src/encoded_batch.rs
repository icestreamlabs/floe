use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use datafusion::arrow::array::{ArrayRef, UInt32Array, UInt64Builder};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::delta_batch::{DeltaBatchBuffer, DeltaBatchConfig};

pub(crate) const ENCODED_BATCH_ROW_LIMIT: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EncodedRowBatchMode {
    Snapshot,
    Delta,
}

#[derive(Debug)]
pub(crate) struct ExpandedEncodedBatch {
    pub(crate) batch: RecordBatch,
    pub(crate) diffs: Vec<i64>,
}

#[derive(Clone, Copy)]
pub(crate) struct VirtualU64Column<'a> {
    pub(crate) name: &'a str,
    pub(crate) value: u64,
}

pub(crate) fn project_schema(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> Result<(SchemaRef, Vec<usize>)> {
    let indices = projection
        .cloned()
        .unwrap_or_else(|| (0..schema.fields().len()).collect());
    let mut fields = Vec::with_capacity(indices.len());
    for index in &indices {
        let Some(field) = schema.fields().get(*index) else {
            bail!(
                "projection index {index} out of bounds for schema with {} columns",
                schema.fields().len()
            );
        };
        fields.push((**field).clone());
    }
    Ok((Arc::new(Schema::new(fields)), indices))
}

pub(crate) fn build_expanded_batches_from_encoded_rows<I>(
    rows: I,
    schema: SchemaRef,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
    virtual_u64: Option<VirtualU64Column<'_>>,
    mode: EncodedRowBatchMode,
) -> Result<(SchemaRef, Vec<ExpandedEncodedBatch>)>
where
    I: IntoIterator<Item = (Vec<u8>, i64)>,
{
    let rows = normalize_rows(rows, mode)?;
    let (projected_schema, projected_indices) = project_schema(&schema, projection)?;
    let virtual_index = virtual_u64.and_then(|column| {
        schema
            .fields()
            .iter()
            .position(|field| field.name() == column.name)
    });
    let source_indices = projected_source_indices(&projected_indices, virtual_index);

    if projected_indices.is_empty() {
        return build_zero_column_batches(projected_schema, &rows, limit, mode);
    }

    let expansion = expansion_plan(&rows, limit, mode)?;
    if expansion.is_empty() {
        return Ok((
            Arc::clone(&projected_schema),
            vec![ExpandedEncodedBatch {
                batch: RecordBatch::new_empty(projected_schema),
                diffs: Vec::new(),
            }],
        ));
    }

    if source_indices.is_empty() {
        return build_virtual_only_batches(
            projected_schema,
            &projected_indices,
            virtual_index,
            virtual_u64,
            expansion,
        );
    }

    let decode_schema = schema_for_indices(&schema, &source_indices)?;
    let mut buffer = DeltaBatchBuffer::new_projected(
        decode_schema,
        Arc::<[usize]>::from(source_indices.clone()),
        false,
        DeltaBatchConfig {
            max_rows: usize::MAX,
            max_bytes: usize::MAX,
        },
    )
    .context("create encoded row Arrow expansion buffer")?;

    for row in rows.iter().map(|row| row.0.clone()) {
        if buffer.push(row, 1, None)?.is_some() {
            bail!("unbounded encoded row expansion buffer flushed before manual flush");
        }
    }
    let Some(decoded_batch) = buffer.flush_manual()? else {
        return Ok((
            Arc::clone(&projected_schema),
            vec![ExpandedEncodedBatch {
                batch: RecordBatch::new_empty(projected_schema),
                diffs: Vec::new(),
            }],
        ));
    };

    let source_positions = source_indices
        .iter()
        .copied()
        .enumerate()
        .map(|(slot, source_idx)| (source_idx, slot))
        .collect::<HashMap<_, _>>();
    build_projected_batches(
        projected_schema,
        &projected_indices,
        virtual_index,
        virtual_u64,
        &decoded_batch,
        &source_positions,
        expansion,
    )
}

fn normalize_rows<I>(rows: I, mode: EncodedRowBatchMode) -> Result<Vec<(Vec<u8>, i64)>>
where
    I: IntoIterator<Item = (Vec<u8>, i64)>,
{
    match mode {
        EncodedRowBatchMode::Snapshot => {
            let mut output = Vec::new();
            for (row, diff) in rows {
                if diff < 0 {
                    bail!("snapshot contains negative diff {diff}");
                }
                if diff != 0 {
                    output.push((row, diff));
                }
            }
            Ok(output)
        }
        EncodedRowBatchMode::Delta => {
            let mut merged = HashMap::<Vec<u8>, i64>::new();
            for (row, diff) in rows {
                if diff == 0 {
                    continue;
                }
                let next = merged.get(&row).copied().unwrap_or(0).saturating_add(diff);
                if next == 0 {
                    merged.remove(&row);
                } else {
                    merged.insert(row, next);
                }
            }
            Ok(merged.into_iter().collect())
        }
    }
}

fn projected_source_indices(
    projected_indices: &[usize],
    virtual_index: Option<usize>,
) -> Vec<usize> {
    let mut source_indices = projected_indices
        .iter()
        .copied()
        .filter(|source_idx| Some(*source_idx) != virtual_index)
        .collect::<Vec<_>>();
    source_indices.sort_unstable();
    source_indices.dedup();
    source_indices
}

fn schema_for_indices(schema: &SchemaRef, indices: &[usize]) -> Result<SchemaRef> {
    let mut fields = Vec::with_capacity(indices.len());
    for index in indices {
        let Some(field) = schema.fields().get(*index) else {
            bail!(
                "source index {index} out of bounds for schema with {} columns",
                schema.fields().len()
            );
        };
        fields.push((**field).clone());
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn expansion_plan(
    rows: &[(Vec<u8>, i64)],
    limit: Option<usize>,
    mode: EncodedRowBatchMode,
) -> Result<Vec<(u32, i64)>> {
    let mut output = Vec::new();
    let mut remaining = limit.unwrap_or(usize::MAX);
    for (row_idx, (_row, diff)) in rows.iter().enumerate() {
        if remaining == 0 {
            break;
        }
        let row_idx = u32::try_from(row_idx).context("too many encoded rows to expand")?;
        let repeat = usize::try_from(diff.checked_abs().context("diff overflow")?)
            .context("diff does not fit usize")?;
        let repeat = repeat.min(remaining);
        if repeat == 0 {
            continue;
        }
        let out_diff = match mode {
            EncodedRowBatchMode::Snapshot => 1,
            EncodedRowBatchMode::Delta if *diff > 0 => 1,
            EncodedRowBatchMode::Delta => -1,
        };
        output.extend(std::iter::repeat_n((row_idx, out_diff), repeat));
        remaining = remaining.saturating_sub(repeat);
    }
    Ok(output)
}

fn build_zero_column_batches(
    schema: SchemaRef,
    rows: &[(Vec<u8>, i64)],
    limit: Option<usize>,
    mode: EncodedRowBatchMode,
) -> Result<(SchemaRef, Vec<ExpandedEncodedBatch>)> {
    let expansion = expansion_plan(rows, limit, mode)?;
    let options = RecordBatchOptions::new().with_row_count(Some(expansion.len()));
    let batch = RecordBatch::try_new_with_options(Arc::clone(&schema), vec![], &options)?;
    Ok((
        schema,
        vec![ExpandedEncodedBatch {
            batch,
            diffs: expansion.into_iter().map(|(_, diff)| diff).collect(),
        }],
    ))
}

fn build_virtual_only_batches(
    projected_schema: SchemaRef,
    projected_indices: &[usize],
    virtual_index: Option<usize>,
    virtual_u64: Option<VirtualU64Column<'_>>,
    expansion: Vec<(u32, i64)>,
) -> Result<(SchemaRef, Vec<ExpandedEncodedBatch>)> {
    build_projected_batches(
        projected_schema,
        projected_indices,
        virtual_index,
        virtual_u64,
        &RecordBatch::new_empty(Arc::new(Schema::empty())),
        &HashMap::new(),
        expansion,
    )
}

fn build_projected_batches(
    projected_schema: SchemaRef,
    projected_indices: &[usize],
    virtual_index: Option<usize>,
    virtual_u64: Option<VirtualU64Column<'_>>,
    decoded_batch: &RecordBatch,
    source_positions: &HashMap<usize, usize>,
    expansion: Vec<(u32, i64)>,
) -> Result<(SchemaRef, Vec<ExpandedEncodedBatch>)> {
    let mut batches = Vec::new();
    for chunk in expansion.chunks(ENCODED_BATCH_ROW_LIMIT) {
        let take_indices = UInt32Array::from_iter_values(chunk.iter().map(|(row_idx, _)| *row_idx));
        let arrays = projected_indices
            .iter()
            .copied()
            .map(|source_idx| {
                if Some(source_idx) == virtual_index {
                    return build_virtual_u64_array(virtual_u64, chunk.len());
                }
                let decoded_slot = source_positions.get(&source_idx).copied().ok_or_else(|| {
                    anyhow!("projection source column index {source_idx} was not decoded")
                })?;
                let array = decoded_batch.column(decoded_slot);
                take(array.as_ref(), &take_indices, None).map_err(anyhow::Error::from)
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
        let batch = RecordBatch::try_new(Arc::clone(&projected_schema), arrays)?;
        batches.push(ExpandedEncodedBatch {
            batch,
            diffs: chunk.iter().map(|(_, diff)| *diff).collect(),
        });
    }
    if batches.is_empty() {
        batches.push(ExpandedEncodedBatch {
            batch: RecordBatch::new_empty(Arc::clone(&projected_schema)),
            diffs: Vec::new(),
        });
    }
    Ok((projected_schema, batches))
}

fn build_virtual_u64_array(
    virtual_u64: Option<VirtualU64Column<'_>>,
    rows: usize,
) -> Result<ArrayRef> {
    let Some(column) = virtual_u64 else {
        bail!("projection requested virtual column but no virtual column was provided");
    };
    let mut builder = UInt64Builder::with_capacity(rows);
    for _ in 0..rows {
        builder.append_value(column.value);
    }
    Ok(Arc::new(builder.finish()))
}

pub(crate) fn append_virtual_u64_field(schema: &SchemaRef, name: &str) -> SchemaRef {
    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| (**field).clone())
        .collect();
    fields.push(Field::new(name, DataType::UInt64, false));
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, Int64Array, StringArray, UInt64Array};

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("label", DataType::Utf8, true),
        ]))
    }

    fn encoded_row(id: i64, label: &str) -> Vec<u8> {
        let mut row = Vec::new();
        row.extend_from_slice(&2_u32.to_le_bytes());
        row.push(0x01);
        row.extend_from_slice(&id.to_le_bytes());
        row.push(0x02);
        row.extend_from_slice(&(label.len() as u32).to_le_bytes());
        row.extend_from_slice(label.as_bytes());
        row
    }

    #[test]
    fn expands_snapshot_rows_with_arrow_take_and_virtual_version() {
        let schema = append_virtual_u64_field(&schema(), "__floe_mv_version");
        let projection = vec![1, 2];
        let (_schema, batches) = build_expanded_batches_from_encoded_rows(
            vec![(encoded_row(1, "one"), 2), (encoded_row(2, "two"), 1)],
            schema,
            Some(&projection),
            None,
            Some(VirtualU64Column {
                name: "__floe_mv_version",
                value: 7,
            }),
            EncodedRowBatchMode::Snapshot,
        )
        .expect("build batches");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].diffs, vec![1, 1, 1]);
        let labels = batches[0]
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(labels.value(0), "one");
        assert_eq!(labels.value(1), "one");
        assert_eq!(labels.value(2), "two");
        let versions = batches[0]
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(versions.values(), &[7, 7, 7]);
    }

    #[test]
    fn coalesces_delta_rows_and_expands_signed_diffs() {
        let (_schema, batches) = build_expanded_batches_from_encoded_rows(
            vec![
                (encoded_row(1, "one"), 2),
                (encoded_row(1, "one"), -1),
                (encoded_row(2, "two"), -2),
            ],
            schema(),
            None,
            None,
            None,
            EncodedRowBatchMode::Delta,
        )
        .expect("build batches");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].diffs.iter().sum::<i64>(), -1);
        assert_eq!(batches[0].batch.num_rows(), 3);
        let ids = batches[0]
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(ids.value(0) == 1 || ids.value(0) == 2);
    }
}
