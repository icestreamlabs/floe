use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
#[cfg(test)]
use dbsp::{
    TableDescriptor, nexmark_auction_alias_table, nexmark_auction_table, nexmark_bid_alias_table,
    nexmark_bid_table, nexmark_person_alias_table, nexmark_person_table,
};

use crate::delta_batch::{DeltaBatchBuffer, DeltaBatchConfig};
use crate::stream_types::Diff;

type KeyedDelta = (Vec<u8>, Vec<u8>, Diff);
type KeyedTimeDelta = (Vec<u8>, Diff, Vec<u8>, i64);

#[cfg(test)]
fn source_primary_key_columns(source_name: &str) -> Option<Vec<usize>> {
    source_table(source_name).map(|table| table.primary_key().columns().to_vec())
}

#[cfg(test)]
fn build_source_delta_batch(
    source_name: &str,
    base_schema: SchemaRef,
    rows: impl IntoIterator<Item = (Vec<u8>, Diff)>,
) -> Result<RecordBatch> {
    let key_columns = source_primary_key_columns(source_name);
    build_delta_batch(base_schema, rows, key_columns.as_deref())
}

#[cfg(test)]
fn build_delta_batch(
    base_schema: SchemaRef,
    rows: impl IntoIterator<Item = (Vec<u8>, Diff)>,
    key_columns: Option<&[usize]>,
) -> Result<RecordBatch> {
    let config = DeltaBatchConfig {
        max_rows: usize::MAX,
        max_bytes: usize::MAX,
    };
    let mut buffer = if let Some(columns) = key_columns {
        DeltaBatchBuffer::new_keyed(base_schema, Arc::<[usize]>::from(columns.to_vec()), config)?
    } else {
        DeltaBatchBuffer::new(base_schema, false, config)?
    };
    let delta_schema = buffer.delta_schema();
    for (row, diff) in rows {
        let _ = buffer.push(row, diff, None)?;
    }
    Ok(buffer
        .flush_manual()?
        .unwrap_or_else(|| RecordBatch::new_empty(Arc::clone(&delta_schema))))
}

#[derive(Clone)]
pub struct VectorizedEncodedKeyExtractor {
    base_schema: SchemaRef,
    key_columns: Arc<Vec<usize>>,
}

pub struct VectorizedKeyedTimeDelta {
    pub row: Vec<u8>,
    pub diff: Diff,
    pub key: Vec<u8>,
    pub event_ts: i64,
    pub batch_row: usize,
}

pub struct VectorizedKeyedTimeBatch {
    pub batch: RecordBatch,
    pub input_positions: HashMap<usize, usize>,
    pub deltas: Vec<VectorizedKeyedTimeDelta>,
}

impl VectorizedEncodedKeyExtractor {
    pub fn new(base_schema: SchemaRef, key_columns: Arc<Vec<usize>>) -> Result<Self> {
        for column in key_columns.iter().copied() {
            if column >= base_schema.fields().len() {
                bail!(
                    "key column {} is outside schema width {}",
                    column,
                    base_schema.fields().len()
                );
            }
        }
        Ok(Self {
            base_schema,
            key_columns,
        })
    }

    pub fn extract_keyed_deltas(&self, rows: &[(Vec<u8>, Diff)]) -> Result<Vec<KeyedDelta>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let eval_schema = projected_arrow_schema(&self.base_schema, self.key_columns.as_ref())?;
        let mut buffer = DeltaBatchBuffer::new_projected(
            eval_schema,
            Arc::<[usize]>::from(self.key_columns.as_ref().clone()),
            false,
            DeltaBatchConfig {
                max_rows: usize::MAX,
                max_bytes: usize::MAX,
            },
        )?;
        let mut staged_rows = Vec::with_capacity(rows.len());
        for (row_idx, (row, diff)) in rows.iter().enumerate() {
            if *diff == 0 {
                continue;
            }
            if buffer.push_ref(row, *diff, None)?.is_some() {
                bail!("unbounded vectorized key extractor flushed before manual flush");
            }
            staged_rows.push((row_idx, *diff));
        }

        let Some(batch) = buffer.flush_manual()? else {
            return Ok(Vec::new());
        };
        let mut output = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            if (0..self.key_columns.len()).any(|idx| batch.column(idx).is_null(row_idx)) {
                continue;
            }
            let mut key = Vec::with_capacity(4 + self.key_columns.len().saturating_mul(16));
            let count = u32::try_from(self.key_columns.len()).context("join key too wide")?;
            key.extend_from_slice(&count.to_le_bytes());
            for column_idx in 0..self.key_columns.len() {
                append_arrow_key_value(batch.column(column_idx).as_ref(), row_idx, &mut key)?;
            }
            let (source_idx, diff) = staged_rows
                .get(row_idx)
                .ok_or_else(|| anyhow!("vectorized key extractor row index out of bounds"))?;
            let row = rows
                .get(*source_idx)
                .ok_or_else(|| anyhow!("vectorized key extractor source index out of bounds"))?
                .0
                .clone();
            output.push((key, row, *diff));
        }
        Ok(output)
    }

    pub fn extract_keyed_time_deltas(
        &self,
        rows: &[(Vec<u8>, Diff)],
        time_column: usize,
    ) -> Result<Vec<KeyedTimeDelta>> {
        Ok(self
            .extract_keyed_time_batch_with_columns(rows, time_column, &[])?
            .map(|batch| {
                batch
                    .deltas
                    .into_iter()
                    .map(|delta| (delta.row, delta.diff, delta.key, delta.event_ts))
                    .collect()
            })
            .unwrap_or_default())
    }

    pub fn extract_keyed_time_batch_with_columns(
        &self,
        rows: &[(Vec<u8>, Diff)],
        time_column: usize,
        extra_columns: &[usize],
    ) -> Result<Option<VectorizedKeyedTimeBatch>> {
        if rows.is_empty() {
            return Ok(None);
        }
        if time_column >= self.base_schema.fields().len() {
            bail!(
                "time column {} is outside schema width {}",
                time_column,
                self.base_schema.fields().len()
            );
        }

        let mut input_columns = self.key_columns.as_ref().clone();
        let time_position = input_columns
            .iter()
            .position(|column| *column == time_column)
            .unwrap_or_else(|| {
                input_columns.push(time_column);
                input_columns.len() - 1
            });
        for column in extra_columns.iter().copied() {
            if column >= self.base_schema.fields().len() {
                bail!(
                    "extra column {} is outside schema width {}",
                    column,
                    self.base_schema.fields().len()
                );
            }
            if !input_columns.contains(&column) {
                input_columns.push(column);
            }
        }
        let input_positions = input_columns
            .iter()
            .copied()
            .enumerate()
            .map(|(position, column)| (column, position))
            .collect::<HashMap<_, _>>();
        let eval_schema = projected_arrow_schema(&self.base_schema, &input_columns)?;
        let mut buffer = DeltaBatchBuffer::new_projected(
            eval_schema,
            Arc::<[usize]>::from(input_columns),
            false,
            DeltaBatchConfig {
                max_rows: usize::MAX,
                max_bytes: usize::MAX,
            },
        )?;
        let mut staged_rows = Vec::with_capacity(rows.len());
        for (row_idx, (row, diff)) in rows.iter().enumerate() {
            if *diff == 0 {
                continue;
            }
            if buffer.push_ref(row, *diff, None)?.is_some() {
                bail!("unbounded vectorized key extractor flushed before manual flush");
            }
            staged_rows.push((row_idx, *diff));
        }

        let Some(batch) = buffer.flush_manual()? else {
            return Ok(None);
        };
        let time_array = batch.column(time_position);
        let mut deltas = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            if time_array.is_null(row_idx)
                || (0..self.key_columns.len()).any(|idx| batch.column(idx).is_null(row_idx))
            {
                continue;
            }
            let event_ts = arrow_i64_like_value(time_array.as_ref(), row_idx)?;
            let mut key = Vec::with_capacity(4 + self.key_columns.len().saturating_mul(16));
            let count = u32::try_from(self.key_columns.len()).context("window key too wide")?;
            key.extend_from_slice(&count.to_le_bytes());
            for column_idx in 0..self.key_columns.len() {
                append_arrow_key_value(batch.column(column_idx).as_ref(), row_idx, &mut key)?;
            }
            let (source_idx, diff) = staged_rows
                .get(row_idx)
                .ok_or_else(|| anyhow!("vectorized key extractor row index out of bounds"))?;
            let row = rows
                .get(*source_idx)
                .ok_or_else(|| anyhow!("vectorized key extractor source index out of bounds"))?
                .0
                .clone();
            deltas.push(VectorizedKeyedTimeDelta {
                row,
                diff: *diff,
                key,
                event_ts,
                batch_row: row_idx,
            });
        }
        Ok(Some(VectorizedKeyedTimeBatch {
            batch,
            input_positions,
            deltas,
        }))
    }
}

fn projected_arrow_schema(input_schema: &SchemaRef, columns: &[usize]) -> Result<SchemaRef> {
    let fields = columns
        .iter()
        .map(|idx| {
            input_schema
                .fields()
                .get(*idx)
                .map(|field| (**field).clone())
                .ok_or_else(|| {
                    anyhow!(
                        "vectorized key input column {idx} is out of bounds for schema width {}",
                        input_schema.fields().len()
                    )
                })
        })
        .collect::<Result<Vec<Field>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn arrow_i64_like_value(array: &dyn Array, row_idx: usize) -> Result<i64> {
    match array.data_type() {
        DataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("expected Int64 time array"))?;
            Ok(values.value(row_idx))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| anyhow!("expected TimestampMillisecond time array"))?;
            Ok(values.value(row_idx))
        }
        other => bail!("unsupported Arrow i64-like time type: {other:?}"),
    }
}

fn append_arrow_key_value(array: &dyn Array, row_idx: usize, encoded: &mut Vec<u8>) -> Result<()> {
    match array.data_type() {
        DataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("expected Int64 key array"))?;
            encoded.push(0x01);
            encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
        }
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("expected Utf8 key array"))?;
            encoded.push(0x02);
            let bytes = values.value(row_idx).as_bytes();
            let len = u32::try_from(bytes.len()).context("utf8 join key too large")?;
            encoded.extend_from_slice(&len.to_le_bytes());
            encoded.extend_from_slice(bytes);
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| anyhow!("expected TimestampMillisecond key array"))?;
            encoded.push(0x03);
            encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
        }
        DataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("expected Boolean key array"))?;
            encoded.push(0x04);
            encoded.push(u8::from(values.value(row_idx)));
        }
        DataType::Date32 => {
            let values = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| anyhow!("expected Date32 key array"))?;
            encoded.push(0x09);
            encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
        }
        DataType::Decimal128(_, _) => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| anyhow!("expected Decimal128 key array"))?;
            encoded.push(0x0B);
            encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
        }
        other => bail!("unsupported Arrow join key type: {other:?}"),
    }
    Ok(())
}

#[cfg(test)]
fn source_table(source_name: &str) -> Option<&'static TableDescriptor> {
    match source_name {
        "nexmark_person" => Some(nexmark_person_table()),
        "person" => Some(nexmark_person_alias_table()),
        "nexmark_auction" => Some(nexmark_auction_table()),
        "auction" => Some(nexmark_auction_alias_table()),
        "nexmark_bid" => Some(nexmark_bid_table()),
        "bid" => Some(nexmark_bid_alias_table()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, BinaryArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn person_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn person_row(id: i64, name: &str) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(4 + 9 + 8 + name.len());
        encoded.extend_from_slice(&(2_u32).to_le_bytes());
        encoded.push(0x01);
        encoded.extend_from_slice(&id.to_le_bytes());
        encoded.push(0x02);
        let bytes = name.as_bytes();
        encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        encoded.extend_from_slice(bytes);
        encoded
    }

    fn pruned_time_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("url", DataType::Utf8, false),
            Field::new(
                "event_ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]))
    }

    fn pruned_time_row(id: i64, event_ts: i64) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&(3_u32).to_le_bytes());
        encoded.push(0x01);
        encoded.extend_from_slice(&id.to_le_bytes());
        encoded.push(0x06);
        encoded.push(0x03);
        encoded.extend_from_slice(&event_ts.to_le_bytes());
        encoded
    }

    #[test]
    fn generates_key_column_for_primary_key_sources() {
        let batch = build_source_delta_batch(
            "nexmark_person",
            person_schema(),
            vec![(person_row(7, "alice"), 1)],
        )
        .expect("delta batch");
        assert_eq!(batch.num_columns(), 4);
        let keys = batch
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("key array");
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn key_encoding_is_stable_across_batches() {
        let batch_one = build_source_delta_batch(
            "nexmark_person",
            person_schema(),
            vec![(person_row(11, "alice"), 1)],
        )
        .expect("batch one");
        let batch_two = build_source_delta_batch(
            "nexmark_person",
            person_schema(),
            vec![(person_row(11, "alice"), -1)],
        )
        .expect("batch two");

        let keys_one = batch_one
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("key array one");
        let keys_two = batch_two
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("key array two");
        assert_eq!(keys_one.value(0), keys_two.value(0));
    }

    #[test]
    fn vectorized_key_extractor_projects_keys_from_delta_batch() {
        let extractor =
            VectorizedEncodedKeyExtractor::new(person_schema(), Arc::new(vec![0])).expect("key");
        let row_a = person_row(11, "alice");
        let row_b = person_row(12, "bob");

        let keyed = extractor
            .extract_keyed_deltas(&[(row_a.clone(), 1), (row_b.clone(), -1), (row_b.clone(), 0)])
            .expect("extract");

        assert_eq!(keyed.len(), 2);
        assert_eq!(keyed[0].1, row_a);
        assert_eq!(keyed[0].2, 1);
        assert_eq!(keyed[1].1, row_b);
        assert_eq!(keyed[1].2, -1);
        assert_ne!(keyed[0].0, keyed[1].0);
    }

    #[test]
    fn vectorized_key_extractor_ignores_unneeded_pruned_columns() {
        let extractor = VectorizedEncodedKeyExtractor::new(pruned_time_schema(), Arc::new(vec![0]))
            .expect("key");
        let row = pruned_time_row(42, 1_700_000);

        let keyed_time = extractor
            .extract_keyed_time_deltas(&[(row.clone(), 1)], 2)
            .expect("extract keyed time");

        assert_eq!(keyed_time.len(), 1);
        assert_eq!(keyed_time[0].0, row);
        assert_eq!(keyed_time[0].1, 1);
        assert_eq!(keyed_time[0].3, 1_700_000);
    }
}
