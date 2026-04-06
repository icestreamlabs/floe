use std::mem::size_of;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;

use dbsp::circuit::{KEY_COLUMN_NAME, WEIGHT_COLUMN_NAME};

use crate::encoding::decode_all_encoded_row_scalars;
use crate::metrics;
use crate::scalar_array_builder::ScalarColumnBuilder;
use crate::stream_types::Diff;

#[derive(Clone, Copy, Debug)]
pub enum FlushReason {
    MaxRows,
    MaxBytes,
    Manual,
}

#[derive(Clone, Debug)]
pub struct DeltaBatchConfig {
    pub max_rows: usize,
    pub max_bytes: usize,
}

impl Default for DeltaBatchConfig {
    fn default() -> Self {
        Self {
            max_rows: 4096,
            max_bytes: 4 * 1024 * 1024,
        }
    }
}

pub struct DeltaBatchBuffer {
    base_schema: SchemaRef,
    delta_schema: SchemaRef,
    config: DeltaBatchConfig,
    columns: Vec<ScalarColumnBuilder>,
    row_count: usize,
    weights: Vec<Diff>,
    keys: Option<Vec<Vec<u8>>>,
    estimated_bytes: usize,
}

impl DeltaBatchBuffer {
    pub fn new(
        base_schema: SchemaRef,
        include_key: bool,
        config: DeltaBatchConfig,
    ) -> Result<Self> {
        if base_schema.index_of(WEIGHT_COLUMN_NAME).is_ok()
            || base_schema.index_of(KEY_COLUMN_NAME).is_ok()
        {
            bail!(
                "base schema must not contain reserved delta columns {} or {}",
                WEIGHT_COLUMN_NAME,
                KEY_COLUMN_NAME
            );
        }

        let mut fields: Vec<Field> = base_schema
            .fields()
            .iter()
            .map(|field| (**field).clone())
            .collect();
        if include_key {
            fields.push(Field::new(KEY_COLUMN_NAME, DataType::Binary, false));
        }
        fields.push(Field::new(WEIGHT_COLUMN_NAME, DataType::Int64, false));
        let delta_schema = Arc::new(Schema::new(fields));
        let columns = base_schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), config.max_rows))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            base_schema,
            delta_schema,
            config,
            columns,
            row_count: 0,
            weights: Vec::new(),
            keys: include_key.then(Vec::new),
            estimated_bytes: 0,
        })
    }

    pub fn push(
        &mut self,
        row: Vec<u8>,
        weight: Diff,
        key: Option<Vec<u8>>,
    ) -> Result<Option<RecordBatch>> {
        if weight == 0 {
            return Ok(None);
        }
        let decoded = decode_all_encoded_row_scalars(&row)?;
        if decoded.len() != self.base_schema.fields().len() {
            return Err(anyhow!(
                "encoded row has {} columns but schema has {}",
                decoded.len(),
                self.base_schema.fields().len()
            ));
        }

        match (&mut self.keys, key) {
            (Some(keys), Some(key)) => {
                self.estimated_bytes += key.len();
                keys.push(key);
            }
            (Some(_), None) => bail!("delta buffer expects key bytes"),
            (None, Some(_)) => bail!("delta buffer does not accept keys"),
            (None, None) => {}
        }

        self.estimated_bytes += estimate_encoded_row_bytes(&row);
        for (idx, value) in decoded.iter().enumerate() {
            self.columns[idx].append_encoded_scalar(value.as_ref())?;
        }
        self.row_count += 1;
        self.weights.push(weight);

        if let Some(reason) = self.should_flush() {
            return self.flush(reason);
        }
        Ok(None)
    }

    pub fn flush_manual(&mut self) -> Result<Option<RecordBatch>> {
        self.flush(FlushReason::Manual)
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub fn delta_schema(&self) -> SchemaRef {
        Arc::clone(&self.delta_schema)
    }

    fn should_flush(&self) -> Option<FlushReason> {
        if self.row_count >= self.config.max_rows {
            return Some(FlushReason::MaxRows);
        }
        if self.estimated_bytes >= self.config.max_bytes {
            return Some(FlushReason::MaxBytes);
        }
        None
    }

    fn flush(&mut self, reason: FlushReason) -> Result<Option<RecordBatch>> {
        if self.row_count == 0 {
            return Ok(None);
        }

        let mut arrays: Vec<ArrayRef> = self
            .columns
            .iter_mut()
            .enumerate()
            .map(|(_idx, col)| Ok(col.finish_array()))
            .collect::<Result<_>>()?;

        if let Some(keys) = self.keys.as_mut() {
            let mut key_builder = ScalarColumnBuilder::new(&DataType::Binary, keys.len())?;
            for key in keys.drain(..) {
                key_builder.append_binary_value(&key)?;
            }
            arrays.push(key_builder.finish_array());
        }

        let mut weight_builder = ScalarColumnBuilder::new(&DataType::Int64, self.weights.len())?;
        for weight in self.weights.drain(..) {
            weight_builder.append_i64_value(weight)?;
        }
        arrays.push(weight_builder.finish_array());

        let batch = RecordBatch::try_new(Arc::clone(&self.delta_schema), arrays)
            .map_err(anyhow::Error::from)?;

        let rows = batch.num_rows();
        let bytes = self.estimated_bytes;
        self.row_count = 0;
        self.estimated_bytes = 0;
        metrics::observe_delta_batch(rows, bytes);
        metrics::inc_delta_batch_flushes();
        tracing::debug!(rows, bytes, ?reason, "delta batch flushed");

        Ok(Some(batch))
    }
}

fn estimate_encoded_row_bytes(row: &[u8]) -> usize {
    size_of::<Diff>() + row.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn row(id: i64, name: &str) -> Vec<u8> {
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

    #[test]
    fn flushes_on_row_threshold() {
        let config = DeltaBatchConfig {
            max_rows: 2,
            max_bytes: usize::MAX,
        };
        let mut buffer = DeltaBatchBuffer::new(schema(), false, config).expect("buffer");
        assert!(buffer.push(row(1, "a"), 1, None).unwrap().is_none());
        let batch = buffer.push(row(2, "b"), 1, None).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(buffer.is_empty());
    }

    #[test]
    fn flushes_on_byte_threshold() {
        let config = DeltaBatchConfig {
            max_rows: usize::MAX,
            max_bytes: 1,
        };
        let mut buffer = DeltaBatchBuffer::new(schema(), false, config).expect("buffer");
        let batch = buffer.push(row(1, "a"), 1, None).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(buffer.is_empty());
    }

    #[test]
    fn manual_flush_drains_remaining_rows() {
        let config = DeltaBatchConfig {
            max_rows: 10,
            max_bytes: usize::MAX,
        };
        let mut buffer = DeltaBatchBuffer::new(schema(), false, config).expect("buffer");
        assert!(buffer.push(row(1, "a"), 1, None).unwrap().is_none());
        let batch = buffer.flush_manual().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(buffer.is_empty());
    }
}
