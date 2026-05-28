use std::mem::size_of;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;

use dbsp::circuit::{KEY_COLUMN_NAME, WEIGHT_COLUMN_NAME};

use crate::encoding::{
    EncodedRowScalar, decode_all_encoded_row_scalars_into, decode_encoded_row_scalars_into,
};
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
    input_columns: Option<Arc<[usize]>>,
    computed_key_columns: Option<Arc<[usize]>>,
    columns: Vec<ScalarColumnBuilder>,
    row_count: usize,
    weights: Vec<Diff>,
    keys: Option<Vec<Vec<u8>>>,
    estimated_bytes: usize,
    decode_scratch: Vec<Option<EncodedRowScalar>>,
}

impl DeltaBatchBuffer {
    pub fn new(
        base_schema: SchemaRef,
        include_key: bool,
        config: DeltaBatchConfig,
    ) -> Result<Self> {
        Self::new_with_input_columns(base_schema, None, include_key, None, false, config)
    }

    pub fn new_keyed(
        base_schema: SchemaRef,
        key_columns: Arc<[usize]>,
        config: DeltaBatchConfig,
    ) -> Result<Self> {
        for column in key_columns.iter().copied() {
            if column >= base_schema.fields().len() {
                bail!(
                    "key column {} is outside schema width {}",
                    column,
                    base_schema.fields().len()
                );
            }
        }
        Self::new_with_input_columns(base_schema, None, true, Some(key_columns), false, config)
    }

    pub fn new_projected(
        base_schema: SchemaRef,
        input_columns: Arc<[usize]>,
        include_key: bool,
        config: DeltaBatchConfig,
    ) -> Result<Self> {
        if base_schema.fields().len() != input_columns.len() {
            bail!(
                "projected delta buffer schema has {} fields but {} input columns were requested",
                base_schema.fields().len(),
                input_columns.len()
            );
        }
        Self::new_with_input_columns(
            base_schema,
            Some(input_columns),
            include_key,
            None,
            true,
            config,
        )
    }

    fn new_with_input_columns(
        base_schema: SchemaRef,
        input_columns: Option<Arc<[usize]>>,
        include_key: bool,
        computed_key_columns: Option<Arc<[usize]>>,
        payload_nullable: bool,
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
            .map(|field| {
                let field = (**field).clone();
                if payload_nullable {
                    field.with_nullable(true)
                } else {
                    field
                }
            })
            .collect();
        if include_key {
            fields.push(Field::new(KEY_COLUMN_NAME, DataType::Binary, false));
        }
        fields.push(Field::new(WEIGHT_COLUMN_NAME, DataType::Int64, false));
        let delta_schema = Arc::new(Schema::new(fields));
        let initial_capacity = initial_column_capacity(&config);
        let columns = base_schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), initial_capacity))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            base_schema,
            delta_schema,
            config,
            input_columns,
            computed_key_columns,
            columns,
            row_count: 0,
            weights: Vec::new(),
            keys: include_key.then(Vec::new),
            estimated_bytes: 0,
            decode_scratch: Vec::new(),
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
        if let Some(input_columns) = self.input_columns.as_ref() {
            decode_encoded_row_scalars_into(&row, input_columns, &mut self.decode_scratch)?;
        } else {
            decode_all_encoded_row_scalars_into(&row, &mut self.decode_scratch)?;
        }
        if self.decode_scratch.len() != self.base_schema.fields().len() {
            return Err(anyhow!(
                "encoded row has {} columns but schema has {}",
                self.decode_scratch.len(),
                self.base_schema.fields().len()
            ));
        }

        match (&mut self.keys, key, self.computed_key_columns.is_some()) {
            (Some(_), Some(_), true) => bail!("delta buffer computes key bytes from Arrow columns"),
            (Some(keys), Some(key), false) => {
                self.estimated_bytes += key.len();
                keys.push(key);
            }
            (Some(_), None, false) => bail!("delta buffer expects key bytes"),
            (Some(_), None, true) => {}
            (None, Some(_), _) => bail!("delta buffer does not accept keys"),
            (None, None, _) => {}
        }

        self.estimated_bytes += estimate_encoded_row_bytes(&row);
        for (idx, value) in self.decode_scratch.iter().enumerate() {
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

        let rows = self.row_count;
        let mut arrays: Vec<ArrayRef> = self
            .columns
            .iter_mut()
            .enumerate()
            .map(|(_idx, col)| Ok(col.finish_array()))
            .collect::<Result<_>>()?;

        if let Some(keys) = self.keys.as_mut() {
            let mut key_builder = ScalarColumnBuilder::new(&DataType::Binary, rows)?;
            if let Some(key_columns) = self.computed_key_columns.as_ref() {
                for row_idx in 0..rows {
                    let key = encode_arrow_key_columns(&arrays, key_columns, row_idx)?;
                    self.estimated_bytes += key.len();
                    key_builder.append_binary_value(&key)?;
                }
            } else {
                for key in keys.drain(..) {
                    key_builder.append_binary_value(&key)?;
                }
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

fn encode_arrow_key_columns(
    arrays: &[ArrayRef],
    key_columns: &[usize],
    row_idx: usize,
) -> Result<Vec<u8>> {
    let count = u32::try_from(key_columns.len()).map_err(|_| anyhow!("too many key columns"))?;
    let mut encoded = Vec::with_capacity(4 + key_columns.len().saturating_mul(16));
    encoded.extend_from_slice(&count.to_le_bytes());
    for column in key_columns {
        let array = arrays
            .get(*column)
            .ok_or_else(|| anyhow!("key column {column} is outside materialized batch"))?;
        if array.is_null(row_idx) {
            bail!("key column {column} evaluated to NULL");
        }
        append_arrow_key_value(array.as_ref(), row_idx, &mut encoded)?;
    }
    Ok(encoded)
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
            let len = u32::try_from(bytes.len()).map_err(|_| anyhow!("utf8 key too large"))?;
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
        other => bail!("unsupported Arrow key type: {other:?}"),
    }
    Ok(())
}

fn initial_column_capacity(config: &DeltaBatchConfig) -> usize {
    match config.max_rows {
        0 => 0,
        usize::MAX => DeltaBatchConfig::default().max_rows,
        rows => rows,
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
