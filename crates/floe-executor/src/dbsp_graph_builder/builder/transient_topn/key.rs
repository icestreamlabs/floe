use super::*;
use crate::delta_batch::{DeltaBatchBuffer, DeltaBatchConfig};
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;

type MaterializedTopNKeyBatch = (RecordBatch, Vec<(Vec<u8>, i64)>);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct TransientTopNSortSpec {
    ascending: bool,
    nulls_first: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum TransientTopNValue {
    Null,
    Int64(i64),
    Timestamp(i64),
    Utf8(String),
    Bool(bool),
}

impl Ord for TransientTopNValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use TransientTopNValue::*;
        let rank = |value: &TransientTopNValue| -> u8 {
            match value {
                Null => 0,
                Int64(_) => 1,
                Timestamp(_) => 2,
                Utf8(_) => 3,
                Bool(_) => 4,
            }
        };

        let left_rank = rank(self);
        let right_rank = rank(other);
        if left_rank != right_rank {
            return left_rank.cmp(&right_rank);
        }

        match (self, other) {
            (Null, Null) => std::cmp::Ordering::Equal,
            (Int64(a), Int64(b)) => a.cmp(b),
            (Timestamp(a), Timestamp(b)) => a.cmp(b),
            (Utf8(a), Utf8(b)) => a.cmp(b),
            (Bool(a), Bool(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        }
    }
}

impl PartialOrd for TransientTopNValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct TransientTopNKey {
    specs: Arc<Vec<TransientTopNSortSpec>>,
    values: Vec<TransientTopNValue>,
    pub(super) tie_breaker: Vec<u8>,
}

impl TransientTopNKey {
    pub(super) fn new(
        specs: Arc<Vec<TransientTopNSortSpec>>,
        values: Vec<TransientTopNValue>,
        tie_breaker: Vec<u8>,
    ) -> Self {
        Self {
            specs,
            values,
            tie_breaker,
        }
    }
}

impl Ord for TransientTopNKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for (idx, spec) in self.specs.iter().enumerate() {
            let left = self.values.get(idx);
            let right = other.values.get(idx);
            let (left, right) = match (left, right) {
                (Some(left), Some(right)) => (left, right),
                _ => continue,
            };

            let cmp = match (left, right) {
                (TransientTopNValue::Null, TransientTopNValue::Null) => std::cmp::Ordering::Equal,
                (TransientTopNValue::Null, _) => {
                    if spec.nulls_first {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    }
                }
                (_, TransientTopNValue::Null) => {
                    if spec.nulls_first {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                }
                _ => {
                    let cmp = left.cmp(right);
                    if spec.ascending { cmp } else { cmp.reverse() }
                }
            };

            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }

        self.tie_breaker.cmp(&other.tie_breaker)
    }
}

impl PartialOrd for TransientTopNKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone)]
pub(in crate::dbsp_graph_builder::builder) struct TransientTopNKeyLayout {
    pub(in crate::dbsp_graph_builder::builder) input_schema: Arc<RowSchema>,
    pub(in crate::dbsp_graph_builder::builder) partition_columns: Arc<Vec<usize>>,
    pub(in crate::dbsp_graph_builder::builder) order_columns: Arc<Vec<usize>>,
    pub(in crate::dbsp_graph_builder::builder) order_types: Arc<Vec<DbspScalarType>>,
    pub(in crate::dbsp_graph_builder::builder) precompute_evaluator:
        Option<Arc<VectorizedFilterProjectEvaluator>>,
}

#[derive(Clone)]
pub(super) struct TransientTopNKeyExtractor {
    graph_id: String,
    projected_schema: SchemaRef,
    projected_columns: Arc<Vec<usize>>,
    partition_positions: Arc<Vec<usize>>,
    order_positions: Arc<Vec<usize>>,
    order_value_types: Arc<Vec<DbspScalarType>>,
    order_specs: Arc<Vec<TransientTopNSortSpec>>,
}

pub(super) struct TransientTopNKeyedDelta {
    pub(super) row_key: Vec<u8>,
    pub(super) diff: i64,
    pub(super) partition_key: Option<Vec<u8>>,
    pub(super) order_key: Option<TransientTopNKey>,
}

pub(super) struct TransientDirectPartitionTopNKeyedDelta {
    pub(super) diff: i64,
    pub(super) partition_value: i64,
    pub(super) order_key: TransientTopNKey,
}

pub(super) struct TransientDirectTop1KeyedDelta {
    pub(super) row_key: Vec<u8>,
    pub(super) diff: i64,
    pub(super) partition_key: TransientDirectTop1PartitionKey,
    pub(super) order_value: i64,
}

impl TransientTopNKeyExtractor {
    pub(super) fn for_layout(
        graph_id: impl Into<String>,
        key_layout: &TransientTopNKeyLayout,
        order_specs: Arc<Vec<TransientTopNSortSpec>>,
    ) -> Result<Self> {
        Self::new(
            graph_id,
            Arc::clone(&key_layout.input_schema),
            Arc::clone(&key_layout.partition_columns),
            Arc::clone(&key_layout.order_columns),
            Arc::clone(&key_layout.order_types),
            order_specs,
        )
    }

    pub(super) fn new(
        graph_id: impl Into<String>,
        input_schema: Arc<RowSchema>,
        partition_columns: Arc<Vec<usize>>,
        order_columns: Arc<Vec<usize>>,
        order_value_types: Arc<Vec<DbspScalarType>>,
        order_specs: Arc<Vec<TransientTopNSortSpec>>,
    ) -> Result<Self> {
        let arrow_schema = input_schema.to_arrow_schema();
        let (projected_columns, partition_positions, order_positions) =
            build_topn_projected_positions(partition_columns.as_ref(), order_columns.as_ref());
        let projected_schema = projected_arrow_schema(&arrow_schema, &projected_columns)?;
        Ok(Self {
            graph_id: graph_id.into(),
            projected_schema,
            projected_columns: Arc::new(projected_columns),
            partition_positions: Arc::new(partition_positions),
            order_positions: Arc::new(order_positions),
            order_value_types,
            order_specs,
        })
    }

    pub(super) fn extract_topn(
        &self,
        deltas: &[(Vec<u8>, i64)],
    ) -> Result<Vec<TransientTopNKeyedDelta>> {
        let Some((batch, staged_rows)) = self.materialize_key_batch(deltas)? else {
            return Ok(Vec::new());
        };

        let mut output = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let (row_key, diff) = staged_rows
                .get(row_idx)
                .ok_or_else(|| anyhow!("transient topn key row index out of bounds"))?;
            output.push(TransientTopNKeyedDelta {
                row_key: row_key.clone(),
                diff: *diff,
                partition_key: Some(self.partition_key_from_batch(&batch, row_idx)?),
                order_key: Some(self.order_key_from_batch(&batch, row_idx, row_key)?),
            });
        }
        Ok(output)
    }

    pub(super) fn extract_direct_partition_topn(
        &self,
        deltas: &[(Vec<u8>, i64)],
        partition_idx: usize,
    ) -> Result<Vec<TransientDirectPartitionTopNKeyedDelta>> {
        let Some((batch, staged_rows)) = self.materialize_key_batch(deltas)? else {
            return Ok(Vec::new());
        };
        let partition_position = self.position_for_input_column(partition_idx)?;

        let mut output = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let Some(partition_value) =
                arrow_int64_value(batch.column(partition_position).as_ref(), row_idx)?
            else {
                continue;
            };
            let (row_key, diff) = staged_rows
                .get(row_idx)
                .ok_or_else(|| anyhow!("transient topn key row index out of bounds"))?;
            output.push(TransientDirectPartitionTopNKeyedDelta {
                diff: *diff,
                partition_value,
                order_key: self.order_key_from_batch(&batch, row_idx, row_key)?,
            });
        }
        Ok(output)
    }

    pub(super) fn extract_direct_top1(
        &self,
        deltas: &[(Vec<u8>, i64)],
        partition_layout: TransientDirectTop1PartitionLayout,
        order_idx: usize,
    ) -> Result<Vec<TransientDirectTop1KeyedDelta>> {
        let Some((batch, staged_rows)) = self.materialize_key_batch(deltas)? else {
            return Ok(Vec::new());
        };
        let order_position = self.position_for_input_column(order_idx)?;

        let mut output = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let Some(partition_key) =
                direct_top1_partition_key_from_batch(self, &batch, row_idx, partition_layout)?
            else {
                continue;
            };
            let Some(order_value) =
                arrow_i64_like_value(batch.column(order_position).as_ref(), row_idx)?
            else {
                continue;
            };
            let (row_key, diff) = staged_rows
                .get(row_idx)
                .ok_or_else(|| anyhow!("transient topn key row index out of bounds"))?;
            output.push(TransientDirectTop1KeyedDelta {
                row_key: row_key.clone(),
                diff: *diff,
                partition_key,
                order_value,
            });
        }
        Ok(output)
    }

    fn materialize_key_batch(
        &self,
        deltas: &[(Vec<u8>, i64)],
    ) -> Result<Option<MaterializedTopNKeyBatch>> {
        if deltas.is_empty() {
            return Ok(None);
        }
        let mut buffer = DeltaBatchBuffer::new_projected(
            Arc::clone(&self.projected_schema),
            Arc::<[usize]>::from(self.projected_columns.as_ref().clone()),
            false,
            DeltaBatchConfig {
                max_rows: usize::MAX,
                max_bytes: usize::MAX,
            },
        )
        .context("create transient topn projected key batch")?;
        let mut staged_rows = Vec::with_capacity(deltas.len());
        for (row_key, diff) in deltas {
            if *diff == 0 {
                continue;
            }
            if buffer.push_ref(row_key, *diff, None)?.is_some() {
                bail!("unbounded transient topn key extractor flushed before manual flush");
            }
            staged_rows.push((row_key.clone(), *diff));
        }

        let Some(batch) = buffer.flush_manual()? else {
            return Ok(None);
        };
        Ok(Some((batch, staged_rows)))
    }

    fn partition_key_from_batch(&self, batch: &RecordBatch, row_idx: usize) -> Result<Vec<u8>> {
        if self.partition_positions.is_empty() {
            return Ok(Vec::new());
        }
        encode_arrow_columns(batch, self.partition_positions.as_ref(), row_idx)
    }

    fn order_key_from_batch(
        &self,
        batch: &RecordBatch,
        row_idx: usize,
        row_key: &[u8],
    ) -> Result<TransientTopNKey> {
        let mut values = Vec::with_capacity(self.order_positions.len());
        for (position, expected_type) in self
            .order_positions
            .iter()
            .zip(self.order_value_types.iter())
        {
            values.push(transient_topn_value_from_arrow(
                batch.column(*position).as_ref(),
                row_idx,
                expected_type,
            )?);
        }
        Ok(TransientTopNKey::new(
            Arc::clone(&self.order_specs),
            values,
            row_key.to_vec(),
        ))
    }

    fn position_for_input_column(&self, column_idx: usize) -> Result<usize> {
        self.projected_columns
            .iter()
            .position(|column| *column == column_idx)
            .ok_or_else(|| {
                anyhow!(
                    "transient topn key extractor missing projected column {column_idx} for graph {}",
                    self.graph_id
                )
            })
    }
}

pub(super) fn transient_topn_order_specs(topn: &DbspTopNNode) -> Arc<Vec<TransientTopNSortSpec>> {
    Arc::new(
        topn.order_by()
            .iter()
            .map(|expr| TransientTopNSortSpec {
                ascending: expr.ascending(),
                nulls_first: expr.nulls_first(),
            })
            .collect(),
    )
}

fn build_topn_projected_positions(
    partition_columns: &[usize],
    order_columns: &[usize],
) -> (Vec<usize>, Vec<usize>, Vec<usize>) {
    let mut projected_columns = Vec::<usize>::new();
    let mut position_for = |column_idx: usize| {
        if let Some(position) = projected_columns
            .iter()
            .position(|existing| *existing == column_idx)
        {
            position
        } else {
            projected_columns.push(column_idx);
            projected_columns.len() - 1
        }
    };

    let partition_positions = partition_columns
        .iter()
        .copied()
        .map(&mut position_for)
        .collect::<Vec<_>>();
    let order_positions = order_columns
        .iter()
        .copied()
        .map(&mut position_for)
        .collect::<Vec<_>>();
    (projected_columns, partition_positions, order_positions)
}

pub(super) fn projected_arrow_schema(
    input_schema: &SchemaRef,
    columns: &[usize],
) -> Result<SchemaRef> {
    let fields = columns
        .iter()
        .map(|idx| {
            input_schema
                .fields()
                .get(*idx)
                .map(|field| (**field).clone())
                .ok_or_else(|| {
                    anyhow!(
                        "transient topn input column {idx} is out of bounds for schema width {}",
                        input_schema.fields().len()
                    )
                })
        })
        .collect::<Result<Vec<ArrowField>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

pub(super) fn encode_arrow_columns(
    batch: &RecordBatch,
    positions: &[usize],
    row_idx: usize,
) -> Result<Vec<u8>> {
    let count = u32::try_from(positions.len()).context("too many transient topn key columns")?;
    let mut encoded = Vec::with_capacity(4 + positions.len().saturating_mul(16));
    encoded.extend_from_slice(&count.to_le_bytes());
    for position in positions.iter().copied() {
        append_arrow_encoded_value(batch.column(position).as_ref(), row_idx, &mut encoded)?;
    }
    Ok(encoded)
}

fn append_arrow_encoded_value(
    array: &dyn Array,
    row_idx: usize,
    encoded: &mut Vec<u8>,
) -> Result<()> {
    match array.data_type() {
        DataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("expected Int64 transient topn key array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x05);
            } else {
                encoded.push(0x01);
                encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
            }
        }
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("expected Utf8 transient topn key array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x06);
            } else {
                encoded.push(0x02);
                let bytes = values.value(row_idx).as_bytes();
                let len =
                    u32::try_from(bytes.len()).context("transient topn utf8 key too large")?;
                encoded.extend_from_slice(&len.to_le_bytes());
                encoded.extend_from_slice(bytes);
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| anyhow!("expected TimestampMillisecond transient topn key array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x07);
            } else {
                encoded.push(0x03);
                encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
            }
        }
        DataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("expected Boolean transient topn key array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x08);
            } else {
                encoded.push(0x04);
                encoded.push(u8::from(values.value(row_idx)));
            }
        }
        DataType::Date32 => {
            let values = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| anyhow!("expected Date32 transient topn key array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x0A);
            } else {
                encoded.push(0x09);
                encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
            }
        }
        DataType::Decimal128(_, _) => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| anyhow!("expected Decimal128 transient topn key array"))?;
            if values.is_null(row_idx) {
                encoded.push(0x0C);
            } else {
                encoded.push(0x0B);
                encoded.extend_from_slice(&values.value(row_idx).to_le_bytes());
            }
        }
        other => bail!("unsupported Arrow transient topn key type: {other:?}"),
    }
    Ok(())
}

fn transient_topn_value_from_arrow(
    array: &dyn Array,
    row_idx: usize,
    expected_type: &DbspScalarType,
) -> Result<TransientTopNValue> {
    if array.is_null(row_idx) {
        return Ok(TransientTopNValue::Null);
    }
    match (array.data_type(), expected_type) {
        (DataType::Int64, DbspScalarType::Int64) => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("expected Int64 transient topn order array"))?;
            Ok(TransientTopNValue::Int64(values.value(row_idx)))
        }
        (DataType::Timestamp(TimeUnit::Millisecond, _), DbspScalarType::TimestampMillis) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    anyhow!("expected TimestampMillisecond transient topn order array")
                })?;
            Ok(TransientTopNValue::Timestamp(values.value(row_idx)))
        }
        (DataType::Utf8, DbspScalarType::Utf8) => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("expected Utf8 transient topn order array"))?;
            Ok(TransientTopNValue::Utf8(values.value(row_idx).to_string()))
        }
        (DataType::Boolean, DbspScalarType::Bool) => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("expected Boolean transient topn order array"))?;
            Ok(TransientTopNValue::Bool(values.value(row_idx)))
        }
        (actual, expected) => Err(anyhow!(
            "transient topn order key type mismatch: expected {expected:?}, Arrow column is {actual:?}"
        )),
    }
}

fn arrow_int64_value(array: &dyn Array, row_idx: usize) -> Result<Option<i64>> {
    let values = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("expected Int64 transient topn direct key array"))?;
    if values.is_null(row_idx) {
        Ok(None)
    } else {
        Ok(Some(values.value(row_idx)))
    }
}

fn arrow_i64_like_value(array: &dyn Array, row_idx: usize) -> Result<Option<i64>> {
    if array.is_null(row_idx) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("expected Int64 transient topn direct order array"))?;
            Ok(Some(values.value(row_idx)))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    anyhow!("expected TimestampMillisecond transient topn direct order array")
                })?;
            Ok(Some(values.value(row_idx)))
        }
        other => bail!("unsupported transient topn i64-like direct key type: {other:?}"),
    }
}

fn direct_top1_partition_key_from_batch(
    extractor: &TransientTopNKeyExtractor,
    batch: &RecordBatch,
    row_idx: usize,
    partition_layout: TransientDirectTop1PartitionLayout,
) -> Result<Option<TransientDirectTop1PartitionKey>> {
    let key = match partition_layout {
        TransientDirectTop1PartitionLayout::One(partition_idx) => {
            let position = extractor.position_for_input_column(partition_idx)?;
            let Some(partition_value) =
                arrow_int64_value(batch.column(position).as_ref(), row_idx)?
            else {
                return Ok(None);
            };
            TransientDirectTop1PartitionKey::One(partition_value)
        }
        TransientDirectTop1PartitionLayout::Two(partition_indices) => {
            let first_position = extractor.position_for_input_column(partition_indices[0])?;
            let second_position = extractor.position_for_input_column(partition_indices[1])?;
            let Some(first_partition_value) =
                arrow_int64_value(batch.column(first_position).as_ref(), row_idx)?
            else {
                return Ok(None);
            };
            let Some(second_partition_value) =
                arrow_int64_value(batch.column(second_position).as_ref(), row_idx)?
            else {
                return Ok(None);
            };
            TransientDirectTop1PartitionKey::Two(first_partition_value, second_partition_value)
        }
    };
    Ok(Some(key))
}

#[derive(Clone, Copy)]
pub(in crate::dbsp_graph_builder::builder) enum TransientDirectTop1PartitionLayout {
    One(usize),
    Two([usize; 2]),
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(in crate::dbsp_graph_builder::builder) enum TransientDirectTop1PartitionKey {
    One(i64),
    Two(i64, i64),
}
