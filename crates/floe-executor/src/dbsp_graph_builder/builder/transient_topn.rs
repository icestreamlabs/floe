use super::*;
use crate::delta_batch::{DeltaBatchBuffer, DeltaBatchConfig};
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use std::time::Instant;

#[derive(Clone, Debug, Eq, PartialEq)]
struct TransientTopNSortSpec {
    ascending: bool,
    nulls_first: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TransientTopNValue {
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
struct TransientTopNKey {
    specs: Arc<Vec<TransientTopNSortSpec>>,
    values: Vec<TransientTopNValue>,
    tie_breaker: Vec<u8>,
}

impl TransientTopNKey {
    fn new(
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
pub(super) struct TransientTopNKeyLayout {
    pub(super) input_schema: Arc<RowSchema>,
    pub(super) partition_columns: Arc<Vec<usize>>,
    pub(super) order_columns: Arc<Vec<usize>>,
    pub(super) order_types: Arc<Vec<DbspScalarType>>,
    pub(super) precompute_evaluator: Option<Arc<VectorizedFilterProjectEvaluator>>,
}

#[derive(Clone)]
struct TransientTopNKeyExtractor {
    graph_id: String,
    projected_schema: SchemaRef,
    projected_columns: Arc<Vec<usize>>,
    partition_positions: Arc<Vec<usize>>,
    order_positions: Arc<Vec<usize>>,
    order_value_types: Arc<Vec<DbspScalarType>>,
    order_specs: Arc<Vec<TransientTopNSortSpec>>,
}

struct TransientTopNKeyedDelta {
    row_key: Vec<u8>,
    diff: i64,
    partition_key: Option<Vec<u8>>,
    order_key: Option<TransientTopNKey>,
}

struct TransientDirectPartitionTopNKeyedDelta {
    diff: i64,
    partition_value: i64,
    order_key: TransientTopNKey,
}

struct TransientDirectInt64TopNKeyedDelta {
    row_key: Vec<u8>,
    diff: i64,
    partition_value: i64,
    order_value: i64,
}

struct TransientDirectTop1KeyedDelta {
    row_key: Vec<u8>,
    diff: i64,
    partition_key: TransientDirectTop1PartitionKey,
    order_value: i64,
}

impl TransientTopNKeyExtractor {
    fn for_layout(
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

    fn new(
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

    fn extract_topn(&self, deltas: &[(Vec<u8>, i64)]) -> Result<Vec<TransientTopNKeyedDelta>> {
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

    fn extract_direct_partition_topn(
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

    fn extract_direct_int64_topn(
        &self,
        deltas: &[(Vec<u8>, i64)],
        partition_idx: usize,
        order_idx: usize,
    ) -> Result<Vec<TransientDirectInt64TopNKeyedDelta>> {
        let Some((batch, staged_rows)) = self.materialize_key_batch(deltas)? else {
            return Ok(Vec::new());
        };
        let partition_position = self.position_for_input_column(partition_idx)?;
        let order_position = self.position_for_input_column(order_idx)?;

        let mut output = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let Some(partition_value) =
                arrow_int64_value(batch.column(partition_position).as_ref(), row_idx)?
            else {
                continue;
            };
            let Some(order_value) =
                arrow_int64_value(batch.column(order_position).as_ref(), row_idx)?
            else {
                continue;
            };
            let (row_key, diff) = staged_rows
                .get(row_idx)
                .ok_or_else(|| anyhow!("transient topn key row index out of bounds"))?;
            output.push(TransientDirectInt64TopNKeyedDelta {
                row_key: row_key.clone(),
                diff: *diff,
                partition_value,
                order_value,
            });
        }
        Ok(output)
    }

    fn extract_direct_top1(
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
    ) -> Result<Option<(RecordBatch, Vec<(Vec<u8>, i64)>)>> {
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
            if buffer.push(row_key.clone(), *diff, None)?.is_some() {
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

fn transient_topn_order_specs(topn: &DbspTopNNode) -> Arc<Vec<TransientTopNSortSpec>> {
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
                        "transient topn input column {idx} is out of bounds for schema width {}",
                        input_schema.fields().len()
                    )
                })
        })
        .collect::<Result<Vec<ArrowField>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn encode_arrow_columns(
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

pub(super) struct TransientTopNProcessor {
    graph_id: String,
    key_extractor: TransientTopNKeyExtractor,
    limit: usize,
    offset: usize,
    order_index: BTreeMap<Vec<u8>, BTreeMap<TransientTopNKey, i64>>,
    partition_output_cache: BTreeMap<Vec<u8>, HashMap<Vec<u8>, i64>>,
    profile_enabled: bool,
    profiled_batches: usize,
}

pub(super) struct TransientTop1Processor {
    key_extractor: TransientTopNKeyExtractor,
    order_index: HashMap<Vec<u8>, BTreeMap<(TransientTopNKey, Vec<u8>), i64>>,
    partition_output_cache: HashMap<Vec<u8>, Vec<u8>>,
}

impl TransientTopNProcessor {
    pub(super) fn new(
        graph_id: impl Into<String>,
        topn: &DbspTopNNode,
        key_layout: &TransientTopNKeyLayout,
        _append_only_input: bool,
    ) -> Self {
        let graph_id = graph_id.into();
        let order_specs = transient_topn_order_specs(topn);
        let key_extractor =
            TransientTopNKeyExtractor::for_layout(graph_id.clone(), key_layout, order_specs)
                .expect("transient topn key layout should be valid");
        Self {
            graph_id,
            key_extractor,
            limit: topn.limit(),
            offset: topn.offset(),
            order_index: BTreeMap::new(),
            partition_output_cache: BTreeMap::new(),
            profile_enabled: tracing::enabled!(tracing::Level::DEBUG),
            profiled_batches: 0,
        }
    }

    pub(super) fn apply_deltas(
        &mut self,
        deltas: Vec<(Vec<u8>, i64)>,
    ) -> Result<Vec<(Vec<u8>, i64)>> {
        let input_delta_count = deltas.len();
        let profile_this_batch = self.profile_enabled && self.profiled_batches < 16;
        let total_start = profile_this_batch.then(Instant::now);
        let mut key_eval_us = 0u128;
        let mut mutation_us = 0u128;

        let mut affected_partitions = BTreeSet::new();
        let key_start = profile_this_batch.then(Instant::now);
        let keyed_deltas = self.key_extractor.extract_topn(&deltas)?;
        if let Some(key_start) = key_start {
            key_eval_us += key_start.elapsed().as_micros();
        }
        for keyed in keyed_deltas {
            let TransientTopNKeyedDelta {
                diff,
                partition_key,
                order_key,
                ..
            } = keyed;
            let (Some(partition_key), Some(order_key)) = (partition_key, order_key) else {
                continue;
            };
            affected_partitions.insert(partition_key.clone());

            let mutation_start = profile_this_batch.then(Instant::now);
            let partition_index = self.order_index.entry(partition_key.clone()).or_default();
            let previous_weight = partition_index.get(&order_key).copied().unwrap_or(0);
            let next_weight = previous_weight.saturating_add(diff);
            if next_weight <= 0 {
                partition_index.remove(&order_key);
                if partition_index.is_empty() {
                    self.order_index.remove(&partition_key);
                }
            } else {
                partition_index.insert(order_key, next_weight);
            }
            if let Some(mutation_start) = mutation_start {
                mutation_us += mutation_start.elapsed().as_micros();
            }
        }

        let recompute_start = profile_this_batch.then(Instant::now);
        let mut recompute_rows_scanned = 0usize;
        let mut affected_partition_count = 0usize;
        let mut output_deltas = HashMap::new();
        for partition_key in affected_partitions {
            affected_partition_count += 1;
            let previous_output = self
                .partition_output_cache
                .remove(&partition_key)
                .unwrap_or_default();
            let next_output = self
                .order_index
                .get(&partition_key)
                .map(|partition_index| {
                    if profile_this_batch {
                        recompute_rows_scanned += partition_index.len();
                    }
                    self.compute_partition_topn(partition_index)
                })
                .unwrap_or_default();
            accumulate_weight_deltas(&mut output_deltas, &previous_output, &next_output);
            if !next_output.is_empty() {
                self.partition_output_cache
                    .insert(partition_key, next_output);
            }
        }

        let output_deltas = output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect::<Vec<_>>();

        if profile_this_batch {
            self.profiled_batches += 1;
            let recompute_us = recompute_start
                .expect("recompute start present")
                .elapsed()
                .as_micros();
            let total_us = total_start
                .expect("total start present")
                .elapsed()
                .as_micros();
            tracing::info!(
                graph_id = %self.graph_id,
                input_delta_count,
                affected_partition_count,
                retained_partitions = self.partition_output_cache.len(),
                recompute_rows_scanned,
                output_delta_count = output_deltas.len(),
                key_eval_us,
                mutation_us,
                recompute_us,
                total_us,
                "transient topn batch profile"
            );
        }

        Ok(output_deltas)
    }

    fn compute_partition_topn(
        &self,
        partition_index: &BTreeMap<TransientTopNKey, i64>,
    ) -> HashMap<Vec<u8>, i64> {
        if self.limit == 0 {
            return HashMap::new();
        }

        let mut remaining_skip = self.offset;
        let mut remaining_take = self.limit;
        let mut output = HashMap::new();

        for (order_key, weight) in partition_index {
            if remaining_take == 0 {
                break;
            }

            let mut remaining_weight = *weight;
            if remaining_skip > 0 {
                let available = usize::try_from(remaining_weight).unwrap_or(usize::MAX);
                let skip = remaining_skip.min(available);
                remaining_skip -= skip;
                remaining_weight -= skip as i64;
            }

            if remaining_weight <= 0 {
                continue;
            }

            let available = usize::try_from(remaining_weight).unwrap_or(usize::MAX);
            let take = remaining_take.min(available);
            if take > 0 {
                output.insert(order_key.tie_breaker.clone(), take as i64);
                remaining_take -= take;
            }
        }

        output
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(super) fn snapshot_deltas(&self) -> Vec<(Vec<u8>, i64)> {
        let retain_count = self.offset.saturating_add(self.limit);
        if retain_count == 0 {
            return Vec::new();
        }

        self.order_index
            .values()
            .flat_map(|partition_index| {
                let mut remaining = retain_count;
                partition_index
                    .iter()
                    .filter_map(move |(order_key, weight)| {
                        if remaining == 0 || *weight <= 0 {
                            return None;
                        }
                        let retained = usize::try_from(*weight)
                            .unwrap_or(usize::MAX)
                            .min(remaining);
                        remaining -= retained;
                        Some((order_key.tie_breaker.clone(), retained as i64))
                    })
            })
            .collect()
    }
}

#[derive(Default)]
struct TransientAppendOnlyTopNPartitionState {
    visible_rows: BTreeMap<TransientTopNKey, i64>,
    visible_count: usize,
}

struct TransientAppendOnlyTopNProcessor {
    graph_id: String,
    key_extractor: TransientTopNKeyExtractor,
    limit: usize,
    profile_enabled: bool,
    profiled_batches: usize,
    partitions: HashMap<Vec<u8>, TransientAppendOnlyTopNPartitionState>,
}

impl TransientAppendOnlyTopNProcessor {
    fn new(
        graph_id: impl Into<String>,
        topn: &DbspTopNNode,
        key_layout: &TransientTopNKeyLayout,
    ) -> Self {
        let graph_id = graph_id.into();
        let order_specs = transient_topn_order_specs(topn);
        let key_extractor =
            TransientTopNKeyExtractor::for_layout(graph_id.clone(), key_layout, order_specs)
                .expect("transient topn key layout should be valid");
        Self {
            graph_id,
            key_extractor,
            limit: topn.limit(),
            profile_enabled: tracing::enabled!(tracing::Level::DEBUG),
            profiled_batches: 0,
            partitions: HashMap::new(),
        }
    }

    fn apply_deltas(&mut self, deltas: Vec<(Vec<u8>, i64)>) -> Result<Vec<(Vec<u8>, i64)>> {
        let input_delta_count = deltas.len();
        let profile_this_batch = self.profile_enabled && self.profiled_batches < 16;
        let total_start = profile_this_batch.then(Instant::now);
        let mut key_eval_us = 0u128;
        let mut partition_apply_us = 0u128;
        let mut trimmed_rows = 0usize;
        let mut skipped_rows = 0usize;
        let mut affected_partitions = HashSet::new();
        let mut output_deltas = HashMap::new();

        let key_start = profile_this_batch.then(Instant::now);
        let keyed_deltas = self.key_extractor.extract_topn(&deltas)?;
        if let Some(key_start) = key_start {
            key_eval_us += key_start.elapsed().as_micros();
        }
        for keyed in keyed_deltas {
            let TransientTopNKeyedDelta {
                diff,
                partition_key,
                order_key,
                ..
            } = keyed;
            if diff < 0 {
                bail!(
                    "append-only transient topn received negative diff for graph {}",
                    self.graph_id
                );
            }

            let (Some(partition_key), Some(order_key)) = (partition_key, order_key) else {
                continue;
            };

            affected_partitions.insert(partition_key.clone());
            let apply_start = profile_this_batch.then(Instant::now);
            let state = self.partitions.entry(partition_key).or_default();
            Self::apply_positive_delta(
                state,
                order_key,
                diff,
                self.limit,
                &mut output_deltas,
                &mut trimmed_rows,
                &mut skipped_rows,
            );
            if let Some(apply_start) = apply_start {
                partition_apply_us += apply_start.elapsed().as_micros();
            }
        }

        let output_deltas = output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect::<Vec<_>>();

        if profile_this_batch {
            self.profiled_batches += 1;
            let total_us = total_start
                .expect("total start present")
                .elapsed()
                .as_micros();
            tracing::info!(
                graph_id = %self.graph_id,
                input_delta_count,
                affected_partition_count = affected_partitions.len(),
                retained_partitions = self.partitions.len(),
                trimmed_rows,
                skipped_rows,
                output_delta_count = output_deltas.len(),
                key_eval_us,
                partition_apply_us,
                total_us,
                "transient append-only topn profile"
            );
        }

        Ok(output_deltas)
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn snapshot_deltas(&self) -> Vec<(Vec<u8>, i64)> {
        self.partitions
            .values()
            .flat_map(|state| {
                state.visible_rows.iter().filter_map(|(order_key, weight)| {
                    (*weight > 0).then_some((order_key.tie_breaker.clone(), *weight))
                })
            })
            .collect()
    }

    fn apply_positive_delta(
        state: &mut TransientAppendOnlyTopNPartitionState,
        order_key: TransientTopNKey,
        diff: i64,
        limit: usize,
        output_deltas: &mut HashMap<Vec<u8>, i64>,
        trimmed_rows: &mut usize,
        skipped_rows: &mut usize,
    ) {
        if limit == 0 {
            return;
        }

        if state.visible_count >= limit
            && let Some((worst_key, _)) = state.visible_rows.last_key_value()
            && order_key > *worst_key
        {
            *skipped_rows = skipped_rows.saturating_add(diff as usize);
            return;
        }

        let row_key = order_key.tie_breaker.clone();
        let entry = state.visible_rows.entry(order_key).or_insert(0);
        *entry = entry.saturating_add(diff);
        state.visible_count = state.visible_count.saturating_add(diff as usize);
        accumulate_single_weight_delta(output_deltas, row_key, diff);

        while state.visible_count > limit {
            let overflow = state.visible_count - limit;
            let Some((worst_key, worst_weight)) = state
                .visible_rows
                .last_key_value()
                .map(|(key, weight)| (key.clone(), *weight))
            else {
                break;
            };
            let removable = usize::try_from(worst_weight)
                .unwrap_or(usize::MAX)
                .min(overflow) as i64;
            if removable <= 0 {
                break;
            }
            if let Some(weight) = state.visible_rows.get_mut(&worst_key) {
                *weight -= removable;
                if *weight <= 0 {
                    state.visible_rows.remove(&worst_key);
                }
            }
            state.visible_count -= removable as usize;
            *trimmed_rows = trimmed_rows.saturating_add(removable as usize);
            accumulate_single_weight_delta(
                output_deltas,
                worst_key.tie_breaker.clone(),
                -removable,
            );
        }
    }
}

impl TransientTop1Processor {
    pub(super) fn new(
        graph_id: impl Into<String>,
        topn: &DbspTopNNode,
        key_layout: &TransientTopNKeyLayout,
    ) -> Self {
        let graph_id = graph_id.into();
        let order_specs = transient_topn_order_specs(topn);
        let key_extractor =
            TransientTopNKeyExtractor::for_layout(graph_id.clone(), key_layout, order_specs)
                .expect("transient topn key layout should be valid");
        Self {
            key_extractor,
            order_index: HashMap::new(),
            partition_output_cache: HashMap::new(),
        }
    }

    pub(super) fn apply_deltas(
        &mut self,
        deltas: Vec<(Vec<u8>, i64)>,
    ) -> Result<Vec<(Vec<u8>, i64)>> {
        let mut output_deltas = HashMap::new();
        for keyed in self.key_extractor.extract_topn(&deltas)? {
            let TransientTopNKeyedDelta {
                row_key,
                diff,
                partition_key,
                order_key,
            } = keyed;
            let (Some(partition_key), Some(order_key)) = (partition_key, order_key) else {
                continue;
            };

            let previous_top = self.partition_output_cache.get(&partition_key).cloned();
            let partition_now_empty = {
                let partition_index = self.order_index.entry(partition_key.clone()).or_default();
                let index_key = (order_key, row_key.clone());
                let previous_weight = partition_index.get(&index_key).copied().unwrap_or(0);
                let next_weight = previous_weight.saturating_add(diff);
                if next_weight <= 0 {
                    partition_index.remove(&index_key);
                } else {
                    partition_index.insert(index_key, next_weight);
                }
                partition_index.is_empty()
            };

            let next_top = if partition_now_empty {
                self.order_index.remove(&partition_key);
                None
            } else {
                self.order_index
                    .get(&partition_key)
                    .and_then(|partition_index| {
                        partition_index
                            .first_key_value()
                            .map(|((_order_key, row_key), _)| row_key.clone())
                    })
            };

            if previous_top == next_top {
                continue;
            }
            if let Some(previous_top) = previous_top {
                let entry = output_deltas.entry(previous_top).or_insert(0);
                *entry -= 1;
            }
            match next_top {
                Some(next_top) => {
                    let entry = output_deltas.entry(next_top.clone()).or_insert(0);
                    *entry += 1;
                    self.partition_output_cache.insert(partition_key, next_top);
                }
                None => {
                    self.partition_output_cache.remove(&partition_key);
                }
            }
        }

        Ok(output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect())
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(super) fn snapshot_deltas(&self) -> Vec<(Vec<u8>, i64)> {
        self.partition_output_cache
            .iter()
            .filter_map(|(partition_key, row_key)| {
                let weight = self.order_index.get(partition_key)?.iter().find_map(
                    |((_order_key, candidate_row), weight)| {
                        (candidate_row == row_key).then_some(weight)
                    },
                )?;
                (*weight > 0).then_some((row_key.clone(), 1))
            })
            .collect()
    }
}

#[derive(Clone)]
struct TransientBatchTopNPartitionUpdate {
    row_key: Vec<u8>,
    order_key: TransientTopNKey,
    diff: i64,
}

#[derive(Clone)]
struct TransientBatchTopNLiveRow {
    order_key: TransientTopNKey,
    weight: i64,
}

#[derive(Default)]
struct TransientBatchTopNPartitionState {
    live_rows: HashMap<Vec<u8>, TransientBatchTopNLiveRow>,
    output_rows: Vec<(Vec<u8>, i64)>,
}

#[derive(Clone, Copy)]
struct TransientDirectInt64TopNConfig {
    partition_idx: usize,
    order_idx: usize,
    ascending: bool,
}

#[derive(Clone, Copy)]
pub(super) enum TransientDirectTop1PartitionLayout {
    One(usize),
    Two([usize; 2]),
}

#[derive(Clone)]
pub(super) struct TransientDirectTop1Config {
    pub(super) partition_layout: TransientDirectTop1PartitionLayout,
    pub(super) order_idx: usize,
    pub(super) ascending: bool,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) enum TransientDirectTop1PartitionKey {
    One(i64),
    Two(i64, i64),
}

#[derive(Clone)]
struct TransientDirectTop1PartitionUpdate {
    row_key: Vec<u8>,
    order_value: i64,
    diff: i64,
}

#[derive(Clone)]
pub(super) struct TransientDirectTop1LiveRow {
    order_value: i64,
    weight: i64,
}

#[derive(Default)]
pub(super) struct TransientDirectTop1PartitionState {
    pub(super) live_rows: HashMap<Vec<u8>, TransientDirectTop1LiveRow>,
    top_row: Option<Vec<u8>>,
}

#[derive(Clone)]
struct TransientDirectInt64TopNPartitionUpdate {
    row_key: Vec<u8>,
    order_value: i64,
    diff: i64,
}

#[derive(Clone)]
struct TransientDirectInt64TopNLiveRow {
    order_value: i64,
    weight: i64,
}

#[derive(Default)]
struct TransientDirectInt64TopNPartitionState {
    live_rows: HashMap<Vec<u8>, TransientDirectInt64TopNLiveRow>,
    output_rows: Vec<(Vec<u8>, i64)>,
}

#[derive(Clone)]
struct TransientDirectPartitionTopNConfig {
    partition_idx: usize,
}

struct TransientDirectPartitionTopNProcessor {
    graph_id: String,
    partition_idx: usize,
    key_extractor: TransientTopNKeyExtractor,
    limit: usize,
    offset: usize,
    order_index: HashMap<i64, BTreeMap<TransientTopNKey, i64>>,
    partition_output_cache: HashMap<i64, HashMap<Vec<u8>, i64>>,
    profile_enabled: bool,
    profiled_batches: usize,
}

struct TransientDirectInt64TopNProcessor {
    graph_id: String,
    partition_idx: usize,
    order_idx: usize,
    ascending: bool,
    limit: usize,
    key_extractor: TransientTopNKeyExtractor,
    partitions: HashMap<i64, TransientDirectInt64TopNPartitionState>,
    profile_enabled: bool,
    profiled_batches: usize,
}

pub(super) struct TransientDirectTop1Processor {
    graph_id: String,
    partition_layout: TransientDirectTop1PartitionLayout,
    order_idx: usize,
    ascending: bool,
    compact_append_only_state: bool,
    key_extractor: TransientTopNKeyExtractor,
    pub(super) partitions:
        HashMap<TransientDirectTop1PartitionKey, TransientDirectTop1PartitionState>,
    profile_enabled: bool,
    profiled_batches: usize,
}

struct TransientBatchTopNProcessor {
    graph_id: String,
    key_extractor: TransientTopNKeyExtractor,
    limit: usize,
    partitions: HashMap<Vec<u8>, TransientBatchTopNPartitionState>,
    profile_enabled: bool,
    profiled_batches: usize,
}

impl TransientBatchTopNProcessor {
    fn new(
        graph_id: impl Into<String>,
        topn: &DbspTopNNode,
        key_layout: &TransientTopNKeyLayout,
        _append_only_input: bool,
    ) -> Self {
        let graph_id = graph_id.into();
        let order_specs = transient_topn_order_specs(topn);
        let key_extractor =
            TransientTopNKeyExtractor::for_layout(graph_id.clone(), key_layout, order_specs)
                .expect("transient topn key layout should be valid");
        Self {
            graph_id,
            key_extractor,
            limit: topn.limit(),
            partitions: HashMap::new(),
            profile_enabled: tracing::enabled!(tracing::Level::DEBUG),
            profiled_batches: 0,
        }
    }

    fn apply_deltas(&mut self, deltas: Vec<(Vec<u8>, i64)>) -> Result<Vec<(Vec<u8>, i64)>> {
        let input_delta_count = deltas.len();
        let profile_this_batch = self.profile_enabled && self.profiled_batches < 16;
        let total_start = profile_this_batch.then(Instant::now);
        let mut key_eval_us = 0u128;

        let grouping_start = profile_this_batch.then(Instant::now);
        let mut partition_updates =
            HashMap::<Vec<u8>, Vec<TransientBatchTopNPartitionUpdate>>::new();
        let key_start = profile_this_batch.then(Instant::now);
        let keyed_deltas = self.key_extractor.extract_topn(&deltas)?;
        if let Some(key_start) = key_start {
            key_eval_us += key_start.elapsed().as_micros();
        }
        for keyed in keyed_deltas {
            let TransientTopNKeyedDelta {
                row_key,
                diff,
                partition_key,
                order_key,
            } = keyed;
            let (Some(partition_key), Some(order_key)) = (partition_key, order_key) else {
                continue;
            };
            partition_updates.entry(partition_key).or_default().push(
                TransientBatchTopNPartitionUpdate {
                    row_key,
                    order_key,
                    diff,
                },
            );
        }
        let grouping_us = grouping_start
            .map(|start| start.elapsed().as_micros())
            .unwrap_or(0);

        let partition_apply_start = profile_this_batch.then(Instant::now);
        let mut output_deltas = HashMap::new();
        let mut affected_partition_count = 0usize;
        let mut candidate_rows_considered = 0usize;
        let mut exact_rows_sorted = 0usize;
        for (partition_key, updates) in partition_updates {
            affected_partition_count += 1;
            let mut state = self.partitions.remove(&partition_key).unwrap_or_default();
            let previous_output = std::mem::take(&mut state.output_rows);
            let next_output = self.apply_partition_updates(
                &mut state,
                &previous_output,
                &updates,
                &mut candidate_rows_considered,
                &mut exact_rows_sorted,
            );
            Self::accumulate_output_row_deltas(&mut output_deltas, &previous_output, &next_output);
            state.output_rows = next_output;
            if !state.live_rows.is_empty() {
                self.partitions.insert(partition_key, state);
            }
        }
        let partition_apply_us = partition_apply_start
            .map(|start| start.elapsed().as_micros())
            .unwrap_or(0);

        let output_deltas = output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect::<Vec<_>>();

        if profile_this_batch {
            self.profiled_batches += 1;
            let total_us = total_start
                .expect("total start present")
                .elapsed()
                .as_micros();
            tracing::info!(
                graph_id = %self.graph_id,
                input_delta_count,
                affected_partition_count,
                retained_partitions = self.partitions.len(),
                candidate_rows_considered,
                exact_rows_sorted,
                output_delta_count = output_deltas.len(),
                key_eval_us,
                grouping_us,
                partition_apply_us,
                total_us,
                "transient batch topn profile"
            );
        }

        Ok(output_deltas)
    }

    fn apply_partition_updates(
        &self,
        state: &mut TransientBatchTopNPartitionState,
        previous_output: &[(Vec<u8>, i64)],
        updates: &[TransientBatchTopNPartitionUpdate],
        candidate_rows_considered: &mut usize,
        exact_rows_sorted: &mut usize,
    ) -> Vec<(Vec<u8>, i64)> {
        if updates.iter().all(|update| update.diff > 0) {
            self.apply_partition_updates_append_only(
                state,
                previous_output,
                updates,
                candidate_rows_considered,
            )
        } else {
            self.apply_partition_updates_exact(state, updates, exact_rows_sorted)
        }
    }

    fn apply_partition_updates_append_only(
        &self,
        state: &mut TransientBatchTopNPartitionState,
        previous_output: &[(Vec<u8>, i64)],
        updates: &[TransientBatchTopNPartitionUpdate],
        candidate_rows_considered: &mut usize,
    ) -> Vec<(Vec<u8>, i64)> {
        let mut updated_rows = Vec::with_capacity(updates.len());
        for update in updates {
            let next_weight = Self::apply_live_row_update(state, update);
            if next_weight > 0 {
                updated_rows.push(update.row_key.clone());
            }
        }

        updated_rows.sort_by(|left, right| {
            let left_key = &state
                .live_rows
                .get(left)
                .expect("updated row must exist after append-only update")
                .order_key;
            let right_key = &state
                .live_rows
                .get(right)
                .expect("updated row must exist after append-only update")
                .order_key;
            left_key.cmp(right_key)
        });
        updated_rows.dedup();

        *candidate_rows_considered += previous_output.len() + updated_rows.len();
        self.merge_output_rows(state, previous_output, &updated_rows)
    }

    fn apply_partition_updates_exact(
        &self,
        state: &mut TransientBatchTopNPartitionState,
        updates: &[TransientBatchTopNPartitionUpdate],
        exact_rows_sorted: &mut usize,
    ) -> Vec<(Vec<u8>, i64)> {
        for update in updates {
            Self::apply_live_row_update(state, update);
        }

        let mut rows = state
            .live_rows
            .iter()
            .filter_map(|(row_key, live_row)| {
                (live_row.weight > 0).then_some((
                    row_key.clone(),
                    live_row.order_key.clone(),
                    live_row.weight,
                ))
            })
            .collect::<Vec<_>>();
        *exact_rows_sorted += rows.len();
        rows.sort_by(|left, right| left.1.cmp(&right.1));
        self.build_output_from_sorted_rows(
            rows.into_iter()
                .map(|(row_key, _order_key, weight)| (row_key, weight)),
        )
    }

    fn apply_live_row_update(
        state: &mut TransientBatchTopNPartitionState,
        update: &TransientBatchTopNPartitionUpdate,
    ) -> i64 {
        let next_weight = match state.live_rows.get(&update.row_key) {
            Some(live_row) => live_row.weight.saturating_add(update.diff),
            None => update.diff,
        };
        if next_weight <= 0 {
            state.live_rows.remove(&update.row_key);
            return 0;
        }
        state.live_rows.insert(
            update.row_key.clone(),
            TransientBatchTopNLiveRow {
                order_key: update.order_key.clone(),
                weight: next_weight,
            },
        );
        next_weight
    }

    fn merge_output_rows(
        &self,
        state: &TransientBatchTopNPartitionState,
        previous_output: &[(Vec<u8>, i64)],
        updated_rows: &[Vec<u8>],
    ) -> Vec<(Vec<u8>, i64)> {
        if self.limit == 0 {
            return Vec::new();
        }

        let mut output = Vec::new();
        let mut previous_idx = 0usize;
        let mut updated_idx = 0usize;
        let mut remaining_take = self.limit;

        while remaining_take > 0
            && (previous_idx < previous_output.len() || updated_idx < updated_rows.len())
        {
            while previous_idx < previous_output.len() {
                let row_key = &previous_output[previous_idx].0;
                match state.live_rows.get(row_key) {
                    Some(live_row) if live_row.weight > 0 => break,
                    _ => previous_idx += 1,
                }
            }
            while updated_idx < updated_rows.len() {
                let row_key = &updated_rows[updated_idx];
                match state.live_rows.get(row_key) {
                    Some(live_row) if live_row.weight > 0 => break,
                    _ => updated_idx += 1,
                }
            }

            let choice = match (
                previous_output.get(previous_idx),
                updated_rows.get(updated_idx),
            ) {
                (Some((previous_row_key, _)), Some(updated_row_key)) => {
                    let previous_key = &state
                        .live_rows
                        .get(previous_row_key)
                        .expect("previous output row must still exist")
                        .order_key;
                    let updated_key = &state
                        .live_rows
                        .get(updated_row_key)
                        .expect("updated row must still exist")
                        .order_key;
                    match previous_key.cmp(updated_key) {
                        std::cmp::Ordering::Less => {
                            let row_key = previous_row_key.clone();
                            previous_idx += 1;
                            Some(row_key)
                        }
                        std::cmp::Ordering::Greater => {
                            let row_key = updated_row_key.clone();
                            updated_idx += 1;
                            Some(row_key)
                        }
                        std::cmp::Ordering::Equal => {
                            let row_key = previous_row_key.clone();
                            previous_idx += 1;
                            updated_idx += 1;
                            Some(row_key)
                        }
                    }
                }
                (Some((previous_row_key, _)), None) => {
                    let row_key = previous_row_key.clone();
                    previous_idx += 1;
                    Some(row_key)
                }
                (None, Some(updated_row_key)) => {
                    let row_key = updated_row_key.clone();
                    updated_idx += 1;
                    Some(row_key)
                }
                (None, None) => None,
            };

            let Some(row_key) = choice else {
                break;
            };
            let Some(live_row) = state.live_rows.get(&row_key) else {
                continue;
            };
            if live_row.weight <= 0 {
                continue;
            }
            let available = usize::try_from(live_row.weight).unwrap_or(usize::MAX);
            let take = remaining_take.min(available);
            if take == 0 {
                continue;
            }
            output.push((row_key, take as i64));
            remaining_take -= take;
        }

        output
    }

    fn build_output_from_sorted_rows(
        &self,
        rows: impl IntoIterator<Item = (Vec<u8>, i64)>,
    ) -> Vec<(Vec<u8>, i64)> {
        if self.limit == 0 {
            return Vec::new();
        }

        let mut remaining_take = self.limit;
        let mut output = Vec::new();
        for (row_key, weight) in rows {
            if remaining_take == 0 {
                break;
            }
            if weight <= 0 {
                continue;
            }
            let available = usize::try_from(weight).unwrap_or(usize::MAX);
            let take = remaining_take.min(available);
            if take == 0 {
                continue;
            }
            output.push((row_key, take as i64));
            remaining_take -= take;
        }
        output
    }

    fn accumulate_output_row_deltas(
        output_deltas: &mut HashMap<Vec<u8>, i64>,
        previous_output: &[(Vec<u8>, i64)],
        next_output: &[(Vec<u8>, i64)],
    ) {
        for (row_key, previous_weight) in previous_output {
            let next_weight = next_output
                .iter()
                .find_map(|(next_row_key, next_weight)| {
                    (next_row_key == row_key).then_some(*next_weight)
                })
                .unwrap_or(0);
            let delta = next_weight.saturating_sub(*previous_weight);
            if delta != 0 {
                let entry = output_deltas.entry(row_key.clone()).or_insert(0);
                *entry = entry.saturating_add(delta);
                if *entry == 0 {
                    output_deltas.remove(row_key);
                }
            }
        }
        for (row_key, next_weight) in next_output {
            if previous_output
                .iter()
                .any(|(previous_row_key, _)| previous_row_key == row_key)
            {
                continue;
            }
            if *next_weight != 0 {
                let entry = output_deltas.entry(row_key.clone()).or_insert(0);
                *entry = entry.saturating_add(*next_weight);
                if *entry == 0 {
                    output_deltas.remove(row_key);
                }
            }
        }
    }
}

impl TransientDirectPartitionTopNProcessor {
    fn new(
        graph_id: impl Into<String>,
        config: TransientDirectPartitionTopNConfig,
        topn: &DbspTopNNode,
        key_layout: &TransientTopNKeyLayout,
    ) -> Self {
        let graph_id = graph_id.into();
        let order_specs = transient_topn_order_specs(topn);
        let key_extractor =
            TransientTopNKeyExtractor::for_layout(graph_id.clone(), key_layout, order_specs)
                .expect("transient topn key layout should be valid");
        Self {
            graph_id,
            partition_idx: config.partition_idx,
            key_extractor,
            limit: topn.limit(),
            offset: topn.offset(),
            order_index: HashMap::new(),
            partition_output_cache: HashMap::new(),
            profile_enabled: tracing::enabled!(tracing::Level::DEBUG),
            profiled_batches: 0,
        }
    }

    fn apply_deltas(&mut self, deltas: Vec<(Vec<u8>, i64)>) -> Result<Vec<(Vec<u8>, i64)>> {
        let input_delta_count = deltas.len();
        let profile_this_batch = self.profile_enabled && self.profiled_batches < 16;
        let total_start = profile_this_batch.then(Instant::now);
        let mut key_eval_us = 0u128;
        let mut mutation_us = 0u128;

        let mut affected_partitions = HashSet::new();
        let key_start = profile_this_batch.then(Instant::now);
        let keyed_deltas = self
            .key_extractor
            .extract_direct_partition_topn(&deltas, self.partition_idx)?;
        if let Some(key_start) = key_start {
            key_eval_us += key_start.elapsed().as_micros();
        }
        for keyed in keyed_deltas {
            let TransientDirectPartitionTopNKeyedDelta {
                diff,
                partition_value,
                order_key,
            } = keyed;
            affected_partitions.insert(partition_value);

            let mutation_start = profile_this_batch.then(Instant::now);
            let partition_index = self.order_index.entry(partition_value).or_default();
            let previous_weight = partition_index.get(&order_key).copied().unwrap_or(0);
            let next_weight = previous_weight.saturating_add(diff);
            if next_weight <= 0 {
                partition_index.remove(&order_key);
                if partition_index.is_empty() {
                    self.order_index.remove(&partition_value);
                }
            } else {
                partition_index.insert(order_key, next_weight);
            }
            if let Some(mutation_start) = mutation_start {
                mutation_us += mutation_start.elapsed().as_micros();
            }
        }

        let recompute_start = profile_this_batch.then(Instant::now);
        let mut recompute_rows_scanned = 0usize;
        let mut output_deltas = HashMap::new();
        let affected_partition_count = affected_partitions.len();
        for partition_key in affected_partitions {
            let previous_output = self
                .partition_output_cache
                .remove(&partition_key)
                .unwrap_or_default();
            let next_output = self
                .order_index
                .get(&partition_key)
                .map(|partition_index| {
                    if profile_this_batch {
                        recompute_rows_scanned += partition_index.len();
                    }
                    self.compute_partition_topn(partition_index)
                })
                .unwrap_or_default();
            accumulate_weight_deltas(&mut output_deltas, &previous_output, &next_output);
            if !next_output.is_empty() {
                self.partition_output_cache
                    .insert(partition_key, next_output);
            }
        }

        let output_deltas = output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect::<Vec<_>>();

        if profile_this_batch {
            self.profiled_batches += 1;
            let recompute_us = recompute_start
                .expect("recompute start present")
                .elapsed()
                .as_micros();
            let total_us = total_start
                .expect("total start present")
                .elapsed()
                .as_micros();
            tracing::info!(
                graph_id = %self.graph_id,
                input_delta_count,
                affected_partition_count,
                retained_partitions = self.partition_output_cache.len(),
                recompute_rows_scanned,
                output_delta_count = output_deltas.len(),
                key_eval_us,
                mutation_us,
                recompute_us,
                total_us,
                "transient direct-partition topn batch profile"
            );
        }

        Ok(output_deltas)
    }

    fn compute_partition_topn(
        &self,
        partition_index: &BTreeMap<TransientTopNKey, i64>,
    ) -> HashMap<Vec<u8>, i64> {
        if self.limit == 0 {
            return HashMap::new();
        }

        let mut remaining_skip = self.offset;
        let mut remaining_take = self.limit;
        let mut output = HashMap::new();
        for (order_key, weight) in partition_index {
            if remaining_take == 0 {
                break;
            }

            let mut remaining_weight = *weight;
            if remaining_skip > 0 {
                let available = usize::try_from(remaining_weight).unwrap_or(usize::MAX);
                let skip = remaining_skip.min(available);
                remaining_skip -= skip;
                remaining_weight -= skip as i64;
            }

            if remaining_weight <= 0 {
                continue;
            }

            let available = usize::try_from(remaining_weight).unwrap_or(usize::MAX);
            let take = remaining_take.min(available);
            if take > 0 {
                output.insert(order_key.tie_breaker.clone(), take as i64);
                remaining_take -= take;
            }
        }
        output
    }
}

impl TransientDirectInt64TopNProcessor {
    fn new(
        graph_id: impl Into<String>,
        config: TransientDirectInt64TopNConfig,
        topn: &DbspTopNNode,
    ) -> Self {
        let graph_id = graph_id.into();
        let order_specs = transient_topn_order_specs(topn);
        let order_type = topn
            .output_schema()
            .field(config.order_idx)
            .expect("direct int64 topn order index should be in bounds")
            .data_type
            .clone();
        let key_extractor = TransientTopNKeyExtractor::new(
            graph_id.clone(),
            Arc::clone(topn.output_schema()),
            Arc::new(vec![config.partition_idx]),
            Arc::new(vec![config.order_idx]),
            Arc::new(vec![order_type]),
            order_specs,
        )
        .expect("direct int64 transient topn key layout should be valid");
        Self {
            graph_id,
            partition_idx: config.partition_idx,
            order_idx: config.order_idx,
            ascending: config.ascending,
            limit: topn.limit(),
            key_extractor,
            partitions: HashMap::new(),
            profile_enabled: tracing::enabled!(tracing::Level::DEBUG),
            profiled_batches: 0,
        }
    }

    fn apply_deltas(&mut self, deltas: Vec<(Vec<u8>, i64)>) -> Result<Vec<(Vec<u8>, i64)>> {
        let input_delta_count = deltas.len();
        let profile_this_batch = self.profile_enabled && self.profiled_batches < 16;
        let total_start = profile_this_batch.then(Instant::now);
        let mut key_eval_us = 0u128;

        let grouping_start = profile_this_batch.then(Instant::now);
        let mut partition_updates =
            HashMap::<i64, Vec<TransientDirectInt64TopNPartitionUpdate>>::new();
        let key_start = profile_this_batch.then(Instant::now);
        let keyed_deltas = self.key_extractor.extract_direct_int64_topn(
            &deltas,
            self.partition_idx,
            self.order_idx,
        )?;
        if let Some(key_start) = key_start {
            key_eval_us += key_start.elapsed().as_micros();
        }
        for keyed in keyed_deltas {
            let TransientDirectInt64TopNKeyedDelta {
                row_key,
                diff,
                partition_value,
                order_value,
            } = keyed;
            partition_updates.entry(partition_value).or_default().push(
                TransientDirectInt64TopNPartitionUpdate {
                    row_key,
                    order_value,
                    diff,
                },
            );
        }
        let grouping_us = grouping_start
            .map(|start| start.elapsed().as_micros())
            .unwrap_or(0);

        let partition_apply_start = profile_this_batch.then(Instant::now);
        let mut output_deltas = HashMap::new();
        let mut affected_partition_count = 0usize;
        let mut candidate_rows_considered = 0usize;
        let mut exact_rows_sorted = 0usize;
        for (partition_value, updates) in partition_updates {
            affected_partition_count += 1;
            let mut state = self.partitions.remove(&partition_value).unwrap_or_default();
            let previous_output = std::mem::take(&mut state.output_rows);
            let next_output = self.apply_partition_updates(
                &mut state,
                &previous_output,
                &updates,
                &mut candidate_rows_considered,
                &mut exact_rows_sorted,
            );
            TransientBatchTopNProcessor::accumulate_output_row_deltas(
                &mut output_deltas,
                &previous_output,
                &next_output,
            );
            state.output_rows = next_output;
            if !state.live_rows.is_empty() {
                self.partitions.insert(partition_value, state);
            }
        }
        let partition_apply_us = partition_apply_start
            .map(|start| start.elapsed().as_micros())
            .unwrap_or(0);

        let output_deltas = output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect::<Vec<_>>();

        if profile_this_batch {
            self.profiled_batches += 1;
            let total_us = total_start
                .expect("total start present")
                .elapsed()
                .as_micros();
            tracing::info!(
                graph_id = %self.graph_id,
                input_delta_count,
                affected_partition_count,
                retained_partitions = self.partitions.len(),
                candidate_rows_considered,
                exact_rows_sorted,
                output_delta_count = output_deltas.len(),
                key_eval_us,
                grouping_us,
                partition_apply_us,
                total_us,
                "transient direct int64 batch topn profile"
            );
        }

        Ok(output_deltas)
    }

    fn apply_partition_updates(
        &self,
        state: &mut TransientDirectInt64TopNPartitionState,
        previous_output: &[(Vec<u8>, i64)],
        updates: &[TransientDirectInt64TopNPartitionUpdate],
        candidate_rows_considered: &mut usize,
        exact_rows_sorted: &mut usize,
    ) -> Vec<(Vec<u8>, i64)> {
        if previous_output.is_empty() && updates.iter().all(|update| update.diff > 0) {
            self.apply_partition_updates_append_only(
                state,
                previous_output,
                updates,
                candidate_rows_considered,
            )
        } else {
            self.apply_partition_updates_exact(state, updates, exact_rows_sorted)
        }
    }

    fn apply_partition_updates_append_only(
        &self,
        state: &mut TransientDirectInt64TopNPartitionState,
        previous_output: &[(Vec<u8>, i64)],
        updates: &[TransientDirectInt64TopNPartitionUpdate],
        candidate_rows_considered: &mut usize,
    ) -> Vec<(Vec<u8>, i64)> {
        let mut updated_rows = Vec::with_capacity(updates.len());
        for update in updates {
            let next_weight = Self::apply_live_row_update(state, update);
            if next_weight > 0 {
                updated_rows.push(update.row_key.clone());
            }
        }

        updated_rows.sort_by(|left, right| self.compare_live_rows(state, left, right));
        updated_rows.dedup();

        *candidate_rows_considered += previous_output.len() + updated_rows.len();
        self.merge_output_rows(state, previous_output, &updated_rows)
    }

    fn apply_partition_updates_exact(
        &self,
        state: &mut TransientDirectInt64TopNPartitionState,
        updates: &[TransientDirectInt64TopNPartitionUpdate],
        exact_rows_sorted: &mut usize,
    ) -> Vec<(Vec<u8>, i64)> {
        for update in updates {
            Self::apply_live_row_update(state, update);
        }

        let mut rows = state
            .live_rows
            .iter()
            .filter_map(|(row_key, live_row)| {
                (live_row.weight > 0).then_some((
                    row_key.clone(),
                    live_row.order_value,
                    live_row.weight,
                ))
            })
            .collect::<Vec<_>>();
        *exact_rows_sorted += rows.len();
        rows.sort_by(|left, right| {
            self.compare_order_and_tie_breaker(left.1, &left.0, right.1, &right.0)
        });
        self.build_output_from_sorted_rows(
            rows.into_iter()
                .map(|(row_key, _order_value, weight)| (row_key, weight)),
        )
    }

    fn apply_live_row_update(
        state: &mut TransientDirectInt64TopNPartitionState,
        update: &TransientDirectInt64TopNPartitionUpdate,
    ) -> i64 {
        let next_weight = match state.live_rows.get(&update.row_key) {
            Some(live_row) => live_row.weight.saturating_add(update.diff),
            None => update.diff,
        };
        if next_weight <= 0 {
            state.live_rows.remove(&update.row_key);
            return 0;
        }
        state.live_rows.insert(
            update.row_key.clone(),
            TransientDirectInt64TopNLiveRow {
                order_value: update.order_value,
                weight: next_weight,
            },
        );
        next_weight
    }

    fn merge_output_rows(
        &self,
        state: &TransientDirectInt64TopNPartitionState,
        previous_output: &[(Vec<u8>, i64)],
        updated_rows: &[Vec<u8>],
    ) -> Vec<(Vec<u8>, i64)> {
        if self.limit == 0 {
            return Vec::new();
        }

        let mut output = Vec::new();
        let mut previous_idx = 0usize;
        let mut updated_idx = 0usize;
        let mut remaining_take = self.limit;

        while remaining_take > 0
            && (previous_idx < previous_output.len() || updated_idx < updated_rows.len())
        {
            while previous_idx < previous_output.len() {
                let row_key = &previous_output[previous_idx].0;
                match state.live_rows.get(row_key) {
                    Some(live_row) if live_row.weight > 0 => break,
                    _ => previous_idx += 1,
                }
            }
            while updated_idx < updated_rows.len() {
                let row_key = &updated_rows[updated_idx];
                match state.live_rows.get(row_key) {
                    Some(live_row) if live_row.weight > 0 => break,
                    _ => updated_idx += 1,
                }
            }

            let choice = match (
                previous_output.get(previous_idx),
                updated_rows.get(updated_idx),
            ) {
                (Some((previous_row_key, _)), Some(updated_row_key)) => {
                    let previous_live_row = state
                        .live_rows
                        .get(previous_row_key)
                        .expect("previous output row must still exist");
                    let updated_live_row = state
                        .live_rows
                        .get(updated_row_key)
                        .expect("updated row must still exist");
                    match self.compare_order_and_tie_breaker(
                        previous_live_row.order_value,
                        previous_row_key,
                        updated_live_row.order_value,
                        updated_row_key,
                    ) {
                        std::cmp::Ordering::Less => {
                            let row_key = previous_row_key.clone();
                            previous_idx += 1;
                            Some(row_key)
                        }
                        std::cmp::Ordering::Greater => {
                            let row_key = updated_row_key.clone();
                            updated_idx += 1;
                            Some(row_key)
                        }
                        std::cmp::Ordering::Equal => {
                            let row_key = previous_row_key.clone();
                            previous_idx += 1;
                            updated_idx += 1;
                            Some(row_key)
                        }
                    }
                }
                (Some((previous_row_key, _)), None) => {
                    let row_key = previous_row_key.clone();
                    previous_idx += 1;
                    Some(row_key)
                }
                (None, Some(updated_row_key)) => {
                    let row_key = updated_row_key.clone();
                    updated_idx += 1;
                    Some(row_key)
                }
                (None, None) => None,
            };

            let Some(row_key) = choice else {
                break;
            };
            let Some(live_row) = state.live_rows.get(&row_key) else {
                continue;
            };
            if live_row.weight <= 0 {
                continue;
            }
            let available = usize::try_from(live_row.weight).unwrap_or(usize::MAX);
            let take = remaining_take.min(available);
            if take == 0 {
                continue;
            }
            output.push((row_key, take as i64));
            remaining_take -= take;
        }

        output
    }

    fn build_output_from_sorted_rows(
        &self,
        rows: impl IntoIterator<Item = (Vec<u8>, i64)>,
    ) -> Vec<(Vec<u8>, i64)> {
        if self.limit == 0 {
            return Vec::new();
        }

        let mut remaining_take = self.limit;
        let mut output = Vec::new();
        for (row_key, weight) in rows {
            if remaining_take == 0 {
                break;
            }
            if weight <= 0 {
                continue;
            }
            let available = usize::try_from(weight).unwrap_or(usize::MAX);
            let take = remaining_take.min(available);
            if take == 0 {
                continue;
            }
            output.push((row_key, take as i64));
            remaining_take -= take;
        }
        output
    }

    fn compare_live_rows(
        &self,
        state: &TransientDirectInt64TopNPartitionState,
        left: &Vec<u8>,
        right: &Vec<u8>,
    ) -> std::cmp::Ordering {
        let left_live_row = state
            .live_rows
            .get(left)
            .expect("live row must exist for left comparison");
        let right_live_row = state
            .live_rows
            .get(right)
            .expect("live row must exist for right comparison");
        self.compare_order_and_tie_breaker(
            left_live_row.order_value,
            left,
            right_live_row.order_value,
            right,
        )
    }

    fn compare_order_and_tie_breaker(
        &self,
        left_order: i64,
        left_row_key: &Vec<u8>,
        right_order: i64,
        right_row_key: &Vec<u8>,
    ) -> std::cmp::Ordering {
        let order_cmp = if self.ascending {
            left_order.cmp(&right_order)
        } else {
            right_order.cmp(&left_order)
        };
        if order_cmp != std::cmp::Ordering::Equal {
            return order_cmp;
        }
        left_row_key.cmp(right_row_key)
    }
}
impl TransientDirectTop1Processor {
    pub(super) fn new(
        graph_id: impl Into<String>,
        topn: &DbspTopNNode,
        config: TransientDirectTop1Config,
        compact_append_only_state: bool,
    ) -> Self {
        let graph_id = graph_id.into();
        let partition_columns = match config.partition_layout {
            TransientDirectTop1PartitionLayout::One(partition_idx) => vec![partition_idx],
            TransientDirectTop1PartitionLayout::Two(partition_indices) => {
                partition_indices.to_vec()
            }
        };
        let order_specs = transient_topn_order_specs(topn);
        let order_type = topn
            .output_schema()
            .field(config.order_idx)
            .expect("direct top1 order index should be in bounds")
            .data_type
            .clone();
        let key_extractor = TransientTopNKeyExtractor::new(
            graph_id.clone(),
            Arc::clone(topn.output_schema()),
            Arc::new(partition_columns),
            Arc::new(vec![config.order_idx]),
            Arc::new(vec![order_type]),
            order_specs,
        )
        .expect("direct top1 transient topn key layout should be valid");
        Self {
            graph_id,
            partition_layout: config.partition_layout,
            order_idx: config.order_idx,
            ascending: config.ascending,
            compact_append_only_state,
            key_extractor,
            partitions: HashMap::new(),
            profile_enabled: tracing::enabled!(tracing::Level::DEBUG),
            profiled_batches: 0,
        }
    }

    pub(super) fn apply_deltas(
        &mut self,
        deltas: Vec<(Vec<u8>, i64)>,
    ) -> Result<Vec<(Vec<u8>, i64)>> {
        let input_delta_count = deltas.len();
        let profile_this_batch = self.profile_enabled && self.profiled_batches < 16;
        let total_start = profile_this_batch.then(Instant::now);
        let mut key_eval_us = 0u128;

        let grouping_start = profile_this_batch.then(Instant::now);
        let mut partition_updates = HashMap::<
            TransientDirectTop1PartitionKey,
            Vec<TransientDirectTop1PartitionUpdate>,
        >::new();
        let key_start = profile_this_batch.then(Instant::now);
        let keyed_deltas = self.key_extractor.extract_direct_top1(
            &deltas,
            self.partition_layout,
            self.order_idx,
        )?;
        if let Some(key_start) = key_start {
            key_eval_us += key_start.elapsed().as_micros();
        }
        for keyed in keyed_deltas {
            let TransientDirectTop1KeyedDelta {
                row_key,
                diff,
                partition_key,
                order_value,
            } = keyed;
            partition_updates.entry(partition_key).or_default().push(
                TransientDirectTop1PartitionUpdate {
                    row_key,
                    order_value,
                    diff,
                },
            );
        }
        let grouping_us = grouping_start
            .map(|start| start.elapsed().as_micros())
            .unwrap_or(0);

        let partition_apply_start = profile_this_batch.then(Instant::now);
        let mut output_deltas = HashMap::new();
        let mut affected_partition_count = 0usize;
        let mut exact_rows_scanned = 0usize;
        for (partition_key, updates) in partition_updates {
            affected_partition_count += 1;
            let mut state = self.partitions.remove(&partition_key).unwrap_or_default();
            let previous_top = state.top_row.clone();
            let next_top = if updates.iter().all(|update| update.diff > 0) {
                self.apply_partition_updates_append_only(&mut state, &updates)
            } else {
                self.apply_partition_updates_exact(&mut state, &updates, &mut exact_rows_scanned)
            };

            if previous_top != next_top {
                if let Some(previous_top) = previous_top {
                    let entry = output_deltas.entry(previous_top).or_insert(0);
                    *entry -= 1;
                }
                if let Some(next_top_row) = next_top.clone() {
                    let entry = output_deltas.entry(next_top_row).or_insert(0);
                    *entry += 1;
                }
            }

            state.top_row = next_top;
            if !state.live_rows.is_empty() {
                self.partitions.insert(partition_key, state);
            }
        }
        let partition_apply_us = partition_apply_start
            .map(|start| start.elapsed().as_micros())
            .unwrap_or(0);

        let output_deltas = output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect::<Vec<_>>();

        if profile_this_batch {
            self.profiled_batches += 1;
            let total_us = total_start
                .expect("total start present")
                .elapsed()
                .as_micros();
            tracing::info!(
                graph_id = %self.graph_id,
                input_delta_count,
                affected_partition_count,
                retained_partitions = self.partitions.len(),
                exact_rows_scanned,
                output_delta_count = output_deltas.len(),
                key_eval_us,
                grouping_us,
                partition_apply_us,
                total_us,
                "transient direct top1 profile"
            );
        }

        Ok(output_deltas)
    }

    #[cfg(test)]
    pub(super) fn snapshot_deltas(&self) -> Vec<(Vec<u8>, i64)> {
        self.partitions
            .values()
            .filter_map(|state| {
                let row_key = state.top_row.as_ref()?;
                let weight = state.live_rows.get(row_key)?.weight;
                (weight > 0).then_some((row_key.clone(), weight))
            })
            .collect()
    }

    fn apply_partition_updates_append_only(
        &self,
        state: &mut TransientDirectTop1PartitionState,
        updates: &[TransientDirectTop1PartitionUpdate],
    ) -> Option<Vec<u8>> {
        let mut next_top = state.top_row.clone();
        for update in updates {
            let next_weight = Self::apply_live_row_update(state, update);
            if next_weight <= 0 {
                continue;
            }
            let previous_top = next_top.clone();
            match next_top.as_ref() {
                Some(current_top) => {
                    if self.compare_live_rows(state, &update.row_key, current_top)
                        == std::cmp::Ordering::Less
                    {
                        next_top = Some(update.row_key.clone());
                    }
                }
                None => {
                    next_top = Some(update.row_key.clone());
                }
            }
            if next_top.as_ref() == Some(&update.row_key)
                && previous_top.as_ref() != Some(&update.row_key)
                && self.compact_append_only_state
            {
                let retained = state
                    .live_rows
                    .get(&update.row_key)
                    .cloned()
                    .expect("winning append-only top1 row must be live");
                state.live_rows.clear();
                state.live_rows.insert(update.row_key.clone(), retained);
            } else if previous_top.as_ref() != Some(&update.row_key)
                && self.compact_append_only_state
            {
                state.live_rows.remove(&update.row_key);
            }
        }
        next_top
    }

    fn apply_partition_updates_exact(
        &self,
        state: &mut TransientDirectTop1PartitionState,
        updates: &[TransientDirectTop1PartitionUpdate],
        exact_rows_scanned: &mut usize,
    ) -> Option<Vec<u8>> {
        for update in updates {
            Self::apply_live_row_update(state, update);
        }

        *exact_rows_scanned += state.live_rows.len();
        let mut best_row_key: Option<&Vec<u8>> = None;
        let mut best_order_value = 0i64;
        for (row_key, live_row) in &state.live_rows {
            if live_row.weight <= 0 {
                continue;
            }
            match best_row_key {
                Some(current_best) => {
                    if self.compare_order_and_tie_breaker(
                        live_row.order_value,
                        row_key,
                        best_order_value,
                        current_best,
                    ) == std::cmp::Ordering::Less
                    {
                        best_row_key = Some(row_key);
                        best_order_value = live_row.order_value;
                    }
                }
                None => {
                    best_row_key = Some(row_key);
                    best_order_value = live_row.order_value;
                }
            }
        }
        best_row_key.cloned()
    }

    fn apply_live_row_update(
        state: &mut TransientDirectTop1PartitionState,
        update: &TransientDirectTop1PartitionUpdate,
    ) -> i64 {
        let next_weight = match state.live_rows.get(&update.row_key) {
            Some(live_row) => live_row.weight.saturating_add(update.diff),
            None => update.diff,
        };
        if next_weight <= 0 {
            state.live_rows.remove(&update.row_key);
            return 0;
        }
        state.live_rows.insert(
            update.row_key.clone(),
            TransientDirectTop1LiveRow {
                order_value: update.order_value,
                weight: next_weight,
            },
        );
        next_weight
    }

    fn compare_live_rows(
        &self,
        state: &TransientDirectTop1PartitionState,
        left: &Vec<u8>,
        right: &Vec<u8>,
    ) -> std::cmp::Ordering {
        let left_live_row = state
            .live_rows
            .get(left)
            .expect("live row must exist for left comparison");
        let right_live_row = state
            .live_rows
            .get(right)
            .expect("live row must exist for right comparison");
        self.compare_order_and_tie_breaker(
            left_live_row.order_value,
            left,
            right_live_row.order_value,
            right,
        )
    }

    fn compare_order_and_tie_breaker(
        &self,
        left_order: i64,
        left_row_key: &Vec<u8>,
        right_order: i64,
        right_row_key: &Vec<u8>,
    ) -> std::cmp::Ordering {
        let order_cmp = if self.ascending {
            left_order.cmp(&right_order)
        } else {
            right_order.cmp(&left_order)
        };
        if order_cmp != std::cmp::Ordering::Equal {
            return order_cmp;
        }
        left_row_key.cmp(right_row_key)
    }
}

fn build_transient_topn_key_layout(topn: &DbspTopNNode) -> Result<TransientTopNKeyLayout> {
    let input_schema = Arc::clone(topn.output_schema());
    let direct_partition_columns = topn
        .partition_by()
        .iter()
        .map(|expr| projection_direct_column_index_expression(expr.expr(), input_schema.as_ref()))
        .collect::<Vec<_>>();
    let direct_order_columns = topn
        .order_by()
        .iter()
        .map(|expr| {
            projection_direct_column_index_expression(
                expr.expression().expr(),
                input_schema.as_ref(),
            )
        })
        .collect::<Vec<_>>();

    if direct_partition_columns.iter().all(Option::is_some)
        && direct_order_columns.iter().all(Option::is_some)
    {
        return Ok(TransientTopNKeyLayout {
            input_schema: Arc::clone(&input_schema),
            partition_columns: Arc::new(
                direct_partition_columns
                    .into_iter()
                    .collect::<Option<Vec<_>>>()
                    .expect("all direct transient topn partition columns should be present"),
            ),
            order_columns: Arc::new(
                direct_order_columns
                    .iter()
                    .copied()
                    .collect::<Option<Vec<_>>>()
                    .expect("all direct transient topn order columns should be present"),
            ),
            order_types: Arc::new(
                direct_order_columns
                    .iter()
                    .copied()
                    .collect::<Option<Vec<_>>>()
                    .expect("all direct transient topn order columns should be present")
                    .into_iter()
                    .map(|column_idx| {
                        input_schema
                            .field(column_idx)
                            .map(|field| field.data_type.clone())
                            .expect("transient topn order key column index should be in bounds")
                    })
                    .collect(),
            ),
            precompute_evaluator: None,
        });
    }

    let mut items =
        Vec::with_capacity(input_schema.len() + topn.partition_by().len() + topn.order_by().len());
    for field in input_schema.fields() {
        items.push(dbsp::circuit::plan::ProjectItem {
            expr: Expr::Column(Column::new_unqualified(field.name.clone())),
            alias: Some(field.name.clone()),
        });
    }

    let mut expression_columns = HashMap::new();
    let mut seen = HashSet::new();
    let mut next_index = input_schema.len();
    let mut partition_columns = Vec::with_capacity(topn.partition_by().len());
    for (index, expr) in topn.partition_by().iter().enumerate() {
        if let Some(column_idx) = direct_partition_columns[index] {
            partition_columns.push(column_idx);
            continue;
        }
        let key = transient_topn_expression_lookup_key(expr.expr());
        if seen.insert(key.clone()) {
            let alias = format!("__floe_transient_topn_partition_expr_{index}");
            items.push(dbsp::circuit::plan::ProjectItem {
                expr: expr.expr().clone(),
                alias: Some(alias),
            });
            expression_columns.insert(key.clone(), next_index);
            next_index += 1;
        }
        partition_columns.push(
            *expression_columns
                .get(&key)
                .expect("transient topn partition expression column should be registered"),
        );
    }

    let mut order_columns = Vec::with_capacity(topn.order_by().len());
    for (index, expr) in topn.order_by().iter().enumerate() {
        if let Some(column_idx) = direct_order_columns[index] {
            order_columns.push(column_idx);
            continue;
        }
        let key = transient_topn_expression_lookup_key(expr.expression().expr());
        if seen.insert(key.clone()) {
            let alias = format!("__floe_transient_topn_order_expr_{index}");
            items.push(dbsp::circuit::plan::ProjectItem {
                expr: expr.expression().expr().clone(),
                alias: Some(alias),
            });
            expression_columns.insert(key.clone(), next_index);
            next_index += 1;
        }
        order_columns.push(
            *expression_columns
                .get(&key)
                .expect("transient topn order expression column should be registered"),
        );
    }

    let project_node = DbspProjectNode::try_new(Arc::clone(&input_schema), items)
        .context("build transient topn expression precompute projection")?;
    let evaluator = VectorizedFilterProjectEvaluator::for_map(
        project_node.expressions(),
        Arc::clone(&input_schema),
    )
    .context("initialize transient topn precompute evaluator")?;
    let projected_schema = project_node.output_schema();
    let order_types = order_columns
        .iter()
        .map(|column_idx| {
            projected_schema
                .field(*column_idx)
                .map(|field| field.data_type.clone())
                .ok_or_else(|| {
                    anyhow!("transient topn order key column index {column_idx} out of bounds")
                })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(TransientTopNKeyLayout {
        input_schema: Arc::clone(projected_schema),
        partition_columns: Arc::new(partition_columns),
        order_columns: Arc::new(order_columns),
        order_types: Arc::new(order_types),
        precompute_evaluator: Some(Arc::new(evaluator)),
    })
}

fn transient_topn_expression_lookup_key(expr: &Expr) -> String {
    match expr {
        Expr::Alias(alias) => transient_topn_expression_lookup_key(alias.expr.as_ref()),
        other => other.to_string(),
    }
}

fn accumulate_weight_deltas(
    output_deltas: &mut HashMap<Vec<u8>, i64>,
    previous_output: &HashMap<Vec<u8>, i64>,
    next_output: &HashMap<Vec<u8>, i64>,
) {
    for (row_key, previous_weight) in previous_output {
        let next_weight = next_output.get(row_key).copied().unwrap_or(0);
        let delta = next_weight.saturating_sub(*previous_weight);
        if delta != 0 {
            let entry = output_deltas.entry(row_key.clone()).or_insert(0);
            *entry = entry.saturating_add(delta);
            if *entry == 0 {
                output_deltas.remove(row_key);
            }
        }
    }
    for (row_key, next_weight) in next_output {
        if previous_output.contains_key(row_key) {
            continue;
        }
        if *next_weight != 0 {
            let entry = output_deltas.entry(row_key.clone()).or_insert(0);
            *entry = entry.saturating_add(*next_weight);
            if *entry == 0 {
                output_deltas.remove(row_key);
            }
        }
    }
}

fn accumulate_single_weight_delta(
    output_deltas: &mut HashMap<Vec<u8>, i64>,
    row_key: Vec<u8>,
    diff: i64,
) {
    if diff == 0 {
        return;
    }
    let entry = output_deltas.entry(row_key.clone()).or_insert(0);
    *entry = entry.saturating_add(diff);
    if *entry == 0 {
        output_deltas.remove(&row_key);
    }
}

fn try_build_direct_partitioned_top1_config(
    topn: &DbspTopNNode,
) -> Option<TransientDirectTop1Config> {
    if topn.offset() != 0 || topn.limit() != 1 {
        return None;
    }
    if topn.partition_by().is_empty() || topn.partition_by().len() > 2 || topn.order_by().len() != 1
    {
        return None;
    }

    let schema = topn.output_schema();
    let partition_indices = topn
        .partition_by()
        .iter()
        .map(|expr| projection_direct_column_index_expression(expr.expr(), schema.as_ref()))
        .collect::<Option<Vec<_>>>()?;

    for partition_idx in &partition_indices {
        let partition_field = schema.field(*partition_idx)?;
        if partition_field.data_type != dbsp::circuit::types::DbspScalarType::Int64
            || partition_field.nullable
        {
            return None;
        }
    }

    let order_idx = projection_direct_column_index_expression(
        topn.order_by()[0].expression().expr(),
        schema.as_ref(),
    )?;
    let order_field = schema.field(order_idx)?;
    if !matches!(
        order_field.data_type,
        dbsp::circuit::types::DbspScalarType::Int64
            | dbsp::circuit::types::DbspScalarType::TimestampMillis
    ) || order_field.nullable
    {
        return None;
    }

    let partition_layout = match partition_indices.as_slice() {
        [partition_idx] => TransientDirectTop1PartitionLayout::One(*partition_idx),
        [first_partition_idx, second_partition_idx] => {
            TransientDirectTop1PartitionLayout::Two([*first_partition_idx, *second_partition_idx])
        }
        _ => return None,
    };

    Some(TransientDirectTop1Config {
        partition_layout,
        order_idx,
        ascending: topn.order_by()[0].ascending(),
    })
}

fn try_build_direct_partition_topn_config(
    topn: &DbspTopNNode,
) -> Option<TransientDirectPartitionTopNConfig> {
    if topn.offset() != 0 || topn.limit() == 0 || topn.partition_by().len() != 1 {
        return None;
    }

    let schema = topn.output_schema();
    let partition_idx =
        projection_direct_column_index_expression(topn.partition_by()[0].expr(), schema.as_ref())?;
    let partition_field = schema.field(partition_idx)?;
    if partition_field.data_type != dbsp::circuit::types::DbspScalarType::Int64
        || partition_field.nullable
    {
        return None;
    }

    Some(TransientDirectPartitionTopNConfig { partition_idx })
}

#[allow(dead_code)]
fn try_build_direct_int64_partitioned_topn_config(
    topn: &DbspTopNNode,
) -> Option<TransientDirectInt64TopNConfig> {
    if topn.offset() != 0 || topn.limit() == 0 || topn.limit() > 64 {
        return None;
    }
    if topn.partition_by().len() != 1 || topn.order_by().len() != 1 {
        return None;
    }

    let schema = topn.output_schema();
    let partition_idx =
        projection_direct_column_index_expression(topn.partition_by()[0].expr(), schema.as_ref())?;
    let order_idx = projection_direct_column_index_expression(
        topn.order_by()[0].expression().expr(),
        schema.as_ref(),
    )?;

    let partition_field = schema.field(partition_idx)?;
    let order_field = schema.field(order_idx)?;
    if partition_field.data_type != dbsp::circuit::types::DbspScalarType::Int64
        || partition_field.nullable
    {
        return None;
    }
    if order_field.data_type != dbsp::circuit::types::DbspScalarType::Int64 || order_field.nullable
    {
        return None;
    }

    Some(TransientDirectInt64TopNConfig {
        partition_idx,
        order_idx,
        ascending: topn.order_by()[0].ascending(),
    })
}

pub(super) fn build_direct_projection_transform(
    columns: Arc<Vec<usize>>,
    input_schema: Arc<RowSchema>,
) -> Arc<DeltaTransformFn> {
    let input_arrow_schema = input_schema.to_arrow_schema();
    Arc::new(move |deltas| {
        let columns = Arc::clone(&columns);
        let input_arrow_schema = Arc::clone(&input_arrow_schema);
        Box::pin(async move {
            project_encoded_deltas(deltas.as_ref(), columns.as_ref(), input_arrow_schema)
        })
    })
}

pub(super) fn fold_topn_root_output_projection(shape: &mut TransientSourceTopNRootShape) {
    if let Some(output_projection) = shape.output_projection.take() {
        shape.transform = compose_optional_delta_transform(
            shape.transform.take(),
            build_direct_projection_transform(
                output_projection,
                Arc::clone(shape.topn.output_schema()),
            ),
        );
    }
}

fn project_encoded_deltas(
    deltas: &[(Vec<u8>, i64)],
    columns: &[usize],
    input_schema: SchemaRef,
) -> Result<Vec<(Vec<u8>, i64)>> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }
    let projected_schema = projected_arrow_schema(&input_schema, columns)?;
    let mut buffer = DeltaBatchBuffer::new_projected(
        projected_schema,
        Arc::<[usize]>::from(columns.to_vec()),
        false,
        DeltaBatchConfig {
            max_rows: usize::MAX,
            max_bytes: usize::MAX,
        },
    )
    .context("create transient topn projected output batch")?;
    let mut staged_weights = Vec::with_capacity(deltas.len());
    for (encoded, weight) in deltas {
        if *weight == 0 {
            continue;
        }
        if buffer.push(encoded.clone(), *weight, None)?.is_some() {
            bail!("unbounded transient topn projection flushed before manual flush");
        }
        staged_weights.push(*weight);
    }
    let Some(batch) = buffer.flush_manual()? else {
        return Ok(Vec::new());
    };
    let projected_positions = (0..columns.len()).collect::<Vec<_>>();
    let mut output = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let weight = staged_weights
            .get(row_idx)
            .copied()
            .ok_or_else(|| anyhow!("transient topn projection row index out of bounds"))?;
        let projected = encode_arrow_columns(&batch, &projected_positions, row_idx)?;
        output.push((projected, weight));
    }
    Ok(output)
}

pub(super) fn build_transient_topn_receiver(
    graph_id: &str,
    topn: &DbspTopNNode,
    upstream: TransientSourceHandleStream,
    input_transform: Arc<DeltaTransformFn>,
    output_projection: Option<Arc<Vec<usize>>>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> mpsc::UnboundedReceiver<TransientMaterializeBatch> {
    // Source roots are ZSet inputs, not a proven append-only contract. Keeping
    // full TopN input state is required to recompute replacement winners after
    // retractions; winner-only compact state is only correct for strictly
    // append-only streams.
    let append_only_input = false;
    let compact_append_only_state = false;
    let upstream_rx = build_transient_source_receiver(
        graph_id,
        format!("transient-topn-source:{graph_id}"),
        upstream,
        input_transform,
        cancel,
        task_events,
    );
    build_transient_topn_receiver_from_batches(
        graph_id,
        topn,
        upstream_rx,
        append_only_input,
        compact_append_only_state,
        output_projection,
        cancel,
        task_events,
        state_table,
        state_label,
    )
}

pub(super) fn build_transient_topn_receiver_from_batches(
    graph_id: &str,
    topn: &DbspTopNNode,
    mut upstream_rx: mpsc::UnboundedReceiver<TransientMaterializeBatch>,
    append_only_input: bool,
    compact_append_only_state: bool,
    output_projection: Option<Arc<Vec<usize>>>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> mpsc::UnboundedReceiver<TransientMaterializeBatch> {
    let (tx, rx) = mpsc::unbounded_channel::<TransientMaterializeBatch>();
    let graph_id = graph_id.to_string();
    let task_label = format!("transient-topn:{graph_id}");
    let task_events = task_events.clone();
    let cancel = cancel.clone();
    let state_label = state_label.into();
    let debug_transient_join = tracing::enabled!(tracing::Level::DEBUG);
    let topn_output_schema = topn.output_schema().to_arrow_schema();
    if let Some(config) = try_build_direct_partitioned_top1_config(topn) {
        let mut processor = TransientDirectTop1Processor::new(
            graph_id.clone(),
            topn,
            config,
            compact_append_only_state,
        );
        let output_projection = output_projection.clone();
        let output_projection_schema = Arc::clone(&topn_output_schema);
        let state_table = state_table.clone();
        let state_label = state_label.clone();
        tokio::spawn(async move {
            let mut persistent_state =
                match PersistentTransientInputState::load(state_table, &graph_id, &state_label)
                    .await
                {
                    Ok(state) => state,
                    Err(err) => {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        return;
                    }
                };
            if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                return;
            }
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        if !compact_append_only_state {
                            if let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let output_deltas = match processor.apply_deltas(input_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if compact_append_only_state {
                            if let Err(err) = persistent_state.apply_deltas(&output_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let output_deltas = match output_projection.as_ref() {
                            Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref(), Arc::clone(&output_projection_schema)) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            },
                            None => output_deltas,
                        };
                        if debug_transient_join {
                            eprintln!(
                                "transient-topn-output graph_id={} version={} rows={}",
                                graph_id,
                                batch.version,
                                output_deltas.len()
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(output_deltas),
                            deltas_consolidated: false,
                        }).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        return rx;
    }

    let use_direct_int64_partitioned_topn = false;
    if use_direct_int64_partitioned_topn {
        if let Some(config) = try_build_direct_int64_partitioned_topn_config(topn) {
            let mut processor =
                TransientDirectInt64TopNProcessor::new(graph_id.clone(), config, topn);
            let output_projection = output_projection.clone();
            let output_projection_schema = Arc::clone(&topn_output_schema);
            let state_table = state_table.clone();
            let state_label = state_label.clone();
            tokio::spawn(async move {
                let mut persistent_state =
                    match PersistentTransientInputState::load(state_table, &graph_id, &state_label)
                        .await
                    {
                        Ok(state) => state,
                        Err(err) => {
                            report_graph_task_error(
                                &task_events,
                                &graph_id,
                                task_label.clone(),
                                err,
                            );
                            return;
                        }
                    };
                if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                    return;
                }
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        maybe_batch = upstream_rx.recv() => {
                            let Some(batch) = maybe_batch else {
                                break;
                            };
                            let input_deltas = batch.deltas.as_ref().clone();
                            if let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                            let output_deltas = match processor.apply_deltas(input_deltas) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            };
                            let output_deltas = match output_projection.as_ref() {
                                Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref(), Arc::clone(&output_projection_schema)) {
                                    Ok(deltas) => deltas,
                                    Err(err) => {
                                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                        break;
                                    }
                                },
                                None => output_deltas,
                            };
                            if debug_transient_join {
                                eprintln!(
                                    "transient-topn-output graph_id={} version={} rows={}",
                                    graph_id,
                                    batch.version,
                                    output_deltas.len()
                                );
                            }
                            if tx.send(TransientMaterializeBatch {
                                version: batch.version,
                                deltas: Arc::new(output_deltas),
                                deltas_consolidated: false,
                            }).is_err() {
                                break;
                            }
                        }
                    }
                }
            });
            return rx;
        }
    }

    let use_partitioned_top1 =
        topn.limit() == 1 && topn.offset() == 0 && !topn.partition_by().is_empty();
    let key_layout = match build_transient_topn_key_layout(topn) {
        Ok(layout) => layout,
        Err(err) => {
            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
            return rx;
        }
    };

    if use_partitioned_top1 {
        let mut processor = TransientTop1Processor::new(graph_id.clone(), topn, &key_layout);
        let precompute_evaluator = key_layout.precompute_evaluator.clone();
        let output_projection = output_projection.clone();
        let output_projection_schema = Arc::clone(&topn_output_schema);
        let state_table = state_table.clone();
        let state_label = state_label.clone();
        tokio::spawn(async move {
            let mut persistent_state =
                match PersistentTransientInputState::load(state_table, &graph_id, &state_label)
                    .await
                {
                    Ok(state) => state,
                    Err(err) => {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        return;
                    }
                };
            if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                return;
            }
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                            match evaluator
                                .transform_delta_arrow(&graph_id, Arc::new(input_deltas))
                                .await
                            {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            }
                        } else {
                            input_deltas
                        };
                        if !compact_append_only_state
                            && let Err(err) = persistent_state.apply_deltas(&input_deltas).await
                        {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                        let output_deltas = match processor.apply_deltas(input_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if compact_append_only_state
                            && let Err(err) = persistent_state.apply_deltas(&output_deltas).await
                        {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                        let output_deltas = match output_projection.as_ref() {
                            Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref(), Arc::clone(&output_projection_schema)) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            },
                            None => output_deltas,
                        };
                        if debug_transient_join {
                            eprintln!(
                                "transient-topn-output graph_id={} version={} rows={}",
                                graph_id,
                                batch.version,
                                output_deltas.len()
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(output_deltas),
                            deltas_consolidated: false,
                        }).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        return rx;
    }

    if let Some(config) = try_build_direct_partition_topn_config(topn) {
        let mut processor =
            TransientDirectPartitionTopNProcessor::new(graph_id.clone(), config, topn, &key_layout);
        let precompute_evaluator = key_layout.precompute_evaluator.clone();
        let output_projection = output_projection.clone();
        let output_projection_schema = Arc::clone(&topn_output_schema);
        let state_table = state_table.clone();
        let state_label = state_label.clone();
        tokio::spawn(async move {
            let mut persistent_state =
                match PersistentTransientInputState::load(state_table, &graph_id, &state_label)
                    .await
                {
                    Ok(state) => state,
                    Err(err) => {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        return;
                    }
                };
            if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                return;
            }
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                            match evaluator
                                .transform_delta_arrow(&graph_id, Arc::new(input_deltas))
                                .await
                            {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            }
                        } else {
                            input_deltas
                        };
                        if !compact_append_only_state {
                            if let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let output_deltas = match processor.apply_deltas(input_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if compact_append_only_state {
                            if let Err(err) = persistent_state.apply_deltas(&output_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let output_deltas = match output_projection.as_ref() {
                            Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref(), Arc::clone(&output_projection_schema)) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            },
                            None => output_deltas,
                        };
                        if debug_transient_join {
                            eprintln!(
                                "transient-topn-output graph_id={} version={} rows={}",
                                graph_id,
                                batch.version,
                                output_deltas.len()
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(output_deltas),
                            deltas_consolidated: false,
                        }).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        return rx;
    }

    let use_append_only_partitioned_topn = append_only_input
        && topn.offset() == 0
        && topn.limit() > 1
        && !topn.partition_by().is_empty();

    if use_append_only_partitioned_topn {
        let mut processor =
            TransientAppendOnlyTopNProcessor::new(graph_id.clone(), topn, &key_layout);
        let precompute_evaluator = key_layout.precompute_evaluator.clone();
        let output_projection = output_projection.clone();
        let output_projection_schema = Arc::clone(&topn_output_schema);
        let state_table = state_table.clone();
        let state_label = state_label.clone();
        tokio::spawn(async move {
            let mut persistent_state =
                match PersistentTransientInputState::load(state_table, &graph_id, &state_label)
                    .await
                {
                    Ok(state) => state,
                    Err(err) => {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        return;
                    }
                };
            if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                return;
            }
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                            match evaluator
                                .transform_delta_arrow(&graph_id, Arc::new(input_deltas))
                                .await
                            {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            }
                        } else {
                            input_deltas
                        };
                        if !compact_append_only_state {
                            if let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let output_deltas = match processor.apply_deltas(input_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if compact_append_only_state {
                            if let Err(err) = persistent_state.apply_deltas(&output_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let output_deltas = match output_projection.as_ref() {
                            Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref(), Arc::clone(&output_projection_schema)) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            },
                            None => output_deltas,
                        };
                        if debug_transient_join {
                            eprintln!(
                                "transient-topn-output graph_id={} version={} rows={}",
                                graph_id,
                                batch.version,
                                output_deltas.len()
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(output_deltas),
                            deltas_consolidated: false,
                        }).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        return rx;
    }

    let use_vectorized_partitioned_topn = false
        && topn.offset() == 0
        && topn.limit() > 1
        && topn.limit() <= 64
        && !topn.partition_by().is_empty();

    if use_vectorized_partitioned_topn {
        let mut processor = TransientBatchTopNProcessor::new(
            graph_id.clone(),
            topn,
            &key_layout,
            append_only_input,
        );
        let precompute_evaluator = key_layout.precompute_evaluator.clone();
        let output_projection = output_projection.clone();
        let output_projection_schema = Arc::clone(&topn_output_schema);
        let state_table = state_table.clone();
        let state_label = state_label.clone();
        tokio::spawn(async move {
            let mut persistent_state =
                match PersistentTransientInputState::load(state_table, &graph_id, &state_label)
                    .await
                {
                    Ok(state) => state,
                    Err(err) => {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        return;
                    }
                };
            if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                return;
            }
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                            match evaluator
                                .transform_delta_arrow(&graph_id, Arc::new(input_deltas))
                                .await
                            {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            }
                        } else {
                            input_deltas
                        };
                        if let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                        let output_deltas = match processor.apply_deltas(input_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        let output_deltas = match output_projection.as_ref() {
                            Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref(), Arc::clone(&output_projection_schema)) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            },
                            None => output_deltas,
                        };
                        if debug_transient_join {
                            eprintln!(
                                "transient-topn-output graph_id={} version={} rows={}",
                                graph_id,
                                batch.version,
                                output_deltas.len()
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(output_deltas),
                            deltas_consolidated: false,
                        }).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        return rx;
    }

    let mut processor =
        TransientTopNProcessor::new(graph_id.clone(), topn, &key_layout, append_only_input);
    let precompute_evaluator = key_layout.precompute_evaluator.clone();
    let output_projection = output_projection.clone();
    let output_projection_schema = Arc::clone(&topn_output_schema);
    let state_table = state_table.clone();
    let state_label = state_label.clone();

    tokio::spawn(async move {
        let mut persistent_state =
            match PersistentTransientInputState::load(state_table, &graph_id, &state_label).await {
                Ok(state) => state,
                Err(err) => {
                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                    return;
                }
            };
        if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
            return;
        }
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                maybe_batch = upstream_rx.recv() => {
                    let Some(batch) = maybe_batch else {
                        break;
                    };
                    let input_deltas = batch.deltas.as_ref().clone();
                    let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                        match evaluator
                            .transform_delta_arrow(&graph_id, Arc::new(input_deltas))
                            .await
                        {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                    } else {
                        input_deltas
                    };
                    if !compact_append_only_state
                        && let Err(err) = persistent_state.apply_deltas(&input_deltas).await
                    {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        break;
                    }
                    let output_deltas = match processor.apply_deltas(input_deltas) {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if compact_append_only_state
                        && let Err(err) = persistent_state.apply_deltas(&output_deltas).await
                    {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        break;
                    }
                    let output_deltas = match output_projection.as_ref() {
                        Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref(), Arc::clone(&output_projection_schema)) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        },
                        None => output_deltas,
                    };
                    if debug_transient_join {
                        eprintln!(
                            "transient-topn-output graph_id={} version={} rows={}",
                            graph_id,
                            batch.version,
                            output_deltas.len()
                        );
                    }
                    if tx.send(TransientMaterializeBatch {
                        version: batch.version,
                        deltas: Arc::new(output_deltas),
                        deltas_consolidated: false,
                    }).is_err() {
                        break;
                    }
                }
            }
        }
    });

    rx
}
