use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array,
    Int64Array, StringArray, TimestampMillisecondArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::{Result as DFResult, internal_err};
use datafusion::error::DataFusionError;

use dbsp::{KEY_COLUMN_NAME, WEIGHT_COLUMN_NAME};

use crate::scalar_array_builder::ScalarColumnBuilder;

const CONSOLIDATED_BATCH_ROW_LIMIT: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsolidationMode {
    ByAllColumns,
    ByKey,
}

#[derive(Debug, Clone)]
pub struct DeltaConsolidator {
    schema: SchemaRef,
    mode: ConsolidationMode,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConsolidationStats {
    pub input_rows: usize,
    pub grouped_rows: usize,
    pub output_rows: usize,
    pub zero_weight_dropped_rows: usize,
}

#[derive(Debug, Clone)]
pub struct ConsolidationOutput {
    pub batches: Vec<RecordBatch>,
    pub stats: ConsolidationStats,
}

impl DeltaConsolidator {
    pub fn new(schema: SchemaRef) -> DFResult<Self> {
        Self::with_mode(schema, ConsolidationMode::ByAllColumns)
    }

    pub fn with_mode(schema: SchemaRef, mode: ConsolidationMode) -> DFResult<Self> {
        validate_schema(&schema, mode)?;
        Ok(Self { schema, mode })
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub async fn consolidate(&self, batches: Vec<RecordBatch>) -> DFResult<Vec<RecordBatch>> {
        Ok(self.consolidate_with_stats(batches).await?.batches)
    }

    pub async fn consolidate_with_stats(
        &self,
        batches: Vec<RecordBatch>,
    ) -> DFResult<ConsolidationOutput> {
        if batches.is_empty() {
            return Ok(ConsolidationOutput {
                batches: vec![RecordBatch::new_empty(Arc::clone(&self.schema))],
                stats: ConsolidationStats::default(),
            });
        }

        for batch in &batches {
            if batch.schema().as_ref() != self.schema.as_ref() {
                return internal_err!("delta batch schema does not match consolidator schema");
            }
        }
        if self.mode == ConsolidationMode::ByKey {
            validate_key_payload_consistency(&batches, &self.schema)?;
        }
        let input_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();

        let output = consolidate_rows(&batches, &self.schema, self.mode)?;
        let grouped_rows = output.grouped_rows;
        let output_rows = output.output_rows;
        let mut batches = output.batches;
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        Ok(ConsolidationOutput {
            batches,
            stats: ConsolidationStats {
                input_rows,
                grouped_rows,
                output_rows,
                zero_weight_dropped_rows: grouped_rows.saturating_sub(output_rows),
            },
        })
    }
}

pub fn weighted_snapshot_schema(base_schema: &SchemaRef) -> DFResult<SchemaRef> {
    if base_schema.index_of(WEIGHT_COLUMN_NAME).is_ok() {
        return internal_err!(
            "snapshot schema already contains {} column",
            WEIGHT_COLUMN_NAME
        );
    }

    let mut fields = base_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new(WEIGHT_COLUMN_NAME, DataType::Int64, false));
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        base_schema.metadata().clone(),
    )))
}

pub fn add_weight_column(
    batch: &RecordBatch,
    weighted_schema: &SchemaRef,
    weight: i64,
) -> DFResult<RecordBatch> {
    let base_field_count = batch.schema().fields().len();
    if weighted_schema.fields().len() != base_field_count + 1 {
        return internal_err!("weighted schema does not match input snapshot schema");
    }
    for (batch_field, weighted_field) in batch
        .schema()
        .fields()
        .iter()
        .zip(weighted_schema.fields().iter())
    {
        if batch_field.as_ref() != weighted_field.as_ref() {
            return internal_err!("weighted schema does not match input snapshot schema");
        }
    }
    let weight_field = weighted_schema.field(base_field_count);
    if weight_field.name() != WEIGHT_COLUMN_NAME || weight_field.data_type() != &DataType::Int64 {
        return internal_err!(
            "weighted schema must end with Int64 {} column",
            WEIGHT_COLUMN_NAME
        );
    }

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int64Array::from_value(weight, batch.num_rows())) as ArrayRef);
    Ok(RecordBatch::try_new(Arc::clone(weighted_schema), columns)?)
}

pub fn add_weight_column_to_batches(
    batches: &[RecordBatch],
    weighted_schema: &SchemaRef,
    weight: i64,
) -> DFResult<Vec<RecordBatch>> {
    batches
        .iter()
        .map(|batch| add_weight_column(batch, weighted_schema, weight))
        .collect()
}

pub async fn diff_bounded_output_batches(
    base_schema: SchemaRef,
    previous: &[RecordBatch],
    next: &[RecordBatch],
) -> DFResult<ConsolidationOutput> {
    for batch in previous.iter().chain(next.iter()) {
        if batch.schema().as_ref() != base_schema.as_ref() {
            return internal_err!("bounded output batch schema does not match delta schema");
        }
    }

    let weighted_schema = weighted_snapshot_schema(&base_schema)?;
    let mut weighted_batches = add_weight_column_to_batches(previous, &weighted_schema, -1)?;
    weighted_batches.extend(add_weight_column_to_batches(next, &weighted_schema, 1)?);
    DeltaConsolidator::new(weighted_schema)?
        .consolidate_with_stats(weighted_batches)
        .await
}

pub fn diff_bounded_output_batches_by_row(
    base_schema: SchemaRef,
    previous: &[RecordBatch],
    next: &[RecordBatch],
) -> DFResult<ConsolidationOutput> {
    for batch in previous.iter().chain(next.iter()) {
        if batch.schema().as_ref() != base_schema.as_ref() {
            return internal_err!("bounded output batch schema does not match delta schema");
        }
    }

    let weighted_schema = weighted_snapshot_schema(&base_schema)?;
    let converter = row_converter_for_schema(&base_schema)?;
    let mut groups: HashMap<Vec<u8>, DiffRowGroup> = HashMap::new();
    accumulate_diff_rows(&converter, previous, DiffSide::Previous, -1, &mut groups)?;
    accumulate_diff_rows(&converter, next, DiffSide::Next, 1, &mut groups)?;

    let grouped_rows = groups.len();
    let rows = groups
        .into_values()
        .filter_map(|group| {
            if group.weight == 0 {
                return None;
            }
            let side = if group.weight > 0 {
                DiffSide::Next
            } else {
                DiffSide::Previous
            };
            let source = match side {
                DiffSide::Previous => group.previous,
                DiffSide::Next => group.next,
            }?;
            Some(DiffOutputRow {
                source,
                side,
                weight: group.weight,
            })
        })
        .collect::<Vec<_>>();
    let output_rows = rows.len();
    let batches = build_diff_batches(&weighted_schema, previous, next, &rows)?;

    Ok(ConsolidationOutput {
        batches,
        stats: ConsolidationStats {
            input_rows: previous
                .iter()
                .chain(next.iter())
                .map(RecordBatch::num_rows)
                .sum(),
            grouped_rows,
            output_rows,
            zero_weight_dropped_rows: grouped_rows.saturating_sub(output_rows),
        },
    })
}

#[derive(Clone, Copy)]
enum DiffSide {
    Previous,
    Next,
}

#[derive(Clone, Copy)]
struct DiffRowGroup {
    previous: Option<SourceRowRef>,
    next: Option<SourceRowRef>,
    weight: i64,
}

#[derive(Clone, Copy)]
struct DiffOutputRow {
    source: SourceRowRef,
    side: DiffSide,
    weight: i64,
}

fn row_converter_for_schema(schema: &SchemaRef) -> DFResult<RowConverter> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| SortField::new(field.data_type().clone()))
        .collect::<Vec<_>>();
    RowConverter::new(fields).map_err(|err| DataFusionError::Execution(err.to_string()))
}

fn accumulate_diff_rows(
    converter: &RowConverter,
    batches: &[RecordBatch],
    side: DiffSide,
    weight: i64,
    groups: &mut HashMap<Vec<u8>, DiffRowGroup>,
) -> DFResult<()> {
    for (batch_idx, batch) in batches.iter().enumerate() {
        if batch.num_rows() == 0 {
            continue;
        }
        let rows = converter.convert_columns(batch.columns())?;
        for row_idx in 0..batch.num_rows() {
            let entry = groups
                .entry(rows.row(row_idx).data().to_vec())
                .or_insert(DiffRowGroup {
                    previous: None,
                    next: None,
                    weight: 0,
                });
            let source = SourceRowRef { batch_idx, row_idx };
            match side {
                DiffSide::Previous => entry.previous = Some(source),
                DiffSide::Next => entry.next = Some(source),
            }
            entry.weight = entry.weight.saturating_add(weight);
        }
    }
    Ok(())
}

fn build_diff_batches(
    schema: &SchemaRef,
    previous: &[RecordBatch],
    next: &[RecordBatch],
    rows: &[DiffOutputRow],
) -> DFResult<Vec<RecordBatch>> {
    let mut output = Vec::new();
    let mut builders = new_output_builders(schema)?;
    let weight_idx = schema
        .fields()
        .len()
        .checked_sub(1)
        .ok_or_else(|| DataFusionError::Internal("weighted diff schema is empty".into()))?;
    let mut buffered_rows = 0usize;

    for row in rows {
        let source_batches = match row.side {
            DiffSide::Previous => previous,
            DiffSide::Next => next,
        };
        let source_batch = source_batches.get(row.source.batch_idx).ok_or_else(|| {
            DataFusionError::Internal("diff source batch index out of bounds".into())
        })?;
        for (column_idx, builder) in builders.iter_mut().enumerate() {
            if column_idx == weight_idx {
                builder
                    .append_i64_value(row.weight)
                    .map_err(to_execution_error)?;
            } else {
                builder
                    .append_array_value(
                        source_batch.column(column_idx).as_ref(),
                        row.source.row_idx,
                    )
                    .map_err(to_execution_error)?;
            }
        }
        buffered_rows += 1;
        if buffered_rows == CONSOLIDATED_BATCH_ROW_LIMIT {
            output.push(finish_consolidated_batch(schema, &mut builders)?);
            buffered_rows = 0;
        }
    }

    if buffered_rows > 0 {
        output.push(finish_consolidated_batch(schema, &mut builders)?);
    }
    if output.is_empty() {
        output.push(RecordBatch::new_empty(Arc::clone(schema)));
    }
    Ok(output)
}

fn validate_key_payload_consistency(batches: &[RecordBatch], schema: &SchemaRef) -> DFResult<()> {
    let key_idx = match schema.index_of(KEY_COLUMN_NAME) {
        Ok(idx) => idx,
        Err(_) => return internal_err!("missing {} column", KEY_COLUMN_NAME),
    };
    let payload_indices = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            (field.name() != KEY_COLUMN_NAME && field.name() != WEIGHT_COLUMN_NAME).then_some(idx)
        })
        .collect::<Vec<_>>();

    let payload_count = u32::try_from(payload_indices.len()).map_err(|_| {
        datafusion::error::DataFusionError::Internal("payload too wide".to_string())
    })?;
    let mut payload_by_key: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let Some(keys) = batch.column(key_idx).as_any().downcast_ref::<BinaryArray>() else {
            return internal_err!("{} column must be Binary", KEY_COLUMN_NAME);
        };
        let payload_columns = payload_indices
            .iter()
            .map(|idx| batch.column(*idx).clone())
            .collect::<Vec<_>>();
        for row in 0..batch.num_rows() {
            let key = keys.value(row).to_vec();
            let payload = encode_payload_row(payload_columns.as_slice(), row, payload_count)?;
            match payload_by_key.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(payload);
                }
                Entry::Occupied(existing) => {
                    if existing.get() != &payload {
                        return internal_err!(
                            "conflicting payloads found for {} while consolidating by key",
                            KEY_COLUMN_NAME
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

fn encode_payload_row(columns: &[ArrayRef], row: usize, payload_count: u32) -> DFResult<Vec<u8>> {
    let mut payload = Vec::with_capacity(4 + (columns.len() * 9));
    payload.extend_from_slice(&payload_count.to_le_bytes());
    for column in columns {
        encode_payload_cell(column, row, &mut payload)?;
    }
    Ok(payload)
}

fn encode_payload_cell(column: &ArrayRef, row: usize, payload: &mut Vec<u8>) -> DFResult<()> {
    if row >= column.len() {
        return internal_err!("payload row index out of bounds while consolidating by key");
    }
    if column.is_null(row) {
        match column.data_type() {
            DataType::Int64 => payload.push(0x05),
            DataType::Utf8 => payload.push(0x06),
            DataType::Timestamp(TimeUnit::Millisecond, _) => payload.push(0x07),
            DataType::Boolean => payload.push(0x08),
            DataType::Date32 => payload.push(0x0A),
            DataType::Decimal128(_, _) => payload.push(0x0C),
            DataType::Float64 => payload.push(0x0E),
            DataType::UInt64 => payload.push(0x0F),
            DataType::Null => payload.push(0x00),
            other => {
                return internal_err!(
                    "unsupported payload type for by-key consolidation: {other:?}"
                );
            }
        }
        return Ok(());
    }

    match column.data_type() {
        DataType::Int64 => {
            let Some(values) = column.as_any().downcast_ref::<Int64Array>() else {
                return internal_err!("expected Int64 payload array");
            };
            payload.push(0x01);
            payload.extend_from_slice(&values.value(row).to_le_bytes());
        }
        DataType::Utf8 => {
            let Some(values) = column.as_any().downcast_ref::<StringArray>() else {
                return internal_err!("expected Utf8 payload array");
            };
            payload.push(0x02);
            let bytes = values.value(row).as_bytes();
            let len = u32::try_from(bytes.len()).map_err(|_| {
                datafusion::error::DataFusionError::Internal(
                    "utf8 payload value too large".to_string(),
                )
            })?;
            payload.extend_from_slice(&len.to_le_bytes());
            payload.extend_from_slice(bytes);
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let Some(values) = column.as_any().downcast_ref::<TimestampMillisecondArray>() else {
                return internal_err!("expected timestamp(ms) payload array");
            };
            payload.push(0x03);
            payload.extend_from_slice(&values.value(row).to_le_bytes());
        }
        DataType::Boolean => {
            let Some(values) = column.as_any().downcast_ref::<BooleanArray>() else {
                return internal_err!("expected boolean payload array");
            };
            payload.push(0x04);
            payload.push(if values.value(row) { 1 } else { 0 });
        }
        DataType::Date32 => {
            let Some(values) = column.as_any().downcast_ref::<Date32Array>() else {
                return internal_err!("expected Date32 payload array");
            };
            payload.push(0x09);
            payload.extend_from_slice(&values.value(row).to_le_bytes());
        }
        DataType::Decimal128(_, _) => {
            let Some(values) = column.as_any().downcast_ref::<Decimal128Array>() else {
                return internal_err!("expected Decimal128 payload array");
            };
            payload.push(0x0B);
            payload.extend_from_slice(&values.value(row).to_le_bytes());
        }
        DataType::Float64 => {
            let Some(values) = column.as_any().downcast_ref::<Float64Array>() else {
                return internal_err!("expected Float64 payload array");
            };
            payload.push(0x0D);
            payload.extend_from_slice(&values.value(row).to_bits().to_le_bytes());
        }
        DataType::UInt64 => {
            let Some(values) = column.as_any().downcast_ref::<UInt64Array>() else {
                return internal_err!("expected UInt64 payload array");
            };
            payload.push(0x10);
            payload.extend_from_slice(&values.value(row).to_le_bytes());
        }
        DataType::Null => {
            payload.push(0x00);
        }
        other => {
            return internal_err!("unsupported payload type for by-key consolidation: {other:?}");
        }
    }
    Ok(())
}

fn validate_schema(schema: &SchemaRef, mode: ConsolidationMode) -> DFResult<()> {
    let weight_idx = match schema.index_of(WEIGHT_COLUMN_NAME) {
        Ok(idx) => idx,
        Err(_) => return internal_err!("missing {} column", WEIGHT_COLUMN_NAME),
    };
    let weight_field = schema.field(weight_idx);
    if weight_field.data_type() != &DataType::Int64 {
        return internal_err!("{} column must be Int64", WEIGHT_COLUMN_NAME);
    }

    if mode == ConsolidationMode::ByKey && schema.index_of(KEY_COLUMN_NAME).is_err() {
        return internal_err!("missing {} column", KEY_COLUMN_NAME);
    }

    Ok(())
}

#[derive(Clone, Copy)]
struct SourceRowRef {
    batch_idx: usize,
    row_idx: usize,
}

#[derive(Clone, Copy)]
struct GroupedRow {
    source: SourceRowRef,
    weight: i64,
}

struct ConsolidatedRows {
    batches: Vec<RecordBatch>,
    grouped_rows: usize,
    output_rows: usize,
}

fn consolidate_rows(
    batches: &[RecordBatch],
    schema: &SchemaRef,
    mode: ConsolidationMode,
) -> DFResult<ConsolidatedRows> {
    validate_schema(schema, mode)?;
    let weight_idx = schema.index_of(WEIGHT_COLUMN_NAME)?;
    let grouping_indices = grouping_indices(schema, mode)?;
    let grouping_count = u32::try_from(grouping_indices.len())
        .map_err(|_| DataFusionError::Internal("delta grouping key too wide".to_string()))?;
    let key_idx = if mode == ConsolidationMode::ByKey {
        Some(schema.index_of(KEY_COLUMN_NAME)?)
    } else {
        None
    };

    let mut groups: HashMap<Vec<u8>, GroupedRow> = HashMap::new();
    for (batch_idx, batch) in batches.iter().enumerate() {
        let Some(weights) = batch
            .column(weight_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
        else {
            return internal_err!("{} column must be Int64", WEIGHT_COLUMN_NAME);
        };
        let grouping_columns = grouping_indices
            .iter()
            .map(|idx| Arc::clone(batch.column(*idx)))
            .collect::<Vec<_>>();
        let key_values = match key_idx {
            Some(idx) => Some(
                batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "{KEY_COLUMN_NAME} column must be Binary"
                        ))
                    })?,
            ),
            None => None,
        };

        for row_idx in 0..batch.num_rows() {
            if weights.is_null(row_idx) {
                return internal_err!("{} column cannot contain NULL", WEIGHT_COLUMN_NAME);
            }
            let group_key = if let Some(keys) = key_values {
                if keys.is_null(row_idx) {
                    return internal_err!("{} column cannot contain NULL", KEY_COLUMN_NAME);
                }
                keys.value(row_idx).to_vec()
            } else {
                encode_payload_row(&grouping_columns, row_idx, grouping_count)?
            };
            let source = SourceRowRef { batch_idx, row_idx };
            groups
                .entry(group_key)
                .and_modify(|group| {
                    group.weight = group.weight.saturating_add(weights.value(row_idx));
                })
                .or_insert(GroupedRow {
                    source,
                    weight: weights.value(row_idx),
                });
        }
    }

    let grouped_rows = groups.len();
    let mut rows = groups
        .into_iter()
        .filter_map(|(key, row)| (row.weight != 0).then_some((key, row)))
        .collect::<Vec<_>>();
    rows.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    let output_rows = rows.len();
    let rows = rows.into_iter().map(|(_, row)| row).collect::<Vec<_>>();
    let batches = build_consolidated_batches(schema, weight_idx, batches, &rows)?;
    Ok(ConsolidatedRows {
        batches,
        grouped_rows,
        output_rows,
    })
}

fn grouping_indices(schema: &SchemaRef, mode: ConsolidationMode) -> DFResult<Vec<usize>> {
    Ok(match mode {
        ConsolidationMode::ByAllColumns => schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| (field.name() != WEIGHT_COLUMN_NAME).then_some(idx))
            .collect(),
        ConsolidationMode::ByKey => vec![schema.index_of(KEY_COLUMN_NAME)?],
    })
}

fn build_consolidated_batches(
    schema: &SchemaRef,
    weight_idx: usize,
    source_batches: &[RecordBatch],
    rows: &[GroupedRow],
) -> DFResult<Vec<RecordBatch>> {
    let mut output = Vec::new();
    let mut builders = new_output_builders(schema)?;
    let mut buffered_rows = 0usize;

    for row in rows {
        let source_batch = source_batches
            .get(row.source.batch_idx)
            .ok_or_else(|| DataFusionError::Internal("source batch index out of bounds".into()))?;
        for (column_idx, builder) in builders.iter_mut().enumerate() {
            if column_idx == weight_idx {
                builder
                    .append_i64_value(row.weight)
                    .map_err(to_execution_error)?;
            } else {
                builder
                    .append_array_value(
                        source_batch.column(column_idx).as_ref(),
                        row.source.row_idx,
                    )
                    .map_err(to_execution_error)?;
            }
        }
        buffered_rows += 1;
        if buffered_rows == CONSOLIDATED_BATCH_ROW_LIMIT {
            output.push(finish_consolidated_batch(schema, &mut builders)?);
            buffered_rows = 0;
        }
    }

    if buffered_rows > 0 {
        output.push(finish_consolidated_batch(schema, &mut builders)?);
    }
    Ok(output)
}

fn new_output_builders(schema: &SchemaRef) -> DFResult<Vec<ScalarColumnBuilder>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            ScalarColumnBuilder::new(field.data_type(), CONSOLIDATED_BATCH_ROW_LIMIT)
                .map_err(to_execution_error)
        })
        .collect()
}

fn finish_consolidated_batch(
    schema: &SchemaRef,
    builders: &mut [ScalarColumnBuilder],
) -> DFResult<RecordBatch> {
    let arrays = builders
        .iter_mut()
        .map(ScalarColumnBuilder::finish_array)
        .collect::<Vec<_>>();
    Ok(RecordBatch::try_new(Arc::clone(schema), arrays)?)
}

fn to_execution_error(err: anyhow::Error) -> DataFusionError {
    DataFusionError::Execution(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BinaryArray, Int64Array, StringArray, UInt64Array};

    fn int_weight_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN_NAME, DataType::Int64, false),
        ]))
    }

    #[tokio::test]
    async fn consolidates_by_all_columns_without_datafusion_roundtrip() {
        let schema = int_weight_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(Int64Array::from(vec![1, -1, 3])),
            ],
        )
        .expect("delta batch");

        let output = DeltaConsolidator::new(Arc::clone(&schema))
            .expect("consolidator")
            .consolidate_with_stats(vec![batch])
            .await
            .expect("consolidate");

        assert_eq!(output.stats.input_rows, 3);
        assert_eq!(output.stats.grouped_rows, 2);
        assert_eq!(output.stats.output_rows, 1);
        assert_eq!(output.stats.zero_weight_dropped_rows, 1);
        let ids = output.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let weights = output.batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("weight column");
        assert_eq!(ids.values(), &[2]);
        assert_eq!(weights.values(), &[3]);
    }

    #[tokio::test]
    async fn consolidates_uint64_columns_without_datafusion_roundtrip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("rank", DataType::UInt64, false),
            Field::new(WEIGHT_COLUMN_NAME, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from(vec![1, 1, 2])),
                Arc::new(Int64Array::from(vec![1, -1, 3])),
            ],
        )
        .expect("delta batch");

        let output = DeltaConsolidator::new(Arc::clone(&schema))
            .expect("consolidator")
            .consolidate_with_stats(vec![batch])
            .await
            .expect("consolidate");

        assert_eq!(output.stats.input_rows, 3);
        assert_eq!(output.stats.grouped_rows, 2);
        assert_eq!(output.stats.output_rows, 1);
        let ranks = output.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("rank column");
        let weights = output.batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("weight column");
        assert_eq!(ranks.values(), &[2]);
        assert_eq!(weights.values(), &[3]);
    }

    #[tokio::test]
    async fn consolidates_by_key_preserving_payload() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Utf8, false),
            Field::new(KEY_COLUMN_NAME, DataType::Binary, false),
            Field::new(WEIGHT_COLUMN_NAME, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["row", "row", "other"])),
                Arc::new(BinaryArray::from_vec(vec![b"k1", b"k1", b"k2"])),
                Arc::new(Int64Array::from(vec![1, 2, -1])),
            ],
        )
        .expect("keyed delta batch");

        let output = DeltaConsolidator::with_mode(Arc::clone(&schema), ConsolidationMode::ByKey)
            .expect("consolidator")
            .consolidate_with_stats(vec![batch])
            .await
            .expect("consolidate by key");

        assert_eq!(output.stats.output_rows, 2);
        let payloads = output.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload column");
        let weights = output.batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("weight column");
        assert_eq!(payloads.value(0), "row");
        assert_eq!(weights.value(0), 3);
        assert_eq!(payloads.value(1), "other");
        assert_eq!(weights.value(1), -1);
    }
}
