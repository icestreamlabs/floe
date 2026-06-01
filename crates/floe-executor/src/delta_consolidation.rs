use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int64Array,
    StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result as DFResult, internal_err};
use datafusion::datasource::MemTable;
use datafusion::functions_aggregate::expr_fn::{min, sum};
use datafusion::prelude::{Expr, SessionContext, col, lit};

use dbsp::circuit::{KEY_COLUMN_NAME, WEIGHT_COLUMN_NAME};

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

        let ctx = SessionContext::new();
        let table = MemTable::try_new(Arc::clone(&self.schema), vec![batches])?;
        ctx.register_table("delta", Arc::new(table))?;

        let df = ctx.table("delta").await?;
        let (group_exprs, aggr_exprs, select_exprs) = build_exprs(&self.schema, self.mode)?;
        let df = df.aggregate(group_exprs, aggr_exprs)?;
        let df = df.select(select_exprs)?;
        let grouped = df.collect().await?;
        let grouped_rows = grouped.iter().map(RecordBatch::num_rows).sum::<usize>();

        let mut batches = filter_zero_weight_rows(grouped, Arc::clone(&self.schema)).await?;
        let output_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
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

pub async fn diff_snapshot_batches(
    base_schema: SchemaRef,
    previous: &[RecordBatch],
    next: &[RecordBatch],
) -> DFResult<ConsolidationOutput> {
    for batch in previous.iter().chain(next.iter()) {
        if batch.schema().as_ref() != base_schema.as_ref() {
            return internal_err!("snapshot batch schema does not match diff schema");
        }
    }

    let weighted_schema = weighted_snapshot_schema(&base_schema)?;
    let mut weighted_batches = add_weight_column_to_batches(previous, &weighted_schema, -1)?;
    weighted_batches.extend(add_weight_column_to_batches(next, &weighted_schema, 1)?);
    DeltaConsolidator::new(weighted_schema)?
        .consolidate_with_stats(weighted_batches)
        .await
}

async fn filter_zero_weight_rows(
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
) -> DFResult<Vec<RecordBatch>> {
    validate_schema(&schema, ConsolidationMode::ByAllColumns)?;
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let ctx = SessionContext::new();
    let table_schema = batches
        .first()
        .map(RecordBatch::schema)
        .unwrap_or_else(|| Arc::clone(&schema));
    let table = MemTable::try_new(table_schema, vec![batches])?;
    ctx.register_table("grouped_delta", Arc::new(table))?;

    let filtered = ctx
        .table("grouped_delta")
        .await?
        .filter(col(WEIGHT_COLUMN_NAME).not_eq(lit(0_i64)))?
        .collect()
        .await?;
    filtered
        .into_iter()
        .map(|batch| normalize_batch_schema(batch, &schema))
        .collect()
}

fn normalize_batch_schema(
    batch: RecordBatch,
    expected_schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    if batch.schema().as_ref() == expected_schema.as_ref() {
        return Ok(batch);
    }
    if batch.num_columns() != expected_schema.fields().len() {
        return internal_err!("consolidated batch schema column count changed");
    }
    let batch_schema = batch.schema();
    for (idx, expected) in expected_schema.fields().iter().enumerate() {
        let actual = batch_schema.field(idx);
        if actual.name() != expected.name() || actual.data_type() != expected.data_type() {
            return internal_err!("consolidated batch schema does not match declared delta schema");
        }
    }
    Ok(RecordBatch::try_new(
        Arc::clone(expected_schema),
        batch.columns().to_vec(),
    )?)
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

fn build_exprs(
    schema: &SchemaRef,
    mode: ConsolidationMode,
) -> DFResult<(Vec<Expr>, Vec<Expr>, Vec<Expr>)> {
    validate_schema(schema, mode)?;

    let mut group_exprs = Vec::new();
    let mut aggr_exprs = Vec::new();

    match mode {
        ConsolidationMode::ByAllColumns => {
            for field in schema.fields() {
                if field.name() == WEIGHT_COLUMN_NAME {
                    continue;
                }
                group_exprs.push(col(field.name()));
            }
            aggr_exprs.push(sum(col(WEIGHT_COLUMN_NAME)).alias(WEIGHT_COLUMN_NAME));
        }
        ConsolidationMode::ByKey => {
            group_exprs.push(col(KEY_COLUMN_NAME));
            for field in schema.fields() {
                let name = field.name();
                if name == KEY_COLUMN_NAME || name == WEIGHT_COLUMN_NAME {
                    continue;
                }
                aggr_exprs.push(min(col(name)).alias(name));
            }
            aggr_exprs.push(sum(col(WEIGHT_COLUMN_NAME)).alias(WEIGHT_COLUMN_NAME));
        }
    }

    let select_exprs = schema
        .fields()
        .iter()
        .map(|field| col(field.name()))
        .collect::<Vec<_>>();

    Ok((group_exprs, aggr_exprs, select_exprs))
}
