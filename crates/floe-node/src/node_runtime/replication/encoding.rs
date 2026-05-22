use std::collections::HashMap;
use std::io::Write as _;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, Int64Builder, StringBuilder,
    TimestampMillisecondBuilder,
};
use arrow_array::{ArrayRef, Decimal128Array, RecordBatch};
use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow_ipc::{CompressionType, MetadataVersion};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use floe_cdc_core::{
    CdcChange, CdcColumn, CdcColumnarColumn, CdcColumnarRowBatch, CdcRow, CdcRowKey, CdcSourceId,
    CdcSourcePosition, CdcTableId, CdcTableSchema, CdcTransactionId, ChangeBatch, TransactionBatch,
};
use floe_config::{ReplicationArrowIpcCompressionConfig, ReplicationEncodingConfig};
use floe_core::RowValue;
use floe_core::catalog::ColumnType;
use floe_node_core::debezium_encoder::{
    DebeziumBatchEncodeOptions, DebeziumEncodeContext, DebeziumEncodedRecord,
    DebeziumEnvelopeConfig, encode_debezium_change_batch_with_options,
};
use floe_storage::CdcBufferRecord;
use rayon::prelude::*;

use super::super::{ReplicationPipelineRuntimeFormat, ReplicationPipelineRuntimePlan};
use super::{
    FLOE_HEADER_IDEMPOTENCY_KEY, FLOE_HEADER_PIPELINE, FLOE_HEADER_RECORD_SEQUENCE,
    FLOE_HEADER_SOURCE, FLOE_HEADER_SOURCE_POSITION, FLOE_HEADER_SOURCE_TABLE,
    FLOE_HEADER_TRANSACTION_ID, FLOE_JSON_DELETED_FIELD, FLOE_JSON_PARALLEL_RECORD_THRESHOLD,
    FLOE_JSON_VERSION, FLOE_JSON_VERSION_FIELD,
};

#[cfg(test)]
pub(super) fn encode_pipeline_transaction_records(
    plan: &ReplicationPipelineRuntimePlan,
    schemas: &HashMap<CdcTableId, CdcTableSchema>,
    transaction: &TransactionBatch,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    encode_pipeline_transaction_records_with_settings(
        plan,
        schemas,
        transaction,
        ReplicationEncodingConfig::default(),
    )
}

pub(super) fn encode_pipeline_transaction_records_with_settings(
    plan: &ReplicationPipelineRuntimePlan,
    schemas: &HashMap<CdcTableId, CdcTableSchema>,
    transaction: &TransactionBatch,
    settings: ReplicationEncodingConfig,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    encode_pipeline_transaction_records_with_metadata_and_settings(
        plan,
        schemas,
        transaction,
        settings,
        settings.kafka_metadata_headers,
    )
}

#[cfg(test)]
pub(super) fn encode_pipeline_transaction_records_with_metadata(
    plan: &ReplicationPipelineRuntimePlan,
    schemas: &HashMap<CdcTableId, CdcTableSchema>,
    transaction: &TransactionBatch,
    include_metadata_headers: bool,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    encode_pipeline_transaction_records_with_metadata_and_settings(
        plan,
        schemas,
        transaction,
        ReplicationEncodingConfig::default(),
        include_metadata_headers,
    )
}

pub(super) fn encode_pipeline_transaction_records_with_metadata_and_settings(
    plan: &ReplicationPipelineRuntimePlan,
    schemas: &HashMap<CdcTableId, CdcTableSchema>,
    transaction: &TransactionBatch,
    settings: ReplicationEncodingConfig,
    include_metadata_headers: bool,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    let mut matching_batches = transaction
        .change_batches()
        .iter()
        .filter(|batch| batch.table_id() == &plan.table_id)
        .peekable();
    if matching_batches.peek().is_none() {
        return Ok(Vec::new());
    }

    let schema = schemas.get(&plan.table_id).ok_or_else(|| {
        anyhow!(
            "replication pipeline '{}' references missing CDC schema '{}'",
            plan.name,
            plan.table_id.as_str()
        )
    })?;
    let mut records = Vec::new();
    let mut next_sequence = 0usize;
    for change_batch in matching_batches {
        let mut batch_records = encode_pipeline_buffer_records_with_settings(
            plan,
            schema,
            change_batch,
            transaction,
            settings,
        )?;
        if include_metadata_headers {
            add_replication_record_metadata(
                plan,
                transaction.commit_position(),
                transaction.transaction_id(),
                &mut batch_records,
                next_sequence,
            );
        }
        next_sequence = next_sequence.saturating_add(batch_records.len());
        records.extend(batch_records);
    }
    Ok(records)
}

pub(super) fn add_replication_record_metadata(
    plan: &ReplicationPipelineRuntimePlan,
    source_position: &CdcSourcePosition,
    transaction_id: Option<&CdcTransactionId>,
    records: &mut [CdcBufferRecord],
    start_sequence: usize,
) {
    let source_position = source_position_key(source_position);
    let transaction_id = transaction_id.map(|id| id.as_str().to_string());
    for (idx, record) in records.iter_mut().enumerate() {
        let sequence = start_sequence.saturating_add(idx);
        let idempotency_key = replication_record_idempotency_key(
            plan,
            &source_position,
            transaction_id.as_deref(),
            sequence,
        );
        let mut enriched = std::mem::replace(record, CdcBufferRecord::new(None, None));
        enriched = enriched
            .with_header(FLOE_HEADER_IDEMPOTENCY_KEY, idempotency_key.into_bytes())
            .with_header(FLOE_HEADER_PIPELINE, plan.name.as_bytes().to_vec())
            .with_header(FLOE_HEADER_SOURCE, plan.source_name.as_bytes().to_vec())
            .with_header(
                FLOE_HEADER_SOURCE_TABLE,
                plan.upstream_table.as_bytes().to_vec(),
            )
            .with_header(
                FLOE_HEADER_SOURCE_POSITION,
                source_position.as_bytes().to_vec(),
            )
            .with_header(
                FLOE_HEADER_RECORD_SEQUENCE,
                sequence.to_string().into_bytes(),
            );
        if let Some(transaction_id) = transaction_id.as_deref() {
            enriched = enriched.with_header(
                FLOE_HEADER_TRANSACTION_ID,
                transaction_id.as_bytes().to_vec(),
            );
        }
        *record = enriched;
    }
}

fn replication_record_idempotency_key(
    plan: &ReplicationPipelineRuntimePlan,
    source_position: &str,
    transaction_id: Option<&str>,
    sequence: usize,
) -> String {
    match transaction_id {
        Some(transaction_id) => format!(
            "{}/{}/{}/{sequence}",
            plan.name, plan.upstream_table, transaction_id
        ),
        None => format!(
            "{}/{}/{source_position}/{sequence}",
            plan.name, plan.upstream_table
        ),
    }
}

pub(super) fn chunk_snapshot_transaction_with_settings(
    source_id: &CdcSourceId,
    transaction: &TransactionBatch,
    settings: ReplicationEncodingConfig,
) -> anyhow::Result<Option<Vec<TransactionBatch>>> {
    let Some(transaction_id) = transaction.transaction_id() else {
        return Ok(None);
    };
    let batches_per_chunk = settings.snapshot_batches_per_chunk.max(1);
    if !transaction_id.as_str().starts_with("snapshot:")
        || transaction.change_batches().len() <= batches_per_chunk
    {
        return Ok(None);
    }
    if !transaction
        .change_batches()
        .iter()
        .all(|batch| batch.snapshot_insert_rows().is_some())
    {
        return Ok(None);
    }

    let chunk_count = transaction
        .change_batches()
        .len()
        .div_ceil(batches_per_chunk);
    let mut chunks = Vec::with_capacity(chunk_count);
    for (idx, batch_chunk) in transaction
        .change_batches()
        .chunks(batches_per_chunk)
        .enumerate()
    {
        chunks.push(TransactionBatch::new(
            source_id.clone(),
            Some(CdcTransactionId::new(format!(
                "{}:chunk:{idx:06}",
                transaction_id.as_str()
            ))?),
            transaction.start_position().cloned(),
            transaction.commit_position().clone(),
            batch_chunk.to_vec(),
        )?);
    }
    Ok(Some(chunks))
}

#[cfg(test)]
pub(super) fn encode_pipeline_buffer_records(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    batch: &ChangeBatch,
    transaction: &TransactionBatch,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    encode_pipeline_buffer_records_with_settings(
        plan,
        schema,
        batch,
        transaction,
        ReplicationEncodingConfig::default(),
    )
}

pub(super) fn encode_pipeline_buffer_records_with_settings(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    batch: &ChangeBatch,
    transaction: &TransactionBatch,
    settings: ReplicationEncodingConfig,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    if let Some(rows) = batch.snapshot_insert_rows() {
        return match plan.format {
            ReplicationPipelineRuntimeFormat::FloeJson => {
                encode_floe_json_snapshot_pipeline_records(plan, schema, rows)
            }
            ReplicationPipelineRuntimeFormat::DebeziumJson => {
                let records = encode_debezium_pipeline_records(plan, schema, batch, transaction)?;
                debezium_records_to_buffer_records(&records)
            }
            ReplicationPipelineRuntimeFormat::ArrowIpc => {
                encode_arrow_ipc_snapshot_pipeline_records(
                    plan,
                    schema,
                    rows,
                    transaction,
                    settings,
                )
            }
        };
    }

    match plan.format {
        ReplicationPipelineRuntimeFormat::FloeJson => {
            encode_floe_json_pipeline_records(plan, schema, batch)
        }
        ReplicationPipelineRuntimeFormat::DebeziumJson => {
            let records = encode_debezium_pipeline_records(plan, schema, batch, transaction)?;
            debezium_records_to_buffer_records(&records)
        }
        ReplicationPipelineRuntimeFormat::ArrowIpc => {
            encode_arrow_ipc_pipeline_records(plan, schema, batch, transaction, settings)
        }
    }
}

fn encode_floe_json_pipeline_records(
    _plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    batch: &ChangeBatch,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    validate_floe_json_schema(schema)?;
    let encoder = FloeJsonRowEncoder::new(schema)?;
    if batch.changes().len() >= FLOE_JSON_PARALLEL_RECORD_THRESHOLD {
        return batch
            .changes()
            .par_iter()
            .map(|change| encode_floe_json_change_record(change, &encoder, schema))
            .collect::<Vec<_>>()
            .into_iter()
            .collect();
    }
    batch
        .changes()
        .iter()
        .map(|change| encode_floe_json_change_record(change, &encoder, schema))
        .collect()
}

fn encode_floe_json_change_record(
    change: &CdcChange,
    encoder: &FloeJsonRowEncoder,
    schema: &CdcTableSchema,
) -> anyhow::Result<CdcBufferRecord> {
    match change {
        CdcChange::Insert { row } => floe_json_record_from_row(row, row, encoder, false),
        CdcChange::Update { key, before, after } => {
            let key = match (key.as_ref(), before.as_ref()) {
                (Some(key), _) => floe_json_key_bytes_from_key(key, encoder)?,
                (None, Some(before)) => floe_json_key_bytes_from_row(before, encoder)?,
                (None, None) => floe_json_key_bytes_from_row(after, encoder)?,
            };
            Ok(CdcBufferRecord::new(
                Some(key),
                Some(floe_json_value_bytes_from_row(after, encoder, false)?),
            ))
        }
        CdcChange::Delete { key, before } => {
            let (key, value) = match (key.as_ref(), before.as_ref()) {
                (Some(key), Some(row)) => (
                    floe_json_key_bytes_from_key(key, encoder)?,
                    floe_json_value_bytes_from_row(row, encoder, true)?,
                ),
                (Some(key), None) => (
                    floe_json_key_bytes_from_key(key, encoder)?,
                    floe_json_value_bytes_from_key(key, encoder)?,
                ),
                (None, Some(row)) => (
                    floe_json_key_bytes_from_row(row, encoder)?,
                    floe_json_value_bytes_from_row(row, encoder, true)?,
                ),
                (None, None) => {
                    return Err(anyhow!("CDC delete requires a key or before row"));
                }
            };
            Ok(CdcBufferRecord::new(Some(key), Some(value)))
        }
        CdcChange::Truncate => Err(anyhow!(
            "Floe JSON replication for table '{}' does not support truncate",
            schema.table_id().as_str()
        )),
    }
}

pub(super) fn encode_floe_json_buffered_change_batches(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    batches: &[ChangeBatch],
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    let record_count = batches.iter().map(ChangeBatch::change_count).sum::<usize>();
    let mut records = Vec::with_capacity(record_count);
    for batch in batches {
        anyhow::ensure!(
            batch.table_id() == &plan.table_id,
            "replication pipeline '{}' buffered change batch table '{}' does not match plan table '{}'",
            plan.name,
            batch.table_id().as_str(),
            plan.table_id.as_str()
        );
        if let Some(rows) = batch.snapshot_insert_rows() {
            records.extend(encode_floe_json_snapshot_pipeline_records(
                plan, schema, rows,
            )?);
        } else {
            records.extend(encode_floe_json_pipeline_records(plan, schema, batch)?);
        }
    }
    Ok(records)
}

fn encode_floe_json_snapshot_pipeline_records(
    _plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    rows: &CdcColumnarRowBatch,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    validate_floe_json_schema(schema)?;
    schema.validate_columnar_rows(rows)?;
    let encoder = FloeJsonColumnarEncoder::new(schema)?;
    let mut records = Vec::with_capacity(rows.row_count());
    for row_idx in 0..rows.row_count() {
        records.push(floe_json_record_from_columnar_row(rows, row_idx, &encoder)?);
    }
    Ok(records)
}

pub(super) fn validate_floe_json_schema(schema: &CdcTableSchema) -> anyhow::Result<()> {
    for column in schema.columns() {
        anyhow::ensure!(
            column.name() != FLOE_JSON_DELETED_FIELD && column.name() != FLOE_JSON_VERSION_FIELD,
            "Floe JSON replication for table '{}' cannot encode source column '{}' because it is a reserved metadata field",
            schema.table_id().as_str(),
            column.name()
        );
    }
    Ok(())
}

struct FloeJsonColumnarField {
    column_idx: usize,
    name: String,
    prefix: Vec<u8>,
    data_type: ColumnType,
}

struct FloeJsonRowEncoder {
    value_fields: Vec<FloeJsonColumnarField>,
    key_fields: Vec<FloeJsonColumnarField>,
}

struct FloeJsonColumnarEncoder {
    value_fields: Vec<FloeJsonColumnarField>,
    key_fields: Vec<FloeJsonColumnarField>,
}

impl FloeJsonRowEncoder {
    fn new(schema: &CdcTableSchema) -> anyhow::Result<Self> {
        Ok(Self {
            value_fields: floe_json_value_fields(schema)?,
            key_fields: floe_json_key_fields(schema)?,
        })
    }
}

impl FloeJsonColumnarEncoder {
    fn new(schema: &CdcTableSchema) -> anyhow::Result<Self> {
        let value_fields = floe_json_value_fields(schema)?;
        let key_fields = floe_json_key_fields(schema)?;
        Ok(Self {
            value_fields,
            key_fields,
        })
    }
}

fn floe_json_value_fields(schema: &CdcTableSchema) -> anyhow::Result<Vec<FloeJsonColumnarField>> {
    schema
        .columns()
        .iter()
        .enumerate()
        .map(|(column_idx, column)| {
            Ok(FloeJsonColumnarField {
                column_idx,
                name: column.name().to_string(),
                prefix: encoded_json_field_prefix(column.name(), column_idx == 0)?,
                data_type: column.data_type().clone(),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()
}

fn floe_json_key_fields(schema: &CdcTableSchema) -> anyhow::Result<Vec<FloeJsonColumnarField>> {
    let primary_key_indices = schema.primary_key_indices();
    schema
        .primary_key()
        .columns()
        .iter()
        .zip(primary_key_indices)
        .enumerate()
        .map(|(key_idx, (column_name, column_idx))| {
            let column = &schema.columns()[column_idx];
            Ok(FloeJsonColumnarField {
                column_idx,
                name: column_name.clone(),
                prefix: encoded_json_field_prefix(column_name, key_idx == 0)?,
                data_type: column.data_type().clone(),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()
}

fn encoded_json_field_prefix(field_name: &str, first: bool) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(field_name.len() + 4);
    if !first {
        out.push(b',');
    }
    serde_json::to_writer(&mut out, field_name)?;
    out.push(b':');
    Ok(out)
}

fn floe_json_record_from_columnar_row(
    rows: &CdcColumnarRowBatch,
    row_idx: usize,
    encoder: &FloeJsonColumnarEncoder,
) -> anyhow::Result<CdcBufferRecord> {
    Ok(CdcBufferRecord::new(
        Some(floe_json_columnar_key_bytes(rows, row_idx, encoder)?),
        Some(floe_json_columnar_value_bytes(
            rows, row_idx, encoder, false,
        )?),
    ))
}

fn floe_json_columnar_key_bytes(
    rows: &CdcColumnarRowBatch,
    row_idx: usize,
    encoder: &FloeJsonColumnarEncoder,
) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(encoder.key_fields.len() * 24);
    out.push(b'{');
    for field in &encoder.key_fields {
        out.extend_from_slice(&field.prefix);
        let column = rows
            .columns()
            .get(field.column_idx)
            .ok_or_else(|| anyhow!("CDC column index {} out of bounds", field.column_idx))?;
        append_floe_json_columnar_value(&mut out, column, row_idx, field, false)?;
    }
    out.push(b'}');
    Ok(out)
}

fn floe_json_columnar_value_bytes(
    rows: &CdcColumnarRowBatch,
    row_idx: usize,
    encoder: &FloeJsonColumnarEncoder,
    deleted: bool,
) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(encoder.value_fields.len() * 32 + 64);
    out.push(b'{');
    for field in &encoder.value_fields {
        out.extend_from_slice(&field.prefix);
        let column = rows
            .columns()
            .get(field.column_idx)
            .ok_or_else(|| anyhow!("CDC column index {} out of bounds", field.column_idx))?;
        append_floe_json_columnar_value(&mut out, column, row_idx, field, true)?;
    }
    let mut first = encoder.value_fields.is_empty();
    append_floe_json_metadata(&mut out, &mut first, deleted)?;
    out.push(b'}');
    Ok(out)
}

fn floe_json_record_from_row(
    key_row: &CdcRow,
    row: &CdcRow,
    encoder: &FloeJsonRowEncoder,
    deleted: bool,
) -> anyhow::Result<CdcBufferRecord> {
    Ok(CdcBufferRecord::new(
        Some(floe_json_key_bytes_from_row(key_row, encoder)?),
        Some(floe_json_value_bytes_from_row(row, encoder, deleted)?),
    ))
}

fn floe_json_key_bytes_from_row(
    row: &CdcRow,
    encoder: &FloeJsonRowEncoder,
) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(encoder.key_fields.len() * 24);
    out.push(b'{');
    for field in &encoder.key_fields {
        out.extend_from_slice(&field.prefix);
        let value = row
            .values()
            .get(field.column_idx)
            .ok_or_else(|| anyhow!("CDC row missing primary-key column '{}'", field.name))?
            .as_ref()
            .ok_or_else(|| anyhow!("CDC primary-key column '{}' cannot be NULL", field.name))?;
        append_floe_json_value(&mut out, value, &field.data_type)?;
    }
    out.push(b'}');
    Ok(out)
}

fn floe_json_key_bytes_from_key(
    key: &CdcRowKey,
    encoder: &FloeJsonRowEncoder,
) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(
        key.values().len() == encoder.key_fields.len(),
        "CDC row key has {} values but schema expects {}",
        key.values().len(),
        encoder.key_fields.len()
    );
    let mut out = Vec::with_capacity(encoder.key_fields.len() * 24);
    out.push(b'{');
    for (field, value) in encoder.key_fields.iter().zip(key.values()) {
        out.extend_from_slice(&field.prefix);
        append_floe_json_value(&mut out, value, &field.data_type)?;
    }
    out.push(b'}');
    Ok(out)
}

fn floe_json_value_bytes_from_row(
    row: &CdcRow,
    encoder: &FloeJsonRowEncoder,
    deleted: bool,
) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(
        row.values().len() == encoder.value_fields.len(),
        "CDC row has {} values but schema expects {}",
        row.values().len(),
        encoder.value_fields.len()
    );
    let mut out = Vec::with_capacity(encoder.value_fields.len() * 32 + 64);
    out.push(b'{');
    for field in &encoder.value_fields {
        out.extend_from_slice(&field.prefix);
        let value = row
            .values()
            .get(field.column_idx)
            .ok_or_else(|| anyhow!("CDC row missing column '{}'", field.name))?;
        if let Some(value) = value {
            append_floe_json_value(&mut out, value, &field.data_type)?;
        } else {
            out.extend_from_slice(b"null");
        }
    }
    let mut first = encoder.value_fields.is_empty();
    append_floe_json_metadata(&mut out, &mut first, deleted)?;
    out.push(b'}');
    Ok(out)
}

fn floe_json_value_bytes_from_key(
    key: &CdcRowKey,
    encoder: &FloeJsonRowEncoder,
) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(
        key.values().len() == encoder.key_fields.len(),
        "CDC row key has {} values but schema expects {}",
        key.values().len(),
        encoder.key_fields.len()
    );
    let mut out = Vec::with_capacity(encoder.key_fields.len() * 24 + 64);
    out.push(b'{');
    for (field, value) in encoder.key_fields.iter().zip(key.values()) {
        out.extend_from_slice(&field.prefix);
        append_floe_json_value(&mut out, value, &field.data_type)?;
    }
    let mut first = encoder.key_fields.is_empty();
    append_floe_json_metadata(&mut out, &mut first, true)?;
    out.push(b'}');
    Ok(out)
}

fn append_floe_json_metadata(
    out: &mut Vec<u8>,
    first: &mut bool,
    deleted: bool,
) -> anyhow::Result<()> {
    if !*first {
        out.push(b',');
    }
    *first = false;
    if deleted {
        out.extend_from_slice(br#""__floe_deleted":true,"__floe_version":"#);
    } else {
        out.extend_from_slice(br#""__floe_deleted":false,"__floe_version":"#);
    }
    write!(out, "{FLOE_JSON_VERSION}")?;
    Ok(())
}

fn append_floe_json_value(
    out: &mut Vec<u8>,
    value: &RowValue,
    data_type: &ColumnType,
) -> anyhow::Result<()> {
    match value {
        RowValue::Int64(value) => write!(out, "{value}")?,
        RowValue::Bool(value) => out.extend_from_slice(if *value { b"true" } else { b"false" }),
        RowValue::Utf8(value) => serde_json::to_writer(out, value)?,
        RowValue::TimestampMillis(value) => write!(out, "{value}")?,
        RowValue::DateDays(value) => write!(out, "{value}")?,
        RowValue::Decimal128(value) => match data_type {
            ColumnType::Decimal128 { scale, .. } => {
                append_decimal128_json_string(out, *value, *scale)?;
            }
            _ => serde_json::to_writer(out, &value.to_string())?,
        },
        RowValue::Numeric(value) => serde_json::to_writer(out, value)?,
    }
    Ok(())
}

fn append_floe_json_columnar_value(
    out: &mut Vec<u8>,
    column: &CdcColumnarColumn,
    row_idx: usize,
    field: &FloeJsonColumnarField,
    allow_null: bool,
) -> anyhow::Result<()> {
    match column {
        CdcColumnarColumn::Int64(values) => match columnar_value(values, row_idx)? {
            Some(value) => write!(out, "{value}")?,
            None => append_floe_json_columnar_null(out, field, allow_null)?,
        },
        CdcColumnarColumn::Bool(values) => match columnar_value(values, row_idx)? {
            Some(value) => out.extend_from_slice(if *value { b"true" } else { b"false" }),
            None => append_floe_json_columnar_null(out, field, allow_null)?,
        },
        CdcColumnarColumn::Utf8(values) => match columnar_value(values, row_idx)? {
            Some(value) => serde_json::to_writer(out, value)?,
            None => append_floe_json_columnar_null(out, field, allow_null)?,
        },
        CdcColumnarColumn::TimestampMillis(values) => match columnar_value(values, row_idx)? {
            Some(value) => write!(out, "{value}")?,
            None => append_floe_json_columnar_null(out, field, allow_null)?,
        },
        CdcColumnarColumn::DateDays(values) => match columnar_value(values, row_idx)? {
            Some(value) => write!(out, "{value}")?,
            None => append_floe_json_columnar_null(out, field, allow_null)?,
        },
        CdcColumnarColumn::Decimal128 { values, .. } => match columnar_value(values, row_idx)? {
            Some(value) => match &field.data_type {
                ColumnType::Decimal128 { scale, .. } => {
                    append_decimal128_json_string(out, *value, *scale)?;
                }
                _ => serde_json::to_writer(out, &value.to_string())?,
            },
            None => append_floe_json_columnar_null(out, field, allow_null)?,
        },
        CdcColumnarColumn::Numeric(values) => match columnar_value(values, row_idx)? {
            Some(value) => serde_json::to_writer(out, value)?,
            None => append_floe_json_columnar_null(out, field, allow_null)?,
        },
    }
    Ok(())
}

fn columnar_value<T>(values: &[Option<T>], row_idx: usize) -> anyhow::Result<Option<&T>> {
    values
        .get(row_idx)
        .map(Option::as_ref)
        .ok_or_else(|| anyhow!("CDC columnar row index {row_idx} out of bounds"))
}

fn append_floe_json_columnar_null(
    out: &mut Vec<u8>,
    field: &FloeJsonColumnarField,
    allow_null: bool,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        allow_null,
        "CDC primary key column '{}' cannot be NULL",
        field.name
    );
    out.extend_from_slice(b"null");
    Ok(())
}

fn append_decimal128_json_string(out: &mut Vec<u8>, value: i128, scale: i8) -> anyhow::Result<()> {
    out.push(b'"');
    append_decimal128_text(out, value, scale)?;
    out.push(b'"');
    Ok(())
}

fn append_decimal128_text(out: &mut Vec<u8>, value: i128, scale: i8) -> anyhow::Result<()> {
    if scale <= 0 {
        write!(out, "{value}")?;
        return Ok(());
    }
    let scale = scale as u32;
    let factor = 10_u128
        .checked_pow(scale)
        .ok_or_else(|| anyhow!("Decimal128 scale {scale} is too large"))?;
    if value < 0 {
        out.push(b'-');
    }
    let magnitude = value.unsigned_abs();
    let whole = magnitude / factor;
    let fraction = magnitude % factor;
    write!(out, "{whole}.{fraction:0width$}", width = scale as usize)?;
    Ok(())
}

pub(super) fn format_decimal128_for_json(value: i128, scale: i8) -> String {
    if scale <= 0 {
        return value.to_string();
    }
    let scale = scale as u32;
    let factor = 10_i128.pow(scale);
    let sign = if value < 0 { "-" } else { "" };
    let magnitude = value.abs();
    let whole = magnitude / factor;
    let fraction = magnitude % factor;
    format!("{sign}{whole}.{fraction:0width$}", width = scale as usize)
}

pub(super) fn encode_debezium_pipeline_records(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    batch: &ChangeBatch,
    transaction: &TransactionBatch,
) -> anyhow::Result<Vec<DebeziumEncodedRecord>> {
    let config = DebeziumEnvelopeConfig::new(&plan.source_name)?
        .with_database_name(&plan.database_name)
        .with_emit_tombstones(plan.emit_tombstones)
        .with_transaction_metadata(plan.include_transaction_metadata);
    let is_snapshot = transaction
        .transaction_id()
        .is_some_and(|tx| tx.as_str().starts_with("snapshot:"));
    encode_debezium_change_batch_with_options(
        schema,
        batch,
        &config,
        DebeziumEncodeContext {
            source_position: Some(transaction.commit_position()),
            transaction_id: transaction.transaction_id(),
            sequence: Some(0),
            ts_ms: None,
        },
        DebeziumBatchEncodeOptions {
            snapshot_read: is_snapshot,
        },
    )
}

fn debezium_records_to_buffer_records(
    records: &[DebeziumEncodedRecord],
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    records
        .iter()
        .map(|record| {
            Ok(CdcBufferRecord::new(
                record.key_json_bytes()?,
                record.value_json_bytes()?,
            ))
        })
        .collect()
}

fn encode_arrow_ipc_pipeline_records(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    batch: &ChangeBatch,
    transaction: &TransactionBatch,
    settings: ReplicationEncodingConfig,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    let mut records = Vec::new();
    let rows_per_record = settings.arrow_ipc_rows_per_record.max(1);
    let mut builder = ArrowIpcChangeBatchBuilder::new(schema, rows_per_record);
    let is_snapshot = transaction
        .transaction_id()
        .is_some_and(|tx| tx.as_str().starts_with("snapshot:"));
    for (idx, change) in batch.changes().iter().enumerate() {
        let sequence = u64::try_from(idx).unwrap_or(u64::MAX);
        match change {
            CdcChange::Insert { row } => {
                builder.append_row(row, if is_snapshot { "r" } else { "c" }, 1, sequence)?;
            }
            CdcChange::Update { before, after, .. } => {
                if let Some(before) = before {
                    builder.append_row(before, "u_before", -1, sequence)?;
                    flush_arrow_ipc_record_if_full(
                        plan,
                        transaction,
                        &mut builder,
                        &mut records,
                        settings,
                    )?;
                }
                builder.append_row(after, "u", 1, sequence)?;
            }
            CdcChange::Delete { key, before } => match before {
                Some(row) => builder.append_row(row, "d", -1, sequence)?,
                None => {
                    let key = key.as_ref().ok_or_else(|| {
                        anyhow!(
                            "CDC Arrow IPC delete for table '{}' requires a key or before row",
                            schema.table_id().as_str()
                        )
                    })?;
                    let key_row = key_only_row(schema, key)?;
                    builder.append_values(&key_row, "d", -1, sequence)?;
                }
            },
            CdcChange::Truncate => {
                return Err(anyhow!(
                    "CDC Arrow IPC truncate for table '{}' is not supported",
                    schema.table_id().as_str()
                ));
            }
        }
        flush_arrow_ipc_record_if_full(plan, transaction, &mut builder, &mut records, settings)?;
    }
    if !builder.is_empty() {
        records.push(finish_arrow_ipc_record(
            plan,
            transaction,
            &mut builder,
            settings,
        )?);
    }
    Ok(records)
}

fn encode_arrow_ipc_snapshot_pipeline_records(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    rows: &CdcColumnarRowBatch,
    transaction: &TransactionBatch,
    settings: ReplicationEncodingConfig,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    schema.validate_columnar_rows(rows)?;
    let mut records = Vec::new();
    let rows_per_record = settings.arrow_ipc_rows_per_record.max(1);
    for start in (0..rows.row_count()).step_by(rows_per_record) {
        let len = rows.row_count().saturating_sub(start).min(rows_per_record);
        let batch = arrow_ipc_snapshot_record_batch(schema, rows, start, len)?;
        records.push(arrow_ipc_record_from_batch(
            plan,
            transaction,
            start / rows_per_record,
            batch,
            settings.arrow_ipc_compression,
        )?);
    }
    Ok(records)
}

fn flush_arrow_ipc_record_if_full(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    builder: &mut ArrowIpcChangeBatchBuilder,
    records: &mut Vec<CdcBufferRecord>,
    settings: ReplicationEncodingConfig,
) -> anyhow::Result<()> {
    if builder.is_full() {
        records.push(finish_arrow_ipc_record(
            plan,
            transaction,
            builder,
            settings,
        )?);
    }
    Ok(())
}

fn finish_arrow_ipc_record(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    builder: &mut ArrowIpcChangeBatchBuilder,
    settings: ReplicationEncodingConfig,
) -> anyhow::Result<CdcBufferRecord> {
    let chunk_idx = builder.chunk_idx();
    let batch = builder.finish()?;
    arrow_ipc_record_from_batch(
        plan,
        transaction,
        chunk_idx,
        batch,
        settings.arrow_ipc_compression,
    )
}

fn arrow_ipc_record_from_batch(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    chunk_idx: usize,
    batch: RecordBatch,
    compression: Option<ReplicationArrowIpcCompressionConfig>,
) -> anyhow::Result<CdcBufferRecord> {
    let mut value = Vec::new();
    {
        let mut writer = arrow_ipc_stream_writer(&mut value, batch.schema().as_ref(), compression)
            .context("create replication Arrow IPC writer")?;
        writer
            .write(&batch)
            .context("write replication Arrow IPC batch")?;
        writer
            .finish()
            .context("finish replication Arrow IPC batch")?;
    }
    let key = format!(
        "{}/{}/{chunk_idx:020}",
        plan.upstream_table,
        source_position_key(transaction.commit_position())
    )
    .into_bytes();
    Ok(CdcBufferRecord::new(Some(key), Some(value)))
}

fn arrow_ipc_stream_writer<'a>(
    value: &'a mut Vec<u8>,
    schema: &ArrowSchema,
    compression: Option<ReplicationArrowIpcCompressionConfig>,
) -> anyhow::Result<StreamWriter<&'a mut Vec<u8>>> {
    let Some(compression) = compression else {
        return StreamWriter::try_new(value, schema)
            .context("create uncompressed Arrow IPC writer");
    };
    let options = IpcWriteOptions::try_new(64, false, MetadataVersion::V5)
        .context("create Arrow IPC writer options")?
        .try_with_compression(Some(arrow_ipc_compression_type(compression)))
        .context("configure Arrow IPC compression")?;
    StreamWriter::try_new_with_options(value, schema, options)
        .with_context(|| format!("create {compression:?} Arrow IPC writer"))
}

fn arrow_ipc_compression_type(
    compression: ReplicationArrowIpcCompressionConfig,
) -> CompressionType {
    match compression {
        ReplicationArrowIpcCompressionConfig::Lz4Frame => CompressionType::LZ4_FRAME,
    }
}

fn arrow_ipc_snapshot_record_batch(
    schema: &CdcTableSchema,
    rows: &CdcColumnarRowBatch,
    start: usize,
    len: usize,
) -> anyhow::Result<RecordBatch> {
    let end = start.saturating_add(len);
    anyhow::ensure!(
        end <= rows.row_count(),
        "CDC Arrow IPC snapshot range {start}..{end} exceeds {} rows",
        rows.row_count()
    );
    let mut arrays = rows
        .columns()
        .iter()
        .zip(schema.columns())
        .map(|(values, column)| arrow_ipc_columnar_array(values, column, start, end))
        .collect::<anyhow::Result<Vec<_>>>()?;

    let mut operations = StringBuilder::with_capacity(len, len);
    let mut diffs = Int64Builder::with_capacity(len);
    let mut sequences = Int64Builder::with_capacity(len);
    for sequence in start..end {
        operations.append_value("r");
        diffs.append_value(1);
        sequences.append_value(i64::try_from(sequence).unwrap_or(i64::MAX));
    }
    arrays.push(Arc::new(operations.finish()));
    arrays.push(Arc::new(diffs.finish()));
    arrays.push(Arc::new(sequences.finish()));

    RecordBatch::try_new(arrow_ipc_schema(schema), arrays)
        .context("build replication Arrow IPC snapshot batch")
}

fn arrow_ipc_columnar_array(
    values: &CdcColumnarColumn,
    column: &CdcColumn,
    start: usize,
    end: usize,
) -> anyhow::Result<ArrayRef> {
    anyhow::ensure!(
        values.data_type() == column.data_type().clone(),
        "CDC Arrow IPC snapshot column '{}' type {:?} does not match {:?}",
        column.name(),
        values.data_type(),
        column.data_type()
    );
    let array: ArrayRef = match values {
        CdcColumnarColumn::Int64(values) => {
            Arc::new(arrow_array::Int64Array::from(values[start..end].to_vec()))
        }
        CdcColumnarColumn::Bool(values) => {
            Arc::new(arrow_array::BooleanArray::from(values[start..end].to_vec()))
        }
        CdcColumnarColumn::Utf8(values) => {
            let mut builder = StringBuilder::with_capacity(end - start, (end - start) * 16);
            for value in &values[start..end] {
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        CdcColumnarColumn::TimestampMillis(values) => Arc::new(
            arrow_array::TimestampMillisecondArray::from(values[start..end].to_vec()),
        ),
        CdcColumnarColumn::DateDays(values) => {
            Arc::new(arrow_array::Date32Array::from(values[start..end].to_vec()))
        }
        CdcColumnarColumn::Decimal128 {
            precision,
            scale,
            values,
        } => Arc::new(
            Decimal128Array::from(values[start..end].to_vec())
                .with_precision_and_scale(*precision, *scale)
                .context("build Decimal128 Arrow IPC snapshot column")?,
        ),
        CdcColumnarColumn::Numeric(values) => {
            let mut builder = StringBuilder::with_capacity(end - start, (end - start) * 16);
            for value in &values[start..end] {
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
    };
    Ok(array)
}

fn arrow_ipc_schema(schema: &CdcTableSchema) -> Arc<ArrowSchema> {
    let mut fields = schema
        .columns()
        .iter()
        .map(|column| {
            ArrowField::new(
                column.name(),
                match column.data_type() {
                    ColumnType::Int64 => DataType::Int64,
                    ColumnType::Bool => DataType::Boolean,
                    ColumnType::Utf8 => DataType::Utf8,
                    ColumnType::TimestampMillis => {
                        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
                    }
                    ColumnType::DateDays => DataType::Date32,
                    ColumnType::Decimal128 { precision, scale } => {
                        DataType::Decimal128(*precision, *scale)
                    }
                    ColumnType::Numeric => DataType::Utf8,
                },
                true,
            )
        })
        .collect::<Vec<_>>();
    fields.push(ArrowField::new("__op", DataType::Utf8, false));
    fields.push(ArrowField::new("__diff", DataType::Int64, false));
    fields.push(ArrowField::new("__sequence", DataType::Int64, false));
    Arc::new(ArrowSchema::new(fields))
}

fn key_only_row(schema: &CdcTableSchema, key: &CdcRowKey) -> anyhow::Result<Vec<Option<RowValue>>> {
    key.validate_against_schema(schema)?;
    let mut values = vec![None; schema.columns().len()];
    for (value, column_idx) in key.values().iter().zip(schema.primary_key_indices()) {
        values[column_idx] = Some(value.clone());
    }
    Ok(values)
}

pub(super) fn source_position_key(position: &CdcSourcePosition) -> String {
    match position {
        CdcSourcePosition::Postgres {
            commit_lsn,
            event_lsn,
        } => match event_lsn {
            Some(event_lsn) => format!("pg/{commit_lsn}/{event_lsn}"),
            None => format!("pg/{commit_lsn}"),
        },
        CdcSourcePosition::Opaque { value } => format!("opaque/{value}"),
    }
}

enum ArrowIpcColumnBuilder {
    Int64(Int64Builder),
    Bool(BooleanBuilder),
    Utf8(StringBuilder),
    TimestampMillis(TimestampMillisecondBuilder),
    DateDays(Date32Builder),
    Decimal128(Decimal128Builder),
    Numeric(StringBuilder),
}

impl ArrowIpcColumnBuilder {
    fn new(data_type: &ColumnType, capacity: usize) -> Self {
        match data_type {
            ColumnType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            ColumnType::Bool => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            ColumnType::Utf8 => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 16)),
            ColumnType::TimestampMillis => {
                Self::TimestampMillis(TimestampMillisecondBuilder::with_capacity(capacity))
            }
            ColumnType::DateDays => Self::DateDays(Date32Builder::with_capacity(capacity)),
            ColumnType::Decimal128 { precision, scale } => Self::Decimal128(
                Decimal128Builder::with_capacity(capacity)
                    .with_data_type(DataType::Decimal128(*precision, *scale)),
            ),
            ColumnType::Numeric => {
                Self::Numeric(StringBuilder::with_capacity(capacity, capacity * 16))
            }
        }
    }

    fn append(&mut self, column: &CdcColumn, value: Option<&RowValue>) -> anyhow::Result<()> {
        match (self, column.data_type(), value) {
            (Self::Int64(builder), ColumnType::Int64, Some(RowValue::Int64(value))) => {
                builder.append_value(*value);
            }
            (Self::Bool(builder), ColumnType::Bool, Some(RowValue::Bool(value))) => {
                builder.append_value(*value);
            }
            (Self::Utf8(builder), ColumnType::Utf8, Some(RowValue::Utf8(value))) => {
                builder.append_value(value);
            }
            (
                Self::TimestampMillis(builder),
                ColumnType::TimestampMillis,
                Some(RowValue::TimestampMillis(value)),
            ) => {
                builder.append_value(*value);
            }
            (Self::DateDays(builder), ColumnType::DateDays, Some(RowValue::DateDays(value))) => {
                builder.append_value(*value);
            }
            (
                Self::Decimal128(builder),
                ColumnType::Decimal128 { .. },
                Some(RowValue::Decimal128(value)),
            ) => {
                builder.append_value(*value);
            }
            (Self::Numeric(builder), ColumnType::Numeric, Some(RowValue::Numeric(value))) => {
                builder.append_value(value);
            }
            (Self::Int64(builder), ColumnType::Int64, None) => builder.append_null(),
            (Self::Bool(builder), ColumnType::Bool, None) => builder.append_null(),
            (Self::Utf8(builder), ColumnType::Utf8, None) => builder.append_null(),
            (Self::TimestampMillis(builder), ColumnType::TimestampMillis, None) => {
                builder.append_null();
            }
            (Self::DateDays(builder), ColumnType::DateDays, None) => builder.append_null(),
            (Self::Decimal128(builder), ColumnType::Decimal128 { .. }, None) => {
                builder.append_null();
            }
            (Self::Numeric(builder), ColumnType::Numeric, None) => builder.append_null(),
            (_, _, Some(value)) => {
                return Err(anyhow!(
                    "CDC Arrow IPC value for column '{}' does not match type {:?}: {:?}",
                    column.name(),
                    column.data_type(),
                    value
                ));
            }
            _ => {
                return Err(anyhow!(
                    "CDC Arrow IPC builder for column '{}' does not match type {:?}",
                    column.name(),
                    column.data_type()
                ));
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::TimestampMillis(builder) => Arc::new(builder.finish()),
            Self::DateDays(builder) => Arc::new(builder.finish()),
            Self::Decimal128(builder) => Arc::new(builder.finish()),
            Self::Numeric(builder) => Arc::new(builder.finish()),
        }
    }
}

struct ArrowIpcChangeBatchBuilder {
    schema: CdcTableSchema,
    arrow_schema: Arc<ArrowSchema>,
    columns: Vec<ArrowIpcColumnBuilder>,
    operations: StringBuilder,
    diffs: Int64Builder,
    sequences: Int64Builder,
    len: usize,
    capacity: usize,
    chunk_idx: usize,
}

impl ArrowIpcChangeBatchBuilder {
    fn new(schema: &CdcTableSchema, capacity: usize) -> Self {
        Self {
            schema: schema.clone(),
            arrow_schema: arrow_ipc_schema(schema),
            columns: schema
                .columns()
                .iter()
                .map(|column| ArrowIpcColumnBuilder::new(column.data_type(), capacity))
                .collect(),
            operations: StringBuilder::with_capacity(capacity, capacity * 2),
            diffs: Int64Builder::with_capacity(capacity),
            sequences: Int64Builder::with_capacity(capacity),
            len: 0,
            capacity,
            chunk_idx: 0,
        }
    }

    fn append_row(
        &mut self,
        row: &CdcRow,
        operation: &str,
        diff: i64,
        sequence: u64,
    ) -> anyhow::Result<()> {
        self.append_values(row.values(), operation, diff, sequence)
    }

    fn append_values(
        &mut self,
        values: &[Option<RowValue>],
        operation: &str,
        diff: i64,
        sequence: u64,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            values.len() == self.schema.columns().len(),
            "CDC Arrow IPC row has {} values, expected {}",
            values.len(),
            self.schema.columns().len()
        );
        for ((builder, column), value) in self
            .columns
            .iter_mut()
            .zip(self.schema.columns())
            .zip(values)
        {
            builder.append(column, value.as_ref())?;
        }
        self.operations.append_value(operation);
        self.diffs.append_value(diff);
        self.sequences
            .append_value(i64::try_from(sequence).unwrap_or(i64::MAX));
        self.len += 1;
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn is_full(&self) -> bool {
        self.len >= self.capacity
    }

    fn chunk_idx(&self) -> usize {
        self.chunk_idx
    }

    fn finish(&mut self) -> anyhow::Result<RecordBatch> {
        let mut arrays = self
            .columns
            .iter_mut()
            .map(ArrowIpcColumnBuilder::finish)
            .collect::<Vec<_>>();
        arrays.push(Arc::new(self.operations.finish()));
        arrays.push(Arc::new(self.diffs.finish()));
        arrays.push(Arc::new(self.sequences.finish()));
        let batch = RecordBatch::try_new(Arc::clone(&self.arrow_schema), arrays)
            .context("build replication Arrow IPC batch")?;
        self.columns = self
            .schema
            .columns()
            .iter()
            .map(|column| ArrowIpcColumnBuilder::new(column.data_type(), self.capacity))
            .collect();
        self.operations = StringBuilder::with_capacity(self.capacity, self.capacity * 2);
        self.diffs = Int64Builder::with_capacity(self.capacity);
        self.sequences = Int64Builder::with_capacity(self.capacity);
        self.len = 0;
        self.chunk_idx += 1;
        Ok(batch)
    }
}
