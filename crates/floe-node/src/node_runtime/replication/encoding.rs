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
use floe_core::decimal::append_decimal128_text;
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

#[path = "encoding/arrow_ipc_encoding.rs"]
mod arrow_ipc_encoding;

use self::arrow_ipc_encoding::{
    encode_arrow_ipc_pipeline_records, encode_arrow_ipc_snapshot_pipeline_records,
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
    let primary_key_indices = schema.primary_key_indices()?;
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
