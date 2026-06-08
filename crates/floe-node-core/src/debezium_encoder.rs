use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use floe_cdc_core::{
    CdcChange, CdcRow, CdcRowKey, CdcSourcePosition, CdcTableSchema, CdcTransactionId, ChangeBatch,
};
use floe_core::{RowValue, catalog::ColumnType, decimal::format_decimal128};
use serde_json::{Map, Value, json};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DebeziumEnvelopeConfig {
    source_name: String,
    database_name: String,
    emit_tombstones: bool,
    include_transaction_metadata: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DebeziumEncodeContext<'a> {
    pub source_position: Option<&'a CdcSourcePosition>,
    pub transaction_id: Option<&'a CdcTransactionId>,
    pub sequence: Option<u64>,
    pub ts_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DebeziumBatchEncodeOptions {
    pub snapshot_read: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DebeziumEncodedRecord {
    key: Option<Value>,
    value: Option<Value>,
}

impl DebeziumEnvelopeConfig {
    pub fn new(source_name: impl Into<String>) -> Result<Self> {
        let source_name = source_name.into();
        anyhow::ensure!(
            !source_name.trim().is_empty(),
            "Debezium source name cannot be empty"
        );
        Ok(Self {
            database_name: source_name.clone(),
            source_name,
            emit_tombstones: false,
            include_transaction_metadata: false,
        })
    }

    pub fn with_database_name(mut self, database_name: impl Into<String>) -> Self {
        let database_name = database_name.into();
        if !database_name.trim().is_empty() {
            self.database_name = database_name;
        }
        self
    }

    pub fn with_emit_tombstones(mut self, emit_tombstones: bool) -> Self {
        self.emit_tombstones = emit_tombstones;
        self
    }

    pub fn with_transaction_metadata(mut self, include_transaction_metadata: bool) -> Self {
        self.include_transaction_metadata = include_transaction_metadata;
        self
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn emit_tombstones(&self) -> bool {
        self.emit_tombstones
    }

    pub fn include_transaction_metadata(&self) -> bool {
        self.include_transaction_metadata
    }
}

impl DebeziumEncodedRecord {
    pub fn new(key: Option<Value>, value: Option<Value>) -> Self {
        Self { key, value }
    }

    pub fn key(&self) -> Option<&Value> {
        self.key.as_ref()
    }

    pub fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }

    pub fn key_json_bytes(&self) -> Result<Option<Vec<u8>>> {
        self.key
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()
            .context("encode Debezium Kafka key")
    }

    pub fn value_json_bytes(&self) -> Result<Option<Vec<u8>>> {
        self.value
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()
            .context("encode Debezium Kafka value")
    }
}

pub fn encode_debezium_change_batch(
    schema: &CdcTableSchema,
    batch: &ChangeBatch,
    config: &DebeziumEnvelopeConfig,
    context: DebeziumEncodeContext<'_>,
) -> Result<Vec<DebeziumEncodedRecord>> {
    encode_debezium_change_batch_with_options(
        schema,
        batch,
        config,
        context,
        DebeziumBatchEncodeOptions::default(),
    )
}

pub fn encode_debezium_change_batch_with_options(
    schema: &CdcTableSchema,
    batch: &ChangeBatch,
    config: &DebeziumEnvelopeConfig,
    context: DebeziumEncodeContext<'_>,
    options: DebeziumBatchEncodeOptions,
) -> Result<Vec<DebeziumEncodedRecord>> {
    anyhow::ensure!(
        batch.table_id() == schema.table_id(),
        "Debezium batch table '{}' does not match schema table '{}'",
        batch.table_id().as_str(),
        schema.table_id().as_str()
    );
    if let Some(rows) = batch.snapshot_insert_rows() {
        let mut records = Vec::with_capacity(rows.row_count());
        for row_idx in 0..rows.row_count() {
            let mut change_context = context;
            change_context.sequence = Some(
                context
                    .sequence
                    .unwrap_or(0)
                    .saturating_add(u64::try_from(row_idx).unwrap_or(u64::MAX)),
            );
            records.push(encode_debezium_snapshot_row(
                schema,
                &rows.row(row_idx)?,
                config,
                change_context,
            )?);
        }
        return Ok(records);
    }
    let mut records = Vec::new();
    for (idx, change) in batch.changes().iter().enumerate() {
        let mut change_context = context;
        change_context.sequence = Some(
            context
                .sequence
                .unwrap_or(0)
                .saturating_add(u64::try_from(idx).unwrap_or(u64::MAX)),
        );
        if options.snapshot_read
            && let CdcChange::Insert { row } = change
        {
            records.push(encode_debezium_snapshot_row(
                schema,
                row,
                config,
                change_context,
            )?);
            continue;
        }
        records.extend(encode_debezium_change(
            schema,
            change,
            config,
            change_context,
        )?);
    }
    Ok(records)
}

pub fn encode_debezium_change(
    schema: &CdcTableSchema,
    change: &CdcChange,
    config: &DebeziumEnvelopeConfig,
    context: DebeziumEncodeContext<'_>,
) -> Result<Vec<DebeziumEncodedRecord>> {
    change.validate_against_schema(schema)?;
    let ts_ms = context.ts_ms.unwrap_or_else(current_unix_time_ms);
    let source = source_metadata(schema, config, context, ts_ms, false);
    let (key, before, after, op) = match change {
        CdcChange::Insert { row } => (
            Some(schema.primary_key_from_row(row)?),
            Value::Null,
            row_to_json(schema, row)?,
            "c",
        ),
        CdcChange::Update { key, before, after } => {
            let key = key_for_update(schema, key.as_ref(), before.as_ref(), after)?;
            (
                Some(key),
                before
                    .as_ref()
                    .map(|row| row_to_json(schema, row))
                    .transpose()?
                    .unwrap_or(Value::Null),
                row_to_json(schema, after)?,
                "u",
            )
        }
        CdcChange::Delete { key, before } => {
            let key = key_for_delete(schema, key.as_ref(), before.as_ref())?;
            (
                Some(key),
                before
                    .as_ref()
                    .map(|row| row_to_json(schema, row))
                    .transpose()?
                    .unwrap_or(Value::Null),
                Value::Null,
                "d",
            )
        }
        CdcChange::Truncate => (None, Value::Null, Value::Null, "t"),
    };

    let key_payload = key
        .as_ref()
        .map(|key| row_key_to_json(schema, key))
        .transpose()?;
    let key_record = key_payload.map(|payload| wrap_debezium_key(schema, config, payload));
    let value = wrap_debezium_value(
        schema,
        config,
        envelope(before, after, source, op, ts_ms, config, context),
    );
    let mut records = vec![DebeziumEncodedRecord::new(key_record.clone(), Some(value))];
    if matches!(change, CdcChange::Delete { .. }) && config.emit_tombstones {
        records.push(DebeziumEncodedRecord::new(key_record, None));
    }
    Ok(records)
}

pub fn encode_debezium_snapshot_row(
    schema: &CdcTableSchema,
    row: &CdcRow,
    config: &DebeziumEnvelopeConfig,
    context: DebeziumEncodeContext<'_>,
) -> Result<DebeziumEncodedRecord> {
    schema.validate_row(row)?;
    let key = schema.primary_key_from_row(row)?;
    let ts_ms = context.ts_ms.unwrap_or_else(current_unix_time_ms);
    Ok(DebeziumEncodedRecord::new(
        Some(wrap_debezium_key(
            schema,
            config,
            row_key_to_json(schema, &key)?,
        )),
        Some(wrap_debezium_value(
            schema,
            config,
            envelope(
                Value::Null,
                row_to_json(schema, row)?,
                source_metadata(schema, config, context, ts_ms, true),
                "r",
                ts_ms,
                config,
                context,
            ),
        )),
    ))
}

fn envelope(
    before: Value,
    after: Value,
    source: Value,
    op: &str,
    ts_ms: i64,
    config: &DebeziumEnvelopeConfig,
    context: DebeziumEncodeContext<'_>,
) -> Value {
    let mut object = Map::new();
    object.insert("before".to_string(), before);
    object.insert("after".to_string(), after);
    object.insert("source".to_string(), source);
    object.insert("op".to_string(), Value::String(op.to_string()));
    object.insert("ts_ms".to_string(), json!(ts_ms));
    object.insert("ts_us".to_string(), json!(ts_ms.saturating_mul(1_000)));
    object.insert("ts_ns".to_string(), json!(ts_ms.saturating_mul(1_000_000)));
    if config.include_transaction_metadata {
        let transaction = context.transaction_id.map_or(Value::Null, |tx| {
            json!({
                "id": tx.as_str(),
                "total_order": context.sequence.unwrap_or(0),
                "data_collection_order": context.sequence.unwrap_or(0),
            })
        });
        object.insert("transaction".to_string(), transaction);
    }
    Value::Object(object)
}

fn source_metadata(
    schema: &CdcTableSchema,
    config: &DebeziumEnvelopeConfig,
    context: DebeziumEncodeContext<'_>,
    ts_ms: i64,
    snapshot: bool,
) -> Value {
    let mut source = Map::new();
    source.insert("version".to_string(), Value::String("floe".to_string()));
    source.insert(
        "connector".to_string(),
        Value::String("postgresql".to_string()),
    );
    source.insert(
        "name".to_string(),
        Value::String(config.source_name().to_string()),
    );
    source.insert("ts_ms".to_string(), json!(ts_ms));
    source.insert("ts_us".to_string(), json!(ts_ms.saturating_mul(1_000)));
    source.insert("ts_ns".to_string(), json!(ts_ms.saturating_mul(1_000_000)));
    source.insert(
        "snapshot".to_string(),
        Value::String(if snapshot { "true" } else { "false" }.to_string()),
    );
    source.insert(
        "db".to_string(),
        Value::String(config.database_name().to_string()),
    );
    source.insert("sequence".to_string(), Value::Null);
    source.insert(
        "schema".to_string(),
        Value::String(schema.upstream_table().schema().to_string()),
    );
    source.insert(
        "table".to_string(),
        Value::String(schema.upstream_table().table().to_string()),
    );
    source.insert(
        "txId".to_string(),
        transaction_id_to_debezium_txid(context.transaction_id).unwrap_or(Value::Null),
    );
    source.insert("lsn".to_string(), Value::Null);
    source.insert("xmin".to_string(), Value::Null);
    if let Some(position) = context.source_position {
        match position {
            CdcSourcePosition::Postgres {
                commit_lsn,
                event_lsn,
            } => {
                let commit_lsn_u64 = postgres_lsn_to_u64(commit_lsn);
                let event_lsn_u64 = event_lsn.as_deref().and_then(postgres_lsn_to_u64);
                if let Some(commit_lsn_u64) = commit_lsn_u64 {
                    source.insert(
                        "lsn".to_string(),
                        json!(event_lsn_u64.unwrap_or(commit_lsn_u64)),
                    );
                }
                source.insert(
                    "sequence".to_string(),
                    Value::String(format!(
                        "[{},{}]",
                        commit_lsn_u64
                            .map(|lsn| format!("\"{lsn}\""))
                            .unwrap_or_else(|| "null".to_string()),
                        event_lsn_u64
                            .map(|lsn| format!("\"{lsn}\""))
                            .unwrap_or_else(|| "null".to_string())
                    )),
                );
            }
            CdcSourcePosition::Opaque { value } => {
                source.insert("position".to_string(), Value::String(value.clone()));
            }
        }
    }
    Value::Object(source)
}

fn wrap_debezium_key(
    schema: &CdcTableSchema,
    config: &DebeziumEnvelopeConfig,
    payload: Value,
) -> Value {
    wrap_debezium_message(debezium_key_schema(schema, config), payload)
}

fn wrap_debezium_value(
    schema: &CdcTableSchema,
    config: &DebeziumEnvelopeConfig,
    payload: Value,
) -> Value {
    wrap_debezium_message(debezium_envelope_schema(schema, config), payload)
}

fn wrap_debezium_message(schema: Value, payload: Value) -> Value {
    json!({
        "schema": schema,
        "payload": payload,
    })
}

fn debezium_key_schema(schema: &CdcTableSchema, config: &DebeziumEnvelopeConfig) -> Value {
    let fields = schema
        .primary_key()
        .columns()
        .iter()
        .filter_map(|column_name| {
            schema
                .columns()
                .iter()
                .find(|column| column.name() == column_name)
                .map(|column| debezium_column_schema(column.name(), column.data_type(), false))
        })
        .collect::<Vec<_>>();
    json!({
        "type": "struct",
        "fields": fields,
        "optional": false,
        "name": format!("{}.{}.{}.Key", config.source_name(), schema.upstream_table().schema(), schema.upstream_table().table()),
    })
}

fn debezium_envelope_schema(schema: &CdcTableSchema, config: &DebeziumEnvelopeConfig) -> Value {
    let mut fields = Vec::with_capacity(if config.include_transaction_metadata() {
        7
    } else {
        6
    });
    fields.push(debezium_named_schema(
        row_struct_schema(schema, config, true),
        "before",
        true,
    ));
    fields.push(debezium_named_schema(
        row_struct_schema(schema, config, true),
        "after",
        true,
    ));
    fields.push(debezium_named_schema(
        debezium_source_schema(),
        "source",
        false,
    ));
    fields.push(debezium_primitive_schema("op", "string", false));
    fields.push(debezium_primitive_schema("ts_ms", "int64", true));
    fields.push(debezium_primitive_schema("ts_us", "int64", true));
    fields.push(debezium_primitive_schema("ts_ns", "int64", true));
    if config.include_transaction_metadata() {
        fields.push(debezium_named_schema(
            debezium_transaction_schema(),
            "transaction",
            true,
        ));
    }
    json!({
        "type": "struct",
        "fields": fields,
        "optional": false,
        "name": format!("{}.{}.{}.Envelope", config.source_name(), schema.upstream_table().schema(), schema.upstream_table().table()),
    })
}

fn row_struct_schema(
    schema: &CdcTableSchema,
    config: &DebeziumEnvelopeConfig,
    optional: bool,
) -> Value {
    let fields = schema
        .columns()
        .iter()
        .map(|column| debezium_column_schema(column.name(), column.data_type(), column.nullable()))
        .collect::<Vec<_>>();
    json!({
        "type": "struct",
        "fields": fields,
        "optional": optional,
        "name": format!("{}.{}.{}.Value", config.source_name(), schema.upstream_table().schema(), schema.upstream_table().table()),
    })
}

fn debezium_source_schema() -> Value {
    json!({
        "type": "struct",
        "fields": [
            debezium_primitive_schema("version", "string", false),
            debezium_primitive_schema("connector", "string", false),
            debezium_primitive_schema("name", "string", false),
            debezium_primitive_schema("ts_ms", "int64", true),
            debezium_primitive_schema("ts_us", "int64", true),
            debezium_primitive_schema("ts_ns", "int64", true),
            debezium_primitive_schema("snapshot", "string", true),
            debezium_primitive_schema("db", "string", false),
            debezium_primitive_schema("sequence", "string", true),
            debezium_primitive_schema("schema", "string", false),
            debezium_primitive_schema("table", "string", false),
            debezium_primitive_schema("txId", "int64", true),
            debezium_primitive_schema("lsn", "int64", true),
            debezium_primitive_schema("xmin", "int64", true),
        ],
        "optional": false,
        "name": "io.debezium.connector.postgresql.Source",
    })
}

fn debezium_transaction_schema() -> Value {
    json!({
        "type": "struct",
        "fields": [
            debezium_primitive_schema("id", "string", false),
            debezium_primitive_schema("total_order", "int64", false),
            debezium_primitive_schema("data_collection_order", "int64", false),
        ],
        "optional": true,
        "name": "event.block",
        "version": 1,
    })
}

fn debezium_column_schema(name: &str, data_type: &ColumnType, optional: bool) -> Value {
    match data_type {
        ColumnType::Int64 => debezium_primitive_schema(name, "int64", optional),
        ColumnType::Bool => debezium_primitive_schema(name, "boolean", optional),
        ColumnType::Utf8 | ColumnType::Numeric | ColumnType::Decimal128 { .. } => {
            debezium_primitive_schema(name, "string", optional)
        }
        ColumnType::TimestampMillis => {
            let mut schema = debezium_primitive_schema(name, "int64", optional);
            if let Value::Object(object) = &mut schema {
                object.insert(
                    "name".to_string(),
                    Value::String("io.debezium.time.Timestamp".to_string()),
                );
                object.insert("version".to_string(), json!(1));
            }
            schema
        }
        ColumnType::DateDays => {
            let mut schema = debezium_primitive_schema(name, "int32", optional);
            if let Value::Object(object) = &mut schema {
                object.insert(
                    "name".to_string(),
                    Value::String("io.debezium.time.Date".to_string()),
                );
                object.insert("version".to_string(), json!(1));
            }
            schema
        }
    }
}

fn debezium_primitive_schema(field: &str, schema_type: &str, optional: bool) -> Value {
    json!({
        "type": schema_type,
        "optional": optional,
        "field": field,
    })
}

fn debezium_named_schema(mut schema: Value, field: &str, optional: bool) -> Value {
    if let Value::Object(object) = &mut schema {
        object.insert("field".to_string(), Value::String(field.to_string()));
        object.insert("optional".to_string(), Value::Bool(optional));
    }
    schema
}

fn postgres_lsn_to_u64(lsn: &str) -> Option<u64> {
    let (upper, lower) = lsn.split_once('/')?;
    let upper = u64::from_str_radix(upper, 16).ok()?;
    let lower = u64::from_str_radix(lower, 16).ok()?;
    Some((upper << 32) | lower)
}

fn transaction_id_to_debezium_txid(transaction_id: Option<&CdcTransactionId>) -> Option<Value> {
    let value = transaction_id?.as_str();
    let xid = value
        .strip_prefix("pg-xid-")
        .unwrap_or(value)
        .parse::<i64>()
        .ok()?;
    Some(json!(xid))
}

fn key_for_update(
    schema: &CdcTableSchema,
    explicit_key: Option<&CdcRowKey>,
    before: Option<&CdcRow>,
    after: &CdcRow,
) -> Result<CdcRowKey> {
    if let Some(key) = explicit_key {
        return Ok(key.clone());
    }
    if let Some(before) = before {
        return schema.primary_key_from_row(before);
    }
    schema.primary_key_from_row(after)
}

fn key_for_delete(
    schema: &CdcTableSchema,
    explicit_key: Option<&CdcRowKey>,
    before: Option<&CdcRow>,
) -> Result<CdcRowKey> {
    if let Some(key) = explicit_key {
        return Ok(key.clone());
    }
    let before = before.context("CDC delete requires a key or before row")?;
    schema.primary_key_from_row(before)
}

fn row_key_to_json(schema: &CdcTableSchema, key: &CdcRowKey) -> Result<Value> {
    key.validate_against_schema(schema)?;
    let mut object = Map::new();
    for (column, value) in schema.primary_key().columns().iter().zip(key.values()) {
        let column_definition = schema
            .columns()
            .iter()
            .find(|definition| definition.name() == column)
            .ok_or_else(|| {
                anyhow::anyhow!("CDC primary-key column '{column}' missing from schema")
            })?;
        object.insert(
            column.clone(),
            row_value_to_json(value, column_definition.data_type())?,
        );
    }
    Ok(Value::Object(object))
}

fn row_to_json(schema: &CdcTableSchema, row: &CdcRow) -> Result<Value> {
    schema.validate_row(row)?;
    let mut object = Map::new();
    for (column, value) in schema.columns().iter().zip(row.values()) {
        let value = match value {
            Some(value) => row_value_to_json(value, column.data_type())?,
            None => Value::Null,
        };
        object.insert(column.name().to_string(), value);
    }
    Ok(Value::Object(object))
}

fn row_value_to_json(
    value: &RowValue,
    data_type: &floe_core::catalog::ColumnType,
) -> Result<Value> {
    match value {
        RowValue::Int64(value) => Ok(json!(*value)),
        RowValue::Bool(value) => Ok(json!(*value)),
        RowValue::Utf8(value) => Ok(Value::String(value.clone())),
        RowValue::TimestampMillis(value) => Ok(json!(*value)),
        RowValue::DateDays(value) => Ok(json!(*value)),
        RowValue::Decimal128(value) => match data_type {
            floe_core::catalog::ColumnType::Decimal128 { scale, .. } => {
                Ok(Value::String(format_decimal128(*value, *scale)?))
            }
            _ => Ok(Value::String(value.to_string())),
        },
        RowValue::Numeric(value) => Ok(Value::String(value.clone())),
    }
}

fn current_unix_time_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests;
