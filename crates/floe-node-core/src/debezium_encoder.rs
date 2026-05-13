use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use floe_cdc_core::{
    CdcChange, CdcRow, CdcRowKey, CdcSourcePosition, CdcTableSchema, CdcTransactionId, ChangeBatch,
};
use floe_core::RowValue;
use serde_json::{Map, Value, json};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DebeziumEnvelopeConfig {
    source_name: String,
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
            source_name,
            emit_tombstones: false,
            include_transaction_metadata: false,
        })
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
    let source = source_metadata(schema, config, context.source_position, false);
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

    let key_json = key
        .as_ref()
        .map(|key| row_key_to_json(schema, key))
        .transpose()?;
    let value = envelope(before, after, source, op, ts_ms, config, context);
    let mut records = vec![DebeziumEncodedRecord::new(key_json.clone(), Some(value))];
    if matches!(change, CdcChange::Delete { .. }) && config.emit_tombstones {
        records.push(DebeziumEncodedRecord::new(key_json, None));
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
        Some(row_key_to_json(schema, &key)?),
        Some(envelope(
            Value::Null,
            row_to_json(schema, row)?,
            source_metadata(schema, config, context.source_position, true),
            "r",
            ts_ms,
            config,
            context,
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
    position: Option<&CdcSourcePosition>,
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
    source.insert(
        "schema".to_string(),
        Value::String(schema.upstream_table().schema().to_string()),
    );
    source.insert(
        "table".to_string(),
        Value::String(schema.upstream_table().table().to_string()),
    );
    source.insert(
        "snapshot".to_string(),
        Value::String(if snapshot { "true" } else { "false" }.to_string()),
    );
    if let Some(position) = position {
        match position {
            CdcSourcePosition::Postgres {
                commit_lsn,
                event_lsn,
            } => {
                source.insert("lsn".to_string(), Value::String(commit_lsn.clone()));
                source.insert("commit_lsn".to_string(), Value::String(commit_lsn.clone()));
                if let Some(event_lsn) = event_lsn {
                    source.insert("event_lsn".to_string(), Value::String(event_lsn.clone()));
                }
            }
            CdcSourcePosition::Opaque { value } => {
                source.insert("position".to_string(), Value::String(value.clone()));
            }
        }
    }
    Value::Object(source)
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
        object.insert(column.clone(), row_value_to_json(value));
    }
    Ok(Value::Object(object))
}

fn row_to_json(schema: &CdcTableSchema, row: &CdcRow) -> Result<Value> {
    schema.validate_row(row)?;
    let mut object = Map::new();
    for (column, value) in schema.columns().iter().zip(row.values()) {
        object.insert(
            column.name().to_string(),
            value.as_ref().map(row_value_to_json).unwrap_or(Value::Null),
        );
    }
    Ok(Value::Object(object))
}

fn row_value_to_json(value: &RowValue) -> Value {
    match value {
        RowValue::Int64(value) => json!(*value),
        RowValue::Bool(value) => json!(*value),
        RowValue::Utf8(value) => Value::String(value.clone()),
        RowValue::TimestampMillis(value) => json!(*value),
        RowValue::DateDays(value) => json!(*value),
        RowValue::Numeric(value) => Value::String(value.clone()),
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
mod tests {
    use super::*;
    use floe_cdc_core::{CdcColumn, CdcPrimaryKey, CdcTableId, ChangeBatch, UpstreamTableRef};
    use floe_core::catalog::ColumnType;

    #[test]
    fn encodes_insert_with_composite_key_and_transaction_metadata() {
        let schema = orders_schema();
        let config = DebeziumEnvelopeConfig::new("pg_main")
            .unwrap()
            .with_transaction_metadata(true);
        let tx = CdcTransactionId::new("tx-7").unwrap();
        let position =
            CdcSourcePosition::postgres("0/16B6C50", Some("0/16B6C40".to_string())).unwrap();
        let records = encode_debezium_change(
            &schema,
            &CdcChange::Insert {
                row: row(7, 42, 99, Some("new")),
            },
            &config,
            DebeziumEncodeContext {
                source_position: Some(&position),
                transaction_id: Some(&tx),
                sequence: Some(3),
                ts_ms: Some(1234),
            },
        )
        .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key(), Some(&json!({"tenant_id": 7, "id": 42})));
        let value = records[0].value().unwrap();
        assert_eq!(value["op"], "c");
        assert_eq!(value["ts_ms"], 1234);
        assert_eq!(value["before"], Value::Null);
        assert_eq!(value["after"]["amount"], 99);
        assert_eq!(value["source"]["name"], "pg_main");
        assert_eq!(value["source"]["schema"], "public");
        assert_eq!(value["source"]["table"], "orders");
        assert_eq!(value["source"]["commit_lsn"], "0/16B6C50");
        assert_eq!(value["source"]["event_lsn"], "0/16B6C40");
        assert_eq!(value["transaction"]["id"], "tx-7");
        assert_eq!(value["transaction"]["total_order"], 3);
    }

    #[test]
    fn encodes_update_before_after_images() {
        let schema = orders_schema();
        let config = DebeziumEnvelopeConfig::new("pg_main").unwrap();
        let records = encode_debezium_change(
            &schema,
            &CdcChange::Update {
                key: None,
                before: Some(row(7, 42, 99, Some("new"))),
                after: row(7, 42, 110, Some("paid")),
            },
            &config,
            DebeziumEncodeContext {
                ts_ms: Some(1234),
                ..Default::default()
            },
        )
        .unwrap();

        let value = records[0].value().unwrap();
        assert_eq!(records[0].key(), Some(&json!({"tenant_id": 7, "id": 42})));
        assert_eq!(value["op"], "u");
        assert_eq!(value["before"]["status"], "new");
        assert_eq!(value["after"]["status"], "paid");
    }

    #[test]
    fn encodes_delete_and_optional_tombstone() {
        let schema = orders_schema();
        let config = DebeziumEnvelopeConfig::new("pg_main")
            .unwrap()
            .with_emit_tombstones(true);
        let records = encode_debezium_change(
            &schema,
            &CdcChange::Delete {
                key: Some(CdcRowKey::new([RowValue::Int64(7), RowValue::Int64(42)]).unwrap()),
                before: Some(row(7, 42, 99, Some("new"))),
            },
            &config,
            DebeziumEncodeContext {
                ts_ms: Some(1234),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key(), Some(&json!({"tenant_id": 7, "id": 42})));
        assert_eq!(records[0].value().unwrap()["op"], "d");
        assert_eq!(records[0].value().unwrap()["before"]["amount"], 99);
        assert_eq!(records[0].value().unwrap()["after"], Value::Null);
        assert_eq!(records[1].key(), Some(&json!({"tenant_id": 7, "id": 42})));
        assert_eq!(records[1].value(), None);
    }

    #[test]
    fn encodes_snapshot_and_truncate_operations() {
        let schema = orders_schema();
        let config = DebeziumEnvelopeConfig::new("pg_main").unwrap();
        let snapshot = encode_debezium_snapshot_row(
            &schema,
            &row(7, 42, 99, None),
            &config,
            DebeziumEncodeContext {
                ts_ms: Some(1234),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(snapshot.key(), Some(&json!({"tenant_id": 7, "id": 42})));
        assert_eq!(snapshot.value().unwrap()["op"], "r");
        assert_eq!(snapshot.value().unwrap()["source"]["snapshot"], "true");

        let truncate = encode_debezium_change(
            &schema,
            &CdcChange::Truncate,
            &config,
            DebeziumEncodeContext {
                ts_ms: Some(1234),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(truncate[0].key(), None);
        assert_eq!(truncate[0].value().unwrap()["op"], "t");
    }

    #[test]
    fn encodes_change_batch_with_incrementing_sequence() {
        let schema = orders_schema();
        let config = DebeziumEnvelopeConfig::new("pg_main")
            .unwrap()
            .with_transaction_metadata(true);
        let tx = CdcTransactionId::new("tx-7").unwrap();
        let batch = ChangeBatch::new(
            schema.table_id().clone(),
            vec![
                CdcChange::Insert {
                    row: row(7, 42, 99, None),
                },
                CdcChange::Insert {
                    row: row(7, 43, 100, None),
                },
            ],
        )
        .unwrap();
        let records = encode_debezium_change_batch(
            &schema,
            &batch,
            &config,
            DebeziumEncodeContext {
                transaction_id: Some(&tx),
                sequence: Some(10),
                ts_ms: Some(1234),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0].value().unwrap()["transaction"]["total_order"],
            10
        );
        assert_eq!(
            records[1].value().unwrap()["transaction"]["total_order"],
            11
        );
    }

    fn orders_schema() -> CdcTableSchema {
        CdcTableSchema::new(
            CdcTableId::new("orders").unwrap(),
            UpstreamTableRef::new("public", "orders").unwrap(),
            vec![
                CdcColumn::new("tenant_id", ColumnType::Int64, false).unwrap(),
                CdcColumn::new("id", ColumnType::Int64, false).unwrap(),
                CdcColumn::new("amount", ColumnType::Int64, false).unwrap(),
                CdcColumn::new("status", ColumnType::Utf8, true).unwrap(),
            ],
            CdcPrimaryKey::new(["tenant_id", "id"]).unwrap(),
        )
        .unwrap()
    }

    fn row(tenant_id: i64, id: i64, amount: i64, status: Option<&str>) -> CdcRow {
        CdcRow::new([
            Some(RowValue::Int64(tenant_id)),
            Some(RowValue::Int64(id)),
            Some(RowValue::Int64(amount)),
            status.map(|status| RowValue::Utf8(status.to_string())),
        ])
        .unwrap()
    }
}
