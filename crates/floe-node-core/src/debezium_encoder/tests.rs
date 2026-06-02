use super::*;
use floe_cdc_core::{CdcColumn, CdcPrimaryKey, CdcTableId, ChangeBatch, UpstreamTableRef};
use floe_core::catalog::ColumnType;

#[test]
fn encodes_insert_with_composite_key_and_transaction_metadata() {
    let schema = orders_schema();
    let config = DebeziumEnvelopeConfig::new("pg_main")
        .unwrap()
        .with_database_name("inventory")
        .with_transaction_metadata(true);
    let tx = CdcTransactionId::new("pg-xid-7").unwrap();
    let position = CdcSourcePosition::postgres("0/16B6C50", Some("0/16B6C40".to_string())).unwrap();
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
    assert_eq!(key_payload(&records[0]), &json!({"tenant_id": 7, "id": 42}));
    assert_eq!(
        records[0].key().unwrap()["schema"]["name"],
        "pg_main.public.orders.Key"
    );
    let value = value_payload(&records[0]);
    assert_eq!(value["op"], "c");
    assert_eq!(value["ts_ms"], 1234);
    assert_eq!(value["ts_us"], 1_234_000);
    assert_eq!(value["ts_ns"], 1_234_000_000);
    assert_eq!(value["before"], Value::Null);
    assert_eq!(value["after"]["amount"], 99);
    assert_eq!(value["source"]["name"], "pg_main");
    assert_eq!(value["source"]["db"], "inventory");
    assert_eq!(value["source"]["schema"], "public");
    assert_eq!(value["source"]["table"], "orders");
    assert_eq!(value["source"]["lsn"], 23_817_280);
    assert_eq!(value["source"]["txId"], 7);
    assert_eq!(value["source"]["xmin"], Value::Null);
    assert_eq!(value["transaction"]["id"], "pg-xid-7");
    assert_eq!(value["transaction"]["total_order"], 3);
    assert_eq!(
        records[0].value().unwrap()["schema"]["name"],
        "pg_main.public.orders.Envelope"
    );
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

    let value = value_payload(&records[0]);
    assert_eq!(key_payload(&records[0]), &json!({"tenant_id": 7, "id": 42}));
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
    assert_eq!(key_payload(&records[0]), &json!({"tenant_id": 7, "id": 42}));
    assert_eq!(value_payload(&records[0])["op"], "d");
    assert_eq!(value_payload(&records[0])["before"]["amount"], 99);
    assert_eq!(value_payload(&records[0])["after"], Value::Null);
    assert_eq!(key_payload(&records[1]), &json!({"tenant_id": 7, "id": 42}));
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
    assert_eq!(key_payload(&snapshot), &json!({"tenant_id": 7, "id": 42}));
    assert_eq!(value_payload(&snapshot)["op"], "r");
    assert_eq!(value_payload(&snapshot)["source"]["snapshot"], "true");

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
    assert_eq!(value_payload(&truncate[0])["op"], "t");
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
    assert_eq!(value_payload(&records[0])["transaction"]["total_order"], 10);
    assert_eq!(value_payload(&records[1])["transaction"]["total_order"], 11);
}

#[test]
fn change_batch_encoder_can_emit_insert_changes_as_snapshot_reads() {
    let schema = orders_schema();
    let config = DebeziumEnvelopeConfig::new("pg_main")
        .unwrap()
        .with_database_name("postgres");
    let batch = ChangeBatch::new(
        schema.table_id().clone(),
        vec![CdcChange::Insert {
            row: row(7, 42, 99, Some("open")),
        }],
    )
    .unwrap();
    let records = encode_debezium_change_batch_with_options(
        &schema,
        &batch,
        &config,
        DebeziumEncodeContext::default(),
        DebeziumBatchEncodeOptions {
            snapshot_read: true,
        },
    )
    .unwrap();

    assert_eq!(records.len(), 1);
    assert_eq!(value_payload(&records[0])["op"], "r");
    assert_eq!(value_payload(&records[0])["source"]["snapshot"], "true");
}

#[test]
fn connect_schema_carries_debezium_field_shapes() {
    let schema = orders_schema();
    let config = DebeziumEnvelopeConfig::new("pg_main")
        .unwrap()
        .with_database_name("inventory")
        .with_transaction_metadata(true);
    let record = encode_debezium_snapshot_row(
        &schema,
        &row(7, 42, 99, Some("open")),
        &config,
        DebeziumEncodeContext {
            ts_ms: Some(1234),
            ..Default::default()
        },
    )
    .unwrap();

    let key = record.key().unwrap();
    assert_eq!(key["schema"]["name"], "pg_main.public.orders.Key");
    assert_eq!(key["schema"]["fields"][0]["field"], "tenant_id");
    assert_eq!(key["payload"], json!({"tenant_id": 7, "id": 42}));

    let value = record.value().unwrap();
    assert_eq!(value["schema"]["name"], "pg_main.public.orders.Envelope");
    assert_eq!(value["schema"]["fields"][0]["field"], "before");
    assert_eq!(
        value["schema"]["fields"][2]["name"],
        "io.debezium.connector.postgresql.Source"
    );
    assert_eq!(value["schema"]["fields"][3]["field"], "op");
    assert_eq!(value["schema"]["fields"][7]["field"], "transaction");
    assert_eq!(value["payload"]["source"]["db"], "inventory");
    assert_eq!(value["payload"]["after"]["status"], "open");
}

#[test]
fn envelope_payload_exposes_debezium_compatibility_fields() {
    let schema = orders_schema();
    let config = DebeziumEnvelopeConfig::new("pg_main")
        .unwrap()
        .with_database_name("inventory")
        .with_transaction_metadata(true);
    let tx = CdcTransactionId::new("pg-xid-9").unwrap();
    let position = CdcSourcePosition::postgres("0/16B6D00", Some("0/16B6C40".to_string())).unwrap();
    let records = encode_debezium_change(
        &schema,
        &CdcChange::Update {
            key: None,
            before: Some(row(7, 42, 99, Some("open"))),
            after: row(7, 42, 125, Some("paid")),
        },
        &config,
        DebeziumEncodeContext {
            source_position: Some(&position),
            transaction_id: Some(&tx),
            sequence: Some(4),
            ts_ms: Some(1234),
        },
    )
    .unwrap();

    let value = records[0].value().unwrap();
    let schema_fields = value["schema"]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|field| field["field"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        schema_fields,
        vec![
            "before",
            "after",
            "source",
            "op",
            "ts_ms",
            "ts_us",
            "ts_ns",
            "transaction",
        ]
    );

    let payload = value_payload(&records[0]);
    for field in [
        "before",
        "after",
        "source",
        "op",
        "ts_ms",
        "ts_us",
        "ts_ns",
        "transaction",
    ] {
        assert!(
            payload.get(field).is_some(),
            "missing payload field {field}"
        );
    }
    for source_field in [
        "version",
        "connector",
        "name",
        "ts_ms",
        "ts_us",
        "ts_ns",
        "snapshot",
        "db",
        "sequence",
        "schema",
        "table",
        "txId",
        "lsn",
        "xmin",
    ] {
        assert!(
            payload["source"].get(source_field).is_some(),
            "missing source field {source_field}"
        );
    }
    assert_eq!(payload["op"], "u");
    assert_eq!(payload["before"]["status"], "open");
    assert_eq!(payload["after"]["status"], "paid");
    assert_eq!(payload["transaction"]["id"], "pg-xid-9");
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

fn key_payload(record: &DebeziumEncodedRecord) -> &Value {
    &record.key().expect("key")["payload"]
}

fn value_payload(record: &DebeziumEncodedRecord) -> &Value {
    &record.value().expect("value")["payload"]
}
