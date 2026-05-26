use super::encoding::{
    encode_debezium_pipeline_records, encode_pipeline_buffer_records,
    encode_pipeline_transaction_records, encode_pipeline_transaction_records_with_metadata,
};
use super::target_state::{delivered_target_state, failed_target_state};
use super::writers::{
    PostgresParamValue, PostgresReplicationPipelineWriter, parse_floe_json_record_key,
    parse_floe_json_record_value, postgres_key_params_from_json, postgres_row_params_from_json,
};
use super::*;
use floe_cdc_core::{
    CdcChange, CdcColumn, CdcColumnarColumn, CdcColumnarRowBatch, CdcPrimaryKey, CdcRow, CdcRowKey,
    CdcTransactionId, ChangeBatch, UpstreamTableRef,
};
use floe_config::ReplicationArrowIpcCompressionConfig;
use floe_core::RowValue;
use floe_core::catalog::ColumnType;
use std::collections::BTreeMap;

#[test]
fn materialized_transaction_filters_non_materialized_batches() {
    let source_id = CdcSourceId::new("pg_main").unwrap();
    let materialized = CdcTableId::new("orders").unwrap();
    let passthrough = CdcTableId::new("pg_main:public.customers").unwrap();
    let transaction = TransactionBatch::new(
        source_id.clone(),
        None,
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![
            ChangeBatch::new(
                materialized.clone(),
                vec![CdcChange::Insert {
                    row: row(1, "open"),
                }],
            )
            .unwrap(),
            ChangeBatch::new(passthrough, vec![CdcChange::Insert { row: row(2, "new") }]).unwrap(),
        ],
    )
    .unwrap();

    let filtered = materialized_transaction(
        &source_id,
        &HashSet::from([materialized.clone()]),
        &transaction,
    )
    .unwrap()
    .unwrap();

    assert_eq!(filtered.change_batches().len(), 1);
    assert_eq!(filtered.change_batches()[0].table_id(), &materialized);
}

#[test]
fn pipeline_snapshot_records_use_read_operation() {
    let plan = ReplicationPipelineRuntimePlan {
        name: "p".to_string(),
        source_name: "pg_main".to_string(),
        database_name: "postgres".to_string(),
        upstream_table: "public.orders".to_string(),
        table_id: CdcTableId::new("orders").unwrap(),
        schema: schema(CdcTableId::new("orders").unwrap()),
        schema_evolution_policy: PostgresSchemaEvolutionPolicy::FailFast,
        target: ReplicationPipelineRuntimeTarget::Kafka {
            brokers: "localhost:9092".to_string(),
            topic: "orders".to_string(),
        },
        format: ReplicationPipelineRuntimeFormat::DebeziumJson,
        buffer_mode: ReplicationPipelineRuntimeBufferMode::Durable,
        buffer_policy: CatalogReplicationBufferPolicy::default(),
        error_policy: CatalogReplicationErrorPolicy::default(),
        emit_tombstones: false,
        include_transaction_metadata: false,
    };
    let schema = schema(plan.table_id.clone());
    let batch = ChangeBatch::new(
        plan.table_id.clone(),
        vec![CdcChange::Insert {
            row: row(1, "open"),
        }],
    )
    .unwrap();
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("snapshot:0/16B6C50").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![batch.clone()],
    )
    .unwrap();

    let records = encode_debezium_pipeline_records(&plan, &schema, &batch, &transaction).unwrap();
    assert_eq!(records.len(), 1);
    let payload = &records[0].value().unwrap()["payload"];
    assert_eq!(payload["op"], "r");
    assert_eq!(payload["source"]["snapshot"], "true");
    assert_eq!(payload["source"]["db"], "postgres");
}

#[test]
fn pipeline_debezium_records_are_buffered_as_encoded_kafka_payloads() {
    let plan = ReplicationPipelineRuntimePlan {
        name: "p".to_string(),
        source_name: "pg_main".to_string(),
        database_name: "postgres".to_string(),
        upstream_table: "public.orders".to_string(),
        table_id: CdcTableId::new("orders").unwrap(),
        schema: schema(CdcTableId::new("orders").unwrap()),
        schema_evolution_policy: PostgresSchemaEvolutionPolicy::FailFast,
        target: ReplicationPipelineRuntimeTarget::Kafka {
            brokers: "localhost:9092".to_string(),
            topic: "orders".to_string(),
        },
        format: ReplicationPipelineRuntimeFormat::DebeziumJson,
        buffer_mode: ReplicationPipelineRuntimeBufferMode::Durable,
        buffer_policy: CatalogReplicationBufferPolicy::default(),
        error_policy: CatalogReplicationErrorPolicy::default(),
        emit_tombstones: false,
        include_transaction_metadata: true,
    };
    let schema = schema(plan.table_id.clone());
    let batch = ChangeBatch::new(
        plan.table_id.clone(),
        vec![CdcChange::Insert {
            row: row(1, "open"),
        }],
    )
    .unwrap();
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("pg-xid-55").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![batch.clone()],
    )
    .unwrap();

    let records = encode_pipeline_buffer_records(&plan, &schema, &batch, &transaction)
        .expect("encode debezium records");
    let value: serde_json::Value =
        serde_json::from_slice(records[0].value().expect("value")).unwrap();
    assert_eq!(value["schema"]["name"], "pg_main.public.orders.Envelope");
    assert_eq!(value["payload"]["source"]["txId"], 55);

    let prepared = prepare_replication_buffer_append(&plan, &transaction, records.clone()).unwrap();
    assert_eq!(
        prepared.append.payload_format(),
        CdcBufferPayloadFormat::KafkaRecords
    );
    assert_eq!(prepared.append.records(), records.as_slice());
    assert!(prepared.target_records.is_none());
}

#[test]
fn pipeline_floe_json_records_encode_compact_row_messages() {
    let plan = ReplicationPipelineRuntimePlan {
        name: "p".to_string(),
        source_name: "pg_main".to_string(),
        database_name: "postgres".to_string(),
        upstream_table: "public.orders".to_string(),
        table_id: CdcTableId::new("orders").unwrap(),
        schema: schema(CdcTableId::new("orders").unwrap()),
        schema_evolution_policy: PostgresSchemaEvolutionPolicy::FailFast,
        target: ReplicationPipelineRuntimeTarget::Kafka {
            brokers: "localhost:9092".to_string(),
            topic: "orders".to_string(),
        },
        format: ReplicationPipelineRuntimeFormat::FloeJson,
        buffer_mode: ReplicationPipelineRuntimeBufferMode::Durable,
        buffer_policy: CatalogReplicationBufferPolicy::default(),
        error_policy: CatalogReplicationErrorPolicy::default(),
        emit_tombstones: false,
        include_transaction_metadata: false,
    };
    let schema = schema(plan.table_id.clone());
    let batch = ChangeBatch::new(
        plan.table_id.clone(),
        vec![
            CdcChange::Insert {
                row: row(1, "open"),
            },
            CdcChange::Delete {
                key: Some(CdcRowKey::new([RowValue::Int64(2)]).unwrap()),
                before: None,
            },
        ],
    )
    .unwrap();
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("tx-1").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![batch.clone()],
    )
    .unwrap();

    let records = encode_pipeline_buffer_records(&plan, &schema, &batch, &transaction)
        .expect("encode floe json records");
    assert_eq!(records.len(), 2);
    let first_key: serde_json::Value =
        serde_json::from_slice(records[0].key().expect("key")).unwrap();
    let first_value: serde_json::Value =
        serde_json::from_slice(records[0].value().expect("value")).unwrap();
    let delete_value: serde_json::Value =
        serde_json::from_slice(records[1].value().expect("delete value")).unwrap();

    assert_eq!(first_key, serde_json::json!({"id": 1}));
    assert_eq!(first_value["id"], 1);
    assert_eq!(first_value["status"], "open");
    assert_eq!(first_value[FLOE_JSON_DELETED_FIELD], false);
    assert_eq!(first_value[FLOE_JSON_VERSION_FIELD], FLOE_JSON_VERSION);
    assert_eq!(delete_value["id"], 2);
    assert_eq!(delete_value[FLOE_JSON_DELETED_FIELD], true);
    assert_eq!(delete_value[FLOE_JSON_VERSION_FIELD], FLOE_JSON_VERSION);
}

#[test]
fn postgres_target_writer_builds_upsert_and_delete_sql() {
    let schema = CdcTableSchema::new(
        CdcTableId::new("orders").unwrap(),
        UpstreamTableRef::new("public", "orders").unwrap(),
        vec![
            CdcColumn::new("id", ColumnType::Int64, false).unwrap(),
            CdcColumn::new("status", ColumnType::Utf8, true).unwrap(),
            CdcColumn::new("order_date", ColumnType::DateDays, true).unwrap(),
            CdcColumn::new(
                "amount",
                ColumnType::decimal128(12, 2).expect("decimal type"),
                true,
            )
            .unwrap(),
        ],
        CdcPrimaryKey::new(["id"]).unwrap(),
    )
    .unwrap();
    let writer = PostgresReplicationPipelineWriter::new(
        "postgres://postgres:postgres@localhost/postgres",
        "public.orders_copy",
        schema,
    )
    .expect("writer");

    assert_eq!(
        writer.insert_sql,
        "INSERT INTO \"public\".\"orders_copy\" (\"id\", \"status\", \"order_date\", \"amount\") VALUES ($1, $2, DATE '1970-01-01' + $3::integer, $4::numeric) ON CONFLICT (\"id\") DO UPDATE SET \"status\" = EXCLUDED.\"status\", \"order_date\" = EXCLUDED.\"order_date\", \"amount\" = EXCLUDED.\"amount\""
    );
    assert_eq!(
        writer.delete_sql,
        "DELETE FROM \"public\".\"orders_copy\" WHERE \"id\" = $1"
    );
}

#[test]
fn postgres_target_params_decode_floe_json_records() {
    let schema = CdcTableSchema::new(
        CdcTableId::new("orders").unwrap(),
        UpstreamTableRef::new("public", "orders").unwrap(),
        vec![
            CdcColumn::new("id", ColumnType::Int64, false).unwrap(),
            CdcColumn::new("status", ColumnType::Utf8, true).unwrap(),
            CdcColumn::new("order_date", ColumnType::DateDays, true).unwrap(),
            CdcColumn::new(
                "amount",
                ColumnType::decimal128(12, 2).expect("decimal type"),
                true,
            )
            .unwrap(),
        ],
        CdcPrimaryKey::new(["id"]).unwrap(),
    )
    .unwrap();
    let record = CdcBufferRecord::new(
        Some(br#"{"id":7}"#.to_vec()),
        Some(
            br#"{"id":7,"status":"open","order_date":19358,"amount":"123.45","__floe_deleted":false,"__floe_version":1}"#
                .to_vec(),
        ),
    );
    let value = parse_floe_json_record_value(&record).expect("value");
    let key = parse_floe_json_record_key(&record).expect("key");

    assert_eq!(
        postgres_row_params_from_json(&schema, &value).expect("row params"),
        vec![
            PostgresParamValue::Int64(Some(7)),
            PostgresParamValue::Text(Some("open".to_string())),
            PostgresParamValue::Int32(Some(19358)),
            PostgresParamValue::Text(Some("123.45".to_string())),
        ]
    );
    assert_eq!(
        postgres_key_params_from_json(&schema, &key).expect("key params"),
        vec![PostgresParamValue::Int64(Some(7))]
    );
}

#[test]
fn pipeline_arrow_ipc_records_encode_batches_without_json() {
    let plan = ReplicationPipelineRuntimePlan {
        name: "p".to_string(),
        source_name: "pg_main".to_string(),
        database_name: "postgres".to_string(),
        upstream_table: "public.orders".to_string(),
        table_id: CdcTableId::new("orders").unwrap(),
        schema: schema(CdcTableId::new("orders").unwrap()),
        schema_evolution_policy: PostgresSchemaEvolutionPolicy::FailFast,
        target: ReplicationPipelineRuntimeTarget::Kafka {
            brokers: "localhost:9092".to_string(),
            topic: "orders".to_string(),
        },
        format: ReplicationPipelineRuntimeFormat::ArrowIpc,
        buffer_mode: ReplicationPipelineRuntimeBufferMode::Durable,
        buffer_policy: CatalogReplicationBufferPolicy::default(),
        error_policy: CatalogReplicationErrorPolicy::default(),
        emit_tombstones: false,
        include_transaction_metadata: false,
    };
    let schema = schema(plan.table_id.clone());
    let batch = ChangeBatch::new(
        plan.table_id.clone(),
        vec![
            CdcChange::Insert {
                row: row(1, "open"),
            },
            CdcChange::Update {
                key: None,
                before: Some(row(1, "open")),
                after: row(1, "paid"),
            },
        ],
    )
    .unwrap();
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("tx-1").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![batch.clone()],
    )
    .unwrap();

    let records = encode_pipeline_buffer_records(&plan, &schema, &batch, &transaction)
        .expect("encode arrow records");
    assert_eq!(records.len(), 1);
    let payload = records[0].value().expect("payload");
    assert!(!payload.starts_with(b"{"));

    let mut reader = arrow_ipc::reader::StreamReader::try_new(payload, None).expect("arrow reader");
    let batch = reader
        .next()
        .expect("one record batch")
        .expect("decode batch");
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.schema().field(0).name(), "id");
    assert_eq!(batch.schema().field(2).name(), "__op");
}

#[test]
fn pipeline_arrow_ipc_records_encode_columnar_snapshot_without_json() {
    let plan = ReplicationPipelineRuntimePlan {
        name: "p".to_string(),
        source_name: "pg_main".to_string(),
        database_name: "postgres".to_string(),
        upstream_table: "public.orders".to_string(),
        table_id: CdcTableId::new("orders").unwrap(),
        schema: schema(CdcTableId::new("orders").unwrap()),
        schema_evolution_policy: PostgresSchemaEvolutionPolicy::FailFast,
        target: ReplicationPipelineRuntimeTarget::Kafka {
            brokers: "localhost:9092".to_string(),
            topic: "orders".to_string(),
        },
        format: ReplicationPipelineRuntimeFormat::ArrowIpc,
        buffer_mode: ReplicationPipelineRuntimeBufferMode::Durable,
        buffer_policy: CatalogReplicationBufferPolicy::default(),
        error_policy: CatalogReplicationErrorPolicy::default(),
        emit_tombstones: false,
        include_transaction_metadata: false,
    };
    let schema = schema(plan.table_id.clone());
    let rows = CdcColumnarRowBatch::new(vec![
        CdcColumnarColumn::Int64(vec![Some(1), Some(2)]),
        CdcColumnarColumn::Utf8(vec![Some("open".to_string()), Some("paid".to_string())]),
    ])
    .unwrap();
    let batch =
        ChangeBatch::new_snapshot_insert(plan.table_id.clone(), rows).expect("snapshot batch");
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("snapshot:0/16B6C50").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![batch.clone()],
    )
    .unwrap();

    let records = encode_pipeline_buffer_records(&plan, &schema, &batch, &transaction)
        .expect("encode arrow snapshot records");
    assert_eq!(records.len(), 1);
    let payload = records[0].value().expect("payload");
    assert!(!payload.starts_with(b"{"));

    let mut reader = arrow_ipc::reader::StreamReader::try_new(payload, None).expect("arrow reader");
    let batch = reader
        .next()
        .expect("one record batch")
        .expect("decode batch");
    assert_eq!(batch.num_rows(), 2);
    let ops = batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("op column");
    assert_eq!(ops.value(0), "r");
    assert_eq!(ops.value(1), "r");
}

#[test]
fn pipeline_transaction_records_include_all_matching_snapshot_chunks() {
    let plan = ReplicationPipelineRuntimePlan {
        name: "p".to_string(),
        source_name: "pg_main".to_string(),
        database_name: "postgres".to_string(),
        upstream_table: "public.orders".to_string(),
        table_id: CdcTableId::new("orders").unwrap(),
        schema: schema(CdcTableId::new("orders").unwrap()),
        schema_evolution_policy: PostgresSchemaEvolutionPolicy::FailFast,
        target: ReplicationPipelineRuntimeTarget::Kafka {
            brokers: "localhost:9092".to_string(),
            topic: "orders".to_string(),
        },
        format: ReplicationPipelineRuntimeFormat::ArrowIpc,
        buffer_mode: ReplicationPipelineRuntimeBufferMode::Durable,
        buffer_policy: CatalogReplicationBufferPolicy::default(),
        error_policy: CatalogReplicationErrorPolicy::default(),
        emit_tombstones: false,
        include_transaction_metadata: false,
    };
    let schema = schema(plan.table_id.clone());
    let first_rows = CdcColumnarRowBatch::new(vec![
        CdcColumnarColumn::Int64(vec![Some(1)]),
        CdcColumnarColumn::Utf8(vec![Some("open".to_string())]),
    ])
    .unwrap();
    let second_rows = CdcColumnarRowBatch::new(vec![
        CdcColumnarColumn::Int64(vec![Some(2), Some(3)]),
        CdcColumnarColumn::Utf8(vec![Some("paid".to_string()), Some("void".to_string())]),
    ])
    .unwrap();
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("snapshot:0/16B6C50").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![
            ChangeBatch::new_snapshot_insert(plan.table_id.clone(), first_rows)
                .expect("first snapshot batch"),
            ChangeBatch::new_snapshot_insert(plan.table_id.clone(), second_rows)
                .expect("second snapshot batch"),
        ],
    )
    .unwrap();
    let schemas = HashMap::from([(plan.table_id.clone(), schema)]);

    let records = encode_pipeline_transaction_records(&plan, &schemas, &transaction)
        .expect("encode transaction records");

    assert_eq!(records.len(), 2);
    let decoded_rows = records
        .iter()
        .map(|record| {
            let payload = record.value().expect("payload");
            let mut reader =
                arrow_ipc::reader::StreamReader::try_new(payload, None).expect("arrow reader");
            reader
                .next()
                .expect("one record batch")
                .expect("decode batch")
                .num_rows()
        })
        .collect::<Vec<_>>();
    assert_eq!(decoded_rows, vec![1, 2]);
}

#[test]
fn pipeline_transaction_records_filter_multi_table_transactions_per_target() {
    let orders_id = CdcTableId::new("orders").unwrap();
    let customers_id = CdcTableId::new("customers").unwrap();
    let orders_plan = test_plan("orders_pipe", orders_id.clone(), "public.orders");
    let customers_plan = test_plan("customers_pipe", customers_id.clone(), "public.customers");
    let schemas = HashMap::from([
        (
            orders_id.clone(),
            schema_for_table(orders_id.clone(), "orders"),
        ),
        (
            customers_id.clone(),
            schema_for_table(customers_id.clone(), "customers"),
        ),
    ]);
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("pg-xid-77").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![
            ChangeBatch::new(
                orders_id.clone(),
                vec![CdcChange::Insert {
                    row: row(1, "open"),
                }],
            )
            .unwrap(),
            ChangeBatch::new(
                customers_id.clone(),
                vec![CdcChange::Insert {
                    row: row(9, "active"),
                }],
            )
            .unwrap(),
            ChangeBatch::new(
                orders_id.clone(),
                vec![CdcChange::Insert {
                    row: row(2, "paid"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();

    let orders_records =
        encode_pipeline_transaction_records(&orders_plan, &schemas, &transaction).unwrap();
    let customers_records =
        encode_pipeline_transaction_records(&customers_plan, &schemas, &transaction).unwrap();

    assert_eq!(orders_records.len(), 2);
    assert_eq!(customers_records.len(), 1);
    let first_order: serde_json::Value =
        serde_json::from_slice(orders_records[0].value().expect("first order")).unwrap();
    let second_order: serde_json::Value =
        serde_json::from_slice(orders_records[1].value().expect("second order")).unwrap();
    let customer: serde_json::Value =
        serde_json::from_slice(customers_records[0].value().expect("customer")).unwrap();
    assert_eq!(first_order["id"], 1);
    assert_eq!(second_order["id"], 2);
    assert_eq!(customer["id"], 9);
}

#[test]
fn pipeline_transaction_records_omit_metadata_headers_by_default() {
    let table_id = CdcTableId::new("orders").unwrap();
    let plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    let schemas = HashMap::from([(table_id.clone(), schema(table_id.clone()))]);
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("pg-xid-77").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![CdcChange::Insert {
                    row: row(1, "open"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();

    let records = encode_pipeline_transaction_records(&plan, &schemas, &transaction).unwrap();

    assert!(records[0].headers().is_empty());
}

#[test]
fn pipeline_transaction_records_can_include_idempotency_headers() {
    let table_id = CdcTableId::new("orders").unwrap();
    let plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    let schemas = HashMap::from([(table_id.clone(), schema(table_id.clone()))]);
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("pg-xid-77").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![
                    CdcChange::Insert {
                        row: row(1, "open"),
                    },
                    CdcChange::Insert {
                        row: row(2, "paid"),
                    },
                ],
            )
            .unwrap(),
        ],
    )
    .unwrap();

    let records =
        encode_pipeline_transaction_records_with_metadata(&plan, &schemas, &transaction, true)
            .unwrap();

    assert_eq!(
        header_value(&records[0], FLOE_HEADER_IDEMPOTENCY_KEY),
        Some("orders_pipe/public.orders/pg-xid-77/0")
    );
    assert_eq!(
        header_value(&records[1], FLOE_HEADER_IDEMPOTENCY_KEY),
        Some("orders_pipe/public.orders/pg-xid-77/1")
    );
    assert_eq!(
        header_value(&records[0], FLOE_HEADER_SOURCE_POSITION),
        Some("pg/0/16B6C50")
    );
    assert_eq!(
        header_value(&records[0], FLOE_HEADER_TRANSACTION_ID),
        Some("pg-xid-77")
    );
    assert_eq!(
        header_value(&records[0], FLOE_HEADER_RECORD_SEQUENCE),
        Some("0")
    );
    assert_eq!(
        header_value(&records[0], FLOE_HEADER_SOURCE_TABLE),
        Some("public.orders")
    );
}

#[test]
fn pipeline_checkpoint_preserves_transaction_schema_versions() {
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("pg-xid-77").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![
            ChangeBatch::new(
                CdcTableId::new("orders").unwrap(),
                vec![CdcChange::Insert {
                    row: row(1, "open"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap()
    .with_schema_versions(floe_cdc_core::CdcSchemaVersionMap::from([(
        "orders".to_string(),
        42,
    )]));

    let checkpoint = pipeline_checkpoint_from_transaction(&transaction);

    assert_eq!(checkpoint.schema_versions().get("orders"), Some(&42));
}

#[tokio::test]
async fn target_checkpoint_state_makes_partial_delivery_explicit() {
    let table_id = CdcTableId::new("orders").unwrap();
    let plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("pg-xid-77").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![CdcChange::Insert {
                    row: row(1, "open"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let records = vec![CdcBufferRecord::new(Some(vec![1]), Some(vec![2]))];
    let prepared = prepare_replication_buffer_append(&plan, &transaction, records).unwrap();
    let storage = SlateCatalog::in_memory().await.unwrap();
    let buffer_store = storage.cdc_buffer_store();
    let manifest = buffer_store
        .append_transaction(&prepared.append)
        .await
        .unwrap();

    let pending = pending_target_state(&plan, &manifest);
    assert_eq!(pending["buffer.status"], "durable");
    assert_eq!(pending["target.delivery.status"], "pending");
    assert_eq!(pending["target.delivery.replay_may_duplicate"], "true");
    assert_eq!(pending["target.kind"], "kafka");
    assert_eq!(pending["source.position.postgres.commit_lsn"], "0/16B6C50");

    let delivered = delivered_target_state(
        &plan,
        &manifest,
        std::collections::BTreeMap::from([
            ("kafka.topic".to_string(), "orders".to_string()),
            ("kafka.partition.0.offset".to_string(), "42".to_string()),
        ]),
    );
    assert_eq!(delivered["buffer.status"], "delivered");
    assert_eq!(delivered["target.delivery.status"], "delivered");
    assert_eq!(delivered["target.delivery.replay_may_duplicate"], "false");
    assert_eq!(delivered["kafka.partition.0.offset"], "42");

    let failed = failed_target_state(&plan, &manifest, &anyhow!("kafka unavailable"));
    assert_eq!(failed["buffer.status"], "durable");
    assert_eq!(failed["target.delivery.status"], "failed");
    assert_eq!(failed["target.delivery.replay_may_duplicate"], "true");
    assert!(failed["target.last_error"].contains("kafka unavailable"));
}

#[tokio::test]
async fn status_snapshots_expose_buffer_checkpoint_replay_and_error_state() {
    let table_id = CdcTableId::new("orders").unwrap();
    let plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    let runtime = test_runtime_with_plan(plan.clone());
    runtime.set_replay_state(&plan.name, true);
    runtime.set_last_target_error(&plan.name, "kafka unavailable".to_string());
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("pg-xid-77").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![CdcChange::Insert {
                    row: row(1, "open"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let prepared = prepare_replication_buffer_append(
        &plan,
        &transaction,
        vec![CdcBufferRecord::new(Some(vec![1]), Some(vec![2]))],
    )
    .unwrap();
    let storage = SlateCatalog::in_memory().await.unwrap();
    let buffer_store = storage.cdc_buffer_store();
    let manifest = buffer_store
        .append_transaction(&prepared.append)
        .await
        .unwrap();
    storage
        .put_replication_pipeline_checkpoint(
            ReplicationPipelineCheckpoint::new(
                &plan.name,
                &plan.source_name,
                manifest.source_position().clone(),
                manifest.transaction_id().cloned(),
                pending_target_state(&plan, &manifest),
                current_unix_time_ms(),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let snapshots = runtime.status_snapshots(&storage).await.unwrap();
    let snapshot = snapshots.first().expect("snapshot");

    assert_eq!(snapshot.pipeline_name(), "orders_pipe");
    assert_eq!(snapshot.source_name(), "pg_main");
    assert_eq!(snapshot.target_kind(), "kafka");
    assert_eq!(snapshot.pending_transactions(), 1);
    assert_eq!(snapshot.pending_records(), manifest.record_count());
    assert!(snapshot.pending_bytes() > 0);
    assert!(snapshot.oldest_pending_age_ms().is_some());
    assert!(snapshot.replaying());
    assert_eq!(snapshot.last_error(), Some("kafka unavailable"));
    assert_eq!(
        snapshot.checkpoint_position(),
        Some(manifest.source_position())
    );
    let checkpoint_lsn_bytes = PostgresLsn::parse("0/16B6C50").unwrap().as_u64();
    assert_eq!(snapshot.checkpoint_lsn_bytes(), Some(checkpoint_lsn_bytes));
    assert_eq!(
        snapshot
            .checkpoint_transaction_id()
            .map(CdcTransactionId::as_str),
        Some("pg-xid-77")
    );
    assert_eq!(snapshot.target_state()["target.delivery.status"], "pending");

    let debug_state = Arc::new(tokio::sync::RwLock::new(
        http_ingest::CdcReplicationDebugState::default(),
    ));
    {
        let mut state = debug_state.write().await;
        state
            .postgres_sources
            .push(http_ingest::PostgresCdcDebugSourceState {
                source: "pg_main".to_string(),
                slot: Some("slot_main".to_string()),
                upstream_lsn: Some(PostgresLsn::from_u64(checkpoint_lsn_bytes + 48).to_pg_string()),
                upstream_lsn_bytes: Some(checkpoint_lsn_bytes + 48),
                durable_lsn: Some(PostgresLsn::from_u64(checkpoint_lsn_bytes).to_pg_string()),
                durable_lsn_bytes: Some(checkpoint_lsn_bytes),
                source_lag_bytes: Some(48),
                ..http_ingest::PostgresCdcDebugSourceState::default()
            });
    }
    runtime
        .refresh_debug_state(&storage, &debug_state)
        .await
        .unwrap();
    let debug_state = debug_state.read().await;
    let debug_pipeline = debug_state.pipelines.first().expect("debug pipeline");
    assert_eq!(debug_state.refresh_error, None);
    assert_eq!(debug_pipeline.pipeline, "orders_pipe");
    assert_eq!(debug_pipeline.source, "pg_main");
    assert_eq!(debug_pipeline.target_kind, "kafka");
    assert_eq!(
        debug_pipeline.checkpoint_position.as_deref(),
        Some("pg/0/16B6C50")
    );
    assert_eq!(
        debug_pipeline.checkpoint_lsn_bytes,
        Some(checkpoint_lsn_bytes)
    );
    assert_eq!(debug_pipeline.checkpoint_lag_bytes, Some(48));
    assert_eq!(
        debug_pipeline.checkpoint_transaction_id.as_deref(),
        Some("pg-xid-77")
    );
    assert_eq!(debug_pipeline.pending_transactions, 1);
    assert_eq!(debug_pipeline.pending_records, manifest.record_count());
    assert!(debug_pipeline.pending_bytes > 0);
    assert!(debug_pipeline.oldest_pending_age_ms.is_some());
    assert!(debug_pipeline.replaying);
    assert_eq!(
        debug_pipeline.last_error.as_deref(),
        Some("kafka unavailable")
    );
    assert_eq!(
        debug_pipeline.target_state["target.delivery.status"],
        "pending"
    );
}

#[tokio::test]
async fn status_snapshots_track_target_outage_replay_and_recovery() {
    let table_id = CdcTableId::new("orders").unwrap();
    let plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    let runtime = test_runtime_with_plan(plan.clone());
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("pg-xid-88").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6D00", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![CdcChange::Insert {
                    row: row(2, "pending"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let prepared = prepare_replication_buffer_append(
        &plan,
        &transaction,
        vec![CdcBufferRecord::new(Some(vec![2]), Some(vec![4]))],
    )
    .unwrap();
    let storage = SlateCatalog::in_memory().await.unwrap();
    let buffer_store = storage.cdc_buffer_store();
    let manifest = buffer_store
        .append_transaction(&prepared.append)
        .await
        .unwrap();

    runtime
        .mark_manifest_delivery_failed(&plan, &storage, &manifest, anyhow!("kafka outage"))
        .await
        .unwrap();
    let failed = runtime.status_snapshots(&storage).await.unwrap();
    let failed = failed.first().expect("failed snapshot");
    assert_eq!(failed.pending_transactions(), 1);
    assert_eq!(failed.pending_records(), manifest.record_count());
    assert_eq!(failed.last_error(), Some("kafka outage"));
    assert!(!failed.replaying());
    assert_eq!(failed.target_state()["target.delivery.status"], "failed");
    assert_eq!(
        failed.target_state()["target.delivery.replay_may_duplicate"],
        "true"
    );

    runtime.set_replay_state(&plan.name, true);
    let replaying = runtime.status_snapshots(&storage).await.unwrap();
    let replaying = replaying.first().expect("replaying snapshot");
    assert!(replaying.replaying());
    assert_eq!(replaying.last_error(), Some("kafka outage"));
    runtime.set_source_backpressure_state(&plan.name, true);
    let backpressured = runtime.status_snapshots(&storage).await.unwrap();
    let backpressured = backpressured.first().expect("backpressured snapshot");
    assert!(backpressured.source_backpressure_active());
    runtime.set_source_backpressure_state(&plan.name, false);

    runtime
        .mark_manifest_delivered(
            &plan,
            &buffer_store,
            &storage,
            &manifest,
            std::collections::BTreeMap::from([
                ("kafka.topic".to_string(), "orders".to_string()),
                ("kafka.partition.0.offset".to_string(), "99".to_string()),
            ]),
        )
        .await
        .unwrap();
    runtime.set_replay_state(&plan.name, false);

    let recovered = runtime.status_snapshots(&storage).await.unwrap();
    let recovered = recovered.first().expect("recovered snapshot");
    assert_eq!(recovered.pending_transactions(), 0);
    assert_eq!(recovered.pending_records(), 0);
    assert_eq!(recovered.pending_bytes(), 0);
    assert_eq!(recovered.oldest_pending_age_ms(), None);
    assert!(!recovered.replaying());
    assert_eq!(recovered.last_error(), None);
    assert_eq!(
        recovered.checkpoint_position(),
        Some(manifest.source_position())
    );
    assert_eq!(
        recovered
            .checkpoint_transaction_id()
            .map(CdcTransactionId::as_str),
        Some("pg-xid-88")
    );
    assert_eq!(
        recovered.target_state()["target.delivery.status"],
        "delivered"
    );
    assert_eq!(
        recovered.target_state()["target.delivery.replay_may_duplicate"],
        "false"
    );
    assert_eq!(recovered.target_state()["kafka.partition.0.offset"], "99");

    let debug_state = Arc::new(tokio::sync::RwLock::new(
        http_ingest::CdcReplicationDebugState::default(),
    ));
    runtime
        .refresh_debug_state(&storage, &debug_state)
        .await
        .unwrap();
    let debug_state = debug_state.read().await;
    let pipeline = debug_state.pipelines.first().expect("debug pipeline");
    assert_eq!(pipeline.pending_transactions, 0);
    assert_eq!(pipeline.pending_objects, 0);
    assert!(!pipeline.replaying);
    assert!(!pipeline.source_backpressure_active);
    assert_eq!(pipeline.last_error, None);
    assert_eq!(pipeline.target_state["target.delivery.status"], "delivered");
}

#[tokio::test]
async fn durable_pipeline_buffers_source_progress_when_target_is_down() {
    let table_id = CdcTableId::new("orders").unwrap();
    let plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    let runtime = test_runtime_with_plan(plan.clone());
    let storage = SlateCatalog::in_memory().await.unwrap();
    let schemas = HashMap::from([(plan.table_id.clone(), plan.schema.clone())]);
    let source_id = CdcSourceId::new("pg_main").unwrap();
    let first = TransactionBatch::new(
        source_id.clone(),
        Some(CdcTransactionId::new("pg-xid-101").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id.clone(),
                vec![CdcChange::Insert {
                    row: row(1, "open"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let second = TransactionBatch::new(
        source_id.clone(),
        Some(CdcTransactionId::new("pg-xid-102").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6D00", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![CdcChange::Insert {
                    row: row(2, "paid"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();

    assert_eq!(
        runtime
            .run_transaction(&source_id, &schemas, &first, Some(&storage))
            .await
            .expect("buffer first transaction"),
        1
    );
    assert_eq!(
        runtime
            .run_transaction(&source_id, &schemas, &second, Some(&storage))
            .await
            .expect("buffer second transaction"),
        1
    );

    let buffer_store = storage.cdc_buffer_store();
    let pending = buffer_store
        .pending_transactions(&plan.name, 10)
        .await
        .expect("pending transactions");
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].source_position(), first.commit_position());
    assert_eq!(pending[1].source_position(), second.commit_position());

    let source_frontier = buffer_store
        .source_frontier(&plan.name)
        .await
        .expect("source frontier")
        .expect("source frontier");
    assert_eq!(source_frontier.source_position(), second.commit_position());
    assert_eq!(
        source_frontier
            .transaction_id()
            .map(CdcTransactionId::as_str),
        Some("pg-xid-102")
    );
    assert_eq!(
        buffer_store
            .delivery_frontier(&plan.name)
            .await
            .expect("delivery frontier"),
        None
    );

    let checkpoint = storage
        .replication_pipeline_checkpoint(&plan.name)
        .await
        .expect("checkpoint")
        .expect("checkpoint");
    assert_eq!(checkpoint.source_position(), first.commit_position());
    assert_eq!(
        checkpoint.target_state()["target.delivery.status"],
        "failed"
    );
    assert_eq!(
        checkpoint.target_state()["target.delivery.replay_may_duplicate"],
        "true"
    );

    let restarted = test_runtime_with_plan(plan.clone());
    assert_eq!(
        restarted
            .replay_buffered(&storage)
            .await
            .expect("replay buffered transactions"),
        0
    );
    let still_pending = buffer_store
        .pending_transactions(&plan.name, 10)
        .await
        .expect("pending after restart replay");
    assert_eq!(still_pending.len(), 2);
}

#[tokio::test]
async fn durable_pipeline_dead_letters_and_advances_when_policy_allows() {
    let table_id = CdcTableId::new("orders").unwrap();
    let mut plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    plan.error_policy = CatalogReplicationErrorPolicy::new(
        CatalogReplicationErrorPolicyMode::DeadLetterAndContinue,
        None,
    );
    let runtime = test_runtime_with_plan(plan.clone());
    let storage = SlateCatalog::in_memory().await.unwrap();
    let schemas = HashMap::from([(plan.table_id.clone(), plan.schema.clone())]);
    let source_id = CdcSourceId::new("pg_main").unwrap();
    let transaction = TransactionBatch::new(
        source_id.clone(),
        Some(CdcTransactionId::new("pg-xid-301").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![CdcChange::Insert {
                    row: row(1, "open"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();

    assert_eq!(
        runtime
            .run_transaction(&source_id, &schemas, &transaction, Some(&storage))
            .await
            .expect("dead-letter transaction"),
        1
    );

    let buffer_store = storage.cdc_buffer_store();
    let pending = buffer_store
        .pending_transactions(&plan.name, 10)
        .await
        .expect("pending transactions");
    assert!(pending.is_empty());
    let dlq_entries = storage
        .replication_pipeline_dlq_entries(&plan.name)
        .await
        .expect("dlq entries");
    assert_eq!(dlq_entries.len(), 1);
    let dlq_entry = &dlq_entries[0];
    assert_eq!(dlq_entry.source_position(), transaction.commit_position());
    assert_eq!(dlq_entry.error_class(), "kafka_delivery");
    assert_eq!(dlq_entry.payload_format(), Some("kafka_records"));
    let payload = storage
        .replication_pipeline_dlq_payload(dlq_entry.payload_object_key().unwrap())
        .await
        .expect("dlq payload");
    let records =
        floe_storage::decode_cdc_buffer_records_payload(&payload).expect("decode payload");
    assert_eq!(records.len(), 1);

    let checkpoint = storage
        .replication_pipeline_checkpoint(&plan.name)
        .await
        .expect("checkpoint")
        .expect("checkpoint");
    assert_eq!(checkpoint.source_position(), transaction.commit_position());
    assert_eq!(
        checkpoint.target_state()["target.delivery.status"],
        "dead_lettered"
    );
    assert_eq!(
        checkpoint.target_state()["target.delivery.replay_may_duplicate"],
        "false"
    );
    assert_eq!(
        checkpoint.target_state()["target.dlq.id"],
        dlq_entry.dlq_id()
    );
    assert!(checkpoint.target_state()["target.last_error"].contains("has no Kafka writer"));
}

#[tokio::test]
async fn replay_dead_letters_pending_buffer_when_policy_allows() {
    let table_id = CdcTableId::new("orders").unwrap();
    let mut plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    plan.error_policy = CatalogReplicationErrorPolicy::new(
        CatalogReplicationErrorPolicyMode::DeadLetterAndContinue,
        None,
    );
    let runtime = test_runtime_with_plan(plan.clone());
    let storage = SlateCatalog::in_memory().await.unwrap();
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("pg-xid-302").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6D00", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![CdcChange::Insert {
                    row: row(2, "paid"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let prepared = prepare_replication_buffer_append(
        &plan,
        &transaction,
        vec![CdcBufferRecord::new(Some(vec![2]), Some(vec![3]))],
    )
    .unwrap();
    let buffer_store = storage.cdc_buffer_store();
    let manifest = buffer_store
        .append_transaction(&prepared.append)
        .await
        .expect("append pending transaction");

    assert_eq!(
        runtime
            .replay_buffered(&storage)
            .await
            .expect("dead-letter pending transaction"),
        1
    );
    assert!(
        buffer_store
            .pending_transactions(&plan.name, 10)
            .await
            .expect("pending transactions")
            .is_empty()
    );
    let delivered = buffer_store
        .delivery_frontier(&plan.name)
        .await
        .expect("delivery frontier")
        .expect("delivery frontier");
    assert_eq!(delivered.source_position(), manifest.source_position());

    let checkpoint = storage
        .replication_pipeline_checkpoint(&plan.name)
        .await
        .expect("checkpoint")
        .expect("checkpoint");
    assert_eq!(checkpoint.source_position(), manifest.source_position());
    assert_eq!(
        checkpoint.target_state()["target.delivery.status"],
        "dead_lettered"
    );
    assert_eq!(
        storage
            .replication_pipeline_dlq_entries(&plan.name)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn retry_dlq_entry_records_attempt_when_target_still_fails() {
    let table_id = CdcTableId::new("orders").unwrap();
    let plan = test_plan("orders_pipe", table_id, "public.orders");
    let runtime = test_runtime_with_plan(plan.clone());
    let storage = SlateCatalog::in_memory().await.unwrap();
    let dlq_id = "entry-1";
    let payload_object_key = storage
        .put_replication_pipeline_dlq_payload(
            &plan.name,
            dlq_id,
            floe_storage::encode_cdc_buffer_records_payload(&[CdcBufferRecord::new(
                Some(br#"{"id":1}"#.to_vec()),
                Some(br#"{"id":1,"status":"open"}"#.to_vec()),
            )])
            .expect("encode records"),
        )
        .await
        .expect("persist payload");
    let entry = ReplicationPipelineDlqEntry::new(
        &plan.name,
        dlq_id,
        &plan.source_name,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6E00", None).unwrap(),
        Some(CdcTransactionId::new("pg-xid-401").unwrap()),
        "kafka_delivery",
        "broker unavailable",
        1,
        Some(payload_object_key),
        Some("kafka_records".to_string()),
        24,
        BTreeMap::new(),
        current_unix_time_ms(),
    )
    .unwrap();
    storage
        .put_replication_pipeline_dlq_entry(entry)
        .await
        .expect("persist entry");

    let err = runtime
        .retry_dlq_entry(&storage, &plan.name, dlq_id)
        .await
        .expect_err("retry should fail without a writer");
    assert!(err.to_string().contains("retry replication pipeline"));
    let attempted = storage
        .replication_pipeline_dlq_entry(&plan.name, dlq_id)
        .await
        .expect("load entry")
        .expect("entry exists");
    assert_eq!(attempted.status(), ReplicationPipelineDlqStatus::Pending);
    assert_eq!(attempted.attempt_count(), 2);
}

#[tokio::test]
async fn durable_pipeline_stops_source_progress_when_buffer_cap_remains_exceeded() {
    let table_id = CdcTableId::new("orders").unwrap();
    let mut plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    plan.buffer_policy = CatalogReplicationBufferPolicy::new(None, None, Some(1), None);
    let runtime = test_runtime_with_plan(plan.clone());
    let storage = SlateCatalog::in_memory().await.unwrap();
    let schemas = HashMap::from([(plan.table_id.clone(), plan.schema.clone())]);
    let source_id = CdcSourceId::new("pg_main").unwrap();
    let first = TransactionBatch::new(
        source_id.clone(),
        Some(CdcTransactionId::new("pg-xid-201").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id.clone(),
                vec![CdcChange::Insert {
                    row: row(1, "open"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let second = TransactionBatch::new(
        source_id.clone(),
        Some(CdcTransactionId::new("pg-xid-202").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6D00", None).unwrap(),
        vec![
            ChangeBatch::new(
                table_id,
                vec![CdcChange::Insert {
                    row: row(2, "paid"),
                }],
            )
            .unwrap(),
        ],
    )
    .unwrap();

    assert_eq!(
        runtime
            .run_transaction(&source_id, &schemas, &first, Some(&storage))
            .await
            .expect("buffer first transaction"),
        1
    );
    let error = runtime
        .run_transaction(&source_id, &schemas, &second, Some(&storage))
        .await
        .expect_err("second transaction should trip the pending object cap");
    assert!(error.to_string().contains("durable buffer limit exceeded"));

    let buffer_store = storage.cdc_buffer_store();
    let pending = buffer_store
        .pending_transactions(&plan.name, 10)
        .await
        .expect("pending transactions");
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0].transaction_id().map(CdcTransactionId::as_str),
        Some("pg-xid-201")
    );
    let source_frontier = buffer_store
        .source_frontier(&plan.name)
        .await
        .expect("source frontier")
        .expect("source frontier");
    assert_eq!(source_frontier.source_position(), first.commit_position());
    assert_eq!(
        source_frontier
            .transaction_id()
            .map(CdcTransactionId::as_str),
        Some("pg-xid-201")
    );

    let checkpoint = storage
        .replication_pipeline_checkpoint(&plan.name)
        .await
        .expect("checkpoint")
        .expect("checkpoint");
    assert_eq!(checkpoint.source_position(), first.commit_position());
    assert_eq!(
        checkpoint.transaction_id().map(CdcTransactionId::as_str),
        Some("pg-xid-201")
    );

    let snapshots = runtime.status_snapshots(&storage).await.unwrap();
    let snapshot = snapshots.first().expect("snapshot");
    assert_eq!(snapshot.pending_transactions(), 1);
    assert_eq!(snapshot.pending_records(), pending[0].record_count());
    assert!(snapshot.source_backpressure_active());
}

#[test]
fn buffer_limit_violation_accounts_for_incoming_payload_bytes() {
    let limits = ReplicationBufferLimits {
        max_pending_bytes: Some(100),
        max_pending_records: None,
        max_pending_transactions: None,
        max_pending_age_ms: None,
    };

    assert_eq!(
        buffer_limit_violation(0, 0, 70, None, 31, 0, limits),
        Some(ReplicationBufferLimitViolation::Bytes {
            pending_bytes: 70,
            incoming_bytes: 31,
            max_pending_bytes: 100,
        })
    );
    assert_eq!(buffer_limit_violation(0, 0, 70, None, 30, 0, limits), None);
}

#[test]
fn buffer_limit_violation_accounts_for_pending_records() {
    let limits = ReplicationBufferLimits {
        max_pending_bytes: None,
        max_pending_records: Some(10),
        max_pending_transactions: None,
        max_pending_age_ms: None,
    };

    assert_eq!(
        buffer_limit_violation(0, 8, 0, None, 0, 3, limits),
        Some(ReplicationBufferLimitViolation::Records {
            pending_records: 8,
            incoming_records: 3,
            max_pending_records: 10,
        })
    );
    assert_eq!(buffer_limit_violation(0, 8, 0, None, 0, 2, limits), None);
}

#[test]
fn buffer_limit_violation_accounts_for_pending_objects() {
    let limits = ReplicationBufferLimits {
        max_pending_bytes: None,
        max_pending_records: None,
        max_pending_transactions: Some(2),
        max_pending_age_ms: None,
    };

    assert_eq!(
        buffer_limit_violation(2, 0, 0, None, 0, 1, limits),
        Some(ReplicationBufferLimitViolation::Objects {
            pending_transactions: 2,
            incoming_transactions: 1,
            max_pending_transactions: 2,
        })
    );
    assert_eq!(buffer_limit_violation(1, 0, 0, None, 0, 1, limits), None);
}

#[test]
fn buffer_limit_violation_checks_oldest_pending_age() {
    let limits = ReplicationBufferLimits {
        max_pending_bytes: None,
        max_pending_records: None,
        max_pending_transactions: None,
        max_pending_age_ms: Some(1_000),
    };

    assert_eq!(
        buffer_limit_violation(0, 0, 0, Some(1_001), 0, 0, limits),
        Some(ReplicationBufferLimitViolation::Age {
            oldest_pending_age_ms: 1_001,
            max_pending_age_ms: 1_000,
        })
    );
    assert_eq!(
        buffer_limit_violation(0, 0, 0, Some(1_000), 0, 0, limits),
        None
    );
}

#[test]
fn estimated_buffer_payload_bytes_includes_record_framing() {
    let records = vec![
        CdcBufferRecord::new(Some(vec![1, 2, 3]), Some(vec![4])),
        CdcBufferRecord::new(None, Some(vec![5, 6])),
    ];

    assert_eq!(estimated_buffer_payload_bytes(&records), 70);
}

#[test]
fn zero_buffer_limit_override_disables_default_limit() {
    assert_eq!(effective_usize_limit(Some(0), Some(100)), None);
    assert_eq!(effective_u64_limit(Some(0), Some(100)), None);
    assert_eq!(effective_usize_limit(None, Some(100)), Some(100));
    assert_eq!(effective_u64_limit(None, Some(100)), Some(100));
}

#[test]
fn parses_arrow_ipc_compression_override() {
    assert_eq!(
        ReplicationArrowIpcCompressionConfig::parse("lz4"),
        Some(ReplicationArrowIpcCompressionConfig::Lz4Frame)
    );
    assert_eq!(
        ReplicationArrowIpcCompressionConfig::parse("lz4-frame"),
        Some(ReplicationArrowIpcCompressionConfig::Lz4Frame)
    );
    assert_eq!(ReplicationArrowIpcCompressionConfig::parse("none"), None);
    assert_eq!(ReplicationArrowIpcCompressionConfig::parse("bogus"), None);
}

fn schema(table_id: CdcTableId) -> CdcTableSchema {
    schema_for_table(table_id, "orders")
}

fn schema_for_table(table_id: CdcTableId, upstream_table: &str) -> CdcTableSchema {
    CdcTableSchema::new(
        table_id,
        UpstreamTableRef::new("public", upstream_table).unwrap(),
        vec![
            CdcColumn::new("id", ColumnType::Int64, false).unwrap(),
            CdcColumn::new("status", ColumnType::Utf8, true).unwrap(),
        ],
        CdcPrimaryKey::new(["id"]).unwrap(),
    )
    .unwrap()
}

fn test_plan(
    name: &str,
    table_id: CdcTableId,
    upstream_table: &str,
) -> ReplicationPipelineRuntimePlan {
    ReplicationPipelineRuntimePlan {
        name: name.to_string(),
        source_name: "pg_main".to_string(),
        database_name: "postgres".to_string(),
        upstream_table: upstream_table.to_string(),
        table_id: table_id.clone(),
        schema: schema_for_table(table_id, upstream_table.strip_prefix("public.").unwrap()),
        schema_evolution_policy: PostgresSchemaEvolutionPolicy::FailFast,
        target: ReplicationPipelineRuntimeTarget::Kafka {
            brokers: "localhost:9092".to_string(),
            topic: upstream_table.to_string(),
        },
        format: ReplicationPipelineRuntimeFormat::FloeJson,
        buffer_mode: ReplicationPipelineRuntimeBufferMode::Durable,
        buffer_policy: CatalogReplicationBufferPolicy::default(),
        error_policy: CatalogReplicationErrorPolicy::default(),
        emit_tombstones: false,
        include_transaction_metadata: false,
    }
}

fn test_runtime_with_plan(plan: ReplicationPipelineRuntimePlan) -> ReplicationPipelineRuntime {
    ReplicationPipelineRuntime {
        pipelines_by_source: HashMap::from([(
            CdcSourceId::new(plan.source_name.clone()).unwrap(),
            vec![plan],
        )]),
        kafka_writers_by_pipeline: HashMap::new(),
        postgres_writers_by_pipeline: HashMap::new(),
        buffer_cleanup_last_by_pipeline: Mutex::new(HashMap::new()),
        replay_state_by_pipeline: Mutex::new(HashMap::new()),
        backpressure_state_by_pipeline: Mutex::new(HashMap::new()),
        last_target_error_by_pipeline: Mutex::new(HashMap::new()),
        settings: FloeReplicationConfig::default(),
    }
}

fn row(id: i64, status: &str) -> CdcRow {
    CdcRow::new([
        Some(RowValue::Int64(id)),
        Some(RowValue::Utf8(status.to_string())),
    ])
    .unwrap()
}

fn header_value<'a>(record: &'a CdcBufferRecord, key: &str) -> Option<&'a str> {
    record
        .headers()
        .iter()
        .find(|header| header.key() == key)
        .and_then(|header| std::str::from_utf8(header.value()).ok())
}
