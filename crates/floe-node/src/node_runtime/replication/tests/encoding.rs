use super::*;

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
    let mut plan = test_plan("p", CdcTableId::new("orders").unwrap(), "public.orders");
    plan.format = ReplicationPipelineRuntimeFormat::DebeziumJson;
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
    let mut plan = test_plan("p", CdcTableId::new("orders").unwrap(), "public.orders");
    plan.format = ReplicationPipelineRuntimeFormat::DebeziumJson;
    plan.include_transaction_metadata = true;
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
fn pipeline_debezium_records_validate_actual_kafka_shape() {
    let mut plan = test_plan("p", CdcTableId::new("orders").unwrap(), "public.orders");
    plan.format = ReplicationPipelineRuntimeFormat::DebeziumJson;
    plan.emit_tombstones = true;
    plan.include_transaction_metadata = true;
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
            CdcChange::Delete {
                key: None,
                before: Some(row(2, "void")),
            },
        ],
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
        .expect("encode debezium kafka records");

    assert_eq!(records.len(), 4);
    let insert_key = kafka_record_key_json(&records[0]);
    let insert_value = kafka_record_value_json(&records[0]).expect("insert value");
    let update_value = kafka_record_value_json(&records[1]).expect("update value");
    let delete_key = kafka_record_key_json(&records[2]);
    let delete_value = kafka_record_value_json(&records[2]).expect("delete value");

    assert_eq!(insert_key["payload"], serde_json::json!({"id": 1}));
    assert_eq!(
        insert_value["schema"]["name"],
        "pg_main.public.orders.Envelope"
    );
    assert_eq!(insert_value["payload"]["op"], "c");
    assert_eq!(insert_value["payload"]["before"], serde_json::Value::Null);
    assert_eq!(insert_value["payload"]["after"]["status"], "open");
    assert_eq!(insert_value["payload"]["source"]["connector"], "postgresql");
    assert_eq!(insert_value["payload"]["source"]["db"], "postgres");
    assert_eq!(insert_value["payload"]["source"]["schema"], "public");
    assert_eq!(insert_value["payload"]["source"]["table"], "orders");
    assert_eq!(insert_value["payload"]["source"]["txId"], 55);
    assert_eq!(insert_value["payload"]["source"]["lsn"], 23_817_296);
    assert_eq!(insert_value["payload"]["transaction"]["id"], "pg-xid-55");
    assert_eq!(insert_value["payload"]["transaction"]["total_order"], 0);

    assert_eq!(update_value["payload"]["op"], "u");
    assert_eq!(update_value["payload"]["before"]["status"], "open");
    assert_eq!(update_value["payload"]["after"]["status"], "paid");
    assert_eq!(update_value["payload"]["transaction"]["total_order"], 1);

    assert_eq!(delete_key["payload"], serde_json::json!({"id": 2}));
    assert_eq!(delete_value["payload"]["op"], "d");
    assert_eq!(delete_value["payload"]["before"]["status"], "void");
    assert_eq!(delete_value["payload"]["after"], serde_json::Value::Null);
    assert_eq!(delete_value["payload"]["transaction"]["total_order"], 2);
    assert_eq!(kafka_record_key_json(&records[3]), delete_key);
    assert_eq!(records[3].value(), None);
}

#[test]
fn pipeline_floe_json_records_encode_compact_row_messages() {
    let plan = test_plan("p", CdcTableId::new("orders").unwrap(), "public.orders");
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
fn kafka_json_encoders_cover_string_backed_postgres_types_and_timestamps() {
    let table_id = CdcTableId::new("orders").unwrap();
    let schema = CdcTableSchema::new(
        table_id.clone(),
        UpstreamTableRef::new("public", "orders").unwrap(),
        vec![
            CdcColumn::new("id", ColumnType::Utf8, false).unwrap(),
            CdcColumn::new("payload", ColumnType::Utf8, true).unwrap(),
            CdcColumn::new("blob", ColumnType::Utf8, true).unwrap(),
            CdcColumn::new("updated_at", ColumnType::TimestampMillis, true).unwrap(),
        ],
        CdcPrimaryKey::new(["id"]).unwrap(),
    )
    .unwrap();
    let batch = ChangeBatch::new(
        table_id.clone(),
        vec![CdcChange::Insert {
            row: CdcRow::new([
                Some(RowValue::Utf8(
                    "550e8400-e29b-41d4-a716-446655440000".to_string(),
                )),
                Some(RowValue::Utf8(r#"{"state":"paid"}"#.to_string())),
                Some(RowValue::Utf8(r#"\xdeadbeef"#.to_string())),
                Some(RowValue::TimestampMillis(1_704_165_845_678)),
            ])
            .unwrap(),
        }],
    )
    .unwrap();
    let transaction = TransactionBatch::new(
        CdcSourceId::new("pg_main").unwrap(),
        Some(CdcTransactionId::new("pg-xid-91").unwrap()),
        None,
        floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
        vec![batch.clone()],
    )
    .unwrap();
    let mut plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
    plan.schema = schema.clone();

    let floe_records =
        encode_pipeline_buffer_records(&plan, &schema, &batch, &transaction).unwrap();
    let floe_key = kafka_record_key_json(&floe_records[0]);
    let floe_value = kafka_record_value_json(&floe_records[0]).unwrap();
    assert_eq!(
        floe_key,
        serde_json::json!({"id": "550e8400-e29b-41d4-a716-446655440000"})
    );
    assert_eq!(floe_value["payload"], r#"{"state":"paid"}"#);
    assert_eq!(floe_value["blob"], r#"\xdeadbeef"#);
    assert_eq!(floe_value["updated_at"], 1_704_165_845_678_i64);

    plan.format = ReplicationPipelineRuntimeFormat::DebeziumJson;
    let debezium_records =
        encode_pipeline_buffer_records(&plan, &schema, &batch, &transaction).unwrap();
    let debezium_value = kafka_record_value_json(&debezium_records[0]).unwrap();
    assert_eq!(
        debezium_value["payload"]["after"]["id"],
        "550e8400-e29b-41d4-a716-446655440000"
    );
    assert_eq!(
        debezium_value["payload"]["after"]["payload"],
        r#"{"state":"paid"}"#
    );
    assert_eq!(debezium_value["payload"]["after"]["blob"], r#"\xdeadbeef"#);
    assert_eq!(
        debezium_value["payload"]["after"]["updated_at"],
        1_704_165_845_678_i64
    );
    assert_eq!(
        debezium_value["schema"]["fields"][1]["fields"][3]["name"],
        "io.debezium.time.Timestamp"
    );
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
fn postgres_target_compatibility_accepts_matching_table() {
    let schema = schema(CdcTableId::new("orders").unwrap());
    let target = PostgresTargetTableInfo::new(
        vec![
            PostgresTargetColumnInfo::new("id", "bigint", true, false, false),
            PostgresTargetColumnInfo::new("status", "text", false, false, false),
            PostgresTargetColumnInfo::new(
                "created_at",
                "timestamp with time zone",
                true,
                true,
                false,
            ),
        ],
        vec![vec!["id".to_string()]],
    );

    validate_postgres_target_table_compatibility(&schema, "public.orders_copy", &target)
        .expect("compatible target");
}

#[test]
fn postgres_target_compatibility_rejects_missing_columns_and_pk_index() {
    let schema = schema(CdcTableId::new("orders").unwrap());
    let missing_column = PostgresTargetTableInfo::new(
        vec![PostgresTargetColumnInfo::new(
            "id", "bigint", true, false, false,
        )],
        vec![vec!["id".to_string()]],
    );
    let err = validate_postgres_target_table_compatibility(
        &schema,
        "public.orders_copy",
        &missing_column,
    )
    .expect_err("missing CDC column should fail");
    assert!(format!("{err:#}").contains("missing CDC column 'status'"));

    let missing_pk = PostgresTargetTableInfo::new(
        vec![
            PostgresTargetColumnInfo::new("id", "bigint", true, false, false),
            PostgresTargetColumnInfo::new("status", "text", false, false, false),
        ],
        Vec::new(),
    );
    let err =
        validate_postgres_target_table_compatibility(&schema, "public.orders_copy", &missing_pk)
            .expect_err("missing PK index should fail");
    assert!(format!("{err:#}").contains("no unique index matching CDC primary key"));
}

#[test]
fn postgres_target_compatibility_rejects_incompatible_required_columns() {
    let schema = schema(CdcTableId::new("orders").unwrap());
    let incompatible_status = PostgresTargetTableInfo::new(
        vec![
            PostgresTargetColumnInfo::new("id", "bigint", true, false, false),
            PostgresTargetColumnInfo::new("status", "integer", false, false, false),
        ],
        vec![vec!["id".to_string()]],
    );
    let err = validate_postgres_target_table_compatibility(
        &schema,
        "public.orders_copy",
        &incompatible_status,
    )
    .expect_err("type mismatch should fail");
    assert!(format!("{err:#}").contains("has type 'integer'"));

    let required_extra = PostgresTargetTableInfo::new(
        vec![
            PostgresTargetColumnInfo::new("id", "bigint", true, false, false),
            PostgresTargetColumnInfo::new("status", "text", false, false, false),
            PostgresTargetColumnInfo::new("tenant_id", "bigint", true, false, false),
        ],
        vec![vec!["id".to_string()]],
    );
    let err = validate_postgres_target_table_compatibility(
        &schema,
        "public.orders_copy",
        &required_extra,
    )
    .expect_err("required extra column should fail");
    assert!(format!("{err:#}").contains("required column 'tenant_id'"));

    let not_null_status = PostgresTargetTableInfo::new(
        vec![
            PostgresTargetColumnInfo::new("id", "bigint", true, false, false),
            PostgresTargetColumnInfo::new("status", "text", true, false, false),
        ],
        vec![vec!["id".to_string()]],
    );
    let err = validate_postgres_target_table_compatibility(
        &schema,
        "public.orders_copy",
        &not_null_status,
    )
    .expect_err("nullable source into not-null target should fail");
    assert!(format!("{err:#}").contains("CDC schema allows NULL"));
}

#[test]
fn postgres_target_sql_casts_string_backed_native_types() {
    let schema = CdcTableSchema::new(
        CdcTableId::new("orders").unwrap(),
        UpstreamTableRef::new("public", "orders").unwrap(),
        vec![
            CdcColumn::new("id", ColumnType::Utf8, false).unwrap(),
            CdcColumn::new("payload", ColumnType::Utf8, true).unwrap(),
            CdcColumn::new("blob", ColumnType::Utf8, true).unwrap(),
        ],
        CdcPrimaryKey::new(["id"]).unwrap(),
    )
    .unwrap();
    let target = PostgresTargetTableInfo::new(
        vec![
            PostgresTargetColumnInfo::new("id", "uuid", true, false, false),
            PostgresTargetColumnInfo::new("payload", "jsonb", false, false, false),
            PostgresTargetColumnInfo::new("blob", "bytea", false, false, false),
        ],
        vec![vec!["id".to_string()]],
    );

    validate_postgres_target_table_compatibility(&schema, "public.orders_copy", &target)
        .expect("string-backed native target types should be compatible");
    assert_eq!(
        postgres_upsert_sql_with_target(&schema, "public.orders_copy", Some(&target))
            .expect("upsert sql"),
        "INSERT INTO \"public\".\"orders_copy\" (\"id\", \"payload\", \"blob\") VALUES ($1::uuid, $2::jsonb, $3::bytea) ON CONFLICT (\"id\") DO UPDATE SET \"payload\" = EXCLUDED.\"payload\", \"blob\" = EXCLUDED.\"blob\""
    );
    assert_eq!(
        postgres_delete_sql_with_target(&schema, "public.orders_copy", Some(&target))
            .expect("delete sql"),
        "DELETE FROM \"public\".\"orders_copy\" WHERE \"id\" = $1::uuid"
    );
}

#[test]
fn pipeline_arrow_ipc_records_encode_batches_without_json() {
    let mut plan = test_plan("p", CdcTableId::new("orders").unwrap(), "public.orders");
    plan.format = ReplicationPipelineRuntimeFormat::ArrowIpc;
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
    let mut plan = test_plan("p", CdcTableId::new("orders").unwrap(), "public.orders");
    plan.format = ReplicationPipelineRuntimeFormat::ArrowIpc;
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
    let mut plan = test_plan("p", CdcTableId::new("orders").unwrap(), "public.orders");
    plan.format = ReplicationPipelineRuntimeFormat::ArrowIpc;
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
