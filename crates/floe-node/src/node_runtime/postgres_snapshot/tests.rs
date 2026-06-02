use super::adaptive_concurrency::*;
use super::commit_utils::*;
use super::schema::*;
use super::snapshot_load::*;
use super::table_scan::*;
use super::*;
use floe_cdc_core::{CdcChange, CdcColumn, CdcPrimaryKey, UpstreamTableRef};
use floe_core::RowValue;
use floe_core::catalog::ColumnType;
use std::sync::Arc;

#[test]
fn parses_decimal_text_without_allocation_sensitive_edge_cases() {
    assert_eq!(parse_decimal_text_to_i128("123.45", 2).unwrap(), 12_345);
    assert_eq!(parse_decimal_text_to_i128("123", 2).unwrap(), 12_300);
    assert_eq!(parse_decimal_text_to_i128("-0.07", 2).unwrap(), -7);
    assert_eq!(parse_decimal_text_to_i128("+42.1", 3).unwrap(), 42_100);
    assert_eq!(parse_decimal_text_to_i128(" .5 ", 2).unwrap(), 50);
}

#[test]
fn quotes_exported_snapshot_literal() {
    assert_eq!(
        quote_pg_literal("00000003-0000001B-1"),
        "'00000003-0000001B-1'"
    );
    assert_eq!(quote_pg_literal("snap'shot"), "'snap''shot'");
}

#[test]
fn rejects_decimal_text_that_cannot_match_scale() {
    assert!(parse_decimal_text_to_i128("1.234", 2).is_err());
    assert!(parse_decimal_text_to_i128("1.2.3", 2).is_err());
    assert!(parse_decimal_text_to_i128("", 2).is_err());
    assert!(parse_decimal_text_to_i128("abc", 2).is_err());
    assert!(parse_decimal_text_to_i128("1.0", -1).is_err());
}

#[test]
fn postgres_cdc_type_mapping_covers_claimed_common_types() -> Result<()> {
    assert_eq!(
        postgres_column_type("int8", "bigint", None, None)?,
        ColumnType::Int64
    );
    assert_eq!(
        postgres_column_type("int4", "integer", None, None)?,
        ColumnType::Int64
    );
    assert_eq!(
        postgres_column_type("bool", "boolean", None, None)?,
        ColumnType::Bool
    );
    assert_eq!(
        postgres_column_type("text", "text", None, None)?,
        ColumnType::Utf8
    );
    assert_eq!(
        postgres_column_type("varchar", "character varying", None, None)?,
        ColumnType::Utf8
    );
    assert_eq!(
        postgres_column_type("uuid", "uuid", None, None)?,
        ColumnType::Utf8
    );
    assert_eq!(
        postgres_column_type("jsonb", "jsonb", None, None)?,
        ColumnType::Utf8
    );
    assert_eq!(
        postgres_column_type("bytea", "bytea", None, None)?,
        ColumnType::Utf8
    );
    assert_eq!(
        postgres_column_type("date", "date", None, None)?,
        ColumnType::DateDays
    );
    assert_eq!(
        postgres_column_type("timestamp", "timestamp without time zone", None, None)?,
        ColumnType::TimestampMillis
    );
    assert_eq!(
        postgres_column_type("timestamptz", "timestamp with time zone", None, None)?,
        ColumnType::TimestampMillis
    );
    assert_eq!(
        postgres_column_type("numeric", "numeric", Some(12), Some(2))?,
        ColumnType::decimal128(12, 2)?
    );
    assert_eq!(
        postgres_column_type("numeric", "numeric", None, None)?,
        ColumnType::Numeric
    );

    assert!(postgres_type_compatible(
        &ColumnType::Numeric,
        "numeric",
        "numeric",
        Some(12),
        Some(2)
    ));
    assert!(postgres_type_compatible(
        &ColumnType::decimal128(12, 2)?,
        "numeric",
        "numeric",
        Some(12),
        Some(2)
    ));
    assert!(!postgres_type_compatible(
        &ColumnType::decimal128(12, 3)?,
        "numeric",
        "numeric",
        Some(12),
        Some(2)
    ));

    Ok(())
}

#[test]
fn int64_primary_key_chunks_cover_range_without_overlap() {
    let chunks = int64_snapshot_range_chunks("id", 1, 10, 3);

    assert_eq!(
        chunks,
        vec![
            SnapshotTableChunk::Int64Range {
                column: "id".to_string(),
                lower_inclusive: 1,
                upper_exclusive: Some(5),
            },
            SnapshotTableChunk::Int64Range {
                column: "id".to_string(),
                lower_inclusive: 5,
                upper_exclusive: Some(9),
            },
            SnapshotTableChunk::Int64Range {
                column: "id".to_string(),
                lower_inclusive: 9,
                upper_exclusive: None,
            },
        ]
    );
    assert_eq!(
        snapshot_table_query(&snapshot_test_schema(), &chunks[0]),
        r#"SELECT "id", "status"::text AS "status" FROM "public"."orders" WHERE "id" >= 1 AND "id" < 5"#
    );
    assert_eq!(
        snapshot_table_query(&snapshot_test_schema(), &chunks[2]),
        r#"SELECT "id", "status"::text AS "status" FROM "public"."orders" WHERE "id" >= 9"#
    );
}

#[test]
fn snapshot_chunking_requires_single_int64_primary_key() {
    let int64_schema = snapshot_test_schema();
    assert_eq!(
        single_int64_primary_key_column(&int64_schema).map(CdcColumn::name),
        Some("id")
    );

    let text_pk_schema = CdcTableSchema::new(
        CdcTableId::new("orders_by_status").expect("table id"),
        UpstreamTableRef::new("public", "orders").expect("upstream"),
        vec![
            CdcColumn::new("id", ColumnType::Int64, false).expect("id"),
            CdcColumn::new("status", ColumnType::Utf8, false).expect("status"),
        ],
        CdcPrimaryKey::new(["status"]).expect("primary key"),
    )
    .expect("schema");
    assert!(single_int64_primary_key_column(&text_pk_schema).is_none());

    let composite_schema = CdcTableSchema::new(
        CdcTableId::new("orders_composite").expect("table id"),
        UpstreamTableRef::new("public", "orders").expect("upstream"),
        vec![
            CdcColumn::new("id", ColumnType::Int64, false).expect("id"),
            CdcColumn::new("status", ColumnType::Utf8, false).expect("status"),
        ],
        CdcPrimaryKey::new(["id", "status"]).expect("primary key"),
    )
    .expect("schema");
    assert!(single_int64_primary_key_column(&composite_schema).is_none());
}

#[test]
fn adaptive_snapshot_concurrency_decision_uses_wal_and_scan_pressure() {
    let config = SnapshotAdaptiveConcurrencyConfig {
        enabled: true,
        min_workers: 1,
        max_workers: 4,
        wal_buffer_high_watermark_percent: 75,
        wal_buffer_low_watermark_percent: 25,
        slow_scan_ms: 1_000,
        controller_interval: Duration::from_millis(500),
    };

    assert_eq!(
        snapshot_concurrency_decision(
            config,
            4,
            SnapshotWalBufferPressure {
                pending_events: 8,
                capacity: 10,
            },
            SnapshotSinkHealth::Healthy,
            None,
        ),
        Some(SnapshotConcurrencyDecision {
            target_workers: 3,
            direction: "decrease",
            reason: "wal_buffer_high",
        })
    );
    assert_eq!(
        snapshot_concurrency_decision(
            config,
            3,
            SnapshotWalBufferPressure {
                pending_events: 1,
                capacity: 10,
            },
            SnapshotSinkHealth::Healthy,
            Some(SnapshotScanObservation {
                elapsed_ms: 2_000,
                rows: 10,
            }),
        ),
        Some(SnapshotConcurrencyDecision {
            target_workers: 2,
            direction: "decrease",
            reason: "snapshot_scan_slow",
        })
    );
    assert_eq!(
        snapshot_concurrency_decision(
            config,
            2,
            SnapshotWalBufferPressure {
                pending_events: 1,
                capacity: 10,
            },
            SnapshotSinkHealth::Healthy,
            None,
        ),
        Some(SnapshotConcurrencyDecision {
            target_workers: 3,
            direction: "increase",
            reason: "wal_buffer_low",
        })
    );
    assert_eq!(
        snapshot_concurrency_decision(
            config,
            4,
            SnapshotWalBufferPressure {
                pending_events: 1,
                capacity: 10,
            },
            SnapshotSinkHealth::Backpressured,
            None,
        ),
        Some(SnapshotConcurrencyDecision {
            target_workers: 1,
            direction: "decrease",
            reason: "sink_backpressure",
        })
    );
    assert_eq!(
        snapshot_concurrency_decision(
            config,
            1,
            SnapshotWalBufferPressure {
                pending_events: 1,
                capacity: 10,
            },
            SnapshotSinkHealth::TargetError,
            None,
        ),
        None
    );
}

#[tokio::test]
async fn snapshot_scan_limiter_respects_dynamic_target() {
    let limiter = Arc::new(SnapshotScanLimiter::new("pg_test", "slot_test", 2));
    let first_permit = limiter.acquire().await;
    let second_permit = limiter.acquire().await;
    assert_eq!(limiter.active_workers(), 2);
    assert_eq!(limiter.set_target(1), Some((2, 1)));

    let acquire_waiter = {
        let limiter = Arc::clone(&limiter);
        tokio::spawn(async move { limiter.acquire().await })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!acquire_waiter.is_finished());

    drop(first_permit);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!acquire_waiter.is_finished());

    drop(second_permit);
    let third_permit = tokio::time::timeout(Duration::from_secs(1), acquire_waiter)
        .await
        .expect("scan permit acquisition should resume")
        .expect("scan permit task should succeed");
    assert_eq!(limiter.active_workers(), 1);
    drop(third_permit);
    assert_eq!(limiter.active_workers(), 0);
}

#[tokio::test]
async fn cancelled_snapshot_before_commit_leaves_no_checkpoint_for_retry() {
    let source_id = CdcSourceId::new("pg_main").expect("source id");
    let table_id = CdcTableId::new("orders").expect("table id");
    let catalog = floe_storage::SlateCatalog::in_memory()
        .await
        .expect("catalog");
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(catalog.db()));
    let table_store = CdcTableStore::new(table);
    let runtime_plan = PostgresCdcRuntimePlan {
        source_id: source_id.clone(),
        schemas: HashMap::new(),
        schema_evolution_policy: PostgresSchemaEvolutionPolicy::FailFast,
        replication_pipelines: Vec::new(),
    };
    let lsn = PostgresLsn::from_u64(120);
    let snapshot = PostgresSnapshot {
        lsn,
        transaction: snapshot_transaction_batch(
            &source_id,
            lsn,
            vec![
                ChangeBatch::new(
                    table_id,
                    vec![CdcChange::Insert {
                        row: floe_cdc_core::CdcRow::new([
                            Some(RowValue::Int64(1)),
                            Some(RowValue::Utf8("snapshot".to_string())),
                        ])
                        .expect("row"),
                    }],
                )
                .expect("snapshot change batch"),
            ],
        )
        .expect("snapshot transaction"),
        row_count: 1,
        wal_stream: None,
    };
    let (sender, mut receiver) = mpsc::channel(1);
    let (_commit_sender, mut commit_receiver) = watch::channel(PostgresCdcCommit::default());
    let cancel = CancellationToken::new();
    cancel.cancel();

    let err = finish_loaded_postgres_snapshot(
        "slot",
        "publication",
        &runtime_plan,
        &table_store,
        &sender,
        Some(&mut commit_receiver),
        &cancel,
        snapshot,
    )
    .await
    .expect_err("cancelled snapshot should not finish");

    assert!(format!("{err:#}").contains("cancelled while waiting for initial Postgres snapshot"));
    let queued = receiver.recv().await.expect("queued snapshot transaction");
    assert_eq!(queued.slot, "slot");
    assert_eq!(queued.source_id, source_id);
    assert_eq!(
        queued
            .transaction
            .transaction_id()
            .map(CdcTransactionId::as_str),
        Some("snapshot:0/78")
    );
    assert_eq!(
        table_store
            .load_checkpoint(&queued.source_id)
            .await
            .expect("load checkpoint"),
        None
    );
    assert!(
        receiver.try_recv().is_err(),
        "cancelled snapshot finalization should enqueue at most one retryable snapshot transaction"
    );
}

fn snapshot_test_schema() -> CdcTableSchema {
    CdcTableSchema::new(
        CdcTableId::new("orders").expect("table id"),
        UpstreamTableRef::new("public", "orders").expect("upstream"),
        vec![
            CdcColumn::new("id", ColumnType::Int64, false).expect("id"),
            CdcColumn::new("status", ColumnType::Utf8, true).expect("status"),
        ],
        CdcPrimaryKey::new(["id"]).expect("primary key"),
    )
    .expect("schema")
}
