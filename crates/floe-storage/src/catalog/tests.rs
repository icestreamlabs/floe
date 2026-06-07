use super::*;
use arrow_schema::{DataType, Field, Schema};
use floe_cdc_core::{CdcSourcePosition, CdcTransactionId};
use floe_core::catalog::{
    CatalogSourceConnector, CatalogSourceDefinition, ColumnDefinition, ColumnType,
    PostgresCdcSourceDefinition, ReplicationBufferMode, ReplicationPipelineDefinition,
    ReplicationPipelineFormat, ReplicationPipelineTarget, SourceBackedTableDefinition,
    TableDefinition,
};

#[tokio::test]
async fn roundtrip_typed_rows() {
    let catalog = SlateCatalog::in_memory().await.expect("open catalog");

    let table = TableDefinition::new(
        "typed_rows",
        vec![
            ColumnDefinition::new_typed("name", ColumnType::Utf8, true),
            ColumnDefinition::new_typed("active", ColumnType::Bool, false),
            ColumnDefinition::new_typed("seen_at", ColumnType::TimestampMillis, false),
        ],
    )
    .unwrap();

    catalog.upsert_table(table.clone()).await.unwrap();

    let row = vec![
        RowValue::Utf8("alice".to_string()),
        RowValue::Bool(true),
        RowValue::TimestampMillis(1_700_000_000_000),
    ];
    catalog.insert_row(&table, &row).await.unwrap();

    let rows = catalog.read_rows(&table).await.unwrap();
    assert_eq!(rows, vec![row]);
}

#[tokio::test]
async fn persists_materialized_view_metadata_and_schema() {
    let catalog = SlateCatalog::in_memory().await.expect("open catalog");
    let metadata = MaterializedViewMetadata::new("mv_meta", "SELECT 1 AS value", false);
    catalog
        .upsert_materialized_view(metadata.clone())
        .await
        .expect("persist metadata");

    let loaded = catalog
        .materialized_view("mv_meta")
        .await
        .expect("load metadata")
        .expect("metadata exists");
    assert_eq!(loaded, metadata);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    catalog
        .save_materialized_view_schema("mv_meta", Arc::clone(&schema))
        .await
        .expect("persist schema");

    let loaded_schema = catalog
        .materialized_view_schema("mv_meta")
        .await
        .expect("load schema")
        .expect("schema exists");
    assert_eq!(loaded_schema.as_ref(), schema.as_ref());
}

#[tokio::test]
async fn roundtrip_catalog_sources_and_source_backed_tables() {
    let catalog = SlateCatalog::in_memory().await.expect("open catalog");
    let source = CatalogSourceDefinition::new(
        "pg_main",
        CatalogSourceConnector::PostgresCdc(
            PostgresCdcSourceDefinition::new(
                "postgres://postgres:postgres@localhost/postgres",
                "floe_slot",
                Some("floe_pub".to_string()),
                Some(false),
            )
            .expect("postgres source"),
        ),
    )
    .expect("source");
    catalog
        .upsert_catalog_source(source.clone())
        .await
        .expect("persist source");

    let loaded = catalog
        .catalog_source("pg_main")
        .await
        .expect("load source")
        .expect("source exists");
    assert_eq!(loaded, source);
    assert_eq!(catalog.catalog_sources().await.unwrap(), vec![source]);

    let binding =
        SourceBackedTableDefinition::new("orders", "pg_main", "public.orders").expect("binding");
    catalog
        .upsert_source_backed_table(binding.clone())
        .await
        .expect("persist binding");
    let loaded_binding = catalog
        .source_backed_table("orders")
        .await
        .expect("load binding")
        .expect("binding exists");
    assert_eq!(loaded_binding, binding);
    assert_eq!(catalog.source_backed_tables().await.unwrap(), vec![binding]);
}

#[tokio::test]
async fn roundtrip_replication_pipeline_and_checkpoint() {
    let catalog = SlateCatalog::in_memory().await.expect("open catalog");
    let pipeline = ReplicationPipelineDefinition::new(
        "pg_orders_to_kafka",
        "pg_main",
        "public.orders",
        ReplicationPipelineTarget::Kafka {
            brokers: "localhost:9092".to_string(),
            topic: "orders_cdc".to_string(),
        },
        ReplicationPipelineFormat::DebeziumJson,
        ReplicationBufferMode::Durable,
        floe_core::catalog::ReplicationBufferPolicy::default(),
        true,
        true,
        floe_core::catalog::ReplicationErrorPolicy::default(),
    )
    .expect("pipeline");
    catalog
        .upsert_replication_pipeline(pipeline.clone())
        .await
        .expect("persist pipeline");

    let loaded = catalog
        .replication_pipeline("pg_orders_to_kafka")
        .await
        .expect("load pipeline")
        .expect("pipeline exists");
    assert_eq!(loaded, pipeline);
    assert_eq!(
        catalog.replication_pipelines().await.unwrap(),
        vec![pipeline]
    );

    let mut target_state = BTreeMap::new();
    target_state.insert("kafka.topic".to_string(), "orders_cdc".to_string());
    target_state.insert("kafka.partition.0.offset".to_string(), "42".to_string());
    let checkpoint = ReplicationPipelineCheckpoint::new(
        "pg_orders_to_kafka",
        "pg_main",
        CdcSourcePosition::postgres("0/16B6C50", None).expect("position"),
        Some(CdcTransactionId::new("tx-7").expect("transaction")),
        target_state,
        1_700_000_000_000,
    )
    .expect("checkpoint");
    catalog
        .put_replication_pipeline_checkpoint(checkpoint.clone())
        .await
        .expect("persist checkpoint");
    let loaded_checkpoint = catalog
        .replication_pipeline_checkpoint("pg_orders_to_kafka")
        .await
        .expect("load checkpoint")
        .expect("checkpoint exists");
    assert_eq!(loaded_checkpoint, checkpoint);

    let dlq_payload = b"encoded failed records".to_vec();
    let dlq_payload_object_key = catalog
        .put_replication_pipeline_dlq_payload(
            "pg_orders_to_kafka",
            "0_16B6C50_tx_7",
            dlq_payload.clone(),
        )
        .await
        .expect("persist dlq payload");
    assert_eq!(
        catalog
            .replication_pipeline_dlq_payload(&dlq_payload_object_key)
            .await
            .expect("load dlq payload"),
        dlq_payload
    );
    let dlq_entry = ReplicationPipelineDlqEntry::new(
        "pg_orders_to_kafka",
        "0_16B6C50_tx_7",
        "pg_main",
        CdcSourcePosition::postgres("0/16B6C50", None).expect("position"),
        Some(CdcTransactionId::new("tx-7").expect("transaction")),
        "kafka_delivery",
        "broker unavailable",
        2,
        Some(dlq_payload_object_key),
        Some("kafka_records".to_string()),
        4096,
        BTreeMap::from([("kafka.topic".to_string(), "orders_cdc".to_string())]),
        1_700_000_000_001,
    )
    .expect("dlq entry");
    catalog
        .put_replication_pipeline_dlq_entry(dlq_entry.clone())
        .await
        .expect("persist dlq entry");

    let loaded_dlq_entry = catalog
        .replication_pipeline_dlq_entry("pg_orders_to_kafka", "0_16B6C50_tx_7")
        .await
        .expect("load dlq entry")
        .expect("dlq entry exists");
    assert_eq!(loaded_dlq_entry, dlq_entry);
    assert_eq!(
        catalog
            .replication_pipeline_dlq_entries("pg_orders_to_kafka")
            .await
            .unwrap(),
        vec![dlq_entry]
    );

    let updated_dlq_entry = catalog
        .update_replication_pipeline_dlq_entry_status(
            "pg_orders_to_kafka",
            "0_16B6C50_tx_7",
            ReplicationPipelineDlqStatus::Replayed,
            1_700_000_000_002,
        )
        .await
        .expect("update dlq status")
        .expect("dlq entry exists");
    assert_eq!(
        updated_dlq_entry.status(),
        ReplicationPipelineDlqStatus::Replayed
    );
    assert_eq!(
        updated_dlq_entry.last_updated_at_unix_ms(),
        1_700_000_000_002
    );
    assert_eq!(updated_dlq_entry.status_reason(), None);

    let attempted_dlq_entry = catalog
        .record_replication_pipeline_dlq_retry_attempt(
            "pg_orders_to_kafka",
            "0_16B6C50_tx_7",
            1_700_000_000_003,
        )
        .await
        .expect("record retry attempt")
        .expect("dlq entry exists");
    assert_eq!(attempted_dlq_entry.attempt_count(), 3);
    assert_eq!(
        attempted_dlq_entry.last_updated_at_unix_ms(),
        1_700_000_000_003
    );

    let discarded_dlq_entry = catalog
        .update_replication_pipeline_dlq_entry_status_with_reason(
            "pg_orders_to_kafka",
            "0_16B6C50_tx_7",
            ReplicationPipelineDlqStatus::Discarded,
            Some("operator skipped duplicate".to_string()),
            1_700_000_000_004,
        )
        .await
        .expect("discard dlq entry")
        .expect("dlq entry exists");
    assert_eq!(
        discarded_dlq_entry.status(),
        ReplicationPipelineDlqStatus::Discarded
    );
    assert_eq!(
        discarded_dlq_entry.status_reason(),
        Some("operator skipped duplicate")
    );
}

#[tokio::test]
async fn replication_pipeline_dlq_stats_count_statuses_without_loading_entries() {
    let catalog = SlateCatalog::in_memory().await.expect("open catalog");
    for (idx, created_at) in [(1, 1_000_u64), (2, 1_200_u64), (3, 1_500_u64)] {
        let entry = ReplicationPipelineDlqEntry::new(
            "pg_orders_to_kafka",
            format!("entry-{idx}"),
            "pg_main",
            CdcSourcePosition::postgres(&format!("0/{:X}", 0x16B6C50 + idx), None)
                .expect("position"),
            Some(CdcTransactionId::new(format!("tx-{idx}")).expect("transaction")),
            "target_delivery",
            "target unavailable",
            1,
            None,
            Some("kafka_records".to_string()),
            1024,
            BTreeMap::new(),
            created_at,
        )
        .expect("dlq entry");
        catalog
            .put_replication_pipeline_dlq_entry(entry)
            .await
            .expect("persist dlq entry");
    }
    catalog
        .update_replication_pipeline_dlq_entry_status(
            "pg_orders_to_kafka",
            "entry-2",
            ReplicationPipelineDlqStatus::Replayed,
            1_600,
        )
        .await
        .expect("replay entry");
    catalog
        .update_replication_pipeline_dlq_entry_status_with_reason(
            "pg_orders_to_kafka",
            "entry-3",
            ReplicationPipelineDlqStatus::Discarded,
            Some("duplicate".to_string()),
            1_700,
        )
        .await
        .expect("discard entry");

    let stats = catalog
        .replication_pipeline_dlq_stats("pg_orders_to_kafka", 2_000)
        .await
        .expect("dlq stats");

    assert_eq!(stats.pending_entries(), 1);
    assert_eq!(stats.replayed_entries(), 1);
    assert_eq!(stats.discarded_entries(), 1);
    assert_eq!(stats.oldest_pending_age_ms(), Some(1_000));
}

#[tokio::test]
async fn replication_pipeline_dlq_page_filters_orders_and_paginates() {
    let catalog = SlateCatalog::in_memory().await.expect("open catalog");
    for (dlq_id, created_at) in [
        ("entry-c", 1_300_u64),
        ("entry-a", 1_000_u64),
        ("entry-b", 1_200_u64),
        ("entry-d", 1_400_u64),
    ] {
        let entry = ReplicationPipelineDlqEntry::new(
            "pg_orders_to_kafka",
            dlq_id,
            "pg_main",
            CdcSourcePosition::postgres("0/16B6C50", None).expect("position"),
            Some(CdcTransactionId::new(format!("tx-{dlq_id}")).expect("transaction")),
            "target_delivery",
            "target unavailable",
            1,
            None,
            Some("kafka_records".to_string()),
            1024,
            BTreeMap::new(),
            created_at,
        )
        .expect("dlq entry");
        catalog
            .put_replication_pipeline_dlq_entry(entry)
            .await
            .expect("persist dlq entry");
    }
    catalog
        .update_replication_pipeline_dlq_entry_status(
            "pg_orders_to_kafka",
            "entry-b",
            ReplicationPipelineDlqStatus::Replayed,
            1_500,
        )
        .await
        .expect("replay entry");

    let page = catalog
        .replication_pipeline_dlq_entries_page("pg_orders_to_kafka", None, 1, 2, 2_000)
        .await
        .expect("dlq page");
    assert_eq!(page.total_matching(), 4);
    assert_eq!(page.oldest_pending_age_ms(), Some(1_000));
    assert_eq!(
        page.entries()
            .iter()
            .map(ReplicationPipelineDlqEntry::dlq_id)
            .collect::<Vec<_>>(),
        vec!["entry-b", "entry-c"]
    );

    let pending_page = catalog
        .replication_pipeline_dlq_entries_page(
            "pg_orders_to_kafka",
            Some(ReplicationPipelineDlqStatus::Pending),
            0,
            10,
            2_000,
        )
        .await
        .expect("pending page");
    assert_eq!(pending_page.total_matching(), 3);
    assert_eq!(pending_page.oldest_pending_age_ms(), Some(1_000));
    assert_eq!(
        pending_page
            .entries()
            .iter()
            .map(ReplicationPipelineDlqEntry::dlq_id)
            .collect::<Vec<_>>(),
        vec!["entry-a", "entry-c", "entry-d"]
    );
}

#[tokio::test]
async fn roundtrip_postgres_replication_pipeline_target() {
    let catalog = SlateCatalog::in_memory().await.expect("open catalog");
    let pipeline = ReplicationPipelineDefinition::new(
        "pg_orders_to_postgres",
        "pg_main",
        "public.orders",
        ReplicationPipelineTarget::Postgres {
            connection: "postgres://postgres:postgres@localhost/postgres".to_string(),
            table: "public.orders_copy".to_string(),
        },
        ReplicationPipelineFormat::FloeJson,
        ReplicationBufferMode::Durable,
        floe_core::catalog::ReplicationBufferPolicy::default(),
        false,
        false,
        floe_core::catalog::ReplicationErrorPolicy::default(),
    )
    .expect("pipeline");

    catalog
        .upsert_replication_pipeline(pipeline.clone())
        .await
        .expect("persist pipeline");

    let loaded = catalog
        .replication_pipeline("pg_orders_to_postgres")
        .await
        .expect("load pipeline")
        .expect("pipeline exists");
    assert_eq!(loaded, pipeline);
}

#[tokio::test]
async fn roundtrip_table_definitions() {
    let catalog = SlateCatalog::in_memory().await.expect("open catalog");

    let table = TableDefinition::new(
        "stream",
        vec![
            ColumnDefinition::new("id", true),
            ColumnDefinition::new("value", false),
        ],
    )
    .unwrap();

    catalog.upsert_table(table.clone()).await.unwrap();

    let loaded = catalog.table("stream").await.unwrap().unwrap();
    assert_eq!(loaded.name(), "stream");
    assert_eq!(loaded.columns().len(), 2);

    catalog
        .insert_row(&table, &vec![RowValue::Int64(1), RowValue::Int64(10)])
        .await
        .unwrap();
    catalog
        .insert_row(&table, &vec![RowValue::Int64(2), RowValue::Int64(20)])
        .await
        .unwrap();

    let rows = catalog.read_rows(&table).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&vec![RowValue::Int64(1), RowValue::Int64(10)]));
    assert!(rows.contains(&vec![RowValue::Int64(2), RowValue::Int64(20)]));
}

#[tokio::test]
async fn close_is_idempotent() {
    let catalog = SlateCatalog::in_memory().await.expect("open catalog");
    catalog.close().await.expect("close catalog");
    catalog.close().await.expect("close catalog again");
}
