use super::encoding::{
    encode_debezium_pipeline_records, encode_pipeline_buffer_records,
    encode_pipeline_transaction_records, encode_pipeline_transaction_records_with_metadata,
};
use super::reconciliation::*;
use super::target_state::{
    TargetFailureClass, classify_target_write_failure, delivered_target_state, failed_target_state,
};
use super::writers::{
    PostgresParamValue, PostgresReplicationPipelineWriter, PostgresTargetColumnInfo,
    PostgresTargetTableInfo, parse_floe_json_record_key, parse_floe_json_record_value,
    postgres_delete_sql_with_target, postgres_key_params_from_json, postgres_row_params_from_json,
    postgres_upsert_sql_with_target, validate_postgres_target_table_compatibility,
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

mod buffer_limits;
mod durable_pipelines;
mod encoding;
mod status_reconciliation;
fn kafka_record_key_json(record: &CdcBufferRecord) -> serde_json::Value {
    serde_json::from_slice(record.key().expect("key")).expect("decode key JSON")
}

fn kafka_record_value_json(record: &CdcBufferRecord) -> Option<serde_json::Value> {
    record
        .value()
        .map(|value| serde_json::from_slice(value).expect("decode value JSON"))
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
        source_connection: "postgres://floe:secret@localhost/postgres".to_string(),
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
    test_runtime_with_plans(vec![plan])
}

fn test_runtime_with_plans(
    plans: Vec<ReplicationPipelineRuntimePlan>,
) -> ReplicationPipelineRuntime {
    let mut pipelines_by_source: HashMap<CdcSourceId, Vec<ReplicationPipelineRuntimePlan>> =
        HashMap::new();
    for plan in plans {
        pipelines_by_source
            .entry(CdcSourceId::new(plan.source_name.clone()).unwrap())
            .or_default()
            .push(plan);
    }
    ReplicationPipelineRuntime {
        pipelines_by_source,
        kafka_writers_by_pipeline: HashMap::new(),
        postgres_writers_by_pipeline: HashMap::new(),
        buffer_cleanup_last_by_pipeline: Mutex::new(HashMap::new()),
        integrity_report_cache_by_pipeline: Mutex::new(HashMap::new()),
        replay_state_by_pipeline: Mutex::new(HashMap::new()),
        backpressure_state_by_pipeline: Mutex::new(HashMap::new()),
        last_target_error_by_pipeline: Mutex::new(HashMap::new()),
        settings: FloeReplicationConfig::default(),
    }
}

async fn persist_test_dlq_entry(
    storage: &SlateCatalog,
    plan: &ReplicationPipelineRuntimePlan,
    dlq_id: &str,
    lsn: &str,
    transaction_id: &str,
    created_at_unix_ms: u64,
) -> anyhow::Result<ReplicationPipelineDlqEntry> {
    let payload = floe_storage::encode_cdc_buffer_records_payload(&[CdcBufferRecord::new(
        Some(br#"{"id":1}"#.to_vec()),
        Some(br#"{"id":1,"status":"open"}"#.to_vec()),
    )])?;
    let payload_bytes = payload.len();
    let payload_object_key = storage
        .put_replication_pipeline_dlq_payload(&plan.name, dlq_id, payload)
        .await?;
    let entry = ReplicationPipelineDlqEntry::new(
        &plan.name,
        dlq_id,
        &plan.source_name,
        floe_cdc_core::CdcSourcePosition::postgres(lsn, None)?,
        Some(CdcTransactionId::new(transaction_id)?),
        "kafka_delivery",
        "broker unavailable",
        1,
        Some(payload_object_key),
        Some("kafka_records".to_string()),
        payload_bytes,
        BTreeMap::new(),
        created_at_unix_ms,
    )?;
    storage
        .put_replication_pipeline_dlq_entry(entry.clone())
        .await?;
    Ok(entry)
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
