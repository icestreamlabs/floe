use anyhow::Context;
use floe_cdc_core::{CdcSourcePosition, CdcTransactionId};
use floe_storage::{
    CdcBufferRecord, ReplicationPipelineDlqEntry, SlateCatalog, encode_cdc_buffer_records_payload,
};

use super::super::ReplicationPipelineRuntimePlan;
use super::target_state::{dead_letter_target_state, replication_pipeline_dlq_id, target_kind};
use super::{current_unix_time_ms, encoding};

pub(super) async fn persist_dead_letter_records(
    plan: &ReplicationPipelineRuntimePlan,
    storage: &SlateCatalog,
    source_position: &CdcSourcePosition,
    transaction_id: Option<&CdcTransactionId>,
    records: &[CdcBufferRecord],
    err: &anyhow::Error,
) -> anyhow::Result<ReplicationPipelineDlqEntry> {
    let dlq_id = replication_pipeline_dlq_id(source_position, transaction_id);
    let payload = encode_cdc_buffer_records_payload(records)
        .context("encode replication pipeline DLQ payload")?;
    let payload_bytes = payload.len();
    let payload_object_key = storage
        .put_replication_pipeline_dlq_payload(&plan.name, &dlq_id, payload)
        .await
        .with_context(|| {
            format!(
                "persist replication pipeline '{}' dead-letter payload",
                plan.name
            )
        })?;
    let entry = ReplicationPipelineDlqEntry::new(
        &plan.name,
        dlq_id,
        &plan.source_name,
        source_position.clone(),
        transaction_id.cloned(),
        format!("{}_delivery", target_kind(plan)),
        format!("{err:#}"),
        1,
        Some(payload_object_key),
        Some("kafka_records".to_string()),
        payload_bytes,
        dead_letter_target_state(plan, err),
        current_unix_time_ms(),
    )?;
    storage
        .put_replication_pipeline_dlq_entry(entry.clone())
        .await
        .with_context(|| {
            format!(
                "persist replication pipeline '{}' dead-letter entry",
                plan.name
            )
        })?;
    tracing::warn!(
        pipeline = %plan.name,
        source = %plan.source_name,
        target_kind = target_kind(plan),
        dlq_id = %entry.dlq_id(),
        records = records.len(),
        payload_bytes = entry.payload_bytes(),
        source_position = %encoding::source_position_key(source_position),
        transaction_id = transaction_id.map(CdcTransactionId::as_str),
        error = %err,
        "replication pipeline target write dead-lettered"
    );
    Ok(entry)
}
