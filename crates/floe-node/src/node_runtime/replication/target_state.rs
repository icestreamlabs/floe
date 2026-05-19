use std::collections::BTreeMap;

use floe_cdc_core::{CdcSourcePosition, CdcTransactionId, TransactionBatch};
use floe_core::catalog::ReplicationErrorPolicyMode as CatalogReplicationErrorPolicyMode;
use floe_storage::{
    CdcBufferPayloadFormat, CdcBufferedTransactionManifest, ReplicationPipelineDlqEntry,
};

use super::super::{ReplicationPipelineRuntimePlan, ReplicationPipelineRuntimeTarget};
use super::{current_unix_time_ms, encoding};

pub(super) fn pending_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
) -> BTreeMap<String, String> {
    let mut state = base_target_state(plan, manifest);
    state.insert("buffer.status".to_string(), "durable".to_string());
    state.insert("target.delivery.status".to_string(), "pending".to_string());
    state.insert(
        "target.delivery.replay_may_duplicate".to_string(),
        "true".to_string(),
    );
    state
}

pub(super) fn delivered_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    mut target_state: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    target_state.extend(base_target_state(plan, manifest));
    target_state.insert("buffer.status".to_string(), "delivered".to_string());
    target_state.insert(
        "target.delivery.status".to_string(),
        "delivered".to_string(),
    );
    target_state.insert(
        "target.delivery.replay_may_duplicate".to_string(),
        "false".to_string(),
    );
    target_state
}

pub(super) fn failed_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    err: &anyhow::Error,
) -> BTreeMap<String, String> {
    let mut state = base_target_state(plan, manifest);
    state.insert("buffer.status".to_string(), "durable".to_string());
    state.insert("target.delivery.status".to_string(), "failed".to_string());
    state.insert(
        "target.delivery.replay_may_duplicate".to_string(),
        "true".to_string(),
    );
    state.insert(
        "target.last_error".to_string(),
        truncate_target_error(&format!("{err:#}")),
    );
    state
}

pub(super) fn dead_lettered_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    dlq_entry: &ReplicationPipelineDlqEntry,
    err: &anyhow::Error,
) -> BTreeMap<String, String> {
    let mut state = base_target_state(plan, manifest);
    state.insert("buffer.status".to_string(), "dead_lettered".to_string());
    add_dead_letter_state(&mut state, dlq_entry, err);
    state
}

pub(super) fn direct_delivered_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    record_count: usize,
    payload_format: CdcBufferPayloadFormat,
    mut target_state: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    target_state.insert("source.table".to_string(), plan.upstream_table.clone());
    target_state.insert("target.kind".to_string(), target_kind(plan).to_string());
    if let Some(transaction_id) = transaction.transaction_id() {
        target_state.insert(
            "source.transaction_id".to_string(),
            transaction_id.as_str().to_string(),
        );
    }
    match transaction.commit_position() {
        CdcSourcePosition::Postgres {
            commit_lsn,
            event_lsn,
        } => {
            target_state.insert(
                "source.position.postgres.commit_lsn".to_string(),
                commit_lsn.clone(),
            );
            if let Some(event_lsn) = event_lsn {
                target_state.insert(
                    "source.position.postgres.event_lsn".to_string(),
                    event_lsn.clone(),
                );
            }
        }
        CdcSourcePosition::Opaque { value } => {
            target_state.insert("source.position".to_string(), value.clone());
        }
    }
    target_state.insert("buffer.status".to_string(), "not_buffered".to_string());
    target_state.insert("buffer.record_count".to_string(), record_count.to_string());
    target_state.insert(
        "buffer.payload_format".to_string(),
        format!("{payload_format:?}"),
    );
    target_state.insert(
        "target.delivery.status".to_string(),
        "delivered".to_string(),
    );
    target_state.insert(
        "target.delivery.replay_may_duplicate".to_string(),
        "false".to_string(),
    );
    target_state
}

pub(super) fn direct_dead_lettered_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    record_count: usize,
    payload_format: CdcBufferPayloadFormat,
    dlq_entry: &ReplicationPipelineDlqEntry,
    err: &anyhow::Error,
) -> BTreeMap<String, String> {
    let mut state = direct_delivered_target_state(
        plan,
        transaction,
        record_count,
        payload_format,
        BTreeMap::new(),
    );
    add_dead_letter_state(&mut state, dlq_entry, err);
    state
}

fn add_dead_letter_state(
    state: &mut BTreeMap<String, String>,
    dlq_entry: &ReplicationPipelineDlqEntry,
    err: &anyhow::Error,
) {
    state.insert(
        "target.delivery.status".to_string(),
        "dead_lettered".to_string(),
    );
    state.insert(
        "target.delivery.replay_may_duplicate".to_string(),
        "false".to_string(),
    );
    state.insert("target.dlq.id".to_string(), dlq_entry.dlq_id().to_string());
    state.insert(
        "target.dlq.status".to_string(),
        dlq_entry.status().as_str().to_string(),
    );
    if let Some(payload_object_key) = dlq_entry.payload_object_key() {
        state.insert(
            "target.dlq.payload_object_key".to_string(),
            payload_object_key.to_string(),
        );
    }
    state.insert(
        "target.last_error".to_string(),
        truncate_target_error(&format!("{err:#}")),
    );
}

pub(super) fn dead_letter_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    err: &anyhow::Error,
) -> BTreeMap<String, String> {
    let mut state = BTreeMap::new();
    state.insert("target.kind".to_string(), target_kind(plan).to_string());
    state.insert(
        "target.delivery.status".to_string(),
        "dead_lettered".to_string(),
    );
    state.insert(
        "target.last_error".to_string(),
        truncate_target_error(&format!("{err:#}")),
    );
    state
}

fn base_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
) -> BTreeMap<String, String> {
    let mut state = BTreeMap::new();
    state.insert("source.table".to_string(), plan.upstream_table.clone());
    state.insert("target.kind".to_string(), target_kind(plan).to_string());
    state.insert(
        "buffer.transaction_key".to_string(),
        manifest.transaction_key().to_string(),
    );
    state.insert(
        "buffer.record_count".to_string(),
        manifest.record_count().to_string(),
    );
    state.insert(
        "buffer.payload_format".to_string(),
        format!("{:?}", manifest.payload_format()),
    );
    if let Some(transaction_id) = manifest.transaction_id() {
        state.insert(
            "source.transaction_id".to_string(),
            transaction_id.as_str().to_string(),
        );
    }
    match manifest.source_position() {
        CdcSourcePosition::Postgres {
            commit_lsn,
            event_lsn,
        } => {
            state.insert(
                "source.position.postgres.commit_lsn".to_string(),
                commit_lsn.clone(),
            );
            if let Some(event_lsn) = event_lsn {
                state.insert(
                    "source.position.postgres.event_lsn".to_string(),
                    event_lsn.clone(),
                );
            }
        }
        CdcSourcePosition::Opaque { value } => {
            state.insert("source.position".to_string(), value.clone());
        }
    }
    state
}

pub(super) fn target_kind(plan: &ReplicationPipelineRuntimePlan) -> &'static str {
    match &plan.target {
        ReplicationPipelineRuntimeTarget::Kafka { .. } => "kafka",
        ReplicationPipelineRuntimeTarget::Postgres { .. } => "postgres",
    }
}

pub(super) fn replication_pipeline_uses_dlq(plan: &ReplicationPipelineRuntimePlan) -> bool {
    plan.error_policy.mode() == CatalogReplicationErrorPolicyMode::DeadLetterAndContinue
}

pub(super) fn replication_pipeline_dlq_id(
    source_position: &CdcSourcePosition,
    transaction_id: Option<&CdcTransactionId>,
) -> String {
    let position = encoding::source_position_key(source_position);
    let transaction = transaction_id.map_or("none", CdcTransactionId::as_str);
    format!(
        "{}-{}-{}",
        hex_component(position.as_bytes()),
        hex_component(transaction.as_bytes()),
        current_unix_time_ms()
    )
}

fn hex_component(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0F) as usize] as char);
    }
    out
}

pub(super) fn truncate_target_error(message: &str) -> String {
    const MAX_ERROR_LEN: usize = 512;
    if message.len() <= MAX_ERROR_LEN {
        return message.to_string();
    }
    let mut truncated = message
        .chars()
        .take(MAX_ERROR_LEN.saturating_sub(3))
        .collect::<String>();
    truncated.push_str("...");
    truncated
}
