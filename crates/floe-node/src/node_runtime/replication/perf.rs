use std::time::Duration;

use floe_cdc_core::{ChangeBatch, TransactionBatch};
use floe_storage::{CdcBufferPayloadFormat, CdcBufferRecord, CdcBufferedTransactionManifest};

use super::super::ReplicationPipelineRuntimePlan;

pub(super) fn log_replication_pipeline_perf(
    perf_enabled: bool,
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    records: usize,
    payload_bytes: usize,
    encode_elapsed: Duration,
    total_elapsed: Duration,
) {
    if !perf_enabled {
        return;
    }
    let changes = transaction
        .change_batches()
        .iter()
        .map(ChangeBatch::change_count)
        .sum::<usize>();
    tracing::info!(
        pipeline = %plan.name,
        source = %transaction.source_id().as_str(),
        table = %plan.table_id.as_str(),
        upstream_table = %plan.upstream_table,
        format = ?plan.format,
        buffer_mode = ?plan.buffer_mode,
        error_policy = %plan.error_policy.mode().as_str(),
        change_batches = transaction.change_batches().len(),
        changes,
        records,
        payload_bytes,
        encode_ms = encode_elapsed.as_millis() as u64,
        total_ms = total_elapsed.as_millis() as u64,
        commit_position = ?transaction.commit_position(),
        "postgres cdc replication pipeline transaction processed"
    );
}

pub(super) fn log_replication_direct_delivery_perf(
    perf_enabled: bool,
    plan: &ReplicationPipelineRuntimePlan,
    records: usize,
    payload_format: CdcBufferPayloadFormat,
    payload_bytes: usize,
    target_send_elapsed: Duration,
    checkpoint_elapsed: Duration,
) {
    if !perf_enabled {
        return;
    }
    tracing::info!(
        pipeline = %plan.name,
        records,
        buffer_payload_format = ?payload_format,
        buffer_payload_bytes = payload_bytes,
        target_send_ms = target_send_elapsed.as_millis() as u64,
        delivery_checkpoint_ms = checkpoint_elapsed.as_millis() as u64,
        "postgres cdc durable replication pipeline direct delivery completed"
    );
}

pub(super) fn log_replication_kafka_send_perf(
    perf_enabled: bool,
    topic: &str,
    records: &[CdcBufferRecord],
    partition_offsets: usize,
    enqueue_elapsed: Duration,
    delivery_wait_elapsed: Duration,
    total_elapsed: Duration,
) {
    if !perf_enabled {
        return;
    }
    let key_bytes = records
        .iter()
        .map(|record| record.key().map_or(0, <[u8]>::len))
        .sum::<usize>();
    let value_bytes = records
        .iter()
        .map(|record| record.value().map_or(0, <[u8]>::len))
        .sum::<usize>();
    tracing::info!(
        topic,
        records = records.len(),
        key_bytes,
        value_bytes,
        partition_offsets,
        enqueue_ms = enqueue_elapsed.as_millis() as u64,
        delivery_wait_ms = delivery_wait_elapsed.as_millis() as u64,
        total_ms = total_elapsed.as_millis() as u64,
        "postgres cdc replication Kafka target send completed"
    );
}

pub(super) fn log_replication_buffer_append_perf(
    perf_enabled: bool,
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    append_elapsed: Duration,
) {
    if !perf_enabled {
        return;
    }
    tracing::info!(
        pipeline = %plan.name,
        records = manifest.record_count(),
        buffer_payload_format = ?manifest.payload_format(),
        buffer_payload_bytes = manifest.payload_bytes(),
        append_ms = append_elapsed.as_millis() as u64,
        "postgres cdc durable replication buffer append completed"
    );
}

pub(super) fn log_replication_replay_payload_perf(
    perf_enabled: bool,
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    payload_load_elapsed: Duration,
    encode_elapsed: Duration,
    records: usize,
) {
    if !perf_enabled {
        return;
    }
    tracing::info!(
        pipeline = %plan.name,
        records,
        buffer_payload_format = ?manifest.payload_format(),
        buffer_payload_bytes = manifest.payload_bytes(),
        load_ms = payload_load_elapsed.as_millis() as u64,
        encode_ms = encode_elapsed.as_millis() as u64,
        "postgres cdc durable replication payload replay prepared"
    );
}

pub(super) fn log_replication_replay_delivery_perf(
    perf_enabled: bool,
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    delivery_elapsed: Duration,
    delivered_records: usize,
) {
    if !perf_enabled {
        return;
    }
    tracing::info!(
        pipeline = %plan.name,
        delivered_records,
        records = manifest.record_count(),
        buffer_payload_format = ?manifest.payload_format(),
        delivery_ms = delivery_elapsed.as_millis() as u64,
        "postgres cdc durable replication replay delivery completed"
    );
}
