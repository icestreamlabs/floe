use std::time::{Duration, Instant};

use anyhow::Context;
use floe_storage::{
    CdcBufferPayloadFormat, CdcBufferPayloadStorage, CdcBufferRecord, CdcBufferStore,
    CdcBufferedTransactionManifest,
};

use super::super::{ReplicationPipelineRuntimeFormat, ReplicationPipelineRuntimePlan};
use super::perf::log_replication_replay_payload_perf;
use super::{CDC_PERF_LOGGING_ENABLED, encoding};

pub(super) async fn load_manifest_records(
    plan: &ReplicationPipelineRuntimePlan,
    buffer_store: &CdcBufferStore,
    manifest: &CdcBufferedTransactionManifest,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    let payload_load_started_at = CDC_PERF_LOGGING_ENABLED.then(Instant::now);
    let records = match manifest.payload_format() {
        CdcBufferPayloadFormat::KafkaRecords => {
            let records = buffer_store.records(manifest).await.with_context(|| {
                format!(
                    "load replication pipeline '{}' buffered payloads",
                    plan.name
                )
            })?;
            record_object_store_get(plan, manifest);
            records
        }
        CdcBufferPayloadFormat::ChangeBatches => {
            anyhow::ensure!(
                plan.format == ReplicationPipelineRuntimeFormat::FloeJson,
                "replication pipeline '{}' cannot replay change batch buffer payloads for {:?}",
                plan.name,
                plan.format
            );
            let batches = buffer_store
                .change_batches(manifest)
                .await
                .with_context(|| {
                    format!(
                        "load replication pipeline '{}' buffered change batches",
                        plan.name
                    )
                })?;
            record_object_store_get(plan, manifest);
            let payload_load_elapsed = elapsed_or_zero(payload_load_started_at);
            let encode_started_at = CDC_PERF_LOGGING_ENABLED.then(Instant::now);
            let mut records =
                encoding::encode_floe_json_buffered_change_batches(plan, &plan.schema, &batches)?;
            encoding::add_replication_record_metadata(
                plan,
                manifest.source_position(),
                manifest.transaction_id(),
                &mut records,
                0,
            );
            log_replication_replay_payload_perf(
                plan,
                manifest,
                payload_load_elapsed,
                elapsed_or_zero(encode_started_at),
                records.len(),
            );
            records
        }
    };
    if manifest.payload_format() == CdcBufferPayloadFormat::KafkaRecords {
        log_replication_replay_payload_perf(
            plan,
            manifest,
            elapsed_or_zero(payload_load_started_at),
            Duration::ZERO,
            records.len(),
        );
    }
    Ok(records)
}

fn record_object_store_get(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
) {
    if manifest.payload_storage() == CdcBufferPayloadStorage::ObjectStore {
        crate::metrics::inc_cdc_buffer_object_op(&plan.name, "get", 1);
    }
}

fn elapsed_or_zero(started_at: Option<Instant>) -> Duration {
    started_at
        .map(|started_at| started_at.elapsed())
        .unwrap_or(Duration::ZERO)
}
