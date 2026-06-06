use std::time::{Duration, Instant};

use anyhow::Context;
use floe_cdc_core::CdcTransactionId;
use floe_storage::{
    CdcBufferPayloadFormat, CdcBufferRecord, CdcBufferStore, CdcBufferedTransactionManifest,
    SlateCatalog,
};

use super::super::{ReplicationPipelineRuntimeFormat, ReplicationPipelineRuntimePlan};
use super::buffer::record_buffer_stats;
use super::perf::{log_replication_replay_delivery_perf, log_replication_replay_payload_perf};
use super::runtime_state::ReplicationReplayStateGuard;
use super::target_state::target_kind;
use super::{ReplicationPipelineRuntime, encoding};
use tokio_util::sync::CancellationToken;

const REPLICATION_BUFFER_REPLAY_LIMIT: usize = 1024;

impl ReplicationPipelineRuntime {
    pub(super) async fn replay_pending_for_plan(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        buffer_store: &CdcBufferStore,
        storage: &SlateCatalog,
        cancel: &CancellationToken,
    ) -> anyhow::Result<usize> {
        let _replay_guard = ReplicationReplayStateGuard::new(self, &plan.name);
        let mut delivered_records = 0usize;
        let pending = buffer_store
            .pending_transactions(&plan.name, REPLICATION_BUFFER_REPLAY_LIMIT)
            .await
            .with_context(|| {
                format!(
                    "load pending replication pipeline '{}' buffer transactions",
                    plan.name
                )
            })?;
        let pending_transactions = pending.len();
        if pending_transactions > 0 {
            tracing::info!(
                pipeline = %plan.name,
                source = %plan.source_name,
                target_kind = target_kind(plan),
                pending_transactions,
                replay_limit = REPLICATION_BUFFER_REPLAY_LIMIT,
                "replication pipeline durable buffer replay started"
            );
        }
        let mut attempted_transactions = 0usize;
        let mut delivered_transactions = 0usize;
        for manifest in pending {
            attempted_transactions = attempted_transactions.saturating_add(1);
            let replay_started_at = Instant::now();
            let records =
                load_manifest_records(plan, buffer_store, &manifest, self.settings.perf_log)
                    .await?;
            let delivery_started_at = Instant::now();
            let delivered = self
                .deliver_manifest_records(plan, buffer_store, storage, &manifest, &records, cancel)
                .await?;
            let delivery_elapsed = delivery_started_at.elapsed();
            crate::metrics::observe_cdc_buffer_replay_phase_latency_ms(
                &plan.name,
                "target_delivery",
                delivery_elapsed.as_millis() as u64,
            );
            crate::metrics::record_cdc_buffer_replay(
                &plan.name,
                delivered,
                replay_started_at.elapsed().as_millis() as u64,
            );
            log_replication_replay_delivery_perf(
                self.settings.perf_log,
                plan,
                &manifest,
                delivery_elapsed,
                delivered,
            );
            if delivered == 0 {
                tracing::warn!(
                    pipeline = %plan.name,
                    source = %plan.source_name,
                    target_kind = target_kind(plan),
                    transaction_key = %manifest.transaction_key(),
                    records = manifest.record_count(),
                    payload_bytes = manifest.payload_bytes(),
                    source_position = %encoding::source_position_key(manifest.source_position()),
                    transaction_id = manifest.transaction_id().map(CdcTransactionId::as_str),
                    "replication pipeline durable buffer replay paused because target delivery made no progress"
                );
                break;
            }
            delivered_transactions = delivered_transactions.saturating_add(1);
            delivered_records = delivered_records.saturating_add(delivered);
            self.spawn_cleanup_delivered_if_due(plan, buffer_store);
        }
        if pending_transactions > 0 {
            tracing::info!(
                pipeline = %plan.name,
                source = %plan.source_name,
                target_kind = target_kind(plan),
                pending_transactions,
                attempted_transactions,
                delivered_transactions,
                delivered_records,
                replay_exhausted = attempted_transactions == pending_transactions,
                "replication pipeline durable buffer replay finished"
            );
        }
        record_buffer_stats(buffer_store, &plan.name).await?;
        Ok(delivered_records)
    }
}

pub(super) async fn load_manifest_records(
    plan: &ReplicationPipelineRuntimePlan,
    buffer_store: &CdcBufferStore,
    manifest: &CdcBufferedTransactionManifest,
    perf_enabled: bool,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    let payload_load_started_at = Instant::now();
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
            let payload_load_elapsed = payload_load_started_at.elapsed();
            crate::metrics::observe_cdc_buffer_replay_phase_latency_ms(
                &plan.name,
                "payload_load",
                payload_load_elapsed.as_millis() as u64,
            );
            let encode_started_at = Instant::now();
            let mut records =
                encoding::encode_floe_json_buffered_change_batches(plan, &plan.schema, &batches)?;
            let encode_elapsed = encode_started_at.elapsed();
            crate::metrics::observe_cdc_buffer_replay_phase_latency_ms(
                &plan.name,
                "encode",
                encode_elapsed.as_millis() as u64,
            );
            encoding::add_replication_record_metadata(
                plan,
                manifest.source_position(),
                manifest.transaction_id(),
                &mut records,
                0,
            );
            log_replication_replay_payload_perf(
                perf_enabled,
                plan,
                manifest,
                payload_load_elapsed,
                encode_elapsed,
                records.len(),
            );
            records
        }
    };
    if manifest.payload_format() == CdcBufferPayloadFormat::KafkaRecords {
        let payload_load_elapsed = payload_load_started_at.elapsed();
        crate::metrics::observe_cdc_buffer_replay_phase_latency_ms(
            &plan.name,
            "payload_load",
            payload_load_elapsed.as_millis() as u64,
        );
        log_replication_replay_payload_perf(
            perf_enabled,
            plan,
            manifest,
            payload_load_elapsed,
            Duration::ZERO,
            records.len(),
        );
    }
    Ok(records)
}

fn record_object_store_get(
    plan: &ReplicationPipelineRuntimePlan,
    _manifest: &CdcBufferedTransactionManifest,
) {
    crate::metrics::inc_cdc_buffer_object_op(&plan.name, "get", 1);
}
