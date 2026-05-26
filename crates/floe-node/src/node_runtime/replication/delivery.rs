use anyhow::{Context, anyhow};
use floe_storage::{
    CdcBufferRecord, CdcBufferStore, CdcBufferedTransactionManifest, ReplicationPipelineCheckpoint,
    SlateCatalog,
};

use super::super::{ReplicationPipelineRuntimePlan, ReplicationPipelineRuntimeTarget};
use super::dead_letter::persist_dead_letter_records;
use super::target_state::{
    classify_target_write_failure, dead_lettered_target_state, delivered_target_state,
    failed_target_state, replication_pipeline_uses_dlq, target_kind,
};
use super::{ReplicationPipelineRuntime, current_unix_time_ms};

impl ReplicationPipelineRuntime {
    pub(super) async fn deliver_manifest_records(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        buffer_store: &CdcBufferStore,
        storage: &SlateCatalog,
        manifest: &CdcBufferedTransactionManifest,
        records: &[CdcBufferRecord],
    ) -> anyhow::Result<usize> {
        match self.send_records_to_target(plan, records).await {
            Ok(target_state) => {
                self.mark_manifest_delivered(plan, buffer_store, storage, manifest, target_state)
                    .await
            }
            Err(err) => {
                if replication_pipeline_uses_dlq(plan) {
                    self.record_target_write_failure(plan, &err);
                    return self
                        .mark_manifest_dead_lettered(
                            plan,
                            buffer_store,
                            storage,
                            manifest,
                            records,
                            &err,
                        )
                        .await;
                }
                self.mark_manifest_delivery_failed(plan, storage, manifest, err)
                    .await?;
                Ok(0)
            }
        }
    }

    pub(super) async fn send_records_to_target(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        records: &[CdcBufferRecord],
    ) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
        match &plan.target {
            ReplicationPipelineRuntimeTarget::Kafka { .. } => {
                let writer = self
                    .kafka_writers_by_pipeline
                    .get(&plan.name)
                    .ok_or_else(|| {
                        anyhow!("replication pipeline '{}' has no Kafka writer", plan.name)
                    })?;
                writer.send_records(records).await
            }
            ReplicationPipelineRuntimeTarget::Postgres { .. } => {
                let writer = self
                    .postgres_writers_by_pipeline
                    .get(&plan.name)
                    .ok_or_else(|| {
                        anyhow!(
                            "replication pipeline '{}' has no Postgres writer",
                            plan.name
                        )
                    })?;
                writer.send_records(records).await
            }
        }
    }

    pub(super) async fn mark_manifest_delivered(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        buffer_store: &CdcBufferStore,
        storage: &SlateCatalog,
        manifest: &CdcBufferedTransactionManifest,
        target_state: std::collections::BTreeMap<String, String>,
    ) -> anyhow::Result<usize> {
        let delivered_at = current_unix_time_ms();
        buffer_store
            .mark_delivered_without_durable_wait(manifest, delivered_at)
            .await
            .with_context(|| {
                format!(
                    "mark replication pipeline '{}' buffered transaction delivered",
                    plan.name
                )
            })?;
        self.clear_last_target_error(&plan.name);
        storage
            .put_replication_pipeline_checkpoint_without_durable_wait(
                ReplicationPipelineCheckpoint::new(
                    &plan.name,
                    &plan.source_name,
                    manifest.source_position().clone(),
                    manifest.transaction_id().cloned(),
                    delivered_target_state(plan, manifest, target_state),
                    delivered_at,
                )?,
            )
            .await
            .with_context(|| {
                format!(
                    "persist replication pipeline '{}' delivery checkpoint",
                    plan.name
                )
            })?;
        Ok(manifest.record_count())
    }

    pub(super) async fn mark_manifest_delivery_failed(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        storage: &SlateCatalog,
        manifest: &CdcBufferedTransactionManifest,
        err: anyhow::Error,
    ) -> anyhow::Result<()> {
        self.record_target_write_failure(plan, &err);
        storage
            .put_replication_pipeline_checkpoint_without_durable_wait(
                ReplicationPipelineCheckpoint::new(
                    &plan.name,
                    &plan.source_name,
                    manifest.source_position().clone(),
                    manifest.transaction_id().cloned(),
                    failed_target_state(plan, manifest, &err),
                    current_unix_time_ms(),
                )?,
            )
            .await
            .with_context(|| {
                format!(
                    "persist replication pipeline '{}' failed delivery checkpoint",
                    plan.name
                )
            })?;
        Ok(())
    }

    async fn mark_manifest_dead_lettered(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        buffer_store: &CdcBufferStore,
        storage: &SlateCatalog,
        manifest: &CdcBufferedTransactionManifest,
        records: &[CdcBufferRecord],
        err: &anyhow::Error,
    ) -> anyhow::Result<usize> {
        let dlq_entry = persist_dead_letter_records(
            plan,
            storage,
            manifest.source_position(),
            manifest.transaction_id(),
            records,
            err,
        )
        .await?;
        let dead_lettered_at = current_unix_time_ms();
        buffer_store
            .mark_delivered(manifest, dead_lettered_at)
            .await
            .with_context(|| {
                format!(
                    "mark replication pipeline '{}' buffered transaction dead-lettered",
                    plan.name
                )
            })?;
        storage
            .put_replication_pipeline_checkpoint(ReplicationPipelineCheckpoint::new(
                &plan.name,
                &plan.source_name,
                manifest.source_position().clone(),
                manifest.transaction_id().cloned(),
                dead_lettered_target_state(plan, manifest, &dlq_entry, err),
                dead_lettered_at,
            )?)
            .await
            .with_context(|| {
                format!(
                    "persist replication pipeline '{}' dead-letter checkpoint",
                    plan.name
                )
            })?;
        Ok(manifest.record_count())
    }

    pub(super) fn record_target_write_failure(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        err: &anyhow::Error,
    ) {
        let failure_class = classify_target_write_failure(plan, err);
        self.set_last_target_error(&plan.name, format!("{err:#}"));
        crate::metrics::inc_cdc_replication_target_failure(
            &plan.name,
            target_kind(plan),
            failure_class.as_str(),
        );
        match &plan.target {
            ReplicationPipelineRuntimeTarget::Kafka { .. } => {
                crate::metrics::inc_sink_failure(&plan.name, "kafka_replication");
                tracing::warn!(
                    pipeline = %plan.name,
                    error = %err,
                    "replication pipeline target write failed; buffered transaction remains pending"
                );
            }
            ReplicationPipelineRuntimeTarget::Postgres { .. } => {
                crate::metrics::inc_sink_failure(&plan.name, "postgres_replication");
                tracing::warn!(
                    pipeline = %plan.name,
                    error = %err,
                    "replication pipeline Postgres target write failed; buffered transaction remains pending"
                );
            }
        }
    }
}
