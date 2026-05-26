use std::time::Instant;

use anyhow::{Context, anyhow};
use floe_storage::{CdcBufferCleanupPolicy, CdcBufferStore};

use super::super::ReplicationPipelineRuntimePlan;
use super::{ReplicationPipelineRuntime, current_unix_time_ms};

impl ReplicationPipelineRuntime {
    pub(super) async fn cleanup_delivered_if_due(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        buffer_store: &CdcBufferStore,
    ) -> anyhow::Result<()> {
        let now = current_unix_time_ms();
        if !self.claim_cleanup_due(&plan.name, now)? {
            return Ok(());
        }
        let cleanup_started_at = Instant::now();
        let summary = buffer_store
            .cleanup_delivered(
                &plan.name,
                CdcBufferCleanupPolicy::new(self.settings.buffer_cleanup.delivered_retention_ms),
                now,
            )
            .await
            .with_context(|| {
                format!(
                    "cleanup replication pipeline '{}' delivered buffer",
                    plan.name
                )
            })?;
        let orphan_summary = buffer_store
            .cleanup_orphan_payload_objects(
                &plan.name,
                self.settings.buffer_cleanup.orphan_retention_ms,
                now,
            )
            .await
            .with_context(|| {
                format!(
                    "cleanup replication pipeline '{}' orphan buffer payload objects",
                    plan.name
                )
            })?;
        crate::metrics::record_cdc_buffer_cleanup(
            &plan.name,
            summary.deleted_transactions(),
            summary.deleted_records(),
            summary
                .deleted_bytes()
                .saturating_add(orphan_summary.deleted_bytes()),
            cleanup_started_at.elapsed().as_millis() as u64,
        );
        crate::metrics::inc_cdc_buffer_object_op(
            &plan.name,
            "delete",
            summary
                .deleted_transactions()
                .saturating_add(orphan_summary.deleted_objects()),
        );
        Ok(())
    }

    pub(super) fn spawn_cleanup_delivered_if_due(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        buffer_store: &CdcBufferStore,
    ) {
        let now = current_unix_time_ms();
        match self.claim_cleanup_due(&plan.name, now) {
            Ok(true) => {
                let cleanup_store = buffer_store.clone();
                let pipeline_name = plan.name.clone();
                let delivered_retention_ms = self.settings.buffer_cleanup.delivered_retention_ms;
                let orphan_retention_ms = self.settings.buffer_cleanup.orphan_retention_ms;
                tokio::spawn(async move {
                    let cleanup_started_at = Instant::now();
                    let cleanup_result = async {
                        let summary = cleanup_store
                            .cleanup_delivered(
                                &pipeline_name,
                                CdcBufferCleanupPolicy::new(delivered_retention_ms),
                                now,
                            )
                            .await?;
                        let orphan_summary = cleanup_store
                            .cleanup_orphan_payload_objects(
                                &pipeline_name,
                                orphan_retention_ms,
                                now,
                            )
                            .await?;
                        Ok::<_, anyhow::Error>((summary, orphan_summary))
                    }
                    .await;
                    match cleanup_result {
                        Ok(summary) => {
                            crate::metrics::record_cdc_buffer_cleanup(
                                &pipeline_name,
                                summary.0.deleted_transactions(),
                                summary.0.deleted_records(),
                                summary
                                    .0
                                    .deleted_bytes()
                                    .saturating_add(summary.1.deleted_bytes()),
                                cleanup_started_at.elapsed().as_millis() as u64,
                            );
                            crate::metrics::inc_cdc_buffer_object_op(
                                &pipeline_name,
                                "delete",
                                summary
                                    .0
                                    .deleted_transactions()
                                    .saturating_add(summary.1.deleted_objects()),
                            );
                        }
                        Err(err) => {
                            tracing::warn!(
                                pipeline = %pipeline_name,
                                error = %err,
                                "replication pipeline delivered buffer cleanup failed"
                            );
                        }
                    }
                });
            }
            Ok(false) => {}
            Err(err) => {
                tracing::warn!(
                    pipeline = %plan.name,
                    error = %err,
                    "replication pipeline delivered buffer cleanup scheduling failed"
                );
            }
        }
    }

    fn claim_cleanup_due(&self, pipeline_name: &str, now: u64) -> anyhow::Result<bool> {
        let cleanup_interval_ms = self.settings.buffer_cleanup.cleanup_interval_ms;
        let mut last_by_pipeline = self
            .buffer_cleanup_last_by_pipeline
            .lock()
            .map_err(|_| anyhow!("replication buffer cleanup tracker lock poisoned"))?;
        let should_cleanup = cleanup_interval_ms == 0
            || last_by_pipeline
                .get(pipeline_name)
                .is_none_or(|last| now.saturating_sub(*last) >= cleanup_interval_ms);
        if should_cleanup {
            last_by_pipeline.insert(pipeline_name.to_string(), now);
        }
        Ok(should_cleanup)
    }
}
