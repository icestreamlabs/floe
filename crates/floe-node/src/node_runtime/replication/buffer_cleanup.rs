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
        crate::metrics::inc_cdc_buffer_object_op(
            &plan.name,
            "delete",
            summary.deleted_transactions(),
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
                tokio::spawn(async move {
                    match cleanup_store
                        .cleanup_delivered(
                            &pipeline_name,
                            CdcBufferCleanupPolicy::new(delivered_retention_ms),
                            now,
                        )
                        .await
                    {
                        Ok(summary) => {
                            crate::metrics::inc_cdc_buffer_object_op(
                                &pipeline_name,
                                "delete",
                                summary.deleted_transactions(),
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
