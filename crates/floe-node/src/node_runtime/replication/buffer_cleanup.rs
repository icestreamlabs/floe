use std::sync::LazyLock;

use anyhow::{Context, anyhow};
use floe_storage::{CdcBufferCleanupPolicy, CdcBufferStore};

use super::super::ReplicationPipelineRuntimePlan;
use super::{ReplicationPipelineRuntime, current_unix_time_ms};

const DEFAULT_REPLICATION_BUFFER_DELIVERED_RETENTION_MS: u64 = 5_000;
const DEFAULT_REPLICATION_BUFFER_CLEANUP_INTERVAL_MS: u64 = 5_000;

static REPLICATION_BUFFER_DELIVERED_RETENTION_MS: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("FLOE_REPLICATION_BUFFER_DELIVERED_RETENTION_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_REPLICATION_BUFFER_DELIVERED_RETENTION_MS)
});
static REPLICATION_BUFFER_CLEANUP_INTERVAL_MS: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("FLOE_REPLICATION_BUFFER_CLEANUP_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_REPLICATION_BUFFER_CLEANUP_INTERVAL_MS)
});

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
                CdcBufferCleanupPolicy::new(*REPLICATION_BUFFER_DELIVERED_RETENTION_MS),
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
                tokio::spawn(async move {
                    match cleanup_store
                        .cleanup_delivered(
                            &pipeline_name,
                            CdcBufferCleanupPolicy::new(*REPLICATION_BUFFER_DELIVERED_RETENTION_MS),
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
        let cleanup_interval_ms = *REPLICATION_BUFFER_CLEANUP_INTERVAL_MS;
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
