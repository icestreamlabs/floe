use super::*;

impl ReplicationPipelineRuntime {
    pub(in crate::node_runtime) async fn replay_buffered(
        &self,
        storage: &SlateCatalog,
    ) -> anyhow::Result<usize> {
        let buffer_store = storage.cdc_buffer_store();
        let mut delivered = 0usize;
        for plans in self.pipelines_by_source.values() {
            for plan in plans {
                delivered = delivered.saturating_add(
                    self.replay_pending_for_plan(plan, &buffer_store, storage)
                        .await?,
                );
                self.cleanup_delivered_if_due(plan, &buffer_store).await?;
            }
        }
        Ok(delivered)
    }

    pub(in crate::node_runtime) async fn status_snapshots(
        &self,
        storage: &SlateCatalog,
    ) -> anyhow::Result<Vec<ReplicationPipelineStatusSnapshot>> {
        let buffer_store = storage.cdc_buffer_store();
        let replaying_by_pipeline = self
            .replay_state_by_pipeline
            .lock()
            .map(|state| state.clone())
            .map_err(|_| anyhow!("replication replay state lock poisoned"))?;
        let last_error_by_pipeline = self
            .last_target_error_by_pipeline
            .lock()
            .map(|errors| errors.clone())
            .map_err(|_| anyhow!("replication target error state lock poisoned"))?;
        let backpressure_by_pipeline = self
            .backpressure_state_by_pipeline
            .lock()
            .map(|state| state.clone())
            .map_err(|_| anyhow!("replication backpressure state lock poisoned"))?;
        let mut snapshots = Vec::new();
        for plans in self.pipelines_by_source.values() {
            for plan in plans {
                let now_unix_ms = current_unix_time_ms();
                let stats = buffer_store
                    .stats(&plan.name, now_unix_ms)
                    .await
                    .with_context(|| {
                        format!(
                            "load CDC buffer stats for replication pipeline '{}'",
                            plan.name
                        )
                    })?;
                let limits = effective_replication_buffer_limits(
                    plan,
                    ReplicationBufferLimits::from_config(self.settings.buffer_limits),
                );
                record_buffer_cap_utilization(&plan.name, &stats, limits);
                let integrity = buffer_store
                    .integrity_report(&plan.name)
                    .await
                    .with_context(|| {
                        format!(
                            "load CDC buffer integrity report for replication pipeline '{}'",
                            plan.name
                        )
                    })?;
                crate::metrics::record_cdc_buffer_integrity(
                    &plan.name,
                    integrity.missing_payload_objects(),
                    integrity.orphan_payload_objects(),
                    integrity.orphan_payload_bytes(),
                );
                let dlq_entries = storage
                    .replication_pipeline_dlq_entries(&plan.name)
                    .await
                    .with_context(|| {
                        format!("load replication pipeline '{}' DLQ entries", plan.name)
                    })?;
                let mut dlq_pending_entries = 0usize;
                let mut dlq_replayed_entries = 0usize;
                let mut dlq_discarded_entries = 0usize;
                let mut oldest_dlq_pending_age_ms = None;
                for entry in &dlq_entries {
                    match entry.status() {
                        ReplicationPipelineDlqStatus::Pending => {
                            dlq_pending_entries = dlq_pending_entries.saturating_add(1);
                            let age_ms = now_unix_ms.saturating_sub(entry.created_at_unix_ms());
                            oldest_dlq_pending_age_ms = Some(
                                oldest_dlq_pending_age_ms
                                    .map_or(age_ms, |oldest| std::cmp::max(oldest, age_ms)),
                            );
                        }
                        ReplicationPipelineDlqStatus::Replayed => {
                            dlq_replayed_entries = dlq_replayed_entries.saturating_add(1);
                        }
                        ReplicationPipelineDlqStatus::Discarded => {
                            dlq_discarded_entries = dlq_discarded_entries.saturating_add(1);
                        }
                    }
                }
                crate::metrics::record_cdc_replication_dlq_stats(
                    &plan.name,
                    dlq_pending_entries,
                    dlq_replayed_entries,
                    dlq_discarded_entries,
                    oldest_dlq_pending_age_ms,
                );
                let checkpoint = storage
                    .replication_pipeline_checkpoint(&plan.name)
                    .await
                    .with_context(|| {
                        format!("load replication pipeline '{}' checkpoint", plan.name)
                    })?;
                let (
                    checkpoint_position,
                    checkpoint_lsn_bytes,
                    checkpoint_transaction_id,
                    target_state,
                ) = checkpoint
                    .map(|checkpoint| {
                        let checkpoint_lsn_bytes =
                            postgres_position_lsn_bytes(checkpoint.source_position());
                        (
                            Some(checkpoint.source_position().clone()),
                            checkpoint_lsn_bytes,
                            checkpoint.transaction_id().cloned(),
                            checkpoint.target_state().clone(),
                        )
                    })
                    .unwrap_or((None, None, None, std::collections::BTreeMap::new()));
                snapshots.push(ReplicationPipelineStatusSnapshot {
                    pipeline_name: plan.name.clone(),
                    source_name: plan.source_name.clone(),
                    schema_evolution_policy: plan.schema_evolution_policy.as_str().to_string(),
                    error_policy: plan.error_policy.mode().as_str().to_string(),
                    target_kind: target_kind(plan).to_string(),
                    checkpoint_position,
                    checkpoint_lsn_bytes,
                    checkpoint_transaction_id,
                    target_state,
                    pending_transactions: stats.pending_transactions(),
                    pending_objects: stats.pending_objects(),
                    pending_records: stats.pending_records(),
                    pending_bytes: stats.pending_bytes(),
                    oldest_pending_age_ms: stats.oldest_pending_age_ms(),
                    dlq_pending_entries,
                    dlq_replayed_entries,
                    dlq_discarded_entries,
                    oldest_dlq_pending_age_ms,
                    missing_payload_objects: integrity.missing_payload_objects(),
                    orphan_payload_objects: integrity.orphan_payload_objects(),
                    orphan_payload_bytes: integrity.orphan_payload_bytes(),
                    replaying: replaying_by_pipeline
                        .get(&plan.name)
                        .copied()
                        .unwrap_or(false),
                    source_backpressure_active: backpressure_by_pipeline
                        .get(&plan.name)
                        .copied()
                        .unwrap_or(false),
                    last_error: last_error_by_pipeline.get(&plan.name).cloned(),
                });
            }
        }
        snapshots.sort_by(|left, right| left.pipeline_name.cmp(&right.pipeline_name));
        Ok(snapshots)
    }

    pub(in crate::node_runtime) async fn refresh_debug_state(
        &self,
        storage: &SlateCatalog,
        shared: &Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    ) -> anyhow::Result<()> {
        match self.status_snapshots(storage).await {
            Ok(snapshots) => {
                let mut next_state = cdc_replication_debug_state_from_snapshots(snapshots);
                let mut state = shared.write().await;
                next_state.postgres_sources = state.postgres_sources.clone();
                enrich_pipeline_checkpoint_lag(&mut next_state);
                *state = next_state;
                Ok(())
            }
            Err(err) => {
                let message = err.to_string();
                let mut state = shared.write().await;
                state.updated_at_unix_ms = current_unix_time_ms();
                state.refresh_error = Some(message);
                Err(err)
            }
        }
    }
}
