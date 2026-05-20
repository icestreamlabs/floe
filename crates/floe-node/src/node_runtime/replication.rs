use super::*;

use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use floe_cdc_core::{CdcSourceId, CdcTableId, CdcTableSchema, TransactionBatch};
use floe_config::ReplicationConfig as FloeReplicationConfig;
#[cfg(test)]
use floe_storage::CdcBufferRecord;
use floe_storage::{
    CdcBufferPayloadFormat, CdcBufferStore, ReplicationPipelineCheckpoint, SlateCatalog,
};
use futures::future::join_all;

pub(super) struct ReplicationPipelineRuntime {
    pipelines_by_source: HashMap<CdcSourceId, Vec<ReplicationPipelineRuntimePlan>>,
    kafka_writers_by_pipeline: HashMap<String, Arc<writers::KafkaReplicationPipelineWriter>>,
    postgres_writers_by_pipeline: HashMap<String, Arc<writers::PostgresReplicationPipelineWriter>>,
    buffer_cleanup_last_by_pipeline: Mutex<HashMap<String, u64>>,
    replay_state_by_pipeline: Mutex<HashMap<String, bool>>,
    backpressure_state_by_pipeline: Mutex<HashMap<String, bool>>,
    last_target_error_by_pipeline: Mutex<HashMap<String, String>>,
    settings: FloeReplicationConfig,
}

impl ReplicationPipelineRuntime {
    pub(super) fn new(
        plans: impl IntoIterator<Item = ReplicationPipelineRuntimePlan>,
        settings: FloeReplicationConfig,
    ) -> anyhow::Result<Self> {
        let mut pipelines_by_source: HashMap<CdcSourceId, Vec<ReplicationPipelineRuntimePlan>> =
            HashMap::new();
        let mut kafka_writers_by_pipeline = HashMap::new();
        let mut postgres_writers_by_pipeline = HashMap::new();

        for plan in plans {
            match &plan.target {
                ReplicationPipelineRuntimeTarget::Kafka { brokers, topic } => {
                    kafka_writers_by_pipeline.insert(
                        plan.name.clone(),
                        Arc::new(writers::KafkaReplicationPipelineWriter::new(
                            brokers,
                            topic,
                            plan.buffer_mode,
                        )?),
                    );
                }
                ReplicationPipelineRuntimeTarget::Postgres { connection, table } => {
                    anyhow::ensure!(
                        plan.format == ReplicationPipelineRuntimeFormat::FloeJson,
                        "replication pipeline '{}' uses a Postgres target, which currently requires format = 'floe_json'",
                        plan.name
                    );
                    postgres_writers_by_pipeline.insert(
                        plan.name.clone(),
                        Arc::new(writers::PostgresReplicationPipelineWriter::new(
                            connection,
                            table,
                            plan.schema.clone(),
                        )?),
                    );
                }
            }
            pipelines_by_source
                .entry(CdcSourceId::new(plan.source_name.clone())?)
                .or_default()
                .push(plan);
        }

        Ok(Self {
            pipelines_by_source,
            kafka_writers_by_pipeline,
            postgres_writers_by_pipeline,
            buffer_cleanup_last_by_pipeline: Mutex::new(HashMap::new()),
            replay_state_by_pipeline: Mutex::new(HashMap::new()),
            backpressure_state_by_pipeline: Mutex::new(HashMap::new()),
            last_target_error_by_pipeline: Mutex::new(HashMap::new()),
            settings,
        })
    }

    pub(super) fn has_pipelines_for_source(&self, source_id: &CdcSourceId) -> bool {
        self.pipelines_by_source
            .get(source_id)
            .is_some_and(|plans| !plans.is_empty())
    }

    pub(super) async fn replay_buffered(&self, storage: &SlateCatalog) -> anyhow::Result<usize> {
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

    #[allow(dead_code)]
    pub(super) async fn status_snapshots(
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
                let stats = buffer_store
                    .stats(&plan.name, current_unix_time_ms())
                    .await
                    .with_context(|| {
                        format!(
                            "load CDC buffer stats for replication pipeline '{}'",
                            plan.name
                        )
                    })?;
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
                    pending_records: stats.pending_records(),
                    pending_bytes: stats.pending_bytes(),
                    oldest_pending_age_ms: stats.oldest_pending_age_ms(),
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

    pub(super) async fn refresh_debug_state(
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

    pub(super) async fn run_transaction(
        &self,
        source_id: &CdcSourceId,
        schemas: &HashMap<CdcTableId, CdcTableSchema>,
        transaction: &TransactionBatch,
        storage: Option<&SlateCatalog>,
    ) -> anyhow::Result<usize> {
        let Some(plans) = self.pipelines_by_source.get(source_id) else {
            return Ok(0);
        };

        if plans
            .iter()
            .any(|plan| plan.format == ReplicationPipelineRuntimeFormat::FloeJson)
            && let Some(chunks) = encoding::chunk_snapshot_transaction(source_id, transaction)?
        {
            let mut written = 0usize;
            let chunk_count = chunks.len();
            for chunk in chunks {
                written = written.saturating_add(
                    self.run_transaction_for_plans(plans, schemas, &chunk, storage, false)
                        .await?,
                );
            }
            if let Some(storage) = storage
                && plans
                    .iter()
                    .any(|plan| plan.buffer_mode == ReplicationPipelineRuntimeBufferMode::Durable)
            {
                let flush_started_at = CDC_PERF_LOGGING_ENABLED.then(Instant::now);
                storage
                    .cdc_buffer_store()
                    .flush()
                    .await
                    .context("flush chunked replication buffer appends")?;
                for plan in plans.iter().filter(|plan| {
                    plan.buffer_mode == ReplicationPipelineRuntimeBufferMode::Durable
                }) {
                    crate::metrics::inc_cdc_buffer_forced_flush(&plan.name);
                }
                if let Some(started_at) = flush_started_at {
                    tracing::info!(
                        source = %source_id.as_str(),
                        chunks = chunk_count,
                        flush_ms = started_at.elapsed().as_millis() as u64,
                        "postgres cdc chunked replication buffer flush completed"
                    );
                }
            }
            return Ok(written);
        }

        if let Some(storage) = storage
            && plans
                .iter()
                .any(|plan| plan.buffer_mode == ReplicationPipelineRuntimeBufferMode::Durable)
        {
            let written = self
                .run_transaction_for_plans(plans, schemas, transaction, Some(storage), false)
                .await?;
            let flush_started_at = CDC_PERF_LOGGING_ENABLED.then(Instant::now);
            storage
                .cdc_buffer_store()
                .flush()
                .await
                .context("flush replication buffer appends")?;
            for plan in plans
                .iter()
                .filter(|plan| plan.buffer_mode == ReplicationPipelineRuntimeBufferMode::Durable)
            {
                crate::metrics::inc_cdc_buffer_forced_flush(&plan.name);
            }
            if let Some(started_at) = flush_started_at {
                tracing::info!(
                    source = %source_id.as_str(),
                    records = written,
                    flush_ms = started_at.elapsed().as_millis() as u64,
                    "postgres cdc replication buffer flush completed"
                );
            }
            return Ok(written);
        }

        self.run_transaction_for_plans(plans, schemas, transaction, storage, true)
            .await
    }

    async fn run_transaction_for_plans(
        &self,
        plans: &[ReplicationPipelineRuntimePlan],
        schemas: &HashMap<CdcTableId, CdcTableSchema>,
        transaction: &TransactionBatch,
        storage: Option<&SlateCatalog>,
        await_durable_buffer_append: bool,
    ) -> anyhow::Result<usize> {
        let ordered_plans = ordered_replication_plans_for_transaction(plans, transaction);
        if ordered_plans.len() > 1 && replication_pipeline_targets_are_distinct(plans) {
            let results = join_all(ordered_plans.into_iter().map(|plan| {
                self.run_transaction_for_plan(
                    plan,
                    schemas,
                    transaction,
                    storage,
                    await_durable_buffer_append,
                )
            }))
            .await;
            let mut written = 0usize;
            for result in results {
                written = written.saturating_add(result?);
            }
            return Ok(written);
        }

        let mut written = 0usize;
        for plan in ordered_plans {
            written = written.saturating_add(
                self.run_transaction_for_plan(
                    plan,
                    schemas,
                    transaction,
                    storage,
                    await_durable_buffer_append,
                )
                .await?,
            );
        }

        Ok(written)
    }

    async fn run_transaction_for_plan(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        schemas: &HashMap<CdcTableId, CdcTableSchema>,
        transaction: &TransactionBatch,
        storage: Option<&SlateCatalog>,
        await_durable_buffer_append: bool,
    ) -> anyhow::Result<usize> {
        let perf_enabled = *CDC_PERF_LOGGING_ENABLED;
        let perf_started_at = perf_enabled.then(Instant::now);
        let encode_started_at = perf_enabled.then(Instant::now);
        let buffered_records =
            encoding::encode_pipeline_transaction_records(plan, schemas, transaction)?;
        let encode_elapsed = encode_started_at
            .map(|started_at| started_at.elapsed())
            .unwrap_or(Duration::ZERO);
        if buffered_records.is_empty() {
            return Ok(0);
        }
        let record_count = buffered_records.len();
        let payload_bytes = if perf_enabled {
            estimated_buffer_payload_bytes(&buffered_records)
        } else {
            0
        };
        if plan.buffer_mode == ReplicationPipelineRuntimeBufferMode::NoBuffer {
            if let Err(err) = self.send_records_to_target(plan, &buffered_records).await {
                self.record_target_write_failure(plan, &err);
                let Some(storage) = storage else {
                    return Err(err);
                };
                if !replication_pipeline_uses_dlq(plan) {
                    return Err(err);
                }
                let dlq_entry = persist_dead_letter_records(
                    plan,
                    storage,
                    transaction.commit_position(),
                    transaction.transaction_id(),
                    &buffered_records,
                    &err,
                )
                .await?;
                storage
                    .put_replication_pipeline_checkpoint(ReplicationPipelineCheckpoint::new(
                        &plan.name,
                        &plan.source_name,
                        transaction.commit_position().clone(),
                        transaction.transaction_id().cloned(),
                        direct_dead_lettered_target_state(
                            plan,
                            transaction,
                            record_count,
                            CdcBufferPayloadFormat::KafkaRecords,
                            &dlq_entry,
                            &err,
                        ),
                        current_unix_time_ms(),
                    )?)
                    .await
                    .with_context(|| {
                        format!(
                            "persist replication pipeline '{}' dead-letter checkpoint",
                            plan.name
                        )
                    })?;
                log_replication_pipeline_perf(
                    plan,
                    transaction,
                    record_count,
                    payload_bytes,
                    encode_elapsed,
                    perf_started_at
                        .map(|started_at| started_at.elapsed())
                        .unwrap_or(Duration::ZERO),
                );
                return Ok(buffered_records.len());
            }
            log_replication_pipeline_perf(
                plan,
                transaction,
                record_count,
                payload_bytes,
                encode_elapsed,
                perf_started_at
                    .map(|started_at| started_at.elapsed())
                    .unwrap_or(Duration::ZERO),
            );
            return Ok(buffered_records.len());
        }
        if let Some(storage) = storage {
            let buffer_store = storage.cdc_buffer_store();
            let had_pending = !buffer_store
                .pending_transactions(&plan.name, 1)
                .await
                .with_context(|| {
                    format!(
                        "check pending replication pipeline '{}' buffer transactions",
                        plan.name
                    )
                })?
                .is_empty();
            let prepared_append =
                prepare_replication_buffer_append(plan, transaction, buffered_records)?;
            let incoming_bytes = estimated_buffer_payload_bytes(prepared_append.target_records());
            let incoming_records = prepared_append.append.record_count();
            self.enforce_buffer_limits_before_append(
                plan,
                &buffer_store,
                storage,
                incoming_bytes,
                incoming_records,
                had_pending,
            )
            .await?;
            let has_pending_after_guardrail = if had_pending {
                !buffer_store
                    .pending_transactions(&plan.name, 1)
                    .await
                    .with_context(|| {
                        format!(
                            "check pending replication pipeline '{}' buffer transactions after guardrail drain",
                            plan.name
                        )
                    })?
                    .is_empty()
            } else {
                false
            };
            if has_pending_after_guardrail {
                let append_started_at = perf_enabled.then(Instant::now);
                let manifest = append_buffer_transaction(
                    &buffer_store,
                    &prepared_append.append,
                    await_durable_buffer_append,
                )
                .await
                .with_context(|| {
                    format!(
                        "append replication pipeline '{}' transaction buffer",
                        plan.name
                    )
                })?;
                let append_elapsed = append_started_at
                    .map(|started_at| started_at.elapsed())
                    .unwrap_or(Duration::ZERO);
                log_replication_buffer_append_perf(plan, &manifest, append_elapsed);
                storage
                    .put_replication_pipeline_checkpoint_without_durable_wait(
                        ReplicationPipelineCheckpoint::new(
                            &plan.name,
                            &plan.source_name,
                            transaction.commit_position().clone(),
                            transaction.transaction_id().cloned(),
                            pending_target_state(plan, &manifest),
                            current_unix_time_ms(),
                        )?,
                    )
                    .await
                    .with_context(|| {
                        format!("persist replication pipeline '{}' checkpoint", plan.name)
                    })?;
                self.replay_pending_for_plan(plan, &buffer_store, storage)
                    .await?;
                record_buffer_stats(&buffer_store, &plan.name).await?;
                log_replication_pipeline_perf(
                    plan,
                    transaction,
                    manifest.record_count(),
                    payload_bytes,
                    encode_elapsed,
                    perf_started_at
                        .map(|started_at| started_at.elapsed())
                        .unwrap_or(Duration::ZERO),
                );
                return Ok(manifest.record_count());
            }

            let target_send_started_at = perf_enabled.then(Instant::now);
            match self
                .send_records_to_target(plan, prepared_append.target_records())
                .await
            {
                Ok(target_state) => {
                    let target_send_elapsed = target_send_started_at
                        .map(|started_at| started_at.elapsed())
                        .unwrap_or(Duration::ZERO);
                    let checkpoint_started_at = perf_enabled.then(Instant::now);
                    storage
                        .put_replication_pipeline_checkpoint_without_durable_wait(
                            ReplicationPipelineCheckpoint::new(
                                &plan.name,
                                &plan.source_name,
                                transaction.commit_position().clone(),
                                transaction.transaction_id().cloned(),
                                direct_delivered_target_state(
                                    plan,
                                    transaction,
                                    record_count,
                                    prepared_append.append.payload_format(),
                                    target_state,
                                ),
                                current_unix_time_ms(),
                            )?,
                        )
                        .await
                        .with_context(|| {
                            format!("persist replication pipeline '{}' checkpoint", plan.name)
                        })?;
                    let checkpoint_elapsed = checkpoint_started_at
                        .map(|started_at| started_at.elapsed())
                        .unwrap_or(Duration::ZERO);
                    log_replication_direct_delivery_perf(
                        plan,
                        record_count,
                        prepared_append.append.payload_format(),
                        incoming_bytes,
                        target_send_elapsed,
                        checkpoint_elapsed,
                    );
                    record_buffer_stats(&buffer_store, &plan.name).await?;
                    log_replication_pipeline_perf(
                        plan,
                        transaction,
                        record_count,
                        payload_bytes,
                        encode_elapsed,
                        perf_started_at
                            .map(|started_at| started_at.elapsed())
                            .unwrap_or(Duration::ZERO),
                    );
                    Ok(record_count)
                }
                Err(err) => {
                    self.record_target_write_failure(plan, &err);
                    if replication_pipeline_uses_dlq(plan) {
                        let dlq_entry = persist_dead_letter_records(
                            plan,
                            storage,
                            transaction.commit_position(),
                            transaction.transaction_id(),
                            prepared_append.target_records(),
                            &err,
                        )
                        .await?;
                        storage
                            .put_replication_pipeline_checkpoint(
                                ReplicationPipelineCheckpoint::new(
                                    &plan.name,
                                    &plan.source_name,
                                    transaction.commit_position().clone(),
                                    transaction.transaction_id().cloned(),
                                    direct_dead_lettered_target_state(
                                        plan,
                                        transaction,
                                        record_count,
                                        prepared_append.append.payload_format(),
                                        &dlq_entry,
                                        &err,
                                    ),
                                    current_unix_time_ms(),
                                )?,
                            )
                            .await
                            .with_context(|| {
                                format!(
                                    "persist replication pipeline '{}' dead-letter checkpoint",
                                    plan.name
                                )
                            })?;
                        record_buffer_stats(&buffer_store, &plan.name).await?;
                        log_replication_pipeline_perf(
                            plan,
                            transaction,
                            record_count,
                            payload_bytes,
                            encode_elapsed,
                            perf_started_at
                                .map(|started_at| started_at.elapsed())
                                .unwrap_or(Duration::ZERO),
                        );
                        return Ok(record_count);
                    }
                    let append_started_at = perf_enabled.then(Instant::now);
                    let manifest = append_buffer_transaction(
                        &buffer_store,
                        &prepared_append.append,
                        await_durable_buffer_append,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "append replication pipeline '{}' transaction buffer after target failure",
                            plan.name
                        )
                    })?;
                    let append_elapsed = append_started_at
                        .map(|started_at| started_at.elapsed())
                        .unwrap_or(Duration::ZERO);
                    log_replication_buffer_append_perf(plan, &manifest, append_elapsed);
                    self.mark_manifest_delivery_failed(plan, storage, &manifest, err)
                        .await?;
                    record_buffer_stats(&buffer_store, &plan.name).await?;
                    log_replication_pipeline_perf(
                        plan,
                        transaction,
                        record_count,
                        payload_bytes,
                        encode_elapsed,
                        perf_started_at
                            .map(|started_at| started_at.elapsed())
                            .unwrap_or(Duration::ZERO),
                    );
                    Ok(manifest.record_count())
                }
            }
        } else {
            if let Err(err) = self.send_records_to_target(plan, &buffered_records).await {
                self.record_target_write_failure(plan, &err);
                return Err(err);
            }
            log_replication_pipeline_perf(
                plan,
                transaction,
                record_count,
                payload_bytes,
                encode_elapsed,
                perf_started_at
                    .map(|started_at| started_at.elapsed())
                    .unwrap_or(Duration::ZERO),
            );
            Ok(buffered_records.len())
        }
    }

    async fn enforce_buffer_limits_before_append(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        buffer_store: &CdcBufferStore,
        storage: &SlateCatalog,
        incoming_bytes: usize,
        incoming_records: usize,
        has_pending: bool,
    ) -> anyhow::Result<()> {
        let limits = effective_replication_buffer_limits(plan);
        if !limits.enabled() {
            self.set_source_backpressure_state(&plan.name, false);
            return Ok(());
        }

        if !has_pending {
            if let Some(violation) =
                buffer_limit_violation(0, 0, 0, None, incoming_bytes, incoming_records, limits)
            {
                log_replication_buffer_backpressure(
                    plan,
                    "incoming_transaction",
                    None,
                    incoming_bytes,
                    incoming_records,
                    limits,
                    violation,
                    None,
                );
                self.set_source_backpressure_state(&plan.name, true);
                return Err(anyhow!(
                    "replication pipeline '{}' durable buffer limit exceeded: {violation}; refusing to append more CDC data so the source applies backpressure through its replication slot",
                    plan.name
                ));
            }
            self.set_source_backpressure_state(&plan.name, false);
            return Ok(());
        }

        let mut stats = buffer_store
            .stats(&plan.name, current_unix_time_ms())
            .await
            .with_context(|| {
                format!(
                    "load CDC buffer stats before appending replication pipeline '{}'",
                    plan.name
                )
            })?;
        let Some(mut violation) = buffer_limit_violation(
            stats.pending_transactions(),
            stats.pending_records(),
            stats.pending_bytes(),
            stats.oldest_pending_age_ms(),
            incoming_bytes,
            incoming_records,
            limits,
        ) else {
            self.set_source_backpressure_state(&plan.name, false);
            return Ok(());
        };

        crate::metrics::inc_cdc_buffer_drain_attempt(&plan.name);
        tracing::warn!(
            pipeline = %plan.name,
            pending_transactions = stats.pending_transactions(),
            pending_records = stats.pending_records(),
            pending_bytes = stats.pending_bytes(),
            oldest_pending_age_ms = stats.oldest_pending_age_ms(),
            incoming_bytes,
            incoming_records,
            violation = %violation,
            "replication pipeline durable buffer limit reached; attempting to drain before accepting more CDC data"
        );

        let delivered = self
            .replay_pending_for_plan(plan, buffer_store, storage)
            .await?;
        if delivered > 0 {
            self.spawn_cleanup_delivered_if_due(plan, buffer_store);
        }
        stats = buffer_store
            .stats(&plan.name, current_unix_time_ms())
            .await
            .with_context(|| {
                format!(
                    "load CDC buffer stats after guardrail drain for replication pipeline '{}'",
                    plan.name
                )
            })?;
        tracing::info!(
            pipeline = %plan.name,
            source = %plan.source_name,
            target_kind = target_kind(plan),
            delivered_records = delivered,
            pending_transactions = stats.pending_transactions(),
            pending_records = stats.pending_records(),
            pending_bytes = stats.pending_bytes(),
            oldest_pending_age_ms = stats.oldest_pending_age_ms(),
            incoming_bytes,
            incoming_records,
            max_pending_bytes = limits.max_pending_bytes,
            max_pending_records = limits.max_pending_records,
            max_pending_transactions = limits.max_pending_transactions,
            max_pending_age_ms = limits.max_pending_age_ms,
            "replication pipeline durable buffer guardrail drain completed"
        );
        if let Some(current_violation) = buffer_limit_violation(
            stats.pending_transactions(),
            stats.pending_records(),
            stats.pending_bytes(),
            stats.oldest_pending_age_ms(),
            incoming_bytes,
            incoming_records,
            limits,
        ) {
            violation = current_violation;
            log_replication_buffer_backpressure(
                plan,
                "after_guardrail_drain",
                Some(&stats),
                incoming_bytes,
                incoming_records,
                limits,
                violation,
                Some(delivered),
            );
            self.set_source_backpressure_state(&plan.name, true);
            return Err(anyhow!(
                "replication pipeline '{}' durable buffer limit exceeded after draining: {violation}; refusing to append more CDC data so the source applies backpressure through its replication slot",
                plan.name
            ));
        }
        self.set_source_backpressure_state(&plan.name, false);
        Ok(())
    }
}

fn current_unix_time_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

mod buffer;
mod buffer_cleanup;
mod config;
mod dead_letter;
mod delivery;
mod encoding;
mod perf;
mod plan_helpers;
mod replay;
mod runtime_state;
mod status;
mod target_state;
mod writers;

#[cfg(test)]
use buffer::{
    ReplicationBufferLimitViolation, ReplicationBufferLimits, effective_u64_limit,
    effective_usize_limit,
};
use buffer::{
    append_buffer_transaction, buffer_limit_violation, effective_replication_buffer_limits,
    estimated_buffer_payload_bytes, log_replication_buffer_backpressure,
    prepare_replication_buffer_append, record_buffer_stats,
};
use config::{
    CDC_PERF_LOGGING_ENABLED, FLOE_HEADER_IDEMPOTENCY_KEY, FLOE_HEADER_PIPELINE,
    FLOE_HEADER_RECORD_SEQUENCE, FLOE_HEADER_SOURCE, FLOE_HEADER_SOURCE_POSITION,
    FLOE_HEADER_SOURCE_TABLE, FLOE_HEADER_TRANSACTION_ID, FLOE_JSON_DELETED_FIELD,
    FLOE_JSON_PARALLEL_RECORD_THRESHOLD, FLOE_JSON_VERSION, FLOE_JSON_VERSION_FIELD,
    REPLICATION_ARROW_IPC_COMPRESSION, REPLICATION_ARROW_IPC_ROWS_PER_RECORD,
    REPLICATION_KAFKA_ACKS, REPLICATION_KAFKA_BATCH_NUM_MESSAGES, REPLICATION_KAFKA_BATCH_SIZE,
    REPLICATION_KAFKA_ENABLE_IDEMPOTENCE, REPLICATION_KAFKA_LINGER_MS,
    REPLICATION_KAFKA_MESSAGE_MAX_BYTES, REPLICATION_KAFKA_MESSAGE_SEND_MAX_RETRIES,
    REPLICATION_KAFKA_MESSAGE_TIMEOUT_MS, REPLICATION_KAFKA_METADATA_HEADERS,
    REPLICATION_KAFKA_METADATA_WARMUP_TIMEOUT, REPLICATION_KAFKA_QUEUE_MAX_KBYTES,
    REPLICATION_KAFKA_QUEUE_MAX_MESSAGES, REPLICATION_KAFKA_RETRY_ATTEMPTS,
    REPLICATION_KAFKA_RETRY_BASE_MS, REPLICATION_KAFKA_SEND_TIMEOUT,
    REPLICATION_SNAPSHOT_BATCHES_PER_CHUNK,
};
use dead_letter::persist_dead_letter_records;
use perf::{
    log_replication_buffer_append_perf, log_replication_direct_delivery_perf,
    log_replication_kafka_send_perf, log_replication_pipeline_perf,
};
pub(super) use plan_helpers::{
    materialized_transaction, pipeline_checkpoint_from_transaction, replication_pipeline_table_id,
};
use plan_helpers::{
    ordered_replication_plans_for_transaction, replication_pipeline_targets_are_distinct,
};
use status::{
    ReplicationPipelineStatusSnapshot, cdc_replication_debug_state_from_snapshots,
    enrich_pipeline_checkpoint_lag, postgres_position_lsn_bytes,
};
use target_state::{
    direct_dead_lettered_target_state, direct_delivered_target_state, pending_target_state,
    replication_pipeline_uses_dlq, target_kind,
};

#[cfg(test)]
mod tests;
