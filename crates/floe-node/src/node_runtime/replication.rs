use super::*;

use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_ipc::CompressionType;
use floe_cdc_core::{
    CdcCheckpoint, CdcSourceId, CdcTableId, CdcTableSchema, CdcTransactionId, ChangeBatch,
    TransactionBatch,
};
#[cfg(test)]
use floe_storage::CdcBufferRecord;
use floe_storage::{
    CdcBufferPayloadFormat, CdcBufferStore, ReplicationPipelineCheckpoint, SlateCatalog,
};
use futures::future::join_all;

const REPLICATION_KAFKA_RETRY_ATTEMPTS: usize = 5;
const REPLICATION_KAFKA_RETRY_BASE_MS: u64 = 50;
const REPLICATION_KAFKA_MESSAGE_TIMEOUT_MS: &str = "1000";
const DEFAULT_REPLICATION_KAFKA_MESSAGE_MAX_BYTES: &str = "10485760";
const DEFAULT_REPLICATION_KAFKA_ACKS: &str = "1";
const DEFAULT_REPLICATION_KAFKA_ENABLE_IDEMPOTENCE: &str = "false";
const DEFAULT_REPLICATION_KAFKA_BATCH_SIZE: &str = "1000000";
const DEFAULT_REPLICATION_KAFKA_BATCH_NUM_MESSAGES: &str = "1000000";
const DEFAULT_REPLICATION_KAFKA_LINGER_MS: &str = "1";
const DEFAULT_REPLICATION_KAFKA_QUEUE_MAX_MESSAGES: &str = "1000000";
const DEFAULT_REPLICATION_KAFKA_QUEUE_MAX_KBYTES: &str = "1048576";
const DEFAULT_REPLICATION_KAFKA_MESSAGE_SEND_MAX_RETRIES: &str = "0";
const REPLICATION_KAFKA_SEND_TIMEOUT: Duration = Duration::from_secs(2);
const REPLICATION_KAFKA_METADATA_WARMUP_TIMEOUT: Duration = Duration::from_millis(500);
const REPLICATION_BUFFER_REPLAY_LIMIT: usize = 1024;
const FLOE_JSON_VERSION: i64 = 1;
const FLOE_JSON_DELETED_FIELD: &str = "__floe_deleted";
const FLOE_JSON_VERSION_FIELD: &str = "__floe_version";
const FLOE_HEADER_IDEMPOTENCY_KEY: &str = "floe-idempotency-key";
const FLOE_HEADER_PIPELINE: &str = "floe-pipeline";
const FLOE_HEADER_SOURCE: &str = "floe-source";
const FLOE_HEADER_SOURCE_TABLE: &str = "floe-source-table";
const FLOE_HEADER_SOURCE_POSITION: &str = "floe-source-position";
const FLOE_HEADER_TRANSACTION_ID: &str = "floe-transaction-id";
const FLOE_HEADER_RECORD_SEQUENCE: &str = "floe-record-sequence";
const DEFAULT_REPLICATION_ARROW_IPC_ROWS_PER_RECORD: usize = 16_384;
const DEFAULT_REPLICATION_SNAPSHOT_BATCHES_PER_CHUNK: usize = 1;
const DEFAULT_REPLICATION_KAFKA_METADATA_HEADERS: bool = false;
const FLOE_JSON_PARALLEL_RECORD_THRESHOLD: usize = 4_096;
static REPLICATION_KAFKA_MESSAGE_MAX_BYTES: LazyLock<String> = LazyLock::new(|| {
    std::env::var("FLOE_REPLICATION_KAFKA_MESSAGE_MAX_BYTES")
        .ok()
        .filter(|value| value.parse::<usize>().is_ok_and(|bytes| bytes > 0))
        .unwrap_or_else(|| DEFAULT_REPLICATION_KAFKA_MESSAGE_MAX_BYTES.to_string())
});
static REPLICATION_KAFKA_ACKS: LazyLock<String> = LazyLock::new(|| {
    std::env::var("FLOE_REPLICATION_KAFKA_ACKS")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_REPLICATION_KAFKA_ACKS.to_string())
});
static REPLICATION_KAFKA_ENABLE_IDEMPOTENCE: LazyLock<String> = LazyLock::new(|| {
    std::env::var("FLOE_REPLICATION_KAFKA_ENABLE_IDEMPOTENCE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_REPLICATION_KAFKA_ENABLE_IDEMPOTENCE.to_string())
});
static REPLICATION_KAFKA_BATCH_SIZE: LazyLock<String> = LazyLock::new(|| {
    env_positive_usize_string(
        "FLOE_REPLICATION_KAFKA_BATCH_SIZE",
        DEFAULT_REPLICATION_KAFKA_BATCH_SIZE,
    )
});
static REPLICATION_KAFKA_BATCH_NUM_MESSAGES: LazyLock<String> = LazyLock::new(|| {
    env_positive_usize_string(
        "FLOE_REPLICATION_KAFKA_BATCH_NUM_MESSAGES",
        DEFAULT_REPLICATION_KAFKA_BATCH_NUM_MESSAGES,
    )
});
static REPLICATION_KAFKA_LINGER_MS: LazyLock<String> = LazyLock::new(|| {
    env_usize_string(
        "FLOE_REPLICATION_KAFKA_LINGER_MS",
        DEFAULT_REPLICATION_KAFKA_LINGER_MS,
    )
});
static REPLICATION_KAFKA_QUEUE_MAX_MESSAGES: LazyLock<String> = LazyLock::new(|| {
    env_usize_string(
        "FLOE_REPLICATION_KAFKA_QUEUE_MAX_MESSAGES",
        DEFAULT_REPLICATION_KAFKA_QUEUE_MAX_MESSAGES,
    )
});
static REPLICATION_KAFKA_QUEUE_MAX_KBYTES: LazyLock<String> = LazyLock::new(|| {
    env_usize_string(
        "FLOE_REPLICATION_KAFKA_QUEUE_MAX_KBYTES",
        DEFAULT_REPLICATION_KAFKA_QUEUE_MAX_KBYTES,
    )
});
static REPLICATION_KAFKA_MESSAGE_SEND_MAX_RETRIES: LazyLock<String> = LazyLock::new(|| {
    env_usize_string(
        "FLOE_REPLICATION_KAFKA_MESSAGE_SEND_MAX_RETRIES",
        DEFAULT_REPLICATION_KAFKA_MESSAGE_SEND_MAX_RETRIES,
    )
});
static REPLICATION_ARROW_IPC_ROWS_PER_RECORD: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("FLOE_REPLICATION_ARROW_IPC_ROWS_PER_RECORD")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_REPLICATION_ARROW_IPC_ROWS_PER_RECORD)
});
static REPLICATION_SNAPSHOT_BATCHES_PER_CHUNK: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("FLOE_REPLICATION_SNAPSHOT_BATCHES_PER_CHUNK")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_REPLICATION_SNAPSHOT_BATCHES_PER_CHUNK)
});
static CDC_PERF_LOGGING_ENABLED: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("FLOE_CDC_PERF_LOG")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
});
static REPLICATION_ARROW_IPC_COMPRESSION: LazyLock<Option<ReplicationArrowIpcCompression>> =
    LazyLock::new(|| {
        std::env::var("FLOE_REPLICATION_ARROW_IPC_COMPRESSION")
            .ok()
            .and_then(|value| ReplicationArrowIpcCompression::parse(&value))
    });
static REPLICATION_KAFKA_METADATA_HEADERS: LazyLock<bool> = LazyLock::new(|| {
    env_bool(
        "FLOE_REPLICATION_KAFKA_METADATA_HEADERS",
        DEFAULT_REPLICATION_KAFKA_METADATA_HEADERS,
    )
});

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicationArrowIpcCompression {
    Lz4Frame,
}

impl ReplicationArrowIpcCompression {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "none" | "off" | "false" | "0" => None,
            "lz4" | "lz4_frame" | "lz4-frame" => Some(Self::Lz4Frame),
            other => {
                tracing::warn!(
                    compression = other,
                    "unsupported replication Arrow IPC compression; falling back to uncompressed IPC"
                );
                None
            }
        }
    }

    fn arrow_type(self) -> CompressionType {
        match self {
            Self::Lz4Frame => CompressionType::LZ4_FRAME,
        }
    }
}

fn env_usize_string(name: &str, default_value: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| value.parse::<usize>().is_ok())
        .unwrap_or_else(|| default_value.to_string())
}

fn env_positive_usize_string(name: &str, default_value: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| value.parse::<usize>().is_ok_and(|parsed| parsed > 0))
        .unwrap_or_else(|| default_value.to_string())
}

fn env_bool(name: &str, default_value: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default_value)
}

pub(super) struct ReplicationPipelineRuntime {
    pipelines_by_source: HashMap<CdcSourceId, Vec<ReplicationPipelineRuntimePlan>>,
    kafka_writers_by_pipeline: HashMap<String, Arc<writers::KafkaReplicationPipelineWriter>>,
    postgres_writers_by_pipeline: HashMap<String, Arc<writers::PostgresReplicationPipelineWriter>>,
    buffer_cleanup_last_by_pipeline: Mutex<HashMap<String, u64>>,
    replay_state_by_pipeline: Mutex<HashMap<String, bool>>,
    backpressure_state_by_pipeline: Mutex<HashMap<String, bool>>,
    last_target_error_by_pipeline: Mutex<HashMap<String, String>>,
}

struct ReplicationReplayStateGuard<'a> {
    runtime: &'a ReplicationPipelineRuntime,
    pipeline_name: String,
}

impl<'a> ReplicationReplayStateGuard<'a> {
    fn new(runtime: &'a ReplicationPipelineRuntime, pipeline_name: &str) -> Self {
        runtime.set_replay_state(pipeline_name, true);
        Self {
            runtime,
            pipeline_name: pipeline_name.to_string(),
        }
    }
}

impl Drop for ReplicationReplayStateGuard<'_> {
    fn drop(&mut self) {
        self.runtime.set_replay_state(&self.pipeline_name, false);
    }
}

impl ReplicationPipelineRuntime {
    pub(super) fn new(
        plans: impl IntoIterator<Item = ReplicationPipelineRuntimePlan>,
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

    async fn replay_pending_for_plan(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        buffer_store: &CdcBufferStore,
        storage: &SlateCatalog,
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
            let records = replay::load_manifest_records(plan, buffer_store, &manifest).await?;
            let delivery_started_at = CDC_PERF_LOGGING_ENABLED.then(Instant::now);
            let delivered = self
                .deliver_manifest_records(plan, buffer_store, storage, &manifest, &records)
                .await?;
            let delivery_elapsed = delivery_started_at
                .map(|started_at| started_at.elapsed())
                .unwrap_or(Duration::ZERO);
            log_replication_replay_delivery_perf(plan, &manifest, delivery_elapsed, delivered);
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

    fn set_replay_state(&self, pipeline_name: &str, replaying: bool) {
        crate::metrics::record_cdc_replication_replaying(pipeline_name, replaying);
        match self.replay_state_by_pipeline.lock() {
            Ok(mut state) => {
                state.insert(pipeline_name.to_string(), replaying);
            }
            Err(_) => {
                tracing::warn!(
                    pipeline = %pipeline_name,
                    replaying,
                    "replication pipeline replay state lock poisoned"
                );
            }
        }
    }

    fn set_source_backpressure_state(&self, pipeline_name: &str, active: bool) {
        crate::metrics::record_cdc_buffer_source_backpressure_active(pipeline_name, active);
        match self.backpressure_state_by_pipeline.lock() {
            Ok(mut state) => {
                state.insert(pipeline_name.to_string(), active);
            }
            Err(_) => {
                tracing::warn!(
                    pipeline = %pipeline_name,
                    active,
                    "replication pipeline backpressure state lock poisoned"
                );
            }
        }
    }

    fn set_last_target_error(&self, pipeline_name: &str, error: String) {
        crate::metrics::record_cdc_replication_target_error(pipeline_name, true);
        match self.last_target_error_by_pipeline.lock() {
            Ok(mut errors) => {
                errors.insert(pipeline_name.to_string(), truncate_target_error(&error));
            }
            Err(_) => {
                tracing::warn!(
                    pipeline = %pipeline_name,
                    "replication pipeline target error state lock poisoned"
                );
            }
        }
    }

    fn clear_last_target_error(&self, pipeline_name: &str) {
        crate::metrics::record_cdc_replication_target_error(pipeline_name, false);
        if let Ok(mut errors) = self.last_target_error_by_pipeline.lock() {
            errors.remove(pipeline_name);
        }
    }
}

fn ordered_replication_plans_for_transaction<'a>(
    plans: &'a [ReplicationPipelineRuntimePlan],
    transaction: &TransactionBatch,
) -> Vec<&'a ReplicationPipelineRuntimePlan> {
    let mut ordered = plans.iter().collect::<Vec<_>>();
    if ordered.len() <= 1 || !replication_pipeline_targets_are_distinct(plans) {
        return ordered;
    }
    ordered.sort_by(|left, right| {
        transaction_change_count_for_table(transaction, &right.table_id).cmp(
            &transaction_change_count_for_table(transaction, &left.table_id),
        )
    });
    ordered
}

fn replication_pipeline_targets_are_distinct(plans: &[ReplicationPipelineRuntimePlan]) -> bool {
    let mut targets = HashSet::with_capacity(plans.len());
    plans
        .iter()
        .all(|plan| targets.insert(replication_pipeline_target_identity(plan)))
}

fn replication_pipeline_target_identity(plan: &ReplicationPipelineRuntimePlan) -> String {
    match &plan.target {
        ReplicationPipelineRuntimeTarget::Kafka { brokers, topic } => {
            format!("kafka\0{brokers}\0{topic}")
        }
        ReplicationPipelineRuntimeTarget::Postgres { connection, table } => {
            format!("postgres\0{connection}\0{table}")
        }
    }
}

fn transaction_change_count_for_table(
    transaction: &TransactionBatch,
    table_id: &CdcTableId,
) -> usize {
    transaction
        .change_batches()
        .iter()
        .filter(|batch| batch.table_id() == table_id)
        .map(ChangeBatch::change_count)
        .sum()
}

pub(super) fn replication_pipeline_table_id(
    source_name: &str,
    upstream_table: &str,
) -> anyhow::Result<CdcTableId> {
    CdcTableId::new(format!("{source_name}:{upstream_table}"))
}

pub(super) fn materialized_transaction(
    source_id: &CdcSourceId,
    materialized_table_ids: &HashSet<CdcTableId>,
    transaction: &TransactionBatch,
) -> anyhow::Result<Option<TransactionBatch>> {
    let change_batches = transaction
        .change_batches()
        .iter()
        .filter(|batch| materialized_table_ids.contains(batch.table_id()))
        .cloned()
        .collect::<Vec<_>>();
    if change_batches.is_empty() {
        return Ok(None);
    }
    Ok(Some(TransactionBatch::new(
        source_id.clone(),
        transaction.transaction_id().cloned(),
        transaction.start_position().cloned(),
        transaction.commit_position().clone(),
        change_batches,
    )?))
}

pub(super) fn pipeline_checkpoint_from_transaction(
    transaction: &TransactionBatch,
) -> CdcCheckpoint {
    CdcCheckpoint::new(
        transaction.source_id().clone(),
        transaction.commit_position().clone(),
        transaction.transaction_id().cloned(),
    )
    .with_schema_versions(transaction.schema_versions().clone())
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
mod dead_letter;
mod delivery;
mod encoding;
mod perf;
mod replay;
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
use dead_letter::persist_dead_letter_records;
use perf::{
    log_replication_buffer_append_perf, log_replication_direct_delivery_perf,
    log_replication_kafka_send_perf, log_replication_pipeline_perf,
    log_replication_replay_delivery_perf,
};
use status::{
    ReplicationPipelineStatusSnapshot, cdc_replication_debug_state_from_snapshots,
    enrich_pipeline_checkpoint_lag, postgres_position_lsn_bytes,
};
use target_state::{
    direct_dead_lettered_target_state, direct_delivered_target_state, pending_target_state,
    replication_pipeline_uses_dlq, target_kind, truncate_target_error,
};

#[cfg(test)]
mod tests;
