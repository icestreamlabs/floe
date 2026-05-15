use super::*;

use std::fmt;
use std::io::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, Int64Builder, StringBuilder,
    TimestampMillisecondBuilder,
};
use arrow_array::{ArrayRef, Decimal128Array, RecordBatch};
use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow_ipc::{CompressionType, MetadataVersion};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use floe_cdc_core::{
    CdcColumnarColumn, CdcColumnarRowBatch, CdcRow, CdcRowKey, CdcSourcePosition, CdcTransactionId,
};
use floe_core::RowValue;
use floe_node_core::debezium_encoder::{
    DebeziumEncodeContext, DebeziumEncodedRecord, DebeziumEnvelopeConfig, encode_debezium_change,
    encode_debezium_snapshot_row,
};
use floe_storage::{
    CdcBufferAppend, CdcBufferCleanupPolicy, CdcBufferPayloadFormat, CdcBufferRecord,
    CdcBufferStore, CdcBufferedTransactionManifest, ReplicationPipelineCheckpoint, SlateCatalog,
};
use rdkafka::ClientConfig;
use rdkafka::client::ClientContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message;
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};

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
const DEFAULT_REPLICATION_BUFFER_DELIVERED_RETENTION_MS: u64 = 5_000;
const DEFAULT_REPLICATION_BUFFER_CLEANUP_INTERVAL_MS: u64 = 5_000;
const DEFAULT_REPLICATION_BUFFER_MAX_PENDING_BYTES: usize = 10 * 1024 * 1024 * 1024;
const DEFAULT_REPLICATION_BUFFER_MAX_PENDING_AGE_MS: u64 = 0;
const DEFAULT_REPLICATION_ARROW_IPC_ROWS_PER_RECORD: usize = 16_384;
const DEFAULT_REPLICATION_SNAPSHOT_BATCHES_PER_CHUNK: usize = 1;
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
static REPLICATION_BUFFER_MAX_PENDING_BYTES: LazyLock<Option<usize>> = LazyLock::new(|| {
    env_usize_limit(
        "FLOE_REPLICATION_BUFFER_MAX_PENDING_BYTES",
        DEFAULT_REPLICATION_BUFFER_MAX_PENDING_BYTES,
    )
});
static REPLICATION_BUFFER_MAX_PENDING_AGE_MS: LazyLock<Option<u64>> = LazyLock::new(|| {
    env_u64_limit(
        "FLOE_REPLICATION_BUFFER_MAX_PENDING_AGE_MS",
        DEFAULT_REPLICATION_BUFFER_MAX_PENDING_AGE_MS,
    )
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplicationBufferLimits {
    max_pending_bytes: Option<usize>,
    max_pending_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicationBufferLimitViolation {
    PendingBytes {
        pending_bytes: usize,
        incoming_bytes: usize,
        max_pending_bytes: usize,
    },
    PendingAge {
        oldest_pending_age_ms: u64,
        max_pending_age_ms: u64,
    },
}

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

impl ReplicationBufferLimits {
    fn enabled(self) -> bool {
        self.max_pending_bytes.is_some() || self.max_pending_age_ms.is_some()
    }
}

impl fmt::Display for ReplicationBufferLimitViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PendingBytes {
                pending_bytes,
                incoming_bytes,
                max_pending_bytes,
            } => write!(
                f,
                "pending buffer bytes would be {} with incoming {} bytes, above max {} bytes",
                pending_bytes.saturating_add(*incoming_bytes),
                incoming_bytes,
                max_pending_bytes
            ),
            Self::PendingAge {
                oldest_pending_age_ms,
                max_pending_age_ms,
            } => write!(
                f,
                "oldest pending transaction age is {oldest_pending_age_ms} ms, above max {max_pending_age_ms} ms"
            ),
        }
    }
}

fn replication_buffer_limits() -> ReplicationBufferLimits {
    ReplicationBufferLimits {
        max_pending_bytes: *REPLICATION_BUFFER_MAX_PENDING_BYTES,
        max_pending_age_ms: *REPLICATION_BUFFER_MAX_PENDING_AGE_MS,
    }
}

fn effective_replication_buffer_limits(
    plan: &ReplicationPipelineRuntimePlan,
) -> ReplicationBufferLimits {
    let defaults = replication_buffer_limits();
    ReplicationBufferLimits {
        max_pending_bytes: effective_usize_limit(
            plan.buffer_policy.max_pending_bytes(),
            defaults.max_pending_bytes,
        ),
        max_pending_age_ms: effective_u64_limit(
            plan.buffer_policy.max_pending_age_ms(),
            defaults.max_pending_age_ms,
        ),
    }
}

fn effective_usize_limit(
    override_value: Option<usize>,
    default_value: Option<usize>,
) -> Option<usize> {
    match override_value {
        Some(0) => None,
        Some(value) => Some(value),
        None => default_value,
    }
}

fn effective_u64_limit(override_value: Option<u64>, default_value: Option<u64>) -> Option<u64> {
    match override_value {
        Some(0) => None,
        Some(value) => Some(value),
        None => default_value,
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

fn env_usize_limit(name: &str, default_value: usize) -> Option<usize> {
    let value = std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default_value);
    (value > 0).then_some(value)
}

fn env_u64_limit(name: &str, default_value: u64) -> Option<u64> {
    let value = std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default_value);
    (value > 0).then_some(value)
}

pub(super) struct ReplicationPipelineRuntime {
    pipelines_by_source: HashMap<CdcSourceId, Vec<ReplicationPipelineRuntimePlan>>,
    kafka_writers_by_pipeline: HashMap<String, Arc<KafkaReplicationPipelineWriter>>,
    buffer_cleanup_last_by_pipeline: Mutex<HashMap<String, u64>>,
    replay_state_by_pipeline: Mutex<HashMap<String, bool>>,
    last_target_error_by_pipeline: Mutex<HashMap<String, String>>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ReplicationPipelineStatusSnapshot {
    pipeline_name: String,
    source_name: String,
    target_kind: String,
    checkpoint_position: Option<CdcSourcePosition>,
    checkpoint_transaction_id: Option<CdcTransactionId>,
    target_state: std::collections::BTreeMap<String, String>,
    pending_transactions: usize,
    pending_records: usize,
    pending_bytes: usize,
    oldest_pending_age_ms: Option<u64>,
    replaying: bool,
    last_error: Option<String>,
}

#[allow(dead_code)]
impl ReplicationPipelineStatusSnapshot {
    pub(super) fn pipeline_name(&self) -> &str {
        &self.pipeline_name
    }

    pub(super) fn source_name(&self) -> &str {
        &self.source_name
    }

    pub(super) fn target_kind(&self) -> &str {
        &self.target_kind
    }

    pub(super) fn checkpoint_position(&self) -> Option<&CdcSourcePosition> {
        self.checkpoint_position.as_ref()
    }

    pub(super) fn checkpoint_transaction_id(&self) -> Option<&CdcTransactionId> {
        self.checkpoint_transaction_id.as_ref()
    }

    pub(super) fn target_state(&self) -> &std::collections::BTreeMap<String, String> {
        &self.target_state
    }

    pub(super) fn pending_transactions(&self) -> usize {
        self.pending_transactions
    }

    pub(super) fn pending_records(&self) -> usize {
        self.pending_records
    }

    pub(super) fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    pub(super) fn oldest_pending_age_ms(&self) -> Option<u64> {
        self.oldest_pending_age_ms
    }

    pub(super) fn replaying(&self) -> bool {
        self.replaying
    }

    pub(super) fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}

struct KafkaReplicationPipelineWriter {
    producer: ThreadedProducer<KafkaReplicationPipelineContext>,
    topic: String,
}

#[derive(Clone)]
struct KafkaReplicationPipelineContext;

impl ClientContext for KafkaReplicationPipelineContext {}

impl ProducerContext for KafkaReplicationPipelineContext {
    type DeliveryOpaque = Arc<KafkaDeliveryBatchState>;

    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        delivery_state: Arc<KafkaDeliveryBatchState>,
    ) {
        delivery_state.record(delivery_result);
    }
}

struct KafkaDeliveryBatchState {
    expected: usize,
    completed: AtomicUsize,
    failed: AtomicUsize,
    offsets_by_partition: Mutex<std::collections::BTreeMap<i32, i64>>,
    first_error: Mutex<Option<String>>,
    notify: tokio::sync::Notify,
}

impl KafkaDeliveryBatchState {
    fn new(expected: usize) -> Arc<Self> {
        Arc::new(Self {
            expected,
            completed: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
            offsets_by_partition: Mutex::new(std::collections::BTreeMap::new()),
            first_error: Mutex::new(None),
            notify: tokio::sync::Notify::new(),
        })
    }

    fn record(&self, delivery_result: &DeliveryResult<'_>) {
        match delivery_result {
            Ok(message) => {
                if let Ok(mut offsets_by_partition) = self.offsets_by_partition.lock() {
                    offsets_by_partition
                        .entry(message.partition())
                        .and_modify(|current| *current = (*current).max(message.offset()))
                        .or_insert(message.offset());
                }
            }
            Err((err, _message)) => {
                self.failed.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut first_error) = self.first_error.lock()
                    && first_error.is_none()
                {
                    *first_error = Some(err.to_string());
                }
            }
        }
        let completed = self.completed.fetch_add(1, Ordering::AcqRel) + 1;
        if completed >= self.expected {
            self.notify.notify_waiters();
        }
    }

    async fn wait(
        &self,
        timeout: Duration,
    ) -> anyhow::Result<std::collections::BTreeMap<i32, i64>> {
        let started_at = Instant::now();
        while self.completed.load(Ordering::Acquire) < self.expected {
            let elapsed = started_at.elapsed();
            if elapsed >= timeout {
                return Err(anyhow!(
                    "replication pipeline Kafka send timed out after {} delivered of {} records",
                    self.completed.load(Ordering::Acquire),
                    self.expected
                ));
            }
            tokio::time::timeout(timeout - elapsed, self.notify.notified())
                .await
                .map_err(|_| {
                    anyhow!(
                        "replication pipeline Kafka send timed out after {} delivered of {} records",
                        self.completed.load(Ordering::Acquire),
                        self.expected
                    )
                })?;
        }
        if self.failed.load(Ordering::Acquire) > 0 {
            let first_error = self
                .first_error
                .lock()
                .ok()
                .and_then(|first_error| first_error.clone())
                .unwrap_or_else(|| "unknown Kafka delivery error".to_string());
            return Err(anyhow!(
                "replication pipeline Kafka send failed for {} of {} records: {first_error}",
                self.failed.load(Ordering::Acquire),
                self.expected
            ));
        }
        self.offsets_by_partition
            .lock()
            .map(|offsets| offsets.clone())
            .map_err(|_| anyhow!("replication pipeline Kafka delivery offsets lock poisoned"))
    }
}

struct PreparedReplicationBufferAppend {
    append: CdcBufferAppend,
    target_records: Option<Vec<CdcBufferRecord>>,
}

impl PreparedReplicationBufferAppend {
    fn target_records(&self) -> &[CdcBufferRecord] {
        self.target_records
            .as_deref()
            .unwrap_or_else(|| self.append.records())
    }
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

        for plan in plans {
            match &plan.target {
                ReplicationPipelineRuntimeTarget::Kafka { brokers, topic } => {
                    kafka_writers_by_pipeline.insert(
                        plan.name.clone(),
                        Arc::new(KafkaReplicationPipelineWriter::new(
                            brokers,
                            topic,
                            plan.buffer_mode,
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
            buffer_cleanup_last_by_pipeline: Mutex::new(HashMap::new()),
            replay_state_by_pipeline: Mutex::new(HashMap::new()),
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
                let (checkpoint_position, checkpoint_transaction_id, target_state) = checkpoint
                    .map(|checkpoint| {
                        (
                            Some(checkpoint.source_position().clone()),
                            checkpoint.transaction_id().cloned(),
                            checkpoint.target_state().clone(),
                        )
                    })
                    .unwrap_or((None, None, std::collections::BTreeMap::new()));
                snapshots.push(ReplicationPipelineStatusSnapshot {
                    pipeline_name: plan.name.clone(),
                    source_name: plan.source_name.clone(),
                    target_kind: target_kind(plan).to_string(),
                    checkpoint_position,
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
                *shared.write().await = cdc_replication_debug_state_from_snapshots(snapshots);
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
            && let Some(chunks) = chunk_snapshot_transaction(source_id, transaction)?
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
        let mut written = 0usize;
        for plan in plans {
            let perf_enabled = *CDC_PERF_LOGGING_ENABLED;
            let perf_started_at = perf_enabled.then(Instant::now);
            let encode_started_at = perf_enabled.then(Instant::now);
            let buffered_records = encode_pipeline_transaction_records(plan, schemas, transaction)?;
            let encode_elapsed = encode_started_at
                .map(|started_at| started_at.elapsed())
                .unwrap_or(Duration::ZERO);
            if buffered_records.is_empty() {
                continue;
            }
            let record_count = buffered_records.len();
            let payload_bytes = if perf_enabled {
                estimated_buffer_payload_bytes(&buffered_records)
            } else {
                0
            };
            if plan.buffer_mode == ReplicationPipelineRuntimeBufferMode::NoBuffer {
                match &plan.target {
                    ReplicationPipelineRuntimeTarget::Kafka { .. } => {
                        let writer =
                            self.kafka_writers_by_pipeline
                                .get(&plan.name)
                                .ok_or_else(|| {
                                    anyhow!(
                                        "replication pipeline '{}' has no Kafka writer",
                                        plan.name
                                    )
                                })?;
                        writer.send_records(&buffered_records).await?;
                    }
                }
                written = written.saturating_add(buffered_records.len());
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
                continue;
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
                let incoming_bytes =
                    estimated_buffer_payload_bytes(prepared_append.target_records());
                self.enforce_buffer_limits_before_append(
                    plan,
                    &buffer_store,
                    storage,
                    incoming_bytes,
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
                    written = written.saturating_add(manifest.record_count());
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
                    continue;
                }

                let append_send_started_at = perf_enabled.then(Instant::now);
                let append_future = async {
                    let started_at = perf_enabled.then(Instant::now);
                    let result = append_buffer_transaction(
                        &buffer_store,
                        &prepared_append.append,
                        await_durable_buffer_append,
                    )
                    .await;
                    let elapsed = started_at
                        .map(|started_at| started_at.elapsed())
                        .unwrap_or(Duration::ZERO);
                    (result, elapsed)
                };
                let target_future = async {
                    let started_at = perf_enabled.then(Instant::now);
                    let result = self
                        .send_records_to_target(plan, prepared_append.target_records())
                        .await;
                    let elapsed = started_at
                        .map(|started_at| started_at.elapsed())
                        .unwrap_or(Duration::ZERO);
                    (result, elapsed)
                };
                let (
                    (manifest_result, buffer_append_elapsed),
                    (target_result, target_send_elapsed),
                ) = tokio::join!(append_future, target_future);
                let append_send_elapsed = append_send_started_at
                    .map(|started_at| started_at.elapsed())
                    .unwrap_or(Duration::ZERO);
                let manifest = manifest_result.with_context(|| {
                    format!(
                        "append replication pipeline '{}' transaction buffer",
                        plan.name
                    )
                })?;
                let checkpoint_started_at = perf_enabled.then(Instant::now);
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
                let checkpoint_elapsed = checkpoint_started_at
                    .map(|started_at| started_at.elapsed())
                    .unwrap_or(Duration::ZERO);
                written = written.saturating_add(manifest.record_count());
                match target_result {
                    Ok(target_state) => {
                        let delivery_started_at = perf_enabled.then(Instant::now);
                        let delivered = self
                            .mark_manifest_delivered(
                                plan,
                                &buffer_store,
                                storage,
                                &manifest,
                                target_state,
                            )
                            .await?;
                        let delivery_elapsed = delivery_started_at
                            .map(|started_at| started_at.elapsed())
                            .unwrap_or(Duration::ZERO);
                        log_replication_durable_stage_perf(
                            plan,
                            &manifest,
                            manifest.record_count(),
                            append_send_elapsed,
                            buffer_append_elapsed,
                            target_send_elapsed,
                            checkpoint_elapsed,
                            delivery_elapsed,
                        );
                        if delivered > 0 {
                            self.spawn_cleanup_delivered_if_due(plan, &buffer_store);
                        }
                    }
                    Err(err) => {
                        self.mark_manifest_delivery_failed(plan, storage, &manifest, err)
                            .await?;
                    }
                }
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
            } else {
                match &plan.target {
                    ReplicationPipelineRuntimeTarget::Kafka { .. } => {
                        let writer =
                            self.kafka_writers_by_pipeline
                                .get(&plan.name)
                                .ok_or_else(|| {
                                    anyhow!(
                                        "replication pipeline '{}' has no Kafka writer",
                                        plan.name
                                    )
                                })?;
                        if let Err(err) = writer.send_records(&buffered_records).await {
                            self.record_target_write_failure(plan, &err);
                            return Err(err);
                        }
                    }
                }
                written = written.saturating_add(buffered_records.len());
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
            }
        }

        Ok(written)
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
        for manifest in pending {
            let payload_load_started_at = CDC_PERF_LOGGING_ENABLED.then(Instant::now);
            let records = match manifest.payload_format() {
                CdcBufferPayloadFormat::KafkaRecords => {
                    buffer_store.records(&manifest).await.with_context(|| {
                        format!(
                            "load replication pipeline '{}' buffered payloads",
                            plan.name
                        )
                    })?
                }
                CdcBufferPayloadFormat::ChangeBatches => {
                    anyhow::ensure!(
                        plan.format == ReplicationPipelineRuntimeFormat::FloeJson,
                        "replication pipeline '{}' cannot replay change batch buffer payloads for {:?}",
                        plan.name,
                        plan.format
                    );
                    let batches =
                        buffer_store
                            .change_batches(&manifest)
                            .await
                            .with_context(|| {
                                format!(
                                    "load replication pipeline '{}' buffered change batches",
                                    plan.name
                                )
                            })?;
                    let payload_load_elapsed = payload_load_started_at
                        .map(|started_at| started_at.elapsed())
                        .unwrap_or(Duration::ZERO);
                    let encode_started_at = CDC_PERF_LOGGING_ENABLED.then(Instant::now);
                    let records =
                        encode_floe_json_buffered_change_batches(plan, &plan.schema, &batches)?;
                    let encode_elapsed = encode_started_at
                        .map(|started_at| started_at.elapsed())
                        .unwrap_or(Duration::ZERO);
                    log_replication_replay_payload_perf(
                        plan,
                        &manifest,
                        payload_load_elapsed,
                        encode_elapsed,
                        records.len(),
                    );
                    records
                }
            };
            if manifest.payload_format() == CdcBufferPayloadFormat::KafkaRecords {
                let payload_load_elapsed = payload_load_started_at
                    .map(|started_at| started_at.elapsed())
                    .unwrap_or(Duration::ZERO);
                log_replication_replay_payload_perf(
                    plan,
                    &manifest,
                    payload_load_elapsed,
                    Duration::ZERO,
                    records.len(),
                );
            }
            let delivery_started_at = CDC_PERF_LOGGING_ENABLED.then(Instant::now);
            let delivered = self
                .deliver_manifest_records(plan, buffer_store, storage, &manifest, &records)
                .await?;
            let delivery_elapsed = delivery_started_at
                .map(|started_at| started_at.elapsed())
                .unwrap_or(Duration::ZERO);
            log_replication_replay_delivery_perf(plan, &manifest, delivery_elapsed, delivered);
            if delivered == 0 {
                break;
            }
            delivered_records = delivered_records.saturating_add(delivered);
            self.spawn_cleanup_delivered_if_due(plan, buffer_store);
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
        has_pending: bool,
    ) -> anyhow::Result<()> {
        let limits = effective_replication_buffer_limits(plan);
        if !limits.enabled() {
            return Ok(());
        }

        if !has_pending {
            if let Some(violation) = buffer_limit_violation(0, None, incoming_bytes, limits) {
                return Err(anyhow!(
                    "replication pipeline '{}' durable buffer limit exceeded: {violation}; refusing to append more CDC data so the source applies backpressure through its replication slot",
                    plan.name
                ));
            }
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
            stats.pending_bytes(),
            stats.oldest_pending_age_ms(),
            incoming_bytes,
            limits,
        ) else {
            return Ok(());
        };

        tracing::warn!(
            pipeline = %plan.name,
            pending_transactions = stats.pending_transactions(),
            pending_records = stats.pending_records(),
            pending_bytes = stats.pending_bytes(),
            oldest_pending_age_ms = stats.oldest_pending_age_ms(),
            incoming_bytes,
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
        if let Some(current_violation) = buffer_limit_violation(
            stats.pending_bytes(),
            stats.oldest_pending_age_ms(),
            incoming_bytes,
            limits,
        ) {
            violation = current_violation;
            return Err(anyhow!(
                "replication pipeline '{}' durable buffer limit exceeded after draining: {violation}; refusing to append more CDC data so the source applies backpressure through its replication slot",
                plan.name
            ));
        }
        Ok(())
    }

    async fn deliver_manifest_records(
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
                self.mark_manifest_delivery_failed(plan, storage, manifest, err)
                    .await?;
                Ok(0)
            }
        }
    }

    async fn send_records_to_target(
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
                let mut target_state = writer.send_records(records).await?;
                target_state.insert("source.table".to_string(), plan.upstream_table.clone());
                Ok(target_state)
            }
        }
    }

    async fn mark_manifest_delivered(
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

    async fn mark_manifest_delivery_failed(
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

    fn record_target_write_failure(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        err: &anyhow::Error,
    ) {
        self.set_last_target_error(&plan.name, format!("{err:#}"));
        match &plan.target {
            ReplicationPipelineRuntimeTarget::Kafka { .. } => {
                crate::metrics::inc_sink_failure(&plan.name, "kafka_replication");
                tracing::warn!(
                    pipeline = %plan.name,
                    error = %err,
                    "replication pipeline target write failed; buffered transaction remains pending"
                );
            }
        }
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

    async fn cleanup_delivered_if_due(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        buffer_store: &CdcBufferStore,
    ) -> anyhow::Result<()> {
        let now = current_unix_time_ms();
        if !self.claim_cleanup_due(&plan.name, now)? {
            return Ok(());
        }
        buffer_store
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
        Ok(())
    }

    fn spawn_cleanup_delivered_if_due(
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
                    if let Err(err) = cleanup_store
                        .cleanup_delivered(
                            &pipeline_name,
                            CdcBufferCleanupPolicy::new(*REPLICATION_BUFFER_DELIVERED_RETENTION_MS),
                            now,
                        )
                        .await
                    {
                        tracing::warn!(
                            pipeline = %pipeline_name,
                            error = %err,
                            "replication pipeline delivered buffer cleanup failed"
                        );
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

impl KafkaReplicationPipelineWriter {
    fn new(
        brokers: &str,
        topic: &str,
        _buffer_mode: ReplicationPipelineRuntimeBufferMode,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !brokers.trim().is_empty(),
            "replication Kafka brokers cannot be empty"
        );
        anyhow::ensure!(
            !topic.trim().is_empty(),
            "replication Kafka topic cannot be empty"
        );
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", REPLICATION_KAFKA_MESSAGE_TIMEOUT_MS)
            .set(
                "message.max.bytes",
                REPLICATION_KAFKA_MESSAGE_MAX_BYTES.as_str(),
            )
            .set("acks", REPLICATION_KAFKA_ACKS.as_str())
            .set(
                "enable.idempotence",
                REPLICATION_KAFKA_ENABLE_IDEMPOTENCE.as_str(),
            )
            .set("compression.type", "none")
            .set("linger.ms", REPLICATION_KAFKA_LINGER_MS.as_str())
            .set("batch.size", REPLICATION_KAFKA_BATCH_SIZE.as_str())
            .set(
                "batch.num.messages",
                REPLICATION_KAFKA_BATCH_NUM_MESSAGES.as_str(),
            )
            .set(
                "queue.buffering.max.messages",
                REPLICATION_KAFKA_QUEUE_MAX_MESSAGES.as_str(),
            )
            .set(
                "queue.buffering.max.kbytes",
                REPLICATION_KAFKA_QUEUE_MAX_KBYTES.as_str(),
            )
            .set(
                "message.send.max.retries",
                REPLICATION_KAFKA_MESSAGE_SEND_MAX_RETRIES.as_str(),
            );
        let producer: ThreadedProducer<KafkaReplicationPipelineContext> = config
            .create_with_context(KafkaReplicationPipelineContext)
            .context("create replication pipeline Kafka producer")?;
        if let Err(err) = producer
            .client()
            .fetch_metadata(Some(topic), REPLICATION_KAFKA_METADATA_WARMUP_TIMEOUT)
        {
            tracing::debug!(
                topic,
                error = %err,
                "replication pipeline Kafka metadata warm-up failed; first send will retry metadata"
            );
        }
        Ok(Self {
            producer,
            topic: topic.to_string(),
        })
    }

    async fn send_records(
        &self,
        records: &[CdcBufferRecord],
    ) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
        let delivery_state = KafkaDeliveryBatchState::new(records.len());
        for record in records {
            self.enqueue_record_with_retry(record, Arc::clone(&delivery_state))
                .await?;
        }
        let offsets_by_partition = delivery_state.wait(REPLICATION_KAFKA_SEND_TIMEOUT).await?;

        let mut target_state = std::collections::BTreeMap::new();
        target_state.insert("kafka.topic".to_string(), self.topic.clone());
        for (partition, offset) in offsets_by_partition {
            target_state.insert(
                format!("kafka.partition.{partition}.offset"),
                offset.to_string(),
            );
        }
        Ok(target_state)
    }

    async fn enqueue_record_with_retry(
        &self,
        record: &CdcBufferRecord,
        delivery_state: Arc<KafkaDeliveryBatchState>,
    ) -> anyhow::Result<()> {
        for attempt in 0..REPLICATION_KAFKA_RETRY_ATTEMPTS {
            let mut kafka_record =
                BaseRecord::<[u8], [u8], Arc<KafkaDeliveryBatchState>>::with_opaque_to(
                    &self.topic,
                    Arc::clone(&delivery_state),
                );
            if let Some(key) = record.key() {
                kafka_record = kafka_record.key(key);
            }
            if let Some(value) = record.value() {
                kafka_record = kafka_record.payload(value);
            }

            match self.producer.send(kafka_record) {
                Ok(()) => return Ok(()),
                Err((err, _record))
                    if is_kafka_queue_full(&err)
                        && attempt + 1 < REPLICATION_KAFKA_RETRY_ATTEMPTS =>
                {
                    let delay_ms = REPLICATION_KAFKA_RETRY_BASE_MS.saturating_mul(
                        1_u64 << u32::try_from(attempt).unwrap_or(u32::MAX).min(16),
                    );
                    tracing::warn!(
                        topic = %self.topic,
                        attempt = attempt + 1,
                        error = %err,
                        "replication pipeline Kafka producer queue is full; retrying"
                    );
                    self.producer.poll(Duration::from_millis(0));
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                Err((err, _record)) => {
                    return Err(anyhow!(
                        "replication pipeline Kafka enqueue failed after retries: {err}"
                    ));
                }
            }
        }
        Err(anyhow!(
            "replication pipeline Kafka enqueue failed after retries"
        ))
    }
}

fn is_kafka_queue_full(err: &KafkaError) -> bool {
    matches!(
        err,
        KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
    )
}

async fn append_buffer_transaction(
    buffer_store: &CdcBufferStore,
    append: &CdcBufferAppend,
    await_durable: bool,
) -> anyhow::Result<CdcBufferedTransactionManifest> {
    if await_durable {
        buffer_store.append_transaction(append).await
    } else {
        buffer_store
            .append_transaction_without_durable_wait(append)
            .await
    }
}

fn prepare_replication_buffer_append(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    target_records: Vec<CdcBufferRecord>,
) -> anyhow::Result<PreparedReplicationBufferAppend> {
    let buffered_at_unix_ms = current_unix_time_ms();
    if plan.format == ReplicationPipelineRuntimeFormat::FloeJson {
        let change_batches = matching_change_batches(plan, transaction);
        anyhow::ensure!(
            !change_batches.is_empty(),
            "replication pipeline '{}' has target records but no matching CDC change batches",
            plan.name
        );
        return Ok(PreparedReplicationBufferAppend {
            append: CdcBufferAppend::new_change_batches(
                &plan.name,
                &plan.source_name,
                plan.table_id.as_str(),
                transaction.commit_position().clone(),
                transaction.transaction_id().cloned(),
                change_batches,
                buffered_at_unix_ms,
            )?
            .with_schema_versions(transaction.schema_versions().clone()),
            target_records: Some(target_records),
        });
    }

    Ok(PreparedReplicationBufferAppend {
        append: CdcBufferAppend::new(
            &plan.name,
            &plan.source_name,
            plan.table_id.as_str(),
            transaction.commit_position().clone(),
            transaction.transaction_id().cloned(),
            target_records,
            buffered_at_unix_ms,
        )?
        .with_schema_versions(transaction.schema_versions().clone()),
        target_records: None,
    })
}

fn matching_change_batches(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
) -> Vec<ChangeBatch> {
    transaction
        .change_batches()
        .iter()
        .filter(|batch| batch.table_id() == &plan.table_id)
        .cloned()
        .collect()
}

fn encode_pipeline_transaction_records(
    plan: &ReplicationPipelineRuntimePlan,
    schemas: &HashMap<CdcTableId, CdcTableSchema>,
    transaction: &TransactionBatch,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    let mut matching_batches = transaction
        .change_batches()
        .iter()
        .filter(|batch| batch.table_id() == &plan.table_id)
        .peekable();
    if matching_batches.peek().is_none() {
        return Ok(Vec::new());
    }

    let schema = schemas.get(&plan.table_id).ok_or_else(|| {
        anyhow!(
            "replication pipeline '{}' references missing CDC schema '{}'",
            plan.name,
            plan.table_id.as_str()
        )
    })?;
    let mut records = Vec::new();
    for change_batch in matching_batches {
        records.extend(encode_pipeline_buffer_records(
            plan,
            schema,
            change_batch,
            transaction,
        )?);
    }
    Ok(records)
}

fn chunk_snapshot_transaction(
    source_id: &CdcSourceId,
    transaction: &TransactionBatch,
) -> anyhow::Result<Option<Vec<TransactionBatch>>> {
    let Some(transaction_id) = transaction.transaction_id() else {
        return Ok(None);
    };
    let batches_per_chunk = *REPLICATION_SNAPSHOT_BATCHES_PER_CHUNK;
    if !transaction_id.as_str().starts_with("snapshot:")
        || transaction.change_batches().len() <= batches_per_chunk
    {
        return Ok(None);
    }
    if !transaction
        .change_batches()
        .iter()
        .all(|batch| batch.snapshot_insert_rows().is_some())
    {
        return Ok(None);
    }

    let chunk_count = transaction
        .change_batches()
        .len()
        .div_ceil(batches_per_chunk);
    let mut chunks = Vec::with_capacity(chunk_count);
    for (idx, batch_chunk) in transaction
        .change_batches()
        .chunks(batches_per_chunk)
        .enumerate()
    {
        chunks.push(TransactionBatch::new(
            source_id.clone(),
            Some(CdcTransactionId::new(format!(
                "{}:chunk:{idx:06}",
                transaction_id.as_str()
            ))?),
            transaction.start_position().cloned(),
            transaction.commit_position().clone(),
            batch_chunk.to_vec(),
        )?);
    }
    Ok(Some(chunks))
}

fn encode_pipeline_buffer_records(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    batch: &ChangeBatch,
    transaction: &TransactionBatch,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    if let Some(rows) = batch.snapshot_insert_rows() {
        return match plan.format {
            ReplicationPipelineRuntimeFormat::FloeJson => {
                encode_floe_json_snapshot_pipeline_records(plan, schema, rows)
            }
            ReplicationPipelineRuntimeFormat::DebeziumJson => {
                let records =
                    encode_debezium_snapshot_pipeline_records(plan, schema, rows, transaction)?;
                debezium_records_to_buffer_records(&records)
            }
            ReplicationPipelineRuntimeFormat::ArrowIpc => {
                encode_arrow_ipc_snapshot_pipeline_records(plan, schema, rows, transaction)
            }
        };
    }

    match plan.format {
        ReplicationPipelineRuntimeFormat::FloeJson => {
            encode_floe_json_pipeline_records(plan, schema, batch)
        }
        ReplicationPipelineRuntimeFormat::DebeziumJson => {
            let records = encode_debezium_pipeline_records(plan, schema, batch, transaction)?;
            debezium_records_to_buffer_records(&records)
        }
        ReplicationPipelineRuntimeFormat::ArrowIpc => {
            encode_arrow_ipc_pipeline_records(plan, schema, batch, transaction)
        }
    }
}

fn encode_floe_json_pipeline_records(
    _plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    batch: &ChangeBatch,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    validate_floe_json_schema(schema)?;
    let mut records = Vec::with_capacity(batch.changes().len());
    for change in batch.changes() {
        match change {
            CdcChange::Insert { row } => {
                let key = schema.primary_key_from_row(row)?;
                records.push(floe_json_record_from_row(schema, &key, row, false)?);
            }
            CdcChange::Update { key, before, after } => {
                let key = cdc_key_for_update(schema, key.as_ref(), before.as_ref(), after)?;
                records.push(floe_json_record_from_row(schema, &key, after, false)?);
            }
            CdcChange::Delete { key, before } => {
                let key = cdc_key_for_delete(schema, key.as_ref(), before.as_ref())?;
                let value = match before {
                    Some(row) => floe_json_value_bytes_from_row(schema, row, true)?,
                    None => floe_json_value_bytes_from_key(schema, &key)?,
                };
                records.push(CdcBufferRecord::new(
                    Some(floe_json_key_bytes(schema, &key)?),
                    Some(value),
                ));
            }
            CdcChange::Truncate => {
                return Err(anyhow!(
                    "Floe JSON replication for table '{}' does not support truncate",
                    schema.table_id().as_str()
                ));
            }
        }
    }
    Ok(records)
}

fn encode_floe_json_buffered_change_batches(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    batches: &[ChangeBatch],
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    let record_count = batches.iter().map(ChangeBatch::change_count).sum::<usize>();
    let mut records = Vec::with_capacity(record_count);
    for batch in batches {
        anyhow::ensure!(
            batch.table_id() == &plan.table_id,
            "replication pipeline '{}' buffered change batch table '{}' does not match plan table '{}'",
            plan.name,
            batch.table_id().as_str(),
            plan.table_id.as_str()
        );
        if let Some(rows) = batch.snapshot_insert_rows() {
            records.extend(encode_floe_json_snapshot_pipeline_records(
                plan, schema, rows,
            )?);
        } else {
            records.extend(encode_floe_json_pipeline_records(plan, schema, batch)?);
        }
    }
    Ok(records)
}

fn encode_floe_json_snapshot_pipeline_records(
    _plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    rows: &CdcColumnarRowBatch,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    validate_floe_json_schema(schema)?;
    schema.validate_columnar_rows(rows)?;
    let encoder = FloeJsonColumnarEncoder::new(schema)?;
    let mut records = Vec::with_capacity(rows.row_count());
    for row_idx in 0..rows.row_count() {
        records.push(floe_json_record_from_columnar_row(rows, row_idx, &encoder)?);
    }
    Ok(records)
}

fn cdc_key_for_update(
    schema: &CdcTableSchema,
    explicit_key: Option<&CdcRowKey>,
    before: Option<&CdcRow>,
    after: &CdcRow,
) -> anyhow::Result<CdcRowKey> {
    if let Some(key) = explicit_key {
        return Ok(key.clone());
    }
    if let Some(before) = before {
        return schema.primary_key_from_row(before);
    }
    schema.primary_key_from_row(after)
}

fn cdc_key_for_delete(
    schema: &CdcTableSchema,
    explicit_key: Option<&CdcRowKey>,
    before: Option<&CdcRow>,
) -> anyhow::Result<CdcRowKey> {
    if let Some(key) = explicit_key {
        return Ok(key.clone());
    }
    let before = before.ok_or_else(|| anyhow!("CDC delete requires a key or before row"))?;
    schema.primary_key_from_row(before)
}

fn validate_floe_json_schema(schema: &CdcTableSchema) -> anyhow::Result<()> {
    for column in schema.columns() {
        anyhow::ensure!(
            column.name() != FLOE_JSON_DELETED_FIELD && column.name() != FLOE_JSON_VERSION_FIELD,
            "Floe JSON replication for table '{}' cannot encode source column '{}' because it is a reserved metadata field",
            schema.table_id().as_str(),
            column.name()
        );
    }
    Ok(())
}

fn floe_json_record_from_row(
    schema: &CdcTableSchema,
    key: &CdcRowKey,
    row: &CdcRow,
    deleted: bool,
) -> anyhow::Result<CdcBufferRecord> {
    Ok(CdcBufferRecord::new(
        Some(floe_json_key_bytes(schema, key)?),
        Some(floe_json_value_bytes_from_row(schema, row, deleted)?),
    ))
}

struct FloeJsonColumnarEncoder {
    value_fields: Vec<FloeJsonColumnarField>,
    key_fields: Vec<FloeJsonColumnarField>,
}

struct FloeJsonColumnarField {
    column_idx: usize,
    name: String,
    prefix: Vec<u8>,
    data_type: ColumnType,
}

impl FloeJsonColumnarEncoder {
    fn new(schema: &CdcTableSchema) -> anyhow::Result<Self> {
        let value_fields = schema
            .columns()
            .iter()
            .enumerate()
            .map(|(column_idx, column)| {
                Ok(FloeJsonColumnarField {
                    column_idx,
                    name: column.name().to_string(),
                    prefix: encoded_json_field_prefix(column.name(), column_idx == 0)?,
                    data_type: column.data_type().clone(),
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let primary_key_indices = schema.primary_key_indices();
        let key_fields = schema
            .primary_key()
            .columns()
            .iter()
            .zip(primary_key_indices)
            .enumerate()
            .map(|(key_idx, (column_name, column_idx))| {
                let column = &schema.columns()[column_idx];
                Ok(FloeJsonColumnarField {
                    column_idx,
                    name: column_name.clone(),
                    prefix: encoded_json_field_prefix(column_name, key_idx == 0)?,
                    data_type: column.data_type().clone(),
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self {
            value_fields,
            key_fields,
        })
    }
}

fn encoded_json_field_prefix(field_name: &str, first: bool) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(field_name.len() + 4);
    if !first {
        out.push(b',');
    }
    serde_json::to_writer(&mut out, field_name)?;
    out.push(b':');
    Ok(out)
}

fn floe_json_record_from_columnar_row(
    rows: &CdcColumnarRowBatch,
    row_idx: usize,
    encoder: &FloeJsonColumnarEncoder,
) -> anyhow::Result<CdcBufferRecord> {
    Ok(CdcBufferRecord::new(
        Some(floe_json_columnar_key_bytes(rows, row_idx, encoder)?),
        Some(floe_json_columnar_value_bytes(
            rows, row_idx, encoder, false,
        )?),
    ))
}

fn floe_json_columnar_key_bytes(
    rows: &CdcColumnarRowBatch,
    row_idx: usize,
    encoder: &FloeJsonColumnarEncoder,
) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(encoder.key_fields.len() * 24);
    out.push(b'{');
    for field in &encoder.key_fields {
        out.extend_from_slice(&field.prefix);
        let column = rows
            .columns()
            .get(field.column_idx)
            .ok_or_else(|| anyhow!("CDC column index {} out of bounds", field.column_idx))?;
        append_floe_json_columnar_value(&mut out, column, row_idx, field, false)?;
    }
    out.push(b'}');
    Ok(out)
}

fn floe_json_columnar_value_bytes(
    rows: &CdcColumnarRowBatch,
    row_idx: usize,
    encoder: &FloeJsonColumnarEncoder,
    deleted: bool,
) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(encoder.value_fields.len() * 32 + 64);
    out.push(b'{');
    for field in &encoder.value_fields {
        out.extend_from_slice(&field.prefix);
        let column = rows
            .columns()
            .get(field.column_idx)
            .ok_or_else(|| anyhow!("CDC column index {} out of bounds", field.column_idx))?;
        append_floe_json_columnar_value(&mut out, column, row_idx, field, true)?;
    }
    let mut first = encoder.value_fields.is_empty();
    append_floe_json_metadata(&mut out, &mut first, deleted)?;
    out.push(b'}');
    Ok(out)
}

fn floe_json_key_bytes(schema: &CdcTableSchema, key: &CdcRowKey) -> anyhow::Result<Vec<u8>> {
    key.validate_against_schema(schema)?;
    let mut out = Vec::with_capacity(schema.primary_key().columns().len() * 24);
    out.push(b'{');
    let mut first = true;
    for (column, value) in schema.primary_key().columns().iter().zip(key.values()) {
        let column_definition = schema
            .columns()
            .iter()
            .find(|definition| definition.name() == column)
            .ok_or_else(|| anyhow!("CDC primary-key column '{column}' missing from schema"))?;
        append_json_field_name(&mut out, &mut first, column)?;
        append_floe_json_value(&mut out, value, column_definition.data_type())?;
    }
    out.push(b'}');
    Ok(out)
}

fn floe_json_value_bytes_from_row(
    schema: &CdcTableSchema,
    row: &CdcRow,
    deleted: bool,
) -> anyhow::Result<Vec<u8>> {
    schema.validate_row(row)?;
    let mut out = Vec::with_capacity(schema.columns().len() * 32 + 64);
    out.push(b'{');
    let mut first = true;
    for (column, value) in schema.columns().iter().zip(row.values()) {
        append_json_field_name(&mut out, &mut first, column.name())?;
        if let Some(value) = value.as_ref() {
            append_floe_json_value(&mut out, value, column.data_type())?;
        } else {
            out.extend_from_slice(b"null");
        }
    }
    append_floe_json_metadata(&mut out, &mut first, deleted)?;
    out.push(b'}');
    Ok(out)
}

fn floe_json_value_bytes_from_key(
    schema: &CdcTableSchema,
    key: &CdcRowKey,
) -> anyhow::Result<Vec<u8>> {
    key.validate_against_schema(schema)?;
    let mut out = Vec::with_capacity(schema.primary_key().columns().len() * 24 + 64);
    out.push(b'{');
    let mut first = true;
    for (column, value) in schema.primary_key().columns().iter().zip(key.values()) {
        let column_definition = schema
            .columns()
            .iter()
            .find(|definition| definition.name() == column)
            .ok_or_else(|| anyhow!("CDC primary-key column '{column}' missing from schema"))?;
        append_json_field_name(&mut out, &mut first, column)?;
        append_floe_json_value(&mut out, value, column_definition.data_type())?;
    }
    append_floe_json_metadata(&mut out, &mut first, true)?;
    out.push(b'}');
    Ok(out)
}

fn append_json_field_name(
    out: &mut Vec<u8>,
    first: &mut bool,
    field_name: &str,
) -> anyhow::Result<()> {
    if *first {
        *first = false;
    } else {
        out.push(b',');
    }
    serde_json::to_writer(&mut *out, field_name)?;
    out.push(b':');
    Ok(())
}

fn append_floe_json_metadata(
    out: &mut Vec<u8>,
    first: &mut bool,
    deleted: bool,
) -> anyhow::Result<()> {
    if !*first {
        out.push(b',');
    }
    *first = false;
    if deleted {
        out.extend_from_slice(br#""__floe_deleted":true,"__floe_version":"#);
    } else {
        out.extend_from_slice(br#""__floe_deleted":false,"__floe_version":"#);
    }
    write!(out, "{FLOE_JSON_VERSION}")?;
    Ok(())
}

fn append_floe_json_value(
    out: &mut Vec<u8>,
    value: &RowValue,
    data_type: &ColumnType,
) -> anyhow::Result<()> {
    match value {
        RowValue::Int64(value) => write!(out, "{value}")?,
        RowValue::Bool(value) => out.extend_from_slice(if *value { b"true" } else { b"false" }),
        RowValue::Utf8(value) => serde_json::to_writer(out, value)?,
        RowValue::TimestampMillis(value) => write!(out, "{value}")?,
        RowValue::DateDays(value) => write!(out, "{value}")?,
        RowValue::Decimal128(value) => match data_type {
            ColumnType::Decimal128 { scale, .. } => {
                serde_json::to_writer(out, &format_decimal128_for_json(*value, *scale))?;
            }
            _ => serde_json::to_writer(out, &value.to_string())?,
        },
        RowValue::Numeric(value) => serde_json::to_writer(out, value)?,
    }
    Ok(())
}

fn append_floe_json_columnar_value(
    out: &mut Vec<u8>,
    column: &CdcColumnarColumn,
    row_idx: usize,
    field: &FloeJsonColumnarField,
    allow_null: bool,
) -> anyhow::Result<()> {
    match column {
        CdcColumnarColumn::Int64(values) => match columnar_value(values, row_idx)? {
            Some(value) => write!(out, "{value}")?,
            None => append_floe_json_columnar_null(out, field, allow_null)?,
        },
        CdcColumnarColumn::Bool(values) => match columnar_value(values, row_idx)? {
            Some(value) => out.extend_from_slice(if *value { b"true" } else { b"false" }),
            None => append_floe_json_columnar_null(out, field, allow_null)?,
        },
        CdcColumnarColumn::Utf8(values) => match columnar_value(values, row_idx)? {
            Some(value) => serde_json::to_writer(out, value)?,
            None => append_floe_json_columnar_null(out, field, allow_null)?,
        },
        CdcColumnarColumn::TimestampMillis(values) => match columnar_value(values, row_idx)? {
            Some(value) => write!(out, "{value}")?,
            None => append_floe_json_columnar_null(out, field, allow_null)?,
        },
        CdcColumnarColumn::DateDays(values) => match columnar_value(values, row_idx)? {
            Some(value) => write!(out, "{value}")?,
            None => append_floe_json_columnar_null(out, field, allow_null)?,
        },
        CdcColumnarColumn::Decimal128 { values, .. } => match columnar_value(values, row_idx)? {
            Some(value) => match &field.data_type {
                ColumnType::Decimal128 { scale, .. } => {
                    serde_json::to_writer(out, &format_decimal128_for_json(*value, *scale))?;
                }
                _ => serde_json::to_writer(out, &value.to_string())?,
            },
            None => append_floe_json_columnar_null(out, field, allow_null)?,
        },
        CdcColumnarColumn::Numeric(values) => match columnar_value(values, row_idx)? {
            Some(value) => serde_json::to_writer(out, value)?,
            None => append_floe_json_columnar_null(out, field, allow_null)?,
        },
    }
    Ok(())
}

fn columnar_value<T>(values: &[Option<T>], row_idx: usize) -> anyhow::Result<Option<&T>> {
    values
        .get(row_idx)
        .map(Option::as_ref)
        .ok_or_else(|| anyhow!("CDC columnar row index {row_idx} out of bounds"))
}

fn append_floe_json_columnar_null(
    out: &mut Vec<u8>,
    field: &FloeJsonColumnarField,
    allow_null: bool,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        allow_null,
        "CDC primary key column '{}' cannot be NULL",
        field.name
    );
    out.extend_from_slice(b"null");
    Ok(())
}

fn format_decimal128_for_json(value: i128, scale: i8) -> String {
    if scale <= 0 {
        return value.to_string();
    }
    let scale = scale as u32;
    let factor = 10_i128.pow(scale);
    let sign = if value < 0 { "-" } else { "" };
    let magnitude = value.abs();
    let whole = magnitude / factor;
    let fraction = magnitude % factor;
    format!("{sign}{whole}.{fraction:0width$}", width = scale as usize)
}

fn encode_debezium_pipeline_records(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    batch: &ChangeBatch,
    transaction: &TransactionBatch,
) -> anyhow::Result<Vec<DebeziumEncodedRecord>> {
    let config = DebeziumEnvelopeConfig::new(&plan.source_name)?
        .with_database_name(&plan.database_name)
        .with_emit_tombstones(plan.emit_tombstones)
        .with_transaction_metadata(plan.include_transaction_metadata);
    let is_snapshot = transaction
        .transaction_id()
        .is_some_and(|tx| tx.as_str().starts_with("snapshot:"));
    let mut records = Vec::new();
    for (idx, change) in batch.changes().iter().enumerate() {
        let context = DebeziumEncodeContext {
            source_position: Some(transaction.commit_position()),
            transaction_id: transaction.transaction_id(),
            sequence: Some(u64::try_from(idx).unwrap_or(u64::MAX)),
            ts_ms: None,
        };
        if is_snapshot && let CdcChange::Insert { row } = change {
            records.push(encode_debezium_snapshot_row(schema, row, &config, context)?);
            continue;
        }
        records.extend(encode_debezium_change(schema, change, &config, context)?);
    }
    Ok(records)
}

fn encode_debezium_snapshot_pipeline_records(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    rows: &CdcColumnarRowBatch,
    transaction: &TransactionBatch,
) -> anyhow::Result<Vec<DebeziumEncodedRecord>> {
    let config = DebeziumEnvelopeConfig::new(&plan.source_name)?
        .with_database_name(&plan.database_name)
        .with_emit_tombstones(plan.emit_tombstones)
        .with_transaction_metadata(plan.include_transaction_metadata);
    let mut records = Vec::with_capacity(rows.row_count());
    for row_idx in 0..rows.row_count() {
        let row = rows.row(row_idx)?;
        let context = DebeziumEncodeContext {
            source_position: Some(transaction.commit_position()),
            transaction_id: transaction.transaction_id(),
            sequence: Some(u64::try_from(row_idx).unwrap_or(u64::MAX)),
            ts_ms: None,
        };
        records.push(encode_debezium_snapshot_row(
            schema, &row, &config, context,
        )?);
    }
    Ok(records)
}

fn debezium_records_to_buffer_records(
    records: &[DebeziumEncodedRecord],
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    records
        .iter()
        .map(|record| {
            Ok(CdcBufferRecord::new(
                record.key_json_bytes()?,
                record.value_json_bytes()?,
            ))
        })
        .collect()
}

fn encode_arrow_ipc_pipeline_records(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    batch: &ChangeBatch,
    transaction: &TransactionBatch,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    let mut records = Vec::new();
    let rows_per_record = *REPLICATION_ARROW_IPC_ROWS_PER_RECORD;
    let mut builder = ArrowIpcChangeBatchBuilder::new(schema, rows_per_record);
    let is_snapshot = transaction
        .transaction_id()
        .is_some_and(|tx| tx.as_str().starts_with("snapshot:"));
    for (idx, change) in batch.changes().iter().enumerate() {
        let sequence = u64::try_from(idx).unwrap_or(u64::MAX);
        match change {
            CdcChange::Insert { row } => {
                builder.append_row(row, if is_snapshot { "r" } else { "c" }, 1, sequence)?;
            }
            CdcChange::Update { before, after, .. } => {
                if let Some(before) = before {
                    builder.append_row(before, "u_before", -1, sequence)?;
                    flush_arrow_ipc_record_if_full(plan, transaction, &mut builder, &mut records)?;
                }
                builder.append_row(after, "u", 1, sequence)?;
            }
            CdcChange::Delete { key, before } => match before {
                Some(row) => builder.append_row(row, "d", -1, sequence)?,
                None => {
                    let key = key.as_ref().ok_or_else(|| {
                        anyhow!(
                            "CDC Arrow IPC delete for table '{}' requires a key or before row",
                            schema.table_id().as_str()
                        )
                    })?;
                    let key_row = key_only_row(schema, key)?;
                    builder.append_values(&key_row, "d", -1, sequence)?;
                }
            },
            CdcChange::Truncate => {
                return Err(anyhow!(
                    "CDC Arrow IPC truncate for table '{}' is not supported",
                    schema.table_id().as_str()
                ));
            }
        }
        flush_arrow_ipc_record_if_full(plan, transaction, &mut builder, &mut records)?;
    }
    if !builder.is_empty() {
        records.push(finish_arrow_ipc_record(plan, transaction, &mut builder)?);
    }
    Ok(records)
}

fn encode_arrow_ipc_snapshot_pipeline_records(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    rows: &CdcColumnarRowBatch,
    transaction: &TransactionBatch,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    schema.validate_columnar_rows(rows)?;
    let mut records = Vec::new();
    let rows_per_record = *REPLICATION_ARROW_IPC_ROWS_PER_RECORD;
    for start in (0..rows.row_count()).step_by(rows_per_record) {
        let len = rows.row_count().saturating_sub(start).min(rows_per_record);
        let batch = arrow_ipc_snapshot_record_batch(schema, rows, start, len)?;
        records.push(arrow_ipc_record_from_batch(
            plan,
            transaction,
            start / rows_per_record,
            batch,
        )?);
    }
    Ok(records)
}

fn flush_arrow_ipc_record_if_full(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    builder: &mut ArrowIpcChangeBatchBuilder,
    records: &mut Vec<CdcBufferRecord>,
) -> anyhow::Result<()> {
    if builder.is_full() {
        records.push(finish_arrow_ipc_record(plan, transaction, builder)?);
    }
    Ok(())
}

fn finish_arrow_ipc_record(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    builder: &mut ArrowIpcChangeBatchBuilder,
) -> anyhow::Result<CdcBufferRecord> {
    let chunk_idx = builder.chunk_idx();
    let batch = builder.finish()?;
    arrow_ipc_record_from_batch(plan, transaction, chunk_idx, batch)
}

fn arrow_ipc_record_from_batch(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    chunk_idx: usize,
    batch: RecordBatch,
) -> anyhow::Result<CdcBufferRecord> {
    let mut value = Vec::new();
    {
        let mut writer = arrow_ipc_stream_writer(&mut value, batch.schema().as_ref())
            .context("create replication Arrow IPC writer")?;
        writer
            .write(&batch)
            .context("write replication Arrow IPC batch")?;
        writer
            .finish()
            .context("finish replication Arrow IPC batch")?;
    }
    let key = format!(
        "{}/{}/{chunk_idx:020}",
        plan.upstream_table,
        source_position_key(transaction.commit_position())
    )
    .into_bytes();
    Ok(CdcBufferRecord::new(Some(key), Some(value)))
}

fn arrow_ipc_stream_writer<'a>(
    value: &'a mut Vec<u8>,
    schema: &ArrowSchema,
) -> anyhow::Result<StreamWriter<&'a mut Vec<u8>>> {
    let Some(compression) = *REPLICATION_ARROW_IPC_COMPRESSION else {
        return StreamWriter::try_new(value, schema)
            .context("create uncompressed Arrow IPC writer");
    };
    let options = IpcWriteOptions::try_new(64, false, MetadataVersion::V5)
        .context("create Arrow IPC writer options")?
        .try_with_compression(Some(compression.arrow_type()))
        .context("configure Arrow IPC compression")?;
    StreamWriter::try_new_with_options(value, schema, options)
        .with_context(|| format!("create {compression:?} Arrow IPC writer"))
}

fn arrow_ipc_snapshot_record_batch(
    schema: &CdcTableSchema,
    rows: &CdcColumnarRowBatch,
    start: usize,
    len: usize,
) -> anyhow::Result<RecordBatch> {
    let end = start.saturating_add(len);
    anyhow::ensure!(
        end <= rows.row_count(),
        "CDC Arrow IPC snapshot range {start}..{end} exceeds {} rows",
        rows.row_count()
    );
    let mut arrays = rows
        .columns()
        .iter()
        .zip(schema.columns())
        .map(|(values, column)| arrow_ipc_columnar_array(values, column, start, end))
        .collect::<anyhow::Result<Vec<_>>>()?;

    let mut operations = StringBuilder::with_capacity(len, len);
    let mut diffs = Int64Builder::with_capacity(len);
    let mut sequences = Int64Builder::with_capacity(len);
    for sequence in start..end {
        operations.append_value("r");
        diffs.append_value(1);
        sequences.append_value(i64::try_from(sequence).unwrap_or(i64::MAX));
    }
    arrays.push(Arc::new(operations.finish()));
    arrays.push(Arc::new(diffs.finish()));
    arrays.push(Arc::new(sequences.finish()));

    RecordBatch::try_new(arrow_ipc_schema(schema), arrays)
        .context("build replication Arrow IPC snapshot batch")
}

fn arrow_ipc_columnar_array(
    values: &CdcColumnarColumn,
    column: &CdcColumn,
    start: usize,
    end: usize,
) -> anyhow::Result<ArrayRef> {
    anyhow::ensure!(
        values.data_type() == column.data_type().clone(),
        "CDC Arrow IPC snapshot column '{}' type {:?} does not match {:?}",
        column.name(),
        values.data_type(),
        column.data_type()
    );
    let array: ArrayRef = match values {
        CdcColumnarColumn::Int64(values) => {
            Arc::new(arrow_array::Int64Array::from(values[start..end].to_vec()))
        }
        CdcColumnarColumn::Bool(values) => {
            Arc::new(arrow_array::BooleanArray::from(values[start..end].to_vec()))
        }
        CdcColumnarColumn::Utf8(values) => {
            let mut builder = StringBuilder::with_capacity(end - start, (end - start) * 16);
            for value in &values[start..end] {
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        CdcColumnarColumn::TimestampMillis(values) => Arc::new(
            arrow_array::TimestampMillisecondArray::from(values[start..end].to_vec()),
        ),
        CdcColumnarColumn::DateDays(values) => {
            Arc::new(arrow_array::Date32Array::from(values[start..end].to_vec()))
        }
        CdcColumnarColumn::Decimal128 {
            precision,
            scale,
            values,
        } => Arc::new(
            Decimal128Array::from(values[start..end].to_vec())
                .with_precision_and_scale(*precision, *scale)
                .context("build Decimal128 Arrow IPC snapshot column")?,
        ),
        CdcColumnarColumn::Numeric(values) => {
            let mut builder = StringBuilder::with_capacity(end - start, (end - start) * 16);
            for value in &values[start..end] {
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
    };
    Ok(array)
}

fn arrow_ipc_schema(schema: &CdcTableSchema) -> Arc<ArrowSchema> {
    let mut fields = schema
        .columns()
        .iter()
        .map(|column| {
            ArrowField::new(
                column.name(),
                match column.data_type() {
                    ColumnType::Int64 => DataType::Int64,
                    ColumnType::Bool => DataType::Boolean,
                    ColumnType::Utf8 => DataType::Utf8,
                    ColumnType::TimestampMillis => {
                        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
                    }
                    ColumnType::DateDays => DataType::Date32,
                    ColumnType::Decimal128 { precision, scale } => {
                        DataType::Decimal128(*precision, *scale)
                    }
                    ColumnType::Numeric => DataType::Utf8,
                },
                true,
            )
        })
        .collect::<Vec<_>>();
    fields.push(ArrowField::new("__op", DataType::Utf8, false));
    fields.push(ArrowField::new("__diff", DataType::Int64, false));
    fields.push(ArrowField::new("__sequence", DataType::Int64, false));
    Arc::new(ArrowSchema::new(fields))
}

fn key_only_row(schema: &CdcTableSchema, key: &CdcRowKey) -> anyhow::Result<Vec<Option<RowValue>>> {
    key.validate_against_schema(schema)?;
    let mut values = vec![None; schema.columns().len()];
    for (value, column_idx) in key.values().iter().zip(schema.primary_key_indices()) {
        values[column_idx] = Some(value.clone());
    }
    Ok(values)
}

fn source_position_key(position: &CdcSourcePosition) -> String {
    match position {
        CdcSourcePosition::Postgres {
            commit_lsn,
            event_lsn,
        } => match event_lsn {
            Some(event_lsn) => format!("pg/{commit_lsn}/{event_lsn}"),
            None => format!("pg/{commit_lsn}"),
        },
        CdcSourcePosition::Opaque { value } => format!("opaque/{value}"),
    }
}

fn cdc_replication_debug_state_from_snapshots(
    snapshots: Vec<ReplicationPipelineStatusSnapshot>,
) -> http_ingest::CdcReplicationDebugState {
    http_ingest::CdcReplicationDebugState {
        updated_at_unix_ms: current_unix_time_ms(),
        refresh_error: None,
        pipelines: snapshots
            .into_iter()
            .map(|snapshot| http_ingest::CdcReplicationDebugPipelineState {
                pipeline: snapshot.pipeline_name().to_string(),
                source: snapshot.source_name().to_string(),
                target_kind: snapshot.target_kind().to_string(),
                checkpoint_position: snapshot.checkpoint_position().map(source_position_key),
                checkpoint_transaction_id: snapshot
                    .checkpoint_transaction_id()
                    .map(|transaction_id| transaction_id.as_str().to_string()),
                target_state: snapshot.target_state().clone(),
                pending_transactions: snapshot.pending_transactions(),
                pending_records: snapshot.pending_records(),
                pending_bytes: snapshot.pending_bytes(),
                oldest_pending_age_ms: snapshot.oldest_pending_age_ms(),
                replaying: snapshot.replaying(),
                last_error: snapshot.last_error().map(str::to_string),
            })
            .collect(),
    }
}

enum ArrowIpcColumnBuilder {
    Int64(Int64Builder),
    Bool(BooleanBuilder),
    Utf8(StringBuilder),
    TimestampMillis(TimestampMillisecondBuilder),
    DateDays(Date32Builder),
    Decimal128(Decimal128Builder),
    Numeric(StringBuilder),
}

impl ArrowIpcColumnBuilder {
    fn new(data_type: &ColumnType, capacity: usize) -> Self {
        match data_type {
            ColumnType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            ColumnType::Bool => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            ColumnType::Utf8 => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 16)),
            ColumnType::TimestampMillis => {
                Self::TimestampMillis(TimestampMillisecondBuilder::with_capacity(capacity))
            }
            ColumnType::DateDays => Self::DateDays(Date32Builder::with_capacity(capacity)),
            ColumnType::Decimal128 { precision, scale } => Self::Decimal128(
                Decimal128Builder::with_capacity(capacity)
                    .with_data_type(DataType::Decimal128(*precision, *scale)),
            ),
            ColumnType::Numeric => {
                Self::Numeric(StringBuilder::with_capacity(capacity, capacity * 16))
            }
        }
    }

    fn append(&mut self, column: &CdcColumn, value: Option<&RowValue>) -> anyhow::Result<()> {
        match (self, column.data_type(), value) {
            (Self::Int64(builder), ColumnType::Int64, Some(RowValue::Int64(value))) => {
                builder.append_value(*value);
            }
            (Self::Bool(builder), ColumnType::Bool, Some(RowValue::Bool(value))) => {
                builder.append_value(*value);
            }
            (Self::Utf8(builder), ColumnType::Utf8, Some(RowValue::Utf8(value))) => {
                builder.append_value(value);
            }
            (
                Self::TimestampMillis(builder),
                ColumnType::TimestampMillis,
                Some(RowValue::TimestampMillis(value)),
            ) => {
                builder.append_value(*value);
            }
            (Self::DateDays(builder), ColumnType::DateDays, Some(RowValue::DateDays(value))) => {
                builder.append_value(*value);
            }
            (
                Self::Decimal128(builder),
                ColumnType::Decimal128 { .. },
                Some(RowValue::Decimal128(value)),
            ) => {
                builder.append_value(*value);
            }
            (Self::Numeric(builder), ColumnType::Numeric, Some(RowValue::Numeric(value))) => {
                builder.append_value(value);
            }
            (Self::Int64(builder), ColumnType::Int64, None) => builder.append_null(),
            (Self::Bool(builder), ColumnType::Bool, None) => builder.append_null(),
            (Self::Utf8(builder), ColumnType::Utf8, None) => builder.append_null(),
            (Self::TimestampMillis(builder), ColumnType::TimestampMillis, None) => {
                builder.append_null();
            }
            (Self::DateDays(builder), ColumnType::DateDays, None) => builder.append_null(),
            (Self::Decimal128(builder), ColumnType::Decimal128 { .. }, None) => {
                builder.append_null();
            }
            (Self::Numeric(builder), ColumnType::Numeric, None) => builder.append_null(),
            (_, _, Some(value)) => {
                return Err(anyhow!(
                    "CDC Arrow IPC value for column '{}' does not match type {:?}: {:?}",
                    column.name(),
                    column.data_type(),
                    value
                ));
            }
            _ => {
                return Err(anyhow!(
                    "CDC Arrow IPC builder for column '{}' does not match type {:?}",
                    column.name(),
                    column.data_type()
                ));
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::TimestampMillis(builder) => Arc::new(builder.finish()),
            Self::DateDays(builder) => Arc::new(builder.finish()),
            Self::Decimal128(builder) => Arc::new(builder.finish()),
            Self::Numeric(builder) => Arc::new(builder.finish()),
        }
    }
}

struct ArrowIpcChangeBatchBuilder {
    schema: CdcTableSchema,
    arrow_schema: Arc<ArrowSchema>,
    columns: Vec<ArrowIpcColumnBuilder>,
    operations: StringBuilder,
    diffs: Int64Builder,
    sequences: Int64Builder,
    len: usize,
    capacity: usize,
    chunk_idx: usize,
}

impl ArrowIpcChangeBatchBuilder {
    fn new(schema: &CdcTableSchema, capacity: usize) -> Self {
        Self {
            schema: schema.clone(),
            arrow_schema: arrow_ipc_schema(schema),
            columns: schema
                .columns()
                .iter()
                .map(|column| ArrowIpcColumnBuilder::new(column.data_type(), capacity))
                .collect(),
            operations: StringBuilder::with_capacity(capacity, capacity * 2),
            diffs: Int64Builder::with_capacity(capacity),
            sequences: Int64Builder::with_capacity(capacity),
            len: 0,
            capacity,
            chunk_idx: 0,
        }
    }

    fn append_row(
        &mut self,
        row: &CdcRow,
        operation: &str,
        diff: i64,
        sequence: u64,
    ) -> anyhow::Result<()> {
        self.append_values(row.values(), operation, diff, sequence)
    }

    fn append_values(
        &mut self,
        values: &[Option<RowValue>],
        operation: &str,
        diff: i64,
        sequence: u64,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            values.len() == self.schema.columns().len(),
            "CDC Arrow IPC row has {} values, expected {}",
            values.len(),
            self.schema.columns().len()
        );
        for ((builder, column), value) in self
            .columns
            .iter_mut()
            .zip(self.schema.columns())
            .zip(values)
        {
            builder.append(column, value.as_ref())?;
        }
        self.operations.append_value(operation);
        self.diffs.append_value(diff);
        self.sequences
            .append_value(i64::try_from(sequence).unwrap_or(i64::MAX));
        self.len += 1;
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn is_full(&self) -> bool {
        self.len >= self.capacity
    }

    fn chunk_idx(&self) -> usize {
        self.chunk_idx
    }

    fn finish(&mut self) -> anyhow::Result<RecordBatch> {
        let mut arrays = self
            .columns
            .iter_mut()
            .map(ArrowIpcColumnBuilder::finish)
            .collect::<Vec<_>>();
        arrays.push(Arc::new(self.operations.finish()));
        arrays.push(Arc::new(self.diffs.finish()));
        arrays.push(Arc::new(self.sequences.finish()));
        let batch = RecordBatch::try_new(Arc::clone(&self.arrow_schema), arrays)
            .context("build replication Arrow IPC batch")?;
        self.columns = self
            .schema
            .columns()
            .iter()
            .map(|column| ArrowIpcColumnBuilder::new(column.data_type(), self.capacity))
            .collect();
        self.operations = StringBuilder::with_capacity(self.capacity, self.capacity * 2);
        self.diffs = Int64Builder::with_capacity(self.capacity);
        self.sequences = Int64Builder::with_capacity(self.capacity);
        self.len = 0;
        self.chunk_idx += 1;
        Ok(batch)
    }
}

fn pending_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
) -> std::collections::BTreeMap<String, String> {
    let mut state = base_target_state(plan, manifest);
    state.insert("buffer.status".to_string(), "durable".to_string());
    state.insert("target.delivery.status".to_string(), "pending".to_string());
    state.insert(
        "target.delivery.replay_may_duplicate".to_string(),
        "true".to_string(),
    );
    state
}

fn delivered_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    mut target_state: std::collections::BTreeMap<String, String>,
) -> std::collections::BTreeMap<String, String> {
    target_state.extend(base_target_state(plan, manifest));
    target_state.insert("buffer.status".to_string(), "delivered".to_string());
    target_state.insert(
        "target.delivery.status".to_string(),
        "delivered".to_string(),
    );
    target_state.insert(
        "target.delivery.replay_may_duplicate".to_string(),
        "false".to_string(),
    );
    target_state
}

fn failed_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    err: &anyhow::Error,
) -> std::collections::BTreeMap<String, String> {
    let mut state = base_target_state(plan, manifest);
    state.insert("buffer.status".to_string(), "durable".to_string());
    state.insert("target.delivery.status".to_string(), "failed".to_string());
    state.insert(
        "target.delivery.replay_may_duplicate".to_string(),
        "true".to_string(),
    );
    state.insert(
        "target.last_error".to_string(),
        truncate_target_error(&format!("{err:#}")),
    );
    state
}

fn base_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
) -> std::collections::BTreeMap<String, String> {
    let mut state = std::collections::BTreeMap::new();
    state.insert("source.table".to_string(), plan.upstream_table.clone());
    state.insert("target.kind".to_string(), target_kind(plan).to_string());
    state.insert(
        "buffer.transaction_key".to_string(),
        manifest.transaction_key().to_string(),
    );
    state.insert(
        "buffer.record_count".to_string(),
        manifest.record_count().to_string(),
    );
    state.insert(
        "buffer.payload_format".to_string(),
        format!("{:?}", manifest.payload_format()),
    );
    if let Some(transaction_id) = manifest.transaction_id() {
        state.insert(
            "source.transaction_id".to_string(),
            transaction_id.as_str().to_string(),
        );
    }
    match manifest.source_position() {
        CdcSourcePosition::Postgres {
            commit_lsn,
            event_lsn,
        } => {
            state.insert(
                "source.position.postgres.commit_lsn".to_string(),
                commit_lsn.clone(),
            );
            if let Some(event_lsn) = event_lsn {
                state.insert(
                    "source.position.postgres.event_lsn".to_string(),
                    event_lsn.clone(),
                );
            }
        }
        CdcSourcePosition::Opaque { value } => {
            state.insert("source.position".to_string(), value.clone());
        }
    }
    state
}

fn target_kind(plan: &ReplicationPipelineRuntimePlan) -> &'static str {
    match &plan.target {
        ReplicationPipelineRuntimeTarget::Kafka { .. } => "kafka",
    }
}

fn truncate_target_error(message: &str) -> String {
    const MAX_ERROR_LEN: usize = 512;
    if message.len() <= MAX_ERROR_LEN {
        return message.to_string();
    }
    let mut truncated = message
        .chars()
        .take(MAX_ERROR_LEN.saturating_sub(3))
        .collect::<String>();
    truncated.push_str("...");
    truncated
}

async fn record_buffer_stats(
    buffer_store: &CdcBufferStore,
    pipeline_name: &str,
) -> anyhow::Result<()> {
    let stats = buffer_store
        .stats(pipeline_name, current_unix_time_ms())
        .await
        .with_context(|| format!("load CDC buffer stats for pipeline '{pipeline_name}'"))?;
    crate::metrics::record_cdc_buffer_pending(
        pipeline_name,
        stats.pending_transactions(),
        stats.pending_records(),
        stats.pending_bytes(),
        stats.oldest_pending_age_ms(),
    );
    Ok(())
}

fn log_replication_pipeline_perf(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    records: usize,
    payload_bytes: usize,
    encode_elapsed: Duration,
    total_elapsed: Duration,
) {
    if !*CDC_PERF_LOGGING_ENABLED {
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

fn log_replication_durable_stage_perf(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    records: usize,
    append_send_elapsed: Duration,
    buffer_append_elapsed: Duration,
    target_send_elapsed: Duration,
    checkpoint_elapsed: Duration,
    delivery_elapsed: Duration,
) {
    if !*CDC_PERF_LOGGING_ENABLED {
        return;
    }
    tracing::info!(
        pipeline = %plan.name,
        records,
        buffer_payload_format = ?manifest.payload_format(),
        buffer_payload_bytes = manifest.payload_bytes(),
        append_and_target_send_ms = append_send_elapsed.as_millis() as u64,
        buffer_append_ms = buffer_append_elapsed.as_millis() as u64,
        target_send_ms = target_send_elapsed.as_millis() as u64,
        pending_checkpoint_ms = checkpoint_elapsed.as_millis() as u64,
        delivery_checkpoint_ms = delivery_elapsed.as_millis() as u64,
        "postgres cdc durable replication pipeline stages processed"
    );
}

fn log_replication_buffer_append_perf(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    append_elapsed: Duration,
) {
    if !*CDC_PERF_LOGGING_ENABLED {
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

fn log_replication_replay_payload_perf(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    payload_load_elapsed: Duration,
    encode_elapsed: Duration,
    records: usize,
) {
    if !*CDC_PERF_LOGGING_ENABLED {
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

fn log_replication_replay_delivery_perf(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    delivery_elapsed: Duration,
    delivered_records: usize,
) {
    if !*CDC_PERF_LOGGING_ENABLED {
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

fn estimated_buffer_payload_bytes(records: &[CdcBufferRecord]) -> usize {
    records.iter().fold(16usize, |bytes, record| {
        bytes.saturating_add(16).saturating_add(record.byte_len())
    })
}

fn buffer_limit_violation(
    pending_bytes: usize,
    oldest_pending_age_ms: Option<u64>,
    incoming_bytes: usize,
    limits: ReplicationBufferLimits,
) -> Option<ReplicationBufferLimitViolation> {
    if let Some(max_pending_bytes) = limits.max_pending_bytes
        && pending_bytes.saturating_add(incoming_bytes) > max_pending_bytes
    {
        return Some(ReplicationBufferLimitViolation::PendingBytes {
            pending_bytes,
            incoming_bytes,
            max_pending_bytes,
        });
    }
    if let Some(max_pending_age_ms) = limits.max_pending_age_ms
        && let Some(oldest_pending_age_ms) = oldest_pending_age_ms
        && oldest_pending_age_ms > max_pending_age_ms
    {
        return Some(ReplicationBufferLimitViolation::PendingAge {
            oldest_pending_age_ms,
            max_pending_age_ms,
        });
    }
    None
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

#[cfg(test)]
mod tests {
    use super::*;
    use floe_cdc_core::{CdcColumn, CdcPrimaryKey, CdcRow, CdcTransactionId, UpstreamTableRef};
    use floe_core::RowValue;
    use floe_core::catalog::ColumnType;

    #[test]
    fn materialized_transaction_filters_non_materialized_batches() {
        let source_id = CdcSourceId::new("pg_main").unwrap();
        let materialized = CdcTableId::new("orders").unwrap();
        let passthrough = CdcTableId::new("pg_main:public.customers").unwrap();
        let transaction = TransactionBatch::new(
            source_id.clone(),
            None,
            None,
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
            vec![
                ChangeBatch::new(
                    materialized.clone(),
                    vec![CdcChange::Insert {
                        row: row(1, "open"),
                    }],
                )
                .unwrap(),
                ChangeBatch::new(passthrough, vec![CdcChange::Insert { row: row(2, "new") }])
                    .unwrap(),
            ],
        )
        .unwrap();

        let filtered = materialized_transaction(
            &source_id,
            &HashSet::from([materialized.clone()]),
            &transaction,
        )
        .unwrap()
        .unwrap();

        assert_eq!(filtered.change_batches().len(), 1);
        assert_eq!(filtered.change_batches()[0].table_id(), &materialized);
    }

    #[test]
    fn pipeline_snapshot_records_use_read_operation() {
        let plan = ReplicationPipelineRuntimePlan {
            name: "p".to_string(),
            source_name: "pg_main".to_string(),
            database_name: "postgres".to_string(),
            upstream_table: "public.orders".to_string(),
            table_id: CdcTableId::new("orders").unwrap(),
            schema: schema(CdcTableId::new("orders").unwrap()),
            target: ReplicationPipelineRuntimeTarget::Kafka {
                brokers: "localhost:9092".to_string(),
                topic: "orders".to_string(),
            },
            format: ReplicationPipelineRuntimeFormat::DebeziumJson,
            buffer_mode: ReplicationPipelineRuntimeBufferMode::Durable,
            buffer_policy: CatalogReplicationBufferPolicy::default(),
            emit_tombstones: false,
            include_transaction_metadata: false,
        };
        let schema = schema(plan.table_id.clone());
        let batch = ChangeBatch::new(
            plan.table_id.clone(),
            vec![CdcChange::Insert {
                row: row(1, "open"),
            }],
        )
        .unwrap();
        let transaction = TransactionBatch::new(
            CdcSourceId::new("pg_main").unwrap(),
            Some(CdcTransactionId::new("snapshot:0/16B6C50").unwrap()),
            None,
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
            vec![batch.clone()],
        )
        .unwrap();

        let records =
            encode_debezium_pipeline_records(&plan, &schema, &batch, &transaction).unwrap();
        assert_eq!(records.len(), 1);
        let payload = &records[0].value().unwrap()["payload"];
        assert_eq!(payload["op"], "r");
        assert_eq!(payload["source"]["snapshot"], "true");
        assert_eq!(payload["source"]["db"], "postgres");
    }

    #[test]
    fn pipeline_debezium_records_are_buffered_as_encoded_kafka_payloads() {
        let plan = ReplicationPipelineRuntimePlan {
            name: "p".to_string(),
            source_name: "pg_main".to_string(),
            database_name: "postgres".to_string(),
            upstream_table: "public.orders".to_string(),
            table_id: CdcTableId::new("orders").unwrap(),
            schema: schema(CdcTableId::new("orders").unwrap()),
            target: ReplicationPipelineRuntimeTarget::Kafka {
                brokers: "localhost:9092".to_string(),
                topic: "orders".to_string(),
            },
            format: ReplicationPipelineRuntimeFormat::DebeziumJson,
            buffer_mode: ReplicationPipelineRuntimeBufferMode::Durable,
            buffer_policy: CatalogReplicationBufferPolicy::default(),
            emit_tombstones: false,
            include_transaction_metadata: true,
        };
        let schema = schema(plan.table_id.clone());
        let batch = ChangeBatch::new(
            plan.table_id.clone(),
            vec![CdcChange::Insert {
                row: row(1, "open"),
            }],
        )
        .unwrap();
        let transaction = TransactionBatch::new(
            CdcSourceId::new("pg_main").unwrap(),
            Some(CdcTransactionId::new("pg-xid-55").unwrap()),
            None,
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
            vec![batch.clone()],
        )
        .unwrap();

        let records = encode_pipeline_buffer_records(&plan, &schema, &batch, &transaction)
            .expect("encode debezium records");
        let value: serde_json::Value =
            serde_json::from_slice(records[0].value().expect("value")).unwrap();
        assert_eq!(value["schema"]["name"], "pg_main.public.orders.Envelope");
        assert_eq!(value["payload"]["source"]["txId"], 55);

        let prepared =
            prepare_replication_buffer_append(&plan, &transaction, records.clone()).unwrap();
        assert_eq!(
            prepared.append.payload_format(),
            CdcBufferPayloadFormat::KafkaRecords
        );
        assert_eq!(prepared.append.records(), records.as_slice());
        assert!(prepared.target_records.is_none());
    }

    #[test]
    fn pipeline_floe_json_records_encode_compact_row_messages() {
        let plan = ReplicationPipelineRuntimePlan {
            name: "p".to_string(),
            source_name: "pg_main".to_string(),
            database_name: "postgres".to_string(),
            upstream_table: "public.orders".to_string(),
            table_id: CdcTableId::new("orders").unwrap(),
            schema: schema(CdcTableId::new("orders").unwrap()),
            target: ReplicationPipelineRuntimeTarget::Kafka {
                brokers: "localhost:9092".to_string(),
                topic: "orders".to_string(),
            },
            format: ReplicationPipelineRuntimeFormat::FloeJson,
            buffer_mode: ReplicationPipelineRuntimeBufferMode::Durable,
            buffer_policy: CatalogReplicationBufferPolicy::default(),
            emit_tombstones: false,
            include_transaction_metadata: false,
        };
        let schema = schema(plan.table_id.clone());
        let batch = ChangeBatch::new(
            plan.table_id.clone(),
            vec![
                CdcChange::Insert {
                    row: row(1, "open"),
                },
                CdcChange::Delete {
                    key: Some(CdcRowKey::new([RowValue::Int64(2)]).unwrap()),
                    before: None,
                },
            ],
        )
        .unwrap();
        let transaction = TransactionBatch::new(
            CdcSourceId::new("pg_main").unwrap(),
            Some(CdcTransactionId::new("tx-1").unwrap()),
            None,
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
            vec![batch.clone()],
        )
        .unwrap();

        let records = encode_pipeline_buffer_records(&plan, &schema, &batch, &transaction)
            .expect("encode floe json records");
        assert_eq!(records.len(), 2);
        let first_key: serde_json::Value =
            serde_json::from_slice(records[0].key().expect("key")).unwrap();
        let first_value: serde_json::Value =
            serde_json::from_slice(records[0].value().expect("value")).unwrap();
        let delete_value: serde_json::Value =
            serde_json::from_slice(records[1].value().expect("delete value")).unwrap();

        assert_eq!(first_key, serde_json::json!({"id": 1}));
        assert_eq!(first_value["id"], 1);
        assert_eq!(first_value["status"], "open");
        assert_eq!(first_value[FLOE_JSON_DELETED_FIELD], false);
        assert_eq!(first_value[FLOE_JSON_VERSION_FIELD], FLOE_JSON_VERSION);
        assert_eq!(delete_value["id"], 2);
        assert_eq!(delete_value[FLOE_JSON_DELETED_FIELD], true);
        assert_eq!(delete_value[FLOE_JSON_VERSION_FIELD], FLOE_JSON_VERSION);
    }

    #[test]
    fn pipeline_arrow_ipc_records_encode_batches_without_json() {
        let plan = ReplicationPipelineRuntimePlan {
            name: "p".to_string(),
            source_name: "pg_main".to_string(),
            database_name: "postgres".to_string(),
            upstream_table: "public.orders".to_string(),
            table_id: CdcTableId::new("orders").unwrap(),
            schema: schema(CdcTableId::new("orders").unwrap()),
            target: ReplicationPipelineRuntimeTarget::Kafka {
                brokers: "localhost:9092".to_string(),
                topic: "orders".to_string(),
            },
            format: ReplicationPipelineRuntimeFormat::ArrowIpc,
            buffer_mode: ReplicationPipelineRuntimeBufferMode::Durable,
            buffer_policy: CatalogReplicationBufferPolicy::default(),
            emit_tombstones: false,
            include_transaction_metadata: false,
        };
        let schema = schema(plan.table_id.clone());
        let batch = ChangeBatch::new(
            plan.table_id.clone(),
            vec![
                CdcChange::Insert {
                    row: row(1, "open"),
                },
                CdcChange::Update {
                    key: None,
                    before: Some(row(1, "open")),
                    after: row(1, "paid"),
                },
            ],
        )
        .unwrap();
        let transaction = TransactionBatch::new(
            CdcSourceId::new("pg_main").unwrap(),
            Some(CdcTransactionId::new("tx-1").unwrap()),
            None,
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
            vec![batch.clone()],
        )
        .unwrap();

        let records = encode_pipeline_buffer_records(&plan, &schema, &batch, &transaction)
            .expect("encode arrow records");
        assert_eq!(records.len(), 1);
        let payload = records[0].value().expect("payload");
        assert!(!payload.starts_with(b"{"));

        let mut reader =
            arrow_ipc::reader::StreamReader::try_new(payload, None).expect("arrow reader");
        let batch = reader
            .next()
            .expect("one record batch")
            .expect("decode batch");
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(2).name(), "__op");
    }

    #[test]
    fn pipeline_arrow_ipc_records_encode_columnar_snapshot_without_json() {
        let plan = ReplicationPipelineRuntimePlan {
            name: "p".to_string(),
            source_name: "pg_main".to_string(),
            database_name: "postgres".to_string(),
            upstream_table: "public.orders".to_string(),
            table_id: CdcTableId::new("orders").unwrap(),
            schema: schema(CdcTableId::new("orders").unwrap()),
            target: ReplicationPipelineRuntimeTarget::Kafka {
                brokers: "localhost:9092".to_string(),
                topic: "orders".to_string(),
            },
            format: ReplicationPipelineRuntimeFormat::ArrowIpc,
            buffer_mode: ReplicationPipelineRuntimeBufferMode::Durable,
            buffer_policy: CatalogReplicationBufferPolicy::default(),
            emit_tombstones: false,
            include_transaction_metadata: false,
        };
        let schema = schema(plan.table_id.clone());
        let rows = CdcColumnarRowBatch::new(vec![
            CdcColumnarColumn::Int64(vec![Some(1), Some(2)]),
            CdcColumnarColumn::Utf8(vec![Some("open".to_string()), Some("paid".to_string())]),
        ])
        .unwrap();
        let batch =
            ChangeBatch::new_snapshot_insert(plan.table_id.clone(), rows).expect("snapshot batch");
        let transaction = TransactionBatch::new(
            CdcSourceId::new("pg_main").unwrap(),
            Some(CdcTransactionId::new("snapshot:0/16B6C50").unwrap()),
            None,
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
            vec![batch.clone()],
        )
        .unwrap();

        let records = encode_pipeline_buffer_records(&plan, &schema, &batch, &transaction)
            .expect("encode arrow snapshot records");
        assert_eq!(records.len(), 1);
        let payload = records[0].value().expect("payload");
        assert!(!payload.starts_with(b"{"));

        let mut reader =
            arrow_ipc::reader::StreamReader::try_new(payload, None).expect("arrow reader");
        let batch = reader
            .next()
            .expect("one record batch")
            .expect("decode batch");
        assert_eq!(batch.num_rows(), 2);
        let ops = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("op column");
        assert_eq!(ops.value(0), "r");
        assert_eq!(ops.value(1), "r");
    }

    #[test]
    fn pipeline_transaction_records_include_all_matching_snapshot_chunks() {
        let plan = ReplicationPipelineRuntimePlan {
            name: "p".to_string(),
            source_name: "pg_main".to_string(),
            database_name: "postgres".to_string(),
            upstream_table: "public.orders".to_string(),
            table_id: CdcTableId::new("orders").unwrap(),
            schema: schema(CdcTableId::new("orders").unwrap()),
            target: ReplicationPipelineRuntimeTarget::Kafka {
                brokers: "localhost:9092".to_string(),
                topic: "orders".to_string(),
            },
            format: ReplicationPipelineRuntimeFormat::ArrowIpc,
            buffer_mode: ReplicationPipelineRuntimeBufferMode::Durable,
            buffer_policy: CatalogReplicationBufferPolicy::default(),
            emit_tombstones: false,
            include_transaction_metadata: false,
        };
        let schema = schema(plan.table_id.clone());
        let first_rows = CdcColumnarRowBatch::new(vec![
            CdcColumnarColumn::Int64(vec![Some(1)]),
            CdcColumnarColumn::Utf8(vec![Some("open".to_string())]),
        ])
        .unwrap();
        let second_rows = CdcColumnarRowBatch::new(vec![
            CdcColumnarColumn::Int64(vec![Some(2), Some(3)]),
            CdcColumnarColumn::Utf8(vec![Some("paid".to_string()), Some("void".to_string())]),
        ])
        .unwrap();
        let transaction = TransactionBatch::new(
            CdcSourceId::new("pg_main").unwrap(),
            Some(CdcTransactionId::new("snapshot:0/16B6C50").unwrap()),
            None,
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
            vec![
                ChangeBatch::new_snapshot_insert(plan.table_id.clone(), first_rows)
                    .expect("first snapshot batch"),
                ChangeBatch::new_snapshot_insert(plan.table_id.clone(), second_rows)
                    .expect("second snapshot batch"),
            ],
        )
        .unwrap();
        let schemas = HashMap::from([(plan.table_id.clone(), schema)]);

        let records = encode_pipeline_transaction_records(&plan, &schemas, &transaction)
            .expect("encode transaction records");

        assert_eq!(records.len(), 2);
        let decoded_rows = records
            .iter()
            .map(|record| {
                let payload = record.value().expect("payload");
                let mut reader =
                    arrow_ipc::reader::StreamReader::try_new(payload, None).expect("arrow reader");
                reader
                    .next()
                    .expect("one record batch")
                    .expect("decode batch")
                    .num_rows()
            })
            .collect::<Vec<_>>();
        assert_eq!(decoded_rows, vec![1, 2]);
    }

    #[test]
    fn pipeline_transaction_records_filter_multi_table_transactions_per_target() {
        let orders_id = CdcTableId::new("orders").unwrap();
        let customers_id = CdcTableId::new("customers").unwrap();
        let orders_plan = test_plan("orders_pipe", orders_id.clone(), "public.orders");
        let customers_plan = test_plan("customers_pipe", customers_id.clone(), "public.customers");
        let schemas = HashMap::from([
            (
                orders_id.clone(),
                schema_for_table(orders_id.clone(), "orders"),
            ),
            (
                customers_id.clone(),
                schema_for_table(customers_id.clone(), "customers"),
            ),
        ]);
        let transaction = TransactionBatch::new(
            CdcSourceId::new("pg_main").unwrap(),
            Some(CdcTransactionId::new("pg-xid-77").unwrap()),
            None,
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
            vec![
                ChangeBatch::new(
                    orders_id.clone(),
                    vec![CdcChange::Insert {
                        row: row(1, "open"),
                    }],
                )
                .unwrap(),
                ChangeBatch::new(
                    customers_id.clone(),
                    vec![CdcChange::Insert {
                        row: row(9, "active"),
                    }],
                )
                .unwrap(),
                ChangeBatch::new(
                    orders_id.clone(),
                    vec![CdcChange::Insert {
                        row: row(2, "paid"),
                    }],
                )
                .unwrap(),
            ],
        )
        .unwrap();

        let orders_records =
            encode_pipeline_transaction_records(&orders_plan, &schemas, &transaction).unwrap();
        let customers_records =
            encode_pipeline_transaction_records(&customers_plan, &schemas, &transaction).unwrap();

        assert_eq!(orders_records.len(), 2);
        assert_eq!(customers_records.len(), 1);
        let first_order: serde_json::Value =
            serde_json::from_slice(orders_records[0].value().expect("first order")).unwrap();
        let second_order: serde_json::Value =
            serde_json::from_slice(orders_records[1].value().expect("second order")).unwrap();
        let customer: serde_json::Value =
            serde_json::from_slice(customers_records[0].value().expect("customer")).unwrap();
        assert_eq!(first_order["id"], 1);
        assert_eq!(second_order["id"], 2);
        assert_eq!(customer["id"], 9);
    }

    #[test]
    fn pipeline_checkpoint_preserves_transaction_schema_versions() {
        let transaction = TransactionBatch::new(
            CdcSourceId::new("pg_main").unwrap(),
            Some(CdcTransactionId::new("pg-xid-77").unwrap()),
            None,
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
            vec![
                ChangeBatch::new(
                    CdcTableId::new("orders").unwrap(),
                    vec![CdcChange::Insert {
                        row: row(1, "open"),
                    }],
                )
                .unwrap(),
            ],
        )
        .unwrap()
        .with_schema_versions(floe_cdc_core::CdcSchemaVersionMap::from([(
            "orders".to_string(),
            42,
        )]));

        let checkpoint = pipeline_checkpoint_from_transaction(&transaction);

        assert_eq!(checkpoint.schema_versions().get("orders"), Some(&42));
    }

    #[tokio::test]
    async fn target_checkpoint_state_makes_partial_delivery_explicit() {
        let table_id = CdcTableId::new("orders").unwrap();
        let plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
        let transaction = TransactionBatch::new(
            CdcSourceId::new("pg_main").unwrap(),
            Some(CdcTransactionId::new("pg-xid-77").unwrap()),
            None,
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
            vec![
                ChangeBatch::new(
                    table_id,
                    vec![CdcChange::Insert {
                        row: row(1, "open"),
                    }],
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let records = vec![CdcBufferRecord::new(Some(vec![1]), Some(vec![2]))];
        let prepared = prepare_replication_buffer_append(&plan, &transaction, records).unwrap();
        let storage = SlateCatalog::in_memory().await.unwrap();
        let buffer_store = storage.cdc_buffer_store();
        let manifest = buffer_store
            .append_transaction(&prepared.append)
            .await
            .unwrap();

        let pending = pending_target_state(&plan, &manifest);
        assert_eq!(pending["buffer.status"], "durable");
        assert_eq!(pending["target.delivery.status"], "pending");
        assert_eq!(pending["target.delivery.replay_may_duplicate"], "true");
        assert_eq!(pending["target.kind"], "kafka");
        assert_eq!(pending["source.position.postgres.commit_lsn"], "0/16B6C50");

        let delivered = delivered_target_state(
            &plan,
            &manifest,
            std::collections::BTreeMap::from([
                ("kafka.topic".to_string(), "orders".to_string()),
                ("kafka.partition.0.offset".to_string(), "42".to_string()),
            ]),
        );
        assert_eq!(delivered["buffer.status"], "delivered");
        assert_eq!(delivered["target.delivery.status"], "delivered");
        assert_eq!(delivered["target.delivery.replay_may_duplicate"], "false");
        assert_eq!(delivered["kafka.partition.0.offset"], "42");

        let failed = failed_target_state(&plan, &manifest, &anyhow!("kafka unavailable"));
        assert_eq!(failed["buffer.status"], "durable");
        assert_eq!(failed["target.delivery.status"], "failed");
        assert_eq!(failed["target.delivery.replay_may_duplicate"], "true");
        assert!(failed["target.last_error"].contains("kafka unavailable"));
    }

    #[tokio::test]
    async fn status_snapshots_expose_buffer_checkpoint_replay_and_error_state() {
        let table_id = CdcTableId::new("orders").unwrap();
        let plan = test_plan("orders_pipe", table_id.clone(), "public.orders");
        let runtime = test_runtime_with_plan(plan.clone());
        runtime.set_replay_state(&plan.name, true);
        runtime.set_last_target_error(&plan.name, "kafka unavailable".to_string());
        let transaction = TransactionBatch::new(
            CdcSourceId::new("pg_main").unwrap(),
            Some(CdcTransactionId::new("pg-xid-77").unwrap()),
            None,
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
            vec![
                ChangeBatch::new(
                    table_id,
                    vec![CdcChange::Insert {
                        row: row(1, "open"),
                    }],
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let prepared = prepare_replication_buffer_append(
            &plan,
            &transaction,
            vec![CdcBufferRecord::new(Some(vec![1]), Some(vec![2]))],
        )
        .unwrap();
        let storage = SlateCatalog::in_memory().await.unwrap();
        let buffer_store = storage.cdc_buffer_store();
        let manifest = buffer_store
            .append_transaction(&prepared.append)
            .await
            .unwrap();
        storage
            .put_replication_pipeline_checkpoint(
                ReplicationPipelineCheckpoint::new(
                    &plan.name,
                    &plan.source_name,
                    manifest.source_position().clone(),
                    manifest.transaction_id().cloned(),
                    pending_target_state(&plan, &manifest),
                    current_unix_time_ms(),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let snapshots = runtime.status_snapshots(&storage).await.unwrap();
        let snapshot = snapshots.first().expect("snapshot");

        assert_eq!(snapshot.pipeline_name(), "orders_pipe");
        assert_eq!(snapshot.source_name(), "pg_main");
        assert_eq!(snapshot.target_kind(), "kafka");
        assert_eq!(snapshot.pending_transactions(), 1);
        assert_eq!(snapshot.pending_records(), manifest.record_count());
        assert!(snapshot.pending_bytes() > 0);
        assert!(snapshot.oldest_pending_age_ms().is_some());
        assert!(snapshot.replaying());
        assert_eq!(snapshot.last_error(), Some("kafka unavailable"));
        assert_eq!(
            snapshot.checkpoint_position(),
            Some(manifest.source_position())
        );
        assert_eq!(
            snapshot
                .checkpoint_transaction_id()
                .map(CdcTransactionId::as_str),
            Some("pg-xid-77")
        );
        assert_eq!(snapshot.target_state()["target.delivery.status"], "pending");

        let debug_state = Arc::new(tokio::sync::RwLock::new(
            http_ingest::CdcReplicationDebugState::default(),
        ));
        runtime
            .refresh_debug_state(&storage, &debug_state)
            .await
            .unwrap();
        let debug_state = debug_state.read().await;
        let debug_pipeline = debug_state.pipelines.first().expect("debug pipeline");
        assert_eq!(debug_state.refresh_error, None);
        assert_eq!(debug_pipeline.pipeline, "orders_pipe");
        assert_eq!(debug_pipeline.source, "pg_main");
        assert_eq!(debug_pipeline.target_kind, "kafka");
        assert_eq!(
            debug_pipeline.checkpoint_position.as_deref(),
            Some("pg/0/16B6C50")
        );
        assert_eq!(
            debug_pipeline.checkpoint_transaction_id.as_deref(),
            Some("pg-xid-77")
        );
        assert_eq!(debug_pipeline.pending_transactions, 1);
        assert_eq!(debug_pipeline.pending_records, manifest.record_count());
        assert!(debug_pipeline.pending_bytes > 0);
        assert!(debug_pipeline.oldest_pending_age_ms.is_some());
        assert!(debug_pipeline.replaying);
        assert_eq!(
            debug_pipeline.last_error.as_deref(),
            Some("kafka unavailable")
        );
        assert_eq!(
            debug_pipeline.target_state["target.delivery.status"],
            "pending"
        );
    }

    #[test]
    fn buffer_limit_violation_accounts_for_incoming_payload_bytes() {
        let limits = ReplicationBufferLimits {
            max_pending_bytes: Some(100),
            max_pending_age_ms: None,
        };

        assert_eq!(
            buffer_limit_violation(70, None, 31, limits),
            Some(ReplicationBufferLimitViolation::PendingBytes {
                pending_bytes: 70,
                incoming_bytes: 31,
                max_pending_bytes: 100,
            })
        );
        assert_eq!(buffer_limit_violation(70, None, 30, limits), None);
    }

    #[test]
    fn buffer_limit_violation_checks_oldest_pending_age() {
        let limits = ReplicationBufferLimits {
            max_pending_bytes: None,
            max_pending_age_ms: Some(1_000),
        };

        assert_eq!(
            buffer_limit_violation(0, Some(1_001), 0, limits),
            Some(ReplicationBufferLimitViolation::PendingAge {
                oldest_pending_age_ms: 1_001,
                max_pending_age_ms: 1_000,
            })
        );
        assert_eq!(buffer_limit_violation(0, Some(1_000), 0, limits), None);
    }

    #[test]
    fn estimated_buffer_payload_bytes_includes_record_framing() {
        let records = vec![
            CdcBufferRecord::new(Some(vec![1, 2, 3]), Some(vec![4])),
            CdcBufferRecord::new(None, Some(vec![5, 6])),
        ];

        assert_eq!(estimated_buffer_payload_bytes(&records), 54);
    }

    #[test]
    fn zero_buffer_limit_override_disables_default_limit() {
        assert_eq!(effective_usize_limit(Some(0), Some(100)), None);
        assert_eq!(effective_u64_limit(Some(0), Some(100)), None);
        assert_eq!(effective_usize_limit(None, Some(100)), Some(100));
        assert_eq!(effective_u64_limit(None, Some(100)), Some(100));
    }

    #[test]
    fn parses_arrow_ipc_compression_override() {
        assert_eq!(
            ReplicationArrowIpcCompression::parse("lz4"),
            Some(ReplicationArrowIpcCompression::Lz4Frame)
        );
        assert_eq!(
            ReplicationArrowIpcCompression::parse("lz4-frame"),
            Some(ReplicationArrowIpcCompression::Lz4Frame)
        );
        assert_eq!(ReplicationArrowIpcCompression::parse("none"), None);
        assert_eq!(ReplicationArrowIpcCompression::parse("bogus"), None);
    }

    fn schema(table_id: CdcTableId) -> CdcTableSchema {
        schema_for_table(table_id, "orders")
    }

    fn schema_for_table(table_id: CdcTableId, upstream_table: &str) -> CdcTableSchema {
        CdcTableSchema::new(
            table_id,
            UpstreamTableRef::new("public", upstream_table).unwrap(),
            vec![
                CdcColumn::new("id", ColumnType::Int64, false).unwrap(),
                CdcColumn::new("status", ColumnType::Utf8, true).unwrap(),
            ],
            CdcPrimaryKey::new(["id"]).unwrap(),
        )
        .unwrap()
    }

    fn test_plan(
        name: &str,
        table_id: CdcTableId,
        upstream_table: &str,
    ) -> ReplicationPipelineRuntimePlan {
        ReplicationPipelineRuntimePlan {
            name: name.to_string(),
            source_name: "pg_main".to_string(),
            database_name: "postgres".to_string(),
            upstream_table: upstream_table.to_string(),
            table_id: table_id.clone(),
            schema: schema_for_table(table_id, upstream_table.strip_prefix("public.").unwrap()),
            target: ReplicationPipelineRuntimeTarget::Kafka {
                brokers: "localhost:9092".to_string(),
                topic: upstream_table.to_string(),
            },
            format: ReplicationPipelineRuntimeFormat::FloeJson,
            buffer_mode: ReplicationPipelineRuntimeBufferMode::Durable,
            buffer_policy: CatalogReplicationBufferPolicy::default(),
            emit_tombstones: false,
            include_transaction_metadata: false,
        }
    }

    fn test_runtime_with_plan(plan: ReplicationPipelineRuntimePlan) -> ReplicationPipelineRuntime {
        ReplicationPipelineRuntime {
            pipelines_by_source: HashMap::from([(
                CdcSourceId::new(plan.source_name.clone()).unwrap(),
                vec![plan],
            )]),
            kafka_writers_by_pipeline: HashMap::new(),
            buffer_cleanup_last_by_pipeline: Mutex::new(HashMap::new()),
            replay_state_by_pipeline: Mutex::new(HashMap::new()),
            last_target_error_by_pipeline: Mutex::new(HashMap::new()),
        }
    }

    fn row(id: i64, status: &str) -> CdcRow {
        CdcRow::new([
            Some(RowValue::Int64(id)),
            Some(RowValue::Utf8(status.to_string())),
        ])
        .unwrap()
    }
}
