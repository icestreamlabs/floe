use super::*;

use std::ffi::{CString, c_void};
use std::fmt;
use std::io::Write as _;
use std::ptr;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
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
    CdcBufferAppend, CdcBufferCleanupPolicy, CdcBufferPayloadFormat, CdcBufferPayloadStorage,
    CdcBufferRecord, CdcBufferStats, CdcBufferStore, CdcBufferedTransactionManifest,
    ReplicationPipelineCheckpoint, ReplicationPipelineDlqEntry, SlateCatalog,
    encode_cdc_buffer_records_payload,
};
use futures::future::join_all;
use rayon::prelude::*;
use rdkafka::ClientConfig;
use rdkafka::bindings as rdsys;
use rdkafka::client::ClientContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Message, OwnedHeaders};
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use rdkafka::types::RDKafkaTopic;
use tokio_postgres::types::ToSql;

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
const DEFAULT_REPLICATION_BUFFER_DELIVERED_RETENTION_MS: u64 = 5_000;
const DEFAULT_REPLICATION_BUFFER_CLEANUP_INTERVAL_MS: u64 = 5_000;
const DEFAULT_REPLICATION_BUFFER_MAX_PENDING_BYTES: usize = 10 * 1024 * 1024 * 1024;
const DEFAULT_REPLICATION_BUFFER_MAX_PENDING_RECORDS: usize = 0;
const DEFAULT_REPLICATION_BUFFER_MAX_PENDING_TRANSACTIONS: usize = 0;
const DEFAULT_REPLICATION_BUFFER_MAX_PENDING_AGE_MS: u64 = 0;
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
static REPLICATION_BUFFER_MAX_PENDING_RECORDS: LazyLock<Option<usize>> = LazyLock::new(|| {
    env_usize_limit(
        "FLOE_REPLICATION_BUFFER_MAX_PENDING_RECORDS",
        DEFAULT_REPLICATION_BUFFER_MAX_PENDING_RECORDS,
    )
});
static REPLICATION_BUFFER_MAX_PENDING_TRANSACTIONS: LazyLock<Option<usize>> = LazyLock::new(|| {
    env_usize_limit(
        "FLOE_REPLICATION_BUFFER_MAX_PENDING_TRANSACTIONS",
        DEFAULT_REPLICATION_BUFFER_MAX_PENDING_TRANSACTIONS,
    )
    .or_else(|| {
        env_usize_limit(
            "FLOE_REPLICATION_BUFFER_MAX_PENDING_OBJECTS",
            DEFAULT_REPLICATION_BUFFER_MAX_PENDING_TRANSACTIONS,
        )
    })
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
static REPLICATION_KAFKA_METADATA_HEADERS: LazyLock<bool> = LazyLock::new(|| {
    env_bool(
        "FLOE_REPLICATION_KAFKA_METADATA_HEADERS",
        DEFAULT_REPLICATION_KAFKA_METADATA_HEADERS,
    )
});

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplicationBufferLimits {
    max_pending_bytes: Option<usize>,
    max_pending_records: Option<usize>,
    max_pending_transactions: Option<usize>,
    max_pending_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicationBufferLimitViolation {
    Bytes {
        pending_bytes: usize,
        incoming_bytes: usize,
        max_pending_bytes: usize,
    },
    Records {
        pending_records: usize,
        incoming_records: usize,
        max_pending_records: usize,
    },
    Objects {
        pending_transactions: usize,
        incoming_transactions: usize,
        max_pending_transactions: usize,
    },
    Age {
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
        self.max_pending_bytes.is_some()
            || self.max_pending_records.is_some()
            || self.max_pending_transactions.is_some()
            || self.max_pending_age_ms.is_some()
    }
}

impl fmt::Display for ReplicationBufferLimitViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes {
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
            Self::Records {
                pending_records,
                incoming_records,
                max_pending_records,
            } => write!(
                f,
                "pending buffer records would be {} with incoming {} records, above max {} records",
                pending_records.saturating_add(*incoming_records),
                incoming_records,
                max_pending_records
            ),
            Self::Objects {
                pending_transactions,
                incoming_transactions,
                max_pending_transactions,
            } => write!(
                f,
                "pending buffer objects would be {} with incoming {} object, above max {} objects",
                pending_transactions.saturating_add(*incoming_transactions),
                incoming_transactions,
                max_pending_transactions
            ),
            Self::Age {
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
        max_pending_records: *REPLICATION_BUFFER_MAX_PENDING_RECORDS,
        max_pending_transactions: *REPLICATION_BUFFER_MAX_PENDING_TRANSACTIONS,
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
        max_pending_records: effective_usize_limit(
            plan.buffer_policy.max_pending_records(),
            defaults.max_pending_records,
        ),
        max_pending_transactions: effective_usize_limit(
            plan.buffer_policy.max_pending_transactions(),
            defaults.max_pending_transactions,
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
    kafka_writers_by_pipeline: HashMap<String, Arc<KafkaReplicationPipelineWriter>>,
    postgres_writers_by_pipeline: HashMap<String, Arc<PostgresReplicationPipelineWriter>>,
    buffer_cleanup_last_by_pipeline: Mutex<HashMap<String, u64>>,
    replay_state_by_pipeline: Mutex<HashMap<String, bool>>,
    backpressure_state_by_pipeline: Mutex<HashMap<String, bool>>,
    last_target_error_by_pipeline: Mutex<HashMap<String, String>>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ReplicationPipelineStatusSnapshot {
    pipeline_name: String,
    source_name: String,
    schema_evolution_policy: String,
    error_policy: String,
    target_kind: String,
    checkpoint_position: Option<CdcSourcePosition>,
    checkpoint_lsn_bytes: Option<u64>,
    checkpoint_transaction_id: Option<CdcTransactionId>,
    target_state: std::collections::BTreeMap<String, String>,
    pending_transactions: usize,
    pending_records: usize,
    pending_bytes: usize,
    oldest_pending_age_ms: Option<u64>,
    replaying: bool,
    source_backpressure_active: bool,
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

    pub(super) fn schema_evolution_policy(&self) -> &str {
        &self.schema_evolution_policy
    }

    pub(super) fn error_policy(&self) -> &str {
        &self.error_policy
    }

    pub(super) fn target_kind(&self) -> &str {
        &self.target_kind
    }

    pub(super) fn checkpoint_position(&self) -> Option<&CdcSourcePosition> {
        self.checkpoint_position.as_ref()
    }

    pub(super) fn checkpoint_lsn_bytes(&self) -> Option<u64> {
        self.checkpoint_lsn_bytes
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

    pub(super) fn source_backpressure_active(&self) -> bool {
        self.source_backpressure_active
    }

    pub(super) fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}

struct KafkaReplicationPipelineWriter {
    producer: ThreadedProducer<KafkaReplicationPipelineContext>,
    native_topic: KafkaNativeTopic,
    topic: String,
    partition_offsets: usize,
}

struct KafkaNativeTopic {
    ptr: *mut RDKafkaTopic,
}

unsafe impl Send for KafkaNativeTopic {}
unsafe impl Sync for KafkaNativeTopic {}

impl KafkaNativeTopic {
    fn new(
        producer: &ThreadedProducer<KafkaReplicationPipelineContext>,
        topic: &str,
    ) -> anyhow::Result<Self> {
        let topic_cstring =
            CString::new(topic).context("replication pipeline Kafka topic contains null byte")?;
        let ptr = unsafe {
            rdsys::rd_kafka_topic_new(
                producer.client().native_ptr(),
                topic_cstring.as_ptr(),
                ptr::null_mut(),
            )
        };
        if ptr.is_null() {
            return Err(anyhow!(
                "create replication pipeline Kafka native topic handle for {topic}"
            ));
        }
        Ok(Self { ptr })
    }
}

impl Drop for KafkaNativeTopic {
    fn drop(&mut self) {
        unsafe {
            rdsys::rd_kafka_topic_destroy(self.ptr);
        }
    }
}

struct PostgresReplicationPipelineWriter {
    connection: String,
    target_table: String,
    insert_sql: String,
    delete_sql: String,
    schema: CdcTableSchema,
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
    partition_offsets: Vec<AtomicI64>,
    overflow_offsets_by_partition: Mutex<std::collections::BTreeMap<i32, i64>>,
    first_error: Mutex<Option<String>>,
    notify: tokio::sync::Notify,
}

impl KafkaDeliveryBatchState {
    fn new(expected: usize, partition_offsets: usize) -> Arc<Self> {
        Arc::new(Self {
            expected,
            completed: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
            partition_offsets: (0..partition_offsets).map(|_| AtomicI64::new(-1)).collect(),
            overflow_offsets_by_partition: Mutex::new(std::collections::BTreeMap::new()),
            first_error: Mutex::new(None),
            notify: tokio::sync::Notify::new(),
        })
    }

    fn record(&self, delivery_result: &DeliveryResult<'_>) {
        match delivery_result {
            Ok(message) => {
                let partition = message.partition();
                if let Ok(partition_idx) = usize::try_from(partition)
                    && let Some(offset) = self.partition_offsets.get(partition_idx)
                {
                    offset.fetch_max(message.offset(), Ordering::AcqRel);
                } else if let Ok(mut offsets_by_partition) =
                    self.overflow_offsets_by_partition.lock()
                {
                    offsets_by_partition
                        .entry(partition)
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
        let mut offsets_by_partition = std::collections::BTreeMap::new();
        for (partition, offset) in self.partition_offsets.iter().enumerate() {
            let offset = offset.load(Ordering::Acquire);
            if offset >= 0 {
                offsets_by_partition.insert(partition as i32, offset);
            }
        }
        let overflow_offsets = self
            .overflow_offsets_by_partition
            .lock()
            .map_err(|_| anyhow!("replication pipeline Kafka delivery offsets lock poisoned"))?;
        for (partition, offset) in overflow_offsets.iter() {
            offsets_by_partition
                .entry(*partition)
                .and_modify(|current| *current = (*current).max(*offset))
                .or_insert(*offset);
        }
        Ok(offsets_by_partition)
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
        let mut postgres_writers_by_pipeline = HashMap::new();

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
                ReplicationPipelineRuntimeTarget::Postgres { connection, table } => {
                    anyhow::ensure!(
                        plan.format == ReplicationPipelineRuntimeFormat::FloeJson,
                        "replication pipeline '{}' uses a Postgres target, which currently requires format = 'floe_json'",
                        plan.name
                    );
                    postgres_writers_by_pipeline.insert(
                        plan.name.clone(),
                        Arc::new(PostgresReplicationPipelineWriter::new(
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
                let dlq_entry = self
                    .persist_dead_letter_records(
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
                        let dlq_entry = self
                            .persist_dead_letter_records(
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
            let payload_load_started_at = CDC_PERF_LOGGING_ENABLED.then(Instant::now);
            let records = match manifest.payload_format() {
                CdcBufferPayloadFormat::KafkaRecords => {
                    let records = buffer_store.records(&manifest).await.with_context(|| {
                        format!(
                            "load replication pipeline '{}' buffered payloads",
                            plan.name
                        )
                    })?;
                    if manifest.payload_storage() == CdcBufferPayloadStorage::ObjectStore {
                        crate::metrics::inc_cdc_buffer_object_op(&plan.name, "get", 1);
                    }
                    records
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
                    if manifest.payload_storage() == CdcBufferPayloadStorage::ObjectStore {
                        crate::metrics::inc_cdc_buffer_object_op(&plan.name, "get", 1);
                    }
                    let payload_load_elapsed = payload_load_started_at
                        .map(|started_at| started_at.elapsed())
                        .unwrap_or(Duration::ZERO);
                    let encode_started_at = CDC_PERF_LOGGING_ENABLED.then(Instant::now);
                    let mut records = encoding::encode_floe_json_buffered_change_batches(
                        plan,
                        &plan.schema,
                        &batches,
                    )?;
                    encoding::add_replication_record_metadata(
                        plan,
                        manifest.source_position(),
                        manifest.transaction_id(),
                        &mut records,
                        0,
                    );
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

    async fn mark_manifest_dead_lettered(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        buffer_store: &CdcBufferStore,
        storage: &SlateCatalog,
        manifest: &CdcBufferedTransactionManifest,
        records: &[CdcBufferRecord],
        err: &anyhow::Error,
    ) -> anyhow::Result<usize> {
        let dlq_entry = self
            .persist_dead_letter_records(
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

    async fn persist_dead_letter_records(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        storage: &SlateCatalog,
        source_position: &CdcSourcePosition,
        transaction_id: Option<&CdcTransactionId>,
        records: &[CdcBufferRecord],
        err: &anyhow::Error,
    ) -> anyhow::Result<ReplicationPipelineDlqEntry> {
        let dlq_id = replication_pipeline_dlq_id(source_position, transaction_id);
        let payload = encode_cdc_buffer_records_payload(records)
            .context("encode replication pipeline DLQ payload")?;
        let payload_bytes = payload.len();
        let payload_object_key = storage
            .put_replication_pipeline_dlq_payload(&plan.name, &dlq_id, payload)
            .await
            .with_context(|| {
                format!(
                    "persist replication pipeline '{}' dead-letter payload",
                    plan.name
                )
            })?;
        let entry = ReplicationPipelineDlqEntry::new(
            &plan.name,
            dlq_id,
            &plan.source_name,
            source_position.clone(),
            transaction_id.cloned(),
            format!("{}_delivery", target_kind(plan)),
            format!("{err:#}"),
            1,
            Some(payload_object_key),
            Some("kafka_records".to_string()),
            payload_bytes,
            dead_letter_target_state(plan, err),
            current_unix_time_ms(),
        )?;
        storage
            .put_replication_pipeline_dlq_entry(entry.clone())
            .await
            .with_context(|| {
                format!(
                    "persist replication pipeline '{}' dead-letter entry",
                    plan.name
                )
            })?;
        tracing::warn!(
            pipeline = %plan.name,
            source = %plan.source_name,
            target_kind = target_kind(plan),
            dlq_id = %entry.dlq_id(),
            records = records.len(),
            payload_bytes = entry.payload_bytes(),
            source_position = %encoding::source_position_key(source_position),
            transaction_id = transaction_id.map(CdcTransactionId::as_str),
            error = %err,
            "replication pipeline target write dead-lettered"
        );
        Ok(entry)
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

    async fn cleanup_delivered_if_due(
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
        let native_topic = KafkaNativeTopic::new(&producer, topic)?;
        let partition_offsets = match producer
            .client()
            .fetch_metadata(Some(topic), REPLICATION_KAFKA_METADATA_WARMUP_TIMEOUT)
        {
            Ok(metadata) => metadata
                .topics()
                .iter()
                .find(|metadata_topic| metadata_topic.name() == topic)
                .map(|metadata_topic| metadata_topic.partitions().len())
                .unwrap_or(0),
            Err(err) => {
                tracing::debug!(
                    topic,
                    error = %err,
                    "replication pipeline Kafka metadata warm-up failed; first send will retry metadata"
                );
                0
            }
        };
        Ok(Self {
            producer,
            native_topic,
            topic: topic.to_string(),
            partition_offsets,
        })
    }

    async fn send_records(
        &self,
        records: &[CdcBufferRecord],
    ) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
        let perf_enabled = *CDC_PERF_LOGGING_ENABLED;
        let perf_started_at = perf_enabled.then(Instant::now);
        let enqueue_started_at = perf_enabled.then(Instant::now);
        let delivery_state = KafkaDeliveryBatchState::new(records.len(), self.partition_offsets);
        for record in records {
            self.enqueue_record_with_retry(record, Arc::clone(&delivery_state))
                .await?;
        }
        let enqueue_elapsed = enqueue_started_at
            .map(|started_at| started_at.elapsed())
            .unwrap_or(Duration::ZERO);
        let delivery_wait_started_at = perf_enabled.then(Instant::now);
        let offsets_by_partition = match delivery_state.wait(REPLICATION_KAFKA_SEND_TIMEOUT).await {
            Ok(offsets) => offsets,
            Err(err) => {
                tracing::warn!(
                    topic = %self.topic,
                    records = records.len(),
                    timeout_ms = REPLICATION_KAFKA_SEND_TIMEOUT.as_millis() as u64,
                    error = %err,
                    "replication pipeline Kafka delivery wait failed"
                );
                return Err(err);
            }
        };
        let delivery_wait_elapsed = delivery_wait_started_at
            .map(|started_at| started_at.elapsed())
            .unwrap_or(Duration::ZERO);
        log_replication_kafka_send_perf(
            &self.topic,
            records,
            self.partition_offsets,
            enqueue_elapsed,
            delivery_wait_elapsed,
            perf_started_at
                .map(|started_at| started_at.elapsed())
                .unwrap_or(Duration::ZERO),
        );

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
        if record.headers().is_empty() {
            return self
                .enqueue_record_direct_with_retry(record, delivery_state)
                .await;
        }

        for attempt in 0..REPLICATION_KAFKA_RETRY_ATTEMPTS {
            let attempt_number = attempt + 1;
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
            if !record.headers().is_empty() {
                let headers =
                    record
                        .headers()
                        .iter()
                        .fold(OwnedHeaders::new(), |headers, header| {
                            headers.insert(Header {
                                key: header.key(),
                                value: Some(header.value()),
                            })
                        });
                kafka_record = kafka_record.headers(headers);
            }

            match self.producer.send(kafka_record) {
                Ok(()) => return Ok(()),
                Err((err, _record))
                    if is_kafka_queue_full(&err)
                        && attempt_number < REPLICATION_KAFKA_RETRY_ATTEMPTS =>
                {
                    let delay_ms = REPLICATION_KAFKA_RETRY_BASE_MS.saturating_mul(
                        1_u64 << u32::try_from(attempt).unwrap_or(u32::MAX).min(16),
                    );
                    tracing::warn!(
                        topic = %self.topic,
                        attempt = attempt_number,
                        max_attempts = REPLICATION_KAFKA_RETRY_ATTEMPTS,
                        retry_delay_ms = delay_ms,
                        record_bytes = record.byte_len(),
                        key_bytes = record.key().map(|key| key.len()),
                        value_bytes = record.value().map(|value| value.len()),
                        error = %err,
                        "replication pipeline Kafka producer queue is full; retrying"
                    );
                    self.producer.poll(Duration::from_millis(0));
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                Err((err, _record)) if is_kafka_queue_full(&err) => {
                    tracing::warn!(
                        topic = %self.topic,
                        attempt = attempt_number,
                        max_attempts = REPLICATION_KAFKA_RETRY_ATTEMPTS,
                        record_bytes = record.byte_len(),
                        key_bytes = record.key().map(|key| key.len()),
                        value_bytes = record.value().map(|value| value.len()),
                        error = %err,
                        "replication pipeline Kafka producer queue remained full after retries"
                    );
                    return Err(anyhow!(
                        "replication pipeline Kafka enqueue failed after retries: {err}"
                    ));
                }
                Err((err, _record)) => {
                    tracing::warn!(
                        topic = %self.topic,
                        attempt = attempt_number,
                        max_attempts = REPLICATION_KAFKA_RETRY_ATTEMPTS,
                        record_bytes = record.byte_len(),
                        key_bytes = record.key().map(|key| key.len()),
                        value_bytes = record.value().map(|value| value.len()),
                        error = %err,
                        "replication pipeline Kafka enqueue failed without retry"
                    );
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

    async fn enqueue_record_direct_with_retry(
        &self,
        record: &CdcBufferRecord,
        delivery_state: Arc<KafkaDeliveryBatchState>,
    ) -> anyhow::Result<()> {
        for attempt in 0..REPLICATION_KAFKA_RETRY_ATTEMPTS {
            let attempt_number = attempt + 1;
            match self.enqueue_record_direct(record, Arc::clone(&delivery_state)) {
                Ok(()) => return Ok(()),
                Err(err)
                    if is_kafka_queue_full(&err)
                        && attempt_number < REPLICATION_KAFKA_RETRY_ATTEMPTS =>
                {
                    let delay_ms = REPLICATION_KAFKA_RETRY_BASE_MS.saturating_mul(
                        1_u64 << u32::try_from(attempt).unwrap_or(u32::MAX).min(16),
                    );
                    tracing::warn!(
                        topic = %self.topic,
                        attempt = attempt_number,
                        max_attempts = REPLICATION_KAFKA_RETRY_ATTEMPTS,
                        retry_delay_ms = delay_ms,
                        record_bytes = record.byte_len(),
                        key_bytes = record.key().map(|key| key.len()),
                        value_bytes = record.value().map(|value| value.len()),
                        error = %err,
                        "replication pipeline Kafka producer queue is full; retrying"
                    );
                    self.producer.poll(Duration::from_millis(0));
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                Err(err) if is_kafka_queue_full(&err) => {
                    tracing::warn!(
                        topic = %self.topic,
                        attempt = attempt_number,
                        max_attempts = REPLICATION_KAFKA_RETRY_ATTEMPTS,
                        record_bytes = record.byte_len(),
                        key_bytes = record.key().map(|key| key.len()),
                        value_bytes = record.value().map(|value| value.len()),
                        error = %err,
                        "replication pipeline Kafka producer queue remained full after retries"
                    );
                    return Err(anyhow!(
                        "replication pipeline Kafka enqueue failed after retries: {err}"
                    ));
                }
                Err(err) => {
                    tracing::warn!(
                        topic = %self.topic,
                        attempt = attempt_number,
                        max_attempts = REPLICATION_KAFKA_RETRY_ATTEMPTS,
                        record_bytes = record.byte_len(),
                        key_bytes = record.key().map(|key| key.len()),
                        value_bytes = record.value().map(|value| value.len()),
                        error = %err,
                        "replication pipeline Kafka enqueue failed without retry"
                    );
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

    fn enqueue_record_direct(
        &self,
        record: &CdcBufferRecord,
        delivery_state: Arc<KafkaDeliveryBatchState>,
    ) -> Result<(), KafkaError> {
        let payload = record.value();
        let payload_ptr = payload.map_or(ptr::null_mut(), |payload| {
            payload.as_ptr().cast::<c_void>().cast_mut()
        });
        let payload_len = payload.map_or(0, <[u8]>::len);
        let key = record.key();
        let key_ptr = key.map_or(ptr::null(), |key| key.as_ptr().cast::<c_void>());
        let key_len = key.map_or(0, <[u8]>::len);
        let opaque_ptr = Arc::into_raw(delivery_state).cast::<c_void>().cast_mut();
        let produce_result = unsafe {
            rdsys::rd_kafka_produce(
                self.native_topic.ptr,
                -1,
                rdsys::RD_KAFKA_MSG_F_COPY,
                payload_ptr,
                payload_len,
                key_ptr,
                key_len,
                opaque_ptr,
            )
        };
        if produce_result == 0 {
            Ok(())
        } else {
            unsafe {
                drop(Arc::from_raw(
                    opaque_ptr.cast::<KafkaDeliveryBatchState>().cast_const(),
                ));
            }
            Err(KafkaError::MessageProduction(RDKafkaErrorCode::from(
                unsafe { rdsys::rd_kafka_last_error() },
            )))
        }
    }
}

#[derive(Debug, PartialEq)]
enum PostgresParamValue {
    Int64(Option<i64>),
    Bool(Option<bool>),
    Text(Option<String>),
    Float64(Option<f64>),
    Int32(Option<i32>),
}

impl PostgresParamValue {
    fn null(data_type: &ColumnType) -> Self {
        match data_type {
            ColumnType::Int64 => Self::Int64(None),
            ColumnType::Bool => Self::Bool(None),
            ColumnType::Utf8 => Self::Text(None),
            ColumnType::TimestampMillis => Self::Float64(None),
            ColumnType::DateDays => Self::Int32(None),
            ColumnType::Decimal128 { .. } | ColumnType::Numeric => Self::Text(None),
        }
    }

    fn as_tosql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Int64(value) => value,
            Self::Bool(value) => value,
            Self::Text(value) => value,
            Self::Float64(value) => value,
            Self::Int32(value) => value,
        }
    }
}

impl PostgresReplicationPipelineWriter {
    fn new(connection: &str, table: &str, schema: CdcTableSchema) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !connection.trim().is_empty(),
            "replication Postgres connection cannot be empty"
        );
        let target_table = quote_postgres_qualified_name(table)?;
        encoding::validate_floe_json_schema(&schema)?;
        Ok(Self {
            connection: connection.to_string(),
            target_table,
            insert_sql: postgres_upsert_sql(&schema, table)?,
            delete_sql: postgres_delete_sql(&schema, table)?,
            schema,
        })
    }

    async fn send_records(
        &self,
        records: &[CdcBufferRecord],
    ) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
        if records.is_empty() {
            return Ok(self.target_state(0));
        }

        let (mut client, connection) =
            tokio_postgres::connect(self.connection.as_str(), tokio_postgres::NoTls)
                .await
                .context("connect replication pipeline Postgres target")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(
                    error = %err,
                    "replication pipeline Postgres target connection failed"
                );
            }
        });

        let transaction = client
            .transaction()
            .await
            .context("start replication pipeline Postgres target transaction")?;
        for record in records {
            self.apply_record(&transaction, record).await?;
        }
        transaction
            .commit()
            .await
            .context("commit replication pipeline Postgres target transaction")?;
        Ok(self.target_state(records.len()))
    }

    async fn apply_record(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        record: &CdcBufferRecord,
    ) -> anyhow::Result<()> {
        let value = parse_floe_json_record_value(record)?;
        let deleted = value
            .get(FLOE_JSON_DELETED_FIELD)
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if deleted {
            let key = parse_floe_json_record_key(record).unwrap_or_else(|_| value.clone());
            let params = postgres_key_params_from_json(&self.schema, &key)?;
            let refs = params
                .iter()
                .map(PostgresParamValue::as_tosql)
                .collect::<Vec<_>>();
            transaction
                .execute(&self.delete_sql, &refs)
                .await
                .with_context(|| {
                    format!(
                        "delete CDC row from replication pipeline Postgres target {}",
                        self.target_table
                    )
                })?;
            return Ok(());
        }

        let params = postgres_row_params_from_json(&self.schema, &value)?;
        let refs = params
            .iter()
            .map(PostgresParamValue::as_tosql)
            .collect::<Vec<_>>();
        transaction
            .execute(&self.insert_sql, &refs)
            .await
            .with_context(|| {
                format!(
                    "upsert CDC row into replication pipeline Postgres target {}",
                    self.target_table
                )
            })?;
        Ok(())
    }

    fn target_state(&self, records: usize) -> std::collections::BTreeMap<String, String> {
        std::collections::BTreeMap::from([
            ("postgres.table".to_string(), self.target_table.clone()),
            ("postgres.records_applied".to_string(), records.to_string()),
        ])
    }
}

fn is_kafka_queue_full(err: &KafkaError) -> bool {
    matches!(
        err,
        KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
    )
}

fn parse_floe_json_record_value(
    record: &CdcBufferRecord,
) -> anyhow::Result<serde_json::Map<String, serde_json::Value>> {
    let value = record
        .value()
        .ok_or_else(|| anyhow!("Floe JSON Postgres target record is missing a value"))?;
    let value = serde_json::from_slice::<serde_json::Value>(value)
        .context("parse Floe JSON Postgres target record value")?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow!("Floe JSON Postgres target record value must be an object"))
}

fn parse_floe_json_record_key(
    record: &CdcBufferRecord,
) -> anyhow::Result<serde_json::Map<String, serde_json::Value>> {
    let key = record
        .key()
        .ok_or_else(|| anyhow!("Floe JSON Postgres target record is missing a key"))?;
    let key = serde_json::from_slice::<serde_json::Value>(key)
        .context("parse Floe JSON Postgres target record key")?;
    key.as_object()
        .cloned()
        .ok_or_else(|| anyhow!("Floe JSON Postgres target record key must be an object"))
}

fn postgres_row_params_from_json(
    schema: &CdcTableSchema,
    object: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<Vec<PostgresParamValue>> {
    schema
        .columns()
        .iter()
        .map(|column| postgres_param_from_json(column, object.get(column.name())))
        .collect()
}

fn postgres_key_params_from_json(
    schema: &CdcTableSchema,
    object: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<Vec<PostgresParamValue>> {
    schema
        .primary_key()
        .columns()
        .iter()
        .map(|column_name| {
            let column = schema
                .columns()
                .iter()
                .find(|column| column.name() == column_name)
                .ok_or_else(|| {
                    anyhow!("CDC primary-key column '{column_name}' missing from schema")
                })?;
            postgres_param_from_json(column, object.get(column.name()))
        })
        .collect()
}

fn postgres_param_from_json(
    column: &CdcColumn,
    value: Option<&serde_json::Value>,
) -> anyhow::Result<PostgresParamValue> {
    let Some(value) = value else {
        anyhow::ensure!(
            column.nullable(),
            "CDC column '{}' is required for Postgres target",
            column.name()
        );
        return Ok(PostgresParamValue::null(column.data_type()));
    };
    if value.is_null() {
        anyhow::ensure!(
            column.nullable(),
            "CDC column '{}' cannot be NULL for Postgres target",
            column.name()
        );
        return Ok(PostgresParamValue::null(column.data_type()));
    }
    match column.data_type() {
        ColumnType::Int64 => Ok(PostgresParamValue::Int64(Some(json_i64(
            column.name(),
            value,
        )?))),
        ColumnType::Bool => Ok(PostgresParamValue::Bool(Some(json_bool(
            column.name(),
            value,
        )?))),
        ColumnType::Utf8 => Ok(PostgresParamValue::Text(Some(json_string(
            column.name(),
            value,
        )?))),
        ColumnType::TimestampMillis => Ok(PostgresParamValue::Float64(Some(json_i64(
            column.name(),
            value,
        )? as f64))),
        ColumnType::DateDays => Ok(PostgresParamValue::Int32(Some(json_i32(
            column.name(),
            value,
        )?))),
        ColumnType::Decimal128 { scale, .. } => {
            let text = if let Some(value) = value.as_str() {
                value.to_string()
            } else {
                encoding::format_decimal128_for_json(
                    i128::from(json_i64(column.name(), value)?),
                    *scale,
                )
            };
            Ok(PostgresParamValue::Text(Some(text)))
        }
        ColumnType::Numeric => Ok(PostgresParamValue::Text(Some(json_scalar_string(
            column.name(),
            value,
        )?))),
    }
}

fn json_i64(column: &str, value: &serde_json::Value) -> anyhow::Result<i64> {
    if let Some(value) = value.as_i64() {
        return Ok(value);
    }
    if let Some(value) = value.as_str() {
        return value
            .parse::<i64>()
            .with_context(|| format!("parse CDC column '{column}' as i64"));
    }
    Err(anyhow!("CDC column '{column}' must be an integer"))
}

fn json_i32(column: &str, value: &serde_json::Value) -> anyhow::Result<i32> {
    let value = json_i64(column, value)?;
    i32::try_from(value).with_context(|| format!("CDC column '{column}' exceeds i32 range"))
}

fn json_bool(column: &str, value: &serde_json::Value) -> anyhow::Result<bool> {
    if let Some(value) = value.as_bool() {
        return Ok(value);
    }
    if let Some(value) = value.as_str() {
        return value
            .parse::<bool>()
            .with_context(|| format!("parse CDC column '{column}' as bool"));
    }
    Err(anyhow!("CDC column '{column}' must be a boolean"))
}

fn json_string(column: &str, value: &serde_json::Value) -> anyhow::Result<String> {
    value
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("CDC column '{column}' must be a string"))
}

fn json_scalar_string(column: &str, value: &serde_json::Value) -> anyhow::Result<String> {
    match value {
        serde_json::Value::String(value) => Ok(value.clone()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        _ => Err(anyhow!("CDC column '{column}' must be a scalar value")),
    }
}

fn postgres_upsert_sql(schema: &CdcTableSchema, table: &str) -> anyhow::Result<String> {
    let table = quote_postgres_qualified_name(table)?;
    let columns = schema
        .columns()
        .iter()
        .map(|column| quote_postgres_ident(column.name()))
        .collect::<Vec<_>>();
    let values = schema
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, column)| postgres_value_expr(idx + 1, column.data_type()))
        .collect::<Vec<_>>();
    let primary_keys = schema
        .primary_key()
        .columns()
        .iter()
        .map(|column| quote_postgres_ident(column))
        .collect::<Vec<_>>();
    let primary_key_names = schema
        .primary_key()
        .columns()
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let updates = schema
        .columns()
        .iter()
        .filter(|column| !primary_key_names.contains(column.name()))
        .map(|column| {
            let quoted = quote_postgres_ident(column.name());
            format!("{quoted} = EXCLUDED.{quoted}")
        })
        .collect::<Vec<_>>();
    let conflict_action = if updates.is_empty() {
        "DO NOTHING".to_string()
    } else {
        format!("DO UPDATE SET {}", updates.join(", "))
    };
    Ok(format!(
        "INSERT INTO {table} ({}) VALUES ({}) ON CONFLICT ({}) {conflict_action}",
        columns.join(", "),
        values.join(", "),
        primary_keys.join(", ")
    ))
}

fn postgres_delete_sql(schema: &CdcTableSchema, table: &str) -> anyhow::Result<String> {
    let table = quote_postgres_qualified_name(table)?;
    let predicates = schema
        .primary_key()
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, column_name)| {
            let column = schema
                .columns()
                .iter()
                .find(|column| column.name() == column_name)
                .ok_or_else(|| {
                    anyhow!("CDC primary-key column '{column_name}' missing from schema")
                })?;
            Ok(format!(
                "{} = {}",
                quote_postgres_ident(column.name()),
                postgres_value_expr(idx + 1, column.data_type())
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(format!(
        "DELETE FROM {table} WHERE {}",
        predicates.join(" AND ")
    ))
}

fn postgres_value_expr(param_idx: usize, data_type: &ColumnType) -> String {
    match data_type {
        ColumnType::TimestampMillis => {
            format!("to_timestamp(${param_idx}::double precision / 1000.0)")
        }
        ColumnType::DateDays => format!("DATE '1970-01-01' + ${param_idx}::integer"),
        ColumnType::Decimal128 { .. } | ColumnType::Numeric => {
            format!("${param_idx}::numeric")
        }
        ColumnType::Int64 | ColumnType::Bool | ColumnType::Utf8 => format!("${param_idx}"),
    }
}

fn quote_postgres_qualified_name(name: &str) -> anyhow::Result<String> {
    let parts = name
        .split('.')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(quote_postgres_ident)
        .collect::<Vec<_>>();
    anyhow::ensure!(!parts.is_empty(), "Postgres target table cannot be empty");
    Ok(parts.join("."))
}

fn quote_postgres_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

async fn append_buffer_transaction(
    buffer_store: &CdcBufferStore,
    append: &CdcBufferAppend,
    await_durable: bool,
) -> anyhow::Result<CdcBufferedTransactionManifest> {
    let manifest = if await_durable {
        buffer_store.append_transaction(append).await
    } else {
        buffer_store
            .append_transaction_without_durable_wait(append)
            .await
    }?;
    crate::metrics::inc_cdc_buffer_object_op(append.pipeline_name(), "create", 1);
    Ok(manifest)
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

fn prepare_replication_buffer_append(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    target_records: Vec<CdcBufferRecord>,
) -> anyhow::Result<PreparedReplicationBufferAppend> {
    let buffered_at_unix_ms = current_unix_time_ms();
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

fn log_replication_buffer_backpressure(
    plan: &ReplicationPipelineRuntimePlan,
    phase: &str,
    stats: Option<&CdcBufferStats>,
    incoming_bytes: usize,
    incoming_records: usize,
    limits: ReplicationBufferLimits,
    violation: ReplicationBufferLimitViolation,
    delivered_records: Option<usize>,
) {
    tracing::warn!(
        pipeline = %plan.name,
        source = %plan.source_name,
        target_kind = target_kind(plan),
        phase,
        violation_kind = buffer_limit_violation_kind(violation),
        violation = %violation,
        pending_transactions = stats.map(CdcBufferStats::pending_transactions).unwrap_or(0),
        pending_records = stats.map(CdcBufferStats::pending_records).unwrap_or(0),
        pending_bytes = stats.map(CdcBufferStats::pending_bytes).unwrap_or(0),
        oldest_pending_age_ms = stats.and_then(CdcBufferStats::oldest_pending_age_ms),
        incoming_bytes,
        incoming_records,
        delivered_records,
        max_pending_bytes = limits.max_pending_bytes,
        max_pending_records = limits.max_pending_records,
        max_pending_transactions = limits.max_pending_transactions,
        max_pending_age_ms = limits.max_pending_age_ms,
        "replication pipeline durable buffer guardrail applying CDC source backpressure"
    );
}

fn buffer_limit_violation_kind(violation: ReplicationBufferLimitViolation) -> &'static str {
    match violation {
        ReplicationBufferLimitViolation::Bytes { .. } => "pending_bytes",
        ReplicationBufferLimitViolation::Records { .. } => "pending_records",
        ReplicationBufferLimitViolation::Objects { .. } => "pending_objects",
        ReplicationBufferLimitViolation::Age { .. } => "pending_age",
    }
}

fn cdc_replication_debug_state_from_snapshots(
    snapshots: Vec<ReplicationPipelineStatusSnapshot>,
) -> http_ingest::CdcReplicationDebugState {
    http_ingest::CdcReplicationDebugState {
        updated_at_unix_ms: current_unix_time_ms(),
        refresh_error: None,
        postgres_sources: Vec::new(),
        pipelines: snapshots
            .into_iter()
            .map(|snapshot| http_ingest::CdcReplicationDebugPipelineState {
                pipeline: snapshot.pipeline_name().to_string(),
                source: snapshot.source_name().to_string(),
                schema_evolution_policy: snapshot.schema_evolution_policy().to_string(),
                error_policy: snapshot.error_policy().to_string(),
                target_kind: snapshot.target_kind().to_string(),
                checkpoint_position: snapshot
                    .checkpoint_position()
                    .map(encoding::source_position_key),
                checkpoint_lsn_bytes: snapshot.checkpoint_lsn_bytes(),
                checkpoint_lag_bytes: None,
                checkpoint_transaction_id: snapshot
                    .checkpoint_transaction_id()
                    .map(|transaction_id| transaction_id.as_str().to_string()),
                target_state: snapshot.target_state().clone(),
                pending_transactions: snapshot.pending_transactions(),
                pending_objects: snapshot.pending_transactions(),
                pending_records: snapshot.pending_records(),
                pending_bytes: snapshot.pending_bytes(),
                oldest_pending_age_ms: snapshot.oldest_pending_age_ms(),
                replaying: snapshot.replaying(),
                source_backpressure_active: snapshot.source_backpressure_active(),
                last_error: snapshot.last_error().map(str::to_string),
            })
            .collect(),
    }
}

fn postgres_position_lsn_bytes(position: &CdcSourcePosition) -> Option<u64> {
    let CdcSourcePosition::Postgres { commit_lsn, .. } = position else {
        return None;
    };
    PostgresLsn::parse(commit_lsn).ok().map(|lsn| lsn.as_u64())
}

fn enrich_pipeline_checkpoint_lag(state: &mut http_ingest::CdcReplicationDebugState) {
    let upstream_lsn_by_source = state
        .postgres_sources
        .iter()
        .filter_map(|source| {
            source
                .upstream_lsn_bytes
                .map(|upstream_lsn| (source.source.as_str(), upstream_lsn))
        })
        .collect::<HashMap<_, _>>();
    for pipeline in &mut state.pipelines {
        pipeline.checkpoint_lag_bytes = pipeline.checkpoint_lsn_bytes.and_then(|checkpoint_lsn| {
            upstream_lsn_by_source
                .get(pipeline.source.as_str())
                .map(|upstream_lsn| upstream_lsn.saturating_sub(checkpoint_lsn))
        });
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

fn dead_lettered_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    dlq_entry: &ReplicationPipelineDlqEntry,
    err: &anyhow::Error,
) -> std::collections::BTreeMap<String, String> {
    let mut state = base_target_state(plan, manifest);
    state.insert("buffer.status".to_string(), "dead_lettered".to_string());
    add_dead_letter_state(&mut state, dlq_entry, err);
    state
}

fn direct_delivered_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    record_count: usize,
    payload_format: CdcBufferPayloadFormat,
    mut target_state: std::collections::BTreeMap<String, String>,
) -> std::collections::BTreeMap<String, String> {
    target_state.insert("source.table".to_string(), plan.upstream_table.clone());
    target_state.insert("target.kind".to_string(), target_kind(plan).to_string());
    if let Some(transaction_id) = transaction.transaction_id() {
        target_state.insert(
            "source.transaction_id".to_string(),
            transaction_id.as_str().to_string(),
        );
    }
    match transaction.commit_position() {
        CdcSourcePosition::Postgres {
            commit_lsn,
            event_lsn,
        } => {
            target_state.insert(
                "source.position.postgres.commit_lsn".to_string(),
                commit_lsn.clone(),
            );
            if let Some(event_lsn) = event_lsn {
                target_state.insert(
                    "source.position.postgres.event_lsn".to_string(),
                    event_lsn.clone(),
                );
            }
        }
        CdcSourcePosition::Opaque { value } => {
            target_state.insert("source.position".to_string(), value.clone());
        }
    }
    target_state.insert("buffer.status".to_string(), "not_buffered".to_string());
    target_state.insert("buffer.record_count".to_string(), record_count.to_string());
    target_state.insert(
        "buffer.payload_format".to_string(),
        format!("{payload_format:?}"),
    );
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

fn direct_dead_lettered_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    record_count: usize,
    payload_format: CdcBufferPayloadFormat,
    dlq_entry: &ReplicationPipelineDlqEntry,
    err: &anyhow::Error,
) -> std::collections::BTreeMap<String, String> {
    let mut state = direct_delivered_target_state(
        plan,
        transaction,
        record_count,
        payload_format,
        std::collections::BTreeMap::new(),
    );
    add_dead_letter_state(&mut state, dlq_entry, err);
    state
}

fn add_dead_letter_state(
    state: &mut std::collections::BTreeMap<String, String>,
    dlq_entry: &ReplicationPipelineDlqEntry,
    err: &anyhow::Error,
) {
    state.insert(
        "target.delivery.status".to_string(),
        "dead_lettered".to_string(),
    );
    state.insert(
        "target.delivery.replay_may_duplicate".to_string(),
        "false".to_string(),
    );
    state.insert("target.dlq.id".to_string(), dlq_entry.dlq_id().to_string());
    state.insert(
        "target.dlq.status".to_string(),
        dlq_entry.status().as_str().to_string(),
    );
    if let Some(payload_object_key) = dlq_entry.payload_object_key() {
        state.insert(
            "target.dlq.payload_object_key".to_string(),
            payload_object_key.to_string(),
        );
    }
    state.insert(
        "target.last_error".to_string(),
        truncate_target_error(&format!("{err:#}")),
    );
}

fn dead_letter_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    err: &anyhow::Error,
) -> std::collections::BTreeMap<String, String> {
    let mut state = std::collections::BTreeMap::new();
    state.insert("target.kind".to_string(), target_kind(plan).to_string());
    state.insert(
        "target.delivery.status".to_string(),
        "dead_lettered".to_string(),
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
        ReplicationPipelineRuntimeTarget::Postgres { .. } => "postgres",
    }
}

fn replication_pipeline_uses_dlq(plan: &ReplicationPipelineRuntimePlan) -> bool {
    plan.error_policy.mode() == CatalogReplicationErrorPolicyMode::DeadLetterAndContinue
}

fn replication_pipeline_dlq_id(
    source_position: &CdcSourcePosition,
    transaction_id: Option<&CdcTransactionId>,
) -> String {
    let position = encoding::source_position_key(source_position);
    let transaction = transaction_id.map_or("none", CdcTransactionId::as_str);
    format!(
        "{}-{}-{}",
        hex_component(position.as_bytes()),
        hex_component(transaction.as_bytes()),
        current_unix_time_ms()
    )
}

fn hex_component(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0F) as usize] as char);
    }
    out
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
        error_policy = %plan.error_policy.mode().as_str(),
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

fn log_replication_direct_delivery_perf(
    plan: &ReplicationPipelineRuntimePlan,
    records: usize,
    payload_format: CdcBufferPayloadFormat,
    payload_bytes: usize,
    target_send_elapsed: Duration,
    checkpoint_elapsed: Duration,
) {
    if !*CDC_PERF_LOGGING_ENABLED {
        return;
    }
    tracing::info!(
        pipeline = %plan.name,
        records,
        buffer_payload_format = ?payload_format,
        buffer_payload_bytes = payload_bytes,
        target_send_ms = target_send_elapsed.as_millis() as u64,
        delivery_checkpoint_ms = checkpoint_elapsed.as_millis() as u64,
        "postgres cdc durable replication pipeline direct delivery completed"
    );
}

fn log_replication_kafka_send_perf(
    topic: &str,
    records: &[CdcBufferRecord],
    partition_offsets: usize,
    enqueue_elapsed: Duration,
    delivery_wait_elapsed: Duration,
    total_elapsed: Duration,
) {
    if !*CDC_PERF_LOGGING_ENABLED {
        return;
    }
    let key_bytes = records
        .iter()
        .map(|record| record.key().map_or(0, <[u8]>::len))
        .sum::<usize>();
    let value_bytes = records
        .iter()
        .map(|record| record.value().map_or(0, <[u8]>::len))
        .sum::<usize>();
    tracing::info!(
        topic,
        records = records.len(),
        key_bytes,
        value_bytes,
        partition_offsets,
        enqueue_ms = enqueue_elapsed.as_millis() as u64,
        delivery_wait_ms = delivery_wait_elapsed.as_millis() as u64,
        total_ms = total_elapsed.as_millis() as u64,
        "postgres cdc replication Kafka target send completed"
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
        bytes
            .saturating_add(24)
            .saturating_add(record.byte_len())
            .saturating_add(record.headers().len().saturating_mul(16))
    })
}

fn buffer_limit_violation(
    pending_transactions: usize,
    pending_records: usize,
    pending_bytes: usize,
    oldest_pending_age_ms: Option<u64>,
    incoming_bytes: usize,
    incoming_records: usize,
    limits: ReplicationBufferLimits,
) -> Option<ReplicationBufferLimitViolation> {
    if let Some(max_pending_bytes) = limits.max_pending_bytes
        && pending_bytes.saturating_add(incoming_bytes) > max_pending_bytes
    {
        return Some(ReplicationBufferLimitViolation::Bytes {
            pending_bytes,
            incoming_bytes,
            max_pending_bytes,
        });
    }
    if let Some(max_pending_records) = limits.max_pending_records
        && pending_records.saturating_add(incoming_records) > max_pending_records
    {
        return Some(ReplicationBufferLimitViolation::Records {
            pending_records,
            incoming_records,
            max_pending_records,
        });
    }
    if let Some(max_pending_transactions) = limits.max_pending_transactions
        && pending_transactions.saturating_add(1) > max_pending_transactions
    {
        return Some(ReplicationBufferLimitViolation::Objects {
            pending_transactions,
            incoming_transactions: 1,
            max_pending_transactions,
        });
    }
    if let Some(max_pending_age_ms) = limits.max_pending_age_ms
        && let Some(oldest_pending_age_ms) = oldest_pending_age_ms
        && oldest_pending_age_ms > max_pending_age_ms
    {
        return Some(ReplicationBufferLimitViolation::Age {
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

mod encoding;

#[cfg(test)]
mod tests;
