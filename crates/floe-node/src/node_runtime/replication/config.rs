use std::sync::LazyLock;
use std::time::Duration;

use arrow_ipc::CompressionType;

pub(super) const REPLICATION_KAFKA_RETRY_ATTEMPTS: usize = 5;
pub(super) const REPLICATION_KAFKA_RETRY_BASE_MS: u64 = 50;
pub(super) const REPLICATION_KAFKA_MESSAGE_TIMEOUT_MS: &str = "1000";
pub(super) const REPLICATION_KAFKA_SEND_TIMEOUT: Duration = Duration::from_secs(2);
pub(super) const REPLICATION_KAFKA_METADATA_WARMUP_TIMEOUT: Duration = Duration::from_millis(500);
pub(super) const FLOE_JSON_VERSION: i64 = 1;
pub(super) const FLOE_JSON_DELETED_FIELD: &str = "__floe_deleted";
pub(super) const FLOE_JSON_VERSION_FIELD: &str = "__floe_version";
pub(super) const FLOE_HEADER_IDEMPOTENCY_KEY: &str = "floe-idempotency-key";
pub(super) const FLOE_HEADER_PIPELINE: &str = "floe-pipeline";
pub(super) const FLOE_HEADER_SOURCE: &str = "floe-source";
pub(super) const FLOE_HEADER_SOURCE_TABLE: &str = "floe-source-table";
pub(super) const FLOE_HEADER_SOURCE_POSITION: &str = "floe-source-position";
pub(super) const FLOE_HEADER_TRANSACTION_ID: &str = "floe-transaction-id";
pub(super) const FLOE_HEADER_RECORD_SEQUENCE: &str = "floe-record-sequence";
const DEFAULT_REPLICATION_ARROW_IPC_ROWS_PER_RECORD: usize = 16_384;
const DEFAULT_REPLICATION_SNAPSHOT_BATCHES_PER_CHUNK: usize = 1;
const DEFAULT_REPLICATION_KAFKA_METADATA_HEADERS: bool = false;
pub(super) const FLOE_JSON_PARALLEL_RECORD_THRESHOLD: usize = 4_096;
pub(super) static REPLICATION_ARROW_IPC_ROWS_PER_RECORD: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("FLOE_REPLICATION_ARROW_IPC_ROWS_PER_RECORD")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_REPLICATION_ARROW_IPC_ROWS_PER_RECORD)
});
pub(super) static REPLICATION_SNAPSHOT_BATCHES_PER_CHUNK: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("FLOE_REPLICATION_SNAPSHOT_BATCHES_PER_CHUNK")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_REPLICATION_SNAPSHOT_BATCHES_PER_CHUNK)
});
pub(super) static CDC_PERF_LOGGING_ENABLED: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("FLOE_CDC_PERF_LOG")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
});
pub(super) static REPLICATION_ARROW_IPC_COMPRESSION: LazyLock<
    Option<ReplicationArrowIpcCompression>,
> = LazyLock::new(|| {
    std::env::var("FLOE_REPLICATION_ARROW_IPC_COMPRESSION")
        .ok()
        .and_then(|value| ReplicationArrowIpcCompression::parse(&value))
});
pub(super) static REPLICATION_KAFKA_METADATA_HEADERS: LazyLock<bool> = LazyLock::new(|| {
    env_bool(
        "FLOE_REPLICATION_KAFKA_METADATA_HEADERS",
        DEFAULT_REPLICATION_KAFKA_METADATA_HEADERS,
    )
});

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReplicationArrowIpcCompression {
    Lz4Frame,
}

impl ReplicationArrowIpcCompression {
    pub(super) fn parse(value: &str) -> Option<Self> {
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

    pub(super) fn arrow_type(self) -> CompressionType {
        match self {
            Self::Lz4Frame => CompressionType::LZ4_FRAME,
        }
    }
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
