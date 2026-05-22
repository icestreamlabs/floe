use std::time::Duration;

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
pub(super) const FLOE_JSON_PARALLEL_RECORD_THRESHOLD: usize = 4_096;
