use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use url::Url;

use floe_core::catalog::PostgresCdcSchemaEvolutionPolicy;
use floe_node_core::generator::{AUCTION_SOURCE_NAME, BID_SOURCE_NAME, PERSON_SOURCE_NAME};
use floe_node_core::source::SourceRegistry;
use floe_sql_parser::{
    CreateSourceDefinition, MaterializedViewDefinition, SinkConnector, SinkDefinition,
    SourceConnector,
};

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    #[serde(default)]
    pub connectors: Vec<ConnectorConfig>,
    #[serde(default)]
    pub materialized_views: Vec<MaterializedViewConfig>,
    #[serde(default)]
    pub sinks: Vec<SinkConfig>,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub maintenance: MaintenanceConfig,
    #[serde(default)]
    pub replication: ReplicationConfig,
    #[serde(default)]
    pub postgres_cdc: PostgresCdcConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedViewConfig {
    pub name: String,
    pub query: String,
    #[serde(default)]
    pub if_not_exists: bool,
}

impl MaterializedViewConfig {
    pub fn to_definition(&self) -> MaterializedViewDefinition {
        MaterializedViewDefinition::new(self.name.clone(), self.query.clone(), self.if_not_exists)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    #[serde(default)]
    pub events_per_second: Option<f64>,
    #[serde(default)]
    pub max_events: Option<u64>,
    #[serde(default)]
    pub ingest_queue_capacity: Option<usize>,
    #[serde(default)]
    pub ingest_batch_size: Option<usize>,
    #[serde(default)]
    pub ingest_batch_per_source: Option<usize>,
    #[serde(default)]
    pub ingest_batch_per_connector: Option<usize>,
    #[serde(default)]
    pub mv_retain_last: Option<usize>,
    #[serde(default)]
    pub http_host: Option<String>,
    #[serde(default)]
    pub kafka_group_id: Option<String>,
    #[serde(default)]
    pub kafka_poll_ms: Option<u64>,
    #[serde(default)]
    pub kafka_max_messages: Option<usize>,
    #[serde(default)]
    pub watermark_idle_source_ms: Option<u64>,
    #[serde(default)]
    pub subscribe_channel_capacity: Option<usize>,
    #[serde(default)]
    pub subscribe_max_catchup_versions: Option<i64>,
    #[serde(default)]
    pub admin_port: Option<u16>,
    #[serde(default)]
    pub pgwire_addr: Option<String>,
    #[serde(default)]
    pub pgwire_enabled: Option<bool>,
    #[serde(default)]
    pub mv_flush: MvFlushConfig,
    #[serde(default)]
    pub mv_snapshot: MvSnapshotConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MvFlushConfig {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub max_pending_deltas: Option<usize>,
    #[serde(default)]
    pub max_pending_versions: Option<usize>,
    #[serde(default)]
    pub max_pending_rows: Option<usize>,
    #[serde(default)]
    pub max_pending_bytes: Option<usize>,
    #[serde(default)]
    pub max_delay_ms: Option<u64>,
    #[serde(default)]
    pub flush_on_catchup_boundary: Option<bool>,
    #[serde(default)]
    pub flush_on_shutdown: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MvSnapshotConfig {
    #[serde(default)]
    pub max_pending_batches: Option<usize>,
    #[serde(default)]
    pub max_pending_rows: Option<usize>,
    #[serde(default)]
    pub max_delay_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(default)]
    pub await_durable: Option<bool>,
    #[serde(default)]
    pub data_dir: Option<String>,
    #[serde(default)]
    pub object_store_from_env: bool,
    #[serde(default)]
    pub object_store_env_file: Option<String>,
    #[serde(default)]
    pub slatedb_name: Option<String>,
    #[serde(default)]
    pub slatedb_config: Option<String>,
    #[serde(default)]
    pub slatedb_env_prefix: Option<String>,
    #[serde(default)]
    pub slatedb_close_timeout_ms: Option<u64>,
    #[serde(default)]
    pub zset_compaction_max_chain_len: Option<usize>,
    #[serde(default)]
    pub zset_compaction_max_segments: Option<usize>,
    #[serde(default)]
    pub zset_compaction_backoff_ticks: Option<u64>,
    #[serde(default)]
    pub zset_compaction_max_concurrent_jobs: Option<usize>,
    #[serde(default)]
    pub zset_gc_grace_period_ms: Option<u64>,
    #[serde(default)]
    pub source_journal: Option<SourceJournalConfig>,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SourceJournalConfig {
    Auto,
    Full,
    None,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceConfig {
    #[serde(default)]
    pub paused: Option<bool>,
    #[serde(default)]
    pub inspect_namespace: Vec<String>,
    #[serde(default)]
    pub compact_namespace: Vec<String>,
    #[serde(default)]
    pub gc_namespace: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReplicationConfig {
    #[serde(default)]
    pub buffer_cleanup: ReplicationBufferCleanupConfig,
    #[serde(default)]
    pub buffer_limits: ReplicationBufferLimitsConfig,
    #[serde(default)]
    pub kafka: ReplicationKafkaProducerConfig,
    #[serde(default)]
    pub encoding: ReplicationEncodingConfig,
    #[serde(default)]
    pub perf_log: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PostgresCdcConfig {
    #[serde(default)]
    pub snapshot: PostgresCdcSnapshotConfig,
    #[serde(default)]
    pub reconnect: PostgresCdcReconnectConfig,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PostgresCdcReconnectConfig {
    #[serde(default = "default_postgres_cdc_reconnect_max_reconnects")]
    pub max_reconnects: usize,
    #[serde(default = "default_postgres_cdc_reconnect_retry_base_ms")]
    pub retry_base_ms: u64,
    #[serde(default = "default_postgres_cdc_reconnect_retry_max_backoff_ms")]
    pub retry_max_backoff_ms: u64,
}

impl Default for PostgresCdcReconnectConfig {
    fn default() -> Self {
        Self {
            max_reconnects: DEFAULT_POSTGRES_CDC_RECONNECT_MAX_RECONNECTS,
            retry_base_ms: DEFAULT_POSTGRES_CDC_RECONNECT_RETRY_BASE_MS,
            retry_max_backoff_ms: DEFAULT_POSTGRES_CDC_RECONNECT_RETRY_MAX_BACKOFF_MS,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PostgresCdcSnapshotConfig {
    #[serde(default = "default_postgres_cdc_snapshot_rows_per_batch")]
    pub rows_per_batch: usize,
    #[serde(default = "default_postgres_cdc_snapshot_max_workers")]
    pub max_workers: usize,
    #[serde(default = "default_postgres_cdc_snapshot_intra_table_chunks")]
    pub intra_table_chunks: usize,
    #[serde(default = "default_postgres_cdc_snapshot_adaptive_concurrency")]
    pub adaptive_concurrency: bool,
    #[serde(default = "default_postgres_cdc_snapshot_min_workers")]
    pub min_workers: usize,
    #[serde(default = "default_postgres_cdc_snapshot_wal_buffer_high_watermark_percent")]
    pub wal_buffer_high_watermark_percent: usize,
    #[serde(default = "default_postgres_cdc_snapshot_wal_buffer_low_watermark_percent")]
    pub wal_buffer_low_watermark_percent: usize,
    #[serde(default = "default_postgres_cdc_snapshot_slow_scan_ms")]
    pub slow_scan_ms: u64,
    #[serde(default = "default_postgres_cdc_snapshot_controller_interval_ms")]
    pub controller_interval_ms: u64,
    #[serde(default)]
    pub perf_log: bool,
}

impl Default for PostgresCdcSnapshotConfig {
    fn default() -> Self {
        Self {
            rows_per_batch: DEFAULT_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH,
            max_workers: DEFAULT_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS,
            intra_table_chunks: DEFAULT_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS,
            adaptive_concurrency: true,
            min_workers: DEFAULT_POSTGRES_CDC_SNAPSHOT_MIN_WORKERS,
            wal_buffer_high_watermark_percent:
                DEFAULT_POSTGRES_CDC_SNAPSHOT_WAL_BUFFER_HIGH_WATERMARK_PERCENT,
            wal_buffer_low_watermark_percent:
                DEFAULT_POSTGRES_CDC_SNAPSHOT_WAL_BUFFER_LOW_WATERMARK_PERCENT,
            slow_scan_ms: DEFAULT_POSTGRES_CDC_SNAPSHOT_SLOW_SCAN_MS,
            controller_interval_ms: DEFAULT_POSTGRES_CDC_SNAPSHOT_CONTROLLER_INTERVAL_MS,
            perf_log: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReplicationBufferCleanupConfig {
    #[serde(default = "default_replication_buffer_delivered_retention_ms")]
    pub delivered_retention_ms: u64,
    #[serde(default = "default_replication_buffer_orphan_retention_ms")]
    pub orphan_retention_ms: u64,
    #[serde(default = "default_replication_buffer_cleanup_interval_ms")]
    pub cleanup_interval_ms: u64,
}

impl ReplicationBufferCleanupConfig {
    pub const DEFAULT: Self = Self {
        delivered_retention_ms: DEFAULT_REPLICATION_BUFFER_DELIVERED_RETENTION_MS,
        orphan_retention_ms: DEFAULT_REPLICATION_BUFFER_ORPHAN_RETENTION_MS,
        cleanup_interval_ms: DEFAULT_REPLICATION_BUFFER_CLEANUP_INTERVAL_MS,
    };
}

impl Default for ReplicationBufferCleanupConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReplicationBufferLimitsConfig {
    #[serde(default = "default_replication_buffer_max_pending_bytes")]
    pub max_pending_bytes: usize,
    #[serde(default)]
    pub max_pending_records: usize,
    #[serde(default)]
    pub max_pending_transactions: usize,
    #[serde(default)]
    pub max_pending_age_ms: u64,
}

impl ReplicationBufferLimitsConfig {
    pub const DEFAULT: Self = Self {
        max_pending_bytes: DEFAULT_REPLICATION_BUFFER_MAX_PENDING_BYTES,
        max_pending_records: 0,
        max_pending_transactions: 0,
        max_pending_age_ms: 0,
    };
}

impl Default for ReplicationBufferLimitsConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReplicationKafkaProducerConfig {
    #[serde(default = "default_replication_kafka_message_max_bytes")]
    pub message_max_bytes: usize,
    #[serde(default = "default_replication_kafka_acks")]
    pub acks: String,
    #[serde(default)]
    pub enable_idempotence: bool,
    #[serde(default = "default_replication_kafka_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_replication_kafka_batch_num_messages")]
    pub batch_num_messages: usize,
    #[serde(default = "default_replication_kafka_linger_ms")]
    pub linger_ms: usize,
    #[serde(default = "default_replication_kafka_queue_max_messages")]
    pub queue_max_messages: usize,
    #[serde(default = "default_replication_kafka_queue_max_kbytes")]
    pub queue_max_kbytes: usize,
    #[serde(default)]
    pub message_send_max_retries: usize,
}

impl Default for ReplicationKafkaProducerConfig {
    fn default() -> Self {
        Self {
            message_max_bytes: DEFAULT_REPLICATION_KAFKA_MESSAGE_MAX_BYTES,
            acks: default_replication_kafka_acks(),
            enable_idempotence: false,
            batch_size: DEFAULT_REPLICATION_KAFKA_BATCH_SIZE,
            batch_num_messages: DEFAULT_REPLICATION_KAFKA_BATCH_NUM_MESSAGES,
            linger_ms: DEFAULT_REPLICATION_KAFKA_LINGER_MS,
            queue_max_messages: DEFAULT_REPLICATION_KAFKA_QUEUE_MAX_MESSAGES,
            queue_max_kbytes: DEFAULT_REPLICATION_KAFKA_QUEUE_MAX_KBYTES,
            message_send_max_retries: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReplicationEncodingConfig {
    #[serde(default = "default_replication_arrow_ipc_rows_per_record")]
    pub arrow_ipc_rows_per_record: usize,
    #[serde(default = "default_replication_snapshot_batches_per_chunk")]
    pub snapshot_batches_per_chunk: usize,
    #[serde(default)]
    pub arrow_ipc_compression: Option<ReplicationArrowIpcCompressionConfig>,
    #[serde(default)]
    pub kafka_metadata_headers: bool,
}

impl Default for ReplicationEncodingConfig {
    fn default() -> Self {
        Self {
            arrow_ipc_rows_per_record: DEFAULT_REPLICATION_ARROW_IPC_ROWS_PER_RECORD,
            snapshot_batches_per_chunk: DEFAULT_REPLICATION_SNAPSHOT_BATCHES_PER_CHUNK,
            arrow_ipc_compression: None,
            kafka_metadata_headers: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationArrowIpcCompressionConfig {
    #[serde(alias = "lz4", alias = "lz4-frame")]
    Lz4Frame,
}

impl ReplicationArrowIpcCompressionConfig {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "none" | "off" | "false" | "0" => None,
            "lz4" | "lz4_frame" | "lz4-frame" => Some(Self::Lz4Frame),
            _ => None,
        }
    }
}

const DEFAULT_REPLICATION_BUFFER_DELIVERED_RETENTION_MS: u64 = 5_000;
const DEFAULT_REPLICATION_BUFFER_ORPHAN_RETENTION_MS: u64 = 60_000;
const DEFAULT_REPLICATION_BUFFER_CLEANUP_INTERVAL_MS: u64 = 5_000;
const DEFAULT_REPLICATION_BUFFER_MAX_PENDING_BYTES: usize = 10 * 1024 * 1024 * 1024;
const DEFAULT_REPLICATION_KAFKA_MESSAGE_MAX_BYTES: usize = 10_485_760;
const DEFAULT_REPLICATION_KAFKA_BATCH_SIZE: usize = 1_000_000;
const DEFAULT_REPLICATION_KAFKA_BATCH_NUM_MESSAGES: usize = 1_000_000;
const DEFAULT_REPLICATION_KAFKA_LINGER_MS: usize = 1;
const DEFAULT_REPLICATION_KAFKA_QUEUE_MAX_MESSAGES: usize = 1_000_000;
const DEFAULT_REPLICATION_KAFKA_QUEUE_MAX_KBYTES: usize = 1_048_576;
const DEFAULT_REPLICATION_ARROW_IPC_ROWS_PER_RECORD: usize = 16_384;
const DEFAULT_REPLICATION_SNAPSHOT_BATCHES_PER_CHUNK: usize = 1;
const DEFAULT_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH: usize = 16_384;
const DEFAULT_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS: usize = 1;
const DEFAULT_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS: usize = 1;
const DEFAULT_POSTGRES_CDC_SNAPSHOT_MIN_WORKERS: usize = 1;
const DEFAULT_POSTGRES_CDC_SNAPSHOT_WAL_BUFFER_HIGH_WATERMARK_PERCENT: usize = 75;
const DEFAULT_POSTGRES_CDC_SNAPSHOT_WAL_BUFFER_LOW_WATERMARK_PERCENT: usize = 25;
const DEFAULT_POSTGRES_CDC_SNAPSHOT_SLOW_SCAN_MS: u64 = 30_000;
const DEFAULT_POSTGRES_CDC_SNAPSHOT_CONTROLLER_INTERVAL_MS: u64 = 500;
const DEFAULT_POSTGRES_CDC_RECONNECT_MAX_RECONNECTS: usize = 10;
const DEFAULT_POSTGRES_CDC_RECONNECT_RETRY_BASE_MS: u64 = 1_000;
const DEFAULT_POSTGRES_CDC_RECONNECT_RETRY_MAX_BACKOFF_MS: u64 = 30_000;

fn default_replication_buffer_delivered_retention_ms() -> u64 {
    DEFAULT_REPLICATION_BUFFER_DELIVERED_RETENTION_MS
}

fn default_replication_buffer_orphan_retention_ms() -> u64 {
    DEFAULT_REPLICATION_BUFFER_ORPHAN_RETENTION_MS
}

fn default_replication_buffer_cleanup_interval_ms() -> u64 {
    DEFAULT_REPLICATION_BUFFER_CLEANUP_INTERVAL_MS
}

fn default_replication_buffer_max_pending_bytes() -> usize {
    DEFAULT_REPLICATION_BUFFER_MAX_PENDING_BYTES
}

fn default_replication_kafka_message_max_bytes() -> usize {
    DEFAULT_REPLICATION_KAFKA_MESSAGE_MAX_BYTES
}

fn default_replication_kafka_acks() -> String {
    "1".to_string()
}

fn default_replication_kafka_batch_size() -> usize {
    DEFAULT_REPLICATION_KAFKA_BATCH_SIZE
}

fn default_replication_kafka_batch_num_messages() -> usize {
    DEFAULT_REPLICATION_KAFKA_BATCH_NUM_MESSAGES
}

fn default_replication_kafka_linger_ms() -> usize {
    DEFAULT_REPLICATION_KAFKA_LINGER_MS
}

fn default_replication_kafka_queue_max_messages() -> usize {
    DEFAULT_REPLICATION_KAFKA_QUEUE_MAX_MESSAGES
}

fn default_replication_kafka_queue_max_kbytes() -> usize {
    DEFAULT_REPLICATION_KAFKA_QUEUE_MAX_KBYTES
}

fn default_replication_arrow_ipc_rows_per_record() -> usize {
    DEFAULT_REPLICATION_ARROW_IPC_ROWS_PER_RECORD
}

fn default_replication_snapshot_batches_per_chunk() -> usize {
    DEFAULT_REPLICATION_SNAPSHOT_BATCHES_PER_CHUNK
}

fn default_postgres_cdc_snapshot_rows_per_batch() -> usize {
    DEFAULT_POSTGRES_CDC_SNAPSHOT_ROWS_PER_BATCH
}

fn default_postgres_cdc_snapshot_max_workers() -> usize {
    DEFAULT_POSTGRES_CDC_SNAPSHOT_MAX_WORKERS
}

fn default_postgres_cdc_snapshot_intra_table_chunks() -> usize {
    DEFAULT_POSTGRES_CDC_SNAPSHOT_INTRA_TABLE_CHUNKS
}

fn default_postgres_cdc_snapshot_adaptive_concurrency() -> bool {
    true
}

fn default_postgres_cdc_snapshot_min_workers() -> usize {
    DEFAULT_POSTGRES_CDC_SNAPSHOT_MIN_WORKERS
}

fn default_postgres_cdc_snapshot_wal_buffer_high_watermark_percent() -> usize {
    DEFAULT_POSTGRES_CDC_SNAPSHOT_WAL_BUFFER_HIGH_WATERMARK_PERCENT
}

fn default_postgres_cdc_snapshot_wal_buffer_low_watermark_percent() -> usize {
    DEFAULT_POSTGRES_CDC_SNAPSHOT_WAL_BUFFER_LOW_WATERMARK_PERCENT
}

fn default_postgres_cdc_snapshot_slow_scan_ms() -> u64 {
    DEFAULT_POSTGRES_CDC_SNAPSHOT_SLOW_SCAN_MS
}

fn default_postgres_cdc_snapshot_controller_interval_ms() -> u64 {
    DEFAULT_POSTGRES_CDC_SNAPSHOT_CONTROLLER_INTERVAL_MS
}

fn default_postgres_cdc_reconnect_max_reconnects() -> usize {
    DEFAULT_POSTGRES_CDC_RECONNECT_MAX_RECONNECTS
}

fn default_postgres_cdc_reconnect_retry_base_ms() -> u64 {
    DEFAULT_POSTGRES_CDC_RECONNECT_RETRY_BASE_MS
}

fn default_postgres_cdc_reconnect_retry_max_backoff_ms() -> u64 {
    DEFAULT_POSTGRES_CDC_RECONNECT_RETRY_MAX_BACKOFF_MS
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConnectorConfig {
    Kafka {
        #[serde(default)]
        name: Option<String>,
        brokers: String,
        topics: Vec<String>,
        #[serde(default)]
        group_id: Option<String>,
        #[serde(default)]
        default_source: Option<String>,
        #[serde(default)]
        poll_ms: Option<u64>,
        #[serde(default)]
        max_messages_per_tick: Option<usize>,
        #[serde(default)]
        format: Option<String>,
    },
    File {
        #[serde(default)]
        name: Option<String>,
        path: String,
        #[serde(default)]
        default_source: Option<String>,
    },
    Http {
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        host: Option<String>,
        port: u16,
        #[serde(default)]
        default_source: Option<String>,
    },
    Generator {
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        events_per_second: Option<f64>,
        #[serde(default)]
        max_events: Option<u64>,
    },
    ObjectStore {
        #[serde(default)]
        name: Option<String>,
        url: String,
        #[serde(default)]
        default_source: Option<String>,
    },
    PostgresCdc {
        #[serde(default)]
        name: Option<String>,
        connection: String,
        slot: String,
        #[serde(default)]
        publication: Option<String>,
        #[serde(default)]
        include_tables: Option<Vec<String>>,
        #[serde(default)]
        include_schema_in_source: Option<bool>,
        #[serde(default)]
        schema_evolution_policy: Option<PostgresCdcSchemaEvolutionPolicy>,
        #[serde(default)]
        auto_create_slot: Option<bool>,
        #[serde(default)]
        auto_create_publication: Option<bool>,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SinkConfig {
    Kafka {
        #[serde(default)]
        name: Option<String>,
        brokers: String,
        topic: String,
        mv: String,
        #[serde(default)]
        format: Option<String>,
        #[serde(default)]
        key_columns: Option<Vec<String>>,
        #[serde(default)]
        with_snapshot: Option<bool>,
        #[serde(default)]
        as_of: Option<i64>,
        #[serde(default)]
        batch_rows: Option<usize>,
        #[serde(default)]
        batch_bytes: Option<usize>,
        #[serde(default)]
        queue_capacity: Option<usize>,
        #[serde(default)]
        retry_max_attempts: Option<usize>,
        #[serde(default)]
        retry_base_ms: Option<u64>,
        #[serde(default)]
        retry_max_backoff_ms: Option<u64>,
        #[serde(default)]
        transactional_id: Option<String>,
        #[serde(default)]
        checkpoint_topic: Option<String>,
        #[serde(default)]
        checkpoint_partition: Option<i32>,
    },
    File {
        #[serde(default)]
        name: Option<String>,
        path: String,
        mv: String,
        #[serde(default)]
        with_snapshot: Option<bool>,
        #[serde(default)]
        as_of: Option<i64>,
        #[serde(default)]
        append: Option<bool>,
        #[serde(default)]
        batch_rows: Option<usize>,
        #[serde(default)]
        batch_bytes: Option<usize>,
        #[serde(default)]
        queue_capacity: Option<usize>,
    },
    Http {
        #[serde(default)]
        name: Option<String>,
        url: String,
        mv: String,
        #[serde(default)]
        with_snapshot: Option<bool>,
        #[serde(default)]
        as_of: Option<i64>,
        #[serde(default)]
        batch_size: Option<usize>,
        #[serde(default)]
        batch_rows: Option<usize>,
        #[serde(default)]
        batch_bytes: Option<usize>,
        #[serde(default)]
        queue_capacity: Option<usize>,
        #[serde(default)]
        retry_max_attempts: Option<usize>,
        #[serde(default)]
        retry_base_ms: Option<u64>,
        #[serde(default)]
        retry_max_backoff_ms: Option<u64>,
    },
    Postgres {
        #[serde(default)]
        name: Option<String>,
        connection: String,
        table: String,
        mv: String,
        #[serde(default)]
        mode: Option<String>,
        #[serde(default)]
        primary_key: Option<Vec<String>>,
        #[serde(default)]
        with_snapshot: Option<bool>,
        #[serde(default)]
        as_of: Option<i64>,
        #[serde(default)]
        retry_max_attempts: Option<usize>,
        #[serde(default)]
        retry_base_ms: Option<u64>,
        #[serde(default)]
        retry_max_backoff_ms: Option<u64>,
    },
}

#[derive(Debug, Clone)]
pub struct ConnectorSpec {
    pub name: String,
    pub config: ConnectorConfig,
}

#[derive(Debug, Clone)]
pub struct SinkSpec {
    pub name: String,
    pub config: SinkConfig,
}

mod loading;
mod normalization;
mod validation;

pub use loading::{load_config, load_toml_config, parse_toml_config};
pub use normalization::{
    apply_connector_properties, connector_spec_from_sql, materialized_view_definitions_from_config,
    normalize_connectors, normalize_sinks, sink_spec_from_sql,
};
use validation::validate_node_config;

#[cfg(test)]
#[path = "lib/tests.rs"]
mod tests;
