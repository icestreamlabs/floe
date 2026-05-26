use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use url::Url;

use floe_core::catalog::PostgresCdcSchemaEvolutionPolicy;
use floe_node_core::generator::{AUCTION_SOURCE_NAME, BID_SOURCE_NAME, PERSON_SOURCE_NAME};
use floe_node_core::source::SourceRegistry;
use floe_sql_parser::{MaterializedViewDefinition, SinkConnector, SinkDefinition};

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

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OutputConsolidationModeConfig {
    AllColumns,
    Key,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    #[serde(default)]
    pub events_per_second: Option<f64>,
    #[serde(default)]
    pub max_events: Option<u64>,
    #[serde(default)]
    pub output_consolidation_mode: Option<OutputConsolidationModeConfig>,
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
    pub pre_tick_commit_delay_ms: Option<u64>,
    #[serde(default)]
    pub tail_channel_capacity: Option<usize>,
    #[serde(default)]
    pub tail_max_catchup_versions: Option<i64>,
    #[serde(default)]
    pub transient_segment_max_nodes: Option<usize>,
    #[serde(default)]
    pub transient_segment_min_score: Option<i32>,
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
    apply_connector_properties, materialized_view_definitions_from_config, normalize_connectors,
    normalize_sinks, sink_spec_from_sql,
};
use validation::validate_node_config;

#[cfg(test)]
mod tests {
    use super::*;
    use floe_sql_parser::{SinkConnector, SinkDefinition};

    #[test]
    fn normalize_assigns_unique_names() {
        let configs = vec![
            ConnectorConfig::Kafka {
                name: None,
                brokers: "localhost:9092".to_string(),
                topics: vec!["a".to_string()],
                group_id: None,
                default_source: None,
                poll_ms: None,
                max_messages_per_tick: None,
                format: None,
            },
            ConnectorConfig::Kafka {
                name: None,
                brokers: "localhost:9092".to_string(),
                topics: vec!["b".to_string()],
                group_id: None,
                default_source: None,
                poll_ms: None,
                max_messages_per_tick: None,
                format: None,
            },
        ];
        let specs = normalize_connectors(configs).expect("normalize");
        assert_eq!(specs[0].name, "kafka");
        assert_eq!(specs[1].name, "kafka_2");
    }

    #[test]
    fn load_config_accepts_toml() {
        let input = r#"
            [[connectors]]
            type = "generator"
            events_per_second = 12.5
            max_events = 100
        "#;
        let config: NodeConfig = toml::from_str(input).expect("parse toml");
        assert_eq!(config.connectors.len(), 1);
    }

    #[test]
    fn parse_toml_config_accepts_multiline_sql() {
        let input = r#"
            [[materialized_views]]
            name = "mv_orders"
            query = '''
            CREATE MATERIALIZED VIEW mv_orders AS
            SELECT customer_id, count(*) AS order_count
            FROM orders
            GROUP BY customer_id
            '''
        "#;

        let config = parse_toml_config(input).expect("parse toml config");

        assert_eq!(config.materialized_views.len(), 1);
        assert!(
            config.materialized_views[0]
                .query
                .contains("GROUP BY customer_id")
        );
    }

    #[test]
    fn maps_sql_sink_definition_to_runtime_config() {
        let definition = SinkDefinition::new(
            "out_http",
            "mv_bid",
            SinkConnector::Http {
                url: "http://localhost:8080".to_string(),
                batch_size: Some(16),
            },
            true,
            Some(7),
        );
        let spec = sink_spec_from_sql(&definition).expect("map sink");
        match spec.config {
            SinkConfig::Http {
                name,
                url,
                mv,
                with_snapshot,
                as_of,
                batch_size,
                ..
            } => {
                assert_eq!(name.as_deref(), Some("out_http"));
                assert_eq!(url, "http://localhost:8080");
                assert_eq!(mv, "mv_bid");
                assert_eq!(with_snapshot, Some(true));
                assert_eq!(as_of, Some(7));
                assert_eq!(batch_size, Some(16));
            }
            other => panic!("expected HTTP sink config, got {other:?}"),
        }
    }

    #[test]
    fn maps_sql_kafka_debezium_sink_options_to_runtime_config() {
        let definition = SinkDefinition::new(
            "out_orders",
            "mv_orders",
            SinkConnector::Kafka {
                brokers: "localhost:9092".to_string(),
                topic: "orders".to_string(),
                format: Some("debezium_json".to_string()),
                key_columns: vec!["tenant_id".to_string(), "id".to_string()],
            },
            false,
            None,
        );
        let spec = sink_spec_from_sql(&definition).expect("map sink");
        match spec.config {
            SinkConfig::Kafka {
                name,
                brokers,
                topic,
                mv,
                format,
                key_columns,
                ..
            } => {
                assert_eq!(name.as_deref(), Some("out_orders"));
                assert_eq!(brokers, "localhost:9092");
                assert_eq!(topic, "orders");
                assert_eq!(mv, "mv_orders");
                assert_eq!(format.as_deref(), Some("debezium_json"));
                assert_eq!(
                    key_columns,
                    Some(vec!["tenant_id".to_string(), "id".to_string()])
                );
            }
            other => panic!("expected Kafka sink config, got {other:?}"),
        }
    }

    #[test]
    fn maps_sql_postgres_sink_options_to_runtime_config() {
        let definition = SinkDefinition::new(
            "out_orders",
            "mv_orders",
            SinkConnector::Postgres {
                connection: "postgres://postgres:postgres@localhost/postgres".to_string(),
                table: "public.orders_copy".to_string(),
                mode: Some("upsert".to_string()),
                primary_key: vec!["tenant_id".to_string(), "id".to_string()],
            },
            true,
            Some(9),
        );
        let spec = sink_spec_from_sql(&definition).expect("map sink");
        match spec.config {
            SinkConfig::Postgres {
                name,
                connection,
                table,
                mv,
                mode,
                primary_key,
                with_snapshot,
                as_of,
                ..
            } => {
                assert_eq!(name.as_deref(), Some("out_orders"));
                assert_eq!(
                    connection,
                    "postgres://postgres:postgres@localhost/postgres"
                );
                assert_eq!(table, "public.orders_copy");
                assert_eq!(mv, "mv_orders");
                assert_eq!(mode.as_deref(), Some("upsert"));
                assert_eq!(
                    primary_key,
                    Some(vec!["tenant_id".to_string(), "id".to_string()])
                );
                assert_eq!(with_snapshot, Some(true));
                assert_eq!(as_of, Some(9));
            }
            other => panic!("expected Postgres sink config, got {other:?}"),
        }
    }

    #[test]
    fn validation_rejects_empty_kafka_topics() {
        let config = NodeConfig {
            connectors: vec![ConnectorConfig::Kafka {
                name: Some("kafka_ingest".to_string()),
                brokers: "localhost:9092".to_string(),
                topics: vec![],
                group_id: Some("floe".to_string()),
                default_source: Some("nexmark_bid".to_string()),
                poll_ms: Some(100),
                max_messages_per_tick: Some(64),
                format: None,
            }],
            ..NodeConfig::default()
        };
        let err = validate_node_config(&config).expect_err("validation should fail");
        assert!(
            err.to_string()
                .contains("connectors[0].topics must not be empty")
        );
    }

    #[test]
    fn validation_rejects_invalid_object_store_url() {
        let config = NodeConfig {
            connectors: vec![ConnectorConfig::ObjectStore {
                name: None,
                url: "not a url".to_string(),
                default_source: Some("nexmark_bid".to_string()),
            }],
            ..NodeConfig::default()
        };
        let err = validate_node_config(&config).expect_err("validation should fail");
        assert!(
            err.to_string()
                .contains("connectors[0].url must be a valid URL")
        );
    }

    #[test]
    fn validation_rejects_unknown_kafka_format() {
        let config = NodeConfig {
            connectors: vec![ConnectorConfig::Kafka {
                name: Some("kafka_ingest".to_string()),
                brokers: "localhost:9092".to_string(),
                topics: vec!["events".to_string()],
                group_id: Some("floe".to_string()),
                default_source: Some("nexmark_bid".to_string()),
                poll_ms: Some(100),
                max_messages_per_tick: Some(64),
                format: Some("bad_format".to_string()),
            }],
            ..NodeConfig::default()
        };
        let err = validate_node_config(&config).expect_err("validation should fail");
        assert!(
            err.to_string()
                .contains("connectors[0].format must be one of")
        );
    }

    #[test]
    fn validation_rejects_non_positive_watermark_idle_source_ms() {
        let config = NodeConfig {
            runtime: RuntimeConfig {
                watermark_idle_source_ms: Some(0),
                ..RuntimeConfig::default()
            },
            ..NodeConfig::default()
        };
        let err = validate_node_config(&config).expect_err("validation should fail");
        assert!(
            err.to_string()
                .contains("runtime.watermark_idle_source_ms must be greater than 0")
        );
    }

    #[test]
    fn validation_rejects_non_positive_mv_flush_max_pending_deltas() {
        let config = NodeConfig {
            runtime: RuntimeConfig {
                mv_flush: MvFlushConfig {
                    max_pending_deltas: Some(0),
                    ..MvFlushConfig::default()
                },
                ..RuntimeConfig::default()
            },
            ..NodeConfig::default()
        };
        let err = validate_node_config(&config).expect_err("validation should fail");
        assert!(
            err.to_string()
                .contains("runtime.mv_flush.max_pending_deltas must be greater than 0")
        );
    }

    #[test]
    fn validation_rejects_invalid_http_sink_url() {
        let config = NodeConfig {
            connectors: vec![ConnectorConfig::Generator {
                name: None,
                events_per_second: Some(1.0),
                max_events: None,
            }],
            sinks: vec![SinkConfig::Http {
                name: Some("sink_http".to_string()),
                url: "://missing-scheme".to_string(),
                mv: "mv_bid".to_string(),
                with_snapshot: Some(true),
                as_of: None,
                batch_size: Some(1),
                batch_rows: None,
                batch_bytes: None,
                queue_capacity: None,
                retry_max_attempts: None,
                retry_base_ms: None,
                retry_max_backoff_ms: None,
            }],
            ..NodeConfig::default()
        };
        let err = validate_node_config(&config).expect_err("validation should fail");
        assert!(err.to_string().contains("sinks[0].url must be a valid URL"));
    }

    #[test]
    fn validation_rejects_negative_kafka_checkpoint_partition() {
        let config = NodeConfig {
            connectors: vec![ConnectorConfig::Generator {
                name: None,
                events_per_second: Some(1.0),
                max_events: None,
            }],
            sinks: vec![SinkConfig::Kafka {
                name: Some("sink_kafka".to_string()),
                brokers: "localhost:9092".to_string(),
                topic: "out".to_string(),
                mv: "mv_bid".to_string(),
                format: None,
                key_columns: None,
                with_snapshot: Some(false),
                as_of: None,
                batch_rows: Some(1),
                batch_bytes: None,
                queue_capacity: None,
                retry_max_attempts: None,
                retry_base_ms: None,
                retry_max_backoff_ms: None,
                transactional_id: Some("tx-1".to_string()),
                checkpoint_topic: Some("out_checkpoint".to_string()),
                checkpoint_partition: Some(-1),
            }],
            ..NodeConfig::default()
        };
        let err = validate_node_config(&config).expect_err("validation should fail");
        assert!(
            err.to_string()
                .contains("sinks[0].checkpoint_partition must be >= 0")
        );
    }

    #[test]
    fn validation_requires_key_columns_for_debezium_kafka_sink() {
        let config = NodeConfig {
            connectors: vec![ConnectorConfig::Generator {
                name: None,
                events_per_second: Some(1.0),
                max_events: None,
            }],
            sinks: vec![SinkConfig::Kafka {
                name: Some("sink_kafka".to_string()),
                brokers: "localhost:9092".to_string(),
                topic: "out".to_string(),
                mv: "mv_bid".to_string(),
                format: Some("debezium_json".to_string()),
                key_columns: None,
                with_snapshot: Some(false),
                as_of: None,
                batch_rows: Some(1),
                batch_bytes: None,
                queue_capacity: None,
                retry_max_attempts: None,
                retry_base_ms: None,
                retry_max_backoff_ms: None,
                transactional_id: None,
                checkpoint_topic: None,
                checkpoint_partition: None,
            }],
            ..NodeConfig::default()
        };
        let err = validate_node_config(&config).expect_err("validation should fail");
        assert!(err.to_string().contains("sinks[0].key_columns is required"));
    }

    #[test]
    fn validation_requires_primary_key_for_postgres_upsert_sink() {
        let config = NodeConfig {
            connectors: vec![ConnectorConfig::Generator {
                name: None,
                events_per_second: Some(1.0),
                max_events: None,
            }],
            sinks: vec![SinkConfig::Postgres {
                name: Some("sink_pg".to_string()),
                connection: "postgres://postgres:postgres@localhost/postgres".to_string(),
                table: "public.orders_copy".to_string(),
                mv: "mv_orders".to_string(),
                mode: Some("upsert".to_string()),
                primary_key: None,
                with_snapshot: Some(false),
                as_of: None,
                retry_max_attempts: None,
                retry_base_ms: None,
                retry_max_backoff_ms: None,
            }],
            ..NodeConfig::default()
        };
        let err = validate_node_config(&config).expect_err("validation should fail");
        assert!(err.to_string().contains("sinks[0].primary_key is required"));
    }

    #[test]
    fn validation_rejects_non_positive_replication_encoding_rows_per_record() {
        let config = NodeConfig {
            replication: ReplicationConfig {
                encoding: ReplicationEncodingConfig {
                    arrow_ipc_rows_per_record: 0,
                    ..ReplicationEncodingConfig::default()
                },
                ..ReplicationConfig::default()
            },
            ..NodeConfig::default()
        };

        let err = validate_node_config(&config).expect_err("validation should fail");
        assert!(
            err.to_string()
                .contains("replication.encoding.arrow_ipc_rows_per_record must be greater than 0")
        );
    }

    #[test]
    fn validation_rejects_invalid_postgres_cdc_snapshot_watermark() {
        let config = NodeConfig {
            postgres_cdc: PostgresCdcConfig {
                snapshot: PostgresCdcSnapshotConfig {
                    wal_buffer_high_watermark_percent: 0,
                    ..PostgresCdcSnapshotConfig::default()
                },
            },
            ..NodeConfig::default()
        };

        let err = validate_node_config(&config).expect_err("validation should fail");
        assert!(err.to_string().contains(
            "postgres_cdc.snapshot.wal_buffer_high_watermark_percent must be between 1 and 100"
        ));
    }

    #[test]
    fn load_config_accepts_materialized_views_and_runtime_sections() {
        let input = r#"
            [[connectors]]
            type = "generator"

            [[materialized_views]]
            name = "mv_cfg"
            query = "SELECT * FROM nexmark_bid"

            [runtime]
            ingest_batch_size = 128
            mv_retain_last = 5
            admin_port = 8082
            pgwire_addr = "127.0.0.1:6543"
            pgwire_enabled = false
            pre_tick_commit_delay_ms = 10
            tail_channel_capacity = 512
            tail_max_catchup_versions = 64
            transient_segment_max_nodes = 48
            transient_segment_min_score = 0

            [runtime.mv_snapshot]
            max_pending_batches = 2048
            max_pending_rows = 500000
            max_delay_ms = 2000

            [storage]
            await_durable = true
            data_dir = "/tmp/floe-data"
            source_journal = "auto"
            slatedb_close_timeout_ms = 1000
            zset_compaction_max_chain_len = 64

            [maintenance]
            paused = true
            inspect_namespace = ["mv::mv_cfg"]
        "#;
        let config: NodeConfig = toml::from_str(input).expect("parse toml");
        assert_eq!(config.materialized_views.len(), 1);
        assert_eq!(config.runtime.ingest_batch_size, Some(128));
        assert_eq!(config.runtime.mv_snapshot.max_pending_batches, Some(2048));
        assert_eq!(config.runtime.mv_snapshot.max_pending_rows, Some(500000));
        assert_eq!(config.runtime.mv_snapshot.max_delay_ms, Some(2000));
        assert_eq!(config.runtime.admin_port, Some(8082));
        assert_eq!(
            config.runtime.pgwire_addr.as_deref(),
            Some("127.0.0.1:6543")
        );
        assert_eq!(config.runtime.pgwire_enabled, Some(false));
        assert_eq!(config.runtime.pre_tick_commit_delay_ms, Some(10));
        assert_eq!(config.runtime.tail_channel_capacity, Some(512));
        assert_eq!(config.runtime.tail_max_catchup_versions, Some(64));
        assert_eq!(config.runtime.transient_segment_max_nodes, Some(48));
        assert_eq!(config.runtime.transient_segment_min_score, Some(0));
        assert_eq!(config.storage.await_durable, Some(true));
        assert_eq!(config.storage.data_dir.as_deref(), Some("/tmp/floe-data"));
        assert_eq!(
            config.storage.source_journal,
            Some(SourceJournalConfig::Auto)
        );
        assert_eq!(config.storage.slatedb_close_timeout_ms, Some(1000));
        assert_eq!(config.maintenance.paused, Some(true));
    }

    #[test]
    fn load_config_accepts_object_store_storage_section() {
        let input = r#"
            [storage]
            object_store_from_env = true
            object_store_env_file = "/tmp/object-store.env"
            slatedb_name = "floe-test"
        "#;
        let config: NodeConfig = toml::from_str(input).expect("parse toml");
        validate_node_config(&config).expect("valid object-store config");
        assert!(config.storage.object_store_from_env);
        assert_eq!(
            config.storage.object_store_env_file.as_deref(),
            Some("/tmp/object-store.env")
        );
        assert_eq!(config.storage.slatedb_name.as_deref(), Some("floe-test"));
    }

    #[test]
    fn load_config_accepts_replication_buffer_cleanup_section() {
        let input = r#"
            [replication.buffer_cleanup]
            delivered_retention_ms = 1000
            orphan_retention_ms = 5000
            cleanup_interval_ms = 250
        "#;

        let config = parse_toml_config(input).expect("parse toml");

        assert_eq!(
            config.replication.buffer_cleanup.delivered_retention_ms,
            1000
        );
        assert_eq!(config.replication.buffer_cleanup.orphan_retention_ms, 5000);
        assert_eq!(config.replication.buffer_cleanup.cleanup_interval_ms, 250);
    }

    #[test]
    fn load_config_accepts_replication_buffer_limits_section() {
        let input = r#"
            [replication.buffer_limits]
            max_pending_bytes = 123
            max_pending_records = 456
            max_pending_transactions = 7
            max_pending_age_ms = 89
        "#;

        let config = parse_toml_config(input).expect("parse toml");

        assert_eq!(config.replication.buffer_limits.max_pending_bytes, 123);
        assert_eq!(config.replication.buffer_limits.max_pending_records, 456);
        assert_eq!(config.replication.buffer_limits.max_pending_transactions, 7);
        assert_eq!(config.replication.buffer_limits.max_pending_age_ms, 89);
    }

    #[test]
    fn load_config_accepts_replication_kafka_section() {
        let input = r#"
            [replication.kafka]
            message_max_bytes = 2000000
            acks = "all"
            enable_idempotence = true
            batch_size = 300000
            batch_num_messages = 400000
            linger_ms = 2
            queue_max_messages = 500000
            queue_max_kbytes = 600000
            message_send_max_retries = 3
        "#;

        let config = parse_toml_config(input).expect("parse toml");

        assert_eq!(config.replication.kafka.message_max_bytes, 2_000_000);
        assert_eq!(config.replication.kafka.acks, "all");
        assert!(config.replication.kafka.enable_idempotence);
        assert_eq!(config.replication.kafka.batch_size, 300_000);
        assert_eq!(config.replication.kafka.batch_num_messages, 400_000);
        assert_eq!(config.replication.kafka.linger_ms, 2);
        assert_eq!(config.replication.kafka.queue_max_messages, 500_000);
        assert_eq!(config.replication.kafka.queue_max_kbytes, 600_000);
        assert_eq!(config.replication.kafka.message_send_max_retries, 3);
    }

    #[test]
    fn load_config_accepts_replication_encoding_section() {
        let input = r#"
            [replication]
            perf_log = true

            [replication.encoding]
            arrow_ipc_rows_per_record = 2048
            snapshot_batches_per_chunk = 4
            arrow_ipc_compression = "lz4_frame"
            kafka_metadata_headers = true
        "#;

        let config = parse_toml_config(input).expect("parse toml");

        assert!(config.replication.perf_log);
        assert_eq!(config.replication.encoding.arrow_ipc_rows_per_record, 2048);
        assert_eq!(config.replication.encoding.snapshot_batches_per_chunk, 4);
        assert_eq!(
            config.replication.encoding.arrow_ipc_compression,
            Some(ReplicationArrowIpcCompressionConfig::Lz4Frame)
        );
        assert!(config.replication.encoding.kafka_metadata_headers);
    }

    #[test]
    fn load_config_accepts_postgres_cdc_snapshot_section() {
        let input = r#"
            [postgres_cdc.snapshot]
            rows_per_batch = 8192
            max_workers = 4
            intra_table_chunks = 8
            adaptive_concurrency = false
            min_workers = 2
            wal_buffer_high_watermark_percent = 80
            wal_buffer_low_watermark_percent = 20
            slow_scan_ms = 12000
            controller_interval_ms = 250
            perf_log = true
        "#;

        let config = parse_toml_config(input).expect("parse toml");
        let snapshot = config.postgres_cdc.snapshot;

        assert_eq!(snapshot.rows_per_batch, 8192);
        assert_eq!(snapshot.max_workers, 4);
        assert_eq!(snapshot.intra_table_chunks, 8);
        assert!(!snapshot.adaptive_concurrency);
        assert_eq!(snapshot.min_workers, 2);
        assert_eq!(snapshot.wal_buffer_high_watermark_percent, 80);
        assert_eq!(snapshot.wal_buffer_low_watermark_percent, 20);
        assert_eq!(snapshot.slow_scan_ms, 12_000);
        assert_eq!(snapshot.controller_interval_ms, 250);
        assert!(snapshot.perf_log);
    }

    #[test]
    fn replication_arrow_ipc_compression_config_parses_alias_values() {
        assert_eq!(
            ReplicationArrowIpcCompressionConfig::parse("lz4"),
            Some(ReplicationArrowIpcCompressionConfig::Lz4Frame)
        );
        assert_eq!(
            ReplicationArrowIpcCompressionConfig::parse("lz4-frame"),
            Some(ReplicationArrowIpcCompressionConfig::Lz4Frame)
        );
        assert_eq!(ReplicationArrowIpcCompressionConfig::parse("none"), None);
        assert_eq!(ReplicationArrowIpcCompressionConfig::parse("bogus"), None);
    }

    #[test]
    fn validation_rejects_duplicate_materialized_view_names() {
        let config = NodeConfig {
            connectors: vec![ConnectorConfig::Generator {
                name: None,
                events_per_second: Some(1.0),
                max_events: None,
            }],
            materialized_views: vec![
                MaterializedViewConfig {
                    name: "mv_dup".to_string(),
                    query: "SELECT 1".to_string(),
                    if_not_exists: false,
                },
                MaterializedViewConfig {
                    name: "mv_dup".to_string(),
                    query: "SELECT 2".to_string(),
                    if_not_exists: false,
                },
            ],
            ..NodeConfig::default()
        };
        let err = validate_node_config(&config).expect_err("validation should fail");
        assert!(
            err.to_string()
                .contains("duplicate materialized view name 'mv_dup'")
        );
    }

    #[test]
    fn validation_rejects_non_positive_mv_snapshot_max_pending_batches() {
        let config = NodeConfig {
            runtime: RuntimeConfig {
                mv_snapshot: MvSnapshotConfig {
                    max_pending_batches: Some(0),
                    ..MvSnapshotConfig::default()
                },
                ..RuntimeConfig::default()
            },
            ..NodeConfig::default()
        };
        let err = validate_node_config(&config).expect_err("validation should fail");
        assert!(
            err.to_string()
                .contains("runtime.mv_snapshot.max_pending_batches must be greater than 0")
        );
    }

    #[test]
    fn materialized_view_definitions_from_config_maps_fields() {
        let config_views = vec![MaterializedViewConfig {
            name: "mv_cfg".to_string(),
            query: "SELECT * FROM nexmark_bid".to_string(),
            if_not_exists: true,
        }];
        let definitions = materialized_view_definitions_from_config(&config_views);
        assert_eq!(definitions.len(), 1);
        assert_eq!(definitions[0].name(), "mv_cfg");
        assert_eq!(definitions[0].query(), "SELECT * FROM nexmark_bid");
        assert!(definitions[0].if_not_exists());
    }
}
