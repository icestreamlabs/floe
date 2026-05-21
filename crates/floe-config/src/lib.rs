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
    pub slatedb_config: Option<String>,
    #[serde(default)]
    pub slatedb_env_prefix: Option<String>,
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
}

impl ReplicationConfig {
    pub fn with_legacy_env_overrides(mut self) -> Self {
        if let Some(value) = env_u64("FLOE_REPLICATION_BUFFER_DELIVERED_RETENTION_MS") {
            self.buffer_cleanup.delivered_retention_ms = value;
        }
        if let Some(value) = env_u64("FLOE_REPLICATION_BUFFER_CLEANUP_INTERVAL_MS") {
            self.buffer_cleanup.cleanup_interval_ms = value;
        }
        if let Some(value) = env_usize("FLOE_REPLICATION_BUFFER_MAX_PENDING_BYTES") {
            self.buffer_limits.max_pending_bytes = value;
        }
        if let Some(value) = env_usize("FLOE_REPLICATION_BUFFER_MAX_PENDING_RECORDS") {
            self.buffer_limits.max_pending_records = value;
        }
        if let Some(value) = env_usize("FLOE_REPLICATION_BUFFER_MAX_PENDING_TRANSACTIONS")
            .or_else(|| env_usize("FLOE_REPLICATION_BUFFER_MAX_PENDING_OBJECTS"))
        {
            self.buffer_limits.max_pending_transactions = value;
        }
        if let Some(value) = env_u64("FLOE_REPLICATION_BUFFER_MAX_PENDING_AGE_MS") {
            self.buffer_limits.max_pending_age_ms = value;
        }
        if let Some(value) = env_positive_usize("FLOE_REPLICATION_KAFKA_MESSAGE_MAX_BYTES") {
            self.kafka.message_max_bytes = value;
        }
        if let Some(value) = env_nonempty_string("FLOE_REPLICATION_KAFKA_ACKS") {
            self.kafka.acks = value;
        }
        if let Some(value) = env_bool("FLOE_REPLICATION_KAFKA_ENABLE_IDEMPOTENCE") {
            self.kafka.enable_idempotence = value;
        }
        if let Some(value) = env_positive_usize("FLOE_REPLICATION_KAFKA_BATCH_SIZE") {
            self.kafka.batch_size = value;
        }
        if let Some(value) = env_positive_usize("FLOE_REPLICATION_KAFKA_BATCH_NUM_MESSAGES") {
            self.kafka.batch_num_messages = value;
        }
        if let Some(value) = env_usize("FLOE_REPLICATION_KAFKA_LINGER_MS") {
            self.kafka.linger_ms = value;
        }
        if let Some(value) = env_usize("FLOE_REPLICATION_KAFKA_QUEUE_MAX_MESSAGES") {
            self.kafka.queue_max_messages = value;
        }
        if let Some(value) = env_usize("FLOE_REPLICATION_KAFKA_QUEUE_MAX_KBYTES") {
            self.kafka.queue_max_kbytes = value;
        }
        if let Some(value) = env_usize("FLOE_REPLICATION_KAFKA_MESSAGE_SEND_MAX_RETRIES") {
            self.kafka.message_send_max_retries = value;
        }
        self
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReplicationBufferCleanupConfig {
    #[serde(default = "default_replication_buffer_delivered_retention_ms")]
    pub delivered_retention_ms: u64,
    #[serde(default = "default_replication_buffer_cleanup_interval_ms")]
    pub cleanup_interval_ms: u64,
}

impl ReplicationBufferCleanupConfig {
    pub const DEFAULT: Self = Self {
        delivered_retention_ms: DEFAULT_REPLICATION_BUFFER_DELIVERED_RETENTION_MS,
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

const DEFAULT_REPLICATION_BUFFER_DELIVERED_RETENTION_MS: u64 = 5_000;
const DEFAULT_REPLICATION_BUFFER_CLEANUP_INTERVAL_MS: u64 = 5_000;
const DEFAULT_REPLICATION_BUFFER_MAX_PENDING_BYTES: usize = 10 * 1024 * 1024 * 1024;
const DEFAULT_REPLICATION_KAFKA_MESSAGE_MAX_BYTES: usize = 10_485_760;
const DEFAULT_REPLICATION_KAFKA_BATCH_SIZE: usize = 1_000_000;
const DEFAULT_REPLICATION_KAFKA_BATCH_NUM_MESSAGES: usize = 1_000_000;
const DEFAULT_REPLICATION_KAFKA_LINGER_MS: usize = 1;
const DEFAULT_REPLICATION_KAFKA_QUEUE_MAX_MESSAGES: usize = 1_000_000;
const DEFAULT_REPLICATION_KAFKA_QUEUE_MAX_KBYTES: usize = 1_048_576;

fn default_replication_buffer_delivered_retention_ms() -> u64 {
    DEFAULT_REPLICATION_BUFFER_DELIVERED_RETENTION_MS
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

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
}

fn env_positive_usize(name: &str) -> Option<usize> {
    env_usize(name).filter(|value| *value > 0)
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
}

fn env_nonempty_string(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn env_bool(name: &str) -> Option<bool> {
    std::env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
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
        effectively_once: Option<bool>,
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

            [runtime.mv_snapshot]
            max_pending_batches = 2048
            max_pending_rows = 500000
            max_delay_ms = 2000

            [storage]
            await_durable = true
            source_journal = "auto"
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
        assert_eq!(config.storage.await_durable, Some(true));
        assert_eq!(
            config.storage.source_journal,
            Some(SourceJournalConfig::Auto)
        );
        assert_eq!(config.maintenance.paused, Some(true));
    }

    #[test]
    fn load_config_accepts_replication_buffer_cleanup_section() {
        let input = r#"
            [replication.buffer_cleanup]
            delivered_retention_ms = 1000
            cleanup_interval_ms = 250
        "#;

        let config = parse_toml_config(input).expect("parse toml");

        assert_eq!(
            config.replication.buffer_cleanup.delivered_retention_ms,
            1000
        );
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
