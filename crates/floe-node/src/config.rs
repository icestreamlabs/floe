use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result, bail};
use reqwest::Url;
use serde::Deserialize;

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
        poll_ms: Option<u64>,
        #[serde(default)]
        max_changes: Option<usize>,
        #[serde(default)]
        default_schema: Option<String>,
        #[serde(default)]
        include_tables: Option<Vec<String>>,
        #[serde(default)]
        include_schema_in_source: Option<bool>,
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

pub fn load_config(path: impl AsRef<Path>) -> Result<NodeConfig> {
    let path = path.as_ref();
    let contents =
        std::fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    let config = match ext.as_str() {
        "toml" => toml::from_str(&contents).context("parse toml config"),
        "yaml" | "yml" => serde_yaml::from_str(&contents).context("parse yaml config"),
        "json" => serde_json::from_str(&contents).context("parse json config"),
        _ => parse_config_fallback(&contents),
    }?;
    validate_node_config(&config).context("validate node config")?;
    Ok(config)
}

fn parse_config_fallback(contents: &str) -> Result<NodeConfig> {
    if let Ok(config) = serde_json::from_str(contents) {
        return Ok(config);
    }
    if let Ok(config) = serde_yaml::from_str(contents) {
        return Ok(config);
    }
    toml::from_str(contents).context("parse config (tried json, yaml, toml)")
}

fn validate_node_config(config: &NodeConfig) -> Result<()> {
    let mut seen_mv_names = HashSet::new();
    for (index, mv) in config.materialized_views.iter().enumerate() {
        ensure_non_empty(&mv.name, &format!("materialized_views[{index}].name"))?;
        ensure_non_empty(&mv.query, &format!("materialized_views[{index}].query"))?;
        if !seen_mv_names.insert(mv.name.clone()) {
            bail!(
                "duplicate materialized view name '{}' in materialized_views[{index}]",
                mv.name
            );
        }
    }
    for (index, connector) in config.connectors.iter().enumerate() {
        validate_connector(connector, index)?;
    }
    for (index, sink) in config.sinks.iter().enumerate() {
        validate_sink(sink, index)?;
    }
    validate_runtime_config(&config.runtime)?;
    validate_storage_config(&config.storage)?;
    validate_maintenance_config(&config.maintenance)?;
    Ok(())
}

fn validate_runtime_config(runtime: &RuntimeConfig) -> Result<()> {
    if let Some(rate) = runtime.events_per_second
        && rate <= 0.0
    {
        bail!("runtime.events_per_second must be greater than 0");
    }
    if let Some(max_events) = runtime.max_events
        && max_events == 0
    {
        bail!("runtime.max_events must be greater than 0");
    }
    ensure_optional_positive_usize(
        runtime.ingest_queue_capacity,
        "runtime.ingest_queue_capacity",
    )?;
    ensure_optional_positive_usize(runtime.ingest_batch_size, "runtime.ingest_batch_size")?;
    ensure_optional_positive_usize(
        runtime.ingest_batch_per_source,
        "runtime.ingest_batch_per_source",
    )?;
    ensure_optional_positive_usize(
        runtime.ingest_batch_per_connector,
        "runtime.ingest_batch_per_connector",
    )?;
    ensure_optional_non_empty(runtime.http_host.as_deref(), "runtime.http_host")?;
    ensure_optional_non_empty(runtime.kafka_group_id.as_deref(), "runtime.kafka_group_id")?;
    ensure_optional_positive_u64(runtime.kafka_poll_ms, "runtime.kafka_poll_ms")?;
    ensure_optional_positive_usize(runtime.kafka_max_messages, "runtime.kafka_max_messages")?;
    Ok(())
}

fn validate_storage_config(storage: &StorageConfig) -> Result<()> {
    ensure_optional_non_empty(storage.slatedb_config.as_deref(), "storage.slatedb_config")?;
    ensure_optional_non_empty(
        storage.slatedb_env_prefix.as_deref(),
        "storage.slatedb_env_prefix",
    )?;
    ensure_optional_positive_usize(
        storage.zset_compaction_max_chain_len,
        "storage.zset_compaction_max_chain_len",
    )?;
    ensure_optional_positive_usize(
        storage.zset_compaction_max_segments,
        "storage.zset_compaction_max_segments",
    )?;
    ensure_optional_positive_usize(
        storage.zset_compaction_max_concurrent_jobs,
        "storage.zset_compaction_max_concurrent_jobs",
    )?;
    ensure_optional_positive_u64(
        storage.zset_gc_grace_period_ms,
        "storage.zset_gc_grace_period_ms",
    )?;
    Ok(())
}

fn validate_maintenance_config(maintenance: &MaintenanceConfig) -> Result<()> {
    for (index, namespace) in maintenance.inspect_namespace.iter().enumerate() {
        ensure_non_empty(
            namespace,
            &format!("maintenance.inspect_namespace[{index}]"),
        )?;
    }
    for (index, namespace) in maintenance.compact_namespace.iter().enumerate() {
        ensure_non_empty(
            namespace,
            &format!("maintenance.compact_namespace[{index}]"),
        )?;
    }
    for (index, namespace) in maintenance.gc_namespace.iter().enumerate() {
        ensure_non_empty(namespace, &format!("maintenance.gc_namespace[{index}]"))?;
    }
    Ok(())
}

fn validate_connector(connector: &ConnectorConfig, index: usize) -> Result<()> {
    match connector {
        ConnectorConfig::Kafka {
            name,
            brokers,
            topics,
            group_id,
            default_source,
            poll_ms: _,
            max_messages_per_tick,
        } => {
            ensure_non_empty(brokers, &format!("connectors[{index}].brokers"))?;
            if topics.is_empty() {
                bail!("connectors[{index}].topics must not be empty");
            }
            for (topic_index, topic) in topics.iter().enumerate() {
                ensure_non_empty(topic, &format!("connectors[{index}].topics[{topic_index}]"))?;
            }
            ensure_optional_non_empty(name.as_deref(), &format!("connectors[{index}].name"))?;
            ensure_optional_non_empty(
                group_id.as_deref(),
                &format!("connectors[{index}].group_id"),
            )?;
            ensure_optional_non_empty(
                default_source.as_deref(),
                &format!("connectors[{index}].default_source"),
            )?;
            ensure_optional_positive_usize(
                *max_messages_per_tick,
                &format!("connectors[{index}].max_messages_per_tick"),
            )?;
        }
        ConnectorConfig::File {
            name,
            path,
            default_source,
        } => {
            ensure_non_empty(path, &format!("connectors[{index}].path"))?;
            ensure_optional_non_empty(name.as_deref(), &format!("connectors[{index}].name"))?;
            ensure_optional_non_empty(
                default_source.as_deref(),
                &format!("connectors[{index}].default_source"),
            )?;
        }
        ConnectorConfig::Http {
            name,
            host,
            port,
            default_source,
        } => {
            ensure_optional_non_empty(name.as_deref(), &format!("connectors[{index}].name"))?;
            ensure_optional_non_empty(host.as_deref(), &format!("connectors[{index}].host"))?;
            if *port == 0 {
                bail!("connectors[{index}].port must be greater than 0");
            }
            ensure_optional_non_empty(
                default_source.as_deref(),
                &format!("connectors[{index}].default_source"),
            )?;
        }
        ConnectorConfig::Generator {
            name,
            events_per_second,
            max_events,
        } => {
            ensure_optional_non_empty(name.as_deref(), &format!("connectors[{index}].name"))?;
            if let Some(rate) = events_per_second
                && *rate <= 0.0
            {
                bail!("connectors[{index}].events_per_second must be greater than 0");
            }
            if let Some(limit) = max_events
                && *limit == 0
            {
                bail!("connectors[{index}].max_events must be greater than 0");
            }
        }
        ConnectorConfig::ObjectStore {
            name,
            url,
            default_source,
        } => {
            ensure_optional_non_empty(name.as_deref(), &format!("connectors[{index}].name"))?;
            ensure_non_empty(url, &format!("connectors[{index}].url"))?;
            Url::parse(url).with_context(|| {
                format!("connectors[{index}].url must be a valid URL (found '{url}')")
            })?;
            ensure_optional_non_empty(
                default_source.as_deref(),
                &format!("connectors[{index}].default_source"),
            )?;
        }
        ConnectorConfig::PostgresCdc {
            name,
            connection,
            slot,
            poll_ms: _,
            max_changes,
            default_schema,
            include_tables,
            include_schema_in_source: _,
        } => {
            ensure_optional_non_empty(name.as_deref(), &format!("connectors[{index}].name"))?;
            ensure_non_empty(connection, &format!("connectors[{index}].connection"))?;
            Url::parse(connection).with_context(|| {
                format!(
                    "connectors[{index}].connection must be a valid postgres URL (found '{connection}')"
                )
            })?;
            ensure_non_empty(slot, &format!("connectors[{index}].slot"))?;
            ensure_optional_positive_usize(
                *max_changes,
                &format!("connectors[{index}].max_changes"),
            )?;
            ensure_optional_non_empty(
                default_schema.as_deref(),
                &format!("connectors[{index}].default_schema"),
            )?;
            if let Some(tables) = include_tables {
                if tables.is_empty() {
                    bail!("connectors[{index}].include_tables must not be empty");
                }
                for (table_index, table) in tables.iter().enumerate() {
                    ensure_non_empty(
                        table,
                        &format!("connectors[{index}].include_tables[{table_index}]"),
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn validate_sink(sink: &SinkConfig, index: usize) -> Result<()> {
    match sink {
        SinkConfig::Kafka {
            name,
            brokers,
            topic,
            mv,
            with_snapshot: _,
            as_of: _,
            batch_rows,
            batch_bytes,
            queue_capacity,
            retry_max_attempts,
            retry_base_ms,
            retry_max_backoff_ms,
        } => {
            ensure_optional_non_empty(name.as_deref(), &format!("sinks[{index}].name"))?;
            ensure_non_empty(brokers, &format!("sinks[{index}].brokers"))?;
            ensure_non_empty(topic, &format!("sinks[{index}].topic"))?;
            ensure_non_empty(mv, &format!("sinks[{index}].mv"))?;
            ensure_optional_positive_usize(*batch_rows, &format!("sinks[{index}].batch_rows"))?;
            ensure_optional_positive_usize(*batch_bytes, &format!("sinks[{index}].batch_bytes"))?;
            ensure_optional_positive_usize(
                *queue_capacity,
                &format!("sinks[{index}].queue_capacity"),
            )?;
            ensure_optional_positive_usize(
                *retry_max_attempts,
                &format!("sinks[{index}].retry_max_attempts"),
            )?;
            ensure_optional_positive_u64(*retry_base_ms, &format!("sinks[{index}].retry_base_ms"))?;
            ensure_optional_positive_u64(
                *retry_max_backoff_ms,
                &format!("sinks[{index}].retry_max_backoff_ms"),
            )?;
        }
        SinkConfig::File {
            name,
            path,
            mv,
            with_snapshot: _,
            as_of: _,
            append: _,
            batch_rows,
            batch_bytes,
            queue_capacity,
        } => {
            ensure_optional_non_empty(name.as_deref(), &format!("sinks[{index}].name"))?;
            ensure_non_empty(path, &format!("sinks[{index}].path"))?;
            ensure_non_empty(mv, &format!("sinks[{index}].mv"))?;
            ensure_optional_positive_usize(*batch_rows, &format!("sinks[{index}].batch_rows"))?;
            ensure_optional_positive_usize(*batch_bytes, &format!("sinks[{index}].batch_bytes"))?;
            ensure_optional_positive_usize(
                *queue_capacity,
                &format!("sinks[{index}].queue_capacity"),
            )?;
        }
        SinkConfig::Http {
            name,
            url,
            mv,
            with_snapshot: _,
            as_of: _,
            batch_size,
            batch_rows,
            batch_bytes,
            queue_capacity,
            retry_max_attempts,
            retry_base_ms,
            retry_max_backoff_ms,
        } => {
            ensure_optional_non_empty(name.as_deref(), &format!("sinks[{index}].name"))?;
            ensure_non_empty(url, &format!("sinks[{index}].url"))?;
            Url::parse(url).with_context(|| {
                format!("sinks[{index}].url must be a valid URL (found '{url}')")
            })?;
            ensure_non_empty(mv, &format!("sinks[{index}].mv"))?;
            ensure_optional_positive_usize(*batch_size, &format!("sinks[{index}].batch_size"))?;
            ensure_optional_positive_usize(*batch_rows, &format!("sinks[{index}].batch_rows"))?;
            ensure_optional_positive_usize(*batch_bytes, &format!("sinks[{index}].batch_bytes"))?;
            ensure_optional_positive_usize(
                *queue_capacity,
                &format!("sinks[{index}].queue_capacity"),
            )?;
            ensure_optional_positive_usize(
                *retry_max_attempts,
                &format!("sinks[{index}].retry_max_attempts"),
            )?;
            ensure_optional_positive_u64(*retry_base_ms, &format!("sinks[{index}].retry_base_ms"))?;
            ensure_optional_positive_u64(
                *retry_max_backoff_ms,
                &format!("sinks[{index}].retry_max_backoff_ms"),
            )?;
        }
    }
    Ok(())
}

fn ensure_non_empty(value: &str, field_path: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field_path} must not be empty");
    }
    Ok(())
}

fn ensure_optional_non_empty(value: Option<&str>, field_path: &str) -> Result<()> {
    if let Some(value) = value {
        ensure_non_empty(value, field_path)?;
    }
    Ok(())
}

fn ensure_optional_positive_usize(value: Option<usize>, field_path: &str) -> Result<()> {
    if let Some(value) = value
        && value == 0
    {
        bail!("{field_path} must be greater than 0");
    }
    Ok(())
}

fn ensure_optional_positive_u64(value: Option<u64>, field_path: &str) -> Result<()> {
    if let Some(value) = value
        && value == 0
    {
        bail!("{field_path} must be greater than 0");
    }
    Ok(())
}

pub fn normalize_connectors(connectors: Vec<ConnectorConfig>) -> Result<Vec<ConnectorSpec>> {
    let mut counts: HashMap<&'static str, usize> = HashMap::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut specs = Vec::with_capacity(connectors.len());

    for connector in connectors {
        let base = connector.type_name();
        let name = connector
            .explicit_name()
            .map(ToString::to_string)
            .unwrap_or_else(|| {
                let entry = counts.entry(base).or_insert(0);
                *entry += 1;
                if *entry == 1 {
                    base.to_string()
                } else {
                    format!("{base}_{}", entry)
                }
            });
        if !seen.insert(name.clone()) {
            bail!("duplicate connector name '{name}'");
        }
        specs.push(ConnectorSpec {
            name,
            config: connector,
        });
    }

    Ok(specs)
}

pub fn normalize_sinks(sinks: Vec<SinkConfig>) -> Result<Vec<SinkSpec>> {
    let mut counts: HashMap<&'static str, usize> = HashMap::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut specs = Vec::with_capacity(sinks.len());

    for sink in sinks {
        let base = sink.type_name();
        let name = sink
            .explicit_name()
            .map(ToString::to_string)
            .unwrap_or_else(|| {
                let entry = counts.entry(base).or_insert(0);
                *entry += 1;
                if *entry == 1 {
                    base.to_string()
                } else {
                    format!("{base}_{}", entry)
                }
            });
        if !seen.insert(name.clone()) {
            bail!("duplicate sink name '{name}'");
        }
        specs.push(SinkSpec { name, config: sink });
    }

    Ok(specs)
}

pub fn sink_spec_from_sql(definition: &SinkDefinition) -> Result<SinkSpec> {
    let config = match definition.connector() {
        SinkConnector::Kafka { brokers, topic } => SinkConfig::Kafka {
            name: Some(definition.name().to_string()),
            brokers: brokers.clone(),
            topic: topic.clone(),
            mv: definition.mv_name().to_string(),
            with_snapshot: Some(definition.with_snapshot()),
            as_of: definition.as_of(),
            batch_rows: None,
            batch_bytes: None,
            queue_capacity: None,
            retry_max_attempts: None,
            retry_base_ms: None,
            retry_max_backoff_ms: None,
        },
        SinkConnector::File { path, append } => SinkConfig::File {
            name: Some(definition.name().to_string()),
            path: path.clone(),
            mv: definition.mv_name().to_string(),
            with_snapshot: Some(definition.with_snapshot()),
            as_of: definition.as_of(),
            append: *append,
            batch_rows: None,
            batch_bytes: None,
            queue_capacity: None,
        },
        SinkConnector::Http { url, batch_size } => SinkConfig::Http {
            name: Some(definition.name().to_string()),
            url: url.clone(),
            mv: definition.mv_name().to_string(),
            with_snapshot: Some(definition.with_snapshot()),
            as_of: definition.as_of(),
            batch_size: *batch_size,
            batch_rows: None,
            batch_bytes: None,
            queue_capacity: None,
            retry_max_attempts: None,
            retry_base_ms: None,
            retry_max_backoff_ms: None,
        },
    };
    Ok(SinkSpec {
        name: definition.name().to_string(),
        config,
    })
}

pub fn materialized_view_definitions_from_config(
    views: &[MaterializedViewConfig],
) -> Vec<MaterializedViewDefinition> {
    views
        .iter()
        .map(MaterializedViewConfig::to_definition)
        .collect()
}

pub fn apply_connector_properties(registry: &mut SourceRegistry, connectors: &[ConnectorSpec]) {
    for connector in connectors {
        let sources = connector.sources(registry);
        if sources.is_empty() {
            tracing::debug!(
                connector = %connector.name,
                connector_type = connector.config.type_name(),
                "connector has no mapped sources for properties"
            );
            continue;
        }
        let props = connector.property_pairs();
        for source in sources {
            let Some(definition) = registry.get(&source).cloned() else {
                tracing::warn!(
                    connector = %connector.name,
                    source = %source,
                    "connector config references unknown source"
                );
                continue;
            };
            let mut updated = definition.clone();
            updated.set_property(
                format!("connector.{}.type", connector.name),
                connector.config.type_name(),
            );
            for (key, value) in &props {
                updated.set_property(
                    format!("connector.{}.{}", connector.name, key),
                    value.clone(),
                );
            }
            registry.register(updated);
        }
    }
}

impl ConnectorConfig {
    fn type_name(&self) -> &'static str {
        match self {
            ConnectorConfig::Kafka { .. } => "kafka",
            ConnectorConfig::File { .. } => "file",
            ConnectorConfig::Http { .. } => "http",
            ConnectorConfig::Generator { .. } => "generator",
            ConnectorConfig::ObjectStore { .. } => "object_store",
            ConnectorConfig::PostgresCdc { .. } => "postgres_cdc",
        }
    }

    fn explicit_name(&self) -> Option<&str> {
        match self {
            ConnectorConfig::Kafka { name, .. }
            | ConnectorConfig::File { name, .. }
            | ConnectorConfig::Http { name, .. }
            | ConnectorConfig::Generator { name, .. }
            | ConnectorConfig::ObjectStore { name, .. }
            | ConnectorConfig::PostgresCdc { name, .. } => name.as_deref(),
        }
    }
}

impl ConnectorSpec {
    fn sources(&self, registry: &SourceRegistry) -> Vec<String> {
        match &self.config {
            ConnectorConfig::Generator { .. } => vec![
                PERSON_SOURCE_NAME.to_string(),
                AUCTION_SOURCE_NAME.to_string(),
                BID_SOURCE_NAME.to_string(),
            ],
            ConnectorConfig::Kafka {
                default_source,
                topics,
                ..
            } => {
                if let Some(source) = default_source {
                    return vec![source.clone()];
                }
                topics
                    .iter()
                    .filter(|topic| registry.contains(topic.as_str()))
                    .cloned()
                    .collect()
            }
            ConnectorConfig::File { default_source, .. }
            | ConnectorConfig::Http { default_source, .. }
            | ConnectorConfig::ObjectStore { default_source, .. } => {
                default_source.clone().into_iter().collect()
            }
            ConnectorConfig::PostgresCdc { include_tables, .. } => {
                include_tables.clone().unwrap_or_default()
            }
        }
    }

    fn property_pairs(&self) -> Vec<(String, String)> {
        match &self.config {
            ConnectorConfig::Kafka {
                brokers,
                topics,
                group_id,
                default_source,
                poll_ms,
                max_messages_per_tick,
                ..
            } => {
                let mut props = Vec::new();
                props.push(("brokers".to_string(), brokers.clone()));
                props.push(("topics".to_string(), topics.join(",")));
                if let Some(group_id) = group_id {
                    props.push(("group_id".to_string(), group_id.clone()));
                }
                if let Some(default_source) = default_source {
                    props.push(("default_source".to_string(), default_source.clone()));
                }
                if let Some(poll_ms) = poll_ms {
                    props.push(("poll_ms".to_string(), poll_ms.to_string()));
                }
                if let Some(max_messages) = max_messages_per_tick {
                    props.push((
                        "max_messages_per_tick".to_string(),
                        max_messages.to_string(),
                    ));
                }
                props
            }
            ConnectorConfig::File {
                path,
                default_source,
                ..
            } => {
                let mut props = vec![("path".to_string(), path.clone())];
                if let Some(default_source) = default_source {
                    props.push(("default_source".to_string(), default_source.clone()));
                }
                props
            }
            ConnectorConfig::Http {
                host,
                port,
                default_source,
                ..
            } => {
                let mut props = vec![("port".to_string(), port.to_string())];
                if let Some(host) = host {
                    props.push(("host".to_string(), host.clone()));
                }
                if let Some(default_source) = default_source {
                    props.push(("default_source".to_string(), default_source.clone()));
                }
                props
            }
            ConnectorConfig::Generator {
                events_per_second,
                max_events,
                ..
            } => {
                let mut props = Vec::new();
                if let Some(events_per_second) = events_per_second {
                    props.push((
                        "events_per_second".to_string(),
                        events_per_second.to_string(),
                    ));
                }
                if let Some(max_events) = max_events {
                    props.push(("max_events".to_string(), max_events.to_string()));
                }
                props
            }
            ConnectorConfig::ObjectStore {
                url,
                default_source,
                ..
            } => {
                let mut props = vec![("url".to_string(), url.clone())];
                if let Some(default_source) = default_source {
                    props.push(("default_source".to_string(), default_source.clone()));
                }
                props
            }
            ConnectorConfig::PostgresCdc {
                slot,
                poll_ms,
                max_changes,
                default_schema,
                include_tables,
                include_schema_in_source,
                ..
            } => {
                let mut props = vec![("slot".to_string(), slot.clone())];
                if let Some(poll_ms) = poll_ms {
                    props.push(("poll_ms".to_string(), poll_ms.to_string()));
                }
                if let Some(max_changes) = max_changes {
                    props.push(("max_changes".to_string(), max_changes.to_string()));
                }
                if let Some(default_schema) = default_schema {
                    props.push(("default_schema".to_string(), default_schema.clone()));
                }
                if let Some(include_tables) = include_tables {
                    props.push(("include_tables".to_string(), include_tables.join(",")));
                }
                if let Some(include_schema_in_source) = include_schema_in_source {
                    props.push((
                        "include_schema_in_source".to_string(),
                        include_schema_in_source.to_string(),
                    ));
                }
                props
            }
        }
    }
}

impl SinkConfig {
    fn type_name(&self) -> &'static str {
        match self {
            SinkConfig::Kafka { .. } => "kafka",
            SinkConfig::File { .. } => "file",
            SinkConfig::Http { .. } => "http",
        }
    }

    fn explicit_name(&self) -> Option<&str> {
        match self {
            SinkConfig::Kafka { name, .. }
            | SinkConfig::File { name, .. }
            | SinkConfig::Http { name, .. } => name.as_deref(),
        }
    }
}

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
            },
            ConnectorConfig::Kafka {
                name: None,
                brokers: "localhost:9092".to_string(),
                topics: vec!["b".to_string()],
                group_id: None,
                default_source: None,
                poll_ms: None,
                max_messages_per_tick: None,
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

            [storage]
            await_durable = true
            zset_compaction_max_chain_len = 64

            [maintenance]
            paused = true
            inspect_namespace = ["mv::mv_cfg"]
        "#;
        let config: NodeConfig = toml::from_str(input).expect("parse toml");
        assert_eq!(config.materialized_views.len(), 1);
        assert_eq!(config.runtime.ingest_batch_size, Some(128));
        assert_eq!(config.storage.await_durable, Some(true));
        assert_eq!(config.maintenance.paused, Some(true));
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
