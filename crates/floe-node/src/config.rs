use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use floe_node_core::generator::{AUCTION_SOURCE_NAME, BID_SOURCE_NAME, PERSON_SOURCE_NAME};
use floe_node_core::source::SourceRegistry;

#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    #[serde(default)]
    pub connectors: Vec<ConnectorConfig>,
    #[serde(default)]
    pub sinks: Vec<SinkConfig>,
}

#[derive(Debug, Clone, Deserialize)]
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

    match ext.as_str() {
        "toml" => toml::from_str(&contents).context("parse toml config"),
        "yaml" | "yml" => serde_yaml::from_str(&contents).context("parse yaml config"),
        "json" => serde_json::from_str(&contents).context("parse json config"),
        _ => parse_config_fallback(&contents),
    }
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
}
