use super::*;

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

pub fn connector_spec_from_sql(
    definition: &CreateSourceDefinition,
) -> Result<Option<ConnectorSpec>> {
    let schema_default_source = (!definition.columns().is_empty()).then(|| definition.name());
    let default_source = |configured: Option<&str>| -> Result<Option<String>> {
        if let (Some(configured), Some(schema_source)) = (configured, schema_default_source)
            && configured != schema_source
        {
            bail!(
                "CREATE SOURCE '{}' declares an inline schema, so default_source must be omitted or match the source name",
                definition.name()
            );
        }
        Ok(configured
            .or(schema_default_source)
            .map(ToString::to_string))
    };
    let config = match definition.connector() {
        SourceConnector::Kafka(options) => ConnectorConfig::Kafka {
            name: Some(definition.name().to_string()),
            brokers: options.brokers().to_string(),
            topics: options.topics().to_vec(),
            group_id: options.group_id().map(ToString::to_string),
            default_source: default_source(options.default_source())?,
            poll_ms: options.poll_ms(),
            max_messages_per_tick: options.max_messages_per_tick(),
            format: options.format().map(ToString::to_string),
        },
        SourceConnector::File(options) => ConnectorConfig::File {
            name: Some(definition.name().to_string()),
            path: options.path().to_string(),
            default_source: default_source(options.default_source())?,
        },
        SourceConnector::Http(options) => ConnectorConfig::Http {
            name: Some(definition.name().to_string()),
            host: options.host().map(ToString::to_string),
            port: options.port(),
            default_source: default_source(options.default_source())?,
        },
        SourceConnector::Generator(options) => ConnectorConfig::Generator {
            name: Some(definition.name().to_string()),
            events_per_second: options.events_per_second(),
            max_events: options.max_events(),
        },
        SourceConnector::ObjectStore(options) => ConnectorConfig::ObjectStore {
            name: Some(definition.name().to_string()),
            url: options.url().to_string(),
            default_source: default_source(options.default_source())?,
        },
        SourceConnector::PostgresCdc(_) => return Ok(None),
    };
    validation::validate_connector(&config, 0).context("validate SQL CREATE SOURCE connector")?;
    Ok(Some(ConnectorSpec {
        name: definition.name().to_string(),
        config,
    }))
}

pub fn sink_spec_from_sql(definition: &SinkDefinition) -> Result<SinkSpec> {
    let options = definition.options();
    let config = match definition.connector() {
        SinkConnector::Kafka {
            brokers,
            topic,
            format,
            key_columns,
        } => SinkConfig::Kafka {
            name: Some(definition.name().to_string()),
            brokers: brokers.clone(),
            topic: topic.clone(),
            mv: definition.mv_name().to_string(),
            format: format.clone(),
            key_columns: (!key_columns.is_empty()).then(|| key_columns.clone()),
            with_snapshot: Some(definition.with_snapshot()),
            as_of: definition.as_of(),
            batch_rows: options.batch_rows(),
            batch_bytes: options.batch_bytes(),
            queue_capacity: options.queue_capacity(),
            retry_max_attempts: options.retry_max_attempts(),
            retry_base_ms: options.retry_base_ms(),
            retry_max_backoff_ms: options.retry_max_backoff_ms(),
            transactional_id: options.transactional_id().map(ToString::to_string),
            checkpoint_topic: options.checkpoint_topic().map(ToString::to_string),
            checkpoint_partition: options.checkpoint_partition(),
        },
        SinkConnector::File { path, append } => SinkConfig::File {
            name: Some(definition.name().to_string()),
            path: path.clone(),
            mv: definition.mv_name().to_string(),
            with_snapshot: Some(definition.with_snapshot()),
            as_of: definition.as_of(),
            append: *append,
            batch_rows: options.batch_rows(),
            batch_bytes: options.batch_bytes(),
            queue_capacity: options.queue_capacity(),
        },
        SinkConnector::Http { url, batch_size } => SinkConfig::Http {
            name: Some(definition.name().to_string()),
            url: url.clone(),
            mv: definition.mv_name().to_string(),
            with_snapshot: Some(definition.with_snapshot()),
            as_of: definition.as_of(),
            batch_size: *batch_size,
            batch_rows: options.batch_rows(),
            batch_bytes: options.batch_bytes(),
            queue_capacity: options.queue_capacity(),
            retry_max_attempts: options.retry_max_attempts(),
            retry_base_ms: options.retry_base_ms(),
            retry_max_backoff_ms: options.retry_max_backoff_ms(),
        },
        SinkConnector::Postgres {
            connection,
            table,
            mode,
            primary_key,
        } => SinkConfig::Postgres {
            name: Some(definition.name().to_string()),
            connection: connection.clone(),
            table: table.clone(),
            mv: definition.mv_name().to_string(),
            mode: mode.clone(),
            primary_key: (!primary_key.is_empty()).then(|| primary_key.clone()),
            with_snapshot: Some(definition.with_snapshot()),
            as_of: definition.as_of(),
            retry_max_attempts: options.retry_max_attempts(),
            retry_base_ms: options.retry_base_ms(),
            retry_max_backoff_ms: options.retry_max_backoff_ms(),
        },
    };
    validation::validate_sink(&config, 0).context("validate SQL CREATE SINK")?;
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
            ConnectorConfig::PostgresCdc { include_tables, .. } => include_tables
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|table| {
                    if registry.contains(&table) {
                        table
                    } else {
                        table
                            .rsplit_once('.')
                            .map(|(_, name)| name.to_string())
                            .unwrap_or(table)
                    }
                })
                .collect(),
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
                format,
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
                if let Some(format) = format {
                    props.push(("format".to_string(), format.clone()));
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
                publication,
                include_tables,
                include_schema_in_source,
                schema_evolution_policy,
                auto_create_slot,
                auto_create_publication,
                ..
            } => {
                let mut props = vec![("slot".to_string(), slot.clone())];
                if let Some(publication) = publication {
                    props.push(("publication".to_string(), publication.clone()));
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
                if let Some(schema_evolution_policy) = schema_evolution_policy {
                    props.push((
                        "schema_evolution_policy".to_string(),
                        schema_evolution_policy.as_str().to_string(),
                    ));
                }
                if let Some(auto_create_slot) = auto_create_slot {
                    props.push(("auto_create_slot".to_string(), auto_create_slot.to_string()));
                }
                if let Some(auto_create_publication) = auto_create_publication {
                    props.push((
                        "auto_create_publication".to_string(),
                        auto_create_publication.to_string(),
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
            SinkConfig::Postgres { .. } => "postgres",
        }
    }

    fn explicit_name(&self) -> Option<&str> {
        match self {
            SinkConfig::Kafka { name, .. }
            | SinkConfig::File { name, .. }
            | SinkConfig::Http { name, .. }
            | SinkConfig::Postgres { name, .. } => name.as_deref(),
        }
    }
}
