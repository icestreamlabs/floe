use super::super::*;

#[allow(clippy::too_many_arguments)]
pub(in crate::node_runtime) async fn postgres_cdc_runtime_plan(
    connector_name: &str,
    connection_string: &str,
    schema_evolution_policy: PostgresSchemaEvolutionPolicy,
    include_tables: Option<&[String]>,
    registry: &SourceRegistry,
    source_tables: &HashMap<String, SourceBackedTableDefinition>,
    replication_pipelines: &HashMap<String, CatalogReplicationPipelineDefinition>,
) -> anyhow::Result<Option<PostgresCdcRuntimePlan>> {
    let has_source_tables = source_tables
        .values()
        .any(|table| table.source_name() == connector_name);
    let has_replication_pipelines = replication_pipelines
        .values()
        .any(|pipeline| pipeline.source_name() == connector_name);
    let include_tables = include_tables.unwrap_or(&[]);
    if include_tables.is_empty() && !has_source_tables && !has_replication_pipelines {
        return Ok(None);
    };
    let database_name = postgres_database_name(connection_string, connector_name);

    let mut schemas = HashMap::new();
    let mut table_id_by_upstream = HashMap::<String, CdcTableId>::new();
    let pipeline_upstreams = replication_pipelines
        .values()
        .filter(|pipeline| pipeline.source_name() == connector_name)
        .map(|pipeline| pipeline.upstream_table().to_string())
        .collect::<HashSet<_>>();

    for binding in source_tables
        .values()
        .filter(|table| table.source_name() == connector_name)
    {
        let definition = registry.get(binding.table_name()).ok_or_else(|| {
            anyhow!(
                "source-backed table '{}' has no registered table definition",
                binding.table_name()
            )
        })?;
        if !source_definition_has_primary_key(definition) {
            return Err(anyhow!(
                "Postgres CDC source-backed table '{}' has no primary key",
                definition.name()
            ));
        }
        let schema = cdc_table_schema_from_source_definition(
            definition,
            upstream_table_ref_for_postgres_include_table(binding.upstream_table())?,
        )?;
        table_id_by_upstream.insert(
            binding.upstream_table().to_string(),
            schema.table_id().clone(),
        );
        schemas.insert(schema.table_id().clone(), schema);
    }

    for include_table in include_tables {
        if table_id_by_upstream.contains_key(include_table) {
            continue;
        }
        if pipeline_upstreams.contains(include_table) {
            tracing::debug!(
                connector = %connector_name,
                table = %include_table,
                "Postgres CDC table is included for a replication pipeline; skipping hidden CDC table materialization"
            );
            continue;
        }
        let source_name = source_name_for_postgres_include_table(include_table, registry);
        let Some(definition) = registry.get(&source_name) else {
            return Err(anyhow!(
                "Postgres CDC include table '{}' for connector '{}' is not bound to a Floe CDC table or replication pipeline; declare CREATE TABLE ... FROM {} TABLE '{}' or CREATE REPLICATION PIPELINE",
                include_table,
                connector_name,
                connector_name,
                include_table
            ));
        };
        if !source_definition_has_primary_key(definition) {
            return Err(anyhow!(
                "Postgres CDC include table '{}' for connector '{}' maps to source '{}' without a primary key; CDC tables must declare a primary key",
                include_table,
                connector_name,
                definition.name()
            ));
        }
        let schema = cdc_table_schema_from_source_definition(
            definition,
            upstream_table_ref_for_postgres_include_table(include_table)?,
        )?;
        table_id_by_upstream.insert(include_table.to_string(), schema.table_id().clone());
        schemas.insert(schema.table_id().clone(), schema);
    }

    let mut pipeline_plans = Vec::new();
    for pipeline in replication_pipelines
        .values()
        .filter(|pipeline| pipeline.source_name() == connector_name)
    {
        let table_id = if let Some(table_id) = table_id_by_upstream.get(pipeline.upstream_table()) {
            table_id.clone()
        } else if let Some((table_id, schema)) =
            replication_pipeline_schema_from_registry(pipeline.upstream_table(), registry)?
        {
            table_id_by_upstream.insert(pipeline.upstream_table().to_string(), table_id.clone());
            schemas.insert(table_id.clone(), schema);
            table_id
        } else {
            let table_id =
                replication_pipeline_table_id(connector_name, pipeline.upstream_table())?;
            let schema = super::super::postgres_snapshot::discover_postgres_cdc_table_schema(
                connection_string,
                table_id.clone(),
                upstream_table_ref_for_postgres_include_table(pipeline.upstream_table())?,
            )
            .await
            .with_context(|| {
                format!(
                    "discover schema for replication pipeline '{}' table '{}'",
                    pipeline.name(),
                    pipeline.upstream_table()
                )
            })?;
            table_id_by_upstream.insert(pipeline.upstream_table().to_string(), table_id.clone());
            schemas.insert(table_id.clone(), schema);
            table_id
        };
        let schema = schemas.get(&table_id).cloned().ok_or_else(|| {
            anyhow!(
                "replication pipeline '{}' has no CDC schema for table '{}'",
                pipeline.name(),
                pipeline.upstream_table()
            )
        })?;
        pipeline_plans.push(replication_pipeline_runtime_plan_from_catalog(
            pipeline,
            schema,
            connection_string.to_string(),
            database_name.clone(),
            schema_evolution_policy,
        )?);
    }

    if schemas.is_empty() {
        return Ok(None);
    }

    Ok(Some(PostgresCdcRuntimePlan {
        source_id: CdcSourceId::new(connector_name)?,
        schemas,
        schema_evolution_policy,
        replication_pipelines: pipeline_plans,
    }))
}

pub(super) fn postgres_schema_evolution_policy_from_catalog(
    policy: CatalogPostgresCdcSchemaEvolutionPolicy,
) -> PostgresSchemaEvolutionPolicy {
    match policy {
        CatalogPostgresCdcSchemaEvolutionPolicy::FailFast => {
            PostgresSchemaEvolutionPolicy::FailFast
        }
        CatalogPostgresCdcSchemaEvolutionPolicy::IgnoreCompatible => {
            PostgresSchemaEvolutionPolicy::IgnoreCompatible
        }
        CatalogPostgresCdcSchemaEvolutionPolicy::ApplyCompatibleAdditions => {
            PostgresSchemaEvolutionPolicy::ApplyCompatibleAdditions
        }
    }
}

fn replication_pipeline_schema_from_registry(
    upstream_table: &str,
    registry: &SourceRegistry,
) -> anyhow::Result<Option<(CdcTableId, CdcTableSchema)>> {
    let source_name = source_name_for_postgres_include_table(upstream_table, registry);
    let Some(definition) = registry.get(&source_name) else {
        return Ok(None);
    };
    if !source_definition_has_primary_key(definition) {
        tracing::warn!(
            source = %definition.name(),
            table = %upstream_table,
            "Postgres CDC replication pipeline source definition has no primary key; falling back to live schema discovery"
        );
        return Ok(None);
    }
    let schema = cdc_table_schema_from_source_definition(
        definition,
        upstream_table_ref_for_postgres_include_table(upstream_table)?,
    )?;
    Ok(Some((schema.table_id().clone(), schema)))
}

fn replication_pipeline_runtime_plan_from_catalog(
    pipeline: &CatalogReplicationPipelineDefinition,
    schema: CdcTableSchema,
    source_connection: String,
    database_name: String,
    schema_evolution_policy: PostgresSchemaEvolutionPolicy,
) -> anyhow::Result<ReplicationPipelineRuntimePlan> {
    let table_id = schema.table_id().clone();
    let target = match pipeline.target() {
        CatalogReplicationPipelineTarget::Kafka { brokers, topic } => {
            ReplicationPipelineRuntimeTarget::Kafka {
                brokers: brokers.clone(),
                topic: topic.clone(),
            }
        }
        CatalogReplicationPipelineTarget::Postgres { connection, table } => {
            ReplicationPipelineRuntimeTarget::Postgres {
                connection: connection.clone(),
                table: table.clone(),
            }
        }
    };
    Ok(ReplicationPipelineRuntimePlan {
        name: pipeline.name().to_string(),
        source_name: pipeline.source_name().to_string(),
        source_connection,
        database_name,
        upstream_table: pipeline.upstream_table().to_string(),
        table_id,
        schema,
        schema_evolution_policy,
        target,
        format: match pipeline.format() {
            CatalogReplicationPipelineFormat::FloeJson => {
                ReplicationPipelineRuntimeFormat::FloeJson
            }
            CatalogReplicationPipelineFormat::DebeziumJson => {
                ReplicationPipelineRuntimeFormat::DebeziumJson
            }
            CatalogReplicationPipelineFormat::ArrowIpc => {
                ReplicationPipelineRuntimeFormat::ArrowIpc
            }
        },
        buffer_mode: match pipeline.buffer_mode() {
            CatalogReplicationBufferMode::Durable => ReplicationPipelineRuntimeBufferMode::Durable,
            CatalogReplicationBufferMode::NoBuffer => {
                ReplicationPipelineRuntimeBufferMode::NoBuffer
            }
        },
        buffer_policy: pipeline.buffer_policy(),
        error_policy: pipeline.error_policy(),
        emit_tombstones: pipeline.emit_tombstones(),
        include_transaction_metadata: pipeline.include_transaction_metadata(),
    })
}

fn postgres_database_name(connection_string: &str, fallback: &str) -> String {
    connection_string
        .parse::<tokio_postgres::Config>()
        .ok()
        .and_then(|config| config.get_dbname().map(ToString::to_string))
        .filter(|database| !database.trim().is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn source_name_for_postgres_include_table(table: &str, registry: &SourceRegistry) -> String {
    if registry.contains(table) {
        return table.to_string();
    }
    table
        .rsplit_once('.')
        .map(|(_, name)| name.to_string())
        .unwrap_or_else(|| table.to_string())
}

fn upstream_table_ref_for_postgres_include_table(table: &str) -> anyhow::Result<UpstreamTableRef> {
    match table.split_once('.') {
        Some((schema, name)) => Ok(UpstreamTableRef::new(schema, name)?),
        None => Ok(UpstreamTableRef::new("public", table)?),
    }
}

pub(super) fn insert_catalog_source_definition(
    sources: &mut HashMap<String, CatalogSourceDefinition>,
    definition: CatalogSourceDefinition,
    origin: &str,
) -> anyhow::Result<()> {
    if sources
        .insert(definition.name().to_string(), definition)
        .is_some()
    {
        return Err(anyhow!("duplicate source definition from {origin}"));
    }
    Ok(())
}

pub(super) fn insert_source_backed_table_definition(
    tables: &mut HashMap<String, SourceBackedTableDefinition>,
    definition: SourceBackedTableDefinition,
    origin: &str,
) -> anyhow::Result<()> {
    if tables
        .insert(definition.table_name().to_string(), definition)
        .is_some()
    {
        return Err(anyhow!(
            "duplicate source-backed table definition from {origin}"
        ));
    }
    Ok(())
}

pub(super) fn insert_replication_pipeline_definition(
    pipelines: &mut HashMap<String, CatalogReplicationPipelineDefinition>,
    definition: CatalogReplicationPipelineDefinition,
    origin: &str,
) -> anyhow::Result<()> {
    if pipelines
        .insert(definition.name().to_string(), definition)
        .is_some()
    {
        return Err(anyhow!(
            "duplicate replication pipeline definition from {origin}"
        ));
    }
    Ok(())
}

pub(super) fn validate_source_backed_tables(
    catalog_sources: &HashMap<String, CatalogSourceDefinition>,
    source_tables: &HashMap<String, SourceBackedTableDefinition>,
    source_registry: &SourceRegistry,
) -> anyhow::Result<()> {
    for binding in source_tables.values() {
        let source = catalog_sources.get(binding.source_name()).ok_or_else(|| {
            anyhow!(
                "table '{}' references unknown source '{}'",
                binding.table_name(),
                binding.source_name()
            )
        })?;
        match source.connector() {
            CatalogSourceConnector::PostgresCdc(_) => {}
        }
        let table_definition = source_registry.get(binding.table_name()).ok_or_else(|| {
            anyhow!(
                "source-backed table '{}' has no registered table definition",
                binding.table_name()
            )
        })?;
        if !source_definition_has_primary_key(table_definition) {
            return Err(anyhow!(
                "CDC table '{}' must declare a primary key",
                binding.table_name()
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_replication_pipelines(
    catalog_sources: &HashMap<String, CatalogSourceDefinition>,
    pipelines: &HashMap<String, CatalogReplicationPipelineDefinition>,
) -> anyhow::Result<()> {
    for pipeline in pipelines.values() {
        let source = catalog_sources.get(pipeline.source_name()).ok_or_else(|| {
            anyhow!(
                "replication pipeline '{}' references unknown source '{}'",
                pipeline.name(),
                pipeline.source_name()
            )
        })?;
        match source.connector() {
            CatalogSourceConnector::PostgresCdc(_) => {}
        }
        match pipeline.target() {
            CatalogReplicationPipelineTarget::Kafka { .. } => {}
            CatalogReplicationPipelineTarget::Postgres { .. } => {}
        }
    }
    Ok(())
}

pub(in crate::node_runtime) fn validate_materialized_views_do_not_query_raw_cdc_sources(
    catalog_sources: &HashMap<String, CatalogSourceDefinition>,
    materialized_views: &[MaterializedViewDefinition],
) -> anyhow::Result<()> {
    let raw_cdc_sources = catalog_sources
        .values()
        .map(|source| match source.connector() {
            CatalogSourceConnector::PostgresCdc(_) => source.name().to_string(),
        })
        .collect::<BTreeSet<_>>();
    if raw_cdc_sources.is_empty() {
        return Ok(());
    }

    for view in materialized_views {
        let references = floe_sql_parser::referenced_table_names_in_query(view.query())
            .with_context(|| {
                format!(
                    "inspect source references for materialized view '{}'",
                    view.name()
                )
            })?;
        if let Some(source) = references.iter().find_map(|reference| {
            raw_cdc_sources
                .iter()
                .find(|source| raw_cdc_reference_matches(reference, source))
        }) {
            return Err(anyhow!(
                "materialized view '{}' reads raw CDC source '{}'; create a CDC table with CREATE TABLE ... FROM {} TABLE ... or use CREATE REPLICATION PIPELINE for passthrough",
                view.name(),
                source,
                source
            ));
        }
    }
    Ok(())
}

fn raw_cdc_reference_matches(reference: &str, source: &str) -> bool {
    reference == source
        || reference
            .strip_prefix(source)
            .is_some_and(|rest| rest.starts_with('.'))
}

pub(in crate::node_runtime) fn merge_catalog_source_connectors(
    connector_specs: &mut Vec<config::ConnectorSpec>,
    catalog_sources: &HashMap<String, CatalogSourceDefinition>,
    source_tables: &HashMap<String, SourceBackedTableDefinition>,
    replication_pipelines: &HashMap<String, CatalogReplicationPipelineDefinition>,
) -> anyhow::Result<()> {
    let mut existing_names = connector_specs
        .iter()
        .map(|connector| connector.name.clone())
        .collect::<BTreeSet<_>>();
    let mut sorted_sources = catalog_sources.values().collect::<Vec<_>>();
    sorted_sources.sort_by(|left, right| left.name().cmp(right.name()));

    for source in sorted_sources {
        let include_tables = source_tables
            .values()
            .filter(|table| table.source_name() == source.name())
            .map(|table| table.upstream_table().to_string())
            .chain(
                replication_pipelines
                    .values()
                    .filter(|pipeline| pipeline.source_name() == source.name())
                    .map(|pipeline| pipeline.upstream_table().to_string()),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if include_tables.is_empty() {
            continue;
        }
        if !existing_names.insert(source.name().to_string()) {
            return Err(anyhow!(
                "source '{}' conflicts with an existing connector name",
                source.name()
            ));
        }
        let config = match source.connector() {
            CatalogSourceConnector::PostgresCdc(postgres) => {
                postgres
                    .connection()
                    .parse::<tokio_postgres::Config>()
                    .with_context(|| {
                        format!(
                            "source '{}' has an invalid Postgres connection string",
                            source.name()
                        )
                    })?;
                ConnectorConfig::PostgresCdc {
                    name: Some(source.name().to_string()),
                    connection: postgres.connection().to_string(),
                    slot: postgres.slot().to_string(),
                    publication: postgres.publication().map(ToString::to_string),
                    include_tables: Some(include_tables),
                    include_schema_in_source: postgres.include_schema_in_source(),
                    schema_evolution_policy: Some(postgres.schema_evolution_policy()),
                    auto_create_slot: Some(postgres.auto_create_slot()),
                    auto_create_publication: Some(postgres.auto_create_publication()),
                }
            }
        };
        connector_specs.push(config::ConnectorSpec {
            name: source.name().to_string(),
            config,
        });
    }

    Ok(())
}
