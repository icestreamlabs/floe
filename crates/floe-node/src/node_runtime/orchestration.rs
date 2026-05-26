use super::*;

pub(super) fn source_journal_required_sources(
    registry: &SourceRegistry,
    transient_only_sources: &BTreeSet<String>,
    mode: SourceJournalConfig,
) -> BTreeSet<String> {
    match mode {
        SourceJournalConfig::Full => transient_only_sources.clone(),
        SourceJournalConfig::None => BTreeSet::new(),
        SourceJournalConfig::Auto => transient_only_sources
            .iter()
            .filter(|source| {
                registry
                    .get(source.as_str())
                    .is_none_or(|definition| !source_is_replayable_from_connector(definition))
            })
            .cloned()
            .collect(),
    }
}

fn source_is_replayable_from_connector(definition: &SourceDefinition) -> bool {
    definition.properties().iter().any(|(key, value)| {
        key.starts_with("connector.") && key.ends_with(".type") && value.as_str() == "kafka"
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn postgres_cdc_runtime_plan(
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
            let schema = super::postgres_snapshot::discover_postgres_cdc_table_schema(
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

fn postgres_schema_evolution_policy_from_catalog(
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

fn insert_catalog_source_definition(
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

fn insert_source_backed_table_definition(
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

fn insert_replication_pipeline_definition(
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

fn validate_source_backed_tables(
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

fn validate_replication_pipelines(
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

pub(super) fn validate_materialized_views_do_not_query_raw_cdc_sources(
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

pub(super) fn merge_catalog_source_connectors(
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

async fn run_native_postgres_cdc_connector(
    mut config: PostgresCdcSourceConfig,
    runtime_plan: PostgresCdcRuntimePlan,
    snapshot_settings: PostgresCdcSnapshotConfig,
    reconnect_settings: PostgresCdcReconnectConfig,
    table_store: CdcTableStore,
    cdc_replication_debug: Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    sender: mpsc::Sender<QueuedCdcTransaction>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    config.validate()?;
    let connection_string = config.connection_string.clone();
    let slot = config.slot.clone();
    let publication = config.publication.clone();
    super::postgres_snapshot::ensure_postgres_cdc_publication_and_slot(
        &connection_string,
        &slot,
        &publication,
        &runtime_plan,
        config.auto_create_slot,
        config.auto_create_publication,
    )
    .await?;
    let initial_snapshot = super::postgres_snapshot::run_initial_postgres_snapshot_if_needed(
        &connection_string,
        &slot,
        &publication,
        &runtime_plan,
        &table_store,
        &sender,
        &cdc_replication_debug,
        snapshot_settings,
        config.commit_lsn_rx.as_mut(),
        &cancel,
    )
    .await?;
    if let Some(lsn) = initial_snapshot.lsn {
        metrics::record_postgres_cdc_upstream_lsn(
            runtime_plan.source_id.as_str(),
            &slot,
            lsn.as_u64(),
        );
        metrics::record_postgres_cdc_durable_lsn(
            runtime_plan.source_id.as_str(),
            &slot,
            lsn.as_u64(),
        );
        record_postgres_cdc_debug_lsn(
            &cdc_replication_debug,
            runtime_plan.source_id.as_str(),
            &slot,
            Some(lsn.as_u64()),
            Some(lsn.as_u64()),
        );
    }
    if let Some(wal_stream) = initial_snapshot.wal_stream {
        match forward_buffered_postgres_wal_stream(
            wal_stream,
            sender.clone(),
            config.commit_lsn_rx.as_mut(),
            &cancel,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(err) if cancel.is_cancelled() => return Err(err),
            Err(err) if !is_reconnectable_postgres_cdc_error(&err) => return Err(err),
            Err(err) => {
                tracing::warn!(
                    source = %runtime_plan.source_id.as_str(),
                    slot = %slot,
                    error = %err,
                    "buffered Postgres CDC WAL stream failed; reconnecting from durable checkpoint"
                );
            }
        }
    }
    let reconnect_policy = PostgresCdcRuntimeReconnectPolicy::from_config(reconnect_settings);
    run_native_postgres_cdc_wal_stream_with_reconnect(
        &connection_string,
        &slot,
        &publication,
        &runtime_plan,
        &table_store,
        &cdc_replication_debug,
        sender,
        config.commit_lsn_rx.as_mut(),
        &cancel,
        reconnect_policy,
    )
    .await
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PostgresCdcRuntimeReconnectPolicy {
    pub(super) max_reconnects: usize,
    pub(super) retry_base: Duration,
    pub(super) retry_max_backoff: Duration,
}

impl PostgresCdcRuntimeReconnectPolicy {
    fn from_config(config: PostgresCdcReconnectConfig) -> Self {
        Self {
            max_reconnects: config.max_reconnects,
            retry_base: Duration::from_millis(config.retry_base_ms),
            retry_max_backoff: Duration::from_millis(config.retry_max_backoff_ms),
        }
    }

    pub(super) fn backoff_for_reconnect(self, reconnect_idx: usize) -> Duration {
        let base_ms = self.retry_base.as_millis() as u64;
        let max_ms = self.retry_max_backoff.as_millis() as u64;
        let factor = if reconnect_idx >= 63 {
            u64::MAX
        } else {
            1_u64 << reconnect_idx
        };
        Duration::from_millis(base_ms.saturating_mul(factor).min(max_ms))
    }
}

async fn run_native_postgres_cdc_wal_stream_with_reconnect(
    connection_string: &str,
    slot: &str,
    publication: &str,
    runtime_plan: &PostgresCdcRuntimePlan,
    table_store: &CdcTableStore,
    cdc_replication_debug: &Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    sender: mpsc::Sender<QueuedCdcTransaction>,
    mut commit_lsn_rx: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    cancel: &CancellationToken,
    policy: PostgresCdcRuntimeReconnectPolicy,
) -> anyhow::Result<()> {
    let mut reconnects = 0usize;
    loop {
        match run_native_postgres_cdc_wal_stream_once(
            connection_string,
            slot,
            publication,
            runtime_plan,
            table_store,
            cdc_replication_debug,
            sender.clone(),
            commit_lsn_rx.as_deref_mut(),
            cancel,
            reconnects as u64,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(err) if cancel.is_cancelled() => return Err(err),
            Err(err) if !is_reconnectable_postgres_cdc_error(&err) => return Err(err),
            Err(err) if reconnects < policy.max_reconnects => {
                let backoff = policy.backoff_for_reconnect(reconnects);
                reconnects = reconnects.saturating_add(1);
                metrics::record_postgres_cdc_source_connected(
                    runtime_plan.source_id.as_str(),
                    slot,
                    false,
                );
                metrics::inc_postgres_cdc_reconnect(
                    runtime_plan.source_id.as_str(),
                    slot,
                    "scheduled",
                );
                record_postgres_cdc_debug_connection_state(
                    cdc_replication_debug,
                    runtime_plan.source_id.as_str(),
                    slot,
                    false,
                    reconnects as u64,
                    Some(
                        "Postgres CDC stream disconnected; reconnecting from durable checkpoint"
                            .to_string(),
                    ),
                );
                tracing::warn!(
                    source = %runtime_plan.source_id.as_str(),
                    slot,
                    reconnects,
                    max_reconnects = policy.max_reconnects,
                    retry_delay_ms = backoff.as_millis() as u64,
                    error = %err,
                    "Postgres CDC stream failed; reconnecting from durable checkpoint"
                );
                if !backoff.is_zero() {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            return Err(anyhow!("cancelled before Postgres CDC reconnect"));
                        }
                        _ = tokio::time::sleep(backoff) => {}
                    }
                }
            }
            Err(err) => {
                metrics::record_postgres_cdc_source_connected(
                    runtime_plan.source_id.as_str(),
                    slot,
                    false,
                );
                metrics::inc_postgres_cdc_reconnect(
                    runtime_plan.source_id.as_str(),
                    slot,
                    "exhausted",
                );
                record_postgres_cdc_debug_connection_state(
                    cdc_replication_debug,
                    runtime_plan.source_id.as_str(),
                    slot,
                    false,
                    reconnects as u64,
                    Some("Postgres CDC stream reconnect attempts exhausted".to_string()),
                );
                return Err(err).with_context(|| {
                    format!("Postgres CDC stream failed after {reconnects} reconnect attempt(s)")
                });
            }
        }
    }
}

async fn run_native_postgres_cdc_wal_stream_once(
    connection_string: &str,
    slot: &str,
    publication: &str,
    runtime_plan: &PostgresCdcRuntimePlan,
    table_store: &CdcTableStore,
    cdc_replication_debug: &Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    sender: mpsc::Sender<QueuedCdcTransaction>,
    mut commit_lsn_rx: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    cancel: &CancellationToken,
    reconnect_attempts: u64,
) -> anyhow::Result<()> {
    let start_lsn = stored_slot_start_lsn(connection_string, slot)
        .await
        .map_err(reconnectable_postgres_cdc_error)
        .with_context(|| format!("load Postgres logical slot '{slot}' start LSN"))?;
    let replication_config =
        replication_config_from_connection_string(connection_string, slot, publication, start_lsn)?;
    let replication_config = config_with_stored_cdc_checkpoint(
        replication_config,
        &table_store,
        &runtime_plan.source_id,
    )
    .await?;
    tracing::info!(
        source = %runtime_plan.source_id.as_str(),
        slot = %slot,
        tables = runtime_plan.schemas.len(),
        start_lsn = ?replication_config.start_lsn(),
        "starting native Postgres CDC replication stream"
    );
    let mut replication = PostgresReplicationClient::connect(&replication_config)
        .await
        .map_err(reconnectable_postgres_cdc_error)
        .context("connect native Postgres CDC transaction stream")?;
    metrics::record_postgres_cdc_source_connected(runtime_plan.source_id.as_str(), slot, true);
    record_postgres_cdc_debug_connection_state(
        cdc_replication_debug,
        runtime_plan.source_id.as_str(),
        slot,
        true,
        reconnect_attempts,
        None,
    );
    tracing::info!(
        source = %runtime_plan.source_id.as_str(),
        slot = %slot,
        "native Postgres CDC replication stream connected"
    );
    let router = PostgresTableRouter::from_schemas(runtime_plan.schemas.values());
    let mut assembler = PostgresTransactionAssembler::with_schemas(
        runtime_plan.source_id.clone(),
        router,
        runtime_plan.schemas.clone(),
        runtime_plan.schema_evolution_policy,
    );
    let mut last_committed_tick_id = 0_u64;
    let mut last_enqueued_lsn = None;

    let result = async {
        loop {
            update_native_postgres_applied_lsn(
                &mut replication,
                commit_lsn_rx.as_deref_mut(),
                slot,
                &mut last_committed_tick_id,
            )?;

            let event = tokio::select! {
                _ = cancel.cancelled() => break,
                event = replication.recv() => event
                    .map_err(reconnectable_postgres_cdc_error)
                    .context("receive native Postgres CDC event")?,
            };
            let Some(event) = event else {
                break;
            };
            if let Some(frontier_lsn) = postgres_replication_event_frontier_lsn(&event) {
                metrics::record_postgres_cdc_upstream_lsn(
                    runtime_plan.source_id.as_str(),
                    &slot,
                    frontier_lsn.as_u64(),
                );
                record_postgres_cdc_debug_lsn(
                    &cdc_replication_debug,
                    runtime_plan.source_id.as_str(),
                    &slot,
                    Some(frontier_lsn.as_u64()),
                    None,
                );
            }
            if matches!(event, PostgresReplicationEvent::StoppedAt { .. }) {
                break;
            }
            let transaction = match assembler.accept_event(event) {
                Ok(transaction) => {
                    let observations = assembler.drain_schema_evolution_observations();
                    if !observations.is_empty() {
                        record_postgres_schema_evolution_observations(
                            &cdc_replication_debug,
                            &runtime_plan.source_id,
                            observations,
                        )
                        .await;
                    }
                    transaction
                }
                Err(err) => {
                    let observations = assembler.drain_schema_evolution_observations();
                    if !observations.is_empty() {
                        record_postgres_schema_evolution_observations(
                            &cdc_replication_debug,
                            &runtime_plan.source_id,
                            observations,
                        )
                        .await;
                    }
                    return Err(err);
                }
            };
            let Some(transaction) = transaction else {
                continue;
            };
            tracing::debug!(
                source = %runtime_plan.source_id.as_str(),
                slot = %slot,
                change_batches = transaction.change_batches().len(),
                commit_position = ?transaction.commit_position(),
                "assembled native Postgres CDC transaction"
            );
            let transaction_lsn = PostgresLsn::from_source_position(transaction.commit_position())?;
            sender
                .send(QueuedCdcTransaction {
                    slot: slot.to_string(),
                    source_id: runtime_plan.source_id.clone(),
                    transaction,
                })
                .await
                .map_err(|err| {
                    anyhow!("failed to enqueue native Postgres CDC transaction: {err}")
                })?;
            last_enqueued_lsn = Some(transaction_lsn);
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    replication.stop();
    let shutdown_result = replication.shutdown().await;
    if let Err(err) = result {
        wait_for_enqueued_postgres_cdc_lsn(commit_lsn_rx, slot, last_enqueued_lsn, cancel).await?;
        return Err(err);
    }
    shutdown_result
}

async fn forward_buffered_postgres_wal_stream(
    mut stream: BufferedPostgresWalStream,
    sender: mpsc::Sender<QueuedCdcTransaction>,
    commit_lsn_rx: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!(
        slot = %stream.slot,
        snapshot_lsn = %stream.snapshot_lsn,
        "forwarding buffered Postgres CDC WAL stream after durable initial snapshot"
    );

    let mut last_enqueued_lsn = None;
    loop {
        let transaction = tokio::select! {
            _ = cancel.cancelled() => break,
            transaction = stream.receiver.recv() => transaction,
        };
        let Some(transaction) = transaction else {
            break;
        };
        let transaction_lsn =
            PostgresLsn::from_source_position(transaction.transaction.commit_position())?;
        if let Err(err) = sender.send(transaction).await {
            stream.task.abort();
            let _ = stream.task.await;
            return Err(anyhow!(
                "failed to enqueue buffered native Postgres CDC transaction: {err}"
            ));
        }
        last_enqueued_lsn = Some(transaction_lsn);
    }

    let stream_result = match stream.task.await {
        Ok(result) => result,
        Err(err) if err.is_cancelled() && cancel.is_cancelled() => Ok(()),
        Err(err) => Err(anyhow!("buffered Postgres CDC WAL task failed: {err}")),
    };
    if let Err(err) = stream_result {
        wait_for_enqueued_postgres_cdc_lsn(commit_lsn_rx, &stream.slot, last_enqueued_lsn, cancel)
            .await?;
        return Err(err);
    }
    Ok(())
}

async fn wait_for_enqueued_postgres_cdc_lsn(
    receiver: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    slot: &str,
    target_lsn: Option<PostgresLsn>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let Some(target_lsn) = target_lsn else {
        return Ok(());
    };
    super::postgres_snapshot::wait_for_postgres_cdc_commit(
        receiver,
        slot,
        target_lsn,
        cancel,
        "queued Postgres CDC transactions to become durable before reconnect",
    )
    .await
}

fn postgres_replication_event_frontier_lsn(
    event: &PostgresReplicationEvent,
) -> Option<PostgresLsn> {
    match event {
        PostgresReplicationEvent::KeepAlive { wal_end, .. } => Some(*wal_end),
        PostgresReplicationEvent::Begin { final_lsn, .. } => Some(*final_lsn),
        PostgresReplicationEvent::XLogData { wal_end, .. } => Some(*wal_end),
        PostgresReplicationEvent::Commit { end_lsn, .. } => Some(*end_lsn),
        PostgresReplicationEvent::Message { lsn, .. } => Some(*lsn),
        PostgresReplicationEvent::StoppedAt { reached } => Some(*reached),
    }
}

fn update_native_postgres_applied_lsn(
    replication: &mut PostgresReplicationClient,
    receiver: Option<&mut watch::Receiver<PostgresCdcCommit>>,
    slot: &str,
    last_committed_tick_id: &mut u64,
) -> anyhow::Result<()> {
    let Some(receiver) = receiver else {
        return Ok(());
    };

    let mut latest_commit = None;
    while receiver.has_changed().unwrap_or(false) {
        latest_commit = Some(receiver.borrow_and_update().clone());
    }
    let Some(commit) = latest_commit else {
        return Ok(());
    };
    if commit.tick_id <= *last_committed_tick_id {
        return Ok(());
    }

    if let Some(target_lsn) = commit
        .slots
        .iter()
        .find(|entry| entry.slot == slot)
        .map(|entry| entry.lsn.as_str())
    {
        replication.update_applied_lsn(PostgresLsn::parse(target_lsn)?);
    }
    *last_committed_tick_id = commit.tick_id;
    Ok(())
}

fn build_tick_commit_for_checkpoint(
    epoch: u64,
    frontier: u64,
    checkpoint_manager: &CheckpointManager,
    mv_versions: &[MaterializedViewTickVersion],
    committed_kafka_offsets: &HashMap<(Arc<str>, i32), i64>,
) -> TickCommit {
    TickCommit::new(
        epoch,
        frontier,
        checkpoint_manager.snapshot_offsets(),
        mv_versions.to_vec(),
        checkpoint_manager.snapshot_sink_cursors(),
    )
    .with_kafka_offsets(checkpoint_kafka_offsets(committed_kafka_offsets))
    .with_operator_states(checkpoint_operator_states())
}

fn checkpoint_kafka_offsets(
    committed_kafka_offsets: &HashMap<(Arc<str>, i32), i64>,
) -> Vec<KafkaCheckpointOffset> {
    let mut offsets = committed_kafka_offsets
        .iter()
        .map(|((topic, partition), offset)| KafkaCheckpointOffset {
            topic: topic.to_string(),
            partition: *partition,
            offset: *offset,
        })
        .collect::<Vec<_>>();
    offsets.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then(left.partition.cmp(&right.partition))
    });
    offsets
}

fn checkpoint_operator_states() -> Vec<floe_executor::checkpoint::DbspHandleRecord> {
    dbsp::snapshot_operator_states()
        .into_iter()
        .map(|handle| {
            floe_executor::checkpoint::DbspHandleRecord::operator_state(
                handle.name,
                handle.namespace,
                handle.version,
            )
        })
        .collect()
}

fn record_postgres_cdc_lsn_progress(
    committed_postgres_lsns: &mut HashMap<String, (u64, String)>,
    tick_postgres_lsns: &HashMap<String, (u64, String)>,
    tick_postgres_sources: &HashMap<String, String>,
    tick_postgres_table_lsns: &[(String, String, String, u64)],
    cdc_replication_debug: &Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
) {
    advance_postgres_cdc_commit_state(committed_postgres_lsns, tick_postgres_lsns);
    for (slot, (lsn_value, _)) in tick_postgres_lsns {
        if let Some(source) = tick_postgres_sources.get(slot) {
            metrics::record_postgres_cdc_durable_lsn(source, slot, *lsn_value);
            record_postgres_cdc_debug_lsn(
                cdc_replication_debug,
                source,
                slot,
                None,
                Some(*lsn_value),
            );
        }
    }
    for (source, slot, table, lsn_value) in tick_postgres_table_lsns {
        metrics::record_postgres_cdc_table_applied_lsn(source, slot, table, *lsn_value);
    }
}

fn notify_postgres_cdc_commit_senders(
    epoch: u64,
    committed_postgres_lsns: &HashMap<String, (u64, String)>,
    tick_postgres_lsns: &HashMap<String, (u64, String)>,
    senders: &[watch::Sender<PostgresCdcCommit>],
) {
    if tick_postgres_lsns.is_empty() || senders.is_empty() {
        return;
    }
    let postgres_commit_start = Instant::now();
    let commit = build_postgres_cdc_commit(epoch, committed_postgres_lsns);
    for sender in senders {
        let _ = sender.send(commit.clone());
    }
    metrics::observe_tick_phase_latency_ms(
        "postgres_cdc_commit_notify",
        postgres_commit_start.elapsed().as_millis() as u64,
    );
}

pub(crate) async fn run() -> anyhow::Result<()> {
    init_tracing();
    metrics::init();
    let Some(mut run_args) = parse_run_command()? else {
        return Ok(());
    };

    let config = if let Some(path) = run_args.config.as_deref() {
        Some(load_config(path)?)
    } else {
        None
    };

    if let Some(config) = config.as_ref() {
        apply_runtime_config_defaults(&mut run_args, config);
    }
    let postgres_cdc_settings = config
        .as_ref()
        .map(|cfg| cfg.postgres_cdc)
        .unwrap_or_default();

    if run_args.config.is_none()
        && run_args.kafka_brokers.is_some()
        && run_args.kafka_topics.is_empty()
    {
        return Err(anyhow::anyhow!(
            "--kafka-topics is required when --kafka-brokers is set"
        ));
    }
    if run_args.object_store_from_env && run_args.data_dir.is_some() {
        return Err(anyhow::anyhow!(
            "--data-dir cannot be used with --object-store-from-env"
        ));
    }
    if !run_args.object_store_from_env && run_args.object_store_env_file.is_some() {
        return Err(anyhow::anyhow!(
            "--object-store-env-file requires --object-store-from-env"
        ));
    }
    if !run_args.object_store_from_env && run_args.slatedb_name.is_some() {
        return Err(anyhow::anyhow!(
            "--slatedb-name requires --object-store-from-env"
        ));
    }
    let awaited_durable = run_args.slatedb_await_durable.unwrap_or(true);
    SlateTable::set_default_await_durable(awaited_durable);
    if !awaited_durable {
        tracing::warn!(
            "SlateDB durable-await is disabled; committed writes may be acknowledged before object-store durability"
        );
    }
    let stream_gc = StreamGcConfig {
        grace_period_ms: run_args.zset_gc_grace_period_ms,
    };
    let gc_policy = GcPolicy {
        grace_period: Duration::from_millis(stream_gc.grace_period_ms),
    };

    if config.is_some() {
        let ignored_flags = cli_connector_creation_flags(&run_args);
        if !ignored_flags.is_empty() {
            tracing::warn!(
                ignored_flags = ?ignored_flags,
                "connector creation flags are ignored when --config is provided"
            );
        }
    }

    let (mut connector_specs, mut sink_specs) = if let Some(config) = config.as_ref() {
        let connectors = normalize_connectors(config.connectors.clone())?;
        let sinks = normalize_sinks(config.sinks.clone())?;
        (connectors, sinks)
    } else {
        let connectors = normalize_connectors(connectors_from_cli(&run_args))?;
        (connectors, Vec::new())
    };
    let mut source_registry = SourceRegistry::new();
    source_registry.extend(floe_node_core::generator::definitions()?);

    let slate_settings = load_slatedb_settings(&run_args)?;
    let storage_config = server::ServerStorageConfig {
        data_dir: run_args.data_dir.clone().map(PathBuf::from),
        object_store_from_env: run_args.object_store_from_env,
        object_store_env_file: run_args.object_store_env_file.clone(),
        slatedb_name: run_args.slatedb_name.clone(),
    };
    let storage = if run_args.dry_run {
        None
    } else {
        Some(server::init_storage(storage_config, slate_settings).await?)
    };
    let mut materialized_view_map: HashMap<String, MaterializedViewDefinition> = HashMap::new();
    let mut catalog_sources: HashMap<String, CatalogSourceDefinition> = HashMap::new();
    let mut source_backed_tables: HashMap<String, SourceBackedTableDefinition> = HashMap::new();
    let mut replication_pipelines: HashMap<String, CatalogReplicationPipelineDefinition> =
        HashMap::new();
    let mut sql_sink_specs = Vec::new();
    if let Some(storage) = storage.as_ref() {
        for definition in storage
            .catalog_sources()
            .await
            .context("load persisted source definitions")?
        {
            insert_catalog_source_definition(&mut catalog_sources, definition, "catalog")?;
        }
        for definition in storage
            .source_backed_tables()
            .await
            .context("load persisted source-backed table definitions")?
        {
            insert_source_backed_table_definition(
                &mut source_backed_tables,
                definition,
                "catalog",
            )?;
        }
        for definition in storage
            .replication_pipelines()
            .await
            .context("load persisted replication pipeline definitions")?
        {
            insert_replication_pipeline_definition(
                &mut replication_pipelines,
                definition,
                "catalog",
            )?;
        }
        let db = storage.db();
        let stored_views = storage
            .materialized_views()
            .await
            .context("load persisted materialized views")?;
        let gc_table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        for metadata in &stored_views {
            let namespace = floe_executor::namespaces::materialized_view(metadata.name())
                .with_context(|| {
                    format!(
                        "derive namespace for materialized view '{}'",
                        metadata.name()
                    )
                })?;
            let gc = GcService::new(gc_table.clone(), namespace.clone(), gc_policy);
            let (_, recovered_intents) = gc
                .recover_startup()
                .await
                .with_context(|| format!("run startup GC recovery for namespace '{namespace}'"))?;
            if recovered_intents > 0 {
                tracing::info!(
                    view = %metadata.name(),
                    namespace = %namespace,
                    recovered_intents,
                    "recovered stale manifest intents during startup"
                );
            }
        }
        for metadata in stored_views {
            let definition = MaterializedViewDefinition::new(
                metadata.name(),
                metadata.query(),
                metadata.if_not_exists(),
            );
            materialized_view_map.insert(definition.name().to_string(), definition);
        }
    }

    if let Some(config) = config.as_ref() {
        for definition in materialized_view_definitions_from_config(&config.materialized_views) {
            upsert_materialized_view_definition(
                &mut materialized_view_map,
                definition,
                storage.as_ref(),
                "config file",
            )
            .await?;
        }
    }

    if let Some(sql_program) = run_args.mv_query.as_deref() {
        for statement in parse_floe_program(sql_program)? {
            match statement {
                FloeStatement::CreateSource(definition) => {
                    let source = catalog_source_definition_from_sql(&definition)?;
                    if let Some(storage) = storage.as_ref() {
                        storage
                            .upsert_catalog_source(source.clone())
                            .await
                            .with_context(|| {
                                format!("persist source definition '{}'", source.name())
                            })?;
                    }
                    catalog_sources.insert(source.name().to_string(), source);
                }
                FloeStatement::CreateTable(definition) => {
                    let table = table_definition_from_sql(&definition)?;
                    let source_backed_table = source_backed_table_definition_from_sql(&definition)?;
                    if let Some(binding) = source_backed_table.as_ref()
                        && !catalog_sources.contains_key(binding.source_name())
                    {
                        return Err(anyhow!(
                            "table '{}' references unknown source '{}'",
                            binding.table_name(),
                            binding.source_name()
                        ));
                    }
                    if let Some(storage) = storage.as_ref() {
                        storage.upsert_table(table.clone()).await.with_context(|| {
                            format!("persist table definition '{}'", table.name())
                        })?;
                        if let Some(binding) = source_backed_table.as_ref() {
                            storage
                                .upsert_source_backed_table(binding.clone())
                                .await
                                .with_context(|| {
                                    format!(
                                        "persist source-backed table definition '{}'",
                                        binding.table_name()
                                    )
                                })?;
                        }
                    }
                    source_registry.register(source_definition_from_table(&table)?);
                    if let Some(binding) = source_backed_table {
                        source_backed_tables.insert(binding.table_name().to_string(), binding);
                    }
                }
                FloeStatement::CreateMaterializedView(definition) => {
                    upsert_materialized_view_definition(
                        &mut materialized_view_map,
                        definition,
                        storage.as_ref(),
                        "--mv-query",
                    )
                    .await?;
                }
                FloeStatement::CreateSink(definition) => {
                    sql_sink_specs.push(sink_spec_from_sql(&definition)?);
                }
                FloeStatement::CreateReplicationPipeline(definition) => {
                    let pipeline = replication_pipeline_definition_from_sql(&definition)?;
                    if !catalog_sources.contains_key(pipeline.source_name()) {
                        return Err(anyhow!(
                            "replication pipeline '{}' references unknown source '{}'",
                            pipeline.name(),
                            pipeline.source_name()
                        ));
                    }
                    if let Some(storage) = storage.as_ref() {
                        storage
                            .upsert_replication_pipeline(pipeline.clone())
                            .await
                            .with_context(|| {
                                format!(
                                    "persist replication pipeline definition '{}'",
                                    pipeline.name()
                                )
                            })?;
                    }
                    insert_replication_pipeline_definition(
                        &mut replication_pipelines,
                        pipeline,
                        "--mv-query",
                    )?;
                }
                FloeStatement::Tail { .. } | FloeStatement::Subscribe { .. } => {
                    return Err(anyhow!(
                        "TAIL/SUBSCRIBE statements are not supported in --mv-query programs"
                    ));
                }
            }
        }
    }

    merge_sql_sinks(&mut sink_specs, sql_sink_specs, &materialized_view_map)?;

    if let Some(storage) = storage.as_ref() {
        for table in storage
            .tables()
            .await
            .context("load persisted table definitions")?
        {
            source_registry.register(source_definition_from_table(&table)?);
        }
    }
    validate_source_backed_tables(&catalog_sources, &source_backed_tables, &source_registry)?;
    validate_replication_pipelines(&catalog_sources, &replication_pipelines)?;
    merge_catalog_source_connectors(
        &mut connector_specs,
        &catalog_sources,
        &source_backed_tables,
        &replication_pipelines,
    )?;
    log_startup_banner(&run_args, &connector_specs);
    apply_connector_properties(&mut source_registry, &connector_specs);
    let available_sources = available_sources_from_registry(&source_registry);

    let mut materialized_views: Vec<MaterializedViewDefinition> =
        materialized_view_map.into_values().collect();
    materialized_views.sort_by(|a, b| a.name().cmp(b.name()));
    validate_single_materialized_view(&materialized_views)?;
    validate_materialized_views_do_not_query_raw_cdc_sources(
        &catalog_sources,
        &materialized_views,
    )?;
    log_operator_hints(
        &connector_specs,
        &available_sources,
        &materialized_views,
        &sink_specs,
        &run_args,
    );

    let planned_materialized_views =
        plan_materialized_views(&source_registry, &materialized_views).await?;
    let mut tail_execution_config = TailExecutionConfig::default();
    if let Some(channel_capacity) = run_args.tail_channel_capacity {
        tail_execution_config.channel_capacity = channel_capacity;
    }
    if let Some(max_catchup_versions) = run_args.tail_max_catchup_versions {
        tail_execution_config.max_catchup_versions = max_catchup_versions;
    }
    let mut persistence_policy_config = PersistencePolicyConfig::default();
    if let Some(max_nodes) = run_args.transient_segment_max_nodes {
        persistence_policy_config.max_transient_segment_nodes = max_nodes;
    }
    if let Some(min_score) = run_args.transient_segment_min_score {
        persistence_policy_config.min_transient_segment_score = min_score;
    }
    let circuit_plans = build_dataflows(
        &planned_materialized_views,
        &available_sources,
        &source_registry,
    )?;
    let mut all_required_sources: BTreeSet<String> = BTreeSet::new();
    let available_source_names: BTreeSet<String> = available_sources.iter().cloned().collect();
    let mut plan_required_sources: Vec<BTreeSet<String>> = Vec::with_capacity(circuit_plans.len());
    let mut transient_eligible_sources: BTreeSet<String> = BTreeSet::new();
    let mut durable_required_sources: BTreeSet<String> = BTreeSet::new();
    for (mv_idx, plan) in circuit_plans.iter().enumerate() {
        let view_name = planned_materialized_views[mv_idx]
            .definition()
            .name()
            .to_string();
        let ValidatedPlan {
            required_sources, ..
        } = validate_dbsp_plan(plan, &available_source_names, &view_name)?;
        all_required_sources.extend(required_sources.iter().cloned());
        if let Some(source_names) =
            source_batch_journal_root_sources_with_config(plan, persistence_policy_config)?
            && !source_names.is_empty()
            && source_names == required_sources
        {
            transient_eligible_sources.extend(source_names);
        } else {
            durable_required_sources.extend(required_sources.iter().cloned());
        }
        plan_required_sources.push(required_sources);
    }
    let transient_only_sources: BTreeSet<String> = transient_eligible_sources
        .difference(&durable_required_sources)
        .cloned()
        .collect();
    tracing::info!(
        transient_eligible_sources = ?transient_eligible_sources,
        durable_required_sources = ?durable_required_sources,
        transient_only_sources = ?transient_only_sources,
        "resolved source durability sets"
    );
    let source_journal_mode = config
        .as_ref()
        .and_then(|config| config.storage.source_journal)
        .unwrap_or(SourceJournalConfig::Auto);
    let source_journal_required_sources = source_journal_required_sources(
        &source_registry,
        &transient_only_sources,
        source_journal_mode,
    );
    let source_journal_skipped_sources: BTreeSet<String> = transient_only_sources
        .difference(&source_journal_required_sources)
        .cloned()
        .collect();
    tracing::info!(
        mode = ?source_journal_mode,
        journaled_sources = ?source_journal_required_sources,
        skipped_sources = ?source_journal_skipped_sources,
        "resolved transient source journal policy"
    );
    let non_replayable_skipped_sources: BTreeSet<String> = source_journal_skipped_sources
        .iter()
        .filter(|source| {
            source_registry
                .get(source.as_str())
                .is_none_or(|definition| !source_is_replayable_from_connector(definition))
        })
        .cloned()
        .collect();
    if !non_replayable_skipped_sources.is_empty() {
        tracing::warn!(
            sources = ?non_replayable_skipped_sources,
            "source journal disabled for non-replayable transient sources; committed source rows will not be recoverable or queryable after restart"
        );
    }
    let mut transient_required_columns_by_source = {
        let definition_by_name: HashMap<&str, &SourceDefinition> = source_registry
            .definitions()
            .iter()
            .map(|definition| (definition.name(), definition))
            .collect();
        let mut required_columns_by_source: HashMap<String, BTreeSet<usize>> = HashMap::new();
        let mut pruning_blocked_sources = BTreeSet::new();
        for (plan, required_sources) in circuit_plans.iter().zip(plan_required_sources.iter()) {
            let Some(requirements) = plan_source_requirements(plan)? else {
                pruning_blocked_sources.extend(required_sources.iter().cloned());
                continue;
            };
            let covered_sources: BTreeSet<_> = requirements
                .iter()
                .map(|requirement| requirement.source_name.clone())
                .collect();
            if covered_sources != *required_sources {
                pruning_blocked_sources.extend(required_sources.iter().cloned());
                continue;
            }
            for requirement in requirements {
                required_columns_by_source
                    .entry(requirement.source_name)
                    .or_default()
                    .extend(requirement.required_columns);
            }
        }
        let mut masks = HashMap::new();
        for (source_name, required_columns) in required_columns_by_source {
            if pruning_blocked_sources.contains(&source_name) {
                continue;
            }
            let definition = definition_by_name
                .get(source_name.as_str())
                .copied()
                .ok_or_else(|| anyhow!("missing source definition for '{source_name}'"))?;
            if required_columns.len() >= definition.columns().len() {
                continue;
            }
            let mut mask = vec![false; definition.columns().len()];
            for column_idx in required_columns {
                let Some(required) = mask.get_mut(column_idx) else {
                    return Err(anyhow!(
                        "required column index {column_idx} out of bounds for source '{source_name}'"
                    ));
                };
                *required = true;
            }
            if mask.iter().all(|required| *required) {
                continue;
            }
            masks.insert(source_name, Arc::<[bool]>::from(mask));
        }
        masks
    };
    for source in &source_journal_required_sources {
        transient_required_columns_by_source.remove(source);
    }
    if !transient_required_columns_by_source.is_empty() {
        let pruned_sources = transient_required_columns_by_source
            .iter()
            .map(|(source, columns)| {
                let required = columns.iter().filter(|required| **required).count();
                format!("{source}:{required}/{}", columns.len())
            })
            .collect::<Vec<_>>();
        tracing::info!(
            pruned_sources = ?pruned_sources,
            "resolved source column pruning"
        );
    }
    if run_args.dry_run {
        tracing::info!(
            connector_count = connector_specs.len(),
            source_count = all_required_sources.len(),
            materialized_view_count = materialized_views.len(),
            sink_count = sink_specs.len(),
            circuit_plan_count = circuit_plans.len(),
            "dry-run validation succeeded"
        );
        return Ok(());
    }
    let storage = storage.expect("storage initialized when not in dry-run");
    let db = storage.db();
    let checkpoint_table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));
    let cdc_table_store = CdcTableStore::new(Arc::clone(&checkpoint_table));
    let checkpoint_manager =
        CheckpointManager::new(CHECKPOINT_GRAPH_ID, Arc::clone(&checkpoint_table))
            .await
            .context("initialize tick checkpoint manager")?;
    let initial_sink_cursors = checkpoint_manager.snapshot_sink_cursors();
    let recovered_tick_commit = checkpoint_manager.latest_tick_commit().cloned();
    if let Some(tick_commit) = recovered_tick_commit.as_ref() {
        dbsp::install_operator_state_restore(
            tick_commit
                .operator_states
                .iter()
                .filter(|handle| {
                    handle.kind == floe_executor::checkpoint::handle_kinds::OPERATOR_STATE
                })
                .map(|handle| {
                    dbsp::OperatorStateHandle::new(
                        handle.name.clone(),
                        handle.namespace.clone(),
                        handle.version,
                    )
                })
                .collect(),
        );
    } else {
        dbsp::install_operator_state_restore(Vec::new());
    }
    if let Some(tick_commit) = checkpoint_manager.latest_tick_commit() {
        metrics::record_last_committed_tick(tick_commit.tick_id);
    }
    let source_batch_journal = SourceBatchJournal::new(checkpoint_manager.store().table());
    let outer_registry = {
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut registry =
            OuterStreamRegistry::from_validated_sources(&all_required_sources, &mut bridge)
                .await
                .context("initialize outer DBSP streams for sources")?;
        for source in &transient_only_sources {
            registry.set_durable_enabled(source, false);
            let recoverable = source_journal_required_sources.contains(source)
                || source_registry
                    .get(source.as_str())
                    .is_some_and(source_is_replayable_from_connector);
            registry.set_recoverable(source, recoverable);
        }
        registry
    };
    let outer_registry = Arc::new(Mutex::new(outer_registry));
    if circuit_plans.is_empty() {
        tracing::warn!("DBSP planning produced no circuit plans.");
    } else {
        tracing::info!(
            circuit_plans = circuit_plans.len(),
            "DBSP planning produced circuit plans"
        );
        for plan in &circuit_plans {
            tracing::debug!(root = plan.root, "circuit plan root node");
        }
    }

    let mv_retention = if run_args.mv_retain_last == 0 {
        StreamRetention::None
    } else {
        StreamRetention::KeepLast {
            keep_last: run_args.mv_retain_last,
        }
    };

    let mv_registry = Arc::new(MaterializedViewRegistry::new_with_retention(
        if run_args.mv_retain_last == 0 {
            None
        } else {
            Some(run_args.mv_retain_last)
        },
    ));
    let mut graph_builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .context("initialize DBSP graph builder")?;
    graph_builder.set_persistence_policy_config(persistence_policy_config);
    if let Some(config) = config.as_ref() {
        graph_builder.set_mv_flush_coalescing(mv_flush_coalescing_config(&config.runtime.mv_flush));
        graph_builder.set_mv_overlay_snapshot(mv_snapshot_config(&config.runtime.mv_snapshot));
    }
    let output_mode =
        resolve_output_consolidation_mode(run_args.output_consolidation_mode, &source_registry);
    let consolidation_mode = match output_mode {
        cli::OutputConsolidationMode::AllColumns => ConsolidationMode::ByAllColumns,
        cli::OutputConsolidationMode::Key => ConsolidationMode::ByKey,
    };
    graph_builder.set_output_consolidation_mode(consolidation_mode);
    let stream_compaction = StreamCompactionConfig {
        max_chain_len: run_args.zset_compaction_max_chain_len,
        max_segments: run_args.zset_compaction_max_segments,
        scheduler_backoff_ticks: run_args.zset_compaction_backoff_ticks,
        scheduler_max_concurrent_jobs: run_args.zset_compaction_max_concurrent_jobs,
    };
    graph_builder
        .set_stream_compaction(
            CompactionPolicy {
                max_chain_len: stream_compaction.max_chain_len,
                max_segments: stream_compaction.max_segments,
                max_bucket_segments: stream_compaction.max_segments,
            },
            CompactionSchedulerConfig {
                failure_backoff_ticks: stream_compaction.scheduler_backoff_ticks,
                max_concurrent_jobs: stream_compaction.scheduler_max_concurrent_jobs,
                ..Default::default()
            },
        )
        .await;
    if run_args.maintenance_paused {
        graph_builder.pause_maintenance().await;
        tracing::info!("maintenance started in paused mode");
    }
    for namespace in &run_args.maintenance_inspect_namespace {
        let summary = graph_builder
            .inspect_namespace_storage(namespace)
            .await
            .with_context(|| format!("inspect namespace '{namespace}'"))?;
        tracing::info!(
            namespace = %summary.namespace,
            data_manifest_version = ?summary.data_manifest_version,
            index_manifest_version = ?summary.index_manifest_version,
            pinned_handle_count = summary.pinned_handle_count,
            reachable_data_manifest_count = summary.reachable_data_manifest_count,
            reachable_index_manifest_count = summary.reachable_index_manifest_count,
            reachable_segment_count = summary.reachable_segment_count,
            "namespace storage summary"
        );
    }
    for namespace in &run_args.maintenance_compact_namespace {
        let compacted = graph_builder
            .run_namespace_compaction_once(namespace)
            .await
            .with_context(|| format!("compact namespace '{namespace}'"))?;
        tracing::info!(
            namespace = %namespace,
            compacted_version = ?compacted,
            "maintenance compaction request completed"
        );
    }
    for namespace in &run_args.maintenance_gc_namespace {
        let sweep_stats = graph_builder
            .run_namespace_gc_once(namespace, gc_policy)
            .await
            .with_context(|| format!("run GC sweep for namespace '{namespace}'"))?;
        tracing::info!(
            namespace = %namespace,
            marked = sweep_stats.marked,
            deleted = sweep_stats.deleted,
            skipped_reachable = sweep_stats.skipped_reachable,
            recovered_intents = sweep_stats.recovered_intents,
            "maintenance GC sweep completed"
        );
    }
    let event_watermark = Arc::new(AtomicI64::new(-1));
    let executor_running = Arc::new(AtomicBool::new(true));
    let storage_reachable = Arc::new(AtomicBool::new(true));
    let runtime_ready = Arc::new(AtomicBool::new(false));
    let runtime_cancel = CancellationToken::new();
    let ingest_cancel = CancellationToken::new();
    let sink_cancel = CancellationToken::new();
    let service_cancel = CancellationToken::new();
    let shutdown_signal = CancellationToken::new();
    let runtime_failure = Arc::new(StdMutex::new(None::<String>));
    let (task_event_tx, mut task_event_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let graph_cancel = runtime_cancel.clone();
    let cancel_for_monitor = runtime_cancel.clone();
    let failure_for_monitor = Arc::clone(&runtime_failure);
    let task_monitor: JoinHandle<()> = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_for_monitor.cancelled() => break,
                maybe_event = task_event_rx.recv() => {
                    let Some(event) = maybe_event else {
                        break;
                    };
                    tracing::error!(
                        graph_id = %event.graph_id,
                        task = %event.task,
                        error = %event.error,
                        error_chain = %format!("{:#}", event.error),
                        "graph background task failed"
                    );
                    record_runtime_failure(
                        &failure_for_monitor,
                        format!(
                            "graph background task failed (graph='{}', task='{}'): {}",
                            event.graph_id, event.task, event.error
                        ),
                    );
                    cancel_for_monitor.cancel();
                }
            }
        }
    });
    for (idx, plan) in circuit_plans.iter().enumerate() {
        let mv_def = &planned_materialized_views[idx];
        let view_name = mv_def.definition().name();
        let namespace = floe_executor::namespaces::materialized_view(view_name)
            .unwrap_or_else(|_| format!("materialized_view/{view_name}"));
        let required_sources = &plan_required_sources[idx];
        let (handle_streams, transient_streams) = {
            let registry_guard = outer_registry.lock().await;
            (
                gather_handle_streams(&registry_guard, required_sources),
                gather_transient_streams(&registry_guard, required_sources),
            )
        };
        tracing::info!(
            view = %view_name,
            namespace = %namespace,
            required_sources = ?required_sources,
            handle_streams = ?handle_streams.keys(),
            transient_streams = ?transient_streams.keys(),
            "building DBSP graph"
        );

        let enable_source_batch_journal =
            source_batch_journal_root_sources_with_config(plan, persistence_policy_config)?
                .as_ref()
                .is_some_and(|source_names| {
                    !source_names.is_empty() && source_names == required_sources
                });

        graph_builder
            .build(BuildInputs {
                graph_id: view_name,
                view_name,
                plan,
                cancel: graph_cancel.clone(),
                task_events: task_event_tx.clone(),
                mv_registry: Arc::clone(&mv_registry),
                outer_handle_streams: &handle_streams,
                outer_transient_streams: &transient_streams,
                enable_source_batch_journal,
                restore_transient_helper_state: required_sources
                    .iter()
                    .all(|source| source_journal_skipped_sources.contains(source)),
                mv_retention,
                watermark: Arc::clone(&event_watermark),
            })
            .await
            .with_context(|| format!("building DBSP graph for '{view_name}'"))?;
    }
    if let Some(tick_commit) = recovered_tick_commit.as_ref()
        && !source_journal_required_sources.is_empty()
    {
        let replayed = {
            let mut registry_guard = outer_registry.lock().await;
            source_batch_journal
                .replay_committed_entries_up_to(
                    &mut registry_guard,
                    tick_commit.tick_id,
                    &source_journal_required_sources,
                )
                .await
                .context("replay committed source batch journal entries")?
        };
        tracing::info!(
            replayed_entries = replayed,
            committed_tick = tick_commit.tick_id,
            journaled_sources = ?source_journal_required_sources,
            "replayed committed source batch journal entries"
        );
        for mv_version in &tick_commit.mv_versions {
            let Some(handle) = mv_registry.get(&mv_version.view) else {
                continue;
            };
            if handle.latest_version().unwrap_or(-1) >= mv_version.version as i64 {
                continue;
            }
            let mut rx = handle.version_watch();
            let target_version = mv_version.version as i64;
            tokio::time::timeout(Duration::from_secs(5), async move {
                loop {
                    if rx.borrow().unwrap_or(-1) >= target_version {
                        break;
                    }
                    if rx.changed().await.is_err() {
                        break;
                    }
                }
            })
            .await
            .with_context(|| {
                format!(
                    "wait for replayed materialized view '{}' to reach version {}",
                    mv_version.view, mv_version.version
                )
            })?;
        }
    }
    if recovered_tick_commit.is_some() && !source_journal_skipped_sources.is_empty() {
        tracing::info!(
            replayable_sources = ?source_journal_skipped_sources,
            "skipped source-batch journal replay for sources expected to resume from connector offsets"
        );
    }
    let queue_capacity = run_args.ingest_queue_capacity;
    let max_batch = run_args.ingest_batch_size;
    let max_batch_per_source = run_args.ingest_batch_per_source;
    let max_batch_per_connector = run_args.ingest_batch_per_connector;
    let pre_tick_commit_delay_ms = run_args.pre_tick_commit_delay_ms.unwrap_or(0);
    let watermark_idle_source_ms = run_args.watermark_idle_source_ms.unwrap_or(0);
    let watermark_idle_source_ms = if watermark_idle_source_ms == 0 {
        DEFAULT_WATERMARK_IDLE_SOURCE_MS
    } else {
        watermark_idle_source_ms
    };

    let runtime_cancel_for_propagation = runtime_cancel.clone();
    let ingest_cancel_for_propagation = ingest_cancel.clone();
    let sink_cancel_for_propagation = sink_cancel.clone();
    let service_cancel_for_propagation = service_cancel.clone();
    let cancellation_propagation_handle: JoinHandle<()> = tokio::spawn(async move {
        runtime_cancel_for_propagation.cancelled().await;
        ingest_cancel_for_propagation.cancel();
        sink_cancel_for_propagation.cancel();
        service_cancel_for_propagation.cancel();
    });

    let admin_port = run_args.admin_port.unwrap_or(DEFAULT_ADMIN_PORT);
    let watermark_debug = Arc::new(tokio::sync::RwLock::new(http_ingest::WatermarkDebugState {
        policy: "min_active_sources".to_string(),
        ..http_ingest::WatermarkDebugState::default()
    }));
    let cdc_replication_debug = Arc::new(tokio::sync::RwLock::new(
        http_ingest::CdcReplicationDebugState::default(),
    ));
    let admin_health = HttpIngestHealth {
        executor_running: Arc::clone(&executor_running),
        storage_reachable: Arc::clone(&storage_reachable),
        runtime_ready: Arc::clone(&runtime_ready),
        watermark_debug: Some(Arc::clone(&watermark_debug)),
        cdc_replication_debug: Some(Arc::clone(&cdc_replication_debug)),
    };
    let connector_count = connector_specs.len();
    tracing::info!(
        connector_count,
        queue_capacity,
        max_batch,
        max_batch_per_source,
        max_batch_per_connector,
        "resolved ingest execution limits"
    );

    let mut connector_handles: Vec<JoinHandle<()>> = Vec::new();
    let mut connector_queues: Vec<ConnectorQueue> = Vec::new();
    let mut kafka_commit_senders: Vec<watch::Sender<KafkaOffsetCommit>> = Vec::new();
    let mut postgres_cdc_commit_senders: Vec<watch::Sender<PostgresCdcCommit>> = Vec::new();
    let definitions = source_registry.definitions().to_vec();
    let transient_required_columns_by_source = Arc::new(transient_required_columns_by_source);
    let source_id_by_name: HashMap<String, usize> = definitions
        .iter()
        .enumerate()
        .map(|(idx, definition)| (definition.name().to_string(), idx))
        .collect();
    let mut postgres_cdc_runtime_plans_by_connector = HashMap::new();
    for connector in &connector_specs {
        let ConnectorConfig::PostgresCdc {
            connection,
            schema_evolution_policy,
            include_tables,
            ..
        } = &connector.config
        else {
            continue;
        };
        let schema_evolution_policy = schema_evolution_policy
            .as_ref()
            .copied()
            .unwrap_or(CatalogPostgresCdcSchemaEvolutionPolicy::FailFast);
        if let Some(plan) = postgres_cdc_runtime_plan(
            &connector.name,
            connection,
            postgres_schema_evolution_policy_from_catalog(schema_evolution_policy),
            include_tables.as_deref(),
            &source_registry,
            &source_backed_tables,
            &replication_pipelines,
        )
        .await
        .with_context(|| {
            format!(
                "build native Postgres CDC runtime plan for connector '{}'",
                connector.name
            )
        })? {
            metrics::record_postgres_cdc_schema_evolution_policy(
                plan.source_id.as_str(),
                plan.schema_evolution_policy.as_str(),
            );
            postgres_cdc_runtime_plans_by_connector.insert(connector.name.clone(), plan);
        }
    }
    initialize_postgres_cdc_debug_sources(
        &cdc_replication_debug,
        postgres_cdc_runtime_plans_by_connector.values(),
    )
    .await;
    let replication_settings = config
        .as_ref()
        .map(|cfg| cfg.replication.clone())
        .unwrap_or_default();
    let replication_pipeline_runtime = Arc::new(ReplicationPipelineRuntime::new(
        postgres_cdc_runtime_plans_by_connector
            .values()
            .flat_map(|plan| plan.replication_pipelines.iter().cloned()),
        replication_settings,
    )?);
    replication_pipeline_runtime
        .refresh_debug_state(&storage, &cdc_replication_debug)
        .await
        .context("refresh initial CDC replication debug state")?;
    let replayed_replication_records = replication_pipeline_runtime
        .replay_buffered(&storage)
        .await
        .context("replay buffered replication pipeline records")?;
    replication_pipeline_runtime
        .refresh_debug_state(&storage, &cdc_replication_debug)
        .await
        .context("refresh CDC replication debug state after replay")?;
    if replayed_replication_records > 0 {
        tracing::info!(
            records = replayed_replication_records,
            "replayed buffered replication pipeline records during startup"
        );
    }
    let cdc_replication_debug_cancel = service_cancel.clone();
    let cdc_replication_debug_for_refresh = Arc::clone(&cdc_replication_debug);
    let storage_for_replication_debug = storage.clone();
    let replication_pipeline_runtime_for_debug = Arc::clone(&replication_pipeline_runtime);
    let cdc_replication_debug_handle: JoinHandle<()> = tokio::spawn(async move {
        let mut refresh_interval = tokio::time::interval(Duration::from_secs(1));
        refresh_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cdc_replication_debug_cancel.cancelled() => break,
                _ = refresh_interval.tick() => {
                    if let Err(err) = replication_pipeline_runtime_for_debug
                        .refresh_debug_state(
                            &storage_for_replication_debug,
                            &cdc_replication_debug_for_refresh,
                        )
                        .await
                    {
                        tracing::warn!(
                            error = %err,
                            "failed to refresh CDC replication debug state"
                        );
                    }
                }
            }
        }
    });
    let admin_config = HttpAdminConfig {
        host: run_args.http_host.clone(),
        port: admin_port,
        health: admin_health,
        storage_db: Some(db.clone()),
        storage_catalog: Some(storage.clone()),
        replication_runtime: Some(Arc::clone(&replication_pipeline_runtime)),
        materialized_views: Some(Arc::clone(&mv_registry)),
    };
    let admin_cancel = service_cancel.clone();
    let runtime_cancel_for_admin = runtime_cancel.clone();
    let failure_for_admin = Arc::clone(&runtime_failure);
    let admin_handle: JoinHandle<()> = tokio::spawn(async move {
        if let Err(err) = http_ingest::run_admin_server(admin_config, admin_cancel.clone()).await {
            tracing::error!(error = %err, "admin HTTP server failed");
            record_runtime_failure(
                &failure_for_admin,
                format!("admin HTTP server failed: {err}"),
            );
            runtime_cancel_for_admin.cancel();
        }
    });
    let source_names_by_id = Arc::new(
        definitions
            .iter()
            .map(|definition| definition.name().to_string())
            .collect::<Vec<_>>(),
    );
    let decoders_by_source_id = Arc::new(
        definitions
            .iter()
            .map(|definition| {
                all_required_sources.contains(definition.name()).then(|| {
                    SourceRowDecoder::new_with_encoded_required_columns(
                        definition.clone(),
                        transient_required_columns_by_source
                            .get(definition.name())
                            .map(Arc::clone),
                    )
                })
            })
            .collect::<Vec<_>>(),
    );
    let materialized_source_ids = Arc::new(
        definitions
            .iter()
            .map(|definition| all_required_sources.contains(definition.name()))
            .collect::<Vec<_>>(),
    );
    let source_journal_source_ids = Arc::new(
        definitions
            .iter()
            .enumerate()
            .filter_map(|(idx, definition)| {
                source_journal_required_sources
                    .contains(definition.name())
                    .then_some(idx)
            })
            .collect::<Vec<_>>(),
    );
    let cdc_schemas_by_source_id = Arc::new(
        postgres_cdc_runtime_plans_by_connector
            .values()
            .map(|plan| (plan.source_id.clone(), plan.schemas.clone()))
            .collect::<HashMap<_, _>>(),
    );
    let cdc_stateful_table_ids_by_source_id = Arc::new(
        postgres_cdc_runtime_plans_by_connector
            .values()
            .map(|plan| {
                (
                    plan.source_id.clone(),
                    plan.schemas.keys().cloned().collect::<HashSet<_>>(),
                )
            })
            .collect::<HashMap<_, _>>(),
    );
    let (connector_sender, connector_receiver) = core_source::routed_channel(queue_capacity);
    let (cdc_transaction_sender, cdc_transaction_receiver) =
        mpsc::channel::<QueuedCdcTransaction>(queue_capacity);
    let pending_event_counter = core_source::PendingAppendIngestEventCounter::default();
    let (sink_checkpoint_tx, sink_checkpoint_rx) = mpsc::unbounded_channel::<SinkCursor>();
    let sink_resume_cursors: HashMap<String, SinkCursor> = initial_sink_cursors
        .iter()
        .cloned()
        .map(|cursor| (cursor.sink.clone(), cursor))
        .collect();
    let recovered_kafka_offsets = recovered_tick_commit
        .as_ref()
        .map(|commit| commit.kafka_offsets.clone())
        .unwrap_or_default();

    for (connector_id, connector) in connector_specs.into_iter().enumerate() {
        let sender = core_source::routed_sender(
            connector_id,
            connector_sender.clone(),
            pending_event_counter.clone(),
        );
        let postgres_cdc_runtime_plan = postgres_cdc_runtime_plans_by_connector
            .get(&connector.name)
            .cloned();
        connector_queues.push(ConnectorQueue::new(connector_id, connector.name.clone()));
        let cancel = ingest_cancel.clone();
        let runtime_cancel = runtime_cancel.clone();
        let failure_state = Arc::clone(&runtime_failure);
        match connector.config {
            ConnectorConfig::Http {
                host,
                port,
                default_source,
                ..
            } => {
                let config = HttpIngestConfig {
                    host: host.unwrap_or_else(|| run_args.http_host.clone()),
                    port,
                    default_source,
                    health: Some(HttpIngestHealth {
                        executor_running: Arc::clone(&executor_running),
                        storage_reachable: Arc::clone(&storage_reachable),
                        runtime_ready: Arc::clone(&runtime_ready),
                        watermark_debug: Some(Arc::clone(&watermark_debug)),
                        cdc_replication_debug: Some(Arc::clone(&cdc_replication_debug)),
                    }),
                };
                let failure_state = Arc::clone(&failure_state);
                connector_handles.push(tokio::spawn(async move {
                    if let Err(err) =
                        http_ingest::run_http_ingest(config, sender, cancel.clone()).await
                    {
                        tracing::error!(error = %err, "HTTP ingest server failed");
                        record_runtime_failure(
                            &failure_state,
                            format!("HTTP ingest connector failed: {err}"),
                        );
                        runtime_cancel.cancel();
                    }
                }));
            }
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
                let group_id = group_id.unwrap_or_else(|| run_args.kafka_group_id.clone());
                let poll_timeout = Duration::from_millis(poll_ms.unwrap_or(run_args.kafka_poll_ms));
                let max_messages_per_tick =
                    max_messages_per_tick.unwrap_or(run_args.kafka_max_messages);
                let default_source_id = default_source
                    .as_deref()
                    .and_then(|source| source_id_by_name.get(source).copied());
                let connector_has_recovered_offsets = recovered_kafka_offsets
                    .iter()
                    .any(|offset| topics.iter().any(|topic| topic == &offset.topic));
                let should_replay_from_kafka = connector_has_recovered_offsets
                    && (default_source
                        .as_ref()
                        .is_some_and(|source| source_journal_skipped_sources.contains(source))
                        || default_source.is_none() && !source_journal_skipped_sources.is_empty());
                let resume_from_offsets = if should_replay_from_kafka {
                    recovered_kafka_offsets
                        .iter()
                        .filter(|offset| topics.iter().any(|topic| topic == &offset.topic))
                        .map(|offset| KafkaTopicPartitionOffset {
                            topic: offset.topic.clone(),
                            partition: offset.partition,
                            offset: offset.offset,
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                let (commit_tx, commit_rx) = watch::channel(KafkaOffsetCommit::default());
                kafka_commit_senders.push(commit_tx);
                let definitions = definitions.clone();
                let transient_required_columns_by_source =
                    Arc::clone(&transient_required_columns_by_source);
                let failure_state = Arc::clone(&failure_state);
                connector_handles.push(tokio::spawn(async move {
                    let config = KafkaConnectorConfig {
                        brokers,
                        topics,
                        group_id,
                        default_source,
                        default_source_id,
                        poll_timeout,
                        max_messages_per_tick,
                        message_format: format,
                        commit_offsets_rx: Some(commit_rx),
                        resume_from_offsets,
                    };
                    let mut connector = match KafkaConnector::new(
                        config,
                        definitions,
                        transient_required_columns_by_source
                            .iter()
                            .map(|(source, columns)| (source.clone(), Arc::clone(columns)))
                            .collect(),
                    ) {
                        Ok(connector) => connector,
                        Err(err) => {
                            tracing::error!(error = %err, "Kafka connector config invalid");
                            record_runtime_failure(
                                &failure_state,
                                format!("Kafka connector config invalid: {err}"),
                            );
                            runtime_cancel.cancel();
                            return;
                        }
                    };
                    let ctx = ConnectorContext::new(sender);
                    if let Err(err) = run_connector(&mut connector, &ctx, cancel.clone()).await {
                        tracing::error!(error = %err, "Kafka connector failed");
                        record_runtime_failure(
                            &failure_state,
                            format!("Kafka connector failed: {err}"),
                        );
                        runtime_cancel.cancel();
                    }
                }));
            }
            ConnectorConfig::File {
                path,
                default_source,
                ..
            } => {
                let definitions = definitions.clone();
                let failure_state = Arc::clone(&failure_state);
                connector_handles.push(tokio::spawn(async move {
                    let config = FileConnectorConfig {
                        path: path.into(),
                        default_source,
                    };
                    let mut connector = FileConnector::new(config, definitions);
                    let ctx = ConnectorContext::new(sender);
                    if let Err(err) = run_connector(&mut connector, &ctx, cancel.clone()).await {
                        tracing::error!(error = %err, "File connector failed");
                        record_runtime_failure(
                            &failure_state,
                            format!("File connector failed: {err}"),
                        );
                        runtime_cancel.cancel();
                    }
                }));
            }
            ConnectorConfig::Generator {
                events_per_second,
                max_events,
                ..
            } => {
                let events_per_second = events_per_second.unwrap_or(run_args.events_per_second);
                let max_events = max_events.or(run_args.max_events);
                let generator_config = floe_node_core::generator::Config {
                    events_per_second,
                    max_events,
                };
                let failure_state = Arc::clone(&failure_state);
                connector_handles.push(tokio::spawn(async move {
                    let mut connector =
                        match floe_node_core::generator::NexmarkConnector::new(generator_config) {
                            Ok(connector) => connector,
                            Err(err) => {
                                tracing::error!(error = %err, "Nexmark connector config invalid");
                                record_runtime_failure(
                                    &failure_state,
                                    format!("Nexmark connector config invalid: {err}"),
                                );
                                runtime_cancel.cancel();
                                return;
                            }
                        };
                    let ctx = ConnectorContext::new(sender);
                    if let Err(err) = run_connector(&mut connector, &ctx, cancel.clone()).await {
                        tracing::error!(error = %err, "Nexmark connector failed");
                        record_runtime_failure(
                            &failure_state,
                            format!("Nexmark connector failed: {err}"),
                        );
                        runtime_cancel.cancel();
                    }
                }));
            }
            ConnectorConfig::ObjectStore {
                url,
                default_source,
                ..
            } => {
                let definitions = definitions.clone();
                let failure_state = Arc::clone(&failure_state);
                connector_handles.push(tokio::spawn(async move {
                    let config = ObjectStoreConnectorConfig {
                        url,
                        default_source,
                    };
                    let mut connector = ObjectStoreConnector::new(config, definitions);
                    let ctx = ConnectorContext::new(sender);
                    if let Err(err) = run_connector(&mut connector, &ctx, cancel.clone()).await {
                        tracing::error!(error = %err, "Object store connector failed");
                        record_runtime_failure(
                            &failure_state,
                            format!("Object store connector failed: {err}"),
                        );
                        runtime_cancel.cancel();
                    }
                }));
            }
            ConnectorConfig::PostgresCdc {
                connection,
                slot,
                publication,
                auto_create_slot,
                auto_create_publication,
                ..
            } => {
                let publication = publication.unwrap_or_else(default_postgres_publication);
                let Some(runtime_plan) = postgres_cdc_runtime_plan else {
                    tracing::error!(
                        connector = %connector.name,
                        "Postgres CDC connector is not bound to a CDC table or replication pipeline"
                    );
                    record_runtime_failure(
                        &failure_state,
                        format!(
                            "Postgres CDC connector '{}' must be used by CREATE TABLE ... FROM or CREATE REPLICATION PIPELINE",
                            connector.name
                        ),
                    );
                    runtime_cancel.cancel();
                    continue;
                };
                let (commit_tx, commit_rx) = watch::channel(PostgresCdcCommit::default());
                postgres_cdc_commit_senders.push(commit_tx);
                let failure_state = Arc::clone(&failure_state);
                let config = PostgresCdcSourceConfig {
                    connection_string: connection,
                    slot,
                    publication,
                    auto_create_slot: auto_create_slot.unwrap_or(true),
                    auto_create_publication: auto_create_publication.unwrap_or(true),
                    commit_lsn_rx: Some(commit_rx),
                };
                tracing::info!(
                    connector = %connector.name,
                    source = %runtime_plan.source_id.as_str(),
                    tables = runtime_plan.schemas.len(),
                    schema_evolution_policy = ?runtime_plan.schema_evolution_policy,
                    "using native Postgres CDC table runtime"
                );
                let transaction_sender = cdc_transaction_sender.clone();
                let table_store = cdc_table_store.clone();
                let cdc_replication_debug = Arc::clone(&cdc_replication_debug);
                let snapshot_settings = postgres_cdc_settings.snapshot;
                let reconnect_settings = postgres_cdc_settings.reconnect;
                connector_handles.push(tokio::spawn(async move {
                    if let Err(err) = run_native_postgres_cdc_connector(
                        config,
                        runtime_plan,
                        snapshot_settings,
                        reconnect_settings,
                        table_store,
                        cdc_replication_debug,
                        transaction_sender,
                        cancel.clone(),
                    )
                    .await
                    {
                        if cancel.is_cancelled() {
                            tracing::debug!(
                                error = %err,
                                "native Postgres CDC connector stopped during shutdown"
                            );
                            return;
                        }
                        tracing::error!(error = %err, "native Postgres CDC connector failed");
                        record_runtime_failure(
                            &failure_state,
                            format!("native Postgres CDC connector failed: {err}"),
                        );
                        runtime_cancel.cancel();
                    }
                }));
            }
        }
    }
    drop(connector_sender);
    drop(cdc_transaction_sender);
    let outer_for_task = Arc::clone(&outer_registry);
    let cdc_table_store_for_task = cdc_table_store.clone();
    let cdc_schemas_by_source_id_for_task = Arc::clone(&cdc_schemas_by_source_id);
    let cdc_stateful_table_ids_by_source_id_for_task =
        Arc::clone(&cdc_stateful_table_ids_by_source_id);
    let decoders_by_source_id_for_task = Arc::clone(&decoders_by_source_id);
    let materialized_source_ids_for_task = Arc::clone(&materialized_source_ids);
    let source_names_by_id_for_task = Arc::clone(&source_names_by_id);
    let watermark_for_task = Arc::clone(&event_watermark);
    let mv_for_task = Arc::clone(&mv_registry);
    let kafka_commit_senders_for_task = kafka_commit_senders;
    let postgres_cdc_commit_senders_for_task = postgres_cdc_commit_senders;
    let mut sink_checkpoint_rx_for_task = sink_checkpoint_rx;
    const MAX_SINK_CURSOR_UPDATES_PER_ITER: usize = 4096;
    let watermark_debug_for_task = Arc::clone(&watermark_debug);
    let cdc_replication_debug_for_task = Arc::clone(&cdc_replication_debug);
    let executor_running_for_task = Arc::clone(&executor_running);
    let failure_for_executor = Arc::clone(&runtime_failure);
    let source_journal_source_ids_for_task = Arc::clone(&source_journal_source_ids);
    let source_id_by_name_for_task = source_id_by_name;
    let storage_for_replication_task = storage.clone();
    let replication_pipeline_runtime_for_task = Arc::clone(&replication_pipeline_runtime);
    let mut connector_receiver_for_task = connector_receiver;
    let mut cdc_transaction_receiver_for_task = cdc_transaction_receiver;
    let tracked_mv_names: Vec<String> = planned_materialized_views
        .iter()
        .map(|plan| plan.definition().name().to_string())
        .collect();
    let executor_cancel = runtime_cancel.clone();
    let executor_handle: JoinHandle<()> = tokio::spawn(async move {
        let mut connector_queues = connector_queues;
        let mut cdc_transaction_queue = VecDeque::new();
        let mut checkpoint_manager = checkpoint_manager;
        let mut next_connector = 0usize;
        let mut epoch: u64 = 0;
        let mut last_mv_versions: HashMap<String, u64> = HashMap::new();
        let mut committed_source_offsets: HashMap<(String, u32), u64> = HashMap::new();
        let mut latest_source_offsets: HashMap<(String, u32), u64> = HashMap::new();
        let mut committed_kafka_offsets: HashMap<(Arc<str>, i32), i64> = HashMap::new();
        let mut committed_postgres_lsns: HashMap<String, (u64, String)> = HashMap::new();
        let mut mv_last_update_at_ms: HashMap<String, u64> = tracked_mv_names
            .iter()
            .map(|view| (view.clone(), current_unix_time_ms()))
            .collect();
        let mut last_checkpoint_commit_at = Instant::now();
        let mut source_watermarks: HashMap<String, i64> = HashMap::new();
        let mut source_last_seen_at: HashMap<String, Instant> = HashMap::new();
        let watermark_idle_timeout = Duration::from_millis(watermark_idle_source_ms);
        let executor_loop_started = Instant::now();
        let mut first_nonempty_decode_logged = false;
        let mut first_tick_commit_logged = false;
        if let Some(existing_commit) = checkpoint_manager.latest_tick_commit() {
            metrics::record_last_committed_tick(existing_commit.tick_id);
            epoch = existing_commit.tick_id;
            let restored_watermark = i64::try_from(existing_commit.frontier).unwrap_or(i64::MAX);
            watermark_for_task.store(restored_watermark.max(0), Ordering::Relaxed);
            for mv_version in &existing_commit.mv_versions {
                last_mv_versions.insert(mv_version.view.clone(), mv_version.version);
                mv_last_update_at_ms.insert(
                    mv_version.view.clone(),
                    existing_commit.committed_at_unix_ms,
                );
            }
            for offset in &existing_commit.source_offsets {
                let key = (offset.source.clone(), offset.partition);
                committed_source_offsets.insert(key.clone(), offset.offset);
                latest_source_offsets.insert(key, offset.offset);
                metrics::record_source_offset_lag(&offset.source, offset.partition, 0);
            }
            for offset in &existing_commit.kafka_offsets {
                committed_kafka_offsets.insert(
                    (Arc::<str>::from(offset.topic.as_str()), offset.partition),
                    offset.offset,
                );
            }
            let now_ms = current_unix_time_ms();
            let age_secs = now_ms.saturating_sub(existing_commit.committed_at_unix_ms) / 1_000;
            metrics::record_checkpoint_age_seconds(age_secs);
            metrics::record_watermark_lag_ms(now_ms.saturating_sub(existing_commit.frontier));
            metrics::record_global_watermark_ms(
                i64::try_from(existing_commit.frontier).unwrap_or(i64::MAX),
            );
            record_mv_freshness_metrics(&mv_last_update_at_ms, now_ms);
        }
        'executor: loop {
            for _ in 0..MAX_SINK_CURSOR_UPDATES_PER_ITER {
                match sink_checkpoint_rx_for_task.try_recv() {
                    Ok(cursor) => {
                        checkpoint_manager.update_sink_cursor(
                            &cursor.sink,
                            &cursor.mv_name,
                            cursor.last_emitted_mv_version,
                            cursor.row_index,
                        );
                    }
                    Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
                }
            }
            let now_ms = current_unix_time_ms();
            metrics::record_checkpoint_age_seconds(last_checkpoint_commit_at.elapsed().as_secs());
            record_mv_freshness_metrics(&mv_last_update_at_ms, now_ms);
            if executor_cancel.is_cancelled() {
                break;
            }
            if connector_queues.is_empty() && cdc_transaction_queue.is_empty() {
                break;
            }
            if connector_queues
                .iter()
                .all(|queue| queue.pending.is_empty())
                && cdc_transaction_queue.is_empty()
            {
                let has_events = loop {
                    let connector_receiver_active = !connector_receiver_for_task.is_closed();
                    let cdc_receiver_active = !cdc_schemas_by_source_id_for_task.is_empty()
                        && !cdc_transaction_receiver_for_task.is_closed();
                    match (connector_receiver_active, cdc_receiver_active) {
                        (false, false) => break false,
                        (true, false) => {
                            break tokio::select! {
                                _ = executor_cancel.cancelled() => false,
                                has_events = recv_from_ready(&mut connector_receiver_for_task, &mut connector_queues) => has_events,
                            };
                        }
                        (false, true) => {
                            break tokio::select! {
                                _ = executor_cancel.cancelled() => false,
                                has_events = recv_cdc_from_ready(
                                    &mut cdc_transaction_receiver_for_task,
                                    &mut cdc_transaction_queue,
                                ) => has_events,
                            };
                        }
                        (true, true) => {
                            let has_events = tokio::select! {
                                _ = executor_cancel.cancelled() => false,
                                has_events = recv_cdc_from_ready(
                                    &mut cdc_transaction_receiver_for_task,
                                    &mut cdc_transaction_queue,
                                ) => has_events,
                                has_events = recv_from_ready(&mut connector_receiver_for_task, &mut connector_queues) => has_events,
                            };
                            if has_events {
                                break true;
                            }
                        }
                    }
                };
                if !has_events {
                    break;
                }
            }
            drain_ready(&mut connector_receiver_for_task, &mut connector_queues);
            if !cdc_schemas_by_source_id_for_task.is_empty() {
                drain_cdc_ready(
                    &mut cdc_transaction_receiver_for_task,
                    &mut cdc_transaction_queue,
                );
            }

            let pending_epoch = epoch.saturating_add(1);
            let source_count = source_names_by_id_for_task.len();
            let decode_start = Instant::now();
            let mut tick_commit_acks = Vec::new();
            let mut decoded_counts = vec![0usize; source_count];
            let mut tick_source_offsets = vec![None::<HashMap<u32, u64>>; source_count];
            let mut tick_kafka_offsets: HashMap<(Arc<str>, i32), i64> = HashMap::new();
            let mut tick_postgres_lsns: HashMap<String, (u64, String)> = HashMap::new();
            let mut tick_postgres_sources: HashMap<String, String> = HashMap::new();
            let mut tick_postgres_table_lsns: Vec<(String, String, String, u64)> = Vec::new();
            let mut tick_source_max_event_ts = vec![None::<i64>; source_count];
            let mut encoded_batches_by_source = vec![Vec::new(); source_count];
            let mut commit_acks_by_source = vec![Vec::new(); source_count];
            let mut cdc_staged_writes = None::<WriteBatch>;
            let mut per_connector_counts = vec![0usize; connector_queues.len()];
            let batch_len: usize;
            let mut decoded_rows_len = 0usize;

            if let Some(cdc_transaction) = cdc_transaction_queue.pop_front() {
                batch_len = 1;
                tracing::debug!(
                    source = %cdc_transaction.source_id.as_str(),
                    slot = %cdc_transaction.slot,
                    change_batches = cdc_transaction.transaction.change_batches().len(),
                    changes = cdc_transaction
                        .transaction
                        .change_batches()
                        .iter()
                        .map(ChangeBatch::change_count)
                        .sum::<usize>(),
                    commit_position = ?cdc_transaction.transaction.commit_position(),
                    "executor applying native CDC transaction"
                );
                let Some(schemas) =
                    cdc_schemas_by_source_id_for_task.get(&cdc_transaction.source_id)
                else {
                    let message = format!(
                        "received native CDC transaction for unknown source '{}'",
                        cdc_transaction.source_id.as_str()
                    );
                    tracing::error!("{message}");
                    record_runtime_failure(&failure_for_executor, message);
                    executor_cancel.cancel();
                    break 'executor;
                };
                let cdc_transaction_batch = match cdc_table_store_for_task
                    .complete_unchanged_toast(schemas, &cdc_transaction.transaction)
                    .await
                {
                    Ok(transaction) => transaction,
                    Err(err) => {
                        let message = format!(
                            "failed to complete native CDC unchanged TOAST values for source '{}': {err}",
                            cdc_transaction.source_id.as_str()
                        );
                        tracing::error!(error = %err, "{message}");
                        record_runtime_failure(&failure_for_executor, message);
                        executor_cancel.cancel();
                        break 'executor;
                    }
                };
                let stateful_table_ids = cdc_stateful_table_ids_by_source_id_for_task
                    .get(&cdc_transaction.source_id)
                    .cloned()
                    .unwrap_or_default();
                let stateful_transaction = match materialized_transaction(
                    &cdc_transaction.source_id,
                    &stateful_table_ids,
                    &cdc_transaction_batch,
                ) {
                    Ok(transaction) => transaction,
                    Err(err) => {
                        let message = format!(
                            "failed to split native CDC state transaction for source '{}': {err}",
                            cdc_transaction.source_id.as_str()
                        );
                        tracing::error!(error = %err, "{message}");
                        record_runtime_failure(&failure_for_executor, message);
                        executor_cancel.cancel();
                        break 'executor;
                    }
                };
                let mut staged_writes = WriteBatch::new();
                let mut apply_result = None;
                if let Some(transaction) = stateful_transaction.as_ref() {
                    apply_result = match cdc_table_store_for_task
                        .stage_transaction(schemas, transaction, &mut staged_writes)
                        .await
                    {
                        Ok(result) => Some(result),
                        Err(err) => {
                            let message = format!(
                                "failed to stage native CDC transaction for source '{}': {err}",
                                cdc_transaction.source_id.as_str()
                            );
                            tracing::error!(error = %err, "{message}");
                            record_runtime_failure(&failure_for_executor, message);
                            executor_cancel.cancel();
                            break 'executor;
                        }
                    };
                }
                let pipeline_records = if replication_pipeline_runtime_for_task
                    .has_pipelines_for_source(&cdc_transaction.source_id)
                {
                    match replication_pipeline_runtime_for_task
                        .run_transaction(
                            &cdc_transaction.source_id,
                            schemas,
                            &cdc_transaction_batch,
                            Some(&storage_for_replication_task),
                        )
                        .await
                    {
                        Ok(records) => records,
                        Err(err) => {
                            let message = format!(
                                "failed to run replication pipelines for source '{}': {err}",
                                cdc_transaction.source_id.as_str()
                            );
                            tracing::error!(error = %err, "{message}");
                            record_runtime_failure(&failure_for_executor, message);
                            executor_cancel.cancel();
                            break 'executor;
                        }
                    }
                } else {
                    0
                };
                let feedback_position = apply_result
                    .as_ref()
                    .map(|result| result.checkpoint().position())
                    .unwrap_or_else(|| cdc_transaction_batch.commit_position());
                let feedback_lsn = match PostgresLsn::from_source_position(feedback_position) {
                    Ok(lsn) => lsn,
                    Err(err) => {
                        let message = format!(
                            "failed to derive native CDC feedback LSN for source '{}': {err}",
                            cdc_transaction.source_id.as_str()
                        );
                        tracing::error!(error = %err, "{message}");
                        record_runtime_failure(&failure_for_executor, message);
                        executor_cancel.cancel();
                        break 'executor;
                    }
                };
                if stateful_transaction.is_none() && pipeline_records > 0 {
                    let checkpoint = pipeline_checkpoint_from_transaction(&cdc_transaction_batch);
                    if let Err(err) = cdc_table_store_for_task
                        .commit_checkpoint(&checkpoint)
                        .await
                    {
                        let message = format!(
                            "failed to commit replication-only CDC checkpoint for source '{}': {err}",
                            cdc_transaction.source_id.as_str()
                        );
                        tracing::error!(error = %err, "{message}");
                        record_runtime_failure(&failure_for_executor, message);
                        executor_cancel.cancel();
                        break 'executor;
                    }
                }
                tick_postgres_lsns.insert(
                    cdc_transaction.slot.clone(),
                    (feedback_lsn.as_u64(), feedback_lsn.to_pg_string()),
                );
                tick_postgres_sources.insert(
                    cdc_transaction.slot.clone(),
                    cdc_transaction.source_id.as_str().to_string(),
                );
                for change_batch in cdc_transaction_batch.change_batches() {
                    tick_postgres_table_lsns.push((
                        cdc_transaction.source_id.as_str().to_string(),
                        cdc_transaction.slot.clone(),
                        change_batch.table_id().as_str().to_string(),
                        feedback_lsn.as_u64(),
                    ));
                }
                if stateful_transaction.is_none() {
                    if pipeline_records > 0 {
                        record_postgres_cdc_lsn_progress(
                            &mut committed_postgres_lsns,
                            &tick_postgres_lsns,
                            &tick_postgres_sources,
                            &tick_postgres_table_lsns,
                            &cdc_replication_debug_for_task,
                        );
                        notify_postgres_cdc_commit_senders(
                            epoch,
                            &committed_postgres_lsns,
                            &tick_postgres_lsns,
                            &postgres_cdc_commit_senders_for_task,
                        );
                        metrics::record_checkpoint_age_seconds(0);
                        last_checkpoint_commit_at = Instant::now();
                    }
                    continue;
                }
                let apply_result = apply_result
                    .as_ref()
                    .expect("materialized transaction should produce apply result");
                for table_deltas in apply_result.table_deltas() {
                    let source_name = table_deltas.table_id().as_str();
                    let Some(source_id) = source_id_by_name_for_task.get(source_name).copied()
                    else {
                        tracing::debug!(
                            source = %source_name,
                            "dropping native CDC state delta for table outside DBSP source registry"
                        );
                        continue;
                    };
                    if !materialized_source_ids_for_task
                        .get(source_id)
                        .copied()
                        .unwrap_or(false)
                    {
                        tracing::debug!(
                            source = %source_name,
                            "dropping native CDC deltas for source outside active materialization set"
                        );
                        continue;
                    }
                    let Some(decoder) = decoders_by_source_id_for_task
                        .get(source_id)
                        .and_then(|decoder| decoder.as_ref())
                    else {
                        let message =
                            format!("received CDC deltas for unknown source '{source_name}'");
                        tracing::error!(source = %source_name, "{message}");
                        record_runtime_failure(&failure_for_executor, message);
                        executor_cancel.cancel();
                        break 'executor;
                    };
                    match encode_cdc_table_deltas(decoder, table_deltas) {
                        Ok(mut encoded) => {
                            decoded_counts[source_id] =
                                decoded_counts[source_id].saturating_add(encoded.len());
                            decoded_rows_len = decoded_rows_len.saturating_add(encoded.len());
                            encoded_batches_by_source[source_id].append(&mut encoded);
                        }
                        Err(err) => {
                            let message = format!(
                                "failed to encode native CDC deltas for source '{source_name}': {err}"
                            );
                            tracing::error!(source = %source_name, error = %err, "{message}");
                            record_runtime_failure(&failure_for_executor, message);
                            executor_cancel.cancel();
                            break 'executor;
                        }
                    }
                }
                if !apply_result.already_committed() {
                    cdc_staged_writes = Some(staged_writes);
                }
            } else {
                let selection = build_batch(
                    &mut connector_queues,
                    &source_id_by_name_for_task,
                    source_id_by_name_for_task.len(),
                    next_connector,
                    max_batch,
                    max_batch_per_source,
                    max_batch_per_connector,
                    &pending_event_counter,
                );
                let BatchSelection {
                    batch,
                    per_connector_counts: selected_per_connector_counts,
                } = selection;
                per_connector_counts = selected_per_connector_counts;

                if batch.is_empty() {
                    continue;
                }

                next_connector = if connector_queues.is_empty() {
                    0
                } else {
                    (next_connector + 1) % connector_queues.len()
                };

                batch_len = batch.len();
                let mut encoded_rows = Vec::with_capacity(batch_len);
                let decode_span = tracing::debug_span!(
                    "ingest_decode",
                    epoch = pending_epoch,
                    raw_batch_size = batch_len
                );
                let _decode_guard = decode_span.enter();
                for SelectedAppendIngestEvent {
                    source_id,
                    mut event,
                    commit_ack,
                } in batch
                {
                    let Some(source_id) = source_id else {
                        let source_name = event.source().to_string();
                        tracing::debug!(
                            source = %source_name,
                            "dropping event for unknown source"
                        );
                        if let Some(ack) = commit_ack {
                            ack.record_failed(format!("unknown source '{source_name}'"))
                                .await;
                        }
                        continue;
                    };
                    let source_name = source_names_by_id_for_task[source_id].as_str();
                    if let Some((partition, offset)) = event_fast_resume_offset(&event)
                        .or_else(|| event_resume_offset(event.resume_token()))
                    {
                        let entry = tick_source_offsets[source_id]
                            .get_or_insert_with(HashMap::new)
                            .entry(partition)
                            .or_insert(0);
                        *entry = (*entry).max(offset);
                    }
                    if let Some((topic, partition, offset)) = event_fast_kafka_offset(&event)
                        .or_else(|| event_kafka_offset(event.resume_token()))
                    {
                        let entry = tick_kafka_offsets.entry((topic, partition)).or_insert(0);
                        *entry = (*entry).max(offset);
                    }
                    if !materialized_source_ids_for_task
                        .get(source_id)
                        .copied()
                        .unwrap_or(false)
                    {
                        tracing::debug!(
                            source = %source_name,
                            "dropping event for source outside active materialization set"
                        );
                        if let Some(ack) = commit_ack {
                            ack.record_failed(format!(
                                "source '{source_name}' is outside the active materialization set"
                            ))
                            .await;
                        }
                        continue;
                    }
                    let Some(decoder) = decoders_by_source_id_for_task
                        .get(source_id)
                        .and_then(|decoder| decoder.as_ref())
                    else {
                        let message = format!("received event for unknown source '{source_name}'");
                        tracing::error!(source = %source_name, "{message}");
                        if let Some(ack) = commit_ack {
                            ack.record_failed(message.clone()).await;
                        }
                        record_runtime_failure(&failure_for_executor, message);
                        executor_cancel.cancel();
                        break 'executor;
                    };
                    let event_ts = if let Some(preencoded_row_key) = event.take_preencoded_row_key()
                    {
                        encoded_rows.push((source_id, preencoded_row_key, commit_ack));
                        None
                    } else {
                        match decoder.encode_row_key(&event) {
                            Ok((encoded, event_ts)) => {
                                encoded_rows.push((source_id, encoded, commit_ack));
                                event_ts
                            }
                            Err(err) => {
                                tracing::warn!(
                                    source = %source_name,
                                    error = %err,
                                    "failed to encode append ingest event"
                                );
                                if let Some(ack) = commit_ack {
                                    ack.record_failed(format!(
                                        "failed to encode append ingest event for '{source_name}': {err}"
                                    ))
                                    .await;
                                }
                                continue;
                            }
                        }
                    };
                    // Prefer row-derived event time (from decoded timestamp columns) when available.
                    // Connector-level event_time_ms is a fallback for sources without row timestamps.
                    let event_ts = event_ts.or(event.event_time_ms());
                    if let Some(ts) = event_ts {
                        let ts_i64 = i64::try_from(ts).unwrap_or(i64::MAX);
                        let entry = tick_source_max_event_ts[source_id].get_or_insert(i64::MIN);
                        *entry = (*entry).max(ts_i64);
                    }
                    decoded_counts[source_id] = decoded_counts[source_id].saturating_add(1);
                }

                if encoded_rows.is_empty() {
                    continue;
                }

                decoded_rows_len = encoded_rows.len();
                for (source_id, encoded, commit_ack) in encoded_rows {
                    encoded_batches_by_source[source_id].push((encoded, 1));
                    if let Some(ack) = commit_ack {
                        commit_acks_by_source[source_id].push(ack);
                    }
                }
            }
            let decode_latency_ms = decode_start.elapsed().as_millis() as u64;
            metrics::observe_decode_latency_ms(decode_latency_ms);
            metrics::observe_tick_phase_latency_ms("decode", decode_latency_ms);
            if !first_nonempty_decode_logged {
                first_nonempty_decode_logged = true;
                tracing::info!(
                    epoch = pending_epoch,
                    batch_size = batch_len,
                    decoded_rows = decoded_rows_len,
                    decode_latency_ms,
                    time_to_first_nonempty_decode_ms =
                        executor_loop_started.elapsed().as_millis() as u64,
                    "executor decoded first non-empty ingest batch"
                );
            }
            tracing::debug!(
                decoded_rows = decoded_rows_len,
                latency_ms = decode_latency_ms,
                "decoded ingest batch"
            );

            if decoded_rows_len == 0 {
                // Replication-only CDC tables still need their staged CDC state and LSN feedback
                // committed even when no DBSP source rows are emitted.
                if !tick_postgres_lsns.is_empty() || cdc_staged_writes.is_some() {
                    epoch = pending_epoch;
                    let mv_versions =
                        collect_mv_versions_for_commit(&mv_for_task, &mut last_mv_versions);
                    let frontier = watermark_for_task
                        .load(Ordering::Relaxed)
                        .max(0)
                        .try_into()
                        .unwrap_or(0_u64);
                    let tick_commit = build_tick_commit_for_checkpoint(
                        epoch,
                        frontier,
                        &checkpoint_manager,
                        &mv_versions,
                        &committed_kafka_offsets,
                    );
                    let checkpoint_write_start = Instant::now();
                    let checkpoint_result = if let Some(staged_writes) = cdc_staged_writes {
                        checkpoint_manager
                            .persist_tick_commit_with_source_batches_and_staged_writes(
                                tick_commit,
                                &[],
                                staged_writes,
                            )
                            .await
                    } else {
                        checkpoint_manager
                            .persist_tick_commit_with_source_batches(tick_commit, &[])
                            .await
                    };
                    if let Err(err) = checkpoint_result {
                        metrics::observe_tick_phase_latency_ms(
                            "checkpoint_write",
                            checkpoint_write_start.elapsed().as_millis() as u64,
                        );
                        tracing::error!(
                            epoch,
                            error = %err,
                            "failed to persist CDC-only tick commit"
                        );
                        record_runtime_failure(
                            &failure_for_executor,
                            format!("failed to persist CDC-only tick commit {epoch}: {err}"),
                        );
                        executor_cancel.cancel();
                        break 'executor;
                    }
                    metrics::observe_tick_phase_latency_ms(
                        "checkpoint_write",
                        checkpoint_write_start.elapsed().as_millis() as u64,
                    );
                    record_postgres_cdc_lsn_progress(
                        &mut committed_postgres_lsns,
                        &tick_postgres_lsns,
                        &tick_postgres_sources,
                        &tick_postgres_table_lsns,
                        &cdc_replication_debug_for_task,
                    );
                    notify_postgres_cdc_commit_senders(
                        epoch,
                        &committed_postgres_lsns,
                        &tick_postgres_lsns,
                        &postgres_cdc_commit_senders_for_task,
                    );
                    for mv_version in &mv_versions {
                        mv_last_update_at_ms
                            .insert(mv_version.view.clone(), current_unix_time_ms());
                    }
                    metrics::record_last_committed_tick(epoch);
                    metrics::record_checkpoint_age_seconds(0);
                    last_checkpoint_commit_at = Instant::now();
                }
                continue;
            }
            let mut registry = outer_for_task.lock().await;
            let mut changed = false;
            for (source_id, encoded_batch) in encoded_batches_by_source.into_iter().enumerate() {
                if encoded_batch.is_empty() {
                    continue;
                }
                let source_name = source_names_by_id_for_task[source_id].as_str();
                let Some(writer) = registry.writer_mut(&source_name) else {
                    tracing::warn!(
                        source = %source_name,
                        rows = encoded_batch.len(),
                        "no writer for source, skipping encoded row batch"
                    );
                    continue;
                };
                if let Err(err) = writer.append_encoded_batch(encoded_batch) {
                    tracing::error!(
                        source = %source_name,
                        error = %err,
                        "failed to append encoded row batch"
                    );
                    for ack in commit_acks_by_source[source_id].drain(..) {
                        ack.record_failed(format!(
                            "failed to append encoded row batch for '{source_name}': {err}"
                        ))
                        .await;
                    }
                    continue;
                }
                tick_commit_acks.append(&mut commit_acks_by_source[source_id]);
                changed = true;
            }

            let mut source_journal_batches = Vec::new();
            for &source_id in source_journal_source_ids_for_task.iter() {
                let source_name = source_names_by_id_for_task[source_id].as_str();
                let Some(writer) = registry.writer_mut(source_name) else {
                    continue;
                };
                let Some(batch) = writer
                    .pending_transient_batch(i64::try_from(pending_epoch).unwrap_or(i64::MAX))
                else {
                    continue;
                };
                source_journal_batches.push((
                    source_id,
                    tick_source_max_event_ts[source_id],
                    batch.deltas,
                ));
            }
            drop(registry);

            if !changed {
                continue;
            }

            epoch = pending_epoch;
            let now_instant = Instant::now();
            for (source_id, max_event_ts) in tick_source_max_event_ts.iter().enumerate() {
                let Some(max_event_ts) = *max_event_ts else {
                    continue;
                };
                let source = source_names_by_id_for_task[source_id].clone();
                let watermark_entry = source_watermarks.entry(source.clone()).or_insert(i64::MIN);
                *watermark_entry = (*watermark_entry).max(max_event_ts);
                metrics::record_source_watermark_ms(&source, *watermark_entry);
                source_last_seen_at.insert(source, now_instant);
            }
            let prev_watermark = watermark_for_task.load(Ordering::Relaxed);
            let global_candidate = compute_global_watermark(
                &source_watermarks,
                &source_last_seen_at,
                now_instant,
                watermark_idle_timeout,
            );
            let next_watermark = advance_global_watermark(prev_watermark, global_candidate);
            let tick_start = Instant::now();
            let tick_span = tracing::info_span!(
                "connector_tick",
                epoch,
                watermark = watermark_for_task.load(Ordering::Relaxed),
            );
            let _tick_guard = tick_span.enter();
            if epoch <= 8 || epoch % 128 == 0 {
                tracing::info!(
                    epoch,
                    batch_size = batch_len,
                    decoded_rows = decoded_rows_len,
                    "tick begin"
                );
            }
            if pre_tick_commit_delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(pre_tick_commit_delay_ms)).await;
            }
            // Advance frontier for all sources this epoch, even if they had no rows.
            let mut registry = outer_for_task.lock().await;
            let tick_all_start = Instant::now();
            if let Err(err) = registry
                .tick_all_with_version(i64::try_from(epoch).unwrap_or(i64::MAX))
                .await
            {
                metrics::observe_tick_phase_latency_ms(
                    "state_write",
                    tick_all_start.elapsed().as_millis() as u64,
                );
                tracing::error!(epoch, error = %err, "failed to tick outer streams");
                metrics::inc_ingest_tick("error");
                continue;
            } else if should_sample(&TICK_LOG_COUNTER, TICK_LOG_SAMPLE_EVERY) {
                metrics::observe_tick_phase_latency_ms(
                    "state_write",
                    tick_all_start.elapsed().as_millis() as u64,
                );
                tracing::debug!(epoch, "advanced all source frontiers");
                metrics::inc_ingest_tick("ok");
            } else {
                metrics::observe_tick_phase_latency_ms(
                    "state_write",
                    tick_all_start.elapsed().as_millis() as u64,
                );
                metrics::inc_ingest_tick("ok");
            }
            let state_write_latency_ms = tick_all_start.elapsed().as_millis() as u64;
            if epoch <= 8 || epoch % 128 == 0 {
                tracing::info!(epoch, state_write_latency_ms, "tick state_write completed");
            }
            drop(registry);
            for (source_id, offsets) in tick_source_offsets.iter().enumerate() {
                let Some(offsets) = offsets.as_ref() else {
                    continue;
                };
                let source = source_names_by_id_for_task[source_id].as_str();
                for (&partition, &offset) in offsets {
                    let key = (source.to_string(), partition);
                    let latest_entry = latest_source_offsets.entry(key.clone()).or_insert(0);
                    *latest_entry = (*latest_entry).max(offset);
                    let committed_offset = committed_source_offsets.get(&key).copied().unwrap_or(0);
                    metrics::record_source_offset_lag(
                        source,
                        partition,
                        latest_entry.saturating_sub(committed_offset),
                    );
                }
            }
            for (source_id, offsets) in tick_source_offsets.iter().enumerate() {
                let Some(offsets) = offsets.as_ref() else {
                    continue;
                };
                let source = source_names_by_id_for_task[source_id].as_str();
                for (&partition, &offset) in offsets {
                    checkpoint_manager.update_partition_offset(source, partition, offset);
                }
            }
            let frontier = next_watermark.max(0).try_into().unwrap_or(0_u64);
            let mv_versions = collect_mv_versions_for_commit(&mv_for_task, &mut last_mv_versions);
            let mut next_committed_kafka_offsets = committed_kafka_offsets.clone();
            advance_kafka_offset_commit_state(
                &mut next_committed_kafka_offsets,
                &tick_kafka_offsets,
            );
            let tick_commit = build_tick_commit_for_checkpoint(
                epoch,
                frontier,
                &checkpoint_manager,
                &mv_versions,
                &next_committed_kafka_offsets,
            );
            let committed_at_ms = tick_commit.committed_at_unix_ms;
            let source_journal_commit_batches: Vec<_> = source_journal_batches
                .iter()
                .map(|(source_id, max_event_time_ms, deltas)| {
                    (
                        source_names_by_id_for_task[*source_id].clone(),
                        *max_event_time_ms,
                        deltas.clone(),
                    )
                })
                .collect();
            let checkpoint_write_start = Instant::now();
            let checkpoint_result = if let Some(staged_writes) = cdc_staged_writes {
                checkpoint_manager
                    .persist_tick_commit_with_source_batches_and_staged_writes(
                        tick_commit,
                        &source_journal_commit_batches,
                        staged_writes,
                    )
                    .await
            } else {
                checkpoint_manager
                    .persist_tick_commit_with_source_batches(
                        tick_commit,
                        &source_journal_commit_batches,
                    )
                    .await
            };
            if let Err(err) = checkpoint_result {
                metrics::observe_tick_phase_latency_ms(
                    "checkpoint_write",
                    checkpoint_write_start.elapsed().as_millis() as u64,
                );
                tracing::error!(epoch, error = %err, "failed to persist tick commit");
                for ack in tick_commit_acks {
                    ack.record_failed(format!("failed to persist tick commit {epoch}: {err}"))
                        .await;
                }
                record_runtime_failure(
                    &failure_for_executor,
                    format!("failed to persist tick commit {epoch}: {err}"),
                );
                executor_cancel.cancel();
                break;
            }
            metrics::observe_tick_phase_latency_ms(
                "checkpoint_write",
                checkpoint_write_start.elapsed().as_millis() as u64,
            );
            let checkpoint_write_latency_ms = checkpoint_write_start.elapsed().as_millis() as u64;
            if epoch <= 8 || epoch % 128 == 0 {
                tracing::info!(
                    epoch,
                    checkpoint_write_latency_ms,
                    "tick checkpoint_write completed"
                );
            }
            if !first_tick_commit_logged {
                first_tick_commit_logged = true;
                tracing::info!(
                    epoch,
                    batch_size = batch_len,
                    decoded_rows = decoded_rows_len,
                    state_write_latency_ms,
                    checkpoint_write_latency_ms,
                    time_to_first_tick_commit_ms =
                        executor_loop_started.elapsed().as_millis() as u64,
                    "executor committed first tick"
                );
            }
            for ack in tick_commit_acks {
                ack.record_committed().await;
            }
            for (source_id, offsets) in tick_source_offsets.iter().enumerate() {
                let Some(offsets) = offsets.as_ref() else {
                    continue;
                };
                let source = source_names_by_id_for_task[source_id].as_str();
                for (&partition, &offset) in offsets {
                    let key = (source.to_string(), partition);
                    let committed_entry = committed_source_offsets.entry(key.clone()).or_insert(0);
                    *committed_entry = (*committed_entry).max(offset);
                    let latest_offset = latest_source_offsets.get(&key).copied().unwrap_or(offset);
                    metrics::record_source_offset_lag(
                        source,
                        partition,
                        latest_offset.saturating_sub(*committed_entry),
                    );
                }
            }
            for mv_version in &mv_versions {
                mv_last_update_at_ms.insert(mv_version.view.clone(), committed_at_ms);
            }
            advance_kafka_offset_commit_state(&mut committed_kafka_offsets, &tick_kafka_offsets);
            record_postgres_cdc_lsn_progress(
                &mut committed_postgres_lsns,
                &tick_postgres_lsns,
                &tick_postgres_sources,
                &tick_postgres_table_lsns,
                &cdc_replication_debug_for_task,
            );
            record_mv_freshness_metrics(&mv_last_update_at_ms, current_unix_time_ms());
            metrics::record_last_committed_tick(epoch);
            metrics::record_checkpoint_age_seconds(0);
            last_checkpoint_commit_at = Instant::now();
            if next_watermark != prev_watermark {
                watermark_for_task.store(next_watermark, Ordering::Relaxed);
            }
            if next_watermark >= 0 {
                metrics::record_global_watermark_ms(next_watermark);
                mv_for_task.update_watermark_all(next_watermark as u64);
                let now_ms = current_unix_time_ms();
                let watermark_ms = u64::try_from(next_watermark).unwrap_or(u64::MAX);
                metrics::record_watermark_lag_ms(now_ms.saturating_sub(watermark_ms));
            }
            {
                let mut debug_state = watermark_debug_for_task.write().await;
                debug_state.updated_at_unix_ms = current_unix_time_ms();
                debug_state.global_watermark_ms = (next_watermark >= 0).then_some(next_watermark);
                let mut sources = Vec::with_capacity(source_watermarks.len());
                for (source, watermark) in &source_watermarks {
                    let idle = source_last_seen_at
                        .get(source)
                        .map(|last| now_instant.duration_since(*last) >= watermark_idle_timeout)
                        .unwrap_or(true);
                    sources.push(http_ingest::WatermarkDebugSourceState {
                        source: source.clone(),
                        watermark_ms: *watermark,
                        idle,
                    });
                }
                sources.sort_by(|left, right| left.source.cmp(&right.source));
                debug_state.sources = sources;
            }
            if !tick_kafka_offsets.is_empty() && !kafka_commit_senders_for_task.is_empty() {
                let kafka_commit_start = Instant::now();
                let commit = build_kafka_offset_commit(epoch, &committed_kafka_offsets);
                for sender in &kafka_commit_senders_for_task {
                    let _ = sender.send(commit.clone());
                }
                metrics::observe_tick_phase_latency_ms(
                    "kafka_commit_notify",
                    kafka_commit_start.elapsed().as_millis() as u64,
                );
            }
            notify_postgres_cdc_commit_senders(
                epoch,
                &committed_postgres_lsns,
                &tick_postgres_lsns,
                &postgres_cdc_commit_senders_for_task,
            );
            let tick_latency_ms = tick_start.elapsed().as_millis() as u64;
            metrics::observe_tick_latency_ms(tick_latency_ms);
            tracing::debug!(tick_latency_ms, "connector tick completed");

            let queue_depth: usize = connector_queues
                .iter()
                .map(|queue| queue.pending.len())
                .sum();
            let total_queue_depth = queue_depth.saturating_add(connector_receiver_for_task.len());
            metrics::record_ingest_queue_depth(total_queue_depth);
            if should_sample(&INGEST_METRICS_COUNTER, INGEST_METRICS_SAMPLE_EVERY) {
                let per_source: Vec<_> = decoded_counts
                    .iter()
                    .enumerate()
                    .filter_map(|(source_id, count)| {
                        (*count > 0)
                            .then_some((source_names_by_id_for_task[source_id].as_str(), *count))
                    })
                    .collect();
                let per_connector: Vec<_> = connector_queues
                    .iter()
                    .map(|queue| (queue.name.as_str(), per_connector_counts[queue.id]))
                    .filter(|(_, count)| *count > 0)
                    .collect();
                tracing::info!(
                    epoch,
                    queue_depth = total_queue_depth,
                    batch_size = batch_len,
                    pending = total_queue_depth,
                    decoded_rows = decoded_rows_len,
                    max_batch,
                    max_batch_per_source,
                    max_batch_per_connector,
                    decode_latency_ms,
                    state_write_latency_ms,
                    checkpoint_write_latency_ms,
                    tick_latency_ms,
                    per_source = ?per_source,
                    per_connector = ?per_connector,
                    "ingest batch metrics"
                );
            }
        }
        let final_frontier = watermark_for_task
            .load(Ordering::Relaxed)
            .max(0)
            .try_into()
            .unwrap_or(0_u64);
        let outer_registry = outer_for_task.lock().await;
        if let Err(err) = checkpoint_manager
            .persist_snapshot(final_frontier, mv_for_task.as_ref(), &outer_registry)
            .await
        {
            tracing::warn!(error = %err, "final checkpoint persistence failed");
        }
        executor_running_for_task.store(false, Ordering::Relaxed);
    });

    let source_bridge = Arc::new(Mutex::new(DbspBridge::new(Arc::clone(&db)).await?));
    let query = FloeQueryContext::new(storage);
    query
        .preload_tables()
        .await
        .context("failed to register tables with DataFusion")?;
    register_source_tables(
        &query,
        &source_registry,
        Arc::clone(&source_bridge),
        &source_journal_required_sources,
        Arc::clone(&checkpoint_table),
        CHECKPOINT_GRAPH_ID,
    )
    .await
    .context("register source tables")?;
    register_materialized_view_tables(&query, &planned_materialized_views, &mv_registry)
        .await
        .context("register materialized view tables")?;
    runtime_ready.store(true, Ordering::Relaxed);

    let sink_handles = sinks::spawn_sinks(
        sink_specs,
        Arc::clone(&mv_registry),
        sink_resume_cursors,
        Some(sink_checkpoint_tx),
        sink_cancel.clone(),
        runtime_cancel.clone(),
        Arc::clone(&runtime_failure),
    );

    let signal_handle = spawn_signal_handler(
        runtime_cancel.clone(),
        ingest_cancel.clone(),
        shutdown_signal.clone(),
    );

    let pgwire_addr = run_args
        .pgwire_addr
        .clone()
        .unwrap_or_else(|| DEFAULT_PGWIRE_ADDR.to_string());
    let server_handle = spawn_pgwire_server(
        query.clone(),
        Arc::clone(&mv_registry),
        service_cancel.clone(),
        runtime_cancel.clone(),
        Arc::clone(&runtime_failure),
        !run_args.disable_pgwire,
        pgwire_addr,
        server::ServerRuntimeConfig {
            tail: tail_execution_config,
        },
    );

    tokio::select! {
        _ = runtime_cancel.cancelled() => {}
        _ = shutdown_signal.cancelled() => {}
    }
    let graceful_shutdown = shutdown_signal.is_cancelled() && !runtime_cancel.is_cancelled();
    let mut executor_handle = Some(executor_handle);
    if graceful_shutdown {
        ingest_cancel.cancel();
        if let Some(handle) = executor_handle.take()
            && let Err(err) = handle.await
            && !err.is_cancelled()
        {
            tracing::error!(
                error = %err,
                "executor task joined with error during graceful shutdown"
            );
        }
    }
    sink_cancel.cancel();
    service_cancel.cancel();
    runtime_cancel.cancel();
    ingest_cancel.cancel();
    drop(task_event_tx);

    for handle in connector_handles {
        if let Err(err) = handle.await
            && !err.is_cancelled()
        {
            tracing::error!(error = %err, "connector task joined with error");
        }
    }

    for handle in sink_handles {
        if let Err(err) = handle.await
            && !err.is_cancelled()
        {
            tracing::error!(error = %err, "sink task joined with error");
        }
    }

    if let Err(err) = admin_handle.await
        && !err.is_cancelled()
    {
        tracing::error!(error = %err, "admin HTTP server task joined with error");
    }

    if let Err(err) = cdc_replication_debug_handle.await
        && !err.is_cancelled()
    {
        tracing::error!(error = %err, "CDC replication debug task joined with error");
    }

    if let Some(handle) = executor_handle.take()
        && let Err(err) = handle.await
        && !err.is_cancelled()
    {
        tracing::error!(error = %err, "executor task joined with error");
    }

    if let Err(err) = task_monitor.await
        && !err.is_cancelled()
    {
        tracing::error!(error = %err, "graph monitor task joined with error");
    }

    let server_result = match server_handle.await {
        Ok(result) => result,
        Err(err) if err.is_cancelled() => Ok(()),
        Err(err) => Err(anyhow!("pgwire server task join error: {err}")),
    };
    if let Err(err) = signal_handle.await
        && !err.is_cancelled()
    {
        tracing::error!(error = %err, "signal task joined with error");
    }
    if let Err(err) = cancellation_propagation_handle.await
        && !err.is_cancelled()
    {
        tracing::error!(error = %err, "cancellation propagation task joined with error");
    }

    drop(source_bridge);
    drop(query);
    drop(mv_registry);
    drop(outer_registry);

    let close_timeout = Duration::from_millis(
        run_args
            .slatedb_close_timeout_ms
            .unwrap_or(DEFAULT_SLATEDB_CLOSE_TIMEOUT_MS),
    );
    let close_result = match tokio::time::timeout(close_timeout, db.close()).await {
        Ok(result) => result.map_err(anyhow::Error::new),
        Err(_) => {
            tracing::warn!(
                timeout_ms = close_timeout.as_millis() as u64,
                "timed out closing SlateDB; continuing shutdown"
            );
            Ok(())
        }
    };

    if let Some(message) = runtime_failure
        .lock()
        .expect("runtime failure lock poisoned")
        .clone()
    {
        return Err(anyhow!(message));
    }

    close_result?;

    server_result
}
