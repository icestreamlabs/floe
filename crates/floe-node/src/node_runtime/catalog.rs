use super::*;

pub(super) fn table_definition_from_sql(
    definition: &CreateTableDefinition,
) -> anyhow::Result<TableDefinition> {
    let columns = definition
        .columns()
        .iter()
        .map(|column| {
            ColumnDefinition::new_typed_nullable(
                column.name(),
                column.data_type().clone(),
                column.nullable(),
                column.primary_key(),
            )
        })
        .collect();
    TableDefinition::new(definition.name(), columns)
}

pub(super) fn catalog_source_definition_from_sql(
    definition: &CreateSourceDefinition,
) -> anyhow::Result<CatalogSourceDefinition> {
    let connector = match definition.connector() {
        SourceConnector::PostgresCdc(options) => {
            CatalogSourceConnector::PostgresCdc(PostgresCdcSourceDefinition::new_with_setup_policy(
                options.connection(),
                options.slot(),
                options.publication().map(ToString::to_string),
                options.include_schema_in_source(),
                options.schema_evolution_policy(),
                options.auto_create_slot(),
                options.auto_create_publication(),
            )?)
        }
    };
    CatalogSourceDefinition::new(definition.name(), connector)
}

pub(super) fn source_backed_table_definition_from_sql(
    definition: &CreateTableDefinition,
) -> anyhow::Result<Option<SourceBackedTableDefinition>> {
    definition
        .source()
        .map(|source| {
            SourceBackedTableDefinition::new(
                definition.name(),
                source.source_name(),
                source.upstream_table(),
            )
        })
        .transpose()
}

pub(super) fn replication_pipeline_definition_from_sql(
    definition: &SqlReplicationPipelineDefinition,
) -> anyhow::Result<CatalogReplicationPipelineDefinition> {
    CatalogReplicationPipelineDefinition::new(
        definition.name(),
        definition.source_name(),
        definition.upstream_table(),
        definition.target().clone(),
        definition.format(),
        definition.buffer_mode(),
        definition.buffer_policy(),
        definition.emit_tombstones(),
        definition.include_transaction_metadata(),
        definition.error_policy(),
    )
}

pub(super) fn source_definition_from_table(
    table: &TableDefinition,
) -> anyhow::Result<SourceDefinition> {
    let columns = table
        .columns()
        .iter()
        .map(|column| {
            let data_type = match column.data_type() {
                ColumnType::Int64 => SourceDataType::Int64,
                ColumnType::Bool => SourceDataType::Bool,
                ColumnType::Utf8 => SourceDataType::Utf8,
                ColumnType::TimestampMillis => SourceDataType::TimestampMillis,
                ColumnType::DateDays => SourceDataType::DateDays,
                ColumnType::Decimal128 { precision, scale } => SourceDataType::Decimal128 {
                    precision: *precision,
                    scale: *scale,
                },
                ColumnType::Numeric => SourceDataType::Numeric,
            };
            SourceColumn::new_nullable(column.name(), data_type, column.nullable())
        })
        .collect();
    let mut definition = SourceDefinition::new(table.name(), columns)?;
    let primary_key = table
        .columns()
        .iter()
        .find(|column| column.is_primary_key())
        .ok_or_else(|| anyhow!("table '{}' has no primary key column", table.name()))?;
    definition.set_property(SOURCE_PRIMARY_KEY_PROPERTY, primary_key.name());
    Ok(definition)
}

pub(super) fn source_definition_has_primary_key(definition: &SourceDefinition) -> bool {
    source_definition_primary_key_columns(definition).is_some()
}

pub(super) fn source_definition_primary_key_columns(
    definition: &SourceDefinition,
) -> Option<Vec<String>> {
    let columns = definition
        .property(SOURCE_PRIMARY_KEY_PROPERTY)?
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    (!columns.is_empty()).then_some(columns)
}

pub(super) fn cdc_table_schema_from_source_definition(
    definition: &SourceDefinition,
    upstream_table: UpstreamTableRef,
) -> anyhow::Result<CdcTableSchema> {
    let columns = definition
        .columns()
        .iter()
        .map(|column| {
            CdcColumn::new(
                column.name(),
                column.data_type().column_type(),
                column.nullable(),
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let primary_key = CdcPrimaryKey::new(
        source_definition_primary_key_columns(definition)
            .ok_or_else(|| anyhow!("source '{}' has no primary key", definition.name()))?,
    )?;
    CdcTableSchema::new(
        CdcTableId::new(definition.name())?,
        upstream_table,
        columns,
        primary_key,
    )
}

pub(super) fn resolve_output_consolidation_mode(
    requested: cli::OutputConsolidationMode,
    source_registry: &SourceRegistry,
) -> cli::OutputConsolidationMode {
    if requested == cli::OutputConsolidationMode::AllColumns
        && source_registry
            .definitions()
            .iter()
            .any(source_definition_has_primary_key)
    {
        cli::OutputConsolidationMode::Key
    } else {
        requested
    }
}

pub(super) async fn register_materialized_view_tables(
    context: &FloeQueryContext,
    planned: &[PlannedMaterializedView],
    registry: &Arc<MaterializedViewRegistry>,
) -> anyhow::Result<()> {
    if planned.is_empty() {
        return Ok(());
    }

    let session = context.session();
    let storage = context.storage();
    for mv in planned {
        let arrow_schema = df_schema_to_arrow(mv.logical_plan().schema())?;
        registry.set_schema(mv.definition().name(), arrow_schema.clone());
        storage
            .save_materialized_view_schema(mv.definition().name(), arrow_schema.clone())
            .await
            .with_context(|| {
                format!(
                    "persist schema metadata for materialized view '{}'",
                    mv.definition().name()
                )
            })?;
        let provider = MaterializedViewTableProvider::new(
            Arc::clone(registry),
            mv.definition().name().to_string(),
            arrow_schema,
        );
        session
            .register_table(mv.definition().name(), Arc::new(provider))
            .context("register materialized view provider")?;
    }

    Ok(())
}

pub(super) async fn register_source_tables(
    context: &FloeQueryContext,
    sources: &SourceRegistry,
    bridge: Arc<Mutex<DbspBridge>>,
    journaled_sources: &BTreeSet<String>,
    checkpoint_table: Arc<dyn KeyValueTable>,
    checkpoint_graph_id: &str,
) -> anyhow::Result<()> {
    let session = context.session();
    for definition in sources.definitions() {
        let schema = definition.to_arrow_schema();
        let mut provider = SourceTableProvider::new(
            Arc::clone(&bridge),
            definition.name(),
            definition.name(),
            schema,
            definition.property(SOURCE_PRIMARY_KEY_PROPERTY),
        )?;
        if journaled_sources.contains(definition.name()) {
            provider = provider.with_source_journal(
                Arc::clone(&checkpoint_table),
                checkpoint_graph_id,
                definition.name(),
            );
        }
        let _ = session.deregister_table(definition.name());
        session
            .register_table(definition.name(), Arc::new(provider))
            .with_context(|| format!("register source table {}", definition.name()))?;

        if let Some(short_name) = definition.name().strip_prefix("nexmark_") {
            let alias_schema = camel_case_schema(definition);
            let mut alias_provider = SourceTableProvider::new(
                Arc::clone(&bridge),
                short_name,
                definition.name(),
                alias_schema,
                definition.property(SOURCE_PRIMARY_KEY_PROPERTY),
            )?;
            if journaled_sources.contains(definition.name()) {
                alias_provider = alias_provider.with_source_journal(
                    Arc::clone(&checkpoint_table),
                    checkpoint_graph_id,
                    definition.name(),
                );
            }
            let _ = session.deregister_table(short_name);
            session
                .register_table(short_name, Arc::new(alias_provider))
                .with_context(|| {
                    format!(
                        "register alias table {short_name} for source {}",
                        definition.name()
                    )
                })?;
        }
    }
    Ok(())
}

pub(super) fn df_schema_to_arrow(schema: &DFSchemaRef) -> anyhow::Result<SchemaRef> {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

pub(super) fn gather_handle_streams(
    registry: &OuterStreamRegistry,
    sources: &BTreeSet<String>,
) -> HashMap<String, dbsp::DeltaHandleStream> {
    let mut map = HashMap::new();
    for source in sources {
        if let Some(stream) = registry.delta_handle_stream(source) {
            map.insert(source.clone(), stream);
        }
    }
    map
}

pub(super) fn gather_transient_streams(
    registry: &OuterStreamRegistry,
    sources: &BTreeSet<String>,
) -> HashMap<String, floe_executor::outer_stream::TransientSourceHandleStream> {
    let mut map = HashMap::new();
    for source in sources {
        if let Some(stream) = registry.transient_stream(source) {
            map.insert(source.clone(), stream);
        }
    }
    map
}
