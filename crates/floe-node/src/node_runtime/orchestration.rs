use super::*;

mod checkpointing;
mod connectors;
mod executor_task;
mod postgres_runtime;
mod query_runtime;
mod runtime_services;
mod runtime_shutdown;
mod runtime_sources;
mod runtime_tasks;
mod source_replay;
mod source_requirements;
mod wal_stream;

use checkpointing::{
    VectorizedSourceJournalTransientBatch, build_tick_commit_for_checkpoint,
    build_vectorized_source_journal_commit_batches, notify_postgres_cdc_commit_senders,
    record_postgres_cdc_lsn_progress,
};
use connectors::{SpawnConnectorTasksConfig, SpawnedConnectorTasks, spawn_connector_tasks};
use executor_task::{
    ExecutorBatchLimits, ExecutorCdcContext, ExecutorCheckpointContext, ExecutorIngestContext,
    ExecutorRuntimeContext, ExecutorSourceContext, ExecutorTaskContext, spawn_executor_task,
};
#[cfg(test)]
pub(super) use postgres_runtime::PostgresCdcRuntimePlanRequest;
use postgres_runtime::{
    insert_catalog_source_definition, insert_replication_pipeline_definition,
    insert_source_backed_table_definition, postgres_schema_evolution_policy_from_catalog,
    validate_replication_pipelines, validate_source_backed_tables,
};
pub(super) use postgres_runtime::{
    merge_catalog_source_connectors, postgres_cdc_runtime_plan,
    validate_materialized_views_do_not_query_raw_cdc_sources,
};
use query_runtime::{RuntimeFrontendServices, StartRuntimeFrontendServicesConfig};
use runtime_services::{RuntimeServicesConfig, start_runtime_services};
use runtime_shutdown::{RuntimeShutdownContext, shutdown_runtime};
use runtime_sources::build_runtime_source_indexes;
use runtime_tasks::spawn_cancellation_propagation;
use source_replay::{
    ReplayCommittedVectorizedSourceJournalConfig,
    replay_committed_vectorized_source_journal_entries, source_is_replayable_from_connector,
};
pub(super) use source_replay::{
    apply_durable_table_source_journal_policy, kafka_metadata_journal_required_sources,
    source_journal_required_sources,
};
use source_requirements::required_column_masks_by_source_id;
#[cfg(test)]
pub(super) use wal_stream::PostgresCdcRuntimeReconnectPolicy;
use wal_stream::{NativePostgresCdcConnectorConfig, run_native_postgres_cdc_connector};

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
    let mut durable_table_source_names: BTreeSet<String> = BTreeSet::new();
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
                    durable_table_source_names.insert(table.name().to_string());
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
                FloeStatement::Subscribe { .. } => {
                    return Err(anyhow!(
                        "SUBSCRIBE statements are not supported in --mv-query programs"
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
            durable_table_source_names.insert(table.name().to_string());
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
    let mut subscribe_execution_config = SubscribeExecutionConfig::default();
    if let Some(channel_capacity) = run_args.subscribe_channel_capacity {
        subscribe_execution_config.channel_capacity = channel_capacity;
    }
    if let Some(max_catchup_versions) = run_args.subscribe_max_catchup_versions {
        subscribe_execution_config.max_catchup_versions = max_catchup_versions;
    }
    let circuit_plans = build_dataflows(
        &planned_materialized_views,
        &available_sources,
        &source_registry,
    )?;
    let mut all_required_sources: BTreeSet<String> = BTreeSet::new();
    let available_source_names: BTreeSet<String> = available_sources.iter().cloned().collect();
    let mut plan_required_sources: Vec<BTreeSet<String>> = Vec::with_capacity(circuit_plans.len());
    for (mv_idx, plan) in circuit_plans.iter().enumerate() {
        let view_name = planned_materialized_views[mv_idx]
            .definition()
            .name()
            .to_string();
        let ValidatedPlan {
            required_sources, ..
        } = validate_dbsp_plan(plan, &available_source_names, &view_name)?;
        all_required_sources.extend(required_sources.iter().cloned());
        plan_required_sources.push(required_sources);
    }
    all_required_sources.extend(durable_table_source_names.iter().cloned());
    let source_journal_mode = config
        .as_ref()
        .and_then(|config| config.storage.source_journal)
        .unwrap_or(SourceJournalConfig::Auto);
    let mut source_journal_required_sources = source_journal_required_sources(
        &source_registry,
        &all_required_sources,
        source_journal_mode,
    );
    let mut kafka_metadata_journal_required_sources = kafka_metadata_journal_required_sources(
        &source_registry,
        &all_required_sources,
        source_journal_mode,
    );
    apply_durable_table_source_journal_policy(
        &mut source_journal_required_sources,
        &mut kafka_metadata_journal_required_sources,
        &durable_table_source_names,
        &all_required_sources,
        source_journal_mode,
    );
    if !source_journal_required_sources.is_empty()
        || !kafka_metadata_journal_required_sources.is_empty()
    {
        tracing::info!(
            source_journal_sources = ?source_journal_required_sources,
            kafka_metadata_sources = ?kafka_metadata_journal_required_sources,
            "resolved vectorized source replay journals"
        );
    }
    let source_replay_covered_sources: BTreeSet<String> = source_journal_required_sources
        .union(&kafka_metadata_journal_required_sources)
        .cloned()
        .collect();
    let source_journal_skipped_sources: BTreeSet<String> = all_required_sources
        .difference(&source_replay_covered_sources)
        .cloned()
        .collect();
    tracing::info!(
        mode = ?source_journal_mode,
        journaled_sources = ?source_journal_required_sources,
        kafka_metadata_sources = ?kafka_metadata_journal_required_sources,
        skipped_sources = ?source_journal_skipped_sources,
        "resolved source replay journal policy"
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
            "source journal disabled for non-replayable sources; committed source rows will not be recoverable or queryable after restart"
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
    let storage = storage.context("storage initialized when not in dry-run")?;
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
        dbsp::install_operator_state_restore_for_graph(
            CHECKPOINT_GRAPH_ID,
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
        dbsp::install_operator_state_restore_for_graph(CHECKPOINT_GRAPH_ID, Vec::new());
    }
    if let Some(tick_commit) = checkpoint_manager.latest_tick_commit() {
        metrics::record_last_committed_tick(tick_commit.tick_id);
    }
    let vectorized_source_batch_journal =
        VectorizedSourceBatchJournal::new(checkpoint_manager.store().table());
    let kafka_source_journal = KafkaSourceJournal::new(checkpoint_manager.store().table());
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

    let mv_registry = Arc::new(MaterializedViewRegistry::new_with_retention(
        if run_args.mv_retain_last == 0 {
            None
        } else {
            Some(run_args.mv_retain_last)
        },
    ));
    let vectorized_mv_plans = planned_materialized_views
        .iter()
        .map(|mv| {
            let arrow_schema = df_schema_to_arrow(mv.logical_plan().schema())?;
            Ok(VectorizedMaterializedViewPlan::new(
                mv.definition().name().to_string(),
                mv.definition().query().to_string(),
                arrow_schema,
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let source_query_table_names = if run_args.disable_pgwire {
        BTreeSet::new()
    } else {
        durable_table_source_names.clone()
    };
    let mut vectorized_runtime_options = VectorizedExecutionRuntimeOptions::default()
        .with_operator_state_table(checkpoint_manager.store().table())
        .without_grouped_stats_arrow_snapshots();
    if !source_query_table_names.is_empty() {
        vectorized_runtime_options = vectorized_runtime_options
            .with_source_query_tables_for(source_query_table_names.clone());
    }
    let mut vectorized_runtime = VectorizedExecutionRuntime::new_with_udfs_and_options(
        &source_registry,
        vectorized_mv_plans,
        Arc::clone(&mv_registry),
        planner_udfs(),
        vectorized_runtime_options,
    )
    .await
    .context("initialize vectorized execution runtime")?;
    let vectorized_source_table_providers = vectorized_runtime.table_providers();
    let mut maintenance = DbspMaintenance::new(Arc::clone(&db))
        .await
        .context("initialize DBSP maintenance")?;
    let stream_compaction = StreamCompactionConfig {
        max_chain_len: run_args.zset_compaction_max_chain_len,
        max_segments: run_args.zset_compaction_max_segments,
        scheduler_backoff_ticks: run_args.zset_compaction_backoff_ticks,
        scheduler_max_concurrent_jobs: run_args.zset_compaction_max_concurrent_jobs,
    };
    maintenance.set_stream_compaction(
        CompactionPolicy {
            max_chain_len: stream_compaction.max_chain_len,
            max_segments: stream_compaction.max_segments,
            max_bucket_segments: stream_compaction.max_segments,
        },
        CompactionSchedulerConfig {
            failure_backoff_ticks: stream_compaction.scheduler_backoff_ticks,
            max_concurrent_jobs: stream_compaction.scheduler_max_concurrent_jobs,
        },
    );
    if run_args.maintenance_paused {
        maintenance.pause();
        tracing::info!("maintenance started in paused mode");
    }
    for namespace in &run_args.maintenance_inspect_namespace {
        let summary = maintenance
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
        let compacted = maintenance
            .compact_namespace_once(namespace)
            .await
            .with_context(|| format!("compact namespace '{namespace}'"))?;
        tracing::info!(
            namespace = %namespace,
            compacted_version = ?compacted,
            "maintenance compaction request completed"
        );
    }
    for namespace in &run_args.maintenance_gc_namespace {
        let sweep_stats = maintenance
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
    let mut replayed_committed_source_batches = false;
    let connector_resume_only_sources: BTreeSet<String> = source_journal_skipped_sources
        .difference(&kafka_metadata_journal_required_sources)
        .cloned()
        .collect();
    if recovered_tick_commit.is_some() && !connector_resume_only_sources.is_empty() {
        tracing::info!(
            sources = ?connector_resume_only_sources,
            "skipped source-batch journal replay for sources expected to resume from connector offsets or helper state"
        );
    }
    let queue_capacity = run_args.ingest_queue_capacity;
    let max_batch = run_args.ingest_batch_size;
    let max_batch_per_source = run_args.ingest_batch_per_source;
    let max_batch_per_connector = run_args.ingest_batch_per_connector;
    let watermark_idle_source_ms = run_args.watermark_idle_source_ms.unwrap_or(0);
    let watermark_idle_source_ms = if watermark_idle_source_ms == 0 {
        DEFAULT_WATERMARK_IDLE_SOURCE_MS
    } else {
        watermark_idle_source_ms
    };

    let cancellation_propagation_handle = spawn_cancellation_propagation(
        runtime_cancel.clone(),
        ingest_cancel.clone(),
        sink_cancel.clone(),
        service_cancel.clone(),
    );

    let watermark_debug = Arc::new(tokio::sync::RwLock::new(http_ingest::WatermarkDebugState {
        policy: "min_active_sources".to_string(),
        ..http_ingest::WatermarkDebugState::default()
    }));
    let cdc_replication_debug = Arc::new(tokio::sync::RwLock::new(
        http_ingest::CdcReplicationDebugState::default(),
    ));
    let connector_count = connector_specs.len();
    tracing::info!(
        connector_count,
        queue_capacity,
        max_batch,
        max_batch_per_source,
        max_batch_per_connector,
        "resolved ingest execution limits"
    );

    let definitions = source_registry.definitions().to_vec();
    let source_id_by_name: HashMap<String, usize> = definitions
        .iter()
        .enumerate()
        .map(|(idx, definition)| (definition.name().to_string(), idx))
        .collect();
    let required_columns_by_source_id = required_column_masks_by_source_id(
        &definitions,
        &all_required_sources,
        &circuit_plans,
        &plan_required_sources,
        &source_journal_required_sources,
    )?;
    let runtime_services = start_runtime_services(RuntimeServicesConfig {
        connector_specs: &connector_specs,
        source_registry: &source_registry,
        source_backed_tables: &source_backed_tables,
        replication_pipelines: &replication_pipelines,
        node_config: config.as_ref(),
        storage: storage.clone(),
        service_cancel: service_cancel.clone(),
        runtime_cancel: runtime_cancel.clone(),
        runtime_failure: Arc::clone(&runtime_failure),
        run_args: &run_args,
        executor_running: Arc::clone(&executor_running),
        storage_reachable: Arc::clone(&storage_reachable),
        runtime_ready: Arc::clone(&runtime_ready),
        watermark_debug: Arc::clone(&watermark_debug),
        cdc_replication_debug: Arc::clone(&cdc_replication_debug),
        mv_registry: Arc::clone(&mv_registry),
    })
    .await?;
    let postgres_cdc_runtime_plans_by_connector =
        runtime_services.postgres_cdc_runtime_plans_by_connector;
    let replication_pipeline_runtime = runtime_services.replication_pipeline_runtime;
    let admin_handle = runtime_services.admin_handle;
    let cdc_replication_debug_handle = runtime_services.cdc_replication_debug_handle;
    let runtime_source_indexes = build_runtime_source_indexes(
        &definitions,
        &all_required_sources,
        required_columns_by_source_id,
        &source_query_table_names,
        &kafka_metadata_journal_required_sources,
        &source_journal_required_sources,
        &postgres_cdc_runtime_plans_by_connector,
    );
    let (connector_sender, connector_receiver) = core_source::routed_channel(queue_capacity);
    let (cdc_transaction_sender, cdc_transaction_receiver) =
        mpsc::channel::<QueuedCdcTransaction>(queue_capacity);
    let pending_event_counter = core_source::PendingAppendIngestEventCounter::default();
    let (sink_checkpoint_tx, sink_checkpoint_rx) =
        mpsc::channel::<SinkCursor>(sinks::SINK_CHECKPOINT_CHANNEL_CAPACITY);
    let sink_resume_cursors: HashMap<String, SinkCursor> = initial_sink_cursors
        .iter()
        .cloned()
        .map(|cursor| (cursor.sink.clone(), cursor))
        .collect();
    let recovered_kafka_offsets = recovered_tick_commit
        .as_ref()
        .map(|commit| commit.kafka_offsets.clone())
        .unwrap_or_default();
    if let Some(tick_commit) = recovered_tick_commit.as_ref()
        && !source_replay_covered_sources.is_empty()
    {
        let (replayed_raw, replayed_kafka) = replay_committed_vectorized_source_journal_entries(
            ReplayCommittedVectorizedSourceJournalConfig {
                source_batch_journal: &vectorized_source_batch_journal,
                kafka_journal: &kafka_source_journal,
                vectorized_runtime: &mut vectorized_runtime,
                max_tick_id: tick_commit.tick_id,
                raw_journal_sources: &source_journal_required_sources,
                kafka_metadata_sources: &kafka_metadata_journal_required_sources,
                connector_specs: &connector_specs,
                run_args: &run_args,
                definitions: &definitions,
                source_id_by_name: &source_id_by_name,
            },
        )
        .await
        .context("replay committed vectorized source journal entries")?;
        tracing::info!(
            replayed_vectorized_entries = replayed_raw,
            replayed_kafka_metadata_entries = replayed_kafka,
            committed_tick = tick_commit.tick_id,
            vectorized_journal_sources = ?source_journal_required_sources,
            kafka_metadata_sources = ?kafka_metadata_journal_required_sources,
            "replayed committed vectorized source journal entries"
        );
        replayed_committed_source_batches = true;
    }
    if let Some(tick_commit) = recovered_tick_commit.as_ref()
        && replayed_committed_source_batches
    {
        for mv_version in &tick_commit.mv_versions {
            let Some(handle) = mv_registry.get(&mv_version.view) else {
                continue;
            };
            if handle.latest_version().unwrap_or(-1) >= mv_version.version as i64 {
                continue;
            }
            let target_version = mv_version.version as i64;
            wait_for_materialized_view_visible(&handle, target_version, &runtime_cancel)
                .await
                .with_context(|| {
                    format!(
                        "wait for replayed materialized view '{}' to reach version {}",
                        mv_version.view, mv_version.version
                    )
                })?;
        }
    }

    let SpawnedConnectorTasks {
        handles: connector_handles,
        queues: connector_queues,
        kafka_commit_senders,
        postgres_cdc_commit_senders,
    } = spawn_connector_tasks(SpawnConnectorTasksConfig {
        connector_specs,
        definitions: definitions.clone(),
        postgres_cdc_runtime_plans_by_connector: &postgres_cdc_runtime_plans_by_connector,
        connector_sender: connector_sender.clone(),
        pending_event_counter: pending_event_counter.clone(),
        ingest_cancel: ingest_cancel.clone(),
        runtime_cancel: runtime_cancel.clone(),
        runtime_failure: Arc::clone(&runtime_failure),
        run_args: &run_args,
        recovered_kafka_offsets: recovered_kafka_offsets.clone(),
        source_journal_skipped_sources: source_journal_skipped_sources.clone(),
        required_columns_by_source_id: Arc::clone(
            &runtime_source_indexes.required_columns_by_source_id,
        ),
        query_batches_by_source_id: Arc::clone(&runtime_source_indexes.query_batches_by_source_id),
        materialized_source_ids: Arc::clone(&runtime_source_indexes.materialized_source_ids),
        kafka_metadata_journal_source_ids: Arc::clone(
            &runtime_source_indexes.kafka_metadata_journal_source_ids,
        ),
        executor_running: Arc::clone(&executor_running),
        storage_reachable: Arc::clone(&storage_reachable),
        runtime_ready: Arc::clone(&runtime_ready),
        watermark_debug: Arc::clone(&watermark_debug),
        cdc_replication_debug: Arc::clone(&cdc_replication_debug),
        cdc_transaction_sender: cdc_transaction_sender.clone(),
        cdc_table_store: cdc_table_store.clone(),
        postgres_cdc_settings,
    });
    drop(connector_sender);
    drop(cdc_transaction_sender);
    let executor_handle = spawn_executor_task(ExecutorTaskContext {
        runtime: ExecutorRuntimeContext {
            event_watermark: Arc::clone(&event_watermark),
            mv_registry: Arc::clone(&mv_registry),
            vectorized_runtime,
            runtime_cancel: runtime_cancel.clone(),
            executor_running: Arc::clone(&executor_running),
            runtime_failure: Arc::clone(&runtime_failure),
        },
        sources: ExecutorSourceContext {
            active_source_definitions_by_id: Arc::clone(
                &runtime_source_indexes.active_source_definitions_by_id,
            ),
            required_columns_by_source_id: Arc::clone(
                &runtime_source_indexes.required_columns_by_source_id,
            ),
            query_batches_by_source_id: Arc::clone(
                &runtime_source_indexes.query_batches_by_source_id,
            ),
            materialized_source_ids: Arc::clone(&runtime_source_indexes.materialized_source_ids),
            source_names_by_id: Arc::clone(&runtime_source_indexes.source_names_by_id),
            source_id_by_name,
            definitions,
            kafka_metadata_journal_source_ids: Arc::clone(
                &runtime_source_indexes.kafka_metadata_journal_source_ids,
            ),
            source_journal_required_sources: Arc::clone(
                &runtime_source_indexes.source_journal_required_sources_for_task,
            ),
        },
        cdc: ExecutorCdcContext {
            cdc_table_store: cdc_table_store.clone(),
            cdc_schemas_by_source_id: Arc::clone(&runtime_source_indexes.cdc_schemas_by_source_id),
            cdc_stateful_table_ids_by_source_id: Arc::clone(
                &runtime_source_indexes.cdc_stateful_table_ids_by_source_id,
            ),
            cdc_transaction_receiver,
            cdc_replication_debug: Arc::clone(&cdc_replication_debug),
            postgres_cdc_commit_senders,
            storage: storage.clone(),
            replication_pipeline_runtime: Arc::clone(&replication_pipeline_runtime),
        },
        ingest: ExecutorIngestContext {
            connector_receiver,
            connector_queues,
            kafka_commit_senders,
            pending_event_counter,
        },
        checkpoint: ExecutorCheckpointContext {
            sink_checkpoint_rx,
            checkpoint_manager,
            tracked_mv_names: planned_materialized_views
                .iter()
                .map(|plan| plan.definition().name().to_string())
                .collect(),
            watermark_debug: Arc::clone(&watermark_debug),
            watermark_idle_source_ms,
        },
        limits: ExecutorBatchLimits {
            max_batch,
            max_batch_per_source,
            max_batch_per_connector,
        },
    });
    let RuntimeFrontendServices {
        query,
        sink_handles,
        signal_handle,
        server_handle,
    } = query_runtime::start_runtime_frontend_services(StartRuntimeFrontendServicesConfig {
        storage: storage.clone(),
        vectorized_source_table_providers,
        planned_materialized_views: &planned_materialized_views,
        mv_registry: &mv_registry,
        sink_specs,
        sink_resume_cursors,
        sink_checkpoint_tx,
        sink_cancel: sink_cancel.clone(),
        runtime_cancel: runtime_cancel.clone(),
        ingest_cancel: ingest_cancel.clone(),
        shutdown_signal: shutdown_signal.clone(),
        service_cancel: service_cancel.clone(),
        runtime_failure: Arc::clone(&runtime_failure),
        pgwire_addr: run_args.pgwire_addr.clone(),
        pgwire_enabled: !run_args.disable_pgwire,
        subscribe_execution_config,
    })
    .await?;
    runtime_ready.store(true, Ordering::Relaxed);

    shutdown_runtime(RuntimeShutdownContext {
        runtime_cancel,
        shutdown_signal,
        ingest_cancel,
        sink_cancel,
        service_cancel,
        connector_handles,
        sink_handles,
        admin_handle,
        cdc_replication_debug_handle,
        executor_handle,
        server_handle,
        signal_handle,
        cancellation_propagation_handle,
        query,
        mv_registry,
        db,
        slatedb_close_timeout_ms: run_args.slatedb_close_timeout_ms,
        runtime_failure,
    })
    .await
}
