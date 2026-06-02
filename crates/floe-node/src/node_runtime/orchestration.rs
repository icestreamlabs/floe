use super::*;

mod checkpointing;
mod postgres_runtime;
mod source_replay;
mod wal_stream;

use checkpointing::{
    VectorizedSourceJournalTransientBatch, build_tick_commit_for_checkpoint,
    build_vectorized_source_journal_commit_batches, notify_postgres_cdc_commit_senders,
    record_postgres_cdc_lsn_progress,
};
use postgres_runtime::{
    insert_catalog_source_definition, insert_replication_pipeline_definition,
    insert_source_backed_table_definition, postgres_schema_evolution_policy_from_catalog,
    validate_replication_pipelines, validate_source_backed_tables,
};
pub(super) use postgres_runtime::{
    merge_catalog_source_connectors, postgres_cdc_runtime_plan,
    validate_materialized_views_do_not_query_raw_cdc_sources,
};
pub(super) use source_replay::{
    kafka_metadata_journal_required_sources, source_journal_required_sources,
};
use source_replay::{
    replay_committed_vectorized_source_journal_entries, source_is_replayable_from_connector,
};
#[cfg(test)]
pub(super) use wal_stream::PostgresCdcRuntimeReconnectPolicy;
use wal_stream::run_native_postgres_cdc_connector;

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
    let vectorized_replay_candidate_sources = all_required_sources.clone();
    let source_journal_required_sources = source_journal_required_sources(
        &source_registry,
        &vectorized_replay_candidate_sources,
        source_journal_mode,
    );
    let kafka_metadata_journal_required_sources = kafka_metadata_journal_required_sources(
        &source_registry,
        &vectorized_replay_candidate_sources,
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
    let source_journal_skipped_sources: BTreeSet<String> = transient_only_sources
        .difference(&source_replay_covered_sources)
        .cloned()
        .collect();
    tracing::info!(
        mode = ?source_journal_mode,
        journaled_sources = ?source_journal_required_sources,
        kafka_metadata_sources = ?kafka_metadata_journal_required_sources,
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
    let vectorized_source_batch_journal =
        VectorizedSourceBatchJournal::new(checkpoint_manager.store().table());
    let kafka_source_journal = KafkaSourceJournal::new(checkpoint_manager.store().table());
    let outer_registry = {
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut registry =
            OuterStreamRegistry::from_validated_sources(&all_required_sources, &mut bridge)
                .await
                .context("initialize outer DBSP streams for sources")?;
        for source in &transient_only_sources {
            registry.set_durable_enabled(source, false);
            let recoverable = source_replay_covered_sources.contains(source);
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

    let _mv_retention = if run_args.mv_retain_last == 0 {
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
    let mut vectorized_runtime = VectorizedExecutionRuntime::new_with_udfs(
        &source_registry,
        vectorized_mv_plans,
        Arc::clone(&mv_registry),
        planner_udfs(),
    )
    .await
    .context("initialize vectorized execution runtime")?;
    let vectorized_source_table_providers = vectorized_runtime.table_providers();
    let mut graph_builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .context("initialize DBSP graph builder")?;
    graph_builder.set_persistence_policy_config(persistence_policy_config);
    if let Some(config) = config.as_ref() {
        graph_builder.set_mv_flush_coalescing(mv_flush_coalescing_config(&config.runtime.mv_flush));
        graph_builder.set_mv_overlay_snapshot(mv_snapshot_config(&config.runtime.mv_snapshot));
    }
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
    let (task_event_tx, mut task_event_rx) =
        mpsc::channel::<GraphTaskError>(GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
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
    for (idx, _plan) in circuit_plans.iter().enumerate() {
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

        tracing::info!(
            view = %view_name,
            namespace = %namespace,
            required_sources = ?required_sources,
            handle_streams = ?handle_streams.keys(),
            transient_streams = ?transient_streams.keys(),
            "skipping legacy DBSP row-wise graph; vectorized runtime owns materialization"
        );
    }
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
    let active_source_definitions_by_id = Arc::new(
        definitions
            .iter()
            .map(|definition| {
                all_required_sources
                    .contains(definition.name())
                    .then_some(definition.clone())
            })
            .collect::<Vec<_>>(),
    );
    let materialized_source_ids = Arc::new(
        definitions
            .iter()
            .map(|definition| all_required_sources.contains(definition.name()))
            .collect::<Vec<_>>(),
    );
    let kafka_metadata_journal_source_ids = Arc::new(
        definitions
            .iter()
            .enumerate()
            .filter_map(|(idx, definition)| {
                kafka_metadata_journal_required_sources
                    .contains(definition.name())
                    .then_some(idx)
            })
            .collect::<Vec<_>>(),
    );
    let source_journal_required_sources_for_task =
        Arc::new(source_journal_required_sources.clone());
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
            &vectorized_source_batch_journal,
            &kafka_source_journal,
            &mut vectorized_runtime,
            tick_commit.tick_id,
            &source_journal_required_sources,
            &kafka_metadata_journal_required_sources,
            &connector_specs,
            &run_args,
            &definitions,
            &source_id_by_name,
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
                let failure_state = Arc::clone(&failure_state);
                connector_handles.push(tokio::spawn(async move {
                    let config = KafkaConnectorConfig {
                        brokers,
                        topics,
                        group_id,
                        default_source,
                        poll_timeout,
                        max_messages_per_tick,
                        message_format: format,
                        commit_offsets_rx: Some(commit_rx),
                        resume_from_offsets,
                    };
                    let mut connector =
                        match KafkaConnector::new(config, definitions, HashMap::new()) {
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
    let active_source_definitions_by_id_for_task = Arc::clone(&active_source_definitions_by_id);
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
    let kafka_metadata_journal_source_ids_for_task = Arc::clone(&kafka_metadata_journal_source_ids);
    let source_journal_required_sources_for_task =
        Arc::clone(&source_journal_required_sources_for_task);
    let source_id_by_name_for_task = source_id_by_name;
    let storage_for_replication_task = storage.clone();
    let replication_pipeline_runtime_for_task = Arc::clone(&replication_pipeline_runtime);
    let mut connector_receiver_for_task = connector_receiver;
    let mut cdc_transaction_receiver_for_task = cdc_transaction_receiver;
    let mut vectorized_runtime_for_task = vectorized_runtime;
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
            let mut tick_kafka_source_ranges =
                vec![
                    None::<HashMap<(Arc<str>, i32), KafkaSourceJournalRangeAccumulator>>;
                    source_count
                ];
            let mut tick_postgres_lsns: HashMap<String, (u64, String)> = HashMap::new();
            let mut tick_postgres_sources: HashMap<String, String> = HashMap::new();
            let mut tick_postgres_table_lsns: Vec<(String, String, String, u64)> = Vec::new();
            let mut tick_source_max_event_ts = vec![None::<i64>; source_count];
            let mut arrow_batches_by_source = vec![Vec::new(); source_count];
            let mut weighted_arrow_batches_by_source = vec![Vec::new(); source_count];
            let mut vectorized_source_journal_batches =
                Vec::<VectorizedSourceJournalTransientBatch>::new();
            let mut arrow_builders_by_source = active_source_definitions_by_id_for_task
                .iter()
                .map(|definition| {
                    definition.as_ref().map(|definition| {
                        SourceArrowBatchBuilder::new(definition.clone(), max_batch_per_source)
                    })
                })
                .collect::<Vec<_>>();
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
                    let Some(definition) = definitions.get(source_id) else {
                        let message =
                            format!("received CDC deltas for unknown source '{source_name}'");
                        tracing::error!(source = %source_name, "{message}");
                        record_runtime_failure(&failure_for_executor, message);
                        executor_cancel.cancel();
                        break 'executor;
                    };
                    match CdcArrowDeltaBatch::from_table_deltas(definition, table_deltas).and_then(
                        |arrow_delta| {
                            let weighted_schema =
                                floe_executor::delta_consolidation::weighted_snapshot_schema(
                                    &definition.to_arrow_schema(),
                                )?;
                            weighted_batch_from_diffs(
                                arrow_delta.record_batch(),
                                &weighted_schema,
                                arrow_delta.diffs(),
                            )
                            .map(|batch| (arrow_delta.len(), batch))
                        },
                    ) {
                        Ok((row_count, batch)) => {
                            decoded_counts[source_id] =
                                decoded_counts[source_id].saturating_add(row_count);
                            decoded_rows_len = decoded_rows_len.saturating_add(row_count);
                            weighted_arrow_batches_by_source[source_id].push(batch);
                        }
                        Err(err) => {
                            let message = format!(
                                "failed to build native CDC Arrow deltas for source '{source_name}': {err}"
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
                let decode_span = tracing::debug_span!(
                    "ingest_decode",
                    epoch = pending_epoch,
                    raw_batch_size = batch_len
                );
                let _decode_guard = decode_span.enter();
                for SelectedAppendIngestEvent {
                    source_id,
                    event,
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
                    let kafka_position = event_fast_kafka_offset(&event)
                        .or_else(|| event_kafka_offset(event.resume_token()));
                    if let Some((partition, offset)) = event_fast_resume_offset(&event)
                        .or_else(|| event_resume_offset(event.resume_token()))
                    {
                        let entry = tick_source_offsets[source_id]
                            .get_or_insert_with(HashMap::new)
                            .entry(partition)
                            .or_insert(0);
                        *entry = (*entry).max(offset);
                    }
                    if let Some((topic, partition, offset)) = kafka_position.clone() {
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
                    let Some(builder) = arrow_builders_by_source
                        .get_mut(source_id)
                        .and_then(Option::as_mut)
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
                    let event_ts = match builder.append_event(&event) {
                        Ok(event_ts) => event_ts,
                        Err(err) => {
                            tracing::warn!(
                                source = %source_name,
                                error = %err,
                                "failed to decode append ingest event into Arrow"
                            );
                            if let Some(ack) = commit_ack {
                                ack.record_failed(format!(
                                    "failed to decode append ingest event for '{source_name}': {err}"
                                ))
                                .await;
                            }
                            continue;
                        }
                    };
                    if kafka_metadata_journal_source_ids_for_task.contains(&source_id)
                        && let Some((topic, partition, offset)) = kafka_position.clone()
                    {
                        observe_kafka_source_journal_event(
                            &mut tick_kafka_source_ranges[source_id],
                            topic,
                            partition,
                            offset,
                            &event,
                        );
                    }
                    // Prefer row-derived event time (from decoded timestamp columns) when available.
                    // Connector-level event_time_ms is a fallback for sources without row timestamps.
                    let event_ts = event_ts.or(event.event_time_ms());
                    if let Some(ts) = event_ts {
                        let ts_i64 = i64::try_from(ts).unwrap_or(i64::MAX);
                        let entry = tick_source_max_event_ts[source_id].get_or_insert(i64::MIN);
                        *entry = (*entry).max(ts_i64);
                    }
                    decoded_counts[source_id] = decoded_counts[source_id].saturating_add(1);
                    if let Some(ack) = commit_ack {
                        commit_acks_by_source[source_id].push(ack);
                    }
                }
                for (source_id, builder) in arrow_builders_by_source.iter_mut().enumerate() {
                    let Some(builder) = builder.as_mut() else {
                        continue;
                    };
                    match builder.finish() {
                        Ok(Some(batch)) => {
                            decoded_rows_len = decoded_rows_len.saturating_add(batch.num_rows());
                            arrow_batches_by_source[source_id].push(batch);
                        }
                        Ok(None) => {}
                        Err(err) => {
                            let source_name = source_names_by_id_for_task[source_id].as_str();
                            tracing::error!(
                                source = %source_name,
                                error = %err,
                                "failed to finish Arrow ingest batch"
                            );
                            record_runtime_failure(
                                &failure_for_executor,
                                format!(
                                    "failed to finish Arrow ingest batch for '{source_name}': {err}"
                                ),
                            );
                            executor_cancel.cancel();
                            break 'executor;
                        }
                    }
                }
                if decoded_rows_len == 0 {
                    continue;
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
                            .persist_tick_commit_with_staged_writes(tick_commit, staged_writes)
                            .await
                    } else {
                        checkpoint_manager.persist_tick_commit(tick_commit).await
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
            for source_id in 0..source_count {
                let source_name = source_names_by_id_for_task[source_id].as_str();
                if !source_journal_required_sources_for_task.contains(source_name) {
                    continue;
                }
                let Some(definition) = definitions.get(source_id) else {
                    continue;
                };
                let source_schema = definition.to_arrow_schema();
                let weighted_schema =
                    match floe_executor::delta_consolidation::weighted_snapshot_schema(
                        &source_schema,
                    ) {
                        Ok(schema) => schema,
                        Err(err) => {
                            let message = format!(
                                "failed to build vectorized source journal schema for '{source_name}': {err}"
                            );
                            tracing::error!(source = %source_name, error = %err, "{message}");
                            record_runtime_failure(&failure_for_executor, message);
                            executor_cancel.cancel();
                            break 'executor;
                        }
                    };
                let mut journal_batches = Vec::with_capacity(
                    arrow_batches_by_source[source_id].len()
                        + weighted_arrow_batches_by_source[source_id].len(),
                );
                for batch in &arrow_batches_by_source[source_id] {
                    match floe_executor::delta_consolidation::add_weight_column(
                        batch,
                        &weighted_schema,
                        1,
                    ) {
                        Ok(weighted) => journal_batches.push(weighted),
                        Err(err) => {
                            let message = format!(
                                "failed to build vectorized source journal batch for '{source_name}': {err}"
                            );
                            tracing::error!(source = %source_name, error = %err, "{message}");
                            record_runtime_failure(&failure_for_executor, message);
                            executor_cancel.cancel();
                            break 'executor;
                        }
                    }
                }
                journal_batches.extend(weighted_arrow_batches_by_source[source_id].iter().cloned());
                if !journal_batches.is_empty() {
                    vectorized_source_journal_batches.push((
                        source_id,
                        tick_source_max_event_ts[source_id],
                        journal_batches,
                    ));
                }
            }
            let mut changed = false;
            for (source_id, batches) in arrow_batches_by_source.into_iter().enumerate() {
                let source_name = source_names_by_id_for_task[source_id].as_str();
                for batch in batches {
                    if let Err(err) = vectorized_runtime_for_task
                        .append_source_batch(source_name, batch)
                        .await
                    {
                        tracing::error!(
                            source = %source_name,
                            error = %err,
                            "failed to append Arrow source batch"
                        );
                        for ack in commit_acks_by_source[source_id].drain(..) {
                            ack.record_failed(format!(
                                "failed to append Arrow source batch for '{source_name}': {err}"
                            ))
                            .await;
                        }
                        continue;
                    }
                    changed = true;
                }
            }
            for (source_id, batches) in weighted_arrow_batches_by_source.into_iter().enumerate() {
                let source_name = source_names_by_id_for_task[source_id].as_str();
                for batch in batches {
                    if let Err(err) = vectorized_runtime_for_task
                        .apply_weighted_source_delta(source_name, batch)
                        .await
                    {
                        tracing::error!(
                            source = %source_name,
                            error = %err,
                            "failed to apply weighted Arrow source delta"
                        );
                        for ack in commit_acks_by_source[source_id].drain(..) {
                            ack.record_failed(format!(
                                "failed to apply weighted Arrow source delta for '{source_name}': {err}"
                            ))
                            .await;
                        }
                        continue;
                    }
                    changed = true;
                }
            }
            for acks in &mut commit_acks_by_source {
                tick_commit_acks.append(acks);
            }
            let mut kafka_metadata_journal_batches = Vec::new();
            for &source_id in kafka_metadata_journal_source_ids_for_task.iter() {
                let Some(ranges_by_partition) = tick_kafka_source_ranges[source_id].take() else {
                    continue;
                };
                let mut ranges = ranges_by_partition
                    .into_values()
                    .map(KafkaSourceJournalRangeAccumulator::into_range)
                    .collect::<Vec<_>>();
                ranges.sort_by(|left, right| {
                    left.topic
                        .cmp(&right.topic)
                        .then(left.partition.cmp(&right.partition))
                });
                if ranges.is_empty() {
                    continue;
                }
                kafka_metadata_journal_batches.push((
                    source_names_by_id_for_task[source_id].clone(),
                    tick_source_max_event_ts[source_id],
                    ranges,
                ));
            }
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
            let tick_all_start = Instant::now();
            if let Err(err) = vectorized_runtime_for_task
                .run_tick(i64::try_from(epoch).unwrap_or(i64::MAX))
                .await
            {
                metrics::observe_tick_phase_latency_ms(
                    "state_write",
                    tick_all_start.elapsed().as_millis() as u64,
                );
                tracing::error!(epoch, error = %err, "failed to run vectorized materialization tick");
                metrics::inc_ingest_tick("error");
                continue;
            } else if should_sample(&TICK_LOG_COUNTER, TICK_LOG_SAMPLE_EVERY) {
                metrics::observe_tick_phase_latency_ms(
                    "state_write",
                    tick_all_start.elapsed().as_millis() as u64,
                );
                tracing::debug!(epoch, "completed vectorized materialization tick");
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

            let mv_visibility_start = Instant::now();
            let target_mv_version = i64::try_from(epoch).unwrap_or(i64::MAX);
            match wait_for_materialized_views_visible(
                &mv_for_task,
                target_mv_version,
                &executor_cancel,
            )
            .await
            {
                Ok(waited_views) => {
                    let mv_visibility_latency_ms = mv_visibility_start.elapsed().as_millis() as u64;
                    metrics::observe_tick_phase_latency_ms(
                        "mv_visibility",
                        mv_visibility_latency_ms,
                    );
                    if waited_views > 0 && (epoch <= 8 || epoch % 128 == 0) {
                        tracing::info!(
                            epoch,
                            waited_views,
                            mv_visibility_latency_ms,
                            "tick materialized views visible"
                        );
                    }
                }
                Err(err) => {
                    metrics::observe_tick_phase_latency_ms(
                        "mv_visibility",
                        mv_visibility_start.elapsed().as_millis() as u64,
                    );
                    tracing::error!(
                        epoch,
                        error = %err,
                        "failed while waiting for materialized view visibility"
                    );
                    record_runtime_failure(
                        &failure_for_executor,
                        format!(
                            "failed waiting for materialized view visibility at tick {epoch}: {err}"
                        ),
                    );
                    executor_cancel.cancel();
                    break 'executor;
                }
            }

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
            let vectorized_source_journal_commit_batches =
                build_vectorized_source_journal_commit_batches(
                    &source_names_by_id_for_task,
                    &vectorized_source_journal_batches,
                );
            let mut staged_writes_for_checkpoint = cdc_staged_writes;
            let mut vectorized_journal_stage_error = None;
            if !vectorized_source_journal_commit_batches.is_empty() {
                let staged_writes =
                    staged_writes_for_checkpoint.get_or_insert_with(WriteBatch::new);
                for (source, max_event_time_ms, batches) in
                    &vectorized_source_journal_commit_batches
                {
                    if let Err(err) = append_vectorized_entry_to_batch(
                        staged_writes,
                        source,
                        epoch,
                        *max_event_time_ms,
                        batches,
                    ) {
                        vectorized_journal_stage_error = Some(err);
                        break;
                    }
                }
            }
            let checkpoint_write_start = Instant::now();
            let checkpoint_result = if let Some(err) = vectorized_journal_stage_error {
                Err(err)
            } else if let Some(staged_writes) = staged_writes_for_checkpoint {
                checkpoint_manager
                    .persist_tick_commit_with_kafka_metadata_and_staged_writes(
                        tick_commit,
                        &kafka_metadata_journal_batches,
                        staged_writes,
                    )
                    .await
            } else {
                checkpoint_manager
                    .persist_tick_commit_with_kafka_metadata(
                        tick_commit,
                        &kafka_metadata_journal_batches,
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

    let query = FloeQueryContext::new(storage);
    query
        .preload_tables()
        .await
        .context("failed to register tables with DataFusion")?;
    {
        let session = query.session();
        for (name, provider) in vectorized_source_table_providers {
            let _ = session.deregister_table(&name);
            session
                .register_table(&name, provider)
                .with_context(|| format!("register vectorized source table {name}"))?;
        }
    }
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
            subscribe: subscribe_execution_config,
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
