use super::*;

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

    if run_args.config.is_none()
        && run_args.kafka_brokers.is_some()
        && run_args.kafka_topics.is_empty()
    {
        return Err(anyhow::anyhow!(
            "--kafka-topics is required when --kafka-brokers is set"
        ));
    }
    let awaited_durable = run_args.slatedb_await_durable.unwrap_or(true);
    SlateTable::set_default_await_durable(awaited_durable);
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

    let (connector_specs, mut sink_specs) = if let Some(config) = config.as_ref() {
        let connectors = normalize_connectors(config.connectors.clone())?;
        if connectors.is_empty() {
            return Err(anyhow!("config must declare at least one connector"));
        }
        let sinks = normalize_sinks(config.sinks.clone())?;
        (connectors, sinks)
    } else {
        let connectors = normalize_connectors(connectors_from_cli(&run_args))?;
        (connectors, Vec::new())
    };
    log_startup_banner(&run_args, &connector_specs);

    let mut source_registry = SourceRegistry::new();
    source_registry.extend(floe_node_core::generator::definitions()?);

    let slate_settings = load_slatedb_settings(&run_args)?;
    let storage = if run_args.dry_run {
        None
    } else {
        Some(server::init_storage(slate_settings).await?)
    };
    let mut materialized_view_map: HashMap<String, MaterializedViewDefinition> = HashMap::new();
    let mut sql_sink_specs = Vec::new();
    if let Some(storage) = storage.as_ref() {
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
                FloeStatement::CreateTable(definition) => {
                    let table = table_definition_from_sql(&definition)?;
                    if let Some(storage) = storage.as_ref() {
                        storage.upsert_table(table.clone()).await.with_context(|| {
                            format!("persist table definition '{}'", table.name())
                        })?;
                    }
                    source_registry.register(source_definition_from_table(&table)?);
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
                FloeStatement::Tail { .. } => {
                    return Err(anyhow!(
                        "TAIL statements are not supported in --mv-query programs"
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
    apply_connector_properties(&mut source_registry, &connector_specs);
    let available_sources = available_sources_from_registry(&source_registry);

    let mut materialized_views: Vec<MaterializedViewDefinition> =
        materialized_view_map.into_values().collect();
    materialized_views.sort_by(|a, b| a.name().cmp(b.name()));
    log_operator_hints(
        &connector_specs,
        &available_sources,
        &materialized_views,
        &sink_specs,
    );

    let planned_materialized_views =
        plan_materialized_views(&source_registry, &materialized_views).await?;
    let circuit_plans = build_dataflows(&planned_materialized_views, &available_sources)?;
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
    all_required_sources.extend(
        source_registry
            .definitions()
            .iter()
            .map(|definition| definition.name().to_string()),
    );
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
    let checkpoint_manager = CheckpointManager::new(CHECKPOINT_GRAPH_ID, checkpoint_table)
        .await
        .context("initialize tick checkpoint manager")?;
    let initial_sink_cursors = checkpoint_manager.snapshot_sink_cursors();
    if let Some(tick_commit) = checkpoint_manager.latest_tick_commit() {
        metrics::record_last_committed_tick(tick_commit.tick_id);
    }
    let outer_registry = {
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        OuterStreamRegistry::from_validated_sources(&all_required_sources, &mut bridge)
            .await
            .context("initialize outer DBSP streams for sources")?
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
    if let Some(config) = config.as_ref() {
        graph_builder.set_mv_flush_coalescing(mv_flush_coalescing_config(&config.runtime.mv_flush));
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
        let handle_streams = {
            let registry_guard = outer_registry.lock().await;
            gather_handle_streams(&registry_guard, required_sources)
        };
        tracing::info!(
            view = %view_name,
            namespace = %namespace,
            required_sources = ?required_sources,
            handle_streams = ?handle_streams.keys(),
            "building DBSP graph"
        );

        graph_builder
            .build(BuildInputs {
                graph_id: view_name,
                view_name,
                plan,
                cancel: graph_cancel.clone(),
                task_events: task_event_tx.clone(),
                mv_registry: Arc::clone(&mv_registry),
                outer_handle_streams: &handle_streams,
                mv_retention,
                watermark: Arc::clone(&event_watermark),
            })
            .await
            .with_context(|| format!("building DBSP graph for '{view_name}'"))?;
    }
    let decoder_registry: HashMap<String, SourceRowDecoder> = source_registry
        .definitions()
        .iter()
        .filter(|definition| all_required_sources.contains(definition.name()))
        .map(|definition| {
            (
                definition.name().to_string(),
                SourceRowDecoder::new(definition.clone()),
            )
        })
        .collect();
    let decoder_registry = Arc::new(decoder_registry);

    let queue_capacity = run_args.ingest_queue_capacity;
    let max_batch = run_args.ingest_batch_size;
    let max_batch_per_source = run_args.ingest_batch_per_source;
    let max_batch_per_connector = run_args.ingest_batch_per_connector;
    let configured_watermark_idle_source_ms = config
        .as_ref()
        .and_then(|cfg| cfg.runtime.watermark_idle_source_ms);

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

    let admin_port = std::env::var(ADMIN_PORT_ENV)
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(DEFAULT_ADMIN_PORT);
    let watermark_debug = Arc::new(tokio::sync::RwLock::new(http_ingest::WatermarkDebugState {
        policy: "min_active_sources".to_string(),
        ..http_ingest::WatermarkDebugState::default()
    }));
    let admin_health = HttpIngestHealth {
        executor_running: Arc::clone(&executor_running),
        storage_reachable: Arc::clone(&storage_reachable),
        runtime_ready: Arc::clone(&runtime_ready),
        watermark_debug: Some(Arc::clone(&watermark_debug)),
    };
    let admin_config = HttpAdminConfig {
        host: run_args.http_host.clone(),
        port: admin_port,
        health: admin_health,
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
    let connector_count = connector_specs.len();
    let per_connector_queue_capacity = (queue_capacity / connector_count).max(1);
    tracing::info!(
        connector_count,
        queue_capacity,
        per_connector_queue_capacity,
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
    let (sink_checkpoint_tx, sink_checkpoint_rx) = mpsc::unbounded_channel::<SinkCursor>();
    let sink_resume_cursors: HashMap<String, SinkCursor> = initial_sink_cursors
        .iter()
        .cloned()
        .map(|cursor| (cursor.sink.clone(), cursor))
        .collect();

    for connector in connector_specs {
        let (sender, receiver) = core_source::channel(per_connector_queue_capacity);
        connector_queues.push(ConnectorQueue::new(connector.name.clone(), receiver));
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
                    };
                    let mut connector = match KafkaConnector::new(config, definitions) {
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
                poll_ms,
                max_changes,
                default_schema,
                include_tables,
                include_schema_in_source,
                ..
            } => {
                let poll_interval = Duration::from_millis(poll_ms.unwrap_or(1000));
                let max_changes = max_changes.unwrap_or(1000);
                let default_schema = default_schema.unwrap_or_else(|| "public".to_string());
                let include_schema_in_source = include_schema_in_source.unwrap_or(false);
                let (commit_tx, commit_rx) = watch::channel(PostgresCdcCommit::default());
                postgres_cdc_commit_senders.push(commit_tx);
                let definitions = definitions.clone();
                let failure_state = Arc::clone(&failure_state);
                connector_handles.push(tokio::spawn(async move {
                    let config = PostgresCdcConnectorConfig {
                        connection_string: connection,
                        slot,
                        poll_interval,
                        max_changes,
                        default_schema,
                        include_tables,
                        include_schema_in_source,
                        commit_lsn_rx: Some(commit_rx),
                    };
                    let mut connector = match PostgresCdcConnector::new(config, definitions) {
                        Ok(connector) => connector,
                        Err(err) => {
                            tracing::error!(error = %err, "Postgres CDC connector config invalid");
                            record_runtime_failure(
                                &failure_state,
                                format!("Postgres CDC connector config invalid: {err}"),
                            );
                            runtime_cancel.cancel();
                            return;
                        }
                    };
                    let ctx = ConnectorContext::new(sender);
                    if let Err(err) = run_connector(&mut connector, &ctx, cancel.clone()).await {
                        tracing::error!(error = %err, "Postgres CDC connector failed");
                        record_runtime_failure(
                            &failure_state,
                            format!("Postgres CDC connector failed: {err}"),
                        );
                        runtime_cancel.cancel();
                    }
                }));
            }
        }
    }
    let outer_for_task = Arc::clone(&outer_registry);
    let decoder_for_task = Arc::clone(&decoder_registry);
    let watermark_for_task = Arc::clone(&event_watermark);
    let mv_for_task = Arc::clone(&mv_registry);
    let kafka_commit_senders_for_task = kafka_commit_senders;
    let postgres_cdc_commit_senders_for_task = postgres_cdc_commit_senders;
    let mut sink_checkpoint_rx_for_task = sink_checkpoint_rx;
    const MAX_SINK_CURSOR_UPDATES_PER_ITER: usize = 4096;
    let watermark_debug_for_task = Arc::clone(&watermark_debug);
    let executor_running_for_task = Arc::clone(&executor_running);
    let failure_for_executor = Arc::clone(&runtime_failure);
    let tracked_mv_names: Vec<String> = planned_materialized_views
        .iter()
        .map(|plan| plan.definition().name().to_string())
        .collect();
    let executor_cancel = runtime_cancel.clone();
    let executor_handle: JoinHandle<()> = tokio::spawn(async move {
        let mut connector_queues = connector_queues;
        let mut checkpoint_manager = checkpoint_manager;
        let mut next_connector = 0usize;
        let mut epoch: u64 = 0;
        let mut last_mv_versions: HashMap<String, u64> = HashMap::new();
        let mut committed_source_offsets: HashMap<(String, u32), u64> = HashMap::new();
        let mut latest_source_offsets: HashMap<(String, u32), u64> = HashMap::new();
        let mut mv_last_update_at_ms: HashMap<String, u64> = tracked_mv_names
            .iter()
            .map(|view| (view.clone(), current_unix_time_ms()))
            .collect();
        let mut last_checkpoint_commit_at = Instant::now();
        let mut source_watermarks: HashMap<String, i64> = HashMap::new();
        let mut source_last_seen_at: HashMap<String, Instant> = HashMap::new();
        let pre_tick_commit_delay_ms = std::env::var("FLOE_TEST_PRE_TICK_COMMIT_DELAY_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let watermark_idle_source_ms = std::env::var("FLOE_WATERMARK_IDLE_SOURCE_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(
                configured_watermark_idle_source_ms.unwrap_or(DEFAULT_WATERMARK_IDLE_SOURCE_MS),
            );
        let watermark_idle_timeout = Duration::from_millis(watermark_idle_source_ms);
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
            if connector_queues.is_empty() {
                break;
            }
            if connector_queues
                .iter()
                .all(|queue| queue.pending.is_empty())
            {
                let has_events = tokio::select! {
                    _ = executor_cancel.cancelled() => false,
                    has_events = recv_from_any(&mut connector_queues) => has_events,
                };
                if !has_events {
                    break;
                }
            }
            drain_connectors(&mut connector_queues, per_connector_queue_capacity);
            connector_queues.retain(|queue| !(queue.closed && queue.pending.is_empty()));

            let BatchSelection {
                batch,
                per_connector_counts,
            } = build_batch(
                &mut connector_queues,
                next_connector,
                max_batch,
                max_batch_per_source,
                max_batch_per_connector,
            );

            if batch.is_empty() {
                continue;
            }

            next_connector = if connector_queues.is_empty() {
                0
            } else {
                (next_connector + 1) % connector_queues.len()
            };

            let pending_epoch = epoch.saturating_add(1);
            let batch_len = batch.len();
            let decode_start = Instant::now();
            let mut decoded_rows = Vec::new();
            let mut encoded_rows = Vec::with_capacity(batch_len);
            let mut decoded_counts: HashMap<String, usize> = HashMap::new();
            let mut tick_source_offsets: HashMap<(String, u32), u64> = HashMap::new();
            let mut tick_kafka_offsets: HashMap<(String, i32), i64> = HashMap::new();
            let mut tick_postgres_lsns: HashMap<String, (u64, String)> = HashMap::new();
            let mut tick_source_max_event_ts: HashMap<String, i64> = HashMap::new();
            let decode_span = tracing::debug_span!(
                "ingest_decode",
                epoch = pending_epoch,
                raw_batch_size = batch_len
            );
            let _decode_guard = decode_span.enter();
            for event in batch {
                let source_name = event.source().to_string();
                if let Some((partition, offset)) = event_resume_offset(event.resume_token()) {
                    let entry = tick_source_offsets
                        .entry((source_name.clone(), partition))
                        .or_insert(0);
                    *entry = (*entry).max(offset);
                }
                if let Some((topic, partition, offset)) = event_kafka_offset(event.resume_token()) {
                    let entry = tick_kafka_offsets.entry((topic, partition)).or_insert(0);
                    *entry = (*entry).max(offset);
                }
                if let Some((slot, lsn_value, lsn_text)) = event_postgres_lsn(event.resume_token())
                {
                    let entry = tick_postgres_lsns
                        .entry(slot)
                        .or_insert_with(|| (lsn_value, lsn_text.clone()));
                    if lsn_value > entry.0 {
                        *entry = (lsn_value, lsn_text);
                    }
                }
                let decoder = match lookup_decoder_for_source(&decoder_for_task, &source_name) {
                    Ok(decoder) => decoder,
                    Err(err) => {
                        let message = err.to_string();
                        tracing::error!(source = %source_name, "{message}");
                        record_runtime_failure(&failure_for_executor, message);
                        executor_cancel.cancel();
                        break 'executor;
                    }
                };
                let event_ts = if let Some(preencoded_row_key) = event.preencoded_row_key() {
                    encoded_rows.push((source_name.clone(), preencoded_row_key.to_vec()));
                    None
                } else {
                    match decoder.encode_row_key(&event) {
                        Ok((encoded, event_ts)) => {
                            encoded_rows.push((source_name.clone(), encoded));
                            event_ts
                        }
                        Err(err) => {
                            tracing::debug!(
                                source = %source_name,
                                error = %err,
                                "direct source encoding fell back to row decode"
                            );
                            let (row, event_ts) = match decoder.decode(&event) {
                                Ok(result) => result,
                                Err(err) => {
                                    tracing::warn!(
                                        source = %source_name,
                                        error = %err,
                                        "failed to decode source event"
                                    );
                                    continue;
                                }
                            };
                            decoded_rows.push((source_name.clone(), row));
                            event_ts
                        }
                    }
                };
                let event_ts = event.event_time_ms().or(event_ts);
                if let Some(ts) = event_ts {
                    let ts_i64 = i64::try_from(ts).unwrap_or(i64::MAX);
                    let entry = tick_source_max_event_ts
                        .entry(source_name.clone())
                        .or_insert(i64::MIN);
                    *entry = (*entry).max(ts_i64);
                }
                *decoded_counts.entry(source_name.clone()).or_insert(0) += 1;
            }
            let decode_latency_ms = decode_start.elapsed().as_millis() as u64;
            metrics::observe_decode_latency_ms(decode_latency_ms);
            metrics::observe_tick_phase_latency_ms("decode", decode_latency_ms);
            tracing::debug!(
                decoded_rows = decoded_rows.len().saturating_add(encoded_rows.len()),
                latency_ms = decode_latency_ms,
                "decoded ingest batch"
            );

            if decoded_rows.is_empty() && encoded_rows.is_empty() {
                continue;
            }

            let decoded_rows_len = decoded_rows.len().saturating_add(encoded_rows.len());
            let mut registry = outer_for_task.lock().await;
            let mut changed = false;
            for (source_name, row) in decoded_rows {
                let Some(writer) = registry.writer_mut(&source_name) else {
                    tracing::warn!(
                        source = %source_name,
                        "no writer for source, skipping row"
                    );
                    continue;
                };
                if let Err(err) = writer.append(&row, 1) {
                    tracing::error!(
                        source = %source_name,
                        error = %err,
                        "failed to append row"
                    );
                    continue;
                }
                changed = true;
                if should_sample(&INGEST_LOG_COUNTER, INGEST_LOG_SAMPLE_EVERY) {
                    if source_name == generator::BID_SOURCE_NAME {
                        tracing::debug!(row = ?row, "ingested bid row");
                    } else if source_name == generator::AUCTION_SOURCE_NAME {
                        tracing::debug!(row = ?row, "ingested auction row");
                    }
                }
            }
            for (source_name, encoded) in encoded_rows {
                let Some(writer) = registry.writer_mut(&source_name) else {
                    tracing::warn!(
                        source = %source_name,
                        "no writer for source, skipping encoded row"
                    );
                    continue;
                };
                if let Err(err) = writer.append_encoded(encoded, 1) {
                    tracing::error!(
                        source = %source_name,
                        error = %err,
                        "failed to append encoded row"
                    );
                    continue;
                }
                changed = true;
            }

            if !changed {
                continue;
            }

            epoch = pending_epoch;
            let now_instant = Instant::now();
            for (source, max_event_ts) in tick_source_max_event_ts {
                let watermark_entry = source_watermarks.entry(source.clone()).or_insert(i64::MIN);
                *watermark_entry = (*watermark_entry).max(max_event_ts);
                metrics::record_source_watermark_ms(&source, *watermark_entry);
                source_last_seen_at.insert(source, now_instant);
            }
            if let Some(global_candidate) = compute_global_watermark(
                &source_watermarks,
                &source_last_seen_at,
                now_instant,
                watermark_idle_timeout,
            ) {
                let prev = watermark_for_task.load(Ordering::Relaxed);
                let next = advance_global_watermark(prev, Some(global_candidate));
                if next != prev {
                    watermark_for_task.store(next, Ordering::Relaxed);
                }
                if next >= 0 {
                    metrics::record_global_watermark_ms(next);
                    mv_for_task.update_watermark_all(next as u64);
                    let now_ms = current_unix_time_ms();
                    let watermark_ms = u64::try_from(next).unwrap_or(u64::MAX);
                    metrics::record_watermark_lag_ms(now_ms.saturating_sub(watermark_ms));
                }
            }
            {
                let mut debug_state = watermark_debug_for_task.write().await;
                debug_state.updated_at_unix_ms = current_unix_time_ms();
                let global = watermark_for_task.load(Ordering::Relaxed);
                debug_state.global_watermark_ms = (global >= 0).then_some(global);
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
            let tick_all_start = Instant::now();
            if let Err(err) = registry.tick_all().await {
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
            for ((source, partition), offset) in &tick_source_offsets {
                let key = (source.clone(), *partition);
                let latest_entry = latest_source_offsets.entry(key.clone()).or_insert(0);
                *latest_entry = (*latest_entry).max(*offset);
                let committed_offset = committed_source_offsets.get(&key).copied().unwrap_or(0);
                metrics::record_source_offset_lag(
                    source.as_str(),
                    *partition,
                    latest_entry.saturating_sub(committed_offset),
                );
            }
            for ((source, partition), offset) in &tick_source_offsets {
                checkpoint_manager.update_partition_offset(source.as_str(), *partition, *offset);
            }
            let frontier = watermark_for_task
                .load(Ordering::Relaxed)
                .max(0)
                .try_into()
                .unwrap_or(0_u64);
            let mv_versions = collect_mv_versions_for_commit(&mv_for_task, &mut last_mv_versions);
            let tick_commit = TickCommit::new(
                epoch,
                frontier,
                checkpoint_manager.snapshot_offsets(),
                mv_versions.clone(),
                checkpoint_manager.snapshot_sink_cursors(),
            );
            let committed_at_ms = tick_commit.committed_at_unix_ms;
            let checkpoint_write_start = Instant::now();
            if let Err(err) = checkpoint_manager.persist_tick_commit(tick_commit).await {
                metrics::observe_tick_phase_latency_ms(
                    "checkpoint_write",
                    checkpoint_write_start.elapsed().as_millis() as u64,
                );
                tracing::error!(epoch, error = %err, "failed to persist tick commit");
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
            for ((source, partition), offset) in &tick_source_offsets {
                let key = (source.clone(), *partition);
                let committed_entry = committed_source_offsets.entry(key.clone()).or_insert(0);
                *committed_entry = (*committed_entry).max(*offset);
                let latest_offset = latest_source_offsets.get(&key).copied().unwrap_or(*offset);
                metrics::record_source_offset_lag(
                    source.as_str(),
                    *partition,
                    latest_offset.saturating_sub(*committed_entry),
                );
            }
            for mv_version in &mv_versions {
                mv_last_update_at_ms.insert(mv_version.view.clone(), committed_at_ms);
            }
            record_mv_freshness_metrics(&mv_last_update_at_ms, current_unix_time_ms());
            metrics::record_last_committed_tick(epoch);
            metrics::record_checkpoint_age_seconds(0);
            last_checkpoint_commit_at = Instant::now();
            if !tick_kafka_offsets.is_empty() && !kafka_commit_senders_for_task.is_empty() {
                let kafka_commit_start = Instant::now();
                let commit = build_kafka_offset_commit(epoch, &tick_kafka_offsets);
                for sender in &kafka_commit_senders_for_task {
                    let _ = sender.send(commit.clone());
                }
                metrics::observe_tick_phase_latency_ms(
                    "kafka_commit_notify",
                    kafka_commit_start.elapsed().as_millis() as u64,
                );
            }
            if !tick_postgres_lsns.is_empty() && !postgres_cdc_commit_senders_for_task.is_empty() {
                let postgres_commit_start = Instant::now();
                let commit = build_postgres_cdc_commit(epoch, &tick_postgres_lsns);
                for sender in &postgres_cdc_commit_senders_for_task {
                    let _ = sender.send(commit.clone());
                }
                metrics::observe_tick_phase_latency_ms(
                    "postgres_cdc_commit_notify",
                    postgres_commit_start.elapsed().as_millis() as u64,
                );
            }
            let tick_latency_ms = tick_start.elapsed().as_millis() as u64;
            metrics::observe_tick_latency_ms(tick_latency_ms);
            tracing::debug!(tick_latency_ms, "connector tick completed");

            let queue_depth: usize = connector_queues
                .iter()
                .map(|queue| queue.pending.len() + queue.receiver.len())
                .sum();
            metrics::record_ingest_queue_depth(queue_depth);
            if should_sample(&INGEST_METRICS_COUNTER, INGEST_METRICS_SAMPLE_EVERY) {
                tracing::info!(
                    epoch,
                    queue_depth,
                    batch_size = batch_len,
                    pending = queue_depth,
                    decoded_rows = decoded_rows_len,
                    max_batch,
                    max_batch_per_source,
                    max_batch_per_connector,
                    decode_latency_ms,
                    state_write_latency_ms,
                    checkpoint_write_latency_ms,
                    tick_latency_ms,
                    per_source = ?decoded_counts,
                    per_connector = ?per_connector_counts,
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
            tracing::warn!(error = %err, "best-effort final checkpoint persistence failed");
        }
        executor_running_for_task.store(false, Ordering::Relaxed);
    });

    let source_bridge = Arc::new(Mutex::new(DbspBridge::new(Arc::clone(&db)).await?));
    let query = FloeQueryContext::new(storage);
    query
        .preload_tables()
        .await
        .context("failed to register tables with DataFusion")?;
    register_source_tables(&query, &source_registry, Arc::clone(&source_bridge))
        .await
        .context("register source tables")?;
    register_materialized_view_tables(&query, &planned_materialized_views, &mv_registry)
        .await
        .context("register materialized view tables")?;
    runtime_ready.store(true, Ordering::Relaxed);

    let sink_handles = sinks::spawn_sinks(
        sink_specs,
        query.clone(),
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

    let server_handle = spawn_pgwire_server(
        query.clone(),
        Arc::clone(&mv_registry),
        service_cancel.clone(),
        runtime_cancel.clone(),
        Arc::clone(&runtime_failure),
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

    if let Some(message) = runtime_failure
        .lock()
        .expect("runtime failure lock poisoned")
        .clone()
    {
        return Err(anyhow!(message));
    }

    server_result
}
