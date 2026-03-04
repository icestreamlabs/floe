mod cli;
mod config;
mod http_ingest;
mod metrics;
mod sinks;

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use clap::Parser;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::DFSchemaRef;
use dbsp::collections::CompactionPolicy;
use dbsp::storage::gc::{GcPolicy, GcService};
use dbsp::storage::{KeyValueTable, SlateTable};
use dbsp::{CompactionSchedulerConfig, StreamRetention};
use floe_core::catalog::{ColumnDefinition, ColumnType, TableDefinition};
use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};
use floe_executor::checkpoint::{CheckpointManager, MaterializedViewTickVersion, TickCommit};
use floe_executor::{
    BuildInputs, ConsolidationMode, DbspBridge, DbspGraphBuilder, FloeQueryContext, GraphTaskError,
    MaterializedViewRegistry, MaterializedViewTableProvider, OuterStreamRegistry, SourceRowDecoder,
    SourceTableProvider, ValidatedPlan, validate_dbsp_plan,
};
use floe_node_core::connector::{ConnectorContext, run_connector};
use floe_node_core::file_connector::{FileConnector, FileConnectorConfig};
use floe_node_core::generator;
use floe_node_core::kafka_connector::{
    KafkaConnector, KafkaConnectorConfig, KafkaOffsetCommit, KafkaTopicPartitionOffset,
};
use floe_node_core::object_store_connector::{ObjectStoreConnector, ObjectStoreConnectorConfig};
use floe_node_core::planner::{
    PlannedMaterializedView, camel_case_schema, plan_materialized_views,
};
use floe_node_core::postgres_cdc_connector::{
    PostgresCdcCommit, PostgresCdcConnector, PostgresCdcConnectorConfig, PostgresSlotCommit,
};
use floe_node_core::tail_client;
use floe_server as server;
use floe_sql_parser::{
    CreateTableDefinition, FloeStatement, MaterializedViewDefinition, SqlColumnType,
    parse_floe_program,
};
use floe_storage::MaterializedViewMetadata;
use futures::future::select_all;
use slatedb::config::{CompactorOptions, Settings};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::{
    ConnectorConfig, NodeConfig, OutputConsolidationModeConfig, SinkConfig, SinkSpec,
    apply_connector_properties, load_config, materialized_view_definitions_from_config,
    normalize_connectors, normalize_sinks, sink_spec_from_sql,
};

static INGEST_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
static TICK_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
static INGEST_METRICS_COUNTER: AtomicU64 = AtomicU64::new(0);
const INGEST_LOG_SAMPLE_EVERY: u64 = 512;
const TICK_LOG_SAMPLE_EVERY: u64 = 128;
const INGEST_METRICS_SAMPLE_EVERY: u64 = 128;
const SLATEDB_CONFIG_ENV: &str = "FLOE_SLATEDB_CONFIG";
const SLATEDB_ENV_PREFIX_ENV: &str = "FLOE_SLATEDB_ENV_PREFIX";
const DEFAULT_SLATEDB_ENV_PREFIX: &str = "SLATEDB_";
const DEFAULT_EVENTS_PER_SECOND: f64 = 10.0;
const DEFAULT_MV_RETAIN_LAST: usize = 1;
const DEFAULT_ZSET_COMPACTION_MAX_CHAIN_LEN: usize = 32;
const DEFAULT_ZSET_COMPACTION_MAX_SEGMENTS: usize = 256;
const DEFAULT_ZSET_COMPACTION_BACKOFF_TICKS: u64 = 1;
const DEFAULT_ZSET_COMPACTION_MAX_CONCURRENT_JOBS: usize = 1;
const DEFAULT_ZSET_GC_GRACE_PERIOD_MS: u64 = 30_000;
const DEFAULT_HTTP_HOST: &str = "127.0.0.1";
const DEFAULT_KAFKA_GROUP_ID: &str = "floe";
const DEFAULT_KAFKA_POLL_MS: u64 = 100;
const DEFAULT_KAFKA_MAX_MESSAGES: usize = 256;
const DEFAULT_INGEST_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_INGEST_BATCH_SIZE: usize = 256;
const DEFAULT_INGEST_BATCH_PER_SOURCE: usize = 64;
const DEFAULT_INGEST_BATCH_PER_CONNECTOR: usize = 64;
const DEFAULT_ADMIN_PORT: u16 = 8081;
const CHECKPOINT_GRAPH_ID: &str = "floe_runtime";
const SOURCE_PRIMARY_KEY_PROPERTY: &str = "primary_key";
const ADMIN_PORT_ENV: &str = "FLOE_ADMIN_PORT";
const DEFAULT_WATERMARK_IDLE_SOURCE_MS: u64 = 30_000;

struct ConnectorQueue {
    name: String,
    receiver: core_source::SourceEventReceiver,
    pending: VecDeque<core_source::SourceEvent>,
    closed: bool,
}

struct BatchSelection {
    batch: Vec<core_source::SourceEvent>,
    per_connector_counts: HashMap<String, usize>,
}

impl ConnectorQueue {
    fn new(name: impl Into<String>, receiver: core_source::SourceEventReceiver) -> Self {
        Self {
            name: name.into(),
            receiver,
            pending: VecDeque::new(),
            closed: false,
        }
    }
}

use floe_node_core::executor::{
    StreamCompactionConfig, StreamGcConfig, available_sources_from_registry, build_dataflows,
};
use floe_node_core::source as core_source;
use floe_node_core::source::SourceRegistry;
use http_ingest::{HttpAdminConfig, HttpIngestConfig, HttpIngestHealth};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    metrics::init();
    let cli = cli::Cli::parse();
    let mut run_args = match cli.command {
        cli::Command::Run(args) => args,
        cli::Command::Tail(args) => {
            let config = args.to_config()?;
            tail_client::run(config)?;
            return Ok(());
        }
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
            },
            CompactionSchedulerConfig {
                failure_backoff_ticks: stream_compaction.scheduler_backoff_ticks,
                max_concurrent_jobs: stream_compaction.scheduler_max_concurrent_jobs,
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
    let admin_health = HttpIngestHealth {
        executor_running: Arc::clone(&executor_running),
        storage_reachable: Arc::clone(&storage_reachable),
        runtime_ready: Arc::clone(&runtime_ready),
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

    let mut connector_handles: Vec<JoinHandle<()>> = Vec::new();
    let mut connector_queues: Vec<ConnectorQueue> = Vec::new();
    let mut kafka_commit_senders: Vec<watch::Sender<KafkaOffsetCommit>> = Vec::new();
    let mut postgres_cdc_commit_senders: Vec<watch::Sender<PostgresCdcCommit>> = Vec::new();
    let definitions = source_registry.definitions().to_vec();

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
            .unwrap_or(DEFAULT_WATERMARK_IDLE_SOURCE_MS);
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
            record_mv_freshness_metrics(&mv_last_update_at_ms, now_ms);
        }
        'executor: loop {
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
            let mut decoded_rows = Vec::with_capacity(batch_len);
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
                let event_ts = event.event_time_ms().or(event_ts);
                if let Some(ts) = event_ts {
                    let ts_i64 = i64::try_from(ts).unwrap_or(i64::MAX);
                    let entry = tick_source_max_event_ts
                        .entry(source_name.clone())
                        .or_insert(i64::MIN);
                    *entry = (*entry).max(ts_i64);
                }
                *decoded_counts.entry(source_name.clone()).or_insert(0) += 1;
                decoded_rows.push((source_name, row));
            }
            let decode_latency_ms = decode_start.elapsed().as_millis() as u64;
            metrics::observe_decode_latency_ms(decode_latency_ms);
            metrics::observe_tick_phase_latency_ms("decode", decode_latency_ms);
            tracing::debug!(
                decoded_rows = decoded_rows.len(),
                latency_ms = decode_latency_ms,
                "decoded ingest batch"
            );

            if decoded_rows.is_empty() {
                continue;
            }

            let decoded_rows_len = decoded_rows.len();
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

            if !changed {
                continue;
            }

            epoch = pending_epoch;
            let now_instant = Instant::now();
            for (source, max_event_ts) in tick_source_max_event_ts {
                let watermark_entry = source_watermarks.entry(source.clone()).or_insert(i64::MIN);
                *watermark_entry = (*watermark_entry).max(max_event_ts);
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
                    mv_for_task.update_watermark_all(next as u64);
                    let now_ms = current_unix_time_ms();
                    let watermark_ms = u64::try_from(next).unwrap_or(u64::MAX);
                    metrics::record_watermark_lag_ms(now_ms.saturating_sub(watermark_ms));
                }
            }
            let tick_start = Instant::now();
            let tick_span = tracing::info_span!(
                "connector_tick",
                epoch,
                watermark = watermark_for_task.load(Ordering::Relaxed),
            );
            let _tick_guard = tick_span.enter();
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
                    decode_latency_ms,
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
        sink_cancel.clone(),
        runtime_cancel.clone(),
        Arc::clone(&runtime_failure),
    );

    let signal_cancel = runtime_cancel.clone();
    let signal_ingest_cancel = ingest_cancel.clone();
    let signal_shutdown = shutdown_signal.clone();
    let signal_handle = tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};

            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(err) => {
                    tracing::error!(error = %err, "failed to listen for SIGTERM");
                    signal_cancel.cancel();
                    return;
                }
            };

            tokio::select! {
                signal = tokio::signal::ctrl_c() => {
                    match signal {
                        Ok(()) => tracing::info!("shutdown signal received"),
                        Err(err) => tracing::error!(error = %err, "failed to listen for Ctrl-C"),
                    }
                    signal_ingest_cancel.cancel();
                    signal_shutdown.cancel();
                }
                _ = sigterm.recv() => {
                    tracing::info!("SIGTERM received");
                    signal_ingest_cancel.cancel();
                    signal_shutdown.cancel();
                }
                _ = signal_cancel.cancelled() => {}
            }
        }
        #[cfg(not(unix))]
        {
            tokio::select! {
                signal = tokio::signal::ctrl_c() => {
                    match signal {
                        Ok(()) => tracing::info!("shutdown signal received"),
                        Err(err) => tracing::error!(error = %err, "failed to listen for Ctrl-C"),
                    }
                    signal_ingest_cancel.cancel();
                    signal_shutdown.cancel();
                }
                _ = signal_cancel.cancelled() => {}
            }
        }
    });

    let query_for_server = query.clone();
    let mv_for_server = Arc::clone(&mv_registry);
    let server_cancel = service_cancel.clone();
    let runtime_cancel_for_server = runtime_cancel.clone();
    let failure_for_server = Arc::clone(&runtime_failure);
    let disable_pgwire = std::env::var("FLOE_DISABLE_PGWIRE")
        .ok()
        .map(|value| {
            value == "1" || value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("yes")
        })
        .unwrap_or(false);
    let server_handle: JoinHandle<anyhow::Result<()>> = if disable_pgwire {
        tracing::warn!("pgwire server disabled by FLOE_DISABLE_PGWIRE");
        tokio::spawn(async move {
            server_cancel.cancelled().await;
            Ok(())
        })
    } else {
        tokio::spawn(async move {
            let result =
                server::run_with_shutdown(query_for_server, mv_for_server, server_cancel.clone())
                    .await;
            if let Err(err) = &result {
                record_runtime_failure(&failure_for_server, format!("pgwire server failed: {err}"));
                runtime_cancel_for_server.cancel();
            }
            result
        })
    };

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

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn apply_runtime_config_defaults(args: &mut cli::RunArgs, config: &NodeConfig) {
    let runtime = &config.runtime;
    let storage = &config.storage;
    let maintenance = &config.maintenance;

    if args.events_per_second == DEFAULT_EVENTS_PER_SECOND {
        if let Some(events_per_second) = runtime.events_per_second {
            args.events_per_second = events_per_second;
        }
    }
    if args.max_events.is_none() {
        args.max_events = runtime.max_events;
    }
    if args.output_consolidation_mode == cli::OutputConsolidationMode::AllColumns {
        if let Some(mode) = runtime.output_consolidation_mode {
            args.output_consolidation_mode = match mode {
                OutputConsolidationModeConfig::AllColumns => {
                    cli::OutputConsolidationMode::AllColumns
                }
                OutputConsolidationModeConfig::Key => cli::OutputConsolidationMode::Key,
            };
        }
    }
    if args.ingest_queue_capacity == DEFAULT_INGEST_QUEUE_CAPACITY {
        if let Some(capacity) = runtime.ingest_queue_capacity {
            args.ingest_queue_capacity = capacity;
        }
    }
    if args.ingest_batch_size == DEFAULT_INGEST_BATCH_SIZE {
        if let Some(batch_size) = runtime.ingest_batch_size {
            args.ingest_batch_size = batch_size;
        }
    }
    if args.ingest_batch_per_source == DEFAULT_INGEST_BATCH_PER_SOURCE {
        if let Some(limit) = runtime.ingest_batch_per_source {
            args.ingest_batch_per_source = limit;
        }
    }
    if args.ingest_batch_per_connector == DEFAULT_INGEST_BATCH_PER_CONNECTOR {
        if let Some(limit) = runtime.ingest_batch_per_connector {
            args.ingest_batch_per_connector = limit;
        }
    }
    if args.mv_retain_last == DEFAULT_MV_RETAIN_LAST {
        if let Some(retain_last) = runtime.mv_retain_last {
            args.mv_retain_last = retain_last;
        }
    }
    if args.http_host == DEFAULT_HTTP_HOST {
        if let Some(host) = runtime.http_host.as_ref() {
            args.http_host = host.clone();
        }
    }
    if args.kafka_group_id == DEFAULT_KAFKA_GROUP_ID {
        if let Some(group_id) = runtime.kafka_group_id.as_ref() {
            args.kafka_group_id = group_id.clone();
        }
    }
    if args.kafka_poll_ms == DEFAULT_KAFKA_POLL_MS {
        if let Some(poll_ms) = runtime.kafka_poll_ms {
            args.kafka_poll_ms = poll_ms;
        }
    }
    if args.kafka_max_messages == DEFAULT_KAFKA_MAX_MESSAGES {
        if let Some(max_messages) = runtime.kafka_max_messages {
            args.kafka_max_messages = max_messages;
        }
    }

    if args.slatedb_await_durable.is_none() {
        args.slatedb_await_durable = storage.await_durable;
    }
    if args.slatedb_config.is_none() {
        args.slatedb_config = storage.slatedb_config.clone();
    }
    if args.slatedb_env_prefix.is_none() {
        args.slatedb_env_prefix = storage.slatedb_env_prefix.clone();
    }
    if args.zset_compaction_max_chain_len == DEFAULT_ZSET_COMPACTION_MAX_CHAIN_LEN {
        if let Some(max_chain_len) = storage.zset_compaction_max_chain_len {
            args.zset_compaction_max_chain_len = max_chain_len;
        }
    }
    if args.zset_compaction_max_segments == DEFAULT_ZSET_COMPACTION_MAX_SEGMENTS {
        if let Some(max_segments) = storage.zset_compaction_max_segments {
            args.zset_compaction_max_segments = max_segments;
        }
    }
    if args.zset_compaction_backoff_ticks == DEFAULT_ZSET_COMPACTION_BACKOFF_TICKS {
        if let Some(backoff_ticks) = storage.zset_compaction_backoff_ticks {
            args.zset_compaction_backoff_ticks = backoff_ticks;
        }
    }
    if args.zset_compaction_max_concurrent_jobs == DEFAULT_ZSET_COMPACTION_MAX_CONCURRENT_JOBS {
        if let Some(max_jobs) = storage.zset_compaction_max_concurrent_jobs {
            args.zset_compaction_max_concurrent_jobs = max_jobs;
        }
    }
    if args.zset_gc_grace_period_ms == DEFAULT_ZSET_GC_GRACE_PERIOD_MS {
        if let Some(grace_ms) = storage.zset_gc_grace_period_ms {
            args.zset_gc_grace_period_ms = grace_ms;
        }
    }

    if !args.maintenance_paused {
        if let Some(paused) = maintenance.paused {
            args.maintenance_paused = paused;
        }
    }
    if args.maintenance_inspect_namespace.is_empty() {
        args.maintenance_inspect_namespace = maintenance.inspect_namespace.clone();
    }
    if args.maintenance_compact_namespace.is_empty() {
        args.maintenance_compact_namespace = maintenance.compact_namespace.clone();
    }
    if args.maintenance_gc_namespace.is_empty() {
        args.maintenance_gc_namespace = maintenance.gc_namespace.clone();
    }
}

async fn upsert_materialized_view_definition(
    materialized_view_map: &mut HashMap<String, MaterializedViewDefinition>,
    definition: MaterializedViewDefinition,
    storage: Option<&Arc<floe_storage::SlateCatalog>>,
    source: &str,
) -> anyhow::Result<()> {
    let name = definition.name().to_string();
    if definition.if_not_exists() && materialized_view_map.contains_key(&name) {
        tracing::info!(
            view = %name,
            source = %source,
            "materialized view already exists; skipping due to IF NOT EXISTS"
        );
        return Ok(());
    }
    if let Some(storage) = storage {
        let metadata = MaterializedViewMetadata::new(
            definition.name(),
            definition.query(),
            definition.if_not_exists(),
        );
        storage
            .upsert_materialized_view(metadata)
            .await
            .with_context(|| {
                format!(
                    "persist materialized view definition for '{}' from {}",
                    definition.name(),
                    source
                )
            })?;
    }
    materialized_view_map.insert(name, definition);
    Ok(())
}

fn load_slatedb_settings(args: &cli::RunArgs) -> anyhow::Result<Option<Settings>> {
    let config_path = args
        .slatedb_config
        .clone()
        .or_else(|| std::env::var(SLATEDB_CONFIG_ENV).ok());

    let mut settings = if let Some(path) = config_path {
        Some(
            Settings::from_file(&path)
                .map_err(|err| anyhow!("failed to load SlateDB settings from {path}: {err}"))?,
        )
    } else {
        let prefix = args
            .slatedb_env_prefix
            .clone()
            .or_else(|| std::env::var(SLATEDB_ENV_PREFIX_ENV).ok())
            .unwrap_or_else(|| DEFAULT_SLATEDB_ENV_PREFIX.to_string());
        let prefix = prefix.trim();
        if !prefix.is_empty() && env_has_prefix(prefix) {
            Some(Settings::from_env(prefix).map_err(|err| {
                anyhow!("failed to load SlateDB settings from env prefix '{prefix}': {err}")
            })?)
        } else {
            None
        }
    };

    if settings.is_none() && slatedb_overrides_present(args) {
        settings = Some(Settings::default());
    }
    if let Some(settings) = settings.as_mut() {
        apply_slatedb_overrides(settings, args);
    }
    Ok(settings)
}

fn env_has_prefix(prefix: &str) -> bool {
    std::env::vars().any(|(key, _)| key.starts_with(prefix))
}

fn slatedb_overrides_present(args: &cli::RunArgs) -> bool {
    args.slatedb_flush_interval_ms.is_some()
        || args.slatedb_l0_sst_size_bytes.is_some()
        || args.slatedb_max_unflushed_bytes.is_some()
        || args.slatedb_compaction_max_sst_bytes.is_some()
        || args.slatedb_compaction_max_concurrent.is_some()
        || args.slatedb_cache_dir.is_some()
        || args.slatedb_cache_max_bytes.is_some()
        || args.slatedb_cache_part_bytes.is_some()
        || args.slatedb_cache_puts
}

fn apply_slatedb_overrides(settings: &mut Settings, args: &cli::RunArgs) {
    if let Some(interval_ms) = args.slatedb_flush_interval_ms {
        settings.flush_interval = if interval_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(interval_ms))
        };
    }
    if let Some(bytes) = args.slatedb_l0_sst_size_bytes {
        settings.l0_sst_size_bytes = bytes;
    }
    if let Some(bytes) = args.slatedb_max_unflushed_bytes {
        settings.max_unflushed_bytes = bytes;
    }
    if let Some(max_sst_size) = args.slatedb_compaction_max_sst_bytes {
        let compactor = settings
            .compactor_options
            .get_or_insert_with(CompactorOptions::default);
        compactor.max_sst_size = max_sst_size;
    }
    if let Some(max_concurrent) = args.slatedb_compaction_max_concurrent {
        let compactor = settings
            .compactor_options
            .get_or_insert_with(CompactorOptions::default);
        compactor.max_concurrent_compactions = max_concurrent;
    }
    if let Some(dir) = args.slatedb_cache_dir.as_ref() {
        settings.object_store_cache_options.root_folder = Some(PathBuf::from(dir));
    }
    if let Some(max_bytes) = args.slatedb_cache_max_bytes {
        settings.object_store_cache_options.max_cache_size_bytes = Some(max_bytes);
    }
    if let Some(part_bytes) = args.slatedb_cache_part_bytes {
        settings.object_store_cache_options.part_size_bytes = part_bytes;
    }
    if args.slatedb_cache_puts {
        settings.object_store_cache_options.cache_puts = true;
    }
}

fn connectors_from_cli(args: &cli::RunArgs) -> Vec<ConnectorConfig> {
    let mut connectors = Vec::new();
    if let Some(port) = args.http_port {
        connectors.push(ConnectorConfig::Http {
            name: None,
            host: Some(args.http_host.clone()),
            port,
            default_source: args.http_source.clone(),
        });
    }
    if let Some(brokers) = args.kafka_brokers.clone() {
        connectors.push(ConnectorConfig::Kafka {
            name: None,
            brokers,
            topics: args.kafka_topics.clone(),
            group_id: Some(args.kafka_group_id.clone()),
            default_source: args.kafka_default_source.clone(),
            poll_ms: Some(args.kafka_poll_ms),
            max_messages_per_tick: Some(args.kafka_max_messages),
        });
    }
    if let Some(path) = args.input_file.clone() {
        connectors.push(ConnectorConfig::File {
            name: None,
            path,
            default_source: args.input_source.clone(),
        });
    }
    connectors.push(ConnectorConfig::Generator {
        name: None,
        events_per_second: Some(args.events_per_second),
        max_events: args.max_events,
    });
    connectors
}

fn cli_connector_creation_flags(args: &cli::RunArgs) -> Vec<&'static str> {
    let mut flags = Vec::new();
    if args.http_port.is_some() {
        flags.push("--http-port");
    }
    if args.kafka_brokers.is_some() {
        flags.push("--kafka-brokers");
    }
    if !args.kafka_topics.is_empty() {
        flags.push("--kafka-topics");
    }
    if args.input_file.is_some() {
        flags.push("--input-file");
    }
    flags
}

fn log_startup_banner(args: &cli::RunArgs, connectors: &[config::ConnectorSpec]) {
    let pgwire_addr =
        std::env::var("FLOE_PG_ADDR").unwrap_or_else(|_| "127.0.0.1:6432".to_string());
    let storage_mode = std::env::var("FLOE_DATA_DIR")
        .map(|dir| format!("filesystem({dir})"))
        .unwrap_or_else(|_| "in-memory".to_string());
    let connector_names: Vec<&str> = connectors
        .iter()
        .map(|connector| connector.name.as_str())
        .collect();
    let http_addrs: Vec<String> = connectors
        .iter()
        .filter_map(|connector| match &connector.config {
            ConnectorConfig::Http { host, port, .. } => Some(format!(
                "{}:{}",
                host.as_deref().unwrap_or(args.http_host.as_str()),
                port
            )),
            _ => None,
        })
        .collect();

    tracing::info!(
        storage_mode = %storage_mode,
        pgwire_addr = %pgwire_addr,
        http_addrs = ?http_addrs,
        connectors = ?connector_names,
        mv_retain_last = args.mv_retain_last,
        zset_compaction_max_chain_len = args.zset_compaction_max_chain_len,
        zset_compaction_max_segments = args.zset_compaction_max_segments,
        zset_compaction_backoff_ticks = args.zset_compaction_backoff_ticks,
        zset_compaction_max_concurrent_jobs = args.zset_compaction_max_concurrent_jobs,
        zset_gc_grace_period_ms = args.zset_gc_grace_period_ms,
        "startup banner"
    );
}

fn sink_mv_name(config: &SinkConfig) -> &str {
    match config {
        SinkConfig::Kafka { mv, .. }
        | SinkConfig::File { mv, .. }
        | SinkConfig::Http { mv, .. } => mv,
    }
}

fn merge_sql_sinks(
    sink_specs: &mut Vec<SinkSpec>,
    sql_sink_specs: Vec<SinkSpec>,
    materialized_view_map: &HashMap<String, MaterializedViewDefinition>,
) -> anyhow::Result<()> {
    let mut sink_names: BTreeSet<String> =
        sink_specs.iter().map(|spec| spec.name.clone()).collect();
    for sink in sql_sink_specs {
        let mv_name = sink_mv_name(&sink.config);
        if !materialized_view_map.contains_key(mv_name) {
            return Err(anyhow!(
                "sink '{}' references unknown materialized view '{}'",
                sink.name,
                mv_name
            ));
        }
        if !sink_names.insert(sink.name.clone()) {
            return Err(anyhow!("duplicate sink name '{}'", sink.name));
        }
        sink_specs.push(sink);
    }
    Ok(())
}

fn table_definition_from_sql(
    definition: &CreateTableDefinition,
) -> anyhow::Result<TableDefinition> {
    let columns = definition
        .columns()
        .iter()
        .map(|column| {
            let data_type = match column.data_type() {
                SqlColumnType::Int64 => ColumnType::Int64,
                SqlColumnType::Bool => ColumnType::Bool,
                SqlColumnType::Utf8 => ColumnType::Utf8,
                SqlColumnType::TimestampMillis => ColumnType::TimestampMillis,
            };
            ColumnDefinition::new_typed_nullable(
                column.name(),
                data_type,
                column.nullable(),
                column.primary_key(),
            )
        })
        .collect();
    TableDefinition::new(definition.name(), columns)
}

fn source_definition_from_table(table: &TableDefinition) -> anyhow::Result<SourceDefinition> {
    let columns = table
        .columns()
        .iter()
        .map(|column| {
            let data_type = match column.data_type() {
                ColumnType::Int64 => SourceDataType::Int64,
                ColumnType::Bool => SourceDataType::Bool,
                ColumnType::Utf8 => SourceDataType::Utf8,
                ColumnType::TimestampMillis => SourceDataType::TimestampMillis,
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

fn source_definition_has_primary_key(definition: &SourceDefinition) -> bool {
    definition.property(SOURCE_PRIMARY_KEY_PROPERTY).is_some()
}

fn lookup_decoder_for_source<'a>(
    decoders: &'a HashMap<String, SourceRowDecoder>,
    source_name: &str,
) -> anyhow::Result<&'a SourceRowDecoder> {
    decoders
        .get(source_name)
        .ok_or_else(|| anyhow!("received event for unknown source '{source_name}'"))
}

fn resolve_output_consolidation_mode(
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

fn should_sample(counter: &AtomicU64, every: u64) -> bool {
    if every == 0 {
        return true;
    }
    counter
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(every)
}

fn record_runtime_failure(state: &Arc<StdMutex<Option<String>>>, message: String) {
    metrics::inc_runtime_error("runtime");
    let mut guard = state.lock().expect("runtime failure lock poisoned");
    if guard.is_none() {
        *guard = Some(message);
    }
}

fn collect_mv_versions_for_commit(
    registry: &Arc<MaterializedViewRegistry>,
    last_versions: &mut HashMap<String, u64>,
) -> Vec<MaterializedViewTickVersion> {
    let mut committed = Vec::new();
    for handle in registry.handles() {
        let Some(frontier) = handle.latest_version() else {
            continue;
        };
        if frontier < 0 {
            continue;
        }
        let Some(zset_handle) = handle.handle_for_version(frontier) else {
            continue;
        };
        let view = handle.name().to_string();
        let version = zset_handle.version;
        let entry = last_versions.entry(view.clone()).or_insert(0);
        if version > *entry {
            committed.push(MaterializedViewTickVersion { view, version });
            *entry = version;
        }
    }
    committed.sort_by(|left, right| left.view.cmp(&right.view));
    committed
}

fn compute_global_watermark(
    source_watermarks: &HashMap<String, i64>,
    source_last_seen_at: &HashMap<String, Instant>,
    now: Instant,
    idle_timeout: Duration,
) -> Option<i64> {
    let mut global: Option<i64> = None;
    for (source, watermark) in source_watermarks {
        let Some(last_seen) = source_last_seen_at.get(source) else {
            continue;
        };
        if now.duration_since(*last_seen) > idle_timeout {
            continue;
        }
        global = Some(global.map_or(*watermark, |current| current.min(*watermark)));
    }
    global
}

fn advance_global_watermark(previous: i64, candidate: Option<i64>) -> i64 {
    candidate.map_or(previous, |value| previous.max(value))
}

fn record_mv_freshness_metrics(last_update_at_ms: &HashMap<String, u64>, now_ms: u64) {
    for (view, last_update_ms) in last_update_at_ms {
        let age_seconds = now_ms.saturating_sub(*last_update_ms) / 1_000;
        metrics::record_mv_freshness_seconds(view, age_seconds);
    }
}

fn event_resume_offset(token: Option<&core_source::SourceResumeToken>) -> Option<(u32, u64)> {
    match token? {
        core_source::SourceResumeToken::Kafka {
            partition, offset, ..
        } => {
            let partition = u32::try_from(*partition).ok()?;
            let offset = u64::try_from(*offset).ok()?;
            Some((partition, offset))
        }
        core_source::SourceResumeToken::PostgresCdc { lsn, .. } => {
            parse_postgres_lsn(lsn).map(|offset| (0, offset))
        }
        core_source::SourceResumeToken::File { cursor }
        | core_source::SourceResumeToken::Generator { position: cursor }
        | core_source::SourceResumeToken::ObjectStore { cursor } => Some((0, *cursor)),
    }
}

fn event_kafka_offset(
    token: Option<&core_source::SourceResumeToken>,
) -> Option<(String, i32, i64)> {
    match token? {
        core_source::SourceResumeToken::Kafka {
            topic,
            partition,
            offset,
        } => Some((topic.clone(), *partition, *offset)),
        _ => None,
    }
}

fn event_postgres_lsn(
    token: Option<&core_source::SourceResumeToken>,
) -> Option<(String, u64, String)> {
    match token? {
        core_source::SourceResumeToken::PostgresCdc { slot, lsn, .. } => {
            let slot = slot.clone().unwrap_or_else(|| "default".to_string());
            let value = parse_postgres_lsn(lsn)?;
            Some((slot, value, lsn.clone()))
        }
        _ => None,
    }
}

fn build_kafka_offset_commit(
    tick_id: u64,
    offsets: &HashMap<(String, i32), i64>,
) -> KafkaOffsetCommit {
    let mut entries: Vec<KafkaTopicPartitionOffset> = offsets
        .iter()
        .map(|((topic, partition), offset)| KafkaTopicPartitionOffset {
            topic: topic.clone(),
            partition: *partition,
            offset: *offset,
        })
        .collect();
    entries.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then(left.partition.cmp(&right.partition))
    });
    KafkaOffsetCommit {
        tick_id,
        offsets: entries,
    }
}

fn build_postgres_cdc_commit(
    tick_id: u64,
    slots: &HashMap<String, (u64, String)>,
) -> PostgresCdcCommit {
    let mut entries: Vec<PostgresSlotCommit> = slots
        .iter()
        .map(|(slot, (_, lsn))| PostgresSlotCommit {
            slot: slot.clone(),
            lsn: lsn.clone(),
        })
        .collect();
    entries.sort_by(|left, right| left.slot.cmp(&right.slot));
    PostgresCdcCommit {
        tick_id,
        slots: entries,
    }
}

fn parse_postgres_lsn(lsn: &str) -> Option<u64> {
    let (left, right) = lsn.trim().split_once('/')?;
    let high = u64::from_str_radix(left, 16).ok()?;
    let low = u64::from_str_radix(right, 16).ok()?;
    Some((high << 32) | low)
}

fn current_unix_time_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().try_into().unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}

fn log_operator_hints(
    connectors: &[config::ConnectorSpec],
    available_sources: &BTreeSet<String>,
    materialized_views: &[MaterializedViewDefinition],
    sinks: &[SinkSpec],
) {
    let connector_names: Vec<&str> = connectors
        .iter()
        .map(|connector| connector.name.as_str())
        .collect();
    let sink_names: Vec<&str> = sinks.iter().map(|sink| sink.name.as_str()).collect();
    let mv_names: Vec<&str> = materialized_views.iter().map(|mv| mv.name()).collect();
    let pgwire_addr =
        std::env::var("FLOE_PG_ADDR").unwrap_or_else(|_| "127.0.0.1:6432".to_string());

    tracing::info!(
        pgwire_addr = %pgwire_addr,
        connectors = ?connector_names,
        sources = ?available_sources,
        materialized_views = ?mv_names,
        sinks = ?sink_names,
        "runtime topology"
    );

    for mv_name in mv_names {
        tracing::info!(
            mv = %mv_name,
            tail_mv = %format!("cargo run -p floe-node -- tail --mv {mv_name}"),
            tail_sql = %format!("cargo run -p floe-node -- tail --sql \"TAIL {mv_name} WITH (SNAPSHOT)\""),
            pgwire_addr = %pgwire_addr,
            "tail hint"
        );
    }
}

async fn recv_from_any(queues: &mut Vec<ConnectorQueue>) -> bool {
    if queues.is_empty() {
        return false;
    }
    let (event, index) = {
        let futures: Vec<_> = queues
            .iter_mut()
            .map(|queue| Box::pin(queue.receiver.recv()))
            .collect();
        let (event, index, _remaining) = select_all(futures).await;
        (event, index)
    };
    match event {
        Some(event) => {
            queues[index].pending.push_back(event);
        }
        None => {
            queues[index].closed = true;
        }
    }
    queues.retain(|queue| !(queue.closed && queue.pending.is_empty()));
    !queues.is_empty()
}

fn drain_connectors(queues: &mut [ConnectorQueue], capacity: usize) {
    for queue in queues.iter_mut() {
        while queue.pending.len() < capacity {
            match queue.receiver.try_recv() {
                Ok(event) => queue.pending.push_back(event),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    queue.closed = true;
                    break;
                }
            }
        }
    }
}

fn build_batch(
    queues: &mut [ConnectorQueue],
    start_index: usize,
    max_batch: usize,
    max_per_source: usize,
    max_per_connector: usize,
) -> BatchSelection {
    let mut batch = Vec::with_capacity(max_batch);
    let mut per_source_counts: HashMap<String, usize> = HashMap::new();
    let mut per_connector_counts: HashMap<String, usize> = HashMap::new();
    let mut deferred: Vec<VecDeque<core_source::SourceEvent>> = vec![VecDeque::new(); queues.len()];
    let connector_count = queues.len();
    for step in 0..connector_count {
        let idx = (start_index + step) % connector_count;
        let queue = &mut queues[idx];
        let deferred_queue = &mut deferred[idx];
        let per_connector = per_connector_counts.entry(queue.name.clone()).or_insert(0);
        while *per_connector < max_per_connector && batch.len() < max_batch {
            let Some(event) = queue.pending.pop_front() else {
                break;
            };
            let source = event.source();
            let count = per_source_counts.entry(source.to_string()).or_insert(0);
            if *count >= max_per_source {
                deferred_queue.push_back(event);
                continue;
            }
            *count += 1;
            *per_connector += 1;
            batch.push(event);
        }
    }
    for (queue, mut deferred_queue) in queues.iter_mut().zip(deferred) {
        if !deferred_queue.is_empty() {
            deferred_queue.append(&mut queue.pending);
            queue.pending = deferred_queue;
        }
    }

    BatchSelection {
        batch,
        per_connector_counts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use floe_sql_parser::parse_floe_statement;
    use serde_json::json;

    fn default_run_args() -> cli::RunArgs {
        cli::RunArgs {
            events_per_second: DEFAULT_EVENTS_PER_SECOND,
            max_events: None,
            mv_query: None,
            config: None,
            dry_run: false,
            slatedb_config: None,
            slatedb_env_prefix: None,
            slatedb_flush_interval_ms: None,
            slatedb_l0_sst_size_bytes: None,
            slatedb_max_unflushed_bytes: None,
            slatedb_compaction_max_sst_bytes: None,
            slatedb_compaction_max_concurrent: None,
            slatedb_await_durable: None,
            slatedb_cache_dir: None,
            slatedb_cache_max_bytes: None,
            slatedb_cache_part_bytes: None,
            slatedb_cache_puts: false,
            mv_retain_last: DEFAULT_MV_RETAIN_LAST,
            zset_compaction_max_chain_len: DEFAULT_ZSET_COMPACTION_MAX_CHAIN_LEN,
            zset_compaction_max_segments: DEFAULT_ZSET_COMPACTION_MAX_SEGMENTS,
            zset_compaction_backoff_ticks: DEFAULT_ZSET_COMPACTION_BACKOFF_TICKS,
            zset_compaction_max_concurrent_jobs: DEFAULT_ZSET_COMPACTION_MAX_CONCURRENT_JOBS,
            zset_gc_grace_period_ms: DEFAULT_ZSET_GC_GRACE_PERIOD_MS,
            maintenance_paused: false,
            maintenance_inspect_namespace: Vec::new(),
            maintenance_compact_namespace: Vec::new(),
            maintenance_gc_namespace: Vec::new(),
            output_consolidation_mode: cli::OutputConsolidationMode::AllColumns,
            input_file: None,
            input_source: None,
            kafka_brokers: None,
            kafka_topics: Vec::new(),
            kafka_group_id: DEFAULT_KAFKA_GROUP_ID.to_string(),
            kafka_default_source: None,
            kafka_poll_ms: DEFAULT_KAFKA_POLL_MS,
            kafka_max_messages: DEFAULT_KAFKA_MAX_MESSAGES,
            ingest_queue_capacity: DEFAULT_INGEST_QUEUE_CAPACITY,
            ingest_batch_size: DEFAULT_INGEST_BATCH_SIZE,
            ingest_batch_per_source: DEFAULT_INGEST_BATCH_PER_SOURCE,
            ingest_batch_per_connector: DEFAULT_INGEST_BATCH_PER_CONNECTOR,
            http_host: DEFAULT_HTTP_HOST.to_string(),
            http_port: None,
            http_source: None,
        }
    }

    fn event(source: &str, id: i64) -> core_source::SourceEvent {
        core_source::SourceEvent::new(source, json!({ "id": id }))
    }

    #[test]
    fn build_batch_limits_per_connector() {
        let (_tx_a, rx_a) = core_source::channel(8);
        let (_tx_b, rx_b) = core_source::channel(8);
        let mut queues = vec![
            ConnectorQueue {
                name: "a".to_string(),
                receiver: rx_a,
                pending: VecDeque::from([event("s1", 1), event("s1", 2)]),
                closed: false,
            },
            ConnectorQueue {
                name: "b".to_string(),
                receiver: rx_b,
                pending: VecDeque::from([event("s2", 3), event("s2", 4)]),
                closed: false,
            },
        ];

        let selection = build_batch(&mut queues, 0, 10, 10, 1);
        assert_eq!(selection.batch.len(), 2);
        assert_eq!(selection.per_connector_counts.get("a"), Some(&1));
        assert_eq!(selection.per_connector_counts.get("b"), Some(&1));
        assert_eq!(queues[0].pending.len(), 1);
        assert_eq!(queues[1].pending.len(), 1);
    }

    #[test]
    fn build_batch_limits_per_source() {
        let (_tx, rx) = core_source::channel(8);
        let mut queues = vec![ConnectorQueue {
            name: "a".to_string(),
            receiver: rx,
            pending: VecDeque::from([event("s1", 1), event("s1", 2), event("s1", 3)]),
            closed: false,
        }];

        let selection = build_batch(&mut queues, 0, 10, 1, 10);
        assert_eq!(selection.batch.len(), 1);
        assert_eq!(queues[0].pending.len(), 2);
    }

    #[test]
    fn merge_sql_sinks_validates_mv_reference() {
        let mut sink_specs = Vec::new();
        let sql_sink_specs = vec![SinkSpec {
            name: "sink_missing".to_string(),
            config: SinkConfig::File {
                name: Some("sink_missing".to_string()),
                path: "/tmp/out.jsonl".to_string(),
                mv: "missing_mv".to_string(),
                with_snapshot: Some(false),
                as_of: None,
                append: Some(true),
                batch_rows: None,
                batch_bytes: None,
                queue_capacity: None,
            },
        }];
        let materialized_view_map = HashMap::new();

        let err = merge_sql_sinks(&mut sink_specs, sql_sink_specs, &materialized_view_map)
            .expect_err("expected unknown mv validation error");
        assert!(
            err.to_string()
                .contains("references unknown materialized view 'missing_mv'")
        );
    }

    #[test]
    fn merge_sql_sinks_rejects_duplicate_names() {
        let mut sink_specs = vec![SinkSpec {
            name: "sink_dup".to_string(),
            config: SinkConfig::File {
                name: Some("sink_dup".to_string()),
                path: "/tmp/first.jsonl".to_string(),
                mv: "mv_a".to_string(),
                with_snapshot: Some(false),
                as_of: None,
                append: Some(true),
                batch_rows: None,
                batch_bytes: None,
                queue_capacity: None,
            },
        }];
        let sql_sink_specs = vec![SinkSpec {
            name: "sink_dup".to_string(),
            config: SinkConfig::Http {
                name: Some("sink_dup".to_string()),
                url: "http://localhost:8080".to_string(),
                mv: "mv_a".to_string(),
                with_snapshot: Some(true),
                as_of: None,
                batch_size: Some(1),
                batch_rows: None,
                batch_bytes: None,
                queue_capacity: None,
                retry_max_attempts: None,
                retry_base_ms: None,
                retry_max_backoff_ms: None,
            },
        }];
        let mut materialized_view_map = HashMap::new();
        materialized_view_map.insert(
            "mv_a".to_string(),
            MaterializedViewDefinition::new("mv_a", "SELECT 1", false),
        );

        let err = merge_sql_sinks(&mut sink_specs, sql_sink_specs, &materialized_view_map)
            .expect_err("expected duplicate sink name error");
        assert!(err.to_string().contains("duplicate sink name 'sink_dup'"));
    }

    #[test]
    fn runtime_failure_records_first_error_only() {
        let state = Arc::new(StdMutex::new(None::<String>));
        record_runtime_failure(&state, "first".to_string());
        record_runtime_failure(&state, "second".to_string());
        assert_eq!(
            state.lock().expect("runtime failure lock").as_deref(),
            Some("first")
        );
    }

    #[test]
    fn cli_connector_creation_flags_collects_explicit_connector_inputs() {
        let mut args = default_run_args();
        args.config = Some("node.toml".to_string());
        args.input_file = Some("/tmp/events.jsonl".to_string());
        args.kafka_brokers = Some("localhost:9092".to_string());
        args.kafka_topics = vec!["nexmark_bid".to_string()];
        args.http_port = Some(8080);
        let flags = cli_connector_creation_flags(&args);
        assert_eq!(
            flags,
            vec![
                "--http-port",
                "--kafka-brokers",
                "--kafka-topics",
                "--input-file"
            ]
        );
    }

    #[test]
    fn log_operator_hints_handles_empty_materialized_views() {
        let connectors = vec![config::ConnectorSpec {
            name: "generator".to_string(),
            config: ConnectorConfig::Generator {
                name: None,
                events_per_second: Some(10.0),
                max_events: None,
            },
        }];
        let available_sources = BTreeSet::from(["nexmark_bid".to_string()]);
        log_operator_hints(&connectors, &available_sources, &[], &[]);
    }

    #[test]
    fn log_startup_banner_handles_mixed_connectors() {
        let args = default_run_args();
        let connectors = vec![
            config::ConnectorSpec {
                name: "generator".to_string(),
                config: ConnectorConfig::Generator {
                    name: None,
                    events_per_second: Some(10.0),
                    max_events: None,
                },
            },
            config::ConnectorSpec {
                name: "http".to_string(),
                config: ConnectorConfig::Http {
                    name: None,
                    host: Some("127.0.0.1".to_string()),
                    port: 8080,
                    default_source: Some("nexmark_bid".to_string()),
                },
            },
        ];
        log_startup_banner(&args, &connectors);
    }

    #[test]
    fn apply_runtime_config_defaults_uses_config_when_cli_values_are_defaults() {
        let mut args = default_run_args();
        let config = NodeConfig {
            runtime: config::RuntimeConfig {
                events_per_second: Some(25.0),
                max_events: Some(123),
                output_consolidation_mode: Some(OutputConsolidationModeConfig::Key),
                ingest_queue_capacity: Some(2048),
                ingest_batch_size: Some(512),
                ingest_batch_per_source: Some(128),
                ingest_batch_per_connector: Some(96),
                mv_retain_last: Some(7),
                http_host: Some("0.0.0.0".to_string()),
                kafka_group_id: Some("cfg-group".to_string()),
                kafka_poll_ms: Some(250),
                kafka_max_messages: Some(1024),
            },
            storage: config::StorageConfig {
                await_durable: Some(true),
                slatedb_config: Some("/tmp/slatedb.toml".to_string()),
                slatedb_env_prefix: Some("CFG_".to_string()),
                zset_compaction_max_chain_len: Some(99),
                zset_compaction_max_segments: Some(500),
                zset_compaction_backoff_ticks: Some(8),
                zset_compaction_max_concurrent_jobs: Some(4),
                zset_gc_grace_period_ms: Some(1_000),
            },
            maintenance: config::MaintenanceConfig {
                paused: Some(true),
                inspect_namespace: vec!["ns.inspect".to_string()],
                compact_namespace: vec!["ns.compact".to_string()],
                gc_namespace: vec!["ns.gc".to_string()],
            },
            ..NodeConfig::default()
        };

        apply_runtime_config_defaults(&mut args, &config);

        assert_eq!(args.events_per_second, 25.0);
        assert_eq!(args.max_events, Some(123));
        assert_eq!(
            args.output_consolidation_mode,
            cli::OutputConsolidationMode::Key
        );
        assert_eq!(args.ingest_queue_capacity, 2048);
        assert_eq!(args.ingest_batch_size, 512);
        assert_eq!(args.ingest_batch_per_source, 128);
        assert_eq!(args.ingest_batch_per_connector, 96);
        assert_eq!(args.mv_retain_last, 7);
        assert_eq!(args.http_host, "0.0.0.0");
        assert_eq!(args.kafka_group_id, "cfg-group");
        assert_eq!(args.kafka_poll_ms, 250);
        assert_eq!(args.kafka_max_messages, 1024);
        assert_eq!(args.slatedb_await_durable, Some(true));
        assert_eq!(args.slatedb_config.as_deref(), Some("/tmp/slatedb.toml"));
        assert_eq!(args.slatedb_env_prefix.as_deref(), Some("CFG_"));
        assert_eq!(args.zset_compaction_max_chain_len, 99);
        assert_eq!(args.zset_compaction_max_segments, 500);
        assert_eq!(args.zset_compaction_backoff_ticks, 8);
        assert_eq!(args.zset_compaction_max_concurrent_jobs, 4);
        assert_eq!(args.zset_gc_grace_period_ms, 1_000);
        assert!(args.maintenance_paused);
        assert_eq!(
            args.maintenance_inspect_namespace,
            vec!["ns.inspect".to_string()]
        );
        assert_eq!(
            args.maintenance_compact_namespace,
            vec!["ns.compact".to_string()]
        );
        assert_eq!(args.maintenance_gc_namespace, vec!["ns.gc".to_string()]);
    }

    #[test]
    fn apply_runtime_config_defaults_preserves_explicit_cli_values() {
        let mut args = default_run_args();
        args.events_per_second = 77.0;
        args.output_consolidation_mode = cli::OutputConsolidationMode::Key;
        args.ingest_batch_size = 999;
        args.maintenance_paused = true;
        args.slatedb_await_durable = Some(true);

        let config = NodeConfig {
            runtime: config::RuntimeConfig {
                events_per_second: Some(25.0),
                output_consolidation_mode: Some(OutputConsolidationModeConfig::AllColumns),
                ingest_batch_size: Some(128),
                ..config::RuntimeConfig::default()
            },
            storage: config::StorageConfig {
                await_durable: Some(false),
                ..config::StorageConfig::default()
            },
            maintenance: config::MaintenanceConfig {
                paused: Some(false),
                ..config::MaintenanceConfig::default()
            },
            ..NodeConfig::default()
        };

        apply_runtime_config_defaults(&mut args, &config);

        assert_eq!(args.events_per_second, 77.0);
        assert_eq!(
            args.output_consolidation_mode,
            cli::OutputConsolidationMode::Key
        );
        assert_eq!(args.ingest_batch_size, 999);
        assert!(args.maintenance_paused);
        assert_eq!(args.slatedb_await_durable, Some(true));
    }

    #[test]
    fn table_definition_from_sql_preserves_primary_key_and_nullability() {
        let statement = parse_floe_statement(
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, note TEXT, enabled BOOL NOT NULL, created_at TIMESTAMP)",
        )
        .expect("parse create table");
        let FloeStatement::CreateTable(definition) = statement else {
            panic!("expected create table statement");
        };
        let table = table_definition_from_sql(&definition).expect("table definition");
        assert_eq!(table.name(), "orders");
        assert_eq!(table.columns().len(), 4);
        assert_eq!(table.primary_key_index(), 0);
        assert!(!table.columns()[0].nullable());
        assert!(table.columns()[1].nullable());
        assert!(!table.columns()[2].nullable());
        assert!(table.columns()[3].nullable());
    }

    #[test]
    fn source_definition_from_table_sets_pk_property() {
        let statement = parse_floe_statement(
            "CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT, active BOOL)",
        )
        .expect("parse create table");
        let FloeStatement::CreateTable(definition) = statement else {
            panic!("expected create table statement");
        };
        let table = table_definition_from_sql(&definition).expect("table definition");
        let source = source_definition_from_table(&table).expect("source definition");
        assert_eq!(source.name(), "users");
        assert_eq!(source.property(SOURCE_PRIMARY_KEY_PROPERTY), Some("id"));
        assert!(source_definition_has_primary_key(&source));
        assert!(!source.columns()[0].nullable());
        assert!(source.columns()[1].nullable());
    }

    #[test]
    fn resolve_output_consolidation_mode_defaults_to_key_when_pk_present() {
        let statement = parse_floe_statement("CREATE TABLE users (id BIGINT PRIMARY KEY)")
            .expect("parse create table");
        let FloeStatement::CreateTable(definition) = statement else {
            panic!("expected create table statement");
        };
        let table = table_definition_from_sql(&definition).expect("table definition");
        let source = source_definition_from_table(&table).expect("source definition");
        let mut registry = SourceRegistry::new();
        registry.register(source);

        assert_eq!(
            resolve_output_consolidation_mode(cli::OutputConsolidationMode::AllColumns, &registry),
            cli::OutputConsolidationMode::Key
        );
    }

    #[test]
    fn resolve_output_consolidation_mode_keeps_all_columns_without_pk() {
        let mut registry = SourceRegistry::new();
        registry.extend(generator::definitions().expect("generator definitions"));

        assert_eq!(
            resolve_output_consolidation_mode(cli::OutputConsolidationMode::AllColumns, &registry),
            cli::OutputConsolidationMode::AllColumns
        );
    }

    #[test]
    fn lookup_decoder_for_source_rejects_unknown_source() {
        let decoders: HashMap<String, SourceRowDecoder> = HashMap::new();
        let err = lookup_decoder_for_source(&decoders, "missing_source")
            .expect_err("unknown source should fail");
        assert!(
            err.to_string()
                .contains("received event for unknown source 'missing_source'")
        );
    }

    #[test]
    fn build_postgres_cdc_commit_orders_slots() {
        let mut slots = HashMap::new();
        slots.insert("z_slot".to_string(), (10_u64, "0/0000000A".to_string()));
        slots.insert("a_slot".to_string(), (3_u64, "0/00000003".to_string()));
        let commit = build_postgres_cdc_commit(7, &slots);
        assert_eq!(commit.tick_id, 7);
        assert_eq!(commit.slots.len(), 2);
        assert_eq!(commit.slots[0].slot, "a_slot");
        assert_eq!(commit.slots[1].slot, "z_slot");
    }

    #[test]
    fn event_postgres_lsn_extracts_slot_and_value() {
        let token = core_source::SourceResumeToken::PostgresCdc {
            slot: Some("cdc_slot".to_string()),
            lsn: "16/B3738".to_string(),
            txid: None,
        };
        let (slot, value, lsn) =
            event_postgres_lsn(Some(&token)).expect("postgres resume token should parse");
        assert_eq!(slot, "cdc_slot");
        assert_eq!(lsn, "16/B3738");
        assert_eq!(value, parse_postgres_lsn("16/B3738").expect("parse lsn"));
    }

    #[test]
    fn event_resume_offset_extracts_postgres_lsn() {
        let token = core_source::SourceResumeToken::PostgresCdc {
            slot: Some("slot_a".to_string()),
            lsn: "0/0000002A".to_string(),
            txid: Some(5),
        };
        assert_eq!(
            event_resume_offset(Some(&token)),
            Some((0, parse_postgres_lsn("0/0000002A").expect("parse lsn")))
        );
    }

    #[test]
    fn compute_global_watermark_uses_min_of_active_sources() {
        let now = Instant::now();
        let mut source_watermarks = HashMap::new();
        source_watermarks.insert("s1".to_string(), 5_000);
        source_watermarks.insert("s2".to_string(), 3_000);

        let mut source_last_seen = HashMap::new();
        source_last_seen.insert("s1".to_string(), now);
        source_last_seen.insert("s2".to_string(), now);

        assert_eq!(
            compute_global_watermark(
                &source_watermarks,
                &source_last_seen,
                now,
                Duration::from_secs(30),
            ),
            Some(3_000)
        );
    }

    #[test]
    fn compute_global_watermark_skips_idle_sources() {
        let now = Instant::now();
        let mut source_watermarks = HashMap::new();
        source_watermarks.insert("active".to_string(), 9_000);
        source_watermarks.insert("idle".to_string(), 1_000);

        let mut source_last_seen = HashMap::new();
        source_last_seen.insert("active".to_string(), now);
        source_last_seen.insert("idle".to_string(), now - Duration::from_secs(60));

        assert_eq!(
            compute_global_watermark(
                &source_watermarks,
                &source_last_seen,
                now,
                Duration::from_secs(30),
            ),
            Some(9_000)
        );
    }

    #[test]
    fn advance_global_watermark_is_monotonic() {
        assert_eq!(advance_global_watermark(5_000, Some(4_000)), 5_000);
        assert_eq!(advance_global_watermark(5_000, Some(7_000)), 7_000);
        assert_eq!(advance_global_watermark(5_000, None), 5_000);
    }
}

async fn register_materialized_view_tables(
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

async fn register_source_tables(
    context: &FloeQueryContext,
    sources: &SourceRegistry,
    bridge: Arc<Mutex<DbspBridge>>,
) -> anyhow::Result<()> {
    let session = context.session();
    for definition in sources.definitions() {
        let schema = definition.to_arrow_schema();
        let provider = SourceTableProvider::new(
            Arc::clone(&bridge),
            definition.name(),
            definition.name(),
            schema,
            definition.property(SOURCE_PRIMARY_KEY_PROPERTY),
        )?;
        session
            .register_table(definition.name(), Arc::new(provider))
            .with_context(|| format!("register source table {}", definition.name()))?;

        if let Some(short_name) = definition.name().strip_prefix("nexmark_") {
            let alias_schema = camel_case_schema(definition);
            let alias_provider = SourceTableProvider::new(
                Arc::clone(&bridge),
                short_name,
                definition.name(),
                alias_schema,
                definition.property(SOURCE_PRIMARY_KEY_PROPERTY),
            )?;
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

fn df_schema_to_arrow(schema: &DFSchemaRef) -> anyhow::Result<SchemaRef> {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

fn gather_handle_streams(
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
