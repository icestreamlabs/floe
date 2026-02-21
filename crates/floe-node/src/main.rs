mod cli;
mod config;
mod http_ingest;
mod metrics;
mod sinks;

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use clap::Parser;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::DFSchemaRef;
use dbsp::collections::CompactionPolicy;
use dbsp::storage::gc::{GcPolicy, GcService};
use dbsp::storage::{KeyValueTable, SlateTable};
use dbsp::{CompactionSchedulerConfig, StreamRetention};
use floe_executor::{
    BuildInputs, ConsolidationMode, DbspBridge, DbspGraphBuilder, FloeQueryContext, GraphTaskError,
    MaterializedViewRegistry, MaterializedViewTableProvider, OuterStreamRegistry, SourceRowDecoder,
    SourceTableProvider, ValidatedPlan, validate_dbsp_plan,
};
use floe_node_core::connector::{ConnectorContext, run_connector};
use floe_node_core::file_connector::{FileConnector, FileConnectorConfig};
use floe_node_core::generator;
use floe_node_core::kafka_connector::{KafkaConnector, KafkaConnectorConfig};
use floe_node_core::object_store_connector::{ObjectStoreConnector, ObjectStoreConnectorConfig};
use floe_node_core::planner::{
    PlannedMaterializedView, camel_case_schema, plan_materialized_views,
};
use floe_node_core::postgres_cdc_connector::{PostgresCdcConnector, PostgresCdcConnectorConfig};
use floe_node_core::tail_client;
use floe_server as server;
use floe_sql_parser::{FloeStatement, MaterializedViewDefinition, parse_floe_program};
use floe_storage::MaterializedViewMetadata;
use futures::future::select_all;
use slatedb::config::{CompactorOptions, Settings};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::{
    ConnectorConfig, SinkConfig, SinkSpec, apply_connector_properties, load_config,
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
use http_ingest::HttpIngestConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    metrics::init();
    let cli = cli::Cli::parse();
    let run_args = match cli.command {
        cli::Command::Run(args) => args,
        cli::Command::Tail(args) => {
            let config = args.to_config()?;
            tail_client::run(config)?;
            return Ok(());
        }
    };

    if run_args.kafka_brokers.is_some() && run_args.kafka_topics.is_empty() {
        return Err(anyhow::anyhow!(
            "--kafka-topics is required when --kafka-brokers is set"
        ));
    }
    let stream_gc = StreamGcConfig {
        grace_period_ms: run_args.zset_gc_grace_period_ms,
    };
    let gc_policy = GcPolicy {
        grace_period: Duration::from_millis(stream_gc.grace_period_ms),
    };

    let config = if let Some(path) = run_args.config.as_deref() {
        Some(load_config(path)?)
    } else {
        None
    };

    let (connector_specs, mut sink_specs) = if let Some(config) = config {
        let connectors = normalize_connectors(config.connectors)?;
        if connectors.is_empty() {
            return Err(anyhow!("config must declare at least one connector"));
        }
        let sinks = normalize_sinks(config.sinks)?;
        (connectors, sinks)
    } else {
        let connectors = normalize_connectors(connectors_from_cli(&run_args))?;
        (connectors, Vec::new())
    };

    let mut source_registry = SourceRegistry::new();
    source_registry.extend(floe_node_core::generator::definitions()?);
    apply_connector_properties(&mut source_registry, &connector_specs);
    let available_sources = available_sources_from_registry(&source_registry);

    let slate_settings = load_slatedb_settings(&run_args)?;
    let storage = server::init_storage(slate_settings).await?;
    let db = storage.db();
    let mut materialized_view_map: HashMap<String, MaterializedViewDefinition> = HashMap::new();
    let mut sql_sink_specs = Vec::new();
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
    if let Some(sql_program) = run_args.mv_query.as_deref() {
        for statement in parse_floe_program(sql_program)? {
            match statement {
                FloeStatement::CreateMaterializedView(definition) => {
                    let name = definition.name().to_string();
                    if definition.if_not_exists() && materialized_view_map.contains_key(&name) {
                        tracing::info!(
                            view = %name,
                            "materialized view already exists; skipping due to IF NOT EXISTS"
                        );
                    } else {
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
                                    "persist materialized view definition for '{}'",
                                    definition.name()
                                )
                            })?;
                        materialized_view_map.insert(name, definition);
                    }
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

    let mut materialized_views: Vec<MaterializedViewDefinition> =
        materialized_view_map.into_values().collect();
    materialized_views.sort_by(|a, b| a.name().cmp(b.name()));

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
    let consolidation_mode = match run_args.output_consolidation_mode {
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
    let (task_event_tx, mut task_event_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    let graph_cancel = CancellationToken::new();
    let cancel_for_monitor = graph_cancel.clone();
    let task_monitor: JoinHandle<()> = tokio::spawn(async move {
        while let Some(event) = task_event_rx.recv().await {
            tracing::error!(
                graph_id = %event.graph_id,
                task = %event.task,
                error = %event.error,
                "graph background task failed"
            );
            cancel_for_monitor.cancel();
        }
    });
    for (idx, plan) in circuit_plans.iter().enumerate() {
        let mv_def = &planned_materialized_views[idx];
        let view_name = mv_def.definition().name();
        let required_sources = &plan_required_sources[idx];
        let handle_streams = {
            let registry_guard = outer_registry.lock().await;
            gather_handle_streams(&registry_guard, required_sources)
        };
        tracing::info!(
            view = %view_name,
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

    let connector_cancel = CancellationToken::new();
    let connector_count = connector_specs.len();
    let per_connector_queue_capacity = (queue_capacity / connector_count).max(1);

    let mut connector_handles: Vec<JoinHandle<()>> = Vec::new();
    let mut connector_queues: Vec<ConnectorQueue> = Vec::new();
    let definitions = source_registry.definitions().to_vec();

    for connector in connector_specs {
        let (sender, receiver) = core_source::channel(per_connector_queue_capacity);
        connector_queues.push(ConnectorQueue::new(connector.name.clone(), receiver));
        let cancel = connector_cancel.clone();
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
                };
                connector_handles.push(tokio::spawn(async move {
                    if let Err(err) = http_ingest::run_http_ingest(config, sender, cancel).await {
                        tracing::error!(error = %err, "HTTP ingest server failed");
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
                let definitions = definitions.clone();
                connector_handles.push(tokio::spawn(async move {
                    let config = KafkaConnectorConfig {
                        brokers,
                        topics,
                        group_id,
                        default_source,
                        poll_timeout,
                        max_messages_per_tick,
                    };
                    let mut connector = match KafkaConnector::new(config, definitions) {
                        Ok(connector) => connector,
                        Err(err) => {
                            tracing::error!(error = %err, "Kafka connector config invalid");
                            return;
                        }
                    };
                    let ctx = ConnectorContext::new(sender);
                    if let Err(err) = run_connector(&mut connector, &ctx, cancel).await {
                        tracing::error!(error = %err, "Kafka connector failed");
                    }
                }));
            }
            ConnectorConfig::File {
                path,
                default_source,
                ..
            } => {
                let definitions = definitions.clone();
                connector_handles.push(tokio::spawn(async move {
                    let config = FileConnectorConfig {
                        path: path.into(),
                        default_source,
                    };
                    let mut connector = FileConnector::new(config, definitions);
                    let ctx = ConnectorContext::new(sender);
                    if let Err(err) = run_connector(&mut connector, &ctx, cancel).await {
                        tracing::error!(error = %err, "File connector failed");
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
                connector_handles.push(tokio::spawn(async move {
                    let mut connector =
                        match floe_node_core::generator::NexmarkConnector::new(generator_config) {
                            Ok(connector) => connector,
                            Err(err) => {
                                tracing::error!(error = %err, "Nexmark connector config invalid");
                                return;
                            }
                        };
                    let ctx = ConnectorContext::new(sender);
                    if let Err(err) = run_connector(&mut connector, &ctx, cancel).await {
                        tracing::error!(error = %err, "Nexmark connector failed");
                    }
                }));
            }
            ConnectorConfig::ObjectStore {
                url,
                default_source,
                ..
            } => {
                let definitions = definitions.clone();
                connector_handles.push(tokio::spawn(async move {
                    let config = ObjectStoreConnectorConfig {
                        url,
                        default_source,
                    };
                    let mut connector = ObjectStoreConnector::new(config, definitions);
                    let ctx = ConnectorContext::new(sender);
                    if let Err(err) = run_connector(&mut connector, &ctx, cancel).await {
                        tracing::error!(error = %err, "Object store connector failed");
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
                let definitions = definitions.clone();
                connector_handles.push(tokio::spawn(async move {
                    let config = PostgresCdcConnectorConfig {
                        connection_string: connection,
                        slot,
                        poll_interval,
                        max_changes,
                        default_schema,
                        include_tables,
                        include_schema_in_source,
                    };
                    let mut connector = match PostgresCdcConnector::new(config, definitions) {
                        Ok(connector) => connector,
                        Err(err) => {
                            tracing::error!(error = %err, "Postgres CDC connector config invalid");
                            return;
                        }
                    };
                    let ctx = ConnectorContext::new(sender);
                    if let Err(err) = run_connector(&mut connector, &ctx, cancel).await {
                        tracing::error!(error = %err, "Postgres CDC connector failed");
                    }
                }));
            }
        }
    }
    let outer_for_task = Arc::clone(&outer_registry);
    let decoder_for_task = Arc::clone(&decoder_registry);
    let watermark_for_task = Arc::clone(&event_watermark);
    let mv_for_task = Arc::clone(&mv_registry);
    let executor_handle: JoinHandle<()> = tokio::spawn(async move {
        let mut connector_queues = connector_queues;
        let mut next_connector = 0usize;
        let mut epoch: u64 = 0;
        loop {
            if connector_queues.is_empty() {
                break;
            }
            if connector_queues
                .iter()
                .all(|queue| queue.pending.is_empty())
            {
                if !recv_from_any(&mut connector_queues).await {
                    break;
                }
            }
            drain_connectors(&mut connector_queues, per_connector_queue_capacity);

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
            let mut batch_max_event_ts: Option<u64> = None;
            let decode_span = tracing::debug_span!(
                "ingest_decode",
                epoch = pending_epoch,
                raw_batch_size = batch_len
            );
            let _decode_guard = decode_span.enter();
            for event in batch {
                let source_name = event.source().to_string();
                let decoder = match decoder_for_task.get(&source_name) {
                    Some(decoder) => decoder,
                    None => continue,
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
                if let Some(ts) = event_ts {
                    batch_max_event_ts = Some(batch_max_event_ts.map_or(ts, |max| max.max(ts)));
                }
                *decoded_counts.entry(source_name.clone()).or_insert(0) += 1;
                decoded_rows.push((source_name, row));
            }
            let decode_latency_ms = decode_start.elapsed().as_millis() as u64;
            metrics::observe_decode_latency_ms(decode_latency_ms);
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
            if let Some(batch_watermark) = batch_max_event_ts {
                let watermark_value = i64::try_from(batch_watermark).unwrap_or(i64::MAX);
                let prev = watermark_for_task.load(Ordering::Relaxed);
                let next = prev.max(watermark_value);
                if next != prev {
                    watermark_for_task.store(next, Ordering::Relaxed);
                }
                if next >= 0 {
                    mv_for_task.update_watermark_all(next as u64);
                }
            }
            let tick_start = Instant::now();
            let tick_span = tracing::info_span!(
                "connector_tick",
                epoch,
                watermark = watermark_for_task.load(Ordering::Relaxed),
            );
            let _tick_guard = tick_span.enter();
            // Advance frontier for all sources this epoch, even if they had no rows.
            if let Err(err) = registry.tick_all().await {
                tracing::error!(epoch, error = %err, "failed to tick outer streams");
            } else if should_sample(&TICK_LOG_COUNTER, TICK_LOG_SAMPLE_EVERY) {
                tracing::debug!(epoch, "advanced all source frontiers");
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

    let sink_handles = sinks::spawn_sinks(
        sink_specs,
        query.clone(),
        Arc::clone(&mv_registry),
        connector_cancel.clone(),
    );

    let server_result = server::run(query, Arc::clone(&mv_registry)).await;

    connector_cancel.cancel();
    executor_handle.abort();
    task_monitor.abort();

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

    if let Err(err) = executor_handle.await
        && !err.is_cancelled()
    {
        tracing::error!(error = %err, "executor task joined with error");
    }

    if let Err(err) = task_monitor.await
        && !err.is_cancelled()
    {
        tracing::error!(error = %err, "graph monitor task joined with error");
    }

    server_result
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
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

fn should_sample(counter: &AtomicU64, every: u64) -> bool {
    if every == 0 {
        return true;
    }
    counter
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(every)
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

fn drain_connectors(queues: &mut Vec<ConnectorQueue>, capacity: usize) {
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
    queues.retain(|queue| !(queue.closed && queue.pending.is_empty()));
}

fn build_batch(
    queues: &mut Vec<ConnectorQueue>,
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
    use serde_json::json;

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
