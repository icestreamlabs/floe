mod cli;
mod http_ingest;

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Parser;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::DFSchemaRef;
use floe_executor::{
    BuildInputs, DbspBridge, DbspGraphBuilder, FloeQueryContext, GraphTaskError,
    MaterializedViewRegistry, MaterializedViewTableProvider, OuterStreamRegistry, SourceRowDecoder,
    SourceTableProvider, ValidatedPlan, validate_dbsp_plan,
};
use floe_node_core::connector::{ConnectorContext, run_connector};
use floe_node_core::file_connector::{FileConnector, FileConnectorConfig};
use floe_node_core::generator;
use floe_node_core::kafka_connector::{KafkaConnector, KafkaConnectorConfig};
use floe_node_core::planner::{
    PlannedMaterializedView, camel_case_schema, plan_materialized_views,
};
use floe_node_core::tail_client;
use floe_server as server;
use floe_sql_parser::{MaterializedViewDefinition, parse_materialized_view};
use floe_storage::MaterializedViewMetadata;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

static INGEST_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
static TICK_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
static INGEST_METRICS_COUNTER: AtomicU64 = AtomicU64::new(0);
const INGEST_LOG_SAMPLE_EVERY: u64 = 512;
const TICK_LOG_SAMPLE_EVERY: u64 = 128;
const INGEST_METRICS_SAMPLE_EVERY: u64 = 128;

use floe_node_core::executor::{available_sources_from_registry, build_dataflows};
use floe_node_core::source as core_source;
use floe_node_core::source::SourceRegistry;
use http_ingest::HttpIngestConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
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
    if run_args.kafka_brokers.is_some() && run_args.input_file.is_some() {
        return Err(anyhow::anyhow!(
            "--kafka-brokers cannot be combined with --input-file"
        ));
    }

    let mut source_registry = SourceRegistry::new();
    source_registry.extend(floe_node_core::generator::definitions()?);
    let available_sources = available_sources_from_registry(&source_registry);

    let storage = server::init_storage().await?;
    let db = storage.db();
    let mut materialized_view_map: HashMap<String, MaterializedViewDefinition> = HashMap::new();
    let stored_views = storage
        .materialized_views()
        .await
        .context("load persisted materialized views")?;
    for metadata in stored_views {
        let definition = MaterializedViewDefinition::new(
            metadata.name(),
            metadata.query(),
            metadata.if_not_exists(),
        );
        materialized_view_map.insert(definition.name().to_string(), definition);
    }
    if let Some(sql) = run_args.mv_query.as_deref() {
        let definition = parse_materialized_view(sql)?;
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
                    format!("persist materialized view definition for '{}'", definition.name())
                })?;
            materialized_view_map.insert(name, definition);
        }
    }
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

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let mut graph_builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .context("initialize DBSP graph builder")?;
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
    let (event_tx, event_rx) = core_source::channel(queue_capacity);

    let generator_config = floe_node_core::generator::Config {
        events_per_second: run_args.events_per_second,
        max_events: run_args.max_events,
    };

    let connector_cancel = CancellationToken::new();

    let http_ingest_handle: Option<JoinHandle<()>> = if let Some(port) = run_args.http_port {
        let sender = event_tx.clone();
        let cancel = connector_cancel.clone();
        let config = HttpIngestConfig {
            host: run_args.http_host.clone(),
            port,
            default_source: run_args.http_source.clone(),
        };
        Some(tokio::spawn(async move {
            if let Err(err) = http_ingest::run_http_ingest(config, sender, cancel).await {
                tracing::error!(error = %err, "HTTP ingest server failed");
            }
        }))
    } else {
        None
    };

    let generator_handle: JoinHandle<()> = {
        let sender = event_tx.clone();
        let cancel = connector_cancel.clone();
        let input_file = run_args.input_file.clone();
        let input_source = run_args.input_source.clone();
        let kafka_brokers = run_args.kafka_brokers.clone();
        let kafka_topics = run_args.kafka_topics.clone();
        let kafka_group_id = run_args.kafka_group_id.clone();
        let kafka_default_source = run_args.kafka_default_source.clone();
        let kafka_poll_ms = run_args.kafka_poll_ms;
        let kafka_max_messages = run_args.kafka_max_messages;
        let definitions = source_registry.definitions().to_vec();
        tokio::spawn(async move {
            if let Some(brokers) = kafka_brokers {
                let definitions = definitions.clone();
                let config = KafkaConnectorConfig {
                    brokers,
                    topics: kafka_topics,
                    group_id: kafka_group_id,
                    default_source: kafka_default_source,
                    poll_timeout: Duration::from_millis(kafka_poll_ms),
                    max_messages_per_tick: kafka_max_messages,
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
                return;
            }

            if let Some(path) = input_file {
                let definitions = definitions.clone();
                let config = FileConnectorConfig {
                    path: path.into(),
                    default_source: input_source,
                };
                let mut connector = FileConnector::new(config, definitions);
                let ctx = ConnectorContext::new(sender);
                if let Err(err) = run_connector(&mut connector, &ctx, cancel).await {
                    tracing::error!(error = %err, "File connector failed");
                }
                return;
            }

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
        })
    };

    let sender_for_metrics = event_tx.clone();
    let outer_for_task = Arc::clone(&outer_registry);
    let decoder_for_task = Arc::clone(&decoder_registry);
    let executor_handle: JoinHandle<()> = tokio::spawn(async move {
        let mut rx = event_rx;
        let mut pending: VecDeque<core_source::SourceEvent> = VecDeque::new();
        let mut epoch: u64 = 0;
        loop {
            if pending.is_empty() {
                match rx.recv().await {
                    Some(event) => pending.push_back(event),
                    None => break,
                }
            }

            while pending.len() < queue_capacity {
                match rx.try_recv() {
                    Ok(event) => pending.push_back(event),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }

            let mut batch = Vec::with_capacity(max_batch);
            let mut per_source_counts: HashMap<String, usize> = HashMap::new();
            let mut remaining: VecDeque<core_source::SourceEvent> = VecDeque::new();
            while let Some(event) = pending.pop_front() {
                if batch.len() >= max_batch {
                    remaining.push_back(event);
                    continue;
                }
                let source = event.source();
                let count = per_source_counts.entry(source.to_string()).or_insert(0);
                if *count >= max_batch_per_source {
                    remaining.push_back(event);
                    continue;
                }
                *count += 1;
                batch.push(event);
            }
            pending = remaining;

            if batch.is_empty() {
                continue;
            }

            let batch_len = batch.len();
            let decode_start = Instant::now();
            let mut decoded_rows = Vec::with_capacity(batch_len);
            let mut decoded_counts: HashMap<String, usize> = HashMap::new();
            for event in batch {
                let source_name = event.source().to_string();
                let decoder = match decoder_for_task.get(&source_name) {
                    Some(decoder) => decoder,
                    None => continue,
                };
                let (row, _ts) = match decoder.decode(&event) {
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
                *decoded_counts.entry(source_name.clone()).or_insert(0) += 1;
                decoded_rows.push((source_name, row));
            }
            let decode_latency_ms = decode_start.elapsed().as_millis() as u64;

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

            epoch = epoch.saturating_add(1);
            let tick_start = Instant::now();
            // Advance frontier for all sources this epoch, even if they had no rows.
            if let Err(err) = registry.tick_all().await {
                tracing::error!(epoch, error = %err, "failed to tick outer streams");
            } else if should_sample(&TICK_LOG_COUNTER, TICK_LOG_SAMPLE_EVERY) {
                tracing::debug!(epoch, "advanced all source frontiers");
            }
            let tick_latency_ms = tick_start.elapsed().as_millis() as u64;

            if should_sample(&INGEST_METRICS_COUNTER, INGEST_METRICS_SAMPLE_EVERY) {
                let queue_depth = queue_capacity
                    .saturating_sub(sender_for_metrics.capacity())
                    .saturating_add(pending.len());
                tracing::info!(
                    epoch,
                    queue_depth,
                    batch_size = batch_len,
                    pending = pending.len(),
                    decoded_rows = decoded_rows_len,
                    decode_latency_ms,
                    tick_latency_ms,
                    per_source = ?decoded_counts,
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

    let server_result = server::run(query, Arc::clone(&mv_registry)).await;

    connector_cancel.cancel();
    drop(event_tx);
    executor_handle.abort();
    task_monitor.abort();

    if let Err(err) = generator_handle.await
        && !err.is_cancelled()
    {
        tracing::error!(error = %err, "generator task joined with error");
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

    if let Some(handle) = http_ingest_handle {
        handle.abort();
        if let Err(err) = handle.await
            && !err.is_cancelled()
        {
            tracing::error!(error = %err, "http ingest task joined with error");
        }
    }

    server_result
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn should_sample(counter: &AtomicU64, every: u64) -> bool {
    if every == 0 {
        return true;
    }
    counter
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(every)
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
