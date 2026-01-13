mod cli;
mod executor;
mod generator;
mod planner;
mod server;
mod source;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::DFSchemaRef;
use floe_executor::{
    BuildInputs, DbspBridge, DbspGraphBuilder, FloeQueryContext, GraphTaskError,
    MaterializedViewRegistry, MaterializedViewTableProvider, OuterStreamRegistry, SourceRowDecoder,
    ValidatedPlan, validate_dbsp_plan,
};
use floe_sql_parser::{MaterializedViewDefinition, parse_materialized_view};
use planner::{PlannedMaterializedView, plan_materialized_views};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::executor::{available_sources_from_registry, build_dataflows};
use crate::source::SourceRegistry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = cli::Cli::parse();

    if cli.mv_query.len() > 1 {
        anyhow::bail!("--mv-query may only be provided once");
    }

    let mut source_registry = SourceRegistry::new();
    source_registry.extend(generator::definitions()?);
    let available_sources = available_sources_from_registry(&source_registry);

    let storage = server::init_storage().await?;
    let db = storage.db();
    let mut materialized_views: Vec<MaterializedViewDefinition> = Vec::new();
    if let Some(sql) = cli.mv_query.first() {
        materialized_views.push(parse_materialized_view(sql)?);
    }

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
    let outer_registry = {
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        OuterStreamRegistry::from_validated_sources(&all_required_sources, &mut bridge)
            .await
            .context("initialize outer DBSP streams for sources")?
    };
    let outer_registry = Arc::new(Mutex::new(outer_registry));
    if circuit_plans.is_empty() {
        eprintln!("DBSP planning produced no circuit plans.");
    } else {
        eprintln!(
            "DBSP planning produced {} circuit plan(s):",
            circuit_plans.len()
        );
        for plan in &circuit_plans {
            eprintln!("  • CircuitPlan root node id = {}", plan.root);
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
            eprintln!(
                "graph '{}' background task '{}' failed: {:#}",
                event.graph_id, event.task, event.error
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
        eprintln!(
            "building DBSP graph for view '{view_name}' with required sources {:?} and handle streams {:?}",
            required_sources,
            handle_streams.keys()
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

    let (event_tx, event_rx) = source::channel(1024);

    let generator_config = generator::Config {
        events_per_second: cli.events_per_second,
        max_events: cli.max_events,
    };

    let generator_handle: JoinHandle<()> = {
        let sender = event_tx.clone();
        tokio::spawn(async move {
            if let Err(err) = generator::run(generator_config, sender).await {
                eprintln!("Nexmark generator failed: {err}");
            }
        })
    };

    let outer_for_task = Arc::clone(&outer_registry);
    let decoder_for_task = Arc::clone(&decoder_registry);
    let executor_handle: JoinHandle<()> = tokio::spawn(async move {
        let mut rx = event_rx;
        let mut epoch: u64 = 0;
        const MAX_BATCH: usize = 256;
        while let Some(first_event) = rx.recv().await {
            let mut batch = Vec::with_capacity(MAX_BATCH);
            batch.push(first_event);
            while batch.len() < MAX_BATCH {
                match rx.try_recv() {
                    Ok(event) => batch.push(event),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }

            let mut decoded_rows = Vec::with_capacity(batch.len());
            for event in batch {
                let source_name = event.source().to_string();
                let decoder = match decoder_for_task.get(&source_name) {
                    Some(decoder) => decoder,
                    None => continue,
                };
                let (row, _ts) = match decoder.decode(&event) {
                    Ok(result) => result,
                    Err(err) => {
                        eprintln!("failed to decode source event for {source_name}: {err}");
                        continue;
                    }
                };
                decoded_rows.push((source_name, row));
            }

            if decoded_rows.is_empty() {
                continue;
            }

            let mut registry = outer_for_task.lock().await;
            let mut changed = false;
            for (source_name, row) in decoded_rows {
                let Some(writer) = registry.writer_mut(&source_name) else {
                    eprintln!("no writer for source {source_name}, skipping row");
                    continue;
                };
                if let Err(err) = writer.append(&row, 1) {
                    eprintln!("failed to append row for '{source_name}': {err}");
                    continue;
                }
                changed = true;
                if source_name == generator::BID_SOURCE_NAME {
                    eprintln!("ingested bid row: {:?}", row);
                } else if source_name == generator::AUCTION_SOURCE_NAME {
                    eprintln!("ingested auction row: {:?}", row);
                }
            }

            if !changed {
                continue;
            }

            epoch = epoch.saturating_add(1);
            // Advance frontier for all sources this epoch, even if they had no rows.
            if let Err(err) = registry.tick_all().await {
                eprintln!("failed to tick outer streams at epoch {epoch}: {err}");
            } else {
                eprintln!("advanced all source frontiers to epoch {epoch}");
            }
        }
    });

    let query = FloeQueryContext::new(storage);
    query
        .preload_tables()
        .await
        .context("failed to register tables with DataFusion")?;
    register_materialized_view_tables(&query, &planned_materialized_views, &mv_registry)
        .context("register materialized view tables")?;

    let server_result = server::run(query, Arc::clone(&mv_registry)).await;

    drop(event_tx);
    generator_handle.abort();
    executor_handle.abort();
    task_monitor.abort();

    if let Err(err) = generator_handle.await
        && !err.is_cancelled()
    {
        eprintln!("generator task joined with error: {err}");
    }

    if let Err(err) = executor_handle.await
        && !err.is_cancelled()
    {
        eprintln!("executor task joined with error: {err}");
    }

    if let Err(err) = task_monitor.await
        && !err.is_cancelled()
    {
        eprintln!("graph monitor task joined with error: {err}");
    }

    server_result
}

fn register_materialized_view_tables(
    context: &FloeQueryContext,
    planned: &[PlannedMaterializedView],
    registry: &Arc<MaterializedViewRegistry>,
) -> anyhow::Result<()> {
    if planned.is_empty() {
        return Ok(());
    }

    let session = context.session();
    for mv in planned {
        let arrow_schema = df_schema_to_arrow(mv.logical_plan().schema())?;
        registry.set_schema(mv.definition().name(), arrow_schema.clone());
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
