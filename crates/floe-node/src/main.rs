mod cli;
mod executor;
mod generator;
mod planner;
mod server;
mod source;
mod sql;

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::DFSchemaRef;
use floe_executor::{FloeQueryContext, MaterializedViewRegistry, MaterializedViewTableProvider};
use floe_sql_parser::{MaterializedViewDefinition, parse_materialized_view};
use planner::{PlannedMaterializedView, plan_materialized_views};
use tokio::task::JoinHandle;

use crate::executor::{MaterializedExecutor, build_dataflows, build_executor_sources};
use crate::source::SourceRegistry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = cli::Cli::parse();

    let mut source_registry = SourceRegistry::new();
    source_registry.extend(generator::definitions()?);

    let storage = server::init_storage().await?;
    let storage_db = storage.db();

    let mut materialized_views: Vec<MaterializedViewDefinition> = Vec::new();
    if let Some(sql) = cli.mv_query.as_deref() {
        materialized_views.push(parse_materialized_view(sql)?);
    }

    let planned_materialized_views =
        plan_materialized_views(&source_registry, &materialized_views).await?;
    let dataflow_plans = build_dataflows(&planned_materialized_views)?;

    let executor_sources = Arc::new(build_executor_sources(&source_registry));
    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let executor = if dataflow_plans.is_empty() {
        None
    } else {
        Some(
            MaterializedExecutor::new(
                &dataflow_plans,
                Arc::clone(&executor_sources),
                Arc::clone(&mv_registry),
                Some(storage_db.clone()),
            )
            .await?,
        )
    };

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

    let mut maybe_rx = Some(event_rx);
    let executor_handle: Option<JoinHandle<()>> = if let Some(mut executor) = executor {
        Some(tokio::spawn(async move {
            let mut rx = maybe_rx.take().expect("receiver available");
            while let Some(event) = rx.recv().await {
                if let Err(err) = executor.ingest(event.clone()).await {
                    eprintln!("executor ingestion failed: {err}");
                    continue;
                }
            }
        }))
    } else {
        Some(tokio::spawn(async move {
            let mut rx = maybe_rx.take().expect("receiver available");
            while let Some(event) = rx.recv().await {
                match event.to_json_string() {
                    Ok(line) => println!("{line}"),
                    Err(err) => eprintln!("failed to encode source event: {err}"),
                }
            }
        }))
    };

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
    if let Some(handle) = &executor_handle {
        handle.abort();
    }

    if let Err(err) = generator_handle.await {
        if !err.is_cancelled() {
            eprintln!("generator task joined with error: {err}");
        }
    }

    if let Some(handle) = executor_handle {
        if let Err(err) = handle.await {
            if !err.is_cancelled() {
                eprintln!("executor task joined with error: {err}");
            }
        }
    }

    let _ = source_registry;
    let _ = planned_materialized_views;

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
