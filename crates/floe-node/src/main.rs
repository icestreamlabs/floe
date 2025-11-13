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
    let mut materialized_views: Vec<MaterializedViewDefinition> = Vec::new();
    if let Some(sql) = cli.mv_query.first() {
        materialized_views.push(parse_materialized_view(sql)?);
    }

    let planned_materialized_views =
        plan_materialized_views(&source_registry, &materialized_views).await?;
    let circuit_plans = build_dataflows(&planned_materialized_views, &available_sources)?;
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
        eprintln!(
            "DBSP planning-only mode enabled (Phase 7 / Task 1). Execution is temporarily disabled."
        );
    }

    let mv_registry = Arc::new(MaterializedViewRegistry::new());

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

    let executor_handle: JoinHandle<()> = tokio::spawn(async move {
        let mut rx = event_rx;
        while let Some(event) = rx.recv().await {
            match event.to_json_string() {
                Ok(line) => println!("{line}"),
                Err(err) => eprintln!("failed to encode source event: {err}"),
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

    if let Err(err) = generator_handle.await {
        if !err.is_cancelled() {
            eprintln!("generator task joined with error: {err}");
        }
    }

    if let Err(err) = executor_handle.await {
        if !err.is_cancelled() {
            eprintln!("executor task joined with error: {err}");
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
