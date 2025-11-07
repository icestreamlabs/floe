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
use floe_executor::{FloeQueryContext, MaterializedViewRegistry, Timestamp};
use floe_sql_parser::{MaterializedViewDefinition, parse_materialized_view};
use planner::plan_materialized_views;
use tokio::task::JoinHandle;

use crate::executor::{MaterializedExecutor, build_dataflows, build_executor_sources};
use crate::source::SourceRegistry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = cli::Cli::parse();

    let mut source_registry = SourceRegistry::new();
    source_registry.extend(generator::definitions()?);

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
        Some(MaterializedExecutor::new(
            &dataflow_plans,
            Arc::clone(&executor_sources),
            Arc::clone(&mv_registry),
        )?)
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
            let mut timestamp: Timestamp = 0;
            let mut rx = maybe_rx.take().expect("receiver available");
            while let Some(event) = rx.recv().await {
                timestamp = timestamp.saturating_add(1);
                if let Err(err) = executor.ingest(event.clone(), timestamp) {
                    eprintln!("executor ingestion failed: {err}");
                    continue;
                }
                if let Err(err) = executor.advance_source_watermark(event.source(), timestamp) {
                    eprintln!("failed to update watermark for {}: {err}", event.source());
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

    let storage = server::init_storage().await?;
    let query = FloeQueryContext::new(storage);
    query
        .preload_tables()
        .await
        .context("failed to register tables with DataFusion")?;

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
