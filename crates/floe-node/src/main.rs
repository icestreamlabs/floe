mod cli;
mod generator;
mod planner;
mod server;
mod source;
mod sql;

use clap::Parser;
use floe_sql_parser::{MaterializedViewDefinition, parse_materialized_view};
use planner::plan_materialized_views;
use tokio::task::JoinHandle;

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

    let (event_tx, mut event_rx) = source::channel(1024);

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

    let printer_handle: JoinHandle<()> = tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event.to_json_string() {
                Ok(line) => println!("{line}"),
                Err(err) => eprintln!("failed to encode source event: {err}"),
            }
        }
    });

    let server_result = server::run().await;

    drop(event_tx);
    generator_handle.abort();
    printer_handle.abort();

    if let Err(err) = generator_handle.await {
        if !err.is_cancelled() {
            eprintln!("generator task joined with error: {err}");
        }
    }

    if let Err(err) = printer_handle.await {
        if !err.is_cancelled() {
            eprintln!("printer task joined with error: {err}");
        }
    }

    let _ = source_registry;
    let _ = planned_materialized_views;

    server_result
}
