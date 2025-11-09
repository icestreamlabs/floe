use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use slatedb::Db;

use crate::checkpoint::{CheckpointManager, CheckpointStore};
use crate::circuit_builder::{Circuit, CircuitContext, SourceRegistry};
use crate::dataflow_plan::DataflowPlan;
use crate::dbsp_bridge::DbspBridge;
use crate::materialized_view::MaterializedViewRegistry;
use crate::operators::EventQueue;
use crate::outer_stream::OuterStreamRegistry;

mod barrier;
mod graph_builder;
mod ingest;
mod tick_loop;

pub use graph_builder::{BuiltGraph, build_graph};
pub use ingest::{IngestedRow, ScanRuntime};
pub use tick_loop::TickLoop;

use ingest::ExecutionRuntime;

#[cfg(test)]
mod tests;

pub async fn instantiate_tick_loop(
    plan: &DataflowPlan,
    sources: Arc<SourceRegistry>,
    mv_registry: Arc<MaterializedViewRegistry>,
    db: Option<Arc<Db>>,
) -> Result<TickLoop> {
    let mut circuit = Circuit::new();
    let mut ctx = CircuitContext::new(&mut circuit, Arc::clone(&sources));
    ctx.build_plan(plan)
        .context("build circuit plan from dataflow")?;

    let queue: EventQueue = Arc::new(Mutex::new(VecDeque::new()));
    let mut bridge = match db {
        Some(db) => Some(
            DbspBridge::new(db)
                .await
                .context("initialize DBSP bridge")?,
        ),
        None => None,
    };
    let (checkpoint_table, checkpoint_manifest) = if let Some(bridge_ref) = bridge.as_ref() {
        let table = bridge_ref.table();
        let store = CheckpointStore::new(table.clone(), plan.graph_id.clone());
        let manifest = store.load_latest().await?;
        (Some(table), manifest)
    } else {
        (None, None)
    };
    let built = build_graph(
        &ctx,
        plan,
        Arc::clone(&mv_registry),
        &queue,
        checkpoint_manifest.as_ref(),
        bridge.as_mut(),
    )
    .await?;

    let checkpoint = if let Some(table) = checkpoint_table {
        Some(
            CheckpointManager::new_with_manifest(
                plan.graph_id.clone(),
                table,
                checkpoint_manifest.clone(),
            )
            .await
            .context("initialize checkpoint manager")?,
        )
    } else {
        None
    };

    let runtime = ExecutionRuntime::new(ScanRuntime::new(sources));
    let outer_streams = if let Some(bridge_ref) = bridge.as_mut() {
        if !built.scan_bindings.is_empty() {
            let sources: Vec<String> = built
                .scan_bindings
                .iter()
                .map(|(src, _)| src.clone())
                .collect();
            Some(
                OuterStreamRegistry::from_sources(sources, bridge_ref)
                    .await
                    .context("initialize outer stream registry")?,
            )
        } else {
            None
        }
    } else {
        None
    };
    let mut tick = TickLoop::with_graph(
        runtime,
        built.ops,
        queue,
        built.scan_operator_map,
        outer_streams,
        checkpoint,
    );
    if let Some(manager) = tick.checkpoint.as_ref() {
        if let Some(manifest) = manager.latest_manifest() {
            tick.barrier_clock.bootstrap(manifest.watermark);
        }
    }
    tick.register_bindings(&built.scan_bindings)?;
    Ok(tick)
}
