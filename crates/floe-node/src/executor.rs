use std::sync::Arc;

use anyhow::Result;
use floe_core::source::SourceEvent;
use floe_executor::{
    DataflowPlan, MaterializedViewRegistry, QueryPlanner, SourceRegistry, TickLoop, Timestamp,
    instantiate_tick_loop,
};

use crate::planner::PlannedMaterializedView;
use crate::source;

pub fn build_executor_sources(sources: &source::SourceRegistry) -> SourceRegistry {
    let mut registry = SourceRegistry::new();
    registry.extend(sources.definitions().iter().cloned());
    registry
}

pub fn build_dataflows(views: &[PlannedMaterializedView]) -> Result<Vec<DataflowPlan>> {
    let planner = QueryPlanner::new();
    views
        .iter()
        .map(|planned| planner.plan(planned.logical_plan(), planned.definition().name()))
        .collect()
}

pub struct MaterializedExecutor {
    tick_loops: Vec<TickLoop>,
}

impl MaterializedExecutor {
    pub fn new(
        plans: &[DataflowPlan],
        sources: Arc<SourceRegistry>,
        mv_registry: Arc<MaterializedViewRegistry>,
    ) -> Result<Self> {
        let mut tick_loops = Vec::with_capacity(plans.len());
        for plan in plans {
            let tick = instantiate_tick_loop(plan, Arc::clone(&sources), Arc::clone(&mv_registry))?;
            tick_loops.push(tick);
        }

        Ok(Self { tick_loops })
    }

    pub fn ingest(&mut self, event: SourceEvent, timestamp: Timestamp) -> Result<()> {
        if self.tick_loops.is_empty() {
            return Ok(());
        }
        for tick in self.tick_loops.iter_mut() {
            tick.process_events(std::iter::once((event.clone(), timestamp)))?;
        }
        Ok(())
    }

    pub fn advance_source_watermark(&mut self, source: &str, watermark: Timestamp) -> Result<()> {
        if self.tick_loops.is_empty() {
            return Ok(());
        }
        for tick in self.tick_loops.iter_mut() {
            tick.advance_source_watermark(source, watermark)?;
        }
        Ok(())
    }
}
