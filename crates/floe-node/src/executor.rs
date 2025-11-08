use std::sync::Arc;

use anyhow::Result;
use floe_core::source::SourceEvent;
use floe_executor::{
    DataflowPlan, MaterializedViewRegistry, QueryPlanner, SourceRegistry, TickLoop, Timestamp,
    instantiate_tick_loop,
};
use slatedb::Db;

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
    next_fallback_ts: Timestamp,
}

impl MaterializedExecutor {
    pub async fn new(
        plans: &[DataflowPlan],
        sources: Arc<SourceRegistry>,
        mv_registry: Arc<MaterializedViewRegistry>,
        db: Option<Arc<Db>>,
    ) -> Result<Self> {
        let mut tick_loops = Vec::with_capacity(plans.len());
        for plan in plans {
            let tick = instantiate_tick_loop(
                plan,
                Arc::clone(&sources),
                Arc::clone(&mv_registry),
                db.clone(),
            )
            .await?;
            tick_loops.push(tick);
        }

        Ok(Self {
            tick_loops,
            next_fallback_ts: 0,
        })
    }

    pub async fn ingest(&mut self, event: SourceEvent) -> Result<()> {
        if self.tick_loops.is_empty() {
            return Ok(());
        }
        self.next_fallback_ts = self.next_fallback_ts.saturating_add(1);
        let fallback_ts = self.next_fallback_ts;
        for tick in self.tick_loops.iter_mut() {
            tick.process_events(std::iter::once((event.clone(), fallback_ts)))
                .await?;
        }
        Ok(())
    }
}
