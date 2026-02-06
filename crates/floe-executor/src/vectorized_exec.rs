use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::collect;

use crate::delta_consolidation::{
    ConsolidationMode, ConsolidationOutput, ConsolidationStats, DeltaConsolidator,
};

#[derive(Debug, Clone)]
pub struct VectorizedTickOutput {
    pub batches: Vec<datafusion::arrow::record_batch::RecordBatch>,
    pub stats: ConsolidationStats,
}

pub struct VectorizedPlanExecutor {
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
    consolidator: DeltaConsolidator,
}

impl VectorizedPlanExecutor {
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        task_ctx: Arc<TaskContext>,
        output_delta_schema: SchemaRef,
        mode: ConsolidationMode,
    ) -> Result<Self> {
        let consolidator = DeltaConsolidator::with_mode(output_delta_schema, mode)?;
        Ok(Self {
            plan,
            task_ctx,
            consolidator,
        })
    }

    pub async fn run_tick(&self) -> Result<VectorizedTickOutput> {
        let raw_batches = collect(Arc::clone(&self.plan), Arc::clone(&self.task_ctx)).await?;
        let ConsolidationOutput { batches, stats } = self
            .consolidator
            .consolidate_with_stats(raw_batches)
            .await?;
        Ok(VectorizedTickOutput { batches, stats })
    }

    pub fn plan_ptr(&self) -> *const dyn ExecutionPlan {
        Arc::as_ptr(&self.plan)
    }
}
