use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::common::{Result as DFResult, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream, Statistics,
};

#[derive(Debug)]
pub struct SnapshotScanExec {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    partition_count: usize,
    cache: PlanProperties,
}

impl SnapshotScanExec {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        let partition_count = batches.len().max(1);
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative);
        Self {
            schema,
            batches,
            partition_count,
            cache,
        }
    }
}

impl DisplayAs for SnapshotScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SnapshotScanExec: partitions={}", self.partition_count)
            }
            DisplayFormatType::TreeRender => write!(f, ""),
        }
    }
}

impl ExecutionPlan for SnapshotScanExec {
    fn name(&self) -> &str {
        "SnapshotScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("SnapshotScanExec does not support children")
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition >= self.partition_count {
            return internal_err!("invalid partition {partition} for SnapshotScanExec");
        }
        let batches = self
            .batches
            .iter()
            .enumerate()
            .filter_map(|(idx, batch)| {
                (idx % self.partition_count == partition).then_some(batch.clone())
            })
            .collect::<Vec<_>>();
        let stream = MemoryStream::try_new(batches, Arc::clone(&self.schema), None)?;
        Ok(Box::pin(stream))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Statistics> {
        if let Some(idx) = partition
            && idx >= self.partition_count
        {
            return internal_err!("invalid partition index {idx} for SnapshotScanExec");
        }
        let mut rows = 0usize;
        let mut bytes = 0usize;
        for (idx, batch) in self.batches.iter().enumerate() {
            if let Some(partition) = partition
                && idx % self.partition_count != partition
            {
                continue;
            }
            rows = rows.saturating_add(batch.num_rows());
            bytes = bytes.saturating_add(batch.get_array_memory_size());
        }
        Ok(Statistics::new_unknown(self.schema.as_ref())
            .with_num_rows(Precision::Exact(rows))
            .with_total_byte_size(Precision::Exact(bytes)))
    }
}
