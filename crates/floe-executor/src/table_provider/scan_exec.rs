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

const SNAPSHOT_SCAN_TARGET_ROWS_PER_PARTITION: usize = 4096;
const SNAPSHOT_SCAN_MAX_PARTITIONS: usize = 16;

#[derive(Debug)]
pub struct SnapshotScanExec {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    partition_count: usize,
    cache: PlanProperties,
}

impl SnapshotScanExec {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        let partition_count = partition_count_for_batches(&batches);
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

pub(crate) fn partition_count_for_batches(batches: &[RecordBatch]) -> usize {
    let total_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    if total_rows == 0 {
        return 1;
    }
    total_rows
        .div_ceil(SNAPSHOT_SCAN_TARGET_ROWS_PER_PARTITION)
        .clamp(1, SNAPSHOT_SCAN_MAX_PARTITIONS)
        .min(total_rows)
}

pub(crate) fn partition_record_batches(
    batches: &[RecordBatch],
    partition: usize,
    partition_count: usize,
) -> Vec<RecordBatch> {
    let total_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    if total_rows == 0 {
        return Vec::new();
    }

    let base_rows = total_rows / partition_count;
    let extra_rows = total_rows % partition_count;
    let start = partition
        .saturating_mul(base_rows)
        .saturating_add(partition.min(extra_rows));
    let len = base_rows + usize::from(partition < extra_rows);
    let end = start.saturating_add(len);

    let mut result = Vec::new();
    let mut batch_start = 0usize;
    for batch in batches {
        let batch_rows = batch.num_rows();
        let batch_end = batch_start.saturating_add(batch_rows);
        let overlap_start = start.max(batch_start);
        let overlap_end = end.min(batch_end);
        if overlap_start < overlap_end {
            result.push(batch.slice(
                overlap_start.saturating_sub(batch_start),
                overlap_end.saturating_sub(overlap_start),
            ));
        }
        batch_start = batch_end;
        if batch_start >= end {
            break;
        }
    }
    result
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
        let batches = partition_record_batches(&self.batches, partition, self.partition_count);
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
        let batches = match partition {
            Some(partition) => {
                partition_record_batches(&self.batches, partition, self.partition_count)
            }
            None => self.batches.clone(),
        };
        for batch in batches {
            rows = rows.saturating_add(batch.num_rows());
            bytes = bytes.saturating_add(batch.get_array_memory_size());
        }
        Ok(Statistics::new_unknown(self.schema.as_ref())
            .with_num_rows(Precision::Exact(rows))
            .with_total_byte_size(Precision::Exact(bytes)))
    }
}
