use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arc_swap::ArcSwap;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::{Result as DFResult, internal_err};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream, Statistics,
};

const DYNAMIC_STATE_SCAN_PARTITIONS: usize = 16;

#[derive(Clone)]
pub struct DynamicStateTableProvider {
    schema: SchemaRef,
    state: Arc<ArcSwap<Vec<RecordBatch>>>,
    exec: Arc<DynamicStateExec>,
}

impl DynamicStateTableProvider {
    pub fn new(schema: SchemaRef) -> Self {
        let state = Arc::new(ArcSwap::from_pointee(Vec::new()));
        let exec = Arc::new(DynamicStateExec::new(
            Arc::clone(&schema),
            Arc::clone(&state),
        ));
        Self {
            schema,
            state,
            exec,
        }
    }

    pub fn set_batches(&self, batches: Vec<RecordBatch>) {
        self.state.store(Arc::new(batches));
    }

    pub fn append_batches(&self, mut batches: Vec<RecordBatch>) {
        if batches.is_empty() {
            return;
        }
        let current = self.state.load_full();
        if current.is_empty() {
            self.state.store(Arc::new(batches));
            return;
        }
        let mut next = Vec::with_capacity(current.len().saturating_add(batches.len()));
        next.extend(current.iter().cloned());
        next.append(&mut batches);
        self.state.store(Arc::new(next));
    }

    pub fn set_snapshot(&self, snapshot: Arc<Vec<RecordBatch>>) {
        self.state.store(snapshot);
    }

    pub fn snapshot(&self) -> Arc<Vec<RecordBatch>> {
        self.state.load_full()
    }

    pub fn exec(&self) -> Arc<DynamicStateExec> {
        Arc::clone(&self.exec)
    }
}

impl fmt::Debug for DynamicStateTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamicStateTableProvider")
            .field("schema", &self.schema)
            .finish()
    }
}

#[async_trait::async_trait]
impl TableProvider for DynamicStateTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut exec: Arc<dyn ExecutionPlan> = if limit.is_some() {
            Arc::new(DynamicStateExec::new_with_limit(
                Arc::clone(&self.schema),
                Arc::clone(&self.state),
                limit,
            ))
        } else {
            self.exec()
        };

        if let Some(projection) = projection {
            let exprs = projection
                .iter()
                .map(|index| {
                    let field = self.schema.field(*index);
                    ProjectionExpr {
                        expr: Arc::new(Column::new(field.name(), *index)),
                        alias: field.name().to_string(),
                    }
                })
                .collect::<Vec<_>>();
            exec = Arc::new(ProjectionExec::try_new(exprs, exec)?);
        }

        Ok(exec)
    }
}

#[derive(Debug)]
pub struct DynamicStateExec {
    schema: SchemaRef,
    state: Arc<ArcSwap<Vec<RecordBatch>>>,
    limit: Option<usize>,
    partition_count: usize,
    cache: PlanProperties,
}

#[derive(Debug, Clone, Copy)]
struct SnapshotStatistics {
    rows: usize,
    bytes: usize,
    exact_bytes: bool,
}

impl DynamicStateExec {
    pub fn new(schema: SchemaRef, state: Arc<ArcSwap<Vec<RecordBatch>>>) -> Self {
        Self::new_with_limit(schema, state, None)
    }

    fn new_with_limit(
        schema: SchemaRef,
        state: Arc<ArcSwap<Vec<RecordBatch>>>,
        limit: Option<usize>,
    ) -> Self {
        let partition_count = if limit.is_some() {
            1
        } else {
            DYNAMIC_STATE_SCAN_PARTITIONS
        };
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative);

        Self {
            schema,
            state,
            limit,
            partition_count,
            cache,
        }
    }

    fn snapshot_batches(&self) -> Arc<Vec<RecordBatch>> {
        self.state.load_full()
    }

    fn limited_snapshot_batches(&self, partition: usize) -> Vec<RecordBatch> {
        let snapshot = self.snapshot_batches();
        let Some(limit) = self.limit else {
            return snapshot
                .iter()
                .enumerate()
                .filter_map(|(idx, batch)| {
                    (idx % self.partition_count == partition).then_some(batch.clone())
                })
                .collect();
        };
        if limit == 0 {
            return Vec::new();
        }

        let mut batches = Vec::new();
        let mut remaining = limit;
        for batch in snapshot.iter() {
            if remaining == 0 {
                break;
            }
            if batch.num_rows() <= remaining {
                batches.push(batch.clone());
                remaining -= batch.num_rows();
            } else {
                batches.push(batch.slice(0, remaining));
                break;
            }
        }
        batches
    }

    fn snapshot_statistics(&self, partition: Option<usize>) -> SnapshotStatistics {
        let snapshot = self.snapshot_batches();
        let mut rows = 0usize;
        let mut bytes = 0usize;
        let mut exact_bytes = true;
        let mut remaining = self.limit.unwrap_or(usize::MAX);

        for (idx, batch) in snapshot.iter().enumerate() {
            if self.limit.is_none()
                && let Some(partition) = partition
                && idx % self.partition_count != partition
            {
                continue;
            }
            if remaining == 0 {
                break;
            }

            let batch_rows = batch.num_rows();
            let batch_bytes = batch.get_array_memory_size();
            if batch_rows <= remaining {
                rows = rows.saturating_add(batch_rows);
                bytes = bytes.saturating_add(batch_bytes);
                remaining = remaining.saturating_sub(batch_rows);
            } else {
                rows = rows.saturating_add(remaining);
                if batch_rows > 0 {
                    let partial_bytes = batch_bytes
                        .saturating_mul(remaining)
                        .checked_div(batch_rows)
                        .unwrap_or(0);
                    bytes = bytes.saturating_add(partial_bytes);
                }
                exact_bytes = false;
                break;
            }
        }

        SnapshotStatistics {
            rows,
            bytes,
            exact_bytes,
        }
    }
}

impl DisplayAs for DynamicStateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DynamicStateExec: partitions={}", self.partition_count)
            }
            DisplayFormatType::TreeRender => write!(f, ""),
        }
    }
}

impl ExecutionPlan for DynamicStateExec {
    fn name(&self) -> &str {
        "DynamicStateExec"
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
            internal_err!("DynamicStateExec does not support children")
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition >= self.partition_count {
            return internal_err!("Invalid partition {partition} for DynamicStateExec");
        }

        let batches = self.limited_snapshot_batches(partition);
        let stream = MemoryStream::try_new(batches, Arc::clone(&self.schema), None)?;
        Ok(Box::pin(stream))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Statistics> {
        if let Some(idx) = partition
            && idx >= self.partition_count
        {
            return internal_err!("Invalid partition index {idx} for DynamicStateExec");
        }

        let stats = self.snapshot_statistics(partition);
        let byte_size = if stats.exact_bytes {
            Precision::Exact(stats.bytes)
        } else {
            Precision::Inexact(stats.bytes)
        };

        Ok(Statistics::new_unknown(self.schema.as_ref())
            .with_num_rows(Precision::Exact(stats.rows))
            .with_total_byte_size(byte_size))
    }
}
