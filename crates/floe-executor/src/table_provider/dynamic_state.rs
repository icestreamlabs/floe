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
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut exec: Arc<dyn ExecutionPlan> = self.exec();

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
    cache: PlanProperties,
}

impl DynamicStateExec {
    pub fn new(schema: SchemaRef, state: Arc<ArcSwap<Vec<RecordBatch>>>) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::RoundRobinBatch(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative);

        Self {
            schema,
            state,
            cache,
        }
    }

    fn snapshot_batches(&self) -> Arc<Vec<RecordBatch>> {
        self.state.load_full()
    }
}

impl DisplayAs for DynamicStateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DynamicStateExec: partitions=1")
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
        if partition > 0 {
            return internal_err!("Invalid partition {partition} for DynamicStateExec");
        }

        let snapshot = self.snapshot_batches();
        let batches = snapshot.iter().cloned().collect::<Vec<_>>();
        let stream = MemoryStream::try_new(batches, Arc::clone(&self.schema), None)?;
        Ok(Box::pin(stream))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Statistics> {
        if let Some(idx) = partition {
            if idx != 0 {
                return internal_err!("Invalid partition index {idx} for DynamicStateExec");
            }
        }

        let snapshot = self.snapshot_batches();
        let mut rows = 0usize;
        let mut bytes = 0usize;
        for batch in snapshot.iter() {
            rows = rows.saturating_add(batch.num_rows());
            bytes = bytes.saturating_add(batch.get_array_memory_size());
        }

        Ok(Statistics::new_unknown(self.schema.as_ref())
            .with_num_rows(Precision::Exact(rows))
            .with_total_byte_size(Precision::Exact(bytes)))
    }
}
