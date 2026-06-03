use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use arc_swap::ArcSwap;
use datafusion::arrow::array::{ArrayRef, BooleanBuilder};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::{Result as DFResult, internal_err};
use datafusion::error::DataFusionError;
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
    state: Arc<ArcSwap<Vec<DynamicStateBatch>>>,
    key_state: Option<Arc<Mutex<DynamicKeyState>>>,
    next_generation: Arc<AtomicU64>,
    exec: Arc<DynamicStateExec>,
}

impl DynamicStateTableProvider {
    pub fn new(schema: SchemaRef) -> Self {
        Self::new_with_optional_key_indices(schema, None)
    }

    pub fn new_with_key_indices(schema: SchemaRef, key_indices: Vec<usize>) -> Result<Self> {
        for idx in &key_indices {
            if *idx >= schema.fields().len() {
                bail!(
                    "dynamic state key column index {} is outside schema width {}",
                    idx,
                    schema.fields().len()
                );
            }
        }
        Ok(Self::new_with_optional_key_indices(
            schema,
            Some(key_indices),
        ))
    }

    fn new_with_optional_key_indices(schema: SchemaRef, key_indices: Option<Vec<usize>>) -> Self {
        let state = Arc::new(ArcSwap::from_pointee(Vec::new()));
        let key_state = key_indices.map(|key_indices| {
            Arc::new(Mutex::new(DynamicKeyState {
                key_indices,
                latest_generation_by_key: HashMap::new(),
            }))
        });
        let next_generation = Arc::new(AtomicU64::new(1));
        let exec = Arc::new(DynamicStateExec::new(
            Arc::clone(&schema),
            Arc::clone(&state),
            key_state.clone(),
        ));
        Self {
            schema,
            state,
            key_state,
            next_generation,
            exec,
        }
    }

    pub fn set_batches(&self, batches: Vec<RecordBatch>) {
        let state_batches = self.state_batches(batches);
        if let Some(key_state) = self.key_state.as_ref()
            && let Err(err) = rebuild_key_state(&self.schema, &state_batches, key_state)
        {
            tracing::warn!(error = %err, "failed to rebuild dynamic source key index");
        }
        self.state.store(Arc::new(state_batches));
    }

    pub fn append_batches(&self, batches: Vec<RecordBatch>) {
        if batches.is_empty() {
            return;
        }
        let current = self.state.load_full();
        if current.is_empty() {
            self.set_batches(batches);
            return;
        }

        let state_batches = self.state_batches(batches);
        if state_batches.is_empty() {
            return;
        }
        if let Some(key_state) = self.key_state.as_ref()
            && let Err(err) = index_appended_batches(&self.schema, &state_batches, key_state)
        {
            tracing::warn!(error = %err, "failed to update dynamic source key index");
        }

        let mut next = Vec::with_capacity(current.len().saturating_add(state_batches.len()));
        next.extend(current.iter().cloned());
        next.extend(state_batches);
        self.state.store(Arc::new(next));
    }

    pub fn apply_keyed_delta(
        &self,
        touched_keys: &HashSet<Vec<u8>>,
        positive_batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let Some(key_state) = self.key_state.as_ref() else {
            bail!("dynamic state provider is not keyed");
        };
        for batch in &positive_batches {
            if batch.schema().as_ref() != self.schema.as_ref() {
                bail!("keyed delta batch schema does not match dynamic state schema");
            }
        }

        let state_batches = self.state_batches(positive_batches);
        index_keyed_delta(&self.schema, touched_keys, &state_batches, key_state)?;
        if state_batches.is_empty() {
            return Ok(());
        }

        let current = self.state.load_full();
        let mut next = Vec::with_capacity(current.len().saturating_add(state_batches.len()));
        next.extend(current.iter().cloned());
        next.extend(state_batches);
        self.state.store(Arc::new(next));
        Ok(())
    }

    pub fn set_snapshot(&self, snapshot: Arc<Vec<RecordBatch>>) {
        self.set_batches(snapshot.iter().cloned().collect());
    }

    pub fn snapshot(&self) -> Arc<Vec<RecordBatch>> {
        match effective_snapshot_batches(&self.schema, &self.state.load_full(), &self.key_state) {
            Ok(batches) => Arc::new(batches),
            Err(err) => {
                tracing::warn!(error = %err, "failed to materialize dynamic state snapshot");
                Arc::new(Vec::new())
            }
        }
    }

    pub fn exec(&self) -> Arc<DynamicStateExec> {
        Arc::clone(&self.exec)
    }

    fn state_batches(&self, batches: Vec<RecordBatch>) -> Vec<DynamicStateBatch> {
        batches
            .into_iter()
            .filter(|batch| batch.num_rows() > 0)
            .map(|batch| DynamicStateBatch {
                generation: self.next_generation.fetch_add(1, Ordering::Relaxed),
                batch,
            })
            .collect()
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
                self.key_state.clone(),
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
    state: Arc<ArcSwap<Vec<DynamicStateBatch>>>,
    key_state: Option<Arc<Mutex<DynamicKeyState>>>,
    limit: Option<usize>,
    partition_count: usize,
    cache: PlanProperties,
}

#[derive(Debug, Clone)]
struct DynamicStateBatch {
    generation: u64,
    batch: RecordBatch,
}

#[derive(Debug)]
struct DynamicKeyState {
    key_indices: Vec<usize>,
    latest_generation_by_key: HashMap<Vec<u8>, Option<u64>>,
}

#[derive(Debug, Clone, Copy)]
struct SnapshotStatistics {
    rows: usize,
    bytes: usize,
    exact_bytes: bool,
}

impl DynamicStateExec {
    fn new(
        schema: SchemaRef,
        state: Arc<ArcSwap<Vec<DynamicStateBatch>>>,
        key_state: Option<Arc<Mutex<DynamicKeyState>>>,
    ) -> Self {
        Self::new_with_limit(schema, state, key_state, None)
    }

    fn new_with_limit(
        schema: SchemaRef,
        state: Arc<ArcSwap<Vec<DynamicStateBatch>>>,
        key_state: Option<Arc<Mutex<DynamicKeyState>>>,
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
            key_state,
            limit,
            partition_count,
            cache,
        }
    }

    fn snapshot_batches(&self) -> Arc<Vec<DynamicStateBatch>> {
        self.state.load_full()
    }

    fn limited_snapshot_batches(&self, partition: usize) -> DFResult<Vec<RecordBatch>> {
        let snapshot = self.snapshot_batches();
        let Some(limit) = self.limit else {
            return partition_effective_batches(
                &self.schema,
                &snapshot,
                &self.key_state,
                partition,
                self.partition_count,
            );
        };
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut batches = Vec::new();
        let mut remaining = limit;
        for batch in effective_snapshot_batches(&self.schema, &snapshot, &self.key_state)? {
            if remaining == 0 {
                break;
            }
            if batch.num_rows() <= remaining {
                remaining -= batch.num_rows();
                batches.push(batch);
            } else {
                batches.push(batch.slice(0, remaining));
                break;
            }
        }
        Ok(batches)
    }

    fn snapshot_statistics(&self, partition: Option<usize>) -> DFResult<SnapshotStatistics> {
        let snapshot = self.snapshot_batches();
        let batches = if self.limit.is_none()
            && let Some(partition) = partition
        {
            partition_effective_batches(
                &self.schema,
                &snapshot,
                &self.key_state,
                partition,
                self.partition_count,
            )?
        } else {
            effective_snapshot_batches(&self.schema, &snapshot, &self.key_state)?
        };
        let mut rows = 0usize;
        let mut bytes = 0usize;
        let mut exact_bytes = true;
        let mut remaining = self.limit.unwrap_or(usize::MAX);

        for batch in batches {
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

        Ok(SnapshotStatistics {
            rows,
            bytes,
            exact_bytes,
        })
    }
}

fn partition_effective_batches(
    schema: &SchemaRef,
    snapshot: &[DynamicStateBatch],
    key_state: &Option<Arc<Mutex<DynamicKeyState>>>,
    partition: usize,
    partition_count: usize,
) -> DFResult<Vec<RecordBatch>> {
    let partition_batches = snapshot
        .iter()
        .enumerate()
        .filter_map(|(idx, batch)| (idx % partition_count == partition).then_some(batch.clone()))
        .collect::<Vec<_>>();
    effective_snapshot_batches(schema, &partition_batches, key_state)
}

fn effective_snapshot_batches(
    schema: &SchemaRef,
    snapshot: &[DynamicStateBatch],
    key_state: &Option<Arc<Mutex<DynamicKeyState>>>,
) -> DFResult<Vec<RecordBatch>> {
    let Some(key_state) = key_state else {
        return Ok(snapshot.iter().map(|entry| entry.batch.clone()).collect());
    };
    let key_state = key_state
        .lock()
        .map_err(|_| DataFusionError::Internal("dynamic source key index lock poisoned".into()))?;
    let converter = key_row_converter(schema, &key_state.key_indices)?;
    let mut visible = Vec::with_capacity(snapshot.len());
    for entry in snapshot {
        if entry.batch.num_rows() == 0 {
            continue;
        }
        let rows = converter
            .convert_columns(&project_columns(&entry.batch, &key_state.key_indices))
            .map_err(to_datafusion_error)?;
        let mut keep = BooleanBuilder::with_capacity(entry.batch.num_rows());
        let mut kept_rows = 0usize;
        for row_idx in 0..entry.batch.num_rows() {
            let keep_row = key_state
                .latest_generation_by_key
                .get(rows.row(row_idx).data())
                .and_then(|generation| *generation)
                == Some(entry.generation);
            if keep_row {
                kept_rows = kept_rows.saturating_add(1);
            }
            keep.append_value(keep_row);
        }
        if kept_rows == entry.batch.num_rows() {
            visible.push(entry.batch.clone());
        } else if kept_rows > 0 {
            visible.push(filter_record_batch(&entry.batch, &keep.finish())?);
        }
    }
    Ok(visible)
}

fn rebuild_key_state(
    schema: &SchemaRef,
    batches: &[DynamicStateBatch],
    key_state: &Arc<Mutex<DynamicKeyState>>,
) -> Result<()> {
    let mut key_state = key_state
        .lock()
        .map_err(|_| anyhow::anyhow!("dynamic source key index lock poisoned"))?;
    key_state.latest_generation_by_key.clear();
    let converter =
        key_row_converter(schema, &key_state.key_indices).map_err(anyhow::Error::new)?;
    for entry in batches {
        index_batch_keys(
            &converter,
            &key_state.key_indices.clone(),
            entry,
            &mut key_state,
        )?;
    }
    Ok(())
}

fn index_appended_batches(
    schema: &SchemaRef,
    batches: &[DynamicStateBatch],
    key_state: &Arc<Mutex<DynamicKeyState>>,
) -> Result<()> {
    let mut key_state = key_state
        .lock()
        .map_err(|_| anyhow::anyhow!("dynamic source key index lock poisoned"))?;
    let converter =
        key_row_converter(schema, &key_state.key_indices).map_err(anyhow::Error::new)?;
    for entry in batches {
        index_batch_keys(
            &converter,
            &key_state.key_indices.clone(),
            entry,
            &mut key_state,
        )?;
    }
    Ok(())
}

fn index_keyed_delta(
    schema: &SchemaRef,
    touched_keys: &HashSet<Vec<u8>>,
    positive_batches: &[DynamicStateBatch],
    key_state: &Arc<Mutex<DynamicKeyState>>,
) -> Result<()> {
    let mut key_state = key_state
        .lock()
        .map_err(|_| anyhow::anyhow!("dynamic source key index lock poisoned"))?;
    for key in touched_keys {
        key_state.latest_generation_by_key.insert(key.clone(), None);
    }
    let converter =
        key_row_converter(schema, &key_state.key_indices).map_err(anyhow::Error::new)?;
    for entry in positive_batches {
        index_batch_keys(
            &converter,
            &key_state.key_indices.clone(),
            entry,
            &mut key_state,
        )?;
    }
    Ok(())
}

fn index_batch_keys(
    converter: &RowConverter,
    key_indices: &[usize],
    entry: &DynamicStateBatch,
    key_state: &mut DynamicKeyState,
) -> Result<()> {
    let rows = converter
        .convert_columns(&project_columns(&entry.batch, key_indices))
        .context("encode dynamic source keys")?;
    for row_idx in 0..entry.batch.num_rows() {
        key_state
            .latest_generation_by_key
            .insert(rows.row(row_idx).data().to_vec(), Some(entry.generation));
    }
    Ok(())
}

fn key_row_converter(schema: &SchemaRef, key_indices: &[usize]) -> DFResult<RowConverter> {
    let fields = key_indices
        .iter()
        .map(|idx| SortField::new(schema.field(*idx).data_type().clone()))
        .collect::<Vec<_>>();
    RowConverter::new(fields).map_err(to_datafusion_error)
}

fn project_columns(batch: &RecordBatch, indices: &[usize]) -> Vec<ArrayRef> {
    indices
        .iter()
        .map(|idx| Arc::clone(batch.column(*idx)))
        .collect()
}

fn to_datafusion_error(err: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(err), None)
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

        let batches = self.limited_snapshot_batches(partition)?;
        let stream = MemoryStream::try_new(batches, Arc::clone(&self.schema), None)?;
        Ok(Box::pin(stream))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Statistics> {
        if let Some(idx) = partition
            && idx >= self.partition_count
        {
            return internal_err!("Invalid partition index {idx} for DynamicStateExec");
        }

        let stats = self.snapshot_statistics(partition)?;
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
