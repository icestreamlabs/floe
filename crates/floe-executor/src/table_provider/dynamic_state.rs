use std::any::Any;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result, bail};
use arc_swap::ArcSwap;
use datafusion::arrow::array::ArrayRef;
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

use super::scan_exec::partition_record_batches;

const DYNAMIC_STATE_SCAN_PARTITIONS: usize = 16;

#[derive(Clone)]
pub(crate) struct DynamicStateTableProvider {
    schema: SchemaRef,
    state: Arc<ArcSwap<DynamicStateSnapshot>>,
    key_indices: Option<Arc<[usize]>>,
    keyed_state: Option<Arc<RwLock<DynamicKeyedState>>>,
    next_generation: Arc<AtomicU64>,
    exec: Arc<DynamicStateExec>,
}

impl DynamicStateTableProvider {
    pub(crate) fn new(schema: SchemaRef) -> Self {
        Self::new_with_optional_key_indices(schema, None)
    }

    pub(crate) fn new_with_key_indices(schema: SchemaRef, key_indices: Vec<usize>) -> Result<Self> {
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
        let state = Arc::new(ArcSwap::from_pointee(DynamicStateSnapshot::default()));
        let key_indices = key_indices.map(Arc::<[usize]>::from);
        let keyed_state = key_indices
            .as_ref()
            .map(|_| Arc::new(RwLock::new(DynamicKeyedState::default())));
        let next_generation = Arc::new(AtomicU64::new(1));
        let exec = Arc::new(DynamicStateExec::new(
            Arc::clone(&schema),
            Arc::clone(&state),
            key_indices.clone(),
            keyed_state.clone(),
        ));
        Self {
            schema,
            state,
            key_indices,
            keyed_state,
            next_generation,
            exec,
        }
    }

    pub(crate) fn set_batches(&self, batches: Vec<RecordBatch>) -> Result<()> {
        let state_batches = self.state_batches(batches)?;
        if self.key_indices.is_some() {
            return self.replace_keyed_state(state_batches);
        }
        self.publish_state(state_batches)
    }

    pub(crate) fn append_batches(&self, batches: Vec<RecordBatch>) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let state_batches = self.state_batches(batches)?;
        if state_batches.is_empty() {
            return Ok(());
        }

        if self.key_indices.is_some() {
            return self.upsert_keyed_state(state_batches);
        }

        let current = self.state.load_full();
        let mut next = current.batches.clone();
        next.extend(state_batches);
        self.publish_state(next)
    }

    pub(crate) fn apply_keyed_delta(
        &self,
        touched_keys: &HashSet<Vec<u8>>,
        positive_batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let Some(key_indices) = self.key_indices.as_deref() else {
            bail!("dynamic state provider is not keyed");
        };
        if touched_keys.is_empty() && positive_batches.is_empty() {
            return Ok(());
        }

        let state_batches = self.state_batches(positive_batches)?;
        let keyed_state = self
            .keyed_state
            .as_ref()
            .context("dynamic state keyed storage is not initialized")?;
        let converter = key_row_converter(&self.schema, key_indices).map_err(anyhow::Error::new)?;
        let mut keyed_state = keyed_state
            .write()
            .map_err(|_| anyhow::anyhow!("dynamic state keyed storage lock poisoned"))?;
        for key in touched_keys {
            keyed_state.rows.remove(key);
        }
        keyed_state.insert_state_batches(&converter, key_indices, state_batches)
    }

    pub(crate) fn snapshot(&self) -> Result<Arc<Vec<RecordBatch>>> {
        if let Some(keyed_state) = self.keyed_state.as_ref() {
            let keyed_state = keyed_state
                .read()
                .map_err(|_| anyhow::anyhow!("dynamic state keyed storage lock poisoned"))?;
            return Ok(Arc::new(effective_snapshot_batches(
                &keyed_state.snapshot(),
            )));
        }
        Ok(Arc::new(effective_snapshot_batches(
            &self.state.load_full(),
        )))
    }

    fn exec(&self) -> Arc<DynamicStateExec> {
        Arc::clone(&self.exec)
    }

    fn state_batches(&self, batches: Vec<RecordBatch>) -> Result<Vec<DynamicStateBatch>> {
        let mut state_batches = Vec::new();
        for batch in batches {
            if batch.schema().as_ref() != self.schema.as_ref() {
                bail!("dynamic state batch schema does not match provider schema");
            }
            if batch.num_rows() == 0 {
                continue;
            }
            state_batches.push(DynamicStateBatch {
                generation: self.next_generation.fetch_add(1, Ordering::Relaxed),
                batch,
            });
        }
        Ok(state_batches)
    }

    fn publish_state(&self, batches: Vec<DynamicStateBatch>) -> Result<()> {
        let snapshot = DynamicStateSnapshot { batches };
        self.state.store(Arc::new(snapshot));
        Ok(())
    }

    fn replace_keyed_state(&self, state_batches: Vec<DynamicStateBatch>) -> Result<()> {
        let key_indices = self
            .key_indices
            .as_deref()
            .context("dynamic state provider is not keyed")?;
        let keyed_state = self
            .keyed_state
            .as_ref()
            .context("dynamic state keyed storage is not initialized")?;
        let converter = key_row_converter(&self.schema, key_indices).map_err(anyhow::Error::new)?;
        let mut keyed_state = keyed_state
            .write()
            .map_err(|_| anyhow::anyhow!("dynamic state keyed storage lock poisoned"))?;
        keyed_state.rows.clear();
        keyed_state.insert_state_batches(&converter, key_indices, state_batches)
    }

    fn upsert_keyed_state(&self, state_batches: Vec<DynamicStateBatch>) -> Result<()> {
        let key_indices = self
            .key_indices
            .as_deref()
            .context("dynamic state provider is not keyed")?;
        let keyed_state = self
            .keyed_state
            .as_ref()
            .context("dynamic state keyed storage is not initialized")?;
        let converter = key_row_converter(&self.schema, key_indices).map_err(anyhow::Error::new)?;
        let mut keyed_state = keyed_state
            .write()
            .map_err(|_| anyhow::anyhow!("dynamic state keyed storage lock poisoned"))?;
        keyed_state.insert_state_batches(&converter, key_indices, state_batches)
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
                self.key_indices.clone(),
                self.keyed_state.clone(),
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
pub(crate) struct DynamicStateExec {
    schema: SchemaRef,
    state: Arc<ArcSwap<DynamicStateSnapshot>>,
    key_indices: Option<Arc<[usize]>>,
    keyed_state: Option<Arc<RwLock<DynamicKeyedState>>>,
    limit: Option<usize>,
    partition_count: usize,
    cache: PlanProperties,
}

#[derive(Debug, Default)]
struct DynamicStateSnapshot {
    batches: Vec<DynamicStateBatch>,
}

#[derive(Debug, Clone)]
struct DynamicStateBatch {
    generation: u64,
    batch: RecordBatch,
}

#[derive(Debug, Default)]
struct DynamicKeyedState {
    rows: BTreeMap<Vec<u8>, DynamicStateBatch>,
}

impl DynamicKeyedState {
    fn snapshot(&self) -> DynamicStateSnapshot {
        DynamicStateSnapshot {
            batches: self.rows.values().cloned().collect(),
        }
    }

    fn insert_state_batches(
        &mut self,
        converter: &RowConverter,
        key_indices: &[usize],
        state_batches: Vec<DynamicStateBatch>,
    ) -> Result<()> {
        for entry in state_batches {
            let rows = converter
                .convert_columns(&project_columns(&entry.batch, key_indices))
                .context("encode dynamic source keys")?;
            for row_idx in 0..entry.batch.num_rows() {
                self.rows.insert(
                    rows.row(row_idx).data().to_vec(),
                    DynamicStateBatch {
                        generation: entry.generation,
                        batch: entry.batch.slice(row_idx, 1),
                    },
                );
            }
        }
        Ok(())
    }
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
        state: Arc<ArcSwap<DynamicStateSnapshot>>,
        key_indices: Option<Arc<[usize]>>,
        keyed_state: Option<Arc<RwLock<DynamicKeyedState>>>,
    ) -> Self {
        Self::new_with_limit(schema, state, key_indices, keyed_state, None)
    }

    fn new_with_limit(
        schema: SchemaRef,
        state: Arc<ArcSwap<DynamicStateSnapshot>>,
        key_indices: Option<Arc<[usize]>>,
        keyed_state: Option<Arc<RwLock<DynamicKeyedState>>>,
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
            key_indices,
            keyed_state,
            limit,
            partition_count,
            cache,
        }
    }

    fn snapshot_batches(&self) -> DFResult<Arc<DynamicStateSnapshot>> {
        if let Some(keyed_state) = self.keyed_state.as_ref() {
            let keyed_state = keyed_state.read().map_err(|_| {
                DataFusionError::Execution("dynamic state keyed storage lock poisoned".to_string())
            })?;
            return Ok(Arc::new(keyed_state.snapshot()));
        }
        Ok(self.state.load_full())
    }

    fn limited_snapshot_batches(&self, partition: usize) -> DFResult<Vec<RecordBatch>> {
        let snapshot = self.snapshot_batches()?;
        let Some(limit) = self.limit else {
            return partition_effective_batches(&snapshot, partition, self.partition_count);
        };
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut batches = Vec::new();
        let mut remaining = limit;
        for batch in effective_snapshot_batches(&snapshot) {
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
        let snapshot = self.snapshot_batches()?;
        let batches = if self.limit.is_none()
            && let Some(partition) = partition
        {
            partition_effective_batches(&snapshot, partition, self.partition_count)?
        } else {
            effective_snapshot_batches(&snapshot)
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
    snapshot: &DynamicStateSnapshot,
    partition: usize,
    partition_count: usize,
) -> DFResult<Vec<RecordBatch>> {
    Ok(partition_record_batches(
        &effective_snapshot_batches(snapshot),
        partition,
        partition_count,
    ))
}

fn effective_snapshot_batches(snapshot: &DynamicStateSnapshot) -> Vec<RecordBatch> {
    snapshot
        .batches
        .iter()
        .map(|entry| entry.batch.clone())
        .collect()
}

#[cfg(test)]
fn state_batch_keys(
    schema: &SchemaRef,
    key_indices: &[usize],
    batches: &[DynamicStateBatch],
) -> Result<HashSet<Vec<u8>>> {
    let converter = key_row_converter(schema, key_indices).map_err(anyhow::Error::new)?;
    let mut keys = HashSet::new();
    for entry in batches {
        collect_batch_keys(&converter, key_indices, entry, &mut keys)?;
    }
    Ok(keys)
}

#[cfg(test)]
fn collect_batch_keys(
    converter: &RowConverter,
    key_indices: &[usize],
    entry: &DynamicStateBatch,
    keys: &mut HashSet<Vec<u8>>,
) -> Result<()> {
    let rows = converter
        .convert_columns(&project_columns(&entry.batch, key_indices))
        .context("encode dynamic source keys")?;
    for row_idx in 0..entry.batch.num_rows() {
        keys.insert(rows.row(row_idx).data().to_vec());
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
                write!(
                    f,
                    "DynamicStateExec: partitions={}, keyed={}",
                    self.partition_count,
                    self.key_indices.is_some()
                )
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]))
    }

    fn batch(schema: SchemaRef, rows: &[(i64, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|(id, _)| *id))),
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|(_, amount)| *amount),
                )),
            ],
        )
        .expect("record batch")
    }

    fn touched_key(schema: &SchemaRef, id: i64) -> HashSet<Vec<u8>> {
        let probe = DynamicStateBatch {
            generation: 0,
            batch: batch(Arc::clone(schema), &[(id, 0)]),
        };
        state_batch_keys(schema, &[0], &[probe]).expect("encode key")
    }

    fn snapshot_rows(provider: &DynamicStateTableProvider) -> Vec<(i64, i64)> {
        let mut rows = provider
            .snapshot()
            .expect("snapshot")
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id column");
                let amounts = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("amount column");
                (0..batch.num_rows())
                    .map(|idx| (ids.value(idx), amounts.value(idx)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    }

    #[test]
    fn keyed_delta_replaces_touched_generations_without_history_growth() {
        let schema = schema();
        let provider =
            DynamicStateTableProvider::new_with_key_indices(Arc::clone(&schema), vec![0])
                .expect("keyed provider");
        provider
            .append_batches(vec![batch(Arc::clone(&schema), &[(1, 10), (2, 20)])])
            .expect("seed rows");

        for amount in [30, 40, 50] {
            provider
                .apply_keyed_delta(
                    &touched_key(&schema, 1),
                    vec![batch(Arc::clone(&schema), &[(1, amount)])],
                )
                .expect("apply keyed update");
            let row_count = provider
                .keyed_state
                .as_ref()
                .expect("keyed state")
                .read()
                .expect("keyed state lock")
                .rows
                .len();
            assert!(row_count <= 2, "keyed updates should not retain stale rows");
        }

        assert_eq!(snapshot_rows(&provider), vec![(1, 50), (2, 20)]);

        provider
            .apply_keyed_delta(&touched_key(&schema, 2), Vec::new())
            .expect("apply keyed delete");
        assert_eq!(snapshot_rows(&provider), vec![(1, 50)]);
        assert_eq!(
            provider
                .keyed_state
                .as_ref()
                .expect("keyed state")
                .read()
                .expect("keyed state lock")
                .rows
                .len(),
            1
        );
    }
}
