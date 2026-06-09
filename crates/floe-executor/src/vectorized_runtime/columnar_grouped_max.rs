use std::collections::{BTreeSet, HashMap, hash_map::Entry};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, Int64Builder, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::logical_expr::logical_plan::{Aggregate, Projection};
use datafusion::logical_expr::{Expr, LogicalPlan, ScalarUDF};
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarZSet};
use dbsp::storage::{KeyValueTable, keyspace};
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

use crate::delta_consolidation::weighted_snapshot_schema;
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::scalar_array_builder::ScalarColumnBuilder;
use crate::vectorized_runtime::source_state::incremental_source_for_plan;
use crate::vectorized_source_delta::unit_source_delta_batches;

use super::{
    IncrementalMaterializedViewState, VectorizedMaterializedViewState, VectorizedSourceState,
    apply_weighted_snapshot_delta, build_incremental_materialized_view_state_from_logical_plan,
    collect_incremental_output,
};

const SUMMARY_TAG: u8 = b's';
const VALUE_TAG: u8 = b'v';

pub(super) struct ColumnarGroupedMaxPlan {
    source_name: String,
    projection: Projection,
    projection_schema: SchemaRef,
    group_schema: SchemaRef,
    output_mapping: Vec<usize>,
    max_idx: usize,
}

impl ColumnarGroupedMaxPlan {
    pub(super) fn source_names(&self) -> BTreeSet<String> {
        [self.source_name.clone()].into_iter().collect()
    }
}

pub(super) struct ColumnarGroupedMaxMaterializedViewState {
    source_name: String,
    source_schema: SchemaRef,
    input_zset: SlateBackedColumnarZSet,
    output_zset: SlateBackedColumnarZSet,
    max_state: SlateGroupedMaxState,
    projection_delta: IncrementalMaterializedViewState,
    projection_schema: SchemaRef,
    group_schema: SchemaRef,
    output_mapping: Vec<usize>,
    max_idx: usize,
    initial_snapshot: Vec<RecordBatch>,
}

impl ColumnarGroupedMaxMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

struct SlateGroupedMaxState {
    table: Arc<dyn KeyValueTable>,
    key_prefix: Vec<u8>,
}

struct PendingMaxGroupDelta {
    value_deltas: HashMap<i64, i64>,
    batch: RecordBatch,
    row_idx: usize,
}

pub(super) struct ColumnarGroupedMaxTick {
    pub(super) delta: ColumnarZSet,
    pub(super) next_snapshot: Vec<RecordBatch>,
    pub(super) input_changed: bool,
}

pub(super) fn columnar_grouped_max_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    output_schema: &SchemaRef,
) -> Result<Option<ColumnarGroupedMaxPlan>> {
    let Some((aggregate, projection)) = grouped_max_aggregate_for_plan(plan) else {
        return Ok(None);
    };
    if aggregate.group_expr.is_empty() || aggregate.aggr_expr.len() != 1 {
        return Ok(None);
    }
    if aggregate
        .group_expr
        .iter()
        .any(|expr| matches!(expr, Expr::GroupingSet(_)))
    {
        return Ok(None);
    }
    let Some(max_value_expr) = max_value_expr(&aggregate.aggr_expr[0]) else {
        return Ok(None);
    };

    let max_idx = aggregate.group_expr.len();
    let aggregate_schema = df_schema_to_arrow(&aggregate.schema)?;
    if aggregate_schema.fields().len() != max_idx + 1
        || aggregate_schema.field(max_idx).data_type() != &DataType::Int64
    {
        return Ok(None);
    }
    let output_mapping = match output_mapping_for_projection(projection, aggregate, output_schema) {
        Some(mapping) => mapping,
        None => return Ok(None),
    };
    if output_mapping
        .iter()
        .any(|idx| *idx >= aggregate_schema.fields().len())
    {
        return Ok(None);
    }
    for (output_field, source_idx) in output_schema.fields().iter().zip(output_mapping.iter()) {
        if output_field.data_type() != aggregate_schema.field(*source_idx).data_type() {
            return Ok(None);
        }
    }

    let Some(source_name) = incremental_source_for_plan(aggregate.input.as_ref(), sources) else {
        return Ok(None);
    };
    let mut projection_expr = aggregate.group_expr.clone();
    projection_expr.push(max_value_expr);
    let value_projection = Projection::try_new(projection_expr, Arc::clone(&aggregate.input))
        .context("build grouped-max value projection")?;
    let projection_schema = df_schema_to_arrow(&value_projection.schema)?;
    if projection_schema.fields().len() != max_idx + 1
        || projection_schema.field(max_idx).data_type() != &DataType::Int64
    {
        return Ok(None);
    }
    let group_fields = projection_schema
        .fields()
        .iter()
        .take(max_idx)
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let group_schema = Arc::new(Schema::new(group_fields));

    Ok(Some(ColumnarGroupedMaxPlan {
        source_name,
        projection: value_projection,
        projection_schema,
        group_schema,
        output_mapping,
        max_idx,
    }))
}

pub(super) async fn build_columnar_grouped_max_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarGroupedMaxPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarGroupedMaxMaterializedViewState> {
    let mv_namespace = namespaces::materialized_view(view_name)?;
    build_columnar_grouped_max_materialized_view_state_in_namespace(
        table,
        mv_namespace,
        output_schema,
        plan,
        sources,
        udfs,
    )
    .await
}

pub(super) async fn build_columnar_grouped_max_materialized_view_state_in_namespace(
    table: Arc<dyn KeyValueTable>,
    mv_namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarGroupedMaxPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarGroupedMaxMaterializedViewState> {
    let source = sources
        .get(&plan.source_name)
        .ok_or_else(|| anyhow::anyhow!("unknown vectorized source '{}'", plan.source_name))?;
    let input_namespace = format!("{mv_namespace}/columnar/grouped_max/input");
    let output_namespace = format!("{mv_namespace}/columnar/grouped_max/output");
    let state_namespace = format!("{mv_namespace}/columnar/grouped_max/state");
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed grouped-max output zset")?;
    let initial_snapshot = snapshot_batches_from_zset(
        &output_zset
            .materialize_columnar()
            .await
            .context("load grouped-max output snapshot")?,
    )?;
    let projection_delta = build_incremental_materialized_view_state_from_logical_plan(
        &plan.source_name,
        sources,
        udfs,
        &LogicalPlan::Projection(plan.projection.clone()),
    )
    .await
    .context("build grouped-max vectorized value projection delta plan")?;

    Ok(ColumnarGroupedMaxMaterializedViewState {
        source_name: plan.source_name,
        source_schema: Arc::clone(&source.schema),
        input_zset: SlateBackedColumnarZSet::new(
            Arc::clone(&table),
            input_namespace,
            Arc::clone(&source.schema),
        )
        .await
        .context("initialize SlateDB-backed grouped-max input zset")?,
        output_zset,
        max_state: SlateGroupedMaxState::new(table, &state_namespace),
        projection_delta,
        projection_schema: plan.projection_schema,
        group_schema: plan.group_schema,
        output_mapping: plan.output_mapping,
        max_idx: plan.max_idx,
        initial_snapshot,
    })
}

pub(super) async fn run_columnar_grouped_max_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(columnar) = mv.columnar_grouped_max.as_mut() else {
        return Ok(false);
    };

    let plan_start = Instant::now();
    let tick = run_columnar_grouped_max_state_tick(
        columnar,
        insert_batches,
        weighted_delta_batches,
        &mv.output_schema,
        &mv.previous_snapshot,
    )
    .await
    .with_context(|| {
        format!(
            "evaluate Slate-backed grouped-max columnar snapshot delta for '{}'",
            mv.view_name
        )
    })?;

    let delta_batches = tick.delta.batches().to_vec();
    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, tick.next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = tick.next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "columnar_grouped_max",
        "SlateDB-backed grouped-max columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

pub(super) async fn run_columnar_grouped_max_state_tick(
    columnar: &mut ColumnarGroupedMaxMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    output_schema: &SchemaRef,
    previous_snapshot: &[RecordBatch],
) -> Result<ColumnarGroupedMaxTick> {
    let input_delta =
        if let Some(weighted_batches) = weighted_delta_batches.get(columnar.source_name.as_str()) {
            ColumnarZSet::try_new_weighted(
                Arc::clone(&columnar.source_schema),
                weighted_batches.clone(),
            )
            .with_context(|| {
                format!(
                    "build weighted grouped-max input delta for '{}'",
                    columnar.source_name
                )
            })?
        } else if let Some(source_batches) = insert_batches.get(columnar.source_name.as_str()) {
            ColumnarZSet::from_value_batches(
                Arc::clone(&columnar.source_schema),
                source_batches.clone(),
                1,
            )
            .with_context(|| {
                format!(
                    "build insert grouped-max input delta for '{}'",
                    columnar.source_name
                )
            })?
        } else {
            ColumnarZSet::empty(Arc::clone(&columnar.source_schema))?
        };

    let persisted_input_delta = if let Some(handle) = columnar
        .input_zset
        .create_version(&input_delta, None)
        .await?
    {
        columnar.input_zset.read_delta(&handle).await?
    } else {
        input_delta
    };
    let input_changed = !persisted_input_delta.batches().is_empty();
    let pending = grouped_max_pending_delta(columnar, persisted_input_delta.batches()).await?;
    let output_delta_batches = apply_grouped_max_delta(columnar, pending).await?;
    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build grouped-max output zset delta")?;
    let persisted_output_delta = if let Some(handle) = columnar
        .output_zset
        .create_version(
            &output_delta,
            columnar
                .output_zset
                .current_handle()
                .map(|handle| handle.version),
        )
        .await?
    {
        columnar.output_zset.read_delta(&handle).await?
    } else {
        output_delta
    };

    let delta_batches = persisted_output_delta.batches().to_vec();
    let next_snapshot =
        apply_weighted_snapshot_delta(output_schema, previous_snapshot, delta_batches.clone())
            .await
            .context("apply Slate-backed grouped-max columnar snapshot delta")?;

    Ok(ColumnarGroupedMaxTick {
        delta: persisted_output_delta,
        next_snapshot,
        input_changed,
    })
}

async fn grouped_max_pending_delta(
    columnar: &ColumnarGroupedMaxMaterializedViewState,
    input_batches: &[RecordBatch],
) -> Result<HashMap<Vec<u8>, PendingMaxGroupDelta>> {
    let mut pending = HashMap::new();
    if input_batches.is_empty() {
        return Ok(pending);
    }

    let mut positive_source_batches = Vec::new();
    let mut negative_source_batches = Vec::new();
    for batch in input_batches {
        let unit_delta =
            unit_source_delta_batches(&columnar.source_schema, batch)?.with_context(|| {
                format!(
                    "grouped-max materialized view received non-unit weighted source deltas for '{}'",
                    columnar.source_name
                )
            })?;
        positive_source_batches.extend(unit_delta.positive);
        negative_source_batches.extend(unit_delta.negative);
    }

    let positive_output = collect_incremental_output(
        &columnar.projection_delta,
        &positive_source_batches,
        &columnar.projection_schema,
    )
    .await?;
    add_projected_value_batches_to_pending(columnar, &positive_output, 1, &mut pending)?;
    let negative_output = collect_incremental_output(
        &columnar.projection_delta,
        &negative_source_batches,
        &columnar.projection_schema,
    )
    .await?;
    add_projected_value_batches_to_pending(columnar, &negative_output, -1, &mut pending)?;
    pending.retain(|_, delta| !delta.value_deltas.is_empty());
    Ok(pending)
}

fn add_projected_value_batches_to_pending(
    columnar: &ColumnarGroupedMaxMaterializedViewState,
    batches: &[RecordBatch],
    sign: i64,
    pending: &mut HashMap<Vec<u8>, PendingMaxGroupDelta>,
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    let converter = row_converter_for_schema(&columnar.group_schema)?;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let group_columns = (0..columnar.max_idx)
            .map(|idx| Arc::clone(batch.column(idx)))
            .collect::<Vec<ArrayRef>>();
        let group_rows = converter
            .convert_columns(&group_columns)
            .context("encode grouped-max group keys")?;
        let values = batch
            .column(columnar.max_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("grouped-max value must be Int64"))?;
        for row_idx in 0..batch.num_rows() {
            if values.is_null(row_idx) {
                continue;
            }
            let value = values.value(row_idx);
            let key = group_rows.row(row_idx).data().to_vec();
            let group = pending.entry(key).or_insert_with(|| PendingMaxGroupDelta {
                value_deltas: HashMap::new(),
                batch: batch.clone(),
                row_idx,
            });
            match group.value_deltas.entry(value) {
                Entry::Occupied(mut entry) => {
                    let next = entry
                        .get()
                        .checked_add(sign)
                        .ok_or_else(|| anyhow::anyhow!("grouped-max value delta overflow"))?;
                    if next == 0 {
                        entry.remove();
                    } else {
                        *entry.get_mut() = next;
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(sign);
                }
            }
        }
    }
    Ok(())
}

async fn apply_grouped_max_delta(
    columnar: &ColumnarGroupedMaxMaterializedViewState,
    pending: HashMap<Vec<u8>, PendingMaxGroupDelta>,
) -> Result<Vec<RecordBatch>> {
    let mut builder = WeightedMaxOutputBuilder::new(
        columnar.output_zset.value_schema(),
        &columnar.output_mapping,
    )?;
    if pending.is_empty() {
        return builder.finish();
    }

    let mut writes = WriteBatch::new();
    for (group_key, delta) in pending {
        let old_max = columnar.max_state.load_max(&group_key).await?;
        let mut updated_counts = HashMap::new();
        for (value, value_delta) in &delta.value_deltas {
            let old_count = columnar
                .max_state
                .load_value_count(&group_key, *value)
                .await?;
            let new_count = old_count
                .checked_add(*value_delta)
                .ok_or_else(|| anyhow::anyhow!("grouped-max value count overflow"))?;
            if new_count < 0 {
                bail!("grouped-max state removed more values than were present");
            }
            updated_counts.insert(*value, new_count);
            columnar
                .max_state
                .write_value_count(&mut writes, &group_key, *value, new_count)?;
        }
        let new_max = columnar
            .max_state
            .new_max_after_delta(&group_key, old_max, &updated_counts)
            .await?;
        if old_max != new_max {
            if let Some(old_max) = old_max {
                builder.append(&delta.batch, delta.row_idx, columnar.max_idx, old_max, -1)?;
            }
            if let Some(new_max) = new_max {
                builder.append(&delta.batch, delta.row_idx, columnar.max_idx, new_max, 1)?;
            }
        }
        columnar
            .max_state
            .write_max(&mut writes, &group_key, new_max)?;
    }
    columnar
        .max_state
        .table
        .write_batch(writes)
        .await
        .context("persist grouped-max state updates")?;
    builder.finish()
}

impl SlateGroupedMaxState {
    fn new(table: Arc<dyn KeyValueTable>, namespace: &str) -> Self {
        Self {
            table,
            key_prefix: keyspace::namespace_prefix(keyspace::prefix::INDEX, namespace),
        }
    }

    async fn load_max(&self, group_key: &[u8]) -> Result<Option<i64>> {
        let Some(bytes) = self
            .table
            .get_bytes(&self.group_key(SUMMARY_TAG, group_key)?)
            .await
            .context("read grouped-max summary state")?
        else {
            return Ok(None);
        };
        Ok(Some(decode_i64(bytes.as_ref())?))
    }

    async fn load_value_count(&self, group_key: &[u8], value: i64) -> Result<i64> {
        let Some(bytes) = self
            .table
            .get_bytes(&self.value_key(group_key, value)?)
            .await
            .context("read grouped-max value count state")?
        else {
            return Ok(0);
        };
        decode_i64(bytes.as_ref())
    }

    async fn new_max_after_delta(
        &self,
        group_key: &[u8],
        old_max: Option<i64>,
        updated_counts: &HashMap<i64, i64>,
    ) -> Result<Option<i64>> {
        let mut max_added = None;
        for (value, count) in updated_counts {
            if *count > 0 {
                max_added = Some(max_added.map_or(*value, |current: i64| current.max(*value)));
            }
        }

        match old_max {
            None => Ok(max_added),
            Some(old_max) => {
                let old_max_still_present = match updated_counts.get(&old_max) {
                    Some(count) => *count > 0,
                    None => true,
                };
                if old_max_still_present {
                    return Ok(Some(max_added.map_or(old_max, |value| value.max(old_max))));
                }
                self.scan_max_with_overlay(group_key, updated_counts).await
            }
        }
    }

    async fn scan_max_with_overlay(
        &self,
        group_key: &[u8],
        updated_counts: &HashMap<i64, i64>,
    ) -> Result<Option<i64>> {
        let value_prefix = self.group_key(VALUE_TAG, group_key)?;
        let mut max = None;
        for (key, value_bytes) in self
            .table
            .scan_prefix(&value_prefix, &ScanOptions::default())
            .await
            .context("scan grouped-max value count state")?
        {
            let value = decode_i64_sortable(
                key.get(value_prefix.len()..)
                    .ok_or_else(|| anyhow::anyhow!("invalid grouped-max value key"))?,
            )?;
            let old_count = decode_i64(&value_bytes)?;
            let count = updated_counts.get(&value).copied().unwrap_or(old_count);
            if count > 0 {
                max = Some(max.map_or(value, |current: i64| current.max(value)));
            }
        }
        for (value, count) in updated_counts {
            if *count > 0 {
                max = Some(max.map_or(*value, |current: i64| current.max(*value)));
            }
        }
        Ok(max)
    }

    fn write_value_count(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        value: i64,
        count: i64,
    ) -> Result<()> {
        let key = self.value_key(group_key, value)?;
        if count == 0 {
            batch.delete(key);
        } else {
            batch.put(key, count.to_be_bytes());
        }
        Ok(())
    }

    fn write_max(&self, batch: &mut WriteBatch, group_key: &[u8], max: Option<i64>) -> Result<()> {
        let key = self.group_key(SUMMARY_TAG, group_key)?;
        if let Some(max) = max {
            batch.put(key, max.to_be_bytes());
        } else {
            batch.delete(key);
        }
        Ok(())
    }

    fn value_key(&self, group_key: &[u8], value: i64) -> Result<Vec<u8>> {
        let mut key = self.group_key(VALUE_TAG, group_key)?;
        key.extend_from_slice(&encode_i64_sortable(value));
        Ok(key)
    }

    fn group_key(&self, tag: u8, group_key: &[u8]) -> Result<Vec<u8>> {
        let len =
            u32::try_from(group_key.len()).context("grouped-max group key exceeds u32 bytes")?;
        let mut key = self.key_prefix.clone();
        key.push(tag);
        key.extend_from_slice(&len.to_be_bytes());
        key.extend_from_slice(group_key);
        Ok(key)
    }
}

struct WeightedMaxOutputBuilder {
    weighted_schema: SchemaRef,
    output_mapping: Vec<usize>,
    builders: Vec<ScalarColumnBuilder>,
    weights: Int64Builder,
    rows: usize,
}

impl WeightedMaxOutputBuilder {
    fn new(schema: SchemaRef, output_mapping: &[usize]) -> Result<Self> {
        let builders = schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), 1024))
            .collect::<Result<Vec<_>>>()?;
        let weighted_schema = weighted_snapshot_schema(&schema)?;
        Ok(Self {
            weighted_schema,
            output_mapping: output_mapping.to_vec(),
            builders,
            weights: Int64Builder::with_capacity(1024),
            rows: 0,
        })
    }

    fn append(
        &mut self,
        projection_batch: &RecordBatch,
        row_idx: usize,
        max_idx: usize,
        max: i64,
        weight: i64,
    ) -> Result<()> {
        for (output_idx, source_idx) in self.output_mapping.iter().copied().enumerate() {
            if source_idx == max_idx {
                self.builders[output_idx].append_i64_value(max)?;
            } else {
                self.builders[output_idx]
                    .append_array_value(projection_batch.column(source_idx).as_ref(), row_idx)?;
            }
        }
        self.weights.append_value(weight);
        self.rows = self.rows.saturating_add(1);
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<RecordBatch>> {
        if self.rows == 0 {
            return Ok(Vec::new());
        }
        let mut columns = self
            .builders
            .iter_mut()
            .map(ScalarColumnBuilder::finish_array)
            .collect::<Vec<_>>();
        columns.push(Arc::new(self.weights.finish()) as ArrayRef);
        Ok(vec![RecordBatch::try_new(self.weighted_schema, columns)?])
    }
}

fn grouped_max_aggregate_for_plan(plan: &LogicalPlan) -> Option<(&Aggregate, Option<&Projection>)> {
    match plan {
        LogicalPlan::Aggregate(aggregate) => Some((aggregate, None)),
        LogicalPlan::Projection(projection) => match projection.input.as_ref() {
            LogicalPlan::Aggregate(aggregate) => Some((aggregate, Some(projection))),
            _ => None,
        },
        LogicalPlan::SubqueryAlias(alias) => grouped_max_aggregate_for_plan(alias.input.as_ref()),
        _ => None,
    }
}

fn output_mapping_for_projection(
    projection: Option<&Projection>,
    aggregate: &Aggregate,
    output_schema: &SchemaRef,
) -> Option<Vec<usize>> {
    let aggregate_schema = &aggregate.schema;
    let max_idx = aggregate.group_expr.len();
    match projection {
        Some(projection) => {
            if projection.expr.len() != output_schema.fields().len() {
                return None;
            }
            projection
                .expr
                .iter()
                .map(|expr| output_expr_source_idx(strip_alias(expr), aggregate_schema, max_idx))
                .collect()
        }
        None => {
            if aggregate_schema.fields().len() != output_schema.fields().len() {
                return None;
            }
            Some((0..aggregate_schema.fields().len()).collect())
        }
    }
}

fn output_expr_source_idx(
    expr: &Expr,
    aggregate_schema: &datafusion::common::DFSchemaRef,
    max_idx: usize,
) -> Option<usize> {
    if max_value_expr(expr).is_some() {
        return Some(max_idx);
    }
    let Expr::Column(column) = expr else {
        return None;
    };
    aggregate_schema
        .fields()
        .iter()
        .position(|field| field.name() == &column.name)
}

fn max_value_expr(expr: &Expr) -> Option<Expr> {
    let Expr::AggregateFunction(aggregate) = strip_alias(expr) else {
        return None;
    };
    let params = &aggregate.params;
    if !aggregate.func.name().eq_ignore_ascii_case("max")
        || params.distinct
        || params.filter.is_some()
        || !params.order_by.is_empty()
        || params.null_treatment.is_some()
    {
        return None;
    }
    let [expr] = params.args.as_slice() else {
        return None;
    };
    Some(expr.clone())
}

fn strip_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => strip_alias(alias.expr.as_ref()),
        _ => expr,
    }
}

fn df_schema_to_arrow(schema: &datafusion::common::DFSchemaRef) -> Result<SchemaRef> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

fn row_converter_for_schema(schema: &SchemaRef) -> Result<RowConverter> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| SortField::new(field.data_type().clone()))
        .collect::<Vec<_>>();
    RowConverter::new(fields).context("build grouped-max Arrow row converter")
}

fn snapshot_batches_from_zset(zset: &ColumnarZSet) -> Result<Vec<RecordBatch>> {
    let weight_idx = zset.weighted_schema().index_of(WEIGHT_COLUMN_NAME)?;
    let mut batches = zset
        .batches()
        .iter()
        .filter(|batch| batch.num_rows() > 0)
        .map(|batch| -> Result<RecordBatch> {
            let weights = batch
                .column(weight_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("columnar zset weight column must be Int64"))?;
            let mut indices = Vec::new();
            for row_idx in 0..weights.len() {
                if weights.is_null(row_idx) {
                    bail!("materialized columnar zset weight cannot be NULL");
                }
                let weight = weights.value(row_idx);
                if weight < 0 {
                    bail!("materialized columnar zset contains negative weight");
                }
                let row_idx =
                    u32::try_from(row_idx).context("columnar zset batch exceeds u32 rows")?;
                for _ in 0..weight {
                    indices.push(row_idx);
                }
            }
            let indices = UInt32Array::from(indices);
            let columns = batch
                .columns()
                .iter()
                .take(zset.value_column_count())
                .map(|column| take(column.as_ref(), &indices, None))
                .collect::<std::result::Result<Vec<ArrayRef>, _>>()?;
            Ok(RecordBatch::try_new(zset.value_schema(), columns)?)
        })
        .collect::<Result<Vec<_>>>()?;
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(zset.value_schema()));
    }
    Ok(batches)
}

fn encode_i64_sortable(value: i64) -> [u8; 8] {
    ((value as u64) ^ (1 << 63)).to_be_bytes()
}

fn decode_i64_sortable(bytes: &[u8]) -> Result<i64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-max value key suffix must be 8 bytes"))?;
    Ok((u64::from_be_bytes(bytes) ^ (1 << 63)) as i64)
}

fn decode_i64(bytes: &[u8]) -> Result<i64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-max state value must be 8 bytes"))?;
    Ok(i64::from_be_bytes(bytes))
}
