use std::collections::{HashMap, hash_map::Entry};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, Int64Builder, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::{Column, ScalarValue};
use datafusion::functions_aggregate::count::count_all;
use datafusion::logical_expr::logical_plan::{Aggregate, Distinct, Projection};
use datafusion::logical_expr::{Expr, LogicalPlan, ScalarUDF};
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarZSet};
use dbsp::storage::{KeyValueTable, keyspace};
use slatedb::WriteBatch;

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

pub(super) struct ColumnarGroupedCountPlan {
    source_name: String,
    aggregate: Aggregate,
    aggregate_schema: SchemaRef,
    group_schema: SchemaRef,
    output_mapping: Vec<usize>,
    count_idx: usize,
}

pub(super) struct ColumnarGroupedCountMaterializedViewState {
    source_name: String,
    source_schema: SchemaRef,
    input_zset: SlateBackedColumnarZSet,
    output_zset: SlateBackedColumnarZSet,
    count_state: SlateGroupedCountState,
    aggregate_delta: IncrementalMaterializedViewState,
    aggregate_schema: SchemaRef,
    group_schema: SchemaRef,
    output_mapping: Vec<usize>,
    count_idx: usize,
    initial_snapshot: Vec<RecordBatch>,
}

impl ColumnarGroupedCountMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

struct SlateGroupedCountState {
    table: Arc<dyn KeyValueTable>,
    key_prefix: Vec<u8>,
}

struct PendingGroupDelta {
    delta: i64,
    batch: RecordBatch,
    row_idx: usize,
}

pub(super) fn columnar_grouped_count_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    output_schema: &SchemaRef,
) -> Result<Option<ColumnarGroupedCountPlan>> {
    let Some((aggregate, projection)) = grouped_count_aggregate_for_plan(plan)? else {
        return Ok(None);
    };
    if aggregate.group_expr.is_empty() || aggregate.aggr_expr.len() != 1 {
        return Ok(None);
    }
    if !is_count_star_expr(&aggregate.aggr_expr[0]) {
        return Ok(None);
    }
    if aggregate
        .group_expr
        .iter()
        .any(|expr| matches!(expr, Expr::GroupingSet(_)))
    {
        return Ok(None);
    }

    let count_idx = aggregate.group_expr.len();
    let aggregate_schema = df_schema_to_arrow(&aggregate.schema)?;
    if aggregate_schema.fields().len() != count_idx + 1
        || aggregate_schema.field(count_idx).data_type() != &DataType::Int64
    {
        return Ok(None);
    }
    let output_mapping =
        match output_mapping_for_projection(projection.as_ref(), &aggregate, output_schema) {
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
    let group_fields = aggregate_schema
        .fields()
        .iter()
        .take(count_idx)
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let group_schema = Arc::new(Schema::new(group_fields));

    Ok(Some(ColumnarGroupedCountPlan {
        source_name,
        aggregate: aggregate.clone(),
        aggregate_schema,
        group_schema,
        output_mapping,
        count_idx,
    }))
}

pub(super) async fn build_columnar_grouped_count_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarGroupedCountPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarGroupedCountMaterializedViewState> {
    let source = sources
        .get(&plan.source_name)
        .ok_or_else(|| anyhow::anyhow!("unknown vectorized source '{}'", plan.source_name))?;
    let mv_namespace = namespaces::materialized_view(view_name)?;
    let input_namespace = format!("{mv_namespace}/columnar/grouped_count/input");
    let output_namespace = format!("{mv_namespace}/columnar/grouped_count/output");
    let state_namespace = format!("{mv_namespace}/columnar/grouped_count/state");
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed grouped-count output zset")?;
    let initial_snapshot = snapshot_batches_from_zset(
        &output_zset
            .materialize_columnar()
            .await
            .context("load grouped-count output snapshot")?,
    )?;
    let aggregate_delta = build_incremental_materialized_view_state_from_logical_plan(
        &plan.source_name,
        sources,
        udfs,
        &LogicalPlan::Aggregate(plan.aggregate.clone()),
    )
    .await
    .context("build grouped-count vectorized aggregate delta plan")?;

    Ok(ColumnarGroupedCountMaterializedViewState {
        source_name: plan.source_name,
        source_schema: Arc::clone(&source.schema),
        input_zset: SlateBackedColumnarZSet::new(
            Arc::clone(&table),
            input_namespace,
            Arc::clone(&source.schema),
        )
        .await
        .context("initialize SlateDB-backed grouped-count input zset")?,
        output_zset,
        count_state: SlateGroupedCountState::new(table, &state_namespace),
        aggregate_delta,
        aggregate_schema: plan.aggregate_schema,
        group_schema: plan.group_schema,
        output_mapping: plan.output_mapping,
        count_idx: plan.count_idx,
        initial_snapshot,
    })
}

pub(super) async fn run_columnar_grouped_count_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(columnar) = mv.columnar_grouped_count.as_mut() else {
        return Ok(false);
    };

    let plan_start = Instant::now();
    let input_delta =
        if let Some(weighted_batches) = weighted_delta_batches.get(columnar.source_name.as_str()) {
            ColumnarZSet::try_new_weighted(
                Arc::clone(&columnar.source_schema),
                weighted_batches.clone(),
            )
            .with_context(|| {
                format!(
                    "build weighted grouped-count input delta for '{}'",
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
                    "build insert grouped-count input delta for '{}'",
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
    let pending = grouped_count_pending_delta(columnar, persisted_input_delta.batches()).await?;
    let output_delta_batches = apply_grouped_count_delta(columnar, pending).await?;
    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build grouped-count output zset delta")?;
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
    let next_snapshot = apply_weighted_snapshot_delta(
        &mv.output_schema,
        &mv.previous_snapshot,
        delta_batches.clone(),
    )
    .await
    .with_context(|| {
        format!(
            "apply Slate-backed grouped-count columnar snapshot delta for '{}'",
            mv.view_name
        )
    })?;

    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "columnar_grouped_count",
        "SlateDB-backed grouped-count columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

async fn grouped_count_pending_delta(
    columnar: &ColumnarGroupedCountMaterializedViewState,
    input_batches: &[RecordBatch],
) -> Result<HashMap<Vec<u8>, PendingGroupDelta>> {
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
                    "grouped-count materialized view received non-unit weighted source deltas for '{}'",
                    columnar.source_name
                )
            })?;
        positive_source_batches.extend(unit_delta.positive);
        negative_source_batches.extend(unit_delta.negative);
    }

    let positive_output = collect_incremental_output(
        &columnar.aggregate_delta,
        &positive_source_batches,
        &columnar.aggregate_schema,
    )
    .await?;
    add_aggregate_batches_to_pending(columnar, &positive_output, 1, &mut pending)?;
    let negative_output = collect_incremental_output(
        &columnar.aggregate_delta,
        &negative_source_batches,
        &columnar.aggregate_schema,
    )
    .await?;
    add_aggregate_batches_to_pending(columnar, &negative_output, -1, &mut pending)?;
    pending.retain(|_, delta| delta.delta != 0);
    Ok(pending)
}

fn add_aggregate_batches_to_pending(
    columnar: &ColumnarGroupedCountMaterializedViewState,
    batches: &[RecordBatch],
    sign: i64,
    pending: &mut HashMap<Vec<u8>, PendingGroupDelta>,
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    let converter = row_converter_for_schema(&columnar.group_schema)?;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let group_columns = (0..columnar.count_idx)
            .map(|idx| Arc::clone(batch.column(idx)))
            .collect::<Vec<ArrayRef>>();
        let group_rows = converter
            .convert_columns(&group_columns)
            .context("encode grouped-count aggregate group keys")?;
        let counts = batch
            .column(columnar.count_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("grouped-count aggregate count must be Int64"))?;
        for row_idx in 0..batch.num_rows() {
            if counts.is_null(row_idx) {
                bail!("grouped-count aggregate count cannot be NULL");
            }
            let count = counts.value(row_idx);
            if count == 0 {
                continue;
            }
            let delta = count
                .checked_mul(sign)
                .ok_or_else(|| anyhow::anyhow!("grouped-count delta overflow"))?;
            let key = group_rows.row(row_idx).data().to_vec();
            match pending.entry(key) {
                Entry::Occupied(mut entry) => {
                    let current = entry.get().delta;
                    entry.get_mut().delta = current
                        .checked_add(delta)
                        .ok_or_else(|| anyhow::anyhow!("grouped-count pending delta overflow"))?;
                }
                Entry::Vacant(entry) => {
                    entry.insert(PendingGroupDelta {
                        delta,
                        batch: batch.clone(),
                        row_idx,
                    });
                }
            }
        }
    }
    Ok(())
}

async fn apply_grouped_count_delta(
    columnar: &ColumnarGroupedCountMaterializedViewState,
    pending: HashMap<Vec<u8>, PendingGroupDelta>,
) -> Result<Vec<RecordBatch>> {
    let mut builder = WeightedOutputBuilder::new(
        columnar.output_zset.value_schema(),
        &columnar.output_mapping,
    )?;
    if pending.is_empty() {
        return builder.finish();
    }

    let mut writes = WriteBatch::new();
    let mut wrote_state = false;
    let output_includes_count = columnar.output_mapping.contains(&columnar.count_idx);
    for (group_key, delta) in pending {
        let old_count = columnar.count_state.load_count(&group_key).await?;
        let new_count = old_count
            .checked_add(delta.delta)
            .ok_or_else(|| anyhow::anyhow!("grouped-count state overflow"))?;
        if new_count < 0 {
            bail!("grouped-count state removed more rows than were present");
        }
        if output_includes_count && old_count > 0 {
            builder.append(
                &delta.batch,
                delta.row_idx,
                columnar.count_idx,
                old_count,
                -1,
            )?;
        }
        if output_includes_count && new_count > 0 {
            builder.append(
                &delta.batch,
                delta.row_idx,
                columnar.count_idx,
                new_count,
                1,
            )?;
        }
        if !output_includes_count && old_count > 0 && new_count == 0 {
            builder.append(
                &delta.batch,
                delta.row_idx,
                columnar.count_idx,
                old_count,
                -1,
            )?;
        }
        if !output_includes_count && old_count == 0 && new_count > 0 {
            builder.append(
                &delta.batch,
                delta.row_idx,
                columnar.count_idx,
                new_count,
                1,
            )?;
        }
        columnar
            .count_state
            .write_count(&mut writes, &group_key, new_count);
        wrote_state = true;
    }
    if wrote_state {
        columnar
            .count_state
            .table
            .write_batch(writes)
            .await
            .context("persist grouped-count state updates")?;
    }
    builder.finish()
}

impl SlateGroupedCountState {
    fn new(table: Arc<dyn KeyValueTable>, namespace: &str) -> Self {
        Self {
            table,
            key_prefix: keyspace::namespace_prefix(keyspace::prefix::INDEX, namespace),
        }
    }

    async fn load_count(&self, group_key: &[u8]) -> Result<i64> {
        let Some(bytes) = self
            .table
            .get_bytes(&self.state_key(group_key))
            .await
            .context("read grouped-count state")?
        else {
            return Ok(0);
        };
        decode_i64(bytes.as_ref())
    }

    fn write_count(&self, batch: &mut WriteBatch, group_key: &[u8], count: i64) {
        let key = self.state_key(group_key);
        if count == 0 {
            batch.delete(key);
        } else {
            batch.put(key, count.to_be_bytes());
        }
    }

    fn state_key(&self, group_key: &[u8]) -> Vec<u8> {
        let mut key = self.key_prefix.clone();
        key.extend_from_slice(group_key);
        key
    }
}

struct WeightedOutputBuilder {
    weighted_schema: SchemaRef,
    output_mapping: Vec<usize>,
    builders: Vec<ScalarColumnBuilder>,
    weights: Int64Builder,
    rows: usize,
}

impl WeightedOutputBuilder {
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
        aggregate_batch: &RecordBatch,
        row_idx: usize,
        count_idx: usize,
        count: i64,
        weight: i64,
    ) -> Result<()> {
        for (output_idx, source_idx) in self.output_mapping.iter().copied().enumerate() {
            if source_idx == count_idx {
                self.builders[output_idx].append_i64_value(count)?;
            } else {
                self.builders[output_idx]
                    .append_array_value(aggregate_batch.column(source_idx).as_ref(), row_idx)?;
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

fn grouped_count_aggregate_for_plan(
    plan: &LogicalPlan,
) -> Result<Option<(Aggregate, Option<Projection>)>> {
    match plan {
        LogicalPlan::Aggregate(aggregate) => grouped_count_aggregate_for_aggregate(aggregate)
            .map(|aggregate| aggregate.map(|aggregate| (aggregate, None))),
        LogicalPlan::Projection(projection) => match projection.input.as_ref() {
            LogicalPlan::Aggregate(aggregate) => grouped_count_aggregate_for_aggregate(aggregate)
                .map(|aggregate| aggregate.map(|aggregate| (aggregate, Some(projection.clone())))),
            _ => Ok(None),
        },
        LogicalPlan::SubqueryAlias(alias) => grouped_count_aggregate_for_plan(alias.input.as_ref()),
        LogicalPlan::Distinct(Distinct::All(input)) => {
            distinct_count_aggregate_for_input(input.as_ref())
                .map(|aggregate| aggregate.map(|aggregate| (aggregate, None)))
        }
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            grouped_count_aggregate_for_plan(sort.input.as_ref())
        }
        _ => Ok(None),
    }
}

fn grouped_count_aggregate_for_aggregate(aggregate: &Aggregate) -> Result<Option<Aggregate>> {
    match aggregate.aggr_expr.len() {
        0 => distinct_count_aggregate_for_input_with_groups(
            aggregate.input.as_ref(),
            aggregate.group_expr.clone(),
        )
        .map(Some),
        1 if is_count_star_expr(&aggregate.aggr_expr[0]) => Ok(Some(aggregate.clone())),
        _ => Ok(None),
    }
}

fn distinct_count_aggregate_for_input(input: &LogicalPlan) -> Result<Option<Aggregate>> {
    let group_expr = (0..input.schema().fields().len())
        .map(|idx| {
            let (qualifier, field) = input.schema().qualified_field(idx);
            Expr::Column(Column::new(qualifier.cloned(), field.name()))
        })
        .collect::<Vec<_>>();
    if group_expr.is_empty() {
        return Ok(None);
    }
    distinct_count_aggregate_for_input_with_groups(input, group_expr).map(Some)
}

fn distinct_count_aggregate_for_input_with_groups(
    input: &LogicalPlan,
    group_expr: Vec<Expr>,
) -> Result<Aggregate> {
    Aggregate::try_new(Arc::new(input.clone()), group_expr, vec![count_all()])
        .context("build hidden grouped-count aggregate for distinct rows")
}

fn output_mapping_for_projection(
    projection: Option<&Projection>,
    aggregate: &Aggregate,
    output_schema: &SchemaRef,
) -> Option<Vec<usize>> {
    let aggregate_schema = &aggregate.schema;
    let count_idx = aggregate.group_expr.len();
    match projection {
        Some(projection) => {
            if projection.expr.len() != output_schema.fields().len() {
                return None;
            }
            projection
                .expr
                .iter()
                .map(|expr| output_expr_source_idx(strip_alias(expr), aggregate_schema, count_idx))
                .collect()
        }
        None => {
            if aggregate_schema.fields().len() == output_schema.fields().len() {
                return Some((0..aggregate_schema.fields().len()).collect());
            }
            if count_idx == output_schema.fields().len()
                && output_schema
                    .fields()
                    .iter()
                    .zip(aggregate_schema.fields().iter().take(count_idx))
                    .all(|(output_field, aggregate_field)| {
                        output_field.data_type() == aggregate_field.data_type()
                    })
            {
                return Some((0..count_idx).collect());
            }
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
    count_idx: usize,
) -> Option<usize> {
    if is_count_star_expr(expr) {
        return Some(count_idx);
    }
    let Expr::Column(column) = expr else {
        return None;
    };
    aggregate_schema
        .fields()
        .iter()
        .position(|field| field.name() == &column.name)
}

fn is_count_star_expr(expr: &Expr) -> bool {
    let Expr::AggregateFunction(aggregate) = strip_alias(expr) else {
        return false;
    };
    let params = &aggregate.params;
    aggregate.func.name().eq_ignore_ascii_case("count")
        && !params.distinct
        && params.filter.is_none()
        && params.order_by.is_empty()
        && params.null_treatment.is_none()
        && matches!(
            params.args.as_slice(),
            [Expr::Literal(ScalarValue::Int64(Some(1)), _)]
        )
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
    RowConverter::new(fields).context("build grouped-count Arrow row converter")
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

fn decode_i64(bytes: &[u8]) -> Result<i64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-count state value must be 8 bytes"))?;
    Ok(i64::from_be_bytes(bytes))
}
