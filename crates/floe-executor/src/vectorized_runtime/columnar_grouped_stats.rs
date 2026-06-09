use std::collections::{HashMap, hash_map::Entry};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, Int64Builder, UInt32Array,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::ScalarValue;
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

const GROUP_TAG: u8 = b'g';
const SCALAR_TAG: u8 = b'a';
const MINMAX_TAG: u8 = b'm';
const VALUE_TAG: u8 = b'v';

pub(super) struct ColumnarGroupedStatsPlan {
    source_name: String,
    projection: Projection,
    projection_schema: SchemaRef,
    group_schema: SchemaRef,
    specs: Vec<AggregateSpec>,
    output_mapping: Vec<usize>,
    group_count: usize,
}

pub(super) struct ColumnarGroupedStatsMaterializedViewState {
    source_name: String,
    source_schema: SchemaRef,
    input_zset: SlateBackedColumnarZSet,
    output_zset: SlateBackedColumnarZSet,
    stats_state: SlateGroupedStatsState,
    projection_delta: IncrementalMaterializedViewState,
    projection_schema: SchemaRef,
    group_schema: SchemaRef,
    specs: Vec<AggregateSpec>,
    output_mapping: Vec<usize>,
    group_count: usize,
    initial_snapshot: Vec<RecordBatch>,
}

impl ColumnarGroupedStatsMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

#[derive(Clone)]
struct AggregateSpec {
    kind: AggregateKind,
    value_idx: Option<usize>,
    filter_idx: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregateKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

struct SlateGroupedStatsState {
    table: Arc<dyn KeyValueTable>,
    key_prefix: Vec<u8>,
    assume_empty: bool,
    group_counts: Mutex<HashMap<Vec<u8>, i64>>,
    i64_values: Mutex<HashMap<(Vec<u8>, usize), i64>>,
    pairs: Mutex<HashMap<(Vec<u8>, usize), (i64, i64)>>,
    minmax_values: Mutex<HashMap<(Vec<u8>, usize), Option<i64>>>,
    value_counts: Mutex<HashMap<(Vec<u8>, usize, i64), i64>>,
}

struct PendingStatsGroupDelta {
    row_count_delta: i64,
    agg_deltas: Vec<AggregateDelta>,
    batch: RecordBatch,
    row_idx: usize,
}

#[derive(Clone)]
enum AggregateDelta {
    Count { count_delta: i64 },
    Sum { sum_delta: i64 },
    Avg { sum_delta: i64, count_delta: i64 },
    MinMax { value_deltas: HashMap<i64, i64> },
}

#[derive(Clone, PartialEq)]
enum AggregateValue {
    Int64(i64),
    Float64(f64),
    Null,
}

pub(super) fn columnar_grouped_stats_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    output_schema: &SchemaRef,
) -> Result<Option<ColumnarGroupedStatsPlan>> {
    let Some((aggregate, projection)) = grouped_stats_aggregate_for_plan(plan) else {
        return Ok(None);
    };
    if aggregate.group_expr.is_empty() || aggregate.aggr_expr.is_empty() {
        return Ok(None);
    }
    if aggregate
        .group_expr
        .iter()
        .any(|expr| matches!(expr, Expr::GroupingSet(_)))
    {
        return Ok(None);
    }

    let group_count = aggregate.group_expr.len();
    let aggregate_schema = df_schema_to_arrow(&aggregate.schema)?;
    if aggregate_schema.fields().len() != group_count + aggregate.aggr_expr.len() {
        return Ok(None);
    }
    let Some(source_name) = incremental_source_for_plan(aggregate.input.as_ref(), sources) else {
        return Ok(None);
    };

    let mut projection_expr = aggregate.group_expr.clone();
    let mut specs = Vec::with_capacity(aggregate.aggr_expr.len());
    for (agg_idx, expr) in aggregate.aggr_expr.iter().enumerate() {
        let output_type = aggregate_schema.field(group_count + agg_idx).data_type();
        let Some(spec) = aggregate_spec_for_expr(expr, output_type, &mut projection_expr) else {
            return Ok(None);
        };
        specs.push(spec);
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

    let projection_plan = Projection::try_new(projection_expr, Arc::clone(&aggregate.input))
        .context("build grouped-stats value projection")?;
    let projection_schema = df_schema_to_arrow(&projection_plan.schema)?;
    for spec in &specs {
        if let Some(value_idx) = spec.value_idx
            && projection_schema.field(value_idx).data_type() != &DataType::Int64
        {
            return Ok(None);
        }
        if let Some(filter_idx) = spec.filter_idx
            && projection_schema.field(filter_idx).data_type() != &DataType::Boolean
        {
            return Ok(None);
        }
    }
    let group_fields = projection_schema
        .fields()
        .iter()
        .take(group_count)
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let group_schema = Arc::new(Schema::new(group_fields));

    Ok(Some(ColumnarGroupedStatsPlan {
        source_name,
        projection: projection_plan,
        projection_schema,
        group_schema,
        specs,
        output_mapping,
        group_count,
    }))
}

pub(super) async fn build_columnar_grouped_stats_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarGroupedStatsPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarGroupedStatsMaterializedViewState> {
    let source = sources
        .get(&plan.source_name)
        .ok_or_else(|| anyhow::anyhow!("unknown vectorized source '{}'", plan.source_name))?;
    let mv_namespace = namespaces::materialized_view(view_name)?;
    let input_namespace = format!("{mv_namespace}/columnar/grouped_stats/input");
    let output_namespace = format!("{mv_namespace}/columnar/grouped_stats/output");
    let state_namespace = format!("{mv_namespace}/columnar/grouped_stats/state");
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed grouped-stats output zset")?;
    let initial_snapshot = snapshot_batches_from_zset(
        &output_zset
            .materialize_columnar()
            .await
            .context("load grouped-stats output snapshot")?,
    )?;
    let projection_delta = build_incremental_materialized_view_state_from_logical_plan(
        &plan.source_name,
        sources,
        udfs,
        &LogicalPlan::Projection(plan.projection.clone()),
    )
    .await
    .context("build grouped-stats vectorized projection delta plan")?;

    Ok(ColumnarGroupedStatsMaterializedViewState {
        source_name: plan.source_name,
        source_schema: Arc::clone(&source.schema),
        input_zset: SlateBackedColumnarZSet::new(
            Arc::clone(&table),
            input_namespace,
            Arc::clone(&source.schema),
        )
        .await
        .context("initialize SlateDB-backed grouped-stats input zset")?,
        stats_state: SlateGroupedStatsState::new(
            table,
            &state_namespace,
            output_zset.current_handle().is_none(),
        ),
        output_zset,
        projection_delta,
        projection_schema: plan.projection_schema,
        group_schema: plan.group_schema,
        specs: plan.specs,
        output_mapping: plan.output_mapping,
        group_count: plan.group_count,
        initial_snapshot,
    })
}

pub(super) async fn run_columnar_grouped_stats_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(columnar) = mv.columnar_grouped_stats.as_mut() else {
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
                    "build weighted grouped-stats input delta for '{}'",
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
                    "build insert grouped-stats input delta for '{}'",
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
    let pending = grouped_stats_pending_delta(columnar, persisted_input_delta.batches()).await?;
    let output_delta_batches = apply_grouped_stats_delta(columnar, pending).await?;
    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build grouped-stats output zset delta")?;
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
            "apply Slate-backed grouped-stats columnar snapshot delta for '{}'",
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
        mode = "columnar_grouped_stats",
        "SlateDB-backed grouped-stats columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

async fn grouped_stats_pending_delta(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    input_batches: &[RecordBatch],
) -> Result<HashMap<Vec<u8>, PendingStatsGroupDelta>> {
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
                    "grouped-stats materialized view received non-unit weighted source deltas for '{}'",
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
    add_projected_stats_batches_to_pending(columnar, &positive_output, 1, &mut pending)?;
    let negative_output = collect_incremental_output(
        &columnar.projection_delta,
        &negative_source_batches,
        &columnar.projection_schema,
    )
    .await?;
    add_projected_stats_batches_to_pending(columnar, &negative_output, -1, &mut pending)?;
    pending.retain(|_, delta| {
        delta.row_count_delta != 0 || !aggregate_deltas_empty(&delta.agg_deltas)
    });
    Ok(pending)
}

fn add_projected_stats_batches_to_pending(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    batches: &[RecordBatch],
    sign: i64,
    pending: &mut HashMap<Vec<u8>, PendingStatsGroupDelta>,
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    let converter = row_converter_for_schema(&columnar.group_schema)?;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let group_columns = (0..columnar.group_count)
            .map(|idx| Arc::clone(batch.column(idx)))
            .collect::<Vec<ArrayRef>>();
        let group_rows = converter
            .convert_columns(&group_columns)
            .context("encode grouped-stats group keys")?;
        let value_arrays = projected_value_arrays(batch, &columnar.specs)?;
        let filter_arrays = projected_filter_arrays(batch, &columnar.specs)?;
        for row_idx in 0..batch.num_rows() {
            let key = group_rows.row(row_idx).data().to_vec();
            let group = pending
                .entry(key)
                .or_insert_with(|| PendingStatsGroupDelta {
                    row_count_delta: 0,
                    agg_deltas: columnar
                        .specs
                        .iter()
                        .map(|spec| AggregateDelta::for_kind(spec.kind))
                        .collect(),
                    batch: batch.clone(),
                    row_idx,
                });
            group.row_count_delta = group
                .row_count_delta
                .checked_add(sign)
                .ok_or_else(|| anyhow::anyhow!("grouped-stats row count delta overflow"))?;
            for (agg_idx, spec) in columnar.specs.iter().enumerate() {
                if !filter_allows(&filter_arrays[agg_idx], row_idx) {
                    continue;
                }
                match (&mut group.agg_deltas[agg_idx], spec.kind) {
                    (AggregateDelta::Count { count_delta }, AggregateKind::Count) => {
                        *count_delta = count_delta
                            .checked_add(sign)
                            .ok_or_else(|| anyhow::anyhow!("grouped-stats count delta overflow"))?;
                    }
                    (AggregateDelta::Sum { sum_delta }, AggregateKind::Sum) => {
                        let Some(value) = projected_i64_value(&value_arrays[agg_idx], row_idx)
                        else {
                            continue;
                        };
                        let signed = value
                            .checked_mul(sign)
                            .ok_or_else(|| anyhow::anyhow!("grouped-stats sum delta overflow"))?;
                        *sum_delta = sum_delta
                            .checked_add(signed)
                            .ok_or_else(|| anyhow::anyhow!("grouped-stats sum delta overflow"))?;
                    }
                    (
                        AggregateDelta::Avg {
                            sum_delta,
                            count_delta,
                        },
                        AggregateKind::Avg,
                    ) => {
                        let Some(value) = projected_i64_value(&value_arrays[agg_idx], row_idx)
                        else {
                            continue;
                        };
                        let signed = value.checked_mul(sign).ok_or_else(|| {
                            anyhow::anyhow!("grouped-stats avg sum delta overflow")
                        })?;
                        *sum_delta = sum_delta.checked_add(signed).ok_or_else(|| {
                            anyhow::anyhow!("grouped-stats avg sum delta overflow")
                        })?;
                        *count_delta = count_delta.checked_add(sign).ok_or_else(|| {
                            anyhow::anyhow!("grouped-stats avg count delta overflow")
                        })?;
                    }
                    (
                        AggregateDelta::MinMax { value_deltas },
                        AggregateKind::Min | AggregateKind::Max,
                    ) => {
                        let Some(value) = projected_i64_value(&value_arrays[agg_idx], row_idx)
                        else {
                            continue;
                        };
                        match value_deltas.entry(value) {
                            Entry::Occupied(mut entry) => {
                                let next = entry.get().checked_add(sign).ok_or_else(|| {
                                    anyhow::anyhow!("grouped-stats min/max value delta overflow")
                                })?;
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
                    _ => bail!("grouped-stats aggregate delta kind mismatch"),
                }
            }
        }
    }
    Ok(())
}

async fn apply_grouped_stats_delta(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    pending: HashMap<Vec<u8>, PendingStatsGroupDelta>,
) -> Result<Vec<RecordBatch>> {
    let mut builder = WeightedStatsOutputBuilder::new(
        columnar.output_zset.value_schema(),
        &columnar.output_mapping,
    )?;
    if pending.is_empty() {
        return builder.finish();
    }

    let mut writes = WriteBatch::new();
    for (group_key, delta) in pending {
        let old_row_count = columnar.stats_state.load_group_count(&group_key).await?;
        let old_values = load_aggregate_values(columnar, &group_key).await?;
        let new_row_count = old_row_count
            .checked_add(delta.row_count_delta)
            .ok_or_else(|| anyhow::anyhow!("grouped-stats row count overflow"))?;
        if new_row_count < 0 {
            bail!("grouped-stats state removed more rows than were present");
        }
        let new_values =
            apply_aggregate_deltas(columnar, &group_key, &delta.agg_deltas, &mut writes).await?;
        columnar
            .stats_state
            .write_group_count(&mut writes, &group_key, new_row_count)?;

        if old_row_count > 0 && (new_row_count == 0 || old_values != new_values) {
            builder.append(
                &delta.batch,
                delta.row_idx,
                columnar.group_count,
                &old_values,
                -1,
            )?;
        }
        if new_row_count > 0 && (old_row_count == 0 || old_values != new_values) {
            builder.append(
                &delta.batch,
                delta.row_idx,
                columnar.group_count,
                &new_values,
                1,
            )?;
        }
    }
    columnar
        .stats_state
        .table
        .write_batch(writes)
        .await
        .context("persist grouped-stats state updates")?;
    builder.finish()
}

async fn load_aggregate_values(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    group_key: &[u8],
) -> Result<Vec<AggregateValue>> {
    let mut values = Vec::with_capacity(columnar.specs.len());
    for (idx, spec) in columnar.specs.iter().enumerate() {
        values.push(match spec.kind {
            AggregateKind::Count => {
                AggregateValue::Int64(columnar.stats_state.load_i64(group_key, idx).await?)
            }
            AggregateKind::Sum => {
                AggregateValue::Int64(columnar.stats_state.load_i64(group_key, idx).await?)
            }
            AggregateKind::Avg => {
                let (sum, count) = columnar.stats_state.load_pair(group_key, idx).await?;
                if count == 0 {
                    AggregateValue::Null
                } else {
                    AggregateValue::Float64(sum as f64 / count as f64)
                }
            }
            AggregateKind::Min | AggregateKind::Max => columnar
                .stats_state
                .load_minmax(group_key, idx)
                .await?
                .map(AggregateValue::Int64)
                .unwrap_or(AggregateValue::Null),
        });
    }
    Ok(values)
}

async fn apply_aggregate_deltas(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    group_key: &[u8],
    deltas: &[AggregateDelta],
    writes: &mut WriteBatch,
) -> Result<Vec<AggregateValue>> {
    let mut values = Vec::with_capacity(columnar.specs.len());
    for (idx, (spec, delta)) in columnar.specs.iter().zip(deltas.iter()).enumerate() {
        values.push(match (spec.kind, delta) {
            (AggregateKind::Count, AggregateDelta::Count { count_delta }) => {
                let old = columnar.stats_state.load_i64(group_key, idx).await?;
                let new = old
                    .checked_add(*count_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats count overflow"))?;
                if new < 0 {
                    bail!("grouped-stats count became negative");
                }
                columnar
                    .stats_state
                    .write_i64(writes, group_key, idx, new)?;
                AggregateValue::Int64(new)
            }
            (AggregateKind::Sum, AggregateDelta::Sum { sum_delta }) => {
                let old = columnar.stats_state.load_i64(group_key, idx).await?;
                let new = old
                    .checked_add(*sum_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats sum overflow"))?;
                columnar
                    .stats_state
                    .write_i64(writes, group_key, idx, new)?;
                AggregateValue::Int64(new)
            }
            (
                AggregateKind::Avg,
                AggregateDelta::Avg {
                    sum_delta,
                    count_delta,
                },
            ) => {
                let (old_sum, old_count) = columnar.stats_state.load_pair(group_key, idx).await?;
                let new_sum = old_sum
                    .checked_add(*sum_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats avg sum overflow"))?;
                let new_count = old_count
                    .checked_add(*count_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats avg count overflow"))?;
                if new_count < 0 {
                    bail!("grouped-stats avg count became negative");
                }
                columnar
                    .stats_state
                    .write_pair(writes, group_key, idx, new_sum, new_count)?;
                if new_count == 0 {
                    AggregateValue::Null
                } else {
                    AggregateValue::Float64(new_sum as f64 / new_count as f64)
                }
            }
            (AggregateKind::Min | AggregateKind::Max, AggregateDelta::MinMax { value_deltas }) => {
                let old = columnar.stats_state.load_minmax(group_key, idx).await?;
                let mut updated_counts = HashMap::new();
                for (value, value_delta) in value_deltas {
                    let old_count = columnar
                        .stats_state
                        .load_value_count(group_key, idx, *value)
                        .await?;
                    let new_count = old_count.checked_add(*value_delta).ok_or_else(|| {
                        anyhow::anyhow!("grouped-stats min/max value count overflow")
                    })?;
                    if new_count < 0 {
                        bail!("grouped-stats min/max removed more values than were present");
                    }
                    updated_counts.insert(*value, new_count);
                    columnar
                        .stats_state
                        .write_value_count(writes, group_key, idx, *value, new_count)?;
                }
                let new = columnar
                    .stats_state
                    .new_minmax_after_delta(group_key, idx, spec.kind, old, &updated_counts)
                    .await?;
                columnar
                    .stats_state
                    .write_minmax(writes, group_key, idx, new)?;
                new.map(AggregateValue::Int64)
                    .unwrap_or(AggregateValue::Null)
            }
            _ => bail!("grouped-stats aggregate state kind mismatch"),
        });
    }
    Ok(values)
}

impl AggregateDelta {
    fn for_kind(kind: AggregateKind) -> Self {
        match kind {
            AggregateKind::Count => Self::Count { count_delta: 0 },
            AggregateKind::Sum => Self::Sum { sum_delta: 0 },
            AggregateKind::Avg => Self::Avg {
                sum_delta: 0,
                count_delta: 0,
            },
            AggregateKind::Min | AggregateKind::Max => Self::MinMax {
                value_deltas: HashMap::new(),
            },
        }
    }
}

fn aggregate_deltas_empty(deltas: &[AggregateDelta]) -> bool {
    deltas.iter().all(|delta| match delta {
        AggregateDelta::Count { count_delta } => *count_delta == 0,
        AggregateDelta::Sum { sum_delta } => *sum_delta == 0,
        AggregateDelta::Avg {
            sum_delta,
            count_delta,
        } => *sum_delta == 0 && *count_delta == 0,
        AggregateDelta::MinMax { value_deltas } => value_deltas.is_empty(),
    })
}

fn projected_value_arrays<'a>(
    batch: &'a RecordBatch,
    specs: &[AggregateSpec],
) -> Result<Vec<Option<&'a Int64Array>>> {
    specs
        .iter()
        .map(|spec| {
            spec.value_idx
                .map(|idx| {
                    batch
                        .column(idx)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| anyhow::anyhow!("grouped-stats value must be Int64"))
                })
                .transpose()
        })
        .collect()
}

fn projected_filter_arrays<'a>(
    batch: &'a RecordBatch,
    specs: &[AggregateSpec],
) -> Result<Vec<Option<&'a BooleanArray>>> {
    specs
        .iter()
        .map(|spec| {
            spec.filter_idx
                .map(|idx| {
                    batch
                        .column(idx)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| anyhow::anyhow!("grouped-stats filter must be Boolean"))
                })
                .transpose()
        })
        .collect()
}

fn projected_i64_value(values: &Option<&Int64Array>, row_idx: usize) -> Option<i64> {
    let values = values.as_ref()?;
    (!values.is_null(row_idx)).then(|| values.value(row_idx))
}

fn filter_allows(filter: &Option<&BooleanArray>, row_idx: usize) -> bool {
    match filter {
        Some(filter) => !filter.is_null(row_idx) && filter.value(row_idx),
        None => true,
    }
}

impl SlateGroupedStatsState {
    fn new(table: Arc<dyn KeyValueTable>, namespace: &str, assume_empty: bool) -> Self {
        Self {
            table,
            key_prefix: keyspace::namespace_prefix(keyspace::prefix::INDEX, namespace),
            assume_empty,
            group_counts: Mutex::new(HashMap::new()),
            i64_values: Mutex::new(HashMap::new()),
            pairs: Mutex::new(HashMap::new()),
            minmax_values: Mutex::new(HashMap::new()),
            value_counts: Mutex::new(HashMap::new()),
        }
    }

    async fn load_group_count(&self, group_key: &[u8]) -> Result<i64> {
        let cache_key = group_key.to_vec();
        if let Some(value) = self
            .group_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats group count cache poisoned"))?
            .get(&cache_key)
            .copied()
        {
            return Ok(value);
        }
        if self.assume_empty {
            return Ok(0);
        }
        let value = self
            .load_key_i64(&self.group_key(GROUP_TAG, group_key)?)
            .await?;
        self.group_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats group count cache poisoned"))?
            .insert(cache_key, value);
        Ok(value)
    }

    fn write_group_count(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        count: i64,
    ) -> Result<()> {
        self.write_key_i64(batch, self.group_key(GROUP_TAG, group_key)?, count);
        self.group_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats group count cache poisoned"))?
            .insert(group_key.to_vec(), count);
        Ok(())
    }

    async fn load_i64(&self, group_key: &[u8], agg_idx: usize) -> Result<i64> {
        let cache_key = (group_key.to_vec(), agg_idx);
        if let Some(value) = self
            .i64_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats i64 cache poisoned"))?
            .get(&cache_key)
            .copied()
        {
            return Ok(value);
        }
        if self.assume_empty {
            return Ok(0);
        }
        let value = self
            .load_key_i64(&self.aggregate_key(SCALAR_TAG, group_key, agg_idx)?)
            .await?;
        self.i64_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats i64 cache poisoned"))?
            .insert(cache_key, value);
        Ok(value)
    }

    fn write_i64(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        value: i64,
    ) -> Result<()> {
        self.write_key_i64(
            batch,
            self.aggregate_key(SCALAR_TAG, group_key, agg_idx)?,
            value,
        );
        self.i64_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats i64 cache poisoned"))?
            .insert((group_key.to_vec(), agg_idx), value);
        Ok(())
    }

    async fn load_pair(&self, group_key: &[u8], agg_idx: usize) -> Result<(i64, i64)> {
        let cache_key = (group_key.to_vec(), agg_idx);
        if let Some(value) = self
            .pairs
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats pair cache poisoned"))?
            .get(&cache_key)
            .copied()
        {
            return Ok(value);
        }
        if self.assume_empty {
            return Ok((0, 0));
        }
        let Some(bytes) = self
            .table
            .get_bytes(&self.aggregate_key(SCALAR_TAG, group_key, agg_idx)?)
            .await
            .context("read grouped-stats pair state")?
        else {
            return Ok((0, 0));
        };
        let value = decode_i64_pair(bytes.as_ref())?;
        self.pairs
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats pair cache poisoned"))?
            .insert(cache_key, value);
        Ok(value)
    }

    fn write_pair(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        sum: i64,
        count: i64,
    ) -> Result<()> {
        let key = self.aggregate_key(SCALAR_TAG, group_key, agg_idx)?;
        if sum == 0 && count == 0 {
            batch.delete(key);
        } else {
            let mut bytes = Vec::with_capacity(16);
            bytes.extend_from_slice(&sum.to_be_bytes());
            bytes.extend_from_slice(&count.to_be_bytes());
            batch.put(key, bytes);
        }
        self.pairs
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats pair cache poisoned"))?
            .insert((group_key.to_vec(), agg_idx), (sum, count));
        Ok(())
    }

    async fn load_minmax(&self, group_key: &[u8], agg_idx: usize) -> Result<Option<i64>> {
        let cache_key = (group_key.to_vec(), agg_idx);
        if let Some(value) = self
            .minmax_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats min/max cache poisoned"))?
            .get(&cache_key)
            .copied()
        {
            return Ok(value);
        }
        if self.assume_empty {
            return Ok(None);
        }
        let Some(bytes) = self
            .table
            .get_bytes(&self.aggregate_key(MINMAX_TAG, group_key, agg_idx)?)
            .await
            .context("read grouped-stats min/max state")?
        else {
            return Ok(None);
        };
        let value = Some(decode_i64(bytes.as_ref())?);
        self.minmax_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats min/max cache poisoned"))?
            .insert(cache_key, value);
        Ok(value)
    }

    fn write_minmax(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        value: Option<i64>,
    ) -> Result<()> {
        let key = self.aggregate_key(MINMAX_TAG, group_key, agg_idx)?;
        if let Some(value) = value {
            batch.put(key, value.to_be_bytes());
        } else {
            batch.delete(key);
        }
        self.minmax_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats min/max cache poisoned"))?
            .insert((group_key.to_vec(), agg_idx), value);
        Ok(())
    }

    async fn load_value_count(&self, group_key: &[u8], agg_idx: usize, value: i64) -> Result<i64> {
        let cache_key = (group_key.to_vec(), agg_idx, value);
        if let Some(count) = self
            .value_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats value count cache poisoned"))?
            .get(&cache_key)
            .copied()
        {
            return Ok(count);
        }
        if self.assume_empty {
            return Ok(0);
        }
        let count = self
            .load_key_i64(&self.value_key(group_key, agg_idx, value)?)
            .await?;
        self.value_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats value count cache poisoned"))?
            .insert(cache_key, count);
        Ok(count)
    }

    fn write_value_count(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        value: i64,
        count: i64,
    ) -> Result<()> {
        self.write_key_i64(batch, self.value_key(group_key, agg_idx, value)?, count);
        self.value_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats value count cache poisoned"))?
            .insert((group_key.to_vec(), agg_idx, value), count);
        Ok(())
    }

    async fn new_minmax_after_delta(
        &self,
        group_key: &[u8],
        agg_idx: usize,
        kind: AggregateKind,
        old: Option<i64>,
        updated_counts: &HashMap<i64, i64>,
    ) -> Result<Option<i64>> {
        let mut added = None;
        for (value, count) in updated_counts {
            if *count > 0 {
                added = Some(match added {
                    Some(current) => minmax_value(kind, current, *value),
                    None => *value,
                });
            }
        }
        match old {
            None => Ok(added),
            Some(old) => {
                let old_still_present = match updated_counts.get(&old) {
                    Some(count) => *count > 0,
                    None => true,
                };
                if old_still_present {
                    return Ok(Some(match added {
                        Some(value) => minmax_value(kind, old, value),
                        None => old,
                    }));
                }
                self.scan_minmax_with_overlay(group_key, agg_idx, kind, updated_counts)
                    .await
            }
        }
    }

    async fn scan_minmax_with_overlay(
        &self,
        group_key: &[u8],
        agg_idx: usize,
        kind: AggregateKind,
        updated_counts: &HashMap<i64, i64>,
    ) -> Result<Option<i64>> {
        let value_prefix = self.value_key_prefix(group_key, agg_idx)?;
        let mut out = None;
        for (key, value_bytes) in self
            .table
            .scan_prefix(&value_prefix, &ScanOptions::default())
            .await
            .context("scan grouped-stats min/max value state")?
        {
            let value = decode_i64_sortable(
                key.get(value_prefix.len()..)
                    .ok_or_else(|| anyhow::anyhow!("invalid grouped-stats value key"))?,
            )?;
            let old_count = decode_i64(&value_bytes)?;
            let count = updated_counts.get(&value).copied().unwrap_or(old_count);
            if count > 0 {
                out = Some(match out {
                    Some(current) => minmax_value(kind, current, value),
                    None => value,
                });
            }
        }
        for (value, count) in updated_counts {
            if *count > 0 {
                out = Some(match out {
                    Some(current) => minmax_value(kind, current, *value),
                    None => *value,
                });
            }
        }
        Ok(out)
    }

    async fn load_key_i64(&self, key: &[u8]) -> Result<i64> {
        let Some(bytes) = self
            .table
            .get_bytes(key)
            .await
            .context("read grouped-stats i64 state")?
        else {
            return Ok(0);
        };
        decode_i64(bytes.as_ref())
    }

    fn write_key_i64(&self, batch: &mut WriteBatch, key: Vec<u8>, value: i64) {
        if value == 0 {
            batch.delete(key);
        } else {
            batch.put(key, value.to_be_bytes());
        }
    }

    fn value_key(&self, group_key: &[u8], agg_idx: usize, value: i64) -> Result<Vec<u8>> {
        let mut key = self.value_key_prefix(group_key, agg_idx)?;
        key.extend_from_slice(&encode_i64_sortable(value));
        Ok(key)
    }

    fn value_key_prefix(&self, group_key: &[u8], agg_idx: usize) -> Result<Vec<u8>> {
        self.aggregate_key(VALUE_TAG, group_key, agg_idx)
    }

    fn aggregate_key(&self, tag: u8, group_key: &[u8], agg_idx: usize) -> Result<Vec<u8>> {
        let agg_idx =
            u16::try_from(agg_idx).context("grouped-stats aggregate index exceeds u16")?;
        let mut key = self.group_key(tag, group_key)?;
        key.extend_from_slice(&agg_idx.to_be_bytes());
        Ok(key)
    }

    fn group_key(&self, tag: u8, group_key: &[u8]) -> Result<Vec<u8>> {
        let len =
            u32::try_from(group_key.len()).context("grouped-stats group key exceeds u32 bytes")?;
        let mut key = self.key_prefix.clone();
        key.push(tag);
        key.extend_from_slice(&len.to_be_bytes());
        key.extend_from_slice(group_key);
        Ok(key)
    }
}

struct WeightedStatsOutputBuilder {
    weighted_schema: SchemaRef,
    output_mapping: Vec<usize>,
    builders: Vec<ScalarColumnBuilder>,
    weights: Int64Builder,
    rows: usize,
}

impl WeightedStatsOutputBuilder {
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
        group_count: usize,
        aggregate_values: &[AggregateValue],
        weight: i64,
    ) -> Result<()> {
        for (output_idx, source_idx) in self.output_mapping.iter().copied().enumerate() {
            if source_idx < group_count {
                self.builders[output_idx]
                    .append_array_value(projection_batch.column(source_idx).as_ref(), row_idx)?;
            } else {
                let aggregate_idx = source_idx - group_count;
                match aggregate_values
                    .get(aggregate_idx)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats output mapping out of bounds"))?
                {
                    AggregateValue::Int64(value) => {
                        self.builders[output_idx].append_i64_value(*value)?;
                    }
                    AggregateValue::Float64(value) => {
                        self.builders[output_idx].append_f64_value(*value)?;
                    }
                    AggregateValue::Null => {
                        self.builders[output_idx].append_encoded_scalar(None)?;
                    }
                }
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

fn aggregate_spec_for_expr(
    expr: &Expr,
    output_type: &DataType,
    projection_expr: &mut Vec<Expr>,
) -> Option<AggregateSpec> {
    let Expr::AggregateFunction(aggregate) = strip_alias(expr) else {
        return None;
    };
    let params = &aggregate.params;
    if params.distinct || !params.order_by.is_empty() || params.null_treatment.is_some() {
        return None;
    }
    let filter_idx = params.filter.as_ref().map(|filter| {
        let idx = projection_expr.len();
        projection_expr.push(
            filter
                .as_ref()
                .clone()
                .alias(format!("__floe_grouped_stats_filter_{idx}")),
        );
        idx
    });
    let name = aggregate.func.name();
    if name.eq_ignore_ascii_case("count") {
        if !is_count_star_args(&params.args) || output_type != &DataType::Int64 {
            return None;
        }
        return Some(AggregateSpec {
            kind: AggregateKind::Count,
            value_idx: None,
            filter_idx,
        });
    }
    let [value_expr] = params.args.as_slice() else {
        return None;
    };
    let value_idx = projection_expr.len();
    projection_expr.push(
        value_expr
            .clone()
            .alias(format!("__floe_grouped_stats_value_{value_idx}")),
    );
    let kind = if name.eq_ignore_ascii_case("sum") && output_type == &DataType::Int64 {
        AggregateKind::Sum
    } else if name.eq_ignore_ascii_case("avg") && output_type == &DataType::Float64 {
        AggregateKind::Avg
    } else if name.eq_ignore_ascii_case("min") && output_type == &DataType::Int64 {
        AggregateKind::Min
    } else if name.eq_ignore_ascii_case("max") && output_type == &DataType::Int64 {
        AggregateKind::Max
    } else {
        return None;
    };
    Some(AggregateSpec {
        kind,
        value_idx: Some(value_idx),
        filter_idx,
    })
}

fn grouped_stats_aggregate_for_plan(
    plan: &LogicalPlan,
) -> Option<(&Aggregate, Option<&Projection>)> {
    match plan {
        LogicalPlan::Aggregate(aggregate) => Some((aggregate, None)),
        LogicalPlan::Projection(projection) => match projection.input.as_ref() {
            LogicalPlan::Aggregate(aggregate) => Some((aggregate, Some(projection))),
            _ => None,
        },
        LogicalPlan::SubqueryAlias(alias) => grouped_stats_aggregate_for_plan(alias.input.as_ref()),
        _ => None,
    }
}

fn output_mapping_for_projection(
    projection: Option<&Projection>,
    aggregate: &Aggregate,
    output_schema: &SchemaRef,
) -> Option<Vec<usize>> {
    let aggregate_schema = &aggregate.schema;
    match projection {
        Some(projection) => {
            if projection.expr.len() != output_schema.fields().len() {
                return None;
            }
            projection
                .expr
                .iter()
                .map(|expr| output_expr_source_idx(strip_alias(expr), aggregate_schema))
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
) -> Option<usize> {
    let Expr::Column(column) = expr else {
        let expr_name = strip_alias(expr).schema_name().to_string();
        return aggregate_schema
            .fields()
            .iter()
            .position(|field| field.name() == &expr_name);
    };
    aggregate_schema
        .fields()
        .iter()
        .position(|field| field.name() == &column.name)
}

fn is_count_star_args(args: &[Expr]) -> bool {
    matches!(args, [Expr::Literal(ScalarValue::Int64(Some(1)), _)])
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
    RowConverter::new(fields).context("build grouped-stats Arrow row converter")
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

fn minmax_value(kind: AggregateKind, left: i64, right: i64) -> i64 {
    match kind {
        AggregateKind::Min => left.min(right),
        AggregateKind::Max => left.max(right),
        _ => unreachable!("minmax_value called for non-min/max aggregate"),
    }
}

fn encode_i64_sortable(value: i64) -> [u8; 8] {
    ((value as u64) ^ (1 << 63)).to_be_bytes()
}

fn decode_i64_sortable(bytes: &[u8]) -> Result<i64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-stats value key suffix must be 8 bytes"))?;
    Ok((u64::from_be_bytes(bytes) ^ (1 << 63)) as i64)
}

fn decode_i64(bytes: &[u8]) -> Result<i64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-stats state value must be 8 bytes"))?;
    Ok(i64::from_be_bytes(bytes))
}

fn decode_i64_pair(bytes: &[u8]) -> Result<(i64, i64)> {
    if bytes.len() != 16 {
        bail!("grouped-stats pair state value must be 16 bytes");
    }
    Ok((decode_i64(&bytes[..8])?, decode_i64(&bytes[8..])?))
}
