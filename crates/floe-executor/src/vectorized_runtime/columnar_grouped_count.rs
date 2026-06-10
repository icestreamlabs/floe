use std::collections::{BTreeSet, HashMap, hash_map::Entry};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use ahash::AHashMap;
use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{
    Array, ArrayBuilder, ArrayRef, Int64Array, Int64Builder, UInt32Array,
};
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
    collect_incremental_output, profile,
};

pub(super) struct ColumnarGroupedCountPlan {
    source_name: String,
    aggregate: Aggregate,
    hop_group_projection: Option<Projection>,
    hop_groups: Vec<HopGroup>,
    aggregate_schema: SchemaRef,
    group_schema: SchemaRef,
    hop_group_projection_schema: Option<SchemaRef>,
    output_mapping: Vec<usize>,
    count_idx: usize,
}

impl ColumnarGroupedCountPlan {
    pub(super) fn source_names(&self) -> BTreeSet<String> {
        [self.source_name.clone()].into_iter().collect()
    }
}

pub(super) struct ColumnarGroupedCountMaterializedViewState {
    source_name: String,
    source_schema: SchemaRef,
    input_zset: SlateBackedColumnarZSet,
    output_zset: SlateBackedColumnarZSet,
    count_state: SlateGroupedCountState,
    aggregate_delta: Option<IncrementalMaterializedViewState>,
    hop_group_projection_delta: Option<IncrementalMaterializedViewState>,
    aggregate_schema: SchemaRef,
    group_schema: SchemaRef,
    hop_group_projection_schema: Option<SchemaRef>,
    hop_groups: Vec<HopGroup>,
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
    count_log_prefix: Vec<u8>,
    count_sequence_key: Vec<u8>,
    next_count_segment_id: Mutex<u64>,
    counts: Mutex<AHashMap<Vec<u8>, i64>>,
}

struct PendingGroupDelta {
    delta: i64,
    batch: RecordBatch,
    row_idx: usize,
}

#[derive(Debug, Clone)]
struct HopGroup {
    group_idx: usize,
    slide_ms: i64,
    size_ms: i64,
}

pub(super) struct ColumnarGroupedCountTick {
    pub(super) delta: ColumnarZSet,
    pub(super) next_snapshot: Vec<RecordBatch>,
    pub(super) input_changed: bool,
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
    let (hop_groups, hop_group_projection, hop_group_projection_schema) =
        hop_group_projection_for_aggregate(&aggregate)?;
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
        hop_group_projection,
        hop_groups,
        aggregate_schema,
        group_schema,
        hop_group_projection_schema,
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
    let mv_namespace = namespaces::materialized_view(view_name)?;
    build_columnar_grouped_count_materialized_view_state_in_namespace(
        table,
        mv_namespace,
        output_schema,
        plan,
        sources,
        udfs,
    )
    .await
}

pub(super) async fn build_columnar_grouped_count_materialized_view_state_in_namespace(
    table: Arc<dyn KeyValueTable>,
    mv_namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarGroupedCountPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarGroupedCountMaterializedViewState> {
    let source = sources
        .get(&plan.source_name)
        .ok_or_else(|| anyhow::anyhow!("unknown vectorized source '{}'", plan.source_name))?;
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
    let (aggregate_delta, hop_group_projection_delta) =
        if let Some(hop_group_projection) = plan.hop_group_projection.as_ref() {
            let projection_delta = build_incremental_materialized_view_state_from_logical_plan(
                &plan.source_name,
                sources,
                udfs,
                &LogicalPlan::Projection(hop_group_projection.clone()),
            )
            .await
            .context("build grouped-count HOP group projection delta plan")?;
            (None, Some(projection_delta))
        } else {
            let aggregate_delta = build_incremental_materialized_view_state_from_logical_plan(
                &plan.source_name,
                sources,
                udfs,
                &LogicalPlan::Aggregate(plan.aggregate.clone()),
            )
            .await
            .context("build grouped-count vectorized aggregate delta plan")?;
            (Some(aggregate_delta), None)
        };

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
        count_state: SlateGroupedCountState::new(table, &state_namespace)
            .await
            .context("initialize SlateDB-backed grouped-count state")?,
        aggregate_delta,
        hop_group_projection_delta,
        aggregate_schema: plan.aggregate_schema,
        group_schema: plan.group_schema,
        hop_group_projection_schema: plan.hop_group_projection_schema,
        hop_groups: plan.hop_groups,
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
    let tick = run_columnar_grouped_count_state_tick(
        columnar,
        insert_batches,
        weighted_delta_batches,
        &mv.output_schema,
        &mv.previous_snapshot,
    )
    .await
    .with_context(|| {
        format!(
            "evaluate Slate-backed grouped-count columnar snapshot delta for '{}'",
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
        mode = "columnar_grouped_count",
        "SlateDB-backed grouped-count columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

pub(super) async fn run_columnar_grouped_count_state_tick(
    columnar: &mut ColumnarGroupedCountMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    output_schema: &SchemaRef,
    previous_snapshot: &[RecordBatch],
) -> Result<ColumnarGroupedCountTick> {
    let total_start = profile::start();
    let phase_start = profile::start();
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
    profile::record_since("grouped_count.prepare_input", phase_start);
    let phase_start = profile::start();
    columnar
        .input_zset
        .create_version(&input_delta, None)
        .await?;
    profile::record_since("grouped_count.input_create_version", phase_start);
    let input_changed = !input_delta.batches().is_empty();
    let phase_start = profile::start();
    let pending = grouped_count_pending_delta(columnar, input_delta.batches()).await?;
    profile::record_since("grouped_count.pending_delta", phase_start);
    let phase_start = profile::start();
    let output_delta_batches = apply_grouped_count_delta(columnar, pending).await?;
    profile::record_since("grouped_count.apply_delta", phase_start);
    let phase_start = profile::start();
    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build grouped-count output zset delta")?;
    profile::record_since("grouped_count.build_output_zset", phase_start);
    let phase_start = profile::start();
    columnar
        .output_zset
        .create_version(
            &output_delta,
            columnar
                .output_zset
                .current_handle()
                .map(|handle| handle.version),
        )
        .await?;
    profile::record_since("grouped_count.output_create_version", phase_start);

    let phase_start = profile::start();
    let delta_batches = output_delta.batches().to_vec();
    let next_snapshot =
        apply_weighted_snapshot_delta(output_schema, previous_snapshot, delta_batches.clone())
            .await
            .context("apply Slate-backed grouped-count columnar snapshot delta")?;
    profile::record_since("grouped_count.output_snapshot_delta", phase_start);
    profile::record_since("grouped_count.total", total_start);

    Ok(ColumnarGroupedCountTick {
        delta: output_delta,
        next_snapshot,
        input_changed,
    })
}

async fn grouped_count_pending_delta(
    columnar: &ColumnarGroupedCountMaterializedViewState,
    input_batches: &[RecordBatch],
) -> Result<AHashMap<Vec<u8>, PendingGroupDelta>> {
    let mut pending = AHashMap::new();
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

    if let Some(hop_projection_delta) = columnar.hop_group_projection_delta.as_ref() {
        let projection_schema = columnar
            .hop_group_projection_schema
            .as_ref()
            .context("grouped-count HOP projection schema missing")?;
        let positive_groups = collect_incremental_output(
            hop_projection_delta,
            &positive_source_batches,
            projection_schema,
        )
        .await?;
        add_hop_group_batches_to_pending(columnar, &positive_groups, 1, &mut pending)?;
        let negative_groups = collect_incremental_output(
            hop_projection_delta,
            &negative_source_batches,
            projection_schema,
        )
        .await?;
        add_hop_group_batches_to_pending(columnar, &negative_groups, -1, &mut pending)?;
    } else {
        let aggregate_delta = columnar
            .aggregate_delta
            .as_ref()
            .context("grouped-count aggregate delta plan missing")?;
        let positive_output = collect_incremental_output(
            aggregate_delta,
            &positive_source_batches,
            &columnar.aggregate_schema,
        )
        .await?;
        add_aggregate_batches_to_pending(columnar, &positive_output, 1, &mut pending)?;
        let negative_output = collect_incremental_output(
            aggregate_delta,
            &negative_source_batches,
            &columnar.aggregate_schema,
        )
        .await?;
        add_aggregate_batches_to_pending(columnar, &negative_output, -1, &mut pending)?;
    }
    pending.retain(|_, delta| delta.delta != 0);
    Ok(pending)
}

fn add_hop_group_batches_to_pending(
    columnar: &ColumnarGroupedCountMaterializedViewState,
    batches: &[RecordBatch],
    sign: i64,
    pending: &mut AHashMap<Vec<u8>, PendingGroupDelta>,
) -> Result<()> {
    let aggregate_batches = expand_hop_group_batches(columnar, batches)?;
    add_aggregate_batches_to_pending(columnar, &aggregate_batches, sign, pending)
}

fn expand_hop_group_batches(
    columnar: &ColumnarGroupedCountMaterializedViewState,
    batches: &[RecordBatch],
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let mut builders = columnar
            .group_schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), batch.num_rows()))
            .collect::<Result<Vec<_>>>()?;
        let mut counts = Int64Builder::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let mut hop_values = vec![None; columnar.count_idx];
            append_hop_group_combinations(
                columnar,
                batch,
                row_idx,
                0,
                &mut hop_values,
                &mut builders,
                &mut counts,
            )?;
        }
        let expanded_rows = counts.len();
        if expanded_rows == 0 {
            continue;
        }
        let mut columns = builders
            .iter_mut()
            .map(ScalarColumnBuilder::finish_array)
            .collect::<Vec<_>>();
        columns.push(Arc::new(counts.finish()) as ArrayRef);
        let aggregate_schema = Arc::new(Schema::new(
            columnar
                .group_schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .chain(std::iter::once(Field::new(
                    columnar.aggregate_schema.field(columnar.count_idx).name(),
                    DataType::Int64,
                    false,
                )))
                .collect::<Vec<_>>(),
        ));
        out.push(RecordBatch::try_new(aggregate_schema, columns)?);
    }
    Ok(out)
}

fn append_hop_group_combinations(
    columnar: &ColumnarGroupedCountMaterializedViewState,
    batch: &RecordBatch,
    row_idx: usize,
    group_idx: usize,
    hop_values: &mut [Option<i64>],
    builders: &mut [ScalarColumnBuilder],
    counts: &mut Int64Builder,
) -> Result<()> {
    if group_idx == columnar.count_idx {
        for idx in 0..columnar.count_idx {
            if let Some(start) = hop_values[idx] {
                builders[idx].append_timestamp_millis_value(start)?;
            } else {
                builders[idx].append_array_value(batch.column(idx).as_ref(), row_idx)?;
            }
        }
        counts.append_value(1);
        return Ok(());
    }

    if let Some(hop) = columnar
        .hop_groups
        .iter()
        .find(|hop| hop.group_idx == group_idx)
    {
        let starts = hop_window_starts(batch.column(group_idx).as_ref(), row_idx, hop)?;
        for start in starts {
            hop_values[group_idx] = Some(start);
            append_hop_group_combinations(
                columnar,
                batch,
                row_idx,
                group_idx + 1,
                hop_values,
                builders,
                counts,
            )?;
        }
        hop_values[group_idx] = None;
        return Ok(());
    }

    append_hop_group_combinations(
        columnar,
        batch,
        row_idx,
        group_idx + 1,
        hop_values,
        builders,
        counts,
    )
}

fn hop_window_starts(array: &dyn Array, row_idx: usize, hop: &HopGroup) -> Result<Vec<i64>> {
    if hop.slide_ms <= 0 || hop.size_ms <= 0 {
        bail!("HOP window slide and size must be positive");
    }
    if array.is_null(row_idx) {
        return Ok(Vec::new());
    }
    let values = array
        .as_any()
        .downcast_ref::<datafusion::arrow::array::TimestampMillisecondArray>()
        .ok_or_else(|| anyhow::anyhow!("HOP group expression must produce timestamp(ms)"))?;
    let time_ms = values.value(row_idx);
    let last_start = time_ms.div_euclid(hop.slide_ms) * hop.slide_ms;
    let mut starts = Vec::new();
    let mut offset = 0_i64;
    while offset < hop.size_ms {
        let start = last_start
            .checked_sub(offset)
            .ok_or_else(|| anyhow::anyhow!("HOP window start overflow"))?;
        let end = start
            .checked_add(hop.size_ms)
            .ok_or_else(|| anyhow::anyhow!("HOP window end overflow"))?;
        if time_ms >= start && time_ms < end {
            starts.push(start);
        }
        offset = offset
            .checked_add(hop.slide_ms)
            .ok_or_else(|| anyhow::anyhow!("HOP window offset overflow"))?;
    }
    Ok(starts)
}

fn add_aggregate_batches_to_pending(
    columnar: &ColumnarGroupedCountMaterializedViewState,
    batches: &[RecordBatch],
    sign: i64,
    pending: &mut AHashMap<Vec<u8>, PendingGroupDelta>,
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
    pending: AHashMap<Vec<u8>, PendingGroupDelta>,
) -> Result<Vec<RecordBatch>> {
    let total_start = profile::start();
    let mut builder = WeightedOutputBuilder::new(
        columnar.output_zset.value_schema(),
        &columnar.output_mapping,
    )?;
    if pending.is_empty() {
        let phase_start = profile::start();
        let output = builder.finish();
        profile::record_since("grouped_count.apply_finish_output", phase_start);
        profile::record_since("grouped_count.apply_total_inner", total_start);
        return output;
    }

    let pending = pending.into_iter().collect::<Vec<_>>();
    let phase_start = profile::start();
    let old_counts = columnar
        .count_state
        .load_counts(pending.iter().map(|(group_key, _)| group_key.as_slice()))?;
    profile::record_since("grouped_count.apply_state_lookup", phase_start);
    let mut count_updates = Vec::with_capacity(pending.len());
    let output_includes_count = columnar.output_mapping.contains(&columnar.count_idx);
    let phase_start = profile::start();
    for ((group_key, delta), old_count) in pending.into_iter().zip(old_counts) {
        let new_count = old_count
            .checked_add(delta.delta)
            .ok_or_else(|| anyhow::anyhow!("grouped-count state overflow"))?;
        if new_count < 0 {
            bail!("grouped-count state removed more rows than were present");
        }
        if new_count == old_count {
            continue;
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
        count_updates.push((group_key, new_count));
    }
    profile::record_since("grouped_count.apply_update_loop", phase_start);
    if !count_updates.is_empty() {
        let mut writes = WriteBatch::new();
        columnar
            .count_state
            .write_count_updates(&mut writes, &count_updates)?;
        let phase_start = profile::start();
        columnar
            .count_state
            .table
            .write_batch(writes)
            .await
            .context("persist grouped-count state updates")?;
        profile::record_since("grouped_count.apply_write_batch", phase_start);
        let phase_start = profile::start();
        columnar.count_state.apply_count_updates(count_updates)?;
        profile::record_since("grouped_count.apply_cache_update", phase_start);
    }
    let phase_start = profile::start();
    let output = builder.finish();
    profile::record_since("grouped_count.apply_finish_output", phase_start);
    profile::record_since("grouped_count.apply_total_inner", total_start);
    output
}

impl SlateGroupedCountState {
    async fn new(table: Arc<dyn KeyValueTable>, namespace: &str) -> Result<Self> {
        let key_prefix = keyspace::namespace_prefix(keyspace::prefix::INDEX, namespace);
        let count_log_namespace = format!("{namespace}__count_log");
        let count_meta_namespace = format!("{namespace}__count_meta");
        let count_log_prefix =
            keyspace::namespace_prefix(keyspace::prefix::INDEX, &count_log_namespace);
        let mut count_sequence_key =
            keyspace::namespace_prefix(keyspace::prefix::INDEX, &count_meta_namespace);
        count_sequence_key.extend_from_slice(b"sequence");
        let next_count_segment_id =
            read_count_sequence(table.as_ref(), &count_sequence_key).await?;
        let state = Self {
            table,
            key_prefix,
            count_log_prefix,
            count_sequence_key,
            next_count_segment_id: Mutex::new(next_count_segment_id),
            counts: Mutex::new(AHashMap::new()),
        };
        let counts = state
            .load_all_counts()
            .await
            .context("load grouped-count state head")?;
        *state
            .counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-count state head poisoned"))? = counts;
        Ok(state)
    }

    async fn load_all_counts(&self) -> Result<AHashMap<Vec<u8>, i64>> {
        let mut counts = AHashMap::new();
        for (key, value) in self
            .table
            .scan_prefix(&self.key_prefix, &ScanOptions::default())
            .await
            .context("scan grouped-count legacy state")?
        {
            if key.as_slice() == self.count_sequence_key.as_slice()
                || key.starts_with(&self.count_log_prefix)
            {
                continue;
            }
            let group_key = self.group_key_from_legacy_state_key(&key)?;
            let count = decode_i64(&value)?;
            if count != 0 {
                counts.insert(group_key.to_vec(), count);
            }
        }
        let mut log_entries = Vec::new();
        for (key, value_bytes) in self
            .table
            .scan_prefix(&self.count_log_prefix, &ScanOptions::default())
            .await
            .context("scan grouped-count state log")?
        {
            log_entries.push((self.count_log_segment_id(&key)?, value_bytes));
        }
        log_entries.sort_by_key(|(segment_id, _)| *segment_id);
        for (_, value_bytes) in log_entries {
            for (group_key, count) in decode_count_log_updates(&value_bytes)? {
                if count == 0 {
                    counts.remove(group_key.as_slice());
                } else {
                    counts.insert(group_key, count);
                }
            }
        }
        Ok(counts)
    }

    fn load_counts<'a>(&self, group_keys: impl IntoIterator<Item = &'a [u8]>) -> Result<Vec<i64>> {
        let counts = self
            .counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-count state head poisoned"))?;
        Ok(group_keys
            .into_iter()
            .map(|group_key| counts.get(group_key).copied().unwrap_or(0))
            .collect())
    }

    fn apply_count_updates(&self, updates: Vec<(Vec<u8>, i64)>) -> Result<()> {
        let mut counts = self
            .counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-count state head poisoned"))?;
        for (group_key, count) in updates {
            if count == 0 {
                counts.remove(&group_key);
            } else {
                counts.insert(group_key, count);
            }
        }
        Ok(())
    }

    fn write_count_updates(
        &self,
        batch: &mut WriteBatch,
        updates: &[(Vec<u8>, i64)],
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let mut next_segment_id = self
            .next_count_segment_id
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-count sequence poisoned"))?;
        let segment_id = *next_segment_id;
        *next_segment_id = next_segment_id.saturating_add(1);
        batch.put(
            self.count_log_key(segment_id),
            encode_count_log_updates(updates)?,
        );
        batch.put(
            self.count_sequence_key.clone(),
            (*next_segment_id).to_be_bytes(),
        );
        Ok(())
    }

    #[cfg(test)]
    fn legacy_state_key(&self, group_key: &[u8]) -> Vec<u8> {
        let mut key = self.key_prefix.clone();
        key.extend_from_slice(group_key);
        key
    }

    fn count_log_key(&self, segment_id: u64) -> Vec<u8> {
        let mut key = self.count_log_prefix.clone();
        key.extend_from_slice(&segment_id.to_be_bytes());
        key
    }

    fn count_log_segment_id(&self, key: &[u8]) -> Result<u64> {
        if !key.starts_with(&self.count_log_prefix) {
            bail!("grouped-count log key prefix mismatch");
        }
        let suffix = &key[self.count_log_prefix.len()..];
        let bytes: [u8; 8] = suffix
            .try_into()
            .map_err(|_| anyhow::anyhow!("grouped-count log segment id must be 8 bytes"))?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn group_key_from_legacy_state_key<'a>(&self, key: &'a [u8]) -> Result<&'a [u8]> {
        if !key.starts_with(&self.key_prefix) {
            bail!("grouped-count state key prefix mismatch");
        }
        Ok(&key[self.key_prefix.len()..])
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

fn hop_group_projection_for_aggregate(
    aggregate: &Aggregate,
) -> Result<(Vec<HopGroup>, Option<Projection>, Option<SchemaRef>)> {
    let mut hop_groups = Vec::new();
    let mut projection_expr = Vec::with_capacity(aggregate.group_expr.len());
    for (group_idx, expr) in aggregate.group_expr.iter().enumerate() {
        if let Some((time_expr, slide_ms, size_ms)) = hop_group_expr(expr)? {
            hop_groups.push(HopGroup {
                group_idx,
                slide_ms,
                size_ms,
            });
            projection_expr.push(time_expr);
        } else {
            projection_expr.push(expr.clone());
        }
    }
    if hop_groups.is_empty() {
        return Ok((hop_groups, None, None));
    }

    let projection = Projection::try_new(projection_expr, Arc::clone(&aggregate.input))
        .context("build grouped-count HOP group projection")?;
    let projection_schema = df_schema_to_arrow(&projection.schema)?;
    let aggregate_schema = df_schema_to_arrow(&aggregate.schema)?;
    for (idx, projected) in projection_schema.fields().iter().enumerate() {
        let expected = aggregate_schema.field(idx);
        if projected.data_type() != expected.data_type() {
            bail!(
                "grouped-count HOP projection field {} type {:?} does not match aggregate group type {:?}",
                idx,
                projected.data_type(),
                expected.data_type()
            );
        }
    }
    Ok((hop_groups, Some(projection), Some(projection_schema)))
}

fn hop_group_expr(expr: &Expr) -> Result<Option<(Expr, i64, i64)>> {
    let Expr::ScalarFunction(function) = strip_alias(expr) else {
        return Ok(None);
    };
    if !function.name().eq_ignore_ascii_case("hop") {
        return Ok(None);
    }
    if function.args.len() < 3 {
        bail!("HOP group expression requires time, slide, and size arguments");
    }
    let slide_ms = literal_i64(&function.args[1]).context("parse HOP slide milliseconds")?;
    let size_ms = literal_i64(&function.args[2]).context("parse HOP size milliseconds")?;
    if slide_ms <= 0 || size_ms <= 0 {
        bail!("HOP slide and size must be positive");
    }
    Ok(Some((function.args[0].clone(), slide_ms, size_ms)))
}

fn literal_i64(expr: &Expr) -> Result<i64> {
    match strip_alias(expr) {
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Ok(*value),
        other => bail!("expected Int64 literal, found {other:?}"),
    }
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

async fn read_count_sequence(table: &dyn KeyValueTable, key: &[u8]) -> Result<u64> {
    let Some(bytes) = table
        .get_bytes(key)
        .await
        .context("read grouped-count sequence")?
    else {
        return Ok(1);
    };
    let bytes: [u8; 8] = bytes
        .as_ref()
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-count sequence must be 8 bytes"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn encode_count_log_updates(updates: &[(Vec<u8>, i64)]) -> Result<Vec<u8>> {
    let mut capacity = 4;
    for (group_key, _) in updates {
        capacity += 4 + group_key.len() + 8;
    }
    let mut out = Vec::with_capacity(capacity);
    let update_count =
        u32::try_from(updates.len()).context("grouped-count log update count too large")?;
    out.extend_from_slice(&update_count.to_be_bytes());
    for (group_key, count) in updates {
        let group_key_len =
            u32::try_from(group_key.len()).context("grouped-count group key too large")?;
        out.extend_from_slice(&group_key_len.to_be_bytes());
        out.extend_from_slice(group_key);
        out.extend_from_slice(&count.to_be_bytes());
    }
    Ok(out)
}

fn decode_count_log_updates(bytes: &[u8]) -> Result<Vec<(Vec<u8>, i64)>> {
    let mut cursor = 0;
    let update_count = read_u32_at(bytes, &mut cursor)?;
    let mut updates = Vec::with_capacity(update_count as usize);
    for _ in 0..update_count {
        let group_key_len = read_u32_at(bytes, &mut cursor)? as usize;
        let group_key = read_bytes_at(
            bytes,
            &mut cursor,
            group_key_len,
            "grouped-count log group key",
        )?
        .to_vec();
        let count = decode_i64(read_bytes_at(
            bytes,
            &mut cursor,
            8,
            "grouped-count log count",
        )?)?;
        updates.push((group_key, count));
    }
    if cursor != bytes.len() {
        bail!("grouped-count log payload has trailing bytes");
    }
    Ok(updates)
}

fn read_u32_at(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let chunk = read_bytes_at(bytes, cursor, 4, "grouped-count u32")?;
    let value = <[u8; 4]>::try_from(chunk)
        .map(u32::from_be_bytes)
        .map_err(|_| anyhow::anyhow!("grouped-count u32 expected 4 bytes"))?;
    Ok(value)
}

fn read_bytes_at<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
    label: &str,
) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| anyhow::anyhow!("{label} overflow"))?;
    let chunk = bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow::anyhow!("{label} truncated"))?;
    *cursor = end;
    Ok(chunk)
}

fn decode_i64(bytes: &[u8]) -> Result<i64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-count state value must be 8 bytes"))?;
    Ok(i64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    use object_store::memory::InMemory;
    use slatedb::Db;

    async fn build_table(name: &str) -> Arc<dyn KeyValueTable> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
        Arc::new(dbsp::storage::SlateTable::new(db))
    }

    #[tokio::test]
    async fn grouped_count_state_replays_logged_updates() {
        let table = build_table("grouped-count-log-replay").await;
        let state = SlateGroupedCountState::new(Arc::clone(&table), "grouped_count")
            .await
            .expect("state");

        let mut batch = WriteBatch::new();
        let updates = vec![(b"a".to_vec(), 2), (b"b".to_vec(), 5)];
        state
            .write_count_updates(&mut batch, &updates)
            .expect("write count updates");
        table.write_batch(batch).await.expect("persist updates");
        state
            .apply_count_updates(updates)
            .expect("apply count updates");

        let reopened = SlateGroupedCountState::new(Arc::clone(&table), "grouped_count")
            .await
            .expect("reopen state");
        assert_eq!(
            reopened
                .load_counts([b"a".as_slice(), b"b".as_slice(), b"c".as_slice()])
                .expect("load counts"),
            vec![2, 5, 0]
        );

        let mut batch = WriteBatch::new();
        let updates = vec![(b"a".to_vec(), 0), (b"b".to_vec(), 7)];
        reopened
            .write_count_updates(&mut batch, &updates)
            .expect("write second count updates");
        table
            .write_batch(batch)
            .await
            .expect("persist second updates");

        let replayed = SlateGroupedCountState::new(Arc::clone(&table), "grouped_count")
            .await
            .expect("replay state");
        assert_eq!(
            replayed
                .load_counts([b"a".as_slice(), b"b".as_slice(), b"c".as_slice()])
                .expect("load replayed counts"),
            vec![0, 7, 0]
        );
    }

    #[tokio::test]
    async fn grouped_count_state_reads_legacy_raw_count_keys() {
        let table = build_table("grouped-count-legacy-state").await;
        let state = SlateGroupedCountState::new(Arc::clone(&table), "grouped_count")
            .await
            .expect("state");
        let mut batch = WriteBatch::new();
        batch.put(state.legacy_state_key(b"legacy"), 11_i64.to_be_bytes());
        table
            .write_batch(batch)
            .await
            .expect("persist legacy count");

        let reopened = SlateGroupedCountState::new(Arc::clone(&table), "grouped_count")
            .await
            .expect("reopen state");
        assert_eq!(
            reopened
                .load_counts([b"legacy".as_slice(), b"missing".as_slice()])
                .expect("load legacy count"),
            vec![11, 0]
        );
    }
}
