use std::collections::{HashMap, hash_map::Entry};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use ahash::AHashMap;
use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{
    Array, ArrayBuilder, ArrayRef, Int64Array, Int64Builder, TimestampMillisecondArray,
    UInt32Array, UInt64Array,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
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

use crate::columnar_snapshot::columnar_zset_weight_sum;
use crate::delta_consolidation::weighted_snapshot_schema;
use crate::mv::registry::{ColumnarMaterializedViewStorage, MaterializedViewRegistry};
use crate::namespaces;
use crate::scalar_array_builder::ScalarColumnBuilder;
use crate::vectorized_runtime::source_state::incremental_source_for_plan;
use crate::vectorized_source_delta::unit_source_delta_batches;

use super::{
    IncrementalMaterializedViewState, VectorizedMaterializedViewState, VectorizedSourceState,
    build_incremental_materialized_view_state_from_logical_plan, collect_incremental_output,
    profile,
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
    append_only_single_hop: Option<AppendOnlySingleHopCountPlan>,
}

pub(super) struct ColumnarGroupedCountMaterializedViewState {
    source_name: String,
    source_schema: SchemaRef,
    output_zset: SlateBackedColumnarZSet,
    count_state: SlateGroupedCountState,
    append_only_single_hop: Option<AppendOnlySingleHopCountState>,
    aggregate_delta: Option<IncrementalMaterializedViewState>,
    hop_group_projection_delta: Option<IncrementalMaterializedViewState>,
    aggregate_schema: SchemaRef,
    group_schema: SchemaRef,
    hop_group_projection_schema: Option<SchemaRef>,
    hop_groups: Vec<HopGroup>,
    output_mapping: Vec<usize>,
    count_idx: usize,
    row_count: i64,
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
struct AppendOnlySingleHopCountPlan {
    value_group_idx: usize,
    hop_group_idx: usize,
    value_kind: AppendOnlySingleHopValueKind,
    output_columns: Vec<AppendOnlySingleHopOutputColumn>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppendOnlySingleHopValueKind {
    Int64,
    UInt64,
}

#[derive(Debug, Clone, Copy)]
enum AppendOnlySingleHopOutputColumn {
    Value,
    HopStart,
    Count,
}

struct AppendOnlySingleHopCountState {
    plan: AppendOnlySingleHopCountPlan,
    table: Arc<dyn KeyValueTable>,
    count_log_prefix: Vec<u8>,
    count_sequence_key: Vec<u8>,
    next_count_segment_id: Mutex<u64>,
    counts: Mutex<AHashMap<AppendOnlySingleHopKey, i64>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct AppendOnlySingleHopKey {
    value: AppendOnlySingleHopValue,
    window_start_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AppendOnlySingleHopValue {
    Null,
    Int64(i64),
    UInt64(u64),
}

#[derive(Debug, Clone)]
struct HopGroup {
    group_idx: usize,
    slide_ms: i64,
    size_ms: i64,
}

pub(super) struct ColumnarGroupedCountTick {
    pub(super) delta: ColumnarZSet,
    pub(super) row_count_delta: i64,
}

pub(super) struct ColumnarGroupedCountPublication {
    pub(super) delta_rows: usize,
    pub(super) snapshot_rows: usize,
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
    let append_only_single_hop = sources
        .get(&source_name)
        .filter(|source| source.append_only)
        .and_then(|_| {
            append_only_single_hop_count_plan(
                &group_schema,
                &hop_groups,
                count_idx,
                &output_mapping,
            )
        });

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
        append_only_single_hop,
    }))
}

fn append_only_single_hop_count_plan(
    group_schema: &SchemaRef,
    hop_groups: &[HopGroup],
    count_idx: usize,
    output_mapping: &[usize],
) -> Option<AppendOnlySingleHopCountPlan> {
    if hop_groups.len() != 1 || count_idx != 2 {
        return None;
    }
    let hop_group_idx = hop_groups[0].group_idx;
    if hop_group_idx >= count_idx {
        return None;
    }
    let value_group_idx = if hop_group_idx == 0 { 1 } else { 0 };
    let value_kind = match group_schema.field(value_group_idx).data_type() {
        DataType::Int64 => AppendOnlySingleHopValueKind::Int64,
        DataType::UInt64 => AppendOnlySingleHopValueKind::UInt64,
        _ => return None,
    };
    match group_schema.field(hop_group_idx).data_type() {
        DataType::Timestamp(TimeUnit::Millisecond, _) => {}
        _ => return None,
    }

    let mut output_columns = Vec::with_capacity(output_mapping.len());
    for source_idx in output_mapping {
        if *source_idx == value_group_idx {
            output_columns.push(AppendOnlySingleHopOutputColumn::Value);
        } else if *source_idx == hop_group_idx {
            output_columns.push(AppendOnlySingleHopOutputColumn::HopStart);
        } else if *source_idx == count_idx {
            output_columns.push(AppendOnlySingleHopOutputColumn::Count);
        } else {
            return None;
        }
    }
    Some(AppendOnlySingleHopCountPlan {
        value_group_idx,
        hop_group_idx,
        value_kind,
        output_columns,
    })
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
    let output_namespace = format!("{mv_namespace}/columnar/grouped_count/output");
    let state_namespace = format!("{mv_namespace}/columnar/grouped_count/state");
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed grouped-count output zset")?;
    let initial_output = output_zset
        .materialize_columnar()
        .await
        .context("load grouped-count output snapshot")?;
    let initial_row_count = columnar_zset_weight_sum(&initial_output)?;
    let initial_snapshot = snapshot_batches_from_zset(&initial_output)?;
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
    let count_state = SlateGroupedCountState::new(Arc::clone(&table), &state_namespace)
        .await
        .context("initialize SlateDB-backed grouped-count state")?;
    let append_only_single_hop = if count_state.is_empty()? {
        match plan.append_only_single_hop.clone() {
            Some(fast_plan) => Some(
                AppendOnlySingleHopCountState::new(table, &state_namespace, fast_plan)
                    .await
                    .context("initialize append-only single-HOP grouped-count state")?,
            ),
            None => None,
        }
    } else {
        None
    };

    Ok(ColumnarGroupedCountMaterializedViewState {
        source_name: plan.source_name,
        source_schema: Arc::clone(&source.schema),
        output_zset,
        count_state,
        append_only_single_hop,
        aggregate_delta,
        hop_group_projection_delta,
        aggregate_schema: plan.aggregate_schema,
        group_schema: plan.group_schema,
        hop_group_projection_schema: plan.hop_group_projection_schema,
        hop_groups: plan.hop_groups,
        output_mapping: plan.output_mapping,
        count_idx: plan.count_idx,
        row_count: initial_row_count,
        initial_snapshot,
    })
}

pub(super) async fn run_columnar_grouped_count_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<()> {
    let super::MaterializedViewOperator::GroupedCount(columnar) = &mut mv.operator else {
        unreachable!("grouped-count tick dispatched to another operator")
    };

    let plan_start = Instant::now();
    let tick =
        run_columnar_grouped_count_state_tick(columnar, insert_batches, weighted_delta_batches)
            .await
            .with_context(|| {
                format!(
                    "evaluate Slate-backed grouped-count columnar delta for '{}'",
                    mv.view_name
                )
            })?;

    let publication = publish_columnar_grouped_count_tick(
        registry,
        &mv.view_name,
        &mv.output_schema,
        columnar,
        tick,
        version,
    )?;
    tracing::debug!(
        view = %mv.view_name,
        version,
        delta_rows = publication.delta_rows,
        snapshot_rows = publication.snapshot_rows,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "columnar_grouped_count",
        "SlateDB-backed grouped-count columnar DBSP materialized view tick completed"
    );
    Ok(())
}

pub(super) fn publish_columnar_grouped_count_tick(
    registry: &MaterializedViewRegistry,
    view_name: &str,
    output_schema: &SchemaRef,
    columnar: &mut ColumnarGroupedCountMaterializedViewState,
    tick: ColumnarGroupedCountTick,
    version: i64,
) -> Result<ColumnarGroupedCountPublication> {
    let delta_batches = tick.delta.batches().to_vec();
    let delta_rows = delta_batches
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    columnar.row_count = columnar.row_count.saturating_add(tick.row_count_delta);
    if columnar.row_count < 0 {
        bail!("grouped-count columnar materialized view '{view_name}' row count became negative");
    }
    let snapshot_rows =
        usize::try_from(columnar.row_count).context("grouped-count row count exceeds usize")?;
    let handle = registry.register(view_name.to_string());
    if let Some(zset_handle) = columnar.output_zset.current_handle() {
        handle.publish_columnar_version(
            version,
            zset_handle,
            ColumnarMaterializedViewStorage::new(
                Arc::clone(&columnar.count_state.table),
                Arc::clone(output_schema),
            ),
            snapshot_rows,
            delta_batches,
        );
    } else {
        handle.publish_arrow_version(
            version,
            vec![RecordBatch::new_empty(Arc::clone(output_schema))],
            delta_batches,
        );
    }
    Ok(ColumnarGroupedCountPublication {
        delta_rows,
        snapshot_rows,
    })
}

pub(super) async fn run_columnar_grouped_count_state_tick(
    columnar: &mut ColumnarGroupedCountMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarGroupedCountTick> {
    let tick_start = Instant::now();
    let total_start = profile::start();
    let prepare_start = Instant::now();
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
    let prepare_ms = prepare_start.elapsed().as_millis() as u64;
    profile::record_since("grouped_count.prepare_input", phase_start);
    let input_delta_rows = input_delta
        .batches()
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    let apply_start = Instant::now();
    let pending_start = Instant::now();
    let phase_start = profile::start();
    let fast_output_delta_batches =
        append_only_single_hop_output_delta_batches(columnar, input_delta.batches()).await?;
    let pending_ms;
    let pending_count;
    let output_delta_batches = if let Some(output_delta_batches) = fast_output_delta_batches {
        pending_ms = pending_start.elapsed().as_millis() as u64;
        pending_count = output_delta_batches.iter().map(RecordBatch::num_rows).sum();
        profile::record_since("grouped_count.pending_delta", phase_start);
        output_delta_batches
    } else {
        let pending = grouped_count_pending_delta(columnar, input_delta.batches()).await?;
        pending_count = pending.len();
        pending_ms = pending_start.elapsed().as_millis() as u64;
        profile::record_since("grouped_count.pending_delta", phase_start);
        let phase_start = profile::start();
        let output_delta_batches = apply_grouped_count_delta(columnar, pending).await?;
        profile::record_since("grouped_count.apply_delta", phase_start);
        output_delta_batches
    };
    let apply_ms = apply_start.elapsed().as_millis() as u64;
    let build_output_start = Instant::now();
    let phase_start = profile::start();
    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build grouped-count output zset delta")?;
    let build_output_ms = build_output_start.elapsed().as_millis() as u64;
    profile::record_since("grouped_count.build_output_zset", phase_start);
    let output_create_start = Instant::now();
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
    let output_create_ms = output_create_start.elapsed().as_millis() as u64;
    profile::record_since("grouped_count.output_create_version", phase_start);

    let delta_batches = output_delta.batches().to_vec();
    let output_delta_rows = delta_batches
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    let row_count_delta =
        columnar_zset_weight_sum(&output_delta).context("compute grouped-count row-count delta")?;
    profile::record_since("grouped_count.total", total_start);
    tracing::debug!(
        source = %columnar.source_name,
        input_delta_rows,
        pending_count,
        output_delta_rows,
        prepare_ms,
        pending_ms,
        apply_ms,
        build_output_ms,
        output_create_ms,
        snapshot_ms = 0_u64,
        row_count_delta,
        total_ms = tick_start.elapsed().as_millis() as u64,
        mode = "columnar_grouped_count",
        "SlateDB-backed grouped-count columnar DBSP state tick completed"
    );

    Ok(ColumnarGroupedCountTick {
        delta: output_delta,
        row_count_delta,
    })
}

async fn append_only_single_hop_output_delta_batches(
    columnar: &ColumnarGroupedCountMaterializedViewState,
    input_batches: &[RecordBatch],
) -> Result<Option<Vec<RecordBatch>>> {
    let Some(state) = columnar.append_only_single_hop.as_ref() else {
        return Ok(None);
    };
    let hop_projection_delta = columnar
        .hop_group_projection_delta
        .as_ref()
        .context("append-only single-HOP grouped count requires HOP projection state")?;
    if input_batches.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let mut positive_source_batches = Vec::new();
    for batch in input_batches {
        let unit_delta =
            unit_source_delta_batches(&columnar.source_schema, batch)?.with_context(|| {
                format!(
                    "append-only grouped-count materialized view received non-unit weighted source deltas for '{}'",
                    columnar.source_name
                )
            })?;
        if unit_delta.negative.iter().any(|batch| batch.num_rows() > 0) {
            bail!("append-only single-HOP grouped count received a retraction");
        }
        positive_source_batches.extend(
            unit_delta
                .positive
                .into_iter()
                .filter(|batch| batch.num_rows() > 0),
        );
    }
    if positive_source_batches.is_empty() {
        return Ok(Some(Vec::new()));
    }

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
    let hop = columnar
        .hop_groups
        .first()
        .context("append-only single-HOP grouped count requires one HOP group")?;
    let pending = state.pending_delta(&positive_groups, hop)?;
    state
        .apply_pending_delta(pending, columnar.output_zset.value_schema())
        .await
        .map(Some)
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
    if columnar.hop_groups.len() == 1 {
        return expand_single_hop_group_batches(columnar, batches);
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
        out.push(RecordBatch::try_new(
            aggregate_batch_schema(columnar),
            columns,
        )?);
    }
    Ok(out)
}

fn expand_single_hop_group_batches(
    columnar: &ColumnarGroupedCountMaterializedViewState,
    batches: &[RecordBatch],
) -> Result<Vec<RecordBatch>> {
    let hop = columnar
        .hop_groups
        .first()
        .context("single HOP expansion requires one HOP group")?;
    if hop.group_idx >= columnar.count_idx {
        bail!("HOP group index exceeds grouped-count group column count");
    }
    let window_capacity = hop_window_count_upper_bound(hop)?;
    let aggregate_schema = aggregate_batch_schema(columnar);
    let mut out = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let capacity = batch.num_rows().saturating_mul(window_capacity);
        let mut builders = columnar
            .group_schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), capacity))
            .collect::<Result<Vec<_>>>()?;
        let mut counts = Int64Builder::with_capacity(capacity);
        let hop_times = batch
            .column(hop.group_idx)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| anyhow::anyhow!("HOP group expression must produce timestamp(ms)"))?;

        for row_idx in 0..batch.num_rows() {
            if hop_times.is_null(row_idx) {
                continue;
            }
            let emitted = append_hop_window_starts_from_time(
                hop_times.value(row_idx),
                hop,
                &mut builders[hop.group_idx],
                &mut counts,
            )?;
            if emitted == 0 {
                continue;
            }
            for (group_idx, builder) in builders.iter_mut().enumerate().take(columnar.count_idx) {
                if group_idx == hop.group_idx {
                    continue;
                }
                builder.append_array_value_repeated(
                    batch.column(group_idx).as_ref(),
                    row_idx,
                    emitted,
                )?;
            }
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
        out.push(RecordBatch::try_new(
            Arc::clone(&aggregate_schema),
            columns,
        )?);
    }
    Ok(out)
}

fn aggregate_batch_schema(columnar: &ColumnarGroupedCountMaterializedViewState) -> SchemaRef {
    Arc::new(Schema::new(
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
    ))
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

fn hop_window_count_upper_bound(hop: &HopGroup) -> Result<usize> {
    if hop.slide_ms <= 0 || hop.size_ms <= 0 {
        bail!("HOP window slide and size must be positive");
    }
    let numerator = hop
        .size_ms
        .checked_add(hop.slide_ms - 1)
        .ok_or_else(|| anyhow::anyhow!("HOP window count overflow"))?;
    usize::try_from(numerator / hop.slide_ms).context("HOP window count exceeds usize")
}

fn append_hop_window_starts_from_time(
    time_ms: i64,
    hop: &HopGroup,
    builder: &mut ScalarColumnBuilder,
    counts: &mut Int64Builder,
) -> Result<usize> {
    if hop.slide_ms <= 0 || hop.size_ms <= 0 {
        bail!("HOP window slide and size must be positive");
    }
    let last_start = time_ms.div_euclid(hop.slide_ms) * hop.slide_ms;
    let mut emitted = 0usize;
    let mut offset = 0_i64;
    while offset < hop.size_ms {
        let start = last_start
            .checked_sub(offset)
            .ok_or_else(|| anyhow::anyhow!("HOP window start overflow"))?;
        let end = start
            .checked_add(hop.size_ms)
            .ok_or_else(|| anyhow::anyhow!("HOP window end overflow"))?;
        if time_ms >= start && time_ms < end {
            builder.append_timestamp_millis_value(start)?;
            counts.append_value(1);
            emitted = emitted.saturating_add(1);
        }
        offset = offset
            .checked_add(hop.slide_ms)
            .ok_or_else(|| anyhow::anyhow!("HOP window offset overflow"))?;
    }
    Ok(emitted)
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

impl AppendOnlySingleHopCountState {
    async fn new(
        table: Arc<dyn KeyValueTable>,
        namespace: &str,
        plan: AppendOnlySingleHopCountPlan,
    ) -> Result<Self> {
        let count_log_namespace = format!("{namespace}__append_only_single_hop_count_log");
        let count_meta_namespace = format!("{namespace}__append_only_single_hop_count_meta");
        let count_log_prefix =
            keyspace::namespace_prefix(keyspace::prefix::INDEX, &count_log_namespace);
        let mut count_sequence_key =
            keyspace::namespace_prefix(keyspace::prefix::INDEX, &count_meta_namespace);
        count_sequence_key.extend_from_slice(b"sequence");
        let next_count_segment_id =
            read_count_sequence(table.as_ref(), &count_sequence_key).await?;
        let state = Self {
            plan,
            table,
            count_log_prefix,
            count_sequence_key,
            next_count_segment_id: Mutex::new(next_count_segment_id),
            counts: Mutex::new(AHashMap::new()),
        };
        let counts = state
            .load_all_counts()
            .await
            .context("load append-only single-HOP grouped-count state head")?;
        *state.counts.lock().map_err(|_| {
            anyhow::anyhow!("append-only single-HOP grouped-count state head poisoned")
        })? = counts;
        Ok(state)
    }

    fn pending_delta(
        &self,
        batches: &[RecordBatch],
        hop: &HopGroup,
    ) -> Result<AHashMap<AppendOnlySingleHopKey, i64>> {
        let mut pending = AHashMap::new();
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let hop_times = batch
                .column(self.plan.hop_group_idx)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    anyhow::anyhow!("HOP group expression must produce timestamp(ms)")
                })?;
            match self.plan.value_kind {
                AppendOnlySingleHopValueKind::Int64 => {
                    let values = batch
                        .column(self.plan.value_group_idx)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "append-only single-HOP grouped count expected Int64 group key"
                            )
                        })?;
                    for row_idx in 0..batch.num_rows() {
                        if hop_times.is_null(row_idx) {
                            continue;
                        }
                        let value = if values.is_null(row_idx) {
                            AppendOnlySingleHopValue::Null
                        } else {
                            AppendOnlySingleHopValue::Int64(values.value(row_idx))
                        };
                        add_single_hop_pending_windows(
                            value,
                            hop_times.value(row_idx),
                            hop,
                            &mut pending,
                        )?;
                    }
                }
                AppendOnlySingleHopValueKind::UInt64 => {
                    let values = batch
                        .column(self.plan.value_group_idx)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "append-only single-HOP grouped count expected UInt64 group key"
                            )
                        })?;
                    for row_idx in 0..batch.num_rows() {
                        if hop_times.is_null(row_idx) {
                            continue;
                        }
                        let value = if values.is_null(row_idx) {
                            AppendOnlySingleHopValue::Null
                        } else {
                            AppendOnlySingleHopValue::UInt64(values.value(row_idx))
                        };
                        add_single_hop_pending_windows(
                            value,
                            hop_times.value(row_idx),
                            hop,
                            &mut pending,
                        )?;
                    }
                }
            }
        }
        Ok(pending)
    }

    async fn apply_pending_delta(
        &self,
        pending: AHashMap<AppendOnlySingleHopKey, i64>,
        output_schema: SchemaRef,
    ) -> Result<Vec<RecordBatch>> {
        let total_start = profile::start();
        let mut builder = AppendOnlySingleHopOutputBuilder::new(
            output_schema,
            &self.plan.output_columns,
            pending.len().saturating_mul(2),
        )?;
        if pending.is_empty() {
            let output = builder.finish();
            profile::record_since("grouped_count.append_only_hop_total", total_start);
            return output;
        }

        let pending = pending.into_iter().collect::<Vec<_>>();
        let phase_start = profile::start();
        let old_counts = self.load_counts(pending.iter().map(|(key, _)| *key))?;
        profile::record_since("grouped_count.append_only_hop_state_lookup", phase_start);

        let phase_start = profile::start();
        let mut count_updates = Vec::with_capacity(pending.len());
        for ((key, delta), old_count) in pending.into_iter().zip(old_counts) {
            let new_count = old_count
                .checked_add(delta)
                .ok_or_else(|| anyhow::anyhow!("append-only single-HOP grouped-count overflow"))?;
            if new_count < 0 {
                bail!("append-only single-HOP grouped-count removed more rows than were present");
            }
            if new_count == old_count {
                continue;
            }
            if old_count > 0 {
                builder.append(key, old_count, -1)?;
            }
            if new_count > 0 {
                builder.append(key, new_count, 1)?;
            }
            count_updates.push((key, new_count));
        }
        profile::record_since("grouped_count.append_only_hop_update_loop", phase_start);

        if !count_updates.is_empty() {
            let mut writes = WriteBatch::new();
            self.write_count_updates(&mut writes, &count_updates)?;
            let phase_start = profile::start();
            self.table
                .write_batch(writes)
                .await
                .context("persist append-only single-HOP grouped-count state updates")?;
            profile::record_since("grouped_count.append_only_hop_write_batch", phase_start);
            let phase_start = profile::start();
            self.apply_count_updates(count_updates)?;
            profile::record_since("grouped_count.append_only_hop_cache_update", phase_start);
        }

        let output = builder.finish();
        profile::record_since("grouped_count.append_only_hop_total", total_start);
        output
    }

    async fn load_all_counts(&self) -> Result<AHashMap<AppendOnlySingleHopKey, i64>> {
        let mut log_entries = Vec::new();
        for (key, value_bytes) in self
            .table
            .scan_prefix(&self.count_log_prefix, &ScanOptions::default())
            .await
            .context("scan append-only single-HOP grouped-count state log")?
        {
            log_entries.push((self.count_log_segment_id(&key)?, value_bytes));
        }
        log_entries.sort_by_key(|(segment_id, _)| *segment_id);
        let mut counts = AHashMap::new();
        for (_, value_bytes) in log_entries {
            for (key, count) in
                decode_append_only_single_hop_count_updates(&value_bytes, self.plan.value_kind)?
            {
                if count == 0 {
                    counts.remove(&key);
                } else {
                    counts.insert(key, count);
                }
            }
        }
        Ok(counts)
    }

    fn load_counts(
        &self,
        keys: impl IntoIterator<Item = AppendOnlySingleHopKey>,
    ) -> Result<Vec<i64>> {
        let counts = self.counts.lock().map_err(|_| {
            anyhow::anyhow!("append-only single-HOP grouped-count state head poisoned")
        })?;
        Ok(keys
            .into_iter()
            .map(|key| counts.get(&key).copied().unwrap_or(0))
            .collect())
    }

    fn apply_count_updates(&self, updates: Vec<(AppendOnlySingleHopKey, i64)>) -> Result<()> {
        let mut counts = self.counts.lock().map_err(|_| {
            anyhow::anyhow!("append-only single-HOP grouped-count state head poisoned")
        })?;
        for (key, count) in updates {
            if count == 0 {
                counts.remove(&key);
            } else {
                counts.insert(key, count);
            }
        }
        Ok(())
    }

    fn write_count_updates(
        &self,
        batch: &mut WriteBatch,
        updates: &[(AppendOnlySingleHopKey, i64)],
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let mut next_segment_id = self.next_count_segment_id.lock().map_err(|_| {
            anyhow::anyhow!("append-only single-HOP grouped-count sequence poisoned")
        })?;
        let segment_id = *next_segment_id;
        *next_segment_id = next_segment_id.saturating_add(1);
        batch.put(
            self.count_log_key(segment_id),
            encode_append_only_single_hop_count_updates(updates, self.plan.value_kind)?,
        );
        batch.put(
            self.count_sequence_key.clone(),
            (*next_segment_id).to_be_bytes(),
        );
        Ok(())
    }

    fn count_log_key(&self, segment_id: u64) -> Vec<u8> {
        let mut key = self.count_log_prefix.clone();
        key.extend_from_slice(&segment_id.to_be_bytes());
        key
    }

    fn count_log_segment_id(&self, key: &[u8]) -> Result<u64> {
        if !key.starts_with(&self.count_log_prefix) {
            bail!("append-only single-HOP grouped-count log key prefix mismatch");
        }
        let suffix = &key[self.count_log_prefix.len()..];
        let bytes: [u8; 8] = suffix.try_into().map_err(|_| {
            anyhow::anyhow!("append-only single-HOP grouped-count segment id must be 8 bytes")
        })?;
        Ok(u64::from_be_bytes(bytes))
    }
}

fn add_single_hop_pending_windows(
    value: AppendOnlySingleHopValue,
    time_ms: i64,
    hop: &HopGroup,
    pending: &mut AHashMap<AppendOnlySingleHopKey, i64>,
) -> Result<()> {
    if hop.slide_ms <= 0 || hop.size_ms <= 0 {
        bail!("HOP window slide and size must be positive");
    }
    let last_start = time_ms.div_euclid(hop.slide_ms) * hop.slide_ms;
    let mut offset = 0_i64;
    while offset < hop.size_ms {
        let start = last_start
            .checked_sub(offset)
            .ok_or_else(|| anyhow::anyhow!("HOP window start overflow"))?;
        let end = start
            .checked_add(hop.size_ms)
            .ok_or_else(|| anyhow::anyhow!("HOP window end overflow"))?;
        if time_ms >= start && time_ms < end {
            let key = AppendOnlySingleHopKey {
                value,
                window_start_ms: start,
            };
            let entry = pending.entry(key).or_insert(0);
            *entry = entry
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("append-only single-HOP pending overflow"))?;
        }
        offset = offset
            .checked_add(hop.slide_ms)
            .ok_or_else(|| anyhow::anyhow!("HOP window offset overflow"))?;
    }
    Ok(())
}

struct AppendOnlySingleHopOutputBuilder {
    weighted_schema: SchemaRef,
    output_columns: Vec<AppendOnlySingleHopOutputColumn>,
    builders: Vec<ScalarColumnBuilder>,
    weights: Int64Builder,
    rows: usize,
}

impl AppendOnlySingleHopOutputBuilder {
    fn new(
        schema: SchemaRef,
        output_columns: &[AppendOnlySingleHopOutputColumn],
        capacity: usize,
    ) -> Result<Self> {
        let capacity = capacity.max(1);
        let builders = schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), capacity))
            .collect::<Result<Vec<_>>>()?;
        let weighted_schema = weighted_snapshot_schema(&schema)?;
        Ok(Self {
            weighted_schema,
            output_columns: output_columns.to_vec(),
            builders,
            weights: Int64Builder::with_capacity(capacity),
            rows: 0,
        })
    }

    fn append(&mut self, key: AppendOnlySingleHopKey, count: i64, weight: i64) -> Result<()> {
        for (output_idx, output_column) in self.output_columns.iter().copied().enumerate() {
            match output_column {
                AppendOnlySingleHopOutputColumn::Value => match key.value {
                    AppendOnlySingleHopValue::Null => {
                        self.builders[output_idx].append_encoded_scalar(None)?;
                    }
                    AppendOnlySingleHopValue::Int64(value) => {
                        self.builders[output_idx].append_i64_value(value)?;
                    }
                    AppendOnlySingleHopValue::UInt64(value) => {
                        self.builders[output_idx].append_u64_value(value)?;
                    }
                },
                AppendOnlySingleHopOutputColumn::HopStart => {
                    self.builders[output_idx].append_timestamp_millis_value(key.window_start_ms)?;
                }
                AppendOnlySingleHopOutputColumn::Count => {
                    self.builders[output_idx].append_i64_value(count)?;
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

    fn is_empty(&self) -> Result<bool> {
        let counts = self
            .counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-count state head poisoned"))?;
        Ok(counts.is_empty())
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

fn encode_append_only_single_hop_count_updates(
    updates: &[(AppendOnlySingleHopKey, i64)],
    value_kind: AppendOnlySingleHopValueKind,
) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(1 + 4 + updates.len().saturating_mul(25));
    out.push(append_only_single_hop_value_kind_tag(value_kind));
    let update_count = u32::try_from(updates.len())
        .context("append-only single-HOP grouped-count update count too large")?;
    out.extend_from_slice(&update_count.to_be_bytes());
    for (key, count) in updates {
        match (value_kind, key.value) {
            (_, AppendOnlySingleHopValue::Null) => {
                out.push(0);
                out.extend_from_slice(&0_u64.to_be_bytes());
            }
            (AppendOnlySingleHopValueKind::Int64, AppendOnlySingleHopValue::Int64(value)) => {
                out.push(1);
                out.extend_from_slice(&value.to_be_bytes());
            }
            (AppendOnlySingleHopValueKind::UInt64, AppendOnlySingleHopValue::UInt64(value)) => {
                out.push(1);
                out.extend_from_slice(&value.to_be_bytes());
            }
            _ => bail!("append-only single-HOP grouped-count key type mismatch"),
        }
        out.extend_from_slice(&key.window_start_ms.to_be_bytes());
        out.extend_from_slice(&count.to_be_bytes());
    }
    Ok(out)
}

fn decode_append_only_single_hop_count_updates(
    bytes: &[u8],
    value_kind: AppendOnlySingleHopValueKind,
) -> Result<Vec<(AppendOnlySingleHopKey, i64)>> {
    let mut cursor = 0;
    let tag = *read_bytes_at(
        bytes,
        &mut cursor,
        1,
        "append-only single-HOP grouped-count value kind",
    )?
    .first()
    .context("append-only single-HOP grouped-count value kind missing")?;
    if tag != append_only_single_hop_value_kind_tag(value_kind) {
        bail!("append-only single-HOP grouped-count value kind mismatch");
    }
    let update_count = read_u32_at(bytes, &mut cursor)?;
    let mut updates = Vec::with_capacity(update_count as usize);
    for _ in 0..update_count {
        let value_present = *read_bytes_at(
            bytes,
            &mut cursor,
            1,
            "append-only single-HOP grouped-count group value marker",
        )?
        .first()
        .context("append-only single-HOP grouped-count group value marker missing")?;
        let value_bytes = read_bytes_at(
            bytes,
            &mut cursor,
            8,
            "append-only single-HOP grouped-count group value",
        )?;
        let value = match value_present {
            0 => AppendOnlySingleHopValue::Null,
            1 => match value_kind {
                AppendOnlySingleHopValueKind::Int64 => {
                    AppendOnlySingleHopValue::Int64(decode_i64(value_bytes)?)
                }
                AppendOnlySingleHopValueKind::UInt64 => {
                    AppendOnlySingleHopValue::UInt64(decode_u64(value_bytes)?)
                }
            },
            _ => {
                bail!("append-only single-HOP grouped-count group value marker must be 0 or 1")
            }
        };
        let window_start_ms = decode_i64(read_bytes_at(
            bytes,
            &mut cursor,
            8,
            "append-only single-HOP grouped-count window start",
        )?)?;
        let count = decode_i64(read_bytes_at(
            bytes,
            &mut cursor,
            8,
            "append-only single-HOP grouped-count count",
        )?)?;
        updates.push((
            AppendOnlySingleHopKey {
                value,
                window_start_ms,
            },
            count,
        ));
    }
    if cursor != bytes.len() {
        bail!("append-only single-HOP grouped-count log payload has trailing bytes");
    }
    Ok(updates)
}

fn append_only_single_hop_value_kind_tag(value_kind: AppendOnlySingleHopValueKind) -> u8 {
    match value_kind {
        AppendOnlySingleHopValueKind::Int64 => 1,
        AppendOnlySingleHopValueKind::UInt64 => 2,
    }
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

fn decode_u64(bytes: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-count state value must be 8 bytes"))?;
    Ok(u64::from_be_bytes(bytes))
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
