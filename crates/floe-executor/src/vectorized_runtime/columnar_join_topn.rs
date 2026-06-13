use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, Int64Builder, StringArray, TimestampMillisecondArray, UInt32Array,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::logical_plan::{Filter, Join, TableScan, Window};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, Operator, ScalarUDF};
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarIndexedZSet, SlateBackedColumnarZSet};
use dbsp::storage::KeyValueTable;

use crate::delta_consolidation::{diff_bounded_output_batches, weighted_snapshot_schema};
use crate::encoding::EncodedRowScalar;
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::scalar_array_builder::ScalarColumnBuilder;
use crate::vectorized_runtime::source_state::resolve_source_table;

use super::{
    VectorizedMaterializedViewState, VectorizedSourceState, apply_keyed_source_snapshot_delta,
    apply_weighted_snapshot_delta, profile,
};

pub(super) struct ColumnarJoinTopNPlan {
    left_source: String,
    right_source: String,
    kind: ColumnarJoinTopNPlanKind,
}

pub(super) struct PartitionedJoinTop1ValueInput {
    pub(super) input: LogicalPlan,
    pub(super) partition_by: Vec<Expr>,
    pub(super) value_expr: Expr,
}

impl ColumnarJoinTopNPlan {
    pub(super) fn is_partitioned_best_bid(&self) -> bool {
        matches!(
            self.kind,
            ColumnarJoinTopNPlanKind::PartitionedBestBid { .. }
        )
    }
}

enum ColumnarJoinTopNPlanKind {
    PartitionedBestBid {
        left_key_column: String,
        right_key_column: String,
        left_partition_columns: Vec<String>,
        output_mapping: Option<Vec<JoinTopNOutputMappingPlan>>,
    },
}

pub(super) struct ColumnarJoinTopNMaterializedViewState {
    left: JoinTopNSourceState,
    right: JoinTopNSourceState,
    output_zset: SlateBackedColumnarZSet,
    evaluator: JoinTopNEvaluator,
    initial_snapshot: Vec<RecordBatch>,
}

impl ColumnarJoinTopNMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

pub(super) struct ColumnarJoinTopNTick {
    pub(super) delta: ColumnarZSet,
    pub(super) next_snapshot: Vec<RecordBatch>,
    pub(super) input_changed: bool,
}

struct JoinTopNSourceState {
    source_name: String,
    schema: SchemaRef,
    primary_key_columns: Vec<String>,
    key_idx: Option<usize>,
    input_zset: SlateBackedColumnarZSet,
    input_index: Option<Box<SlateBackedColumnarIndexedZSet>>,
}

enum JoinTopNEvaluator {
    PartitionedBestBid(JoinTopNBestBidEvaluator),
}

struct JoinTopNBestBidEvaluator {
    output_schema: SchemaRef,
    left: JoinTopNLeftIndices,
    right: JoinTopNRightIndices,
    partition_left_indices: Vec<usize>,
    output_mapping: Vec<JoinTopNOutputSource>,
}

struct JoinTopNLeftIndices {
    id: usize,
    item_name: usize,
    description: usize,
    initial_bid: usize,
    reserve: usize,
    date_time: usize,
    expires: usize,
    seller: usize,
    category: usize,
    extra: usize,
}

struct JoinTopNRightIndices {
    auction: usize,
    bidder: usize,
    price: usize,
    date_time: usize,
    extra: usize,
}

enum JoinTopNOutputSource {
    Left(usize),
    Right(usize),
    RowNumberOne,
}

enum JoinTopNOutputMappingPlan {
    Left(String),
    Right(String),
    RowNumberOne,
}

struct JoinTopNBestBid {
    left_batch_idx: usize,
    left_row_idx: usize,
    right_batch_idx: usize,
    right_row_idx: usize,
    price: i64,
    bid_time: i64,
    bidder: i64,
    bid_extra: Option<String>,
}

type JoinTopNPartitionKey = Vec<Option<EncodedRowScalar>>;

struct JoinTopNPreviousBestBid {
    batch_idx: usize,
    row_idx: usize,
    price: i64,
    bid_time: i64,
    bidder: i64,
    bid_extra: Option<String>,
}

struct JoinTopNOutputStateIndices {
    partition: Vec<usize>,
    price: usize,
    bid_time: usize,
    bidder: usize,
    bid_extra: usize,
}

struct JoinTopNLeftRow {
    batch_idx: usize,
    row_idx: usize,
    auction_start: i64,
    auction_expires: i64,
    partition_key: JoinTopNPartitionKey,
}

pub(super) fn columnar_join_topn_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarJoinTopNPlan>> {
    if contains_aggregate(plan) {
        return Ok(None);
    }
    if let Some(plan) = partitioned_best_bid_join_topn_plan_for_plan(plan, sources)? {
        return Ok(Some(plan));
    }
    Ok(None)
}

pub(super) fn partitioned_join_top1_value_input_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<PartitionedJoinTop1ValueInput>> {
    let Some((_, filter)) = row_number_filter_for_plan(plan) else {
        return Ok(None);
    };
    let Some((window, _)) = extract_window_plan(filter.input.as_ref()) else {
        return Ok(None);
    };
    let Some(ColumnarJoinTopNPlan {
        kind: ColumnarJoinTopNPlanKind::PartitionedBestBid { .. },
        ..
    }) = partitioned_best_bid_join_topn_plan_for_plan(plan, sources)?
    else {
        return Ok(None);
    };
    let [window_expr] = window.window_expr.as_slice() else {
        return Ok(None);
    };
    let Expr::WindowFunction(window_function) = strip_alias(window_expr) else {
        return Ok(None);
    };
    let [first_order, ..] = window_function.params.order_by.as_slice() else {
        return Ok(None);
    };
    if first_order.asc {
        return Ok(None);
    }
    Ok(Some(PartitionedJoinTop1ValueInput {
        input: window.input.as_ref().clone(),
        partition_by: window_function.params.partition_by.clone(),
        value_expr: first_order.expr.clone(),
    }))
}

fn partitioned_best_bid_join_topn_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarJoinTopNPlan>> {
    let Some((rank_column, filter)) = row_number_filter_for_plan(plan) else {
        return Ok(None);
    };
    let Some((window, projection)) = extract_window_plan(filter.input.as_ref()) else {
        return Ok(None);
    };
    if window.window_expr.len() != 1 {
        return Ok(None);
    }
    let joins = joins_for_plan(window.input.as_ref());
    let [join] = joins.as_slice() else {
        return Ok(None);
    };
    if join.join_type != JoinType::Inner || (join.on.is_empty() && join.filter.is_none()) {
        return Ok(None);
    }
    let Some(left_source) = single_source_for_plan(join.left.as_ref(), sources) else {
        return Ok(None);
    };
    let Some(right_source) = single_source_for_plan(join.right.as_ref(), sources) else {
        return Ok(None);
    };
    if left_source == right_source {
        return Ok(None);
    }
    let all_sources = source_set_for_plan(plan, sources);
    if all_sources.len() != 2
        || !all_sources.contains(&left_source)
        || !all_sources.contains(&right_source)
    {
        return Ok(None);
    }
    if contains_unsupported_join_topn_wrapper(plan) {
        return Ok(None);
    }

    let Some((left_key_column, right_key_column)) =
        join_key_columns(join, &left_source, &right_source, sources)
    else {
        return Ok(None);
    };
    let Some(left_source_state) = sources.get(&left_source) else {
        return Ok(None);
    };
    let Some(right_source_state) = sources.get(&right_source) else {
        return Ok(None);
    };
    let Some(left_partition_columns) = left_partition_columns_by_join_key(
        window,
        &left_source_state.schema,
        &right_source_state.schema,
        &left_key_column,
    ) else {
        return Ok(None);
    };
    let output_mapping = projection.as_ref().and_then(|projection| {
        output_mapping_for_projection(
            projection,
            &rank_column,
            join,
            &left_source_state.schema,
            &right_source_state.schema,
        )
    });

    Ok(Some(ColumnarJoinTopNPlan {
        left_source,
        right_source,
        kind: ColumnarJoinTopNPlanKind::PartitionedBestBid {
            left_key_column,
            right_key_column,
            left_partition_columns,
            output_mapping,
        },
    }))
}

pub(super) async fn build_columnar_join_topn_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarJoinTopNPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarJoinTopNMaterializedViewState> {
    let mv_namespace = namespaces::materialized_view(view_name)?;
    let (left_namespace, right_namespace) = if plan.left_source == plan.right_source {
        (
            format!(
                "{mv_namespace}/columnar/join_topn/left/{}/input",
                plan.left_source
            ),
            format!(
                "{mv_namespace}/columnar/join_topn/right/{}/input",
                plan.right_source
            ),
        )
    } else {
        (
            format!(
                "{mv_namespace}/columnar/join_topn/{}/input",
                plan.left_source
            ),
            format!(
                "{mv_namespace}/columnar/join_topn/{}/input",
                plan.right_source
            ),
        )
    };
    let output_namespace = format!("{mv_namespace}/columnar/join_topn/output");
    build_columnar_join_topn_materialized_view_state_in_namespaces(
        table,
        left_namespace,
        right_namespace,
        output_namespace,
        output_schema,
        plan,
        sources,
        udfs,
    )
    .await
}

pub(super) async fn build_columnar_join_topn_materialized_view_state_in_namespaces(
    table: Arc<dyn KeyValueTable>,
    left_namespace: String,
    right_namespace: String,
    output_namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarJoinTopNPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    _udfs: &[ScalarUDF],
) -> Result<ColumnarJoinTopNMaterializedViewState> {
    let left_source = sources
        .get(&plan.left_source)
        .ok_or_else(|| anyhow::anyhow!("unknown join-topn source '{}'", plan.left_source))?;
    let right_source = sources
        .get(&plan.right_source)
        .ok_or_else(|| anyhow::anyhow!("unknown join-topn source '{}'", plan.right_source))?;
    let (left_key_idx, right_key_idx) = match &plan.kind {
        ColumnarJoinTopNPlanKind::PartitionedBestBid {
            left_key_column,
            right_key_column,
            ..
        } => (
            Some(
                left_source
                    .schema
                    .index_of(left_key_column)
                    .with_context(|| format!("find join-topn left key '{left_key_column}'"))?,
            ),
            Some(
                right_source
                    .schema
                    .index_of(right_key_column)
                    .with_context(|| format!("find join-topn right key '{right_key_column}'"))?,
            ),
        ),
    };

    let left_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        left_namespace.clone(),
        Arc::clone(&left_source.schema),
    )
    .await
    .context("initialize SlateDB-backed join-topn left input zset")?;
    let right_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        right_namespace.clone(),
        Arc::clone(&right_source.schema),
    )
    .await
    .context("initialize SlateDB-backed join-topn right input zset")?;
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed join-topn output zset")?;
    let initial_snapshot = snapshot_batches_from_zset(
        &output_zset
            .materialize_columnar()
            .await
            .context("load join-topn output snapshot")?,
    )?;
    let left_snapshot_zset = left_zset
        .materialize_columnar()
        .await
        .context("load join-topn left input snapshot")?;
    let right_snapshot_zset = right_zset
        .materialize_columnar()
        .await
        .context("load join-topn right input snapshot")?;
    let mut left_index = SlateBackedColumnarIndexedZSet::new(
        Arc::clone(&table),
        format!("{left_namespace}/index"),
        Arc::clone(&left_source.schema),
        vec![left_key_idx.context("partitioned join-topn left key index is missing")?],
    )
    .await
    .context("initialize SlateDB-backed join-topn left input index")?;
    left_index
        .rebuild_from_zset(&left_snapshot_zset)
        .await
        .context("rebuild SlateDB-backed join-topn left input index")?;
    let mut right_index = SlateBackedColumnarIndexedZSet::new(
        Arc::clone(&table),
        format!("{right_namespace}/index"),
        Arc::clone(&right_source.schema),
        vec![right_key_idx.context("partitioned join-topn right key index is missing")?],
    )
    .await
    .context("initialize SlateDB-backed join-topn right input index")?;
    right_index
        .rebuild_from_zset(&right_snapshot_zset)
        .await
        .context("rebuild SlateDB-backed join-topn right input index")?;
    let left_index = Some(Box::new(left_index));
    let right_index = Some(Box::new(right_index));
    let left_name = plan.left_source;
    let right_name = plan.right_source;
    let evaluator = match plan.kind {
        ColumnarJoinTopNPlanKind::PartitionedBestBid {
            left_partition_columns,
            output_mapping,
            ..
        } => JoinTopNEvaluator::PartitionedBestBid(
            JoinTopNBestBidEvaluator::build(
                &left_source.schema,
                &right_source.schema,
                output_schema,
                &left_partition_columns,
                output_mapping.as_deref(),
            )
            .context("build join-topn vectorized evaluator")?,
        ),
    };

    Ok(ColumnarJoinTopNMaterializedViewState {
        left: JoinTopNSourceState {
            source_name: left_name,
            schema: Arc::clone(&left_source.schema),
            primary_key_columns: left_source.primary_key_columns.clone(),
            key_idx: left_key_idx,
            input_index: left_index,
            input_zset: left_zset,
        },
        right: JoinTopNSourceState {
            source_name: right_name,
            schema: Arc::clone(&right_source.schema),
            primary_key_columns: right_source.primary_key_columns.clone(),
            key_idx: right_key_idx,
            input_index: right_index,
            input_zset: right_zset,
        },
        output_zset,
        evaluator,
        initial_snapshot,
    })
}

pub(super) async fn run_columnar_join_topn_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(columnar) = mv.columnar_join_topn.as_mut() else {
        return Ok(false);
    };
    let plan_start = Instant::now();
    let tick = run_columnar_join_topn_state_tick(
        columnar,
        insert_batches,
        weighted_delta_batches,
        &mv.output_schema,
        &mv.previous_snapshot,
    )
    .await?;

    let delta_batches = tick.delta.batches().to_vec();
    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, tick.next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = tick.next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "columnar_join_topn",
        "SlateDB-backed join-topn columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

pub(super) async fn run_columnar_join_topn_state_tick(
    columnar: &mut ColumnarJoinTopNMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    output_schema: &SchemaRef,
    previous_snapshot: &[RecordBatch],
) -> Result<ColumnarJoinTopNTick> {
    let total_start = profile::start();
    let phase_start = profile::start();
    let left_input_delta =
        source_input_delta(&columnar.left, insert_batches, weighted_delta_batches)?;
    let right_input_delta =
        source_input_delta(&columnar.right, insert_batches, weighted_delta_batches)?;
    profile::record_since("join_topn.source_input_delta", phase_start);
    let phase_start = profile::start();
    let left_delta =
        persisted_source_delta(&mut columnar.left.input_zset, left_input_delta).await?;
    let right_delta =
        persisted_source_delta(&mut columnar.right.input_zset, right_input_delta).await?;
    profile::record_since("join_topn.persist_source_delta", phase_start);
    let input_changed = !left_delta.batches().is_empty() || !right_delta.batches().is_empty();

    let output_delta_batches = match &columnar.evaluator {
        JoinTopNEvaluator::PartitionedBestBid(evaluator) => {
            let phase_start = profile::start();
            let left_key_idx = columnar
                .left
                .key_idx
                .context("partitioned join-topn left key index is missing")?;
            let right_key_idx = columnar
                .right
                .key_idx
                .context("partitioned join-topn right key index is missing")?;
            let mut touched_keys = HashSet::new();
            collect_i64_keys_from_delta(&left_delta, left_key_idx, &mut touched_keys)?;
            collect_i64_keys_from_delta(&right_delta, right_key_idx, &mut touched_keys)?;
            profile::record_since("join_topn.collect_touched_keys", phase_start);

            if left_delta.is_empty()
                && columnar_zset_is_insert_only(&right_delta)?
                && let Some(output_state_indices) = evaluator.output_state_indices()
            {
                let phase_start = profile::start();
                let previous_left = lookup_indexed_join_topn_state_for_i64_keys(
                    columnar
                        .left
                        .input_index
                        .as_deref()
                        .context("partitioned join-topn left source index missing")?,
                    &touched_keys,
                    &columnar.left.schema,
                    "left",
                )
                .await?;
                profile::record_since("join_topn.lookup_previous_left", phase_start);
                let phase_start = profile::start();
                let output_delta = evaluator
                    .append_only_right_output_delta(
                        output_schema,
                        &previous_left,
                        &right_delta,
                        previous_snapshot,
                        &output_state_indices,
                    )
                    .context("build append-only right join-topn output delta")?;
                profile::record_since("join_topn.append_only_right_merge", phase_start);
                output_delta
            } else {
                let phase_start = profile::start();
                let previous_left = lookup_indexed_join_topn_state_for_i64_keys(
                    columnar
                        .left
                        .input_index
                        .as_deref()
                        .context("partitioned join-topn left source index missing")?,
                    &touched_keys,
                    &columnar.left.schema,
                    "left",
                )
                .await?;
                profile::record_since("join_topn.lookup_previous_left", phase_start);
                let phase_start = profile::start();
                let previous_right = lookup_indexed_join_topn_state_for_i64_keys(
                    columnar
                        .right
                        .input_index
                        .as_deref()
                        .context("partitioned join-topn right source index missing")?,
                    &touched_keys,
                    &columnar.right.schema,
                    "right",
                )
                .await?;
                profile::record_since("join_topn.lookup_previous_right", phase_start);
                let phase_start = profile::start();
                let next_left = apply_source_snapshot_delta(
                    &columnar.left.schema,
                    &columnar.left.primary_key_columns,
                    &previous_left,
                    &left_delta,
                )
                .await?;
                let next_right = apply_source_snapshot_delta(
                    &columnar.right.schema,
                    &columnar.right.primary_key_columns,
                    &previous_right,
                    &right_delta,
                )
                .await?;
                profile::record_since("join_topn.apply_input_delta", phase_start);

                let (previous_output, next_output) = if touched_keys.is_empty() {
                    (Vec::new(), Vec::new())
                } else {
                    let phase_start = profile::start();
                    let previous_output = evaluator
                        .evaluate(&previous_left, &previous_right)
                        .await
                        .context("evaluate previous join-topn partition outputs")?;
                    profile::record_since("join_topn.evaluate_previous", phase_start);
                    let phase_start = profile::start();
                    let next_output = evaluator
                        .evaluate(&next_left, &next_right)
                        .await
                        .context("evaluate next join-topn partition outputs")?;
                    profile::record_since("join_topn.evaluate_next", phase_start);
                    (previous_output, next_output)
                };
                let phase_start = profile::start();
                let diff = diff_bounded_output_batches(
                    Arc::clone(output_schema),
                    &previous_output,
                    &next_output,
                )
                .await
                .context("diff join-topn partition outputs")?;
                profile::record_since("join_topn.diff_output", phase_start);
                diff.batches
            }
        }
    };

    let phase_start = profile::start();
    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build join-topn output zset delta")?;
    profile::record_since("join_topn.build_output_zset", phase_start);
    let phase_start = profile::start();
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
    profile::record_since("join_topn.output_create_version", phase_start);

    let phase_start = profile::start();
    let delta_batches = persisted_output_delta.batches().to_vec();
    let next_snapshot =
        apply_weighted_snapshot_delta(output_schema, previous_snapshot, delta_batches.clone())
            .await
            .context("apply Slate-backed join-topn columnar snapshot delta")?;
    profile::record_since("join_topn.output_snapshot_delta", phase_start);

    let phase_start = profile::start();
    if let Some(index) = columnar.left.input_index.as_deref_mut() {
        index
            .apply_delta(&left_delta)
            .await
            .context("apply left join-topn delta to SlateDB-backed columnar index")?;
    }
    if let Some(index) = columnar.right.input_index.as_deref_mut() {
        index
            .apply_delta(&right_delta)
            .await
            .context("apply right join-topn delta to SlateDB-backed columnar index")?;
    }
    profile::record_since("join_topn.update_indexes", phase_start);
    profile::record_since("join_topn.total", total_start);
    Ok(ColumnarJoinTopNTick {
        delta: persisted_output_delta,
        next_snapshot,
        input_changed,
    })
}

fn source_input_delta(
    source: &JoinTopNSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    if let Some(weighted_batches) = weighted_delta_batches.get(source.source_name.as_str()) {
        ColumnarZSet::try_new_weighted(Arc::clone(&source.schema), weighted_batches.clone())
            .with_context(|| {
                format!(
                    "build weighted join-topn input delta for '{}'",
                    source.source_name
                )
            })
    } else if let Some(source_batches) = insert_batches.get(source.source_name.as_str()) {
        ColumnarZSet::from_value_batches(Arc::clone(&source.schema), source_batches.clone(), 1)
            .with_context(|| {
                format!(
                    "build insert join-topn input delta for '{}'",
                    source.source_name
                )
            })
    } else {
        ColumnarZSet::empty(Arc::clone(&source.schema))
    }
}

async fn persisted_source_delta(
    zset: &mut SlateBackedColumnarZSet,
    input_delta: ColumnarZSet,
) -> Result<ColumnarZSet> {
    let base = zset.current_handle().map(|handle| handle.version);
    if let Some(handle) = zset.create_version(&input_delta, base).await? {
        zset.read_delta(&handle).await
    } else {
        Ok(input_delta)
    }
}

async fn apply_source_snapshot_delta(
    schema: &SchemaRef,
    primary_key_columns: &[String],
    previous: &[RecordBatch],
    delta: &ColumnarZSet,
) -> Result<Vec<RecordBatch>> {
    if delta.batches().is_empty() {
        return Ok(previous.to_vec());
    }
    apply_keyed_source_snapshot_delta(
        schema,
        primary_key_columns,
        previous,
        delta.batches().to_vec(),
    )
    .await
}

fn collect_i64_keys_from_delta(
    delta: &ColumnarZSet,
    key_idx: usize,
    output: &mut HashSet<i64>,
) -> Result<()> {
    let weight_idx = delta.value_column_count();
    for batch in delta.batches().iter().filter(|batch| batch.num_rows() > 0) {
        let keys = batch
            .column(key_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("join-topn key column must be Int64"))?;
        let weights = batch
            .column(weight_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("columnar zset weight column must be Int64"))?;
        for row_idx in 0..batch.num_rows() {
            if keys.is_null(row_idx) || weights.is_null(row_idx) || weights.value(row_idx) == 0 {
                continue;
            }
            output.insert(keys.value(row_idx));
        }
    }
    Ok(())
}

fn columnar_zset_is_insert_only(delta: &ColumnarZSet) -> Result<bool> {
    if delta.is_empty() {
        return Ok(false);
    }
    let weight_idx = delta.value_column_count();
    for batch in delta.batches() {
        let weights = batch
            .column(weight_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("columnar zset weight column must be Int64"))?;
        for row_idx in 0..batch.num_rows() {
            if weights.is_null(row_idx) || weights.value(row_idx) <= 0 {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

async fn lookup_indexed_join_topn_state_for_i64_keys(
    index: &SlateBackedColumnarIndexedZSet,
    keys: &HashSet<i64>,
    state_schema: &SchemaRef,
    side: &str,
) -> Result<Vec<RecordBatch>> {
    let key_batches = i64_lookup_key_batches(keys, &index.key_schema())
        .with_context(|| format!("build {side} join-topn lookup keys"))?;
    let weighted_lookup = index
        .lookup_key_batches(&key_batches)
        .await
        .with_context(|| format!("lookup {side} join-topn state by indexed keys"))?;
    if weighted_lookup.is_empty() {
        return Ok(Vec::new());
    }
    apply_weighted_snapshot_delta(state_schema, &[], weighted_lookup.batches().to_vec())
        .await
        .with_context(|| format!("materialize {side} indexed join-topn state lookup"))
}

fn i64_lookup_key_batches(keys: &HashSet<i64>, key_schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    if key_schema.fields().len() != 1 {
        bail!("partitioned join-topn indexed lookup requires one key column");
    }
    if !matches!(key_schema.field(0).data_type(), DataType::Int64) {
        bail!("partitioned join-topn indexed lookup key must be Int64");
    }
    let mut values = keys.iter().copied().collect::<Vec<_>>();
    values.sort_unstable();
    let batch = RecordBatch::try_new(
        Arc::clone(key_schema),
        vec![Arc::new(Int64Array::from(values)) as ArrayRef],
    )?;
    Ok(vec![batch])
}

impl JoinTopNBestBidEvaluator {
    fn build(
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        output_schema: &SchemaRef,
        left_partition_columns: &[String],
        output_mapping_plan: Option<&[JoinTopNOutputMappingPlan]>,
    ) -> Result<Self> {
        let left = JoinTopNLeftIndices {
            id: field_index(left_schema, &["id"])?,
            item_name: field_index(left_schema, &["itemName", "item_name"])?,
            description: field_index(left_schema, &["description"])?,
            initial_bid: field_index(left_schema, &["initialBid", "initial_bid"])?,
            reserve: field_index(left_schema, &["reserve"])?,
            date_time: field_index(left_schema, &["dateTime", "date_time"])?,
            expires: field_index(left_schema, &["expires"])?,
            seller: field_index(left_schema, &["seller"])?,
            category: field_index(left_schema, &["category"])?,
            extra: field_index(left_schema, &["extra"])?,
        };
        let right = JoinTopNRightIndices {
            auction: field_index(right_schema, &["auction"])?,
            bidder: field_index(right_schema, &["bidder"])?,
            price: field_index(right_schema, &["price"])?,
            date_time: field_index(right_schema, &["dateTime", "date_time"])?,
            extra: field_index(right_schema, &["extra"])?,
        };
        let partition_left_indices = left_partition_columns
            .iter()
            .map(|column| {
                left_schema
                    .index_of(column)
                    .with_context(|| format!("find join-topn partition column '{column}'"))
            })
            .collect::<Result<Vec<_>>>()?;
        let output_mapping = match output_mapping_plan {
            Some(mapping_plan) if mapping_plan.len() == output_schema.fields().len() => {
                output_mapping_from_plan(mapping_plan, left_schema, right_schema)
                    .or_else(|_| name_based_output_mapping(output_schema, &left, &right))?
            }
            _ => name_based_output_mapping(output_schema, &left, &right)?,
        };
        Ok(Self {
            output_schema: Arc::clone(output_schema),
            left,
            right,
            partition_left_indices,
            output_mapping,
        })
    }

    async fn evaluate(
        &self,
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
    ) -> Result<Vec<RecordBatch>> {
        let left_rows = self.left_rows_by_auction(left_batches)?;
        if left_rows.is_empty() {
            return Ok(Vec::new());
        }
        let best_by_partition = self.best_bids_by_partition(&left_rows, right_batches)?;
        if best_by_partition.is_empty() {
            return Ok(Vec::new());
        }

        let mut builders = self
            .output_schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), best_by_partition.len()))
            .collect::<Result<Vec<_>>>()?;

        let mut partition_keys = best_by_partition.keys().cloned().collect::<Vec<_>>();
        partition_keys.sort();
        for partition_key in partition_keys {
            let best = best_by_partition
                .get(&partition_key)
                .expect("best bid missing for partition key");
            self.append_output_row(&mut builders, left_batches, right_batches, best)?;
        }

        let columns = builders
            .iter_mut()
            .map(ScalarColumnBuilder::finish_array)
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), columns)?;
        if batch.num_rows() == 0 {
            Ok(Vec::new())
        } else {
            Ok(vec![batch])
        }
    }

    fn append_only_right_output_delta(
        &self,
        output_schema: &SchemaRef,
        left_batches: &[RecordBatch],
        right_delta: &ColumnarZSet,
        previous_snapshot: &[RecordBatch],
        output_state_indices: &JoinTopNOutputStateIndices,
    ) -> Result<Vec<RecordBatch>> {
        let left_rows = self.left_rows_by_auction(left_batches)?;
        if left_rows.is_empty() {
            return Ok(Vec::new());
        }
        let candidate_best = self.best_bids_by_partition(&left_rows, right_delta.batches())?;
        if candidate_best.is_empty() {
            return Ok(Vec::new());
        }
        let previous_best = previous_best_bids_by_partition(
            previous_snapshot,
            output_state_indices,
            candidate_best.keys(),
        )?;

        let capacity = candidate_best.len().saturating_mul(2);
        let mut builders = output_schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), capacity))
            .collect::<Result<Vec<_>>>()?;
        let mut weights = Int64Builder::with_capacity(capacity);
        let mut row_count = 0usize;

        let mut partition_keys = candidate_best.keys().cloned().collect::<Vec<_>>();
        partition_keys.sort();
        for partition_key in partition_keys {
            let candidate = candidate_best
                .get(&partition_key)
                .expect("candidate best bid missing for partition key");
            if let Some(previous) = previous_best.get(&partition_key) {
                if !candidate_orders_before_previous(candidate, previous) {
                    continue;
                }
                append_existing_output_row(
                    &mut builders,
                    &previous_snapshot[previous.batch_idx],
                    previous.row_idx,
                )?;
                weights.append_value(-1);
                row_count += 1;
            }
            self.append_output_row(
                &mut builders,
                left_batches,
                right_delta.batches(),
                candidate,
            )?;
            weights.append_value(1);
            row_count += 1;
        }

        let value_columns = builders
            .iter_mut()
            .map(ScalarColumnBuilder::finish_array)
            .collect::<Vec<_>>();
        if row_count == 0 {
            return Ok(Vec::new());
        }
        let mut columns = value_columns;
        columns.push(Arc::new(weights.finish()) as ArrayRef);
        let batch = RecordBatch::try_new(weighted_snapshot_schema(output_schema)?, columns)?;
        Ok(vec![batch])
    }

    fn output_state_indices(&self) -> Option<JoinTopNOutputStateIndices> {
        let mut partition = Vec::with_capacity(self.partition_left_indices.len());
        for partition_idx in &self.partition_left_indices {
            let output_idx = self.output_mapping.iter().position(
                |source| matches!(source, JoinTopNOutputSource::Left(idx) if idx == partition_idx),
            )?;
            partition.push(output_idx);
        }
        let price = self.output_mapping.iter().position(
            |source| matches!(source, JoinTopNOutputSource::Right(idx) if *idx == self.right.price),
        )?;
        let bid_time = self.output_mapping.iter().position(|source| {
            matches!(source, JoinTopNOutputSource::Right(idx) if *idx == self.right.date_time)
        })?;
        let bidder = self.output_mapping.iter().position(|source| {
            matches!(source, JoinTopNOutputSource::Right(idx) if *idx == self.right.bidder)
        })?;
        let bid_extra = self.output_mapping.iter().position(
            |source| matches!(source, JoinTopNOutputSource::Right(idx) if *idx == self.right.extra),
        )?;
        Some(JoinTopNOutputStateIndices {
            partition,
            price,
            bid_time,
            bidder,
            bid_extra,
        })
    }

    fn left_rows_by_auction(
        &self,
        left_batches: &[RecordBatch],
    ) -> Result<HashMap<i64, Vec<JoinTopNLeftRow>>> {
        let mut left_rows = HashMap::new();
        for (left_batch_idx, left_batch) in left_batches.iter().enumerate() {
            let left_ids = int64_column(left_batch, self.left.id)?;
            for left_row_idx in 0..left_batch.num_rows() {
                if left_ids.is_null(left_row_idx) {
                    continue;
                }
                let auction_id = left_ids.value(left_row_idx);
                let auction_start =
                    i64_or_timestamp_value(left_batch, self.left.date_time, left_row_idx)?;
                let auction_expires =
                    i64_or_timestamp_value(left_batch, self.left.expires, left_row_idx)?;
                let partition_key = self.left_partition_key(left_batch, left_row_idx)?;
                left_rows
                    .entry(auction_id)
                    .or_insert_with(Vec::new)
                    .push(JoinTopNLeftRow {
                        batch_idx: left_batch_idx,
                        row_idx: left_row_idx,
                        auction_start,
                        auction_expires,
                        partition_key,
                    });
            }
        }
        Ok(left_rows)
    }

    fn left_partition_key(
        &self,
        left_batch: &RecordBatch,
        left_row_idx: usize,
    ) -> Result<JoinTopNPartitionKey> {
        self.partition_left_indices
            .iter()
            .map(|idx| encoded_scalar(left_batch, *idx, left_row_idx))
            .collect()
    }

    fn best_bids_by_partition(
        &self,
        left_rows: &HashMap<i64, Vec<JoinTopNLeftRow>>,
        right_batches: &[RecordBatch],
    ) -> Result<HashMap<JoinTopNPartitionKey, JoinTopNBestBid>> {
        let mut best_by_partition = HashMap::with_capacity(left_rows.len());
        for (right_batch_idx, right_batch) in right_batches.iter().enumerate() {
            let right_auctions = int64_column(right_batch, self.right.auction)?;
            let right_bidders = int64_column(right_batch, self.right.bidder)?;
            let right_prices = int64_column(right_batch, self.right.price)?;
            let right_extras = string_column(right_batch, self.right.extra)?;
            for right_row_idx in 0..right_batch.num_rows() {
                if right_auctions.is_null(right_row_idx)
                    || right_bidders.is_null(right_row_idx)
                    || right_prices.is_null(right_row_idx)
                {
                    continue;
                }
                let auction_id = right_auctions.value(right_row_idx);
                let Some(left_matches) = left_rows.get(&auction_id) else {
                    continue;
                };
                let bid_time =
                    i64_or_timestamp_value(right_batch, self.right.date_time, right_row_idx)?;
                let price = right_prices.value(right_row_idx);
                let bidder = right_bidders.value(right_row_idx);
                let bid_extra = if right_extras.is_null(right_row_idx) {
                    None
                } else {
                    Some(right_extras.value(right_row_idx))
                };
                for left in left_matches {
                    if bid_time < left.auction_start || bid_time > left.auction_expires {
                        continue;
                    }
                    let replace =
                        best_by_partition
                            .get(&left.partition_key)
                            .is_none_or(|current| {
                                bid_orders_before(price, bid_time, bidder, bid_extra, current)
                            });
                    if replace {
                        best_by_partition.insert(
                            left.partition_key.clone(),
                            JoinTopNBestBid {
                                left_batch_idx: left.batch_idx,
                                left_row_idx: left.row_idx,
                                right_batch_idx,
                                right_row_idx,
                                price,
                                bid_time,
                                bidder,
                                bid_extra: bid_extra.map(str::to_owned),
                            },
                        );
                    }
                }
            }
        }
        Ok(best_by_partition)
    }

    fn append_output_row(
        &self,
        builders: &mut [ScalarColumnBuilder],
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
        best: &JoinTopNBestBid,
    ) -> Result<()> {
        let left = &left_batches[best.left_batch_idx];
        let right = &right_batches[best.right_batch_idx];
        for (builder, source) in builders.iter_mut().zip(self.output_mapping.iter()) {
            let value = match source {
                JoinTopNOutputSource::Left(idx) => encoded_scalar(left, *idx, best.left_row_idx)?,
                JoinTopNOutputSource::Right(idx) => {
                    encoded_scalar(right, *idx, best.right_row_idx)?
                }
                JoinTopNOutputSource::RowNumberOne => {
                    builder.append_u64_value(1)?;
                    continue;
                }
            };
            builder.append_encoded_scalar(value.as_ref())?;
        }
        Ok(())
    }
}

fn previous_best_bids_by_partition<'a, I>(
    previous_snapshot: &[RecordBatch],
    output_state_indices: &JoinTopNOutputStateIndices,
    candidate_keys: I,
) -> Result<HashMap<JoinTopNPartitionKey, JoinTopNPreviousBestBid>>
where
    I: Iterator<Item = &'a JoinTopNPartitionKey>,
{
    let wanted = candidate_keys.cloned().collect::<HashSet<_>>();
    if wanted.is_empty() {
        return Ok(HashMap::new());
    }
    let mut previous = HashMap::with_capacity(wanted.len());
    for (batch_idx, batch) in previous_snapshot.iter().enumerate() {
        for row_idx in 0..batch.num_rows() {
            let partition_key = output_partition_key(batch, row_idx, output_state_indices)?;
            if !wanted.contains(&partition_key) {
                continue;
            }
            let price = i64_or_timestamp_value(batch, output_state_indices.price, row_idx)?;
            let bid_time = i64_or_timestamp_value(batch, output_state_indices.bid_time, row_idx)?;
            let bidder = i64_or_timestamp_value(batch, output_state_indices.bidder, row_idx)?;
            let bid_extra_values = string_column(batch, output_state_indices.bid_extra)?;
            let bid_extra = if bid_extra_values.is_null(row_idx) {
                None
            } else {
                Some(bid_extra_values.value(row_idx).to_string())
            };
            previous.insert(
                partition_key,
                JoinTopNPreviousBestBid {
                    batch_idx,
                    row_idx,
                    price,
                    bid_time,
                    bidder,
                    bid_extra,
                },
            );
        }
    }
    Ok(previous)
}

fn output_partition_key(
    batch: &RecordBatch,
    row_idx: usize,
    output_state_indices: &JoinTopNOutputStateIndices,
) -> Result<JoinTopNPartitionKey> {
    output_state_indices
        .partition
        .iter()
        .map(|idx| encoded_scalar(batch, *idx, row_idx))
        .collect()
}

fn append_existing_output_row(
    builders: &mut [ScalarColumnBuilder],
    batch: &RecordBatch,
    row_idx: usize,
) -> Result<()> {
    for (idx, builder) in builders.iter_mut().enumerate() {
        let value = encoded_scalar(batch, idx, row_idx)?;
        builder.append_encoded_scalar(value.as_ref())?;
    }
    Ok(())
}

fn candidate_orders_before_previous(
    candidate: &JoinTopNBestBid,
    previous: &JoinTopNPreviousBestBid,
) -> bool {
    candidate.price > previous.price
        || (candidate.price == previous.price
            && (candidate.bid_time < previous.bid_time
                || (candidate.bid_time == previous.bid_time
                    && (candidate.bidder < previous.bidder
                        || (candidate.bidder == previous.bidder
                            && optional_str_cmp_asc(
                                candidate.bid_extra.as_deref(),
                                previous.bid_extra.as_deref(),
                            ) == Ordering::Less)))))
}

fn output_mapping_from_plan(
    mapping_plan: &[JoinTopNOutputMappingPlan],
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
) -> Result<Vec<JoinTopNOutputSource>> {
    mapping_plan
        .iter()
        .map(|source| match source {
            JoinTopNOutputMappingPlan::Left(column) => left_schema
                .index_of(column)
                .map(JoinTopNOutputSource::Left)
                .with_context(|| format!("find join-topn left output column '{column}'")),
            JoinTopNOutputMappingPlan::Right(column) => right_schema
                .index_of(column)
                .map(JoinTopNOutputSource::Right)
                .with_context(|| format!("find join-topn right output column '{column}'")),
            JoinTopNOutputMappingPlan::RowNumberOne => Ok(JoinTopNOutputSource::RowNumberOne),
        })
        .collect()
}

fn name_based_output_mapping(
    output_schema: &SchemaRef,
    left: &JoinTopNLeftIndices,
    right: &JoinTopNRightIndices,
) -> Result<Vec<JoinTopNOutputSource>> {
    output_schema
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            "id" => Ok(JoinTopNOutputSource::Left(left.id)),
            "itemName" => Ok(JoinTopNOutputSource::Left(left.item_name)),
            "description" => Ok(JoinTopNOutputSource::Left(left.description)),
            "initialBid" => Ok(JoinTopNOutputSource::Left(left.initial_bid)),
            "reserve" => Ok(JoinTopNOutputSource::Left(left.reserve)),
            "dateTime" => Ok(JoinTopNOutputSource::Left(left.date_time)),
            "expires" => Ok(JoinTopNOutputSource::Left(left.expires)),
            "seller" => Ok(JoinTopNOutputSource::Left(left.seller)),
            "category" => Ok(JoinTopNOutputSource::Left(left.category)),
            "extra" => Ok(JoinTopNOutputSource::Left(left.extra)),
            "auction" => Ok(JoinTopNOutputSource::Right(right.auction)),
            "bidder" => Ok(JoinTopNOutputSource::Right(right.bidder)),
            "price" => Ok(JoinTopNOutputSource::Right(right.price)),
            "bidTime" => Ok(JoinTopNOutputSource::Right(right.date_time)),
            "bidExtra" => Ok(JoinTopNOutputSource::Right(right.extra)),
            "rownum" | "rn" | "rank_number" => Ok(JoinTopNOutputSource::RowNumberOne),
            other => bail!("unsupported join-topn output field '{other}'"),
        })
        .collect()
}

fn bid_orders_before(
    price: i64,
    bid_time: i64,
    bidder: i64,
    bid_extra: Option<&str>,
    current: &JoinTopNBestBid,
) -> bool {
    price > current.price
        || (price == current.price
            && (bid_time < current.bid_time
                || (bid_time == current.bid_time
                    && (bidder < current.bidder
                        || (bidder == current.bidder
                            && optional_str_cmp_asc(bid_extra, current.bid_extra.as_deref())
                                == Ordering::Less)))))
}

fn optional_str_cmp_asc(left: Option<&str>, right: Option<&str>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => left.cmp(right),
    }
}

fn int64_column(batch: &RecordBatch, idx: usize) -> Result<&Int64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("join-topn column must be Int64"))
}

fn string_column(batch: &RecordBatch, idx: usize) -> Result<&StringArray> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("join-topn column must be Utf8"))
}

fn field_index(schema: &SchemaRef, names: &[&str]) -> Result<usize> {
    for name in names {
        if let Ok(idx) = schema.index_of(name) {
            return Ok(idx);
        }
    }
    bail!("join-topn schema missing field aliases {names:?}")
}

fn i64_or_timestamp_value(batch: &RecordBatch, idx: usize, row_idx: usize) -> Result<i64> {
    match batch.schema().field(idx).data_type() {
        DataType::Int64 => {
            let values = int64_column(batch, idx)?;
            if values.is_null(row_idx) {
                bail!("join-topn time column cannot be NULL");
            }
            Ok(values.value(row_idx))
        }
        DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, _) => {
            let values = batch
                .column(idx)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| anyhow::anyhow!("join-topn column must be TimestampMillis"))?;
            if values.is_null(row_idx) {
                bail!("join-topn timestamp column cannot be NULL");
            }
            Ok(values.value(row_idx))
        }
        other => bail!("join-topn unsupported time column type {other:?}"),
    }
}

fn encoded_scalar(
    batch: &RecordBatch,
    idx: usize,
    row_idx: usize,
) -> Result<Option<EncodedRowScalar>> {
    if batch.column(idx).is_null(row_idx) {
        return Ok(None);
    }
    match batch.schema().field(idx).data_type() {
        DataType::Int64 => Ok(Some(EncodedRowScalar::Int64(
            int64_column(batch, idx)?.value(row_idx),
        ))),
        DataType::Utf8 => {
            let values = batch
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("join-topn column must be Utf8"))?;
            Ok(Some(EncodedRowScalar::Utf8(
                values.value(row_idx).to_string(),
            )))
        }
        DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, _) => {
            let values = batch
                .column(idx)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| anyhow::anyhow!("join-topn column must be TimestampMillis"))?;
            Ok(Some(EncodedRowScalar::TimestampMillis(
                values.value(row_idx),
            )))
        }
        other => bail!("join-topn unsupported output column type {other:?}"),
    }
}

fn row_number_filter_for_plan(plan: &LogicalPlan) -> Option<(String, &Filter)> {
    match plan {
        LogicalPlan::Projection(projection) => {
            row_number_filter_for_plan(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => {
            let rank_column = extract_row_number_limit_column(&filter.predicate)?;
            Some((rank_column, filter))
        }
        LogicalPlan::SubqueryAlias(alias) => row_number_filter_for_plan(alias.input.as_ref()),
        _ => None,
    }
}

fn extract_row_number_limit_column(predicate: &Expr) -> Option<String> {
    let Expr::BinaryExpr(binary) = predicate else {
        return None;
    };
    match (&*binary.left, binary.op, &*binary.right) {
        (Expr::Column(column), Operator::LtEq, literal @ Expr::Literal(_, _))
            if literal_to_i128(literal) == Some(1) =>
        {
            Some(column.name.clone())
        }
        (Expr::Column(column), Operator::Lt, literal @ Expr::Literal(_, _))
            if literal_to_i128(literal) == Some(2) =>
        {
            Some(column.name.clone())
        }
        (literal @ Expr::Literal(_, _), Operator::GtEq, Expr::Column(column))
            if literal_to_i128(literal) == Some(1) =>
        {
            Some(column.name.clone())
        }
        (literal @ Expr::Literal(_, _), Operator::Gt, Expr::Column(column))
            if literal_to_i128(literal) == Some(0) =>
        {
            Some(column.name.clone())
        }
        (Expr::Column(column), Operator::Eq, literal @ Expr::Literal(_, _))
        | (literal @ Expr::Literal(_, _), Operator::Eq, Expr::Column(column))
            if literal_to_i128(literal) == Some(1) =>
        {
            Some(column.name.clone())
        }
        _ => None,
    }
}

fn literal_to_i128(expr: &Expr) -> Option<i128> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    match value {
        ScalarValue::Int8(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt8(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt64(Some(value)) => Some(i128::from(*value)),
        _ => None,
    }
}

fn extract_window_plan(input: &LogicalPlan) -> Option<(&Window, Option<Vec<Expr>>)> {
    let direct = strip_passthrough_wrappers(input);
    if let LogicalPlan::Window(window) = direct {
        return Some((window, None));
    }

    let projection = match direct {
        LogicalPlan::Projection(projection) => projection,
        _ => return None,
    };
    let window = match strip_passthrough_wrappers(projection.input.as_ref()) {
        LogicalPlan::Window(window) => window,
        _ => return None,
    };
    Some((window, Some(projection.expr.clone())))
}

fn output_mapping_for_projection(
    projection: &[Expr],
    rank_column: &str,
    join: &Join,
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
) -> Option<Vec<JoinTopNOutputMappingPlan>> {
    let left_relations = relation_names_for_plan(join.left.as_ref());
    let right_relations = relation_names_for_plan(join.right.as_ref());
    projection
        .iter()
        .map(|expr| {
            output_mapping_for_projection_expr(
                expr,
                rank_column,
                left_schema,
                right_schema,
                &left_relations,
                &right_relations,
            )
        })
        .collect()
}

fn output_mapping_for_projection_expr(
    expr: &Expr,
    rank_column: &str,
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    left_relations: &BTreeSet<String>,
    right_relations: &BTreeSet<String>,
) -> Option<JoinTopNOutputMappingPlan> {
    match expr {
        Expr::Column(column) if column.name == rank_column => {
            Some(JoinTopNOutputMappingPlan::RowNumberOne)
        }
        Expr::Column(column) => {
            if let Some(relation) = column.relation.as_ref().map(ToString::to_string) {
                let in_left = left_relations.contains(&relation);
                let in_right = right_relations.contains(&relation);
                return match (in_left, in_right) {
                    (true, false) => Some(JoinTopNOutputMappingPlan::Left(column.name.clone())),
                    (false, true) => Some(JoinTopNOutputMappingPlan::Right(column.name.clone())),
                    _ => None,
                };
            }
            let in_left = left_schema.index_of(&column.name).is_ok();
            let in_right = right_schema.index_of(&column.name).is_ok();
            match (in_left, in_right) {
                (true, false) => Some(JoinTopNOutputMappingPlan::Left(column.name.clone())),
                (false, true) => Some(JoinTopNOutputMappingPlan::Right(column.name.clone())),
                _ => None,
            }
        }
        Expr::Alias(alias) if alias.name == rank_column => {
            Some(JoinTopNOutputMappingPlan::RowNumberOne)
        }
        Expr::Alias(alias) => output_mapping_for_projection_expr(
            alias.expr.as_ref(),
            rank_column,
            left_schema,
            right_schema,
            left_relations,
            right_relations,
        ),
        _ => None,
    }
}

fn relation_names_for_plan(plan: &LogicalPlan) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    collect_relation_names(plan, &mut out);
    out
}

fn collect_relation_names(plan: &LogicalPlan, out: &mut BTreeSet<String>) {
    match plan {
        LogicalPlan::TableScan(scan) => {
            out.insert(scan.table_name.to_string());
            out.insert(scan.table_name.table().to_string());
        }
        LogicalPlan::SubqueryAlias(alias) => {
            out.insert(alias.alias.to_string());
            out.insert(alias.alias.table().to_string());
            collect_relation_names(alias.input.as_ref(), out);
        }
        LogicalPlan::Projection(projection) => {
            collect_relation_names(projection.input.as_ref(), out)
        }
        LogicalPlan::Filter(filter) => collect_relation_names(filter.input.as_ref(), out),
        LogicalPlan::Repartition(repartition) => {
            collect_relation_names(repartition.input.as_ref(), out)
        }
        LogicalPlan::Window(window) => collect_relation_names(window.input.as_ref(), out),
        LogicalPlan::Sort(sort) => collect_relation_names(sort.input.as_ref(), out),
        LogicalPlan::Limit(limit) => collect_relation_names(limit.input.as_ref(), out),
        _ => {}
    }
}

fn strip_passthrough_wrappers(mut plan: &LogicalPlan) -> &LogicalPlan {
    loop {
        match plan {
            LogicalPlan::SubqueryAlias(alias) => {
                plan = alias.input.as_ref();
            }
            LogicalPlan::Repartition(repartition) => {
                plan = repartition.input.as_ref();
            }
            _ => return plan,
        }
    }
}

fn joins_for_plan<'a>(plan: &'a LogicalPlan) -> Vec<&'a Join> {
    let mut joins = Vec::new();
    collect_joins(plan, &mut joins);
    joins
}

fn collect_joins<'a>(plan: &'a LogicalPlan, joins: &mut Vec<&'a Join>) {
    match plan {
        LogicalPlan::Join(join) => {
            joins.push(join);
            collect_joins(join.left.as_ref(), joins);
            collect_joins(join.right.as_ref(), joins);
        }
        LogicalPlan::Projection(projection) => collect_joins(projection.input.as_ref(), joins),
        LogicalPlan::Filter(filter) => collect_joins(filter.input.as_ref(), joins),
        LogicalPlan::SubqueryAlias(alias) => collect_joins(alias.input.as_ref(), joins),
        LogicalPlan::Sort(sort) => collect_joins(sort.input.as_ref(), joins),
        LogicalPlan::Limit(limit) => collect_joins(limit.input.as_ref(), joins),
        LogicalPlan::Window(window) => collect_joins(window.input.as_ref(), joins),
        _ => {}
    }
}

fn join_key_columns(
    join: &Join,
    left_source: &str,
    right_source: &str,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Option<(String, String)> {
    let mut candidates = Vec::new();
    for (left, right) in &join.on {
        let (Some(left), Some(right)) = (column_name(left), column_name(right)) else {
            continue;
        };
        candidates.push((left, right));
    }
    if let Some(filter) = join.filter.as_ref() {
        collect_equality_column_pairs(filter, &mut candidates);
    }
    let left_schema = &sources.get(left_source)?.schema;
    let right_schema = &sources.get(right_source)?.schema;
    for (first, second) in candidates {
        let first_left = left_schema.index_of(&first).is_ok();
        let first_right = right_schema.index_of(&first).is_ok();
        let second_left = left_schema.index_of(&second).is_ok();
        let second_right = right_schema.index_of(&second).is_ok();
        if first_left && second_right && !(first_right && second_left) {
            return Some((first, second));
        }
        if second_left && first_right && !(second_right && first_left) {
            return Some((second, first));
        }
    }
    None
}

fn collect_equality_column_pairs(expr: &Expr, out: &mut Vec<(String, String)>) {
    let Expr::BinaryExpr(binary) = expr else {
        return;
    };
    if binary.op == Operator::Eq {
        if let (Some(left), Some(right)) = (column_name(&binary.left), column_name(&binary.right)) {
            out.push((left, right));
        }
        return;
    }
    if matches!(binary.op, Operator::And) {
        collect_equality_column_pairs(&binary.left, out);
        collect_equality_column_pairs(&binary.right, out);
    }
}

fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        _ => None,
    }
}

fn left_partition_columns_by_join_key(
    window: &Window,
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    left_key_column: &str,
) -> Option<Vec<String>> {
    let mut accepted = None;
    for expr in &window.window_expr {
        let Expr::WindowFunction(window) = strip_alias(expr) else {
            return None;
        };
        let mut columns = Vec::with_capacity(window.params.partition_by.len());
        for partition_expr in &window.params.partition_by {
            let Expr::Column(column) = strip_alias(partition_expr) else {
                return None;
            };
            let in_left = left_schema.index_of(&column.name).is_ok();
            let in_right = right_schema.index_of(&column.name).is_ok();
            if !in_left || in_right {
                return None;
            }
            columns.push(column.name.clone());
        }
        if !columns.iter().any(|column| column == left_key_column) {
            return None;
        }
        match accepted.as_ref() {
            Some(accepted) if accepted != &columns => return None,
            Some(_) => {}
            None => accepted = Some(columns),
        }
    }
    accepted
}

fn strip_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => strip_alias(alias.expr.as_ref()),
        _ => expr,
    }
}

fn contains_aggregate(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_) => true,
        LogicalPlan::Projection(projection) => contains_aggregate(projection.input.as_ref()),
        LogicalPlan::Filter(filter) => contains_aggregate(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => contains_aggregate(alias.input.as_ref()),
        LogicalPlan::Sort(sort) => contains_aggregate(sort.input.as_ref()),
        LogicalPlan::Limit(limit) => contains_aggregate(limit.input.as_ref()),
        LogicalPlan::Window(window) => contains_aggregate(window.input.as_ref()),
        LogicalPlan::Join(join) => {
            contains_aggregate(join.left.as_ref()) || contains_aggregate(join.right.as_ref())
        }
        _ => false,
    }
}

fn single_source_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Option<String> {
    let sources = source_set_for_plan(plan, sources);
    if sources.len() == 1 {
        sources.into_iter().next()
    } else {
        None
    }
}

fn source_set_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    collect_sources(plan, sources, &mut out);
    out
}

fn collect_sources(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    out: &mut BTreeSet<String>,
) {
    match plan {
        LogicalPlan::TableScan(scan) => {
            if let Some(source_name) = table_scan_source(scan, sources) {
                out.insert(source_name);
            }
        }
        LogicalPlan::Projection(projection) => {
            collect_sources(projection.input.as_ref(), sources, out)
        }
        LogicalPlan::Filter(filter) => collect_sources(filter.input.as_ref(), sources, out),
        LogicalPlan::SubqueryAlias(alias) => collect_sources(alias.input.as_ref(), sources, out),
        LogicalPlan::Sort(sort) => collect_sources(sort.input.as_ref(), sources, out),
        LogicalPlan::Limit(limit) => collect_sources(limit.input.as_ref(), sources, out),
        LogicalPlan::Window(window) => collect_sources(window.input.as_ref(), sources, out),
        LogicalPlan::Join(join) => {
            collect_sources(join.left.as_ref(), sources, out);
            collect_sources(join.right.as_ref(), sources, out);
        }
        _ => {}
    }
}

fn table_scan_source(
    scan: &TableScan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Option<String> {
    resolve_source_table(scan.table_name.table().to_string(), sources)
}

fn contains_unsupported_join_topn_wrapper(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => {
            contains_unsupported_join_topn_wrapper(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => {
            contains_unsupported_join_topn_wrapper(filter.input.as_ref())
        }
        LogicalPlan::SubqueryAlias(alias) => {
            contains_unsupported_join_topn_wrapper(alias.input.as_ref())
        }
        LogicalPlan::Window(window) => {
            contains_unsupported_join_topn_wrapper(window.input.as_ref())
        }
        LogicalPlan::Join(join) => {
            contains_unsupported_join_topn_wrapper(join.left.as_ref())
                || contains_unsupported_join_topn_wrapper(join.right.as_ref())
        }
        LogicalPlan::TableScan(_) => false,
        _ => true,
    }
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
