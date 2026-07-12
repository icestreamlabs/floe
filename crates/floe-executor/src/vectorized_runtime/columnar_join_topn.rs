use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, Int64Builder, StringArray, TimestampMillisecondArray, UInt32Array,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::logical_plan::{Filter, Join, Window};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, Operator};
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarIndexedZSet, SlateBackedColumnarZSet};
use dbsp::storage::{KeyValueTable, keyspace};
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

use crate::columnar_snapshot::columnar_zset_weight_sum;
use crate::delta_consolidation::{diff_bounded_output_batches, weighted_snapshot_schema};
use crate::encoding::EncodedRowScalar;
use crate::mv::registry::{ColumnarMaterializedViewStorage, MaterializedViewRegistry};
use crate::namespaces;
use crate::scalar_array_builder::ScalarColumnBuilder;

use super::{
    VectorizedMaterializedViewState, VectorizedSourceState, apply_keyed_source_snapshot_delta,
    apply_weighted_snapshot_delta, profile,
};

const APPEND_ONLY_LEFT_SNAPSHOT_ROW_LIMIT: usize = 100_000;

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
    operator_table: Arc<dyn KeyValueTable>,
    left: JoinTopNSourceState,
    right: JoinTopNSourceState,
    output_zset: SlateBackedColumnarZSet,
    current_output_index: Option<JoinTopNCurrentOutputIndex>,
    append_only_left_snapshot: Option<Vec<RecordBatch>>,
    evaluator: JoinTopNEvaluator,
    row_count: i64,
    initial_snapshot: Vec<RecordBatch>,
}

impl ColumnarJoinTopNMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

pub(super) struct ColumnarJoinTopNTick {
    pub(super) delta: ColumnarZSet,
    pub(super) row_count_delta: i64,
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

#[derive(Clone, Copy)]
struct RightDeltaOutputContext<'a> {
    output_schema: &'a SchemaRef,
    left_batches: &'a [RecordBatch],
    output_index: &'a JoinTopNCurrentOutputIndex,
    output_state_indices: &'a JoinTopNOutputStateIndices,
}

struct RightInsertDeltaContext<'a> {
    output: RightDeltaOutputContext<'a>,
    left_rows: &'a HashMap<i64, Vec<JoinTopNLeftRow>>,
    left_rows_ms: u64,
    total_start: Option<Instant>,
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
    encoded_output_row: Bytes,
    price: i64,
    bid_time: i64,
    bidder: i64,
    bid_extra: Option<String>,
}

#[derive(Clone)]
struct JoinTopNPreviousBestOrder {
    price: i64,
    bid_time: i64,
    bidder: i64,
    bid_extra: Option<String>,
}

#[derive(Clone)]
struct JoinTopNOutputStateIndices {
    partition: Vec<usize>,
    price: usize,
    bid_time: usize,
    bidder: usize,
    bid_extra: usize,
}

struct JoinTopNCurrentOutputIndex {
    table: Arc<dyn KeyValueTable>,
    key_prefix: Vec<u8>,
    output_schema: SchemaRef,
    output_state_indices: JoinTopNOutputStateIndices,
    order_values: Mutex<HashMap<Vec<u8>, JoinTopNPreviousBestOrder>>,
}

#[derive(Clone, Copy)]
enum RightDeltaWeightFilter {
    NonZero,
    Positive,
}

struct RetractedCurrentBestPartitions {
    auction_keys: HashSet<i64>,
    partition_keys: HashSet<JoinTopNPartitionKey>,
    partition_auction_keys: HashMap<JoinTopNPartitionKey, i64>,
}

impl RetractedCurrentBestPartitions {
    fn resolve_with_dominating_positive_candidates(
        &mut self,
        candidate_best: &HashMap<JoinTopNPartitionKey, JoinTopNBestBid>,
        previous_best: &HashMap<JoinTopNPartitionKey, JoinTopNPreviousBestBid>,
    ) {
        let resolved = self
            .partition_keys
            .iter()
            .filter(|partition_key| {
                let Some(candidate) = candidate_best.get(*partition_key) else {
                    return false;
                };
                let Some(previous) = previous_best.get(*partition_key) else {
                    return false;
                };
                candidate_orders_before_previous(candidate, previous)
            })
            .cloned()
            .collect::<Vec<_>>();
        for partition_key in resolved {
            self.partition_keys.remove(&partition_key);
            self.partition_auction_keys.remove(&partition_key);
        }
        self.auction_keys = self.partition_auction_keys.values().copied().collect();
    }
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
    let Expr::WindowFunction(window_function) = super::columnar_utils::strip_alias(window_expr)
    else {
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
    let all_sources = super::columnar_utils::source_set_for_plan(plan, sources);
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
    let left_relations = relation_names_for_plan(join.left.as_ref());
    let right_relations = relation_names_for_plan(join.right.as_ref());
    let Some(left_partition_columns) = left_partition_columns_by_join_key(
        window,
        &left_source_state.schema,
        &right_source_state.schema,
        &left_relations,
        &right_relations,
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
        output_namespace.clone(),
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed join-topn output zset")?;
    let initial_output = output_zset
        .materialize_columnar()
        .await
        .context("load join-topn output snapshot")?;
    let initial_row_count = columnar_zset_weight_sum(&initial_output)?;
    let initial_snapshot = crate::columnar_snapshot::columnar_zset_snapshot(&initial_output)?;
    let left_snapshot_zset = left_zset
        .materialize_columnar()
        .await
        .context("load join-topn left input snapshot")?;
    let right_snapshot_zset = right_zset
        .materialize_columnar()
        .await
        .context("load join-topn right input snapshot")?;
    let left_index_namespace = format!("{left_namespace}/index");
    let left_snapshot = crate::columnar_snapshot::columnar_zset_snapshot(&left_snapshot_zset)?;
    let append_only_left_snapshot = if left_source.append_only
        && record_batch_row_count(&left_snapshot) <= APPEND_ONLY_LEFT_SNAPSHOT_ROW_LIMIT
    {
        Some(left_snapshot)
    } else {
        None
    };
    let left_index_keys =
        vec![left_key_idx.context("partitioned join-topn left key index is missing")?];
    let mut left_index = SlateBackedColumnarIndexedZSet::new(
        Arc::clone(&table),
        left_index_namespace,
        Arc::clone(&left_source.schema),
        left_index_keys,
    )
    .await
    .context("initialize SlateDB-backed join-topn left input index")?;
    left_index
        .rebuild_from_zset(&left_snapshot_zset)
        .await
        .context("rebuild SlateDB-backed join-topn left input index")?;
    let right_index_namespace = format!("{right_namespace}/index");
    let right_index_keys =
        vec![right_key_idx.context("partitioned join-topn right key index is missing")?];
    let mut right_index = SlateBackedColumnarIndexedZSet::new(
        Arc::clone(&table),
        right_index_namespace,
        Arc::clone(&right_source.schema),
        right_index_keys,
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
    let current_output_index = match &evaluator {
        JoinTopNEvaluator::PartitionedBestBid(evaluator) => {
            if let Some(output_state_indices) = evaluator.output_state_indices() {
                let output_index = JoinTopNCurrentOutputIndex::new(
                    Arc::clone(&table),
                    &format!("{output_namespace}/current_output"),
                    Arc::clone(output_schema),
                    output_state_indices,
                )
                .await
                .context("initialize SlateDB-backed join-topn current output index")?;
                output_index
                    .rebuild_from_snapshot(&initial_snapshot)
                    .await
                    .context("rebuild SlateDB-backed join-topn current output index")?;
                Some(output_index)
            } else {
                None
            }
        }
    };

    Ok(ColumnarJoinTopNMaterializedViewState {
        operator_table: table,
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
        current_output_index,
        append_only_left_snapshot,
        evaluator,
        row_count: initial_row_count,
        initial_snapshot,
    })
}

pub(super) async fn run_columnar_join_topn_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<()> {
    let super::MaterializedViewOperator::JoinTopN(columnar) = &mut mv.operator else {
        unreachable!("join-topn tick dispatched to another operator")
    };
    let timing_enabled = tracing::enabled!(tracing::Level::DEBUG);
    let plan_start = timing_start(timing_enabled);
    let tick = run_columnar_join_topn_state_tick(
        columnar,
        insert_batches,
        weighted_delta_batches,
        &mv.output_schema,
    )
    .await?;

    let delta_batches = tick.delta.batches().to_vec();
    let delta_rows = delta_batches
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    columnar.row_count = columnar.row_count.saturating_add(tick.row_count_delta);
    if columnar.row_count < 0 {
        bail!(
            "join-topn columnar materialized view '{}' row count became negative",
            mv.view_name
        );
    }
    let snapshot_rows =
        usize::try_from(columnar.row_count).context("join-topn row count exceeds usize")?;
    let handle = registry.register(mv.view_name.clone());
    if let Some(zset_handle) = columnar.output_zset.current_handle() {
        handle.publish_columnar_version(
            version,
            zset_handle,
            ColumnarMaterializedViewStorage::new(
                Arc::clone(&columnar.operator_table),
                Arc::clone(&mv.output_schema),
            ),
            snapshot_rows,
            delta_batches,
        );
    } else {
        handle.publish_arrow_version(
            version,
            vec![RecordBatch::new_empty(Arc::clone(&mv.output_schema))],
            delta_batches,
        );
    }
    tracing::debug!(
        view = %mv.view_name,
        version,
        delta_rows,
        snapshot_rows,
        input_changed = tick.input_changed,
        total_ms = elapsed_ms(plan_start),
        mode = "columnar_join_topn",
        "SlateDB-backed join-topn columnar DBSP materialized view tick completed"
    );
    Ok(())
}

pub(super) async fn run_columnar_join_topn_state_tick(
    columnar: &mut ColumnarJoinTopNMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    output_schema: &SchemaRef,
) -> Result<ColumnarJoinTopNTick> {
    #[derive(Default)]
    struct TickPhaseTimings {
        touched_key_count: usize,
        collect_keys_ms: u64,
        lookup_left_ms: u64,
        lookup_right_ms: u64,
        apply_input_ms: u64,
        evaluate_previous_ms: u64,
        evaluate_next_ms: u64,
        diff_ms: u64,
        append_only_merge_ms: u64,
    }

    let total_start = profile::start();
    let timing_enabled = tracing::enabled!(tracing::Level::DEBUG);
    let total_wall_start = timing_start(timing_enabled);
    let phase_start = profile::start();
    let source_input_start = timing_start(timing_enabled);
    let left_input_delta =
        source_input_delta(&columnar.left, insert_batches, weighted_delta_batches)?;
    let right_input_delta =
        source_input_delta(&columnar.right, insert_batches, weighted_delta_batches)?;
    let source_input_ms = elapsed_ms(source_input_start);
    profile::record_since("join_topn.source_input_delta", phase_start);
    let phase_start = profile::start();
    let persist_source_start = timing_start(timing_enabled);
    let left_delta =
        persisted_source_delta(&mut columnar.left.input_zset, left_input_delta).await?;
    let right_delta =
        persisted_source_delta(&mut columnar.right.input_zset, right_input_delta).await?;
    let persist_source_ms = elapsed_ms(persist_source_start);
    profile::record_since("join_topn.persist_source_delta", phase_start);
    let input_changed = !left_delta.batches().is_empty() || !right_delta.batches().is_empty();
    let left_delta_rows = left_delta.num_rows();
    let right_delta_rows = right_delta.num_rows();
    let mut timings = TickPhaseTimings::default();

    let output_delta_batches =
        match &columnar.evaluator {
            JoinTopNEvaluator::PartitionedBestBid(evaluator) => {
                let phase_start = profile::start();
                let collect_keys_start = timing_start(timing_enabled);
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
                timings.touched_key_count = touched_keys.len();
                timings.collect_keys_ms = elapsed_ms(collect_keys_start);
                profile::record_since("join_topn.collect_touched_keys", phase_start);

                if left_delta.is_empty()
                    && right_delta.num_rows() > 0
                    && let Some(output_state_indices) = evaluator.output_state_indices()
                {
                    let phase_start = profile::start();
                    let lookup_left_start = timing_start(timing_enabled);
                    let previous_left_lookup;
                    let previous_left =
                        if let Some(snapshot) = columnar.append_only_left_snapshot.as_ref() {
                            snapshot.as_slice()
                        } else {
                            previous_left_lookup =
                                lookup_indexed_join_topn_state_for_i64_keys(
                                    columnar.left.input_index.as_deref().context(
                                        "partitioned join-topn left source index missing",
                                    )?,
                                    &touched_keys,
                                    &columnar.left.schema,
                                    "left",
                                )
                                .await?;
                            previous_left_lookup.as_slice()
                        };
                    timings.lookup_left_ms = elapsed_ms(lookup_left_start);
                    profile::record_since("join_topn.lookup_previous_left", phase_start);
                    let phase_start = profile::start();
                    let append_only_merge_start = timing_start(timing_enabled);
                    let output_delta = evaluator
                        .right_delta_output_delta(
                            &right_delta,
                            columnar
                                .right
                                .input_index
                                .as_deref()
                                .context("partitioned join-topn right source index missing")?,
                            &columnar.right.schema,
                            &columnar.right.primary_key_columns,
                            RightDeltaOutputContext {
                                output_schema,
                                left_batches: previous_left,
                                output_index: columnar.current_output_index.as_ref().context(
                                    "partitioned join-topn current output index missing",
                                )?,
                                output_state_indices: &output_state_indices,
                            },
                        )
                        .await
                        .context("build right-side join-topn output delta")?;
                    timings.append_only_merge_ms = elapsed_ms(append_only_merge_start);
                    profile::record_since("join_topn.append_only_right_merge", phase_start);
                    output_delta
                } else if right_delta.is_empty()
                    && columnar_zset_is_delete_only(&left_delta)?
                    && let Some(output_index) = columnar.current_output_index.as_ref()
                {
                    let phase_start = profile::start();
                    let left_delete_start = timing_start(timing_enabled);
                    let partition_keys = evaluator.left_delete_partition_keys(&left_delta)?;
                    let previous_output = output_index
                        .lookup_snapshot_for_partition_keys(partition_keys.iter())
                        .await
                        .context("lookup current join-topn output for left deletes")?;
                    let output_delta =
                        negative_output_delta_from_snapshot(output_schema, &previous_output)
                            .context("build join-topn left-delete output delta")?;
                    timings.append_only_merge_ms = elapsed_ms(left_delete_start);
                    profile::record_since("join_topn.left_delete_merge", phase_start);
                    output_delta
                } else {
                    let phase_start = profile::start();
                    let lookup_left_start = timing_start(timing_enabled);
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
                    timings.lookup_left_ms = elapsed_ms(lookup_left_start);
                    profile::record_since("join_topn.lookup_previous_left", phase_start);
                    let phase_start = profile::start();
                    let lookup_right_start = timing_start(timing_enabled);
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
                    timings.lookup_right_ms = elapsed_ms(lookup_right_start);
                    profile::record_since("join_topn.lookup_previous_right", phase_start);
                    let phase_start = profile::start();
                    let apply_input_start = timing_start(timing_enabled);
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
                    timings.apply_input_ms = elapsed_ms(apply_input_start);
                    profile::record_since("join_topn.apply_input_delta", phase_start);

                    let (previous_output, next_output) = if touched_keys.is_empty() {
                        (Vec::new(), Vec::new())
                    } else {
                        let phase_start = profile::start();
                        let evaluate_previous_start = timing_start(timing_enabled);
                        let previous_output = evaluator
                            .evaluate(&previous_left, &previous_right)
                            .await
                            .context("evaluate previous join-topn partition outputs")?;
                        timings.evaluate_previous_ms = elapsed_ms(evaluate_previous_start);
                        profile::record_since("join_topn.evaluate_previous", phase_start);
                        let phase_start = profile::start();
                        let evaluate_next_start = timing_start(timing_enabled);
                        let next_output = evaluator
                            .evaluate(&next_left, &next_right)
                            .await
                            .context("evaluate next join-topn partition outputs")?;
                        timings.evaluate_next_ms = elapsed_ms(evaluate_next_start);
                        profile::record_since("join_topn.evaluate_next", phase_start);
                        (previous_output, next_output)
                    };
                    let phase_start = profile::start();
                    let diff_start = timing_start(timing_enabled);
                    let diff = diff_bounded_output_batches(
                        Arc::clone(output_schema),
                        &previous_output,
                        &next_output,
                    )
                    .await
                    .context("diff join-topn partition outputs")?;
                    timings.diff_ms = elapsed_ms(diff_start);
                    profile::record_since("join_topn.diff_output", phase_start);
                    diff.batches
                }
            }
        };

    let phase_start = profile::start();
    let build_output_start = timing_start(timing_enabled);
    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build join-topn output zset delta")?;
    let output_delta_rows = output_delta.num_rows();
    let build_output_ms = elapsed_ms(build_output_start);
    profile::record_since("join_topn.build_output_zset", phase_start);
    let phase_start = profile::start();
    let output_create_start = timing_start(timing_enabled);
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
    let output_create_ms = elapsed_ms(output_create_start);
    profile::record_since("join_topn.output_create_version", phase_start);

    let phase_start = profile::start();
    let row_count_start = timing_start(timing_enabled);
    let row_count_delta = columnar_zset_weight_sum(&persisted_output_delta)
        .context("compute join-topn output row-count delta")?;
    let row_count_ms = elapsed_ms(row_count_start);
    profile::record_since("join_topn.row_count_delta", phase_start);

    let phase_start = profile::start();
    let update_output_index_start = timing_start(timing_enabled);
    if let Some(index) = columnar.current_output_index.as_ref() {
        index
            .apply_output_delta(&persisted_output_delta)
            .await
            .context("apply join-topn output delta to SlateDB-backed current output index")?;
    }
    let update_output_index_ms = elapsed_ms(update_output_index_start);
    profile::record_since("join_topn.update_output_index", phase_start);

    if let Some(snapshot) = columnar.append_only_left_snapshot.as_mut()
        && !apply_append_only_left_snapshot_delta(snapshot, &left_delta)?
    {
        columnar.append_only_left_snapshot = None;
    }

    let phase_start = profile::start();
    let update_indexes_start = timing_start(timing_enabled);
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
    let update_indexes_ms = elapsed_ms(update_indexes_start);
    profile::record_since("join_topn.update_indexes", phase_start);
    tracing::debug!(
        left_delta_rows,
        right_delta_rows,
        touched_key_count = timings.touched_key_count,
        output_delta_rows,
        persisted_output_delta_rows = persisted_output_delta.num_rows(),
        row_count_delta,
        input_changed,
        source_input_ms,
        persist_source_ms,
        collect_keys_ms = timings.collect_keys_ms,
        lookup_left_ms = timings.lookup_left_ms,
        lookup_right_ms = timings.lookup_right_ms,
        apply_input_ms = timings.apply_input_ms,
        evaluate_previous_ms = timings.evaluate_previous_ms,
        evaluate_next_ms = timings.evaluate_next_ms,
        diff_ms = timings.diff_ms,
        append_only_merge_ms = timings.append_only_merge_ms,
        build_output_ms,
        output_create_ms,
        row_count_ms,
        update_output_index_ms,
        update_indexes_ms,
        total_ms = elapsed_ms(total_wall_start),
        "join-topn state tick phase timings"
    );
    profile::record_since("join_topn.total", total_start);
    Ok(ColumnarJoinTopNTick {
        delta: persisted_output_delta,
        row_count_delta,
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

fn apply_append_only_left_snapshot_delta(
    snapshot: &mut Vec<RecordBatch>,
    delta: &ColumnarZSet,
) -> Result<bool> {
    if delta.is_empty() {
        return Ok(true);
    }
    let mut delta_batches = crate::columnar_snapshot::columnar_zset_snapshot(delta)?;
    let delta_rows = record_batch_row_count(&delta_batches);
    if delta_rows == 0 {
        return Ok(true);
    }
    let current_rows = record_batch_row_count(snapshot);
    if current_rows.saturating_add(delta_rows) > APPEND_ONLY_LEFT_SNAPSHOT_ROW_LIMIT {
        return Ok(false);
    }
    if snapshot.len() == 1 && snapshot[0].num_rows() == 0 {
        snapshot.clear();
    }
    snapshot.append(&mut delta_batches);
    Ok(true)
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
    if primary_key_columns
        .iter()
        .any(|column| schema.index_of(column).is_err())
    {
        return apply_weighted_snapshot_delta(schema, previous, delta.batches().to_vec()).await;
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

fn filter_columnar_zset_to_i64_keys(
    delta: &ColumnarZSet,
    key_idx: usize,
    wanted_keys: &HashSet<i64>,
) -> Result<ColumnarZSet> {
    if delta.is_empty() || wanted_keys.is_empty() {
        return ColumnarZSet::empty(delta.value_schema());
    }
    let mut batches = Vec::new();
    for batch in delta.batches() {
        let keys = int64_column(batch, key_idx)?;
        let indices = (0..batch.num_rows())
            .filter(|row_idx| {
                !keys.is_null(*row_idx) && wanted_keys.contains(&keys.value(*row_idx))
            })
            .map(|row_idx| u32::try_from(row_idx).context("join-topn delta row index exceeds u32"))
            .collect::<Result<Vec<_>>>()?;
        if indices.is_empty() {
            continue;
        }
        if indices.len() == batch.num_rows() {
            batches.push(batch.clone());
            continue;
        }
        let indices = UInt32Array::from(indices);
        let columns = batch
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), &indices, None))
            .collect::<std::result::Result<Vec<ArrayRef>, _>>()
            .context("filter join-topn right delta to recompute keys")?;
        batches.push(RecordBatch::try_new(batch.schema(), columns)?);
    }
    ColumnarZSet::try_new_weighted(delta.value_schema(), batches)
}

fn columnar_zset_is_delete_only(delta: &ColumnarZSet) -> Result<bool> {
    if delta.is_empty() {
        return Ok(false);
    }
    let weight_idx = delta.value_column_count();
    let mut saw_delete = false;
    for batch in delta.batches() {
        let weights = weight_column_at(batch, weight_idx)?;
        for row_idx in 0..batch.num_rows() {
            if weights.is_null(row_idx) {
                bail!("join-topn delta weight column cannot contain NULL");
            }
            let weight = weights.value(row_idx);
            if weight > 0 {
                return Ok(false);
            }
            if weight < 0 {
                saw_delete = true;
            }
        }
    }
    Ok(saw_delete)
}

fn columnar_zset_is_insert_only(delta: &ColumnarZSet) -> Result<bool> {
    if delta.is_empty() {
        return Ok(false);
    }
    let weight_idx = delta.value_column_count();
    let mut saw_insert = false;
    for batch in delta.batches() {
        let weights = weight_column_at(batch, weight_idx)?;
        for row_idx in 0..batch.num_rows() {
            if weights.is_null(row_idx) {
                bail!("join-topn delta weight column cannot contain NULL");
            }
            let weight = weights.value(row_idx);
            if weight < 0 {
                return Ok(false);
            }
            if weight > 0 {
                saw_insert = true;
            }
        }
    }
    Ok(saw_insert)
}

fn negative_output_delta_from_snapshot(
    output_schema: &SchemaRef,
    snapshot: &[RecordBatch],
) -> Result<Vec<RecordBatch>> {
    let row_count = record_batch_row_count(snapshot);
    if row_count == 0 {
        return Ok(Vec::new());
    }
    let mut builders = output_schema
        .fields()
        .iter()
        .map(|field| ScalarColumnBuilder::new(field.data_type(), row_count))
        .collect::<Result<Vec<_>>>()?;
    let mut weights = Int64Builder::with_capacity(row_count);
    for batch in snapshot {
        for row_idx in 0..batch.num_rows() {
            append_existing_output_row(&mut builders, batch, row_idx)?;
            weights.append_value(-1);
        }
    }
    let mut columns = builders
        .iter_mut()
        .map(ScalarColumnBuilder::finish_array)
        .collect::<Vec<_>>();
    columns.push(Arc::new(weights.finish()) as ArrayRef);
    Ok(vec![RecordBatch::try_new(
        weighted_snapshot_schema(output_schema)?,
        columns,
    )?])
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

fn record_batch_row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn timing_start(enabled: bool) -> Option<Instant> {
    enabled.then(Instant::now)
}

fn elapsed_ms(start: Option<Instant>) -> u64 {
    start
        .map(|start| start.elapsed().as_millis() as u64)
        .unwrap_or(0)
}

fn weight_column_at(batch: &RecordBatch, weight_idx: usize) -> Result<&Int64Array> {
    batch
        .column(weight_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("join-topn delta weight column must be Int64"))
}

fn right_delta_weight_matches(filter: RightDeltaWeightFilter, weight: i64) -> bool {
    match filter {
        RightDeltaWeightFilter::NonZero => weight != 0,
        RightDeltaWeightFilter::Positive => weight > 0,
    }
}

impl JoinTopNCurrentOutputIndex {
    async fn new(
        table: Arc<dyn KeyValueTable>,
        namespace: &str,
        output_schema: SchemaRef,
        output_state_indices: JoinTopNOutputStateIndices,
    ) -> Result<Self> {
        let mut key_prefix = keyspace::namespace_prefix(keyspace::prefix::INDEX, namespace);
        key_prefix.extend_from_slice(b"current/");
        Ok(Self {
            table,
            key_prefix,
            output_schema,
            output_state_indices,
            order_values: Mutex::new(HashMap::new()),
        })
    }

    async fn rebuild_from_snapshot(&self, snapshot: &[RecordBatch]) -> Result<()> {
        let mut writes = WriteBatch::new();
        let mut has_writes = false;
        let mut order_values = HashMap::new();
        for (key, _) in self
            .table
            .scan_prefix_bytes(&self.key_prefix, &ScanOptions::default())
            .await
            .context("scan join-topn current output index for rebuild")?
        {
            writes.delete(&key);
            has_writes = true;
        }
        for batch in snapshot {
            for row_idx in 0..batch.num_rows() {
                let partition_key =
                    output_partition_key(batch, row_idx, &self.output_state_indices)?;
                let payload = encode_partition_key_payload(&partition_key)?;
                let key = self.state_key_for_payload(&payload);
                let value = encode_current_output_row(batch, row_idx, &self.output_schema)?;
                let order = current_output_order(batch, row_idx, &self.output_state_indices)?;
                writes.put_bytes(Bytes::from(key), Bytes::from(value));
                order_values.insert(payload, order);
                has_writes = true;
            }
        }
        if has_writes {
            self.table
                .write_batch(writes)
                .await
                .context("persist rebuilt join-topn current output index")?;
        }
        *self
            .order_values
            .lock()
            .map_err(|_| anyhow::anyhow!("join-topn current order index poisoned"))? = order_values;
        Ok(())
    }

    async fn apply_output_delta(&self, delta: &ColumnarZSet) -> Result<()> {
        if delta.is_empty() {
            return Ok(());
        }
        let mut positive_updates: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut positive_orders: HashMap<Vec<u8>, JoinTopNPreviousBestOrder> = HashMap::new();
        let mut negative_updates: HashSet<Vec<u8>> = HashSet::new();
        let mut negative_payloads: HashSet<Vec<u8>> = HashSet::new();
        let weight_idx = delta.value_column_count();
        for batch in delta.batches() {
            let weights = weight_column_at(batch, weight_idx)?;
            for row_idx in 0..batch.num_rows() {
                if weights.is_null(row_idx) || weights.value(row_idx) == 0 {
                    continue;
                }
                let partition_key =
                    output_partition_key(batch, row_idx, &self.output_state_indices)?;
                let payload = encode_partition_key_payload(&partition_key)?;
                let key = self.state_key_for_payload(&payload);
                if weights.value(row_idx) > 0 {
                    positive_updates.insert(
                        key,
                        encode_current_output_row(batch, row_idx, &self.output_schema)?,
                    );
                    positive_orders.insert(
                        payload,
                        current_output_order(batch, row_idx, &self.output_state_indices)?,
                    );
                } else {
                    negative_updates.insert(key);
                    negative_payloads.insert(payload);
                }
            }
        }
        if positive_updates.is_empty() && negative_updates.is_empty() {
            return Ok(());
        }
        let mut writes = WriteBatch::new();
        for key in negative_updates {
            if !positive_updates.contains_key(&key) {
                writes.delete(key);
            }
        }
        for (key, value) in positive_updates {
            writes.put_bytes(Bytes::from(key), Bytes::from(value));
        }
        self.table
            .write_batch(writes)
            .await
            .context("persist join-topn current output index delta")?;
        let mut order_values = self
            .order_values
            .lock()
            .map_err(|_| anyhow::anyhow!("join-topn current order index poisoned"))?;
        for payload in negative_payloads {
            if !positive_orders.contains_key(&payload) {
                order_values.remove(&payload);
            }
        }
        for (payload, order) in positive_orders {
            order_values.insert(payload, order);
        }
        Ok(())
    }

    async fn lookup_snapshot_for_partition_keys<'a, I>(
        &self,
        partition_keys: I,
    ) -> Result<Vec<RecordBatch>>
    where
        I: Iterator<Item = &'a JoinTopNPartitionKey>,
    {
        let values = self
            .lookup_values_for_partition_keys(partition_keys)
            .await?;
        current_output_values_to_batches(&self.output_schema, values)
    }

    async fn lookup_values_for_partition_keys<'a, I>(&self, partition_keys: I) -> Result<Vec<Bytes>>
    where
        I: Iterator<Item = &'a JoinTopNPartitionKey>,
    {
        let mut payloads = BTreeSet::new();
        let mut single_i64_keys = true;
        for partition_key in partition_keys {
            if !matches!(partition_key.as_slice(), [Some(EncodedRowScalar::Int64(_))]) {
                single_i64_keys = false;
            }
            payloads.insert(encode_partition_key_payload(partition_key)?);
        }
        if payloads.is_empty() {
            return Ok(Vec::new());
        }

        Ok(if single_i64_keys && payloads.len() >= 64 {
            self.lookup_values_by_range(&payloads).await?
        } else {
            self.lookup_values_by_point_gets(&payloads).await?
        })
    }

    async fn lookup_values_by_range(&self, payloads: &BTreeSet<Vec<u8>>) -> Result<Vec<Bytes>> {
        let wanted = payloads.iter().cloned().collect::<HashSet<_>>();
        let start_payload = payloads
            .first()
            .context("join-topn current output range lookup missing start")?;
        let end_payload = payloads
            .last()
            .context("join-topn current output range lookup missing end")?;
        let start = self.state_key_for_payload(start_payload);
        let mut end = self.state_key_for_payload(end_payload);
        end.push(0xFF);
        let entries = self
            .table
            .scan_range_bytes(start..end, &join_topn_output_scan_options())
            .await
            .context("range-scan join-topn current output index")?;
        let mut values = Vec::new();
        for (key, value) in entries {
            let payload = self.payload_from_state_key(key.as_ref())?;
            if wanted.contains(payload) {
                values.push(value);
            }
        }
        Ok(values)
    }

    async fn lookup_values_by_point_gets(
        &self,
        payloads: &BTreeSet<Vec<u8>>,
    ) -> Result<Vec<Bytes>> {
        let mut values = Vec::new();
        for payload in payloads {
            if let Some(value) = self
                .table
                .get_bytes(&self.state_key_for_payload(payload))
                .await
                .context("get join-topn current output index entry")?
            {
                values.push(value);
            }
        }
        Ok(values)
    }

    fn state_key_for_payload(&self, payload: &[u8]) -> Vec<u8> {
        let mut key = self.key_prefix.clone();
        key.extend_from_slice(payload);
        key
    }

    fn payload_from_state_key<'a>(&self, key: &'a [u8]) -> Result<&'a [u8]> {
        if !key.starts_with(&self.key_prefix) {
            bail!("join-topn current output index key prefix mismatch");
        }
        Ok(&key[self.key_prefix.len()..])
    }
}

fn join_topn_output_scan_options() -> ScanOptions {
    ScanOptions {
        read_ahead_bytes: 1024 * 1024,
        cache_blocks: true,
        max_fetch_tasks: 4,
        ..ScanOptions::default()
    }
}

fn encode_partition_key_payload(partition_key: &JoinTopNPartitionKey) -> Result<Vec<u8>> {
    let column_count =
        u32::try_from(partition_key.len()).context("join-topn partition key too wide")?;
    let mut out = Vec::new();
    out.extend_from_slice(&column_count.to_be_bytes());
    for value in partition_key {
        encode_partition_key_scalar(value.as_ref(), &mut out)?;
    }
    Ok(out)
}

fn encode_partition_key_scalar(value: Option<&EncodedRowScalar>, out: &mut Vec<u8>) -> Result<()> {
    match value {
        None => out.push(0),
        Some(EncodedRowScalar::Int64(value)) => {
            out.push(1);
            out.extend_from_slice(&sortable_i64(*value).to_be_bytes());
        }
        Some(EncodedRowScalar::Utf8(value)) => {
            out.push(2);
            let len = u32::try_from(value.len()).context("join-topn key string too large")?;
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(value.as_bytes());
        }
        Some(EncodedRowScalar::TimestampMillis(value)) => {
            out.push(3);
            out.extend_from_slice(&sortable_i64(*value).to_be_bytes());
        }
        Some(EncodedRowScalar::Bool(value)) => {
            out.push(4);
            out.push(u8::from(*value));
        }
        Some(EncodedRowScalar::DateDays(value)) => {
            out.push(5);
            out.extend_from_slice(&sortable_i32(*value).to_be_bytes());
        }
        Some(EncodedRowScalar::Decimal128(value)) => {
            out.push(6);
            out.extend_from_slice(&sortable_i128(*value).to_be_bytes());
        }
    }
    Ok(())
}

fn sortable_i64(value: i64) -> u64 {
    (value as u64) ^ 0x8000_0000_0000_0000
}

fn sortable_i32(value: i32) -> u32 {
    (value as u32) ^ 0x8000_0000
}

fn sortable_i128(value: i128) -> u128 {
    (value as u128) ^ 0x8000_0000_0000_0000_0000_0000_0000_0000
}

fn encode_current_output_row(
    batch: &RecordBatch,
    row_idx: usize,
    output_schema: &SchemaRef,
) -> Result<Vec<u8>> {
    let column_count =
        u32::try_from(output_schema.fields().len()).context("join-topn output row too wide")?;
    let mut out = Vec::new();
    out.extend_from_slice(&column_count.to_be_bytes());
    for column_idx in 0..output_schema.fields().len() {
        let value = encoded_scalar(batch, column_idx, row_idx)?;
        encode_current_output_scalar(value.as_ref(), &mut out)?;
    }
    Ok(out)
}

fn encode_current_output_scalar(value: Option<&EncodedRowScalar>, out: &mut Vec<u8>) -> Result<()> {
    match value {
        None => out.push(0),
        Some(EncodedRowScalar::Int64(value)) => {
            out.push(1);
            out.extend_from_slice(&value.to_be_bytes());
        }
        Some(EncodedRowScalar::Utf8(value)) => {
            out.push(2);
            let len = u32::try_from(value.len()).context("join-topn output string too large")?;
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(value.as_bytes());
        }
        Some(EncodedRowScalar::TimestampMillis(value)) => {
            out.push(3);
            out.extend_from_slice(&value.to_be_bytes());
        }
        Some(EncodedRowScalar::Bool(value)) => {
            out.push(4);
            out.push(u8::from(*value));
        }
        Some(EncodedRowScalar::DateDays(value)) => {
            out.push(5);
            out.extend_from_slice(&value.to_be_bytes());
        }
        Some(EncodedRowScalar::Decimal128(value)) => {
            out.push(6);
            out.extend_from_slice(&value.to_be_bytes());
        }
    }
    Ok(())
}

fn current_output_values_to_batches(
    schema: &SchemaRef,
    values: Vec<Bytes>,
) -> Result<Vec<RecordBatch>> {
    if values.is_empty() {
        return Ok(Vec::new());
    }
    let mut builders = schema
        .fields()
        .iter()
        .map(|field| ScalarColumnBuilder::new(field.data_type(), values.len()))
        .collect::<Result<Vec<_>>>()?;
    for value in values {
        let scalars = decode_current_output_row(&value, schema.fields().len())?;
        for (builder, scalar) in builders.iter_mut().zip(scalars.iter()) {
            builder.append_encoded_scalar(scalar.as_ref())?;
        }
    }
    let columns = builders
        .iter_mut()
        .map(ScalarColumnBuilder::finish_array)
        .collect::<Vec<_>>();
    Ok(vec![RecordBatch::try_new(Arc::clone(schema), columns)?])
}

fn decode_current_output_row(
    bytes: &[u8],
    expected_columns: usize,
) -> Result<Vec<Option<EncodedRowScalar>>> {
    let mut cursor = 0usize;
    let column_count = usize::try_from(read_u32_be(bytes, &mut cursor)?)
        .context("join-topn current output column count out of range")?;
    if column_count != expected_columns {
        bail!(
            "join-topn current output row has {column_count} columns, expected {expected_columns}"
        );
    }
    let mut values = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        values.push(decode_current_output_scalar(bytes, &mut cursor)?);
    }
    if cursor != bytes.len() {
        bail!("join-topn current output row has trailing bytes");
    }
    Ok(values)
}

fn decode_current_output_previous_best(
    bytes: Bytes,
    expected_columns: usize,
    output_state_indices: &JoinTopNOutputStateIndices,
) -> Result<(JoinTopNPartitionKey, JoinTopNPreviousBestBid)> {
    let mut cursor = 0usize;
    let column_count = usize::try_from(read_u32_be(&bytes, &mut cursor)?)
        .context("join-topn current output column count out of range")?;
    if column_count != expected_columns {
        bail!(
            "join-topn current output row has {column_count} columns, expected {expected_columns}"
        );
    }

    let mut partition_key = vec![None; output_state_indices.partition.len()];
    let mut price = None;
    let mut bid_time = None;
    let mut bidder = None;
    let mut bid_extra = None;

    for column_idx in 0..column_count {
        let partition_position = output_state_indices
            .partition
            .iter()
            .position(|partition_idx| *partition_idx == column_idx);
        if partition_position.is_some()
            || column_idx == output_state_indices.price
            || column_idx == output_state_indices.bid_time
            || column_idx == output_state_indices.bidder
            || column_idx == output_state_indices.bid_extra
        {
            let value = decode_current_output_scalar(&bytes, &mut cursor)?;
            if let Some(position) = partition_position {
                partition_key[position] = value;
            } else if column_idx == output_state_indices.price {
                price = Some(current_output_i64(value.as_ref(), "price")?);
            } else if column_idx == output_state_indices.bid_time {
                bid_time = Some(current_output_i64(value.as_ref(), "bid_time")?);
            } else if column_idx == output_state_indices.bidder {
                bidder = Some(current_output_i64(value.as_ref(), "bidder")?);
            } else if column_idx == output_state_indices.bid_extra {
                bid_extra = Some(current_output_string(value)?);
            }
        } else {
            skip_current_output_scalar(&bytes, &mut cursor)?;
        }
    }
    if cursor != bytes.len() {
        bail!("join-topn current output row has trailing bytes");
    }
    let price = price.context("join-topn current output row missing price column")?;
    let bid_time = bid_time.context("join-topn current output row missing bid time column")?;
    let bidder = bidder.context("join-topn current output row missing bidder column")?;
    let bid_extra = bid_extra.context("join-topn current output row missing bid extra column")?;
    Ok((
        partition_key,
        JoinTopNPreviousBestBid {
            encoded_output_row: bytes,
            price,
            bid_time,
            bidder,
            bid_extra,
        },
    ))
}

fn current_output_order(
    batch: &RecordBatch,
    row_idx: usize,
    output_state_indices: &JoinTopNOutputStateIndices,
) -> Result<JoinTopNPreviousBestOrder> {
    let price = current_output_i64(
        encoded_scalar(batch, output_state_indices.price, row_idx)?.as_ref(),
        "price",
    )?;
    let bid_time = current_output_i64(
        encoded_scalar(batch, output_state_indices.bid_time, row_idx)?.as_ref(),
        "bid_time",
    )?;
    let bidder = current_output_i64(
        encoded_scalar(batch, output_state_indices.bidder, row_idx)?.as_ref(),
        "bidder",
    )?;
    let bid_extra = current_output_string(encoded_scalar(
        batch,
        output_state_indices.bid_extra,
        row_idx,
    )?)?;
    Ok(JoinTopNPreviousBestOrder {
        price,
        bid_time,
        bidder,
        bid_extra,
    })
}

fn append_current_output_row_bytes(
    builders: &mut [ScalarColumnBuilder],
    bytes: &[u8],
    expected_columns: usize,
) -> Result<()> {
    let mut cursor = 0usize;
    let column_count = usize::try_from(read_u32_be(bytes, &mut cursor)?)
        .context("join-topn current output column count out of range")?;
    if column_count != expected_columns || column_count != builders.len() {
        bail!(
            "join-topn current output row has {column_count} columns, expected {expected_columns}"
        );
    }
    for builder in builders {
        let scalar = decode_current_output_scalar(bytes, &mut cursor)?;
        builder.append_encoded_scalar(scalar.as_ref())?;
    }
    if cursor != bytes.len() {
        bail!("join-topn current output row has trailing bytes");
    }
    Ok(())
}

fn current_output_i64(value: Option<&EncodedRowScalar>, label: &str) -> Result<i64> {
    match value {
        Some(EncodedRowScalar::Int64(value)) | Some(EncodedRowScalar::TimestampMillis(value)) => {
            Ok(*value)
        }
        Some(other) => Err(anyhow::anyhow!(
            "join-topn current output {label} expected Int64/TimestampMillis, found {other:?}"
        )),
        None => Err(anyhow::anyhow!(
            "join-topn current output {label} cannot be NULL"
        )),
    }
}

fn current_output_string(value: Option<EncodedRowScalar>) -> Result<Option<String>> {
    match value {
        Some(EncodedRowScalar::Utf8(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(other) => Err(anyhow::anyhow!(
            "join-topn current output bid extra expected Utf8, found {other:?}"
        )),
    }
}

fn decode_current_output_scalar(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<Option<EncodedRowScalar>> {
    let tag = *bytes
        .get(*cursor)
        .ok_or_else(|| anyhow::anyhow!("join-topn current output scalar tag truncated"))?;
    *cursor += 1;
    match tag {
        0 => Ok(None),
        1 => Ok(Some(EncodedRowScalar::Int64(read_i64_be(bytes, cursor)?))),
        2 => {
            let len = usize::try_from(read_u32_be(bytes, cursor)?)
                .context("join-topn current output string length out of range")?;
            let text = read_bytes_at(bytes, cursor, len, "join-topn current output string")?;
            Ok(Some(EncodedRowScalar::Utf8(
                std::str::from_utf8(text)
                    .context("join-topn current output string is not UTF-8")?
                    .to_string(),
            )))
        }
        3 => Ok(Some(EncodedRowScalar::TimestampMillis(read_i64_be(
            bytes, cursor,
        )?))),
        4 => {
            let [value] = read_bytes_at(bytes, cursor, 1, "join-topn current output bool")? else {
                bail!("join-topn current output bool has invalid width")
            };
            Ok(Some(EncodedRowScalar::Bool(*value != 0)))
        }
        5 => Ok(Some(EncodedRowScalar::DateDays(read_i32_be(
            bytes, cursor,
        )?))),
        6 => Ok(Some(EncodedRowScalar::Decimal128(read_i128_be(
            bytes, cursor,
        )?))),
        other => bail!("unknown join-topn current output scalar tag {other}"),
    }
}

fn skip_current_output_scalar(bytes: &[u8], cursor: &mut usize) -> Result<()> {
    let tag = *bytes
        .get(*cursor)
        .ok_or_else(|| anyhow::anyhow!("join-topn current output scalar tag truncated"))?;
    *cursor += 1;
    match tag {
        0 => Ok(()),
        1 | 3 => read_bytes_at(bytes, cursor, 8, "join-topn current output i64").map(drop),
        2 => {
            let len = usize::try_from(read_u32_be(bytes, cursor)?)
                .context("join-topn current output string length out of range")?;
            read_bytes_at(bytes, cursor, len, "join-topn current output string").map(drop)
        }
        4 => read_bytes_at(bytes, cursor, 1, "join-topn current output bool").map(drop),
        5 => read_bytes_at(bytes, cursor, 4, "join-topn current output i32").map(drop),
        6 => read_bytes_at(bytes, cursor, 16, "join-topn current output i128").map(drop),
        other => bail!("unknown join-topn current output scalar tag {other}"),
    }
}

fn read_u32_be(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    Ok(u32::from_be_bytes(read_array_at(
        bytes,
        cursor,
        "join-topn u32",
    )?))
}

fn read_i32_be(bytes: &[u8], cursor: &mut usize) -> Result<i32> {
    Ok(i32::from_be_bytes(read_array_at(
        bytes,
        cursor,
        "join-topn i32",
    )?))
}

fn read_i64_be(bytes: &[u8], cursor: &mut usize) -> Result<i64> {
    Ok(i64::from_be_bytes(read_array_at(
        bytes,
        cursor,
        "join-topn i64",
    )?))
}

fn read_i128_be(bytes: &[u8], cursor: &mut usize) -> Result<i128> {
    Ok(i128::from_be_bytes(read_array_at(
        bytes,
        cursor,
        "join-topn i128",
    )?))
}

fn read_array_at<const N: usize>(bytes: &[u8], cursor: &mut usize, label: &str) -> Result<[u8; N]> {
    let chunk = read_bytes_at(bytes, cursor, N, label)?;
    chunk
        .try_into()
        .map_err(|_| anyhow::anyhow!("{label} expected {N} bytes"))
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

        let mut best_rows = best_by_partition.into_iter().collect::<Vec<_>>();
        best_rows.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        for (_, best) in &best_rows {
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

    async fn right_delta_output_delta(
        &self,
        right_delta: &ColumnarZSet,
        right_index: &SlateBackedColumnarIndexedZSet,
        right_schema: &SchemaRef,
        right_primary_key_columns: &[String],
        context: RightDeltaOutputContext<'_>,
    ) -> Result<Vec<RecordBatch>> {
        let RightDeltaOutputContext {
            output_schema,
            left_batches,
            output_index,
            output_state_indices,
        } = context;
        let timing_enabled = tracing::enabled!(tracing::Level::DEBUG);
        let total_start = timing_start(timing_enabled);
        let left_rows_start = timing_start(timing_enabled);
        let left_rows = self.left_rows_by_auction(left_batches)?;
        let left_rows_ms = elapsed_ms(left_rows_start);
        if left_rows.is_empty() {
            return Ok(Vec::new());
        }
        if columnar_zset_is_insert_only(right_delta)? {
            return self
                .right_insert_delta_output_delta(
                    right_delta,
                    RightInsertDeltaContext {
                        output: RightDeltaOutputContext {
                            output_schema,
                            left_batches,
                            output_index,
                            output_state_indices,
                        },
                        left_rows: &left_rows,
                        left_rows_ms,
                        total_start,
                    },
                )
                .await;
        }
        let candidate_start = timing_start(timing_enabled);
        let affected_partitions = self.right_delta_partition_keys(
            &left_rows,
            right_delta.batches(),
            right_delta.value_column_count(),
            RightDeltaWeightFilter::NonZero,
        )?;
        let candidate_best = self.best_bids_by_partition_with_weight_filter(
            &left_rows,
            right_delta.batches(),
            right_delta.value_column_count(),
            RightDeltaWeightFilter::Positive,
        )?;
        let candidate_ms = elapsed_ms(candidate_start);
        if affected_partitions.is_empty() {
            return Ok(Vec::new());
        }
        let previous_lookup_start = timing_start(timing_enabled);
        let previous_values = output_index
            .lookup_values_for_partition_keys(affected_partitions.iter())
            .await?;
        let previous_lookup_ms = elapsed_ms(previous_lookup_start);
        let previous_best_start = timing_start(timing_enabled);
        let previous_best = previous_best_bids_from_current_output_values(
            previous_values.iter().cloned(),
            output_schema.fields().len(),
            output_state_indices,
            affected_partitions.iter(),
        )?;
        let previous_best_ms = elapsed_ms(previous_best_start);
        let retracted_start = timing_start(timing_enabled);
        let mut retracted_current =
            self.retracted_current_best_partitions(&left_rows, right_delta, &previous_best)?;
        retracted_current
            .resolve_with_dominating_positive_candidates(&candidate_best, &previous_best);
        let retracted_ms = elapsed_ms(retracted_start);

        let build_start = timing_start(timing_enabled);
        let capacity = candidate_best
            .len()
            .saturating_add(retracted_current.partition_keys.len())
            .saturating_mul(2);
        let mut builders = output_schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), capacity))
            .collect::<Result<Vec<_>>>()?;
        let mut weights = Int64Builder::with_capacity(capacity);
        let mut row_count = 0usize;

        let compare_start = timing_start(timing_enabled);
        let mut candidates = candidate_best.iter().collect::<Vec<_>>();
        candidates.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        for (partition_key, candidate) in candidates {
            if retracted_current.partition_keys.contains(partition_key) {
                continue;
            }
            if let Some(previous) = previous_best.get(partition_key) {
                if !candidate_orders_before_previous(candidate, previous) {
                    continue;
                }
                append_current_output_row_bytes(
                    &mut builders,
                    &previous.encoded_output_row,
                    output_schema.fields().len(),
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
        let compare_ms = elapsed_ms(compare_start);

        let recompute_start = timing_start(timing_enabled);
        let recompute_batches = if retracted_current.auction_keys.is_empty() {
            Vec::new()
        } else {
            let previous_right = lookup_indexed_join_topn_state_for_i64_keys(
                right_index,
                &retracted_current.auction_keys,
                right_schema,
                "right",
            )
            .await?;
            let scoped_right_delta = filter_columnar_zset_to_i64_keys(
                right_delta,
                self.right.auction,
                &retracted_current.auction_keys,
            )?;
            let next_right = apply_source_snapshot_delta(
                right_schema,
                right_primary_key_columns,
                &previous_right,
                &scoped_right_delta,
            )
            .await?;
            let previous_output = self
                .evaluate(left_batches, &previous_right)
                .await
                .context("evaluate previous join-topn output for retracted winners")?;
            let next_output = self
                .evaluate(left_batches, &next_right)
                .await
                .context("evaluate next join-topn output for retracted winners")?;
            diff_bounded_output_batches(Arc::clone(output_schema), &previous_output, &next_output)
                .await
                .context("diff retracted-winner join-topn output")?
                .batches
        };
        let recompute_ms = elapsed_ms(recompute_start);

        let value_columns = builders
            .iter_mut()
            .map(ScalarColumnBuilder::finish_array)
            .collect::<Vec<_>>();
        let direct_batch = if row_count == 0 {
            None
        } else {
            let mut columns = value_columns;
            columns.push(Arc::new(weights.finish()) as ArrayRef);
            Some(RecordBatch::try_new(
                weighted_snapshot_schema(output_schema)?,
                columns,
            )?)
        };
        let mut output_batches = Vec::new();
        if let Some(batch) = direct_batch {
            output_batches.push(batch);
        }
        output_batches.extend(recompute_batches);
        if output_batches.is_empty() {
            tracing::debug!(
                left_row_count = left_rows.values().map(Vec::len).sum::<usize>(),
                candidate_count = candidate_best.len(),
                affected_partition_count = affected_partitions.len(),
                previous_snapshot_rows = previous_values.len(),
                previous_best_count = previous_best.len(),
                retracted_current_count = retracted_current.partition_keys.len(),
                output_delta_rows = row_count,
                left_rows_ms,
                candidate_ms,
                previous_lookup_ms,
                previous_best_ms,
                retracted_ms,
                build_ms = elapsed_ms(build_start),
                compare_ms,
                recompute_ms,
                total_ms = elapsed_ms(total_start),
                "join-topn append-only merge phase timings"
            );
            return Ok(Vec::new());
        }
        tracing::debug!(
            left_row_count = left_rows.values().map(Vec::len).sum::<usize>(),
            candidate_count = candidate_best.len(),
            affected_partition_count = affected_partitions.len(),
            previous_snapshot_rows = previous_values.len(),
            previous_best_count = previous_best.len(),
            retracted_current_count = retracted_current.partition_keys.len(),
            output_delta_rows = row_count,
            left_rows_ms,
            candidate_ms,
            previous_lookup_ms,
            previous_best_ms,
            retracted_ms,
            build_ms = elapsed_ms(build_start),
            compare_ms,
            recompute_ms,
            total_ms = elapsed_ms(total_start),
            "join-topn append-only merge phase timings"
        );
        Ok(output_batches)
    }

    async fn right_insert_delta_output_delta(
        &self,
        right_delta: &ColumnarZSet,
        context: RightInsertDeltaContext<'_>,
    ) -> Result<Vec<RecordBatch>> {
        let RightInsertDeltaContext {
            output:
                RightDeltaOutputContext {
                    output_schema,
                    left_batches,
                    output_index,
                    output_state_indices,
                },
            left_rows,
            left_rows_ms,
            total_start,
        } = context;
        let timing_enabled = tracing::enabled!(tracing::Level::DEBUG);
        let candidate_start = timing_start(timing_enabled);
        let candidate_best = self.best_bids_by_partition_with_weight_filter(
            left_rows,
            right_delta.batches(),
            right_delta.value_column_count(),
            RightDeltaWeightFilter::Positive,
        )?;
        let candidate_ms = elapsed_ms(candidate_start);
        if candidate_best.is_empty() {
            return Ok(Vec::new());
        }

        let (previous_order_count, mut replacement_keys, previous_lookup_ms, compare_ms) = {
            let previous_lookup_start = timing_start(timing_enabled);
            let previous_orders = output_index
                .order_values
                .lock()
                .map_err(|_| anyhow::anyhow!("join-topn current order index poisoned"))?;
            let previous_order_count = previous_orders.len();
            let previous_lookup_ms = elapsed_ms(previous_lookup_start);

            let compare_start = timing_start(timing_enabled);
            let mut replacement_keys = Vec::new();
            for (partition_key, candidate) in candidate_best.iter() {
                let payload = encode_partition_key_payload(partition_key)?;
                let Some(previous) = previous_orders.get(&payload) else {
                    replacement_keys.push(partition_key.clone());
                    continue;
                };
                if candidate_orders_before_order(candidate, previous) {
                    replacement_keys.push(partition_key.clone());
                }
            }
            let compare_ms = elapsed_ms(compare_start);
            (
                previous_order_count,
                replacement_keys,
                previous_lookup_ms,
                compare_ms,
            )
        };
        replacement_keys.sort();
        if replacement_keys.is_empty() {
            tracing::debug!(
                left_row_count = left_rows.values().map(Vec::len).sum::<usize>(),
                candidate_count = candidate_best.len(),
                affected_partition_count = candidate_best.len(),
                previous_snapshot_rows = previous_order_count,
                previous_best_count = previous_order_count,
                retracted_current_count = 0usize,
                output_delta_rows = 0usize,
                left_rows_ms,
                candidate_ms,
                previous_lookup_ms,
                previous_best_ms = 0u64,
                retracted_ms = 0u64,
                build_ms = compare_ms,
                compare_ms,
                recompute_ms = 0u64,
                total_ms = elapsed_ms(total_start),
                "join-topn append-only merge phase timings"
            );
            return Ok(Vec::new());
        }

        let previous_best_start = timing_start(timing_enabled);
        let previous_values = output_index
            .lookup_values_for_partition_keys(replacement_keys.iter())
            .await?;
        let previous_best = previous_best_bids_from_current_output_values(
            previous_values.iter().cloned(),
            output_schema.fields().len(),
            output_state_indices,
            replacement_keys.iter(),
        )?;
        let previous_best_ms = elapsed_ms(previous_best_start);

        let build_start = timing_start(timing_enabled);
        let capacity = replacement_keys.len().saturating_mul(2);
        let mut builders = output_schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), capacity))
            .collect::<Result<Vec<_>>>()?;
        let mut weights = Int64Builder::with_capacity(capacity);
        let mut row_count = 0usize;
        for partition_key in replacement_keys.iter() {
            if let Some(previous) = previous_best.get(partition_key) {
                append_current_output_row_bytes(
                    &mut builders,
                    &previous.encoded_output_row,
                    output_schema.fields().len(),
                )?;
                weights.append_value(-1);
                row_count += 1;
            }
            let candidate = candidate_best
                .get(partition_key)
                .ok_or_else(|| anyhow::anyhow!("join-topn replacement candidate is missing"))?;
            self.append_output_row(
                &mut builders,
                left_batches,
                right_delta.batches(),
                candidate,
            )?;
            weights.append_value(1);
            row_count += 1;
        }
        let mut columns = builders
            .iter_mut()
            .map(ScalarColumnBuilder::finish_array)
            .collect::<Vec<_>>();
        columns.push(Arc::new(weights.finish()) as ArrayRef);
        let output = vec![RecordBatch::try_new(
            weighted_snapshot_schema(output_schema)?,
            columns,
        )?];
        let build_ms = elapsed_ms(build_start);
        tracing::debug!(
            left_row_count = left_rows.values().map(Vec::len).sum::<usize>(),
            candidate_count = candidate_best.len(),
            affected_partition_count = candidate_best.len(),
            previous_snapshot_rows = previous_order_count,
            previous_best_count = previous_best.len(),
            retracted_current_count = 0usize,
            output_delta_rows = row_count,
            left_rows_ms,
            candidate_ms,
            previous_lookup_ms,
            previous_best_ms,
            retracted_ms = 0u64,
            build_ms,
            compare_ms,
            recompute_ms = 0u64,
            total_ms = elapsed_ms(total_start),
            "join-topn append-only merge phase timings"
        );
        Ok(output)
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

    fn left_delete_partition_keys(
        &self,
        left_delta: &ColumnarZSet,
    ) -> Result<HashSet<JoinTopNPartitionKey>> {
        let mut partition_keys = HashSet::new();
        let weight_idx = left_delta.value_column_count();
        for batch in left_delta.batches() {
            let weights = weight_column_at(batch, weight_idx)?;
            for row_idx in 0..batch.num_rows() {
                if weights.is_null(row_idx) || weights.value(row_idx) >= 0 {
                    continue;
                }
                partition_keys.insert(self.left_partition_key(batch, row_idx)?);
            }
        }
        Ok(partition_keys)
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

    fn best_bids_by_partition_with_weight_filter(
        &self,
        left_rows: &HashMap<i64, Vec<JoinTopNLeftRow>>,
        right_batches: &[RecordBatch],
        weight_idx: usize,
        weight_filter: RightDeltaWeightFilter,
    ) -> Result<HashMap<JoinTopNPartitionKey, JoinTopNBestBid>> {
        let mut best_by_partition = HashMap::with_capacity(left_rows.len());
        for (right_batch_idx, right_batch) in right_batches.iter().enumerate() {
            let right_auctions = int64_column(right_batch, self.right.auction)?;
            let right_bidders = int64_column(right_batch, self.right.bidder)?;
            let right_prices = int64_column(right_batch, self.right.price)?;
            let right_extras = string_column(right_batch, self.right.extra)?;
            let weights = weight_column_at(right_batch, weight_idx)?;
            for right_row_idx in 0..right_batch.num_rows() {
                if weights.is_null(right_row_idx)
                    || !right_delta_weight_matches(weight_filter, weights.value(right_row_idx))
                    || right_auctions.is_null(right_row_idx)
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

    fn right_delta_partition_keys(
        &self,
        left_rows: &HashMap<i64, Vec<JoinTopNLeftRow>>,
        right_batches: &[RecordBatch],
        weight_idx: usize,
        weight_filter: RightDeltaWeightFilter,
    ) -> Result<HashSet<JoinTopNPartitionKey>> {
        let mut partition_keys = HashSet::new();
        for right_batch in right_batches {
            let right_auctions = int64_column(right_batch, self.right.auction)?;
            let right_bidders = int64_column(right_batch, self.right.bidder)?;
            let right_prices = int64_column(right_batch, self.right.price)?;
            let weights = weight_column_at(right_batch, weight_idx)?;
            for right_row_idx in 0..right_batch.num_rows() {
                if weights.is_null(right_row_idx)
                    || !right_delta_weight_matches(weight_filter, weights.value(right_row_idx))
                    || right_auctions.is_null(right_row_idx)
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
                for left in left_matches {
                    if bid_time >= left.auction_start && bid_time <= left.auction_expires {
                        partition_keys.insert(left.partition_key.clone());
                    }
                }
            }
        }
        Ok(partition_keys)
    }

    fn retracted_current_best_partitions(
        &self,
        left_rows: &HashMap<i64, Vec<JoinTopNLeftRow>>,
        right_delta: &ColumnarZSet,
        previous_best: &HashMap<JoinTopNPartitionKey, JoinTopNPreviousBestBid>,
    ) -> Result<RetractedCurrentBestPartitions> {
        let mut auction_keys = HashSet::new();
        let mut partition_keys = HashSet::new();
        let mut partition_auction_keys = HashMap::new();
        let weight_idx = right_delta.value_column_count();
        for right_batch in right_delta.batches() {
            let right_auctions = int64_column(right_batch, self.right.auction)?;
            let right_bidders = int64_column(right_batch, self.right.bidder)?;
            let right_prices = int64_column(right_batch, self.right.price)?;
            let right_extras = string_column(right_batch, self.right.extra)?;
            let weights = weight_column_at(right_batch, weight_idx)?;
            for right_row_idx in 0..right_batch.num_rows() {
                if weights.is_null(right_row_idx)
                    || weights.value(right_row_idx) >= 0
                    || right_auctions.is_null(right_row_idx)
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
                    let Some(previous) = previous_best.get(&left.partition_key) else {
                        continue;
                    };
                    if right_row_matches_previous_best(price, bid_time, bidder, bid_extra, previous)
                    {
                        auction_keys.insert(auction_id);
                        partition_keys.insert(left.partition_key.clone());
                        partition_auction_keys.insert(left.partition_key.clone(), auction_id);
                    }
                }
            }
        }
        Ok(RetractedCurrentBestPartitions {
            auction_keys,
            partition_keys,
            partition_auction_keys,
        })
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

fn previous_best_bids_from_current_output_values<'a, I, J>(
    values: I,
    expected_columns: usize,
    output_state_indices: &JoinTopNOutputStateIndices,
    candidate_keys: J,
) -> Result<HashMap<JoinTopNPartitionKey, JoinTopNPreviousBestBid>>
where
    I: IntoIterator<Item = Bytes>,
    J: Iterator<Item = &'a JoinTopNPartitionKey>,
{
    let wanted = candidate_keys.cloned().collect::<HashSet<_>>();
    if wanted.is_empty() {
        return Ok(HashMap::new());
    }
    let mut previous = HashMap::with_capacity(wanted.len());
    for value in values {
        let (partition_key, best) =
            decode_current_output_previous_best(value, expected_columns, output_state_indices)?;
        if wanted.contains(&partition_key) {
            previous.insert(partition_key, best);
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
    candidate_orders_before_values(
        candidate,
        previous.price,
        previous.bid_time,
        previous.bidder,
        previous.bid_extra.as_deref(),
    )
}

fn candidate_orders_before_order(
    candidate: &JoinTopNBestBid,
    previous: &JoinTopNPreviousBestOrder,
) -> bool {
    candidate_orders_before_values(
        candidate,
        previous.price,
        previous.bid_time,
        previous.bidder,
        previous.bid_extra.as_deref(),
    )
}

fn candidate_orders_before_values(
    candidate: &JoinTopNBestBid,
    previous_price: i64,
    previous_bid_time: i64,
    previous_bidder: i64,
    previous_bid_extra: Option<&str>,
) -> bool {
    candidate.price > previous_price
        || (candidate.price == previous_price
            && (candidate.bid_time < previous_bid_time
                || (candidate.bid_time == previous_bid_time
                    && (candidate.bidder < previous_bidder
                        || (candidate.bidder == previous_bidder
                            && optional_str_cmp_asc(
                                candidate.bid_extra.as_deref(),
                                previous_bid_extra,
                            ) == Ordering::Less)))))
}

fn right_row_matches_previous_best(
    price: i64,
    bid_time: i64,
    bidder: i64,
    bid_extra: Option<&str>,
    previous: &JoinTopNPreviousBestBid,
) -> bool {
    price == previous.price
        && bid_time == previous.bid_time
        && bidder == previous.bidder
        && bid_extra == previous.bid_extra.as_deref()
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

fn joins_for_plan(plan: &LogicalPlan) -> Vec<&Join> {
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
        let (Some(left), Some(right)) = (expr_column(left), expr_column(right)) else {
            continue;
        };
        candidates.push((left.clone(), right.clone()));
    }
    if let Some(filter) = join.filter.as_ref() {
        collect_equality_column_pairs(filter, &mut candidates);
    }
    let left_schema = &sources.get(left_source)?.schema;
    let right_schema = &sources.get(right_source)?.schema;
    let left_relations = relation_names_for_plan(join.left.as_ref());
    let right_relations = relation_names_for_plan(join.right.as_ref());
    for (first, second) in candidates {
        let first_side = join_column_side(
            &first,
            left_schema,
            right_schema,
            &left_relations,
            &right_relations,
        );
        let second_side = join_column_side(
            &second,
            left_schema,
            right_schema,
            &left_relations,
            &right_relations,
        );
        match (first_side, second_side) {
            (Some(JoinInputSide::Left), Some(JoinInputSide::Right)) => {
                return Some((first.name, second.name));
            }
            (Some(JoinInputSide::Right), Some(JoinInputSide::Left)) => {
                return Some((second.name, first.name));
            }
            _ => {}
        }
    }
    None
}

fn collect_equality_column_pairs(
    expr: &Expr,
    out: &mut Vec<(datafusion::common::Column, datafusion::common::Column)>,
) {
    let Expr::BinaryExpr(binary) = expr else {
        return;
    };
    if binary.op == Operator::Eq {
        if let (Some(left), Some(right)) = (expr_column(&binary.left), expr_column(&binary.right)) {
            out.push((left.clone(), right.clone()));
        }
        return;
    }
    if matches!(binary.op, Operator::And) {
        collect_equality_column_pairs(&binary.left, out);
        collect_equality_column_pairs(&binary.right, out);
    }
}

fn expr_column(expr: &Expr) -> Option<&datafusion::common::Column> {
    match expr {
        Expr::Column(column) => Some(column),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JoinInputSide {
    Left,
    Right,
}

fn join_column_side(
    column: &datafusion::common::Column,
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    left_relations: &BTreeSet<String>,
    right_relations: &BTreeSet<String>,
) -> Option<JoinInputSide> {
    if let Some(relation) = column.relation.as_ref().map(ToString::to_string) {
        let in_left = left_relations.contains(&relation);
        let in_right = right_relations.contains(&relation);
        return match (in_left, in_right) {
            (true, false) => Some(JoinInputSide::Left),
            (false, true) => Some(JoinInputSide::Right),
            _ => None,
        };
    }
    let in_left = left_schema.index_of(&column.name).is_ok();
    let in_right = right_schema.index_of(&column.name).is_ok();
    match (in_left, in_right) {
        (true, false) => Some(JoinInputSide::Left),
        (false, true) => Some(JoinInputSide::Right),
        _ => None,
    }
}

fn left_partition_columns_by_join_key(
    window: &Window,
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    left_relations: &BTreeSet<String>,
    right_relations: &BTreeSet<String>,
    left_key_column: &str,
) -> Option<Vec<String>> {
    let mut accepted = None;
    for expr in &window.window_expr {
        let Expr::WindowFunction(window) = super::columnar_utils::strip_alias(expr) else {
            return None;
        };
        let mut columns = Vec::with_capacity(window.params.partition_by.len());
        for partition_expr in &window.params.partition_by {
            let Expr::Column(column) = super::columnar_utils::strip_alias(partition_expr) else {
                return None;
            };
            if join_column_side(
                column,
                left_schema,
                right_schema,
                left_relations,
                right_relations,
            ) != Some(JoinInputSide::Left)
            {
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
    let sources = super::columnar_utils::source_set_for_plan(plan, sources);
    if sources.len() == 1 {
        sources.into_iter().next()
    } else {
        None
    }
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
