use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::logical_expr::logical_plan::{Join, TableScan};
use datafusion::logical_expr::{
    Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, ScalarUDF,
};
use datafusion::physical_plan::collect;
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarIndexedZSet, SlateBackedColumnarZSet};
use dbsp::storage::KeyValueTable;

use crate::delta_consolidation::{
    add_weight_column_to_batches, diff_snapshot_batches, weighted_snapshot_schema,
};
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::table_provider::DynamicStateTableProvider;
use crate::vectorized_runtime::source_state::{rename_batches, resolve_source_table};
use crate::vectorized_source_delta::unit_source_delta_batches;

use super::columnar_composed::{
    ColumnarComposedMaterializedViewState, ColumnarComposedPlan,
    build_columnar_join_aggregate_materialized_view_state_in_namespace,
    columnar_join_aggregate_plan_for_plan, run_columnar_composed_state_tick,
};
use super::columnar_grouped_count::{
    ColumnarGroupedCountMaterializedViewState, ColumnarGroupedCountPlan,
    build_columnar_grouped_count_materialized_view_state_in_namespace,
    columnar_grouped_count_plan_for_plan, run_columnar_grouped_count_state_tick,
};
use super::columnar_grouped_max::{
    ColumnarGroupedMaxMaterializedViewState, ColumnarGroupedMaxPlan,
    build_columnar_grouped_max_materialized_view_state_in_namespace,
    columnar_grouped_max_plan_for_plan, run_columnar_grouped_max_state_tick,
};
use super::columnar_grouped_stats::{
    ColumnarGroupedStatsMaterializedViewState, ColumnarGroupedStatsPlan,
    build_columnar_grouped_stats_materialized_view_state_in_namespace,
    columnar_grouped_stats_plan_for_plan, run_columnar_grouped_stats_state_tick,
};
use super::columnar_join_topn::{
    ColumnarJoinTopNMaterializedViewState, ColumnarJoinTopNPlan,
    build_columnar_join_topn_materialized_view_state_in_namespaces,
    columnar_join_topn_plan_for_plan, run_columnar_join_topn_state_tick,
};
use super::columnar_multijoin::{
    ColumnarMultiJoinMaterializedViewState, ColumnarMultiJoinPlan,
    build_columnar_multijoin_materialized_view_state_in_namespace,
    columnar_multijoin_plan_for_plan, run_columnar_multijoin_state_tick,
};
use super::columnar_topn::{TopNEvaluator, columnar_topn_plan_for_plan};
use super::columnar_union::{
    ColumnarUnionMaterializedViewState, ColumnarUnionPlan,
    build_columnar_union_materialized_view_state_in_namespace, columnar_union_plan_for_plan,
    run_columnar_union_state_tick,
};
use super::{
    VectorizedMaterializedViewState, VectorizedSourceState, apply_weighted_snapshot_delta,
    normalize_batches,
};

pub(super) struct ColumnarJoinPlan {
    logical_plan: LogicalPlan,
    left: ColumnarJoinInputPlan,
    right: ColumnarJoinInputPlan,
    join_key_pairs: Vec<ColumnarJoinKeyPair>,
    execution_strategy: ColumnarJoinExecutionStrategy,
}

impl ColumnarJoinPlan {
    pub(super) fn source_names(&self) -> BTreeSet<String> {
        self.left
            .source_names()
            .into_iter()
            .chain(self.right.source_names())
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColumnarJoinExecutionStrategy {
    IncrementalInner,
    SnapshotDiff,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ColumnarJoinKeyPair {
    left: String,
    right: String,
}

struct ColumnarJoinKeyIndices {
    left: Vec<usize>,
    right: Vec<usize>,
}

pub(super) struct ColumnarJoinMaterializedViewState {
    left: ColumnarJoinSourceState,
    right: ColumnarJoinSourceState,
    output_zset: SlateBackedColumnarZSet,
    join_key_indices: Option<ColumnarJoinKeyIndices>,
    left_delta_right_state: Option<JoinDeltaEvaluator>,
    left_state_right_delta: Option<JoinDeltaEvaluator>,
    left_delta_right_delta: JoinDeltaEvaluator,
    initial_snapshot: Vec<RecordBatch>,
    execution_strategy: ColumnarJoinExecutionStrategy,
}

impl ColumnarJoinMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }

    #[cfg(test)]
    pub(super) fn execution_strategy_name(&self) -> &'static str {
        match self.execution_strategy {
            ColumnarJoinExecutionStrategy::IncrementalInner => "incremental_inner",
            ColumnarJoinExecutionStrategy::SnapshotDiff => "snapshot_diff",
        }
    }
}

struct ColumnarJoinSourceState {
    input_name: String,
    source_name: Option<String>,
    schema: SchemaRef,
    input_zset: Option<SlateBackedColumnarZSet>,
    input_index: Option<Box<SlateBackedColumnarIndexedZSet>>,
    snapshot: Vec<RecordBatch>,
    constant: Option<ColumnarJoinConstantState>,
    topn: Option<ColumnarJoinTopNInputState>,
    join_topn: Option<ColumnarJoinJoinTopNInputState>,
    join: Option<ColumnarJoinJoinInputState>,
    multijoin: Option<ColumnarJoinMultiJoinInputState>,
    union: Option<ColumnarJoinUnionInputState>,
    grouped_max: Option<ColumnarJoinGroupedMaxInputState>,
    grouped_count: Option<ColumnarJoinGroupedCountInputState>,
    grouped_stats: Option<ColumnarJoinGroupedStatsInputState>,
    join_aggregate: Option<ColumnarJoinJoinAggregateInputState>,
}

struct ColumnarJoinConstantState {
    state_table: Arc<dyn KeyValueTable>,
    initialized_key: Vec<u8>,
    initialized: bool,
    pending_snapshot: Vec<RecordBatch>,
}

struct ColumnarJoinTopNInputState {
    source_name: String,
    source_schema: SchemaRef,
    source_input_zset: SlateBackedColumnarZSet,
    source_snapshot: Vec<RecordBatch>,
    evaluator: TopNEvaluator,
}

struct ColumnarJoinJoinTopNInputState {
    state: ColumnarJoinTopNMaterializedViewState,
}

struct ColumnarJoinJoinInputState {
    state: Box<ColumnarJoinMaterializedViewState>,
}

struct ColumnarJoinMultiJoinInputState {
    state: ColumnarMultiJoinMaterializedViewState,
}

struct ColumnarJoinUnionInputState {
    state: ColumnarUnionMaterializedViewState,
}

struct ColumnarJoinGroupedMaxInputState {
    state: Box<ColumnarGroupedMaxMaterializedViewState>,
}

struct ColumnarJoinGroupedCountInputState {
    state: Box<ColumnarGroupedCountMaterializedViewState>,
}

struct ColumnarJoinGroupedStatsInputState {
    state: Box<ColumnarGroupedStatsMaterializedViewState>,
}

struct ColumnarJoinJoinAggregateInputState {
    state: Box<ColumnarComposedMaterializedViewState>,
}

struct ColumnarJoinInputPlan {
    input_name: String,
    schema: SchemaRef,
    kind: ColumnarJoinInputPlanKind,
}

enum ColumnarJoinInputPlanKind {
    Source {
        source_name: String,
    },
    Constant {
        logical_plan: LogicalPlan,
    },
    TopN {
        source_name: String,
        logical_plan: LogicalPlan,
    },
    JoinTopN {
        plan: ColumnarJoinTopNPlan,
    },
    Join {
        plan: Box<ColumnarJoinPlan>,
    },
    MultiJoin {
        plan: ColumnarMultiJoinPlan,
    },
    Union {
        plan: ColumnarUnionPlan,
    },
    GroupedMax {
        plan: ColumnarGroupedMaxPlan,
    },
    GroupedCount {
        plan: ColumnarGroupedCountPlan,
    },
    GroupedStats {
        plan: Box<ColumnarGroupedStatsPlan>,
    },
    JoinAggregate {
        plan: Box<ColumnarComposedPlan>,
    },
}

struct JoinInputTick {
    delta: ColumnarZSet,
    changed: bool,
    next_snapshot: Option<Vec<RecordBatch>>,
}

pub(super) struct ColumnarJoinTick {
    pub(super) delta: ColumnarZSet,
    pub(super) next_snapshot: Vec<RecordBatch>,
    pub(super) input_changed: bool,
}

struct JoinDeltaEvaluator {
    ctx: SessionContext,
    logical_plan: LogicalPlan,
    plan: Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    rebuild_each_evaluate: bool,
    left_input: JoinEvaluatorInput,
    right_input: JoinEvaluatorInput,
    output_schema: SchemaRef,
}

struct JoinEvaluatorInput {
    provider: Arc<DynamicStateTableProvider>,
    alias_schema: Option<SchemaRef>,
    alias_provider: Option<Arc<DynamicStateTableProvider>>,
}

struct JoinEvaluatorInputPlan {
    input_name: String,
    schema: SchemaRef,
    source_name: Option<String>,
}

impl JoinEvaluatorInputPlan {
    fn from_join_input(input: &ColumnarJoinInputPlan) -> Self {
        Self {
            input_name: input.input_name.clone(),
            schema: Arc::clone(&input.schema),
            source_name: input.source_name(),
        }
    }
}

struct JoinSignedDelta {
    positive: Vec<RecordBatch>,
    negative: Vec<RecordBatch>,
}

pub(super) fn columnar_join_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarJoinPlan>> {
    columnar_join_plan_for_plan_with_options(plan, sources, true)
}

fn columnar_join_plan_for_plan_with_options(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    skip_derived_join_inputs: bool,
) -> Result<Option<ColumnarJoinPlan>> {
    let mut joins = Vec::new();
    collect_joins(plan, sources, &mut joins, false, skip_derived_join_inputs)?;
    let [join] = joins.as_slice() else {
        return Ok(None);
    };
    if !is_supported_join_type(&join.join_type) || (join.on.is_empty() && join.filter.is_none()) {
        return Ok(None);
    }
    let Some(left) = join_input_plan_for_side(join.left.as_ref(), sources, "left")? else {
        return Ok(None);
    };
    let Some(right) = join_input_plan_for_side(join.right.as_ref(), sources, "right")? else {
        return Ok(None);
    };
    let all_sources = source_set_for_plan(plan, sources);
    let expected_sources = left
        .source_names()
        .into_iter()
        .chain(right.source_names())
        .collect::<BTreeSet<_>>();
    if all_sources != expected_sources {
        return Ok(None);
    }
    if contains_unsupported_join_wrapper(plan, sources)? {
        return Ok(None);
    }
    let join_key_pairs = simple_join_key_pairs(join, &left.schema, &right.schema);
    let execution_strategy = if matches!(join.join_type, JoinType::Inner)
        && left.source_name() != right.source_name()
        && left.is_source()
        && right.is_source()
        && !join_key_pairs.is_empty()
    {
        ColumnarJoinExecutionStrategy::IncrementalInner
    } else {
        ColumnarJoinExecutionStrategy::SnapshotDiff
    };

    Ok(Some(ColumnarJoinPlan {
        logical_plan: plan.clone(),
        left,
        right,
        join_key_pairs,
        execution_strategy,
    }))
}

fn simple_join_key_pairs(
    join: &Join,
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
) -> Vec<ColumnarJoinKeyPair> {
    let mut pairs = Vec::new();
    for (left, right) in &join.on {
        if let Some(pair) = oriented_join_key_pair(left, right, left_schema, right_schema) {
            pairs.push(pair);
        }
    }
    if let Some(filter) = join.filter.as_ref() {
        collect_filter_join_key_pairs(filter, left_schema, right_schema, &mut pairs);
    }
    let mut seen = BTreeSet::new();
    pairs.retain(|pair| seen.insert((pair.left.clone(), pair.right.clone())));
    pairs
}

fn collect_filter_join_key_pairs(
    expr: &Expr,
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    out: &mut Vec<ColumnarJoinKeyPair>,
) {
    let Expr::BinaryExpr(binary) = expr else {
        return;
    };
    if binary.op == Operator::And {
        collect_filter_join_key_pairs(binary.left.as_ref(), left_schema, right_schema, out);
        collect_filter_join_key_pairs(binary.right.as_ref(), left_schema, right_schema, out);
        return;
    }
    if binary.op == Operator::Eq
        && let Some(pair) =
            oriented_join_key_pair(&binary.left, &binary.right, left_schema, right_schema)
    {
        out.push(pair);
    }
}

fn oriented_join_key_pair(
    first: &Expr,
    second: &Expr,
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
) -> Option<ColumnarJoinKeyPair> {
    let (Expr::Column(first), Expr::Column(second)) = (first, second) else {
        return None;
    };
    let first_left = left_schema.index_of(&first.name).is_ok();
    let first_right = right_schema.index_of(&first.name).is_ok();
    let second_left = left_schema.index_of(&second.name).is_ok();
    let second_right = right_schema.index_of(&second.name).is_ok();
    if first_left && second_right && (!first_right || !second_left || first.name == second.name) {
        return Some(ColumnarJoinKeyPair {
            left: first.name.clone(),
            right: second.name.clone(),
        });
    }
    if second_left && first_right && (!second_right || !first_left || first.name == second.name) {
        return Some(ColumnarJoinKeyPair {
            left: second.name.clone(),
            right: first.name.clone(),
        });
    }
    None
}

fn resolve_join_key_indices(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    key_pairs: &[ColumnarJoinKeyPair],
) -> Result<ColumnarJoinKeyIndices> {
    let left = key_pairs
        .iter()
        .map(|pair| {
            left_schema.index_of(&pair.left).with_context(|| {
                format!(
                    "left join key column '{}' not found in input schema",
                    pair.left
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let right = key_pairs
        .iter()
        .map(|pair| {
            right_schema.index_of(&pair.right).with_context(|| {
                format!(
                    "right join key column '{}' not found in input schema",
                    pair.right
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ColumnarJoinKeyIndices { left, right })
}

pub(super) async fn build_columnar_join_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarJoinPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarJoinMaterializedViewState> {
    let mv_namespace = namespaces::materialized_view(view_name)?;
    Box::pin(build_columnar_join_materialized_view_state_in_namespace(
        table,
        mv_namespace,
        output_schema,
        plan,
        sources,
        udfs,
    ))
    .await
}

pub(super) async fn build_columnar_join_materialized_view_state_in_namespace(
    table: Arc<dyn KeyValueTable>,
    mv_namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarJoinPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarJoinMaterializedViewState> {
    let ColumnarJoinPlan {
        logical_plan,
        left,
        right,
        join_key_pairs,
        execution_strategy,
    } = plan;
    let join_key_indices = if execution_strategy == ColumnarJoinExecutionStrategy::IncrementalInner
    {
        Some(
            resolve_join_key_indices(&left.schema, &right.schema, &join_key_pairs)
                .context("resolve columnar join index keys")?,
        )
    } else {
        None
    };
    let shared_source_input =
        left.is_source() && right.is_source() && left.source_name() == right.source_name();
    let (left_namespace, right_namespace) = if shared_source_input {
        (
            format!(
                "{mv_namespace}/columnar/join/left/{}/input",
                left.input_name
            ),
            format!(
                "{mv_namespace}/columnar/join/right/{}/input",
                right.input_name
            ),
        )
    } else {
        (
            join_input_namespace(&mv_namespace, "left", &left),
            join_input_namespace(&mv_namespace, "right", &right),
        )
    };
    let output_namespace = format!("{mv_namespace}/columnar/join/output");

    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed join output zset")?;
    let initial_snapshot = snapshot_batches_from_zset(
        &output_zset
            .materialize_columnar()
            .await
            .context("load join output snapshot")?,
    )?;
    let output_initialized = output_zset.current_handle().is_some();

    let left_evaluator_plan = JoinEvaluatorInputPlan::from_join_input(&left);
    let right_evaluator_plan = JoinEvaluatorInputPlan::from_join_input(&right);

    let left = Box::pin(build_join_input_state(
        Arc::clone(&table),
        &mv_namespace,
        "left",
        left_namespace,
        left,
        sources,
        udfs,
        output_initialized,
        join_key_indices
            .as_ref()
            .map(|indices| indices.left.as_slice()),
    ))
    .await
    .context("build SlateDB-backed left join input state")?;
    let right = Box::pin(build_join_input_state(
        table,
        &mv_namespace,
        "right",
        right_namespace,
        right,
        sources,
        udfs,
        output_initialized,
        join_key_indices
            .as_ref()
            .map(|indices| indices.right.as_slice()),
    ))
    .await
    .context("build SlateDB-backed right join input state")?;

    // DataFusion physical join plans are not reusable with swapped dynamic inputs across
    // collect calls. Keep the logical plan cached, but rebuild the physical plan per delta
    // evaluation so cross-tick indexed joins observe the current provider batches.
    let rebuild_each_evaluate = true;
    let build_incremental_delta_evaluators =
        execution_strategy == ColumnarJoinExecutionStrategy::IncrementalInner;
    let left_delta_right_state = if build_incremental_delta_evaluators {
        Some(
            JoinDeltaEvaluator::build(
                logical_plan.clone(),
                sources,
                udfs,
                output_schema,
                &left_evaluator_plan,
                &right_evaluator_plan,
                rebuild_each_evaluate,
            )
            .await
            .context("build left-delta/right-state join evaluator")?,
        )
    } else {
        None
    };
    let left_state_right_delta = if build_incremental_delta_evaluators {
        Some(
            JoinDeltaEvaluator::build(
                logical_plan.clone(),
                sources,
                udfs,
                output_schema,
                &left_evaluator_plan,
                &right_evaluator_plan,
                rebuild_each_evaluate,
            )
            .await
            .context("build left-state/right-delta join evaluator")?,
        )
    } else {
        None
    };
    let left_delta_right_delta = JoinDeltaEvaluator::build(
        logical_plan,
        sources,
        udfs,
        output_schema,
        &left_evaluator_plan,
        &right_evaluator_plan,
        rebuild_each_evaluate,
    )
    .await
    .context("build left-delta/right-delta join evaluator")?;

    Ok(ColumnarJoinMaterializedViewState {
        left,
        right,
        output_zset,
        join_key_indices,
        left_delta_right_state,
        left_state_right_delta,
        left_delta_right_delta,
        initial_snapshot,
        execution_strategy,
    })
}

fn join_input_namespace(mv_namespace: &str, side: &str, input: &ColumnarJoinInputPlan) -> String {
    match &input.kind {
        ColumnarJoinInputPlanKind::Source { .. } => {
            format!("{mv_namespace}/columnar/join/{}/input", input.input_name)
        }
        ColumnarJoinInputPlanKind::Constant { .. } => {
            format!("{mv_namespace}/columnar/join/{side}/constant/input")
        }
        ColumnarJoinInputPlanKind::TopN { .. } => {
            format!("{mv_namespace}/columnar/join/{side}/topn/input")
        }
        ColumnarJoinInputPlanKind::JoinTopN { .. } => {
            format!("{mv_namespace}/columnar/join/{side}/join_topn")
        }
        ColumnarJoinInputPlanKind::Join { .. } => {
            format!("{mv_namespace}/columnar/join/{side}/join")
        }
        ColumnarJoinInputPlanKind::MultiJoin { .. } => {
            format!("{mv_namespace}/columnar/join/{side}/multijoin")
        }
        ColumnarJoinInputPlanKind::Union { .. } => {
            format!("{mv_namespace}/columnar/join/{side}/union")
        }
        ColumnarJoinInputPlanKind::GroupedMax { .. } => {
            format!("{mv_namespace}/columnar/join/{side}/grouped_max")
        }
        ColumnarJoinInputPlanKind::GroupedCount { .. } => {
            format!("{mv_namespace}/columnar/join/{side}/grouped_count")
        }
        ColumnarJoinInputPlanKind::GroupedStats { .. } => {
            format!("{mv_namespace}/columnar/join/{side}/grouped_stats")
        }
        ColumnarJoinInputPlanKind::JoinAggregate { .. } => {
            format!("{mv_namespace}/columnar/join/{side}/join_aggregate")
        }
    }
}

async fn build_join_side_input_zset(
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    schema: &SchemaRef,
    side: &str,
    input_name: &str,
) -> Result<SlateBackedColumnarZSet> {
    Box::pin(SlateBackedColumnarZSet::new(
        table,
        namespace,
        Arc::clone(schema),
    ))
    .await
    .with_context(|| format!("initialize SlateDB-backed {side} join input zset for '{input_name}'"))
}

async fn build_join_input_state(
    table: Arc<dyn KeyValueTable>,
    mv_namespace: &str,
    side: &str,
    namespace: String,
    input: ColumnarJoinInputPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
    output_initialized: bool,
    index_key_indices: Option<&[usize]>,
) -> Result<ColumnarJoinSourceState> {
    match input.kind {
        ColumnarJoinInputPlanKind::Source { source_name } => {
            let index_namespace = format!("{namespace}/index");
            let input_zset = Box::pin(build_join_side_input_zset(
                Arc::clone(&table),
                namespace,
                &input.schema,
                side,
                &input.input_name,
            ))
            .await?;
            let source = sources
                .get(&source_name)
                .ok_or_else(|| anyhow::anyhow!("unknown join source '{source_name}'"))?;
            let snapshot_zset = input_zset
                .materialize_columnar()
                .await
                .with_context(|| format!("load {side} join input snapshot"))?;
            let snapshot = snapshot_batches_from_zset(&snapshot_zset)?;
            let input_index = if let Some(key_indices) = index_key_indices {
                let mut index = SlateBackedColumnarIndexedZSet::new(
                    Arc::clone(&table),
                    index_namespace,
                    Arc::clone(&source.schema),
                    key_indices.to_vec(),
                )
                .await
                .with_context(|| {
                    format!(
                        "initialize SlateDB-backed {side} join input index for '{}'",
                        input.input_name
                    )
                })?;
                index
                    .rebuild_from_zset(&snapshot_zset)
                    .await
                    .with_context(|| {
                        format!(
                            "rebuild SlateDB-backed {side} join input index for '{}'",
                            input.input_name
                        )
                    })?;
                Some(Box::new(index))
            } else {
                None
            };
            Ok(ColumnarJoinSourceState {
                input_name: input.input_name,
                source_name: Some(source_name),
                schema: Arc::clone(&source.schema),
                snapshot,
                input_zset: Some(input_zset),
                input_index,
                constant: None,
                topn: None,
                join_topn: None,
                join: None,
                multijoin: None,
                union: None,
                grouped_max: None,
                grouped_count: None,
                grouped_stats: None,
                join_aggregate: None,
            })
        }
        ColumnarJoinInputPlanKind::Constant { logical_plan } => {
            let input_zset = build_join_side_input_zset(
                Arc::clone(&table),
                namespace,
                &input.schema,
                side,
                &input.input_name,
            )
            .await?;
            let initialized_key =
                format!("{mv_namespace}/columnar/join/{side}/constant/state/initialized")
                    .into_bytes();
            let initialized = table
                .get_bytes(&initialized_key)
                .await
                .with_context(|| format!("read {side} join constant initialized marker"))?
                .is_some()
                || output_initialized;
            let has_persisted_input = input_zset.current_handle().is_some();
            let persisted_snapshot = if has_persisted_input {
                snapshot_batches_from_zset(
                    &input_zset
                        .materialize_columnar()
                        .await
                        .with_context(|| format!("load {side} join constant input snapshot"))?,
                )?
            } else {
                vec![RecordBatch::new_empty(Arc::clone(&input.schema))]
            };
            let pending_snapshot = if initialized {
                Vec::new()
            } else if has_persisted_input {
                persisted_snapshot.clone()
            } else {
                evaluate_constant_join_input(logical_plan, &input.schema, udfs)
                    .await
                    .with_context(|| format!("evaluate {side} join constant input"))?
            };
            let snapshot = if initialized || has_persisted_input {
                persisted_snapshot
            } else {
                vec![RecordBatch::new_empty(Arc::clone(&input.schema))]
            };
            Ok(ColumnarJoinSourceState {
                input_name: input.input_name,
                source_name: None,
                schema: input.schema,
                input_zset: Some(input_zset),
                input_index: None,
                snapshot,
                constant: Some(ColumnarJoinConstantState {
                    state_table: table,
                    initialized_key,
                    initialized,
                    pending_snapshot,
                }),
                topn: None,
                join_topn: None,
                join: None,
                multijoin: None,
                union: None,
                grouped_max: None,
                grouped_count: None,
                grouped_stats: None,
                join_aggregate: None,
            })
        }
        ColumnarJoinInputPlanKind::TopN {
            source_name,
            logical_plan,
        } => {
            let input_zset = build_join_side_input_zset(
                Arc::clone(&table),
                namespace,
                &input.schema,
                side,
                &input.input_name,
            )
            .await?;
            let source = sources
                .get(&source_name)
                .ok_or_else(|| anyhow::anyhow!("unknown topn join source '{source_name}'"))?;
            let source_input_namespace =
                format!("{mv_namespace}/columnar/join/{side}/topn/source_input");
            let source_input_zset = SlateBackedColumnarZSet::new(
                Arc::clone(&table),
                source_input_namespace,
                Arc::clone(&source.schema),
            )
            .await
            .with_context(|| {
                format!(
                    "initialize SlateDB-backed {side} join topn raw input zset for '{}'",
                    input.input_name
                )
            })?;
            let source_snapshot = snapshot_batches_from_zset(
                &source_input_zset
                    .materialize_columnar()
                    .await
                    .with_context(|| format!("load {side} join topn raw input snapshot"))?,
            )?;
            let snapshot = snapshot_batches_from_zset(
                &input_zset
                    .materialize_columnar()
                    .await
                    .with_context(|| format!("load {side} join topn input snapshot"))?,
            )?;
            let evaluator =
                TopNEvaluator::build(logical_plan, &source_name, source, udfs, &input.schema)
                    .await
                    .with_context(|| {
                        format!(
                            "build SlateDB-backed {side} join topn evaluator for '{}'",
                            input.input_name
                        )
                    })?;
            Ok(ColumnarJoinSourceState {
                input_name: input.input_name,
                source_name: None,
                schema: input.schema,
                input_zset: Some(input_zset),
                input_index: None,
                snapshot,
                constant: None,
                topn: Some(ColumnarJoinTopNInputState {
                    source_name,
                    source_schema: Arc::clone(&source.schema),
                    source_input_zset,
                    source_snapshot,
                    evaluator,
                }),
                join_topn: None,
                join: None,
                multijoin: None,
                union: None,
                grouped_max: None,
                grouped_count: None,
                grouped_stats: None,
                join_aggregate: None,
            })
        }
        ColumnarJoinInputPlanKind::JoinTopN { plan } => {
            let left_namespace = format!("{namespace}/left_input");
            let right_namespace = format!("{namespace}/right_input");
            let output_namespace = format!("{namespace}/output");
            let state = build_columnar_join_topn_materialized_view_state_in_namespaces(
                table,
                left_namespace,
                right_namespace,
                output_namespace,
                &input.schema,
                plan,
                sources,
                udfs,
            )
            .await
            .with_context(|| {
                format!(
                    "build SlateDB-backed {side} join join-topn input for '{}'",
                    input.input_name
                )
            })?;
            let snapshot = state.initial_snapshot();
            Ok(ColumnarJoinSourceState {
                input_name: input.input_name,
                source_name: None,
                schema: input.schema,
                input_zset: None,
                input_index: None,
                snapshot,
                constant: None,
                topn: None,
                join_topn: Some(ColumnarJoinJoinTopNInputState { state }),
                join: None,
                multijoin: None,
                union: None,
                grouped_max: None,
                grouped_count: None,
                grouped_stats: None,
                join_aggregate: None,
            })
        }
        ColumnarJoinInputPlanKind::Join { plan } => {
            let join_namespace = format!("{namespace}/state");
            let state = Box::pin(build_columnar_join_materialized_view_state_in_namespace(
                table,
                join_namespace,
                &input.schema,
                *plan,
                sources,
                udfs,
            ))
            .await
            .with_context(|| {
                format!(
                    "build SlateDB-backed {side} nested join input for '{}'",
                    input.input_name
                )
            })?;
            let snapshot = state.initial_snapshot();
            Ok(ColumnarJoinSourceState {
                input_name: input.input_name,
                source_name: None,
                schema: input.schema,
                input_zset: None,
                input_index: None,
                snapshot,
                constant: None,
                topn: None,
                join_topn: None,
                join: Some(ColumnarJoinJoinInputState {
                    state: Box::new(state),
                }),
                multijoin: None,
                union: None,
                grouped_max: None,
                grouped_count: None,
                grouped_stats: None,
                join_aggregate: None,
            })
        }
        ColumnarJoinInputPlanKind::MultiJoin { plan } => {
            let multijoin_namespace = format!("{namespace}/state");
            let state = build_columnar_multijoin_materialized_view_state_in_namespace(
                table,
                multijoin_namespace,
                &input.schema,
                plan,
                sources,
                udfs,
            )
            .await
            .with_context(|| {
                format!(
                    "build SlateDB-backed {side} nested multijoin input for '{}'",
                    input.input_name
                )
            })?;
            let snapshot = state.initial_snapshot();
            Ok(ColumnarJoinSourceState {
                input_name: input.input_name,
                source_name: None,
                schema: input.schema,
                input_zset: None,
                input_index: None,
                snapshot,
                constant: None,
                topn: None,
                join_topn: None,
                join: None,
                multijoin: Some(ColumnarJoinMultiJoinInputState { state }),
                union: None,
                grouped_max: None,
                grouped_count: None,
                grouped_stats: None,
                join_aggregate: None,
            })
        }
        ColumnarJoinInputPlanKind::Union { plan } => {
            let union_namespace = format!("{namespace}/state");
            let state = build_columnar_union_materialized_view_state_in_namespace(
                table,
                union_namespace,
                &input.schema,
                plan,
                sources,
                udfs,
            )
            .await
            .with_context(|| {
                format!(
                    "build SlateDB-backed {side} nested union input for '{}'",
                    input.input_name
                )
            })?;
            let snapshot = state.initial_snapshot();
            Ok(ColumnarJoinSourceState {
                input_name: input.input_name,
                source_name: None,
                schema: input.schema,
                input_zset: None,
                input_index: None,
                snapshot,
                constant: None,
                topn: None,
                join_topn: None,
                join: None,
                multijoin: None,
                union: Some(ColumnarJoinUnionInputState { state }),
                grouped_max: None,
                grouped_count: None,
                grouped_stats: None,
                join_aggregate: None,
            })
        }
        ColumnarJoinInputPlanKind::GroupedMax { plan } => {
            let grouped_max_namespace = format!("{namespace}/state");
            let state = Box::pin(build_boxed_grouped_max_join_input_state(
                table,
                grouped_max_namespace,
                &input.schema,
                plan,
                sources,
                udfs,
            ))
            .await
            .with_context(|| {
                format!(
                    "build SlateDB-backed {side} nested grouped-max input for '{}'",
                    input.input_name
                )
            })?;
            let snapshot = state.initial_snapshot();
            Ok(ColumnarJoinSourceState {
                input_name: input.input_name,
                source_name: None,
                schema: input.schema,
                input_zset: None,
                input_index: None,
                snapshot,
                constant: None,
                topn: None,
                join_topn: None,
                join: None,
                multijoin: None,
                union: None,
                grouped_max: Some(ColumnarJoinGroupedMaxInputState { state }),
                grouped_count: None,
                grouped_stats: None,
                join_aggregate: None,
            })
        }
        ColumnarJoinInputPlanKind::GroupedCount { plan } => {
            let grouped_count_namespace = format!("{namespace}/state");
            let state = Box::pin(build_boxed_grouped_count_join_input_state(
                table,
                grouped_count_namespace,
                &input.schema,
                plan,
                sources,
                udfs,
            ))
            .await
            .with_context(|| {
                format!(
                    "build SlateDB-backed {side} nested grouped-count input for '{}'",
                    input.input_name
                )
            })?;
            let snapshot = state.initial_snapshot();
            Ok(ColumnarJoinSourceState {
                input_name: input.input_name,
                source_name: None,
                schema: input.schema,
                input_zset: None,
                input_index: None,
                snapshot,
                constant: None,
                topn: None,
                join_topn: None,
                join: None,
                multijoin: None,
                union: None,
                grouped_max: None,
                grouped_count: Some(ColumnarJoinGroupedCountInputState { state }),
                grouped_stats: None,
                join_aggregate: None,
            })
        }
        ColumnarJoinInputPlanKind::GroupedStats { plan } => {
            let grouped_stats_namespace = format!("{namespace}/state");
            let state = Box::pin(build_boxed_grouped_stats_join_input_state(
                table,
                grouped_stats_namespace,
                &input.schema,
                *plan,
                sources,
                udfs,
            ))
            .await
            .with_context(|| {
                format!(
                    "build SlateDB-backed {side} nested grouped-stats input for '{}'",
                    input.input_name
                )
            })?;
            let snapshot = state.initial_snapshot();
            Ok(ColumnarJoinSourceState {
                input_name: input.input_name,
                source_name: None,
                schema: input.schema,
                input_zset: None,
                input_index: None,
                snapshot,
                constant: None,
                topn: None,
                join_topn: None,
                join: None,
                multijoin: None,
                union: None,
                grouped_max: None,
                grouped_count: None,
                grouped_stats: Some(ColumnarJoinGroupedStatsInputState { state }),
                join_aggregate: None,
            })
        }
        ColumnarJoinInputPlanKind::JoinAggregate { plan } => {
            let join_aggregate_namespace = format!("{namespace}/state");
            let state = Box::pin(build_boxed_join_aggregate_join_input_state(
                table,
                join_aggregate_namespace,
                &input.schema,
                *plan,
                sources,
                udfs,
            ))
            .await
            .with_context(|| {
                format!(
                    "build SlateDB-backed {side} nested join-aggregate input for '{}'",
                    input.input_name
                )
            })?;
            let snapshot = state.initial_snapshot();
            Ok(ColumnarJoinSourceState {
                input_name: input.input_name,
                source_name: None,
                schema: input.schema,
                input_zset: None,
                input_index: None,
                snapshot,
                constant: None,
                topn: None,
                join_topn: None,
                join: None,
                multijoin: None,
                union: None,
                grouped_max: None,
                grouped_count: None,
                grouped_stats: None,
                join_aggregate: Some(ColumnarJoinJoinAggregateInputState { state }),
            })
        }
    }
}

async fn build_boxed_grouped_max_join_input_state(
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarGroupedMaxPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<Box<ColumnarGroupedMaxMaterializedViewState>> {
    Ok(Box::new(
        Box::pin(
            build_columnar_grouped_max_materialized_view_state_in_namespace(
                table,
                namespace,
                output_schema,
                plan,
                sources,
                udfs,
            ),
        )
        .await?,
    ))
}

async fn build_boxed_grouped_count_join_input_state(
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarGroupedCountPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<Box<ColumnarGroupedCountMaterializedViewState>> {
    Ok(Box::new(
        Box::pin(
            build_columnar_grouped_count_materialized_view_state_in_namespace(
                table,
                namespace,
                output_schema,
                plan,
                sources,
                udfs,
            ),
        )
        .await?,
    ))
}

async fn build_boxed_grouped_stats_join_input_state(
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarGroupedStatsPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<Box<ColumnarGroupedStatsMaterializedViewState>> {
    Ok(Box::new(
        Box::pin(
            build_columnar_grouped_stats_materialized_view_state_in_namespace(
                table,
                namespace,
                output_schema,
                plan,
                sources,
                udfs,
            ),
        )
        .await?,
    ))
}

async fn build_boxed_join_aggregate_join_input_state(
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarComposedPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<Box<ColumnarComposedMaterializedViewState>> {
    Ok(Box::new(
        Box::pin(
            build_columnar_join_aggregate_materialized_view_state_in_namespace(
                table,
                namespace,
                output_schema,
                plan,
                sources,
                udfs,
            ),
        )
        .await?,
    ))
}

async fn evaluate_constant_join_input(
    logical_plan: LogicalPlan,
    schema: &SchemaRef,
    udfs: &[ScalarUDF],
) -> Result<Vec<RecordBatch>> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    for udf in udfs.iter().cloned() {
        ctx.register_udf(udf);
    }
    let physical_plan = ctx
        .state()
        .create_physical_plan(&logical_plan)
        .await
        .context("create constant join input physical plan")?;
    let mut batches = collect(physical_plan, ctx.task_ctx())
        .await
        .context("execute constant join input")?;
    batches = normalize_batches(batches, schema)?;
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(Arc::clone(schema)));
    }
    Ok(batches)
}

pub(super) async fn run_columnar_join_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(columnar) = mv.columnar_join.as_mut() else {
        return Ok(false);
    };
    let plan_start = Instant::now();
    let tick = Box::pin(run_columnar_join_state_tick(
        columnar,
        insert_batches,
        weighted_delta_batches,
        &mv.output_schema,
        &mv.previous_snapshot,
    ))
    .await?;

    let delta_batches = tick.delta.batches().to_vec();
    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, tick.next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = tick.next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = if columnar.execution_strategy == ColumnarJoinExecutionStrategy::SnapshotDiff {
            "columnar_join_snapshot_diff"
        } else {
            "columnar_join"
        },
        "SlateDB-backed join columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

pub(super) async fn run_columnar_join_state_tick(
    columnar: &mut ColumnarJoinMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    output_schema: &SchemaRef,
    previous_snapshot: &[RecordBatch],
) -> Result<ColumnarJoinTick> {
    run_columnar_join_state_tick_inner(
        columnar,
        insert_batches,
        weighted_delta_batches,
        output_schema,
        previous_snapshot,
        true,
    )
    .await
}

pub(super) async fn run_columnar_join_state_tick_delta_only(
    columnar: &mut ColumnarJoinMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    output_schema: &SchemaRef,
    previous_snapshot: &[RecordBatch],
) -> Result<ColumnarJoinTick> {
    run_columnar_join_state_tick_inner(
        columnar,
        insert_batches,
        weighted_delta_batches,
        output_schema,
        previous_snapshot,
        false,
    )
    .await
}

async fn run_columnar_join_state_tick_inner(
    columnar: &mut ColumnarJoinMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    output_schema: &SchemaRef,
    previous_snapshot: &[RecordBatch],
    maintain_output_snapshot: bool,
) -> Result<ColumnarJoinTick> {
    if columnar.execution_strategy == ColumnarJoinExecutionStrategy::SnapshotDiff {
        return Box::pin(run_columnar_snapshot_diff_join_state_tick(
            columnar,
            insert_batches,
            weighted_delta_batches,
            output_schema,
            previous_snapshot,
        ))
        .await;
    }

    let left_input_delta =
        source_input_delta(&columnar.left, insert_batches, weighted_delta_batches)?;
    let right_input_delta =
        source_input_delta(&columnar.right, insert_batches, weighted_delta_batches)?;
    let left_delta = {
        let left_zset = columnar
            .left
            .input_zset
            .as_mut()
            .context("incremental join left source zset missing")?;
        persisted_source_delta(left_zset, left_input_delta).await?
    };
    let right_delta = {
        let right_zset = columnar
            .right
            .input_zset
            .as_mut()
            .context("incremental join right source zset missing")?;
        persisted_source_delta(right_zset, right_input_delta).await?
    };
    let left_signed = signed_source_delta(&columnar.left.schema, left_delta.batches())?;
    let right_signed = signed_source_delta(&columnar.right.schema, right_delta.batches())?;
    let join_key_indices = columnar
        .join_key_indices
        .as_ref()
        .context("incremental join key indices missing")?;
    let right_state_for_left_delta = lookup_indexed_join_state_for_delta(
        columnar
            .right
            .input_index
            .as_deref()
            .context("incremental join right source index missing")?,
        left_delta.batches(),
        &join_key_indices.left,
        &columnar.right.schema,
        "right",
    )
    .await?;
    let left_state_for_right_delta = lookup_indexed_join_state_for_delta(
        columnar
            .left
            .input_index
            .as_deref()
            .context("incremental join left source index missing")?,
        right_delta.batches(),
        &join_key_indices.right,
        &columnar.left.schema,
        "left",
    )
    .await?;

    let mut output_delta_batches = Vec::new();
    collect_join_outputs(
        columnar,
        &mut output_delta_batches,
        &left_signed.positive,
        &right_state_for_left_delta,
        1,
        JoinEvaluatorKind::LeftDeltaRightState,
    )
    .await?;
    collect_join_outputs(
        columnar,
        &mut output_delta_batches,
        &left_signed.negative,
        &right_state_for_left_delta,
        -1,
        JoinEvaluatorKind::LeftDeltaRightState,
    )
    .await?;
    collect_join_outputs(
        columnar,
        &mut output_delta_batches,
        &left_state_for_right_delta,
        &right_signed.positive,
        1,
        JoinEvaluatorKind::LeftStateRightDelta,
    )
    .await?;
    collect_join_outputs(
        columnar,
        &mut output_delta_batches,
        &left_state_for_right_delta,
        &right_signed.negative,
        -1,
        JoinEvaluatorKind::LeftStateRightDelta,
    )
    .await?;
    collect_delta_delta_outputs(
        columnar,
        &mut output_delta_batches,
        &left_signed,
        &right_signed,
    )
    .await?;

    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build join output zset delta")?;
    tracing::debug!(
        left_delta_rows = left_delta
            .batches()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        right_delta_rows = right_delta
            .batches()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        right_state_rows = right_state_for_left_delta
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        left_state_rows = left_state_for_right_delta
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        output_delta_rows = output_delta
            .batches()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        mode = "columnar_join_incremental",
        "SlateDB-backed join columnar DBSP state tick completed"
    );
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

    let next_snapshot = if maintain_output_snapshot {
        apply_weighted_snapshot_delta(
            output_schema,
            previous_snapshot,
            persisted_output_delta.batches().to_vec(),
        )
        .await
        .context("apply Slate-backed join columnar snapshot delta")?
    } else {
        Vec::new()
    };
    if let Some(index) = columnar.left.input_index.as_deref_mut() {
        index
            .apply_delta(&left_delta)
            .await
            .context("apply left join delta to SlateDB-backed columnar index")?;
    }
    if let Some(index) = columnar.right.input_index.as_deref_mut() {
        index
            .apply_delta(&right_delta)
            .await
            .context("apply right join delta to SlateDB-backed columnar index")?;
    }
    if maintain_output_snapshot {
        columnar.left.snapshot = apply_source_snapshot_delta(
            &columnar.left.schema,
            &columnar.left.snapshot,
            &left_delta,
        )
        .await?;
        columnar.right.snapshot = apply_source_snapshot_delta(
            &columnar.right.schema,
            &columnar.right.snapshot,
            &right_delta,
        )
        .await?;
    }

    Ok(ColumnarJoinTick {
        delta: persisted_output_delta,
        next_snapshot,
        input_changed: !left_delta.batches().is_empty() || !right_delta.batches().is_empty(),
    })
}

async fn run_columnar_snapshot_diff_join_state_tick(
    columnar: &mut ColumnarJoinMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    output_schema: &SchemaRef,
    previous_snapshot: &[RecordBatch],
) -> Result<ColumnarJoinTick> {
    let left_tick = Box::pin(prepare_join_input_tick(
        &mut columnar.left,
        insert_batches,
        weighted_delta_batches,
    ))
    .await?;
    let right_tick = Box::pin(prepare_join_input_tick(
        &mut columnar.right,
        insert_batches,
        weighted_delta_batches,
    ))
    .await?;
    let has_input_change = left_tick.changed || right_tick.changed;
    let (next_left_snapshot, next_right_snapshot) = if has_input_change {
        if columnar.left.shares_source_with(&columnar.right) {
            let next_source_snapshot =
                materialize_join_input_snapshot(&columnar.left, "shared").await?;
            (next_source_snapshot.clone(), next_source_snapshot)
        } else {
            let next_left_snapshot = match left_tick.next_snapshot {
                Some(snapshot) => snapshot,
                None => next_join_source_snapshot(&columnar.left, &left_tick.delta, "left").await?,
            };
            let next_right_snapshot = match right_tick.next_snapshot {
                Some(snapshot) => snapshot,
                None => {
                    next_join_source_snapshot(&columnar.right, &right_tick.delta, "right").await?
                }
            };
            (next_left_snapshot, next_right_snapshot)
        }
    } else {
        (
            columnar.left.snapshot.clone(),
            columnar.right.snapshot.clone(),
        )
    };

    let output_delta_batches = if has_input_change {
        let next_output = columnar
            .left_delta_right_delta
            .evaluate(
                &columnar.left.input_name,
                &next_left_snapshot,
                &columnar.right.input_name,
                &next_right_snapshot,
            )
            .await
            .context("evaluate next snapshot-diff join output")?;
        diff_snapshot_batches(Arc::clone(output_schema), previous_snapshot, &next_output)
            .await
            .context("diff snapshot-diff join output")?
            .batches
    } else {
        Vec::new()
    };

    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build snapshot-diff join output zset delta")?;
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
            .context("apply Slate-backed snapshot-diff join columnar snapshot delta")?;

    columnar.left.snapshot = next_left_snapshot;
    columnar.right.snapshot = next_right_snapshot;
    mark_join_constant_initialized(&mut columnar.left).await?;
    mark_join_constant_initialized(&mut columnar.right).await?;
    Ok(ColumnarJoinTick {
        delta: persisted_output_delta,
        next_snapshot,
        input_changed: has_input_change,
    })
}

enum JoinEvaluatorKind {
    LeftDeltaRightState,
    LeftStateRightDelta,
    LeftDeltaRightDelta,
}

async fn collect_delta_delta_outputs(
    columnar: &ColumnarJoinMaterializedViewState,
    output: &mut Vec<RecordBatch>,
    left: &JoinSignedDelta,
    right: &JoinSignedDelta,
) -> Result<()> {
    collect_join_outputs(
        columnar,
        output,
        &left.positive,
        &right.positive,
        1,
        JoinEvaluatorKind::LeftDeltaRightDelta,
    )
    .await?;
    collect_join_outputs(
        columnar,
        output,
        &left.positive,
        &right.negative,
        -1,
        JoinEvaluatorKind::LeftDeltaRightDelta,
    )
    .await?;
    collect_join_outputs(
        columnar,
        output,
        &left.negative,
        &right.positive,
        -1,
        JoinEvaluatorKind::LeftDeltaRightDelta,
    )
    .await?;
    collect_join_outputs(
        columnar,
        output,
        &left.negative,
        &right.negative,
        1,
        JoinEvaluatorKind::LeftDeltaRightDelta,
    )
    .await
}

async fn collect_join_outputs(
    columnar: &ColumnarJoinMaterializedViewState,
    output: &mut Vec<RecordBatch>,
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    weight: i64,
    kind: JoinEvaluatorKind,
) -> Result<()> {
    if left_batches.iter().all(|batch| batch.num_rows() == 0)
        || right_batches.iter().all(|batch| batch.num_rows() == 0)
    {
        return Ok(());
    }
    let evaluator = match kind {
        JoinEvaluatorKind::LeftDeltaRightState => columnar
            .left_delta_right_state
            .as_ref()
            .context("left-delta/right-state join evaluator was not built")?,
        JoinEvaluatorKind::LeftStateRightDelta => columnar
            .left_state_right_delta
            .as_ref()
            .context("left-state/right-delta join evaluator was not built")?,
        JoinEvaluatorKind::LeftDeltaRightDelta => &columnar.left_delta_right_delta,
    };
    let joined = evaluator
        .evaluate(
            &columnar.left.input_name,
            left_batches,
            &columnar.right.input_name,
            right_batches,
        )
        .await?;
    let weighted_schema = weighted_snapshot_schema(&evaluator.output_schema)?;
    output.extend(add_weight_column_to_batches(
        &joined,
        &weighted_schema,
        weight,
    )?);
    Ok(())
}

async fn prepare_join_input_tick(
    source: &mut ColumnarJoinSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<JoinInputTick> {
    if source.join.is_some() {
        return Box::pin(prepare_nested_join_input_tick(
            source,
            insert_batches,
            weighted_delta_batches,
        ))
        .await;
    }
    if source.multijoin.is_some() {
        return Box::pin(prepare_nested_multijoin_input_tick(
            source,
            insert_batches,
            weighted_delta_batches,
        ))
        .await;
    }
    if source.union.is_some() {
        return Box::pin(prepare_nested_union_input_tick(
            source,
            insert_batches,
            weighted_delta_batches,
        ))
        .await;
    }
    if source.grouped_max.is_some() {
        return Box::pin(prepare_nested_grouped_max_input_tick(
            source,
            insert_batches,
            weighted_delta_batches,
        ))
        .await;
    }
    if source.grouped_count.is_some() {
        return Box::pin(prepare_nested_grouped_count_input_tick(
            source,
            insert_batches,
            weighted_delta_batches,
        ))
        .await;
    }
    if source.grouped_stats.is_some() {
        return Box::pin(prepare_nested_grouped_stats_input_tick(
            source,
            insert_batches,
            weighted_delta_batches,
        ))
        .await;
    }
    if source.join_aggregate.is_some() {
        return Box::pin(prepare_nested_join_aggregate_input_tick(
            source,
            insert_batches,
            weighted_delta_batches,
        ))
        .await;
    }
    if source.join_topn.is_some() {
        return Box::pin(prepare_join_topn_join_input_tick(
            source,
            insert_batches,
            weighted_delta_batches,
        ))
        .await;
    }
    if source.topn.is_some() {
        return Box::pin(prepare_topn_join_input_tick(
            source,
            insert_batches,
            weighted_delta_batches,
        ))
        .await;
    }
    if source.constant.is_some() {
        return Box::pin(prepare_constant_join_input_tick(source)).await;
    }

    let input_delta = source_input_delta(source, insert_batches, weighted_delta_batches)?;
    let input_zset = source
        .input_zset
        .as_mut()
        .context("join source input zset missing")?;
    let delta = persisted_source_delta(input_zset, input_delta).await?;
    let changed = !delta.batches().is_empty();
    Ok(JoinInputTick {
        delta,
        changed,
        next_snapshot: None,
    })
}

async fn prepare_nested_union_input_tick(
    source: &mut ColumnarJoinSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<JoinInputTick> {
    let Some(union) = source.union.as_mut() else {
        return Ok(JoinInputTick {
            delta: ColumnarZSet::empty(Arc::clone(&source.schema))?,
            changed: false,
            next_snapshot: None,
        });
    };
    let tick = Box::pin(run_columnar_union_state_tick(
        &mut union.state,
        insert_batches,
        weighted_delta_batches,
        &source.schema,
        &source.snapshot,
    ))
    .await
    .with_context(|| format!("evaluate nested union input '{}'", source.input_name))?;
    let changed = !tick.delta.batches().is_empty();
    if !tick.input_changed {
        return Ok(JoinInputTick {
            delta: tick.delta,
            changed: false,
            next_snapshot: None,
        });
    }
    if !changed {
        source.snapshot = tick.next_snapshot;
        return Ok(JoinInputTick {
            delta: tick.delta,
            changed: false,
            next_snapshot: None,
        });
    }
    Ok(JoinInputTick {
        delta: tick.delta,
        changed: true,
        next_snapshot: Some(tick.next_snapshot),
    })
}

async fn prepare_nested_grouped_max_input_tick(
    source: &mut ColumnarJoinSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<JoinInputTick> {
    let Some(grouped_max) = source.grouped_max.as_mut() else {
        return Ok(JoinInputTick {
            delta: ColumnarZSet::empty(Arc::clone(&source.schema))?,
            changed: false,
            next_snapshot: None,
        });
    };
    let tick = Box::pin(run_columnar_grouped_max_state_tick(
        grouped_max.state.as_mut(),
        insert_batches,
        weighted_delta_batches,
        &source.schema,
        &source.snapshot,
    ))
    .await
    .with_context(|| format!("evaluate nested grouped-max input '{}'", source.input_name))?;
    let changed = !tick.delta.batches().is_empty();
    if !tick.input_changed {
        return Ok(JoinInputTick {
            delta: tick.delta,
            changed: false,
            next_snapshot: None,
        });
    }
    if !changed {
        source.snapshot = tick.next_snapshot;
        return Ok(JoinInputTick {
            delta: tick.delta,
            changed: false,
            next_snapshot: None,
        });
    }
    Ok(JoinInputTick {
        delta: tick.delta,
        changed: true,
        next_snapshot: Some(tick.next_snapshot),
    })
}

async fn prepare_nested_grouped_count_input_tick(
    source: &mut ColumnarJoinSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<JoinInputTick> {
    let Some(grouped_count) = source.grouped_count.as_mut() else {
        return Ok(JoinInputTick {
            delta: ColumnarZSet::empty(Arc::clone(&source.schema))?,
            changed: false,
            next_snapshot: None,
        });
    };
    let tick = Box::pin(run_columnar_grouped_count_state_tick(
        grouped_count.state.as_mut(),
        insert_batches,
        weighted_delta_batches,
        &source.schema,
        &source.snapshot,
    ))
    .await
    .with_context(|| {
        format!(
            "evaluate nested grouped-count input '{}'",
            source.input_name
        )
    })?;
    let changed = !tick.delta.batches().is_empty();
    if !tick.input_changed {
        return Ok(JoinInputTick {
            delta: tick.delta,
            changed: false,
            next_snapshot: None,
        });
    }
    if !changed {
        source.snapshot = tick.next_snapshot;
        return Ok(JoinInputTick {
            delta: tick.delta,
            changed: false,
            next_snapshot: None,
        });
    }
    Ok(JoinInputTick {
        delta: tick.delta,
        changed: true,
        next_snapshot: Some(tick.next_snapshot),
    })
}

async fn prepare_nested_grouped_stats_input_tick(
    source: &mut ColumnarJoinSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<JoinInputTick> {
    let Some(grouped_stats) = source.grouped_stats.as_mut() else {
        return Ok(JoinInputTick {
            delta: ColumnarZSet::empty(Arc::clone(&source.schema))?,
            changed: false,
            next_snapshot: None,
        });
    };
    let tick = Box::pin(run_columnar_grouped_stats_state_tick(
        grouped_stats.state.as_mut(),
        insert_batches,
        weighted_delta_batches,
        &source.schema,
        &source.snapshot,
    ))
    .await
    .with_context(|| {
        format!(
            "evaluate nested grouped-stats input '{}'",
            source.input_name
        )
    })?;
    let changed = !tick.delta.batches().is_empty();
    if !tick.input_changed {
        return Ok(JoinInputTick {
            delta: tick.delta,
            changed: false,
            next_snapshot: None,
        });
    }
    if !changed {
        source.snapshot = tick.next_snapshot;
        return Ok(JoinInputTick {
            delta: tick.delta,
            changed: false,
            next_snapshot: None,
        });
    }
    Ok(JoinInputTick {
        delta: tick.delta,
        changed: true,
        next_snapshot: Some(tick.next_snapshot),
    })
}

async fn prepare_nested_join_aggregate_input_tick(
    source: &mut ColumnarJoinSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<JoinInputTick> {
    let Some(join_aggregate) = source.join_aggregate.as_mut() else {
        return Ok(JoinInputTick {
            delta: ColumnarZSet::empty(Arc::clone(&source.schema))?,
            changed: false,
            next_snapshot: None,
        });
    };
    let tick = Box::pin(run_columnar_composed_state_tick(
        join_aggregate.state.as_mut(),
        insert_batches,
        weighted_delta_batches,
        &source.schema,
        &source.snapshot,
    ))
    .await
    .with_context(|| {
        format!(
            "evaluate nested join-aggregate input '{}'",
            source.input_name
        )
    })?;
    let changed = !tick.delta.batches().is_empty();
    if !tick.input_changed {
        return Ok(JoinInputTick {
            delta: tick.delta,
            changed: false,
            next_snapshot: None,
        });
    }
    if !changed {
        source.snapshot = tick.next_snapshot;
        return Ok(JoinInputTick {
            delta: tick.delta,
            changed: false,
            next_snapshot: None,
        });
    }
    Ok(JoinInputTick {
        delta: tick.delta,
        changed: true,
        next_snapshot: Some(tick.next_snapshot),
    })
}

async fn prepare_nested_multijoin_input_tick(
    source: &mut ColumnarJoinSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<JoinInputTick> {
    let Some(multijoin) = source.multijoin.as_mut() else {
        return Ok(JoinInputTick {
            delta: ColumnarZSet::empty(Arc::clone(&source.schema))?,
            changed: false,
            next_snapshot: None,
        });
    };
    let tick = Box::pin(run_columnar_multijoin_state_tick(
        &mut multijoin.state,
        insert_batches,
        weighted_delta_batches,
        &source.schema,
        &source.snapshot,
    ))
    .await
    .with_context(|| format!("evaluate nested multijoin input '{}'", source.input_name))?;
    let changed = !tick.delta.batches().is_empty();
    if !tick.input_changed {
        return Ok(JoinInputTick {
            delta: tick.delta,
            changed: false,
            next_snapshot: None,
        });
    }
    if !changed {
        source.snapshot = tick.next_snapshot;
        return Ok(JoinInputTick {
            delta: tick.delta,
            changed: false,
            next_snapshot: None,
        });
    }
    Ok(JoinInputTick {
        delta: tick.delta,
        changed: true,
        next_snapshot: Some(tick.next_snapshot),
    })
}

async fn prepare_nested_join_input_tick(
    source: &mut ColumnarJoinSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<JoinInputTick> {
    let Some(join) = source.join.as_mut() else {
        return Ok(JoinInputTick {
            delta: ColumnarZSet::empty(Arc::clone(&source.schema))?,
            changed: false,
            next_snapshot: None,
        });
    };
    let tick = Box::pin(run_columnar_join_state_tick(
        &mut join.state,
        insert_batches,
        weighted_delta_batches,
        &source.schema,
        &source.snapshot,
    ))
    .await
    .with_context(|| format!("evaluate nested join input '{}'", source.input_name))?;
    let changed = !tick.delta.batches().is_empty();
    if !tick.input_changed {
        return Ok(JoinInputTick {
            delta: tick.delta,
            changed: false,
            next_snapshot: None,
        });
    }
    if !changed {
        source.snapshot = tick.next_snapshot;
        return Ok(JoinInputTick {
            delta: tick.delta,
            changed: false,
            next_snapshot: None,
        });
    }
    Ok(JoinInputTick {
        delta: tick.delta,
        changed: true,
        next_snapshot: Some(tick.next_snapshot),
    })
}

async fn prepare_join_topn_join_input_tick(
    source: &mut ColumnarJoinSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<JoinInputTick> {
    let Some(join_topn) = source.join_topn.as_mut() else {
        return Ok(JoinInputTick {
            delta: ColumnarZSet::empty(Arc::clone(&source.schema))?,
            changed: false,
            next_snapshot: None,
        });
    };
    let tick = Box::pin(run_columnar_join_topn_state_tick(
        &mut join_topn.state,
        insert_batches,
        weighted_delta_batches,
        &source.schema,
        &source.snapshot,
    ))
    .await
    .with_context(|| format!("evaluate join join-topn input '{}'", source.input_name))?;
    let changed = !tick.delta.batches().is_empty();
    if !tick.input_changed {
        return Ok(JoinInputTick {
            delta: tick.delta,
            changed: false,
            next_snapshot: None,
        });
    }
    if !changed {
        source.snapshot = tick.next_snapshot;
        return Ok(JoinInputTick {
            delta: tick.delta,
            changed: false,
            next_snapshot: None,
        });
    }
    Ok(JoinInputTick {
        delta: tick.delta,
        changed: true,
        next_snapshot: Some(tick.next_snapshot),
    })
}

async fn prepare_topn_join_input_tick(
    source: &mut ColumnarJoinSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<JoinInputTick> {
    let Some(topn) = source.topn.as_mut() else {
        return Ok(JoinInputTick {
            delta: ColumnarZSet::empty(Arc::clone(&source.schema))?,
            changed: false,
            next_snapshot: None,
        });
    };
    let raw_input_delta = topn_source_input_delta(topn, insert_batches, weighted_delta_batches)?;
    let raw_delta = persisted_source_delta(&mut topn.source_input_zset, raw_input_delta).await?;
    let has_raw_change = !raw_delta.batches().is_empty();
    if !has_raw_change {
        return Ok(JoinInputTick {
            delta: ColumnarZSet::empty(Arc::clone(&source.schema))?,
            changed: false,
            next_snapshot: None,
        });
    }

    let next_source_snapshot =
        apply_source_snapshot_delta(&topn.source_schema, &topn.source_snapshot, &raw_delta).await?;
    let next_topn_snapshot = topn
        .evaluator
        .evaluate(&next_source_snapshot)
        .await
        .with_context(|| format!("evaluate join topn input '{}'", source.input_name))?;
    let topn_delta_batches = diff_snapshot_batches(
        Arc::clone(&source.schema),
        &source.snapshot,
        &next_topn_snapshot,
    )
    .await
    .with_context(|| format!("diff join topn input '{}'", source.input_name))?
    .batches;
    let input_zset = source
        .input_zset
        .as_mut()
        .context("join topn input zset missing")?;
    let topn_delta = ColumnarZSet::try_new_weighted(input_zset.value_schema(), topn_delta_batches)
        .with_context(|| format!("build join topn input delta for '{}'", source.input_name))?;
    let delta = persisted_source_delta(input_zset, topn_delta).await?;
    let changed = !delta.batches().is_empty();

    topn.source_snapshot = next_source_snapshot;
    if !changed {
        source.snapshot = next_topn_snapshot;
        return Ok(JoinInputTick {
            delta,
            changed: false,
            next_snapshot: None,
        });
    }

    Ok(JoinInputTick {
        delta,
        changed: true,
        next_snapshot: Some(next_topn_snapshot),
    })
}

async fn prepare_constant_join_input_tick(
    source: &mut ColumnarJoinSourceState,
) -> Result<JoinInputTick> {
    let Some((initialized, pending_snapshot)) = source
        .constant
        .as_ref()
        .map(|constant| (constant.initialized, constant.pending_snapshot.clone()))
    else {
        return Ok(JoinInputTick {
            delta: ColumnarZSet::empty(Arc::clone(&source.schema))?,
            changed: false,
            next_snapshot: None,
        });
    };
    if initialized {
        return Ok(JoinInputTick {
            delta: ColumnarZSet::empty(Arc::clone(&source.schema))?,
            changed: false,
            next_snapshot: None,
        });
    }

    let input_zset = source
        .input_zset
        .as_mut()
        .context("join constant input zset missing")?;
    let delta = if input_zset.current_handle().is_some() {
        ColumnarZSet::empty(Arc::clone(&source.schema))?
    } else {
        let input_delta =
            ColumnarZSet::from_value_batches(Arc::clone(&source.schema), pending_snapshot, 1)
                .with_context(|| {
                    format!(
                        "build constant join input delta for '{}'",
                        source.input_name
                    )
                })?;
        persisted_source_delta(input_zset, input_delta).await?
    };
    let next_snapshot = materialize_join_input_snapshot(source, &source.input_name).await?;
    Ok(JoinInputTick {
        delta,
        changed: true,
        next_snapshot: Some(next_snapshot),
    })
}

async fn mark_join_constant_initialized(source: &mut ColumnarJoinSourceState) -> Result<()> {
    let Some(constant) = source.constant.as_mut() else {
        return Ok(());
    };
    if constant.initialized {
        return Ok(());
    }
    constant
        .state_table
        .put(&constant.initialized_key, b"1")
        .await
        .with_context(|| {
            format!(
                "persist SlateDB-backed join constant initialized marker for '{}'",
                source.input_name
            )
        })?;
    constant.initialized = true;
    constant.pending_snapshot.clear();
    Ok(())
}

fn source_input_delta(
    source: &ColumnarJoinSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    let Some(source_name) = source.source_name.as_deref() else {
        return ColumnarZSet::empty(Arc::clone(&source.schema));
    };
    if let Some(weighted_batches) = weighted_delta_batches.get(source_name) {
        ColumnarZSet::try_new_weighted(Arc::clone(&source.schema), weighted_batches.clone())
            .with_context(|| {
                format!(
                    "build weighted join input delta for '{}'",
                    source.input_name
                )
            })
    } else if let Some(source_batches) = insert_batches.get(source_name) {
        ColumnarZSet::from_value_batches(Arc::clone(&source.schema), source_batches.clone(), 1)
            .with_context(|| format!("build insert join input delta for '{}'", source.input_name))
    } else {
        ColumnarZSet::empty(Arc::clone(&source.schema))
    }
}

fn topn_source_input_delta(
    topn: &ColumnarJoinTopNInputState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    if let Some(weighted_batches) = weighted_delta_batches.get(topn.source_name.as_str()) {
        ColumnarZSet::try_new_weighted(Arc::clone(&topn.source_schema), weighted_batches.clone())
            .with_context(|| {
                format!(
                    "build weighted join topn raw input delta for '{}'",
                    topn.source_name
                )
            })
    } else if let Some(source_batches) = insert_batches.get(topn.source_name.as_str()) {
        ColumnarZSet::from_value_batches(Arc::clone(&topn.source_schema), source_batches.clone(), 1)
            .with_context(|| {
                format!(
                    "build insert join topn raw input delta for '{}'",
                    topn.source_name
                )
            })
    } else {
        ColumnarZSet::empty(Arc::clone(&topn.source_schema))
    }
}

async fn next_join_source_snapshot(
    source: &ColumnarJoinSourceState,
    delta: &ColumnarZSet,
    side: &str,
) -> Result<Vec<RecordBatch>> {
    if delta.batches().is_empty() {
        return Ok(source.snapshot.clone());
    }
    materialize_join_input_snapshot(source, side).await
}

async fn materialize_join_input_snapshot(
    source: &ColumnarJoinSourceState,
    side: &str,
) -> Result<Vec<RecordBatch>> {
    let Some(input_zset) = source.input_zset.as_ref() else {
        return Ok(source.snapshot.clone());
    };
    snapshot_batches_from_zset(
        &input_zset
            .materialize_columnar()
            .await
            .with_context(|| format!("materialize {side} join input zset"))?,
    )
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

fn signed_source_delta(
    schema: &SchemaRef,
    input_batches: &[RecordBatch],
) -> Result<JoinSignedDelta> {
    let mut positive = Vec::new();
    let mut negative = Vec::new();
    for batch in input_batches {
        let unit_delta = unit_source_delta_batches(schema, batch)?
            .context("join received non-unit source delta")?;
        positive.extend(unit_delta.positive);
        negative.extend(unit_delta.negative);
    }
    Ok(JoinSignedDelta { positive, negative })
}

async fn lookup_indexed_join_state_for_delta(
    state_index: &SlateBackedColumnarIndexedZSet,
    delta_batches: &[RecordBatch],
    delta_key_indices: &[usize],
    state_schema: &SchemaRef,
    side: &str,
) -> Result<Vec<RecordBatch>> {
    let key_batches =
        lookup_key_batches_from_delta(delta_batches, delta_key_indices, &state_index.key_schema())
            .with_context(|| format!("build {side} join state lookup keys"))?;
    let weighted_lookup = state_index
        .lookup_key_batches(&key_batches)
        .await
        .with_context(|| format!("lookup {side} join state by indexed keys"))?;
    if weighted_lookup.is_empty() {
        return Ok(Vec::new());
    }
    apply_weighted_snapshot_delta(state_schema, &[], weighted_lookup.batches().to_vec())
        .await
        .with_context(|| format!("materialize {side} indexed join state lookup"))
}

fn lookup_key_batches_from_delta(
    delta_batches: &[RecordBatch],
    delta_key_indices: &[usize],
    lookup_key_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    if delta_key_indices.len() != lookup_key_schema.fields().len() {
        bail!("join lookup key count does not match indexed key schema");
    }
    let mut key_batches = Vec::new();
    for batch in delta_batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let columns = delta_key_indices
            .iter()
            .map(|idx| {
                if *idx >= batch.num_columns() {
                    bail!("join delta key column {idx} out of bounds");
                }
                Ok(Arc::clone(batch.column(*idx)))
            })
            .collect::<Result<Vec<_>>>()?;
        key_batches.push(
            RecordBatch::try_new(Arc::clone(lookup_key_schema), columns)
                .context("build join lookup key batch")?,
        );
    }
    Ok(key_batches)
}

async fn apply_source_snapshot_delta(
    schema: &SchemaRef,
    previous: &[RecordBatch],
    delta: &ColumnarZSet,
) -> Result<Vec<RecordBatch>> {
    if delta.batches().is_empty() {
        return Ok(previous.to_vec());
    }
    apply_weighted_snapshot_delta(schema, previous, delta.batches().to_vec()).await
}

impl JoinDeltaEvaluator {
    async fn build(
        logical_plan: LogicalPlan,
        sources: &HashMap<String, VectorizedSourceState>,
        udfs: &[ScalarUDF],
        output_schema: &SchemaRef,
        left: &JoinEvaluatorInputPlan,
        right: &JoinEvaluatorInputPlan,
        rebuild_each_evaluate: bool,
    ) -> Result<Self> {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        for udf in udfs.iter().cloned() {
            ctx.register_udf(udf);
        }
        let left_input = JoinEvaluatorInput::new(left, sources, rebuild_each_evaluate)?;
        let right_input = JoinEvaluatorInput::new(right, sources, rebuild_each_evaluate)?;
        let logical_plan =
            rebind_join_logical_plan(logical_plan, left, &left_input, right, &right_input)?;
        let plan = if rebuild_each_evaluate {
            None
        } else {
            Some(ctx.state().create_physical_plan(&logical_plan).await?)
        };
        Ok(Self {
            ctx,
            logical_plan,
            plan,
            rebuild_each_evaluate,
            left_input,
            right_input,
            output_schema: Arc::clone(output_schema),
        })
    }

    async fn evaluate(
        &self,
        left_source: &str,
        left_batches: &[RecordBatch],
        right_source: &str,
        right_batches: &[RecordBatch],
    ) -> Result<Vec<RecordBatch>> {
        self.left_input
            .set_batches(left_batches)
            .with_context(|| format!("set left join evaluator input for '{left_source}'"))?;
        self.right_input
            .set_batches(right_batches)
            .with_context(|| format!("set right join evaluator input for '{right_source}'"))?;
        let plan = if self.rebuild_each_evaluate {
            self.ctx
                .state()
                .create_physical_plan(&self.logical_plan)
                .await
                .context("rebuild vectorized join delta physical plan")?
        } else {
            Arc::clone(
                self.plan
                    .as_ref()
                    .context("cached vectorized join delta physical plan missing")?,
            )
        };
        let collected = collect(plan, self.ctx.task_ctx()).await;
        self.clear_inputs()?;
        normalize_batches(
            collected.context("execute vectorized join delta evaluator")?,
            &self.output_schema,
        )
    }

    fn clear_inputs(&self) -> Result<()> {
        self.left_input.clear()?;
        self.right_input.clear()?;
        Ok(())
    }
}

fn dynamic_join_provider(
    schema: SchemaRef,
    single_partition_scan: bool,
) -> Arc<DynamicStateTableProvider> {
    let provider = if single_partition_scan {
        DynamicStateTableProvider::new_with_scan_partitions(schema, 1)
    } else {
        DynamicStateTableProvider::new(schema)
    };
    Arc::new(provider)
}

impl JoinEvaluatorInput {
    fn new(
        input: &JoinEvaluatorInputPlan,
        sources: &HashMap<String, VectorizedSourceState>,
        single_partition_scan: bool,
    ) -> Result<Self> {
        let provider = dynamic_join_provider(Arc::clone(&input.schema), single_partition_scan);
        let (alias_schema, alias_provider) = match &input.source_name {
            Some(source_name) => {
                let source = sources
                    .get(source_name)
                    .ok_or_else(|| anyhow::anyhow!("unknown join source '{source_name}'"))?;
                if let (Some(_alias), Some(alias_schema)) = (
                    source_name.strip_prefix("nexmark_"),
                    source.alias_schema.as_ref(),
                ) {
                    (
                        Some(Arc::clone(alias_schema)),
                        Some(dynamic_join_provider(
                            Arc::clone(alias_schema),
                            single_partition_scan,
                        )),
                    )
                } else {
                    (None, None)
                }
            }
            None => (None, None),
        };
        Ok(Self {
            provider,
            alias_schema,
            alias_provider,
        })
    }

    fn provider_for_table(
        &self,
        input: &JoinEvaluatorInputPlan,
        table_name: &str,
    ) -> Option<Arc<dyn TableProvider>> {
        let source_name = input.source_name.as_ref()?;
        if table_name == source_name {
            return Some(Arc::clone(&self.provider) as Arc<dyn TableProvider>);
        }
        if source_name.strip_prefix("nexmark_") == Some(table_name)
            && let Some(alias_provider) = self.alias_provider.as_ref()
        {
            return Some(Arc::clone(alias_provider) as Arc<dyn TableProvider>);
        }
        None
    }

    fn scan_plan(&self, input_name: &str) -> Result<LogicalPlan> {
        LogicalPlanBuilder::scan(
            input_name,
            provider_as_source(Arc::clone(&self.provider) as Arc<dyn TableProvider>),
            None,
        )?
        .build()
        .map_err(Into::into)
    }

    fn set_batches(&self, batches: &[RecordBatch]) -> Result<()> {
        self.provider.set_batches(batches.to_vec())?;
        if let (Some(alias_schema), Some(alias_provider)) =
            (self.alias_schema.as_ref(), self.alias_provider.as_ref())
        {
            alias_provider.set_batches(rename_batches(batches, alias_schema)?)?;
        }
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        self.provider.set_batches(Vec::new())?;
        if let Some(alias_provider) = self.alias_provider.as_ref() {
            alias_provider.set_batches(Vec::new())?;
        }
        Ok(())
    }
}

fn rebind_join_logical_plan(
    logical_plan: LogicalPlan,
    left: &JoinEvaluatorInputPlan,
    left_input: &JoinEvaluatorInput,
    right: &JoinEvaluatorInputPlan,
    right_input: &JoinEvaluatorInput,
) -> Result<LogicalPlan> {
    match logical_plan {
        LogicalPlan::Projection(mut projection) => {
            projection.input = Arc::new(rebind_join_logical_plan(
                projection.input.as_ref().clone(),
                left,
                left_input,
                right,
                right_input,
            )?);
            Ok(LogicalPlan::Projection(projection))
        }
        LogicalPlan::Filter(mut filter) => {
            filter.input = Arc::new(rebind_join_logical_plan(
                filter.input.as_ref().clone(),
                left,
                left_input,
                right,
                right_input,
            )?);
            Ok(LogicalPlan::Filter(filter))
        }
        LogicalPlan::SubqueryAlias(mut alias) => {
            alias.input = Arc::new(rebind_join_logical_plan(
                alias.input.as_ref().clone(),
                left,
                left_input,
                right,
                right_input,
            )?);
            Ok(LogicalPlan::SubqueryAlias(alias))
        }
        LogicalPlan::Sort(mut sort) if sort.fetch.is_none() => {
            sort.input = Arc::new(rebind_join_logical_plan(
                sort.input.as_ref().clone(),
                left,
                left_input,
                right,
                right_input,
            )?);
            Ok(LogicalPlan::Sort(sort))
        }
        LogicalPlan::Join(mut join) => {
            join.left = Arc::new(
                rebind_join_side_logical_plan(join.left.as_ref().clone(), left, left_input)
                    .context("rebind left join side")?,
            );
            join.right = Arc::new(
                rebind_join_side_logical_plan(join.right.as_ref().clone(), right, right_input)
                    .context("rebind right join side")?,
            );
            Ok(LogicalPlan::Join(join))
        }
        other => Ok(other),
    }
}

fn rebind_join_side_logical_plan(
    logical_plan: LogicalPlan,
    input_plan: &JoinEvaluatorInputPlan,
    input: &JoinEvaluatorInput,
) -> Result<LogicalPlan> {
    if input_plan.source_name.is_none() {
        return input.scan_plan(&input_plan.input_name);
    }
    let transformed = logical_plan.transform_up(|plan| match plan {
        LogicalPlan::TableScan(mut scan) => {
            let table_name = scan.table_name.table();
            let Some(provider) = input.provider_for_table(input_plan, table_name) else {
                return Err(datafusion::error::DataFusionError::Plan(format!(
                    "join side expected source '{}' but found table scan '{table_name}'",
                    input_plan.input_name
                )));
            };
            scan.source = provider_as_source(provider);
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        }
        other => Ok(Transformed::no(other)),
    })?;
    Ok(transformed.data)
}

impl ColumnarJoinInputPlan {
    fn source_name(&self) -> Option<String> {
        match &self.kind {
            ColumnarJoinInputPlanKind::Source { source_name } => Some(source_name.clone()),
            ColumnarJoinInputPlanKind::Constant { .. } => None,
            ColumnarJoinInputPlanKind::TopN { source_name, .. } => Some(source_name.clone()),
            ColumnarJoinInputPlanKind::JoinTopN { .. } => None,
            ColumnarJoinInputPlanKind::Join { .. } => None,
            ColumnarJoinInputPlanKind::MultiJoin { .. } => None,
            ColumnarJoinInputPlanKind::Union { .. } => None,
            ColumnarJoinInputPlanKind::GroupedMax { .. } => None,
            ColumnarJoinInputPlanKind::GroupedCount { .. } => None,
            ColumnarJoinInputPlanKind::GroupedStats { .. } => None,
            ColumnarJoinInputPlanKind::JoinAggregate { .. } => None,
        }
    }

    fn source_names(&self) -> BTreeSet<String> {
        match &self.kind {
            ColumnarJoinInputPlanKind::Source { source_name }
            | ColumnarJoinInputPlanKind::TopN { source_name, .. } => {
                [source_name.clone()].into_iter().collect()
            }
            ColumnarJoinInputPlanKind::JoinTopN { plan } => {
                plan.source_names().into_iter().collect()
            }
            ColumnarJoinInputPlanKind::Join { plan } => plan
                .left
                .source_names()
                .into_iter()
                .chain(plan.right.source_names())
                .collect(),
            ColumnarJoinInputPlanKind::MultiJoin { plan } => plan.source_names(),
            ColumnarJoinInputPlanKind::Union { plan } => plan.source_names(),
            ColumnarJoinInputPlanKind::GroupedMax { plan } => plan.source_names(),
            ColumnarJoinInputPlanKind::GroupedCount { plan } => plan.source_names(),
            ColumnarJoinInputPlanKind::GroupedStats { plan } => plan.source_names(),
            ColumnarJoinInputPlanKind::JoinAggregate { plan } => plan.source_names(),
            ColumnarJoinInputPlanKind::Constant { .. } => BTreeSet::new(),
        }
    }

    fn is_source(&self) -> bool {
        matches!(self.kind, ColumnarJoinInputPlanKind::Source { .. })
    }
}

impl ColumnarJoinSourceState {
    fn shares_source_with(&self, other: &Self) -> bool {
        self.source_name.is_some() && self.source_name == other.source_name
    }
}

fn join_input_plan_for_side(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    side: &str,
) -> Result<Option<ColumnarJoinInputPlan>> {
    if !plan_contains_table_scan(plan) {
        let input_name =
            constant_relation_name(plan).unwrap_or_else(|| format!("__floe_join_{side}_constant"));
        return Ok(Some(ColumnarJoinInputPlan {
            input_name,
            schema: df_schema_to_arrow(plan.schema()),
            kind: ColumnarJoinInputPlanKind::Constant {
                logical_plan: plan.clone(),
            },
        }));
    }

    if let Some(join_topn) = columnar_join_topn_plan_for_plan(plan, sources)? {
        let input_name =
            constant_relation_name(plan).unwrap_or_else(|| format!("__floe_join_{side}_join_topn"));
        return Ok(Some(ColumnarJoinInputPlan {
            input_name,
            schema: df_schema_to_arrow(plan.schema()),
            kind: ColumnarJoinInputPlanKind::JoinTopN { plan: join_topn },
        }));
    }

    if let Some(topn) = columnar_topn_plan_for_plan(plan, sources)?
        && let Some(source_name) = topn.source_name()
    {
        let input_name =
            constant_relation_name(plan).unwrap_or_else(|| format!("__floe_join_{side}_topn"));
        return Ok(Some(ColumnarJoinInputPlan {
            input_name,
            schema: df_schema_to_arrow(plan.schema()),
            kind: ColumnarJoinInputPlanKind::TopN {
                source_name,
                logical_plan: topn.logical_plan,
            },
        }));
    }

    let schema = df_schema_to_arrow(plan.schema());
    if let Some(grouped_count) = columnar_grouped_count_plan_for_plan(plan, sources, &schema)? {
        let input_name = constant_relation_name(plan)
            .or_else(|| derived_relation_name(plan))
            .unwrap_or_else(|| format!("__floe_join_{side}_grouped_count"));
        return Ok(Some(ColumnarJoinInputPlan {
            input_name,
            schema,
            kind: ColumnarJoinInputPlanKind::GroupedCount {
                plan: grouped_count,
            },
        }));
    }

    if let Some(input_name) = derived_relation_name(plan) {
        let schema = df_schema_to_arrow(plan.schema());
        if let Some(grouped_max) = columnar_grouped_max_plan_for_plan(plan, sources, &schema)? {
            return Ok(Some(ColumnarJoinInputPlan {
                input_name,
                schema,
                kind: ColumnarJoinInputPlanKind::GroupedMax { plan: grouped_max },
            }));
        }
        if let Some(grouped_stats) = columnar_grouped_stats_plan_for_plan(plan, sources, &schema)? {
            return Ok(Some(ColumnarJoinInputPlan {
                input_name,
                schema,
                kind: ColumnarJoinInputPlanKind::GroupedStats {
                    plan: Box::new(grouped_stats),
                },
            }));
        }
        if let Some(join_aggregate) = columnar_join_aggregate_plan_for_plan(plan, sources)? {
            return Ok(Some(ColumnarJoinInputPlan {
                input_name,
                schema,
                kind: ColumnarJoinInputPlanKind::JoinAggregate {
                    plan: Box::new(join_aggregate),
                },
            }));
        }
    }

    if let Some(input_name) = derived_relation_name(plan)
        && let Some(join) = columnar_join_plan_for_plan_with_options(plan, sources, false)?
    {
        return Ok(Some(ColumnarJoinInputPlan {
            input_name,
            schema: df_schema_to_arrow(plan.schema()),
            kind: ColumnarJoinInputPlanKind::Join {
                plan: Box::new(join),
            },
        }));
    }

    if let Some(input_name) = derived_relation_name(plan)
        && let Some(multijoin) = columnar_multijoin_plan_for_plan(plan, sources)?
    {
        return Ok(Some(ColumnarJoinInputPlan {
            input_name,
            schema: df_schema_to_arrow(plan.schema()),
            kind: ColumnarJoinInputPlanKind::MultiJoin { plan: multijoin },
        }));
    }

    if let Some(input_name) = derived_relation_name(plan)
        && let Some(union) = columnar_union_plan_for_plan(plan, sources)?
    {
        return Ok(Some(ColumnarJoinInputPlan {
            input_name,
            schema: df_schema_to_arrow(plan.schema()),
            kind: ColumnarJoinInputPlanKind::Union { plan: union },
        }));
    }

    let Some(source_name) = single_source_for_plan(plan, sources) else {
        return Ok(None);
    };
    let Some(source) = sources.get(&source_name) else {
        return Ok(None);
    };
    Ok(Some(ColumnarJoinInputPlan {
        input_name: source_name.clone(),
        schema: Arc::clone(&source.schema),
        kind: ColumnarJoinInputPlanKind::Source { source_name },
    }))
}

fn constant_relation_name(plan: &LogicalPlan) -> Option<String> {
    plan.schema()
        .iter()
        .find_map(|(relation, _)| relation.map(ToString::to_string))
}

fn derived_relation_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Projection(projection) => derived_relation_name(projection.input.as_ref()),
        LogicalPlan::Filter(filter) => derived_relation_name(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => {
            constant_relation_name(plan).or_else(|| Some(alias.alias.to_string()))
        }
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            derived_relation_name(sort.input.as_ref())
        }
        _ => None,
    }
}

fn collect_joins<'a>(
    plan: &'a LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    joins: &mut Vec<&'a Join>,
    skip_this_derived_join: bool,
    skip_derived_join_inputs: bool,
) -> Result<()> {
    if columnar_join_topn_plan_for_plan(plan, sources)?.is_some() {
        return Ok(());
    }
    if skip_this_derived_join
        && derived_relation_name(plan).is_some()
        && (columnar_grouped_max_plan_for_plan(plan, sources, &df_schema_to_arrow(plan.schema()))?
            .is_some()
            || columnar_grouped_count_plan_for_plan(
                plan,
                sources,
                &df_schema_to_arrow(plan.schema()),
            )?
            .is_some()
            || columnar_grouped_stats_plan_for_plan(
                plan,
                sources,
                &df_schema_to_arrow(plan.schema()),
            )?
            .is_some()
            || columnar_join_plan_for_plan_with_options(plan, sources, false)?.is_some()
            || columnar_multijoin_plan_for_plan(plan, sources)?.is_some()
            || columnar_union_plan_for_plan(plan, sources)?.is_some()
            || columnar_join_aggregate_plan_for_plan(plan, sources)?.is_some())
    {
        return Ok(());
    }
    match plan {
        LogicalPlan::Join(join) => {
            joins.push(join);
            collect_joins(
                join.left.as_ref(),
                sources,
                joins,
                skip_derived_join_inputs,
                skip_derived_join_inputs,
            )?;
            collect_joins(
                join.right.as_ref(),
                sources,
                joins,
                skip_derived_join_inputs,
                skip_derived_join_inputs,
            )?;
        }
        LogicalPlan::Projection(projection) => collect_joins(
            projection.input.as_ref(),
            sources,
            joins,
            skip_this_derived_join,
            skip_derived_join_inputs,
        )?,
        LogicalPlan::Filter(filter) => collect_joins(
            filter.input.as_ref(),
            sources,
            joins,
            skip_this_derived_join,
            skip_derived_join_inputs,
        )?,
        LogicalPlan::SubqueryAlias(alias) => collect_joins(
            alias.input.as_ref(),
            sources,
            joins,
            skip_this_derived_join,
            skip_derived_join_inputs,
        )?,
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => collect_joins(
            sort.input.as_ref(),
            sources,
            joins,
            skip_this_derived_join,
            skip_derived_join_inputs,
        )?,
        _ => {}
    }
    Ok(())
}

fn is_supported_join_type(join_type: &JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner
            | JoinType::Left
            | JoinType::Right
            | JoinType::Full
            | JoinType::LeftSemi
            | JoinType::RightSemi
            | JoinType::LeftAnti
            | JoinType::RightAnti
    )
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
        LogicalPlan::Aggregate(aggregate) => {
            collect_sources(aggregate.input.as_ref(), sources, out)
        }
        LogicalPlan::Distinct(distinct) => collect_sources(distinct.input(), sources, out),
        LogicalPlan::Union(union) => {
            for input in &union.inputs {
                collect_sources(input.as_ref(), sources, out);
            }
        }
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

fn contains_unsupported_join_wrapper(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<bool> {
    match plan {
        LogicalPlan::Projection(projection) => {
            contains_unsupported_join_wrapper(projection.input.as_ref(), sources)
        }
        LogicalPlan::Filter(filter) => {
            contains_unsupported_join_wrapper(filter.input.as_ref(), sources)
        }
        LogicalPlan::SubqueryAlias(alias) => {
            contains_unsupported_join_wrapper(alias.input.as_ref(), sources)
        }
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            contains_unsupported_join_wrapper(sort.input.as_ref(), sources)
        }
        LogicalPlan::Join(join) => {
            Ok(
                contains_unsupported_join_side_wrapper(join.left.as_ref(), sources)?
                    || contains_unsupported_join_side_wrapper(join.right.as_ref(), sources)?,
            )
        }
        LogicalPlan::TableScan(_) => Ok(false),
        _ => Ok(true),
    }
}

fn contains_unsupported_join_side_wrapper(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<bool> {
    if !plan_contains_table_scan(plan) {
        return Ok(false);
    }
    if columnar_topn_plan_for_plan(plan, sources)?.is_some() {
        return Ok(false);
    }
    if columnar_join_topn_plan_for_plan(plan, sources)?.is_some() {
        return Ok(false);
    }
    if derived_relation_name(plan).is_some()
        && columnar_grouped_max_plan_for_plan(plan, sources, &df_schema_to_arrow(plan.schema()))?
            .is_some()
    {
        return Ok(false);
    }
    if columnar_grouped_count_plan_for_plan(plan, sources, &df_schema_to_arrow(plan.schema()))?
        .is_some()
    {
        return Ok(false);
    }
    if derived_relation_name(plan).is_some()
        && columnar_grouped_stats_plan_for_plan(plan, sources, &df_schema_to_arrow(plan.schema()))?
            .is_some()
    {
        return Ok(false);
    }
    if derived_relation_name(plan).is_some()
        && columnar_join_plan_for_plan_with_options(plan, sources, false)?.is_some()
    {
        return Ok(false);
    }
    if derived_relation_name(plan).is_some()
        && columnar_multijoin_plan_for_plan(plan, sources)?.is_some()
    {
        return Ok(false);
    }
    if derived_relation_name(plan).is_some()
        && columnar_union_plan_for_plan(plan, sources)?.is_some()
    {
        return Ok(false);
    }
    if derived_relation_name(plan).is_some()
        && columnar_join_aggregate_plan_for_plan(plan, sources)?.is_some()
    {
        return Ok(false);
    }
    match plan {
        LogicalPlan::Projection(projection) => {
            contains_unsupported_join_side_wrapper(projection.input.as_ref(), sources)
        }
        LogicalPlan::Filter(filter) => {
            contains_unsupported_join_side_wrapper(filter.input.as_ref(), sources)
        }
        LogicalPlan::SubqueryAlias(alias) => {
            contains_unsupported_join_side_wrapper(alias.input.as_ref(), sources)
        }
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            contains_unsupported_join_side_wrapper(sort.input.as_ref(), sources)
        }
        LogicalPlan::TableScan(_) => Ok(false),
        _ => Ok(true),
    }
}

fn plan_contains_table_scan(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if matches!(node, LogicalPlan::TableScan(_)) {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    found
}

fn df_schema_to_arrow(schema: &datafusion::common::DFSchemaRef) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
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
