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
use datafusion::logical_expr::{JoinType, LogicalPlan, LogicalPlanBuilder, ScalarUDF};
use datafusion::physical_plan::collect;
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarZSet};
use dbsp::storage::KeyValueTable;

use crate::delta_consolidation::{
    add_weight_column_to_batches, diff_snapshot_batches, weighted_snapshot_schema,
};
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::table_provider::DynamicStateTableProvider;
use crate::vectorized_runtime::source_state::{rename_batches, resolve_source_table};
use crate::vectorized_source_delta::unit_source_delta_batches;

use super::columnar_topn::{TopNEvaluator, columnar_topn_plan_for_plan};
use super::{
    VectorizedMaterializedViewState, VectorizedSourceState, apply_weighted_snapshot_delta,
    normalize_batches,
};

pub(super) struct ColumnarJoinPlan {
    logical_plan: LogicalPlan,
    left: ColumnarJoinInputPlan,
    right: ColumnarJoinInputPlan,
    execution_strategy: ColumnarJoinExecutionStrategy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColumnarJoinExecutionStrategy {
    IncrementalInner,
    SnapshotDiff,
}

pub(super) struct ColumnarJoinMaterializedViewState {
    left: ColumnarJoinSourceState,
    right: ColumnarJoinSourceState,
    output_zset: SlateBackedColumnarZSet,
    left_delta_right_state: JoinDeltaEvaluator,
    left_state_right_delta: JoinDeltaEvaluator,
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
    input_zset: SlateBackedColumnarZSet,
    snapshot: Vec<RecordBatch>,
    constant: Option<ColumnarJoinConstantState>,
    topn: Option<ColumnarJoinTopNInputState>,
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
}

struct JoinInputTick {
    delta: ColumnarZSet,
    changed: bool,
    next_snapshot: Option<Vec<RecordBatch>>,
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

struct JoinSignedDelta {
    positive: Vec<RecordBatch>,
    negative: Vec<RecordBatch>,
}

pub(super) fn columnar_join_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarJoinPlan>> {
    let mut joins = Vec::new();
    collect_joins(plan, &mut joins);
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
    let expected_sources = [left.source_name(), right.source_name()]
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
    if all_sources != expected_sources {
        return Ok(None);
    }
    if contains_unsupported_join_wrapper(plan, sources)? {
        return Ok(None);
    }
    let execution_strategy = if matches!(join.join_type, JoinType::Inner)
        && left.source_name() != right.source_name()
        && left.is_source()
        && right.is_source()
    {
        ColumnarJoinExecutionStrategy::IncrementalInner
    } else {
        ColumnarJoinExecutionStrategy::SnapshotDiff
    };

    Ok(Some(ColumnarJoinPlan {
        logical_plan: plan.clone(),
        left,
        right,
        execution_strategy,
    }))
}

pub(super) async fn build_columnar_join_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarJoinPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarJoinMaterializedViewState> {
    let ColumnarJoinPlan {
        logical_plan,
        left,
        right,
        execution_strategy,
    } = plan;
    let mv_namespace = namespaces::materialized_view(view_name)?;
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

    let rebuild_each_evaluate = execution_strategy == ColumnarJoinExecutionStrategy::SnapshotDiff;
    let left_delta_right_state = JoinDeltaEvaluator::build(
        logical_plan.clone(),
        sources,
        udfs,
        output_schema,
        &left,
        &right,
        rebuild_each_evaluate,
    )
    .await
    .context("build left-delta/right-state join evaluator")?;
    let left_state_right_delta = JoinDeltaEvaluator::build(
        logical_plan.clone(),
        sources,
        udfs,
        output_schema,
        &left,
        &right,
        rebuild_each_evaluate,
    )
    .await
    .context("build left-state/right-delta join evaluator")?;
    let left_delta_right_delta = JoinDeltaEvaluator::build(
        logical_plan,
        sources,
        udfs,
        output_schema,
        &left,
        &right,
        rebuild_each_evaluate,
    )
    .await
    .context("build left-delta/right-delta join evaluator")?;
    let left = build_join_input_state(
        Arc::clone(&table),
        &mv_namespace,
        "left",
        left_namespace,
        left,
        sources,
        udfs,
        output_initialized,
    )
    .await
    .context("build SlateDB-backed left join input state")?;
    let right = build_join_input_state(
        table,
        &mv_namespace,
        "right",
        right_namespace,
        right,
        sources,
        udfs,
        output_initialized,
    )
    .await
    .context("build SlateDB-backed right join input state")?;

    Ok(ColumnarJoinMaterializedViewState {
        left,
        right,
        output_zset,
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
    }
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
) -> Result<ColumnarJoinSourceState> {
    let input_zset =
        SlateBackedColumnarZSet::new(Arc::clone(&table), namespace, Arc::clone(&input.schema))
            .await
            .with_context(|| {
                format!(
                    "initialize SlateDB-backed {side} join input zset for '{}'",
                    input.input_name
                )
            })?;

    match input.kind {
        ColumnarJoinInputPlanKind::Source { source_name } => {
            let source = sources
                .get(&source_name)
                .ok_or_else(|| anyhow::anyhow!("unknown join source '{source_name}'"))?;
            Ok(ColumnarJoinSourceState {
                input_name: input.input_name,
                source_name: Some(source_name),
                schema: Arc::clone(&source.schema),
                snapshot: snapshot_batches_from_zset(
                    &input_zset
                        .materialize_columnar()
                        .await
                        .with_context(|| format!("load {side} join input snapshot"))?,
                )?,
                input_zset,
                constant: None,
                topn: None,
            })
        }
        ColumnarJoinInputPlanKind::Constant { logical_plan } => {
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
                input_zset,
                snapshot,
                constant: Some(ColumnarJoinConstantState {
                    state_table: table,
                    initialized_key,
                    initialized,
                    pending_snapshot,
                }),
                topn: None,
            })
        }
        ColumnarJoinInputPlanKind::TopN {
            source_name,
            logical_plan,
        } => {
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
                input_zset,
                snapshot,
                constant: None,
                topn: Some(ColumnarJoinTopNInputState {
                    source_name,
                    source_schema: Arc::clone(&source.schema),
                    source_input_zset,
                    source_snapshot,
                    evaluator,
                }),
            })
        }
    }
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
    let Some(execution_strategy) = mv
        .columnar_join
        .as_ref()
        .map(|columnar| columnar.execution_strategy)
    else {
        return Ok(false);
    };
    if execution_strategy == ColumnarJoinExecutionStrategy::SnapshotDiff {
        return run_columnar_snapshot_diff_join_materialized_view_tick(
            registry,
            insert_batches,
            weighted_delta_batches,
            mv,
            version,
        )
        .await;
    }
    let Some(columnar) = mv.columnar_join.as_mut() else {
        return Ok(false);
    };
    let plan_start = Instant::now();

    let left_input_delta =
        source_input_delta(&columnar.left, insert_batches, weighted_delta_batches)?;
    let right_input_delta =
        source_input_delta(&columnar.right, insert_batches, weighted_delta_batches)?;
    let left_delta =
        persisted_source_delta(&mut columnar.left.input_zset, left_input_delta).await?;
    let right_delta =
        persisted_source_delta(&mut columnar.right.input_zset, right_input_delta).await?;
    let left_signed = signed_source_delta(&columnar.left.schema, left_delta.batches())?;
    let right_signed = signed_source_delta(&columnar.right.schema, right_delta.batches())?;

    let mut output_delta_batches = Vec::new();
    collect_join_outputs(
        columnar,
        &mut output_delta_batches,
        &left_signed.positive,
        &columnar.right.snapshot,
        1,
        JoinEvaluatorKind::LeftDeltaRightState,
    )
    .await?;
    collect_join_outputs(
        columnar,
        &mut output_delta_batches,
        &left_signed.negative,
        &columnar.right.snapshot,
        -1,
        JoinEvaluatorKind::LeftDeltaRightState,
    )
    .await?;
    collect_join_outputs(
        columnar,
        &mut output_delta_batches,
        &columnar.left.snapshot,
        &right_signed.positive,
        1,
        JoinEvaluatorKind::LeftStateRightDelta,
    )
    .await?;
    collect_join_outputs(
        columnar,
        &mut output_delta_batches,
        &columnar.left.snapshot,
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
            "apply Slate-backed join columnar snapshot delta for '{}'",
            mv.view_name
        )
    })?;
    columnar.left.snapshot =
        apply_source_snapshot_delta(&columnar.left.schema, &columnar.left.snapshot, &left_delta)
            .await?;
    columnar.right.snapshot = apply_source_snapshot_delta(
        &columnar.right.schema,
        &columnar.right.snapshot,
        &right_delta,
    )
    .await?;

    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "columnar_join",
        "SlateDB-backed join columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

async fn run_columnar_snapshot_diff_join_materialized_view_tick(
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

    let left_tick =
        prepare_join_input_tick(&mut columnar.left, insert_batches, weighted_delta_batches).await?;
    let right_tick =
        prepare_join_input_tick(&mut columnar.right, insert_batches, weighted_delta_batches)
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
        diff_snapshot_batches(
            Arc::clone(&mv.output_schema),
            &mv.previous_snapshot,
            &next_output,
        )
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
    let next_snapshot = apply_weighted_snapshot_delta(
        &mv.output_schema,
        &mv.previous_snapshot,
        delta_batches.clone(),
    )
    .await
    .with_context(|| {
        format!(
            "apply Slate-backed snapshot-diff join columnar snapshot delta for '{}'",
            mv.view_name
        )
    })?;

    columnar.left.snapshot = next_left_snapshot;
    columnar.right.snapshot = next_right_snapshot;
    mark_join_constant_initialized(&mut columnar.left).await?;
    mark_join_constant_initialized(&mut columnar.right).await?;
    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "columnar_join_snapshot_diff",
        "SlateDB-backed snapshot-diff join columnar DBSP materialized view tick completed"
    );
    Ok(true)
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
        JoinEvaluatorKind::LeftDeltaRightState => &columnar.left_delta_right_state,
        JoinEvaluatorKind::LeftStateRightDelta => &columnar.left_state_right_delta,
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
    if source.topn.is_some() {
        return prepare_topn_join_input_tick(source, insert_batches, weighted_delta_batches).await;
    }
    if source.constant.is_some() {
        return prepare_constant_join_input_tick(source).await;
    }

    let input_delta = source_input_delta(source, insert_batches, weighted_delta_batches)?;
    let delta = persisted_source_delta(&mut source.input_zset, input_delta).await?;
    let changed = !delta.batches().is_empty();
    Ok(JoinInputTick {
        delta,
        changed,
        next_snapshot: None,
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
    let topn_delta =
        ColumnarZSet::try_new_weighted(source.input_zset.value_schema(), topn_delta_batches)
            .with_context(|| format!("build join topn input delta for '{}'", source.input_name))?;
    let delta = persisted_source_delta(&mut source.input_zset, topn_delta).await?;
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

    let delta = if source.input_zset.current_handle().is_some() {
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
        persisted_source_delta(&mut source.input_zset, input_delta).await?
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
    snapshot_batches_from_zset(
        &source
            .input_zset
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
        left: &ColumnarJoinInputPlan,
        right: &ColumnarJoinInputPlan,
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
        input: &ColumnarJoinInputPlan,
        sources: &HashMap<String, VectorizedSourceState>,
        single_partition_scan: bool,
    ) -> Result<Self> {
        let provider = dynamic_join_provider(Arc::clone(&input.schema), single_partition_scan);
        let (alias_schema, alias_provider) = match &input.kind {
            ColumnarJoinInputPlanKind::Source { source_name } => {
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
            ColumnarJoinInputPlanKind::Constant { .. } | ColumnarJoinInputPlanKind::TopN { .. } => {
                (None, None)
            }
        };
        Ok(Self {
            provider,
            alias_schema,
            alias_provider,
        })
    }

    fn provider_for_table(
        &self,
        input: &ColumnarJoinInputPlan,
        table_name: &str,
    ) -> Option<Arc<dyn TableProvider>> {
        let ColumnarJoinInputPlanKind::Source { source_name } = &input.kind else {
            return None;
        };
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
    left: &ColumnarJoinInputPlan,
    left_input: &JoinEvaluatorInput,
    right: &ColumnarJoinInputPlan,
    right_input: &JoinEvaluatorInput,
) -> Result<LogicalPlan> {
    let transformed = logical_plan.transform_up(|plan| match plan {
        LogicalPlan::Join(mut join) => {
            join.left = Arc::new(
                rebind_join_side_logical_plan(join.left.as_ref().clone(), left, left_input)
                    .map_err(|err| datafusion::error::DataFusionError::Plan(err.to_string()))?,
            );
            join.right = Arc::new(
                rebind_join_side_logical_plan(join.right.as_ref().clone(), right, right_input)
                    .map_err(|err| datafusion::error::DataFusionError::Plan(err.to_string()))?,
            );
            Ok(Transformed::yes(LogicalPlan::Join(join)))
        }
        other => Ok(Transformed::no(other)),
    })?;
    Ok(transformed.data)
}

fn rebind_join_side_logical_plan(
    logical_plan: LogicalPlan,
    input_plan: &ColumnarJoinInputPlan,
    input: &JoinEvaluatorInput,
) -> Result<LogicalPlan> {
    if matches!(
        &input_plan.kind,
        ColumnarJoinInputPlanKind::Constant { .. } | ColumnarJoinInputPlanKind::TopN { .. }
    ) {
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

    if let Some(topn) = columnar_topn_plan_for_plan(plan, sources)? {
        let input_name =
            constant_relation_name(plan).unwrap_or_else(|| format!("__floe_join_{side}_topn"));
        return Ok(Some(ColumnarJoinInputPlan {
            input_name,
            schema: df_schema_to_arrow(plan.schema()),
            kind: ColumnarJoinInputPlanKind::TopN {
                source_name: topn.source_name,
                logical_plan: topn.logical_plan,
            },
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
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            collect_joins(sort.input.as_ref(), joins)
        }
        _ => {}
    }
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
