use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::logical_expr::logical_plan::{Join, TableScan};
use datafusion::logical_expr::{
    Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, ScalarUDF,
};
use datafusion::physical_plan::{ExecutionPlan, collect};
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarIndexedZSet, SlateBackedColumnarZSet};
use dbsp::storage::KeyValueTable;

use crate::columnar_snapshot::columnar_zset_weight_sum;
use crate::delta_consolidation::{add_weight_column_to_batches, weighted_snapshot_schema};
use crate::mv::registry::{ColumnarMaterializedViewStorage, MaterializedViewRegistry};
use crate::namespaces;
use crate::table_provider::DynamicStateTableProvider;
use crate::vectorized_runtime::source_state::{rename_batches, resolve_source_table};
use crate::vectorized_source_delta::unit_source_delta_batches;

use super::profile;
use super::{
    VectorizedMaterializedViewState, VectorizedSourceState, apply_keyed_source_snapshot_delta,
    apply_weighted_snapshot_delta, normalize_batches,
};

const APPEND_ONLY_JOIN_SNAPSHOT_ROW_LIMIT: usize = 100_000;

pub(super) struct ColumnarJoinPlan {
    logical_plan: LogicalPlan,
    left: ColumnarJoinInputPlan,
    right: ColumnarJoinInputPlan,
    join_key_pairs: Vec<ColumnarJoinKeyPair>,
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
    operator_table: Arc<dyn KeyValueTable>,
    left: ColumnarJoinSourceState,
    right: ColumnarJoinSourceState,
    output_zset: SlateBackedColumnarZSet,
    join_key_indices: Option<ColumnarJoinKeyIndices>,
    left_delta_right_state: Option<JoinDeltaEvaluator>,
    left_state_right_delta: Option<JoinDeltaEvaluator>,
    left_delta_right_delta: JoinDeltaEvaluator,
    initial_snapshot: Vec<RecordBatch>,
    row_count: i64,
    persist_source_input_zsets: bool,
}

impl ColumnarJoinMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }

    #[cfg(test)]
    pub(super) fn execution_strategy_name(&self) -> &'static str {
        "incremental_inner"
    }
}

pub(super) fn columnar_join_plan_sources_append_only(
    plan: &ColumnarJoinPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> bool {
    [
        join_input_source_name(&plan.left),
        join_input_source_name(&plan.right),
    ]
    .into_iter()
    .all(|source_name| {
        sources
            .get(source_name)
            .is_some_and(|source| source.append_only)
    })
}

fn join_input_source_name(input: &ColumnarJoinInputPlan) -> &str {
    match &input.kind {
        ColumnarJoinInputPlanKind::Source { source_name } => source_name,
    }
}

struct ColumnarJoinSourceState {
    input_name: String,
    source_name: Option<String>,
    schema: SchemaRef,
    primary_key_columns: Vec<String>,
    input_filter: Option<JoinInputFilterEvaluator>,
    input_zset: Option<SlateBackedColumnarZSet>,
    input_index: Option<Box<SlateBackedColumnarIndexedZSet>>,
    snapshot: Vec<RecordBatch>,
    append_only_snapshot_enabled: bool,
    defer_index_maintenance: bool,
    index_stale: bool,
}

struct ColumnarJoinInputPlan {
    input_name: String,
    schema: SchemaRef,
    kind: ColumnarJoinInputPlanKind,
    local_filters: Vec<Expr>,
}

enum ColumnarJoinInputPlanKind {
    Source { source_name: String },
}

pub(super) struct ColumnarJoinTick {
    pub(super) delta: ColumnarZSet,
    pub(super) next_snapshot: Vec<RecordBatch>,
    pub(super) row_count_delta: i64,
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

struct JoinInputFilterEvaluator {
    ctx: SessionContext,
    provider: Arc<DynamicStateTableProvider>,
    logical_plan: LogicalPlan,
    weighted_schema: SchemaRef,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JoinPredicateSide {
    Left,
    Right,
}

impl JoinInputFilterEvaluator {
    async fn build(
        input_name: &str,
        value_schema: &SchemaRef,
        filters: Vec<Expr>,
        udfs: &[ScalarUDF],
    ) -> Result<Option<Self>> {
        if filters.is_empty() {
            return Ok(None);
        }
        let weighted_schema = weighted_snapshot_schema(value_schema)?;
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        for udf in udfs.iter().cloned() {
            ctx.register_udf(udf);
        }
        let provider = Arc::new(DynamicStateTableProvider::new_with_scan_partitions(
            Arc::clone(&weighted_schema),
            1,
        ));
        let mut logical_plan = LogicalPlanBuilder::scan(
            input_name,
            provider_as_source(Arc::clone(&provider) as Arc<dyn TableProvider>),
            None,
        )?
        .build()?;
        for filter in filters {
            logical_plan = LogicalPlanBuilder::from(logical_plan)
                .filter(unqualify_expr_columns(filter)?)?
                .build()?;
        }
        Ok(Some(Self {
            ctx,
            provider,
            logical_plan,
            weighted_schema,
        }))
    }

    async fn evaluate(&self, weighted_batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
        if weighted_batches.iter().all(|batch| batch.num_rows() == 0) {
            return Ok(Vec::new());
        }
        self.provider
            .set_batches(weighted_batches.to_vec())
            .context("set join input filter batches")?;
        let result = match self
            .ctx
            .state()
            .create_physical_plan(&self.logical_plan)
            .await
        {
            Ok(plan) => collect(plan, self.ctx.task_ctx())
                .await
                .context("execute join input local filter")
                .and_then(|batches| normalize_batches(batches, &self.weighted_schema)),
            Err(err) => Err(err).context("build join input local filter plan"),
        };
        let clear_result = self.provider.set_batches(Vec::new());
        match (result, clear_result) {
            (Ok(batches), Ok(())) => Ok(batches),
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err).context("clear join input local filter batches"),
        }
    }
}

pub(super) fn columnar_join_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarJoinPlan>> {
    let mut joins = Vec::new();
    collect_joins(plan, &mut joins)?;
    let [join] = joins.as_slice() else {
        return Ok(None);
    };
    if !is_supported_join_type(&join.join_type) || (join.on.is_empty() && join.filter.is_none()) {
        return Ok(None);
    }
    let Some(mut left) = join_input_plan_for_side(join.left.as_ref(), sources, "left")? else {
        return Ok(None);
    };
    let Some(mut right) = join_input_plan_for_side(join.right.as_ref(), sources, "right")? else {
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
    let (left_filters, right_filters) =
        local_join_filters(plan, join, &left.schema, &right.schema)?;
    left.local_filters = left_filters;
    right.local_filters = right_filters;
    let join_key_pairs = simple_join_key_pairs(join, &left.schema, &right.schema);
    if !matches!(join.join_type, JoinType::Inner)
        || left.source_name() == right.source_name()
        || !left.is_source()
        || !right.is_source()
        || join_key_pairs.is_empty()
    {
        return Ok(None);
    }

    Ok(Some(ColumnarJoinPlan {
        logical_plan: plan.clone(),
        left,
        right,
        join_key_pairs,
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

fn local_join_filters(
    plan: &LogicalPlan,
    join: &Join,
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
) -> Result<(Vec<Expr>, Vec<Expr>)> {
    let mut predicates = Vec::new();
    collect_filter_conjuncts(plan, &mut predicates);
    if let Some(filter) = join.filter.as_ref() {
        split_conjunctions(filter, &mut predicates);
    }

    let mut left = Vec::new();
    let mut right = Vec::new();
    let mut seen_left = HashSet::new();
    let mut seen_right = HashSet::new();
    for predicate in predicates {
        match local_predicate_side(&predicate, left_schema, right_schema) {
            Some(JoinPredicateSide::Left) => {
                let predicate = unqualify_expr_columns(predicate)?;
                if seen_left.insert(format!("{predicate:?}")) {
                    left.push(predicate);
                }
            }
            Some(JoinPredicateSide::Right) => {
                let predicate = unqualify_expr_columns(predicate)?;
                if seen_right.insert(format!("{predicate:?}")) {
                    right.push(predicate);
                }
            }
            None => {}
        }
    }
    Ok((left, right))
}

fn collect_filter_conjuncts(plan: &LogicalPlan, out: &mut Vec<Expr>) {
    match plan {
        LogicalPlan::Filter(filter) => {
            split_conjunctions(&filter.predicate, out);
            collect_filter_conjuncts(filter.input.as_ref(), out);
        }
        LogicalPlan::Projection(projection) => {
            collect_filter_conjuncts(projection.input.as_ref(), out)
        }
        LogicalPlan::SubqueryAlias(alias) => collect_filter_conjuncts(alias.input.as_ref(), out),
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            collect_filter_conjuncts(sort.input.as_ref(), out)
        }
        LogicalPlan::Join(join) => {
            collect_filter_conjuncts(join.left.as_ref(), out);
            collect_filter_conjuncts(join.right.as_ref(), out);
        }
        _ => {}
    }
}

fn split_conjunctions(expr: &Expr, out: &mut Vec<Expr>) {
    let Expr::BinaryExpr(binary) = expr else {
        out.push(expr.clone());
        return;
    };
    if binary.op == Operator::And {
        split_conjunctions(binary.left.as_ref(), out);
        split_conjunctions(binary.right.as_ref(), out);
    } else {
        out.push(expr.clone());
    }
}

fn local_predicate_side(
    expr: &Expr,
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
) -> Option<JoinPredicateSide> {
    let mut side = None;
    for column in expr.column_refs() {
        let left_has = left_schema.index_of(&column.name).is_ok();
        let right_has = right_schema.index_of(&column.name).is_ok();
        let column_side = match (left_has, right_has) {
            (true, false) => JoinPredicateSide::Left,
            (false, true) => JoinPredicateSide::Right,
            _ => return None,
        };
        if side.is_some_and(|current| current != column_side) {
            return None;
        }
        side = Some(column_side);
    }
    side
}

fn unqualify_expr_columns(expr: Expr) -> Result<Expr> {
    Ok(expr
        .transform_up(|expr| match expr {
            Expr::Column(mut column) => {
                column.relation = None;
                Ok(Transformed::yes(Expr::Column(column)))
            }
            other => Ok(Transformed::no(other)),
        })?
        .data)
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
    build_columnar_join_materialized_view_state_in_namespace_with_options(
        table,
        mv_namespace,
        output_schema,
        plan,
        sources,
        udfs,
        true,
        false,
    )
    .await
}

pub(super) async fn build_columnar_join_materialized_view_state_in_namespace_delta_only(
    table: Arc<dyn KeyValueTable>,
    mv_namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarJoinPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarJoinMaterializedViewState> {
    build_columnar_join_materialized_view_state_in_namespace_with_options(
        table,
        mv_namespace,
        output_schema,
        plan,
        sources,
        udfs,
        false,
        false,
    )
    .await
}

pub(super) async fn build_columnar_join_materialized_view_state_in_namespace_delta_only_with_persistent_inputs(
    table: Arc<dyn KeyValueTable>,
    mv_namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarJoinPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarJoinMaterializedViewState> {
    build_columnar_join_materialized_view_state_in_namespace_with_options(
        table,
        mv_namespace,
        output_schema,
        plan,
        sources,
        udfs,
        true,
        true,
    )
    .await
}

async fn build_columnar_join_materialized_view_state_in_namespace_with_options(
    table: Arc<dyn KeyValueTable>,
    mv_namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarJoinPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
    persist_source_input_zsets: bool,
    reuse_physical_plan: bool,
) -> Result<ColumnarJoinMaterializedViewState> {
    let ColumnarJoinPlan {
        logical_plan,
        left,
        right,
        join_key_pairs,
    } = plan;
    let join_key_indices = Some(
        resolve_join_key_indices(&left.schema, &right.schema, &join_key_pairs)
            .context("resolve columnar join index keys")?,
    );
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
    let initial_output = output_zset
        .materialize_columnar()
        .await
        .context("load join output snapshot")?;
    let initial_row_count = columnar_zset_weight_sum(&initial_output)?;
    let initial_snapshot = snapshot_batches_from_zset(&initial_output)?;
    let output_initialized = output_zset.current_handle().is_some();

    let left_evaluator_plan = JoinEvaluatorInputPlan::from_join_input(&left);
    let right_evaluator_plan = JoinEvaluatorInputPlan::from_join_input(&right);

    let mut left = Box::pin(build_join_input_state(
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
        persist_source_input_zsets,
        false,
    ))
    .await
    .context("build SlateDB-backed left join input state")?;
    let right_segment_backed_large_ranges =
        persist_source_input_zsets && left.append_only_snapshot_enabled;
    let mut right = Box::pin(build_join_input_state(
        Arc::clone(&table),
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
        persist_source_input_zsets,
        right_segment_backed_large_ranges,
    ))
    .await
    .context("build SlateDB-backed right join input state")?;
    if persist_source_input_zsets && left.append_only_snapshot_enabled {
        right.defer_index_maintenance = true;
    }
    if persist_source_input_zsets && right.append_only_snapshot_enabled {
        left.defer_index_maintenance = true;
    }

    // Reusing DataFusion physical join plans is only valid for selected nested
    // join inputs. Generic materialized joins still rebuild so provider batches
    // are never captured stale by a physical plan.
    let rebuild_each_evaluate = !reuse_physical_plan;
    let left_delta_right_state = Some(
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
    );
    let left_state_right_delta = Some(
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
    );
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
        operator_table: Arc::clone(&table),
        left,
        right,
        output_zset,
        join_key_indices,
        left_delta_right_state,
        left_state_right_delta,
        left_delta_right_delta,
        initial_snapshot,
        row_count: initial_row_count,
        persist_source_input_zsets,
    })
}

fn join_input_namespace(mv_namespace: &str, side: &str, input: &ColumnarJoinInputPlan) -> String {
    let _ = side;
    format!("{mv_namespace}/columnar/join/{}/input", input.input_name)
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
    _mv_namespace: &str,
    side: &str,
    namespace: String,
    input: ColumnarJoinInputPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
    _output_initialized: bool,
    index_key_indices: Option<&[usize]>,
    persist_source_input_zsets: bool,
    segment_backed_large_ranges: bool,
) -> Result<ColumnarJoinSourceState> {
    let ColumnarJoinInputPlanKind::Source { source_name } = &input.kind;
    let source_name = source_name.clone();
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
    let input_filter = JoinInputFilterEvaluator::build(
        &input.input_name,
        &input.schema,
        input.local_filters.clone(),
        udfs,
    )
    .await
    .with_context(|| {
        format!(
            "build {side} join input local filter for '{}'",
            input.input_name
        )
    })?;
    let snapshot_zset = input_zset
        .materialize_columnar()
        .await
        .with_context(|| format!("load {side} join input snapshot"))?;
    let index_snapshot_zset = filter_join_source_delta(input_filter.as_ref(), snapshot_zset)
        .await
        .with_context(|| format!("filter {side} join input snapshot"))?;
    let snapshot = snapshot_batches_from_zset(&index_snapshot_zset)?;
    let append_only_snapshot_enabled = source.append_only
        && record_batch_row_count(&snapshot) <= APPEND_ONLY_JOIN_SNAPSHOT_ROW_LIMIT;
    let input_index = if let Some(key_indices) = index_key_indices {
        let mut index = if segment_backed_large_ranges || !persist_source_input_zsets {
            SlateBackedColumnarIndexedZSet::new_with_segment_backed_large_ranges(
                Arc::clone(&table),
                index_namespace,
                Arc::clone(&source.schema),
                key_indices.to_vec(),
            )
            .await
        } else {
            SlateBackedColumnarIndexedZSet::new(
                Arc::clone(&table),
                index_namespace,
                Arc::clone(&source.schema),
                key_indices.to_vec(),
            )
            .await
        }
        .with_context(|| {
            format!(
                "initialize SlateDB-backed {side} join input index for '{}'",
                input.input_name
            )
        })?;
        let should_rebuild_index = persist_source_input_zsets
            || (!index.has_persisted_segments() && !index_snapshot_zset.is_empty());
        if should_rebuild_index {
            index
                .rebuild_from_zset(&index_snapshot_zset)
                .await
                .with_context(|| {
                    format!(
                        "rebuild SlateDB-backed {side} join input index for '{}'",
                        input.input_name
                    )
                })?;
        }
        Some(Box::new(index))
    } else {
        None
    };
    Ok(ColumnarJoinSourceState {
        input_name: input.input_name,
        source_name: Some(source_name),
        schema: Arc::clone(&source.schema),
        primary_key_columns: source.primary_key_columns.clone(),
        input_filter,
        snapshot,
        append_only_snapshot_enabled,
        defer_index_maintenance: false,
        index_stale: false,
        input_zset: Some(input_zset),
        input_index,
    })
}

pub(super) async fn run_columnar_join_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<()> {
    let super::MaterializedViewOperator::Join(columnar) = &mut mv.operator else {
        unreachable!("join tick dispatched to non-join operator")
    };
    let plan_start = Instant::now();
    let tick = Box::pin(run_columnar_join_state_tick_inner(
        columnar,
        insert_batches,
        weighted_delta_batches,
        &mv.output_schema,
        &mv.previous_snapshot,
        true,
        false,
    ))
    .await?;

    let delta_batches = tick.delta.batches().to_vec();
    columnar.row_count = columnar.row_count.saturating_add(tick.row_count_delta);
    if columnar.row_count < 0 {
        bail!(
            "join columnar materialized view '{}' row count became negative",
            mv.view_name
        );
    }
    let snapshot_rows =
        usize::try_from(columnar.row_count).context("join row count exceeds usize")?;
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
        mv.previous_snapshot = tick.next_snapshot;
    }
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "columnar_join",
        "SlateDB-backed join columnar DBSP materialized view tick completed"
    );
    Ok(())
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
    persist_output_zset: bool,
    maintain_output_snapshot: bool,
) -> Result<ColumnarJoinTick> {
    let total_start = profile::start();
    let phase_start = profile::start();
    let prepare_source_delta_start = Instant::now();
    let left_input_delta =
        source_input_delta(&columnar.left, insert_batches, weighted_delta_batches)?;
    let right_input_delta =
        source_input_delta(&columnar.right, insert_batches, weighted_delta_batches)?;
    let persist_source_delta =
        persist_output_zset || maintain_output_snapshot || columnar.persist_source_input_zsets;
    let left_delta = {
        let left_zset = columnar
            .left
            .input_zset
            .as_mut()
            .context("incremental join left source zset missing")?;
        prepare_join_source_delta(left_zset, left_input_delta, persist_source_delta).await?
    };
    let left_delta = filter_join_source_delta(columnar.left.input_filter.as_ref(), left_delta)
        .await
        .context("filter left join source delta")?;
    let right_delta = {
        let right_zset = columnar
            .right
            .input_zset
            .as_mut()
            .context("incremental join right source zset missing")?;
        prepare_join_source_delta(right_zset, right_input_delta, persist_source_delta).await?
    };
    let right_delta = filter_join_source_delta(columnar.right.input_filter.as_ref(), right_delta)
        .await
        .context("filter right join source delta")?;
    let prepare_source_delta_ms = prepare_source_delta_start.elapsed().as_millis() as u64;
    profile::record_since("join.prepare_source_delta", phase_start);
    let phase_start = profile::start();
    let signed_delta_start = Instant::now();
    let left_signed = signed_source_delta(&columnar.left.schema, left_delta.batches())?;
    let right_signed = signed_source_delta(&columnar.right.schema, right_delta.batches())?;
    let signed_delta_ms = signed_delta_start.elapsed().as_millis() as u64;
    profile::record_since("join.signed_delta", phase_start);
    let join_key_indices = columnar
        .join_key_indices
        .as_ref()
        .context("incremental join key indices missing")?;
    let phase_start = profile::start();
    let lookup_right_start = Instant::now();
    let right_state_for_left_delta = if left_delta.is_empty() {
        Vec::new()
    } else if columnar.right.append_only_snapshot_enabled {
        columnar.right.snapshot.clone()
    } else {
        ensure_join_input_index_current(&mut columnar.right, "right").await?;
        lookup_indexed_join_state_for_delta(
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
        .await?
    };
    let lookup_right_ms = lookup_right_start.elapsed().as_millis() as u64;
    profile::record_since("join.lookup_right_total", phase_start);
    let phase_start = profile::start();
    let lookup_left_start = Instant::now();
    let left_state_for_right_delta = if right_delta.is_empty() {
        Vec::new()
    } else if columnar.left.append_only_snapshot_enabled {
        columnar.left.snapshot.clone()
    } else {
        ensure_join_input_index_current(&mut columnar.left, "left").await?;
        lookup_indexed_join_state_for_delta(
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
        .await?
    };
    let lookup_left_ms = lookup_left_start.elapsed().as_millis() as u64;
    profile::record_since("join.lookup_left_total", phase_start);

    let phase_start = profile::start();
    let collect_outputs_start = Instant::now();
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
    let collect_outputs_ms = collect_outputs_start.elapsed().as_millis() as u64;
    profile::record_since("join.collect_outputs", phase_start);

    let phase_start = profile::start();
    let build_output_start = Instant::now();
    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build join output zset delta")?;
    let build_output_ms = build_output_start.elapsed().as_millis() as u64;
    profile::record_since("join.build_output_zset", phase_start);
    let left_delta_rows = left_delta
        .batches()
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    let right_delta_rows = right_delta
        .batches()
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    let right_state_rows = right_state_for_left_delta
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    let left_state_rows = left_state_for_right_delta
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    let output_delta_rows = output_delta
        .batches()
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    let mut output_create_ms = 0_u64;
    if persist_output_zset {
        let phase_start = profile::start();
        let output_create_start = Instant::now();
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
        output_create_ms = output_create_start.elapsed().as_millis() as u64;
        profile::record_since("join.output_create_version", phase_start);
    }
    let persisted_output_delta = output_delta;

    let mut output_snapshot_ms = 0_u64;
    let next_snapshot = if maintain_output_snapshot {
        let phase_start = profile::start();
        let output_snapshot_start = Instant::now();
        let next_snapshot = apply_weighted_snapshot_delta(
            output_schema,
            previous_snapshot,
            persisted_output_delta.batches().to_vec(),
        )
        .await
        .context("apply Slate-backed join columnar snapshot delta")?;
        output_snapshot_ms = output_snapshot_start.elapsed().as_millis() as u64;
        profile::record_since("join.output_snapshot_delta", phase_start);
        next_snapshot
    } else {
        Vec::new()
    };
    let mut apply_left_index_ms = 0_u64;
    if let Some(index) = columnar.left.input_index.as_deref_mut() {
        let phase_start = profile::start();
        let apply_left_index_start = Instant::now();
        if columnar.left.defer_index_maintenance && !left_delta.is_empty() {
            columnar.left.index_stale = true;
        } else {
            index
                .apply_delta(&left_delta)
                .await
                .context("apply left join delta to SlateDB-backed columnar index")?;
        }
        apply_left_index_ms = apply_left_index_start.elapsed().as_millis() as u64;
        profile::record_since("join.apply_left_index", phase_start);
    }
    let mut apply_right_index_ms = 0_u64;
    if let Some(index) = columnar.right.input_index.as_deref_mut() {
        let phase_start = profile::start();
        let apply_right_index_start = Instant::now();
        if columnar.right.defer_index_maintenance && !right_delta.is_empty() {
            columnar.right.index_stale = true;
        } else {
            index
                .apply_delta(&right_delta)
                .await
                .context("apply right join delta to SlateDB-backed columnar index")?;
        }
        apply_right_index_ms = apply_right_index_start.elapsed().as_millis() as u64;
        profile::record_since("join.apply_right_index", phase_start);
    }

    if !maintain_output_snapshot {
        if columnar.left.append_only_snapshot_enabled
            && !apply_append_only_join_snapshot_delta(&mut columnar.left.snapshot, &left_delta)?
        {
            columnar.left.append_only_snapshot_enabled = false;
            columnar.left.snapshot.clear();
        }
        if columnar.right.append_only_snapshot_enabled
            && !apply_append_only_join_snapshot_delta(&mut columnar.right.snapshot, &right_delta)?
        {
            columnar.right.append_only_snapshot_enabled = false;
            columnar.right.snapshot.clear();
        }
    }

    let mut source_snapshot_delta_ms = 0_u64;
    if maintain_output_snapshot {
        let phase_start = profile::start();
        let source_snapshot_delta_start = Instant::now();
        columnar.left.snapshot = apply_source_snapshot_delta(
            &columnar.left.schema,
            &columnar.left.primary_key_columns,
            &columnar.left.snapshot,
            &left_delta,
        )
        .await?;
        columnar.right.snapshot = apply_source_snapshot_delta(
            &columnar.right.schema,
            &columnar.right.primary_key_columns,
            &columnar.right.snapshot,
            &right_delta,
        )
        .await?;
        if columnar.left.append_only_snapshot_enabled
            && record_batch_row_count(&columnar.left.snapshot) > APPEND_ONLY_JOIN_SNAPSHOT_ROW_LIMIT
        {
            columnar.left.append_only_snapshot_enabled = false;
            columnar.left.snapshot.clear();
        }
        if columnar.right.append_only_snapshot_enabled
            && record_batch_row_count(&columnar.right.snapshot)
                > APPEND_ONLY_JOIN_SNAPSHOT_ROW_LIMIT
        {
            columnar.right.append_only_snapshot_enabled = false;
            columnar.right.snapshot.clear();
        }
        source_snapshot_delta_ms = source_snapshot_delta_start.elapsed().as_millis() as u64;
        profile::record_since("join.source_snapshot_delta", phase_start);
    }

    tracing::debug!(
        left_delta_rows,
        right_delta_rows,
        right_state_rows,
        left_state_rows,
        output_delta_rows,
        prepare_source_delta_ms,
        signed_delta_ms,
        lookup_right_ms,
        lookup_left_ms,
        collect_outputs_ms,
        build_output_ms,
        output_create_ms,
        output_snapshot_ms,
        apply_left_index_ms,
        apply_right_index_ms,
        source_snapshot_delta_ms,
        mode = "columnar_join_incremental",
        "SlateDB-backed join columnar DBSP state tick completed"
    );

    profile::record_since("join.total", total_start);
    Ok(ColumnarJoinTick {
        row_count_delta: columnar_zset_weight_sum(&persisted_output_delta)
            .context("compute join row-count delta")?,
        delta: persisted_output_delta,
        next_snapshot,
        input_changed: !left_delta.batches().is_empty() || !right_delta.batches().is_empty(),
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

async fn persisted_source_delta(
    zset: &mut SlateBackedColumnarZSet,
    input_delta: ColumnarZSet,
) -> Result<ColumnarZSet> {
    let base = zset.current_handle().map(|handle| handle.version);
    zset.create_version(&input_delta, base).await?;
    Ok(input_delta)
}

async fn prepare_join_source_delta(
    zset: &mut SlateBackedColumnarZSet,
    input_delta: ColumnarZSet,
    persist: bool,
) -> Result<ColumnarZSet> {
    if persist {
        persisted_source_delta(zset, input_delta).await
    } else {
        Ok(input_delta)
    }
}

fn apply_append_only_join_snapshot_delta(
    snapshot: &mut Vec<RecordBatch>,
    delta: &ColumnarZSet,
) -> Result<bool> {
    if delta.is_empty() {
        return Ok(true);
    }
    let mut delta_batches = snapshot_batches_from_zset(delta)?;
    let delta_rows = record_batch_row_count(&delta_batches);
    if delta_rows == 0 {
        return Ok(true);
    }
    let current_rows = record_batch_row_count(snapshot);
    if current_rows.saturating_add(delta_rows) > APPEND_ONLY_JOIN_SNAPSHOT_ROW_LIMIT {
        return Ok(false);
    }
    if snapshot.len() == 1 && snapshot[0].num_rows() == 0 {
        snapshot.clear();
    }
    snapshot.append(&mut delta_batches);
    Ok(true)
}

fn record_batch_row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

async fn ensure_join_input_index_current(
    source: &mut ColumnarJoinSourceState,
    side: &str,
) -> Result<()> {
    if !source.index_stale {
        return Ok(());
    }
    let input_zset = source
        .input_zset
        .as_mut()
        .context("deferred join source zset missing")?;
    let snapshot_zset = input_zset
        .materialize_columnar()
        .await
        .with_context(|| format!("materialize stale {side} join input zset"))?;
    let index_snapshot_zset = filter_join_source_delta(source.input_filter.as_ref(), snapshot_zset)
        .await
        .with_context(|| format!("filter stale {side} join input snapshot"))?;
    source
        .input_index
        .as_deref_mut()
        .context("deferred join source index missing")?
        .rebuild_from_zset(&index_snapshot_zset)
        .await
        .with_context(|| format!("rebuild deferred {side} join input index"))?;
    source.index_stale = false;
    Ok(())
}

async fn filter_join_source_delta(
    input_filter: Option<&JoinInputFilterEvaluator>,
    delta: ColumnarZSet,
) -> Result<ColumnarZSet> {
    let Some(input_filter) = input_filter else {
        return Ok(delta);
    };
    if delta.is_empty() {
        return Ok(delta);
    }
    let value_schema = delta.value_schema();
    let filtered_batches = input_filter
        .evaluate(delta.batches())
        .await
        .context("evaluate join input local filter")?;
    ColumnarZSet::try_new_weighted(value_schema, filtered_batches)
        .context("build filtered join source delta")
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
    let total_start = profile::start();
    let key_phase = match side {
        "right" => "join.lookup_right_build_keys",
        "left" => "join.lookup_left_build_keys",
        _ => "join.lookup_build_keys",
    };
    let lookup_phase = match side {
        "right" => "join.lookup_right_index_scan",
        "left" => "join.lookup_left_index_scan",
        _ => "join.lookup_index_scan",
    };
    let materialize_phase = match side {
        "right" => "join.lookup_right_materialize",
        "left" => "join.lookup_left_materialize",
        _ => "join.lookup_materialize",
    };
    let total_phase = match side {
        "right" => "join.lookup_right_inner_total",
        "left" => "join.lookup_left_inner_total",
        _ => "join.lookup_inner_total",
    };

    let phase_start = profile::start();
    let key_batches =
        lookup_key_batches_from_delta(delta_batches, delta_key_indices, &state_index.key_schema())
            .with_context(|| format!("build {side} join state lookup keys"))?;
    profile::record_since(key_phase, phase_start);
    let phase_start = profile::start();
    let weighted_lookup = state_index
        .lookup_key_batches(&key_batches)
        .await
        .with_context(|| format!("lookup {side} join state by indexed keys"))?;
    profile::record_since(lookup_phase, phase_start);
    if weighted_lookup.is_empty() {
        profile::record_since(total_phase, total_start);
        return Ok(Vec::new());
    }
    let phase_start = profile::start();
    let materialized = if let Some(unit_positive) =
        strip_unit_positive_weight_column(state_schema, weighted_lookup.batches())?
    {
        unit_positive
    } else {
        apply_weighted_snapshot_delta(state_schema, &[], weighted_lookup.batches().to_vec())
            .await
            .with_context(|| format!("materialize {side} indexed join state lookup"))?
    };
    profile::record_since(materialize_phase, phase_start);
    profile::record_since(total_phase, total_start);
    Ok(materialized)
}

fn strip_unit_positive_weight_column(
    schema: &SchemaRef,
    weighted_batches: &[RecordBatch],
) -> Result<Option<Vec<RecordBatch>>> {
    let weighted_schema = weighted_snapshot_schema(schema)?;
    let weight_idx = weighted_schema.index_of(WEIGHT_COLUMN_NAME)?;
    for batch in weighted_batches {
        if batch.schema().as_ref() != weighted_schema.as_ref() {
            bail!("indexed join state lookup schema does not match weighted state schema");
        }
        let weights = batch
            .column(weight_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("indexed join state weight column must be Int64"))?;
        for row_idx in 0..batch.num_rows() {
            if weights.is_null(row_idx) || weights.value(row_idx) != 1 {
                return Ok(None);
            }
        }
    }

    let mut output = Vec::new();
    for batch in weighted_batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| (idx != weight_idx).then(|| Arc::clone(column)))
            .collect::<Vec<_>>();
        output.push(
            RecordBatch::try_new(Arc::clone(schema), columns)
                .context("strip indexed join state weight column")?,
        );
    }
    Ok(Some(output))
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
            fresh_execution_plan_from_template(Arc::clone(
                self.plan
                    .as_ref()
                    .context("cached vectorized join delta physical plan missing")?,
            ))
            .context("reset cached vectorized join delta physical plan")?
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

fn fresh_execution_plan_from_template(
    plan: Arc<dyn ExecutionPlan>,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    // DataFusion hash/nested-loop joins keep build-side state inside the
    // physical plan. Reuse the optimized plan shape, but reset every node so
    // dynamic input providers are read fresh for each delta evaluation.
    let children = plan
        .children()
        .into_iter()
        .map(|child| fresh_execution_plan_from_template(Arc::clone(child)))
        .collect::<datafusion::error::Result<Vec<_>>>()?;
    plan.with_new_children(children)?.reset_state()
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
        }
    }

    fn source_names(&self) -> BTreeSet<String> {
        match &self.kind {
            ColumnarJoinInputPlanKind::Source { source_name } => {
                [source_name.clone()].into_iter().collect()
            }
        }
    }

    fn is_source(&self) -> bool {
        matches!(self.kind, ColumnarJoinInputPlanKind::Source { .. })
    }
}

fn join_input_plan_for_side(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    _side: &str,
) -> Result<Option<ColumnarJoinInputPlan>> {
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
        local_filters: Vec::new(),
    }))
}

fn collect_joins<'a>(plan: &'a LogicalPlan, joins: &mut Vec<&'a Join>) -> Result<()> {
    match plan {
        LogicalPlan::Join(join) => {
            joins.push(join);
            collect_joins(join.left.as_ref(), joins)?;
            collect_joins(join.right.as_ref(), joins)?;
        }
        LogicalPlan::Projection(projection) => collect_joins(projection.input.as_ref(), joins)?,
        LogicalPlan::Filter(filter) => collect_joins(filter.input.as_ref(), joins)?,
        LogicalPlan::SubqueryAlias(alias) => collect_joins(alias.input.as_ref(), joins)?,
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            collect_joins(sort.input.as_ref(), joins)?
        }
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
        LogicalPlan::TableScan(scan) => Ok(table_scan_source(scan, sources).is_none()),
        _ => Ok(true),
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
