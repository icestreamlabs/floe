use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, TimestampMillisecondArray, UInt32Array,
};
use datafusion::arrow::compute::{concat_batches, take};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchemaRef};
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::logical_expr::logical_plan::{Join, TableScan};
use datafusion::logical_expr::{
    BinaryExpr, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, ScalarUDF,
    UserDefinedLogicalNodeCore,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::collect;
use dbsp::FloeAsofJoinNode;
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarZSet};
use dbsp::storage::KeyValueTable;

use crate::delta_consolidation::diff_snapshot_batches;
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::scalar_array_builder::ScalarColumnBuilder;
use crate::table_provider::DynamicStateTableProvider;
use crate::vectorized_runtime::source_state::{rename_batches, resolve_source_table};

use super::{
    VectorizedMaterializedViewState, VectorizedSourceState, apply_weighted_snapshot_delta,
    normalize_batches,
};

pub(super) struct ColumnarComposedPlan {
    logical_plan: LogicalPlan,
    source_names: Vec<String>,
}

pub(super) struct ColumnarComposedMaterializedViewState {
    sources: Vec<ColumnarComposedSourceState>,
    output_zset: SlateBackedColumnarZSet,
    evaluator: ComposedEvaluator,
    initial_snapshot: Vec<RecordBatch>,
    operator_label: &'static str,
    log_mode: &'static str,
}

impl ColumnarComposedMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

struct ColumnarComposedSourceState {
    source_name: String,
    schema: SchemaRef,
    input_zset: SlateBackedColumnarZSet,
    snapshot: Vec<RecordBatch>,
}

struct ComposedEvaluator {
    ctx: SessionContext,
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    inputs: HashMap<String, ComposedEvaluatorInput>,
    asof_joins: Vec<ComposedAsofJoinEvaluator>,
    output_schema: SchemaRef,
}

struct ComposedEvaluatorInput {
    provider: Arc<DynamicStateTableProvider>,
    alias_schema: Option<SchemaRef>,
    alias_provider: Option<Arc<DynamicStateTableProvider>>,
}

struct ComposedAsofJoinSpec {
    table_name: String,
    provider: Arc<DynamicStateTableProvider>,
    output_schema: SchemaRef,
    left: LogicalPlan,
    right: LogicalPlan,
    join_type: JoinType,
    on: Vec<(Expr, Expr)>,
    filter: Option<Expr>,
}

struct ComposedAsofJoinEvaluator {
    table_name: String,
    provider: Arc<DynamicStateTableProvider>,
    output_schema: SchemaRef,
    left_plan: Arc<dyn ExecutionPlan>,
    right_plan: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
    key_pairs: Vec<AsofKeyPair>,
    left_timestamp_idx: usize,
    right_timestamp_idx: usize,
}

struct AsofKeyPair {
    left_idx: usize,
    right_idx: usize,
}

struct AsofColumnRewrite {
    relation: Option<String>,
    name: String,
    table_name: String,
    replacement_name: String,
}

pub(super) fn columnar_composed_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarComposedPlan>> {
    let mut joins = Vec::new();
    collect_joins(plan, &mut joins);
    if joins.iter().any(|join| {
        !is_supported_join_type(&join.join_type) || (join.on.is_empty() && join.filter.is_none())
    }) {
        return Ok(None);
    }

    let source_names = source_set_for_plan(plan, sources);
    if source_names.is_empty() {
        return Ok(None);
    }

    Ok(Some(ColumnarComposedPlan {
        logical_plan: plan.clone(),
        source_names: source_names.into_iter().collect(),
    }))
}

pub(super) fn columnar_asof_join_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarComposedPlan>> {
    if !plan_contains_asof_extension(plan) {
        return Ok(None);
    }
    columnar_composed_plan_for_plan(plan, sources)
}

pub(super) fn columnar_self_join_aggregate_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarComposedPlan>> {
    if !contains_self_join_aggregate(plan, sources) {
        return Ok(None);
    }
    columnar_composed_plan_for_plan(plan, sources)
}

pub(super) fn columnar_distinct_aggregate_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarComposedPlan>> {
    if !contains_distinct_aggregate(plan) {
        return Ok(None);
    }
    columnar_composed_plan_for_plan(plan, sources)
}

pub(super) fn plan_contains_asof_extension(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Extension(extension) => extension
            .node
            .as_any()
            .downcast_ref::<FloeAsofJoinNode>()
            .is_some(),
        LogicalPlan::Projection(projection) => {
            plan_contains_asof_extension(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => plan_contains_asof_extension(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => plan_contains_asof_extension(alias.input.as_ref()),
        LogicalPlan::Subquery(subquery) => plan_contains_asof_extension(subquery.subquery.as_ref()),
        LogicalPlan::Aggregate(aggregate) => plan_contains_asof_extension(aggregate.input.as_ref()),
        LogicalPlan::Sort(sort) => plan_contains_asof_extension(sort.input.as_ref()),
        LogicalPlan::Limit(limit) => plan_contains_asof_extension(limit.input.as_ref()),
        LogicalPlan::Window(window) => plan_contains_asof_extension(window.input.as_ref()),
        LogicalPlan::Repartition(repartition) => {
            plan_contains_asof_extension(repartition.input.as_ref())
        }
        LogicalPlan::Distinct(distinct) => plan_contains_asof_extension(distinct.input()),
        LogicalPlan::Join(join) => {
            plan_contains_asof_extension(join.left.as_ref())
                || plan_contains_asof_extension(join.right.as_ref())
        }
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .any(|input| plan_contains_asof_extension(input.as_ref())),
        _ => false,
    }
}

fn contains_self_join_aggregate(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> bool {
    match plan {
        LogicalPlan::Aggregate(aggregate) => contains_self_join(aggregate.input.as_ref(), sources),
        LogicalPlan::Projection(projection) => {
            contains_self_join_aggregate(projection.input.as_ref(), sources)
        }
        LogicalPlan::Filter(filter) => contains_self_join_aggregate(filter.input.as_ref(), sources),
        LogicalPlan::SubqueryAlias(alias) => {
            contains_self_join_aggregate(alias.input.as_ref(), sources)
        }
        LogicalPlan::Sort(sort) => contains_self_join_aggregate(sort.input.as_ref(), sources),
        LogicalPlan::Limit(limit) => contains_self_join_aggregate(limit.input.as_ref(), sources),
        _ => false,
    }
}

fn contains_self_join(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> bool {
    match plan {
        LogicalPlan::Join(join) => {
            let left_sources = source_set_for_plan(join.left.as_ref(), sources);
            let right_sources = source_set_for_plan(join.right.as_ref(), sources);
            (!left_sources.is_empty() && left_sources.len() == 1 && left_sources == right_sources)
                || contains_self_join(join.left.as_ref(), sources)
                || contains_self_join(join.right.as_ref(), sources)
        }
        LogicalPlan::Projection(projection) => {
            contains_self_join(projection.input.as_ref(), sources)
        }
        LogicalPlan::Filter(filter) => contains_self_join(filter.input.as_ref(), sources),
        LogicalPlan::SubqueryAlias(alias) => contains_self_join(alias.input.as_ref(), sources),
        LogicalPlan::Subquery(subquery) => contains_self_join(subquery.subquery.as_ref(), sources),
        LogicalPlan::Aggregate(aggregate) => contains_self_join(aggregate.input.as_ref(), sources),
        LogicalPlan::Sort(sort) => contains_self_join(sort.input.as_ref(), sources),
        LogicalPlan::Limit(limit) => contains_self_join(limit.input.as_ref(), sources),
        LogicalPlan::Window(window) => contains_self_join(window.input.as_ref(), sources),
        LogicalPlan::Repartition(repartition) => {
            contains_self_join(repartition.input.as_ref(), sources)
        }
        LogicalPlan::Distinct(distinct) => contains_self_join(distinct.input(), sources),
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .any(|input| contains_self_join(input.as_ref(), sources)),
        _ => false,
    }
}

fn contains_distinct_aggregate(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(aggregate) => contains_distinct(aggregate.input.as_ref()),
        LogicalPlan::Projection(projection) => {
            contains_distinct_aggregate(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => contains_distinct_aggregate(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => contains_distinct_aggregate(alias.input.as_ref()),
        LogicalPlan::Sort(sort) => contains_distinct_aggregate(sort.input.as_ref()),
        LogicalPlan::Limit(limit) => contains_distinct_aggregate(limit.input.as_ref()),
        _ => false,
    }
}

fn contains_distinct(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Distinct(_) => true,
        LogicalPlan::Projection(projection) => contains_distinct(projection.input.as_ref()),
        LogicalPlan::Filter(filter) => contains_distinct(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => contains_distinct(alias.input.as_ref()),
        LogicalPlan::Subquery(subquery) => contains_distinct(subquery.subquery.as_ref()),
        LogicalPlan::Aggregate(aggregate) => contains_distinct(aggregate.input.as_ref()),
        LogicalPlan::Sort(sort) => contains_distinct(sort.input.as_ref()),
        LogicalPlan::Limit(limit) => contains_distinct(limit.input.as_ref()),
        LogicalPlan::Window(window) => contains_distinct(window.input.as_ref()),
        LogicalPlan::Repartition(repartition) => contains_distinct(repartition.input.as_ref()),
        LogicalPlan::Join(join) => {
            contains_distinct(join.left.as_ref()) || contains_distinct(join.right.as_ref())
        }
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .any(|input| contains_distinct(input.as_ref())),
        _ => false,
    }
}

pub(super) async fn build_columnar_composed_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarComposedPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarComposedMaterializedViewState> {
    build_columnar_snapshot_diff_materialized_view_state(
        table,
        view_name,
        output_schema,
        plan,
        sources,
        udfs,
        "composed",
        "composed",
        "columnar_composed_snapshot_diff",
    )
    .await
}

pub(super) async fn build_columnar_asof_join_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarComposedPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarComposedMaterializedViewState> {
    build_columnar_snapshot_diff_materialized_view_state(
        table,
        view_name,
        output_schema,
        plan,
        sources,
        udfs,
        "asof_join",
        "ASOF join",
        "columnar_asof_join_snapshot_diff",
    )
    .await
}

pub(super) async fn build_columnar_self_join_aggregate_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarComposedPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarComposedMaterializedViewState> {
    build_columnar_snapshot_diff_materialized_view_state(
        table,
        view_name,
        output_schema,
        plan,
        sources,
        udfs,
        "self_join_aggregate",
        "self-join aggregate",
        "columnar_self_join_aggregate_snapshot_diff",
    )
    .await
}

pub(super) async fn build_columnar_distinct_aggregate_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarComposedPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarComposedMaterializedViewState> {
    build_columnar_snapshot_diff_materialized_view_state(
        table,
        view_name,
        output_schema,
        plan,
        sources,
        udfs,
        "distinct_aggregate",
        "distinct aggregate",
        "columnar_distinct_aggregate_snapshot_diff",
    )
    .await
}

async fn build_columnar_snapshot_diff_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarComposedPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
    namespace_segment: &'static str,
    operator_label: &'static str,
    log_mode: &'static str,
) -> Result<ColumnarComposedMaterializedViewState> {
    let mv_namespace = namespaces::materialized_view(view_name)?;
    let output_namespace = format!("{mv_namespace}/columnar/{namespace_segment}/output");
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .with_context(|| format!("initialize SlateDB-backed {operator_label} output zset"))?;
    let initial_snapshot = snapshot_batches_from_zset(
        &output_zset
            .materialize_columnar()
            .await
            .context("load composed output snapshot")?,
    )?;

    let mut source_states = Vec::new();
    for source_name in &plan.source_names {
        let source = sources
            .get(source_name)
            .ok_or_else(|| anyhow::anyhow!("unknown composed source '{source_name}'"))?;
        let input_namespace =
            format!("{mv_namespace}/columnar/{namespace_segment}/{source_name}/input");
        let input_zset = SlateBackedColumnarZSet::new(
            Arc::clone(&table),
            input_namespace,
            Arc::clone(&source.schema),
        )
        .await
        .with_context(|| {
            format!("initialize SlateDB-backed {operator_label} input zset for '{source_name}'")
        })?;
        let snapshot = snapshot_batches_from_zset(
            &input_zset
                .materialize_columnar()
                .await
                .with_context(|| format!("load composed input snapshot for '{source_name}'"))?,
        )?;
        source_states.push(ColumnarComposedSourceState {
            source_name: source_name.clone(),
            schema: Arc::clone(&source.schema),
            input_zset,
            snapshot,
        });
    }

    let evaluator = ComposedEvaluator::build(
        plan.logical_plan,
        sources,
        udfs,
        output_schema,
        &plan.source_names,
    )
    .await
    .context("build snapshot-diff composed evaluator")?;

    Ok(ColumnarComposedMaterializedViewState {
        sources: source_states,
        output_zset,
        evaluator,
        initial_snapshot,
        operator_label,
        log_mode,
    })
}

pub(super) async fn run_columnar_composed_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    run_columnar_snapshot_diff_materialized_view_tick(
        registry,
        insert_batches,
        weighted_delta_batches,
        mv,
        version,
        ColumnarSnapshotDiffSlot::Composed,
    )
    .await
}

pub(super) async fn run_columnar_asof_join_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    run_columnar_snapshot_diff_materialized_view_tick(
        registry,
        insert_batches,
        weighted_delta_batches,
        mv,
        version,
        ColumnarSnapshotDiffSlot::AsofJoin,
    )
    .await
}

pub(super) async fn run_columnar_self_join_aggregate_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    run_columnar_snapshot_diff_materialized_view_tick(
        registry,
        insert_batches,
        weighted_delta_batches,
        mv,
        version,
        ColumnarSnapshotDiffSlot::SelfJoinAggregate,
    )
    .await
}

pub(super) async fn run_columnar_distinct_aggregate_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    run_columnar_snapshot_diff_materialized_view_tick(
        registry,
        insert_batches,
        weighted_delta_batches,
        mv,
        version,
        ColumnarSnapshotDiffSlot::DistinctAggregate,
    )
    .await
}

#[derive(Clone, Copy)]
enum ColumnarSnapshotDiffSlot {
    Composed,
    AsofJoin,
    SelfJoinAggregate,
    DistinctAggregate,
}

async fn run_columnar_snapshot_diff_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
    slot: ColumnarSnapshotDiffSlot,
) -> Result<bool> {
    let Some(columnar) = (match slot {
        ColumnarSnapshotDiffSlot::Composed => mv.columnar_composed.as_mut(),
        ColumnarSnapshotDiffSlot::AsofJoin => mv.columnar_asof_join.as_mut(),
        ColumnarSnapshotDiffSlot::SelfJoinAggregate => mv.columnar_self_join_aggregate.as_mut(),
        ColumnarSnapshotDiffSlot::DistinctAggregate => mv.columnar_distinct_aggregate.as_mut(),
    }) else {
        return Ok(false);
    };
    let plan_start = Instant::now();

    let mut persisted_deltas = HashMap::new();
    let mut has_input_change = false;
    for source in &mut columnar.sources {
        let input_delta = source_input_delta(source, insert_batches, weighted_delta_batches)?;
        let delta = persisted_source_delta(&mut source.input_zset, input_delta).await?;
        has_input_change |= !delta.batches().is_empty();
        persisted_deltas.insert(source.source_name.clone(), delta);
    }

    let mut next_source_snapshots = HashMap::new();
    for source in &columnar.sources {
        let delta = persisted_deltas.get(&source.source_name).ok_or_else(|| {
            anyhow::anyhow!("missing composed delta for '{}'", source.source_name)
        })?;
        let snapshot = if delta.batches().is_empty() {
            source.snapshot.clone()
        } else {
            materialize_source_snapshot(source).await?
        };
        next_source_snapshots.insert(source.source_name.clone(), snapshot);
    }

    let output_delta_batches = if has_input_change {
        let next_output = columnar
            .evaluator
            .evaluate(&next_source_snapshots)
            .await
            .context("evaluate next snapshot-diff composed output")?;
        diff_snapshot_batches(
            Arc::clone(&mv.output_schema),
            &mv.previous_snapshot,
            &next_output,
        )
        .await
        .context("diff snapshot-diff composed output")?
        .batches
    } else {
        Vec::new()
    };

    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build snapshot-diff composed output zset delta")?;
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
            "apply Slate-backed snapshot-diff {} columnar snapshot delta for '{}'",
            columnar.operator_label, mv.view_name
        )
    })?;
    for source in &mut columnar.sources {
        source.snapshot = next_source_snapshots
            .remove(&source.source_name)
            .ok_or_else(|| anyhow::anyhow!("missing next snapshot for '{}'", source.source_name))?;
    }

    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = columnar.log_mode,
        operator = columnar.operator_label,
        "SlateDB-backed snapshot-diff columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

fn source_input_delta(
    source: &ColumnarComposedSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    if let Some(weighted_batches) = weighted_delta_batches.get(source.source_name.as_str()) {
        ColumnarZSet::try_new_weighted(Arc::clone(&source.schema), weighted_batches.clone())
            .with_context(|| {
                format!(
                    "build weighted composed input delta for '{}'",
                    source.source_name
                )
            })
    } else if let Some(source_batches) = insert_batches.get(source.source_name.as_str()) {
        ColumnarZSet::from_value_batches(Arc::clone(&source.schema), source_batches.clone(), 1)
            .with_context(|| {
                format!(
                    "build insert composed input delta for '{}'",
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

async fn materialize_source_snapshot(
    source: &ColumnarComposedSourceState,
) -> Result<Vec<RecordBatch>> {
    snapshot_batches_from_zset(
        &source
            .input_zset
            .materialize_columnar()
            .await
            .with_context(|| {
                format!(
                    "materialize composed input zset for '{}'",
                    source.source_name
                )
            })?,
    )
}

impl ComposedEvaluator {
    async fn build(
        logical_plan: LogicalPlan,
        sources: &HashMap<String, VectorizedSourceState>,
        udfs: &[ScalarUDF],
        output_schema: &SchemaRef,
        source_names: &[String],
    ) -> Result<Self> {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        for udf in udfs.iter().cloned() {
            ctx.register_udf(udf);
        }
        let mut inputs = HashMap::new();
        for source_name in source_names {
            inputs.insert(
                source_name.clone(),
                ComposedEvaluatorInput::new(source_name, sources)?,
            );
        }
        let mut asof_specs = Vec::new();
        let logical_plan =
            rebind_composed_logical_plan(logical_plan, sources, &inputs, &mut asof_specs)?;
        let mut asof_joins = Vec::with_capacity(asof_specs.len());
        for spec in asof_specs {
            asof_joins.push(
                ComposedAsofJoinEvaluator::build(&ctx, spec)
                    .await
                    .context("build composed ASOF evaluator")?,
            );
        }
        let plan = ctx.state().create_physical_plan(&logical_plan).await?;
        Ok(Self {
            ctx,
            plan,
            inputs,
            asof_joins,
            output_schema: Arc::clone(output_schema),
        })
    }

    async fn evaluate(
        &self,
        source_snapshots: &HashMap<String, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>> {
        for (source_name, input) in &self.inputs {
            let batches = source_snapshots
                .get(source_name)
                .ok_or_else(|| anyhow::anyhow!("missing composed input '{source_name}'"))?;
            input
                .set_batches(source_name, batches)
                .with_context(|| format!("set composed evaluator input for '{source_name}'"))?;
        }
        for asof_join in &self.asof_joins {
            asof_join.evaluate(&self.ctx).await.with_context(|| {
                format!("evaluate composed ASOF input {}", asof_join.table_name)
            })?;
        }
        let collected = collect(Arc::clone(&self.plan), self.ctx.task_ctx()).await;
        self.clear_inputs()?;
        normalize_batches(
            collected.context("execute vectorized composed evaluator")?,
            &self.output_schema,
        )
    }

    fn clear_inputs(&self) -> Result<()> {
        for input in self.inputs.values() {
            input.clear()?;
        }
        for asof_join in &self.asof_joins {
            asof_join.clear()?;
        }
        Ok(())
    }
}

impl ComposedEvaluatorInput {
    fn new(source_name: &str, sources: &HashMap<String, VectorizedSourceState>) -> Result<Self> {
        let source = sources
            .get(source_name)
            .ok_or_else(|| anyhow::anyhow!("unknown composed source '{source_name}'"))?;
        let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&source.schema)));
        let (alias_schema, alias_provider) = if let (Some(_alias), Some(alias_schema)) = (
            source_name.strip_prefix("nexmark_"),
            source.alias_schema.as_ref(),
        ) {
            let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(alias_schema)));
            (Some(Arc::clone(alias_schema)), Some(provider))
        } else {
            (None, None)
        };
        Ok(Self {
            provider,
            alias_schema,
            alias_provider,
        })
    }

    fn provider_for_table(
        &self,
        source_name: &str,
        table_name: &str,
    ) -> Option<Arc<dyn TableProvider>> {
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

    fn set_batches(&self, source_name: &str, batches: &[RecordBatch]) -> Result<()> {
        self.provider.set_batches(batches.to_vec())?;
        if let (Some(alias_schema), Some(alias_provider)) =
            (self.alias_schema.as_ref(), self.alias_provider.as_ref())
        {
            alias_provider.set_batches(rename_batches(batches, alias_schema)?)?;
        }
        if self.provider_for_table(source_name, source_name).is_none() {
            bail!("unknown composed evaluator source '{source_name}'");
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

impl ComposedAsofJoinEvaluator {
    async fn build(ctx: &SessionContext, spec: ComposedAsofJoinSpec) -> Result<Self> {
        let left_schema = spec.left.schema();
        let right_schema = spec.right.schema();
        let key_pairs = spec
            .on
            .iter()
            .map(|(left, right)| {
                Ok(AsofKeyPair {
                    left_idx: column_expr_index(left, left_schema)
                        .with_context(|| format!("analyze ASOF left key expression {left}"))?,
                    right_idx: column_expr_index(right, right_schema)
                        .with_context(|| format!("analyze ASOF right key expression {right}"))?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut key_pairs = key_pairs;
        let (filter_key_pairs, left_timestamp_idx, right_timestamp_idx) =
            analyze_asof_filter(spec.filter.as_ref(), left_schema, right_schema)?;
        for pair in filter_key_pairs {
            push_asof_key_pair(&mut key_pairs, pair);
        }
        validate_asof_key_types(&spec.left, &spec.right, &key_pairs)?;
        validate_asof_timestamp_types(
            &spec.left,
            &spec.right,
            left_timestamp_idx,
            right_timestamp_idx,
        )?;

        let left_plan = ctx
            .state()
            .create_physical_plan(&spec.left)
            .await
            .context("create ASOF left physical plan")?;
        let right_plan = ctx
            .state()
            .create_physical_plan(&spec.right)
            .await
            .context("create ASOF right physical plan")?;

        Ok(Self {
            table_name: spec.table_name,
            provider: spec.provider,
            output_schema: spec.output_schema,
            left_plan,
            right_plan,
            join_type: spec.join_type,
            key_pairs,
            left_timestamp_idx,
            right_timestamp_idx,
        })
    }

    async fn evaluate(&self, ctx: &SessionContext) -> Result<()> {
        let left = collect(Arc::clone(&self.left_plan), ctx.task_ctx())
            .await
            .context("execute ASOF left input")?;
        let right = collect(Arc::clone(&self.right_plan), ctx.task_ctx())
            .await
            .context("execute ASOF right input")?;
        let left = concat_or_empty(self.left_plan.schema(), left)?;
        let right = concat_or_empty(self.right_plan.schema(), right)?;
        let output = evaluate_asof_join(
            &left,
            &right,
            &self.output_schema,
            self.join_type,
            &self.key_pairs,
            self.left_timestamp_idx,
            self.right_timestamp_idx,
        )?;
        self.provider.set_batches(vec![output])
    }

    fn clear(&self) -> Result<()> {
        self.provider.set_batches(Vec::new())
    }
}

fn rebind_composed_logical_plan(
    logical_plan: LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    inputs: &HashMap<String, ComposedEvaluatorInput>,
    asof_specs: &mut Vec<ComposedAsofJoinSpec>,
) -> Result<LogicalPlan> {
    let mut asof_rewrites = Vec::new();
    let transformed = logical_plan.transform_up(|plan| match plan {
        LogicalPlan::TableScan(mut scan) => {
            let table_name = scan.table_name.table();
            let Some(source_name) = table_scan_source(&scan, sources) else {
                return Err(datafusion::error::DataFusionError::Plan(format!(
                    "composed table scan '{table_name}' is not a known source"
                )));
            };
            let input = inputs.get(&source_name).ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "composed source '{source_name}' has no evaluator input"
                ))
            })?;
            let provider = input
                .provider_for_table(&source_name, table_name)
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "composed source '{source_name}' cannot provide table '{table_name}'"
                    ))
                })?;
            scan.source = provider_as_source(provider);
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        }
        LogicalPlan::Extension(extension) => {
            let Some(asof) = extension.node.as_any().downcast_ref::<FloeAsofJoinNode>() else {
                return Ok(Transformed::no(LogicalPlan::Extension(extension)));
            };
            let table_name = format!("__floe_composed_asof_{}", asof_specs.len());
            let (output_schema, rewrites) =
                asof_provider_schema_and_rewrites(table_name.as_str(), asof.schema());
            let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&output_schema)));
            let table_provider = Arc::clone(&provider) as Arc<dyn TableProvider>;
            let scan = LogicalPlanBuilder::scan(
                table_name.as_str(),
                provider_as_source(table_provider),
                None,
            )?
            .build()?;
            asof_specs.push(ComposedAsofJoinSpec {
                table_name: table_name.clone(),
                provider,
                output_schema,
                left: asof.left().clone(),
                right: asof.right().clone(),
                join_type: asof.join_type(),
                on: asof.on().to_vec(),
                filter: asof.filter().cloned(),
            });
            asof_rewrites.extend(rewrites);
            Ok(Transformed::yes(scan))
        }
        other => Ok(Transformed::no(other)),
    })?;
    rewrite_asof_column_references(transformed.data, &asof_rewrites)
}

fn asof_provider_schema_and_rewrites(
    table_name: &str,
    schema: &DFSchemaRef,
) -> (SchemaRef, Vec<AsofColumnRewrite>) {
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut rewrites = Vec::with_capacity(schema.fields().len());
    let mut name_counts: HashMap<String, usize> = HashMap::new();
    for (idx, (relation, field)) in schema.iter().enumerate() {
        let count = name_counts.entry(field.name().to_string()).or_insert(0);
        let replacement_name = if *count == 0 {
            field.name().to_string()
        } else {
            format!("{}__floe_asof_{idx}", field.name())
        };
        *count += 1;
        fields.push(Field::new(
            replacement_name.clone(),
            field.data_type().clone(),
            field.is_nullable(),
        ));
        rewrites.push(AsofColumnRewrite {
            relation: relation.map(ToString::to_string),
            name: field.name().to_string(),
            table_name: table_name.to_string(),
            replacement_name,
        });
    }
    (Arc::new(Schema::new(fields)), rewrites)
}

fn rewrite_asof_column_references(
    logical_plan: LogicalPlan,
    rewrites: &[AsofColumnRewrite],
) -> Result<LogicalPlan> {
    if rewrites.is_empty() {
        return Ok(logical_plan);
    }
    let transformed = logical_plan
        .transform_up(|plan| plan.map_expressions(|expr| rewrite_asof_expr(expr, rewrites)))?;
    Ok(transformed.data)
}

fn rewrite_asof_expr(
    expr: Expr,
    rewrites: &[AsofColumnRewrite],
) -> datafusion::common::Result<Transformed<Expr>> {
    let transformed = expr.transform(|expr| {
        if let Expr::Column(column) = &expr
            && let Some(rewrite) = asof_column_rewrite(column, rewrites)
        {
            return Ok(Transformed::yes(Expr::Column(Column::new(
                Some(rewrite.table_name.as_str()),
                rewrite.replacement_name.clone(),
            ))));
        }
        Ok(Transformed::no(expr))
    })?;
    Ok(Transformed::yes(transformed.data))
}

fn asof_column_rewrite<'a>(
    column: &Column,
    rewrites: &'a [AsofColumnRewrite],
) -> Option<&'a AsofColumnRewrite> {
    let relation = column.relation.as_ref().map(ToString::to_string);
    if let Some(rewrite) = rewrites
        .iter()
        .find(|rewrite| rewrite.relation == relation && rewrite.name == column.name)
    {
        return Some(rewrite);
    }
    if column.relation.is_none() {
        let mut matches = rewrites
            .iter()
            .filter(|rewrite| rewrite.name == column.name);
        let first = matches.next()?;
        if matches.next().is_none() {
            return Some(first);
        }
    }
    None
}

fn concat_or_empty(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<RecordBatch> {
    let non_empty = batches
        .iter()
        .filter(|batch| batch.num_rows() > 0)
        .collect::<Vec<_>>();
    if non_empty.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    concat_batches(&schema, non_empty).context("concatenate ASOF input batches")
}

fn evaluate_asof_join(
    left: &RecordBatch,
    right: &RecordBatch,
    output_schema: &SchemaRef,
    join_type: JoinType,
    key_pairs: &[AsofKeyPair],
    left_timestamp_idx: usize,
    right_timestamp_idx: usize,
) -> Result<RecordBatch> {
    if !matches!(join_type, JoinType::Inner | JoinType::Left) {
        bail!("composed ASOF evaluator supports INNER and LEFT ASOF joins only");
    }
    let left_width = left.num_columns();
    let right_width = right.num_columns();
    if output_schema.fields().len() != left_width + right_width {
        bail!(
            "ASOF output schema width {} does not match left/right widths {} + {}",
            output_schema.fields().len(),
            left_width,
            right_width
        );
    }

    let left_key_indices = key_pairs
        .iter()
        .map(|pair| pair.left_idx)
        .collect::<Vec<_>>();
    let right_key_indices = key_pairs
        .iter()
        .map(|pair| pair.right_idx)
        .collect::<Vec<_>>();
    let (left_keys, left_key_nulls) = encoded_join_keys(left, &left_key_indices)?;
    let (right_keys, right_key_nulls) = encoded_join_keys(right, &right_key_indices)?;
    let left_times = timestamp_values(left.column(left_timestamp_idx).as_ref())?;
    let right_times = timestamp_values(right.column(right_timestamp_idx).as_ref())?;
    let mut right_index: HashMap<Vec<u8>, Vec<(i64, usize)>> = HashMap::new();
    for row_idx in 0..right.num_rows() {
        if right_key_nulls[row_idx] {
            continue;
        }
        let Some(timestamp) = right_times[row_idx] else {
            continue;
        };
        right_index
            .entry(right_keys[row_idx].clone())
            .or_default()
            .push((timestamp, row_idx));
    }
    for rows in right_index.values_mut() {
        rows.sort_by(|(left_ts, left_row), (right_ts, right_row)| {
            left_ts.cmp(right_ts).then_with(|| left_row.cmp(right_row))
        });
    }

    let mut builders = output_schema
        .fields()
        .iter()
        .map(|field| ScalarColumnBuilder::new(field.data_type(), left.num_rows()))
        .collect::<Result<Vec<_>>>()?;
    for left_row in 0..left.num_rows() {
        let right_match = if left_key_nulls[left_row] {
            None
        } else if let Some(left_timestamp) = left_times[left_row] {
            right_index
                .get(&left_keys[left_row])
                .and_then(|candidates| asof_right_match(candidates, left_timestamp))
        } else {
            None
        };
        if right_match.is_none() && join_type == JoinType::Inner {
            continue;
        }
        append_asof_output_row(
            left,
            right,
            left_row,
            right_match,
            left_width,
            &mut builders,
        )?;
    }

    let arrays = builders
        .iter_mut()
        .map(ScalarColumnBuilder::finish_array)
        .collect::<Vec<_>>();
    Ok(RecordBatch::try_new(Arc::clone(output_schema), arrays)?)
}

fn asof_right_match(candidates: &[(i64, usize)], left_timestamp: i64) -> Option<usize> {
    let idx = candidates.partition_point(|(right_timestamp, _)| *right_timestamp <= left_timestamp);
    idx.checked_sub(1).map(|idx| candidates[idx].1)
}

fn append_asof_output_row(
    left: &RecordBatch,
    right: &RecordBatch,
    left_row: usize,
    right_row: Option<usize>,
    left_width: usize,
    builders: &mut [ScalarColumnBuilder],
) -> Result<()> {
    for column_idx in 0..left_width {
        builders[column_idx].append_array_value(left.column(column_idx).as_ref(), left_row)?;
    }
    for column_idx in 0..right.num_columns() {
        let builder = &mut builders[left_width + column_idx];
        if let Some(right_row) = right_row {
            builder.append_array_value(right.column(column_idx).as_ref(), right_row)?;
        } else {
            builder.append_encoded_scalar(None)?;
        }
    }
    Ok(())
}

fn encoded_join_keys(batch: &RecordBatch, indices: &[usize]) -> Result<(Vec<Vec<u8>>, Vec<bool>)> {
    if indices.is_empty() {
        return Ok((
            vec![Vec::new(); batch.num_rows()],
            vec![false; batch.num_rows()],
        ));
    }
    let sort_fields = indices
        .iter()
        .map(|idx| SortField::new(batch.schema().field(*idx).data_type().clone()))
        .collect::<Vec<_>>();
    let converter = RowConverter::new(sort_fields).map_err(anyhow::Error::new)?;
    let columns = indices
        .iter()
        .map(|idx| Arc::clone(batch.column(*idx)))
        .collect::<Vec<_>>();
    let rows = converter
        .convert_columns(&columns)
        .map_err(anyhow::Error::new)?;
    let mut keys = Vec::with_capacity(batch.num_rows());
    let mut nulls = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        keys.push(rows.row(row_idx).data().to_vec());
        nulls.push(
            indices
                .iter()
                .any(|idx| batch.column(*idx).is_null(row_idx)),
        );
    }
    Ok((keys, nulls))
}

fn timestamp_values(array: &dyn Array) -> Result<Vec<Option<i64>>> {
    match array.data_type() {
        DataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("expected Int64 ASOF timestamp array")?;
            Ok((0..values.len())
                .map(|idx| (!values.is_null(idx)).then(|| values.value(idx)))
                .collect())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .context("expected timestamp(ms) ASOF timestamp array")?;
            Ok((0..values.len())
                .map(|idx| (!values.is_null(idx)).then(|| values.value(idx)))
                .collect())
        }
        other => bail!("unsupported ASOF timestamp type {other:?}"),
    }
}

fn column_expr_index(expr: &Expr, schema: &DFSchemaRef) -> Result<usize> {
    let Expr::Column(column) = expr else {
        bail!("ASOF composed evaluator currently supports column expressions only, found {expr}");
    };
    schema.index_of_column(column).map_err(anyhow::Error::new)
}

fn maybe_column_expr_index(expr: &Expr, schema: &DFSchemaRef) -> Option<usize> {
    match expr {
        Expr::Column(column) => schema.maybe_index_of_column(column),
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum AsofColumnSide {
    Left(usize),
    Right(usize),
}

fn analyze_asof_filter(
    filter: Option<&Expr>,
    left_schema: &DFSchemaRef,
    right_schema: &DFSchemaRef,
) -> Result<(Vec<AsofKeyPair>, usize, usize)> {
    let filter = filter.context("ASOF joins require a MATCH_CONDITION filter")?;
    let mut key_pairs = Vec::new();
    let mut timestamp_pair = None;
    analyze_asof_filter_expr(
        filter,
        left_schema,
        right_schema,
        &mut key_pairs,
        &mut timestamp_pair,
    )?;
    let Some((left_timestamp_idx, right_timestamp_idx)) = timestamp_pair else {
        bail!("ASOF joins require exactly one right_timestamp <= left_timestamp predicate");
    };
    Ok((key_pairs, left_timestamp_idx, right_timestamp_idx))
}

fn analyze_asof_filter_expr(
    expr: &Expr,
    left_schema: &DFSchemaRef,
    right_schema: &DFSchemaRef,
    key_pairs: &mut Vec<AsofKeyPair>,
    timestamp_pair: &mut Option<(usize, usize)>,
) -> Result<()> {
    match expr {
        Expr::Alias(alias) => analyze_asof_filter_expr(
            alias.expr.as_ref(),
            left_schema,
            right_schema,
            key_pairs,
            timestamp_pair,
        ),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::And => {
            analyze_asof_filter_expr(left, left_schema, right_schema, key_pairs, timestamp_pair)?;
            analyze_asof_filter_expr(right, left_schema, right_schema, key_pairs, timestamp_pair)
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left_side = asof_column_side(left, left_schema, right_schema);
            let right_side = asof_column_side(right, left_schema, right_schema);
            match (op, left_side, right_side) {
                (
                    Operator::Eq,
                    Some(AsofColumnSide::Left(left_idx)),
                    Some(AsofColumnSide::Right(right_idx)),
                )
                | (
                    Operator::Eq,
                    Some(AsofColumnSide::Right(right_idx)),
                    Some(AsofColumnSide::Left(left_idx)),
                ) => {
                    push_asof_key_pair(
                        key_pairs,
                        AsofKeyPair {
                            left_idx,
                            right_idx,
                        },
                    );
                    Ok(())
                }
                (
                    Operator::LtEq,
                    Some(AsofColumnSide::Right(right_idx)),
                    Some(AsofColumnSide::Left(left_idx)),
                )
                | (
                    Operator::GtEq,
                    Some(AsofColumnSide::Left(left_idx)),
                    Some(AsofColumnSide::Right(right_idx)),
                ) => {
                    if timestamp_pair.replace((left_idx, right_idx)).is_some() {
                        bail!("ASOF joins require exactly one timestamp predicate");
                    }
                    Ok(())
                }
                _ => bail!("unsupported ASOF residual predicate {expr}"),
            }
        }
        _ => bail!("unsupported ASOF residual predicate {expr}"),
    }
}

fn asof_column_side(
    expr: &Expr,
    left_schema: &DFSchemaRef,
    right_schema: &DFSchemaRef,
) -> Option<AsofColumnSide> {
    let left_idx = maybe_column_expr_index(expr, left_schema);
    let right_idx = maybe_column_expr_index(expr, right_schema);
    match (left_idx, right_idx) {
        (Some(idx), None) => Some(AsofColumnSide::Left(idx)),
        (None, Some(idx)) => Some(AsofColumnSide::Right(idx)),
        _ => None,
    }
}

fn push_asof_key_pair(key_pairs: &mut Vec<AsofKeyPair>, pair: AsofKeyPair) {
    if !key_pairs
        .iter()
        .any(|existing| existing.left_idx == pair.left_idx && existing.right_idx == pair.right_idx)
    {
        key_pairs.push(pair);
    }
}

fn validate_asof_key_types(
    left: &LogicalPlan,
    right: &LogicalPlan,
    key_pairs: &[AsofKeyPair],
) -> Result<()> {
    let left_schema = left.schema().as_arrow();
    let right_schema = right.schema().as_arrow();
    for pair in key_pairs {
        let left_type = left_schema.field(pair.left_idx).data_type();
        let right_type = right_schema.field(pair.right_idx).data_type();
        if left_type != right_type {
            bail!("ASOF key type mismatch: left {left_type:?}, right {right_type:?}");
        }
    }
    Ok(())
}

fn validate_asof_timestamp_types(
    left: &LogicalPlan,
    right: &LogicalPlan,
    left_timestamp_idx: usize,
    right_timestamp_idx: usize,
) -> Result<()> {
    let left_type = left
        .schema()
        .as_arrow()
        .field(left_timestamp_idx)
        .data_type();
    let right_type = right
        .schema()
        .as_arrow()
        .field(right_timestamp_idx)
        .data_type();
    if left_type != right_type {
        bail!("ASOF timestamp type mismatch: left {left_type:?}, right {right_type:?}");
    }
    if !matches!(
        left_type,
        DataType::Int64 | DataType::Timestamp(TimeUnit::Millisecond, _)
    ) {
        bail!("ASOF timestamp type must be Int64 or timestamp(ms), found {left_type:?}");
    }
    Ok(())
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
        LogicalPlan::Subquery(subquery) => collect_joins(subquery.subquery.as_ref(), joins),
        LogicalPlan::Aggregate(aggregate) => collect_joins(aggregate.input.as_ref(), joins),
        LogicalPlan::Sort(sort) => collect_joins(sort.input.as_ref(), joins),
        LogicalPlan::Limit(limit) => collect_joins(limit.input.as_ref(), joins),
        LogicalPlan::Window(window) => collect_joins(window.input.as_ref(), joins),
        LogicalPlan::Repartition(repartition) => collect_joins(repartition.input.as_ref(), joins),
        LogicalPlan::Distinct(distinct) => collect_joins(distinct.input(), joins),
        LogicalPlan::Union(union) => {
            for input in &union.inputs {
                collect_joins(input.as_ref(), joins);
            }
        }
        LogicalPlan::Extension(extension) => {
            if let Some(asof) = extension.node.as_any().downcast_ref::<FloeAsofJoinNode>() {
                collect_joins(asof.left(), joins);
                collect_joins(asof.right(), joins);
            }
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
        LogicalPlan::Subquery(subquery) => {
            collect_sources(subquery.subquery.as_ref(), sources, out)
        }
        LogicalPlan::Aggregate(aggregate) => {
            collect_sources(aggregate.input.as_ref(), sources, out)
        }
        LogicalPlan::Sort(sort) => collect_sources(sort.input.as_ref(), sources, out),
        LogicalPlan::Limit(limit) => collect_sources(limit.input.as_ref(), sources, out),
        LogicalPlan::Window(window) => collect_sources(window.input.as_ref(), sources, out),
        LogicalPlan::Repartition(repartition) => {
            collect_sources(repartition.input.as_ref(), sources, out)
        }
        LogicalPlan::Distinct(distinct) => collect_sources(distinct.input(), sources, out),
        LogicalPlan::Join(join) => {
            collect_sources(join.left.as_ref(), sources, out);
            collect_sources(join.right.as_ref(), sources, out);
        }
        LogicalPlan::Union(union) => {
            for input in &union.inputs {
                collect_sources(input.as_ref(), sources, out);
            }
        }
        LogicalPlan::Extension(extension) => {
            if let Some(asof) = extension.node.as_any().downcast_ref::<FloeAsofJoinNode>() {
                collect_sources(asof.left(), sources, out);
                collect_sources(asof.right(), sources, out);
            }
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
