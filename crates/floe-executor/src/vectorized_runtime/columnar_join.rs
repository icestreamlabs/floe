use std::collections::{BTreeSet, HashMap};
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
use datafusion::logical_expr::{JoinType, LogicalPlan, ScalarUDF};
use datafusion::physical_plan::collect;
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarZSet};
use dbsp::storage::KeyValueTable;

use crate::delta_consolidation::{add_weight_column_to_batches, weighted_snapshot_schema};
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::table_provider::DynamicStateTableProvider;
use crate::vectorized_runtime::source_state::{rename_batches, resolve_source_table};
use crate::vectorized_source_delta::unit_source_delta_batches;

use super::{
    VectorizedMaterializedViewState, VectorizedSourceState, apply_weighted_snapshot_delta,
    normalize_batches,
};

pub(super) struct ColumnarJoinPlan {
    logical_plan: LogicalPlan,
    left_source: String,
    right_source: String,
}

pub(super) struct ColumnarJoinMaterializedViewState {
    left: ColumnarJoinSourceState,
    right: ColumnarJoinSourceState,
    output_zset: SlateBackedColumnarZSet,
    left_delta_right_state: JoinDeltaEvaluator,
    left_state_right_delta: JoinDeltaEvaluator,
    left_delta_right_delta: JoinDeltaEvaluator,
    initial_snapshot: Vec<RecordBatch>,
}

impl ColumnarJoinMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

struct ColumnarJoinSourceState {
    source_name: String,
    schema: SchemaRef,
    input_zset: SlateBackedColumnarZSet,
    snapshot: Vec<RecordBatch>,
}

struct JoinDeltaEvaluator {
    ctx: SessionContext,
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    inputs: HashMap<String, JoinEvaluatorInput>,
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
    if contains_unsupported_join_wrapper(plan) {
        return Ok(None);
    }

    Ok(Some(ColumnarJoinPlan {
        logical_plan: plan.clone(),
        left_source,
        right_source,
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
    let left_source = sources
        .get(&plan.left_source)
        .ok_or_else(|| anyhow::anyhow!("unknown join source '{}'", plan.left_source))?;
    let right_source = sources
        .get(&plan.right_source)
        .ok_or_else(|| anyhow::anyhow!("unknown join source '{}'", plan.right_source))?;
    let mv_namespace = namespaces::materialized_view(view_name)?;
    let left_namespace = format!("{mv_namespace}/columnar/join/{}/input", plan.left_source);
    let right_namespace = format!("{mv_namespace}/columnar/join/{}/input", plan.right_source);
    let output_namespace = format!("{mv_namespace}/columnar/join/output");

    let left_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        left_namespace,
        Arc::clone(&left_source.schema),
    )
    .await
    .context("initialize SlateDB-backed join left input zset")?;
    let right_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        right_namespace,
        Arc::clone(&right_source.schema),
    )
    .await
    .context("initialize SlateDB-backed join right input zset")?;
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

    let logical_plan = plan.logical_plan;
    let left_name = plan.left_source;
    let right_name = plan.right_source;
    let left_delta_right_state = JoinDeltaEvaluator::build(
        logical_plan.clone(),
        sources,
        udfs,
        output_schema,
        [&left_name, &right_name],
    )
    .await
    .context("build left-delta/right-state join evaluator")?;
    let left_state_right_delta = JoinDeltaEvaluator::build(
        logical_plan.clone(),
        sources,
        udfs,
        output_schema,
        [&left_name, &right_name],
    )
    .await
    .context("build left-state/right-delta join evaluator")?;
    let left_delta_right_delta = JoinDeltaEvaluator::build(
        logical_plan,
        sources,
        udfs,
        output_schema,
        [&left_name, &right_name],
    )
    .await
    .context("build left-delta/right-delta join evaluator")?;

    Ok(ColumnarJoinMaterializedViewState {
        left: ColumnarJoinSourceState {
            source_name: left_name,
            schema: Arc::clone(&left_source.schema),
            snapshot: snapshot_batches_from_zset(
                &left_zset
                    .materialize_columnar()
                    .await
                    .context("load join left input snapshot")?,
            )?,
            input_zset: left_zset,
        },
        right: ColumnarJoinSourceState {
            source_name: right_name,
            schema: Arc::clone(&right_source.schema),
            snapshot: snapshot_batches_from_zset(
                &right_zset
                    .materialize_columnar()
                    .await
                    .context("load join right input snapshot")?,
            )?,
            input_zset: right_zset,
        },
        output_zset,
        left_delta_right_state,
        left_state_right_delta,
        left_delta_right_delta,
        initial_snapshot,
    })
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
            &columnar.left.source_name,
            left_batches,
            &columnar.right.source_name,
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
    if let Some(weighted_batches) = weighted_delta_batches.get(source.source_name.as_str()) {
        ColumnarZSet::try_new_weighted(Arc::clone(&source.schema), weighted_batches.clone())
            .with_context(|| {
                format!(
                    "build weighted join input delta for '{}'",
                    source.source_name
                )
            })
    } else if let Some(source_batches) = insert_batches.get(source.source_name.as_str()) {
        ColumnarZSet::from_value_batches(Arc::clone(&source.schema), source_batches.clone(), 1)
            .with_context(|| format!("build insert join input delta for '{}'", source.source_name))
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
        source_names: [&str; 2],
    ) -> Result<Self> {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        for udf in udfs.iter().cloned() {
            ctx.register_udf(udf);
        }
        let mut inputs = HashMap::new();
        let mut provider_by_table = HashMap::new();
        for source_name in source_names {
            let source = sources
                .get(source_name)
                .ok_or_else(|| anyhow::anyhow!("unknown join source '{source_name}'"))?;
            let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&source.schema)));
            provider_by_table.insert(
                source_name.to_string(),
                Arc::clone(&provider) as Arc<dyn TableProvider>,
            );
            let (alias_schema, alias_provider) = if let (Some(alias), Some(alias_schema)) = (
                source_name.strip_prefix("nexmark_"),
                source.alias_schema.as_ref(),
            ) {
                let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(alias_schema)));
                provider_by_table.insert(
                    alias.to_string(),
                    Arc::clone(&provider) as Arc<dyn TableProvider>,
                );
                (Some(Arc::clone(alias_schema)), Some(provider))
            } else {
                (None, None)
            };
            inputs.insert(
                source_name.to_string(),
                JoinEvaluatorInput {
                    provider,
                    alias_schema,
                    alias_provider,
                },
            );
        }
        let logical_plan = rebind_join_logical_plan(logical_plan, &provider_by_table)?;
        let plan = ctx.state().create_physical_plan(&logical_plan).await?;
        Ok(Self {
            ctx,
            plan,
            inputs,
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
        self.set_input_batches(left_source, left_batches)?;
        self.set_input_batches(right_source, right_batches)?;
        let collected = collect(Arc::clone(&self.plan), self.ctx.task_ctx()).await;
        self.clear_inputs()?;
        normalize_batches(
            collected.context("execute vectorized join delta evaluator")?,
            &self.output_schema,
        )
    }

    fn set_input_batches(&self, source_name: &str, batches: &[RecordBatch]) -> Result<()> {
        let input = self
            .inputs
            .get(source_name)
            .ok_or_else(|| anyhow::anyhow!("unknown join evaluator source '{source_name}'"))?;
        input.provider.set_batches(batches.to_vec())?;
        if let (Some(alias_schema), Some(alias_provider)) =
            (input.alias_schema.as_ref(), input.alias_provider.as_ref())
        {
            alias_provider.set_batches(rename_batches(batches, alias_schema)?)?;
        }
        Ok(())
    }

    fn clear_inputs(&self) -> Result<()> {
        for input in self.inputs.values() {
            input.provider.set_batches(Vec::new())?;
            if let Some(alias_provider) = input.alias_provider.as_ref() {
                alias_provider.set_batches(Vec::new())?;
            }
        }
        Ok(())
    }
}

fn rebind_join_logical_plan(
    logical_plan: LogicalPlan,
    provider_by_table: &HashMap<String, Arc<dyn TableProvider>>,
) -> Result<LogicalPlan> {
    let transformed = logical_plan.transform_up(|plan| match plan {
        LogicalPlan::TableScan(mut scan) => {
            let table_name = scan.table_name.table();
            let Some(provider) = provider_by_table.get(table_name) else {
                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
            };
            scan.source = provider_as_source(Arc::clone(provider));
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        }
        other => Ok(Transformed::no(other)),
    })?;
    Ok(transformed.data)
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
        _ => {}
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

fn contains_unsupported_join_wrapper(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => {
            contains_unsupported_join_wrapper(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => contains_unsupported_join_wrapper(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => {
            contains_unsupported_join_wrapper(alias.input.as_ref())
        }
        LogicalPlan::Join(join) => {
            contains_unsupported_join_wrapper(join.left.as_ref())
                || contains_unsupported_join_wrapper(join.right.as_ref())
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
