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

use crate::delta_consolidation::diff_snapshot_batches;
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::table_provider::DynamicStateTableProvider;
use crate::vectorized_runtime::source_state::{rename_batches, resolve_source_table};

use super::{
    VectorizedMaterializedViewState, VectorizedSourceState, apply_weighted_snapshot_delta,
    normalize_batches,
};

pub(super) struct ColumnarMultiJoinPlan {
    logical_plan: LogicalPlan,
    source_names: Vec<String>,
}

impl ColumnarMultiJoinPlan {
    pub(super) fn source_names(&self) -> BTreeSet<String> {
        self.source_names.iter().cloned().collect()
    }
}

pub(super) struct ColumnarMultiJoinMaterializedViewState {
    sources: Vec<ColumnarMultiJoinSourceState>,
    output_zset: SlateBackedColumnarZSet,
    evaluator: MultiJoinEvaluator,
    initial_snapshot: Vec<RecordBatch>,
}

impl ColumnarMultiJoinMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

struct ColumnarMultiJoinSourceState {
    source_name: String,
    schema: SchemaRef,
    input_zset: SlateBackedColumnarZSet,
    snapshot: Vec<RecordBatch>,
}

pub(super) struct ColumnarMultiJoinTick {
    pub(super) delta: ColumnarZSet,
    pub(super) next_snapshot: Vec<RecordBatch>,
    pub(super) input_changed: bool,
}

struct MultiJoinEvaluator {
    ctx: SessionContext,
    logical_plan: LogicalPlan,
    inputs: HashMap<String, MultiJoinEvaluatorInput>,
    output_schema: SchemaRef,
}

struct MultiJoinEvaluatorInput {
    provider: Arc<DynamicStateTableProvider>,
    alias_schema: Option<SchemaRef>,
    alias_provider: Option<Arc<DynamicStateTableProvider>>,
}

pub(super) fn columnar_multijoin_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarMultiJoinPlan>> {
    let mut joins = Vec::new();
    collect_joins(plan, &mut joins);
    if joins.len() < 2
        || joins.iter().any(|join| {
            !is_supported_join_type(&join.join_type)
                || (join.on.is_empty() && join.filter.is_none())
        })
    {
        return Ok(None);
    }
    if contains_unsupported_multijoin_wrapper(plan) {
        return Ok(None);
    }
    let source_names = source_set_for_plan(plan, sources);
    if source_names.len() < 2 {
        return Ok(None);
    }
    let mut scan_sources = Vec::new();
    collect_table_scan_sources(plan, sources, &mut scan_sources);
    if scan_sources.len() != source_names.len() {
        return Ok(None);
    }

    Ok(Some(ColumnarMultiJoinPlan {
        logical_plan: plan.clone(),
        source_names: source_names.into_iter().collect(),
    }))
}

pub(super) async fn build_columnar_multijoin_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarMultiJoinPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarMultiJoinMaterializedViewState> {
    let mv_namespace = namespaces::materialized_view(view_name)?;
    build_columnar_multijoin_materialized_view_state_in_namespace(
        table,
        mv_namespace,
        output_schema,
        plan,
        sources,
        udfs,
    )
    .await
}

pub(super) async fn build_columnar_multijoin_materialized_view_state_in_namespace(
    table: Arc<dyn KeyValueTable>,
    mv_namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarMultiJoinPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarMultiJoinMaterializedViewState> {
    let output_namespace = format!("{mv_namespace}/columnar/multijoin/output");
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed multijoin output zset")?;
    let initial_snapshot = snapshot_batches_from_zset(
        &output_zset
            .materialize_columnar()
            .await
            .context("load multijoin output snapshot")?,
    )?;

    let mut source_states = Vec::new();
    for source_name in &plan.source_names {
        let source = sources
            .get(source_name)
            .ok_or_else(|| anyhow::anyhow!("unknown multijoin source '{source_name}'"))?;
        let input_namespace = format!("{mv_namespace}/columnar/multijoin/{source_name}/input");
        let input_zset = SlateBackedColumnarZSet::new(
            Arc::clone(&table),
            input_namespace,
            Arc::clone(&source.schema),
        )
        .await
        .with_context(|| initialize_source_zset_context(source_name))?;
        let snapshot = snapshot_batches_from_zset(
            &input_zset
                .materialize_columnar()
                .await
                .with_context(|| format!("load multijoin input snapshot for '{source_name}'"))?,
        )?;
        source_states.push(ColumnarMultiJoinSourceState {
            source_name: source_name.clone(),
            schema: Arc::clone(&source.schema),
            input_zset,
            snapshot,
        });
    }

    let evaluator = MultiJoinEvaluator::build(
        plan.logical_plan,
        sources,
        udfs,
        output_schema,
        &plan.source_names,
    )
    .await
    .context("build snapshot-diff multijoin evaluator")?;

    Ok(ColumnarMultiJoinMaterializedViewState {
        sources: source_states,
        output_zset,
        evaluator,
        initial_snapshot,
    })
}

pub(super) async fn run_columnar_multijoin_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(columnar) = mv.columnar_multijoin.as_mut() else {
        return Ok(false);
    };
    let plan_start = Instant::now();
    let tick = run_columnar_multijoin_state_tick(
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
        mode = "columnar_multijoin_snapshot_diff",
        "SlateDB-backed snapshot-diff multijoin columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

pub(super) async fn run_columnar_multijoin_state_tick(
    columnar: &mut ColumnarMultiJoinMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    output_schema: &SchemaRef,
    previous_snapshot: &[RecordBatch],
) -> Result<ColumnarMultiJoinTick> {
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
            anyhow::anyhow!("missing multijoin delta for '{}'", source.source_name)
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
            .context("evaluate next snapshot-diff multijoin output")?;
        diff_snapshot_batches(Arc::clone(output_schema), previous_snapshot, &next_output)
            .await
            .context("diff snapshot-diff multijoin output")?
            .batches
    } else {
        Vec::new()
    };

    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build snapshot-diff multijoin output zset delta")?;
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
            .context("apply Slate-backed snapshot-diff multijoin columnar snapshot delta")?;
    for source in &mut columnar.sources {
        source.snapshot = next_source_snapshots
            .remove(&source.source_name)
            .ok_or_else(|| anyhow::anyhow!("missing next snapshot for '{}'", source.source_name))?;
    }

    Ok(ColumnarMultiJoinTick {
        delta: persisted_output_delta,
        next_snapshot,
        input_changed: has_input_change,
    })
}

fn initialize_source_zset_context(source_name: &str) -> String {
    format!("initialize SlateDB-backed multijoin input zset for '{source_name}'")
}

fn source_input_delta(
    source: &ColumnarMultiJoinSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    if let Some(weighted_batches) = weighted_delta_batches.get(source.source_name.as_str()) {
        ColumnarZSet::try_new_weighted(Arc::clone(&source.schema), weighted_batches.clone())
            .with_context(|| {
                format!(
                    "build weighted multijoin input delta for '{}'",
                    source.source_name
                )
            })
    } else if let Some(source_batches) = insert_batches.get(source.source_name.as_str()) {
        ColumnarZSet::from_value_batches(Arc::clone(&source.schema), source_batches.clone(), 1)
            .with_context(|| {
                format!(
                    "build insert multijoin input delta for '{}'",
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
    source: &ColumnarMultiJoinSourceState,
) -> Result<Vec<RecordBatch>> {
    snapshot_batches_from_zset(
        &source
            .input_zset
            .materialize_columnar()
            .await
            .with_context(|| {
                format!(
                    "materialize multijoin input zset for '{}'",
                    source.source_name
                )
            })?,
    )
}

impl MultiJoinEvaluator {
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
                MultiJoinEvaluatorInput::new(source_name, sources)?,
            );
        }
        let logical_plan = rebind_multijoin_logical_plan(logical_plan, sources, &inputs)?;
        ctx.state().create_physical_plan(&logical_plan).await?;
        Ok(Self {
            ctx,
            logical_plan,
            inputs,
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
                .ok_or_else(|| anyhow::anyhow!("missing multijoin input '{source_name}'"))?;
            input
                .set_batches(source_name, batches)
                .with_context(|| format!("set multijoin evaluator input for '{source_name}'"))?;
        }
        let plan = self
            .ctx
            .state()
            .create_physical_plan(&self.logical_plan)
            .await
            .context("rebuild vectorized multijoin physical plan")?;
        let collected = collect(plan, self.ctx.task_ctx()).await;
        self.clear_inputs()?;
        normalize_batches(
            collected.context("execute vectorized multijoin evaluator")?,
            &self.output_schema,
        )
    }

    fn clear_inputs(&self) -> Result<()> {
        for input in self.inputs.values() {
            input.clear()?;
        }
        Ok(())
    }
}

impl MultiJoinEvaluatorInput {
    fn new(source_name: &str, sources: &HashMap<String, VectorizedSourceState>) -> Result<Self> {
        let source = sources
            .get(source_name)
            .ok_or_else(|| anyhow::anyhow!("unknown multijoin source '{source_name}'"))?;
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
            bail!("unknown multijoin evaluator source '{source_name}'");
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

fn rebind_multijoin_logical_plan(
    logical_plan: LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    inputs: &HashMap<String, MultiJoinEvaluatorInput>,
) -> Result<LogicalPlan> {
    let transformed = logical_plan.transform_up(|plan| match plan {
        LogicalPlan::TableScan(mut scan) => {
            let table_name = scan.table_name.table();
            let Some(source_name) = table_scan_source(&scan, sources) else {
                return Err(datafusion::error::DataFusionError::Plan(format!(
                    "multijoin table scan '{table_name}' is not a known source"
                )));
            };
            let input = inputs.get(&source_name).ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "multijoin source '{source_name}' has no evaluator input"
                ))
            })?;
            let provider = input
                .provider_for_table(&source_name, table_name)
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "multijoin source '{source_name}' cannot provide table '{table_name}'"
                    ))
                })?;
            scan.source = provider_as_source(provider);
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
        LogicalPlan::Limit(limit) => collect_joins(limit.input.as_ref(), joins),
        LogicalPlan::Sort(sort) => collect_joins(sort.input.as_ref(), joins),
        _ => {}
    }
}

fn collect_table_scan_sources(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    out: &mut Vec<String>,
) {
    match plan {
        LogicalPlan::TableScan(scan) => {
            if let Some(source_name) = table_scan_source(scan, sources) {
                out.push(source_name);
            }
        }
        LogicalPlan::Projection(projection) => {
            collect_table_scan_sources(projection.input.as_ref(), sources, out)
        }
        LogicalPlan::Filter(filter) => {
            collect_table_scan_sources(filter.input.as_ref(), sources, out)
        }
        LogicalPlan::SubqueryAlias(alias) => {
            collect_table_scan_sources(alias.input.as_ref(), sources, out)
        }
        LogicalPlan::Limit(limit) => collect_table_scan_sources(limit.input.as_ref(), sources, out),
        LogicalPlan::Sort(sort) => collect_table_scan_sources(sort.input.as_ref(), sources, out),
        LogicalPlan::Join(join) => {
            collect_table_scan_sources(join.left.as_ref(), sources, out);
            collect_table_scan_sources(join.right.as_ref(), sources, out);
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
        LogicalPlan::Limit(limit) => collect_sources(limit.input.as_ref(), sources, out),
        LogicalPlan::Sort(sort) => collect_sources(sort.input.as_ref(), sources, out),
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

fn contains_unsupported_multijoin_wrapper(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => {
            contains_unsupported_multijoin_wrapper(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => {
            contains_unsupported_multijoin_wrapper(filter.input.as_ref())
        }
        LogicalPlan::SubqueryAlias(alias) => {
            contains_unsupported_multijoin_wrapper(alias.input.as_ref())
        }
        LogicalPlan::Limit(limit) => {
            limit.fetch.is_none() || contains_unsupported_multijoin_wrapper(limit.input.as_ref())
        }
        LogicalPlan::Sort(sort) => {
            sort.expr.is_empty() || contains_unsupported_multijoin_wrapper(sort.input.as_ref())
        }
        LogicalPlan::Join(join) => {
            contains_unsupported_multijoin_wrapper(join.left.as_ref())
                || contains_unsupported_multijoin_wrapper(join.right.as_ref())
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
