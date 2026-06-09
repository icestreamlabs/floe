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
use datafusion::logical_expr::logical_plan::TableScan;
use datafusion::logical_expr::{LogicalPlan, ScalarUDF};
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

pub(super) struct ColumnarUnionPlan {
    logical_plan: LogicalPlan,
    source_names: Vec<String>,
}

pub(super) struct ColumnarUnionMaterializedViewState {
    sources: Vec<ColumnarUnionSourceState>,
    output_zset: SlateBackedColumnarZSet,
    evaluator: UnionDeltaEvaluator,
    initial_snapshot: Vec<RecordBatch>,
}

impl ColumnarUnionMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

struct ColumnarUnionSourceState {
    source_name: String,
    schema: SchemaRef,
    input_zset: SlateBackedColumnarZSet,
}

struct UnionDeltaEvaluator {
    ctx: SessionContext,
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    inputs: HashMap<String, UnionEvaluatorInput>,
    output_schema: SchemaRef,
}

struct UnionEvaluatorInput {
    provider: Arc<DynamicStateTableProvider>,
    alias_schema: Option<SchemaRef>,
    alias_provider: Option<Arc<DynamicStateTableProvider>>,
}

struct UnionSignedDelta {
    positive: Vec<RecordBatch>,
    negative: Vec<RecordBatch>,
}

pub(super) fn columnar_union_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarUnionPlan>> {
    if !contains_union(plan) || contains_unsupported_union_wrapper(plan) {
        return Ok(None);
    }
    let source_names = source_set_for_plan(plan, sources)
        .into_iter()
        .collect::<Vec<_>>();
    if source_names.is_empty() {
        return Ok(None);
    }

    Ok(Some(ColumnarUnionPlan {
        logical_plan: plan.clone(),
        source_names,
    }))
}

pub(super) async fn build_columnar_union_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarUnionPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarUnionMaterializedViewState> {
    let mv_namespace = namespaces::materialized_view(view_name)?;
    let output_namespace = format!("{mv_namespace}/columnar/union/output");
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed union output zset")?;
    let initial_snapshot = snapshot_batches_from_zset(
        &output_zset
            .materialize_columnar()
            .await
            .context("load union output snapshot")?,
    )?;

    let mut source_states = Vec::with_capacity(plan.source_names.len());
    for source_name in &plan.source_names {
        let source = sources
            .get(source_name)
            .ok_or_else(|| anyhow::anyhow!("unknown union source '{source_name}'"))?;
        let input_namespace = format!("{mv_namespace}/columnar/union/{source_name}/input");
        source_states.push(ColumnarUnionSourceState {
            source_name: source_name.clone(),
            schema: Arc::clone(&source.schema),
            input_zset: SlateBackedColumnarZSet::new(
                Arc::clone(&table),
                input_namespace,
                Arc::clone(&source.schema),
            )
            .await
            .with_context(|| {
                format!("initialize SlateDB-backed union input zset for '{source_name}'")
            })?,
        });
    }

    let evaluator = UnionDeltaEvaluator::build(
        plan.logical_plan,
        sources,
        udfs,
        output_schema,
        &plan.source_names,
    )
    .await
    .context("build union delta evaluator")?;

    Ok(ColumnarUnionMaterializedViewState {
        sources: source_states,
        output_zset,
        evaluator,
        initial_snapshot,
    })
}

pub(super) async fn run_columnar_union_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(columnar) = mv.columnar_union.as_mut() else {
        return Ok(false);
    };
    let plan_start = Instant::now();

    let mut positive_by_source = HashMap::new();
    let mut negative_by_source = HashMap::new();
    for source in &mut columnar.sources {
        let input_delta = source_input_delta(source, insert_batches, weighted_delta_batches)?;
        let persisted_delta = persisted_source_delta(&mut source.input_zset, input_delta).await?;
        let signed = signed_source_delta(&source.schema, persisted_delta.batches())
            .with_context(|| format!("split union source delta for '{}'", source.source_name))?;
        positive_by_source.insert(source.source_name.clone(), signed.positive);
        negative_by_source.insert(source.source_name.clone(), signed.negative);
    }

    let mut output_delta_batches = Vec::new();
    collect_union_outputs(columnar, &positive_by_source, &mut output_delta_batches, 1).await?;
    collect_union_outputs(columnar, &negative_by_source, &mut output_delta_batches, -1).await?;

    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build union output zset delta")?;
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
            "apply Slate-backed union columnar snapshot delta for '{}'",
            mv.view_name
        )
    })?;

    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "columnar_union",
        "SlateDB-backed union columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

async fn collect_union_outputs(
    columnar: &ColumnarUnionMaterializedViewState,
    source_batches: &HashMap<String, Vec<RecordBatch>>,
    output: &mut Vec<RecordBatch>,
    weight: i64,
) -> Result<()> {
    if source_batches
        .values()
        .all(|batches| batches.iter().all(|batch| batch.num_rows() == 0))
    {
        return Ok(());
    }
    let rows = columnar.evaluator.evaluate(source_batches).await?;
    let weighted_schema = weighted_snapshot_schema(&columnar.evaluator.output_schema)?;
    output.extend(add_weight_column_to_batches(
        &rows,
        &weighted_schema,
        weight,
    )?);
    Ok(())
}

fn source_input_delta(
    source: &ColumnarUnionSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    if let Some(weighted_batches) = weighted_delta_batches.get(source.source_name.as_str()) {
        ColumnarZSet::try_new_weighted(Arc::clone(&source.schema), weighted_batches.clone())
            .with_context(|| {
                format!(
                    "build weighted union input delta for '{}'",
                    source.source_name
                )
            })
    } else if let Some(source_batches) = insert_batches.get(source.source_name.as_str()) {
        ColumnarZSet::from_value_batches(Arc::clone(&source.schema), source_batches.clone(), 1)
            .with_context(|| {
                format!(
                    "build insert union input delta for '{}'",
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

fn signed_source_delta(
    schema: &SchemaRef,
    input_batches: &[RecordBatch],
) -> Result<UnionSignedDelta> {
    let mut positive = Vec::new();
    let mut negative = Vec::new();
    for batch in input_batches {
        let unit_delta = unit_source_delta_batches(schema, batch)?
            .context("union received non-unit source delta")?;
        positive.extend(unit_delta.positive);
        negative.extend(unit_delta.negative);
    }
    Ok(UnionSignedDelta { positive, negative })
}

impl UnionDeltaEvaluator {
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
        let mut provider_by_table = HashMap::new();
        for source_name in source_names {
            let source = sources
                .get(source_name)
                .ok_or_else(|| anyhow::anyhow!("unknown union source '{source_name}'"))?;
            let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&source.schema)));
            provider_by_table.insert(
                source_name.clone(),
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
                source_name.clone(),
                UnionEvaluatorInput {
                    provider,
                    alias_schema,
                    alias_provider,
                },
            );
        }

        let logical_plan = rebind_union_logical_plan(logical_plan, &provider_by_table)?;
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
        source_batches: &HashMap<String, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>> {
        self.set_input_batches(source_batches)?;
        let collected = collect(Arc::clone(&self.plan), self.ctx.task_ctx()).await;
        self.clear_inputs()?;
        normalize_batches(
            collected.context("execute vectorized union delta evaluator")?,
            &self.output_schema,
        )
    }

    fn set_input_batches(&self, source_batches: &HashMap<String, Vec<RecordBatch>>) -> Result<()> {
        for (source_name, input) in &self.inputs {
            let batches = source_batches
                .get(source_name)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            input.provider.set_batches(batches.to_vec())?;
            if let (Some(alias_schema), Some(alias_provider)) =
                (input.alias_schema.as_ref(), input.alias_provider.as_ref())
            {
                alias_provider.set_batches(rename_batches(batches, alias_schema)?)?;
            }
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

fn rebind_union_logical_plan(
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

fn contains_union(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Union(_) => true,
        LogicalPlan::Projection(projection) => contains_union(projection.input.as_ref()),
        LogicalPlan::Filter(filter) => contains_union(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => contains_union(alias.input.as_ref()),
        _ => false,
    }
}

fn contains_unsupported_union_wrapper(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => {
            contains_unsupported_union_wrapper(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => contains_unsupported_union_wrapper(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => {
            contains_unsupported_union_wrapper(alias.input.as_ref())
        }
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .any(|input| contains_unsupported_union_wrapper(input.as_ref())),
        LogicalPlan::TableScan(_) => false,
        _ => true,
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
        LogicalPlan::Union(union) => {
            for input in &union.inputs {
                collect_sources(input.as_ref(), sources, out);
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
