use std::collections::{BTreeSet, HashMap, hash_map::Entry};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::logical_expr::logical_plan::{Distinct, TableScan};
use datafusion::logical_expr::{LogicalPlan, ScalarUDF};
use datafusion::physical_plan::collect;
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarZSet};
use dbsp::storage::{KeyValueTable, keyspace};
use slatedb::WriteBatch;

use crate::delta_consolidation::{add_weight_column_to_batches, weighted_snapshot_schema};
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::scalar_array_builder::ScalarColumnBuilder;
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
    distinct: bool,
}

pub(super) struct ColumnarUnionMaterializedViewState {
    sources: Vec<ColumnarUnionSourceState>,
    output_zset: SlateBackedColumnarZSet,
    evaluator: UnionDeltaEvaluator,
    distinct_state: Option<SlateUnionDistinctState>,
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

struct SlateUnionDistinctState {
    table: Arc<dyn KeyValueTable>,
    key_prefix: Vec<u8>,
}

struct PendingDistinctDelta {
    delta: i64,
    batch: RecordBatch,
    row_idx: usize,
}

pub(super) fn columnar_union_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarUnionPlan>> {
    let Some((logical_plan, distinct)) = union_execution_plan(plan) else {
        return Ok(None);
    };
    if contains_unsupported_union_wrapper(&logical_plan) {
        return Ok(None);
    }
    let source_names = source_set_for_plan(&logical_plan, sources)
        .into_iter()
        .collect::<Vec<_>>();
    if source_names.is_empty() {
        return Ok(None);
    }

    Ok(Some(ColumnarUnionPlan {
        logical_plan,
        source_names,
        distinct,
    }))
}

fn union_execution_plan(plan: &LogicalPlan) -> Option<(LogicalPlan, bool)> {
    match plan {
        LogicalPlan::Distinct(Distinct::All(input)) if contains_union(input.as_ref()) => {
            Some((input.as_ref().clone(), true))
        }
        LogicalPlan::SubqueryAlias(alias) => union_execution_plan(alias.input.as_ref()),
        _ if contains_union(plan) => Some((plan.clone(), false)),
        _ => None,
    }
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
    let distinct_state_namespace = format!("{mv_namespace}/columnar/union/distinct_state");
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
        distinct_state: plan
            .distinct
            .then(|| SlateUnionDistinctState::new(table, &distinct_state_namespace)),
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
    let output_delta_batches = union_output_delta_batches(columnar, output_delta_batches).await?;

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

async fn union_output_delta_batches(
    columnar: &ColumnarUnionMaterializedViewState,
    raw_delta_batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>> {
    if columnar.distinct_state.is_none() || raw_delta_batches.is_empty() {
        return Ok(raw_delta_batches);
    }
    let pending =
        union_distinct_pending_delta(&columnar.evaluator.output_schema, &raw_delta_batches)?;
    apply_union_distinct_delta(columnar, pending).await
}

fn union_distinct_pending_delta(
    output_schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<HashMap<Vec<u8>, PendingDistinctDelta>> {
    let mut pending = HashMap::new();
    if batches.is_empty() {
        return Ok(pending);
    }
    let converter = row_converter_for_schema(output_schema)?;
    let value_column_count = output_schema.fields().len();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let weight_idx = batch.schema().index_of(WEIGHT_COLUMN_NAME)?;
        let value_columns = batch
            .columns()
            .iter()
            .take(value_column_count)
            .cloned()
            .collect::<Vec<_>>();
        let rows = converter
            .convert_columns(&value_columns)
            .context("encode union distinct output row keys")?;
        let weights = batch
            .column(weight_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("union output weight column must be Int64"))?;
        for row_idx in 0..batch.num_rows() {
            if weights.is_null(row_idx) {
                bail!("union output weight cannot be NULL");
            }
            let delta = weights.value(row_idx);
            if delta == 0 {
                continue;
            }
            let key = rows.row(row_idx).data().to_vec();
            match pending.entry(key) {
                Entry::Occupied(mut entry) => {
                    let current = entry.get().delta;
                    entry.get_mut().delta = current
                        .checked_add(delta)
                        .ok_or_else(|| anyhow::anyhow!("union distinct pending delta overflow"))?;
                }
                Entry::Vacant(entry) => {
                    entry.insert(PendingDistinctDelta {
                        delta,
                        batch: batch.clone(),
                        row_idx,
                    });
                }
            }
        }
    }
    pending.retain(|_, delta| delta.delta != 0);
    Ok(pending)
}

async fn apply_union_distinct_delta(
    columnar: &ColumnarUnionMaterializedViewState,
    pending: HashMap<Vec<u8>, PendingDistinctDelta>,
) -> Result<Vec<RecordBatch>> {
    let mut builder = UnionDistinctOutputBuilder::new(&columnar.evaluator.output_schema)?;
    if pending.is_empty() {
        return builder.finish();
    }
    let distinct_state = columnar
        .distinct_state
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("union distinct state is not initialized"))?;
    let mut writes = WriteBatch::new();
    let mut wrote_state = false;
    for (row_key, delta) in pending {
        let old_count = distinct_state.load_count(&row_key).await?;
        let new_count = old_count
            .checked_add(delta.delta)
            .ok_or_else(|| anyhow::anyhow!("union distinct state overflow"))?;
        if new_count < 0 {
            bail!("union distinct state removed more rows than were present");
        }
        if old_count > 0 && new_count == 0 {
            builder.append(&delta.batch, delta.row_idx, -1)?;
        }
        if old_count == 0 && new_count > 0 {
            builder.append(&delta.batch, delta.row_idx, 1)?;
        }
        distinct_state.write_count(&mut writes, &row_key, new_count);
        wrote_state = true;
    }
    if wrote_state {
        distinct_state
            .table
            .write_batch(writes)
            .await
            .context("persist union distinct state updates")?;
    }
    builder.finish()
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

struct UnionDistinctOutputBuilder {
    weighted_schema: SchemaRef,
    builders: Vec<ScalarColumnBuilder>,
    weights: datafusion::arrow::array::Int64Builder,
    rows: usize,
}

impl UnionDistinctOutputBuilder {
    fn new(schema: &SchemaRef) -> Result<Self> {
        let builders = schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), 1024))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            weighted_schema: weighted_snapshot_schema(schema)?,
            builders,
            weights: datafusion::arrow::array::Int64Builder::with_capacity(1024),
            rows: 0,
        })
    }

    fn append(&mut self, batch: &RecordBatch, row_idx: usize, weight: i64) -> Result<()> {
        for (column_idx, builder) in self.builders.iter_mut().enumerate() {
            builder.append_array_value(batch.column(column_idx).as_ref(), row_idx)?;
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

impl SlateUnionDistinctState {
    fn new(table: Arc<dyn KeyValueTable>, namespace: &str) -> Self {
        Self {
            table,
            key_prefix: keyspace::namespace_prefix(keyspace::prefix::INDEX, namespace),
        }
    }

    async fn load_count(&self, row_key: &[u8]) -> Result<i64> {
        let Some(bytes) = self
            .table
            .get_bytes(&self.state_key(row_key))
            .await
            .context("read union distinct state")?
        else {
            return Ok(0);
        };
        decode_i64(bytes.as_ref())
    }

    fn write_count(&self, batch: &mut WriteBatch, row_key: &[u8], count: i64) {
        let key = self.state_key(row_key);
        if count == 0 {
            batch.delete(key);
        } else {
            batch.put(key, count.to_be_bytes());
        }
    }

    fn state_key(&self, row_key: &[u8]) -> Vec<u8> {
        let mut key = self.key_prefix.clone();
        key.extend_from_slice(row_key);
        key
    }
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

fn row_converter_for_schema(schema: &SchemaRef) -> Result<RowConverter> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| SortField::new(field.data_type().clone()))
        .collect::<Vec<_>>();
    RowConverter::new(fields).context("build union distinct Arrow row converter")
}

fn decode_i64(bytes: &[u8]) -> Result<i64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("union distinct state value must be 8 bytes"))?;
    Ok(i64::from_be_bytes(bytes))
}
