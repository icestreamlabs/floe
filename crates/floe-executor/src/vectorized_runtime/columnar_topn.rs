use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::catalog::TableProvider;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::logical_plan::{Filter, Limit, Sort, TableScan, Window};
use datafusion::logical_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, Operator, ScalarUDF, WindowFunctionDefinition,
};
use datafusion::physical_plan::collect;
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarZSet};
use dbsp::storage::KeyValueTable;

use crate::delta_consolidation::diff_snapshot_batches;
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::table_provider::DynamicStateTableProvider;
use crate::vectorized_runtime::source_state::{rename_batches, resolve_source_table};

use super::columnar_composed::{
    ColumnarComposedMaterializedViewState, ColumnarComposedPlan,
    build_columnar_aggregate_join_materialized_view_state_in_namespace,
    columnar_aggregate_join_plan_for_plan, run_columnar_composed_state_tick,
};
use super::{
    VectorizedMaterializedViewState, VectorizedSourceState, apply_weighted_snapshot_delta,
    normalize_batches,
};

pub(super) struct ColumnarTopNPlan {
    pub(super) logical_plan: LogicalPlan,
    input: ColumnarTopNInputPlan,
    partition_columns: Vec<String>,
    full_snapshot_diff: bool,
}

enum ColumnarTopNInputPlan {
    Source {
        source_name: String,
    },
    AggregateJoin {
        input_name: String,
        schema: SchemaRef,
        plan: Box<ColumnarComposedPlan>,
    },
}

impl ColumnarTopNPlan {
    pub(super) fn source_name(&self) -> Option<String> {
        match &self.input {
            ColumnarTopNInputPlan::Source { source_name } => Some(source_name.clone()),
            ColumnarTopNInputPlan::AggregateJoin { .. } => None,
        }
    }
}

pub(super) struct ColumnarTopNMaterializedViewState {
    input_name: String,
    source_schema: SchemaRef,
    input_zset: Option<SlateBackedColumnarZSet>,
    aggregate_join: Option<Box<ColumnarComposedMaterializedViewState>>,
    output_zset: SlateBackedColumnarZSet,
    evaluator: TopNEvaluator,
    partition_indices: Vec<usize>,
    partition_converter: RowConverter,
    source_snapshot: Vec<RecordBatch>,
    initial_snapshot: Vec<RecordBatch>,
    full_snapshot_diff: bool,
}

struct TopNInputTick {
    delta: ColumnarZSet,
    input_changed: bool,
    next_source_snapshot: Option<Vec<RecordBatch>>,
}

impl ColumnarTopNMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

pub(super) struct TopNEvaluator {
    ctx: SessionContext,
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    provider: Arc<DynamicStateTableProvider>,
    alias_schema: Option<SchemaRef>,
    alias_provider: Option<Arc<DynamicStateTableProvider>>,
    output_schema: SchemaRef,
}

pub(super) fn columnar_topn_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarTopNPlan>> {
    let partition_columns = if let Some((rank_column, filter)) = row_number_filter_for_plan(plan) {
        let Some((window, _projection_without_rank)) =
            extract_window_plan(filter.input.as_ref(), &rank_column)
        else {
            return Ok(None);
        };
        if window.window_expr.len() != 1 {
            return Ok(None);
        }
        let Some((_alias, window_function)) = row_number_window_function(&window.window_expr[0])
        else {
            return Ok(None);
        };
        let partition_columns = window_function
            .params
            .partition_by
            .iter()
            .map(partition_column_name)
            .collect::<Option<Vec<_>>>();
        let Some(partition_columns) = partition_columns else {
            return Ok(None);
        };
        partition_columns
    } else if global_sort_limit_for_plan(plan) {
        Vec::new()
    } else {
        return Ok(None);
    };
    let input = if let Some(source_name) = single_source_for_plan(plan, sources) {
        if contains_unsupported_topn_wrapper(plan) {
            return Ok(None);
        }
        ColumnarTopNInputPlan::Source { source_name }
    } else if let Some((input_name, schema, aggregate_join)) =
        aggregate_join_topn_input_for_plan(plan, sources)?
    {
        ColumnarTopNInputPlan::AggregateJoin {
            input_name,
            schema,
            plan: Box::new(aggregate_join),
        }
    } else {
        return Ok(None);
    };
    let full_snapshot_diff =
        contains_aggregate(plan) || matches!(input, ColumnarTopNInputPlan::AggregateJoin { .. });

    Ok(Some(ColumnarTopNPlan {
        full_snapshot_diff,
        logical_plan: plan.clone(),
        input,
        partition_columns,
    }))
}

pub(super) async fn build_columnar_topn_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarTopNPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarTopNMaterializedViewState> {
    let mv_namespace = namespaces::materialized_view(view_name)?;
    let output_namespace = format!("{mv_namespace}/columnar/topn/output");
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed topn output zset")?;
    let initial_snapshot = snapshot_batches_from_zset(
        &output_zset
            .materialize_columnar()
            .await
            .context("load topn output snapshot")?,
    )?;
    match plan.input {
        ColumnarTopNInputPlan::Source { source_name } => {
            let source = sources
                .get(&source_name)
                .ok_or_else(|| anyhow::anyhow!("unknown topn source '{source_name}'"))?;
            let partition_indices = plan
                .partition_columns
                .iter()
                .map(|column| partition_column_index(source, column))
                .collect::<Result<Vec<_>>>()?;
            let partition_converter =
                row_converter_for_indices(&source.schema, &partition_indices)?;
            let input_namespace = format!("{mv_namespace}/columnar/topn/input");
            let input_zset = SlateBackedColumnarZSet::new(
                Arc::clone(&table),
                input_namespace,
                Arc::clone(&source.schema),
            )
            .await
            .context("initialize SlateDB-backed topn input zset")?;
            let source_snapshot = snapshot_batches_from_zset(
                &input_zset
                    .materialize_columnar()
                    .await
                    .context("load topn input snapshot")?,
            )?;
            let evaluator =
                TopNEvaluator::build(plan.logical_plan, &source_name, source, udfs, output_schema)
                    .await
                    .context("build topn vectorized evaluator")?;

            Ok(ColumnarTopNMaterializedViewState {
                input_name: source_name,
                source_schema: Arc::clone(&source.schema),
                input_zset: Some(input_zset),
                aggregate_join: None,
                output_zset,
                evaluator,
                partition_indices,
                partition_converter,
                source_snapshot,
                initial_snapshot,
                full_snapshot_diff: plan.full_snapshot_diff,
            })
        }
        ColumnarTopNInputPlan::AggregateJoin {
            input_name,
            schema,
            plan: aggregate_join_plan,
        } => {
            let partition_indices = plan
                .partition_columns
                .iter()
                .map(|column| partition_column_index_for_schema(&schema, column))
                .collect::<Result<Vec<_>>>()?;
            let partition_converter = row_converter_for_indices(&schema, &partition_indices)?;
            let aggregate_join_namespace = format!("{mv_namespace}/columnar/topn/aggregate_join");
            let aggregate_join = Box::pin(build_boxed_aggregate_join_topn_input_state(
                table,
                aggregate_join_namespace,
                &schema,
                *aggregate_join_plan,
                sources,
                udfs,
            ))
            .await
            .with_context(|| {
                format!(
                    "build SlateDB-backed topn aggregate-join input for '{}'",
                    input_name
                )
            })?;
            let source_snapshot = aggregate_join.initial_snapshot();
            let evaluator = TopNEvaluator::build_derived_input(
                plan.logical_plan,
                &input_name,
                &schema,
                udfs,
                output_schema,
            )
            .await
            .context("build aggregate-join topn vectorized evaluator")?;

            Ok(ColumnarTopNMaterializedViewState {
                input_name,
                source_schema: schema,
                input_zset: None,
                aggregate_join: Some(aggregate_join),
                output_zset,
                evaluator,
                partition_indices,
                partition_converter,
                source_snapshot,
                initial_snapshot,
                full_snapshot_diff: plan.full_snapshot_diff,
            })
        }
    }
}

async fn build_boxed_aggregate_join_topn_input_state(
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarComposedPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<Box<ColumnarComposedMaterializedViewState>> {
    Ok(Box::new(
        build_columnar_aggregate_join_materialized_view_state_in_namespace(
            table,
            namespace,
            output_schema,
            plan,
            sources,
            udfs,
        )
        .await?,
    ))
}

pub(super) async fn run_columnar_topn_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(columnar) = mv.columnar_topn.as_mut() else {
        return Ok(false);
    };
    let plan_start = Instant::now();

    let input_tick = prepare_topn_input_tick(columnar, insert_batches, weighted_delta_batches)
        .await
        .context("prepare SlateDB-backed topn input tick")?;
    if columnar.full_snapshot_diff {
        return run_columnar_topn_full_snapshot_diff_tick(
            registry, mv, version, input_tick, plan_start,
        )
        .await;
    }
    if columnar.input_zset.is_none() {
        bail!("non-snapshot-diff topn requires a source input zset");
    }
    let persisted_input_delta = input_tick.delta;
    let touched_partitions = touched_partition_keys(
        &columnar.partition_converter,
        &columnar.partition_indices,
        persisted_input_delta.batches(),
    )?;
    let previous_source_for_keys = filter_batches_to_partition_keys(
        &columnar.source_schema,
        &columnar.partition_converter,
        &columnar.partition_indices,
        &columnar.source_snapshot,
        &touched_partitions,
    )?;
    let next_source_snapshot = apply_source_snapshot_delta(
        &columnar.source_schema,
        &columnar.source_snapshot,
        &persisted_input_delta,
    )
    .await?;
    let next_source_for_keys = filter_batches_to_partition_keys(
        &columnar.source_schema,
        &columnar.partition_converter,
        &columnar.partition_indices,
        &next_source_snapshot,
        &touched_partitions,
    )?;

    let previous_output = columnar
        .evaluator
        .evaluate(&previous_source_for_keys)
        .await
        .context("evaluate previous topn partition outputs")?;
    let next_output = columnar
        .evaluator
        .evaluate(&next_source_for_keys)
        .await
        .context("evaluate next topn partition outputs")?;
    let diff = diff_snapshot_batches(
        Arc::clone(&mv.output_schema),
        &previous_output,
        &next_output,
    )
    .await
    .context("diff topn partition outputs")?;

    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), diff.batches)
            .context("build topn output zset delta")?;
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
            "apply Slate-backed topn columnar snapshot delta for '{}'",
            mv.view_name
        )
    })?;

    columnar.source_snapshot = next_source_snapshot;
    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "columnar_topn",
        "SlateDB-backed topn columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

async fn run_columnar_topn_full_snapshot_diff_tick(
    registry: &MaterializedViewRegistry,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
    input_tick: TopNInputTick,
    plan_start: Instant,
) -> Result<bool> {
    let Some(columnar) = mv.columnar_topn.as_mut() else {
        return Ok(false);
    };
    let has_input_change = input_tick.input_changed;
    let next_source_snapshot = if let Some(snapshot) = input_tick.next_source_snapshot {
        snapshot
    } else if has_input_change {
        apply_source_snapshot_delta(
            &columnar.source_schema,
            &columnar.source_snapshot,
            &input_tick.delta,
        )
        .await?
    } else {
        columnar.source_snapshot.clone()
    };

    let output_delta_batches = if has_input_change {
        let next_output = columnar
            .evaluator
            .evaluate(&next_source_snapshot)
            .await
            .context("evaluate next aggregate-topn output")?;
        diff_snapshot_batches(
            Arc::clone(&mv.output_schema),
            &mv.previous_snapshot,
            &next_output,
        )
        .await
        .context("diff aggregate-topn output")?
        .batches
    } else {
        Vec::new()
    };

    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build aggregate-topn output zset delta")?;
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
            "apply Slate-backed aggregate-topn columnar snapshot delta for '{}'",
            mv.view_name
        )
    })?;

    columnar.source_snapshot = next_source_snapshot;
    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "columnar_aggregate_topn",
        "SlateDB-backed aggregate-topn columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

async fn prepare_topn_input_tick(
    columnar: &mut ColumnarTopNMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<TopNInputTick> {
    if columnar.aggregate_join.is_some() {
        return prepare_aggregate_join_topn_input_tick(
            columnar,
            insert_batches,
            weighted_delta_batches,
        )
        .await;
    }
    let input_delta = source_input_delta(columnar, insert_batches, weighted_delta_batches)?;
    let input_zset = columnar
        .input_zset
        .as_mut()
        .context("topn source input zset missing")?;
    let delta = persisted_source_delta(input_zset, input_delta).await?;
    let input_changed = !delta.batches().is_empty();
    Ok(TopNInputTick {
        delta,
        input_changed,
        next_source_snapshot: None,
    })
}

async fn prepare_aggregate_join_topn_input_tick(
    columnar: &mut ColumnarTopNMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<TopNInputTick> {
    let Some(aggregate_join) = columnar.aggregate_join.as_mut() else {
        return Ok(TopNInputTick {
            delta: ColumnarZSet::empty(Arc::clone(&columnar.source_schema))?,
            input_changed: false,
            next_source_snapshot: None,
        });
    };
    let tick = run_columnar_composed_state_tick(
        aggregate_join.as_mut(),
        insert_batches,
        weighted_delta_batches,
        &columnar.source_schema,
        &columnar.source_snapshot,
    )
    .await
    .with_context(|| {
        format!(
            "evaluate topn aggregate-join input '{}'",
            columnar.input_name
        )
    })?;
    let input_changed = !tick.delta.batches().is_empty();
    if tick.input_changed && !input_changed {
        columnar.source_snapshot = tick.next_snapshot;
        return Ok(TopNInputTick {
            delta: tick.delta,
            input_changed: false,
            next_source_snapshot: None,
        });
    }
    Ok(TopNInputTick {
        delta: tick.delta,
        input_changed,
        next_source_snapshot: input_changed.then_some(tick.next_snapshot),
    })
}

fn source_input_delta(
    columnar: &ColumnarTopNMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    if let Some(weighted_batches) = weighted_delta_batches.get(columnar.input_name.as_str()) {
        ColumnarZSet::try_new_weighted(
            Arc::clone(&columnar.source_schema),
            weighted_batches.clone(),
        )
        .with_context(|| {
            format!(
                "build weighted topn input delta for '{}'",
                columnar.input_name
            )
        })
    } else if let Some(source_batches) = insert_batches.get(columnar.input_name.as_str()) {
        ColumnarZSet::from_value_batches(
            Arc::clone(&columnar.source_schema),
            source_batches.clone(),
            1,
        )
        .with_context(|| {
            format!(
                "build insert topn input delta for '{}'",
                columnar.input_name
            )
        })
    } else {
        ColumnarZSet::empty(Arc::clone(&columnar.source_schema))
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
    previous: &[RecordBatch],
    delta: &ColumnarZSet,
) -> Result<Vec<RecordBatch>> {
    if delta.batches().is_empty() {
        return Ok(previous.to_vec());
    }
    apply_weighted_snapshot_delta(schema, previous, delta.batches().to_vec()).await
}

impl TopNEvaluator {
    pub(super) async fn build(
        logical_plan: LogicalPlan,
        source_name: &str,
        source: &VectorizedSourceState,
        udfs: &[ScalarUDF],
        output_schema: &SchemaRef,
    ) -> Result<Self> {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        for udf in udfs.iter().cloned() {
            ctx.register_udf(udf);
        }
        let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&source.schema)));
        let mut provider_by_table = HashMap::new();
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
        let logical_plan = rebind_topn_logical_plan(logical_plan, &provider_by_table)?;
        let plan = ctx.state().create_physical_plan(&logical_plan).await?;
        Ok(Self {
            ctx,
            plan,
            provider,
            alias_schema,
            alias_provider,
            output_schema: Arc::clone(output_schema),
        })
    }

    pub(super) async fn evaluate(&self, batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
        self.provider.set_batches(batches.to_vec())?;
        if let (Some(alias_schema), Some(alias_provider)) =
            (self.alias_schema.as_ref(), self.alias_provider.as_ref())
        {
            alias_provider.set_batches(rename_batches(batches, alias_schema)?)?;
        }
        let collected = collect(Arc::clone(&self.plan), self.ctx.task_ctx()).await;
        self.provider.set_batches(Vec::new())?;
        if let Some(alias_provider) = self.alias_provider.as_ref() {
            alias_provider.set_batches(Vec::new())?;
        }
        normalize_batches(
            collected.context("execute vectorized topn evaluator")?,
            &self.output_schema,
        )
    }

    async fn build_derived_input(
        logical_plan: LogicalPlan,
        input_name: &str,
        input_schema: &SchemaRef,
        udfs: &[ScalarUDF],
        output_schema: &SchemaRef,
    ) -> Result<Self> {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        for udf in udfs.iter().cloned() {
            ctx.register_udf(udf);
        }
        let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(input_schema)));
        let logical_plan = rebind_topn_derived_input_logical_plan(
            logical_plan,
            input_name,
            Arc::clone(&provider) as Arc<dyn TableProvider>,
        )?;
        let plan = ctx.state().create_physical_plan(&logical_plan).await?;
        Ok(Self {
            ctx,
            plan,
            provider,
            alias_schema: None,
            alias_provider: None,
            output_schema: Arc::clone(output_schema),
        })
    }
}

fn touched_partition_keys(
    converter: &RowConverter,
    partition_indices: &[usize],
    batches: &[RecordBatch],
) -> Result<HashSet<Vec<u8>>> {
    let mut keys = HashSet::new();
    if partition_indices.is_empty() {
        if batches.iter().any(|batch| batch.num_rows() > 0) {
            keys.insert(Vec::new());
        }
        return Ok(keys);
    }
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let rows = converter
            .convert_columns(&project_columns(batch, partition_indices))
            .context("encode touched topn partition keys")?;
        for row_idx in 0..batch.num_rows() {
            keys.insert(rows.row(row_idx).data().to_vec());
        }
    }
    Ok(keys)
}

fn filter_batches_to_partition_keys(
    schema: &SchemaRef,
    converter: &RowConverter,
    partition_indices: &[usize],
    batches: &[RecordBatch],
    keys: &HashSet<Vec<u8>>,
) -> Result<Vec<RecordBatch>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    if partition_indices.is_empty() {
        return Ok(batches.to_vec());
    }
    let mut output = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let rows = converter
            .convert_columns(&project_columns(batch, partition_indices))
            .context("encode topn snapshot partition keys")?;
        let mut indices = Vec::new();
        for row_idx in 0..batch.num_rows() {
            if keys.contains(rows.row(row_idx).data()) {
                indices.push(u32::try_from(row_idx).context("topn batch exceeds u32 rows")?);
            }
        }
        if indices.is_empty() {
            continue;
        }
        let indices = UInt32Array::from(indices);
        let columns = batch
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), &indices, None))
            .collect::<std::result::Result<Vec<ArrayRef>, _>>()?;
        output.push(RecordBatch::try_new(Arc::clone(schema), columns)?);
    }
    Ok(output)
}

fn project_columns(batch: &RecordBatch, indices: &[usize]) -> Vec<ArrayRef> {
    indices
        .iter()
        .map(|idx| Arc::clone(batch.column(*idx)))
        .collect()
}

fn row_converter_for_indices(schema: &SchemaRef, indices: &[usize]) -> Result<RowConverter> {
    let fields = indices
        .iter()
        .map(|idx| SortField::new(schema.field(*idx).data_type().clone()))
        .collect::<Vec<_>>();
    RowConverter::new(fields).context("build topn partition Arrow row converter")
}

fn partition_column_index(source: &VectorizedSourceState, column: &str) -> Result<usize> {
    if let Ok(idx) = source.schema.index_of(column) {
        return Ok(idx);
    }
    if let Some(alias_schema) = source.alias_schema.as_ref()
        && let Ok(idx) = alias_schema.index_of(column)
    {
        return Ok(idx);
    }
    bail!("topn partition column '{column}' missing from source schema")
}

fn partition_column_index_for_schema(schema: &SchemaRef, column: &str) -> Result<usize> {
    schema
        .index_of(column)
        .with_context(|| format!("topn partition column '{column}' missing from input schema"))
}

fn derived_relation_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Projection(projection) => derived_relation_name(projection.input.as_ref()),
        LogicalPlan::Filter(filter) => derived_relation_name(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => Some(alias.alias.to_string()),
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            derived_relation_name(sort.input.as_ref())
        }
        _ => None,
    }
}

fn df_schema_to_arrow(schema: &datafusion::common::DFSchemaRef) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

fn rebind_topn_logical_plan(
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

fn rebind_topn_derived_input_logical_plan(
    logical_plan: LogicalPlan,
    input_name: &str,
    provider: Arc<dyn TableProvider>,
) -> Result<LogicalPlan> {
    match logical_plan {
        LogicalPlan::Projection(mut projection) => {
            projection.input = Arc::new(rebind_topn_derived_input_logical_plan(
                projection.input.as_ref().clone(),
                input_name,
                provider,
            )?);
            Ok(LogicalPlan::Projection(projection))
        }
        LogicalPlan::Filter(mut filter) => {
            filter.input = Arc::new(rebind_topn_derived_input_logical_plan(
                filter.input.as_ref().clone(),
                input_name,
                provider,
            )?);
            Ok(LogicalPlan::Filter(filter))
        }
        LogicalPlan::Limit(mut limit) => {
            limit.input = Arc::new(rebind_topn_derived_input_logical_plan(
                limit.input.as_ref().clone(),
                input_name,
                provider,
            )?);
            Ok(LogicalPlan::Limit(limit))
        }
        LogicalPlan::Sort(mut sort) => {
            sort.input = Arc::new(rebind_topn_derived_input_logical_plan(
                sort.input.as_ref().clone(),
                input_name,
                provider,
            )?);
            Ok(LogicalPlan::Sort(sort))
        }
        LogicalPlan::SubqueryAlias(mut alias) => {
            if alias.alias.table() == input_name {
                return scan_plan_for_provider(input_name, provider);
            }
            alias.input = Arc::new(rebind_topn_derived_input_logical_plan(
                alias.input.as_ref().clone(),
                input_name,
                provider,
            )?);
            Ok(LogicalPlan::SubqueryAlias(alias))
        }
        other => Ok(other),
    }
}

fn scan_plan_for_provider(
    input_name: &str,
    provider: Arc<dyn TableProvider>,
) -> Result<LogicalPlan> {
    LogicalPlanBuilder::scan(input_name, provider_as_source(provider), None)?
        .build()
        .map_err(Into::into)
}

fn row_number_filter_for_plan(plan: &LogicalPlan) -> Option<(String, &Filter)> {
    match plan {
        LogicalPlan::Projection(projection) => {
            row_number_filter_for_plan(projection.input.as_ref())
        }
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            row_number_filter_for_plan(sort.input.as_ref())
        }
        LogicalPlan::Filter(filter) => {
            if let Some((rank_column, _limit)) = extract_row_number_limit(&filter.predicate) {
                Some((rank_column, filter))
            } else {
                row_number_filter_for_plan(filter.input.as_ref())
            }
        }
        LogicalPlan::SubqueryAlias(alias) => row_number_filter_for_plan(alias.input.as_ref()),
        _ => None,
    }
}

fn global_sort_limit_for_plan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => {
            global_sort_limit_for_plan(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => global_sort_limit_for_plan(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => global_sort_limit_for_plan(alias.input.as_ref()),
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            global_sort_limit_for_plan(sort.input.as_ref())
        }
        LogicalPlan::Limit(limit) => {
            limit_has_nonnegative_skip_and_positive_fetch(limit)
                && sort_input_for_limit(limit.input.as_ref())
        }
        LogicalPlan::Sort(sort) => sort_has_positive_fetch(sort),
        _ => false,
    }
}

fn aggregate_join_topn_input_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<(String, SchemaRef, ColumnarComposedPlan)>> {
    if !global_sort_limit_for_plan(plan) {
        return Ok(None);
    }
    let Some(input) = global_topn_input_plan(plan) else {
        return Ok(None);
    };
    let Some(input_name) = derived_relation_name(input) else {
        return Ok(None);
    };
    let Some(aggregate_join) = columnar_aggregate_join_plan_for_plan(input, sources)? else {
        return Ok(None);
    };
    Ok(Some((
        input_name,
        df_schema_to_arrow(input.schema()),
        aggregate_join,
    )))
}

fn global_topn_input_plan(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Projection(projection) => global_topn_input_plan(projection.input.as_ref()),
        LogicalPlan::Filter(filter) => global_topn_input_plan(filter.input.as_ref()),
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            global_topn_input_plan(sort.input.as_ref())
        }
        LogicalPlan::Limit(limit) if limit_has_nonnegative_skip_and_positive_fetch(limit) => {
            sorted_input_for_limit(limit.input.as_ref())
        }
        LogicalPlan::Sort(sort) if sort_has_positive_fetch(sort) => Some(sort.input.as_ref()),
        _ => None,
    }
}

fn sorted_input_for_limit(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => sorted_input_for_limit(alias.input.as_ref()),
        LogicalPlan::Projection(projection) => sorted_input_for_limit(projection.input.as_ref()),
        LogicalPlan::Sort(sort) if !sort.expr.is_empty() => Some(sort.input.as_ref()),
        _ => None,
    }
}

fn limit_has_nonnegative_skip_and_positive_fetch(limit: &Limit) -> bool {
    let skip = limit
        .skip
        .as_deref()
        .map(literal_to_nonnegative_usize)
        .unwrap_or(Some(0));
    let fetch = limit.fetch.as_deref().and_then(literal_to_positive_usize);
    skip.is_some() && fetch.is_some()
}

fn sort_input_for_limit(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => sort_input_for_limit(alias.input.as_ref()),
        LogicalPlan::Projection(projection) => sort_input_for_limit(projection.input.as_ref()),
        LogicalPlan::Sort(sort) => !sort.expr.is_empty(),
        _ => false,
    }
}

fn sort_has_positive_fetch(sort: &Sort) -> bool {
    !sort.expr.is_empty() && sort.fetch.is_some_and(|fetch| fetch > 0)
}

fn extract_row_number_limit(predicate: &Expr) -> Option<(String, usize)> {
    let Expr::BinaryExpr(binary) = predicate else {
        return None;
    };
    if binary.op == Operator::And {
        let left = extract_row_number_limit(binary.left.as_ref());
        let right = extract_row_number_limit(binary.right.as_ref());
        return match (left, right) {
            (Some(found), None) | (None, Some(found)) => Some(found),
            _ => None,
        };
    }
    let (column, literal, kind) = match (&*binary.left, binary.op, &*binary.right) {
        (Expr::Column(column), Operator::LtEq, literal @ Expr::Literal(_, _)) => (
            column.name.clone(),
            literal,
            RowNumberPredicateKind::InclusiveUpper,
        ),
        (Expr::Column(column), Operator::Lt, literal @ Expr::Literal(_, _)) => (
            column.name.clone(),
            literal,
            RowNumberPredicateKind::ExclusiveUpper,
        ),
        (literal @ Expr::Literal(_, _), Operator::GtEq, Expr::Column(column)) => (
            column.name.clone(),
            literal,
            RowNumberPredicateKind::InclusiveUpper,
        ),
        (literal @ Expr::Literal(_, _), Operator::Gt, Expr::Column(column)) => (
            column.name.clone(),
            literal,
            RowNumberPredicateKind::ExclusiveUpper,
        ),
        (Expr::Column(column), Operator::Eq, literal @ Expr::Literal(_, _))
        | (literal @ Expr::Literal(_, _), Operator::Eq, Expr::Column(column)) => (
            column.name.clone(),
            literal,
            RowNumberPredicateKind::Equality,
        ),
        _ => return None,
    };
    let value = literal_to_positive_usize(literal)?;
    let limit = match kind {
        RowNumberPredicateKind::InclusiveUpper => value,
        RowNumberPredicateKind::ExclusiveUpper => {
            if value <= 1 {
                return None;
            }
            value - 1
        }
        RowNumberPredicateKind::Equality => 1,
    };
    (limit > 0).then_some((column, limit))
}

#[derive(Clone, Copy)]
enum RowNumberPredicateKind {
    InclusiveUpper,
    ExclusiveUpper,
    Equality,
}

fn literal_to_nonnegative_usize(expr: &Expr) -> Option<usize> {
    literal_to_i128(expr)
        .filter(|value| *value >= 0)
        .and_then(|value| usize::try_from(value).ok())
}

fn literal_to_positive_usize(expr: &Expr) -> Option<usize> {
    literal_to_i128(expr)
        .filter(|value| *value > 0)
        .and_then(|value| usize::try_from(value).ok())
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

fn extract_window_plan<'a>(
    input: &'a LogicalPlan,
    rank_column: &str,
) -> Option<(&'a Window, Option<Vec<Expr>>)> {
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

    let mut saw_rank = false;
    let mut remaining = Vec::with_capacity(projection.expr.len());
    for expr in &projection.expr {
        if projection_expr_matches_rank(expr, rank_column) {
            saw_rank = true;
            continue;
        }
        remaining.push(expr.clone());
    }
    saw_rank.then_some((window, Some(remaining)))
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

fn row_number_window_function(expr: &Expr) -> Option<(String, &WindowFunction)> {
    let (alias, window) = match expr {
        Expr::Alias(alias) => {
            let Expr::WindowFunction(window) = alias.expr.as_ref() else {
                return None;
            };
            (alias.name.clone(), window.as_ref())
        }
        Expr::WindowFunction(window) => (expr.schema_name().to_string(), window.as_ref()),
        _ => return None,
    };
    let is_row_number = matches!(
        &window.fun,
        WindowFunctionDefinition::WindowUDF(udf)
            if udf.name().eq_ignore_ascii_case("row_number")
    );
    if !is_row_number
        || window.params.filter.is_some()
        || window.params.null_treatment.is_some()
        || window.params.distinct
    {
        return None;
    }
    Some((alias, window))
}

fn partition_column_name(expr: &Expr) -> Option<String> {
    match strip_alias(expr) {
        Expr::Column(column) => Some(column.name.clone()),
        _ => None,
    }
}

fn projection_expr_matches_rank(expr: &Expr, rank_column: &str) -> bool {
    match expr {
        Expr::Column(column) => column.name == rank_column,
        Expr::Alias(alias) => alias.name == rank_column,
        _ => false,
    }
}

fn strip_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => strip_alias(alias.expr.as_ref()),
        _ => expr,
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
        LogicalPlan::Aggregate(aggregate) => {
            collect_sources(aggregate.input.as_ref(), sources, out)
        }
        LogicalPlan::Window(window) => collect_sources(window.input.as_ref(), sources, out),
        LogicalPlan::Limit(limit) => collect_sources(limit.input.as_ref(), sources, out),
        LogicalPlan::Sort(sort) => collect_sources(sort.input.as_ref(), sources, out),
        _ => {}
    }
}

fn table_scan_source(
    scan: &TableScan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Option<String> {
    resolve_source_table(scan.table_name.table().to_string(), sources)
}

fn contains_unsupported_topn_wrapper(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => {
            contains_unsupported_topn_wrapper(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => contains_unsupported_topn_wrapper(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => {
            contains_unsupported_topn_wrapper(alias.input.as_ref())
        }
        LogicalPlan::Aggregate(aggregate) => {
            contains_unsupported_topn_wrapper(aggregate.input.as_ref())
        }
        LogicalPlan::Window(window) => contains_unsupported_topn_wrapper(window.input.as_ref()),
        LogicalPlan::Limit(limit) => contains_unsupported_topn_wrapper(limit.input.as_ref()),
        LogicalPlan::Sort(sort) => contains_unsupported_topn_wrapper(sort.input.as_ref()),
        LogicalPlan::TableScan(_) => false,
        _ => true,
    }
}

fn contains_aggregate(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_) => true,
        LogicalPlan::Projection(projection) => contains_aggregate(projection.input.as_ref()),
        LogicalPlan::Filter(filter) => contains_aggregate(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => contains_aggregate(alias.input.as_ref()),
        LogicalPlan::Window(window) => contains_aggregate(window.input.as_ref()),
        LogicalPlan::Limit(limit) => contains_aggregate(limit.input.as_ref()),
        LogicalPlan::Sort(sort) => contains_aggregate(sort.input.as_ref()),
        _ => false,
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
