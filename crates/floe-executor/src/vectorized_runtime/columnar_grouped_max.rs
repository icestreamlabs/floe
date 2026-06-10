use std::collections::{BTreeSet, HashMap, hash_map::Entry};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, Int64Builder, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::catalog::TableProvider;
use datafusion::common::Column;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::logical_expr::logical_plan::{Aggregate, Projection};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, ScalarUDF};
use datafusion::physical_plan::{ExecutionPlan, collect};
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarZSet};
use dbsp::storage::{KeyValueTable, keyspace};
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

use crate::delta_consolidation::weighted_snapshot_schema;
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::scalar_array_builder::ScalarColumnBuilder;
use crate::table_provider::DynamicStateTableProvider;
use crate::vectorized_runtime::source_state::incremental_source_for_plan;
use crate::vectorized_source_delta::unit_source_delta_batches;

use super::columnar_join::{
    ColumnarJoinMaterializedViewState, ColumnarJoinPlan,
    build_columnar_join_materialized_view_state_in_namespace, columnar_join_plan_for_plan,
    run_columnar_join_state_tick_delta_only,
};
use super::{
    IncrementalMaterializedViewState, VectorizedMaterializedViewState, VectorizedSourceState,
    apply_weighted_snapshot_delta, build_incremental_materialized_view_state_from_logical_plan,
    collect_incremental_output, direct_project_record_batches, direct_projection_indices,
    normalize_batches,
};

const SUMMARY_TAG: u8 = b's';
const VALUE_TAG: u8 = b'v';

pub(super) struct ColumnarGroupedMaxPlan {
    input: ColumnarGroupedMaxInputPlan,
    projection: Projection,
    projection_schema: SchemaRef,
    group_schema: SchemaRef,
    output_mapping: Vec<usize>,
    max_idx: usize,
}

impl ColumnarGroupedMaxPlan {
    pub(super) fn source_names(&self) -> BTreeSet<String> {
        match &self.input {
            ColumnarGroupedMaxInputPlan::Source { source_name } => {
                [source_name.clone()].into_iter().collect()
            }
            ColumnarGroupedMaxInputPlan::Join { plan, .. } => plan.source_names(),
        }
    }
}

enum ColumnarGroupedMaxInputPlan {
    Source {
        source_name: String,
    },
    Join {
        input_name: String,
        source_schema: SchemaRef,
        projection_input_schema: SchemaRef,
        plan: Box<ColumnarJoinPlan>,
    },
}

pub(super) struct ColumnarGroupedMaxMaterializedViewState {
    input_name: String,
    source_schema: SchemaRef,
    input_zset: Option<SlateBackedColumnarZSet>,
    join: Option<Box<ColumnarJoinMaterializedViewState>>,
    input_snapshot: Vec<RecordBatch>,
    output_zset: SlateBackedColumnarZSet,
    max_state: SlateGroupedMaxState,
    projection_delta: GroupedMaxProjectionState,
    projection_schema: SchemaRef,
    group_schema: SchemaRef,
    output_mapping: Vec<usize>,
    max_idx: usize,
    initial_snapshot: Vec<RecordBatch>,
}

impl ColumnarGroupedMaxMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

enum GroupedMaxProjectionState {
    Source(IncrementalMaterializedViewState),
    Derived(GroupedMaxDerivedProjectionState),
}

struct GroupedMaxDerivedProjectionState {
    ctx: SessionContext,
    provider: Arc<DynamicStateTableProvider>,
    input_schema: SchemaRef,
    plan: Arc<dyn ExecutionPlan>,
    direct_projection: Option<Vec<usize>>,
}

struct SlateGroupedMaxState {
    table: Arc<dyn KeyValueTable>,
    key_prefix: Vec<u8>,
    bounds_key: Vec<u8>,
    group_bounds: Mutex<GroupKeyBoundsState>,
}

#[derive(Clone)]
enum GroupKeyBoundsState {
    Unknown,
    Empty,
    Present { min: Vec<u8>, max: Vec<u8> },
}

struct GroupKeyBoundsUpdate {
    min: Vec<u8>,
    max: Vec<u8>,
}

struct PendingMaxGroupDelta {
    value_deltas: HashMap<i64, i64>,
    batch: RecordBatch,
    row_idx: usize,
}

pub(super) struct ColumnarGroupedMaxTick {
    pub(super) delta: ColumnarZSet,
    pub(super) next_snapshot: Vec<RecordBatch>,
    pub(super) input_changed: bool,
}

pub(super) fn columnar_grouped_max_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    output_schema: &SchemaRef,
) -> Result<Option<ColumnarGroupedMaxPlan>> {
    let Some((aggregate, projection)) = grouped_max_aggregate_for_plan(plan) else {
        return Ok(None);
    };
    if aggregate.group_expr.is_empty() || aggregate.aggr_expr.len() != 1 {
        return Ok(None);
    }
    if aggregate
        .group_expr
        .iter()
        .any(|expr| matches!(expr, Expr::GroupingSet(_)))
    {
        return Ok(None);
    }
    let Some(max_value_expr) = max_value_expr(&aggregate.aggr_expr[0]) else {
        return Ok(None);
    };

    let max_idx = aggregate.group_expr.len();
    let aggregate_schema = df_schema_to_arrow(&aggregate.schema)?;
    if aggregate_schema.fields().len() != max_idx + 1
        || aggregate_schema.field(max_idx).data_type() != &DataType::Int64
    {
        return Ok(None);
    }
    let output_mapping = match output_mapping_for_projection(projection, aggregate, output_schema) {
        Some(mapping) => mapping,
        None => return Ok(None),
    };
    if output_mapping
        .iter()
        .any(|idx| *idx >= aggregate_schema.fields().len())
    {
        return Ok(None);
    }
    for (output_field, source_idx) in output_schema.fields().iter().zip(output_mapping.iter()) {
        if output_field.data_type() != aggregate_schema.field(*source_idx).data_type() {
            return Ok(None);
        }
    }

    let input =
        if let Some(source_name) = incremental_source_for_plan(aggregate.input.as_ref(), sources) {
            ColumnarGroupedMaxInputPlan::Source { source_name }
        } else if let Some(join) = columnar_join_plan_for_plan(aggregate.input.as_ref(), sources)? {
            let source_schema = df_schema_to_arrow(aggregate.input.schema())?;
            let projection_input_schema = derived_projection_input_schema(&source_schema);
            let input_name = derived_relation_name(aggregate.input.as_ref())
                .unwrap_or_else(|| "__floe_grouped_max_join_input".to_string());
            ColumnarGroupedMaxInputPlan::Join {
                input_name,
                source_schema,
                projection_input_schema,
                plan: Box::new(join),
            }
        } else {
            return Ok(None);
        };

    let mut projection_expr = aggregate.group_expr.clone();
    projection_expr.push(max_value_expr);
    let projection_input = match &input {
        ColumnarGroupedMaxInputPlan::Source { .. } => aggregate.input.as_ref().clone(),
        ColumnarGroupedMaxInputPlan::Join {
            input_name,
            projection_input_schema,
            ..
        } => {
            projection_expr = rewrite_projection_exprs_for_derived_input(
                projection_expr,
                aggregate.input.schema(),
                projection_input_schema,
            )?;
            scan_plan_for_derived_input(input_name, projection_input_schema)?
        }
    };
    let value_projection = Projection::try_new(projection_expr, Arc::new(projection_input))
        .context("build grouped-max value projection")?;
    let projection_schema = df_schema_to_arrow(&value_projection.schema)?;
    if projection_schema.fields().len() != max_idx + 1
        || projection_schema.field(max_idx).data_type() != &DataType::Int64
    {
        return Ok(None);
    }
    let group_fields = projection_schema
        .fields()
        .iter()
        .take(max_idx)
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let group_schema = Arc::new(Schema::new(group_fields));

    Ok(Some(ColumnarGroupedMaxPlan {
        input,
        projection: value_projection,
        projection_schema,
        group_schema,
        output_mapping,
        max_idx,
    }))
}

pub(super) async fn build_columnar_grouped_max_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarGroupedMaxPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarGroupedMaxMaterializedViewState> {
    let mv_namespace = namespaces::materialized_view(view_name)?;
    build_columnar_grouped_max_materialized_view_state_in_namespace(
        table,
        mv_namespace,
        output_schema,
        plan,
        sources,
        udfs,
    )
    .await
}

pub(super) async fn build_columnar_grouped_max_materialized_view_state_in_namespace(
    table: Arc<dyn KeyValueTable>,
    mv_namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarGroupedMaxPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarGroupedMaxMaterializedViewState> {
    let output_namespace = format!("{mv_namespace}/columnar/grouped_max/output");
    let state_namespace = format!("{mv_namespace}/columnar/grouped_max/state");
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed grouped-max output zset")?;
    let initial_snapshot = snapshot_batches_from_zset(
        &output_zset
            .materialize_columnar()
            .await
            .context("load grouped-max output snapshot")?,
    )?;
    let (input_name, source_schema, input_zset, join, input_snapshot, projection_delta) =
        match plan.input {
            ColumnarGroupedMaxInputPlan::Source { source_name } => {
                let source = sources.get(&source_name).ok_or_else(|| {
                    anyhow::anyhow!("unknown vectorized source '{}'", source_name)
                })?;
                let input_namespace = format!("{mv_namespace}/columnar/grouped_max/input");
                let input_zset = Box::pin(SlateBackedColumnarZSet::new(
                    Arc::clone(&table),
                    input_namespace,
                    Arc::clone(&source.schema),
                ))
                .await
                .context("initialize SlateDB-backed grouped-max input zset")?;
                let projection_delta = build_incremental_materialized_view_state_from_logical_plan(
                    &source_name,
                    sources,
                    udfs,
                    &LogicalPlan::Projection(plan.projection.clone()),
                )
                .await
                .context("build grouped-max vectorized value projection delta plan")?;
                (
                    source_name,
                    Arc::clone(&source.schema),
                    Some(input_zset),
                    None,
                    Vec::new(),
                    GroupedMaxProjectionState::Source(projection_delta),
                )
            }
            ColumnarGroupedMaxInputPlan::Join {
                input_name,
                source_schema,
                projection_input_schema,
                plan: join_plan,
            } => {
                let join_namespace = format!("{mv_namespace}/columnar/grouped_max/join_input");
                let join = Box::pin(build_boxed_join_grouped_max_input_state(
                    Arc::clone(&table),
                    join_namespace,
                    &source_schema,
                    *join_plan,
                    sources,
                    udfs,
                ))
                .await
                .with_context(|| {
                    format!(
                        "build SlateDB-backed grouped-max join input for '{}'",
                        input_name
                    )
                })?;
                let input_snapshot = join.initial_snapshot();
                let projection_delta = build_derived_projection_state(
                    LogicalPlan::Projection(plan.projection.clone()),
                    &input_name,
                    &projection_input_schema,
                    udfs,
                )
                .await
                .with_context(|| {
                    format!(
                        "build grouped-max derived projection delta plan for '{}'",
                        input_name
                    )
                })?;
                (
                    input_name,
                    source_schema,
                    None,
                    Some(join),
                    input_snapshot,
                    GroupedMaxProjectionState::Derived(projection_delta),
                )
            }
        };

    let assume_empty_state = output_zset.current_handle().is_none();

    Ok(ColumnarGroupedMaxMaterializedViewState {
        input_name,
        source_schema,
        input_zset,
        join,
        input_snapshot,
        output_zset,
        max_state: SlateGroupedMaxState::new(table, &state_namespace, assume_empty_state).await?,
        projection_delta,
        projection_schema: plan.projection_schema,
        group_schema: plan.group_schema,
        output_mapping: plan.output_mapping,
        max_idx: plan.max_idx,
        initial_snapshot,
    })
}

async fn build_boxed_join_grouped_max_input_state(
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarJoinPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<Box<ColumnarJoinMaterializedViewState>> {
    Ok(Box::new(
        Box::pin(build_columnar_join_materialized_view_state_in_namespace(
            table,
            namespace,
            output_schema,
            plan,
            sources,
            udfs,
        ))
        .await?,
    ))
}

async fn build_derived_projection_state(
    logical_plan: LogicalPlan,
    input_name: &str,
    input_schema: &SchemaRef,
    udfs: &[ScalarUDF],
) -> Result<GroupedMaxDerivedProjectionState> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    for udf in udfs.iter().cloned() {
        ctx.register_udf(udf);
    }
    let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(input_schema)));
    let direct_projection = direct_projection_indices(&logical_plan, input_schema);
    let logical_plan =
        rebind_derived_projection_plan(logical_plan, input_name, Arc::clone(&provider))?;
    let plan = ctx.state().create_physical_plan(&logical_plan).await?;
    Ok(GroupedMaxDerivedProjectionState {
        ctx,
        provider,
        input_schema: Arc::clone(input_schema),
        plan,
        direct_projection,
    })
}

fn rebind_derived_projection_plan(
    logical_plan: LogicalPlan,
    input_name: &str,
    provider: Arc<DynamicStateTableProvider>,
) -> Result<LogicalPlan> {
    let transformed = logical_plan.transform_up(|plan| match plan {
        LogicalPlan::TableScan(mut scan) if scan.table_name.table() == input_name => {
            scan.source = provider_as_source(Arc::clone(&provider) as Arc<dyn TableProvider>);
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        }
        other => Ok(Transformed::no(other)),
    })?;
    Ok(transformed.data)
}

pub(super) async fn run_columnar_grouped_max_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(columnar) = mv.columnar_grouped_max.as_mut() else {
        return Ok(false);
    };

    let plan_start = Instant::now();
    let tick = run_columnar_grouped_max_state_tick(
        columnar,
        insert_batches,
        weighted_delta_batches,
        &mv.output_schema,
        &mv.previous_snapshot,
    )
    .await
    .with_context(|| {
        format!(
            "evaluate Slate-backed grouped-max columnar snapshot delta for '{}'",
            mv.view_name
        )
    })?;

    let delta_batches = tick.delta.batches().to_vec();
    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, tick.next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = tick.next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "columnar_grouped_max",
        "SlateDB-backed grouped-max columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

pub(super) async fn run_columnar_grouped_max_state_tick(
    columnar: &mut ColumnarGroupedMaxMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    output_schema: &SchemaRef,
    previous_snapshot: &[RecordBatch],
) -> Result<ColumnarGroupedMaxTick> {
    run_columnar_grouped_max_state_tick_inner(
        columnar,
        insert_batches,
        weighted_delta_batches,
        output_schema,
        previous_snapshot,
        true,
    )
    .await
}

pub(super) async fn run_columnar_grouped_max_state_tick_delta_only(
    columnar: &mut ColumnarGroupedMaxMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    output_schema: &SchemaRef,
    previous_snapshot: &[RecordBatch],
) -> Result<ColumnarGroupedMaxTick> {
    run_columnar_grouped_max_state_tick_inner(
        columnar,
        insert_batches,
        weighted_delta_batches,
        output_schema,
        previous_snapshot,
        false,
    )
    .await
}

async fn run_columnar_grouped_max_state_tick_inner(
    columnar: &mut ColumnarGroupedMaxMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    output_schema: &SchemaRef,
    previous_snapshot: &[RecordBatch],
    maintain_output_snapshot: bool,
) -> Result<ColumnarGroupedMaxTick> {
    let persisted_input_delta =
        prepare_grouped_max_input_delta(columnar, insert_batches, weighted_delta_batches).await?;
    let input_changed = !persisted_input_delta.batches().is_empty();
    let pending = grouped_max_pending_delta(columnar, persisted_input_delta.batches()).await?;
    let output_delta_batches = apply_grouped_max_delta(columnar, pending).await?;
    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build grouped-max output zset delta")?;
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
        .context("apply Slate-backed grouped-max columnar snapshot delta")?
    } else {
        Vec::new()
    };

    Ok(ColumnarGroupedMaxTick {
        delta: persisted_output_delta,
        next_snapshot,
        input_changed,
    })
}

async fn prepare_grouped_max_input_delta(
    columnar: &mut ColumnarGroupedMaxMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    if columnar.join.is_some() {
        return prepare_join_grouped_max_input_delta(
            columnar,
            insert_batches,
            weighted_delta_batches,
        )
        .await;
    }

    let input_delta =
        if let Some(weighted_batches) = weighted_delta_batches.get(columnar.input_name.as_str()) {
            ColumnarZSet::try_new_weighted(
                Arc::clone(&columnar.source_schema),
                weighted_batches.clone(),
            )
            .with_context(|| {
                format!(
                    "build weighted grouped-max input delta for '{}'",
                    columnar.input_name
                )
            })?
        } else if let Some(source_batches) = insert_batches.get(columnar.input_name.as_str()) {
            ColumnarZSet::from_value_batches(
                Arc::clone(&columnar.source_schema),
                source_batches.clone(),
                1,
            )
            .with_context(|| {
                format!(
                    "build insert grouped-max input delta for '{}'",
                    columnar.input_name
                )
            })?
        } else {
            ColumnarZSet::empty(Arc::clone(&columnar.source_schema))?
        };

    let input_zset = columnar
        .input_zset
        .as_mut()
        .context("grouped-max source input zset missing")?;
    if let Some(handle) = input_zset.create_version(&input_delta, None).await? {
        input_zset.read_delta(&handle).await
    } else {
        Ok(input_delta)
    }
}

async fn prepare_join_grouped_max_input_delta(
    columnar: &mut ColumnarGroupedMaxMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    let Some(join) = columnar.join.as_mut() else {
        return ColumnarZSet::empty(Arc::clone(&columnar.source_schema));
    };
    let tick = Box::pin(run_columnar_join_state_tick_delta_only(
        join.as_mut(),
        insert_batches,
        weighted_delta_batches,
        &columnar.source_schema,
        &columnar.input_snapshot,
    ))
    .await
    .with_context(|| {
        format!(
            "evaluate grouped-max nested join input '{}'",
            columnar.input_name
        )
    })?;
    if tick.input_changed && !tick.next_snapshot.is_empty() {
        columnar.input_snapshot = tick.next_snapshot;
    }
    Ok(tick.delta)
}

async fn grouped_max_pending_delta(
    columnar: &ColumnarGroupedMaxMaterializedViewState,
    input_batches: &[RecordBatch],
) -> Result<HashMap<Vec<u8>, PendingMaxGroupDelta>> {
    let mut pending = HashMap::new();
    if input_batches.is_empty() {
        return Ok(pending);
    }

    let mut positive_source_batches = Vec::new();
    let mut negative_source_batches = Vec::new();
    for batch in input_batches {
        let unit_delta =
            unit_source_delta_batches(&columnar.source_schema, batch)?.with_context(|| {
                format!(
                    "grouped-max materialized view received non-unit weighted source deltas for '{}'",
                    columnar.input_name
                )
            })?;
        positive_source_batches.extend(unit_delta.positive);
        negative_source_batches.extend(unit_delta.negative);
    }

    let positive_output =
        collect_grouped_max_projection_output(columnar, &positive_source_batches).await?;
    add_projected_value_batches_to_pending(columnar, &positive_output, 1, &mut pending)?;
    let negative_output =
        collect_grouped_max_projection_output(columnar, &negative_source_batches).await?;
    add_projected_value_batches_to_pending(columnar, &negative_output, -1, &mut pending)?;
    pending.retain(|_, delta| !delta.value_deltas.is_empty());
    Ok(pending)
}

async fn collect_grouped_max_projection_output(
    columnar: &ColumnarGroupedMaxMaterializedViewState,
    source_batches: &[RecordBatch],
) -> Result<Vec<RecordBatch>> {
    if source_batches.is_empty() {
        return Ok(Vec::new());
    }
    match &columnar.projection_delta {
        GroupedMaxProjectionState::Source(incremental) => {
            collect_incremental_output(incremental, source_batches, &columnar.projection_schema)
                .await
        }
        GroupedMaxProjectionState::Derived(derived) => {
            if let Some(indices) = derived.direct_projection.as_ref() {
                return direct_project_record_batches(
                    source_batches,
                    &columnar.projection_schema,
                    indices,
                    "grouped-max",
                );
            }
            let provider_batches =
                rewrap_record_batches_with_schema(source_batches, &derived.input_schema)?;
            derived.provider.set_batches(provider_batches)?;
            let collected = collect(Arc::clone(&derived.plan), derived.ctx.task_ctx()).await;
            derived.provider.set_batches(Vec::new())?;
            normalize_batches(
                collected.context("execute grouped-max derived projection")?,
                &columnar.projection_schema,
            )
        }
    }
}

fn add_projected_value_batches_to_pending(
    columnar: &ColumnarGroupedMaxMaterializedViewState,
    batches: &[RecordBatch],
    sign: i64,
    pending: &mut HashMap<Vec<u8>, PendingMaxGroupDelta>,
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    let converter = row_converter_for_schema(&columnar.group_schema)?;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let group_columns = (0..columnar.max_idx)
            .map(|idx| Arc::clone(batch.column(idx)))
            .collect::<Vec<ArrayRef>>();
        let group_rows = converter
            .convert_columns(&group_columns)
            .context("encode grouped-max group keys")?;
        let values = batch
            .column(columnar.max_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("grouped-max value must be Int64"))?;
        for row_idx in 0..batch.num_rows() {
            if values.is_null(row_idx) {
                continue;
            }
            let value = values.value(row_idx);
            let key = group_rows.row(row_idx).data().to_vec();
            let group = pending.entry(key).or_insert_with(|| PendingMaxGroupDelta {
                value_deltas: HashMap::new(),
                batch: batch.clone(),
                row_idx,
            });
            match group.value_deltas.entry(value) {
                Entry::Occupied(mut entry) => {
                    let next = entry
                        .get()
                        .checked_add(sign)
                        .ok_or_else(|| anyhow::anyhow!("grouped-max value delta overflow"))?;
                    if next == 0 {
                        entry.remove();
                    } else {
                        *entry.get_mut() = next;
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(sign);
                }
            }
        }
    }
    Ok(())
}

async fn apply_grouped_max_delta(
    columnar: &ColumnarGroupedMaxMaterializedViewState,
    pending: HashMap<Vec<u8>, PendingMaxGroupDelta>,
) -> Result<Vec<RecordBatch>> {
    let mut builder = WeightedMaxOutputBuilder::new(
        columnar.output_zset.value_schema(),
        &columnar.output_mapping,
    )?;
    if pending.is_empty() {
        return builder.finish();
    }

    let mut writes = WriteBatch::new();
    let mut bounds_update = None;
    for (group_key, delta) in pending {
        let old_max = columnar.max_state.load_max(&group_key).await?;
        let mut updated_counts = HashMap::new();
        for (value, value_delta) in &delta.value_deltas {
            let old_count =
                if columnar
                    .max_state
                    .value_count_read_required(old_max, *value, *value_delta)
                {
                    columnar
                        .max_state
                        .load_value_count(&group_key, *value)
                        .await?
                } else {
                    0
                };
            let new_count = old_count
                .checked_add(*value_delta)
                .ok_or_else(|| anyhow::anyhow!("grouped-max value count overflow"))?;
            if new_count < 0 {
                bail!("grouped-max state removed more values than were present");
            }
            updated_counts.insert(*value, new_count);
            columnar
                .max_state
                .write_value_count(&mut writes, &group_key, *value, new_count)?;
        }
        let new_max = columnar
            .max_state
            .new_max_after_delta(&group_key, old_max, &updated_counts)
            .await?;
        if old_max != new_max {
            if let Some(old_max) = old_max {
                builder.append(&delta.batch, delta.row_idx, columnar.max_idx, old_max, -1)?;
            }
            if let Some(new_max) = new_max {
                builder.append(&delta.batch, delta.row_idx, columnar.max_idx, new_max, 1)?;
            }
        }
        columnar
            .max_state
            .write_max(&mut writes, &group_key, new_max)?;
        if new_max.is_some() {
            merge_group_key_bounds_update(&mut bounds_update, &group_key);
        }
    }
    columnar
        .max_state
        .write_group_bounds(&mut writes, bounds_update.as_ref())?;
    columnar
        .max_state
        .table
        .write_batch(writes)
        .await
        .context("persist grouped-max state updates")?;
    builder.finish()
}

impl SlateGroupedMaxState {
    async fn new(
        table: Arc<dyn KeyValueTable>,
        namespace: &str,
        assume_empty: bool,
    ) -> Result<Self> {
        let key_prefix = keyspace::namespace_prefix(keyspace::prefix::INDEX, namespace);
        let mut bounds_key = key_prefix.clone();
        bounds_key.extend_from_slice(b"bounds/group_key");
        let group_bounds = match table
            .get_bytes(&bounds_key)
            .await
            .context("read grouped-max group key bounds")?
        {
            Some(bytes) => decode_group_key_bounds(bytes.as_ref())?,
            None if assume_empty => GroupKeyBoundsState::Empty,
            None => GroupKeyBoundsState::Unknown,
        };
        Ok(Self {
            table,
            key_prefix,
            bounds_key,
            group_bounds: Mutex::new(group_bounds),
        })
    }

    async fn load_max(&self, group_key: &[u8]) -> Result<Option<i64>> {
        if !self.group_key_may_exist(group_key)? {
            return Ok(None);
        }
        let Some(bytes) = self
            .table
            .get_bytes(&self.group_key(SUMMARY_TAG, group_key)?)
            .await
            .context("read grouped-max summary state")?
        else {
            return Ok(None);
        };
        Ok(Some(decode_i64(bytes.as_ref())?))
    }

    async fn load_value_count(&self, group_key: &[u8], value: i64) -> Result<i64> {
        let Some(bytes) = self
            .table
            .get_bytes(&self.value_key(group_key, value)?)
            .await
            .context("read grouped-max value count state")?
        else {
            return Ok(0);
        };
        decode_i64(bytes.as_ref())
    }

    fn value_count_read_required(
        &self,
        old_max: Option<i64>,
        value: i64,
        value_delta: i64,
    ) -> bool {
        match old_max {
            None => false,
            Some(old_max) => value_delta < 0 || value <= old_max,
        }
    }

    async fn new_max_after_delta(
        &self,
        group_key: &[u8],
        old_max: Option<i64>,
        updated_counts: &HashMap<i64, i64>,
    ) -> Result<Option<i64>> {
        let mut max_added = None;
        for (value, count) in updated_counts {
            if *count > 0 {
                max_added = Some(max_added.map_or(*value, |current: i64| current.max(*value)));
            }
        }

        match old_max {
            None => Ok(max_added),
            Some(old_max) => {
                let old_max_still_present = match updated_counts.get(&old_max) {
                    Some(count) => *count > 0,
                    None => true,
                };
                if old_max_still_present {
                    return Ok(Some(max_added.map_or(old_max, |value| value.max(old_max))));
                }
                self.scan_max_with_overlay(group_key, updated_counts).await
            }
        }
    }

    async fn scan_max_with_overlay(
        &self,
        group_key: &[u8],
        updated_counts: &HashMap<i64, i64>,
    ) -> Result<Option<i64>> {
        let value_prefix = self.group_key(VALUE_TAG, group_key)?;
        let mut max = None;
        for (key, value_bytes) in self
            .table
            .scan_prefix(&value_prefix, &ScanOptions::default())
            .await
            .context("scan grouped-max value count state")?
        {
            let value = decode_i64_sortable(
                key.get(value_prefix.len()..)
                    .ok_or_else(|| anyhow::anyhow!("invalid grouped-max value key"))?,
            )?;
            let old_count = decode_i64(&value_bytes)?;
            let count = updated_counts.get(&value).copied().unwrap_or(old_count);
            if count > 0 {
                max = Some(max.map_or(value, |current: i64| current.max(value)));
            }
        }
        for (value, count) in updated_counts {
            if *count > 0 {
                max = Some(max.map_or(*value, |current: i64| current.max(*value)));
            }
        }
        Ok(max)
    }

    fn write_value_count(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        value: i64,
        count: i64,
    ) -> Result<()> {
        let key = self.value_key(group_key, value)?;
        if count == 0 {
            batch.delete(key);
        } else {
            batch.put(key, count.to_be_bytes());
        }
        Ok(())
    }

    fn write_max(&self, batch: &mut WriteBatch, group_key: &[u8], max: Option<i64>) -> Result<()> {
        let key = self.group_key(SUMMARY_TAG, group_key)?;
        if let Some(max) = max {
            batch.put(key, max.to_be_bytes());
        } else {
            batch.delete(key);
        }
        Ok(())
    }

    fn group_key_may_exist(&self, group_key: &[u8]) -> Result<bool> {
        let bounds = self
            .group_bounds
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-max group bounds poisoned"))?;
        Ok(match &*bounds {
            GroupKeyBoundsState::Unknown => true,
            GroupKeyBoundsState::Empty => false,
            GroupKeyBoundsState::Present { min, max } => {
                group_key >= min.as_slice() && group_key <= max.as_slice()
            }
        })
    }

    fn write_group_bounds(
        &self,
        batch: &mut WriteBatch,
        update: Option<&GroupKeyBoundsUpdate>,
    ) -> Result<()> {
        let Some(update) = update else {
            return Ok(());
        };
        let mut bounds = self
            .group_bounds
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-max group bounds poisoned"))?;
        let next = match bounds.clone() {
            GroupKeyBoundsState::Unknown => None,
            GroupKeyBoundsState::Empty => Some(GroupKeyBoundsState::Present {
                min: update.min.clone(),
                max: update.max.clone(),
            }),
            GroupKeyBoundsState::Present { min, max } => Some(GroupKeyBoundsState::Present {
                min: if update.min.as_slice() < min.as_slice() {
                    update.min.clone()
                } else {
                    min.clone()
                },
                max: if update.max.as_slice() > max.as_slice() {
                    update.max.clone()
                } else {
                    max.clone()
                },
            }),
        };
        if let Some(next) = next {
            let encoded = encode_group_key_bounds(&next)?;
            *bounds = next;
            batch.put(self.bounds_key.clone(), encoded);
        }
        Ok(())
    }

    fn value_key(&self, group_key: &[u8], value: i64) -> Result<Vec<u8>> {
        let mut key = self.group_key(VALUE_TAG, group_key)?;
        key.extend_from_slice(&encode_i64_sortable(value));
        Ok(key)
    }

    fn group_key(&self, tag: u8, group_key: &[u8]) -> Result<Vec<u8>> {
        let len =
            u32::try_from(group_key.len()).context("grouped-max group key exceeds u32 bytes")?;
        let mut key = self.key_prefix.clone();
        key.push(tag);
        key.extend_from_slice(&len.to_be_bytes());
        key.extend_from_slice(group_key);
        Ok(key)
    }
}

struct WeightedMaxOutputBuilder {
    weighted_schema: SchemaRef,
    output_mapping: Vec<usize>,
    builders: Vec<ScalarColumnBuilder>,
    weights: Int64Builder,
    rows: usize,
}

impl WeightedMaxOutputBuilder {
    fn new(schema: SchemaRef, output_mapping: &[usize]) -> Result<Self> {
        let builders = schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), 1024))
            .collect::<Result<Vec<_>>>()?;
        let weighted_schema = weighted_snapshot_schema(&schema)?;
        Ok(Self {
            weighted_schema,
            output_mapping: output_mapping.to_vec(),
            builders,
            weights: Int64Builder::with_capacity(1024),
            rows: 0,
        })
    }

    fn append(
        &mut self,
        projection_batch: &RecordBatch,
        row_idx: usize,
        max_idx: usize,
        max: i64,
        weight: i64,
    ) -> Result<()> {
        for (output_idx, source_idx) in self.output_mapping.iter().copied().enumerate() {
            if source_idx == max_idx {
                self.builders[output_idx].append_i64_value(max)?;
            } else {
                self.builders[output_idx]
                    .append_array_value(projection_batch.column(source_idx).as_ref(), row_idx)?;
            }
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

fn grouped_max_aggregate_for_plan(plan: &LogicalPlan) -> Option<(&Aggregate, Option<&Projection>)> {
    match plan {
        LogicalPlan::Aggregate(aggregate) => Some((aggregate, None)),
        LogicalPlan::Projection(projection) => match projection.input.as_ref() {
            LogicalPlan::Aggregate(aggregate) => Some((aggregate, Some(projection))),
            _ => None,
        },
        LogicalPlan::SubqueryAlias(alias) => grouped_max_aggregate_for_plan(alias.input.as_ref()),
        _ => None,
    }
}

fn output_mapping_for_projection(
    projection: Option<&Projection>,
    aggregate: &Aggregate,
    output_schema: &SchemaRef,
) -> Option<Vec<usize>> {
    let aggregate_schema = &aggregate.schema;
    let max_idx = aggregate.group_expr.len();
    match projection {
        Some(projection) => {
            if projection.expr.len() != output_schema.fields().len() {
                return None;
            }
            projection
                .expr
                .iter()
                .map(|expr| output_expr_source_idx(strip_alias(expr), aggregate_schema, max_idx))
                .collect()
        }
        None => {
            if aggregate_schema.fields().len() != output_schema.fields().len() {
                return None;
            }
            Some((0..aggregate_schema.fields().len()).collect())
        }
    }
}

fn output_expr_source_idx(
    expr: &Expr,
    aggregate_schema: &datafusion::common::DFSchemaRef,
    max_idx: usize,
) -> Option<usize> {
    if max_value_expr(expr).is_some() {
        return Some(max_idx);
    }
    let Expr::Column(column) = expr else {
        return None;
    };
    aggregate_schema
        .fields()
        .iter()
        .position(|field| field.name() == &column.name)
}

fn max_value_expr(expr: &Expr) -> Option<Expr> {
    let Expr::AggregateFunction(aggregate) = strip_alias(expr) else {
        return None;
    };
    let params = &aggregate.params;
    if !aggregate.func.name().eq_ignore_ascii_case("max")
        || params.distinct
        || params.filter.is_some()
        || !params.order_by.is_empty()
        || params.null_treatment.is_some()
    {
        return None;
    }
    let [expr] = params.args.as_slice() else {
        return None;
    };
    Some(expr.clone())
}

fn strip_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => strip_alias(alias.expr.as_ref()),
        _ => expr,
    }
}

fn derived_projection_input_schema(source_schema: &SchemaRef) -> SchemaRef {
    let fields = source_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            Field::new(
                format!("__floe_col_{idx}"),
                field.data_type().clone(),
                field.is_nullable(),
            )
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

fn rewrite_projection_exprs_for_derived_input(
    exprs: Vec<Expr>,
    input_schema: &datafusion::common::DFSchemaRef,
    projection_input_schema: &SchemaRef,
) -> Result<Vec<Expr>> {
    exprs
        .into_iter()
        .map(|expr| {
            rewrite_projection_expr_for_derived_input(expr, input_schema, projection_input_schema)
        })
        .collect()
}

fn rewrite_projection_expr_for_derived_input(
    expr: Expr,
    input_schema: &datafusion::common::DFSchemaRef,
    projection_input_schema: &SchemaRef,
) -> Result<Expr> {
    expr.transform_up(|expr| match expr {
        Expr::Column(column) => {
            let idx = input_schema.index_of_column(&column)?;
            let field = projection_input_schema.field(idx);
            Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                field.name().clone(),
            ))))
        }
        other => Ok(Transformed::no(other)),
    })
    .map(|result| result.data)
    .map_err(anyhow::Error::new)
}

fn scan_plan_for_derived_input(input_name: &str, schema: &SchemaRef) -> Result<LogicalPlan> {
    let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(schema)));
    LogicalPlanBuilder::scan(
        input_name,
        provider_as_source(provider as Arc<dyn TableProvider>),
        None,
    )?
    .build()
    .map_err(Into::into)
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

fn rewrap_record_batches_with_schema(
    batches: &[RecordBatch],
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    batches
        .iter()
        .map(|batch| {
            if batch.num_columns() != schema.fields().len() {
                bail!(
                    "grouped-max derived input batch width {} does not match schema width {}",
                    batch.num_columns(),
                    schema.fields().len()
                );
            }
            for (idx, field) in schema.fields().iter().enumerate() {
                let actual_type = batch.column(idx).data_type();
                if actual_type != field.data_type() {
                    bail!(
                        "grouped-max derived input column {} type {:?} does not match expected {:?}",
                        idx,
                        actual_type,
                        field.data_type()
                    );
                }
            }
            RecordBatch::try_new(Arc::clone(schema), batch.columns().to_vec()).map_err(Into::into)
        })
        .collect()
}

fn df_schema_to_arrow(schema: &datafusion::common::DFSchemaRef) -> Result<SchemaRef> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

fn row_converter_for_schema(schema: &SchemaRef) -> Result<RowConverter> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| SortField::new(field.data_type().clone()))
        .collect::<Vec<_>>();
    RowConverter::new(fields).context("build grouped-max Arrow row converter")
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

fn merge_group_key_bounds_update(update: &mut Option<GroupKeyBoundsUpdate>, group_key: &[u8]) {
    match update {
        Some(update) => {
            if group_key < update.min.as_slice() {
                update.min = group_key.to_vec();
            }
            if group_key > update.max.as_slice() {
                update.max = group_key.to_vec();
            }
        }
        None => {
            *update = Some(GroupKeyBoundsUpdate {
                min: group_key.to_vec(),
                max: group_key.to_vec(),
            });
        }
    }
}

fn encode_group_key_bounds(bounds: &GroupKeyBoundsState) -> Result<Vec<u8>> {
    let GroupKeyBoundsState::Present { min, max } = bounds else {
        bail!("grouped-max can only persist present group key bounds");
    };
    let min_len = u32::try_from(min.len()).context("grouped-max min group key too large")?;
    let max_len = u32::try_from(max.len()).context("grouped-max max group key too large")?;
    let mut out = Vec::with_capacity(8 + min.len() + max.len());
    out.extend_from_slice(&min_len.to_be_bytes());
    out.extend_from_slice(min);
    out.extend_from_slice(&max_len.to_be_bytes());
    out.extend_from_slice(max);
    Ok(out)
}

fn decode_group_key_bounds(bytes: &[u8]) -> Result<GroupKeyBoundsState> {
    let mut cursor = 0;
    let min_len = read_u32_at(bytes, &mut cursor)? as usize;
    let min = read_bytes_at(bytes, &mut cursor, min_len, "grouped-max min group key")?.to_vec();
    let max_len = read_u32_at(bytes, &mut cursor)? as usize;
    let max = read_bytes_at(bytes, &mut cursor, max_len, "grouped-max max group key")?.to_vec();
    if cursor != bytes.len() {
        bail!("grouped-max group key bounds payload has trailing bytes");
    }
    Ok(GroupKeyBoundsState::Present { min, max })
}

fn read_u32_at(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let chunk = read_bytes_at(bytes, cursor, 4, "grouped-max u32")?;
    let value = <[u8; 4]>::try_from(chunk)
        .map(u32::from_be_bytes)
        .map_err(|_| anyhow::anyhow!("grouped-max u32 expected 4 bytes"))?;
    Ok(value)
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

fn encode_i64_sortable(value: i64) -> [u8; 8] {
    ((value as u64) ^ (1 << 63)).to_be_bytes()
}

fn decode_i64_sortable(bytes: &[u8]) -> Result<i64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-max value key suffix must be 8 bytes"))?;
    Ok((u64::from_be_bytes(bytes) ^ (1 << 63)) as i64)
}

fn decode_i64(bytes: &[u8]) -> Result<i64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-max state value must be 8 bytes"))?;
    Ok(i64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use slatedb::Db;

    struct CountingTable {
        inner: Arc<dyn KeyValueTable>,
        get_bytes_calls: AtomicUsize,
    }

    impl CountingTable {
        fn new(inner: Arc<dyn KeyValueTable>) -> Self {
            Self {
                inner,
                get_bytes_calls: AtomicUsize::new(0),
            }
        }

        fn reset_get_bytes_calls(&self) {
            self.get_bytes_calls.store(0, Ordering::Relaxed);
        }

        fn get_bytes_calls(&self) -> usize {
            self.get_bytes_calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl KeyValueTable for CountingTable {
        async fn get_bytes(&self, key: &[u8]) -> Result<Option<Bytes>> {
            self.get_bytes_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.get_bytes(key).await
        }

        async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
            self.inner.write_batch(batch).await
        }

        async fn scan_range_bytes(
            &self,
            range: Range<Vec<u8>>,
            options: &ScanOptions,
        ) -> Result<Vec<(Bytes, Bytes)>> {
            self.inner.scan_range_bytes(range, options).await
        }
    }

    async fn build_table(name: &str) -> Arc<dyn KeyValueTable> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
        Arc::new(dbsp::storage::SlateTable::new(db))
    }

    #[tokio::test]
    async fn grouped_max_skips_summary_reads_outside_persisted_bounds() {
        let inner = build_table("grouped-max-key-bounds").await;
        let counting = Arc::new(CountingTable::new(inner));
        let table: Arc<dyn KeyValueTable> = counting.clone();
        let state = SlateGroupedMaxState::new(Arc::clone(&table), "grouped_max", true)
            .await
            .expect("state");

        counting.reset_get_bytes_calls();
        assert_eq!(state.load_max(&[1]).await.expect("fresh empty load"), None);
        assert_eq!(counting.get_bytes_calls(), 0);

        let mut batch = WriteBatch::new();
        let mut bounds_update = None;
        state
            .write_value_count(&mut batch, &[10], 100, 1)
            .expect("write count 10");
        state
            .write_max(&mut batch, &[10], Some(100))
            .expect("write max 10");
        merge_group_key_bounds_update(&mut bounds_update, &[10]);
        state
            .write_value_count(&mut batch, &[20], 200, 1)
            .expect("write count 20");
        state
            .write_max(&mut batch, &[20], Some(200))
            .expect("write max 20");
        merge_group_key_bounds_update(&mut bounds_update, &[20]);
        state
            .write_group_bounds(&mut batch, bounds_update.as_ref())
            .expect("write bounds");
        table.write_batch(batch).await.expect("persist state");

        counting.reset_get_bytes_calls();
        assert_eq!(state.load_max(&[1]).await.expect("below bounds"), None);
        assert_eq!(counting.get_bytes_calls(), 0);

        counting.reset_get_bytes_calls();
        assert_eq!(state.load_max(&[30]).await.expect("above bounds"), None);
        assert_eq!(counting.get_bytes_calls(), 0);

        counting.reset_get_bytes_calls();
        assert_eq!(
            state.load_max(&[10]).await.expect("inside bounds"),
            Some(100)
        );
        assert_eq!(counting.get_bytes_calls(), 1);

        let reopened = SlateGroupedMaxState::new(Arc::clone(&table), "grouped_max", false)
            .await
            .expect("reopened state");
        counting.reset_get_bytes_calls();
        assert_eq!(
            reopened
                .load_max(&[30])
                .await
                .expect("reopened above bounds"),
            None
        );
        assert_eq!(counting.get_bytes_calls(), 0);
    }

    #[tokio::test]
    async fn grouped_max_missing_bounds_remains_conservative() {
        let inner = build_table("grouped-max-missing-key-bounds").await;
        let counting = Arc::new(CountingTable::new(inner));
        let table: Arc<dyn KeyValueTable> = counting.clone();
        let state = SlateGroupedMaxState::new(Arc::clone(&table), "grouped_max", true)
            .await
            .expect("state");
        let mut batch = WriteBatch::new();
        let mut bounds_update = None;
        state
            .write_value_count(&mut batch, &[10], 100, 1)
            .expect("write count");
        state
            .write_max(&mut batch, &[10], Some(100))
            .expect("write max");
        merge_group_key_bounds_update(&mut bounds_update, &[10]);
        state
            .write_group_bounds(&mut batch, bounds_update.as_ref())
            .expect("write bounds");
        table.write_batch(batch).await.expect("persist state");
        table
            .delete(&state.bounds_key)
            .await
            .expect("delete bounds");

        let reopened = SlateGroupedMaxState::new(Arc::clone(&table), "grouped_max", false)
            .await
            .expect("reopened state");
        counting.reset_get_bytes_calls();
        assert_eq!(
            reopened.load_max(&[30]).await.expect("unknown bounds load"),
            None
        );
        assert_eq!(counting.get_bytes_calls(), 1);
    }
}
