use std::collections::{HashMap, hash_map::Entry};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, Int64Builder, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::logical_expr::logical_plan::{Aggregate, Projection};
use datafusion::logical_expr::{Expr, LogicalPlan, ScalarUDF};
use datafusion::physical_plan::{ExecutionPlan, collect};
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarIndexedZSet, SlateBackedColumnarZSet};
use dbsp::storage::{KeyValueTable, keyspace};
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

use crate::delta_consolidation::{add_weight_column_to_batches, weighted_snapshot_schema};
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::scalar_array_builder::ScalarColumnBuilder;
use crate::table_provider::DynamicStateTableProvider;
use crate::vectorized_runtime::source_state::incremental_source_for_plan;
use crate::vectorized_source_delta::{insert_only_source_delta_batch, unit_source_delta_batches};

use super::columnar_join::{
    ColumnarJoinMaterializedViewState, ColumnarJoinPlan,
    build_columnar_join_materialized_view_state_in_namespace_delta_only_with_persistent_inputs,
    columnar_join_plan_for_plan, columnar_join_plan_sources_append_only,
    run_columnar_join_state_tick_delta_only,
};
use super::profile;
use super::{
    IncrementalMaterializedViewState, VectorizedMaterializedViewState, VectorizedSourceState,
    apply_weighted_snapshot_delta, build_incremental_materialized_view_state_from_logical_plan,
    collect_incremental_output, direct_project_record_batches, direct_projection_indices,
    normalize_batches,
};

const APPEND_ONLY_GROUPED_MAX_STREAMING_ROW_LIMIT: usize = 8_192;

const SUMMARY_TAG: u8 = b's';
const SUMMARY_LOG_TAG: u8 = b'l';
const SUMMARY_SEQUENCE_TAG: u8 = b'q';
const SUMMARY_BUCKET_COUNT: u16 = 16;

pub(super) struct ColumnarGroupedMaxPlan {
    input: ColumnarGroupedMaxInputPlan,
    append_only_input: bool,
    projection: Projection,
    projection_schema: SchemaRef,
    group_schema: SchemaRef,
    output_mapping: Vec<usize>,
    max_idx: usize,
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
        preprojected: bool,
    },
}

pub(super) struct ColumnarGroupedMaxMaterializedViewState {
    input_name: String,
    append_only_input: bool,
    source_schema: SchemaRef,
    input_zset: Option<SlateBackedColumnarZSet>,
    join: Option<Box<ColumnarJoinMaterializedViewState>>,
    input_snapshot: Vec<RecordBatch>,
    output_zset: SlateBackedColumnarZSet,
    max_state: SlateGroupedMaxState,
    value_index: SlateBackedColumnarIndexedZSet,
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
    Direct(Vec<usize>),
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
    summary_sequence_key: Vec<u8>,
    next_summary_segment_id: Mutex<u64>,
    group_bounds: Mutex<GroupKeyBoundsState>,
    max_summaries: Mutex<HashMap<Vec<u8>, i64>>,
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

struct PendingMaxDelta {
    groups: HashMap<Vec<u8>, PendingMaxGroupDelta>,
    projected_delta: ColumnarZSet,
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

    let mut projection_expr = aggregate.group_expr.clone();
    projection_expr.push(max_value_expr);
    let (input, append_only_input, value_projection) =
        if let Some(source_name) = incremental_source_for_plan(aggregate.input.as_ref(), sources) {
            let append_only_input = sources
                .get(&source_name)
                .is_some_and(|source| source.append_only);
            let projection_input = aggregate.input.as_ref().clone();
            let value_projection = Projection::try_new(projection_expr, Arc::new(projection_input))
                .context("build grouped-max value projection")?;
            (
                ColumnarGroupedMaxInputPlan::Source { source_name },
                append_only_input,
                value_projection,
            )
        } else {
            let projected_join = Projection::try_new(
                projection_expr.clone(),
                Arc::new(aggregate.input.as_ref().clone()),
            )
            .context("build grouped-max projected join input")?;
            let projected_join_plan = LogicalPlan::Projection(projected_join.clone());
            let Some(join) = columnar_join_plan_for_plan(&projected_join_plan, sources)? else {
                return Ok(None);
            };
            let append_only_input = columnar_join_plan_sources_append_only(&join, sources);
            let projection_schema = df_schema_to_arrow(&projected_join.schema)?;
            let projection_input_schema = derived_projection_input_schema(&projection_schema);
            let input_name = derived_relation_name(aggregate.input.as_ref())
                .unwrap_or_else(|| "__floe_grouped_max_join_input".to_string());
            (
                ColumnarGroupedMaxInputPlan::Join {
                    input_name,
                    source_schema: Arc::clone(&projection_schema),
                    projection_input_schema,
                    plan: Box::new(join),
                    preprojected: true,
                },
                append_only_input,
                projected_join,
            )
        };
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
        append_only_input,
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
    let value_index_namespace = format!("{mv_namespace}/columnar/grouped_max/value_index");
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
                preprojected,
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
                let projection_delta = if preprojected {
                    GroupedMaxProjectionState::Direct(
                        (0..plan.projection_schema.fields().len()).collect(),
                    )
                } else {
                    GroupedMaxProjectionState::Derived(
                        build_derived_projection_state(
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
                        })?,
                    )
                };
                (
                    input_name,
                    source_schema,
                    None,
                    Some(join),
                    input_snapshot,
                    projection_delta,
                )
            }
        };

    let assume_empty_state = output_zset.current_handle().is_none();

    let group_key_indices = (0..plan.max_idx).collect::<Vec<_>>();
    let value_index = SlateBackedColumnarIndexedZSet::new(
        Arc::clone(&table),
        value_index_namespace,
        Arc::clone(&plan.projection_schema),
        group_key_indices,
    )
    .await
    .context("initialize SlateDB-backed grouped-max value index")?;

    Ok(ColumnarGroupedMaxMaterializedViewState {
        input_name,
        append_only_input: plan.append_only_input,
        source_schema,
        input_zset,
        join,
        input_snapshot,
        output_zset,
        max_state: SlateGroupedMaxState::new(table, &state_namespace, assume_empty_state).await?,
        value_index,
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
        Box::pin(
            build_columnar_join_materialized_view_state_in_namespace_delta_only_with_persistent_inputs(
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
) -> Result<()> {
    let super::MaterializedViewOperator::GroupedMax(columnar) = &mut mv.operator else {
        unreachable!("grouped-max tick dispatched to another operator")
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
    Ok(())
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
    let total_start = profile::start();
    let phase_start = profile::start();
    let prepare_start = Instant::now();
    let persisted_input_delta =
        prepare_grouped_max_input_delta(columnar, insert_batches, weighted_delta_batches).await?;
    let prepare_ms = prepare_start.elapsed().as_millis() as u64;
    profile::record_since("grouped_max.prepare_input", phase_start);
    let input_changed = !persisted_input_delta.batches().is_empty();
    let streaming_start = Instant::now();
    let streaming_output =
        grouped_max_append_only_streaming_delta(columnar, persisted_input_delta.batches()).await?;
    let (pending_group_count, projected_delta_rows, pending_ms, output_delta_batches, apply_ms) =
        if let Some(output_delta_batches) = streaming_output {
            (
                0,
                0,
                0,
                output_delta_batches,
                streaming_start.elapsed().as_millis() as u64,
            )
        } else {
            let phase_start = profile::start();
            let pending_start = Instant::now();
            let pending =
                grouped_max_pending_delta(columnar, persisted_input_delta.batches()).await?;
            let pending_group_count = pending.groups.len();
            let projected_delta_rows = pending
                .projected_delta
                .batches()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>();
            let pending_ms = pending_start.elapsed().as_millis() as u64;
            profile::record_since("grouped_max.pending_delta", phase_start);
            let phase_start = profile::start();
            let apply_start = Instant::now();
            let output_delta_batches = apply_grouped_max_delta(columnar, pending).await?;
            let apply_ms = apply_start.elapsed().as_millis() as u64;
            profile::record_since("grouped_max.apply_delta", phase_start);
            (
                pending_group_count,
                projected_delta_rows,
                pending_ms,
                output_delta_batches,
                apply_ms,
            )
        };
    let phase_start = profile::start();
    let build_output_start = Instant::now();
    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build grouped-max output zset delta")?;
    let build_output_ms = build_output_start.elapsed().as_millis() as u64;
    profile::record_since("grouped_max.build_output_zset", phase_start);
    let input_delta_rows = persisted_input_delta
        .batches()
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    let output_delta_rows = output_delta
        .batches()
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    let mut output_create_ms = 0_u64;
    if maintain_output_snapshot {
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
        profile::record_since("grouped_max.output_create_version", phase_start);
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
        .context("apply Slate-backed grouped-max columnar snapshot delta")?;
        output_snapshot_ms = output_snapshot_start.elapsed().as_millis() as u64;
        profile::record_since("grouped_max.output_snapshot_delta", phase_start);
        next_snapshot
    } else {
        Vec::new()
    };

    tracing::debug!(
        input = %columnar.input_name,
        input_delta_rows,
        pending_group_count,
        projected_delta_rows,
        output_delta_rows,
        prepare_ms,
        pending_ms,
        apply_ms,
        build_output_ms,
        output_create_ms,
        output_snapshot_ms,
        maintain_output_snapshot,
        "grouped-max state tick phase timings"
    );

    profile::record_since("grouped_max.total", total_start);
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
    let join_start = Instant::now();
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
    tracing::debug!(
        input = %columnar.input_name,
        join_ms = join_start.elapsed().as_millis() as u64,
        delta_rows = tick
            .delta
            .batches()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        "grouped-max nested join input prepared"
    );
    if tick.input_changed && !tick.next_snapshot.is_empty() {
        columnar.input_snapshot = tick.next_snapshot;
    }
    Ok(tick.delta)
}

async fn grouped_max_append_only_streaming_delta(
    columnar: &mut ColumnarGroupedMaxMaterializedViewState,
    input_batches: &[RecordBatch],
) -> Result<Option<Vec<RecordBatch>>> {
    if !columnar.append_only_input {
        return Ok(None);
    }
    let input_row_count = input_batches
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    if input_row_count == 0 {
        return Ok(Some(Vec::new()));
    }
    if input_row_count > APPEND_ONLY_GROUPED_MAX_STREAMING_ROW_LIMIT {
        return Ok(None);
    }

    let phase_start = profile::start();
    let mut positive_source_batches = Vec::new();
    for batch in input_batches {
        let Some(insert_batch) = insert_only_source_delta_batch(&columnar.source_schema, batch)?
        else {
            return Ok(None);
        };
        positive_source_batches.push(insert_batch);
    }
    profile::record_since("grouped_max.pending_split_source_delta", phase_start);

    let phase_start = profile::start();
    let positive_output =
        collect_grouped_max_projection_output(columnar, &positive_source_batches).await?;
    profile::record_since("grouped_max.pending_project_positive", phase_start);
    if positive_output.iter().all(|batch| batch.num_rows() == 0) {
        return Ok(Some(Vec::new()));
    }

    let phase_start = profile::start();
    let mut builder = WeightedMaxOutputBuilder::new(
        columnar.output_zset.value_schema(),
        &columnar.output_mapping,
    )?;
    let converter = row_converter_for_schema(&columnar.group_schema)?;
    let mut current_maxes = HashMap::<Vec<u8>, Option<i64>>::with_capacity(input_row_count);
    let mut max_updates = Vec::new();
    let mut bounds_update = None;

    for batch in &positive_output {
        if batch.num_rows() == 0 {
            continue;
        }
        let group_columns = (0..columnar.max_idx)
            .map(|idx| Arc::clone(batch.column(idx)))
            .collect::<Vec<ArrayRef>>();
        let group_rows = converter
            .convert_columns(&group_columns)
            .context("encode grouped-max streaming group keys")?;
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
            let group_key = group_rows.row(row_idx).data().to_vec();
            let old_max = match current_maxes.get(&group_key) {
                Some(max) => *max,
                None => columnar.max_state.load_max(&group_key)?,
            };
            let new_max = match old_max {
                Some(old_max) if old_max >= value => Some(old_max),
                _ => Some(value),
            };
            current_maxes.insert(group_key.clone(), new_max);
            if old_max != new_max {
                if let Some(old_max) = old_max {
                    builder.append(&batch, row_idx, columnar.max_idx, old_max, -1)?;
                }
                if let Some(new_max) = new_max {
                    builder.append(&batch, row_idx, columnar.max_idx, new_max, 1)?;
                }
                max_updates.push((group_key.clone(), new_max));
            }
            merge_group_key_bounds_update(&mut bounds_update, &group_key);
        }
    }
    profile::record_since("grouped_max.apply_update_loop", phase_start);

    let phase_start = profile::start();
    let mut writes = WriteBatch::new();
    columnar
        .max_state
        .write_max_updates(&mut writes, &max_updates)?;
    columnar
        .max_state
        .write_group_bounds(&mut writes, bounds_update.as_ref())?;
    profile::record_since("grouped_max.apply_write_batch_build_tail", phase_start);

    let phase_start = profile::start();
    columnar
        .max_state
        .table
        .write_batch(writes)
        .await
        .context("persist streaming append-only grouped-max state updates")?;
    profile::record_since("grouped_max.apply_write_batch", phase_start);

    let phase_start = profile::start();
    columnar.max_state.apply_max_updates(max_updates)?;
    profile::record_since("grouped_max.apply_summary_cache_update", phase_start);

    let phase_start = profile::start();
    let output = builder.finish()?;
    profile::record_since("grouped_max.apply_finish_output", phase_start);
    Ok(Some(output))
}

async fn grouped_max_pending_delta(
    columnar: &ColumnarGroupedMaxMaterializedViewState,
    input_batches: &[RecordBatch],
) -> Result<PendingMaxDelta> {
    let mut pending = HashMap::new();
    if input_batches.is_empty() {
        return Ok(PendingMaxDelta {
            groups: pending,
            projected_delta: ColumnarZSet::empty(Arc::clone(&columnar.projection_schema))?,
        });
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

    let weighted_schema = weighted_snapshot_schema(&columnar.projection_schema)?;
    let mut weighted_projected =
        add_weight_column_to_batches(&positive_output, &weighted_schema, 1)
            .context("build grouped-max positive projected value delta")?;
    weighted_projected.extend(
        add_weight_column_to_batches(&negative_output, &weighted_schema, -1)
            .context("build grouped-max negative projected value delta")?,
    );
    let projected_delta =
        ColumnarZSet::try_new_weighted(Arc::clone(&columnar.projection_schema), weighted_projected)
            .context("build grouped-max projected value zset delta")?;

    Ok(PendingMaxDelta {
        groups: pending,
        projected_delta,
    })
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
        GroupedMaxProjectionState::Direct(indices) => direct_project_record_batches(
            source_batches,
            &columnar.projection_schema,
            indices,
            "grouped-max",
        ),
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
    columnar: &mut ColumnarGroupedMaxMaterializedViewState,
    pending: PendingMaxDelta,
) -> Result<Vec<RecordBatch>> {
    let total_start = profile::start();
    let mut builder = WeightedMaxOutputBuilder::new(
        columnar.output_zset.value_schema(),
        &columnar.output_mapping,
    )?;
    if pending.groups.is_empty() {
        let phase_start = profile::start();
        let output = builder.finish();
        profile::record_since("grouped_max.apply_finish_output", phase_start);
        profile::record_since("grouped_max.apply_total_inner", total_start);
        return output;
    }
    if columnar.append_only_input && columnar_zset_is_insert_only(&pending.projected_delta)? {
        return apply_append_only_grouped_max_delta(columnar, pending, total_start).await;
    }

    let phase_start = profile::start();
    let value_index_start = Instant::now();
    columnar
        .value_index
        .apply_delta(&pending.projected_delta)
        .await
        .context("persist grouped-max projected value index delta")?;
    let value_index_ms = value_index_start.elapsed().as_millis() as u64;
    profile::record_since("grouped_max.apply_value_index", phase_start);

    let mut group_work = Vec::with_capacity(pending.groups.len());
    let mut recompute_count = 0_usize;
    for (group_key, delta) in pending.groups {
        let old_max = columnar.max_state.load_max(&group_key)?;
        let recompute = grouped_max_recompute_required(old_max, &delta);
        if recompute {
            recompute_count = recompute_count.saturating_add(1);
        }
        group_work.push((group_key, delta, old_max, recompute));
    }
    let pending_group_count = group_work.len();
    let recompute_lookup_start = Instant::now();
    let recomputed_maxes = batched_recomputed_grouped_maxes(columnar, &group_work).await?;
    let recompute_lookup_ms = recompute_lookup_start.elapsed().as_millis() as u64;

    let mut writes = WriteBatch::new();
    let mut bounds_update = None;
    let mut max_updates = Vec::new();
    let phase_start = profile::start();
    let update_loop_start = Instant::now();
    for (group_key, delta, old_max, _recompute) in group_work {
        let new_max = new_max_after_projected_delta(
            old_max,
            &delta,
            recomputed_maxes.get(&group_key).copied(),
        );
        if old_max != new_max {
            if let Some(old_max) = old_max {
                builder.append(&delta.batch, delta.row_idx, columnar.max_idx, old_max, -1)?;
            }
            if let Some(new_max) = new_max {
                builder.append(&delta.batch, delta.row_idx, columnar.max_idx, new_max, 1)?;
            }
            max_updates.push((group_key.clone(), new_max));
        }
        if new_max.is_some() {
            merge_group_key_bounds_update(&mut bounds_update, &group_key);
        }
    }
    let update_loop_ms = update_loop_start.elapsed().as_millis() as u64;
    profile::record_since("grouped_max.apply_update_loop", phase_start);
    let phase_start = profile::start();
    let write_build_start = Instant::now();
    columnar
        .max_state
        .write_max_updates(&mut writes, &max_updates)?;
    columnar
        .max_state
        .write_group_bounds(&mut writes, bounds_update.as_ref())?;
    let write_build_ms = write_build_start.elapsed().as_millis() as u64;
    profile::record_since("grouped_max.apply_write_batch_build_tail", phase_start);
    let phase_start = profile::start();
    let write_start = Instant::now();
    columnar
        .max_state
        .table
        .write_batch(writes)
        .await
        .context("persist grouped-max state updates")?;
    let write_ms = write_start.elapsed().as_millis() as u64;
    profile::record_since("grouped_max.apply_write_batch", phase_start);
    let phase_start = profile::start();
    let cache_start = Instant::now();
    let max_update_count = max_updates.len();
    columnar.max_state.apply_max_updates(max_updates)?;
    let cache_ms = cache_start.elapsed().as_millis() as u64;
    profile::record_since("grouped_max.apply_summary_cache_update", phase_start);
    let phase_start = profile::start();
    let finish_start = Instant::now();
    let output = builder.finish();
    let finish_ms = finish_start.elapsed().as_millis() as u64;
    profile::record_since("grouped_max.apply_finish_output", phase_start);
    tracing::debug!(
        pending_group_count,
        recompute_count,
        max_update_count,
        value_index_ms,
        recompute_lookup_ms,
        update_loop_ms,
        write_build_ms,
        write_ms,
        cache_ms,
        finish_ms,
        "grouped-max apply phase timings"
    );
    profile::record_since("grouped_max.apply_total_inner", total_start);
    output
}

async fn apply_append_only_grouped_max_delta(
    columnar: &mut ColumnarGroupedMaxMaterializedViewState,
    pending: PendingMaxDelta,
    total_start: Option<Instant>,
) -> Result<Vec<RecordBatch>> {
    let mut builder = WeightedMaxOutputBuilder::new(
        columnar.output_zset.value_schema(),
        &columnar.output_mapping,
    )?;
    let mut writes = WriteBatch::new();
    let mut bounds_update = None;
    let mut max_updates = Vec::new();
    let phase_start = profile::start();
    let update_loop_start = Instant::now();
    let group_work = pending.groups.into_iter().collect::<Vec<_>>();
    let old_maxes = columnar
        .max_state
        .load_maxes(group_work.iter().map(|(group_key, _)| group_key))?;
    for ((group_key, delta), old_max) in group_work.into_iter().zip(old_maxes) {
        let max_added = max_added_for_delta(&delta);
        let new_max = match (old_max, max_added) {
            (None, max_added) => max_added,
            (Some(old_max), Some(max_added)) if max_added > old_max => Some(max_added),
            (Some(old_max), _) => Some(old_max),
        };
        if old_max != new_max {
            if let Some(old_max) = old_max {
                builder.append(&delta.batch, delta.row_idx, columnar.max_idx, old_max, -1)?;
            }
            if let Some(new_max) = new_max {
                builder.append(&delta.batch, delta.row_idx, columnar.max_idx, new_max, 1)?;
            }
            max_updates.push((group_key.clone(), new_max));
        }
        if new_max.is_some() {
            merge_group_key_bounds_update(&mut bounds_update, &group_key);
        }
    }
    let update_loop_ms = update_loop_start.elapsed().as_millis() as u64;
    profile::record_since("grouped_max.apply_update_loop", phase_start);

    let phase_start = profile::start();
    let write_build_start = Instant::now();
    columnar
        .max_state
        .write_max_updates(&mut writes, &max_updates)?;
    columnar
        .max_state
        .write_group_bounds(&mut writes, bounds_update.as_ref())?;
    let write_build_ms = write_build_start.elapsed().as_millis() as u64;
    profile::record_since("grouped_max.apply_write_batch_build_tail", phase_start);

    let phase_start = profile::start();
    let write_start = Instant::now();
    columnar
        .max_state
        .table
        .write_batch(writes)
        .await
        .context("persist append-only grouped-max state updates")?;
    let write_ms = write_start.elapsed().as_millis() as u64;
    profile::record_since("grouped_max.apply_write_batch", phase_start);

    let phase_start = profile::start();
    let cache_start = Instant::now();
    let max_update_count = max_updates.len();
    columnar.max_state.apply_max_updates(max_updates)?;
    let cache_ms = cache_start.elapsed().as_millis() as u64;
    profile::record_since("grouped_max.apply_summary_cache_update", phase_start);

    let phase_start = profile::start();
    let finish_start = Instant::now();
    let output = builder.finish()?;
    let finish_ms = finish_start.elapsed().as_millis() as u64;
    profile::record_since("grouped_max.apply_finish_output", phase_start);
    tracing::debug!(
        pending_group_count = max_update_count,
        recompute_count = 0usize,
        max_update_count,
        value_index_ms = 0u64,
        recompute_lookup_ms = 0u64,
        update_loop_ms,
        write_build_ms,
        write_ms,
        cache_ms,
        finish_ms,
        mode = "append_only",
        "grouped-max apply phase timings"
    );
    profile::record_since("grouped_max.apply_total_inner", total_start);
    Ok(output)
}

fn columnar_zset_is_insert_only(delta: &ColumnarZSet) -> Result<bool> {
    if delta.is_empty() {
        return Ok(false);
    }
    let weight_idx = delta.value_column_count();
    let mut saw_insert = false;
    for batch in delta.batches() {
        let weights = batch
            .column(weight_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("grouped-max delta weight column must be Int64"))?;
        for row_idx in 0..batch.num_rows() {
            if weights.is_null(row_idx) {
                bail!("grouped-max delta weight column cannot contain NULL");
            }
            let weight = weights.value(row_idx);
            if weight < 0 {
                return Ok(false);
            }
            if weight > 0 {
                saw_insert = true;
            }
        }
    }
    Ok(saw_insert)
}

fn grouped_max_recompute_required(old_max: Option<i64>, delta: &PendingMaxGroupDelta) -> bool {
    let Some(old_max) = old_max else {
        return false;
    };
    let max_added = delta
        .value_deltas
        .iter()
        .filter_map(|(value, delta)| (*delta > 0).then_some(*value))
        .max();
    if max_added.is_some_and(|value| value > old_max) {
        return false;
    }
    delta
        .value_deltas
        .get(&old_max)
        .is_some_and(|value_delta| *value_delta < 0)
}

fn new_max_after_projected_delta(
    old_max: Option<i64>,
    delta: &PendingMaxGroupDelta,
    recomputed_max: Option<i64>,
) -> Option<i64> {
    let max_added = max_added_for_delta(delta);
    match old_max {
        None => max_added,
        Some(old_max) => {
            if max_added.is_some_and(|value| value > old_max) {
                return max_added;
            }
            let old_max_may_be_removed = delta
                .value_deltas
                .get(&old_max)
                .is_some_and(|value_delta| *value_delta < 0);
            if !old_max_may_be_removed {
                return Some(old_max);
            }
            recomputed_max
        }
    }
}

fn max_added_for_delta(delta: &PendingMaxGroupDelta) -> Option<i64> {
    delta
        .value_deltas
        .iter()
        .filter_map(|(value, delta)| (*delta > 0).then_some(*value))
        .max()
}

async fn batched_recomputed_grouped_maxes(
    columnar: &ColumnarGroupedMaxMaterializedViewState,
    group_work: &[(Vec<u8>, PendingMaxGroupDelta, Option<i64>, bool)],
) -> Result<HashMap<Vec<u8>, i64>> {
    let recompute_deltas = group_work
        .iter()
        .filter_map(|(_, delta, _, recompute)| recompute.then_some(delta))
        .collect::<Vec<_>>();
    if recompute_deltas.is_empty() {
        return Ok(HashMap::new());
    }
    let lookup_batch = group_lookup_batch_for_deltas(columnar, &recompute_deltas)?;
    let values = columnar
        .value_index
        .lookup_key_batches(&[lookup_batch])
        .await
        .context("lookup grouped-max values by group")?;
    max_by_group_from_projected_value_zset(&values, columnar.max_idx, &columnar.group_schema)
}

fn group_lookup_batch_for_deltas(
    columnar: &ColumnarGroupedMaxMaterializedViewState,
    deltas: &[&PendingMaxGroupDelta],
) -> Result<RecordBatch> {
    let mut builders = columnar
        .group_schema
        .fields()
        .iter()
        .map(|field| ScalarColumnBuilder::new(field.data_type(), deltas.len()))
        .collect::<Result<Vec<_>>>()?;
    for delta in deltas {
        for (idx, builder) in builders.iter_mut().enumerate() {
            builder
                .append_array_value(delta.batch.column(idx).as_ref(), delta.row_idx)
                .with_context(|| format!("append grouped-max lookup key column {idx}"))?;
        }
    }
    let columns = builders
        .iter_mut()
        .map(ScalarColumnBuilder::finish_array)
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::clone(&columnar.group_schema), columns)
        .context("build grouped-max group lookup batch")
}

fn max_by_group_from_projected_value_zset(
    zset: &ColumnarZSet,
    max_idx: usize,
    group_schema: &SchemaRef,
) -> Result<HashMap<Vec<u8>, i64>> {
    let mut counts = HashMap::<Vec<u8>, HashMap<i64, i64>>::new();
    let converter = row_converter_for_schema(group_schema)?;
    for batch in zset.batches() {
        if batch.num_rows() == 0 {
            continue;
        }
        let group_columns = (0..max_idx)
            .map(|idx| Arc::clone(batch.column(idx)))
            .collect::<Vec<ArrayRef>>();
        let group_rows = converter
            .convert_columns(&group_columns)
            .context("encode grouped-max recompute lookup keys")?;
        let values = batch
            .column(max_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("grouped-max indexed value must be Int64"))?;
        let weights = batch
            .column(max_idx + 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("grouped-max indexed weight must be Int64"))?;
        for row_idx in 0..batch.num_rows() {
            if values.is_null(row_idx) {
                continue;
            }
            let group_key = group_rows.row(row_idx).data().to_vec();
            let value = values.value(row_idx);
            let weight = weights.value(row_idx);
            let group_counts = counts.entry(group_key).or_default();
            let count = group_counts.entry(value).or_insert(0_i64);
            let next = (*count)
                .checked_add(weight)
                .ok_or_else(|| anyhow::anyhow!("grouped-max indexed count overflow"))?;
            *count = next;
        }
    }
    Ok(counts
        .into_iter()
        .filter_map(|(group_key, value_counts)| {
            value_counts
                .into_iter()
                .filter_map(|(value, count)| (count > 0).then_some(value))
                .max()
                .map(|max| (group_key, max))
        })
        .collect())
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
        let mut summary_sequence_key = key_prefix.clone();
        summary_sequence_key.push(SUMMARY_SEQUENCE_TAG);
        let group_bounds = match table
            .get_bytes(&bounds_key)
            .await
            .context("read grouped-max group key bounds")?
        {
            Some(bytes) => decode_group_key_bounds(bytes.as_ref())?,
            None if assume_empty => GroupKeyBoundsState::Empty,
            None => GroupKeyBoundsState::Unknown,
        };
        let next_summary_segment_id =
            read_summary_sequence(table.as_ref(), &summary_sequence_key).await?;
        let state = Self {
            table,
            key_prefix,
            bounds_key,
            summary_sequence_key,
            next_summary_segment_id: Mutex::new(next_summary_segment_id),
            group_bounds: Mutex::new(group_bounds),
            max_summaries: Mutex::new(HashMap::new()),
        };
        let max_summaries = state
            .load_all_max_summaries()
            .await
            .context("load grouped-max summary head")?;
        *state
            .max_summaries
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-max max summary head poisoned"))? = max_summaries;
        Ok(state)
    }

    fn load_max(&self, group_key: &[u8]) -> Result<Option<i64>> {
        if !self.group_key_may_exist(group_key)? {
            return Ok(None);
        }
        let max_summaries = self
            .max_summaries
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-max max summary head poisoned"))?;
        Ok(max_summaries.get(group_key).copied())
    }

    fn load_maxes<'a>(
        &self,
        group_keys: impl IntoIterator<Item = &'a Vec<u8>>,
    ) -> Result<Vec<Option<i64>>> {
        let bounds = self
            .group_bounds
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-max group bounds poisoned"))?;
        let max_summaries = self
            .max_summaries
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-max max summary head poisoned"))?;
        Ok(group_keys
            .into_iter()
            .map(|group_key| {
                if group_key_may_exist_in_bounds(&bounds, group_key) {
                    max_summaries.get(group_key).copied()
                } else {
                    None
                }
            })
            .collect())
    }

    async fn load_all_max_summaries(&self) -> Result<HashMap<Vec<u8>, i64>> {
        let summary_prefix = self.tag_prefix(SUMMARY_TAG);
        let mut values = HashMap::new();
        for (key, value_bytes) in self
            .table
            .scan_prefix(&summary_prefix, &ScanOptions::default())
            .await
            .context("scan grouped-max summary state")?
        {
            let group_key = self.group_key_from_tagged_state_key(SUMMARY_TAG, &key)?;
            values.insert(group_key.to_vec(), decode_i64(&value_bytes)?);
        }
        let summary_log_prefix = self.tag_prefix(SUMMARY_LOG_TAG);
        let mut log_entries = Vec::new();
        for (key, value_bytes) in self
            .table
            .scan_prefix(&summary_log_prefix, &ScanOptions::default())
            .await
            .context("scan grouped-max summary log")?
        {
            log_entries.push((self.summary_log_segment_id(&key)?, value_bytes));
        }
        log_entries.sort_by_key(|(segment_id, _)| *segment_id);
        for (_, value_bytes) in log_entries {
            for (group_key, max) in decode_summary_log_updates(value_bytes.as_ref())? {
                if let Some(max) = max {
                    values.insert(group_key, max);
                } else {
                    values.remove(group_key.as_slice());
                }
            }
        }
        Ok(values)
    }

    fn write_max_updates(
        &self,
        batch: &mut WriteBatch,
        updates: &[(Vec<u8>, Option<i64>)],
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let mut next_segment_id = self
            .next_summary_segment_id
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-max summary sequence poisoned"))?;
        let segment_id = *next_segment_id;
        *next_segment_id = next_segment_id.saturating_add(1);

        let mut buckets: HashMap<u16, Vec<(Vec<u8>, Option<i64>)>> = HashMap::new();
        for (group_key, max) in updates {
            buckets
                .entry(summary_bucket(group_key))
                .or_default()
                .push((group_key.clone(), *max));
        }
        for (bucket, mut bucket_updates) in buckets {
            bucket_updates.sort_by(|(left, _), (right, _)| left.cmp(right));
            batch.put(
                self.summary_log_key(bucket, segment_id),
                encode_summary_log_updates(&bucket_updates)?,
            );
        }
        batch.put(
            self.summary_sequence_key.clone(),
            (*next_segment_id).to_be_bytes(),
        );
        Ok(())
    }

    fn apply_max_updates(&self, updates: Vec<(Vec<u8>, Option<i64>)>) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let mut max_summaries = self
            .max_summaries
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-max max summary head poisoned"))?;
        for (group_key, max) in updates {
            if let Some(max) = max {
                max_summaries.insert(group_key, max);
            } else {
                max_summaries.remove(group_key.as_slice());
            }
        }
        Ok(())
    }

    fn group_key_may_exist(&self, group_key: &[u8]) -> Result<bool> {
        let bounds = self
            .group_bounds
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-max group bounds poisoned"))?;
        Ok(group_key_may_exist_in_bounds(&bounds, group_key))
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

    fn tag_prefix(&self, tag: u8) -> Vec<u8> {
        let mut prefix = self.key_prefix.clone();
        prefix.push(tag);
        prefix
    }

    fn summary_log_key(&self, bucket: u16, segment_id: u64) -> Vec<u8> {
        let mut key = self.tag_prefix(SUMMARY_LOG_TAG);
        key.extend_from_slice(&bucket.to_be_bytes());
        key.extend_from_slice(&segment_id.to_be_bytes());
        key
    }

    fn summary_log_segment_id(&self, key: &[u8]) -> Result<u64> {
        let prefix_len = self.key_prefix.len() + 1 + 2;
        if key.len() != prefix_len + 8
            || !key.starts_with(&self.key_prefix)
            || key.get(self.key_prefix.len()) != Some(&SUMMARY_LOG_TAG)
        {
            bail!("grouped-max summary log key prefix mismatch");
        }
        let bytes = key
            .get(prefix_len..prefix_len + 8)
            .ok_or_else(|| anyhow::anyhow!("grouped-max summary log segment id truncated"))?;
        Ok(u64::from_be_bytes(bytes.try_into()?))
    }

    fn group_key_from_tagged_state_key<'a>(&self, tag: u8, key: &'a [u8]) -> Result<&'a [u8]> {
        let tag_index = self.key_prefix.len();
        if !key.starts_with(&self.key_prefix) || key.get(tag_index) != Some(&tag) {
            bail!("grouped-max state key prefix mismatch");
        }
        let mut cursor = tag_index + 1;
        let group_key_len = read_u32_at(key, &mut cursor)? as usize;
        let group_key = read_bytes_at(key, &mut cursor, group_key_len, "grouped-max group key")?;
        if cursor != key.len() {
            bail!("grouped-max summary state key has trailing bytes");
        }
        Ok(group_key)
    }
}

fn group_key_may_exist_in_bounds(bounds: &GroupKeyBoundsState, group_key: &[u8]) -> bool {
    match bounds {
        GroupKeyBoundsState::Unknown => true,
        GroupKeyBoundsState::Empty => false,
        GroupKeyBoundsState::Present { min, max } => {
            group_key >= min.as_slice() && group_key <= max.as_slice()
        }
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

async fn read_summary_sequence(table: &dyn KeyValueTable, key: &[u8]) -> Result<u64> {
    let Some(bytes) = table
        .get_bytes(key)
        .await
        .context("read grouped-max summary sequence")?
    else {
        return Ok(1);
    };
    let bytes: [u8; 8] = bytes
        .as_ref()
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-max summary sequence must be 8 bytes"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn summary_bucket(group_key: &[u8]) -> u16 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in group_key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % u64::from(SUMMARY_BUCKET_COUNT)) as u16
}

fn encode_summary_log_updates(updates: &[(Vec<u8>, Option<i64>)]) -> Result<Vec<u8>> {
    let mut capacity = 4;
    for (group_key, _) in updates {
        capacity += 4 + group_key.len() + 1 + 8;
    }
    let mut out = Vec::with_capacity(capacity);
    let update_count =
        u32::try_from(updates.len()).context("grouped-max summary log update count too large")?;
    out.extend_from_slice(&update_count.to_be_bytes());
    for (group_key, max) in updates {
        let group_key_len =
            u32::try_from(group_key.len()).context("grouped-max summary group key too large")?;
        out.extend_from_slice(&group_key_len.to_be_bytes());
        out.extend_from_slice(group_key);
        match max {
            Some(max) => {
                out.push(1);
                out.extend_from_slice(&max.to_be_bytes());
            }
            None => {
                out.push(0);
                out.extend_from_slice(&0_i64.to_be_bytes());
            }
        }
    }
    Ok(out)
}

fn decode_summary_log_updates(bytes: &[u8]) -> Result<Vec<(Vec<u8>, Option<i64>)>> {
    let mut cursor = 0;
    let update_count = read_u32_at(bytes, &mut cursor)?;
    let mut updates = Vec::with_capacity(update_count as usize);
    for _ in 0..update_count {
        let group_key_len = read_u32_at(bytes, &mut cursor)? as usize;
        let group_key = read_bytes_at(
            bytes,
            &mut cursor,
            group_key_len,
            "grouped-max summary group key",
        )?
        .to_vec();
        let tag = *read_bytes_at(bytes, &mut cursor, 1, "grouped-max summary update tag")?
            .first()
            .ok_or_else(|| anyhow::anyhow!("grouped-max summary update tag missing"))?;
        let max = decode_i64(read_bytes_at(
            bytes,
            &mut cursor,
            8,
            "grouped-max summary max value",
        )?)?;
        let max = match tag {
            0 => None,
            1 => Some(max),
            other => bail!("invalid grouped-max summary update tag {other}"),
        };
        updates.push((group_key, max));
    }
    if cursor != bytes.len() {
        bail!("grouped-max summary log payload has trailing bytes");
    }
    Ok(updates)
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
        assert_eq!(state.load_max(&[1]).expect("fresh empty load"), None);
        assert_eq!(counting.get_bytes_calls(), 0);

        let mut batch = WriteBatch::new();
        let mut bounds_update = None;
        let mut max_updates = Vec::new();
        state
            .write_max_updates(&mut batch, &[(vec![10], Some(100))])
            .expect("write max update 10");
        max_updates.push((vec![10], Some(100)));
        merge_group_key_bounds_update(&mut bounds_update, &[10]);
        state
            .write_max_updates(&mut batch, &[(vec![20], Some(200))])
            .expect("write max update 20");
        max_updates.push((vec![20], Some(200)));
        merge_group_key_bounds_update(&mut bounds_update, &[20]);
        state
            .write_group_bounds(&mut batch, bounds_update.as_ref())
            .expect("write bounds");
        table.write_batch(batch).await.expect("persist state");
        state
            .apply_max_updates(max_updates)
            .expect("apply max summary head updates");

        counting.reset_get_bytes_calls();
        assert_eq!(state.load_max(&[1]).expect("below bounds"), None);
        assert_eq!(counting.get_bytes_calls(), 0);

        counting.reset_get_bytes_calls();
        assert_eq!(state.load_max(&[30]).expect("above bounds"), None);
        assert_eq!(counting.get_bytes_calls(), 0);

        counting.reset_get_bytes_calls();
        assert_eq!(state.load_max(&[10]).expect("inside bounds"), Some(100));
        assert_eq!(counting.get_bytes_calls(), 0);

        let reopened = SlateGroupedMaxState::new(Arc::clone(&table), "grouped_max", false)
            .await
            .expect("reopened state");
        counting.reset_get_bytes_calls();
        assert_eq!(
            reopened.load_max(&[30]).expect("reopened above bounds"),
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
        let mut max_updates = Vec::new();
        state
            .write_max_updates(&mut batch, &[(vec![10], Some(100))])
            .expect("write max update");
        max_updates.push((vec![10], Some(100)));
        merge_group_key_bounds_update(&mut bounds_update, &[10]);
        state
            .write_group_bounds(&mut batch, bounds_update.as_ref())
            .expect("write bounds");
        table.write_batch(batch).await.expect("persist state");
        state
            .apply_max_updates(max_updates)
            .expect("apply max summary head updates");
        table
            .delete(&state.bounds_key)
            .await
            .expect("delete bounds");

        let reopened = SlateGroupedMaxState::new(Arc::clone(&table), "grouped_max", false)
            .await
            .expect("reopened state");
        counting.reset_get_bytes_calls();
        assert_eq!(reopened.load_max(&[30]).expect("unknown bounds load"), None);
        assert_eq!(counting.get_bytes_calls(), 0);
    }
}
