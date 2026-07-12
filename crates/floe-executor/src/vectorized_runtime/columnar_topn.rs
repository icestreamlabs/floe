use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    StringArray, TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::catalog::TableProvider;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::logical_plan::{Filter, Limit, Sort, Window};
use datafusion::logical_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, Operator, ScalarUDF, WindowFunctionDefinition,
};
use datafusion::physical_plan::collect;
use dbsp::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarIndexedZSet, SlateBackedColumnarZSet};
use dbsp::storage::{KeyValueTable, keyspace};
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

use crate::columnar_snapshot::columnar_zset_weight_sum;
use crate::delta_consolidation::{
    diff_bounded_output_batches, diff_bounded_output_batches_by_row, weighted_snapshot_schema,
};
use crate::mv::registry::{ColumnarMaterializedViewStorage, MaterializedViewRegistry};
use crate::namespaces;
use crate::scalar_array_builder::ScalarColumnBuilder;
use crate::table_provider::DynamicStateTableProvider;
use crate::vectorized_runtime::source_state::rename_batches;

use super::columnar_grouped_stats::{
    ColumnarGroupedStatsMaterializedViewState, ColumnarGroupedStatsPlan,
    build_columnar_grouped_stats_materialized_view_state_in_namespace,
    columnar_grouped_stats_plan_for_plan, run_columnar_grouped_stats_state_tick,
};
use super::{
    VectorizedMaterializedViewState, VectorizedSourceState, apply_keyed_source_snapshot_delta,
    apply_weighted_snapshot_delta, direct_project_record_batches, normalize_batches, profile,
};

pub(super) struct ColumnarTopNPlan {
    pub(super) logical_plan: LogicalPlan,
    input: ColumnarTopNInputPlan,
    partition_columns: Vec<String>,
    append_only_fast_path: bool,
    row_number_limit: Option<usize>,
    append_only_direct_plan: Option<AppendOnlyDirectTopNPlan>,
}

struct AppendOnlyDirectTopNPlan {
    limit: usize,
    orderings: Vec<AppendOnlyDirectTopNOrdering>,
}

struct AppendOnlyDirectTopNOrdering {
    column: String,
    asc: bool,
    nulls_first: bool,
}

enum ColumnarTopNInputPlan {
    Source {
        source_name: String,
    },
    GroupedStats {
        input_name: String,
        schema: SchemaRef,
        plan: Box<ColumnarGroupedStatsPlan>,
    },
}

pub(super) struct ColumnarTopNMaterializedViewState {
    operator_table: Arc<dyn KeyValueTable>,
    input_name: String,
    source_schema: SchemaRef,
    input: super::IncrementalInputOperator,
    output_zset: SlateBackedColumnarZSet,
    evaluator: TopNEvaluator,
    partition_indices: Vec<usize>,
    partition_converter: RowConverter,
    source_primary_key_columns: Vec<String>,
    source_snapshot: Vec<RecordBatch>,
    source_index: Option<SlateBackedColumnarIndexedZSet>,
    partition_counts: Option<SlateBackedTopNPartitionCounts>,
    source_snapshot_current: bool,
    initial_snapshot: Vec<RecordBatch>,
    row_count: i64,
    append_only_input: bool,
    append_only_fast_path: bool,
    row_number_limit: Option<usize>,
    source_output_projection: Option<Vec<usize>>,
    append_only_direct: Option<AppendOnlyDirectTopNState>,
}

struct AppendOnlyDirectTopNState {
    limit: usize,
    orderings: Vec<AppendOnlyDirectTopNOrderingState>,
}

struct AppendOnlyDirectTopNOrderingState {
    index: usize,
    asc: bool,
    nulls_first: bool,
}

struct TopNInputTick {
    delta: ColumnarZSet,
    input_changed: bool,
    next_source_snapshot: Option<Vec<RecordBatch>>,
    append_only_source_delta: bool,
}

struct SlateBackedTopNPartitionCounts {
    table: Arc<dyn KeyValueTable>,
    count_prefix: Vec<u8>,
    state_key: Vec<u8>,
}

pub(super) struct ColumnarTopNTick {
    pub(super) delta: ColumnarZSet,
    pub(super) next_snapshot: Vec<RecordBatch>,
    pub(super) row_count_delta: i64,
    pub(super) input_changed: bool,
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
    let mut append_only_fast_path = false;
    let mut row_number_limit = None;
    let mut append_only_direct_plan = None;
    let partition_columns = if let Some((rank_column, filter)) = row_number_filter_for_plan(plan) {
        let upper_bound = extract_standalone_row_number_upper_bound_limit(&filter.predicate);
        append_only_fast_path = upper_bound.is_some();
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
        if let Some((_rank_column, limit)) = upper_bound {
            row_number_limit = Some(limit);
            append_only_direct_plan = append_only_direct_topn_plan(window_function, limit);
        }
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
    let input = if let Some((input_name, schema, grouped_stats)) =
        grouped_stats_topn_input_for_plan(plan, sources)?
    {
        ColumnarTopNInputPlan::GroupedStats {
            input_name,
            schema,
            plan: Box::new(grouped_stats),
        }
    } else if contains_aggregate(plan) {
        return Ok(None);
    } else if let Some(source_name) = single_source_for_plan(plan, sources) {
        if contains_unsupported_topn_wrapper(plan) {
            return Ok(None);
        }
        ColumnarTopNInputPlan::Source { source_name }
    } else {
        return Ok(None);
    };

    Ok(Some(ColumnarTopNPlan {
        append_only_fast_path,
        row_number_limit,
        append_only_direct_plan,
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
    build_columnar_topn_materialized_view_state_in_namespace(
        table,
        mv_namespace,
        output_schema,
        plan,
        sources,
        udfs,
    )
    .await
}

pub(super) async fn build_columnar_topn_materialized_view_state_in_namespace(
    table: Arc<dyn KeyValueTable>,
    mv_namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarTopNPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarTopNMaterializedViewState> {
    let output_namespace = format!("{mv_namespace}/columnar/topn/output");
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed topn output zset")?;
    let initial_output = output_zset
        .materialize_columnar()
        .await
        .context("load topn output snapshot")?;
    let initial_row_count = columnar_zset_weight_sum(&initial_output)?;
    let initial_snapshot = crate::columnar_snapshot::columnar_zset_snapshot(&initial_output)?;
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
            let append_only_direct = append_only_direct_topn_state_for_source(
                source,
                plan.append_only_direct_plan.as_ref(),
            )?;
            let source_output_projection = direct_topn_source_output_projection_indices(
                &plan.logical_plan,
                &source.schema,
                output_schema,
            );
            let input_namespace = format!("{mv_namespace}/columnar/topn/input");
            let input_zset = SlateBackedColumnarZSet::new(
                Arc::clone(&table),
                input_namespace,
                Arc::clone(&source.schema),
            )
            .await
            .context("initialize SlateDB-backed topn input zset")?;
            let input_snapshot_zset = input_zset
                .materialize_columnar()
                .await
                .context("load topn input snapshot")?;
            let source_snapshot =
                crate::columnar_snapshot::columnar_zset_snapshot(&input_snapshot_zset)?;
            let source_index = if partition_indices.is_empty() {
                None
            } else {
                let mut index = SlateBackedColumnarIndexedZSet::new(
                    Arc::clone(&table),
                    format!("{mv_namespace}/columnar/topn/input_index"),
                    Arc::clone(&source.schema),
                    partition_indices.clone(),
                )
                .await
                .context("initialize SlateDB-backed topn source partition index")?;
                if !index.has_persisted_segments() && !input_snapshot_zset.is_empty() {
                    index
                        .rebuild_from_zset(&input_snapshot_zset)
                        .await
                        .context("rebuild SlateDB-backed topn source partition index")?;
                }
                Some(index)
            };
            let partition_counts = if partition_indices.is_empty() {
                None
            } else {
                let counts = SlateBackedTopNPartitionCounts::new(
                    Arc::clone(&table),
                    format!("{mv_namespace}/columnar/topn/partition_counts"),
                );
                if !counts.is_initialized().await? {
                    counts
                        .rebuild_from_batches(
                            &partition_converter,
                            &partition_indices,
                            &source_snapshot,
                        )
                        .await
                        .context("rebuild SlateDB-backed topn partition counts")?;
                }
                Some(counts)
            };
            let evaluator =
                TopNEvaluator::build(plan.logical_plan, &source_name, source, udfs, output_schema)
                    .await
                    .context("build topn vectorized evaluator")?;

            Ok(ColumnarTopNMaterializedViewState {
                operator_table: Arc::clone(&table),
                input_name: source_name,
                source_schema: Arc::clone(&source.schema),
                input: super::IncrementalInputOperator::Source(Box::new(input_zset)),
                output_zset,
                evaluator,
                partition_indices,
                partition_converter,
                source_primary_key_columns: source.primary_key_columns.clone(),
                source_snapshot,
                source_index,
                partition_counts,
                source_snapshot_current: true,
                initial_snapshot,
                row_count: initial_row_count,
                append_only_input: source.append_only,
                append_only_fast_path: plan.append_only_fast_path,
                row_number_limit: plan.row_number_limit,
                source_output_projection,
                append_only_direct,
            })
        }
        ColumnarTopNInputPlan::GroupedStats {
            input_name,
            schema,
            plan: grouped_stats_plan,
        } => {
            let partition_indices = plan
                .partition_columns
                .iter()
                .map(|column| partition_column_index_for_schema(&schema, column))
                .collect::<Result<Vec<_>>>()?;
            let partition_converter = row_converter_for_indices(&schema, &partition_indices)?;
            let grouped_stats_namespace = format!("{mv_namespace}/columnar/topn/grouped_stats");
            let grouped_stats = Box::pin(build_boxed_grouped_stats_topn_input_state(
                Arc::clone(&table),
                grouped_stats_namespace,
                &schema,
                *grouped_stats_plan,
                sources,
                udfs,
            ))
            .await
            .with_context(|| {
                format!(
                    "build SlateDB-backed topn grouped-stats input for '{}'",
                    input_name
                )
            })?;
            let source_snapshot = grouped_stats.initial_snapshot();
            let evaluator = TopNEvaluator::build_derived_input(
                plan.logical_plan,
                &input_name,
                &schema,
                udfs,
                output_schema,
            )
            .await
            .context("build grouped-stats topn vectorized evaluator")?;

            Ok(ColumnarTopNMaterializedViewState {
                operator_table: Arc::clone(&table),
                input_name,
                source_schema: schema,
                input: super::IncrementalInputOperator::GroupedStats(grouped_stats),
                output_zset,
                evaluator,
                partition_indices,
                partition_converter,
                source_primary_key_columns: Vec::new(),
                source_snapshot,
                source_index: None,
                partition_counts: None,
                source_snapshot_current: true,
                initial_snapshot,
                row_count: initial_row_count,
                append_only_input: false,
                append_only_fast_path: plan.append_only_fast_path,
                row_number_limit: plan.row_number_limit,
                source_output_projection: None,
                append_only_direct: None,
            })
        }
    }
}

async fn build_boxed_grouped_stats_topn_input_state(
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarGroupedStatsPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<Box<ColumnarGroupedStatsMaterializedViewState>> {
    Ok(Box::new(
        build_columnar_grouped_stats_materialized_view_state_in_namespace(
            table,
            namespace,
            output_schema,
            plan,
            sources,
            udfs,
            true,
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
) -> Result<()> {
    let super::MaterializedViewOperator::TopN(columnar) = &mut mv.operator else {
        unreachable!("topn tick dispatched to non-topn operator")
    };
    let plan_start = Instant::now();

    let maintain_output_snapshot = columnar.source_index.is_none()
        || (columnar.append_only_input && columnar.append_only_fast_path);
    let tick = run_columnar_topn_state_tick_inner(
        columnar,
        insert_batches,
        weighted_delta_batches,
        &mv.output_schema,
        &mv.previous_snapshot,
        maintain_output_snapshot,
    )
    .await?;

    let delta_batches = tick.delta.batches().to_vec();
    columnar.row_count = columnar.row_count.saturating_add(tick.row_count_delta);
    if columnar.row_count < 0 {
        bail!(
            "topn columnar materialized view '{}' row count became negative",
            mv.view_name
        );
    }
    let snapshot_rows =
        usize::try_from(columnar.row_count).context("topn row count exceeds usize")?;
    let handle = registry.register(mv.view_name.clone());
    if maintain_output_snapshot {
        handle.publish_arrow_version(version, tick.next_snapshot.clone(), delta_batches);
        mv.previous_snapshot = tick.next_snapshot;
    } else if let Some(zset_handle) = columnar.output_zset.current_handle() {
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
        mode = "columnar_topn",
        "SlateDB-backed topn columnar DBSP materialized view tick completed"
    );
    Ok(())
}

pub(super) async fn run_columnar_topn_state_tick(
    columnar: &mut ColumnarTopNMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    output_schema: &SchemaRef,
    previous_snapshot: &[RecordBatch],
) -> Result<ColumnarTopNTick> {
    run_columnar_topn_state_tick_inner(
        columnar,
        insert_batches,
        weighted_delta_batches,
        output_schema,
        previous_snapshot,
        true,
    )
    .await
}

async fn run_columnar_topn_state_tick_inner(
    columnar: &mut ColumnarTopNMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    output_schema: &SchemaRef,
    previous_snapshot: &[RecordBatch],
    maintain_output_snapshot: bool,
) -> Result<ColumnarTopNTick> {
    let total_start = profile::start();
    let phase_start = profile::start();
    let prepare_start = Instant::now();
    let input_tick = prepare_topn_input_tick(columnar, insert_batches, weighted_delta_batches)
        .await
        .context("prepare SlateDB-backed topn input tick")?;
    let prepare_ms = prepare_start.elapsed().as_millis() as u64;
    profile::record_since("topn.prepare_input", phase_start);
    let input_changed = input_tick.input_changed;
    let persisted_input_delta = input_tick.delta;
    let phase_start = profile::start();
    let touched_start = Instant::now();
    let touched_partitions = touched_partition_keys(
        &columnar.partition_converter,
        &columnar.partition_indices,
        persisted_input_delta.batches(),
    )?;
    let touched_ms = touched_start.elapsed().as_millis() as u64;
    profile::record_since("topn.touched_partitions", phase_start);
    if columnar.source_index.is_some()
        && !maintain_output_snapshot
        && let Some(tick) = run_columnar_topn_indexed_source_state_tick(
            columnar,
            &persisted_input_delta,
            output_schema,
            previous_snapshot,
            input_changed,
            maintain_output_snapshot,
        )
        .await?
    {
        profile::record_since("topn.total", total_start);
        return Ok(tick);
    }
    if columnar.append_only_fast_path
        && input_tick.append_only_source_delta
        && columnar.source_snapshot_current
        && let Some(tick) = run_columnar_topn_append_only_source_state_tick(
            columnar,
            &persisted_input_delta,
            &touched_partitions,
            output_schema,
            previous_snapshot,
            input_changed,
        )
        .await?
    {
        profile::record_since("topn.total", total_start);
        return Ok(tick);
    }
    if columnar.source_index.is_some()
        && maintain_output_snapshot
        && let Some(tick) = run_columnar_topn_indexed_source_state_tick(
            columnar,
            &persisted_input_delta,
            output_schema,
            previous_snapshot,
            input_changed,
            maintain_output_snapshot,
        )
        .await?
    {
        profile::record_since("topn.total", total_start);
        return Ok(tick);
    }
    let phase_start = profile::start();
    let source_snapshot_start = Instant::now();
    let next_source_snapshot = if let Some(snapshot) = input_tick.next_source_snapshot {
        snapshot
    } else {
        apply_source_snapshot_delta(
            &columnar.source_schema,
            &columnar.source_primary_key_columns,
            &columnar.source_snapshot,
            &persisted_input_delta,
        )
        .await?
    };
    let source_snapshot_ms = source_snapshot_start.elapsed().as_millis() as u64;
    profile::record_since("topn.source_snapshot_delta", phase_start);

    let identity_check_start = Instant::now();
    let under_limit_identity = if let Some(limit) = columnar.row_number_limit {
        topn_source_is_under_limit_identity(columnar, output_schema, &next_source_snapshot, limit)?
    } else {
        false
    };
    let identity_check_ms = identity_check_start.elapsed().as_millis() as u64;
    if under_limit_identity {
        let phase_start = profile::start();
        let build_output_start = Instant::now();
        let projection = columnar
            .source_output_projection
            .as_ref()
            .context("under-limit identity topn source projection missing")?;
        let output_delta = direct_project_weighted_columnar_zset(
            &persisted_input_delta,
            output_schema,
            projection,
        )
        .context("build under-limit identity topn output zset delta")?;
        let build_output_ms = build_output_start.elapsed().as_millis() as u64;
        profile::record_since("topn.identity_build_output_zset", phase_start);

        let phase_start = profile::start();
        let output_create_start = Instant::now();
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
        let output_create_ms = output_create_start.elapsed().as_millis() as u64;
        profile::record_since("topn.identity_output_create_version", phase_start);

        if let Some(index) = columnar.source_index.as_mut() {
            index
                .apply_delta(&persisted_input_delta)
                .await
                .context("apply topn identity source delta to partition index")?;
        }
        let next_snapshot =
            direct_project_record_batches(&next_source_snapshot, output_schema, projection, "topn")
                .context("build under-limit identity topn output snapshot")?;

        tracing::debug!(
            input = %columnar.input_name,
            path = "under_limit_identity",
            input_delta_rows = persisted_input_delta.num_rows(),
            touched_partition_count = touched_partitions.len(),
            previous_source_rows = record_batch_row_count(&columnar.source_snapshot),
            next_source_rows = record_batch_row_count(&next_source_snapshot),
            previous_output_rows = record_batch_row_count(previous_snapshot),
            next_output_rows = record_batch_row_count(&next_snapshot),
            output_delta_rows = persisted_output_delta.num_rows(),
            next_snapshot_rows = record_batch_row_count(&next_snapshot),
            prepare_ms,
            touched_ms,
            source_snapshot_ms,
            identity_check_ms,
            build_output_ms,
            output_create_ms,
            "topn state tick phase timings"
        );

        columnar.source_snapshot = next_source_snapshot;
        columnar.source_snapshot_current = true;
        profile::record_since("topn.total", total_start);
        return Ok(ColumnarTopNTick {
            row_count_delta: columnar_zset_weight_sum(&persisted_output_delta)
                .context("compute topn identity row-count delta")?,
            delta: persisted_output_delta,
            next_snapshot,
            input_changed,
        });
    }

    let phase_start = profile::start();
    let previous_filter_start = Instant::now();
    let previous_source_for_keys = filter_batches_to_partition_keys(
        &columnar.source_schema,
        &columnar.partition_converter,
        &columnar.partition_indices,
        &columnar.source_snapshot,
        &touched_partitions,
    )?;
    let previous_filter_ms = previous_filter_start.elapsed().as_millis() as u64;
    profile::record_since("topn.previous_source_for_keys", phase_start);
    let phase_start = profile::start();
    let next_filter_start = Instant::now();
    let next_source_for_keys = filter_batches_to_partition_keys(
        &columnar.source_schema,
        &columnar.partition_converter,
        &columnar.partition_indices,
        &next_source_snapshot,
        &touched_partitions,
    )?;
    let next_filter_ms = next_filter_start.elapsed().as_millis() as u64;
    profile::record_since("topn.next_source_for_keys", phase_start);

    let phase_start = profile::start();
    let evaluate_previous_start = Instant::now();
    let previous_output = columnar
        .evaluator
        .evaluate(&previous_source_for_keys)
        .await
        .context("evaluate previous topn partition outputs")?;
    let evaluate_previous_ms = evaluate_previous_start.elapsed().as_millis() as u64;
    profile::record_since("topn.evaluate_previous", phase_start);
    let phase_start = profile::start();
    let evaluate_next_start = Instant::now();
    let next_output = columnar
        .evaluator
        .evaluate(&next_source_for_keys)
        .await
        .context("evaluate next topn partition outputs")?;
    let evaluate_next_ms = evaluate_next_start.elapsed().as_millis() as u64;
    profile::record_since("topn.evaluate_next", phase_start);
    let phase_start = profile::start();
    let diff_start = Instant::now();
    let diff =
        diff_bounded_output_batches(Arc::clone(output_schema), &previous_output, &next_output)
            .await
            .context("diff topn partition outputs")?;
    let diff_ms = diff_start.elapsed().as_millis() as u64;
    profile::record_since("topn.diff_output", phase_start);

    let phase_start = profile::start();
    let build_output_start = Instant::now();
    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), diff.batches)
            .context("build topn output zset delta")?;
    let build_output_ms = build_output_start.elapsed().as_millis() as u64;
    profile::record_since("topn.build_output_zset", phase_start);
    let phase_start = profile::start();
    let output_create_start = Instant::now();
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
    let output_create_ms = output_create_start.elapsed().as_millis() as u64;
    profile::record_since("topn.output_create_version", phase_start);

    let delta_batches = persisted_output_delta.batches().to_vec();
    let phase_start = profile::start();
    let output_snapshot_start = Instant::now();
    let next_snapshot =
        apply_weighted_snapshot_delta(output_schema, previous_snapshot, delta_batches)
            .await
            .context("apply Slate-backed topn columnar snapshot delta")?;
    let output_snapshot_ms = output_snapshot_start.elapsed().as_millis() as u64;
    profile::record_since("topn.output_snapshot_delta", phase_start);

    tracing::debug!(
        input = %columnar.input_name,
        input_delta_rows = persisted_input_delta.num_rows(),
        touched_partition_count = touched_partitions.len(),
        previous_source_rows = record_batch_row_count(&previous_source_for_keys),
        next_source_rows = record_batch_row_count(&next_source_for_keys),
        previous_output_rows = record_batch_row_count(&previous_output),
        next_output_rows = record_batch_row_count(&next_output),
        output_delta_rows = persisted_output_delta.num_rows(),
        next_snapshot_rows = record_batch_row_count(&next_snapshot),
        prepare_ms,
        touched_ms,
        previous_filter_ms,
        source_snapshot_ms,
        identity_check_ms,
        next_filter_ms,
        evaluate_previous_ms,
        evaluate_next_ms,
        diff_ms,
        build_output_ms,
        output_create_ms,
        output_snapshot_ms,
        "topn state tick phase timings"
    );

    columnar.source_snapshot = next_source_snapshot;
    if let Some(index) = columnar.source_index.as_mut() {
        index
            .apply_delta(&persisted_input_delta)
            .await
            .context("apply topn source delta to partition index")?;
    }
    columnar.source_snapshot_current = true;
    profile::record_since("topn.total", total_start);
    Ok(ColumnarTopNTick {
        row_count_delta: columnar_zset_weight_sum(&persisted_output_delta)
            .context("compute topn row-count delta")?,
        delta: persisted_output_delta,
        next_snapshot,
        input_changed,
    })
}

async fn run_columnar_topn_append_only_source_state_tick(
    columnar: &mut ColumnarTopNMaterializedViewState,
    input_delta: &ColumnarZSet,
    touched_partitions: &HashSet<Vec<u8>>,
    output_schema: &SchemaRef,
    previous_snapshot: &[RecordBatch],
    input_changed: bool,
) -> Result<Option<ColumnarTopNTick>> {
    if !matches!(columnar.input, super::IncrementalInputOperator::Source(_))
        || !schemas_match_by_position(&columnar.source_schema, output_schema)
    {
        return Ok(None);
    }
    let Some(delta_value_batches) =
        unit_positive_delta_value_batches(&columnar.source_schema, input_delta.batches())?
    else {
        return Ok(None);
    };

    let phase_start = profile::start();
    let (previous_output_for_keys, untouched_previous_output) = split_batches_by_partition_keys(
        output_schema,
        &columnar.partition_converter,
        &columnar.partition_indices,
        previous_snapshot,
        touched_partitions,
    )?;
    profile::record_since("topn.append_only_previous_output_for_keys", phase_start);

    if let Some(direct) = columnar.append_only_direct.as_ref() {
        let phase_start = profile::start();
        let merge = merge_append_only_direct_topn(
            output_schema,
            &columnar.partition_converter,
            &columnar.partition_indices,
            direct,
            &previous_output_for_keys,
            &delta_value_batches,
        )?;
        profile::record_since("topn.append_only_direct_merge", phase_start);

        let phase_start = profile::start();
        let output_delta = ColumnarZSet::try_new_weighted(
            columnar.output_zset.value_schema(),
            merge.delta_batches,
        )
        .context("build direct append-only topn output zset delta")?;
        profile::record_since("topn.append_only_build_output_zset", phase_start);

        let phase_start = profile::start();
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
        profile::record_since("topn.append_only_output_create_version", phase_start);

        let phase_start = profile::start();
        let mut next_snapshot = untouched_previous_output;
        next_snapshot.extend(
            merge
                .next_output
                .into_iter()
                .filter(|batch| batch.num_rows() > 0),
        );
        if next_snapshot.is_empty() {
            next_snapshot.push(RecordBatch::new_empty(Arc::clone(output_schema)));
        }
        profile::record_since("topn.append_only_output_snapshot_replace", phase_start);

        let phase_start = profile::start();
        if let Some(index) = columnar.source_index.as_mut() {
            index
                .apply_delta(input_delta)
                .await
                .context("apply direct append-only topn source delta to partition index")?;
        }
        columnar.source_snapshot = apply_source_snapshot_delta(
            &columnar.source_schema,
            &columnar.source_primary_key_columns,
            &columnar.source_snapshot,
            input_delta,
        )
        .await?;
        columnar.source_snapshot_current = true;
        profile::record_since("topn.append_only_source_snapshot_delta", phase_start);

        return Ok(Some(ColumnarTopNTick {
            row_count_delta: columnar_zset_weight_sum(&persisted_output_delta)
                .context("compute direct append-only topn row-count delta")?,
            delta: persisted_output_delta,
            next_snapshot,
            input_changed,
        }));
    }

    let phase_start = profile::start();
    let mut candidate_batches = super::columnar_utils::rewrap_record_batches_with_schema(
        &previous_output_for_keys,
        &columnar.source_schema,
    )?;
    candidate_batches.extend(delta_value_batches);
    profile::record_since("topn.append_only_candidate_batches", phase_start);

    let phase_start = profile::start();
    let next_output = columnar
        .evaluator
        .evaluate(&candidate_batches)
        .await
        .context("evaluate append-only topn candidate output")?;
    profile::record_since("topn.append_only_evaluate_next", phase_start);

    let phase_start = profile::start();
    let diff = diff_bounded_output_batches_by_row(
        Arc::clone(output_schema),
        &previous_output_for_keys,
        &next_output,
    )
    .context("diff append-only topn output")?;
    profile::record_since("topn.append_only_diff_output", phase_start);

    let phase_start = profile::start();
    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), diff.batches)
            .context("build append-only topn output zset delta")?;
    profile::record_since("topn.append_only_build_output_zset", phase_start);

    let phase_start = profile::start();
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
    profile::record_since("topn.append_only_output_create_version", phase_start);

    let phase_start = profile::start();
    let mut next_snapshot = untouched_previous_output;
    next_snapshot.extend(next_output.into_iter().filter(|batch| batch.num_rows() > 0));
    if next_snapshot.is_empty() {
        next_snapshot.push(RecordBatch::new_empty(Arc::clone(output_schema)));
    }
    profile::record_since("topn.append_only_output_snapshot_replace", phase_start);

    let phase_start = profile::start();
    if let Some(index) = columnar.source_index.as_mut() {
        index
            .apply_delta(input_delta)
            .await
            .context("apply append-only topn source delta to partition index")?;
    }
    columnar.source_snapshot = apply_source_snapshot_delta(
        &columnar.source_schema,
        &columnar.source_primary_key_columns,
        &columnar.source_snapshot,
        input_delta,
    )
    .await?;
    columnar.source_snapshot_current = true;
    profile::record_since("topn.append_only_source_snapshot_delta", phase_start);

    Ok(Some(ColumnarTopNTick {
        row_count_delta: columnar_zset_weight_sum(&persisted_output_delta)
            .context("compute append-only topn row-count delta")?,
        delta: persisted_output_delta,
        next_snapshot,
        input_changed,
    }))
}

async fn run_columnar_topn_indexed_source_state_tick(
    columnar: &mut ColumnarTopNMaterializedViewState,
    input_delta: &ColumnarZSet,
    output_schema: &SchemaRef,
    previous_snapshot: &[RecordBatch],
    input_changed: bool,
    maintain_output_snapshot: bool,
) -> Result<Option<ColumnarTopNTick>> {
    if input_delta.is_empty() || columnar.partition_indices.is_empty() {
        return Ok(None);
    }

    let key_batches = {
        let index = columnar
            .source_index
            .as_ref()
            .context("topn source partition index missing")?;
        lookup_key_batches_from_delta(
            input_delta.batches(),
            &columnar.partition_indices,
            &index.key_schema(),
        )?
    };
    if key_batches.iter().all(|batch| batch.num_rows() == 0) {
        return Ok(None);
    }

    let count_deltas = columnar
        .partition_counts
        .as_ref()
        .map(|_| {
            partition_count_deltas_from_zset(
                &columnar.partition_converter,
                &columnar.partition_indices,
                input_delta,
            )
        })
        .transpose()?;
    let count_load_start = Instant::now();
    let previous_partition_counts =
        if let (Some(counts), Some(deltas)) = (columnar.partition_counts.as_ref(), &count_deltas) {
            let keys = deltas.keys().cloned().collect::<HashSet<_>>();
            counts.load_counts(&keys).await?
        } else {
            HashMap::new()
        };
    let count_load_ms = count_load_start.elapsed().as_millis() as u64;

    let indexed_under_limit_identity_from_counts = if let Some(limit) = columnar.row_number_limit {
        if let (Some(projection), Some(deltas)) =
            (columnar.source_output_projection.as_ref(), &count_deltas)
        {
            projection.len() == output_schema.fields().len()
                && partition_counts_with_deltas_within_limit(
                    &previous_partition_counts,
                    deltas,
                    limit,
                )?
        } else {
            false
        }
    } else {
        false
    };
    if indexed_under_limit_identity_from_counts {
        let projection = columnar
            .source_output_projection
            .as_ref()
            .context("indexed under-limit topn source projection missing")?;

        let phase_start = profile::start();
        let build_output_start = Instant::now();
        let output_delta =
            direct_project_weighted_columnar_zset(input_delta, output_schema, projection)
                .context("build indexed count-under-limit topn output zset delta")?;
        let build_output_ms = build_output_start.elapsed().as_millis() as u64;
        profile::record_since("topn.indexed_count_identity_build_output_zset", phase_start);

        let phase_start = profile::start();
        let output_create_start = Instant::now();
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
        let output_create_ms = output_create_start.elapsed().as_millis() as u64;
        profile::record_since(
            "topn.indexed_count_identity_output_create_version",
            phase_start,
        );

        let phase_start = profile::start();
        let apply_index_start = Instant::now();
        if let Some(index) = columnar.source_index.as_mut() {
            index
                .apply_delta(input_delta)
                .await
                .context("apply indexed count-under-limit topn source delta to partition index")?;
        }
        if let (Some(counts), Some(deltas)) = (columnar.partition_counts.as_ref(), &count_deltas) {
            counts
                .apply_deltas(deltas, &previous_partition_counts)
                .await?;
        }
        columnar.source_snapshot_current = false;
        let apply_index_ms = apply_index_start.elapsed().as_millis() as u64;
        profile::record_since(
            "topn.indexed_count_identity_apply_source_index",
            phase_start,
        );
        tracing::debug!(
            input = %columnar.input_name,
            path = "indexed_count_under_limit_identity",
            input_delta_rows = input_delta.num_rows(),
            key_batch_rows = key_batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            output_delta_rows = persisted_output_delta.num_rows(),
            count_delta_keys = count_deltas.as_ref().map(HashMap::len).unwrap_or(0),
            count_load_ms,
            build_output_ms,
            output_create_ms,
            apply_index_ms,
            "indexed topn state tick phase timings"
        );

        return Ok(Some(ColumnarTopNTick {
            row_count_delta: columnar_zset_weight_sum(&persisted_output_delta)
                .context("compute indexed count-under-limit topn row-count delta")?,
            delta: persisted_output_delta,
            next_snapshot: Vec::new(),
            input_changed,
        }));
    }

    if let Some(tick) = run_columnar_topn_indexed_direct_top1_state_tick(
        columnar,
        input_delta,
        DirectTop1TickContext {
            output_schema,
            previous_snapshot,
            maintain_output_snapshot,
            key_batch_rows: key_batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            count_deltas: count_deltas.as_ref(),
            previous_partition_counts: &previous_partition_counts,
            count_load_ms,
            input_changed,
        },
    )
    .await?
    {
        return Ok(Some(tick));
    }

    let phase_start = profile::start();
    let previous_lookup_start = Instant::now();
    let previous_lookup = {
        let index = columnar
            .source_index
            .as_ref()
            .context("topn source partition index missing")?;
        index
            .lookup_key_batches(&key_batches)
            .await
            .context("lookup topn source partitions from SlateDB-backed index")?
    };
    let previous_source_for_keys = materialize_columnar_zset_values(&previous_lookup)
        .await
        .context("materialize indexed topn previous source partitions")?;
    let previous_lookup_ms = previous_lookup_start.elapsed().as_millis() as u64;
    profile::record_since("topn.indexed_lookup_previous", phase_start);

    let phase_start = profile::start();
    let next_source_start = Instant::now();
    let next_source_for_keys = apply_source_snapshot_delta(
        &columnar.source_schema,
        &columnar.source_primary_key_columns,
        &previous_source_for_keys,
        input_delta,
    )
    .await
    .context("apply topn indexed source partition delta")?;
    let next_source_ms = next_source_start.elapsed().as_millis() as u64;
    profile::record_since("topn.indexed_next_source_for_keys", phase_start);

    let indexed_under_limit_identity = if let Some(limit) = columnar.row_number_limit {
        if let Some(projection) = columnar.source_output_projection.as_ref() {
            projection.len() == output_schema.fields().len()
                && partition_row_counts_within_limit(
                    &columnar.partition_converter,
                    &columnar.partition_indices,
                    &previous_source_for_keys,
                    limit,
                )?
                && partition_row_counts_within_limit(
                    &columnar.partition_converter,
                    &columnar.partition_indices,
                    &next_source_for_keys,
                    limit,
                )?
        } else {
            false
        }
    } else {
        false
    };
    if indexed_under_limit_identity {
        let projection = columnar
            .source_output_projection
            .as_ref()
            .context("indexed under-limit topn source projection missing")?;

        let phase_start = profile::start();
        let build_output_start = Instant::now();
        let output_delta =
            direct_project_weighted_columnar_zset(input_delta, output_schema, projection)
                .context("build indexed under-limit identity topn output zset delta")?;
        let build_output_ms = build_output_start.elapsed().as_millis() as u64;
        profile::record_since("topn.indexed_identity_build_output_zset", phase_start);

        let phase_start = profile::start();
        let output_create_start = Instant::now();
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
        let output_create_ms = output_create_start.elapsed().as_millis() as u64;
        profile::record_since("topn.indexed_identity_output_create_version", phase_start);

        let phase_start = profile::start();
        let apply_index_start = Instant::now();
        if let Some(index) = columnar.source_index.as_mut() {
            index
                .apply_delta(input_delta)
                .await
                .context("apply indexed under-limit topn source delta to partition index")?;
        }
        if let (Some(counts), Some(deltas)) = (columnar.partition_counts.as_ref(), &count_deltas) {
            counts
                .apply_deltas(deltas, &previous_partition_counts)
                .await?;
        }
        columnar.source_snapshot_current = false;
        let apply_index_ms = apply_index_start.elapsed().as_millis() as u64;
        profile::record_since("topn.indexed_identity_apply_source_index", phase_start);
        tracing::debug!(
            input = %columnar.input_name,
            path = "indexed_under_limit_identity",
            input_delta_rows = input_delta.num_rows(),
            key_batch_rows = key_batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            previous_source_rows = record_batch_row_count(&previous_source_for_keys),
            next_source_rows = record_batch_row_count(&next_source_for_keys),
            output_delta_rows = persisted_output_delta.num_rows(),
            count_load_ms,
            previous_lookup_ms,
            next_source_ms,
            build_output_ms,
            output_create_ms,
            apply_index_ms,
            "indexed topn state tick phase timings"
        );

        return Ok(Some(ColumnarTopNTick {
            row_count_delta: columnar_zset_weight_sum(&persisted_output_delta)
                .context("compute indexed under-limit topn row-count delta")?,
            delta: persisted_output_delta,
            next_snapshot: Vec::new(),
            input_changed,
        }));
    }

    let phase_start = profile::start();
    let evaluate_previous_start = Instant::now();
    let previous_output = columnar
        .evaluator
        .evaluate(&previous_source_for_keys)
        .await
        .context("evaluate indexed previous topn partition outputs")?;
    let evaluate_previous_ms = evaluate_previous_start.elapsed().as_millis() as u64;
    profile::record_since("topn.indexed_evaluate_previous", phase_start);

    let phase_start = profile::start();
    let evaluate_next_start = Instant::now();
    let next_output = columnar
        .evaluator
        .evaluate(&next_source_for_keys)
        .await
        .context("evaluate indexed next topn partition outputs")?;
    let evaluate_next_ms = evaluate_next_start.elapsed().as_millis() as u64;
    profile::record_since("topn.indexed_evaluate_next", phase_start);

    let phase_start = profile::start();
    let diff_start = Instant::now();
    let diff =
        diff_bounded_output_batches(Arc::clone(output_schema), &previous_output, &next_output)
            .await
            .context("diff indexed topn partition outputs")?;
    let diff_ms = diff_start.elapsed().as_millis() as u64;
    profile::record_since("topn.indexed_diff_output", phase_start);

    let phase_start = profile::start();
    let build_output_start = Instant::now();
    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), diff.batches)
            .context("build indexed topn output zset delta")?;
    let build_output_ms = build_output_start.elapsed().as_millis() as u64;
    profile::record_since("topn.indexed_build_output_zset", phase_start);

    let phase_start = profile::start();
    let output_create_start = Instant::now();
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
    let output_create_ms = output_create_start.elapsed().as_millis() as u64;
    profile::record_since("topn.indexed_output_create_version", phase_start);

    let next_snapshot = if maintain_output_snapshot {
        let delta_batches = persisted_output_delta.batches().to_vec();
        let phase_start = profile::start();
        let output_snapshot_start = Instant::now();
        let next_snapshot =
            apply_weighted_snapshot_delta(output_schema, previous_snapshot, delta_batches)
                .await
                .context("apply indexed topn output snapshot delta")?;
        let output_snapshot_ms = output_snapshot_start.elapsed().as_millis() as u64;
        profile::record_since("topn.indexed_output_snapshot_delta", phase_start);
        tracing::debug!(
            output_snapshot_ms,
            "indexed topn output snapshot phase completed"
        );
        next_snapshot
    } else {
        Vec::new()
    };

    let phase_start = profile::start();
    let apply_index_start = Instant::now();
    if let Some(index) = columnar.source_index.as_mut() {
        index
            .apply_delta(input_delta)
            .await
            .context("apply topn source delta to partition index")?;
    }
    if let (Some(counts), Some(deltas)) = (columnar.partition_counts.as_ref(), &count_deltas) {
        counts
            .apply_deltas(deltas, &previous_partition_counts)
            .await?;
    }
    columnar.source_snapshot_current = false;
    let apply_index_ms = apply_index_start.elapsed().as_millis() as u64;
    profile::record_since("topn.indexed_apply_source_index", phase_start);
    tracing::debug!(
        input = %columnar.input_name,
        input_delta_rows = input_delta.num_rows(),
        key_batch_rows = key_batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        previous_source_rows = record_batch_row_count(&previous_source_for_keys),
        next_source_rows = record_batch_row_count(&next_source_for_keys),
        previous_output_rows = record_batch_row_count(&previous_output),
        next_output_rows = record_batch_row_count(&next_output),
        output_delta_rows = persisted_output_delta.num_rows(),
        count_load_ms,
        previous_lookup_ms,
        next_source_ms,
        evaluate_previous_ms,
        evaluate_next_ms,
        diff_ms,
        build_output_ms,
        output_create_ms,
        apply_index_ms,
        "indexed topn state tick phase timings"
    );

    Ok(Some(ColumnarTopNTick {
        row_count_delta: columnar_zset_weight_sum(&persisted_output_delta)
            .context("compute indexed topn row-count delta")?,
        delta: persisted_output_delta,
        next_snapshot,
        input_changed,
    }))
}

struct DirectTop1TickContext<'a> {
    output_schema: &'a SchemaRef,
    previous_snapshot: &'a [RecordBatch],
    maintain_output_snapshot: bool,
    key_batch_rows: usize,
    count_deltas: Option<&'a HashMap<Vec<u8>, i64>>,
    previous_partition_counts: &'a HashMap<Vec<u8>, i64>,
    count_load_ms: u64,
    input_changed: bool,
}

async fn run_columnar_topn_indexed_direct_top1_state_tick(
    columnar: &mut ColumnarTopNMaterializedViewState,
    input_delta: &ColumnarZSet,
    context: DirectTop1TickContext<'_>,
) -> Result<Option<ColumnarTopNTick>> {
    let DirectTop1TickContext {
        output_schema,
        previous_snapshot,
        maintain_output_snapshot,
        key_batch_rows,
        count_deltas,
        previous_partition_counts,
        count_load_ms,
        input_changed,
    } = context;
    if columnar.row_number_limit != Some(1) {
        return Ok(None);
    }
    let Some(direct) = columnar.append_only_direct.as_ref() else {
        return Ok(None);
    };
    if direct.limit != 1 {
        return Ok(None);
    }
    let Some(projection) = columnar.source_output_projection.as_ref() else {
        return Ok(None);
    };
    if projection.len() != output_schema.fields().len() {
        return Ok(None);
    }
    let Some(output_partition_indices) =
        output_indices_for_source_indices(projection, &columnar.partition_indices)
    else {
        return Ok(None);
    };
    let Some(output_direct) = direct_topn_state_for_output_projection(direct, projection) else {
        return Ok(None);
    };

    let materialize_output_start = Instant::now();
    let previous_output_zset = columnar
        .output_zset
        .materialize_columnar()
        .await
        .context("materialize direct top1 current output")?;
    let materialize_output_ms = materialize_output_start.elapsed().as_millis() as u64;

    let project_delta_start = Instant::now();
    let projected_delta =
        direct_project_weighted_columnar_zset(input_delta, output_schema, projection)
            .context("project direct top1 input delta to output schema")?;
    let project_delta_ms = project_delta_start.elapsed().as_millis() as u64;

    let classify_start = Instant::now();
    let direct_delta = match build_direct_top1_delta(
        output_schema,
        &output_partition_indices,
        &output_direct,
        &previous_output_zset,
        &projected_delta,
    )
    .context("build direct top1 output delta")?
    {
        Some(delta) => delta,
        None => return Ok(None),
    };
    let classify_ms = classify_start.elapsed().as_millis() as u64;

    let phase_start = profile::start();
    let build_output_start = Instant::now();
    let output_delta = ColumnarZSet::try_new_weighted(
        columnar.output_zset.value_schema(),
        direct_delta.delta_batches,
    )
    .context("build direct indexed top1 output zset delta")?;
    let build_output_ms = build_output_start.elapsed().as_millis() as u64;
    profile::record_since("topn.indexed_direct_top1_build_output_zset", phase_start);

    let phase_start = profile::start();
    let output_create_start = Instant::now();
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
    let output_create_ms = output_create_start.elapsed().as_millis() as u64;
    profile::record_since(
        "topn.indexed_direct_top1_output_create_version",
        phase_start,
    );

    let next_snapshot = if maintain_output_snapshot {
        let delta_batches = persisted_output_delta.batches().to_vec();
        let phase_start = profile::start();
        let output_snapshot_start = Instant::now();
        let next_snapshot =
            apply_weighted_snapshot_delta(output_schema, previous_snapshot, delta_batches)
                .await
                .context("apply direct indexed top1 output snapshot delta")?;
        tracing::debug!(
            output_snapshot_ms = output_snapshot_start.elapsed().as_millis() as u64,
            "direct indexed top1 output snapshot phase completed"
        );
        profile::record_since(
            "topn.indexed_direct_top1_output_snapshot_delta",
            phase_start,
        );
        next_snapshot
    } else {
        Vec::new()
    };

    let phase_start = profile::start();
    let apply_index_start = Instant::now();
    if let Some(index) = columnar.source_index.as_mut() {
        index
            .apply_delta(input_delta)
            .await
            .context("apply direct indexed top1 source delta to partition index")?;
    }
    if let (Some(counts), Some(deltas)) = (columnar.partition_counts.as_ref(), count_deltas) {
        counts
            .apply_deltas(deltas, previous_partition_counts)
            .await?;
    }
    columnar.source_snapshot_current = false;
    let apply_index_ms = apply_index_start.elapsed().as_millis() as u64;
    profile::record_since("topn.indexed_direct_top1_apply_source_index", phase_start);

    tracing::debug!(
        input = %columnar.input_name,
        path = "indexed_direct_top1",
        input_delta_rows = input_delta.num_rows(),
        key_batch_rows,
        previous_output_rows = previous_output_zset.num_rows(),
        projected_delta_rows = projected_delta.num_rows(),
        output_delta_rows = persisted_output_delta.num_rows(),
        negative_top_rows = direct_delta.negative_count,
        positive_top_rows = direct_delta.positive_count,
        count_load_ms,
        materialize_output_ms,
        project_delta_ms,
        classify_ms,
        build_output_ms,
        output_create_ms,
        apply_index_ms,
        "indexed topn state tick phase timings"
    );

    Ok(Some(ColumnarTopNTick {
        row_count_delta: columnar_zset_weight_sum(&persisted_output_delta)
            .context("compute direct indexed top1 row-count delta")?,
        delta: persisted_output_delta,
        next_snapshot,
        input_changed,
    }))
}

const DIRECT_TOPN_OUTPUT_BATCH_ROWS: usize = 4096;
const TOPN_PARTITION_COUNT_SCAN_MIN_KEYS: usize = 256;
const TOPN_PARTITION_COUNT_INITIALIZED: &[u8] = b"1";

struct DirectTopNMergeOutput {
    next_output: Vec<RecordBatch>,
    delta_batches: Vec<RecordBatch>,
}

struct DirectTop1DeltaOutput {
    delta_batches: Vec<RecordBatch>,
    negative_count: usize,
    positive_count: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DirectTopNRowSide {
    Previous,
    Delta,
}

#[derive(Clone)]
struct DirectTopNCandidate {
    side: DirectTopNRowSide,
    batch_idx: usize,
    row_idx: usize,
    ordinal: usize,
}

#[derive(Clone, Copy)]
struct DirectTopNRowRef {
    side: DirectTopNRowSide,
    batch_idx: usize,
    row_idx: usize,
}

#[derive(Clone)]
struct DirectTop1SelectedRow {
    candidate: DirectTopNCandidate,
    row_key: Vec<u8>,
}

enum DirectTopNOrderArray<'a> {
    Int64(&'a Int64Array),
    Utf8(&'a StringArray),
    TimestampMillis(&'a TimestampMillisecondArray),
    Boolean(&'a BooleanArray),
    Date32(&'a Date32Array),
    Decimal128(&'a Decimal128Array),
    Float64(&'a Float64Array),
    UInt64(&'a UInt64Array),
    Null,
}

fn build_direct_top1_delta(
    output_schema: &SchemaRef,
    output_partition_indices: &[usize],
    direct: &AppendOnlyDirectTopNState,
    previous_output: &ColumnarZSet,
    projected_delta: &ColumnarZSet,
) -> Result<Option<DirectTop1DeltaOutput>> {
    let value_indices = output_value_indices(output_schema);
    let value_converter = row_converter_for_indices(output_schema, &value_indices)?;
    let partition_converter = if output_partition_indices.is_empty() {
        None
    } else {
        Some(row_converter_for_indices(
            output_schema,
            output_partition_indices,
        )?)
    };
    let current_rows = match direct_top1_current_rows(
        output_partition_indices,
        partition_converter.as_ref(),
        &value_converter,
        previous_output,
    )? {
        Some(rows) => rows,
        None => return Ok(None),
    };
    let previous_order_columns = direct_topn_order_columns(direct, previous_output.batches())?;
    let delta_order_columns = direct_topn_order_columns(direct, projected_delta.batches())?;
    let best_positive_rows = match direct_top1_best_positive_rows(
        projected_delta,
        DirectTop1SelectionContext {
            output_partition_indices,
            partition_converter: partition_converter.as_ref(),
            value_converter: &value_converter,
            direct,
            previous_order_columns: &previous_order_columns,
            delta_order_columns: &delta_order_columns,
            current_rows: &current_rows,
        },
    )? {
        Some(rows) => rows,
        None => return Ok(None),
    };

    let mut negatives = Vec::new();
    let mut positives = Vec::new();
    let mut best_positive_rows = best_positive_rows.into_iter().collect::<Vec<_>>();
    best_positive_rows.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    for (partition_key, positive) in best_positive_rows {
        if let Some(current) = current_rows.get(&partition_key) {
            let order = compare_direct_topn_candidates(
                direct,
                &previous_order_columns,
                &delta_order_columns,
                &positive.candidate,
                &current.candidate,
            );
            match order {
                Ordering::Less => {
                    negatives.push(DirectTopNRowRef {
                        side: DirectTopNRowSide::Previous,
                        batch_idx: current.candidate.batch_idx,
                        row_idx: current.candidate.row_idx,
                    });
                    positives.push(DirectTopNRowRef {
                        side: DirectTopNRowSide::Delta,
                        batch_idx: positive.candidate.batch_idx,
                        row_idx: positive.candidate.row_idx,
                    });
                }
                Ordering::Equal => {
                    if positive.row_key != current.row_key {
                        return Ok(None);
                    }
                }
                Ordering::Greater => {}
            }
        } else {
            positives.push(DirectTopNRowRef {
                side: DirectTopNRowSide::Delta,
                batch_idx: positive.candidate.batch_idx,
                row_idx: positive.candidate.row_idx,
            });
        }
    }

    let weighted_schema = weighted_snapshot_schema(output_schema)?;
    let negative_count = negatives.len();
    let positive_count = positives.len();
    let delta_batches = build_direct_topn_weighted_batches(
        &weighted_schema,
        previous_output.batches(),
        projected_delta.batches(),
        &negatives,
        &positives,
    )?;
    Ok(Some(DirectTop1DeltaOutput {
        delta_batches,
        negative_count,
        positive_count,
    }))
}

fn direct_top1_current_rows(
    output_partition_indices: &[usize],
    partition_converter: Option<&RowConverter>,
    value_converter: &RowConverter,
    previous_output: &ColumnarZSet,
) -> Result<Option<HashMap<Vec<u8>, DirectTop1SelectedRow>>> {
    let mut rows_by_partition = HashMap::new();
    let value_indices = (0..previous_output.value_column_count()).collect::<Vec<_>>();
    for (batch_idx, batch) in previous_output.batches().iter().enumerate() {
        if batch.num_rows() == 0 {
            continue;
        }
        let value_rows = value_converter
            .convert_columns(&project_columns(batch, &value_indices))
            .context("encode direct top1 current output rows")?;
        let partition_rows = if output_partition_indices.is_empty() {
            None
        } else {
            Some(
                partition_converter
                    .context("direct top1 output partition converter missing")?
                    .convert_columns(&project_columns(batch, output_partition_indices))
                    .context("encode direct top1 current output partition rows")?,
            )
        };
        let weights = topn_weight_column(batch, previous_output.value_column_count())?;
        for row_idx in 0..batch.num_rows() {
            if weights.is_null(row_idx) || weights.value(row_idx) != 1 {
                return Ok(None);
            }
            let partition_key = partition_rows
                .as_ref()
                .map(|rows| rows.row(row_idx).data().to_vec())
                .unwrap_or_default();
            if rows_by_partition.contains_key(&partition_key) {
                return Ok(None);
            }
            rows_by_partition.insert(
                partition_key,
                DirectTop1SelectedRow {
                    candidate: DirectTopNCandidate {
                        side: DirectTopNRowSide::Previous,
                        batch_idx,
                        row_idx,
                        ordinal: row_idx,
                    },
                    row_key: value_rows.row(row_idx).data().to_vec(),
                },
            );
        }
    }
    Ok(Some(rows_by_partition))
}

struct DirectTop1SelectionContext<'a> {
    output_partition_indices: &'a [usize],
    partition_converter: Option<&'a RowConverter>,
    value_converter: &'a RowConverter,
    direct: &'a AppendOnlyDirectTopNState,
    previous_order_columns: &'a [Vec<DirectTopNOrderArray<'a>>],
    delta_order_columns: &'a [Vec<DirectTopNOrderArray<'a>>],
    current_rows: &'a HashMap<Vec<u8>, DirectTop1SelectedRow>,
}

fn direct_top1_best_positive_rows(
    projected_delta: &ColumnarZSet,
    context: DirectTop1SelectionContext<'_>,
) -> Result<Option<HashMap<Vec<u8>, DirectTop1SelectedRow>>> {
    let DirectTop1SelectionContext {
        output_partition_indices,
        partition_converter,
        value_converter,
        direct,
        previous_order_columns,
        delta_order_columns,
        current_rows,
    } = context;
    let mut best_positive_by_partition: HashMap<Vec<u8>, DirectTop1SelectedRow> = HashMap::new();
    let value_indices = (0..projected_delta.value_column_count()).collect::<Vec<_>>();
    let mut ordinal = 0usize;
    for (batch_idx, batch) in projected_delta.batches().iter().enumerate() {
        if batch.num_rows() == 0 {
            continue;
        }
        let value_rows = value_converter
            .convert_columns(&project_columns(batch, &value_indices))
            .context("encode direct top1 delta rows")?;
        let partition_rows = if output_partition_indices.is_empty() {
            None
        } else {
            Some(
                partition_converter
                    .context("direct top1 delta partition converter missing")?
                    .convert_columns(&project_columns(batch, output_partition_indices))
                    .context("encode direct top1 delta partition rows")?,
            )
        };
        let weights = topn_weight_column(batch, projected_delta.value_column_count())?;
        for row_idx in 0..batch.num_rows() {
            if weights.is_null(row_idx) {
                return Ok(None);
            }
            let weight = weights.value(row_idx);
            if weight == 0 {
                continue;
            }
            if !matches!(weight, -1 | 1) {
                return Ok(None);
            }
            let partition_key = partition_rows
                .as_ref()
                .map(|rows| rows.row(row_idx).data().to_vec())
                .unwrap_or_default();
            let row_key = value_rows.row(row_idx).data().to_vec();
            if weight < 0 {
                if current_rows
                    .get(&partition_key)
                    .is_some_and(|current| current.row_key == row_key)
                {
                    return Ok(None);
                }
                continue;
            }

            let selected = DirectTop1SelectedRow {
                candidate: DirectTopNCandidate {
                    side: DirectTopNRowSide::Delta,
                    batch_idx,
                    row_idx,
                    ordinal,
                },
                row_key,
            };
            ordinal = ordinal.saturating_add(1);
            match best_positive_by_partition.get_mut(&partition_key) {
                Some(current_best) => {
                    let order = compare_direct_topn_candidates(
                        direct,
                        previous_order_columns,
                        delta_order_columns,
                        &selected.candidate,
                        &current_best.candidate,
                    );
                    match order {
                        Ordering::Less => {
                            *current_best = selected;
                        }
                        Ordering::Equal => {
                            if selected.row_key != current_best.row_key {
                                return Ok(None);
                            }
                        }
                        Ordering::Greater => {}
                    }
                }
                None => {
                    best_positive_by_partition.insert(partition_key, selected);
                }
            }
        }
    }
    Ok(Some(best_positive_by_partition))
}

fn output_value_indices(output_schema: &SchemaRef) -> Vec<usize> {
    (0..output_schema.fields().len()).collect()
}

fn output_indices_for_source_indices(
    projection: &[usize],
    source_indices: &[usize],
) -> Option<Vec<usize>> {
    source_indices
        .iter()
        .map(|source_idx| {
            projection
                .iter()
                .position(|projected_source_idx| projected_source_idx == source_idx)
        })
        .collect()
}

fn direct_topn_state_for_output_projection(
    direct: &AppendOnlyDirectTopNState,
    projection: &[usize],
) -> Option<AppendOnlyDirectTopNState> {
    let orderings = direct
        .orderings
        .iter()
        .map(|ordering| {
            let output_idx = projection
                .iter()
                .position(|source_idx| *source_idx == ordering.index)?;
            Some(AppendOnlyDirectTopNOrderingState {
                index: output_idx,
                asc: ordering.asc,
                nulls_first: ordering.nulls_first,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(AppendOnlyDirectTopNState {
        limit: direct.limit,
        orderings,
    })
}

fn merge_append_only_direct_topn(
    output_schema: &SchemaRef,
    partition_converter: &RowConverter,
    partition_indices: &[usize],
    direct: &AppendOnlyDirectTopNState,
    previous: &[RecordBatch],
    delta: &[RecordBatch],
) -> Result<DirectTopNMergeOutput> {
    let mut groups: HashMap<Vec<u8>, Vec<DirectTopNCandidate>> = HashMap::new();
    let mut ordinal = 0usize;
    let phase_start = profile::start();
    accumulate_direct_topn_candidates(
        partition_converter,
        partition_indices,
        previous,
        DirectTopNRowSide::Previous,
        &mut ordinal,
        &mut groups,
    )?;
    profile::record_since("topn.append_only_direct_accumulate_previous", phase_start);
    let phase_start = profile::start();
    accumulate_direct_topn_candidates(
        partition_converter,
        partition_indices,
        delta,
        DirectTopNRowSide::Delta,
        &mut ordinal,
        &mut groups,
    )?;
    profile::record_since("topn.append_only_direct_accumulate_delta", phase_start);

    let phase_start = profile::start();
    let previous_order_columns = direct_topn_order_columns(direct, previous)?;
    let delta_order_columns = direct_topn_order_columns(direct, delta)?;
    let mut previous_selected = previous
        .iter()
        .map(|batch| vec![false; batch.num_rows()])
        .collect::<Vec<_>>();
    let mut delta_selected = delta
        .iter()
        .map(|batch| vec![false; batch.num_rows()])
        .collect::<Vec<_>>();
    let mut positives = Vec::new();
    let mut groups = groups.into_iter().collect::<Vec<_>>();
    groups.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    for (_partition_key, mut rows) in groups {
        rows.sort_unstable_by(|left, right| {
            compare_direct_topn_candidates(
                direct,
                &previous_order_columns,
                &delta_order_columns,
                left,
                right,
            )
            .then_with(|| direct_topn_side_rank(left.side).cmp(&direct_topn_side_rank(right.side)))
            .then_with(|| left.ordinal.cmp(&right.ordinal))
        });
        for row in rows.into_iter().take(direct.limit) {
            let selected_ref = DirectTopNRowRef {
                side: row.side,
                batch_idx: row.batch_idx,
                row_idx: row.row_idx,
            };
            if row.side == DirectTopNRowSide::Previous {
                if let Some(batch_selected) = previous_selected.get_mut(row.batch_idx)
                    && let Some(selected) = batch_selected.get_mut(row.row_idx)
                {
                    *selected = true;
                }
            } else {
                if let Some(batch_selected) = delta_selected.get_mut(row.batch_idx)
                    && let Some(selected) = batch_selected.get_mut(row.row_idx)
                {
                    *selected = true;
                }
                positives.push(selected_ref);
            }
        }
    }
    profile::record_since("topn.append_only_direct_select", phase_start);

    let phase_start = profile::start();
    let mut negatives = Vec::new();
    for (batch_idx, batch_selected) in previous_selected.iter().enumerate() {
        for (row_idx, selected) in batch_selected.iter().enumerate() {
            if !selected {
                negatives.push(DirectTopNRowRef {
                    side: DirectTopNRowSide::Previous,
                    batch_idx,
                    row_idx,
                });
            }
        }
    }
    profile::record_since("topn.append_only_direct_negatives", phase_start);

    let phase_start = profile::start();
    let next_output = build_direct_topn_selected_batches(
        output_schema,
        previous,
        delta,
        &previous_selected,
        &delta_selected,
    )
    .context("build direct append-only topn next output")?;
    profile::record_since("topn.append_only_direct_build_next", phase_start);
    let phase_start = profile::start();
    let weighted_schema = weighted_snapshot_schema(output_schema)?;
    let delta_batches = build_direct_topn_weighted_batches(
        &weighted_schema,
        previous,
        delta,
        &negatives,
        &positives,
    )
    .context("build direct append-only topn delta")?;
    profile::record_since("topn.append_only_direct_build_delta", phase_start);

    Ok(DirectTopNMergeOutput {
        next_output,
        delta_batches,
    })
}

fn accumulate_direct_topn_candidates(
    partition_converter: &RowConverter,
    partition_indices: &[usize],
    batches: &[RecordBatch],
    side: DirectTopNRowSide,
    ordinal: &mut usize,
    groups: &mut HashMap<Vec<u8>, Vec<DirectTopNCandidate>>,
) -> Result<()> {
    for (batch_idx, batch) in batches.iter().enumerate() {
        if batch.num_rows() == 0 {
            continue;
        }
        let partition_rows = if partition_indices.is_empty() {
            None
        } else {
            Some(
                partition_converter
                    .convert_columns(&project_columns(batch, partition_indices))
                    .context("encode direct topn partition keys")?,
            )
        };
        for row_idx in 0..batch.num_rows() {
            let partition_key = partition_rows
                .as_ref()
                .map(|rows| rows.row(row_idx).data().to_vec())
                .unwrap_or_default();
            groups
                .entry(partition_key)
                .or_default()
                .push(DirectTopNCandidate {
                    side,
                    batch_idx,
                    row_idx,
                    ordinal: *ordinal,
                });
            *ordinal = ordinal.saturating_add(1);
        }
    }
    Ok(())
}

fn direct_topn_order_columns<'a>(
    direct: &AppendOnlyDirectTopNState,
    batches: &'a [RecordBatch],
) -> Result<Vec<Vec<DirectTopNOrderArray<'a>>>> {
    batches
        .iter()
        .map(|batch| {
            direct
                .orderings
                .iter()
                .map(|ordering| direct_topn_order_array(batch.column(ordering.index).as_ref()))
                .collect::<Result<Vec<_>>>()
        })
        .collect()
}

fn direct_topn_order_array(array: &dyn Array) -> Result<DirectTopNOrderArray<'_>> {
    match array.data_type() {
        DataType::Int64 => Ok(DirectTopNOrderArray::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("direct topn Int64 order column")?,
        )),
        DataType::Utf8 => Ok(DirectTopNOrderArray::Utf8(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .context("direct topn Utf8 order column")?,
        )),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(DirectTopNOrderArray::TimestampMillis(
            array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .context("direct topn Timestamp(Millisecond) order column")?,
        )),
        DataType::Boolean => Ok(DirectTopNOrderArray::Boolean(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("direct topn Boolean order column")?,
        )),
        DataType::Date32 => Ok(DirectTopNOrderArray::Date32(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .context("direct topn Date32 order column")?,
        )),
        DataType::Decimal128(_, _) => Ok(DirectTopNOrderArray::Decimal128(
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .context("direct topn Decimal128 order column")?,
        )),
        DataType::Float64 => Ok(DirectTopNOrderArray::Float64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("direct topn Float64 order column")?,
        )),
        DataType::UInt64 => Ok(DirectTopNOrderArray::UInt64(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .context("direct topn UInt64 order column")?,
        )),
        DataType::Null => Ok(DirectTopNOrderArray::Null),
        other => bail!("unsupported direct topn order column type: {other:?}"),
    }
}

fn compare_direct_topn_candidates(
    direct: &AppendOnlyDirectTopNState,
    previous_order_columns: &[Vec<DirectTopNOrderArray<'_>>],
    delta_order_columns: &[Vec<DirectTopNOrderArray<'_>>],
    left: &DirectTopNCandidate,
    right: &DirectTopNCandidate,
) -> Ordering {
    let left_columns =
        direct_topn_candidate_order_columns(previous_order_columns, delta_order_columns, left);
    let right_columns =
        direct_topn_candidate_order_columns(previous_order_columns, delta_order_columns, right);
    for (ordering_idx, ordering) in direct.orderings.iter().enumerate() {
        let order = compare_direct_topn_array_values(
            &left_columns[ordering_idx],
            left.row_idx,
            &right_columns[ordering_idx],
            right.row_idx,
            ordering.asc,
            ordering.nulls_first,
        );
        if order != Ordering::Equal {
            return order;
        }
    }
    Ordering::Equal
}

fn direct_topn_candidate_order_columns<'a>(
    previous: &'a [Vec<DirectTopNOrderArray<'a>>],
    delta: &'a [Vec<DirectTopNOrderArray<'a>>],
    candidate: &DirectTopNCandidate,
) -> &'a [DirectTopNOrderArray<'a>] {
    match candidate.side {
        DirectTopNRowSide::Previous => &previous[candidate.batch_idx],
        DirectTopNRowSide::Delta => &delta[candidate.batch_idx],
    }
}

fn compare_direct_topn_array_values(
    left: &DirectTopNOrderArray<'_>,
    left_idx: usize,
    right: &DirectTopNOrderArray<'_>,
    right_idx: usize,
    asc: bool,
    nulls_first: bool,
) -> Ordering {
    match (
        direct_topn_order_array_is_null(left, left_idx),
        direct_topn_order_array_is_null(right, right_idx),
    ) {
        (true, true) => return Ordering::Equal,
        (true, false) => {
            return if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        (false, true) => {
            return if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        (false, false) => {}
    }

    let order = match (left, right) {
        (DirectTopNOrderArray::Int64(left), DirectTopNOrderArray::Int64(right)) => {
            left.value(left_idx).cmp(&right.value(right_idx))
        }
        (DirectTopNOrderArray::Utf8(left), DirectTopNOrderArray::Utf8(right)) => {
            left.value(left_idx).cmp(right.value(right_idx))
        }
        (
            DirectTopNOrderArray::TimestampMillis(left),
            DirectTopNOrderArray::TimestampMillis(right),
        ) => left.value(left_idx).cmp(&right.value(right_idx)),
        (DirectTopNOrderArray::Boolean(left), DirectTopNOrderArray::Boolean(right)) => {
            left.value(left_idx).cmp(&right.value(right_idx))
        }
        (DirectTopNOrderArray::Date32(left), DirectTopNOrderArray::Date32(right)) => {
            left.value(left_idx).cmp(&right.value(right_idx))
        }
        (DirectTopNOrderArray::Decimal128(left), DirectTopNOrderArray::Decimal128(right)) => {
            left.value(left_idx).cmp(&right.value(right_idx))
        }
        (DirectTopNOrderArray::Float64(left), DirectTopNOrderArray::Float64(right)) => {
            left.value(left_idx).total_cmp(&right.value(right_idx))
        }
        (DirectTopNOrderArray::UInt64(left), DirectTopNOrderArray::UInt64(right)) => {
            left.value(left_idx).cmp(&right.value(right_idx))
        }
        (DirectTopNOrderArray::Null, DirectTopNOrderArray::Null) => Ordering::Equal,
        _ => unreachable!("direct topn order column type mismatch"),
    };
    if asc { order } else { order.reverse() }
}

fn direct_topn_order_array_is_null(array: &DirectTopNOrderArray<'_>, row_idx: usize) -> bool {
    match array {
        DirectTopNOrderArray::Int64(array) => array.is_null(row_idx),
        DirectTopNOrderArray::Utf8(array) => array.is_null(row_idx),
        DirectTopNOrderArray::TimestampMillis(array) => array.is_null(row_idx),
        DirectTopNOrderArray::Boolean(array) => array.is_null(row_idx),
        DirectTopNOrderArray::Date32(array) => array.is_null(row_idx),
        DirectTopNOrderArray::Decimal128(array) => array.is_null(row_idx),
        DirectTopNOrderArray::Float64(array) => array.is_null(row_idx),
        DirectTopNOrderArray::UInt64(array) => array.is_null(row_idx),
        DirectTopNOrderArray::Null => true,
    }
}

fn direct_topn_side_rank(side: DirectTopNRowSide) -> u8 {
    match side {
        DirectTopNRowSide::Previous => 0,
        DirectTopNRowSide::Delta => 1,
    }
}

fn build_direct_topn_selected_batches(
    schema: &SchemaRef,
    previous: &[RecordBatch],
    delta: &[RecordBatch],
    previous_selected: &[Vec<bool>],
    delta_selected: &[Vec<bool>],
) -> Result<Vec<RecordBatch>> {
    let mut output = Vec::new();
    append_direct_topn_selected_batches(schema, previous, previous_selected, &mut output)?;
    append_direct_topn_selected_batches(schema, delta, delta_selected, &mut output)?;
    if output.is_empty() {
        output.push(RecordBatch::new_empty(Arc::clone(schema)));
    }
    Ok(output)
}

fn append_direct_topn_selected_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    selected: &[Vec<bool>],
    output: &mut Vec<RecordBatch>,
) -> Result<()> {
    for (batch, selected) in batches.iter().zip(selected) {
        if batch.num_rows() == 0 {
            continue;
        }
        let indices = selected
            .iter()
            .enumerate()
            .filter(|(_, selected)| **selected)
            .map(|(idx, _)| u32::try_from(idx).context("direct topn batch exceeds u32 rows"))
            .collect::<Result<Vec<_>>>()?;
        if indices.is_empty() {
            continue;
        }
        if indices.len() == batch.num_rows() {
            output.push(RecordBatch::try_new(
                Arc::clone(schema),
                batch.columns().to_vec(),
            )?);
        } else {
            output.push(take_batch_rows(schema, batch, indices)?);
        }
    }
    Ok(())
}

fn build_direct_topn_weighted_batches(
    schema: &SchemaRef,
    previous: &[RecordBatch],
    delta: &[RecordBatch],
    negatives: &[DirectTopNRowRef],
    positives: &[DirectTopNRowRef],
) -> Result<Vec<RecordBatch>> {
    let mut output = Vec::new();
    let mut builders = direct_topn_builders(schema)?;
    let value_column_count = schema.fields().len().saturating_sub(1);
    let mut buffered_rows = 0usize;

    for row in negatives {
        append_direct_topn_row(
            &mut builders,
            value_column_count,
            previous,
            delta,
            *row,
            Some(-1),
        )?;
        buffered_rows += 1;
        if buffered_rows == DIRECT_TOPN_OUTPUT_BATCH_ROWS {
            output.push(finish_direct_topn_batch(schema, &mut builders)?);
            buffered_rows = 0;
        }
    }
    for row in positives {
        append_direct_topn_row(
            &mut builders,
            value_column_count,
            previous,
            delta,
            *row,
            Some(1),
        )?;
        buffered_rows += 1;
        if buffered_rows == DIRECT_TOPN_OUTPUT_BATCH_ROWS {
            output.push(finish_direct_topn_batch(schema, &mut builders)?);
            buffered_rows = 0;
        }
    }

    if buffered_rows > 0 {
        output.push(finish_direct_topn_batch(schema, &mut builders)?);
    }
    if output.is_empty() {
        output.push(RecordBatch::new_empty(Arc::clone(schema)));
    }
    Ok(output)
}

fn direct_topn_builders(schema: &SchemaRef) -> Result<Vec<ScalarColumnBuilder>> {
    schema
        .fields()
        .iter()
        .map(|field| ScalarColumnBuilder::new(field.data_type(), DIRECT_TOPN_OUTPUT_BATCH_ROWS))
        .collect()
}

fn append_direct_topn_row(
    builders: &mut [ScalarColumnBuilder],
    value_column_count: usize,
    previous: &[RecordBatch],
    delta: &[RecordBatch],
    row: DirectTopNRowRef,
    weight: Option<i64>,
) -> Result<()> {
    let source_batches = match row.side {
        DirectTopNRowSide::Previous => previous,
        DirectTopNRowSide::Delta => delta,
    };
    let source_batch = source_batches
        .get(row.batch_idx)
        .context("direct topn source batch index out of bounds")?;
    for (column_idx, builder) in builders.iter_mut().enumerate() {
        if column_idx == value_column_count {
            builder.append_i64_value(weight.context("direct topn missing row weight")?)?;
        } else {
            builder.append_array_value(source_batch.column(column_idx).as_ref(), row.row_idx)?;
        }
    }
    Ok(())
}

fn finish_direct_topn_batch(
    schema: &SchemaRef,
    builders: &mut [ScalarColumnBuilder],
) -> Result<RecordBatch> {
    let columns = builders
        .iter_mut()
        .map(ScalarColumnBuilder::finish_array)
        .collect::<Vec<_>>();
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

impl SlateBackedTopNPartitionCounts {
    fn new(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        let mut count_prefix = keyspace::namespace_prefix(keyspace::prefix::INDEX, &namespace);
        count_prefix.extend_from_slice(b"counts/");
        let mut state_key = keyspace::namespace_prefix(keyspace::prefix::INDEX, &namespace);
        state_key.extend_from_slice(b"state/initialized");
        Self {
            table,
            count_prefix,
            state_key,
        }
    }

    async fn is_initialized(&self) -> Result<bool> {
        Ok(self
            .table
            .get_bytes(&self.state_key)
            .await
            .context("load topn partition count state")?
            .is_some())
    }

    async fn rebuild_from_batches(
        &self,
        converter: &RowConverter,
        partition_indices: &[usize],
        batches: &[RecordBatch],
    ) -> Result<()> {
        let counts = partition_row_count_map(converter, partition_indices, batches)?;
        let mut writes = WriteBatch::new();
        for (key, _) in self
            .table
            .scan_prefix_bytes(&self.count_prefix, &ScanOptions::default())
            .await
            .context("scan topn partition counts for rebuild")?
        {
            writes.delete(&key);
        }
        for (partition_key, count) in counts {
            if count > 0 {
                writes.put_bytes(
                    Bytes::from(self.count_key(&partition_key)),
                    Bytes::from(count.to_be_bytes().to_vec()),
                );
            }
        }
        writes.put_bytes(
            Bytes::from(self.state_key.clone()),
            Bytes::from(TOPN_PARTITION_COUNT_INITIALIZED.to_vec()),
        );
        self.table
            .write_batch(writes)
            .await
            .context("persist rebuilt topn partition counts")
    }

    async fn load_counts(&self, keys: &HashSet<Vec<u8>>) -> Result<HashMap<Vec<u8>, i64>> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }
        let mut counts = HashMap::with_capacity(keys.len());
        if keys.len() >= TOPN_PARTITION_COUNT_SCAN_MIN_KEYS {
            for (state_key, value) in self
                .table
                .scan_prefix_bytes(&self.count_prefix, &ScanOptions::default())
                .await
                .context("scan topn partition counts")?
            {
                let Some(partition_key) = state_key.strip_prefix(self.count_prefix.as_slice())
                else {
                    continue;
                };
                if keys.contains(partition_key) {
                    counts.insert(partition_key.to_vec(), decode_partition_count(&value)?);
                }
            }
            return Ok(counts);
        }

        for key in keys {
            if let Some(value) = self
                .table
                .get_bytes(&self.count_key(key))
                .await
                .context("load topn partition count")?
            {
                counts.insert(key.clone(), decode_partition_count(&value)?);
            }
        }
        Ok(counts)
    }

    async fn apply_deltas(
        &self,
        deltas: &HashMap<Vec<u8>, i64>,
        previous_counts: &HashMap<Vec<u8>, i64>,
    ) -> Result<()> {
        if deltas.is_empty() {
            return Ok(());
        }
        let mut writes = WriteBatch::new();
        let mut has_writes = false;
        for (partition_key, delta) in deltas {
            if *delta == 0 {
                continue;
            }
            let previous = previous_counts.get(partition_key).copied().unwrap_or(0);
            let next = previous
                .checked_add(*delta)
                .context("topn partition count overflow")?;
            if next < 0 {
                bail!("topn partition count became negative");
            }
            let state_key = self.count_key(partition_key);
            if next == 0 {
                writes.delete(state_key);
            } else {
                writes.put_bytes(
                    Bytes::from(state_key),
                    Bytes::from(next.to_be_bytes().to_vec()),
                );
            }
            has_writes = true;
        }
        if has_writes {
            self.table
                .write_batch(writes)
                .await
                .context("persist topn partition count deltas")?;
        }
        Ok(())
    }

    fn count_key(&self, partition_key: &[u8]) -> Vec<u8> {
        let mut key = self.count_prefix.clone();
        key.extend_from_slice(partition_key);
        key
    }
}

async fn prepare_topn_input_tick(
    columnar: &mut ColumnarTopNMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<TopNInputTick> {
    let total_start = profile::start();
    match &mut columnar.input {
        super::IncrementalInputOperator::Source(input_zset) => {
            let phase_start = profile::start();
            let append_only_source_delta = weighted_delta_batches
                .get(columnar.input_name.as_str())
                .is_none()
                && insert_batches
                    .get(columnar.input_name.as_str())
                    .is_some_and(|batches| batches.iter().any(|batch| batch.num_rows() > 0));
            let input_delta = if let Some(weighted_batches) =
                weighted_delta_batches.get(columnar.input_name.as_str())
            {
                ColumnarZSet::try_new_weighted(
                    Arc::clone(&columnar.source_schema),
                    weighted_batches.clone(),
                )
                .with_context(|| {
                    format!(
                        "build weighted topn input delta for '{}'",
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
                        "build insert topn input delta for '{}'",
                        columnar.input_name
                    )
                })?
            } else {
                ColumnarZSet::empty(Arc::clone(&columnar.source_schema))?
            };
            profile::record_since("topn.source_input_delta", phase_start);
            let phase_start = profile::start();
            let delta = persisted_source_delta(input_zset, input_delta).await?;
            profile::record_since("topn.persist_source_delta", phase_start);
            let input_changed = !delta.batches().is_empty();
            profile::record_since("topn.prepare_input_total", total_start);
            Ok(TopNInputTick {
                delta,
                input_changed,
                next_source_snapshot: None,
                append_only_source_delta,
            })
        }
        super::IncrementalInputOperator::GroupedStats(grouped_stats) => {
            let tick = run_columnar_grouped_stats_state_tick(
                grouped_stats,
                insert_batches,
                weighted_delta_batches,
                &columnar.source_schema,
                &columnar.source_snapshot,
            )
            .await
            .with_context(|| {
                format!(
                    "evaluate topn grouped-stats input '{}'",
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
                    append_only_source_delta: false,
                });
            }
            Ok(TopNInputTick {
                delta: tick.delta,
                input_changed,
                next_source_snapshot: input_changed.then_some(tick.next_snapshot),
                append_only_source_delta: false,
            })
        }
        unsupported => bail!(
            "topn received unsupported compiled input operator '{}'",
            unsupported.kind()
        ),
    }
}

fn lookup_key_batches_from_delta(
    delta_batches: &[RecordBatch],
    key_indices: &[usize],
    lookup_key_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    if key_indices.len() != lookup_key_schema.fields().len() {
        bail!("topn lookup key count does not match indexed key schema");
    }
    let mut key_batches = Vec::new();
    for batch in delta_batches.iter().filter(|batch| batch.num_rows() > 0) {
        let mut columns = Vec::with_capacity(key_indices.len());
        for (output_idx, input_idx) in key_indices.iter().copied().enumerate() {
            let column = batch.column(input_idx);
            let expected = lookup_key_schema.field(output_idx).data_type();
            if column.data_type() != expected {
                bail!(
                    "topn lookup key column {} type {:?} does not match indexed key type {:?}",
                    output_idx,
                    column.data_type(),
                    expected
                );
            }
            columns.push(Arc::clone(column));
        }
        key_batches.push(
            RecordBatch::try_new(Arc::clone(lookup_key_schema), columns)
                .context("build topn lookup key batch")?,
        );
    }
    Ok(key_batches)
}

async fn materialize_columnar_zset_values(zset: &ColumnarZSet) -> Result<Vec<RecordBatch>> {
    apply_weighted_snapshot_delta(&zset.value_schema(), &[], zset.batches().to_vec()).await
}

fn record_batch_row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn topn_source_is_under_limit_identity(
    columnar: &ColumnarTopNMaterializedViewState,
    output_schema: &SchemaRef,
    next_source_snapshot: &[RecordBatch],
    limit: usize,
) -> Result<bool> {
    if columnar.source_output_projection.is_none() {
        return Ok(false);
    }
    let projection = columnar
        .source_output_projection
        .as_ref()
        .context("topn source output projection missing")?;
    if projection.len() != output_schema.fields().len() {
        return Ok(false);
    }
    if !partition_row_counts_within_limit(
        &columnar.partition_converter,
        &columnar.partition_indices,
        &columnar.source_snapshot,
        limit,
    )? {
        return Ok(false);
    }
    partition_row_counts_within_limit(
        &columnar.partition_converter,
        &columnar.partition_indices,
        next_source_snapshot,
        limit,
    )
}

fn direct_topn_source_output_projection_indices(
    logical_plan: &LogicalPlan,
    source_schema: &SchemaRef,
    output_schema: &SchemaRef,
) -> Option<Vec<usize>> {
    let (rank_column, filter) = row_number_filter_for_plan(logical_plan)?;
    let (_window, projection_without_rank) =
        extract_window_plan(filter.input.as_ref(), &rank_column)?;
    let projection_without_rank = projection_without_rank?;
    direct_projection_indices_for_exprs(&projection_without_rank, source_schema, output_schema)
}

fn direct_projection_indices_for_exprs(
    exprs: &[Expr],
    input_schema: &SchemaRef,
    output_schema: &SchemaRef,
) -> Option<Vec<usize>> {
    if exprs.len() != output_schema.fields().len() {
        return None;
    }
    exprs
        .iter()
        .enumerate()
        .map(|(output_idx, expr)| {
            let Expr::Column(column) = super::columnar_utils::strip_alias(expr) else {
                return None;
            };
            let input_idx = input_schema.index_of(&column.name).ok()?;
            (input_schema.field(input_idx).data_type()
                == output_schema.field(output_idx).data_type())
            .then_some(input_idx)
        })
        .collect()
}

fn partition_row_counts_within_limit(
    converter: &RowConverter,
    partition_indices: &[usize],
    batches: &[RecordBatch],
    limit: usize,
) -> Result<bool> {
    if partition_indices.is_empty() {
        return Ok(record_batch_row_count(batches) <= limit);
    }
    let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let rows = converter
            .convert_columns(&project_columns(batch, partition_indices))
            .context("encode topn under-limit partition keys")?;
        for row_idx in 0..batch.num_rows() {
            let count = counts.entry(rows.row(row_idx).data().to_vec()).or_insert(0);
            *count = count.saturating_add(1);
            if *count > limit {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn partition_row_count_map(
    converter: &RowConverter,
    partition_indices: &[usize],
    batches: &[RecordBatch],
) -> Result<HashMap<Vec<u8>, i64>> {
    let mut counts = HashMap::new();
    if partition_indices.is_empty() {
        let count = i64::try_from(record_batch_row_count(batches))
            .context("topn global partition row count exceeds i64")?;
        if count > 0 {
            counts.insert(Vec::new(), count);
        }
        return Ok(counts);
    }
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let rows = converter
            .convert_columns(&project_columns(batch, partition_indices))
            .context("encode topn partition count keys")?;
        for row_idx in 0..batch.num_rows() {
            add_partition_count_delta(&mut counts, rows.row(row_idx).data().to_vec(), 1)?;
        }
    }
    Ok(counts)
}

fn partition_count_deltas_from_zset(
    converter: &RowConverter,
    partition_indices: &[usize],
    delta: &ColumnarZSet,
) -> Result<HashMap<Vec<u8>, i64>> {
    let mut deltas = HashMap::new();
    if partition_indices.is_empty() {
        let mut total = 0i64;
        for batch in delta.batches().iter().filter(|batch| batch.num_rows() > 0) {
            let weights = topn_weight_column(batch, delta.value_column_count())?;
            for row_idx in 0..batch.num_rows() {
                total = total
                    .checked_add(weights.value(row_idx))
                    .context("topn global partition count delta overflow")?;
            }
        }
        if total != 0 {
            deltas.insert(Vec::new(), total);
        }
        return Ok(deltas);
    }
    for batch in delta.batches().iter().filter(|batch| batch.num_rows() > 0) {
        let weights = topn_weight_column(batch, delta.value_column_count())?;
        let rows = converter
            .convert_columns(&project_columns(batch, partition_indices))
            .context("encode topn partition count delta keys")?;
        for row_idx in 0..batch.num_rows() {
            add_partition_count_delta(
                &mut deltas,
                rows.row(row_idx).data().to_vec(),
                weights.value(row_idx),
            )?;
        }
    }
    Ok(deltas)
}

fn partition_counts_with_deltas_within_limit(
    previous_counts: &HashMap<Vec<u8>, i64>,
    deltas: &HashMap<Vec<u8>, i64>,
    limit: usize,
) -> Result<bool> {
    let limit = i64::try_from(limit).context("topn row-number limit exceeds i64")?;
    for (partition_key, delta) in deltas {
        let previous = previous_counts.get(partition_key).copied().unwrap_or(0);
        if previous < 0 || previous > limit {
            return Ok(false);
        }
        let next = previous
            .checked_add(*delta)
            .context("topn partition count overflow")?;
        if next < 0 || next > limit {
            return Ok(false);
        }
    }
    Ok(true)
}

fn add_partition_count_delta(
    counts: &mut HashMap<Vec<u8>, i64>,
    partition_key: Vec<u8>,
    delta: i64,
) -> Result<()> {
    let entry = counts.entry(partition_key).or_insert(0);
    *entry = entry
        .checked_add(delta)
        .context("topn partition count delta overflow")?;
    Ok(())
}

fn topn_weight_column(batch: &RecordBatch, weight_idx: usize) -> Result<&Int64Array> {
    batch
        .column(weight_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .with_context(|| format!("topn weight column {weight_idx} is not Int64"))
}

fn decode_partition_count(bytes: &[u8]) -> Result<i64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .context("topn partition count state has invalid length")?;
    Ok(i64::from_be_bytes(bytes))
}

fn direct_project_weighted_columnar_zset(
    delta: &ColumnarZSet,
    value_schema: &SchemaRef,
    indices: &[usize],
) -> Result<ColumnarZSet> {
    if indices.len() != value_schema.fields().len() {
        bail!(
            "topn identity projection width {} does not match output width {}",
            indices.len(),
            value_schema.fields().len()
        );
    }
    let weighted_schema = weighted_snapshot_schema(value_schema)?;
    let mut batches = Vec::with_capacity(delta.batches().len());
    for batch in delta.batches().iter().filter(|batch| batch.num_rows() > 0) {
        let mut columns = Vec::with_capacity(weighted_schema.fields().len());
        for (output_idx, input_idx) in indices.iter().copied().enumerate() {
            let column = batch.column(input_idx);
            let expected_type = value_schema.field(output_idx).data_type();
            if column.data_type() != expected_type {
                bail!(
                    "topn identity delta column {} type {:?} does not match expected {:?}",
                    output_idx,
                    column.data_type(),
                    expected_type
                );
            }
            columns.push(Arc::clone(column));
        }
        let weight_idx = delta.value_column_count();
        columns.push(Arc::clone(batch.column(weight_idx)));
        batches.push(RecordBatch::try_new(Arc::clone(&weighted_schema), columns)?);
    }
    ColumnarZSet::try_new_weighted(Arc::clone(value_schema), batches)
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

fn schemas_match_by_position(input_schema: &SchemaRef, output_schema: &SchemaRef) -> bool {
    input_schema.fields().len() == output_schema.fields().len()
        && input_schema
            .fields()
            .iter()
            .zip(output_schema.fields())
            .all(|(input, output)| input.data_type() == output.data_type())
}

fn unit_positive_delta_value_batches(
    value_schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<Option<Vec<RecordBatch>>> {
    let mut output = Vec::with_capacity(batches.len());
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let Ok(weight_idx) = batch.schema().index_of(WEIGHT_COLUMN_NAME) else {
            return Ok(None);
        };
        let Some(weights) = batch
            .column(weight_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
        else {
            return Ok(None);
        };
        for row_idx in 0..weights.len() {
            if weights.is_null(row_idx) || weights.value(row_idx) != 1 {
                return Ok(None);
            }
        }
        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .filter(|(idx, _)| *idx != weight_idx)
            .map(|(_, column)| Arc::clone(column))
            .collect::<Vec<_>>();
        if columns.len() != value_schema.fields().len() {
            return Ok(None);
        }
        for (idx, field) in value_schema.fields().iter().enumerate() {
            if columns[idx].data_type() != field.data_type() {
                return Ok(None);
            }
        }
        output.push(RecordBatch::try_new(Arc::clone(value_schema), columns)?);
    }
    Ok(Some(output))
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
        let (alias_schema, alias_provider) = if let (Some(alias), Some(alias_schema)) =
            (source.alias_name.as_deref(), source.alias_schema.as_ref())
        {
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

fn split_batches_by_partition_keys(
    schema: &SchemaRef,
    converter: &RowConverter,
    partition_indices: &[usize],
    batches: &[RecordBatch],
    keys: &HashSet<Vec<u8>>,
) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>)> {
    if keys.is_empty() {
        return Ok((Vec::new(), batches.to_vec()));
    }
    if partition_indices.is_empty() {
        return Ok((batches.to_vec(), Vec::new()));
    }
    let mut matching = Vec::new();
    let mut remaining = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let rows = converter
            .convert_columns(&project_columns(batch, partition_indices))
            .context("encode topn snapshot partition keys")?;
        let mut matching_indices = Vec::new();
        let mut remaining_indices = Vec::new();
        for row_idx in 0..batch.num_rows() {
            let row_idx = u32::try_from(row_idx).context("topn batch exceeds u32 rows")?;
            if keys.contains(rows.row(row_idx as usize).data()) {
                matching_indices.push(row_idx);
            } else {
                remaining_indices.push(row_idx);
            }
        }
        if matching_indices.len() == batch.num_rows() {
            matching.push(batch.clone());
        } else if !matching_indices.is_empty() {
            matching.push(take_batch_rows(schema, batch, matching_indices)?);
        }
        if remaining_indices.len() == batch.num_rows() {
            remaining.push(batch.clone());
        } else if !remaining_indices.is_empty() {
            remaining.push(take_batch_rows(schema, batch, remaining_indices)?);
        }
    }
    Ok((matching, remaining))
}

fn take_batch_rows(
    schema: &SchemaRef,
    batch: &RecordBatch,
    indices: Vec<u32>,
) -> Result<RecordBatch> {
    let indices = UInt32Array::from(indices);
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<ArrayRef>, _>>()?;
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
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

fn append_only_direct_topn_state_for_source(
    source: &VectorizedSourceState,
    plan: Option<&AppendOnlyDirectTopNPlan>,
) -> Result<Option<AppendOnlyDirectTopNState>> {
    let Some(plan) = plan else {
        return Ok(None);
    };
    let mut orderings = Vec::with_capacity(plan.orderings.len());
    for ordering in &plan.orderings {
        let Ok(idx) = partition_column_index(source, &ordering.column) else {
            return Ok(None);
        };
        let data_type = source.schema.field(idx).data_type();
        if !direct_topn_order_type_supported(data_type) {
            return Ok(None);
        }
        orderings.push(AppendOnlyDirectTopNOrderingState {
            index: idx,
            asc: ordering.asc,
            nulls_first: ordering.nulls_first,
        });
    }
    if orderings.is_empty() {
        return Ok(None);
    }
    Ok(Some(AppendOnlyDirectTopNState {
        limit: plan.limit,
        orderings,
    }))
}

fn direct_topn_order_type_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int64
            | DataType::Utf8
            | DataType::Timestamp(TimeUnit::Millisecond, _)
            | DataType::Boolean
            | DataType::Date32
            | DataType::Decimal128(_, _)
            | DataType::Float64
            | DataType::UInt64
            | DataType::Null
    )
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
        LogicalPlan::Limit(mut limit)
            if limit_has_nonnegative_skip_and_positive_fetch(&limit)
                && sort_input_for_limit(limit.input.as_ref()) =>
        {
            limit.input = Arc::new(rebind_topn_limit_sort_input_logical_plan(
                limit.input.as_ref().clone(),
                input_name,
                provider,
            )?);
            Ok(LogicalPlan::Limit(limit))
        }
        LogicalPlan::Limit(mut limit) => {
            limit.input = Arc::new(rebind_topn_derived_input_logical_plan(
                limit.input.as_ref().clone(),
                input_name,
                provider,
            )?);
            Ok(LogicalPlan::Limit(limit))
        }
        LogicalPlan::Sort(mut sort) if sort_has_positive_fetch(&sort) => {
            sort.input = Arc::new(scan_plan_for_provider(input_name, provider)?);
            Ok(LogicalPlan::Sort(sort))
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

fn rebind_topn_limit_sort_input_logical_plan(
    logical_plan: LogicalPlan,
    input_name: &str,
    provider: Arc<dyn TableProvider>,
) -> Result<LogicalPlan> {
    match logical_plan {
        LogicalPlan::Projection(mut projection) => {
            projection.input = Arc::new(rebind_topn_limit_sort_input_logical_plan(
                projection.input.as_ref().clone(),
                input_name,
                provider,
            )?);
            Ok(LogicalPlan::Projection(projection))
        }
        LogicalPlan::SubqueryAlias(mut alias) => {
            if alias.alias.table() == input_name {
                return scan_plan_for_provider(input_name, provider);
            }
            alias.input = Arc::new(rebind_topn_limit_sort_input_logical_plan(
                alias.input.as_ref().clone(),
                input_name,
                provider,
            )?);
            Ok(LogicalPlan::SubqueryAlias(alias))
        }
        LogicalPlan::Sort(mut sort) if !sort.expr.is_empty() => {
            sort.input = Arc::new(scan_plan_for_provider(input_name, provider)?);
            Ok(LogicalPlan::Sort(sort))
        }
        other => rebind_topn_derived_input_logical_plan(other, input_name, provider),
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

fn grouped_stats_topn_input_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<(String, SchemaRef, ColumnarGroupedStatsPlan)>> {
    if !global_sort_limit_for_plan(plan) {
        return Ok(None);
    }
    let Some(input) = global_topn_input_plan(plan) else {
        return Ok(None);
    };
    let input_name = super::columnar_utils::derived_relation_name(input)
        .unwrap_or_else(|| "__floe_topn_grouped_stats_input".into());
    let schema = super::columnar_utils::df_schema_to_arrow(input.schema())?;
    let Some(grouped_stats) = columnar_grouped_stats_plan_for_plan(input, sources, &schema)? else {
        return Ok(None);
    };
    Ok(Some((input_name, schema, grouped_stats)))
}

fn global_topn_input_plan(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Projection(projection) => global_topn_input_plan(projection.input.as_ref()),
        LogicalPlan::Filter(filter) => global_topn_input_plan(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => global_topn_input_plan(alias.input.as_ref()),
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

fn append_only_direct_topn_plan(
    window_function: &WindowFunction,
    limit: usize,
) -> Option<AppendOnlyDirectTopNPlan> {
    if window_function.params.order_by.is_empty() {
        return None;
    }
    let orderings = window_function
        .params
        .order_by
        .iter()
        .map(|sort| {
            Some(AppendOnlyDirectTopNOrdering {
                column: partition_column_name(&sort.expr)?,
                asc: sort.asc,
                nulls_first: sort.nulls_first,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(AppendOnlyDirectTopNPlan { limit, orderings })
}

fn extract_standalone_row_number_upper_bound_limit(predicate: &Expr) -> Option<(String, usize)> {
    if let Expr::BinaryExpr(binary) = predicate
        && binary.op == Operator::And
    {
        return None;
    }
    extract_row_number_upper_bound_limit(predicate)
}

fn extract_row_number_upper_bound_limit(predicate: &Expr) -> Option<(String, usize)> {
    let Expr::BinaryExpr(binary) = predicate else {
        return None;
    };
    if binary.op == Operator::And {
        let left = extract_row_number_upper_bound_limit(binary.left.as_ref());
        let right = extract_row_number_upper_bound_limit(binary.right.as_ref());
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
        RowNumberPredicateKind::Equality => return None,
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
    match super::columnar_utils::strip_alias(expr) {
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

fn single_source_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Option<String> {
    let sources = super::columnar_utils::source_set_for_plan(plan, sources);
    if sources.len() == 1 {
        sources.into_iter().next()
    } else {
        None
    }
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
