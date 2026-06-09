use std::collections::{BTreeSet, HashMap, hash_map::Entry};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int64Array, Int64Builder,
    StringArray, TimestampMillisecondArray, UInt32Array,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, ScalarValue};
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

use crate::delta_consolidation::{add_weight_column_to_batches, weighted_snapshot_schema};
use crate::encoding::EncodedRowScalar;
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::scalar_array_builder::ScalarColumnBuilder;
use crate::table_provider::DynamicStateTableProvider;
use crate::vectorized_runtime::source_state::incremental_source_for_plan;
use crate::vectorized_source_delta::unit_source_delta_batches;

use super::columnar_join::{
    ColumnarJoinMaterializedViewState, ColumnarJoinPlan,
    build_columnar_join_materialized_view_state_in_namespace, columnar_join_plan_for_plan,
    run_columnar_join_state_tick,
};
use super::columnar_join_topn::{
    ColumnarJoinTopNMaterializedViewState, ColumnarJoinTopNPlan,
    build_columnar_join_topn_materialized_view_state_in_namespaces,
    columnar_join_topn_plan_for_plan, run_columnar_join_topn_state_tick,
};
use super::columnar_multijoin::{
    ColumnarMultiJoinMaterializedViewState, ColumnarMultiJoinPlan,
    build_columnar_multijoin_materialized_view_state_in_namespace,
    columnar_multijoin_plan_for_plan, run_columnar_multijoin_state_tick,
};
use super::columnar_topn::{
    ColumnarTopNMaterializedViewState, ColumnarTopNPlan,
    build_columnar_topn_materialized_view_state_in_namespace, columnar_topn_plan_for_plan,
    run_columnar_topn_state_tick,
};
use super::{
    IncrementalMaterializedViewState, VectorizedMaterializedViewState, VectorizedSourceState,
    apply_weighted_snapshot_delta, build_incremental_materialized_view_state_from_logical_plan,
    collect_incremental_output, normalize_batches,
};

const GROUP_TAG: u8 = b'g';
const SCALAR_TAG: u8 = b'a';
const MINMAX_TAG: u8 = b'm';
const VALUE_TAG: u8 = b'v';

pub(super) struct ColumnarGroupedStatsPlan {
    input: ColumnarGroupedStatsInputPlan,
    projection: Projection,
    projection_schema: SchemaRef,
    aggregate_schema: SchemaRef,
    group_schema: SchemaRef,
    specs: Vec<AggregateSpec>,
    output_mapping: Vec<usize>,
    group_count: usize,
    post_aggregate_plan: Option<LogicalPlan>,
}

impl ColumnarGroupedStatsPlan {
    pub(super) fn source_names(&self) -> BTreeSet<String> {
        match &self.input {
            ColumnarGroupedStatsInputPlan::Source { source_name } => {
                [source_name.clone()].into_iter().collect()
            }
            ColumnarGroupedStatsInputPlan::Join { plan, .. } => plan.source_names(),
            ColumnarGroupedStatsInputPlan::MultiJoin { plan, .. } => plan.source_names(),
            ColumnarGroupedStatsInputPlan::JoinTopN { plan, .. } => {
                plan.source_names().into_iter().collect()
            }
            ColumnarGroupedStatsInputPlan::TopN { plan, .. } => plan.source_names(),
        }
    }
}

enum ColumnarGroupedStatsInputPlan {
    Source {
        source_name: String,
    },
    Join {
        input_name: String,
        source_schema: SchemaRef,
        projection_input_schema: SchemaRef,
        plan: Box<ColumnarJoinPlan>,
    },
    MultiJoin {
        input_name: String,
        source_schema: SchemaRef,
        projection_input_schema: SchemaRef,
        plan: Box<ColumnarMultiJoinPlan>,
    },
    JoinTopN {
        input_name: String,
        source_schema: SchemaRef,
        projection_input_schema: SchemaRef,
        plan: Box<ColumnarJoinTopNPlan>,
    },
    TopN {
        input_name: String,
        source_schema: SchemaRef,
        projection_input_schema: SchemaRef,
        plan: Box<ColumnarTopNPlan>,
    },
}

pub(super) struct ColumnarGroupedStatsMaterializedViewState {
    input_name: String,
    source_schema: SchemaRef,
    input_zset: Option<SlateBackedColumnarZSet>,
    join: Option<Box<ColumnarJoinMaterializedViewState>>,
    multijoin: Option<Box<ColumnarMultiJoinMaterializedViewState>>,
    join_topn: Option<Box<ColumnarJoinTopNMaterializedViewState>>,
    topn: Option<Box<ColumnarTopNMaterializedViewState>>,
    input_snapshot: Vec<RecordBatch>,
    output_zset: SlateBackedColumnarZSet,
    stats_state: SlateGroupedStatsState,
    projection_delta: GroupedStatsProjectionState,
    projection_schema: SchemaRef,
    aggregate_schema: SchemaRef,
    post_aggregate: Option<PostAggregateTransformState>,
    group_schema: SchemaRef,
    specs: Vec<AggregateSpec>,
    output_mapping: Vec<usize>,
    group_count: usize,
    initial_snapshot: Vec<RecordBatch>,
}

enum GroupedStatsProjectionState {
    Source(IncrementalMaterializedViewState),
    Derived(GroupedStatsDerivedProjectionState),
}

struct GroupedStatsDerivedProjectionState {
    ctx: SessionContext,
    provider: Arc<DynamicStateTableProvider>,
    input_schema: SchemaRef,
    plan: Arc<dyn ExecutionPlan>,
}

impl ColumnarGroupedStatsMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

#[derive(Clone)]
struct AggregateSpec {
    kind: AggregateKind,
    value_idx: Option<usize>,
    filter_idx: Option<usize>,
    value_type: Option<AggregateValueType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregateKind {
    Count,
    DistinctCount,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregateValueType {
    Any,
    Int64,
    Utf8,
    TimestampMillis,
    DateDays,
    Bool,
    Decimal128,
}

struct SlateGroupedStatsState {
    table: Arc<dyn KeyValueTable>,
    key_prefix: Vec<u8>,
    assume_empty: bool,
    group_counts: Mutex<HashMap<Vec<u8>, i64>>,
    i64_values: Mutex<HashMap<(Vec<u8>, usize), i64>>,
    i128_values: Mutex<HashMap<(Vec<u8>, usize), i128>>,
    pairs: Mutex<HashMap<(Vec<u8>, usize), (i64, i64)>>,
    minmax_values: Mutex<HashMap<(Vec<u8>, usize), Option<i64>>>,
    i128_minmax_values: Mutex<HashMap<(Vec<u8>, usize), Option<i128>>>,
    value_counts: Mutex<HashMap<(Vec<u8>, usize, i64), i64>>,
    i128_value_counts: Mutex<HashMap<(Vec<u8>, usize, i128), i64>>,
    string_minmax_values: Mutex<HashMap<(Vec<u8>, usize), Option<String>>>,
    string_value_counts: Mutex<HashMap<(Vec<u8>, usize, String), i64>>,
}

struct GroupedStatsPlanMatch<'a> {
    aggregate: &'a Aggregate,
    projection: Option<&'a Projection>,
    post_aggregate_plan: Option<LogicalPlan>,
}

struct PostAggregateTransformState {
    ctx: SessionContext,
    provider: Arc<DynamicStateTableProvider>,
    plan: Arc<dyn ExecutionPlan>,
}

struct PendingStatsGroupDelta {
    row_count_delta: i64,
    agg_deltas: Vec<AggregateDelta>,
    batch: RecordBatch,
    row_idx: usize,
}

pub(super) struct ColumnarGroupedStatsTick {
    pub(super) delta: ColumnarZSet,
    pub(super) next_snapshot: Vec<RecordBatch>,
    pub(super) input_changed: bool,
}

#[derive(Clone)]
enum AggregateDelta {
    Count { count_delta: i64 },
    DistinctCountI64 { value_deltas: HashMap<i64, i64> },
    DistinctCountI128 { value_deltas: HashMap<i128, i64> },
    DistinctCountUtf8 { value_deltas: HashMap<String, i64> },
    Sum { sum_delta: i64 },
    SumI128 { sum_delta: i128 },
    Avg { sum_delta: i64, count_delta: i64 },
    MinMaxI64 { value_deltas: HashMap<i64, i64> },
    MinMaxI128 { value_deltas: HashMap<i128, i64> },
    MinMaxUtf8 { value_deltas: HashMap<String, i64> },
}

#[derive(Clone, PartialEq)]
enum AggregateValue {
    Int64(i64),
    Float64(f64),
    Utf8(String),
    TimestampMillis(i64),
    DateDays(i32),
    Decimal128(i128),
    Null,
}

const POST_AGGREGATE_SOURCE_NAME: &str = "__floe_grouped_stats_aggregate";

pub(super) fn columnar_grouped_stats_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    output_schema: &SchemaRef,
) -> Result<Option<ColumnarGroupedStatsPlan>> {
    let Some(plan_match) = grouped_stats_aggregate_for_plan(plan) else {
        return Ok(None);
    };
    let aggregate = plan_match.aggregate;
    if aggregate.aggr_expr.is_empty() {
        return Ok(None);
    }
    if aggregate
        .group_expr
        .iter()
        .any(|expr| matches!(expr, Expr::GroupingSet(_)))
    {
        return Ok(None);
    }

    let group_count = aggregate.group_expr.len();
    let aggregate_schema = df_schema_to_arrow(&aggregate.schema)?;
    if aggregate_schema.fields().len() != group_count + aggregate.aggr_expr.len() {
        return Ok(None);
    }
    let input = if let Some(source_name) =
        incremental_source_for_plan(aggregate.input.as_ref(), sources)
    {
        ColumnarGroupedStatsInputPlan::Source { source_name }
    } else if let Some(mut join) = columnar_join_plan_for_plan(aggregate.input.as_ref(), sources)? {
        join.force_snapshot_diff_execution();
        let source_schema = df_schema_to_arrow(aggregate.input.schema())?;
        let projection_input_schema = derived_projection_input_schema(&source_schema);
        let input_name = derived_relation_name(aggregate.input.as_ref())
            .unwrap_or_else(|| "__floe_grouped_stats_join_input".to_string());
        ColumnarGroupedStatsInputPlan::Join {
            input_name,
            source_schema,
            projection_input_schema,
            plan: Box::new(join),
        }
    } else if let Some(multijoin) =
        columnar_multijoin_plan_for_plan(aggregate.input.as_ref(), sources)?
    {
        let source_schema = df_schema_to_arrow(aggregate.input.schema())?;
        let projection_input_schema = derived_projection_input_schema(&source_schema);
        let input_name = derived_relation_name(aggregate.input.as_ref())
            .unwrap_or_else(|| "__floe_grouped_stats_multijoin_input".to_string());
        ColumnarGroupedStatsInputPlan::MultiJoin {
            input_name,
            source_schema,
            projection_input_schema,
            plan: Box::new(multijoin),
        }
    } else if let Some(join_topn) =
        columnar_join_topn_plan_for_plan(aggregate.input.as_ref(), sources)?
    {
        let source_schema = df_schema_to_arrow(aggregate.input.schema())?;
        let projection_input_schema = derived_projection_input_schema(&source_schema);
        let input_name = derived_relation_name(aggregate.input.as_ref())
            .unwrap_or_else(|| "__floe_grouped_stats_join_topn_input".to_string());
        ColumnarGroupedStatsInputPlan::JoinTopN {
            input_name,
            source_schema,
            projection_input_schema,
            plan: Box::new(join_topn),
        }
    } else if let Some(topn) = columnar_topn_plan_for_plan(aggregate.input.as_ref(), sources)? {
        let source_schema = df_schema_to_arrow(aggregate.input.schema())?;
        let projection_input_schema = derived_projection_input_schema(&source_schema);
        let input_name = derived_relation_name(aggregate.input.as_ref())
            .unwrap_or_else(|| "__floe_grouped_stats_topn_input".to_string());
        ColumnarGroupedStatsInputPlan::TopN {
            input_name,
            source_schema,
            projection_input_schema,
            plan: Box::new(topn),
        }
    } else {
        return Ok(None);
    };

    let mut projection_expr = aggregate.group_expr.clone();
    let mut specs = Vec::with_capacity(aggregate.aggr_expr.len());
    for (agg_idx, expr) in aggregate.aggr_expr.iter().enumerate() {
        let output_type = aggregate_schema.field(group_count + agg_idx).data_type();
        let Some(spec) = aggregate_spec_for_expr(expr, output_type, &mut projection_expr) else {
            return Ok(None);
        };
        specs.push(spec);
    }

    let mut post_aggregate_plan = plan_match.post_aggregate_plan.clone();
    let output_mapping = if post_aggregate_plan.is_some() {
        Vec::new()
    } else if let Some(mapping) =
        output_mapping_for_projection(plan_match.projection, aggregate, output_schema)
    {
        mapping
    } else if plan_match.projection.is_some() {
        post_aggregate_plan = Some(plan.clone());
        Vec::new()
    } else {
        return Ok(None);
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
    if let Some(post_plan) = post_aggregate_plan.as_ref() {
        let post_schema = df_schema_to_arrow(post_plan.schema())?;
        if post_schema.fields().len() != output_schema.fields().len() {
            return Ok(None);
        }
        for (output_field, post_field) in output_schema.fields().iter().zip(post_schema.fields()) {
            if output_field.data_type() != post_field.data_type() {
                return Ok(None);
            }
        }
    }
    if projection_expr.is_empty() {
        projection_expr.push(
            Expr::Literal(ScalarValue::Int64(Some(1)), None).alias("__floe_grouped_stats_row"),
        );
    }
    let projection_input = match &input {
        ColumnarGroupedStatsInputPlan::Source { .. } => aggregate.input.as_ref().clone(),
        ColumnarGroupedStatsInputPlan::Join {
            input_name,
            projection_input_schema,
            ..
        }
        | ColumnarGroupedStatsInputPlan::MultiJoin {
            input_name,
            projection_input_schema,
            ..
        }
        | ColumnarGroupedStatsInputPlan::JoinTopN {
            input_name,
            projection_input_schema,
            ..
        }
        | ColumnarGroupedStatsInputPlan::TopN {
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

    let projection_plan = Projection::try_new(projection_expr, Arc::new(projection_input))
        .context("build grouped-stats value projection")?;
    let projection_schema = df_schema_to_arrow(&projection_plan.schema)?;
    for spec in &mut specs {
        if let (Some(value_idx), Some(value_type)) = (spec.value_idx, spec.value_type) {
            let actual_type = projection_schema.field(value_idx).data_type();
            let supported = match value_type {
                AggregateValueType::Any if spec.kind == AggregateKind::DistinctCount => {
                    match actual_type {
                        DataType::Int64 => {
                            spec.value_type = Some(AggregateValueType::Int64);
                            true
                        }
                        DataType::Utf8 => {
                            spec.value_type = Some(AggregateValueType::Utf8);
                            true
                        }
                        DataType::Timestamp(TimeUnit::Millisecond, _) => {
                            spec.value_type = Some(AggregateValueType::TimestampMillis);
                            true
                        }
                        DataType::Date32 => {
                            spec.value_type = Some(AggregateValueType::DateDays);
                            true
                        }
                        DataType::Boolean => {
                            spec.value_type = Some(AggregateValueType::Bool);
                            true
                        }
                        DataType::Decimal128(_, _) => {
                            spec.value_type = Some(AggregateValueType::Decimal128);
                            true
                        }
                        _ => false,
                    }
                }
                AggregateValueType::Any => true,
                AggregateValueType::Int64 => actual_type == &DataType::Int64,
                AggregateValueType::Utf8 => actual_type == &DataType::Utf8,
                AggregateValueType::TimestampMillis => {
                    matches!(actual_type, DataType::Timestamp(TimeUnit::Millisecond, _))
                }
                AggregateValueType::DateDays => actual_type == &DataType::Date32,
                AggregateValueType::Bool => actual_type == &DataType::Boolean,
                AggregateValueType::Decimal128 => matches!(actual_type, DataType::Decimal128(_, _)),
            };
            if !supported {
                return Ok(None);
            }
        }
        if let Some(filter_idx) = spec.filter_idx
            && projection_schema.field(filter_idx).data_type() != &DataType::Boolean
        {
            return Ok(None);
        }
    }
    let group_fields = projection_schema
        .fields()
        .iter()
        .take(group_count)
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let group_schema = Arc::new(Schema::new(group_fields));

    Ok(Some(ColumnarGroupedStatsPlan {
        input,
        projection: projection_plan,
        projection_schema,
        aggregate_schema,
        group_schema,
        specs,
        output_mapping,
        group_count,
        post_aggregate_plan,
    }))
}

pub(super) async fn build_columnar_grouped_stats_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarGroupedStatsPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarGroupedStatsMaterializedViewState> {
    let mv_namespace = namespaces::materialized_view(view_name)?;
    build_columnar_grouped_stats_materialized_view_state_in_namespace(
        table,
        mv_namespace,
        output_schema,
        plan,
        sources,
        udfs,
    )
    .await
}

pub(super) async fn build_columnar_grouped_stats_materialized_view_state_in_namespace(
    table: Arc<dyn KeyValueTable>,
    mv_namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarGroupedStatsPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarGroupedStatsMaterializedViewState> {
    let output_namespace = format!("{mv_namespace}/columnar/grouped_stats/output");
    let state_namespace = format!("{mv_namespace}/columnar/grouped_stats/state");
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed grouped-stats output zset")?;
    let initial_snapshot = snapshot_batches_from_zset(
        &output_zset
            .materialize_columnar()
            .await
            .context("load grouped-stats output snapshot")?,
    )?;
    let (
        input_name,
        source_schema,
        input_zset,
        join,
        multijoin,
        join_topn,
        topn,
        input_snapshot,
        projection_delta,
    ) = match plan.input {
        ColumnarGroupedStatsInputPlan::Source { source_name } => {
            let source = sources
                .get(&source_name)
                .ok_or_else(|| anyhow::anyhow!("unknown vectorized source '{}'", source_name))?;
            let input_namespace = format!("{mv_namespace}/columnar/grouped_stats/input");
            let input_zset = Box::pin(SlateBackedColumnarZSet::new(
                Arc::clone(&table),
                input_namespace,
                Arc::clone(&source.schema),
            ))
            .await
            .context("initialize SlateDB-backed grouped-stats input zset")?;
            let projection_delta = build_incremental_materialized_view_state_from_logical_plan(
                &source_name,
                sources,
                udfs,
                &LogicalPlan::Projection(plan.projection.clone()),
            )
            .await
            .context("build grouped-stats vectorized projection delta plan")?;
            (
                source_name,
                Arc::clone(&source.schema),
                Some(input_zset),
                None,
                None,
                None,
                None,
                Vec::new(),
                GroupedStatsProjectionState::Source(projection_delta),
            )
        }
        ColumnarGroupedStatsInputPlan::Join {
            input_name,
            source_schema,
            projection_input_schema,
            plan: join_plan,
        } => {
            let join_namespace = format!("{mv_namespace}/columnar/grouped_stats/join_input");
            let join = Box::pin(build_boxed_join_grouped_stats_input_state(
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
                    "build SlateDB-backed grouped-stats join input for '{}'",
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
                    "build grouped-stats derived projection delta plan for '{}'",
                    input_name
                )
            })?;
            (
                input_name,
                source_schema,
                None,
                Some(join),
                None,
                None,
                None,
                input_snapshot,
                GroupedStatsProjectionState::Derived(projection_delta),
            )
        }
        ColumnarGroupedStatsInputPlan::MultiJoin {
            input_name,
            source_schema,
            projection_input_schema,
            plan: multijoin_plan,
        } => {
            let multijoin_namespace =
                format!("{mv_namespace}/columnar/grouped_stats/multijoin_input");
            let multijoin = Box::pin(build_boxed_multijoin_grouped_stats_input_state(
                Arc::clone(&table),
                multijoin_namespace,
                &source_schema,
                *multijoin_plan,
                sources,
                udfs,
            ))
            .await
            .with_context(|| {
                format!(
                    "build SlateDB-backed grouped-stats multijoin input for '{}'",
                    input_name
                )
            })?;
            let input_snapshot = multijoin.initial_snapshot();
            let projection_delta = build_derived_projection_state(
                LogicalPlan::Projection(plan.projection.clone()),
                &input_name,
                &projection_input_schema,
                udfs,
            )
            .await
            .with_context(|| {
                format!(
                    "build grouped-stats multijoin projection delta plan for '{}'",
                    input_name
                )
            })?;
            (
                input_name,
                source_schema,
                None,
                None,
                Some(multijoin),
                None,
                None,
                input_snapshot,
                GroupedStatsProjectionState::Derived(projection_delta),
            )
        }
        ColumnarGroupedStatsInputPlan::JoinTopN {
            input_name,
            source_schema,
            projection_input_schema,
            plan: join_topn_plan,
        } => {
            let join_topn_namespace =
                format!("{mv_namespace}/columnar/grouped_stats/join_topn_input");
            let join_topn = Box::pin(build_boxed_join_topn_grouped_stats_input_state(
                Arc::clone(&table),
                join_topn_namespace,
                &source_schema,
                *join_topn_plan,
                sources,
                udfs,
            ))
            .await
            .with_context(|| {
                format!(
                    "build SlateDB-backed grouped-stats join-topn input for '{}'",
                    input_name
                )
            })?;
            let input_snapshot = join_topn.initial_snapshot();
            let projection_delta = build_derived_projection_state(
                LogicalPlan::Projection(plan.projection.clone()),
                &input_name,
                &projection_input_schema,
                udfs,
            )
            .await
            .with_context(|| {
                format!(
                    "build grouped-stats join-topn projection delta plan for '{}'",
                    input_name
                )
            })?;
            (
                input_name,
                source_schema,
                None,
                None,
                None,
                Some(join_topn),
                None,
                input_snapshot,
                GroupedStatsProjectionState::Derived(projection_delta),
            )
        }
        ColumnarGroupedStatsInputPlan::TopN {
            input_name,
            source_schema,
            projection_input_schema,
            plan: topn_plan,
        } => {
            let topn_namespace = format!("{mv_namespace}/columnar/grouped_stats/topn_input");
            let topn = Box::pin(build_boxed_topn_grouped_stats_input_state(
                Arc::clone(&table),
                topn_namespace,
                &source_schema,
                *topn_plan,
                sources,
                udfs,
            ))
            .await
            .with_context(|| {
                format!(
                    "build SlateDB-backed grouped-stats topn input for '{}'",
                    input_name
                )
            })?;
            let input_snapshot = topn.initial_snapshot();
            let projection_delta = build_derived_projection_state(
                LogicalPlan::Projection(plan.projection.clone()),
                &input_name,
                &projection_input_schema,
                udfs,
            )
            .await
            .with_context(|| {
                format!(
                    "build grouped-stats topn projection delta plan for '{}'",
                    input_name
                )
            })?;
            (
                input_name,
                source_schema,
                None,
                None,
                None,
                None,
                Some(topn),
                input_snapshot,
                GroupedStatsProjectionState::Derived(projection_delta),
            )
        }
    };
    let post_aggregate = match plan.post_aggregate_plan {
        Some(post_plan) => Some(
            build_post_aggregate_transform_state(
                Arc::clone(&plan.aggregate_schema),
                &post_plan,
                udfs,
            )
            .await
            .context("build grouped-stats post-aggregate transform")?,
        ),
        None => None,
    };

    Ok(ColumnarGroupedStatsMaterializedViewState {
        input_name,
        source_schema,
        input_zset,
        join,
        multijoin,
        join_topn,
        topn,
        input_snapshot,
        stats_state: SlateGroupedStatsState::new(
            table,
            &state_namespace,
            output_zset.current_handle().is_none(),
        ),
        output_zset,
        projection_delta,
        projection_schema: plan.projection_schema,
        aggregate_schema: plan.aggregate_schema,
        post_aggregate,
        group_schema: plan.group_schema,
        specs: plan.specs,
        output_mapping: plan.output_mapping,
        group_count: plan.group_count,
        initial_snapshot,
    })
}

async fn build_boxed_join_grouped_stats_input_state(
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarJoinPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<Box<ColumnarJoinMaterializedViewState>> {
    Ok(Box::new(
        build_columnar_join_materialized_view_state_in_namespace(
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

async fn build_boxed_multijoin_grouped_stats_input_state(
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarMultiJoinPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<Box<ColumnarMultiJoinMaterializedViewState>> {
    Ok(Box::new(
        build_columnar_multijoin_materialized_view_state_in_namespace(
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

async fn build_boxed_join_topn_grouped_stats_input_state(
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarJoinTopNPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<Box<ColumnarJoinTopNMaterializedViewState>> {
    let left_namespace = format!("{namespace}/left/input");
    let right_namespace = format!("{namespace}/right/input");
    let output_namespace = format!("{namespace}/output");
    Ok(Box::new(
        build_columnar_join_topn_materialized_view_state_in_namespaces(
            table,
            left_namespace,
            right_namespace,
            output_namespace,
            output_schema,
            plan,
            sources,
            udfs,
        )
        .await?,
    ))
}

async fn build_boxed_topn_grouped_stats_input_state(
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarTopNPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<Box<ColumnarTopNMaterializedViewState>> {
    Ok(Box::new(
        build_columnar_topn_materialized_view_state_in_namespace(
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

async fn build_derived_projection_state(
    logical_plan: LogicalPlan,
    input_name: &str,
    input_schema: &SchemaRef,
    udfs: &[ScalarUDF],
) -> Result<GroupedStatsDerivedProjectionState> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    for udf in udfs.iter().cloned() {
        ctx.register_udf(udf);
    }
    let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(input_schema)));
    let logical_plan =
        rebind_derived_projection_plan(logical_plan, input_name, Arc::clone(&provider))?;
    let plan = ctx.state().create_physical_plan(&logical_plan).await?;
    Ok(GroupedStatsDerivedProjectionState {
        ctx,
        provider,
        input_schema: Arc::clone(input_schema),
        plan,
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

pub(super) async fn run_columnar_grouped_stats_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(columnar) = mv.columnar_grouped_stats.as_mut() else {
        return Ok(false);
    };

    let plan_start = Instant::now();
    let tick = run_columnar_grouped_stats_state_tick(
        columnar,
        insert_batches,
        weighted_delta_batches,
        &mv.output_schema,
        &mv.previous_snapshot,
    )
    .await
    .with_context(|| {
        format!(
            "evaluate Slate-backed grouped-stats columnar snapshot delta for '{}'",
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
        mode = "columnar_grouped_stats",
        "SlateDB-backed grouped-stats columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

pub(super) async fn run_columnar_grouped_stats_state_tick(
    columnar: &mut ColumnarGroupedStatsMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    output_schema: &SchemaRef,
    previous_snapshot: &[RecordBatch],
) -> Result<ColumnarGroupedStatsTick> {
    let persisted_input_delta =
        prepare_grouped_stats_input_delta(columnar, insert_batches, weighted_delta_batches).await?;
    let input_changed = !persisted_input_delta.batches().is_empty();
    let pending = grouped_stats_pending_delta(columnar, persisted_input_delta.batches()).await?;
    let output_delta_batches = apply_grouped_stats_delta(columnar, pending).await?;
    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build grouped-stats output zset delta")?;
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
            .context("apply Slate-backed grouped-stats columnar snapshot delta")?;

    Ok(ColumnarGroupedStatsTick {
        delta: persisted_output_delta,
        next_snapshot,
        input_changed,
    })
}

async fn prepare_grouped_stats_input_delta(
    columnar: &mut ColumnarGroupedStatsMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    if columnar.join.is_some() {
        return prepare_join_grouped_stats_input_delta(
            columnar,
            insert_batches,
            weighted_delta_batches,
        )
        .await;
    }
    if columnar.multijoin.is_some() {
        return prepare_multijoin_grouped_stats_input_delta(
            columnar,
            insert_batches,
            weighted_delta_batches,
        )
        .await;
    }
    if columnar.join_topn.is_some() {
        return prepare_join_topn_grouped_stats_input_delta(
            columnar,
            insert_batches,
            weighted_delta_batches,
        )
        .await;
    }
    if columnar.topn.is_some() {
        return prepare_topn_grouped_stats_input_delta(
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
                    "build weighted grouped-stats input delta for '{}'",
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
                    "build insert grouped-stats input delta for '{}'",
                    columnar.input_name
                )
            })?
        } else {
            ColumnarZSet::empty(Arc::clone(&columnar.source_schema))?
        };

    let input_zset = columnar
        .input_zset
        .as_mut()
        .context("grouped-stats source input zset missing")?;
    if let Some(handle) = input_zset.create_version(&input_delta, None).await? {
        input_zset.read_delta(&handle).await
    } else {
        Ok(input_delta)
    }
}

async fn prepare_join_grouped_stats_input_delta(
    columnar: &mut ColumnarGroupedStatsMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    let Some(join) = columnar.join.as_mut() else {
        return ColumnarZSet::empty(Arc::clone(&columnar.source_schema));
    };
    let tick = Box::pin(run_columnar_join_state_tick(
        join.as_mut(),
        insert_batches,
        weighted_delta_batches,
        &columnar.source_schema,
        &columnar.input_snapshot,
    ))
    .await
    .with_context(|| {
        format!(
            "evaluate grouped-stats nested join input '{}'",
            columnar.input_name
        )
    })?;
    if tick.input_changed {
        columnar.input_snapshot = tick.next_snapshot;
    }
    Ok(tick.delta)
}

async fn prepare_multijoin_grouped_stats_input_delta(
    columnar: &mut ColumnarGroupedStatsMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    let Some(multijoin) = columnar.multijoin.as_mut() else {
        return ColumnarZSet::empty(Arc::clone(&columnar.source_schema));
    };
    let tick = Box::pin(run_columnar_multijoin_state_tick(
        multijoin.as_mut(),
        insert_batches,
        weighted_delta_batches,
        &columnar.source_schema,
        &columnar.input_snapshot,
    ))
    .await
    .with_context(|| {
        format!(
            "evaluate grouped-stats nested multijoin input '{}'",
            columnar.input_name
        )
    })?;
    if tick.input_changed {
        columnar.input_snapshot = tick.next_snapshot;
    }
    Ok(tick.delta)
}

async fn prepare_join_topn_grouped_stats_input_delta(
    columnar: &mut ColumnarGroupedStatsMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    let Some(join_topn) = columnar.join_topn.as_mut() else {
        return ColumnarZSet::empty(Arc::clone(&columnar.source_schema));
    };
    let tick = Box::pin(run_columnar_join_topn_state_tick(
        join_topn.as_mut(),
        insert_batches,
        weighted_delta_batches,
        &columnar.source_schema,
        &columnar.input_snapshot,
    ))
    .await
    .with_context(|| {
        format!(
            "evaluate grouped-stats nested join-topn input '{}'",
            columnar.input_name
        )
    })?;
    if tick.input_changed {
        columnar.input_snapshot = tick.next_snapshot;
    }
    Ok(tick.delta)
}

async fn prepare_topn_grouped_stats_input_delta(
    columnar: &mut ColumnarGroupedStatsMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    let Some(topn) = columnar.topn.as_mut() else {
        return ColumnarZSet::empty(Arc::clone(&columnar.source_schema));
    };
    let tick = Box::pin(run_columnar_topn_state_tick(
        topn.as_mut(),
        insert_batches,
        weighted_delta_batches,
        &columnar.source_schema,
        &columnar.input_snapshot,
    ))
    .await
    .with_context(|| {
        format!(
            "evaluate grouped-stats nested topn input '{}'",
            columnar.input_name
        )
    })?;
    if tick.input_changed {
        columnar.input_snapshot = tick.next_snapshot;
    }
    Ok(tick.delta)
}

async fn grouped_stats_pending_delta(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    input_batches: &[RecordBatch],
) -> Result<HashMap<Vec<u8>, PendingStatsGroupDelta>> {
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
                    "grouped-stats materialized view received non-unit weighted source deltas for '{}'",
                    columnar.input_name
                )
            })?;
        positive_source_batches.extend(unit_delta.positive);
        negative_source_batches.extend(unit_delta.negative);
    }

    let positive_output =
        collect_grouped_stats_projection_output(columnar, &positive_source_batches).await?;
    add_projected_stats_batches_to_pending(columnar, &positive_output, 1, &mut pending)?;
    let negative_output =
        collect_grouped_stats_projection_output(columnar, &negative_source_batches).await?;
    add_projected_stats_batches_to_pending(columnar, &negative_output, -1, &mut pending)?;
    pending.retain(|_, delta| {
        delta.row_count_delta != 0 || !aggregate_deltas_empty(&delta.agg_deltas)
    });
    Ok(pending)
}

async fn collect_grouped_stats_projection_output(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    source_batches: &[RecordBatch],
) -> Result<Vec<RecordBatch>> {
    if source_batches.is_empty() {
        return Ok(Vec::new());
    }
    match &columnar.projection_delta {
        GroupedStatsProjectionState::Source(incremental) => {
            collect_incremental_output(incremental, source_batches, &columnar.projection_schema)
                .await
        }
        GroupedStatsProjectionState::Derived(derived) => {
            let provider_batches =
                rewrap_record_batches_with_schema(source_batches, &derived.input_schema)?;
            derived.provider.set_batches(provider_batches)?;
            let collected = collect(Arc::clone(&derived.plan), derived.ctx.task_ctx()).await;
            derived.provider.set_batches(Vec::new())?;
            normalize_batches(
                collected.context("execute grouped-stats derived projection")?,
                &columnar.projection_schema,
            )
        }
    }
}

fn add_projected_stats_batches_to_pending(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    batches: &[RecordBatch],
    sign: i64,
    pending: &mut HashMap<Vec<u8>, PendingStatsGroupDelta>,
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    let converter = if columnar.group_count == 0 {
        None
    } else {
        Some(row_converter_for_schema(&columnar.group_schema)?)
    };
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let value_arrays = projected_value_arrays(batch, &columnar.specs)?;
        let filter_arrays = projected_filter_arrays(batch, &columnar.specs)?;
        match converter.as_ref() {
            Some(converter) => {
                let group_columns = (0..columnar.group_count)
                    .map(|idx| Arc::clone(batch.column(idx)))
                    .collect::<Vec<ArrayRef>>();
                let group_rows = converter
                    .convert_columns(&group_columns)
                    .context("encode grouped-stats group keys")?;
                for row_idx in 0..batch.num_rows() {
                    add_projected_stats_row_to_pending(
                        columnar,
                        batch,
                        row_idx,
                        group_rows.row(row_idx).data().to_vec(),
                        &value_arrays,
                        &filter_arrays,
                        sign,
                        pending,
                    )?;
                }
            }
            None => {
                for row_idx in 0..batch.num_rows() {
                    add_projected_stats_row_to_pending(
                        columnar,
                        batch,
                        row_idx,
                        Vec::new(),
                        &value_arrays,
                        &filter_arrays,
                        sign,
                        pending,
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn add_projected_stats_row_to_pending(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    batch: &RecordBatch,
    row_idx: usize,
    key: Vec<u8>,
    value_arrays: &[ProjectedValueArray<'_>],
    filter_arrays: &[Option<&BooleanArray>],
    sign: i64,
    pending: &mut HashMap<Vec<u8>, PendingStatsGroupDelta>,
) -> Result<()> {
    let group = pending
        .entry(key)
        .or_insert_with(|| PendingStatsGroupDelta {
            row_count_delta: 0,
            agg_deltas: columnar
                .specs
                .iter()
                .map(AggregateDelta::for_spec)
                .collect(),
            batch: batch.clone(),
            row_idx,
        });
    group.row_count_delta = group
        .row_count_delta
        .checked_add(sign)
        .ok_or_else(|| anyhow::anyhow!("grouped-stats row count delta overflow"))?;
    for (agg_idx, spec) in columnar.specs.iter().enumerate() {
        if !filter_allows(&filter_arrays[agg_idx], row_idx) {
            continue;
        }
        match (&mut group.agg_deltas[agg_idx], spec.kind) {
            (AggregateDelta::Count { count_delta }, AggregateKind::Count) => {
                if spec.value_idx.is_some()
                    && !projected_value_is_non_null(&value_arrays[agg_idx], row_idx)
                {
                    continue;
                }
                *count_delta = count_delta
                    .checked_add(sign)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats count delta overflow"))?;
            }
            (AggregateDelta::DistinctCountI64 { value_deltas }, AggregateKind::DistinctCount) => {
                let Some(value) = projected_distinct_i64_value(&value_arrays[agg_idx], row_idx)
                else {
                    continue;
                };
                update_i64_value_delta(value_deltas, value, sign)?;
            }
            (AggregateDelta::DistinctCountI128 { value_deltas }, AggregateKind::DistinctCount) => {
                let Some(value) = projected_i128_value(&value_arrays[agg_idx], row_idx) else {
                    continue;
                };
                update_i128_value_delta(value_deltas, value, sign)?;
            }
            (AggregateDelta::DistinctCountUtf8 { value_deltas }, AggregateKind::DistinctCount) => {
                let Some(value) = projected_utf8_value(&value_arrays[agg_idx], row_idx) else {
                    continue;
                };
                update_string_value_delta(value_deltas, value.to_string(), sign)?;
            }
            (AggregateDelta::Sum { sum_delta }, AggregateKind::Sum) => {
                let Some(value) = projected_i64_value(&value_arrays[agg_idx], row_idx) else {
                    continue;
                };
                let signed = value
                    .checked_mul(sign)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats sum delta overflow"))?;
                *sum_delta = sum_delta
                    .checked_add(signed)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats sum delta overflow"))?;
            }
            (AggregateDelta::SumI128 { sum_delta }, AggregateKind::Sum) => {
                let Some(value) = projected_i128_value(&value_arrays[agg_idx], row_idx) else {
                    continue;
                };
                let signed = value
                    .checked_mul(i128::from(sign))
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats decimal sum delta overflow"))?;
                *sum_delta = sum_delta
                    .checked_add(signed)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats decimal sum delta overflow"))?;
            }
            (
                AggregateDelta::Avg {
                    sum_delta,
                    count_delta,
                },
                AggregateKind::Avg,
            ) => {
                let Some(value) = projected_i64_value(&value_arrays[agg_idx], row_idx) else {
                    continue;
                };
                let signed = value
                    .checked_mul(sign)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats avg sum delta overflow"))?;
                *sum_delta = sum_delta
                    .checked_add(signed)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats avg sum delta overflow"))?;
                *count_delta = count_delta
                    .checked_add(sign)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats avg count delta overflow"))?;
            }
            (
                AggregateDelta::MinMaxI64 { value_deltas },
                AggregateKind::Min | AggregateKind::Max,
            ) => {
                let Some(value) = projected_ordered_i64_value(&value_arrays[agg_idx], row_idx)
                else {
                    continue;
                };
                update_i64_value_delta(value_deltas, value, sign)?;
            }
            (
                AggregateDelta::MinMaxI128 { value_deltas },
                AggregateKind::Min | AggregateKind::Max,
            ) => {
                let Some(value) = projected_i128_value(&value_arrays[agg_idx], row_idx) else {
                    continue;
                };
                update_i128_value_delta(value_deltas, value, sign)?;
            }
            (
                AggregateDelta::MinMaxUtf8 { value_deltas },
                AggregateKind::Min | AggregateKind::Max,
            ) => {
                let Some(value) = projected_utf8_value(&value_arrays[agg_idx], row_idx) else {
                    continue;
                };
                update_string_value_delta(value_deltas, value.to_string(), sign)?;
            }
            _ => bail!("grouped-stats aggregate delta kind mismatch"),
        }
    }
    Ok(())
}

async fn apply_grouped_stats_delta(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    pending: HashMap<Vec<u8>, PendingStatsGroupDelta>,
) -> Result<Vec<RecordBatch>> {
    let mut direct_builder = WeightedStatsOutputBuilder::new(
        columnar.output_zset.value_schema(),
        &columnar.output_mapping,
    )?;
    let mut old_aggregate_builder = AggregateStatsOutputBuilder::new(
        Arc::clone(&columnar.aggregate_schema),
        columnar.group_count,
    )?;
    let mut new_aggregate_builder = AggregateStatsOutputBuilder::new(
        Arc::clone(&columnar.aggregate_schema),
        columnar.group_count,
    )?;
    if pending.is_empty() {
        return direct_builder.finish();
    }

    let mut writes = WriteBatch::new();
    for (group_key, delta) in pending {
        let old_row_count = columnar.stats_state.load_group_count(&group_key).await?;
        let old_values = load_aggregate_values(columnar, &group_key).await?;
        let new_row_count = old_row_count
            .checked_add(delta.row_count_delta)
            .ok_or_else(|| anyhow::anyhow!("grouped-stats row count overflow"))?;
        if new_row_count < 0 {
            bail!("grouped-stats state removed more rows than were present");
        }
        let new_values =
            apply_aggregate_deltas(columnar, &group_key, &delta.agg_deltas, &mut writes).await?;
        columnar
            .stats_state
            .write_group_count(&mut writes, &group_key, new_row_count)?;

        if old_row_count > 0 && (new_row_count == 0 || old_values != new_values) {
            if columnar.post_aggregate.is_some() {
                old_aggregate_builder.append(&delta.batch, delta.row_idx, &old_values)?;
            } else {
                direct_builder.append(
                    &delta.batch,
                    delta.row_idx,
                    columnar.group_count,
                    &old_values,
                    -1,
                )?;
            }
        }
        if new_row_count > 0 && (old_row_count == 0 || old_values != new_values) {
            if columnar.post_aggregate.is_some() {
                new_aggregate_builder.append(&delta.batch, delta.row_idx, &new_values)?;
            } else {
                direct_builder.append(
                    &delta.batch,
                    delta.row_idx,
                    columnar.group_count,
                    &new_values,
                    1,
                )?;
            }
        }
    }
    let output_delta_batches = if let Some(post_aggregate) = columnar.post_aggregate.as_ref() {
        post_aggregate_delta_batches(
            post_aggregate,
            columnar.output_zset.value_schema(),
            old_aggregate_builder.finish()?,
            new_aggregate_builder.finish()?,
        )
        .await?
    } else {
        direct_builder.finish()?
    };
    columnar
        .stats_state
        .table
        .write_batch(writes)
        .await
        .context("persist grouped-stats state updates")?;
    Ok(output_delta_batches)
}

async fn load_aggregate_values(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    group_key: &[u8],
) -> Result<Vec<AggregateValue>> {
    let mut values = Vec::with_capacity(columnar.specs.len());
    for (idx, spec) in columnar.specs.iter().enumerate() {
        values.push(match spec.kind {
            AggregateKind::Count => {
                AggregateValue::Int64(columnar.stats_state.load_i64(group_key, idx).await?)
            }
            AggregateKind::DistinctCount => {
                AggregateValue::Int64(columnar.stats_state.load_i64(group_key, idx).await?)
            }
            AggregateKind::Sum => match spec.value_type {
                Some(AggregateValueType::Decimal128) => AggregateValue::Decimal128(
                    columnar.stats_state.load_i128(group_key, idx).await?,
                ),
                _ => AggregateValue::Int64(columnar.stats_state.load_i64(group_key, idx).await?),
            },
            AggregateKind::Avg => {
                let (sum, count) = columnar.stats_state.load_pair(group_key, idx).await?;
                if count == 0 {
                    AggregateValue::Null
                } else {
                    AggregateValue::Float64(sum as f64 / count as f64)
                }
            }
            AggregateKind::Min | AggregateKind::Max => match spec.value_type {
                Some(AggregateValueType::Int64) => columnar
                    .stats_state
                    .load_minmax(group_key, idx)
                    .await?
                    .map(AggregateValue::Int64)
                    .unwrap_or(AggregateValue::Null),
                Some(AggregateValueType::TimestampMillis | AggregateValueType::DateDays) => {
                    columnar
                        .stats_state
                        .load_minmax(group_key, idx)
                        .await?
                        .map(|value| aggregate_value_from_ordered_i64(spec.value_type, value))
                        .transpose()?
                        .unwrap_or(AggregateValue::Null)
                }
                Some(AggregateValueType::Utf8) => columnar
                    .stats_state
                    .load_string_minmax(group_key, idx)
                    .await?
                    .map(AggregateValue::Utf8)
                    .unwrap_or(AggregateValue::Null),
                Some(AggregateValueType::Decimal128) => columnar
                    .stats_state
                    .load_i128_minmax(group_key, idx)
                    .await?
                    .map(AggregateValue::Decimal128)
                    .unwrap_or(AggregateValue::Null),
                Some(AggregateValueType::Any | AggregateValueType::Bool) | None => {
                    AggregateValue::Null
                }
            },
        });
    }
    Ok(values)
}

async fn apply_aggregate_deltas(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    group_key: &[u8],
    deltas: &[AggregateDelta],
    writes: &mut WriteBatch,
) -> Result<Vec<AggregateValue>> {
    let mut values = Vec::with_capacity(columnar.specs.len());
    for (idx, (spec, delta)) in columnar.specs.iter().zip(deltas.iter()).enumerate() {
        values.push(match (spec.kind, delta) {
            (AggregateKind::Count, AggregateDelta::Count { count_delta }) => {
                let old = columnar.stats_state.load_i64(group_key, idx).await?;
                let new = old
                    .checked_add(*count_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats count overflow"))?;
                if new < 0 {
                    bail!("grouped-stats count became negative");
                }
                columnar
                    .stats_state
                    .write_i64(writes, group_key, idx, new)?;
                AggregateValue::Int64(new)
            }
            (AggregateKind::DistinctCount, AggregateDelta::DistinctCountI64 { value_deltas }) => {
                let old = columnar.stats_state.load_i64(group_key, idx).await?;
                let mut new = old;
                for (value, value_delta) in value_deltas {
                    let old_count = columnar
                        .stats_state
                        .load_value_count(group_key, idx, *value)
                        .await?;
                    let new_count = old_count.checked_add(*value_delta).ok_or_else(|| {
                        anyhow::anyhow!("grouped-stats distinct value count overflow")
                    })?;
                    if new_count < 0 {
                        bail!("grouped-stats distinct removed more values than were present");
                    }
                    if old_count == 0 && new_count > 0 {
                        new = new
                            .checked_add(1)
                            .ok_or_else(|| anyhow::anyhow!("grouped-stats distinct overflow"))?;
                    } else if old_count > 0 && new_count == 0 {
                        new = new
                            .checked_sub(1)
                            .ok_or_else(|| anyhow::anyhow!("grouped-stats distinct underflow"))?;
                    }
                    columnar
                        .stats_state
                        .write_value_count(writes, group_key, idx, *value, new_count)?;
                }
                if new < 0 {
                    bail!("grouped-stats distinct count became negative");
                }
                columnar
                    .stats_state
                    .write_i64(writes, group_key, idx, new)?;
                AggregateValue::Int64(new)
            }
            (AggregateKind::DistinctCount, AggregateDelta::DistinctCountUtf8 { value_deltas }) => {
                let old = columnar.stats_state.load_i64(group_key, idx).await?;
                let mut new = old;
                for (value, value_delta) in value_deltas {
                    let old_count = columnar
                        .stats_state
                        .load_string_value_count(group_key, idx, value)
                        .await?;
                    let new_count = old_count.checked_add(*value_delta).ok_or_else(|| {
                        anyhow::anyhow!("grouped-stats string distinct value count overflow")
                    })?;
                    if new_count < 0 {
                        bail!(
                            "grouped-stats string distinct removed more values than were present"
                        );
                    }
                    if old_count == 0 && new_count > 0 {
                        new = new.checked_add(1).ok_or_else(|| {
                            anyhow::anyhow!("grouped-stats string distinct overflow")
                        })?;
                    } else if old_count > 0 && new_count == 0 {
                        new = new.checked_sub(1).ok_or_else(|| {
                            anyhow::anyhow!("grouped-stats string distinct underflow")
                        })?;
                    }
                    columnar
                        .stats_state
                        .write_string_value_count(writes, group_key, idx, value, new_count)?;
                }
                if new < 0 {
                    bail!("grouped-stats string distinct count became negative");
                }
                columnar
                    .stats_state
                    .write_i64(writes, group_key, idx, new)?;
                AggregateValue::Int64(new)
            }
            (AggregateKind::DistinctCount, AggregateDelta::DistinctCountI128 { value_deltas }) => {
                let old = columnar.stats_state.load_i64(group_key, idx).await?;
                let mut new = old;
                for (value, value_delta) in value_deltas {
                    let old_count = columnar
                        .stats_state
                        .load_i128_value_count(group_key, idx, *value)
                        .await?;
                    let new_count = old_count.checked_add(*value_delta).ok_or_else(|| {
                        anyhow::anyhow!("grouped-stats decimal distinct value count overflow")
                    })?;
                    if new_count < 0 {
                        bail!(
                            "grouped-stats decimal distinct removed more values than were present"
                        );
                    }
                    if old_count == 0 && new_count > 0 {
                        new = new.checked_add(1).ok_or_else(|| {
                            anyhow::anyhow!("grouped-stats decimal distinct overflow")
                        })?;
                    } else if old_count > 0 && new_count == 0 {
                        new = new.checked_sub(1).ok_or_else(|| {
                            anyhow::anyhow!("grouped-stats decimal distinct underflow")
                        })?;
                    }
                    columnar
                        .stats_state
                        .write_i128_value_count(writes, group_key, idx, *value, new_count)?;
                }
                if new < 0 {
                    bail!("grouped-stats decimal distinct count became negative");
                }
                columnar
                    .stats_state
                    .write_i64(writes, group_key, idx, new)?;
                AggregateValue::Int64(new)
            }
            (AggregateKind::Sum, AggregateDelta::Sum { sum_delta }) => {
                let old = columnar.stats_state.load_i64(group_key, idx).await?;
                let new = old
                    .checked_add(*sum_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats sum overflow"))?;
                columnar
                    .stats_state
                    .write_i64(writes, group_key, idx, new)?;
                AggregateValue::Int64(new)
            }
            (AggregateKind::Sum, AggregateDelta::SumI128 { sum_delta }) => {
                let old = columnar.stats_state.load_i128(group_key, idx).await?;
                let new = old
                    .checked_add(*sum_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats decimal sum overflow"))?;
                columnar
                    .stats_state
                    .write_i128(writes, group_key, idx, new)?;
                AggregateValue::Decimal128(new)
            }
            (
                AggregateKind::Avg,
                AggregateDelta::Avg {
                    sum_delta,
                    count_delta,
                },
            ) => {
                let (old_sum, old_count) = columnar.stats_state.load_pair(group_key, idx).await?;
                let new_sum = old_sum
                    .checked_add(*sum_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats avg sum overflow"))?;
                let new_count = old_count
                    .checked_add(*count_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats avg count overflow"))?;
                if new_count < 0 {
                    bail!("grouped-stats avg count became negative");
                }
                columnar
                    .stats_state
                    .write_pair(writes, group_key, idx, new_sum, new_count)?;
                if new_count == 0 {
                    AggregateValue::Null
                } else {
                    AggregateValue::Float64(new_sum as f64 / new_count as f64)
                }
            }
            (
                AggregateKind::Min | AggregateKind::Max,
                AggregateDelta::MinMaxI64 { value_deltas },
            ) => {
                let old = columnar.stats_state.load_minmax(group_key, idx).await?;
                let mut updated_counts = HashMap::new();
                for (value, value_delta) in value_deltas {
                    let old_count = columnar
                        .stats_state
                        .load_value_count(group_key, idx, *value)
                        .await?;
                    let new_count = old_count.checked_add(*value_delta).ok_or_else(|| {
                        anyhow::anyhow!("grouped-stats min/max value count overflow")
                    })?;
                    if new_count < 0 {
                        bail!("grouped-stats min/max removed more values than were present");
                    }
                    updated_counts.insert(*value, new_count);
                    columnar
                        .stats_state
                        .write_value_count(writes, group_key, idx, *value, new_count)?;
                }
                let new = columnar
                    .stats_state
                    .new_minmax_after_delta(group_key, idx, spec.kind, old, &updated_counts)
                    .await?;
                columnar
                    .stats_state
                    .write_minmax(writes, group_key, idx, new)?;
                new.map(|value| aggregate_value_from_ordered_i64(spec.value_type, value))
                    .transpose()?
                    .unwrap_or(AggregateValue::Null)
            }
            (
                AggregateKind::Min | AggregateKind::Max,
                AggregateDelta::MinMaxUtf8 { value_deltas },
            ) => {
                let old = columnar
                    .stats_state
                    .load_string_minmax(group_key, idx)
                    .await?;
                let mut updated_counts = HashMap::new();
                for (value, value_delta) in value_deltas {
                    let old_count = columnar
                        .stats_state
                        .load_string_value_count(group_key, idx, value)
                        .await?;
                    let new_count = old_count.checked_add(*value_delta).ok_or_else(|| {
                        anyhow::anyhow!("grouped-stats string min/max value count overflow")
                    })?;
                    if new_count < 0 {
                        bail!("grouped-stats string min/max removed more values than were present");
                    }
                    updated_counts.insert(value.clone(), new_count);
                    columnar
                        .stats_state
                        .write_string_value_count(writes, group_key, idx, value, new_count)?;
                }
                let new = columnar
                    .stats_state
                    .new_string_minmax_after_delta(group_key, idx, spec.kind, old, &updated_counts)
                    .await?;
                columnar
                    .stats_state
                    .write_string_minmax(writes, group_key, idx, new.as_deref())?;
                new.map(AggregateValue::Utf8)
                    .unwrap_or(AggregateValue::Null)
            }
            (
                AggregateKind::Min | AggregateKind::Max,
                AggregateDelta::MinMaxI128 { value_deltas },
            ) => {
                let old = columnar
                    .stats_state
                    .load_i128_minmax(group_key, idx)
                    .await?;
                let mut updated_counts = HashMap::new();
                for (value, value_delta) in value_deltas {
                    let old_count = columnar
                        .stats_state
                        .load_i128_value_count(group_key, idx, *value)
                        .await?;
                    let new_count = old_count.checked_add(*value_delta).ok_or_else(|| {
                        anyhow::anyhow!("grouped-stats decimal min/max value count overflow")
                    })?;
                    if new_count < 0 {
                        bail!(
                            "grouped-stats decimal min/max removed more values than were present"
                        );
                    }
                    updated_counts.insert(*value, new_count);
                    columnar
                        .stats_state
                        .write_i128_value_count(writes, group_key, idx, *value, new_count)?;
                }
                let new = columnar
                    .stats_state
                    .new_i128_minmax_after_delta(group_key, idx, spec.kind, old, &updated_counts)
                    .await?;
                columnar
                    .stats_state
                    .write_i128_minmax(writes, group_key, idx, new)?;
                new.map(AggregateValue::Decimal128)
                    .unwrap_or(AggregateValue::Null)
            }
            _ => bail!("grouped-stats aggregate state kind mismatch"),
        });
    }
    Ok(values)
}

impl AggregateDelta {
    fn for_spec(spec: &AggregateSpec) -> Self {
        match (spec.kind, spec.value_type) {
            (AggregateKind::Count, _) => Self::Count { count_delta: 0 },
            (AggregateKind::DistinctCount, Some(AggregateValueType::Utf8)) => {
                Self::DistinctCountUtf8 {
                    value_deltas: HashMap::new(),
                }
            }
            (AggregateKind::DistinctCount, Some(AggregateValueType::Decimal128)) => {
                Self::DistinctCountI128 {
                    value_deltas: HashMap::new(),
                }
            }
            (AggregateKind::DistinctCount, _) => Self::DistinctCountI64 {
                value_deltas: HashMap::new(),
            },
            (AggregateKind::Sum, Some(AggregateValueType::Decimal128)) => {
                Self::SumI128 { sum_delta: 0 }
            }
            (AggregateKind::Sum, _) => Self::Sum { sum_delta: 0 },
            (AggregateKind::Avg, _) => Self::Avg {
                sum_delta: 0,
                count_delta: 0,
            },
            (AggregateKind::Min | AggregateKind::Max, Some(AggregateValueType::Utf8)) => {
                Self::MinMaxUtf8 {
                    value_deltas: HashMap::new(),
                }
            }
            (AggregateKind::Min | AggregateKind::Max, Some(AggregateValueType::Decimal128)) => {
                Self::MinMaxI128 {
                    value_deltas: HashMap::new(),
                }
            }
            (AggregateKind::Min | AggregateKind::Max, _) => Self::MinMaxI64 {
                value_deltas: HashMap::new(),
            },
        }
    }
}

fn aggregate_deltas_empty(deltas: &[AggregateDelta]) -> bool {
    deltas.iter().all(|delta| match delta {
        AggregateDelta::Count { count_delta } => *count_delta == 0,
        AggregateDelta::DistinctCountI64 { value_deltas } => value_deltas.is_empty(),
        AggregateDelta::DistinctCountI128 { value_deltas } => value_deltas.is_empty(),
        AggregateDelta::DistinctCountUtf8 { value_deltas } => value_deltas.is_empty(),
        AggregateDelta::Sum { sum_delta } => *sum_delta == 0,
        AggregateDelta::SumI128 { sum_delta } => *sum_delta == 0,
        AggregateDelta::Avg {
            sum_delta,
            count_delta,
        } => *sum_delta == 0 && *count_delta == 0,
        AggregateDelta::MinMaxI64 { value_deltas } => value_deltas.is_empty(),
        AggregateDelta::MinMaxI128 { value_deltas } => value_deltas.is_empty(),
        AggregateDelta::MinMaxUtf8 { value_deltas } => value_deltas.is_empty(),
    })
}

enum ProjectedValueArray<'a> {
    None,
    Any(&'a dyn Array),
    Int64(&'a Int64Array),
    Utf8(&'a StringArray),
    TimestampMillis(&'a TimestampMillisecondArray),
    DateDays(&'a Date32Array),
    Bool(&'a BooleanArray),
    Decimal128(&'a Decimal128Array),
}

fn projected_value_arrays<'a>(
    batch: &'a RecordBatch,
    specs: &[AggregateSpec],
) -> Result<Vec<ProjectedValueArray<'a>>> {
    specs
        .iter()
        .map(|spec| {
            let Some(idx) = spec.value_idx else {
                return Ok(ProjectedValueArray::None);
            };
            match spec.value_type {
                Some(AggregateValueType::Any) => {
                    Ok(ProjectedValueArray::Any(batch.column(idx).as_ref()))
                }
                Some(AggregateValueType::Int64) => batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(ProjectedValueArray::Int64)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats value must be Int64")),
                Some(AggregateValueType::Utf8) => batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(ProjectedValueArray::Utf8)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats value must be Utf8")),
                Some(AggregateValueType::TimestampMillis) => batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .map(ProjectedValueArray::TimestampMillis)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats value must be TimestampMillis")),
                Some(AggregateValueType::DateDays) => batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .map(ProjectedValueArray::DateDays)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats value must be Date32")),
                Some(AggregateValueType::Bool) => batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .map(ProjectedValueArray::Bool)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats value must be Boolean")),
                Some(AggregateValueType::Decimal128) => batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .map(ProjectedValueArray::Decimal128)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats value must be Decimal128")),
                None => Ok(ProjectedValueArray::None),
            }
        })
        .collect()
}

fn projected_filter_arrays<'a>(
    batch: &'a RecordBatch,
    specs: &[AggregateSpec],
) -> Result<Vec<Option<&'a BooleanArray>>> {
    specs
        .iter()
        .map(|spec| {
            spec.filter_idx
                .map(|idx| {
                    batch
                        .column(idx)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| anyhow::anyhow!("grouped-stats filter must be Boolean"))
                })
                .transpose()
        })
        .collect()
}

fn projected_i64_value(values: &ProjectedValueArray<'_>, row_idx: usize) -> Option<i64> {
    let ProjectedValueArray::Int64(values) = values else {
        return None;
    };
    (!values.is_null(row_idx)).then(|| values.value(row_idx))
}

fn projected_distinct_i64_value(values: &ProjectedValueArray<'_>, row_idx: usize) -> Option<i64> {
    match values {
        ProjectedValueArray::Int64(values) => {
            (!values.is_null(row_idx)).then(|| values.value(row_idx))
        }
        ProjectedValueArray::TimestampMillis(values) => {
            (!values.is_null(row_idx)).then(|| values.value(row_idx))
        }
        ProjectedValueArray::DateDays(values) => {
            (!values.is_null(row_idx)).then(|| i64::from(values.value(row_idx)))
        }
        ProjectedValueArray::Bool(values) => {
            (!values.is_null(row_idx)).then(|| if values.value(row_idx) { 1 } else { 0 })
        }
        ProjectedValueArray::None | ProjectedValueArray::Any(_) | ProjectedValueArray::Utf8(_) => {
            None
        }
        ProjectedValueArray::Decimal128(_) => None,
    }
}

fn projected_i128_value(values: &ProjectedValueArray<'_>, row_idx: usize) -> Option<i128> {
    let ProjectedValueArray::Decimal128(values) = values else {
        return None;
    };
    (!values.is_null(row_idx)).then(|| values.value(row_idx))
}

fn projected_ordered_i64_value(values: &ProjectedValueArray<'_>, row_idx: usize) -> Option<i64> {
    match values {
        ProjectedValueArray::Int64(values) => {
            (!values.is_null(row_idx)).then(|| values.value(row_idx))
        }
        ProjectedValueArray::TimestampMillis(values) => {
            (!values.is_null(row_idx)).then(|| values.value(row_idx))
        }
        ProjectedValueArray::DateDays(values) => {
            (!values.is_null(row_idx)).then(|| i64::from(values.value(row_idx)))
        }
        ProjectedValueArray::None
        | ProjectedValueArray::Any(_)
        | ProjectedValueArray::Utf8(_)
        | ProjectedValueArray::Bool(_)
        | ProjectedValueArray::Decimal128(_) => None,
    }
}

fn projected_value_is_non_null(values: &ProjectedValueArray<'_>, row_idx: usize) -> bool {
    match values {
        ProjectedValueArray::None => true,
        ProjectedValueArray::Any(values) => !values.is_null(row_idx),
        ProjectedValueArray::Int64(values) => !values.is_null(row_idx),
        ProjectedValueArray::Utf8(values) => !values.is_null(row_idx),
        ProjectedValueArray::TimestampMillis(values) => !values.is_null(row_idx),
        ProjectedValueArray::DateDays(values) => !values.is_null(row_idx),
        ProjectedValueArray::Bool(values) => !values.is_null(row_idx),
        ProjectedValueArray::Decimal128(values) => !values.is_null(row_idx),
    }
}

fn projected_utf8_value<'a>(
    values: &'a ProjectedValueArray<'a>,
    row_idx: usize,
) -> Option<&'a str> {
    let ProjectedValueArray::Utf8(values) = values else {
        return None;
    };
    (!values.is_null(row_idx)).then(|| values.value(row_idx))
}

fn update_i64_value_delta(deltas: &mut HashMap<i64, i64>, value: i64, sign: i64) -> Result<()> {
    match deltas.entry(value) {
        Entry::Occupied(mut entry) => {
            let next = entry
                .get()
                .checked_add(sign)
                .ok_or_else(|| anyhow::anyhow!("grouped-stats i64 value delta overflow"))?;
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
    Ok(())
}

fn update_i128_value_delta(deltas: &mut HashMap<i128, i64>, value: i128, sign: i64) -> Result<()> {
    match deltas.entry(value) {
        Entry::Occupied(mut entry) => {
            let next = entry
                .get()
                .checked_add(sign)
                .ok_or_else(|| anyhow::anyhow!("grouped-stats i128 value delta overflow"))?;
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
    Ok(())
}

fn update_string_value_delta(
    deltas: &mut HashMap<String, i64>,
    value: String,
    sign: i64,
) -> Result<()> {
    match deltas.entry(value) {
        Entry::Occupied(mut entry) => {
            let next = entry
                .get()
                .checked_add(sign)
                .ok_or_else(|| anyhow::anyhow!("grouped-stats string value delta overflow"))?;
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
    Ok(())
}

fn filter_allows(filter: &Option<&BooleanArray>, row_idx: usize) -> bool {
    match filter {
        Some(filter) => !filter.is_null(row_idx) && filter.value(row_idx),
        None => true,
    }
}

impl SlateGroupedStatsState {
    fn new(table: Arc<dyn KeyValueTable>, namespace: &str, assume_empty: bool) -> Self {
        Self {
            table,
            key_prefix: keyspace::namespace_prefix(keyspace::prefix::INDEX, namespace),
            assume_empty,
            group_counts: Mutex::new(HashMap::new()),
            i64_values: Mutex::new(HashMap::new()),
            i128_values: Mutex::new(HashMap::new()),
            pairs: Mutex::new(HashMap::new()),
            minmax_values: Mutex::new(HashMap::new()),
            i128_minmax_values: Mutex::new(HashMap::new()),
            value_counts: Mutex::new(HashMap::new()),
            i128_value_counts: Mutex::new(HashMap::new()),
            string_minmax_values: Mutex::new(HashMap::new()),
            string_value_counts: Mutex::new(HashMap::new()),
        }
    }

    async fn load_group_count(&self, group_key: &[u8]) -> Result<i64> {
        let cache_key = group_key.to_vec();
        if let Some(value) = self
            .group_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats group count cache poisoned"))?
            .get(&cache_key)
            .copied()
        {
            return Ok(value);
        }
        if self.assume_empty {
            return Ok(0);
        }
        let value = self
            .load_key_i64(&self.group_key(GROUP_TAG, group_key)?)
            .await?;
        self.group_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats group count cache poisoned"))?
            .insert(cache_key, value);
        Ok(value)
    }

    fn write_group_count(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        count: i64,
    ) -> Result<()> {
        self.write_key_i64(batch, self.group_key(GROUP_TAG, group_key)?, count);
        self.group_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats group count cache poisoned"))?
            .insert(group_key.to_vec(), count);
        Ok(())
    }

    async fn load_i64(&self, group_key: &[u8], agg_idx: usize) -> Result<i64> {
        let cache_key = (group_key.to_vec(), agg_idx);
        if let Some(value) = self
            .i64_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats i64 cache poisoned"))?
            .get(&cache_key)
            .copied()
        {
            return Ok(value);
        }
        if self.assume_empty {
            return Ok(0);
        }
        let value = self
            .load_key_i64(&self.aggregate_key(SCALAR_TAG, group_key, agg_idx)?)
            .await?;
        self.i64_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats i64 cache poisoned"))?
            .insert(cache_key, value);
        Ok(value)
    }

    fn write_i64(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        value: i64,
    ) -> Result<()> {
        self.write_key_i64(
            batch,
            self.aggregate_key(SCALAR_TAG, group_key, agg_idx)?,
            value,
        );
        self.i64_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats i64 cache poisoned"))?
            .insert((group_key.to_vec(), agg_idx), value);
        Ok(())
    }

    async fn load_i128(&self, group_key: &[u8], agg_idx: usize) -> Result<i128> {
        let cache_key = (group_key.to_vec(), agg_idx);
        if let Some(value) = self
            .i128_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats i128 cache poisoned"))?
            .get(&cache_key)
            .copied()
        {
            return Ok(value);
        }
        if self.assume_empty {
            return Ok(0);
        }
        let value = self
            .load_key_i128(&self.aggregate_key(SCALAR_TAG, group_key, agg_idx)?)
            .await?;
        self.i128_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats i128 cache poisoned"))?
            .insert(cache_key, value);
        Ok(value)
    }

    fn write_i128(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        value: i128,
    ) -> Result<()> {
        self.write_key_i128(
            batch,
            self.aggregate_key(SCALAR_TAG, group_key, agg_idx)?,
            value,
        );
        self.i128_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats i128 cache poisoned"))?
            .insert((group_key.to_vec(), agg_idx), value);
        Ok(())
    }

    async fn load_pair(&self, group_key: &[u8], agg_idx: usize) -> Result<(i64, i64)> {
        let cache_key = (group_key.to_vec(), agg_idx);
        if let Some(value) = self
            .pairs
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats pair cache poisoned"))?
            .get(&cache_key)
            .copied()
        {
            return Ok(value);
        }
        if self.assume_empty {
            return Ok((0, 0));
        }
        let Some(bytes) = self
            .table
            .get_bytes(&self.aggregate_key(SCALAR_TAG, group_key, agg_idx)?)
            .await
            .context("read grouped-stats pair state")?
        else {
            return Ok((0, 0));
        };
        let value = decode_i64_pair(bytes.as_ref())?;
        self.pairs
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats pair cache poisoned"))?
            .insert(cache_key, value);
        Ok(value)
    }

    fn write_pair(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        sum: i64,
        count: i64,
    ) -> Result<()> {
        let key = self.aggregate_key(SCALAR_TAG, group_key, agg_idx)?;
        if sum == 0 && count == 0 {
            batch.delete(key);
        } else {
            let mut bytes = Vec::with_capacity(16);
            bytes.extend_from_slice(&sum.to_be_bytes());
            bytes.extend_from_slice(&count.to_be_bytes());
            batch.put(key, bytes);
        }
        self.pairs
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats pair cache poisoned"))?
            .insert((group_key.to_vec(), agg_idx), (sum, count));
        Ok(())
    }

    async fn load_minmax(&self, group_key: &[u8], agg_idx: usize) -> Result<Option<i64>> {
        let cache_key = (group_key.to_vec(), agg_idx);
        if let Some(value) = self
            .minmax_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats min/max cache poisoned"))?
            .get(&cache_key)
            .copied()
        {
            return Ok(value);
        }
        if self.assume_empty {
            return Ok(None);
        }
        let Some(bytes) = self
            .table
            .get_bytes(&self.aggregate_key(MINMAX_TAG, group_key, agg_idx)?)
            .await
            .context("read grouped-stats min/max state")?
        else {
            return Ok(None);
        };
        let value = Some(decode_i64(bytes.as_ref())?);
        self.minmax_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats min/max cache poisoned"))?
            .insert(cache_key, value);
        Ok(value)
    }

    fn write_minmax(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        value: Option<i64>,
    ) -> Result<()> {
        let key = self.aggregate_key(MINMAX_TAG, group_key, agg_idx)?;
        if let Some(value) = value {
            batch.put(key, value.to_be_bytes());
        } else {
            batch.delete(key);
        }
        self.minmax_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats min/max cache poisoned"))?
            .insert((group_key.to_vec(), agg_idx), value);
        Ok(())
    }

    async fn load_i128_minmax(&self, group_key: &[u8], agg_idx: usize) -> Result<Option<i128>> {
        let cache_key = (group_key.to_vec(), agg_idx);
        if let Some(value) = self
            .i128_minmax_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats i128 min/max cache poisoned"))?
            .get(&cache_key)
            .copied()
        {
            return Ok(value);
        }
        if self.assume_empty {
            return Ok(None);
        }
        let Some(bytes) = self
            .table
            .get_bytes(&self.aggregate_key(MINMAX_TAG, group_key, agg_idx)?)
            .await
            .context("read grouped-stats i128 min/max state")?
        else {
            return Ok(None);
        };
        let value = Some(decode_i128(bytes.as_ref())?);
        self.i128_minmax_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats i128 min/max cache poisoned"))?
            .insert(cache_key, value);
        Ok(value)
    }

    fn write_i128_minmax(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        value: Option<i128>,
    ) -> Result<()> {
        let key = self.aggregate_key(MINMAX_TAG, group_key, agg_idx)?;
        if let Some(value) = value {
            batch.put(key, value.to_be_bytes());
        } else {
            batch.delete(key);
        }
        self.i128_minmax_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats i128 min/max cache poisoned"))?
            .insert((group_key.to_vec(), agg_idx), value);
        Ok(())
    }

    async fn load_string_minmax(&self, group_key: &[u8], agg_idx: usize) -> Result<Option<String>> {
        let cache_key = (group_key.to_vec(), agg_idx);
        if let Some(value) = self
            .string_minmax_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats string min/max cache poisoned"))?
            .get(&cache_key)
            .cloned()
        {
            return Ok(value);
        }
        if self.assume_empty {
            return Ok(None);
        }
        let Some(bytes) = self
            .table
            .get_bytes(&self.aggregate_key(MINMAX_TAG, group_key, agg_idx)?)
            .await
            .context("read grouped-stats string min/max state")?
        else {
            return Ok(None);
        };
        let value = Some(String::from_utf8(bytes.to_vec()).context("decode string min/max")?);
        self.string_minmax_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats string min/max cache poisoned"))?
            .insert(cache_key, value.clone());
        Ok(value)
    }

    fn write_string_minmax(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        value: Option<&str>,
    ) -> Result<()> {
        let key = self.aggregate_key(MINMAX_TAG, group_key, agg_idx)?;
        if let Some(value) = value {
            batch.put(key, value.as_bytes());
        } else {
            batch.delete(key);
        }
        self.string_minmax_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats string min/max cache poisoned"))?
            .insert(
                (group_key.to_vec(), agg_idx),
                value.map(ToString::to_string),
            );
        Ok(())
    }

    async fn load_value_count(&self, group_key: &[u8], agg_idx: usize, value: i64) -> Result<i64> {
        let cache_key = (group_key.to_vec(), agg_idx, value);
        if let Some(count) = self
            .value_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats value count cache poisoned"))?
            .get(&cache_key)
            .copied()
        {
            return Ok(count);
        }
        if self.assume_empty {
            return Ok(0);
        }
        let count = self
            .load_key_i64(&self.value_key(group_key, agg_idx, value)?)
            .await?;
        self.value_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats value count cache poisoned"))?
            .insert(cache_key, count);
        Ok(count)
    }

    fn write_value_count(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        value: i64,
        count: i64,
    ) -> Result<()> {
        self.write_key_i64(batch, self.value_key(group_key, agg_idx, value)?, count);
        self.value_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats value count cache poisoned"))?
            .insert((group_key.to_vec(), agg_idx, value), count);
        Ok(())
    }

    async fn load_i128_value_count(
        &self,
        group_key: &[u8],
        agg_idx: usize,
        value: i128,
    ) -> Result<i64> {
        let cache_key = (group_key.to_vec(), agg_idx, value);
        if let Some(count) = self
            .i128_value_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats i128 value count cache poisoned"))?
            .get(&cache_key)
            .copied()
        {
            return Ok(count);
        }
        if self.assume_empty {
            return Ok(0);
        }
        let count = self
            .load_key_i64(&self.i128_value_key(group_key, agg_idx, value)?)
            .await?;
        self.i128_value_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats i128 value count cache poisoned"))?
            .insert(cache_key, count);
        Ok(count)
    }

    fn write_i128_value_count(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        value: i128,
        count: i64,
    ) -> Result<()> {
        self.write_key_i64(
            batch,
            self.i128_value_key(group_key, agg_idx, value)?,
            count,
        );
        self.i128_value_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats i128 value count cache poisoned"))?
            .insert((group_key.to_vec(), agg_idx, value), count);
        Ok(())
    }

    async fn load_string_value_count(
        &self,
        group_key: &[u8],
        agg_idx: usize,
        value: &str,
    ) -> Result<i64> {
        let cache_key = (group_key.to_vec(), agg_idx, value.to_string());
        if let Some(count) = self
            .string_value_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats string value count cache poisoned"))?
            .get(&cache_key)
            .copied()
        {
            return Ok(count);
        }
        if self.assume_empty {
            return Ok(0);
        }
        let count = self
            .load_key_i64(&self.string_value_key(group_key, agg_idx, value)?)
            .await?;
        self.string_value_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats string value count cache poisoned"))?
            .insert(cache_key, count);
        Ok(count)
    }

    fn write_string_value_count(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        value: &str,
        count: i64,
    ) -> Result<()> {
        self.write_key_i64(
            batch,
            self.string_value_key(group_key, agg_idx, value)?,
            count,
        );
        self.string_value_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats string value count cache poisoned"))?
            .insert((group_key.to_vec(), agg_idx, value.to_string()), count);
        Ok(())
    }

    async fn new_minmax_after_delta(
        &self,
        group_key: &[u8],
        agg_idx: usize,
        kind: AggregateKind,
        old: Option<i64>,
        updated_counts: &HashMap<i64, i64>,
    ) -> Result<Option<i64>> {
        let mut added = None;
        for (value, count) in updated_counts {
            if *count > 0 {
                added = Some(match added {
                    Some(current) => minmax_value(kind, current, *value),
                    None => *value,
                });
            }
        }
        match old {
            None => Ok(added),
            Some(old) => {
                let old_still_present = match updated_counts.get(&old) {
                    Some(count) => *count > 0,
                    None => true,
                };
                if old_still_present {
                    return Ok(Some(match added {
                        Some(value) => minmax_value(kind, old, value),
                        None => old,
                    }));
                }
                self.scan_minmax_with_overlay(group_key, agg_idx, kind, updated_counts)
                    .await
            }
        }
    }

    async fn scan_minmax_with_overlay(
        &self,
        group_key: &[u8],
        agg_idx: usize,
        kind: AggregateKind,
        updated_counts: &HashMap<i64, i64>,
    ) -> Result<Option<i64>> {
        let value_prefix = self.value_key_prefix(group_key, agg_idx)?;
        let mut out = None;
        for (key, value_bytes) in self
            .table
            .scan_prefix(&value_prefix, &ScanOptions::default())
            .await
            .context("scan grouped-stats min/max value state")?
        {
            let value = decode_i64_sortable(
                key.get(value_prefix.len()..)
                    .ok_or_else(|| anyhow::anyhow!("invalid grouped-stats value key"))?,
            )?;
            let old_count = decode_i64(&value_bytes)?;
            let count = updated_counts.get(&value).copied().unwrap_or(old_count);
            if count > 0 {
                out = Some(match out {
                    Some(current) => minmax_value(kind, current, value),
                    None => value,
                });
            }
        }
        for (value, count) in updated_counts {
            if *count > 0 {
                out = Some(match out {
                    Some(current) => minmax_value(kind, current, *value),
                    None => *value,
                });
            }
        }
        Ok(out)
    }

    async fn new_i128_minmax_after_delta(
        &self,
        group_key: &[u8],
        agg_idx: usize,
        kind: AggregateKind,
        old: Option<i128>,
        updated_counts: &HashMap<i128, i64>,
    ) -> Result<Option<i128>> {
        let mut added = None;
        for (value, count) in updated_counts {
            if *count > 0 {
                added = Some(match added {
                    Some(current) => minmax_i128_value(kind, current, *value),
                    None => *value,
                });
            }
        }
        match old {
            None => Ok(added),
            Some(old) => {
                let old_still_present = match updated_counts.get(&old) {
                    Some(count) => *count > 0,
                    None => true,
                };
                if old_still_present {
                    return Ok(Some(match added {
                        Some(value) => minmax_i128_value(kind, old, value),
                        None => old,
                    }));
                }
                self.scan_i128_minmax_with_overlay(group_key, agg_idx, kind, updated_counts)
                    .await
            }
        }
    }

    async fn scan_i128_minmax_with_overlay(
        &self,
        group_key: &[u8],
        agg_idx: usize,
        kind: AggregateKind,
        updated_counts: &HashMap<i128, i64>,
    ) -> Result<Option<i128>> {
        let value_prefix = self.value_key_prefix(group_key, agg_idx)?;
        let mut out = None;
        for (key, value_bytes) in self
            .table
            .scan_prefix(&value_prefix, &ScanOptions::default())
            .await
            .context("scan grouped-stats i128 min/max value state")?
        {
            let value = decode_i128_sortable(
                key.get(value_prefix.len()..)
                    .ok_or_else(|| anyhow::anyhow!("invalid grouped-stats i128 value key"))?,
            )?;
            let old_count = decode_i64(&value_bytes)?;
            let count = updated_counts.get(&value).copied().unwrap_or(old_count);
            if count > 0 {
                out = Some(match out {
                    Some(current) => minmax_i128_value(kind, current, value),
                    None => value,
                });
            }
        }
        for (value, count) in updated_counts {
            if *count > 0 {
                out = Some(match out {
                    Some(current) => minmax_i128_value(kind, current, *value),
                    None => *value,
                });
            }
        }
        Ok(out)
    }

    async fn new_string_minmax_after_delta(
        &self,
        group_key: &[u8],
        agg_idx: usize,
        kind: AggregateKind,
        old: Option<String>,
        updated_counts: &HashMap<String, i64>,
    ) -> Result<Option<String>> {
        let mut added: Option<String> = None;
        for (value, count) in updated_counts {
            if *count > 0 {
                added = Some(match added {
                    Some(current) => minmax_string(kind, current, value.clone()),
                    None => value.clone(),
                });
            }
        }
        match old {
            None => Ok(added),
            Some(old) => {
                let old_still_present = match updated_counts.get(&old) {
                    Some(count) => *count > 0,
                    None => true,
                };
                if old_still_present {
                    return Ok(Some(match added {
                        Some(value) => minmax_string(kind, old, value),
                        None => old,
                    }));
                }
                self.scan_string_minmax_with_overlay(group_key, agg_idx, kind, updated_counts)
                    .await
            }
        }
    }

    async fn scan_string_minmax_with_overlay(
        &self,
        group_key: &[u8],
        agg_idx: usize,
        kind: AggregateKind,
        updated_counts: &HashMap<String, i64>,
    ) -> Result<Option<String>> {
        let value_prefix = self.value_key_prefix(group_key, agg_idx)?;
        let mut out = None;
        for (key, value_bytes) in self
            .table
            .scan_prefix(&value_prefix, &ScanOptions::default())
            .await
            .context("scan grouped-stats string min/max value state")?
        {
            let value = String::from_utf8(
                key.get(value_prefix.len()..)
                    .ok_or_else(|| anyhow::anyhow!("invalid grouped-stats string value key"))?
                    .to_vec(),
            )
            .context("decode grouped-stats string value key")?;
            let old_count = decode_i64(&value_bytes)?;
            let count = updated_counts.get(&value).copied().unwrap_or(old_count);
            if count > 0 {
                out = Some(match out {
                    Some(current) => minmax_string(kind, current, value),
                    None => value,
                });
            }
        }
        for (value, count) in updated_counts {
            if *count > 0 {
                out = Some(match out {
                    Some(current) => minmax_string(kind, current, value.clone()),
                    None => value.clone(),
                });
            }
        }
        Ok(out)
    }

    async fn load_key_i64(&self, key: &[u8]) -> Result<i64> {
        let Some(bytes) = self
            .table
            .get_bytes(key)
            .await
            .context("read grouped-stats i64 state")?
        else {
            return Ok(0);
        };
        decode_i64(bytes.as_ref())
    }

    async fn load_key_i128(&self, key: &[u8]) -> Result<i128> {
        let Some(bytes) = self
            .table
            .get_bytes(key)
            .await
            .context("read grouped-stats i128 state")?
        else {
            return Ok(0);
        };
        decode_i128(bytes.as_ref())
    }

    fn write_key_i64(&self, batch: &mut WriteBatch, key: Vec<u8>, value: i64) {
        if value == 0 {
            batch.delete(key);
        } else {
            batch.put(key, value.to_be_bytes());
        }
    }

    fn write_key_i128(&self, batch: &mut WriteBatch, key: Vec<u8>, value: i128) {
        if value == 0 {
            batch.delete(key);
        } else {
            batch.put(key, value.to_be_bytes());
        }
    }

    fn value_key(&self, group_key: &[u8], agg_idx: usize, value: i64) -> Result<Vec<u8>> {
        let mut key = self.value_key_prefix(group_key, agg_idx)?;
        key.extend_from_slice(&encode_i64_sortable(value));
        Ok(key)
    }

    fn i128_value_key(&self, group_key: &[u8], agg_idx: usize, value: i128) -> Result<Vec<u8>> {
        let mut key = self.value_key_prefix(group_key, agg_idx)?;
        key.extend_from_slice(&encode_i128_sortable(value));
        Ok(key)
    }

    fn string_value_key(&self, group_key: &[u8], agg_idx: usize, value: &str) -> Result<Vec<u8>> {
        let mut key = self.value_key_prefix(group_key, agg_idx)?;
        key.extend_from_slice(value.as_bytes());
        Ok(key)
    }

    fn value_key_prefix(&self, group_key: &[u8], agg_idx: usize) -> Result<Vec<u8>> {
        self.aggregate_key(VALUE_TAG, group_key, agg_idx)
    }

    fn aggregate_key(&self, tag: u8, group_key: &[u8], agg_idx: usize) -> Result<Vec<u8>> {
        let agg_idx =
            u16::try_from(agg_idx).context("grouped-stats aggregate index exceeds u16")?;
        let mut key = self.group_key(tag, group_key)?;
        key.extend_from_slice(&agg_idx.to_be_bytes());
        Ok(key)
    }

    fn group_key(&self, tag: u8, group_key: &[u8]) -> Result<Vec<u8>> {
        let len =
            u32::try_from(group_key.len()).context("grouped-stats group key exceeds u32 bytes")?;
        let mut key = self.key_prefix.clone();
        key.push(tag);
        key.extend_from_slice(&len.to_be_bytes());
        key.extend_from_slice(group_key);
        Ok(key)
    }
}

struct AggregateStatsOutputBuilder {
    schema: SchemaRef,
    group_count: usize,
    builders: Vec<ScalarColumnBuilder>,
    rows: usize,
}

impl AggregateStatsOutputBuilder {
    fn new(schema: SchemaRef, group_count: usize) -> Result<Self> {
        let builders = schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), 1024))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            schema,
            group_count,
            builders,
            rows: 0,
        })
    }

    fn append(
        &mut self,
        projection_batch: &RecordBatch,
        row_idx: usize,
        aggregate_values: &[AggregateValue],
    ) -> Result<()> {
        for source_idx in 0..self.schema.fields().len() {
            if source_idx < self.group_count {
                self.builders[source_idx]
                    .append_array_value(projection_batch.column(source_idx).as_ref(), row_idx)?;
            } else {
                let aggregate_idx = source_idx - self.group_count;
                append_aggregate_value(
                    &mut self.builders[source_idx],
                    aggregate_values.get(aggregate_idx).ok_or_else(|| {
                        anyhow::anyhow!("grouped-stats aggregate row mapping out of bounds")
                    })?,
                )?;
            }
        }
        self.rows = self.rows.saturating_add(1);
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<RecordBatch>> {
        if self.rows == 0 {
            return Ok(Vec::new());
        }
        let columns = self
            .builders
            .iter_mut()
            .map(ScalarColumnBuilder::finish_array)
            .collect::<Vec<_>>();
        Ok(vec![RecordBatch::try_new(self.schema, columns)?])
    }
}

struct WeightedStatsOutputBuilder {
    weighted_schema: SchemaRef,
    output_mapping: Vec<usize>,
    builders: Vec<ScalarColumnBuilder>,
    weights: Int64Builder,
    rows: usize,
}

impl WeightedStatsOutputBuilder {
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
        group_count: usize,
        aggregate_values: &[AggregateValue],
        weight: i64,
    ) -> Result<()> {
        for (output_idx, source_idx) in self.output_mapping.iter().copied().enumerate() {
            if source_idx < group_count {
                self.builders[output_idx]
                    .append_array_value(projection_batch.column(source_idx).as_ref(), row_idx)?;
            } else {
                let aggregate_idx = source_idx - group_count;
                match aggregate_values
                    .get(aggregate_idx)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats output mapping out of bounds"))?
                {
                    value => append_aggregate_value(&mut self.builders[output_idx], value)?,
                }
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

fn append_aggregate_value(builder: &mut ScalarColumnBuilder, value: &AggregateValue) -> Result<()> {
    match value {
        AggregateValue::Int64(value) => builder.append_i64_value(*value),
        AggregateValue::Float64(value) => builder.append_f64_value(*value),
        AggregateValue::Utf8(value) => {
            builder.append_encoded_scalar(Some(&EncodedRowScalar::Utf8(value.clone())))
        }
        AggregateValue::TimestampMillis(value) => {
            builder.append_encoded_scalar(Some(&EncodedRowScalar::TimestampMillis(*value)))
        }
        AggregateValue::DateDays(value) => {
            builder.append_encoded_scalar(Some(&EncodedRowScalar::DateDays(*value)))
        }
        AggregateValue::Decimal128(value) => {
            builder.append_encoded_scalar(Some(&EncodedRowScalar::Decimal128(*value)))
        }
        AggregateValue::Null => builder.append_encoded_scalar(None),
    }
}

async fn post_aggregate_delta_batches(
    post_aggregate: &PostAggregateTransformState,
    output_schema: SchemaRef,
    old_aggregate_rows: Vec<RecordBatch>,
    new_aggregate_rows: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>> {
    let weighted_schema = weighted_snapshot_schema(&output_schema)?;
    let old_output = post_aggregate
        .collect(old_aggregate_rows, &output_schema)
        .await
        .context("evaluate grouped-stats old post-aggregate rows")?;
    let mut output_delta = add_weight_column_to_batches(&old_output, &weighted_schema, -1)?;
    let new_output = post_aggregate
        .collect(new_aggregate_rows, &output_schema)
        .await
        .context("evaluate grouped-stats new post-aggregate rows")?;
    output_delta.extend(add_weight_column_to_batches(
        &new_output,
        &weighted_schema,
        1,
    )?);
    Ok(output_delta)
}

impl PostAggregateTransformState {
    async fn collect(
        &self,
        aggregate_rows: Vec<RecordBatch>,
        output_schema: &SchemaRef,
    ) -> Result<Vec<RecordBatch>> {
        if aggregate_rows.is_empty() {
            return Ok(Vec::new());
        }
        self.provider
            .set_batches(aggregate_rows)
            .context("set grouped-stats post-aggregate input rows")?;
        let collected = collect(Arc::clone(&self.plan), self.ctx.task_ctx()).await;
        self.provider
            .set_batches(Vec::new())
            .context("clear grouped-stats post-aggregate input rows")?;
        normalize_batches(
            collected.context("execute grouped-stats post-aggregate transform")?,
            output_schema,
        )
    }
}

fn aggregate_spec_for_expr(
    expr: &Expr,
    output_type: &DataType,
    projection_expr: &mut Vec<Expr>,
) -> Option<AggregateSpec> {
    let Expr::AggregateFunction(aggregate) = strip_alias(expr) else {
        return None;
    };
    let params = &aggregate.params;
    if !params.order_by.is_empty() || params.null_treatment.is_some() {
        return None;
    }
    let filter_idx = params.filter.as_ref().map(|filter| {
        let idx = projection_expr.len();
        projection_expr.push(
            filter
                .as_ref()
                .clone()
                .alias(format!("__floe_grouped_stats_filter_{idx}")),
        );
        idx
    });
    let name = aggregate.func.name();
    if name.eq_ignore_ascii_case("count") {
        if output_type != &DataType::Int64 {
            return None;
        }
        if params.distinct {
            let [value_expr] = params.args.as_slice() else {
                return None;
            };
            let value_idx = projection_expr.len();
            projection_expr.push(
                value_expr
                    .clone()
                    .alias(format!("__floe_grouped_stats_value_{value_idx}")),
            );
            return Some(AggregateSpec {
                kind: AggregateKind::DistinctCount,
                value_idx: Some(value_idx),
                filter_idx,
                value_type: Some(AggregateValueType::Any),
            });
        }
        if !is_count_star_args(&params.args) {
            let [value_expr] = params.args.as_slice() else {
                return None;
            };
            let value_idx = projection_expr.len();
            projection_expr.push(
                value_expr
                    .clone()
                    .alias(format!("__floe_grouped_stats_count_value_{value_idx}")),
            );
            return Some(AggregateSpec {
                kind: AggregateKind::Count,
                value_idx: Some(value_idx),
                filter_idx,
                value_type: Some(AggregateValueType::Any),
            });
        }
        return Some(AggregateSpec {
            kind: AggregateKind::Count,
            value_idx: None,
            filter_idx,
            value_type: None,
        });
    }
    if params.distinct {
        return None;
    }
    let [value_expr] = params.args.as_slice() else {
        return None;
    };
    let value_idx = projection_expr.len();
    projection_expr.push(
        value_expr
            .clone()
            .alias(format!("__floe_grouped_stats_value_{value_idx}")),
    );
    let (kind, value_type) = if name.eq_ignore_ascii_case("sum")
        && matches!(output_type, DataType::Int64 | DataType::Decimal128(_, _))
    {
        (
            AggregateKind::Sum,
            aggregate_value_type_for_data_type(output_type)?,
        )
    } else if name.eq_ignore_ascii_case("avg") && output_type == &DataType::Float64 {
        (AggregateKind::Avg, AggregateValueType::Int64)
    } else if name.eq_ignore_ascii_case("min")
        && matches!(
            output_type,
            DataType::Int64
                | DataType::Utf8
                | DataType::Timestamp(TimeUnit::Millisecond, _)
                | DataType::Date32
                | DataType::Decimal128(_, _)
        )
    {
        (
            AggregateKind::Min,
            aggregate_value_type_for_data_type(output_type)?,
        )
    } else if name.eq_ignore_ascii_case("max")
        && matches!(
            output_type,
            DataType::Int64
                | DataType::Utf8
                | DataType::Timestamp(TimeUnit::Millisecond, _)
                | DataType::Date32
                | DataType::Decimal128(_, _)
        )
    {
        (
            AggregateKind::Max,
            aggregate_value_type_for_data_type(output_type)?,
        )
    } else {
        return None;
    };
    Some(AggregateSpec {
        kind,
        value_idx: Some(value_idx),
        filter_idx,
        value_type: Some(value_type),
    })
}

fn aggregate_value_type_for_data_type(data_type: &DataType) -> Option<AggregateValueType> {
    match data_type {
        DataType::Int64 => Some(AggregateValueType::Int64),
        DataType::Utf8 => Some(AggregateValueType::Utf8),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Some(AggregateValueType::TimestampMillis),
        DataType::Date32 => Some(AggregateValueType::DateDays),
        DataType::Decimal128(_, _) => Some(AggregateValueType::Decimal128),
        _ => None,
    }
}

fn aggregate_value_from_ordered_i64(
    value_type: Option<AggregateValueType>,
    value: i64,
) -> Result<AggregateValue> {
    match value_type {
        Some(AggregateValueType::TimestampMillis) => Ok(AggregateValue::TimestampMillis(value)),
        Some(AggregateValueType::DateDays) => Ok(AggregateValue::DateDays(
            i32::try_from(value).context("grouped-stats Date32 min/max value out of range")?,
        )),
        Some(AggregateValueType::Int64) | Some(AggregateValueType::Any) | None => {
            Ok(AggregateValue::Int64(value))
        }
        Some(
            AggregateValueType::Utf8 | AggregateValueType::Bool | AggregateValueType::Decimal128,
        ) => {
            bail!("grouped-stats non-numeric min/max cannot be decoded from ordered i64 state")
        }
    }
}

async fn build_post_aggregate_transform_state(
    aggregate_schema: SchemaRef,
    post_aggregate_plan: &LogicalPlan,
    udfs: &[ScalarUDF],
) -> Result<PostAggregateTransformState> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    for udf in udfs.iter().cloned() {
        ctx.register_udf(udf);
    }
    let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(
        &aggregate_schema,
    )));
    ctx.register_table(
        POST_AGGREGATE_SOURCE_NAME,
        Arc::clone(&provider) as Arc<dyn TableProvider>,
    )
    .context("register grouped-stats post-aggregate input")?;
    let table_scan = ctx
        .table(POST_AGGREGATE_SOURCE_NAME)
        .await
        .context("build grouped-stats post-aggregate table scan")?
        .logical_plan()
        .clone();
    let logical_plan = rebind_post_aggregate_plan(post_aggregate_plan.clone(), table_scan)?;
    let plan = ctx
        .state()
        .create_physical_plan(&logical_plan)
        .await
        .context("create grouped-stats post-aggregate physical plan")?;
    Ok(PostAggregateTransformState {
        ctx,
        provider,
        plan,
    })
}

fn rebind_post_aggregate_plan(
    logical_plan: LogicalPlan,
    aggregate_scan: LogicalPlan,
) -> Result<LogicalPlan> {
    let mut replaced = false;
    let logical_plan = unqualify_post_aggregate_columns(logical_plan)?;
    let transformed = logical_plan.transform_up(|plan| match plan {
        LogicalPlan::Aggregate(_) if !replaced => {
            replaced = true;
            Ok(Transformed::yes(aggregate_scan.clone()))
        }
        other => Ok(Transformed::no(other)),
    })?;
    if !replaced {
        bail!("grouped-stats post-aggregate plan did not contain an aggregate");
    }
    Ok(transformed.data)
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
                    "grouped-stats derived input batch width {} does not match schema width {}",
                    batch.num_columns(),
                    schema.fields().len()
                );
            }
            for (idx, field) in schema.fields().iter().enumerate() {
                let actual_type = batch.column(idx).data_type();
                if actual_type != field.data_type() {
                    bail!(
                        "grouped-stats derived input column {} type {:?} does not match expected {:?}",
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

fn unqualify_post_aggregate_columns(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(match plan {
        LogicalPlan::Projection(mut projection) => {
            projection.expr = projection
                .expr
                .into_iter()
                .map(unqualify_post_aggregate_expr)
                .collect::<Result<Vec<_>>>()?;
            projection.input = Arc::new(unqualify_post_aggregate_columns(
                projection.input.as_ref().clone(),
            )?);
            LogicalPlan::Projection(projection)
        }
        LogicalPlan::Filter(mut filter) => {
            filter.predicate = unqualify_post_aggregate_expr(filter.predicate)?;
            filter.input = Arc::new(unqualify_post_aggregate_columns(
                filter.input.as_ref().clone(),
            )?);
            LogicalPlan::Filter(filter)
        }
        LogicalPlan::SubqueryAlias(mut alias) => {
            alias.input = Arc::new(unqualify_post_aggregate_columns(
                alias.input.as_ref().clone(),
            )?);
            LogicalPlan::SubqueryAlias(alias)
        }
        other => other,
    })
}

fn unqualify_post_aggregate_expr(expr: Expr) -> Result<Expr> {
    expr.transform_up(|expr| match expr {
        Expr::Column(column) => Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
            column.name,
        )))),
        other => Ok(Transformed::no(other)),
    })
    .map(|result| result.data)
    .map_err(anyhow::Error::new)
}

fn grouped_stats_aggregate_for_plan(plan: &LogicalPlan) -> Option<GroupedStatsPlanMatch<'_>> {
    match plan {
        LogicalPlan::Aggregate(aggregate) => Some(GroupedStatsPlanMatch {
            aggregate,
            projection: None,
            post_aggregate_plan: None,
        }),
        LogicalPlan::Projection(projection) => match projection.input.as_ref() {
            LogicalPlan::Aggregate(aggregate) => Some(GroupedStatsPlanMatch {
                aggregate,
                projection: Some(projection),
                post_aggregate_plan: None,
            }),
            LogicalPlan::Filter(filter) => match filter.input.as_ref() {
                LogicalPlan::Aggregate(aggregate) => Some(GroupedStatsPlanMatch {
                    aggregate,
                    projection: None,
                    post_aggregate_plan: Some(plan.clone()),
                }),
                _ => aggregate_under_post_aggregate_transform(projection.input.as_ref()).map(
                    |aggregate| GroupedStatsPlanMatch {
                        aggregate,
                        projection: None,
                        post_aggregate_plan: Some(plan.clone()),
                    },
                ),
            },
            _ => aggregate_under_post_aggregate_transform(projection.input.as_ref()).map(
                |aggregate| GroupedStatsPlanMatch {
                    aggregate,
                    projection: None,
                    post_aggregate_plan: Some(plan.clone()),
                },
            ),
        },
        LogicalPlan::Filter(filter) => match filter.input.as_ref() {
            LogicalPlan::Aggregate(aggregate) => Some(GroupedStatsPlanMatch {
                aggregate,
                projection: None,
                post_aggregate_plan: Some(plan.clone()),
            }),
            _ => aggregate_under_post_aggregate_transform(filter.input.as_ref()).map(|aggregate| {
                GroupedStatsPlanMatch {
                    aggregate,
                    projection: None,
                    post_aggregate_plan: Some(plan.clone()),
                }
            }),
        },
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            grouped_stats_aggregate_for_plan(sort.input.as_ref())
        }
        LogicalPlan::SubqueryAlias(alias) => grouped_stats_aggregate_for_plan(alias.input.as_ref()),
        _ => {
            aggregate_under_post_aggregate_transform(plan).map(|aggregate| GroupedStatsPlanMatch {
                aggregate,
                projection: None,
                post_aggregate_plan: Some(plan.clone()),
            })
        }
    }
}

fn aggregate_under_post_aggregate_transform(plan: &LogicalPlan) -> Option<&Aggregate> {
    match plan {
        LogicalPlan::Aggregate(aggregate) => Some(aggregate),
        LogicalPlan::Projection(projection) => {
            aggregate_under_post_aggregate_transform(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => {
            aggregate_under_post_aggregate_transform(filter.input.as_ref())
        }
        LogicalPlan::SubqueryAlias(alias) => {
            aggregate_under_post_aggregate_transform(alias.input.as_ref())
        }
        _ => None,
    }
}

fn output_mapping_for_projection(
    projection: Option<&Projection>,
    aggregate: &Aggregate,
    output_schema: &SchemaRef,
) -> Option<Vec<usize>> {
    let aggregate_schema = &aggregate.schema;
    match projection {
        Some(projection) => {
            if projection.expr.len() != output_schema.fields().len() {
                return None;
            }
            projection
                .expr
                .iter()
                .map(|expr| output_expr_source_idx(strip_alias(expr), aggregate_schema))
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
) -> Option<usize> {
    let Expr::Column(column) = expr else {
        let expr_name = strip_alias(expr).schema_name().to_string();
        return aggregate_schema
            .fields()
            .iter()
            .position(|field| field.name() == &expr_name);
    };
    aggregate_schema
        .fields()
        .iter()
        .position(|field| field.name() == &column.name)
}

fn is_count_star_args(args: &[Expr]) -> bool {
    matches!(args, [Expr::Literal(ScalarValue::Int64(Some(1)), _)])
}

fn strip_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => strip_alias(alias.expr.as_ref()),
        _ => expr,
    }
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
    RowConverter::new(fields).context("build grouped-stats Arrow row converter")
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

fn minmax_value(kind: AggregateKind, left: i64, right: i64) -> i64 {
    match kind {
        AggregateKind::Min => left.min(right),
        AggregateKind::Max => left.max(right),
        _ => unreachable!("minmax_value called for non-min/max aggregate"),
    }
}

fn minmax_i128_value(kind: AggregateKind, left: i128, right: i128) -> i128 {
    match kind {
        AggregateKind::Min => left.min(right),
        AggregateKind::Max => left.max(right),
        _ => unreachable!("minmax_i128_value called for non-min/max aggregate"),
    }
}

fn minmax_string(kind: AggregateKind, left: String, right: String) -> String {
    match kind {
        AggregateKind::Min => left.min(right),
        AggregateKind::Max => left.max(right),
        _ => unreachable!("minmax_string called for non-min/max aggregate"),
    }
}

fn encode_i64_sortable(value: i64) -> [u8; 8] {
    ((value as u64) ^ (1 << 63)).to_be_bytes()
}

fn encode_i128_sortable(value: i128) -> [u8; 16] {
    ((value as u128) ^ (1 << 127)).to_be_bytes()
}

fn decode_i64_sortable(bytes: &[u8]) -> Result<i64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-stats value key suffix must be 8 bytes"))?;
    Ok((u64::from_be_bytes(bytes) ^ (1 << 63)) as i64)
}

fn decode_i128_sortable(bytes: &[u8]) -> Result<i128> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-stats i128 value key suffix must be 16 bytes"))?;
    Ok((u128::from_be_bytes(bytes) ^ (1 << 127)) as i128)
}

fn decode_i64(bytes: &[u8]) -> Result<i64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-stats state value must be 8 bytes"))?;
    Ok(i64::from_be_bytes(bytes))
}

fn decode_i128(bytes: &[u8]) -> Result<i128> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-stats i128 state value must be 16 bytes"))?;
    Ok(i128::from_be_bytes(bytes))
}

fn decode_i64_pair(bytes: &[u8]) -> Result<(i64, i64)> {
    if bytes.len() != 16 {
        bail!("grouped-stats pair state value must be 16 bytes");
    }
    Ok((decode_i64(&bytes[..8])?, decode_i64(&bytes[8..])?))
}
