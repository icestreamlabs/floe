use std::collections::{BTreeSet, HashMap, HashSet, hash_map::Entry};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
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
use datafusion::functions_aggregate::expr_fn::max;
use datafusion::logical_expr::logical_plan::{Aggregate, Projection};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, ScalarUDF};
use datafusion::physical_plan::{ExecutionPlan, collect};
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarZSet};
use dbsp::storage::{KeyValueTable, keyspace, prefix_bounds};
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

use crate::columnar_snapshot::columnar_zset_weight_sum;
use crate::delta_consolidation::{add_weight_column_to_batches, weighted_snapshot_schema};
use crate::encoding::EncodedRowScalar;
use crate::mv::registry::{ColumnarMaterializedViewStorage, MaterializedViewRegistry};
use crate::namespaces;
use crate::scalar_array_builder::ScalarColumnBuilder;
use crate::table_provider::DynamicStateTableProvider;
use crate::vectorized_runtime::source_state::incremental_source_for_plan;
use crate::vectorized_source_delta::{insert_only_source_delta_batch, unit_source_delta_batches};

use super::columnar_grouped_max::{
    ColumnarGroupedMaxMaterializedViewState, ColumnarGroupedMaxPlan,
    build_columnar_grouped_max_materialized_view_state_in_namespace,
    columnar_grouped_max_plan_for_plan, run_columnar_grouped_max_state_tick_delta_only,
};
use super::columnar_join::{
    ColumnarJoinMaterializedViewState, ColumnarJoinPlan,
    build_columnar_join_materialized_view_state_in_namespace_delta_only,
    columnar_join_plan_for_plan, run_columnar_join_state_tick_delta_only,
};
use super::columnar_join_topn::{
    ColumnarJoinTopNMaterializedViewState, ColumnarJoinTopNPlan,
    build_columnar_join_topn_materialized_view_state_in_namespaces,
    columnar_join_topn_plan_for_plan, partitioned_join_top1_value_input_for_plan,
    run_columnar_join_topn_state_tick,
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
use super::profile;
use super::{
    IncrementalMaterializedViewState, VectorizedMaterializedViewState, VectorizedSourceState,
    apply_weighted_snapshot_delta, build_incremental_materialized_view_state_from_logical_plan,
    collect_incremental_output, direct_project_record_batches, direct_projection_indices,
    normalize_batches,
};

const GROUP_TAG: u8 = b'g';
const SCALAR_TAG: u8 = b'a';
const MINMAX_TAG: u8 = b'm';
const VALUE_TAG: u8 = b'v';
const COMPACT_TAG: u8 = b'c';
const APPEND_ONLY_DISTINCT_SEGMENT_TAG: u8 = b'd';
const COMPACT_STATE_VERSION: u8 = 1;
const APPEND_ONLY_DISTINCT_SEGMENT_VERSION: u8 = 1;
const APPEND_ONLY_DISTINCT_I64_TAG: u8 = 1;
const APPEND_ONLY_DISTINCT_I128_TAG: u8 = 2;
const APPEND_ONLY_DISTINCT_STRING_TAG: u8 = 3;
const COMPACT_AGG_UNSUPPORTED_TAG: u8 = 0;
const COMPACT_AGG_I64_TAG: u8 = 1;
const COMPACT_AGG_PAIR_TAG: u8 = 2;
const COMPACT_AGG_MINMAX_NONE_TAG: u8 = 3;
const COMPACT_AGG_MINMAX_I64_TAG: u8 = 4;
const COMPACT_SNAPSHOT_MAGIC: &[u8; 4] = b"cgss";
const COMPACT_SNAPSHOT_VERSION: u8 = 1;
const COMPACT_SNAPSHOT_DENSE_WRITE_MIN_GROUPS: usize = 1024;

pub(super) struct ColumnarGroupedStatsPlan {
    input: ColumnarGroupedStatsInputPlan,
    append_only_input: bool,
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
            ColumnarGroupedStatsInputPlan::GroupedMax { plan, .. } => plan.source_names(),
            ColumnarGroupedStatsInputPlan::GroupedStats { plan, .. } => plan.source_names(),
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
    GroupedMax {
        input_name: String,
        source_schema: SchemaRef,
        projection_input_schema: SchemaRef,
        plan: Box<ColumnarGroupedMaxPlan>,
    },
    GroupedStats {
        input_name: String,
        source_schema: SchemaRef,
        projection_input_schema: SchemaRef,
        plan: Box<ColumnarGroupedStatsPlan>,
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
    append_only_input: bool,
    source_schema: SchemaRef,
    input_zset: Option<SlateBackedColumnarZSet>,
    join: Option<Box<ColumnarJoinMaterializedViewState>>,
    multijoin: Option<Box<ColumnarMultiJoinMaterializedViewState>>,
    join_topn: Option<Box<ColumnarJoinTopNMaterializedViewState>>,
    grouped_max: Option<Box<ColumnarGroupedMaxMaterializedViewState>>,
    grouped_stats: Option<Box<ColumnarGroupedStatsMaterializedViewState>>,
    topn: Option<Box<ColumnarTopNMaterializedViewState>>,
    input_snapshot: Vec<RecordBatch>,
    output_zset: SlateBackedColumnarZSet,
    stats_state: SlateGroupedStatsState,
    publish_arrow_snapshots: bool,
    row_count: i64,
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
    direct_projection: Option<Vec<usize>>,
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

fn compact_grouped_stats_specs(specs: &[AggregateSpec]) -> Option<Vec<AggregateSpec>> {
    specs
        .iter()
        .all(|spec| match spec.kind {
            AggregateKind::Count => true,
            AggregateKind::Sum | AggregateKind::Avg => {
                !matches!(spec.value_type, Some(AggregateValueType::Decimal128))
            }
            AggregateKind::Min | AggregateKind::Max => matches!(
                spec.value_type,
                Some(
                    AggregateValueType::Int64
                        | AggregateValueType::TimestampMillis
                        | AggregateValueType::DateDays
                )
            ),
            AggregateKind::DistinctCount => false,
        })
        .then(|| specs.to_vec())
}

struct SlateGroupedStatsState {
    table: Arc<dyn KeyValueTable>,
    key_prefix: Vec<u8>,
    compact_snapshot_key: Vec<u8>,
    assume_empty: bool,
    compact_specs: Option<Vec<AggregateSpec>>,
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
    append_only_value_presences: Mutex<AppendOnlyDistinctPresenceMap<i64>>,
    append_only_i128_value_presences: Mutex<AppendOnlyDistinctPresenceMap<i128>>,
    append_only_string_value_presences: Mutex<AppendOnlyDistinctPresenceMap<String>>,
    compact_values: Mutex<CompactGroupStateMap>,
    compact_snapshot_loaded: Mutex<bool>,
    compact_snapshot_active: Mutex<bool>,
}

type PendingStatsGroupDeltas = HashMap<Vec<u8>, PendingStatsGroupDelta>;
type CompactGroupStateMap = HashMap<Vec<u8>, CompactGroupState>;
type GroupAggregateKey = (Vec<u8>, usize);
type AppendOnlyDistinctPresenceMap<T> =
    HashMap<GroupAggregateKey, AppendOnlyDistinctPresenceState<T>>;

struct AppendOnlyDistinctPresenceState<T> {
    values: HashSet<T>,
    next_segment_id: u64,
}

#[derive(Clone)]
struct CompactGroupState {
    row_count: i64,
    aggregates: Vec<CompactAggregateState>,
}

#[derive(Clone)]
enum CompactAggregateState {
    I64(i64),
    Pair { sum: i64, count: i64 },
    MinMaxI64(Option<i64>),
    Unsupported,
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

struct PendingCompactStatsGroupDelta {
    row_count_delta: i64,
    agg_deltas: Vec<CompactAggregateDelta>,
    batch: RecordBatch,
    row_idx: usize,
}

type PendingCompactStatsGroupDeltas = HashMap<Vec<u8>, PendingCompactStatsGroupDelta>;

enum CompactAggregateDelta {
    Count { count_delta: i64 },
    Sum { sum_delta: i64 },
    Avg { sum_delta: i64, count_delta: i64 },
    MinMaxI64 { value: Option<i64> },
    Unsupported,
}

pub(super) struct ColumnarGroupedStatsTick {
    pub(super) delta: ColumnarZSet,
    pub(super) next_snapshot: Vec<RecordBatch>,
    pub(super) row_count_delta: i64,
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
    let input =
        if let Some(source_name) = incremental_source_for_plan(aggregate.input.as_ref(), sources) {
            ColumnarGroupedStatsInputPlan::Source { source_name }
        } else if let Some((grouped_max, source_schema)) =
            grouped_stats_top1_value_grouped_max_input_for_aggregate(aggregate, sources)?
        {
            let projection_input_schema = derived_projection_input_schema(&source_schema);
            ColumnarGroupedStatsInputPlan::GroupedMax {
                input_name: "__floe_grouped_stats_top1_value_grouped_max_input".to_string(),
                source_schema,
                projection_input_schema,
                plan: Box::new(grouped_max),
            }
        } else if let Some(grouped_max) = columnar_grouped_max_plan_for_plan(
            aggregate.input.as_ref(),
            sources,
            &df_schema_to_arrow(aggregate.input.schema())?,
        )? {
            let source_schema = df_schema_to_arrow(aggregate.input.schema())?;
            let projection_input_schema = derived_projection_input_schema(&source_schema);
            let input_name = derived_relation_name(aggregate.input.as_ref())
                .unwrap_or_else(|| "__floe_grouped_stats_grouped_max_input".to_string());
            ColumnarGroupedStatsInputPlan::GroupedMax {
                input_name,
                source_schema,
                projection_input_schema,
                plan: Box::new(grouped_max),
            }
        } else if let Some(grouped_stats) = columnar_grouped_stats_plan_for_plan(
            aggregate.input.as_ref(),
            sources,
            &df_schema_to_arrow(aggregate.input.schema())?,
        )? {
            let source_schema = df_schema_to_arrow(aggregate.input.schema())?;
            let projection_input_schema = derived_projection_input_schema(&source_schema);
            let input_name = derived_relation_name(aggregate.input.as_ref())
                .unwrap_or_else(|| "__floe_grouped_stats_grouped_stats_input".to_string());
            ColumnarGroupedStatsInputPlan::GroupedStats {
                input_name,
                source_schema,
                projection_input_schema,
                plan: Box::new(grouped_stats),
            }
        } else if let Some(join) = columnar_join_plan_for_plan(aggregate.input.as_ref(), sources)? {
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
            if join_topn.is_partitioned_best_bid() {
                return Ok(None);
            }
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
    let append_only_input = grouped_stats_input_is_append_only(&input, sources);

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
        if output_mapping_matches_types(&mapping, &aggregate_schema, output_schema) {
            mapping
        } else if plan_match.projection.is_some() {
            post_aggregate_plan = Some(plan.clone());
            Vec::new()
        } else {
            return Ok(None);
        }
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
        | ColumnarGroupedStatsInputPlan::GroupedMax {
            input_name,
            projection_input_schema,
            ..
        }
        | ColumnarGroupedStatsInputPlan::GroupedStats {
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
        append_only_input,
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

fn grouped_stats_input_is_append_only(
    input: &ColumnarGroupedStatsInputPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> bool {
    match input {
        ColumnarGroupedStatsInputPlan::Source { source_name } => sources
            .get(source_name)
            .is_some_and(|source| source.append_only),
        ColumnarGroupedStatsInputPlan::Join { .. }
        | ColumnarGroupedStatsInputPlan::MultiJoin { .. }
        | ColumnarGroupedStatsInputPlan::JoinTopN { .. }
        | ColumnarGroupedStatsInputPlan::GroupedMax { .. }
        | ColumnarGroupedStatsInputPlan::GroupedStats { .. }
        | ColumnarGroupedStatsInputPlan::TopN { .. } => false,
    }
}

pub(super) async fn build_columnar_grouped_stats_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarGroupedStatsPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
    publish_arrow_snapshots: bool,
) -> Result<ColumnarGroupedStatsMaterializedViewState> {
    let mv_namespace = namespaces::materialized_view(view_name)?;
    build_columnar_grouped_stats_materialized_view_state_in_namespace(
        table,
        mv_namespace,
        output_schema,
        plan,
        sources,
        udfs,
        publish_arrow_snapshots,
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
    publish_arrow_snapshots: bool,
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
    let initial_output = output_zset
        .materialize_columnar()
        .await
        .context("load grouped-stats output snapshot")?;
    let initial_row_count = columnar_zset_weight_sum(&initial_output)?;
    let initial_snapshot = snapshot_batches_from_zset(&initial_output)?;
    let append_only_input = plan.append_only_input;
    let (
        input_name,
        source_schema,
        input_zset,
        join,
        multijoin,
        join_topn,
        grouped_max,
        grouped_stats,
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
                None,
                None,
                input_snapshot,
                GroupedStatsProjectionState::Derived(projection_delta),
            )
        }
        ColumnarGroupedStatsInputPlan::GroupedMax {
            input_name,
            source_schema,
            projection_input_schema,
            plan: grouped_max_plan,
        } => {
            let grouped_max_namespace =
                format!("{mv_namespace}/columnar/grouped_stats/grouped_max_input");
            let grouped_max = Box::pin(build_boxed_grouped_max_grouped_stats_input_state(
                Arc::clone(&table),
                grouped_max_namespace,
                &source_schema,
                *grouped_max_plan,
                sources,
                udfs,
            ))
            .await
            .with_context(|| {
                format!(
                    "build SlateDB-backed grouped-stats grouped-max input for '{}'",
                    input_name
                )
            })?;
            let input_snapshot = grouped_max.initial_snapshot();
            let projection_delta = build_derived_projection_state(
                LogicalPlan::Projection(plan.projection.clone()),
                &input_name,
                &projection_input_schema,
                udfs,
            )
            .await
            .with_context(|| {
                format!(
                    "build grouped-stats grouped-max projection delta plan for '{}'",
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
                Some(grouped_max),
                None,
                None,
                input_snapshot,
                GroupedStatsProjectionState::Derived(projection_delta),
            )
        }
        ColumnarGroupedStatsInputPlan::GroupedStats {
            input_name,
            source_schema,
            projection_input_schema,
            plan: grouped_stats_plan,
        } => {
            let grouped_stats_namespace =
                format!("{mv_namespace}/columnar/grouped_stats/grouped_stats_input");
            let grouped_stats = Box::pin(build_boxed_grouped_stats_grouped_stats_input_state(
                Arc::clone(&table),
                grouped_stats_namespace,
                &source_schema,
                *grouped_stats_plan,
                sources,
                udfs,
            ))
            .await
            .with_context(|| {
                format!(
                    "build SlateDB-backed grouped-stats grouped-stats input for '{}'",
                    input_name
                )
            })?;
            let input_snapshot = grouped_stats.initial_snapshot();
            let projection_delta = build_derived_projection_state(
                LogicalPlan::Projection(plan.projection.clone()),
                &input_name,
                &projection_input_schema,
                udfs,
            )
            .await
            .with_context(|| {
                format!(
                    "build grouped-stats grouped-stats projection delta plan for '{}'",
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
                None,
                Some(grouped_stats),
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
    let compact_specs = compact_grouped_stats_specs(&plan.specs);

    Ok(ColumnarGroupedStatsMaterializedViewState {
        input_name,
        append_only_input,
        source_schema,
        input_zset,
        join,
        multijoin,
        join_topn,
        grouped_max,
        grouped_stats,
        topn,
        input_snapshot,
        stats_state: SlateGroupedStatsState::new(
            table,
            &state_namespace,
            output_zset.current_handle().is_none(),
            compact_specs,
        ),
        output_zset,
        projection_delta,
        publish_arrow_snapshots,
        row_count: initial_row_count,
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
        build_columnar_join_materialized_view_state_in_namespace_delta_only(
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

async fn build_boxed_grouped_max_grouped_stats_input_state(
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarGroupedMaxPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<Box<ColumnarGroupedMaxMaterializedViewState>> {
    Ok(Box::new(
        Box::pin(
            build_columnar_grouped_max_materialized_view_state_in_namespace(
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

async fn build_boxed_grouped_stats_grouped_stats_input_state(
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    output_schema: &SchemaRef,
    plan: ColumnarGroupedStatsPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<Box<ColumnarGroupedStatsMaterializedViewState>> {
    Ok(Box::new(
        Box::pin(
            build_columnar_grouped_stats_materialized_view_state_in_namespace(
                table,
                namespace,
                output_schema,
                plan,
                sources,
                udfs,
                true,
            ),
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
    let direct_projection = direct_projection_indices(&logical_plan, input_schema);
    let logical_plan =
        rebind_derived_projection_plan(logical_plan, input_name, Arc::clone(&provider))?;
    let plan = ctx.state().create_physical_plan(&logical_plan).await?;
    Ok(GroupedStatsDerivedProjectionState {
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
    let delta_rows = delta_batches
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    let input_changed = tick.input_changed;
    columnar.row_count = columnar.row_count.saturating_add(tick.row_count_delta);
    if columnar.row_count < 0 {
        bail!(
            "grouped-stats columnar materialized view '{}' row count became negative",
            mv.view_name
        );
    }
    let snapshot_rows =
        usize::try_from(columnar.row_count).context("grouped-stats row count exceeds usize")?;
    let handle = registry.register(mv.view_name.clone());
    if columnar.publish_arrow_snapshots {
        handle.publish_arrow_version(version, tick.next_snapshot.clone(), delta_batches);
        mv.previous_snapshot = tick.next_snapshot;
    } else if let Some(zset_handle) = columnar.output_zset.current_handle() {
        handle.publish_columnar_version(
            version,
            zset_handle,
            ColumnarMaterializedViewStorage::new(
                Arc::clone(&columnar.stats_state.table),
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
    }
    tracing::debug!(
        view = %mv.view_name,
        version,
        delta_rows,
        snapshot_rows,
        input_changed,
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
    let total_start = profile::start();
    let phase_start = profile::start();
    let persisted_input_delta =
        prepare_grouped_stats_input_delta(columnar, insert_batches, weighted_delta_batches).await?;
    profile::record_since("grouped_stats.prepare_input", phase_start);
    let input_changed = !persisted_input_delta.batches().is_empty();
    let output_delta_batches = if columnar.append_only_input
        && columnar.stats_state.compact_enabled()
    {
        let phase_start = profile::start();
        if let Some(pending) = grouped_stats_append_only_compact_pending_delta(
            columnar,
            persisted_input_delta.batches(),
        )
        .await?
        {
            profile::record_since("grouped_stats.pending_delta", phase_start);
            let phase_start = profile::start();
            let output_delta_batches =
                apply_append_only_compact_grouped_stats_compact_delta(columnar, pending).await?;
            profile::record_since("grouped_stats.apply_delta", phase_start);
            output_delta_batches
        } else {
            let pending =
                grouped_stats_pending_delta(columnar, persisted_input_delta.batches()).await?;
            profile::record_since("grouped_stats.pending_delta", phase_start);
            let phase_start = profile::start();
            let output_delta_batches = apply_grouped_stats_delta(columnar, pending).await?;
            profile::record_since("grouped_stats.apply_delta", phase_start);
            output_delta_batches
        }
    } else {
        let phase_start = profile::start();
        let pending =
            grouped_stats_pending_delta(columnar, persisted_input_delta.batches()).await?;
        profile::record_since("grouped_stats.pending_delta", phase_start);
        let phase_start = profile::start();
        let output_delta_batches = apply_grouped_stats_delta(columnar, pending).await?;
        profile::record_since("grouped_stats.apply_delta", phase_start);
        output_delta_batches
    };
    let phase_start = profile::start();
    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build grouped-stats output zset delta")?;
    profile::record_since("grouped_stats.build_output_zset", phase_start);
    let phase_start = profile::start();
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
    profile::record_since("grouped_stats.output_create_version", phase_start);
    let persisted_output_delta = output_delta;

    let delta_batches = persisted_output_delta.batches().to_vec();
    let row_count_delta = columnar_zset_weight_sum(&persisted_output_delta)
        .context("compute grouped-stats output row-count delta")?;
    let next_snapshot = if columnar.publish_arrow_snapshots {
        let phase_start = profile::start();
        let next_snapshot =
            apply_weighted_snapshot_delta(output_schema, previous_snapshot, delta_batches.clone())
                .await
                .context("apply Slate-backed grouped-stats columnar snapshot delta")?;
        profile::record_since("grouped_stats.output_snapshot_delta", phase_start);
        next_snapshot
    } else {
        Vec::new()
    };

    profile::record_since("grouped_stats.total", total_start);
    Ok(ColumnarGroupedStatsTick {
        delta: persisted_output_delta,
        next_snapshot,
        row_count_delta,
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
    if columnar.grouped_max.is_some() {
        return prepare_grouped_max_grouped_stats_input_delta(
            columnar,
            insert_batches,
            weighted_delta_batches,
        )
        .await;
    }
    if columnar.grouped_stats.is_some() {
        return prepare_grouped_stats_grouped_stats_input_delta(
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
            "evaluate grouped-stats nested join input '{}'",
            columnar.input_name
        )
    })?;
    if tick.input_changed && !tick.next_snapshot.is_empty() {
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

async fn prepare_grouped_max_grouped_stats_input_delta(
    columnar: &mut ColumnarGroupedStatsMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    let Some(grouped_max) = columnar.grouped_max.as_mut() else {
        return ColumnarZSet::empty(Arc::clone(&columnar.source_schema));
    };
    let tick = Box::pin(run_columnar_grouped_max_state_tick_delta_only(
        grouped_max.as_mut(),
        insert_batches,
        weighted_delta_batches,
        &columnar.source_schema,
        &columnar.input_snapshot,
    ))
    .await
    .with_context(|| {
        format!(
            "evaluate grouped-stats nested grouped-max input '{}'",
            columnar.input_name
        )
    })?;
    if tick.input_changed && !tick.next_snapshot.is_empty() {
        columnar.input_snapshot = tick.next_snapshot;
    }
    Ok(tick.delta)
}

async fn prepare_grouped_stats_grouped_stats_input_delta(
    columnar: &mut ColumnarGroupedStatsMaterializedViewState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    let Some(grouped_stats) = columnar.grouped_stats.as_mut() else {
        return ColumnarZSet::empty(Arc::clone(&columnar.source_schema));
    };
    let tick = Box::pin(run_columnar_grouped_stats_state_tick(
        grouped_stats.as_mut(),
        insert_batches,
        weighted_delta_batches,
        &columnar.source_schema,
        &columnar.input_snapshot,
    ))
    .await
    .with_context(|| {
        format!(
            "evaluate grouped-stats nested grouped-stats input '{}'",
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
) -> Result<PendingStatsGroupDeltas> {
    let input_row_count = input_batches.iter().map(RecordBatch::num_rows).sum();
    let mut pending = PendingStatsGroupDeltas::with_capacity(input_row_count);
    if input_batches.is_empty() {
        return Ok(pending);
    }

    let mut positive_source_batches = Vec::new();
    let mut negative_source_batches = Vec::new();
    let phase_start = profile::start();
    for batch in input_batches {
        if columnar.append_only_input
            && let Some(insert_batch) =
                insert_only_source_delta_batch(&columnar.source_schema, batch)?
        {
            positive_source_batches.push(insert_batch);
            continue;
        }
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
    profile::record_since("grouped_stats.pending_split_source_delta", phase_start);

    let phase_start = profile::start();
    let positive_output =
        collect_grouped_stats_projection_output(columnar, &positive_source_batches).await?;
    profile::record_since("grouped_stats.pending_project_positive", phase_start);
    let phase_start = profile::start();
    add_projected_stats_batches_to_pending(columnar, &positive_output, 1, &mut pending)?;
    profile::record_since("grouped_stats.pending_add_positive", phase_start);
    let phase_start = profile::start();
    let negative_output =
        collect_grouped_stats_projection_output(columnar, &negative_source_batches).await?;
    profile::record_since("grouped_stats.pending_project_negative", phase_start);
    let phase_start = profile::start();
    add_projected_stats_batches_to_pending(columnar, &negative_output, -1, &mut pending)?;
    profile::record_since("grouped_stats.pending_add_negative", phase_start);
    let phase_start = profile::start();
    pending.retain(|_, delta| {
        delta.row_count_delta != 0 || !aggregate_deltas_empty(&delta.agg_deltas)
    });
    profile::record_since("grouped_stats.pending_retain", phase_start);
    Ok(pending)
}

async fn grouped_stats_append_only_compact_pending_delta(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    input_batches: &[RecordBatch],
) -> Result<Option<PendingCompactStatsGroupDeltas>> {
    if !columnar.append_only_input || !columnar.stats_state.compact_enabled() {
        return Ok(None);
    }
    let input_row_count = input_batches.iter().map(RecordBatch::num_rows).sum();
    let mut positive_source_batches = Vec::new();
    let phase_start = profile::start();
    for batch in input_batches {
        let Some(insert_batch) = insert_only_source_delta_batch(&columnar.source_schema, batch)?
        else {
            return Ok(None);
        };
        positive_source_batches.push(insert_batch);
    }
    profile::record_since("grouped_stats.pending_split_source_delta", phase_start);

    let phase_start = profile::start();
    let positive_output =
        collect_grouped_stats_projection_output(columnar, &positive_source_batches).await?;
    profile::record_since("grouped_stats.pending_project_positive", phase_start);
    let phase_start = profile::start();
    let mut pending = PendingCompactStatsGroupDeltas::with_capacity(input_row_count);
    add_projected_compact_stats_batches_to_pending(columnar, &positive_output, &mut pending)?;
    profile::record_since("grouped_stats.pending_add_positive", phase_start);
    let phase_start = profile::start();
    pending.retain(|_, delta| delta.row_count_delta != 0);
    profile::record_since("grouped_stats.pending_retain", phase_start);
    Ok(Some(pending))
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
            if let Some(indices) = derived.direct_projection.as_ref() {
                return direct_project_record_batches(
                    source_batches,
                    &columnar.projection_schema,
                    indices,
                    "grouped-stats",
                );
            }
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
    pending: &mut PendingStatsGroupDeltas,
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    if columnar.append_only_input && sign < 0 && batches.iter().any(|batch| batch.num_rows() > 0) {
        bail!("append-only grouped-stats input received a negative delta");
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

fn add_projected_compact_stats_batches_to_pending(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    batches: &[RecordBatch],
    pending: &mut PendingCompactStatsGroupDeltas,
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
                    .context("encode grouped-stats compact group keys")?;
                for row_idx in 0..batch.num_rows() {
                    add_projected_compact_stats_row_to_pending(
                        columnar,
                        batch,
                        row_idx,
                        group_rows.row(row_idx).data().to_vec(),
                        &value_arrays,
                        &filter_arrays,
                        pending,
                    )?;
                }
            }
            None => {
                for row_idx in 0..batch.num_rows() {
                    add_projected_compact_stats_row_to_pending(
                        columnar,
                        batch,
                        row_idx,
                        Vec::new(),
                        &value_arrays,
                        &filter_arrays,
                        pending,
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn add_projected_compact_stats_row_to_pending(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    batch: &RecordBatch,
    row_idx: usize,
    key: Vec<u8>,
    value_arrays: &[ProjectedValueArray<'_>],
    filter_arrays: &[Option<&BooleanArray>],
    pending: &mut PendingCompactStatsGroupDeltas,
) -> Result<()> {
    let group = pending
        .entry(key)
        .or_insert_with(|| PendingCompactStatsGroupDelta {
            row_count_delta: 0,
            agg_deltas: columnar
                .specs
                .iter()
                .map(CompactAggregateDelta::for_spec)
                .collect(),
            batch: batch.clone(),
            row_idx,
        });
    group.row_count_delta = group
        .row_count_delta
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("grouped-stats row count delta overflow"))?;
    for (agg_idx, spec) in columnar.specs.iter().enumerate() {
        if !filter_allows(&filter_arrays[agg_idx], row_idx) {
            continue;
        }
        match (&mut group.agg_deltas[agg_idx], spec.kind) {
            (CompactAggregateDelta::Count { count_delta }, AggregateKind::Count) => {
                if spec.value_idx.is_some()
                    && !projected_value_is_non_null(&value_arrays[agg_idx], row_idx)
                {
                    continue;
                }
                *count_delta = count_delta
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats count delta overflow"))?;
            }
            (CompactAggregateDelta::Sum { sum_delta }, AggregateKind::Sum) => {
                let Some(value) = projected_i64_value(&value_arrays[agg_idx], row_idx) else {
                    continue;
                };
                *sum_delta = sum_delta
                    .checked_add(value)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats sum delta overflow"))?;
            }
            (
                CompactAggregateDelta::Avg {
                    sum_delta,
                    count_delta,
                },
                AggregateKind::Avg,
            ) => {
                let Some(value) = projected_i64_value(&value_arrays[agg_idx], row_idx) else {
                    continue;
                };
                *sum_delta = sum_delta
                    .checked_add(value)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats avg sum delta overflow"))?;
                *count_delta = count_delta
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats avg count delta overflow"))?;
            }
            (
                CompactAggregateDelta::MinMaxI64 { value },
                AggregateKind::Min | AggregateKind::Max,
            ) => {
                let Some(next) = projected_ordered_i64_value(&value_arrays[agg_idx], row_idx)
                else {
                    continue;
                };
                *value = Some(match *value {
                    Some(current) => minmax_value(spec.kind, current, next),
                    None => next,
                });
            }
            _ => bail!("grouped-stats compact aggregate delta kind mismatch"),
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
    pending: &mut PendingStatsGroupDeltas,
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
    pending: PendingStatsGroupDeltas,
) -> Result<Vec<RecordBatch>> {
    let output_row_capacity = pending.len().saturating_mul(2).max(1024);
    let mut direct_builder = WeightedStatsOutputBuilder::with_capacity(
        columnar.output_zset.value_schema(),
        &columnar.output_mapping,
        output_row_capacity,
    )?;
    let mut old_aggregate_builder = AggregateStatsOutputBuilder::with_capacity(
        Arc::clone(&columnar.aggregate_schema),
        columnar.group_count,
        pending.len().max(1024),
    )?;
    let mut new_aggregate_builder = AggregateStatsOutputBuilder::with_capacity(
        Arc::clone(&columnar.aggregate_schema),
        columnar.group_count,
        pending.len().max(1024),
    )?;
    if pending.is_empty() {
        return direct_builder.finish();
    }

    let compact_enabled = columnar.stats_state.compact_enabled();
    if compact_enabled && columnar.append_only_input {
        return apply_append_only_compact_grouped_stats_delta(
            columnar,
            pending,
            direct_builder,
            old_aggregate_builder,
            new_aggregate_builder,
        )
        .await;
    }
    if compact_enabled {
        return apply_compact_grouped_stats_delta(
            columnar,
            pending,
            direct_builder,
            old_aggregate_builder,
            new_aggregate_builder,
        )
        .await;
    }
    let mut writes = WriteBatch::new();
    for (group_key, delta) in pending {
        let phase_start = profile::start();
        let old_row_count = columnar.stats_state.load_group_count(&group_key).await?;
        let old_values = load_aggregate_values(columnar, &group_key).await?;
        profile::record_since("grouped_stats.apply_load_old", phase_start);
        let phase_start = profile::start();
        let new_row_count = old_row_count
            .checked_add(delta.row_count_delta)
            .ok_or_else(|| anyhow::anyhow!("grouped-stats row count overflow"))?;
        if new_row_count < 0 {
            bail!("grouped-stats state removed more rows than were present");
        }
        let new_values =
            apply_aggregate_deltas(columnar, &group_key, &delta.agg_deltas, &mut writes).await?;
        if compact_enabled {
            columnar
                .stats_state
                .cache_group_count(&group_key, new_row_count)?;
            let compact_state =
                compact_state_from_current(columnar, &group_key, new_row_count).await?;
            columnar
                .stats_state
                .write_compact_state(&mut writes, &group_key, compact_state)?;
        } else {
            columnar
                .stats_state
                .write_group_count(&mut writes, &group_key, new_row_count)?;
        }
        profile::record_since("grouped_stats.apply_update_state", phase_start);

        let phase_start = profile::start();
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
        profile::record_since("grouped_stats.apply_build_rows", phase_start);
    }
    let phase_start = profile::start();
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
    profile::record_since("grouped_stats.apply_finish_output", phase_start);
    let phase_start = profile::start();
    columnar
        .stats_state
        .table
        .write_batch(writes)
        .await
        .context("persist grouped-stats state updates")?;
    profile::record_since("grouped_stats.apply_write_batch", phase_start);
    Ok(output_delta_batches)
}

async fn apply_compact_grouped_stats_delta(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    pending: PendingStatsGroupDeltas,
    mut direct_builder: WeightedStatsOutputBuilder,
    mut old_aggregate_builder: AggregateStatsOutputBuilder,
    mut new_aggregate_builder: AggregateStatsOutputBuilder,
) -> Result<Vec<RecordBatch>> {
    let mut writes = WriteBatch::new();
    columnar
        .stats_state
        .load_compact_snapshot_if_needed()
        .await?;
    for (group_key, delta) in pending {
        let phase_start = profile::start();
        let mut state = load_compact_or_legacy_group_state(columnar, &group_key).await?;
        profile::record_since("grouped_stats.apply_load_old", phase_start);

        let phase_start = profile::start();
        let old_state = state.clone();
        let old_row_count = old_state.row_count;
        state.row_count = old_row_count
            .checked_add(delta.row_count_delta)
            .ok_or_else(|| anyhow::anyhow!("grouped-stats row count overflow"))?;
        if state.row_count < 0 {
            bail!("grouped-stats state removed more rows than were present");
        }
        apply_compact_aggregate_deltas(
            columnar,
            &group_key,
            &mut state,
            &delta.agg_deltas,
            &mut writes,
        )
        .await?;
        let output_changed = old_row_count == 0
            || state.row_count == 0
            || !compact_group_state_outputs_equal(&columnar.specs, &old_state, &state)?;
        profile::record_since("grouped_stats.apply_update_state", phase_start);

        let phase_start = profile::start();
        if old_row_count > 0 && output_changed {
            if columnar.post_aggregate.is_some() {
                old_aggregate_builder.append_compact_state(
                    &delta.batch,
                    delta.row_idx,
                    &columnar.specs,
                    &old_state,
                )?;
            } else {
                direct_builder.append_compact_state(
                    &delta.batch,
                    delta.row_idx,
                    columnar.group_count,
                    &columnar.specs,
                    &old_state,
                    -1,
                )?;
            }
        }
        if state.row_count > 0 && output_changed {
            if columnar.post_aggregate.is_some() {
                new_aggregate_builder.append_compact_state(
                    &delta.batch,
                    delta.row_idx,
                    &columnar.specs,
                    &state,
                )?;
            } else {
                direct_builder.append_compact_state(
                    &delta.batch,
                    delta.row_idx,
                    columnar.group_count,
                    &columnar.specs,
                    &state,
                    1,
                )?;
            }
        }
        profile::record_since("grouped_stats.apply_build_rows", phase_start);

        let phase_start = profile::start();
        columnar
            .stats_state
            .write_compact_state(&mut writes, &group_key, state)?;
        profile::record_since("grouped_stats.apply_cache_state", phase_start);
    }

    let phase_start = profile::start();
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
    profile::record_since("grouped_stats.apply_finish_output", phase_start);

    let phase_start = profile::start();
    columnar
        .stats_state
        .table
        .write_batch(writes)
        .await
        .context("persist compact grouped-stats state updates")?;
    profile::record_since("grouped_stats.apply_write_batch", phase_start);
    Ok(output_delta_batches)
}

async fn apply_append_only_compact_grouped_stats_delta(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    pending: PendingStatsGroupDeltas,
    mut direct_builder: WeightedStatsOutputBuilder,
    mut old_aggregate_builder: AggregateStatsOutputBuilder,
    mut new_aggregate_builder: AggregateStatsOutputBuilder,
) -> Result<Vec<RecordBatch>> {
    let mut writes = WriteBatch::new();
    columnar
        .stats_state
        .load_compact_snapshot_if_needed()
        .await?;
    let write_compact_snapshot = columnar
        .stats_state
        .should_write_compact_snapshot(pending.len())?;
    for (group_key, delta) in pending {
        let phase_start = profile::start();
        let mut initial_state = compact_state_initial_for_mutation(columnar, &group_key).await?;
        profile::record_since("grouped_stats.apply_load_old", phase_start);

        columnar.stats_state.mutate_compact_state(
            &group_key,
            || {
                initial_state
                    .take()
                    .map(Ok)
                    .unwrap_or_else(|| empty_compact_group_state(columnar))
            },
            |state| {
                let phase_start = profile::start();
                let old_state = state.clone();
                let old_row_count = old_state.row_count;
                state.row_count = old_row_count
                    .checked_add(delta.row_count_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats row count overflow"))?;
                if state.row_count < 0 {
                    bail!("grouped-stats state removed more rows than were present");
                }
                apply_append_only_compact_aggregate_deltas(columnar, state, &delta.agg_deltas)?;
                let output_changed = old_row_count == 0
                    || state.row_count == 0
                    || !compact_group_state_outputs_equal(&columnar.specs, &old_state, state)?;
                profile::record_since("grouped_stats.apply_update_state", phase_start);

                let phase_start = profile::start();
                if old_row_count > 0 && output_changed {
                    if columnar.post_aggregate.is_some() {
                        old_aggregate_builder.append_compact_state(
                            &delta.batch,
                            delta.row_idx,
                            &columnar.specs,
                            &old_state,
                        )?;
                    } else {
                        direct_builder.append_compact_state(
                            &delta.batch,
                            delta.row_idx,
                            columnar.group_count,
                            &columnar.specs,
                            &old_state,
                            -1,
                        )?;
                    }
                }
                if state.row_count > 0 && output_changed {
                    if columnar.post_aggregate.is_some() {
                        new_aggregate_builder.append_compact_state(
                            &delta.batch,
                            delta.row_idx,
                            &columnar.specs,
                            state,
                        )?;
                    } else {
                        direct_builder.append_compact_state(
                            &delta.batch,
                            delta.row_idx,
                            columnar.group_count,
                            &columnar.specs,
                            state,
                            1,
                        )?;
                    }
                }
                profile::record_since("grouped_stats.apply_build_rows", phase_start);

                let phase_start = profile::start();
                if !write_compact_snapshot {
                    columnar.stats_state.write_compact_state_to_batch(
                        &mut writes,
                        &group_key,
                        state,
                    )?;
                }
                profile::record_since("grouped_stats.apply_cache_state", phase_start);
                Ok(())
            },
        )?;
    }

    let phase_start = profile::start();
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
    profile::record_since("grouped_stats.apply_finish_output", phase_start);

    if write_compact_snapshot {
        let phase_start = profile::start();
        columnar.stats_state.write_compact_snapshot(&mut writes)?;
        profile::record_since("grouped_stats.apply_snapshot_state", phase_start);
    }

    let phase_start = profile::start();
    columnar
        .stats_state
        .table
        .write_batch(writes)
        .await
        .context("persist append-only grouped-stats state updates")?;
    profile::record_since("grouped_stats.apply_write_batch", phase_start);
    Ok(output_delta_batches)
}

async fn apply_append_only_compact_grouped_stats_compact_delta(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    pending: PendingCompactStatsGroupDeltas,
) -> Result<Vec<RecordBatch>> {
    let output_row_capacity = pending.len().saturating_mul(2).max(1024);
    let mut direct_builder = WeightedStatsOutputBuilder::with_capacity(
        columnar.output_zset.value_schema(),
        &columnar.output_mapping,
        output_row_capacity,
    )?;
    let mut old_aggregate_builder = AggregateStatsOutputBuilder::with_capacity(
        Arc::clone(&columnar.aggregate_schema),
        columnar.group_count,
        pending.len().max(1024),
    )?;
    let mut new_aggregate_builder = AggregateStatsOutputBuilder::with_capacity(
        Arc::clone(&columnar.aggregate_schema),
        columnar.group_count,
        pending.len().max(1024),
    )?;
    if pending.is_empty() {
        return direct_builder.finish();
    }

    let mut writes = WriteBatch::new();
    columnar
        .stats_state
        .load_compact_snapshot_if_needed()
        .await?;
    let write_compact_snapshot = columnar
        .stats_state
        .should_write_compact_snapshot(pending.len())?;
    for (group_key, delta) in pending {
        let phase_start = profile::start();
        let mut initial_state = compact_state_initial_for_mutation(columnar, &group_key).await?;
        profile::record_since("grouped_stats.apply_load_old", phase_start);

        columnar.stats_state.mutate_compact_state(
            &group_key,
            || {
                initial_state
                    .take()
                    .map(Ok)
                    .unwrap_or_else(|| empty_compact_group_state(columnar))
            },
            |state| {
                let phase_start = profile::start();
                let old_state = state.clone();
                let old_row_count = old_state.row_count;
                state.row_count = old_row_count
                    .checked_add(delta.row_count_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats row count overflow"))?;
                if state.row_count < 0 {
                    bail!("grouped-stats state removed more rows than were present");
                }
                apply_append_only_compact_aggregate_compact_deltas(
                    &columnar.specs,
                    state,
                    &delta.agg_deltas,
                )?;
                let output_changed = old_row_count == 0
                    || state.row_count == 0
                    || !compact_group_state_outputs_equal(&columnar.specs, &old_state, state)?;
                profile::record_since("grouped_stats.apply_update_state", phase_start);

                let phase_start = profile::start();
                if old_row_count > 0 && output_changed {
                    if columnar.post_aggregate.is_some() {
                        old_aggregate_builder.append_compact_state(
                            &delta.batch,
                            delta.row_idx,
                            &columnar.specs,
                            &old_state,
                        )?;
                    } else {
                        direct_builder.append_compact_state(
                            &delta.batch,
                            delta.row_idx,
                            columnar.group_count,
                            &columnar.specs,
                            &old_state,
                            -1,
                        )?;
                    }
                }
                if state.row_count > 0 && output_changed {
                    if columnar.post_aggregate.is_some() {
                        new_aggregate_builder.append_compact_state(
                            &delta.batch,
                            delta.row_idx,
                            &columnar.specs,
                            state,
                        )?;
                    } else {
                        direct_builder.append_compact_state(
                            &delta.batch,
                            delta.row_idx,
                            columnar.group_count,
                            &columnar.specs,
                            state,
                            1,
                        )?;
                    }
                }
                profile::record_since("grouped_stats.apply_build_rows", phase_start);

                let phase_start = profile::start();
                if !write_compact_snapshot {
                    columnar.stats_state.write_compact_state_to_batch(
                        &mut writes,
                        &group_key,
                        state,
                    )?;
                }
                profile::record_since("grouped_stats.apply_cache_state", phase_start);
                Ok(())
            },
        )?;
    }

    let phase_start = profile::start();
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
    profile::record_since("grouped_stats.apply_finish_output", phase_start);

    if write_compact_snapshot {
        let phase_start = profile::start();
        columnar.stats_state.write_compact_snapshot(&mut writes)?;
        profile::record_since("grouped_stats.apply_snapshot_state", phase_start);
    }

    let phase_start = profile::start();
    columnar
        .stats_state
        .table
        .write_batch(writes)
        .await
        .context("persist append-only compact grouped-stats state updates")?;
    profile::record_since("grouped_stats.apply_write_batch", phase_start);
    Ok(output_delta_batches)
}

async fn load_compact_or_legacy_group_state(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    group_key: &[u8],
) -> Result<CompactGroupState> {
    if let Some(state) = columnar.stats_state.load_compact_state(group_key).await? {
        return Ok(state);
    }
    if columnar.stats_state.compact_snapshot_active()? {
        return empty_compact_group_state(columnar);
    }
    if columnar.stats_state.assume_empty {
        return empty_compact_group_state(columnar);
    }
    let row_count = columnar.stats_state.load_group_count(group_key).await?;
    compact_state_from_current(columnar, group_key, row_count).await
}

async fn compact_state_initial_for_mutation(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    group_key: &[u8],
) -> Result<Option<CompactGroupState>> {
    if columnar.stats_state.assume_empty || columnar.stats_state.compact_snapshot_active()? {
        return Ok(None);
    }
    if columnar.stats_state.has_compact_state(group_key)? {
        return Ok(None);
    }
    load_compact_or_legacy_group_state(columnar, group_key)
        .await
        .map(Some)
}

fn empty_compact_group_state(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
) -> Result<CompactGroupState> {
    let specs = columnar
        .stats_state
        .compact_specs
        .as_ref()
        .context("grouped-stats compact state requested for non-compact plan")?;
    let aggregates = specs
        .iter()
        .map(|spec| match spec.kind {
            AggregateKind::Count | AggregateKind::Sum => Ok(CompactAggregateState::I64(0)),
            AggregateKind::Avg => Ok(CompactAggregateState::Pair { sum: 0, count: 0 }),
            AggregateKind::Min | AggregateKind::Max => Ok(CompactAggregateState::MinMaxI64(None)),
            AggregateKind::DistinctCount => {
                bail!("grouped-stats distinct count is not compactable")
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(CompactGroupState {
        row_count: 0,
        aggregates,
    })
}

fn compact_group_state_outputs_equal(
    specs: &[AggregateSpec],
    left: &CompactGroupState,
    right: &CompactGroupState,
) -> Result<bool> {
    if left.aggregates.len() != specs.len() || right.aggregates.len() != specs.len() {
        bail!("grouped-stats compact aggregate state length mismatch");
    }
    for (idx, spec) in specs.iter().enumerate() {
        if !compact_aggregate_output_equal(spec, &left.aggregates[idx], &right.aggregates[idx])? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn compact_aggregate_output_equal(
    spec: &AggregateSpec,
    left: &CompactAggregateState,
    right: &CompactAggregateState,
) -> Result<bool> {
    match (spec.kind, left, right) {
        (
            AggregateKind::Count | AggregateKind::Sum,
            CompactAggregateState::I64(left),
            CompactAggregateState::I64(right),
        ) => Ok(left == right),
        (
            AggregateKind::Avg,
            CompactAggregateState::Pair {
                sum: left_sum,
                count: left_count,
            },
            CompactAggregateState::Pair {
                sum: right_sum,
                count: right_count,
            },
        ) => match (*left_count == 0, *right_count == 0) {
            (true, true) => Ok(true),
            (true, false) | (false, true) => Ok(false),
            (false, false) => Ok(
                *left_sum as f64 / *left_count as f64 == *right_sum as f64 / *right_count as f64
            ),
        },
        (
            AggregateKind::Min | AggregateKind::Max,
            CompactAggregateState::MinMaxI64(left),
            CompactAggregateState::MinMaxI64(right),
        ) => Ok(left == right),
        _ => bail!("grouped-stats compact aggregate state kind mismatch"),
    }
}

fn apply_append_only_compact_aggregate_deltas(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    state: &mut CompactGroupState,
    deltas: &[AggregateDelta],
) -> Result<()> {
    for (idx, (spec, delta)) in columnar.specs.iter().zip(deltas.iter()).enumerate() {
        let aggregate = state
            .aggregates
            .get_mut(idx)
            .ok_or_else(|| anyhow::anyhow!("grouped-stats compact aggregate index missing"))?;
        match (spec.kind, delta, aggregate) {
            (
                AggregateKind::Count,
                AggregateDelta::Count { count_delta },
                CompactAggregateState::I64(value),
            ) => {
                *value = value
                    .checked_add(*count_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats count overflow"))?;
                if *value < 0 {
                    bail!("grouped-stats count became negative");
                }
            }
            (
                AggregateKind::Sum,
                AggregateDelta::Sum { sum_delta },
                CompactAggregateState::I64(value),
            ) => {
                *value = value
                    .checked_add(*sum_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats sum overflow"))?;
            }
            (
                AggregateKind::Avg,
                AggregateDelta::Avg {
                    sum_delta,
                    count_delta,
                },
                CompactAggregateState::Pair { sum, count },
            ) => {
                *sum = sum
                    .checked_add(*sum_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats avg sum overflow"))?;
                *count = count
                    .checked_add(*count_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats avg count overflow"))?;
                if *count < 0 {
                    bail!("grouped-stats avg count became negative");
                }
            }
            (
                AggregateKind::Min | AggregateKind::Max,
                AggregateDelta::MinMaxI64 { value_deltas },
                CompactAggregateState::MinMaxI64(value),
            ) => {
                for (delta_value, value_delta) in value_deltas {
                    if *value_delta < 0 {
                        bail!("append-only grouped-stats min/max received a negative delta");
                    }
                    if *value_delta > 0 {
                        *value = Some(match *value {
                            Some(current) => minmax_value(spec.kind, current, *delta_value),
                            None => *delta_value,
                        });
                    }
                }
            }
            _ => bail!("grouped-stats append-only compact aggregate state kind mismatch"),
        }
    }
    Ok(())
}

fn apply_append_only_compact_aggregate_compact_deltas(
    specs: &[AggregateSpec],
    state: &mut CompactGroupState,
    deltas: &[CompactAggregateDelta],
) -> Result<()> {
    for (idx, (spec, delta)) in specs.iter().zip(deltas.iter()).enumerate() {
        let aggregate = state
            .aggregates
            .get_mut(idx)
            .ok_or_else(|| anyhow::anyhow!("grouped-stats compact aggregate index missing"))?;
        match (spec.kind, delta, aggregate) {
            (
                AggregateKind::Count,
                CompactAggregateDelta::Count { count_delta },
                CompactAggregateState::I64(value),
            ) => {
                *value = value
                    .checked_add(*count_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats count overflow"))?;
                if *value < 0 {
                    bail!("grouped-stats count became negative");
                }
            }
            (
                AggregateKind::Sum,
                CompactAggregateDelta::Sum { sum_delta },
                CompactAggregateState::I64(value),
            ) => {
                *value = value
                    .checked_add(*sum_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats sum overflow"))?;
            }
            (
                AggregateKind::Avg,
                CompactAggregateDelta::Avg {
                    sum_delta,
                    count_delta,
                },
                CompactAggregateState::Pair { sum, count },
            ) => {
                *sum = sum
                    .checked_add(*sum_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats avg sum overflow"))?;
                *count = count
                    .checked_add(*count_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats avg count overflow"))?;
                if *count < 0 {
                    bail!("grouped-stats avg count became negative");
                }
            }
            (
                AggregateKind::Min | AggregateKind::Max,
                CompactAggregateDelta::MinMaxI64 {
                    value: Some(delta_value),
                },
                CompactAggregateState::MinMaxI64(value),
            ) => {
                *value = Some(match *value {
                    Some(current) => minmax_value(spec.kind, current, *delta_value),
                    None => *delta_value,
                });
            }
            (
                AggregateKind::Min | AggregateKind::Max,
                CompactAggregateDelta::MinMaxI64 { value: None },
                CompactAggregateState::MinMaxI64(_),
            ) => {}
            _ => bail!("grouped-stats append-only compact aggregate delta kind mismatch"),
        }
    }
    Ok(())
}

async fn apply_compact_aggregate_deltas(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    group_key: &[u8],
    state: &mut CompactGroupState,
    deltas: &[AggregateDelta],
    writes: &mut WriteBatch,
) -> Result<()> {
    for (idx, (spec, delta)) in columnar.specs.iter().zip(deltas.iter()).enumerate() {
        let aggregate = state
            .aggregates
            .get_mut(idx)
            .ok_or_else(|| anyhow::anyhow!("grouped-stats compact aggregate index missing"))?;
        match (spec.kind, delta, aggregate) {
            (
                AggregateKind::Count,
                AggregateDelta::Count { count_delta },
                CompactAggregateState::I64(value),
            ) => {
                *value = value
                    .checked_add(*count_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats count overflow"))?;
                if *value < 0 {
                    bail!("grouped-stats count became negative");
                }
            }
            (
                AggregateKind::Sum,
                AggregateDelta::Sum { sum_delta },
                CompactAggregateState::I64(value),
            ) => {
                *value = value
                    .checked_add(*sum_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats sum overflow"))?;
            }
            (
                AggregateKind::Avg,
                AggregateDelta::Avg {
                    sum_delta,
                    count_delta,
                },
                CompactAggregateState::Pair { sum, count },
            ) => {
                *sum = sum
                    .checked_add(*sum_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats avg sum overflow"))?;
                *count = count
                    .checked_add(*count_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats avg count overflow"))?;
                if *count < 0 {
                    bail!("grouped-stats avg count became negative");
                }
            }
            (
                AggregateKind::Min | AggregateKind::Max,
                AggregateDelta::MinMaxI64 { value_deltas },
                CompactAggregateState::MinMaxI64(value),
            ) => {
                let old = *value;
                let mut updated_counts = HashMap::with_capacity(value_deltas.len());
                for (delta_value, value_delta) in value_deltas {
                    let old_count = columnar
                        .stats_state
                        .load_value_count(group_key, idx, *delta_value)
                        .await?;
                    let new_count = old_count.checked_add(*value_delta).ok_or_else(|| {
                        anyhow::anyhow!("grouped-stats min/max value count overflow")
                    })?;
                    if new_count < 0 {
                        bail!("grouped-stats min/max removed more values than were present");
                    }
                    updated_counts.insert(*delta_value, new_count);
                    columnar.stats_state.write_value_count(
                        writes,
                        group_key,
                        idx,
                        *delta_value,
                        new_count,
                    )?;
                }
                *value = columnar
                    .stats_state
                    .new_minmax_after_delta(group_key, idx, spec.kind, old, &updated_counts)
                    .await?;
            }
            (AggregateKind::DistinctCount, _, _) => {
                bail!("grouped-stats distinct count is not compactable")
            }
            _ => bail!("grouped-stats compact aggregate state kind mismatch"),
        }
    }
    Ok(())
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

async fn compact_state_from_current(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    group_key: &[u8],
    row_count: i64,
) -> Result<CompactGroupState> {
    let specs = columnar
        .stats_state
        .compact_specs
        .as_ref()
        .context("grouped-stats compact state requested for non-compact plan")?;
    let mut aggregates = Vec::with_capacity(specs.len());
    for (idx, spec) in specs.iter().enumerate() {
        aggregates.push(match spec.kind {
            AggregateKind::Count => {
                CompactAggregateState::I64(columnar.stats_state.load_i64(group_key, idx).await?)
            }
            AggregateKind::Sum => {
                if matches!(spec.value_type, Some(AggregateValueType::Decimal128)) {
                    bail!("grouped-stats decimal sum is not compactable");
                }
                CompactAggregateState::I64(columnar.stats_state.load_i64(group_key, idx).await?)
            }
            AggregateKind::Avg => {
                let (sum, count) = columnar.stats_state.load_pair(group_key, idx).await?;
                CompactAggregateState::Pair { sum, count }
            }
            AggregateKind::Min | AggregateKind::Max => match spec.value_type {
                Some(
                    AggregateValueType::Int64
                    | AggregateValueType::TimestampMillis
                    | AggregateValueType::DateDays,
                ) => CompactAggregateState::MinMaxI64(
                    columnar.stats_state.load_minmax(group_key, idx).await?,
                ),
                _ => bail!("grouped-stats aggregate is not compactable"),
            },
            AggregateKind::DistinctCount => {
                bail!("grouped-stats distinct count is not compactable")
            }
        });
    }
    Ok(CompactGroupState {
        row_count,
        aggregates,
    })
}

async fn apply_aggregate_deltas(
    columnar: &ColumnarGroupedStatsMaterializedViewState,
    group_key: &[u8],
    deltas: &[AggregateDelta],
    writes: &mut WriteBatch,
) -> Result<Vec<AggregateValue>> {
    let compact_enabled = columnar.stats_state.compact_enabled();
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
                if compact_enabled {
                    columnar.stats_state.cache_i64(group_key, idx, new)?;
                } else {
                    columnar
                        .stats_state
                        .write_i64(writes, group_key, idx, new)?;
                }
                AggregateValue::Int64(new)
            }
            (AggregateKind::DistinctCount, AggregateDelta::DistinctCountI64 { value_deltas }) => {
                let old = columnar.stats_state.load_i64(group_key, idx).await?;
                let mut new = old;
                if columnar.append_only_input {
                    let mut values = Vec::with_capacity(value_deltas.len());
                    for (value, value_delta) in value_deltas {
                        if *value_delta < 0 {
                            bail!(
                                "append-only grouped-stats distinct count received a negative delta"
                            );
                        }
                        if *value_delta > 0 {
                            values.push(*value);
                        }
                    }
                    let added = columnar
                        .stats_state
                        .write_append_only_value_presences(writes, group_key, idx, values)
                        .await?;
                    new = new
                        .checked_add(added)
                        .ok_or_else(|| anyhow::anyhow!("grouped-stats distinct overflow"))?;
                } else {
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
                            new = new.checked_add(1).ok_or_else(|| {
                                anyhow::anyhow!("grouped-stats distinct overflow")
                            })?;
                        } else if old_count > 0 && new_count == 0 {
                            new = new.checked_sub(1).ok_or_else(|| {
                                anyhow::anyhow!("grouped-stats distinct underflow")
                            })?;
                        }
                        columnar.stats_state.write_value_count(
                            writes, group_key, idx, *value, new_count,
                        )?;
                    }
                }
                if new < 0 {
                    bail!("grouped-stats distinct count became negative");
                }
                if new != old {
                    columnar
                        .stats_state
                        .write_i64(writes, group_key, idx, new)?;
                }
                AggregateValue::Int64(new)
            }
            (AggregateKind::DistinctCount, AggregateDelta::DistinctCountUtf8 { value_deltas }) => {
                let old = columnar.stats_state.load_i64(group_key, idx).await?;
                let mut new = old;
                if columnar.append_only_input {
                    let mut values = Vec::with_capacity(value_deltas.len());
                    for (value, value_delta) in value_deltas {
                        if *value_delta < 0 {
                            bail!(
                                "append-only grouped-stats string distinct count received a negative delta"
                            );
                        }
                        if *value_delta > 0 {
                            values.push(value.as_str());
                        }
                    }
                    let added = columnar
                        .stats_state
                        .write_append_only_string_value_presences(writes, group_key, idx, values)
                        .await?;
                    new = new.checked_add(added).ok_or_else(|| {
                        anyhow::anyhow!("grouped-stats string distinct overflow")
                    })?;
                } else {
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
                        columnar.stats_state.write_string_value_count(
                            writes, group_key, idx, value, new_count,
                        )?;
                    }
                }
                if new < 0 {
                    bail!("grouped-stats string distinct count became negative");
                }
                if new != old {
                    columnar
                        .stats_state
                        .write_i64(writes, group_key, idx, new)?;
                }
                AggregateValue::Int64(new)
            }
            (AggregateKind::DistinctCount, AggregateDelta::DistinctCountI128 { value_deltas }) => {
                let old = columnar.stats_state.load_i64(group_key, idx).await?;
                let mut new = old;
                if columnar.append_only_input {
                    let mut values = Vec::with_capacity(value_deltas.len());
                    for (value, value_delta) in value_deltas {
                        if *value_delta < 0 {
                            bail!(
                                "append-only grouped-stats decimal distinct count received a negative delta"
                            );
                        }
                        if *value_delta > 0 {
                            values.push(*value);
                        }
                    }
                    let added = columnar
                        .stats_state
                        .write_append_only_i128_value_presences(writes, group_key, idx, values)
                        .await?;
                    new = new.checked_add(added).ok_or_else(|| {
                        anyhow::anyhow!("grouped-stats decimal distinct overflow")
                    })?;
                } else {
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
                        columnar.stats_state.write_i128_value_count(
                            writes, group_key, idx, *value, new_count,
                        )?;
                    }
                }
                if new < 0 {
                    bail!("grouped-stats decimal distinct count became negative");
                }
                if new != old {
                    columnar
                        .stats_state
                        .write_i64(writes, group_key, idx, new)?;
                }
                AggregateValue::Int64(new)
            }
            (AggregateKind::Sum, AggregateDelta::Sum { sum_delta }) => {
                let old = columnar.stats_state.load_i64(group_key, idx).await?;
                let new = old
                    .checked_add(*sum_delta)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats sum overflow"))?;
                if compact_enabled {
                    columnar.stats_state.cache_i64(group_key, idx, new)?;
                } else {
                    columnar
                        .stats_state
                        .write_i64(writes, group_key, idx, new)?;
                }
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
                if compact_enabled {
                    columnar
                        .stats_state
                        .cache_pair(group_key, idx, new_sum, new_count)?;
                } else {
                    columnar
                        .stats_state
                        .write_pair(writes, group_key, idx, new_sum, new_count)?;
                }
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
                if columnar.append_only_input {
                    let mut new = old;
                    for (value, value_delta) in value_deltas {
                        if *value_delta < 0 {
                            bail!("append-only grouped-stats min/max received a negative delta");
                        }
                        if *value_delta > 0 {
                            new = Some(match new {
                                Some(current) => minmax_value(spec.kind, current, *value),
                                None => *value,
                            });
                        }
                    }
                    if compact_enabled {
                        columnar.stats_state.cache_minmax(group_key, idx, new)?;
                    } else {
                        columnar
                            .stats_state
                            .write_minmax(writes, group_key, idx, new)?;
                    }
                    return_value_from_i64_minmax(spec, new)?
                } else {
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
                if compact_enabled {
                    columnar.stats_state.cache_minmax(group_key, idx, new)?;
                } else {
                    columnar
                        .stats_state
                        .write_minmax(writes, group_key, idx, new)?;
                }
                    return_value_from_i64_minmax(spec, new)?
                }
            }
            (
                AggregateKind::Min | AggregateKind::Max,
                AggregateDelta::MinMaxUtf8 { value_deltas },
            ) => {
                let old = columnar
                    .stats_state
                    .load_string_minmax(group_key, idx)
                    .await?;
                if columnar.append_only_input {
                    let mut new = old;
                    for (value, value_delta) in value_deltas {
                        if *value_delta < 0 {
                            bail!(
                                "append-only grouped-stats string min/max received a negative delta"
                            );
                        }
                        if *value_delta > 0 {
                            new = Some(match new {
                                Some(current) => minmax_string(spec.kind, current, value.clone()),
                                None => value.clone(),
                            });
                        }
                    }
                    columnar
                        .stats_state
                        .write_string_minmax(writes, group_key, idx, new.as_deref())?;
                    new.map(AggregateValue::Utf8).unwrap_or(AggregateValue::Null)
                } else {
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
            }
            (
                AggregateKind::Min | AggregateKind::Max,
                AggregateDelta::MinMaxI128 { value_deltas },
            ) => {
                let old = columnar
                    .stats_state
                    .load_i128_minmax(group_key, idx)
                    .await?;
                if columnar.append_only_input {
                    let mut new = old;
                    for (value, value_delta) in value_deltas {
                        if *value_delta < 0 {
                            bail!(
                                "append-only grouped-stats decimal min/max received a negative delta"
                            );
                        }
                        if *value_delta > 0 {
                            new = Some(match new {
                                Some(current) => minmax_i128_value(spec.kind, current, *value),
                                None => *value,
                            });
                        }
                    }
                    columnar
                        .stats_state
                        .write_i128_minmax(writes, group_key, idx, new)?;
                    new.map(AggregateValue::Decimal128)
                        .unwrap_or(AggregateValue::Null)
                } else {
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

impl CompactAggregateDelta {
    fn for_spec(spec: &AggregateSpec) -> Self {
        match spec.kind {
            AggregateKind::Count => Self::Count { count_delta: 0 },
            AggregateKind::Sum => {
                if matches!(spec.value_type, Some(AggregateValueType::Decimal128)) {
                    Self::Unsupported
                } else {
                    Self::Sum { sum_delta: 0 }
                }
            }
            AggregateKind::Avg => Self::Avg {
                sum_delta: 0,
                count_delta: 0,
            },
            AggregateKind::Min | AggregateKind::Max => match spec.value_type {
                Some(
                    AggregateValueType::Int64
                    | AggregateValueType::TimestampMillis
                    | AggregateValueType::DateDays,
                ) => Self::MinMaxI64 { value: None },
                _ => Self::Unsupported,
            },
            AggregateKind::DistinctCount => Self::Unsupported,
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
    fn new(
        table: Arc<dyn KeyValueTable>,
        namespace: &str,
        assume_empty: bool,
        compact_specs: Option<Vec<AggregateSpec>>,
    ) -> Self {
        let key_prefix = keyspace::namespace_prefix(keyspace::prefix::INDEX, namespace);
        let mut compact_snapshot_key = key_prefix.clone();
        compact_snapshot_key.extend_from_slice(b"compact_snapshot/current");
        Self {
            table,
            key_prefix,
            compact_snapshot_key,
            assume_empty,
            compact_specs,
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
            append_only_value_presences: Mutex::new(HashMap::new()),
            append_only_i128_value_presences: Mutex::new(HashMap::new()),
            append_only_string_value_presences: Mutex::new(HashMap::new()),
            compact_values: Mutex::new(CompactGroupStateMap::new()),
            compact_snapshot_loaded: Mutex::new(false),
            compact_snapshot_active: Mutex::new(false),
        }
    }

    fn compact_enabled(&self) -> bool {
        self.compact_specs.is_some()
    }

    async fn load_compact_snapshot_if_needed(&self) -> Result<bool> {
        let Some(specs) = self.compact_specs.as_ref() else {
            return Ok(false);
        };
        if *self
            .compact_snapshot_loaded
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats compact snapshot loaded flag poisoned"))?
        {
            return self.compact_snapshot_active();
        }
        if self.assume_empty {
            *self.compact_snapshot_loaded.lock().map_err(|_| {
                anyhow::anyhow!("grouped-stats compact snapshot loaded flag poisoned")
            })? = true;
            return Ok(false);
        }

        let snapshot = self
            .table
            .get_bytes(&self.compact_snapshot_key)
            .await
            .context("read grouped-stats compact state snapshot")?;
        let active = if let Some(bytes) = snapshot {
            let values = decode_compact_group_snapshot(specs, bytes.as_ref())?;
            *self
                .compact_values
                .lock()
                .map_err(|_| anyhow::anyhow!("grouped-stats compact state cache poisoned"))? =
                values;
            true
        } else {
            false
        };
        *self.compact_snapshot_loaded.lock().map_err(|_| {
            anyhow::anyhow!("grouped-stats compact snapshot loaded flag poisoned")
        })? = true;
        *self.compact_snapshot_active.lock().map_err(|_| {
            anyhow::anyhow!("grouped-stats compact snapshot active flag poisoned")
        })? = active;
        Ok(active)
    }

    fn compact_snapshot_active(&self) -> Result<bool> {
        self.compact_snapshot_active
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats compact snapshot active flag poisoned"))
            .map(|active| *active)
    }

    fn should_write_compact_snapshot(&self, dirty_groups: usize) -> Result<bool> {
        if self.compact_specs.is_none() {
            return Ok(false);
        }
        if self.compact_snapshot_active()? {
            return Ok(true);
        }
        if !self.assume_empty {
            return Ok(false);
        }
        if dirty_groups < COMPACT_SNAPSHOT_DENSE_WRITE_MIN_GROUPS {
            return Ok(false);
        }
        let cached_groups = self
            .compact_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats compact state cache poisoned"))?
            .len();
        let state_groups = cached_groups.max(dirty_groups);
        Ok(dirty_groups.saturating_mul(2) >= state_groups)
    }

    async fn load_compact_state(&self, group_key: &[u8]) -> Result<Option<CompactGroupState>> {
        if let Some(value) = self
            .compact_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats compact state cache poisoned"))?
            .get(group_key)
            .cloned()
        {
            return Ok(Some(value));
        }
        let Some(specs) = self.compact_specs.as_ref() else {
            return Ok(None);
        };
        let snapshot_active = self.load_compact_snapshot_if_needed().await?;
        if let Some(value) = self
            .compact_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats compact state cache poisoned"))?
            .get(group_key)
            .cloned()
        {
            return Ok(Some(value));
        }
        if snapshot_active {
            return Ok(None);
        }
        if self.assume_empty {
            return Ok(None);
        }
        let Some(bytes) = self
            .table
            .get_bytes(&self.compact_key(group_key)?)
            .await
            .context("read grouped-stats compact state")?
        else {
            return Ok(None);
        };
        let value = decode_compact_group_state(specs, bytes.as_ref())?;
        self.compact_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats compact state cache poisoned"))?
            .insert(group_key.to_vec(), value.clone());
        Ok(Some(value))
    }

    fn has_compact_state(&self, group_key: &[u8]) -> Result<bool> {
        self.compact_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats compact state cache poisoned"))
            .map(|values| values.contains_key(group_key))
    }

    fn mutate_compact_state<R>(
        &self,
        group_key: &[u8],
        initial_state: impl FnOnce() -> Result<CompactGroupState>,
        update: impl FnOnce(&mut CompactGroupState) -> Result<R>,
    ) -> Result<R> {
        let mut values = self
            .compact_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats compact state cache poisoned"))?;
        if !values.contains_key(group_key) {
            values.insert(group_key.to_vec(), initial_state()?);
        }
        let (result, remove_after_update) = {
            let state = values
                .get_mut(group_key)
                .ok_or_else(|| anyhow::anyhow!("grouped-stats compact state cache missing"))?;
            let result = update(state)?;
            (result, state.row_count == 0)
        };
        if remove_after_update {
            values.remove(group_key);
        }
        Ok(result)
    }

    fn write_compact_snapshot(&self, batch: &mut WriteBatch) -> Result<()> {
        let snapshot_bytes = {
            let values = self
                .compact_values
                .lock()
                .map_err(|_| anyhow::anyhow!("grouped-stats compact state cache poisoned"))?;
            encode_compact_group_snapshot(&values)?
        };
        batch.put_bytes(
            Bytes::from(self.compact_snapshot_key.clone()),
            Bytes::from(snapshot_bytes),
        );
        *self.compact_snapshot_loaded.lock().map_err(|_| {
            anyhow::anyhow!("grouped-stats compact snapshot loaded flag poisoned")
        })? = true;
        *self.compact_snapshot_active.lock().map_err(|_| {
            anyhow::anyhow!("grouped-stats compact snapshot active flag poisoned")
        })? = true;
        Ok(())
    }

    fn write_compact_state(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        state: CompactGroupState,
    ) -> Result<()> {
        self.write_compact_state_to_batch(batch, group_key, &state)?;
        if state.row_count == 0 {
            self.compact_values
                .lock()
                .map_err(|_| anyhow::anyhow!("grouped-stats compact state cache poisoned"))?
                .remove(group_key);
        } else {
            self.compact_values
                .lock()
                .map_err(|_| anyhow::anyhow!("grouped-stats compact state cache poisoned"))?
                .insert(group_key.to_vec(), state);
        }
        Ok(())
    }

    fn write_compact_state_to_batch(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        state: &CompactGroupState,
    ) -> Result<()> {
        let key = self.compact_key(group_key)?;
        if state.row_count == 0 {
            batch.delete(key);
            self.delete_legacy_compacted_state_keys(batch, group_key)?;
        } else {
            batch.put_bytes(
                Bytes::from(key),
                Bytes::from(encode_compact_group_state(state)?),
            );
        }
        Ok(())
    }

    fn delete_legacy_compacted_state_keys(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
    ) -> Result<()> {
        batch.delete(self.group_key(GROUP_TAG, group_key)?);
        let Some(specs) = self.compact_specs.as_ref() else {
            return Ok(());
        };
        for (idx, spec) in specs.iter().enumerate() {
            match spec.kind {
                AggregateKind::Count | AggregateKind::Sum | AggregateKind::Avg => {
                    batch.delete(self.aggregate_key(SCALAR_TAG, group_key, idx)?);
                }
                AggregateKind::Min | AggregateKind::Max => {
                    batch.delete(self.aggregate_key(MINMAX_TAG, group_key, idx)?);
                }
                AggregateKind::DistinctCount => {}
            }
        }
        Ok(())
    }

    fn cache_group_count(&self, group_key: &[u8], count: i64) -> Result<()> {
        self.group_counts
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats group count cache poisoned"))?
            .insert(group_key.to_vec(), count);
        Ok(())
    }

    fn cache_i64(&self, group_key: &[u8], agg_idx: usize, value: i64) -> Result<()> {
        self.i64_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats i64 cache poisoned"))?
            .insert((group_key.to_vec(), agg_idx), value);
        Ok(())
    }

    fn cache_pair(&self, group_key: &[u8], agg_idx: usize, sum: i64, count: i64) -> Result<()> {
        self.pairs
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats pair cache poisoned"))?
            .insert((group_key.to_vec(), agg_idx), (sum, count));
        Ok(())
    }

    fn cache_minmax(&self, group_key: &[u8], agg_idx: usize, value: Option<i64>) -> Result<()> {
        self.minmax_values
            .lock()
            .map_err(|_| anyhow::anyhow!("grouped-stats min/max cache poisoned"))?
            .insert((group_key.to_vec(), agg_idx), value);
        Ok(())
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
        if let Some(state) = self.load_compact_state(group_key).await? {
            self.cache_group_count(group_key, state.row_count)?;
            return Ok(state.row_count);
        }
        if self.assume_empty || self.compact_snapshot_active()? {
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
        if let Some(state) = self.load_compact_state(group_key).await?
            && let Some(CompactAggregateState::I64(value)) = state.aggregates.get(agg_idx)
        {
            self.cache_i64(group_key, agg_idx, *value)?;
            return Ok(*value);
        }
        if self.assume_empty || self.compact_snapshot_active()? {
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
        if let Some(state) = self.load_compact_state(group_key).await?
            && let Some(CompactAggregateState::Pair { sum, count }) = state.aggregates.get(agg_idx)
        {
            self.cache_pair(group_key, agg_idx, *sum, *count)?;
            return Ok((*sum, *count));
        }
        if self.assume_empty || self.compact_snapshot_active()? {
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
            batch.put_bytes(Bytes::from(key), Bytes::from(bytes));
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
        if let Some(state) = self.load_compact_state(group_key).await?
            && let Some(CompactAggregateState::MinMaxI64(value)) = state.aggregates.get(agg_idx)
        {
            self.cache_minmax(group_key, agg_idx, *value)?;
            return Ok(*value);
        }
        if self.assume_empty || self.compact_snapshot_active()? {
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
            batch.put_bytes(
                Bytes::from(key),
                Bytes::copy_from_slice(&value.to_be_bytes()),
            );
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
            batch.put_bytes(
                Bytes::from(key),
                Bytes::copy_from_slice(&value.to_be_bytes()),
            );
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
            batch.put_bytes(Bytes::from(key), Bytes::copy_from_slice(value.as_bytes()));
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

    async fn write_append_only_value_presences<I>(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        values: I,
    ) -> Result<i64>
    where
        I: IntoIterator<Item = i64>,
    {
        let cache_key = (group_key.to_vec(), agg_idx);
        let needs_load = !self.assume_empty
            && !self
                .append_only_value_presences
                .lock()
                .map_err(|_| anyhow::anyhow!("grouped-stats append-only value cache poisoned"))?
                .contains_key(&cache_key);
        let loaded = if needs_load {
            Some(
                self.load_append_only_value_presence_state(group_key, agg_idx)
                    .await?,
            )
        } else {
            None
        };
        let mut new_values = Vec::new();
        let segment_id = {
            let mut presences = self
                .append_only_value_presences
                .lock()
                .map_err(|_| anyhow::anyhow!("grouped-stats append-only value cache poisoned"))?;
            let state = presences.entry(cache_key).or_insert_with(|| {
                loaded.unwrap_or_else(|| AppendOnlyDistinctPresenceState {
                    values: HashSet::new(),
                    next_segment_id: 0,
                })
            });
            let segment_id = state.next_segment_id;
            for value in values {
                if state.values.insert(value) {
                    new_values.push(value);
                }
            }
            if !new_values.is_empty() {
                state.next_segment_id = state.next_segment_id.checked_add(1).ok_or_else(|| {
                    anyhow::anyhow!("grouped-stats append-only distinct segment id overflow")
                })?;
            }
            segment_id
        };
        if new_values.is_empty() {
            return Ok(0);
        }
        new_values.sort_unstable();
        let added =
            i64::try_from(new_values.len()).context("grouped-stats distinct count exceeds i64")?;
        self.write_append_only_distinct_segment(
            batch,
            group_key,
            agg_idx,
            segment_id,
            encode_append_only_i64_distinct_segment(&new_values)?,
        )?;
        Ok(added)
    }

    async fn load_append_only_value_presence_state(
        &self,
        group_key: &[u8],
        agg_idx: usize,
    ) -> Result<AppendOnlyDistinctPresenceState<i64>> {
        let mut values = HashSet::new();
        let mut next_segment_id = 0_u64;
        let segment_prefix = self.append_only_distinct_segment_prefix(group_key, agg_idx)?;
        for (key, bytes) in self
            .table
            .scan_prefix(&segment_prefix, &ScanOptions::default())
            .await
            .context("scan grouped-stats append-only distinct value segments")?
        {
            let segment_id = decode_append_only_distinct_segment_id(&segment_prefix, &key)?;
            next_segment_id = next_segment_id.max(segment_id.saturating_add(1));
            for value in decode_append_only_i64_distinct_segment(&bytes)? {
                values.insert(value);
            }
        }
        let value_prefix = self.value_key_prefix(group_key, agg_idx)?;
        for (key, value_bytes) in self
            .table
            .scan_prefix(&value_prefix, &ScanOptions::default())
            .await
            .context("scan grouped-stats legacy append-only distinct value state")?
        {
            if decode_i64(&value_bytes)? <= 0 {
                continue;
            }
            let value = decode_i64_sortable(
                key.get(value_prefix.len()..)
                    .ok_or_else(|| anyhow::anyhow!("invalid grouped-stats value key"))?,
            )?;
            values.insert(value);
        }
        Ok(AppendOnlyDistinctPresenceState {
            values,
            next_segment_id,
        })
    }

    async fn write_append_only_i128_value_presences<I>(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        values: I,
    ) -> Result<i64>
    where
        I: IntoIterator<Item = i128>,
    {
        let cache_key = (group_key.to_vec(), agg_idx);
        let needs_load = !self.assume_empty
            && !self
                .append_only_i128_value_presences
                .lock()
                .map_err(|_| {
                    anyhow::anyhow!("grouped-stats append-only i128 value cache poisoned")
                })?
                .contains_key(&cache_key);
        let loaded = if needs_load {
            Some(
                self.load_append_only_i128_value_presence_state(group_key, agg_idx)
                    .await?,
            )
        } else {
            None
        };
        let mut new_values = Vec::new();
        let segment_id = {
            let mut presences = self.append_only_i128_value_presences.lock().map_err(|_| {
                anyhow::anyhow!("grouped-stats append-only i128 value cache poisoned")
            })?;
            let state = presences.entry(cache_key).or_insert_with(|| {
                loaded.unwrap_or_else(|| AppendOnlyDistinctPresenceState {
                    values: HashSet::new(),
                    next_segment_id: 0,
                })
            });
            let segment_id = state.next_segment_id;
            for value in values {
                if state.values.insert(value) {
                    new_values.push(value);
                }
            }
            if !new_values.is_empty() {
                state.next_segment_id = state.next_segment_id.checked_add(1).ok_or_else(|| {
                    anyhow::anyhow!("grouped-stats append-only distinct segment id overflow")
                })?;
            }
            segment_id
        };
        if new_values.is_empty() {
            return Ok(0);
        }
        new_values.sort_unstable();
        let added = i64::try_from(new_values.len())
            .context("grouped-stats decimal distinct count exceeds i64")?;
        self.write_append_only_distinct_segment(
            batch,
            group_key,
            agg_idx,
            segment_id,
            encode_append_only_i128_distinct_segment(&new_values)?,
        )?;
        Ok(added)
    }

    async fn load_append_only_i128_value_presence_state(
        &self,
        group_key: &[u8],
        agg_idx: usize,
    ) -> Result<AppendOnlyDistinctPresenceState<i128>> {
        let mut values = HashSet::new();
        let mut next_segment_id = 0_u64;
        let segment_prefix = self.append_only_distinct_segment_prefix(group_key, agg_idx)?;
        for (key, bytes) in self
            .table
            .scan_prefix(&segment_prefix, &ScanOptions::default())
            .await
            .context("scan grouped-stats append-only decimal distinct value segments")?
        {
            let segment_id = decode_append_only_distinct_segment_id(&segment_prefix, &key)?;
            next_segment_id = next_segment_id.max(segment_id.saturating_add(1));
            for value in decode_append_only_i128_distinct_segment(&bytes)? {
                values.insert(value);
            }
        }
        let value_prefix = self.value_key_prefix(group_key, agg_idx)?;
        for (key, value_bytes) in self
            .table
            .scan_prefix(&value_prefix, &ScanOptions::default())
            .await
            .context("scan grouped-stats legacy append-only decimal distinct value state")?
        {
            if decode_i64(&value_bytes)? <= 0 {
                continue;
            }
            let value = decode_i128_sortable(
                key.get(value_prefix.len()..)
                    .ok_or_else(|| anyhow::anyhow!("invalid grouped-stats i128 value key"))?,
            )?;
            values.insert(value);
        }
        Ok(AppendOnlyDistinctPresenceState {
            values,
            next_segment_id,
        })
    }

    async fn write_append_only_string_value_presences<'a, I>(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        values: I,
    ) -> Result<i64>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let cache_key = (group_key.to_vec(), agg_idx);
        let needs_load = !self.assume_empty
            && !self
                .append_only_string_value_presences
                .lock()
                .map_err(|_| {
                    anyhow::anyhow!("grouped-stats append-only string value cache poisoned")
                })?
                .contains_key(&cache_key);
        let loaded = if needs_load {
            Some(
                self.load_append_only_string_value_presence_state(group_key, agg_idx)
                    .await?,
            )
        } else {
            None
        };
        let mut new_values = Vec::new();
        let segment_id = {
            let mut presences = self
                .append_only_string_value_presences
                .lock()
                .map_err(|_| {
                    anyhow::anyhow!("grouped-stats append-only string value cache poisoned")
                })?;
            let state = presences.entry(cache_key).or_insert_with(|| {
                loaded.unwrap_or_else(|| AppendOnlyDistinctPresenceState {
                    values: HashSet::new(),
                    next_segment_id: 0,
                })
            });
            let segment_id = state.next_segment_id;
            for value in values {
                if state.values.insert(value.to_string()) {
                    new_values.push(value.to_string());
                }
            }
            if !new_values.is_empty() {
                state.next_segment_id = state.next_segment_id.checked_add(1).ok_or_else(|| {
                    anyhow::anyhow!("grouped-stats append-only distinct segment id overflow")
                })?;
            }
            segment_id
        };
        if new_values.is_empty() {
            return Ok(0);
        }
        new_values.sort_unstable();
        let added = i64::try_from(new_values.len())
            .context("grouped-stats string distinct count exceeds i64")?;
        self.write_append_only_distinct_segment(
            batch,
            group_key,
            agg_idx,
            segment_id,
            encode_append_only_string_distinct_segment(&new_values)?,
        )?;
        Ok(added)
    }

    async fn load_append_only_string_value_presence_state(
        &self,
        group_key: &[u8],
        agg_idx: usize,
    ) -> Result<AppendOnlyDistinctPresenceState<String>> {
        let mut values = HashSet::new();
        let mut next_segment_id = 0_u64;
        let segment_prefix = self.append_only_distinct_segment_prefix(group_key, agg_idx)?;
        for (key, bytes) in self
            .table
            .scan_prefix(&segment_prefix, &ScanOptions::default())
            .await
            .context("scan grouped-stats append-only string distinct value segments")?
        {
            let segment_id = decode_append_only_distinct_segment_id(&segment_prefix, &key)?;
            next_segment_id = next_segment_id.max(segment_id.saturating_add(1));
            for value in decode_append_only_string_distinct_segment(&bytes)? {
                values.insert(value);
            }
        }
        let value_prefix = self.value_key_prefix(group_key, agg_idx)?;
        for (key, value_bytes) in self
            .table
            .scan_prefix(&value_prefix, &ScanOptions::default())
            .await
            .context("scan grouped-stats legacy append-only string distinct value state")?
        {
            if decode_i64(&value_bytes)? <= 0 {
                continue;
            }
            let value = String::from_utf8(
                key.get(value_prefix.len()..)
                    .ok_or_else(|| anyhow::anyhow!("invalid grouped-stats string value key"))?
                    .to_vec(),
            )
            .context("decode grouped-stats string value key")?;
            values.insert(value);
        }
        Ok(AppendOnlyDistinctPresenceState {
            values,
            next_segment_id,
        })
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
        let mut out = minmax_added_i64(kind, updated_counts);
        let value_prefix = self.value_key_prefix(group_key, agg_idx)?;
        if kind == AggregateKind::Min {
            let mut visit = |key: &[u8], value_bytes: &[u8]| -> Result<bool> {
                let value = decode_i64_sortable(
                    key.get(value_prefix.len()..)
                        .ok_or_else(|| anyhow::anyhow!("invalid grouped-stats value key"))?,
                )?;
                let old_count = decode_i64(value_bytes)?;
                let count = updated_counts.get(&value).copied().unwrap_or(old_count);
                if count > 0 {
                    out = Some(match out {
                        Some(current) => minmax_value(kind, current, value),
                        None => value,
                    });
                    return Ok(false);
                }
                Ok(true)
            };
            self.table
                .scan_range_bytes_until(
                    prefix_bounds(&value_prefix),
                    &ScanOptions::default(),
                    &mut visit,
                )
                .await
                .context("scan grouped-stats min value state")?;
            return Ok(out);
        }
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
            batch.put_bytes(
                Bytes::from(key),
                Bytes::copy_from_slice(&value.to_be_bytes()),
            );
        }
    }

    fn write_key_i128(&self, batch: &mut WriteBatch, key: Vec<u8>, value: i128) {
        if value == 0 {
            batch.delete(key);
        } else {
            batch.put_bytes(
                Bytes::from(key),
                Bytes::copy_from_slice(&value.to_be_bytes()),
            );
        }
    }

    fn write_append_only_distinct_segment(
        &self,
        batch: &mut WriteBatch,
        group_key: &[u8],
        agg_idx: usize,
        segment_id: u64,
        bytes: Vec<u8>,
    ) -> Result<()> {
        batch.put_bytes(
            Bytes::from(self.append_only_distinct_segment_key(group_key, agg_idx, segment_id)?),
            Bytes::from(bytes),
        );
        Ok(())
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

    fn append_only_distinct_segment_prefix(
        &self,
        group_key: &[u8],
        agg_idx: usize,
    ) -> Result<Vec<u8>> {
        self.aggregate_key(APPEND_ONLY_DISTINCT_SEGMENT_TAG, group_key, agg_idx)
    }

    fn append_only_distinct_segment_key(
        &self,
        group_key: &[u8],
        agg_idx: usize,
        segment_id: u64,
    ) -> Result<Vec<u8>> {
        let mut key = self.append_only_distinct_segment_prefix(group_key, agg_idx)?;
        key.extend_from_slice(&segment_id.to_be_bytes());
        Ok(key)
    }

    fn aggregate_key(&self, tag: u8, group_key: &[u8], agg_idx: usize) -> Result<Vec<u8>> {
        let agg_idx =
            u16::try_from(agg_idx).context("grouped-stats aggregate index exceeds u16")?;
        let mut key = self.group_key(tag, group_key)?;
        key.extend_from_slice(&agg_idx.to_be_bytes());
        Ok(key)
    }

    fn compact_key(&self, group_key: &[u8]) -> Result<Vec<u8>> {
        self.group_key(COMPACT_TAG, group_key)
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
    fn with_capacity(schema: SchemaRef, group_count: usize, capacity: usize) -> Result<Self> {
        let builders = schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), capacity))
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

    fn append_compact_state(
        &mut self,
        projection_batch: &RecordBatch,
        row_idx: usize,
        specs: &[AggregateSpec],
        state: &CompactGroupState,
    ) -> Result<()> {
        for source_idx in 0..self.schema.fields().len() {
            if source_idx < self.group_count {
                self.builders[source_idx]
                    .append_array_value(projection_batch.column(source_idx).as_ref(), row_idx)?;
            } else {
                let aggregate_idx = source_idx - self.group_count;
                append_compact_aggregate_state_value(
                    &mut self.builders[source_idx],
                    specs.get(aggregate_idx).ok_or_else(|| {
                        anyhow::anyhow!("grouped-stats aggregate row mapping out of bounds")
                    })?,
                    state.aggregates.get(aggregate_idx).ok_or_else(|| {
                        anyhow::anyhow!("grouped-stats compact aggregate row mapping out of bounds")
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
    fn with_capacity(schema: SchemaRef, output_mapping: &[usize], capacity: usize) -> Result<Self> {
        let builders = schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), capacity))
            .collect::<Result<Vec<_>>>()?;
        let weighted_schema = weighted_snapshot_schema(&schema)?;
        Ok(Self {
            weighted_schema,
            output_mapping: output_mapping.to_vec(),
            builders,
            weights: Int64Builder::with_capacity(capacity),
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
                let value = aggregate_values
                    .get(aggregate_idx)
                    .ok_or_else(|| anyhow::anyhow!("grouped-stats output mapping out of bounds"))?;
                append_aggregate_value(&mut self.builders[output_idx], value)?;
            }
        }
        self.weights.append_value(weight);
        self.rows = self.rows.saturating_add(1);
        Ok(())
    }

    fn append_compact_state(
        &mut self,
        projection_batch: &RecordBatch,
        row_idx: usize,
        group_count: usize,
        specs: &[AggregateSpec],
        state: &CompactGroupState,
        weight: i64,
    ) -> Result<()> {
        for (output_idx, source_idx) in self.output_mapping.iter().copied().enumerate() {
            if source_idx < group_count {
                self.builders[output_idx]
                    .append_array_value(projection_batch.column(source_idx).as_ref(), row_idx)?;
            } else {
                let aggregate_idx = source_idx - group_count;
                append_compact_aggregate_state_value(
                    &mut self.builders[output_idx],
                    specs.get(aggregate_idx).ok_or_else(|| {
                        anyhow::anyhow!("grouped-stats output mapping out of bounds")
                    })?,
                    state.aggregates.get(aggregate_idx).ok_or_else(|| {
                        anyhow::anyhow!("grouped-stats compact output mapping out of bounds")
                    })?,
                )?;
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

fn append_compact_aggregate_state_value(
    builder: &mut ScalarColumnBuilder,
    spec: &AggregateSpec,
    state: &CompactAggregateState,
) -> Result<()> {
    match (spec.kind, state) {
        (AggregateKind::Count | AggregateKind::Sum, CompactAggregateState::I64(value)) => {
            builder.append_i64_value(*value)
        }
        (AggregateKind::Avg, CompactAggregateState::Pair { sum, count }) => {
            if *count == 0 {
                builder.append_encoded_scalar(None)
            } else {
                builder.append_f64_value(*sum as f64 / *count as f64)
            }
        }
        (
            AggregateKind::Min | AggregateKind::Max,
            CompactAggregateState::MinMaxI64(Some(value)),
        ) => match spec.value_type {
            Some(AggregateValueType::TimestampMillis) => {
                builder.append_timestamp_millis_value(*value)
            }
            Some(AggregateValueType::DateDays) => {
                let value = i32::try_from(*value)
                    .context("grouped-stats Date32 min/max value out of range")?;
                builder.append_encoded_scalar(Some(&EncodedRowScalar::DateDays(value)))
            }
            Some(AggregateValueType::Int64 | AggregateValueType::Any) | None => {
                builder.append_i64_value(*value)
            }
            Some(
                AggregateValueType::Utf8
                | AggregateValueType::Bool
                | AggregateValueType::Decimal128,
            ) => bail!("grouped-stats non-numeric min/max cannot be appended from compact state"),
        },
        (AggregateKind::Min | AggregateKind::Max, CompactAggregateState::MinMaxI64(None)) => {
            builder.append_encoded_scalar(None)
        }
        _ => bail!("grouped-stats compact aggregate state kind mismatch"),
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

fn grouped_stats_top1_value_grouped_max_input_for_aggregate(
    aggregate: &Aggregate,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<(ColumnarGroupedMaxPlan, SchemaRef)>> {
    // AVG(x) over ROW_NUMBER() <= 1 ordered by x DESC is AVG(MAX(x) per
    // full row-number partition); secondary tie breakers do not change x.
    let [avg_expr] = aggregate.aggr_expr.as_slice() else {
        return Ok(None);
    };
    let Some(avg_value_expr) = avg_value_expr_for_top1_value_rewrite(avg_expr) else {
        return Ok(None);
    };
    let Some(top1_input) =
        partitioned_join_top1_value_input_for_plan(aggregate.input.as_ref(), sources)?
    else {
        return Ok(None);
    };
    if !column_exprs_match(avg_value_expr, &top1_input.value_expr) {
        return Ok(None);
    }
    if !aggregate.group_expr.iter().all(|expr| {
        column_expr_name(expr)
            .map(|name| {
                top1_input
                    .partition_by
                    .iter()
                    .any(|partition_expr| column_expr_name(partition_expr) == Some(name))
            })
            .unwrap_or(false)
    }) {
        return Ok(None);
    }

    let value_idx = match aggregate_input_column_index(aggregate, avg_value_expr) {
        Some(idx) => idx,
        None => return Ok(None),
    };
    if value_idx != aggregate.group_expr.len() {
        return Ok(None);
    }
    for (expected_idx, group_expr) in aggregate.group_expr.iter().enumerate() {
        if aggregate_input_column_index(aggregate, group_expr) != Some(expected_idx) {
            return Ok(None);
        }
    }

    let grouped_max_value_alias = "__floe_top1_value";
    let grouped_max_aggregate = LogicalPlanBuilder::from(top1_input.input)
        .aggregate(
            top1_input.partition_by.clone(),
            vec![max(top1_input.value_expr.clone()).alias(grouped_max_value_alias)],
        )?
        .build()?;
    let grouped_max_schema = grouped_max_aggregate.schema();
    let mut projected_exprs = Vec::with_capacity(aggregate.group_expr.len() + 1);
    for (group_idx, group_expr) in aggregate.group_expr.iter().enumerate() {
        let Some(group_name) = column_expr_name(group_expr) else {
            return Ok(None);
        };
        let Some(partition_idx) = top1_input
            .partition_by
            .iter()
            .position(|partition_expr| column_expr_name(partition_expr) == Some(group_name))
        else {
            return Ok(None);
        };
        let field = grouped_max_schema.field(partition_idx);
        let output_field = aggregate.input.schema().field(group_idx);
        projected_exprs.push(
            Expr::Column(Column::new_unqualified(field.name().clone()))
                .alias(output_field.name().clone()),
        );
    }
    let value_field = aggregate.input.schema().field(value_idx);
    projected_exprs.push(
        Expr::Column(Column::new_unqualified(grouped_max_value_alias))
            .alias(value_field.name().clone()),
    );

    let synthetic_grouped_max_plan = LogicalPlanBuilder::from(grouped_max_aggregate)
        .project(projected_exprs)?
        .build()?;
    let source_schema = df_schema_to_arrow(synthetic_grouped_max_plan.schema())?;
    let Some(grouped_max) =
        columnar_grouped_max_plan_for_plan(&synthetic_grouped_max_plan, sources, &source_schema)?
    else {
        return Ok(None);
    };
    Ok(Some((grouped_max, source_schema)))
}

fn avg_value_expr_for_top1_value_rewrite(expr: &Expr) -> Option<&Expr> {
    let Expr::AggregateFunction(aggregate) = strip_alias(expr) else {
        return None;
    };
    let params = &aggregate.params;
    if params.distinct
        || !params.order_by.is_empty()
        || params.filter.is_some()
        || params.null_treatment.is_some()
        || !aggregate.func.name().eq_ignore_ascii_case("avg")
    {
        return None;
    }
    let [value_expr] = params.args.as_slice() else {
        return None;
    };
    Some(value_expr)
}

fn aggregate_input_column_index(aggregate: &Aggregate, expr: &Expr) -> Option<usize> {
    let Expr::Column(column) = strip_alias(expr) else {
        return None;
    };
    aggregate.input.schema().index_of_column(column).ok()
}

fn column_exprs_match(left: &Expr, right: &Expr) -> bool {
    strip_alias(left) == strip_alias(right)
        || match (column_expr_name(left), column_expr_name(right)) {
            (Some(left), Some(right)) => left == right,
            _ => false,
        }
}

fn column_expr_name(expr: &Expr) -> Option<&str> {
    match strip_alias(expr) {
        Expr::Column(column) => Some(column.name.as_str()),
        _ => None,
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

fn return_value_from_i64_minmax(
    spec: &AggregateSpec,
    value: Option<i64>,
) -> Result<AggregateValue> {
    value
        .map(|value| aggregate_value_from_ordered_i64(spec.value_type, value))
        .transpose()
        .map(|value| value.unwrap_or(AggregateValue::Null))
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
    let transformed = logical_plan.transform_down(|plan| match plan {
        LogicalPlan::Aggregate(_) if !replaced => {
            replaced = true;
            Ok(Transformed::complete(aggregate_scan.clone()))
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

fn output_mapping_matches_types(
    mapping: &[usize],
    aggregate_schema: &SchemaRef,
    output_schema: &SchemaRef,
) -> bool {
    mapping.len() == output_schema.fields().len()
        && mapping
            .iter()
            .zip(output_schema.fields())
            .all(|(source_idx, output_field)| {
                *source_idx < aggregate_schema.fields().len()
                    && output_field.data_type() == aggregate_schema.field(*source_idx).data_type()
            })
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

fn minmax_added_i64(kind: AggregateKind, updated_counts: &HashMap<i64, i64>) -> Option<i64> {
    updated_counts
        .iter()
        .filter_map(|(value, count)| (*count > 0).then_some(*value))
        .reduce(|left, right| minmax_value(kind, left, right))
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

fn encode_append_only_i64_distinct_segment(values: &[i64]) -> Result<Vec<u8>> {
    let value_count =
        u32::try_from(values.len()).context("append-only distinct segment exceeds u32 values")?;
    let mut bytes = Vec::with_capacity(2 + 4 + values.len() * 8);
    bytes.push(APPEND_ONLY_DISTINCT_SEGMENT_VERSION);
    bytes.push(APPEND_ONLY_DISTINCT_I64_TAG);
    bytes.extend_from_slice(&value_count.to_be_bytes());
    for value in values {
        bytes.extend_from_slice(&value.to_be_bytes());
    }
    Ok(bytes)
}

fn decode_append_only_i64_distinct_segment(bytes: &[u8]) -> Result<Vec<i64>> {
    let mut offset =
        append_only_distinct_segment_values_offset(bytes, APPEND_ONLY_DISTINCT_I64_TAG, 8)?;
    let value_count = append_only_distinct_segment_value_count(bytes)?;
    let mut values = Vec::with_capacity(value_count);
    for _ in 0..value_count {
        values.push(compact_read_i64(bytes, &mut offset)?);
    }
    if offset != bytes.len() {
        bail!("append-only i64 distinct segment has trailing bytes");
    }
    Ok(values)
}

fn encode_append_only_i128_distinct_segment(values: &[i128]) -> Result<Vec<u8>> {
    let value_count =
        u32::try_from(values.len()).context("append-only distinct segment exceeds u32 values")?;
    let mut bytes = Vec::with_capacity(2 + 4 + values.len() * 16);
    bytes.push(APPEND_ONLY_DISTINCT_SEGMENT_VERSION);
    bytes.push(APPEND_ONLY_DISTINCT_I128_TAG);
    bytes.extend_from_slice(&value_count.to_be_bytes());
    for value in values {
        bytes.extend_from_slice(&value.to_be_bytes());
    }
    Ok(bytes)
}

fn decode_append_only_i128_distinct_segment(bytes: &[u8]) -> Result<Vec<i128>> {
    let mut offset =
        append_only_distinct_segment_values_offset(bytes, APPEND_ONLY_DISTINCT_I128_TAG, 16)?;
    let value_count = append_only_distinct_segment_value_count(bytes)?;
    let mut values = Vec::with_capacity(value_count);
    for _ in 0..value_count {
        values.push(compact_read_i128(bytes, &mut offset)?);
    }
    if offset != bytes.len() {
        bail!("append-only decimal distinct segment has trailing bytes");
    }
    Ok(values)
}

fn encode_append_only_string_distinct_segment(values: &[String]) -> Result<Vec<u8>> {
    let value_count =
        u32::try_from(values.len()).context("append-only distinct segment exceeds u32 values")?;
    let value_bytes = values
        .iter()
        .map(|value| 4usize.saturating_add(value.len()))
        .sum::<usize>();
    let mut bytes = Vec::with_capacity(2 + 4 + value_bytes);
    bytes.push(APPEND_ONLY_DISTINCT_SEGMENT_VERSION);
    bytes.push(APPEND_ONLY_DISTINCT_STRING_TAG);
    bytes.extend_from_slice(&value_count.to_be_bytes());
    for value in values {
        let len = u32::try_from(value.len())
            .context("append-only string distinct value exceeds u32 bytes")?;
        bytes.extend_from_slice(&len.to_be_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }
    Ok(bytes)
}

fn decode_append_only_string_distinct_segment(bytes: &[u8]) -> Result<Vec<String>> {
    let mut offset =
        append_only_distinct_segment_values_offset(bytes, APPEND_ONLY_DISTINCT_STRING_TAG, 0)?;
    let value_count = append_only_distinct_segment_value_count(bytes)?;
    let mut values = Vec::with_capacity(value_count);
    for _ in 0..value_count {
        let len = compact_read_u32(bytes, &mut offset)? as usize;
        let value = String::from_utf8(compact_take(bytes, &mut offset, len)?.to_vec())
            .context("decode append-only string distinct value")?;
        values.push(value);
    }
    if offset != bytes.len() {
        bail!("append-only string distinct segment has trailing bytes");
    }
    Ok(values)
}

fn append_only_distinct_segment_values_offset(
    bytes: &[u8],
    expected_type: u8,
    fixed_value_width: usize,
) -> Result<usize> {
    if bytes.len() < 6 {
        bail!("append-only distinct segment is too short");
    }
    if bytes[0] != APPEND_ONLY_DISTINCT_SEGMENT_VERSION {
        bail!(
            "unsupported append-only distinct segment version {}",
            bytes[0]
        );
    }
    if bytes[1] != expected_type {
        bail!("append-only distinct segment type mismatch");
    }
    let value_count = append_only_distinct_segment_value_count(bytes)?;
    if fixed_value_width > 0
        && bytes.len() != 6usize.saturating_add(value_count.saturating_mul(fixed_value_width))
    {
        bail!("append-only fixed-width distinct segment length mismatch");
    }
    Ok(6)
}

fn append_only_distinct_segment_value_count(bytes: &[u8]) -> Result<usize> {
    if bytes.len() < 6 {
        bail!("append-only distinct segment is too short");
    }
    let count: [u8; 4] = bytes[2..6]
        .try_into()
        .map_err(|_| anyhow::anyhow!("append-only distinct segment count must be u32"))?;
    Ok(u32::from_be_bytes(count) as usize)
}

fn decode_append_only_distinct_segment_id(prefix: &[u8], key: &[u8]) -> Result<u64> {
    let suffix = key
        .get(prefix.len()..)
        .ok_or_else(|| anyhow::anyhow!("invalid append-only distinct segment key"))?;
    let suffix: [u8; 8] = suffix
        .try_into()
        .map_err(|_| anyhow::anyhow!("append-only distinct segment key suffix must be u64"))?;
    Ok(u64::from_be_bytes(suffix))
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

fn encode_compact_group_state(state: &CompactGroupState) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(compact_group_state_encoded_len(state)?);
    append_compact_group_state_bytes(&mut bytes, state)?;
    Ok(bytes)
}

fn compact_group_state_encoded_len(state: &CompactGroupState) -> Result<usize> {
    let aggregate_count = u16::try_from(state.aggregates.len())
        .context("grouped-stats compact aggregate count exceeds u16")?;
    let mut len = 1usize + 8 + std::mem::size_of_val(&aggregate_count);
    for aggregate in &state.aggregates {
        len = len.saturating_add(match aggregate {
            CompactAggregateState::Unsupported | CompactAggregateState::MinMaxI64(None) => 1,
            CompactAggregateState::I64(_) | CompactAggregateState::MinMaxI64(Some(_)) => 1 + 8,
            CompactAggregateState::Pair { .. } => 1 + 16,
        });
    }
    Ok(len)
}

fn append_compact_group_state_bytes(bytes: &mut Vec<u8>, state: &CompactGroupState) -> Result<()> {
    let aggregate_count = u16::try_from(state.aggregates.len())
        .context("grouped-stats compact aggregate count exceeds u16")?;
    bytes.push(COMPACT_STATE_VERSION);
    bytes.extend_from_slice(&state.row_count.to_be_bytes());
    bytes.extend_from_slice(&aggregate_count.to_be_bytes());
    for aggregate in &state.aggregates {
        match aggregate {
            CompactAggregateState::Unsupported => {
                bytes.push(COMPACT_AGG_UNSUPPORTED_TAG);
            }
            CompactAggregateState::I64(value) => {
                bytes.push(COMPACT_AGG_I64_TAG);
                bytes.extend_from_slice(&value.to_be_bytes());
            }
            CompactAggregateState::Pair { sum, count } => {
                bytes.push(COMPACT_AGG_PAIR_TAG);
                bytes.extend_from_slice(&sum.to_be_bytes());
                bytes.extend_from_slice(&count.to_be_bytes());
            }
            CompactAggregateState::MinMaxI64(None) => {
                bytes.push(COMPACT_AGG_MINMAX_NONE_TAG);
            }
            CompactAggregateState::MinMaxI64(Some(value)) => {
                bytes.push(COMPACT_AGG_MINMAX_I64_TAG);
                bytes.extend_from_slice(&value.to_be_bytes());
            }
        }
    }
    Ok(())
}

fn decode_compact_group_state(specs: &[AggregateSpec], bytes: &[u8]) -> Result<CompactGroupState> {
    let mut offset = 0;
    let version = compact_read_u8(bytes, &mut offset)?;
    if version != COMPACT_STATE_VERSION {
        bail!("unsupported grouped-stats compact state version {version}");
    }
    let row_count = compact_read_i64(bytes, &mut offset)?;
    let aggregate_count = usize::from(compact_read_u16(bytes, &mut offset)?);
    if aggregate_count != specs.len() {
        bail!("grouped-stats compact state aggregate count mismatch");
    }
    let mut aggregates = Vec::with_capacity(aggregate_count);
    for spec in specs {
        let tag = compact_read_u8(bytes, &mut offset)?;
        let aggregate = match tag {
            COMPACT_AGG_UNSUPPORTED_TAG => CompactAggregateState::Unsupported,
            COMPACT_AGG_I64_TAG => {
                CompactAggregateState::I64(compact_read_i64(bytes, &mut offset)?)
            }
            COMPACT_AGG_PAIR_TAG => {
                let sum = compact_read_i64(bytes, &mut offset)?;
                let count = compact_read_i64(bytes, &mut offset)?;
                CompactAggregateState::Pair { sum, count }
            }
            COMPACT_AGG_MINMAX_NONE_TAG => CompactAggregateState::MinMaxI64(None),
            COMPACT_AGG_MINMAX_I64_TAG => {
                CompactAggregateState::MinMaxI64(Some(compact_read_i64(bytes, &mut offset)?))
            }
            _ => bail!("unknown grouped-stats compact aggregate tag {tag}"),
        };
        if !compact_aggregate_state_matches_spec(&aggregate, spec) {
            bail!("grouped-stats compact aggregate state does not match aggregate spec");
        }
        aggregates.push(aggregate);
    }
    if offset != bytes.len() {
        bail!("grouped-stats compact state has trailing bytes");
    }
    Ok(CompactGroupState {
        row_count,
        aggregates,
    })
}

fn encode_compact_group_snapshot(values: &CompactGroupStateMap) -> Result<Vec<u8>> {
    let entry_count = u32::try_from(values.values().filter(|state| state.row_count != 0).count())
        .context("grouped-stats compact snapshot entry count exceeds u32")?;
    let mut bytes = Vec::with_capacity(COMPACT_SNAPSHOT_MAGIC.len() + 1 + 4 + values.len() * 32);
    bytes.extend_from_slice(COMPACT_SNAPSHOT_MAGIC);
    bytes.push(COMPACT_SNAPSHOT_VERSION);
    bytes.extend_from_slice(&entry_count.to_be_bytes());
    for (group_key, state) in values {
        if state.row_count == 0 {
            continue;
        }
        let group_key_len = u32::try_from(group_key.len())
            .context("grouped-stats compact snapshot group key length exceeds u32")?;
        let state_len = u32::try_from(compact_group_state_encoded_len(state)?)
            .context("grouped-stats compact snapshot state length exceeds u32")?;
        bytes.extend_from_slice(&group_key_len.to_be_bytes());
        bytes.extend_from_slice(group_key);
        bytes.extend_from_slice(&state_len.to_be_bytes());
        append_compact_group_state_bytes(&mut bytes, state)?;
    }
    Ok(bytes)
}

fn decode_compact_group_snapshot(
    specs: &[AggregateSpec],
    bytes: &[u8],
) -> Result<CompactGroupStateMap> {
    let mut offset = 0;
    let magic = compact_take(bytes, &mut offset, COMPACT_SNAPSHOT_MAGIC.len())?;
    if magic != COMPACT_SNAPSHOT_MAGIC {
        bail!("invalid grouped-stats compact snapshot magic");
    }
    let version = compact_read_u8(bytes, &mut offset)?;
    if version != COMPACT_SNAPSHOT_VERSION {
        bail!("unsupported grouped-stats compact snapshot version {version}");
    }
    let entry_count = compact_read_u32(bytes, &mut offset)? as usize;
    let mut values = CompactGroupStateMap::with_capacity(entry_count);
    for _ in 0..entry_count {
        let group_key_len = compact_read_u32(bytes, &mut offset)? as usize;
        let group_key = compact_take(bytes, &mut offset, group_key_len)?.to_vec();
        let state_len = compact_read_u32(bytes, &mut offset)? as usize;
        let state =
            decode_compact_group_state(specs, compact_take(bytes, &mut offset, state_len)?)?;
        if state.row_count != 0 && values.insert(group_key, state).is_some() {
            bail!("duplicate grouped-stats compact snapshot group key");
        }
    }
    if offset != bytes.len() {
        bail!("grouped-stats compact snapshot has trailing bytes");
    }
    Ok(values)
}

fn compact_aggregate_state_matches_spec(
    state: &CompactAggregateState,
    spec: &AggregateSpec,
) -> bool {
    match spec.kind {
        AggregateKind::Count => matches!(state, CompactAggregateState::I64(_)),
        AggregateKind::Sum => {
            matches!(state, CompactAggregateState::I64(_))
                && !matches!(spec.value_type, Some(AggregateValueType::Decimal128))
        }
        AggregateKind::Avg => matches!(state, CompactAggregateState::Pair { .. }),
        AggregateKind::Min | AggregateKind::Max => {
            matches!(state, CompactAggregateState::MinMaxI64(_))
                && matches!(
                    spec.value_type,
                    Some(
                        AggregateValueType::Int64
                            | AggregateValueType::TimestampMillis
                            | AggregateValueType::DateDays
                    )
                )
        }
        AggregateKind::DistinctCount => matches!(state, CompactAggregateState::Unsupported),
    }
}

fn compact_read_u8(bytes: &[u8], offset: &mut usize) -> Result<u8> {
    let value = *compact_take(bytes, offset, 1)?
        .first()
        .ok_or_else(|| anyhow::anyhow!("grouped-stats compact state expected u8"))?;
    Ok(value)
}

fn compact_read_u16(bytes: &[u8], offset: &mut usize) -> Result<u16> {
    let value: [u8; 2] = compact_take(bytes, offset, 2)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-stats compact state expected u16"))?;
    Ok(u16::from_be_bytes(value))
}

fn compact_read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let value: [u8; 4] = compact_take(bytes, offset, 4)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-stats compact state expected u32"))?;
    Ok(u32::from_be_bytes(value))
}

fn compact_read_i64(bytes: &[u8], offset: &mut usize) -> Result<i64> {
    let value: [u8; 8] = compact_take(bytes, offset, 8)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-stats compact state expected i64"))?;
    Ok(i64::from_be_bytes(value))
}

fn compact_read_i128(bytes: &[u8], offset: &mut usize) -> Result<i128> {
    let value: [u8; 16] = compact_take(bytes, offset, 16)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("grouped-stats compact state expected i128"))?;
    Ok(i128::from_be_bytes(value))
}

fn compact_take<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| anyhow::anyhow!("grouped-stats compact state offset overflow"))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| anyhow::anyhow!("truncated grouped-stats compact state"))?;
    *offset = end;
    Ok(value)
}
