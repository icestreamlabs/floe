use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, StringArray, TimestampMillisecondArray, UInt32Array,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::logical_expr::logical_plan::{Filter, Join, Limit, Sort, TableScan, Window};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, Operator, ScalarUDF};
use datafusion::physical_plan::collect;
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarZSet};
use dbsp::storage::KeyValueTable;

use crate::delta_consolidation::diff_snapshot_batches;
use crate::encoding::EncodedRowScalar;
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::scalar_array_builder::ScalarColumnBuilder;
use crate::table_provider::DynamicStateTableProvider;
use crate::vectorized_runtime::source_state::{rename_batches, resolve_source_table};

use super::{
    VectorizedMaterializedViewState, VectorizedSourceState, apply_weighted_snapshot_delta,
    normalize_batches,
};

pub(super) struct ColumnarJoinTopNPlan {
    left_source: String,
    right_source: String,
    kind: ColumnarJoinTopNPlanKind,
}

enum ColumnarJoinTopNPlanKind {
    PartitionedBestBid {
        left_key_column: String,
        right_key_column: String,
    },
    GlobalSnapshotDiff {
        logical_plan: LogicalPlan,
    },
}

pub(super) struct ColumnarJoinTopNMaterializedViewState {
    left: JoinTopNSourceState,
    right: JoinTopNSourceState,
    output_zset: SlateBackedColumnarZSet,
    evaluator: JoinTopNEvaluator,
    initial_snapshot: Vec<RecordBatch>,
}

impl ColumnarJoinTopNMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

struct JoinTopNSourceState {
    source_name: String,
    schema: SchemaRef,
    key_idx: Option<usize>,
    input_zset: SlateBackedColumnarZSet,
    snapshot: Vec<RecordBatch>,
}

enum JoinTopNEvaluator {
    PartitionedBestBid(JoinTopNBestBidEvaluator),
    GlobalSnapshotDiff(GlobalJoinTopNEvaluator),
}

struct JoinTopNBestBidEvaluator {
    output_schema: SchemaRef,
    left: JoinTopNLeftIndices,
    right: JoinTopNRightIndices,
    output_mapping: Vec<JoinTopNOutputSource>,
}

struct GlobalJoinTopNEvaluator {
    ctx: SessionContext,
    logical_plan: LogicalPlan,
    left_input: GlobalJoinTopNInput,
    right_input: GlobalJoinTopNInput,
    output_schema: SchemaRef,
}

struct GlobalJoinTopNInput {
    provider: Arc<DynamicStateTableProvider>,
    alias_schema: Option<SchemaRef>,
    alias_provider: Option<Arc<DynamicStateTableProvider>>,
}

struct JoinTopNLeftIndices {
    id: usize,
    item_name: usize,
    description: usize,
    initial_bid: usize,
    reserve: usize,
    date_time: usize,
    expires: usize,
    seller: usize,
    category: usize,
    extra: usize,
}

struct JoinTopNRightIndices {
    auction: usize,
    bidder: usize,
    price: usize,
    date_time: usize,
    extra: usize,
}

enum JoinTopNOutputSource {
    Left(usize),
    Right(usize),
}

struct JoinTopNBestBid {
    left_batch_idx: usize,
    left_row_idx: usize,
    right_batch_idx: usize,
    right_row_idx: usize,
    price: i64,
    bid_time: i64,
}

pub(super) fn columnar_join_topn_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarJoinTopNPlan>> {
    if contains_aggregate(plan) {
        return Ok(None);
    }
    if let Some(plan) = partitioned_best_bid_join_topn_plan_for_plan(plan, sources)? {
        return Ok(Some(plan));
    }
    global_join_topn_plan_for_plan(plan, sources)
}

fn partitioned_best_bid_join_topn_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarJoinTopNPlan>> {
    let Some((_rank_column, filter)) = row_number_filter_for_plan(plan) else {
        return Ok(None);
    };
    let Some((window, _projection_without_rank)) = extract_window_plan(filter.input.as_ref())
    else {
        return Ok(None);
    };
    if window.window_expr.len() != 1 {
        return Ok(None);
    }
    let joins = joins_for_plan(window.input.as_ref());
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
    if contains_unsupported_join_topn_wrapper(plan) {
        return Ok(None);
    }

    let Some((left_key_column, right_key_column)) =
        join_key_columns(join, &left_source, &right_source, sources)
    else {
        return Ok(None);
    };
    if !window_partitions_by_join_key(window, &left_key_column) {
        return Ok(None);
    }

    Ok(Some(ColumnarJoinTopNPlan {
        left_source,
        right_source,
        kind: ColumnarJoinTopNPlanKind::PartitionedBestBid {
            left_key_column,
            right_key_column,
        },
    }))
}

fn global_join_topn_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarJoinTopNPlan>> {
    if !global_sort_limit_for_plan(plan) {
        return Ok(None);
    }
    let joins = joins_for_plan(plan);
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
    let all_sources = source_set_for_plan(plan, sources);
    let expected_sources = [left_source.clone(), right_source.clone()]
        .into_iter()
        .collect::<BTreeSet<_>>();
    if all_sources != expected_sources {
        return Ok(None);
    }
    if contains_unsupported_global_join_topn_wrapper(plan) {
        return Ok(None);
    }
    Ok(Some(ColumnarJoinTopNPlan {
        left_source,
        right_source,
        kind: ColumnarJoinTopNPlanKind::GlobalSnapshotDiff {
            logical_plan: plan.clone(),
        },
    }))
}

pub(super) async fn build_columnar_join_topn_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarJoinTopNPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    udfs: &[ScalarUDF],
) -> Result<ColumnarJoinTopNMaterializedViewState> {
    let left_source = sources
        .get(&plan.left_source)
        .ok_or_else(|| anyhow::anyhow!("unknown join-topn source '{}'", plan.left_source))?;
    let right_source = sources
        .get(&plan.right_source)
        .ok_or_else(|| anyhow::anyhow!("unknown join-topn source '{}'", plan.right_source))?;
    let (left_key_idx, right_key_idx) = match &plan.kind {
        ColumnarJoinTopNPlanKind::PartitionedBestBid {
            left_key_column,
            right_key_column,
        } => (
            Some(
                left_source
                    .schema
                    .index_of(left_key_column)
                    .with_context(|| format!("find join-topn left key '{left_key_column}'"))?,
            ),
            Some(
                right_source
                    .schema
                    .index_of(right_key_column)
                    .with_context(|| format!("find join-topn right key '{right_key_column}'"))?,
            ),
        ),
        ColumnarJoinTopNPlanKind::GlobalSnapshotDiff { .. } => (None, None),
    };

    let mv_namespace = namespaces::materialized_view(view_name)?;
    let (left_namespace, right_namespace) = if plan.left_source == plan.right_source {
        (
            format!(
                "{mv_namespace}/columnar/join_topn/left/{}/input",
                plan.left_source
            ),
            format!(
                "{mv_namespace}/columnar/join_topn/right/{}/input",
                plan.right_source
            ),
        )
    } else {
        (
            format!(
                "{mv_namespace}/columnar/join_topn/{}/input",
                plan.left_source
            ),
            format!(
                "{mv_namespace}/columnar/join_topn/{}/input",
                plan.right_source
            ),
        )
    };
    let output_namespace = format!("{mv_namespace}/columnar/join_topn/output");

    let left_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        left_namespace,
        Arc::clone(&left_source.schema),
    )
    .await
    .context("initialize SlateDB-backed join-topn left input zset")?;
    let right_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        right_namespace,
        Arc::clone(&right_source.schema),
    )
    .await
    .context("initialize SlateDB-backed join-topn right input zset")?;
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed join-topn output zset")?;
    let initial_snapshot = snapshot_batches_from_zset(
        &output_zset
            .materialize_columnar()
            .await
            .context("load join-topn output snapshot")?,
    )?;

    let left_name = plan.left_source;
    let right_name = plan.right_source;
    let evaluator = match plan.kind {
        ColumnarJoinTopNPlanKind::PartitionedBestBid { .. } => {
            JoinTopNEvaluator::PartitionedBestBid(
                JoinTopNBestBidEvaluator::build(
                    &left_source.schema,
                    &right_source.schema,
                    output_schema,
                )
                .context("build join-topn vectorized evaluator")?,
            )
        }
        ColumnarJoinTopNPlanKind::GlobalSnapshotDiff { logical_plan } => {
            JoinTopNEvaluator::GlobalSnapshotDiff(
                GlobalJoinTopNEvaluator::build(
                    logical_plan,
                    &left_name,
                    &right_name,
                    sources,
                    output_schema,
                    udfs,
                )
                .await
                .context("build global join-topn vectorized evaluator")?,
            )
        }
    };

    Ok(ColumnarJoinTopNMaterializedViewState {
        left: JoinTopNSourceState {
            source_name: left_name,
            schema: Arc::clone(&left_source.schema),
            key_idx: left_key_idx,
            snapshot: snapshot_batches_from_zset(
                &left_zset
                    .materialize_columnar()
                    .await
                    .context("load join-topn left input snapshot")?,
            )?,
            input_zset: left_zset,
        },
        right: JoinTopNSourceState {
            source_name: right_name,
            schema: Arc::clone(&right_source.schema),
            key_idx: right_key_idx,
            snapshot: snapshot_batches_from_zset(
                &right_zset
                    .materialize_columnar()
                    .await
                    .context("load join-topn right input snapshot")?,
            )?,
            input_zset: right_zset,
        },
        output_zset,
        evaluator,
        initial_snapshot,
    })
}

pub(super) async fn run_columnar_join_topn_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(columnar) = mv.columnar_join_topn.as_mut() else {
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

    let next_left_snapshot =
        apply_source_snapshot_delta(&columnar.left.schema, &columnar.left.snapshot, &left_delta)
            .await?;
    let next_right_snapshot = apply_source_snapshot_delta(
        &columnar.right.schema,
        &columnar.right.snapshot,
        &right_delta,
    )
    .await?;
    let output_delta_batches = match &columnar.evaluator {
        JoinTopNEvaluator::PartitionedBestBid(evaluator) => {
            let left_key_idx = columnar
                .left
                .key_idx
                .context("partitioned join-topn left key index is missing")?;
            let right_key_idx = columnar
                .right
                .key_idx
                .context("partitioned join-topn right key index is missing")?;
            let mut touched_keys = HashSet::new();
            collect_i64_keys_from_delta(&left_delta, left_key_idx, &mut touched_keys)?;
            collect_i64_keys_from_delta(&right_delta, right_key_idx, &mut touched_keys)?;

            let previous_left = filter_batches_to_i64_keys(
                &columnar.left.schema,
                left_key_idx,
                &columnar.left.snapshot,
                &touched_keys,
            )?;
            let previous_right = filter_batches_to_i64_keys(
                &columnar.right.schema,
                right_key_idx,
                &columnar.right.snapshot,
                &touched_keys,
            )?;
            let next_left = filter_batches_to_i64_keys(
                &columnar.left.schema,
                left_key_idx,
                &next_left_snapshot,
                &touched_keys,
            )?;
            let next_right = filter_batches_to_i64_keys(
                &columnar.right.schema,
                right_key_idx,
                &next_right_snapshot,
                &touched_keys,
            )?;

            let (previous_output, next_output) = if touched_keys.is_empty() {
                (Vec::new(), Vec::new())
            } else {
                (
                    evaluator
                        .evaluate(&previous_left, &previous_right)
                        .await
                        .context("evaluate previous join-topn partition outputs")?,
                    evaluator
                        .evaluate(&next_left, &next_right)
                        .await
                        .context("evaluate next join-topn partition outputs")?,
                )
            };
            diff_snapshot_batches(
                Arc::clone(&mv.output_schema),
                &previous_output,
                &next_output,
            )
            .await
            .context("diff join-topn partition outputs")?
            .batches
        }
        JoinTopNEvaluator::GlobalSnapshotDiff(evaluator) => {
            if left_delta.batches().is_empty() && right_delta.batches().is_empty() {
                Vec::new()
            } else {
                let next_output = evaluator
                    .evaluate(
                        &columnar.left.source_name,
                        &next_left_snapshot,
                        &columnar.right.source_name,
                        &next_right_snapshot,
                    )
                    .await
                    .context("evaluate global join-topn output")?;
                diff_snapshot_batches(
                    Arc::clone(&mv.output_schema),
                    &mv.previous_snapshot,
                    &next_output,
                )
                .await
                .context("diff global join-topn output")?
                .batches
            }
        }
    };

    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build join-topn output zset delta")?;
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
            "apply Slate-backed join-topn columnar snapshot delta for '{}'",
            mv.view_name
        )
    })?;

    columnar.left.snapshot = next_left_snapshot;
    columnar.right.snapshot = next_right_snapshot;
    let handle = registry.register(mv.view_name.clone());
    handle.publish_arrow_version(version, next_snapshot.clone(), delta_batches);
    mv.previous_snapshot = next_snapshot;
    tracing::debug!(
        view = %mv.view_name,
        version,
        total_ms = plan_start.elapsed().as_millis() as u64,
        mode = "columnar_join_topn",
        "SlateDB-backed join-topn columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

fn source_input_delta(
    source: &JoinTopNSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    if let Some(weighted_batches) = weighted_delta_batches.get(source.source_name.as_str()) {
        ColumnarZSet::try_new_weighted(Arc::clone(&source.schema), weighted_batches.clone())
            .with_context(|| {
                format!(
                    "build weighted join-topn input delta for '{}'",
                    source.source_name
                )
            })
    } else if let Some(source_batches) = insert_batches.get(source.source_name.as_str()) {
        ColumnarZSet::from_value_batches(Arc::clone(&source.schema), source_batches.clone(), 1)
            .with_context(|| {
                format!(
                    "build insert join-topn input delta for '{}'",
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

fn collect_i64_keys_from_delta(
    delta: &ColumnarZSet,
    key_idx: usize,
    output: &mut HashSet<i64>,
) -> Result<()> {
    let weight_idx = delta.value_column_count();
    for batch in delta.batches().iter().filter(|batch| batch.num_rows() > 0) {
        let keys = batch
            .column(key_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("join-topn key column must be Int64"))?;
        let weights = batch
            .column(weight_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("columnar zset weight column must be Int64"))?;
        for row_idx in 0..batch.num_rows() {
            if keys.is_null(row_idx) || weights.is_null(row_idx) || weights.value(row_idx) == 0 {
                continue;
            }
            output.insert(keys.value(row_idx));
        }
    }
    Ok(())
}

fn filter_batches_to_i64_keys(
    schema: &SchemaRef,
    key_idx: usize,
    batches: &[RecordBatch],
    keys: &HashSet<i64>,
) -> Result<Vec<RecordBatch>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut output = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let values = batch
            .column(key_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("join-topn key column must be Int64"))?;
        let mut indices = Vec::new();
        for row_idx in 0..batch.num_rows() {
            if !values.is_null(row_idx) && keys.contains(&values.value(row_idx)) {
                indices.push(u32::try_from(row_idx).context("join-topn batch exceeds u32 rows")?);
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

impl JoinTopNBestBidEvaluator {
    fn build(
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        output_schema: &SchemaRef,
    ) -> Result<Self> {
        let left = JoinTopNLeftIndices {
            id: field_index(left_schema, &["id"])?,
            item_name: field_index(left_schema, &["itemName", "item_name"])?,
            description: field_index(left_schema, &["description"])?,
            initial_bid: field_index(left_schema, &["initialBid", "initial_bid"])?,
            reserve: field_index(left_schema, &["reserve"])?,
            date_time: field_index(left_schema, &["dateTime", "date_time"])?,
            expires: field_index(left_schema, &["expires"])?,
            seller: field_index(left_schema, &["seller"])?,
            category: field_index(left_schema, &["category"])?,
            extra: field_index(left_schema, &["extra"])?,
        };
        let right = JoinTopNRightIndices {
            auction: field_index(right_schema, &["auction"])?,
            bidder: field_index(right_schema, &["bidder"])?,
            price: field_index(right_schema, &["price"])?,
            date_time: field_index(right_schema, &["dateTime", "date_time"])?,
            extra: field_index(right_schema, &["extra"])?,
        };
        let output_mapping = output_schema
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "id" => Ok(JoinTopNOutputSource::Left(left.id)),
                "itemName" => Ok(JoinTopNOutputSource::Left(left.item_name)),
                "description" => Ok(JoinTopNOutputSource::Left(left.description)),
                "initialBid" => Ok(JoinTopNOutputSource::Left(left.initial_bid)),
                "reserve" => Ok(JoinTopNOutputSource::Left(left.reserve)),
                "dateTime" => Ok(JoinTopNOutputSource::Left(left.date_time)),
                "expires" => Ok(JoinTopNOutputSource::Left(left.expires)),
                "seller" => Ok(JoinTopNOutputSource::Left(left.seller)),
                "category" => Ok(JoinTopNOutputSource::Left(left.category)),
                "extra" => Ok(JoinTopNOutputSource::Left(left.extra)),
                "auction" => Ok(JoinTopNOutputSource::Right(right.auction)),
                "bidder" => Ok(JoinTopNOutputSource::Right(right.bidder)),
                "price" => Ok(JoinTopNOutputSource::Right(right.price)),
                "bidTime" => Ok(JoinTopNOutputSource::Right(right.date_time)),
                "bidExtra" => Ok(JoinTopNOutputSource::Right(right.extra)),
                other => bail!("unsupported join-topn output field '{other}'"),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            output_schema: Arc::clone(output_schema),
            left,
            right,
            output_mapping,
        })
    }

    async fn evaluate(
        &self,
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
    ) -> Result<Vec<RecordBatch>> {
        let mut builders = self
            .output_schema
            .fields()
            .iter()
            .map(|field| ScalarColumnBuilder::new(field.data_type(), left_batches.len()))
            .collect::<Result<Vec<_>>>()?;

        for (left_batch_idx, left_batch) in left_batches.iter().enumerate() {
            let left_ids = int64_column(left_batch, self.left.id)?;
            for left_row_idx in 0..left_batch.num_rows() {
                if left_ids.is_null(left_row_idx) {
                    continue;
                }
                let auction_id = left_ids.value(left_row_idx);
                let Some(best) = self.best_bid_for_auction(
                    auction_id,
                    left_batch_idx,
                    left_row_idx,
                    left_batches,
                    right_batches,
                )?
                else {
                    continue;
                };
                self.append_output_row(&mut builders, left_batches, right_batches, &best)?;
            }
        }

        let columns = builders
            .iter_mut()
            .map(ScalarColumnBuilder::finish_array)
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), columns)?;
        if batch.num_rows() == 0 {
            Ok(Vec::new())
        } else {
            Ok(vec![batch])
        }
    }

    fn best_bid_for_auction(
        &self,
        auction_id: i64,
        left_batch_idx: usize,
        left_row_idx: usize,
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
    ) -> Result<Option<JoinTopNBestBid>> {
        let left_batch = &left_batches[left_batch_idx];
        let auction_start = i64_or_timestamp_value(left_batch, self.left.date_time, left_row_idx)?;
        let auction_expires = i64_or_timestamp_value(left_batch, self.left.expires, left_row_idx)?;
        let mut best: Option<JoinTopNBestBid> = None;
        for (right_batch_idx, right_batch) in right_batches.iter().enumerate() {
            let right_auctions = int64_column(right_batch, self.right.auction)?;
            let right_prices = int64_column(right_batch, self.right.price)?;
            for right_row_idx in 0..right_batch.num_rows() {
                if right_auctions.is_null(right_row_idx)
                    || right_prices.is_null(right_row_idx)
                    || right_auctions.value(right_row_idx) != auction_id
                {
                    continue;
                }
                let bid_time =
                    i64_or_timestamp_value(right_batch, self.right.date_time, right_row_idx)?;
                if bid_time < auction_start || bid_time > auction_expires {
                    continue;
                }
                let price = right_prices.value(right_row_idx);
                let replace = match &best {
                    Some(current) => {
                        price > current.price
                            || (price == current.price && bid_time < current.bid_time)
                    }
                    None => true,
                };
                if replace {
                    best = Some(JoinTopNBestBid {
                        left_batch_idx,
                        left_row_idx,
                        right_batch_idx,
                        right_row_idx,
                        price,
                        bid_time,
                    });
                }
            }
        }
        Ok(best)
    }

    fn append_output_row(
        &self,
        builders: &mut [ScalarColumnBuilder],
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
        best: &JoinTopNBestBid,
    ) -> Result<()> {
        let left = &left_batches[best.left_batch_idx];
        let right = &right_batches[best.right_batch_idx];
        for (builder, source) in builders.iter_mut().zip(self.output_mapping.iter()) {
            let value = match source {
                JoinTopNOutputSource::Left(idx) => encoded_scalar(left, *idx, best.left_row_idx)?,
                JoinTopNOutputSource::Right(idx) => {
                    encoded_scalar(right, *idx, best.right_row_idx)?
                }
            };
            builder.append_encoded_scalar(value.as_ref())?;
        }
        Ok(())
    }
}

impl GlobalJoinTopNEvaluator {
    async fn build(
        logical_plan: LogicalPlan,
        left_source_name: &str,
        right_source_name: &str,
        sources: &HashMap<String, VectorizedSourceState>,
        output_schema: &SchemaRef,
        udfs: &[ScalarUDF],
    ) -> Result<Self> {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        for udf in udfs.iter().cloned() {
            ctx.register_udf(udf);
        }
        let left_input = GlobalJoinTopNInput::new(left_source_name, sources)?;
        let right_input = GlobalJoinTopNInput::new(right_source_name, sources)?;
        let logical_plan = rebind_global_join_topn_logical_plan(
            logical_plan,
            left_source_name,
            &left_input,
            right_source_name,
            &right_input,
        )?;
        Ok(Self {
            ctx,
            logical_plan,
            left_input,
            right_input,
            output_schema: Arc::clone(output_schema),
        })
    }

    async fn evaluate(
        &self,
        left_source_name: &str,
        left_batches: &[RecordBatch],
        right_source_name: &str,
        right_batches: &[RecordBatch],
    ) -> Result<Vec<RecordBatch>> {
        self.left_input
            .set_batches(left_batches)
            .with_context(|| format!("set global join-topn left input for '{left_source_name}'"))?;
        self.right_input
            .set_batches(right_batches)
            .with_context(|| {
                format!("set global join-topn right input for '{right_source_name}'")
            })?;
        let plan = self
            .ctx
            .state()
            .create_physical_plan(&self.logical_plan)
            .await
            .context("rebuild global join-topn physical plan")?;
        let collected = collect(plan, self.ctx.task_ctx()).await;
        self.clear_inputs()?;
        normalize_batches(
            collected.context("execute global join-topn evaluator")?,
            &self.output_schema,
        )
    }

    fn clear_inputs(&self) -> Result<()> {
        self.left_input.clear()?;
        self.right_input.clear()?;
        Ok(())
    }
}

impl GlobalJoinTopNInput {
    fn new(source_name: &str, sources: &HashMap<String, VectorizedSourceState>) -> Result<Self> {
        let source = sources
            .get(source_name)
            .ok_or_else(|| anyhow::anyhow!("unknown global join-topn source '{source_name}'"))?;
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

    fn set_batches(&self, batches: &[RecordBatch]) -> Result<()> {
        self.provider.set_batches(batches.to_vec())?;
        if let (Some(alias_schema), Some(alias_provider)) =
            (self.alias_schema.as_ref(), self.alias_provider.as_ref())
        {
            alias_provider.set_batches(rename_batches(batches, alias_schema)?)?;
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

fn rebind_global_join_topn_logical_plan(
    logical_plan: LogicalPlan,
    left_source_name: &str,
    left_input: &GlobalJoinTopNInput,
    right_source_name: &str,
    right_input: &GlobalJoinTopNInput,
) -> Result<LogicalPlan> {
    let transformed = logical_plan.transform_up(|plan| match plan {
        LogicalPlan::TableScan(mut scan) => {
            let table_name = scan.table_name.table();
            let provider = left_input
                .provider_for_table(left_source_name, table_name)
                .or_else(|| right_input.provider_for_table(right_source_name, table_name));
            let Some(provider) = provider else {
                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
            };
            scan.source = provider_as_source(provider);
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        }
        other => Ok(Transformed::no(other)),
    })?;
    Ok(transformed.data)
}

fn int64_column(batch: &RecordBatch, idx: usize) -> Result<&Int64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("join-topn column must be Int64"))
}

fn field_index(schema: &SchemaRef, names: &[&str]) -> Result<usize> {
    for name in names {
        if let Ok(idx) = schema.index_of(name) {
            return Ok(idx);
        }
    }
    bail!("join-topn schema missing field aliases {names:?}")
}

fn i64_or_timestamp_value(batch: &RecordBatch, idx: usize, row_idx: usize) -> Result<i64> {
    match batch.schema().field(idx).data_type() {
        DataType::Int64 => {
            let values = int64_column(batch, idx)?;
            if values.is_null(row_idx) {
                bail!("join-topn time column cannot be NULL");
            }
            Ok(values.value(row_idx))
        }
        DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, _) => {
            let values = batch
                .column(idx)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| anyhow::anyhow!("join-topn column must be TimestampMillis"))?;
            if values.is_null(row_idx) {
                bail!("join-topn timestamp column cannot be NULL");
            }
            Ok(values.value(row_idx))
        }
        other => bail!("join-topn unsupported time column type {other:?}"),
    }
}

fn encoded_scalar(
    batch: &RecordBatch,
    idx: usize,
    row_idx: usize,
) -> Result<Option<EncodedRowScalar>> {
    if batch.column(idx).is_null(row_idx) {
        return Ok(None);
    }
    match batch.schema().field(idx).data_type() {
        DataType::Int64 => Ok(Some(EncodedRowScalar::Int64(
            int64_column(batch, idx)?.value(row_idx),
        ))),
        DataType::Utf8 => {
            let values = batch
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("join-topn column must be Utf8"))?;
            Ok(Some(EncodedRowScalar::Utf8(
                values.value(row_idx).to_string(),
            )))
        }
        DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, _) => {
            let values = batch
                .column(idx)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| anyhow::anyhow!("join-topn column must be TimestampMillis"))?;
            Ok(Some(EncodedRowScalar::TimestampMillis(
                values.value(row_idx),
            )))
        }
        other => bail!("join-topn unsupported output column type {other:?}"),
    }
}

fn row_number_filter_for_plan(plan: &LogicalPlan) -> Option<(String, &Filter)> {
    match plan {
        LogicalPlan::Projection(projection) => {
            row_number_filter_for_plan(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => {
            let rank_column = extract_row_number_limit_column(&filter.predicate)?;
            Some((rank_column, filter))
        }
        LogicalPlan::SubqueryAlias(alias) => row_number_filter_for_plan(alias.input.as_ref()),
        _ => None,
    }
}

fn extract_row_number_limit_column(predicate: &Expr) -> Option<String> {
    let Expr::BinaryExpr(binary) = predicate else {
        return None;
    };
    match (&*binary.left, binary.op, &*binary.right) {
        (Expr::Column(column), Operator::LtEq | Operator::Lt, Expr::Literal(_, _)) => {
            Some(column.name.clone())
        }
        _ => None,
    }
}

fn extract_window_plan(input: &LogicalPlan) -> Option<(&Window, Option<Vec<Expr>>)> {
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
    Some((window, Some(projection.expr.clone())))
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

fn joins_for_plan<'a>(plan: &'a LogicalPlan) -> Vec<&'a Join> {
    let mut joins = Vec::new();
    collect_joins(plan, &mut joins);
    joins
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
        LogicalPlan::Sort(sort) => collect_joins(sort.input.as_ref(), joins),
        LogicalPlan::Limit(limit) => collect_joins(limit.input.as_ref(), joins),
        LogicalPlan::Window(window) => collect_joins(window.input.as_ref(), joins),
        _ => {}
    }
}

fn join_key_columns(
    join: &Join,
    left_source: &str,
    right_source: &str,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Option<(String, String)> {
    let mut candidates = Vec::new();
    for (left, right) in &join.on {
        let (Some(left), Some(right)) = (column_name(left), column_name(right)) else {
            continue;
        };
        candidates.push((left, right));
    }
    if let Some(filter) = join.filter.as_ref() {
        collect_equality_column_pairs(filter, &mut candidates);
    }
    let left_schema = &sources.get(left_source)?.schema;
    let right_schema = &sources.get(right_source)?.schema;
    for (first, second) in candidates {
        let first_left = left_schema.index_of(&first).is_ok();
        let first_right = right_schema.index_of(&first).is_ok();
        let second_left = left_schema.index_of(&second).is_ok();
        let second_right = right_schema.index_of(&second).is_ok();
        if first_left && second_right && !(first_right && second_left) {
            return Some((first, second));
        }
        if second_left && first_right && !(second_right && first_left) {
            return Some((second, first));
        }
    }
    None
}

fn collect_equality_column_pairs(expr: &Expr, out: &mut Vec<(String, String)>) {
    let Expr::BinaryExpr(binary) = expr else {
        return;
    };
    if binary.op == Operator::Eq {
        if let (Some(left), Some(right)) = (column_name(&binary.left), column_name(&binary.right)) {
            out.push((left, right));
        }
        return;
    }
    if matches!(binary.op, Operator::And) {
        collect_equality_column_pairs(&binary.left, out);
        collect_equality_column_pairs(&binary.right, out);
    }
}

fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        _ => None,
    }
}

fn window_partitions_by_join_key(window: &Window, left_key_column: &str) -> bool {
    window.window_expr.iter().all(|expr| {
        let Expr::WindowFunction(window) = strip_alias(expr) else {
            return false;
        };
        window.params.partition_by.len() == 1
            && matches!(
                strip_alias(&window.params.partition_by[0]),
                Expr::Column(column) if column.name == left_key_column
            )
    })
}

fn strip_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => strip_alias(alias.expr.as_ref()),
        _ => expr,
    }
}

fn contains_aggregate(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_) => true,
        LogicalPlan::Projection(projection) => contains_aggregate(projection.input.as_ref()),
        LogicalPlan::Filter(filter) => contains_aggregate(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => contains_aggregate(alias.input.as_ref()),
        LogicalPlan::Sort(sort) => contains_aggregate(sort.input.as_ref()),
        LogicalPlan::Limit(limit) => contains_aggregate(limit.input.as_ref()),
        LogicalPlan::Window(window) => contains_aggregate(window.input.as_ref()),
        LogicalPlan::Join(join) => {
            contains_aggregate(join.left.as_ref()) || contains_aggregate(join.right.as_ref())
        }
        _ => false,
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
        LogicalPlan::Sort(sort) => collect_sources(sort.input.as_ref(), sources, out),
        LogicalPlan::Limit(limit) => collect_sources(limit.input.as_ref(), sources, out),
        LogicalPlan::Window(window) => collect_sources(window.input.as_ref(), sources, out),
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

fn global_sort_limit_for_plan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => {
            global_sort_limit_for_plan(projection.input.as_ref())
        }
        LogicalPlan::SubqueryAlias(alias) => global_sort_limit_for_plan(alias.input.as_ref()),
        LogicalPlan::Limit(Limit { input, fetch, .. }) if fetch.is_some() => {
            contains_non_empty_sort(input.as_ref())
        }
        LogicalPlan::Sort(Sort { expr, fetch, .. }) => fetch.is_some() && !expr.is_empty(),
        _ => false,
    }
}

fn contains_non_empty_sort(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Sort(sort) => !sort.expr.is_empty(),
        LogicalPlan::Projection(projection) => contains_non_empty_sort(projection.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => contains_non_empty_sort(alias.input.as_ref()),
        LogicalPlan::Filter(filter) => contains_non_empty_sort(filter.input.as_ref()),
        _ => false,
    }
}

fn contains_unsupported_global_join_topn_wrapper(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => {
            contains_unsupported_global_join_topn_wrapper(projection.input.as_ref())
        }
        LogicalPlan::SubqueryAlias(alias) => {
            contains_unsupported_global_join_topn_wrapper(alias.input.as_ref())
        }
        LogicalPlan::Filter(filter) => {
            contains_unsupported_global_join_topn_wrapper(filter.input.as_ref())
        }
        LogicalPlan::Limit(limit) => {
            limit.fetch.is_none()
                || contains_unsupported_global_join_topn_wrapper(limit.input.as_ref())
        }
        LogicalPlan::Sort(sort) => {
            sort.expr.is_empty()
                || contains_unsupported_global_join_topn_wrapper(sort.input.as_ref())
        }
        LogicalPlan::Join(join) => {
            contains_unsupported_global_join_input_wrapper(join.left.as_ref())
                || contains_unsupported_global_join_input_wrapper(join.right.as_ref())
        }
        LogicalPlan::TableScan(_) => false,
        _ => true,
    }
}

fn contains_unsupported_global_join_input_wrapper(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => {
            contains_unsupported_global_join_input_wrapper(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => {
            contains_unsupported_global_join_input_wrapper(filter.input.as_ref())
        }
        LogicalPlan::SubqueryAlias(alias) => {
            contains_unsupported_global_join_input_wrapper(alias.input.as_ref())
        }
        LogicalPlan::TableScan(_) => false,
        _ => true,
    }
}

fn contains_unsupported_join_topn_wrapper(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => {
            contains_unsupported_join_topn_wrapper(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => {
            contains_unsupported_join_topn_wrapper(filter.input.as_ref())
        }
        LogicalPlan::SubqueryAlias(alias) => {
            contains_unsupported_join_topn_wrapper(alias.input.as_ref())
        }
        LogicalPlan::Window(window) => {
            contains_unsupported_join_topn_wrapper(window.input.as_ref())
        }
        LogicalPlan::Join(join) => {
            contains_unsupported_join_topn_wrapper(join.left.as_ref())
                || contains_unsupported_join_topn_wrapper(join.right.as_ref())
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
