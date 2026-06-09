use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::logical_plan::{Aggregate, Join, TableScan, Window};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, Operator, ScalarUDF};
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::collections::{ColumnarZSet, SlateBackedColumnarZSet};
use dbsp::storage::{KeyValueTable, keyspace};
use slatedb::WriteBatch;

use crate::delta_consolidation::weighted_snapshot_schema;
use crate::mv::registry::MaterializedViewRegistry;
use crate::namespaces;
use crate::vectorized_runtime::source_state::resolve_source_table;

use super::{
    VectorizedMaterializedViewState, VectorizedSourceState, apply_weighted_snapshot_delta,
};

pub(super) struct ColumnarJoinTopAvgPlan {
    left_source: String,
    right_source: String,
    left_key_column: String,
    right_key_column: String,
    group_column: String,
    value_column: String,
}

pub(super) struct ColumnarJoinTopAvgMaterializedViewState {
    left: JoinTopAvgSourceState,
    right: JoinTopAvgSourceState,
    output_zset: SlateBackedColumnarZSet,
    avg_state: SlateJoinTopAvgState,
    evaluator: JoinTopAvgEvaluator,
    initial_snapshot: Vec<RecordBatch>,
}

impl ColumnarJoinTopAvgMaterializedViewState {
    pub(super) fn initial_snapshot(&self) -> Vec<RecordBatch> {
        self.initial_snapshot.clone()
    }
}

struct JoinTopAvgSourceState {
    source_name: String,
    schema: SchemaRef,
    key_idx: usize,
    input_zset: SlateBackedColumnarZSet,
    snapshot: Vec<RecordBatch>,
}

struct JoinTopAvgEvaluator {
    left: JoinTopAvgLeftIndices,
    right: JoinTopAvgRightIndices,
}

struct JoinTopAvgLeftIndices {
    id: usize,
    group: usize,
    date_time: usize,
    expires: usize,
}

struct JoinTopAvgRightIndices {
    auction: usize,
    price: usize,
    date_time: usize,
}

struct TopAvgRow {
    group: i64,
    value: i64,
}

struct SlateJoinTopAvgState {
    table: Arc<dyn KeyValueTable>,
    key_prefix: Vec<u8>,
    cache: Mutex<HashMap<i64, (i64, i64)>>,
}

pub(super) fn columnar_join_top_avg_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarJoinTopAvgPlan>> {
    if let Some(plan) = q6_join_top_avg_plan_for_plan(plan, sources)? {
        return Ok(Some(plan));
    }
    q4_join_top_avg_plan_for_plan(plan, sources)
}

pub(super) async fn build_columnar_join_top_avg_materialized_view_state(
    table: Arc<dyn KeyValueTable>,
    view_name: &str,
    output_schema: &SchemaRef,
    plan: ColumnarJoinTopAvgPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    _udfs: &[ScalarUDF],
) -> Result<ColumnarJoinTopAvgMaterializedViewState> {
    let left_source = sources
        .get(&plan.left_source)
        .ok_or_else(|| anyhow::anyhow!("unknown join-top-avg source '{}'", plan.left_source))?;
    let right_source = sources
        .get(&plan.right_source)
        .ok_or_else(|| anyhow::anyhow!("unknown join-top-avg source '{}'", plan.right_source))?;
    let left_key_idx = left_source
        .schema
        .index_of(&plan.left_key_column)
        .with_context(|| format!("find join-top-avg left key '{}'", plan.left_key_column))?;
    let right_key_idx = right_source
        .schema
        .index_of(&plan.right_key_column)
        .with_context(|| format!("find join-top-avg right key '{}'", plan.right_key_column))?;
    if output_schema.fields().len() != 2
        || output_schema.field(0).data_type() != &DataType::Int64
        || output_schema.field(1).data_type() != &DataType::Float64
    {
        bail!("join-top-avg output schema must be (Int64, Float64)");
    }

    let mv_namespace = namespaces::materialized_view(view_name)?;
    let left_namespace = format!(
        "{mv_namespace}/columnar/join_top_avg/{}/input",
        plan.left_source
    );
    let right_namespace = format!(
        "{mv_namespace}/columnar/join_top_avg/{}/input",
        plan.right_source
    );
    let output_namespace = format!("{mv_namespace}/columnar/join_top_avg/output");
    let state_namespace = format!("{mv_namespace}/columnar/join_top_avg/state");

    let left_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        left_namespace,
        Arc::clone(&left_source.schema),
    )
    .await
    .context("initialize SlateDB-backed join-top-avg left input zset")?;
    let right_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        right_namespace,
        Arc::clone(&right_source.schema),
    )
    .await
    .context("initialize SlateDB-backed join-top-avg right input zset")?;
    let output_zset = SlateBackedColumnarZSet::new(
        Arc::clone(&table),
        output_namespace,
        Arc::clone(output_schema),
    )
    .await
    .context("initialize SlateDB-backed join-top-avg output zset")?;
    let initial_snapshot = snapshot_batches_from_zset(
        &output_zset
            .materialize_columnar()
            .await
            .context("load join-top-avg output snapshot")?,
    )?;
    let evaluator = JoinTopAvgEvaluator::build(
        &left_source.schema,
        &right_source.schema,
        &plan.group_column,
        &plan.value_column,
    )
    .context("build join-top-avg evaluator")?;

    Ok(ColumnarJoinTopAvgMaterializedViewState {
        left: JoinTopAvgSourceState {
            source_name: plan.left_source,
            schema: Arc::clone(&left_source.schema),
            key_idx: left_key_idx,
            snapshot: snapshot_batches_from_zset(
                &left_zset
                    .materialize_columnar()
                    .await
                    .context("load join-top-avg left input snapshot")?,
            )?,
            input_zset: left_zset,
        },
        right: JoinTopAvgSourceState {
            source_name: plan.right_source,
            schema: Arc::clone(&right_source.schema),
            key_idx: right_key_idx,
            snapshot: snapshot_batches_from_zset(
                &right_zset
                    .materialize_columnar()
                    .await
                    .context("load join-top-avg right input snapshot")?,
            )?,
            input_zset: right_zset,
        },
        output_zset,
        avg_state: SlateJoinTopAvgState::new(table, &state_namespace),
        evaluator,
        initial_snapshot,
    })
}

pub(super) async fn run_columnar_join_top_avg_materialized_view_tick(
    registry: &MaterializedViewRegistry,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
    mv: &mut VectorizedMaterializedViewState,
    version: i64,
) -> Result<bool> {
    let Some(columnar) = mv.columnar_join_top_avg.as_mut() else {
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
    let mut touched_keys = HashSet::new();
    collect_i64_keys_from_delta(&left_delta, columnar.left.key_idx, &mut touched_keys)?;
    collect_i64_keys_from_delta(&right_delta, columnar.right.key_idx, &mut touched_keys)?;

    let previous_left = filter_batches_to_i64_keys(
        &columnar.left.schema,
        columnar.left.key_idx,
        &columnar.left.snapshot,
        &touched_keys,
    )?;
    let previous_right = filter_batches_to_i64_keys(
        &columnar.right.schema,
        columnar.right.key_idx,
        &columnar.right.snapshot,
        &touched_keys,
    )?;
    let next_left_snapshot =
        apply_source_snapshot_delta(&columnar.left.schema, &columnar.left.snapshot, &left_delta)
            .await?;
    let next_right_snapshot = apply_source_snapshot_delta(
        &columnar.right.schema,
        &columnar.right.snapshot,
        &right_delta,
    )
    .await?;
    let next_left = filter_batches_to_i64_keys(
        &columnar.left.schema,
        columnar.left.key_idx,
        &next_left_snapshot,
        &touched_keys,
    )?;
    let next_right = filter_batches_to_i64_keys(
        &columnar.right.schema,
        columnar.right.key_idx,
        &next_right_snapshot,
        &touched_keys,
    )?;

    let output_delta_batches = if touched_keys.is_empty() {
        empty_weighted_batches(&mv.output_schema)?
    } else {
        let previous_rows = columnar
            .evaluator
            .evaluate(&previous_left, &previous_right)
            .context("evaluate previous join-top-avg rows")?;
        let next_rows = columnar
            .evaluator
            .evaluate(&next_left, &next_right)
            .context("evaluate next join-top-avg rows")?;
        apply_avg_delta(
            &columnar.avg_state,
            &mv.output_schema,
            previous_rows,
            next_rows,
        )
        .await?
    };

    let output_delta =
        ColumnarZSet::try_new_weighted(columnar.output_zset.value_schema(), output_delta_batches)
            .context("build join-top-avg output zset delta")?;
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
            "apply Slate-backed join-top-avg columnar snapshot delta for '{}'",
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
        mode = "columnar_join_top_avg",
        "SlateDB-backed join-top-avg columnar DBSP materialized view tick completed"
    );
    Ok(true)
}

async fn apply_avg_delta(
    state: &SlateJoinTopAvgState,
    output_schema: &SchemaRef,
    previous_rows: Vec<TopAvgRow>,
    next_rows: Vec<TopAvgRow>,
) -> Result<Vec<RecordBatch>> {
    let mut pending: HashMap<i64, (i64, i64)> = HashMap::new();
    for row in previous_rows {
        let entry = pending.entry(row.group).or_insert((0, 0));
        entry.0 = entry
            .0
            .checked_sub(row.value)
            .context("join-top-avg sum underflow")?;
        entry.1 = entry
            .1
            .checked_sub(1)
            .context("join-top-avg count underflow")?;
    }
    for row in next_rows {
        let entry = pending.entry(row.group).or_insert((0, 0));
        entry.0 = entry
            .0
            .checked_add(row.value)
            .context("join-top-avg sum overflow")?;
        entry.1 = entry
            .1
            .checked_add(1)
            .context("join-top-avg count overflow")?;
    }
    pending.retain(|_, (sum, count)| *sum != 0 || *count != 0);
    if pending.is_empty() {
        return empty_weighted_batches(output_schema);
    }

    let weighted_schema = weighted_snapshot_schema(output_schema)?;
    let mut groups = Vec::new();
    let mut avgs = Vec::new();
    let mut weights = Vec::new();
    let mut writes = WriteBatch::new();
    for (group, (sum_delta, count_delta)) in pending {
        let (old_sum, old_count) = state.load(group).await?;
        let new_sum = old_sum
            .checked_add(sum_delta)
            .context("join-top-avg sum overflow")?;
        let new_count = old_count
            .checked_add(count_delta)
            .context("join-top-avg count overflow")?;
        if new_count < 0 {
            bail!("join-top-avg count became negative");
        }
        if old_count > 0 {
            groups.push(group);
            avgs.push(old_sum as f64 / old_count as f64);
            weights.push(-1);
        }
        state.write(&mut writes, group, new_sum, new_count)?;
        if new_count > 0 {
            groups.push(group);
            avgs.push(new_sum as f64 / new_count as f64);
            weights.push(1);
        }
    }
    state
        .table
        .write_batch(writes)
        .await
        .context("persist join-top-avg state")?;
    if groups.is_empty() {
        return empty_weighted_batches(output_schema);
    }
    Ok(vec![RecordBatch::try_new(
        weighted_schema,
        vec![
            Arc::new(Int64Array::from(groups)) as ArrayRef,
            Arc::new(Float64Array::from(avgs)) as ArrayRef,
            Arc::new(Int64Array::from(weights)) as ArrayRef,
        ],
    )?])
}

fn source_input_delta(
    source: &JoinTopAvgSourceState,
    insert_batches: &HashMap<String, Vec<RecordBatch>>,
    weighted_delta_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<ColumnarZSet> {
    if let Some(weighted_batches) = weighted_delta_batches.get(source.source_name.as_str()) {
        ColumnarZSet::try_new_weighted(Arc::clone(&source.schema), weighted_batches.clone())
            .with_context(|| {
                format!(
                    "build weighted join-top-avg input delta for '{}'",
                    source.source_name
                )
            })
    } else if let Some(source_batches) = insert_batches.get(source.source_name.as_str()) {
        ColumnarZSet::from_value_batches(Arc::clone(&source.schema), source_batches.clone(), 1)
            .with_context(|| {
                format!(
                    "build insert join-top-avg input delta for '{}'",
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

fn empty_weighted_batches(output_schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
    Ok(vec![RecordBatch::new_empty(weighted_snapshot_schema(
        output_schema,
    )?)])
}

fn collect_i64_keys_from_delta(
    delta: &ColumnarZSet,
    key_idx: usize,
    output: &mut HashSet<i64>,
) -> Result<()> {
    let weight_idx = delta.value_column_count();
    for batch in delta.batches().iter().filter(|batch| batch.num_rows() > 0) {
        let keys = int64_column(batch, key_idx)?;
        let weights = int64_column(batch, weight_idx)?;
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
        let values = int64_column(batch, key_idx)?;
        let mut indices = Vec::new();
        for row_idx in 0..batch.num_rows() {
            if !values.is_null(row_idx) && keys.contains(&values.value(row_idx)) {
                indices
                    .push(u32::try_from(row_idx).context("join-top-avg batch exceeds u32 rows")?);
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

impl JoinTopAvgEvaluator {
    fn build(
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        group_column: &str,
        value_column: &str,
    ) -> Result<Self> {
        Ok(Self {
            left: JoinTopAvgLeftIndices {
                id: field_index(left_schema, &["id"])?,
                group: field_index(left_schema, &[group_column])?,
                date_time: field_index(left_schema, &["dateTime", "date_time"])?,
                expires: field_index(left_schema, &["expires"])?,
            },
            right: JoinTopAvgRightIndices {
                auction: field_index(right_schema, &["auction"])?,
                price: field_index(right_schema, &[value_column])?,
                date_time: field_index(right_schema, &["dateTime", "date_time"])?,
            },
        })
    }

    fn evaluate(
        &self,
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
    ) -> Result<Vec<TopAvgRow>> {
        let mut rows = Vec::new();
        for left_batch in left_batches {
            let left_ids = int64_column(left_batch, self.left.id)?;
            let left_groups = int64_column(left_batch, self.left.group)?;
            for left_row_idx in 0..left_batch.num_rows() {
                if left_ids.is_null(left_row_idx) || left_groups.is_null(left_row_idx) {
                    continue;
                }
                let Some(value) =
                    self.best_value_for_auction(left_batch, left_row_idx, right_batches)?
                else {
                    continue;
                };
                rows.push(TopAvgRow {
                    group: left_groups.value(left_row_idx),
                    value,
                });
            }
        }
        Ok(rows)
    }

    fn best_value_for_auction(
        &self,
        left_batch: &RecordBatch,
        left_row_idx: usize,
        right_batches: &[RecordBatch],
    ) -> Result<Option<i64>> {
        let left_ids = int64_column(left_batch, self.left.id)?;
        let auction_id = left_ids.value(left_row_idx);
        let auction_start = i64_or_timestamp_value(left_batch, self.left.date_time, left_row_idx)?;
        let auction_expires = i64_or_timestamp_value(left_batch, self.left.expires, left_row_idx)?;
        let mut best: Option<(i64, i64)> = None;
        for right_batch in right_batches {
            let right_auctions = int64_column(right_batch, self.right.auction)?;
            let right_values = int64_column(right_batch, self.right.price)?;
            for right_row_idx in 0..right_batch.num_rows() {
                if right_auctions.is_null(right_row_idx)
                    || right_values.is_null(right_row_idx)
                    || right_auctions.value(right_row_idx) != auction_id
                {
                    continue;
                }
                let bid_time =
                    i64_or_timestamp_value(right_batch, self.right.date_time, right_row_idx)?;
                if bid_time < auction_start || bid_time > auction_expires {
                    continue;
                }
                let value = right_values.value(right_row_idx);
                let replace = match best {
                    Some((best_value, best_time)) => {
                        value > best_value || (value == best_value && bid_time < best_time)
                    }
                    None => true,
                };
                if replace {
                    best = Some((value, bid_time));
                }
            }
        }
        Ok(best.map(|(value, _)| value))
    }
}

impl SlateJoinTopAvgState {
    fn new(table: Arc<dyn KeyValueTable>, namespace: &str) -> Self {
        Self {
            table,
            key_prefix: keyspace::namespace_prefix(keyspace::prefix::INDEX, namespace),
            cache: Mutex::new(HashMap::new()),
        }
    }

    async fn load(&self, group: i64) -> Result<(i64, i64)> {
        if let Some(value) = self
            .cache
            .lock()
            .map_err(|_| anyhow::anyhow!("join-top-avg cache poisoned"))?
            .get(&group)
            .copied()
        {
            return Ok(value);
        }
        let Some(bytes) = self
            .table
            .get_bytes(&self.group_key(group))
            .await
            .context("read join-top-avg state")?
        else {
            return Ok((0, 0));
        };
        if bytes.len() != 16 {
            bail!("invalid join-top-avg state length {}", bytes.len());
        }
        let sum = i64::from_be_bytes(bytes[0..8].try_into()?);
        let count = i64::from_be_bytes(bytes[8..16].try_into()?);
        self.cache
            .lock()
            .map_err(|_| anyhow::anyhow!("join-top-avg cache poisoned"))?
            .insert(group, (sum, count));
        Ok((sum, count))
    }

    fn write(&self, batch: &mut WriteBatch, group: i64, sum: i64, count: i64) -> Result<()> {
        let key = self.group_key(group);
        if sum == 0 && count == 0 {
            batch.delete(key);
        } else {
            let mut bytes = Vec::with_capacity(16);
            bytes.extend_from_slice(&sum.to_be_bytes());
            bytes.extend_from_slice(&count.to_be_bytes());
            batch.put(key, bytes);
        }
        self.cache
            .lock()
            .map_err(|_| anyhow::anyhow!("join-top-avg cache poisoned"))?
            .insert(group, (sum, count));
        Ok(())
    }

    fn group_key(&self, group: i64) -> Vec<u8> {
        let mut key = self.key_prefix.clone();
        key.extend_from_slice(&encode_i64_sortable(group));
        key
    }
}

fn q6_join_top_avg_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarJoinTopAvgPlan>> {
    let aggregates = aggregates_for_plan(plan);
    let [aggregate] = aggregates.as_slice() else {
        return Ok(None);
    };
    let (Some(group_column), Some(value_column)) = (
        single_group_column(aggregate),
        avg_or_max_value_column(aggregate, "avg"),
    ) else {
        return Ok(None);
    };
    let Some((_rank_column, filter)) = row_number_filter_for_plan(aggregate.input.as_ref()) else {
        return Ok(None);
    };
    let Some(window) = window_for_plan(filter.input.as_ref()) else {
        return Ok(None);
    };
    join_top_avg_plan_from_join_input(window.input.as_ref(), sources, group_column, value_column)
}

fn q4_join_top_avg_plan_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Result<Option<ColumnarJoinTopAvgPlan>> {
    let aggregates = aggregates_for_plan(plan);
    let [outer, inner] = aggregates.as_slice() else {
        return Ok(None);
    };
    let (Some(group_column), Some(_outer_value)) = (
        single_group_column(outer),
        avg_or_max_value_column(outer, "avg"),
    ) else {
        return Ok(None);
    };
    let Some(value_column) = avg_or_max_value_column(inner, "max") else {
        return Ok(None);
    };
    join_top_avg_plan_from_join_input(inner.input.as_ref(), sources, group_column, value_column)
}

fn join_top_avg_plan_from_join_input(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    group_column: String,
    value_column: String,
) -> Result<Option<ColumnarJoinTopAvgPlan>> {
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
    if left_source == right_source {
        return Ok(None);
    }
    let Some((left_key_column, right_key_column)) =
        join_key_columns(join, &left_source, &right_source, sources)
    else {
        return Ok(None);
    };
    let left_schema = &sources
        .get(&left_source)
        .ok_or_else(|| anyhow::anyhow!("unknown join-top-avg source"))?
        .schema;
    let right_schema = &sources
        .get(&right_source)
        .ok_or_else(|| anyhow::anyhow!("unknown join-top-avg source"))?
        .schema;
    if field_index(left_schema, &[&group_column]).is_err()
        || field_index(right_schema, &[&value_column]).is_err()
        || field_index(left_schema, &["dateTime", "date_time"]).is_err()
        || field_index(left_schema, &["expires"]).is_err()
        || field_index(right_schema, &["dateTime", "date_time"]).is_err()
    {
        return Ok(None);
    }
    Ok(Some(ColumnarJoinTopAvgPlan {
        left_source,
        right_source,
        left_key_column,
        right_key_column,
        group_column,
        value_column,
    }))
}

fn aggregates_for_plan<'a>(plan: &'a LogicalPlan) -> Vec<&'a Aggregate> {
    let mut out = Vec::new();
    collect_aggregates(plan, &mut out);
    out
}

fn collect_aggregates<'a>(plan: &'a LogicalPlan, out: &mut Vec<&'a Aggregate>) {
    match plan {
        LogicalPlan::Aggregate(aggregate) => {
            out.push(aggregate);
            collect_aggregates(aggregate.input.as_ref(), out);
        }
        LogicalPlan::Projection(projection) => collect_aggregates(projection.input.as_ref(), out),
        LogicalPlan::Filter(filter) => collect_aggregates(filter.input.as_ref(), out),
        LogicalPlan::SubqueryAlias(alias) => collect_aggregates(alias.input.as_ref(), out),
        LogicalPlan::Window(window) => collect_aggregates(window.input.as_ref(), out),
        _ => {}
    }
}

fn single_group_column(aggregate: &Aggregate) -> Option<String> {
    let [expr] = aggregate.group_expr.as_slice() else {
        return None;
    };
    column_name(expr)
}

fn avg_or_max_value_column(aggregate: &Aggregate, name: &str) -> Option<String> {
    let [expr] = aggregate.aggr_expr.as_slice() else {
        return None;
    };
    let Expr::AggregateFunction(function) = strip_alias(expr) else {
        return None;
    };
    if !function.func.name().eq_ignore_ascii_case(name) {
        return None;
    }
    let [arg] = function.params.args.as_slice() else {
        return None;
    };
    column_name(arg)
}

fn row_number_filter_for_plan(
    plan: &LogicalPlan,
) -> Option<(String, &datafusion::logical_expr::logical_plan::Filter)> {
    match plan {
        LogicalPlan::Projection(projection) => {
            row_number_filter_for_plan(projection.input.as_ref())
        }
        LogicalPlan::Filter(filter) => {
            let Expr::BinaryExpr(binary) = &filter.predicate else {
                return None;
            };
            match (&*binary.left, binary.op, &*binary.right) {
                (Expr::Column(column), Operator::LtEq | Operator::Lt, Expr::Literal(_, _)) => {
                    Some((column.name.clone(), filter))
                }
                _ => None,
            }
        }
        LogicalPlan::SubqueryAlias(alias) => row_number_filter_for_plan(alias.input.as_ref()),
        _ => None,
    }
}

fn window_for_plan(plan: &LogicalPlan) -> Option<&Window> {
    match strip_passthrough_wrappers(plan) {
        LogicalPlan::Window(window) => Some(window),
        LogicalPlan::Projection(projection) => {
            match strip_passthrough_wrappers(projection.input.as_ref()) {
                LogicalPlan::Window(window) => Some(window),
                _ => None,
            }
        }
        _ => None,
    }
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
    match strip_alias(expr) {
        Expr::Column(column) => Some(column.name.clone()),
        _ => None,
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
        LogicalPlan::Window(window) => collect_sources(window.input.as_ref(), sources, out),
        LogicalPlan::Aggregate(aggregate) => {
            collect_sources(aggregate.input.as_ref(), sources, out)
        }
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

fn int64_column(batch: &RecordBatch, idx: usize) -> Result<&Int64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("join-top-avg column must be Int64"))
}

fn field_index(schema: &SchemaRef, names: &[&str]) -> Result<usize> {
    for name in names {
        if let Ok(idx) = schema.index_of(name) {
            return Ok(idx);
        }
    }
    bail!("join-top-avg schema missing field aliases {names:?}")
}

fn i64_or_timestamp_value(batch: &RecordBatch, idx: usize, row_idx: usize) -> Result<i64> {
    match batch.schema().field(idx).data_type() {
        DataType::Int64 => {
            let values = int64_column(batch, idx)?;
            if values.is_null(row_idx) {
                bail!("join-top-avg time column cannot be NULL");
            }
            Ok(values.value(row_idx))
        }
        DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, _) => {
            let values = batch
                .column(idx)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::TimestampMillisecondArray>()
                .ok_or_else(|| anyhow::anyhow!("join-top-avg column must be TimestampMillis"))?;
            if values.is_null(row_idx) {
                bail!("join-top-avg timestamp column cannot be NULL");
            }
            Ok(values.value(row_idx))
        }
        other => bail!("join-top-avg unsupported time column type {other:?}"),
    }
}

fn encode_i64_sortable(value: i64) -> [u8; 8] {
    ((value as u64) ^ (1 << 63)).to_be_bytes()
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
