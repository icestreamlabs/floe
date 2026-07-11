use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow_array::builder::Int64Builder;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow_ord::sort::sort_to_indices;
use arrow_schema::SortOptions;
use arrow_select::concat::{concat, concat_batches};
use arrow_select::take::take;

use crate::collections::columnar_zset::i64_column;
use crate::collections::{ColumnarI64ZSet, SlateBackedColumnarI64ZSet};
use crate::handles::ZSetHandle;
use crate::storage::KeyValueTable;

#[derive(Debug)]
pub struct ColumnarCountByKeyOp {
    state_snapshot: ColumnarI64ZSet,
    state_deltas: ColumnarI64ZSet,
    last_output_delta: ColumnarI64ZSet,
}

pub struct SlateBackedColumnarCountByKeyOp {
    state_snapshot: ColumnarI64ZSet,
    state_zset: SlateBackedColumnarI64ZSet,
    output_zset: SlateBackedColumnarI64ZSet,
    last_output_delta: ColumnarI64ZSet,
    last_output_handle: Option<ZSetHandle>,
}

struct CountUpdate {
    state_snapshot: ColumnarI64ZSet,
    state_delta: ColumnarI64ZSet,
    output_delta: ColumnarI64ZSet,
}

struct GroupedI64Weights {
    keys: Int64Array,
    weights: Int64Array,
}

struct StateColumns {
    keys: Int64Array,
    counts: Int64Array,
}

struct CountRowsBuilder {
    keys: Int64Builder,
    counts: Int64Builder,
    weights: Int64Builder,
}

impl CountRowsBuilder {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            keys: Int64Builder::with_capacity(capacity),
            counts: Int64Builder::with_capacity(capacity),
            weights: Int64Builder::with_capacity(capacity),
        }
    }

    fn append(&mut self, key: i64, count: i64, weight: i64) {
        self.keys.append_value(key);
        self.counts.append_value(count);
        self.weights.append_value(weight);
    }

    fn finish(mut self) -> Result<ColumnarI64ZSet> {
        count_zset_from_arrays(
            self.keys.finish(),
            self.counts.finish(),
            self.weights.finish(),
        )
    }
}

impl SlateBackedColumnarCountByKeyOp {
    pub async fn new(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Result<Self> {
        let namespace = namespace.into();
        let state_zset = SlateBackedColumnarI64ZSet::new(
            Arc::clone(&table),
            format!("{namespace}/state"),
            &["key", "count"],
        )
        .await?;
        let state_snapshot = state_zset
            .materialize_columnar()
            .await
            .context("load columnar count state snapshot")?;
        Ok(Self {
            state_snapshot,
            state_zset,
            output_zset: SlateBackedColumnarI64ZSet::new(
                table,
                format!("{namespace}/output"),
                &["key", "count"],
            )
            .await?,
            last_output_delta: empty_count_zset(),
            last_output_handle: None,
        })
    }

    pub async fn apply_delta(&mut self, input: &ColumnarI64ZSet) -> Result<ColumnarI64ZSet> {
        let update = compute_count_by_key_update(&self.state_snapshot, input)?;
        if !update.state_delta.is_empty() {
            let base = self
                .state_zset
                .current_handle()
                .map(|handle| handle.version);
            self.state_zset
                .create_version(&update.state_delta, base)
                .await?;
        }
        let output_handle = if update.output_delta.is_empty() {
            Some(self.output_zset.handle_for_version(0))
        } else {
            Some(
                self.output_zset
                    .create_version(&update.output_delta, None)
                    .await?
                    .context("columnar count produced non-empty output without a version")?,
            )
        };
        self.state_snapshot = update.state_snapshot;
        self.last_output_delta = update.output_delta.clone();
        self.last_output_handle = output_handle;
        Ok(update.output_delta)
    }

    pub fn last_output_delta(&self) -> &ColumnarI64ZSet {
        &self.last_output_delta
    }

    pub fn last_output_handle(&self) -> Option<&ZSetHandle> {
        self.last_output_handle.as_ref()
    }

    pub fn state_snapshot(&self) -> &ColumnarI64ZSet {
        &self.state_snapshot
    }

    pub async fn read_output_delta(&self, handle: &ZSetHandle) -> Result<ColumnarI64ZSet> {
        self.output_zset.read_delta(handle).await
    }
}

impl Default for ColumnarCountByKeyOp {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnarCountByKeyOp {
    pub fn new() -> Self {
        Self {
            state_snapshot: empty_count_zset(),
            state_deltas: empty_count_zset(),
            last_output_delta: empty_count_zset(),
        }
    }

    pub fn apply_delta(&mut self, input: &ColumnarI64ZSet) -> Result<ColumnarI64ZSet> {
        let update = compute_count_by_key_update(&self.state_snapshot, input)?;
        if !update.state_delta.is_empty() {
            self.state_deltas.extend(update.state_delta)?;
        }
        self.state_snapshot = update.state_snapshot;
        self.last_output_delta = update.output_delta.clone();
        Ok(update.output_delta)
    }

    pub fn state_deltas(&self) -> &ColumnarI64ZSet {
        &self.state_deltas
    }

    pub fn last_output_delta(&self) -> &ColumnarI64ZSet {
        &self.last_output_delta
    }

    pub fn state_snapshot(&self) -> &ColumnarI64ZSet {
        &self.state_snapshot
    }
}

fn compute_count_by_key_update(
    state_snapshot: &ColumnarI64ZSet,
    input: &ColumnarI64ZSet,
) -> Result<CountUpdate> {
    let grouped = group_input_delta_arrow(input)?;
    if grouped.keys.is_empty() {
        return Ok(CountUpdate {
            state_snapshot: state_snapshot.clone(),
            state_delta: empty_count_zset(),
            output_delta: empty_count_zset(),
        });
    }

    let state = state_columns(state_snapshot)?;
    let state_len = state.keys.len();
    let grouped_len = grouped.keys.len();
    let zero_state_deltas = repeated_i64(0, state_len);
    let zero_delta_old_counts = repeated_i64(0, grouped_len);

    let combined_keys = concat_i64_arrays(&[&state.keys, &grouped.keys], "concat count keys")?;
    let combined_old_counts = concat_i64_arrays(
        &[&state.counts, &zero_delta_old_counts],
        "concat count old state",
    )?;
    let combined_deltas = concat_i64_arrays(
        &[&zero_state_deltas, &grouped.weights],
        "concat count deltas",
    )?;

    let indices = sort_to_indices(&combined_keys, Some(SortOptions::new(false, false)), None)
        .context("sort count keys")?;
    let sorted_keys_ref = take(&combined_keys, &indices, None).context("take sorted count keys")?;
    let sorted_old_counts_ref =
        take(&combined_old_counts, &indices, None).context("take sorted old counts")?;
    let sorted_deltas_ref =
        take(&combined_deltas, &indices, None).context("take sorted count deltas")?;
    let sorted_keys = array_ref_as_i64(&sorted_keys_ref, "sorted count keys")?;
    let sorted_old_counts = array_ref_as_i64(&sorted_old_counts_ref, "sorted old counts")?;
    let sorted_deltas = array_ref_as_i64(&sorted_deltas_ref, "sorted count deltas")?;

    count_update_from_sorted_combined_kernel(sorted_keys, sorted_old_counts, sorted_deltas)
}

fn group_input_delta_arrow(input: &ColumnarI64ZSet) -> Result<GroupedI64Weights> {
    if input.value_column_count() != 1 {
        bail!(
            "columnar count expects one key column, found {}",
            input.value_column_count()
        );
    }
    if input.is_empty() {
        return Ok(GroupedI64Weights {
            keys: empty_i64_array(),
            weights: empty_i64_array(),
        });
    }

    let batch = concat_batches(&input.schema(), input.batches())
        .context("concat columnar count input batches")?;
    let keys = i64_column(&batch, 0)?;
    let weights = i64_column(&batch, input.value_column_count())?;
    let indices = sort_to_indices(keys, Some(SortOptions::new(false, false)), None)
        .context("sort input delta keys")?;
    let sorted_keys_ref = take(keys, &indices, None).context("take sorted input keys")?;
    let sorted_weights_ref = take(weights, &indices, None).context("take sorted input weights")?;
    let sorted_keys = array_ref_as_i64(&sorted_keys_ref, "sorted input keys")?;
    let sorted_weights = array_ref_as_i64(&sorted_weights_ref, "sorted input weights")?;

    group_sorted_i64_weights_kernel(sorted_keys, sorted_weights)
}

fn state_columns(state_snapshot: &ColumnarI64ZSet) -> Result<StateColumns> {
    if state_snapshot.is_empty() {
        return Ok(StateColumns {
            keys: empty_i64_array(),
            counts: empty_i64_array(),
        });
    }
    if state_snapshot.value_column_count() != 2 {
        bail!(
            "columnar count state snapshot expects key,count columns, found {}",
            state_snapshot.value_column_count()
        );
    }
    let batch = concat_batches(&state_snapshot.schema(), state_snapshot.batches())
        .context("concat columnar count state snapshot")?;
    Ok(StateColumns {
        keys: i64_column(&batch, 0)?.clone(),
        counts: i64_column(&batch, 1)?.clone(),
    })
}

fn group_sorted_i64_weights_kernel(
    sorted_keys: &Int64Array,
    sorted_weights: &Int64Array,
) -> Result<GroupedI64Weights> {
    if sorted_keys.len() != sorted_weights.len() {
        bail!("sorted key and weight arrays have mismatched lengths");
    }
    if sorted_keys.is_empty() {
        return Ok(GroupedI64Weights {
            keys: empty_i64_array(),
            weights: empty_i64_array(),
        });
    }

    let mut grouped_keys = Int64Builder::with_capacity(sorted_keys.len());
    let mut grouped_weights = Int64Builder::with_capacity(sorted_keys.len());
    let mut current_key = sorted_keys.value(0);
    let mut current_weight = 0_i64;

    for row_idx in 0..sorted_keys.len() {
        let key = sorted_keys.value(row_idx);
        if key != current_key {
            append_grouped_weight(
                &mut grouped_keys,
                &mut grouped_weights,
                current_key,
                current_weight,
            );
            current_key = key;
            current_weight = 0;
        }
        current_weight = current_weight.saturating_add(sorted_weights.value(row_idx));
    }
    append_grouped_weight(
        &mut grouped_keys,
        &mut grouped_weights,
        current_key,
        current_weight,
    );

    Ok(GroupedI64Weights {
        keys: grouped_keys.finish(),
        weights: grouped_weights.finish(),
    })
}

fn count_update_from_sorted_combined_kernel(
    sorted_keys: &Int64Array,
    sorted_old_counts: &Int64Array,
    sorted_deltas: &Int64Array,
) -> Result<CountUpdate> {
    if sorted_keys.len() != sorted_old_counts.len() || sorted_keys.len() != sorted_deltas.len() {
        bail!("sorted count kernel arrays have mismatched lengths");
    }
    if sorted_keys.is_empty() {
        return Ok(CountUpdate {
            state_snapshot: empty_count_zset(),
            state_delta: empty_count_zset(),
            output_delta: empty_count_zset(),
        });
    }

    let group_capacity = sorted_keys.len();
    let mut snapshot = CountRowsBuilder::with_capacity(group_capacity);
    let mut state = CountRowsBuilder::with_capacity(group_capacity.saturating_mul(2));
    let mut output = CountRowsBuilder::with_capacity(group_capacity.saturating_mul(2));

    let mut current_key = sorted_keys.value(0);
    let mut old = 0_i64;
    let mut delta = 0_i64;

    for row_idx in 0..sorted_keys.len() {
        let key = sorted_keys.value(row_idx);
        if key != current_key {
            append_count_update_group(
                &mut snapshot,
                &mut state,
                &mut output,
                current_key,
                old,
                delta,
            );
            current_key = key;
            old = 0;
            delta = 0;
        }
        old = old.saturating_add(sorted_old_counts.value(row_idx));
        delta = delta.saturating_add(sorted_deltas.value(row_idx));
    }
    append_count_update_group(
        &mut snapshot,
        &mut state,
        &mut output,
        current_key,
        old,
        delta,
    );

    Ok(CountUpdate {
        state_snapshot: snapshot.finish()?,
        state_delta: state.finish()?,
        output_delta: output.finish()?,
    })
}

fn append_grouped_weight(
    keys: &mut Int64Builder,
    weights: &mut Int64Builder,
    key: i64,
    weight: i64,
) {
    if weight == 0 {
        return;
    }
    keys.append_value(key);
    weights.append_value(weight);
}

fn append_count_update_group(
    snapshot: &mut CountRowsBuilder,
    state: &mut CountRowsBuilder,
    output: &mut CountRowsBuilder,
    key: i64,
    old: i64,
    delta: i64,
) {
    let new = old.saturating_add(delta);

    if old != new {
        if old != 0 {
            state.append(key, old, -1);
            output.append(key, old, -1);
        }
        if new != 0 {
            state.append(key, new, 1);
            output.append(key, new, 1);
        }
    }

    if new != 0 {
        snapshot.append(key, new, 1);
    }
}

fn concat_i64_arrays(arrays: &[&Int64Array], context: &'static str) -> Result<Int64Array> {
    let arrays = arrays
        .iter()
        .map(|array| *array as &dyn Array)
        .collect::<Vec<_>>();
    let array_ref = concat(&arrays).context(context)?;
    Ok(array_ref_as_i64(&array_ref, context)?.clone())
}

fn count_zset_from_arrays(
    keys: Int64Array,
    counts: Int64Array,
    weights: Int64Array,
) -> Result<ColumnarI64ZSet> {
    if keys.is_empty() {
        return Ok(empty_count_zset());
    }
    if keys.len() != counts.len() || keys.len() != weights.len() {
        bail!("columnar count arrays have mismatched lengths");
    }
    let schema = empty_count_zset().schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(keys) as ArrayRef,
            Arc::new(counts) as ArrayRef,
            Arc::new(weights) as ArrayRef,
        ],
    )
    .context("build columnar count zset batch")?;
    ColumnarI64ZSet::try_new(schema, 2, vec![batch])
}

fn repeated_i64(value: i64, len: usize) -> Int64Array {
    let mut builder = Int64Builder::with_capacity(len);
    builder.append_value_n(value, len);
    builder.finish()
}

fn empty_i64_array() -> Int64Array {
    Int64Array::from(Vec::<i64>::new())
}

fn empty_count_zset() -> ColumnarI64ZSet {
    ColumnarI64ZSet::empty(&["key", "count"])
}

fn array_ref_as_i64<'a>(array: &'a ArrayRef, context: &'static str) -> Result<&'a Int64Array> {
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .with_context(|| format!("{context}: expected Int64Array"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use slatedb::Db;

    use crate::storage::SlateTable;

    fn one_column_zset(keys: Vec<i64>, weights: Vec<i64>) -> ColumnarI64ZSet {
        ColumnarI64ZSet::from_i64_columns(&["key"], &[keys], weights).expect("zset")
    }

    async fn build_table(name: &str) -> Arc<dyn KeyValueTable> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
        Arc::new(SlateTable::new(db))
    }

    #[test]
    fn emits_retractions_and_insertions_for_count_changes() {
        let mut op = ColumnarCountByKeyOp::new();

        let first = op
            .apply_delta(&one_column_zset(vec![7, 7], vec![1, 1]))
            .expect("first delta");
        let first_materialized = first.materialize().expect("first materialized");
        assert_eq!(first_materialized.get(&vec![7, 2]), Some(&1));

        let second = op
            .apply_delta(&one_column_zset(vec![7], vec![1]))
            .expect("second delta");
        let second_materialized = second.materialize().expect("second materialized");
        assert_eq!(second_materialized.get(&vec![7, 2]), Some(&-1));
        assert_eq!(second_materialized.get(&vec![7, 3]), Some(&1));
        assert_count_snapshot(op.state_snapshot(), &[(7, 3)]);
    }

    #[test]
    fn removes_zero_count_state() {
        let mut op = ColumnarCountByKeyOp::new();
        op.apply_delta(&one_column_zset(vec![7, 7], vec![1, 1]))
            .expect("seed");

        let output = op
            .apply_delta(&one_column_zset(vec![7], vec![-2]))
            .expect("remove");
        let materialized = output.materialize().expect("materialized");
        assert_eq!(materialized.get(&vec![7, 2]), Some(&-1));
        assert_count_snapshot(op.state_snapshot(), &[]);

        let state_materialized = op.state_deltas().materialize().expect("state");
        assert!(state_materialized.is_empty());
    }

    #[tokio::test]
    async fn slate_backed_operator_persists_state_and_output_zsets() {
        let table = build_table("slate-backed-columnar-count").await;
        let mut op = SlateBackedColumnarCountByKeyOp::new(table, "count")
            .await
            .expect("op");

        op.apply_delta(&one_column_zset(vec![1, 1, 2], vec![1, 1, 1]))
            .await
            .expect("first");
        op.apply_delta(&one_column_zset(vec![1, 2], vec![-1, 1]))
            .await
            .expect("second");

        assert_count_snapshot(op.state_snapshot(), &[(1, 1), (2, 2)]);
        let output_delta = op.last_output_delta();
        let output_materialized = output_delta.materialize().expect("output delta");
        assert_eq!(output_materialized.get(&vec![1, 2]), Some(&-1));
        assert_eq!(output_materialized.get(&vec![1, 1]), Some(&1));
        assert_eq!(output_materialized.get(&vec![2, 1]), Some(&-1));
        assert_eq!(output_materialized.get(&vec![2, 2]), Some(&1));
    }

    fn assert_count_snapshot(snapshot: &ColumnarI64ZSet, expected: &[(i64, i64)]) {
        let state = state_columns(snapshot).expect("state columns");
        assert_eq!(state.keys.len(), expected.len());
        for (row_idx, (key, count)) in expected.iter().enumerate() {
            assert_eq!(state.keys.value(row_idx), *key);
            assert_eq!(state.counts.value(row_idx), *count);
        }
    }
}
