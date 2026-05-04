use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Context;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::sync::Mutex as AsyncMutex;

use crate::algebra::AbelianGroup;
use crate::collections::IndexedBatchZSet;
use crate::collections::zset::VersionedZSet;
use crate::handles::ZSetHandle;
use crate::operators::count_aggregate::{
    CountAggregateOp, CountAggregateRow, CountAggregateSlotKind, CountAggregateSlotUpdate,
    DistinctGroupKey, GroupedCountState,
};
use crate::relation_state::RelationState;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::DeltaHandleStream;
use crate::stream::runtime::{
    DeltaOperator, HandleOperatorRuntime, RuntimeErrorHandler, report_runtime_error,
};
use crate::stream::util::{
    build_exact_stream_from_values, collect_values, publish_scheduled_value, push_value_in_place,
};

type BatchRowEvaluator<V, K, D> =
    Arc<dyn Fn(&[(V, i64)]) -> Vec<(CountAggregateRow<K, D>, i64)> + Send + Sync>;

pub struct DbspCountAggregate {
    stream: DeltaHandleStream,
}

struct TransientCountAggregateState<K, V, D>
where
    K: Clone + Eq + Hash,
    V: Clone + Eq + Hash,
    D: Clone + Eq + Hash,
{
    row_evaluator: BatchRowEvaluator<V, K, D>,
    slot_kinds: Vec<CountAggregateSlotKind>,
    grouped_state: HashMap<K, GroupedCountState>,
    distinct_weights: HashMap<(DistinctGroupKey<K>, D), i64>,
}

pub struct DbspTransientCountAggregate<K, V, D>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    D: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    D::Archived: RkyvDeserialize<D, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    state: AsyncMutex<TransientCountAggregateState<K, V, D>>,
}

impl DbspCountAggregate {
    pub async fn new<K, V, D, FRow>(
        input: &DeltaHandleStream,
        row_evaluator: FRow,
        slot_kinds: Vec<CountAggregateSlotKind>,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> anyhow::Result<Self>
    where
        K: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        V: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        D: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        D::Archived: RkyvDeserialize<D, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        FRow: Fn(&V) -> Option<CountAggregateRow<K, D>> + Send + Sync + 'static,
    {
        Self::new_batch(
            input,
            move |delta_values: &[(V, i64)]| {
                delta_values
                    .iter()
                    .filter_map(|(value, weight)| row_evaluator(value).map(|row| (row, *weight)))
                    .collect()
            },
            slot_kinds,
            error_handler,
        )
        .await
    }

    pub async fn new_batch<K, V, D, FRow>(
        input: &DeltaHandleStream,
        row_evaluator: FRow,
        slot_kinds: Vec<CountAggregateSlotKind>,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> anyhow::Result<Self>
    where
        K: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        V: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        D: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        D::Archived: RkyvDeserialize<D, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        FRow: Fn(&[(V, i64)]) -> Vec<(CountAggregateRow<K, D>, i64)> + Send + Sync + 'static,
    {
        Self::new_batch_with_append_only_input(
            input,
            row_evaluator,
            slot_kinds,
            false,
            error_handler,
        )
        .await
    }

    pub async fn new_batch_with_append_only_input<K, V, D, FRow>(
        input: &DeltaHandleStream,
        row_evaluator: FRow,
        slot_kinds: Vec<CountAggregateSlotKind>,
        append_only_input: bool,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> anyhow::Result<Self>
    where
        K: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        V: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        D: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        D::Archived: RkyvDeserialize<D, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        FRow: Fn(&[(V, i64)]) -> Vec<(CountAggregateRow<K, D>, i64)> + Send + Sync + 'static,
    {
        let table = input.table();
        let frontier = input.current_time();
        let horizon = input.semantic_horizon();
        let aggregate_id = NEXT_COUNT_AGGREGATE_ID.fetch_add(1, Ordering::Relaxed);
        let output_ns = format!("count_aggregate_output_{aggregate_id}");
        let empty_handle = ZSetHandle {
            ns: output_ns.clone(),
            version: 0,
        };

        let state = RelationState::<(K, GroupedCountState)>::empty(
            table.clone(),
            format!("count_aggregate_state_{aggregate_id}"),
        )
        .await?;
        let output_dict = Arc::new(
            Dictionary::<(K, Vec<i64>)>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .context("create output dictionary for count aggregate")?,
        );
        let output = VersionedZSet::new(output_dict, table.clone(), output_ns.clone())
            .await
            .context("create output zset for count aggregate")?;
        let distinct_index = slot_kinds
            .iter()
            .any(|kind| matches!(kind, CountAggregateSlotKind::Distinct))
            .then(|| {
                IndexedBatchZSet::new(
                    table.clone(),
                    format!("count_aggregate_distinct_{aggregate_id}"),
                )
            });

        let mut op = CountAggregateOp::new_batch(
            state,
            table.clone(),
            Arc::new(row_evaluator) as BatchRowEvaluator<V, K, D>,
            output,
            slot_kinds,
            distinct_index,
        );
        if append_only_input {
            op.enable_append_only_input();
        }
        let count_aggregate_op = Arc::new(AsyncMutex::new(op));

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(ZSetHandleGroup {
            default: empty_handle.clone(),
        });

        let history = collect_values(input, horizon).await?;
        let mut output_handles = Vec::with_capacity(history.len());
        for (ts, handle) in history.into_iter().enumerate() {
            let out_handle = {
                let mut op_guard = count_aggregate_op.lock().await;
                op_guard
                    .on_step(ts as i64, std::slice::from_ref(&handle))
                    .await?
            }
            .unwrap_or_else(|| empty_handle.clone());
            output_handles.push(out_handle);
        }

        let mut stream = build_exact_stream_from_values(
            table.clone(),
            handle_group,
            "count_aggregate_output_stream/",
            frontier,
            horizon,
            &output_handles,
            empty_handle.clone(),
        )
        .await?;
        stream.flush().await?;
        {
            let mut op_guard = count_aggregate_op.lock().await;
            op_guard.enable_live_output_replayable();
        }

        let writer = Arc::new(AsyncMutex::new(stream.clone()));

        let mut runtime = HandleOperatorRuntime::new(vec![input.stream()], move |ts, handles| {
            let op = Arc::clone(&count_aggregate_op);
            let writer = Arc::clone(&writer);
            let empty_handle = empty_handle.clone();
            let handles_vec = handles.to_vec();
            Box::pin(async move {
                if handles_vec.len() != 1 {
                    return Err(anyhow::anyhow!(
                        "count aggregate runtime expected 1 handle, got {}",
                        handles_vec.len()
                    ));
                }
                if ts <= horizon {
                    let mut writer_guard = writer.lock().await;
                    publish_scheduled_value(&mut writer_guard, ts).await?;
                    return Ok(());
                }
                let mut op_guard = op.lock().await;
                let out_handle = op_guard
                    .on_step(ts, &handles_vec)
                    .await?
                    .unwrap_or_else(|| empty_handle.clone());
                let mut writer_guard = writer.lock().await;
                push_value_in_place(&mut writer_guard, out_handle);
                writer_guard.flush().await?;
                Ok(())
            })
        });

        let error_handler = error_handler.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = runtime.step().await {
                    report_runtime_error(&error_handler, "count_aggregate", err);
                    break;
                }
            }
        });

        Ok(Self {
            stream: DeltaHandleStream::new(stream),
        })
    }

    pub fn stream(&self) -> DeltaHandleStream {
        self.stream.clone()
    }
}

impl<K, V, D> DbspTransientCountAggregate<K, V, D>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    D: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    D::Archived: RkyvDeserialize<D, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub async fn new<FRow>(
        row_evaluator: FRow,
        slot_kinds: Vec<CountAggregateSlotKind>,
    ) -> anyhow::Result<Self>
    where
        FRow: Fn(&V) -> Option<CountAggregateRow<K, D>> + Send + Sync + 'static,
    {
        Self::new_batch(
            move |delta_values: &[(V, i64)]| {
                delta_values
                    .iter()
                    .filter_map(|(value, weight)| row_evaluator(value).map(|row| (row, *weight)))
                    .collect()
            },
            slot_kinds,
        )
        .await
    }

    pub async fn new_batch<FRow>(
        row_evaluator: FRow,
        slot_kinds: Vec<CountAggregateSlotKind>,
    ) -> anyhow::Result<Self>
    where
        FRow: Fn(&[(V, i64)]) -> Vec<(CountAggregateRow<K, D>, i64)> + Send + Sync + 'static,
    {
        Ok(Self {
            state: AsyncMutex::new(TransientCountAggregateState {
                row_evaluator: Arc::new(row_evaluator) as BatchRowEvaluator<V, K, D>,
                slot_kinds,
                grouped_state: HashMap::new(),
                distinct_weights: HashMap::new(),
            }),
        })
    }

    pub async fn apply_deltas(
        &self,
        delta_values: Vec<(V, i64)>,
    ) -> anyhow::Result<Vec<((K, Vec<i64>), i64)>> {
        let mut state = self.state.lock().await;
        let deltas = state.apply_deltas(delta_values);
        Ok(deltas.into_iter().filter(|(_, diff)| *diff != 0).collect())
    }
}

impl<K, V, D> TransientCountAggregateState<K, V, D>
where
    K: Clone + Eq + Hash,
    V: Clone + Eq + Hash,
    D: Clone + Eq + Hash,
{
    fn coalesce_deltas(&self, deltas: Vec<(V, i64)>) -> HashMap<V, i64> {
        let mut merged = HashMap::new();
        for (row, weight) in deltas {
            let entry = merged.entry(row.clone()).or_insert(0);
            *entry += weight;
            if *entry == 0 {
                merged.remove(&row);
            }
        }
        merged
    }

    fn apply_deltas(&mut self, delta_values: Vec<(V, i64)>) -> HashMap<(K, Vec<i64>), i64> {
        if delta_values.is_empty() {
            return HashMap::new();
        }

        let coalesced = self.coalesce_deltas(delta_values);
        if coalesced.is_empty() {
            return HashMap::new();
        }

        let arity = self.slot_kinds.len();
        let mut grouped_deltas: HashMap<K, GroupedCountState> = HashMap::new();
        let mut distinct_deltas: HashMap<(DistinctGroupKey<K>, D), i64> = HashMap::new();
        let row_updates = (self.row_evaluator)(
            &coalesced
                .into_iter()
                .filter(|(_, weight)| *weight != 0)
                .collect::<Vec<_>>(),
        );
        for (row_update, weight) in row_updates {
            if weight == 0 {
                continue;
            }
            if row_update.slots.len() != arity {
                tracing::warn!(
                    expected = arity,
                    actual = row_update.slots.len(),
                    "transient count aggregate row evaluator returned unexpected slot vector width"
                );
                continue;
            }

            let entry = grouped_deltas
                .entry(row_update.key.clone())
                .or_insert_with(|| GroupedCountState::zero(arity));
            entry.total_rows += weight;
            for (slot_idx, slot) in row_update.slots.into_iter().enumerate() {
                match (&self.slot_kinds[slot_idx], slot) {
                    (CountAggregateSlotKind::Linear, CountAggregateSlotUpdate::Linear(value)) => {
                        entry.counts[slot_idx] += value * weight;
                    }
                    (
                        CountAggregateSlotKind::Distinct,
                        CountAggregateSlotUpdate::Distinct(Some(distinct_value)),
                    ) => {
                        let distinct_key = DistinctGroupKey {
                            group_key: row_update.key.clone(),
                            slot: slot_idx as u32,
                        };
                        let delta_entry = distinct_deltas
                            .entry((distinct_key, distinct_value))
                            .or_insert(0);
                        *delta_entry += weight;
                    }
                    (
                        CountAggregateSlotKind::Distinct,
                        CountAggregateSlotUpdate::Distinct(None),
                    ) => {}
                    (expected_kind, actual) => {
                        tracing::warn!(
                            ?expected_kind,
                            slot_idx,
                            actual_kind = match actual {
                                CountAggregateSlotUpdate::Linear(_) => "linear",
                                CountAggregateSlotUpdate::Distinct(_) => "distinct",
                            },
                            "transient count aggregate row evaluator returned mismatched slot kind"
                        );
                    }
                }
            }
        }

        if grouped_deltas.is_empty() && distinct_deltas.is_empty() {
            return HashMap::new();
        }

        for ((distinct_key, distinct_value), delta) in distinct_deltas {
            if delta == 0 {
                continue;
            }
            let old_weight = self
                .distinct_weights
                .get(&(distinct_key.clone(), distinct_value.clone()))
                .copied()
                .unwrap_or(0);
            let new_weight = old_weight + delta;
            let entry = grouped_deltas
                .entry(distinct_key.group_key.clone())
                .or_insert_with(|| GroupedCountState::zero(arity));
            if old_weight > 0 && new_weight <= 0 {
                entry.counts[distinct_key.slot as usize] -= 1;
            } else if old_weight <= 0 && new_weight > 0 {
                entry.counts[distinct_key.slot as usize] += 1;
            }
            if new_weight == 0 {
                self.distinct_weights
                    .remove(&(distinct_key, distinct_value));
            } else {
                self.distinct_weights
                    .insert((distinct_key, distinct_value), new_weight);
            }
        }

        let mut output_deltas: HashMap<(K, Vec<i64>), i64> = HashMap::new();
        for (key, delta_state) in grouped_deltas {
            let old_state = self.grouped_state.get(&key).cloned();
            let new_state = match old_state.as_ref() {
                Some(old) => {
                    let next = old.apply_delta(&delta_state);
                    if next.is_present() { Some(next) } else { None }
                }
                None => {
                    if delta_state.is_present() {
                        Some(delta_state)
                    } else {
                        None
                    }
                }
            };

            if old_state == new_state {
                continue;
            }

            let old_output = old_state.as_ref().map(|state| state.counts.clone());
            let new_output = new_state.as_ref().map(|state| state.counts.clone());
            match (old_output, new_output) {
                (Some(old), Some(new)) if old == new => {}
                (Some(old), Some(new)) => {
                    output_deltas.insert((key.clone(), old), -1);
                    output_deltas.insert((key.clone(), new), 1);
                }
                (Some(old), None) => {
                    output_deltas.insert((key.clone(), old), -1);
                }
                (None, Some(new)) => {
                    output_deltas.insert((key.clone(), new), 1);
                }
                (None, None) => {}
            }

            if let Some(new_state) = new_state {
                self.grouped_state.insert(key, new_state);
            } else {
                self.grouped_state.remove(&key);
            }
        }

        output_deltas
    }
}

#[derive(Clone)]
struct ZSetHandleGroup {
    default: ZSetHandle,
}

#[async_trait::async_trait]
impl AbelianGroup<ZSetHandle> for ZSetHandleGroup {
    async fn add(&self, a: &ZSetHandle, _b: &ZSetHandle) -> ZSetHandle {
        a.clone()
    }

    async fn neg(&self, a: &ZSetHandle) -> ZSetHandle {
        a.clone()
    }

    async fn identity(&self) -> ZSetHandle {
        self.default.clone()
    }
}

static NEXT_COUNT_AGGREGATE_ID: AtomicUsize = AtomicUsize::new(0);
