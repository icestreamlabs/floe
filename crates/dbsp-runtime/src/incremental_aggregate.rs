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
use crate::collections::zset::VersionedZSet;
use crate::collections::{DEFAULT_HOT_KEY_COMPACTION_THRESHOLD, IndexedBatchZSet};
use crate::ephemeral_state::build_ephemeral_state_table;
use crate::handles::ZSetHandle;
use crate::operators::incremental_aggregate::{
    AggregateValue, DistinctGroupKey, GroupedIncrementalAggregateState, IncrementalAggregateOp,
    IncrementalAggregateRow, IncrementalAggregateSlotKind,
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

type BatchRowEvaluator<V, K> =
    Arc<dyn Fn(&[(V, i64)]) -> Vec<(V, IncrementalAggregateRow<K>, i64)> + Send + Sync>;

pub struct DbspIncrementalAggregate {
    stream: DeltaHandleStream,
}

pub struct DbspTransientIncrementalAggregate<K, V>
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
{
    op: AsyncMutex<IncrementalAggregateOp<K, V>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransientIncrementalAggregateGroupedState<K> {
    pub key: K,
    pub total_rows: i64,
    pub slots: Vec<crate::operators::incremental_aggregate::IncrementalAggregateSlotState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransientIncrementalAggregateDistinctWeight<K> {
    pub group_key: K,
    pub slot: u32,
    pub value: AggregateValue,
    pub weight: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransientIncrementalAggregateInputWeight<K, V> {
    pub group_key: K,
    pub value: V,
    pub weight: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TransientIncrementalAggregateSnapshot<K, V> {
    pub grouped: Vec<TransientIncrementalAggregateGroupedState<K>>,
    pub distinct: Vec<TransientIncrementalAggregateDistinctWeight<K>>,
    pub input: Vec<TransientIncrementalAggregateInputWeight<K, V>>,
}

impl DbspIncrementalAggregate {
    pub async fn new_batch<K, V, FRow>(
        input: &DeltaHandleStream,
        row_evaluator: FRow,
        slot_kinds: Vec<IncrementalAggregateSlotKind>,
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
        FRow: Fn(&[(V, i64)]) -> Vec<(V, IncrementalAggregateRow<K>, i64)> + Send + Sync + 'static,
    {
        let table = input.table();
        let frontier = input.current_time();
        let horizon = input.semantic_horizon();
        let aggregate_id = NEXT_INCREMENTAL_AGGREGATE_ID.fetch_add(1, Ordering::Relaxed);
        let output_ns = format!("incremental_aggregate_output_{aggregate_id}");
        let empty_handle = ZSetHandle {
            ns: output_ns.clone(),
            version: 0,
        };

        let state = RelationState::<(K, GroupedIncrementalAggregateState)>::empty(
            table.clone(),
            format!("incremental_aggregate_state_{aggregate_id}"),
        )
        .await?;
        let output_dict = Arc::new(
            Dictionary::<(K, Vec<AggregateValue>)>::with_table(
                table.clone(),
                output_ns.clone(),
                None,
            )
            .await
            .context("create output dictionary for incremental aggregate")?,
        );
        let output = VersionedZSet::new(output_dict, table.clone(), output_ns.clone())
            .await
            .context("create output zset for incremental aggregate")?;
        let distinct_index = slot_kinds
            .iter()
            .any(|kind| matches!(kind, IncrementalAggregateSlotKind::CountDistinct))
            .then(|| {
                IndexedBatchZSet::with_hot_key_compaction_threshold(
                    table.clone(),
                    format!("incremental_aggregate_distinct_{aggregate_id}"),
                    DEFAULT_HOT_KEY_COMPACTION_THRESHOLD,
                )
            });
        let input_index = slot_kinds
            .iter()
            .any(|kind| {
                matches!(
                    kind,
                    IncrementalAggregateSlotKind::Min(_) | IncrementalAggregateSlotKind::Max(_)
                )
            })
            .then(|| {
                IndexedBatchZSet::with_hot_key_compaction_threshold(
                    table.clone(),
                    format!("incremental_aggregate_index_{aggregate_id}"),
                    DEFAULT_HOT_KEY_COMPACTION_THRESHOLD,
                )
            });

        let aggregate_op = Arc::new(AsyncMutex::new(IncrementalAggregateOp::new_batch(
            state,
            table.clone(),
            Arc::new(row_evaluator) as BatchRowEvaluator<V, K>,
            output,
            slot_kinds,
            distinct_index,
            input_index,
        )));

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(ZSetHandleGroup {
            default: empty_handle.clone(),
        });

        let history = collect_values(input, horizon).await?;
        let mut output_handles = Vec::with_capacity(history.len());
        for (ts, handle) in history.into_iter().enumerate() {
            let out_handle = {
                let mut op_guard = aggregate_op.lock().await;
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
            "incremental_aggregate_output_stream/",
            frontier,
            horizon,
            &output_handles,
            empty_handle.clone(),
        )
        .await?;
        stream.flush().await?;
        {
            let mut op_guard = aggregate_op.lock().await;
            op_guard.enable_live_output_replayable();
        }

        let writer = Arc::new(AsyncMutex::new(stream.clone()));

        let mut runtime = HandleOperatorRuntime::new(vec![input.stream()], move |ts, handles| {
            let op = Arc::clone(&aggregate_op);
            let writer = Arc::clone(&writer);
            let empty_handle = empty_handle.clone();
            let handles_vec = handles.to_vec();
            Box::pin(async move {
                if handles_vec.len() != 1 {
                    return Err(anyhow::anyhow!(
                        "incremental aggregate runtime expected 1 handle, got {}",
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
                    report_runtime_error(&error_handler, "incremental_aggregate", err);
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

impl<K, V> DbspTransientIncrementalAggregate<K, V>
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
{
    pub async fn new_batch<FRow>(
        row_evaluator: FRow,
        slot_kinds: Vec<IncrementalAggregateSlotKind>,
    ) -> anyhow::Result<Self>
    where
        FRow: Fn(&[(V, i64)]) -> Vec<(V, IncrementalAggregateRow<K>, i64)> + Send + Sync + 'static,
    {
        let aggregate_id = NEXT_INCREMENTAL_AGGREGATE_ID.fetch_add(1, Ordering::Relaxed);
        let table = build_ephemeral_state_table(&format!(
            "transient_incremental_aggregate_state_{aggregate_id}"
        ))
        .await?;
        let state = RelationState::<(K, GroupedIncrementalAggregateState)>::empty(
            table.clone(),
            format!("transient_incremental_aggregate_state_{aggregate_id}"),
        )
        .await?;
        let output_ns = format!("transient_incremental_aggregate_output_{aggregate_id}");
        let output_dict = Arc::new(
            Dictionary::<(K, Vec<AggregateValue>)>::with_table(
                table.clone(),
                output_ns.clone(),
                None,
            )
            .await
            .context("create output dictionary for transient incremental aggregate")?,
        );
        let output = VersionedZSet::new(output_dict, table.clone(), output_ns)
            .await
            .context("create output zset for transient incremental aggregate")?;
        let distinct_index = slot_kinds
            .iter()
            .any(|kind| matches!(kind, IncrementalAggregateSlotKind::CountDistinct))
            .then(|| {
                IndexedBatchZSet::new_replayable(
                    table.clone(),
                    format!("transient_incremental_aggregate_distinct_{aggregate_id}"),
                )
            });
        let input_index = slot_kinds
            .iter()
            .any(|kind| {
                matches!(
                    kind,
                    IncrementalAggregateSlotKind::Min(_) | IncrementalAggregateSlotKind::Max(_)
                )
            })
            .then(|| {
                IndexedBatchZSet::new_replayable(
                    table.clone(),
                    format!("transient_incremental_aggregate_index_{aggregate_id}"),
                )
            });

        let mut op = IncrementalAggregateOp::new_batch(
            state,
            table,
            Arc::new(row_evaluator) as BatchRowEvaluator<V, K>,
            output,
            slot_kinds,
            distinct_index,
            input_index,
        );
        op.enable_live_output_replayable();

        Ok(Self {
            op: AsyncMutex::new(op),
        })
    }

    pub async fn apply_deltas(
        &self,
        delta_values: Vec<(V, i64)>,
    ) -> anyhow::Result<Vec<((K, Vec<AggregateValue>), i64)>> {
        let mut op = self.op.lock().await;
        let deltas = op.apply_delta_values(&delta_values).await?;
        Ok(deltas.into_iter().filter(|(_, diff)| *diff != 0).collect())
    }

    pub async fn evict_keys_where<F>(
        &self,
        predicate: F,
    ) -> anyhow::Result<Vec<((K, Vec<AggregateValue>), i64)>>
    where
        F: Fn(&K) -> bool,
    {
        let mut op = self.op.lock().await;
        let deltas = op.evict_keys_where(predicate).await?;
        Ok(deltas.into_iter().filter(|(_, diff)| *diff != 0).collect())
    }

    pub async fn enable_append_only_input(&self) {
        let mut op = self.op.lock().await;
        op.enable_append_only_input();
    }

    pub async fn snapshot_state(
        &self,
    ) -> anyhow::Result<TransientIncrementalAggregateSnapshot<K, V>> {
        let mut op = self.op.lock().await;
        let grouped = op
            .snapshot_grouped_state()
            .await?
            .into_iter()
            .map(|(key, state)| TransientIncrementalAggregateGroupedState {
                key,
                total_rows: state.total_rows(),
                slots: state.slots().to_vec(),
            })
            .collect();
        let distinct = op
            .snapshot_distinct_index()?
            .into_iter()
            .map(
                |(key, value, weight)| TransientIncrementalAggregateDistinctWeight {
                    group_key: key.group_key,
                    slot: key.slot,
                    value,
                    weight,
                },
            )
            .collect();
        let input = op
            .snapshot_input_index()?
            .into_iter()
            .map(
                |(group_key, value, weight)| TransientIncrementalAggregateInputWeight {
                    group_key,
                    value,
                    weight,
                },
            )
            .collect();
        Ok(TransientIncrementalAggregateSnapshot {
            grouped,
            distinct,
            input,
        })
    }

    pub async fn restore_state(
        &self,
        snapshot: TransientIncrementalAggregateSnapshot<K, V>,
    ) -> anyhow::Result<()> {
        let mut op = self.op.lock().await;
        op.restore_grouped_state(
            snapshot
                .grouped
                .into_iter()
                .map(|group| {
                    (
                        group.key,
                        GroupedIncrementalAggregateState::from_parts(group.total_rows, group.slots),
                    )
                })
                .collect(),
        );
        op.restore_distinct_index(
            snapshot
                .distinct
                .into_iter()
                .map(|entry| {
                    (
                        DistinctGroupKey {
                            group_key: entry.group_key,
                            slot: entry.slot,
                        },
                        entry.value,
                        entry.weight,
                    )
                })
                .collect(),
        )
        .await?;
        op.restore_input_index(
            snapshot
                .input
                .into_iter()
                .map(|entry| (entry.group_key, entry.value, entry.weight))
                .collect(),
        )
        .await
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

static NEXT_INCREMENTAL_AGGREGATE_ID: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::operators::incremental_aggregate::{
        AggregateValueType, IncrementalAggregateSlotUpdate,
    };

    fn test_row(value: &i64) -> Option<IncrementalAggregateRow<i64>> {
        Some(IncrementalAggregateRow {
            key: value / 10,
            slots: vec![
                IncrementalAggregateSlotUpdate::Count(1),
                IncrementalAggregateSlotUpdate::Value(Some(AggregateValue::Int64(*value))),
                IncrementalAggregateSlotUpdate::Value(Some(AggregateValue::Int64(*value))),
                IncrementalAggregateSlotUpdate::Value(Some(AggregateValue::Int64(*value))),
            ],
        })
    }

    fn test_batch_rows(deltas: &[(i64, i64)]) -> Vec<(i64, IncrementalAggregateRow<i64>, i64)> {
        deltas
            .iter()
            .filter_map(|(value, weight)| test_row(value).map(|row| (*value, row, *weight)))
            .collect()
    }

    fn test_slot_kinds() -> Vec<IncrementalAggregateSlotKind> {
        vec![
            IncrementalAggregateSlotKind::Count,
            IncrementalAggregateSlotKind::Sum(AggregateValueType::Int64),
            IncrementalAggregateSlotKind::Min(AggregateValueType::Int64),
            IncrementalAggregateSlotKind::CountDistinct,
        ]
    }

    #[tokio::test]
    async fn transient_incremental_aggregate_snapshot_restores_state() {
        let processor = DbspTransientIncrementalAggregate::<i64, i64>::new_batch(
            test_batch_rows,
            test_slot_kinds(),
        )
        .await
        .expect("create transient incremental aggregate");
        processor.enable_append_only_input().await;
        processor
            .apply_deltas(vec![(11, 1), (12, 1), (21, 1)])
            .await
            .expect("apply initial rows");
        let snapshot = processor.snapshot_state().await.expect("snapshot state");

        let restored = DbspTransientIncrementalAggregate::<i64, i64>::new_batch(
            test_batch_rows,
            test_slot_kinds(),
        )
        .await
        .expect("create restored transient incremental aggregate");
        restored.enable_append_only_input().await;
        restored
            .restore_state(snapshot)
            .await
            .expect("restore snapshot");

        let expected = processor
            .apply_deltas(vec![(10, 1), (12, 1)])
            .await
            .expect("apply follow-up to original")
            .into_iter()
            .collect::<HashSet<_>>();
        let actual = restored
            .apply_deltas(vec![(10, 1), (12, 1)])
            .await
            .expect("apply follow-up to restored")
            .into_iter()
            .collect::<HashSet<_>>();

        assert_eq!(actual, expected);
    }
}
