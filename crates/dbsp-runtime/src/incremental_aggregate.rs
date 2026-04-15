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
use crate::ephemeral_state::build_ephemeral_state_table;
use crate::handles::ZSetHandle;
use crate::operators::incremental_aggregate::{
    AggregateValue, GroupedIncrementalAggregateState, IncrementalAggregateOp,
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

type RowEvaluator<V, K> = Arc<dyn Fn(&V) -> Option<IncrementalAggregateRow<K>> + Send + Sync>;

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

impl DbspIncrementalAggregate {
    pub async fn new<K, V, FRow>(
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
        FRow: Fn(&V) -> Option<IncrementalAggregateRow<K>> + Send + Sync + 'static,
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
                IndexedBatchZSet::new_replayable(
                    table.clone(),
                    format!("incremental_aggregate_distinct_{aggregate_id}"),
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
                    format!("incremental_aggregate_index_{aggregate_id}"),
                )
            });

        let aggregate_op = Arc::new(AsyncMutex::new(IncrementalAggregateOp::new(
            state,
            table.clone(),
            Arc::new(row_evaluator) as RowEvaluator<V, K>,
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
            op_guard.state.enable_live_replayable();
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
    pub async fn new<FRow>(
        row_evaluator: FRow,
        slot_kinds: Vec<IncrementalAggregateSlotKind>,
    ) -> anyhow::Result<Self>
    where
        FRow: Fn(&V) -> Option<IncrementalAggregateRow<K>> + Send + Sync + 'static,
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

        let mut op = IncrementalAggregateOp::new(
            state,
            table,
            Arc::new(row_evaluator) as RowEvaluator<V, K>,
            output,
            slot_kinds,
            distinct_index,
            input_index,
        );
        op.state.enable_live_replayable();
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

    pub async fn enable_append_only_input(&self) {
        let mut op = self.op.lock().await;
        op.enable_append_only_input();
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
