use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::sync::Mutex as AsyncMutex;

use crate::algebra::AbelianGroup;
use crate::collections::RangeKey;
use crate::collections::zset::VersionedZSet;
use crate::handles::ZSetHandle;
use crate::operators::range_join::RangeJoinOp;
use crate::relation_state::RelationState;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::DeltaHandleStream;
use crate::stream::Stream;
use crate::stream::runtime::{
    DeltaOperator, HandleOperatorRuntime, RuntimeErrorHandler, report_runtime_error,
};
use crate::stream::util::{
    build_exact_stream_from_values, collect_values, publish_scheduled_value, push_value_in_place,
};

/// Range-join wrapper that drives `RangeJoinOp` over handle streams.
pub struct DbspRangeJoin {
    stream: DeltaHandleStream,
}

impl DbspRangeJoin {
    #[allow(clippy::too_many_arguments)]
    pub async fn new_batch<L, R, O, K, LR, RK, P, F>(
        left: &DeltaHandleStream,
        right: &DeltaHandleStream,
        left_range: LR,
        right_key: RK,
        predicate: P,
        projector: F,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> Result<Self>
    where
        L: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        R: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        O: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        K: Archive
            + Clone
            + Eq
            + Hash
            + Ord
            + RangeKey
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        LR: Fn(&[(L, i64)]) -> Vec<(K, K, L, i64)> + Send + Sync + Clone + 'static,
        RK: Fn(&[(R, i64)]) -> Vec<(K, R, i64)> + Send + Sync + Clone + 'static,
        P: Fn(&L, &R) -> bool + Send + Sync + Clone + 'static,
        F: Fn(&L, &R) -> O + Send + Sync + Clone + 'static,
    {
        Self::new_batch_with_state_namespace(
            left,
            right,
            None,
            left_range,
            right_key,
            predicate,
            projector,
            error_handler,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn new_batch_with_state_namespace<L, R, O, K, LR, RK, P, F>(
        left: &DeltaHandleStream,
        right: &DeltaHandleStream,
        state_namespace: Option<String>,
        left_range: LR,
        right_key: RK,
        predicate: P,
        projector: F,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> Result<Self>
    where
        L: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        R: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        O: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        K: Archive
            + Clone
            + Eq
            + Hash
            + Ord
            + RangeKey
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        LR: Fn(&[(L, i64)]) -> Vec<(K, K, L, i64)> + Send + Sync + Clone + 'static,
        RK: Fn(&[(R, i64)]) -> Vec<(K, R, i64)> + Send + Sync + Clone + 'static,
        P: Fn(&L, &R) -> bool + Send + Sync + Clone + 'static,
        F: Fn(&L, &R) -> O + Send + Sync + Clone + 'static,
    {
        let table = left.table();
        let frontier = left.current_time().max(right.current_time());
        let horizon = left.semantic_horizon().max(right.semantic_horizon());
        let range_join_id = state_namespace.unwrap_or_else(|| {
            NEXT_RANGE_JOIN_ID
                .fetch_add(1, Ordering::Relaxed)
                .to_string()
        });

        let left_state = RelationState::empty(
            table.clone(),
            format!("range_join_left_state_{range_join_id}"),
        )
        .await?;
        let right_state = RelationState::empty(
            table.clone(),
            format!("range_join_right_state_{range_join_id}"),
        )
        .await?;

        let output_ns = format!("range_join_output_{range_join_id}");
        let output_dict = Arc::new(
            Dictionary::<O>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .context("create output dictionary for range join")?,
        );
        let output = VersionedZSet::new(output_dict, table.clone(), output_ns.clone())
            .await
            .context("create output zset for range join")?;
        let right_index = crate::collections::IndexedBatchZSet::with_range_index(
            table.clone(),
            format!("range_join_right_index_{range_join_id}"),
        );
        right_index
            .restore_committed_checkpoint()
            .await
            .context("restore committed right range-join index")?;

        let range_join_op = Arc::new(AsyncMutex::new(RangeJoinOp::new_batch(
            left_state,
            right_state,
            right_index,
            Arc::new(left_range),
            Arc::new(right_key),
            Arc::new(predicate),
            Arc::new(projector),
            table.clone(),
            output,
            None,
        )));
        let empty_handle = ZSetHandle {
            ns: output_ns.clone(),
            version: 0,
        };

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(ZSetHandleGroup {
            default: empty_handle.clone(),
        });

        let left_history = collect_values(left, horizon).await?;
        let right_history = collect_values(right, horizon).await?;
        let mut output_handles = Vec::with_capacity((horizon + 1) as usize);
        for ts in 0..=horizon {
            let handles = vec![
                left_history[ts as usize].clone(),
                right_history[ts as usize].clone(),
            ];
            let out_handle = {
                let mut op_guard = range_join_op.lock().await;
                op_guard.on_step(ts, &handles).await?
            }
            .unwrap_or_else(|| empty_handle.clone());
            output_handles.push(out_handle);
        }

        let mut stream = build_exact_stream_from_values(
            table.clone(),
            handle_group,
            "range_join_output_stream/",
            frontier,
            horizon,
            &output_handles,
            empty_handle.clone(),
        )
        .await?;
        stream.flush().await?;

        let writer = Arc::new(AsyncMutex::new(stream.clone()));

        let op = Arc::clone(&range_join_op);
        let mut runtime =
            HandleOperatorRuntime::new(vec![left.stream(), right.stream()], move |ts, handles| {
                let op = Arc::clone(&op);
                let writer = Arc::clone(&writer);
                let empty_handle = empty_handle.clone();
                let handles = handles.to_vec();
                Box::pin(async move {
                    if handles.len() != 2 {
                        return Err(anyhow!(
                            "range join runtime expected 2 handles, got {}",
                            handles.len()
                        ));
                    }
                    if ts <= horizon {
                        let mut writer_guard = writer.lock().await;
                        publish_scheduled_value(&mut writer_guard, ts).await?;
                        return Ok(());
                    }
                    drive_range_join(&op, &writer, &empty_handle, ts, handles).await
                })
            });

        let error_handler = error_handler.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = runtime.step().await {
                    report_runtime_error(&error_handler, "range join", err);
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

async fn drive_range_join<L, R, O, K>(
    op: &Arc<AsyncMutex<RangeJoinOp<L, R, O, K>>>,
    writer: &Arc<AsyncMutex<Stream<ZSetHandle>>>,
    empty_handle: &ZSetHandle,
    ts: i64,
    handles: Vec<ZSetHandle>,
) -> Result<()>
where
    L: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    K: Archive
        + Clone
        + Eq
        + Hash
        + Ord
        + RangeKey
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut op_guard = op.lock().await;
    let out = op_guard
        .on_step(ts, &handles)
        .await?
        .unwrap_or_else(|| empty_handle.clone());
    let mut writer_guard = writer.lock().await;
    push_value_in_place(&mut writer_guard, out);
    writer_guard.flush().await?;
    Ok(())
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

static NEXT_RANGE_JOIN_ID: AtomicUsize = AtomicUsize::new(0);
