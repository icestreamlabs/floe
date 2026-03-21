use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::sync::Mutex as AsyncMutex;

use crate::algebra::AbelianGroup;
use crate::collections::zset::VersionedZSet;
use crate::handles::ZSetHandle;
use crate::operators::semijoin::{SemiJoinMode, SemiJoinOp};
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

/// Semijoin wrapper that drives `SemiJoinOp` over handle streams.
pub struct DbspSemiJoin {
    stream: DeltaHandleStream,
}

impl DbspSemiJoin {
    pub async fn new<L, R, K, KL, KR>(
        left: &DeltaHandleStream,
        right: &DeltaHandleStream,
        left_key: KL,
        right_key: KR,
        mode: SemiJoinMode,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> Result<Self>
    where
        L: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        R: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        K: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        KL: Fn(&L) -> Option<K> + Send + Sync + Clone + 'static,
        KR: Fn(&R) -> Option<K> + Send + Sync + Clone + 'static,
    {
        let table = left.table();
        let frontier = left.current_time().max(right.current_time());
        let horizon = left.semantic_horizon().max(right.semantic_horizon());
        let semijoin_id = NEXT_SEMIJOIN_ID.fetch_add(1, Ordering::Relaxed);

        let left_state =
            RelationState::empty(table.clone(), format!("semijoin_left_state_{semijoin_id}"))
                .await?;
        let right_state =
            RelationState::empty(table.clone(), format!("semijoin_right_state_{semijoin_id}"))
                .await?;

        let output_ns = format!("semijoin_output_{semijoin_id}");
        let output_dict = Arc::new(
            Dictionary::<L>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .context("create output dictionary for semijoin")?,
        );
        let output = VersionedZSet::new(output_dict, table.clone(), output_ns.clone())
            .await
            .context("create output zset for semijoin")?;
        let left_index = crate::collections::IndexedBatchZSet::new(
            table.clone(),
            format!("semijoin_left_index_{semijoin_id}"),
        );
        let right_index = crate::collections::IndexedBatchZSet::new(
            table.clone(),
            format!("semijoin_right_index_{semijoin_id}"),
        );

        let semijoin_op = Arc::new(AsyncMutex::new(SemiJoinOp::new(
            left_state,
            right_state,
            left_index,
            right_index,
            Arc::new(left_key),
            Arc::new(right_key),
            mode,
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
                let mut op_guard = semijoin_op.lock().await;
                op_guard.on_step(ts, &handles).await?
            }
            .unwrap_or_else(|| empty_handle.clone());
            output_handles.push(out_handle);
        }

        let mut stream = build_exact_stream_from_values(
            table.clone(),
            handle_group,
            "semijoin_output_stream/",
            frontier,
            horizon,
            &output_handles,
            empty_handle.clone(),
        )
        .await?;
        stream.flush().await?;

        let writer = Arc::new(AsyncMutex::new(stream.clone()));

        let op = Arc::clone(&semijoin_op);
        let mut runtime =
            HandleOperatorRuntime::new(vec![left.stream(), right.stream()], move |ts, handles| {
                let op = Arc::clone(&op);
                let writer = Arc::clone(&writer);
                let empty_handle = empty_handle.clone();
                let handles = handles.to_vec();
                Box::pin(async move {
                    if handles.len() != 2 {
                        return Err(anyhow!(
                            "semijoin runtime expected 2 handles, got {}",
                            handles.len()
                        ));
                    }
                    if ts <= horizon {
                        let mut writer_guard = writer.lock().await;
                        publish_scheduled_value(&mut writer_guard, ts).await?;
                        return Ok(());
                    }
                    drive_semijoin(&op, &writer, &empty_handle, ts, handles).await
                })
            });

        let error_handler = error_handler.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = runtime.step().await {
                    report_runtime_error(&error_handler, "semijoin", err);
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

async fn drive_semijoin<L, R, K>(
    op: &Arc<AsyncMutex<SemiJoinOp<L, R, K>>>,
    writer: &Arc<AsyncMutex<Stream<ZSetHandle>>>,
    empty_handle: &ZSetHandle,
    ts: i64,
    handles: Vec<ZSetHandle>,
) -> Result<()>
where
    L: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    K: Archive
        + Clone
        + Eq
        + std::hash::Hash
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

static NEXT_SEMIJOIN_ID: AtomicUsize = AtomicUsize::new(0);
