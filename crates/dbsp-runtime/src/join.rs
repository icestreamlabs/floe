use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use anyhow::{Context, Result};
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::sync::Mutex as AsyncMutex;

use crate::algebra::AbelianGroup;
use crate::collections::zset::VersionedZSet;
use crate::handles::ZSetHandle;
use crate::operators::join::JoinOp;
use crate::relation_state::RelationState;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::{
    DeltaOperator, HandleOperatorRuntime, RuntimeErrorHandler, report_runtime_error,
};
use crate::stream::util::{
    build_derived_stream, collect_values, push_value_in_place, set_default_in_place,
};
use crate::stream::{DeltaHandleStream, Stream};

static JOIN_STEP_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
const JOIN_STEP_LOG_SAMPLE_EVERY: u64 = 256;

/// Join wrapper that drives the JoinOp operator over handle streams without requiring aligned timestamps.
pub struct DbspJoin {
    stream: DeltaHandleStream,
}

impl DbspJoin {
    pub async fn new<L, R, O, K, KL, KR, P, F>(
        left: &DeltaHandleStream,
        right: &DeltaHandleStream,
        left_key: KL,
        right_key: KR,
        predicate: P,
        projector: F,
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
        O: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
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
        P: Fn(&L, &R) -> bool + Send + Sync + Clone + 'static,
        F: Fn(&L, &R) -> O + Send + Sync + Clone + 'static,
    {
        let table = left.table();
        let join_id = NEXT_JOIN_ID.fetch_add(1, Ordering::Relaxed);

        let left_state =
            RelationState::empty(table.clone(), format!("join_left_state_{join_id}")).await?;
        let right_state =
            RelationState::empty(table.clone(), format!("join_right_state_{join_id}")).await?;

        let output_ns = format!("join_output_{join_id}");
        let output_dict = Arc::new(
            Dictionary::<O>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .context("create output dictionary for join")?,
        );
        let output = VersionedZSet::new(output_dict, table.clone(), output_ns.clone())
            .await
            .context("create output zset for join")?;
        let left_index = crate::collections::IndexedBatchZSet::new(
            table.clone(),
            format!("join_left_index_{join_id}"),
        );
        let right_index = crate::collections::IndexedBatchZSet::new(
            table.clone(),
            format!("join_right_index_{join_id}"),
        );

        let join_op = Arc::new(AsyncMutex::new(JoinOp::new(
            left_state,
            right_state,
            left_index,
            right_index,
            Arc::new(left_key),
            Arc::new(right_key),
            Arc::new(predicate),
            Arc::new(projector),
            table.clone(),
            output,
            None,
        )));

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(ZSetHandleGroup {
            default: ZSetHandle {
                ns: output_ns.clone(),
                version: 0,
            },
        });
        let mut stream =
            build_derived_stream(table.clone(), handle_group, "join_output_stream/").await?;
        set_default_in_place(
            &mut stream,
            ZSetHandle {
                ns: output_ns,
                version: 0,
            },
        );

        let writer = Arc::new(AsyncMutex::new(stream.clone()));

        // Rehydrate join state from any existing input handles before going live.
        let left_history = collect_values(left, left.current_time()).await?;
        let right_history = collect_values(right, right.current_time()).await?;
        let left_default = left.default_value();
        let right_default = right.default_value();
        let replay_len = left_history.len().max(right_history.len());
        for ts in 0..replay_len {
            let handles = vec![
                left_history
                    .get(ts)
                    .cloned()
                    .unwrap_or_else(|| left_default.clone()),
                right_history
                    .get(ts)
                    .cloned()
                    .unwrap_or_else(|| right_default.clone()),
            ];
            drive_join(&join_op, &writer, ts as i64, handles).await?;
        }

        let op = Arc::clone(&join_op);
        let mut runtime =
            HandleOperatorRuntime::new(vec![left.stream(), right.stream()], move |ts, handles| {
                let op = Arc::clone(&op);
                let writer = Arc::clone(&writer);
                let handles = handles.to_vec();
                Box::pin(async move {
                    // If either side did not change at this ts, synthesize an empty delta
                    // handle in the corresponding namespace so downstream logic observes
                    // aligned timestamps with zero deltas.
                    if handles.len() != 2 {
                        return Err(anyhow::anyhow!(
                            "join runtime expected 2 handles, got {}",
                            handles.len()
                        ));
                    }
                    drive_join(&op, &writer, ts, handles).await
                })
            });

        let error_handler = error_handler.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = runtime.step().await {
                    report_runtime_error(&error_handler, "join", err);
                    break;
                }
            }
        });

        stream.flush().await?;
        Ok(Self {
            stream: DeltaHandleStream::new(stream),
        })
    }

    pub fn stream(&self) -> DeltaHandleStream {
        self.stream.clone()
    }
}

async fn drive_join<L, R, O, K>(
    op: &Arc<AsyncMutex<JoinOp<L, R, O, K>>>,
    writer: &Arc<AsyncMutex<Stream<ZSetHandle>>>,
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
    O: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
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
    let span = tracing::trace_span!(
        "join_step",
        ts,
        left_ns = tracing::field::Empty,
        left_version = tracing::field::Empty,
        right_ns = tracing::field::Empty,
        right_version = tracing::field::Empty
    );
    let _enter = span.enter();
    if let Some(left) = handles.first() {
        span.record("left_ns", left.ns.as_str());
        span.record("left_version", left.version);
    }
    if let Some(right) = handles.get(1) {
        span.record("right_ns", right.ns.as_str());
        span.record("right_version", right.version);
    }
    if JOIN_STEP_LOG_COUNTER
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(JOIN_STEP_LOG_SAMPLE_EVERY)
    {
        tracing::trace!("join step");
    }
    let mut op_guard = op.lock().await;
    if let Some(out) = op_guard.on_step(ts, &handles).await? {
        let mut writer_guard = writer.lock().await;
        push_value_in_place(&mut writer_guard, out);
        writer_guard.flush().await?;
    }
    Ok(())
}

#[derive(Clone)]
struct ZSetHandleGroup {
    default: ZSetHandle,
}

#[async_trait]
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

static NEXT_JOIN_ID: AtomicUsize = AtomicUsize::new(0);
