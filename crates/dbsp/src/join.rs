use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::{build_derived_stream, push_value_in_place, set_default_in_place};
use crate::stream::{Stream, StreamCursor};

/// Join wrapper that drives the JoinOp operator over handle streams without requiring aligned timestamps.
pub struct DbspJoin {
    stream: Stream<ZSetHandle>,
}

impl DbspJoin {
    pub async fn new<L, R, O, P, F>(
        left: &Stream<ZSetHandle>,
        right: &Stream<ZSetHandle>,
        predicate: P,
        projector: F,
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

        let join_op = Arc::new(AsyncMutex::new(JoinOp::new(
            left_state,
            right_state,
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
        let op = Arc::clone(&join_op);
        let mut left_cursor = StreamCursor::new(left.clone());
        let mut right_cursor = StreamCursor::new(right.clone());
        tokio::spawn(async move {
            let mut last_left: Option<ZSetHandle> = None;
            let mut last_right: Option<ZSetHandle> = None;
            loop {
                tokio::select! {
                    left_next = left_cursor.next() => {
                        match left_next {
                            Ok((ts, handle)) => {
                                last_left = Some(handle.clone());
                                if let Some(out) = drive_join(&op, &writer, ts, vec![Some(handle.clone()), last_right.clone()]).await {
                                    if let Err(err) = out {
                                        eprintln!("join left path error: {err}");
                                        break;
                                    }
                                }
                            }
                            Err(err) => { eprintln!("join left stream closed: {err}"); break; }
                        }
                    }
                    right_next = right_cursor.next() => {
                        match right_next {
                            Ok((ts, handle)) => {
                                last_right = Some(handle.clone());
                                if let Some(out) = drive_join(&op, &writer, ts, vec![last_left.clone(), Some(handle.clone())]).await {
                                    if let Err(err) = out {
                                        eprintln!("join right path error: {err}");
                                        break;
                                    }
                                }
                            }
                            Err(err) => { eprintln!("join right stream closed: {err}"); break; }
                        }
                    }
                }
            }
        });

        stream.flush().await?;
        Ok(Self { stream })
    }

    pub fn stream(&self) -> Stream<ZSetHandle> {
        self.stream.clone()
    }
}

async fn drive_join<L, R, O>(
    op: &Arc<AsyncMutex<JoinOp<L, R, O>>>,
    writer: &Arc<AsyncMutex<Stream<ZSetHandle>>>,
    ts: i64,
    handles: Vec<Option<ZSetHandle>>,
) -> Option<Result<()>>
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
{
    if handles.is_empty() || handles.iter().any(|h| h.is_none()) {
        return None;
    }
    let handles: Vec<ZSetHandle> = handles.into_iter().filter_map(|h| h).collect();
    let mut op_guard = op.lock().await;
    match op_guard.on_step(ts, &handles).await {
        Ok(Some(out)) => {
            let mut writer_guard = writer.lock().await;
            push_value_in_place(&mut writer_guard, out);
            if let Err(err) = writer_guard.flush().await {
                return Some(Err(err));
            }
            Some(Ok(()))
        }
        Ok(None) => None,
        Err(err) => Some(Err(err)),
    }
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
