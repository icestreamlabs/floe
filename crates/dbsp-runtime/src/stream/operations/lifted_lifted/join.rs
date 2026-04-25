use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::handles::{StreamHandle, ZSetHandle};
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::core::stream::{Stream, StreamEvaluator};
use crate::stream::groups::HandleGroup;
use crate::stream::util::{LIFTED_JOIN_STREAM_PREFIX, build_evaluated_stream};

use super::super::zset::lifted_join_zset_stream;

pub async fn lifted_lifted_join_zset_stream<L, R, O, P, F>(
    left: &Stream<StreamHandle>,
    right: &Stream<StreamHandle>,
    predicate: P,
    projector: F,
) -> Result<Stream<StreamHandle>>
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
    P: Fn(&L, &R) -> bool + Send + Sync + Clone + 'static,
    F: Fn(&L, &R) -> O + Send + Sync + Clone + 'static,
{
    let frontier = left.current_time().max(right.current_time());
    let horizon = left.semantic_horizon().max(right.semantic_horizon());

    let default_handle =
        compute_join_handle_at(left, right, 0, predicate.clone(), projector.clone()).await?;
    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));

    let mut result_stream = build_evaluated_stream(
        left.table(),
        handle_group,
        Arc::new(LiftedLiftedJoinEvaluator {
            left: left.clone(),
            right: right.clone(),
            predicate,
            projector,
            _marker: PhantomData,
        }),
        LIFTED_JOIN_STREAM_PREFIX,
        frontier,
        horizon,
    )
    .await?;

    result_stream.flush().await?;
    Ok(result_stream)
}

struct LiftedLiftedJoinEvaluator<L, R, O, P, F>
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
    P: Fn(&L, &R) -> bool + Send + Sync + Clone + 'static,
    F: Fn(&L, &R) -> O + Send + Sync + Clone + 'static,
{
    left: Stream<StreamHandle>,
    right: Stream<StreamHandle>,
    predicate: P,
    projector: F,
    _marker: PhantomData<(L, R, O)>,
}

#[async_trait]
impl<L, R, O, P, F> StreamEvaluator<StreamHandle> for LiftedLiftedJoinEvaluator<L, R, O, P, F>
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
    P: Fn(&L, &R) -> bool + Send + Sync + Clone + 'static,
    F: Fn(&L, &R) -> O + Send + Sync + Clone + 'static,
{
    async fn value_at(
        &self,
        timestamp: i64,
        _group: Arc<dyn AbelianGroup<StreamHandle>>,
    ) -> Result<StreamHandle> {
        compute_join_handle_at(
            &self.left,
            &self.right,
            timestamp,
            self.predicate.clone(),
            self.projector.clone(),
        )
        .await
    }
}

async fn compute_join_handle_at<L, R, O, P, F>(
    left: &Stream<StreamHandle>,
    right: &Stream<StreamHandle>,
    timestamp: i64,
    predicate: P,
    projector: F,
) -> Result<StreamHandle>
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
    P: Fn(&L, &R) -> bool + Send + Sync + Clone + 'static,
    F: Fn(&L, &R) -> O + Send + Sync + Clone + 'static,
{
    let mut left_outer = left.clone();
    let mut right_outer = right.clone();
    let left_handle = left_outer.get(timestamp).await?;
    let right_handle = right_outer.get(timestamp).await?;

    let left_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(ZSetHandle {
        ns: left_handle.ns.clone(),
        version: 0,
    }));
    let right_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(ZSetHandle {
        ns: right_handle.ns.clone(),
        version: 0,
    }));

    let left_stream = left
        .resolve_handle(&left_handle, left_group)
        .await
        .context("resolve left stream for lifted-lifted join")?;
    let right_stream = right
        .resolve_handle(&right_handle, right_group)
        .await
        .context("resolve right stream for lifted-lifted join")?;

    let mut result_stream =
        lifted_join_zset_stream::<L, R, O, _, _>(&left_stream, &right_stream, predicate, projector)
            .await?;
    result_stream.flush().await?;
    Ok(result_stream.handle())
}
