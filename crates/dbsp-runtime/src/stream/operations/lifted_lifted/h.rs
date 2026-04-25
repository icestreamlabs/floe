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
use crate::stream::util::{LIFTED_H_STREAM_PREFIX, build_evaluated_stream};

use super::super::zset_integral::lifted_h_zset_stream;

pub async fn lifted_lifted_h_zset_stream<K>(
    diff_stream: &Stream<StreamHandle>,
    integrated_stream: &Stream<StreamHandle>,
) -> Result<Stream<StreamHandle>>
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
{
    let frontier = diff_stream
        .current_time()
        .max(integrated_stream.current_time());
    let horizon = diff_stream
        .semantic_horizon()
        .max(integrated_stream.semantic_horizon());

    let default_handle = compute_h_handle_at::<K>(diff_stream, integrated_stream, 0).await?;
    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));

    let mut result_stream = build_evaluated_stream(
        diff_stream.table(),
        handle_group,
        Arc::new(LiftedLiftedHEvaluator {
            diff_stream: diff_stream.clone(),
            integrated_stream: integrated_stream.clone(),
            _marker: PhantomData::<K>,
        }),
        LIFTED_H_STREAM_PREFIX,
        frontier,
        horizon,
    )
    .await?;

    result_stream.flush().await?;
    Ok(result_stream)
}

struct LiftedLiftedHEvaluator<K>
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
{
    diff_stream: Stream<StreamHandle>,
    integrated_stream: Stream<StreamHandle>,
    _marker: PhantomData<K>,
}

#[async_trait]
impl<K> StreamEvaluator<StreamHandle> for LiftedLiftedHEvaluator<K>
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
{
    async fn value_at(
        &self,
        timestamp: i64,
        _group: Arc<dyn AbelianGroup<StreamHandle>>,
    ) -> Result<StreamHandle> {
        compute_h_handle_at::<K>(&self.diff_stream, &self.integrated_stream, timestamp).await
    }
}

async fn compute_h_handle_at<K>(
    diff_stream: &Stream<StreamHandle>,
    integrated_stream: &Stream<StreamHandle>,
    timestamp: i64,
) -> Result<StreamHandle>
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
{
    let mut diff_outer = diff_stream.clone();
    let mut state_outer = integrated_stream.clone();
    let diff_handle = diff_outer.get(timestamp).await?;
    let state_handle = state_outer.get(timestamp).await?;

    let diff_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(ZSetHandle {
        ns: diff_handle.ns.clone(),
        version: 0,
    }));
    let state_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(ZSetHandle {
        ns: state_handle.ns.clone(),
        version: 0,
    }));

    let diff_inner = diff_stream
        .resolve_handle(&diff_handle, diff_group)
        .await
        .context("resolve diff stream for lifted-lifted H")?;
    let state_inner = integrated_stream
        .resolve_handle(&state_handle, state_group)
        .await
        .context("resolve integrated stream for lifted-lifted H")?;

    let mut result_stream = lifted_h_zset_stream::<K>(&diff_inner, &state_inner).await?;
    result_stream.flush().await?;
    Ok(result_stream.handle())
}
