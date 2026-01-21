use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::handles::{StreamHandle, ZSetHandle};
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::core::stream::Stream;
use crate::stream::groups::HandleGroup;
use crate::stream::util::{
    LIFTED_JOIN_STREAM_PREFIX, build_derived_stream, collect_values, push_value_in_place,
    set_default_in_place,
};

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
    let left_handles = collect_values(left, left.current_time()).await?;
    let right_handles = collect_values(right, right.current_time()).await?;
    let total = left_handles.len().min(right_handles.len());
    let mut output_handles = Vec::with_capacity(total);

    for t in 0..total {
        let left_inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(ZSetHandle {
                ns: left_handles[t].ns.clone(),
                version: 0,
            }));
        let right_inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(ZSetHandle {
                ns: right_handles[t].ns.clone(),
                version: 0,
            }));

        let left_stream = left
            .resolve_handle(&left_handles[t], left_inner_group.clone())
            .await
            .context("resolve left stream for lifted-lifted join")?;
        let right_stream = right
            .resolve_handle(&right_handles[t], right_inner_group.clone())
            .await
            .context("resolve right stream for lifted-lifted join")?;

        let mut result_stream = lifted_join_zset_stream::<L, R, O, _, _>(
            &left_stream,
            &right_stream,
            predicate.clone(),
            projector.clone(),
        )
        .await?;
        result_stream.flush().await?;
        output_handles.push(result_stream.handle());
    }

    if output_handles.is_empty() {
        return Err(anyhow!("lifted_lifted_join_zset_stream produced no output"));
    }
    let default_handle = output_handles[0].clone();
    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream =
        build_derived_stream(left.table(), handle_group, LIFTED_JOIN_STREAM_PREFIX).await?;

    set_default_in_place(&mut result_stream, default_handle.clone());
    for handle in output_handles.iter().skip(1) {
        push_value_in_place(&mut result_stream, handle.clone());
    }
    if let Some(latest) = output_handles.last() {
        set_default_in_place(&mut result_stream, latest.clone());
    }

    result_stream.flush().await?;
    Ok(result_stream)
}
