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
    LIFTED_JOIN_STREAM_PREFIX, build_exact_stream_from_values, collect_values,
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
    let frontier = left.current_time().max(right.current_time());
    let horizon = left.semantic_horizon().max(right.semantic_horizon());
    let left_handles = collect_values(left, horizon).await?;
    let right_handles = collect_values(right, horizon).await?;
    let mut output_handles = Vec::with_capacity((horizon + 1) as usize);

    for t in 0..=horizon {
        let left_inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(ZSetHandle {
                ns: left_handles[t as usize].ns.clone(),
                version: 0,
            }));
        let right_inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(ZSetHandle {
                ns: right_handles[t as usize].ns.clone(),
                version: 0,
            }));

        let left_stream = left
            .resolve_handle(&left_handles[t as usize], left_inner_group.clone())
            .await
            .context("resolve left stream for lifted-lifted join")?;
        let right_stream = right
            .resolve_handle(&right_handles[t as usize], right_inner_group.clone())
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
    let left_default_handle = left.default_value();
    let right_default_handle = right.default_value();
    let default_handle = if let Some(existing) = left_handles
        .iter()
        .zip(right_handles.iter())
        .zip(output_handles.iter())
        .find_map(|((left_handle, right_handle), derived)| {
            if *left_handle == left_default_handle && *right_handle == right_default_handle {
                Some(derived.clone())
            } else {
                None
            }
        }) {
        existing
    } else {
        let left_inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(ZSetHandle {
                ns: left_default_handle.ns.clone(),
                version: 0,
            }));
        let right_inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(ZSetHandle {
                ns: right_default_handle.ns.clone(),
                version: 0,
            }));
        let left_stream = left
            .resolve_handle(&left_default_handle, left_inner_group.clone())
            .await
            .context("resolve default left stream for lifted-lifted join")?;
        let right_stream = right
            .resolve_handle(&right_default_handle, right_inner_group.clone())
            .await
            .context("resolve default right stream for lifted-lifted join")?;
        let mut result_stream = lifted_join_zset_stream::<L, R, O, _, _>(
            &left_stream,
            &right_stream,
            predicate.clone(),
            projector.clone(),
        )
        .await?;
        result_stream.flush().await?;
        result_stream.handle()
    };
    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream = build_exact_stream_from_values(
        left.table(),
        handle_group,
        LIFTED_JOIN_STREAM_PREFIX,
        frontier,
        horizon,
        &output_handles,
        default_handle,
    )
    .await?;

    result_stream.flush().await?;
    Ok(result_stream)
}
