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

use super::super::core::stream::Stream;
use super::super::groups::HandleGroup;
use super::super::util::{
    LIFTED_H_STREAM_PREFIX, LIFTED_JOIN_STREAM_PREFIX, LIFTED_PROJECT_STREAM_PREFIX,
    LIFTED_SELECT_STREAM_PREFIX, build_derived_stream, collect_values, push_value_in_place,
    set_default_in_place,
};
use super::zset::{lifted_join_zset_stream, lifted_project_zset_stream, lifted_select_zset_stream};
use super::zset_integral::lifted_h_zset_stream;

pub async fn lifted_lifted_select_zset_stream<K, P>(
    input: &Stream<StreamHandle>,
    predicate: P,
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
    P: Fn(&K) -> bool + Send + Sync + Clone,
{
    let handles = collect_values(input, input.timestamp).await?;
    let mut output_handles = Vec::with_capacity(handles.len());

    for handle in &handles {
        let inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(ZSetHandle {
                ns: handle.ns.clone(),
                version: 0,
            }));
        let inner_stream = input
            .resolve_handle(handle, inner_group.clone())
            .await
            .context("resolve inner stream for lifted-lifted select")?;
        let mut result_stream =
            lifted_select_zset_stream::<K, _>(&inner_stream, predicate.clone()).await?;
        result_stream.flush().await?;
        output_handles.push(result_stream.handle());
    }

    if output_handles.is_empty() {
        return Err(anyhow!(
            "lifted_lifted_select_zset_stream produced no output"
        ));
    }
    let default_handle = output_handles[0].clone();
    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream = build_derived_stream(
        input.table.clone(),
        handle_group,
        LIFTED_SELECT_STREAM_PREFIX,
    )
    .await?;

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

pub async fn lifted_lifted_project_zset_stream<K, R, F>(
    input: &Stream<StreamHandle>,
    projector: F,
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
    R: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(&K) -> R + Send + Sync + Clone,
{
    let handles = collect_values(input, input.timestamp).await?;
    let mut output_handles = Vec::with_capacity(handles.len());

    for handle in &handles {
        let inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(ZSetHandle {
                ns: handle.ns.clone(),
                version: 0,
            }));
        let inner_stream = input
            .resolve_handle(handle, inner_group.clone())
            .await
            .context("resolve inner stream for lifted-lifted project")?;
        let mut result_stream =
            lifted_project_zset_stream::<K, R, _>(&inner_stream, projector.clone()).await?;
        result_stream.flush().await?;
        output_handles.push(result_stream.handle());
    }

    if output_handles.is_empty() {
        return Err(anyhow!(
            "lifted_lifted_project_zset_stream produced no output"
        ));
    }
    let default_handle = output_handles[0].clone();
    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream = build_derived_stream(
        input.table.clone(),
        handle_group,
        LIFTED_PROJECT_STREAM_PREFIX,
    )
    .await?;

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
    P: Fn(&L, &R) -> bool + Send + Sync + Clone,
    F: Fn(&L, &R) -> O + Send + Sync + Clone,
{
    let left_handles = collect_values(left, left.timestamp).await?;
    let right_handles = collect_values(right, right.timestamp).await?;
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
        build_derived_stream(left.table.clone(), handle_group, LIFTED_JOIN_STREAM_PREFIX).await?;

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
    let diff_handles = collect_values(diff_stream, diff_stream.timestamp).await?;
    let state_handles = collect_values(integrated_stream, integrated_stream.timestamp).await?;
    let total = diff_handles.len().min(state_handles.len());
    let mut output_handles = Vec::with_capacity(total);

    for t in 0..total {
        let diff_handle = &diff_handles[t];
        let state_handle = &state_handles[t];

        let diff_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(ZSetHandle {
                ns: diff_handle.ns.clone(),
                version: 0,
            }));
        let state_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(ZSetHandle {
                ns: state_handle.ns.clone(),
                version: 0,
            }));

        let diff_inner = diff_stream
            .resolve_handle(diff_handle, diff_group.clone())
            .await
            .context("resolve diff stream for lifted-lifted H")?;
        let state_inner = integrated_stream
            .resolve_handle(state_handle, state_group.clone())
            .await
            .context("resolve integrated stream for lifted-lifted H")?;

        let mut result_stream = lifted_h_zset_stream::<K>(&diff_inner, &state_inner).await?;
        result_stream.flush().await?;
        output_handles.push(result_stream.handle());
    }

    if output_handles.is_empty() {
        return Err(anyhow!("lifted_lifted_h_zset_stream produced no output"));
    }
    let default_handle = output_handles[0].clone();
    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream = build_derived_stream(
        diff_stream.table.clone(),
        handle_group,
        LIFTED_H_STREAM_PREFIX,
    )
    .await?;

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
