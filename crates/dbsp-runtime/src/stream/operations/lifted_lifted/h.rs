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
    LIFTED_H_STREAM_PREFIX, build_derived_stream, collect_values, push_value_in_place,
    set_default_in_place,
};

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
    let diff_handles = collect_values(diff_stream, diff_stream.current_time()).await?;
    let state_handles = collect_values(integrated_stream, integrated_stream.current_time()).await?;
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
    let mut result_stream =
        build_derived_stream(diff_stream.table(), handle_group, LIFTED_H_STREAM_PREFIX).await?;

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
