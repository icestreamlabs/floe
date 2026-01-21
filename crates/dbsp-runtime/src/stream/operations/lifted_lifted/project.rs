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
    LIFTED_PROJECT_STREAM_PREFIX, build_derived_stream, collect_values, push_value_in_place,
    set_default_in_place,
};

use super::super::zset::lifted_project_zset_stream;

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
    F: Fn(&K) -> R + Send + Sync + Clone + 'static,
{
    let handles = collect_values(input, input.current_time()).await?;
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
    let mut result_stream =
        build_derived_stream(input.table(), handle_group, LIFTED_PROJECT_STREAM_PREFIX).await?;

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
