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
use crate::stream::util::{LIFTED_H_STREAM_PREFIX, build_exact_stream_from_values, collect_values};

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
    let diff_handles = collect_values(diff_stream, horizon).await?;
    let state_handles = collect_values(integrated_stream, horizon).await?;
    let mut output_handles = Vec::with_capacity((horizon + 1) as usize);

    for t in 0..=horizon {
        let diff_handle = &diff_handles[t as usize];
        let state_handle = &state_handles[t as usize];

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
    let diff_default_handle = diff_stream.default_value();
    let state_default_handle = integrated_stream.default_value();
    let default_handle = if let Some(existing) = diff_handles
        .iter()
        .zip(state_handles.iter())
        .zip(output_handles.iter())
        .find_map(|((diff_handle, state_handle), derived)| {
            if *diff_handle == diff_default_handle && *state_handle == state_default_handle {
                Some(derived.clone())
            } else {
                None
            }
        }) {
        existing
    } else {
        let diff_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(ZSetHandle {
                ns: diff_default_handle.ns.clone(),
                version: 0,
            }));
        let state_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(ZSetHandle {
                ns: state_default_handle.ns.clone(),
                version: 0,
            }));
        let diff_inner = diff_stream
            .resolve_handle(&diff_default_handle, diff_group.clone())
            .await
            .context("resolve default diff stream for lifted-lifted H")?;
        let state_inner = integrated_stream
            .resolve_handle(&state_default_handle, state_group.clone())
            .await
            .context("resolve default integrated stream for lifted-lifted H")?;
        let mut result_stream = lifted_h_zset_stream::<K>(&diff_inner, &state_inner).await?;
        result_stream.flush().await?;
        result_stream.handle()
    };
    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream = build_exact_stream_from_values(
        diff_stream.table(),
        handle_group,
        LIFTED_H_STREAM_PREFIX,
        frontier,
        horizon,
        &output_handles,
        default_handle,
    )
    .await?;

    result_stream.flush().await?;
    Ok(result_stream)
}
