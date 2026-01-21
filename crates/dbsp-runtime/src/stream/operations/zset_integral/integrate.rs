use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::handles::{StreamHandle, ZSetHandle};
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::groups::HandleGroup;
use crate::stream::util::{
    ZSET_INTEGRAL_PREFIX, ZSET_INTEGRAL_STREAM_PREFIX, build_derived_stream, collect_values,
    compute_delta, materialize_zset_handle, next_lifted_zset_namespace, push_value_in_place,
    resolve_apply_handle_op, set_default_in_place,
};
use crate::stream::{Stream, StreamRetention, ZSetStream};

pub async fn lifted_integrate_zset<K>(
    stream: &Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<ZSetHandle>>,
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
    resolve_apply_handle_op(
        stream,
        inner_group,
        |inner| async move { integrate_zset_handle_stream::<K>(&inner).await },
        "stream_lift_integrate/",
    )
    .await
}

pub(crate) async fn integrate_zset_handle_stream<K>(
    stream: &Stream<ZSetHandle>,
) -> Result<Stream<ZSetHandle>>
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
    let handles = collect_values(stream, stream.current_time()).await?;
    let table = stream.table();
    let namespace = next_lifted_zset_namespace(ZSET_INTEGRAL_PREFIX);
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), namespace.clone(), None)
            .await
            .context("build dictionary for integrated zset stream")?,
    );
    let mut aggregator = ZSetStream::new(dict, table.clone(), namespace, StreamRetention::None)
        .await
        .context("create aggregator for integrated zset stream")?;

    let mut caches: HashMap<String, Arc<Dictionary<K>>> = HashMap::new();
    let mut previous_state: HashMap<K, i64> = HashMap::new();
    let mut integral_state: HashMap<K, i64> = HashMap::new();
    let mut last_integral_state: HashMap<K, i64> = HashMap::new();
    let mut result_handles = Vec::with_capacity(handles.len());

    for handle in handles {
        let state = materialize_zset_handle::<K>(table.clone(), &mut caches, &handle)
            .await
            .context("materialize zset state for integration")?;
        let deltas = compute_delta(&previous_state, &state);
        previous_state = state;

        for (key, weight) in deltas {
            let entry = integral_state.entry(key).or_insert(0);
            *entry = (*entry).saturating_add(weight);
        }
        integral_state.retain(|_, weight| *weight != 0);

        let integral_delta = compute_delta(&last_integral_state, &integral_state);
        aggregator.add_deltas(integral_delta);
        let handle = aggregator
            .flush()
            .await
            .context("flush integrated zset stream")?;
        result_handles.push(handle);
        last_integral_state = integral_state.clone();
    }

    let default_handle = result_handles
        .first()
        .cloned()
        .unwrap_or_else(|| aggregator.current_handle().clone());
    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream =
        build_derived_stream(table, handle_group, ZSET_INTEGRAL_STREAM_PREFIX).await?;

    if result_handles.is_empty() {
        set_default_in_place(&mut result_stream, default_handle);
    } else {
        set_default_in_place(&mut result_stream, result_handles[0].clone());
        for handle in result_handles.iter().skip(1) {
            push_value_in_place(&mut result_stream, handle.clone());
        }
        if let Some(last) = result_handles.last() {
            set_default_in_place(&mut result_stream, last.clone());
        }
    }

    result_stream.flush().await?;
    Ok(result_stream)
}
