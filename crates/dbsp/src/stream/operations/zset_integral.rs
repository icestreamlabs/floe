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

use super::super::core::stream::Stream;
use super::super::groups::HandleGroup;
use super::super::util::{
    LIFTED_H_STREAM_PREFIX, LIFTED_H_ZSET_PREFIX, ZSET_INTEGRAL_PREFIX,
    ZSET_INTEGRAL_STREAM_PREFIX, build_derived_stream, collect_values, compute_delta,
    materialize_zset_handle, next_lifted_zset_namespace, push_value_in_place,
    resolve_apply_handle_op, set_default_in_place,
};
use super::super::zset_stream::{StreamRetention, ZSetStream};

pub async fn lifted_h_zset_stream<K>(
    diff_stream: &Stream<ZSetHandle>,
    integrated_stream: &Stream<ZSetHandle>,
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
    let diff_handles = collect_values(diff_stream, diff_stream.current_time()).await?;
    let state_handles = collect_values(integrated_stream, integrated_stream.current_time()).await?;
    let total = diff_handles.len().min(state_handles.len());
    let table = diff_stream.table();
    let namespace = next_lifted_zset_namespace(LIFTED_H_ZSET_PREFIX);
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), namespace.clone(), None)
            .await
            .context("build dictionary for lifted H")?,
    );
    let mut zset_stream = ZSetStream::new(dict, table.clone(), namespace, StreamRetention::None)
        .await
        .context("create ZSet stream for lifted H")?;

    let mut diff_cache: HashMap<String, Arc<Dictionary<K>>> = HashMap::new();
    let mut state_cache: HashMap<String, Arc<Dictionary<K>>> = HashMap::new();
    let mut previous: HashMap<K, i64> = HashMap::new();
    let mut output_handles = Vec::with_capacity(total);

    for t in 0..total {
        let diff_map =
            materialize_zset_handle::<K>(table.clone(), &mut diff_cache, &diff_handles[t]).await?;
        let state_map =
            materialize_zset_handle::<K>(table.clone(), &mut state_cache, &state_handles[t])
                .await?;

        let mut distincted = HashMap::new();
        for (key, &diff_weight) in &diff_map {
            let state_weight = state_map.get(key).copied().unwrap_or(0);
            let coalesced = diff_weight + state_weight;
            if state_weight > 0 && coalesced <= 0 {
                distincted.insert(key.clone(), -1);
                continue;
            }
            if state_weight <= 0 && coalesced > 0 {
                distincted.insert(key.clone(), 1);
                continue;
            }
            if state_weight == 0 && diff_weight > 0 {
                distincted.insert(key.clone(), 1);
            }
        }

        let deltas = compute_delta(&previous, &distincted);
        zset_stream.add_deltas(deltas);
        let handle = zset_stream.flush().await.context("flush lifted H result")?;
        output_handles.push(handle);
        previous = distincted;
    }

    let default_handle = output_handles
        .first()
        .cloned()
        .unwrap_or_else(|| zset_stream.current_handle().clone());
    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream =
        build_derived_stream(table.clone(), handle_group, LIFTED_H_STREAM_PREFIX).await?;

    if output_handles.is_empty() {
        set_default_in_place(&mut result_stream, default_handle);
    } else {
        set_default_in_place(&mut result_stream, output_handles[0].clone());
        for handle in output_handles.iter().skip(1) {
            push_value_in_place(&mut result_stream, handle.clone());
        }
        if let Some(last) = output_handles.last() {
            set_default_in_place(&mut result_stream, last.clone());
        }
    }

    result_stream.flush().await?;
    Ok(result_stream)
}

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

async fn integrate_zset_handle_stream<K>(stream: &Stream<ZSetHandle>) -> Result<Stream<ZSetHandle>>
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
