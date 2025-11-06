use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::handles::ZSetHandle;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::super::core::stream::Stream;
use super::super::groups::HandleGroup;
use super::super::util::{
    LIFTED_JOIN_STREAM_PREFIX, LIFTED_JOIN_ZSET_PREFIX, LIFTED_PROJECT_STREAM_PREFIX,
    LIFTED_PROJECT_ZSET_PREFIX, LIFTED_SELECT_STREAM_PREFIX, LIFTED_SELECT_ZSET_PREFIX,
    build_derived_stream, collect_values, compute_delta, materialize_zset_handle,
    next_lifted_zset_namespace, push_value_in_place, set_default_in_place,
};
use super::super::zset_stream::{StreamRetention, ZSetStream};

pub async fn lifted_select_zset_stream<K, P>(
    input: &Stream<ZSetHandle>,
    predicate: P,
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
    P: Fn(&K) -> bool + Send + Sync + Clone,
{
    let handles = collect_values(input, input.timestamp).await?;
    let table = input.table.clone();
    let namespace = next_lifted_zset_namespace(LIFTED_SELECT_ZSET_PREFIX);
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), namespace.clone(), None)
            .await
            .context("build dictionary for lifted select")?,
    );
    let mut zset_stream = ZSetStream::new(dict, table.clone(), namespace, StreamRetention::None)
        .await
        .context("create ZSet stream for lifted select")?;

    let mut dict_cache: HashMap<String, Arc<Dictionary<K>>> = HashMap::new();
    let mut previous: HashMap<K, i64> = HashMap::new();
    let mut output_handles = Vec::with_capacity(handles.len());

    for handle in handles {
        let materialized =
            materialize_zset_handle::<K>(table.clone(), &mut dict_cache, &handle).await?;

        let mut filtered = HashMap::new();
        for (key, weight) in materialized {
            if predicate(&key) && weight != 0 {
                filtered.insert(key, weight);
            }
        }

        let deltas = compute_delta(&previous, &filtered);
        zset_stream.add_deltas(deltas);
        let handle = zset_stream
            .flush()
            .await
            .context("flush lifted select result")?;
        output_handles.push(handle);
        previous = filtered;
    }

    let default_handle = output_handles
        .first()
        .cloned()
        .unwrap_or_else(|| zset_stream.current_handle().clone());
    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream =
        build_derived_stream(table.clone(), handle_group, LIFTED_SELECT_STREAM_PREFIX).await?;

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

pub async fn lifted_project_zset_stream<K, R, F>(
    input: &Stream<ZSetHandle>,
    projector: F,
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
    let table = input.table.clone();
    let namespace = next_lifted_zset_namespace(LIFTED_PROJECT_ZSET_PREFIX);
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), namespace.clone(), None)
            .await
            .context("build dictionary for lifted project")?,
    );
    let mut zset_stream = ZSetStream::new(dict, table.clone(), namespace, StreamRetention::None)
        .await
        .context("create ZSet stream for lifted project")?;

    let mut dict_cache: HashMap<String, Arc<Dictionary<K>>> = HashMap::new();
    let mut previous: HashMap<R, i64> = HashMap::new();
    let mut output_handles = Vec::with_capacity(handles.len());

    for handle in handles {
        let materialized =
            materialize_zset_handle::<K>(table.clone(), &mut dict_cache, &handle).await?;

        let mut projected: HashMap<R, i64> = HashMap::new();
        for (key, weight) in materialized {
            if weight == 0 {
                continue;
            }
            let result_key = projector(&key);
            *projected.entry(result_key).or_insert(0) += weight;
        }
        projected.retain(|_, weight| *weight != 0);

        let deltas = compute_delta(&previous, &projected);
        zset_stream.add_deltas(deltas);
        let handle = zset_stream
            .flush()
            .await
            .context("flush lifted project result")?;
        output_handles.push(handle);
        previous = projected;
    }

    let default_handle = output_handles
        .first()
        .cloned()
        .unwrap_or_else(|| zset_stream.current_handle().clone());
    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream =
        build_derived_stream(table.clone(), handle_group, LIFTED_PROJECT_STREAM_PREFIX).await?;

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

pub async fn lifted_join_zset_stream<L, R, O, P, F>(
    left: &Stream<ZSetHandle>,
    right: &Stream<ZSetHandle>,
    predicate: P,
    projector: F,
) -> Result<Stream<ZSetHandle>>
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
    let table = left.table.clone();
    let namespace = next_lifted_zset_namespace(LIFTED_JOIN_ZSET_PREFIX);
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), namespace.clone(), None)
            .await
            .context("build dictionary for lifted join")?,
    );
    let mut zset_stream = ZSetStream::new(dict, table.clone(), namespace, StreamRetention::None)
        .await
        .context("create ZSet stream for lifted join")?;

    let mut left_cache: HashMap<String, Arc<Dictionary<L>>> = HashMap::new();
    let mut right_cache: HashMap<String, Arc<Dictionary<R>>> = HashMap::new();
    let mut previous: HashMap<O, i64> = HashMap::new();
    let mut output_handles = Vec::with_capacity(total);

    for t in 0..total {
        let left_map =
            materialize_zset_handle::<L>(table.clone(), &mut left_cache, &left_handles[t]).await?;
        let right_map =
            materialize_zset_handle::<R>(table.clone(), &mut right_cache, &right_handles[t])
                .await?;

        let mut joined: HashMap<O, i64> = HashMap::new();
        for (left_key, &left_weight) in &left_map {
            if left_weight == 0 {
                continue;
            }
            for (right_key, &right_weight) in &right_map {
                if right_weight == 0 {
                    continue;
                }
                if predicate(left_key, right_key) {
                    let projected = projector(left_key, right_key);
                    *joined.entry(projected).or_insert(0) += left_weight * right_weight;
                }
            }
        }
        joined.retain(|_, weight| *weight != 0);

        let deltas = compute_delta(&previous, &joined);
        zset_stream.add_deltas(deltas);
        let handle = zset_stream
            .flush()
            .await
            .context("flush lifted join result")?;
        output_handles.push(handle);
        previous = joined;
    }

    let default_handle = output_handles
        .first()
        .cloned()
        .unwrap_or_else(|| zset_stream.current_handle().clone());
    let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream =
        build_derived_stream(table.clone(), handle_group, LIFTED_JOIN_STREAM_PREFIX).await?;

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
