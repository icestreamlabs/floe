use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::handles::{StreamHandle, ZSetHandle, ZSetHandleView};
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::core::stream::Stream;
use super::groups::HandleGroup;

pub(crate) static DERIVED_NAMESPACE_COUNTER: AtomicU64 = AtomicU64::new(0);
pub(crate) static LIFTED_ZSET_NAMESPACE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) const LIFTED_SELECT_ZSET_PREFIX: &str = "zset_lifted_select/";
pub(crate) const LIFTED_PROJECT_ZSET_PREFIX: &str = "zset_lifted_project/";
pub(crate) const LIFTED_JOIN_ZSET_PREFIX: &str = "zset_lifted_join/";
pub(crate) const LIFTED_H_ZSET_PREFIX: &str = "zset_lifted_h/";
pub(crate) const LIFTED_SELECT_STREAM_PREFIX: &str = "stream_lifted_select/";
pub(crate) const LIFTED_PROJECT_STREAM_PREFIX: &str = "stream_lifted_project/";
pub(crate) const LIFTED_JOIN_STREAM_PREFIX: &str = "stream_lifted_join/";
pub(crate) const LIFTED_H_STREAM_PREFIX: &str = "stream_lifted_h/";
pub(crate) const ZSET_SUM_PREFIX: &str = "zset_sum/";
pub(crate) const ZSET_INTEGRAL_PREFIX: &str = "zset_integral/";
pub(crate) const ZSET_INTEGRAL_STREAM_PREFIX: &str = "stream_zset_integral/";
pub(crate) const DELTA_LIFTED_JOIN_STREAM_PREFIX: &str = "stream_delta_lifted_join/";
pub(crate) const DELTA_NAMESPACE_SUFFIX: &str = "/delta";

fn dictionary_namespace_for_handle(ns: &str) -> &str {
    ns.strip_suffix(DELTA_NAMESPACE_SUFFIX).unwrap_or(ns)
}

pub(crate) fn delta_handle_namespace(namespace: &str) -> String {
    format!("{namespace}{DELTA_NAMESPACE_SUFFIX}")
}

pub(crate) async fn open_delta_handle_stream(
    input: &Stream<ZSetHandle>,
) -> Result<Stream<ZSetHandle>> {
    let delta_namespace = delta_handle_namespace(input.namespace());
    let default_hint = ZSetHandle {
        ns: delta_namespace.clone(),
        version: 0,
    };
    let group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(default_hint));
    Stream::with_table(input.table(), delta_namespace, group)
        .await
        .context("open companion delta handle stream")
}

pub(crate) async fn collect_values<T>(stream: &Stream<T>, up_to: i64) -> Result<Vec<T>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut clone = stream.clone();
    if up_to < 0 {
        return Ok(Vec::new());
    }
    let mut values = Vec::with_capacity((up_to + 1) as usize);
    for t in 0..=up_to {
        values.push(clone.get(t).await?);
    }
    Ok(values)
}

pub(crate) fn next_derived_namespace(prefix: &str) -> String {
    let id = DERIVED_NAMESPACE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}{}", prefix, id)
}

pub(crate) fn next_lifted_zset_namespace(prefix: &str) -> String {
    let id = LIFTED_ZSET_NAMESPACE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}{id}")
}

pub(crate) async fn build_derived_stream<T>(
    table: Arc<dyn KeyValueTable>,
    group: Arc<dyn AbelianGroup<T>>,
    prefix: &str,
) -> Result<Stream<T>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let namespace = next_derived_namespace(prefix);
    Stream::with_table(table, namespace, group).await
}

pub(crate) async fn build_exact_stream_from_values<T>(
    table: Arc<dyn KeyValueTable>,
    group: Arc<dyn AbelianGroup<T>>,
    prefix: &str,
    frontier: i64,
    horizon: i64,
    values: &[T],
    tail_default: T,
) -> Result<Stream<T>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut result = build_derived_stream(table, group, prefix).await?;

    if let Some(first) = values.first() {
        set_default_in_place(&mut result, first.clone());

        for t in 1..=frontier {
            push_value_in_place(&mut result, values[t as usize].clone());
        }

        if first != &tail_default {
            set_default_at_in_place(&result, frontier + 1, tail_default.clone());
        }

        if horizon > frontier {
            for t in (frontier + 1)..=horizon {
                set_value_at_in_place(&result, t, values[t as usize].clone());
            }
        }
    }

    Ok(result)
}

pub(crate) async fn publish_scheduled_value<T>(stream: &mut Stream<T>, ts: i64) -> Result<()>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let value = stream
        .get(ts)
        .await
        .with_context(|| format!("load scheduled stream value at {ts}"))?;
    push_value_in_place(stream, value);
    stream.flush().await?;
    Ok(())
}

pub(crate) async fn apply_on_resolved_handles<T, Fut>(
    input: &Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<T>>,
    namespace_prefix: &str,
    mut op: impl FnMut(Stream<T>) -> Fut,
) -> Result<Stream<StreamHandle>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    Fut: Future<Output = Result<Stream<T>>>,
{
    let frontier = input.current_time();
    let horizon = input.semantic_horizon();
    let handles = collect_values(input, horizon).await?;
    let mut derived_handles = Vec::with_capacity(handles.len());

    for handle in &handles {
        let inner = input
            .resolve_handle(handle, inner_group.clone())
            .await
            .context("resolve handle for lifted operator")?;
        let mut derived = op(inner).await?;
        derived.flush().await?;
        derived_handles.push(derived.handle());
    }

    let input_default_handle = input.default_value();
    let default_handle = if let Some(existing) = handles
        .iter()
        .zip(derived_handles.iter())
        .find_map(|(handle, derived)| {
            if *handle == input_default_handle {
                Some(derived.clone())
            } else {
                None
            }
        }) {
        existing
    } else {
        let default_inner = input
            .resolve_handle(&input_default_handle, inner_group)
            .await
            .context("resolve default handle for lifted operator")?;
        let mut default_derived = op(default_inner).await?;
        default_derived.flush().await?;
        default_derived.handle()
    };
    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result = build_exact_stream_from_values(
        input.table(),
        handle_group,
        namespace_prefix,
        frontier,
        horizon,
        &derived_handles,
        default_handle,
    )
    .await?;

    result.flush().await?;
    Ok(result)
}

pub(crate) async fn resolve_apply_handle_op<T, Fut>(
    outer: &Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<T>>,
    op: impl FnMut(Stream<T>) -> Fut,
    out_prefix: &str,
) -> Result<Stream<StreamHandle>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    Fut: Future<Output = Result<Stream<T>>>,
{
    apply_on_resolved_handles(outer, inner_group, out_prefix, op).await
}

// Use this for operators that require the full integrated ZSet snapshot.
pub async fn materialize_zset_handle<K>(
    table: Arc<dyn KeyValueTable>,
    cache: &mut HashMap<String, Arc<Dictionary<K>>>,
    handle: &ZSetHandle,
) -> Result<HashMap<K, i64>>
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
    let total_start = Instant::now();
    let dict_ns = dictionary_namespace_for_handle(&handle.ns);
    let dict_open_start = Instant::now();
    let (dict, dict_cache_hit) = if let Some(existing) = cache.get(dict_ns) {
        (existing.clone(), true)
    } else {
        let dictionary = Arc::new(
            Dictionary::with_table(table.clone(), dict_ns.to_string(), None)
                .await
                .context("open dictionary for ZSet handle")?,
        );
        cache.insert(dict_ns.to_string(), dictionary.clone());
        (dictionary, false)
    };
    let dict_open_ms = dict_open_start.elapsed().as_millis() as u64;

    let view = ZSetHandleView::new(dict, table, handle.ns.clone(), handle.version);
    let materialize_start = Instant::now();
    let mut map = view
        .materialize()
        .await
        .context("materialize ZSet handle")?;
    let materialize_ms = materialize_start.elapsed().as_millis() as u64;
    let rows_before_retain = map.len();
    map.retain(|_, weight| *weight != 0);
    tracing::debug!(
        namespace = %handle.ns,
        version = handle.version,
        dict_ns,
        dict_cache_hit,
        dict_open_ms,
        rows_before_retain,
        rows_after_retain = map.len(),
        materialize_ms,
        total_ms = total_start.elapsed().as_millis() as u64,
        "zset handle materialize breakdown"
    );
    Ok(map)
}

// Use this for delta-first operators that only need the newest layer.
pub async fn delta_zset_handle<K>(
    table: Arc<dyn KeyValueTable>,
    cache: &mut HashMap<String, Arc<Dictionary<K>>>,
    handle: &ZSetHandle,
) -> Result<Vec<(K, i64)>>
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
    let total_start = Instant::now();
    let dict_ns = dictionary_namespace_for_handle(&handle.ns);
    let dict_open_start = Instant::now();
    let (dict, dict_cache_hit) = if let Some(existing) = cache.get(dict_ns) {
        (existing.clone(), true)
    } else {
        let dictionary = Arc::new(
            Dictionary::with_table(table.clone(), dict_ns.to_string(), None)
                .await
                .context("open dictionary for ZSet handle")?,
        );
        cache.insert(dict_ns.to_string(), dictionary.clone());
        (dictionary, false)
    };
    let dict_open_ms = dict_open_start.elapsed().as_millis() as u64;

    let view = ZSetHandleView::new(dict, table, handle.ns.clone(), handle.version);
    let delta_iter_start = Instant::now();
    let mut deltas = view
        .delta_iter()
        .await
        .context("delta iterate ZSet handle")?;
    let delta_iter_ms = delta_iter_start.elapsed().as_millis() as u64;
    let rows_before_retain = deltas.len();
    deltas.retain(|(_, delta)| *delta != 0);
    tracing::debug!(
        namespace = %handle.ns,
        version = handle.version,
        dict_ns,
        dict_cache_hit,
        dict_open_ms,
        rows_before_retain,
        rows_after_retain = deltas.len(),
        delta_iter_ms,
        total_ms = total_start.elapsed().as_millis() as u64,
        "zset handle delta_iter breakdown"
    );
    Ok(deltas)
}

pub fn compute_delta<K>(previous: &HashMap<K, i64>, next: &HashMap<K, i64>) -> Vec<(K, i64)>
where
    K: Eq + Hash + Clone,
{
    let mut deltas = Vec::new();

    for (key, &next_weight) in next {
        let prev_weight = previous.get(key).copied().unwrap_or(0);
        if next_weight != prev_weight {
            deltas.push((key.clone(), next_weight - prev_weight));
        }
    }

    for (key, &prev_weight) in previous {
        if !next.contains_key(key) && prev_weight != 0 {
            deltas.push((key.clone(), -prev_weight));
        }
    }

    deltas.retain(|(_, delta)| *delta != 0);
    deltas
}

pub(crate) fn set_default_in_place<T>(stream: &mut Stream<T>, value: T)
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    stream.set_default_in_place(value);
}

pub(crate) fn set_default_at_in_place<T>(stream: &Stream<T>, timestamp: i64, value: T)
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    stream.set_default_at_in_place(timestamp, value);
}

pub(crate) fn push_value_in_place<T>(stream: &mut Stream<T>, value: T)
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    stream.push_value_in_place(value);
}

pub(crate) fn set_value_at_in_place<T>(stream: &Stream<T>, timestamp: i64, value: T)
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    stream.set_value_at_in_place(timestamp, value);
}
