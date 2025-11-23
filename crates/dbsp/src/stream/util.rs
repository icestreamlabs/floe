use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::time::{Duration, sleep};

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
    if up_to > clone.current_time() {
        clone.get(up_to).await?;
    } else {
        clone.get(clone.current_time()).await?;
    }
    clone.to_vec().await
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
    let handles = collect_values(input, input.current_time()).await?;
    let mut derived_handles = Vec::with_capacity(handles.len());

    for handle in handles {
        let inner = input
            .resolve_handle(&handle, inner_group.clone())
            .await
            .context("resolve handle for lifted operator")?;
        let mut derived = op(inner).await?;
        derived.flush().await?;
        derived_handles.push(derived.handle());
    }

    let default_handle = derived_handles
        .first()
        .cloned()
        .unwrap_or_else(|| input.default_value());
    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result = build_derived_stream(input.table(), handle_group, namespace_prefix).await?;

    if derived_handles.is_empty() {
        set_default_in_place(&mut result, default_handle);
    } else {
        set_default_in_place(&mut result, derived_handles[0].clone());
        for handle in derived_handles.iter().skip(1) {
            push_value_in_place(&mut result, handle.clone());
        }
        if let Some(last) = derived_handles.last() {
            set_default_in_place(&mut result, last.clone());
        }
    }

    result.flush().await?;
    Ok(result)
}

pub(crate) async fn resolve_apply_handle_op<T, Fut>(
    outer: &Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<T>>,
    mut op: impl FnMut(Stream<T>) -> Fut,
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
    apply_on_resolved_handles(outer, inner_group, out_prefix, |inner| op(inner)).await
}

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
    let dict = if let Some(existing) = cache.get(&handle.ns) {
        existing.clone()
    } else {
        let dictionary = Arc::new(
            Dictionary::with_table(table.clone(), handle.ns.clone(), None)
                .await
                .context("open dictionary for ZSet handle")?,
        );
        cache.insert(handle.ns.clone(), dictionary.clone());
        dictionary
    };

    let view = ZSetHandleView::new(dict, table, handle.ns.clone(), handle.version);
    let mut attempts = 0;
    let mut map = loop {
        match view.materialize().await {
            Ok(map) => break map,
            Err(err) => {
                attempts += 1;
                if attempts > 3 || !err.to_string().contains("manifest version") {
                    return Err(err).context("materialize ZSet handle");
                }
                sleep(Duration::from_millis(10)).await;
            }
        }
    };
    map.retain(|_, weight| *weight != 0);
    Ok(map)
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
