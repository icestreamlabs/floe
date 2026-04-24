use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::algebra::AbelianGroup;
use crate::collections::zset::VersionedZSet;
use crate::handles::{StreamHandle, ZSetHandle, ZSetHandleView};
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::core::stream::{Stream, StreamEvaluator};
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
const TRANSIENT_ZSET_BATCH_REGISTRY_MAX_ENTRIES: usize = 512;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TransientZSetBatchKey {
    namespace: String,
    version: u64,
    type_id: TypeId,
}

#[derive(Default)]
struct TransientZSetBatchRegistry {
    entries: HashMap<TransientZSetBatchKey, Arc<dyn Any + Send + Sync>>,
    order: VecDeque<TransientZSetBatchKey>,
}

static TRANSIENT_ZSET_BATCH_REGISTRY: LazyLock<Mutex<TransientZSetBatchRegistry>> =
    LazyLock::new(|| Mutex::new(TransientZSetBatchRegistry::default()));

fn transient_zset_batch_key<K>(handle: &ZSetHandle) -> TransientZSetBatchKey
where
    K: Send + Sync + 'static,
{
    TransientZSetBatchKey {
        namespace: handle.ns.clone(),
        version: handle.version,
        type_id: TypeId::of::<K>(),
    }
}

fn evict_excess_transient_zset_batches(registry: &mut TransientZSetBatchRegistry) {
    while registry.entries.len() > TRANSIENT_ZSET_BATCH_REGISTRY_MAX_ENTRIES {
        let Some(candidate) = registry.order.pop_front() else {
            break;
        };
        registry.entries.remove(&candidate);
    }
}

pub fn publish_transient_zset_batch<K>(handle: &ZSetHandle, batch: Arc<Vec<(K, i64)>>)
where
    K: Send + Sync + 'static,
{
    if handle.version == 0 || batch.is_empty() {
        return;
    }

    let key = transient_zset_batch_key::<K>(handle);
    let payload: Arc<dyn Any + Send + Sync> = Arc::new(batch) as Arc<dyn Any + Send + Sync>;
    let mut registry = TRANSIENT_ZSET_BATCH_REGISTRY
        .lock()
        .expect("transient zset batch registry lock poisoned");
    registry.entries.insert(key.clone(), payload);
    registry.order.push_back(key);
    evict_excess_transient_zset_batches(&mut registry);
}

pub fn transient_zset_batch<K>(handle: &ZSetHandle) -> Option<Arc<Vec<(K, i64)>>>
where
    K: Send + Sync + 'static,
{
    if handle.version == 0 {
        return Some(Arc::new(Vec::new()));
    }

    let key = transient_zset_batch_key::<K>(handle);
    let payload = {
        let registry = TRANSIENT_ZSET_BATCH_REGISTRY
            .lock()
            .expect("transient zset batch registry lock poisoned");
        registry.entries.get(&key).cloned()
    }?;
    let batch = Arc::downcast::<Arc<Vec<(K, i64)>>>(payload).ok()?;
    Some(batch.as_ref().clone())
}

fn dictionary_namespace_for_handle(ns: &str) -> &str {
    ns.strip_suffix(DELTA_NAMESPACE_SUFFIX).unwrap_or(ns)
}

/// Reusable decoder for delta handle rows that avoids reopening the underlying
/// versioned ZSet for every observed handle version.
pub struct DeltaZSetHandleReader<K>
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
    table: Arc<dyn KeyValueTable>,
    dict_cache: HashMap<String, Arc<Dictionary<K>>>,
    zset_cache: HashMap<String, VersionedZSet<K>>,
}

impl<K> DeltaZSetHandleReader<K>
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
    pub fn new(table: Arc<dyn KeyValueTable>) -> Self {
        Self {
            table,
            dict_cache: HashMap::new(),
            zset_cache: HashMap::new(),
        }
    }

    pub async fn read(&mut self, handle: &ZSetHandle) -> Result<Vec<(K, i64)>> {
        if handle.version == 0 {
            return Ok(Vec::new());
        }
        if let Some(batch) = transient_zset_batch::<K>(handle) {
            tracing::debug!(
                namespace = %handle.ns,
                version = handle.version,
                rows = batch.len(),
                "zset handle reader transient delta hit"
            );
            return Ok(batch.as_ref().clone());
        }

        let total_start = Instant::now();
        let dict_ns = dictionary_namespace_for_handle(&handle.ns);

        let dict_open_start = Instant::now();
        let (dict, dict_cache_hit) = if let Some(existing) = self.dict_cache.get(dict_ns) {
            (existing.clone(), true)
        } else {
            let dictionary = Arc::new(
                Dictionary::with_table(self.table.clone(), dict_ns.to_string(), None)
                    .await
                    .context("open dictionary for ZSet handle")?,
            );
            self.dict_cache
                .insert(dict_ns.to_string(), dictionary.clone());
            (dictionary, false)
        };
        let dict_open_ms = dict_open_start.elapsed().as_millis() as u64;

        let zset_open_start = Instant::now();
        if !self.zset_cache.contains_key(&handle.ns) {
            let versioned = VersionedZSet::new(dict, self.table.clone(), handle.ns.clone())
                .await
                .context("open versioned ZSet for delta handle reader")?;
            self.zset_cache.insert(handle.ns.clone(), versioned);
        }
        let zset_open_ms = zset_open_start.elapsed().as_millis() as u64;

        let delta_iter_start = Instant::now();
        let zset = self
            .zset_cache
            .get(&handle.ns)
            .ok_or_else(|| anyhow::anyhow!("missing cached versioned ZSet for {}", handle.ns))?;
        let mut deltas = zset
            .delta_iter_with_dict(handle.version)
            .await
            .context("delta iterate ZSet handle")?;
        let delta_iter_ms = delta_iter_start.elapsed().as_millis() as u64;
        let rows_before_retain = deltas.len();
        deltas.retain(|(_, delta)| *delta != 0);
        publish_transient_zset_batch(handle, Arc::new(deltas.clone()));

        tracing::debug!(
            namespace = %handle.ns,
            version = handle.version,
            dict_ns,
            dict_cache_hit,
            dict_open_ms,
            zset_open_ms,
            rows_before_retain,
            rows_after_retain = deltas.len(),
            delta_iter_ms,
            total_ms = total_start.elapsed().as_millis() as u64,
            "zset handle delta reader breakdown"
        );
        Ok(deltas)
    }
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

pub(crate) async fn build_evaluated_stream<T>(
    table: Arc<dyn KeyValueTable>,
    group: Arc<dyn AbelianGroup<T>>,
    evaluator: Arc<dyn StreamEvaluator<T>>,
    namespace_prefix: &str,
    frontier: i64,
    horizon: i64,
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
    let namespace = next_derived_namespace(namespace_prefix);
    let mut result = Stream::evaluated_with_table(table, namespace, group, evaluator).await?;

    for t in 0..=horizon {
        let value = result
            .derived_value_at(t)
            .await?
            .expect("evaluated stream missing evaluator");
        if t == 0 {
            set_default_in_place(&mut result, value);
        } else if t <= frontier {
            push_value_in_place(&mut result, value);
        } else {
            set_value_at_in_place(&result, t, value);
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

struct HandleOpEvaluator<T, F, Fut>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(Stream<T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Stream<T>>> + Send + 'static,
{
    input: Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<T>>,
    op: Arc<F>,
}

#[async_trait::async_trait]
impl<T, F, Fut> StreamEvaluator<StreamHandle> for HandleOpEvaluator<T, F, Fut>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(Stream<T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Stream<T>>> + Send + 'static,
{
    async fn value_at(
        &self,
        timestamp: i64,
        _group: Arc<dyn AbelianGroup<StreamHandle>>,
    ) -> Result<StreamHandle> {
        let mut input = self.input.clone();
        let handle = input.get(timestamp).await?;
        let inner = self
            .input
            .resolve_handle(&handle, self.inner_group.clone())
            .await
            .context("resolve handle for lifted evaluator")?;
        let mut derived = (self.op)(inner).await?;
        derived.flush().await?;
        Ok(derived.handle())
    }
}

pub(crate) async fn apply_on_resolved_handles<T, F, Fut>(
    input: &Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<T>>,
    namespace_prefix: &str,
    op: F,
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
    F: Fn(Stream<T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Stream<T>>> + Send + 'static,
{
    let frontier = input.current_time();
    let horizon = input.semantic_horizon();
    let mut input_for_identity = input.clone();
    let first_handle = input_for_identity.get(0).await?;
    let first_inner = input
        .resolve_handle(&first_handle, inner_group.clone())
        .await
        .context("resolve first handle for lifted operator identity")?;
    let mut first_derived = op(first_inner).await?;
    first_derived.flush().await?;
    let default_handle = first_derived.handle();
    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));

    let mut result = build_evaluated_stream(
        input.table(),
        handle_group,
        Arc::new(HandleOpEvaluator {
            input: input.clone(),
            inner_group,
            op: Arc::new(op),
        }),
        namespace_prefix,
        frontier,
        horizon,
    )
    .await?;

    result.flush().await?;
    Ok(result)
}

pub(crate) async fn resolve_apply_handle_op<T, F, Fut>(
    outer: &Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<T>>,
    op: F,
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
    F: Fn(Stream<T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Stream<T>>> + Send + 'static,
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
    Ok(delta_zset_handle_batch(table, cache, handle)
        .await?
        .as_ref()
        .clone())
}

pub async fn delta_zset_handle_batch<K>(
    table: Arc<dyn KeyValueTable>,
    cache: &mut HashMap<String, Arc<Dictionary<K>>>,
    handle: &ZSetHandle,
) -> Result<Arc<Vec<(K, i64)>>>
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
    if handle.version == 0 {
        return Ok(Arc::new(Vec::new()));
    }
    if let Some(batch) = transient_zset_batch::<K>(handle) {
        tracing::debug!(
            namespace = %handle.ns,
            version = handle.version,
            rows = batch.len(),
            "zset handle transient delta hit"
        );
        return Ok(batch);
    }

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
    let deltas = Arc::new(deltas);
    publish_transient_zset_batch(handle, Arc::clone(&deltas));
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

#[cfg(test)]
mod tests {
    use super::{delta_zset_handle_batch, publish_transient_zset_batch};
    use crate::handles::ZSetHandle;
    use crate::storage::{KeyValueTable, SlateTable};
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[tokio::test]
    async fn delta_zset_handle_batch_uses_transient_registry_before_storage() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("transient_zset_registry", store)
                .await
                .expect("open SlateDB"),
        );
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        let handle = ZSetHandle {
            ns: "transient_only_delta".to_string(),
            version: 7,
        };
        let expected = Arc::new(vec![("alpha".to_string(), 1), ("beta".to_string(), -1)]);
        publish_transient_zset_batch(&handle, Arc::clone(&expected));

        let actual = delta_zset_handle_batch::<String>(table, &mut HashMap::new(), &handle)
            .await
            .expect("load transient delta batch");

        assert_eq!(actual.as_ref(), expected.as_ref());
    }
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
