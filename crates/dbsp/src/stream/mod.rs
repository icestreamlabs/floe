use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::convert::TryFrom;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::config::ScanOptions;
use slatedb::{Db, WriteBatch};

use crate::algebra::AbelianGroup;
use crate::collections::zset::{self, SegmentRecord, VersionedZSet, ZSet};
use crate::handles::{StreamHandle, ZSetHandle, ZSetHandleView};
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{self, RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::storage::keyspace::{self, namespace_prefix};
use crate::storage::timestamps;
use crate::storage::{KeyValueTable, SlateTable};

/// A SlateDB-backed port of `pydbsp.stream.Stream`.
///
/// The stream persists non-default values and default change events in SlateDB while
/// keeping only the working set (pending writes, cached lookups) in memory.
pub struct Stream<T>
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
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    data_prefix: Vec<u8>,
    default_prefix: Vec<u8>,
    state_key: Vec<u8>,
    group: Arc<dyn AbelianGroup<T>>,

    timestamp: i64,
    identity: bool,
    default: T,

    pending_data: BTreeMap<i64, T>,
    pending_defaults: BTreeMap<i64, T>,
    pending_state: bool,

    data_cache: HashMap<i64, T>,
    default_changes: BTreeMap<i64, T>,
    last_default_ts: i64,
}

impl<T> Clone for Stream<T>
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
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            namespace: self.namespace.clone(),
            data_prefix: self.data_prefix.clone(),
            default_prefix: self.default_prefix.clone(),
            state_key: self.state_key.clone(),
            group: self.group.clone(),
            timestamp: self.timestamp,
            identity: self.identity,
            default: self.default.clone(),
            pending_data: self.pending_data.clone(),
            pending_defaults: self.pending_defaults.clone(),
            pending_state: self.pending_state,
            data_cache: self.data_cache.clone(),
            default_changes: self.default_changes.clone(),
            last_default_ts: self.last_default_ts,
        }
    }
}

impl<T> Stream<T>
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
    pub async fn new(
        db: Arc<Db>,
        namespace: impl Into<String>,
        group: Arc<dyn AbelianGroup<T>>,
    ) -> Result<Self> {
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        Self::with_table(table, namespace, group).await
    }

    pub async fn with_table(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        group: Arc<dyn AbelianGroup<T>>,
    ) -> Result<Self> {
        let namespace = namespace.into();
        let base = namespace_prefix(keyspace::prefix::STREAM, &namespace);

        let mut data_prefix = base.clone();
        data_prefix.extend_from_slice(b"data/");

        let mut default_prefix = base.clone();
        default_prefix.extend_from_slice(b"default/");

        let mut state_key = base.clone();
        state_key.extend_from_slice(b"meta/state");

        let initial_default = group.identity().await;

        let mut stream = Self {
            table,
            namespace: namespace.clone(),
            data_prefix,
            default_prefix,
            state_key,
            group,
            timestamp: 0,
            identity: true,
            default: initial_default.clone(),
            pending_data: BTreeMap::new(),
            pending_defaults: BTreeMap::new(),
            pending_state: false,
            data_cache: HashMap::new(),
            default_changes: BTreeMap::new(),
            last_default_ts: 0,
        };

        stream.clear_intent().await?;

        if let Some(bytes) = stream.table.get(&stream.state_key).await? {
            let (timestamp, identity, default, last_default_ts) =
                if let Ok(tuple) = encoding::decode::<(i64, bool, T, i64)>(&bytes) {
                    tuple
                } else {
                    let (timestamp, identity, default) = encoding::decode::<(i64, bool, T)>(&bytes)
                        .context("unable to decode legacy stream state")?;
                    (timestamp, identity, default, timestamp)
                };
            stream.timestamp = timestamp;
            stream.identity = identity;
            stream.default = default.clone();
            stream.last_default_ts = last_default_ts;
            stream.default_changes = stream.load_default_changes().await?;
            stream.last_default_ts = stream.default_changes.keys().copied().max().unwrap_or(0);
            if stream
                .default_changes
                .range(..=stream.timestamp)
                .rev()
                .next()
                .map(|(_, value)| value.clone())
                .is_none()
            {
                stream.default_changes.insert(0, default.clone());
            }
        } else {
            stream.default = initial_default.clone();
            stream.default_changes.insert(0, initial_default.clone());
            stream.last_default_ts = 0;
            stream.pending_defaults.insert(0, initial_default.clone());
            stream.pending_state = true;
            stream.flush().await?;
        }

        stream.data_cache.reserve(16);
        Ok(stream)
    }

    pub async fn open_at(
        db: Arc<Db>,
        namespace: impl Into<String>,
        group: Arc<dyn AbelianGroup<T>>,
        frontier: i64,
    ) -> Result<Self> {
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        Self::open_at_with_table(table, namespace, group, frontier).await
    }

    pub async fn open_at_with_table(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        group: Arc<dyn AbelianGroup<T>>,
        frontier: i64,
    ) -> Result<Self> {
        let namespace = namespace.into();
        let mut stream = Self::with_table(table, namespace.clone(), group).await?;
        if frontier > stream.timestamp {
            stream.advance_to(frontier).await?;
        }

        // Ensure caches include state at the requested frontier for immediate reads.
        if frontier >= 0 {
            stream.get(frontier).await?;
        }

        Ok(stream)
    }

    pub fn group(&self) -> Arc<dyn AbelianGroup<T>> {
        self.group.clone()
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn current_time(&self) -> i64 {
        self.timestamp
    }

    pub fn is_identity(&self) -> bool {
        self.identity
    }

    pub fn handle(&self) -> StreamHandle {
        StreamHandle {
            ns: self.namespace.clone(),
            frontier: self.timestamp,
        }
    }

    pub async fn send(&mut self, element: T) -> Result<i64> {
        let next_timestamp = self.timestamp + 1;
        if element != self.default {
            self.pending_data.insert(next_timestamp, element.clone());
            self.data_cache.insert(next_timestamp, element);
            self.identity = false;
        }
        self.timestamp = next_timestamp;
        self.pending_state = true;
        Ok(next_timestamp)
    }

    pub async fn set_default(&mut self, new_default: T) -> Result<()> {
        self.default = new_default.clone();
        self.pending_defaults
            .insert(self.timestamp, new_default.clone());
        self.pending_state = true;
        Ok(())
    }

    pub async fn get(&mut self, timestamp: i64) -> Result<T> {
        if timestamp < 0 {
            return Err(anyhow!("timestamp cannot be negative"));
        }

        if timestamp > self.timestamp {
            self.advance_to(timestamp).await?;
        }

        if let Some(value) = self.pending_data.get(&timestamp) {
            return Ok(value.clone());
        }

        if let Some(value) = self.data_cache.get(&timestamp) {
            return Ok(value.clone());
        }

        let encoded_key = self.encode_data_key(timestamp)?;
        if let Some(bytes) = self.table.get(&encoded_key).await? {
            let value: T = encoding::decode(&bytes).context("unable to decode stream value")?;
            self.data_cache.insert(timestamp, value.clone());
            Ok(value)
        } else {
            Ok(self.default_at(timestamp))
        }
    }

    pub async fn latest(&mut self) -> Result<T> {
        self.get(self.timestamp).await
    }

    pub async fn to_vec(&mut self) -> Result<Vec<T>> {
        let mut values = Vec::with_capacity((self.timestamp + 1) as usize);
        for t in 0..=self.timestamp {
            values.push(self.get(t).await?);
        }
        Ok(values)
    }

    pub async fn flush(&mut self) -> Result<()> {
        let mut batch = WriteBatch::new();
        let mut dirty = false;

        if self.flush_defaults_into(&mut batch)? {
            dirty = true;
        }
        if self.flush_data_into(&mut batch)? {
            dirty = true;
        }
        if self.flush_state_into(&mut batch)? {
            dirty = true;
        }

        if dirty {
            let intent_key = self.encode_intent_key();
            batch.put(intent_key.clone(), vec![1]);
            self.table.write_batch(batch).await?;

            let mut clear_batch = WriteBatch::new();
            clear_batch.delete(intent_key);
            self.table.write_batch(clear_batch).await?;
        }

        Ok(())
    }

    fn flush_data_into(&mut self, batch: &mut WriteBatch) -> Result<bool> {
        if self.pending_data.is_empty() {
            return Ok(false);
        }

        let mut pending = BTreeMap::new();
        std::mem::swap(&mut pending, &mut self.pending_data);

        for (timestamp, value) in pending {
            let key = self.encode_data_key(timestamp)?;
            let encoded = encoding::encode(&value).context("unable to encode stream value")?;
            batch.put(key, encoded);
            self.data_cache.insert(timestamp, value);
        }

        Ok(true)
    }

    fn flush_defaults_into(&mut self, batch: &mut WriteBatch) -> Result<bool> {
        if self.pending_defaults.is_empty() {
            return Ok(false);
        }

        let mut pending = BTreeMap::new();
        std::mem::swap(&mut pending, &mut self.pending_defaults);

        for (timestamp, value) in pending {
            let key = self.encode_default_key(timestamp)?;
            let encoded = encoding::encode(&value).context("unable to encode default change")?;
            batch.put(key, encoded);
            self.default_changes.insert(timestamp, value);
            self.last_default_ts = self.last_default_ts.max(timestamp);
        }

        Ok(true)
    }

    fn flush_state_into(&mut self, batch: &mut WriteBatch) -> Result<bool> {
        if !self.pending_state {
            return Ok(false);
        }

        let state = (
            self.timestamp,
            self.identity,
            self.default.clone(),
            self.last_default_ts,
        );
        let encoded = encoding::encode(&state).context("unable to encode stream state")?;
        batch.put(self.state_key.clone(), encoded);
        self.pending_state = false;
        Ok(true)
    }

    async fn advance_to(&mut self, timestamp: i64) -> Result<()> {
        while self.timestamp < timestamp {
            self.send(self.default.clone()).await?;
        }
        Ok(())
    }

    fn default_at(&self, timestamp: i64) -> T {
        if let Some((_, value)) = self.pending_defaults.range(..=timestamp).next_back() {
            return value.clone();
        }

        if let Some((_, value)) = self.default_changes.range(..=timestamp).next_back() {
            return value.clone();
        }

        self.default.clone()
    }

    fn encode_data_key(&self, timestamp: i64) -> Result<Vec<u8>> {
        timestamps::append(self.data_prefix.as_slice(), timestamp)
    }

    fn encode_default_key(&self, timestamp: i64) -> Result<Vec<u8>> {
        timestamps::append(self.default_prefix.as_slice(), timestamp)
    }

    async fn clear_intent(&self) -> Result<()> {
        let intent_key = self.encode_intent_key();
        if self.table.get(&intent_key).await?.is_some() {
            let mut batch = WriteBatch::new();
            batch.delete(intent_key);
            self.table.write_batch(batch).await?;
        }
        Ok(())
    }

    fn encode_intent_key(&self) -> Vec<u8> {
        let mut key = self.state_key.clone();
        key.extend_from_slice(b"/intent");
        key
    }

    async fn load_default_changes(&self) -> Result<BTreeMap<i64, T>> {
        let entries = self
            .table
            .scan_prefix(self.default_prefix.as_slice(), &ScanOptions::default())
            .await?;

        let mut changes = BTreeMap::new();
        for (key_bytes, value_bytes) in entries {
            let timestamp =
                timestamps::extract(self.default_prefix.as_slice(), key_bytes.as_ref())?;
            let value: T = encoding::decode(value_bytes.as_ref())
                .context("unable to decode default change")?;
            changes.insert(timestamp, value);
        }
        Ok(changes)
    }
}

impl Stream<StreamHandle> {
    pub async fn resolve_handle<T>(
        &self,
        handle: &StreamHandle,
        group: Arc<dyn AbelianGroup<T>>,
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
        Stream::open_at_with_table(
            self.table.clone(),
            handle.ns.clone(),
            group,
            handle.frontier,
        )
        .await
    }

    pub async fn resolve_latest<T>(&mut self, group: Arc<dyn AbelianGroup<T>>) -> Result<Stream<T>>
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
        let handle = self.latest().await?;
        self.resolve_handle(&handle, group).await
    }
}

async fn collect_values<T>(stream: &Stream<T>, up_to: i64) -> Result<Vec<T>>
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
    if up_to > clone.timestamp {
        clone.get(up_to).await?;
    } else {
        clone.get(clone.timestamp).await?;
    }
    clone.to_vec().await
}

static DERIVED_NAMESPACE_COUNTER: AtomicU64 = AtomicU64::new(0);
static LIFTED_ZSET_NAMESPACE_COUNTER: AtomicU64 = AtomicU64::new(0);
const LIFTED_SELECT_ZSET_PREFIX: &str = "zset_lifted_select/";
const LIFTED_PROJECT_ZSET_PREFIX: &str = "zset_lifted_project/";
const LIFTED_JOIN_ZSET_PREFIX: &str = "zset_lifted_join/";
const LIFTED_H_ZSET_PREFIX: &str = "zset_lifted_h/";
const LIFTED_SELECT_STREAM_PREFIX: &str = "stream_lifted_select/";
const LIFTED_PROJECT_STREAM_PREFIX: &str = "stream_lifted_project/";
const LIFTED_JOIN_STREAM_PREFIX: &str = "stream_lifted_join/";
const LIFTED_H_STREAM_PREFIX: &str = "stream_lifted_h/";
const ZSET_SUM_PREFIX: &str = "zset_sum/";
const ZSET_INTEGRAL_PREFIX: &str = "zset_integral/";
const ZSET_INTEGRAL_STREAM_PREFIX: &str = "stream_zset_integral/";
const DELTA_LIFTED_JOIN_STREAM_PREFIX: &str = "stream_delta_lifted_join/";

fn next_derived_namespace(prefix: &str) -> String {
    let id = DERIVED_NAMESPACE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}{}", prefix, id)
}

fn next_lifted_zset_namespace(prefix: &str) -> String {
    let id = LIFTED_ZSET_NAMESPACE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}{id}")
}

async fn build_derived_stream<T>(
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

async fn apply_on_resolved_handles<T, Fut>(
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
    let handles = collect_values(input, input.timestamp).await?;
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
        .unwrap_or_else(|| input.default.clone());
    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result =
        build_derived_stream(input.table.clone(), handle_group, namespace_prefix).await?;

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

async fn resolve_apply_handle_op<T, Fut>(
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

async fn materialize_zset_handle<K>(
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
    let mut map = view
        .materialize()
        .await
        .context("materialize ZSet handle")?;
    map.retain(|_, weight| *weight != 0);
    Ok(map)
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
    let handles = collect_values(stream, stream.timestamp).await?;
    let table = stream.table.clone();
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

fn compute_delta<K>(previous: &HashMap<K, i64>, next: &HashMap<K, i64>) -> Vec<(K, i64)>
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

fn set_default_in_place<T>(stream: &mut Stream<T>, value: T)
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
    stream.default = value.clone();
    stream.pending_defaults.insert(stream.timestamp, value);
    stream.pending_state = true;
}

fn push_value_in_place<T>(stream: &mut Stream<T>, value: T)
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
    let next_timestamp = stream.timestamp + 1;
    if value != stream.default {
        stream.pending_data.insert(next_timestamp, value.clone());
        stream.data_cache.insert(next_timestamp, value);
        stream.identity = false;
    }
    stream.timestamp = next_timestamp;
    stream.pending_state = true;
}

pub async fn delay<T>(input: &Stream<T>) -> Result<Stream<T>>
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
    let values = collect_values(input, input.timestamp).await?;
    let mut result =
        build_derived_stream(input.table.clone(), input.group.clone(), "stream_delay/").await?;

    let mut last_output = None;
    for t in 1..=input.timestamp {
        let value = values[(t - 1) as usize].clone();
        push_value_in_place(&mut result, value.clone());
        last_output = Some(value);
    }

    if let Some(last) = last_output {
        set_default_in_place(&mut result, last);
    }

    Ok(result)
}

pub async fn differentiate<T>(input: &Stream<T>) -> Result<Stream<T>>
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
    let values = collect_values(input, input.timestamp).await?;
    let group = input.group.clone();
    let mut result =
        build_derived_stream(input.table.clone(), group.clone(), "stream_diff/").await?;

    if let Some(first) = values.first() {
        let mut last_output = first.clone();
        set_default_in_place(&mut result, first.clone());

        for t in 1..=input.timestamp {
            let current = &values[t as usize];
            let previous = &values[(t - 1) as usize];
            let neg_prev = group.neg(previous).await;
            let diff = group.add(current, &neg_prev).await;
            last_output = diff.clone();
            push_value_in_place(&mut result, diff);
        }

        set_default_in_place(&mut result, last_output);
    }

    Ok(result)
}

pub async fn integrate<T>(input: &Stream<T>) -> Result<Stream<T>>
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
    let values = collect_values(input, input.timestamp).await?;
    let group = input.group.clone();
    let mut result =
        build_derived_stream(input.table.clone(), group.clone(), "stream_integrate/").await?;

    if let Some(first) = values.first() {
        let mut acc = first.clone();
        set_default_in_place(&mut result, acc.clone());

        for t in 1..=input.timestamp {
            let current = &values[t as usize];
            acc = group.add(&acc, current).await;
            push_value_in_place(&mut result, acc.clone());
        }

        set_default_in_place(&mut result, acc);
    }

    Ok(result)
}

pub async fn lift1<I, O, F>(
    input: &Stream<I>,
    output_group: Arc<dyn AbelianGroup<O>>,
    function: F,
) -> Result<Stream<O>>
where
    I: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    I::Archived: RkyvDeserialize<I, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(&I) -> O + Send + Sync,
{
    let values = collect_values(input, input.timestamp).await?;
    let mut result =
        build_derived_stream(input.table.clone(), output_group.clone(), "stream_lift1/").await?;

    if let Some(first) = values.first() {
        let mut last = function(first);
        set_default_in_place(&mut result, last.clone());

        for t in 1..=input.timestamp {
            let value = function(&values[t as usize]);
            last = value.clone();
            push_value_in_place(&mut result, value);
        }

        set_default_in_place(&mut result, last);
    }

    Ok(result)
}

pub async fn lift2<L, R, O, F>(
    left: &Stream<L>,
    right: &Stream<R>,
    output_group: Arc<dyn AbelianGroup<O>>,
    function: F,
) -> Result<Stream<O>>
where
    L: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(&L, &R) -> O + Send + Sync,
{
    let frontier = left.timestamp.max(right.timestamp);
    let left_values = collect_values(left, frontier).await?;
    let right_values = collect_values(right, frontier).await?;
    let mut result =
        build_derived_stream(left.table.clone(), output_group.clone(), "stream_lift2/").await?;

    if let Some((first_left, first_right)) = left_values.first().zip(right_values.first()) {
        let mut last = function(first_left, first_right);
        set_default_in_place(&mut result, last.clone());

        for t in 1..=frontier {
            let value = function(&left_values[t as usize], &right_values[t as usize]);
            last = value.clone();
            push_value_in_place(&mut result, value);
        }

        set_default_in_place(&mut result, last);
    }

    Ok(result)
}

pub async fn incrementalize2<T, R, O, F>(
    left: &Stream<T>,
    right: &Stream<R>,
    output_group: Arc<dyn AbelianGroup<O>>,
    function: F,
) -> Result<Stream<O>>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(&T, &R) -> O + Send + Sync + Clone + 'static,
{
    let integrated_left = integrate(left).await?;
    let delayed_integrated_left = delay(&integrated_left).await?;

    let integrated_right = integrate(right).await?;
    let delayed_integrated_right = delay(&integrated_right).await?;

    let f_ab = lift2(left, right, output_group.clone(), function.clone()).await?;
    let f_a_delayed_b = lift2(
        left,
        &delayed_integrated_right,
        output_group.clone(),
        function.clone(),
    )
    .await?;
    let f_delayed_a_b = lift2(
        &delayed_integrated_left,
        right,
        output_group.clone(),
        function,
    )
    .await?;

    let addition = StreamAddition::from_stream(&f_ab);
    let partial = addition.add(&f_ab, &f_a_delayed_b).await;
    let summed = addition.add(&partial, &f_delayed_a_b).await;
    Ok(summed)
}

pub async fn stream_introduction<T>(
    table: Arc<dyn KeyValueTable>,
    group: Arc<dyn AbelianGroup<T>>,
    value: T,
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
    let mut stream = build_derived_stream(table, group.clone(), "stream_intro/").await?;
    set_default_in_place(&mut stream, value);
    stream.flush().await?;
    Ok(stream)
}

pub async fn stream_elimination<T>(stream: &Stream<T>) -> Result<T>
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
    let values = collect_values(stream, stream.timestamp).await?;
    let group = stream.group();
    let mut acc = group.identity().await;
    for value in values {
        acc = group.add(&acc, &value).await;
    }
    Ok(acc)
}

pub async fn lifted_delay<T>(
    stream: &Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<T>>,
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
{
    resolve_apply_handle_op(
        stream,
        inner_group,
        |inner| async move { delay(&inner).await },
        "stream_lift_delay/",
    )
    .await
}

pub async fn lifted_integrate<T>(
    stream: &Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<T>>,
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
{
    resolve_apply_handle_op(
        stream,
        inner_group,
        |inner| async move { integrate(&inner).await },
        "stream_lift_integrate/",
    )
    .await
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

pub async fn lifted_differentiate<T>(
    stream: &Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<T>>,
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
{
    resolve_apply_handle_op(
        stream,
        inner_group,
        |inner| async move { differentiate(&inner).await },
        "stream_lift_differentiate/",
    )
    .await
}

pub async fn lifted_stream_introduction<T>(stream: &Stream<T>) -> Result<Stream<StreamHandle>>
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
    let values = collect_values(stream, stream.timestamp).await?;
    let group = stream.group();
    let table = stream.table.clone();

    let mut outputs = Vec::with_capacity(values.len());
    for value in &values {
        let mut introduced =
            stream_introduction(table.clone(), group.clone(), value.clone()).await?;
        introduced.flush().await?;
        outputs.push(introduced.handle());
    }

    let default_handle = if let Some(first) = outputs.first() {
        first.clone()
    } else {
        let identity = group.identity().await;
        let mut identity_stream =
            stream_introduction(table.clone(), group.clone(), identity).await?;
        identity_stream.flush().await?;
        identity_stream.handle()
    };

    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result = build_derived_stream(table, handle_group, "stream_lift_intro/").await?;

    if outputs.is_empty() {
        set_default_in_place(&mut result, default_handle);
    } else {
        set_default_in_place(&mut result, outputs[0].clone());
        for handle in outputs.iter().skip(1) {
            push_value_in_place(&mut result, handle.clone());
        }
        if let Some(last) = outputs.last() {
            set_default_in_place(&mut result, last.clone());
        }
    }

    Ok(result)
}

pub async fn lifted_stream_elimination<T>(
    stream: &Stream<StreamHandle>,
    inner_group: Arc<dyn AbelianGroup<T>>,
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
    let handles = collect_values(stream, stream.timestamp).await?;
    let mut outputs = Vec::with_capacity(handles.len());
    for handle in &handles {
        let inner = stream
            .resolve_handle(handle, inner_group.clone())
            .await
            .context("resolve handle for lifted stream elimination")?;
        outputs.push(stream_elimination(&inner).await?);
    }

    let default_value = if let Some(first) = outputs.first() {
        first.clone()
    } else {
        inner_group.identity().await
    };

    let mut result = build_derived_stream(
        stream.table.clone(),
        inner_group.clone(),
        "stream_lift_elim/",
    )
    .await?;

    if outputs.is_empty() {
        set_default_in_place(&mut result, default_value);
    } else {
        set_default_in_place(&mut result, outputs[0].clone());
        for value in outputs.iter().skip(1) {
            push_value_in_place(&mut result, value.clone());
        }
        if let Some(last) = outputs.last() {
            set_default_in_place(&mut result, last.clone());
        }
    }

    Ok(result)
}

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
    let diff_handles = collect_values(diff_stream, diff_stream.timestamp).await?;
    let state_handles = collect_values(integrated_stream, integrated_stream.timestamp).await?;
    let total = diff_handles.len().min(state_handles.len());
    let table = diff_stream.table.clone();
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
        let diff_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(ZSetHandle {
                ns: diff_handles[t].ns.clone(),
                version: 0,
            }));
        let state_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(ZSetHandle {
                ns: state_handles[t].ns.clone(),
                version: 0,
            }));

        let diff_inner = diff_stream
            .resolve_handle(&diff_handles[t], diff_group.clone())
            .await
            .context("resolve diff stream for lifted-lifted H")?;
        let state_inner = integrated_stream
            .resolve_handle(&state_handles[t], state_group.clone())
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

pub async fn delta_lifted_delta_lifted_join<L, R, O, P, F>(
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
    let left_inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(ZSetHandle {
            ns: left.default.ns.clone(),
            version: 0,
        }));
    let right_inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
        Arc::new(HandleGroup::new(ZSetHandle {
            ns: right.default.ns.clone(),
            version: 0,
        }));

    let int_l = lifted_integrate_zset::<L>(left, left_inner_group.clone()).await?;
    let d_int_l = lifted_delay(&int_l, left_inner_group.clone()).await?;
    let i_int_l = lifted_integrate_zset::<L>(&int_l, left_inner_group.clone()).await?;

    let int_r = lifted_integrate_zset::<R>(right, right_inner_group.clone()).await?;
    let d_int_r = lifted_delay(&int_r, right_inner_group.clone()).await?;
    let i_int_r = lifted_integrate_zset::<R>(&int_r, right_inner_group.clone()).await?;
    let d_i_int_r = lifted_delay(&i_int_r, right_inner_group.clone()).await?;

    let join1 = lifted_lifted_join_zset_stream::<L, R, O, _, _>(
        &d_int_l,
        &d_int_r,
        predicate.clone(),
        projector.clone(),
    )
    .await?;

    let join2 = lifted_lifted_join_zset_stream::<L, R, O, _, _>(
        &i_int_l,
        right,
        predicate.clone(),
        projector.clone(),
    )
    .await?;

    let join3 = lifted_lifted_join_zset_stream::<L, R, O, _, _>(
        &int_l,
        &d_int_r,
        predicate.clone(),
        projector.clone(),
    )
    .await?;

    let join4 =
        lifted_lifted_join_zset_stream::<L, R, O, _, _>(left, &d_i_int_r, predicate, projector)
            .await?;

    let mut total_ts = join1
        .timestamp
        .min(join2.timestamp)
        .min(join3.timestamp)
        .min(join4.timestamp);
    if total_ts < 0 {
        total_ts = 0;
    }

    let table = left.table.clone();

    let mut components = [join1, join2, join3, join4];
    let mut caches: Vec<HashMap<String, Arc<Dictionary<O>>>> =
        vec![HashMap::new(); components.len()];

    let ns = next_lifted_zset_namespace(ZSET_SUM_PREFIX);
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), ns.clone(), None)
            .await
            .context("build dictionary for delta lifted join")?,
    );
    let mut aggregator = ZSetStream::new(dict, table.clone(), ns, StreamRetention::None)
        .await
        .context("create aggregator stream for delta lifted join")?;

    let mut previous: HashMap<O, i64> = HashMap::new();
    let capacity = usize::try_from(total_ts.saturating_add(1)).unwrap_or(usize::MAX);
    let mut aggregated_handles = Vec::with_capacity(capacity);

    // Align strictly across components by stopping at the shortest timeline.
    for t in 0..=total_ts {
        let mut combined: HashMap<O, i64> = HashMap::new();

        for (idx, component) in components.iter_mut().enumerate() {
            let handle = component
                .get(t)
                .await
                .context("read component handle for delta lifted join")?;
            let inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
                Arc::new(HandleGroup::new(ZSetHandle {
                    ns: handle.ns.clone(),
                    version: 0,
                }));
            let mut resolved = component
                .resolve_handle(&handle, inner_group)
                .await
                .context("resolve component inner stream")?;
            let zset_handle = resolved
                .latest()
                .await
                .context("read component zset handle")?;
            let map = materialize_zset_handle::<O>(table.clone(), &mut caches[idx], &zset_handle)
                .await
                .context("materialize component zset")?;

            for (key, weight) in map {
                let entry = combined.entry(key).or_insert(0);
                *entry = (*entry).saturating_add(weight);
            }
        }

        combined.retain(|_, weight| *weight != 0);
        let deltas = compute_delta(&previous, &combined);
        aggregator.add_deltas(deltas);
        aggregator
            .flush()
            .await
            .context("flush aggregated zset stream")?;
        previous = combined;

        aggregated_handles.push(aggregator.stream.handle());
    }

    let fallback_handle = aggregator.stream.handle();
    let default_handle = aggregated_handles
        .first()
        .cloned()
        .unwrap_or(fallback_handle);
    let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
        Arc::new(HandleGroup::new(default_handle.clone()));
    let mut result_stream =
        build_derived_stream(table.clone(), handle_group, DELTA_LIFTED_JOIN_STREAM_PREFIX).await?;

    if aggregated_handles.is_empty() {
        set_default_in_place(&mut result_stream, default_handle);
    } else {
        set_default_in_place(&mut result_stream, aggregated_handles[0].clone());
        for handle in aggregated_handles.iter().skip(1) {
            push_value_in_place(&mut result_stream, handle.clone());
        }
        if let Some(last) = aggregated_handles.last() {
            set_default_in_place(&mut result_stream, last.clone());
        }
    }

    result_stream.flush().await?;
    Ok(result_stream)
}

#[derive(Clone)]
struct HandleGroup<T>
where
    T: Clone + Send + Sync + 'static,
{
    default: T,
}

impl<T> HandleGroup<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn new(default: T) -> Self {
        Self { default }
    }
}

#[async_trait]
impl<T> AbelianGroup<T> for HandleGroup<T>
where
    T: Clone + Send + Sync + 'static,
{
    async fn add(&self, _a: &T, _b: &T) -> T {
        panic!("handle addition is unsupported")
    }

    async fn neg(&self, _a: &T) -> T {
        panic!("handle negation is unsupported")
    }

    async fn identity(&self) -> T {
        self.default.clone()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamRetention {
    None,
    KeepLast { keep_last: usize },
    AllButLatest,
}

impl StreamRetention {
    fn window_size(self) -> Option<usize> {
        match self {
            StreamRetention::None => None,
            StreamRetention::KeepLast { keep_last } if keep_last > 0 => Some(keep_last),
            StreamRetention::KeepLast { .. } => None,
            StreamRetention::AllButLatest => Some(1),
        }
    }
}

pub struct ZSetStream<K>
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
    stream: Stream<ZSetHandle>,
    versioned: VersionedZSet<K>,
    overlay: HashMap<K, i64>,
    retention: StreamRetention,
    retention_window: VecDeque<ZSetHandle>,
    retention_counts: HashMap<u64, usize>,
    current_handle: ZSetHandle,
}

impl<K> ZSetStream<K>
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
    pub async fn new(
        dict: Arc<Dictionary<K>>,
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        retention: StreamRetention,
    ) -> Result<Self> {
        let namespace = namespace.into();
        let versioned = VersionedZSet::new(dict, table.clone(), namespace.clone()).await?;
        let default_hint = ZSetHandle {
            ns: namespace.clone(),
            version: 0,
        };
        let group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(default_hint));
        let stream = Stream::with_table(table, namespace.clone(), group).await?;
        let default_handle = stream.default.clone();

        let history = collect_values(&stream, stream.current_time()).await?;
        let current_handle = history.last().cloned().unwrap_or(default_handle.clone());
        let (retention_window, retention_counts) =
            initialize_retention(&history, retention.window_size());

        Ok(Self {
            stream,
            versioned,
            overlay: HashMap::new(),
            retention,
            retention_window,
            retention_counts,
            current_handle,
        })
    }

    pub fn add_delta(&mut self, key: K, weight: i64) {
        if weight == 0 {
            return;
        }

        match self.overlay.entry(key) {
            Entry::Occupied(mut entry) => {
                let updated = *entry.get() + weight;
                if updated == 0 {
                    entry.remove();
                } else {
                    *entry.get_mut() = updated;
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(weight);
            }
        }
    }

    pub fn add_deltas<I>(&mut self, deltas: I)
    where
        I: IntoIterator<Item = (K, i64)>,
    {
        for (key, weight) in deltas {
            self.add_delta(key, weight);
        }
    }

    pub async fn flush(&mut self) -> Result<ZSetHandle> {
        let overlay = std::mem::take(&mut self.overlay);
        if overlay.is_empty() {
            return self.flush_without_version_update().await;
        }
        self.flush_with_overlay(overlay).await
    }

    pub async fn get_handle(&mut self, timestamp: i64) -> Result<ZSetHandle> {
        self.stream.get(timestamp).await
    }

    pub async fn latest_handle(&mut self) -> Result<ZSetHandle> {
        self.stream.latest().await
    }

    pub fn versioned(&mut self) -> &mut VersionedZSet<K> {
        &mut self.versioned
    }

    pub fn current_handle(&self) -> &ZSetHandle {
        &self.current_handle
    }

    pub fn handle_view(&self, handle: &ZSetHandle) -> ZSetHandleView<K> {
        ZSetHandleView::new(
            self.versioned.dictionary(),
            self.versioned.table(),
            handle.ns.clone(),
            handle.version,
        )
    }

    pub fn latest_view(&self) -> ZSetHandleView<K> {
        self.handle_view(&self.current_handle)
    }

    pub fn namespace(&self) -> &str {
        self.versioned.namespace()
    }

    #[cfg(test)]
    pub(crate) fn stream_intent_key(&self) -> Vec<u8> {
        self.stream.encode_intent_key()
    }

    async fn flush_without_version_update(&mut self) -> Result<ZSetHandle> {
        let handle = self.current_handle.clone();
        self.stream
            .send(handle.clone())
            .await
            .context("advance stream without deltas")?;

        let mut batch = WriteBatch::new();
        let stream_intent = self.stream.encode_intent_key();
        let dirty = self.flush_stream_into_batch(&mut batch)?;
        if dirty {
            batch.put(stream_intent.clone(), vec![1]);
            self.versioned
                .table()
                .write_batch(batch)
                .await
                .context("persist stream state")?;

            let mut cleanup = WriteBatch::new();
            cleanup.delete(stream_intent.clone());
            self.versioned
                .table()
                .write_batch(cleanup)
                .await
                .context("clear stream intent")?;
        }

        let releases = self.record_handle(handle.clone());
        self.apply_retention(releases).await?;
        self.current_handle = handle.clone();
        Ok(handle)
    }

    async fn flush_with_overlay(&mut self, overlay: HashMap<K, i64>) -> Result<ZSetHandle> {
        let dict = self.versioned.dictionary();
        let mut dict_batch = dict.batch();
        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        for (key, delta) in overlay {
            if delta == 0 {
                continue;
            }
            let id = dict_batch
                .intern(&key)
                .await
                .context("intern key while staging overlay")?;
            buckets.entry(bucket_for(id)).or_default().push((id, delta));
        }
        drop(dict_batch);

        let mut segments = Vec::new();
        for (bucket, mut deltas) in buckets {
            deltas.retain(|(_, delta)| *delta != 0);
            if deltas.is_empty() {
                continue;
            }
            deltas.sort_by_key(|(id, _)| *id);
            segments.push(SegmentRecord {
                id: 0,
                bucket,
                deltas,
            });
        }

        if segments.is_empty() {
            return self.flush_without_version_update().await;
        }

        let base = if self.current_handle.version == 0 {
            None
        } else {
            Some(self.current_handle.version)
        };

        let mut batch = WriteBatch::new();
        let plan = self
            .versioned
            .enqueue_version_with_base(segments, base, 1, &mut batch)
            .await
            .context("enqueue versioned ZSet update")?;

        let new_handle = self.versioned.handle_for_version(plan.version);
        let stream_intent = self.stream.encode_intent_key();
        let version_intent = self.versioned.intent_key_bytes().to_vec();

        self.stream
            .send(new_handle.clone())
            .await
            .context("append handle to stream")?;

        if self.flush_stream_into_batch(&mut batch)? {
            batch.put(stream_intent.clone(), vec![1]);
        }

        self.versioned
            .table()
            .write_batch(batch)
            .await
            .context("write combined stream and version update")?;

        let mut cleanup = WriteBatch::new();
        cleanup.delete(stream_intent.clone());
        cleanup.delete(version_intent.clone());
        self.versioned
            .table()
            .write_batch(cleanup)
            .await
            .context("clear intents after versioned update")?;

        self.versioned.apply_version_plan(&plan);
        self.current_handle = new_handle.clone();

        let releases = self.record_handle(new_handle.clone());
        self.apply_retention(releases).await?;

        Ok(new_handle)
    }

    fn flush_stream_into_batch(&mut self, batch: &mut WriteBatch) -> Result<bool> {
        let mut dirty = false;
        if self.stream.flush_defaults_into(batch)? {
            dirty = true;
        }
        if self.stream.flush_data_into(batch)? {
            dirty = true;
        }
        if self.stream.flush_state_into(batch)? {
            dirty = true;
        }
        Ok(dirty)
    }

    fn record_handle(&mut self, handle: ZSetHandle) -> Vec<u64> {
        let mut releases = Vec::new();
        if let Some(limit) = self.retention.window_size() {
            if limit == 0 {
                return releases;
            }

            if self.retention_window.len() >= limit {
                if let Some(evicted) = self.retention_window.pop_front() {
                    if let Some(count) = self.retention_counts.get_mut(&evicted.version) {
                        if *count == 1 {
                            self.retention_counts.remove(&evicted.version);
                            if evicted.version != 0 {
                                releases.push(evicted.version);
                            }
                        } else {
                            *count -= 1;
                        }
                    }
                }
            }

            *self.retention_counts.entry(handle.version).or_insert(0) += 1;
            self.retention_window.push_back(handle);
        }
        releases
    }

    async fn apply_retention(&mut self, releases: Vec<u64>) -> Result<()> {
        for version in releases {
            self.versioned
                .release_version(version)
                .await
                .context("release version during retention")?;
        }
        Ok(())
    }
}

fn initialize_retention(
    history: &[ZSetHandle],
    window: Option<usize>,
) -> (VecDeque<ZSetHandle>, HashMap<u64, usize>) {
    let mut window_handles = VecDeque::new();
    let mut counts = HashMap::new();

    if let Some(limit) = window {
        if limit > 0 {
            let skip = history.len().saturating_sub(limit);
            for handle in history.iter().skip(skip).cloned() {
                *counts.entry(handle.version).or_insert(0) += 1;
                window_handles.push_back(handle);
            }
        }
    }

    (window_handles, counts)
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

pub struct StreamAddition<T>
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
    group: Arc<dyn AbelianGroup<T>>,
    table: Arc<dyn KeyValueTable>,
    namespace_prefix: String,
    counter: AtomicU64,
}

impl<T> StreamAddition<T>
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
    pub fn new(
        group: Arc<dyn AbelianGroup<T>>,
        table: Arc<dyn KeyValueTable>,
        namespace_prefix: impl Into<String>,
    ) -> Self {
        Self {
            group,
            table,
            namespace_prefix: namespace_prefix.into(),
            counter: AtomicU64::new(0),
        }
    }

    pub fn from_stream(stream: &Stream<T>) -> Self {
        Self::new(stream.group.clone(), stream.table.clone(), "stream_add/")
    }

    fn next_namespace(&self) -> String {
        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        format!("{}{}", self.namespace_prefix, id)
    }

    async fn build_stream(&self) -> Result<Stream<T>> {
        let namespace = self.next_namespace();
        Stream::with_table(self.table.clone(), namespace, self.group.clone()).await
    }
}

#[async_trait]
impl<T> AbelianGroup<Stream<T>> for StreamAddition<T>
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
    async fn add(&self, a: &Stream<T>, b: &Stream<T>) -> Stream<T> {
        let max_ts = a.timestamp.max(b.timestamp);
        let values_a = collect_values(a, max_ts)
            .await
            .expect("collect stream values for left operand");
        let values_b = collect_values(b, max_ts)
            .await
            .expect("collect stream values for right operand");

        let mut result = self
            .build_stream()
            .await
            .expect("failed to construct stream for addition");
        if !values_a.is_empty() && !values_b.is_empty() {
            let default_value = self.group.add(&values_a[0], &values_b[0]).await;
            set_default_in_place(&mut result, default_value.clone());

            for t in 1..=max_ts {
                let sum = self
                    .group
                    .add(&values_a[t as usize], &values_b[t as usize])
                    .await;
                push_value_in_place(&mut result, sum);
            }
        }

        result
    }

    async fn neg(&self, a: &Stream<T>) -> Stream<T> {
        let max_ts = a.timestamp;
        let values = collect_values(a, max_ts)
            .await
            .expect("collect stream values for negation");
        let mut result = self
            .build_stream()
            .await
            .expect("failed to construct stream for negation");

        if let Some(first) = values.get(0) {
            let default_value = self.group.neg(first).await;
            set_default_in_place(&mut result, default_value.clone());

            for t in 1..=max_ts {
                let value = self.group.neg(&values[t as usize]).await;
                push_value_in_place(&mut result, value);
            }
        }

        result
    }

    async fn identity(&self) -> Stream<T> {
        self.build_stream()
            .await
            .expect("failed to construct stream for identity")
    }
}

pub struct LiftedSelect<P> {
    predicate: P,
}

impl<P> LiftedSelect<P> {
    pub fn new(predicate: P) -> Self {
        Self { predicate }
    }

    pub async fn apply<K>(&self, zset: &ZSet<K>) -> Result<ZSet<K>>
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
        P: Fn(&K) -> bool + Send + Sync,
    {
        zset::select(zset, &self.predicate).await
    }
}

pub struct LiftedProject<F> {
    projector: F,
}

impl<F> LiftedProject<F> {
    pub fn new(projector: F) -> Self {
        Self { projector }
    }

    pub async fn apply<K, R>(&self, zset: &ZSet<K>) -> Result<ZSet<R>>
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
        F: Fn(&K) -> R + Send + Sync,
    {
        zset::project(zset, &self.projector).await
    }
}

pub struct LiftedJoin<P, F> {
    predicate: P,
    projector: F,
}

impl<P, F> LiftedJoin<P, F> {
    pub fn new(predicate: P, projector: F) -> Self {
        Self {
            predicate,
            projector,
        }
    }

    pub async fn apply<L, R, O>(&self, left: &ZSet<L>, right: &ZSet<R>) -> Result<ZSet<O>>
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
        P: Fn(&L, &R) -> bool + Send + Sync,
        F: Fn(&L, &R) -> O + Send + Sync,
    {
        zset::join(left, right, &self.predicate, &self.projector).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::storage::dictionary::Dictionary;
    use async_trait::async_trait;
    use object_store::memory::InMemory;
    use slatedb::{Db, WriteBatch};

    struct IntegerGroup;

    #[async_trait]
    impl AbelianGroup<i64> for IntegerGroup {
        async fn add(&self, a: &i64, b: &i64) -> i64 {
            a + b
        }

        async fn neg(&self, a: &i64) -> i64 {
            -a
        }

        async fn identity(&self) -> i64 {
            0
        }
    }

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open("stream-test", store).await.expect("open SlateDB"))
    }

    #[tokio::test]
    async fn send_and_get_values() {
        let db = build_db().await;
        let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
        let mut stream = Stream::new(db.clone(), "ints", group).await.unwrap();

        assert_eq!(stream.current_time(), 0);
        assert_eq!(stream.get(0).await.unwrap(), 0);

        stream.send(5).await.unwrap();
        stream.flush().await.unwrap();

        assert_eq!(stream.current_time(), 1);
        assert_eq!(stream.get(1).await.unwrap(), 5);
        assert_eq!(stream.latest().await.unwrap(), 5);

        let mut reload = Stream::new(db, "ints", Arc::new(IntegerGroup))
            .await
            .unwrap();
        assert_eq!(reload.current_time(), 1);
        assert_eq!(reload.get(1).await.unwrap(), 5);
    }

    #[tokio::test]
    async fn fills_with_default_when_reading_ahead() {
        let db = build_db().await;
        let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
        let mut stream = Stream::new(db, "ahead", group).await.unwrap();

        let value = stream.get(5).await.unwrap();
        assert_eq!(value, 0);
        assert_eq!(stream.current_time(), 5);
    }

    #[tokio::test]
    async fn persists_default_changes() {
        let db = build_db().await;
        let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
        let mut stream = Stream::new(db.clone(), "defaults", group).await.unwrap();

        stream.send(0).await.unwrap();
        stream.set_default(10).await.unwrap();
        stream.send(10).await.unwrap();
        stream.flush().await.unwrap();

        let mut reload = Stream::new(db, "defaults", Arc::new(IntegerGroup))
            .await
            .unwrap();
        assert_eq!(reload.get(2).await.unwrap(), 10);
    }

    #[tokio::test]
    async fn remembers_last_default_ts() {
        let db = build_db().await;
        let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
        let mut stream = Stream::new(db.clone(), "last_default", group.clone())
            .await
            .expect("build stream");

        stream.set_default(5).await.expect("set default");
        stream.flush().await.expect("flush default");
        stream.send(5).await.expect("send value");
        stream.set_default(7).await.expect("set second default");
        stream.flush().await.expect("flush stream");

        let mut reopened = Stream::new(db, "last_default", group)
            .await
            .expect("reopen stream");

        assert_eq!(reopened.last_default_ts, 1);
        assert_eq!(reopened.get(1).await.expect("get value"), 7);
        assert_eq!(reopened.default_at(2), 7);
    }

    #[tokio::test]
    async fn clears_intent_on_restart() {
        let db = build_db().await;
        let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
        let mut stream = Stream::new(db.clone(), "intent", group.clone())
            .await
            .expect("create stream");

        stream.send(42).await.expect("send value");
        stream.flush().await.expect("flush stream");

        let intent_key = stream.encode_intent_key();

        let mut batch = WriteBatch::new();
        batch.put(intent_key.clone(), vec![1]);
        stream
            .table
            .write_batch(batch)
            .await
            .expect("write leftover intent");

        let mut recovered = Stream::new(db, "intent", group)
            .await
            .expect("reopen stream");

        assert!(
            recovered
                .table
                .get(&intent_key)
                .await
                .expect("get intent")
                .is_none(),
            "intent key should be cleared on reopen"
        );
        assert_eq!(recovered.get(1).await.expect("get value"), 42);
    }

    #[tokio::test]
    async fn stream_addition_and_negation() {
        let db = build_db().await;
        let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

        let mut left = Stream::new(db.clone(), "left", group.clone())
            .await
            .expect("create left stream");
        let mut right = Stream::new(db.clone(), "right", group.clone())
            .await
            .expect("create right stream");

        left.send(1).await.expect("send left t1");
        left.send(4).await.expect("send left t2");

        right.set_default(2).await.expect("set right default");
        right.send(2).await.expect("send right t1");
        right.send(8).await.expect("send right t2");

        let addition = StreamAddition::from_stream(&left);

        let mut sum = addition.add(&left, &right).await;
        assert_eq!(sum.get(0).await.expect("sum t0"), 2);
        assert_eq!(sum.get(1).await.expect("sum t1"), 3);
        assert_eq!(sum.get(2).await.expect("sum t2"), 12);

        let mut neg = addition.neg(&left).await;
        assert_eq!(neg.get(0).await.expect("neg t0"), 0);
        assert_eq!(neg.get(1).await.expect("neg t1"), -1);
        assert_eq!(neg.get(2).await.expect("neg t2"), -4);

        let identity = addition.identity().await;
        assert!(identity.is_identity());
    }

    #[tokio::test]
    async fn delay_shifts_stream_values() {
        let db = build_db().await;
        let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

        let mut source = Stream::new(db.clone(), "delay_input", group.clone())
            .await
            .expect("create stream");
        source.send(5).await.expect("send t1");
        source.send(10).await.expect("send t2");
        source.send(15).await.expect("send t3");

        let mut delayed = delay(&source).await.expect("apply delay");
        assert_eq!(delayed.get(0).await.expect("t0"), 0);
        assert_eq!(delayed.get(1).await.expect("t1"), 0);
        assert_eq!(delayed.get(2).await.expect("t2"), 5);
        assert_eq!(delayed.get(3).await.expect("t3"), 10);
        assert_eq!(delayed.get(4).await.expect("t4"), 10);
    }

    #[tokio::test]
    async fn differentiate_computes_deltas() {
        let db = build_db().await;
        let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

        let mut source = Stream::new(db.clone(), "differentiate_input", group.clone())
            .await
            .expect("create stream");
        source.send(2).await.expect("send t1");
        source.send(6).await.expect("send t2");
        source.send(9).await.expect("send t3");

        let mut diff = differentiate(&source).await.expect("apply diff");
        assert_eq!(diff.get(0).await.expect("t0"), 0);
        assert_eq!(diff.get(1).await.expect("t1"), 2);
        assert_eq!(diff.get(2).await.expect("t2"), 4);
        assert_eq!(diff.get(3).await.expect("t3"), 3);
        assert_eq!(diff.get(4).await.expect("t4"), 3);
    }

    #[tokio::test]
    async fn integrate_accumulates_stream() {
        let db = build_db().await;
        let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

        let mut source = Stream::new(db.clone(), "integrate_input", group.clone())
            .await
            .expect("create stream");
        source.send(1).await.expect("send t1");
        source.send(2).await.expect("send t2");
        source.send(3).await.expect("send t3");

        let mut integrated = integrate(&source).await.expect("apply integrate");
        assert_eq!(integrated.get(0).await.expect("t0"), 0);
        assert_eq!(integrated.get(1).await.expect("t1"), 1);
        assert_eq!(integrated.get(2).await.expect("t2"), 3);
        assert_eq!(integrated.get(3).await.expect("t3"), 6);
        assert_eq!(integrated.get(4).await.expect("t4"), 6);
    }

    #[tokio::test]
    async fn lift1_applies_function_to_stream() {
        let db = build_db().await;
        let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

        let mut source = Stream::new(db.clone(), "lift1_input", group.clone())
            .await
            .expect("create stream");
        source.send(3).await.expect("send t1");
        source.send(5).await.expect("send t2");

        let mut lifted = lift1(&source, group.clone(), |value: &i64| value * 2)
            .await
            .expect("apply lift1");
        assert_eq!(lifted.get(0).await.expect("t0"), 0);
        assert_eq!(lifted.get(1).await.expect("t1"), 6);
        assert_eq!(lifted.get(2).await.expect("t2"), 10);
        assert_eq!(lifted.get(3).await.expect("t3"), 10);
    }

    #[tokio::test]
    async fn lift2_combines_two_streams() {
        let db = build_db().await;
        let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

        let mut left = Stream::new(db.clone(), "lift2_left", group.clone())
            .await
            .expect("create left");
        left.send(1).await.expect("left t1");
        left.send(3).await.expect("left t2");

        let mut right = Stream::new(db.clone(), "lift2_right", group.clone())
            .await
            .expect("create right");
        right.set_default(5).await.expect("set right default");
        right.send(5).await.expect("right t1");
        right.send(7).await.expect("right t2");

        let mut combined = lift2(&left, &right, group.clone(), |l: &i64, r: &i64| l + r)
            .await
            .expect("apply lift2");
        assert_eq!(combined.get(0).await.expect("t0"), 5);
        assert_eq!(combined.get(1).await.expect("t1"), 6);
        assert_eq!(combined.get(2).await.expect("t2"), 10);
        assert_eq!(combined.get(3).await.expect("t3"), 10);
    }

    #[tokio::test]
    async fn stream_introduction_and_elimination_round_trip() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

        let introduced = stream_introduction(table.clone(), group.clone(), 5)
            .await
            .expect("introduce value");
        let eliminated = stream_elimination(&introduced)
            .await
            .expect("eliminate introduced stream");
        assert_eq!(eliminated, 5);

        let mut aggregate = Stream::with_table(table, "stream_elimination", group.clone())
            .await
            .expect("create aggregate stream");
        aggregate.send(2).await.expect("send first");
        aggregate.send(3).await.expect("send second");
        let summed = stream_elimination(&aggregate)
            .await
            .expect("eliminate aggregate stream");
        assert_eq!(summed, 5);
    }

    #[tokio::test]
    async fn lifted_delay_operates_on_stream_handles() {
        let db = build_db().await;
        let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

        let mut inner_a = Stream::new(db.clone(), "lifted_delay_inner_a", group.clone())
            .await
            .expect("create inner stream a");
        inner_a.send(1).await.expect("inner a t1");
        inner_a.send(2).await.expect("inner a t2");
        inner_a.flush().await.expect("flush inner a");

        let mut inner_b = Stream::new(db.clone(), "lifted_delay_inner_b", group.clone())
            .await
            .expect("create inner stream b");
        inner_b.send(5).await.expect("inner b t1");
        inner_b.send(6).await.expect("inner b t2");
        inner_b.flush().await.expect("flush inner b");

        let handle_a = inner_a.handle();
        let handle_b = inner_b.handle();
        let handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
            Arc::new(HandleGroup::new(handle_a.clone()));

        let mut outer = Stream::new(db.clone(), "lifted_delay_outer", handle_group)
            .await
            .expect("create outer stream");
        outer.send(handle_a.clone()).await.expect("outer t1");
        outer.send(handle_b.clone()).await.expect("outer t2");

        let mut delayed = lifted_delay(&outer, group.clone())
            .await
            .expect("apply lifted delay");

        let mut handles = Vec::new();
        for t in 0..=delayed.current_time() {
            handles.push(
                delayed
                    .get(t)
                    .await
                    .expect("read delayed handle for timeline"),
            );
        }

        let mut resolved_first = delayed
            .resolve_handle(&handles[0], group.clone())
            .await
            .expect("resolve first delayed stream");
        assert_eq!(resolved_first.get(0).await.expect("first t0"), 0);
        assert_eq!(resolved_first.get(1).await.expect("first t1"), 0);
        assert_eq!(resolved_first.get(2).await.expect("first t2"), 1);

        let mut resolved_second = delayed
            .resolve_handle(handles.last().expect("last delayed handle"), group.clone())
            .await
            .expect("resolve second delayed stream");
        assert_eq!(resolved_second.get(1).await.expect("second t1"), 0);
        assert_eq!(resolved_second.get(2).await.expect("second t2"), 5);
    }

    #[tokio::test]
    async fn lifted_stream_introduction_and_elimination_round_trip() {
        let db = build_db().await;
        let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

        let mut base = Stream::new(db.clone(), "lifted_intro_base", group.clone())
            .await
            .expect("create base stream");
        base.send(1).await.expect("base t1");
        base.send(3).await.expect("base t2");

        let introduced = lifted_stream_introduction(&base)
            .await
            .expect("apply lifted stream introduction");
        let mut eliminated = lifted_stream_elimination(&introduced, group.clone())
            .await
            .expect("apply lifted stream elimination");

        for t in 0..=base.current_time() {
            assert_eq!(
                eliminated.get(t).await.expect("eliminated value"),
                base.get(t).await.expect("base value")
            );
        }
    }

    #[tokio::test]
    async fn lifted_select_applies_predicate() {
        let db = build_db().await;
        let mut zset = ZSet::new(db, "lifted_select").await.expect("create zset");
        zset.set_weight(1_i32, 1);
        zset.set_weight(2_i32, 2);
        zset.set_weight(3_i32, -1);

        let lifted = LiftedSelect::new(|value: &i32| value % 2 == 0);
        let mut result = lifted.apply(&zset).await.expect("apply lifted select");
        let items: HashMap<_, _> = result.items().await.expect("items").into_iter().collect();
        assert_eq!(items.get(&2), Some(&2));
        assert_eq!(items.len(), 1);
    }

    #[tokio::test]
    async fn lifted_project_applies_function() {
        let db = build_db().await;
        let mut zset = ZSet::new(db, "lifted_project").await.expect("create zset");
        zset.set_weight(1_i32, 1);
        zset.set_weight(2_i32, -1);
        zset.set_weight(3_i32, 4);

        let lifted = LiftedProject::new(|value: &i32| value % 2);
        let mut result = lifted.apply(&zset).await.expect("apply lifted project");
        let items: HashMap<_, _> = result.items().await.expect("items").into_iter().collect();
        assert_eq!(items.get(&1), Some(&5));
        assert_eq!(items.get(&0), Some(&-1));
        assert_eq!(items.len(), 2);
    }

    #[tokio::test]
    async fn lifted_join_combines_zsets() {
        let db = build_db().await;
        let mut left = ZSet::new(db.clone(), "lifted_join_left")
            .await
            .expect("create left zset");
        let mut right = ZSet::new(db, "lifted_join_right")
            .await
            .expect("create right zset");

        left.set_weight("a".to_string(), 2);
        left.set_weight("b".to_string(), -1);
        right.set_weight("a".to_string(), 3);
        right.set_weight("c".to_string(), 5);

        let lifted = LiftedJoin::new(
            |l: &String, r: &String| l == r,
            |l: &String, r: &String| format!("{l}-{r}"),
        );
        let mut result = lifted
            .apply(&left, &right)
            .await
            .expect("apply lifted join");
        let items: HashMap<_, _> = result.items().await.expect("items").into_iter().collect();
        assert_eq!(items.get("a-a"), Some(&6));
        assert_eq!(items.len(), 1);
    }

    #[tokio::test]
    async fn incrementalize2_matches_manual_construction() {
        let db = build_db().await;
        let group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

        let mut left = Stream::new(db.clone(), "inc2_left", group.clone())
            .await
            .expect("create left stream");
        left.send(1).await.expect("left t1");
        left.send(3).await.expect("left t2");

        let mut right = Stream::new(db.clone(), "inc2_right", group.clone())
            .await
            .expect("create right stream");
        right.send(2).await.expect("right t1");
        right.send(4).await.expect("right t2");

        let result = incrementalize2(&left, &right, group.clone(), |a, b| a + b)
            .await
            .expect("compute incrementalize2");

        let integrated_left = integrate(&left).await.expect("integrate left");
        let delayed_integrated_left = delay(&integrated_left)
            .await
            .expect("delay integrated left");

        let integrated_right = integrate(&right).await.expect("integrate right");
        let delayed_integrated_right = delay(&integrated_right)
            .await
            .expect("delay integrated right");

        let f_ab = lift2(&left, &right, group.clone(), |a, b| a + b)
            .await
            .expect("lift2 a,b");
        let f_a_delayed_b = lift2(&left, &delayed_integrated_right, group.clone(), |a, b| {
            a + b
        })
        .await
        .expect("lift2 a, delayed b");
        let f_delayed_a_b = lift2(&delayed_integrated_left, &right, group.clone(), |a, b| {
            a + b
        })
        .await
        .expect("lift2 delayed a, b");

        let addition = StreamAddition::from_stream(&f_ab);
        let partial = addition.add(&f_ab, &f_a_delayed_b).await;
        let manual = addition.add(&partial, &f_delayed_a_b).await;

        let result_values = collect_values(&result, result.timestamp)
            .await
            .expect("collect incrementalize2 values");
        let manual_values = collect_values(&manual, manual.timestamp)
            .await
            .expect("collect manual values");

        assert_eq!(result_values, manual_values);
    }

    #[tokio::test]
    async fn lifted_select_zset_stream_filters_elements() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let dict = Arc::new(
            Dictionary::with_table(table.clone(), "lifted_select_input", None)
                .await
                .expect("build dictionary"),
        );

        let mut zset_stream = ZSetStream::new(
            dict,
            table.clone(),
            "lifted_select_input",
            StreamRetention::None,
        )
        .await
        .expect("create zset stream");

        zset_stream.add_delta("keep".to_string(), 1);
        let handle0 = zset_stream.flush().await.expect("flush first");

        zset_stream.add_delta("keep".to_string(), -1);
        zset_stream.add_delta("drop".to_string(), 1);
        let handle1 = zset_stream.flush().await.expect("flush second");

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(handle0.clone()));
        let mut input_stream =
            Stream::with_table(table.clone(), "lifted_select_stream", handle_group)
                .await
                .expect("create stream of handles");
        set_default_in_place(&mut input_stream, handle0.clone());
        push_value_in_place(&mut input_stream, handle1.clone());
        input_stream.flush().await.expect("flush input stream");

        let mut result = lifted_select_zset_stream::<String, _>(&input_stream, |value: &String| {
            value.starts_with('k')
        })
        .await
        .expect("apply lifted select stream");
        result.flush().await.expect("flush result stream");

        let handles = collect_values(&result, result.timestamp)
            .await
            .expect("collect handles");
        let mut cache = HashMap::new();

        let first = materialize_zset_handle::<String>(table.clone(), &mut cache, &handles[0])
            .await
            .expect("materialize first handle");
        assert_eq!(first.get("keep"), Some(&1));
        assert!(first.get("drop").is_none());

        let second = materialize_zset_handle::<String>(table.clone(), &mut cache, &handles[1])
            .await
            .expect("materialize second handle");
        assert!(
            second.get("keep").is_none(),
            "unexpected keep weight {:?}",
            second.get("keep")
        );
        assert!(second.get("drop").is_none());
    }

    #[tokio::test]
    async fn lifted_select_zset_stream_produces_empty_handles_when_filtered_out() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let dict = Arc::new(
            Dictionary::with_table(table.clone(), "lifted_select_empty", None)
                .await
                .expect("build dictionary for empty select"),
        );

        let mut zset_stream = ZSetStream::new(
            dict,
            table.clone(),
            "lifted_select_empty",
            StreamRetention::None,
        )
        .await
        .expect("create zset stream");

        zset_stream.add_delta("drop".to_string(), 2);
        let handle0 = zset_stream.flush().await.expect("flush first handle");
        zset_stream.add_delta("drop".to_string(), -2);
        zset_stream.add_delta("drop".to_string(), 3);
        let handle1 = zset_stream.flush().await.expect("flush second handle");

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(handle0.clone()));
        let mut input_stream =
            Stream::with_table(table.clone(), "lifted_select_empty_stream", handle_group)
                .await
                .expect("create stream of handles");
        set_default_in_place(&mut input_stream, handle0.clone());
        push_value_in_place(&mut input_stream, handle1.clone());
        input_stream
            .flush()
            .await
            .expect("flush lifted select input stream");

        let mut result =
            lifted_select_zset_stream::<String, _>(&input_stream, |_value: &String| false)
                .await
                .expect("apply lifted select with no matches");
        result.flush().await.expect("flush empty select result");

        let handles = collect_values(&result, result.timestamp)
            .await
            .expect("collect empty select handles");
        assert!(
            !handles.is_empty(),
            "expected neutral handle for empty lifted select"
        );

        let mut cache = HashMap::new();
        for handle in handles {
            let materialized =
                materialize_zset_handle::<String>(table.clone(), &mut cache, &handle)
                    .await
                    .expect("materialize empty select handle");
            assert!(
                materialized.is_empty(),
                "expected empty zset, got {:?}",
                materialized
            );
        }
    }

    #[tokio::test]
    async fn lifted_h_zset_stream_detects_transitions() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let diff_dict = Arc::new(
            Dictionary::with_table(table.clone(), "lifted_h_diff", None)
                .await
                .expect("diff dictionary"),
        );
        let state_dict = Arc::new(
            Dictionary::with_table(table.clone(), "lifted_h_state", None)
                .await
                .expect("state dictionary"),
        );

        let mut diff_stream = ZSetStream::new(
            diff_dict,
            table.clone(),
            "lifted_h_diff",
            StreamRetention::None,
        )
        .await
        .expect("create diff stream");
        let mut state_stream = ZSetStream::new(
            state_dict,
            table.clone(),
            "lifted_h_state",
            StreamRetention::None,
        )
        .await
        .expect("create state stream");

        diff_stream.add_delta("a".to_string(), 1);
        let diff_handle0 = diff_stream.flush().await.expect("flush diff0");

        let state_handle0 = state_stream.flush().await.expect("flush state0");
        state_stream.add_delta("a".to_string(), 1);
        let state_handle1 = state_stream.flush().await.expect("flush state1");

        diff_stream.add_delta("a".to_string(), -2);
        let diff_handle1 = diff_stream.flush().await.expect("flush diff1");

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(diff_handle0.clone()));
        let mut diff_handle_stream =
            Stream::with_table(table.clone(), "lifted_h_diff_handles", handle_group.clone())
                .await
                .expect("create diff handle stream");
        set_default_in_place(&mut diff_handle_stream, diff_handle0.clone());
        push_value_in_place(&mut diff_handle_stream, diff_handle1.clone());
        diff_handle_stream
            .flush()
            .await
            .expect("flush diff handles");

        let mut state_handle_stream =
            Stream::with_table(table.clone(), "lifted_h_state_handles", handle_group)
                .await
                .expect("create state handle stream");
        set_default_in_place(&mut state_handle_stream, state_handle0.clone());
        push_value_in_place(&mut state_handle_stream, state_handle1.clone());
        state_handle_stream
            .flush()
            .await
            .expect("flush state handles");

        let mut debug_cache = HashMap::new();
        let diff_second =
            materialize_zset_handle::<String>(table.clone(), &mut debug_cache, &diff_handle1)
                .await
                .expect("materialize diff second");
        let state_second =
            materialize_zset_handle::<String>(table.clone(), &mut debug_cache, &state_handle1)
                .await
                .expect("materialize state second");
        assert_eq!(diff_second.get("a"), Some(&-1));
        assert_eq!(state_second.get("a"), Some(&1));

        let mut result = lifted_h_zset_stream::<String>(&diff_handle_stream, &state_handle_stream)
            .await
            .expect("apply lifted H stream");
        result.flush().await.expect("flush lifted H result");

        let handles = collect_values(&result, result.timestamp)
            .await
            .expect("collect H handles");
        let mut cache = HashMap::new();

        let first = materialize_zset_handle::<String>(table.clone(), &mut cache, &handles[0])
            .await
            .expect("materialize first H result");
        assert_eq!(first.get("a"), Some(&1));

        let second = materialize_zset_handle::<String>(table.clone(), &mut cache, &handles[1])
            .await
            .expect("materialize second H result");
        assert_eq!(second.get("a"), Some(&-1), "second H map: {:?}", second);
    }

    #[tokio::test]
    async fn lifted_lifted_select_zset_stream_operates_on_nested_streams() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let dict = Arc::new(
            Dictionary::with_table(table.clone(), "lifted_lifted_select", None)
                .await
                .expect("dictionary"),
        );

        let mut zset_stream = ZSetStream::new(
            dict,
            table.clone(),
            "lifted_lifted_select",
            StreamRetention::None,
        )
        .await
        .expect("create zset stream");

        zset_stream.add_delta("keep".to_string(), 2);
        let handle0 = zset_stream.flush().await.expect("flush handle0");

        zset_stream.add_delta("drop".to_string(), 3);
        let handle1 = zset_stream.flush().await.expect("flush handle1");

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(handle0.clone()));
        let mut inner_stream =
            Stream::with_table(table.clone(), "lifted_lifted_select_inner", handle_group)
                .await
                .expect("create inner stream");
        set_default_in_place(&mut inner_stream, handle0.clone());
        push_value_in_place(&mut inner_stream, handle1.clone());
        inner_stream.flush().await.expect("flush inner stream");

        let mut selected =
            lifted_select_zset_stream::<String, _>(&inner_stream, |value: &String| value == "keep")
                .await
                .expect("apply inner lifted select");
        selected.flush().await.expect("flush selected");
        let selected_handle = selected.handle();

        let stream_group: Arc<dyn AbelianGroup<StreamHandle>> =
            Arc::new(HandleGroup::new(selected_handle.clone()));
        let mut outer_stream =
            Stream::with_table(table.clone(), "lifted_lifted_select_outer", stream_group)
                .await
                .expect("create outer stream");
        set_default_in_place(&mut outer_stream, selected_handle.clone());
        outer_stream.flush().await.expect("flush outer stream");

        let mut result =
            lifted_lifted_select_zset_stream::<String, _>(&outer_stream, |value: &String| {
                value == "keep"
            })
            .await
            .expect("apply lifted-lifted select");
        result.flush().await.expect("flush lifted-lifted result");

        let handles = collect_values(&result, result.timestamp)
            .await
            .expect("collect outer handles");
        let resolved_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(handle0.clone()));
        let mut resolved = result
            .resolve_handle(&handles[0], resolved_group)
            .await
            .expect("resolve nested stream");
        resolved.flush().await.expect("flush resolved stream");

        let resolved_handles = collect_values(&resolved, resolved.timestamp)
            .await
            .expect("collect resolved handles");
        let mut cache = HashMap::new();
        let first =
            materialize_zset_handle::<String>(table.clone(), &mut cache, &resolved_handles[0])
                .await
                .expect("materialize resolved first");
        assert_eq!(first.get("keep"), Some(&2));
        assert!(first.get("drop").is_none());
    }

    #[tokio::test]
    async fn delta_lifted_delta_lifted_join_produces_handles() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));

        let dict_a = Arc::new(
            Dictionary::with_table(table.clone(), "delta_join_a", None)
                .await
                .expect("dictionary a"),
        );
        let mut stream_a =
            ZSetStream::new(dict_a, table.clone(), "delta_join_a", StreamRetention::None)
                .await
                .expect("create zset stream a");

        stream_a.add_delta((0_i32, 1_i32), 1);
        stream_a.flush().await.expect("flush a t0");
        stream_a.add_delta((1, 2), 1);
        stream_a.flush().await.expect("flush a t1");

        let dict_b = Arc::new(
            Dictionary::with_table(table.clone(), "delta_join_b", None)
                .await
                .expect("dictionary b"),
        );
        let mut stream_b =
            ZSetStream::new(dict_b, table.clone(), "delta_join_b", StreamRetention::None)
                .await
                .expect("create zset stream b");

        stream_b.add_delta((1_i32, 3_i32), 1);
        stream_b.flush().await.expect("flush b t0");
        stream_b.add_delta((2, 4), 1);
        stream_b.flush().await.expect("flush b t1");

        let a_handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
            Arc::new(HandleGroup::new(stream_a.stream.handle()));
        let b_handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
            Arc::new(HandleGroup::new(stream_b.stream.handle()));

        let mut outer_a =
            Stream::with_table(table.clone(), "delta_join_outer_a", a_handle_group.clone())
                .await
                .expect("create outer stream a");
        let handle_a0 = stream_a.stream.handle();
        set_default_in_place(&mut outer_a, handle_a0.clone());
        let handle_a1 = stream_a.stream.handle();
        push_value_in_place(&mut outer_a, handle_a1.clone());
        outer_a.flush().await.expect("flush outer a");

        let mut outer_b =
            Stream::with_table(table.clone(), "delta_join_outer_b", b_handle_group.clone())
                .await
                .expect("create outer stream b");
        let handle_b0 = stream_b.stream.handle();
        set_default_in_place(&mut outer_b, handle_b0.clone());
        let handle_b1 = stream_b.stream.handle();
        push_value_in_place(&mut outer_b, handle_b1.clone());
        outer_b.flush().await.expect("flush outer b");

        let mut result = delta_lifted_delta_lifted_join(
            &outer_a,
            &outer_b,
            |left: &(i32, i32), right: &(i32, i32)| left.1 == right.0,
            |left: &(i32, i32), right: &(i32, i32)| (left.0, right.1),
        )
        .await
        .expect("compute delta lifted join");
        result
            .flush()
            .await
            .expect("flush delta lifted join output");

        let handles = collect_values(&result, result.timestamp)
            .await
            .expect("collect delta lifted join handles");
        assert!(!handles.is_empty());

        let mut cache = HashMap::new();
        for handle in handles {
            let group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(ZSetHandle {
                ns: handle.ns.clone(),
                version: 0,
            }));
            let mut resolved = result
                .resolve_handle(&handle, group.clone())
                .await
                .expect("resolve nested join stream");
            let zset_handle = resolved.latest().await.expect("load nested handle");
            let map =
                materialize_zset_handle::<(i32, i32)>(table.clone(), &mut cache, &zset_handle)
                    .await
                    .expect("materialize nested zset");
            if handle.frontier > 0 {
                assert!(
                    !map.is_empty(),
                    "expected non-empty map at frontier {}, map {:?}",
                    handle.frontier,
                    map
                );
            }
        }
    }

    #[tokio::test]
    async fn delta_lifted_delta_lifted_join_aligns_to_shortest_stream() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));

        let dict_left = Arc::new(
            Dictionary::with_table(table.clone(), "delta_join_align_left", None)
                .await
                .expect("dictionary left"),
        );
        let mut stream_left = ZSetStream::new(
            dict_left,
            table.clone(),
            "delta_join_align_left",
            StreamRetention::None,
        )
        .await
        .expect("create left zset stream");

        stream_left.add_delta((0_i32, 1_i32), 1);
        stream_left.flush().await.expect("flush left t0");
        stream_left.add_delta((1_i32, 2_i32), 1);
        stream_left.flush().await.expect("flush left t1");

        let dict_right = Arc::new(
            Dictionary::with_table(table.clone(), "delta_join_align_right", None)
                .await
                .expect("dictionary right"),
        );
        let mut stream_right = ZSetStream::new(
            dict_right,
            table.clone(),
            "delta_join_align_right",
            StreamRetention::None,
        )
        .await
        .expect("create right zset stream");

        stream_right.add_delta((1_i32, 3_i32), 1);
        stream_right.flush().await.expect("flush right t0");

        let left_handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
            Arc::new(HandleGroup::new(stream_left.stream.handle()));
        let mut outer_left = Stream::with_table(
            table.clone(),
            "delta_join_align_outer_left",
            left_handle_group,
        )
        .await
        .expect("create outer left stream");
        let left_default = stream_left.stream.handle();
        set_default_in_place(&mut outer_left, left_default.clone());
        let left_latest = stream_left.stream.handle();
        push_value_in_place(&mut outer_left, left_latest.clone());
        outer_left.flush().await.expect("flush outer left stream");

        let right_handle_group: Arc<dyn AbelianGroup<StreamHandle>> =
            Arc::new(HandleGroup::new(stream_right.stream.handle()));
        let mut outer_right = Stream::with_table(
            table.clone(),
            "delta_join_align_outer_right",
            right_handle_group,
        )
        .await
        .expect("create outer right stream");
        let right_default = stream_right.stream.handle();
        set_default_in_place(&mut outer_right, right_default.clone());
        outer_right.flush().await.expect("flush outer right stream");

        let mut result = delta_lifted_delta_lifted_join(
            &outer_left,
            &outer_right,
            |left: &(i32, i32), right: &(i32, i32)| left.1 == right.0,
            |left: &(i32, i32), right: &(i32, i32)| (left.0, right.1),
        )
        .await
        .expect("compute aligned delta lifted join");
        result
            .flush()
            .await
            .expect("flush aligned delta lifted join output");

        assert_eq!(
            result.timestamp, outer_right.timestamp,
            "aggregator should stop at shortest timeline"
        );

        let handles = collect_values(&result, result.timestamp)
            .await
            .expect("collect aligned result handles");
        assert_eq!(
            handles.len(),
            usize::try_from(outer_right.timestamp.saturating_add(1))
                .expect("convert timestamp to length"),
            "expected handles only up to shortest stream frontier"
        );

        let mut cache = HashMap::new();
        if let Some(handle) = handles.first() {
            let group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(ZSetHandle {
                ns: handle.ns.clone(),
                version: 0,
            }));
            let mut resolved = result
                .resolve_handle(handle, group)
                .await
                .expect("resolve aggregated handle");
            let zset_handle = resolved.latest().await.expect("latest aggregated zset");
            let _materialized =
                materialize_zset_handle::<(i32, i32)>(table.clone(), &mut cache, &zset_handle)
                    .await
                    .expect("materialize aligned aggregated zset");
        }
    }

    #[tokio::test]
    async fn zset_stream_handles_round_trip() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "zset_stream_round", None)
                .await
                .expect("build dictionary"),
        );

        let mut stream = ZSetStream::new(
            dict.clone(),
            table.clone(),
            "zset_stream_handles",
            StreamRetention::None,
        )
        .await
        .expect("create zset stream");

        stream.add_delta("apple".to_string(), 3);
        stream.add_delta("banana".to_string(), 5);
        let handle = stream.flush().await.expect("flush overlay");

        let view = stream.handle_view(&handle);
        let materialized = view.materialize().await.expect("materialize view");
        assert_eq!(materialized.get("apple"), Some(&3));
        assert_eq!(materialized.get("banana"), Some(&5));

        drop(stream);

        let dict_reopen = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "zset_stream_round", None)
                .await
                .expect("rebuild dictionary"),
        );
        let mut reopened = ZSetStream::new(
            dict_reopen,
            table.clone(),
            "zset_stream_handles",
            StreamRetention::None,
        )
        .await
        .expect("reopen zset stream");

        let latest_handle = reopened
            .latest_handle()
            .await
            .expect("fetch latest handle after reopen");
        let reopened_view = reopened.handle_view(&latest_handle);
        let reopened_materialized = reopened_view
            .materialize()
            .await
            .expect("materialize reopened view");

        assert_eq!(reopened_materialized.get("apple"), Some(&3));
        assert_eq!(reopened_materialized.get("banana"), Some(&5));
    }

    #[tokio::test]
    async fn zset_stream_reuses_handle_when_overlay_empty() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "zset_stream_reuse", None)
                .await
                .expect("build dictionary"),
        );

        let mut stream = ZSetStream::new(dict, table, "zset_stream_reuse", StreamRetention::None)
            .await
            .expect("build zset stream");

        stream.add_delta("key".to_string(), 1);
        let first = stream.flush().await.expect("flush first delta");
        let second = stream.flush().await.expect("flush without delta");

        assert_eq!(first.version, second.version);
        assert_eq!(stream.current_handle().version, second.version);
    }

    #[tokio::test]
    async fn zset_stream_retention_releases_versions() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "zset_stream_retention", None)
                .await
                .expect("build dictionary"),
        );

        let mut stream = ZSetStream::new(
            dict,
            table.clone(),
            "zset_stream_retention",
            StreamRetention::KeepLast { keep_last: 1 },
        )
        .await
        .expect("build zset stream");

        stream.add_delta("a".to_string(), 1);
        let first = stream.flush().await.expect("flush first");

        stream.add_delta("b".to_string(), 2);
        let second = stream.flush().await.expect("flush second");

        assert_ne!(first.version, second.version);

        let first_version = first.version;

        let (manifest_prefix, table_arc) = {
            let prefix = format!("zset/{}/manifest/", stream.namespace()).into_bytes();
            (prefix, stream.versioned().table())
        };

        let manifests = table_arc
            .scan_prefix(&manifest_prefix, &ScanOptions::default())
            .await
            .expect("scan manifests after retention");
        assert_eq!(manifests.len(), 2);

        let (dict_arc, table_clone, namespace) = {
            let versioned = stream.versioned();
            (
                versioned.dictionary(),
                versioned.table(),
                versioned.namespace().to_string(),
            )
        };

        let versioned_state = VersionedZSet::new(dict_arc, table_clone, namespace)
            .await
            .expect("reopen versioned for inspection");

        let manifest_first = versioned_state
            .manifest_record(first_version)
            .await
            .expect("load manifest for released version");
        assert_eq!(manifest_first.reference_count, 2);

        let manifest_latest = versioned_state.manifest().expect("latest manifest present");
        assert_eq!(manifest_latest.reference_count, 2);
        assert_eq!(manifest_latest.base, Some(first_version));
    }

    #[tokio::test]
    async fn zset_stream_clears_lingering_intents_on_reopen() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let dict = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "zset_stream_intent", None)
                .await
                .expect("build dictionary"),
        );

        let mut stream = ZSetStream::new(
            dict.clone(),
            table.clone(),
            "zset_stream_intent",
            StreamRetention::None,
        )
        .await
        .expect("create zset stream");

        stream.add_delta("x".to_string(), 4);
        stream.flush().await.expect("flush delta");

        let version_intent = stream.versioned().intent_key_bytes().to_vec();
        let stream_intent = stream.stream_intent_key();

        let mut batch = WriteBatch::new();
        batch.put(version_intent.clone(), vec![1]);
        batch.put(stream_intent.clone(), vec![1]);
        table
            .write_batch(batch)
            .await
            .expect("write lingering intents");

        drop(stream);

        let dict_reopen = Arc::new(
            Dictionary::<String>::with_table(table.clone(), "zset_stream_intent", None)
                .await
                .expect("rebuild dictionary"),
        );
        let mut reopened = ZSetStream::new(
            dict_reopen,
            table.clone(),
            "zset_stream_intent",
            StreamRetention::None,
        )
        .await
        .expect("reopen stream");

        assert!(
            table
                .get(&version_intent)
                .await
                .expect("read version intent")
                .is_none()
        );
        assert!(
            table
                .get(&stream_intent)
                .await
                .expect("read stream intent")
                .is_none()
        );

        reopened.add_delta("y".to_string(), 1);
        let handle = reopened.flush().await.expect("flush reopened stream");
        let view = reopened.handle_view(&handle);
        let materialized = view.materialize().await.expect("materialize reopened");
        assert_eq!(materialized.get("y"), Some(&1));
    }

    #[tokio::test]
    async fn stream_handle_round_trip() {
        let db = build_db().await;
        let int_group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);
        let mut inner = Stream::new(db.clone(), "inner_stream", int_group.clone())
            .await
            .expect("create inner stream");

        inner.send(3).await.expect("send first");
        inner.send(7).await.expect("send second");
        inner.flush().await.expect("flush inner");

        let inner_handle = inner.handle();
        assert_eq!(inner_handle.ns, inner.namespace());
        assert_eq!(inner_handle.frontier, inner.current_time());

        let outer_group: Arc<dyn AbelianGroup<StreamHandle>> =
            Arc::new(HandleGroup::new(inner_handle.clone()));
        let mut outer = Stream::new(db.clone(), "outer_stream", outer_group.clone())
            .await
            .expect("create outer stream");

        outer
            .send(inner_handle.clone())
            .await
            .expect("write handle to outer");
        outer.flush().await.expect("flush outer");

        let mut reopened_outer = Stream::new(db.clone(), "outer_stream", outer_group)
            .await
            .expect("reopen outer stream");

        let mut resolved_inner = reopened_outer
            .resolve_latest(int_group.clone())
            .await
            .expect("resolve latest handle");
        assert_eq!(resolved_inner.get(inner_handle.frontier).await.unwrap(), 7);

        let mut reopened_inner = Stream::open_at(
            db,
            inner_handle.ns.clone(),
            int_group,
            inner_handle.frontier,
        )
        .await
        .expect("open inner at handle frontier");
        assert_eq!(reopened_inner.latest().await.expect("reopened latest"), 7);
    }
}
