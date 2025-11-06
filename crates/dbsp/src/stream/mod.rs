use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};
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
