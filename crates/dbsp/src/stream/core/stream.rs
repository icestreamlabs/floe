use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::config::ScanOptions;
use slatedb::{Db, WriteBatch};
use tokio::sync::watch;

use crate::algebra::AbelianGroup;
use crate::handles::{StreamHandle, ZSetHandle};
use crate::storage::encoding::{self, RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::storage::keyspace::{self, namespace_prefix};
use crate::storage::timestamps;
use crate::storage::{KeyValueTable, SlateTable};

/// Logical-time stream: at time `t`, this holds one value of type `T`.
/// For Floe SQL:
///   - `Stream<ZSetHandle>` represents the delta (Delta R_t) of a relation `R` at time `t`.
pub type DeltaStream = Stream<ZSetHandle>;

/// Logical-time stream keyed by a logical transaction index.
/// - Time = logical transaction index.
/// - For each relation `R`: `Stream<ZSetHandle>` represents the delta (Delta R_t) of `R` at time `t`.
/// - `VersionedZSet<K>` is the integrated `R_t`.
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
    core: Arc<StreamCore<T>>,
    frontier_rx: watch::Receiver<i64>,
}

struct StreamCore<T>
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
    state: RwLock<StreamState<T>>,
    frontier_tx: watch::Sender<i64>,
}

struct StreamState<T>
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

impl<T> StreamState<T>
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
    fn new(default: T) -> Self {
        Self {
            timestamp: 0,
            identity: true,
            default,
            pending_data: BTreeMap::new(),
            pending_defaults: BTreeMap::new(),
            pending_state: false,
            data_cache: HashMap::new(),
            default_changes: BTreeMap::new(),
            last_default_ts: 0,
        }
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
}

impl<T> StreamCore<T>
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
    fn encode_data_key(&self, timestamp: i64) -> Result<Vec<u8>> {
        timestamps::append(self.data_prefix.as_slice(), timestamp)
    }

    fn encode_default_key(&self, timestamp: i64) -> Result<Vec<u8>> {
        timestamps::append(self.default_prefix.as_slice(), timestamp)
    }

    fn encode_intent_key(&self) -> Vec<u8> {
        let mut key = self.state_key.clone();
        key.extend_from_slice(b"/intent");
        key
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
            core: Arc::clone(&self.core),
            frontier_rx: self.core.frontier_tx.subscribe(),
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
    fn read_state(&self) -> std::sync::RwLockReadGuard<'_, StreamState<T>> {
        self.core.state.read().expect("stream state poisoned")
    }

    fn write_state(&self) -> std::sync::RwLockWriteGuard<'_, StreamState<T>> {
        self.core.state.write().expect("stream state poisoned")
    }

    fn notify_frontier(&self, ts: i64) {
        let _ = self.core.frontier_tx.send(ts);
    }

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
        let state = StreamState::new(initial_default.clone());
        let (frontier_tx, frontier_rx) = watch::channel(state.timestamp);
        let core = Arc::new(StreamCore {
            table,
            namespace: namespace.clone(),
            data_prefix,
            default_prefix,
            state_key,
            group,
            state: RwLock::new(state),
            frontier_tx,
        });

        let mut stream = Self { core, frontier_rx };
        let mut needs_initial_flush = false;

        stream.core.clear_intent().await?;

        if let Some(bytes) = stream.table().get(&stream.core.state_key).await? {
            let (timestamp, identity, default, last_default_ts) =
                if let Ok(tuple) = encoding::decode::<(i64, bool, T, i64)>(&bytes) {
                    tuple
                } else {
                    let (timestamp, identity, default) = encoding::decode::<(i64, bool, T)>(&bytes)
                        .context("unable to decode legacy stream state")?;
                    (timestamp, identity, default, timestamp)
                };
            {
                let mut state = stream.write_state();
                state.timestamp = timestamp;
                state.identity = identity;
                state.default = default.clone();
                state.last_default_ts = last_default_ts;
            }
            let default_changes = stream.core.load_default_changes().await?;
            {
                let mut state = stream.write_state();
                state.default_changes = default_changes;
                state.last_default_ts = state.default_changes.keys().copied().max().unwrap_or(0);
                let missing_default = {
                    state
                        .default_changes
                        .range(..=state.timestamp)
                        .rev()
                        .next()
                        .is_none()
                };
                if missing_default {
                    let default_value = state.default.clone();
                    state.default_changes.insert(0, default_value);
                }
            }
            stream.notify_frontier(timestamp);
        } else {
            {
                let mut state = stream.write_state();
                state.default_changes.insert(0, initial_default.clone());
                state.last_default_ts = 0;
                state.pending_defaults.insert(0, initial_default.clone());
                state.pending_state = true;
            }
            needs_initial_flush = true;
        }

        {
            let mut state = stream.write_state();
            state.data_cache.reserve(16);
        }

        if needs_initial_flush {
            stream.flush().await?;
        }

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
        let mut stream = Self::with_table(table, namespace, group).await?;
        if frontier > stream.current_time() {
            stream.advance_to(frontier).await?;
        }
        Ok(stream)
    }

    pub fn group(&self) -> Arc<dyn AbelianGroup<T>> {
        self.core.group.clone()
    }

    pub fn namespace(&self) -> &str {
        &self.core.namespace
    }

    pub(crate) fn table(&self) -> Arc<dyn KeyValueTable> {
        self.core.table.clone()
    }

    pub fn current_time(&self) -> i64 {
        *self.frontier_rx.borrow()
    }

    pub fn is_identity(&self) -> bool {
        self.read_state().identity
    }

    pub fn default_value(&self) -> T {
        self.read_state().default.clone()
    }

    #[cfg(test)]
    pub(crate) fn last_default_ts(&self) -> i64 {
        self.read_state().last_default_ts
    }

    pub fn handle(&self) -> StreamHandle {
        StreamHandle {
            ns: self.core.namespace.clone(),
            frontier: self.current_time(),
        }
    }

    pub async fn send(&mut self, element: T) -> Result<i64> {
        let next_timestamp = {
            let mut state = self.write_state();
            let next_timestamp = state.timestamp + 1;
            if element != state.default {
                state.pending_data.insert(next_timestamp, element.clone());
                state.data_cache.insert(next_timestamp, element);
                state.identity = false;
            }
            state.timestamp = next_timestamp;
            state.pending_state = true;
            next_timestamp
        };
        self.notify_frontier(next_timestamp);
        Ok(next_timestamp)
    }

    pub async fn set_default(&mut self, new_default: T) -> Result<()> {
        let mut state = self.write_state();
        let current_ts = state.timestamp;
        state.default = new_default.clone();
        state.pending_defaults.insert(current_ts, new_default);
        state.pending_state = true;
        Ok(())
    }

    pub async fn get(&mut self, timestamp: i64) -> Result<T> {
        if timestamp < 0 {
            return Err(anyhow!("timestamp cannot be negative"));
        }

        loop {
            let mut fetch_key: Option<Vec<u8>> = None;
            let mut fallback_value: Option<T> = None;
            let mut needs_advance = false;

            {
                let state = self.read_state();
                if timestamp > state.timestamp {
                    needs_advance = true;
                } else if let Some(value) = state.pending_data.get(&timestamp) {
                    return Ok(value.clone());
                } else if let Some(value) = state.data_cache.get(&timestamp) {
                    return Ok(value.clone());
                } else {
                    fetch_key = Some(self.core.encode_data_key(timestamp)?);
                    fallback_value = Some(state.default_at(timestamp));
                }
            }

            if needs_advance {
                self.advance_to(timestamp).await?;
                continue;
            }

            if let Some(key) = fetch_key {
                if let Some(bytes) = self.core.table.get(&key).await? {
                    let value: T =
                        encoding::decode(&bytes).context("unable to decode stream value")?;
                    {
                        let mut state = self.write_state();
                        state.data_cache.insert(timestamp, value.clone());
                    }
                    return Ok(value);
                } else if let Some(default_value) = fallback_value {
                    return Ok(default_value);
                }
            }
        }
    }

    pub async fn latest(&mut self) -> Result<T> {
        self.get(self.current_time()).await
    }

    pub async fn latest_with_ts(&mut self) -> Result<(i64, T)> {
        let ts = self.current_time();
        let value = self.get(ts).await?;
        Ok((ts, value))
    }

    pub async fn to_vec(&mut self) -> Result<Vec<T>> {
        let frontier = self.current_time();
        let mut values = Vec::with_capacity((frontier + 1) as usize);
        for t in 0..=frontier {
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
            self.core.table.write_batch(batch).await?;

            let mut cleanup = WriteBatch::new();
            cleanup.delete(intent_key);
            self.core.table.write_batch(cleanup).await?;
        }

        {
            let mut state = self.write_state();
            state.pending_state = false;
        }
        Ok(())
    }

    pub(crate) fn flush_data_into(&mut self, batch: &mut WriteBatch) -> Result<bool> {
        let pending = {
            let mut state = self.write_state();
            if state.pending_data.is_empty() {
                return Ok(false);
            }
            let mut pending_map = BTreeMap::new();
            std::mem::swap(&mut pending_map, &mut state.pending_data);
            pending_map
        };

        for (timestamp, value) in pending {
            let key = self.core.encode_data_key(timestamp)?;
            let encoded = encoding::encode(&value).context("unable to encode stream value")?;
            batch.put(key, encoded);
        }

        Ok(true)
    }

    pub(crate) fn flush_defaults_into(&mut self, batch: &mut WriteBatch) -> Result<bool> {
        let pending = {
            let mut state = self.write_state();
            if state.pending_defaults.is_empty() {
                return Ok(false);
            }
            let mut pending_map = BTreeMap::new();
            std::mem::swap(&mut pending_map, &mut state.pending_defaults);
            pending_map
        };

        for (timestamp, value) in pending {
            let key = self.core.encode_default_key(timestamp)?;
            let encoded = encoding::encode(&value).context("unable to encode default change")?;
            batch.put(key, encoded);
            let mut state = self.write_state();
            state.default_changes.insert(timestamp, value);
            state.last_default_ts = state.last_default_ts.max(timestamp);
        }

        Ok(true)
    }

    pub(crate) fn flush_state_into(&mut self, batch: &mut WriteBatch) -> Result<bool> {
        let snapshot = {
            let state = self.read_state();
            if !state.pending_state {
                return Ok(false);
            }
            (
                state.timestamp,
                state.identity,
                state.default.clone(),
                state.last_default_ts,
            )
        };
        let encoded = encoding::encode(&snapshot).context("unable to encode stream state")?;
        batch.put(self.core.state_key.clone(), encoded);
        Ok(true)
    }

    pub async fn advance_to(&mut self, timestamp: i64) -> Result<()> {
        loop {
            let current = self.current_time();
            if current >= timestamp {
                break;
            }
            let default = { self.read_state().default.clone() };
            self.send(default).await?;
        }
        Ok(())
    }

    pub(crate) fn encode_intent_key(&self) -> Vec<u8> {
        self.core.encode_intent_key()
    }

    pub fn subscribe_frontier(&self) -> watch::Receiver<i64> {
        self.core.frontier_tx.subscribe()
    }

    pub(crate) fn set_default_in_place(&self, value: T) {
        let mut state = self.write_state();
        let current_ts = state.timestamp;
        state.default = value.clone();
        state.pending_defaults.insert(current_ts, value);
        state.pending_state = true;
    }

    pub(crate) fn push_value_in_place(&self, value: T) {
        let next_timestamp = {
            let mut state = self.write_state();
            let next_timestamp = state.timestamp + 1;
            if value != state.default {
                state.pending_data.insert(next_timestamp, value.clone());
                state.data_cache.insert(next_timestamp, value);
                state.identity = false;
            }
            state.timestamp = next_timestamp;
            state.pending_state = true;
            next_timestamp
        };
        self.notify_frontier(next_timestamp);
    }
}
impl Stream<StreamHandle> {
    pub async fn resolve_handle<U>(
        &self,
        handle: &StreamHandle,
        group: Arc<dyn AbelianGroup<U>>,
    ) -> Result<Stream<U>>
    where
        U: Archive
            + Clone
            + PartialEq
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        U::Archived: RkyvDeserialize<U, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    {
        Stream::open_at_with_table(self.table(), handle.ns.clone(), group, handle.frontier).await
    }

    pub async fn resolve_latest<U>(&mut self, group: Arc<dyn AbelianGroup<U>>) -> Result<Stream<U>>
    where
        U: Archive
            + Clone
            + PartialEq
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        U::Archived: RkyvDeserialize<U, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    {
        let handle = self.latest().await?;
        self.resolve_handle(&handle, group).await
    }
}
