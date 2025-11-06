use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::config::ScanOptions;
use slatedb::{Db, WriteBatch};

use crate::algebra::AbelianGroup;
use crate::handles::StreamHandle;
use crate::storage::encoding::{self, RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::storage::keyspace::{self, namespace_prefix};
use crate::storage::timestamps;
use crate::storage::{KeyValueTable, SlateTable};

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
    pub(crate) table: Arc<dyn KeyValueTable>,
    pub(crate) namespace: String,
    pub(crate) data_prefix: Vec<u8>,
    pub(crate) default_prefix: Vec<u8>,
    pub(crate) state_key: Vec<u8>,
    pub(crate) group: Arc<dyn AbelianGroup<T>>,

    pub(crate) timestamp: i64,
    pub(crate) identity: bool,
    pub(crate) default: T,

    pub(crate) pending_data: BTreeMap<i64, T>,
    pub(crate) pending_defaults: BTreeMap<i64, T>,
    pub(crate) pending_state: bool,

    pub(crate) data_cache: HashMap<i64, T>,
    pub(crate) default_changes: BTreeMap<i64, T>,
    pub(crate) last_default_ts: i64,
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
        let mut stream = Self::with_table(table, namespace, group).await?;
        if frontier > stream.timestamp {
            stream.advance_to(frontier).await?;
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

            let mut cleanup = WriteBatch::new();
            cleanup.delete(intent_key);
            self.table.write_batch(cleanup).await?;
        }

        self.pending_state = false;
        Ok(())
    }
    pub(crate) fn flush_data_into(&mut self, batch: &mut WriteBatch) -> Result<bool> {
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

    pub(crate) fn flush_defaults_into(&mut self, batch: &mut WriteBatch) -> Result<bool> {
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

    pub(crate) fn flush_state_into(&mut self, batch: &mut WriteBatch) -> Result<bool> {
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

    pub(crate) fn encode_intent_key(&self) -> Vec<u8> {
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
        Stream::open_at_with_table(
            self.table.clone(),
            handle.ns.clone(),
            group,
            handle.frontier,
        )
        .await
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
