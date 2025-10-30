use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::config::ScanOptions;
use slatedb::{Db, WriteBatch};

use crate::algebra::AbelianGroup;
use crate::storage::encoding::{self, RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::storage::{KeyValueTable, SlateTable};

const STREAM_PREFIX: &str = "stream/";

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
        let mut base = STREAM_PREFIX.as_bytes().to_vec();
        base.extend_from_slice(namespace.as_bytes());
        base.push(b'/');

        let mut data_prefix = base.clone();
        data_prefix.extend_from_slice(b"data/");

        let mut default_prefix = base.clone();
        default_prefix.extend_from_slice(b"default/");

        let mut state_key = base;
        state_key.extend_from_slice(b"meta/state");

        let initial_default = group.identity();

        let mut stream = Self {
            table,
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
        };

        if let Some(bytes) = stream.table.get(&stream.state_key).await? {
            let (timestamp, identity, default): (i64, bool, T) =
                encoding::decode(&bytes).context("unable to decode stream state")?;
            stream.timestamp = timestamp;
            stream.identity = identity;
            stream.default = default.clone();
            stream.default_changes = stream.load_default_changes().await?;
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
            stream.pending_defaults.insert(0, initial_default.clone());
            stream.pending_state = true;
            stream.flush().await?;
        }

        stream.data_cache.reserve(16);
        Ok(stream)
    }

    pub fn group(&self) -> Arc<dyn AbelianGroup<T>> {
        self.group.clone()
    }

    pub fn current_time(&self) -> i64 {
        self.timestamp
    }

    pub fn is_identity(&self) -> bool {
        self.identity
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
            self.table.write_batch(batch).await?;
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
        }

        Ok(true)
    }

    fn flush_state_into(&mut self, batch: &mut WriteBatch) -> Result<bool> {
        if !self.pending_state {
            return Ok(false);
        }

        let state = (self.timestamp, self.identity, self.default.clone());
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
        if timestamp < 0 {
            return Err(anyhow!("timestamp cannot be negative"));
        }

        let mut key = self.data_prefix.clone();
        key.extend_from_slice(&timestamp.to_be_bytes());
        Ok(key)
    }

    fn encode_default_key(&self, timestamp: i64) -> Result<Vec<u8>> {
        if timestamp < 0 {
            return Err(anyhow!("timestamp cannot be negative"));
        }
        let mut key = self.default_prefix.clone();
        key.extend_from_slice(&timestamp.to_be_bytes());
        Ok(key)
    }

    async fn load_default_changes(&self) -> Result<BTreeMap<i64, T>> {
        let entries = self
            .table
            .scan_range(prefix_bounds(&self.default_prefix), &ScanOptions::default())
            .await?;

        let mut changes = BTreeMap::new();
        for (key_bytes, value_bytes) in entries {
            let timestamp = decode_timestamp(&self.default_prefix, key_bytes.as_ref())?;
            let value: T = encoding::decode(value_bytes.as_ref())
                .context("unable to decode default change")?;
            changes.insert(timestamp, value);
        }
        Ok(changes)
    }
}

fn decode_timestamp(prefix: &[u8], key: &[u8]) -> Result<i64> {
    if key.len() != prefix.len() + 8 || !key.starts_with(prefix) {
        return Err(anyhow!("invalid key while decoding timestamp"));
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key[prefix.len()..]);
    Ok(i64::from_be_bytes(bytes))
}

fn prefix_bounds(prefix: &[u8]) -> Range<Vec<u8>> {
    let mut end = prefix.to_vec();
    end.push(0xFF);
    prefix.to_vec()..end
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use object_store::memory::InMemory;

    struct IntegerGroup;

    impl AbelianGroup<i64> for IntegerGroup {
        fn add(&self, a: &i64, b: &i64) -> i64 {
            a + b
        }

        fn neg(&self, a: &i64) -> i64 {
            -a
        }

        fn identity(&self) -> i64 {
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
}
