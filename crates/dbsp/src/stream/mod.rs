use std::collections::{BTreeMap, HashMap};
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
use crate::collections::zset::{self, ZSet};
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
}
