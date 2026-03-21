use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use dbsp_runtime::algebra::AbelianGroup;
use dbsp_runtime::collections::zset::{SegmentRecord, VersionedZSet};
use dbsp_runtime::handles::ZSetHandle;
use dbsp_runtime::storage::KeyValueTable;
use dbsp_runtime::storage::dictionary::{Dictionary, KeyIntern};
use dbsp_runtime::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use dbsp_runtime::stream::Stream as RuntimeStream;
use dbsp_runtime::stream::util::{compute_delta, materialize_zset_handle};
use dbsp_runtime::stream::{DeltaHandleStream, SnapshotHandleStream};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::sync::Mutex as AsyncMutex;

use crate::stream::ReferenceEvaluator;
use crate::stream::Stream;
use crate::values::{GroupValue, IndexedZSet, RuntimeKeyBounds, Set, ZSet};

pub trait RuntimeValueBounds:
    Archive + Clone + PartialEq + Send + Sync + 'static + for<'a> RkyvSerialize<RkyvSerializer<'a>>
where
    Self::Archived: RkyvDeserialize<Self, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
}

impl<T> RuntimeValueBounds for T
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<Self, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
}

#[derive(Clone)]
struct RuntimeGroup<T> {
    marker: PhantomData<T>,
}

#[async_trait]
impl<T> AbelianGroup<T> for RuntimeGroup<T>
where
    T: RuntimeValueBounds + GroupValue,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    async fn add(&self, a: &T, b: &T) -> T {
        a.add(b)
    }

    async fn neg(&self, a: &T) -> T {
        a.neg()
    }

    async fn identity(&self) -> T {
        T::zero()
    }
}

#[derive(Clone)]
struct RuntimeHandleGroup {
    default: ZSetHandle,
}

#[async_trait]
impl AbelianGroup<ZSetHandle> for RuntimeHandleGroup {
    async fn add(&self, a: &ZSetHandle, _b: &ZSetHandle) -> ZSetHandle {
        a.clone()
    }

    async fn neg(&self, a: &ZSetHandle) -> ZSetHandle {
        a.clone()
    }

    async fn identity(&self) -> ZSetHandle {
        self.default.clone()
    }
}

struct ScalarLoweringDriver<T>
where
    T: RuntimeValueBounds + GroupValue,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    semantic: Stream<T>,
    evaluator: ReferenceEvaluator,
    runtime: RuntimeStream<T>,
    emitted_len: usize,
}

impl<T> ScalarLoweringDriver<T>
where
    T: RuntimeValueBounds + GroupValue,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    async fn ensure_prefix(&mut self, observed_len: usize) -> Result<()> {
        while self.emitted_len < observed_len {
            let t = self.emitted_len;
            let value = self.evaluator.at(&self.semantic, t);
            if t == 0 {
                self.runtime.set_default(value).await?;
            } else {
                self.runtime.send(value.clone()).await?;
                self.runtime.set_default(value).await?;
            }
            self.runtime.flush().await?;
            self.emitted_len += 1;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct LoweredScalarStream<T>
where
    T: RuntimeValueBounds + GroupValue,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    stream: RuntimeStream<T>,
    driver: Arc<AsyncMutex<ScalarLoweringDriver<T>>>,
}

impl<T> LoweredScalarStream<T>
where
    T: RuntimeValueBounds + GroupValue,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub fn stream(&self) -> RuntimeStream<T> {
        self.stream.clone()
    }

    pub fn namespace(&self) -> &str {
        self.stream.namespace()
    }

    pub async fn ensure_prefix(&self, observed_len: usize) -> Result<()> {
        self.driver.lock().await.ensure_prefix(observed_len).await
    }

    pub async fn collect_prefix(&self, observed_len: usize) -> Result<Vec<T>> {
        self.ensure_prefix(observed_len).await?;
        let mut stream = self.stream();
        collect_runtime_scalar_prefix(&mut stream, observed_len).await
    }
}

struct ZSetLoweringDriver<K>
where
    K: RuntimeKeyBounds,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    semantic: Stream<ZSet<K>>,
    evaluator: ReferenceEvaluator,
    dict: Arc<Dictionary<K>>,
    snapshot_versioned: VersionedZSet<K>,
    delta_versioned: VersionedZSet<K>,
    snapshot_stream: RuntimeStream<ZSetHandle>,
    delta_stream: RuntimeStream<ZSetHandle>,
    previous_snapshot: HashMap<K, i64>,
    emitted_len: usize,
    delta_default: ZSetHandle,
}

impl<K> ZSetLoweringDriver<K>
where
    K: RuntimeKeyBounds,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    async fn ensure_prefix(&mut self, observed_len: usize) -> Result<()> {
        while self.emitted_len < observed_len {
            let t = self.emitted_len;
            let snapshot = self.evaluator.at(&self.semantic, t);
            let current_snapshot: HashMap<K, i64> = snapshot.iter().cloned().collect();
            let delta_entries = compute_delta(&self.previous_snapshot, &current_snapshot);

            let snapshot_handle = if delta_entries.is_empty() {
                self.snapshot_versioned
                    .current_handle()
                    .unwrap_or_else(|| self.snapshot_versioned.handle_for_version(0))
            } else {
                persist_delta_version(
                    &self.dict,
                    &mut self.snapshot_versioned,
                    &delta_entries,
                    true,
                )
                .await?
            };

            let delta_handle = if delta_entries.is_empty() {
                self.delta_versioned.handle_for_version(0)
            } else {
                persist_delta_version(&self.dict, &mut self.delta_versioned, &delta_entries, false)
                    .await?
            };

            if t == 0 {
                self.snapshot_stream
                    .set_default(snapshot_handle.clone())
                    .await?;
                self.delta_stream.set_default(delta_handle.clone()).await?;
            } else {
                self.snapshot_stream.send(snapshot_handle.clone()).await?;
                self.snapshot_stream.set_default(snapshot_handle).await?;
                self.delta_stream.send(delta_handle).await?;
                self.delta_stream
                    .set_default(self.delta_default.clone())
                    .await?;
            }

            self.snapshot_stream.flush().await?;
            self.delta_stream.flush().await?;
            self.previous_snapshot = current_snapshot;
            self.emitted_len += 1;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct LoweredZSetStream<K>
where
    K: RuntimeKeyBounds,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    table: Arc<dyn KeyValueTable>,
    snapshot: SnapshotHandleStream,
    delta: DeltaHandleStream,
    driver: Arc<AsyncMutex<ZSetLoweringDriver<K>>>,
    marker: PhantomData<K>,
}

impl<K> LoweredZSetStream<K>
where
    K: RuntimeKeyBounds,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub fn snapshot_stream(&self) -> SnapshotHandleStream {
        self.snapshot.clone()
    }

    pub fn delta_stream(&self) -> DeltaHandleStream {
        self.delta.clone()
    }

    pub async fn ensure_prefix(&self, observed_len: usize) -> Result<()> {
        self.driver.lock().await.ensure_prefix(observed_len).await
    }

    pub async fn collect_snapshot_prefix(&self, observed_len: usize) -> Result<Vec<ZSet<K>>> {
        self.ensure_prefix(observed_len).await?;
        let mut stream = self.snapshot.stream();
        collect_runtime_zset_prefix::<K>(self.table.clone(), &mut stream, observed_len).await
    }

    pub async fn collect_delta_prefix(&self, observed_len: usize) -> Result<Vec<ZSet<K>>> {
        self.ensure_prefix(observed_len).await?;
        let mut stream = self.delta.stream();
        collect_runtime_zset_prefix::<K>(self.table.clone(), &mut stream, observed_len).await
    }
}

pub async fn lower_scalar<T>(
    table: Arc<dyn KeyValueTable>,
    namespace: impl Into<String>,
    input: &Stream<T>,
) -> Result<LoweredScalarStream<T>>
where
    T: RuntimeValueBounds + GroupValue,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let group: Arc<dyn AbelianGroup<T>> = Arc::new(RuntimeGroup::<T> {
        marker: PhantomData,
    });
    let stream = RuntimeStream::with_table(table, namespace.into(), group).await?;
    Ok(LoweredScalarStream {
        stream: stream.clone(),
        driver: Arc::new(AsyncMutex::new(ScalarLoweringDriver {
            semantic: input.clone(),
            evaluator: ReferenceEvaluator::default(),
            runtime: stream,
            emitted_len: 0,
        })),
    })
}

pub async fn lower_zset<K>(
    table: Arc<dyn KeyValueTable>,
    namespace: impl Into<String>,
    input: &Stream<ZSet<K>>,
) -> Result<LoweredZSetStream<K>>
where
    K: RuntimeKeyBounds,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let namespace = namespace.into();
    let delta_namespace = format!("{namespace}/delta");
    let dict = Arc::new(Dictionary::<K>::with_table(table.clone(), namespace.clone(), None).await?);
    let snapshot_versioned =
        VersionedZSet::new(dict.clone(), table.clone(), namespace.clone()).await?;
    let delta_versioned =
        VersionedZSet::new(dict.clone(), table.clone(), delta_namespace.clone()).await?;

    let snapshot_default = ZSetHandle {
        ns: namespace.clone(),
        version: 0,
    };
    let delta_default = ZSetHandle {
        ns: delta_namespace.clone(),
        version: 0,
    };

    let snapshot_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(RuntimeHandleGroup {
        default: snapshot_default,
    });
    let delta_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(RuntimeHandleGroup {
        default: delta_default.clone(),
    });

    let snapshot_stream =
        RuntimeStream::with_table(table.clone(), namespace.clone(), snapshot_group).await?;
    let delta_stream =
        RuntimeStream::with_table(table.clone(), delta_namespace.clone(), delta_group).await?;

    Ok(LoweredZSetStream {
        table,
        snapshot: SnapshotHandleStream::new(snapshot_stream.clone()),
        delta: DeltaHandleStream::new(delta_stream.clone()),
        driver: Arc::new(AsyncMutex::new(ZSetLoweringDriver {
            semantic: input.clone(),
            evaluator: ReferenceEvaluator::default(),
            dict,
            snapshot_versioned,
            delta_versioned,
            snapshot_stream,
            delta_stream,
            previous_snapshot: HashMap::new(),
            emitted_len: 0,
            delta_default,
        })),
        marker: PhantomData,
    })
}

pub async fn lower_set<K>(
    table: Arc<dyn KeyValueTable>,
    namespace: impl Into<String>,
    input: &Stream<Set<K>>,
) -> Result<LoweredZSetStream<K>>
where
    K: RuntimeKeyBounds,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let zset_stream = input.lift("lower_set_to_zset", |value| value.to_zset());
    lower_zset(table, namespace, &zset_stream).await
}

pub async fn lower_indexed<K, V>(
    table: Arc<dyn KeyValueTable>,
    namespace: impl Into<String>,
    input: &Stream<IndexedZSet<K, V>>,
) -> Result<LoweredZSetStream<(K, V)>>
where
    K: RuntimeKeyBounds,
    V: RuntimeKeyBounds,
    (K, V): RuntimeKeyBounds,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    <(K, V) as Archive>::Archived:
        RkyvDeserialize<(K, V), RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let pair_stream = input.lift("lower_indexed_to_pairs", |value| value.as_pairs());
    lower_zset(table, namespace, &pair_stream).await
}

pub async fn collect_runtime_scalar_prefix<T>(
    stream: &mut RuntimeStream<T>,
    observed_len: usize,
) -> Result<Vec<T>>
where
    T: RuntimeValueBounds,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut out = Vec::with_capacity(observed_len);
    for t in 0..observed_len {
        out.push(stream.get(t as i64).await?);
    }
    Ok(out)
}

pub async fn collect_runtime_zset_prefix<K>(
    table: Arc<dyn KeyValueTable>,
    stream: &mut RuntimeStream<ZSetHandle>,
    observed_len: usize,
) -> Result<Vec<ZSet<K>>>
where
    K: RuntimeKeyBounds,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut dict_cache = HashMap::new();
    let mut out = Vec::with_capacity(observed_len);
    for t in 0..observed_len {
        let handle = stream.get(t as i64).await?;
        let rows = materialize_zset_handle::<K>(table.clone(), &mut dict_cache, &handle).await?;
        out.push(ZSet::from_weights(rows.into_iter()));
    }
    Ok(out)
}

async fn persist_delta_version<K>(
    dict: &Arc<Dictionary<K>>,
    versioned: &mut VersionedZSet<K>,
    delta_entries: &[(K, i64)],
    chain_on_current: bool,
) -> Result<ZSetHandle>
where
    K: RuntimeKeyBounds,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut by_bucket: HashMap<u16, Vec<(u64, i64)>> = HashMap::new();
    for (key, weight) in delta_entries {
        if *weight == 0 {
            continue;
        }
        let id = dict.intern(key).await?;
        by_bucket
            .entry(bucket_for(id))
            .or_default()
            .push((id, *weight));
    }

    let mut segments: Vec<_> = by_bucket
        .into_iter()
        .map(|(bucket, mut deltas)| {
            deltas.sort_by_key(|(id, _)| *id);
            SegmentRecord {
                id: 0,
                bucket,
                deltas,
            }
        })
        .collect();
    segments.sort_by_key(|segment| segment.bucket);

    if segments.is_empty() {
        return Ok(versioned
            .current_handle()
            .unwrap_or_else(|| versioned.handle_for_version(0)));
    }

    let version = if chain_on_current {
        versioned.create_version(segments).await?
    } else {
        versioned.create_version_with_base(segments, None).await?
    };
    Ok(versioned.handle_for_version(version))
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}
