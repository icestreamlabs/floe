use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::{Result, anyhow};
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

#[derive(Clone)]
pub struct LoweredZSetStream<K> {
    snapshot: SnapshotHandleStream,
    delta: DeltaHandleStream,
    marker: PhantomData<K>,
}

impl<K> LoweredZSetStream<K> {
    pub fn snapshot_stream(&self) -> SnapshotHandleStream {
        self.snapshot.clone()
    }

    pub fn delta_stream(&self) -> DeltaHandleStream {
        self.delta.clone()
    }
}

pub async fn lower_scalar_prefix<T>(
    table: Arc<dyn KeyValueTable>,
    namespace: impl Into<String>,
    input: &Stream<T>,
    observed_len: usize,
) -> Result<RuntimeStream<T>>
where
    T: RuntimeValueBounds + GroupValue,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    if observed_len == 0 {
        return Err(anyhow!("observed_len must be positive"));
    }

    let group: Arc<dyn AbelianGroup<T>> = Arc::new(RuntimeGroup::<T> {
        marker: PhantomData,
    });
    let mut stream = RuntimeStream::with_table(table, namespace.into(), group).await?;
    let observed = ReferenceEvaluator::observe_prefix(input, observed_len);
    stream.set_default(observed[0].clone()).await?;
    for value in observed.iter().skip(1) {
        stream.send(value.clone()).await?;
    }
    stream
        .set_default(observed.last().expect("observed prefix non-empty").clone())
        .await?;
    stream.flush().await?;
    Ok(stream)
}

pub async fn lower_zset_prefix<K>(
    table: Arc<dyn KeyValueTable>,
    namespace: impl Into<String>,
    input: &Stream<ZSet<K>>,
    observed_len: usize,
) -> Result<LoweredZSetStream<K>>
where
    K: RuntimeKeyBounds,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    if observed_len == 0 {
        return Err(anyhow!("observed_len must be positive"));
    }

    let namespace = namespace.into();
    let delta_namespace = format!("{namespace}/delta");
    let dict = Arc::new(Dictionary::<K>::with_table(table.clone(), namespace.clone(), None).await?);
    let mut snapshot_versioned =
        VersionedZSet::new(dict.clone(), table.clone(), namespace.clone()).await?;
    let mut delta_versioned =
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
        default: snapshot_default.clone(),
    });
    let delta_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(RuntimeHandleGroup {
        default: delta_default.clone(),
    });

    let mut snapshot_stream =
        RuntimeStream::with_table(table.clone(), namespace.clone(), snapshot_group).await?;
    let mut delta_stream =
        RuntimeStream::with_table(table.clone(), delta_namespace.clone(), delta_group).await?;

    let observed = ReferenceEvaluator::observe_prefix(input, observed_len);
    let mut previous_snapshot: HashMap<K, i64> = HashMap::new();
    let mut snapshot_handles = Vec::with_capacity(observed_len);
    let mut delta_handles = Vec::with_capacity(observed_len);

    for snapshot in observed {
        let current_snapshot: HashMap<K, i64> = snapshot.iter().cloned().collect();
        let delta_entries = compute_delta(&previous_snapshot, &current_snapshot);

        let snapshot_handle = if delta_entries.is_empty() {
            snapshot_versioned
                .current_handle()
                .unwrap_or_else(|| snapshot_versioned.handle_for_version(0))
        } else {
            persist_delta_version(&dict, &mut snapshot_versioned, &delta_entries, true).await?
        };

        let delta_handle = if delta_entries.is_empty() {
            delta_versioned.handle_for_version(0)
        } else {
            persist_delta_version(&dict, &mut delta_versioned, &delta_entries, false).await?
        };

        snapshot_handles.push(snapshot_handle);
        delta_handles.push(delta_handle);
        previous_snapshot = current_snapshot;
    }

    snapshot_stream
        .set_default(snapshot_handles[0].clone())
        .await?;
    delta_stream.set_default(delta_handles[0].clone()).await?;

    for handle in snapshot_handles.iter().skip(1) {
        snapshot_stream.send(handle.clone()).await?;
    }
    for handle in delta_handles.iter().skip(1) {
        delta_stream.send(handle.clone()).await?;
    }

    snapshot_stream
        .set_default(
            snapshot_handles
                .last()
                .expect("observed prefix non-empty")
                .clone(),
        )
        .await?;
    delta_stream.set_default(delta_default).await?;

    snapshot_stream.flush().await?;
    delta_stream.flush().await?;

    Ok(LoweredZSetStream {
        snapshot: SnapshotHandleStream::new(snapshot_stream),
        delta: DeltaHandleStream::new(delta_stream),
        marker: PhantomData,
    })
}

pub async fn lower_set_prefix<K>(
    table: Arc<dyn KeyValueTable>,
    namespace: impl Into<String>,
    input: &Stream<Set<K>>,
    observed_len: usize,
) -> Result<LoweredZSetStream<K>>
where
    K: RuntimeKeyBounds,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let zset_stream = input.lift("lower_set_to_zset", |value| value.to_zset());
    lower_zset_prefix(table, namespace, &zset_stream, observed_len).await
}

pub async fn lower_indexed_prefix<K, V>(
    table: Arc<dyn KeyValueTable>,
    namespace: impl Into<String>,
    input: &Stream<IndexedZSet<K, V>>,
    observed_len: usize,
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
    lower_zset_prefix(table, namespace, &pair_stream, observed_len).await
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
