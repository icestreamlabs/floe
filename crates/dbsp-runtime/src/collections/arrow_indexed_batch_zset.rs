use std::collections::HashSet;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use ahash::RandomState;
use anyhow::{Context, Result, anyhow};
use arrow_array::builder::BinaryBuilder;
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use hashbrown::HashMap as FastHashMap;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;
use tokio::sync::Mutex as AsyncMutex;

use crate::storage::KeyValueTable;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator, decode, encode};
use crate::storage::segment::{ArrowSegmentStore, SegmentWriteStats, encode_segment_envelope};
use crate::{handles::ZSetHandle, operator_state_registry};

use super::indexed_batch_zset::{ApplyDeltaMetrics, LookupMetrics, RangeKey};

const LOOKUP_CACHE_SHARDS: usize = 64;
const LOOKUP_CACHE_CAPACITY_PER_SHARD: usize = 2048;
const SEGMENT_CACHE_SHARDS: usize = 64;
const SEGMENT_CACHE_CAPACITY_PER_SHARD: usize = 128;
pub const DEFAULT_HOT_KEY_COMPACTION_THRESHOLD: usize = 64;

type FastMap<K, V> = FastHashMap<K, V, RandomState>;
type ValueWeightMap = FastMap<Vec<u8>, i64>;
type RowPosting = (u32, i64);
type RangePostingKey = (Vec<u8>, Vec<u8>);
type SegmentPostings = Vec<RowPosting>;
type SegmentRefsByKey = FastMap<Vec<u8>, FastMap<u64, SegmentPostings>>;

struct CachedSegment {
    values: Vec<Vec<u8>>,
}

impl CachedSegment {
    fn value_bytes(&self, row_index: u32) -> Result<&[u8]> {
        self.values
            .get(row_index as usize)
            .map(|bytes| bytes.as_slice())
            .ok_or_else(|| anyhow!("row index {row_index} out of bounds for cached segment"))
    }
}

pub struct IndexedBatchZSet<K, V>
where
    K: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    V: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    namespace: String,
    table: Arc<dyn KeyValueTable>,
    segment_store: ArrowSegmentStore,
    schema: SchemaRef,
    index_prefix: Vec<u8>,
    reverse_prefix: Vec<u8>,
    range_prefix: Vec<u8>,
    segment_sequence_key: Vec<u8>,
    reverse_enabled: bool,
    range_enabled: bool,
    segment_sequence_lock: AsyncMutex<()>,
    lookup_cache_shards: Vec<Mutex<FastMap<Vec<u8>, ValueWeightMap>>>,
    segment_cache_shards: Vec<Mutex<FastMap<u64, Arc<CachedSegment>>>>,
    hot_key_compaction_threshold: Option<usize>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> IndexedBatchZSet<K, V>
where
    K: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    V: Archive
        + Clone
        + Eq
        + std::hash::Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub fn new(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        Self::build(table, namespace.into(), false, false, None)
    }

    pub fn with_reverse_index(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        Self::build(table, namespace.into(), true, false, None)
    }

    pub fn with_range_index(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        Self::build(table, namespace.into(), false, true, None)
    }

    pub fn with_hot_key_compaction_threshold(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        threshold: usize,
    ) -> Self {
        Self::build(
            table,
            namespace.into(),
            false,
            false,
            Some(threshold.max(1)),
        )
    }

    pub fn engine_kind(&self) -> &'static str {
        "indexed_batch"
    }

    fn build(
        table: Arc<dyn KeyValueTable>,
        namespace: String,
        reverse_enabled: bool,
        range_enabled: bool,
        hot_key_compaction_threshold: Option<usize>,
    ) -> Self {
        let namespace_hash = stable_namespace_hash(namespace.as_bytes());
        let mut base = format!("iba/{namespace_hash:016x}").into_bytes();
        base.push(b'/');

        let mut index_prefix = base.clone();
        index_prefix.extend_from_slice(b"idx/");

        let mut reverse_prefix = base.clone();
        reverse_prefix.extend_from_slice(b"rev/");

        let mut range_prefix = base.clone();
        range_prefix.extend_from_slice(b"rng/");

        let mut segment_sequence_key = base;
        segment_sequence_key.extend_from_slice(b"next_segment_id");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Binary,
            false,
        )]));

        Self {
            namespace: namespace.clone(),
            segment_store: ArrowSegmentStore::new(
                table.clone(),
                format!("iba/{namespace_hash:016x}"),
            ),
            table,
            schema,
            index_prefix,
            reverse_prefix,
            range_prefix,
            segment_sequence_key,
            reverse_enabled,
            range_enabled,
            segment_sequence_lock: AsyncMutex::new(()),
            lookup_cache_shards: make_mutex_shards(LOOKUP_CACHE_SHARDS),
            segment_cache_shards: make_mutex_shards(SEGMENT_CACHE_SHARDS),
            hot_key_compaction_threshold,
            _marker: PhantomData,
        }
    }
}

mod maintenance;
mod operations;

fn make_mutex_shards<T: Default>(shard_count: usize) -> Vec<Mutex<T>> {
    (0..shard_count).map(|_| Mutex::new(T::default())).collect()
}

fn shard_for_bytes(bytes: &[u8], shard_count: usize) -> usize {
    if shard_count == 0 {
        return 0;
    }
    (hash_bytes(bytes) as usize) % shard_count
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = ahash::AHasher::default();
    hasher.write(bytes);
    hasher.finish()
}

fn stable_namespace_hash(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn bytes_prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut next = prefix.to_vec();
    while let Some(byte) = next.last_mut() {
        if *byte != 0xFF {
            *byte += 1;
            return Some(next);
        }
        next.pop();
    }
    None
}

fn encode_index_postings(postings: &[(u32, i64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + postings.len() * (4 + 8));
    out.extend_from_slice(&(postings.len() as u32).to_be_bytes());
    for (row_index, delta) in postings {
        out.extend_from_slice(&row_index.to_be_bytes());
        out.extend_from_slice(&delta.to_be_bytes());
    }
    out
}

fn decode_index_postings(bytes: &[u8]) -> Result<Vec<(u32, i64)>> {
    let mut cursor = 0;
    let count = read_u32(bytes, &mut cursor).context("decode index postings count")? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let row_index = read_u32(bytes, &mut cursor).context("decode index posting row index")?;
        let delta = read_i64(bytes, &mut cursor).context("decode index posting delta")?;
        out.push((row_index, delta));
    }
    if cursor != bytes.len() {
        return Err(anyhow!("index postings payload has trailing bytes"));
    }
    Ok(out)
}

fn encode_reverse_postings(postings: &[(Vec<u8>, i64)]) -> Result<Vec<u8>> {
    let mut capacity: usize = 4;
    for (key_bytes, _) in postings {
        capacity = capacity
            .checked_add(4)
            .and_then(|v| v.checked_add(key_bytes.len()))
            .and_then(|v| v.checked_add(8))
            .ok_or_else(|| anyhow!("reverse postings size overflow"))?;
    }

    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(&(postings.len() as u32).to_be_bytes());
    for (key_bytes, delta) in postings {
        out.extend_from_slice(&encode_len(key_bytes.len())?);
        out.extend_from_slice(key_bytes);
        out.extend_from_slice(&delta.to_be_bytes());
    }
    Ok(out)
}

fn decode_reverse_postings(bytes: &[u8]) -> Result<Vec<(Vec<u8>, i64)>> {
    let mut cursor = 0;
    let count = read_u32(bytes, &mut cursor).context("decode reverse postings count")? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let key_len = read_len(bytes, &mut cursor).context("decode reverse posting key length")?;
        let key_end = cursor
            .checked_add(key_len)
            .ok_or_else(|| anyhow!("reverse posting key length overflow"))?;
        let key_bytes = bytes
            .get(cursor..key_end)
            .ok_or_else(|| anyhow!("reverse posting key truncated"))?
            .to_vec();
        cursor = key_end;
        let delta = read_i64(bytes, &mut cursor).context("decode reverse posting delta")?;
        out.push((key_bytes, delta));
    }
    if cursor != bytes.len() {
        return Err(anyhow!("reverse postings payload has trailing bytes"));
    }
    Ok(out)
}

fn encode_len(len: usize) -> Result<[u8; 4]> {
    let len = u32::try_from(len).map_err(|_| anyhow!("Arrow-index component too large"))?;
    Ok(len.to_be_bytes())
}

fn read_len(bytes: &[u8], cursor: &mut usize) -> Result<usize> {
    Ok(read_u32_at(bytes, cursor, "Arrow-index length")? as usize)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    read_u32_at(bytes, cursor, "Arrow-index u32")
}

fn read_u32_at(bytes: &[u8], cursor: &mut usize, label: &str) -> Result<u32> {
    Ok(u32::from_be_bytes(read_exact_at(bytes, cursor, label)?))
}

fn read_u64(bytes: &[u8], cursor: &mut usize, label: &str) -> Result<u64> {
    Ok(u64::from_be_bytes(read_exact_at(bytes, cursor, label)?))
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64> {
    Ok(i64::from_be_bytes(read_exact_at(
        bytes,
        cursor,
        "Arrow-index i64",
    )?))
}

fn read_exact_at<const N: usize>(bytes: &[u8], cursor: &mut usize, label: &str) -> Result<[u8; N]> {
    let end = cursor
        .checked_add(N)
        .ok_or_else(|| anyhow!("{label} overflow"))?;
    let chunk = bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow!("{label} truncated"))?;
    *cursor = end;
    chunk
        .try_into()
        .map_err(|_| anyhow!("{label} expected {N} bytes"))
}

fn decode_u64_payload(bytes: &[u8]) -> Result<u64> {
    let mut cursor = 0;
    let value = read_u64(bytes, &mut cursor, "Arrow-index u64 payload")?;
    if cursor != bytes.len() {
        return Err(anyhow!("Arrow-index u64 payload has trailing bytes"));
    }
    Ok(value)
}

fn segment_id_from_key_suffix(key: &[u8]) -> Result<u64> {
    if key.len() < 8 {
        return Err(anyhow!("Arrow-index key missing segment id suffix"));
    }
    let segment_bytes = key
        .get(key.len() - 8..)
        .ok_or_else(|| anyhow!("Arrow-index key missing segment id suffix"))?;
    segment_bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| anyhow!("Arrow-index segment id suffix expected 8 bytes"))
}

#[cfg(test)]
mod tests;
