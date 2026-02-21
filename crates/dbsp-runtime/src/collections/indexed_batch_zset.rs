use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use ahash::RandomState;
use anyhow::{Context, Result, anyhow};
use hashbrown::{HashMap as FastHashMap, HashSet as FastHashSet};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;
use tokio::sync::Mutex as AsyncMutex;

use crate::storage::KeyValueTable;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator, decode, encode};
use crate::storage::keyspace::{namespace_prefix, prefix};
use crate::storage::manifest::{IndexManifest, ManifestStatistics, ManifestStore};

/// Encode keys into an order-preserving byte representation for range scans.
pub trait RangeKey {
    fn encode_range_key(&self) -> Vec<u8>;
    fn encoded_len(encoded: &[u8]) -> Result<usize>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct OrderedBytes(pub Vec<u8>);

impl OrderedBytes {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for OrderedBytes {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<&[u8]> for OrderedBytes {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

impl From<&str> for OrderedBytes {
    fn from(value: &str) -> Self {
        Self(value.as_bytes().to_vec())
    }
}

fn encode_memcomparable(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 2);
    for &b in bytes {
        if b == 0 {
            out.push(0);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    out.push(0);
    out.push(0);
    out
}

fn memcomparable_len(encoded: &[u8]) -> Result<usize> {
    let mut idx = 0;
    while idx + 1 < encoded.len() {
        if encoded[idx] != 0 {
            idx += 1;
            continue;
        }
        match encoded[idx + 1] {
            0xFF => {
                idx += 2;
            }
            0x00 => {
                return Ok(idx + 2);
            }
            other => {
                return Err(anyhow!("invalid memcomparable escape byte: {other:#04x}"));
            }
        }
    }
    Err(anyhow!("truncated memcomparable encoding"))
}

impl RangeKey for i64 {
    fn encode_range_key(&self) -> Vec<u8> {
        let shifted = (*self as u64) ^ 0x8000_0000_0000_0000;
        shifted.to_be_bytes().to_vec()
    }

    fn encoded_len(_encoded: &[u8]) -> Result<usize> {
        Ok(8)
    }
}

impl RangeKey for u64 {
    fn encode_range_key(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }

    fn encoded_len(_encoded: &[u8]) -> Result<usize> {
        Ok(8)
    }
}

impl RangeKey for i32 {
    fn encode_range_key(&self) -> Vec<u8> {
        let shifted = (*self as u32) ^ 0x8000_0000;
        shifted.to_be_bytes().to_vec()
    }

    fn encoded_len(_encoded: &[u8]) -> Result<usize> {
        Ok(4)
    }
}

impl RangeKey for u32 {
    fn encode_range_key(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }

    fn encoded_len(_encoded: &[u8]) -> Result<usize> {
        Ok(4)
    }
}

impl RangeKey for OrderedBytes {
    fn encode_range_key(&self) -> Vec<u8> {
        encode_memcomparable(self.as_bytes())
    }

    fn encoded_len(encoded: &[u8]) -> Result<usize> {
        memcomparable_len(encoded)
    }
}

impl RangeKey for String {
    fn encode_range_key(&self) -> Vec<u8> {
        encode_memcomparable(self.as_bytes())
    }

    fn encoded_len(encoded: &[u8]) -> Result<usize> {
        memcomparable_len(encoded)
    }
}

#[derive(Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct OverlayEntry<V> {
    value: V,
    delta: i64,
}

#[derive(Debug)]
struct L0Segment {
    bytes: Vec<u8>,
    blob_start: usize,
    offsets: Vec<u32>,
}

impl L0Segment {
    fn value_bytes(&self, row_index: u32) -> Result<&[u8]> {
        let idx = row_index as usize;
        if idx + 1 >= self.offsets.len() {
            return Err(anyhow!(
                "row index {row_index} out of bounds for segment (len={})",
                self.offsets.len().saturating_sub(1)
            ));
        }
        let start = self.blob_start + self.offsets[idx] as usize;
        let end = self.blob_start + self.offsets[idx + 1] as usize;
        self.bytes
            .get(start..end)
            .ok_or_else(|| anyhow!("segment row slice out of bounds"))
    }
}

const L0_ENTRY_V2: u8 = 2;
const L0_ENTRY_V3_ID: u8 = 3;
const L0_ENTRY_V4_SEGMENT_REF: u8 = 4;
const L0_SEGMENT_V1: u8 = 1;
const INDEXED_STATE_SHARDS: usize = 64;
const DEFAULT_KEY_LOCAL_COMPACTION_THRESHOLD: usize = 0;
const VALUE_ID_CACHE_CAPACITY_PER_SHARD: usize = 4096;
const VALUE_DATA_CACHE_CAPACITY_PER_SHARD: usize = 4096;
const VALUE_DECODE_CACHE_CAPACITY_PER_SHARD: usize = 4096;
const L0_SEGMENT_CACHE_CAPACITY_PER_SHARD: usize = 256;
const ADAPTIVE_COALESCE_THRESHOLD: usize = 512;
const ADAPTIVE_COALESCE_SAMPLE: usize = 256;

type FastMap<K, V> = FastHashMap<K, V, RandomState>;
type FastSet<T> = FastHashSet<T, RandomState>;

#[derive(Default)]
struct KeyLookupState {
    aggregate_by_value_bytes: HashMap<Vec<u8>, i64>,
    last_l0_sequence: u64,
    initialized_from_l1: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ApplyDeltaMetrics {
    pub input_records: usize,
    pub non_zero_input_records: usize,
    pub coalesced_records: usize,
    pub persisted_records: usize,
}

pub struct IndexedBatchZSet<K, V>
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
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    l0_prefix: Vec<u8>,
    l0_segment_prefix: Vec<u8>,
    l0_segment_refcount_prefix: Vec<u8>,
    l1_prefix: Vec<u8>,
    l1_id_prefix: Vec<u8>,
    l0_active_prefix: Vec<u8>,
    compaction_watermark_prefix: Vec<u8>,
    l0_seq_key: Vec<u8>,
    value_id_prefix: Vec<u8>,
    value_data_prefix: Vec<u8>,
    value_seq_key: Vec<u8>,
    reverse_enabled: bool,
    range_enabled: bool,
    key_lookup_state_shards: Vec<Mutex<HashMap<Vec<u8>, Arc<Mutex<KeyLookupState>>>>>,
    known_active_key_shards: Vec<Mutex<HashSet<Vec<u8>>>>,
    value_id_cache_shards: Vec<Mutex<HashMap<Vec<u8>, u64>>>,
    value_data_cache_shards: Vec<Mutex<HashMap<u64, Vec<u8>>>>,
    value_decode_cache_shards: Vec<Mutex<HashMap<Vec<u8>, V>>>,
    l0_segment_cache_shards: Vec<Mutex<HashMap<u64, Arc<L0Segment>>>>,
    key_local_compaction_threshold: usize,
    value_intern_lock: AsyncMutex<()>,
    l0_segment_ref_lock: AsyncMutex<()>,
    marker: PhantomData<(K, V)>,
}

impl<K, V> IndexedBatchZSet<K, V>
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
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub const fn engine_name() -> &'static str {
        "indexed_batch"
    }

    pub fn engine_kind(&self) -> &'static str {
        Self::engine_name()
    }

    pub fn new(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        Self::with_mode(
            table,
            namespace,
            false,
            false,
            DEFAULT_KEY_LOCAL_COMPACTION_THRESHOLD,
        )
    }

    pub fn with_reverse_index(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        Self::with_mode(
            table,
            namespace,
            true,
            false,
            DEFAULT_KEY_LOCAL_COMPACTION_THRESHOLD,
        )
    }

    pub fn with_range_index(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        Self::with_mode(
            table,
            namespace,
            false,
            true,
            DEFAULT_KEY_LOCAL_COMPACTION_THRESHOLD,
        )
    }

    pub fn with_hot_key_compaction_threshold(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        key_local_compaction_threshold: usize,
    ) -> Self {
        Self::with_mode(
            table,
            namespace,
            false,
            false,
            key_local_compaction_threshold,
        )
    }

    fn with_mode(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        reverse_enabled: bool,
        range_enabled: bool,
        key_local_compaction_threshold: usize,
    ) -> Self {
        let namespace = namespace.into();
        let base = namespace_prefix(prefix::INDEX, &namespace);

        let mut l0_prefix = base.clone();
        l0_prefix.extend_from_slice(b"l0/");

        let mut l0_segment_prefix = base.clone();
        l0_segment_prefix.extend_from_slice(b"l0_seg/");

        let mut l0_segment_refcount_prefix = base.clone();
        l0_segment_refcount_prefix.extend_from_slice(b"l0_seg_ref/");

        let mut l1_prefix = base.clone();
        l1_prefix.extend_from_slice(b"l1/");

        let mut l1_id_prefix = base.clone();
        l1_id_prefix.extend_from_slice(b"l1_id/");

        let mut l0_active_prefix = base.clone();
        l0_active_prefix.extend_from_slice(b"l0_active/");

        let mut compaction_watermark_prefix = base.clone();
        compaction_watermark_prefix.extend_from_slice(b"compact_wm/");

        let mut value_id_prefix = base.clone();
        value_id_prefix.extend_from_slice(b"val_id/");

        let mut value_data_prefix = base.clone();
        value_data_prefix.extend_from_slice(b"val_data/");

        let mut l0_seq_key = base.clone();
        l0_seq_key.extend_from_slice(b"l0_seq");

        let mut value_seq_key = base.clone();
        value_seq_key.extend_from_slice(b"val_seq");

        Self {
            table,
            namespace,
            l0_prefix,
            l0_segment_prefix,
            l0_segment_refcount_prefix,
            l1_prefix,
            l1_id_prefix,
            l0_active_prefix,
            compaction_watermark_prefix,
            l0_seq_key,
            value_id_prefix,
            value_data_prefix,
            value_seq_key,
            reverse_enabled,
            range_enabled,
            key_lookup_state_shards: make_mutex_shards(INDEXED_STATE_SHARDS),
            known_active_key_shards: make_mutex_shards(INDEXED_STATE_SHARDS),
            value_id_cache_shards: make_mutex_shards(INDEXED_STATE_SHARDS),
            value_data_cache_shards: make_mutex_shards(INDEXED_STATE_SHARDS),
            value_decode_cache_shards: make_mutex_shards(INDEXED_STATE_SHARDS),
            l0_segment_cache_shards: make_mutex_shards(INDEXED_STATE_SHARDS),
            key_local_compaction_threshold,
            value_intern_lock: AsyncMutex::new(()),
            l0_segment_ref_lock: AsyncMutex::new(()),
            marker: PhantomData,
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub async fn apply_deltas<I>(&self, deltas: I) -> Result<()>
    where
        I: IntoIterator<Item = (K, V, i64)>,
    {
        if self.range_enabled {
            return Err(anyhow!(
                "range index enabled: use apply_deltas_with_range to maintain range keys"
            ));
        }
        self.apply_deltas_internal(deltas).await.map(|_| ())
    }

    pub async fn apply_deltas_with_stats<I>(&self, deltas: I) -> Result<ApplyDeltaMetrics>
    where
        I: IntoIterator<Item = (K, V, i64)>,
    {
        if self.range_enabled {
            return Err(anyhow!(
                "range index enabled: use apply_deltas_with_range to maintain range keys"
            ));
        }
        self.apply_deltas_internal(deltas).await
    }

    pub async fn apply_deltas_with_range<I>(&self, deltas: I) -> Result<()>
    where
        K: RangeKey,
        I: IntoIterator<Item = (K, V, i64)>,
    {
        if !self.range_enabled {
            return Err(anyhow!("range index not enabled"));
        }
        self.apply_deltas_internal(deltas).await.map(|_| ())
    }

    pub async fn apply_deltas_with_range_stats<I>(&self, deltas: I) -> Result<ApplyDeltaMetrics>
    where
        K: RangeKey,
        I: IntoIterator<Item = (K, V, i64)>,
    {
        if !self.range_enabled {
            return Err(anyhow!("range index not enabled"));
        }
        self.apply_deltas_internal(deltas).await
    }

    async fn apply_deltas_internal<I>(&self, deltas: I) -> Result<ApplyDeltaMetrics>
    where
        I: IntoIterator<Item = (K, V, i64)>,
    {
        let mut metrics = ApplyDeltaMetrics::default();

        let mut non_zero_updates: Vec<(K, V, i64)> = Vec::new();
        for (key, value, delta) in deltas {
            metrics.input_records = metrics.input_records.saturating_add(1);
            if delta == 0 {
                continue;
            }
            metrics.non_zero_input_records = metrics.non_zero_input_records.saturating_add(1);
            non_zero_updates.push((key, value, delta));
        }
        if non_zero_updates.is_empty() {
            return Ok(metrics);
        }

        let use_coalescing = Self::should_use_coalescing(&non_zero_updates);
        let use_l0_segment_refs = !use_coalescing;
        let mut coalesced: FastMap<(K, V), i64> = FastMap::default();
        if use_coalescing {
            coalesced.reserve(non_zero_updates.len());
            for (key, value, delta) in non_zero_updates.drain(..) {
                *coalesced.entry((key, value)).or_insert(0) += delta;
            }
            metrics.coalesced_records = coalesced.len();
        } else {
            // No coalescing: treat each input as an independent L0 record.
            metrics.coalesced_records = non_zero_updates.len();
        }

        let mut next_seq = self.next_sequence().await?;
        let segment_id = use_l0_segment_refs.then_some(next_seq);
        let mut batch = WriteBatch::new();
        let mut wrote = false;

        let mut active_keys: FastSet<Vec<u8>> = FastSet::default();
        let mut touched_key_append_counts: FastMap<Vec<u8>, usize> = FastMap::default();
        let mut lookup_cache_updates_bytes: FastMap<Vec<u8>, Vec<(Vec<u8>, i64, u64)>> =
            FastMap::default();
        let mut lookup_cache_updates_rowrefs: FastMap<Vec<u8>, Vec<(u32, i64, u64)>> =
            FastMap::default();
        let mut segment_values: Vec<Vec<u8>> = Vec::new();
        if segment_id.is_some() {
            segment_values = Vec::with_capacity(metrics.coalesced_records);
        }

        if use_coalescing {
            for ((key, value), delta) in coalesced {
                if delta == 0 {
                    continue;
                }
                self.write_l0_record_with_optional_segment(
                    &mut batch,
                    &mut next_seq,
                    segment_id,
                    &mut segment_values,
                    &mut wrote,
                    &mut metrics,
                    &mut active_keys,
                    &mut lookup_cache_updates_bytes,
                    &mut lookup_cache_updates_rowrefs,
                    &mut touched_key_append_counts,
                    key,
                    value,
                    delta,
                )?;
            }
        } else {
            for (key, value, delta) in non_zero_updates {
                self.write_l0_record_with_optional_segment(
                    &mut batch,
                    &mut next_seq,
                    segment_id,
                    &mut segment_values,
                    &mut wrote,
                    &mut metrics,
                    &mut active_keys,
                    &mut lookup_cache_updates_bytes,
                    &mut lookup_cache_updates_rowrefs,
                    &mut touched_key_append_counts,
                    key,
                    value,
                    delta,
                )?;
            }
        }

        if let Some(segment_id) = segment_id {
            if wrote {
                let payload = encode_l0_segment_v1(&segment_values)?;
                batch.put(self.l0_segment_key(segment_id), payload);
                batch.put(
                    self.l0_segment_refcount_key(segment_id),
                    (segment_values.len() as u64).to_be_bytes().to_vec(),
                );
            }
        }

        if wrote {
            let keys_to_mark = self.mark_new_active_keys(active_keys.into_iter())?;
            for key_bytes in keys_to_mark {
                batch.put(
                    self.active_key(&key_bytes)
                        .context("build active-key marker key")?,
                    vec![1],
                );
            }
            batch.put(self.l0_seq_key.clone(), next_seq.to_be_bytes().to_vec());
            self.table
                .write_batch(batch)
                .await
                .context("persist batch-index L0 deltas")?;

            if segment_id.is_some() {
                self.apply_lookup_cache_updates_rowrefs(
                    &lookup_cache_updates_rowrefs,
                    &mut segment_values,
                )?;
            } else {
                self.apply_lookup_cache_updates(&lookup_cache_updates_bytes)?;
            }
            self.maybe_compact_hot_keys(touched_key_append_counts.into_iter())
                .await?;
        }

        Ok(metrics)
    }

    fn should_use_coalescing(updates: &[(K, V, i64)]) -> bool {
        if updates.len() < ADAPTIVE_COALESCE_THRESHOLD {
            return true;
        }
        let sample = updates.len().min(ADAPTIVE_COALESCE_SAMPLE);
        let mut fingerprints = Vec::with_capacity(sample);
        for (key, value, _) in updates.iter().take(sample) {
            let mut hasher = DefaultHasher::new();
            (key, value).hash(&mut hasher);
            fingerprints.push(hasher.finish());
        }
        fingerprints.sort_unstable();
        for window in fingerprints.windows(2) {
            if window[0] == window[1] {
                return true;
            }
        }
        false
    }

    fn write_l0_record_with_optional_segment(
        &self,
        batch: &mut WriteBatch,
        next_seq: &mut u64,
        segment_id: Option<u64>,
        segment_values: &mut Vec<Vec<u8>>,
        wrote: &mut bool,
        metrics: &mut ApplyDeltaMetrics,
        active_keys: &mut FastSet<Vec<u8>>,
        lookup_cache_updates_bytes: &mut FastMap<Vec<u8>, Vec<(Vec<u8>, i64, u64)>>,
        lookup_cache_updates_rowrefs: &mut FastMap<Vec<u8>, Vec<(u32, i64, u64)>>,
        touched_key_append_counts: &mut FastMap<Vec<u8>, usize>,
        key: K,
        value: V,
        delta: i64,
    ) -> Result<()> {
        if delta == 0 {
            return Ok(());
        }

        let key_bytes = encode(&key).context("encode batch-index key")?;
        active_keys.insert(key_bytes.clone());
        let assigned_sequence = *next_seq;

        let entry_bytes = if let Some(segment_id) = segment_id {
            let value_bytes = encode(&value).context("encode batch-index value")?;
            let row_index = u32::try_from(segment_values.len())
                .map_err(|_| anyhow!("segment row index overflow"))?;
            segment_values.push(value_bytes);
            lookup_cache_updates_rowrefs
                .entry(key_bytes.clone())
                .or_default()
                .push((row_index, delta, assigned_sequence));
            encode_l0_payload_v4_segment_ref(segment_id, row_index, delta)
        } else {
            let value_bytes = encode(&value).context("encode batch-index value")?;
            let entry_bytes = encode_l0_payload_v2(&value_bytes, delta);
            lookup_cache_updates_bytes
                .entry(key_bytes.clone())
                .or_default()
                .push((value_bytes, delta, assigned_sequence));
            entry_bytes
        };

        let l0_key = self
            .l0_overlay_key(&key_bytes, assigned_sequence)
            .context("build batch-index L0 key")?;
        batch.put(l0_key, entry_bytes);
        *next_seq = next_seq.saturating_add(1);
        *wrote = true;
        metrics.persisted_records = metrics.persisted_records.saturating_add(1);
        *touched_key_append_counts.entry(key_bytes).or_insert(0) += 1;
        Ok(())
    }

    fn apply_lookup_cache_updates_rowrefs(
        &self,
        updates: &FastMap<Vec<u8>, Vec<(u32, i64, u64)>>,
        segment_values: &mut Vec<Vec<u8>>,
    ) -> Result<()> {
        for (key_bytes, key_updates) in updates {
            let shard = self.shard_for_bytes(key_bytes);
            let state_handle = {
                let guard = self.key_lookup_state_shards[shard]
                    .lock()
                    .map_err(|_| anyhow!("batch-index key lookup shard poisoned"))?;
                guard.get(key_bytes).cloned()
            };
            let Some(state) = state_handle else {
                continue;
            };
            let mut state_guard = state
                .lock()
                .map_err(|_| anyhow!("batch-index key lookup state poisoned"))?;
            if !state_guard.initialized_from_l1 {
                continue;
            }
            for (row_index, delta, sequence) in key_updates {
                let idx = *row_index as usize;
                let value_bytes = segment_values
                    .get_mut(idx)
                    .ok_or_else(|| anyhow!("segment row index {row_index} out of bounds"))?;
                let value_bytes = std::mem::take(value_bytes);
                if value_bytes.is_empty() {
                    return Err(anyhow!("segment row {row_index} already consumed"));
                }

                let next = state_guard
                    .aggregate_by_value_bytes
                    .get(&value_bytes)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(*delta);
                if next == 0 {
                    state_guard.aggregate_by_value_bytes.remove(&value_bytes);
                } else {
                    state_guard
                        .aggregate_by_value_bytes
                        .insert(value_bytes, next);
                }
                state_guard.last_l0_sequence = state_guard.last_l0_sequence.max(*sequence);
            }
        }
        Ok(())
    }

    pub async fn keys_for_value(&self, value: &V) -> Result<Vec<(K, i64)>> {
        if !self.reverse_enabled {
            return Err(anyhow!("reverse index not enabled"));
        }
        let entries = self.entries().await?;
        Ok(entries
            .into_iter()
            .filter_map(|(key, entry_value, weight)| {
                (entry_value == *value).then_some((key, weight))
            })
            .collect())
    }

    pub async fn values_for_key_range(&self, lower: &K, upper: &K) -> Result<Vec<(K, V, i64)>>
    where
        K: RangeKey,
    {
        if !self.range_enabled {
            return Err(anyhow!("range index not enabled"));
        }
        let lower_bytes = lower.encode_range_key();
        let upper_bytes = upper.encode_range_key();
        if lower_bytes >= upper_bytes {
            return Ok(Vec::new());
        }

        let entries = self.entries().await?;
        Ok(entries
            .into_iter()
            .filter(|(key, _, _)| {
                let encoded = key.encode_range_key();
                encoded >= lower_bytes && encoded < upper_bytes
            })
            .collect())
    }

    pub async fn values_for_key(&self, key: &K) -> Result<Vec<(V, i64)>> {
        let key_bytes = encode(key).context("encode batch-index lookup key")?;
        let state = self.lookup_state_for_key(&key_bytes)?;
        self.seed_lookup_state_if_cold(&key_bytes, &state).await?;

        let mut scan_from_sequence = {
            let guard = state
                .lock()
                .map_err(|_| anyhow!("batch-index key lookup state poisoned"))?;
            guard.last_l0_sequence.saturating_add(1)
        };
        if scan_from_sequence == 0 {
            scan_from_sequence = 1;
        }

        let l0_entries = self
            .table
            .scan_range(
                self.l0_range_for_key_from_sequence(&key_bytes, scan_from_sequence)
                    .context("build batch-index L0 range for key lookup")?,
                &ScanOptions::default(),
            )
            .await
            .context("scan batch-index L0 entries")?;
        let mut updates = Vec::new();
        for (entry_key, entry_bytes) in l0_entries {
            let sequence = self
                .sequence_from_l0_key(&entry_key)
                .context("decode batch-index L0 sequence for key lookup")?;
            let (decoded_value, delta) = decode_l0_payload::<V>(&entry_bytes)?;
            let value_bytes = match decoded_value {
                DecodedL0Value::Id(id) => self
                    .value_bytes_for_id(id)
                    .await
                    .with_context(|| format!("load value bytes for id {id} during lookup"))?,
                DecodedL0Value::SegmentRef {
                    segment_id,
                    row_index,
                } => self
                    .value_bytes_for_segment_ref(segment_id, row_index)
                    .await
                    .with_context(|| {
                        format!("load value bytes for segment {segment_id} row {row_index} during lookup")
                    })?,
                DecodedL0Value::Encoded(bytes) => bytes,
                DecodedL0Value::Decoded(value) => {
                    encode(&value).context("encode legacy L0 value during lookup")?
                }
            };
            updates.push((sequence, value_bytes, delta));
        }

        let value_weight_pairs: Vec<(Vec<u8>, i64)> = {
            let mut guard = state
                .lock()
                .map_err(|_| anyhow!("batch-index key lookup state poisoned"))?;
            let mut max_seen_sequence = guard.last_l0_sequence;
            for (sequence, value_bytes, delta) in updates {
                if sequence <= guard.last_l0_sequence {
                    continue;
                }
                let next = guard
                    .aggregate_by_value_bytes
                    .get(&value_bytes)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(delta);
                if next == 0 {
                    guard.aggregate_by_value_bytes.remove(&value_bytes);
                } else {
                    guard.aggregate_by_value_bytes.insert(value_bytes, next);
                }
                max_seen_sequence = max_seen_sequence.max(sequence);
            }
            guard.last_l0_sequence = max_seen_sequence;
            guard
                .aggregate_by_value_bytes
                .iter()
                .map(|(value_bytes, weight)| (value_bytes.clone(), *weight))
                .collect()
        };

        let mut output = Vec::with_capacity(value_weight_pairs.len());
        for (value_bytes, weight) in value_weight_pairs {
            if weight == 0 {
                continue;
            }
            let value = self.decode_value_bytes_cached(&value_bytes)?;
            output.push((value, weight));
        }
        Ok(output)
    }

    async fn seed_lookup_state_if_cold(
        &self,
        key_bytes: &[u8],
        state: &Arc<Mutex<KeyLookupState>>,
    ) -> Result<()> {
        let needs_seed = {
            let guard = state
                .lock()
                .map_err(|_| anyhow!("batch-index key lookup state poisoned"))?;
            !guard.initialized_from_l1
        };
        if !needs_seed {
            return Ok(());
        }

        let seeded = self.seed_lookup_state_from_l1(key_bytes).await?;
        let mut guard = state
            .lock()
            .map_err(|_| anyhow!("batch-index key lookup state poisoned"))?;
        if guard.initialized_from_l1 {
            return Ok(());
        }
        guard.aggregate_by_value_bytes = seeded;
        guard.last_l0_sequence = 0;
        guard.initialized_from_l1 = true;
        Ok(())
    }

    async fn seed_lookup_state_from_l1(&self, key_bytes: &[u8]) -> Result<HashMap<Vec<u8>, i64>> {
        let mut aggregate_by_value_bytes = HashMap::new();

        let l1_id_entries = self
            .table
            .scan_prefix(
                &self
                    .l1_id_prefix_for_key(key_bytes)
                    .context("build L1-id key prefix")?,
                &ScanOptions::default(),
            )
            .await
            .context("scan L1-id entries for lookup seed")?;
        for (entry_key, entry_value) in l1_id_entries {
            let value_id = self
                .value_id_from_l1_id_key(&entry_key)
                .context("decode value id from L1-id key")?;
            let value_bytes = self
                .value_bytes_for_id(value_id)
                .await
                .with_context(|| format!("load value bytes for id {value_id} during seed"))?;
            let weight = decode_weight(&entry_value)?;
            *aggregate_by_value_bytes.entry(value_bytes).or_insert(0) += weight;
        }

        let l1_legacy_entries = self
            .table
            .scan_prefix(
                &self
                    .l1_prefix_for_key(key_bytes)
                    .context("build L1 key prefix")?,
                &ScanOptions::default(),
            )
            .await
            .context("scan legacy L1 entries for lookup seed")?;
        for (entry_key, entry_value) in l1_legacy_entries {
            let value_bytes = self
                .value_bytes_from_l1_key(&entry_key)
                .context("decode legacy L1 value bytes")?;
            let weight = decode_weight(&entry_value)?;
            *aggregate_by_value_bytes
                .entry(value_bytes.to_vec())
                .or_insert(0) += weight;
        }

        aggregate_by_value_bytes.retain(|_, weight| *weight != 0);
        Ok(aggregate_by_value_bytes)
    }

    fn lookup_state_for_key(&self, key_bytes: &[u8]) -> Result<Arc<Mutex<KeyLookupState>>> {
        let shard = self.shard_for_bytes(key_bytes);
        let mut guard = self.key_lookup_state_shards[shard]
            .lock()
            .map_err(|_| anyhow!("batch-index key lookup shard poisoned"))?;
        Ok(Arc::clone(guard.entry(key_bytes.to_vec()).or_insert_with(
            || Arc::new(Mutex::new(KeyLookupState::default())),
        )))
    }

    fn apply_lookup_cache_updates(
        &self,
        updates: &FastMap<Vec<u8>, Vec<(Vec<u8>, i64, u64)>>,
    ) -> Result<()> {
        for (key_bytes, key_updates) in updates {
            let shard = self.shard_for_bytes(key_bytes);
            let state_handle = {
                let guard = self.key_lookup_state_shards[shard]
                    .lock()
                    .map_err(|_| anyhow!("batch-index key lookup shard poisoned"))?;
                guard.get(key_bytes).cloned()
            };
            let Some(state) = state_handle else {
                continue;
            };
            let mut state_guard = state
                .lock()
                .map_err(|_| anyhow!("batch-index key lookup state poisoned"))?;
            if !state_guard.initialized_from_l1 {
                continue;
            }
            for (value_bytes, delta, sequence) in key_updates {
                let next = state_guard
                    .aggregate_by_value_bytes
                    .get(value_bytes)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(*delta);
                if next == 0 {
                    state_guard.aggregate_by_value_bytes.remove(value_bytes);
                } else {
                    state_guard
                        .aggregate_by_value_bytes
                        .insert(value_bytes.clone(), next);
                }
                state_guard.last_l0_sequence = state_guard.last_l0_sequence.max(*sequence);
            }
        }
        Ok(())
    }

    fn mark_new_active_keys<I>(&self, keys: I) -> Result<Vec<Vec<u8>>>
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        let mut pending = Vec::new();
        for key_bytes in keys {
            let shard = self.shard_for_bytes(&key_bytes);
            let mut guard = self.known_active_key_shards[shard]
                .lock()
                .map_err(|_| anyhow!("batch-index active-key shard poisoned"))?;
            if guard.insert(key_bytes.clone()) {
                pending.push(key_bytes);
            }
        }
        Ok(pending)
    }

    async fn maybe_compact_hot_keys<I>(&self, touched_key_append_counts: I) -> Result<()>
    where
        I: IntoIterator<Item = (Vec<u8>, usize)>,
    {
        if self.key_local_compaction_threshold == 0 {
            return Ok(());
        }
        for (key_bytes, appended) in touched_key_append_counts {
            if appended == 0 {
                continue;
            }
            let l0_count = self
                .table
                .scan_prefix(
                    &self
                        .l0_prefix_for_key(&key_bytes)
                        .context("build key-local L0 prefix for adaptive compaction")?,
                    &ScanOptions::default(),
                )
                .await
                .context("scan key-local L0 for adaptive compaction threshold")?
                .len();
            if l0_count < self.key_local_compaction_threshold {
                continue;
            }
            self.compact_single_key_l0_to_l1(&key_bytes)
                .await
                .context("compact hot key after apply_deltas")?;
        }
        Ok(())
    }

    fn reset_lookup_state_for_key(&self, key_bytes: &[u8]) -> Result<()> {
        let shard = self.shard_for_bytes(key_bytes);
        self.key_lookup_state_shards[shard]
            .lock()
            .map_err(|_| anyhow!("batch-index key lookup shard poisoned"))?
            .remove(key_bytes);
        Ok(())
    }

    fn shard_for_bytes(&self, key_bytes: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        use std::hash::Hasher;
        hasher.write(key_bytes);
        (hasher.finish() as usize) % self.key_lookup_state_shards.len()
    }

    fn value_data_cache_shard(&self, value_id: u64) -> usize {
        (value_id as usize) % self.value_data_cache_shards.len()
    }

    fn value_id_cache_shard(&self, value_bytes: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        use std::hash::Hasher;
        hasher.write(value_bytes);
        (hasher.finish() as usize) % self.value_id_cache_shards.len()
    }

    fn l0_segment_cache_shard(&self, segment_id: u64) -> usize {
        (segment_id as usize) % self.l0_segment_cache_shards.len()
    }

    fn cached_value_id_for_bytes(&self, value_bytes: &[u8]) -> Result<Option<u64>> {
        let shard = self.value_id_cache_shard(value_bytes);
        let guard = self.value_id_cache_shards[shard]
            .lock()
            .map_err(|_| anyhow!("batch-index value-id cache shard poisoned"))?;
        Ok(guard.get(value_bytes).copied())
    }

    fn insert_value_id_cache(&self, value_bytes: &[u8], value_id: u64) -> Result<()> {
        let shard = self.value_id_cache_shard(value_bytes);
        let mut guard = self.value_id_cache_shards[shard]
            .lock()
            .map_err(|_| anyhow!("batch-index value-id cache shard poisoned"))?;
        if guard.len() >= VALUE_ID_CACHE_CAPACITY_PER_SHARD && !guard.contains_key(value_bytes) {
            if let Some(evict_key) = guard.keys().next().cloned() {
                guard.remove(&evict_key);
            }
        }
        guard.insert(value_bytes.to_vec(), value_id);
        Ok(())
    }

    fn cached_value_bytes_for_id(&self, value_id: u64) -> Result<Option<Vec<u8>>> {
        let shard = self.value_data_cache_shard(value_id);
        let guard = self.value_data_cache_shards[shard]
            .lock()
            .map_err(|_| anyhow!("batch-index value-data cache shard poisoned"))?;
        Ok(guard.get(&value_id).cloned())
    }

    fn insert_value_data_cache(&self, value_id: u64, value_bytes: &[u8]) -> Result<()> {
        let shard = self.value_data_cache_shard(value_id);
        let mut guard = self.value_data_cache_shards[shard]
            .lock()
            .map_err(|_| anyhow!("batch-index value-data cache shard poisoned"))?;
        if guard.len() >= VALUE_DATA_CACHE_CAPACITY_PER_SHARD && !guard.contains_key(&value_id) {
            if let Some(evict_key) = guard.keys().next().copied() {
                guard.remove(&evict_key);
            }
        }
        guard.insert(value_id, value_bytes.to_vec());
        Ok(())
    }

    fn cached_decoded_value_for_bytes(&self, value_bytes: &[u8]) -> Result<Option<V>> {
        let shard = self.value_id_cache_shard(value_bytes);
        let guard = self.value_decode_cache_shards[shard]
            .lock()
            .map_err(|_| anyhow!("batch-index value decode cache shard poisoned"))?;
        Ok(guard.get(value_bytes).cloned())
    }

    fn insert_decoded_value_cache(&self, value_bytes: &[u8], value: V) -> Result<()> {
        let shard = self.value_id_cache_shard(value_bytes);
        let mut guard = self.value_decode_cache_shards[shard]
            .lock()
            .map_err(|_| anyhow!("batch-index value decode cache shard poisoned"))?;
        if guard.len() >= VALUE_DECODE_CACHE_CAPACITY_PER_SHARD && !guard.contains_key(value_bytes)
        {
            if let Some(evict_key) = guard.keys().next().cloned() {
                guard.remove(&evict_key);
            }
        }
        guard.insert(value_bytes.to_vec(), value);
        Ok(())
    }

    fn decode_value_bytes_cached(&self, value_bytes: &[u8]) -> Result<V> {
        if let Some(cached) = self.cached_decoded_value_for_bytes(value_bytes)? {
            return Ok(cached);
        }
        let decoded = decode::<V>(value_bytes).context("decode value bytes from lookup state")?;
        self.insert_decoded_value_cache(value_bytes, decoded.clone())?;
        Ok(decoded)
    }

    fn cached_l0_segment_for_id(&self, segment_id: u64) -> Result<Option<Arc<L0Segment>>> {
        let shard = self.l0_segment_cache_shard(segment_id);
        let guard = self.l0_segment_cache_shards[shard]
            .lock()
            .map_err(|_| anyhow!("batch-index L0 segment cache shard poisoned"))?;
        Ok(guard.get(&segment_id).cloned())
    }

    fn insert_l0_segment_cache(&self, segment_id: u64, segment: Arc<L0Segment>) -> Result<()> {
        let shard = self.l0_segment_cache_shard(segment_id);
        let mut guard = self.l0_segment_cache_shards[shard]
            .lock()
            .map_err(|_| anyhow!("batch-index L0 segment cache shard poisoned"))?;
        if guard.len() >= L0_SEGMENT_CACHE_CAPACITY_PER_SHARD && !guard.contains_key(&segment_id) {
            if let Some(evict_key) = guard.keys().next().copied() {
                guard.remove(&evict_key);
            }
        }
        guard.insert(segment_id, segment);
        Ok(())
    }

    async fn l0_segment_for_id(&self, segment_id: u64) -> Result<Arc<L0Segment>> {
        if let Some(cached) = self.cached_l0_segment_for_id(segment_id)? {
            return Ok(cached);
        }
        let Some(bytes) = self
            .table
            .get(&self.l0_segment_key(segment_id))
            .await
            .context("read L0 segment payload")?
        else {
            return Err(anyhow!("missing L0 segment payload for id {segment_id}"));
        };
        let len = bytes.len();
        let decoded = decode_l0_segment(bytes).with_context(|| {
            format!("decode L0 segment payload for id {segment_id} (len={len})")
        })?;
        let decoded = Arc::new(decoded);
        self.insert_l0_segment_cache(segment_id, Arc::clone(&decoded))?;
        Ok(decoded)
    }

    async fn value_bytes_for_segment_ref(
        &self,
        segment_id: u64,
        row_index: u32,
    ) -> Result<Vec<u8>> {
        let segment = self
            .l0_segment_for_id(segment_id)
            .await
            .with_context(|| format!("load L0 segment {segment_id}"))?;
        Ok(segment.value_bytes(row_index)?.to_vec())
    }

    async fn value_bytes_for_id(&self, value_id: u64) -> Result<Vec<u8>> {
        if let Some(value_bytes) = self.cached_value_bytes_for_id(value_id)? {
            return Ok(value_bytes);
        }
        let Some(value_bytes) = self
            .table
            .get(&self.value_data_key(value_id))
            .await
            .context("read value bytes by id")?
        else {
            return Err(anyhow!("missing value bytes for id {value_id}"));
        };
        self.insert_value_id_cache(&value_bytes, value_id)?;
        self.insert_value_data_cache(value_id, &value_bytes)?;
        Ok(value_bytes)
    }

    async fn intern_value_bytes_batch(
        &self,
        payloads: Vec<Vec<u8>>,
    ) -> Result<HashMap<Vec<u8>, u64>> {
        if payloads.is_empty() {
            return Ok(HashMap::new());
        }

        let mut resolved: HashMap<Vec<u8>, u64> = HashMap::new();
        let mut pending = Vec::new();
        let mut seen: HashSet<Vec<u8>> = HashSet::new();

        for value_bytes in payloads {
            if !seen.insert(value_bytes.clone()) {
                continue;
            }

            if let Some(value_id) = self.cached_value_id_for_bytes(&value_bytes)? {
                resolved.insert(value_bytes, value_id);
                continue;
            }

            let lookup_key = self
                .value_id_lookup_key(&value_bytes)
                .context("build value-id lookup key for batch intern")?;
            if let Some(existing) = self
                .table
                .get(&lookup_key)
                .await
                .context("read value-id lookup entry for batch intern")?
            {
                let value_id = decode_u64_payload(&existing)?;
                self.insert_value_id_cache(&value_bytes, value_id)?;
                self.insert_value_data_cache(value_id, &value_bytes)?;
                resolved.insert(value_bytes, value_id);
            } else {
                pending.push(value_bytes);
            }
        }

        if pending.is_empty() {
            return Ok(resolved);
        }

        let _guard = self.value_intern_lock.lock().await;
        let mut missing = Vec::new();
        for value_bytes in pending {
            let lookup_key = self
                .value_id_lookup_key(&value_bytes)
                .context("rebuild value-id lookup key for batch intern under lock")?;
            if let Some(existing) = self
                .table
                .get(&lookup_key)
                .await
                .context("re-read value-id lookup entry for batch intern under lock")?
            {
                let value_id = decode_u64_payload(&existing)?;
                self.insert_value_id_cache(&value_bytes, value_id)?;
                self.insert_value_data_cache(value_id, &value_bytes)?;
                resolved.insert(value_bytes, value_id);
            } else {
                missing.push(value_bytes);
            }
        }

        if missing.is_empty() {
            return Ok(resolved);
        }

        let mut next_value_id = self.next_value_id().await?;
        let mut batch = WriteBatch::new();
        let mut assigned = Vec::with_capacity(missing.len());
        for value_bytes in missing {
            let value_id = next_value_id;
            next_value_id = next_value_id.saturating_add(1);
            batch.put(
                self.value_id_lookup_key(&value_bytes)
                    .context("build value-id lookup key for batch write")?,
                value_id.to_be_bytes().to_vec(),
            );
            batch.put(self.value_data_key(value_id), value_bytes.clone());
            assigned.push((value_bytes, value_id));
        }
        batch.put(
            self.value_seq_key.clone(),
            next_value_id.to_be_bytes().to_vec(),
        );
        self.table
            .write_batch(batch)
            .await
            .context("persist batched value-id dictionary entries")?;

        for (value_bytes, value_id) in assigned {
            self.insert_value_id_cache(&value_bytes, value_id)?;
            self.insert_value_data_cache(value_id, &value_bytes)?;
            resolved.insert(value_bytes, value_id);
        }

        Ok(resolved)
    }

    async fn intern_value_bytes(&self, value_bytes: &[u8]) -> Result<u64> {
        if let Some(value_id) = self.cached_value_id_for_bytes(value_bytes)? {
            return Ok(value_id);
        }

        let lookup_key = self
            .value_id_lookup_key(value_bytes)
            .context("build value-id lookup key")?;
        if let Some(existing) = self
            .table
            .get(&lookup_key)
            .await
            .context("read value-id lookup entry")?
        {
            let value_id = decode_u64_payload(&existing)?;
            self.insert_value_id_cache(value_bytes, value_id)?;
            self.insert_value_data_cache(value_id, value_bytes)?;
            return Ok(value_id);
        }

        let _guard = self.value_intern_lock.lock().await;
        if let Some(existing) = self
            .table
            .get(&lookup_key)
            .await
            .context("re-read value-id lookup entry under lock")?
        {
            let value_id = decode_u64_payload(&existing)?;
            self.insert_value_id_cache(value_bytes, value_id)?;
            self.insert_value_data_cache(value_id, value_bytes)?;
            return Ok(value_id);
        }

        let value_id = self.next_value_id().await?;
        let mut batch = WriteBatch::new();
        batch.put(lookup_key, value_id.to_be_bytes().to_vec());
        batch.put(self.value_data_key(value_id), value_bytes.to_vec());
        batch.put(
            self.value_seq_key.clone(),
            value_id.saturating_add(1).to_be_bytes().to_vec(),
        );
        self.table
            .write_batch(batch)
            .await
            .context("persist value-id dictionary entry")?;

        self.insert_value_id_cache(value_bytes, value_id)?;
        self.insert_value_data_cache(value_id, value_bytes)?;
        Ok(value_id)
    }

    async fn value_for_id(&self, value_id: u64) -> Result<V> {
        let value_bytes = self
            .value_bytes_for_id(value_id)
            .await
            .with_context(|| format!("load value bytes for id {value_id}"))?;
        self.decode_value_bytes_cached(&value_bytes)
            .with_context(|| format!("decode value bytes for id {value_id}"))
    }

    async fn next_value_id(&self) -> Result<u64> {
        let Some(bytes) = self
            .table
            .get(&self.value_seq_key)
            .await
            .context("read next value-id key")?
        else {
            return Ok(1);
        };
        decode_u64_payload(&bytes)
    }

    /// Estimates point-lookup read amplification as `L0 scanned + L1 scanned` for one key.
    pub async fn estimated_read_amplification_for_key(&self, key: &K) -> Result<usize> {
        let key_bytes = encode(key).context("encode key for amplification estimate")?;
        let l0_prefix = self
            .l0_prefix_for_key(&key_bytes)
            .context("build L0 prefix for amplification estimate")?;
        let l1_prefix = self
            .l1_prefix_for_key(&key_bytes)
            .context("build L1 prefix for amplification estimate")?;
        let l1_id_prefix = self
            .l1_id_prefix_for_key(&key_bytes)
            .context("build L1-id prefix for amplification estimate")?;
        let l0_entries = self
            .table
            .scan_prefix(&l0_prefix, &ScanOptions::default())
            .await
            .context("scan L0 for amplification estimate")?;
        let l1_entries = self
            .table
            .scan_prefix(&l1_prefix, &ScanOptions::default())
            .await
            .context("scan L1 for amplification estimate")?;
        let l1_id_entries = self
            .table
            .scan_prefix(&l1_id_prefix, &ScanOptions::default())
            .await
            .context("scan L1-id for amplification estimate")?;
        Ok(l0_entries
            .iter()
            .filter(|(entry_key, _)| *entry_key != self.l0_seq_key)
            .count()
            .saturating_add(l1_entries.len())
            .saturating_add(l1_id_entries.len()))
    }

    pub async fn entries(&self) -> Result<Vec<(K, V, i64)>> {
        let mut aggregate: HashMap<(Vec<u8>, u64), i64> = HashMap::new();

        let l1_legacy_entries = self
            .table
            .scan_prefix(&self.l1_prefix, &ScanOptions::default())
            .await
            .context("scan batch-index L1 entries")?;
        for (entry_key, entry_value) in l1_legacy_entries {
            let key_bytes = self
                .key_bytes_from_l1_key(&entry_key)
                .context("decode batch-index L1 key bytes")?;
            let value_bytes = self
                .value_bytes_from_l1_key(&entry_key)
                .context("decode batch-index L1 value bytes")?;
            let value_id = self
                .intern_value_bytes(value_bytes)
                .await
                .context("intern legacy L1 value while materializing entries")?;
            let weight = decode_weight(&entry_value)?;
            *aggregate.entry((key_bytes.to_vec(), value_id)).or_insert(0) += weight;
        }

        let l1_id_entries = self
            .table
            .scan_prefix(&self.l1_id_prefix, &ScanOptions::default())
            .await
            .context("scan batch-index L1-id entries")?;
        for (entry_key, entry_value) in l1_id_entries {
            let key_bytes = self
                .key_bytes_from_l1_id_key(&entry_key)
                .context("decode batch-index L1-id key bytes")?;
            let value_id = self
                .value_id_from_l1_id_key(&entry_key)
                .context("decode batch-index L1-id value id")?;
            let weight = decode_weight(&entry_value)?;
            *aggregate.entry((key_bytes.to_vec(), value_id)).or_insert(0) += weight;
        }

        let l0_entries = self
            .table
            .scan_prefix(&self.l0_prefix, &ScanOptions::default())
            .await
            .context("scan batch-index L0 entries")?;
        for (entry_key, entry_value) in l0_entries {
            if entry_key == self.l0_seq_key {
                continue;
            }
            let key_bytes = self
                .key_bytes_from_l0_key(&entry_key)
                .context("decode batch-index L0 key bytes")?;
            let (decoded_value, delta) = decode_l0_payload::<V>(&entry_value)?;
            let value_id = match decoded_value {
                DecodedL0Value::Id(id) => id,
                DecodedL0Value::SegmentRef {
                    segment_id,
                    row_index,
                } => {
                    let bytes = self
                        .value_bytes_for_segment_ref(segment_id, row_index)
                        .await
                        .with_context(|| {
                            format!(
                                "load value bytes for segment {segment_id} row {row_index} while materializing entries"
                            )
                        })?;
                    self.intern_value_bytes(&bytes)
                        .await
                        .context("intern L0 v4 segment-ref value while materializing entries")?
                }
                DecodedL0Value::Encoded(bytes) => self
                    .intern_value_bytes(&bytes)
                    .await
                    .context("intern L0 v2 value while materializing entries")?,
                DecodedL0Value::Decoded(value) => {
                    let bytes = encode(&value)
                        .context("encode legacy L0 value while materializing entries")?;
                    self.intern_value_bytes(&bytes)
                        .await
                        .context("intern legacy L0 value while materializing entries")?
                }
            };
            *aggregate.entry((key_bytes, value_id)).or_insert(0) += delta;
        }

        let mut out = Vec::new();
        for ((key_bytes, value_id), weight) in aggregate {
            if weight != 0 {
                let key = decode::<K>(&key_bytes).context("decode batch-index key from bytes")?;
                let value = self.value_for_id(value_id).await?;
                out.push((key, value, weight));
            }
        }
        Ok(out)
    }

    /// Compacts all L0 overlay records into L1 compacted blocks.
    pub async fn compact_l0_to_l1(&self) -> Result<usize> {
        self.compact_l0_to_l1_shard(0, 1).await
    }

    /// Compacts the subset of L0 overlay records owned by one shard.
    pub async fn compact_l0_to_l1_shard(&self, shard_id: u16, shard_count: u16) -> Result<usize> {
        if shard_count == 0 {
            return Err(anyhow!("shard_count must be greater than zero"));
        }
        if shard_id >= shard_count {
            return Err(anyhow!(
                "shard_id must be less than shard_count ({shard_id} >= {shard_count})"
            ));
        }

        let mut compacted = 0_usize;
        let active_entries = self
            .table
            .scan_prefix(&self.l0_active_prefix, &ScanOptions::default())
            .await
            .context("scan active keys for compaction")?;
        for (active_key, _) in active_entries {
            let key_bytes = self
                .key_bytes_from_active_key(&active_key)
                .context("decode active key for compaction")?;
            if shard_for_key(&key_bytes, shard_count) != shard_id {
                continue;
            }
            compacted = compacted.saturating_add(
                self.compact_single_key_l0_to_l1(&key_bytes)
                    .await
                    .with_context(|| {
                        format!(
                            "compact key-local L0->L1 for key bytes len={}",
                            key_bytes.len()
                        )
                    })?,
            );
        }
        Ok(compacted)
    }

    async fn compact_single_key_l0_to_l1(&self, key_bytes: &[u8]) -> Result<usize> {
        let current_l0_head = self.next_sequence().await?.saturating_sub(1);
        let mut watermark = self
            .compaction_watermark_for_key(key_bytes)
            .await
            .context("load compaction watermark for key")?;
        if watermark > current_l0_head {
            watermark = 0;
        }

        let scan_from = watermark.saturating_add(1);
        let key_l0_entries = self
            .table
            .scan_range(
                self.l0_range_for_key_from_sequence(key_bytes, scan_from)
                    .context("build key-local L0 compaction range")?,
                &ScanOptions::default(),
            )
            .await
            .context("scan key-local L0 entries for compaction")?;
        if key_l0_entries.is_empty() {
            return Ok(0);
        }

        let mut delta_by_id: HashMap<u64, i64> = HashMap::new();
        let mut l0_payload_deltas: Vec<(Vec<u8>, i64)> = Vec::new();
        let mut segment_deletes: HashMap<u64, u64> = HashMap::new();
        let mut l0_keys = Vec::with_capacity(key_l0_entries.len());
        let mut max_seen_sequence = watermark;
        for (entry_key, entry_value) in key_l0_entries {
            let sequence = self
                .sequence_from_l0_key(&entry_key)
                .context("decode L0 sequence during key-local compaction")?;
            let (decoded_value, delta) = decode_l0_payload::<V>(&entry_value)?;
            match decoded_value {
                DecodedL0Value::Id(id) => {
                    *delta_by_id.entry(id).or_insert(0) += delta;
                }
                DecodedL0Value::SegmentRef {
                    segment_id,
                    row_index,
                } => {
                    let bytes = self
                        .value_bytes_for_segment_ref(segment_id, row_index)
                        .await
                        .with_context(|| {
                            format!(
                                "load value bytes for segment {segment_id} row {row_index} during compaction"
                            )
                        })?;
                    l0_payload_deltas.push((bytes, delta));
                    *segment_deletes.entry(segment_id).or_insert(0) += 1;
                }
                DecodedL0Value::Encoded(bytes) => {
                    l0_payload_deltas.push((bytes, delta));
                }
                DecodedL0Value::Decoded(value) => {
                    let bytes =
                        encode(&value).context("encode legacy L0 value during compaction")?;
                    l0_payload_deltas.push((bytes, delta));
                }
            }
            l0_keys.push(entry_key);
            max_seen_sequence = max_seen_sequence.max(sequence);
        }

        let mut merged_weights_by_id: HashMap<u64, i64> = HashMap::new();
        let mut existing_l1_id_keys = Vec::new();
        let l1_id_entries = self
            .table
            .scan_prefix(
                &self
                    .l1_id_prefix_for_key(key_bytes)
                    .context("build L1-id key prefix for compaction")?,
                &ScanOptions::default(),
            )
            .await
            .context("scan L1-id entries for compaction")?;
        for (entry_key, entry_value) in l1_id_entries {
            let value_id = self
                .value_id_from_l1_id_key(&entry_key)
                .context("decode value id from L1-id key during compaction")?;
            let weight = decode_weight(&entry_value)?;
            *merged_weights_by_id.entry(value_id).or_insert(0) += weight;
            existing_l1_id_keys.push(entry_key);
        }

        let mut existing_legacy_l1_keys = Vec::new();
        let mut legacy_l1_payload_weights: Vec<(Vec<u8>, i64)> = Vec::new();
        let l1_legacy_entries = self
            .table
            .scan_prefix(
                &self
                    .l1_prefix_for_key(key_bytes)
                    .context("build legacy L1 key prefix for compaction")?,
                &ScanOptions::default(),
            )
            .await
            .context("scan legacy L1 entries for compaction")?;
        for (entry_key, entry_value) in l1_legacy_entries {
            let value_bytes = self
                .value_bytes_from_l1_key(&entry_key)
                .context("decode legacy L1 value bytes during compaction")?;
            let weight = decode_weight(&entry_value)?;
            legacy_l1_payload_weights.push((value_bytes.to_vec(), weight));
            existing_legacy_l1_keys.push(entry_key);
        }

        let mut payloads_to_normalize =
            Vec::with_capacity(l0_payload_deltas.len() + legacy_l1_payload_weights.len());
        payloads_to_normalize.extend(
            l0_payload_deltas
                .iter()
                .map(|(value_bytes, _)| value_bytes.clone()),
        );
        payloads_to_normalize.extend(
            legacy_l1_payload_weights
                .iter()
                .map(|(value_bytes, _)| value_bytes.clone()),
        );
        let normalized_ids_by_payload = self
            .intern_value_bytes_batch(payloads_to_normalize)
            .await
            .context("batch normalize payload bytes to value ids during compaction")?;

        for (value_bytes, delta) in l0_payload_deltas {
            let value_id = normalized_ids_by_payload
                .get(&value_bytes)
                .copied()
                .ok_or_else(|| anyhow!("missing normalized id for L0 payload during compaction"))?;
            *delta_by_id.entry(value_id).or_insert(0) += delta;
        }

        for (value_bytes, weight) in legacy_l1_payload_weights {
            let value_id = normalized_ids_by_payload
                .get(&value_bytes)
                .copied()
                .ok_or_else(|| anyhow!("missing normalized id for L1 payload during compaction"))?;
            *merged_weights_by_id.entry(value_id).or_insert(0) += weight;
        }

        for (value_id, delta) in delta_by_id {
            *merged_weights_by_id.entry(value_id).or_insert(0) += delta;
        }
        merged_weights_by_id.retain(|_, weight| *weight != 0);

        let mut batch = WriteBatch::new();
        for key in existing_l1_id_keys {
            batch.delete(key);
        }
        for key in existing_legacy_l1_keys {
            batch.delete(key);
        }
        for (value_id, weight) in merged_weights_by_id {
            batch.put(
                self.l1_id_key(key_bytes, value_id)
                    .context("build L1-id key during compaction")?,
                encode_weight(weight),
            );
        }
        for key in &l0_keys {
            batch.delete(key.clone());
        }
        batch.put(
            self.compaction_watermark_key(key_bytes)
                .context("build compaction watermark key")?,
            max_seen_sequence.to_be_bytes().to_vec(),
        );

        if !segment_deletes.is_empty() {
            // Serialize segment refcount updates to avoid losing decrements across concurrent compactions.
            let _guard = self.l0_segment_ref_lock.lock().await;
            for (segment_id, deleted) in segment_deletes {
                let refcount_key = self.l0_segment_refcount_key(segment_id);
                let Some(existing) = self
                    .table
                    .get(&refcount_key)
                    .await
                    .context("read L0 segment refcount")?
                else {
                    continue;
                };
                let current =
                    decode_u64_payload(&existing).context("decode L0 segment refcount")?;
                let new = current.saturating_sub(deleted);
                if new == 0 {
                    batch.delete(refcount_key);
                    batch.delete(self.l0_segment_key(segment_id));
                } else {
                    batch.put(refcount_key, new.to_be_bytes().to_vec());
                }
            }
            self.table
                .write_batch(batch)
                .await
                .context("persist key-local compaction output")?;
        } else {
            self.table
                .write_batch(batch)
                .await
                .context("persist key-local compaction output")?;
        }
        self.reset_lookup_state_for_key(key_bytes)
            .context("reset lookup state after key-local compaction")?;
        Ok(l0_keys.len())
    }

    /// Publishes an index manifest after compaction with intent-backed atomic semantics.
    pub async fn publish_compacted_manifest(
        &self,
        manifest_store: &ManifestStore<IndexManifest>,
    ) -> Result<IndexManifest> {
        let latest = manifest_store
            .latest_manifest()
            .await
            .context("load latest index manifest before publish")?;
        let base = latest.as_ref().map(|manifest| manifest.version);
        let next_version = base.unwrap_or(0).saturating_add(1);

        let l0_entries = self
            .table
            .scan_prefix(&self.l0_prefix, &ScanOptions::default())
            .await
            .context("scan L0 entries for manifest statistics")?;
        let l1_entries = self
            .table
            .scan_prefix(&self.l1_prefix, &ScanOptions::default())
            .await
            .context("scan legacy L1 entries for manifest statistics")?;
        let l1_id_entries = self
            .table
            .scan_prefix(&self.l1_id_prefix, &ScanOptions::default())
            .await
            .context("scan L1-id entries for manifest statistics")?;

        let l0_count = l0_entries
            .iter()
            .filter(|(key, _)| *key != self.l0_seq_key)
            .count() as u64;
        let l1_count = (l1_entries.len() + l1_id_entries.len()) as u64;
        let total_bytes: u64 = l0_entries
            .iter()
            .chain(l1_entries.iter())
            .chain(l1_id_entries.iter())
            .map(|(key, value)| (key.len() + value.len()) as u64)
            .sum();
        let object_count = l0_count + l1_count;
        let tombstone_ratio = if object_count == 0 {
            0.0
        } else {
            l0_count as f64 / object_count as f64
        };
        let statistics =
            ManifestStatistics::new(object_count, l1_count, total_bytes, tombstone_ratio)
                .context("build index manifest statistics")?;

        let manifest = IndexManifest {
            version: next_version,
            base,
            reference_count: 1,
            statistics,
            l0_segments: vec![l0_count],
            l1_blocks: vec![l1_count],
        };

        manifest_store
            .publish_manifest(&manifest)
            .await
            .context("publish index compaction manifest")?;

        Ok(manifest)
    }

    async fn next_sequence(&self) -> Result<u64> {
        let Some(bytes) = self
            .table
            .get(&self.l0_seq_key)
            .await
            .context("read batch-index sequence key")?
        else {
            return Ok(1);
        };
        let chunk = bytes
            .get(0..8)
            .ok_or_else(|| anyhow!("invalid batch-index sequence payload"))?;
        Ok(u64::from_be_bytes(chunk.try_into().unwrap()))
    }

    fn l0_prefix_for_key(&self, key_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut key = self.l0_prefix.clone();
        key.extend_from_slice(&encode_len(key_bytes.len())?);
        key.extend_from_slice(key_bytes);
        Ok(key)
    }

    fn l0_segment_key(&self, segment_id: u64) -> Vec<u8> {
        let mut key = self.l0_segment_prefix.clone();
        key.extend_from_slice(&segment_id.to_be_bytes());
        key
    }

    fn l0_segment_refcount_key(&self, segment_id: u64) -> Vec<u8> {
        let mut key = self.l0_segment_refcount_prefix.clone();
        key.extend_from_slice(&segment_id.to_be_bytes());
        key
    }

    fn l0_overlay_key(&self, key_bytes: &[u8], sequence: u64) -> Result<Vec<u8>> {
        let mut key = self.l0_prefix_for_key(key_bytes)?;
        key.extend_from_slice(&sequence.to_be_bytes());
        Ok(key)
    }

    fn active_key(&self, key_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut key = self.l0_active_prefix.clone();
        key.extend_from_slice(&encode_len(key_bytes.len())?);
        key.extend_from_slice(key_bytes);
        Ok(key)
    }

    fn key_bytes_from_active_key(&self, key: &[u8]) -> Result<Vec<u8>> {
        if !key.starts_with(&self.l0_active_prefix) {
            return Err(anyhow!("active key missing expected prefix"));
        }
        let mut cursor = self.l0_active_prefix.len();
        let key_len = read_len(key, &mut cursor).context("read active key length")?;
        let end = cursor
            .checked_add(key_len)
            .ok_or_else(|| anyhow!("active key length overflow"))?;
        let key_bytes = key
            .get(cursor..end)
            .ok_or_else(|| anyhow!("active key truncated"))?
            .to_vec();
        if end != key.len() {
            return Err(anyhow!("active key has trailing bytes"));
        }
        Ok(key_bytes)
    }

    fn compaction_watermark_key(&self, key_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut key = self.compaction_watermark_prefix.clone();
        key.extend_from_slice(&encode_len(key_bytes.len())?);
        key.extend_from_slice(key_bytes);
        Ok(key)
    }

    async fn compaction_watermark_for_key(&self, key_bytes: &[u8]) -> Result<u64> {
        let key = self
            .compaction_watermark_key(key_bytes)
            .context("build compaction watermark key")?;
        let Some(bytes) = self
            .table
            .get(&key)
            .await
            .context("read compaction watermark")?
        else {
            return Ok(0);
        };
        let chunk = bytes
            .get(0..8)
            .ok_or_else(|| anyhow!("invalid compaction watermark payload"))?;
        Ok(u64::from_be_bytes(chunk.try_into().unwrap()))
    }

    fn l0_range_for_key_from_sequence(
        &self,
        key_bytes: &[u8],
        sequence: u64,
    ) -> Result<std::ops::Range<Vec<u8>>> {
        let mut start = self.l0_prefix_for_key(key_bytes)?;
        start.extend_from_slice(&sequence.to_be_bytes());
        let mut end = self.l0_prefix_for_key(key_bytes)?;
        end.push(0xFF);
        Ok(start..end)
    }

    fn key_bytes_from_l0_key(&self, key: &[u8]) -> Result<Vec<u8>> {
        let (key_bytes, _) = self.key_and_sequence_from_l0_key(key)?;
        Ok(key_bytes)
    }

    fn sequence_from_l0_key(&self, key: &[u8]) -> Result<u64> {
        let (_, sequence) = self.key_and_sequence_from_l0_key(key)?;
        Ok(sequence)
    }

    fn key_and_sequence_from_l0_key(&self, key: &[u8]) -> Result<(Vec<u8>, u64)> {
        if !key.starts_with(&self.l0_prefix) {
            return Err(anyhow!("batch-index L0 key missing prefix"));
        }
        let mut cursor = self.l0_prefix.len();
        let key_len = read_len(key, &mut cursor).context("read batch-index L0 key length")?;
        let key_end = cursor
            .checked_add(key_len)
            .ok_or_else(|| anyhow!("batch-index L0 key length overflow"))?;
        let key_bytes = key
            .get(cursor..key_end)
            .ok_or_else(|| anyhow!("batch-index L0 key truncated"))?
            .to_vec();
        cursor = key_end;
        let seq_end = cursor
            .checked_add(8)
            .ok_or_else(|| anyhow!("batch-index L0 sequence overflow"))?;
        let seq_chunk = key
            .get(cursor..seq_end)
            .ok_or_else(|| anyhow!("batch-index L0 key has invalid sequence suffix"))?;
        if key.len() != seq_end {
            return Err(anyhow!("batch-index L0 key has invalid sequence suffix"));
        }
        let sequence = u64::from_be_bytes(seq_chunk.try_into().unwrap());
        Ok((key_bytes, sequence))
    }

    fn l1_prefix_for_key(&self, key_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut prefix = self.l1_prefix.clone();
        prefix.extend_from_slice(&encode_len(key_bytes.len())?);
        prefix.extend_from_slice(key_bytes);
        Ok(prefix)
    }

    #[cfg(test)]
    fn l1_key(&self, key_bytes: &[u8], value_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut key = self.l1_prefix_for_key(key_bytes)?;
        key.extend_from_slice(&encode_len(value_bytes.len())?);
        key.extend_from_slice(value_bytes);
        Ok(key)
    }

    fn key_bytes_from_l1_key<'a>(&self, key: &'a [u8]) -> Result<&'a [u8]> {
        if !key.starts_with(&self.l1_prefix) {
            return Err(anyhow!("batch-index L1 key missing prefix"));
        }
        let mut cursor = self.l1_prefix.len();
        let key_len = read_len(key, &mut cursor).context("read batch-index L1 key length")?;
        let end = cursor
            .checked_add(key_len)
            .ok_or_else(|| anyhow!("batch-index L1 key length overflow"))?;
        key.get(cursor..end)
            .ok_or_else(|| anyhow!("batch-index L1 key truncated"))
    }

    fn value_bytes_from_l1_key<'a>(&self, key: &'a [u8]) -> Result<&'a [u8]> {
        if !key.starts_with(&self.l1_prefix) {
            return Err(anyhow!("batch-index L1 key missing prefix"));
        }
        let mut cursor = self.l1_prefix.len();
        let key_len = read_len(key, &mut cursor).context("read batch-index L1 key length")?;
        cursor = cursor
            .checked_add(key_len)
            .ok_or_else(|| anyhow!("batch-index L1 key length overflow"))?;
        let value_len = read_len(key, &mut cursor).context("read batch-index L1 value length")?;
        let end = cursor
            .checked_add(value_len)
            .ok_or_else(|| anyhow!("batch-index L1 value length overflow"))?;
        key.get(cursor..end)
            .ok_or_else(|| anyhow!("batch-index L1 value truncated"))
    }

    fn l1_id_prefix_for_key(&self, key_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut prefix = self.l1_id_prefix.clone();
        prefix.extend_from_slice(&encode_len(key_bytes.len())?);
        prefix.extend_from_slice(key_bytes);
        Ok(prefix)
    }

    fn l1_id_key(&self, key_bytes: &[u8], value_id: u64) -> Result<Vec<u8>> {
        let mut key = self.l1_id_prefix_for_key(key_bytes)?;
        key.extend_from_slice(&value_id.to_be_bytes());
        Ok(key)
    }

    fn key_bytes_from_l1_id_key<'a>(&self, key: &'a [u8]) -> Result<&'a [u8]> {
        if !key.starts_with(&self.l1_id_prefix) {
            return Err(anyhow!("batch-index L1-id key missing prefix"));
        }
        let mut cursor = self.l1_id_prefix.len();
        let key_len = read_len(key, &mut cursor).context("read batch-index L1-id key length")?;
        let end = cursor
            .checked_add(key_len)
            .ok_or_else(|| anyhow!("batch-index L1-id key length overflow"))?;
        key.get(cursor..end)
            .ok_or_else(|| anyhow!("batch-index L1-id key truncated"))
    }

    fn value_id_from_l1_id_key(&self, key: &[u8]) -> Result<u64> {
        if !key.starts_with(&self.l1_id_prefix) {
            return Err(anyhow!("batch-index L1-id key missing prefix"));
        }
        let mut cursor = self.l1_id_prefix.len();
        let key_len = read_len(key, &mut cursor).context("read batch-index L1-id key length")?;
        cursor = cursor
            .checked_add(key_len)
            .ok_or_else(|| anyhow!("batch-index L1-id key length overflow"))?;
        let end = cursor
            .checked_add(8)
            .ok_or_else(|| anyhow!("batch-index L1-id value id overflow"))?;
        let chunk = key
            .get(cursor..end)
            .ok_or_else(|| anyhow!("batch-index L1-id value id truncated"))?;
        if end != key.len() {
            return Err(anyhow!("batch-index L1-id key has trailing bytes"));
        }
        Ok(u64::from_be_bytes(chunk.try_into().unwrap()))
    }

    fn value_id_lookup_key(&self, value_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut key = self.value_id_prefix.clone();
        key.extend_from_slice(&encode_len(value_bytes.len())?);
        key.extend_from_slice(value_bytes);
        Ok(key)
    }

    fn value_data_key(&self, value_id: u64) -> Vec<u8> {
        let mut key = self.value_data_prefix.clone();
        key.extend_from_slice(&value_id.to_be_bytes());
        key
    }
}

fn make_mutex_shards<T: Default>(shard_count: usize) -> Vec<Mutex<T>> {
    (0..shard_count).map(|_| Mutex::new(T::default())).collect()
}

fn shard_for_key(key_bytes: &[u8], shard_count: u16) -> u16 {
    use std::hash::Hasher;

    let mut hasher = DefaultHasher::new();
    hasher.write(key_bytes);
    (hasher.finish() % shard_count as u64) as u16
}

fn encode_len(len: usize) -> Result<[u8; 4]> {
    let len = u32::try_from(len).map_err(|_| anyhow!("batch-index component too large"))?;
    Ok(len.to_be_bytes())
}

fn read_len(bytes: &[u8], cursor: &mut usize) -> Result<usize> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| anyhow!("batch-index length overflow"))?;
    let chunk = bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow!("batch-index length truncated"))?;
    *cursor = end;
    Ok(u32::from_be_bytes(chunk.try_into().unwrap()) as usize)
}

fn encode_weight(weight: i64) -> Vec<u8> {
    weight.to_be_bytes().to_vec()
}

fn encode_l0_payload_v2(value_bytes: &[u8], delta: i64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 8 + value_bytes.len());
    payload.push(L0_ENTRY_V2);
    payload.extend_from_slice(&delta.to_be_bytes());
    payload.extend_from_slice(value_bytes);
    payload
}

fn encode_l0_payload_v4_segment_ref(segment_id: u64, row_index: u32, delta: i64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 8 + 8 + 4);
    payload.push(L0_ENTRY_V4_SEGMENT_REF);
    payload.extend_from_slice(&delta.to_be_bytes());
    payload.extend_from_slice(&segment_id.to_be_bytes());
    payload.extend_from_slice(&row_index.to_be_bytes());
    payload
}

fn encode_l0_segment_v1(values: &[Vec<u8>]) -> Result<Vec<u8>> {
    let count = u32::try_from(values.len()).map_err(|_| anyhow!("too many rows for L0 segment"))?;
    let mut offsets: Vec<u32> = Vec::with_capacity(values.len() + 1);
    offsets.push(0);
    let mut total: u32 = 0;
    for value in values {
        let len = u32::try_from(value.len()).map_err(|_| anyhow!("L0 segment row too large"))?;
        total = total
            .checked_add(len)
            .ok_or_else(|| anyhow!("L0 segment size overflow"))?;
        offsets.push(total);
    }

    let mut out = Vec::with_capacity(1 + 4 + offsets.len() * 4 + total as usize);
    out.push(L0_SEGMENT_V1);
    out.extend_from_slice(&count.to_be_bytes());
    for offset in offsets {
        out.extend_from_slice(&offset.to_be_bytes());
    }
    for value in values {
        out.extend_from_slice(value);
    }
    Ok(out)
}

fn decode_l0_segment(bytes: Vec<u8>) -> Result<L0Segment> {
    let (tag, rest) = bytes
        .split_first()
        .ok_or_else(|| anyhow!("empty L0 segment payload"))?;
    if *tag != L0_SEGMENT_V1 {
        return Err(anyhow!("unknown L0 segment tag: {tag:#04x}"));
    }
    if rest.len() < 4 {
        return Err(anyhow!("truncated L0 segment header"));
    }
    let count = u32::from_be_bytes(rest[0..4].try_into().unwrap()) as usize;
    let offsets_bytes = (count + 1)
        .checked_mul(4)
        .ok_or_else(|| anyhow!("L0 segment offsets overflow"))?;
    let header_len = 1 + 4 + offsets_bytes;
    if bytes.len() < header_len {
        return Err(anyhow!(
            "truncated L0 segment offsets: need {header_len} bytes, got {}",
            bytes.len()
        ));
    }

    let mut offsets = Vec::with_capacity(count + 1);
    let mut cursor = 1 + 4;
    for _ in 0..(count + 1) {
        let chunk = bytes
            .get(cursor..cursor + 4)
            .ok_or_else(|| anyhow!("truncated L0 segment offset"))?;
        offsets.push(u32::from_be_bytes(chunk.try_into().unwrap()));
        cursor += 4;
    }
    for window in offsets.windows(2) {
        if window[0] > window[1] {
            return Err(anyhow!("non-monotonic L0 segment offsets"));
        }
    }
    let blob_start = header_len;
    let blob_len = bytes.len() - blob_start;
    let expected = *offsets
        .last()
        .ok_or_else(|| anyhow!("missing L0 segment offset terminator"))?
        as usize;
    if expected != blob_len {
        return Err(anyhow!(
            "L0 segment blob length mismatch: expected {expected} bytes, got {blob_len}"
        ));
    }

    Ok(L0Segment {
        bytes,
        blob_start,
        offsets,
    })
}

enum DecodedL0Value<V> {
    Id(u64),
    SegmentRef { segment_id: u64, row_index: u32 },
    Encoded(Vec<u8>),
    Decoded(V),
}

fn decode_l0_payload<V>(bytes: &[u8]) -> Result<(DecodedL0Value<V>, i64)>
where
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    if bytes.first().copied() == Some(L0_ENTRY_V3_ID) {
        let delta_bytes = bytes
            .get(1..9)
            .ok_or_else(|| anyhow!("invalid batch-index L0 v3 payload delta"))?;
        let value_id_bytes = bytes
            .get(9..17)
            .ok_or_else(|| anyhow!("invalid batch-index L0 v3 payload value id"))?;
        let delta = i64::from_be_bytes(delta_bytes.try_into().unwrap());
        let value_id = u64::from_be_bytes(value_id_bytes.try_into().unwrap());
        return Ok((DecodedL0Value::Id(value_id), delta));
    }

    if bytes.first().copied() == Some(L0_ENTRY_V2) {
        let delta_bytes = bytes
            .get(1..9)
            .ok_or_else(|| anyhow!("invalid batch-index L0 v2 payload header"))?;
        let value_bytes = bytes
            .get(9..)
            .ok_or_else(|| anyhow!("invalid batch-index L0 v2 payload body"))?;
        let delta = i64::from_be_bytes(delta_bytes.try_into().unwrap());
        return Ok((DecodedL0Value::Encoded(value_bytes.to_vec()), delta));
    }

    if bytes.first().copied() == Some(L0_ENTRY_V4_SEGMENT_REF) {
        let delta_bytes = bytes
            .get(1..9)
            .ok_or_else(|| anyhow!("invalid batch-index L0 v4 payload delta"))?;
        let segment_id_bytes = bytes
            .get(9..17)
            .ok_or_else(|| anyhow!("invalid batch-index L0 v4 payload segment id"))?;
        let row_index_bytes = bytes
            .get(17..21)
            .ok_or_else(|| anyhow!("invalid batch-index L0 v4 payload row index"))?;
        if bytes.len() != 21 {
            return Err(anyhow!("invalid batch-index L0 v4 payload size"));
        }
        let delta = i64::from_be_bytes(delta_bytes.try_into().unwrap());
        let segment_id = u64::from_be_bytes(segment_id_bytes.try_into().unwrap());
        let row_index = u32::from_be_bytes(row_index_bytes.try_into().unwrap());
        return Ok((
            DecodedL0Value::SegmentRef {
                segment_id,
                row_index,
            },
            delta,
        ));
    }

    let entry: OverlayEntry<V> = decode(bytes).context("decode batch-index L0 legacy entry")?;
    Ok((DecodedL0Value::Decoded(entry.value), entry.delta))
}

fn decode_weight(bytes: &[u8]) -> Result<i64> {
    let chunk = bytes
        .get(0..8)
        .ok_or_else(|| anyhow!("expected 8 bytes for batch-index weight"))?;
    Ok(i64::from_be_bytes(chunk.try_into().unwrap()))
}

fn decode_u64_payload(bytes: &[u8]) -> Result<u64> {
    let chunk = bytes
        .get(0..8)
        .ok_or_else(|| anyhow!("expected 8 bytes for batch-index u64 payload"))?;
    Ok(u64::from_be_bytes(chunk.try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::RowReferenceV1;
    use crate::storage::encoding::encode as encode_storage;
    use crate::storage::manifest::{IndexManifest, ManifestStore};
    use object_store::memory::InMemory;
    use slatedb::Db;

    async fn build_table(name: &str) -> Arc<dyn KeyValueTable> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
        Arc::new(crate::storage::SlateTable::new(db))
    }

    #[tokio::test]
    async fn apply_deltas_preserves_weight_semantics() {
        let table = build_table("indexed_batch_semantics").await;
        let index = IndexedBatchZSet::<i64, i64>::new(table, "indexed_batch_semantics");

        index
            .apply_deltas(vec![(1, 10, 2), (1, 11, 3), (1, 10, -1), (1, 11, -3)])
            .await
            .expect("apply overlay deltas");

        let mut values = index.values_for_key(&1).await.expect("values for key");
        values.sort_by_key(|(value, _)| *value);
        assert_eq!(values, vec![(10, 1)]);
    }

    #[tokio::test]
    async fn apply_deltas_does_not_intern_values_on_write_path() {
        let table = build_table("indexed_batch_no_write_intern").await;
        let index =
            IndexedBatchZSet::<i64, i64>::new(table.clone(), "indexed_batch_no_write_intern");

        index
            .apply_deltas(vec![(1, 10, 1), (1, 11, 1), (2, 20, 1)])
            .await
            .expect("apply deltas without foreground interning");

        let value_id_entries = table
            .scan_prefix(&index.value_id_prefix, &ScanOptions::default())
            .await
            .expect("scan value-id entries");
        assert!(
            value_id_entries.is_empty(),
            "write path should not persist value-id lookups"
        );

        let value_data_entries = table
            .scan_prefix(&index.value_data_prefix, &ScanOptions::default())
            .await
            .expect("scan value payload entries");
        assert!(
            value_data_entries.is_empty(),
            "write path should not persist value-data entries"
        );

        let value_seq = table
            .get(&index.value_seq_key)
            .await
            .expect("read value sequence key");
        assert!(
            value_seq.is_none(),
            "write path should not advance dictionary sequence"
        );
    }

    #[tokio::test]
    async fn apply_deltas_coalescing_matches_non_coalesced_behavior() {
        let coalesced_table = build_table("indexed_batch_coalesced").await;
        let coalesced =
            IndexedBatchZSet::<i64, i64>::new(coalesced_table, "indexed_batch_coalesced");
        let non_coalesced_table = build_table("indexed_batch_non_coalesced").await;
        let non_coalesced =
            IndexedBatchZSet::<i64, i64>::new(non_coalesced_table, "indexed_batch_non_coalesced");

        let updates = vec![
            (1, 10, 1),
            (1, 10, 1),
            (1, 10, -1),
            (1, 11, 3),
            (1, 11, -2),
            (2, 20, 4),
            (2, 20, -1),
            (2, 20, -3),
        ];

        let stats = coalesced
            .apply_deltas_with_stats(updates.iter().cloned())
            .await
            .expect("apply coalesced deltas");
        for update in &updates {
            non_coalesced
                .apply_deltas(std::iter::once(*update))
                .await
                .expect("apply non-coalesced delta");
        }

        let mut coalesced_entries = coalesced.entries().await.expect("coalesced entries");
        let mut non_coalesced_entries = non_coalesced
            .entries()
            .await
            .expect("non-coalesced entries");
        coalesced_entries.sort_unstable();
        non_coalesced_entries.sort_unstable();
        assert_eq!(coalesced_entries, non_coalesced_entries);

        assert_eq!(stats.input_records, updates.len());
        assert_eq!(stats.non_zero_input_records, updates.len());
        assert!(
            stats.persisted_records < stats.non_zero_input_records,
            "coalescing should reduce L0 puts within one apply_deltas call"
        );
    }

    #[test]
    fn adaptive_coalescing_detects_duplicates_in_large_batch() {
        let mut updates = Vec::new();
        for idx in 0..(ADAPTIVE_COALESCE_THRESHOLD + 64) {
            updates.push((1_i64, (idx / 2) as i64, 1_i64));
        }

        assert!(
            IndexedBatchZSet::<i64, i64>::should_use_coalescing(&updates),
            "large batches with obvious duplicates should keep coalescing enabled"
        );
    }

    #[test]
    fn adaptive_coalescing_skips_when_no_duplicates_observed() {
        let mut updates = Vec::new();
        for idx in 0..(ADAPTIVE_COALESCE_THRESHOLD + 64) {
            updates.push((1_i64, idx as i64, 1_i64));
        }

        assert!(
            !IndexedBatchZSet::<i64, i64>::should_use_coalescing(&updates),
            "large batches with unique pairs should skip coalescing when safe"
        );
    }

    #[tokio::test]
    async fn large_unique_batch_uses_segment_ref_l0_layout() {
        let table = build_table("indexed_batch_segment_ref_layout").await;
        let index =
            IndexedBatchZSet::<i64, i64>::new(table.clone(), "indexed_batch_segment_ref_layout");

        let mut updates = Vec::new();
        for idx in 0..(ADAPTIVE_COALESCE_THRESHOLD + 32) {
            let key = (idx as i64) % 8;
            updates.push((key, idx as i64, 1_i64));
        }
        index
            .apply_deltas(updates)
            .await
            .expect("apply large unique batch");

        let key_bytes = encode_storage(&0_i64).expect("encode key");
        let l0_entries = table
            .scan_prefix(
                &index
                    .l0_prefix_for_key(&key_bytes)
                    .expect("build key-local L0 prefix"),
                &ScanOptions::default(),
            )
            .await
            .expect("scan key-local L0 entries");
        assert!(
            l0_entries
                .iter()
                .all(|(_, value)| value.first().copied() == Some(L0_ENTRY_V4_SEGMENT_REF)),
            "expected segment-ref L0 payloads for large unique batch"
        );

        let segment_entries = table
            .scan_prefix(&index.l0_segment_prefix, &ScanOptions::default())
            .await
            .expect("scan L0 segment payload entries");
        assert_eq!(segment_entries.len(), 1, "expected one segment payload");

        let segment_ref_entries = table
            .scan_prefix(&index.l0_segment_refcount_prefix, &ScanOptions::default())
            .await
            .expect("scan L0 segment refcount entries");
        assert_eq!(
            segment_ref_entries.len(),
            1,
            "expected one segment refcount"
        );
    }

    #[tokio::test]
    async fn compaction_deletes_unreferenced_segments() {
        let table = build_table("indexed_batch_segment_gc").await;
        let index = IndexedBatchZSet::<i64, i64>::new(table.clone(), "indexed_batch_segment_gc");

        let mut updates = Vec::new();
        for idx in 0..(ADAPTIVE_COALESCE_THRESHOLD + 32) {
            let key = (idx as i64) % 8;
            updates.push((key, idx as i64, 1_i64));
        }
        index
            .apply_deltas(updates)
            .await
            .expect("apply large unique batch");

        index.compact_l0_to_l1().await.expect("compact all keys");

        let segment_entries = table
            .scan_prefix(&index.l0_segment_prefix, &ScanOptions::default())
            .await
            .expect("scan L0 segment payload entries after compaction");
        assert!(
            segment_entries.is_empty(),
            "expected compaction to delete unreferenced segments"
        );

        let segment_ref_entries = table
            .scan_prefix(&index.l0_segment_refcount_prefix, &ScanOptions::default())
            .await
            .expect("scan L0 segment refcount entries after compaction");
        assert!(
            segment_ref_entries.is_empty(),
            "expected compaction to delete segment refcounts"
        );
    }

    #[tokio::test]
    async fn values_for_key_uses_incremental_l0_cursor_after_cache_seed() {
        let table = build_table("indexed_batch_lookup_cursor").await;
        let index = IndexedBatchZSet::<i64, i64>::new(table.clone(), "indexed_batch_lookup_cursor");

        index
            .apply_deltas(vec![(1, 10, 1)])
            .await
            .expect("seed first L0 record");
        let first = index.values_for_key(&1).await.expect("seed lookup cache");
        assert_eq!(first, vec![(10, 1)]);

        let key_bytes = encode_storage(&1_i64).expect("encode key");
        let l0_prefix = index
            .l0_prefix_for_key(&key_bytes)
            .expect("build L0 prefix for key");
        let mut l0_entries = table
            .scan_prefix(&l0_prefix, &ScanOptions::default())
            .await
            .expect("scan key-local L0 entries");
        assert_eq!(l0_entries.len(), 1);
        let (first_l0_key, _) = l0_entries.pop().expect("first L0 key");

        table
            .put(&first_l0_key, &[L0_ENTRY_V2])
            .await
            .expect("corrupt old L0 payload");

        index
            .apply_deltas(vec![(1, 10, 1)])
            .await
            .expect("append new L0 record");

        let second = index
            .values_for_key(&1)
            .await
            .expect("incremental lookup should skip corrupted old entry");
        assert_eq!(second, vec![(10, 2)]);
    }

    #[tokio::test]
    async fn values_for_key_cache_cold_and_warm_equivalent() {
        let table = build_table("indexed_batch_lookup_cache_cold_warm").await;
        let writer = IndexedBatchZSet::<i64, i64>::new(
            table.clone(),
            "indexed_batch_lookup_cache_cold_warm",
        );

        writer
            .apply_deltas(vec![(1, 10, 2), (1, 11, 1)])
            .await
            .expect("write L0 deltas");
        writer.compact_l0_to_l1().await.expect("compact into L1");

        let reader = IndexedBatchZSet::<i64, i64>::new(
            table.clone(),
            "indexed_batch_lookup_cache_cold_warm",
        );
        let mut cold = reader.values_for_key(&1).await.expect("cold lookup");
        cold.sort_unstable();

        let mut warm = reader.values_for_key(&1).await.expect("warm lookup");
        warm.sort_unstable();

        assert_eq!(cold, warm, "cold and warm lookups should match");
        assert_eq!(warm, vec![(10, 2), (11, 1)]);
    }

    #[tokio::test]
    async fn warm_lookup_uses_memory_when_dictionary_bytes_go_missing() {
        let table = build_table("indexed_batch_lookup_memory_first").await;
        let writer =
            IndexedBatchZSet::<i64, i64>::new(table.clone(), "indexed_batch_lookup_memory_first");

        writer
            .apply_deltas(vec![(1, 10, 2), (1, 11, 1)])
            .await
            .expect("write L0 deltas");
        writer.compact_l0_to_l1().await.expect("compact into L1");

        let reader =
            IndexedBatchZSet::<i64, i64>::new(table.clone(), "indexed_batch_lookup_memory_first");
        let mut baseline = reader.values_for_key(&1).await.expect("seed warm cache");
        baseline.sort_unstable();

        let key_bytes = encode_storage(&1_i64).expect("encode key");
        let l1_id_entries = table
            .scan_prefix(
                &reader
                    .l1_id_prefix_for_key(&key_bytes)
                    .expect("build L1-id key prefix"),
                &ScanOptions::default(),
            )
            .await
            .expect("scan L1-id entries");
        assert!(
            !l1_id_entries.is_empty(),
            "expected compacted lookup state to include L1-id entries"
        );
        let first_value_id = reader
            .value_id_from_l1_id_key(&l1_id_entries[0].0)
            .expect("decode first value id");

        table
            .delete(&reader.value_data_key(first_value_id))
            .await
            .expect("delete dictionary payload for first id");

        let mut warm = reader
            .values_for_key(&1)
            .await
            .expect("warm lookup should use in-memory value bytes");
        warm.sort_unstable();
        assert_eq!(warm, baseline);

        let reopened =
            IndexedBatchZSet::<i64, i64>::new(table.clone(), "indexed_batch_lookup_memory_first");
        let err = reopened
            .values_for_key(&1)
            .await
            .expect_err("cold restart should fallback to dictionary payload and fail");
        assert!(
            format!("{err:#}").contains("missing value bytes for id"),
            "expected missing dictionary payload error, got: {err:#}"
        );
    }

    #[tokio::test]
    async fn values_for_key_reads_legacy_overlay_entry_payload() {
        let table = build_table("indexed_batch_legacy_l0").await;
        let index = IndexedBatchZSet::<i64, i64>::new(table.clone(), "indexed_batch_legacy_l0");

        let key_bytes = encode_storage(&7_i64).expect("encode key");
        let legacy = OverlayEntry {
            value: 11_i64,
            delta: 3_i64,
        };
        let legacy_bytes = encode_storage(&legacy).expect("encode legacy overlay entry");
        let l0_key = index
            .l0_overlay_key(&key_bytes, 1)
            .expect("build first L0 key");

        let mut batch = WriteBatch::new();
        batch.put(l0_key, legacy_bytes);
        batch.put(index.l0_seq_key.clone(), 2_u64.to_be_bytes().to_vec());
        table
            .write_batch(batch)
            .await
            .expect("write legacy L0 payload");

        let values = index
            .values_for_key(&7)
            .await
            .expect("lookup legacy L0 entry");
        assert_eq!(values, vec![(11, 3)]);
    }

    #[tokio::test]
    async fn lookup_merges_l0_and_compacted_l1() {
        let table = build_table("indexed_batch_merge").await;
        let index = IndexedBatchZSet::<i64, i64>::new(table, "indexed_batch_merge");

        index
            .apply_deltas(vec![(1, 10, 2), (1, 11, 1)])
            .await
            .expect("apply base deltas");
        let compacted = index.compact_l0_to_l1().await.expect("compact");
        assert_eq!(compacted, 2);

        index
            .apply_deltas(vec![(1, 10, -1), (1, 11, 1), (1, 12, 5)])
            .await
            .expect("apply overlay deltas after compact");

        let mut values = index.values_for_key(&1).await.expect("merged lookup");
        values.sort_by_key(|(value, _)| *value);
        assert_eq!(values, vec![(10, 1), (11, 2), (12, 5)]);
    }

    #[tokio::test]
    async fn restart_keeps_overlay_append_sequence_monotonic() {
        let table = build_table("indexed_batch_restart").await;
        let first = IndexedBatchZSet::<i64, i64>::new(table.clone(), "indexed_batch_restart");
        first
            .apply_deltas(vec![(1, 10, 1)])
            .await
            .expect("first append");

        let reopened = IndexedBatchZSet::<i64, i64>::new(table, "indexed_batch_restart");
        reopened
            .apply_deltas(vec![(1, 10, 2)])
            .await
            .expect("append after restart");

        let values = reopened
            .values_for_key(&1)
            .await
            .expect("lookup after restart");
        assert_eq!(values, vec![(10, 3)]);
    }

    #[tokio::test]
    async fn row_reference_duplicate_retraction_toggle_workload() {
        let table = build_table("indexed_batch_row_refs").await;
        let index = IndexedBatchZSet::<i64, RowReferenceV1>::new(table, "indexed_batch_row_refs");

        let row_ref = RowReferenceV1::new(7, 12, 0);
        index
            .apply_deltas(vec![
                (1, row_ref, 1),
                (1, row_ref, 1),
                (1, row_ref, -1),
                (1, row_ref, -1),
                (1, row_ref, 1),
            ])
            .await
            .expect("apply row-reference toggle deltas");

        let values = index
            .values_for_key(&1)
            .await
            .expect("row-reference lookup");
        assert_eq!(values, vec![(row_ref, 1)]);
    }

    #[tokio::test]
    async fn reverse_index_supports_value_lookups() {
        let table = build_table("indexed_batch_reverse").await;
        let index =
            IndexedBatchZSet::<i64, i64>::with_reverse_index(table, "indexed_batch_reverse");

        index
            .apply_deltas(vec![(1, 10, 2), (2, 10, 1), (1, 11, 3)])
            .await
            .expect("apply deltas");

        let mut values = index.values_for_key(&1).await.expect("values for key");
        values.sort_by_key(|(value, _)| *value);
        assert_eq!(values, vec![(10, 2), (11, 3)]);

        let mut keys = index.keys_for_value(&10).await.expect("keys for value");
        keys.sort_by_key(|(key, _)| *key);
        assert_eq!(keys, vec![(1, 2), (2, 1)]);

        index
            .apply_deltas(vec![(1, 10, -2), (2, 10, -1)])
            .await
            .expect("retract values");
        assert!(
            index
                .keys_for_value(&10)
                .await
                .expect("keys after retract")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn range_index_supports_key_scans() {
        let table = build_table("indexed_batch_range").await;
        let index = IndexedBatchZSet::<i64, i64>::with_range_index(table, "indexed_batch_range");

        index
            .apply_deltas_with_range(vec![(1, 10, 1), (3, 30, 2), (5, 50, 1)])
            .await
            .expect("apply range deltas");

        let mut entries = index
            .values_for_key_range(&2, &6)
            .await
            .expect("range scan");
        entries.sort_by_key(|(key, value, _)| (*key, *value));
        assert_eq!(entries, vec![(3, 30, 2), (5, 50, 1)]);
    }

    #[tokio::test]
    async fn range_index_orders_bytes_lexicographically() {
        let table = build_table("indexed_batch_range_bytes").await;
        let index = IndexedBatchZSet::<OrderedBytes, i64>::with_range_index(
            table,
            "indexed_batch_range_bytes",
        );

        index
            .apply_deltas_with_range(vec![
                (OrderedBytes::from("b"), 10, 1),
                (OrderedBytes::from("aa"), 20, 1),
                (OrderedBytes::from("c"), 30, 1),
            ])
            .await
            .expect("apply bytes range deltas");

        let mut entries = index
            .values_for_key_range(&OrderedBytes::from("b"), &OrderedBytes::from("d"))
            .await
            .expect("range scan");
        entries.sort_by(|(ka, va, _), (kb, vb, _)| {
            ka.as_bytes().cmp(kb.as_bytes()).then_with(|| va.cmp(vb))
        });

        assert_eq!(
            entries,
            vec![
                (OrderedBytes::from("b"), 10, 1),
                (OrderedBytes::from("c"), 30, 1),
            ]
        );
    }

    #[tokio::test]
    async fn shard_compaction_only_processes_selected_shard() {
        let table = build_table("indexed_batch_shards").await;
        let index = IndexedBatchZSet::<i64, i64>::new(table, "indexed_batch_shards");

        index
            .apply_deltas(vec![(1, 10, 1), (2, 20, 1)])
            .await
            .expect("write mixed-shard deltas");

        let key_one_bytes = encode_storage(&1_i64).expect("encode shard key");
        let target_shard = shard_for_key(&key_one_bytes, 2);
        let compacted = index
            .compact_l0_to_l1_shard(target_shard, 2)
            .await
            .expect("compact shard");
        assert!(compacted >= 1);

        let values_one = index.values_for_key(&1).await.expect("values for key one");
        let values_two = index.values_for_key(&2).await.expect("values for key two");
        assert_eq!(values_one, vec![(10, 1)]);
        assert_eq!(values_two, vec![(20, 1)]);
    }

    #[tokio::test]
    async fn publishes_index_manifest_atomically_after_compaction() {
        let table = build_table("indexed_batch_manifest").await;
        let namespace = "indexed_batch_manifest";
        let index = IndexedBatchZSet::<i64, i64>::new(table.clone(), namespace);
        let manifest_store = ManifestStore::<IndexManifest>::index(table, namespace);

        index
            .apply_deltas(vec![(1, 10, 2), (1, 11, 1)])
            .await
            .expect("write L0 deltas");
        index.compact_l0_to_l1().await.expect("compact into L1");

        let first = index
            .publish_compacted_manifest(&manifest_store)
            .await
            .expect("publish first manifest");
        assert_eq!(first.version, 1);
        assert_eq!(first.base, None);

        index
            .apply_deltas(vec![(1, 10, -1)])
            .await
            .expect("write second L0 delta");
        index
            .compact_l0_to_l1()
            .await
            .expect("second compact into L1");

        let second = index
            .publish_compacted_manifest(&manifest_store)
            .await
            .expect("publish second manifest");
        assert_eq!(second.version, 2);
        assert_eq!(second.base, Some(1));

        let latest = manifest_store
            .latest_manifest()
            .await
            .expect("load latest manifest")
            .expect("manifest exists");
        assert_eq!(latest.version, 2);
        assert_eq!(latest.base, Some(1));
    }

    #[tokio::test]
    async fn compaction_preserves_lookup_semantics_and_reduces_read_amplification() {
        let table = build_table("indexed_batch_amp").await;
        let index = IndexedBatchZSet::<i64, i64>::new(table, "indexed_batch_amp");

        index
            .apply_deltas(vec![
                (1, 10, 1),
                (1, 10, -1),
                (1, 11, 1),
                (1, 12, 1),
                (1, 12, -1),
                (1, 13, 1),
            ])
            .await
            .expect("write L0 churn");

        let mut before = index
            .values_for_key(&1)
            .await
            .expect("lookup before compaction");
        before.sort_unstable();
        let amp_before = index
            .estimated_read_amplification_for_key(&1)
            .await
            .expect("estimate amplification before compaction");

        index.compact_l0_to_l1().await.expect("compact L0 into L1");

        let mut after = index
            .values_for_key(&1)
            .await
            .expect("lookup after compaction");
        after.sort_unstable();
        let amp_after = index
            .estimated_read_amplification_for_key(&1)
            .await
            .expect("estimate amplification after compaction");

        assert_eq!(before, after, "lookup semantics must remain stable");
        assert!(
            amp_after <= amp_before,
            "compaction should not increase read amplification"
        );
    }

    #[tokio::test]
    async fn incremental_compaction_compacts_only_new_l0_ranges() {
        let table = build_table("indexed_batch_incremental_compaction").await;
        let index =
            IndexedBatchZSet::<i64, i64>::new(table, "indexed_batch_incremental_compaction");

        index
            .apply_deltas(vec![(1, 10, 1), (1, 11, 1)])
            .await
            .expect("write initial L0 records");
        let first = index.compact_l0_to_l1().await.expect("first compaction");
        assert_eq!(first, 2);

        let second = index.compact_l0_to_l1().await.expect("second compaction");
        assert_eq!(second, 0, "no new L0 entries should be reprocessed");

        index
            .apply_deltas(vec![(1, 10, -1), (1, 12, 3)])
            .await
            .expect("write new L0 records");
        let third = index.compact_l0_to_l1().await.expect("third compaction");
        assert_eq!(third, 2, "only newly appended L0 rows should be compacted");

        let mut values = index
            .values_for_key(&1)
            .await
            .expect("lookup after compactions");
        values.sort_unstable();
        assert_eq!(values, vec![(11, 1), (12, 3)]);
    }

    #[tokio::test]
    async fn values_for_key_reads_v2_encoded_payload() {
        let table = build_table("indexed_batch_l0_v2").await;
        let index = IndexedBatchZSet::<i64, i64>::new(table.clone(), "indexed_batch_l0_v2");

        let key_bytes = encode_storage(&5_i64).expect("encode key");
        let value_bytes = encode_storage(&17_i64).expect("encode value");
        let l0_key = index.l0_overlay_key(&key_bytes, 1).expect("build L0 key");

        let mut batch = WriteBatch::new();
        batch.put(l0_key, encode_l0_payload_v2(&value_bytes, 3));
        batch.put(index.l0_seq_key.clone(), 2_u64.to_be_bytes().to_vec());
        table.write_batch(batch).await.expect("write L0 v2 payload");

        let values = index.values_for_key(&5).await.expect("lookup v2 payload");
        assert_eq!(values, vec![(17, 3)]);
    }

    #[tokio::test]
    async fn values_for_key_reads_mixed_legacy_and_id_l1_layouts() {
        let table = build_table("indexed_batch_mixed_l1_layout").await;
        let index =
            IndexedBatchZSet::<i64, i64>::new(table.clone(), "indexed_batch_mixed_l1_layout");

        let key_bytes = encode_storage(&1_i64).expect("encode key");
        let legacy_value_bytes = encode_storage(&10_i64).expect("encode legacy value");
        let legacy_l1_key = index
            .l1_key(&key_bytes, &legacy_value_bytes)
            .expect("build legacy L1 key");

        let id_value_bytes = encode_storage(&11_i64).expect("encode id value");
        let value_id = index
            .intern_value_bytes(&id_value_bytes)
            .await
            .expect("intern value id");
        let id_l1_key = index
            .l1_id_key(&key_bytes, value_id)
            .expect("build id L1 key");

        let mut batch = WriteBatch::new();
        batch.put(legacy_l1_key, encode_weight(2));
        batch.put(id_l1_key, encode_weight(3));
        table
            .write_batch(batch)
            .await
            .expect("write mixed L1 layouts");

        let mut values = index
            .values_for_key(&1)
            .await
            .expect("lookup mixed L1 layouts");
        values.sort_unstable();
        assert_eq!(values, vec![(10, 2), (11, 3)]);
    }

    #[tokio::test]
    async fn compaction_migrates_legacy_l1_entries_to_id_layout() {
        let table = build_table("indexed_batch_compact_migrate_l1").await;
        let index =
            IndexedBatchZSet::<i64, i64>::new(table.clone(), "indexed_batch_compact_migrate_l1");

        let key_bytes = encode_storage(&1_i64).expect("encode key");
        let value_bytes = encode_storage(&10_i64).expect("encode value");
        let legacy_l1_key = index
            .l1_key(&key_bytes, &value_bytes)
            .expect("build legacy L1 key");
        let mut seed = WriteBatch::new();
        seed.put(legacy_l1_key, encode_weight(2));
        table
            .write_batch(seed)
            .await
            .expect("seed legacy L1 entry for migration");

        index
            .apply_deltas(vec![(1, 10, -1)])
            .await
            .expect("append L0 delta");
        index
            .compact_l0_to_l1()
            .await
            .expect("compact and migrate legacy L1 entries");

        let values = index
            .values_for_key(&1)
            .await
            .expect("lookup after migration");
        assert_eq!(values, vec![(10, 1)]);

        let legacy_entries = table
            .scan_prefix(
                &index
                    .l1_prefix_for_key(&key_bytes)
                    .expect("build legacy L1 prefix"),
                &ScanOptions::default(),
            )
            .await
            .expect("scan legacy L1 entries");
        assert!(
            legacy_entries.is_empty(),
            "legacy L1 entries should be migrated to id layout"
        );

        let id_entries = table
            .scan_prefix(
                &index
                    .l1_id_prefix_for_key(&key_bytes)
                    .expect("build L1-id prefix"),
                &ScanOptions::default(),
            )
            .await
            .expect("scan L1-id entries");
        assert_eq!(id_entries.len(), 1);
    }

    #[tokio::test]
    async fn compaction_normalizes_payloads_to_dictionary_ids_in_background() {
        let table = build_table("indexed_batch_compaction_normalization").await;
        let index = IndexedBatchZSet::<i64, i64>::new(
            table.clone(),
            "indexed_batch_compaction_normalization",
        );

        index
            .apply_deltas(vec![(1, 10, 2), (1, 11, 1)])
            .await
            .expect("write payload-based L0 deltas");

        let pre_compaction_dict = table
            .scan_prefix(&index.value_data_prefix, &ScanOptions::default())
            .await
            .expect("scan dictionary payload entries before compaction");
        assert!(
            pre_compaction_dict.is_empty(),
            "foreground writes should not normalize payloads into dictionary ids"
        );

        index
            .compact_l0_to_l1()
            .await
            .expect("compact and normalize payloads");

        let post_compaction_dict = table
            .scan_prefix(&index.value_data_prefix, &ScanOptions::default())
            .await
            .expect("scan dictionary payload entries after compaction");
        assert_eq!(
            post_compaction_dict.len(),
            2,
            "compaction should batch-normalize unique payloads into dictionary ids"
        );

        let key_bytes = encode_storage(&1_i64).expect("encode key");
        let legacy_entries = table
            .scan_prefix(
                &index
                    .l1_prefix_for_key(&key_bytes)
                    .expect("build legacy L1 prefix"),
                &ScanOptions::default(),
            )
            .await
            .expect("scan legacy L1 entries");
        assert!(
            legacy_entries.is_empty(),
            "compaction output should be normalized to id layout"
        );

        let mut values = index
            .values_for_key(&1)
            .await
            .expect("lookup after compaction normalization");
        values.sort_unstable();
        assert_eq!(values, vec![(10, 2), (11, 1)]);
    }

    #[tokio::test]
    async fn adaptive_hot_key_compaction_triggers_at_threshold() {
        let table = build_table("indexed_batch_adaptive_hot").await;
        let index = IndexedBatchZSet::<i64, i64>::with_hot_key_compaction_threshold(
            table.clone(),
            "indexed_batch_adaptive_hot",
            4,
        );

        let updates = vec![
            (1, 100, 1),
            (1, 101, 1),
            (1, 102, 1),
            (1, 103, 1),
            (1, 104, 1),
            (1, 105, 1),
        ];
        index
            .apply_deltas(updates)
            .await
            .expect("apply hot key updates");

        let key_bytes = encode_storage(&1_i64).expect("encode key");
        let l0_entries = table
            .scan_prefix(
                &index
                    .l0_prefix_for_key(&key_bytes)
                    .expect("build key-local L0 prefix"),
                &ScanOptions::default(),
            )
            .await
            .expect("scan key-local L0 entries after adaptive compaction");
        assert!(
            l0_entries.len() < 4,
            "adaptive compaction should cut key-local L0 buildup"
        );

        let mut values = index
            .values_for_key(&1)
            .await
            .expect("lookup after adaptive compaction");
        values.sort_unstable();
        assert_eq!(
            values,
            vec![(100, 1), (101, 1), (102, 1), (103, 1), (104, 1), (105, 1)]
        );
    }

    #[tokio::test]
    async fn adaptive_hot_key_compaction_is_disabled_by_default() {
        let table = build_table("indexed_batch_adaptive_hot_default_off").await;
        let index = IndexedBatchZSet::<i64, i64>::new(
            table.clone(),
            "indexed_batch_adaptive_hot_default_off",
        );

        let updates = vec![
            (1, 200, 1),
            (1, 201, 1),
            (1, 202, 1),
            (1, 203, 1),
            (1, 204, 1),
            (1, 205, 1),
        ];
        index
            .apply_deltas(updates)
            .await
            .expect("apply hot key updates with default compaction policy");

        let key_bytes = encode_storage(&1_i64).expect("encode key");
        let l0_entries = table
            .scan_prefix(
                &index
                    .l0_prefix_for_key(&key_bytes)
                    .expect("build key-local L0 prefix"),
                &ScanOptions::default(),
            )
            .await
            .expect("scan key-local L0 entries");
        assert_eq!(
            l0_entries.len(),
            6,
            "default policy should not trigger key-local compaction in foreground writes"
        );

        let mut values = index
            .values_for_key(&1)
            .await
            .expect("lookup with default compaction policy");
        values.sort_unstable();
        assert_eq!(
            values,
            vec![(200, 1), (201, 1), (202, 1), (203, 1), (204, 1), (205, 1)]
        );
    }

    #[tokio::test]
    async fn payload_writes_and_background_normalization_preserve_semantics() {
        let table = build_table("indexed_batch_payload_normalization_interplay").await;
        let index = IndexedBatchZSet::<i64, i64>::new(
            table.clone(),
            "indexed_batch_payload_normalization_interplay",
        );

        index
            .apply_deltas(vec![(1, 10, 1), (1, 11, 1), (1, 10, -1)])
            .await
            .expect("apply first payload wave");
        let mut first = index
            .values_for_key(&1)
            .await
            .expect("lookup after first payload wave");
        first.sort_unstable();
        assert_eq!(first, vec![(11, 1)]);

        index
            .compact_l0_to_l1()
            .await
            .expect("first compaction pass");

        index
            .apply_deltas(vec![(1, 12, 2), (1, 11, -1), (1, 13, 1)])
            .await
            .expect("apply second payload wave");
        index
            .compact_l0_to_l1()
            .await
            .expect("second compaction pass");

        let mut final_values = index
            .values_for_key(&1)
            .await
            .expect("lookup after normalization passes");
        final_values.sort_unstable();
        assert_eq!(final_values, vec![(12, 2), (13, 1)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_lookup_and_updates_remain_consistent() {
        let table = build_table("indexed_batch_concurrent").await;
        let index = Arc::new(IndexedBatchZSet::<i64, i64>::new(
            table,
            "indexed_batch_concurrent",
        ));
        index
            .apply_deltas(vec![(1, 10, 1)])
            .await
            .expect("seed concurrent test");
        let seeded = index
            .values_for_key(&1)
            .await
            .expect("seed lookup cache for concurrent test");
        let seeded_total: i64 = seeded.iter().map(|(_, weight)| *weight).sum();
        assert_eq!(seeded_total, 1);

        let writer_index = Arc::clone(&index);
        let writer = tokio::spawn(async move {
            let mut ten_active = true;
            for _ in 0..128 {
                let updates = if ten_active {
                    vec![(1, 10, -1), (1, 11, 1)]
                } else {
                    vec![(1, 11, -1), (1, 10, 1)]
                };
                ten_active = !ten_active;
                writer_index
                    .apply_deltas(updates)
                    .await
                    .expect("apply concurrent toggle updates");
            }
        });

        let mut readers = Vec::new();
        for _ in 0..4 {
            let reader_index = Arc::clone(&index);
            readers.push(tokio::spawn(async move {
                for _ in 0..128 {
                    let values = reader_index
                        .values_for_key(&1)
                        .await
                        .expect("concurrent key lookup");
                    let total_weight: i64 = values.iter().map(|(_, weight)| *weight).sum();
                    assert_eq!(total_weight, 1, "toggle invariant should remain stable");
                }
            }));
        }

        writer.await.expect("join writer task");
        for reader in readers {
            reader.await.expect("join reader task");
        }

        let final_values = index
            .values_for_key(&1)
            .await
            .expect("final lookup after concurrency run");
        let total_weight: i64 = final_values.iter().map(|(_, weight)| *weight).sum();
        assert_eq!(total_weight, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_lookup_updates_and_compaction_remain_consistent() {
        let table = build_table("indexed_batch_concurrent_compaction").await;
        let index = Arc::new(IndexedBatchZSet::<i64, i64>::new(
            table,
            "indexed_batch_concurrent_compaction",
        ));
        index
            .apply_deltas(vec![(1, 10, 1)])
            .await
            .expect("seed concurrent compaction test");
        let seeded = index
            .values_for_key(&1)
            .await
            .expect("seed lookup cache for concurrent compaction test");
        let seeded_total: i64 = seeded.iter().map(|(_, weight)| *weight).sum();
        assert_eq!(seeded_total, 1);

        let writer_index = Arc::clone(&index);
        let writer = tokio::spawn(async move {
            let mut ten_active = true;
            for _ in 0..96 {
                let updates = if ten_active {
                    vec![(1, 10, -1), (1, 11, 1)]
                } else {
                    vec![(1, 11, -1), (1, 10, 1)]
                };
                ten_active = !ten_active;
                writer_index
                    .apply_deltas(updates)
                    .await
                    .expect("apply concurrent updates with compaction");
            }
        });

        let compactor_index = Arc::clone(&index);
        let compactor = tokio::spawn(async move {
            for _ in 0..32 {
                compactor_index
                    .compact_l0_to_l1()
                    .await
                    .expect("run compaction during concurrent traffic");
                tokio::task::yield_now().await;
            }
        });

        let mut readers = Vec::new();
        for _ in 0..4 {
            let reader_index = Arc::clone(&index);
            readers.push(tokio::spawn(async move {
                for _ in 0..96 {
                    let values = reader_index
                        .values_for_key(&1)
                        .await
                        .expect("concurrent lookup while compaction runs");
                    let total_weight: i64 = values.iter().map(|(_, weight)| *weight).sum();
                    assert_eq!(total_weight, 1, "toggle invariant should remain stable");
                }
            }));
        }

        writer.await.expect("join writer task");
        compactor.await.expect("join compactor task");
        for reader in readers {
            reader.await.expect("join reader task");
        }

        let final_values = index
            .values_for_key(&1)
            .await
            .expect("final lookup after concurrent compaction");
        let total_weight: i64 = final_values.iter().map(|(_, weight)| *weight).sum();
        assert_eq!(total_weight, 1);
    }
}
