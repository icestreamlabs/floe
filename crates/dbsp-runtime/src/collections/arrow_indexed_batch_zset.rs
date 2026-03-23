use std::hash::Hasher;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use ahash::RandomState;
use anyhow::{Context, Result, anyhow};
use arrow_array::builder::{BinaryBuilder, Int64Builder};
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

use super::indexed_batch_zset::{ApplyDeltaMetrics, RangeKey};

const LOOKUP_CACHE_SHARDS: usize = 64;
const LOOKUP_CACHE_CAPACITY_PER_SHARD: usize = 2048;
const SEGMENT_CACHE_SHARDS: usize = 64;
const SEGMENT_CACHE_CAPACITY_PER_SHARD: usize = 128;

type FastMap<K, V> = FastHashMap<K, V, RandomState>;
type ValueWeightMap = FastMap<Vec<u8>, i64>;

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
    table: Arc<dyn KeyValueTable>,
    segment_store: ArrowSegmentStore,
    schema: SchemaRef,
    index_prefix: Vec<u8>,
    reverse_prefix: Vec<u8>,
    range_prefix: Vec<u8>,
    range_format_key: Vec<u8>,
    segment_sequence_key: Vec<u8>,
    reverse_enabled: bool,
    range_enabled: bool,
    segment_sequence_lock: AsyncMutex<()>,
    lookup_cache_shards: Vec<Mutex<FastMap<Vec<u8>, ValueWeightMap>>>,
    segment_cache_shards: Vec<Mutex<FastMap<u64, Arc<CachedSegment>>>>,
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
        Self::build(table, namespace.into(), false, false)
    }

    pub fn with_reverse_index(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        Self::build(table, namespace.into(), true, false)
    }

    pub fn with_range_index(table: Arc<dyn KeyValueTable>, namespace: impl Into<String>) -> Self {
        Self::build(table, namespace.into(), false, true)
    }

    pub fn with_hot_key_compaction_threshold(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        _threshold: usize,
    ) -> Self {
        Self::new(table, namespace)
    }

    pub fn engine_kind(&self) -> &'static str {
        "indexed_batch"
    }

    fn build(
        table: Arc<dyn KeyValueTable>,
        namespace: String,
        reverse_enabled: bool,
        range_enabled: bool,
    ) -> Self {
        let mut base = b"indexed_batch_arrow/".to_vec();
        base.extend_from_slice(namespace.as_bytes());
        base.push(b'/');

        let mut index_prefix = base.clone();
        index_prefix.extend_from_slice(b"idx/");

        let mut reverse_prefix = base.clone();
        reverse_prefix.extend_from_slice(b"rev/");

        let mut range_prefix = base.clone();
        range_prefix.extend_from_slice(b"rng/");

        let mut range_format_key = base.clone();
        range_format_key.extend_from_slice(b"range_format");

        let mut segment_sequence_key = base;
        segment_sequence_key.extend_from_slice(b"next_segment_id");

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, false),
            Field::new("value", DataType::Binary, false),
            Field::new("delta", DataType::Int64, false),
        ]));

        Self {
            segment_store: ArrowSegmentStore::new(
                table.clone(),
                format!("indexed_batch_arrow/{namespace}"),
            ),
            table,
            schema,
            index_prefix,
            reverse_prefix,
            range_prefix,
            range_format_key,
            segment_sequence_key,
            reverse_enabled,
            range_enabled,
            segment_sequence_lock: AsyncMutex::new(()),
            lookup_cache_shards: make_mutex_shards(LOOKUP_CACHE_SHARDS),
            segment_cache_shards: make_mutex_shards(SEGMENT_CACHE_SHARDS),
            _marker: PhantomData,
        }
    }

    pub async fn apply_deltas<I>(&self, deltas: I) -> Result<()>
    where
        I: IntoIterator<Item = (K, V, i64)>,
    {
        self.apply_deltas_with_stats(deltas).await.map(|_| ())
    }

    pub async fn apply_deltas_with_stats<I>(&self, deltas: I) -> Result<ApplyDeltaMetrics>
    where
        I: IntoIterator<Item = (K, V, i64)>,
    {
        self.apply_deltas_internal(deltas).await
    }

    pub async fn apply_deltas_with_range<I>(&self, deltas: I) -> Result<()>
    where
        K: RangeKey,
        I: IntoIterator<Item = (K, V, i64)>,
    {
        self.apply_deltas_with_range_stats(deltas).await.map(|_| ())
    }

    pub async fn apply_deltas_with_range_stats<I>(&self, deltas: I) -> Result<ApplyDeltaMetrics>
    where
        K: RangeKey,
        I: IntoIterator<Item = (K, V, i64)>,
    {
        if !self.range_enabled {
            return Err(anyhow!("range index not enabled"));
        }
        self.apply_deltas_internal_with_range(deltas).await
    }

    async fn apply_deltas_internal<I>(&self, deltas: I) -> Result<ApplyDeltaMetrics>
    where
        I: IntoIterator<Item = (K, V, i64)>,
    {
        let mut metrics = ApplyDeltaMetrics::default();
        let mut encoded_rows: Vec<(Vec<u8>, Vec<u8>, i64)> = Vec::new();
        let mut touched_updates: FastMap<Vec<u8>, ValueWeightMap> = FastMap::default();
        let mut key_postings: FastMap<Vec<u8>, Vec<(u32, i64)>> = FastMap::default();
        let mut reverse_postings: FastMap<Vec<u8>, Vec<(Vec<u8>, i64)>> = FastMap::default();
        let mut min_hash = u64::MAX;
        let mut max_hash = 0_u64;
        let mut tombstones = 0_usize;

        for (key, value, delta) in deltas {
            metrics.input_records = metrics.input_records.saturating_add(1);
            if delta == 0 {
                continue;
            }
            metrics.non_zero_input_records = metrics.non_zero_input_records.saturating_add(1);

            let key_bytes = encode(&key).context("encode Arrow-index key")?;
            let value_bytes = encode(&value).context("encode Arrow-index value")?;
            let row_index = u32::try_from(encoded_rows.len())
                .map_err(|_| anyhow!("row index overflow while indexing segment rows"))?;

            key_postings
                .entry(key_bytes.clone())
                .or_default()
                .push((row_index, delta));
            if self.reverse_enabled {
                reverse_postings
                    .entry(value_bytes.clone())
                    .or_default()
                    .push((key_bytes.clone(), delta));
            }

            let key_updates = touched_updates.entry(key_bytes.clone()).or_default();
            *key_updates.entry(value_bytes.clone()).or_insert(0) += delta;

            let key_hash = hash_bytes(&key_bytes);
            min_hash = min_hash.min(key_hash);
            max_hash = max_hash.max(key_hash);
            if delta < 0 {
                tombstones = tombstones.saturating_add(1);
            }
            encoded_rows.push((key_bytes, value_bytes, delta));
        }

        if encoded_rows.is_empty() {
            return Ok(metrics);
        }

        for updates in touched_updates.values_mut() {
            updates.retain(|_, weight| *weight != 0);
        }

        let batch = self.record_batch_from_rows(&encoded_rows)?;
        let tombstone_ratio = tombstones as f64 / encoded_rows.len() as f64;
        let stats = SegmentWriteStats::new(min_hash, max_hash, tombstone_ratio)
            .context("build Arrow-index segment stats")?;
        let (segment_bytes, _) = encode_segment_envelope(Arc::clone(&self.schema), &[batch], stats)
            .context("encode Arrow-index segment envelope")?;

        let _segment_guard = self.segment_sequence_lock.lock().await;
        let mut write_batch = WriteBatch::new();
        let segment_id = self.read_next_segment_id().await?;
        write_batch.put(
            self.segment_sequence_key.clone(),
            segment_id.saturating_add(1).to_be_bytes(),
        );

        write_batch.put(
            self.segment_store.key_for_segment(segment_id),
            segment_bytes,
        );
        for (key_bytes, postings) in key_postings {
            let key = self
                .index_key(&key_bytes, segment_id)
                .context("build Arrow-index key")?;
            let value = encode_index_postings(&postings);
            write_batch.put(key, value);
        }

        if self.reverse_enabled {
            for (value_bytes, postings) in reverse_postings {
                let key = self
                    .reverse_key(&value_bytes, segment_id)
                    .context("build Arrow-index reverse key")?;
                let value = encode_reverse_postings(&postings)?;
                write_batch.put(key, value);
            }
        }

        self.table
            .write_batch(write_batch)
            .await
            .context("persist Arrow-index segment and postings")?;

        self.insert_segment_cache(
            segment_id,
            Arc::new(CachedSegment {
                values: encoded_rows
                    .iter()
                    .map(|(_, value_bytes, _)| value_bytes.clone())
                    .collect(),
            }),
        )?;
        self.apply_lookup_cache_updates(&touched_updates)?;

        metrics.coalesced_records = metrics.non_zero_input_records;
        metrics.persisted_records = encoded_rows.len();
        Ok(metrics)
    }

    async fn apply_deltas_internal_with_range<I>(&self, deltas: I) -> Result<ApplyDeltaMetrics>
    where
        K: RangeKey,
        I: IntoIterator<Item = (K, V, i64)>,
    {
        let mut metrics = ApplyDeltaMetrics::default();
        let mut encoded_rows: Vec<(Vec<u8>, Vec<u8>, i64)> = Vec::new();
        let mut touched_updates: FastMap<Vec<u8>, ValueWeightMap> = FastMap::default();
        let mut key_postings: FastMap<Vec<u8>, Vec<(u32, i64)>> = FastMap::default();
        let mut range_postings: FastMap<(Vec<u8>, Vec<u8>), Vec<(u32, i64)>> = FastMap::default();
        let mut reverse_postings: FastMap<Vec<u8>, Vec<(Vec<u8>, i64)>> = FastMap::default();
        let mut min_hash = u64::MAX;
        let mut max_hash = 0_u64;
        let mut tombstones = 0_usize;

        for (key, value, delta) in deltas {
            metrics.input_records = metrics.input_records.saturating_add(1);
            if delta == 0 {
                continue;
            }
            metrics.non_zero_input_records = metrics.non_zero_input_records.saturating_add(1);

            let key_bytes = encode(&key).context("encode Arrow-index lookup key")?;
            let range_key_bytes = key.encode_range_key();
            let value_bytes = encode(&value).context("encode Arrow-index value")?;
            let row_index = u32::try_from(encoded_rows.len())
                .map_err(|_| anyhow!("row index overflow while indexing segment rows"))?;

            key_postings
                .entry(key_bytes.clone())
                .or_default()
                .push((row_index, delta));
            range_postings
                .entry((range_key_bytes, key_bytes.clone()))
                .or_default()
                .push((row_index, delta));
            if self.reverse_enabled {
                reverse_postings
                    .entry(value_bytes.clone())
                    .or_default()
                    .push((key_bytes.clone(), delta));
            }

            let key_updates = touched_updates.entry(key_bytes.clone()).or_default();
            *key_updates.entry(value_bytes.clone()).or_insert(0) += delta;

            let key_hash = hash_bytes(&key_bytes);
            min_hash = min_hash.min(key_hash);
            max_hash = max_hash.max(key_hash);
            if delta < 0 {
                tombstones = tombstones.saturating_add(1);
            }
            encoded_rows.push((key_bytes, value_bytes, delta));
        }

        if encoded_rows.is_empty() {
            return Ok(metrics);
        }

        for updates in touched_updates.values_mut() {
            updates.retain(|_, weight| *weight != 0);
        }

        let batch = self.record_batch_from_rows(&encoded_rows)?;
        let tombstone_ratio = tombstones as f64 / encoded_rows.len() as f64;
        let stats = SegmentWriteStats::new(min_hash, max_hash, tombstone_ratio)
            .context("build Arrow-index segment stats")?;
        let (segment_bytes, _) = encode_segment_envelope(Arc::clone(&self.schema), &[batch], stats)
            .context("encode Arrow-index segment envelope")?;

        let _segment_guard = self.segment_sequence_lock.lock().await;
        let mut write_batch = WriteBatch::new();
        let segment_id = self.read_next_segment_id().await?;
        write_batch.put(
            self.segment_sequence_key.clone(),
            segment_id.saturating_add(1).to_be_bytes(),
        );
        write_batch.put(self.range_format_key.clone(), b"v2".to_vec());

        write_batch.put(
            self.segment_store.key_for_segment(segment_id),
            segment_bytes,
        );
        for (key_bytes, postings) in key_postings {
            let key = self
                .index_key(&key_bytes, segment_id)
                .context("build Arrow-index key")?;
            let value = encode_index_postings(&postings);
            write_batch.put(key, value);
        }
        for ((range_key_bytes, key_bytes), postings) in range_postings {
            let key = self
                .range_key(&range_key_bytes, &key_bytes, segment_id)
                .context("build Arrow-index range key")?;
            let value = encode_index_postings(&postings);
            write_batch.put(key, value);
        }

        if self.reverse_enabled {
            for (value_bytes, postings) in reverse_postings {
                let key = self
                    .reverse_key(&value_bytes, segment_id)
                    .context("build Arrow-index reverse key")?;
                let value = encode_reverse_postings(&postings)?;
                write_batch.put(key, value);
            }
        }

        self.table
            .write_batch(write_batch)
            .await
            .context("persist Arrow-index segment and postings")?;

        self.insert_segment_cache(
            segment_id,
            Arc::new(CachedSegment {
                values: encoded_rows
                    .iter()
                    .map(|(_, value_bytes, _)| value_bytes.clone())
                    .collect(),
            }),
        )?;
        self.apply_lookup_cache_updates(&touched_updates)?;

        metrics.coalesced_records = metrics.non_zero_input_records;
        metrics.persisted_records = encoded_rows.len();
        Ok(metrics)
    }

    pub async fn values_for_key(&self, key: &K) -> Result<Vec<(V, i64)>> {
        let key_bytes = encode(key).context("encode Arrow-index lookup key")?;
        if let Some(cached) = self.lookup_cache_for_key(&key_bytes)? {
            return self.decode_value_weights(cached);
        }

        let refs = self.segment_refs_for_key(&key_bytes).await?;
        let mut aggregate: ValueWeightMap = FastMap::default();

        for (segment_id, postings) in refs {
            let segment = self
                .segment_for_id(segment_id)
                .await
                .with_context(|| format!("load cached Arrow-index segment {segment_id}"))?;
            for (row_index, delta) in postings {
                let value_bytes = segment
                    .value_bytes(row_index)
                    .with_context(|| {
                        format!("load row {row_index} from Arrow-index segment {segment_id}")
                    })?
                    .to_vec();
                let next = aggregate
                    .get(&value_bytes)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(delta);
                if next == 0 {
                    aggregate.remove(&value_bytes);
                } else {
                    aggregate.insert(value_bytes, next);
                }
            }
        }

        self.store_lookup_cache_for_key(&key_bytes, &aggregate)?;
        self.decode_value_weights(aggregate)
    }

    pub async fn value_weight_for_key_value(&self, key: &K, value: &V) -> Result<i64> {
        let key_bytes = encode(key).context("encode Arrow-index lookup key")?;
        let value_bytes = encode(value).context("encode Arrow-index lookup value")?;
        if let Some(cached) = self.lookup_cache_for_key(&key_bytes)? {
            return Ok(cached.get(&value_bytes).copied().unwrap_or(0));
        }

        let refs = self.segment_refs_for_key(&key_bytes).await?;
        let mut aggregate: ValueWeightMap = FastMap::default();

        for (segment_id, postings) in refs {
            let segment = self
                .segment_for_id(segment_id)
                .await
                .with_context(|| format!("load cached Arrow-index segment {segment_id}"))?;
            for (row_index, delta) in postings {
                let value_bytes = segment
                    .value_bytes(row_index)
                    .with_context(|| {
                        format!("load row {row_index} from Arrow-index segment {segment_id}")
                    })?
                    .to_vec();
                let next = aggregate
                    .get(&value_bytes)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(delta);
                if next == 0 {
                    aggregate.remove(&value_bytes);
                } else {
                    aggregate.insert(value_bytes, next);
                }
            }
        }

        let weight = aggregate.get(&value_bytes).copied().unwrap_or(0);
        self.store_lookup_cache_for_key(&key_bytes, &aggregate)?;
        Ok(weight)
    }

    pub async fn keys_for_value(&self, value: &V) -> Result<Vec<(K, i64)>> {
        if !self.reverse_enabled {
            return Err(anyhow!("reverse index not enabled"));
        }

        let value_bytes = encode(value).context("encode Arrow-index reverse lookup value")?;
        let refs = self.segment_refs_for_value(&value_bytes).await?;
        let mut aggregate: FastMap<Vec<u8>, i64> = FastMap::default();

        for key_deltas in refs.into_values() {
            for (key_bytes, delta) in key_deltas {
                let next = aggregate
                    .get(&key_bytes)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(delta);
                if next == 0 {
                    aggregate.remove(&key_bytes);
                } else {
                    aggregate.insert(key_bytes, next);
                }
            }
        }

        let mut keys = Vec::with_capacity(aggregate.len());
        for (key_bytes, weight) in aggregate {
            let key = decode::<K>(&key_bytes).context("decode Arrow-index key bytes")?;
            keys.push((key, weight));
        }
        Ok(keys)
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
        let range_format = self
            .table
            .get_bytes(&self.range_format_key)
            .await
            .context("read Arrow-index range format marker")?;
        if range_format.is_none() {
            let legacy_entries = self
                .table
                .scan_prefix(&self.index_prefix, &ScanOptions::default())
                .await
                .context("scan legacy Arrow-index key prefix for range compatibility")?;
            if !legacy_entries.is_empty() {
                return Err(anyhow!(
                    "range index namespace is on legacy layout; rebuild/replay is required"
                ));
            }
        }

        let mut refs_by_key: FastMap<Vec<u8>, FastMap<u64, Vec<(u32, i64)>>> = FastMap::default();
        for (entry_key, entry_value) in self
            .table
            .scan_range_bytes(
                self.range_bounds(&lower_bytes, &upper_bytes)?,
                &ScanOptions::default(),
            )
            .await
            .context("scan Arrow-index range postings")?
        {
            let (key_bytes, segment_id) = self
                .decode_range_key::<K>(&entry_key)
                .context("decode Arrow-index range posting key")?;
            refs_by_key
                .entry(key_bytes)
                .or_default()
                .entry(segment_id)
                .or_default()
                .extend(decode_index_postings(&entry_value)?);
        }

        let mut output = Vec::new();
        for (key_bytes, refs) in refs_by_key {
            let mut aggregate: ValueWeightMap = FastMap::default();
            for (segment_id, postings) in refs {
                let segment = self
                    .segment_for_id(segment_id)
                    .await
                    .with_context(|| format!("load cached Arrow-index segment {segment_id}"))?;
                for (row_index, delta) in postings {
                    let value_bytes = segment
                        .value_bytes(row_index)
                        .with_context(|| {
                            format!("load row {row_index} from Arrow-index segment {segment_id}")
                        })?
                        .to_vec();
                    let next = aggregate
                        .get(&value_bytes)
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(delta);
                    if next == 0 {
                        aggregate.remove(&value_bytes);
                    } else {
                        aggregate.insert(value_bytes, next);
                    }
                }
            }

            let key =
                decode::<K>(&key_bytes).context("decode Arrow-index key for range lookup rows")?;
            for (value, weight) in self.decode_value_weights(aggregate)? {
                output.push((key.clone(), value, weight));
            }
        }
        Ok(output)
    }

    pub async fn entries(&self) -> Result<Vec<(K, V, i64)>> {
        let segment_ids = self
            .segment_store
            .list_segment_ids()
            .await
            .context("list Arrow-index segments")?;
        let mut aggregate: FastMap<(Vec<u8>, Vec<u8>), i64> = FastMap::default();

        for segment_id in segment_ids {
            let Some(segment) = self
                .segment_store
                .read_segment(segment_id)
                .await
                .with_context(|| format!("read Arrow-index segment {segment_id}"))?
            else {
                continue;
            };

            for batch in &segment.batches {
                let key_col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| anyhow!("invalid Arrow-index key column type"))?;
                let value_col = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| anyhow!("invalid Arrow-index value column type"))?;
                let delta_col = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| anyhow!("invalid Arrow-index delta column type"))?;

                for idx in 0..batch.num_rows() {
                    let key_bytes = key_col.value(idx).to_vec();
                    let value_bytes = value_col.value(idx).to_vec();
                    let delta = delta_col.value(idx);
                    let next = aggregate
                        .get(&(key_bytes.clone(), value_bytes.clone()))
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(delta);
                    if next == 0 {
                        aggregate.remove(&(key_bytes, value_bytes));
                    } else {
                        aggregate.insert((key_bytes, value_bytes), next);
                    }
                }
            }
        }

        let mut out = Vec::with_capacity(aggregate.len());
        for ((key_bytes, value_bytes), weight) in aggregate {
            let key = decode::<K>(&key_bytes).context("decode key bytes while listing entries")?;
            let value =
                decode::<V>(&value_bytes).context("decode value bytes while listing entries")?;
            out.push((key, value, weight));
        }
        Ok(out)
    }

    pub async fn compact_l0_to_l1(&self) -> Result<usize> {
        Ok(0)
    }

    pub async fn estimated_read_amplification_for_key(&self, key: &K) -> Result<usize> {
        let key_bytes = encode(key).context("encode key for Arrow-index amplification estimate")?;
        let entries = self
            .table
            .scan_prefix(
                &self
                    .index_prefix_for_key(&key_bytes)
                    .context("build Arrow-index key prefix for amplification")?,
                &ScanOptions::default(),
            )
            .await
            .context("scan Arrow-index entries for amplification estimate")?;
        Ok(entries.len())
    }

    async fn segment_refs_for_key(
        &self,
        key_bytes: &[u8],
    ) -> Result<FastMap<u64, Vec<(u32, i64)>>> {
        let entries = self
            .table
            .scan_prefix(
                &self
                    .index_prefix_for_key(key_bytes)
                    .context("build Arrow-index key prefix")?,
                &ScanOptions::default(),
            )
            .await
            .context("scan Arrow-index key prefix")?;

        let mut refs: FastMap<u64, Vec<(u32, i64)>> = FastMap::default();
        for (entry_key, entry_value) in entries {
            let (_key, segment_id) = self
                .decode_index_key(&entry_key)
                .context("decode Arrow-index key")?;
            refs.entry(segment_id)
                .or_default()
                .extend(decode_index_postings(&entry_value)?);
        }
        Ok(refs)
    }

    async fn segment_refs_for_value(
        &self,
        value_bytes: &[u8],
    ) -> Result<FastMap<u64, Vec<(Vec<u8>, i64)>>> {
        let entries = self
            .table
            .scan_prefix(
                &self
                    .reverse_prefix_for_value(value_bytes)
                    .context("build Arrow-index reverse prefix")?,
                &ScanOptions::default(),
            )
            .await
            .context("scan Arrow-index reverse prefix")?;

        let mut refs: FastMap<u64, Vec<(Vec<u8>, i64)>> = FastMap::default();
        for (entry_key, entry_value) in entries {
            let (_value, segment_id) = self
                .decode_reverse_key(&entry_key)
                .context("decode Arrow-index reverse key")?;
            refs.entry(segment_id)
                .or_default()
                .extend(decode_reverse_postings(&entry_value)?);
        }
        Ok(refs)
    }

    async fn read_next_segment_id(&self) -> Result<u64> {
        match self
            .table
            .get(&self.segment_sequence_key)
            .await
            .context("read Arrow-index next segment id")?
        {
            Some(bytes) => decode_u64_payload(&bytes),
            None => Ok(1),
        }
    }

    fn record_batch_from_rows(&self, rows: &[(Vec<u8>, Vec<u8>, i64)]) -> Result<RecordBatch> {
        let mut key_builder = BinaryBuilder::new();
        let mut value_builder = BinaryBuilder::new();
        let mut delta_builder = Int64Builder::new();

        for (key, value, delta) in rows {
            key_builder.append_value(key);
            value_builder.append_value(value);
            delta_builder.append_value(*delta);
        }

        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(key_builder.finish()) as ArrayRef,
                Arc::new(value_builder.finish()) as ArrayRef,
                Arc::new(delta_builder.finish()) as ArrayRef,
            ],
        )
        .context("build Arrow-index record batch")
    }

    fn index_prefix_for_key(&self, key_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut prefix = self.index_prefix.clone();
        prefix.extend_from_slice(&encode_len(key_bytes.len())?);
        prefix.extend_from_slice(key_bytes);
        Ok(prefix)
    }

    fn reverse_prefix_for_value(&self, value_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut prefix = self.reverse_prefix.clone();
        prefix.extend_from_slice(&encode_len(value_bytes.len())?);
        prefix.extend_from_slice(value_bytes);
        Ok(prefix)
    }

    fn index_key(&self, key_bytes: &[u8], segment_id: u64) -> Result<Vec<u8>> {
        let mut key = self.index_prefix_for_key(key_bytes)?;
        key.extend_from_slice(&segment_id.to_be_bytes());
        Ok(key)
    }

    fn reverse_key(&self, value_bytes: &[u8], segment_id: u64) -> Result<Vec<u8>> {
        let mut key = self.reverse_prefix_for_value(value_bytes)?;
        key.extend_from_slice(&segment_id.to_be_bytes());
        Ok(key)
    }

    fn range_key(
        &self,
        range_key_bytes: &[u8],
        key_bytes: &[u8],
        segment_id: u64,
    ) -> Result<Vec<u8>> {
        let mut key = self.range_prefix.clone();
        key.extend_from_slice(range_key_bytes);
        key.extend_from_slice(&encode_len(key_bytes.len())?);
        key.extend_from_slice(key_bytes);
        key.extend_from_slice(&segment_id.to_be_bytes());
        Ok(key)
    }

    fn range_bounds(&self, lower: &[u8], upper: &[u8]) -> Result<Range<Vec<u8>>> {
        if lower >= upper {
            return Err(anyhow!("invalid Arrow-index range bounds"));
        }
        let mut start = self.range_prefix.clone();
        start.extend_from_slice(lower);
        let mut end = self.range_prefix.clone();
        end.extend_from_slice(upper);
        Ok(start..end)
    }

    fn decode_index_key(&self, key: &[u8]) -> Result<(Vec<u8>, u64)> {
        if !key.starts_with(&self.index_prefix) {
            return Err(anyhow!("Arrow-index key missing prefix"));
        }
        let mut cursor = self.index_prefix.len();
        let key_len = read_len(key, &mut cursor)?;
        let key_end = cursor
            .checked_add(key_len)
            .ok_or_else(|| anyhow!("Arrow-index key length overflow"))?;
        let key_bytes = key
            .get(cursor..key_end)
            .ok_or_else(|| anyhow!("Arrow-index key truncated"))?
            .to_vec();
        cursor = key_end;

        let segment_bytes = key
            .get(cursor..cursor + 8)
            .ok_or_else(|| anyhow!("Arrow-index key missing segment id"))?;
        cursor += 8;
        if cursor != key.len() {
            return Err(anyhow!("Arrow-index key has trailing bytes"));
        }

        Ok((
            key_bytes,
            u64::from_be_bytes(segment_bytes.try_into().unwrap()),
        ))
    }

    fn decode_reverse_key(&self, key: &[u8]) -> Result<(Vec<u8>, u64)> {
        if !key.starts_with(&self.reverse_prefix) {
            return Err(anyhow!("Arrow-index reverse key missing prefix"));
        }
        let mut cursor = self.reverse_prefix.len();
        let value_len = read_len(key, &mut cursor)?;
        let value_end = cursor
            .checked_add(value_len)
            .ok_or_else(|| anyhow!("Arrow-index reverse value length overflow"))?;
        let value_bytes = key
            .get(cursor..value_end)
            .ok_or_else(|| anyhow!("Arrow-index reverse value truncated"))?
            .to_vec();
        cursor = value_end;

        let segment_bytes = key
            .get(cursor..cursor + 8)
            .ok_or_else(|| anyhow!("Arrow-index reverse key missing segment id"))?;
        cursor += 8;
        if cursor != key.len() {
            return Err(anyhow!("Arrow-index reverse key has trailing bytes"));
        }

        Ok((
            value_bytes,
            u64::from_be_bytes(segment_bytes.try_into().unwrap()),
        ))
    }

    fn decode_range_key<T>(&self, key: &[u8]) -> Result<(Vec<u8>, u64)>
    where
        T: RangeKey,
    {
        if !key.starts_with(&self.range_prefix) {
            return Err(anyhow!("Arrow-index range key missing prefix"));
        }
        let mut cursor = self.range_prefix.len();
        let range_len =
            T::encoded_len(&key[cursor..]).context("decode Arrow-index range key length")?;
        cursor = cursor
            .checked_add(range_len)
            .ok_or_else(|| anyhow!("Arrow-index range key length overflow"))?;

        let key_len = read_len(key, &mut cursor)?;
        let key_end = cursor
            .checked_add(key_len)
            .ok_or_else(|| anyhow!("Arrow-index range payload length overflow"))?;
        let key_bytes = key
            .get(cursor..key_end)
            .ok_or_else(|| anyhow!("Arrow-index range key payload truncated"))?
            .to_vec();
        cursor = key_end;

        let segment_bytes = key
            .get(cursor..cursor + 8)
            .ok_or_else(|| anyhow!("Arrow-index range key missing segment id"))?;
        cursor += 8;
        if cursor != key.len() {
            return Err(anyhow!("Arrow-index range key has trailing bytes"));
        }

        Ok((
            key_bytes,
            u64::from_be_bytes(segment_bytes.try_into().unwrap()),
        ))
    }

    fn lookup_cache_for_key(&self, key_bytes: &[u8]) -> Result<Option<ValueWeightMap>> {
        let shard = shard_for_bytes(key_bytes, self.lookup_cache_shards.len());
        let guard = self.lookup_cache_shards[shard]
            .lock()
            .map_err(|_| anyhow!("Arrow-index lookup cache shard poisoned"))?;
        Ok(guard.get(key_bytes).cloned())
    }

    fn store_lookup_cache_for_key(&self, key_bytes: &[u8], state: &ValueWeightMap) -> Result<()> {
        let shard = shard_for_bytes(key_bytes, self.lookup_cache_shards.len());
        let mut guard = self.lookup_cache_shards[shard]
            .lock()
            .map_err(|_| anyhow!("Arrow-index lookup cache shard poisoned"))?;
        if guard.len() >= LOOKUP_CACHE_CAPACITY_PER_SHARD
            && !guard.contains_key(key_bytes)
            && let Some(evict_key) = guard.keys().next().cloned()
        {
            guard.remove(&evict_key);
        }
        guard.insert(key_bytes.to_vec(), state.clone());
        Ok(())
    }

    fn apply_lookup_cache_updates(&self, updates: &FastMap<Vec<u8>, ValueWeightMap>) -> Result<()> {
        for (key_bytes, key_updates) in updates {
            let shard = shard_for_bytes(key_bytes, self.lookup_cache_shards.len());
            let mut guard = self.lookup_cache_shards[shard]
                .lock()
                .map_err(|_| anyhow!("Arrow-index lookup cache shard poisoned"))?;
            let Some(state) = guard.get_mut(key_bytes) else {
                continue;
            };
            for (value_bytes, delta) in key_updates {
                let next = state
                    .get(value_bytes)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(*delta);
                if next == 0 {
                    state.remove(value_bytes);
                } else {
                    state.insert(value_bytes.clone(), next);
                }
            }
            if state.is_empty() {
                guard.remove(key_bytes);
            }
        }
        Ok(())
    }

    async fn segment_for_id(&self, segment_id: u64) -> Result<Arc<CachedSegment>> {
        if let Some(cached) = self.cached_segment_for_id(segment_id)? {
            return Ok(cached);
        }

        let Some(segment) = self
            .segment_store
            .read_segment(segment_id)
            .await
            .with_context(|| format!("read Arrow-index segment {segment_id}"))?
        else {
            return Err(anyhow!("missing Arrow-index segment {segment_id}"));
        };

        let mut values = Vec::new();
        for batch in &segment.batches {
            let value_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| anyhow!("invalid Arrow-index value column type"))?;
            for row in 0..batch.num_rows() {
                values.push(value_col.value(row).to_vec());
            }
        }

        let cached = Arc::new(CachedSegment { values });
        self.insert_segment_cache(segment_id, Arc::clone(&cached))?;
        Ok(cached)
    }

    fn cached_segment_for_id(&self, segment_id: u64) -> Result<Option<Arc<CachedSegment>>> {
        let shard = self.segment_cache_shard(segment_id);
        let guard = self.segment_cache_shards[shard]
            .lock()
            .map_err(|_| anyhow!("Arrow-index segment cache shard poisoned"))?;
        Ok(guard.get(&segment_id).cloned())
    }

    fn insert_segment_cache(&self, segment_id: u64, segment: Arc<CachedSegment>) -> Result<()> {
        let shard = self.segment_cache_shard(segment_id);
        let mut guard = self.segment_cache_shards[shard]
            .lock()
            .map_err(|_| anyhow!("Arrow-index segment cache shard poisoned"))?;
        if guard.len() >= SEGMENT_CACHE_CAPACITY_PER_SHARD
            && !guard.contains_key(&segment_id)
            && let Some(evict_key) = guard.keys().next().copied()
        {
            guard.remove(&evict_key);
        }
        guard.insert(segment_id, segment);
        Ok(())
    }

    fn segment_cache_shard(&self, segment_id: u64) -> usize {
        (segment_id as usize) % self.segment_cache_shards.len()
    }

    fn decode_value_weights(&self, aggregate: ValueWeightMap) -> Result<Vec<(V, i64)>> {
        let mut values = Vec::with_capacity(aggregate.len());
        for (value_bytes, weight) in aggregate {
            if weight == 0 {
                continue;
            }
            let value = decode::<V>(&value_bytes).context("decode Arrow-index value bytes")?;
            values.push((value, weight));
        }
        Ok(values)
    }
}

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
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| anyhow!("Arrow-index length overflow"))?;
    let chunk = bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow!("Arrow-index length truncated"))?;
    *cursor = end;
    Ok(u32::from_be_bytes(chunk.try_into().unwrap()) as usize)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| anyhow!("Arrow-index u32 overflow"))?;
    let chunk = bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow!("Arrow-index u32 truncated"))?;
    *cursor = end;
    Ok(u32::from_be_bytes(chunk.try_into().unwrap()))
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| anyhow!("Arrow-index i64 overflow"))?;
    let chunk = bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow!("Arrow-index i64 truncated"))?;
    *cursor = end;
    Ok(i64::from_be_bytes(chunk.try_into().unwrap()))
}

fn decode_u64_payload(bytes: &[u8]) -> Result<u64> {
    let chunk = bytes
        .get(0..8)
        .ok_or_else(|| anyhow!("expected 8 bytes for Arrow-index u64 payload"))?;
    Ok(u64::from_be_bytes(chunk.try_into().unwrap()))
}

#[cfg(test)]
mod tests;
