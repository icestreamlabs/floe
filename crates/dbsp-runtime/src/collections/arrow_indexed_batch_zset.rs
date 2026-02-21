use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow_array::builder::{BinaryBuilder, Int64Builder};
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;
use tokio::sync::Mutex as AsyncMutex;

use crate::storage::KeyValueTable;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator, decode, encode};
use crate::storage::segment::{ArrowSegmentStore, SegmentWriteStats};

use super::indexed_batch_zset::{ApplyDeltaMetrics, RangeKey};

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
    segment_store: ArrowSegmentStore,
    schema: SchemaRef,
    index_prefix: Vec<u8>,
    reverse_prefix: Vec<u8>,
    segment_sequence_key: Vec<u8>,
    reverse_enabled: bool,
    range_enabled: bool,
    segment_sequence_lock: AsyncMutex<()>,
    _marker: PhantomData<(K, V)>,
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
            segment_sequence_key,
            reverse_enabled,
            range_enabled,
            segment_sequence_lock: AsyncMutex::new(()),
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
        self.apply_deltas_internal(deltas).await
    }

    async fn apply_deltas_internal<I>(&self, deltas: I) -> Result<ApplyDeltaMetrics>
    where
        I: IntoIterator<Item = (K, V, i64)>,
    {
        let mut metrics = ApplyDeltaMetrics::default();
        let mut encoded_rows = Vec::new();
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

        let segment_id = self.next_segment_id().await?;
        let batch = self.record_batch_from_rows(&encoded_rows)?;
        let tombstone_ratio = tombstones as f64 / encoded_rows.len() as f64;
        self.segment_store
            .write_segment(
                segment_id,
                Arc::clone(&self.schema),
                &[batch],
                SegmentWriteStats::new(min_hash, max_hash, tombstone_ratio)
                    .context("build Arrow-index segment stats")?,
            )
            .await
            .with_context(|| format!("write Arrow-index segment {segment_id}"))?;

        let mut write_batch = WriteBatch::new();
        for (row_index, (key_bytes, value_bytes, _)) in encoded_rows.iter().enumerate() {
            let row_index = u32::try_from(row_index)
                .map_err(|_| anyhow!("row index overflow while indexing segment rows"))?;
            let key = self
                .index_key(key_bytes, segment_id, row_index)
                .context("build Arrow-index key")?;
            write_batch.put(key, Vec::new());

            if self.reverse_enabled {
                let reverse_key = self
                    .reverse_key(value_bytes, key_bytes, segment_id, row_index)
                    .context("build Arrow-index reverse key")?;
                write_batch.put(reverse_key, Vec::new());
            }
        }

        self.table
            .write_batch(write_batch)
            .await
            .context("persist Arrow-index secondary keys")?;

        metrics.coalesced_records = metrics.non_zero_input_records;
        metrics.persisted_records = encoded_rows.len();
        Ok(metrics)
    }

    pub async fn values_for_key(&self, key: &K) -> Result<Vec<(V, i64)>> {
        let key_bytes = encode(key).context("encode Arrow-index lookup key")?;
        let refs = self.segment_refs_for_key(&key_bytes).await?;

        let mut aggregate = HashMap::<Vec<u8>, i64>::new();
        for (segment_id, row_indexes) in refs {
            let segment = self
                .segment_store
                .read_segment(segment_id)
                .await
                .with_context(|| format!("read Arrow-index segment {segment_id}"))?
                .ok_or_else(|| anyhow!("missing Arrow-index segment {segment_id}"))?;

            for row_index in row_indexes {
                let (_row_key, row_value, row_delta) = row_for_index(&segment.batches, row_index)
                    .with_context(|| {
                    format!("read Arrow-index row {row_index} from segment {segment_id}")
                })?;
                *aggregate.entry(row_value).or_insert(0) += row_delta;
            }
        }

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

    pub async fn keys_for_value(&self, value: &V) -> Result<Vec<(K, i64)>> {
        if !self.reverse_enabled {
            return Err(anyhow!("reverse index not enabled"));
        }

        let value_bytes = encode(value).context("encode Arrow-index reverse lookup value")?;
        let refs = self.segment_refs_for_value(&value_bytes).await?;

        let mut aggregate = HashMap::<Vec<u8>, i64>::new();
        for (segment_id, key_row_refs) in refs {
            let segment = self
                .segment_store
                .read_segment(segment_id)
                .await
                .with_context(|| format!("read Arrow-index segment {segment_id}"))?
                .ok_or_else(|| anyhow!("missing Arrow-index segment {segment_id}"))?;

            for (key_bytes, row_index) in key_row_refs {
                let (_row_key, _row_value, row_delta) = row_for_index(&segment.batches, row_index)
                    .with_context(|| {
                        format!("read Arrow-index row {row_index} from segment {segment_id}")
                    })?;
                *aggregate.entry(key_bytes).or_insert(0) += row_delta;
            }
        }

        let mut keys = Vec::with_capacity(aggregate.len());
        for (key_bytes, weight) in aggregate {
            if weight == 0 {
                continue;
            }
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

        let mut unique_keys = HashSet::new();
        let entries = self
            .table
            .scan_prefix(&self.index_prefix, &ScanOptions::default())
            .await
            .context("scan Arrow-index entries for range lookup")?;
        for (entry_key, _) in entries {
            let (key_bytes, _, _) = self
                .decode_index_key(&entry_key)
                .context("decode Arrow-index key during range lookup")?;
            unique_keys.insert(key_bytes);
        }

        let mut output = Vec::new();
        for key_bytes in unique_keys {
            let key = decode::<K>(&key_bytes).context("decode Arrow-index key for range lookup")?;
            let encoded = key.encode_range_key();
            if encoded < lower_bytes || encoded >= upper_bytes {
                continue;
            }
            for (value, weight) in self.values_for_key(&key).await? {
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
        let mut aggregate = HashMap::<(Vec<u8>, Vec<u8>), i64>::new();

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
                    *aggregate.entry((key_bytes, value_bytes)).or_insert(0) += delta;
                }
            }
        }

        let mut out = Vec::with_capacity(aggregate.len());
        for ((key_bytes, value_bytes), weight) in aggregate {
            if weight == 0 {
                continue;
            }
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

    async fn segment_refs_for_key(&self, key_bytes: &[u8]) -> Result<HashMap<u64, Vec<u32>>> {
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

        let mut refs = HashMap::<u64, Vec<u32>>::new();
        for (entry_key, _) in entries {
            let (_decoded_key, segment_id, row_index) = self
                .decode_index_key(&entry_key)
                .context("decode Arrow-index key entry")?;
            refs.entry(segment_id).or_default().push(row_index);
        }
        Ok(refs)
    }

    async fn segment_refs_for_value(
        &self,
        value_bytes: &[u8],
    ) -> Result<HashMap<u64, Vec<(Vec<u8>, u32)>>> {
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

        let mut refs = HashMap::<u64, Vec<(Vec<u8>, u32)>>::new();
        for (entry_key, _) in entries {
            let (_value, key, segment_id, row_index) = self
                .decode_reverse_key(&entry_key)
                .context("decode Arrow-index reverse key")?;
            refs.entry(segment_id).or_default().push((key, row_index));
        }
        Ok(refs)
    }

    async fn next_segment_id(&self) -> Result<u64> {
        let _guard = self.segment_sequence_lock.lock().await;
        let current = match self
            .table
            .get(&self.segment_sequence_key)
            .await
            .context("read Arrow-index next segment id")?
        {
            Some(bytes) => {
                decode_u64_payload(&bytes).context("decode Arrow-index next segment id")?
            }
            None => 1,
        };
        let next = current.saturating_add(1);
        self.table
            .put(&self.segment_sequence_key, &next.to_be_bytes())
            .await
            .context("store Arrow-index next segment id")?;
        Ok(current)
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

    fn index_key(&self, key_bytes: &[u8], segment_id: u64, row_index: u32) -> Result<Vec<u8>> {
        let mut key = self.index_prefix_for_key(key_bytes)?;
        key.extend_from_slice(&segment_id.to_be_bytes());
        key.extend_from_slice(&row_index.to_be_bytes());
        Ok(key)
    }

    fn reverse_key(
        &self,
        value_bytes: &[u8],
        key_bytes: &[u8],
        segment_id: u64,
        row_index: u32,
    ) -> Result<Vec<u8>> {
        let mut key = self.reverse_prefix_for_value(value_bytes)?;
        key.extend_from_slice(&encode_len(key_bytes.len())?);
        key.extend_from_slice(key_bytes);
        key.extend_from_slice(&segment_id.to_be_bytes());
        key.extend_from_slice(&row_index.to_be_bytes());
        Ok(key)
    }

    fn decode_index_key(&self, key: &[u8]) -> Result<(Vec<u8>, u64, u32)> {
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
        let row_index_bytes = key
            .get(cursor..cursor + 4)
            .ok_or_else(|| anyhow!("Arrow-index key missing row index"))?;
        cursor += 4;
        if cursor != key.len() {
            return Err(anyhow!("Arrow-index key has trailing bytes"));
        }

        Ok((
            key_bytes,
            u64::from_be_bytes(segment_bytes.try_into().unwrap()),
            u32::from_be_bytes(row_index_bytes.try_into().unwrap()),
        ))
    }

    fn decode_reverse_key(&self, key: &[u8]) -> Result<(Vec<u8>, Vec<u8>, u64, u32)> {
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

        let key_len = read_len(key, &mut cursor)?;
        let key_end = cursor
            .checked_add(key_len)
            .ok_or_else(|| anyhow!("Arrow-index reverse key length overflow"))?;
        let key_bytes = key
            .get(cursor..key_end)
            .ok_or_else(|| anyhow!("Arrow-index reverse key truncated"))?
            .to_vec();
        cursor = key_end;

        let segment_bytes = key
            .get(cursor..cursor + 8)
            .ok_or_else(|| anyhow!("Arrow-index reverse key missing segment id"))?;
        cursor += 8;
        let row_index_bytes = key
            .get(cursor..cursor + 4)
            .ok_or_else(|| anyhow!("Arrow-index reverse key missing row index"))?;
        cursor += 4;
        if cursor != key.len() {
            return Err(anyhow!("Arrow-index reverse key has trailing bytes"));
        }

        Ok((
            value_bytes,
            key_bytes,
            u64::from_be_bytes(segment_bytes.try_into().unwrap()),
            u32::from_be_bytes(row_index_bytes.try_into().unwrap()),
        ))
    }
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(bytes);
    hasher.finish()
}

fn row_for_index(batches: &[RecordBatch], row_index: u32) -> Result<(Vec<u8>, Vec<u8>, i64)> {
    let mut remaining = row_index as usize;
    for batch in batches {
        if remaining >= batch.num_rows() {
            remaining -= batch.num_rows();
            continue;
        }

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

        return Ok((
            key_col.value(remaining).to_vec(),
            value_col.value(remaining).to_vec(),
            delta_col.value(remaining),
        ));
    }

    Err(anyhow!(
        "row index {row_index} out of bounds for Arrow-index segment"
    ))
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

fn decode_u64_payload(bytes: &[u8]) -> Result<u64> {
    let chunk = bytes
        .get(0..8)
        .ok_or_else(|| anyhow!("expected 8 bytes for Arrow-index u64 payload"))?;
    Ok(u64::from_be_bytes(chunk.try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use slatedb::Db;

    use crate::storage::SlateTable;

    use super::IndexedBatchZSet;

    async fn build_table(namespace: &str) -> Arc<dyn crate::storage::KeyValueTable> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(namespace, store).await.expect("open SlateDB"));
        Arc::new(SlateTable::new(db))
    }

    #[tokio::test]
    async fn arrow_indexed_lookup_aggregates_weights() {
        let table = build_table("arrow-indexed-lookup").await;
        let index = IndexedBatchZSet::<i64, i64>::new(table, "arrow_indexed_lookup");
        index
            .apply_deltas(vec![(1, 10, 1), (1, 11, 2), (1, 10, -1), (2, 20, 3)])
            .await
            .expect("apply deltas");

        let mut values = index.values_for_key(&1).await.expect("lookup key");
        values.sort_unstable();
        assert_eq!(values, vec![(11, 2)]);
    }

    #[tokio::test]
    async fn arrow_indexed_reverse_lookup_aggregates_keys() {
        let table = build_table("arrow-indexed-reverse").await;
        let index =
            IndexedBatchZSet::<i64, i64>::with_reverse_index(table, "arrow_indexed_reverse");
        index
            .apply_deltas(vec![(1, 10, 1), (2, 10, 3), (1, 10, -1), (3, 11, 2)])
            .await
            .expect("apply deltas");

        let mut keys = index.keys_for_value(&10).await.expect("reverse lookup");
        keys.sort_unstable();
        assert_eq!(keys, vec![(2, 3)]);
    }

    #[tokio::test]
    async fn arrow_indexed_range_scan_filters_keys() {
        let table = build_table("arrow-indexed-range").await;
        let index = IndexedBatchZSet::<i64, i64>::with_range_index(table, "arrow_indexed_range");
        index
            .apply_deltas(vec![(1, 10, 1), (2, 20, 2), (3, 30, 3), (4, 40, 4)])
            .await
            .expect("apply deltas");

        let mut rows = index
            .values_for_key_range(&2, &4)
            .await
            .expect("range lookup");
        rows.sort_unstable();
        assert_eq!(rows, vec![(2, 20, 2), (3, 30, 3)]);
    }
}
