use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, UInt32Array};
use arrow_schema::{Field, Schema, SchemaRef};
use arrow_select::concat::concat_batches;
use arrow_select::take::take;
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

use crate::storage::keyspace;
use crate::storage::segment::{ArrowSegmentStore, encode_segment_envelope};
use crate::storage::{KeyValueTable, prefix_bounds};

use super::columnar_zset::{
    ColumnarZSet, row_converter_for_schema, segment_stats_arrow, validate_weighted_batch,
    value_array_refs,
};

const RANGE_LOOKUP_MIN_KEYS: usize = 128;
const RANGE_LOOKUP_CHUNK_KEYS: usize = 512;

pub struct SlateBackedColumnarIndexedZSet {
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    value_schema: SchemaRef,
    weighted_schema: SchemaRef,
    key_schema: SchemaRef,
    key_indices: Vec<usize>,
    segment_store: ArrowSegmentStore,
    index_prefix: Vec<u8>,
    state_key: Vec<u8>,
    bounds_key: Vec<u8>,
    next_segment_id: u64,
    key_bounds: Option<IndexKeyBounds>,
}

#[derive(Clone)]
struct IndexKeyBounds {
    min: Vec<u8>,
    max: Vec<u8>,
}

impl SlateBackedColumnarIndexedZSet {
    pub async fn new(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        value_schema: SchemaRef,
        key_indices: Vec<usize>,
    ) -> Result<Self> {
        validate_key_indices(&value_schema, &key_indices)?;
        let namespace = namespace.into();
        let weighted_schema =
            super::columnar_zset::weighted_schema_for_value_schema(&value_schema)?;
        let key_schema = key_schema_for_indices(&value_schema, &key_indices);
        let segment_namespace = format!("{namespace}/columnar_index/segments");
        let segment_store = ArrowSegmentStore::new(Arc::clone(&table), segment_namespace);
        let mut index_prefix = keyspace::namespace_prefix(keyspace::prefix::INDEX, &namespace);
        index_prefix.extend_from_slice(b"columnar/");
        let mut state_key = keyspace::namespace_prefix(keyspace::prefix::INDEX, &namespace);
        state_key.extend_from_slice(b"columnar_state/next_segment_id");
        let mut bounds_key = keyspace::namespace_prefix(keyspace::prefix::INDEX, &namespace);
        bounds_key.extend_from_slice(b"columnar_state/key_bounds");
        let next_segment_id = read_next_segment_id(table.as_ref(), &state_key).await?;
        let key_bounds = read_key_bounds(table.as_ref(), &bounds_key).await?;
        Ok(Self {
            table,
            namespace,
            value_schema,
            weighted_schema,
            key_schema,
            key_indices,
            segment_store,
            index_prefix,
            state_key,
            bounds_key,
            next_segment_id,
            key_bounds,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn value_schema(&self) -> SchemaRef {
        Arc::clone(&self.value_schema)
    }

    pub fn weighted_schema(&self) -> SchemaRef {
        Arc::clone(&self.weighted_schema)
    }

    pub fn key_schema(&self) -> SchemaRef {
        Arc::clone(&self.key_schema)
    }

    pub fn key_indices(&self) -> &[usize] {
        &self.key_indices
    }

    pub async fn rebuild_from_zset(&mut self, zset: &ColumnarZSet) -> Result<()> {
        self.validate_delta(zset)?;
        self.clear_persisted().await?;
        self.next_segment_id = 1;
        self.key_bounds = None;
        self.apply_delta(zset).await?;
        Ok(())
    }

    pub async fn apply_delta(&mut self, delta: &ColumnarZSet) -> Result<Option<u64>> {
        self.validate_delta(delta)?;
        if delta.is_empty() {
            return Ok(None);
        }

        let batch = concat_batches(&delta.weighted_schema(), delta.batches())
            .context("concat columnar index delta batches")?;
        let batch = filter_nonzero_weight_rows(&batch, delta.value_column_count())
            .context("filter columnar index zero-weight rows")?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let segment_delta =
            ColumnarZSet::try_new_weighted(Arc::clone(&self.value_schema), vec![batch.clone()])
                .context("build columnar indexed zset segment delta")?;
        let postings = self.index_postings_for_batch(&batch)?;
        if postings.is_empty() {
            return Ok(None);
        }
        let next_bounds = if self.key_bounds.is_some() || self.next_segment_id == 1 {
            merge_key_bounds(self.key_bounds.as_ref(), postings.keys())
        } else {
            None
        };

        let segment_id = self.next_segment_id;
        self.next_segment_id = self.next_segment_id.saturating_add(1);
        let stats = segment_stats_arrow(&segment_delta)?;
        let (segment_bytes, _) = encode_segment_envelope(
            Arc::clone(&self.weighted_schema),
            segment_delta.batches(),
            stats,
        )
        .context("encode columnar indexed zset segment")?;

        let mut write_batch = WriteBatch::new();
        write_batch.put(
            self.segment_store.key_for_segment(segment_id),
            segment_bytes,
        );
        for (key_bytes, key_postings) in postings {
            write_batch.put(
                self.index_key(&key_bytes, segment_id)?,
                encode_index_postings(&key_postings),
            );
        }
        write_batch.put(
            self.state_key.clone(),
            self.next_segment_id.to_be_bytes().to_vec(),
        );
        if let Some(bounds) = next_bounds.as_ref() {
            write_batch.put(self.bounds_key.clone(), encode_key_bounds(bounds)?);
        }
        self.table
            .write_batch(write_batch)
            .await
            .context("persist columnar indexed zset delta")?;
        self.key_bounds = next_bounds;
        Ok(Some(segment_id))
    }

    pub async fn lookup_key_batches(&self, key_batches: &[RecordBatch]) -> Result<ColumnarZSet> {
        let mut key_bytes = self.key_bytes_from_batches(key_batches)?;
        if key_bytes.is_empty() {
            return ColumnarZSet::empty(Arc::clone(&self.value_schema));
        }
        if !keys_overlap_bounds(&key_bytes, self.key_bounds.as_ref()) {
            return ColumnarZSet::empty(Arc::clone(&self.value_schema));
        }

        let mut refs_by_segment: HashMap<u64, Vec<u32>> = HashMap::new();
        if key_bytes.len() >= RANGE_LOOKUP_MIN_KEYS {
            key_bytes.sort_unstable();
            self.lookup_key_ranges(&key_bytes, &mut refs_by_segment)
                .await?;
        } else {
            self.lookup_keys_individually(&key_bytes, &mut refs_by_segment)
                .await?;
        };

        if refs_by_segment.is_empty() {
            return ColumnarZSet::empty(Arc::clone(&self.value_schema));
        }

        let mut batches = Vec::new();
        let mut segment_ids = refs_by_segment.keys().copied().collect::<Vec<_>>();
        segment_ids.sort_unstable();
        for segment_id in segment_ids {
            let indices = refs_by_segment
                .remove(&segment_id)
                .expect("segment refs missing");
            batches.extend(self.take_segment_rows(segment_id, indices).await?);
        }
        ColumnarZSet::try_new_weighted(Arc::clone(&self.value_schema), batches)
            .context("build columnar indexed zset lookup result")
    }

    async fn lookup_keys_individually(
        &self,
        key_bytes: &[Vec<u8>],
        refs_by_segment: &mut HashMap<u64, Vec<u32>>,
    ) -> Result<()> {
        for key in key_bytes {
            let entries = self
                .table
                .scan_range_bytes(
                    prefix_bounds(&self.index_prefix_for_key(key)?),
                    &ScanOptions::default(),
                )
                .await
                .context("scan columnar index key postings")?;
            self.collect_posting_entries(entries, None, refs_by_segment)?;
        }
        Ok(())
    }

    async fn lookup_key_ranges(
        &self,
        sorted_key_bytes: &[Vec<u8>],
        refs_by_segment: &mut HashMap<u64, Vec<u32>>,
    ) -> Result<()> {
        for chunk in sorted_key_bytes.chunks(RANGE_LOOKUP_CHUNK_KEYS) {
            let Some(first_key) = chunk.first() else {
                continue;
            };
            let Some(last_key) = chunk.last() else {
                continue;
            };
            let start = self.index_prefix_for_key(first_key)?;
            let end = prefix_bounds(&self.index_prefix_for_key(last_key)?).end;
            let wanted_keys = chunk.iter().cloned().collect::<HashSet<Vec<u8>>>();
            let entries = self
                .table
                .scan_range_bytes(start..end, &ScanOptions::default())
                .await
                .context("scan columnar index key posting range")?;
            self.collect_posting_entries(entries, Some(&wanted_keys), refs_by_segment)?;
        }
        Ok(())
    }

    fn collect_posting_entries<K, V>(
        &self,
        entries: Vec<(K, V)>,
        wanted_keys: Option<&HashSet<Vec<u8>>>,
        refs_by_segment: &mut HashMap<u64, Vec<u32>>,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        for (entry_key, entry_value) in entries {
            if let Some(wanted_keys) = wanted_keys {
                let key_bytes = self.lookup_key_bytes_from_index_key(entry_key.as_ref())?;
                if !wanted_keys.contains(key_bytes) {
                    continue;
                }
            }
            let segment_id = segment_id_from_index_key(entry_key.as_ref())?;
            for (row_index, weight) in decode_index_postings(entry_value.as_ref())? {
                if weight != 0 {
                    refs_by_segment
                        .entry(segment_id)
                        .or_default()
                        .push(row_index);
                }
            }
        }
        Ok(())
    }

    fn lookup_key_bytes_from_index_key<'a>(&self, index_key: &'a [u8]) -> Result<&'a [u8]> {
        if !index_key.starts_with(&self.index_prefix) {
            bail!("columnar index posting key prefix mismatch");
        }
        let mut cursor = self.index_prefix.len();
        let key_len = usize::try_from(read_u32(index_key, &mut cursor)?)
            .context("columnar index key length out of range")?;
        let key_bytes = read_bytes(index_key, &mut cursor, key_len, "columnar index key bytes")?;
        if index_key.len() < cursor.saturating_add(8) {
            bail!("columnar index posting key missing segment id");
        }
        Ok(key_bytes)
    }

    async fn clear_persisted(&self) -> Result<()> {
        let mut write_batch = WriteBatch::new();
        for (key, _) in self
            .table
            .scan_range_bytes(prefix_bounds(&self.index_prefix), &ScanOptions::default())
            .await
            .context("scan columnar index postings for rebuild")?
        {
            write_batch.delete(key.to_vec());
        }
        for segment_id in self
            .segment_store
            .list_segment_ids()
            .await
            .context("list columnar index segments for rebuild")?
        {
            write_batch.delete(self.segment_store.key_for_segment(segment_id));
        }
        write_batch.delete(self.state_key.clone());
        write_batch.delete(self.bounds_key.clone());
        self.table
            .write_batch(write_batch)
            .await
            .context("clear columnar indexed zset state")?;
        Ok(())
    }

    fn validate_delta(&self, delta: &ColumnarZSet) -> Result<()> {
        if delta.value_schema().as_ref() != self.value_schema.as_ref()
            || delta.weighted_schema().as_ref() != self.weighted_schema.as_ref()
        {
            bail!("columnar indexed zset delta schema mismatch");
        }
        Ok(())
    }

    fn index_postings_for_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<HashMap<Vec<u8>, Vec<(u32, i64)>>> {
        validate_weighted_batch(&self.weighted_schema, &self.value_schema, batch)?;
        let key_columns = self
            .key_indices
            .iter()
            .map(|idx| Arc::clone(batch.column(*idx)))
            .collect::<Vec<_>>();
        let key_rows = row_converter_for_schema(&self.key_schema)?
            .convert_columns(&key_columns)
            .context("encode columnar index keys")?;
        let weights = weight_column(batch, self.value_schema.fields().len())?;
        let mut postings: HashMap<Vec<u8>, Vec<(u32, i64)>> = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            let weight = weights.value(row_idx);
            if weight == 0 {
                continue;
            }
            let row_idx = u32::try_from(row_idx).context("columnar index row index exceeds u32")?;
            postings
                .entry(key_rows.row(row_idx as usize).data().to_vec())
                .or_default()
                .push((row_idx, weight));
        }
        Ok(postings)
    }

    fn key_bytes_from_batches(&self, key_batches: &[RecordBatch]) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::new();
        let mut seen = HashSet::new();
        let converter = row_converter_for_schema(&self.key_schema)?;
        for batch in key_batches {
            if batch.schema().as_ref() != self.key_schema.as_ref() {
                bail!("columnar index lookup key batch schema mismatch");
            }
            if batch.num_rows() == 0 {
                continue;
            }
            let columns = value_array_refs(batch, batch.num_columns());
            let rows = converter
                .convert_columns(&columns)
                .context("encode columnar index lookup keys")?;
            for row_idx in 0..batch.num_rows() {
                let key = rows.row(row_idx).data().to_vec();
                if seen.insert(key.clone()) {
                    keys.push(key);
                }
            }
        }
        Ok(keys)
    }

    async fn take_segment_rows(
        &self,
        segment_id: u64,
        mut indices: Vec<u32>,
    ) -> Result<Vec<RecordBatch>> {
        indices.sort_unstable();
        indices.dedup();
        let Some(segment) = self
            .segment_store
            .read_segment(segment_id)
            .await
            .with_context(|| format!("read columnar indexed zset segment {segment_id}"))?
        else {
            bail!("missing columnar indexed zset segment {segment_id}");
        };
        if segment.schema.as_ref() != self.weighted_schema.as_ref() {
            bail!("columnar indexed zset segment schema mismatch");
        }

        let mut batches = Vec::new();
        let mut global_offset = 0_u32;
        for batch in segment.batches {
            let batch_rows =
                u32::try_from(batch.num_rows()).context("columnar index batch exceeds u32 rows")?;
            let batch_end = global_offset.saturating_add(batch_rows);
            let local_indices = indices
                .iter()
                .copied()
                .filter(|idx| *idx >= global_offset && *idx < batch_end)
                .map(|idx| idx - global_offset)
                .collect::<Vec<_>>();
            if !local_indices.is_empty() {
                batches.push(take_batch_rows(&batch, &local_indices)?);
            }
            global_offset = batch_end;
        }
        Ok(batches)
    }

    fn index_prefix_for_key(&self, key_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut prefix = self.index_prefix.clone();
        prefix.extend_from_slice(&encode_len(key_bytes.len())?);
        prefix.extend_from_slice(key_bytes);
        Ok(prefix)
    }

    fn index_key(&self, key_bytes: &[u8], segment_id: u64) -> Result<Vec<u8>> {
        let mut key = self.index_prefix_for_key(key_bytes)?;
        key.extend_from_slice(&segment_id.to_be_bytes());
        Ok(key)
    }
}

fn validate_key_indices(value_schema: &SchemaRef, key_indices: &[usize]) -> Result<()> {
    if key_indices.is_empty() {
        bail!("columnar indexed zset requires at least one key column");
    }
    let mut seen = HashSet::new();
    for idx in key_indices {
        if *idx >= value_schema.fields().len() {
            bail!("columnar indexed zset key column {idx} out of bounds");
        }
        if !seen.insert(*idx) {
            bail!("columnar indexed zset duplicate key column {idx}");
        }
    }
    Ok(())
}

fn key_schema_for_indices(value_schema: &SchemaRef, key_indices: &[usize]) -> SchemaRef {
    let fields = key_indices
        .iter()
        .map(|idx| value_schema.field(*idx).as_ref().clone())
        .collect::<Vec<Field>>();
    Arc::new(Schema::new(fields))
}

async fn read_next_segment_id(table: &dyn KeyValueTable, state_key: &[u8]) -> Result<u64> {
    let Some(bytes) = table
        .get_bytes(state_key)
        .await
        .context("read columnar indexed zset state")?
    else {
        return Ok(1);
    };
    if bytes.len() != 8 {
        bail!("invalid columnar indexed zset state length {}", bytes.len());
    }
    Ok(u64::from_be_bytes(bytes.as_ref().try_into()?))
}

async fn read_key_bounds(
    table: &dyn KeyValueTable,
    bounds_key: &[u8],
) -> Result<Option<IndexKeyBounds>> {
    let Some(bytes) = table
        .get_bytes(bounds_key)
        .await
        .context("read columnar indexed zset key bounds")?
    else {
        return Ok(None);
    };
    decode_key_bounds(bytes.as_ref()).map(Some)
}

fn merge_key_bounds<'a>(
    current: Option<&IndexKeyBounds>,
    keys: impl IntoIterator<Item = &'a Vec<u8>>,
) -> Option<IndexKeyBounds> {
    let mut min = current.map(|bounds| bounds.min.clone());
    let mut max = current.map(|bounds| bounds.max.clone());
    for key in keys {
        if min
            .as_ref()
            .is_none_or(|current_min| key.as_slice() < current_min.as_slice())
        {
            min = Some(key.clone());
        }
        if max
            .as_ref()
            .is_none_or(|current_max| key.as_slice() > current_max.as_slice())
        {
            max = Some(key.clone());
        }
    }
    min.zip(max).map(|(min, max)| IndexKeyBounds { min, max })
}

fn keys_overlap_bounds(keys: &[Vec<u8>], bounds: Option<&IndexKeyBounds>) -> bool {
    let Some(bounds) = bounds else {
        return true;
    };
    let Some(delta_bounds) = merge_key_bounds(None, keys.iter()) else {
        return false;
    };
    delta_bounds.max.as_slice() >= bounds.min.as_slice()
        && delta_bounds.min.as_slice() <= bounds.max.as_slice()
}

fn encode_key_bounds(bounds: &IndexKeyBounds) -> Result<Vec<u8>> {
    let min_len = u32::try_from(bounds.min.len()).context("columnar index min key too large")?;
    let max_len = u32::try_from(bounds.max.len()).context("columnar index max key too large")?;
    let mut out = Vec::with_capacity(8 + bounds.min.len() + bounds.max.len());
    out.extend_from_slice(&min_len.to_be_bytes());
    out.extend_from_slice(&bounds.min);
    out.extend_from_slice(&max_len.to_be_bytes());
    out.extend_from_slice(&bounds.max);
    Ok(out)
}

fn decode_key_bounds(bytes: &[u8]) -> Result<IndexKeyBounds> {
    let mut cursor = 0;
    let min_len = usize::try_from(read_u32(bytes, &mut cursor)?)
        .context("columnar index min key length out of range")?;
    let min = read_bytes(bytes, &mut cursor, min_len, "columnar index min key")?.to_vec();
    let max_len = usize::try_from(read_u32(bytes, &mut cursor)?)
        .context("columnar index max key length out of range")?;
    let max = read_bytes(bytes, &mut cursor, max_len, "columnar index max key")?.to_vec();
    if cursor != bytes.len() {
        bail!("columnar index key bounds payload has trailing bytes");
    }
    Ok(IndexKeyBounds { min, max })
}

fn filter_nonzero_weight_rows(
    batch: &RecordBatch,
    value_column_count: usize,
) -> Result<RecordBatch> {
    let weights = weight_column(batch, value_column_count)?;
    let indices = (0..batch.num_rows())
        .filter(|row_idx| weights.value(*row_idx) != 0)
        .map(|row_idx| u32::try_from(row_idx).context("columnar index row index exceeds u32"))
        .collect::<Result<Vec<_>>>()?;
    if indices.len() == batch.num_rows() {
        return Ok(batch.clone());
    }
    take_batch_rows(batch, &indices)
}

fn take_batch_rows(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
    let indices = UInt32Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<ArrayRef>, _>>()
        .context("take columnar indexed zset rows")?;
    RecordBatch::try_new(batch.schema(), columns).context("build taken columnar indexed zset batch")
}

fn weight_column(batch: &RecordBatch, value_column_count: usize) -> Result<&Int64Array> {
    batch
        .column(value_column_count)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("columnar indexed zset weight column is not Int64"))
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
    let count = read_u32(bytes, &mut cursor).context("decode columnar index postings count")?;
    let mut out = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let row_index =
            read_u32(bytes, &mut cursor).context("decode columnar index posting row index")?;
        let weight =
            read_i64(bytes, &mut cursor).context("decode columnar index posting weight")?;
        out.push((row_index, weight));
    }
    if cursor != bytes.len() {
        bail!("columnar index postings payload has trailing bytes");
    }
    Ok(out)
}

fn encode_len(len: usize) -> Result<[u8; 4]> {
    let len = u32::try_from(len).map_err(|_| anyhow!("columnar index key too large"))?;
    Ok(len.to_be_bytes())
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    Ok(u32::from_be_bytes(read_exact_at(
        bytes,
        cursor,
        "columnar index u32",
    )?))
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64> {
    Ok(i64::from_be_bytes(read_exact_at(
        bytes,
        cursor,
        "columnar index i64",
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

fn read_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
    label: &str,
) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| anyhow!("{label} overflow"))?;
    let chunk = bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow!("{label} truncated"))?;
    *cursor = end;
    Ok(chunk)
}

fn segment_id_from_index_key(key: &[u8]) -> Result<u64> {
    if key.len() < 8 {
        bail!("columnar index key missing segment id suffix");
    }
    let suffix = key
        .get(key.len() - 8..)
        .ok_or_else(|| anyhow!("columnar index segment id suffix truncated"))?;
    Ok(u64::from_be_bytes(suffix.try_into()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use slatedb::WriteBatch;
    use slatedb::config::ScanOptions;

    use crate::storage::SlateTable;

    struct CountingTable {
        inner: Arc<dyn KeyValueTable>,
        scan_range_calls: AtomicUsize,
    }

    impl CountingTable {
        fn new(inner: Arc<dyn KeyValueTable>) -> Self {
            Self {
                inner,
                scan_range_calls: AtomicUsize::new(0),
            }
        }

        fn reset_scan_range_calls(&self) {
            self.scan_range_calls.store(0, Ordering::Relaxed);
        }

        fn scan_range_calls(&self) -> usize {
            self.scan_range_calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl KeyValueTable for CountingTable {
        async fn get_bytes(&self, key: &[u8]) -> Result<Option<Bytes>> {
            self.inner.get_bytes(key).await
        }

        async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
            self.inner.write_batch(batch).await
        }

        async fn scan_range_bytes(
            &self,
            range: Range<Vec<u8>>,
            options: &ScanOptions,
        ) -> Result<Vec<(Bytes, Bytes)>> {
            self.scan_range_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.scan_range_bytes(range, options).await
        }
    }

    async fn build_table(name: &str) -> Arc<dyn KeyValueTable> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
        Arc::new(SlateTable::new(db))
    }

    fn value_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("note", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]))
    }

    fn weighted_batch(
        ids: Vec<i64>,
        notes: Vec<&str>,
        amounts: Vec<i64>,
        weights: Vec<i64>,
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("note", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new(super::super::COLUMNAR_WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(notes)) as ArrayRef,
                Arc::new(Int64Array::from(amounts)) as ArrayRef,
                Arc::new(Int64Array::from(weights)) as ArrayRef,
            ],
        )
        .expect("weighted batch")
    }

    fn key_batch(ids: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids)) as ArrayRef])
            .expect("key batch")
    }

    fn lookup_rows(zset: &ColumnarZSet) -> Vec<(i64, String, i64, i64)> {
        let mut rows = Vec::new();
        for batch in zset.batches() {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column");
            let notes = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("note column");
            let amounts = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("amount column");
            let weights = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("weight column");
            for row_idx in 0..batch.num_rows() {
                rows.push((
                    ids.value(row_idx),
                    notes.value(row_idx).to_string(),
                    amounts.value(row_idx),
                    weights.value(row_idx),
                ));
            }
        }
        rows.sort();
        rows
    }

    #[tokio::test]
    async fn columnar_index_lookup_returns_arrow_weighted_rows() {
        let table = build_table("columnar-index-lookup").await;
        let mut index =
            SlateBackedColumnarIndexedZSet::new(table, "orders_by_id", value_schema(), vec![0])
                .await
                .expect("index");
        let delta = ColumnarZSet::try_new_weighted(
            value_schema(),
            vec![weighted_batch(
                vec![1, 1, 2, 1],
                vec!["a", "b", "c", "zero"],
                vec![10, 20, 30, 40],
                vec![1, 2, 1, 0],
            )],
        )
        .expect("delta");
        index.apply_delta(&delta).await.expect("apply delta");

        let found = index
            .lookup_key_batches(&[key_batch(vec![1])])
            .await
            .expect("lookup");
        assert_eq!(
            lookup_rows(&found),
            vec![(1, "a".to_string(), 10, 1), (1, "b".to_string(), 20, 2)]
        );
    }

    #[tokio::test]
    async fn columnar_index_large_lookup_uses_chunked_range_scans() {
        let inner = build_table("columnar-index-large-lookup").await;
        let counting = Arc::new(CountingTable::new(inner));
        let table: Arc<dyn KeyValueTable> = counting.clone();
        let mut index =
            SlateBackedColumnarIndexedZSet::new(table, "orders_by_id", value_schema(), vec![0])
                .await
                .expect("index");
        let rows = RANGE_LOOKUP_MIN_KEYS * 2;
        let ids = (0..rows as i64).collect::<Vec<_>>();
        let delta = ColumnarZSet::try_new_weighted(
            value_schema(),
            vec![weighted_batch(
                ids.clone(),
                vec!["n"; rows],
                ids.iter().map(|id| id * 10).collect(),
                vec![1; rows],
            )],
        )
        .expect("delta");
        index.apply_delta(&delta).await.expect("apply delta");

        counting.reset_scan_range_calls();
        let found = index
            .lookup_key_batches(&[key_batch(ids)])
            .await
            .expect("lookup");
        let found_rows = found
            .batches()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>();
        assert_eq!(found_rows, rows);
        assert_eq!(counting.scan_range_calls(), 1);
    }

    #[tokio::test]
    async fn columnar_index_reopens_and_appends_segments() {
        let table = build_table("columnar-index-reopen").await;
        let mut index = SlateBackedColumnarIndexedZSet::new(
            Arc::clone(&table),
            "orders_by_id",
            value_schema(),
            vec![0],
        )
        .await
        .expect("index");
        let first = ColumnarZSet::try_new_weighted(
            value_schema(),
            vec![weighted_batch(
                vec![1, 2],
                vec!["a", "b"],
                vec![10, 20],
                vec![1, 1],
            )],
        )
        .expect("first delta");
        assert_eq!(
            index.apply_delta(&first).await.expect("apply first"),
            Some(1)
        );

        let mut reopened = SlateBackedColumnarIndexedZSet::new(
            Arc::clone(&table),
            "orders_by_id",
            value_schema(),
            vec![0],
        )
        .await
        .expect("reopened index");
        let second = ColumnarZSet::try_new_weighted(
            value_schema(),
            vec![weighted_batch(
                vec![1, 3],
                vec!["a", "c"],
                vec![10, 30],
                vec![-1, 1],
            )],
        )
        .expect("second delta");
        assert_eq!(
            reopened.apply_delta(&second).await.expect("apply second"),
            Some(2)
        );

        let found = reopened
            .lookup_key_batches(&[key_batch(vec![1, 3])])
            .await
            .expect("lookup");
        assert_eq!(
            lookup_rows(&found),
            vec![
                (1, "a".to_string(), 10, -1),
                (1, "a".to_string(), 10, 1),
                (3, "c".to_string(), 30, 1)
            ]
        );
    }

    #[tokio::test]
    async fn columnar_index_skips_lookup_when_keys_are_outside_persisted_bounds() {
        let inner = build_table("columnar-index-key-bounds").await;
        let counting = Arc::new(CountingTable::new(inner));
        let table: Arc<dyn KeyValueTable> = counting.clone();
        let mut index =
            SlateBackedColumnarIndexedZSet::new(table, "orders_by_id", value_schema(), vec![0])
                .await
                .expect("index");
        let delta = ColumnarZSet::try_new_weighted(
            value_schema(),
            vec![weighted_batch(
                vec![10, 20],
                vec!["a", "b"],
                vec![100, 200],
                vec![1, 1],
            )],
        )
        .expect("delta");
        index.apply_delta(&delta).await.expect("apply delta");

        counting.reset_scan_range_calls();
        let below = index
            .lookup_key_batches(&[key_batch(vec![1, 2])])
            .await
            .expect("lookup below bounds");
        assert!(below.is_empty());
        assert_eq!(counting.scan_range_calls(), 0);

        counting.reset_scan_range_calls();
        let above = index
            .lookup_key_batches(&[key_batch(vec![30, 40])])
            .await
            .expect("lookup above bounds");
        assert!(above.is_empty());
        assert_eq!(counting.scan_range_calls(), 0);

        counting.reset_scan_range_calls();
        let within = index
            .lookup_key_batches(&[key_batch(vec![15])])
            .await
            .expect("lookup inside bounds");
        assert!(within.is_empty());
        assert!(counting.scan_range_calls() > 0);

        let table: Arc<dyn KeyValueTable> = counting.clone();
        let reopened =
            SlateBackedColumnarIndexedZSet::new(table, "orders_by_id", value_schema(), vec![0])
                .await
                .expect("reopened index");
        counting.reset_scan_range_calls();
        let reopened_above = reopened
            .lookup_key_batches(&[key_batch(vec![30])])
            .await
            .expect("reopened lookup above bounds");
        assert!(reopened_above.is_empty());
        assert_eq!(counting.scan_range_calls(), 0);
    }

    #[tokio::test]
    async fn columnar_index_without_persisted_bounds_remains_conservative_after_append() {
        let inner = build_table("columnar-index-missing-key-bounds").await;
        let counting = Arc::new(CountingTable::new(inner));
        let table: Arc<dyn KeyValueTable> = counting.clone();
        let mut index = SlateBackedColumnarIndexedZSet::new(
            Arc::clone(&table),
            "orders_by_id",
            value_schema(),
            vec![0],
        )
        .await
        .expect("index");
        let first = ColumnarZSet::try_new_weighted(
            value_schema(),
            vec![weighted_batch(vec![10], vec!["a"], vec![100], vec![1])],
        )
        .expect("first delta");
        index.apply_delta(&first).await.expect("apply first");
        index
            .table
            .delete(&index.bounds_key)
            .await
            .expect("delete bounds");

        let mut reopened = SlateBackedColumnarIndexedZSet::new(
            Arc::clone(&table),
            "orders_by_id",
            value_schema(),
            vec![0],
        )
        .await
        .expect("reopened index");
        let second = ColumnarZSet::try_new_weighted(
            value_schema(),
            vec![weighted_batch(vec![20], vec!["b"], vec![200], vec![1])],
        )
        .expect("second delta");
        reopened.apply_delta(&second).await.expect("apply second");

        counting.reset_scan_range_calls();
        let found = reopened
            .lookup_key_batches(&[key_batch(vec![10])])
            .await
            .expect("lookup old key");
        assert_eq!(lookup_rows(&found), vec![(10, "a".to_string(), 100, 1)]);
        assert!(counting.scan_range_calls() > 0);
    }
}
