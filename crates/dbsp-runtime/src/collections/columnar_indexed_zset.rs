use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, UInt32Array};
use arrow_schema::{Field, Schema, SchemaRef};
use arrow_select::concat::concat_batches;
use arrow_select::take::take;
use bytes::Bytes;
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

use crate::profile;
use crate::storage::KeyValueTable;
use crate::storage::keyspace;
use crate::storage::segment::{
    ArrowSegmentStore, decode_record_batches, encode_record_batches, encode_segment_envelope,
};

use super::columnar_zset::{
    ColumnarZSet, consolidate_columnar_zset, row_converter_for_schema, segment_stats_arrow,
    validate_weighted_batch, value_array_refs,
};

const INDEX_BUCKET_COUNT: u16 = 16;
const INDEX_BUCKET_TAG: u8 = b'b';
const INDEX_RANGE_TAG: u8 = b'r';
const INDEX_BUCKET_INLINE_MAGIC: &[u8; 4] = b"cib2";
const INDEX_BUCKET_INLINE_VERSION: u8 = 1;
const INDEX_RANGE_INLINE_MAGIC: &[u8; 4] = b"cir1";
const INDEX_RANGE_INLINE_VERSION: u8 = 1;
const INDEX_RANGE_SEGMENT_MAGIC: &[u8; 4] = b"crs1";
const INDEX_RANGE_SEGMENT_VERSION: u8 = 1;
const INDEX_RANGE_TARGET_ROWS: usize = 2048;
const INDEX_INLINE_ROW_MAX_ROWS: usize = 2048;

type RowPosting = (u32, i64);
type IndexPostings = Vec<RowPosting>;
type IndexEntry = (Vec<u8>, IndexPostings);
type IndexPostingsByKey = HashMap<Vec<u8>, IndexPostings>;
type IndexEntryChunks = Vec<Vec<IndexEntry>>;

pub struct SlateBackedColumnarIndexedZSet {
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    value_schema: SchemaRef,
    weighted_schema: SchemaRef,
    key_schema: SchemaRef,
    key_indices: Vec<usize>,
    segment_store: ArrowSegmentStore,
    index_prefix: Vec<u8>,
    range_prefix: Vec<u8>,
    current_prefix: Vec<u8>,
    state_key: Vec<u8>,
    bounds_key: Vec<u8>,
    range_state_key: Vec<u8>,
    current_state_key: Vec<u8>,
    next_segment_id: u64,
    key_bounds: Option<IndexKeyBounds>,
    range_lookup_only: bool,
    current_lookup_enabled: bool,
    current_lookup_initialized: bool,
    segment_large_range_rows: bool,
}

#[derive(Clone)]
struct IndexKeyBounds {
    min: Vec<u8>,
    max: Vec<u8>,
}

struct LookupKeySet {
    buckets: HashMap<u16, HashSet<Vec<u8>>>,
    bounds: Option<IndexKeyBounds>,
    len: usize,
}

impl LookupKeySet {
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn overlaps_persisted_bounds(&self, persisted_bounds: Option<&IndexKeyBounds>) -> bool {
        let Some(persisted_bounds) = persisted_bounds else {
            return true;
        };
        let Some(bounds) = self.bounds.as_ref() else {
            return false;
        };
        bounds.max.as_slice() >= persisted_bounds.min.as_slice()
            && bounds.min.as_slice() <= persisted_bounds.max.as_slice()
    }
}

impl SlateBackedColumnarIndexedZSet {
    pub async fn new(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        value_schema: SchemaRef,
        key_indices: Vec<usize>,
    ) -> Result<Self> {
        Self::new_internal(table, namespace, value_schema, key_indices, false, false).await
    }

    pub async fn new_with_current_lookup(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        value_schema: SchemaRef,
        key_indices: Vec<usize>,
    ) -> Result<Self> {
        Self::new_internal(table, namespace, value_schema, key_indices, true, false).await
    }

    pub async fn new_with_segment_backed_large_ranges(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        value_schema: SchemaRef,
        key_indices: Vec<usize>,
    ) -> Result<Self> {
        Self::new_internal(table, namespace, value_schema, key_indices, false, true).await
    }

    async fn new_internal(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        value_schema: SchemaRef,
        key_indices: Vec<usize>,
        current_lookup_enabled: bool,
        segment_large_range_rows: bool,
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
        let mut range_prefix = index_prefix.clone();
        range_prefix.push(INDEX_RANGE_TAG);
        let mut current_prefix = keyspace::namespace_prefix(keyspace::prefix::INDEX, &namespace);
        current_prefix.extend_from_slice(b"columnar_current/");
        let mut state_key = keyspace::namespace_prefix(keyspace::prefix::INDEX, &namespace);
        state_key.extend_from_slice(b"columnar_state/next_segment_id");
        let mut bounds_key = keyspace::namespace_prefix(keyspace::prefix::INDEX, &namespace);
        bounds_key.extend_from_slice(b"columnar_state/key_bounds");
        let mut range_state_key = keyspace::namespace_prefix(keyspace::prefix::INDEX, &namespace);
        range_state_key.extend_from_slice(b"columnar_state/range_lookup_only");
        let mut current_state_key = keyspace::namespace_prefix(keyspace::prefix::INDEX, &namespace);
        current_state_key.extend_from_slice(b"columnar_state/current_lookup_initialized");
        let next_segment_id = read_next_segment_id(table.as_ref(), &state_key).await?;
        let key_bounds = read_key_bounds(table.as_ref(), &bounds_key).await?;
        let range_lookup_only =
            next_segment_id == 1 || read_bool_state(table.as_ref(), &range_state_key).await?;
        let current_lookup_initialized =
            current_lookup_enabled && read_bool_state(table.as_ref(), &current_state_key).await?;
        Ok(Self {
            table,
            namespace,
            value_schema,
            weighted_schema,
            key_schema,
            key_indices,
            segment_store,
            index_prefix,
            range_prefix,
            current_prefix,
            state_key,
            bounds_key,
            range_state_key,
            current_state_key,
            next_segment_id,
            key_bounds,
            range_lookup_only,
            current_lookup_enabled,
            current_lookup_initialized,
            segment_large_range_rows,
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

    pub fn has_persisted_segments(&self) -> bool {
        self.next_segment_id > 1
    }

    pub fn has_current_lookup_state(&self) -> bool {
        self.current_lookup_initialized
    }

    pub async fn rebuild_from_zset(&mut self, zset: &ColumnarZSet) -> Result<()> {
        self.validate_delta(zset)?;
        self.clear_persisted().await?;
        self.next_segment_id = 1;
        self.key_bounds = None;
        self.range_lookup_only = true;
        self.current_lookup_initialized = false;
        self.apply_delta(zset).await?;
        Ok(())
    }

    pub async fn apply_delta(&mut self, delta: &ColumnarZSet) -> Result<Option<u64>> {
        let total_start = profile::start();
        let phase_start = profile::start();
        self.validate_delta(delta)?;
        if delta.is_empty() {
            profile::record_since("columnar_index.apply_delta.total", total_start);
            return Ok(None);
        }
        profile::record_since("columnar_index.apply_delta.validate", phase_start);

        let phase_start = profile::start();
        let batch = concat_batches(&delta.weighted_schema(), delta.batches())
            .context("concat columnar index delta batches")?;
        profile::record_since("columnar_index.apply_delta.concat", phase_start);
        let phase_start = profile::start();
        let batch = filter_nonzero_weight_rows(&batch, delta.value_column_count())
            .context("filter columnar index zero-weight rows")?;
        if batch.num_rows() == 0 {
            profile::record_since("columnar_index.apply_delta.filter_nonzero", phase_start);
            profile::record_since("columnar_index.apply_delta.total", total_start);
            return Ok(None);
        }
        profile::record_since("columnar_index.apply_delta.filter_nonzero", phase_start);

        let phase_start = profile::start();
        let postings = self.index_postings_for_batch(&batch)?;
        if postings.is_empty() {
            profile::record_since("columnar_index.apply_delta.build_postings", phase_start);
            profile::record_since("columnar_index.apply_delta.total", total_start);
            return Ok(None);
        }
        profile::record_since("columnar_index.apply_delta.build_postings", phase_start);
        let phase_start = profile::start();
        let current_mutations = if self.current_lookup_enabled {
            self.current_mutations_for_postings(&batch, &postings)
                .await
                .context("build columnar current index mutations")?
        } else {
            Vec::new()
        };
        profile::record_since("columnar_index.apply_delta.current_mutations", phase_start);
        let phase_start = profile::start();
        let next_bounds = if self.key_bounds.is_some() || self.next_segment_id == 1 {
            merge_key_bounds(self.key_bounds.as_ref(), postings.keys())
        } else {
            None
        };
        profile::record_since("columnar_index.apply_delta.merge_bounds", phase_start);

        let segment_id = self.next_segment_id;
        self.next_segment_id = self.next_segment_id.saturating_add(1);

        let phase_start = profile::start();
        let mut write_batch = WriteBatch::new();
        let range_chunks = range_posting_chunks(postings);
        if !self.segment_large_range_rows || batch.num_rows() <= INDEX_INLINE_ROW_MAX_ROWS {
            for (chunk_id, entries) in range_chunks.into_iter().enumerate() {
                write_batch.put_bytes(
                    Bytes::from(self.range_index_key(segment_id, chunk_id)?),
                    Bytes::from(encode_range_postings_with_inline_rows(
                        &self.weighted_schema,
                        &batch,
                        &entries,
                    )?),
                );
            }
        } else {
            let segment_delta =
                ColumnarZSet::try_new_weighted(Arc::clone(&self.value_schema), vec![batch.clone()])
                    .context("build columnar indexed zset segment delta")?;
            let stats = segment_stats_arrow(&segment_delta)?;
            let (segment_bytes, _) = encode_segment_envelope(
                Arc::clone(&self.weighted_schema),
                segment_delta.batches(),
                stats,
            )
            .context("encode columnar indexed zset segment")?;
            write_batch.put_bytes(
                Bytes::from(self.segment_store.key_for_segment(segment_id)),
                Bytes::from(segment_bytes),
            );
            for (chunk_id, entries) in range_chunks.into_iter().enumerate() {
                write_batch.put_bytes(
                    Bytes::from(self.range_index_key(segment_id, chunk_id)?),
                    Bytes::from(encode_range_postings_with_segment_rows(&entries)?),
                );
            }
        }
        write_batch.put_bytes(
            Bytes::from(self.state_key.clone()),
            Bytes::from(self.next_segment_id.to_be_bytes().to_vec()),
        );
        if let Some(bounds) = next_bounds.as_ref() {
            write_batch.put_bytes(
                Bytes::from(self.bounds_key.clone()),
                Bytes::from(encode_key_bounds(bounds)?),
            );
        }
        if self.range_lookup_only {
            write_batch.put_bytes(
                Bytes::from(self.range_state_key.clone()),
                Bytes::from(vec![1]),
            );
        }
        for mutation in current_mutations {
            match mutation {
                CurrentIndexMutation::Put { key, value } => {
                    write_batch.put_bytes(Bytes::from(key), Bytes::from(value));
                }
            }
        }
        profile::record_since("columnar_index.apply_delta.build_write_batch", phase_start);
        let phase_start = profile::start();
        self.table
            .write_batch(write_batch)
            .await
            .context("persist columnar indexed zset delta")?;
        profile::record_since("columnar_index.apply_delta.write_batch", phase_start);
        let phase_start = profile::start();
        self.key_bounds = next_bounds;
        profile::record_since(
            "columnar_index.apply_delta.update_memory_state",
            phase_start,
        );
        profile::record_since("columnar_index.apply_delta.total", total_start);
        Ok(Some(segment_id))
    }

    pub async fn lookup_key_batches(&self, key_batches: &[RecordBatch]) -> Result<ColumnarZSet> {
        let total_start = profile::start();
        let phase_start = profile::start();
        let lookup_keys = self.lookup_keys_from_batches(key_batches)?;
        if lookup_keys.is_empty() {
            profile::record_since("columnar_index.lookup.key_bytes", phase_start);
            profile::record_since("columnar_index.lookup.total", total_start);
            return ColumnarZSet::empty(Arc::clone(&self.value_schema));
        }
        profile::record_since("columnar_index.lookup.key_bytes", phase_start);
        let phase_start = profile::start();
        if !lookup_keys.overlaps_persisted_bounds(self.key_bounds.as_ref()) {
            profile::record_since("columnar_index.lookup.bounds_check", phase_start);
            profile::record_since("columnar_index.lookup.total", total_start);
            return ColumnarZSet::empty(Arc::clone(&self.value_schema));
        }
        profile::record_since("columnar_index.lookup.bounds_check", phase_start);

        if self.current_lookup_enabled {
            let phase_start = profile::start();
            let output = self
                .lookup_current_key_batches_with_historical_fallback(&lookup_keys)
                .await;
            profile::record_since("columnar_index.lookup.current", phase_start);
            profile::record_since("columnar_index.lookup.total", total_start);
            return output;
        }

        let phase_start = profile::start();
        let output = self.lookup_historical_key_batches(&lookup_keys).await;
        profile::record_since("columnar_index.lookup.historical", phase_start);
        profile::record_since("columnar_index.lookup.total", total_start);
        output
    }

    async fn lookup_historical_key_batches(
        &self,
        lookup_keys: &LookupKeySet,
    ) -> Result<ColumnarZSet> {
        let phase_start = profile::start();
        let mut inline_batches = Vec::new();
        let mut refs_by_segment: HashMap<u64, Vec<u32>> = HashMap::new();
        self.lookup_key_ranges(lookup_keys, &mut inline_batches, &mut refs_by_segment)
            .await?;
        if !self.range_lookup_only {
            self.lookup_key_buckets(
                &lookup_keys.buckets,
                &mut inline_batches,
                &mut refs_by_segment,
            )
            .await?;
        }
        profile::record_since("columnar_index.lookup.collect_postings", phase_start);

        if inline_batches.is_empty() && refs_by_segment.is_empty() {
            return ColumnarZSet::empty(Arc::clone(&self.value_schema));
        }

        let phase_start = profile::start();
        let mut batches = inline_batches;
        let mut segment_ids = refs_by_segment.keys().copied().collect::<Vec<_>>();
        segment_ids.sort_unstable();
        for segment_id in segment_ids {
            let indices = refs_by_segment
                .remove(&segment_id)
                .expect("segment refs missing");
            batches.extend(self.take_segment_rows(segment_id, indices).await?);
        }
        profile::record_since("columnar_index.lookup.take_segment_rows", phase_start);
        let phase_start = profile::start();
        let output = ColumnarZSet::try_new_weighted(Arc::clone(&self.value_schema), batches)
            .context("build columnar indexed zset lookup result");
        profile::record_since("columnar_index.lookup.build_result_zset", phase_start);
        output
    }

    async fn lookup_key_buckets(
        &self,
        key_buckets: &HashMap<u16, HashSet<Vec<u8>>>,
        inline_batches: &mut Vec<RecordBatch>,
        refs_by_segment: &mut HashMap<u64, Vec<u32>>,
    ) -> Result<()> {
        for (bucket, wanted_keys) in key_buckets {
            let mut visit_entry = |entry_key: &[u8], entry_value: &[u8]| -> Result<()> {
                let segment_id = self.segment_id_from_bucket_key(entry_key)?;
                match decode_bucket_lookup_rows(entry_value, wanted_keys, &self.weighted_schema)? {
                    BucketLookupRows::Inline(batches) => inline_batches.extend(batches),
                    BucketLookupRows::Legacy(row_indices) => {
                        if !row_indices.is_empty() {
                            refs_by_segment
                                .entry(segment_id)
                                .or_default()
                                .extend(row_indices);
                        }
                    }
                }
                Ok(())
            };
            self.table
                .scan_prefix_bytes_for_each(
                    &self.bucket_prefix(*bucket),
                    &indexed_lookup_scan_options(),
                    &mut visit_entry,
                )
                .await
                .context("scan columnar index bucket postings")?;
        }
        Ok(())
    }

    async fn lookup_key_ranges(
        &self,
        lookup_keys: &LookupKeySet,
        inline_batches: &mut Vec<RecordBatch>,
        refs_by_segment: &mut HashMap<u64, Vec<u32>>,
    ) -> Result<()> {
        let Some(bounds) = lookup_keys.bounds.as_ref() else {
            return Ok(());
        };
        let mut wanted_keys = HashSet::new();
        for keys in lookup_keys.buckets.values() {
            wanted_keys.extend(keys.iter().cloned());
        }
        if wanted_keys.is_empty() {
            return Ok(());
        }
        let mut visit_entry = |entry_key: &[u8], entry_value: &[u8]| -> Result<()> {
            match decode_range_lookup_rows(
                entry_value,
                &wanted_keys,
                bounds,
                &self.weighted_schema,
            )? {
                RangeLookupRows::Inline(batches) => inline_batches.extend(batches),
                RangeLookupRows::SegmentRefs(row_indices) => {
                    if !row_indices.is_empty() {
                        let segment_id = self.segment_id_from_range_key(entry_key)?;
                        refs_by_segment
                            .entry(segment_id)
                            .or_default()
                            .extend(row_indices);
                    }
                }
            }
            Ok(())
        };
        self.table
            .scan_prefix_bytes_for_each(
                &self.range_prefix,
                &indexed_lookup_scan_options(),
                &mut visit_entry,
            )
            .await
            .context("scan columnar range index postings")?;
        Ok(())
    }

    async fn clear_persisted(&self) -> Result<()> {
        let mut write_batch = WriteBatch::new();
        for (key, _) in self
            .table
            .scan_prefix_bytes(&self.index_prefix, &ScanOptions::default())
            .await
            .context("scan columnar index postings for rebuild")?
        {
            write_batch.delete(&key);
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
        write_batch.delete(self.range_state_key.clone());
        for (key, _) in self
            .table
            .scan_prefix_bytes(&self.current_prefix, &ScanOptions::default())
            .await
            .context("scan columnar current index for rebuild")?
        {
            write_batch.delete(&key);
        }
        write_batch.delete(self.current_state_key.clone());
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

    fn index_postings_for_batch(&self, batch: &RecordBatch) -> Result<IndexPostingsByKey> {
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
        let mut postings = IndexPostingsByKey::new();
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

    async fn current_mutations_for_postings(
        &self,
        batch: &RecordBatch,
        postings: &HashMap<Vec<u8>, Vec<(u32, i64)>>,
    ) -> Result<Vec<CurrentIndexMutation>> {
        let mut mutations = Vec::with_capacity(postings.len());
        for (lookup_key, row_refs) in postings {
            let row_indices = row_refs
                .iter()
                .map(|(row_idx, _)| *row_idx)
                .collect::<Vec<_>>();
            let delta_batch = take_batch_rows(batch, &row_indices)?;
            let delta_zset =
                ColumnarZSet::try_new_weighted(Arc::clone(&self.value_schema), vec![delta_batch])
                    .context("build columnar current index delta zset")?;
            let Some(mut current_zset) = self
                .current_zset_for_key(lookup_key)
                .await
                .context("load columnar current index entry")?
            else {
                continue;
            };
            current_zset
                .extend(delta_zset)
                .context("merge columnar current index delta")?;
            let current_zset = consolidate_columnar_zset(current_zset)
                .context("consolidate columnar current index entry")?;
            let key = self.current_entry_key(lookup_key);
            let value = encode_current_payload(&self.weighted_schema, current_zset.batches())
                .context("encode columnar current index entry")?;
            mutations.push(CurrentIndexMutation::Put { key, value });
        }
        Ok(mutations)
    }

    async fn current_zset_for_key(&self, lookup_key: &[u8]) -> Result<Option<ColumnarZSet>> {
        let Some(bytes) = self
            .table
            .get_bytes(&self.current_entry_key(lookup_key))
            .await
            .context("read columnar current index entry")?
        else {
            return Ok(None);
        };
        let batches = decode_current_payload(&self.weighted_schema, bytes.as_ref())
            .context("decode columnar current index entry")?;
        ColumnarZSet::try_new_weighted(Arc::clone(&self.value_schema), batches)
            .context("build columnar current index zset")
            .map(Some)
    }

    async fn lookup_current_key_batches_with_historical_fallback(
        &self,
        lookup_keys: &LookupKeySet,
    ) -> Result<ColumnarZSet> {
        let mut batches = Vec::new();
        let mut missing_buckets: HashMap<u16, HashSet<Vec<u8>>> = HashMap::new();
        let mut missing_len = 0_usize;
        for (bucket, keys) in &lookup_keys.buckets {
            for lookup_key in keys {
                if let Some(bytes) = self
                    .table
                    .get_bytes(&self.current_entry_key(lookup_key))
                    .await
                    .context("read columnar current index lookup entry")?
                {
                    batches.extend(
                        decode_current_payload(&self.weighted_schema, bytes.as_ref())
                            .context("decode columnar current index lookup entry")?,
                    );
                } else {
                    missing_buckets
                        .entry(*bucket)
                        .or_default()
                        .insert(lookup_key.clone());
                    missing_len = missing_len.saturating_add(1);
                }
            }
        }
        if missing_len > 0 {
            let missing = LookupKeySet {
                buckets: missing_buckets,
                bounds: lookup_keys.bounds.clone(),
                len: missing_len,
            };
            let historical = self
                .lookup_historical_key_batches(&missing)
                .await
                .context("lookup missing columnar current index entries from history")?;
            let current = consolidate_columnar_zset(historical)
                .context("consolidate missing columnar current index entries")?;
            self.write_current_entries_for_lookup_keys(&missing, &current)
                .await
                .context("persist lazy columnar current index entries")?;
            batches.extend(current.batches().to_vec());
        }
        ColumnarZSet::try_new_weighted(Arc::clone(&self.value_schema), batches)
            .context("build columnar current index lookup result")
    }

    async fn write_current_entries_for_lookup_keys(
        &self,
        lookup_keys: &LookupKeySet,
        current: &ColumnarZSet,
    ) -> Result<()> {
        let mut postings: HashMap<Vec<u8>, Vec<(u32, i64)>> = HashMap::new();
        let current_batch = if current.is_empty() {
            None
        } else {
            let batch = concat_batches(&current.weighted_schema(), current.batches())
                .context("concat lazy columnar current index batches")?;
            postings = self
                .index_postings_for_batch(&batch)
                .context("index lazy columnar current rows")?;
            Some(batch)
        };
        let empty_payload =
            encode_current_payload(&self.weighted_schema, &[]).context("encode empty current")?;
        let mut write_batch = WriteBatch::new();
        for lookup_key in lookup_keys.buckets.values().flat_map(|keys| keys.iter()) {
            let key = self.current_entry_key(lookup_key);
            if let (Some(batch), Some(row_refs)) =
                (current_batch.as_ref(), postings.get(lookup_key))
            {
                let row_indices = row_refs
                    .iter()
                    .map(|(row_idx, _)| *row_idx)
                    .collect::<Vec<_>>();
                let value_batch = take_batch_rows(batch, &row_indices)?;
                let value = encode_current_payload(&self.weighted_schema, &[value_batch])
                    .context("encode lazy columnar current entry")?;
                write_batch.put_bytes(Bytes::from(key), Bytes::from(value));
            } else {
                write_batch.put_bytes(Bytes::from(key), Bytes::from(empty_payload.clone()));
            }
        }
        self.table
            .write_batch(write_batch)
            .await
            .context("write lazy columnar current index entries")?;
        Ok(())
    }

    fn lookup_keys_from_batches(&self, key_batches: &[RecordBatch]) -> Result<LookupKeySet> {
        let mut buckets: HashMap<u16, HashSet<Vec<u8>>> = HashMap::new();
        let mut min = None::<Vec<u8>>;
        let mut max = None::<Vec<u8>>;
        let mut len = 0_usize;
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
                let key = rows.row(row_idx).data();
                let bucket = index_bucket(key);
                let bucket_keys = buckets.entry(bucket).or_default();
                if bucket_keys.contains(key) {
                    continue;
                }
                let key = key.to_vec();
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
                bucket_keys.insert(key);
                len = len.saturating_add(1);
            }
        }
        let bounds = min.zip(max).map(|(min, max)| IndexKeyBounds { min, max });
        Ok(LookupKeySet {
            buckets,
            bounds,
            len,
        })
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

    fn bucket_prefix(&self, bucket: u16) -> Vec<u8> {
        let mut prefix = self.index_prefix.clone();
        prefix.push(INDEX_BUCKET_TAG);
        prefix.extend_from_slice(&bucket.to_be_bytes());
        prefix
    }

    fn range_index_key(&self, segment_id: u64, chunk_id: usize) -> Result<Vec<u8>> {
        let chunk_id =
            u32::try_from(chunk_id).context("columnar index range chunk id too large")?;
        let mut key = self.range_prefix.clone();
        key.extend_from_slice(&segment_id.to_be_bytes());
        key.extend_from_slice(&chunk_id.to_be_bytes());
        Ok(key)
    }

    fn segment_id_from_range_key(&self, key: &[u8]) -> Result<u64> {
        let prefix_len = self.range_prefix.len();
        if key.len() != prefix_len + 12 || !key.starts_with(&self.range_prefix) {
            bail!("columnar index range key prefix mismatch");
        }
        let suffix = key
            .get(prefix_len..prefix_len + 8)
            .ok_or_else(|| anyhow!("columnar index range segment id truncated"))?;
        Ok(u64::from_be_bytes(suffix.try_into()?))
    }

    fn segment_id_from_bucket_key(&self, key: &[u8]) -> Result<u64> {
        let prefix_len = self.index_prefix.len() + 1 + 2;
        if key.len() != prefix_len + 8 {
            bail!("columnar index bucket key has invalid length");
        }
        if !key.starts_with(&self.index_prefix) || key[self.index_prefix.len()] != INDEX_BUCKET_TAG
        {
            bail!("columnar index bucket key prefix mismatch");
        }
        let suffix = key
            .get(prefix_len..prefix_len + 8)
            .ok_or_else(|| anyhow!("columnar index bucket segment id truncated"))?;
        Ok(u64::from_be_bytes(suffix.try_into()?))
    }

    fn current_entry_key(&self, lookup_key: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.current_prefix.len() + lookup_key.len());
        key.extend_from_slice(&self.current_prefix);
        key.extend_from_slice(lookup_key);
        key
    }
}

enum CurrentIndexMutation {
    Put { key: Vec<u8>, value: Vec<u8> },
}

enum BucketLookupRows {
    Inline(Vec<RecordBatch>),
    Legacy(Vec<u32>),
}

enum RangeLookupRows {
    Inline(Vec<RecordBatch>),
    SegmentRefs(Vec<u32>),
}

fn indexed_lookup_scan_options() -> ScanOptions {
    ScanOptions {
        read_ahead_bytes: 1024 * 1024,
        cache_blocks: true,
        max_fetch_tasks: 4,
        ..ScanOptions::default()
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

async fn read_bool_state(table: &dyn KeyValueTable, state_key: &[u8]) -> Result<bool> {
    Ok(table
        .get_bytes(state_key)
        .await
        .context("read columnar index boolean state")?
        .is_some_and(|bytes| bytes.as_ref() == [1]))
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

fn range_posting_chunks(postings: IndexPostingsByKey) -> IndexEntryChunks {
    let mut entries = postings.into_iter().collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    let mut chunks = Vec::new();
    let mut chunk = Vec::new();
    let mut chunk_rows = 0usize;
    for (key, postings) in entries {
        let posting_count = postings.len();
        if !chunk.is_empty() && chunk_rows.saturating_add(posting_count) > INDEX_RANGE_TARGET_ROWS {
            chunks.push(std::mem::take(&mut chunk));
            chunk_rows = 0;
        }
        chunk_rows = chunk_rows.saturating_add(posting_count);
        chunk.push((key, postings));
    }
    if !chunk.is_empty() {
        chunks.push(chunk);
    }
    chunks
}

fn index_bucket(key: &[u8]) -> u16 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % u64::from(INDEX_BUCKET_COUNT)) as u16
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
    if indices.len() == batch.num_rows()
        && indices
            .iter()
            .enumerate()
            .all(|(expected, actual)| usize::try_from(*actual).is_ok_and(|idx| idx == expected))
    {
        return Ok(batch.clone());
    }
    let indices = UInt32Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<ArrayRef>, _>>()
        .context("take columnar indexed zset rows")?;
    RecordBatch::try_new(batch.schema(), columns).context("build taken columnar indexed zset batch")
}

fn encode_current_payload(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    encode_record_batches(Arc::clone(schema), batches).context("encode columnar current payload")
}

fn decode_current_payload(schema: &SchemaRef, bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let (decoded_schema, batches) =
        decode_record_batches(bytes).context("decode columnar current payload")?;
    if decoded_schema.as_ref() != schema.as_ref() {
        bail!("columnar current payload schema mismatch");
    }
    Ok(batches)
}

fn weight_column(batch: &RecordBatch, value_column_count: usize) -> Result<&Int64Array> {
    batch
        .column(value_column_count)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("columnar indexed zset weight column is not Int64"))
}

fn encode_bucket_postings_with_inline_rows(
    schema: &SchemaRef,
    batch: &RecordBatch,
    entries: &[IndexEntry],
) -> Result<Vec<u8>> {
    let mut global_row_indices = entries
        .iter()
        .flat_map(|(_, postings)| postings.iter().map(|(row_idx, _)| *row_idx))
        .collect::<Vec<_>>();
    global_row_indices.sort_unstable();
    global_row_indices.dedup();

    let bucket_batch = take_batch_rows(batch, &global_row_indices)?;
    let row_map = global_row_indices
        .iter()
        .enumerate()
        .map(|(local_idx, global_idx)| {
            let local_idx =
                u32::try_from(local_idx).context("columnar index bucket row index exceeds u32")?;
            Ok((*global_idx, local_idx))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    let local_entries = entries
        .iter()
        .map(|(key, postings)| {
            let local_postings = postings
                .iter()
                .map(|(row_idx, weight)| {
                    let local_idx = row_map
                        .get(row_idx)
                        .copied()
                        .ok_or_else(|| anyhow!("columnar index bucket row map missing row"))?;
                    Ok((local_idx, *weight))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((key.clone(), local_postings))
        })
        .collect::<Result<Vec<_>>>()?;

    let postings = encode_bucket_postings(&local_entries)?;
    let rows = encode_record_batches(Arc::clone(schema), &[bucket_batch])
        .context("encode columnar index inline bucket rows")?;
    let postings_len =
        u32::try_from(postings.len()).context("columnar index bucket postings too large")?;
    let rows_len = u32::try_from(rows.len()).context("columnar index inline rows too large")?;

    let mut out =
        Vec::with_capacity(INDEX_BUCKET_INLINE_MAGIC.len() + 1 + 8 + postings.len() + rows.len());
    out.extend_from_slice(INDEX_BUCKET_INLINE_MAGIC);
    out.push(INDEX_BUCKET_INLINE_VERSION);
    out.extend_from_slice(&postings_len.to_be_bytes());
    out.extend_from_slice(&postings);
    out.extend_from_slice(&rows_len.to_be_bytes());
    out.extend_from_slice(&rows);
    Ok(out)
}

fn encode_range_postings_with_inline_rows(
    schema: &SchemaRef,
    batch: &RecordBatch,
    entries: &[IndexEntry],
) -> Result<Vec<u8>> {
    let min_key = entries
        .first()
        .map(|(key, _)| key.as_slice())
        .context("columnar range index entry missing min key")?;
    let max_key = entries
        .last()
        .map(|(key, _)| key.as_slice())
        .context("columnar range index entry missing max key")?;
    let inline_payload = encode_bucket_postings_with_inline_rows(schema, batch, entries)?;
    let min_len = u32::try_from(min_key.len()).context("columnar range min key too large")?;
    let max_len = u32::try_from(max_key.len()).context("columnar range max key too large")?;
    let payload_len =
        u32::try_from(inline_payload.len()).context("columnar range inline payload too large")?;
    let mut out = Vec::with_capacity(
        INDEX_RANGE_INLINE_MAGIC.len()
            + 1
            + 12
            + min_key.len()
            + max_key.len()
            + inline_payload.len(),
    );
    out.extend_from_slice(INDEX_RANGE_INLINE_MAGIC);
    out.push(INDEX_RANGE_INLINE_VERSION);
    out.extend_from_slice(&min_len.to_be_bytes());
    out.extend_from_slice(min_key);
    out.extend_from_slice(&max_len.to_be_bytes());
    out.extend_from_slice(max_key);
    out.extend_from_slice(&payload_len.to_be_bytes());
    out.extend_from_slice(&inline_payload);
    Ok(out)
}

fn encode_range_postings_with_segment_rows(entries: &[IndexEntry]) -> Result<Vec<u8>> {
    let min_key = entries
        .first()
        .map(|(key, _)| key.as_slice())
        .context("columnar range index entry missing min key")?;
    let max_key = entries
        .last()
        .map(|(key, _)| key.as_slice())
        .context("columnar range index entry missing max key")?;
    let postings = encode_bucket_postings(entries)?;
    let min_len = u32::try_from(min_key.len()).context("columnar range min key too large")?;
    let max_len = u32::try_from(max_key.len()).context("columnar range max key too large")?;
    let payload_len =
        u32::try_from(postings.len()).context("columnar range postings payload too large")?;
    let mut out = Vec::with_capacity(
        INDEX_RANGE_SEGMENT_MAGIC.len() + 1 + 12 + min_key.len() + max_key.len() + postings.len(),
    );
    out.extend_from_slice(INDEX_RANGE_SEGMENT_MAGIC);
    out.push(INDEX_RANGE_SEGMENT_VERSION);
    out.extend_from_slice(&min_len.to_be_bytes());
    out.extend_from_slice(min_key);
    out.extend_from_slice(&max_len.to_be_bytes());
    out.extend_from_slice(max_key);
    out.extend_from_slice(&payload_len.to_be_bytes());
    out.extend_from_slice(&postings);
    Ok(out)
}

fn encode_bucket_postings(entries: &[IndexEntry]) -> Result<Vec<u8>> {
    let mut capacity = 4;
    for (key, postings) in entries {
        capacity += 8 + key.len() + postings.len() * 12;
    }
    let mut out = Vec::with_capacity(capacity);
    let entry_count = u32::try_from(entries.len()).context("columnar index bucket too large")?;
    out.extend_from_slice(&entry_count.to_be_bytes());
    for (key, postings) in entries {
        let key_len = u32::try_from(key.len()).context("columnar index key too large")?;
        let posting_count =
            u32::try_from(postings.len()).context("columnar index posting count too large")?;
        out.extend_from_slice(&key_len.to_be_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(&posting_count.to_be_bytes());
        for (row_index, delta) in postings {
            out.extend_from_slice(&row_index.to_be_bytes());
            out.extend_from_slice(&delta.to_be_bytes());
        }
    }
    Ok(out)
}

fn decode_bucket_lookup_rows(
    bytes: &[u8],
    wanted_keys: &HashSet<Vec<u8>>,
    schema: &SchemaRef,
) -> Result<BucketLookupRows> {
    if !bytes.starts_with(INDEX_BUCKET_INLINE_MAGIC) {
        let mut row_indices = Vec::new();
        decode_bucket_postings_for_keys(bytes, wanted_keys, |row_index, weight| {
            if weight != 0 {
                row_indices.push(row_index);
            }
            Ok(())
        })?;
        return Ok(BucketLookupRows::Legacy(row_indices));
    }

    let mut cursor = INDEX_BUCKET_INLINE_MAGIC.len();
    let version = read_u8(bytes, &mut cursor).context("decode columnar index bucket version")?;
    if version != INDEX_BUCKET_INLINE_VERSION {
        bail!("unsupported columnar index inline bucket version {version}");
    }
    let postings_len = usize::try_from(
        read_u32(bytes, &mut cursor).context("decode columnar index inline postings length")?,
    )
    .context("columnar index inline postings length out of range")?;
    let postings = read_bytes(
        bytes,
        &mut cursor,
        postings_len,
        "columnar index inline postings",
    )?;
    let rows_len = usize::try_from(
        read_u32(bytes, &mut cursor).context("decode columnar index inline rows length")?,
    )
    .context("columnar index inline rows length out of range")?;
    let rows = read_bytes(
        bytes,
        &mut cursor,
        rows_len,
        "columnar index inline bucket rows",
    )?;
    if cursor != bytes.len() {
        bail!("columnar index inline bucket payload has trailing bytes");
    }

    let mut local_row_indices = Vec::new();
    decode_bucket_postings_for_keys(postings, wanted_keys, |row_index, weight| {
        if weight != 0 {
            local_row_indices.push(row_index);
        }
        Ok(())
    })?;
    if local_row_indices.is_empty() {
        return Ok(BucketLookupRows::Inline(Vec::new()));
    }

    let (decoded_schema, batches) =
        decode_record_batches(rows).context("decode columnar index inline bucket rows")?;
    if decoded_schema.as_ref() != schema.as_ref() {
        bail!("columnar index inline bucket row schema mismatch");
    }
    let batch = match batches.as_slice() {
        [batch] => batch.clone(),
        _ => concat_batches(schema, &batches).context("concat columnar index inline rows")?,
    };
    Ok(BucketLookupRows::Inline(vec![take_batch_rows(
        &batch,
        &local_row_indices,
    )?]))
}

fn decode_range_lookup_rows(
    bytes: &[u8],
    wanted_keys: &HashSet<Vec<u8>>,
    lookup_bounds: &IndexKeyBounds,
    schema: &SchemaRef,
) -> Result<RangeLookupRows> {
    let inline_rows = if bytes.starts_with(INDEX_RANGE_INLINE_MAGIC) {
        true
    } else if bytes.starts_with(INDEX_RANGE_SEGMENT_MAGIC) {
        false
    } else {
        bail!("columnar range index payload magic mismatch");
    };
    let mut cursor = INDEX_RANGE_INLINE_MAGIC.len();
    let version =
        read_u8(bytes, &mut cursor).context("decode columnar range index payload version")?;
    let expected_version = if inline_rows {
        INDEX_RANGE_INLINE_VERSION
    } else {
        INDEX_RANGE_SEGMENT_VERSION
    };
    if version != expected_version {
        bail!("unsupported columnar range index payload version {version}");
    }
    let min_len = usize::try_from(
        read_u32(bytes, &mut cursor).context("decode columnar range index min key length")?,
    )
    .context("columnar range index min key length out of range")?;
    let min_key = read_bytes(bytes, &mut cursor, min_len, "columnar range index min key")?;
    let max_len = usize::try_from(
        read_u32(bytes, &mut cursor).context("decode columnar range index max key length")?,
    )
    .context("columnar range index max key length out of range")?;
    let max_key = read_bytes(bytes, &mut cursor, max_len, "columnar range index max key")?;
    let payload_len = usize::try_from(
        read_u32(bytes, &mut cursor).context("decode columnar range index payload length")?,
    )
    .context("columnar range index payload length out of range")?;
    let payload = read_bytes(
        bytes,
        &mut cursor,
        payload_len,
        "columnar range index inline payload",
    )?;
    if cursor != bytes.len() {
        bail!("columnar range index payload has trailing bytes");
    }

    if max_key < lookup_bounds.min.as_slice() || min_key > lookup_bounds.max.as_slice() {
        return if inline_rows {
            Ok(RangeLookupRows::Inline(Vec::new()))
        } else {
            Ok(RangeLookupRows::SegmentRefs(Vec::new()))
        };
    }
    if inline_rows {
        match decode_bucket_lookup_rows(payload, wanted_keys, schema)? {
            BucketLookupRows::Inline(batches) => Ok(RangeLookupRows::Inline(batches)),
            BucketLookupRows::Legacy(_) => bail!("columnar range index nested legacy payload"),
        }
    } else {
        let mut row_indices = Vec::new();
        decode_bucket_postings_for_keys(payload, wanted_keys, |row_index, weight| {
            if weight != 0 {
                row_indices.push(row_index);
            }
            Ok(())
        })?;
        Ok(RangeLookupRows::SegmentRefs(row_indices))
    }
}

fn decode_bucket_postings_for_keys(
    bytes: &[u8],
    wanted_keys: &HashSet<Vec<u8>>,
    mut emit: impl FnMut(u32, i64) -> Result<()>,
) -> Result<()> {
    let mut cursor = 0;
    let entry_count =
        read_u32(bytes, &mut cursor).context("decode columnar index bucket entry count")?;
    for _ in 0..entry_count {
        let key_len = usize::try_from(
            read_u32(bytes, &mut cursor).context("decode columnar index bucket key length")?,
        )
        .context("columnar index bucket key length out of range")?;
        let key = read_bytes(bytes, &mut cursor, key_len, "columnar index bucket key")?;
        let posting_count =
            read_u32(bytes, &mut cursor).context("decode columnar index bucket posting count")?;
        let wanted = wanted_keys.contains(key);
        if wanted {
            for _ in 0..posting_count {
                let row_index = read_u32(bytes, &mut cursor)
                    .context("decode columnar index posting row index")?;
                let weight =
                    read_i64(bytes, &mut cursor).context("decode columnar index posting weight")?;
                emit(row_index, weight)?;
            }
        } else {
            skip_bytes(
                bytes,
                &mut cursor,
                usize::try_from(posting_count)
                    .context("columnar index posting count out of range")?
                    .checked_mul(12)
                    .ok_or_else(|| anyhow!("columnar index posting payload overflow"))?,
                "columnar index skipped postings",
            )?;
        }
    }
    if cursor != bytes.len() {
        bail!("columnar index bucket postings payload has trailing bytes");
    }
    Ok(())
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8> {
    Ok(read_exact_at::<1>(bytes, cursor, "columnar index u8")?[0])
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

fn skip_bytes(bytes: &[u8], cursor: &mut usize, len: usize, label: &str) -> Result<()> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| anyhow!("{label} overflow"))?;
    bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow!("{label} truncated"))?;
    *cursor = end;
    Ok(())
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
    async fn columnar_current_index_lookup_returns_consolidated_state() {
        let inner = build_table("columnar-current-index-lookup").await;
        let counting = Arc::new(CountingTable::new(inner));
        let table: Arc<dyn KeyValueTable> = counting.clone();
        let mut index = SlateBackedColumnarIndexedZSet::new_with_current_lookup(
            table,
            "orders_by_id",
            value_schema(),
            vec![0],
        )
        .await
        .expect("index");
        let insert = ColumnarZSet::try_new_weighted(
            value_schema(),
            vec![weighted_batch(vec![1], vec!["a"], vec![10], vec![1])],
        )
        .expect("insert");
        index.apply_delta(&insert).await.expect("apply insert");

        counting.reset_scan_range_calls();
        let found = index
            .lookup_key_batches(&[key_batch(vec![1])])
            .await
            .expect("lookup inserted row");
        assert_eq!(lookup_rows(&found), vec![(1, "a".to_string(), 10, 1)]);
        assert!(counting.scan_range_calls() > 0);

        counting.reset_scan_range_calls();
        let found = index
            .lookup_key_batches(&[key_batch(vec![1])])
            .await
            .expect("lookup materialized row");
        assert_eq!(lookup_rows(&found), vec![(1, "a".to_string(), 10, 1)]);
        assert_eq!(counting.scan_range_calls(), 0);

        let delete = ColumnarZSet::try_new_weighted(
            value_schema(),
            vec![weighted_batch(vec![1], vec!["a"], vec![10], vec![-1])],
        )
        .expect("delete");
        index.apply_delta(&delete).await.expect("apply delete");
        let found = index
            .lookup_key_batches(&[key_batch(vec![1])])
            .await
            .expect("lookup deleted row");
        assert!(lookup_rows(&found).is_empty());
        assert_eq!(counting.scan_range_calls(), 0);
    }

    #[tokio::test]
    async fn columnar_index_large_lookup_uses_bounded_bucket_scans() {
        let inner = build_table("columnar-index-large-lookup").await;
        let counting = Arc::new(CountingTable::new(inner));
        let table: Arc<dyn KeyValueTable> = counting.clone();
        let mut index =
            SlateBackedColumnarIndexedZSet::new(table, "orders_by_id", value_schema(), vec![0])
                .await
                .expect("index");
        let rows = usize::from(INDEX_BUCKET_COUNT) * 2;
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
        assert!(counting.scan_range_calls() > 0);
        assert!(counting.scan_range_calls() <= usize::from(INDEX_BUCKET_COUNT));
    }

    #[tokio::test]
    async fn columnar_index_large_delta_uses_segment_backed_range_rows() {
        let table = build_table("columnar-index-segment-backed-range").await;
        let mut index = SlateBackedColumnarIndexedZSet::new_with_segment_backed_large_ranges(
            table,
            "orders_by_id",
            value_schema(),
            vec![0],
        )
        .await
        .expect("index");
        let rows = INDEX_INLINE_ROW_MAX_ROWS + 16;
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

        let found = index
            .lookup_key_batches(&[key_batch(vec![7, 1024, rows as i64 - 1])])
            .await
            .expect("lookup");
        assert_eq!(
            lookup_rows(&found),
            vec![
                (7, "n".to_string(), 70, 1),
                (1024, "n".to_string(), 10240, 1),
                (rows as i64 - 1, "n".to_string(), (rows as i64 - 1) * 10, 1),
            ]
        );
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
