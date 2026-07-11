use super::*;

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
    pub async fn compact_l0_to_l1(&self) -> Result<usize> {
        if self.reverse_enabled || self.range_enabled {
            return Ok(0);
        }

        let mut keys = HashSet::new();
        for (entry_key, _) in self
            .table
            .scan_prefix(&self.index_prefix, &ScanOptions::default())
            .await
            .context("scan Arrow-index keys for compaction")?
        {
            let (key_bytes, _) = self
                .decode_index_key(&entry_key)
                .context("decode Arrow-index key during compaction")?;
            keys.insert(key_bytes);
        }

        let mut compacted = 0usize;
        for key_bytes in keys {
            if self
                .compact_key_bytes(&key_bytes)
                .await
                .context("compact Arrow-index key")?
            {
                compacted = compacted.saturating_add(1);
            }
        }
        Ok(compacted)
    }

    pub async fn estimated_read_amplification_for_key(&self, key: &K) -> Result<usize> {
        let key_bytes = encode(key).context("encode key for Arrow-index amplification estimate")?;
        self.estimated_read_amplification_for_key_bytes(&key_bytes)
            .await
    }

    pub(super) async fn estimated_read_amplification_for_key_bytes(
        &self,
        key_bytes: &[u8],
    ) -> Result<usize> {
        let entries = self
            .table
            .scan_prefix(
                &self
                    .index_prefix_for_key(key_bytes)
                    .context("build Arrow-index key prefix for amplification")?,
                &ScanOptions::default(),
            )
            .await
            .context("scan Arrow-index entries for amplification estimate")?;
        Ok(entries.len())
    }

    pub(super) async fn compact_hot_keys(&self, key_bytes: &[Vec<u8>]) -> Result<()> {
        let Some(threshold) = self.hot_key_compaction_threshold else {
            return Ok(());
        };
        if self.reverse_enabled || self.range_enabled {
            return Ok(());
        }

        let mut seen = HashSet::new();
        for key in key_bytes {
            if !seen.insert(key.clone()) {
                continue;
            }
            let read_amplification = self
                .estimated_read_amplification_for_key_bytes(key)
                .await
                .context("estimate Arrow-index hot-key read amplification")?;
            if read_amplification > threshold {
                self.compact_key_bytes(key)
                    .await
                    .context("compact Arrow-index hot key")?;
            }
        }
        Ok(())
    }

    pub(super) async fn compact_key_bytes(&self, key_bytes: &[u8]) -> Result<bool> {
        if self.reverse_enabled || self.range_enabled {
            return Ok(false);
        }

        let _segment_guard = self.segment_sequence_lock.lock().await;
        let key_prefix = self
            .index_prefix_for_key(key_bytes)
            .context("build Arrow-index key prefix for compaction")?;
        let entries = self
            .table
            .scan_prefix(&key_prefix, &ScanOptions::default())
            .await
            .context("scan Arrow-index key entries for compaction")?;
        if entries.len() <= 1 {
            return Ok(false);
        }

        let aggregate = self
            .load_persisted_value_weights_for_key(key_bytes)
            .await
            .context("load Arrow-index key aggregate for compaction")?;
        let mut write_batch = WriteBatch::new();
        for (entry_key, _) in entries {
            write_batch.delete(entry_key);
        }

        if aggregate.is_empty() {
            self.table
                .write_batch(write_batch)
                .await
                .context("delete empty Arrow-index key postings during compaction")?;
            self.store_lookup_cache_for_key(key_bytes, &aggregate)?;
            return Ok(true);
        }

        let segment_id = self.read_next_segment_id().await?;
        write_batch.put(
            self.segment_sequence_key.clone(),
            segment_id.saturating_add(1).to_be_bytes(),
        );

        let rows = aggregate
            .iter()
            .map(|(value_bytes, weight)| (key_bytes.to_vec(), value_bytes.clone(), *weight))
            .collect::<Vec<_>>();
        let batch = self.record_batch_from_rows(&rows)?;
        let tombstones = rows.iter().filter(|(_, _, weight)| *weight < 0).count();
        let tombstone_ratio = tombstones as f64 / rows.len() as f64;
        let key_hash = hash_bytes(key_bytes);
        let stats = SegmentWriteStats::new(key_hash, key_hash, tombstone_ratio)
            .context("build compacted Arrow-index segment stats")?;
        let (segment_bytes, _) = encode_segment_envelope(Arc::clone(&self.schema), &[batch], stats)
            .context("encode compacted Arrow-index segment envelope")?;
        write_batch.put(
            self.segment_store.key_for_segment(segment_id),
            segment_bytes,
        );

        let postings = rows
            .iter()
            .enumerate()
            .map(|(idx, (_, _, weight))| {
                u32::try_from(idx)
                    .map(|row_idx| (row_idx, *weight))
                    .map_err(|_| anyhow!("row index overflow while compacting Arrow-index key"))
            })
            .collect::<Result<Vec<_>>>()?;
        write_batch.put(
            self.index_key(key_bytes, segment_id)
                .context("build compacted Arrow-index posting key")?,
            encode_index_postings(&postings),
        );

        self.table
            .write_batch(write_batch)
            .await
            .context("persist compacted Arrow-index key")?;
        self.record_checkpoint(segment_id.saturating_add(1));
        self.insert_segment_cache(
            segment_id,
            Arc::new(CachedSegment {
                values: rows
                    .iter()
                    .map(|(_, value_bytes, _)| value_bytes.clone())
                    .collect(),
            }),
        )?;
        self.store_lookup_cache_for_key(key_bytes, &aggregate)?;
        Ok(true)
    }

    pub(super) async fn segment_refs_for_key_with_metrics(
        &self,
        key_bytes: &[u8],
    ) -> Result<(FastMap<u64, Vec<(u32, i64)>>, LookupMetrics)> {
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

        let mut metrics = LookupMetrics {
            index_segments_examined: entries.len(),
            ..LookupMetrics::default()
        };
        let mut refs: FastMap<u64, Vec<(u32, i64)>> = FastMap::default();
        for (entry_key, entry_value) in entries {
            let (_key, segment_id) = self
                .decode_index_key(&entry_key)
                .context("decode Arrow-index key")?;
            let postings = decode_index_postings(&entry_value)?;
            metrics.index_postings_examined = metrics
                .index_postings_examined
                .saturating_add(postings.len());
            refs.entry(segment_id).or_default().extend(postings);
        }
        Ok((refs, metrics))
    }

    pub(super) async fn truncate_to_next_segment(&self, next_segment_id: u64) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(
            self.segment_sequence_key.clone(),
            next_segment_id.to_be_bytes(),
        );

        for segment_id in self
            .segment_store
            .list_segment_ids()
            .await
            .context("list Arrow-index segments for checkpoint truncate")?
        {
            if segment_id >= next_segment_id {
                batch.delete(self.segment_store.key_for_segment(segment_id));
            }
        }

        self.delete_postings_at_or_after(&mut batch, &self.index_prefix, next_segment_id, |key| {
            self.decode_index_key(key).map(|(_, segment_id)| segment_id)
        })
        .await?;
        self.delete_postings_at_or_after(
            &mut batch,
            &self.reverse_prefix,
            next_segment_id,
            |key| {
                self.decode_reverse_key(key)
                    .map(|(_, segment_id)| segment_id)
            },
        )
        .await?;
        self.delete_postings_at_or_after(&mut batch, &self.range_prefix, next_segment_id, |key| {
            segment_id_from_key_suffix(key)
        })
        .await?;

        self.table
            .write_batch(batch)
            .await
            .context("truncate Arrow-index to committed checkpoint")?;
        self.clear_caches()?;
        Ok(())
    }

    pub(super) async fn delete_postings_at_or_after<F>(
        &self,
        batch: &mut WriteBatch,
        prefix: &[u8],
        next_segment_id: u64,
        decode_segment_id: F,
    ) -> Result<()>
    where
        F: Fn(&[u8]) -> Result<u64>,
    {
        for (key, _) in self
            .table
            .scan_prefix(prefix, &ScanOptions::default())
            .await
            .context("scan Arrow-index postings for checkpoint truncate")?
        {
            let segment_id = decode_segment_id(&key)?;
            if segment_id >= next_segment_id {
                batch.delete(key);
            }
        }
        Ok(())
    }

    pub(super) fn record_checkpoint(&self, next_segment_id: u64) {
        operator_state_registry::record_operator_state(
            self.namespace.clone(),
            ZSetHandle {
                ns: self.namespace.clone(),
                version: next_segment_id,
            },
        );
    }

    pub(super) fn clear_caches(&self) -> Result<()> {
        for shard in &self.lookup_cache_shards {
            shard
                .lock()
                .map_err(|_| anyhow!("Arrow-index lookup cache shard poisoned"))?
                .clear();
        }
        for shard in &self.segment_cache_shards {
            shard
                .lock()
                .map_err(|_| anyhow!("Arrow-index segment cache shard poisoned"))?
                .clear();
        }
        Ok(())
    }

    pub(super) async fn segment_refs_for_value(
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

    pub(super) async fn read_next_segment_id(&self) -> Result<u64> {
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

    pub(super) fn record_batch_from_rows(
        &self,
        rows: &[(Vec<u8>, Vec<u8>, i64)],
    ) -> Result<RecordBatch> {
        let mut value_builder = BinaryBuilder::new();

        for (_, value, _) in rows {
            value_builder.append_value(value);
        }

        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![Arc::new(value_builder.finish()) as ArrayRef],
        )
        .context("build Arrow-index record batch")
    }

    pub(super) fn index_prefix_for_key(&self, key_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut prefix = self.index_prefix.clone();
        prefix.extend_from_slice(&encode_len(key_bytes.len())?);
        prefix.extend_from_slice(key_bytes);
        Ok(prefix)
    }

    pub(super) fn reverse_prefix_for_value(&self, value_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut prefix = self.reverse_prefix.clone();
        prefix.extend_from_slice(&encode_len(value_bytes.len())?);
        prefix.extend_from_slice(value_bytes);
        Ok(prefix)
    }

    pub(super) fn index_key(&self, key_bytes: &[u8], segment_id: u64) -> Result<Vec<u8>> {
        let mut key = self.index_prefix_for_key(key_bytes)?;
        key.extend_from_slice(&segment_id.to_be_bytes());
        Ok(key)
    }

    pub(super) fn reverse_key(&self, value_bytes: &[u8], segment_id: u64) -> Result<Vec<u8>> {
        let mut key = self.reverse_prefix_for_value(value_bytes)?;
        key.extend_from_slice(&segment_id.to_be_bytes());
        Ok(key)
    }

    pub(super) fn range_key(
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

    pub(super) fn range_bounds(&self, lower: &[u8], upper: &[u8]) -> Result<Range<Vec<u8>>> {
        if lower >= upper {
            return Err(anyhow!("invalid Arrow-index range bounds"));
        }
        let mut start = self.range_prefix.clone();
        start.extend_from_slice(lower);
        let mut end = self.range_prefix.clone();
        end.extend_from_slice(upper);
        Ok(start..end)
    }

    pub(super) fn range_posting_prefix(
        &self,
        range_key_bytes: &[u8],
        key_bytes: &[u8],
    ) -> Result<Vec<u8>> {
        let mut key = self.range_prefix.clone();
        key.extend_from_slice(range_key_bytes);
        key.extend_from_slice(&encode_len(key_bytes.len())?);
        key.extend_from_slice(key_bytes);
        Ok(key)
    }

    pub(super) fn decode_index_key(&self, key: &[u8]) -> Result<(Vec<u8>, u64)> {
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

        let segment_id = read_u64(key, &mut cursor, "Arrow-index key segment id")?;
        if cursor != key.len() {
            return Err(anyhow!("Arrow-index key has trailing bytes"));
        }

        Ok((key_bytes, segment_id))
    }

    pub(super) fn decode_reverse_key(&self, key: &[u8]) -> Result<(Vec<u8>, u64)> {
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

        let segment_id = read_u64(key, &mut cursor, "Arrow-index reverse key segment id")?;
        if cursor != key.len() {
            return Err(anyhow!("Arrow-index reverse key has trailing bytes"));
        }

        Ok((value_bytes, segment_id))
    }

    pub(super) fn decode_range_key<T>(&self, key: &[u8]) -> Result<(Vec<u8>, u64)>
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

        let segment_id = read_u64(key, &mut cursor, "Arrow-index range key segment id")?;
        if cursor != key.len() {
            return Err(anyhow!("Arrow-index range key has trailing bytes"));
        }

        Ok((key_bytes, segment_id))
    }

    pub(super) fn decode_range_components<T>(&self, key: &[u8]) -> Result<(Vec<u8>, Vec<u8>, u64)>
    where
        T: RangeKey,
    {
        if !key.starts_with(&self.range_prefix) {
            return Err(anyhow!("Arrow-index range key missing prefix"));
        }
        let mut cursor = self.range_prefix.len();
        let range_len =
            T::encoded_len(&key[cursor..]).context("decode Arrow-index range key length")?;
        let range_end = cursor
            .checked_add(range_len)
            .ok_or_else(|| anyhow!("Arrow-index range key length overflow"))?;
        let range_key_bytes = key
            .get(cursor..range_end)
            .ok_or_else(|| anyhow!("Arrow-index range key truncated"))?
            .to_vec();
        cursor = range_end;

        let key_len = read_len(key, &mut cursor)?;
        let key_end = cursor
            .checked_add(key_len)
            .ok_or_else(|| anyhow!("Arrow-index range payload length overflow"))?;
        let key_bytes = key
            .get(cursor..key_end)
            .ok_or_else(|| anyhow!("Arrow-index range key payload truncated"))?
            .to_vec();
        cursor = key_end;

        let segment_id = read_u64(key, &mut cursor, "Arrow-index range key segment id")?;
        if cursor != key.len() {
            return Err(anyhow!("Arrow-index range key has trailing bytes"));
        }

        Ok((range_key_bytes, key_bytes, segment_id))
    }

    pub(super) fn lookup_cache_for_key(&self, key_bytes: &[u8]) -> Result<Option<ValueWeightMap>> {
        let shard = shard_for_bytes(key_bytes, self.lookup_cache_shards.len());
        let guard = self.lookup_cache_shards[shard]
            .lock()
            .map_err(|_| anyhow!("Arrow-index lookup cache shard poisoned"))?;
        Ok(guard.get(key_bytes).cloned())
    }

    pub(super) fn store_lookup_cache_for_key(
        &self,
        key_bytes: &[u8],
        state: &ValueWeightMap,
    ) -> Result<()> {
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

    pub(super) fn apply_lookup_cache_updates(
        &self,
        updates: &FastMap<Vec<u8>, ValueWeightMap>,
    ) -> Result<()> {
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

    pub(super) async fn load_persisted_value_weights_for_key(
        &self,
        key_bytes: &[u8],
    ) -> Result<ValueWeightMap> {
        self.load_persisted_value_weights_for_key_with_metrics(key_bytes)
            .await
            .map(|(aggregate, _)| aggregate)
    }

    pub(super) async fn load_persisted_value_weights_for_key_with_metrics(
        &self,
        key_bytes: &[u8],
    ) -> Result<(ValueWeightMap, LookupMetrics)> {
        let (refs, metrics) = self.segment_refs_for_key_with_metrics(key_bytes).await?;
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

        Ok((aggregate, metrics))
    }

    pub(super) async fn load_persisted_keys_for_value(
        &self,
        value_bytes: &[u8],
    ) -> Result<FastMap<Vec<u8>, i64>> {
        let refs = self.segment_refs_for_value(value_bytes).await?;
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

        Ok(aggregate)
    }

    pub(super) async fn segment_for_id(&self, segment_id: u64) -> Result<Arc<CachedSegment>> {
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
            let value_column_index = if batch.num_columns() == 1 { 0 } else { 1 };
            let value_col = batch
                .column(value_column_index)
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

    pub(super) fn cached_segment_for_id(
        &self,
        segment_id: u64,
    ) -> Result<Option<Arc<CachedSegment>>> {
        let shard = self.segment_cache_shard(segment_id);
        let guard = self.segment_cache_shards[shard]
            .lock()
            .map_err(|_| anyhow!("Arrow-index segment cache shard poisoned"))?;
        Ok(guard.get(&segment_id).cloned())
    }

    pub(super) fn insert_segment_cache(
        &self,
        segment_id: u64,
        segment: Arc<CachedSegment>,
    ) -> Result<()> {
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

    pub(super) fn segment_cache_shard(&self, segment_id: u64) -> usize {
        (segment_id as usize) % self.segment_cache_shards.len()
    }

    pub(super) fn decode_value_weights(&self, aggregate: ValueWeightMap) -> Result<Vec<(V, i64)>> {
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
