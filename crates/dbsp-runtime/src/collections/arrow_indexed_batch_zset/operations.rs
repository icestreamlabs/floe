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
    pub async fn restore_committed_checkpoint(&self) -> Result<()> {
        if matches!(self.persistence, IndexedStatePersistence::Replayable) {
            return Ok(());
        }
        let Some(handle) = operator_state_registry::restored_operator_state(&self.namespace) else {
            let next_segment_id = self.read_next_segment_id().await?;
            self.record_checkpoint(next_segment_id);
            return Ok(());
        };
        let next_segment_id = handle.version.max(1);
        self.truncate_to_next_segment(next_segment_id).await?;
        self.record_checkpoint(next_segment_id);
        Ok(())
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
        self.apply_deltas_internal_with_range(deltas, true).await
    }

    pub async fn apply_deltas_with_range_only<I>(&self, deltas: I) -> Result<()>
    where
        K: RangeKey,
        I: IntoIterator<Item = (K, V, i64)>,
    {
        self.apply_deltas_with_range_only_stats(deltas)
            .await
            .map(|_| ())
    }

    pub async fn apply_deltas_with_range_only_stats<I>(
        &self,
        deltas: I,
    ) -> Result<ApplyDeltaMetrics>
    where
        K: RangeKey,
        I: IntoIterator<Item = (K, V, i64)>,
    {
        if !self.range_enabled {
            return Err(anyhow!("range index not enabled"));
        }
        self.apply_deltas_internal_with_range(deltas, false).await
    }

    async fn apply_deltas_internal<I>(&self, deltas: I) -> Result<ApplyDeltaMetrics>
    where
        I: IntoIterator<Item = (K, V, i64)>,
    {
        if matches!(self.persistence, IndexedStatePersistence::Replayable) {
            return self.apply_replayable_deltas(deltas);
        }

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

        let touched_key_bytes = touched_updates.keys().cloned().collect::<Vec<_>>();
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
        self.record_checkpoint(segment_id.saturating_add(1));

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
        drop(_segment_guard);
        self.compact_hot_keys(&touched_key_bytes)
            .await
            .context("compact hot Arrow-index keys")?;

        metrics.coalesced_records = metrics.non_zero_input_records;
        metrics.persisted_records = encoded_rows.len();
        Ok(metrics)
    }

    async fn apply_deltas_internal_with_range<I>(
        &self,
        deltas: I,
        write_lookup_index: bool,
    ) -> Result<ApplyDeltaMetrics>
    where
        K: RangeKey,
        I: IntoIterator<Item = (K, V, i64)>,
    {
        if matches!(self.persistence, IndexedStatePersistence::Replayable) {
            return self.apply_replayable_deltas(deltas);
        }

        let mut metrics = ApplyDeltaMetrics::default();
        let mut encoded_rows: Vec<(Vec<u8>, Vec<u8>, i64)> = Vec::new();
        let mut touched_updates: FastMap<Vec<u8>, ValueWeightMap> = FastMap::default();
        let mut key_postings: FastMap<Vec<u8>, Vec<(u32, i64)>> = FastMap::default();
        let mut range_postings: FastMap<RangePostingKey, SegmentPostings> = FastMap::default();
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

            if write_lookup_index {
                key_postings
                    .entry(key_bytes.clone())
                    .or_default()
                    .push((row_index, delta));
            }
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

            if write_lookup_index {
                let key_updates = touched_updates.entry(key_bytes.clone()).or_default();
                *key_updates.entry(value_bytes.clone()).or_insert(0) += delta;
            }

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

        if write_lookup_index {
            for updates in touched_updates.values_mut() {
                updates.retain(|_, weight| *weight != 0);
            }
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
        self.record_checkpoint(segment_id.saturating_add(1));

        self.insert_segment_cache(
            segment_id,
            Arc::new(CachedSegment {
                values: encoded_rows
                    .iter()
                    .map(|(_, value_bytes, _)| value_bytes.clone())
                    .collect(),
            }),
        )?;
        if write_lookup_index {
            self.apply_lookup_cache_updates(&touched_updates)?;
        }

        metrics.coalesced_records = metrics.non_zero_input_records;
        metrics.persisted_records = encoded_rows.len();
        Ok(metrics)
    }

    pub async fn values_for_key(&self, key: &K) -> Result<Vec<(V, i64)>> {
        self.values_for_key_with_metrics(key)
            .await
            .map(|(values, _)| values)
    }

    pub async fn values_for_key_with_metrics(
        &self,
        key: &K,
    ) -> Result<(Vec<(V, i64)>, LookupMetrics)> {
        let mut metrics = LookupMetrics {
            lookup_keys: 1,
            ..LookupMetrics::default()
        };

        if matches!(self.persistence, IndexedStatePersistence::Replayable) {
            let values = self
                .overlay_by_key
                .lock()
                .map_err(|_| anyhow!("Arrow-index overlay-by-key mutex poisoned"))?
                .get(key)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|(_, weight)| *weight != 0)
                .collect::<Vec<_>>();
            metrics.returned_rows = values.len();
            return Ok((values, metrics));
        }

        let key_bytes = encode(key).context("encode Arrow-index lookup key")?;
        if let Some(cached) = self.lookup_cache_for_key(&key_bytes)? {
            metrics.cache_hits = 1;
            let values = self.decode_value_weights(cached)?;
            metrics.returned_rows = values.len();
            return Ok((values, metrics));
        }

        metrics.cache_misses = 1;
        let (aggregate, persisted_metrics) = self
            .load_persisted_value_weights_for_key_with_metrics(&key_bytes)
            .await?;
        metrics.add_assign(persisted_metrics);
        self.store_lookup_cache_for_key(&key_bytes, &aggregate)?;
        let values = self.decode_value_weights(aggregate)?;
        metrics.returned_rows = values.len();
        Ok((values, metrics))
    }

    pub async fn value_weight_for_key_value(&self, key: &K, value: &V) -> Result<i64> {
        if matches!(self.persistence, IndexedStatePersistence::Replayable) {
            return Ok(self
                .overlay_by_key
                .lock()
                .map_err(|_| anyhow!("Arrow-index overlay-by-key mutex poisoned"))?
                .get(key)
                .and_then(|values| values.get(value))
                .copied()
                .unwrap_or(0));
        }

        let key_bytes = encode(key).context("encode Arrow-index lookup key")?;
        let value_bytes = encode(value).context("encode Arrow-index lookup value")?;
        if let Some(cached) = self.lookup_cache_for_key(&key_bytes)? {
            return Ok(cached.get(&value_bytes).copied().unwrap_or(0));
        }

        let aggregate = self
            .load_persisted_value_weights_for_key(&key_bytes)
            .await?;
        let weight = aggregate.get(&value_bytes).copied().unwrap_or(0);
        self.store_lookup_cache_for_key(&key_bytes, &aggregate)?;
        Ok(weight)
    }

    pub fn replayable_snapshot_entries(&self) -> Result<Vec<(K, V, i64)>> {
        if !matches!(self.persistence, IndexedStatePersistence::Replayable) {
            return Err(anyhow!(
                "Arrow-index snapshot entries require replayable persistence"
            ));
        }
        Ok(self
            .overlay_snapshot_by_key()?
            .into_iter()
            .flat_map(|(key, values)| {
                values.into_iter().filter_map(move |(value, weight)| {
                    (weight != 0).then_some((key.clone(), value, weight))
                })
            })
            .collect())
    }

    pub async fn keys_for_value(&self, value: &V) -> Result<Vec<(K, i64)>> {
        if !self.reverse_enabled {
            return Err(anyhow!("reverse index not enabled"));
        }

        if matches!(self.persistence, IndexedStatePersistence::Replayable) {
            return Ok(self
                .overlay_by_value
                .lock()
                .map_err(|_| anyhow!("Arrow-index overlay-by-value mutex poisoned"))?
                .get(value)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|(_, weight)| *weight != 0)
                .collect());
        }

        let value_bytes = encode(value).context("encode Arrow-index reverse lookup value")?;
        let aggregate = self.load_persisted_keys_for_value(&value_bytes).await?;

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

        if matches!(self.persistence, IndexedStatePersistence::Replayable) {
            let overlay_snapshot = self.overlay_snapshot_by_key()?;
            let lower_bytes = lower.encode_range_key();
            let upper_bytes = upper.encode_range_key();
            if lower_bytes >= upper_bytes {
                return Ok(Vec::new());
            }
            let mut output = Vec::new();
            for (key, overlay_values) in overlay_snapshot {
                let range_key = key.encode_range_key();
                if range_key < lower_bytes || range_key >= upper_bytes {
                    continue;
                }
                for (value, weight) in overlay_values {
                    if weight != 0 {
                        output.push((key.clone(), value, weight));
                    }
                }
            }
            return Ok(output);
        }

        let lower_bytes = lower.encode_range_key();
        let upper_bytes = upper.encode_range_key();
        if lower_bytes >= upper_bytes {
            return Ok(Vec::new());
        }
        let mut refs_by_key: SegmentRefsByKey = FastMap::default();
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

    pub async fn first_values_for_key_range(&self, lower: &K, upper: &K) -> Result<Vec<(K, V, i64)>>
    where
        K: RangeKey,
    {
        self.first_values_for_key_range_with_metrics(lower, upper)
            .await
            .map(|(values, _)| values)
    }

    pub async fn first_values_for_key_range_with_metrics(
        &self,
        lower: &K,
        upper: &K,
    ) -> Result<(Vec<(K, V, i64)>, LookupMetrics)>
    where
        K: RangeKey,
    {
        if !self.range_enabled {
            return Err(anyhow!("range index not enabled"));
        }

        let mut metrics = LookupMetrics {
            lookup_keys: 1,
            ..LookupMetrics::default()
        };

        let lower_bytes = lower.encode_range_key();
        let upper_bytes = upper.encode_range_key();
        if lower_bytes >= upper_bytes {
            return Ok((Vec::new(), metrics));
        }

        if matches!(self.persistence, IndexedStatePersistence::Replayable) {
            let overlay_snapshot = self.overlay_snapshot_by_key()?;
            let mut candidates = Vec::new();
            for (key, overlay_values) in overlay_snapshot {
                let range_key = key.encode_range_key();
                if range_key < lower_bytes || range_key >= upper_bytes {
                    continue;
                }
                let key_bytes = encode(&key).context("encode Arrow-index overlay range key")?;
                candidates.push((range_key, key_bytes, key, overlay_values));
            }
            candidates
                .sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

            for (_, _, key, overlay_values) in candidates {
                let output = overlay_values
                    .into_iter()
                    .filter_map(|(value, weight)| {
                        (weight != 0).then_some((key.clone(), value, weight))
                    })
                    .collect::<Vec<_>>();
                if !output.is_empty() {
                    metrics.returned_rows = output.len();
                    return Ok((output, metrics));
                }
            }
            return Ok((Vec::new(), metrics));
        }

        self.ensure_range_layout()
            .await
            .context("validate Arrow-index range layout")?;

        let full_range = self.range_bounds(&lower_bytes, &upper_bytes)?;
        let range_end = full_range.end;
        let mut scan_start = full_range.start;

        while scan_start < range_end {
            let mut first_group_prefix: Option<Vec<u8>> = None;
            let mut first_key_bytes: Option<Vec<u8>> = None;
            let mut should_continue = |entry_key: &[u8], _entry_value: &[u8]| -> Result<bool> {
                let (range_key_bytes, key_bytes, _) = self
                    .decode_range_components::<K>(entry_key)
                    .context("decode Arrow-index range posting key")?;
                let group_prefix = self
                    .range_posting_prefix(&range_key_bytes, &key_bytes)
                    .context("build Arrow-index range posting prefix")?;
                if let Some(first_prefix) = &first_group_prefix {
                    return Ok(&group_prefix == first_prefix);
                }
                first_key_bytes = Some(key_bytes);
                first_group_prefix = Some(group_prefix);
                Ok(true)
            };
            let entries = self
                .table
                .scan_range_bytes_until(
                    scan_start.clone()..range_end.clone(),
                    &ScanOptions::default(),
                    &mut should_continue,
                )
                .await
                .context("scan first Arrow-index range posting group")?;
            let Some(group_prefix) = first_group_prefix else {
                return Ok((Vec::new(), metrics));
            };
            let Some(key_bytes) = first_key_bytes else {
                return Ok((Vec::new(), metrics));
            };

            let mut refs: FastMap<u64, SegmentPostings> = FastMap::default();
            for (entry_key, entry_value) in entries {
                let (range_key_bytes, candidate_key_bytes, segment_id) = self
                    .decode_range_components::<K>(&entry_key)
                    .context("decode Arrow-index range posting key")?;
                let candidate_prefix = self
                    .range_posting_prefix(&range_key_bytes, &candidate_key_bytes)
                    .context("build Arrow-index range posting prefix")?;
                if candidate_prefix != group_prefix {
                    continue;
                }
                let postings = decode_index_postings(&entry_value)?;
                metrics.index_segments_examined = metrics.index_segments_examined.saturating_add(1);
                metrics.index_postings_examined = metrics
                    .index_postings_examined
                    .saturating_add(postings.len());
                refs.entry(segment_id).or_default().extend(postings);
            }

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

            let values = self.decode_value_weights(aggregate)?;
            if !values.is_empty() {
                let key = decode::<K>(&key_bytes)
                    .context("decode Arrow-index key for first range lookup rows")?;
                let output = values
                    .into_iter()
                    .map(|(value, weight)| (key.clone(), value, weight))
                    .collect::<Vec<_>>();
                metrics.returned_rows = output.len();
                return Ok((output, metrics));
            }

            let Some(next_start) = bytes_prefix_successor(&group_prefix) else {
                return Ok((Vec::new(), metrics));
            };
            if next_start <= scan_start {
                return Err(anyhow!("Arrow-index range cursor did not advance"));
            }
            scan_start = next_start;
        }

        Ok((Vec::new(), metrics))
    }

    pub async fn entries(&self) -> Result<Vec<(K, V, i64)>> {
        if matches!(self.persistence, IndexedStatePersistence::Replayable) {
            let overlay_snapshot = self.overlay_snapshot_by_key()?;
            let mut out = Vec::new();
            for (key, overlay_values) in overlay_snapshot {
                for (value, weight) in overlay_values {
                    if weight != 0 {
                        out.push((key.clone(), value, weight));
                    }
                }
            }
            return Ok(out);
        }

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
}
