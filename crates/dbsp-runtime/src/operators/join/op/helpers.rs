use super::*;

impl<L, R, O, K> JoinOp<L, R, O, K>
where
    L: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub(super) fn join_entries_with_maps(
        &self,
        left: Option<&FastHashMap<L, i64>>,
        right: Option<&FastHashMap<R, i64>>,
        acc: &mut FastHashMap<O, i64>,
    ) -> JoinMapMetrics {
        let mut metrics = JoinMapMetrics::default();
        let (Some(left), Some(right)) = (left, right) else {
            return metrics;
        };
        for (lk, lw) in left {
            if *lw == 0 {
                continue;
            }
            metrics.left_rows_examined = metrics.left_rows_examined.saturating_add(1);
            for (rk, rw) in right {
                if *rw == 0 {
                    continue;
                }
                metrics.right_rows_examined = metrics.right_rows_examined.saturating_add(1);
                metrics.candidate_pairs_examined =
                    metrics.candidate_pairs_examined.saturating_add(1);
                if (self.predicate)(lk, rk) {
                    let out = (self.projector)(lk, rk);
                    *acc.entry(out).or_insert(0) += lw * rw;
                    metrics.output_rows = metrics.output_rows.saturating_add(1);
                }
            }
        }
        metrics
    }

    pub(super) fn stage_keyed_deltas<T>(
        &self,
        deltas: &[(T, i64)],
        extractor: &BatchJoinKeyExtractor<T, K>,
    ) -> KeyedRowDeltas<K, T>
    where
        T: Clone + Eq + Hash,
    {
        let mut keyed: KeyedRowDeltas<K, T> = FastHashMap::new();
        for (key, row, weight) in extractor(deltas) {
            if weight == 0 {
                continue;
            }
            let rows = keyed.entry(key).or_default();
            match rows.entry(row) {
                Entry::Occupied(mut entry) => {
                    let next = entry.get().saturating_add(weight);
                    if next == 0 {
                        entry.remove();
                    } else {
                        *entry.get_mut() = next;
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(weight);
                }
            }
        }
        keyed.retain(|_, rows| !rows.is_empty());
        keyed
    }

    pub(super) fn flatten_keyed_updates<T>(keyed: &KeyedRowDeltas<K, T>) -> Vec<(K, T, i64)>
    where
        T: Clone,
    {
        let estimated = keyed.values().map(|rows| rows.len()).sum();
        let mut updates = Vec::with_capacity(estimated);
        for (key, rows) in keyed {
            for (row, weight) in rows {
                if *weight == 0 {
                    continue;
                }
                updates.push((key.clone(), row.clone(), *weight));
            }
        }
        updates
    }

    pub(super) async fn seed_memory_index_for_keys<T>(
        index_store: &IndexedBatchZSet<K, T>,
        memory_index: &mut FastHashMap<K, FastHashMap<T, i64>>,
        keys: impl Iterator<Item = &K>,
    ) -> Result<metrics::LogicalWorkSnapshot>
    where
        T: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    {
        let mut work = metrics::LogicalWorkSnapshot::default();
        let mut missing_keys = Vec::new();
        for key in keys {
            work.state_lookup_keys = work.state_lookup_keys.saturating_add(1);
            if !memory_index.contains_key(key) {
                missing_keys.push(key.clone());
            } else {
                work.cache_hits = work.cache_hits.saturating_add(1);
            }
        }

        for key in missing_keys {
            let (values, lookup_metrics) = index_store
                .values_for_key_with_metrics(&key)
                .await
                .context("load join index entries into memory cache")?;
            work.state_lookup_rows = work.state_lookup_rows.saturating_add(values.len() as u64);
            work.add_lookup_metrics(lookup_metrics);
            let mut rows: FastHashMap<T, i64> = FastHashMap::new();
            for (row, weight) in values {
                if weight == 0 {
                    continue;
                }
                match rows.entry(row) {
                    Entry::Occupied(mut entry) => {
                        let next = entry.get().saturating_add(weight);
                        if next == 0 {
                            entry.remove();
                        } else {
                            *entry.get_mut() = next;
                        }
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(weight);
                    }
                }
            }
            memory_index.insert(key, rows);
        }

        Ok(work)
    }

    pub(super) async fn seed_closed_memory_index_for_keys(
        index_store: &IndexedBatchZSet<K, ()>,
        memory_index: &mut FastHashMap<K, i64>,
        keys: impl Iterator<Item = &K>,
    ) -> Result<metrics::LogicalWorkSnapshot> {
        let mut work = metrics::LogicalWorkSnapshot::default();
        let mut missing_keys = Vec::new();
        for key in keys {
            work.state_lookup_keys = work.state_lookup_keys.saturating_add(1);
            if !memory_index.contains_key(key) {
                missing_keys.push(key.clone());
            } else {
                work.cache_hits = work.cache_hits.saturating_add(1);
            }
        }

        for key in missing_keys {
            let (values, lookup_metrics) = index_store
                .values_for_key_with_metrics(&key)
                .await
                .context("load closed join key entries into memory cache")?;
            work.state_lookup_rows = work.state_lookup_rows.saturating_add(values.len() as u64);
            work.add_lookup_metrics(lookup_metrics);
            let weight = values
                .into_iter()
                .filter_map(|(_, weight)| (weight != 0).then_some(weight))
                .sum::<i64>();
            if weight > 0 {
                memory_index.insert(key, weight);
            } else {
                memory_index.insert(key, 0);
            }
        }

        Ok(work)
    }

    pub(super) async fn apply_deltas_to_versioned<T>(
        versioned: &mut VersionedZSet<T>,
        deltas: &FastHashMap<T, i64>,
        base: Option<u64>,
        state_label: &'static str,
    ) -> Result<ZSetHandle>
    where
        T: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    {
        let mut keyed_deltas: Vec<(&T, i64)> = Vec::new();
        for (key, delta) in deltas {
            if *delta == 0 {
                continue;
            }
            keyed_deltas.push((key, *delta));
        }
        if keyed_deltas.is_empty() {
            if base.is_some()
                && let Some(handle) = versioned.current_handle()
            {
                return Ok(handle);
            }
            return Ok(versioned.handle_for_version(0));
        }

        if versioned.uses_replayable_persistence() {
            anyhow::ensure!(
                base.is_none(),
                "replayable versioned ZSet does not support persisted base chaining"
            );
            let batch = Arc::new(
                keyed_deltas
                    .iter()
                    .map(|(key, delta)| ((*key).clone(), *delta))
                    .collect(),
            );
            return Ok(versioned.publish_replayable_batch(batch));
        }

        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let dict = versioned.dictionary();
        let ids = dict
            .intern_many_values_unique(keyed_deltas.iter().map(|(key, _)| *key))
            .await
            .context("batch intern keys while staging join delta")?;
        for ((_, delta), id) in keyed_deltas.iter().zip(ids.into_iter()) {
            buckets
                .entry(bucket_for(id))
                .or_default()
                .push((id, *delta));
        }

        let mut segments = Vec::new();
        for (bucket, mut bucket_deltas) in buckets {
            bucket_deltas.retain(|(_, delta)| *delta != 0);
            if bucket_deltas.is_empty() {
                continue;
            }
            bucket_deltas.sort_by_key(|(id, _)| *id);
            segments.push(SegmentRecord {
                id: 0,
                bucket,
                deltas: bucket_deltas,
            });
        }

        let persist_start = std::time::Instant::now();
        let mut batch = WriteBatch::new();
        let plan = versioned
            .enqueue_version_with_base(segments, base, 0, &mut batch)
            .await
            .context("schedule join version update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write join version update")?;

        versioned.apply_version_plan(&plan);
        metrics::observe_operator_persistence_latency_ms(
            "join",
            state_label,
            persist_start.elapsed().as_millis() as u64,
        );
        Ok(versioned.handle_for_version(plan.version))
    }

    pub(super) fn apply_keyed_updates_to_memory_index<T>(
        index: &mut FastHashMap<K, FastHashMap<T, i64>>,
        keyed: &KeyedRowDeltas<K, T>,
    ) where
        T: Clone + Eq + Hash,
    {
        for (key, entries) in keyed {
            let should_remove_key = {
                let rows = index.entry(key.clone()).or_default();
                for (row, weight) in entries {
                    if *weight == 0 {
                        continue;
                    }
                    let next = rows.get(row).copied().unwrap_or(0).saturating_add(*weight);
                    if next == 0 {
                        rows.remove(row);
                    } else {
                        rows.insert(row.clone(), next);
                    }
                }
                rows.is_empty()
            };
            if should_remove_key {
                index.remove(key);
            }
        }
    }

    pub(super) fn apply_closed_key_updates_to_memory_index(
        index: &mut FastHashMap<K, i64>,
        updates: &FastHashMap<K, i64>,
    ) {
        for (key, weight) in updates {
            if *weight == 0 {
                continue;
            }
            let next = index.get(key).copied().unwrap_or(0).saturating_add(*weight);
            index.insert(key.clone(), next.max(0));
        }
    }

    pub(super) fn coalesce_closed_key_updates(
        updates: Option<&Arc<Vec<(K, i64)>>>,
    ) -> FastHashMap<K, i64> {
        let mut coalesced = FastHashMap::new();
        let Some(updates) = updates else {
            return coalesced;
        };
        for (key, weight) in updates.iter() {
            if *weight == 0 {
                continue;
            }
            let next = coalesced
                .get(key)
                .copied()
                .unwrap_or(0_i64)
                .saturating_add(*weight);
            if next == 0 {
                coalesced.remove(key);
            } else {
                coalesced.insert(key.clone(), next);
            }
        }
        coalesced
    }

    pub(super) fn add_keyed_delta<T>(keyed: &mut KeyedRowDeltas<K, T>, key: K, row: T, weight: i64)
    where
        T: Clone + Eq + Hash,
    {
        if weight == 0 {
            return;
        }
        let should_remove_key = {
            let rows = keyed.entry(key.clone()).or_default();
            let next = rows.get(&row).copied().unwrap_or(0).saturating_add(weight);
            if next == 0 {
                rows.remove(&row);
            } else {
                rows.insert(row, next);
            }
            rows.is_empty()
        };
        if should_remove_key {
            keyed.remove(&key);
        }
    }

    pub(super) fn left_matches_any_right(
        &self,
        left: &L,
        right_state: Option<&FastHashMap<R, i64>>,
        right_delta: Option<&FastHashMap<R, i64>>,
    ) -> bool {
        right_state
            .into_iter()
            .flat_map(|rows| rows.iter())
            .chain(right_delta.into_iter().flat_map(|rows| rows.iter()))
            .any(|(right, weight)| *weight > 0 && (self.predicate)(left, right))
    }

    pub(super) fn right_matches_any_left(
        &self,
        right: &R,
        left_state: Option<&FastHashMap<L, i64>>,
        left_delta: Option<&FastHashMap<L, i64>>,
    ) -> bool {
        left_state
            .into_iter()
            .flat_map(|rows| rows.iter())
            .chain(left_delta.into_iter().flat_map(|rows| rows.iter()))
            .any(|(left, weight)| *weight > 0 && (self.predicate)(left, right))
    }

    pub(super) fn retained_left_updates(
        &self,
        left_keyed: &KeyedRowDeltas<K, L>,
        right_keyed: &KeyedRowDeltas<K, R>,
        right_closed_key_updates: &FastHashMap<K, i64>,
    ) -> KeyedRowDeltas<K, L> {
        let mut retained = KeyedRowDeltas::default();
        for (key, rows) in left_keyed {
            for (left, weight) in rows {
                let closed = *weight > 0
                    && self.left_retention == JoinInputRetention::DropMatchedAppendOnly
                    && self
                        .right_closed_memory_index
                        .get(key)
                        .copied()
                        .unwrap_or(0)
                        + right_closed_key_updates.get(key).copied().unwrap_or(0)
                        > 0;
                let matched = *weight > 0
                    && self.left_retention == JoinInputRetention::DropMatchedAppendOnly
                    && self.left_matches_any_right(
                        left,
                        self.right_memory_index.get(key),
                        right_keyed.get(key),
                    );
                if !matched && !closed {
                    Self::add_keyed_delta(&mut retained, key.clone(), left.clone(), *weight);
                }
            }
        }

        if self.left_retention == JoinInputRetention::DropMatchedAppendOnly {
            for (key, close_weight) in right_closed_key_updates {
                if *close_weight <= 0 {
                    continue;
                }
                let Some(left_rows) = self.left_memory_index.get(key) else {
                    continue;
                };
                for (left, left_weight) in left_rows {
                    if *left_weight <= 0 {
                        continue;
                    }
                    Self::add_keyed_delta(&mut retained, key.clone(), left.clone(), -*left_weight);
                }
            }
            for (key, right_rows) in right_keyed {
                let Some(left_rows) = self.left_memory_index.get(key) else {
                    continue;
                };
                for (left, left_weight) in left_rows {
                    if *left_weight <= 0 {
                        continue;
                    }
                    let matched = right_rows.iter().any(|(right, right_weight)| {
                        *right_weight > 0 && (self.predicate)(left, right)
                    });
                    if matched {
                        Self::add_keyed_delta(
                            &mut retained,
                            key.clone(),
                            left.clone(),
                            -*left_weight,
                        );
                    }
                }
            }
        }

        retained
    }

    pub(super) fn retained_right_updates(
        &self,
        left_keyed: &KeyedRowDeltas<K, L>,
        right_keyed: &KeyedRowDeltas<K, R>,
        left_closed_key_updates: &FastHashMap<K, i64>,
    ) -> KeyedRowDeltas<K, R> {
        let mut retained = KeyedRowDeltas::default();
        for (key, rows) in right_keyed {
            for (right, weight) in rows {
                let closed = *weight > 0
                    && self.right_retention == JoinInputRetention::DropMatchedAppendOnly
                    && self.left_closed_memory_index.get(key).copied().unwrap_or(0)
                        + left_closed_key_updates.get(key).copied().unwrap_or(0)
                        > 0;
                let matched = *weight > 0
                    && self.right_retention == JoinInputRetention::DropMatchedAppendOnly
                    && self.right_matches_any_left(
                        right,
                        self.left_memory_index.get(key),
                        left_keyed.get(key),
                    );
                if !matched && !closed {
                    Self::add_keyed_delta(&mut retained, key.clone(), right.clone(), *weight);
                }
            }
        }

        if self.right_retention == JoinInputRetention::DropMatchedAppendOnly {
            for (key, close_weight) in left_closed_key_updates {
                if *close_weight <= 0 {
                    continue;
                }
                let Some(right_rows) = self.right_memory_index.get(key) else {
                    continue;
                };
                for (right, right_weight) in right_rows {
                    if *right_weight <= 0 {
                        continue;
                    }
                    Self::add_keyed_delta(
                        &mut retained,
                        key.clone(),
                        right.clone(),
                        -*right_weight,
                    );
                }
            }
            for (key, left_rows) in left_keyed {
                let Some(right_rows) = self.right_memory_index.get(key) else {
                    continue;
                };
                for (right, right_weight) in right_rows {
                    if *right_weight <= 0 {
                        continue;
                    }
                    let matched = left_rows.iter().any(|(left, left_weight)| {
                        *left_weight > 0 && (self.predicate)(left, right)
                    });
                    if matched {
                        Self::add_keyed_delta(
                            &mut retained,
                            key.clone(),
                            right.clone(),
                            -*right_weight,
                        );
                    }
                }
            }
        }

        retained
    }
}
