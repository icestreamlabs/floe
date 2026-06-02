use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;

use crate::collections::IndexedBatchZSet;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::metrics;
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::{delta_zset_handle_batch, publish_transient_zset_batch};

type BatchRowEvaluator<V, K, D> =
    Arc<dyn Fn(&[(V, i64)]) -> Vec<(CountAggregateRow<K, D>, i64)> + Send + Sync>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CountAggregateSlotKind {
    Linear,
    Distinct,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CountAggregateSlotUpdate<D> {
    Linear(i64),
    Distinct(Option<D>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CountAggregateRow<K, D> {
    pub key: K,
    pub slots: Vec<CountAggregateSlotUpdate<D>>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DistinctGroupKey<K> {
    pub(crate) group_key: K,
    pub(crate) slot: u32,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub struct GroupedCountState {
    pub(crate) total_rows: i64,
    pub(crate) counts: Vec<i64>,
}

impl GroupedCountState {
    pub(crate) fn zero(arity: usize) -> Self {
        Self {
            total_rows: 0,
            counts: vec![0; arity],
        }
    }

    pub(crate) fn apply_delta(&self, delta: &GroupedCountState) -> Self {
        let mut next = self.clone();
        next.total_rows += delta.total_rows;
        for (dst, src) in next.counts.iter_mut().zip(delta.counts.iter()) {
            *dst += *src;
        }
        next
    }

    pub(crate) fn is_present(&self) -> bool {
        self.total_rows != 0
    }
}

pub struct CountAggregateOp<K, V, D>
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
    D: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    D::Archived: RkyvDeserialize<D, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub(crate) state: RelationState<(K, GroupedCountState)>,
    pub(crate) table: Arc<dyn KeyValueTable>,
    pub(crate) row_evaluator: BatchRowEvaluator<V, K, D>,
    output: VersionedZSet<(K, Vec<i64>)>,
    dict_cache: HashMap<String, Arc<Dictionary<V>>>,
    state_cache: Option<HashMap<K, GroupedCountState>>,
    slot_kinds: Vec<CountAggregateSlotKind>,
    distinct_index: Option<IndexedBatchZSet<DistinctGroupKey<K>, D>>,
    append_only_input: bool,
    logical_work: metrics::LogicalWorkCollector,
}

impl<K, V, D> CountAggregateOp<K, V, D>
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
    D: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    D::Archived: RkyvDeserialize<D, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub(crate) fn new_batch(
        state: RelationState<(K, GroupedCountState)>,
        table: Arc<dyn KeyValueTable>,
        row_evaluator: BatchRowEvaluator<V, K, D>,
        output: VersionedZSet<(K, Vec<i64>)>,
        slot_kinds: Vec<CountAggregateSlotKind>,
        distinct_index: Option<IndexedBatchZSet<DistinctGroupKey<K>, D>>,
    ) -> Self {
        Self {
            state,
            table,
            row_evaluator,
            output,
            dict_cache: HashMap::new(),
            state_cache: None,
            slot_kinds,
            distinct_index,
            append_only_input: false,
            logical_work: metrics::LogicalWorkCollector::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn last_logical_work(&self) -> metrics::LogicalWorkSnapshot {
        self.logical_work.last_tick()
    }

    pub fn enable_live_output_replayable(&mut self) {
        self.output.enable_replayable_persistence();
    }

    pub fn enable_append_only_input(&mut self) {
        self.append_only_input = true;
    }

    async fn ensure_state_cache(&mut self) -> Result<usize> {
        if self.state_cache.is_some() {
            return Ok(0);
        }

        let materialized = self
            .state
            .integrated
            .materialize()
            .await
            .context("materialize grouped-count integrated state")?;
        let mut cache = HashMap::new();
        let rebuild_rows = materialized.len();
        for ((key, aggregate), weight) in materialized {
            if weight != 0 {
                cache.insert(key, aggregate);
            }
        }
        self.state_cache = Some(cache);
        Ok(rebuild_rows)
    }

    fn coalesce_deltas(&self, deltas: Vec<(V, i64)>) -> HashMap<V, i64> {
        let mut merged = HashMap::new();
        for (row, weight) in deltas {
            let entry = merged.entry(row.clone()).or_insert(0);
            *entry += weight;
            if *entry == 0 {
                merged.remove(&row);
            }
        }
        merged
    }

    async fn apply_deltas_to_versioned<T>(
        versioned: &mut VersionedZSet<T>,
        deltas: &HashMap<T, i64>,
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
            if *delta != 0 {
                keyed_deltas.push((key, *delta));
            }
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
            .context("batch intern keys while staging grouped-count delta")?;
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
            .context("schedule grouped-count version update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write grouped-count version update")?;

        versioned.apply_version_plan(&plan);
        metrics::observe_operator_persistence_latency_ms(
            "count_aggregate",
            state_label,
            persist_start.elapsed().as_millis() as u64,
        );
        Ok(versioned.handle_for_version(plan.version))
    }

    pub async fn apply_delta_values(
        &mut self,
        delta_values: &[(V, i64)],
    ) -> Result<HashMap<(K, Vec<i64>), i64>> {
        self.apply_delta_values_with_work(delta_values, None).await
    }

    async fn apply_delta_values_with_work(
        &mut self,
        delta_values: &[(V, i64)],
        mut logical_work: Option<&mut metrics::LogicalWorkSnapshot>,
    ) -> Result<HashMap<(K, Vec<i64>), i64>> {
        if delta_values.is_empty() {
            return Ok(HashMap::new());
        }
        if self.append_only_input && delta_values.iter().any(|(_, weight)| *weight < 0) {
            anyhow::bail!("append-only count aggregate received negative input weight");
        }

        let row_update_input;
        let row_update_values = if self.append_only_input {
            delta_values
        } else {
            row_update_input = self
                .coalesce_deltas(delta_values.to_vec())
                .into_iter()
                .filter(|(_, weight)| *weight != 0)
                .collect::<Vec<_>>();
            if row_update_input.is_empty() {
                return Ok(HashMap::new());
            }
            row_update_input.as_slice()
        };

        let arity = self.slot_kinds.len();
        let mut grouped_deltas: HashMap<K, GroupedCountState> = HashMap::new();
        let mut distinct_deltas: HashMap<(DistinctGroupKey<K>, D), i64> = HashMap::new();
        let row_updates = (self.row_evaluator)(row_update_values);
        for (row_update, weight) in row_updates {
            if weight == 0 {
                continue;
            }
            if row_update.slots.len() != arity {
                tracing::warn!(
                    expected = arity,
                    actual = row_update.slots.len(),
                    "count aggregate row evaluator returned unexpected slot vector width"
                );
                continue;
            }

            let entry = grouped_deltas
                .entry(row_update.key.clone())
                .or_insert_with(|| GroupedCountState::zero(arity));
            entry.total_rows += weight;
            for (slot_idx, slot) in row_update.slots.into_iter().enumerate() {
                match (&self.slot_kinds[slot_idx], slot) {
                    (CountAggregateSlotKind::Linear, CountAggregateSlotUpdate::Linear(value)) => {
                        entry.counts[slot_idx] += value * weight;
                    }
                    (
                        CountAggregateSlotKind::Distinct,
                        CountAggregateSlotUpdate::Distinct(Some(distinct_value)),
                    ) => {
                        let distinct_key = DistinctGroupKey {
                            group_key: row_update.key.clone(),
                            slot: slot_idx as u32,
                        };
                        let delta_entry = distinct_deltas
                            .entry((distinct_key, distinct_value))
                            .or_insert(0);
                        *delta_entry += weight;
                    }
                    (
                        CountAggregateSlotKind::Distinct,
                        CountAggregateSlotUpdate::Distinct(None),
                    ) => {}
                    (expected_kind, actual) => {
                        tracing::warn!(
                            ?expected_kind,
                            slot_idx,
                            actual_kind = match actual {
                                CountAggregateSlotUpdate::Linear(_) => "linear",
                                CountAggregateSlotUpdate::Distinct(_) => "distinct",
                            },
                            "count aggregate row evaluator returned mismatched slot kind"
                        );
                    }
                }
            }
        }

        if grouped_deltas.is_empty() && distinct_deltas.is_empty() {
            return Ok(HashMap::new());
        }

        if let Some(work) = logical_work.as_deref_mut() {
            let mut affected_groups = grouped_deltas.keys().cloned().collect::<HashSet<_>>();
            for (distinct_key, _) in distinct_deltas.keys() {
                affected_groups.insert(distinct_key.group_key.clone());
            }
            work.changed_groups = affected_groups.len() as u64;
            work.distinct_aux_rows_examined = distinct_deltas.len() as u64;
        }

        if !distinct_deltas.is_empty() {
            let distinct_index = self
                .distinct_index
                .as_ref()
                .context("count aggregate distinct index missing")?;
            let mut distinct_updates = Vec::with_capacity(distinct_deltas.len());
            for ((distinct_key, distinct_value), delta) in distinct_deltas {
                if delta == 0 {
                    continue;
                }
                let old_weight = distinct_index
                    .value_weight_for_key_value(&distinct_key, &distinct_value)
                    .await
                    .context("load count aggregate distinct multiplicity")?;
                if let Some(work) = logical_work.as_deref_mut() {
                    work.state_lookup_keys = work.state_lookup_keys.saturating_add(1);
                    work.state_lookup_rows = work
                        .state_lookup_rows
                        .saturating_add((old_weight != 0) as u64);
                }
                let index_delta = if self.append_only_input {
                    if old_weight > 0 { 0 } else { 1 }
                } else {
                    delta
                };
                let new_weight = old_weight + index_delta;
                let entry = grouped_deltas
                    .entry(distinct_key.group_key.clone())
                    .or_insert_with(|| GroupedCountState::zero(arity));
                if old_weight > 0 && new_weight <= 0 {
                    entry.counts[distinct_key.slot as usize] -= 1;
                } else if old_weight <= 0 && new_weight > 0 {
                    entry.counts[distinct_key.slot as usize] += 1;
                }
                if index_delta != 0 {
                    distinct_updates.push((distinct_key, distinct_value, index_delta));
                }
            }

            if !distinct_updates.is_empty() {
                if let Some(work) = logical_work.as_deref_mut() {
                    work.record_persisted_rows(distinct_updates.len());
                }
                distinct_index
                    .apply_deltas(distinct_updates)
                    .await
                    .context("update count aggregate distinct index")?;
            }
        }

        if grouped_deltas.is_empty() {
            return Ok(HashMap::new());
        }

        let cache_rebuild_rows = self
            .ensure_state_cache()
            .await
            .context("load grouped-count cache")?;
        if cache_rebuild_rows != 0
            && let Some(work) = logical_work.as_deref_mut()
        {
            work.cache_rebuild_rows = cache_rebuild_rows as u64;
            work.state_full_scan_count = 1;
            work.state_scan_rows = work
                .state_scan_rows
                .saturating_add(cache_rebuild_rows as u64);
        }

        let mut state_deltas: HashMap<(K, GroupedCountState), i64> = HashMap::new();
        let mut output_deltas: HashMap<(K, Vec<i64>), i64> = HashMap::new();
        let mut cache_updates = Vec::new();
        {
            let state_cache = self
                .state_cache
                .as_ref()
                .context("grouped-count cache missing")?;

            for (key, delta_state) in grouped_deltas {
                let old_state = state_cache.get(&key).cloned();
                if let Some(work) = logical_work.as_deref_mut() {
                    work.state_lookup_keys = work.state_lookup_keys.saturating_add(1);
                    work.state_lookup_rows = work
                        .state_lookup_rows
                        .saturating_add(old_state.is_some() as u64);
                    work.group_state_rows_examined =
                        work.group_state_rows_examined.saturating_add(1);
                }
                let new_state = match old_state.as_ref() {
                    Some(old) => {
                        let next = old.apply_delta(&delta_state);
                        if next.is_present() { Some(next) } else { None }
                    }
                    None => {
                        if delta_state.is_present() {
                            Some(delta_state)
                        } else {
                            None
                        }
                    }
                };

                if old_state == new_state {
                    continue;
                }

                match (&old_state, &new_state) {
                    (Some(old), Some(new)) => {
                        state_deltas.insert((key.clone(), old.clone()), -1);
                        state_deltas.insert((key.clone(), new.clone()), 1);
                    }
                    (Some(old), None) => {
                        state_deltas.insert((key.clone(), old.clone()), -1);
                    }
                    (None, Some(new)) => {
                        state_deltas.insert((key.clone(), new.clone()), 1);
                    }
                    (None, None) => {}
                }

                let old_output = old_state.as_ref().map(|state| state.counts.clone());
                let new_output = new_state.as_ref().map(|state| state.counts.clone());
                match (old_output, new_output) {
                    (Some(old), Some(new)) if old == new => {}
                    (Some(old), Some(new)) => {
                        output_deltas.insert((key.clone(), old), -1);
                        output_deltas.insert((key.clone(), new), 1);
                    }
                    (Some(old), None) => {
                        output_deltas.insert((key.clone(), old), -1);
                    }
                    (None, Some(new)) => {
                        output_deltas.insert((key.clone(), new), 1);
                    }
                    (None, None) => {}
                }

                cache_updates.push((key, new_state));
            }
        }

        if state_deltas.is_empty() {
            return Ok(HashMap::new());
        }

        let base_version = self.state.base_version_for_update();
        let new_integrated_handle = Self::apply_deltas_to_versioned(
            &mut self.state.integrated,
            &state_deltas,
            base_version,
            "integrated",
        )
        .await
        .context("update grouped-count integrated state")?;
        if let Some(work) = logical_work {
            work.record_persisted_rows(state_deltas.len());
            work.aggregate_state_rows_updated = cache_updates.len() as u64;
        }
        self.state.update_handle(new_integrated_handle);

        if let Some(state_cache) = self.state_cache.as_mut() {
            for (key, value) in cache_updates {
                if let Some(value) = value {
                    state_cache.insert(key, value);
                } else {
                    state_cache.remove(&key);
                }
            }
        }

        Ok(output_deltas)
    }

    pub(crate) async fn evict_keys_where<F>(
        &mut self,
        predicate: F,
    ) -> Result<HashMap<(K, Vec<i64>), i64>>
    where
        F: Fn(&K) -> bool,
    {
        self.ensure_state_cache()
            .await
            .context("load grouped-count cache for eviction")?;

        let keys_to_evict = self
            .state_cache
            .as_ref()
            .context("grouped-count cache missing during eviction")?
            .keys()
            .filter(|key| predicate(key))
            .cloned()
            .collect::<Vec<_>>();
        if keys_to_evict.is_empty() {
            return Ok(HashMap::new());
        }

        if let Some(distinct_index) = self.distinct_index.as_ref() {
            let distinct_slots = self
                .slot_kinds
                .iter()
                .enumerate()
                .filter_map(|(slot_idx, kind)| {
                    matches!(kind, CountAggregateSlotKind::Distinct).then_some(slot_idx as u32)
                })
                .collect::<Vec<_>>();
            let mut distinct_updates = Vec::new();
            for key in &keys_to_evict {
                for slot in &distinct_slots {
                    let distinct_key = DistinctGroupKey {
                        group_key: key.clone(),
                        slot: *slot,
                    };
                    let values = distinct_index
                        .values_for_key(&distinct_key)
                        .await
                        .context("load grouped-count distinct values for eviction")?;
                    for (value, weight) in values {
                        if weight != 0 {
                            distinct_updates.push((distinct_key.clone(), value, -weight));
                        }
                    }
                }
            }

            if !distinct_updates.is_empty() {
                distinct_index
                    .apply_deltas(distinct_updates)
                    .await
                    .context("evict grouped-count distinct index entries")?;
            }
        }

        let mut state_deltas: HashMap<(K, GroupedCountState), i64> = HashMap::new();
        let mut output_deltas: HashMap<(K, Vec<i64>), i64> = HashMap::new();
        {
            let state_cache = self
                .state_cache
                .as_ref()
                .context("grouped-count cache missing during eviction")?;
            for key in &keys_to_evict {
                let Some(old_state) = state_cache.get(key).cloned() else {
                    continue;
                };
                state_deltas.insert((key.clone(), old_state.clone()), -1);
                output_deltas.insert((key.clone(), old_state.counts.clone()), -1);
            }
        }

        if state_deltas.is_empty() {
            return Ok(HashMap::new());
        }

        let base_version = self.state.base_version_for_update();
        let new_integrated_handle = Self::apply_deltas_to_versioned(
            &mut self.state.integrated,
            &state_deltas,
            base_version,
            "integrated",
        )
        .await
        .context("evict grouped-count integrated state")?;
        self.state.update_handle(new_integrated_handle);

        if let Some(state_cache) = self.state_cache.as_mut() {
            for key in keys_to_evict {
                state_cache.remove(&key);
            }
        }

        Ok(output_deltas)
    }

    pub(crate) async fn persist_output_deltas(
        &mut self,
        output_deltas: &HashMap<(K, Vec<i64>), i64>,
    ) -> Result<ZSetHandle> {
        Self::apply_deltas_to_versioned(&mut self.output, output_deltas, None, "output")
            .await
            .context("persist grouped-count output delta")
    }

    pub(crate) fn empty_output_handle(&self) -> ZSetHandle {
        self.output.handle_for_version(0)
    }

    pub(crate) async fn state_entry_count(&mut self) -> Result<usize> {
        self.ensure_state_cache()
            .await
            .context("load grouped-count cache for state size")?;
        Ok(self.state_cache.as_ref().map_or(0, HashMap::len))
    }
}

#[async_trait]
impl<K, V, D> DeltaOperator for CountAggregateOp<K, V, D>
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
    D: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    D::Archived: RkyvDeserialize<D, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let delta_handle = inputs
            .first()
            .cloned()
            .context("count aggregate operator requires one input delta handle")?;

        let delta_values =
            delta_zset_handle_batch::<V>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for count aggregate")?;
        let mut work = metrics::LogicalWorkSnapshot::from_input_delta_rows(delta_values.len());

        let output_deltas = self
            .apply_delta_values_with_work(delta_values.as_ref(), Some(&mut work))
            .await?;
        if output_deltas.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }
        work.record_output_delta_rows(output_deltas.len());

        let delta_handle =
            Self::apply_deltas_to_versioned(&mut self.output, &output_deltas, None, "output")
                .await
                .context("persist grouped-count output delta")?;
        work.record_persisted_rows(output_deltas.len());
        publish_transient_zset_batch(
            &delta_handle,
            Arc::new(output_deltas.into_iter().collect::<Vec<_>>()),
        );
        self.logical_work.finish_tick(work);
        Ok(Some(delta_handle))
    }

    fn logical_work(&self) -> Option<metrics::LogicalWorkSnapshot> {
        Some(self.logical_work.last_tick())
    }
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

#[cfg(test)]
mod tests;
