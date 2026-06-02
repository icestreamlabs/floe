use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;

use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::metrics;
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::{compute_delta, delta_zset_handle_batch, publish_transient_zset_batch};

type BatchKeyPartsFn<K, P, O> =
    Arc<dyn Fn(&[(K, i64)]) -> Vec<(K, i64, Option<P>, Option<O>)> + Send + Sync>;
type PartitionOrderIndex<K, P, O> = BTreeMap<P, BTreeMap<(O, K), i64>>;

/// Top-N operator that applies row-number semantics: it counts multiplicity and
/// supports OFFSET, matching ORDER BY/LIMIT/OFFSET behavior.
pub struct TopNOp<K, P, O>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Ord
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    P: Ord + Clone + Send + Sync + 'static,
    O: Ord + Clone + Send + Sync + 'static,
{
    pub(crate) state: RelationState<K>,
    pub(crate) table: Arc<dyn KeyValueTable>,
    output: VersionedZSet<K>,
    dict_cache: HashMap<String, Arc<Dictionary<K>>>,
    input_cache: Option<HashMap<K, i64>>,
    output_cache: HashMap<K, i64>,
    partition_output_cache: BTreeMap<P, HashMap<K, i64>>,
    // In-memory ordering index for top-N row semantics; rebuilt from storage on restart.
    order_index: Option<PartitionOrderIndex<K, P, O>>,
    row_key_cache: HashMap<K, (Option<P>, Option<O>)>,
    key_parts: BatchKeyPartsFn<K, P, O>,
    limit: usize,
    offset: usize,
    logical_work: metrics::LogicalWorkCollector,
}

impl<K, P, O> TopNOp<K, P, O>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Ord
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    P: Ord + Clone + Send + Sync + 'static,
    O: Ord + Clone + Send + Sync + 'static,
{
    pub fn new_with_batch_key_extractor(
        state: RelationState<K>,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<K>,
        key_parts: BatchKeyPartsFn<K, P, O>,
        limit: usize,
        offset: usize,
    ) -> Self {
        Self {
            state,
            table,
            output,
            dict_cache: HashMap::new(),
            input_cache: None,
            output_cache: HashMap::new(),
            partition_output_cache: BTreeMap::new(),
            order_index: None,
            row_key_cache: HashMap::new(),
            key_parts,
            limit,
            offset,
            logical_work: metrics::LogicalWorkCollector::default(),
        }
    }

    pub fn enable_live_state_replayable(&mut self) {
        self.state.enable_live_replayable();
    }

    pub fn enable_live_output_replayable(&mut self) {
        self.output.enable_replayable_persistence();
    }

    #[cfg(test)]
    pub(crate) fn last_logical_work(&self) -> metrics::LogicalWorkSnapshot {
        self.logical_work.last_tick()
    }

    async fn ensure_input_cache_loaded_from_state(&mut self) -> Result<usize> {
        if self.input_cache.is_some() {
            return Ok(0);
        }

        let mut materialized = self
            .state
            .integrated
            .materialize()
            .await
            .context("materialize topn input state")?;
        materialized.retain(|_, weight| *weight != 0);
        let rebuild_rows = materialized.len();
        self.input_cache = Some(materialized);
        Ok(rebuild_rows)
    }

    fn compute_partition_topn(&self, partition_index: &BTreeMap<(O, K), i64>) -> HashMap<K, i64> {
        if self.limit == 0 {
            return HashMap::new();
        }

        let mut remaining_skip = self.offset;
        let mut remaining_take = self.limit;
        let mut output = HashMap::new();

        for ((_order_key, row), weight) in partition_index.iter() {
            if remaining_take == 0 {
                break;
            }

            let mut remaining_weight = *weight;
            if remaining_skip > 0 {
                let available = usize::try_from(remaining_weight).unwrap_or(usize::MAX);
                let skip = remaining_skip.min(available);
                remaining_skip -= skip;
                remaining_weight -= skip as i64;
            }

            if remaining_weight <= 0 {
                continue;
            }

            let available = usize::try_from(remaining_weight).unwrap_or(usize::MAX);
            let take = remaining_take.min(available);
            if take > 0 {
                output.insert(row.clone(), take as i64);
                remaining_take -= take;
            }
        }

        output
    }

    async fn ensure_order_index_built_from_input_cache(&mut self) -> Result<usize> {
        if self.order_index.is_some() {
            return Ok(0);
        }

        let input_cache = self
            .input_cache
            .as_ref()
            .context("topn input cache missing while building order index")?;
        let rebuild_rows = input_cache.len();
        let entries: Vec<(K, i64)> = input_cache
            .iter()
            .map(|(key, weight)| (key.clone(), *weight))
            .collect();
        let mut index: BTreeMap<P, BTreeMap<(O, K), i64>> = BTreeMap::new();
        for (key, weight) in entries {
            if weight <= 0 {
                continue;
            }
            let (partition_key, order_key) = self.keys_for(&key);
            if let (Some(partition_key), Some(order_key)) = (partition_key, order_key) {
                index
                    .entry(partition_key)
                    .or_default()
                    .insert((order_key, key), weight);
            }
        }
        self.order_index = Some(index);
        Ok(rebuild_rows)
    }

    fn record_cold_cache_rebuild(work: &mut metrics::LogicalWorkSnapshot, rows: usize) {
        if rows == 0 {
            return;
        }

        let rows = rows as u64;
        work.cache_rebuild_rows = work.cache_rebuild_rows.saturating_add(rows);
        work.state_full_scan_count = work.state_full_scan_count.saturating_add(1);
        work.state_scan_rows = work.state_scan_rows.saturating_add(rows);
    }

    fn keys_for(&mut self, key: &K) -> (Option<P>, Option<O>) {
        if let Some(cached) = self.row_key_cache.get(key) {
            return cached.clone();
        }
        let computed = (self.key_parts)(&[(key.clone(), 1)])
            .into_iter()
            .next()
            .map(|(_, _, partition, order)| (partition, order))
            .unwrap_or((None, None));
        self.row_key_cache.insert(key.clone(), computed.clone());
        computed
    }

    fn keys_for_delta_map(
        &mut self,
        rows: &HashMap<K, i64>,
    ) -> Vec<(K, i64, Option<P>, Option<O>)> {
        let mut missing = Vec::new();
        let mut keyed = Vec::with_capacity(rows.len());
        for (key, weight) in rows {
            if let Some((partition, order)) = self.row_key_cache.get(key) {
                keyed.push((key.clone(), *weight, partition.clone(), order.clone()));
            } else {
                missing.push((key.clone(), *weight));
            }
        }
        for (key, weight, partition, order) in (self.key_parts)(&missing) {
            self.row_key_cache
                .insert(key.clone(), (partition.clone(), order.clone()));
            keyed.push((key, weight, partition, order));
        }
        keyed
    }

    async fn apply_deltas_to_versioned(
        versioned: &mut VersionedZSet<K>,
        deltas: &HashMap<K, i64>,
        base: Option<u64>,
        state_label: &'static str,
    ) -> Result<ZSetHandle> {
        let mut keyed_deltas: Vec<(&K, i64)> = Vec::new();
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
        let mut dict_batch = dict.batch();
        for (key, delta) in keyed_deltas {
            let id = dict_batch
                .intern(key)
                .await
                .context("intern key while staging topn delta")?;
            buckets.entry(bucket_for(id)).or_default().push((id, delta));
        }
        drop(dict_batch);

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
            .context("schedule topn version update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write topn version update")?;

        versioned.apply_version_plan(&plan);
        metrics::observe_operator_persistence_latency_ms(
            "topn",
            state_label,
            persist_start.elapsed().as_millis() as u64,
        );
        Ok(versioned.handle_for_version(plan.version))
    }
}

#[async_trait]
impl<K, P, O> DeltaOperator for TopNOp<K, P, O>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Ord
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    P: Ord + Clone + Send + Sync + 'static,
    O: Ord + Clone + Send + Sync + 'static,
{
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let delta_handle = inputs
            .first()
            .cloned()
            .context("topn operator requires one input delta handle")?;

        let delta_values =
            delta_zset_handle_batch::<K>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for topn")?;
        let mut work = metrics::LogicalWorkSnapshot::from_input_delta_rows(delta_values.len());

        if delta_values.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }

        let input_cache_rebuild_rows = self
            .ensure_input_cache_loaded_from_state()
            .await
            .context("load topn input cache")?;
        Self::record_cold_cache_rebuild(&mut work, input_cache_rebuild_rows);

        let mut delta_map = HashMap::new();
        for (key, diff_weight) in delta_values.iter() {
            let entry = delta_map.entry(key.clone()).or_insert(0);
            *entry += *diff_weight;
            if *entry == 0 {
                delta_map.remove(key);
            }
        }

        if delta_map.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }

        let order_index_rebuild_rows = self
            .ensure_order_index_built_from_input_cache()
            .await
            .context("build topn order index")?;
        Self::record_cold_cache_rebuild(&mut work, order_index_rebuild_rows);

        let mut cache_updates = Vec::new();
        for (key, diff_weight, partition_key, order_key) in self.keys_for_delta_map(&delta_map) {
            let existing = self
                .input_cache
                .as_ref()
                .and_then(|cache| cache.get(&key).copied())
                .unwrap_or(0);
            let new_weight = existing + diff_weight;
            let (partition_key, order_key) = if existing > 0 || new_weight > 0 {
                (partition_key, order_key)
            } else {
                (None, None)
            };
            cache_updates.push((key, existing, new_weight, partition_key, order_key));
        }

        let base_version = self.state.base_version_for_update();
        let new_integrated_handle = Self::apply_deltas_to_versioned(
            &mut self.state.integrated,
            &delta_map,
            base_version,
            "integrated_input",
        )
        .await
        .context("update topn input state")?;
        work.record_persisted_rows(delta_map.len());
        self.state.update_handle(new_integrated_handle);

        if let Some(input_cache) = self.input_cache.as_mut() {
            for (key, _old_weight, new_weight, _partition_key, _order_key) in &cache_updates {
                if *new_weight == 0 {
                    input_cache.remove(key);
                } else {
                    input_cache.insert(key.clone(), *new_weight);
                }
            }
        }

        let mut cache_prune = Vec::new();
        let mut affected_partitions = BTreeSet::new();
        if let Some(mut order_index) = self.order_index.take() {
            for (key, old_weight, new_weight, partition_key, order_key) in &cache_updates {
                let old_positive = *old_weight > 0;
                let new_positive = *new_weight > 0;
                if !old_positive && !new_positive {
                    if *new_weight == 0 {
                        cache_prune.push(key.clone());
                    }
                    continue;
                }
                let Some(order_key) = order_key.clone() else {
                    if *new_weight == 0 {
                        cache_prune.push(key.clone());
                    }
                    continue;
                };
                let Some(partition_key) = partition_key.clone() else {
                    if *new_weight == 0 {
                        cache_prune.push(key.clone());
                    }
                    continue;
                };

                affected_partitions.insert(partition_key.clone());
                let index_key = (order_key, key.clone());
                if old_positive && new_positive {
                    order_index
                        .entry(partition_key.clone())
                        .or_default()
                        .insert(index_key, *new_weight);
                } else if old_positive {
                    let mut remove_partition = false;
                    if let Some(partition_index) = order_index.get_mut(&partition_key) {
                        partition_index.remove(&index_key);
                        if partition_index.is_empty() {
                            remove_partition = true;
                        }
                    }
                    if remove_partition {
                        order_index.remove(&partition_key);
                    }
                } else if new_positive {
                    order_index
                        .entry(partition_key.clone())
                        .or_default()
                        .insert(index_key, *new_weight);
                }

                if *new_weight == 0 {
                    cache_prune.push(key.clone());
                }
            }
            self.order_index = Some(order_index);
        }
        work.changed_partitions = affected_partitions.len() as u64;
        for key in cache_prune {
            self.row_key_cache.remove(&key);
        }

        let order_index = self
            .order_index
            .as_ref()
            .context("topn order index missing after update")?;
        let mut output_delta = HashMap::new();
        for partition_key in affected_partitions {
            if let Some(partition_index) = order_index.get(&partition_key) {
                work.partition_rows_examined = work
                    .partition_rows_examined
                    .saturating_add(partition_index.len() as u64);
            }
            let old_partition_output = self
                .partition_output_cache
                .get(&partition_key)
                .cloned()
                .unwrap_or_default();
            let new_partition_output = order_index
                .get(&partition_key)
                .map(|partition_index| self.compute_partition_topn(partition_index))
                .unwrap_or_default();

            for (key, delta) in compute_delta(&old_partition_output, &new_partition_output) {
                if delta == 0 {
                    continue;
                }
                let entry = output_delta.entry(key.clone()).or_insert(0);
                *entry += delta;
                if *entry == 0 {
                    output_delta.remove(&key);
                }
            }

            if new_partition_output.is_empty() {
                self.partition_output_cache.remove(&partition_key);
            } else {
                self.partition_output_cache
                    .insert(partition_key, new_partition_output);
            }
        }
        for (key, delta) in &output_delta {
            let entry = self.output_cache.entry(key.clone()).or_insert(0);
            *entry += *delta;
            if *entry == 0 {
                self.output_cache.remove(key);
            }
        }

        if output_delta.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }
        work.replacement_rows = output_delta.len() as u64;
        work.record_output_delta_rows(output_delta.len());

        let output_handle =
            Self::apply_deltas_to_versioned(&mut self.output, &output_delta, None, "output")
                .await
                .context("persist topn output delta")?;
        work.record_persisted_rows(output_delta.len());
        publish_transient_zset_batch(
            &output_handle,
            Arc::new(output_delta.into_iter().collect::<Vec<_>>()),
        );
        self.logical_work.finish_tick(work);
        Ok(Some(output_handle))
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
