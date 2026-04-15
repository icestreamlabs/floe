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

type PartitionKeyFn<K, P> = Arc<dyn Fn(&K) -> Option<P> + Send + Sync>;
type OrderKeyFn<K, O> = Arc<dyn Fn(&K) -> Option<O> + Send + Sync>;
type KeyPartsFn<K, P, O> = Arc<dyn Fn(&K) -> (Option<P>, Option<O>) + Send + Sync>;

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
    pub state: RelationState<K>,
    pub table: Arc<dyn KeyValueTable>,
    output: VersionedZSet<K>,
    dict_cache: HashMap<String, Arc<Dictionary<K>>>,
    input_cache: Option<HashMap<K, i64>>,
    output_cache: HashMap<K, i64>,
    partition_output_cache: BTreeMap<P, HashMap<K, i64>>,
    // In-memory ordering index for top-N row semantics; rebuilt from storage on restart.
    order_index: Option<BTreeMap<P, BTreeMap<(O, K), i64>>>,
    row_key_cache: HashMap<K, (Option<P>, Option<O>)>,
    key_parts: KeyPartsFn<K, P, O>,
    limit: usize,
    offset: usize,
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
    pub fn new(
        state: RelationState<K>,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<K>,
        partition_key: PartitionKeyFn<K, P>,
        order_key: OrderKeyFn<K, O>,
        limit: usize,
        offset: usize,
    ) -> Self {
        let key_parts = Arc::new(move |key: &K| (partition_key(key), order_key(key)));
        Self::new_with_key_extractor(state, table, output, key_parts, limit, offset)
    }

    pub fn new_with_key_extractor(
        state: RelationState<K>,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<K>,
        key_parts: KeyPartsFn<K, P, O>,
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
        }
    }

    pub fn enable_live_state_replayable(&mut self) {
        self.state.enable_live_replayable();
    }

    pub fn enable_live_output_replayable(&mut self) {
        self.output.enable_replayable_persistence();
    }

    async fn ensure_input_cache(&mut self) -> Result<()> {
        if self.input_cache.is_some() {
            return Ok(());
        }

        let mut materialized = self
            .state
            .integrated
            .materialize()
            .await
            .context("materialize topn input state")?;
        materialized.retain(|_, weight| *weight != 0);
        self.input_cache = Some(materialized);
        Ok(())
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

    async fn ensure_order_index(&mut self) -> Result<()> {
        if self.order_index.is_some() {
            return Ok(());
        }

        let input_cache = self
            .input_cache
            .as_ref()
            .context("topn input cache missing while building order index")?;
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
        Ok(())
    }

    fn keys_for(&mut self, key: &K) -> (Option<P>, Option<O>) {
        if let Some(cached) = self.row_key_cache.get(key) {
            return cached.clone();
        }
        let computed = (self.key_parts)(key);
        self.row_key_cache.insert(key.clone(), computed.clone());
        computed
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

        if delta_values.is_empty() {
            return Ok(Some(self.output.handle_for_version(0)));
        }

        self.ensure_input_cache()
            .await
            .context("load topn input cache")?;

        let mut delta_map = HashMap::new();
        for (key, diff_weight) in delta_values.iter() {
            let entry = delta_map.entry(key.clone()).or_insert(0);
            *entry += *diff_weight;
            if *entry == 0 {
                delta_map.remove(&key);
            }
        }

        if delta_map.is_empty() {
            return Ok(Some(self.output.handle_for_version(0)));
        }

        self.ensure_order_index()
            .await
            .context("build topn order index")?;

        let mut cache_updates = Vec::new();
        for (key, diff_weight) in &delta_map {
            let existing = self
                .input_cache
                .as_ref()
                .and_then(|cache| cache.get(key).copied())
                .unwrap_or(0);
            let new_weight = existing + diff_weight;
            let (partition_key, order_key) = if existing > 0 || new_weight > 0 {
                self.keys_for(key)
            } else {
                (None, None)
            };
            cache_updates.push((key.clone(), existing, new_weight, partition_key, order_key));
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
        for key in cache_prune {
            self.row_key_cache.remove(&key);
        }

        let order_index = self
            .order_index
            .as_ref()
            .context("topn order index missing after update")?;
        let mut output_delta = HashMap::new();
        for partition_key in affected_partitions {
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
            return Ok(Some(self.output.handle_for_version(0)));
        }

        let output_handle =
            Self::apply_deltas_to_versioned(&mut self.output, &output_delta, None, "output")
                .await
                .context("persist topn output delta")?;
        publish_transient_zset_batch(
            &output_handle,
            Arc::new(output_delta.into_iter().collect::<Vec<_>>()),
        );
        Ok(Some(output_handle))
    }
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::zset::SegmentRecord;
    use crate::stream::util::materialize_zset_handle;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::BTreeMap;

    fn bucket_for(id: u64) -> u16 {
        (id >> 48) as u16
    }

    async fn stage_version(
        dict: Arc<Dictionary<i64>>,
        table: Arc<dyn KeyValueTable>,
        namespace: &str,
        deltas: &[(i64, i64)],
    ) -> ZSetHandle {
        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let mut dict_batch = dict.batch();
        for (key, delta) in deltas {
            let id = dict_batch
                .intern(key)
                .await
                .expect("intern test key for topn");
            buckets
                .entry(bucket_for(id))
                .or_default()
                .push((id, *delta));
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

        let mut versioned = VersionedZSet::new(dict, table, namespace.to_string())
            .await
            .expect("build versioned");
        let version = versioned
            .create_version_with_base(segments, None)
            .await
            .expect("create version");
        versioned.handle_for_version(version)
    }

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open("topn", store).await.expect("open SlateDB"))
    }

    fn apply_deltas(state: &mut HashMap<i64, i64>, deltas: &[(i64, i64)]) {
        for (key, delta) in deltas {
            let entry = state.entry(*key).or_insert(0);
            *entry += *delta;
            if *entry == 0 {
                state.remove(key);
            }
        }
    }

    fn recompute_topn(state: &HashMap<i64, i64>, limit: usize, offset: usize) -> HashMap<i64, i64> {
        let mut entries: Vec<(i64, i64)> = state
            .iter()
            .filter_map(|(key, weight)| (*weight > 0).then_some((*key, *weight)))
            .collect();
        entries.sort_by_key(|(key, _)| *key);

        let mut remaining_skip = offset;
        let mut remaining_take = limit;
        let mut output = HashMap::new();
        for (key, weight) in entries {
            if remaining_take == 0 {
                break;
            }
            let mut remaining_weight = weight;
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
            output.insert(key, take as i64);
            remaining_take -= take;
        }
        output
    }

    #[tokio::test]
    async fn topn_operator_emits_ordered_limit_deltas() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topn_input", None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topn_output", None)
                .await
                .expect("output dict"),
        );
        let integrated_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topn_state", None)
                .await
                .expect("state dict"),
        );

        let state = RelationState {
            integrated: VersionedZSet::new(
                integrated_dict,
                table.clone(),
                "topn_state".to_string(),
            )
            .await
            .expect("integrated state"),
            latest_handle: ZSetHandle {
                ns: "topn_state".to_string(),
                version: 0,
            },
        };
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "topn_output".to_string(),
        )
        .await
        .expect("output");

        let partition_key: Arc<dyn Fn(&i64) -> Option<()> + Send + Sync> = Arc::new(|_| Some(()));
        let order_key: Arc<dyn Fn(&i64) -> Option<i64> + Send + Sync> =
            Arc::new(|value| Some(*value));
        let mut op = TopNOp::new(state, table.clone(), output, partition_key, order_key, 2, 0);

        let first_delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "topn_input",
            &[(3, 1), (1, 1), (2, 1)],
        )
        .await;
        let out1 = op
            .on_step(1, &[first_delta])
            .await
            .expect("topn t1")
            .expect("non-empty t1");

        let mut cache = HashMap::new();
        cache.insert("topn_output".to_string(), output_dict.clone());
        let out1_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out1)
            .await
            .expect("materialize output t1");
        assert_eq!(out1_materialized, HashMap::from([(1, 1), (2, 1)]));

        let second_delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "topn_input",
            &[(2, -1), (4, 1)],
        )
        .await;
        let out2 = op
            .on_step(2, &[second_delta])
            .await
            .expect("topn t2")
            .expect("non-empty t2");
        let out2_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out2)
            .await
            .expect("materialize output t2");
        assert_eq!(out2_materialized, HashMap::from([(2, -1), (3, 1)]));
    }

    #[tokio::test]
    async fn topn_operator_matches_full_recompute() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topn_recompute_input", None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topn_recompute_output", None)
                .await
                .expect("output dict"),
        );
        let integrated_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topn_recompute_state", None)
                .await
                .expect("state dict"),
        );

        let state = RelationState {
            integrated: VersionedZSet::new(
                integrated_dict,
                table.clone(),
                "topn_recompute_state".to_string(),
            )
            .await
            .expect("integrated state"),
            latest_handle: ZSetHandle {
                ns: "topn_recompute_state".to_string(),
                version: 0,
            },
        };
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "topn_recompute_output".to_string(),
        )
        .await
        .expect("output");

        let partition_key: Arc<dyn Fn(&i64) -> Option<()> + Send + Sync> = Arc::new(|_| Some(()));
        let order_key: Arc<dyn Fn(&i64) -> Option<i64> + Send + Sync> =
            Arc::new(|value| Some(*value));
        let mut op = TopNOp::new(state, table.clone(), output, partition_key, order_key, 2, 1);

        let steps = vec![vec![(5, 1), (2, 1), (1, 1)], vec![(1, -1), (3, 2)]];

        let mut full_input: HashMap<i64, i64> = HashMap::new();
        let mut full_output: HashMap<i64, i64> = HashMap::new();

        for (idx, deltas) in steps.into_iter().enumerate() {
            let delta_handle = stage_version(
                input_dict.clone(),
                table.clone(),
                "topn_recompute_input",
                &deltas,
            )
            .await;
            let output_handle = op
                .on_step(idx as i64 + 1, &[delta_handle])
                .await
                .expect("run topn step");

            apply_deltas(&mut full_input, &deltas);
            let recompute = recompute_topn(&full_input, 2, 1);
            let expected_delta_vec = compute_delta(&full_output, &recompute);
            let expected_delta: HashMap<i64, i64> = expected_delta_vec.into_iter().collect();

            if let Some(handle) = output_handle {
                let mut cache = HashMap::new();
                cache.insert("topn_recompute_output".to_string(), output_dict.clone());
                let actual_delta =
                    materialize_zset_handle::<i64>(table.clone(), &mut cache, &handle)
                        .await
                        .expect("materialize topn output");
                assert_eq!(actual_delta, expected_delta);
            } else {
                assert!(expected_delta.is_empty());
            }

            let integrated_after = op
                .state
                .integrated
                .materialize()
                .await
                .expect("materialize integrated");
            assert_eq!(integrated_after, full_input);

            full_output = recompute;
        }
    }

    #[tokio::test]
    async fn topn_operator_applies_limit_per_partition() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topn_partition_input", None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topn_partition_output", None)
                .await
                .expect("output dict"),
        );
        let integrated_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topn_partition_state", None)
                .await
                .expect("state dict"),
        );

        let state = RelationState {
            integrated: VersionedZSet::new(
                integrated_dict,
                table.clone(),
                "topn_partition_state".to_string(),
            )
            .await
            .expect("integrated state"),
            latest_handle: ZSetHandle {
                ns: "topn_partition_state".to_string(),
                version: 0,
            },
        };
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "topn_partition_output".to_string(),
        )
        .await
        .expect("output");

        // Key encoding: partition = key / 100, order = key % 100.
        let partition_key: Arc<dyn Fn(&i64) -> Option<i64> + Send + Sync> =
            Arc::new(|value| Some(*value / 100));
        let order_key: Arc<dyn Fn(&i64) -> Option<i64> + Send + Sync> =
            Arc::new(|value| Some(*value % 100));
        let mut op = TopNOp::new(state, table.clone(), output, partition_key, order_key, 1, 0);

        let delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "topn_partition_input",
            &[(101, 1), (102, 1), (201, 1), (203, 1)],
        )
        .await;
        let out = op
            .on_step(1, &[delta])
            .await
            .expect("topn partition step")
            .expect("non-empty delta");

        let mut cache = HashMap::new();
        cache.insert("topn_partition_output".to_string(), output_dict.clone());
        let materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out)
            .await
            .expect("materialize output");
        assert_eq!(materialized, HashMap::from([(101, 1), (201, 1)]));
    }

    #[tokio::test]
    async fn topn_operator_updates_only_affected_partition_output() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topn_partition_local_input", None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topn_partition_local_output", None)
                .await
                .expect("output dict"),
        );
        let integrated_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topn_partition_local_state", None)
                .await
                .expect("state dict"),
        );

        let state = RelationState {
            integrated: VersionedZSet::new(
                integrated_dict,
                table.clone(),
                "topn_partition_local_state".to_string(),
            )
            .await
            .expect("integrated state"),
            latest_handle: ZSetHandle {
                ns: "topn_partition_local_state".to_string(),
                version: 0,
            },
        };
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "topn_partition_local_output".to_string(),
        )
        .await
        .expect("output");

        let partition_key: Arc<dyn Fn(&i64) -> Option<i64> + Send + Sync> =
            Arc::new(|value| Some(*value / 100));
        let order_key: Arc<dyn Fn(&i64) -> Option<i64> + Send + Sync> =
            Arc::new(|value| Some(*value % 100));
        let mut op = TopNOp::new(state, table.clone(), output, partition_key, order_key, 1, 0);

        let initial = stage_version(
            input_dict.clone(),
            table.clone(),
            "topn_partition_local_input",
            &[(101, 1), (102, 1), (201, 1), (202, 1)],
        )
        .await;
        op.on_step(1, &[initial])
            .await
            .expect("initial step")
            .expect("initial output");

        let update = stage_version(
            input_dict,
            table.clone(),
            "topn_partition_local_input",
            &[(100, 1)],
        )
        .await;
        let out = op
            .on_step(2, &[update])
            .await
            .expect("partition-local update")
            .expect("non-empty delta");

        let mut cache = HashMap::new();
        cache.insert(
            "topn_partition_local_output".to_string(),
            output_dict.clone(),
        );
        let materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out)
            .await
            .expect("materialize output");
        assert_eq!(materialized, HashMap::from([(101, -1), (100, 1)]));
    }

    #[tokio::test]
    async fn topn_operator_uses_stable_tie_breaking_and_retractions() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topn_tie_input", None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topn_tie_output", None)
                .await
                .expect("output dict"),
        );
        let integrated_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "topn_tie_state", None)
                .await
                .expect("state dict"),
        );

        let state = RelationState {
            integrated: VersionedZSet::new(
                integrated_dict,
                table.clone(),
                "topn_tie_state".to_string(),
            )
            .await
            .expect("integrated state"),
            latest_handle: ZSetHandle {
                ns: "topn_tie_state".to_string(),
                version: 0,
            },
        };
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "topn_tie_output".to_string(),
        )
        .await
        .expect("output");

        let partition_key: Arc<dyn Fn(&i64) -> Option<()> + Send + Sync> = Arc::new(|_| Some(()));
        // All inserted rows tie on this key (value % 10 == 1).
        let order_key: Arc<dyn Fn(&i64) -> Option<i64> + Send + Sync> =
            Arc::new(|value| Some(*value % 10));
        let mut op = TopNOp::new(state, table.clone(), output, partition_key, order_key, 2, 0);

        let first_delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "topn_tie_input",
            &[(11, 1), (21, 1), (31, 1)],
        )
        .await;
        let out1 = op
            .on_step(1, &[first_delta])
            .await
            .expect("topn tie t1")
            .expect("non-empty t1");

        let mut cache = HashMap::new();
        cache.insert("topn_tie_output".to_string(), output_dict.clone());
        let out1_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out1)
            .await
            .expect("materialize output t1");
        assert_eq!(out1_materialized, HashMap::from([(11, 1), (21, 1)]));

        let second_delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "topn_tie_input",
            &[(11, -1), (41, 1)],
        )
        .await;
        let out2 = op
            .on_step(2, &[second_delta])
            .await
            .expect("topn tie t2")
            .expect("non-empty t2");
        let out2_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out2)
            .await
            .expect("materialize output t2");
        assert_eq!(out2_materialized, HashMap::from([(11, -1), (31, 1)]));
    }
}
