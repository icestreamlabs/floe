use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, ensure};
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;

use crate::collections::IndexedBatchZSet;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::delta_zset_handle;

type KeyExtractor<V, K> = Arc<dyn Fn(&V) -> Option<K> + Send + Sync>;
type Aggregator<K, V, A> = Arc<dyn Fn(&K, &[(V, i64)]) -> Option<A> + Send + Sync>;

pub struct RollingAggregateOp<K, V, A>
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
    A: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    A::Archived: RkyvDeserialize<A, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub state: RelationState<(K, A)>,
    pub index: IndexedBatchZSet<K, V>,
    pub table: Arc<dyn KeyValueTable>,
    pub key_extractor: KeyExtractor<V, K>,
    pub aggregator: Aggregator<K, V, A>,
    output: VersionedZSet<(K, A)>,
    window_size: usize,
    buffer: VecDeque<HashMap<V, i64>>,
    dict_cache: HashMap<String, Arc<Dictionary<V>>>,
    aggregate_cache: Option<HashMap<K, A>>,
}

impl<K, V, A> RollingAggregateOp<K, V, A>
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
    A: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    A::Archived: RkyvDeserialize<A, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub fn new(
        state: RelationState<(K, A)>,
        index: IndexedBatchZSet<K, V>,
        table: Arc<dyn KeyValueTable>,
        key_extractor: KeyExtractor<V, K>,
        aggregator: Aggregator<K, V, A>,
        output: VersionedZSet<(K, A)>,
        window_size: usize,
    ) -> Result<Self> {
        ensure!(window_size > 0, "rolling window size must be positive");
        Ok(Self {
            state,
            index,
            table,
            key_extractor,
            aggregator,
            output,
            window_size,
            buffer: VecDeque::new(),
            dict_cache: HashMap::new(),
            aggregate_cache: None,
        })
    }

    async fn ensure_aggregate_cache(&mut self) -> Result<()> {
        if self.aggregate_cache.is_some() {
            return Ok(());
        }

        let materialized = self
            .state
            .integrated
            .materialize()
            .await
            .context("materialize rolling aggregate state")?;
        let mut cache = HashMap::new();
        for ((key, aggregate), weight) in materialized {
            if weight != 0 {
                cache.insert(key, aggregate);
            }
        }
        self.aggregate_cache = Some(cache);
        Ok(())
    }

    fn coalesce_deltas(&self, deltas: Vec<(V, i64)>) -> HashMap<V, i64>
    where
        V: Clone + Eq + Hash,
    {
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
        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let dict = versioned.dictionary();
        let mut dict_batch = dict.batch();
        for (key, delta) in deltas {
            if *delta == 0 {
                continue;
            }
            let id = dict_batch
                .intern(key)
                .await
                .context("intern key while staging rolling aggregate delta")?;
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

        if segments.is_empty() {
            if base.is_some()
                && let Some(handle) = versioned.current_handle()
            {
                return Ok(handle);
            }
            return Ok(versioned.handle_for_version(0));
        }

        let mut batch = WriteBatch::new();
        let plan = versioned
            .enqueue_version_with_base(segments, base, 0, &mut batch)
            .await
            .context("schedule rolling aggregate update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write rolling aggregate update")?;

        let mut cleanup = WriteBatch::new();
        cleanup.delete(versioned.intent_key_bytes());
        versioned
            .table()
            .write_batch(cleanup)
            .await
            .context("clear rolling aggregate intent")?;

        versioned.apply_version_plan(&plan);
        Ok(versioned.handle_for_version(plan.version))
    }
}

#[async_trait]
impl<K, V, A> DeltaOperator for RollingAggregateOp<K, V, A>
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
    A: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    A::Archived: RkyvDeserialize<A, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let delta_handle = inputs
            .first()
            .cloned()
            .context("rolling aggregate requires one input delta handle")?;

        let delta_values =
            delta_zset_handle::<V>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for rolling aggregate")?;

        let delta_map = self.coalesce_deltas(delta_values);
        self.buffer.push_back(delta_map.clone());

        let mut net_delta = delta_map;
        if self.buffer.len() > self.window_size {
            if let Some(evicted) = self.buffer.pop_front() {
                for (row, weight) in evicted {
                    let entry = net_delta.entry(row.clone()).or_insert(0);
                    *entry -= weight;
                    if *entry == 0 {
                        net_delta.remove(&row);
                    }
                }
            }
        }

        if net_delta.is_empty() {
            return Ok(None);
        }

        let mut keyed_deltas: HashMap<K, Vec<(V, i64)>> = HashMap::new();
        for (row, weight) in &net_delta {
            if *weight == 0 {
                continue;
            }
            if let Some(key) = (self.key_extractor)(row) {
                keyed_deltas
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .push((row.clone(), *weight));
            }
        }

        if keyed_deltas.is_empty() {
            return Ok(None);
        }

        let affected_keys: HashSet<K> = keyed_deltas.keys().cloned().collect();
        let mut index_updates = Vec::new();
        for (key, entries) in &keyed_deltas {
            for (row, weight) in entries {
                index_updates.push((key.clone(), row.clone(), *weight));
            }
        }

        self.index
            .apply_deltas(index_updates)
            .await
            .context("update rolling aggregate index")?;

        self.ensure_aggregate_cache()
            .await
            .context("load rolling aggregate cache")?;

        let mut aggregate_updates: HashMap<(K, A), i64> = HashMap::new();
        let aggregate_cache = self
            .aggregate_cache
            .as_mut()
            .ok_or_else(|| anyhow!("missing rolling aggregate cache"))?;

        for key in affected_keys {
            let values = self
                .index
                .values_for_key(&key)
                .await
                .context("load rolling aggregate values")?;
            let new_value = (self.aggregator)(&key, &values);
            let old_value = aggregate_cache.get(&key).cloned();

            match (old_value, new_value) {
                (Some(old), Some(new)) if old == new => {}
                (Some(old), Some(new)) => {
                    aggregate_updates.insert((key.clone(), old), -1);
                    aggregate_updates.insert((key.clone(), new.clone()), 1);
                    aggregate_cache.insert(key.clone(), new);
                }
                (Some(old), None) => {
                    aggregate_updates.insert((key.clone(), old), -1);
                    aggregate_cache.remove(&key);
                }
                (None, Some(new)) => {
                    aggregate_updates.insert((key.clone(), new.clone()), 1);
                    aggregate_cache.insert(key.clone(), new);
                }
                (None, None) => {}
            }
        }

        if aggregate_updates.is_empty() {
            return Ok(None);
        }

        let base_version = self
            .state
            .integrated
            .current_handle()
            .map(|handle| handle.version);
        let new_integrated_handle = Self::apply_deltas_to_versioned(
            &mut self.state.integrated,
            &aggregate_updates,
            base_version,
        )
        .await
        .context("update rolling aggregate state")?;
        self.state.update_handle(new_integrated_handle);

        let delta_handle =
            Self::apply_deltas_to_versioned(&mut self.output, &aggregate_updates, None)
                .await
                .context("persist rolling aggregate output")?;
        Ok(Some(delta_handle))
    }
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::zset::{SegmentRecord, VersionedZSet};
    use crate::storage::dictionary::Dictionary;
    use crate::stream::runtime::DeltaOperator;
    use crate::stream::util::{compute_delta, materialize_zset_handle};
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::BTreeMap;

    type Row = i64;

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open("rolling_agg", store).await.expect("open SlateDB"))
    }

    async fn stage_version<K>(
        dict: Arc<Dictionary<K>>,
        table: Arc<dyn KeyValueTable>,
        namespace: &str,
        deltas: &[(K, i64)],
    ) -> ZSetHandle
    where
        K: Archive
            + Clone
            + Eq
            + Hash
            + Send
            + Sync
            + 'static
            + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
        K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    {
        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let mut dict_batch = dict.batch();
        for (key, delta) in deltas {
            let id = dict_batch
                .intern(key)
                .await
                .expect("intern key for rolling test");
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

    fn apply_deltas<K: Clone + Eq + Hash>(state: &mut HashMap<K, i64>, deltas: &[(K, i64)]) {
        for (key, delta) in deltas {
            let entry = state.entry(key.clone()).or_insert(0);
            *entry += *delta;
            if *entry == 0 {
                state.remove(key);
            }
        }
    }

    #[tokio::test]
    async fn rolling_aggregate_tracks_last_n_ticks() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));

        let input_dict = Arc::new(
            Dictionary::<Row>::with_table(table.clone(), "rolling_input", None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<(i64, i64)>::with_table(table.clone(), "rolling_output", None)
                .await
                .expect("output dict"),
        );

        let state = RelationState::empty(table.clone(), "rolling_state".to_string())
            .await
            .expect("rolling state");
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "rolling_output".to_string(),
        )
        .await
        .expect("output zset");

        let index = IndexedBatchZSet::new(table.clone(), "rolling_index");
        let key_extractor = Arc::new(|row: &Row| Some(*row % 2));
        let aggregator: Arc<dyn Fn(&i64, &[(Row, i64)]) -> Option<i64> + Send + Sync> =
            Arc::new(|_key, values| {
                let mut count = 0i64;
                let mut has_rows = false;
                for (_row, weight) in values {
                    if *weight == 0 {
                        continue;
                    }
                    has_rows = true;
                    count += *weight;
                }
                if has_rows { Some(count) } else { None }
            });

        let mut op = RollingAggregateOp::new(
            state,
            index,
            table.clone(),
            key_extractor,
            aggregator,
            output,
            2,
        )
        .expect("rolling aggregate op");

        let deltas: Vec<Vec<(Row, i64)>> =
            vec![vec![(1, 1), (2, 1)], vec![(3, 1)], vec![], vec![(4, 1)]];

        let mut window_buffer: VecDeque<HashMap<Row, i64>> = VecDeque::new();
        let mut window_state: HashMap<Row, i64> = HashMap::new();
        let mut prev_output: HashMap<(i64, i64), i64> = HashMap::new();

        let mut cache_out = HashMap::new();
        cache_out.insert("rolling_output".to_string(), output_dict.clone());

        for (step, delta) in deltas.iter().enumerate() {
            let mut delta_map = HashMap::new();
            apply_deltas(&mut delta_map, delta);
            window_buffer.push_back(delta_map.clone());
            apply_deltas(&mut window_state, delta);

            if window_buffer.len() > 2 {
                if let Some(evicted) = window_buffer.pop_front() {
                    let mut evict_deltas = Vec::new();
                    for (row, weight) in evicted {
                        evict_deltas.push((row, -weight));
                    }
                    apply_deltas(&mut window_state, &evict_deltas);
                }
            }

            let mut aggregated: HashMap<(i64, i64), i64> = HashMap::new();
            let mut counts = HashMap::<i64, i64>::new();
            for (row, weight) in &window_state {
                if *weight == 0 {
                    continue;
                }
                *counts.entry(row % 2).or_insert(0) += *weight;
            }
            for (key, count) in counts {
                aggregated.insert((key, count), 1);
            }

            let expected_delta: HashMap<(i64, i64), i64> = compute_delta(&prev_output, &aggregated)
                .into_iter()
                .collect();

            let handle = if delta.is_empty() {
                ZSetHandle {
                    ns: "rolling_input".to_string(),
                    version: 0,
                }
            } else {
                stage_version(input_dict.clone(), table.clone(), "rolling_input", delta).await
            };

            let out_handle = op
                .on_step(step as i64, &[handle])
                .await
                .expect("rolling step");

            if expected_delta.is_empty() {
                assert!(out_handle.is_none(), "expected empty output at step {step}");
            } else {
                let out_handle = out_handle.expect("output handle");
                let materialized = materialize_zset_handle::<(i64, i64)>(
                    table.clone(),
                    &mut cache_out,
                    &out_handle,
                )
                .await
                .expect("materialize output");
                assert_eq!(materialized, expected_delta, "step {step}");
            }

            prev_output = aggregated;
        }
    }
}
