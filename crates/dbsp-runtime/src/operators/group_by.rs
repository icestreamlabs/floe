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

type KeyExtractor<V, K> = Arc<dyn Fn(&V) -> Option<K> + Send + Sync>;
type Aggregator<K, V, A> = Arc<dyn Fn(&K, &[(V, i64)]) -> Option<A> + Send + Sync>;

pub struct GroupByOp<K, V, A>
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
    dict_cache: HashMap<String, Arc<Dictionary<V>>>,
    aggregate_cache: Option<HashMap<K, A>>,
    logical_work: metrics::LogicalWorkCollector,
}

impl<K, V, A> GroupByOp<K, V, A>
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
    ) -> Self {
        debug_assert_eq!(index.engine_kind(), "indexed_batch");
        Self {
            state,
            index,
            table,
            key_extractor,
            aggregator,
            output,
            dict_cache: HashMap::new(),
            aggregate_cache: None,
            logical_work: metrics::LogicalWorkCollector::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn last_logical_work(&self) -> metrics::LogicalWorkSnapshot {
        self.logical_work.last_tick()
    }

    async fn ensure_aggregate_cache(&mut self) -> Result<usize> {
        if self.aggregate_cache.is_some() {
            return Ok(0);
        }

        let materialized = self
            .state
            .integrated
            .materialize()
            .await
            .context("materialize group-by integrated state")?;
        let mut cache = HashMap::new();
        let rebuild_rows = materialized.len();
        for ((key, aggregate), weight) in materialized {
            if weight != 0 {
                cache.insert(key, aggregate);
            }
        }
        self.aggregate_cache = Some(cache);
        Ok(rebuild_rows)
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
        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let dict = versioned.dictionary();
        let mut keyed_deltas: Vec<(&T, i64)> = Vec::new();
        for (key, delta) in deltas {
            if *delta == 0 {
                continue;
            }
            keyed_deltas.push((key, *delta));
        }
        let ids = dict
            .intern_many_values_unique(keyed_deltas.iter().map(|(key, _)| *key))
            .await
            .context("batch intern keys while staging group-by delta")?;
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

        if segments.is_empty() {
            if base.is_some()
                && let Some(handle) = versioned.current_handle()
            {
                return Ok(handle);
            }
            return Ok(versioned.handle_for_version(0));
        }

        let persist_start = std::time::Instant::now();
        let mut batch = WriteBatch::new();
        let plan = versioned
            .enqueue_version_with_base(segments, base, 0, &mut batch)
            .await
            .context("schedule group-by version update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write group-by version update")?;

        versioned.apply_version_plan(&plan);
        metrics::observe_operator_persistence_latency_ms(
            "group_by",
            state_label,
            persist_start.elapsed().as_millis() as u64,
        );
        Ok(versioned.handle_for_version(plan.version))
    }
}

#[async_trait]
impl<K, V, A> DeltaOperator for GroupByOp<K, V, A>
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
            .context("group-by operator requires one input delta handle")?;

        let delta_values =
            delta_zset_handle_batch::<V>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for group-by")?;
        let mut work = metrics::LogicalWorkSnapshot::from_input_delta_rows(delta_values.len());

        if delta_values.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }

        let coalesced = self.coalesce_deltas(delta_values.as_ref().clone());
        if coalesced.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }

        let mut updates = Vec::new();
        let mut affected_keys: HashSet<K> = HashSet::new();
        for (value, weight) in coalesced {
            if weight == 0 {
                continue;
            }
            if let Some(key) = (self.key_extractor)(&value) {
                affected_keys.insert(key.clone());
                updates.push((key, value, weight));
            }
        }

        if updates.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }
        work.changed_groups = affected_keys.len() as u64;

        let index_persist_start = std::time::Instant::now();
        work.record_persisted_rows(updates.len());
        self.index
            .apply_deltas(updates)
            .await
            .context("update group-by index")?;
        metrics::observe_operator_persistence_latency_ms(
            "group_by",
            "index",
            index_persist_start.elapsed().as_millis() as u64,
        );

        let cache_rebuild_rows = self
            .ensure_aggregate_cache()
            .await
            .context("load group-by cache")?;
        if cache_rebuild_rows != 0 {
            work.cache_rebuild_rows = cache_rebuild_rows as u64;
            work.state_full_scan_count = 1;
            work.state_scan_rows = work
                .state_scan_rows
                .saturating_add(cache_rebuild_rows as u64);
        }

        let mut output_deltas: HashMap<(K, A), i64> = HashMap::new();
        let mut cache_updates = Vec::new();
        {
            let aggregate_cache = self
                .aggregate_cache
                .as_ref()
                .context("group-by cache missing")?;

            for key in &affected_keys {
                let (values, lookup_metrics) = self
                    .index
                    .values_for_key_with_metrics(key)
                    .await
                    .context("load group-by key values")?;
                work.add_lookup_metrics(lookup_metrics);
                work.group_state_rows_examined = work
                    .group_state_rows_examined
                    .saturating_add(values.len() as u64);
                let new_value = (self.aggregator)(key, &values);
                let old_value = aggregate_cache.get(key).cloned();

                match (old_value, new_value) {
                    (Some(old), Some(new)) if old == new => {}
                    (Some(old), Some(new)) => {
                        output_deltas.insert((key.clone(), old), -1);
                        output_deltas.insert((key.clone(), new.clone()), 1);
                        cache_updates.push((key.clone(), Some(new)));
                    }
                    (Some(old), None) => {
                        output_deltas.insert((key.clone(), old), -1);
                        cache_updates.push((key.clone(), None));
                    }
                    (None, Some(new)) => {
                        output_deltas.insert((key.clone(), new.clone()), 1);
                        cache_updates.push((key.clone(), Some(new)));
                    }
                    (None, None) => {}
                }
            }
        }

        if output_deltas.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }
        work.aggregate_state_rows_updated = cache_updates.len() as u64;
        work.record_output_delta_rows(output_deltas.len());

        let base_version = self.state.base_version_for_update();
        let new_integrated_handle = Self::apply_deltas_to_versioned(
            &mut self.state.integrated,
            &output_deltas,
            base_version,
            "integrated",
        )
        .await
        .context("update group-by integrated state")?;
        work.record_persisted_rows(output_deltas.len());
        self.state.update_handle(new_integrated_handle);

        if let Some(aggregate_cache) = self.aggregate_cache.as_mut() {
            for (key, value) in cache_updates {
                if let Some(value) = value {
                    aggregate_cache.insert(key, value);
                } else {
                    aggregate_cache.remove(&key);
                }
            }
        }

        let delta_handle =
            Self::apply_deltas_to_versioned(&mut self.output, &output_deltas, None, "output")
                .await
                .context("persist group-by delta output")?;
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
mod tests {
    use super::*;
    use crate::stream::util::{compute_delta, materialize_zset_handle};
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::BTreeMap;
    use std::sync::Arc;

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
                .expect("intern test key for group-by");
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
        Arc::new(Db::open("group_by", store).await.expect("open SlateDB"))
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

    #[tokio::test]
    async fn group_by_updates_aggregates_by_key() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "group_by_input", None)
                .await
                .expect("build input dictionary"),
        );
        let output_dict = Arc::new(
            Dictionary::<(i64, i64)>::with_table(table.clone(), "group_by_output", None)
                .await
                .expect("build output dictionary"),
        );
        let integrated_dict = Arc::new(
            Dictionary::<(i64, i64)>::with_table(table.clone(), "group_by_state", None)
                .await
                .expect("build state dictionary"),
        );

        let state = RelationState {
            integrated: VersionedZSet::new(
                integrated_dict.clone(),
                table.clone(),
                "group_by_state".to_string(),
            )
            .await
            .expect("integrated state"),
            latest_handle: ZSetHandle {
                ns: "group_by_state".to_string(),
                version: 0,
            },
        };
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "group_by_output".to_string(),
        )
        .await
        .expect("output");

        let index = IndexedBatchZSet::new(table.clone(), "group_by_index");
        let key_extractor: KeyExtractor<i64, i64> =
            Arc::new(
                |value: &i64| {
                    if *value >= 0 { Some(value % 2) } else { None }
                },
            );
        let aggregator: Aggregator<i64, i64, i64> = Arc::new(|_, values| {
            if values.is_empty() {
                return None;
            }
            let mut sum = 0i64;
            for (value, weight) in values {
                sum += value * weight;
            }
            Some(sum)
        });

        let mut op = GroupByOp::new(
            state,
            index,
            table.clone(),
            key_extractor,
            aggregator,
            output,
        );

        let first_delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "group_by_input",
            &[(1, 1), (2, 1), (3, 2)],
        )
        .await;
        let out1 = op
            .on_step(1, &[first_delta])
            .await
            .expect("group-by t1")
            .expect("non-empty t1");

        let mut cache = HashMap::new();
        cache.insert("group_by_output".to_string(), output_dict.clone());
        let out1_materialized = crate::stream::util::materialize_zset_handle::<(i64, i64)>(
            table.clone(),
            &mut cache,
            &out1,
        )
        .await
        .expect("materialize output t1");
        assert_eq!(out1_materialized.get(&(1, 7)), Some(&1));
        assert_eq!(out1_materialized.get(&(0, 2)), Some(&1));

        let integrated_after_t1 = op
            .state
            .integrated
            .materialize()
            .await
            .expect("integrated t1");
        assert_eq!(integrated_after_t1.get(&(1, 7)), Some(&1));
        assert_eq!(integrated_after_t1.get(&(0, 2)), Some(&1));

        let second_delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "group_by_input",
            &[(1, -1), (2, -1), (4, 3)],
        )
        .await;
        let out2 = op
            .on_step(2, &[second_delta])
            .await
            .expect("group-by t2")
            .expect("non-empty t2");
        let out2_materialized = crate::stream::util::materialize_zset_handle::<(i64, i64)>(
            table.clone(),
            &mut cache,
            &out2,
        )
        .await
        .expect("materialize output t2");

        assert_eq!(out2_materialized.get(&(1, 7)), Some(&-1));
        assert_eq!(out2_materialized.get(&(1, 6)), Some(&1));
        assert_eq!(out2_materialized.get(&(0, 2)), Some(&-1));
        assert_eq!(out2_materialized.get(&(0, 12)), Some(&1));

        let integrated_after_t2 = op
            .state
            .integrated
            .materialize()
            .await
            .expect("integrated t2");
        assert_eq!(integrated_after_t2.get(&(1, 6)), Some(&1));
        assert_eq!(integrated_after_t2.get(&(0, 12)), Some(&1));
        assert!(!integrated_after_t2.contains_key(&(1, 7)));
        assert!(!integrated_after_t2.contains_key(&(0, 2)));
    }

    #[tokio::test]
    async fn group_by_operator_matches_full_recompute() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "group_by_recompute_input", None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<(i64, i64)>::with_table(table.clone(), "group_by_recompute_output", None)
                .await
                .expect("output dict"),
        );
        let integrated_dict = Arc::new(
            Dictionary::<(i64, i64)>::with_table(table.clone(), "group_by_recompute_state", None)
                .await
                .expect("state dict"),
        );

        let state = RelationState {
            integrated: VersionedZSet::new(
                integrated_dict.clone(),
                table.clone(),
                "group_by_recompute_state".to_string(),
            )
            .await
            .expect("integrated state"),
            latest_handle: ZSetHandle {
                ns: "group_by_recompute_state".to_string(),
                version: 0,
            },
        };
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "group_by_recompute_output".to_string(),
        )
        .await
        .expect("output");
        let index = IndexedBatchZSet::new(table.clone(), "group_by_recompute_index");

        let key_extractor: KeyExtractor<i64, i64> =
            Arc::new(
                |value: &i64| {
                    if *value >= 0 { Some(value % 2) } else { None }
                },
            );
        let aggregator: Aggregator<i64, i64, i64> = Arc::new(|_, values| {
            if values.is_empty() {
                return None;
            }
            let mut sum = 0i64;
            for (value, weight) in values {
                sum += value * weight;
            }
            Some(sum)
        });

        let mut op = GroupByOp::new(
            state,
            index,
            table.clone(),
            key_extractor.clone(),
            aggregator.clone(),
            output,
        );

        let steps = vec![
            vec![(1, 1), (2, 1), (3, 2), (-1, 1)],
            vec![(1, -1), (2, -1), (4, 3)],
        ];

        let mut full_input: HashMap<i64, i64> = HashMap::new();
        let mut full_output: HashMap<(i64, i64), i64> = HashMap::new();

        for (idx, deltas) in steps.into_iter().enumerate() {
            let delta_handle = stage_version(
                input_dict.clone(),
                table.clone(),
                "group_by_recompute_input",
                &deltas,
            )
            .await;
            let output_handle = op
                .on_step(idx as i64 + 1, &[delta_handle])
                .await
                .expect("run group-by step");

            apply_deltas(&mut full_input, &deltas);

            let mut values_by_key: HashMap<i64, Vec<(i64, i64)>> = HashMap::new();
            for (value, weight) in &full_input {
                if let Some(key) = key_extractor(value) {
                    values_by_key
                        .entry(key)
                        .or_default()
                        .push((*value, *weight));
                }
            }

            let mut recompute: HashMap<(i64, i64), i64> = HashMap::new();
            for (key, values) in values_by_key {
                if let Some(aggregate) = aggregator(&key, &values) {
                    recompute.insert((key, aggregate), 1);
                }
            }

            let expected_delta_vec = compute_delta(&full_output, &recompute);
            let expected_delta: HashMap<(i64, i64), i64> = expected_delta_vec.into_iter().collect();

            if let Some(handle) = output_handle {
                let mut cache = HashMap::new();
                cache.insert("group_by_recompute_output".to_string(), output_dict.clone());
                let actual_delta =
                    materialize_zset_handle::<(i64, i64)>(table.clone(), &mut cache, &handle)
                        .await
                        .expect("materialize group-by output");
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
            assert_eq!(integrated_after, recompute);

            full_output = recompute;
        }
    }

    async fn run_group_by_history_probe(history_rows: i64) -> metrics::LogicalWorkSnapshot {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let prefix = format!("group_by_history_{history_rows}");
        let input_ns = format!("{prefix}_input");
        let output_ns = format!("{prefix}_output");
        let state_ns = format!("{prefix}_state");

        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), input_ns.clone(), None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<(i64, i64)>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .expect("output dict"),
        );
        let state_dict = Arc::new(
            Dictionary::<(i64, i64)>::with_table(table.clone(), state_ns.clone(), None)
                .await
                .expect("state dict"),
        );

        let state = RelationState {
            integrated: VersionedZSet::new(state_dict, table.clone(), state_ns.clone())
                .await
                .expect("integrated state"),
            latest_handle: ZSetHandle {
                ns: state_ns,
                version: 0,
            },
        };
        let output = VersionedZSet::new(output_dict.clone(), table.clone(), output_ns.clone())
            .await
            .expect("output");
        let key_extractor: KeyExtractor<i64, i64> = Arc::new(|value: &i64| Some(*value));
        let aggregator: Aggregator<i64, i64, i64> = Arc::new(|_, values| {
            let sum = values
                .iter()
                .fold(0i64, |acc, (value, weight)| acc + value * weight);
            (sum != 0).then_some(sum)
        });
        let mut op = GroupByOp::new(
            state,
            IndexedBatchZSet::new(table.clone(), format!("{prefix}_index")),
            table.clone(),
            key_extractor,
            aggregator,
            output,
        );

        let history = (0..history_rows)
            .map(|idx| (1_000_000 + idx, 1))
            .collect::<Vec<_>>();
        let seed = stage_version(input_dict.clone(), table.clone(), &input_ns, &history).await;
        op.on_step(1, &[seed]).await.expect("seed group-by");

        let fixed = stage_version(input_dict, table.clone(), &input_ns, &[(7, 1)]).await;
        let output = op
            .on_step(2, &[fixed])
            .await
            .expect("fixed group-by")
            .expect("group-by output");

        let mut cache = HashMap::new();
        cache.insert(output_ns, output_dict);
        let materialized = materialize_zset_handle::<(i64, i64)>(table, &mut cache, &output)
            .await
            .expect("materialize fixed group-by");
        assert_eq!(materialized, HashMap::from([((7, 7), 1)]));

        op.last_logical_work()
    }

    #[tokio::test]
    async fn group_by_logical_work_uses_changed_groups_not_unrelated_history() {
        let baseline = run_group_by_history_probe(8).await;
        for history_rows in [128, 1024] {
            let actual = run_group_by_history_probe(history_rows).await;
            assert_eq!(actual.input_delta_rows, baseline.input_delta_rows);
            assert_eq!(actual.changed_groups, baseline.changed_groups);
            assert_eq!(
                actual.group_state_rows_examined,
                baseline.group_state_rows_examined
            );
            assert_eq!(actual.state_lookup_keys, baseline.state_lookup_keys);
            assert_eq!(actual.output_delta_rows, baseline.output_delta_rows);
            assert_eq!(actual.state_full_scan_count, 0);
            assert_eq!(actual.cache_rebuild_rows, 0);
        }

        assert_eq!(baseline.input_delta_rows, 1);
        assert_eq!(baseline.changed_groups, 1);
        assert_eq!(baseline.group_state_rows_examined, 1);
        assert_eq!(baseline.state_lookup_keys, 1);
        assert_eq!(baseline.output_delta_rows, 1);
    }
}
