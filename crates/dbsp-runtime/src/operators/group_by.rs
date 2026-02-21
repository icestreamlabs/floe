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

use crate::collections::LegacyIndexedBatchZSet;
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
    pub index: LegacyIndexedBatchZSet<K, V>,
    pub table: Arc<dyn KeyValueTable>,
    pub key_extractor: KeyExtractor<V, K>,
    pub aggregator: Aggregator<K, V, A>,
    output: VersionedZSet<(K, A)>,
    dict_cache: HashMap<String, Arc<Dictionary<V>>>,
    aggregate_cache: Option<HashMap<K, A>>,
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
        index: LegacyIndexedBatchZSet<K, V>,
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
        }
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
            .context("materialize group-by integrated state")?;
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
                .context("intern key while staging group-by delta")?;
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
            .context("schedule group-by version update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write group-by version update")?;

        let mut cleanup = WriteBatch::new();
        cleanup.delete(versioned.intent_key_bytes());
        versioned
            .table()
            .write_batch(cleanup)
            .await
            .context("clear group-by intent")?;

        versioned.apply_version_plan(&plan);
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
            delta_zset_handle::<V>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for group-by")?;

        if delta_values.is_empty() {
            return Ok(None);
        }

        let coalesced = self.coalesce_deltas(delta_values);
        if coalesced.is_empty() {
            return Ok(None);
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
            return Ok(None);
        }

        self.index
            .apply_deltas(updates)
            .await
            .context("update group-by index")?;

        self.ensure_aggregate_cache()
            .await
            .context("load group-by cache")?;

        let mut output_deltas: HashMap<(K, A), i64> = HashMap::new();
        let mut cache_updates = Vec::new();
        {
            let aggregate_cache = self
                .aggregate_cache
                .as_ref()
                .context("group-by cache missing")?;

            for key in &affected_keys {
                let values = self
                    .index
                    .values_for_key(key)
                    .await
                    .context("load group-by key values")?;
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
            return Ok(None);
        }

        let base_version = self
            .state
            .integrated
            .current_handle()
            .map(|handle| handle.version);
        let new_integrated_handle = Self::apply_deltas_to_versioned(
            &mut self.state.integrated,
            &output_deltas,
            base_version,
        )
        .await
        .context("update group-by integrated state")?;
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

        let delta_handle = Self::apply_deltas_to_versioned(&mut self.output, &output_deltas, None)
            .await
            .context("persist group-by delta output")?;
        Ok(Some(delta_handle))
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

        let index = LegacyIndexedBatchZSet::new(table.clone(), "group_by_index");
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
        let index = LegacyIndexedBatchZSet::new(table.clone(), "group_by_recompute_index");

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
}
