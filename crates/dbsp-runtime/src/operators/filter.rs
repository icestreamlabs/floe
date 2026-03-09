use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;

use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::delta_zset_handle;

pub struct FilterOp<K>
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
{
    pub predicate: Arc<dyn Fn(&K) -> bool + Send + Sync>,
    pub state: RelationState<K>,
    pub table: Arc<dyn KeyValueTable>,
    output: VersionedZSet<K>,
    persist_integrated_state: bool,
    dict_cache: HashMap<String, Arc<Dictionary<K>>>,
}

impl<K> FilterOp<K>
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
{
    pub fn new(
        predicate: Arc<dyn Fn(&K) -> bool + Send + Sync>,
        state: RelationState<K>,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<K>,
    ) -> Self {
        Self::new_with_integrated_state(predicate, state, table, output, true)
    }

    pub fn new_without_integrated_state(
        predicate: Arc<dyn Fn(&K) -> bool + Send + Sync>,
        state: RelationState<K>,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<K>,
    ) -> Self {
        Self::new_with_integrated_state(predicate, state, table, output, false)
    }

    fn new_with_integrated_state(
        predicate: Arc<dyn Fn(&K) -> bool + Send + Sync>,
        state: RelationState<K>,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<K>,
        persist_integrated_state: bool,
    ) -> Self {
        Self {
            predicate,
            state,
            table,
            output,
            persist_integrated_state,
            dict_cache: HashMap::new(),
        }
    }

    async fn apply_deltas_to_versioned(
        versioned: &mut VersionedZSet<K>,
        deltas: &HashMap<K, i64>,
        base: Option<u64>,
    ) -> Result<ZSetHandle> {
        let staged: Vec<(&K, i64)> = deltas
            .iter()
            .filter_map(|(key, delta)| (*delta != 0).then_some((key, *delta)))
            .collect();
        if staged.is_empty() {
            if base.is_some()
                && let Some(handle) = versioned.current_handle()
            {
                return Ok(handle);
            }
            return Ok(versioned.handle_for_version(0));
        }

        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let dict = versioned.dictionary();
        let ids = dict
            .intern_many_values(staged.iter().map(|(key, _)| *key))
            .await
            .context("intern keys while staging filter delta")?;
        for ((_, delta), id) in staged.iter().zip(ids.into_iter()) {
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

        let mut batch = WriteBatch::new();
        let plan = versioned
            .enqueue_version_with_base(segments, base, 0, &mut batch)
            .await
            .context("schedule filter version update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write filtered version update")?;

        versioned.apply_version_plan(&plan);
        Ok(versioned.handle_for_version(plan.version))
    }
}

#[async_trait]
impl<K> DeltaOperator for FilterOp<K>
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
{
    async fn on_step(
        &mut self,
        ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let total_start = Instant::now();
        let input_handle = inputs
            .first()
            .cloned()
            .context("filter operator requires one input delta handle")?;
        let load_start = Instant::now();
        let delta_values =
            delta_zset_handle::<K>(self.table.clone(), &mut self.dict_cache, &input_handle)
                .await
                .context("load input delta for filter")?;
        let input_delta_rows = delta_values.len();
        let load_ms = load_start.elapsed().as_millis() as u64;

        let filter_start = Instant::now();
        let mut filtered: HashMap<K, i64> = HashMap::new();
        for (key, weight) in delta_values {
            if (self.predicate)(&key) {
                let entry = filtered.entry(key.clone()).or_insert(0);
                *entry += weight;
                if *entry == 0 {
                    filtered.remove(&key);
                }
            }
        }
        let filter_ms = filter_start.elapsed().as_millis() as u64;
        let output_delta_rows = filtered.len();

        if filtered.is_empty() {
            tracing::debug!(
                ts,
                input_ns = %input_handle.ns,
                input_version = input_handle.version,
                input_delta_rows,
                output_delta_rows,
                load_ms,
                filter_ms,
                total_ms = total_start.elapsed().as_millis() as u64,
                "filter operator timing (no output)"
            );
            return Ok(None);
        }

        let base_version = self
            .state
            .integrated
            .current_handle()
            .map(|handle| handle.version);
        let (integrated_apply_ms, integrated_version) = if self.persist_integrated_state {
            let integrated_apply_start = Instant::now();
            let new_integrated_handle = Self::apply_deltas_to_versioned(
                &mut self.state.integrated,
                &filtered,
                base_version,
            )
            .await
            .context("update integrated filter state")?;
            let integrated_apply_ms = integrated_apply_start.elapsed().as_millis() as u64;
            let integrated_version = new_integrated_handle.version;
            self.state.update_handle(new_integrated_handle);
            (integrated_apply_ms, Some(integrated_version))
        } else {
            (0, None)
        };

        let output_apply_start = Instant::now();
        let output_handle = Self::apply_deltas_to_versioned(&mut self.output, &filtered, None)
            .await
            .context("persist filter delta output")?;
        let output_apply_ms = output_apply_start.elapsed().as_millis() as u64;
        tracing::debug!(
            ts,
            input_ns = %input_handle.ns,
            input_version = input_handle.version,
            input_delta_rows,
            output_delta_rows,
            base_version = ?base_version,
            integrated_version = ?integrated_version,
            persist_integrated_state = self.persist_integrated_state,
            output_ns = %self.output.namespace(),
            output_version = output_handle.version,
            load_ms,
            filter_ms,
            integrated_apply_ms,
            output_apply_ms,
            total_ms = total_start.elapsed().as_millis() as u64,
            "filter operator timing"
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
    use crate::stream::util::{compute_delta, materialize_zset_handle};
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::BTreeMap;
    use std::sync::Arc;

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
            let id = dict_batch.intern(key).await.expect("intern key for filter");
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
        Arc::new(Db::open("filterop", store).await.expect("open SlateDB"))
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
    async fn filter_operator_passes_matching_deltas() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "filter_input", None)
                .await
                .expect("build input dictionary"),
        );
        let integrated_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "filter_integrated", None)
                .await
                .expect("build integrated dictionary"),
        );
        let output_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "filter_output", None)
                .await
                .expect("build output dictionary"),
        );

        let integrated = VersionedZSet::new(
            integrated_dict.clone(),
            table.clone(),
            "filter_integrated".to_string(),
        )
        .await
        .expect("integrated");
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "filter_output".to_string(),
        )
        .await
        .expect("output");

        let state = RelationState {
            integrated,
            latest_handle: ZSetHandle {
                ns: "filter_integrated".to_string(),
                version: 0,
            },
        };

        let predicate = Arc::new(|k: &i64| *k % 2 == 0);
        let mut op = FilterOp::new(predicate, state, table.clone(), output);

        let first_delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "filter_input",
            &[(1, 1), (2, 2)],
        )
        .await;
        let out1 = op
            .on_step(1, &[first_delta])
            .await
            .expect("run filter t1")
            .expect("non-empty t1");

        let mut cache = HashMap::new();
        cache.insert("filter_output".to_string(), output_dict.clone());
        let out1_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out1)
            .await
            .expect("materialize t1 output");
        let integrated_after_t1 = op
            .state
            .integrated
            .materialize()
            .await
            .expect("integrated t1");

        assert_eq!(out1_materialized.get(&2), Some(&2));
        assert_eq!(out1_materialized.get(&1), None);
        assert_eq!(integrated_after_t1.get(&2), Some(&2));
        assert_eq!(integrated_after_t1.get(&1), None);

        let second_delta = stage_version(
            input_dict.clone(),
            table.clone(),
            "filter_input",
            &[(2, -2), (4, 3)],
        )
        .await;
        let out2 = op
            .on_step(2, &[second_delta])
            .await
            .expect("run filter t2")
            .expect("non-empty t2");

        let out2_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out2)
            .await
            .expect("materialize t2 output");
        let integrated_after_t2 = op
            .state
            .integrated
            .materialize()
            .await
            .expect("integrated t2");

        let mut expected_out2 = HashMap::new();
        expected_out2.insert(2, -2);
        expected_out2.insert(4, 3);
        assert_eq!(out2_materialized, expected_out2);
        assert_eq!(integrated_after_t2.get(&2), None);
        assert_eq!(integrated_after_t2.get(&4), Some(&3));
    }

    #[tokio::test]
    async fn filter_operator_matches_full_recompute() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "filter_recompute_input", None)
                .await
                .expect("build input dictionary"),
        );
        let integrated_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "filter_recompute_integrated", None)
                .await
                .expect("build integrated dictionary"),
        );
        let output_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "filter_recompute_output", None)
                .await
                .expect("build output dictionary"),
        );

        let integrated = VersionedZSet::new(
            integrated_dict.clone(),
            table.clone(),
            "filter_recompute_integrated".to_string(),
        )
        .await
        .expect("integrated");
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "filter_recompute_output".to_string(),
        )
        .await
        .expect("output");

        let state = RelationState {
            integrated,
            latest_handle: ZSetHandle {
                ns: "filter_recompute_integrated".to_string(),
                version: 0,
            },
        };

        let predicate = Arc::new(|k: &i64| *k % 2 == 0);
        let mut op = FilterOp::new(predicate, state, table.clone(), output);

        let steps = vec![vec![(1, 1), (2, 2)], vec![(2, -2), (4, 3)]];
        let mut full_input: HashMap<i64, i64> = HashMap::new();
        let mut full_output: HashMap<i64, i64> = HashMap::new();

        for (idx, deltas) in steps.into_iter().enumerate() {
            let delta_handle = stage_version(
                input_dict.clone(),
                table.clone(),
                "filter_recompute_input",
                &deltas,
            )
            .await;
            let output_handle = op
                .on_step(idx as i64 + 1, &[delta_handle])
                .await
                .expect("run filter step");

            apply_deltas(&mut full_input, &deltas);
            let mut recompute = HashMap::new();
            for (key, weight) in &full_input {
                if *key % 2 == 0 {
                    recompute.insert(*key, *weight);
                }
            }
            recompute.retain(|_, weight| *weight != 0);

            let expected_delta_vec = compute_delta(&full_output, &recompute);
            let expected_delta: HashMap<i64, i64> = expected_delta_vec.into_iter().collect();

            if let Some(handle) = output_handle {
                let mut cache = HashMap::new();
                cache.insert("filter_recompute_output".to_string(), output_dict.clone());
                let actual_delta =
                    materialize_zset_handle::<i64>(table.clone(), &mut cache, &handle)
                        .await
                        .expect("materialize filter output");
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
