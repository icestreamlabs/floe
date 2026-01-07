use std::collections::{BTreeMap, HashMap};
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
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::materialize_zset_handle;

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
        Self {
            predicate,
            state,
            table,
            output,
            dict_cache: HashMap::new(),
        }
    }

    async fn apply_deltas_to_versioned(
        versioned: &mut VersionedZSet<K>,
        deltas: &HashMap<K, i64>,
        base: Option<u64>,
    ) -> Result<ZSetHandle> {
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
                .context("intern key while staging filter delta")?;
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
            if let Some(handle) = versioned.current_handle() {
                return Ok(handle);
            }
            return Ok(versioned.handle_for_version(0));
        }

        let base = base.or_else(|| versioned.current_handle().map(|h| h.version));
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

        let mut cleanup = WriteBatch::new();
        cleanup.delete(versioned.intent_key_bytes().to_vec());
        versioned
            .table()
            .write_batch(cleanup)
            .await
            .context("clear filter intent")?;

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
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let delta_handle = inputs
            .first()
            .cloned()
            .context("filter operator requires one input delta handle")?;
        let delta_map =
            materialize_zset_handle::<K>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("materialize input delta for filter")?;

        let mut filtered: HashMap<K, i64> = HashMap::new();
        for (key, weight) in delta_map {
            if (self.predicate)(&key) {
                let entry = filtered.entry(key.clone()).or_insert(0);
                *entry += weight;
                if *entry == 0 {
                    filtered.remove(&key);
                }
            }
        }

        if filtered.is_empty() {
            return Ok(None);
        }

        let base_version = self
            .state
            .integrated
            .current_handle()
            .map(|handle| handle.version);
        let new_integrated_handle =
            Self::apply_deltas_to_versioned(&mut self.state.integrated, &filtered, base_version)
                .await
                .context("update integrated filter state")?;
        self.state.update_handle(new_integrated_handle);

        let delta_handle = Self::apply_deltas_to_versioned(&mut self.output, &filtered, None)
            .await
            .context("persist filter delta output")?;
        Ok(Some(delta_handle))
    }
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::zset::SegmentRecord;
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
        let delta_out2 = crate::stream::util::compute_delta(&out1_materialized, &out2_materialized);
        let delta_out2: HashMap<_, _> = delta_out2.into_iter().collect();
        let integrated_after_t2 = op
            .state
            .integrated
            .materialize()
            .await
            .expect("integrated t2");

        let mut expected_out2 = HashMap::new();
        expected_out2.insert(2, -2);
        expected_out2.insert(4, 3);
        assert_eq!(delta_out2, expected_out2);
        assert_eq!(integrated_after_t2.get(&2), None);
        assert_eq!(integrated_after_t2.get(&4), Some(&3));
    }
}
