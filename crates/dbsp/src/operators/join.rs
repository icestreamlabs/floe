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

use crate::collections::IndexedZSet;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::materialize_zset_handle;

type JoinPredicate<L, R> = Arc<dyn Fn(&L, &R) -> bool + Send + Sync>;
type JoinProjector<L, R, O> = Arc<dyn Fn(&L, &R) -> O + Send + Sync>;
type JoinKeyExtractor<T, K> = Arc<dyn Fn(&T) -> Option<K> + Send + Sync>;

pub struct JoinOp<L, R, O, K>
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
    pub left_state: RelationState<L>,
    pub right_state: RelationState<R>,
    pub left_index: IndexedZSet<K, L>,
    pub right_index: IndexedZSet<K, R>,
    pub left_key: JoinKeyExtractor<L, K>,
    pub right_key: JoinKeyExtractor<R, K>,
    pub predicate: JoinPredicate<L, R>,
    pub projector: JoinProjector<L, R, O>,
    pub table: Arc<dyn KeyValueTable>,
    pub integrated: Option<RelationState<O>>,
    output: VersionedZSet<O>,
    dict_cache_left: HashMap<String, Arc<Dictionary<L>>>,
    dict_cache_right: HashMap<String, Arc<Dictionary<R>>>,
}

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
    pub fn new(
        left_state: RelationState<L>,
        right_state: RelationState<R>,
        left_index: IndexedZSet<K, L>,
        right_index: IndexedZSet<K, R>,
        left_key: JoinKeyExtractor<L, K>,
        right_key: JoinKeyExtractor<R, K>,
        predicate: JoinPredicate<L, R>,
        projector: JoinProjector<L, R, O>,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<O>,
        integrated: Option<RelationState<O>>,
    ) -> Self {
        Self {
            left_state,
            right_state,
            left_index,
            right_index,
            left_key,
            right_key,
            predicate,
            projector,
            table,
            integrated,
            output,
            dict_cache_left: HashMap::new(),
            dict_cache_right: HashMap::new(),
        }
    }

    fn join_entries(
        &self,
        left: &[(L, i64)],
        right: &[(R, i64)],
        acc: &mut HashMap<O, i64>,
    ) {
        for (lk, lw) in left {
            if *lw == 0 {
                continue;
            }
            for (rk, rw) in right {
                if *rw == 0 {
                    continue;
                }
                if (self.predicate)(lk, rk) {
                    let out = (self.projector)(lk, rk);
                    *acc.entry(out).or_insert(0) += lw * rw;
                }
            }
        }
    }

    fn keyed_deltas<T>(
        &self,
        deltas: &HashMap<T, i64>,
        extractor: &JoinKeyExtractor<T, K>,
    ) -> HashMap<K, Vec<(T, i64)>>
    where
        T: Clone,
    {
        let mut keyed = HashMap::new();
        for (row, weight) in deltas {
            if *weight == 0 {
                continue;
            }
            if let Some(key) = extractor(row) {
                keyed
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .push((row.clone(), *weight));
            }
        }
        keyed
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
                .context("intern key while staging join delta")?;
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
            if base.is_some() {
                if let Some(handle) = versioned.current_handle() {
                    return Ok(handle);
                }
            }
            return Ok(versioned.handle_for_version(0));
        }

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

        let mut cleanup = WriteBatch::new();
        cleanup.delete(versioned.intent_key_bytes());
        versioned
            .table()
            .write_batch(cleanup)
            .await
            .context("clear join intent")?;

        versioned.apply_version_plan(&plan);
        Ok(versioned.handle_for_version(plan.version))
    }
}

#[async_trait]
impl<L, R, O, K> DeltaOperator for JoinOp<L, R, O, K>
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
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let left_delta_handle = inputs
            .first()
            .cloned()
            .context("join operator requires left delta handle")?;
        let right_delta_handle = inputs
            .get(1)
            .cloned()
            .context("join operator requires right delta handle")?;

        let left_delta = materialize_zset_handle::<L>(
            self.table.clone(),
            &mut self.dict_cache_left,
            &left_delta_handle,
        )
        .await
        .context("materialize left delta for join")?;
        let right_delta = materialize_zset_handle::<R>(
            self.table.clone(),
            &mut self.dict_cache_right,
            &right_delta_handle,
        )
        .await
        .context("materialize right delta for join")?;
        let left_keyed = self.keyed_deltas(&left_delta, &self.left_key);
        let right_keyed = self.keyed_deltas(&right_delta, &self.right_key);

        let mut delta_join: HashMap<O, i64> = HashMap::new();
        for (key, left_entries) in &left_keyed {
            let right_entries = self
                .right_index
                .values_for_key(key)
                .await
                .context("load right join index")?;
            self.join_entries(left_entries, &right_entries, &mut delta_join);
        }
        for (key, right_entries) in &right_keyed {
            let left_entries = self
                .left_index
                .values_for_key(key)
                .await
                .context("load left join index")?;
            self.join_entries(&left_entries, right_entries, &mut delta_join);
        }
        for (key, left_entries) in &left_keyed {
            if let Some(right_entries) = right_keyed.get(key) {
                self.join_entries(left_entries, right_entries, &mut delta_join);
            }
        }
        delta_join.retain(|_, w| *w != 0);

        let left_base = self
            .left_state
            .integrated
            .current_handle()
            .map(|handle| handle.version);
        let new_left_handle = Self::apply_deltas_to_versioned(
            &mut self.left_state.integrated,
            &left_delta,
            left_base,
        )
        .await
        .context("update left integrated state")?;
        self.left_state.update_handle(new_left_handle);

        let right_base = self
            .right_state
            .integrated
            .current_handle()
            .map(|handle| handle.version);
        let new_right_handle = Self::apply_deltas_to_versioned(
            &mut self.right_state.integrated,
            &right_delta,
            right_base,
        )
        .await
        .context("update right integrated state")?;
        self.right_state.update_handle(new_right_handle);

        let mut left_updates = Vec::new();
        for (key, entries) in &left_keyed {
            for (row, weight) in entries {
                left_updates.push((key.clone(), row.clone(), *weight));
            }
        }
        self.left_index
            .apply_deltas(left_updates)
            .await
            .context("update left join index")?;

        let mut right_updates = Vec::new();
        for (key, entries) in &right_keyed {
            for (row, weight) in entries {
                right_updates.push((key.clone(), row.clone(), *weight));
            }
        }
        self.right_index
            .apply_deltas(right_updates)
            .await
            .context("update right join index")?;

        if delta_join.is_empty() {
            return Ok(None);
        }

        if let Some(integrated) = &mut self.integrated {
            let base = integrated
                .integrated
                .current_handle()
                .map(|handle| handle.version);
            let new_integrated_handle =
                Self::apply_deltas_to_versioned(&mut integrated.integrated, &delta_join, base)
                    .await
                    .context("update integrated join state")?;
            integrated.update_handle(new_integrated_handle);
        }

        let delta_handle = Self::apply_deltas_to_versioned(&mut self.output, &delta_join, None)
            .await
            .context("persist join delta output")?;
        Ok(Some(delta_handle))
    }
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::IndexedZSet;
    use crate::collections::zset::SegmentRecord;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open("joinop", store).await.expect("open SlateDB"))
    }

    fn bucket_for(id: u64) -> u16 {
        (id >> 48) as u16
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
            let id = dict_batch.intern(key).await.expect("intern key for join");
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

    fn project_sum(l: &i64, r: &i64) -> i64 {
        l + r
    }

    #[tokio::test]
    async fn join_operator_matches_batch_join_over_time() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let left_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "join_left_stream", None)
                .await
                .expect("left dict"),
        );
        let right_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "join_right_stream", None)
                .await
                .expect("right dict"),
        );
        let left_state_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "join_left_state", None)
                .await
                .expect("left state dict"),
        );
        let right_state_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "join_right_state", None)
                .await
                .expect("right state dict"),
        );
        let out_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "join_output", None)
                .await
                .expect("out dict"),
        );
        let integrated_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "join_integrated", None)
                .await
                .expect("join integrated dict"),
        );

        let left_state = RelationState {
            integrated: VersionedZSet::new(
                left_state_dict.clone(),
                table.clone(),
                "join_left_state".to_string(),
            )
            .await
            .expect("left integrated"),
            latest_handle: ZSetHandle {
                ns: "join_left_state".to_string(),
                version: 0,
            },
        };
        let right_state = RelationState {
            integrated: VersionedZSet::new(
                right_state_dict.clone(),
                table.clone(),
                "join_right_state".to_string(),
            )
            .await
            .expect("right integrated"),
            latest_handle: ZSetHandle {
                ns: "join_right_state".to_string(),
                version: 0,
            },
        };
        let output = VersionedZSet::new(out_dict.clone(), table.clone(), "join_output".to_string())
            .await
            .expect("output");
        let match_sum = Arc::new(|l: &i64, r: &i64| *l == *r);
        let projector = Arc::new(project_sum);
        let left_index = IndexedZSet::new(table.clone(), "join_left_index");
        let right_index = IndexedZSet::new(table.clone(), "join_right_index");
        let left_key = Arc::new(|value: &i64| Some(*value));
        let right_key = Arc::new(|value: &i64| Some(*value));
        let integrated_join = RelationState {
            integrated: VersionedZSet::new(
                integrated_dict.clone(),
                table.clone(),
                "join_integrated".to_string(),
            )
            .await
            .expect("join integrated"),
            latest_handle: ZSetHandle {
                ns: "join_integrated".to_string(),
                version: 0,
            },
        };

        let mut op = JoinOp::new(
            left_state,
            right_state,
            left_index,
            right_index,
            left_key,
            right_key,
            match_sum,
            projector,
            table.clone(),
            output,
            Some(integrated_join),
        );

        let mut full_left: HashMap<i64, i64> = HashMap::new();
        let mut full_right: HashMap<i64, i64> = HashMap::new();

        // t1
        let left_delta1 = stage_version(
            left_dict.clone(),
            table.clone(),
            "join_left_stream",
            &[(1, 1)],
        )
        .await;
        let right_delta1 = stage_version(
            right_dict.clone(),
            table.clone(),
            "join_right_stream",
            &[(1, 2)],
        )
        .await;
        full_left.insert(1, 1);
        full_right.insert(1, 2);
        let out1 = op
            .on_step(1, &[left_delta1, right_delta1])
            .await
            .expect("run join t1")
            .expect("non-empty t1");

        let mut cache = HashMap::new();
        cache.insert("join_output".to_string(), out_dict.clone());
        let out1_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out1)
            .await
            .expect("materialize t1 output");
        assert_eq!(out1_materialized, HashMap::from([(2, 2)]));
        let integrated_t1 = op
            .integrated
            .as_ref()
            .unwrap()
            .integrated
            .materialize()
            .await
            .expect("integrated t1");
        assert_eq!(integrated_t1.get(&2), Some(&2));

        // t2: add additional matches/mismatches
        let left_delta2 = stage_version(
            left_dict.clone(),
            table.clone(),
            "join_left_stream",
            &[(2, 1)],
        )
        .await;
        let right_delta2 = stage_version(
            right_dict.clone(),
            table.clone(),
            "join_right_stream",
            &[(2, 3)],
        )
        .await;
        full_left.insert(2, 1);
        full_right.insert(2, 3);
        let out2 = op
            .on_step(2, &[left_delta2, right_delta2])
            .await
            .expect("run join t2")
            .expect("non-empty t2");
        let out2_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out2)
            .await
            .expect("materialize t2 output");

        // Expected joins: (1,1) persists, (2,2) => 4, (1,2) none
        assert_eq!(out2_materialized, HashMap::from([(4, 3)]));

        let mut expected_full_join: HashMap<i64, i64> = HashMap::new();
        for (lk, lw) in &full_left {
            for (rk, rw) in &full_right {
                if lk == rk {
                    *expected_full_join.entry(lk + rk).or_insert(0) += lw * rw;
                }
            }
        }
        expected_full_join.retain(|_, w| *w != 0);
        let integrated_t2 = op
            .integrated
            .as_ref()
            .unwrap()
            .integrated
            .materialize()
            .await
            .expect("integrated t2");
        assert_eq!(integrated_t2, expected_full_join);
    }

    #[tokio::test]
    async fn join_operator_handles_negative_deltas() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let left_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "neg_left_stream", None)
                .await
                .expect("left dict"),
        );
        let right_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "neg_right_stream", None)
                .await
                .expect("right dict"),
        );
        let left_state_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "neg_left_state", None)
                .await
                .expect("left state dict"),
        );
        let right_state_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "neg_right_state", None)
                .await
                .expect("right state dict"),
        );
        let out_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "neg_output", None)
                .await
                .expect("out dict"),
        );
        let integrated_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "neg_integrated", None)
                .await
                .expect("integrated dict"),
        );

        let left_state = RelationState {
            integrated: VersionedZSet::new(
                left_state_dict.clone(),
                table.clone(),
                "neg_left_state".to_string(),
            )
            .await
            .expect("left integrated"),
            latest_handle: ZSetHandle {
                ns: "neg_left_state".to_string(),
                version: 0,
            },
        };
        let right_state = RelationState {
            integrated: VersionedZSet::new(
                right_state_dict.clone(),
                table.clone(),
                "neg_right_state".to_string(),
            )
            .await
            .expect("right integrated"),
            latest_handle: ZSetHandle {
                ns: "neg_right_state".to_string(),
                version: 0,
            },
        };
        let output = VersionedZSet::new(out_dict.clone(), table.clone(), "neg_output".to_string())
            .await
            .expect("output");
        let integrated_join = RelationState {
            integrated: VersionedZSet::new(
                integrated_dict.clone(),
                table.clone(),
                "neg_integrated".to_string(),
            )
            .await
            .expect("integrated join"),
            latest_handle: ZSetHandle {
                ns: "neg_integrated".to_string(),
                version: 0,
            },
        };
        let left_index = IndexedZSet::new(table.clone(), "neg_left_index");
        let right_index = IndexedZSet::new(table.clone(), "neg_right_index");
        let left_key = Arc::new(|value: &i64| Some(*value));
        let right_key = Arc::new(|value: &i64| Some(*value));

        let mut op = JoinOp::new(
            left_state,
            right_state,
            left_index,
            right_index,
            left_key,
            right_key,
            Arc::new(|l: &i64, r: &i64| l == r),
            Arc::new(project_sum),
            table.clone(),
            output,
            Some(integrated_join),
        );

        let left_delta1 = stage_version(
            left_dict.clone(),
            table.clone(),
            "neg_left_stream",
            &[(1, 2)],
        )
        .await;
        let right_delta1 = stage_version(
            right_dict.clone(),
            table.clone(),
            "neg_right_stream",
            &[(1, 3)],
        )
        .await;
        let out1 = op
            .on_step(1, &[left_delta1, right_delta1])
            .await
            .expect("run join t1")
            .expect("non-empty t1");

        let mut cache = HashMap::new();
        cache.insert("neg_output".to_string(), out_dict.clone());
        let out1_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out1)
            .await
            .expect("materialize t1 output");
        assert_eq!(out1_materialized, HashMap::from([(2, 6)]));

        let left_delta2 = stage_version(
            left_dict.clone(),
            table.clone(),
            "neg_left_stream",
            &[(1, -1)],
        )
        .await;
        let right_empty = ZSetHandle {
            ns: "neg_right_stream".to_string(),
            version: 0,
        };
        let out2 = op
            .on_step(2, &[left_delta2, right_empty])
            .await
            .expect("run join t2")
            .expect("non-empty t2");
        let out2_materialized = materialize_zset_handle::<i64>(table.clone(), &mut cache, &out2)
            .await
            .expect("materialize t2 output");
        assert_eq!(out2_materialized, HashMap::from([(2, -3)]));

        let integrated_t2 = op
            .integrated
            .as_ref()
            .unwrap()
            .integrated
            .materialize()
            .await
            .expect("integrated t2");
        assert_eq!(integrated_t2, HashMap::from([(2, 3)]));
    }

    #[tokio::test]
    async fn join_operator_skips_null_keys() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));
        let left_dict = Arc::new(
            Dictionary::<Option<i64>>::with_table(table.clone(), "null_left_stream", None)
                .await
                .expect("left dict"),
        );
        let right_dict = Arc::new(
            Dictionary::<Option<i64>>::with_table(table.clone(), "null_right_stream", None)
                .await
                .expect("right dict"),
        );
        let left_state_dict = Arc::new(
            Dictionary::<Option<i64>>::with_table(table.clone(), "null_left_state", None)
                .await
                .expect("left state dict"),
        );
        let right_state_dict = Arc::new(
            Dictionary::<Option<i64>>::with_table(table.clone(), "null_right_state", None)
                .await
                .expect("right state dict"),
        );
        let out_dict = Arc::new(
            Dictionary::<i64>::with_table(table.clone(), "null_output", None)
                .await
                .expect("out dict"),
        );

        let left_state = RelationState {
            integrated: VersionedZSet::new(
                left_state_dict.clone(),
                table.clone(),
                "null_left_state".to_string(),
            )
            .await
            .expect("left integrated"),
            latest_handle: ZSetHandle {
                ns: "null_left_state".to_string(),
                version: 0,
            },
        };
        let right_state = RelationState {
            integrated: VersionedZSet::new(
                right_state_dict.clone(),
                table.clone(),
                "null_right_state".to_string(),
            )
            .await
            .expect("right integrated"),
            latest_handle: ZSetHandle {
                ns: "null_right_state".to_string(),
                version: 0,
            },
        };
        let output = VersionedZSet::new(out_dict.clone(), table.clone(), "null_output".to_string())
            .await
            .expect("output");
        let left_index = IndexedZSet::new(table.clone(), "null_left_index");
        let right_index = IndexedZSet::new(table.clone(), "null_right_index");
        let left_key = Arc::new(|value: &Option<i64>| value.clone());
        let right_key = Arc::new(|value: &Option<i64>| value.clone());

        let mut op = JoinOp::new(
            left_state,
            right_state,
            left_index,
            right_index,
            left_key,
            right_key,
            Arc::new(|l: &Option<i64>, r: &Option<i64>| matches!((l, r), (Some(a), Some(b)) if a == b)),
            Arc::new(|l: &Option<i64>, r: &Option<i64>| l.unwrap_or(0) + r.unwrap_or(0)),
            table.clone(),
            output,
            None,
        );

        let left_delta = stage_version(
            left_dict.clone(),
            table.clone(),
            "null_left_stream",
            &[(Some(1), 1), (None, 1)],
        )
        .await;
        let right_delta = stage_version(
            right_dict.clone(),
            table.clone(),
            "null_right_stream",
            &[(Some(1), 1), (None, 1)],
        )
        .await;
        let out = op
            .on_step(1, &[left_delta, right_delta])
            .await
            .expect("run join")
            .expect("non-empty join");

        let mut cache = HashMap::new();
        cache.insert("null_output".to_string(), out_dict.clone());
        let out_materialized =
            materialize_zset_handle::<i64>(table.clone(), &mut cache, &out)
                .await
                .expect("materialize join output");
        assert_eq!(out_materialized, HashMap::from([(2, 1)]));
    }
}
