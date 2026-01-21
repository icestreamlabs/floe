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
