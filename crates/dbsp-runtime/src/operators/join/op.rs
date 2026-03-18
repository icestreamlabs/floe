use std::collections::{BTreeMap, HashMap, hash_map::Entry};
use std::hash::Hash;
use std::sync::Arc;

use ahash::AHashMap;
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
use crate::stream::util::delta_zset_handle;

type JoinPredicate<L, R> = Arc<dyn Fn(&L, &R) -> bool + Send + Sync>;
type JoinProjector<L, R, O> = Arc<dyn Fn(&L, &R) -> O + Send + Sync>;
type JoinKeyExtractor<T, K> = Arc<dyn Fn(&T) -> Option<K> + Send + Sync>;
type FastHashMap<K, V> = AHashMap<K, V>;

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
    pub left_index: IndexedBatchZSet<K, L>,
    pub right_index: IndexedBatchZSet<K, R>,
    pub left_key: JoinKeyExtractor<L, K>,
    pub right_key: JoinKeyExtractor<R, K>,
    pub predicate: JoinPredicate<L, R>,
    pub projector: JoinProjector<L, R, O>,
    pub table: Arc<dyn KeyValueTable>,
    pub integrated: Option<RelationState<O>>,
    output: VersionedZSet<O>,
    dict_cache_left: HashMap<String, Arc<Dictionary<L>>>,
    dict_cache_right: HashMap<String, Arc<Dictionary<R>>>,
    left_memory_index: FastHashMap<K, FastHashMap<L, i64>>,
    right_memory_index: FastHashMap<K, FastHashMap<R, i64>>,
    persist_indexes: bool,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        left_state: RelationState<L>,
        right_state: RelationState<R>,
        left_index: IndexedBatchZSet<K, L>,
        right_index: IndexedBatchZSet<K, R>,
        left_key: JoinKeyExtractor<L, K>,
        right_key: JoinKeyExtractor<R, K>,
        predicate: JoinPredicate<L, R>,
        projector: JoinProjector<L, R, O>,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<O>,
        integrated: Option<RelationState<O>>,
    ) -> Self {
        debug_assert_eq!(left_index.engine_kind(), "indexed_batch");
        debug_assert_eq!(right_index.engine_kind(), "indexed_batch");
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
            left_memory_index: FastHashMap::new(),
            right_memory_index: FastHashMap::new(),
            persist_indexes: true,
        }
    }

    pub fn with_persist_indexes(mut self, persist_indexes: bool) -> Self {
        self.persist_indexes = persist_indexes;
        self
    }

    fn join_entries(&self, left: &[(L, i64)], right: &[(R, i64)], acc: &mut FastHashMap<O, i64>) {
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

    fn join_entries_with_right_map(
        &self,
        left: &[(L, i64)],
        right: Option<&FastHashMap<R, i64>>,
        acc: &mut FastHashMap<O, i64>,
    ) {
        let Some(right) = right else {
            return;
        };
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

    fn join_entries_with_left_map(
        &self,
        left: Option<&FastHashMap<L, i64>>,
        right: &[(R, i64)],
        acc: &mut FastHashMap<O, i64>,
    ) {
        let Some(left) = left else {
            return;
        };
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
        deltas: FastHashMap<T, i64>,
        extractor: &JoinKeyExtractor<T, K>,
    ) -> FastHashMap<K, Vec<(T, i64)>>
    where
        T: Eq + Hash,
    {
        let mut keyed = FastHashMap::new();
        for (row, weight) in deltas {
            if weight == 0 {
                continue;
            }
            if let Some(key) = extractor(&row) {
                keyed
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .push((row, weight));
            }
        }
        keyed
    }

    fn coalesce_deltas<T>(&self, deltas: Vec<(T, i64)>) -> FastHashMap<T, i64>
    where
        T: Eq + Hash,
    {
        let mut merged: FastHashMap<T, i64> = FastHashMap::new();
        for (row, weight) in deltas {
            if weight == 0 {
                continue;
            }
            match merged.entry(row) {
                Entry::Occupied(mut entry) => {
                    let next = entry.get().saturating_add(weight);
                    if next == 0 {
                        entry.remove();
                    } else {
                        *entry.get_mut() = next;
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(weight);
                }
            }
        }
        merged
    }

    async fn apply_deltas_to_versioned<T>(
        versioned: &mut VersionedZSet<T>,
        deltas: &FastHashMap<T, i64>,
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
            .context("batch intern keys while staging join delta")?;
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
            .context("schedule join version update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write join version update")?;

        versioned.apply_version_plan(&plan);
        metrics::observe_operator_persistence_latency_ms(
            "join",
            state_label,
            persist_start.elapsed().as_millis() as u64,
        );
        Ok(versioned.handle_for_version(plan.version))
    }

    fn apply_keyed_updates_to_memory_index<T>(
        index: &mut FastHashMap<K, FastHashMap<T, i64>>,
        keyed: &FastHashMap<K, Vec<(T, i64)>>,
    ) where
        T: Clone + Eq + Hash,
    {
        for (key, entries) in keyed {
            let should_remove_key = {
                let rows = index.entry(key.clone()).or_default();
                for (row, weight) in entries {
                    if *weight == 0 {
                        continue;
                    }
                    let next = rows.get(row).copied().unwrap_or(0).saturating_add(*weight);
                    if next == 0 {
                        rows.remove(row);
                    } else {
                        rows.insert(row.clone(), next);
                    }
                }
                rows.is_empty()
            };
            if should_remove_key {
                index.remove(key);
            }
        }
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

        let left_delta_values = delta_zset_handle::<L>(
            self.table.clone(),
            &mut self.dict_cache_left,
            &left_delta_handle,
        )
        .await
        .context("load left delta for join")?;
        let right_delta_values = delta_zset_handle::<R>(
            self.table.clone(),
            &mut self.dict_cache_right,
            &right_delta_handle,
        )
        .await
        .context("load right delta for join")?;
        let left_delta = self.coalesce_deltas(left_delta_values);
        let right_delta = self.coalesce_deltas(right_delta_values);
        let left_keyed = self.keyed_deltas(left_delta, &self.left_key);
        let right_keyed = self.keyed_deltas(right_delta, &self.right_key);

        // Build output delta from pre-update state (A, B) and current deltas
        // (ΔA, ΔB). State/index updates happen after this block to keep
        // each tick atomic.
        let mut delta_join: FastHashMap<O, i64> = FastHashMap::new();
        let has_left = !left_keyed.is_empty();
        let has_right = !right_keyed.is_empty();

        // ΔA ⋈ B
        if has_left {
            for (key, left_entries) in &left_keyed {
                if self.persist_indexes {
                    let right_entries = self
                        .right_index
                        .values_for_key(key)
                        .await
                        .context("load right join index")?;
                    self.join_entries(left_entries, &right_entries, &mut delta_join);
                } else {
                    self.join_entries_with_right_map(
                        left_entries,
                        self.right_memory_index.get(key),
                        &mut delta_join,
                    );
                }
            }
        }

        // A ⋈ ΔB
        if has_right {
            for (key, right_entries) in &right_keyed {
                if self.persist_indexes {
                    let left_entries = self
                        .left_index
                        .values_for_key(key)
                        .await
                        .context("load left join index")?;
                    self.join_entries(&left_entries, right_entries, &mut delta_join);
                } else {
                    self.join_entries_with_left_map(
                        self.left_memory_index.get(key),
                        right_entries,
                        &mut delta_join,
                    );
                }
            }
        }

        // ΔA ⋈ ΔB
        if has_left && has_right {
            for (key, left_entries) in &left_keyed {
                if let Some(right_entries) = right_keyed.get(key) {
                    self.join_entries(left_entries, right_entries, &mut delta_join);
                }
            }
        }
        delta_join.retain(|_, w| *w != 0);

        if !self.persist_indexes {
            Self::apply_keyed_updates_to_memory_index(&mut self.left_memory_index, &left_keyed);
            Self::apply_keyed_updates_to_memory_index(&mut self.right_memory_index, &right_keyed);
        }

        if self.persist_indexes {
            let mut left_updates = Vec::new();
            for (key, entries) in &left_keyed {
                for (row, weight) in entries {
                    left_updates.push((key.clone(), row.clone(), *weight));
                }
            }
            let left_index_persist_start = std::time::Instant::now();
            self.left_index
                .apply_deltas(left_updates)
                .await
                .context("update left join index")?;
            metrics::observe_operator_persistence_latency_ms(
                "join",
                "left_index",
                left_index_persist_start.elapsed().as_millis() as u64,
            );
        }

        if self.persist_indexes {
            let mut right_updates = Vec::new();
            for (key, entries) in &right_keyed {
                for (row, weight) in entries {
                    right_updates.push((key.clone(), row.clone(), *weight));
                }
            }
            let right_index_persist_start = std::time::Instant::now();
            self.right_index
                .apply_deltas(right_updates)
                .await
                .context("update right join index")?;
            metrics::observe_operator_persistence_latency_ms(
                "join",
                "right_index",
                right_index_persist_start.elapsed().as_millis() as u64,
            );
        }

        if delta_join.is_empty() {
            return Ok(None);
        }

        if let Some(integrated) = &mut self.integrated {
            let base = integrated
                .integrated
                .current_handle()
                .map(|handle| handle.version);
            let new_integrated_handle = Self::apply_deltas_to_versioned(
                &mut integrated.integrated,
                &delta_join,
                base,
                "integrated_output",
            )
            .await
            .context("update integrated join state")?;
            integrated.update_handle(new_integrated_handle);
        }

        let delta_handle =
            Self::apply_deltas_to_versioned(&mut self.output, &delta_join, None, "output")
                .await
                .context("persist join delta output")?;
        Ok(Some(delta_handle))
    }
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}
