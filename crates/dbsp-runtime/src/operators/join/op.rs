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
use crate::stream::util::{delta_zset_handle_batch, publish_transient_zset_batch};

type JoinPredicate<L, R> = Arc<dyn Fn(&L, &R) -> bool + Send + Sync>;
type JoinProjector<L, R, O> = Arc<dyn Fn(&L, &R) -> O + Send + Sync>;
type JoinKeyExtractor<T, K> = Arc<dyn Fn(&T) -> Option<K> + Send + Sync>;
type BatchJoinKeyExtractor<T, K> = Arc<dyn Fn(&[(T, i64)]) -> Vec<(K, T, i64)> + Send + Sync>;
type FastHashMap<K, V> = AHashMap<K, V>;
type KeyedRowDeltas<K, T> = FastHashMap<K, FastHashMap<T, i64>>;

pub(crate) struct JoinStepResult<O> {
    pub(crate) delta_batch: Arc<Vec<(O, i64)>>,
    pub(crate) persisted_handle: Option<ZSetHandle>,
}

pub struct JoinTransientInputs<L, R> {
    pub(crate) left: Option<Arc<Vec<(L, i64)>>>,
    pub(crate) right: Option<Arc<Vec<(R, i64)>>>,
}

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
    pub left_key: BatchJoinKeyExtractor<L, K>,
    pub right_key: BatchJoinKeyExtractor<R, K>,
    pub predicate: JoinPredicate<L, R>,
    pub projector: JoinProjector<L, R, O>,
    pub table: Arc<dyn KeyValueTable>,
    pub integrated: Option<RelationState<O>>,
    output: Option<VersionedZSet<O>>,
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
        let left_key = Arc::new(move |deltas: &[(L, i64)]| {
            deltas
                .iter()
                .filter_map(|(row, weight)| left_key(row).map(|key| (key, row.clone(), *weight)))
                .collect()
        });
        let right_key = Arc::new(move |deltas: &[(R, i64)]| {
            deltas
                .iter()
                .filter_map(|(row, weight)| right_key(row).map(|key| (key, row.clone(), *weight)))
                .collect()
        });
        Self::new_batch(
            left_state,
            right_state,
            left_index,
            right_index,
            left_key,
            right_key,
            predicate,
            projector,
            table,
            output,
            integrated,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_batch(
        left_state: RelationState<L>,
        right_state: RelationState<R>,
        left_index: IndexedBatchZSet<K, L>,
        right_index: IndexedBatchZSet<K, R>,
        left_key: BatchJoinKeyExtractor<L, K>,
        right_key: BatchJoinKeyExtractor<R, K>,
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
            output: Some(output),
            dict_cache_left: HashMap::new(),
            dict_cache_right: HashMap::new(),
            left_memory_index: FastHashMap::new(),
            right_memory_index: FastHashMap::new(),
            persist_indexes: true,
        }
    }

    pub fn enable_live_output_replayable(&mut self) {
        if let Some(output) = self.output.as_mut() {
            output.enable_replayable_persistence();
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_without_output(
        left_state: RelationState<L>,
        right_state: RelationState<R>,
        left_index: IndexedBatchZSet<K, L>,
        right_index: IndexedBatchZSet<K, R>,
        left_key: JoinKeyExtractor<L, K>,
        right_key: JoinKeyExtractor<R, K>,
        predicate: JoinPredicate<L, R>,
        projector: JoinProjector<L, R, O>,
        table: Arc<dyn KeyValueTable>,
        integrated: Option<RelationState<O>>,
    ) -> Self {
        let left_key = Arc::new(move |deltas: &[(L, i64)]| {
            deltas
                .iter()
                .filter_map(|(row, weight)| left_key(row).map(|key| (key, row.clone(), *weight)))
                .collect()
        });
        let right_key = Arc::new(move |deltas: &[(R, i64)]| {
            deltas
                .iter()
                .filter_map(|(row, weight)| right_key(row).map(|key| (key, row.clone(), *weight)))
                .collect()
        });
        Self::new_without_output_batch(
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
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_without_output_batch(
        left_state: RelationState<L>,
        right_state: RelationState<R>,
        left_index: IndexedBatchZSet<K, L>,
        right_index: IndexedBatchZSet<K, R>,
        left_key: BatchJoinKeyExtractor<L, K>,
        right_key: BatchJoinKeyExtractor<R, K>,
        predicate: JoinPredicate<L, R>,
        projector: JoinProjector<L, R, O>,
        table: Arc<dyn KeyValueTable>,
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
            output: None,
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

    fn join_entries_with_maps(
        &self,
        left: Option<&FastHashMap<L, i64>>,
        right: Option<&FastHashMap<R, i64>>,
        acc: &mut FastHashMap<O, i64>,
    ) {
        let (Some(left), Some(right)) = (left, right) else {
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

    fn stage_keyed_deltas<T>(
        &self,
        deltas: &[(T, i64)],
        extractor: &BatchJoinKeyExtractor<T, K>,
    ) -> KeyedRowDeltas<K, T>
    where
        T: Clone + Eq + Hash,
    {
        let mut keyed: KeyedRowDeltas<K, T> = FastHashMap::new();
        for (key, row, weight) in extractor(deltas) {
            if weight == 0 {
                continue;
            }
            let rows = keyed.entry(key).or_default();
            match rows.entry(row) {
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
        keyed.retain(|_, rows| !rows.is_empty());
        keyed
    }

    fn flatten_keyed_updates<T>(keyed: &KeyedRowDeltas<K, T>) -> Vec<(K, T, i64)>
    where
        T: Clone,
    {
        let estimated = keyed.values().map(|rows| rows.len()).sum();
        let mut updates = Vec::with_capacity(estimated);
        for (key, rows) in keyed {
            for (row, weight) in rows {
                if *weight == 0 {
                    continue;
                }
                updates.push((key.clone(), row.clone(), *weight));
            }
        }
        updates
    }

    async fn seed_memory_index_for_keys<T>(
        index_store: &IndexedBatchZSet<K, T>,
        memory_index: &mut FastHashMap<K, FastHashMap<T, i64>>,
        keys: impl Iterator<Item = &K>,
    ) -> Result<()>
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
        let mut missing_keys = Vec::new();
        for key in keys {
            if !memory_index.contains_key(key) {
                missing_keys.push(key.clone());
            }
        }

        for key in missing_keys {
            let values = index_store
                .values_for_key(&key)
                .await
                .context("load join index entries into memory cache")?;
            let mut rows: FastHashMap<T, i64> = FastHashMap::new();
            for (row, weight) in values {
                if weight == 0 {
                    continue;
                }
                match rows.entry(row) {
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
            memory_index.insert(key, rows);
        }

        Ok(())
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
        let mut keyed_deltas: Vec<(&T, i64)> = Vec::new();
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
        keyed: &KeyedRowDeltas<K, T>,
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

    async fn step_internal(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
        transient_inputs: Option<JoinTransientInputs<L, R>>,
        persist_output: bool,
    ) -> anyhow::Result<Option<JoinStepResult<O>>> {
        let left_delta_handle = inputs
            .first()
            .cloned()
            .context("join operator requires left delta handle")?;
        let right_delta_handle = inputs
            .get(1)
            .cloned()
            .context("join operator requires right delta handle")?;

        let left_loaded;
        let left_delta_values: &[(L, i64)] = if let Some(batch) = transient_inputs
            .as_ref()
            .and_then(|inputs| inputs.left.as_ref())
        {
            batch.as_ref()
        } else {
            left_loaded = delta_zset_handle_batch::<L>(
                self.table.clone(),
                &mut self.dict_cache_left,
                &left_delta_handle,
            )
            .await
            .context("load left delta for join")?;
            left_loaded.as_ref().as_slice()
        };
        let right_loaded;
        let right_delta_values: &[(R, i64)] = if let Some(batch) = transient_inputs
            .as_ref()
            .and_then(|inputs| inputs.right.as_ref())
        {
            batch.as_ref()
        } else {
            right_loaded = delta_zset_handle_batch::<R>(
                self.table.clone(),
                &mut self.dict_cache_right,
                &right_delta_handle,
            )
            .await
            .context("load right delta for join")?;
            right_loaded.as_ref().as_slice()
        };
        let left_keyed = self.stage_keyed_deltas(left_delta_values, &self.left_key);
        let right_keyed = self.stage_keyed_deltas(right_delta_values, &self.right_key);

        if self.persist_indexes {
            Self::seed_memory_index_for_keys(
                &self.right_index,
                &mut self.right_memory_index,
                left_keyed.keys(),
            )
            .await
            .context("seed right join memory index")?;
            Self::seed_memory_index_for_keys(
                &self.left_index,
                &mut self.left_memory_index,
                right_keyed.keys(),
            )
            .await
            .context("seed left join memory index")?;
        }

        // Build output delta from pre-update state (A, B) and current deltas
        // (ΔA, ΔB). State/index updates happen after this block to keep
        // each tick atomic.
        let mut delta_join: FastHashMap<O, i64> = FastHashMap::new();
        let has_left = !left_keyed.is_empty();
        let has_right = !right_keyed.is_empty();

        // ΔA ⋈ B
        if has_left {
            for (key, left_entries) in &left_keyed {
                self.join_entries_with_maps(
                    Some(left_entries),
                    self.right_memory_index.get(key),
                    &mut delta_join,
                );
            }
        }

        // A ⋈ ΔB
        if has_right {
            for (key, right_entries) in &right_keyed {
                self.join_entries_with_maps(
                    self.left_memory_index.get(key),
                    Some(right_entries),
                    &mut delta_join,
                );
            }
        }

        // ΔA ⋈ ΔB
        if has_left && has_right {
            for (key, left_entries) in &left_keyed {
                if let Some(right_entries) = right_keyed.get(key) {
                    self.join_entries_with_maps(
                        Some(left_entries),
                        Some(right_entries),
                        &mut delta_join,
                    );
                }
            }
        }
        delta_join.retain(|_, w| *w != 0);

        Self::apply_keyed_updates_to_memory_index(&mut self.left_memory_index, &left_keyed);
        Self::apply_keyed_updates_to_memory_index(&mut self.right_memory_index, &right_keyed);

        if self.persist_indexes {
            let left_updates = Self::flatten_keyed_updates(&left_keyed);
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
            let right_updates = Self::flatten_keyed_updates(&right_keyed);
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
            return if persist_output {
                let empty_handle = self
                    .output
                    .as_ref()
                    .context("join output persistence requested without configured output zset")?
                    .handle_for_version(0);
                Ok(Some(JoinStepResult {
                    delta_batch: Arc::new(Vec::new()),
                    persisted_handle: Some(empty_handle),
                }))
            } else {
                Ok(None)
            };
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

        let delta_batch = Arc::new(
            delta_join
                .iter()
                .map(|(row, weight)| (row.clone(), *weight))
                .collect(),
        );

        let persisted_handle = if persist_output {
            let output = self
                .output
                .as_mut()
                .context("join output persistence requested without configured output zset")?;
            let persisted_handle =
                Self::apply_deltas_to_versioned(output, &delta_join, None, "output")
                    .await
                    .context("persist join delta output")?;
            publish_transient_zset_batch(&persisted_handle, Arc::clone(&delta_batch));
            Some(persisted_handle)
        } else {
            None
        };

        Ok(Some(JoinStepResult {
            delta_batch,
            persisted_handle,
        }))
    }

    pub(crate) async fn on_step_transient_with_inputs(
        &mut self,
        ts: i64,
        inputs: &[ZSetHandle],
        transient_inputs: Option<JoinTransientInputs<L, R>>,
    ) -> anyhow::Result<Option<Arc<Vec<(O, i64)>>>> {
        Ok(self
            .step_internal(ts, inputs, transient_inputs, false)
            .await?
            .map(|result| result.delta_batch))
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
        ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        Ok(Some(
            self.step_internal(ts, inputs, None, true)
                .await?
                .context("join persisted path should always emit a handle")?
                .persisted_handle
                .context("join step persisted without output handle")?,
        ))
    }
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}
