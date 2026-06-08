use std::collections::{BTreeMap, HashMap, hash_map::Entry};
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;

use crate::collections::RangeKey;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::collections::{ApplyDeltaMetrics, IndexedBatchZSet};
use crate::handles::ZSetHandle;
use crate::metrics;
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::{delta_zset_handle_batch, publish_transient_zset_batch};

mod interval_index;
use interval_index::LeftIntervalIndex;

pub type BatchLeftRangeExtractor<L, K> =
    Arc<dyn Fn(&[(L, i64)]) -> Vec<(K, K, L, i64)> + Send + Sync>;
pub type BatchRightKeyExtractor<R, K> = Arc<dyn Fn(&[(R, i64)]) -> Vec<(K, R, i64)> + Send + Sync>;
pub type RangeJoinPredicate<L, R> = Arc<dyn Fn(&L, &R) -> bool + Send + Sync>;
pub type RangeJoinProjector<L, R, O> = Arc<dyn Fn(&L, &R) -> O + Send + Sync>;
type RowDeltas<T> = HashMap<T, i64>;
type LeftRangeDeltas<L, K> = HashMap<L, (K, K, i64)>;
type RightKeyedDeltas<R, K> = HashMap<K, HashMap<R, i64>>;
type LeftRangeCache<L, K> = HashMap<L, (K, K, i64)>;
const RANGE_JOIN_DELTA_DELTA_INDEX_THRESHOLD: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeLookupMode {
    All,
    First,
}

pub struct RangeJoinBatchConfig<L, R, O, K>
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
        + Ord
        + RangeKey
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub left_state: RelationState<L>,
    pub right_state: RelationState<R>,
    pub right_index: IndexedBatchZSet<K, R>,
    pub left_range: BatchLeftRangeExtractor<L, K>,
    pub right_key: BatchRightKeyExtractor<R, K>,
    pub predicate: RangeJoinPredicate<L, R>,
    pub projector: RangeJoinProjector<L, R, O>,
    pub table: Arc<dyn KeyValueTable>,
    pub output: VersionedZSet<O>,
    pub integrated: Option<RelationState<O>>,
}

/// Incremental half-open range join.
///
/// Each left row maps to a range `[lower, upper)` over right keys. For each
/// tick this operator computes `ΔL ⋈ R`, `L ⋈ ΔR`, and `ΔL ⋈ ΔR` before
/// mutating state, matching DBSP's delta expansion for joins.
pub struct RangeJoinOp<L, R, O, K>
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
        + Ord
        + RangeKey
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub(crate) left_state: RelationState<L>,
    pub(crate) right_state: RelationState<R>,
    pub(crate) right_index: IndexedBatchZSet<K, R>,
    pub(crate) left_range: BatchLeftRangeExtractor<L, K>,
    pub(crate) right_key: BatchRightKeyExtractor<R, K>,
    pub(crate) predicate: RangeJoinPredicate<L, R>,
    pub(crate) projector: RangeJoinProjector<L, R, O>,
    pub(crate) table: Arc<dyn KeyValueTable>,
    pub(crate) integrated: Option<RelationState<O>>,
    output: VersionedZSet<O>,
    dict_cache_left: HashMap<String, Arc<Dictionary<L>>>,
    dict_cache_right: HashMap<String, Arc<Dictionary<R>>>,
    left_cache: Option<LeftRangeCache<L, K>>,
    left_interval_index: Option<LeftIntervalIndex<L, K>>,
    left_interval_overlay: LeftRangeCache<L, K>,
    range_lookup_mode: RangeLookupMode,
    logical_work: metrics::LogicalWorkCollector,
}

impl<L, R, O, K> RangeJoinOp<L, R, O, K>
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
        + Ord
        + RangeKey
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub fn new_batch(config: RangeJoinBatchConfig<L, R, O, K>) -> Self {
        Self::new_batch_with_lookup_mode(config, RangeLookupMode::All)
    }

    pub fn new_batch_with_lookup_mode(
        config: RangeJoinBatchConfig<L, R, O, K>,
        range_lookup_mode: RangeLookupMode,
    ) -> Self {
        let RangeJoinBatchConfig {
            left_state,
            right_state,
            right_index,
            left_range,
            right_key,
            predicate,
            projector,
            table,
            output,
            integrated,
        } = config;
        debug_assert_eq!(right_index.engine_kind(), "indexed_batch");
        Self {
            left_state,
            right_state,
            right_index,
            left_range,
            right_key,
            predicate,
            projector,
            table,
            integrated,
            output,
            dict_cache_left: HashMap::new(),
            dict_cache_right: HashMap::new(),
            left_cache: None,
            left_interval_index: None,
            left_interval_overlay: HashMap::new(),
            range_lookup_mode,
            logical_work: metrics::LogicalWorkCollector::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn last_logical_work(&self) -> metrics::LogicalWorkSnapshot {
        self.logical_work.last_tick()
    }

    async fn ensure_left_cache(&mut self) -> Result<()> {
        if self.left_cache.is_none() {
            let materialized = self
                .left_state
                .integrated
                .materialize()
                .await
                .context("materialize left range-join state")?;
            let input = materialized
                .into_iter()
                .filter(|(_, weight)| *weight != 0)
                .collect::<Vec<_>>();
            let mut cache = HashMap::new();
            for (lower, upper, row, weight) in (self.left_range)(&input) {
                if weight == 0 || lower >= upper {
                    continue;
                }
                cache.insert(row, (lower, upper, weight));
            }
            self.left_interval_index = Some(LeftIntervalIndex::from_cache(&cache));
            self.left_interval_overlay.clear();
            self.left_cache = Some(cache);
        }
        Ok(())
    }

    fn ensure_left_interval_index(&mut self) -> Result<()> {
        if self.left_interval_index.is_some() {
            return Ok(());
        }
        let cache = self
            .left_cache
            .as_ref()
            .context("range join left cache missing while rebuilding interval index")?;
        self.left_interval_index = Some(LeftIntervalIndex::from_cache(cache));
        self.left_interval_overlay.clear();
        Ok(())
    }

    fn rebuild_left_interval_index_if_overlay_large(&mut self) {
        let Some(cache) = self.left_cache.as_ref() else {
            return;
        };
        let rebuild_threshold = (cache.len() / 8).max(1024);
        if self.left_interval_overlay.len() >= rebuild_threshold {
            self.left_interval_index = Some(LeftIntervalIndex::from_cache(cache));
            self.left_interval_overlay.clear();
        }
    }

    fn coalesce_deltas<T>(deltas: &[(T, i64)]) -> RowDeltas<T>
    where
        T: Clone + Eq + Hash,
    {
        let mut coalesced: RowDeltas<T> = HashMap::new();
        for (row, weight) in deltas {
            if *weight == 0 {
                continue;
            }
            match coalesced.entry(row.clone()) {
                Entry::Occupied(mut entry) => {
                    let next = (*entry.get()).saturating_add(*weight);
                    if next == 0 {
                        entry.remove();
                    } else {
                        *entry.get_mut() = next;
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(*weight);
                }
            }
        }
        coalesced
    }

    fn stage_left_ranges(&self, deltas: &[(L, i64)]) -> LeftRangeDeltas<L, K> {
        let mut staged: LeftRangeDeltas<L, K> = HashMap::new();
        for (lower, upper, row, weight) in (self.left_range)(deltas) {
            if weight == 0 || lower >= upper {
                continue;
            }
            match staged.entry(row) {
                Entry::Occupied(mut entry) => {
                    let next = entry.get().2.saturating_add(weight);
                    if next == 0 {
                        entry.remove();
                    } else {
                        entry.get_mut().0 = lower;
                        entry.get_mut().1 = upper;
                        entry.get_mut().2 = next;
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert((lower, upper, weight));
                }
            }
        }
        staged
    }

    fn stage_right_keys(&self, deltas: &[(R, i64)]) -> RightKeyedDeltas<R, K> {
        let mut staged = HashMap::<K, HashMap<R, i64>>::new();
        for (key, row, weight) in (self.right_key)(deltas) {
            if weight == 0 {
                continue;
            }
            let rows = staged.entry(key.clone()).or_default();
            match rows.entry(row) {
                Entry::Occupied(mut entry) => {
                    let next = (*entry.get()).saturating_add(weight);
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
            if staged.get(&key).is_some_and(HashMap::is_empty) {
                staged.remove(&key);
            }
        }
        staged
    }

    fn add_output(acc: &mut HashMap<O, i64>, row: O, weight: i64) {
        if weight == 0 {
            return;
        }
        match acc.entry(row) {
            Entry::Occupied(mut entry) => {
                let next = (*entry.get()).saturating_add(weight);
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

    fn join_pair(
        predicate: &RangeJoinPredicate<L, R>,
        projector: &RangeJoinProjector<L, R, O>,
        left: &L,
        left_weight: i64,
        right: &R,
        right_weight: i64,
    ) -> Option<(O, i64)> {
        if left_weight == 0 || right_weight == 0 || !predicate(left, right) {
            return None;
        }
        Some((projector(left, right), left_weight * right_weight))
    }

    async fn join_left_delta_with_right_state(
        &self,
        left_ranges: &LeftRangeDeltas<L, K>,
        output_deltas: &mut HashMap<O, i64>,
        work: &mut metrics::LogicalWorkSnapshot,
    ) -> Result<()> {
        if self.range_lookup_mode == RangeLookupMode::All {
            for (left, (lower, upper, left_weight)) in left_ranges {
                let right_entries = self
                    .right_index
                    .values_for_key_range(lower, upper)
                    .await
                    .context("range lookup right index for range join")?;
                work.state_lookup_keys = work.state_lookup_keys.saturating_add(1);
                work.right_state_rows_examined = work
                    .right_state_rows_examined
                    .saturating_add(right_entries.len() as u64);
                work.state_scan_rows = work
                    .state_scan_rows
                    .saturating_add(right_entries.len() as u64);
                for (_, right, right_weight) in right_entries {
                    if let Some((out, weight)) = Self::join_pair(
                        &self.predicate,
                        &self.projector,
                        left,
                        *left_weight,
                        &right,
                        right_weight,
                    ) {
                        Self::add_output(output_deltas, out, weight);
                        work.join_output_rows = work.join_output_rows.saturating_add(1);
                    }
                }
            }
            return Ok(());
        }

        for (left, (lower, upper, left_weight)) in left_ranges {
            let (right_entries, lookup_metrics) = self
                .right_index
                .first_values_for_key_range_with_metrics(lower, upper)
                .await
                .context("first range lookup right index for range join")?;
            work.add_lookup_metrics(lookup_metrics);
            work.state_lookup_keys = work.state_lookup_keys.saturating_add(1);
            work.right_state_rows_examined = work
                .right_state_rows_examined
                .saturating_add(right_entries.len() as u64);
            work.state_scan_rows = work
                .state_scan_rows
                .saturating_add(right_entries.len() as u64);
            for (_, right, right_weight) in right_entries {
                if let Some((out, weight)) = Self::join_pair(
                    &self.predicate,
                    &self.projector,
                    left,
                    *left_weight,
                    &right,
                    right_weight,
                ) {
                    Self::add_output(output_deltas, out, weight);
                    work.join_output_rows = work.join_output_rows.saturating_add(1);
                }
            }
        }
        Ok(())
    }

    fn join_right_delta_with_left_index(
        left_index: &LeftIntervalIndex<L, K>,
        left_overlay: &LeftRangeCache<L, K>,
        right_keyed: &RightKeyedDeltas<R, K>,
        predicate: &RangeJoinPredicate<L, R>,
        projector: &RangeJoinProjector<L, R, O>,
        output_deltas: &mut HashMap<O, i64>,
        work: &mut metrics::LogicalWorkSnapshot,
    ) {
        work.state_lookup_keys = work
            .state_lookup_keys
            .saturating_add(right_keyed.len() as u64);
        for (right_key, right_rows) in right_keyed {
            left_index.visit_point(right_key, &mut |left, _lower, _upper, left_weight| {
                if left_overlay.contains_key(left) {
                    return;
                }
                work.left_state_rows_examined = work.left_state_rows_examined.saturating_add(1);
                work.state_scan_rows = work.state_scan_rows.saturating_add(1);
                for (right, right_weight) in right_rows {
                    if let Some((out, weight)) = Self::join_pair(
                        predicate,
                        projector,
                        left,
                        left_weight,
                        right,
                        *right_weight,
                    ) {
                        Self::add_output(output_deltas, out, weight);
                        work.join_output_rows = work.join_output_rows.saturating_add(1);
                    }
                }
            });
            for (left, (lower, upper, left_weight)) in left_overlay {
                if *left_weight == 0 || right_key < lower || right_key >= upper {
                    continue;
                }
                work.left_state_rows_examined = work.left_state_rows_examined.saturating_add(1);
                work.state_scan_rows = work.state_scan_rows.saturating_add(1);
                for (right, right_weight) in right_rows {
                    if let Some((out, weight)) = Self::join_pair(
                        predicate,
                        projector,
                        left,
                        *left_weight,
                        right,
                        *right_weight,
                    ) {
                        Self::add_output(output_deltas, out, weight);
                        work.join_output_rows = work.join_output_rows.saturating_add(1);
                    }
                }
            }
        }
    }

    fn join_left_delta_with_right_delta(
        &self,
        left_ranges: &LeftRangeDeltas<L, K>,
        right_keyed: &RightKeyedDeltas<R, K>,
        output_deltas: &mut HashMap<O, i64>,
        work: &mut metrics::LogicalWorkSnapshot,
    ) {
        if left_ranges.is_empty() || right_keyed.is_empty() {
            return;
        }
        let candidate_key_pairs = left_ranges.len().saturating_mul(right_keyed.len());
        if candidate_key_pairs > RANGE_JOIN_DELTA_DELTA_INDEX_THRESHOLD {
            let left_index = LeftIntervalIndex::from_cache(left_ranges);
            for (right_key, right_rows) in right_keyed {
                left_index.visit_point(right_key, &mut |left, _lower, _upper, left_weight| {
                    for (right, right_weight) in right_rows {
                        work.delta_delta_rows_examined =
                            work.delta_delta_rows_examined.saturating_add(1);
                        if let Some((out, weight)) = Self::join_pair(
                            &self.predicate,
                            &self.projector,
                            left,
                            left_weight,
                            right,
                            *right_weight,
                        ) {
                            Self::add_output(output_deltas, out, weight);
                            work.join_output_rows = work.join_output_rows.saturating_add(1);
                        }
                    }
                });
            }
            return;
        }

        for (left, (lower, upper, left_weight)) in left_ranges {
            for (right_key, right_rows) in right_keyed {
                if right_key < lower || right_key >= upper {
                    continue;
                }
                for (right, right_weight) in right_rows {
                    work.delta_delta_rows_examined =
                        work.delta_delta_rows_examined.saturating_add(1);
                    if let Some((out, weight)) = Self::join_pair(
                        &self.predicate,
                        &self.projector,
                        left,
                        *left_weight,
                        right,
                        *right_weight,
                    ) {
                        Self::add_output(output_deltas, out, weight);
                        work.join_output_rows = work.join_output_rows.saturating_add(1);
                    }
                }
            }
        }
    }

    fn apply_left_ranges_to_cache(
        cache: &mut LeftRangeCache<L, K>,
        overlay: &mut LeftRangeCache<L, K>,
        left_ranges: &LeftRangeDeltas<L, K>,
    ) -> bool {
        let mut changed = false;
        for (left, (lower, upper, weight)) in left_ranges {
            if *weight == 0 {
                continue;
            }
            match cache.entry(left.clone()) {
                Entry::Occupied(mut entry) => {
                    let next = entry.get().2.saturating_add(*weight);
                    if next == 0 {
                        entry.remove();
                        overlay.insert(left.clone(), (lower.clone(), upper.clone(), 0));
                        changed = true;
                    } else {
                        entry.get_mut().0 = lower.clone();
                        entry.get_mut().1 = upper.clone();
                        entry.get_mut().2 = next;
                        overlay.insert(left.clone(), (lower.clone(), upper.clone(), next));
                        changed = true;
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert((lower.clone(), upper.clone(), *weight));
                    overlay.insert(left.clone(), (lower.clone(), upper.clone(), *weight));
                    changed = true;
                }
            }
        }
        changed
    }

    async fn apply_deltas_to_versioned<T>(
        versioned: &mut VersionedZSet<T>,
        deltas: &RowDeltas<T>,
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
        let keyed_deltas = deltas
            .iter()
            .filter_map(|(key, delta)| (*delta != 0).then_some((key, *delta)))
            .collect::<Vec<_>>();
        if keyed_deltas.is_empty() {
            if base.is_some()
                && let Some(handle) = versioned.current_handle()
            {
                return Ok(handle);
            }
            return Ok(versioned.handle_for_version(0));
        }

        let dict = versioned.dictionary();
        let ids = dict
            .intern_many_values_unique(keyed_deltas.iter().map(|(key, _)| *key))
            .await
            .context("batch intern keys while staging range-join delta")?;

        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
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
            return Ok(versioned.handle_for_version(0));
        }

        let mut batch = WriteBatch::new();
        let plan = versioned
            .enqueue_version_with_base(segments, base, 0, &mut batch)
            .await
            .context("schedule range-join version update")?;
        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write range-join version update")?;
        versioned.apply_version_plan(&plan);
        Ok(versioned.handle_for_version(plan.version))
    }

    fn flatten_right_keyed(right_keyed: &RightKeyedDeltas<R, K>) -> Vec<(K, R, i64)> {
        let mut out = Vec::new();
        for (key, rows) in right_keyed {
            for (row, weight) in rows {
                if *weight != 0 {
                    out.push((key.clone(), row.clone(), *weight));
                }
            }
        }
        out
    }
}

#[async_trait]
impl<L, R, O, K> DeltaOperator for RangeJoinOp<L, R, O, K>
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
        + Ord
        + RangeKey
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    async fn on_step(&mut self, _ts: i64, inputs: &[ZSetHandle]) -> Result<Option<ZSetHandle>> {
        let left_delta_handle = inputs
            .first()
            .cloned()
            .context("range join operator requires left delta handle")?;
        let right_delta_handle = inputs
            .get(1)
            .cloned()
            .context("range join operator requires right delta handle")?;

        let left_delta_values = delta_zset_handle_batch::<L>(
            self.table.clone(),
            &mut self.dict_cache_left,
            &left_delta_handle,
        )
        .await
        .context("load left delta for range join")?;
        let right_delta_values = delta_zset_handle_batch::<R>(
            self.table.clone(),
            &mut self.dict_cache_right,
            &right_delta_handle,
        )
        .await
        .context("load right delta for range join")?;
        let mut work = metrics::LogicalWorkSnapshot {
            input_delta_rows: left_delta_values
                .len()
                .saturating_add(right_delta_values.len()) as u64,
            input_delta_batches: (!left_delta_values.is_empty()) as u64
                + (!right_delta_values.is_empty()) as u64,
            left_delta_rows: left_delta_values.len() as u64,
            right_delta_rows: right_delta_values.len() as u64,
            ..metrics::LogicalWorkSnapshot::default()
        };

        let left_delta = Self::coalesce_deltas(left_delta_values.as_ref());
        let right_delta = Self::coalesce_deltas(right_delta_values.as_ref());
        let left_ranges = self.stage_left_ranges(left_delta_values.as_ref());
        let right_keyed = self.stage_right_keys(right_delta_values.as_ref());
        work.left_changed_keys = left_ranges.len() as u64;
        work.right_changed_keys = right_keyed.len() as u64;

        if left_delta.is_empty() && right_delta.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }

        self.ensure_left_cache().await?;
        let mut output_deltas = HashMap::new();

        self.join_left_delta_with_right_state(&left_ranges, &mut output_deltas, &mut work)
            .await?;
        if !right_keyed.is_empty() {
            self.ensure_left_interval_index()?;
        }
        if let Some(left_index) = self.left_interval_index.as_ref() {
            Self::join_right_delta_with_left_index(
                left_index,
                &self.left_interval_overlay,
                &right_keyed,
                &self.predicate,
                &self.projector,
                &mut output_deltas,
                &mut work,
            );
        }
        self.join_left_delta_with_right_delta(
            &left_ranges,
            &right_keyed,
            &mut output_deltas,
            &mut work,
        );

        let left_base = self.left_state.base_version_for_update();
        let new_left_handle = Self::apply_deltas_to_versioned(
            &mut self.left_state.integrated,
            &left_delta,
            left_base,
        )
        .await
        .context("update left range-join integrated state")?;
        work.record_persisted_rows(left_delta.len());
        self.left_state.update_handle(new_left_handle);

        let right_base = self.right_state.base_version_for_update();
        let new_right_handle = Self::apply_deltas_to_versioned(
            &mut self.right_state.integrated,
            &right_delta,
            right_base,
        )
        .await
        .context("update right range-join integrated state")?;
        work.record_persisted_rows(right_delta.len());
        self.right_state.update_handle(new_right_handle);

        let right_updates = Self::flatten_right_keyed(&right_keyed);
        if !right_updates.is_empty() {
            let ApplyDeltaMetrics {
                persisted_records, ..
            } = self
                .right_index
                .apply_deltas_with_range_stats(right_updates)
                .await
                .context("update right range-join range index")?;
            work.record_persisted_rows(persisted_records);
        }

        if let Some(cache) = self.left_cache.as_mut()
            && Self::apply_left_ranges_to_cache(
                cache,
                &mut self.left_interval_overlay,
                &left_ranges,
            )
        {
            self.rebuild_left_interval_index_if_overlay_large();
        }

        if output_deltas.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }
        work.record_output_delta_rows(output_deltas.len());

        if let Some(integrated) = &mut self.integrated {
            let base = integrated.base_version_for_update();
            let new_integrated_handle =
                Self::apply_deltas_to_versioned(&mut integrated.integrated, &output_deltas, base)
                    .await
                    .context("update integrated range-join state")?;
            work.record_persisted_rows(output_deltas.len());
            integrated.update_handle(new_integrated_handle);
        }

        let delta_handle = Self::apply_deltas_to_versioned(&mut self.output, &output_deltas, None)
            .await
            .context("persist range-join delta output")?;
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
mod tests;
