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

type BatchLeftRangeExtractor<L, K> = Arc<dyn Fn(&[(L, i64)]) -> Vec<(K, K, L, i64)> + Send + Sync>;
type BatchRightKeyExtractor<R, K> = Arc<dyn Fn(&[(R, i64)]) -> Vec<(K, R, i64)> + Send + Sync>;
type RangeJoinPredicate<L, R> = Arc<dyn Fn(&L, &R) -> bool + Send + Sync>;
type RangeJoinProjector<L, R, O> = Arc<dyn Fn(&L, &R) -> O + Send + Sync>;
type RowDeltas<T> = HashMap<T, i64>;
type LeftRangeDeltas<L, K> = HashMap<L, (K, K, i64)>;
type RightKeyedDeltas<R, K> = HashMap<K, HashMap<R, i64>>;
type LeftRangeCache<L, K> = HashMap<L, (K, K, i64)>;

#[derive(Clone)]
struct LeftInterval<L, K> {
    row: L,
    lower: K,
    upper: K,
    weight: i64,
}

struct LeftIntervalNode<L, K> {
    center: K,
    by_lower: Vec<LeftInterval<L, K>>,
    by_upper_desc: Vec<LeftInterval<L, K>>,
    left: Option<Box<LeftIntervalNode<L, K>>>,
    right: Option<Box<LeftIntervalNode<L, K>>>,
}

struct LeftIntervalIndex<L, K> {
    root: Option<Box<LeftIntervalNode<L, K>>>,
}

impl<L, K> LeftIntervalIndex<L, K>
where
    L: Clone,
    K: Clone + Ord,
{
    fn from_cache(cache: &LeftRangeCache<L, K>) -> Self {
        let intervals = cache
            .iter()
            .filter_map(|(row, (lower, upper, weight))| {
                (*weight != 0 && lower < upper).then(|| LeftInterval {
                    row: row.clone(),
                    lower: lower.clone(),
                    upper: upper.clone(),
                    weight: *weight,
                })
            })
            .collect::<Vec<_>>();
        Self {
            root: Self::build_node(intervals),
        }
    }

    fn build_node(intervals: Vec<LeftInterval<L, K>>) -> Option<Box<LeftIntervalNode<L, K>>> {
        if intervals.is_empty() {
            return None;
        }

        let mut lowers = intervals
            .iter()
            .map(|interval| interval.lower.clone())
            .collect::<Vec<_>>();
        lowers.sort();
        let center = lowers[lowers.len() / 2].clone();

        let mut left = Vec::new();
        let mut right = Vec::new();
        let mut center_intervals = Vec::new();
        for interval in intervals {
            if interval.upper <= center {
                left.push(interval);
            } else if interval.lower > center {
                right.push(interval);
            } else {
                center_intervals.push(interval);
            }
        }

        let mut by_lower = center_intervals;
        by_lower.sort_by(|a, b| a.lower.cmp(&b.lower).then_with(|| a.upper.cmp(&b.upper)));
        let mut by_upper_desc = by_lower.clone();
        by_upper_desc.sort_by(|a, b| b.upper.cmp(&a.upper).then_with(|| a.lower.cmp(&b.lower)));

        Some(Box::new(LeftIntervalNode {
            center,
            by_lower,
            by_upper_desc,
            left: Self::build_node(left),
            right: Self::build_node(right),
        }))
    }

    fn visit_point<F>(&self, point: &K, visitor: &mut F)
    where
        F: FnMut(&L, &K, &K, i64),
    {
        if let Some(root) = self.root.as_ref() {
            root.visit_point(point, visitor);
        }
    }
}

impl<L, K> LeftIntervalNode<L, K>
where
    K: Ord,
{
    fn visit_point<F>(&self, point: &K, visitor: &mut F)
    where
        F: FnMut(&L, &K, &K, i64),
    {
        if point < &self.center {
            for interval in &self.by_lower {
                if &interval.lower > point {
                    break;
                }
                visitor(
                    &interval.row,
                    &interval.lower,
                    &interval.upper,
                    interval.weight,
                );
            }
            if let Some(left) = self.left.as_ref() {
                left.visit_point(point, visitor);
            }
        } else {
            for interval in &self.by_upper_desc {
                if &interval.upper <= point {
                    break;
                }
                visitor(
                    &interval.row,
                    &interval.lower,
                    &interval.upper,
                    interval.weight,
                );
            }
            if let Some(right) = self.right.as_ref() {
                right.visit_point(point, visitor);
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeLookupMode {
    All,
    First,
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
    pub left_state: RelationState<L>,
    pub right_state: RelationState<R>,
    pub right_index: IndexedBatchZSet<K, R>,
    pub left_range: BatchLeftRangeExtractor<L, K>,
    pub right_key: BatchRightKeyExtractor<R, K>,
    pub predicate: RangeJoinPredicate<L, R>,
    pub projector: RangeJoinProjector<L, R, O>,
    pub table: Arc<dyn KeyValueTable>,
    pub integrated: Option<RelationState<O>>,
    output: VersionedZSet<O>,
    dict_cache_left: HashMap<String, Arc<Dictionary<L>>>,
    dict_cache_right: HashMap<String, Arc<Dictionary<R>>>,
    left_cache: Option<LeftRangeCache<L, K>>,
    left_interval_index: Option<LeftIntervalIndex<L, K>>,
    left_interval_index_dirty: bool,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new_batch(
        left_state: RelationState<L>,
        right_state: RelationState<R>,
        right_index: IndexedBatchZSet<K, R>,
        left_range: BatchLeftRangeExtractor<L, K>,
        right_key: BatchRightKeyExtractor<R, K>,
        predicate: RangeJoinPredicate<L, R>,
        projector: RangeJoinProjector<L, R, O>,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<O>,
        integrated: Option<RelationState<O>>,
    ) -> Self {
        Self::new_batch_with_lookup_mode(
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
            RangeLookupMode::All,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_batch_with_lookup_mode(
        left_state: RelationState<L>,
        right_state: RelationState<R>,
        right_index: IndexedBatchZSet<K, R>,
        left_range: BatchLeftRangeExtractor<L, K>,
        right_key: BatchRightKeyExtractor<R, K>,
        predicate: RangeJoinPredicate<L, R>,
        projector: RangeJoinProjector<L, R, O>,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<O>,
        integrated: Option<RelationState<O>>,
        range_lookup_mode: RangeLookupMode,
    ) -> Self {
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
            left_interval_index_dirty: true,
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
            self.left_interval_index_dirty = false;
            self.left_cache = Some(cache);
        }
        Ok(())
    }

    fn ensure_left_interval_index(&mut self) -> Result<()> {
        if !self.left_interval_index_dirty && self.left_interval_index.is_some() {
            return Ok(());
        }
        let cache = self
            .left_cache
            .as_ref()
            .context("range join left cache missing while rebuilding interval index")?;
        self.left_interval_index = Some(LeftIntervalIndex::from_cache(cache));
        self.left_interval_index_dirty = false;
        Ok(())
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
        }
    }

    fn join_left_delta_with_right_delta(
        &self,
        left_ranges: &LeftRangeDeltas<L, K>,
        right_keyed: &RightKeyedDeltas<R, K>,
        output_deltas: &mut HashMap<O, i64>,
        work: &mut metrics::LogicalWorkSnapshot,
    ) {
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
        left_ranges: &LeftRangeDeltas<L, K>,
    ) -> bool {
        let mut changed = false;
        for (left, (lower, upper, weight)) in left_ranges {
            match cache.entry(left.clone()) {
                Entry::Occupied(mut entry) => {
                    let next = entry.get().2.saturating_add(*weight);
                    if next == 0 {
                        entry.remove();
                        changed = true;
                    } else {
                        entry.get_mut().0 = lower.clone();
                        entry.get_mut().1 = upper.clone();
                        entry.get_mut().2 = next;
                        changed = true;
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert((lower.clone(), upper.clone(), *weight));
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

        if let Some(cache) = self.left_cache.as_mut() {
            if Self::apply_left_ranges_to_cache(cache, &left_ranges) {
                self.left_interval_index_dirty = true;
            }
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
mod tests {
    use super::*;
    use crate::storage::SlateTable;
    use crate::stream::runtime::DeltaOperator;
    use crate::stream::util::materialize_zset_handle;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::sync::atomic::{AtomicU64, Ordering};

    type LeftRow = (i64, i64, i64);
    type RightRow = (i64, i64);
    type OutRow = (i64, i64);

    static TEST_NAMESPACE_COUNTER: AtomicU64 = AtomicU64::new(1);

    fn next_test_suffix() -> u64 {
        TEST_NAMESPACE_COUNTER.fetch_add(1, Ordering::Relaxed)
    }

    async fn build_db(suffix: u64) -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(
            Db::open(format!("range_join_op_{suffix}"), store)
                .await
                .expect("open SlateDB"),
        )
    }

    async fn stage_version<T>(
        dict: Arc<Dictionary<T>>,
        table: Arc<SlateTable>,
        ns: &str,
        deltas: &[(T, i64)],
    ) -> ZSetHandle
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
        if deltas.is_empty() {
            return ZSetHandle {
                ns: ns.to_string(),
                version: 0,
            };
        }

        let mut zset = VersionedZSet::new(dict, table, ns.to_string())
            .await
            .expect("versioned zset");
        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let values = deltas.iter().map(|(value, _)| value);
        let ids = zset
            .dictionary()
            .intern_many_values_unique(values)
            .await
            .expect("intern values");
        for ((_, weight), id) in deltas.iter().zip(ids.into_iter()) {
            buckets
                .entry(bucket_for(id))
                .or_default()
                .push((id, *weight));
        }
        let segments = buckets
            .into_iter()
            .map(|(bucket, deltas)| SegmentRecord {
                id: 0,
                bucket,
                deltas,
            })
            .collect();
        let version = zset
            .create_version_with_base(segments, None)
            .await
            .expect("create version");
        let handle = zset.handle_for_version(version);
        publish_transient_zset_batch(&handle, Arc::new(deltas.to_vec()));
        handle
    }

    async fn build_op(
        suffix: u64,
    ) -> (
        RangeJoinOp<LeftRow, RightRow, OutRow, i64>,
        Arc<Dictionary<LeftRow>>,
        Arc<Dictionary<RightRow>>,
        Arc<SlateTable>,
    ) {
        let db = build_db(suffix).await;
        let table = Arc::new(SlateTable::new(db));
        let left_dict = Arc::new(
            Dictionary::<LeftRow>::with_table(
                table.clone(),
                format!("range_left_stream_{suffix}"),
                None,
            )
            .await
            .expect("left dict"),
        );
        let right_dict = Arc::new(
            Dictionary::<RightRow>::with_table(
                table.clone(),
                format!("range_right_stream_{suffix}"),
                None,
            )
            .await
            .expect("right dict"),
        );
        let out_dict = Arc::new(
            Dictionary::<OutRow>::with_table(table.clone(), format!("range_output_{suffix}"), None)
                .await
                .expect("output dict"),
        );

        let left_state = RelationState::empty(table.clone(), format!("range_left_state_{suffix}"))
            .await
            .expect("left state");
        let right_state =
            RelationState::empty(table.clone(), format!("range_right_state_{suffix}"))
                .await
                .expect("right state");
        let output = VersionedZSet::new(out_dict, table.clone(), format!("range_output_{suffix}"))
            .await
            .expect("output zset");
        let right_index = IndexedBatchZSet::with_range_index(
            table.clone(),
            format!("range_right_index_{suffix}"),
        );
        let left_range: BatchLeftRangeExtractor<LeftRow, i64> = Arc::new(|deltas| {
            deltas
                .iter()
                .map(|(row @ (_, lower, upper), weight)| (*lower, *upper, *row, *weight))
                .collect()
        });
        let right_key: BatchRightKeyExtractor<RightRow, i64> = Arc::new(|deltas| {
            deltas
                .iter()
                .map(|(row @ (key, _), weight)| (*key, *row, *weight))
                .collect()
        });
        let predicate: RangeJoinPredicate<LeftRow, RightRow> = Arc::new(|_, _| true);
        let projector: RangeJoinProjector<LeftRow, RightRow, OutRow> =
            Arc::new(|left, right| (left.0, right.1));

        let op = RangeJoinOp::new_batch(
            left_state,
            right_state,
            right_index,
            left_range,
            right_key,
            predicate,
            projector,
            table.clone(),
            output,
            None,
        );

        (op, left_dict, right_dict, table)
    }

    #[tokio::test]
    async fn range_join_emits_all_three_delta_terms() {
        let suffix = next_test_suffix();
        let (mut op, left_dict, right_dict, table) = build_op(suffix).await;
        let mut cache = HashMap::new();

        let left_t1 = stage_version(
            left_dict.clone(),
            table.clone(),
            "range_left_stream_t1",
            &[((1, 10, 20), 1)],
        )
        .await;
        let right_t1 = stage_version(
            right_dict.clone(),
            table.clone(),
            "range_right_stream_t1",
            &[((15, 100), 1)],
        )
        .await;
        let out_t1 = op
            .on_step(1, &[left_t1, right_t1])
            .await
            .expect("range join t1")
            .expect("output t1");
        let materialized_t1 = materialize_zset_handle::<OutRow>(table.clone(), &mut cache, &out_t1)
            .await
            .expect("materialize t1");
        assert_eq!(materialized_t1, HashMap::from([((1, 100), 1)]));

        let left_t2 = stage_version(
            left_dict.clone(),
            table.clone(),
            "range_left_stream_t2",
            &[],
        )
        .await;
        let right_t2 = stage_version(
            right_dict.clone(),
            table.clone(),
            "range_right_stream_t2",
            &[((12, 101), 1)],
        )
        .await;
        let out_t2 = op
            .on_step(2, &[left_t2, right_t2])
            .await
            .expect("range join t2")
            .expect("output t2");
        let materialized_t2 = materialize_zset_handle::<OutRow>(table.clone(), &mut cache, &out_t2)
            .await
            .expect("materialize t2");
        assert_eq!(materialized_t2, HashMap::from([((1, 101), 1)]));

        let left_t3 = stage_version(
            left_dict.clone(),
            table.clone(),
            "range_left_stream_t3",
            &[((2, 10, 13), 1)],
        )
        .await;
        let right_t3 = stage_version(
            right_dict.clone(),
            table.clone(),
            "range_right_stream_t3",
            &[],
        )
        .await;
        let out_t3 = op
            .on_step(3, &[left_t3, right_t3])
            .await
            .expect("range join t3")
            .expect("output t3");
        let materialized_t3 = materialize_zset_handle::<OutRow>(table.clone(), &mut cache, &out_t3)
            .await
            .expect("materialize t3");
        assert_eq!(materialized_t3, HashMap::from([((2, 101), 1)]));
        assert_eq!(op.last_logical_work().output_delta_rows, 1);
    }

    #[tokio::test]
    async fn range_join_retracts_right_delta_against_existing_left_ranges() {
        let suffix = next_test_suffix();
        let (mut op, left_dict, right_dict, table) = build_op(suffix).await;
        let mut cache = HashMap::new();

        let left_t1 = stage_version(
            left_dict.clone(),
            table.clone(),
            "range_retract_left_stream_t1",
            &[((1, 10, 20), 1), ((2, 10, 13), 1)],
        )
        .await;
        let right_t1 = stage_version(
            right_dict.clone(),
            table.clone(),
            "range_retract_right_stream_t1",
            &[((12, 101), 1)],
        )
        .await;
        op.on_step(1, &[left_t1, right_t1])
            .await
            .expect("range join t1");

        let left_t2 = stage_version(
            left_dict.clone(),
            table.clone(),
            "range_retract_left_stream_t2",
            &[],
        )
        .await;
        let right_t2 = stage_version(
            right_dict.clone(),
            table.clone(),
            "range_retract_right_stream_t2",
            &[((12, 101), -1)],
        )
        .await;
        let out_t2 = op
            .on_step(2, &[left_t2, right_t2])
            .await
            .expect("range join t2")
            .expect("output t2");
        let materialized_t2 = materialize_zset_handle::<OutRow>(table.clone(), &mut cache, &out_t2)
            .await
            .expect("materialize t2");
        assert_eq!(
            materialized_t2,
            HashMap::from([((1, 101), -1), ((2, 101), -1)])
        );
        assert_eq!(op.last_logical_work().output_delta_rows, 2);
    }

    #[tokio::test]
    async fn range_join_right_delta_uses_left_interval_index() {
        let suffix = next_test_suffix();
        let (mut op, left_dict, right_dict, table) = build_op(suffix).await;
        let mut cache = HashMap::new();

        let left_rows = (0..100)
            .map(|id| ((id, id * 10, id * 10 + 5), 1))
            .collect::<Vec<_>>();
        let left_t1 = stage_version(
            left_dict.clone(),
            table.clone(),
            "range_index_left_stream_t1",
            &left_rows,
        )
        .await;
        let right_t1 = stage_version(
            right_dict.clone(),
            table.clone(),
            "range_index_right_stream_t1",
            &[],
        )
        .await;
        op.on_step(1, &[left_t1, right_t1])
            .await
            .expect("seed left ranges");

        let left_t2 =
            stage_version(left_dict, table.clone(), "range_index_left_stream_t2", &[]).await;
        let right_t2 = stage_version(
            right_dict,
            table.clone(),
            "range_index_right_stream_t2",
            &[((502, 900), 1)],
        )
        .await;
        let out_t2 = op
            .on_step(2, &[left_t2, right_t2])
            .await
            .expect("probe right delta")
            .expect("output t2");
        let materialized_t2 = materialize_zset_handle::<OutRow>(table.clone(), &mut cache, &out_t2)
            .await
            .expect("materialize t2");
        assert_eq!(materialized_t2, HashMap::from([((50, 900), 1)]));
        assert_eq!(
            op.last_logical_work().left_state_rows_examined,
            1,
            "right-delta probing should visit matching left intervals, not the whole left cache",
        );
    }
}
