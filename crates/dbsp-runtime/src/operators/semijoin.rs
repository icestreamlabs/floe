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

use crate::collections::IndexedBatchZSet;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::{delta_zset_handle_batch, publish_transient_zset_batch};

type JoinKeyExtractor<T, K> = Arc<dyn Fn(&T) -> Option<K> + Send + Sync>;
type BatchJoinKeyExtractor<T, K> = Arc<dyn Fn(&[(T, i64)]) -> Vec<(K, T, i64)> + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SemiJoinMode {
    Semi,
    Anti,
}

impl SemiJoinMode {
    fn active(self, right_present: bool) -> bool {
        match self {
            SemiJoinMode::Semi => right_present,
            SemiJoinMode::Anti => !right_present,
        }
    }
}

pub struct SemiJoinOp<L, R, K>
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
    pub right_index: IndexedBatchZSet<K, ()>,
    pub left_key: BatchJoinKeyExtractor<L, K>,
    pub right_key: BatchJoinKeyExtractor<R, K>,
    pub mode: SemiJoinMode,
    pub table: Arc<dyn KeyValueTable>,
    pub integrated: Option<RelationState<L>>,
    output: VersionedZSet<L>,
    dict_cache_left: HashMap<String, Arc<Dictionary<L>>>,
    dict_cache_right: HashMap<String, Arc<Dictionary<R>>>,
}

impl<L, R, K> SemiJoinOp<L, R, K>
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
        right_index: IndexedBatchZSet<K, ()>,
        left_key: JoinKeyExtractor<L, K>,
        right_key: JoinKeyExtractor<R, K>,
        mode: SemiJoinMode,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<L>,
        integrated: Option<RelationState<L>>,
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
            mode,
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
        right_index: IndexedBatchZSet<K, ()>,
        left_key: BatchJoinKeyExtractor<L, K>,
        right_key: BatchJoinKeyExtractor<R, K>,
        mode: SemiJoinMode,
        table: Arc<dyn KeyValueTable>,
        output: VersionedZSet<L>,
        integrated: Option<RelationState<L>>,
    ) -> Self {
        Self {
            left_state,
            right_state,
            left_index,
            right_index,
            left_key,
            right_key,
            mode,
            table,
            integrated,
            output,
            dict_cache_left: HashMap::new(),
            dict_cache_right: HashMap::new(),
        }
    }

    fn keyed_deltas<T>(
        &self,
        deltas: &HashMap<T, i64>,
        extractor: &BatchJoinKeyExtractor<T, K>,
    ) -> HashMap<K, Vec<(T, i64)>>
    where
        T: Clone + Eq + Hash,
    {
        let mut keyed = HashMap::new();
        let rows = deltas
            .iter()
            .map(|(row, weight)| (row.clone(), *weight))
            .collect::<Vec<_>>();
        for (key, row, weight) in extractor(&rows) {
            if weight == 0 {
                continue;
            }
            keyed
                .entry(key)
                .or_insert_with(Vec::new)
                .push((row, weight));
        }
        keyed
    }

    fn coalesce_deltas<T>(&self, deltas: Vec<(T, i64)>) -> HashMap<T, i64>
    where
        T: Clone + Eq + Hash,
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
                .context("intern key while staging semijoin delta")?;
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
            .context("schedule semijoin version update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write semijoin version update")?;

        versioned.apply_version_plan(&plan);
        Ok(versioned.handle_for_version(plan.version))
    }
}

#[async_trait]
impl<L, R, K> DeltaOperator for SemiJoinOp<L, R, K>
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
            .context("semijoin operator requires left delta handle")?;
        let right_delta_handle = inputs
            .get(1)
            .cloned()
            .context("semijoin operator requires right delta handle")?;

        let left_delta_values = delta_zset_handle_batch::<L>(
            self.table.clone(),
            &mut self.dict_cache_left,
            &left_delta_handle,
        )
        .await
        .context("load left delta for semijoin")?;
        let right_delta_values = delta_zset_handle_batch::<R>(
            self.table.clone(),
            &mut self.dict_cache_right,
            &right_delta_handle,
        )
        .await
        .context("load right delta for semijoin")?;

        let left_delta = self.coalesce_deltas(left_delta_values.as_ref().clone());
        let right_delta = self.coalesce_deltas(right_delta_values.as_ref().clone());

        if left_delta.is_empty() && right_delta.is_empty() {
            return Ok(Some(self.output.handle_for_version(0)));
        }

        let left_keyed = self.keyed_deltas(&left_delta, &self.left_key);
        let right_keyed = self.keyed_deltas(&right_delta, &self.right_key);

        let mut right_presence = HashMap::<K, (bool, bool)>::new();
        for (key, entries) in &right_keyed {
            let existing = self
                .right_index
                .values_for_key(key)
                .await
                .context("load right index for semijoin")?;
            let mut current_count = existing.into_iter().map(|(_, weight)| weight).sum::<i64>();
            let prev_present = current_count != 0;
            for (_, delta) in entries {
                current_count += *delta;
            }
            let new_present = current_count != 0;
            right_presence.insert(key.clone(), (prev_present, new_present));
        }

        let mut left_prev_by_key = HashMap::new();
        for (key, (prev_present, new_present)) in &right_presence {
            if self.mode.active(*prev_present) == self.mode.active(*new_present) {
                continue;
            }
            let entries = self
                .left_index
                .values_for_key(key)
                .await
                .context("load left index for semijoin")?;
            if !entries.is_empty() {
                left_prev_by_key.insert(key.clone(), entries);
            }
        }

        let mut output_deltas: HashMap<L, i64> = HashMap::new();
        for (key, entries) in &left_keyed {
            let right_present = match right_presence.get(key) {
                Some((_, new_present)) => *new_present,
                None => {
                    self.right_index
                        .values_for_key(key)
                        .await
                        .context("load right index for semijoin probe")?
                        .into_iter()
                        .map(|(_, weight)| weight)
                        .sum::<i64>()
                        != 0
                }
            };
            if self.mode.active(right_present) {
                for (row, weight) in entries {
                    let entry = output_deltas.entry(row.clone()).or_insert(0);
                    *entry += *weight;
                    if *entry == 0 {
                        output_deltas.remove(row);
                    }
                }
            }
        }

        for (key, (prev_present, new_present)) in &right_presence {
            let prev_active = self.mode.active(*prev_present);
            let new_active = self.mode.active(*new_present);
            if prev_active == new_active {
                continue;
            }
            if let Some(entries) = left_prev_by_key.get(key) {
                let multiplier = if new_active { 1 } else { -1 };
                for (row, weight) in entries {
                    let entry = output_deltas.entry(row.clone()).or_insert(0);
                    *entry += multiplier * *weight;
                    if *entry == 0 {
                        output_deltas.remove(row);
                    }
                }
            }
        }

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

        let mut left_updates = Vec::new();
        for (key, entries) in &left_keyed {
            for (row, weight) in entries {
                left_updates.push((key.clone(), row.clone(), *weight));
            }
        }
        if !left_updates.is_empty() {
            self.left_index
                .apply_deltas(left_updates)
                .await
                .context("update left semijoin index")?;
        }

        let mut right_updates = Vec::new();
        for (key, entries) in &right_keyed {
            for (_, weight) in entries {
                right_updates.push((key.clone(), (), *weight));
            }
        }
        if !right_updates.is_empty() {
            self.right_index
                .apply_deltas(right_updates)
                .await
                .context("update right semijoin index")?;
        }

        if output_deltas.is_empty() {
            return Ok(Some(self.output.handle_for_version(0)));
        }

        if let Some(integrated) = &mut self.integrated {
            let base = integrated
                .integrated
                .current_handle()
                .map(|handle| handle.version);
            let new_integrated_handle =
                Self::apply_deltas_to_versioned(&mut integrated.integrated, &output_deltas, base)
                    .await
                    .context("update integrated semijoin state")?;
            integrated.update_handle(new_integrated_handle);
        }

        let delta_handle = Self::apply_deltas_to_versioned(&mut self.output, &output_deltas, None)
            .await
            .context("persist semijoin delta output")?;
        publish_transient_zset_batch(
            &delta_handle,
            Arc::new(output_deltas.into_iter().collect::<Vec<_>>()),
        );
        Ok(Some(delta_handle))
    }
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::runtime::DeltaOperator;
    use crate::stream::util::{compute_delta, materialize_zset_handle};
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    type Row = (i64, i64);

    static TEST_NAMESPACE_COUNTER: AtomicU64 = AtomicU64::new(1);

    fn next_test_suffix() -> u64 {
        TEST_NAMESPACE_COUNTER.fetch_add(1, Ordering::Relaxed)
    }

    async fn build_db(suffix: u64) -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(
            Db::open(format!("semijoinop_{suffix}"), store)
                .await
                .expect("open SlateDB"),
        )
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
            let id = dict_batch
                .intern(key)
                .await
                .expect("intern key for semijoin test");
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

    fn apply_deltas<K: Clone + Eq + Hash>(state: &mut HashMap<K, i64>, deltas: &[(K, i64)]) {
        for (key, delta) in deltas {
            let entry = state.entry(key.clone()).or_insert(0);
            *entry += *delta;
            if *entry == 0 {
                state.remove(key);
            }
        }
    }

    fn recompute_semijoin(
        left: &HashMap<Row, i64>,
        right: &HashMap<Row, i64>,
        mode: SemiJoinMode,
    ) -> HashMap<Row, i64> {
        let mut right_keys = std::collections::HashSet::<i64>::new();
        for (row, weight) in right {
            if *weight != 0 {
                right_keys.insert(row.0);
            }
        }

        let mut out = HashMap::new();
        for (row, weight) in left {
            if *weight == 0 {
                continue;
            }
            let present = right_keys.contains(&row.0);
            if mode.active(present) {
                out.insert(*row, *weight);
            }
        }
        out
    }

    async fn run_semijoin_case(mode: SemiJoinMode) {
        let suffix = next_test_suffix();
        let db = build_db(suffix).await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));

        let prefix = format!("semijoin_{mode:?}_{suffix}");
        let left_stream_ns = format!("{prefix}_left_stream");
        let right_stream_ns = format!("{prefix}_right_stream");
        let output_ns = format!("{prefix}_output");
        let left_state_ns = format!("{prefix}_left_state");
        let right_state_ns = format!("{prefix}_right_state");
        let left_index_ns = format!("{prefix}_left_index");
        let right_index_ns = format!("{prefix}_right_index");

        let left_dict = Arc::new(
            Dictionary::<Row>::with_table(table.clone(), left_stream_ns.clone(), None)
                .await
                .expect("left dict"),
        );
        let right_dict = Arc::new(
            Dictionary::<Row>::with_table(table.clone(), right_stream_ns.clone(), None)
                .await
                .expect("right dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<Row>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .expect("output dict"),
        );

        let left_state = RelationState::empty(table.clone(), left_state_ns)
            .await
            .expect("left state");
        let right_state = RelationState::empty(table.clone(), right_state_ns)
            .await
            .expect("right state");
        let output = VersionedZSet::new(output_dict.clone(), table.clone(), output_ns.clone())
            .await
            .expect("output zset");
        let left_index = IndexedBatchZSet::new(table.clone(), left_index_ns);
        let right_index = IndexedBatchZSet::new(table.clone(), right_index_ns);

        let left_key = Arc::new(|row: &Row| Some(row.0));
        let right_key = Arc::new(|row: &Row| Some(row.0));

        let mut op = SemiJoinOp::new(
            left_state,
            right_state,
            left_index,
            right_index,
            left_key,
            right_key,
            mode,
            table.clone(),
            output,
            None,
        );

        let left_deltas: Vec<Vec<(Row, i64)>> = vec![
            vec![((1, 10), 1), ((2, 20), 1)],
            vec![((1, 11), 1), ((2, 20), -1)],
            vec![((1, 12), 1), ((3, 30), 1)],
            vec![],
            vec![((1, 13), 1)],
        ];
        let right_deltas: Vec<Vec<(Row, i64)>> = vec![
            vec![((1, 100), 1)],
            vec![((2, 200), 1)],
            vec![((1, 100), -1)],
            vec![((1, 101), 1), ((1, 102), 1)],
            vec![((1, 103), 1)],
        ];

        let mut left_state_map: HashMap<Row, i64> = HashMap::new();
        let mut right_state_map: HashMap<Row, i64> = HashMap::new();
        let mut prev_output: HashMap<Row, i64> = HashMap::new();

        let mut cache_out = HashMap::new();
        cache_out.insert(output_ns.clone(), output_dict.clone());

        for (step, (left_delta, right_delta)) in
            left_deltas.iter().zip(right_deltas.iter()).enumerate()
        {
            apply_deltas(&mut left_state_map, left_delta);
            apply_deltas(&mut right_state_map, right_delta);

            let output_now = recompute_semijoin(&left_state_map, &right_state_map, mode);
            let expected_delta: HashMap<Row, i64> = compute_delta(&prev_output, &output_now)
                .into_iter()
                .collect();

            let left_handle = if left_delta.is_empty() {
                ZSetHandle {
                    ns: left_stream_ns.clone(),
                    version: 0,
                }
            } else {
                stage_version(
                    left_dict.clone(),
                    table.clone(),
                    left_stream_ns.as_str(),
                    left_delta,
                )
                .await
            };
            let right_handle = if right_delta.is_empty() {
                ZSetHandle {
                    ns: right_stream_ns.clone(),
                    version: 0,
                }
            } else {
                stage_version(
                    right_dict.clone(),
                    table.clone(),
                    right_stream_ns.as_str(),
                    right_delta,
                )
                .await
            };

            let out_handle = op
                .on_step(step as i64, &[left_handle, right_handle])
                .await
                .expect("semijoin step");

            let out_handle = out_handle.expect("output handle");
            let materialized =
                materialize_zset_handle::<Row>(table.clone(), &mut cache_out, &out_handle)
                    .await
                    .expect("materialize output");
            assert_eq!(materialized, expected_delta, "step {step}");

            prev_output = output_now;
        }

        let mut right_key_one_values = op
            .right_index
            .values_for_key(&1)
            .await
            .expect("load compact right semijoin index");
        right_key_one_values.sort_unstable();
        assert_eq!(right_key_one_values, vec![((), 3)]);
    }

    #[tokio::test]
    async fn semijoin_operator_matches_recompute() {
        run_semijoin_case(SemiJoinMode::Semi).await;
    }

    #[tokio::test]
    async fn antijoin_operator_matches_recompute() {
        run_semijoin_case(SemiJoinMode::Anti).await;
    }
}
