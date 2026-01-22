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

use crate::collections::{IndexedZSet, RangeKey};
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::runtime::DeltaOperator;
use crate::stream::util::delta_zset_handle;

type JoinKeyExtractor<T, K> = Arc<dyn Fn(&T) -> Option<K> + Send + Sync>;
type RangeFunc<KL, KR> = Arc<dyn Fn(&KL) -> (KR, KR) + Send + Sync>;
type JoinProjector<L, R, O> = Arc<dyn Fn(&L, &R) -> O + Send + Sync>;

pub struct JoinRangeOp<L, R, O, KL, KR>
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
    KL: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    KL::Archived: RkyvDeserialize<KL, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    KR: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    KR::Archived: RkyvDeserialize<KR, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub left_state: RelationState<L>,
    pub right_state: RelationState<R>,
    pub left_index: IndexedZSet<KL, L>,
    pub right_index: IndexedZSet<KR, R>,
    pub left_key: JoinKeyExtractor<L, KL>,
    pub right_key: JoinKeyExtractor<R, KR>,
    pub range_func: RangeFunc<KL, KR>,
    pub projector: JoinProjector<L, R, O>,
    pub table: Arc<dyn KeyValueTable>,
    pub integrated: Option<RelationState<O>>,
    output: VersionedZSet<O>,
    dict_cache_left: HashMap<String, Arc<Dictionary<L>>>,
    dict_cache_right: HashMap<String, Arc<Dictionary<R>>>,
}

impl<L, R, O, KL, KR> JoinRangeOp<L, R, O, KL, KR>
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
    KL: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    KL::Archived: RkyvDeserialize<KL, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    KR: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    KR::Archived: RkyvDeserialize<KR, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        left_state: RelationState<L>,
        right_state: RelationState<R>,
        left_index: IndexedZSet<KL, L>,
        right_index: IndexedZSet<KR, R>,
        left_key: JoinKeyExtractor<L, KL>,
        right_key: JoinKeyExtractor<R, KR>,
        range_func: RangeFunc<KL, KR>,
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
            range_func,
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
                let out = (self.projector)(lk, rk);
                *acc.entry(out).or_insert(0) += lw * rw;
            }
        }
    }

    fn keyed_deltas<T>(
        &self,
        deltas: &HashMap<T, i64>,
        extractor: &JoinKeyExtractor<T, KL>,
    ) -> HashMap<KL, Vec<(T, i64)>>
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

    fn keyed_deltas_right<T>(
        &self,
        deltas: &HashMap<T, i64>,
        extractor: &JoinKeyExtractor<T, KR>,
    ) -> HashMap<KR, Vec<(T, i64)>>
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
                .context("intern key while staging range join delta")?;
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
            if base.is_some() && let Some(handle) = versioned.current_handle() {
                return Ok(handle);
            }
            return Ok(versioned.handle_for_version(0));
        }

        let mut batch = WriteBatch::new();
        let plan = versioned
            .enqueue_version_with_base(segments, base, 0, &mut batch)
            .await
            .context("schedule range join version update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write range join version update")?;

        let mut cleanup = WriteBatch::new();
        cleanup.delete(versioned.intent_key_bytes());
        versioned
            .table()
            .write_batch(cleanup)
            .await
            .context("clear range join intent")?;

        versioned.apply_version_plan(&plan);
        Ok(versioned.handle_for_version(plan.version))
    }

    fn range_contains(key_bytes: &[u8], lower: &[u8], upper: &[u8]) -> bool {
        key_bytes >= lower && key_bytes < upper
    }
}

#[async_trait]
impl<L, R, O, KL, KR> DeltaOperator for JoinRangeOp<L, R, O, KL, KR>
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
    KL: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    KL::Archived: RkyvDeserialize<KL, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    KR: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + RangeKey
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    KR::Archived: RkyvDeserialize<KR, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let left_delta_handle = inputs
            .first()
            .cloned()
            .context("range join requires left delta handle")?;
        let right_delta_handle = inputs
            .get(1)
            .cloned()
            .context("range join requires right delta handle")?;

        let left_delta_values = delta_zset_handle::<L>(
            self.table.clone(),
            &mut self.dict_cache_left,
            &left_delta_handle,
        )
        .await
        .context("load left delta for range join")?;
        let right_delta_values = delta_zset_handle::<R>(
            self.table.clone(),
            &mut self.dict_cache_right,
            &right_delta_handle,
        )
        .await
        .context("load right delta for range join")?;

        let left_delta = self.coalesce_deltas(left_delta_values);
        let right_delta = self.coalesce_deltas(right_delta_values);

        if left_delta.is_empty() && right_delta.is_empty() {
            return Ok(None);
        }

        let left_keyed = self.keyed_deltas(&left_delta, &self.left_key);
        let right_keyed = self.keyed_deltas_right(&right_delta, &self.right_key);

        let mut delta_join: HashMap<O, i64> = HashMap::new();

        for (key, left_entries) in &left_keyed {
            let (lower, upper) = (self.range_func)(key);
            let right_entries = self
                .right_index
                .values_for_key_range(&lower, &upper)
                .await
                .context("load right range index")?;
            if right_entries.is_empty() {
                continue;
            }
            let mut right_rows = Vec::with_capacity(right_entries.len());
            for (_, row, weight) in right_entries {
                right_rows.push((row, weight));
            }
            self.join_entries(left_entries, &right_rows, &mut delta_join);
        }

        if !right_keyed.is_empty() {
            let left_entries = self
                .left_index
                .entries()
                .await
                .context("scan left index for range join")?;
            if !left_entries.is_empty() {
                let mut left_by_key: HashMap<KL, Vec<(L, i64)>> = HashMap::new();
                for (key, row, weight) in left_entries {
                    left_by_key
                        .entry(key)
                        .or_insert_with(Vec::new)
                        .push((row, weight));
                }

                let mut left_ranges = Vec::with_capacity(left_by_key.len());
                for (key, entries) in &left_by_key {
                    let (lower, upper) = (self.range_func)(key);
                    let lower_bytes = lower.encode_range_key();
                    let upper_bytes = upper.encode_range_key();
                    left_ranges.push((lower_bytes, upper_bytes, entries));
                }

                for (right_key, right_entries) in &right_keyed {
                    let right_bytes = right_key.encode_range_key();
                    for (lower_bytes, upper_bytes, left_entries) in &left_ranges {
                        if !Self::range_contains(&right_bytes, lower_bytes, upper_bytes) {
                            continue;
                        }
                        self.join_entries(left_entries, right_entries, &mut delta_join);
                    }
                }
            }
        }

        if !left_keyed.is_empty() && !right_keyed.is_empty() {
            let mut left_ranges = Vec::with_capacity(left_keyed.len());
            for (key, entries) in &left_keyed {
                let (lower, upper) = (self.range_func)(key);
                let lower_bytes = lower.encode_range_key();
                let upper_bytes = upper.encode_range_key();
                left_ranges.push((lower_bytes, upper_bytes, entries));
            }

            for (right_key, right_entries) in &right_keyed {
                let right_bytes = right_key.encode_range_key();
                for (lower_bytes, upper_bytes, left_entries) in &left_ranges {
                    if !Self::range_contains(&right_bytes, lower_bytes, upper_bytes) {
                        continue;
                    }
                    self.join_entries(left_entries, right_entries, &mut delta_join);
                }
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
        if !left_updates.is_empty() {
            self.left_index
                .apply_deltas(left_updates)
                .await
                .context("update left range join index")?;
        }

        let mut right_updates = Vec::new();
        for (key, entries) in &right_keyed {
            for (row, weight) in entries {
                right_updates.push((key.clone(), row.clone(), *weight));
            }
        }
        if !right_updates.is_empty() {
            self.right_index
                .apply_deltas_with_range(right_updates)
                .await
                .context("update right range join index")?;
        }

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
                    .context("update integrated range join state")?;
            integrated.update_handle(new_integrated_handle);
        }

        let delta_handle = Self::apply_deltas_to_versioned(&mut self.output, &delta_join, None)
            .await
            .context("persist range join delta output")?;
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

    type Row = (i64, i64);
    type Out = (i64, i64);

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open("join_range", store).await.expect("open SlateDB"))
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
                .expect("intern key for range join test");
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

    fn recompute_range_join(
        left: &HashMap<Row, i64>,
        right: &HashMap<Row, i64>,
    ) -> HashMap<Out, i64> {
        let mut out = HashMap::new();
        for (l, lw) in left {
            for (r, rw) in right {
                if r.0 >= l.0 - 1 && r.0 < l.0 + 2 {
                    *out.entry((l.1, r.1)).or_insert(0) += lw * rw;
                }
            }
        }
        out.retain(|_, weight| *weight != 0);
        out
    }

    #[tokio::test]
    async fn range_join_operator_matches_recompute() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));

        let left_dict = Arc::new(
            Dictionary::<Row>::with_table(table.clone(), "range_left_stream", None)
                .await
                .expect("left dict"),
        );
        let right_dict = Arc::new(
            Dictionary::<Row>::with_table(table.clone(), "range_right_stream", None)
                .await
                .expect("right dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<Out>::with_table(table.clone(), "range_output", None)
                .await
                .expect("output dict"),
        );

        let left_state = RelationState::empty(table.clone(), "range_left_state".to_string())
            .await
            .expect("left state");
        let right_state = RelationState::empty(table.clone(), "range_right_state".to_string())
            .await
            .expect("right state");
        let output = VersionedZSet::new(output_dict.clone(), table.clone(), "range_output")
            .await
            .expect("output zset");

        let left_index = IndexedZSet::new(table.clone(), "range_left_index");
        let right_index = IndexedZSet::with_range_index(table.clone(), "range_right_index");

        let left_key = Arc::new(|row: &Row| Some(row.0));
        let right_key = Arc::new(|row: &Row| Some(row.0));
        let range_func = Arc::new(|key: &i64| (*key - 1, *key + 2));
        let projector = Arc::new(|left: &Row, right: &Row| (left.1, right.1));

        let mut op = JoinRangeOp::new(
            left_state,
            right_state,
            left_index,
            right_index,
            left_key,
            right_key,
            range_func,
            projector,
            table.clone(),
            output,
            None,
        );

        let left_deltas: Vec<Vec<(Row, i64)>> = vec![
            vec![((1, 10), 1), ((2, 20), 1)],
            vec![((1, 11), 1), ((2, 20), -1)],
            vec![((3, 30), 1)],
            vec![],
            vec![((2, 25), 1)],
        ];
        let right_deltas: Vec<Vec<(Row, i64)>> = vec![
            vec![((2, 200), 1), ((3, 300), 1)],
            vec![((1, 100), 1)],
            vec![((2, 200), -1)],
            vec![((4, 400), 1)],
            vec![],
        ];

        let mut left_state_map: HashMap<Row, i64> = HashMap::new();
        let mut right_state_map: HashMap<Row, i64> = HashMap::new();
        let mut prev_output: HashMap<Out, i64> = HashMap::new();

        let mut cache_out = HashMap::new();
        cache_out.insert("range_output".to_string(), output_dict.clone());

        for (step, (left_delta, right_delta)) in left_deltas
            .iter()
            .zip(right_deltas.iter())
            .enumerate()
        {
            apply_deltas(&mut left_state_map, left_delta);
            apply_deltas(&mut right_state_map, right_delta);

            let output_now = recompute_range_join(&left_state_map, &right_state_map);
            let expected_delta: HashMap<Out, i64> =
                compute_delta(&prev_output, &output_now).into_iter().collect();

            let left_handle = if left_delta.is_empty() {
                ZSetHandle {
                    ns: "range_left_stream".to_string(),
                    version: 0,
                }
            } else {
                stage_version(
                    left_dict.clone(),
                    table.clone(),
                    "range_left_stream",
                    left_delta,
                )
                .await
            };
            let right_handle = if right_delta.is_empty() {
                ZSetHandle {
                    ns: "range_right_stream".to_string(),
                    version: 0,
                }
            } else {
                stage_version(
                    right_dict.clone(),
                    table.clone(),
                    "range_right_stream",
                    right_delta,
                )
                .await
            };

            let out_handle = op
                .on_step(step as i64, &[left_handle, right_handle])
                .await
                .expect("range join step");

            if expected_delta.is_empty() {
                assert!(
                    out_handle.is_none(),
                    "expected empty output at step {step}"
                );
            } else {
                let out_handle = out_handle.expect("output handle");
                let materialized = materialize_zset_handle::<Out>(
                    table.clone(),
                    &mut cache_out,
                    &out_handle,
                )
                .await
                .expect("materialize output");
                assert_eq!(materialized, expected_delta, "step {step}");
            }

            prev_output = output_now;
        }
    }
}
