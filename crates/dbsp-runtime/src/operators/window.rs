use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Context, Result, anyhow, ensure};
use async_trait::async_trait;
use prometheus::{IntCounter, IntGauge, register_int_counter, register_int_gauge};
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
use crate::stream::util::delta_zset_handle;

type KeyExtractor<V, K> = Arc<dyn Fn(&V) -> Option<K> + Send + Sync>;
type TimeExtractor<V> = Arc<dyn Fn(&V) -> Option<i64> + Send + Sync>;
type Aggregator<K, V, A> = Arc<dyn Fn(&K, &[(V, i64)]) -> Option<A> + Send + Sync>;

static WINDOW_DROPPED_TOO_LATE_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "floe_window_events_dropped_too_late_total",
        "Number of input rows dropped by window operators because they arrived beyond allowed lateness",
    )
    .expect("register floe_window_events_dropped_too_late_total")
});

static WINDOW_STATE_ENTRIES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "floe_window_state_entries",
        "Approximate number of active window aggregate entries currently retained",
    )
    .expect("register floe_window_state_entries")
});

#[derive(Clone, Debug, Eq, Hash, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct WindowKey<K> {
    pub start: i64,
    pub end: i64,
    pub key: K,
}

pub struct WindowAggregateOp<K, V, A>
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
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    A: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    A::Archived: RkyvDeserialize<A, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub state: RelationState<(WindowKey<K>, A)>,
    pub index: IndexedBatchZSet<WindowKey<K>, V>,
    pub table: Arc<dyn KeyValueTable>,
    pub key_extractor: KeyExtractor<V, K>,
    pub time_extractor: TimeExtractor<V>,
    pub aggregator: Aggregator<K, V, A>,
    pub watermark: Arc<AtomicI64>,
    output: VersionedZSet<(WindowKey<K>, A)>,
    window_size: i64,
    window_slide: i64,
    allowed_lateness_ms: i64,
    dict_cache: HashMap<String, Arc<Dictionary<V>>>,
    aggregate_cache: Option<HashMap<WindowKey<K>, A>>,
}

impl<K, V, A> WindowAggregateOp<K, V, A>
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
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    A: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    A::Archived: RkyvDeserialize<A, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        state: RelationState<(WindowKey<K>, A)>,
        index: IndexedBatchZSet<WindowKey<K>, V>,
        table: Arc<dyn KeyValueTable>,
        key_extractor: KeyExtractor<V, K>,
        time_extractor: TimeExtractor<V>,
        aggregator: Aggregator<K, V, A>,
        output: VersionedZSet<(WindowKey<K>, A)>,
        window_size: i64,
        window_slide: i64,
        allowed_lateness_ms: i64,
        watermark: Arc<AtomicI64>,
    ) -> Result<Self> {
        ensure!(window_size > 0, "window size must be positive");
        ensure!(window_slide > 0, "window slide must be positive");
        ensure!(
            window_size % window_slide == 0,
            "window size must be a multiple of slide"
        );
        ensure!(
            allowed_lateness_ms >= 0,
            "allowed lateness must be non-negative"
        );
        debug_assert_eq!(index.engine_kind(), "indexed_batch");
        Ok(Self {
            state,
            index,
            table,
            key_extractor,
            time_extractor,
            aggregator,
            watermark,
            output,
            window_size,
            window_slide,
            allowed_lateness_ms,
            dict_cache: HashMap::new(),
            aggregate_cache: None,
        })
    }

    fn windows_for(&self, ts: i64) -> Vec<(i64, i64)> {
        let slide = self.window_slide;
        let size = self.window_size;
        let latest_start = ts.div_euclid(slide) * slide;
        let count = (size / slide).max(1);
        let first_start = latest_start - (count - 1) * slide;
        let mut windows = Vec::with_capacity(count as usize);
        for i in 0..count {
            let start = first_start + i * slide;
            windows.push((start, start + size));
        }
        windows
    }

    fn watermark_cutoff(&self) -> Option<i64> {
        let watermark = self.watermark.load(Ordering::Relaxed);
        if watermark < 0 {
            return None;
        }
        let allowed = self.allowed_lateness_ms.max(0);
        Some(watermark.saturating_sub(allowed))
    }

    fn merge_output_delta(
        updates: &mut HashMap<(WindowKey<K>, A), i64>,
        key: WindowKey<K>,
        aggregate: A,
        weight: i64,
    ) {
        if weight == 0 {
            return;
        }
        let pair = (key, aggregate);
        let entry = updates.entry(pair.clone()).or_insert(0);
        *entry += weight;
        if *entry == 0 {
            updates.remove(&pair);
        }
    }

    async fn ensure_aggregate_cache(&mut self) -> Result<()> {
        if self.aggregate_cache.is_some() {
            return Ok(());
        }

        let materialized = self
            .state
            .integrated
            .materialize()
            .await
            .context("materialize window aggregate state")?;
        let mut cache = HashMap::new();
        for ((key, aggregate), weight) in materialized {
            if weight != 0 {
                cache.insert(key, aggregate);
            }
        }
        self.aggregate_cache = Some(cache);
        Ok(())
    }

    fn coalesce_deltas(&self, deltas: Vec<(V, i64)>) -> HashMap<V, i64>
    where
        V: Clone + Eq + Hash,
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

    async fn evict_expired_windows(
        &mut self,
        cutoff: Option<i64>,
        aggregate_updates: &mut HashMap<(WindowKey<K>, A), i64>,
    ) -> Result<()> {
        let Some(cutoff) = cutoff else {
            return Ok(());
        };
        self.ensure_aggregate_cache()
            .await
            .context("load window aggregate cache for eviction")?;
        let aggregate_cache = self
            .aggregate_cache
            .as_mut()
            .ok_or_else(|| anyhow!("missing window aggregate cache"))?;

        let expired_keys: Vec<WindowKey<K>> = aggregate_cache
            .keys()
            .filter(|key| key.end <= cutoff)
            .cloned()
            .collect();
        if expired_keys.is_empty() {
            return Ok(());
        }

        let mut index_updates = Vec::new();
        for key in expired_keys {
            let Some(old_value) = aggregate_cache.remove(&key) else {
                continue;
            };
            Self::merge_output_delta(aggregate_updates, key.clone(), old_value, -1);
            let values = self
                .index
                .values_for_key(&key)
                .await
                .context("load window aggregate values for eviction")?;
            for (row, weight) in values {
                if weight != 0 {
                    index_updates.push((key.clone(), row, -weight));
                }
            }
        }

        if !index_updates.is_empty() {
            self.index
                .apply_deltas(index_updates)
                .await
                .context("evict expired window aggregate index entries")?;
        }
        Ok(())
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
                .context("intern key while staging window aggregate delta")?;
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
            .context("schedule window aggregate update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write window aggregate update")?;

        let mut cleanup = WriteBatch::new();
        cleanup.delete(versioned.intent_key_bytes());
        versioned
            .table()
            .write_batch(cleanup)
            .await
            .context("clear window aggregate intent")?;

        versioned.apply_version_plan(&plan);
        Ok(versioned.handle_for_version(plan.version))
    }
}

#[async_trait]
impl<K, V, A> DeltaOperator for WindowAggregateOp<K, V, A>
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
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    A: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    A::Archived: RkyvDeserialize<A, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let delta_handle = inputs
            .first()
            .cloned()
            .context("window aggregate requires one input delta handle")?;

        let delta_values =
            delta_zset_handle::<V>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for window aggregate")?;
        let delta_map = self.coalesce_deltas(delta_values);
        let cutoff = self.watermark_cutoff();

        let mut keyed_deltas: HashMap<WindowKey<K>, Vec<(V, i64)>> = HashMap::new();
        let mut dropped_too_late = 0_u64;
        for (row, weight) in &delta_map {
            if *weight == 0 {
                continue;
            }
            let event_ts = match (self.time_extractor)(row) {
                Some(ts) => ts,
                None => continue,
            };
            if event_ts < 0 {
                continue;
            }
            if let Some(cutoff) = cutoff
                && event_ts < cutoff
            {
                dropped_too_late = dropped_too_late.saturating_add(weight.unsigned_abs());
                continue;
            }
            if let Some(key) = (self.key_extractor)(row) {
                for (window_start, window_end) in self.windows_for(event_ts) {
                    let window_key = WindowKey {
                        start: window_start,
                        end: window_end,
                        key: key.clone(),
                    };
                    keyed_deltas
                        .entry(window_key)
                        .or_default()
                        .push((row.clone(), *weight));
                }
            }
        }
        if dropped_too_late > 0 {
            WINDOW_DROPPED_TOO_LATE_TOTAL.inc_by(dropped_too_late);
        }

        let mut aggregate_updates: HashMap<(WindowKey<K>, A), i64> = HashMap::new();
        if !keyed_deltas.is_empty() {
            let affected_keys: HashSet<WindowKey<K>> = keyed_deltas.keys().cloned().collect();
            let mut index_updates = Vec::new();
            for (key, entries) in &keyed_deltas {
                for (row, weight) in entries {
                    index_updates.push((key.clone(), row.clone(), *weight));
                }
            }

            self.index
                .apply_deltas(index_updates)
                .await
                .context("update window aggregate index")?;

            self.ensure_aggregate_cache()
                .await
                .context("load window aggregate cache")?;

            let aggregate_cache = self
                .aggregate_cache
                .as_mut()
                .ok_or_else(|| anyhow!("missing window aggregate cache"))?;

            for key in affected_keys {
                let values = self
                    .index
                    .values_for_key(&key)
                    .await
                    .context("load window aggregate values")?;
                let new_value = (self.aggregator)(&key.key, &values);
                let old_value = aggregate_cache.get(&key).cloned();

                match (old_value, new_value) {
                    (Some(old), Some(new)) if old == new => {}
                    (Some(old), Some(new)) => {
                        Self::merge_output_delta(&mut aggregate_updates, key.clone(), old, -1);
                        Self::merge_output_delta(
                            &mut aggregate_updates,
                            key.clone(),
                            new.clone(),
                            1,
                        );
                        aggregate_cache.insert(key.clone(), new);
                    }
                    (Some(old), None) => {
                        Self::merge_output_delta(&mut aggregate_updates, key.clone(), old, -1);
                        aggregate_cache.remove(&key);
                    }
                    (None, Some(new)) => {
                        Self::merge_output_delta(
                            &mut aggregate_updates,
                            key.clone(),
                            new.clone(),
                            1,
                        );
                        aggregate_cache.insert(key.clone(), new);
                    }
                    (None, None) => {}
                }
            }
        }

        self.evict_expired_windows(cutoff, &mut aggregate_updates)
            .await
            .context("evict expired windows")?;

        if let Some(aggregate_cache) = self.aggregate_cache.as_ref() {
            WINDOW_STATE_ENTRIES.set(i64::try_from(aggregate_cache.len()).unwrap_or(i64::MAX));
        }

        if aggregate_updates.is_empty() {
            return Ok(None);
        }

        let base_version = self
            .state
            .integrated
            .current_handle()
            .map(|handle| handle.version);
        let new_integrated_handle = Self::apply_deltas_to_versioned(
            &mut self.state.integrated,
            &aggregate_updates,
            base_version,
        )
        .await
        .context("update window aggregate state")?;
        self.state.update_handle(new_integrated_handle);

        let delta_handle =
            Self::apply_deltas_to_versioned(&mut self.output, &aggregate_updates, None)
                .await
                .context("persist window aggregate output")?;
        Ok(Some(delta_handle))
    }
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::zset::{SegmentRecord, VersionedZSet};
    use crate::storage::dictionary::Dictionary;
    use crate::stream::runtime::DeltaOperator;
    use crate::stream::util::{compute_delta, materialize_zset_handle};
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicI64;

    type Row = i64;

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open("window_agg", store).await.expect("open SlateDB"))
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
                .expect("intern key for window test");
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

    #[tokio::test]
    async fn window_aggregate_groups_by_window() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));

        let input_dict = Arc::new(
            Dictionary::<Row>::with_table(table.clone(), "window_input", None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<(WindowKey<i64>, i64)>::with_table(table.clone(), "window_output", None)
                .await
                .expect("output dict"),
        );

        let state = RelationState::empty(table.clone(), "window_state".to_string())
            .await
            .expect("window state");
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "window_output".to_string(),
        )
        .await
        .expect("output zset");

        let index = IndexedBatchZSet::new(table.clone(), "window_index");
        let key_extractor = Arc::new(|row: &Row| Some(*row % 2));
        let time_extractor = Arc::new(|row: &Row| Some(*row));
        let aggregator: Arc<dyn Fn(&i64, &[(Row, i64)]) -> Option<i64> + Send + Sync> =
            Arc::new(|_key, values| {
                let mut count = 0i64;
                let mut has_rows = false;
                for (_row, weight) in values {
                    if *weight == 0 {
                        continue;
                    }
                    has_rows = true;
                    count += *weight;
                }
                if has_rows { Some(count) } else { None }
            });
        let watermark = Arc::new(AtomicI64::new(-1));

        let mut op = WindowAggregateOp::new(
            state,
            index,
            table.clone(),
            key_extractor,
            time_extractor,
            aggregator,
            output,
            2,
            2,
            0,
            watermark,
        )
        .expect("window aggregate op");

        let deltas: Vec<Vec<(Row, i64)>> = vec![
            vec![(1, 1), (2, 1)],
            vec![(3, 1)],
            vec![(4, 1), (1, -1)],
            vec![],
        ];

        let mut window_counts: HashMap<WindowKey<i64>, i64> = HashMap::new();
        let mut prev_output: HashMap<(WindowKey<i64>, i64), i64> = HashMap::new();

        let mut cache_out = HashMap::new();
        cache_out.insert("window_output".to_string(), output_dict.clone());

        for (step, delta) in deltas.iter().enumerate() {
            for (row, weight) in delta {
                for (start, end) in op.windows_for(*row) {
                    let key = WindowKey {
                        start,
                        end,
                        key: row % 2,
                    };
                    let entry = window_counts.entry(key.clone()).or_insert(0);
                    *entry += *weight;
                    if *entry == 0 {
                        window_counts.remove(&key);
                    }
                }
            }
            let mut aggregated = HashMap::new();
            for (key, count) in &window_counts {
                aggregated.insert((key.clone(), *count), 1);
            }

            let expected_delta: HashMap<(WindowKey<i64>, i64), i64> =
                compute_delta(&prev_output, &aggregated)
                    .into_iter()
                    .collect();

            let handle = if delta.is_empty() {
                ZSetHandle {
                    ns: "window_input".to_string(),
                    version: 0,
                }
            } else {
                stage_version(input_dict.clone(), table.clone(), "window_input", delta).await
            };

            let out_handle = op
                .on_step(step as i64, &[handle])
                .await
                .expect("window step");

            if expected_delta.is_empty() {
                assert!(out_handle.is_none(), "expected empty output at step {step}");
            } else {
                let out_handle = out_handle.expect("output handle");
                let materialized = materialize_zset_handle::<(WindowKey<i64>, i64)>(
                    table.clone(),
                    &mut cache_out,
                    &out_handle,
                )
                .await
                .expect("materialize output");
                assert_eq!(materialized, expected_delta, "step {step}");
            }

            prev_output = aggregated;
        }
    }

    #[tokio::test]
    async fn window_aggregate_respects_watermark_allowed_lateness_cutoff() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));

        let input_dict = Arc::new(
            Dictionary::<Row>::with_table(table.clone(), "window_late_input", None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<(WindowKey<i64>, i64)>::with_table(
                table.clone(),
                "window_late_output",
                None,
            )
            .await
            .expect("output dict"),
        );

        let state = RelationState::empty(table.clone(), "window_late_state".to_string())
            .await
            .expect("window state");
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "window_late_output".to_string(),
        )
        .await
        .expect("output zset");

        let index = IndexedBatchZSet::new(table.clone(), "window_late_index");
        let key_extractor = Arc::new(|_row: &Row| Some(0_i64));
        let time_extractor = Arc::new(|row: &Row| Some(*row));
        let aggregator: Arc<dyn Fn(&i64, &[(Row, i64)]) -> Option<i64> + Send + Sync> =
            Arc::new(|_key, values| {
                let mut count = 0i64;
                for (_row, weight) in values {
                    count += *weight;
                }
                (count != 0).then_some(count)
            });
        let watermark = Arc::new(AtomicI64::new(5_000));

        let mut op = WindowAggregateOp::new(
            state,
            index,
            table.clone(),
            key_extractor,
            time_extractor,
            aggregator,
            output,
            1_000,
            1_000,
            500,
            watermark,
        )
        .expect("window aggregate op");

        let handle = stage_version(
            input_dict,
            table.clone(),
            "window_late_input",
            &[(4_499, 1), (4_500, 1), (5_200, 1)],
        )
        .await;
        let out = op
            .on_step(1, &[handle])
            .await
            .expect("window step")
            .expect("non-empty output");

        let mut cache = HashMap::new();
        cache.insert("window_late_output".to_string(), output_dict);
        let materialized =
            materialize_zset_handle::<(WindowKey<i64>, i64)>(table.clone(), &mut cache, &out)
                .await
                .expect("materialize output");

        // 4499 is dropped (< watermark - allowed_lateness = 4500).
        assert_eq!(materialized.len(), 2);
        assert_eq!(
            materialized.get(&(
                WindowKey {
                    start: 4_000,
                    end: 5_000,
                    key: 0
                },
                1
            )),
            Some(&1)
        );
        assert_eq!(
            materialized.get(&(
                WindowKey {
                    start: 5_000,
                    end: 6_000,
                    key: 0
                },
                1
            )),
            Some(&1)
        );
    }

    #[tokio::test]
    async fn window_aggregate_accepts_out_of_order_events_within_lateness() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));

        let input_dict = Arc::new(
            Dictionary::<Row>::with_table(table.clone(), "window_ooo_input", None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<(WindowKey<i64>, i64)>::with_table(
                table.clone(),
                "window_ooo_output",
                None,
            )
            .await
            .expect("output dict"),
        );

        let state = RelationState::empty(table.clone(), "window_ooo_state".to_string())
            .await
            .expect("window state");
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "window_ooo_output".to_string(),
        )
        .await
        .expect("output zset");

        let index = IndexedBatchZSet::new(table.clone(), "window_ooo_index");
        let key_extractor = Arc::new(|_row: &Row| Some(0_i64));
        let time_extractor = Arc::new(|row: &Row| Some(*row));
        let aggregator: Arc<dyn Fn(&i64, &[(Row, i64)]) -> Option<i64> + Send + Sync> =
            Arc::new(|_key, values| {
                let mut count = 0i64;
                for (_row, weight) in values {
                    count += *weight;
                }
                (count != 0).then_some(count)
            });
        let watermark = Arc::new(AtomicI64::new(5_000));

        let mut op = WindowAggregateOp::new(
            state,
            index,
            table.clone(),
            key_extractor,
            time_extractor,
            aggregator,
            output,
            1_000,
            1_000,
            500,
            watermark,
        )
        .expect("window aggregate op");

        // 5_200 arrives before 4_600; both are >= watermark - allowed_lateness (4_500).
        let handle = stage_version(
            input_dict,
            table.clone(),
            "window_ooo_input",
            &[(5_200, 1), (4_600, 1)],
        )
        .await;
        let out = op
            .on_step(1, &[handle])
            .await
            .expect("window step")
            .expect("non-empty output");

        let mut cache = HashMap::new();
        cache.insert("window_ooo_output".to_string(), output_dict);
        let materialized =
            materialize_zset_handle::<(WindowKey<i64>, i64)>(table.clone(), &mut cache, &out)
                .await
                .expect("materialize output");

        assert_eq!(materialized.len(), 2);
        assert_eq!(
            materialized.get(&(
                WindowKey {
                    start: 4_000,
                    end: 5_000,
                    key: 0
                },
                1
            )),
            Some(&1)
        );
        assert_eq!(
            materialized.get(&(
                WindowKey {
                    start: 5_000,
                    end: 6_000,
                    key: 0
                },
                1
            )),
            Some(&1)
        );
    }

    #[tokio::test]
    async fn window_aggregate_ignores_too_late_retractions_after_window_close() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));

        let input_dict = Arc::new(
            Dictionary::<Row>::with_table(table.clone(), "window_retract_input", None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<(WindowKey<i64>, i64)>::with_table(
                table.clone(),
                "window_retract_output",
                None,
            )
            .await
            .expect("output dict"),
        );

        let state = RelationState::empty(table.clone(), "window_retract_state".to_string())
            .await
            .expect("window state");
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "window_retract_output".to_string(),
        )
        .await
        .expect("output zset");

        let index = IndexedBatchZSet::new(table.clone(), "window_retract_index");
        let key_extractor = Arc::new(|_row: &Row| Some(0_i64));
        let time_extractor = Arc::new(|row: &Row| Some(*row));
        let aggregator: Arc<dyn Fn(&i64, &[(Row, i64)]) -> Option<i64> + Send + Sync> =
            Arc::new(|_key, values| {
                let mut count = 0i64;
                for (_row, weight) in values {
                    count += *weight;
                }
                (count != 0).then_some(count)
            });
        let watermark = Arc::new(AtomicI64::new(-1));

        let mut op = WindowAggregateOp::new(
            state,
            index,
            table.clone(),
            key_extractor,
            time_extractor,
            aggregator,
            output,
            1_000,
            1_000,
            0,
            Arc::clone(&watermark),
        )
        .expect("window aggregate op");

        let first = stage_version(
            input_dict.clone(),
            table.clone(),
            "window_retract_input",
            &[(1_000, 1)],
        )
        .await;
        let out1 = op
            .on_step(1, &[first])
            .await
            .expect("window step")
            .expect("non-empty output");
        let mut cache = HashMap::new();
        cache.insert("window_retract_output".to_string(), output_dict.clone());
        let out1_materialized =
            materialize_zset_handle::<(WindowKey<i64>, i64)>(table.clone(), &mut cache, &out1)
                .await
                .expect("materialize output");
        assert_eq!(out1_materialized.len(), 1);

        // Advance watermark so event timestamp 1000 is now too late.
        watermark.store(3_000, Ordering::Relaxed);
        let retract = stage_version(
            input_dict,
            table.clone(),
            "window_retract_input",
            &[(1_000, -1)],
        )
        .await;
        let out2 = op
            .on_step(2, &[retract])
            .await
            .expect("window step")
            .expect("eviction output");
        let out2_materialized =
            materialize_zset_handle::<(WindowKey<i64>, i64)>(table.clone(), &mut cache, &out2)
                .await
                .expect("materialize output");
        assert_eq!(
            out2_materialized.get(&(
                WindowKey {
                    start: 1_000,
                    end: 2_000,
                    key: 0
                },
                1
            )),
            Some(&-1)
        );
    }

    #[tokio::test]
    async fn window_aggregate_evicts_expired_windows_on_watermark_advance() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(crate::storage::SlateTable::new(db.clone()));

        let input_dict = Arc::new(
            Dictionary::<Row>::with_table(table.clone(), "window_evict_input", None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<(WindowKey<i64>, i64)>::with_table(
                table.clone(),
                "window_evict_output",
                None,
            )
            .await
            .expect("output dict"),
        );

        let state = RelationState::empty(table.clone(), "window_evict_state".to_string())
            .await
            .expect("window state");
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "window_evict_output".to_string(),
        )
        .await
        .expect("output zset");
        let index = IndexedBatchZSet::new(table.clone(), "window_evict_index");
        let key_extractor = Arc::new(|_row: &Row| Some(0_i64));
        let time_extractor = Arc::new(|row: &Row| Some(*row));
        let aggregator: Arc<dyn Fn(&i64, &[(Row, i64)]) -> Option<i64> + Send + Sync> =
            Arc::new(|_key, values| {
                let mut count = 0_i64;
                for (_row, weight) in values {
                    count += *weight;
                }
                (count != 0).then_some(count)
            });
        let watermark = Arc::new(AtomicI64::new(-1));

        let mut op = WindowAggregateOp::new(
            state,
            index,
            table.clone(),
            key_extractor,
            time_extractor,
            aggregator,
            output,
            1_000,
            1_000,
            0,
            Arc::clone(&watermark),
        )
        .expect("window aggregate op");

        let first = stage_version(
            input_dict.clone(),
            table.clone(),
            "window_evict_input",
            &[(1_000, 1)],
        )
        .await;
        let _ = op
            .on_step(1, &[first])
            .await
            .expect("window step")
            .expect("non-empty output");

        watermark.store(3_000, Ordering::Relaxed);
        let empty_handle = ZSetHandle {
            ns: "window_evict_input".to_string(),
            version: 0,
        };
        let out = op
            .on_step(2, &[empty_handle])
            .await
            .expect("window step")
            .expect("eviction output");

        let mut cache = HashMap::new();
        cache.insert("window_evict_output".to_string(), output_dict);
        let materialized =
            materialize_zset_handle::<(WindowKey<i64>, i64)>(table.clone(), &mut cache, &out)
                .await
                .expect("materialize output");

        assert_eq!(
            materialized.get(&(
                WindowKey {
                    start: 1_000,
                    end: 2_000,
                    key: 0
                },
                1
            )),
            Some(&-1)
        );
    }
}
