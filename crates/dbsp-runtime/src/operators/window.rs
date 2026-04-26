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
type BatchWindowExtractor<V, K> = Arc<dyn Fn(&[(V, i64)]) -> Vec<(V, i64, K, i64)> + Send + Sync>;
type Aggregator<K, V, A> = Arc<dyn Fn(&K, &[(V, i64)]) -> Option<A> + Send + Sync>;

pub(crate) static WINDOW_DROPPED_TOO_LATE_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "floe_window_events_dropped_too_late_total",
        "Number of input rows dropped by window operators because they arrived beyond allowed lateness",
    )
    .expect("register floe_window_events_dropped_too_late_total")
});

pub(crate) static WINDOW_STATE_ENTRIES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "floe_window_state_entries",
        "Approximate number of active window aggregate entries currently retained",
    )
    .expect("register floe_window_state_entries")
});

pub(crate) static WINDOW_STATE_LIMIT_EXCEEDED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "floe_window_state_limit_exceeded_total",
        "Number of times window aggregate state exceeded configured FLOE_WINDOW_STATE_MAX_ENTRIES limit",
    )
    .expect("register floe_window_state_limit_exceeded_total")
});

pub(crate) static WINDOW_STATE_LIMIT: LazyLock<Option<usize>> = LazyLock::new(|| {
    std::env::var("FLOE_WINDOW_STATE_MAX_ENTRIES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
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
    pub window_extractor: BatchWindowExtractor<V, K>,
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
        let window_extractor = Arc::new(move |delta_values: &[(V, i64)]| {
            delta_values
                .iter()
                .filter_map(|(row, weight)| {
                    let event_ts = time_extractor(row)?;
                    let key = key_extractor(row)?;
                    Some((row.clone(), *weight, key, event_ts))
                })
                .collect()
        });
        Self::new_with_batch_extractor(
            state,
            index,
            table,
            window_extractor,
            aggregator,
            output,
            window_size,
            window_slide,
            allowed_lateness_ms,
            watermark,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_batch_extractor(
        state: RelationState<(WindowKey<K>, A)>,
        index: IndexedBatchZSet<WindowKey<K>, V>,
        table: Arc<dyn KeyValueTable>,
        window_extractor: BatchWindowExtractor<V, K>,
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
            window_extractor,
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

    pub fn enable_live_output_replayable(&mut self) {
        self.output.enable_replayable_persistence();
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
        let staged = deltas
            .iter()
            .filter_map(|(key, delta)| (*delta != 0).then_some((key.clone(), *delta)))
            .collect::<Vec<_>>();
        if staged.is_empty() {
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
            return Ok(versioned.publish_replayable_batch(Arc::new(staged)));
        }

        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let dict = versioned.dictionary();
        let mut dict_batch = dict.batch();
        for (key, delta) in staged {
            let id = dict_batch
                .intern(&key)
                .await
                .context("intern key while staging window aggregate delta")?;
            buckets.entry(bucket_for(id)).or_default().push((id, delta));
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
        let delta_rows = delta_map
            .iter()
            .map(|(row, weight)| (row.clone(), *weight))
            .collect::<Vec<_>>();
        for (row, weight, key, event_ts) in (self.window_extractor)(&delta_rows) {
            if weight == 0 {
                continue;
            }
            if event_ts < 0 {
                continue;
            }
            if let Some(cutoff) = cutoff
                && event_ts < cutoff
            {
                dropped_too_late = dropped_too_late.saturating_add(weight.unsigned_abs());
                continue;
            }
            for (window_start, window_end) in self.windows_for(event_ts) {
                let window_key = WindowKey {
                    start: window_start,
                    end: window_end,
                    key: key.clone(),
                };
                keyed_deltas
                    .entry(window_key)
                    .or_default()
                    .push((row.clone(), weight));
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
            if let Some(limit) = *WINDOW_STATE_LIMIT
                && aggregate_cache.len() > limit
            {
                WINDOW_STATE_LIMIT_EXCEEDED_TOTAL.inc();
                tracing::warn!(
                    current_entries = aggregate_cache.len(),
                    limit,
                    "window aggregate state exceeds configured limit"
                );
            }
        }

        if aggregate_updates.is_empty() {
            return Ok(Some(self.output.handle_for_version(0)));
        }

        let base_version = self.state.base_version_for_update();
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
mod tests;
