use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use anyhow::{Context, Result, anyhow, ensure};
use async_trait::async_trait;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;
use tokio::sync::Mutex as AsyncMutex;

use crate::algebra::AbelianGroup;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::operators::window::{
    WINDOW_DROPPED_TOO_LATE_TOTAL, WINDOW_STATE_ENTRIES, WINDOW_STATE_LIMIT,
    WINDOW_STATE_LIMIT_EXCEEDED_TOTAL, WindowKey,
};
use crate::relation_state::RelationState;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::DeltaHandleStream;
use crate::stream::runtime::{
    DeltaOperator, HandleOperatorRuntime, RuntimeErrorHandler, report_runtime_error,
};
use crate::stream::util::{
    build_exact_stream_from_values, collect_values, delta_zset_handle, publish_scheduled_value,
    push_value_in_place,
};

pub struct DbspWindowCountStarAggregate {
    stream: DeltaHandleStream,
}

type RowExtractor<V, K> = Arc<dyn Fn(&V) -> Option<(K, i64)> + Send + Sync>;

struct WindowCountStarAggregateOp<K, V>
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
{
    table: Arc<dyn KeyValueTable>,
    dict_cache: HashMap<String, Arc<Dictionary<V>>>,
    row_extractor: RowExtractor<V, K>,
    state: RelationState<(WindowKey<K>, i64)>,
    output: VersionedZSet<(WindowKey<K>, i64)>,
    state_cache: Option<HashMap<WindowKey<K>, i64>>,
    eviction_schedule: BTreeMap<i64, Vec<WindowKey<K>>>,
    watermark: Arc<AtomicI64>,
    window_size: i64,
    window_slide: i64,
    allowed_lateness_ms: i64,
}

impl<K, V> WindowCountStarAggregateOp<K, V>
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
{
    fn for_each_window<F>(&self, ts: i64, mut visit: F)
    where
        F: FnMut(i64, i64),
    {
        if self.window_size == self.window_slide {
            let start = ts.div_euclid(self.window_slide) * self.window_slide;
            visit(start, start + self.window_size);
            return;
        }

        let latest_start = ts.div_euclid(self.window_slide) * self.window_slide;
        let count = (self.window_size / self.window_slide).max(1);
        let first_start = latest_start - (count - 1) * self.window_slide;
        for i in 0..count {
            let start = first_start + i * self.window_slide;
            visit(start, start + self.window_size);
        }
    }

    fn watermark_cutoff(&self) -> Option<i64> {
        let watermark = self.watermark.load(Ordering::Relaxed);
        if watermark < 0 {
            return None;
        }
        Some(watermark.saturating_sub(self.allowed_lateness_ms.max(0)))
    }

    async fn ensure_state_cache(&mut self) -> Result<()> {
        if self.state_cache.is_some() {
            return Ok(());
        }

        let materialized = self
            .state
            .integrated
            .materialize()
            .await
            .context("materialize window count-star state")?;
        let mut cache = HashMap::new();
        let mut eviction_schedule: BTreeMap<i64, Vec<WindowKey<K>>> = BTreeMap::new();
        for ((key, count), weight) in materialized {
            if weight != 0 {
                cache.insert(key.clone(), count);
                eviction_schedule.entry(key.end).or_default().push(key);
            }
        }
        self.state_cache = Some(cache);
        self.eviction_schedule = eviction_schedule;
        Ok(())
    }

    fn merge_count_delta(
        updates: &mut HashMap<(WindowKey<K>, i64), i64>,
        key: WindowKey<K>,
        count: i64,
        diff: i64,
    ) {
        if diff == 0 {
            return;
        }
        let pair = (key, count);
        let entry = updates.entry(pair.clone()).or_insert(0);
        *entry += diff;
        if *entry == 0 {
            updates.remove(&pair);
        }
    }

    async fn apply_deltas_to_versioned<T>(
        versioned: &mut VersionedZSet<T>,
        deltas: &HashMap<T, i64>,
        base: Option<u64>,
        context_label: &'static str,
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
        let mut keyed_deltas = Vec::new();
        for (key, delta) in deltas {
            if *delta == 0 {
                continue;
            }
            keyed_deltas.push((key, *delta));
        }
        let ids = dict
            .intern_many_values_unique(keyed_deltas.iter().map(|(key, _)| *key))
            .await
            .with_context(|| format!("batch intern keys while staging {context_label}"))?;
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

        let mut batch = WriteBatch::new();
        let plan = versioned
            .enqueue_version_with_base(segments, base, 0, &mut batch)
            .await
            .with_context(|| format!("schedule {context_label}"))?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .with_context(|| format!("write {context_label}"))?;

        versioned.apply_version_plan(&plan);
        Ok(versioned.handle_for_version(plan.version))
    }

    async fn evict_expired_windows(
        &mut self,
        cutoff: Option<i64>,
        updates: &mut HashMap<(WindowKey<K>, i64), i64>,
    ) -> Result<()> {
        let Some(cutoff) = cutoff else {
            return Ok(());
        };
        self.ensure_state_cache().await?;
        let (state_cache, eviction_schedule) =
            match (&mut self.state_cache, &mut self.eviction_schedule) {
                (Some(state_cache), eviction_schedule) => (state_cache, eviction_schedule),
                (None, _) => return Err(anyhow!("missing window count-star state cache")),
            };

        let retained = eviction_schedule.split_off(&(cutoff + 1));
        let expired = std::mem::replace(eviction_schedule, retained);
        for (_, keys) in expired {
            for key in keys {
                let Some(old_count) = state_cache.remove(&key) else {
                    continue;
                };
                Self::merge_count_delta(updates, key, old_count, -1);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<K, V> DeltaOperator for WindowCountStarAggregateOp<K, V>
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
{
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let delta_handle = inputs
            .first()
            .cloned()
            .context("window count-star aggregate requires one input delta handle")?;

        let delta_values =
            delta_zset_handle::<V>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for window count-star aggregate")?;
        let cutoff = self.watermark_cutoff();

        let mut grouped_deltas = HashMap::with_capacity(delta_values.len());
        let mut dropped_too_late = 0_u64;
        for (row, weight) in delta_values {
            if weight == 0 {
                continue;
            }
            let Some((key, event_ts)) = (self.row_extractor)(&row) else {
                continue;
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
            self.for_each_window(event_ts, |window_start, window_end| {
                let window_key = WindowKey {
                    start: window_start,
                    end: window_end,
                    key: key.clone(),
                };
                match grouped_deltas.entry(window_key) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        let next = *entry.get() + weight;
                        if next == 0 {
                            entry.remove();
                        } else {
                            *entry.get_mut() = next;
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(weight);
                    }
                }
            });
        }
        if dropped_too_late > 0 {
            WINDOW_DROPPED_TOO_LATE_TOTAL.inc_by(dropped_too_late);
        }

        self.ensure_state_cache().await?;
        let (state_cache, eviction_schedule) =
            match (&mut self.state_cache, &mut self.eviction_schedule) {
                (Some(state_cache), eviction_schedule) => (state_cache, eviction_schedule),
                (None, _) => return Err(anyhow!("missing window count-star state cache")),
            };
        let mut updates = HashMap::new();
        for (key, delta) in grouped_deltas {
            if delta == 0 {
                continue;
            }
            let old_count = state_cache.get(&key).copied().unwrap_or(0);
            let new_count = old_count.saturating_add(delta);
            if old_count == new_count {
                continue;
            }
            if old_count != 0 {
                Self::merge_count_delta(&mut updates, key.clone(), old_count, -1);
            }
            if new_count != 0 {
                Self::merge_count_delta(&mut updates, key.clone(), new_count, 1);
                if old_count == 0 {
                    eviction_schedule
                        .entry(key.end)
                        .or_default()
                        .push(key.clone());
                }
                state_cache.insert(key, new_count);
            } else {
                state_cache.remove(&key);
            }
        }

        self.evict_expired_windows(cutoff, &mut updates)
            .await
            .context("evict expired count-star windows")?;

        let state_entries = self
            .state_cache
            .as_ref()
            .map(|cache| cache.len())
            .unwrap_or(0);
        WINDOW_STATE_ENTRIES.set(i64::try_from(state_entries).unwrap_or(i64::MAX));
        if let Some(limit) = *WINDOW_STATE_LIMIT
            && state_entries > limit
        {
            WINDOW_STATE_LIMIT_EXCEEDED_TOTAL.inc();
            tracing::warn!(
                current_entries = state_entries,
                limit,
                "window aggregate state exceeds configured limit"
            );
        }

        if updates.is_empty() {
            return Ok(Some(self.output.handle_for_version(0)));
        }

        let base_version = self
            .state
            .integrated
            .current_handle()
            .map(|handle| handle.version);
        let new_integrated_handle = Self::apply_deltas_to_versioned(
            &mut self.state.integrated,
            &updates,
            base_version,
            "window count-star state update",
        )
        .await
        .context("update window count-star state")?;
        self.state.update_handle(new_integrated_handle);

        let delta_handle = Self::apply_deltas_to_versioned(
            &mut self.output,
            &updates,
            None,
            "window count-star output update",
        )
        .await
        .context("persist window count-star output")?;
        Ok(Some(delta_handle))
    }
}

impl DbspWindowCountStarAggregate {
    #[allow(clippy::too_many_arguments)]
    pub async fn new<K, V, FRow>(
        input: &DeltaHandleStream,
        row_extractor: FRow,
        window_size: i64,
        window_slide: i64,
        allowed_lateness_ms: i64,
        watermark: Arc<AtomicI64>,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> anyhow::Result<Self>
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
        FRow: Fn(&V) -> Option<(K, i64)> + Send + Sync + 'static,
    {
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

        let table = input.table();
        let frontier = input.current_time();
        let horizon = input.semantic_horizon();
        let aggregate_id = NEXT_WINDOW_COUNT_STAR_AGGREGATE_ID.fetch_add(1, Ordering::Relaxed);
        let output_ns = format!("window_count_star_output_{aggregate_id}");
        let empty_handle = ZSetHandle {
            ns: output_ns.clone(),
            version: 0,
        };

        let state = RelationState::<(WindowKey<K>, i64)>::empty(
            table.clone(),
            format!("window_count_star_state_{aggregate_id}"),
        )
        .await?;
        let output_dict = Arc::new(
            Dictionary::<(WindowKey<K>, i64)>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .context("create output dictionary for window count-star aggregate")?,
        );
        let output = VersionedZSet::new(output_dict, table.clone(), output_ns.clone())
            .await
            .context("create output zset for window count-star aggregate")?;

        let window_op = Arc::new(AsyncMutex::new(WindowCountStarAggregateOp {
            table: table.clone(),
            dict_cache: HashMap::new(),
            row_extractor: Arc::new(row_extractor),
            state,
            output,
            state_cache: None,
            eviction_schedule: BTreeMap::new(),
            watermark,
            window_size,
            window_slide,
            allowed_lateness_ms,
        }));

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(ZSetHandleGroup {
            default: empty_handle.clone(),
        });

        let history = collect_values(input, horizon).await?;
        let mut output_handles = Vec::with_capacity(history.len());
        for handle in history {
            let out_handle = {
                let mut op_guard = window_op.lock().await;
                op_guard.on_step(0, std::slice::from_ref(&handle)).await?
            }
            .unwrap_or_else(|| empty_handle.clone());
            output_handles.push(out_handle);
        }

        let mut stream = build_exact_stream_from_values(
            table.clone(),
            handle_group,
            "window_count_star_output_stream/",
            frontier,
            horizon,
            &output_handles,
            empty_handle.clone(),
        )
        .await?;
        stream.flush().await?;

        let writer = Arc::new(AsyncMutex::new(stream.clone()));
        let mut runtime = HandleOperatorRuntime::new(vec![input.stream()], move |ts, handles| {
            let op = Arc::clone(&window_op);
            let writer = Arc::clone(&writer);
            let empty_handle = empty_handle.clone();
            let handles_vec = handles.to_vec();
            Box::pin(async move {
                if handles_vec.len() != 1 {
                    return Err(anyhow!(
                        "window count-star aggregate runtime expected 1 handle, got {}",
                        handles_vec.len()
                    ));
                }
                if ts <= horizon {
                    let mut writer_guard = writer.lock().await;
                    publish_scheduled_value(&mut writer_guard, ts).await?;
                    return Ok(());
                }
                let mut op_guard = op.lock().await;
                let out_handle = op_guard
                    .on_step(ts, &handles_vec)
                    .await?
                    .unwrap_or_else(|| empty_handle.clone());
                let mut writer_guard = writer.lock().await;
                push_value_in_place(&mut writer_guard, out_handle);
                writer_guard.flush().await?;
                Ok(())
            })
        });

        let error_handler = error_handler.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = runtime.step().await {
                    report_runtime_error(&error_handler, "window_count_star_aggregate", err);
                    break;
                }
            }
        });

        Ok(Self {
            stream: DeltaHandleStream::new(stream),
        })
    }

    pub fn stream(&self) -> DeltaHandleStream {
        self.stream.clone()
    }
}

#[derive(Clone)]
struct ZSetHandleGroup {
    default: ZSetHandle,
}

#[async_trait]
impl AbelianGroup<ZSetHandle> for ZSetHandleGroup {
    async fn add(&self, a: &ZSetHandle, _b: &ZSetHandle) -> ZSetHandle {
        a.clone()
    }

    async fn neg(&self, a: &ZSetHandle) -> ZSetHandle {
        a.clone()
    }

    async fn identity(&self) -> ZSetHandle {
        self.default.clone()
    }
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

static NEXT_WINDOW_COUNT_STAR_AGGREGATE_ID: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::zset::SegmentRecord;
    use crate::storage::SlateTable;
    use crate::stream::util::materialize_zset_handle;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::BTreeMap;
    use std::sync::atomic::Ordering;

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(
            Db::open("window_count_star_aggregate", store)
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
                .expect("intern key for window count-star test");
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
        if segments.is_empty() {
            return versioned.handle_for_version(0);
        }
        let version = versioned
            .create_version_with_base(segments, None)
            .await
            .expect("create version");
        versioned.handle_for_version(version)
    }

    #[tokio::test]
    async fn window_count_star_operator_tracks_signed_counts_and_eviction() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let state = RelationState::<(WindowKey<i64>, i64)>::empty(
            table.clone(),
            "window_count_star_state_test".to_string(),
        )
        .await
        .expect("state");
        let output_dict = Arc::new(
            Dictionary::<(WindowKey<i64>, i64)>::with_table(
                table.clone(),
                "window_count_star_output_test".to_string(),
                None,
            )
            .await
            .expect("output dict"),
        );
        let output = VersionedZSet::new(
            output_dict,
            table.clone(),
            "window_count_star_output_test".to_string(),
        )
        .await
        .expect("output zset");
        let watermark = Arc::new(AtomicI64::new(-1));
        let mut op = WindowCountStarAggregateOp {
            table: table.clone(),
            dict_cache: HashMap::new(),
            row_extractor: Arc::new(|row: &i64| Some((7_i64, *row))),
            state,
            output,
            state_cache: None,
            eviction_schedule: BTreeMap::new(),
            watermark: Arc::clone(&watermark),
            window_size: 1_000,
            window_slide: 1_000,
            allowed_lateness_ms: 0,
        };

        let input_dict = Arc::new(
            Dictionary::<i64>::with_table(
                table.clone(),
                "window_count_star_input_test".to_string(),
                None,
            )
            .await
            .expect("input dict"),
        );
        let handle1 = stage_version(
            input_dict.clone(),
            table.clone(),
            "window_count_star_input_test",
            &[(1_000_i64, 1)],
        )
        .await;
        let mut cache = HashMap::new();
        let step_one = materialize_zset_handle::<(WindowKey<i64>, i64)>(
            table.clone(),
            &mut cache,
            &op.on_step(1, &[handle1])
                .await
                .expect("run t1")
                .expect("t1 output"),
        )
        .await
        .expect("materialize t1");
        assert_eq!(
            step_one,
            HashMap::from([(
                (
                    WindowKey {
                        start: 1_000,
                        end: 2_000,
                        key: 7,
                    },
                    1,
                ),
                1,
            )])
        );

        let handle2 = stage_version(
            input_dict.clone(),
            table.clone(),
            "window_count_star_input_test",
            &[(1_000_i64, -2)],
        )
        .await;
        let step_two = materialize_zset_handle::<(WindowKey<i64>, i64)>(
            table.clone(),
            &mut cache,
            &op.on_step(2, &[handle2])
                .await
                .expect("run t2")
                .expect("t2 output"),
        )
        .await
        .expect("materialize t2");
        assert_eq!(
            step_two,
            HashMap::from([
                (
                    (
                        WindowKey {
                            start: 1_000,
                            end: 2_000,
                            key: 7
                        },
                        1
                    ),
                    -1
                ),
                (
                    (
                        WindowKey {
                            start: 1_000,
                            end: 2_000,
                            key: 7
                        },
                        -1
                    ),
                    1
                ),
            ])
        );

        watermark.store(2_000, Ordering::Relaxed);
        let handle3 = stage_version(
            input_dict,
            table.clone(),
            "window_count_star_input_test",
            &[],
        )
        .await;
        let step_three = materialize_zset_handle::<(WindowKey<i64>, i64)>(
            table.clone(),
            &mut cache,
            &op.on_step(3, &[handle3])
                .await
                .expect("run t3")
                .expect("t3 output"),
        )
        .await
        .expect("materialize t3");
        assert_eq!(
            step_three,
            HashMap::from([(
                (
                    WindowKey {
                        start: 1_000,
                        end: 2_000,
                        key: 7
                    },
                    -1
                ),
                -1
            )])
        );
    }
}
