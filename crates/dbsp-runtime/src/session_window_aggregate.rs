use std::collections::{BTreeMap, HashMap, HashSet};
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
use crate::collections::{DEFAULT_HOT_KEY_COMPACTION_THRESHOLD, IndexedBatchZSet};
use crate::handles::ZSetHandle;
use crate::metrics;
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

type BatchSessionExtractor<V, K> = Arc<dyn Fn(&[(V, i64)]) -> Vec<(V, i64, K, i64)> + Send + Sync>;
type Aggregator<K, V, A> = Arc<dyn Fn(&K, &[(V, i64)]) -> Option<A> + Send + Sync>;

pub struct DbspSessionWindowAggregate {
    stream: DeltaHandleStream,
}

struct ComputedSession<K, V, A> {
    window_key: WindowKey<K>,
    rows: Vec<(V, i64)>,
    aggregate: Option<A>,
}

struct SessionWindowAggregateOp<K, V, A>
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
    table: Arc<dyn KeyValueTable>,
    dict_cache: HashMap<String, Arc<Dictionary<V>>>,
    row_extractor: BatchSessionExtractor<V, K>,
    aggregator: Aggregator<K, V, A>,
    input_index: IndexedBatchZSet<K, V>,
    state: RelationState<(WindowKey<K>, A)>,
    output: VersionedZSet<(WindowKey<K>, A)>,
    session_cache: Option<HashMap<K, HashMap<WindowKey<K>, A>>>,
    watermark: Arc<AtomicI64>,
    gap_ms: i64,
    allowed_lateness_ms: i64,
    logical_work: metrics::LogicalWorkCollector,
}

impl<K, V, A> SessionWindowAggregateOp<K, V, A>
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
    fn watermark_cutoff(&self) -> Option<i64> {
        let watermark = self.watermark.load(Ordering::Relaxed);
        if watermark < 0 {
            return None;
        }
        Some(watermark.saturating_sub(self.allowed_lateness_ms.max(0)))
    }

    fn coalesce_deltas(&self, deltas: Vec<(V, i64)>) -> HashMap<V, i64> {
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

    async fn ensure_session_cache(&mut self) -> Result<usize> {
        if self.session_cache.is_some() {
            return Ok(0);
        }

        let materialized = self
            .state
            .integrated
            .materialize()
            .await
            .context("materialize session window aggregate state")?;
        let rebuild_rows = materialized.len();
        let mut cache: HashMap<K, HashMap<WindowKey<K>, A>> = HashMap::new();
        for ((window_key, aggregate), weight) in materialized {
            if weight != 0 {
                cache
                    .entry(window_key.key.clone())
                    .or_default()
                    .insert(window_key, aggregate);
            }
        }
        self.session_cache = Some(cache);
        Ok(rebuild_rows)
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

    fn compute_sessions(&self, key: &K, values: Vec<(V, i64)>) -> Vec<ComputedSession<K, V, A>> {
        let delta_rows = values
            .iter()
            .map(|(row, weight)| (row.clone(), *weight))
            .collect::<Vec<_>>();
        let mut timestamped = (self.row_extractor)(&delta_rows)
            .into_iter()
            .filter_map(|(row, weight, extracted_key, event_ts)| {
                (weight != 0 && event_ts >= 0 && &extracted_key == key)
                    .then_some((event_ts, row, weight))
            })
            .collect::<Vec<_>>();
        timestamped.sort_by_key(|(event_ts, _, _)| *event_ts);

        let mut sessions = Vec::new();
        let mut current_start: Option<i64> = None;
        let mut current_end = 0_i64;
        let mut current_rows: Vec<(V, i64)> = Vec::new();

        for (event_ts, row, weight) in timestamped {
            let event_end = event_ts.saturating_add(self.gap_ms);
            match current_start {
                None => {
                    current_start = Some(event_ts);
                    current_end = event_end;
                    current_rows.push((row, weight));
                }
                Some(start) if event_ts < current_end => {
                    current_end = current_end.max(event_end);
                    current_rows.push((row, weight));
                    current_start = Some(start);
                }
                Some(start) => {
                    sessions.push(self.finish_session(key, start, current_end, current_rows));
                    current_start = Some(event_ts);
                    current_end = event_end;
                    current_rows = vec![(row, weight)];
                }
            }
        }

        if let Some(start) = current_start {
            sessions.push(self.finish_session(key, start, current_end, current_rows));
        }
        sessions
    }

    fn finish_session(
        &self,
        key: &K,
        start: i64,
        end: i64,
        rows: Vec<(V, i64)>,
    ) -> ComputedSession<K, V, A> {
        let window_key = WindowKey {
            start,
            end,
            key: key.clone(),
        };
        let aggregate = (self.aggregator)(key, &rows);
        ComputedSession {
            window_key,
            rows,
            aggregate,
        }
    }

    async fn recompute_group(
        &mut self,
        key: &K,
        aggregate_updates: &mut HashMap<(WindowKey<K>, A), i64>,
        work: &mut metrics::LogicalWorkSnapshot,
    ) -> Result<()> {
        self.ensure_session_cache().await?;
        let old_sessions = self
            .session_cache
            .as_mut()
            .ok_or_else(|| anyhow!("missing session window aggregate cache"))?
            .remove(key)
            .unwrap_or_default();

        for (window_key, aggregate) in old_sessions {
            Self::merge_output_delta(aggregate_updates, window_key, aggregate, -1);
        }

        let (values, lookup_metrics) = self
            .input_index
            .values_for_key_with_metrics(key)
            .await
            .context("load session window aggregate input values")?;
        work.add_lookup_metrics(lookup_metrics);
        work.window_rows_examined = work
            .window_rows_examined
            .saturating_add(values.len() as u64);

        let mut new_cache = HashMap::new();
        for session in self.compute_sessions(key, values) {
            if let Some(aggregate) = session.aggregate {
                Self::merge_output_delta(
                    aggregate_updates,
                    session.window_key.clone(),
                    aggregate.clone(),
                    1,
                );
                new_cache.insert(session.window_key, aggregate);
            }
        }

        if !new_cache.is_empty() {
            self.session_cache
                .as_mut()
                .ok_or_else(|| anyhow!("missing session window aggregate cache"))?
                .insert(key.clone(), new_cache);
        }
        Ok(())
    }

    async fn evict_expired_sessions(
        &mut self,
        cutoff: Option<i64>,
        aggregate_updates: &mut HashMap<(WindowKey<K>, A), i64>,
        work: &mut metrics::LogicalWorkSnapshot,
    ) -> Result<()> {
        let Some(cutoff) = cutoff else {
            return Ok(());
        };
        let cache_rebuild_rows = self
            .ensure_session_cache()
            .await
            .context("load session window aggregate cache for eviction")?;
        if cache_rebuild_rows != 0 {
            work.cache_rebuild_rows = work
                .cache_rebuild_rows
                .saturating_add(cache_rebuild_rows as u64);
            work.state_full_scan_count = work.state_full_scan_count.saturating_add(1);
            work.state_scan_rows = work
                .state_scan_rows
                .saturating_add(cache_rebuild_rows as u64);
        }

        let expired_groups = self
            .session_cache
            .as_ref()
            .ok_or_else(|| anyhow!("missing session window aggregate cache"))?
            .iter()
            .filter_map(|(key, sessions)| {
                sessions
                    .keys()
                    .any(|window_key| window_key.end <= cutoff)
                    .then_some(key.clone())
            })
            .collect::<Vec<_>>();
        if expired_groups.is_empty() {
            return Ok(());
        }

        let mut input_updates = Vec::new();
        for key in expired_groups {
            let (values, lookup_metrics) = self
                .input_index
                .values_for_key_with_metrics(&key)
                .await
                .context("load session window aggregate input values for eviction")?;
            work.add_lookup_metrics(lookup_metrics);
            work.window_rows_examined = work
                .window_rows_examined
                .saturating_add(values.len() as u64);

            let sessions = self.compute_sessions(&key, values);
            let mut retained = HashMap::new();
            for session in sessions {
                if session.window_key.end <= cutoff {
                    if let Some(aggregate) = session.aggregate {
                        Self::merge_output_delta(
                            aggregate_updates,
                            session.window_key.clone(),
                            aggregate,
                            -1,
                        );
                    }
                    for (row, weight) in session.rows {
                        if weight != 0 {
                            input_updates.push((key.clone(), row, -weight));
                        }
                    }
                } else if let Some(aggregate) = session.aggregate {
                    retained.insert(session.window_key, aggregate);
                }
            }

            let cache = self
                .session_cache
                .as_mut()
                .ok_or_else(|| anyhow!("missing session window aggregate cache"))?;
            if retained.is_empty() {
                cache.remove(&key);
            } else {
                cache.insert(key, retained);
            }
        }

        if !input_updates.is_empty() {
            work.record_persisted_rows(input_updates.len());
            self.input_index
                .apply_deltas(input_updates)
                .await
                .context("evict expired session window aggregate input rows")?;
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
            ensure!(
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
                .context("intern key while staging session window aggregate delta")?;
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
            .context("schedule session window aggregate update")?;

        versioned
            .table()
            .write_batch(batch)
            .await
            .context("write session window aggregate update")?;

        versioned.apply_version_plan(&plan);
        Ok(versioned.handle_for_version(plan.version))
    }
}

#[async_trait]
impl<K, V, A> DeltaOperator for SessionWindowAggregateOp<K, V, A>
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
    async fn on_step(&mut self, _ts: i64, inputs: &[ZSetHandle]) -> Result<Option<ZSetHandle>> {
        let delta_handle = inputs
            .first()
            .cloned()
            .context("session window aggregate requires one input delta handle")?;

        let delta_values =
            delta_zset_handle::<V>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for session window aggregate")?;
        let mut work = metrics::LogicalWorkSnapshot::from_input_delta_rows(delta_values.len());
        let delta_map = self.coalesce_deltas(delta_values);
        let cutoff = self.watermark_cutoff();

        let delta_rows = delta_map
            .iter()
            .map(|(row, weight)| (row.clone(), *weight))
            .collect::<Vec<_>>();
        let mut input_updates = Vec::new();
        let mut affected_keys = HashSet::new();
        let mut dropped_too_late = 0_u64;
        for (row, weight, key, event_ts) in (self.row_extractor)(&delta_rows) {
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
            affected_keys.insert(key.clone());
            input_updates.push((key, row, weight));
        }
        if dropped_too_late > 0 {
            WINDOW_DROPPED_TOO_LATE_TOTAL.inc_by(dropped_too_late);
        }

        let mut aggregate_updates: HashMap<(WindowKey<K>, A), i64> = HashMap::new();
        if !input_updates.is_empty() {
            work.record_persisted_rows(input_updates.len());
            self.input_index
                .apply_deltas(input_updates)
                .await
                .context("update session window aggregate input index")?;

            let cache_rebuild_rows = self
                .ensure_session_cache()
                .await
                .context("load session window aggregate cache")?;
            if cache_rebuild_rows != 0 {
                work.cache_rebuild_rows = work
                    .cache_rebuild_rows
                    .saturating_add(cache_rebuild_rows as u64);
                work.state_full_scan_count = work.state_full_scan_count.saturating_add(1);
                work.state_scan_rows = work
                    .state_scan_rows
                    .saturating_add(cache_rebuild_rows as u64);
            }

            for key in affected_keys {
                self.recompute_group(&key, &mut aggregate_updates, &mut work)
                    .await?;
            }
        }

        self.evict_expired_sessions(cutoff, &mut aggregate_updates, &mut work)
            .await
            .context("evict expired session windows")?;

        if let Some(session_cache) = self.session_cache.as_ref() {
            let state_entries = session_cache.values().map(HashMap::len).sum::<usize>();
            WINDOW_STATE_ENTRIES.set(i64::try_from(state_entries).unwrap_or(i64::MAX));
            if let Some(limit) = *WINDOW_STATE_LIMIT
                && state_entries > limit
            {
                WINDOW_STATE_LIMIT_EXCEEDED_TOTAL.inc();
                tracing::warn!(
                    current_entries = state_entries,
                    limit,
                    "session window aggregate state exceeds configured limit"
                );
            }
        }

        if aggregate_updates.is_empty() {
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }
        work.changed_windows = work
            .changed_windows
            .saturating_add(aggregate_updates.len() as u64);
        work.aggregate_state_rows_updated = aggregate_updates.len() as u64;
        work.record_output_delta_rows(aggregate_updates.len());

        let base_version = self.state.base_version_for_update();
        let new_integrated_handle = Self::apply_deltas_to_versioned(
            &mut self.state.integrated,
            &aggregate_updates,
            base_version,
        )
        .await
        .context("update session window aggregate state")?;
        work.record_persisted_rows(aggregate_updates.len());
        self.state.update_handle(new_integrated_handle);

        let delta_handle =
            Self::apply_deltas_to_versioned(&mut self.output, &aggregate_updates, None)
                .await
                .context("persist session window aggregate output")?;
        work.record_persisted_rows(aggregate_updates.len());
        self.logical_work.finish_tick(work);
        Ok(Some(delta_handle))
    }

    fn logical_work(&self) -> Option<metrics::LogicalWorkSnapshot> {
        Some(self.logical_work.last_tick())
    }
}

impl DbspSessionWindowAggregate {
    #[allow(clippy::too_many_arguments)]
    pub async fn new_batch<K, V, A, FWindow, FAgg>(
        input: &DeltaHandleStream,
        row_extractor: FWindow,
        aggregator: FAgg,
        gap_ms: i64,
        allowed_lateness_ms: i64,
        watermark: Arc<AtomicI64>,
        error_handler: Option<RuntimeErrorHandler>,
    ) -> Result<Self>
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
        FWindow: Fn(&[(V, i64)]) -> Vec<(V, i64, K, i64)> + Send + Sync + 'static,
        FAgg: Fn(&K, &[(V, i64)]) -> Option<A> + Send + Sync + 'static,
    {
        ensure!(gap_ms > 0, "session gap must be positive");
        ensure!(
            allowed_lateness_ms >= 0,
            "allowed lateness must be non-negative"
        );

        let table = input.table();
        let frontier = input.current_time();
        let horizon = input.semantic_horizon();
        let aggregate_id = NEXT_SESSION_WINDOW_AGG_ID.fetch_add(1, Ordering::Relaxed);
        let output_ns = format!("session_window_agg_output_{aggregate_id}");
        let empty_handle = ZSetHandle {
            ns: output_ns.clone(),
            version: 0,
        };

        let state = RelationState::<(WindowKey<K>, A)>::empty(
            table.clone(),
            format!("session_window_agg_state_{aggregate_id}"),
        )
        .await?;
        let output_dict = Arc::new(
            Dictionary::<(WindowKey<K>, A)>::with_table(table.clone(), output_ns.clone(), None)
                .await
                .context("create output dictionary for session window aggregate")?,
        );
        let output = VersionedZSet::new(output_dict, table.clone(), output_ns.clone())
            .await
            .context("create output zset for session window aggregate")?;
        let input_index = IndexedBatchZSet::with_hot_key_compaction_threshold(
            table.clone(),
            format!("session_window_agg_index_{aggregate_id}"),
            DEFAULT_HOT_KEY_COMPACTION_THRESHOLD,
        );

        let op = Arc::new(AsyncMutex::new(SessionWindowAggregateOp {
            table: table.clone(),
            dict_cache: HashMap::new(),
            row_extractor: Arc::new(row_extractor),
            aggregator: Arc::new(aggregator),
            input_index,
            state,
            output,
            session_cache: None,
            watermark,
            gap_ms,
            allowed_lateness_ms,
            logical_work: metrics::LogicalWorkCollector::default(),
        }));

        let handle_group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(ZSetHandleGroup {
            default: empty_handle.clone(),
        });

        let history = collect_values(input, horizon).await?;
        let mut output_handles = Vec::with_capacity(history.len());
        for handle in history {
            let out_handle = {
                let mut op_guard = op.lock().await;
                op_guard.on_step(0, std::slice::from_ref(&handle)).await?
            }
            .unwrap_or_else(|| empty_handle.clone());
            output_handles.push(out_handle);
        }

        let mut stream = build_exact_stream_from_values(
            table.clone(),
            handle_group,
            "session_window_agg_output_stream/",
            frontier,
            horizon,
            &output_handles,
            empty_handle.clone(),
        )
        .await?;
        stream.flush().await?;
        {
            let mut op_guard = op.lock().await;
            op_guard.output.enable_replayable_persistence();
        }

        let writer = Arc::new(AsyncMutex::new(stream.clone()));
        let mut runtime = HandleOperatorRuntime::new(vec![input.stream()], move |ts, handles| {
            let op = Arc::clone(&op);
            let writer = Arc::clone(&writer);
            let empty_handle = empty_handle.clone();
            let handles_vec = handles.to_vec();
            Box::pin(async move {
                if handles_vec.len() != 1 {
                    return Err(anyhow!(
                        "session window aggregate runtime expected 1 handle, got {}",
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
                    report_runtime_error(&error_handler, "session_window_aggregate", err);
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

static NEXT_SESSION_WINDOW_AGG_ID: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::zset::{SegmentRecord, VersionedZSet};
    use crate::storage::KeyValueTable;
    use crate::storage::SlateTable;
    use crate::storage::dictionary::Dictionary;
    use crate::stream::util::materialize_zset_handle;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::BTreeMap;

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(
            Db::open("session_window_aggregate", store)
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
                .expect("intern key for session window test");
            buckets
                .entry((id >> 48) as u16)
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
            .expect("build versioned input");
        let version = versioned
            .create_version_with_base(segments, None)
            .await
            .expect("create input version");
        versioned.handle_for_version(version)
    }

    #[tokio::test]
    async fn session_window_aggregate_merges_splits_and_evicts() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        let input_dict = Arc::new(
            Dictionary::<(i64, i64, i64)>::with_table(table.clone(), "session_window_input", None)
                .await
                .expect("input dict"),
        );
        let output_dict = Arc::new(
            Dictionary::<(WindowKey<i64>, i64)>::with_table(
                table.clone(),
                "session_window_output",
                None,
            )
            .await
            .expect("output dict"),
        );
        let output = VersionedZSet::new(
            output_dict.clone(),
            table.clone(),
            "session_window_output".to_string(),
        )
        .await
        .expect("output zset");
        let mut op = SessionWindowAggregateOp {
            table: table.clone(),
            dict_cache: HashMap::new(),
            row_extractor: Arc::new(|rows: &[((i64, i64, i64), i64)]| {
                rows.iter()
                    .map(|((group, event_ts, value), weight)| {
                        ((*group, *event_ts, *value), *weight, *group, *event_ts)
                    })
                    .collect()
            }),
            aggregator: Arc::new(|_key: &i64, rows: &[((i64, i64, i64), i64)]| {
                let sum = rows
                    .iter()
                    .map(|((_, _, value), weight)| value * weight)
                    .sum::<i64>();
                Some(sum)
            }),
            input_index: IndexedBatchZSet::with_hot_key_compaction_threshold(
                table.clone(),
                "session_window_index",
                2,
            ),
            state: RelationState::<(WindowKey<i64>, i64)>::empty(
                table.clone(),
                "session_window_state".to_string(),
            )
            .await
            .expect("state"),
            output,
            session_cache: None,
            watermark: Arc::new(AtomicI64::new(-1)),
            gap_ms: 15,
            allowed_lateness_ms: 0,
            logical_work: metrics::LogicalWorkCollector::default(),
        };

        let first = stage_version(
            input_dict.clone(),
            table.clone(),
            "session_window_input",
            &[((1, 0, 10), 1), ((1, 25, 5), 1)],
        )
        .await;
        let mut cache = HashMap::new();
        let step_one = materialize_zset_handle::<(WindowKey<i64>, i64)>(
            table.clone(),
            &mut cache,
            &op.on_step(0, &[first])
                .await
                .expect("run t1")
                .expect("t1 output"),
        )
        .await
        .expect("materialize t1");
        assert_eq!(
            step_one,
            HashMap::from([
                (
                    (
                        WindowKey {
                            start: 0,
                            end: 15,
                            key: 1,
                        },
                        10,
                    ),
                    1,
                ),
                (
                    (
                        WindowKey {
                            start: 25,
                            end: 40,
                            key: 1,
                        },
                        5,
                    ),
                    1,
                ),
            ])
        );

        let bridge = stage_version(
            input_dict.clone(),
            table.clone(),
            "session_window_input",
            &[((1, 12, 7), 1)],
        )
        .await;
        let step_two = materialize_zset_handle::<(WindowKey<i64>, i64)>(
            table.clone(),
            &mut cache,
            &op.on_step(1, &[bridge])
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
                            start: 0,
                            end: 15,
                            key: 1,
                        },
                        10,
                    ),
                    -1,
                ),
                (
                    (
                        WindowKey {
                            start: 25,
                            end: 40,
                            key: 1,
                        },
                        5,
                    ),
                    -1,
                ),
                (
                    (
                        WindowKey {
                            start: 0,
                            end: 40,
                            key: 1,
                        },
                        22,
                    ),
                    1,
                ),
            ])
        );

        let retract_bridge = stage_version(
            input_dict,
            table.clone(),
            "session_window_input",
            &[((1, 12, 7), -1)],
        )
        .await;
        let step_three = materialize_zset_handle::<(WindowKey<i64>, i64)>(
            table.clone(),
            &mut cache,
            &op.on_step(2, &[retract_bridge])
                .await
                .expect("run t3")
                .expect("t3 output"),
        )
        .await
        .expect("materialize t3");
        assert_eq!(
            step_three,
            HashMap::from([
                (
                    (
                        WindowKey {
                            start: 0,
                            end: 40,
                            key: 1,
                        },
                        22,
                    ),
                    -1,
                ),
                (
                    (
                        WindowKey {
                            start: 0,
                            end: 15,
                            key: 1,
                        },
                        10,
                    ),
                    1,
                ),
                (
                    (
                        WindowKey {
                            start: 25,
                            end: 40,
                            key: 1,
                        },
                        5,
                    ),
                    1,
                ),
            ])
        );

        op.watermark.store(45, Ordering::Relaxed);
        let step_four = materialize_zset_handle::<(WindowKey<i64>, i64)>(
            table.clone(),
            &mut cache,
            &op.on_step(
                3,
                &[ZSetHandle {
                    ns: "session_window_input".to_string(),
                    version: 0,
                }],
            )
            .await
            .expect("run t4")
            .expect("t4 output"),
        )
        .await
        .expect("materialize t4");
        assert_eq!(
            step_four,
            HashMap::from([
                (
                    (
                        WindowKey {
                            start: 0,
                            end: 15,
                            key: 1,
                        },
                        10,
                    ),
                    -1,
                ),
                (
                    (
                        WindowKey {
                            start: 25,
                            end: 40,
                            key: 1,
                        },
                        5,
                    ),
                    -1,
                ),
            ])
        );
    }
}
