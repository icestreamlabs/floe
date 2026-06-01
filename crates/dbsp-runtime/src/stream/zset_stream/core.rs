use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;

use ahash::AHashMap;
use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;

use crate::algebra::AbelianGroup;
use crate::collections::zset::{CompactionPolicy, SegmentRecord, VersionWritePlan, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::metrics::{self, FlushWriteMetrics};
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::super::core::stream::Stream;
use super::super::groups::HandleGroup;
use super::super::util::collect_values;
use super::StreamRetention;
use super::ZSetStream;
use super::{CompactionResult, CompactionSchedulerConfig};

const DELTA_SUFFIX: &str = "/delta";
const MAX_WRITE_BATCH_CALLS_WITHOUT_OVERLAY: u64 = 1;
const MAX_WRITE_BATCH_CALLS_WITH_OVERLAY: u64 = 1;

impl<K> ZSetStream<K>
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
{
    pub async fn new(
        dict: Arc<Dictionary<K>>,
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        retention: StreamRetention,
    ) -> Result<Self> {
        let namespace = namespace.into();
        let delta_namespace = format!("{namespace}{DELTA_SUFFIX}");
        let mut versioned = VersionedZSet::new(dict, table.clone(), namespace.clone()).await?;
        let mut delta_versioned = VersionedZSet::new(
            versioned.dictionary(),
            table.clone(),
            delta_namespace.clone(),
        )
        .await?;
        let default_hint = ZSetHandle {
            ns: namespace.clone(),
            version: 0,
        };
        let group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(default_hint));
        let stream = Stream::with_table(table.clone(), namespace.clone(), group).await?;
        let default_handle = stream.default_value();
        let delta_default_hint = ZSetHandle {
            ns: delta_namespace.clone(),
            version: 0,
        };
        let delta_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(HandleGroup::new(delta_default_hint));
        let delta_stream = Stream::with_table(table.clone(), delta_namespace.clone(), delta_group)
            .await
            .context("create delta handle stream")?;
        let delta_default_handle = delta_stream.default_value();

        // ZSetStream retention and visible-version adoption are defined over the
        // committed frontier. Future scheduled handle ticks belong to derived
        // wrapper streams, not to the underlying versioned state reopened here.
        let history = collect_values(&stream, stream.current_time()).await?;
        let current_handle = history.last().cloned().unwrap_or(default_handle.clone());
        let (retention_window, retention_counts) =
            initialize_retention(&history, retention.window_size());
        let delta_history = collect_values(&delta_stream, delta_stream.current_time()).await?;
        let delta_current_handle = delta_history
            .last()
            .cloned()
            .unwrap_or(delta_default_handle.clone());
        let (delta_retention_window, delta_retention_counts) =
            initialize_retention(&delta_history, retention.window_size());
        versioned
            .adopt_persisted_version(current_handle.version)
            .await
            .context("align visible snapshot version on open")?;
        delta_versioned
            .adopt_persisted_version(delta_current_handle.version)
            .await
            .context("align visible delta version on open")?;

        Ok(Self {
            stream,
            delta_stream,
            versioned,
            delta_versioned,
            overlay: HashMap::new(),
            retention,
            compaction: CompactionPolicy::default(),
            compaction_scheduler: super::CompactionScheduler::default(),
            retention_window,
            retention_counts,
            current_handle,
            delta_retention_window,
            delta_retention_counts,
            delta_current_handle,
            pending_compaction: None,
        })
    }

    pub fn add_delta(&mut self, key: K, weight: i64) {
        if weight == 0 {
            return;
        }

        match self.overlay.entry(key) {
            Entry::Occupied(mut entry) => {
                let updated = *entry.get() + weight;
                if updated == 0 {
                    entry.remove();
                } else {
                    *entry.get_mut() = updated;
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(weight);
            }
        }
    }

    pub fn add_deltas<I>(&mut self, deltas: I)
    where
        I: IntoIterator<Item = (K, i64)>,
    {
        let mut coalesced: HashMap<K, i64> = HashMap::new();
        for (key, weight) in deltas {
            if weight == 0 {
                continue;
            }
            match coalesced.entry(key) {
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

        for (key, weight) in coalesced {
            self.add_delta(key, weight);
        }
    }

    pub async fn flush(&mut self) -> Result<ZSetHandle> {
        let (snapshot, _) = self.flush_with_delta().await?;
        Ok(snapshot)
    }

    pub async fn flush_with_delta(&mut self) -> Result<(ZSetHandle, ZSetHandle)> {
        let overlay = std::mem::take(&mut self.overlay);
        let overlay_len = overlay.len();
        let span = tracing::debug_span!(
            "flush",
            namespace = %self.namespace(),
            overlay_len,
            version = tracing::field::Empty,
            delta_version = tracing::field::Empty
        );
        let _enter = span.enter();
        let result = if overlay.is_empty() {
            self.flush_without_version_update().await
        } else {
            self.flush_with_overlay(overlay).await
        };
        if let Ok((snapshot, delta)) = &result {
            span.record("version", snapshot.version);
            span.record("delta_version", delta.version);
            tracing::debug!("flush complete");
        }
        result
    }

    pub async fn get_handle(&mut self, timestamp: i64) -> Result<ZSetHandle> {
        self.stream.get(timestamp).await
    }

    pub async fn latest_handle(&mut self) -> Result<ZSetHandle> {
        self.stream.latest().await
    }

    pub fn versioned(&mut self) -> &mut VersionedZSet<K> {
        &mut self.versioned
    }

    pub fn namespace(&self) -> &str {
        self.versioned.namespace()
    }

    pub fn set_compaction_policy(&mut self, policy: CompactionPolicy) {
        self.compaction = policy;
    }

    pub fn set_compaction_scheduler_config(&mut self, config: CompactionSchedulerConfig) {
        self.compaction_scheduler.set_config(config);
    }

    async fn complete_background_compaction(
        &mut self,
        handle: tokio::task::JoinHandle<anyhow::Result<CompactionResult>>,
    ) -> Result<()> {
        match handle.await {
            Ok(Ok(result)) => {
                self.compaction_scheduler.finish_success();
                if self.versioned.current_handle().map(|handle| handle.version)
                    == Some(result.source_version)
                {
                    self.versioned
                        .create_version_with_base(result.segments, None)
                        .await
                        .context("persist completed background compaction")?;
                }
            }
            Ok(Err(err)) => {
                self.compaction_scheduler.finish_failure();
                tracing::warn!(
                    namespace = %self.namespace(),
                    error = %err,
                    "background compaction failed"
                );
            }
            Err(err) => {
                self.compaction_scheduler.finish_failure();
                tracing::warn!(
                    namespace = %self.namespace(),
                    error = %err,
                    "background compaction task join failed"
                );
            }
        }

        Ok(())
    }

    async fn poll_background_compaction(&mut self) -> Result<()> {
        let Some(handle) = self.pending_compaction.as_ref() else {
            return Ok(());
        };
        if !handle.is_finished() {
            return Ok(());
        }

        let handle = self
            .pending_compaction
            .take()
            .ok_or_else(|| anyhow!("missing background compaction handle"))?;
        self.complete_background_compaction(handle).await
    }

    async fn schedule_background_compaction(&mut self) -> Result<()> {
        if self.compaction.is_disabled() || self.pending_compaction.is_some() {
            return Ok(());
        }

        let Some(source_version) = self.versioned.current_handle().map(|handle| handle.version)
        else {
            return Ok(());
        };

        let chain_stats = self.versioned.chain_stats().await?;
        if !self.compaction.should_compact(chain_stats) || !self.compaction_scheduler.try_start() {
            return Ok(());
        }

        let table = self.versioned.table();
        let namespace = self.namespace().to_string();
        self.pending_compaction = Some(tokio::spawn(async move {
            let dict = Arc::new(
                Dictionary::<K>::with_table(table.clone(), namespace.clone(), None)
                    .await
                    .context("open dictionary for background compaction")?,
            );
            let mut versioned =
                VersionedZSet::<K>::open_for_handle(dict, table, namespace.clone(), source_version)
                    .await
                    .context("open versioned state for background compaction")?;
            let segments = versioned
                .compact_current_detached_segments()
                .await
                .context("compact versioned chain in background")?;
            Ok(CompactionResult {
                source_version,
                segments,
            })
        }));
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_background_compaction(&mut self) -> Result<bool> {
        let Some(handle) = self.pending_compaction.take() else {
            return Ok(false);
        };
        match handle.await {
            Ok(Ok(result)) => {
                self.compaction_scheduler.finish_success();
                if self.versioned.current_handle().map(|handle| handle.version)
                    == Some(result.source_version)
                {
                    self.versioned
                        .create_version_with_base(result.segments, None)
                        .await
                        .context("persist completed background compaction")?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Ok(Err(err)) => {
                self.compaction_scheduler.finish_failure();
                Err(err).context("background compaction failed")
            }
            Err(err) => {
                self.compaction_scheduler.finish_failure();
                Err(anyhow!(err)).context("background compaction task join failed")
            }
        }
    }

    async fn flush_without_version_update(&mut self) -> Result<(ZSetHandle, ZSetHandle)> {
        let flush_start = std::time::Instant::now();
        self.compaction_scheduler.on_tick();
        self.poll_background_compaction().await?;

        let mut write_metrics = FlushWriteMetrics::default();
        let handle = self
            .versioned
            .current_handle()
            .unwrap_or_else(|| self.current_handle.clone());
        let delta_handle = self.delta_versioned.handle_for_version(0);

        let stream_enqueue_start = std::time::Instant::now();
        self.stream
            .send(handle.clone())
            .await
            .context("advance stream without deltas")?;
        self.delta_stream
            .send(delta_handle.clone())
            .await
            .context("advance delta stream without deltas")?;

        let mut batch = WriteBatch::new();
        let (dirty, committed_ts, stream_keys) =
            flush_stream_into_batch(&mut self.stream, &mut batch)?;
        let (delta_dirty, delta_committed_ts, delta_stream_keys) =
            flush_stream_into_batch(&mut self.delta_stream, &mut batch)?;
        let stream_enqueue_ms = stream_enqueue_start.elapsed().as_millis() as u64;

        let dirty = dirty || delta_dirty;
        let mut write_batch_ms = 0u64;
        let mut commit_frontier_ms = 0u64;
        if dirty {
            let committed_ts = committed_ts
                .or(delta_committed_ts)
                .ok_or_else(|| anyhow!("stream flush missing committed timestamp"))?;
            write_metrics.record_write_batch(stream_keys + delta_stream_keys);
            let write_batch_start = std::time::Instant::now();
            self.versioned
                .table()
                .write_batch(batch)
                .await
                .context("persist stream state")?;
            write_batch_ms = write_batch_start.elapsed().as_millis() as u64;

            let commit_start = std::time::Instant::now();
            self.stream.commit_frontier(committed_ts);
            self.delta_stream.commit_frontier(committed_ts);
            commit_frontier_ms = commit_start.elapsed().as_millis() as u64;
        }

        let retention_start = std::time::Instant::now();
        let releases = record_handle(
            &mut self.retention_window,
            &mut self.retention_counts,
            self.retention,
            handle.clone(),
        );
        self.apply_retention(releases).await?;
        let delta_releases = record_handle(
            &mut self.delta_retention_window,
            &mut self.delta_retention_counts,
            self.retention,
            delta_handle.clone(),
        );
        self.apply_delta_retention(delta_releases).await?;
        self.current_handle = handle.clone();
        self.delta_current_handle = delta_handle.clone();
        self.schedule_background_compaction().await?;
        let retention_ms = retention_start.elapsed().as_millis() as u64;

        assert_flush_write_batch_bound(self.namespace(), false, write_metrics);
        metrics::observe_flush_write_metrics(write_metrics);
        tracing::debug!(
            namespace = %self.namespace(),
            dirty,
            stream_enqueue_ms,
            write_batch_ms,
            commit_frontier_ms,
            retention_ms,
            write_batch_calls = write_metrics.write_batch_calls,
            keys_written = write_metrics.keys_written,
            total_flush_ms = flush_start.elapsed().as_millis() as u64,
            "zset flush no-version breakdown"
        );
        Ok((handle, delta_handle))
    }

    async fn flush_with_overlay(
        &mut self,
        overlay: HashMap<K, i64>,
    ) -> Result<(ZSetHandle, ZSetHandle)> {
        let flush_start = std::time::Instant::now();
        self.compaction_scheduler.on_tick();
        self.poll_background_compaction().await?;
        let mut write_metrics = FlushWriteMetrics::default();
        let dict = self.versioned.dictionary();
        let mut buckets: AHashMap<u16, Vec<(u64, i64)>> = AHashMap::new();

        let intern_main_start = std::time::Instant::now();
        let ids = dict
            .intern_many_values_unique(
                overlay
                    .iter()
                    .filter_map(|(key, delta)| (*delta != 0).then_some(key)),
            )
            .await
            .context("intern keys while staging overlay")?;
        for ((_, delta), id) in overlay
            .iter()
            .filter_map(|(key, delta)| (*delta != 0).then_some((key, *delta)))
            .zip(ids.into_iter())
        {
            buckets.entry(bucket_for(id)).or_default().push((id, delta));
        }
        let intern_main_ms = intern_main_start.elapsed().as_millis() as u64;

        let segment_build_start = std::time::Instant::now();
        let mut segments = Vec::new();
        let mut segment_rows = 0usize;
        for (bucket, deltas) in buckets {
            segment_rows += deltas.len();
            segments.push(SegmentRecord {
                id: 0,
                bucket,
                deltas,
            });
        }
        let segment_build_ms = segment_build_start.elapsed().as_millis() as u64;

        if segments.is_empty() {
            tracing::debug!(
                namespace = %self.namespace(),
                overlay_rows = overlay.len(),
                intern_main_ms,
                segment_build_ms,
                total_flush_ms = flush_start.elapsed().as_millis() as u64,
                "zset flush breakdown (empty segments)"
            );
            return self.flush_without_version_update().await;
        }

        let delta_segments = segments.clone();
        let base = self.versioned.current_handle().map(|handle| handle.version);

        let mut batch = WriteBatch::new();
        let enqueue_start = std::time::Instant::now();
        let plan = self
            .versioned
            .enqueue_version_with_base(segments, base, 0, &mut batch)
            .await
            .context("schedule versioned update with overlay")?;
        let delta_plan = if delta_segments.is_empty() {
            None
        } else {
            Some(
                self.delta_versioned
                    .enqueue_version_with_base(delta_segments, None, 0, &mut batch)
                    .await
                    .context("schedule delta version update")?,
            )
        };
        let enqueue_ms = enqueue_start.elapsed().as_millis() as u64;
        let staged_version_keys = version_write_key_count(&plan)
            + delta_plan
                .as_ref()
                .map(version_write_key_count)
                .unwrap_or_default();

        let delta_handle = if let Some(delta_plan) = &delta_plan {
            self.delta_versioned.handle_for_version(delta_plan.version)
        } else {
            self.delta_versioned.handle_for_version(0)
        };
        let new_handle = self.versioned.handle_for_version(plan.version);

        let stream_enqueue_start = std::time::Instant::now();
        self.stream
            .send(new_handle.clone())
            .await
            .context("append handle to stream")?;
        self.delta_stream
            .send(delta_handle.clone())
            .await
            .context("append delta handle to stream")?;
        if self.stream.default_value() != new_handle {
            self.stream.set_default_in_place(new_handle.clone());
        }

        let (stream_dirty, committed_ts, stream_keys) =
            flush_stream_into_batch(&mut self.stream, &mut batch)?;
        let (delta_dirty, delta_committed_ts, delta_stream_keys) =
            flush_stream_into_batch(&mut self.delta_stream, &mut batch)?;
        let stream_enqueue_ms = stream_enqueue_start.elapsed().as_millis() as u64;
        let stream_batch_keys = stream_keys + delta_stream_keys;

        write_metrics.record_write_batch(staged_version_keys + stream_batch_keys);
        let write_batch_start = std::time::Instant::now();
        self.versioned
            .table()
            .write_batch(batch)
            .await
            .context("write coalesced flush updates")?;
        let write_batch_ms = write_batch_start.elapsed().as_millis() as u64;

        let apply_plan_start = std::time::Instant::now();
        self.versioned.apply_version_plan(&plan);
        if let Some(delta_plan) = &delta_plan {
            self.delta_versioned.apply_version_plan(delta_plan);
        }
        self.current_handle = new_handle.clone();
        self.delta_current_handle = delta_handle.clone();
        let apply_plan_ms = apply_plan_start.elapsed().as_millis() as u64;

        let mut commit_frontier_ms = 0u64;
        if stream_dirty || delta_dirty {
            let committed_ts = committed_ts
                .or(delta_committed_ts)
                .ok_or_else(|| anyhow!("stream flush missing committed timestamp"))?;
            let commit_start = std::time::Instant::now();
            self.stream.commit_frontier(committed_ts);
            self.delta_stream.commit_frontier(committed_ts);
            commit_frontier_ms = commit_start.elapsed().as_millis() as u64;
        }

        let retention_start = std::time::Instant::now();
        let releases = record_handle(
            &mut self.retention_window,
            &mut self.retention_counts,
            self.retention,
            new_handle.clone(),
        );
        self.apply_retention(releases).await?;
        let delta_releases = record_handle(
            &mut self.delta_retention_window,
            &mut self.delta_retention_counts,
            self.retention,
            delta_handle.clone(),
        );
        self.apply_delta_retention(delta_releases).await?;
        self.schedule_background_compaction().await?;
        let retention_ms = retention_start.elapsed().as_millis() as u64;

        assert_flush_write_batch_bound(self.namespace(), true, write_metrics);
        metrics::observe_flush_write_metrics(write_metrics);
        tracing::debug!(
            namespace = %self.namespace(),
            overlay_rows = overlay.len(),
            segment_rows,
            segment_count = plan.manifest.buckets.len(),
            intern_main_ms,
            segment_build_ms,
            enqueue_ms,
            stream_enqueue_ms,
            write_batch_ms,
            apply_plan_ms,
            commit_frontier_ms,
            retention_ms,
            write_batch_calls = write_metrics.write_batch_calls,
            keys_written = write_metrics.keys_written,
            total_flush_ms = flush_start.elapsed().as_millis() as u64,
            "zset flush breakdown"
        );

        Ok((new_handle, delta_handle))
    }

    async fn apply_retention(&mut self, releases: Vec<u64>) -> Result<()> {
        for version in releases {
            self.versioned
                .release_version(version)
                .await
                .context("release version during retention")?;
        }
        Ok(())
    }

    async fn apply_delta_retention(&mut self, releases: Vec<u64>) -> Result<()> {
        for version in releases {
            self.delta_versioned
                .release_version(version)
                .await
                .context("release delta version during retention")?;
        }
        Ok(())
    }
}

fn flush_stream_into_batch(
    stream: &mut Stream<ZSetHandle>,
    batch: &mut WriteBatch,
) -> Result<(bool, Option<i64>, usize)> {
    let default_writes = stream.flush_defaults_into(batch)?;
    let data_writes = stream.flush_data_into(batch)?;
    let committed_ts = stream.flush_state_into(batch)?;
    let state_writes = usize::from(committed_ts.is_some());
    let writes = default_writes + data_writes + state_writes;
    Ok((writes > 0, committed_ts, writes))
}

fn record_handle(
    window: &mut VecDeque<ZSetHandle>,
    counts: &mut HashMap<u64, usize>,
    retention: StreamRetention,
    handle: ZSetHandle,
) -> Vec<u64> {
    let mut releases = Vec::new();
    if let Some(limit) = retention.window_size() {
        if limit == 0 {
            return releases;
        }

        *counts.entry(handle.version).or_insert(0) += 1;
        if window.len() >= limit
            && let Some(evicted) = window.pop_front()
            && let Some(count) = counts.get_mut(&evicted.version)
        {
            if *count == 1 {
                counts.remove(&evicted.version);
                if evicted.version != 0 {
                    releases.push(evicted.version);
                }
            } else {
                *count -= 1;
            }
        }
        window.push_back(handle);
    }
    releases
}

fn initialize_retention(
    history: &[ZSetHandle],
    window: Option<usize>,
) -> (VecDeque<ZSetHandle>, HashMap<u64, usize>) {
    let mut window_handles = VecDeque::new();
    let mut counts = HashMap::new();

    if let Some(limit) = window
        && limit > 0
    {
        let skip = history.len().saturating_sub(limit);
        for handle in history.iter().skip(skip).cloned() {
            *counts.entry(handle.version).or_insert(0) += 1;
            window_handles.push_back(handle);
        }
    }

    (window_handles, counts)
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}

fn version_write_key_count(plan: &VersionWritePlan) -> usize {
    plan.manifest
        .buckets
        .values()
        .map(std::vec::Vec::len)
        .sum::<usize>()
        + usize::from(plan.manifest.base.is_some())
        + 2
}

fn assert_flush_write_batch_bound(namespace: &str, has_overlay: bool, metrics: FlushWriteMetrics) {
    let max_calls = if has_overlay {
        MAX_WRITE_BATCH_CALLS_WITH_OVERLAY
    } else {
        MAX_WRITE_BATCH_CALLS_WITHOUT_OVERLAY
    };

    if metrics.write_batch_calls > max_calls {
        tracing::warn!(
            namespace,
            has_overlay,
            write_batch_calls = metrics.write_batch_calls,
            keys_written = metrics.keys_written,
            max_write_batch_calls = max_calls,
            "dbsp flush write-batch bound exceeded"
        );
    }

    debug_assert!(
        metrics.write_batch_calls <= max_calls,
        "dbsp flush for namespace '{namespace}' exceeded write-batch bound: {} > {}",
        metrics.write_batch_calls,
        max_calls
    );
}
