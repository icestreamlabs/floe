use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;

use crate::algebra::AbelianGroup;
use crate::collections::zset::{CompactionPolicy, SegmentRecord, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::StreamRetention;
use super::super::core::stream::Stream;
use super::super::groups::HandleGroup;
use super::super::util::collect_values;
use super::ZSetStream;

const DELTA_SUFFIX: &str = "/delta";

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
        let versioned = VersionedZSet::new(dict, table.clone(), namespace.clone()).await?;
        let delta_dict = Arc::new(
            Dictionary::with_table(table.clone(), delta_namespace.clone(), None)
                .await
                .context("create delta dictionary for zset stream")?,
        );
        let delta_versioned =
            VersionedZSet::new(delta_dict, table.clone(), delta_namespace.clone()).await?;
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

        Ok(Self {
            stream,
            delta_stream,
            versioned,
            delta_versioned,
            overlay: HashMap::new(),
            retention,
            compaction: CompactionPolicy::default(),
            retention_window,
            retention_counts,
            current_handle,
            delta_retention_window,
            delta_retention_counts,
            delta_current_handle,
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
        for (key, weight) in deltas {
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

    async fn flush_without_version_update(&mut self) -> Result<(ZSetHandle, ZSetHandle)> {
        let handle = self.current_handle.clone();
        let delta_handle = self.delta_versioned.handle_for_version(0);
        self.stream
            .send(handle.clone())
            .await
            .context("advance stream without deltas")?;
        self.delta_stream
            .send(delta_handle.clone())
            .await
            .context("advance delta stream without deltas")?;

        let mut batch = WriteBatch::new();
        let stream_intent = self.stream.encode_intent_key();
        let delta_stream_intent = self.delta_stream.encode_intent_key();
        let (dirty, committed_ts) = flush_stream_into_batch(&mut self.stream, &mut batch)?;
        let (delta_dirty, delta_committed_ts) =
            flush_stream_into_batch(&mut self.delta_stream, &mut batch)?;
        let dirty = dirty || delta_dirty;
        if dirty {
            let committed_ts = committed_ts
                .or(delta_committed_ts)
                .ok_or_else(|| anyhow!("stream flush missing committed timestamp"))?;
            batch.put(stream_intent.clone(), vec![1]);
            batch.put(delta_stream_intent.clone(), vec![1]);
            self.versioned
                .table()
                .write_batch(batch)
                .await
                .context("persist stream state")?;

            let mut cleanup = WriteBatch::new();
            cleanup.delete(stream_intent.clone());
            cleanup.delete(delta_stream_intent.clone());
            self.versioned
                .table()
                .write_batch(cleanup)
                .await
                .context("clear stream intent")?;
            self.stream.commit_frontier(committed_ts);
            self.delta_stream.commit_frontier(committed_ts);
        }

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
        Ok((handle, delta_handle))
    }

    async fn flush_with_overlay(
        &mut self,
        overlay: HashMap<K, i64>,
    ) -> Result<(ZSetHandle, ZSetHandle)> {
        let dict = self.versioned.dictionary();
        let mut dict_batch = dict.batch();
        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        let delta_dict = self.delta_versioned.dictionary();
        let mut delta_dict_batch = delta_dict.batch();
        let mut delta_buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        for (key, delta) in &overlay {
            if *delta == 0 {
                continue;
            }
            let id = dict_batch
                .intern(key)
                .await
                .context("intern key while staging overlay")?;
            buckets.entry(bucket_for(id)).or_default().push((id, *delta));
        }

        for (key, delta) in &overlay {
            if *delta == 0 {
                continue;
            }
            let delta_id = delta_dict_batch
                .intern(key)
                .await
                .context("intern key while staging delta overlay")?;
            delta_buckets
                .entry(bucket_for(delta_id))
                .or_default()
                .push((delta_id, *delta));
        }
        drop(dict_batch);
        drop(delta_dict_batch);

        let mut segments = Vec::new();
        for (bucket, mut deltas) in buckets {
            deltas.retain(|(_, delta)| *delta != 0);
            if deltas.is_empty() {
                continue;
            }
            deltas.sort_by_key(|(id, _)| *id);
            segments.push(SegmentRecord {
                id: 0,
                bucket,
                deltas,
            });
        }

        let mut delta_segments = Vec::new();
        for (bucket, mut deltas) in delta_buckets {
            deltas.retain(|(_, delta)| *delta != 0);
            if deltas.is_empty() {
                continue;
            }
            deltas.sort_by_key(|(id, _)| *id);
            delta_segments.push(SegmentRecord {
                id: 0,
                bucket,
                deltas,
            });
        }

        if segments.is_empty() {
            return self.flush_without_version_update().await;
        }

        let base = if self.current_handle.version == 0 {
            None
        } else {
            Some(self.current_handle.version)
        };

        let mut batch = WriteBatch::new();
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

        self.versioned
            .table()
            .write_batch(batch)
            .await
            .context("write versioned updates")?;

        let mut cleanup_versions = WriteBatch::new();
        cleanup_versions.delete(self.versioned.intent_key_bytes());
        cleanup_versions.delete(self.delta_versioned.intent_key_bytes());
        self.versioned
            .table()
            .write_batch(cleanup_versions)
            .await
            .context("clear version intents")?;

        self.versioned.apply_version_plan(&plan);
        if let Some(delta_plan) = &delta_plan {
            self.delta_versioned.apply_version_plan(delta_plan);
        }

        let mut new_handle = self.versioned.handle_for_version(plan.version);
        if !self.compaction.is_disabled() {
            let chain_stats = self.versioned.chain_stats().await?;
            if self.compaction.should_compact(chain_stats) {
                match self.versioned.compact_current().await {
                    Ok(compacted_version) => {
                        new_handle = self.versioned.handle_for_version(compacted_version);
                    }
                    Err(err) if err.to_string().contains("cannot compact empty version") => {}
                    Err(err) => {
                        return Err(err).context("compact versioned chain");
                    }
                }
            }
        }

        let delta_handle = if let Some(delta_plan) = &delta_plan {
            self.delta_versioned.handle_for_version(delta_plan.version)
        } else {
            self.delta_versioned.handle_for_version(0)
        };

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

        let stream_intent = self.stream.encode_intent_key();
        let delta_stream_intent = self.delta_stream.encode_intent_key();
        let mut stream_batch = WriteBatch::new();
        let (stream_dirty, committed_ts) =
            flush_stream_into_batch(&mut self.stream, &mut stream_batch)?;
        if stream_dirty {
            stream_batch.put(stream_intent.clone(), vec![1]);
        }
        let (delta_dirty, delta_committed_ts) =
            flush_stream_into_batch(&mut self.delta_stream, &mut stream_batch)?;
        if delta_dirty {
            stream_batch.put(delta_stream_intent.clone(), vec![1]);
        }

        self.versioned
            .table()
            .write_batch(stream_batch)
            .await
            .context("write stream updates")?;

        let mut cleanup_streams = WriteBatch::new();
        cleanup_streams.delete(stream_intent.clone());
        cleanup_streams.delete(delta_stream_intent.clone());
        self.versioned
            .table()
            .write_batch(cleanup_streams)
            .await
            .context("clear stream intents")?;

        self.current_handle = new_handle.clone();
        self.delta_current_handle = delta_handle.clone();

        if stream_dirty || delta_dirty {
            let committed_ts = committed_ts
                .or(delta_committed_ts)
                .ok_or_else(|| anyhow!("stream flush missing committed timestamp"))?;
            self.stream.commit_frontier(committed_ts);
            self.delta_stream.commit_frontier(committed_ts);
        }

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
) -> Result<(bool, Option<i64>)> {
    let mut dirty = false;
    if stream.flush_defaults_into(batch)? {
        dirty = true;
    }
    if stream.flush_data_into(batch)? {
        dirty = true;
    }
    let committed_ts = stream.flush_state_into(batch)?;
    if committed_ts.is_some() {
        dirty = true;
    }
    Ok((dirty, committed_ts))
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

        *counts.entry(handle.version).or_insert(0) += 1;
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
