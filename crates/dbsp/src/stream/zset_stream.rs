use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;

use crate::algebra::AbelianGroup;
use crate::collections::zset::{SegmentRecord, VersionedZSet};
use crate::handles::{ZSetHandle, ZSetHandleView};
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::core::stream::Stream;
use super::groups::HandleGroup;
use super::util::collect_values;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamRetention {
    None,
    KeepLast { keep_last: usize },
    AllButLatest,
}

impl StreamRetention {
    fn window_size(self) -> Option<usize> {
        match self {
            StreamRetention::None => None,
            StreamRetention::KeepLast { keep_last } if keep_last > 0 => Some(keep_last),
            StreamRetention::KeepLast { .. } => None,
            StreamRetention::AllButLatest => Some(1),
        }
    }
}

pub struct ZSetStream<K>
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
    pub(crate) stream: Stream<ZSetHandle>,
    versioned: VersionedZSet<K>,
    overlay: HashMap<K, i64>,
    retention: StreamRetention,
    retention_window: VecDeque<ZSetHandle>,
    retention_counts: HashMap<u64, usize>,
    current_handle: ZSetHandle,
}

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
        let versioned = VersionedZSet::new(dict, table.clone(), namespace.clone()).await?;
        let default_hint = ZSetHandle {
            ns: namespace.clone(),
            version: 0,
        };
        let group: Arc<dyn AbelianGroup<ZSetHandle>> = Arc::new(HandleGroup::new(default_hint));
        let stream = Stream::with_table(table, namespace.clone(), group).await?;
        let default_handle = stream.default.clone();

        let history = collect_values(&stream, stream.current_time()).await?;
        let current_handle = history.last().cloned().unwrap_or(default_handle.clone());
        let (retention_window, retention_counts) =
            initialize_retention(&history, retention.window_size());

        Ok(Self {
            stream,
            versioned,
            overlay: HashMap::new(),
            retention,
            retention_window,
            retention_counts,
            current_handle,
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
        let overlay = std::mem::take(&mut self.overlay);
        if overlay.is_empty() {
            return self.flush_without_version_update().await;
        }
        self.flush_with_overlay(overlay).await
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

    pub fn current_handle(&self) -> &ZSetHandle {
        &self.current_handle
    }

    pub fn handle_view(&self, handle: &ZSetHandle) -> ZSetHandleView<K> {
        ZSetHandleView::new(
            self.versioned.dictionary(),
            self.versioned.table(),
            handle.ns.clone(),
            handle.version,
        )
    }

    pub fn latest_view(&self) -> ZSetHandleView<K> {
        self.handle_view(&self.current_handle)
    }

    pub fn handle_stream(&self) -> Stream<ZSetHandle> {
        self.stream.clone()
    }

    pub fn namespace(&self) -> &str {
        self.versioned.namespace()
    }

    #[cfg(test)]
    pub(crate) fn stream_intent_key(&self) -> Vec<u8> {
        self.stream.encode_intent_key()
    }

    async fn flush_without_version_update(&mut self) -> Result<ZSetHandle> {
        let handle = self.current_handle.clone();
        self.stream
            .send(handle.clone())
            .await
            .context("advance stream without deltas")?;

        let mut batch = WriteBatch::new();
        let stream_intent = self.stream.encode_intent_key();
        let dirty = self.flush_stream_into_batch(&mut batch)?;
        if dirty {
            batch.put(stream_intent.clone(), vec![1]);
            self.versioned
                .table()
                .write_batch(batch)
                .await
                .context("persist stream state")?;

            let mut cleanup = WriteBatch::new();
            cleanup.delete(stream_intent.clone());
            self.versioned
                .table()
                .write_batch(cleanup)
                .await
                .context("clear stream intent")?;
        }

        let releases = self.record_handle(handle.clone());
        self.apply_retention(releases).await?;
        self.current_handle = handle.clone();
        Ok(handle)
    }

    async fn flush_with_overlay(&mut self, overlay: HashMap<K, i64>) -> Result<ZSetHandle> {
        let dict = self.versioned.dictionary();
        let mut dict_batch = dict.batch();
        let mut buckets: BTreeMap<u16, Vec<(u64, i64)>> = BTreeMap::new();
        for (key, delta) in overlay {
            if delta == 0 {
                continue;
            }
            let id = dict_batch
                .intern(&key)
                .await
                .context("intern key while staging overlay")?;
            buckets.entry(bucket_for(id)).or_default().push((id, delta));
        }
        drop(dict_batch);

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
            .enqueue_version_with_base(segments, base, 1, &mut batch)
            .await
            .context("schedule versioned update with overlay")?;

        let new_handle = self.versioned.handle_for_version(plan.version);
        let stream_intent = self.stream.encode_intent_key();
        let version_intent = self.versioned.intent_key_bytes().to_vec();

        self.stream
            .send(new_handle.clone())
            .await
            .context("append handle to stream")?;

        if self.flush_stream_into_batch(&mut batch)? {
            batch.put(stream_intent.clone(), vec![1]);
        }

        self.versioned
            .table()
            .write_batch(batch)
            .await
            .context("write combined stream and version update")?;

        let mut cleanup = WriteBatch::new();
        cleanup.delete(stream_intent.clone());
        cleanup.delete(version_intent.clone());
        self.versioned
            .table()
            .write_batch(cleanup)
            .await
            .context("clear intents after versioned update")?;

        self.versioned.apply_version_plan(&plan);
        self.current_handle = new_handle.clone();

        let releases = self.record_handle(new_handle.clone());
        self.apply_retention(releases).await?;

        Ok(new_handle)
    }

    fn flush_stream_into_batch(&mut self, batch: &mut WriteBatch) -> Result<bool> {
        let mut dirty = false;
        if self.stream.flush_defaults_into(batch)? {
            dirty = true;
        }
        if self.stream.flush_data_into(batch)? {
            dirty = true;
        }
        if self.stream.flush_state_into(batch)? {
            dirty = true;
        }
        Ok(dirty)
    }

    fn record_handle(&mut self, handle: ZSetHandle) -> Vec<u64> {
        let mut releases = Vec::new();
        if let Some(limit) = self.retention.window_size() {
            if limit == 0 {
                return releases;
            }

            if self.retention_window.len() >= limit {
                if let Some(evicted) = self.retention_window.pop_front() {
                    if let Some(count) = self.retention_counts.get_mut(&evicted.version) {
                        if *count == 1 {
                            self.retention_counts.remove(&evicted.version);
                            if evicted.version != 0 {
                                releases.push(evicted.version);
                            }
                        } else {
                            *count -= 1;
                        }
                    }
                }
            }

            *self.retention_counts.entry(handle.version).or_insert(0) += 1;
            self.retention_window.push_back(handle);
        }
        releases
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
}

fn initialize_retention(
    history: &[ZSetHandle],
    window: Option<usize>,
) -> (VecDeque<ZSetHandle>, HashMap<u64, usize>) {
    let mut window_handles = VecDeque::new();
    let mut counts = HashMap::new();

    if let Some(limit) = window {
        if limit > 0 {
            let skip = history.len().saturating_sub(limit);
            for handle in history.iter().skip(skip).cloned() {
                *counts.entry(handle.version).or_insert(0) += 1;
                window_handles.push_back(handle);
            }
        }
    }

    (window_handles, counts)
}

fn bucket_for(id: u64) -> u16 {
    (id >> 48) as u16
}
