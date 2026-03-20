use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use datafusion::scalar::ScalarValue;
use dbsp::stream::{DeltaHandleStream, SnapshotHandleStream};
use dbsp::{StreamRetention, ZSetStream};
use tokio::sync::mpsc;
use tracing::field;

use crate::codec::ensure_outer_stream_codec;
use crate::dbsp_bridge::DbspBridge;
use crate::encoding::encode_projected_row_key;
use crate::namespaces;
use crate::stream_types::{Diff, EncodedDelta, EncodedDeltaBatch};

/// Handle describing the sealed outer stream state for a single source.
#[derive(Debug, Clone)]
pub struct OuterStreamHandle {
    pub source: String,
    pub namespace: String,
    pub version: u64,
}

/// Snapshot of an outer stream's committed frontier for checkpoint manifests.
#[derive(Debug, Clone)]
pub struct OuterStreamCheckpoint {
    pub source: String,
    pub namespace: String,
    pub version: u64,
    pub frontier: i64,
}

#[derive(Debug, Clone)]
pub struct TransientSourceBatch {
    pub source: String,
    pub version: i64,
    pub deltas: EncodedDeltaBatch,
}

#[derive(Clone)]
pub struct TransientSourceHandleStream {
    subscribers: Arc<Mutex<Vec<mpsc::UnboundedSender<TransientSourceBatch>>>>,
}

impl TransientSourceHandleStream {
    pub fn subscribe(&self) -> mpsc::UnboundedReceiver<TransientSourceBatch> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.subscribers
            .lock()
            .expect("transient source subscribers lock poisoned")
            .push(tx);
        rx
    }
}

/// Batches decoded source rows into a DBSP `ZSetStream`.
pub struct OuterStreamWriter {
    source: String,
    namespace: String,
    stream: ZSetStream<Vec<u8>>,
    durable_enabled: bool,
    transient_version: i64,
    pending_transient_deltas: EncodedDeltaBatch,
    pending_transient_bytes: usize,
    pending_encode_us: u64,
    transient_subscribers: Arc<Mutex<Vec<mpsc::UnboundedSender<TransientSourceBatch>>>>,
}

static TRANSIENT_SOURCE_BATCH_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
const TRANSIENT_SOURCE_BATCH_LOG_SAMPLE_EVERY: u64 = 16;

impl OuterStreamWriter {
    pub fn new(
        source: impl Into<String>,
        namespace: impl Into<String>,
        stream: ZSetStream<Vec<u8>>,
    ) -> Self {
        Self {
            source: source.into(),
            namespace: namespace.into(),
            stream,
            durable_enabled: true,
            transient_version: 0,
            pending_transient_deltas: Arc::new(Vec::new()),
            pending_transient_bytes: 0,
            pending_encode_us: 0,
            transient_subscribers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn handle_stream(&self) -> SnapshotHandleStream {
        self.stream.handle_stream()
    }

    pub fn delta_handle_stream(&self) -> DeltaHandleStream {
        self.stream.delta_handle_stream()
    }

    pub fn transient_stream(&self) -> TransientSourceHandleStream {
        TransientSourceHandleStream {
            subscribers: Arc::clone(&self.transient_subscribers),
        }
    }

    pub fn set_durable_enabled(&mut self, enabled: bool) {
        self.durable_enabled = enabled;
    }

    pub fn durable_enabled(&self) -> bool {
        self.durable_enabled
    }

    pub fn append(&mut self, row: &[ScalarValue], diff: Diff) -> Result<()> {
        if diff == 0 {
            return Ok(());
        }
        let encode_start = Instant::now();
        let key = encode_projected_row_key(row)?;
        self.pending_encode_us = self
            .pending_encode_us
            .saturating_add(encode_start.elapsed().as_micros() as u64);
        self.pending_transient_bytes = self
            .pending_transient_bytes
            .saturating_add(key.len() + std::mem::size_of::<i64>());
        if self.durable_enabled {
            Arc::make_mut(&mut self.pending_transient_deltas).push((key.clone(), diff));
            self.stream.add_delta(key, diff);
        } else {
            Arc::make_mut(&mut self.pending_transient_deltas).push((key, diff));
        }
        Ok(())
    }

    pub fn append_encoded(&mut self, key: Vec<u8>, diff: Diff) -> Result<()> {
        if diff == 0 {
            return Ok(());
        }
        self.pending_transient_bytes = self
            .pending_transient_bytes
            .saturating_add(key.len() + std::mem::size_of::<i64>());
        if self.durable_enabled {
            Arc::make_mut(&mut self.pending_transient_deltas).push((key.clone(), diff));
            self.stream.add_delta(key, diff);
        } else {
            Arc::make_mut(&mut self.pending_transient_deltas).push((key, diff));
        }
        Ok(())
    }

    pub fn pending_transient_batch(&self, version: i64) -> Option<TransientSourceBatch> {
        if self.pending_transient_deltas.is_empty() && !self.has_transient_subscribers() {
            return None;
        }
        Some(TransientSourceBatch {
            source: self.source.clone(),
            version,
            deltas: Arc::clone(&self.pending_transient_deltas),
        })
    }

    /// Advance the stream frontier even when no rows were appended.
    pub async fn tick(&mut self) -> Result<OuterStreamHandle> {
        let span = tracing::debug_span!(
            "tick",
            source = %self.source,
            namespace = %self.namespace,
            version = field::Empty
        );
        let _enter = span.enter();
        let handle_version = self.publish_pending_batch(None).await?;
        span.record("version", handle_version);
        tracing::debug!("outer stream ticked");
        Ok(OuterStreamHandle {
            source: self.source.clone(),
            namespace: self.namespace.clone(),
            version: handle_version,
        })
    }

    pub async fn flush(&mut self) -> Result<OuterStreamHandle> {
        let span = tracing::debug_span!(
            "flush",
            source = %self.source,
            namespace = %self.namespace,
            version = field::Empty
        );
        let _enter = span.enter();
        let handle_version = self.publish_pending_batch(None).await?;
        span.record("version", handle_version);
        tracing::debug!("outer stream flushed");
        Ok(OuterStreamHandle {
            source: self.source.clone(),
            namespace: self.namespace.clone(),
            version: handle_version,
        })
    }

    pub async fn tick_with_version(&mut self, version: i64) -> Result<OuterStreamHandle> {
        let span = tracing::debug_span!(
            "tick",
            source = %self.source,
            namespace = %self.namespace,
            version = field::Empty
        );
        let _enter = span.enter();
        let handle_version = self.publish_pending_batch(Some(version)).await?;
        span.record("version", handle_version);
        tracing::debug!("outer stream ticked with logical version");
        Ok(OuterStreamHandle {
            source: self.source.clone(),
            namespace: self.namespace.clone(),
            version: handle_version,
        })
    }

    pub fn replay_transient_batch(
        &mut self,
        version: i64,
        deltas: Vec<EncodedDelta>,
    ) -> Result<()> {
        self.transient_version = self.transient_version.max(version);
        self.publish_batch(TransientSourceBatch {
            source: self.source.clone(),
            version,
            deltas: Arc::new(deltas),
        });
        Ok(())
    }

    async fn publish_pending_batch(&mut self, version: Option<i64>) -> Result<u64> {
        let publish_empty_transient = version.is_some()
            && self.pending_transient_deltas.is_empty()
            && self.has_transient_subscribers();
        let publish_version = match version {
            Some(version) => version,
            None if self.pending_transient_deltas.is_empty() => self.transient_version,
            None => self.transient_version.saturating_add(1),
        };
        if !self.pending_transient_deltas.is_empty() {
            self.transient_version = publish_version;
            let batch = TransientSourceBatch {
                source: self.source.clone(),
                version: publish_version,
                deltas: std::mem::replace(&mut self.pending_transient_deltas, Arc::new(Vec::new())),
            };
            let delta_rows = batch.deltas.len();
            let delta_bytes = self.pending_transient_bytes;
            let encode_us = self.pending_encode_us;
            self.pending_transient_bytes = 0;
            self.pending_encode_us = 0;
            self.publish_batch(batch);
            if TRANSIENT_SOURCE_BATCH_LOG_COUNTER
                .fetch_add(1, Ordering::Relaxed)
                .is_multiple_of(TRANSIENT_SOURCE_BATCH_LOG_SAMPLE_EVERY)
            {
                tracing::info!(
                    source = %self.source,
                    version = publish_version,
                    delta_rows,
                    delta_bytes,
                    encode_us,
                    durable_enabled = self.durable_enabled,
                    "outer stream transient batch published"
                );
            }
        } else if publish_empty_transient {
            self.transient_version = publish_version;
            self.publish_batch(TransientSourceBatch {
                source: self.source.clone(),
                version: publish_version,
                deltas: Arc::new(Vec::new()),
            });
        }
        if self.durable_enabled {
            Ok(self.stream.flush().await?.version)
        } else {
            Ok(u64::try_from(publish_version.max(0)).unwrap_or(u64::MAX))
        }
    }

    fn has_transient_subscribers(&self) -> bool {
        !self
            .transient_subscribers
            .lock()
            .expect("transient source subscribers lock poisoned")
            .is_empty()
    }

    fn publish_batch(&mut self, batch: TransientSourceBatch) {
        let mut subscribers = self
            .transient_subscribers
            .lock()
            .expect("transient source subscribers lock poisoned");
        subscribers.retain(|sender| sender.send(batch.clone()).is_ok());
    }
}

/// Registry that owns all DBSP outer stream writers for registered sources.
pub struct OuterStreamRegistry {
    writers: HashMap<String, OuterStreamWriter>,
}

impl OuterStreamRegistry {
    pub async fn from_sources(
        sources: impl IntoIterator<Item = String>,
        bridge: &mut DbspBridge,
    ) -> Result<Self> {
        let mut writers = HashMap::new();
        let table = bridge.table();
        let mut seen = HashSet::new();
        for source in sources {
            if !seen.insert(source.clone()) {
                continue;
            }
            let namespace = namespaces::source(&source)?;
            let stream = bridge
                .new_stream(namespace.clone(), StreamRetention::None)
                .await
                .with_context(|| format!("create outer stream for source '{source}'"))?;
            ensure_outer_stream_codec(table.clone(), &namespace)
                .await
                .with_context(|| format!("initialize codec metadata for '{namespace}'"))?;
            writers.insert(
                source.clone(),
                OuterStreamWriter::new(source, namespace, stream),
            );
        }
        Ok(Self { writers })
    }

    pub async fn from_validated_sources(
        validated: &BTreeSet<String>,
        bridge: &mut DbspBridge,
    ) -> Result<Self> {
        Self::from_sources(validated.iter().cloned(), bridge).await
    }

    pub fn writer_mut(&mut self, source: &str) -> Option<&mut OuterStreamWriter> {
        self.writers.get_mut(source)
    }

    pub fn set_durable_enabled(&mut self, source: &str, enabled: bool) {
        if let Some(writer) = self.writers.get_mut(source) {
            writer.set_durable_enabled(enabled);
        }
    }

    pub fn handle_stream(&self, source: &str) -> Option<SnapshotHandleStream> {
        self.writers
            .get(source)
            .map(|writer| writer.handle_stream())
    }

    pub fn delta_handle_stream(&self, source: &str) -> Option<DeltaHandleStream> {
        self.writers
            .get(source)
            .map(|writer| writer.delta_handle_stream())
    }

    pub fn transient_stream(&self, source: &str) -> Option<TransientSourceHandleStream> {
        self.writers
            .get(source)
            .map(|writer| writer.transient_stream())
    }

    pub async fn flush_all(&mut self) -> Result<Vec<OuterStreamHandle>> {
        let mut handles = Vec::with_capacity(self.writers.len());
        for writer in self.writers.values_mut() {
            handles.push(writer.flush().await?);
        }
        Ok(handles)
    }

    pub async fn tick_all(&mut self) -> Result<Vec<OuterStreamHandle>> {
        let mut handles = Vec::with_capacity(self.writers.len());
        for writer in self.writers.values_mut() {
            handles.push(writer.tick().await?);
        }
        Ok(handles)
    }

    pub async fn tick_all_with_version(&mut self, version: i64) -> Result<Vec<OuterStreamHandle>> {
        let mut handles = Vec::with_capacity(self.writers.len());
        for writer in self.writers.values_mut() {
            handles.push(writer.tick_with_version(version).await?);
        }
        Ok(handles)
    }

    pub fn replay_transient_batch(
        &mut self,
        source: &str,
        version: i64,
        deltas: Vec<EncodedDelta>,
    ) -> Result<()> {
        if let Some(writer) = self.writers.get_mut(source) {
            writer.replay_transient_batch(version, deltas)?;
        }
        Ok(())
    }

    pub fn checkpoint_state(&self) -> Vec<OuterStreamCheckpoint> {
        self.writers
            .values()
            .map(|writer| {
                let handle = writer.stream.current_handle().clone();
                let frontier = writer.stream.handle_stream().committed_frontier();
                OuterStreamCheckpoint {
                    source: writer.source.clone(),
                    namespace: handle.ns,
                    version: handle.version,
                    frontier,
                }
            })
            .collect()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.writers.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.writers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::sync::Arc;

    fn row(values: &[i64]) -> Vec<ScalarValue> {
        values
            .iter()
            .map(|v| ScalarValue::Int64(Some(*v)))
            .collect()
    }

    #[tokio::test]
    async fn writer_flushes_handle() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("outer-writer", store).await.expect("db"));
        let mut bridge = DbspBridge::new(db).await.expect("bridge");
        let namespace = namespaces::source("bid").expect("namespace");
        let stream = bridge
            .new_stream(
                namespace.clone(),
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await
            .expect("stream");
        let mut writer = OuterStreamWriter::new("bid", namespace.clone(), stream);
        writer.append(&row(&[1, 2]), 1).expect("append");
        let handle = writer.flush().await.expect("flush");
        assert_eq!(handle.namespace, namespace);
        assert_eq!(handle.source, "bid");
        assert_eq!(handle.version, 1);
    }

    #[tokio::test]
    async fn versioned_tick_publishes_empty_transient_batch_to_subscribers() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("outer-empty-transient", store).await.expect("db"));
        let mut bridge = DbspBridge::new(db).await.expect("bridge");
        let namespace = namespaces::source("bid").expect("namespace");
        let stream = bridge
            .new_stream(
                namespace.clone(),
                StreamRetention::KeepLast { keep_last: 1 },
            )
            .await
            .expect("stream");
        let mut writer = OuterStreamWriter::new("bid", namespace, stream);
        let mut rx = writer.transient_stream().subscribe();

        writer
            .tick_with_version(7)
            .await
            .expect("tick with version");

        let batch = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("receive timeout")
            .expect("transient batch");
        assert_eq!(batch.version, 7);
        assert!(batch.deltas.is_empty());
    }

    #[tokio::test]
    async fn registry_initializes_unique_sources() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("outer-registry", store).await.expect("db"));
        let mut bridge = DbspBridge::new(db).await.expect("bridge");
        let validated =
            BTreeSet::from(["bid".to_string(), "auction".to_string(), "bid".to_string()]);
        let registry = OuterStreamRegistry::from_validated_sources(&validated, &mut bridge)
            .await
            .expect("registry");
        assert_eq!(registry.len(), 2);
        assert!(registry.handle_stream("bid").is_some());
        assert!(registry.handle_stream("auction").is_some());
        assert!(registry.handle_stream("person").is_none());
    }
}
