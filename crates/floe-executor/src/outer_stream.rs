use std::collections::{BTreeSet, HashMap, HashSet};

use anyhow::{Context, Result};
use datafusion::scalar::ScalarValue;
use dbsp::stream::{DeltaHandleStream, SnapshotHandleStream};
use dbsp::{StreamRetention, ZSetStream};
use tracing::field;

use crate::codec::ensure_outer_stream_codec;
use crate::dbsp_bridge::DbspBridge;
use crate::encoding::encode_projected_row_key;
use crate::namespaces;
use crate::stream_types::Diff;

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

/// Batches decoded source rows into a DBSP `ZSetStream`.
pub struct OuterStreamWriter {
    source: String,
    namespace: String,
    stream: ZSetStream<Vec<u8>>,
}

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

    pub fn append(&mut self, row: &[ScalarValue], diff: Diff) -> Result<()> {
        if diff == 0 {
            return Ok(());
        }
        let key = encode_projected_row_key(row)?;
        self.stream.add_delta(key, diff);
        Ok(())
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
        let handle = self.stream.flush().await?;
        span.record("version", handle.version);
        tracing::debug!("outer stream ticked");
        Ok(OuterStreamHandle {
            source: self.source.clone(),
            namespace: self.namespace.clone(),
            version: handle.version,
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
        let handle = self.stream.flush().await?;
        span.record("version", handle.version);
        tracing::debug!("outer stream flushed");
        Ok(OuterStreamHandle {
            source: self.source.clone(),
            namespace: self.namespace.clone(),
            version: handle.version,
        })
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
    async fn registry_initializes_unique_sources() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("outer-registry", store).await.expect("db"));
        let mut bridge = DbspBridge::new(db).await.expect("bridge");
        let registry = OuterStreamRegistry::from_sources(
            vec!["bid".to_string(), "bid".to_string(), "auction".to_string()],
            &mut bridge,
        )
        .await
        .expect("registry");
        assert_eq!(registry.len(), 2);
    }

    #[tokio::test]
    async fn registry_builds_from_validated_sources() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("outer-registry-validated", store)
                .await
                .expect("db"),
        );
        let mut bridge = DbspBridge::new(db).await.expect("bridge");
        let mut validated = BTreeSet::new();
        validated.insert("bid".to_string());
        validated.insert("bid".to_string());
        validated.insert("auction".to_string());
        let mut registry = OuterStreamRegistry::from_validated_sources(&validated, &mut bridge)
            .await
            .expect("registry");
        assert_eq!(registry.len(), validated.len());
        assert!(registry.writer_mut("bid").is_some());
        assert!(registry.writer_mut("auction").is_some());
    }
}
