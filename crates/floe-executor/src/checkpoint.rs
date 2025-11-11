use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use dbsp::storage::KeyValueTable;
use serde::{Deserialize, Serialize};
use slatedb::WriteBatch;

use crate::operator_state::OperatorStateHandle;
use crate::stream_types::Timestamp;

const CHECKPOINT_PREFIX: &str = "checkpoint";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ManifestFormat {
    V1,
    V2,
}

impl Default for ManifestFormat {
    fn default() -> Self {
        ManifestFormat::V1
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DbspHandleRecord {
    pub kind: String,
    pub name: String,
    pub namespace: String,
    pub version: u64,
}

impl DbspHandleRecord {
    pub fn new(
        kind: impl Into<String>,
        name: impl Into<String>,
        namespace: impl Into<String>,
        version: u64,
    ) -> Self {
        Self {
            kind: kind.into(),
            name: name.into(),
            namespace: namespace.into(),
            version,
        }
    }

    pub fn source(name: impl Into<String>, namespace: impl Into<String>, version: u64) -> Self {
        Self::new(handle_kinds::SOURCE, name, namespace, version)
    }

    pub fn operator_state(
        name: impl Into<String>,
        namespace: impl Into<String>,
        version: u64,
    ) -> Self {
        Self::new(handle_kinds::OPERATOR_STATE, name, namespace, version)
    }

    pub fn join_output(
        name: impl Into<String>,
        namespace: impl Into<String>,
        version: u64,
    ) -> Self {
        Self::new(handle_kinds::JOIN_OUTPUT, name, namespace, version)
    }

    pub fn materialized_view(
        name: impl Into<String>,
        namespace: impl Into<String>,
        version: u64,
    ) -> Self {
        Self::new(handle_kinds::MATERIALIZED_VIEW, name, namespace, version)
    }
}

pub fn record_if_nonzero(
    kind: &str,
    name: &str,
    ns: &str,
    version: u64,
) -> Option<DbspHandleRecord> {
    if version > 0 {
        Some(DbspHandleRecord::new(kind, name, ns, version))
    } else {
        None
    }
}

pub mod handle_kinds {
    pub const SOURCE: &str = "source";
    pub const OPERATOR_STATE: &str = "operator_state";
    pub const JOIN_OUTPUT: &str = "join_output";
    pub const MATERIALIZED_VIEW: &str = "mv";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorCheckpointEntry {
    pub operator_index: usize,
    pub handles: Vec<OperatorStateHandle>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedViewCheckpointEntry {
    pub view: String,
    pub namespace: String,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceOffset {
    pub source: String,
    pub partition: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceStreamCheckpointEntry {
    pub source: String,
    pub namespace: String,
    pub version: u64,
    pub partition: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointManifest {
    pub id: u64,
    pub watermark: Timestamp,
    #[serde(default)]
    pub format: ManifestFormat,
    #[serde(default)]
    pub dbsp_handles: Vec<DbspHandleRecord>,
    #[serde(default)]
    pub source_offsets: Vec<SourceOffset>,
    #[serde(default)]
    pub operator_states: Vec<OperatorCheckpointEntry>,
    #[serde(default)]
    pub materialized_views: Vec<MaterializedViewCheckpointEntry>,
    #[serde(default)]
    pub outer_streams: Vec<SourceStreamCheckpointEntry>,
}

impl CheckpointManifest {
    pub fn ensure_dbsp_payload(&mut self) {
        if !self.dbsp_handles.is_empty() {
            return;
        }
        let mut handles = Vec::new();
        for entry in &self.operator_states {
            for handle in &entry.handles {
                handles.push(DbspHandleRecord::operator_state(
                    handle.table.clone(),
                    handle.namespace.clone(),
                    handle.version,
                ));
            }
        }
        for entry in &self.materialized_views {
            handles.push(DbspHandleRecord::materialized_view(
                entry.view.clone(),
                entry.namespace.clone(),
                entry.version,
            ));
        }
        for entry in &self.outer_streams {
            handles.push(DbspHandleRecord::source(
                entry.source.clone(),
                entry.namespace.clone(),
                entry.version,
            ));
        }
        self.dbsp_handles = handles;
    }
}

pub struct CheckpointStore {
    table: Arc<dyn KeyValueTable>,
    graph_id: String,
}

impl CheckpointStore {
    pub fn new(table: Arc<dyn KeyValueTable>, graph_id: impl Into<String>) -> Self {
        Self {
            table,
            graph_id: graph_id.into(),
        }
    }

    pub async fn persist(&self, manifest: &CheckpointManifest) -> Result<()> {
        let mut batch = WriteBatch::new();
        let manifest_key = self.manifest_key(manifest.id);
        let serialized =
            serde_json::to_vec(manifest).context("serialize checkpoint manifest to JSON")?;
        batch.put(manifest_key, serialized);

        let latest_key = self.latest_key();
        batch.put(latest_key, manifest.id.to_be_bytes().to_vec());

        for offset in &manifest.source_offsets {
            let key = self.offset_key(&offset.source);
            let mut value = Vec::with_capacity(8 + 4);
            value.extend_from_slice(&offset.partition.to_be_bytes());
            value.extend_from_slice(&offset.offset.to_be_bytes());
            batch.put(key, value);
        }

        self.table
            .write_batch(batch)
            .await
            .context("persist checkpoint manifest batch")
    }

    pub async fn load_latest(&self) -> Result<Option<CheckpointManifest>> {
        let latest_key = self.latest_key();
        let Some(latest_bytes) = self.table.get(&latest_key).await? else {
            return Ok(None);
        };
        if latest_bytes.len() != 8 {
            bail!("corrupt checkpoint latest key for graph {}", self.graph_id);
        }
        let mut id_bytes = [0u8; 8];
        id_bytes.copy_from_slice(&latest_bytes);
        let id = u64::from_be_bytes(id_bytes);

        let manifest_key = self.manifest_key(id);
        let data = self
            .table
            .get(&manifest_key)
            .await?
            .with_context(|| format!("manifest entry {id} missing for {}", self.graph_id))?;
        let mut manifest: CheckpointManifest =
            serde_json::from_slice(&data).context("decode checkpoint manifest from JSON")?;
        manifest.ensure_dbsp_payload();
        Ok(Some(manifest))
    }

    fn manifest_key(&self, id: u64) -> Vec<u8> {
        format!(
            "{}/{}/manifests/{:020}",
            CHECKPOINT_PREFIX, self.graph_id, id
        )
        .into_bytes()
    }

    fn latest_key(&self) -> Vec<u8> {
        format!("{}/{}/latest", CHECKPOINT_PREFIX, self.graph_id).into_bytes()
    }

    fn offset_key(&self, source: &str) -> Vec<u8> {
        format!("{}/{}/offsets/{}", CHECKPOINT_PREFIX, self.graph_id, source).into_bytes()
    }

    pub fn table(&self) -> Arc<dyn KeyValueTable> {
        self.table.clone()
    }
}

pub struct CheckpointManager {
    graph_id: String,
    store: CheckpointStore,
    next_id: u64,
    offsets: HashMap<String, u64>,
    latest_manifest: Option<CheckpointManifest>,
}

impl CheckpointManager {
    pub async fn new(graph_id: impl Into<String>, table: Arc<dyn KeyValueTable>) -> Result<Self> {
        Self::new_with_manifest(graph_id, table, None).await
    }

    pub async fn new_with_manifest(
        graph_id: impl Into<String>,
        table: Arc<dyn KeyValueTable>,
        manifest: Option<CheckpointManifest>,
    ) -> Result<Self> {
        let graph_id = graph_id.into();
        let store = CheckpointStore::new(table, graph_id.clone());
        let latest_manifest = match manifest {
            Some(mut m) => {
                m.ensure_dbsp_payload();
                Some(m)
            }
            None => store.load_latest().await?,
        };
        let (next_id, offsets) = if let Some(ref manifest) = latest_manifest {
            let offsets = manifest
                .source_offsets
                .iter()
                .map(|offset| (offset.source.clone(), offset.offset))
                .collect();
            (manifest.id.saturating_add(1), offsets)
        } else {
            (1, HashMap::new())
        };

        Ok(Self {
            graph_id,
            store,
            next_id,
            offsets,
            latest_manifest,
        })
    }

    pub fn graph_id(&self) -> &str {
        &self.graph_id
    }

    pub fn update_offset(&mut self, source: &str, offset: u64) {
        self.offsets.insert(source.to_string(), offset);
    }

    pub async fn persist(
        &mut self,
        watermark: Timestamp,
        dbsp_handles: Vec<DbspHandleRecord>,
        source_offsets: Vec<SourceOffset>,
    ) -> Result<()> {
        let manifest = CheckpointManifest {
            id: self.next_id,
            watermark,
            format: ManifestFormat::V2,
            dbsp_handles,
            source_offsets,
            operator_states: Vec::new(),
            materialized_views: Vec::new(),
            outer_streams: Vec::new(),
        };
        self.store.persist(&manifest).await?;
        self.next_id = self.next_id.saturating_add(1);
        self.latest_manifest = Some(manifest);
        Ok(())
    }

    pub fn snapshot_offsets(&self) -> Vec<SourceOffset> {
        self.offsets
            .iter()
            .map(|(source, offset)| SourceOffset {
                source: source.clone(),
                partition: 0,
                offset: *offset,
            })
            .collect()
    }

    pub fn latest_offsets(&self) -> &HashMap<String, u64> {
        &self.offsets
    }

    pub fn store(&self) -> &CheckpointStore {
        &self.store
    }

    pub fn latest_manifest(&self) -> Option<&CheckpointManifest> {
        self.latest_manifest.as_ref()
    }
}
