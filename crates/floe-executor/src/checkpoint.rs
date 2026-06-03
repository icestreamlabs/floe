use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use dbsp::handles::ZSetHandle;
use dbsp::storage::KeyValueTable;
use serde::{Deserialize, Serialize};
use slatedb::WriteBatch;

use crate::dbsp_bridge::DbspBridge;
use crate::mv::registry::{DbspPersistedState, MaterializedViewRegistry};
use crate::operator_state::OperatorStateHandle;
use crate::source_journal::{KafkaSourceJournalRange, append_kafka_source_metadata_entry_to_batch};
use crate::stream_types::Timestamp;

const CHECKPOINT_PREFIX: &str = "checkpoint";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum ManifestFormat {
    #[default]
    V1,
    V2,
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

    pub fn operator_state(
        name: impl Into<String>,
        namespace: impl Into<String>,
        version: u64,
    ) -> Self {
        Self::new(handle_kinds::OPERATOR_STATE, name, namespace, version)
    }

    pub fn materialized_view(
        name: impl Into<String>,
        namespace: impl Into<String>,
        version: u64,
    ) -> Self {
        Self::new(handle_kinds::MATERIALIZED_VIEW, name, namespace, version)
    }
}

pub mod handle_kinds {
    pub const OPERATOR_STATE: &str = "operator_state";
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
    #[serde(default)]
    pub frontier: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceOffset {
    pub source: String,
    pub partition: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MaterializedViewTickVersion {
    pub view: String,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SinkCursor {
    pub sink: String,
    pub mv_name: String,
    pub last_emitted_mv_version: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_index: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KafkaCheckpointOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TickCommit {
    pub tick_id: u64,
    pub frontier: Timestamp,
    #[serde(default)]
    pub source_offsets: Vec<SourceOffset>,
    #[serde(default)]
    pub mv_versions: Vec<MaterializedViewTickVersion>,
    #[serde(default)]
    pub sink_cursors: Vec<SinkCursor>,
    #[serde(default)]
    pub kafka_offsets: Vec<KafkaCheckpointOffset>,
    #[serde(default)]
    pub operator_states: Vec<DbspHandleRecord>,
    pub committed_at_unix_ms: u64,
}

impl TickCommit {
    pub fn new(
        tick_id: u64,
        frontier: Timestamp,
        source_offsets: Vec<SourceOffset>,
        mv_versions: Vec<MaterializedViewTickVersion>,
        sink_cursors: Vec<SinkCursor>,
    ) -> Self {
        Self {
            tick_id,
            frontier,
            source_offsets,
            mv_versions,
            sink_cursors,
            kafka_offsets: Vec::new(),
            operator_states: Vec::new(),
            committed_at_unix_ms: current_unix_time_ms(),
        }
    }

    pub fn with_kafka_offsets(mut self, kafka_offsets: Vec<KafkaCheckpointOffset>) -> Self {
        self.kafka_offsets = kafka_offsets;
        self
    }

    pub fn with_operator_states(mut self, operator_states: Vec<DbspHandleRecord>) -> Self {
        self.operator_states = operator_states;
        self
    }
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
    pub sink_cursors: Vec<SinkCursor>,
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
        batch.put(latest_key, manifest.id.to_be_bytes());

        for offset in &manifest.source_offsets {
            let key = self.offset_key(&offset.source, offset.partition);
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

    pub async fn persist_tick_commit(&self, commit: &TickCommit) -> Result<()> {
        self.persist_tick_commit_with_kafka_metadata(commit, &[])
            .await
    }

    pub async fn persist_tick_commit_with_kafka_metadata(
        &self,
        commit: &TickCommit,
        kafka_source_metadata: &[(String, Option<i64>, Vec<KafkaSourceJournalRange>)],
    ) -> Result<()> {
        let mut batch = WriteBatch::new();
        self.stage_tick_commit_with_kafka_metadata(&mut batch, commit, kafka_source_metadata)?;
        self.table
            .write_batch(batch)
            .await
            .context("persist tick commit batch")
    }

    pub async fn persist_tick_commit_with_staged_writes(
        &self,
        commit: &TickCommit,
        mut batch: WriteBatch,
    ) -> Result<()> {
        self.stage_tick_commit_with_kafka_metadata(&mut batch, commit, &[])?;
        self.table
            .write_batch(batch)
            .await
            .context("persist tick commit batch with staged writes")
    }

    pub async fn persist_tick_commit_with_kafka_metadata_and_staged_writes(
        &self,
        commit: &TickCommit,
        kafka_source_metadata: &[(String, Option<i64>, Vec<KafkaSourceJournalRange>)],
        mut batch: WriteBatch,
    ) -> Result<()> {
        self.stage_tick_commit_with_kafka_metadata(&mut batch, commit, kafka_source_metadata)?;
        self.table
            .write_batch(batch)
            .await
            .context("persist tick commit batch with staged writes")
    }

    fn stage_tick_commit_with_kafka_metadata(
        &self,
        batch: &mut WriteBatch,
        commit: &TickCommit,
        kafka_source_metadata: &[(String, Option<i64>, Vec<KafkaSourceJournalRange>)],
    ) -> Result<()> {
        for (source, max_event_time_ms, ranges) in kafka_source_metadata {
            append_kafka_source_metadata_entry_to_batch(
                batch,
                source,
                commit.tick_id,
                *max_event_time_ms,
                ranges,
            )?;
        }
        let commit_key = self.tick_commit_key(commit.tick_id);
        let serialized = serde_json::to_vec(commit).context("serialize tick commit to JSON")?;
        batch.put(commit_key, serialized);
        let latest_key = self.latest_tick_commit_key();
        batch.put(latest_key, commit.tick_id.to_be_bytes());
        Ok(())
    }

    pub async fn load_latest_tick_commit(&self) -> Result<Option<TickCommit>> {
        let latest_key = self.latest_tick_commit_key();
        let Some(latest_bytes) = self.table.get(&latest_key).await? else {
            return Ok(None);
        };
        if latest_bytes.len() != 8 {
            bail!("corrupt latest tick commit key for graph {}", self.graph_id);
        }
        let mut id_bytes = [0u8; 8];
        id_bytes.copy_from_slice(&latest_bytes);
        let tick_id = u64::from_be_bytes(id_bytes);
        let commit_key = self.tick_commit_key(tick_id);
        let data = self.table.get(&commit_key).await?.with_context(|| {
            format!("tick commit entry {tick_id} missing for {}", self.graph_id)
        })?;
        let commit: TickCommit =
            serde_json::from_slice(&data).context("decode tick commit from JSON")?;
        Ok(Some(commit))
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

    fn offset_key(&self, source: &str, partition: u32) -> Vec<u8> {
        format!(
            "{}/{}/offsets/{}/{}",
            CHECKPOINT_PREFIX, self.graph_id, source, partition
        )
        .into_bytes()
    }

    fn tick_commit_key(&self, tick_id: u64) -> Vec<u8> {
        format!(
            "{}/{}/tick_commits/{:020}",
            CHECKPOINT_PREFIX, self.graph_id, tick_id
        )
        .into_bytes()
    }

    fn latest_tick_commit_key(&self) -> Vec<u8> {
        format!(
            "{}/{}/tick_commits/latest",
            CHECKPOINT_PREFIX, self.graph_id
        )
        .into_bytes()
    }

    pub fn table(&self) -> Arc<dyn KeyValueTable> {
        self.table.clone()
    }
}

pub struct CheckpointManager {
    graph_id: String,
    store: CheckpointStore,
    next_id: u64,
    partition_offsets: HashMap<(String, u32), u64>,
    latest_manifest: Option<CheckpointManifest>,
    latest_tick_commit: Option<TickCommit>,
    sink_cursors: HashMap<String, SinkCursor>,
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
        let latest_tick_commit = store.load_latest_tick_commit().await?;
        let (next_id, partition_offsets) = if let Some(ref manifest) = latest_manifest {
            let offsets = manifest
                .source_offsets
                .iter()
                .map(|offset| ((offset.source.clone(), offset.partition), offset.offset))
                .collect();
            (manifest.id.saturating_add(1), offsets)
        } else {
            (1, HashMap::new())
        };

        let sink_cursors = if let Some(ref commit) = latest_tick_commit {
            commit
                .sink_cursors
                .iter()
                .map(|cursor| (cursor.sink.clone(), cursor.clone()))
                .collect()
        } else if let Some(ref manifest) = latest_manifest {
            manifest
                .sink_cursors
                .iter()
                .map(|cursor| (cursor.sink.clone(), cursor.clone()))
                .collect()
        } else {
            HashMap::new()
        };

        Ok(Self {
            graph_id,
            store,
            next_id,
            partition_offsets,
            latest_manifest,
            latest_tick_commit,
            sink_cursors,
        })
    }

    pub fn graph_id(&self) -> &str {
        &self.graph_id
    }

    pub fn update_offset(&mut self, source: &str, offset: u64) {
        self.update_partition_offset(source, 0, offset);
    }

    pub fn update_partition_offset(&mut self, source: &str, partition: u32, offset: u64) {
        let entry = self
            .partition_offsets
            .entry((source.to_string(), partition))
            .or_insert(0);
        *entry = (*entry).max(offset);
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
            sink_cursors: self.snapshot_sink_cursors(),
        };
        self.store.persist(&manifest).await?;
        self.next_id = self.next_id.saturating_add(1);
        self.latest_manifest = Some(manifest);
        Ok(())
    }

    pub async fn persist_snapshot(
        &mut self,
        watermark: Timestamp,
        mv_registry: &MaterializedViewRegistry,
    ) -> Result<CheckpointManifest> {
        let mut dbsp_handles = Vec::new();
        if let Some(commit) = self.latest_tick_commit.as_ref() {
            dbsp_handles.extend(commit.operator_states.clone());
        }
        let mut manifest = CheckpointManifest {
            id: self.next_id,
            watermark,
            format: ManifestFormat::V2,
            dbsp_handles,
            source_offsets: self.snapshot_offsets(),
            operator_states: Vec::new(),
            materialized_views: materialized_view_entries(mv_registry),
            sink_cursors: self.snapshot_sink_cursors(),
        };
        manifest.ensure_dbsp_payload();
        self.store.persist(&manifest).await?;
        self.next_id = self.next_id.saturating_add(1);
        self.latest_manifest = Some(manifest.clone());
        Ok(manifest)
    }

    pub fn snapshot_offsets(&self) -> Vec<SourceOffset> {
        let mut offsets: Vec<SourceOffset> = self
            .partition_offsets
            .iter()
            .map(|((source, partition), offset)| SourceOffset {
                source: source.clone(),
                partition: *partition,
                offset: *offset,
            })
            .collect();
        offsets.sort_by(|left, right| {
            left.source
                .cmp(&right.source)
                .then(left.partition.cmp(&right.partition))
        });
        offsets
    }

    pub async fn persist_tick_commit(&mut self, commit: TickCommit) -> Result<()> {
        self.persist_tick_commit_with_kafka_metadata(commit, &[])
            .await
    }

    pub async fn persist_tick_commit_with_kafka_metadata(
        &mut self,
        commit: TickCommit,
        kafka_source_metadata: &[(String, Option<i64>, Vec<KafkaSourceJournalRange>)],
    ) -> Result<()> {
        self.store
            .persist_tick_commit_with_kafka_metadata(&commit, kafka_source_metadata)
            .await?;
        for cursor in &commit.sink_cursors {
            self.sink_cursors
                .insert(cursor.sink.clone(), cursor.clone());
        }
        self.latest_tick_commit = Some(commit);
        Ok(())
    }

    pub async fn persist_tick_commit_with_staged_writes(
        &mut self,
        commit: TickCommit,
        batch: WriteBatch,
    ) -> Result<()> {
        self.persist_tick_commit_with_kafka_metadata_and_staged_writes(commit, &[], batch)
            .await
    }

    pub async fn persist_tick_commit_with_kafka_metadata_and_staged_writes(
        &mut self,
        commit: TickCommit,
        kafka_source_metadata: &[(String, Option<i64>, Vec<KafkaSourceJournalRange>)],
        batch: WriteBatch,
    ) -> Result<()> {
        self.store
            .persist_tick_commit_with_kafka_metadata_and_staged_writes(
                &commit,
                kafka_source_metadata,
                batch,
            )
            .await?;
        for cursor in &commit.sink_cursors {
            self.sink_cursors
                .insert(cursor.sink.clone(), cursor.clone());
        }
        self.latest_tick_commit = Some(commit);
        Ok(())
    }

    pub fn update_sink_cursor(
        &mut self,
        sink: &str,
        mv_name: &str,
        last_emitted_mv_version: i64,
        row_index: Option<u64>,
    ) {
        if last_emitted_mv_version < 0 {
            return;
        }
        let entry = self
            .sink_cursors
            .entry(sink.to_string())
            .or_insert_with(|| SinkCursor {
                sink: sink.to_string(),
                mv_name: mv_name.to_string(),
                last_emitted_mv_version,
                row_index,
            });
        if last_emitted_mv_version > entry.last_emitted_mv_version
            || (last_emitted_mv_version == entry.last_emitted_mv_version
                && row_index.unwrap_or(0) > entry.row_index.unwrap_or(0))
        {
            entry.mv_name = mv_name.to_string();
            entry.last_emitted_mv_version = last_emitted_mv_version;
            entry.row_index = row_index;
        }
    }

    pub fn snapshot_sink_cursors(&self) -> Vec<SinkCursor> {
        let mut cursors: Vec<SinkCursor> = self.sink_cursors.values().cloned().collect();
        cursors.sort_by(|left, right| left.sink.cmp(&right.sink));
        cursors
    }

    pub fn store(&self) -> &CheckpointStore {
        &self.store
    }

    pub fn latest_manifest(&self) -> Option<&CheckpointManifest> {
        self.latest_manifest.as_ref()
    }

    pub fn latest_tick_commit(&self) -> Option<&TickCommit> {
        self.latest_tick_commit.as_ref()
    }
}

fn materialized_view_entries(
    registry: &MaterializedViewRegistry,
) -> Vec<MaterializedViewCheckpointEntry> {
    registry
        .handles()
        .into_iter()
        .filter_map(|handle| {
            let frontier = handle.latest_version()?;
            let zset_handle = handle
                .handle_for_version(frontier)
                .or_else(|| handle.handle_at_or_before_version(frontier))
                .or_else(|| {
                    handle.dbsp_state().map(|state| ZSetHandle {
                        ns: state.namespace().to_string(),
                        version: state.version(),
                    })
                })?;
            Some(MaterializedViewCheckpointEntry {
                view: handle.name().to_string(),
                namespace: zset_handle.ns,
                version: zset_handle.version,
                frontier,
            })
        })
        .collect()
}

fn current_unix_time_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().try_into().unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}

pub async fn recover_materialized_views(
    manifest: &CheckpointManifest,
    registry: &MaterializedViewRegistry,
    bridge: &mut DbspBridge,
) -> Result<()> {
    for entry in &manifest.materialized_views {
        let view_handle = registry.register(entry.view.clone());
        let handle = ZSetHandle {
            ns: entry.namespace.clone(),
            version: entry.version,
        };
        let handle_view = bridge
            .handle_view_for(&handle.ns, handle.version)
            .await
            .with_context(|| {
                format!(
                    "open handle view for materialized view '{}' version {}",
                    entry.view, entry.version
                )
            })?;
        let (dict, table, namespace, version) = handle_view.into_parts();
        let frontier = if entry.frontier == 0 {
            i64::try_from(entry.version).unwrap_or(i64::MAX)
        } else {
            entry.frontier
        };
        let logical_version = u64::try_from(frontier.max(0)).unwrap_or(u64::MAX);
        let state = DbspPersistedState::new(dict, table, namespace, version)
            .with_logical_version(logical_version);
        view_handle.set_dbsp_state(state);
        view_handle.publish_version(frontier, handle);
    }
    if manifest.watermark > 0 {
        registry.update_watermark_all(manifest.watermark);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
