use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use dbsp::storage::KeyValueTable;
use serde::{Deserialize, Serialize};
use slatedb::WriteBatch;

use crate::source_journal::{KafkaSourceJournalRange, append_kafka_source_metadata_entry_to_batch};
use crate::stream_types::Timestamp;

const CHECKPOINT_PREFIX: &str = "checkpoint";

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
}

pub mod handle_kinds {
    pub const OPERATOR_STATE: &str = "operator_state";
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

    pub async fn persist_tick_commit(
        &self,
        commit: &TickCommit,
        kafka_source_metadata: &[(String, Option<i64>, Vec<KafkaSourceJournalRange>)],
        staged_writes: Option<WriteBatch>,
    ) -> Result<()> {
        let mut batch = staged_writes.unwrap_or_default();
        self.stage_tick_commit_with_kafka_metadata(&mut batch, commit, kafka_source_metadata)?;
        self.table
            .write_batch(batch)
            .await
            .context("persist tick commit batch")
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
    store: CheckpointStore,
    partition_offsets: HashMap<(String, u32), u64>,
    latest_tick_commit: Option<TickCommit>,
    sink_cursors: HashMap<String, SinkCursor>,
}

impl CheckpointManager {
    pub async fn new(graph_id: impl Into<String>, table: Arc<dyn KeyValueTable>) -> Result<Self> {
        let store = CheckpointStore::new(table, graph_id);
        let latest_tick_commit = store.load_latest_tick_commit().await?;
        let partition_offsets = latest_tick_commit
            .as_ref()
            .map(|commit| {
                commit
                    .source_offsets
                    .iter()
                    .map(|offset| ((offset.source.clone(), offset.partition), offset.offset))
                    .collect()
            })
            .unwrap_or_default();
        let sink_cursors = latest_tick_commit
            .as_ref()
            .map(|commit| {
                commit
                    .sink_cursors
                    .iter()
                    .map(|cursor| (cursor.sink.clone(), cursor.clone()))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            store,
            partition_offsets,
            latest_tick_commit,
            sink_cursors,
        })
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
        self.persist_tick_commit_with(commit, &[], None).await
    }

    pub async fn persist_tick_commit_with(
        &mut self,
        commit: TickCommit,
        kafka_source_metadata: &[(String, Option<i64>, Vec<KafkaSourceJournalRange>)],
        staged_writes: Option<WriteBatch>,
    ) -> Result<()> {
        self.store
            .persist_tick_commit(&commit, kafka_source_metadata, staged_writes)
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

    pub fn latest_tick_commit(&self) -> Option<&TickCommit> {
        self.latest_tick_commit.as_ref()
    }
}

fn current_unix_time_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().try_into().unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests;
