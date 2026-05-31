use std::collections::BTreeMap;
use std::io::Cursor;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::cdc_buffer::CdcBufferStore;
use anyhow::{Context, Result, anyhow, ensure};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use floe_cdc_core::{CdcSourcePosition, CdcTransactionId};
use floe_core::catalog::{
    CatalogSourceDefinition, ReplicationPipelineDefinition, SourceBackedTableDefinition,
    TableDefinition,
};
use floe_core::encoding::{self, ArchivedRow};
use floe_core::{RowValue, RowValues};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use serde::{Deserialize, Serialize};
use slatedb::config::{ScanOptions, Settings, WriteOptions};
use slatedb::{CloseReason, Db, Error as SlateError, ErrorKind, WriteBatch};
use tokio::fs;

use crate::object_payload::{hex_component, load_payload_object, put_payload_object};

const TABLE_DEF_PREFIX: &str = "meta/table/";
const TABLE_DATA_PREFIX: &str = "data/";
const SOURCE_DEF_PREFIX: &str = "meta/source/definition/";
const SOURCE_TABLE_PREFIX: &str = "meta/source/table/";
const MV_DEF_PREFIX: &str = "meta/mv/definition/";
const MV_SCHEMA_PREFIX: &str = "meta/mv/schema/";
const REPLICATION_PIPELINE_DEF_PREFIX: &str = "meta/replication_pipeline/definition/";
const REPLICATION_PIPELINE_CHECKPOINT_PREFIX: &str = "meta/replication_pipeline/checkpoint/";
const REPLICATION_PIPELINE_DLQ_PREFIX: &str = "meta/replication_pipeline/dlq/";

#[derive(Clone)]
pub struct SlateCatalog {
    db: Arc<Db>,
    object_store: Arc<dyn ObjectStore>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MaterializedViewMetadata {
    name: String,
    query: String,
    if_not_exists: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationPipelineCheckpoint {
    pipeline_name: String,
    source_name: String,
    source_position: CdcSourcePosition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transaction_id: Option<CdcTransactionId>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    target_state: BTreeMap<String, String>,
    committed_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationPipelineDlqEntry {
    pipeline_name: String,
    dlq_id: String,
    source_name: String,
    source_position: CdcSourcePosition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transaction_id: Option<CdcTransactionId>,
    error_class: String,
    error_message: String,
    attempt_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload_object_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload_format: Option<String>,
    payload_bytes: usize,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    target_state: BTreeMap<String, String>,
    #[serde(default)]
    status: ReplicationPipelineDlqStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    status_reason: Option<String>,
    created_at_unix_ms: u64,
    last_updated_at_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationPipelineDlqStatus {
    #[default]
    Pending,
    Replayed,
    Discarded,
}

impl MaterializedViewMetadata {
    pub fn new(name: impl Into<String>, query: impl Into<String>, if_not_exists: bool) -> Self {
        Self {
            name: name.into(),
            query: query.into(),
            if_not_exists,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
}

impl ReplicationPipelineCheckpoint {
    pub fn new(
        pipeline_name: impl Into<String>,
        source_name: impl Into<String>,
        source_position: CdcSourcePosition,
        transaction_id: Option<CdcTransactionId>,
        target_state: BTreeMap<String, String>,
        committed_at_unix_ms: u64,
    ) -> Result<Self> {
        let pipeline_name = pipeline_name.into();
        let source_name = source_name.into();
        ensure!(
            !pipeline_name.trim().is_empty(),
            "replication pipeline checkpoint name cannot be empty"
        );
        ensure!(
            !source_name.trim().is_empty(),
            "replication pipeline checkpoint source name cannot be empty"
        );
        Ok(Self {
            pipeline_name,
            source_name,
            source_position,
            transaction_id,
            target_state,
            committed_at_unix_ms,
        })
    }

    pub fn pipeline_name(&self) -> &str {
        &self.pipeline_name
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn source_position(&self) -> &CdcSourcePosition {
        &self.source_position
    }

    pub fn transaction_id(&self) -> Option<&CdcTransactionId> {
        self.transaction_id.as_ref()
    }

    pub fn target_state(&self) -> &BTreeMap<String, String> {
        &self.target_state
    }

    pub fn committed_at_unix_ms(&self) -> u64 {
        self.committed_at_unix_ms
    }
}

impl ReplicationPipelineDlqEntry {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pipeline_name: impl Into<String>,
        dlq_id: impl Into<String>,
        source_name: impl Into<String>,
        source_position: CdcSourcePosition,
        transaction_id: Option<CdcTransactionId>,
        error_class: impl Into<String>,
        error_message: impl Into<String>,
        attempt_count: u32,
        payload_object_key: Option<String>,
        payload_format: Option<String>,
        payload_bytes: usize,
        target_state: BTreeMap<String, String>,
        created_at_unix_ms: u64,
    ) -> Result<Self> {
        let entry = Self {
            pipeline_name: pipeline_name.into(),
            dlq_id: dlq_id.into(),
            source_name: source_name.into(),
            source_position,
            transaction_id,
            error_class: error_class.into(),
            error_message: error_message.into(),
            attempt_count,
            payload_object_key,
            payload_format,
            payload_bytes,
            target_state,
            status: ReplicationPipelineDlqStatus::Pending,
            status_reason: None,
            created_at_unix_ms,
            last_updated_at_unix_ms: created_at_unix_ms,
        };
        entry.validate()?;
        Ok(entry)
    }

    pub fn pipeline_name(&self) -> &str {
        &self.pipeline_name
    }

    pub fn dlq_id(&self) -> &str {
        &self.dlq_id
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn source_position(&self) -> &CdcSourcePosition {
        &self.source_position
    }

    pub fn transaction_id(&self) -> Option<&CdcTransactionId> {
        self.transaction_id.as_ref()
    }

    pub fn error_class(&self) -> &str {
        &self.error_class
    }

    pub fn error_message(&self) -> &str {
        &self.error_message
    }

    pub fn attempt_count(&self) -> u32 {
        self.attempt_count
    }

    pub fn payload_object_key(&self) -> Option<&str> {
        self.payload_object_key.as_deref()
    }

    pub fn payload_format(&self) -> Option<&str> {
        self.payload_format.as_deref()
    }

    pub fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    pub fn target_state(&self) -> &BTreeMap<String, String> {
        &self.target_state
    }

    pub fn status(&self) -> ReplicationPipelineDlqStatus {
        self.status
    }

    pub fn status_reason(&self) -> Option<&str> {
        self.status_reason.as_deref()
    }

    pub fn created_at_unix_ms(&self) -> u64 {
        self.created_at_unix_ms
    }

    pub fn last_updated_at_unix_ms(&self) -> u64 {
        self.last_updated_at_unix_ms
    }

    pub fn with_status(
        mut self,
        status: ReplicationPipelineDlqStatus,
        last_updated_at_unix_ms: u64,
    ) -> Self {
        self.status = status;
        self.status_reason = None;
        self.last_updated_at_unix_ms = last_updated_at_unix_ms;
        self
    }

    pub fn with_status_reason(
        mut self,
        status: ReplicationPipelineDlqStatus,
        reason: Option<String>,
        last_updated_at_unix_ms: u64,
    ) -> Self {
        self.status = status;
        self.status_reason = reason.filter(|reason| !reason.trim().is_empty());
        self.last_updated_at_unix_ms = last_updated_at_unix_ms;
        self
    }

    pub fn record_attempt(mut self, last_updated_at_unix_ms: u64) -> Self {
        self.attempt_count = self.attempt_count.saturating_add(1);
        self.last_updated_at_unix_ms = last_updated_at_unix_ms;
        self
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            !self.pipeline_name.trim().is_empty(),
            "replication pipeline DLQ entry pipeline name cannot be empty"
        );
        ensure!(
            !self.dlq_id.trim().is_empty(),
            "replication pipeline DLQ entry id cannot be empty"
        );
        ensure!(
            !self.dlq_id.contains('/'),
            "replication pipeline DLQ entry id cannot contain '/'"
        );
        ensure!(
            !self.source_name.trim().is_empty(),
            "replication pipeline DLQ entry source name cannot be empty"
        );
        ensure!(
            !self.error_class.trim().is_empty(),
            "replication pipeline DLQ entry error class cannot be empty"
        );
        ensure!(
            !self.error_message.trim().is_empty(),
            "replication pipeline DLQ entry error message cannot be empty"
        );
        if let Some(payload_object_key) = self.payload_object_key.as_deref() {
            ensure!(
                !payload_object_key.trim().is_empty(),
                "replication pipeline DLQ entry payload object key cannot be empty"
            );
        }
        if let Some(payload_format) = self.payload_format.as_deref() {
            ensure!(
                !payload_format.trim().is_empty(),
                "replication pipeline DLQ entry payload format cannot be empty"
            );
        }
        Ok(())
    }
}

impl ReplicationPipelineDlqStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Replayed => "replayed",
            Self::Discarded => "discarded",
        }
    }
}

impl SlateCatalog {
    pub async fn in_memory() -> Result<Self> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Self::with_object_store_with_settings("in-memory", object_store, None).await
    }

    pub async fn with_filesystem(root: impl AsRef<Path>) -> Result<Self> {
        let root: PathBuf = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).await.with_context(|| {
            format!(
                "failed to create SlateDB root directory at {}",
                root.display()
            )
        })?;

        let object_store = LocalFileSystem::new_with_prefix(&root).with_context(|| {
            format!("failed to create local object store at {}", root.display())
        })?;
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store);
        Self::with_object_store_with_settings("floe", object_store, None).await
    }

    pub async fn in_memory_with_settings(settings: Option<Settings>) -> Result<Self> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Self::with_object_store_with_settings("in-memory", object_store, settings).await
    }

    pub async fn with_filesystem_with_settings(
        root: impl AsRef<Path>,
        settings: Option<Settings>,
    ) -> Result<Self> {
        let root: PathBuf = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).await.with_context(|| {
            format!(
                "failed to create SlateDB root directory at {}",
                root.display()
            )
        })?;

        let object_store = LocalFileSystem::new_with_prefix(&root).with_context(|| {
            format!("failed to create local object store at {}", root.display())
        })?;
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store);
        Self::with_object_store_with_settings("floe", object_store, settings).await
    }

    pub async fn with_object_store(
        name: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::with_object_store_with_settings(name, object_store, None).await
    }

    pub async fn with_object_store_with_settings(
        name: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        settings: Option<Settings>,
    ) -> Result<Self> {
        let builder = Db::builder(name.into(), Arc::clone(&object_store));
        let db = match settings {
            Some(settings) => builder
                .with_settings(settings)
                .build()
                .await
                .map_err(|err| anyhow!("unable to open SlateDB: {err}"))?,
            None => builder
                .build()
                .await
                .map_err(|err| anyhow!("unable to open SlateDB: {err}"))?,
        };
        Ok(Self {
            db: Arc::new(db),
            object_store,
        })
    }

    pub async fn register_table(&self, definition: TableDefinition) -> Result<()> {
        let key = table_definition_key(definition.name());
        let encoded = serde_json::to_vec(&definition).with_context(|| {
            format!("failed to serialize table definition {}", definition.name())
        })?;

        if let Some(existing) = self.db.get(&key).await.map_err(map_slate_err)? {
            let existing_def: TableDefinition = serde_json::from_slice(&existing)
                .context("failed to decode existing table definition")?;
            return Err(anyhow!(
                "table {} already exists with definition {:?}",
                definition.name(),
                existing_def
            ));
        }

        self.db
            .put(&key, encoded)
            .await
            .map(|_| ())
            .map_err(map_slate_err)
            .with_context(|| format!("failed to persist table definition {}", definition.name()))
    }

    pub async fn upsert_table(&self, definition: TableDefinition) -> Result<()> {
        let key = table_definition_key(definition.name());
        let encoded = serde_json::to_vec(&definition).with_context(|| {
            format!("failed to serialize table definition {}", definition.name())
        })?;
        self.db
            .put(&key, encoded)
            .await
            .map(|_| ())
            .map_err(map_slate_err)
            .with_context(|| format!("failed to write table definition {}", definition.name()))
    }

    pub async fn table(&self, name: &str) -> Result<Option<TableDefinition>> {
        let key = table_definition_key(name);
        let bytes = self.db.get(key).await.map_err(map_slate_err)?;
        if let Some(bytes) = bytes {
            let definition = serde_json::from_slice(&bytes)
                .with_context(|| format!("failed to parse table definition for {name}"))?;
            Ok(Some(definition))
        } else {
            Ok(None)
        }
    }

    pub async fn tables(&self) -> Result<Vec<TableDefinition>> {
        scan_prefix(&self.db, TABLE_DEF_PREFIX.as_bytes())
            .await?
            .into_iter()
            .map(|value| {
                serde_json::from_slice::<TableDefinition>(&value)
                    .context("failed to deserialize table definition")
            })
            .collect()
    }

    pub async fn upsert_catalog_source(&self, definition: CatalogSourceDefinition) -> Result<()> {
        ensure!(
            !definition.name().trim().is_empty(),
            "catalog source name cannot be empty"
        );
        let key = source_definition_key(definition.name());
        let encoded = serde_json::to_vec(&definition).with_context(|| {
            format!(
                "failed to serialize source definition {}",
                definition.name()
            )
        })?;
        self.db
            .put(&key, encoded)
            .await
            .map(|_| ())
            .map_err(map_slate_err)
            .with_context(|| format!("failed to write source definition {}", definition.name()))
    }

    pub async fn catalog_source(&self, name: &str) -> Result<Option<CatalogSourceDefinition>> {
        let key = source_definition_key(name);
        let bytes = self.db.get(key).await.map_err(map_slate_err)?;
        if let Some(bytes) = bytes {
            let definition = serde_json::from_slice(&bytes)
                .with_context(|| format!("failed to parse source definition for {name}"))?;
            Ok(Some(definition))
        } else {
            Ok(None)
        }
    }

    pub async fn catalog_sources(&self) -> Result<Vec<CatalogSourceDefinition>> {
        scan_prefix(&self.db, SOURCE_DEF_PREFIX.as_bytes())
            .await?
            .into_iter()
            .map(|value| {
                serde_json::from_slice::<CatalogSourceDefinition>(&value)
                    .context("failed to deserialize source definition")
            })
            .collect()
    }

    pub async fn upsert_source_backed_table(
        &self,
        definition: SourceBackedTableDefinition,
    ) -> Result<()> {
        ensure!(
            !definition.table_name().trim().is_empty(),
            "source-backed table name cannot be empty"
        );
        let key = source_table_key(definition.table_name());
        let encoded = serde_json::to_vec(&definition).with_context(|| {
            format!(
                "failed to serialize source-backed table definition {}",
                definition.table_name()
            )
        })?;
        self.db
            .put(&key, encoded)
            .await
            .map(|_| ())
            .map_err(map_slate_err)
            .with_context(|| {
                format!(
                    "failed to write source-backed table definition {}",
                    definition.table_name()
                )
            })
    }

    pub async fn source_backed_table(
        &self,
        table_name: &str,
    ) -> Result<Option<SourceBackedTableDefinition>> {
        let key = source_table_key(table_name);
        let bytes = self.db.get(key).await.map_err(map_slate_err)?;
        if let Some(bytes) = bytes {
            let definition = serde_json::from_slice(&bytes).with_context(|| {
                format!("failed to parse source-backed table definition for {table_name}")
            })?;
            Ok(Some(definition))
        } else {
            Ok(None)
        }
    }

    pub async fn source_backed_tables(&self) -> Result<Vec<SourceBackedTableDefinition>> {
        scan_prefix(&self.db, SOURCE_TABLE_PREFIX.as_bytes())
            .await?
            .into_iter()
            .map(|value| {
                serde_json::from_slice::<SourceBackedTableDefinition>(&value)
                    .context("failed to deserialize source-backed table definition")
            })
            .collect()
    }

    pub async fn insert_row(&self, table: &TableDefinition, row: &RowValues) -> Result<()> {
        table.validate_row(row)?;
        let key = table_row_key(table, row)?;
        let archived = encoding::encode(row)?;
        self.db
            .put(key, archived.bytes())
            .await
            .map(|_| ())
            .map_err(map_slate_err)
            .context("failed to insert row")
    }

    pub async fn read_rows(&self, table: &TableDefinition) -> Result<Vec<RowValues>> {
        let prefix = table_row_prefix(table.name());
        let raw_rows = scan_prefix(&self.db, prefix.as_slice()).await?;
        raw_rows
            .into_iter()
            .map(|value| {
                let row = ArchivedRow::new(value);
                encoding::decode(&row)
            })
            .collect()
    }

    pub async fn upsert_materialized_view(&self, metadata: MaterializedViewMetadata) -> Result<()> {
        ensure!(
            !metadata.name().trim().is_empty(),
            "materialized view name cannot be empty"
        );
        let key = mv_definition_key(metadata.name());
        let encoded = serde_json::to_vec(&metadata).with_context(|| {
            format!(
                "failed to serialize materialized view definition {}",
                metadata.name()
            )
        })?;
        self.db
            .put(&key, encoded)
            .await
            .map(|_| ())
            .map_err(map_slate_err)
            .with_context(|| {
                format!(
                    "failed to persist materialized view definition {}",
                    metadata.name()
                )
            })
    }

    pub async fn materialized_view(&self, name: &str) -> Result<Option<MaterializedViewMetadata>> {
        let key = mv_definition_key(name);
        let bytes = self.db.get(key).await.map_err(map_slate_err)?;
        if let Some(bytes) = bytes {
            let metadata = serde_json::from_slice(&bytes).with_context(|| {
                format!("failed to parse materialized view definition for {name}")
            })?;
            Ok(Some(metadata))
        } else {
            Ok(None)
        }
    }

    pub async fn materialized_views(&self) -> Result<Vec<MaterializedViewMetadata>> {
        scan_prefix(&self.db, MV_DEF_PREFIX.as_bytes())
            .await?
            .into_iter()
            .map(|value| {
                serde_json::from_slice::<MaterializedViewMetadata>(&value)
                    .context("failed to deserialize materialized view definition")
            })
            .collect()
    }

    pub async fn upsert_replication_pipeline(
        &self,
        definition: ReplicationPipelineDefinition,
    ) -> Result<()> {
        ensure!(
            !definition.name().trim().is_empty(),
            "replication pipeline name cannot be empty"
        );
        let key = replication_pipeline_definition_key(definition.name());
        let encoded = serde_json::to_vec(&definition).with_context(|| {
            format!(
                "failed to serialize replication pipeline definition {}",
                definition.name()
            )
        })?;
        self.db
            .put(&key, encoded)
            .await
            .map(|_| ())
            .map_err(map_slate_err)
            .with_context(|| {
                format!(
                    "failed to persist replication pipeline definition {}",
                    definition.name()
                )
            })
    }

    pub async fn replication_pipeline(
        &self,
        name: &str,
    ) -> Result<Option<ReplicationPipelineDefinition>> {
        let key = replication_pipeline_definition_key(name);
        let bytes = self.db.get(key).await.map_err(map_slate_err)?;
        if let Some(bytes) = bytes {
            let metadata = serde_json::from_slice(&bytes).with_context(|| {
                format!("failed to parse replication pipeline definition for {name}")
            })?;
            Ok(Some(metadata))
        } else {
            Ok(None)
        }
    }

    pub async fn replication_pipelines(&self) -> Result<Vec<ReplicationPipelineDefinition>> {
        scan_prefix(&self.db, REPLICATION_PIPELINE_DEF_PREFIX.as_bytes())
            .await?
            .into_iter()
            .map(|value| {
                serde_json::from_slice::<ReplicationPipelineDefinition>(&value)
                    .context("failed to deserialize replication pipeline definition")
            })
            .collect()
    }

    pub async fn put_replication_pipeline_checkpoint(
        &self,
        checkpoint: ReplicationPipelineCheckpoint,
    ) -> Result<()> {
        self.put_replication_pipeline_checkpoint_with_durable_wait(checkpoint, true)
            .await
    }

    pub async fn put_replication_pipeline_checkpoint_without_durable_wait(
        &self,
        checkpoint: ReplicationPipelineCheckpoint,
    ) -> Result<()> {
        self.put_replication_pipeline_checkpoint_with_durable_wait(checkpoint, false)
            .await
    }

    async fn put_replication_pipeline_checkpoint_with_durable_wait(
        &self,
        checkpoint: ReplicationPipelineCheckpoint,
        await_durable: bool,
    ) -> Result<()> {
        let key = replication_pipeline_checkpoint_key(checkpoint.pipeline_name());
        let encoded = serde_json::to_vec(&checkpoint).with_context(|| {
            format!(
                "failed to serialize replication pipeline checkpoint {}",
                checkpoint.pipeline_name()
            )
        })?;
        let mut batch = WriteBatch::new();
        batch.put(&key, encoded);
        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable,
                    ..WriteOptions::default()
                },
            )
            .await
            .map(|_| ())
            .map_err(map_slate_err)
            .with_context(|| {
                format!(
                    "failed to persist replication pipeline checkpoint {}",
                    checkpoint.pipeline_name()
                )
            })
    }

    pub async fn replication_pipeline_checkpoint(
        &self,
        name: &str,
    ) -> Result<Option<ReplicationPipelineCheckpoint>> {
        let key = replication_pipeline_checkpoint_key(name);
        let bytes = self.db.get(key).await.map_err(map_slate_err)?;
        if let Some(bytes) = bytes {
            let checkpoint = serde_json::from_slice(&bytes).with_context(|| {
                format!("failed to parse replication pipeline checkpoint for {name}")
            })?;
            Ok(Some(checkpoint))
        } else {
            Ok(None)
        }
    }

    pub async fn put_replication_pipeline_dlq_entry(
        &self,
        entry: ReplicationPipelineDlqEntry,
    ) -> Result<()> {
        entry.validate()?;
        let key = replication_pipeline_dlq_entry_key(entry.pipeline_name(), entry.dlq_id());
        let encoded = serde_json::to_vec(&entry).with_context(|| {
            format!(
                "failed to serialize replication pipeline '{}' DLQ entry {}",
                entry.pipeline_name(),
                entry.dlq_id()
            )
        })?;
        let mut batch = WriteBatch::new();
        batch.put(&key, encoded);
        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: true,
                    ..WriteOptions::default()
                },
            )
            .await
            .map(|_| ())
            .map_err(map_slate_err)
            .with_context(|| {
                format!(
                    "failed to persist replication pipeline '{}' DLQ entry {}",
                    entry.pipeline_name(),
                    entry.dlq_id()
                )
            })
    }

    pub async fn replication_pipeline_dlq_entry(
        &self,
        pipeline_name: &str,
        dlq_id: &str,
    ) -> Result<Option<ReplicationPipelineDlqEntry>> {
        let key = replication_pipeline_dlq_entry_key(pipeline_name, dlq_id);
        let bytes = self.db.get(key).await.map_err(map_slate_err)?;
        if let Some(bytes) = bytes {
            let entry = serde_json::from_slice(&bytes).with_context(|| {
                format!("failed to parse replication pipeline '{pipeline_name}' DLQ entry {dlq_id}")
            })?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    pub async fn replication_pipeline_dlq_entries(
        &self,
        pipeline_name: &str,
    ) -> Result<Vec<ReplicationPipelineDlqEntry>> {
        let prefix = replication_pipeline_dlq_entry_prefix(pipeline_name);
        scan_prefix(&self.db, &prefix)
            .await?
            .into_iter()
            .map(|value| {
                serde_json::from_slice::<ReplicationPipelineDlqEntry>(&value).with_context(|| {
                    format!(
                        "failed to deserialize replication pipeline '{pipeline_name}' DLQ entry"
                    )
                })
            })
            .collect()
    }

    pub async fn update_replication_pipeline_dlq_entry_status(
        &self,
        pipeline_name: &str,
        dlq_id: &str,
        status: ReplicationPipelineDlqStatus,
        last_updated_at_unix_ms: u64,
    ) -> Result<Option<ReplicationPipelineDlqEntry>> {
        let Some(entry) = self
            .replication_pipeline_dlq_entry(pipeline_name, dlq_id)
            .await?
        else {
            return Ok(None);
        };
        let entry = entry.with_status(status, last_updated_at_unix_ms);
        self.put_replication_pipeline_dlq_entry(entry.clone())
            .await?;
        Ok(Some(entry))
    }

    pub async fn update_replication_pipeline_dlq_entry_status_with_reason(
        &self,
        pipeline_name: &str,
        dlq_id: &str,
        status: ReplicationPipelineDlqStatus,
        reason: Option<String>,
        last_updated_at_unix_ms: u64,
    ) -> Result<Option<ReplicationPipelineDlqEntry>> {
        let Some(entry) = self
            .replication_pipeline_dlq_entry(pipeline_name, dlq_id)
            .await?
        else {
            return Ok(None);
        };
        let entry = entry.with_status_reason(status, reason, last_updated_at_unix_ms);
        self.put_replication_pipeline_dlq_entry(entry.clone())
            .await?;
        Ok(Some(entry))
    }

    pub async fn record_replication_pipeline_dlq_retry_attempt(
        &self,
        pipeline_name: &str,
        dlq_id: &str,
        last_updated_at_unix_ms: u64,
    ) -> Result<Option<ReplicationPipelineDlqEntry>> {
        let Some(entry) = self
            .replication_pipeline_dlq_entry(pipeline_name, dlq_id)
            .await?
        else {
            return Ok(None);
        };
        let entry = entry.record_attempt(last_updated_at_unix_ms);
        self.put_replication_pipeline_dlq_entry(entry.clone())
            .await?;
        Ok(Some(entry))
    }

    pub async fn put_replication_pipeline_dlq_payload(
        &self,
        pipeline_name: &str,
        dlq_id: &str,
        payload: Vec<u8>,
    ) -> Result<String> {
        ensure!(
            !pipeline_name.trim().is_empty(),
            "replication pipeline DLQ payload pipeline name cannot be empty"
        );
        ensure!(
            !dlq_id.trim().is_empty(),
            "replication pipeline DLQ payload id cannot be empty"
        );
        ensure!(
            !dlq_id.contains('/'),
            "replication pipeline DLQ payload id cannot contain '/'"
        );
        let object_key = replication_pipeline_dlq_payload_object_key(pipeline_name, dlq_id);
        put_payload_object(
            &self.object_store,
            &object_key,
            payload,
            "replication pipeline DLQ",
        )
        .await?;
        Ok(object_key)
    }

    pub async fn replication_pipeline_dlq_payload(
        &self,
        payload_object_key: &str,
    ) -> Result<Vec<u8>> {
        ensure!(
            !payload_object_key.trim().is_empty(),
            "replication pipeline DLQ payload object key cannot be empty"
        );
        load_payload_object(
            &self.object_store,
            payload_object_key,
            "replication pipeline DLQ",
        )
        .await
    }

    pub async fn save_materialized_view_schema(&self, name: &str, schema: SchemaRef) -> Result<()> {
        let key = mv_schema_key(name);
        let mut payload = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut payload, schema.as_ref())
                .context("encode materialized view schema via Arrow IPC")?;
            writer.finish().context("finalize schema IPC stream")?;
        }
        self.db
            .put(&key, payload)
            .await
            .map(|_| ())
            .map_err(map_slate_err)
            .with_context(|| format!("persist schema for materialized view '{name}'"))
    }

    pub async fn materialized_view_schema(&self, name: &str) -> Result<Option<SchemaRef>> {
        let key = mv_schema_key(name);
        let bytes = match self
            .db
            .get(&key)
            .await
            .map_err(map_slate_err)
            .with_context(|| format!("load schema metadata for materialized view '{name}'"))?
        {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let cursor = Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None)
            .with_context(|| format!("decode persisted schema for materialized view '{name}'"))?;
        Ok(Some(reader.schema()))
    }

    pub fn db(&self) -> Arc<Db> {
        self.db.clone()
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    pub fn cdc_buffer_store(&self) -> CdcBufferStore {
        CdcBufferStore::with_object_store(self.db(), self.object_store())
    }

    pub async fn close(&self) -> Result<()> {
        match self.db.close().await {
            Ok(()) => Ok(()),
            Err(err) if matches!(err.kind(), ErrorKind::Closed(CloseReason::Clean)) => Ok(()),
            Err(err) => Err(anyhow!("failed to close SlateDB catalog: {err}")),
        }
    }
}

pub fn catalog_db(catalog: &SlateCatalog) -> Arc<Db> {
    catalog.db()
}

async fn scan_prefix(db: &Db, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
    let range = prefix_bounds(prefix);
    let mut iter = db
        .scan_with_options(range, &ScanOptions::default())
        .await
        .map_err(map_slate_err)?;

    let mut values = Vec::new();
    while let Some(kv) = iter.next().await.map_err(map_slate_err)? {
        values.push(kv.value.to_vec());
    }
    Ok(values)
}

fn prefix_bounds(prefix: &[u8]) -> Range<Vec<u8>> {
    let mut end = prefix.to_vec();
    end.push(0xFF);
    prefix.to_vec()..end
}

fn table_definition_key(name: &str) -> Vec<u8> {
    format!("{TABLE_DEF_PREFIX}{name}").into_bytes()
}

fn source_definition_key(name: &str) -> Vec<u8> {
    format!("{SOURCE_DEF_PREFIX}{name}").into_bytes()
}

fn source_table_key(name: &str) -> Vec<u8> {
    format!("{SOURCE_TABLE_PREFIX}{name}").into_bytes()
}

fn mv_definition_key(name: &str) -> Vec<u8> {
    format!("{MV_DEF_PREFIX}{name}").into_bytes()
}

fn mv_schema_key(name: &str) -> Vec<u8> {
    format!("{MV_SCHEMA_PREFIX}{name}").into_bytes()
}

fn replication_pipeline_definition_key(name: &str) -> Vec<u8> {
    format!("{REPLICATION_PIPELINE_DEF_PREFIX}{name}").into_bytes()
}

fn replication_pipeline_checkpoint_key(name: &str) -> Vec<u8> {
    format!("{REPLICATION_PIPELINE_CHECKPOINT_PREFIX}{name}").into_bytes()
}

fn replication_pipeline_dlq_entry_prefix(pipeline_name: &str) -> Vec<u8> {
    format!("{REPLICATION_PIPELINE_DLQ_PREFIX}{pipeline_name}/").into_bytes()
}

fn replication_pipeline_dlq_entry_key(pipeline_name: &str, dlq_id: &str) -> Vec<u8> {
    format!("{REPLICATION_PIPELINE_DLQ_PREFIX}{pipeline_name}/{dlq_id}").into_bytes()
}

fn replication_pipeline_dlq_payload_object_key(pipeline_name: &str, dlq_id: &str) -> String {
    format!(
        "floe_cdc_dlq_blobs/v1/pipeline/{}/{}.bin",
        hex_component(pipeline_name.as_bytes()),
        dlq_id
    )
}

fn table_row_prefix(name: &str) -> Vec<u8> {
    format!("{TABLE_DATA_PREFIX}{name}/").into_bytes()
}

fn table_row_key(table: &TableDefinition, row: &RowValues) -> Result<Vec<u8>> {
    let pk_index = table.primary_key_index();
    let pk_value = row
        .get(pk_index)
        .cloned()
        .ok_or_else(|| anyhow!("missing value for primary key index {}", pk_index))?;
    let mut key = table_row_prefix(table.name());
    key.extend_from_slice(&encode_key_value(&pk_value)?);
    Ok(key)
}

fn encode_key_value(value: &RowValue) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    match value {
        RowValue::Int64(v) => {
            buf.push(0x01);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        RowValue::Bool(flag) => {
            buf.push(0x02);
            buf.push(if *flag { 1 } else { 0 });
        }
        RowValue::Utf8(text) => {
            buf.push(0x03);
            let bytes = text.as_bytes();
            let len =
                u32::try_from(bytes.len()).map_err(|_| anyhow!("string primary key too large"))?;
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(bytes);
        }
        RowValue::TimestampMillis(value) => {
            buf.push(0x04);
            buf.extend_from_slice(&value.to_be_bytes());
        }
        RowValue::DateDays(value) => {
            buf.push(0x05);
            buf.extend_from_slice(&value.to_be_bytes());
        }
        RowValue::Decimal128(value) => {
            buf.push(0x07);
            buf.extend_from_slice(&value.to_be_bytes());
        }
        RowValue::Numeric(value) => {
            buf.push(0x06);
            let bytes = value.as_bytes();
            let len =
                u32::try_from(bytes.len()).map_err(|_| anyhow!("numeric primary key too large"))?;
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(bytes);
        }
    }
    Ok(buf)
}

fn map_slate_err(err: SlateError) -> anyhow::Error {
    anyhow::Error::new(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use floe_cdc_core::{CdcSourcePosition, CdcTransactionId};
    use floe_core::catalog::{
        CatalogSourceConnector, CatalogSourceDefinition, ColumnDefinition, ColumnType,
        PostgresCdcSourceDefinition, ReplicationBufferMode, ReplicationPipelineDefinition,
        ReplicationPipelineFormat, ReplicationPipelineTarget, SourceBackedTableDefinition,
        TableDefinition,
    };

    #[tokio::test]
    async fn roundtrip_typed_rows() {
        let catalog = SlateCatalog::in_memory().await.expect("open catalog");

        let table = TableDefinition::new(
            "typed_rows",
            vec![
                ColumnDefinition::new_typed("name", ColumnType::Utf8, true),
                ColumnDefinition::new_typed("active", ColumnType::Bool, false),
                ColumnDefinition::new_typed("seen_at", ColumnType::TimestampMillis, false),
            ],
        )
        .unwrap();

        catalog.upsert_table(table.clone()).await.unwrap();

        let row = vec![
            RowValue::Utf8("alice".to_string()),
            RowValue::Bool(true),
            RowValue::TimestampMillis(1_700_000_000_000),
        ];
        catalog.insert_row(&table, &row).await.unwrap();

        let rows = catalog.read_rows(&table).await.unwrap();
        assert_eq!(rows, vec![row]);
    }

    #[tokio::test]
    async fn persists_materialized_view_metadata_and_schema() {
        let catalog = SlateCatalog::in_memory().await.expect("open catalog");
        let metadata = MaterializedViewMetadata::new("mv_meta", "SELECT 1 AS value", false);
        catalog
            .upsert_materialized_view(metadata.clone())
            .await
            .expect("persist metadata");

        let loaded = catalog
            .materialized_view("mv_meta")
            .await
            .expect("load metadata")
            .expect("metadata exists");
        assert_eq!(loaded, metadata);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        catalog
            .save_materialized_view_schema("mv_meta", Arc::clone(&schema))
            .await
            .expect("persist schema");

        let loaded_schema = catalog
            .materialized_view_schema("mv_meta")
            .await
            .expect("load schema")
            .expect("schema exists");
        assert_eq!(loaded_schema.as_ref(), schema.as_ref());
    }

    #[tokio::test]
    async fn roundtrip_catalog_sources_and_source_backed_tables() {
        let catalog = SlateCatalog::in_memory().await.expect("open catalog");
        let source = CatalogSourceDefinition::new(
            "pg_main",
            CatalogSourceConnector::PostgresCdc(
                PostgresCdcSourceDefinition::new(
                    "postgres://postgres:postgres@localhost/postgres",
                    "floe_slot",
                    Some("floe_pub".to_string()),
                    Some(false),
                )
                .expect("postgres source"),
            ),
        )
        .expect("source");
        catalog
            .upsert_catalog_source(source.clone())
            .await
            .expect("persist source");

        let loaded = catalog
            .catalog_source("pg_main")
            .await
            .expect("load source")
            .expect("source exists");
        assert_eq!(loaded, source);
        assert_eq!(catalog.catalog_sources().await.unwrap(), vec![source]);

        let binding = SourceBackedTableDefinition::new("orders", "pg_main", "public.orders")
            .expect("binding");
        catalog
            .upsert_source_backed_table(binding.clone())
            .await
            .expect("persist binding");
        let loaded_binding = catalog
            .source_backed_table("orders")
            .await
            .expect("load binding")
            .expect("binding exists");
        assert_eq!(loaded_binding, binding);
        assert_eq!(catalog.source_backed_tables().await.unwrap(), vec![binding]);
    }

    #[tokio::test]
    async fn roundtrip_replication_pipeline_and_checkpoint() {
        let catalog = SlateCatalog::in_memory().await.expect("open catalog");
        let pipeline = ReplicationPipelineDefinition::new(
            "pg_orders_to_kafka",
            "pg_main",
            "public.orders",
            ReplicationPipelineTarget::Kafka {
                brokers: "localhost:9092".to_string(),
                topic: "orders_cdc".to_string(),
            },
            ReplicationPipelineFormat::DebeziumJson,
            ReplicationBufferMode::Durable,
            floe_core::catalog::ReplicationBufferPolicy::default(),
            true,
            true,
            floe_core::catalog::ReplicationErrorPolicy::default(),
        )
        .expect("pipeline");
        catalog
            .upsert_replication_pipeline(pipeline.clone())
            .await
            .expect("persist pipeline");

        let loaded = catalog
            .replication_pipeline("pg_orders_to_kafka")
            .await
            .expect("load pipeline")
            .expect("pipeline exists");
        assert_eq!(loaded, pipeline);
        assert_eq!(
            catalog.replication_pipelines().await.unwrap(),
            vec![pipeline]
        );

        let mut target_state = BTreeMap::new();
        target_state.insert("kafka.topic".to_string(), "orders_cdc".to_string());
        target_state.insert("kafka.partition.0.offset".to_string(), "42".to_string());
        let checkpoint = ReplicationPipelineCheckpoint::new(
            "pg_orders_to_kafka",
            "pg_main",
            CdcSourcePosition::postgres("0/16B6C50", None).expect("position"),
            Some(CdcTransactionId::new("tx-7").expect("transaction")),
            target_state,
            1_700_000_000_000,
        )
        .expect("checkpoint");
        catalog
            .put_replication_pipeline_checkpoint(checkpoint.clone())
            .await
            .expect("persist checkpoint");
        let loaded_checkpoint = catalog
            .replication_pipeline_checkpoint("pg_orders_to_kafka")
            .await
            .expect("load checkpoint")
            .expect("checkpoint exists");
        assert_eq!(loaded_checkpoint, checkpoint);

        let dlq_payload = b"encoded failed records".to_vec();
        let dlq_payload_object_key = catalog
            .put_replication_pipeline_dlq_payload(
                "pg_orders_to_kafka",
                "0_16B6C50_tx_7",
                dlq_payload.clone(),
            )
            .await
            .expect("persist dlq payload");
        assert_eq!(
            catalog
                .replication_pipeline_dlq_payload(&dlq_payload_object_key)
                .await
                .expect("load dlq payload"),
            dlq_payload
        );
        let dlq_entry = ReplicationPipelineDlqEntry::new(
            "pg_orders_to_kafka",
            "0_16B6C50_tx_7",
            "pg_main",
            CdcSourcePosition::postgres("0/16B6C50", None).expect("position"),
            Some(CdcTransactionId::new("tx-7").expect("transaction")),
            "kafka_delivery",
            "broker unavailable",
            2,
            Some(dlq_payload_object_key),
            Some("kafka_records".to_string()),
            4096,
            BTreeMap::from([("kafka.topic".to_string(), "orders_cdc".to_string())]),
            1_700_000_000_001,
        )
        .expect("dlq entry");
        catalog
            .put_replication_pipeline_dlq_entry(dlq_entry.clone())
            .await
            .expect("persist dlq entry");

        let loaded_dlq_entry = catalog
            .replication_pipeline_dlq_entry("pg_orders_to_kafka", "0_16B6C50_tx_7")
            .await
            .expect("load dlq entry")
            .expect("dlq entry exists");
        assert_eq!(loaded_dlq_entry, dlq_entry);
        assert_eq!(
            catalog
                .replication_pipeline_dlq_entries("pg_orders_to_kafka")
                .await
                .unwrap(),
            vec![dlq_entry]
        );

        let updated_dlq_entry = catalog
            .update_replication_pipeline_dlq_entry_status(
                "pg_orders_to_kafka",
                "0_16B6C50_tx_7",
                ReplicationPipelineDlqStatus::Replayed,
                1_700_000_000_002,
            )
            .await
            .expect("update dlq status")
            .expect("dlq entry exists");
        assert_eq!(
            updated_dlq_entry.status(),
            ReplicationPipelineDlqStatus::Replayed
        );
        assert_eq!(
            updated_dlq_entry.last_updated_at_unix_ms(),
            1_700_000_000_002
        );
        assert_eq!(updated_dlq_entry.status_reason(), None);

        let attempted_dlq_entry = catalog
            .record_replication_pipeline_dlq_retry_attempt(
                "pg_orders_to_kafka",
                "0_16B6C50_tx_7",
                1_700_000_000_003,
            )
            .await
            .expect("record retry attempt")
            .expect("dlq entry exists");
        assert_eq!(attempted_dlq_entry.attempt_count(), 3);
        assert_eq!(
            attempted_dlq_entry.last_updated_at_unix_ms(),
            1_700_000_000_003
        );

        let discarded_dlq_entry = catalog
            .update_replication_pipeline_dlq_entry_status_with_reason(
                "pg_orders_to_kafka",
                "0_16B6C50_tx_7",
                ReplicationPipelineDlqStatus::Discarded,
                Some("operator skipped duplicate".to_string()),
                1_700_000_000_004,
            )
            .await
            .expect("discard dlq entry")
            .expect("dlq entry exists");
        assert_eq!(
            discarded_dlq_entry.status(),
            ReplicationPipelineDlqStatus::Discarded
        );
        assert_eq!(
            discarded_dlq_entry.status_reason(),
            Some("operator skipped duplicate")
        );
    }

    #[tokio::test]
    async fn roundtrip_postgres_replication_pipeline_target() {
        let catalog = SlateCatalog::in_memory().await.expect("open catalog");
        let pipeline = ReplicationPipelineDefinition::new(
            "pg_orders_to_postgres",
            "pg_main",
            "public.orders",
            ReplicationPipelineTarget::Postgres {
                connection: "postgres://postgres:postgres@localhost/postgres".to_string(),
                table: "public.orders_copy".to_string(),
            },
            ReplicationPipelineFormat::FloeJson,
            ReplicationBufferMode::Durable,
            floe_core::catalog::ReplicationBufferPolicy::default(),
            false,
            false,
            floe_core::catalog::ReplicationErrorPolicy::default(),
        )
        .expect("pipeline");

        catalog
            .upsert_replication_pipeline(pipeline.clone())
            .await
            .expect("persist pipeline");

        let loaded = catalog
            .replication_pipeline("pg_orders_to_postgres")
            .await
            .expect("load pipeline")
            .expect("pipeline exists");
        assert_eq!(loaded, pipeline);
    }

    #[tokio::test]
    async fn roundtrip_table_definitions() {
        let catalog = SlateCatalog::in_memory().await.expect("open catalog");

        let table = TableDefinition::new(
            "stream",
            vec![
                ColumnDefinition::new("id", true),
                ColumnDefinition::new("value", false),
            ],
        )
        .unwrap();

        catalog.upsert_table(table.clone()).await.unwrap();

        let loaded = catalog.table("stream").await.unwrap().unwrap();
        assert_eq!(loaded.name(), "stream");
        assert_eq!(loaded.columns().len(), 2);

        catalog
            .insert_row(&table, &vec![RowValue::Int64(1), RowValue::Int64(10)])
            .await
            .unwrap();
        catalog
            .insert_row(&table, &vec![RowValue::Int64(2), RowValue::Int64(20)])
            .await
            .unwrap();

        let rows = catalog.read_rows(&table).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&vec![RowValue::Int64(1), RowValue::Int64(10)]));
        assert!(rows.contains(&vec![RowValue::Int64(2), RowValue::Int64(20)]));
    }

    #[tokio::test]
    async fn close_is_idempotent() {
        let catalog = SlateCatalog::in_memory().await.expect("open catalog");
        catalog.close().await.expect("close catalog");
        catalog.close().await.expect("close catalog again");
    }
}
