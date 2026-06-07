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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicationPipelineDlqStats {
    pending_entries: usize,
    replayed_entries: usize,
    discarded_entries: usize,
    oldest_pending_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationPipelineDlqStatus {
    #[default]
    Pending,
    Replayed,
    Discarded,
}

mod keys;
mod metadata;
#[cfg(test)]
mod tests;

pub use keys::catalog_db;
use keys::*;

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

    pub async fn replication_pipeline_dlq_stats(
        &self,
        pipeline_name: &str,
        now_unix_ms: u64,
    ) -> Result<ReplicationPipelineDlqStats> {
        let prefix = replication_pipeline_dlq_entry_prefix(pipeline_name);
        let mut iter = self
            .db
            .scan_with_options(keys::prefix_bounds(&prefix), &ScanOptions::default())
            .await
            .map_err(map_slate_err)?;
        let mut stats = ReplicationPipelineDlqStats::default();
        while let Some(kv) = iter.next().await.map_err(map_slate_err)? {
            let entry: ReplicationPipelineDlqEntry = serde_json::from_slice(&kv.value)
                .with_context(|| {
                    format!(
                        "failed to deserialize replication pipeline '{pipeline_name}' DLQ entry"
                    )
                })?;
            stats.record_entry(&entry, now_unix_ms);
        }
        Ok(stats)
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
