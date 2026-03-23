use std::io::Cursor;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, ensure};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use floe_core::catalog::TableDefinition;
use floe_core::encoding::{self, ArchivedRow};
use floe_core::{RowValue, RowValues};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use serde::{Deserialize, Serialize};
use slatedb::config::{ScanOptions, Settings};
use slatedb::{CloseReason, Db, Error as SlateError, ErrorKind};
use tokio::fs;

const TABLE_DEF_PREFIX: &str = "meta/table/";
const TABLE_DATA_PREFIX: &str = "data/";
const MV_DEF_PREFIX: &str = "meta/mv/definition/";
const MV_SCHEMA_PREFIX: &str = "meta/mv/schema/";

#[derive(Clone)]
pub struct SlateCatalog {
    db: Arc<Db>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MaterializedViewMetadata {
    name: String,
    query: String,
    if_not_exists: bool,
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
        let builder = Db::builder(name.into(), object_store);
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
        Ok(Self { db: Arc::new(db) })
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

fn mv_definition_key(name: &str) -> Vec<u8> {
    format!("{MV_DEF_PREFIX}{name}").into_bytes()
}

fn mv_schema_key(name: &str) -> Vec<u8> {
    format!("{MV_SCHEMA_PREFIX}{name}").into_bytes()
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
    use floe_core::catalog::{ColumnDefinition, ColumnType, TableDefinition};

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
