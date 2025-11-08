use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use floe_core::RowValues;
use floe_core::catalog::TableDefinition;
use floe_core::encoding::{self, ArchivedRow};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use slatedb::config::ScanOptions;
use slatedb::{Db, Error as SlateError};
use tokio::fs;

const TABLE_DEF_PREFIX: &str = "meta/table/";
const TABLE_DATA_PREFIX: &str = "data/";

#[derive(Clone)]
pub struct SlateCatalog {
    db: Arc<Db>,
}

impl SlateCatalog {
    pub async fn in_memory() -> Result<Self> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Self::with_object_store("in-memory", object_store).await
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
        Self::with_object_store("floe", object_store).await
    }

    pub async fn with_object_store(
        name: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let db = Db::open(name.into(), object_store)
            .await
            .map_err(|err| anyhow!("unable to open SlateDB: {err}"))?;
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

    pub fn db(&self) -> Arc<Db> {
        self.db.clone()
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

fn table_row_prefix(name: &str) -> Vec<u8> {
    format!("{TABLE_DATA_PREFIX}{name}/").into_bytes()
}

fn table_row_key(table: &TableDefinition, row: &RowValues) -> Result<Vec<u8>> {
    let pk_index = table.primary_key_index();
    let pk_value = row
        .get(pk_index)
        .copied()
        .ok_or_else(|| anyhow!("missing value for primary key index {}", pk_index))?;
    let mut key = table_row_prefix(table.name());
    key.extend_from_slice(&pk_value.to_be_bytes());
    Ok(key)
}

fn map_slate_err(err: SlateError) -> anyhow::Error {
    anyhow!(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use floe_core::catalog::{ColumnDefinition, TableDefinition};

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

        catalog.insert_row(&table, &vec![1, 10]).await.unwrap();
        catalog.insert_row(&table, &vec![2, 20]).await.unwrap();

        let mut rows = catalog.read_rows(&table).await.unwrap();
        rows.sort();
        assert_eq!(rows, vec![vec![1, 10], vec![2, 20]]);
    }
}
