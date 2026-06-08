use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use dbsp_storage::storage::KeyValueTable;
use floe_cdc_core::{CdcSourceDefinition, CdcSourceId, CdcTableDefinition, CdcTableId};
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

use crate::json::{decode_json, decode_json_value};
use crate::keys::{
    source_metadata_key, source_metadata_prefix, source_table_index_key, source_table_index_prefix,
    table_metadata_key,
};

#[derive(Clone)]
pub struct CdcMetadataStore {
    table: Arc<dyn KeyValueTable>,
}

impl CdcMetadataStore {
    pub fn new(table: Arc<dyn KeyValueTable>) -> Self {
        Self { table }
    }

    pub async fn upsert_source(&self, source: &CdcSourceDefinition) -> Result<()> {
        let encoded = serde_json::to_vec(source).with_context(|| {
            format!(
                "encode CDC source metadata for '{}'",
                source.source_id().as_str()
            )
        })?;
        self.table
            .put(&source_metadata_key(source.source_id())?, &encoded)
            .await
            .with_context(|| {
                format!(
                    "persist CDC source metadata for '{}'",
                    source.source_id().as_str()
                )
            })
    }

    pub async fn load_source(
        &self,
        source_id: &CdcSourceId,
    ) -> Result<Option<CdcSourceDefinition>> {
        let Some(bytes) = self
            .table
            .get(&source_metadata_key(source_id)?)
            .await
            .with_context(|| format!("load CDC source metadata for '{}'", source_id.as_str()))?
        else {
            return Ok(None);
        };
        decode_json(&bytes, "CDC source metadata")
    }

    pub async fn sources(&self) -> Result<Vec<CdcSourceDefinition>> {
        self.table
            .scan_prefix(source_metadata_prefix().as_slice(), &ScanOptions::default())
            .await
            .context("scan CDC source metadata")?
            .into_iter()
            .map(|(_, value)| decode_json_value(&value, "CDC source metadata"))
            .collect()
    }

    pub async fn upsert_table(&self, table_definition: &CdcTableDefinition) -> Result<()> {
        let source = self
            .load_source(table_definition.source_id())
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "CDC source '{}' does not exist",
                    table_definition.source_id().as_str()
                )
            })?;
        source.validate_table_definition(table_definition)?;

        let previous = self.load_table(table_definition.table_id()).await?;
        let encoded = serde_json::to_vec(table_definition).with_context(|| {
            format!(
                "encode CDC table metadata for '{}'",
                table_definition.table_id().as_str()
            )
        })?;

        let mut batch = WriteBatch::new();
        batch.put(
            table_metadata_key(table_definition.table_id())?,
            encoded.clone(),
        );
        batch.put(
            source_table_index_key(table_definition.source_id(), table_definition.table_id())?,
            encoded,
        );
        if let Some(previous) = previous
            && previous.source_id() != table_definition.source_id()
        {
            batch.delete(source_table_index_key(
                previous.source_id(),
                previous.table_id(),
            )?);
        }

        self.table.write_batch(batch).await.with_context(|| {
            format!(
                "persist CDC table metadata for '{}'",
                table_definition.table_id().as_str()
            )
        })
    }

    pub async fn load_table(&self, table_id: &CdcTableId) -> Result<Option<CdcTableDefinition>> {
        let Some(bytes) = self
            .table
            .get(&table_metadata_key(table_id)?)
            .await
            .with_context(|| format!("load CDC table metadata for '{}'", table_id.as_str()))?
        else {
            return Ok(None);
        };
        decode_json(&bytes, "CDC table metadata")
    }

    pub async fn tables_for_source(
        &self,
        source_id: &CdcSourceId,
    ) -> Result<Vec<CdcTableDefinition>> {
        self.table
            .scan_prefix(
                source_table_index_prefix(source_id)?.as_slice(),
                &ScanOptions::default(),
            )
            .await
            .with_context(|| format!("scan CDC table metadata for '{}'", source_id.as_str()))?
            .into_iter()
            .map(|(_, value)| decode_json_value(&value, "CDC table metadata"))
            .collect()
    }
}
