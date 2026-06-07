use std::collections::{BTreeMap, HashMap};

use anyhow::{Result, bail, ensure};
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::ids::{CdcSourceId, CdcTableId, CdcTransactionId};
use crate::position::CdcSourcePosition;
use crate::row::{CdcColumnarRowBatch, CdcRow, CdcRowKey};
use crate::schema::CdcTableSchema;

pub type CdcSchemaVersionMap = BTreeMap<String, u64>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CdcOperation {
    Insert,
    Update,
    Delete,
    Truncate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CdcChange {
    Insert {
        row: CdcRow,
    },
    Update {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key: Option<CdcRowKey>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        before: Option<CdcRow>,
        after: CdcRow,
    },
    Delete {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key: Option<CdcRowKey>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        before: Option<CdcRow>,
    },
    Truncate,
}

impl CdcChange {
    pub fn operation(&self) -> CdcOperation {
        match self {
            CdcChange::Insert { .. } => CdcOperation::Insert,
            CdcChange::Update { .. } => CdcOperation::Update,
            CdcChange::Delete { .. } => CdcOperation::Delete,
            CdcChange::Truncate => CdcOperation::Truncate,
        }
    }

    pub fn validate_against_schema(&self, schema: &CdcTableSchema) -> Result<()> {
        match self {
            CdcChange::Insert { row } => {
                schema.validate_row(row)?;
                schema.primary_key_from_row(row)?;
            }
            CdcChange::Update { key, before, after } => {
                if let Some(key) = key {
                    key.validate_against_schema(schema)?;
                }
                if let Some(before) = before {
                    schema.validate_row_allowing_unchanged_toast(before)?;
                    schema.primary_key_from_row_allowing_unchanged_toast(before)?;
                }
                schema.validate_row_allowing_unchanged_toast(after)?;
                schema.primary_key_from_row_allowing_unchanged_toast(after)?;
            }
            CdcChange::Delete { key, before } => {
                ensure!(
                    key.is_some() || before.is_some(),
                    "CDC delete requires a key or before row"
                );
                if let Some(key) = key {
                    key.validate_against_schema(schema)?;
                }
                if let Some(before) = before {
                    schema.validate_row_allowing_unchanged_toast(before)?;
                    schema.primary_key_from_row_allowing_unchanged_toast(before)?;
                }
            }
            CdcChange::Truncate => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChangeBatch {
    table_id: CdcTableId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    changes: Vec<CdcChange>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_snapshot_insert_rows",
        deserialize_with = "deserialize_snapshot_insert_rows"
    )]
    snapshot_insert_rows: Option<Arc<CdcColumnarRowBatch>>,
}

impl ChangeBatch {
    pub fn new(table_id: CdcTableId, changes: Vec<CdcChange>) -> Result<Self> {
        ensure!(!changes.is_empty(), "CDC change batch cannot be empty");
        Ok(Self {
            table_id,
            changes,
            snapshot_insert_rows: None,
        })
    }

    pub fn new_snapshot_insert(table_id: CdcTableId, rows: CdcColumnarRowBatch) -> Result<Self> {
        ensure!(
            rows.row_count() > 0,
            "CDC snapshot insert batch cannot be empty"
        );
        Ok(Self {
            table_id,
            changes: Vec::new(),
            snapshot_insert_rows: Some(Arc::new(rows)),
        })
    }

    pub fn table_id(&self) -> &CdcTableId {
        &self.table_id
    }

    pub fn changes(&self) -> &[CdcChange] {
        &self.changes
    }

    pub fn snapshot_insert_rows(&self) -> Option<&CdcColumnarRowBatch> {
        self.snapshot_insert_rows.as_deref()
    }

    pub fn change_count(&self) -> usize {
        self.snapshot_insert_rows
            .as_ref()
            .map(|rows| rows.row_count())
            .unwrap_or(self.changes.len())
    }

    pub fn validate_against_schema(&self, schema: &CdcTableSchema) -> Result<()> {
        ensure!(
            &self.table_id == schema.table_id(),
            "CDC change batch table '{}' does not match schema table '{}'",
            self.table_id.as_str(),
            schema.table_id().as_str()
        );
        ensure!(
            !self.changes.is_empty() || self.snapshot_insert_rows.is_some(),
            "CDC change batch cannot be empty"
        );
        ensure!(
            self.changes.is_empty() || self.snapshot_insert_rows.is_none(),
            "CDC change batch cannot mix row changes with snapshot insert rows"
        );
        if let Some(rows) = &self.snapshot_insert_rows {
            schema.validate_columnar_rows(rows)?;
            return Ok(());
        }
        for change in &self.changes {
            change.validate_against_schema(schema)?;
        }
        Ok(())
    }
}

fn serialize_snapshot_insert_rows<S>(
    rows: &Option<Arc<CdcColumnarRowBatch>>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    rows.as_deref().serialize(serializer)
}

fn deserialize_snapshot_insert_rows<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Arc<CdcColumnarRowBatch>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<CdcColumnarRowBatch>::deserialize(deserializer).map(|rows| rows.map(Arc::new))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionBatch {
    source_id: CdcSourceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transaction_id: Option<CdcTransactionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    start_position: Option<CdcSourcePosition>,
    commit_position: CdcSourcePosition,
    change_batches: Vec<ChangeBatch>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    schema_versions: CdcSchemaVersionMap,
}

impl TransactionBatch {
    pub fn new(
        source_id: CdcSourceId,
        transaction_id: Option<CdcTransactionId>,
        start_position: Option<CdcSourcePosition>,
        commit_position: CdcSourcePosition,
        change_batches: Vec<ChangeBatch>,
    ) -> Result<Self> {
        ensure!(
            !change_batches.is_empty(),
            "CDC transaction batch cannot be empty"
        );
        Ok(Self {
            source_id,
            transaction_id,
            start_position,
            commit_position,
            change_batches,
            schema_versions: BTreeMap::new(),
        })
    }

    pub fn with_schema_versions(mut self, schema_versions: CdcSchemaVersionMap) -> Self {
        self.schema_versions = schema_versions;
        self
    }

    pub fn source_id(&self) -> &CdcSourceId {
        &self.source_id
    }

    pub fn transaction_id(&self) -> Option<&CdcTransactionId> {
        self.transaction_id.as_ref()
    }

    pub fn start_position(&self) -> Option<&CdcSourcePosition> {
        self.start_position.as_ref()
    }

    pub fn commit_position(&self) -> &CdcSourcePosition {
        &self.commit_position
    }

    pub fn change_batches(&self) -> &[ChangeBatch] {
        &self.change_batches
    }

    pub fn schema_versions(&self) -> &CdcSchemaVersionMap {
        &self.schema_versions
    }

    pub fn validate_against_schemas(
        &self,
        schemas: &HashMap<CdcTableId, CdcTableSchema>,
    ) -> Result<()> {
        for batch in &self.change_batches {
            let Some(schema) = schemas.get(batch.table_id()) else {
                bail!(
                    "CDC transaction batch references unknown table '{}'",
                    batch.table_id().as_str()
                );
            };
            batch.validate_against_schema(schema)?;
        }
        Ok(())
    }
}
