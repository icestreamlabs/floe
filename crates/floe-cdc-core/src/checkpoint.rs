use std::collections::BTreeMap;

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

use crate::change::{CdcSchemaVersionMap, TransactionBatch};
use crate::ids::{CdcSourceId, CdcTransactionId};
use crate::position::CdcSourcePosition;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcCheckpoint {
    source_id: CdcSourceId,
    position: CdcSourcePosition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transaction_id: Option<CdcTransactionId>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    schema_versions: CdcSchemaVersionMap,
}

impl CdcCheckpoint {
    pub fn new(
        source_id: CdcSourceId,
        position: CdcSourcePosition,
        transaction_id: Option<CdcTransactionId>,
    ) -> Self {
        Self {
            source_id,
            position,
            transaction_id,
            schema_versions: CdcSchemaVersionMap::new(),
        }
    }

    pub fn with_schema_versions(mut self, schema_versions: CdcSchemaVersionMap) -> Self {
        self.schema_versions = schema_versions;
        self
    }

    pub fn from_transaction(transaction: &TransactionBatch) -> Self {
        Self {
            source_id: transaction.source_id().clone(),
            position: transaction.commit_position().clone(),
            transaction_id: transaction.transaction_id().cloned(),
            schema_versions: transaction.schema_versions().clone(),
        }
    }

    pub fn source_id(&self) -> &CdcSourceId {
        &self.source_id
    }

    pub fn position(&self) -> &CdcSourcePosition {
        &self.position
    }

    pub fn transaction_id(&self) -> Option<&CdcTransactionId> {
        self.transaction_id.as_ref()
    }

    pub fn schema_versions(&self) -> &CdcSchemaVersionMap {
        &self.schema_versions
    }

    pub fn covers(&self, other: &Self) -> Result<bool> {
        ensure!(
            self.source_id == other.source_id,
            "CDC checkpoint source '{}' cannot cover source '{}'",
            self.source_id.as_str(),
            other.source_id.as_str()
        );
        self.position.covers(&other.position)
    }
}
