use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CdcSourceId(String);

impl CdcSourceId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        ensure!(!value.trim().is_empty(), "CDC source id cannot be empty");
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CdcTableId(String);

impl CdcTableId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        ensure!(!value.trim().is_empty(), "CDC table id cannot be empty");
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UpstreamTableRef {
    schema: String,
    table: String,
}

impl UpstreamTableRef {
    pub fn new(schema: impl Into<String>, table: impl Into<String>) -> Result<Self> {
        let schema = schema.into();
        let table = table.into();
        ensure!(
            !schema.trim().is_empty(),
            "upstream table schema cannot be empty"
        );
        ensure!(
            !table.trim().is_empty(),
            "upstream table name cannot be empty"
        );
        Ok(Self { schema, table })
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub fn table(&self) -> &str {
        &self.table
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcTransactionId(String);

impl CdcTransactionId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        ensure!(
            !value.trim().is_empty(),
            "CDC transaction id cannot be empty"
        );
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}
