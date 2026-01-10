use anyhow::{Result, anyhow, ensure};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};

use crate::RowValues;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ColumnType {
    Int64,
}

impl ColumnType {
    pub fn arrow_type(&self) -> DataType {
        match self {
            ColumnType::Int64 => DataType::Int64,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnDefinition {
    name: String,
    data_type: ColumnType,
    primary_key: bool,
}

impl ColumnDefinition {
    pub fn new(name: impl Into<String>, primary_key: bool) -> Self {
        Self {
            name: name.into(),
            data_type: ColumnType::Int64,
            primary_key,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &ColumnType {
        &self.data_type
    }

    pub fn is_primary_key(&self) -> bool {
        self.primary_key
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TableDefinition {
    name: String,
    columns: Vec<ColumnDefinition>,
}

impl<'de> Deserialize<'de> for TableDefinition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct TableDefinitionData {
            name: String,
            columns: Vec<ColumnDefinition>,
        }

        let data = TableDefinitionData::deserialize(deserializer)?;
        TableDefinition::new(data.name, data.columns).map_err(de::Error::custom)
    }
}

impl TableDefinition {
    pub fn new(name: impl Into<String>, columns: Vec<ColumnDefinition>) -> Result<Self> {
        let name = name.into();
        ensure!(!name.trim().is_empty(), "table name cannot be empty");
        ensure!(
            !columns.is_empty(),
            "table {} must have at least one column",
            name
        );
        let pk_count = columns.iter().filter(|c| c.is_primary_key()).count();
        ensure!(
            pk_count == 1,
            "table {} must define exactly one primary key column",
            name
        );
        Ok(Self { name, columns })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[ColumnDefinition] {
        &self.columns
    }

    pub fn primary_key_index(&self) -> usize {
        self.columns
            .iter()
            .position(|column| column.is_primary_key())
            .expect("table definition validated to contain a primary key")
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|column| column.name() == name)
    }

    pub fn to_arrow_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|column| Field::new(column.name(), column.data_type().arrow_type(), false))
            .collect();
        SchemaRef::new(Schema::new(fields))
    }

    pub fn validate_row(&self, row: &RowValues) -> Result<()> {
        if row.len() != self.columns.len() {
            return Err(anyhow!(
                "row length {} does not match column count {}",
                row.len(),
                self.columns.len()
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_rejects_invalid_table_definition() {
        let cases = [
            r#"{"name":"","columns":[{"name":"id","data_type":"Int64","primary_key":true}]}"#,
            r#"{"name":"missing_pk","columns":[{"name":"id","data_type":"Int64","primary_key":false}]}"#,
            r#"{"name":"two_pk","columns":[{"name":"id","data_type":"Int64","primary_key":true},{"name":"other","data_type":"Int64","primary_key":true}]}"#,
            r#"{"name":"no_cols","columns":[]}"#,
        ];

        for json in cases {
            let result = serde_json::from_str::<TableDefinition>(json);
            assert!(result.is_err(), "expected error for {json}");
        }
    }

    #[test]
    fn deserialize_accepts_valid_table_definition() {
        let json =
            r#"{"name":"ok","columns":[{"name":"id","data_type":"Int64","primary_key":true}]}"#;
        let table = serde_json::from_str::<TableDefinition>(json).expect("valid table");
        assert_eq!(table.name(), "ok");
        assert_eq!(table.columns().len(), 1);
    }
}
