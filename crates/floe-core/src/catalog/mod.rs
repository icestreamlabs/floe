use anyhow::{Result, anyhow, ensure};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};

use crate::{RowValue, RowValues};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ColumnType {
    Int64,
    Bool,
    Utf8,
    TimestampMillis,
}

impl ColumnType {
    pub fn arrow_type(&self) -> DataType {
        match self {
            ColumnType::Int64 => DataType::Int64,
            ColumnType::Bool => DataType::Boolean,
            ColumnType::Utf8 => DataType::Utf8,
            ColumnType::TimestampMillis => DataType::Timestamp(TimeUnit::Millisecond, None),
        }
    }

    pub fn matches_value(&self, value: &RowValue) -> bool {
        matches!(
            (self, value),
            (ColumnType::Int64, RowValue::Int64(_))
                | (ColumnType::Bool, RowValue::Bool(_))
                | (ColumnType::Utf8, RowValue::Utf8(_))
                | (ColumnType::TimestampMillis, RowValue::TimestampMillis(_))
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnDefinition {
    name: String,
    data_type: ColumnType,
    primary_key: bool,
    #[serde(default)]
    nullable: bool,
}

impl ColumnDefinition {
    pub fn new(name: impl Into<String>, primary_key: bool) -> Self {
        Self::new_typed(name, ColumnType::Int64, primary_key)
    }

    pub fn new_typed(name: impl Into<String>, data_type: ColumnType, primary_key: bool) -> Self {
        Self::new_typed_nullable(name, data_type, false, primary_key)
    }

    pub fn new_typed_nullable(
        name: impl Into<String>,
        data_type: ColumnType,
        nullable: bool,
        primary_key: bool,
    ) -> Self {
        Self {
            name: name.into(),
            data_type,
            primary_key,
            nullable: nullable && !primary_key,
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

    pub fn nullable(&self) -> bool {
        self.nullable
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
            .map(|column| {
                Field::new(
                    column.name(),
                    column.data_type().arrow_type(),
                    column.nullable(),
                )
            })
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
        for (idx, (column, value)) in self.columns.iter().zip(row.iter()).enumerate() {
            if !column.data_type().matches_value(value) {
                return Err(anyhow!(
                    "row value at index {idx} does not match column type {:?}",
                    column.data_type()
                ));
            }
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
    fn validate_row_rejects_mismatched_types() {
        let table = TableDefinition::new(
            "typed",
            vec![
                ColumnDefinition::new_typed("flag", ColumnType::Bool, true),
                ColumnDefinition::new_typed("label", ColumnType::Utf8, false),
            ],
        )
        .expect("valid table");

        let ok = vec![RowValue::Bool(true), RowValue::Utf8("ok".to_string())];
        table.validate_row(&ok).expect("row should validate");

        let bad = vec![RowValue::Int64(1), RowValue::Utf8("bad".to_string())];
        assert!(table.validate_row(&bad).is_err());
    }

    #[test]
    fn column_type_maps_to_arrow_types() {
        let table = TableDefinition::new(
            "types",
            vec![
                ColumnDefinition::new_typed("id", ColumnType::Int64, true),
                ColumnDefinition::new_typed("flag", ColumnType::Bool, false),
                ColumnDefinition::new_typed("label", ColumnType::Utf8, false),
                ColumnDefinition::new_typed("ts", ColumnType::TimestampMillis, false),
            ],
        )
        .expect("valid table");

        let schema = table.to_arrow_schema();
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        assert_eq!(
            schema.field(3).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
    }

    #[test]
    fn deserialize_accepts_valid_table_definition() {
        let json =
            r#"{"name":"ok","columns":[{"name":"id","data_type":"Int64","primary_key":true}]}"#;
        let table = serde_json::from_str::<TableDefinition>(json).expect("valid table");
        assert_eq!(table.name(), "ok");
        assert_eq!(table.columns().len(), 1);
    }

    #[test]
    fn preserves_nullable_column_metadata() {
        let table = TableDefinition::new(
            "nullable_cols",
            vec![
                ColumnDefinition::new_typed("id", ColumnType::Int64, true),
                ColumnDefinition::new_typed_nullable("note", ColumnType::Utf8, true, false),
            ],
        )
        .expect("valid table");
        let encoded = serde_json::to_string(&table).expect("serialize");
        let decoded = serde_json::from_str::<TableDefinition>(&encoded).expect("deserialize");
        assert!(!decoded.columns()[0].nullable());
        assert!(decoded.columns()[1].nullable());
    }
}
