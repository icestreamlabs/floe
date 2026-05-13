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
    DateDays,
    Decimal128 { precision: u8, scale: i8 },
    Numeric,
}

impl ColumnType {
    pub fn decimal128(precision: u8, scale: i8) -> Result<Self> {
        ensure!(
            (1..=38).contains(&precision),
            "Decimal128 precision must be between 1 and 38, got {precision}"
        );
        ensure!(
            scale >= 0 && scale <= precision as i8,
            "Decimal128 scale must be between 0 and precision {precision}, got {scale}"
        );
        Ok(Self::Decimal128 { precision, scale })
    }

    pub fn arrow_type(&self) -> DataType {
        match self {
            ColumnType::Int64 => DataType::Int64,
            ColumnType::Bool => DataType::Boolean,
            ColumnType::Utf8 => DataType::Utf8,
            ColumnType::TimestampMillis => DataType::Timestamp(TimeUnit::Millisecond, None),
            ColumnType::DateDays => DataType::Date32,
            ColumnType::Decimal128 { precision, scale } => DataType::Decimal128(*precision, *scale),
            ColumnType::Numeric => DataType::Utf8,
        }
    }

    pub fn matches_value(&self, value: &RowValue) -> bool {
        matches!(
            (self, value),
            (ColumnType::Int64, RowValue::Int64(_))
                | (ColumnType::Bool, RowValue::Bool(_))
                | (ColumnType::Utf8, RowValue::Utf8(_))
                | (ColumnType::TimestampMillis, RowValue::TimestampMillis(_))
                | (ColumnType::DateDays, RowValue::DateDays(_))
                | (ColumnType::Decimal128 { .. }, RowValue::Decimal128(_))
                | (ColumnType::Numeric, RowValue::Numeric(_))
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogSourceDefinition {
    name: String,
    connector: CatalogSourceConnector,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CatalogSourceConnector {
    PostgresCdc(PostgresCdcSourceDefinition),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostgresCdcSourceDefinition {
    connection: String,
    slot: String,
    publication: Option<String>,
    #[serde(default)]
    include_schema_in_source: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceBackedTableDefinition {
    table_name: String,
    source_name: String,
    upstream_table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationPipelineDefinition {
    name: String,
    source_name: String,
    upstream_table: String,
    target: ReplicationPipelineTarget,
    format: ReplicationPipelineFormat,
    #[serde(default = "default_replication_buffer_mode")]
    buffer_mode: ReplicationBufferMode,
    #[serde(default)]
    buffer_policy: ReplicationBufferPolicy,
    #[serde(default)]
    emit_tombstones: bool,
    #[serde(default)]
    include_transaction_metadata: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ReplicationBufferPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_pending_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_pending_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReplicationPipelineTarget {
    Kafka { brokers: String, topic: String },
    Postgres { connection: String, table: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationPipelineFormat {
    DebeziumJson,
    ArrowIpc,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationBufferMode {
    Durable,
    NoBuffer,
}

fn default_replication_buffer_mode() -> ReplicationBufferMode {
    ReplicationBufferMode::Durable
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

impl CatalogSourceDefinition {
    pub fn new(name: impl Into<String>, connector: CatalogSourceConnector) -> Result<Self> {
        let name = name.into();
        ensure!(!name.trim().is_empty(), "source name cannot be empty");
        Ok(Self { name, connector })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn connector(&self) -> &CatalogSourceConnector {
        &self.connector
    }
}

impl PostgresCdcSourceDefinition {
    pub fn new(
        connection: impl Into<String>,
        slot: impl Into<String>,
        publication: Option<String>,
        include_schema_in_source: Option<bool>,
    ) -> Result<Self> {
        let connection = connection.into();
        let slot = slot.into();
        ensure!(
            !connection.trim().is_empty(),
            "Postgres CDC source connection cannot be empty"
        );
        ensure!(
            !slot.trim().is_empty(),
            "Postgres CDC source slot cannot be empty"
        );
        Ok(Self {
            connection,
            slot,
            publication,
            include_schema_in_source,
        })
    }

    pub fn connection(&self) -> &str {
        &self.connection
    }

    pub fn slot(&self) -> &str {
        &self.slot
    }

    pub fn publication(&self) -> Option<&str> {
        self.publication.as_deref()
    }

    pub fn include_schema_in_source(&self) -> Option<bool> {
        self.include_schema_in_source
    }
}

impl SourceBackedTableDefinition {
    pub fn new(
        table_name: impl Into<String>,
        source_name: impl Into<String>,
        upstream_table: impl Into<String>,
    ) -> Result<Self> {
        let table_name = table_name.into();
        let source_name = source_name.into();
        let upstream_table = upstream_table.into();
        ensure!(!table_name.trim().is_empty(), "table name cannot be empty");
        ensure!(
            !source_name.trim().is_empty(),
            "source name cannot be empty"
        );
        ensure!(
            !upstream_table.trim().is_empty(),
            "upstream table cannot be empty"
        );
        Ok(Self {
            table_name,
            source_name,
            upstream_table,
        })
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn upstream_table(&self) -> &str {
        &self.upstream_table
    }
}

impl ReplicationPipelineDefinition {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: impl Into<String>,
        source_name: impl Into<String>,
        upstream_table: impl Into<String>,
        target: ReplicationPipelineTarget,
        format: ReplicationPipelineFormat,
        buffer_mode: ReplicationBufferMode,
        buffer_policy: ReplicationBufferPolicy,
        emit_tombstones: bool,
        include_transaction_metadata: bool,
    ) -> Result<Self> {
        let name = name.into();
        let source_name = source_name.into();
        let upstream_table = upstream_table.into();
        ensure!(
            !name.trim().is_empty(),
            "replication pipeline name cannot be empty"
        );
        ensure!(
            !source_name.trim().is_empty(),
            "replication pipeline source name cannot be empty"
        );
        ensure!(
            !upstream_table.trim().is_empty(),
            "replication pipeline upstream table cannot be empty"
        );
        target.validate()?;
        Ok(Self {
            name,
            source_name,
            upstream_table,
            target,
            format,
            buffer_mode,
            buffer_policy,
            emit_tombstones,
            include_transaction_metadata,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn upstream_table(&self) -> &str {
        &self.upstream_table
    }

    pub fn target(&self) -> &ReplicationPipelineTarget {
        &self.target
    }

    pub fn format(&self) -> ReplicationPipelineFormat {
        self.format
    }

    pub fn buffer_mode(&self) -> ReplicationBufferMode {
        self.buffer_mode
    }

    pub fn buffer_policy(&self) -> ReplicationBufferPolicy {
        self.buffer_policy
    }

    pub fn emit_tombstones(&self) -> bool {
        self.emit_tombstones
    }

    pub fn include_transaction_metadata(&self) -> bool {
        self.include_transaction_metadata
    }
}

impl ReplicationBufferPolicy {
    pub fn new(max_pending_bytes: Option<usize>, max_pending_age_ms: Option<u64>) -> Self {
        Self {
            max_pending_bytes,
            max_pending_age_ms,
        }
    }

    pub fn max_pending_bytes(&self) -> Option<usize> {
        self.max_pending_bytes
    }

    pub fn max_pending_age_ms(&self) -> Option<u64> {
        self.max_pending_age_ms
    }
}

impl ReplicationPipelineTarget {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Kafka { brokers, topic } => {
                ensure!(
                    !brokers.trim().is_empty(),
                    "replication pipeline Kafka brokers cannot be empty"
                );
                ensure!(
                    !topic.trim().is_empty(),
                    "replication pipeline Kafka topic cannot be empty"
                );
            }
            Self::Postgres { connection, table } => {
                ensure!(
                    !connection.trim().is_empty(),
                    "replication pipeline Postgres connection cannot be empty"
                );
                ensure!(
                    !table.trim().is_empty(),
                    "replication pipeline Postgres target table cannot be empty"
                );
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
