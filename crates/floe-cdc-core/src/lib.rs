use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result, bail, ensure};
use floe_core::RowValue;
use floe_core::catalog::ColumnType;
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CdcSourceCategory {
    AppendOnly,
    Upsert,
    NativeDatabaseCdc,
    Changelog,
    ObjectBatch,
    ExternalTable,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DirectQuerySupport {
    Full,
    StatelessOnly,
    None,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TableMaterializationRequirement {
    Optional,
    RequiredForStatefulQueries,
    AlwaysRequired,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcSourceSemantics {
    category: CdcSourceCategory,
    direct_query_support: DirectQuerySupport,
    table_materialization: TableMaterializationRequirement,
    primary_key_required_for_table: bool,
}

impl CdcSourceSemantics {
    pub fn for_category(category: CdcSourceCategory) -> Self {
        match category {
            CdcSourceCategory::AppendOnly => Self {
                category,
                direct_query_support: DirectQuerySupport::Full,
                table_materialization: TableMaterializationRequirement::Optional,
                primary_key_required_for_table: false,
            },
            CdcSourceCategory::Upsert => Self {
                category,
                direct_query_support: DirectQuerySupport::StatelessOnly,
                table_materialization: TableMaterializationRequirement::RequiredForStatefulQueries,
                primary_key_required_for_table: true,
            },
            CdcSourceCategory::NativeDatabaseCdc => Self {
                category,
                direct_query_support: DirectQuerySupport::None,
                table_materialization: TableMaterializationRequirement::AlwaysRequired,
                primary_key_required_for_table: true,
            },
            CdcSourceCategory::Changelog => Self {
                category,
                direct_query_support: DirectQuerySupport::StatelessOnly,
                table_materialization: TableMaterializationRequirement::RequiredForStatefulQueries,
                primary_key_required_for_table: true,
            },
            CdcSourceCategory::ObjectBatch | CdcSourceCategory::ExternalTable => Self {
                category,
                direct_query_support: DirectQuerySupport::Full,
                table_materialization: TableMaterializationRequirement::Optional,
                primary_key_required_for_table: false,
            },
        }
    }

    pub fn category(&self) -> CdcSourceCategory {
        self.category
    }

    pub fn direct_query_support(&self) -> DirectQuerySupport {
        self.direct_query_support
    }

    pub fn table_materialization(&self) -> TableMaterializationRequirement {
        self.table_materialization
    }

    pub fn primary_key_required_for_table(&self) -> bool {
        self.primary_key_required_for_table
    }

    pub fn validate_direct_query(&self, stateful: bool) -> Result<()> {
        match (self.direct_query_support, stateful) {
            (DirectQuerySupport::Full, _) | (DirectQuerySupport::StatelessOnly, false) => Ok(()),
            (DirectQuerySupport::StatelessOnly, true) => bail!(
                "{:?} sources must be materialized as tables before stateful queries",
                self.category
            ),
            (DirectQuerySupport::None, _) => bail!(
                "{:?} sources must be materialized as tables before they can be queried",
                self.category
            ),
        }
    }

    pub fn validate_table_primary_key(&self, primary_key: Option<&CdcPrimaryKey>) -> Result<()> {
        if self.primary_key_required_for_table && primary_key.is_none() {
            bail!("{:?} tables require a primary key", self.category);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcSourceDefinition {
    source_id: CdcSourceId,
    connector: String,
    semantics: CdcSourceSemantics,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

impl CdcSourceDefinition {
    pub fn new(
        source_id: CdcSourceId,
        connector: impl Into<String>,
        semantics: CdcSourceSemantics,
    ) -> Result<Self> {
        let connector = connector.into();
        ensure!(
            !connector.trim().is_empty(),
            "CDC source connector cannot be empty"
        );
        Ok(Self {
            source_id,
            connector,
            semantics,
            properties: BTreeMap::new(),
        })
    }

    pub fn postgres(source_id: CdcSourceId) -> Result<Self> {
        Self::new(
            source_id,
            "postgres-cdc",
            CdcSourceSemantics::for_category(CdcSourceCategory::NativeDatabaseCdc),
        )
    }

    pub fn source_id(&self) -> &CdcSourceId {
        &self.source_id
    }

    pub fn connector(&self) -> &str {
        &self.connector
    }

    pub fn semantics(&self) -> CdcSourceSemantics {
        self.semantics
    }

    pub fn properties(&self) -> &BTreeMap<String, String> {
        &self.properties
    }

    pub fn property(&self, key: &str) -> Option<&str> {
        self.properties.get(key).map(String::as_str)
    }

    pub fn with_property(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self> {
        self.set_property(key, value)?;
        Ok(self)
    }

    pub fn set_property(&mut self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        let key = key.into();
        ensure!(
            !key.trim().is_empty(),
            "CDC source property key cannot be empty"
        );
        self.properties.insert(key, value.into());
        Ok(())
    }

    pub fn validate_table_definition(&self, table: &CdcTableDefinition) -> Result<()> {
        ensure!(
            table.source_id() == &self.source_id,
            "CDC table '{}' belongs to source '{}', not '{}'",
            table.table_id().as_str(),
            table.source_id().as_str(),
            self.source_id.as_str()
        );
        self.semantics
            .validate_table_primary_key(Some(table.schema().primary_key()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcTableDefinition {
    source_id: CdcSourceId,
    schema: CdcTableSchema,
}

impl CdcTableDefinition {
    pub fn new(source_id: CdcSourceId, schema: CdcTableSchema) -> Self {
        Self { source_id, schema }
    }

    pub fn source_id(&self) -> &CdcSourceId {
        &self.source_id
    }

    pub fn table_id(&self) -> &CdcTableId {
        self.schema.table_id()
    }

    pub fn schema(&self) -> &CdcTableSchema {
        &self.schema
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcPrimaryKey {
    columns: Vec<String>,
}

impl CdcPrimaryKey {
    pub fn new(columns: impl IntoIterator<Item = impl Into<String>>) -> Result<Self> {
        let columns: Vec<String> = columns.into_iter().map(Into::into).collect();
        ensure!(!columns.is_empty(), "CDC primary key cannot be empty");

        let mut seen = HashSet::new();
        for column in &columns {
            ensure!(
                !column.trim().is_empty(),
                "CDC primary key column cannot be empty"
            );
            if !seen.insert(column.as_str()) {
                bail!("duplicate CDC primary key column '{column}'");
            }
        }

        Ok(Self { columns })
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn contains_column(&self, column: &str) -> bool {
        self.columns.iter().any(|existing| existing == column)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcColumn {
    name: String,
    data_type: ColumnType,
    nullable: bool,
}

impl CdcColumn {
    pub fn new(name: impl Into<String>, data_type: ColumnType, nullable: bool) -> Result<Self> {
        let name = name.into();
        ensure!(!name.trim().is_empty(), "CDC column name cannot be empty");
        Ok(Self {
            name,
            data_type,
            nullable,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &ColumnType {
        &self.data_type
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcTableSchema {
    table_id: CdcTableId,
    upstream_table: UpstreamTableRef,
    columns: Vec<CdcColumn>,
    primary_key: CdcPrimaryKey,
}

impl CdcTableSchema {
    pub fn new(
        table_id: CdcTableId,
        upstream_table: UpstreamTableRef,
        columns: Vec<CdcColumn>,
        primary_key: CdcPrimaryKey,
    ) -> Result<Self> {
        ensure!(!columns.is_empty(), "CDC table schema must have columns");

        let mut seen = HashSet::new();
        for column in &columns {
            if !seen.insert(column.name()) {
                bail!("duplicate CDC column '{}'", column.name());
            }
        }

        for key_column in primary_key.columns() {
            let Some(column) = columns.iter().find(|column| column.name() == key_column) else {
                bail!("CDC primary key column '{key_column}' is not in table schema");
            };
            if column.nullable() {
                bail!("CDC primary key column '{key_column}' cannot be nullable");
            }
        }

        Ok(Self {
            table_id,
            upstream_table,
            columns,
            primary_key,
        })
    }

    pub fn table_id(&self) -> &CdcTableId {
        &self.table_id
    }

    pub fn upstream_table(&self) -> &UpstreamTableRef {
        &self.upstream_table
    }

    pub fn columns(&self) -> &[CdcColumn] {
        &self.columns
    }

    pub fn primary_key(&self) -> &CdcPrimaryKey {
        &self.primary_key
    }

    pub fn column_index(&self, column_name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|column| column.name() == column_name)
    }

    pub fn primary_key_indices(&self) -> Vec<usize> {
        self.primary_key
            .columns()
            .iter()
            .map(|column| {
                self.column_index(column)
                    .expect("CDC table schema validated primary-key columns")
            })
            .collect()
    }

    pub fn validate_row(&self, row: &CdcRow) -> Result<()> {
        ensure!(
            row.values().len() == self.columns.len(),
            "CDC row length {} does not match table '{}' column count {}",
            row.values().len(),
            self.table_id().as_str(),
            self.columns.len()
        );
        for (idx, (column, value)) in self.columns.iter().zip(row.values()).enumerate() {
            match value {
                Some(value) if column.data_type().matches_value(value) => {}
                Some(_) => bail!(
                    "CDC row value at column '{}' (index {idx}) does not match type {:?}",
                    column.name(),
                    column.data_type()
                ),
                None if column.nullable() => {}
                None => bail!("CDC row column '{}' cannot be NULL", column.name()),
            }
        }
        Ok(())
    }

    pub fn validate_columnar_rows(&self, rows: &CdcColumnarRowBatch) -> Result<()> {
        ensure!(
            rows.columns().len() == self.columns.len(),
            "CDC columnar row batch column count {} does not match table '{}' column count {}",
            rows.columns().len(),
            self.table_id().as_str(),
            self.columns.len()
        );
        for (idx, (column, values)) in self.columns.iter().zip(rows.columns()).enumerate() {
            ensure!(
                values.data_type() == column.data_type().clone(),
                "CDC columnar batch column '{}' (index {idx}) type {:?} does not match {:?}",
                column.name(),
                values.data_type(),
                column.data_type()
            );
            if !column.nullable() {
                ensure!(
                    !values.has_nulls(),
                    "CDC columnar batch column '{}' cannot contain NULL",
                    column.name()
                );
            }
        }
        for key_column in self.primary_key.columns() {
            let column_idx = self
                .column_index(key_column)
                .expect("CDC table schema validated primary-key columns");
            let values = &rows.columns()[column_idx];
            ensure!(
                !values.has_nulls(),
                "CDC columnar batch primary-key column '{key_column}' cannot contain NULL"
            );
        }
        Ok(())
    }

    pub fn primary_key_from_row(&self, row: &CdcRow) -> Result<CdcRowKey> {
        self.validate_row(row)?;
        let mut values = Vec::with_capacity(self.primary_key.columns().len());
        for idx in self.primary_key_indices() {
            let column = &self.columns[idx];
            let Some(value) = row.values()[idx].clone() else {
                bail!("CDC primary key column '{}' cannot be NULL", column.name());
            };
            values.push(value);
        }
        CdcRowKey::new(values)
    }

    pub fn primary_key_from_columnar_row(
        &self,
        rows: &CdcColumnarRowBatch,
        row_idx: usize,
    ) -> Result<CdcRowKey> {
        ensure!(
            row_idx < rows.row_count(),
            "CDC columnar row index {row_idx} out of bounds for {} rows",
            rows.row_count()
        );
        let mut values = Vec::with_capacity(self.primary_key.columns().len());
        for idx in self.primary_key_indices() {
            let column = &self.columns[idx];
            let column_values = rows
                .columns()
                .get(idx)
                .ok_or_else(|| anyhow::anyhow!("CDC column index {idx} out of bounds"))?;
            ensure!(
                column_values.data_type() == column.data_type().clone(),
                "CDC primary-key column '{}' type {:?} does not match {:?}",
                column.name(),
                column_values.data_type(),
                column.data_type()
            );
            let Some(value) = rows.value(idx, row_idx)? else {
                bail!("CDC primary key column '{}' cannot be NULL", column.name());
            };
            values.push(value);
        }
        CdcRowKey::new(values)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcRow {
    values: Vec<Option<RowValue>>,
}

impl CdcRow {
    pub fn new(values: impl IntoIterator<Item = Option<RowValue>>) -> Result<Self> {
        let values: Vec<Option<RowValue>> = values.into_iter().collect();
        ensure!(!values.is_empty(), "CDC row cannot be empty");
        Ok(Self { values })
    }

    pub fn values(&self) -> &[Option<RowValue>] {
        &self.values
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "values", rename_all = "snake_case")]
pub enum CdcColumnarColumn {
    Int64(Vec<Option<i64>>),
    Bool(Vec<Option<bool>>),
    Utf8(Vec<Option<String>>),
    TimestampMillis(Vec<Option<i64>>),
    DateDays(Vec<Option<i32>>),
    Decimal128 {
        precision: u8,
        scale: i8,
        values: Vec<Option<i128>>,
    },
    Numeric(Vec<Option<String>>),
}

impl CdcColumnarColumn {
    pub fn data_type(&self) -> ColumnType {
        match self {
            Self::Int64(_) => ColumnType::Int64,
            Self::Bool(_) => ColumnType::Bool,
            Self::Utf8(_) => ColumnType::Utf8,
            Self::TimestampMillis(_) => ColumnType::TimestampMillis,
            Self::DateDays(_) => ColumnType::DateDays,
            Self::Decimal128 {
                precision, scale, ..
            } => ColumnType::Decimal128 {
                precision: *precision,
                scale: *scale,
            },
            Self::Numeric(_) => ColumnType::Numeric,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Int64(values) => values.len(),
            Self::Bool(values) => values.len(),
            Self::Utf8(values) => values.len(),
            Self::TimestampMillis(values) => values.len(),
            Self::DateDays(values) => values.len(),
            Self::Decimal128 { values, .. } => values.len(),
            Self::Numeric(values) => values.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn has_nulls(&self) -> bool {
        match self {
            Self::Int64(values) => values.iter().any(Option::is_none),
            Self::Bool(values) => values.iter().any(Option::is_none),
            Self::Utf8(values) => values.iter().any(Option::is_none),
            Self::TimestampMillis(values) => values.iter().any(Option::is_none),
            Self::DateDays(values) => values.iter().any(Option::is_none),
            Self::Decimal128 { values, .. } => values.iter().any(Option::is_none),
            Self::Numeric(values) => values.iter().any(Option::is_none),
        }
    }

    pub fn value(&self, row_idx: usize) -> Result<Option<RowValue>> {
        match self {
            Self::Int64(values) => values
                .get(row_idx)
                .cloned()
                .map(|value| value.map(RowValue::Int64))
                .ok_or_else(|| anyhow::anyhow!("CDC columnar row index {row_idx} out of bounds")),
            Self::Bool(values) => values
                .get(row_idx)
                .cloned()
                .map(|value| value.map(RowValue::Bool))
                .ok_or_else(|| anyhow::anyhow!("CDC columnar row index {row_idx} out of bounds")),
            Self::Utf8(values) => values
                .get(row_idx)
                .cloned()
                .map(|value| value.map(RowValue::Utf8))
                .ok_or_else(|| anyhow::anyhow!("CDC columnar row index {row_idx} out of bounds")),
            Self::TimestampMillis(values) => values
                .get(row_idx)
                .cloned()
                .map(|value| value.map(RowValue::TimestampMillis))
                .ok_or_else(|| anyhow::anyhow!("CDC columnar row index {row_idx} out of bounds")),
            Self::DateDays(values) => values
                .get(row_idx)
                .cloned()
                .map(|value| value.map(RowValue::DateDays))
                .ok_or_else(|| anyhow::anyhow!("CDC columnar row index {row_idx} out of bounds")),
            Self::Decimal128 { values, .. } => values
                .get(row_idx)
                .cloned()
                .map(|value| value.map(RowValue::Decimal128))
                .ok_or_else(|| anyhow::anyhow!("CDC columnar row index {row_idx} out of bounds")),
            Self::Numeric(values) => values
                .get(row_idx)
                .cloned()
                .map(|value| value.map(RowValue::Numeric))
                .ok_or_else(|| anyhow::anyhow!("CDC columnar row index {row_idx} out of bounds")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcColumnarRowBatch {
    columns: Vec<CdcColumnarColumn>,
    row_count: usize,
}

impl CdcColumnarRowBatch {
    pub fn new(columns: Vec<CdcColumnarColumn>) -> Result<Self> {
        ensure!(
            !columns.is_empty(),
            "CDC columnar row batch cannot be empty"
        );
        let row_count = columns[0].len();
        ensure!(
            row_count > 0,
            "CDC columnar row batch must contain at least one row"
        );
        for column in &columns {
            ensure!(
                column.len() == row_count,
                "CDC columnar row batch has mismatched column lengths: expected {row_count}, got {}",
                column.len()
            );
        }
        Ok(Self { columns, row_count })
    }

    pub fn columns(&self) -> &[CdcColumnarColumn] {
        &self.columns
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn value(&self, column_idx: usize, row_idx: usize) -> Result<Option<RowValue>> {
        ensure!(
            row_idx < self.row_count,
            "CDC columnar row index {row_idx} out of bounds for {} rows",
            self.row_count
        );
        let column = self
            .columns
            .get(column_idx)
            .ok_or_else(|| anyhow::anyhow!("CDC column index {column_idx} out of bounds"))?;
        column.value(row_idx)
    }

    pub fn row(&self, row_idx: usize) -> Result<CdcRow> {
        ensure!(
            row_idx < self.row_count,
            "CDC columnar row index {row_idx} out of bounds for {} rows",
            self.row_count
        );
        self.columns
            .iter()
            .map(|column| column.value(row_idx))
            .collect::<Result<Vec<_>>>()
            .and_then(CdcRow::new)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcRowKey {
    values: Vec<RowValue>,
}

impl CdcRowKey {
    pub fn new(values: impl IntoIterator<Item = RowValue>) -> Result<Self> {
        let values: Vec<RowValue> = values.into_iter().collect();
        ensure!(!values.is_empty(), "CDC row key cannot be empty");
        Ok(Self { values })
    }

    pub fn values(&self) -> &[RowValue] {
        &self.values
    }

    pub fn validate_against_schema(&self, schema: &CdcTableSchema) -> Result<()> {
        let indices = schema.primary_key_indices();
        ensure!(
            self.values.len() == indices.len(),
            "CDC row key length {} does not match primary-key column count {}",
            self.values.len(),
            indices.len()
        );
        for (value, column_idx) in self.values.iter().zip(indices) {
            let column = &schema.columns()[column_idx];
            if !column.data_type().matches_value(value) {
                bail!(
                    "CDC row key value for column '{}' does not match type {:?}",
                    column.name(),
                    column.data_type()
                );
            }
        }
        Ok(())
    }
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
                    schema.validate_row(before)?;
                    schema.primary_key_from_row(before)?;
                }
                schema.validate_row(after)?;
                schema.primary_key_from_row(after)?;
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
                    schema.validate_row(before)?;
                    schema.primary_key_from_row(before)?;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    snapshot_insert_rows: Option<CdcColumnarRowBatch>,
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
            snapshot_insert_rows: Some(rows),
        })
    }

    pub fn table_id(&self) -> &CdcTableId {
        &self.table_id
    }

    pub fn changes(&self) -> &[CdcChange] {
        &self.changes
    }

    pub fn snapshot_insert_rows(&self) -> Option<&CdcColumnarRowBatch> {
        self.snapshot_insert_rows.as_ref()
    }

    pub fn change_count(&self) -> usize {
        self.snapshot_insert_rows
            .as_ref()
            .map(CdcColumnarRowBatch::row_count)
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionBatch {
    source_id: CdcSourceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transaction_id: Option<CdcTransactionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    start_position: Option<CdcSourcePosition>,
    commit_position: CdcSourcePosition,
    change_batches: Vec<ChangeBatch>,
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
        })
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcCheckpoint {
    source_id: CdcSourceId,
    position: CdcSourcePosition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transaction_id: Option<CdcTransactionId>,
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
        }
    }

    pub fn from_transaction(transaction: &TransactionBatch) -> Self {
        Self {
            source_id: transaction.source_id().clone(),
            position: transaction.commit_position().clone(),
            transaction_id: transaction.transaction_id().cloned(),
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
pub enum CdcSourcePosition {
    Postgres {
        commit_lsn: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_lsn: Option<String>,
    },
    Opaque {
        value: String,
    },
}

impl CdcSourcePosition {
    pub fn postgres(commit_lsn: impl Into<String>, event_lsn: Option<String>) -> Result<Self> {
        let commit_lsn = commit_lsn.into();
        ensure!(
            !commit_lsn.trim().is_empty(),
            "Postgres CDC commit LSN cannot be empty"
        );
        if let Some(event_lsn) = event_lsn.as_deref() {
            ensure!(
                !event_lsn.trim().is_empty(),
                "Postgres CDC event LSN cannot be empty"
            );
        }
        Ok(Self::Postgres {
            commit_lsn,
            event_lsn,
        })
    }

    pub fn opaque(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        ensure!(
            !value.trim().is_empty(),
            "CDC source position cannot be empty"
        );
        Ok(Self::Opaque { value })
    }

    pub fn covers(&self, other: &Self) -> Result<bool> {
        match (self, other) {
            (
                Self::Postgres {
                    commit_lsn,
                    event_lsn,
                },
                Self::Postgres {
                    commit_lsn: other_commit_lsn,
                    event_lsn: other_event_lsn,
                },
            ) => postgres_position_covers(
                commit_lsn,
                event_lsn.as_deref(),
                other_commit_lsn,
                other_event_lsn.as_deref(),
            ),
            (Self::Opaque { value }, Self::Opaque { value: other }) => Ok(value == other),
            _ => bail!(
                "cannot compare CDC source positions from different position kinds: {:?} and {:?}",
                self,
                other
            ),
        }
    }
}

fn postgres_position_covers(
    commit_lsn: &str,
    event_lsn: Option<&str>,
    other_commit_lsn: &str,
    other_event_lsn: Option<&str>,
) -> Result<bool> {
    let commit_lsn = parse_postgres_lsn(commit_lsn)?;
    let other_commit_lsn = parse_postgres_lsn(other_commit_lsn)?;
    if commit_lsn != other_commit_lsn {
        return Ok(commit_lsn > other_commit_lsn);
    }

    match (event_lsn, other_event_lsn) {
        (None, _) => Ok(true),
        (Some(_), None) => Ok(false),
        (Some(event_lsn), Some(other_event_lsn)) => {
            Ok(parse_postgres_lsn(event_lsn)? >= parse_postgres_lsn(other_event_lsn)?)
        }
    }
}

fn parse_postgres_lsn(value: &str) -> Result<u64> {
    let (high, low) = value
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("invalid Postgres LSN '{value}'"))?;
    let high = u64::from_str_radix(high, 16)
        .with_context(|| format!("parse high half of Postgres LSN '{value}'"))?;
    let low = u64::from_str_radix(low, 16)
        .with_context(|| format!("parse low half of Postgres LSN '{value}'"))?;
    Ok((high << 32) | low)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id_column(nullable: bool) -> CdcColumn {
        CdcColumn::new("id", ColumnType::Int64, nullable).expect("id column")
    }

    fn amount_column() -> CdcColumn {
        CdcColumn::new("amount", ColumnType::Int64, true).expect("amount column")
    }

    fn orders_schema() -> CdcTableSchema {
        CdcTableSchema::new(
            CdcTableId::new("orders").expect("table id"),
            UpstreamTableRef::new("public", "orders").expect("upstream"),
            vec![
                CdcColumn::new("tenant_id", ColumnType::Int64, false).expect("tenant column"),
                id_column(false),
                amount_column(),
                CdcColumn::new("status", ColumnType::Utf8, true).expect("status column"),
            ],
            CdcPrimaryKey::new(["tenant_id", "id"]).expect("primary key"),
        )
        .expect("orders schema")
    }

    fn orders_row(tenant_id: i64, id: i64, amount: Option<i64>, status: Option<&str>) -> CdcRow {
        CdcRow::new([
            Some(RowValue::Int64(tenant_id)),
            Some(RowValue::Int64(id)),
            amount.map(RowValue::Int64),
            status.map(|value| RowValue::Utf8(value.to_string())),
        ])
        .expect("orders row")
    }

    #[test]
    fn native_database_cdc_requires_table_materialization_and_primary_key() {
        let semantics = CdcSourceSemantics::for_category(CdcSourceCategory::NativeDatabaseCdc);
        assert_eq!(semantics.direct_query_support(), DirectQuerySupport::None);
        assert_eq!(
            semantics.table_materialization(),
            TableMaterializationRequirement::AlwaysRequired
        );
        assert!(semantics.primary_key_required_for_table());
        assert!(semantics.validate_direct_query(false).is_err());
        assert!(semantics.validate_table_primary_key(None).is_err());

        let key = CdcPrimaryKey::new(["id"]).expect("primary key");
        semantics
            .validate_table_primary_key(Some(&key))
            .expect("primary key should satisfy CDC table contract");
    }

    #[test]
    fn append_only_sources_allow_direct_queries_without_primary_keys() {
        let semantics = CdcSourceSemantics::for_category(CdcSourceCategory::AppendOnly);
        assert_eq!(semantics.direct_query_support(), DirectQuerySupport::Full);
        assert_eq!(
            semantics.table_materialization(),
            TableMaterializationRequirement::Optional
        );
        assert!(!semantics.primary_key_required_for_table());
        semantics
            .validate_direct_query(true)
            .expect("append-only sources can be directly queried");
        semantics
            .validate_table_primary_key(None)
            .expect("append-only sources do not require a table primary key");
    }

    #[test]
    fn upsert_sources_allow_only_stateless_direct_queries() {
        let semantics = CdcSourceSemantics::for_category(CdcSourceCategory::Upsert);
        assert_eq!(
            semantics.direct_query_support(),
            DirectQuerySupport::StatelessOnly
        );
        semantics
            .validate_direct_query(false)
            .expect("stateless direct query should be allowed");
        assert!(semantics.validate_direct_query(true).is_err());
        assert!(semantics.validate_table_primary_key(None).is_err());
    }

    #[test]
    fn source_definitions_record_connector_semantics_and_properties() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let source = CdcSourceDefinition::postgres(source_id.clone())
            .expect("source")
            .with_property("slot.name", "floe_slot")
            .expect("property")
            .with_property("publication.name", "floe_pub")
            .expect("property");

        assert_eq!(source.source_id(), &source_id);
        assert_eq!(source.connector(), "postgres-cdc");
        assert_eq!(
            source.semantics().category(),
            CdcSourceCategory::NativeDatabaseCdc
        );
        assert_eq!(source.property("slot.name"), Some("floe_slot"));
        assert!(
            CdcSourceDefinition::new(
                source_id,
                "",
                CdcSourceSemantics::for_category(CdcSourceCategory::NativeDatabaseCdc)
            )
            .is_err()
        );
    }

    #[test]
    fn source_definitions_validate_owned_table_definitions() {
        let source = CdcSourceDefinition::postgres(CdcSourceId::new("pg_main").expect("source id"))
            .expect("source");
        let table = CdcTableDefinition::new(source.source_id().clone(), orders_schema());
        source
            .validate_table_definition(&table)
            .expect("table should match source semantics");

        let other_source_table = CdcTableDefinition::new(
            CdcSourceId::new("pg_other").expect("source id"),
            table.schema().clone(),
        );
        assert!(
            source
                .validate_table_definition(&other_source_table)
                .is_err()
        );
    }

    #[test]
    fn primary_key_rejects_empty_and_duplicate_columns() {
        assert!(CdcPrimaryKey::new(Vec::<String>::new()).is_err());
        assert!(CdcPrimaryKey::new(["id", ""]).is_err());
        assert!(CdcPrimaryKey::new(["id", "id"]).is_err());

        let key = CdcPrimaryKey::new(["tenant_id", "id"]).expect("composite primary key");
        assert_eq!(key.columns(), &["tenant_id".to_string(), "id".to_string()]);
        assert!(key.contains_column("tenant_id"));
    }

    #[test]
    fn table_schema_validates_primary_key_columns() {
        let table_id = CdcTableId::new("orders").expect("table id");
        let upstream = UpstreamTableRef::new("public", "orders").expect("upstream");
        let key = CdcPrimaryKey::new(["id"]).expect("primary key");

        CdcTableSchema::new(
            table_id.clone(),
            upstream.clone(),
            vec![id_column(false), amount_column()],
            key.clone(),
        )
        .expect("valid schema");

        let missing_key = CdcTableSchema::new(
            table_id.clone(),
            upstream.clone(),
            vec![amount_column()],
            key.clone(),
        )
        .expect_err("missing primary key column should fail");
        assert!(missing_key.to_string().contains("is not in table schema"));

        let nullable_key = CdcTableSchema::new(
            table_id,
            upstream,
            vec![id_column(true), amount_column()],
            key,
        )
        .expect_err("nullable primary key column should fail");
        assert!(nullable_key.to_string().contains("cannot be nullable"));
    }

    #[test]
    fn table_schema_with_composite_primary_key_serializes() {
        let schema = orders_schema();
        let encoded = serde_json::to_vec(&schema).expect("serialize schema");
        let decoded: CdcTableSchema = serde_json::from_slice(&encoded).expect("decode schema");

        assert_eq!(decoded, schema);
        assert_eq!(
            decoded.primary_key().columns(),
            &["tenant_id".to_string(), "id".to_string()]
        );
    }

    #[test]
    fn source_positions_reject_empty_values() {
        assert!(CdcSourcePosition::postgres("", None).is_err());
        assert!(CdcSourcePosition::postgres("0/16B6C50", Some(String::new())).is_err());
        assert!(CdcSourcePosition::opaque("").is_err());

        let position = CdcSourcePosition::postgres("0/16B6C50", Some("0/16B6C20".to_string()))
            .expect("postgres position");
        assert_eq!(
            position,
            CdcSourcePosition::Postgres {
                commit_lsn: "0/16B6C50".to_string(),
                event_lsn: Some("0/16B6C20".to_string())
            }
        );
    }

    #[test]
    fn source_positions_compare_postgres_frontiers() {
        let commit_20 = CdcSourcePosition::postgres("0/20", None).expect("position");
        let commit_10 = CdcSourcePosition::postgres("0/10", None).expect("position");
        assert!(commit_20.covers(&commit_10).expect("compare"));
        assert!(commit_20.covers(&commit_20).expect("compare"));
        assert!(!commit_10.covers(&commit_20).expect("compare"));

        let event_21 =
            CdcSourcePosition::postgres("0/20", Some("0/21".to_string())).expect("event position");
        let event_22 =
            CdcSourcePosition::postgres("0/20", Some("0/22".to_string())).expect("event position");
        assert!(commit_20.covers(&event_22).expect("compare"));
        assert!(!event_22.covers(&commit_20).expect("compare"));
        assert!(event_22.covers(&event_21).expect("compare"));
        assert!(!event_21.covers(&event_22).expect("compare"));

        let opaque = CdcSourcePosition::opaque("same").expect("opaque");
        assert!(opaque.covers(&opaque).expect("compare"));
        assert!(
            opaque
                .covers(&CdcSourcePosition::opaque("other").expect("opaque"))
                .is_ok_and(|covers| !covers)
        );
        assert!(opaque.covers(&commit_20).is_err());
    }

    #[test]
    fn checkpoints_compare_source_and_position() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let checkpoint = CdcCheckpoint::new(
            source_id.clone(),
            CdcSourcePosition::postgres("0/20", None).expect("position"),
            None,
        );
        let older = CdcCheckpoint::new(
            source_id,
            CdcSourcePosition::postgres("0/10", None).expect("position"),
            None,
        );
        assert!(checkpoint.covers(&older).expect("checkpoint covers"));

        let different_source = CdcCheckpoint::new(
            CdcSourceId::new("pg_other").expect("source id"),
            CdcSourcePosition::postgres("0/10", None).expect("position"),
            None,
        );
        assert!(checkpoint.covers(&different_source).is_err());
    }

    #[test]
    fn rows_validate_against_schema_and_extract_composite_keys() {
        let schema = orders_schema();
        let row = orders_row(7, 42, Some(100), Some("open"));
        schema.validate_row(&row).expect("valid row");

        let key = schema.primary_key_from_row(&row).expect("primary key");
        assert_eq!(key.values(), &[RowValue::Int64(7), RowValue::Int64(42)]);
        key.validate_against_schema(&schema).expect("valid key");

        let wrong_width = CdcRow::new([Some(RowValue::Int64(7))]).expect("row");
        assert!(schema.validate_row(&wrong_width).is_err());

        let null_pk = CdcRow::new([
            Some(RowValue::Int64(7)),
            None,
            Some(RowValue::Int64(100)),
            None,
        ])
        .expect("row");
        assert!(schema.validate_row(&null_pk).is_err());

        let wrong_type = CdcRow::new([
            Some(RowValue::Int64(7)),
            Some(RowValue::Utf8("not-an-id".to_string())),
            Some(RowValue::Int64(100)),
            None,
        ])
        .expect("row");
        assert!(schema.validate_row(&wrong_type).is_err());
    }

    #[test]
    fn change_batches_validate_change_shapes() {
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let before = orders_row(7, 42, Some(100), Some("open"));
        let after = orders_row(7, 42, Some(150), Some("paid"));
        let key = schema.primary_key_from_row(&before).expect("primary key");

        let batch = ChangeBatch::new(
            table_id.clone(),
            vec![
                CdcChange::Insert {
                    row: before.clone(),
                },
                CdcChange::Update {
                    key: Some(key.clone()),
                    before: Some(before),
                    after,
                },
                CdcChange::Delete {
                    key: Some(key),
                    before: None,
                },
            ],
        )
        .expect("change batch");
        batch
            .validate_against_schema(&schema)
            .expect("valid change batch");

        let invalid_delete = ChangeBatch::new(
            table_id,
            vec![CdcChange::Delete {
                key: None,
                before: None,
            }],
        )
        .expect("invalid delete batch");
        assert!(invalid_delete.validate_against_schema(&schema).is_err());
    }

    #[test]
    fn transaction_batches_validate_table_schemas_and_checkpoint_frontier() {
        let schema = orders_schema();
        let batch = ChangeBatch::new(
            schema.table_id().clone(),
            vec![CdcChange::Insert {
                row: orders_row(7, 42, Some(100), Some("open")),
            }],
        )
        .expect("change batch");
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let txid = CdcTransactionId::new("tx-1").expect("txid");
        let commit_position =
            CdcSourcePosition::postgres("0/16B6C50", Some("0/16B6C20".to_string()))
                .expect("position");
        let transaction = TransactionBatch::new(
            source_id.clone(),
            Some(txid.clone()),
            None,
            commit_position.clone(),
            vec![batch],
        )
        .expect("transaction batch");

        let schemas = HashMap::from([(schema.table_id().clone(), schema)]);
        transaction
            .validate_against_schemas(&schemas)
            .expect("valid transaction");

        let checkpoint = CdcCheckpoint::from_transaction(&transaction);
        assert_eq!(checkpoint.source_id(), &source_id);
        assert_eq!(checkpoint.transaction_id(), Some(&txid));
        assert_eq!(checkpoint.position(), &commit_position);

        let missing_schemas = HashMap::new();
        assert!(
            transaction
                .validate_against_schemas(&missing_schemas)
                .is_err()
        );
    }

    #[test]
    fn empty_batches_and_transactions_are_rejected() {
        let table_id = CdcTableId::new("orders").expect("table id");
        assert!(ChangeBatch::new(table_id, Vec::new()).is_err());

        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let commit_position = CdcSourcePosition::opaque("frontier-1").expect("position");
        assert!(TransactionBatch::new(source_id, None, None, commit_position, Vec::new()).is_err());
        assert!(CdcTransactionId::new("").is_err());
    }
}
