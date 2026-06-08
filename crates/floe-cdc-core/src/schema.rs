use std::collections::HashSet;

use anyhow::{Result, bail, ensure};
use floe_core::catalog::ColumnType;
use serde::{Deserialize, Serialize};

use crate::ids::{CdcSourceId, CdcTableId, UpstreamTableRef};
use crate::row::{CdcColumnarRowBatch, CdcRow, CdcRowKey};

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

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

    pub fn stable_fingerprint(&self) -> u64 {
        let mut hash = FNV_OFFSET_BASIS;
        fnv_hash_str(&mut hash, self.table_id.as_str());
        fnv_hash_str(&mut hash, self.upstream_table.schema());
        fnv_hash_str(&mut hash, self.upstream_table.table());
        for column in &self.columns {
            fnv_hash_str(&mut hash, column.name());
            fnv_hash_column_type(&mut hash, column.data_type());
            fnv_hash_u8(&mut hash, u8::from(column.nullable()));
        }
        for key_column in self.primary_key.columns() {
            fnv_hash_str(&mut hash, key_column);
        }
        hash
    }

    pub fn column_index(&self, column_name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|column| column.name() == column_name)
    }

    pub fn primary_key_indices(&self) -> Result<Vec<usize>> {
        self.primary_key
            .columns()
            .iter()
            .map(|column| {
                self.column_index(column).ok_or_else(|| {
                    anyhow::anyhow!("CDC primary key column '{column}' is not in table schema")
                })
            })
            .collect()
    }

    pub fn validate_row(&self, row: &CdcRow) -> Result<()> {
        self.validate_row_with_toast_policy(row, false)
    }

    pub fn validate_row_allowing_unchanged_toast(&self, row: &CdcRow) -> Result<()> {
        self.validate_row_with_toast_policy(row, true)
    }

    fn validate_row_with_toast_policy(
        &self,
        row: &CdcRow,
        allow_unchanged_toast: bool,
    ) -> Result<()> {
        ensure!(
            row.values().len() == self.columns.len(),
            "CDC row length {} does not match table '{}' column count {}",
            row.values().len(),
            self.table_id().as_str(),
            self.columns.len()
        );
        for (idx, (column, value)) in self.columns.iter().zip(row.values()).enumerate() {
            if row.is_unchanged_toast(idx) {
                ensure!(
                    allow_unchanged_toast,
                    "CDC row column '{}' contains unresolved unchanged TOAST",
                    column.name()
                );
                ensure!(
                    !self.primary_key.contains_column(column.name()),
                    "CDC primary-key column '{}' cannot contain unresolved unchanged TOAST",
                    column.name()
                );
                continue;
            }
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
            let column_idx = self.column_index(key_column).ok_or_else(|| {
                anyhow::anyhow!("CDC primary key column '{key_column}' is not in table schema")
            })?;
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
        self.primary_key_from_validated_row(row)
    }

    pub fn primary_key_from_row_allowing_unchanged_toast(&self, row: &CdcRow) -> Result<CdcRowKey> {
        self.validate_row_allowing_unchanged_toast(row)?;
        self.primary_key_from_validated_row(row)
    }

    fn primary_key_from_validated_row(&self, row: &CdcRow) -> Result<CdcRowKey> {
        let mut values = Vec::with_capacity(self.primary_key.columns().len());
        for idx in self.primary_key_indices()? {
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
        for idx in self.primary_key_indices()? {
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

fn fnv_hash_u8(hash: &mut u64, value: u8) {
    *hash ^= u64::from(value);
    *hash = hash.wrapping_mul(FNV_PRIME);
}

fn fnv_hash_i8(hash: &mut u64, value: i8) {
    fnv_hash_u8(hash, value as u8);
}

fn fnv_hash_str(hash: &mut u64, value: &str) {
    for byte in value.as_bytes() {
        fnv_hash_u8(hash, *byte);
    }
    fnv_hash_u8(hash, 0xff);
}

fn fnv_hash_column_type(hash: &mut u64, data_type: &ColumnType) {
    match data_type {
        ColumnType::Int64 => fnv_hash_u8(hash, 1),
        ColumnType::Bool => fnv_hash_u8(hash, 2),
        ColumnType::Utf8 => fnv_hash_u8(hash, 3),
        ColumnType::TimestampMillis => fnv_hash_u8(hash, 4),
        ColumnType::DateDays => fnv_hash_u8(hash, 5),
        ColumnType::Decimal128 { precision, scale } => {
            fnv_hash_u8(hash, 6);
            fnv_hash_u8(hash, *precision);
            fnv_hash_i8(hash, *scale);
        }
        ColumnType::Numeric => fnv_hash_u8(hash, 7),
    }
}
