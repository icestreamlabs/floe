use anyhow::{Result, bail, ensure};
use floe_core::RowValue;
use floe_core::catalog::ColumnType;
use serde::{Deserialize, Serialize};

use crate::schema::CdcTableSchema;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcRow {
    values: Vec<Option<RowValue>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    unchanged_toast_indices: Vec<usize>,
}

impl CdcRow {
    pub fn new(values: impl IntoIterator<Item = Option<RowValue>>) -> Result<Self> {
        let values: Vec<Option<RowValue>> = values.into_iter().collect();
        ensure!(!values.is_empty(), "CDC row cannot be empty");
        Ok(Self {
            values,
            unchanged_toast_indices: Vec::new(),
        })
    }

    pub fn with_unchanged_toast_indices(
        values: impl IntoIterator<Item = Option<RowValue>>,
        indices: impl IntoIterator<Item = usize>,
    ) -> Result<Self> {
        let values: Vec<Option<RowValue>> = values.into_iter().collect();
        ensure!(!values.is_empty(), "CDC row cannot be empty");
        let mut indices: Vec<usize> = indices.into_iter().collect();
        indices.sort_unstable();
        indices.dedup();
        for idx in &indices {
            ensure!(
                *idx < values.len(),
                "unchanged TOAST column index {idx} out of bounds for CDC row with {} columns",
                values.len()
            );
        }
        Ok(Self {
            values,
            unchanged_toast_indices: indices,
        })
    }

    pub fn values(&self) -> &[Option<RowValue>] {
        &self.values
    }

    pub fn has_unchanged_toast(&self) -> bool {
        !self.unchanged_toast_indices.is_empty()
    }

    pub fn unchanged_toast_indices(&self) -> &[usize] {
        &self.unchanged_toast_indices
    }

    pub fn is_unchanged_toast(&self, column_idx: usize) -> bool {
        self.unchanged_toast_indices
            .binary_search(&column_idx)
            .is_ok()
    }

    pub fn resolve_unchanged_toast(&self, previous: &CdcRow) -> Result<Self> {
        if !self.has_unchanged_toast() {
            return Ok(self.clone());
        }
        ensure!(
            self.values.len() == previous.values.len(),
            "cannot resolve unchanged TOAST row with {} columns from previous row with {} columns",
            self.values.len(),
            previous.values.len()
        );
        let mut values = self.values.clone();
        for &idx in &self.unchanged_toast_indices {
            ensure!(
                !previous.is_unchanged_toast(idx),
                "previous CDC row also has unresolved unchanged TOAST at column index {idx}"
            );
            values[idx] = previous.values[idx].clone();
        }
        CdcRow::new(values)
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
