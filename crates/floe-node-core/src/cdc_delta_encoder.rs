use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Date32Builder, Decimal128Array,
    Decimal128Builder, Int64Array, Int64Builder, RecordBatch, StringBuilder,
    TimestampMillisecondArray, TimestampMillisecondBuilder,
};
use datafusion::arrow::datatypes::DataType;
use floe_cdc::CdcTableDeltas;
use floe_cdc_core::{CdcColumnarColumn, CdcColumnarRowBatch, CdcTableId};
use floe_core::RowValue;
use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcDeltaOperation {
    Insert,
    Delete,
}

#[derive(Debug, Clone)]
pub struct CdcArrowDeltaBatch {
    table_id: CdcTableId,
    record_batch: RecordBatch,
    operations: Vec<CdcDeltaOperation>,
    diffs: Vec<i64>,
}

impl CdcArrowDeltaBatch {
    pub fn from_table_deltas(
        definition: &SourceDefinition,
        table_deltas: &CdcTableDeltas,
    ) -> Result<Self> {
        ensure!(
            table_deltas.table_id().as_str() == definition.name(),
            "CDC table '{}' cannot be converted with source definition '{}'",
            table_deltas.table_id().as_str(),
            definition.name()
        );

        if let Some(rows) = table_deltas.snapshot_insert_rows() {
            return Self::from_snapshot_insert_rows(definition, table_deltas.table_id(), rows);
        }

        let row_count = table_deltas.deltas().len();
        let mut builders = definition
            .columns()
            .iter()
            .map(|column| CdcArrowColumnBuilder::new(column.data_type(), row_count))
            .collect::<Vec<_>>();
        let mut operations = Vec::with_capacity(row_count);
        let mut diffs = Vec::with_capacity(row_count);

        for delta in table_deltas.deltas() {
            let operation = operation_from_diff(delta.diff())?;
            let values = delta.row().values();
            ensure!(
                values.len() == definition.columns().len(),
                "CDC row value count {} does not match source '{}' column count {}",
                values.len(),
                definition.name(),
                definition.columns().len()
            );
            for ((builder, column), value) in builders
                .iter_mut()
                .zip(definition.columns())
                .zip(values.iter())
            {
                builder.append(column, value.as_ref())?;
            }
            operations.push(operation);
            diffs.push(delta.diff());
        }

        let arrays = builders
            .into_iter()
            .map(CdcArrowColumnBuilder::finish)
            .collect::<Result<Vec<_>>>()?;
        let record_batch = RecordBatch::try_new(definition.to_arrow_schema(), arrays)
            .context("build CDC Arrow delta batch")?;
        Ok(Self {
            table_id: table_deltas.table_id().clone(),
            record_batch,
            operations,
            diffs,
        })
    }

    fn from_snapshot_insert_rows(
        definition: &SourceDefinition,
        table_id: &CdcTableId,
        rows: &CdcColumnarRowBatch,
    ) -> Result<Self> {
        ensure!(
            rows.columns().len() == definition.columns().len(),
            "CDC snapshot column count {} does not match source '{}' column count {}",
            rows.columns().len(),
            definition.name(),
            definition.columns().len()
        );
        let arrays = rows
            .columns()
            .iter()
            .zip(definition.columns())
            .map(|(values, column)| columnar_values_to_arrow(values, column))
            .collect::<Result<Vec<_>>>()?;
        let record_batch = RecordBatch::try_new(definition.to_arrow_schema(), arrays)
            .context("build CDC snapshot Arrow delta batch")?;
        let row_count = rows.row_count();
        Ok(Self {
            table_id: table_id.clone(),
            record_batch,
            operations: vec![CdcDeltaOperation::Insert; row_count],
            diffs: vec![1; row_count],
        })
    }

    pub fn table_id(&self) -> &CdcTableId {
        &self.table_id
    }

    pub fn record_batch(&self) -> &RecordBatch {
        &self.record_batch
    }

    pub fn operations(&self) -> &[CdcDeltaOperation] {
        &self.operations
    }

    pub fn diffs(&self) -> &[i64] {
        &self.diffs
    }

    pub fn len(&self) -> usize {
        self.diffs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.diffs.is_empty()
    }
}

fn columnar_values_to_arrow(values: &CdcColumnarColumn, column: &SourceColumn) -> Result<ArrayRef> {
    ensure!(
        values.data_type() == column.data_type().column_type(),
        "CDC snapshot column '{}' type {:?} does not match source type {:?}",
        column.name(),
        values.data_type(),
        column.data_type()
    );
    if !column.nullable() {
        ensure!(
            !values.has_nulls(),
            "CDC snapshot column '{}' cannot contain NULL",
            column.name()
        );
    }

    let array: ArrayRef = match values {
        CdcColumnarColumn::Int64(values) => Arc::new(int64_array_from_options(values)),
        CdcColumnarColumn::Bool(values) => Arc::new(bool_array_from_options(values)),
        CdcColumnarColumn::Utf8(values) => {
            let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 16);
            for value in values {
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        CdcColumnarColumn::TimestampMillis(values) => {
            Arc::new(timestamp_millis_array_from_options(values))
        }
        CdcColumnarColumn::DateDays(values) => Arc::new(date32_array_from_options(values)),
        CdcColumnarColumn::Decimal128 {
            precision,
            scale,
            values,
        } => Arc::new(
            decimal128_array_from_options(values)
                .with_precision_and_scale(*precision, *scale)
                .context("build Decimal128 CDC snapshot Arrow column")?,
        ),
        CdcColumnarColumn::Numeric(values) => {
            let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 16);
            for value in values {
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
    };
    Ok(array)
}

fn int64_array_from_options(values: &[Option<i64>]) -> Int64Array {
    let mut builder = Int64Builder::with_capacity(values.len());
    for value in values {
        match value {
            Some(value) => builder.append_value(*value),
            None => builder.append_null(),
        }
    }
    builder.finish()
}

fn bool_array_from_options(values: &[Option<bool>]) -> BooleanArray {
    let mut builder = BooleanBuilder::with_capacity(values.len());
    for value in values {
        match value {
            Some(value) => builder.append_value(*value),
            None => builder.append_null(),
        }
    }
    builder.finish()
}

fn timestamp_millis_array_from_options(values: &[Option<i64>]) -> TimestampMillisecondArray {
    let mut builder = TimestampMillisecondBuilder::with_capacity(values.len());
    for value in values {
        match value {
            Some(value) => builder.append_value(*value),
            None => builder.append_null(),
        }
    }
    builder.finish()
}

fn date32_array_from_options(values: &[Option<i32>]) -> Date32Array {
    let mut builder = Date32Builder::with_capacity(values.len());
    for value in values {
        match value {
            Some(value) => builder.append_value(*value),
            None => builder.append_null(),
        }
    }
    builder.finish()
}

fn decimal128_array_from_options(values: &[Option<i128>]) -> Decimal128Array {
    let mut builder = Decimal128Builder::with_capacity(values.len());
    for value in values {
        match value {
            Some(value) => builder.append_value(*value),
            None => builder.append_null(),
        }
    }
    builder.finish()
}

enum CdcArrowColumnBuilder {
    Int64(Int64Builder),
    Bool(BooleanBuilder),
    Utf8(StringBuilder),
    TimestampMillis(TimestampMillisecondBuilder),
    DateDays(Date32Builder),
    Decimal128 { builder: Decimal128Builder },
    Numeric(StringBuilder),
}

impl CdcArrowColumnBuilder {
    fn new(data_type: &SourceDataType, row_capacity: usize) -> Self {
        match data_type {
            SourceDataType::Int64 => Self::Int64(Int64Builder::with_capacity(row_capacity)),
            SourceDataType::Bool => Self::Bool(BooleanBuilder::with_capacity(row_capacity)),
            SourceDataType::Utf8 => Self::Utf8(StringBuilder::with_capacity(
                row_capacity,
                row_capacity * 16,
            )),
            SourceDataType::TimestampMillis => {
                Self::TimestampMillis(TimestampMillisecondBuilder::with_capacity(row_capacity))
            }
            SourceDataType::DateDays => Self::DateDays(Date32Builder::with_capacity(row_capacity)),
            SourceDataType::Decimal128 { precision, scale } => Self::Decimal128 {
                builder: Decimal128Builder::with_capacity(row_capacity)
                    .with_data_type(DataType::Decimal128(*precision, *scale)),
            },
            SourceDataType::Numeric => Self::Numeric(StringBuilder::with_capacity(
                row_capacity,
                row_capacity * 16,
            )),
        }
    }

    fn append(&mut self, column: &SourceColumn, value: Option<&RowValue>) -> Result<()> {
        if value.is_none() {
            ensure!(
                column.nullable(),
                "null value violates non-nullable column '{}'",
                column.name()
            );
        }
        match (self, column.data_type(), value) {
            (Self::Int64(builder), SourceDataType::Int64, Some(RowValue::Int64(value))) => {
                builder.append_value(*value);
            }
            (Self::Bool(builder), SourceDataType::Bool, Some(RowValue::Bool(value))) => {
                builder.append_value(*value);
            }
            (Self::Utf8(builder), SourceDataType::Utf8, Some(RowValue::Utf8(value))) => {
                builder.append_value(value);
            }
            (
                Self::TimestampMillis(builder),
                SourceDataType::TimestampMillis,
                Some(RowValue::TimestampMillis(value)),
            ) => {
                builder.append_value(*value);
            }
            (
                Self::DateDays(builder),
                SourceDataType::DateDays,
                Some(RowValue::DateDays(value)),
            ) => {
                builder.append_value(*value);
            }
            (
                Self::Decimal128 { builder, .. },
                SourceDataType::Decimal128 { .. },
                Some(RowValue::Decimal128(value)),
            ) => {
                builder.append_value(*value);
            }
            (Self::Numeric(builder), SourceDataType::Numeric, Some(RowValue::Numeric(value))) => {
                builder.append_value(value);
            }
            (Self::Int64(builder), SourceDataType::Int64, None) => builder.append_null(),
            (Self::Bool(builder), SourceDataType::Bool, None) => builder.append_null(),
            (Self::Utf8(builder), SourceDataType::Utf8, None) => builder.append_null(),
            (Self::TimestampMillis(builder), SourceDataType::TimestampMillis, None) => {
                builder.append_null();
            }
            (Self::DateDays(builder), SourceDataType::DateDays, None) => builder.append_null(),
            (Self::Decimal128 { builder, .. }, SourceDataType::Decimal128 { .. }, None) => {
                builder.append_null();
            }
            (Self::Numeric(builder), SourceDataType::Numeric, None) => builder.append_null(),
            (_, _, Some(value)) => {
                bail!(
                    "source row value for column '{}' does not match type {:?}: {:?}",
                    column.name(),
                    column.data_type(),
                    value
                );
            }
            _ => bail!(
                "CDC Arrow builder for column '{}' does not match source type {:?}",
                column.name(),
                column.data_type()
            ),
        }
        Ok(())
    }

    fn finish(mut self) -> Result<ArrayRef> {
        let array: ArrayRef = match &mut self {
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::TimestampMillis(builder) => Arc::new(builder.finish()),
            Self::DateDays(builder) => Arc::new(builder.finish()),
            Self::Decimal128 { builder, .. } => Arc::new(builder.finish()),
            Self::Numeric(builder) => Arc::new(builder.finish()),
        };
        Ok(array)
    }
}

fn operation_from_diff(diff: i64) -> Result<CdcDeltaOperation> {
    match diff {
        1 => Ok(CdcDeltaOperation::Insert),
        -1 => Ok(CdcDeltaOperation::Delete),
        _ => bail!("CDC Arrow delta diff must be +1 or -1, got {diff}"),
    }
}

#[cfg(test)]
mod tests {
    use floe_cdc::{CdcRowDelta, CdcTableDeltas};
    use floe_cdc_core::{CdcColumnarColumn, CdcColumnarRowBatch, CdcRow, CdcTableId};
    use floe_core::RowValue;
    use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};

    use super::*;

    fn orders_definition() -> SourceDefinition {
        SourceDefinition::new(
            "orders",
            vec![
                SourceColumn::new_nullable("tenant_id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
                SourceColumn::new_nullable("note", SourceDataType::Utf8, true),
                SourceColumn::new_nullable("event_time", SourceDataType::TimestampMillis, false),
                SourceColumn::new_nullable("active", SourceDataType::Bool, false),
            ],
        )
        .expect("source definition")
    }

    fn row(
        tenant_id: i64,
        id: i64,
        amount: i64,
        note: Option<&str>,
        event_time: i64,
        active: bool,
    ) -> CdcRow {
        CdcRow::new([
            Some(RowValue::Int64(tenant_id)),
            Some(RowValue::Int64(id)),
            Some(RowValue::Int64(amount)),
            note.map(|value| RowValue::Utf8(value.to_string())),
            Some(RowValue::TimestampMillis(event_time)),
            Some(RowValue::Bool(active)),
        ])
        .expect("row")
    }

    fn deltas() -> CdcTableDeltas {
        CdcTableDeltas::new(
            CdcTableId::new("orders").expect("table id"),
            vec![
                CdcRowDelta::insert(row(7, 1, 500, Some("new"), 1000, true)),
                CdcRowDelta::delete(row(7, 2, 100, None, 2000, false)),
            ],
        )
    }

    #[test]
    fn builds_arrow_delta_batch_with_operation_metadata() {
        let batch = CdcArrowDeltaBatch::from_table_deltas(&orders_definition(), &deltas())
            .expect("arrow batch");

        assert_eq!(batch.len(), 2);
        assert_eq!(
            batch.operations(),
            &[CdcDeltaOperation::Insert, CdcDeltaOperation::Delete]
        );
        assert_eq!(batch.diffs(), &[1, -1]);
        assert_eq!(batch.record_batch().num_columns(), 6);
        assert_eq!(batch.record_batch().num_rows(), 2);
    }

    #[test]
    fn builds_columnar_snapshot_arrow_batch() {
        let rows = CdcColumnarRowBatch::new(vec![
            CdcColumnarColumn::Int64(vec![Some(7), Some(7)]),
            CdcColumnarColumn::Int64(vec![Some(1), Some(2)]),
            CdcColumnarColumn::Int64(vec![Some(500), Some(100)]),
            CdcColumnarColumn::Utf8(vec![Some("new".to_string()), None]),
            CdcColumnarColumn::TimestampMillis(vec![Some(1000), Some(2000)]),
            CdcColumnarColumn::Bool(vec![Some(true), Some(false)]),
        ])
        .expect("columnar rows");
        let deltas =
            CdcTableDeltas::snapshot_insert(CdcTableId::new("orders").expect("table id"), rows);
        let batch =
            CdcArrowDeltaBatch::from_table_deltas(&orders_definition(), &deltas).expect("batch");

        assert_eq!(batch.len(), 2);
        assert_eq!(batch.operations(), &[CdcDeltaOperation::Insert; 2]);
        assert_eq!(batch.diffs(), &[1, 1]);
        assert_eq!(batch.record_batch().num_columns(), 6);
        assert_eq!(batch.record_batch().num_rows(), 2);
    }

    #[test]
    fn rejects_mismatched_cdc_table_and_definition() {
        let deltas = CdcTableDeltas::new(
            CdcTableId::new("customers").expect("table id"),
            vec![CdcRowDelta::insert(
                CdcRow::new([Some(RowValue::Int64(1))]).expect("row"),
            )],
        );

        let err = CdcArrowDeltaBatch::from_table_deltas(&orders_definition(), &deltas)
            .expect_err("mismatch should fail");
        assert!(err.to_string().contains("cannot be converted"));
    }
}
