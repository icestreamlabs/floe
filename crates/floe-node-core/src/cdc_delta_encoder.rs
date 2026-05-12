use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Int64Array, Int64Builder, RecordBatch,
    StringArray, StringBuilder, TimestampMillisecondArray, TimestampMillisecondBuilder,
};
use floe_cdc::CdcTableDeltas;
use floe_cdc_core::{CdcRowKey, CdcTableId, CdcTableSchema};
use floe_core::RowValue;
use floe_core::catalog::ColumnType;
use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};
use floe_executor::SourceRowDecoder;
use floe_executor::stream_types::EncodedDelta;

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

pub fn encode_cdc_table_deltas(
    decoder: &SourceRowDecoder,
    table_deltas: &CdcTableDeltas,
) -> Result<Vec<EncodedDelta>> {
    let arrow_batch = CdcArrowDeltaBatch::from_table_deltas(decoder.definition(), table_deltas)?;
    encode_cdc_arrow_delta_batch(decoder, &arrow_batch)
}

pub fn encode_cdc_arrow_delta_batch(
    decoder: &SourceRowDecoder,
    arrow_batch: &CdcArrowDeltaBatch,
) -> Result<Vec<EncodedDelta>> {
    ensure!(
        arrow_batch.table_id().as_str() == decoder.definition().name(),
        "CDC Arrow table '{}' cannot be encoded with source decoder '{}'",
        arrow_batch.table_id().as_str(),
        decoder.definition().name()
    );
    let encoded_rows = decoder.encode_arrow_batch(arrow_batch.record_batch())?;
    ensure!(
        encoded_rows.len() == arrow_batch.diffs().len(),
        "CDC Arrow batch row count {} does not match diff count {}",
        encoded_rows.len(),
        arrow_batch.diffs().len()
    );
    Ok(encoded_rows
        .into_iter()
        .zip(arrow_batch.diffs())
        .map(|((row, _), diff)| (row, *diff))
        .collect())
}

pub fn encode_cdc_arrow_primary_keys(
    schema: &CdcTableSchema,
    arrow_batch: &CdcArrowDeltaBatch,
) -> Result<Vec<CdcRowKey>> {
    ensure!(
        schema.table_id() == arrow_batch.table_id(),
        "CDC Arrow table '{}' cannot be key-encoded with schema '{}'",
        arrow_batch.table_id().as_str(),
        schema.table_id().as_str()
    );
    let key_indices = schema.primary_key_indices();
    let mut keys = Vec::with_capacity(arrow_batch.len());
    for row_idx in 0..arrow_batch.len() {
        let mut values = Vec::with_capacity(key_indices.len());
        for column_idx in &key_indices {
            let column = &schema.columns()[*column_idx];
            let value = arrow_row_value(
                arrow_batch.record_batch().column(*column_idx).as_ref(),
                row_idx,
                column.data_type(),
            )?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "CDC Arrow primary-key column '{}' cannot be NULL",
                    column.name()
                )
            })?;
            values.push(value);
        }
        keys.push(CdcRowKey::new(values)?);
    }
    Ok(keys)
}

enum CdcArrowColumnBuilder {
    Int64(Int64Builder),
    Bool(BooleanBuilder),
    Utf8(StringBuilder),
    TimestampMillis(TimestampMillisecondBuilder),
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
            (Self::Int64(builder), SourceDataType::Int64, None) => builder.append_null(),
            (Self::Bool(builder), SourceDataType::Bool, None) => builder.append_null(),
            (Self::Utf8(builder), SourceDataType::Utf8, None) => builder.append_null(),
            (Self::TimestampMillis(builder), SourceDataType::TimestampMillis, None) => {
                builder.append_null();
            }
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

fn arrow_row_value(
    array: &dyn Array,
    row_idx: usize,
    data_type: &ColumnType,
) -> Result<Option<RowValue>> {
    if array.is_null(row_idx) {
        return Ok(None);
    }
    match data_type {
        ColumnType::Int64 => {
            let array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("CDC Arrow column is not Int64")?;
            Ok(Some(RowValue::Int64(array.value(row_idx))))
        }
        ColumnType::Bool => {
            let array = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("CDC Arrow column is not Boolean")?;
            Ok(Some(RowValue::Bool(array.value(row_idx))))
        }
        ColumnType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .context("CDC Arrow column is not Utf8")?;
            Ok(Some(RowValue::Utf8(array.value(row_idx).to_string())))
        }
        ColumnType::TimestampMillis => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .context("CDC Arrow column is not TimestampMillis")?;
            Ok(Some(RowValue::TimestampMillis(array.value(row_idx))))
        }
    }
}

#[cfg(test)]
mod tests {
    use floe_cdc::{CdcRowDelta, CdcTableDeltas};
    use floe_cdc_core::{CdcColumn, CdcPrimaryKey, CdcRow, CdcTableId, UpstreamTableRef};
    use floe_core::RowValue;
    use floe_core::catalog::ColumnType;
    use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};
    use floe_executor::encoding::{EncodedRowScalar, decode_all_encoded_row_scalars};

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

    fn orders_schema() -> CdcTableSchema {
        CdcTableSchema::new(
            CdcTableId::new("orders").expect("table id"),
            UpstreamTableRef::new("public", "orders").expect("upstream"),
            vec![
                CdcColumn::new("tenant_id", ColumnType::Int64, false).expect("tenant"),
                CdcColumn::new("id", ColumnType::Int64, false).expect("id"),
                CdcColumn::new("amount", ColumnType::Int64, false).expect("amount"),
                CdcColumn::new("note", ColumnType::Utf8, true).expect("note"),
                CdcColumn::new("event_time", ColumnType::TimestampMillis, false)
                    .expect("event time"),
                CdcColumn::new("active", ColumnType::Bool, false).expect("active"),
            ],
            CdcPrimaryKey::new(["tenant_id", "id"]).expect("primary key"),
        )
        .expect("schema")
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
    fn encodes_cdc_table_deltas_through_arrow_batch() {
        let decoder = SourceRowDecoder::new(orders_definition());
        let encoded = encode_cdc_table_deltas(&decoder, &deltas()).expect("encode deltas");

        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[0].1, 1);
        assert_eq!(encoded[1].1, -1);
        assert_eq!(
            decode_all_encoded_row_scalars(&encoded[0].0).expect("decode insert"),
            vec![
                Some(EncodedRowScalar::Int64(7)),
                Some(EncodedRowScalar::Int64(1)),
                Some(EncodedRowScalar::Int64(500)),
                Some(EncodedRowScalar::Utf8("new".to_string())),
                Some(EncodedRowScalar::TimestampMillis(1000)),
                Some(EncodedRowScalar::Bool(true)),
            ]
        );
        assert_eq!(
            decode_all_encoded_row_scalars(&encoded[1].0).expect("decode delete"),
            vec![
                Some(EncodedRowScalar::Int64(7)),
                Some(EncodedRowScalar::Int64(2)),
                Some(EncodedRowScalar::Int64(100)),
                None,
                Some(EncodedRowScalar::TimestampMillis(2000)),
                Some(EncodedRowScalar::Bool(false)),
            ]
        );
    }

    #[test]
    fn encodes_composite_primary_keys_from_arrow_columns() {
        let arrow_batch = CdcArrowDeltaBatch::from_table_deltas(&orders_definition(), &deltas())
            .expect("arrow batch");

        let keys = encode_cdc_arrow_primary_keys(&orders_schema(), &arrow_batch)
            .expect("encode primary keys");

        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].values(), &[RowValue::Int64(7), RowValue::Int64(1)]);
        assert_eq!(keys[1].values(), &[RowValue::Int64(7), RowValue::Int64(2)]);
    }

    #[test]
    fn rejects_mismatched_cdc_table_and_decoder() {
        let decoder = SourceRowDecoder::new(
            SourceDefinition::new(
                "orders",
                vec![SourceColumn::new_nullable(
                    "id",
                    SourceDataType::Int64,
                    false,
                )],
            )
            .expect("source definition"),
        );
        let deltas = CdcTableDeltas::new(
            CdcTableId::new("customers").expect("table id"),
            vec![CdcRowDelta::insert(
                CdcRow::new([Some(RowValue::Int64(1))]).expect("row"),
            )],
        );

        let err = encode_cdc_table_deltas(&decoder, &deltas).expect_err("mismatch should fail");
        assert!(err.to_string().contains("cannot be converted"));
    }
}
