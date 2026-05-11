use anyhow::{Result, ensure};
use floe_cdc::CdcTableDeltas;
use floe_executor::SourceRowDecoder;
use floe_executor::stream_types::EncodedDelta;

pub fn encode_cdc_table_deltas(
    decoder: &SourceRowDecoder,
    table_deltas: &CdcTableDeltas,
) -> Result<Vec<EncodedDelta>> {
    ensure!(
        table_deltas.table_id().as_str() == decoder.definition().name(),
        "CDC table '{}' cannot be encoded with source decoder '{}'",
        table_deltas.table_id().as_str(),
        decoder.definition().name()
    );

    let mut encoded = Vec::with_capacity(table_deltas.deltas().len());
    for delta in table_deltas.deltas() {
        let (row, _) = decoder.encode_row_values(delta.row().values())?;
        encoded.push((row, delta.diff()));
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use floe_cdc::{CdcRowDelta, CdcTableDeltas};
    use floe_cdc_core::{CdcRow, CdcTableId};
    use floe_core::RowValue;
    use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};
    use floe_executor::encoding::{EncodedRowScalar, decode_all_encoded_row_scalars};

    use super::*;

    #[test]
    fn encodes_cdc_table_deltas_without_json_bridge() {
        let decoder = SourceRowDecoder::new(
            SourceDefinition::new(
                "orders",
                vec![
                    SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                    SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
                    SourceColumn::new_nullable("note", SourceDataType::Utf8, true),
                ],
            )
            .expect("source definition"),
        );
        let deltas = CdcTableDeltas::new(
            CdcTableId::new("orders").expect("table id"),
            vec![
                CdcRowDelta::insert(
                    CdcRow::new([
                        Some(RowValue::Int64(1)),
                        Some(RowValue::Int64(500)),
                        Some(RowValue::Utf8("new".to_string())),
                    ])
                    .expect("insert row"),
                ),
                CdcRowDelta::delete(
                    CdcRow::new([Some(RowValue::Int64(2)), Some(RowValue::Int64(100)), None])
                        .expect("delete row"),
                ),
            ],
        );

        let encoded = encode_cdc_table_deltas(&decoder, &deltas).expect("encode deltas");

        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[0].1, 1);
        assert_eq!(encoded[1].1, -1);
        assert_eq!(
            decode_all_encoded_row_scalars(&encoded[0].0).expect("decode insert"),
            vec![
                Some(EncodedRowScalar::Int64(1)),
                Some(EncodedRowScalar::Int64(500)),
                Some(EncodedRowScalar::Utf8("new".to_string())),
            ]
        );
        assert_eq!(
            decode_all_encoded_row_scalars(&encoded[1].0).expect("decode delete"),
            vec![
                Some(EncodedRowScalar::Int64(2)),
                Some(EncodedRowScalar::Int64(100)),
                None,
            ]
        );
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
        assert!(err.to_string().contains("cannot be encoded"));
    }
}
