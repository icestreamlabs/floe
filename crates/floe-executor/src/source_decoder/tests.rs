use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::record_batch::RecordBatch;
use floe_core::RowValue;
use floe_core::source::{SourceColumn, SourceDataType};
use serde_json::json;

use super::*;
use crate::encoding::{EncodedRowScalar, decode_all_encoded_row_scalars_into};

fn decode_test_row(encoded: &[u8]) -> Vec<Option<EncodedRowScalar>> {
    let mut decoded = Vec::new();
    decode_all_encoded_row_scalars_into(encoded, &mut decoded).expect("decode encoded row");
    decoded
}

fn mixed_definition() -> SourceDefinition {
    SourceDefinition::new(
        "mixed",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("label", SourceDataType::Utf8, true),
            SourceColumn::new_nullable("seen_at", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("enabled", SourceDataType::Bool, false),
        ],
    )
    .expect("definition")
}

#[test]
fn encodes_nexmark_bid_event() {
    let definition = SourceDefinition::new(
        "nexmark_bid",
        vec![
            SourceColumn::new("auction", SourceDataType::Int64),
            SourceColumn::new("bidder", SourceDataType::Int64),
            SourceColumn::new("price", SourceDataType::Int64),
            SourceColumn::new("channel", SourceDataType::Utf8),
            SourceColumn::new("url", SourceDataType::Utf8),
            SourceColumn::new("date_time", SourceDataType::TimestampMillis),
            SourceColumn::new("extra", SourceDataType::Utf8),
        ],
    )
    .expect("definition");
    let decoder = SourceRowDecoder::new(definition);
    let event = AppendIngestEvent::new(
        "nexmark_bid",
        json!({
            "auction": 100,
            "bidder": 42,
            "price": 99,
            "channel": "web",
            "url": "http://example.com",
            "date_time": 1_600_000_000_i64,
            "extra": ""
        }),
    );

    let (encoded, ts) = decoder.encode_row_key(&event).expect("encode");
    let row = decode_test_row(&encoded);
    assert_eq!(row.len(), 7);
    assert_eq!(row[0], Some(EncodedRowScalar::Int64(100)));
    assert_eq!(row[1], Some(EncodedRowScalar::Int64(42)));
    assert_eq!(row[2], Some(EncodedRowScalar::Int64(99)));
    assert_eq!(row[3], Some(EncodedRowScalar::Utf8("web".to_string())));
    assert_eq!(
        row[4],
        Some(EncodedRowScalar::Utf8("http://example.com".to_string()))
    );
    assert_eq!(
        row[5],
        Some(EncodedRowScalar::TimestampMillis(1_600_000_000))
    );
    assert_eq!(ts, Some(1_600_000_000_u64));
}

#[test]
fn encodes_boolean_column() {
    let definition = SourceDefinition::new(
        "flags",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("enabled", SourceDataType::Bool),
        ],
    )
    .expect("definition");
    let decoder = SourceRowDecoder::new(definition);
    let event = AppendIngestEvent::new(
        "flags",
        json!({
            "id": 1,
            "enabled": true
        }),
    );

    let (encoded, ts) = decoder.encode_row_key(&event).expect("encode");
    let row = decode_test_row(&encoded);
    assert_eq!(row.len(), 2);
    assert_eq!(row[0], Some(EncodedRowScalar::Int64(1)));
    assert_eq!(row[1], Some(EncodedRowScalar::Bool(true)));
    assert_eq!(ts, None);
}

#[test]
fn encodes_date_and_numeric_columns() {
    let definition = SourceDefinition::new(
        "lineitem",
        vec![
            SourceColumn::new_nullable("shipdate", SourceDataType::DateDays, false),
            SourceColumn::new_nullable("extendedprice", SourceDataType::Numeric, false),
            SourceColumn::new_nullable(
                "discount",
                SourceDataType::Decimal128 {
                    precision: 15,
                    scale: 2,
                },
                false,
            ),
        ],
    )
    .expect("definition");
    let decoder = SourceRowDecoder::new(definition.clone());
    let event = AppendIngestEvent::new(
        "lineitem",
        json!({
            "shipdate": 10471,
            "extendedprice": "12345.67",
            "discount": "123.45"
        }),
    );

    let (encoded, ts) = decoder.encode_row_key(&event).expect("encode json");
    assert_eq!(ts, None);
    assert_eq!(
        decode_test_row(&encoded),
        vec![
            Some(EncodedRowScalar::DateDays(10471)),
            Some(EncodedRowScalar::Utf8("12345.67".to_string())),
            Some(EncodedRowScalar::Decimal128(12_345)),
        ]
    );

    let batch = RecordBatch::try_new(
        definition.to_arrow_schema(),
        vec![
            Arc::new(Date32Array::from(vec![10471])),
            Arc::new(StringArray::from(vec!["12345.67"])),
            Arc::new(
                Decimal128Array::from(vec![Some(12_345)])
                    .with_precision_and_scale(15, 2)
                    .expect("decimal type"),
            ),
        ],
    )
    .expect("record batch");
    let encoded = decoder.encode_arrow_batch(&batch).expect("encode arrow");
    assert_eq!(
        decode_test_row(&encoded[0].0),
        vec![
            Some(EncodedRowScalar::DateDays(10471)),
            Some(EncodedRowScalar::Utf8("12345.67".to_string())),
            Some(EncodedRowScalar::Decimal128(12_345)),
        ]
    );
}

#[test]
fn encodes_arrow_batch_without_json_payloads() {
    let definition = mixed_definition();
    let decoder = SourceRowDecoder::new(definition.clone());
    let batch = RecordBatch::try_new(
        definition.to_arrow_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("one"), None])),
            Arc::new(TimestampMillisecondArray::from(vec![1000, 2000])),
            Arc::new(BooleanArray::from(vec![true, false])),
        ],
    )
    .expect("record batch");

    let encoded = decoder.encode_arrow_batch(&batch).expect("encode arrow");

    assert_eq!(encoded.len(), 2);
    assert_eq!(encoded[0].1, Some(1000));
    assert_eq!(encoded[1].1, Some(2000));
    assert_eq!(
        decode_test_row(&encoded[0].0),
        vec![
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Utf8("one".to_string())),
            Some(EncodedRowScalar::TimestampMillis(1000)),
            Some(EncodedRowScalar::Bool(true)),
        ]
    );
    assert_eq!(
        decode_test_row(&encoded[1].0),
        vec![
            Some(EncodedRowScalar::Int64(2)),
            None,
            Some(EncodedRowScalar::TimestampMillis(2000)),
            Some(EncodedRowScalar::Bool(false)),
        ]
    );
}

#[test]
fn rejects_missing_required_column() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("definition");
    let decoder = SourceRowDecoder::new(definition);
    let event = AppendIngestEvent::new("orders", json!({"id": 1}));
    let err = decoder
        .encode_row_key(&event)
        .expect_err("missing price should fail");
    assert!(err.to_string().contains("missing field in source payload"));
}

#[test]
fn rejects_wrong_column_type() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("definition");
    let decoder = SourceRowDecoder::new(definition);
    let event = AppendIngestEvent::new("orders", json!({"id": "oops"}));
    let err = decoder
        .encode_row_key(&event)
        .expect_err("type mismatch should fail");
    assert!(err.to_string().contains("expected integer value"));
}

#[test]
fn rejects_null_for_non_nullable_column() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("note", SourceDataType::Utf8, true),
        ],
    )
    .expect("definition");
    let decoder = SourceRowDecoder::new(definition);
    let event = AppendIngestEvent::new("orders", json!({"id": null, "note": null}));
    let err = decoder
        .encode_row_key(&event)
        .expect_err("null id should fail");
    assert!(
        err.to_string()
            .contains("null value violates non-nullable column")
    );
}

#[test]
fn direct_encoding_produces_expected_scalars_and_timestamp() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("note", SourceDataType::Utf8, true),
            SourceColumn::new_nullable("created_at", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("enabled", SourceDataType::Bool, false),
        ],
    )
    .expect("definition");
    let decoder = SourceRowDecoder::new(definition);
    let event = AppendIngestEvent::new(
        "orders",
        json!({
            "id": 42,
            "note": "hello",
            "created_at": 1_700_000_000_i64,
            "enabled": true
        }),
    );

    let (encoded, direct_ts) = decoder.encode_row_key(&event).expect("direct encode");
    let decoded = decode_test_row(&encoded);
    assert_eq!(decoded[0], Some(EncodedRowScalar::Int64(42)));
    assert_eq!(
        decoded[1],
        Some(EncodedRowScalar::Utf8("hello".to_string()))
    );
    assert_eq!(
        decoded[2],
        Some(EncodedRowScalar::TimestampMillis(1_700_000_000))
    );
    assert_eq!(decoded[3], Some(EncodedRowScalar::Bool(true)));
    assert_eq!(direct_ts, Some(1_700_000_000_u64));
}

#[test]
fn direct_encoding_can_omit_unneeded_columns() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("note", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("created_at", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("definition");
    let decoder = SourceRowDecoder::new_with_encoded_required_columns(
        definition,
        Some(Arc::from([true, false, true])),
    );
    let event = AppendIngestEvent::new(
        "orders",
        json!({
            "id": 42,
            "created_at": 1_700_000_000_i64
        }),
    );

    let (encoded, direct_ts) = decoder.encode_row_key(&event).expect("direct encode");
    let decoded = decode_test_row(&encoded);
    assert_eq!(decoded[0], Some(EncodedRowScalar::Int64(42)));
    assert_eq!(decoded[1], None);
    assert_eq!(
        decoded[2],
        Some(EncodedRowScalar::TimestampMillis(1_700_000_000))
    );
    assert_eq!(direct_ts, Some(1_700_000_000_u64));
}

#[test]
fn mask_arrow_batch_for_required_columns_omits_unneeded_columns() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("note", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("created_at", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("definition");
    let mut builder = SourceArrowBatchBuilder::new(definition.clone(), 1);
    let event = AppendIngestEvent::new(
        "orders",
        json!({
            "id": 42,
            "note": "ignored at execution",
            "created_at": 1_700_000_000_i64
        }),
    );

    let event_ts = builder.append_event(&event).expect("append event");
    let batch = builder
        .finish()
        .expect("finish batch")
        .expect("record batch");
    let required_columns = Arc::from([true, false, true]);
    let masked =
        mask_arrow_batch_for_required_columns(&definition, &batch, Some(&required_columns))
            .expect("mask batch");

    assert_eq!(event_ts, Some(1_700_000_000_u64));
    assert_eq!(batch.num_rows(), 1);
    let full_note = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("note array");
    assert_eq!(full_note.value(0), "ignored at execution");
    let masked_note = masked
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("masked note array");
    assert!(!masked_note.is_null(0));
    assert_eq!(masked_note.value(0), "");
}

#[test]
fn typed_row_encoding_matches_json_event_encoding() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("note", SourceDataType::Utf8, true),
            SourceColumn::new_nullable("created_at", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("enabled", SourceDataType::Bool, false),
        ],
    )
    .expect("definition");
    let decoder = SourceRowDecoder::new(definition);
    let event = AppendIngestEvent::new(
        "orders",
        json!({
            "id": 42,
            "note": null,
            "created_at": 1_700_000_000_i64,
            "enabled": true
        }),
    );
    let row_values = vec![
        Some(RowValue::Int64(42)),
        None,
        Some(RowValue::TimestampMillis(1_700_000_000)),
        Some(RowValue::Bool(true)),
    ];

    let json_encoded = decoder.encode_row_key(&event).expect("json encode");
    let typed_encoded = decoder
        .encode_row_values(&row_values)
        .expect("typed row encode");

    assert_eq!(typed_encoded, json_encoded);
    let decoded = decode_test_row(&typed_encoded.0);
    assert_eq!(decoded[0], Some(EncodedRowScalar::Int64(42)));
    assert_eq!(decoded[1], None);
    assert_eq!(
        decoded[2],
        Some(EncodedRowScalar::TimestampMillis(1_700_000_000))
    );
    assert_eq!(decoded[3], Some(EncodedRowScalar::Bool(true)));
}

#[test]
fn typed_row_encoding_rejects_wrong_shape_and_types() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("note", SourceDataType::Utf8, true),
        ],
    )
    .expect("definition");
    let decoder = SourceRowDecoder::new(definition);

    let err = decoder
        .encode_row_values(&[Some(RowValue::Int64(42))])
        .expect_err("row shape should fail");
    assert!(err.to_string().contains("value count"));

    let err = decoder
        .encode_row_values(&[Some(RowValue::Utf8("oops".to_string())), None])
        .expect_err("wrong type should fail");
    assert!(err.to_string().contains("does not match type"));

    let err = decoder
        .encode_row_values(&[None, None])
        .expect_err("null primary column should fail");
    assert!(
        err.to_string()
            .contains("null value violates non-nullable column 'id'")
    );
}
