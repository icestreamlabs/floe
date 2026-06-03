use super::count_eval::*;
use super::incremental_eval::*;
use super::*;
use crate::encoding::extract_encoded_row_scalars;
use datafusion::logical_expr::{col, lit};
use dbsp::circuit::schema::Field;

fn schema(fields: Vec<(&str, DbspScalarType)>) -> Arc<RowSchema> {
    RowSchema::try_new(
        fields
            .into_iter()
            .map(|(name, data_type)| Field::new(name, data_type, true))
            .collect(),
    )
    .expect("schema")
}

fn encode_row(columns: &[Option<EncodedRowScalar>]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(columns.len() as u32).to_le_bytes());
    for column in columns {
        match column {
            None => encoded.push(0x00),
            Some(EncodedRowScalar::Int64(value)) => {
                encoded.push(0x01);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            Some(EncodedRowScalar::Utf8(value)) => {
                encoded.push(0x02);
                let bytes = value.as_bytes();
                encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                encoded.extend_from_slice(bytes);
            }
            Some(EncodedRowScalar::TimestampMillis(value)) => {
                encoded.push(0x03);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            Some(EncodedRowScalar::Bool(value)) => {
                encoded.push(0x04);
                encoded.push(if *value { 1 } else { 0 });
            }
            Some(EncodedRowScalar::DateDays(value)) => {
                encoded.push(0x09);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            Some(EncodedRowScalar::Decimal128(value)) => {
                encoded.push(0x0B);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
        }
    }
    encoded
}

#[test]
fn encode_aggregate_values_supports_count_sum_avg_min_max() {
    let input_schema = schema(vec![
        ("price", DbspScalarType::Int64),
        ("label", DbspScalarType::Utf8),
        ("flag", DbspScalarType::Bool),
    ]);
    let aggregate = DbspAggregateNode::try_new(
        Arc::clone(&input_schema),
        vec![],
        vec![
            (
                DbspAggregateFunction::Count,
                None,
                None,
                false,
                Some("count_star".to_string()),
            ),
            (
                DbspAggregateFunction::Count,
                Some(col("price")),
                None,
                false,
                Some("count_price".to_string()),
            ),
            (
                DbspAggregateFunction::Count,
                Some(col("price")),
                Some(col("flag")),
                true,
                Some("count_distinct_price".to_string()),
            ),
            (
                DbspAggregateFunction::Sum,
                Some(col("price")),
                None,
                false,
                Some("sum_price".to_string()),
            ),
            (
                DbspAggregateFunction::Avg,
                Some(col("price")),
                None,
                false,
                Some("avg_price".to_string()),
            ),
            (
                DbspAggregateFunction::Min,
                Some(col("label")),
                None,
                false,
                Some("min_label".to_string()),
            ),
            (
                DbspAggregateFunction::Max,
                Some(col("label")),
                None,
                false,
                Some("max_label".to_string()),
            ),
        ],
    )
    .expect("aggregate");

    let layout = build_count_eval_layout(
        aggregate.aggregates(),
        input_schema.as_ref(),
        &HashMap::new(),
    );

    let values = vec![
        (
            encode_row(&[
                Some(EncodedRowScalar::Int64(10)),
                Some(EncodedRowScalar::Utf8("b".to_string())),
                Some(EncodedRowScalar::Bool(true)),
            ]),
            1,
        ),
        (
            encode_row(&[
                Some(EncodedRowScalar::Int64(30)),
                Some(EncodedRowScalar::Utf8("a".to_string())),
                Some(EncodedRowScalar::Bool(true)),
            ]),
            1,
        ),
        (
            encode_row(&[
                Some(EncodedRowScalar::Int64(10)),
                Some(EncodedRowScalar::Utf8("c".to_string())),
                Some(EncodedRowScalar::Bool(false)),
            ]),
            1,
        ),
        (
            encode_row(&[None, None, Some(EncodedRowScalar::Bool(true))]),
            1,
        ),
    ];

    let encoded = encode_aggregate_values_from_encoded(
        &layout,
        aggregate.aggregates(),
        input_schema.as_ref(),
        &values,
        "test",
        "aggregate",
    )
    .expect("encode aggregate values")
    .expect("non-empty aggregate output");

    assert_eq!(
        extract_encoded_row_scalars(&encoded, &[0, 1, 2, 3, 4, 5, 6]).expect("decode output"),
        vec![
            Some(EncodedRowScalar::Int64(4)),
            Some(EncodedRowScalar::Int64(3)),
            Some(EncodedRowScalar::Int64(2)),
            Some(EncodedRowScalar::Int64(50)),
            Some(EncodedRowScalar::Int64(16)),
            Some(EncodedRowScalar::Utf8("a".to_string())),
            Some(EncodedRowScalar::Utf8("c".to_string())),
        ]
    );
}

#[test]
fn encode_aggregate_values_supports_decimal_sum() {
    let input_schema = schema(vec![(
        "amount",
        DbspScalarType::Decimal128 {
            precision: 18,
            scale: 2,
        },
    )]);
    let aggregate = DbspAggregateNode::try_new(
        Arc::clone(&input_schema),
        vec![],
        vec![(
            DbspAggregateFunction::Sum,
            Some(col("amount")),
            None,
            false,
            Some("sum_amount".to_string()),
        )],
    )
    .expect("decimal aggregate");
    let layout = build_count_eval_layout(
        aggregate.aggregates(),
        input_schema.as_ref(),
        &HashMap::new(),
    );
    let values = vec![
        (encode_row(&[Some(EncodedRowScalar::Decimal128(1234))]), 1),
        (encode_row(&[Some(EncodedRowScalar::Decimal128(200))]), 2),
        (encode_row(&[None]), 1),
    ];

    let encoded = encode_aggregate_values_from_encoded(
        &layout,
        aggregate.aggregates(),
        input_schema.as_ref(),
        &values,
        "test",
        "aggregate",
    )
    .expect("encode decimal aggregate values")
    .expect("non-empty decimal aggregate output");
    assert_eq!(
        extract_encoded_row_scalars(&encoded, &[0]).expect("decode decimal sum"),
        vec![Some(EncodedRowScalar::Decimal128(1634))]
    );

    let mut encoded_decimal = Vec::new();
    append_encoded_sum_like_value(
        9999,
        &DbspScalarType::Decimal128 {
            precision: 4,
            scale: 2,
        },
        &mut encoded_decimal,
    )
    .expect("append decimal sum");
    assert_eq!(
        extract_encoded_row_scalars(
            &[1_u32.to_le_bytes().as_slice(), &encoded_decimal].concat(),
            &[0]
        )
        .expect("decode appended decimal sum"),
        vec![Some(EncodedRowScalar::Decimal128(9999))]
    );
    assert!(
        append_encoded_sum_like_value(
            10_000,
            &DbspScalarType::Decimal128 {
                precision: 4,
                scale: 2,
            },
            &mut Vec::new(),
        )
        .is_err()
    );
}

#[test]
fn unresolved_filter_and_expression_paths_fall_back_to_zero_updates() {
    let input_schema = schema(vec![("price", DbspScalarType::Int64)]);
    let aggregate = DbspAggregateNode::try_new(
        Arc::clone(&input_schema),
        vec![],
        vec![
            (
                DbspAggregateFunction::Count,
                None,
                Some(col("price").gt(lit(20_i64))),
                false,
                Some("filtered_count_star".to_string()),
            ),
            (
                DbspAggregateFunction::Count,
                Some(col("price") + lit(1_i64)),
                None,
                false,
                Some("count_unresolved_expr".to_string()),
            ),
            (
                DbspAggregateFunction::Count,
                Some(col("price") + lit(1_i64)),
                None,
                true,
                Some("count_distinct_unresolved_expr".to_string()),
            ),
        ],
    )
    .expect("aggregate");

    let layout = build_count_eval_layout(
        aggregate.aggregates(),
        input_schema.as_ref(),
        &HashMap::new(),
    );
    let row = encode_row(&[Some(EncodedRowScalar::Int64(30))]);

    let slot_updates = evaluate_count_batch_row_values(
        &layout,
        aggregate.aggregates(),
        input_schema.as_ref(),
        &[(row.clone(), 1)],
        "test",
        "aggregate",
    )
    .expect("batch slot updates")
    .remove(0);
    assert!(matches!(
        &slot_updates[0],
        dbsp::CountAggregateSlotUpdate::Linear(0)
    ));
    assert!(matches!(
        &slot_updates[1],
        dbsp::CountAggregateSlotUpdate::Linear(0)
    ));
    assert!(matches!(
        &slot_updates[2],
        dbsp::CountAggregateSlotUpdate::Distinct(None)
    ));

    let encoded = encode_aggregate_values_from_encoded(
        &layout,
        aggregate.aggregates(),
        input_schema.as_ref(),
        &[(row, 1)],
        "test",
        "aggregate",
    )
    .expect("encode unresolved output")
    .expect("resolved encoded output");
    assert_eq!(
        extract_encoded_row_scalars(&encoded, &[0, 1, 2]).expect("decode unresolved output"),
        vec![
            Some(EncodedRowScalar::Int64(0)),
            Some(EncodedRowScalar::Int64(0)),
            Some(EncodedRowScalar::Int64(0)),
        ]
    );
}

#[test]
fn decode_failures_and_empty_inputs_return_none_or_default_slots() {
    let input_schema = schema(vec![("price", DbspScalarType::Int64)]);
    let aggregate = DbspAggregateNode::try_new(
        Arc::clone(&input_schema),
        vec![],
        vec![
            (
                DbspAggregateFunction::Count,
                None,
                None,
                false,
                Some("count_star".to_string()),
            ),
            (
                DbspAggregateFunction::Count,
                Some(col("price")),
                None,
                true,
                Some("count_distinct_price".to_string()),
            ),
        ],
    )
    .expect("aggregate");

    let layout = build_count_eval_layout(
        aggregate.aggregates(),
        input_schema.as_ref(),
        &HashMap::new(),
    );

    let invalid_row = vec![0x01_u8];
    let slots = evaluate_count_batch_row_values(
        &layout,
        aggregate.aggregates(),
        input_schema.as_ref(),
        &[(invalid_row.clone(), 1)],
        "test",
        "aggregate",
    )
    .unwrap_or_default()
    .into_iter()
    .next()
    .unwrap_or_else(|| {
        aggregate
            .aggregates()
            .iter()
            .map(|agg| {
                if agg.distinct() {
                    dbsp::CountAggregateSlotUpdate::Distinct(None)
                } else {
                    dbsp::CountAggregateSlotUpdate::Linear(0)
                }
            })
            .collect()
    });
    assert!(matches!(
        slots[0],
        dbsp::CountAggregateSlotUpdate::Linear(0)
    ));
    assert!(matches!(
        slots[1],
        dbsp::CountAggregateSlotUpdate::Distinct(None)
    ));

    assert!(
        encode_aggregate_values_from_encoded(
            &layout,
            aggregate.aggregates(),
            input_schema.as_ref(),
            &[(invalid_row, 1)],
            "test",
            "aggregate",
        )
        .expect("encode invalid rows")
        .is_none()
    );

    assert!(
        encode_aggregate_values_from_encoded(
            &layout,
            aggregate.aggregates(),
            input_schema.as_ref(),
            &[],
            "test",
            "aggregate",
        )
        .expect("encode empty rows")
        .is_none()
    );

    assert!(
        encode_aggregate_values_from_encoded(
            &layout,
            &[],
            input_schema.as_ref(),
            &[(encode_row(&[Some(EncodedRowScalar::Int64(1))]), 1)],
            "test",
            "aggregate",
        )
        .expect("encode empty aggregates")
        .is_none()
    );
}
