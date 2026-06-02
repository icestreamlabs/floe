use super::count_eval::*;
use super::incremental_eval::*;
use super::*;
use crate::encoding::extract_encoded_row_scalars;

mod tests {
    use super::*;
    use datafusion::logical_expr::col;
    use dbsp::circuit::schema::Field;

    fn schema(fields: Vec<(&str, DbspScalarType)>) -> Arc<RowSchema> {
        let fields = fields
            .into_iter()
            .map(|(name, ty)| Field::new(name, ty, true))
            .collect();
        RowSchema::try_new(fields).expect("schema")
    }

    fn encode_test_row(columns: &[Option<EncodedRowScalar>]) -> Vec<u8> {
        let mut encoded = Vec::new();
        let count = u32::try_from(columns.len()).expect("column count fits u32");
        encoded.extend_from_slice(&count.to_le_bytes());
        for value in columns {
            match value {
                None => encoded.push(0x00),
                Some(EncodedRowScalar::Int64(value)) => {
                    encoded.push(0x01);
                    encoded.extend_from_slice(&value.to_le_bytes());
                }
                Some(EncodedRowScalar::Utf8(value)) => {
                    encoded.push(0x02);
                    let bytes = value.as_bytes();
                    let len = u32::try_from(bytes.len()).expect("utf8 length fits u32");
                    encoded.extend_from_slice(&len.to_le_bytes());
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
    fn count_slot_kinds_and_count_star_detection() {
        let input_schema = schema(vec![
            ("price", DbspScalarType::Int64),
            ("bidder", DbspScalarType::Int64),
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
                    Some(col("bidder")),
                    None,
                    true,
                    Some("count_distinct_bidder".to_string()),
                ),
            ],
        )
        .expect("aggregate node");

        let slot_kinds = build_count_aggregate_slot_kinds(aggregate.aggregates());
        assert!(matches!(
            slot_kinds[0],
            dbsp::CountAggregateSlotKind::Linear
        ));
        assert!(matches!(
            slot_kinds[1],
            dbsp::CountAggregateSlotKind::Distinct
        ));

        assert!(is_simple_count_star_aggregate(&[
            aggregate.aggregates()[0].clone()
        ]));
        assert!(!is_simple_count_star_aggregate(aggregate.aggregates()));
        assert!(is_unconditional_count_aggregate(&aggregate.aggregates()[0]));
        assert!(!is_unconditional_count_aggregate(
            &aggregate.aggregates()[1]
        ));
    }

    #[test]
    fn count_and_incremental_evaluators_decode_rows() {
        let input_schema = schema(vec![
            ("price", DbspScalarType::Int64),
            ("bidder", DbspScalarType::Int64),
            ("label", DbspScalarType::Utf8),
        ]);
        let aggregate = DbspAggregateNode::try_new(
            Arc::clone(&input_schema),
            vec![(col("bidder"), None)],
            vec![
                (
                    DbspAggregateFunction::Count,
                    None,
                    None,
                    false,
                    Some("total".to_string()),
                ),
                (
                    DbspAggregateFunction::Count,
                    Some(col("bidder")),
                    None,
                    false,
                    Some("nonnull_bidder".to_string()),
                ),
                (
                    DbspAggregateFunction::Count,
                    Some(col("bidder")),
                    None,
                    true,
                    Some("cheap_distinct_bidder".to_string()),
                ),
                (
                    DbspAggregateFunction::Sum,
                    Some(col("price")),
                    None,
                    false,
                    Some("sum_price".to_string()),
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
        .expect("aggregate node");

        let expression_columns = Arc::new(HashMap::new());
        let count_eval = build_count_batch_row_evaluator(
            Arc::clone(&input_schema),
            aggregate.group_keys().to_vec(),
            aggregate.aggregates().to_vec(),
            Arc::clone(&expression_columns),
            "test".to_string(),
            "aggregate",
        );
        let incr_eval = build_incremental_aggregate_batch_row_evaluator(
            Arc::clone(&input_schema),
            aggregate.group_keys().to_vec(),
            aggregate.aggregates().to_vec(),
            expression_columns,
            "test".to_string(),
            "aggregate",
        );

        let row = encode_test_row(&[
            Some(EncodedRowScalar::Int64(50)),
            Some(EncodedRowScalar::Int64(42)),
            Some(EncodedRowScalar::Utf8("alpha".to_string())),
        ]);
        let count_rows = count_eval(&[(row.clone(), 1)]);
        let count_row = &count_rows.first().expect("count row").0;
        assert_eq!(
            extract_encoded_row_scalars(&count_row.key, &[0]).expect("decode key"),
            vec![Some(EncodedRowScalar::Int64(42))]
        );
        assert!(matches!(
            &count_row.slots[0],
            dbsp::CountAggregateSlotUpdate::Linear(1)
        ));
        assert!(matches!(
            &count_row.slots[1],
            dbsp::CountAggregateSlotUpdate::Linear(1)
        ));
        match &count_row.slots[2] {
            dbsp::CountAggregateSlotUpdate::Distinct(Some(encoded)) => {
                assert_eq!(
                    extract_encoded_row_scalars(encoded, &[0]).expect("decode distinct"),
                    vec![Some(EncodedRowScalar::Int64(42))]
                );
            }
            other => panic!("expected distinct encoded value, found {other:?}"),
        }

        let incr_rows = incr_eval(&[(row.clone(), 1)]);
        let incr_row = &incr_rows.first().expect("incremental row").1;
        assert!(matches!(
            &incr_row.slots[0],
            dbsp::IncrementalAggregateSlotUpdate::Count(1)
        ));
        assert!(matches!(
            &incr_row.slots[3],
            dbsp::IncrementalAggregateSlotUpdate::Value(Some(dbsp::AggregateValue::Int64(50)))
        ));
        assert!(matches!(
            &incr_row.slots[4],
            dbsp::IncrementalAggregateSlotUpdate::Value(Some(dbsp::AggregateValue::Utf8(value)))
                if value == "alpha"
        ));

        let filtered_row = encode_test_row(&[
            Some(EncodedRowScalar::Int64(200)),
            Some(EncodedRowScalar::Int64(7)),
            Some(EncodedRowScalar::Utf8("beta".to_string())),
        ]);
        let filtered_rows = count_eval(&[(filtered_row, 1)]);
        let count_row = &filtered_rows.first().expect("count row").0;
        match &count_row.slots[2] {
            dbsp::CountAggregateSlotUpdate::Distinct(Some(encoded)) => {
                assert_eq!(
                    extract_encoded_row_scalars(encoded, &[0]).expect("decode distinct"),
                    vec![Some(EncodedRowScalar::Int64(7))]
                );
            }
            other => panic!("expected distinct encoded value, found {other:?}"),
        }
    }

    #[test]
    fn incremental_slot_kinds_and_encoding_helpers_work() {
        let input_schema = schema(vec![
            ("price", DbspScalarType::Int64),
            ("label", DbspScalarType::Utf8),
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
                    Some("count".to_string()),
                ),
                (
                    DbspAggregateFunction::Count,
                    Some(col("label")),
                    None,
                    true,
                    Some("distinct_label".to_string()),
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
                    DbspAggregateFunction::Max,
                    Some(col("label")),
                    None,
                    false,
                    Some("max_label".to_string()),
                ),
            ],
        )
        .expect("aggregate node");

        let slot_kinds = build_incremental_aggregate_slot_kinds(aggregate.aggregates())
            .expect("incremental slot kinds");
        assert!(matches!(
            slot_kinds[0],
            dbsp::IncrementalAggregateSlotKind::Count
        ));
        assert!(matches!(
            slot_kinds[1],
            dbsp::IncrementalAggregateSlotKind::CountDistinct
        ));
        assert!(matches!(
            slot_kinds[2],
            dbsp::IncrementalAggregateSlotKind::Sum(dbsp::AggregateValueType::Int64)
        ));
        assert!(matches!(
            slot_kinds[3],
            dbsp::IncrementalAggregateSlotKind::Avg
        ));
        assert!(matches!(
            slot_kinds[4],
            dbsp::IncrementalAggregateSlotKind::Max(dbsp::AggregateValueType::Utf8)
        ));

        assert_eq!(
            aggregate_ordered_value_type_from_dbsp_type(&DbspScalarType::Bool),
            None
        );

        let typed_input_schema = schema(vec![
            ("shipdate", DbspScalarType::DateDays),
            (
                "amount",
                DbspScalarType::Decimal128 {
                    precision: 18,
                    scale: 2,
                },
            ),
        ]);
        let typed_aggregate = DbspAggregateNode::try_new(
            Arc::clone(&typed_input_schema),
            vec![],
            vec![
                (
                    DbspAggregateFunction::Min,
                    Some(col("shipdate")),
                    None,
                    false,
                    Some("min_shipdate".to_string()),
                ),
                (
                    DbspAggregateFunction::Max,
                    Some(col("amount")),
                    None,
                    false,
                    Some("max_amount".to_string()),
                ),
            ],
        )
        .expect("typed aggregate node");
        let typed_slot_kinds = build_incremental_aggregate_slot_kinds(typed_aggregate.aggregates())
            .expect("typed incremental slot kinds");
        assert!(matches!(
            typed_slot_kinds[0],
            dbsp::IncrementalAggregateSlotKind::Min(dbsp::AggregateValueType::DateDays)
        ));
        assert!(matches!(
            typed_slot_kinds[1],
            dbsp::IncrementalAggregateSlotKind::Max(dbsp::AggregateValueType::Decimal128 {
                precision: 18,
                scale: 2,
            })
        ));
        assert_eq!(
            aggregate_numeric_value_type_from_dbsp_type(&DbspScalarType::Decimal128 {
                precision: 18,
                scale: 2,
            }),
            Some(dbsp::AggregateValueType::Decimal128 {
                precision: 18,
                scale: 2,
            })
        );

        let encoded_bounds = encode_window_bounds(10, 20).expect("encode bounds");
        let decoded_bounds =
            extract_encoded_row_scalars(&encoded_bounds, &[0, 1]).expect("decode bounds");
        assert_eq!(
            decoded_bounds,
            vec![
                Some(EncodedRowScalar::TimestampMillis(10)),
                Some(EncodedRowScalar::TimestampMillis(20))
            ]
        );

        let encoded_counts = encode_count_values(&[1, 2, 3]).expect("encode count values");
        let decoded_counts =
            extract_encoded_row_scalars(&encoded_counts, &[0, 1, 2]).expect("decode counts");
        assert_eq!(
            decoded_counts,
            vec![
                Some(EncodedRowScalar::Int64(1)),
                Some(EncodedRowScalar::Int64(2)),
                Some(EncodedRowScalar::Int64(3))
            ]
        );

        let encoded_incremental = encode_incremental_aggregate_values(&[
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::Int64),
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::TimestampMillis),
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::Utf8),
            dbsp::AggregateValue::Int64(9),
            dbsp::AggregateValue::TimestampMillis(11),
            dbsp::AggregateValue::Utf8("x".to_string()),
        ])
        .expect("encode incremental values");
        let decoded_incremental =
            extract_encoded_row_scalars(&encoded_incremental, &[0, 1, 2, 3, 4, 5])
                .expect("decode incremental values");
        assert_eq!(
            decoded_incremental,
            vec![
                None,
                None,
                None,
                Some(EncodedRowScalar::Int64(9)),
                Some(EncodedRowScalar::TimestampMillis(11)),
                Some(EncodedRowScalar::Utf8("x".to_string())),
            ]
        );

        let mut encoded = Vec::new();
        append_encoded_sum_like_value(7, &DbspScalarType::Int64, &mut encoded)
            .expect("append int sum");
        assert_eq!(
            extract_encoded_row_scalars(&[1_u32.to_le_bytes().as_slice(), &encoded].concat(), &[0])
                .expect("decode sum"),
            vec![Some(EncodedRowScalar::Int64(7))]
        );
        assert!(append_encoded_sum_like_value(1, &DbspScalarType::Utf8, &mut Vec::new()).is_err());
    }

    #[test]
    fn scalar_helpers_and_column_resolution_behave_as_expected() {
        assert!(bool_from_encoded_scalar(Some(&EncodedRowScalar::Bool(true))).expect("bool value"));
        assert!(!bool_from_encoded_scalar(None).expect("null bool"));
        assert!(bool_from_encoded_scalar(Some(&EncodedRowScalar::Int64(1))).is_err());

        assert_eq!(
            i64_from_encoded_scalar(Some(&EncodedRowScalar::TimestampMillis(5))),
            Some(5)
        );
        assert_eq!(
            i64_from_encoded_scalar(Some(&EncodedRowScalar::Utf8("x".to_string()))),
            None
        );

        assert_eq!(
            compare_encoded_scalars(&EncodedRowScalar::Int64(1), &EncodedRowScalar::Int64(2)),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            compare_encoded_scalars(
                &EncodedRowScalar::Utf8("a".to_string()),
                &EncodedRowScalar::Utf8("a".to_string())
            ),
            Some(std::cmp::Ordering::Equal)
        );
        assert_eq!(
            compare_encoded_scalars(
                &EncodedRowScalar::Int64(1),
                &EncodedRowScalar::Utf8("a".to_string())
            ),
            None
        );

        let mut scalar = Vec::new();
        append_encoded_scalar(&EncodedRowScalar::Bool(true), &mut scalar).expect("append bool");
        assert_eq!(scalar, vec![0x04, 0x01]);

        let encoded_key = encode_single_encoded_scalar_key(&EncodedRowScalar::Int64(9))
            .expect("encode single scalar key");
        assert_eq!(
            extract_encoded_row_scalars(&encoded_key, &[0]).expect("decode scalar key"),
            vec![Some(EncodedRowScalar::Int64(9))]
        );

        let input_schema = schema(vec![
            ("price", DbspScalarType::Int64),
            ("bidder", DbspScalarType::Int64),
        ]);
        let aggregate = DbspAggregateNode::try_new(
            Arc::clone(&input_schema),
            vec![(col("bidder"), None)],
            vec![(
                DbspAggregateFunction::Count,
                Some(col("price")),
                None,
                false,
                Some("count_price".to_string()),
            )],
        )
        .expect("aggregate node");

        let key_columns = direct_group_key_columns(
            aggregate.group_keys(),
            input_schema.as_ref(),
            &HashMap::new(),
        )
        .expect("direct group key columns");
        assert_eq!(key_columns, vec![1]);

        let key_expr = &aggregate.group_keys()[0];
        assert_eq!(
            direct_column_index(key_expr.expression(), input_schema.as_ref()),
            Some(1)
        );
        assert_eq!(
            resolved_expression_column_index(
                key_expr.expression(),
                input_schema.as_ref(),
                &HashMap::new()
            ),
            Some(1)
        );

        let aliased_expr = dbsp::DbspExpression::analyze(
            datafusion::logical_expr::Expr::Alias(datafusion::logical_expr::expr::Alias::new(
                col("price"),
                None::<String>,
                "p".to_string(),
            )),
            Arc::clone(&input_schema),
        )
        .expect("analyze alias expression");
        assert_eq!(
            direct_column_index(&aliased_expr, input_schema.as_ref()),
            Some(0)
        );

        assert_eq!(expression_lookup_key(aliased_expr.expr()), "price");

        assert_eq!(
            incremental_aggregate_value_from_encoded_scalar(
                Some(&EncodedRowScalar::Int64(7)),
                "graph",
                "ctx"
            ),
            Some(dbsp::AggregateValue::Int64(7))
        );
        assert_eq!(
            incremental_aggregate_value_from_encoded_scalar(
                Some(&EncodedRowScalar::Bool(true)),
                "graph",
                "ctx"
            ),
            None
        );
    }
}
