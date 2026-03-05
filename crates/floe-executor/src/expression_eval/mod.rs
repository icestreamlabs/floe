use anyhow::{Result, anyhow, bail};
use chrono::{TimeZone, Timelike, Utc};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::Column;
use datafusion::logical_expr::expr::Case;
use datafusion::logical_expr::expr::{InList, ScalarFunction};
use datafusion::logical_expr::{Expr as DfExpr, Operator};
use datafusion::scalar::ScalarValue;
use dbsp::circuit::RowSchema;
use regex::Regex;

pub(crate) fn eval_df_expr(
    expr: &DfExpr,
    row: &[ScalarValue],
    schema: &RowSchema,
) -> Result<ScalarValue> {
    match expr {
        DfExpr::Alias(alias) => eval_df_expr(alias.expr.as_ref(), row, schema),
        DfExpr::Column(column) => {
            let idx = resolve_column(schema, column)?;
            row.get(idx)
                .cloned()
                .ok_or_else(|| anyhow!("column index {idx} out of bounds"))
        }
        DfExpr::Literal(value, _) => Ok(value.clone()),
        DfExpr::BinaryExpr(binary) => {
            let left = eval_df_expr(binary.left.as_ref(), row, schema)?;
            let right = eval_df_expr(binary.right.as_ref(), row, schema)?;
            eval_binary(binary.op, left, right)
        }
        DfExpr::Not(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            let result = scalar_to_bool_opt(&value)?.map(|val| !val);
            Ok(ScalarValue::Boolean(result))
        }
        DfExpr::Negative(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            let negated = match value {
                ScalarValue::Int64(v) => ScalarValue::Int64(v.map(|val| -val)),
                ScalarValue::TimestampMillisecond(v, tz) => {
                    ScalarValue::TimestampMillisecond(v.map(|val| -val), tz)
                }
                other => bail!("unsupported type for negation: {other:?}"),
            };
            Ok(negated)
        }
        DfExpr::IsNull(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            Ok(ScalarValue::Boolean(Some(value.is_null())))
        }
        DfExpr::IsNotNull(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            Ok(ScalarValue::Boolean(Some(!value.is_null())))
        }
        DfExpr::IsTrue(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            let result = matches!(scalar_to_bool_opt(&value)?, Some(true));
            Ok(ScalarValue::Boolean(Some(result)))
        }
        DfExpr::IsNotTrue(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            let result = !matches!(scalar_to_bool_opt(&value)?, Some(true));
            Ok(ScalarValue::Boolean(Some(result)))
        }
        DfExpr::IsFalse(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            let result = matches!(scalar_to_bool_opt(&value)?, Some(false));
            Ok(ScalarValue::Boolean(Some(result)))
        }
        DfExpr::IsNotFalse(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            let result = !matches!(scalar_to_bool_opt(&value)?, Some(false));
            Ok(ScalarValue::Boolean(Some(result)))
        }
        DfExpr::IsUnknown(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            Ok(ScalarValue::Boolean(Some(value.is_null())))
        }
        DfExpr::IsNotUnknown(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            Ok(ScalarValue::Boolean(Some(!value.is_null())))
        }
        DfExpr::Like(like) => {
            let value = eval_df_expr(like.expr.as_ref(), row, schema)?;
            let pattern_value = eval_df_expr(like.pattern.as_ref(), row, schema)?;
            let text = match value {
                ScalarValue::Utf8(Some(text)) => text,
                _ => bail!("LIKE expects utf8 input"),
            };
            let pattern = match pattern_value {
                ScalarValue::Utf8(Some(pattern)) => pattern,
                _ => bail!("LIKE pattern must be utf8 literal"),
            };
            Ok(ScalarValue::Boolean(Some(matches_like(&text, &pattern))))
        }
        DfExpr::Cast(cast) => {
            let value = eval_df_expr(cast.expr.as_ref(), row, schema)?;
            cast_value(&value, &cast.data_type)
        }
        DfExpr::TryCast(cast) => {
            let value = eval_df_expr(cast.expr.as_ref(), row, schema)?;
            Ok(try_cast_value(&value, &cast.data_type))
        }
        DfExpr::Case(case) => eval_case(case, row, schema),
        DfExpr::Between(between) => eval_between(between, row, schema),
        DfExpr::InList(in_list) => eval_in_list(in_list, row, schema),
        DfExpr::ScalarFunction(func) => eval_scalar_function(func, row, schema),
        other => bail!("unsupported expression: {other:?}"),
    }
}

// SQL comparisons involving NULL yield NULL (unknown).
pub(crate) fn scalar_equals(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<Option<bool>> {
    if lhs.is_null() || rhs.is_null() {
        return Ok(None);
    }
    let result = match (lhs, rhs) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l == r,
        (
            ScalarValue::TimestampMillisecond(Some(l), _),
            ScalarValue::TimestampMillisecond(Some(r), _),
        ) => l == r,
        (ScalarValue::Utf8(Some(l)), ScalarValue::Utf8(Some(r))) => l == r,
        (ScalarValue::Boolean(Some(l)), ScalarValue::Boolean(Some(r))) => l == r,
        _ => false,
    };
    Ok(Some(result))
}

// SQL predicate contexts treat NULL as false (unknown).
pub(crate) fn scalar_to_bool(value: &ScalarValue) -> Result<bool> {
    Ok(scalar_to_bool_opt(value)?.unwrap_or(false))
}

mod casts;
mod predicates;
mod scalar_functions;

use casts::*;
use predicates::*;
use scalar_functions::*;

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use datafusion::common::Column;
    use datafusion::functions::expr_fn;
    use datafusion::logical_expr::expr::{InList, ScalarFunction};
    use datafusion::logical_expr::expr_fn::create_udf;
    use datafusion::logical_expr::{Between, Expr as DfExpr, Like, Operator, TryCast};
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionImplementation, Volatility};
    use dbsp::circuit::schema::{Field, RowSchema};
    use dbsp::circuit::types::DbspScalarType;
    use std::sync::Arc;

    fn schema(fields: Vec<(&str, DbspScalarType)>) -> Arc<RowSchema> {
        let fields = fields
            .into_iter()
            .map(|(name, ty)| Field::new(name, ty, true))
            .collect();
        RowSchema::try_new(fields).expect("schema")
    }

    fn col(name: &str) -> DfExpr {
        DfExpr::Column(Column::new_unqualified(name.to_string()))
    }

    fn eval(expr: DfExpr, schema: Arc<RowSchema>, row: Vec<ScalarValue>) -> ScalarValue {
        eval_df_expr(&expr, &row, schema.as_ref()).expect("eval")
    }

    fn udf_expr(
        name: &str,
        input_types: Vec<DataType>,
        return_type: DataType,
        args: Vec<DfExpr>,
    ) -> DfExpr {
        let passthrough: ScalarFunctionImplementation = Arc::new(
            |args: &[ColumnarValue]| -> datafusion::common::Result<ColumnarValue> {
                Ok(args
                    .first()
                    .cloned()
                    .unwrap_or(ColumnarValue::Scalar(ScalarValue::Null)))
            },
        );
        let udf = create_udf(
            name,
            input_types,
            return_type,
            Volatility::Immutable,
            passthrough,
        );
        DfExpr::ScalarFunction(ScalarFunction::new_udf(Arc::new(udf), args))
    }

    #[test]
    fn in_list_null_semantics() {
        let schema = schema(vec![("a", DbspScalarType::Int64)]);
        let in_list = DfExpr::InList(InList::new(
            Box::new(col("a")),
            vec![DfExpr::Literal(ScalarValue::Int64(Some(1)), None)],
            false,
        ));
        let value = eval(
            in_list.clone(),
            Arc::clone(&schema),
            vec![ScalarValue::Int64(Some(1))],
        );
        assert_eq!(value, ScalarValue::Boolean(Some(true)));

        let value = eval(
            in_list.clone(),
            Arc::clone(&schema),
            vec![ScalarValue::Int64(Some(2))],
        );
        assert_eq!(value, ScalarValue::Boolean(Some(false)));

        let in_list_null = DfExpr::InList(InList::new(
            Box::new(col("a")),
            vec![
                DfExpr::Literal(ScalarValue::Int64(Some(1)), None),
                DfExpr::Literal(ScalarValue::Int64(None), None),
            ],
            false,
        ));
        let value = eval(
            in_list_null,
            Arc::clone(&schema),
            vec![ScalarValue::Int64(Some(2))],
        );
        assert_eq!(value, ScalarValue::Boolean(None));

        let value = eval(in_list, Arc::clone(&schema), vec![ScalarValue::Int64(None)]);
        assert_eq!(value, ScalarValue::Boolean(None));
    }

    #[test]
    fn between_null_semantics() {
        let schema = schema(vec![("a", DbspScalarType::Int64)]);
        let between = DfExpr::Between(Between::new(
            Box::new(col("a")),
            false,
            Box::new(DfExpr::Literal(ScalarValue::Int64(Some(1)), None)),
            Box::new(DfExpr::Literal(ScalarValue::Int64(Some(3)), None)),
        ));
        let value = eval(
            between.clone(),
            Arc::clone(&schema),
            vec![ScalarValue::Int64(Some(2))],
        );
        assert_eq!(value, ScalarValue::Boolean(Some(true)));

        let value = eval(
            between.clone(),
            Arc::clone(&schema),
            vec![ScalarValue::Int64(Some(5))],
        );
        assert_eq!(value, ScalarValue::Boolean(Some(false)));

        let value = eval(between, Arc::clone(&schema), vec![ScalarValue::Int64(None)]);
        assert_eq!(value, ScalarValue::Boolean(None));
    }

    #[test]
    fn try_cast_returns_null_on_failure() {
        let schema = schema(vec![("a", DbspScalarType::Utf8)]);
        let expr = DfExpr::TryCast(TryCast::new(Box::new(col("a")), DataType::Int64));
        let value = eval(
            expr,
            Arc::clone(&schema),
            vec![ScalarValue::Utf8(Some("not-a-number".to_string()))],
        );
        assert_eq!(value, ScalarValue::Int64(None));
    }

    #[test]
    fn scalar_functions_execute() {
        let schema = schema(vec![("a", DbspScalarType::Utf8)]);
        let lower_expr = expr_fn::lower(col("a"));
        let value = eval(
            lower_expr,
            Arc::clone(&schema),
            vec![ScalarValue::Utf8(Some("HeLLo".to_string()))],
        );
        assert_eq!(value, ScalarValue::Utf8(Some("hello".to_string())));

        let coalesce_expr = expr_fn::coalesce(vec![
            DfExpr::Literal(ScalarValue::Utf8(None), None),
            col("a"),
        ]);
        let value = eval(
            coalesce_expr,
            Arc::clone(&schema),
            vec![ScalarValue::Utf8(Some("ok".to_string()))],
        );
        assert_eq!(value, ScalarValue::Utf8(Some("ok".to_string())));

        let length_expr = expr_fn::length(col("a"));
        let value = eval(
            length_expr,
            Arc::clone(&schema),
            vec![ScalarValue::Utf8(Some("hi".to_string()))],
        );
        assert_eq!(value, ScalarValue::Int64(Some(2)));
    }

    #[test]
    fn like_supports_percent_and_underscore_wildcards() {
        assert!(matches_like("foobarbaz", "%bar%"));
        assert!(matches_like("abcz", "a_c%"));
        assert!(matches_like("a💡c", "a_c"));
        assert!(matches_like("", "%"));
        assert!(!matches_like("abc", "a_d%"));
        assert!(!matches_like("ac", "a_c"));
    }

    #[test]
    fn like_expression_uses_general_pattern_matching() {
        let schema = schema(vec![("txt", DbspScalarType::Utf8)]);
        let expr = DfExpr::Like(Like::new(
            false,
            Box::new(col("txt")),
            Box::new(DfExpr::Literal(
                ScalarValue::Utf8(Some("a_c%".to_string())),
                None,
            )),
            None,
            false,
        ));
        let value = eval(
            expr.clone(),
            Arc::clone(&schema),
            vec![ScalarValue::Utf8(Some("abchello".to_string()))],
        );
        assert_eq!(value, ScalarValue::Boolean(Some(true)));

        let value = eval(
            expr,
            schema,
            vec![ScalarValue::Utf8(Some("ac".to_string()))],
        );
        assert_eq!(value, ScalarValue::Boolean(Some(false)));
    }

    #[test]
    fn nexmark_scalar_functions_execute_with_null_semantics() {
        let schema = schema(vec![
            ("ts", DbspScalarType::TimestampMillis),
            ("url", DbspScalarType::Utf8),
            ("text", DbspScalarType::Utf8),
            ("channel", DbspScalarType::Utf8),
        ]);

        let hour_expr = udf_expr(
            "hour",
            vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
            DataType::Int64,
            vec![col("ts")],
        );
        let value = eval(
            hour_expr,
            Arc::clone(&schema),
            vec![
                ScalarValue::TimestampMillisecond(Some(1_600_000_000), None),
                ScalarValue::Utf8(Some("http://x/?a=1&channel_id=42".to_string())),
                ScalarValue::Utf8(Some("abcccd".to_string())),
                ScalarValue::Utf8(Some("Apple".to_string())),
            ],
        );
        assert_eq!(value, ScalarValue::Int64(Some(12)));

        let date_expr = udf_expr(
            "date_format",
            vec![
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Utf8,
            ],
            DataType::Utf8,
            vec![
                col("ts"),
                DfExpr::Literal(ScalarValue::Utf8(Some("yyyy-MM-dd".to_string())), None),
            ],
        );
        let value = eval(
            date_expr,
            Arc::clone(&schema),
            vec![
                ScalarValue::TimestampMillisecond(Some(1_600_000_000), None),
                ScalarValue::Utf8(Some("http://x/?a=1&channel_id=42".to_string())),
                ScalarValue::Utf8(Some("abcccd".to_string())),
                ScalarValue::Utf8(Some("Apple".to_string())),
            ],
        );
        assert_eq!(value, ScalarValue::Utf8(Some("1970-01-19".to_string())));

        let regex_expr = udf_expr(
            "regexp_extract",
            vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
            DataType::Utf8,
            vec![
                col("url"),
                DfExpr::Literal(
                    ScalarValue::Utf8(Some("(&|^)channel_id=([^&]*)".to_string())),
                    None,
                ),
                DfExpr::Literal(ScalarValue::Int64(Some(2)), None),
            ],
        );
        let value = eval(
            regex_expr.clone(),
            Arc::clone(&schema),
            vec![
                ScalarValue::TimestampMillisecond(Some(1_600_000_000), None),
                ScalarValue::Utf8(Some("http://x/?a=1&channel_id=42".to_string())),
                ScalarValue::Utf8(Some("abcccd".to_string())),
                ScalarValue::Utf8(Some("Apple".to_string())),
            ],
        );
        assert_eq!(value, ScalarValue::Utf8(Some("42".to_string())));
        let value = eval(
            regex_expr,
            Arc::clone(&schema),
            vec![
                ScalarValue::TimestampMillisecond(Some(1_600_000_000), None),
                ScalarValue::Utf8(None),
                ScalarValue::Utf8(Some("abcccd".to_string())),
                ScalarValue::Utf8(Some("Apple".to_string())),
            ],
        );
        assert_eq!(value, ScalarValue::Utf8(None));

        let split_expr = udf_expr(
            "split_index",
            vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
            DataType::Utf8,
            vec![
                col("url"),
                DfExpr::Literal(ScalarValue::Utf8(Some("/".to_string())), None),
                DfExpr::Literal(ScalarValue::Int64(Some(3)), None),
            ],
        );
        let value = eval(
            split_expr,
            Arc::clone(&schema),
            vec![
                ScalarValue::TimestampMillisecond(Some(1_600_000_000), None),
                ScalarValue::Utf8(Some("http://host/a/b/c".to_string())),
                ScalarValue::Utf8(Some("abcccd".to_string())),
                ScalarValue::Utf8(Some("Apple".to_string())),
            ],
        );
        assert_eq!(value, ScalarValue::Utf8(Some("a".to_string())));

        let count_char_expr = udf_expr(
            "count_char",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Int64,
            vec![
                col("text"),
                DfExpr::Literal(ScalarValue::Utf8(Some("c".to_string())), None),
            ],
        );
        let value = eval(
            count_char_expr,
            Arc::clone(&schema),
            vec![
                ScalarValue::TimestampMillisecond(Some(1_600_000_000), None),
                ScalarValue::Utf8(Some("http://x/?channel_id=42".to_string())),
                ScalarValue::Utf8(Some("abcccd".to_string())),
                ScalarValue::Utf8(Some("Apple".to_string())),
            ],
        );
        assert_eq!(value, ScalarValue::Int64(Some(3)));
    }

    #[test]
    fn case_and_nested_predicates_match_q21_shape() {
        let schema = schema(vec![
            ("channel", DbspScalarType::Utf8),
            ("url", DbspScalarType::Utf8),
        ]);

        let lower_channel = expr_fn::lower(col("channel"));
        let extract = udf_expr(
            "regexp_extract",
            vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
            DataType::Utf8,
            vec![
                col("url"),
                DfExpr::Literal(
                    ScalarValue::Utf8(Some("(&|^)channel_id=([^&]*)".to_string())),
                    None,
                ),
                DfExpr::Literal(ScalarValue::Int64(Some(2)), None),
            ],
        );

        let case_expr = DfExpr::Case(Case::new(
            None,
            vec![(
                Box::new(DfExpr::BinaryExpr(
                    datafusion::logical_expr::BinaryExpr::new(
                        Box::new(lower_channel),
                        Operator::Eq,
                        Box::new(DfExpr::Literal(
                            ScalarValue::Utf8(Some("apple".to_string())),
                            None,
                        )),
                    ),
                )),
                Box::new(DfExpr::Literal(
                    ScalarValue::Utf8(Some("0".to_string())),
                    None,
                )),
            )],
            Some(Box::new(extract.clone())),
        ));

        let row = vec![
            ScalarValue::Utf8(Some("custom".to_string())),
            ScalarValue::Utf8(Some("http://x/?a=1&channel_id=17".to_string())),
        ];
        let value = eval(case_expr.clone(), Arc::clone(&schema), row);
        assert_eq!(value, ScalarValue::Utf8(Some("17".to_string())));

        let row = vec![
            ScalarValue::Utf8(Some("Apple".to_string())),
            ScalarValue::Utf8(Some("http://x/?a=1&channel_id=17".to_string())),
        ];
        let value = eval(case_expr, Arc::clone(&schema), row);
        assert_eq!(value, ScalarValue::Utf8(Some("0".to_string())));

        let predicate = DfExpr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(DfExpr::IsNotNull(Box::new(extract))),
            Operator::Or,
            Box::new(DfExpr::InList(InList::new(
                Box::new(expr_fn::lower(col("channel"))),
                vec![
                    DfExpr::Literal(ScalarValue::Utf8(Some("apple".to_string())), None),
                    DfExpr::Literal(ScalarValue::Utf8(Some("google".to_string())), None),
                ],
                false,
            ))),
        ));

        let row = vec![
            ScalarValue::Utf8(Some("custom".to_string())),
            ScalarValue::Utf8(Some("http://x/".to_string())),
        ];
        let value = eval(predicate, Arc::clone(&schema), row);
        assert_eq!(value, ScalarValue::Boolean(Some(false)));
    }

    #[test]
    fn predicate_truth_table_matches_sql_nulls() {
        let schema = schema(vec![
            ("a", DbspScalarType::Bool),
            ("b", DbspScalarType::Bool),
        ]);
        let and_expr = DfExpr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(col("a")),
            Operator::And,
            Box::new(col("b")),
        ));
        let or_expr = DfExpr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(col("a")),
            Operator::Or,
            Box::new(col("b")),
        ));

        let cases = vec![
            (Some(true), Some(true), Some(true), Some(true)),
            (Some(true), Some(false), Some(false), Some(true)),
            (Some(false), Some(true), Some(false), Some(true)),
            (Some(false), Some(false), Some(false), Some(false)),
            (Some(true), None, None, Some(true)),
            (Some(false), None, Some(false), None),
            (None, Some(true), None, Some(true)),
            (None, Some(false), Some(false), None),
            (None, None, None, None),
        ];

        for (left, right, expected_and, expected_or) in cases {
            let row = vec![ScalarValue::Boolean(left), ScalarValue::Boolean(right)];
            let and_val = eval(and_expr.clone(), Arc::clone(&schema), row.clone());
            let or_val = eval(or_expr.clone(), Arc::clone(&schema), row);
            assert_eq!(and_val, ScalarValue::Boolean(expected_and));
            assert_eq!(or_val, ScalarValue::Boolean(expected_or));
        }
    }
}
