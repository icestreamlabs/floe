#[cfg(test)]
use anyhow::Result;
#[cfg(test)]
use datafusion::scalar::ScalarValue;
#[cfg(test)]
use dbsp::{DbspExpression, RowSchema};

#[cfg(test)]
use crate::expression_eval::eval_df_expr;
#[cfg(test)]
use crate::expression_eval::scalar_to_bool;

#[cfg(test)]
pub(super) fn eval_scalar_expression(
    expr: &DbspExpression,
    row: &[ScalarValue],
    schema: &RowSchema,
) -> Result<ScalarValue> {
    eval_df_expr(expr.expr(), row, schema)
}

#[cfg(test)]
pub(super) fn eval_expression(
    expr: &DbspExpression,
    row: &[ScalarValue],
    schema: &RowSchema,
) -> Result<bool> {
    let value = eval_df_expr(expr.expr(), row, schema)?;
    scalar_to_bool(&value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;
    use datafusion::logical_expr::expr::{Between, InList};
    use datafusion::logical_expr::{BinaryExpr, Expr as DfExpr, Operator};
    use dbsp::circuit::schema::{Field, RowSchema};
    use dbsp::circuit::types::DbspScalarType;
    use proptest::prelude::*;
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

    fn test_schema() -> Arc<RowSchema> {
        schema(vec![
            ("a", DbspScalarType::Int64),
            ("b", DbspScalarType::Int64),
            ("c", DbspScalarType::Bool),
            ("d", DbspScalarType::Utf8),
        ])
    }

    fn literal_int(value: Option<i64>) -> DfExpr {
        DfExpr::Literal(ScalarValue::Int64(value), None)
    }

    fn literal_bool(value: Option<bool>) -> DfExpr {
        DfExpr::Literal(ScalarValue::Boolean(value), None)
    }

    fn literal_str(value: Option<String>) -> DfExpr {
        DfExpr::Literal(ScalarValue::Utf8(value), None)
    }

    fn binary(left: DfExpr, op: Operator, right: DfExpr) -> DfExpr {
        DfExpr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
    }

    fn int_expr_strategy() -> BoxedStrategy<DfExpr> {
        let leaf = prop_oneof![
            Just(col("a")),
            Just(col("b")),
            (-100i64..=100i64).prop_map(|value| literal_int(Some(value))),
            Just(literal_int(None)),
        ];
        leaf.prop_recursive(2, 8, 2, |inner| {
            prop_oneof![
                (inner.clone(), inner.clone()).prop_map(|(left, right)| binary(
                    left,
                    Operator::Plus,
                    right
                )),
                (inner.clone(), inner.clone()).prop_map(|(left, right)| binary(
                    left,
                    Operator::Minus,
                    right
                )),
                (inner.clone(), inner.clone()).prop_map(|(left, right)| binary(
                    left,
                    Operator::Multiply,
                    right
                )),
                inner
                    .clone()
                    .prop_map(|expr| DfExpr::Negative(Box::new(expr))),
            ]
        })
        .boxed()
    }

    fn ascii_string_strategy() -> BoxedStrategy<String> {
        prop::collection::vec(0u8..=25, 0..8)
            .prop_map(|bytes| {
                bytes
                    .into_iter()
                    .map(|value| (b'a' + value) as char)
                    .collect::<String>()
            })
            .boxed()
    }

    fn string_expr_strategy() -> BoxedStrategy<DfExpr> {
        let literal = ascii_string_strategy().prop_map(|value| literal_str(Some(value)));
        prop_oneof![Just(col("d")), literal, Just(literal_str(None))].boxed()
    }

    fn bool_expr_strategy() -> BoxedStrategy<DfExpr> {
        let comparison_ops = prop_oneof![
            Just(Operator::Eq),
            Just(Operator::NotEq),
            Just(Operator::Lt),
            Just(Operator::LtEq),
            Just(Operator::Gt),
            Just(Operator::GtEq),
        ];
        let int_compare = (
            int_expr_strategy(),
            comparison_ops.clone(),
            int_expr_strategy(),
        )
            .prop_map(|(left, op, right)| binary(left, op, right));
        let str_compare = (
            string_expr_strategy(),
            comparison_ops,
            string_expr_strategy(),
        )
            .prop_map(|(left, op, right)| binary(left, op, right));
        let between = (
            int_expr_strategy(),
            int_expr_strategy(),
            int_expr_strategy(),
        )
            .prop_map(|(value, low, high)| {
                DfExpr::Between(Between::new(
                    Box::new(value),
                    false,
                    Box::new(low),
                    Box::new(high),
                ))
            });
        let in_list = (
            int_expr_strategy(),
            prop::collection::vec(int_expr_strategy(), 1..4),
        )
            .prop_map(|(value, list)| DfExpr::InList(InList::new(Box::new(value), list, false)));

        let leaf = prop_oneof![
            Just(col("c")),
            any::<bool>().prop_map(|value| literal_bool(Some(value))),
            Just(literal_bool(None)),
            int_compare,
            str_compare,
            between,
            in_list,
            int_expr_strategy().prop_map(|expr| DfExpr::IsNull(Box::new(expr))),
            int_expr_strategy().prop_map(|expr| DfExpr::IsNotNull(Box::new(expr))),
        ];

        leaf.prop_recursive(3, 32, 2, |inner| {
            prop_oneof![
                inner.clone().prop_map(|expr| DfExpr::Not(Box::new(expr))),
                (inner.clone(), inner.clone()).prop_map(|(left, right)| binary(
                    left,
                    Operator::And,
                    right
                )),
                (inner.clone(), inner.clone()).prop_map(|(left, right)| binary(
                    left,
                    Operator::Or,
                    right
                )),
                inner
                    .clone()
                    .prop_map(|expr| DfExpr::IsTrue(Box::new(expr))),
                inner
                    .clone()
                    .prop_map(|expr| DfExpr::IsNotTrue(Box::new(expr))),
                inner
                    .clone()
                    .prop_map(|expr| DfExpr::IsFalse(Box::new(expr))),
                inner
                    .clone()
                    .prop_map(|expr| DfExpr::IsNotFalse(Box::new(expr))),
                inner
                    .clone()
                    .prop_map(|expr| DfExpr::IsUnknown(Box::new(expr))),
                inner
                    .clone()
                    .prop_map(|expr| DfExpr::IsNotUnknown(Box::new(expr))),
            ]
        })
        .boxed()
    }

    fn int_value_strategy() -> BoxedStrategy<ScalarValue> {
        prop_oneof![
            (-100i64..=100i64).prop_map(|value| ScalarValue::Int64(Some(value))),
            Just(ScalarValue::Int64(None)),
        ]
        .boxed()
    }

    fn bool_value_strategy() -> BoxedStrategy<ScalarValue> {
        prop_oneof![
            any::<bool>().prop_map(|value| ScalarValue::Boolean(Some(value))),
            Just(ScalarValue::Boolean(None)),
        ]
        .boxed()
    }

    fn string_value_strategy() -> BoxedStrategy<ScalarValue> {
        prop_oneof![
            ascii_string_strategy().prop_map(|value| ScalarValue::Utf8(Some(value))),
            Just(ScalarValue::Utf8(None)),
        ]
        .boxed()
    }

    fn row_strategy() -> BoxedStrategy<Vec<ScalarValue>> {
        (
            int_value_strategy(),
            int_value_strategy(),
            bool_value_strategy(),
            string_value_strategy(),
        )
            .prop_map(|(a, b, c, d)| vec![a, b, c, d])
            .boxed()
    }

    #[test]
    fn eval_expression_uses_shared_logic() {
        let schema = schema(vec![("a", DbspScalarType::Int64)]);
        let expr = DfExpr::BinaryExpr(BinaryExpr::new(
            Box::new(col("a")),
            Operator::Eq,
            Box::new(col("a")),
        ));
        let analyzed = DbspExpression::analyze(expr, Arc::clone(&schema)).expect("analyze expr");

        let row = vec![ScalarValue::Int64(Some(1))];
        assert!(eval_expression(&analyzed, &row, schema.as_ref()).expect("eval expression"));
    }

    proptest! {
        #[test]
        fn prop_eval_paths_consistent(expr in bool_expr_strategy(), row in row_strategy()) {
            let schema = test_schema();
            let analyzed = match DbspExpression::analyze(expr.clone(), Arc::clone(&schema)) {
                Ok(analyzed) => analyzed,
                Err(_) => return Ok(()),
            };
            let evaluator = crate::expression::ExpressionEvaluator::new(
                Arc::clone(&schema),
                &analyzed,
            );

            let eval_bool = evaluator.eval_bool(&row);
            let eval_expr = eval_expression(&analyzed, &row, schema.as_ref());

            prop_assert_eq!(eval_bool.is_ok(), eval_expr.is_ok());

            if let Ok(value) = eval_bool {
                prop_assert_eq!(value, eval_expr.unwrap());
            }
        }
    }
}
