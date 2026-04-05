use std::sync::Arc;

use anyhow::Result;
use datafusion::scalar::ScalarValue;
use dbsp::circuit::{DbspExpression, RowSchema};

use crate::expression_eval::eval_df_expr;
pub(crate) use crate::expression_eval::scalar_to_bool;

use crate::stream_types::Row;

/// Evaluates a DBSP expression against a decoded row.
#[derive(Clone)]
pub struct ExpressionEvaluator {
    schema: Arc<RowSchema>,
    expr: datafusion::logical_expr::Expr,
}

impl ExpressionEvaluator {
    pub fn new(schema: Arc<RowSchema>, expr: &DbspExpression) -> Self {
        Self {
            schema,
            expr: expr.expr().clone(),
        }
    }

    pub fn eval(&self, row: &Row) -> Result<ScalarValue> {
        eval_df_expr(&self.expr, row, self.schema.as_ref())
    }

    pub fn eval_bool(&self, row: &Row) -> Result<bool> {
        let value = self.eval(row)?;
        scalar_to_bool(&value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;
    use datafusion::functions::expr_fn;
    use datafusion::logical_expr::expr::InList;
    use datafusion::logical_expr::{Between, BinaryExpr, Expr as DfExpr, Operator, TryCast};
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

    fn eval_expr(expr: DfExpr, schema: Arc<RowSchema>, row: Row) -> ScalarValue {
        let analyzed = DbspExpression::analyze(expr, Arc::clone(&schema)).expect("analyze expr");
        let evaluator = ExpressionEvaluator::new(schema, &analyzed);
        evaluator.eval(&row).expect("eval")
    }

    #[test]
    fn null_equals_null_is_filtered_out() {
        let schema = schema(vec![("a", DbspScalarType::Int64)]);
        let expr = DfExpr::BinaryExpr(BinaryExpr::new(
            Box::new(col("a")),
            Operator::Eq,
            Box::new(col("a")),
        ));
        let row = vec![ScalarValue::Int64(None)];
        let analyzed = DbspExpression::analyze(expr, Arc::clone(&schema)).expect("analyze expr");
        let evaluator = ExpressionEvaluator::new(schema, &analyzed);

        let value = evaluator.eval(&row).expect("eval");
        assert!(matches!(value, ScalarValue::Boolean(None)));
        assert!(!evaluator.eval_bool(&row).expect("eval bool"));
    }

    #[test]
    fn boolean_ops_propagate_nulls() {
        let schema = schema(vec![
            ("a", DbspScalarType::Bool),
            ("b", DbspScalarType::Bool),
        ]);
        let and_expr = DfExpr::BinaryExpr(BinaryExpr::new(
            Box::new(col("a")),
            Operator::And,
            Box::new(col("b")),
        ));
        let and_row = vec![ScalarValue::Boolean(Some(true)), ScalarValue::Boolean(None)];
        let and_value = eval_expr(and_expr, Arc::clone(&schema), and_row);
        assert!(matches!(and_value, ScalarValue::Boolean(None)));

        let or_expr = DfExpr::BinaryExpr(BinaryExpr::new(
            Box::new(col("a")),
            Operator::Or,
            Box::new(col("b")),
        ));
        let or_row = vec![
            ScalarValue::Boolean(Some(false)),
            ScalarValue::Boolean(None),
        ];
        let or_value = eval_expr(or_expr, Arc::clone(&schema), or_row);
        assert!(matches!(or_value, ScalarValue::Boolean(None)));

        let not_expr = DfExpr::Not(Box::new(col("a")));
        let not_row = vec![ScalarValue::Boolean(None), ScalarValue::Boolean(Some(true))];
        let not_value = eval_expr(not_expr, schema, not_row);
        assert!(matches!(not_value, ScalarValue::Boolean(None)));
    }

    #[test]
    fn supports_in_between_try_cast_and_scalar_functions() {
        let schema = schema(vec![
            ("a", DbspScalarType::Int64),
            ("b", DbspScalarType::Utf8),
        ]);

        let in_list = DfExpr::InList(InList::new(
            Box::new(col("a")),
            vec![DfExpr::Literal(ScalarValue::Int64(Some(1)), None)],
            false,
        ));
        let in_value = eval_expr(
            in_list,
            Arc::clone(&schema),
            vec![ScalarValue::Int64(Some(1)), ScalarValue::Utf8(None)],
        );
        assert_eq!(in_value, ScalarValue::Boolean(Some(true)));

        let between = DfExpr::Between(Between::new(
            Box::new(col("a")),
            false,
            Box::new(DfExpr::Literal(ScalarValue::Int64(Some(1)), None)),
            Box::new(DfExpr::Literal(ScalarValue::Int64(Some(5)), None)),
        ));
        let between_value = eval_expr(
            between,
            Arc::clone(&schema),
            vec![ScalarValue::Int64(Some(3)), ScalarValue::Utf8(None)],
        );
        assert_eq!(between_value, ScalarValue::Boolean(Some(true)));

        let try_cast = DfExpr::TryCast(TryCast::new(
            Box::new(col("b")),
            datafusion::arrow::datatypes::DataType::Int64,
        ));
        let cast_value = eval_expr(
            try_cast,
            Arc::clone(&schema),
            vec![
                ScalarValue::Int64(Some(0)),
                ScalarValue::Utf8(Some("not-a-number".to_string())),
            ],
        );
        assert_eq!(cast_value, ScalarValue::Int64(None));

        let lower_expr = expr_fn::lower(col("b"));
        let lower_value = eval_expr(
            lower_expr,
            Arc::clone(&schema),
            vec![
                ScalarValue::Int64(Some(0)),
                ScalarValue::Utf8(Some("HeLLo".to_string())),
            ],
        );
        assert_eq!(lower_value, ScalarValue::Utf8(Some("hello".to_string())));

        let coalesce_expr = expr_fn::coalesce(vec![
            DfExpr::Literal(ScalarValue::Utf8(None), None),
            col("b"),
        ]);
        let coalesce_value = eval_expr(
            coalesce_expr,
            schema,
            vec![
                ScalarValue::Int64(Some(0)),
                ScalarValue::Utf8(Some("ok".to_string())),
            ],
        );
        assert_eq!(coalesce_value, ScalarValue::Utf8(Some("ok".to_string())));
    }
}
