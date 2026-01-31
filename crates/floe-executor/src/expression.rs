use std::sync::Arc;

use anyhow::Result;
use datafusion::scalar::ScalarValue;
use dbsp::circuit::{DbspExpression, RowSchema};

use crate::expression_eval::eval_df_expr;
pub(crate) use crate::expression_eval::{scalar_equals, scalar_to_bool};

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
    use datafusion::logical_expr::{BinaryExpr, Expr as DfExpr, Operator};
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
}
