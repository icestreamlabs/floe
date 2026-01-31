use anyhow::Result;
use datafusion::scalar::ScalarValue;
use dbsp::circuit::plan::DbspProjectExpr;
use dbsp::{DbspExpression, DbspPredicate, RowSchema};

use crate::expression_eval::{eval_df_expr, scalar_to_bool};

pub(super) fn eval_predicate(
    predicate: &DbspPredicate,
    row: &[ScalarValue],
    schema: &RowSchema,
) -> Result<bool> {
    let value = eval_df_expr(predicate.expression().expr(), row, schema)?;
    scalar_to_bool(&value)
}

pub(super) fn eval_projection(
    expressions: &[DbspProjectExpr],
    row: &[ScalarValue],
    schema: &RowSchema,
) -> Result<Vec<ScalarValue>> {
    expressions
        .iter()
        .map(|expr| eval_df_expr(expr.expression().expr(), row, schema))
        .collect()
}

pub(super) fn eval_scalar_expression(
    expr: &DbspExpression,
    row: &[ScalarValue],
    schema: &RowSchema,
) -> Result<ScalarValue> {
    eval_df_expr(expr.expr(), row, schema)
}

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
}
