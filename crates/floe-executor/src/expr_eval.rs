use anyhow::{Context, Result, bail};
use datafusion::scalar::ScalarValue;

use crate::dataflow_plan::Expr;
use crate::stream_types::Row;

pub fn evaluate(expr: &Expr, row: &Row) -> Result<ScalarValue> {
    match expr {
        Expr::Column(index) => row
            .get(*index)
            .cloned()
            .with_context(|| format!("column index {index} is out of bounds")),
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Eq(left, right) => {
            let lhs = evaluate(left, row)?;
            let rhs = evaluate(right, row)?;
            Ok(ScalarValue::Boolean(Some(lhs == rhs)))
        }
        Expr::And(left, right) => {
            let lhs = evaluate_bool(left, row)?;
            if !lhs {
                return Ok(ScalarValue::Boolean(Some(false)));
            }
            let rhs = evaluate_bool(right, row)?;
            Ok(ScalarValue::Boolean(Some(lhs && rhs)))
        }
        Expr::Or(left, right) => {
            let lhs = evaluate_bool(left, row)?;
            if lhs {
                return Ok(ScalarValue::Boolean(Some(true)));
            }
            let rhs = evaluate_bool(right, row)?;
            Ok(ScalarValue::Boolean(Some(lhs || rhs)))
        }
        Expr::Add(left, right) => match (evaluate(left, row)?, evaluate(right, row)?) {
            (ScalarValue::Int64(Some(lhs)), ScalarValue::Int64(Some(rhs))) => {
                Ok(ScalarValue::Int64(Some(lhs + rhs)))
            }
            _ => bail!("addition currently supports Int64 values only"),
        },
    }
}

pub fn evaluate_bool(expr: &Expr, row: &Row) -> Result<bool> {
    match evaluate(expr, row)? {
        ScalarValue::Boolean(Some(value)) => Ok(value),
        ScalarValue::Boolean(None) => Ok(false),
        other => bail!("expected boolean expression, got {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluates_column_and_literal() {
        let row = vec![ScalarValue::Int64(Some(10)), ScalarValue::Int64(Some(5))];
        assert_eq!(evaluate(&Expr::column(0), &row).unwrap(), row[0]);
        let literal = ScalarValue::Int64(Some(1));
        assert_eq!(
            evaluate(&Expr::literal(literal.clone()), &row).unwrap(),
            literal
        );
    }

    #[test]
    fn evaluates_arithmetic_and_boolean_logic() {
        let row = vec![ScalarValue::Int64(Some(10)), ScalarValue::Int64(Some(10))];
        let add_expr = Expr::Add(
            Box::new(Expr::column(0)),
            Box::new(Expr::literal(ScalarValue::Int64(Some(5)))),
        );
        assert_eq!(
            evaluate(&add_expr, &row).unwrap(),
            ScalarValue::Int64(Some(15))
        );

        let eq_expr = Expr::Eq(Box::new(Expr::column(0)), Box::new(Expr::column(1)));
        assert_eq!(evaluate_bool(&eq_expr, &row).unwrap(), true);

        let predicate = Expr::And(
            Box::new(eq_expr),
            Box::new(Expr::literal(ScalarValue::Boolean(Some(true)))),
        );
        assert!(evaluate_bool(&predicate, &row).unwrap());
    }
}
