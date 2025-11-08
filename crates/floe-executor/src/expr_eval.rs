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
        Expr::NotEq(left, right) => {
            let lhs = evaluate(left, row)?;
            let rhs = evaluate(right, row)?;
            Ok(ScalarValue::Boolean(Some(lhs != rhs)))
        }
        Expr::Lt(left, right) => {
            let (lhs, rhs) = eval_int64_pair(left, right, row, "less-than comparison")?;
            Ok(ScalarValue::Boolean(Some(lhs < rhs)))
        }
        Expr::LtEq(left, right) => {
            let (lhs, rhs) = eval_int64_pair(left, right, row, "less-than-or-equal comparison")?;
            Ok(ScalarValue::Boolean(Some(lhs <= rhs)))
        }
        Expr::Gt(left, right) => {
            let (lhs, rhs) = eval_int64_pair(left, right, row, "greater-than comparison")?;
            Ok(ScalarValue::Boolean(Some(lhs > rhs)))
        }
        Expr::GtEq(left, right) => {
            let (lhs, rhs) = eval_int64_pair(left, right, row, "greater-than-or-equal comparison")?;
            Ok(ScalarValue::Boolean(Some(lhs >= rhs)))
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
        Expr::Add(left, right) => {
            let (lhs, rhs) = eval_int64_pair(left, right, row, "addition")?;
            Ok(ScalarValue::Int64(Some(lhs + rhs)))
        }
        Expr::Sub(left, right) => {
            let (lhs, rhs) = eval_int64_pair(left, right, row, "subtraction")?;
            Ok(ScalarValue::Int64(Some(lhs - rhs)))
        }
        Expr::Mul(left, right) => {
            let (lhs, rhs) = eval_int64_pair(left, right, row, "multiplication")?;
            Ok(ScalarValue::Int64(Some(lhs * rhs)))
        }
        Expr::Div(left, right) => {
            let (lhs, rhs) = eval_int64_pair(left, right, row, "division")?;
            if rhs == 0 {
                bail!("division by zero is not supported");
            }
            Ok(ScalarValue::Int64(Some(lhs / rhs)))
        }
        Expr::Mod(left, right) => {
            let (lhs, rhs) = eval_int64_pair(left, right, row, "modulo")?;
            if rhs == 0 {
                bail!("modulo by zero is not supported");
            }
            Ok(ScalarValue::Int64(Some(lhs % rhs)))
        }
        Expr::Neg(inner) => {
            let value = eval_int64(inner, row, "unary negation")?;
            Ok(ScalarValue::Int64(Some(-value)))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let needle = evaluate(expr, row)?;
            let mut contains = false;
            for candidate in list {
                if needle == evaluate(candidate, row)? {
                    contains = true;
                    break;
                }
            }
            let result = if *negated { !contains } else { contains };
            Ok(ScalarValue::Boolean(Some(result)))
        }
    }
}

pub fn evaluate_bool(expr: &Expr, row: &Row) -> Result<bool> {
    match evaluate(expr, row)? {
        ScalarValue::Boolean(Some(value)) => Ok(value),
        ScalarValue::Boolean(None) => Ok(false),
        other => bail!("expected boolean expression, got {other:?}"),
    }
}

fn eval_int64(expr: &Expr, row: &Row, context: &str) -> Result<i64> {
    match evaluate(expr, row)? {
        ScalarValue::Int64(Some(value)) => Ok(value),
        ScalarValue::Int64(None) => bail!("{context} does not support NULL Int64 values"),
        other => bail!("{context} supports Int64 values only, got {other:?}"),
    }
}

fn eval_int64_pair(left: &Expr, right: &Expr, row: &Row, context: &str) -> Result<(i64, i64)> {
    let lhs = eval_int64(left, row, context)?;
    let rhs = eval_int64(right, row, context)?;
    Ok((lhs, rhs))
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

    #[test]
    fn evaluates_extended_arithmetic_and_comparisons() {
        let row = vec![
            ScalarValue::Int64(Some(10)),
            ScalarValue::Int64(Some(3)),
            ScalarValue::Utf8(Some("region".into())),
        ];

        let sub_expr = Expr::Sub(Box::new(Expr::column(0)), Box::new(Expr::column(1)));
        assert_eq!(
            evaluate(&sub_expr, &row).unwrap(),
            ScalarValue::Int64(Some(7))
        );

        let mul_expr = Expr::Mul(Box::new(Expr::column(0)), Box::new(Expr::column(1)));
        assert_eq!(
            evaluate(&mul_expr, &row).unwrap(),
            ScalarValue::Int64(Some(30))
        );

        let div_expr = Expr::Div(Box::new(Expr::column(0)), Box::new(Expr::column(1)));
        assert_eq!(
            evaluate(&div_expr, &row).unwrap(),
            ScalarValue::Int64(Some(3))
        );

        let mod_expr = Expr::Mod(Box::new(Expr::column(0)), Box::new(Expr::column(1)));
        assert_eq!(
            evaluate(&mod_expr, &row).unwrap(),
            ScalarValue::Int64(Some(1))
        );

        let neg_expr = Expr::Neg(Box::new(Expr::column(1)));
        assert_eq!(
            evaluate(&neg_expr, &row).unwrap(),
            ScalarValue::Int64(Some(-3))
        );

        let lt_expr = Expr::Lt(Box::new(Expr::column(1)), Box::new(Expr::column(0)));
        assert!(evaluate_bool(&lt_expr, &row).unwrap());

        let gte_expr = Expr::GtEq(Box::new(Expr::column(0)), Box::new(Expr::column(1)));
        assert!(evaluate_bool(&gte_expr, &row).unwrap());

        let in_list_expr = Expr::InList {
            expr: Box::new(Expr::column(2)),
            list: vec![Expr::literal(ScalarValue::Utf8(Some("region".into())))],
            negated: false,
        };
        assert!(evaluate_bool(&in_list_expr, &row).unwrap());
    }
}
