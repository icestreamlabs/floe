use std::cmp::Ordering;

use anyhow::{Context, Result, anyhow, bail};
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
            Ok(ScalarValue::Boolean(Some(scalar_equal(&lhs, &rhs))))
        }
        Expr::NotEq(left, right) => {
            let lhs = evaluate(left, row)?;
            let rhs = evaluate(right, row)?;
            Ok(ScalarValue::Boolean(Some(!scalar_equal(&lhs, &rhs))))
        }
        Expr::Lt(left, right) => {
            let (lhs, rhs) = eval_numeric_pair(left, right, row, "less-than comparison")?;
            let order = compare_numeric(lhs, rhs, "less-than comparison")?;
            Ok(ScalarValue::Boolean(Some(order == Ordering::Less)))
        }
        Expr::LtEq(left, right) => {
            let (lhs, rhs) = eval_numeric_pair(left, right, row, "less-than-or-equal comparison")?;
            let order = compare_numeric(lhs, rhs, "less-than-or-equal comparison")?;
            Ok(ScalarValue::Boolean(Some(order != Ordering::Greater)))
        }
        Expr::Gt(left, right) => {
            let (lhs, rhs) = eval_numeric_pair(left, right, row, "greater-than comparison")?;
            let order = compare_numeric(lhs, rhs, "greater-than comparison")?;
            Ok(ScalarValue::Boolean(Some(order == Ordering::Greater)))
        }
        Expr::GtEq(left, right) => {
            let (lhs, rhs) =
                eval_numeric_pair(left, right, row, "greater-than-or-equal comparison")?;
            let order = compare_numeric(lhs, rhs, "greater-than-or-equal comparison")?;
            Ok(ScalarValue::Boolean(Some(order != Ordering::Less)))
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
            let (lhs, rhs) = eval_numeric_pair(left, right, row, "addition")?;
            Ok(match (lhs, rhs) {
                (NumericValue::Int(l), NumericValue::Int(r)) => ScalarValue::Int64(Some(l + r)),
                _ => ScalarValue::Float64(Some(lhs.to_f64() + rhs.to_f64())),
            })
        }
        Expr::Sub(left, right) => {
            let (lhs, rhs) = eval_numeric_pair(left, right, row, "subtraction")?;
            Ok(match (lhs, rhs) {
                (NumericValue::Int(l), NumericValue::Int(r)) => ScalarValue::Int64(Some(l - r)),
                _ => ScalarValue::Float64(Some(lhs.to_f64() - rhs.to_f64())),
            })
        }
        Expr::Mul(left, right) => {
            let (lhs, rhs) = eval_numeric_pair(left, right, row, "multiplication")?;
            Ok(match (lhs, rhs) {
                (NumericValue::Int(l), NumericValue::Int(r)) => ScalarValue::Int64(Some(l * r)),
                _ => ScalarValue::Float64(Some(lhs.to_f64() * rhs.to_f64())),
            })
        }
        Expr::Div(left, right) => {
            let (lhs, rhs) = eval_numeric_pair(left, right, row, "division")?;
            if rhs.is_zero() {
                bail!("division by zero is not supported");
            }
            Ok(match (lhs, rhs) {
                (NumericValue::Int(l), NumericValue::Int(r)) => ScalarValue::Int64(Some(l / r)),
                _ => ScalarValue::Float64(Some(lhs.to_f64() / rhs.to_f64())),
            })
        }
        Expr::Mod(left, right) => {
            let (lhs, rhs) = eval_int64_pair(left, right, row, "modulo")?;
            if rhs == 0 {
                bail!("modulo by zero is not supported");
            }
            Ok(ScalarValue::Int64(Some(lhs % rhs)))
        }
        Expr::Neg(inner) => {
            let value = eval_numeric(inner, row, "unary negation")?;
            Ok(match value {
                NumericValue::Int(v) => ScalarValue::Int64(Some(-v)),
                NumericValue::Float(v) => ScalarValue::Float64(Some(-v)),
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let needle = evaluate(expr, row)?;
            let mut contains = false;
            for candidate in list {
                if scalar_equal(&needle, &evaluate(candidate, row)?) {
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

fn eval_numeric(expr: &Expr, row: &Row, context: &str) -> Result<NumericValue> {
    match evaluate(expr, row)? {
        ScalarValue::Int64(Some(value)) => Ok(NumericValue::Int(value)),
        ScalarValue::Float64(Some(value)) => Ok(NumericValue::Float(value)),
        ScalarValue::Int64(None) | ScalarValue::Float64(None) => {
            bail!("{context} does not support NULL numeric values")
        }
        other => bail!("{context} supports Int64 or Float64 values only, got {other:?}"),
    }
}

fn eval_numeric_pair(
    left: &Expr,
    right: &Expr,
    row: &Row,
    context: &str,
) -> Result<(NumericValue, NumericValue)> {
    let lhs = eval_numeric(left, row, context)?;
    let rhs = eval_numeric(right, row, context)?;
    Ok((lhs, rhs))
}

fn compare_numeric(lhs: NumericValue, rhs: NumericValue, context: &str) -> Result<Ordering> {
    match (lhs, rhs) {
        (NumericValue::Int(l), NumericValue::Int(r)) => Ok(l.cmp(&r)),
        _ => {
            let ordering = lhs
                .to_f64()
                .partial_cmp(&rhs.to_f64())
                .ok_or_else(|| anyhow!("{context} comparison is undefined for NaN values"))?;
            Ok(ordering)
        }
    }
}

fn numeric_from_scalar(value: &ScalarValue) -> Option<NumericValue> {
    match value {
        ScalarValue::Int64(Some(v)) => Some(NumericValue::Int(*v)),
        ScalarValue::Float64(Some(v)) => Some(NumericValue::Float(*v)),
        _ => None,
    }
}

fn scalar_equal(lhs: &ScalarValue, rhs: &ScalarValue) -> bool {
    match (numeric_from_scalar(lhs), numeric_from_scalar(rhs)) {
        (Some(NumericValue::Int(l)), Some(NumericValue::Int(r))) => l == r,
        (Some(l), Some(r)) => l.to_f64() == r.to_f64(),
        _ => lhs == rhs,
    }
}

#[derive(Clone, Copy, Debug)]
enum NumericValue {
    Int(i64),
    Float(f64),
}

impl NumericValue {
    fn to_f64(self) -> f64 {
        match self {
            NumericValue::Int(v) => v as f64,
            NumericValue::Float(v) => v,
        }
    }

    fn is_zero(self) -> bool {
        match self {
            NumericValue::Int(v) => v == 0,
            NumericValue::Float(v) => v == 0.0,
        }
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

    #[test]
    fn evaluates_float_arithmetic_and_equality() {
        let row = vec![ScalarValue::Int64(Some(4)), ScalarValue::Float64(Some(2.5))];

        let mul_expr = Expr::Mul(
            Box::new(Expr::column(0)),
            Box::new(Expr::literal(ScalarValue::Float64(Some(0.5)))),
        );
        assert_eq!(
            evaluate(&mul_expr, &row).unwrap(),
            ScalarValue::Float64(Some(2.0))
        );

        let div_expr = Expr::Div(Box::new(Expr::column(1)), Box::new(Expr::column(0)));
        assert_eq!(
            evaluate(&div_expr, &row).unwrap(),
            ScalarValue::Float64(Some(0.625))
        );

        let neg_expr = Expr::Neg(Box::new(Expr::column(1)));
        assert_eq!(
            evaluate(&neg_expr, &row).unwrap(),
            ScalarValue::Float64(Some(-2.5))
        );

        let eq_expr = Expr::Eq(
            Box::new(Expr::column(0)),
            Box::new(Expr::literal(ScalarValue::Float64(Some(4.0)))),
        );
        assert!(evaluate_bool(&eq_expr, &row).unwrap());

        let in_list_expr = Expr::InList {
            expr: Box::new(Expr::column(0)),
            list: vec![Expr::literal(ScalarValue::Float64(Some(4.0)))],
            negated: false,
        };
        assert!(evaluate_bool(&in_list_expr, &row).unwrap());

        let gt_expr = Expr::Gt(
            Box::new(Expr::column(1)),
            Box::new(Expr::literal(ScalarValue::Int64(Some(2)))),
        );
        assert!(evaluate_bool(&gt_expr, &row).unwrap());
    }
}
