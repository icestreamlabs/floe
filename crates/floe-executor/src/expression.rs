use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use datafusion::common::Column;
use datafusion::logical_expr::expr::Case;
use datafusion::logical_expr::{Expr as DfExpr, Operator};
use datafusion::scalar::ScalarValue;
use dbsp::circuit::{DbspExpression, RowSchema};

use crate::stream_types::Row;

/// Evaluates a DBSP expression against a decoded row.
#[derive(Clone)]
pub struct ExpressionEvaluator {
    schema: Arc<RowSchema>,
    expr: DfExpr,
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

pub fn scalar_equals(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<bool> {
    let result = match (lhs, rhs) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l == r,
        (
            ScalarValue::TimestampMillisecond(Some(l), _),
            ScalarValue::TimestampMillisecond(Some(r), _),
        ) => l == r,
        (ScalarValue::Utf8(Some(l)), ScalarValue::Utf8(Some(r))) => l == r,
        (ScalarValue::Boolean(Some(l)), ScalarValue::Boolean(Some(r))) => l == r,
        (ScalarValue::Null, ScalarValue::Null) => true,
        _ => false,
    };
    Ok(result)
}

pub fn scalar_to_bool(value: &ScalarValue) -> Result<bool> {
    match value {
        ScalarValue::Boolean(Some(v)) => Ok(*v),
        ScalarValue::Boolean(None) | ScalarValue::Null => Ok(false),
        other => bail!("expected boolean value, found {other:?}"),
    }
}

fn eval_df_expr(expr: &DfExpr, row: &Row, schema: &RowSchema) -> Result<ScalarValue> {
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
            Ok(ScalarValue::Boolean(Some(!scalar_to_bool(&value)?)))
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
            Ok(ScalarValue::Boolean(Some(scalar_to_bool(&value)?)))
        }
        DfExpr::IsNotTrue(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            Ok(ScalarValue::Boolean(Some(!scalar_to_bool(&value)?)))
        }
        DfExpr::IsFalse(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            Ok(ScalarValue::Boolean(Some(!scalar_to_bool(&value)?)))
        }
        DfExpr::IsNotFalse(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            Ok(ScalarValue::Boolean(Some(scalar_to_bool(&value)?)))
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
            match &cast.data_type {
                datafusion::arrow::datatypes::DataType::Timestamp(_, _) => {
                    let number = scalar_to_i64(&value, "cast to timestamp")?;
                    Ok(ScalarValue::TimestampMillisecond(Some(number), None))
                }
                datafusion::arrow::datatypes::DataType::Int64 => {
                    let number = scalar_to_i64(&value, "cast to int64")?;
                    Ok(ScalarValue::Int64(Some(number)))
                }
                other => bail!("unsupported cast target {other:?}"),
            }
        }
        DfExpr::Case(case) => eval_case(case, row, schema),
        other => bail!("unsupported expression: {other:?}"),
    }
}

fn eval_case(case: &Case, row: &Row, schema: &RowSchema) -> Result<ScalarValue> {
    if let Some(base) = case.expr.as_ref() {
        let base_value = eval_df_expr(base, row, schema)?;
        for (when, then) in &case.when_then_expr {
            let when_value = eval_df_expr(when, row, schema)?;
            if scalar_equals(&when_value, &base_value)? {
                return eval_df_expr(then, row, schema);
            }
        }
    } else {
        for (when, then) in &case.when_then_expr {
            let when_value = eval_df_expr(when, row, schema)?;
            if scalar_to_bool(&when_value)? {
                return eval_df_expr(then, row, schema);
            }
        }
    }

    if let Some(else_expr) = case.else_expr.as_ref() {
        eval_df_expr(else_expr, row, schema)
    } else {
        Ok(ScalarValue::Null)
    }
}

fn eval_binary(op: Operator, left: ScalarValue, right: ScalarValue) -> Result<ScalarValue> {
    match op {
        Operator::Eq => Ok(ScalarValue::Boolean(Some(scalar_equals(&left, &right)?))),
        Operator::NotEq => Ok(ScalarValue::Boolean(Some(!scalar_equals(&left, &right)?))),
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
            let ordering = scalar_compare(&left, &right, op)?;
            Ok(ScalarValue::Boolean(Some(ordering)))
        }
        Operator::And => {
            let lhs = scalar_to_bool(&left)?;
            if !lhs {
                return Ok(ScalarValue::Boolean(Some(false)));
            }
            let rhs = scalar_to_bool(&right)?;
            Ok(ScalarValue::Boolean(Some(lhs && rhs)))
        }
        Operator::Or => {
            let lhs = scalar_to_bool(&left)?;
            if lhs {
                return Ok(ScalarValue::Boolean(Some(true)));
            }
            let rhs = scalar_to_bool(&right)?;
            Ok(ScalarValue::Boolean(Some(lhs || rhs)))
        }
        Operator::Plus
        | Operator::Minus
        | Operator::Multiply
        | Operator::Divide
        | Operator::Modulo => {
            let lhs = scalar_to_i64(&left, "arithmetic")?;
            let rhs = scalar_to_i64(&right, "arithmetic")?;
            let value = match op {
                Operator::Plus => lhs + rhs,
                Operator::Minus => lhs - rhs,
                Operator::Multiply => lhs * rhs,
                Operator::Divide => lhs / rhs,
                Operator::Modulo => lhs % rhs,
                _ => unreachable!(),
            };
            Ok(ScalarValue::Int64(Some(value)))
        }
        Operator::StringConcat => {
            let lhs = match left {
                ScalarValue::Utf8(Some(value)) => value,
                _ => bail!("string concat expects utf8 operands"),
            };
            let rhs = match right {
                ScalarValue::Utf8(Some(value)) => value,
                _ => bail!("string concat expects utf8 operands"),
            };
            Ok(ScalarValue::Utf8(Some(lhs + &rhs)))
        }
        _ => bail!("unsupported binary operator {op:?}"),
    }
}

fn scalar_compare(lhs: &ScalarValue, rhs: &ScalarValue, op: Operator) -> Result<bool> {
    let ordering = match (lhs, rhs) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l.cmp(r),
        (
            ScalarValue::TimestampMillisecond(Some(l), _),
            ScalarValue::TimestampMillisecond(Some(r), _),
        ) => l.cmp(r),
        (ScalarValue::Utf8(Some(l)), ScalarValue::Utf8(Some(r))) => l.cmp(r),
        _ => bail!("unsupported comparison operands: {lhs:?} vs {rhs:?}"),
    };
    let result = match op {
        Operator::Lt => ordering.is_lt(),
        Operator::LtEq => ordering.is_le(),
        Operator::Gt => ordering.is_gt(),
        Operator::GtEq => ordering.is_ge(),
        _ => unreachable!(),
    };
    Ok(result)
}

fn scalar_to_i64(value: &ScalarValue, context: &str) -> Result<i64> {
    match value {
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::TimestampMillisecond(Some(v), _) => Ok(*v),
        other => bail!("{context} expects Int64, found {other:?}"),
    }
}

fn matches_like(value: &str, pattern: &str) -> bool {
    if !pattern.contains('%') {
        return value == pattern;
    }
    if let Some(stripped) = pattern.strip_prefix('%') {
        return value.ends_with(stripped);
    }
    if let Some(stripped) = pattern.strip_suffix('%') {
        return value.starts_with(stripped);
    }
    false
}

fn resolve_column(schema: &RowSchema, column: &Column) -> Result<usize> {
    let qualified = column.flat_name();
    if let Some(idx) = schema.field_index(&qualified) {
        return Ok(idx);
    }
    schema
        .field_index(&column.name)
        .ok_or_else(|| anyhow!("column {} not found in schema", column.name))
}
