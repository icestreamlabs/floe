use anyhow::{Result, anyhow, bail};
use datafusion::common::Column;
use datafusion::logical_expr::{Case, Expr as DfExpr, Operator};
use datafusion::scalar::ScalarValue;
use dbsp::circuit::plan::{DbspJoinKey, DbspProjectExpr};
use dbsp::{DbspExpression, DbspPredicate, RowSchema};

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

pub(super) fn resolve_join_key_indices(
    keys: &[DbspJoinKey],
    left_schema: &RowSchema,
    right_schema: &RowSchema,
) -> Result<Vec<(usize, usize)>> {
    let mut indices = Vec::with_capacity(keys.len());
    for key in keys {
        let left_name = match key.left_expression().expr() {
            DfExpr::Column(column) => column.name.clone(),
            other => {
                bail!("join key expression must be a column on the left, found {other:?}");
            }
        };
        let right_name = match key.right_expression().expr() {
            DfExpr::Column(column) => column.name.clone(),
            other => {
                bail!("join key expression must be a column on the right, found {other:?}");
            }
        };
        let left_idx = left_schema
            .field_index(&left_name)
            .ok_or_else(|| anyhow!("left join key column '{left_name}' must exist"))?;
        let right_idx = right_schema
            .field_index(&right_name)
            .ok_or_else(|| anyhow!("right join key column '{right_name}' must exist"))?;
        indices.push((left_idx, right_idx));
    }
    Ok(indices)
}

pub(super) fn eval_expression(
    expr: &DbspExpression,
    row: &[ScalarValue],
    schema: &RowSchema,
) -> Result<bool> {
    let value = eval_df_expr(expr.expr(), row, schema)?;
    scalar_to_bool(&value)
}

fn eval_df_expr(expr: &DfExpr, row: &[ScalarValue], schema: &RowSchema) -> Result<ScalarValue> {
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

fn eval_case(case: &Case, row: &[ScalarValue], schema: &RowSchema) -> Result<ScalarValue> {
    if let Some(base) = case.expr.as_ref() {
        let base_value = eval_df_expr(base, row, schema)?;
        for (when, then) in &case.when_then_expr {
            let when_value = eval_df_expr(when, row, schema)?;
            if scalar_equals(&when_value, &base_value)?.unwrap_or(false) {
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
        Operator::Eq => Ok(ScalarValue::Boolean(scalar_equals(&left, &right)?)),
        Operator::NotEq => {
            let result = scalar_equals(&left, &right)?.map(|value| !value);
            Ok(ScalarValue::Boolean(result))
        }
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
            let ordering = scalar_compare(&left, &right, op)?;
            Ok(ScalarValue::Boolean(ordering))
        }
        Operator::And => {
            let lhs = scalar_to_bool_opt(&left)?;
            let rhs = scalar_to_bool_opt(&right)?;
            let result = match (lhs, rhs) {
                (Some(false), _) => Some(false),
                (Some(true), other) => other,
                (None, Some(false)) => Some(false),
                (None, Some(true)) => None,
                (None, None) => None,
            };
            Ok(ScalarValue::Boolean(result))
        }
        Operator::Or => {
            let lhs = scalar_to_bool_opt(&left)?;
            let rhs = scalar_to_bool_opt(&right)?;
            let result = match (lhs, rhs) {
                (Some(true), _) => Some(true),
                (Some(false), other) => other,
                (None, Some(true)) => Some(true),
                (None, Some(false)) => None,
                (None, None) => None,
            };
            Ok(ScalarValue::Boolean(result))
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

fn scalar_compare(lhs: &ScalarValue, rhs: &ScalarValue, op: Operator) -> Result<Option<bool>> {
    if lhs.is_null() || rhs.is_null() {
        return Ok(None);
    }
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
    Ok(Some(result))
}

// SQL comparisons involving NULL yield NULL (unknown).
fn scalar_equals(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<Option<bool>> {
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
fn scalar_to_bool(value: &ScalarValue) -> Result<bool> {
    Ok(scalar_to_bool_opt(value)?.unwrap_or(false))
}

fn scalar_to_bool_opt(value: &ScalarValue) -> Result<Option<bool>> {
    match value {
        ScalarValue::Boolean(Some(v)) => Ok(Some(*v)),
        ScalarValue::Boolean(None) | ScalarValue::Null => Ok(None),
        other => bail!("expected boolean value, found {other:?}"),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion::common::Column;
    use datafusion::logical_expr::Expr as DfExpr;
    use dbsp::circuit::plan::DbspJoinType;
    use dbsp::circuit::schema::Field;
    use dbsp::circuit::types::DbspScalarType;
    use dbsp::DbspJoinNode;

    fn schema() -> Arc<RowSchema> {
        RowSchema::try_new(vec![Field::new("id", DbspScalarType::Int64, true)]).expect("schema")
    }

    #[test]
    fn join_key_requires_column_expressions() {
        let schema = schema();
        let left_expr = DfExpr::Literal(ScalarValue::Int64(Some(1)), None);
        let right_expr = DfExpr::Column(Column::new_unqualified("id".to_string()));
        let node = DbspJoinNode::try_new(
            DbspJoinType::Inner,
            Arc::clone(&schema),
            Arc::clone(&schema),
            vec![(left_expr, right_expr)],
            None,
        )
        .expect("join node");

        let err =
            resolve_join_key_indices(&node.keys, schema.as_ref(), schema.as_ref()).unwrap_err();
        assert!(err.to_string().contains("left"));
    }

    #[test]
    fn join_key_rejects_non_column_on_right() {
        let schema = schema();
        let left_expr = DfExpr::Column(Column::new_unqualified("id".to_string()));
        let right_expr = DfExpr::Literal(ScalarValue::Int64(Some(1)), None);
        let node = DbspJoinNode::try_new(
            DbspJoinType::Inner,
            Arc::clone(&schema),
            Arc::clone(&schema),
            vec![(left_expr, right_expr)],
            None,
        )
        .expect("join node");

        let err =
            resolve_join_key_indices(&node.keys, schema.as_ref(), schema.as_ref()).unwrap_err();
        assert!(err.to_string().contains("right"));
    }
}
