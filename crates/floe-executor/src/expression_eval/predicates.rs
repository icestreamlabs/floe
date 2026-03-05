use super::*;

pub(super) fn scalar_to_bool_opt(value: &ScalarValue) -> Result<Option<bool>> {
    match value {
        ScalarValue::Boolean(Some(v)) => Ok(Some(*v)),
        ScalarValue::Boolean(None) | ScalarValue::Null => Ok(None),
        other => bail!("expected boolean value, found {other:?}"),
    }
}

pub(super) fn eval_case(
    case: &Case,
    row: &[ScalarValue],
    schema: &RowSchema,
) -> Result<ScalarValue> {
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

pub(super) fn eval_binary(
    op: Operator,
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue> {
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
                other => bail!("unsupported arithmetic operator {other:?}"),
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

pub(super) fn eval_between(
    between: &datafusion::logical_expr::Between,
    row: &[ScalarValue],
    schema: &RowSchema,
) -> Result<ScalarValue> {
    let value = eval_df_expr(between.expr.as_ref(), row, schema)?;
    let low = eval_df_expr(between.low.as_ref(), row, schema)?;
    let high = eval_df_expr(between.high.as_ref(), row, schema)?;

    let lower = scalar_compare(&value, &low, Operator::GtEq)?;
    let upper = scalar_compare(&value, &high, Operator::LtEq)?;
    let combined = and_bool_opt(lower, upper);
    let result = match (combined, between.negated) {
        (Some(value), true) => Some(!value),
        (Some(value), false) => Some(value),
        (None, _) => None,
    };
    Ok(ScalarValue::Boolean(result))
}

pub(super) fn eval_in_list(
    in_list: &InList,
    row: &[ScalarValue],
    schema: &RowSchema,
) -> Result<ScalarValue> {
    let value = eval_df_expr(in_list.expr.as_ref(), row, schema)?;
    if value.is_null() {
        return Ok(ScalarValue::Boolean(None));
    }

    let mut saw_null = false;
    for expr in &in_list.list {
        let item = eval_df_expr(expr, row, schema)?;
        match scalar_equals(&value, &item)? {
            Some(true) => {
                let result = if in_list.negated {
                    Some(false)
                } else {
                    Some(true)
                };
                return Ok(ScalarValue::Boolean(result));
            }
            Some(false) => {}
            None => saw_null = true,
        }
    }

    let result = if saw_null { None } else { Some(false) };
    let result = if in_list.negated {
        result.map(|value| !value)
    } else {
        result
    };
    Ok(ScalarValue::Boolean(result))
}

pub(super) fn scalar_compare(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
    op: Operator,
) -> Result<Option<bool>> {
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
        other => bail!("unsupported comparison operator {other:?}"),
    };
    Ok(Some(result))
}

pub(super) fn scalar_to_i64(value: &ScalarValue, context: &str) -> Result<i64> {
    match value {
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::TimestampMillisecond(Some(v), _) => Ok(*v),
        other => bail!("{context} expects Int64, found {other:?}"),
    }
}

pub(super) fn and_bool_opt(lhs: Option<bool>, rhs: Option<bool>) -> Option<bool> {
    match (lhs, rhs) {
        (Some(false), _) => Some(false),
        (Some(true), other) => other,
        (None, Some(false)) => Some(false),
        (None, Some(true)) => None,
        (None, None) => None,
    }
}

pub(super) fn matches_like(value: &str, pattern: &str) -> bool {
    let value_chars = value.chars().collect::<Vec<_>>();
    let pattern_chars = pattern.chars().collect::<Vec<_>>();
    let m = value_chars.len();
    let n = pattern_chars.len();

    let mut dp = vec![vec![false; n + 1]; m + 1];
    dp[0][0] = true;
    for j in 1..=n {
        if pattern_chars[j - 1] == '%' {
            dp[0][j] = dp[0][j - 1];
        }
    }

    for i in 1..=m {
        for j in 1..=n {
            match pattern_chars[j - 1] {
                '%' => {
                    // '%' matches zero characters (dp[i][j-1]) or one more character (dp[i-1][j]).
                    dp[i][j] = dp[i][j - 1] || dp[i - 1][j];
                }
                '_' => {
                    // '_' matches exactly one character.
                    dp[i][j] = dp[i - 1][j - 1];
                }
                literal => {
                    dp[i][j] = dp[i - 1][j - 1] && value_chars[i - 1] == literal;
                }
            }
        }
    }

    dp[m][n]
}

pub(super) fn resolve_column(schema: &RowSchema, column: &Column) -> Result<usize> {
    let qualified = column.flat_name();
    if let Some(idx) = schema.field_index(&qualified) {
        return Ok(idx);
    }
    schema
        .field_index(&column.name)
        .ok_or_else(|| anyhow!("column {} not found in schema", column.name))
}
