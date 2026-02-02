use anyhow::{Result, anyhow, bail};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::Column;
use datafusion::logical_expr::expr::Case;
use datafusion::logical_expr::expr::{InList, ScalarFunction};
use datafusion::logical_expr::{Expr as DfExpr, Operator};
use datafusion::scalar::ScalarValue;
use dbsp::circuit::RowSchema;

pub(crate) fn eval_df_expr(
    expr: &DfExpr,
    row: &[ScalarValue],
    schema: &RowSchema,
) -> Result<ScalarValue> {
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
        DfExpr::IsUnknown(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            Ok(ScalarValue::Boolean(Some(value.is_null())))
        }
        DfExpr::IsNotUnknown(inner) => {
            let value = eval_df_expr(inner, row, schema)?;
            Ok(ScalarValue::Boolean(Some(!value.is_null())))
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
            cast_value(&value, &cast.data_type)
        }
        DfExpr::TryCast(cast) => {
            let value = eval_df_expr(cast.expr.as_ref(), row, schema)?;
            Ok(try_cast_value(&value, &cast.data_type))
        }
        DfExpr::Case(case) => eval_case(case, row, schema),
        DfExpr::Between(between) => eval_between(between, row, schema),
        DfExpr::InList(in_list) => eval_in_list(in_list, row, schema),
        DfExpr::ScalarFunction(func) => eval_scalar_function(func, row, schema),
        other => bail!("unsupported expression: {other:?}"),
    }
}

// SQL comparisons involving NULL yield NULL (unknown).
pub(crate) fn scalar_equals(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<Option<bool>> {
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
pub(crate) fn scalar_to_bool(value: &ScalarValue) -> Result<bool> {
    Ok(scalar_to_bool_opt(value)?.unwrap_or(false))
}

fn scalar_to_bool_opt(value: &ScalarValue) -> Result<Option<bool>> {
    match value {
        ScalarValue::Boolean(Some(v)) => Ok(Some(*v)),
        ScalarValue::Boolean(None) | ScalarValue::Null => Ok(None),
        other => bail!("expected boolean value, found {other:?}"),
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

fn eval_between(
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

fn eval_in_list(in_list: &InList, row: &[ScalarValue], schema: &RowSchema) -> Result<ScalarValue> {
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

fn eval_scalar_function(
    func: &ScalarFunction,
    row: &[ScalarValue],
    schema: &RowSchema,
) -> Result<ScalarValue> {
    let args = func
        .args
        .iter()
        .map(|arg| eval_df_expr(arg, row, schema))
        .collect::<Result<Vec<_>>>()?;
    let name = func.name().to_ascii_lowercase();
    match name.as_str() {
        "lower" => unary_string_func("lower", &args, |value| value.to_lowercase()),
        "upper" => unary_string_func("upper", &args, |value| value.to_uppercase()),
        "length" | "char_length" | "character_length" => unary_string_len(&args),
        "abs" => unary_int_func("abs", &args, |value| {
            value
                .checked_abs()
                .ok_or_else(|| anyhow!("abs overflow for {value}"))
        }),
        "coalesce" => eval_coalesce(&args),
        "nullif" => eval_nullif(&args),
        "concat" => eval_concat(&args),
        _ => bail!("unsupported scalar function '{}'", func.name()),
    }
}

fn unary_string_func(
    name: &str,
    args: &[ScalarValue],
    f: impl Fn(String) -> String,
) -> Result<ScalarValue> {
    if args.len() != 1 {
        bail!("{name} expects 1 argument, found {}", args.len());
    }
    match &args[0] {
        ScalarValue::Utf8(Some(value)) => Ok(ScalarValue::Utf8(Some(f(value.clone())))),
        ScalarValue::Utf8(None) | ScalarValue::Null => Ok(ScalarValue::Utf8(None)),
        other => bail!("{name} expects utf8 input, found {other:?}"),
    }
}

fn unary_string_len(args: &[ScalarValue]) -> Result<ScalarValue> {
    if args.len() != 1 {
        bail!("length expects 1 argument, found {}", args.len());
    }
    match &args[0] {
        ScalarValue::Utf8(Some(value)) => {
            Ok(ScalarValue::Int64(Some(value.chars().count() as i64)))
        }
        ScalarValue::Utf8(None) | ScalarValue::Null => Ok(ScalarValue::Int64(None)),
        other => bail!("length expects utf8 input, found {other:?}"),
    }
}

fn unary_int_func(
    name: &str,
    args: &[ScalarValue],
    f: impl Fn(i64) -> Result<i64>,
) -> Result<ScalarValue> {
    if args.len() != 1 {
        bail!("{name} expects 1 argument, found {}", args.len());
    }
    match &args[0] {
        ScalarValue::Int64(Some(value)) => Ok(ScalarValue::Int64(Some(f(*value)?))),
        ScalarValue::Int64(None) | ScalarValue::Null => Ok(ScalarValue::Int64(None)),
        other => bail!("{name} expects int64 input, found {other:?}"),
    }
}

fn eval_coalesce(args: &[ScalarValue]) -> Result<ScalarValue> {
    if args.is_empty() {
        bail!("coalesce expects at least 1 argument");
    }
    for value in args {
        if !value.is_null() {
            return Ok(value.clone());
        }
    }
    Ok(null_like(&args[0]))
}

fn eval_nullif(args: &[ScalarValue]) -> Result<ScalarValue> {
    if args.len() != 2 {
        bail!("nullif expects 2 arguments, found {}", args.len());
    }
    let left = &args[0];
    let right = &args[1];
    match scalar_equals(left, right)? {
        Some(true) => Ok(null_like(left)),
        Some(false) => Ok(left.clone()),
        None => Ok(left.clone()),
    }
}

fn eval_concat(args: &[ScalarValue]) -> Result<ScalarValue> {
    if args.is_empty() {
        bail!("concat expects at least 1 argument");
    }
    let mut out = String::new();
    for arg in args {
        match arg {
            ScalarValue::Utf8(Some(value)) => out.push_str(value),
            ScalarValue::Utf8(None) | ScalarValue::Null => return Ok(ScalarValue::Utf8(None)),
            other => bail!("concat expects utf8 input, found {other:?}"),
        }
    }
    Ok(ScalarValue::Utf8(Some(out)))
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
        other => bail!("unsupported comparison operator {other:?}"),
    };
    Ok(Some(result))
}

fn scalar_to_i64(value: &ScalarValue, context: &str) -> Result<i64> {
    match value {
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::TimestampMillisecond(Some(v), _) => Ok(*v),
        other => bail!("{context} expects Int64, found {other:?}"),
    }
}

fn cast_value(value: &ScalarValue, data_type: &DataType) -> Result<ScalarValue> {
    match data_type {
        DataType::Timestamp(TimeUnit::Millisecond, None) => match value {
            ScalarValue::TimestampMillisecond(_, _) => Ok(value.clone()),
            ScalarValue::Int64(Some(_)) => {
                let number = scalar_to_i64(value, "cast to timestamp")?;
                Ok(ScalarValue::TimestampMillisecond(Some(number), None))
            }
            ScalarValue::Int64(None) | ScalarValue::Null => {
                Ok(ScalarValue::TimestampMillisecond(None, None))
            }
            ScalarValue::Utf8(Some(text)) => {
                let parsed = text
                    .parse::<i64>()
                    .map_err(|_| anyhow!("failed to cast utf8 to timestamp"))?;
                Ok(ScalarValue::TimestampMillisecond(Some(parsed), None))
            }
            ScalarValue::Utf8(None) => Ok(ScalarValue::TimestampMillisecond(None, None)),
            other => bail!("unsupported cast to timestamp from {other:?}"),
        },
        DataType::Int64 => match value {
            ScalarValue::Int64(_) => Ok(value.clone()),
            ScalarValue::TimestampMillisecond(Some(val), _) => Ok(ScalarValue::Int64(Some(*val))),
            ScalarValue::TimestampMillisecond(None, _) => Ok(ScalarValue::Int64(None)),
            ScalarValue::Utf8(Some(text)) => {
                let parsed = text
                    .parse::<i64>()
                    .map_err(|_| anyhow!("failed to cast utf8 to int64"))?;
                Ok(ScalarValue::Int64(Some(parsed)))
            }
            ScalarValue::Utf8(None) | ScalarValue::Null => Ok(ScalarValue::Int64(None)),
            other => bail!("unsupported cast to int64 from {other:?}"),
        },
        DataType::Utf8 => match value {
            ScalarValue::Utf8(_) => Ok(value.clone()),
            ScalarValue::Int64(Some(val)) => Ok(ScalarValue::Utf8(Some(val.to_string()))),
            ScalarValue::TimestampMillisecond(Some(val), _) => {
                Ok(ScalarValue::Utf8(Some(val.to_string())))
            }
            ScalarValue::Int64(None)
            | ScalarValue::TimestampMillisecond(None, _)
            | ScalarValue::Null => Ok(ScalarValue::Utf8(None)),
            other => bail!("unsupported cast to utf8 from {other:?}"),
        },
        DataType::Boolean => match value {
            ScalarValue::Boolean(_) => Ok(value.clone()),
            ScalarValue::Null => Ok(ScalarValue::Boolean(None)),
            other => bail!("unsupported cast to boolean from {other:?}"),
        },
        other => bail!("unsupported cast target {other:?}"),
    }
}

fn try_cast_value(value: &ScalarValue, data_type: &DataType) -> ScalarValue {
    match cast_value(value, data_type) {
        Ok(value) => value,
        Err(_) => null_for_type(data_type),
    }
}

fn null_for_type(data_type: &DataType) -> ScalarValue {
    match data_type {
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            ScalarValue::TimestampMillisecond(None, None)
        }
        DataType::Int64 => ScalarValue::Int64(None),
        DataType::Utf8 => ScalarValue::Utf8(None),
        DataType::Boolean => ScalarValue::Boolean(None),
        _ => ScalarValue::Null,
    }
}

fn null_like(value: &ScalarValue) -> ScalarValue {
    match value {
        ScalarValue::Int64(_) => ScalarValue::Int64(None),
        ScalarValue::Utf8(_) => ScalarValue::Utf8(None),
        ScalarValue::TimestampMillisecond(_, tz) => {
            ScalarValue::TimestampMillisecond(None, tz.clone())
        }
        ScalarValue::Boolean(_) => ScalarValue::Boolean(None),
        ScalarValue::Null => ScalarValue::Null,
        other => other.clone(),
    }
}

fn and_bool_opt(lhs: Option<bool>, rhs: Option<bool>) -> Option<bool> {
    match (lhs, rhs) {
        (Some(false), _) => Some(false),
        (Some(true), other) => other,
        (None, Some(false)) => Some(false),
        (None, Some(true)) => None,
        (None, None) => None,
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
    use datafusion::common::Column;
    use datafusion::functions::expr_fn;
    use datafusion::logical_expr::expr::InList;
    use datafusion::logical_expr::{Between, Expr as DfExpr, Operator, TryCast};
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

    fn eval(expr: DfExpr, schema: Arc<RowSchema>, row: Vec<ScalarValue>) -> ScalarValue {
        eval_df_expr(&expr, &row, schema.as_ref()).expect("eval")
    }

    #[test]
    fn in_list_null_semantics() {
        let schema = schema(vec![("a", DbspScalarType::Int64)]);
        let in_list = DfExpr::InList(InList::new(
            Box::new(col("a")),
            vec![DfExpr::Literal(ScalarValue::Int64(Some(1)), None)],
            false,
        ));
        let value = eval(
            in_list.clone(),
            Arc::clone(&schema),
            vec![ScalarValue::Int64(Some(1))],
        );
        assert_eq!(value, ScalarValue::Boolean(Some(true)));

        let value = eval(
            in_list.clone(),
            Arc::clone(&schema),
            vec![ScalarValue::Int64(Some(2))],
        );
        assert_eq!(value, ScalarValue::Boolean(Some(false)));

        let in_list_null = DfExpr::InList(InList::new(
            Box::new(col("a")),
            vec![
                DfExpr::Literal(ScalarValue::Int64(Some(1)), None),
                DfExpr::Literal(ScalarValue::Int64(None), None),
            ],
            false,
        ));
        let value = eval(
            in_list_null,
            Arc::clone(&schema),
            vec![ScalarValue::Int64(Some(2))],
        );
        assert_eq!(value, ScalarValue::Boolean(None));

        let value = eval(in_list, Arc::clone(&schema), vec![ScalarValue::Int64(None)]);
        assert_eq!(value, ScalarValue::Boolean(None));
    }

    #[test]
    fn between_null_semantics() {
        let schema = schema(vec![("a", DbspScalarType::Int64)]);
        let between = DfExpr::Between(Between::new(
            Box::new(col("a")),
            false,
            Box::new(DfExpr::Literal(ScalarValue::Int64(Some(1)), None)),
            Box::new(DfExpr::Literal(ScalarValue::Int64(Some(3)), None)),
        ));
        let value = eval(
            between.clone(),
            Arc::clone(&schema),
            vec![ScalarValue::Int64(Some(2))],
        );
        assert_eq!(value, ScalarValue::Boolean(Some(true)));

        let value = eval(
            between.clone(),
            Arc::clone(&schema),
            vec![ScalarValue::Int64(Some(5))],
        );
        assert_eq!(value, ScalarValue::Boolean(Some(false)));

        let value = eval(between, Arc::clone(&schema), vec![ScalarValue::Int64(None)]);
        assert_eq!(value, ScalarValue::Boolean(None));
    }

    #[test]
    fn try_cast_returns_null_on_failure() {
        let schema = schema(vec![("a", DbspScalarType::Utf8)]);
        let expr = DfExpr::TryCast(TryCast::new(Box::new(col("a")), DataType::Int64));
        let value = eval(
            expr,
            Arc::clone(&schema),
            vec![ScalarValue::Utf8(Some("not-a-number".to_string()))],
        );
        assert_eq!(value, ScalarValue::Int64(None));
    }

    #[test]
    fn scalar_functions_execute() {
        let schema = schema(vec![("a", DbspScalarType::Utf8)]);
        let lower_expr = expr_fn::lower(col("a"));
        let value = eval(
            lower_expr,
            Arc::clone(&schema),
            vec![ScalarValue::Utf8(Some("HeLLo".to_string()))],
        );
        assert_eq!(value, ScalarValue::Utf8(Some("hello".to_string())));

        let coalesce_expr = expr_fn::coalesce(vec![
            DfExpr::Literal(ScalarValue::Utf8(None), None),
            col("a"),
        ]);
        let value = eval(
            coalesce_expr,
            Arc::clone(&schema),
            vec![ScalarValue::Utf8(Some("ok".to_string()))],
        );
        assert_eq!(value, ScalarValue::Utf8(Some("ok".to_string())));

        let length_expr = expr_fn::length(col("a"));
        let value = eval(
            length_expr,
            Arc::clone(&schema),
            vec![ScalarValue::Utf8(Some("hi".to_string()))],
        );
        assert_eq!(value, ScalarValue::Int64(Some(2)));
    }

    #[test]
    fn predicate_truth_table_matches_sql_nulls() {
        let schema = schema(vec![
            ("a", DbspScalarType::Bool),
            ("b", DbspScalarType::Bool),
        ]);
        let and_expr = DfExpr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(col("a")),
            Operator::And,
            Box::new(col("b")),
        ));
        let or_expr = DfExpr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(col("a")),
            Operator::Or,
            Box::new(col("b")),
        ));

        let cases = vec![
            (Some(true), Some(true), Some(true), Some(true)),
            (Some(true), Some(false), Some(false), Some(true)),
            (Some(false), Some(true), Some(false), Some(true)),
            (Some(false), Some(false), Some(false), Some(false)),
            (Some(true), None, None, Some(true)),
            (Some(false), None, Some(false), None),
            (None, Some(true), None, Some(true)),
            (None, Some(false), Some(false), None),
            (None, None, None, None),
        ];

        for (left, right, expected_and, expected_or) in cases {
            let row = vec![ScalarValue::Boolean(left), ScalarValue::Boolean(right)];
            let and_val = eval(and_expr.clone(), Arc::clone(&schema), row.clone());
            let or_val = eval(or_expr.clone(), Arc::clone(&schema), row);
            assert_eq!(and_val, ScalarValue::Boolean(expected_and));
            assert_eq!(or_val, ScalarValue::Boolean(expected_or));
        }
    }
}
