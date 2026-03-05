use super::*;

pub(super) fn eval_scalar_function(
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
        "hour" => eval_hour(&args),
        "date_format" => eval_date_format(&args),
        "regexp_extract" => eval_regexp_extract(&args),
        "split_index" => eval_split_index(&args),
        "count_char" => eval_count_char(&args),
        "proctime" => eval_proctime(&args),
        _ => bail!("unsupported scalar function '{}'", func.name()),
    }
}

pub(super) fn unary_string_func(
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

pub(super) fn unary_string_len(args: &[ScalarValue]) -> Result<ScalarValue> {
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

pub(super) fn unary_int_func(
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

pub(super) fn eval_coalesce(args: &[ScalarValue]) -> Result<ScalarValue> {
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

pub(super) fn eval_nullif(args: &[ScalarValue]) -> Result<ScalarValue> {
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

pub(super) fn eval_concat(args: &[ScalarValue]) -> Result<ScalarValue> {
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

pub(super) fn eval_hour(args: &[ScalarValue]) -> Result<ScalarValue> {
    if args.len() != 1 {
        bail!("hour expects 1 argument, found {}", args.len());
    }
    match &args[0] {
        ScalarValue::TimestampMillisecond(Some(value), _) => {
            let Some(ts) = Utc.timestamp_millis_opt(*value).single() else {
                return Ok(ScalarValue::Int64(None));
            };
            Ok(ScalarValue::Int64(Some(ts.hour() as i64)))
        }
        ScalarValue::TimestampMillisecond(None, _) | ScalarValue::Null => {
            Ok(ScalarValue::Int64(None))
        }
        other => bail!("hour expects timestamp input, found {other:?}"),
    }
}

pub(super) fn eval_date_format(args: &[ScalarValue]) -> Result<ScalarValue> {
    if args.len() != 2 {
        bail!("date_format expects 2 arguments, found {}", args.len());
    }
    let timestamp = match &args[0] {
        ScalarValue::TimestampMillisecond(Some(value), _) => *value,
        ScalarValue::TimestampMillisecond(None, _) | ScalarValue::Null => {
            return Ok(ScalarValue::Utf8(None));
        }
        other => bail!("date_format expects timestamp input, found {other:?}"),
    };
    let pattern = match &args[1] {
        ScalarValue::Utf8(Some(value)) => value,
        ScalarValue::Utf8(None) | ScalarValue::Null => return Ok(ScalarValue::Utf8(None)),
        other => bail!("date_format expects Utf8 pattern, found {other:?}"),
    };
    let Some(ts) = Utc.timestamp_millis_opt(timestamp).single() else {
        return Ok(ScalarValue::Utf8(None));
    };
    let chrono_pattern = normalize_date_format_pattern(pattern);
    Ok(ScalarValue::Utf8(Some(
        ts.format(&chrono_pattern).to_string(),
    )))
}

pub(super) fn eval_regexp_extract(args: &[ScalarValue]) -> Result<ScalarValue> {
    if args.len() != 3 {
        bail!("regexp_extract expects 3 arguments, found {}", args.len());
    }
    let text = match &args[0] {
        ScalarValue::Utf8(Some(value)) => value,
        ScalarValue::Utf8(None) | ScalarValue::Null => return Ok(ScalarValue::Utf8(None)),
        other => bail!("regexp_extract expects Utf8 text input, found {other:?}"),
    };
    let pattern = match &args[1] {
        ScalarValue::Utf8(Some(value)) => value,
        ScalarValue::Utf8(None) | ScalarValue::Null => return Ok(ScalarValue::Utf8(None)),
        other => bail!("regexp_extract expects Utf8 pattern input, found {other:?}"),
    };
    let group = match &args[2] {
        ScalarValue::Int64(Some(value)) => *value,
        ScalarValue::Int64(None) | ScalarValue::Null => return Ok(ScalarValue::Utf8(None)),
        other => bail!("regexp_extract expects Int64 group index, found {other:?}"),
    };
    if group < 0 {
        return Ok(ScalarValue::Utf8(None));
    }
    let regex = match Regex::new(pattern) {
        Ok(regex) => regex,
        Err(_) => return Ok(ScalarValue::Utf8(None)),
    };
    let extracted = regex
        .captures(text)
        .and_then(|caps| caps.get(group as usize))
        .map(|m| m.as_str().to_string());
    Ok(ScalarValue::Utf8(extracted))
}

pub(super) fn eval_split_index(args: &[ScalarValue]) -> Result<ScalarValue> {
    if args.len() != 3 {
        bail!("split_index expects 3 arguments, found {}", args.len());
    }
    let text = match &args[0] {
        ScalarValue::Utf8(Some(value)) => value,
        ScalarValue::Utf8(None) | ScalarValue::Null => return Ok(ScalarValue::Utf8(None)),
        other => bail!("split_index expects Utf8 text input, found {other:?}"),
    };
    let delimiter = match &args[1] {
        ScalarValue::Utf8(Some(value)) => value,
        ScalarValue::Utf8(None) | ScalarValue::Null => return Ok(ScalarValue::Utf8(None)),
        other => bail!("split_index expects Utf8 delimiter input, found {other:?}"),
    };
    let index = match &args[2] {
        ScalarValue::Int64(Some(value)) => *value,
        ScalarValue::Int64(None) | ScalarValue::Null => return Ok(ScalarValue::Utf8(None)),
        other => bail!("split_index expects Int64 index input, found {other:?}"),
    };
    if index < 0 || delimiter.is_empty() {
        return Ok(ScalarValue::Utf8(None));
    }
    let out = text
        .split(delimiter)
        .nth(index as usize)
        .map(ToString::to_string);
    Ok(ScalarValue::Utf8(out))
}

pub(super) fn eval_count_char(args: &[ScalarValue]) -> Result<ScalarValue> {
    if args.len() != 2 {
        bail!("count_char expects 2 arguments, found {}", args.len());
    }
    let text = match &args[0] {
        ScalarValue::Utf8(Some(value)) => value,
        ScalarValue::Utf8(None) | ScalarValue::Null => return Ok(ScalarValue::Int64(None)),
        other => bail!("count_char expects Utf8 text input, found {other:?}"),
    };
    let needle = match &args[1] {
        ScalarValue::Utf8(Some(value)) => value,
        ScalarValue::Utf8(None) | ScalarValue::Null => return Ok(ScalarValue::Int64(None)),
        other => bail!("count_char expects Utf8 needle input, found {other:?}"),
    };
    if needle.is_empty() {
        return Ok(ScalarValue::Int64(Some(0)));
    }
    let count = text.matches(needle).count() as i64;
    Ok(ScalarValue::Int64(Some(count)))
}

pub(super) fn eval_proctime(args: &[ScalarValue]) -> Result<ScalarValue> {
    if !args.is_empty() {
        bail!("proctime expects 0 arguments, found {}", args.len());
    }
    Ok(ScalarValue::TimestampMillisecond(None, None))
}

pub(super) fn normalize_date_format_pattern(pattern: &str) -> String {
    pattern
        .replace("yyyy", "%Y")
        .replace("MM", "%m")
        .replace("dd", "%d")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("ss", "%S")
}
