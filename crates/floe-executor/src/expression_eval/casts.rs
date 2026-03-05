use super::*;

pub(super) fn cast_value(value: &ScalarValue, data_type: &DataType) -> Result<ScalarValue> {
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

pub(super) fn try_cast_value(value: &ScalarValue, data_type: &DataType) -> ScalarValue {
    match cast_value(value, data_type) {
        Ok(value) => value,
        Err(_) => null_for_type(data_type),
    }
}

pub(super) fn null_for_type(data_type: &DataType) -> ScalarValue {
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

pub(super) fn null_like(value: &ScalarValue) -> ScalarValue {
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
