use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Int64Builder, NullArray, StringBuilder,
    TimestampMillisecondBuilder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::scalar::ScalarValue;

use crate::encoding::EncodedRowScalar;

pub(crate) enum ScalarColumnBuilder {
    Int64(Int64Builder),
    Utf8(StringBuilder),
    TimestampMillis {
        builder: TimestampMillisecondBuilder,
        data_type: DataType,
    },
    Bool(BooleanBuilder),
    Binary(BinaryBuilder),
    UInt64(UInt64Builder),
    Null {
        len: usize,
    },
}

impl ScalarColumnBuilder {
    pub(crate) fn new(data_type: &DataType, capacity: usize) -> Result<Self> {
        match data_type {
            DataType::Int64 => Ok(Self::Int64(Int64Builder::with_capacity(capacity))),
            DataType::Utf8 => Ok(Self::Utf8(StringBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(8),
            ))),
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                let data_type = DataType::Timestamp(TimeUnit::Millisecond, tz.clone());
                Ok(Self::TimestampMillis {
                    builder: TimestampMillisecondBuilder::with_capacity(capacity)
                        .with_data_type(data_type.clone()),
                    data_type,
                })
            }
            DataType::Boolean => Ok(Self::Bool(BooleanBuilder::with_capacity(capacity))),
            DataType::Binary => Ok(Self::Binary(BinaryBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(16),
            ))),
            DataType::UInt64 => Ok(Self::UInt64(UInt64Builder::with_capacity(capacity))),
            DataType::Null => Ok(Self::Null { len: 0 }),
            other => Err(anyhow!(
                "unsupported scalar column type for typed array builder: {other:?}"
            )),
        }
    }

    pub(crate) fn append(&mut self, value: &ScalarValue) -> Result<()> {
        match self {
            Self::Int64(builder) => match value {
                ScalarValue::Int64(v) => builder.append_option(*v),
                other if other.is_null() => builder.append_null(),
                other => {
                    return Err(anyhow!(
                        "expected Int64 scalar for Int64 column, found {other:?}"
                    ));
                }
            },
            Self::Utf8(builder) => match value {
                ScalarValue::Utf8(v) => builder.append_option(v.as_deref()),
                other if other.is_null() => builder.append_null(),
                other => {
                    return Err(anyhow!(
                        "expected Utf8 scalar for Utf8 column, found {other:?}"
                    ));
                }
            },
            Self::TimestampMillis { builder, .. } => match value {
                ScalarValue::TimestampMillisecond(v, _) => builder.append_option(*v),
                other if other.is_null() => builder.append_null(),
                other => {
                    return Err(anyhow!(
                        "expected TimestampMillisecond scalar for timestamp(ms) column, found {other:?}"
                    ));
                }
            },
            Self::Bool(builder) => match value {
                ScalarValue::Boolean(v) => builder.append_option(*v),
                other if other.is_null() => builder.append_null(),
                other => {
                    return Err(anyhow!(
                        "expected Boolean scalar for boolean column, found {other:?}"
                    ));
                }
            },
            Self::Binary(builder) => match value {
                ScalarValue::Binary(v) => builder.append_option(v.as_deref()),
                other if other.is_null() => builder.append_null(),
                other => {
                    return Err(anyhow!(
                        "expected Binary scalar for binary column, found {other:?}"
                    ));
                }
            },
            Self::UInt64(builder) => match value {
                ScalarValue::UInt64(v) => builder.append_option(*v),
                other if other.is_null() => builder.append_null(),
                other => {
                    return Err(anyhow!(
                        "expected UInt64 scalar for UInt64 column, found {other:?}"
                    ));
                }
            },
            Self::Null { len } => {
                if value.is_null() {
                    *len += 1;
                } else {
                    return Err(anyhow!(
                        "expected NULL scalar for Null column, found {value:?}"
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn append_encoded_scalar(&mut self, value: Option<&EncodedRowScalar>) -> Result<()> {
        match self {
            Self::Int64(builder) => match value {
                Some(EncodedRowScalar::Int64(value)) => builder.append_value(*value),
                None => builder.append_null(),
                Some(other) => {
                    return Err(anyhow!(
                        "expected Int64 encoded scalar for Int64 column, found {other:?}"
                    ));
                }
            },
            Self::Utf8(builder) => match value {
                Some(EncodedRowScalar::Utf8(value)) => builder.append_value(value),
                None => builder.append_null(),
                Some(other) => {
                    return Err(anyhow!(
                        "expected Utf8 encoded scalar for Utf8 column, found {other:?}"
                    ));
                }
            },
            Self::TimestampMillis { builder, .. } => match value {
                Some(EncodedRowScalar::TimestampMillis(value)) => builder.append_value(*value),
                None => builder.append_null(),
                Some(other) => {
                    return Err(anyhow!(
                        "expected TimestampMillis encoded scalar for timestamp(ms) column, found {other:?}"
                    ));
                }
            },
            Self::Bool(builder) => match value {
                Some(EncodedRowScalar::Bool(value)) => builder.append_value(*value),
                None => builder.append_null(),
                Some(other) => {
                    return Err(anyhow!(
                        "expected Bool encoded scalar for boolean column, found {other:?}"
                    ));
                }
            },
            Self::Binary(builder) => {
                if value.is_none() {
                    builder.append_null();
                } else {
                    return Err(anyhow!(
                        "cannot append encoded scalar to binary column builder"
                    ));
                }
            }
            Self::UInt64(builder) => {
                if value.is_none() {
                    builder.append_null();
                } else {
                    return Err(anyhow!(
                        "cannot append encoded scalar to UInt64 column builder"
                    ));
                }
            }
            Self::Null { len } => {
                if value.is_none() {
                    *len += 1;
                } else {
                    return Err(anyhow!(
                        "expected NULL encoded scalar for Null column, found {value:?}"
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn finish_array(&mut self) -> ArrayRef {
        match self {
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::TimestampMillis { builder, data_type } => {
                Arc::new(builder.finish().with_data_type(data_type.clone()))
            }
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::Binary(builder) => Arc::new(builder.finish()),
            Self::UInt64(builder) => Arc::new(builder.finish()),
            Self::Null { len } => {
                let finished = Arc::new(NullArray::new(*len));
                *len = 0;
                finished
            }
        }
    }
}
