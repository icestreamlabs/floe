use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Int64Builder, NullArray, StringBuilder,
    TimestampMillisecondBuilder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use floe_core::RowValue;

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

    pub(crate) fn append_row_values_column(
        &mut self,
        rows: &[Vec<RowValue>],
        column_idx: usize,
    ) -> Result<()> {
        match self {
            Self::Int64(builder) => {
                for row in rows {
                    match row.get(column_idx) {
                        Some(RowValue::Int64(v)) => builder.append_value(*v),
                        Some(other) => {
                            return Err(anyhow!(
                                "expected Int64 row value for Int64 column, found {other:?}"
                            ));
                        }
                        None => {
                            return Err(anyhow!(
                                "row missing column index {column_idx} for Int64 column"
                            ));
                        }
                    }
                }
            }
            Self::Utf8(builder) => {
                for row in rows {
                    match row.get(column_idx) {
                        Some(RowValue::Utf8(v)) => builder.append_value(v),
                        Some(other) => {
                            return Err(anyhow!(
                                "expected Utf8 row value for Utf8 column, found {other:?}"
                            ));
                        }
                        None => {
                            return Err(anyhow!(
                                "row missing column index {column_idx} for Utf8 column"
                            ));
                        }
                    }
                }
            }
            Self::TimestampMillis { builder, .. } => {
                for row in rows {
                    match row.get(column_idx) {
                        Some(RowValue::TimestampMillis(v)) => builder.append_value(*v),
                        Some(other) => {
                            return Err(anyhow!(
                                "expected TimestampMillis row value for timestamp(ms) column, found {other:?}"
                            ));
                        }
                        None => {
                            return Err(anyhow!(
                                "row missing column index {column_idx} for timestamp(ms) column"
                            ));
                        }
                    }
                }
            }
            Self::Bool(builder) => {
                for row in rows {
                    match row.get(column_idx) {
                        Some(RowValue::Bool(v)) => builder.append_value(*v),
                        Some(other) => {
                            return Err(anyhow!(
                                "expected Bool row value for boolean column, found {other:?}"
                            ));
                        }
                        None => {
                            return Err(anyhow!(
                                "row missing column index {column_idx} for boolean column"
                            ));
                        }
                    }
                }
            }
            Self::Binary(_) => {
                return Err(anyhow!("cannot append RowValue into binary column builder"));
            }
            Self::UInt64(_) => {
                return Err(anyhow!("cannot append RowValue into UInt64 column builder"));
            }
            Self::Null { .. } => {
                return Err(anyhow!("cannot append RowValue into Null column builder"));
            }
        }
        Ok(())
    }

    pub(crate) fn append_u64_value(&mut self, value: u64) -> Result<()> {
        match self {
            Self::UInt64(builder) => {
                builder.append_value(value);
                Ok(())
            }
            other => Err(anyhow!(
                "expected UInt64 column builder when appending u64 value, found {:?}",
                std::mem::discriminant(other)
            )),
        }
    }

    pub(crate) fn append_i64_value(&mut self, value: i64) -> Result<()> {
        match self {
            Self::Int64(builder) => {
                builder.append_value(value);
                Ok(())
            }
            _ => Err(anyhow!(
                "expected Int64 column builder when appending i64 value"
            )),
        }
    }

    pub(crate) fn append_binary_value(&mut self, value: &[u8]) -> Result<()> {
        match self {
            Self::Binary(builder) => {
                builder.append_value(value);
                Ok(())
            }
            _ => Err(anyhow!(
                "expected Binary column builder when appending binary value"
            )),
        }
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

    pub(crate) fn append_encoded_scalar_repeated(
        &mut self,
        value: Option<&EncodedRowScalar>,
        count: usize,
    ) -> Result<()> {
        if count == 0 {
            return Ok(());
        }
        match self {
            Self::Int64(builder) => match value {
                Some(EncodedRowScalar::Int64(value)) => {
                    for _ in 0..count {
                        builder.append_value(*value);
                    }
                }
                None => {
                    for _ in 0..count {
                        builder.append_null();
                    }
                }
                Some(other) => {
                    return Err(anyhow!(
                        "expected Int64 encoded scalar for Int64 column, found {other:?}"
                    ));
                }
            },
            Self::Utf8(builder) => match value {
                Some(EncodedRowScalar::Utf8(value)) => {
                    for _ in 0..count {
                        builder.append_value(value);
                    }
                }
                None => {
                    for _ in 0..count {
                        builder.append_null();
                    }
                }
                Some(other) => {
                    return Err(anyhow!(
                        "expected Utf8 encoded scalar for Utf8 column, found {other:?}"
                    ));
                }
            },
            Self::TimestampMillis { builder, .. } => match value {
                Some(EncodedRowScalar::TimestampMillis(value)) => {
                    for _ in 0..count {
                        builder.append_value(*value);
                    }
                }
                None => {
                    for _ in 0..count {
                        builder.append_null();
                    }
                }
                Some(other) => {
                    return Err(anyhow!(
                        "expected TimestampMillis encoded scalar for timestamp(ms) column, found {other:?}"
                    ));
                }
            },
            Self::Bool(builder) => match value {
                Some(EncodedRowScalar::Bool(value)) => {
                    for _ in 0..count {
                        builder.append_value(*value);
                    }
                }
                None => {
                    for _ in 0..count {
                        builder.append_null();
                    }
                }
                Some(other) => {
                    return Err(anyhow!(
                        "expected Bool encoded scalar for boolean column, found {other:?}"
                    ));
                }
            },
            Self::Binary(builder) => {
                if value.is_none() {
                    for _ in 0..count {
                        builder.append_null();
                    }
                } else {
                    return Err(anyhow!(
                        "cannot append encoded scalar to binary column builder"
                    ));
                }
            }
            Self::UInt64(builder) => {
                if value.is_none() {
                    for _ in 0..count {
                        builder.append_null();
                    }
                } else {
                    return Err(anyhow!(
                        "cannot append encoded scalar to UInt64 column builder"
                    ));
                }
            }
            Self::Null { len } => {
                if value.is_none() {
                    *len += count;
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
