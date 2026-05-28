use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Int64Builder,
    NullArray, StringBuilder, TimestampMillisecondBuilder, UInt64Builder,
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
    DateDays(Date32Builder),
    Decimal128(Decimal128Builder),
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
            DataType::Date32 => Ok(Self::DateDays(Date32Builder::with_capacity(capacity))),
            DataType::Decimal128(precision, scale) => Ok(Self::Decimal128(
                Decimal128Builder::with_capacity(capacity)
                    .with_data_type(DataType::Decimal128(*precision, *scale)),
            )),
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
            Self::Int64(builder) => rows.iter().try_for_each(|row| match row.get(column_idx) {
                Some(RowValue::Int64(v)) => {
                    builder.append_value(*v);
                    Ok(())
                }
                Some(other) => Err(anyhow!(
                    "expected Int64 row value for Int64 column, found {other:?}"
                )),
                None => Err(anyhow!(
                    "row missing column index {column_idx} for Int64 column"
                )),
            })?,
            Self::Utf8(builder) => rows.iter().try_for_each(|row| match row.get(column_idx) {
                Some(RowValue::Utf8(v)) => {
                    builder.append_value(v);
                    Ok(())
                }
                Some(RowValue::Numeric(v)) => {
                    builder.append_value(v);
                    Ok(())
                }
                Some(other) => Err(anyhow!(
                    "expected Utf8 row value for Utf8 column, found {other:?}"
                )),
                None => Err(anyhow!(
                    "row missing column index {column_idx} for Utf8 column"
                )),
            })?,
            Self::TimestampMillis { builder, .. } => {
                rows.iter().try_for_each(|row| match row.get(column_idx) {
                    Some(RowValue::TimestampMillis(v)) => {
                        builder.append_value(*v);
                        Ok(())
                    }
                    Some(other) => Err(anyhow!(
                        "expected TimestampMillis row value for timestamp(ms) column, found {other:?}"
                    )),
                    None => Err(anyhow!(
                        "row missing column index {column_idx} for timestamp(ms) column"
                    )),
                })?
            }
            Self::DateDays(builder) => rows.iter().try_for_each(|row| match row.get(column_idx) {
                Some(RowValue::DateDays(v)) => {
                    builder.append_value(*v);
                    Ok(())
                }
                Some(other) => Err(anyhow!(
                    "expected DateDays row value for Date32 column, found {other:?}"
                )),
                None => Err(anyhow!(
                    "row missing column index {column_idx} for Date32 column"
                )),
            })?,
            Self::Decimal128(builder) => {
                rows.iter().try_for_each(|row| match row.get(column_idx) {
                    Some(RowValue::Decimal128(v)) => {
                        builder.append_value(*v);
                        Ok(())
                    }
                    Some(other) => Err(anyhow!(
                        "expected Decimal128 row value for Decimal128 column, found {other:?}"
                    )),
                    None => Err(anyhow!(
                        "row missing column index {column_idx} for Decimal128 column"
                    )),
                })?
            }
            Self::Bool(builder) => rows.iter().try_for_each(|row| match row.get(column_idx) {
                Some(RowValue::Bool(v)) => {
                    builder.append_value(*v);
                    Ok(())
                }
                Some(other) => Err(anyhow!(
                    "expected Bool row value for boolean column, found {other:?}"
                )),
                None => Err(anyhow!(
                    "row missing column index {column_idx} for boolean column"
                )),
            })?,
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

    #[cfg(test)]
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
            Self::DateDays(builder) => match value {
                Some(EncodedRowScalar::DateDays(value)) => builder.append_value(*value),
                None => builder.append_null(),
                Some(other) => {
                    return Err(anyhow!(
                        "expected DateDays encoded scalar for Date32 column, found {other:?}"
                    ));
                }
            },
            Self::Decimal128(builder) => match value {
                Some(EncodedRowScalar::Decimal128(value)) => builder.append_value(*value),
                None => builder.append_null(),
                Some(other) => {
                    return Err(anyhow!(
                        "expected Decimal128 encoded scalar for Decimal128 column, found {other:?}"
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

    #[cfg(test)]
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
            Self::DateDays(builder) => match value {
                Some(EncodedRowScalar::DateDays(value)) => {
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
                        "expected DateDays encoded scalar for Date32 column, found {other:?}"
                    ));
                }
            },
            Self::Decimal128(builder) => match value {
                Some(EncodedRowScalar::Decimal128(value)) => {
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
                        "expected Decimal128 encoded scalar for Decimal128 column, found {other:?}"
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
            Self::DateDays(builder) => Arc::new(builder.finish()),
            Self::Decimal128(builder) => Arc::new(builder.finish()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Array, BinaryArray, BooleanArray, Int64Array, NullArray, StringArray,
        TimestampMillisecondArray, UInt64Array,
    };

    #[test]
    fn appends_row_values_and_finishes_typed_arrays() {
        let rows = vec![
            vec![
                RowValue::Int64(1),
                RowValue::Utf8("a".to_string()),
                RowValue::TimestampMillis(10),
                RowValue::Bool(true),
            ],
            vec![
                RowValue::Int64(2),
                RowValue::Utf8("b".to_string()),
                RowValue::TimestampMillis(20),
                RowValue::Bool(false),
            ],
        ];

        let mut ints = ScalarColumnBuilder::new(&DataType::Int64, rows.len()).expect("int builder");
        ints.append_row_values_column(&rows, 0)
            .expect("append int rows");
        let ints = ints.finish_array();
        let ints = ints
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int array");
        assert_eq!(ints.values(), &[1, 2]);

        let mut utf8 = ScalarColumnBuilder::new(&DataType::Utf8, rows.len()).expect("utf8 builder");
        utf8.append_row_values_column(&rows, 1)
            .expect("append utf8 rows");
        let utf8 = utf8.finish_array();
        let utf8 = utf8
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 array");
        assert_eq!(utf8.value(0), "a");
        assert_eq!(utf8.value(1), "b");

        let ts_type = DataType::Timestamp(TimeUnit::Millisecond, None);
        let mut ts = ScalarColumnBuilder::new(&ts_type, rows.len()).expect("timestamp builder");
        ts.append_row_values_column(&rows, 2)
            .expect("append timestamp rows");
        let ts = ts.finish_array();
        let ts = ts
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp array");
        assert_eq!(ts.value(0), 10);
        assert_eq!(ts.value(1), 20);

        let mut bools =
            ScalarColumnBuilder::new(&DataType::Boolean, rows.len()).expect("bool builder");
        bools
            .append_row_values_column(&rows, 3)
            .expect("append bool rows");
        let bools = bools.finish_array();
        let bools = bools
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("bool array");
        assert!(bools.value(0));
        assert!(!bools.value(1));
    }

    #[test]
    fn append_encoded_scalar_supports_nulls_and_type_checks() {
        let mut ints = ScalarColumnBuilder::new(&DataType::Int64, 3).expect("int builder");
        ints.append_encoded_scalar(Some(&EncodedRowScalar::Int64(7)))
            .expect("append int");
        ints.append_encoded_scalar(None).expect("append null");
        let err = ints
            .append_encoded_scalar(Some(&EncodedRowScalar::Utf8("x".to_string())))
            .unwrap_err();
        assert!(format!("{err:#}").contains("expected Int64 encoded scalar"));

        let ints = ints.finish_array();
        let ints = ints
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int array");
        assert_eq!(ints.len(), 2);
        assert_eq!(ints.value(0), 7);
        assert!(ints.is_null(1));

        let mut nulls = ScalarColumnBuilder::new(&DataType::Null, 0).expect("null builder");
        nulls.append_encoded_scalar(None).expect("append null row");
        let err = nulls
            .append_encoded_scalar(Some(&EncodedRowScalar::Bool(true)))
            .unwrap_err();
        assert!(format!("{err:#}").contains("expected NULL encoded scalar"));
        let nulls = nulls.finish_array();
        let nulls = nulls
            .as_any()
            .downcast_ref::<NullArray>()
            .expect("null array");
        assert_eq!(nulls.len(), 1);
    }

    #[test]
    fn repeated_and_specialized_appenders_work() {
        let mut bools = ScalarColumnBuilder::new(&DataType::Boolean, 4).expect("bool builder");
        bools
            .append_encoded_scalar_repeated(Some(&EncodedRowScalar::Bool(true)), 2)
            .expect("append repeated bool");
        bools
            .append_encoded_scalar_repeated(None, 2)
            .expect("append repeated null bool");
        let bools = bools.finish_array();
        let bools = bools
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("bool array");
        assert_eq!(bools.len(), 4);
        assert!(bools.value(0));
        assert!(bools.value(1));
        assert!(bools.is_null(2));
        assert!(bools.is_null(3));

        let mut binary = ScalarColumnBuilder::new(&DataType::Binary, 2).expect("binary builder");
        binary
            .append_binary_value(&[1_u8, 2_u8])
            .expect("append binary");
        binary
            .append_encoded_scalar(None)
            .expect("append binary null");
        let binary = binary.finish_array();
        let binary = binary
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary array");
        assert_eq!(binary.value(0), &[1_u8, 2_u8]);
        assert!(binary.is_null(1));

        let mut u64s = ScalarColumnBuilder::new(&DataType::UInt64, 2).expect("u64 builder");
        u64s.append_u64_value(9).expect("append u64");
        let err = u64s.append_i64_value(1).unwrap_err();
        assert!(format!("{err:#}").contains("expected Int64 column builder"));
        u64s.append_encoded_scalar(None).expect("append u64 null");
        let u64s = u64s.finish_array();
        let u64s = u64s
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("u64 array");
        assert_eq!(u64s.value(0), 9);
        assert!(u64s.is_null(1));
    }
}
