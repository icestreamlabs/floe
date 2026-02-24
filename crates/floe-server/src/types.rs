use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use datafusion::arrow::array::{
    Array, BooleanArray, Decimal128Array, Decimal256Array, Int16Array, Int32Array, Int64Array,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, UInt16Array, UInt32Array,
    UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo};
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;
use postgres_types::Type;

use super::user_error;

pub(super) fn encode_stream_row(
    batch: &RecordBatch,
    row_idx: usize,
    schema: Arc<Vec<FieldInfo>>,
) -> PgWireResult<DataRow> {
    let mut encoder = DataRowEncoder::new(schema);
    for col_idx in 0..batch.num_columns() {
        let array = batch.column(col_idx);
        let data_type = batch.schema().field(col_idx).data_type().clone();
        encode_arrow_value(array.as_ref(), row_idx, &data_type, &mut encoder)?;
    }
    Ok(encoder.take_row())
}

pub(super) fn encode_arrow_value(
    array: &dyn Array,
    row_idx: usize,
    data_type: &DataType,
    encoder: &mut DataRowEncoder,
) -> PgWireResult<()> {
    match data_type {
        DataType::Int16 => {
            let array = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| user_error(format!("expected Int16Array for {data_type:?}")))?;
            let value = if array.is_null(row_idx) {
                None
            } else {
                Some(array.value(row_idx))
            };
            encoder.encode_field(&value)
        }
        DataType::UInt16 => {
            let array = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| user_error(format!("expected UInt16Array for {data_type:?}")))?;
            let value = if array.is_null(row_idx) {
                None
            } else {
                Some(array.value(row_idx) as i64)
            };
            encoder.encode_field(&value)
        }
        DataType::Int32 => {
            let array = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| user_error(format!("expected Int32Array for {data_type:?}")))?;
            let value = if array.is_null(row_idx) {
                None
            } else {
                Some(array.value(row_idx))
            };
            encoder.encode_field(&value)
        }
        DataType::UInt32 => {
            let array = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| user_error(format!("expected UInt32Array for {data_type:?}")))?;
            let value = if array.is_null(row_idx) {
                None
            } else {
                Some(array.value(row_idx) as i64)
            };
            encoder.encode_field(&value)
        }
        DataType::Int64 => {
            let array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| user_error(format!("expected Int64Array for {data_type:?}")))?;
            let value = if array.is_null(row_idx) {
                None
            } else {
                Some(array.value(row_idx))
            };
            encoder.encode_field(&value)
        }
        DataType::UInt64 => {
            let array = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| user_error(format!("expected UInt64Array for {data_type:?}")))?;
            let value = if array.is_null(row_idx) {
                None
            } else {
                Some(array.value(row_idx) as i64)
            };
            encoder.encode_field(&value)
        }
        DataType::Boolean => {
            let array = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| user_error(format!("expected BooleanArray for {data_type:?}")))?;
            let value = if array.is_null(row_idx) {
                None
            } else {
                Some(array.value(row_idx))
            };
            encoder.encode_field(&value)
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    user_error(format!(
                        "expected TimestampMicrosecondArray for {data_type:?}"
                    ))
                })?;
            if array.is_null(row_idx) {
                return encoder.encode_field::<Option<NaiveDateTime>>(&None);
            }
            let micros = array.value(row_idx);
            let naive = micros_to_naive_datetime(micros)
                .ok_or_else(|| user_error(format!("timestamp micros {micros} out of range")))?;
            if tz.is_some() {
                let utc: DateTime<Utc> = Utc.from_utc_datetime(&naive);
                encoder.encode_field(&Some(utc))
            } else {
                encoder.encode_field(&Some(naive))
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    user_error(format!(
                        "expected TimestampMillisecondArray for {data_type:?}"
                    ))
                })?;
            if array.is_null(row_idx) {
                return encoder.encode_field::<Option<NaiveDateTime>>(&None);
            }
            let micros = array.value(row_idx).saturating_mul(1000);
            let naive = micros_to_naive_datetime(micros)
                .ok_or_else(|| user_error(format!("timestamp micros {micros} out of range")))?;
            if tz.is_some() {
                let utc: DateTime<Utc> = Utc.from_utc_datetime(&naive);
                encoder.encode_field(&Some(utc))
            } else {
                encoder.encode_field(&Some(naive))
            }
        }
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| user_error(format!("expected StringArray for {data_type:?}")))?;
            let value: Option<&str> = if array.is_null(row_idx) {
                None
            } else {
                Some(array.value(row_idx))
            };
            encoder.encode_field(&value)
        }
        DataType::Decimal128(_, _) => {
            let array = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| user_error(format!("expected Decimal128Array for {data_type:?}")))?;
            if array.is_null(row_idx) {
                encoder.encode_field::<Option<String>>(&None)
            } else {
                let value = array.value_as_string(row_idx);
                encoder.encode_field(&Some(value))
            }
        }
        DataType::Decimal256(_, _) => {
            let array = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| user_error(format!("expected Decimal256Array for {data_type:?}")))?;
            if array.is_null(row_idx) {
                encoder.encode_field::<Option<String>>(&None)
            } else {
                let value = array.value_as_string(row_idx);
                encoder.encode_field(&Some(value))
            }
        }
        other => Err(user_error(format!(
            "unsupported column type {} in result set",
            other
        ))),
    }
}

fn micros_to_naive_datetime(micros: i64) -> Option<NaiveDateTime> {
    DateTime::<Utc>::from_timestamp_micros(micros).map(|dt| dt.naive_utc())
}

pub(super) fn arrow_schema_to_field_info(schema: &SchemaRef) -> PgWireResult<Vec<FieldInfo>> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        match field.data_type() {
            DataType::Int16
            | DataType::UInt16
            | DataType::Int32
            | DataType::UInt32
            | DataType::Int64
            | DataType::UInt64
            | DataType::Boolean
            | DataType::Utf8
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Timestamp(TimeUnit::Millisecond, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => {}
            other => {
                return Err(user_error(format!(
                    "unsupported column type {} in result set",
                    other
                )));
            }
        }
        let pg_type = match field.data_type() {
            DataType::Timestamp(_, Some(_)) => Type::TIMESTAMPTZ,
            DataType::Timestamp(_, None) => Type::TIMESTAMP,
            DataType::Boolean => Type::BOOL,
            DataType::Utf8 => Type::TEXT,
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => Type::NUMERIC,
            _ => Type::INT8,
        };
        fields.push(FieldInfo::new(
            field.name().clone(),
            None,
            None,
            pg_type,
            FieldFormat::Text,
        ));
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use bytes::Buf;
    use datafusion::arrow::array::{
        ArrayRef, BooleanArray, Decimal128Array, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray,
    };

    #[test]
    fn arrow_schema_maps_timestamp_types() {
        let schema = SchemaRef::from(Schema::new(vec![
            Field::new(
                "ts_micros",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "ts_millis",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new("flag", DataType::Boolean, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
        ]));

        let fields = arrow_schema_to_field_info(&schema).expect("map schema");
        assert_eq!(fields.len(), 5);
        assert_eq!(fields[0].datatype(), &Type::TIMESTAMP);
        assert_eq!(fields[1].datatype(), &Type::TIMESTAMPTZ);
        assert_eq!(fields[2].datatype(), &Type::BOOL);
        assert_eq!(fields[3].datatype(), &Type::TEXT);
        assert_eq!(fields[4].datatype(), &Type::NUMERIC);
    }

    #[test]
    fn encode_timestamp_values() {
        // 2024-01-01T00:00:01Z
        let micros = 1_704_067_201_000_000i64;
        let millis = micros / 1000;

        let micros_array = TimestampMicrosecondArray::from(vec![Some(micros), None]);
        let millis_array = {
            use arrow_buffer::{Buffer, NullBuffer};
            use arrow_data::ArrayData;

            let values = Buffer::from_slice_ref([millis, 0]);
            let nulls = NullBuffer::from(vec![true, false]);
            let data = ArrayData::builder(DataType::Timestamp(
                TimeUnit::Millisecond,
                Some("UTC".into()),
            ))
            .len(2)
            .add_buffer(values)
            .null_bit_buffer(Some(nulls.into_inner().into_inner()))
            .build()
            .expect("array data");
            TimestampMillisecondArray::from(data)
        };
        let bool_array = BooleanArray::from(vec![Some(true), None]);
        let utf8_array = StringArray::from(vec![Some("hello"), None]);
        let decimal_array = Decimal128Array::from(vec![Some(12_345i128), None])
            .with_precision_and_scale(10, 2)
            .expect("decimal array");

        let schema = SchemaRef::from(Schema::new(vec![
            Field::new(
                "ts_micros",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "ts_millis",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new("flag", DataType::Boolean, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(micros_array) as ArrayRef,
                Arc::new(millis_array) as ArrayRef,
                Arc::new(bool_array) as ArrayRef,
                Arc::new(utf8_array) as ArrayRef,
                Arc::new(decimal_array) as ArrayRef,
            ],
        )
        .expect("batch");

        let field_info = Arc::new(arrow_schema_to_field_info(&batch.schema()).expect("schema"));
        let row = encode_stream_row(&batch, 0, Arc::clone(&field_info)).expect("encode row");

        // Decode the row buffer to confirm both fields are non-null and encoded.
        let mut buf = row.data.clone();
        let first_len = buf.get_i32();
        assert!(first_len > 0);
        let _ = buf.split_to(first_len as usize);
        let second_len = buf.get_i32();
        assert!(second_len > 0);
        let _ = buf.split_to(second_len as usize);
        let third_len = buf.get_i32();
        assert!(third_len > 0);
        let _ = buf.split_to(third_len as usize);
        let fourth_len = buf.get_i32();
        assert!(fourth_len > 0);
        let _ = buf.split_to(fourth_len as usize);
        let fifth_len = buf.get_i32();
        assert!(fifth_len > 0);

        // Null row should encode null markers.
        let null_row = encode_stream_row(&batch, 1, field_info).expect("encode null row");
        let mut buf_null = null_row.data.clone();
        assert_eq!(buf_null.get_i32(), -1);
        assert_eq!(buf_null.get_i32(), -1);
        assert_eq!(buf_null.get_i32(), -1);
        assert_eq!(buf_null.get_i32(), -1);
        assert_eq!(buf_null.get_i32(), -1);
    }
}
