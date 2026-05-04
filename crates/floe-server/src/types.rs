use std::sync::Arc;

use arrow_pg::datatypes::field_into_pg_type;
use arrow_pg::encoder::encode_value as encode_arrow_pg_value;
use datafusion::arrow::array::{Array, ArrayRef, Decimal256Array};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo};
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;
use postgres_types::Type;

use super::user_error;

#[allow(dead_code)]
pub(super) fn encode_stream_row(
    batch: &RecordBatch,
    row_idx: usize,
    schema: Arc<Vec<FieldInfo>>,
) -> PgWireResult<DataRow> {
    let fields = Arc::clone(&schema);
    let mut encoder = DataRowEncoder::new(schema);
    let batch_schema = batch.schema();
    for col_idx in 0..batch.num_columns() {
        let array = batch.column(col_idx);
        let field = batch_schema.field(col_idx);
        let pg_field = fields
            .get(col_idx)
            .ok_or_else(|| user_error(format!("missing field metadata for column {col_idx}")))?;
        encode_arrow_value(array, field, pg_field, row_idx, &mut encoder)?;
    }
    Ok(encoder.take_row())
}

pub(super) fn encode_arrow_value(
    array: &ArrayRef,
    field: &Field,
    pg_field: &FieldInfo,
    row_idx: usize,
    encoder: &mut DataRowEncoder,
) -> PgWireResult<()> {
    match field.data_type() {
        DataType::Decimal256(_, _) => {
            let array = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| user_error("expected Decimal256Array for Decimal256 field"))?;
            if array.is_null(row_idx) {
                encoder.encode_field::<Option<String>>(&None)
            } else {
                let value = array.value_as_string(row_idx);
                encoder.encode_field(&Some(value))
            }
        }
        _ => encode_arrow_pg_value(encoder, array, row_idx, field, pg_field),
    }
}

pub(super) fn arrow_schema_to_field_info(schema: &SchemaRef) -> PgWireResult<Vec<FieldInfo>> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let pg_type = match field.data_type() {
            DataType::Decimal256(_, _) => Type::NUMERIC,
            _ => field_into_pg_type(field)?,
        };
        fields.push(FieldInfo::new(
            field.name().clone(),
            None,
            None,
            pg_type,
            FieldFormat::Text,
        ))
    }
    if fields.len() != schema.fields().len() {
        return Err(user_error(format!(
            "expected {} fields, encoded {}",
            schema.fields().len(),
            fields.len()
        )));
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
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
