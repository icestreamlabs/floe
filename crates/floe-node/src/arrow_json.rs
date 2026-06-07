use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use floe_core::decimal::format_decimal128;

pub(crate) fn record_batch_row_to_json(
    batch: &RecordBatch,
    row_idx: usize,
    schema: &SchemaRef,
) -> Result<serde_json::Value> {
    if schema.fields().len() != batch.num_columns() {
        bail!(
            "JSON row schema has {} columns but batch has {}",
            schema.fields().len(),
            batch.num_columns()
        );
    }

    let mut object = serde_json::Map::new();
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array = batch.column(col_idx);
        object.insert(field.name().clone(), array_value_to_json(array, row_idx)?);
    }
    Ok(serde_json::Value::Object(object))
}

fn array_value_to_json(array: &ArrayRef, row_idx: usize) -> Result<serde_json::Value> {
    if array.is_null(row_idx) {
        return Ok(serde_json::Value::Null);
    }

    if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx) as f64));
    }
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Date32Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Decimal128Array>() {
        let scale = match values.data_type() {
            DataType::Decimal128(_, scale) => *scale,
            _ => 0,
        };
        return Ok(serde_json::Value::String(format_decimal128(
            values.value(row_idx),
            scale,
        )));
    }

    match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .context("timestamp second array has incompatible type")?;
            Ok(serde_json::Value::from(values.value(row_idx)))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .context("timestamp millisecond array has incompatible type")?;
            Ok(serde_json::Value::from(values.value(row_idx)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .context("timestamp microsecond array has incompatible type")?;
            Ok(serde_json::Value::from(values.value(row_idx)))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .context("timestamp nanosecond array has incompatible type")?;
            Ok(serde_json::Value::from(values.value(row_idx)))
        }
        other => bail!("unsupported Arrow column type for JSON conversion: {other:?}"),
    }
}
