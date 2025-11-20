use anyhow::{Context, Result};
use bytes::BufMut;
use bytes::BytesMut;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;

use super::{finish_message, start_message};

const TAIL_META_COLUMNS: usize = 3;
const TAIL_OP_LITERAL: &str = "1";

/// Encodes a record batch for a TAIL response, prepending metadata columns
/// before the user columns.
pub fn encode_tail_batch(buf: &mut BytesMut, batch: &RecordBatch, version: i64) -> Result<()> {
    for row in 0..batch.num_rows() {
        encode_tail_row(buf, batch, row, version)?;
    }
    Ok(())
}

fn encode_tail_row(
    buf: &mut BytesMut,
    batch: &RecordBatch,
    row: usize,
    version: i64,
) -> Result<()> {
    let len_pos = start_message(buf, b'D');
    let column_count = batch.num_columns() + TAIL_META_COLUMNS;
    buf.put_i16(i16::try_from(column_count).expect("tail data row column overflow"));

    put_text_field(buf, &version.to_string());
    put_text_field(buf, TAIL_OP_LITERAL);
    put_null_field(buf);

    for column in batch.columns() {
        if column.is_null(row) {
            put_null_field(buf);
            continue;
        }
        let value = array_value_to_string(column.as_ref(), row)
            .with_context(|| "format arrow value for tail output".to_string())?;
        put_text_field(buf, &value);
    }

    finish_message(buf, len_pos);
    Ok(())
}

fn put_text_field(buf: &mut BytesMut, value: &str) {
    buf.put_i32(i32::try_from(value.len()).expect("text field length overflow"));
    buf.extend_from_slice(value.as_bytes());
}

fn put_null_field(buf: &mut BytesMut) {
    buf.put_i32(-1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    #[test]
    fn encodes_tail_row_with_metadata() {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            schema.into(),
            vec![Arc::new(Int64Array::from(vec![5])) as ArrayRef],
        )
        .expect("record batch");
        let mut buf = BytesMut::new();
        encode_tail_batch(&mut buf, &batch, 42).expect("encode batch");

        let data = buf.freeze();
        assert_eq!(data[0], b'D');
        let field_count = i16::from_be_bytes([data[5], data[6]]);
        assert_eq!(field_count, 4);

        let mut idx = 7;
        let read_text = |buffer: &[u8], index: &mut usize| -> Option<String> {
            let len = i32::from_be_bytes([
                buffer[*index],
                buffer[*index + 1],
                buffer[*index + 2],
                buffer[*index + 3],
            ]);
            *index += 4;
            if len == -1 {
                return None;
            }
            let len = len as usize;
            let value = std::str::from_utf8(&buffer[*index..*index + len])
                .expect("utf8 value")
                .to_string();
            *index += len;
            Some(value)
        };

        let v = read_text(&data, &mut idx).expect("version value");
        assert_eq!(v, "42");
        let op = read_text(&data, &mut idx).expect("op value");
        assert_eq!(op, "1");
        let time = read_text(&data, &mut idx);
        assert!(time.is_none(), "__time should be NULL");
        let user = read_text(&data, &mut idx).expect("user column");
        assert_eq!(user, "5");
    }
}
