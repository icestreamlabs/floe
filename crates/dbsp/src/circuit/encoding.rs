use anyhow::{Result, anyhow};

use crate::circuit::row::Row;
use crate::circuit::schema::PrimaryKey;
use crate::circuit::types::ScalarValue;

pub struct KeyEncoder {
    buffer: Vec<u8>,
}

impl KeyEncoder {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    pub fn encode_scalar(&mut self, value: &ScalarValue) -> Result<()> {
        encode_scalar(value, &mut self.buffer)
    }

    pub fn finish(self) -> Vec<u8> {
        self.buffer
    }
}

pub fn encode_scalar(value: &ScalarValue, buffer: &mut Vec<u8>) -> Result<()> {
    match value {
        ScalarValue::Int64(v) | ScalarValue::TimestampMillis(v) => {
            buffer.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::Utf8(text) => {
            let len = u32::try_from(text.len()).map_err(|_| anyhow!("string too long"))?;
            buffer.extend_from_slice(&len.to_le_bytes());
            buffer.extend_from_slice(text.as_bytes());
        }
        ScalarValue::Bool(v) => {
            buffer.push(if *v { 1 } else { 0 });
        }
        ScalarValue::Null(ty) => {
            return Err(anyhow!(
                "null value not supported in key encoding for {}",
                ty.name()
            ));
        }
    }
    Ok(())
}

pub fn encode_composite_key(row: &Row, key: &PrimaryKey) -> Result<Vec<u8>> {
    let mut encoder = KeyEncoder::new();
    for &index in key.columns() {
        let value = row
            .value(index)
            .ok_or_else(|| anyhow!("missing value for key column index {index}"))?;
        encoder.encode_scalar(value)?;
    }
    Ok(encoder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit::row::{Row, RowBuilder};
    use crate::circuit::schema::{Field, RowSchema};
    use crate::circuit::types::{DbspScalarType, ScalarValue};
    use std::sync::Arc;

    fn schema() -> Arc<RowSchema> {
        RowSchema::try_new(vec![
            Field::new("id", DbspScalarType::Int64, false),
            Field::new("name", DbspScalarType::Utf8, false),
            Field::new("active", DbspScalarType::Bool, false),
        ])
        .expect("schema")
    }

    #[test]
    fn encodes_composite_keys() {
        let schema = schema();
        let row = RowBuilder::new(schema.clone())
            .push(ScalarValue::Int64(7))
            .unwrap()
            .push(ScalarValue::Utf8("alice".into()))
            .unwrap()
            .push(ScalarValue::Bool(true))
            .unwrap()
            .finish()
            .expect("row");

        let pk = PrimaryKey::new(schema, &["id", "name"]).expect("pk");
        let encoded = encode_composite_key(&row, &pk).expect("encode");
        // Int64 le bytes + string length + bytes
        assert_eq!(encoded[..8], 7i64.to_le_bytes());
        let len = u32::from_le_bytes(encoded[8..12].try_into().unwrap());
        assert_eq!(len, 5);
        assert_eq!(&encoded[12..17], b"alice");
    }

    #[test]
    fn rejects_nulls() {
        let schema = RowSchema::try_new(vec![Field::new("name", DbspScalarType::Utf8, true)])
            .expect("schema");
        let row = Row::new(
            schema.clone(),
            vec![ScalarValue::null(DbspScalarType::Utf8)],
        )
        .expect("row");
        let pk = PrimaryKey::new(schema, &["name"]).expect("pk");
        let err = encode_composite_key(&row, &pk).unwrap_err();
        assert!(err.to_string().contains("null value"));
    }
}
