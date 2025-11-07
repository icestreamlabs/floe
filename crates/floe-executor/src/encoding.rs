use anyhow::{Result, anyhow};
use datafusion::scalar::ScalarValue;

/// Encode a projected row into deterministic bytes for DBSP keys.
pub fn encode_projected_row_key(columns: &[ScalarValue]) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(64);
    let count = u32::try_from(columns.len()).map_err(|_| anyhow!("too many columns in MV key"))?;
    buf.extend_from_slice(&count.to_le_bytes());
    for value in columns {
        match value {
            ScalarValue::Int64(Some(v)) => {
                buf.push(0x01);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            ScalarValue::Utf8(Some(text)) => {
                buf.push(0x02);
                let bytes = text.as_bytes();
                let len = u32::try_from(bytes.len())
                    .map_err(|_| anyhow!("utf8 value too large for MV key"))?;
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            ScalarValue::TimestampMillisecond(Some(v), _) => {
                buf.push(0x03);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            ScalarValue::Boolean(Some(flag)) => {
                buf.push(0x04);
                buf.push(if *flag { 1 } else { 0 });
            }
            ScalarValue::Null => return Err(anyhow!("null values not supported in MV keys")),
            other => return Err(anyhow!("unsupported ScalarValue in MV key: {other:?}")),
        }
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_simple_rows() {
        let row = vec![
            ScalarValue::Int64(Some(42)),
            ScalarValue::Utf8(Some("abc".into())),
            ScalarValue::Boolean(Some(true)),
        ];
        let encoded = encode_projected_row_key(&row).expect("encode");
        assert!(!encoded.is_empty());
    }

    #[test]
    fn rejects_nulls() {
        let row = vec![ScalarValue::Int64(None)];
        let err = encode_projected_row_key(&row).unwrap_err();
        assert!(err.to_string().contains("null"));
    }
}
