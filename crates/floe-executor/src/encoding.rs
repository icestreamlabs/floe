use anyhow::{Result, anyhow};
use datafusion::scalar::ScalarValue;

/// Encode a projected row into deterministic bytes for DBSP keys.
pub fn encode_projected_row_key(columns: &[ScalarValue]) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(64);
    let count = u32::try_from(columns.len()).map_err(|_| anyhow!("too many columns in MV key"))?;
    buf.extend_from_slice(&count.to_le_bytes());
    for value in columns {
        match value {
            ScalarValue::Null => {
                // Backward-compatible untyped NULL marker.
                buf.push(0x00);
            }
            ScalarValue::Int64(Some(v)) => {
                buf.push(0x01);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            ScalarValue::Int64(None) => {
                buf.push(0x05);
            }
            ScalarValue::Utf8(Some(text)) => {
                buf.push(0x02);
                let bytes = text.as_bytes();
                let len = u32::try_from(bytes.len())
                    .map_err(|_| anyhow!("utf8 value too large for MV key"))?;
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            ScalarValue::Utf8(None) => {
                buf.push(0x06);
            }
            ScalarValue::TimestampMillisecond(Some(v), _) => {
                buf.push(0x03);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            ScalarValue::TimestampMillisecond(None, _) => {
                buf.push(0x07);
            }
            ScalarValue::Boolean(Some(flag)) => {
                buf.push(0x04);
                buf.push(if *flag { 1 } else { 0 });
            }
            ScalarValue::Boolean(None) => {
                buf.push(0x08);
            }
            other => return Err(anyhow!("unsupported ScalarValue in MV key: {other:?}")),
        }
    }
    Ok(buf)
}

pub fn decode_projected_row_key(bytes: &[u8]) -> Result<Vec<ScalarValue>> {
    if bytes.len() < 4 {
        return Err(anyhow!("encoded key too short"));
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut cursor = 4;
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        if cursor >= bytes.len() {
            return Err(anyhow!("unexpected end of key while decoding tag"));
        }
        let tag = bytes[cursor];
        cursor += 1;
        match tag {
            0x00 => {
                columns.push(ScalarValue::Null);
            }
            0x01 => {
                let end = cursor + 8;
                let chunk = bytes
                    .get(cursor..end)
                    .ok_or_else(|| anyhow!("truncated int64"))?;
                let value = i64::from_le_bytes(chunk.try_into().unwrap());
                columns.push(ScalarValue::Int64(Some(value)));
                cursor = end;
            }
            0x02 => {
                let len_bytes = bytes
                    .get(cursor..cursor + 4)
                    .ok_or_else(|| anyhow!("truncated string length"))?;
                let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
                cursor += 4;
                let end = cursor + len;
                let chunk = bytes
                    .get(cursor..end)
                    .ok_or_else(|| anyhow!("truncated string payload"))?;
                let text = std::str::from_utf8(chunk)
                    .map_err(|err| anyhow!("utf8 decode error: {err}"))?;
                columns.push(ScalarValue::Utf8(Some(text.to_string())));
                cursor = end;
            }
            0x03 => {
                let end = cursor + 8;
                let chunk = bytes
                    .get(cursor..end)
                    .ok_or_else(|| anyhow!("truncated timestamp"))?;
                let value = i64::from_le_bytes(chunk.try_into().unwrap());
                columns.push(ScalarValue::TimestampMillisecond(Some(value), None));
                cursor = end;
            }
            0x04 => {
                let flag = *bytes
                    .get(cursor)
                    .ok_or_else(|| anyhow!("missing boolean payload"))?;
                columns.push(ScalarValue::Boolean(Some(flag != 0)));
                cursor += 1;
            }
            0x05 => {
                columns.push(ScalarValue::Int64(None));
            }
            0x06 => {
                columns.push(ScalarValue::Utf8(None));
            }
            0x07 => {
                columns.push(ScalarValue::TimestampMillisecond(None, None));
            }
            0x08 => {
                columns.push(ScalarValue::Boolean(None));
            }
            _ => return Err(anyhow!("unknown column tag {tag:#x} in MV key")),
        }
    }
    Ok(columns)
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
    fn round_trips_rows() {
        let row = vec![
            ScalarValue::Int64(Some(10)),
            ScalarValue::Utf8(Some("abc".into())),
            ScalarValue::TimestampMillisecond(Some(1234), None),
            ScalarValue::Boolean(Some(false)),
        ];
        let encoded = encode_projected_row_key(&row).expect("encode");
        let decoded = decode_projected_row_key(&encoded).expect("decode");
        assert_eq!(row, decoded);
    }

    #[test]
    fn encodes_null_values() {
        let row = vec![ScalarValue::Null, ScalarValue::Int64(None)];
        let encoded = encode_projected_row_key(&row).expect("encode");
        let decoded = decode_projected_row_key(&encoded).expect("decode");
        assert_eq!(decoded, row);
    }
}
