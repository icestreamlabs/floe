use anyhow::{Result, anyhow};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum EncodedRowScalar {
    Int64(i64),
    Utf8(String),
    TimestampMillis(i64),
    Bool(bool),
    DateDays(i32),
    Decimal128(i128),
}

pub fn extract_encoded_row_columns(
    bytes: &[u8],
    indices: &[usize],
    require_non_null: bool,
) -> Result<Option<Vec<u8>>> {
    let count = encoded_row_column_count(bytes)?;
    if indices.iter().any(|index| *index >= count) {
        return Err(anyhow!(
            "encoded row has {count} columns but a requested index was out of bounds"
        ));
    }

    let mut requested = indices
        .iter()
        .enumerate()
        .map(|(slot, index)| (*index, slot))
        .collect::<Vec<_>>();
    requested.sort_unstable_by_key(|(index, _)| *index);

    let mut spans = vec![(0usize, 0usize); indices.len()];
    let mut request_idx = 0usize;
    let mut cursor = 4usize;

    for column_idx in 0..count {
        let start = cursor;
        let tag = *bytes
            .get(cursor)
            .ok_or_else(|| anyhow!("unexpected end of key while decoding tag"))?;
        cursor += 1;
        cursor = encoded_field_end(bytes, cursor, tag)?;
        let end = cursor;

        while request_idx < requested.len() && requested[request_idx].0 == column_idx {
            if require_non_null && is_null_field_tag(tag) {
                return Ok(None);
            }
            let slot = requested[request_idx].1;
            spans[slot] = (start, end);
            request_idx += 1;
        }
    }

    let total_payload_len = spans.iter().map(|(start, end)| end - start).sum::<usize>();
    let selected_count =
        u32::try_from(indices.len()).map_err(|_| anyhow!("too many columns in MV key"))?;
    let mut out = Vec::with_capacity(4 + total_payload_len);
    out.extend_from_slice(&selected_count.to_le_bytes());
    for (start, end) in spans {
        out.extend_from_slice(&bytes[start..end]);
    }
    Ok(Some(out))
}

pub(crate) fn extract_encoded_row_scalar(
    bytes: &[u8],
    target_index: usize,
) -> Result<Option<EncodedRowScalar>> {
    let mut values = extract_encoded_row_scalars(bytes, &[target_index])?;
    values
        .pop()
        .ok_or_else(|| anyhow!("missing extracted scalar for index {target_index}"))
}

pub(crate) fn extract_encoded_row_scalars(
    bytes: &[u8],
    indices: &[usize],
) -> Result<Vec<Option<EncodedRowScalar>>> {
    let mut decoded = Vec::new();
    decode_encoded_row_scalars_into(bytes, indices, &mut decoded)?;
    Ok(decoded)
}

pub(crate) fn decode_encoded_row_scalars_into(
    bytes: &[u8],
    indices: &[usize],
    decoded: &mut Vec<Option<EncodedRowScalar>>,
) -> Result<()> {
    decoded.clear();
    decoded.resize(indices.len(), None);

    let count = encoded_row_column_count(bytes)?;
    if indices.is_empty() {
        return Ok(());
    }
    if indices.iter().any(|index| *index >= count) {
        return Err(anyhow!(
            "encoded row has {count} columns but a requested index was out of bounds"
        ));
    }

    let mut requested = indices
        .iter()
        .enumerate()
        .map(|(slot, index)| (*index, slot))
        .collect::<Vec<_>>();
    requested.sort_unstable_by_key(|(index, _)| *index);

    let mut request_idx = 0usize;
    let mut cursor = 4usize;

    for column_idx in 0..count {
        let tag = *bytes
            .get(cursor)
            .ok_or_else(|| anyhow!("unexpected end of key while decoding tag"))?;
        cursor += 1;
        let value_cursor = cursor;
        let end = encoded_field_end(bytes, cursor, tag)?;

        while request_idx < requested.len() && requested[request_idx].0 == column_idx {
            let slot = requested[request_idx].1;
            decoded[slot] = decode_encoded_scalar(bytes, value_cursor, tag)?;
            request_idx += 1;
        }
        cursor = end;
    }

    Ok(())
}

pub fn decode_all_encoded_row_scalars(bytes: &[u8]) -> Result<Vec<Option<EncodedRowScalar>>> {
    let mut decoded = Vec::new();
    decode_all_encoded_row_scalars_into(bytes, &mut decoded)?;
    Ok(decoded)
}

pub(crate) fn decode_all_encoded_row_scalars_into(
    bytes: &[u8],
    decoded: &mut Vec<Option<EncodedRowScalar>>,
) -> Result<()> {
    let count = encoded_row_column_count(bytes)?;
    decoded.clear();
    decoded.resize(count, None);

    let mut cursor = 4usize;
    for slot in decoded.iter_mut() {
        let tag = *bytes
            .get(cursor)
            .ok_or_else(|| anyhow!("unexpected end of key while decoding tag"))?;
        cursor += 1;
        let value_cursor = cursor;
        let end = encoded_field_end(bytes, cursor, tag)?;
        *slot = decode_encoded_scalar(bytes, value_cursor, tag)?;
        cursor = end;
    }
    Ok(())
}

pub fn extract_encoded_row_i64_like_column(
    bytes: &[u8],
    target_index: usize,
) -> Result<Option<i64>> {
    match extract_encoded_row_scalar(bytes, target_index)? {
        Some(EncodedRowScalar::Int64(value) | EncodedRowScalar::TimestampMillis(value)) => {
            Ok(Some(value))
        }
        Some(other) => Err(anyhow!(
            "expected i64-like encoded field at index {target_index}, found {other:?}"
        )),
        None => Ok(None),
    }
}

pub fn extract_encoded_row_columns_and_i64_like_column(
    bytes: &[u8],
    indices: &[usize],
    target_index: usize,
    require_non_null: bool,
) -> Result<Option<(Vec<u8>, i64)>> {
    let count = encoded_row_column_count(bytes)?;
    if indices.iter().any(|index| *index >= count) || target_index >= count {
        return Err(anyhow!(
            "encoded row has {count} columns but a requested index was out of bounds"
        ));
    }

    let mut requested = indices
        .iter()
        .enumerate()
        .map(|(slot, index)| (*index, slot))
        .collect::<Vec<_>>();
    requested.sort_unstable_by_key(|(index, _)| *index);

    let mut spans = vec![(0usize, 0usize); indices.len()];
    let mut request_idx = 0usize;
    let mut target_value = None;
    let mut cursor = 4usize;

    for column_idx in 0..count {
        let start = cursor;
        let tag = *bytes
            .get(cursor)
            .ok_or_else(|| anyhow!("unexpected end of key while decoding tag"))?;
        cursor += 1;
        let value_cursor = cursor;
        cursor = encoded_field_end(bytes, cursor, tag)?;
        let end = cursor;

        while request_idx < requested.len() && requested[request_idx].0 == column_idx {
            if require_non_null && is_null_field_tag(tag) {
                return Ok(None);
            }
            let slot = requested[request_idx].1;
            spans[slot] = (start, end);
            request_idx += 1;
        }

        if column_idx == target_index {
            target_value = match decode_encoded_scalar(bytes, value_cursor, tag)? {
                Some(EncodedRowScalar::Int64(value) | EncodedRowScalar::TimestampMillis(value)) => {
                    Some(value)
                }
                Some(other) => {
                    return Err(anyhow!(
                        "expected i64-like encoded field at index {target_index}, found {other:?}"
                    ));
                }
                None => return Ok(None),
            };
        }
    }

    let total_payload_len = spans.iter().map(|(start, end)| end - start).sum::<usize>();
    let selected_count =
        u32::try_from(indices.len()).map_err(|_| anyhow!("too many columns in MV key"))?;
    let mut out = Vec::with_capacity(4 + total_payload_len);
    out.extend_from_slice(&selected_count.to_le_bytes());
    for (start, end) in spans {
        out.extend_from_slice(&bytes[start..end]);
    }

    Ok(target_value.map(|value| (out, value)))
}

pub fn concat_encoded_rows(left: &[u8], right: &[u8]) -> Result<Vec<u8>> {
    let left_count = encoded_row_column_count(left)?;
    let right_count = encoded_row_column_count(right)?;
    let total_count = left_count
        .checked_add(right_count)
        .ok_or_else(|| anyhow!("combined row has too many columns"))?;
    let total_count =
        u32::try_from(total_count).map_err(|_| anyhow!("too many columns in MV key"))?;

    let mut out = Vec::with_capacity(left.len() + right.len() - 4);
    out.extend_from_slice(&total_count.to_le_bytes());
    out.extend_from_slice(&left[4..]);
    out.extend_from_slice(&right[4..]);
    Ok(out)
}

pub(crate) fn encoded_row_column_count(bytes: &[u8]) -> Result<usize> {
    if bytes.len() < 4 {
        return Err(anyhow!("encoded key too short"));
    }
    let count = u32::from_le_bytes(read_fixed::<4>(bytes, 0, "encoded column count")?) as usize;
    let mut cursor = 4usize;
    for _ in 0..count {
        let tag = *bytes
            .get(cursor)
            .ok_or_else(|| anyhow!("unexpected end of key while decoding tag"))?;
        cursor += 1;
        cursor = encoded_field_end(bytes, cursor, tag)?;
    }
    Ok(count)
}

fn encoded_field_end(bytes: &[u8], cursor: usize, tag: u8) -> Result<usize> {
    match tag {
        0x00 | 0x05 | 0x06 | 0x07 | 0x08 | 0x0A | 0x0C => Ok(cursor),
        0x01 | 0x03 => {
            let end = cursor
                .checked_add(8)
                .ok_or_else(|| anyhow!("fixed-width value offset overflow"))?;
            bytes
                .get(cursor..end)
                .ok_or_else(|| anyhow!("truncated fixed-width value"))?;
            Ok(end)
        }
        0x02 => {
            let len = u32::from_le_bytes(read_fixed::<4>(bytes, cursor, "string length")?) as usize;
            let start = cursor
                .checked_add(4)
                .ok_or_else(|| anyhow!("string payload offset overflow"))?;
            let end = start
                .checked_add(len)
                .ok_or_else(|| anyhow!("string payload length overflow"))?;
            bytes
                .get(start..end)
                .ok_or_else(|| anyhow!("truncated string payload"))?;
            Ok(end)
        }
        0x04 => {
            bytes
                .get(cursor)
                .ok_or_else(|| anyhow!("missing boolean payload"))?;
            cursor
                .checked_add(1)
                .ok_or_else(|| anyhow!("boolean payload offset overflow"))
        }
        0x09 => {
            let end = cursor
                .checked_add(4)
                .ok_or_else(|| anyhow!("date32 value offset overflow"))?;
            bytes
                .get(cursor..end)
                .ok_or_else(|| anyhow!("truncated date32 value"))?;
            Ok(end)
        }
        0x0B => {
            let end = cursor
                .checked_add(16)
                .ok_or_else(|| anyhow!("decimal128 value offset overflow"))?;
            bytes
                .get(cursor..end)
                .ok_or_else(|| anyhow!("truncated decimal128 value"))?;
            Ok(end)
        }
        _ => Err(anyhow!("unknown column tag {tag:#x} in MV key")),
    }
}

fn decode_encoded_scalar(bytes: &[u8], cursor: usize, tag: u8) -> Result<Option<EncodedRowScalar>> {
    match tag {
        0x00 | 0x05 | 0x06 | 0x07 | 0x08 | 0x0A | 0x0C => Ok(None),
        0x01 => {
            let chunk = read_fixed::<8>(bytes, cursor, "int64")?;
            let value = i64::from_le_bytes(chunk);
            Ok(Some(EncodedRowScalar::Int64(value)))
        }
        0x02 => {
            let len = u32::from_le_bytes(read_fixed::<4>(bytes, cursor, "string length")?) as usize;
            let start = cursor
                .checked_add(4)
                .ok_or_else(|| anyhow!("string payload offset overflow"))?;
            let end = start
                .checked_add(len)
                .ok_or_else(|| anyhow!("string payload length overflow"))?;
            let chunk = bytes
                .get(start..end)
                .ok_or_else(|| anyhow!("truncated string payload"))?;
            let text =
                std::str::from_utf8(chunk).map_err(|err| anyhow!("utf8 decode error: {err}"))?;
            Ok(Some(EncodedRowScalar::Utf8(text.to_string())))
        }
        0x03 => {
            let chunk = read_fixed::<8>(bytes, cursor, "timestamp")?;
            let value = i64::from_le_bytes(chunk);
            Ok(Some(EncodedRowScalar::TimestampMillis(value)))
        }
        0x04 => {
            let flag = *bytes
                .get(cursor)
                .ok_or_else(|| anyhow!("missing boolean payload"))?;
            Ok(Some(EncodedRowScalar::Bool(flag != 0)))
        }
        0x09 => {
            let chunk = read_fixed::<4>(bytes, cursor, "date32 value")?;
            let value = i32::from_le_bytes(chunk);
            Ok(Some(EncodedRowScalar::DateDays(value)))
        }
        0x0B => {
            let chunk = read_fixed::<16>(bytes, cursor, "decimal128 value")?;
            let value = i128::from_le_bytes(chunk);
            Ok(Some(EncodedRowScalar::Decimal128(value)))
        }
        _ => Err(anyhow!("unknown column tag {tag:#x} in MV key")),
    }
}

fn read_fixed<const N: usize>(bytes: &[u8], cursor: usize, label: &str) -> Result<[u8; N]> {
    let end = cursor
        .checked_add(N)
        .ok_or_else(|| anyhow!("{label} offset overflow"))?;
    let chunk = bytes
        .get(cursor..end)
        .ok_or_else(|| anyhow!("truncated {label}"))?;
    let mut out = [0_u8; N];
    out.copy_from_slice(chunk);
    Ok(out)
}

fn is_null_field_tag(tag: u8) -> bool {
    matches!(tag, 0x00 | 0x05 | 0x06 | 0x07 | 0x08 | 0x0A | 0x0C)
}

#[cfg(test)]
mod tests {
    use super::*;

    enum TestEncodedField<'a> {
        Null,
        Int64(i64),
        Int64Null,
        Utf8(&'a str),
        Utf8Null,
        TimestampMillis(i64),
        TimestampNull,
        Bool(bool),
        BoolNull,
    }

    fn encode_test_row(fields: &[TestEncodedField<'_>]) -> Vec<u8> {
        let count = u32::try_from(fields.len()).expect("field count fits u32");
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&count.to_le_bytes());
        for field in fields {
            match field {
                TestEncodedField::Null => encoded.push(0x00),
                TestEncodedField::Int64(value) => {
                    encoded.push(0x01);
                    encoded.extend_from_slice(&value.to_le_bytes());
                }
                TestEncodedField::Int64Null => encoded.push(0x05),
                TestEncodedField::Utf8(value) => {
                    encoded.push(0x02);
                    let bytes = value.as_bytes();
                    let len = u32::try_from(bytes.len()).expect("utf8 length fits u32");
                    encoded.extend_from_slice(&len.to_le_bytes());
                    encoded.extend_from_slice(bytes);
                }
                TestEncodedField::Utf8Null => encoded.push(0x06),
                TestEncodedField::TimestampMillis(value) => {
                    encoded.push(0x03);
                    encoded.extend_from_slice(&value.to_le_bytes());
                }
                TestEncodedField::TimestampNull => encoded.push(0x07),
                TestEncodedField::Bool(value) => {
                    encoded.push(0x04);
                    encoded.push(if *value { 1 } else { 0 });
                }
                TestEncodedField::BoolNull => encoded.push(0x08),
            }
        }
        encoded
    }

    fn decode_test_row(encoded: &[u8]) -> Vec<Option<EncodedRowScalar>> {
        let mut decoded = Vec::new();
        decode_all_encoded_row_scalars_into(encoded, &mut decoded).expect("decode");
        decoded
    }

    #[test]
    fn encodes_simple_rows() {
        let encoded = encode_test_row(&[
            TestEncodedField::Int64(42),
            TestEncodedField::Utf8("abc"),
            TestEncodedField::Bool(true),
        ]);
        assert!(!encoded.is_empty());
    }

    #[test]
    fn round_trips_rows() {
        let encoded = encode_test_row(&[
            TestEncodedField::Int64(10),
            TestEncodedField::Utf8("abc"),
            TestEncodedField::TimestampMillis(1234),
            TestEncodedField::Bool(false),
        ]);
        let decoded = decode_test_row(&encoded);
        assert_eq!(
            decoded,
            vec![
                Some(EncodedRowScalar::Int64(10)),
                Some(EncodedRowScalar::Utf8("abc".into())),
                Some(EncodedRowScalar::TimestampMillis(1234)),
                Some(EncodedRowScalar::Bool(false))
            ]
        );
    }

    #[test]
    fn encodes_null_values() {
        let encoded = encode_test_row(&[TestEncodedField::Null, TestEncodedField::Int64Null]);
        let decoded = decode_test_row(&encoded);
        assert_eq!(decoded, vec![None, None]);
    }

    #[test]
    fn extracts_selected_columns_without_full_decode() {
        let encoded = encode_test_row(&[
            TestEncodedField::Int64(10),
            TestEncodedField::Utf8("abc"),
            TestEncodedField::TimestampMillis(1234),
            TestEncodedField::Bool(false),
        ]);
        let selected = extract_encoded_row_columns(&encoded, &[3, 0], true)
            .expect("extract")
            .expect("non-null key");
        let decoded = decode_test_row(&selected);
        assert_eq!(
            decoded,
            vec![
                Some(EncodedRowScalar::Bool(false)),
                Some(EncodedRowScalar::Int64(10))
            ]
        );
    }

    #[test]
    fn selecting_null_key_column_returns_none_when_non_null_required() {
        let encoded =
            encode_test_row(&[TestEncodedField::Int64Null, TestEncodedField::Utf8("abc")]);
        let selected =
            extract_encoded_row_columns(&encoded, &[0], true).expect("extract nullable key");
        assert!(selected.is_none());
    }

    #[test]
    fn concatenates_encoded_rows_without_decode_reencode() {
        let left = encode_test_row(&[TestEncodedField::Int64(10), TestEncodedField::Utf8("left")]);
        let right = encode_test_row(&[
            TestEncodedField::Bool(true),
            TestEncodedField::TimestampMillis(55),
        ]);

        let combined = concat_encoded_rows(&left, &right).expect("concat");
        let decoded = decode_test_row(&combined);
        assert_eq!(
            decoded,
            vec![
                Some(EncodedRowScalar::Int64(10)),
                Some(EncodedRowScalar::Utf8("left".into())),
                Some(EncodedRowScalar::Bool(true)),
                Some(EncodedRowScalar::TimestampMillis(55)),
            ]
        );
    }

    #[test]
    fn encodes_typed_null_variants() {
        let encoded = encode_test_row(&[
            TestEncodedField::Utf8Null,
            TestEncodedField::TimestampNull,
            TestEncodedField::BoolNull,
        ]);
        let decoded = decode_test_row(&encoded);
        assert_eq!(decoded, vec![None, None, None]);
    }
}
