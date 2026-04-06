use anyhow::{Result, anyhow};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EncodedRowProjectionSource {
    Left,
    Right,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EncodedRowProjectionColumn {
    pub source: EncodedRowProjectionSource,
    pub index: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum EncodedRowScalar {
    Int64(i64),
    Utf8(String),
    TimestampMillis(i64),
    Bool(bool),
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
    if indices.is_empty() {
        return Ok(Vec::new());
    }

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

    let mut decoded = vec![None; indices.len()];
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

    Ok(decoded)
}

pub fn decode_all_encoded_row_scalars(bytes: &[u8]) -> Result<Vec<Option<EncodedRowScalar>>> {
    let count = encoded_row_column_count(bytes)?;
    let indices = (0..count).collect::<Vec<_>>();
    extract_encoded_row_scalars(bytes, indices.as_slice())
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

pub(crate) fn project_joined_encoded_rows(
    left: &[u8],
    right: &[u8],
    columns: &[EncodedRowProjectionColumn],
) -> Result<Vec<u8>> {
    let mut left_requests = Vec::new();
    let mut right_requests = Vec::new();
    for (output_idx, column) in columns.iter().enumerate() {
        match column.source {
            EncodedRowProjectionSource::Left => left_requests.push((column.index, output_idx)),
            EncodedRowProjectionSource::Right => right_requests.push((column.index, output_idx)),
        }
    }

    let mut spans_by_output = vec![(0usize, 0usize); columns.len()];
    let mut total_payload_len = 0usize;
    for (output_idx, start, end) in collect_encoded_field_spans(left, &left_requests)? {
        spans_by_output[output_idx] = (start, end);
        total_payload_len += end - start;
    }
    for (output_idx, start, end) in collect_encoded_field_spans(right, &right_requests)? {
        spans_by_output[output_idx] = (start, end);
        total_payload_len += end - start;
    }

    let projected_count =
        u32::try_from(columns.len()).map_err(|_| anyhow!("too many columns in MV key"))?;
    let mut out = Vec::with_capacity(4 + total_payload_len);
    out.extend_from_slice(&projected_count.to_le_bytes());
    for (output_idx, (start, end)) in spans_by_output.into_iter().enumerate() {
        let source_bytes = match columns[output_idx].source {
            EncodedRowProjectionSource::Left => left,
            EncodedRowProjectionSource::Right => right,
        };
        out.extend_from_slice(&source_bytes[start..end]);
    }
    Ok(out)
}

fn encoded_row_column_count(bytes: &[u8]) -> Result<usize> {
    if bytes.len() < 4 {
        return Err(anyhow!("encoded key too short"));
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
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
        0x00 | 0x05 | 0x06 | 0x07 | 0x08 => Ok(cursor),
        0x01 | 0x03 => {
            let end = cursor + 8;
            bytes
                .get(cursor..end)
                .ok_or_else(|| anyhow!("truncated fixed-width value"))?;
            Ok(end)
        }
        0x02 => {
            let len_bytes = bytes
                .get(cursor..cursor + 4)
                .ok_or_else(|| anyhow!("truncated string length"))?;
            let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            let end = cursor + 4 + len;
            bytes
                .get(cursor + 4..end)
                .ok_or_else(|| anyhow!("truncated string payload"))?;
            Ok(end)
        }
        0x04 => {
            bytes
                .get(cursor)
                .ok_or_else(|| anyhow!("missing boolean payload"))?;
            Ok(cursor + 1)
        }
        _ => Err(anyhow!("unknown column tag {tag:#x} in MV key")),
    }
}

fn decode_encoded_scalar(bytes: &[u8], cursor: usize, tag: u8) -> Result<Option<EncodedRowScalar>> {
    match tag {
        0x00 | 0x05 | 0x06 | 0x07 | 0x08 => Ok(None),
        0x01 => {
            let end = cursor + 8;
            let chunk = bytes
                .get(cursor..end)
                .ok_or_else(|| anyhow!("truncated int64"))?;
            let value = i64::from_le_bytes(chunk.try_into().unwrap());
            Ok(Some(EncodedRowScalar::Int64(value)))
        }
        0x02 => {
            let len_bytes = bytes
                .get(cursor..cursor + 4)
                .ok_or_else(|| anyhow!("truncated string length"))?;
            let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            let start = cursor + 4;
            let end = start + len;
            let chunk = bytes
                .get(start..end)
                .ok_or_else(|| anyhow!("truncated string payload"))?;
            let text =
                std::str::from_utf8(chunk).map_err(|err| anyhow!("utf8 decode error: {err}"))?;
            Ok(Some(EncodedRowScalar::Utf8(text.to_string())))
        }
        0x03 => {
            let end = cursor + 8;
            let chunk = bytes
                .get(cursor..end)
                .ok_or_else(|| anyhow!("truncated timestamp"))?;
            let value = i64::from_le_bytes(chunk.try_into().unwrap());
            Ok(Some(EncodedRowScalar::TimestampMillis(value)))
        }
        0x04 => {
            let flag = *bytes
                .get(cursor)
                .ok_or_else(|| anyhow!("missing boolean payload"))?;
            Ok(Some(EncodedRowScalar::Bool(flag != 0)))
        }
        _ => Err(anyhow!("unknown column tag {tag:#x} in MV key")),
    }
}

fn is_null_field_tag(tag: u8) -> bool {
    matches!(tag, 0x00 | 0x05 | 0x06 | 0x07 | 0x08)
}

fn collect_encoded_field_spans(
    bytes: &[u8],
    requests: &[(usize, usize)],
) -> Result<Vec<(usize, usize, usize)>> {
    let count = encoded_row_column_count(bytes)?;
    if requests.iter().any(|(index, _)| *index >= count) {
        return Err(anyhow!(
            "encoded row has {count} columns but a requested index was out of bounds"
        ));
    }

    let mut requested = requests.to_vec();
    requested.sort_unstable_by_key(|(index, _)| *index);

    let mut spans = Vec::with_capacity(requested.len());
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
            spans.push((requested[request_idx].1, start, end));
            request_idx += 1;
        }
    }

    Ok(spans)
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
        let decoded = decode_all_encoded_row_scalars(&encoded).expect("decode");
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
        let decoded = decode_all_encoded_row_scalars(&encoded).expect("decode");
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
        let decoded = decode_all_encoded_row_scalars(&selected).expect("decode");
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
        let decoded = decode_all_encoded_row_scalars(&combined).expect("decode combined");
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
    fn projects_joined_rows_without_full_decode() {
        let left = encode_test_row(&[
            TestEncodedField::Int64(10),
            TestEncodedField::Utf8("left"),
            TestEncodedField::Bool(true),
        ]);
        let right = encode_test_row(&[
            TestEncodedField::TimestampMillis(55),
            TestEncodedField::Int64(99),
        ]);

        let projected = project_joined_encoded_rows(
            &left,
            &right,
            &[
                EncodedRowProjectionColumn {
                    source: EncodedRowProjectionSource::Right,
                    index: 1,
                },
                EncodedRowProjectionColumn {
                    source: EncodedRowProjectionSource::Left,
                    index: 0,
                },
                EncodedRowProjectionColumn {
                    source: EncodedRowProjectionSource::Right,
                    index: 0,
                },
                EncodedRowProjectionColumn {
                    source: EncodedRowProjectionSource::Left,
                    index: 2,
                },
            ],
        )
        .expect("project");
        let decoded = decode_all_encoded_row_scalars(&projected).expect("decode projected");

        assert_eq!(
            decoded,
            vec![
                Some(EncodedRowScalar::Int64(99)),
                Some(EncodedRowScalar::Int64(10)),
                Some(EncodedRowScalar::TimestampMillis(55)),
                Some(EncodedRowScalar::Bool(true)),
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
        let decoded = decode_all_encoded_row_scalars(&encoded).expect("decode null variants");
        assert_eq!(decoded, vec![None, None, None]);
    }
}
