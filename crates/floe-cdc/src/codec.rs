use anyhow::{Context, Result, bail};
use floe_cdc_core::{CdcColumnarColumn, CdcColumnarRowBatch, CdcRow};
use floe_core::RowValue;

use crate::json::decode_json_value;

pub(crate) const CDC_ROW_STATE_MAGIC: &[u8; 8] = b"FCDCRW1\0";
const CDC_ROW_VALUE_NULL: u8 = 0;
const CDC_ROW_VALUE_INT64: u8 = 1;
const CDC_ROW_VALUE_BOOL: u8 = 2;
const CDC_ROW_VALUE_UTF8: u8 = 3;
const CDC_ROW_VALUE_TIMESTAMP_MILLIS: u8 = 4;
const CDC_ROW_VALUE_DATE_DAYS: u8 = 5;
const CDC_ROW_VALUE_NUMERIC: u8 = 6;
const CDC_ROW_VALUE_DECIMAL128: u8 = 7;

pub(crate) fn encode_cdc_row_state(row: &CdcRow) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(CDC_ROW_STATE_MAGIC.len() + 4 + row.values().len() * 9);
    out.extend_from_slice(CDC_ROW_STATE_MAGIC);
    push_u32(&mut out, row.values().len(), "CDC row value count")?;
    for value in row.values() {
        encode_cdc_row_value(&mut out, value.as_ref())?;
    }
    Ok(out)
}

pub(crate) fn encode_cdc_columnar_row_state(
    rows: &CdcColumnarRowBatch,
    row_idx: usize,
) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(CDC_ROW_STATE_MAGIC.len() + 4 + rows.columns().len() * 9);
    out.extend_from_slice(CDC_ROW_STATE_MAGIC);
    push_u32(&mut out, rows.columns().len(), "CDC row value count")?;
    for column in rows.columns() {
        encode_cdc_columnar_row_value(&mut out, column, row_idx)?;
    }
    Ok(out)
}

fn encode_cdc_row_value(out: &mut Vec<u8>, value: Option<&RowValue>) -> Result<()> {
    match value {
        None => out.push(CDC_ROW_VALUE_NULL),
        Some(RowValue::Int64(value)) => {
            out.push(CDC_ROW_VALUE_INT64);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Some(RowValue::Bool(value)) => {
            out.push(CDC_ROW_VALUE_BOOL);
            out.push(u8::from(*value));
        }
        Some(RowValue::Utf8(value)) => {
            out.push(CDC_ROW_VALUE_UTF8);
            push_u32(out, value.len(), "CDC UTF-8 value length")?;
            out.extend_from_slice(value.as_bytes());
        }
        Some(RowValue::TimestampMillis(value)) => {
            out.push(CDC_ROW_VALUE_TIMESTAMP_MILLIS);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Some(RowValue::DateDays(value)) => {
            out.push(CDC_ROW_VALUE_DATE_DAYS);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Some(RowValue::Decimal128(value)) => {
            out.push(CDC_ROW_VALUE_DECIMAL128);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Some(RowValue::Numeric(value)) => {
            out.push(CDC_ROW_VALUE_NUMERIC);
            push_u32(out, value.len(), "CDC numeric value length")?;
            out.extend_from_slice(value.as_bytes());
        }
    }
    Ok(())
}

fn encode_cdc_columnar_row_value(
    out: &mut Vec<u8>,
    column: &CdcColumnarColumn,
    row_idx: usize,
) -> Result<()> {
    match column {
        CdcColumnarColumn::Int64(values) => match values.get(row_idx) {
            Some(Some(value)) => {
                out.push(CDC_ROW_VALUE_INT64);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Some(None) => out.push(CDC_ROW_VALUE_NULL),
            None => bail!("CDC columnar row index {row_idx} out of bounds"),
        },
        CdcColumnarColumn::Bool(values) => match values.get(row_idx) {
            Some(Some(value)) => {
                out.push(CDC_ROW_VALUE_BOOL);
                out.push(u8::from(*value));
            }
            Some(None) => out.push(CDC_ROW_VALUE_NULL),
            None => bail!("CDC columnar row index {row_idx} out of bounds"),
        },
        CdcColumnarColumn::Utf8(values) => match values.get(row_idx) {
            Some(Some(value)) => {
                out.push(CDC_ROW_VALUE_UTF8);
                push_u32(out, value.len(), "CDC UTF-8 value length")?;
                out.extend_from_slice(value.as_bytes());
            }
            Some(None) => out.push(CDC_ROW_VALUE_NULL),
            None => bail!("CDC columnar row index {row_idx} out of bounds"),
        },
        CdcColumnarColumn::TimestampMillis(values) => match values.get(row_idx) {
            Some(Some(value)) => {
                out.push(CDC_ROW_VALUE_TIMESTAMP_MILLIS);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Some(None) => out.push(CDC_ROW_VALUE_NULL),
            None => bail!("CDC columnar row index {row_idx} out of bounds"),
        },
        CdcColumnarColumn::DateDays(values) => match values.get(row_idx) {
            Some(Some(value)) => {
                out.push(CDC_ROW_VALUE_DATE_DAYS);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Some(None) => out.push(CDC_ROW_VALUE_NULL),
            None => bail!("CDC columnar row index {row_idx} out of bounds"),
        },
        CdcColumnarColumn::Decimal128 { values, .. } => match values.get(row_idx) {
            Some(Some(value)) => {
                out.push(CDC_ROW_VALUE_DECIMAL128);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Some(None) => out.push(CDC_ROW_VALUE_NULL),
            None => bail!("CDC columnar row index {row_idx} out of bounds"),
        },
        CdcColumnarColumn::Numeric(values) => match values.get(row_idx) {
            Some(Some(value)) => {
                out.push(CDC_ROW_VALUE_NUMERIC);
                push_u32(out, value.len(), "CDC numeric value length")?;
                out.extend_from_slice(value.as_bytes());
            }
            Some(None) => out.push(CDC_ROW_VALUE_NULL),
            None => bail!("CDC columnar row index {row_idx} out of bounds"),
        },
    }
    Ok(())
}

pub(crate) fn decode_cdc_row_state(bytes: &[u8]) -> Result<CdcRow> {
    if !bytes.starts_with(CDC_ROW_STATE_MAGIC) {
        return decode_json_value(bytes, "legacy CDC row state");
    }
    let mut cursor = CdcRowStateCursor::new(&bytes[CDC_ROW_STATE_MAGIC.len()..]);
    let value_count = cursor.read_u32()? as usize;
    let mut values = Vec::with_capacity(value_count);
    for _ in 0..value_count {
        values.push(cursor.read_value()?);
    }
    if !cursor.is_empty() {
        bail!(
            "CDC row state has {} trailing bytes",
            cursor.remaining_len()
        );
    }
    CdcRow::new(values)
}

fn push_u32(out: &mut Vec<u8>, value: usize, label: &str) -> Result<()> {
    let value = u32::try_from(value).with_context(|| format!("{label} exceeds u32"))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

struct CdcRowStateCursor<'a> {
    bytes: &'a [u8],
}

impl<'a> CdcRowStateCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    fn remaining_len(&self) -> usize {
        self.bytes.len()
    }

    fn read_value(&mut self) -> Result<Option<RowValue>> {
        let tag = self.read_u8()?;
        match tag {
            CDC_ROW_VALUE_NULL => Ok(None),
            CDC_ROW_VALUE_INT64 => Ok(Some(RowValue::Int64(self.read_i64()?))),
            CDC_ROW_VALUE_BOOL => match self.read_u8()? {
                0 => Ok(Some(RowValue::Bool(false))),
                1 => Ok(Some(RowValue::Bool(true))),
                other => bail!("invalid CDC bool value byte {other}"),
            },
            CDC_ROW_VALUE_UTF8 => {
                let len = self.read_u32()? as usize;
                let bytes = self.take(len)?;
                let value = std::str::from_utf8(bytes)
                    .context("decode CDC UTF-8 row value")?
                    .to_string();
                Ok(Some(RowValue::Utf8(value)))
            }
            CDC_ROW_VALUE_TIMESTAMP_MILLIS => Ok(Some(RowValue::TimestampMillis(self.read_i64()?))),
            CDC_ROW_VALUE_DATE_DAYS => Ok(Some(RowValue::DateDays(self.read_i32()?))),
            CDC_ROW_VALUE_DECIMAL128 => Ok(Some(RowValue::Decimal128(self.read_i128()?))),
            CDC_ROW_VALUE_NUMERIC => {
                let len = self.read_u32()? as usize;
                let bytes = self.take(len)?;
                let value = std::str::from_utf8(bytes)
                    .context("decode CDC numeric row value")?
                    .to_string();
                Ok(Some(RowValue::Numeric(value)))
            }
            other => bail!("unknown CDC row value tag {other}"),
        }
    }

    fn read_u8(&mut self) -> Result<u8> {
        let bytes = self.take(1)?;
        Ok(bytes[0])
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(
            bytes.try_into().expect("slice length checked"),
        ))
    }

    fn read_i64(&mut self) -> Result<i64> {
        let bytes = self.take(8)?;
        Ok(i64::from_le_bytes(
            bytes.try_into().expect("slice length checked"),
        ))
    }

    fn read_i32(&mut self) -> Result<i32> {
        let bytes = self.take(4)?;
        Ok(i32::from_le_bytes(
            bytes.try_into().expect("slice length checked"),
        ))
    }

    fn read_i128(&mut self) -> Result<i128> {
        let bytes = self.take(16)?;
        Ok(i128::from_le_bytes(
            bytes.try_into().expect("slice length checked"),
        ))
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.bytes.len() < len {
            bail!(
                "CDC row state ended early: needed {len} bytes, had {}",
                self.bytes.len()
            );
        }
        let (head, tail) = self.bytes.split_at(len);
        self.bytes = tail;
        Ok(head)
    }
}
