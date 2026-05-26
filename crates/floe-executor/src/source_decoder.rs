use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail, ensure};
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use floe_core::RowValue;
use floe_core::source::{AppendIngestEvent, SourceDataType, SourceDefinition};
use serde_json::Value;

use crate::stream_types::Timestamp;

trait PayloadRefExt<'a> {
    fn require_payload(self, message: &'static str) -> Result<&'a Value>;
}

impl<'a> PayloadRefExt<'a> for &'a Value {
    fn require_payload(self, _message: &'static str) -> Result<&'a Value> {
        Ok(self)
    }
}

impl<'a> PayloadRefExt<'a> for Option<&'a Value> {
    fn require_payload(self, message: &'static str) -> Result<&'a Value> {
        self.ok_or_else(|| anyhow!(message))
    }
}

#[derive(Debug, Clone)]
pub struct SourceRowDecoder {
    definition: SourceDefinition,
    encoded_required_columns: Option<Arc<[bool]>>,
}

impl SourceRowDecoder {
    pub fn new(definition: SourceDefinition) -> Self {
        Self {
            definition,
            encoded_required_columns: None,
        }
    }

    pub fn new_with_encoded_required_columns(
        definition: SourceDefinition,
        encoded_required_columns: Option<Arc<[bool]>>,
    ) -> Self {
        Self {
            definition,
            encoded_required_columns,
        }
    }

    pub fn definition(&self) -> &SourceDefinition {
        &self.definition
    }

    pub fn encode_row_key(
        &self,
        event: &AppendIngestEvent,
    ) -> Result<(Vec<u8>, Option<Timestamp>)> {
        if event.source() != self.definition.name() {
            bail!(
                "event source {} does not match definition {}",
                event.source(),
                self.definition.name()
            );
        }
        let payload = AppendIngestEvent::payload(event)
            .require_payload("source payload must be present for encoded events")?;
        let object = payload
            .as_object()
            .context("source payload must be a JSON object")?;
        let mut buf = Vec::with_capacity(64);
        let count = u32::try_from(self.definition.columns().len())
            .context("too many source columns to encode")?;
        buf.extend_from_slice(&count.to_le_bytes());
        let mut event_ts = None;
        for (idx, column) in self.definition.columns().iter().enumerate() {
            if !self.column_required(idx) {
                encode_typed_null(&mut buf, column.data_type());
                continue;
            }
            let value = object.get(column.name());
            encode_value_direct(
                &mut buf,
                column.data_type(),
                value,
                column.nullable(),
                &mut event_ts,
            )?;
        }
        Ok((buf, event_ts))
    }

    pub fn encode_row_values(
        &self,
        values: &[Option<RowValue>],
    ) -> Result<(Vec<u8>, Option<Timestamp>)> {
        if values.len() != self.definition.columns().len() {
            bail!(
                "source row value count {} does not match definition '{}' column count {}",
                values.len(),
                self.definition.name(),
                self.definition.columns().len()
            );
        }

        let mut buf = Vec::with_capacity(64);
        let count = u32::try_from(self.definition.columns().len())
            .context("too many source columns to encode")?;
        buf.extend_from_slice(&count.to_le_bytes());
        let mut event_ts = None;
        for (idx, (column, value)) in self.definition.columns().iter().zip(values).enumerate() {
            if !self.column_required(idx) {
                encode_typed_null(&mut buf, column.data_type());
                continue;
            }
            encode_row_value_direct(
                &mut buf,
                column.name(),
                column.data_type(),
                value.as_ref(),
                column.nullable(),
                &mut event_ts,
            )?;
        }
        Ok((buf, event_ts))
    }

    pub fn encode_arrow_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<Vec<(Vec<u8>, Option<Timestamp>)>> {
        if batch.num_columns() != self.definition.columns().len() {
            bail!(
                "Arrow batch column count {} does not match definition '{}' column count {}",
                batch.num_columns(),
                self.definition.name(),
                self.definition.columns().len()
            );
        }
        let columns = self.prepare_arrow_columns(batch)?;
        let mut rows = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            rows.push(self.encode_prepared_arrow_row(&columns, row_idx)?);
        }
        Ok(rows)
    }

    fn prepare_arrow_columns<'a>(
        &self,
        batch: &'a RecordBatch,
    ) -> Result<Vec<PreparedArrowColumn<'a>>> {
        self.definition
            .columns()
            .iter()
            .enumerate()
            .map(|(idx, column)| {
                let values = if self.column_required(idx) {
                    Some(ArrowColumnValues::new(
                        column.name(),
                        column.data_type(),
                        batch.column(idx).as_ref(),
                    )?)
                } else {
                    None
                };
                Ok(PreparedArrowColumn {
                    name: column.name().to_string(),
                    data_type: column.data_type().clone(),
                    nullable: column.nullable(),
                    values,
                })
            })
            .collect()
    }

    fn encode_prepared_arrow_row(
        &self,
        columns: &[PreparedArrowColumn<'_>],
        row_idx: usize,
    ) -> Result<(Vec<u8>, Option<Timestamp>)> {
        let mut buf = Vec::with_capacity(64);
        let count = u32::try_from(columns.len()).context("too many source columns to encode")?;
        buf.extend_from_slice(&count.to_le_bytes());
        let mut event_ts = None;
        for column in columns {
            let Some(values) = column.values.as_ref() else {
                encode_typed_null(&mut buf, &column.data_type);
                continue;
            };
            encode_prepared_arrow_value_direct(
                &mut buf,
                column.name.as_str(),
                &column.data_type,
                values,
                row_idx,
                column.nullable,
                &mut event_ts,
            )?;
        }
        Ok((buf, event_ts))
    }

    fn column_required(&self, idx: usize) -> bool {
        self.encoded_required_columns
            .as_ref()
            .and_then(|columns| columns.get(idx))
            .copied()
            .unwrap_or(true)
    }
}

struct PreparedArrowColumn<'a> {
    name: String,
    data_type: SourceDataType,
    nullable: bool,
    values: Option<ArrowColumnValues<'a>>,
}

enum ArrowColumnValues<'a> {
    Int64(&'a Int64Array),
    Bool(&'a BooleanArray),
    Utf8(&'a StringArray),
    TimestampMillis(&'a TimestampMillisecondArray),
    DateDays(&'a Date32Array),
    Decimal128(&'a Decimal128Array),
    Numeric(&'a StringArray),
}

impl<'a> ArrowColumnValues<'a> {
    fn new(column_name: &str, data_type: &SourceDataType, array: &'a dyn Array) -> Result<Self> {
        match data_type {
            SourceDataType::Int64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(Self::Int64)
                .with_context(|| format!("Arrow column '{column_name}' is not Int64")),
            SourceDataType::Utf8 => array
                .as_any()
                .downcast_ref::<StringArray>()
                .map(Self::Utf8)
                .with_context(|| format!("Arrow column '{column_name}' is not Utf8")),
            SourceDataType::TimestampMillis => array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .map(Self::TimestampMillis)
                .with_context(|| format!("Arrow column '{column_name}' is not TimestampMillis")),
            SourceDataType::DateDays => array
                .as_any()
                .downcast_ref::<Date32Array>()
                .map(Self::DateDays)
                .with_context(|| format!("Arrow column '{column_name}' is not Date32")),
            SourceDataType::Decimal128 { .. } => array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .map(Self::Decimal128)
                .with_context(|| format!("Arrow column '{column_name}' is not Decimal128")),
            SourceDataType::Numeric => array
                .as_any()
                .downcast_ref::<StringArray>()
                .map(Self::Numeric)
                .with_context(|| format!("Arrow column '{column_name}' is not Numeric/Utf8")),
            SourceDataType::Bool => array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(Self::Bool)
                .with_context(|| format!("Arrow column '{column_name}' is not Boolean")),
        }
    }

    fn is_null(&self, row_idx: usize) -> bool {
        match self {
            Self::Int64(array) => array.is_null(row_idx),
            Self::Bool(array) => array.is_null(row_idx),
            Self::Utf8(array) => array.is_null(row_idx),
            Self::TimestampMillis(array) => array.is_null(row_idx),
            Self::DateDays(array) => array.is_null(row_idx),
            Self::Decimal128(array) => array.is_null(row_idx),
            Self::Numeric(array) => array.is_null(row_idx),
        }
    }
}

fn encode_prepared_arrow_value_direct(
    buf: &mut Vec<u8>,
    column_name: &str,
    data_type: &SourceDataType,
    values: &ArrowColumnValues<'_>,
    row_idx: usize,
    nullable: bool,
    event_ts: &mut Option<Timestamp>,
) -> Result<()> {
    if values.is_null(row_idx) {
        if nullable {
            encode_typed_null(buf, data_type);
            return Ok(());
        }
        bail!("null value violates non-nullable column '{column_name}'");
    }

    match (data_type, values) {
        (SourceDataType::Int64, ArrowColumnValues::Int64(array)) => {
            let number = array.value(row_idx);
            buf.push(0x01);
            buf.extend_from_slice(&number.to_le_bytes());
            Ok(())
        }
        (SourceDataType::Utf8, ArrowColumnValues::Utf8(array)) => {
            let bytes = array.value(row_idx).as_bytes();
            buf.push(0x02);
            let len = u32::try_from(bytes.len()).context("utf8 value too large for MV key")?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(bytes);
            Ok(())
        }
        (SourceDataType::TimestampMillis, ArrowColumnValues::TimestampMillis(array)) => {
            let number = array.value(row_idx);
            buf.push(0x03);
            buf.extend_from_slice(&number.to_le_bytes());
            if event_ts.is_none() && number >= 0 {
                *event_ts = Some(number as u64);
            }
            Ok(())
        }
        (SourceDataType::Bool, ArrowColumnValues::Bool(array)) => {
            buf.push(0x04);
            buf.push(if array.value(row_idx) { 1 } else { 0 });
            Ok(())
        }
        (SourceDataType::DateDays, ArrowColumnValues::DateDays(array)) => {
            let days = array.value(row_idx);
            buf.push(0x09);
            buf.extend_from_slice(&days.to_le_bytes());
            Ok(())
        }
        (SourceDataType::Decimal128 { .. }, ArrowColumnValues::Decimal128(array)) => {
            let number = array.value(row_idx);
            buf.push(0x0B);
            buf.extend_from_slice(&number.to_le_bytes());
            Ok(())
        }
        (SourceDataType::Numeric, ArrowColumnValues::Numeric(array)) => {
            let bytes = array.value(row_idx).as_bytes();
            buf.push(0x02);
            let len = u32::try_from(bytes.len()).context("numeric value too large for MV key")?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(bytes);
            Ok(())
        }
        _ => bail!("prepared Arrow column '{column_name}' does not match type {data_type:?}"),
    }
}

fn encode_row_value_direct(
    buf: &mut Vec<u8>,
    column_name: &str,
    data_type: &SourceDataType,
    value: Option<&RowValue>,
    nullable: bool,
    event_ts: &mut Option<Timestamp>,
) -> Result<()> {
    match value {
        None if nullable => {
            encode_typed_null(buf, data_type);
            Ok(())
        }
        None => bail!("null value violates non-nullable column '{column_name}'"),
        Some(RowValue::Int64(number)) if matches!(data_type, SourceDataType::Int64) => {
            buf.push(0x01);
            buf.extend_from_slice(&number.to_le_bytes());
            Ok(())
        }
        Some(RowValue::Utf8(string)) if matches!(data_type, SourceDataType::Utf8) => {
            buf.push(0x02);
            let bytes = string.as_bytes();
            let len = u32::try_from(bytes.len()).context("utf8 value too large for MV key")?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(bytes);
            Ok(())
        }
        Some(RowValue::TimestampMillis(number))
            if matches!(data_type, SourceDataType::TimestampMillis) =>
        {
            buf.push(0x03);
            buf.extend_from_slice(&number.to_le_bytes());
            if event_ts.is_none() && *number >= 0 {
                *event_ts = Some(*number as u64);
            }
            Ok(())
        }
        Some(RowValue::Bool(flag)) if matches!(data_type, SourceDataType::Bool) => {
            buf.push(0x04);
            buf.push(if *flag { 1 } else { 0 });
            Ok(())
        }
        Some(RowValue::DateDays(days)) if matches!(data_type, SourceDataType::DateDays) => {
            buf.push(0x09);
            buf.extend_from_slice(&days.to_le_bytes());
            Ok(())
        }
        Some(RowValue::Decimal128(number))
            if matches!(data_type, SourceDataType::Decimal128 { .. }) =>
        {
            buf.push(0x0B);
            buf.extend_from_slice(&number.to_le_bytes());
            Ok(())
        }
        Some(RowValue::Numeric(number)) if matches!(data_type, SourceDataType::Numeric) => {
            buf.push(0x02);
            let bytes = number.as_bytes();
            let len = u32::try_from(bytes.len()).context("numeric value too large for MV key")?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(bytes);
            Ok(())
        }
        Some(value) => bail!(
            "source row value for column '{}' does not match type {:?}: {:?}",
            column_name,
            data_type,
            value
        ),
    }
}

fn encode_value_direct(
    buf: &mut Vec<u8>,
    data_type: &SourceDataType,
    value: Option<&Value>,
    nullable: bool,
    event_ts: &mut Option<Timestamp>,
) -> Result<()> {
    match value {
        None if nullable => {
            encode_typed_null(buf, data_type);
            Ok(())
        }
        None => bail!("missing field in source payload"),
        Some(value) if value.is_null() => {
            if nullable {
                encode_typed_null(buf, data_type);
                Ok(())
            } else {
                bail!("null value violates non-nullable column");
            }
        }
        Some(value) => match data_type {
            SourceDataType::Int64 => {
                let number = value
                    .as_i64()
                    .with_context(|| format!("expected integer value, found {value}"))?;
                buf.push(0x01);
                buf.extend_from_slice(&number.to_le_bytes());
                Ok(())
            }
            SourceDataType::Utf8 => {
                let string = value
                    .as_str()
                    .with_context(|| format!("expected string value, found {value}"))?;
                buf.push(0x02);
                let bytes = string.as_bytes();
                let len = u32::try_from(bytes.len()).context("utf8 value too large for MV key")?;
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(bytes);
                Ok(())
            }
            SourceDataType::TimestampMillis => {
                let number = value
                    .as_i64()
                    .with_context(|| format!("expected integer timestamp, found {value}"))?;
                buf.push(0x03);
                buf.extend_from_slice(&number.to_le_bytes());
                if event_ts.is_none() && number >= 0 {
                    *event_ts = Some(number as u64);
                }
                Ok(())
            }
            SourceDataType::Bool => {
                let flag = value
                    .as_bool()
                    .with_context(|| format!("expected boolean value, found {value}"))?;
                buf.push(0x04);
                buf.push(if flag { 1 } else { 0 });
                Ok(())
            }
            SourceDataType::DateDays => {
                let days = value
                    .as_i64()
                    .with_context(|| format!("expected integer date days, found {value}"))?;
                let days = i32::try_from(days)
                    .with_context(|| format!("date days value out of range: {value}"))?;
                buf.push(0x09);
                buf.extend_from_slice(&days.to_le_bytes());
                Ok(())
            }
            SourceDataType::Decimal128 { scale, .. } => {
                let number = match value {
                    Value::String(value) => parse_decimal_text_to_i128(value, *scale)?,
                    Value::Number(value) => parse_decimal_text_to_i128(&value.to_string(), *scale)?,
                    other => bail!("expected decimal string or JSON number, found {other}"),
                };
                buf.push(0x0B);
                buf.extend_from_slice(&number.to_le_bytes());
                Ok(())
            }
            SourceDataType::Numeric => {
                let number = match value {
                    Value::String(value) => value.clone(),
                    Value::Number(value) => value.to_string(),
                    other => bail!("expected numeric string or JSON number, found {other}"),
                };
                buf.push(0x02);
                let bytes = number.as_bytes();
                let len =
                    u32::try_from(bytes.len()).context("numeric value too large for MV key")?;
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(bytes);
                Ok(())
            }
        },
    }
}

fn encode_typed_null(buf: &mut Vec<u8>, data_type: &SourceDataType) {
    match data_type {
        SourceDataType::Int64 => buf.push(0x05),
        SourceDataType::Utf8 => buf.push(0x06),
        SourceDataType::TimestampMillis => buf.push(0x07),
        SourceDataType::Bool => buf.push(0x08),
        SourceDataType::DateDays => buf.push(0x0A),
        SourceDataType::Decimal128 { .. } => buf.push(0x0C),
        SourceDataType::Numeric => buf.push(0x06),
    }
}

fn parse_decimal_text_to_i128(value: &str, scale: i8) -> Result<i128> {
    let scale = u32::try_from(scale).context("Decimal128 scale cannot be negative")?;
    let value = value.trim();
    let (negative, unsigned) = value
        .strip_prefix('-')
        .map(|rest| (true, rest))
        .unwrap_or((false, value));
    let unsigned = unsigned.strip_prefix('+').unwrap_or(unsigned);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let mut digits = String::with_capacity(whole.len() + scale as usize);
    digits.push_str(whole);
    let scale_usize = usize::try_from(scale).expect("u32 scale fits usize");
    ensure!(
        fraction.len() <= scale_usize,
        "decimal value '{value}' has more fractional digits than scale {scale}"
    );
    digits.push_str(fraction);
    digits.extend(std::iter::repeat_n('0', scale_usize - fraction.len()));
    let parsed = digits
        .parse::<i128>()
        .with_context(|| format!("decode decimal value '{value}'"))?;
    Ok(if negative { -parsed } else { parsed })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
        TimestampMillisecondArray,
    };
    use datafusion::arrow::record_batch::RecordBatch;
    use floe_core::RowValue;
    use floe_core::source::{SourceColumn, SourceDataType};
    use serde_json::json;

    use super::*;
    use crate::encoding::{EncodedRowScalar, decode_all_encoded_row_scalars_into};

    fn decode_test_row(encoded: &[u8]) -> Vec<Option<EncodedRowScalar>> {
        let mut decoded = Vec::new();
        decode_all_encoded_row_scalars_into(encoded, &mut decoded).expect("decode encoded row");
        decoded
    }

    fn mixed_definition() -> SourceDefinition {
        SourceDefinition::new(
            "mixed",
            vec![
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("label", SourceDataType::Utf8, true),
                SourceColumn::new_nullable("seen_at", SourceDataType::TimestampMillis, false),
                SourceColumn::new_nullable("enabled", SourceDataType::Bool, false),
            ],
        )
        .expect("definition")
    }

    #[test]
    fn encodes_nexmark_bid_event() {
        let definition = SourceDefinition::new(
            "nexmark_bid",
            vec![
                SourceColumn::new("auction", SourceDataType::Int64),
                SourceColumn::new("bidder", SourceDataType::Int64),
                SourceColumn::new("price", SourceDataType::Int64),
                SourceColumn::new("channel", SourceDataType::Utf8),
                SourceColumn::new("url", SourceDataType::Utf8),
                SourceColumn::new("date_time", SourceDataType::TimestampMillis),
                SourceColumn::new("extra", SourceDataType::Utf8),
            ],
        )
        .expect("definition");
        let decoder = SourceRowDecoder::new(definition);
        let event = AppendIngestEvent::new(
            "nexmark_bid",
            json!({
                "auction": 100,
                "bidder": 42,
                "price": 99,
                "channel": "web",
                "url": "http://example.com",
                "date_time": 1_600_000_000_i64,
                "extra": ""
            }),
        );

        let (encoded, ts) = decoder.encode_row_key(&event).expect("encode");
        let row = decode_test_row(&encoded);
        assert_eq!(row.len(), 7);
        assert_eq!(row[0], Some(EncodedRowScalar::Int64(100)));
        assert_eq!(row[1], Some(EncodedRowScalar::Int64(42)));
        assert_eq!(row[2], Some(EncodedRowScalar::Int64(99)));
        assert_eq!(row[3], Some(EncodedRowScalar::Utf8("web".to_string())));
        assert_eq!(
            row[4],
            Some(EncodedRowScalar::Utf8("http://example.com".to_string()))
        );
        assert_eq!(
            row[5],
            Some(EncodedRowScalar::TimestampMillis(1_600_000_000))
        );
        assert_eq!(ts, Some(1_600_000_000_u64));
    }

    #[test]
    fn encodes_boolean_column() {
        let definition = SourceDefinition::new(
            "flags",
            vec![
                SourceColumn::new("id", SourceDataType::Int64),
                SourceColumn::new("enabled", SourceDataType::Bool),
            ],
        )
        .expect("definition");
        let decoder = SourceRowDecoder::new(definition);
        let event = AppendIngestEvent::new(
            "flags",
            json!({
                "id": 1,
                "enabled": true
            }),
        );

        let (encoded, ts) = decoder.encode_row_key(&event).expect("encode");
        let row = decode_test_row(&encoded);
        assert_eq!(row.len(), 2);
        assert_eq!(row[0], Some(EncodedRowScalar::Int64(1)));
        assert_eq!(row[1], Some(EncodedRowScalar::Bool(true)));
        assert_eq!(ts, None);
    }

    #[test]
    fn encodes_date_and_numeric_columns() {
        let definition = SourceDefinition::new(
            "lineitem",
            vec![
                SourceColumn::new_nullable("shipdate", SourceDataType::DateDays, false),
                SourceColumn::new_nullable("extendedprice", SourceDataType::Numeric, false),
                SourceColumn::new_nullable(
                    "discount",
                    SourceDataType::Decimal128 {
                        precision: 15,
                        scale: 2,
                    },
                    false,
                ),
            ],
        )
        .expect("definition");
        let decoder = SourceRowDecoder::new(definition.clone());
        let event = AppendIngestEvent::new(
            "lineitem",
            json!({
                "shipdate": 10471,
                "extendedprice": "12345.67",
                "discount": "123.45"
            }),
        );

        let (encoded, ts) = decoder.encode_row_key(&event).expect("encode json");
        assert_eq!(ts, None);
        assert_eq!(
            decode_test_row(&encoded),
            vec![
                Some(EncodedRowScalar::DateDays(10471)),
                Some(EncodedRowScalar::Utf8("12345.67".to_string())),
                Some(EncodedRowScalar::Decimal128(12_345)),
            ]
        );

        let batch = RecordBatch::try_new(
            definition.to_arrow_schema(),
            vec![
                Arc::new(Date32Array::from(vec![10471])),
                Arc::new(StringArray::from(vec!["12345.67"])),
                Arc::new(
                    Decimal128Array::from(vec![Some(12_345)])
                        .with_precision_and_scale(15, 2)
                        .expect("decimal type"),
                ),
            ],
        )
        .expect("record batch");
        let encoded = decoder.encode_arrow_batch(&batch).expect("encode arrow");
        assert_eq!(
            decode_test_row(&encoded[0].0),
            vec![
                Some(EncodedRowScalar::DateDays(10471)),
                Some(EncodedRowScalar::Utf8("12345.67".to_string())),
                Some(EncodedRowScalar::Decimal128(12_345)),
            ]
        );
    }

    #[test]
    fn encodes_arrow_batch_without_json_payloads() {
        let definition = mixed_definition();
        let decoder = SourceRowDecoder::new(definition.clone());
        let batch = RecordBatch::try_new(
            definition.to_arrow_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("one"), None])),
                Arc::new(TimestampMillisecondArray::from(vec![1000, 2000])),
                Arc::new(BooleanArray::from(vec![true, false])),
            ],
        )
        .expect("record batch");

        let encoded = decoder.encode_arrow_batch(&batch).expect("encode arrow");

        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[0].1, Some(1000));
        assert_eq!(encoded[1].1, Some(2000));
        assert_eq!(
            decode_test_row(&encoded[0].0),
            vec![
                Some(EncodedRowScalar::Int64(1)),
                Some(EncodedRowScalar::Utf8("one".to_string())),
                Some(EncodedRowScalar::TimestampMillis(1000)),
                Some(EncodedRowScalar::Bool(true)),
            ]
        );
        assert_eq!(
            decode_test_row(&encoded[1].0),
            vec![
                Some(EncodedRowScalar::Int64(2)),
                None,
                Some(EncodedRowScalar::TimestampMillis(2000)),
                Some(EncodedRowScalar::Bool(false)),
            ]
        );
    }

    #[test]
    fn rejects_missing_required_column() {
        let definition = SourceDefinition::new(
            "orders",
            vec![
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            ],
        )
        .expect("definition");
        let decoder = SourceRowDecoder::new(definition);
        let event = AppendIngestEvent::new("orders", json!({"id": 1}));
        let err = decoder
            .encode_row_key(&event)
            .expect_err("missing price should fail");
        assert!(err.to_string().contains("missing field in source payload"));
    }

    #[test]
    fn rejects_wrong_column_type() {
        let definition = SourceDefinition::new(
            "orders",
            vec![SourceColumn::new_nullable(
                "id",
                SourceDataType::Int64,
                false,
            )],
        )
        .expect("definition");
        let decoder = SourceRowDecoder::new(definition);
        let event = AppendIngestEvent::new("orders", json!({"id": "oops"}));
        let err = decoder
            .encode_row_key(&event)
            .expect_err("type mismatch should fail");
        assert!(err.to_string().contains("expected integer value"));
    }

    #[test]
    fn rejects_null_for_non_nullable_column() {
        let definition = SourceDefinition::new(
            "orders",
            vec![
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("note", SourceDataType::Utf8, true),
            ],
        )
        .expect("definition");
        let decoder = SourceRowDecoder::new(definition);
        let event = AppendIngestEvent::new("orders", json!({"id": null, "note": null}));
        let err = decoder
            .encode_row_key(&event)
            .expect_err("null id should fail");
        assert!(
            err.to_string()
                .contains("null value violates non-nullable column")
        );
    }

    #[test]
    fn direct_encoding_produces_expected_scalars_and_timestamp() {
        let definition = SourceDefinition::new(
            "orders",
            vec![
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("note", SourceDataType::Utf8, true),
                SourceColumn::new_nullable("created_at", SourceDataType::TimestampMillis, false),
                SourceColumn::new_nullable("enabled", SourceDataType::Bool, false),
            ],
        )
        .expect("definition");
        let decoder = SourceRowDecoder::new(definition);
        let event = AppendIngestEvent::new(
            "orders",
            json!({
                "id": 42,
                "note": "hello",
                "created_at": 1_700_000_000_i64,
                "enabled": true
            }),
        );

        let (encoded, direct_ts) = decoder.encode_row_key(&event).expect("direct encode");
        let decoded = decode_test_row(&encoded);
        assert_eq!(decoded[0], Some(EncodedRowScalar::Int64(42)));
        assert_eq!(
            decoded[1],
            Some(EncodedRowScalar::Utf8("hello".to_string()))
        );
        assert_eq!(
            decoded[2],
            Some(EncodedRowScalar::TimestampMillis(1_700_000_000))
        );
        assert_eq!(decoded[3], Some(EncodedRowScalar::Bool(true)));
        assert_eq!(direct_ts, Some(1_700_000_000_u64));
    }

    #[test]
    fn direct_encoding_can_omit_unneeded_columns() {
        let definition = SourceDefinition::new(
            "orders",
            vec![
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("note", SourceDataType::Utf8, false),
                SourceColumn::new_nullable("created_at", SourceDataType::TimestampMillis, false),
            ],
        )
        .expect("definition");
        let decoder = SourceRowDecoder::new_with_encoded_required_columns(
            definition,
            Some(Arc::from([true, false, true])),
        );
        let event = AppendIngestEvent::new(
            "orders",
            json!({
                "id": 42,
                "created_at": 1_700_000_000_i64
            }),
        );

        let (encoded, direct_ts) = decoder.encode_row_key(&event).expect("direct encode");
        let decoded = decode_test_row(&encoded);
        assert_eq!(decoded[0], Some(EncodedRowScalar::Int64(42)));
        assert_eq!(decoded[1], None);
        assert_eq!(
            decoded[2],
            Some(EncodedRowScalar::TimestampMillis(1_700_000_000))
        );
        assert_eq!(direct_ts, Some(1_700_000_000_u64));
    }

    #[test]
    fn typed_row_encoding_matches_json_event_encoding() {
        let definition = SourceDefinition::new(
            "orders",
            vec![
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("note", SourceDataType::Utf8, true),
                SourceColumn::new_nullable("created_at", SourceDataType::TimestampMillis, false),
                SourceColumn::new_nullable("enabled", SourceDataType::Bool, false),
            ],
        )
        .expect("definition");
        let decoder = SourceRowDecoder::new(definition);
        let event = AppendIngestEvent::new(
            "orders",
            json!({
                "id": 42,
                "note": null,
                "created_at": 1_700_000_000_i64,
                "enabled": true
            }),
        );
        let row_values = vec![
            Some(RowValue::Int64(42)),
            None,
            Some(RowValue::TimestampMillis(1_700_000_000)),
            Some(RowValue::Bool(true)),
        ];

        let json_encoded = decoder.encode_row_key(&event).expect("json encode");
        let typed_encoded = decoder
            .encode_row_values(&row_values)
            .expect("typed row encode");

        assert_eq!(typed_encoded, json_encoded);
        let decoded = decode_test_row(&typed_encoded.0);
        assert_eq!(decoded[0], Some(EncodedRowScalar::Int64(42)));
        assert_eq!(decoded[1], None);
        assert_eq!(
            decoded[2],
            Some(EncodedRowScalar::TimestampMillis(1_700_000_000))
        );
        assert_eq!(decoded[3], Some(EncodedRowScalar::Bool(true)));
    }

    #[test]
    fn typed_row_encoding_rejects_wrong_shape_and_types() {
        let definition = SourceDefinition::new(
            "orders",
            vec![
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("note", SourceDataType::Utf8, true),
            ],
        )
        .expect("definition");
        let decoder = SourceRowDecoder::new(definition);

        let err = decoder
            .encode_row_values(&[Some(RowValue::Int64(42))])
            .expect_err("row shape should fail");
        assert!(err.to_string().contains("value count"));

        let err = decoder
            .encode_row_values(&[Some(RowValue::Utf8("oops".to_string())), None])
            .expect_err("wrong type should fail");
        assert!(err.to_string().contains("does not match type"));

        let err = decoder
            .encode_row_values(&[None, None])
            .expect_err("null primary column should fail");
        assert!(
            err.to_string()
                .contains("null value violates non-nullable column 'id'")
        );
    }
}
