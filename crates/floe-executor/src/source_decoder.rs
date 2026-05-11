use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use floe_core::RowValue;
use floe_core::source::{SourceDataType, SourceDefinition, SourceEvent};
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

    pub fn encode_row_key(&self, event: &SourceEvent) -> Result<(Vec<u8>, Option<Timestamp>)> {
        if event.source() != self.definition.name() {
            bail!(
                "event source {} does not match definition {}",
                event.source(),
                self.definition.name()
            );
        }
        let payload = SourceEvent::payload(event)
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

    fn column_required(&self, idx: usize) -> bool {
        self.encoded_required_columns
            .as_ref()
            .and_then(|columns| columns.get(idx))
            .copied()
            .unwrap_or(true)
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
        },
    }
}

fn encode_typed_null(buf: &mut Vec<u8>, data_type: &SourceDataType) {
    match data_type {
        SourceDataType::Int64 => buf.push(0x05),
        SourceDataType::Utf8 => buf.push(0x06),
        SourceDataType::TimestampMillis => buf.push(0x07),
        SourceDataType::Bool => buf.push(0x08),
    }
}

#[cfg(test)]
mod tests {
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
        let event = SourceEvent::new(
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
        let event = SourceEvent::new(
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
        let event = SourceEvent::new("orders", json!({"id": 1}));
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
        let event = SourceEvent::new("orders", json!({"id": "oops"}));
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
        let event = SourceEvent::new("orders", json!({"id": null, "note": null}));
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
        let event = SourceEvent::new(
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
        let event = SourceEvent::new(
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
        let event = SourceEvent::new(
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
