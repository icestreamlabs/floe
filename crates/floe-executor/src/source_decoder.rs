use std::sync::Arc;

use anyhow::{Context, Result, bail};
use datafusion::scalar::ScalarValue;
use floe_core::source::{SourceDataType, SourceDefinition, SourceEvent};
use serde_json::Value;

use crate::stream_types::{Row, Timestamp};

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

    pub fn decode(&self, event: &SourceEvent) -> Result<(Row, Option<Timestamp>)> {
        if event.source() != self.definition.name() {
            bail!(
                "event source {} does not match definition {}",
                event.source(),
                self.definition.name()
            );
        }
        let payload = event
            .payload()
            .context("source payload must be present for decoded events")?;
        let object = payload
            .as_object()
            .context("source payload must be a JSON object")?;
        let mut row = Vec::with_capacity(self.definition.columns().len());
        let mut event_ts = None;
        for column in self.definition.columns() {
            let scalar = match object.get(column.name()) {
                Some(value) => convert_value(column.data_type(), value, column.nullable())?,
                None if column.nullable() => null_scalar(column.data_type()),
                None => {
                    bail!("missing field '{}' in source payload", column.name());
                }
            };
            if event_ts.is_none()
                && matches!(column.data_type(), SourceDataType::TimestampMillis)
                && let ScalarValue::TimestampMillisecond(Some(ms), _) = scalar
                && ms >= 0
            {
                event_ts = Some(ms as u64);
            }
            row.push(scalar);
        }
        Ok((row, event_ts))
    }

    pub fn encode_row_key(&self, event: &SourceEvent) -> Result<(Vec<u8>, Option<Timestamp>)> {
        if event.source() != self.definition.name() {
            bail!(
                "event source {} does not match definition {}",
                event.source(),
                self.definition.name()
            );
        }
        let payload = event
            .payload()
            .context("source payload must be present for encoded events")?;
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

    fn column_required(&self, idx: usize) -> bool {
        self.encoded_required_columns
            .as_ref()
            .and_then(|columns| columns.get(idx))
            .copied()
            .unwrap_or(true)
    }
}

fn convert_value(data_type: &SourceDataType, value: &Value, nullable: bool) -> Result<ScalarValue> {
    if value.is_null() {
        if nullable {
            return Ok(null_scalar(data_type));
        }
        bail!("null value violates non-nullable column");
    }
    match data_type {
        SourceDataType::Int64 => {
            let number = value
                .as_i64()
                .with_context(|| format!("expected integer value, found {value}"))?;
            Ok(ScalarValue::Int64(Some(number)))
        }
        SourceDataType::Utf8 => {
            let string = value
                .as_str()
                .with_context(|| format!("expected string value, found {value}"))?;
            Ok(ScalarValue::Utf8(Some(string.to_string())))
        }
        SourceDataType::Bool => {
            let boolean = value
                .as_bool()
                .with_context(|| format!("expected boolean value, found {value}"))?;
            Ok(ScalarValue::Boolean(Some(boolean)))
        }
        SourceDataType::TimestampMillis => {
            let number = value
                .as_i64()
                .with_context(|| format!("expected integer timestamp, found {value}"))?;
            Ok(ScalarValue::TimestampMillisecond(Some(number), None))
        }
    }
}

fn null_scalar(data_type: &SourceDataType) -> ScalarValue {
    match data_type {
        SourceDataType::Int64 => ScalarValue::Int64(None),
        SourceDataType::Utf8 => ScalarValue::Utf8(None),
        SourceDataType::Bool => ScalarValue::Boolean(None),
        SourceDataType::TimestampMillis => ScalarValue::TimestampMillisecond(None, None),
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
    use floe_core::source::{SourceColumn, SourceDataType};
    use serde_json::json;

    use super::*;
    use crate::encoding::{decode_projected_row_key, encode_projected_row_key};

    #[test]
    fn decodes_nexmark_bid_event() {
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

        let (row, ts) = decoder.decode(&event).expect("decode");
        assert_eq!(row.len(), 7);
        assert_eq!(row[0], ScalarValue::Int64(Some(100)));
        assert_eq!(row[1], ScalarValue::Int64(Some(42)));
        assert_eq!(row[2], ScalarValue::Int64(Some(99)));
        assert_eq!(row[3], ScalarValue::Utf8(Some("web".to_string())));
        assert_eq!(
            row[4],
            ScalarValue::Utf8(Some("http://example.com".to_string()))
        );
        assert_eq!(
            row[5],
            ScalarValue::TimestampMillisecond(Some(1_600_000_000), None)
        );
        assert_eq!(ts, Some(1_600_000_000_u64));
    }

    #[test]
    fn decodes_boolean_column() {
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

        let (row, ts) = decoder.decode(&event).expect("decode");
        assert_eq!(row.len(), 2);
        assert_eq!(row[0], ScalarValue::Int64(Some(1)));
        assert_eq!(row[1], ScalarValue::Boolean(Some(true)));
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
            .decode(&event)
            .expect_err("missing price should fail");
        assert!(err.to_string().contains("missing field 'price'"));
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
            .decode(&event)
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
        let err = decoder.decode(&event).expect_err("null id should fail");
        assert!(
            err.to_string()
                .contains("null value violates non-nullable column")
        );
    }

    #[test]
    fn direct_encoding_matches_row_encoding() {
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

        let (row, decoded_ts) = decoder.decode(&event).expect("decode");
        let expected = encode_projected_row_key(&row).expect("encode row");
        let (encoded, direct_ts) = decoder.encode_row_key(&event).expect("direct encode");
        assert_eq!(encoded, expected);
        assert_eq!(direct_ts, decoded_ts);
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
        let decoded = decode_projected_row_key(&encoded).expect("decode encoded row");
        assert_eq!(decoded[0], ScalarValue::Int64(Some(42)));
        assert_eq!(decoded[1], ScalarValue::Utf8(None));
        assert_eq!(
            decoded[2],
            ScalarValue::TimestampMillisecond(Some(1_700_000_000), None)
        );
        assert_eq!(direct_ts, Some(1_700_000_000_u64));
    }
}
