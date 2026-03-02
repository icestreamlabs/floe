use anyhow::{Context, Result, bail};
use datafusion::scalar::ScalarValue;
use floe_core::source::{SourceDataType, SourceDefinition, SourceEvent};
use serde_json::Value;

use crate::stream_types::{Row, Timestamp};

#[derive(Debug, Clone)]
pub struct SourceRowDecoder {
    definition: SourceDefinition,
}

impl SourceRowDecoder {
    pub fn new(definition: SourceDefinition) -> Self {
        Self { definition }
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
        let payload = event.payload();
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

#[cfg(test)]
mod tests {
    use floe_core::source::{SourceColumn, SourceDataType};
    use serde_json::json;

    use super::*;

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
}
