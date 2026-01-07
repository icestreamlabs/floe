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
            let value = object
                .get(column.name())
                .with_context(|| format!("missing field '{}' in source payload", column.name()))?;
            let scalar = convert_value(column.data_type(), value)?;
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

fn convert_value(data_type: &SourceDataType, value: &Value) -> Result<ScalarValue> {
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
        SourceDataType::TimestampMillis => {
            let number = value
                .as_i64()
                .with_context(|| format!("expected integer timestamp, found {value}"))?;
            Ok(ScalarValue::TimestampMillisecond(Some(number), None))
        }
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
}
