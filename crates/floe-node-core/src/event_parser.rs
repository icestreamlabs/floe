use anyhow::{Context, Result, ensure};
use serde_json::Value;

use floe_core::source::SourceEvent;

pub fn parse_event_line(line: &str, default_source: Option<&str>) -> Result<SourceEvent> {
    let value: Value = serde_json::from_str(line).context("decode json line")?;
    parse_event_value(value, default_source)
}

pub fn parse_event_value(value: Value, default_source: Option<&str>) -> Result<SourceEvent> {
    let object = value
        .as_object()
        .context("event payload must be a JSON object")?;

    if let (Some(source), Some(payload)) = (object.get("source"), object.get("data")) {
        let source = source.as_str().context("event source must be a string")?;
        ensure!(payload.is_object(), "event payload must be an object");
        return Ok(SourceEvent::new(source, payload.clone()));
    }

    let source = default_source.context("event payload missing source and no default provided")?;
    ensure!(value.is_object(), "event payload must be an object");
    Ok(SourceEvent::new(source, value))
}
