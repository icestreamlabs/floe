use anyhow::{Context, Result};
use serde::Deserialize;

pub(crate) fn decode_json<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    label: &str,
) -> Result<Option<T>> {
    serde_json::from_slice(bytes)
        .with_context(|| format!("decode {label} from JSON"))
        .map(Some)
}

pub(crate) fn decode_json_value<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    label: &str,
) -> Result<T> {
    serde_json::from_slice(bytes).with_context(|| format!("decode {label} from JSON"))
}
