use anyhow::{Result, anyhow};
use rkyv::rancor::Error;

use crate::RowValues;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ArchivedRow {
    bytes: Vec<u8>,
}

impl ArchivedRow {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

pub fn encode(values: &RowValues) -> Result<ArchivedRow> {
    let aligned = rkyv::api::high::to_bytes::<Error>(&values.clone())
        .map_err(|err| anyhow!("failed to encode row using rkyv: {err}"))?;
    Ok(ArchivedRow::new(aligned.into_vec()))
}

pub fn decode(row: &ArchivedRow) -> Result<RowValues> {
    let values = rkyv::from_bytes::<RowValues, Error>(row.bytes())
        .map_err(|err| anyhow!("failed to decode row using rkyv: {err}"))?;
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RowValue;

    #[test]
    fn roundtrips_row_values() {
        let row = vec![
            RowValue::Int64(42),
            RowValue::Bool(true),
            RowValue::Utf8("hello".to_string()),
            RowValue::TimestampMillis(1_700_000_000_000),
        ];
        let encoded = encode(&row).expect("encode");
        let decoded = decode(&encoded).expect("decode");
        assert_eq!(row, decoded);
    }
}
