use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use dbsp::{
    TableDescriptor, nexmark_auction_alias_table, nexmark_auction_table, nexmark_bid_alias_table,
    nexmark_bid_table, nexmark_person_alias_table, nexmark_person_table,
};

use crate::delta_batch::{DeltaBatchBuffer, DeltaBatchConfig};
use crate::encoding::encode_projected_row_key;
use crate::stream_types::{Diff, Row};

pub fn source_primary_key_columns(source_name: &str) -> Option<Vec<usize>> {
    source_table(source_name).map(|table| table.primary_key().columns().to_vec())
}

pub fn encode_primary_key(row: &Row, key_columns: &[usize]) -> Result<Vec<u8>> {
    let key_row = key_columns
        .iter()
        .map(|index| {
            row.get(*index)
                .cloned()
                .ok_or_else(|| anyhow!("missing key column at index {index}"))
        })
        .collect::<Result<Vec<_>>>()?;
    encode_projected_row_key(&key_row)
}

pub fn build_source_delta_batch(
    source_name: &str,
    base_schema: SchemaRef,
    rows: impl IntoIterator<Item = (Row, Diff)>,
) -> Result<RecordBatch> {
    let key_columns = source_primary_key_columns(source_name);
    build_delta_batch(base_schema, rows, key_columns.as_deref())
}

pub fn build_delta_batch(
    base_schema: SchemaRef,
    rows: impl IntoIterator<Item = (Row, Diff)>,
    key_columns: Option<&[usize]>,
) -> Result<RecordBatch> {
    let include_key = key_columns.is_some();
    let config = DeltaBatchConfig {
        max_rows: usize::MAX,
        max_bytes: usize::MAX,
    };
    let mut buffer = DeltaBatchBuffer::new(base_schema, include_key, config)?;
    let delta_schema = buffer.delta_schema();
    for (row, diff) in rows {
        let key = key_columns
            .map(|indices| encode_primary_key(&row, indices))
            .transpose()?;
        let _ = buffer.push(row, diff, key)?;
    }
    Ok(buffer
        .flush_manual()?
        .unwrap_or_else(|| RecordBatch::new_empty(Arc::clone(&delta_schema))))
}

fn source_table(source_name: &str) -> Option<&'static TableDescriptor> {
    match source_name {
        "nexmark_person" => Some(nexmark_person_table()),
        "person" => Some(nexmark_person_alias_table()),
        "nexmark_auction" => Some(nexmark_auction_table()),
        "auction" => Some(nexmark_auction_alias_table()),
        "nexmark_bid" => Some(nexmark_bid_table()),
        "bid" => Some(nexmark_bid_alias_table()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, BinaryArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::scalar::ScalarValue;

    fn person_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn person_row(id: i64, name: &str) -> Row {
        vec![
            ScalarValue::Int64(Some(id)),
            ScalarValue::Utf8(Some(name.to_string())),
        ]
    }

    #[test]
    fn generates_key_column_for_primary_key_sources() {
        let batch = build_source_delta_batch(
            "nexmark_person",
            person_schema(),
            vec![(person_row(7, "alice"), 1)],
        )
        .expect("delta batch");
        assert_eq!(batch.num_columns(), 4);
        let keys = batch
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("key array");
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn key_encoding_is_stable_across_batches() {
        let batch_one = build_source_delta_batch(
            "nexmark_person",
            person_schema(),
            vec![(person_row(11, "alice"), 1)],
        )
        .expect("batch one");
        let batch_two = build_source_delta_batch(
            "nexmark_person",
            person_schema(),
            vec![(person_row(11, "alice"), -1)],
        )
        .expect("batch two");

        let keys_one = batch_one
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("key array one");
        let keys_two = batch_two
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("key array two");
        assert_eq!(keys_one.value(0), keys_two.value(0));
    }
}
