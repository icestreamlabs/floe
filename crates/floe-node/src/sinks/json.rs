use anyhow::Result;
use datafusion::arrow::datatypes::SchemaRef;
use floe_executor::mv_changelog::MvChangelogBatch;

use crate::arrow_json::record_batch_row_to_json;

pub(super) fn changelog_row_to_json(
    batch: &MvChangelogBatch,
    row_idx: usize,
    schema: &SchemaRef,
) -> Result<serde_json::Value> {
    let mut object = match record_batch_row_to_json(&batch.batch, row_idx, schema)? {
        serde_json::Value::Object(object) => object,
        _ => unreachable!("record batch rows encode as JSON objects"),
    };
    object.insert(
        "__mv_version".to_string(),
        serde_json::Value::from(batch.version),
    );
    object.insert(
        "__op".to_string(),
        serde_json::Value::from(batch.diffs.get(row_idx).copied().unwrap_or(0)),
    );
    object.insert(
        "__time".to_string(),
        batch
            .version_time
            .map(serde_json::Value::from)
            .unwrap_or(serde_json::Value::Null),
    );

    Ok(serde_json::Value::Object(object))
}
