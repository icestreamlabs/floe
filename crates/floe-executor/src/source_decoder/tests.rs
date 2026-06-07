use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};
use serde_json::json;

use super::*;

#[test]
fn source_arrow_batch_builder_prunes_execution_columns_only() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("note", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("created_at", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("definition");
    let required_columns = Arc::from([true, false, true]);
    let mut builder = SourceArrowBatchBuilder::new_with_execution_required_columns(
        definition,
        1,
        Some(required_columns),
    );
    let event = AppendIngestEvent::new(
        "orders",
        json!({
            "id": 42,
            "note": "ignored at execution",
            "created_at": 1_700_000_000_i64
        }),
    );

    let event_ts = builder.append_event(&event).expect("append event");
    let batches = builder
        .finish()
        .expect("finish batch")
        .expect("record batches");

    assert_eq!(event_ts, Some(1_700_000_000_u64));
    let SourceArrowBatches::ExecutionAndQuery { execution, query } = batches else {
        panic!("expected execution and query batches");
    };
    assert_eq!(query.num_rows(), 1);
    let full_note = query
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("note array");
    assert_eq!(full_note.value(0), "ignored at execution");
    let masked_note = execution
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("masked note array");
    assert!(!masked_note.is_null(0));
    assert_eq!(masked_note.value(0), "");
}

#[test]
fn source_arrow_batch_builder_can_skip_query_batches() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("note", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("created_at", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("definition");
    let required_columns = Arc::from([true, false, false]);
    let mut builder = SourceArrowBatchBuilder::new_with_execution_required_columns_and_batch_mode(
        definition,
        1,
        Some(required_columns),
        SourceArrowBatchMode::ExecutionOnly,
    );
    let event = AppendIngestEvent::new(
        "orders",
        json!({
            "id": 42,
            "note": "not materialized",
            "created_at": 1_700_000_000_i64
        }),
    );

    let event_ts = builder.append_event(&event).expect("append event");
    let batches = builder
        .finish()
        .expect("finish batch")
        .expect("record batches");

    assert_eq!(event_ts, Some(1_700_000_000_u64));
    let SourceArrowBatches::ExecutionOnly { execution } = batches else {
        panic!("expected execution-only batch");
    };
    let masked_note = execution
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("masked note array");
    assert_eq!(masked_note.value(0), "");
}
