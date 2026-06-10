use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
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

#[test]
fn source_arrow_batch_builder_appends_json_payload_directly() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("note", SourceDataType::Utf8, true),
            SourceColumn::new_nullable("created_at", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("definition");
    let mut builder = SourceArrowBatchBuilder::new(definition, 2);

    let event_ts = builder
        .append_json_payload(
            "orders",
            br#"{"created_at":1700000000000,"id":42,"note":"direct"}"#,
        )
        .expect("append direct payload");
    builder
        .append_json_payload(
            "orders",
            br#"{"id":43,"note":null,"created_at":1700000000001}"#,
        )
        .expect("append nullable direct payload");
    let batches = builder
        .finish()
        .expect("finish batch")
        .expect("record batches");

    assert_eq!(event_ts, Some(1_700_000_000_000_u64));
    let SourceArrowBatches::ExecutionAndQuery { query, .. } = batches else {
        panic!("expected execution and query batches");
    };
    let ids = query
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id array");
    assert_eq!(ids.value(0), 42);
    assert_eq!(ids.value(1), 43);
    let notes = query
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("note array");
    assert_eq!(notes.value(0), "direct");
    assert!(notes.is_null(1));
}
