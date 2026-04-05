use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::execution::context::SessionContext;

use floe_executor::{ConsolidationMode, DynamicStateTableProvider, VectorizedPlanExecutor};

type WeightedRow = (i64, String, i64);
type JoinedRow = (i64, String, String, i64);

fn state_schema(prefix: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(format!("{prefix}_id"), DataType::Int64, false),
        Field::new(format!("{prefix}_payload"), DataType::Utf8, false),
    ]))
}

fn delta_schema(prefix: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(format!("{prefix}_id"), DataType::Int64, false),
        Field::new(format!("{prefix}_payload"), DataType::Utf8, false),
        Field::new("__weight", DataType::Int64, false),
    ]))
}

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("join_id", DataType::Int64, false),
        Field::new("left_payload", DataType::Utf8, false),
        Field::new("right_payload", DataType::Utf8, false),
        Field::new("__weight", DataType::Int64, false),
    ]))
}

fn state_batch(schema: &SchemaRef, rows: &[(i64, &str)]) -> RecordBatch {
    let ids = Int64Array::from(rows.iter().map(|(id, _)| *id).collect::<Vec<_>>());
    let payloads = StringArray::from(
        rows.iter()
            .map(|(_, payload)| *payload)
            .collect::<Vec<&str>>(),
    );
    RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(ids), Arc::new(payloads)])
        .expect("state batch")
}

fn delta_batch(schema: &SchemaRef, rows: &[WeightedRow]) -> RecordBatch {
    let ids = Int64Array::from(rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>());
    let payloads = StringArray::from(
        rows.iter()
            .map(|(_, payload, _)| payload.as_str())
            .collect::<Vec<&str>>(),
    );
    let weights = Int64Array::from(rows.iter().map(|(_, _, w)| *w).collect::<Vec<_>>());
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(ids), Arc::new(payloads), Arc::new(weights)],
    )
    .expect("delta batch")
}

fn rows_from_batches(batches: &[RecordBatch]) -> Vec<JoinedRow> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("join_id");
        let left_payloads = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("left payload");
        let right_payloads = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("right payload");
        let weights = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("weight");
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                left_payloads.value(row).to_string(),
                right_payloads.value(row).to_string(),
                weights.value(row),
            ));
        }
    }
    rows.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then(a.1.cmp(&b.1))
            .then(a.2.cmp(&b.2))
            .then(a.3.cmp(&b.3))
    });
    rows
}

#[tokio::test]
async fn vectorized_cross_terms_match_expected() -> Result<()> {
    let left_state_schema = state_schema("left");
    let right_state_schema = state_schema("right");
    let left_delta_schema = delta_schema("left");
    let right_delta_schema = delta_schema("right");

    let left_state_provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(
        &left_state_schema,
    )));
    let right_state_provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(
        &right_state_schema,
    )));
    let left_delta_provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(
        &left_delta_schema,
    )));
    let right_delta_provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(
        &right_delta_schema,
    )));

    let left_state = vec![(1, "left-1"), (2, "left-2")];
    let right_state = vec![(1, "right-1"), (2, "right-2")];
    let left_delta = vec![
        (1, "left-1".to_string(), 1),
        (1, "left-1".to_string(), -1),
        (2, "left-2".to_string(), 1),
    ];
    let right_delta = vec![
        (1, "right-1".to_string(), 1),
        (1, "right-1".to_string(), -1),
        (2, "right-2".to_string(), 1),
    ];

    left_state_provider.set_batches(vec![state_batch(&left_state_schema, &left_state)]);
    right_state_provider.set_batches(vec![state_batch(&right_state_schema, &right_state)]);
    left_delta_provider.set_batches(vec![delta_batch(&left_delta_schema, &left_delta)]);
    right_delta_provider.set_batches(vec![delta_batch(&right_delta_schema, &right_delta)]);

    let ctx = SessionContext::new();
    ctx.register_table(
        "left_state",
        Arc::clone(&left_state_provider) as Arc<dyn TableProvider>,
    )?;
    ctx.register_table(
        "right_state",
        Arc::clone(&right_state_provider) as Arc<dyn TableProvider>,
    )?;
    ctx.register_table(
        "left_delta",
        Arc::clone(&left_delta_provider) as Arc<dyn TableProvider>,
    )?;
    ctx.register_table(
        "right_delta",
        Arc::clone(&right_delta_provider) as Arc<dyn TableProvider>,
    )?;

    let query = "
        SELECT ld.left_id AS join_id, ld.left_payload AS left_payload, rs.right_payload AS right_payload, ld.__weight AS __weight
        FROM left_delta ld
        JOIN right_state rs ON ld.left_id = rs.right_id
        UNION ALL
        SELECT ls.left_id AS join_id, ls.left_payload AS left_payload, rd.right_payload AS right_payload, rd.__weight AS __weight
        FROM left_state ls
        JOIN right_delta rd ON ls.left_id = rd.right_id
        UNION ALL
        SELECT ld.left_id AS join_id, ld.left_payload AS left_payload, rd.right_payload AS right_payload, ld.__weight * rd.__weight AS __weight
        FROM left_delta ld
        JOIN right_delta rd ON ld.left_id = rd.right_id
    ";

    let df = ctx.sql(query).await?;
    let plan = df.create_physical_plan().await?;
    let executor = VectorizedPlanExecutor::new(
        plan,
        ctx.task_ctx(),
        output_schema(),
        ConsolidationMode::ByAllColumns,
    )?;
    let vectorized = executor.run_tick().await?;
    let vectorized_rows = rows_from_batches(&vectorized.batches);

    assert_eq!(
        vectorized_rows,
        vec![(2, "left-2".to_string(), "right-2".to_string(), 3)]
    );
    assert_eq!(vectorized.stats.zero_weight_dropped_rows, 1);
    Ok(())
}
