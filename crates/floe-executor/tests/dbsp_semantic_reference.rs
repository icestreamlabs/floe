use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::execution::context::SessionContext;
use dbsp_semantic::ZSet;
use floe_executor::{
    ConsolidationMode, DynamicStateTableProvider, VectorizedPlanExecutor, VectorizedTickOutput,
};

fn delta_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("__weight", DataType::Int64, false),
    ]))
}

fn state_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Int64, false),
    ]))
}

fn filter_project_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("projected", DataType::Int64, false),
        Field::new("__weight", DataType::Int64, false),
    ]))
}

fn join_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("category", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("__weight", DataType::Int64, false),
    ]))
}

fn delta_batch(schema: &SchemaRef, rows: &[(i64, i64, i64)]) -> RecordBatch {
    let ids = Int64Array::from(rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>());
    let prices = Int64Array::from(rows.iter().map(|(_, price, _)| *price).collect::<Vec<_>>());
    let weights = Int64Array::from(
        rows.iter()
            .map(|(_, _, weight)| *weight)
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(ids), Arc::new(prices), Arc::new(weights)],
    )
    .expect("delta batch")
}

fn state_batch(schema: &SchemaRef, rows: &[(i64, i64)]) -> RecordBatch {
    let ids = Int64Array::from(rows.iter().map(|(id, _)| *id).collect::<Vec<_>>());
    let categories = Int64Array::from(
        rows.iter()
            .map(|(_, category)| *category)
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(ids), Arc::new(categories)],
    )
    .expect("state batch")
}

fn two_column_output_zset(output: &VectorizedTickOutput) -> ZSet<i64> {
    let mut rows = Vec::new();
    for batch in &output.batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column");
        let weights = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("weight column");
        for row_idx in 0..batch.num_rows() {
            rows.push((values.value(row_idx), weights.value(row_idx)));
        }
    }
    ZSet::from_weights(rows)
}

fn three_column_output_zset(output: &VectorizedTickOutput) -> ZSet<(i64, i64)> {
    let mut rows = Vec::new();
    for batch in &output.batches {
        let left = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("left column");
        let right = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("right column");
        let weights = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("weight column");
        for row_idx in 0..batch.num_rows() {
            rows.push((
                (left.value(row_idx), right.value(row_idx)),
                weights.value(row_idx),
            ));
        }
    }
    ZSet::from_weights(rows)
}

#[tokio::test]
async fn vectorized_filter_project_matches_zset_reference() -> Result<()> {
    let delta_schema = delta_schema();
    let delta_provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&delta_schema)));
    let ctx = SessionContext::new();
    ctx.register_table(
        "delta",
        Arc::clone(&delta_provider) as Arc<dyn TableProvider>,
    )?;

    let df = ctx
        .sql("SELECT id + 10 AS projected, __weight FROM delta WHERE price >= 10")
        .await?;
    let plan = df.create_physical_plan().await?;
    let executor = VectorizedPlanExecutor::new(
        plan,
        ctx.task_ctx(),
        filter_project_output_schema(),
        ConsolidationMode::ByAllColumns,
    )?;

    let delta_rows = [(1, 9, 1), (2, 10, 1), (2, 10, -1), (5, 20, 3)];
    delta_provider.set_batches(vec![delta_batch(&delta_schema, &delta_rows)]);
    let output = executor.run_tick().await?;

    let expected = ZSet::from_weights(
        delta_rows
            .iter()
            .filter(|(_, price, _)| *price >= 10)
            .map(|(id, _, weight)| (id + 10, *weight)),
    );
    assert_eq!(two_column_output_zset(&output), expected);
    Ok(())
}

#[tokio::test]
async fn vectorized_join_filter_project_matches_zset_reference() -> Result<()> {
    let state_schema = state_schema();
    let delta_schema = delta_schema();
    let state_provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&state_schema)));
    let delta_provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&delta_schema)));
    let ctx = SessionContext::new();
    ctx.register_table(
        "state",
        Arc::clone(&state_provider) as Arc<dyn TableProvider>,
    )?;
    ctx.register_table(
        "delta",
        Arc::clone(&delta_provider) as Arc<dyn TableProvider>,
    )?;

    let df = ctx
        .sql(
            "SELECT s.category, d.price, d.__weight \
             FROM state s JOIN delta d ON s.id = d.id \
             WHERE s.category = 7",
        )
        .await?;
    let plan = df.create_physical_plan().await?;
    let executor = VectorizedPlanExecutor::new(
        plan,
        ctx.task_ctx(),
        join_output_schema(),
        ConsolidationMode::ByAllColumns,
    )?;

    let state_rows = [(1, 7), (2, 8), (3, 7)];
    let delta_rows = [(1, 10, 1), (1, 10, -1), (2, 20, 5), (3, 30, -2)];
    state_provider.set_batches(vec![state_batch(&state_schema, &state_rows)]);
    delta_provider.set_batches(vec![delta_batch(&delta_schema, &delta_rows)]);
    let output = executor.run_tick().await?;

    let expected = ZSet::from_weights(delta_rows.iter().flat_map(|(id, price, weight)| {
        state_rows
            .iter()
            .filter(move |(state_id, category)| state_id == id && *category == 7)
            .map(move |(_, category)| ((*category, *price), *weight))
    }));
    assert_eq!(three_column_output_zset(&output), expected);
    Ok(())
}
