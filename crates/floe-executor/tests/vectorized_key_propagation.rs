use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::{BinaryArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::scalar::ScalarValue;

use floe_executor::{
    ConsolidationMode, DynamicStateTableProvider, VectorizedPlanExecutor, build_delta_batch,
    build_source_delta_batch,
};

fn person_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tag", DataType::Utf8, false),
    ]))
}

fn person_row(id: i64, name: &str) -> Vec<ScalarValue> {
    vec![
        ScalarValue::Int64(Some(id)),
        ScalarValue::Utf8(Some(name.to_string())),
    ]
}

fn right_row(id: i64, tag: &str) -> Vec<ScalarValue> {
    vec![
        ScalarValue::Int64(Some(id)),
        ScalarValue::Utf8(Some(tag.to_string())),
    ]
}

#[tokio::test]
async fn preserves_key_through_filter_projection() -> Result<()> {
    let source_batch = build_source_delta_batch(
        "nexmark_person",
        person_schema(),
        vec![(person_row(7, "alice"), 1), (person_row(8, "bob"), 1)],
    )?;

    let provider = Arc::new(DynamicStateTableProvider::new(source_batch.schema()));
    provider.set_batches(vec![source_batch.clone()]);

    let ctx = SessionContext::new();
    ctx.register_table("src", Arc::clone(&provider) as Arc<dyn TableProvider>)?;
    let df = ctx
        .sql("SELECT id, name, __key, __weight FROM src WHERE id = 7")
        .await?;
    let plan = df.create_physical_plan().await?;
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("__key", DataType::Binary, false),
        Field::new("__weight", DataType::Int64, false),
    ]));
    let executor = VectorizedPlanExecutor::new(
        plan,
        ctx.task_ctx(),
        output_schema,
        ConsolidationMode::ByKey,
    )?;
    let tick = executor.run_tick().await?;

    let key_in = source_batch
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("input key")
        .value(0)
        .to_vec();
    let out_batch = tick
        .batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .expect("output batch");
    let key_out = out_batch
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("output key")
        .value(0)
        .to_vec();
    assert_eq!(key_in, key_out);
    Ok(())
}

#[tokio::test]
async fn preserves_left_key_through_join_when_selected() -> Result<()> {
    let left_batch = build_source_delta_batch(
        "nexmark_person",
        person_schema(),
        vec![(person_row(9, "carol"), 2)],
    )?;
    let right_batch = build_delta_batch(right_schema(), vec![(right_row(9, "x"), 3)], None)?;

    let left_provider = Arc::new(DynamicStateTableProvider::new(left_batch.schema()));
    let right_provider = Arc::new(DynamicStateTableProvider::new(right_batch.schema()));
    left_provider.set_batches(vec![left_batch.clone()]);
    right_provider.set_batches(vec![right_batch]);

    let ctx = SessionContext::new();
    ctx.register_table(
        "left_delta",
        Arc::clone(&left_provider) as Arc<dyn TableProvider>,
    )?;
    ctx.register_table(
        "right_delta",
        Arc::clone(&right_provider) as Arc<dyn TableProvider>,
    )?;

    let df = ctx
        .sql(
            "SELECT l.id, l.name, l.__key, l.__weight * r.__weight AS __weight \
             FROM left_delta l JOIN right_delta r ON l.id = r.id",
        )
        .await?;
    let plan = df.create_physical_plan().await?;
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("__key", DataType::Binary, false),
        Field::new("__weight", DataType::Int64, false),
    ]));
    let executor = VectorizedPlanExecutor::new(
        plan,
        ctx.task_ctx(),
        output_schema,
        ConsolidationMode::ByKey,
    )?;
    let tick = executor.run_tick().await?;
    let out_batch = tick
        .batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .expect("output batch");

    let in_key = left_batch
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("input key")
        .value(0)
        .to_vec();
    let out_key = out_batch
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("output key")
        .value(0)
        .to_vec();
    let out_weight = out_batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("weight array")
        .value(0);
    let out_name = out_batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name array")
        .value(0);

    assert_eq!(in_key, out_key);
    assert_eq!(out_name, "carol");
    assert_eq!(out_weight, 6);
    Ok(())
}
