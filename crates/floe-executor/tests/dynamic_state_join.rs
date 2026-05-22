use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::joins::HashJoinExec;

use floe_executor::table_provider::{DynamicStateExec, DynamicStateTableProvider};

fn make_batch(schema: &SchemaRef, values: &[i64]) -> RecordBatch {
    let array = Int64Array::from(values.to_vec());
    RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(array)]).expect("record batch")
}

fn extract_single_i64(batches: &[RecordBatch]) -> i64 {
    assert_eq!(batches.len(), 1, "expected a single output batch");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1, "expected a single output row");
    let array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 output");
    assert!(array.is_valid(0), "expected non-null output");
    array.value(0)
}

fn find_hash_join(plan: &Arc<dyn ExecutionPlan>) -> Option<&HashJoinExec> {
    if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        return Some(join);
    }

    for child in plan.children() {
        if let Some(join) = find_hash_join(child) {
            return Some(join);
        }
    }

    None
}

fn contains_dynamic_state(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if plan.as_any().is::<DynamicStateExec>() {
        return true;
    }

    plan.children()
        .iter()
        .any(|child| contains_dynamic_state(child))
}

#[tokio::test]
async fn dynamic_state_exec_reads_latest_snapshot() -> datafusion::error::Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&schema)));
    provider.set_batches(vec![make_batch(&schema, &[1, 2, 3])]);

    let ctx = SessionContext::new();
    ctx.register_table("state", Arc::clone(&provider) as Arc<dyn TableProvider>)?;

    let df = ctx.sql("SELECT SUM(id) AS total FROM state").await?;
    let plan = df.create_physical_plan().await?;
    let task_ctx = ctx.state().task_ctx();

    let first = collect(Arc::clone(&plan), Arc::clone(&task_ctx)).await?;
    assert_eq!(extract_single_i64(&first), 6);

    provider.set_batches(vec![make_batch(&schema, &[10, 20])]);

    let second = collect(plan, task_ctx).await?;
    assert_eq!(extract_single_i64(&second), 30);

    Ok(())
}

#[tokio::test]
async fn state_is_build_side_for_hash_join() -> datafusion::error::Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let provider = Arc::new(DynamicStateTableProvider::new(Arc::clone(&schema)));
    provider.set_batches(vec![make_batch(&schema, &[42])]);

    let stream_values: Vec<i64> = (0..1024).map(|value| value as i64).collect();
    let stream_batch = make_batch(&schema, &stream_values);
    let stream_provider = MemTable::try_new(Arc::clone(&schema), vec![vec![stream_batch]])?;

    let ctx = SessionContext::new();
    ctx.register_table("state", Arc::clone(&provider) as Arc<dyn TableProvider>)?;
    ctx.register_table(
        "stream",
        Arc::new(stream_provider) as Arc<dyn TableProvider>,
    )?;

    let df = ctx
        .sql("SELECT s.id FROM state s JOIN stream t ON s.id = t.id")
        .await?;
    let plan = df.create_physical_plan().await?;

    let join = find_hash_join(&plan).unwrap_or_else(|| {
        panic!(
            "expected hash join in plan, got: {}",
            displayable(plan.as_ref()).indent(true)
        )
    });

    assert!(
        contains_dynamic_state(join.left()),
        "expected dynamic state on build side; plan: {}",
        displayable(plan.as_ref()).indent(true)
    );
    assert!(
        !contains_dynamic_state(join.right()),
        "expected stream on probe side; plan: {}",
        displayable(plan.as_ref()).indent(true)
    );

    Ok(())
}
