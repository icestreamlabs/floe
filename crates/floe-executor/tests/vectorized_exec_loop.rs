use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::execution::context::SessionContext;

use floe_executor::{
    ConsolidationMode, DynamicStateTableProvider, VectorizedPlanExecutor, VectorizedTickOutput,
};

fn state_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn delta_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("__weight", DataType::Int64, false),
    ]))
}

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
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

fn delta_batch(schema: &SchemaRef, rows: &[(i64, i64)]) -> RecordBatch {
    let ids = Int64Array::from(rows.iter().map(|(id, _)| *id).collect::<Vec<_>>());
    let weights = Int64Array::from(rows.iter().map(|(_, weight)| *weight).collect::<Vec<_>>());
    RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(ids), Arc::new(weights)])
        .expect("delta batch")
}

fn output_rows(output: &VectorizedTickOutput) -> Vec<(i64, String, i64)> {
    let mut rows = Vec::new();
    for batch in &output.batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id array");
        let payloads = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload array");
        let weights = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("weight array");
        for row_idx in 0..batch.num_rows() {
            rows.push((
                ids.value(row_idx),
                payloads.value(row_idx).to_string(),
                weights.value(row_idx),
            ));
        }
    }
    rows.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    rows
}

fn apply_output(state: &mut HashMap<(i64, String), i64>, rows: &[(i64, String, i64)]) {
    for (id, payload, weight) in rows {
        let key = (*id, payload.clone());
        let entry = state.entry(key.clone()).or_insert(0);
        *entry += *weight;
        if *entry == 0 {
            state.remove(&key);
        }
    }
}

async fn build_executor() -> Result<(
    Arc<DynamicStateTableProvider>,
    Arc<DynamicStateTableProvider>,
    VectorizedPlanExecutor,
)> {
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
        .sql("SELECT s.id, s.payload, d.__weight FROM state s JOIN delta d ON s.id = d.id")
        .await?;
    let plan = df.create_physical_plan().await?;
    let executor = VectorizedPlanExecutor::new(
        plan,
        ctx.task_ctx(),
        output_schema(),
        ConsolidationMode::ByAllColumns,
    )?;
    Ok((state_provider, delta_provider, executor))
}

#[tokio::test]
async fn reuses_physical_plan_across_ticks() -> Result<()> {
    let (state_provider, delta_provider, executor) = build_executor().await?;
    let plan_ptr = executor.plan_ptr();

    let state = state_schema();
    let delta = delta_schema();
    state_provider.set_batches(vec![state_batch(&state, &[(1, "a"), (2, "b")])]);
    delta_provider.set_batches(vec![delta_batch(&delta, &[(1, 1), (1, -1), (2, 1)])]);
    let tick_one = executor.run_tick().await?;
    assert_eq!(executor.plan_ptr(), plan_ptr);
    assert_eq!(output_rows(&tick_one), vec![(2, "b".to_string(), 1)]);
    assert_eq!(tick_one.stats.input_rows, 3);
    assert_eq!(tick_one.stats.grouped_rows, 2);
    assert_eq!(tick_one.stats.output_rows, 1);
    assert_eq!(tick_one.stats.zero_weight_dropped_rows, 1);

    delta_provider.set_batches(vec![delta_batch(&delta, &[(1, 2), (2, -1)])]);
    let tick_two = executor.run_tick().await?;
    assert_eq!(executor.plan_ptr(), plan_ptr);
    assert_eq!(
        output_rows(&tick_two),
        vec![(1, "a".to_string(), 2), (2, "b".to_string(), -1)]
    );
    Ok(())
}

#[tokio::test]
async fn steady_state_loop_updates_state_and_stays_consistent() -> Result<()> {
    let (state_provider, delta_provider, executor) = build_executor().await?;
    let state = state_schema();
    let delta = delta_schema();

    let mut materialized: HashMap<(i64, String), i64> = HashMap::new();

    state_provider.set_batches(vec![state_batch(&state, &[(1, "a"), (2, "b")])]);
    delta_provider.set_batches(vec![delta_batch(&delta, &[(2, 1)])]);
    let tick_one = executor.run_tick().await?;
    apply_output(&mut materialized, &output_rows(&tick_one));
    assert_eq!(materialized.get(&(2, "b".to_string())), Some(&1));

    state_provider.set_batches(vec![state_batch(&state, &[(1, "a"), (2, "b"), (3, "c")])]);
    delta_provider.set_batches(vec![delta_batch(
        &delta,
        &[(3, 1), (3, 1), (1, 1), (1, -1)],
    )]);
    let tick_two = executor.run_tick().await?;
    let tick_two_rows = output_rows(&tick_two);
    assert_eq!(tick_two_rows, vec![(3, "c".to_string(), 2)]);
    apply_output(&mut materialized, &tick_two_rows);

    // Re-running with identical snapshots should produce identical consolidated output.
    let tick_two_repeat = executor.run_tick().await?;
    assert_eq!(output_rows(&tick_two_repeat), tick_two_rows);
    assert_eq!(tick_two_repeat.stats.zero_weight_dropped_rows, 1);

    assert_eq!(materialized.get(&(2, "b".to_string())), Some(&1));
    assert_eq!(materialized.get(&(3, "c".to_string())), Some(&2));
    Ok(())
}
