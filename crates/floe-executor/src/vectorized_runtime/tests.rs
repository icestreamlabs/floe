use super::*;
use crate::source_decoder::{SourceArrowBatchBuilder, SourceArrowBatches};
use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use dbsp::circuit::WEIGHT_COLUMN_NAME;
use dbsp::storage::{KeyValueTable, SlateTable};
use floe_core::source::{
    AppendIngestEvent, SourceColumn, SourceDataType, SourceDefinition, SourceRegistry,
};
use object_store::memory::InMemory;
use serde_json::json;
use slatedb::Db;

fn int64_values(batch: &RecordBatch, column_idx: usize) -> Vec<i64> {
    let values = batch
        .column(column_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    (0..values.len()).map(|idx| values.value(idx)).collect()
}

fn string_values(batch: &RecordBatch, column_idx: usize) -> Vec<String> {
    let values = batch
        .column(column_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    (0..values.len())
        .map(|idx| values.value(idx).to_string())
        .collect()
}

fn id_note_rows(batches: &[RecordBatch]) -> Vec<(i64, String)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let ids = int64_values(batch, 0);
        let notes = string_values(batch, 1);
        rows.extend(ids.into_iter().zip(notes));
    }
    rows.sort();
    rows
}

fn id_count_rows(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let ids = int64_values(batch, 0);
        let counts = int64_values(batch, 1);
        rows.extend(ids.into_iter().zip(counts));
    }
    rows.sort();
    rows
}

fn weighted_id_note_rows(batches: &[RecordBatch]) -> Vec<(i64, String, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let ids = int64_values(batch, 0);
        let notes = string_values(batch, 1);
        let weights = int64_values(batch, weight_idx);
        rows.extend(
            ids.into_iter()
                .zip(notes)
                .zip(weights)
                .map(|((id, note), weight)| (id, note, weight)),
        );
    }
    rows.sort();
    rows
}

fn weighted_id_count_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let weight_idx = batch
            .schema()
            .index_of(WEIGHT_COLUMN_NAME)
            .expect("weight column");
        let ids = int64_values(batch, 0);
        let counts = int64_values(batch, 1);
        let weights = int64_values(batch, weight_idx);
        rows.extend(
            ids.into_iter()
                .zip(counts)
                .zip(weights)
                .map(|((id, count), weight)| (id, count, weight)),
        );
    }
    rows.sort();
    rows
}

async fn build_operator_state_table(name: &str) -> Arc<dyn KeyValueTable> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
    Arc::new(SlateTable::new(db))
}

#[tokio::test]
async fn pruned_execution_batches_do_not_prune_query_provider() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("note", SourceDataType::Utf8),
        ],
    )
    .expect("source definition");
    let required_columns = Some(Arc::<[bool]>::from(vec![true, false]));
    let mut builder = SourceArrowBatchBuilder::new_with_execution_required_columns(
        definition.clone(),
        1,
        required_columns,
    );
    builder
        .append_event(&AppendIngestEvent::new(
            "orders",
            json!({"id": 1, "note": "kept"}),
        ))
        .expect("append source event");
    let batches = builder
        .finish()
        .expect("finish source batches")
        .expect("source batches");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_orders",
            "SELECT id FROM orders",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_source_query_tables(),
    )
    .await
    .expect("runtime");

    let SourceArrowBatches::ExecutionAndQuery { execution, query } = batches else {
        panic!("expected execution and query batches");
    };
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![execution], vec![query])
        .await
        .expect("append source batches");
    runtime.run_tick(1).await.expect("run vectorized tick");

    let handle = registry.get("mv_orders").expect("materialized view");
    let version = handle.latest_version().expect("mv version");
    let snapshot = handle.arrow_snapshot_for(version).expect("mv snapshot");
    let id = snapshot[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("mv id column")
        .value(0);
    assert_eq!(id, 1);

    let provider = runtime
        .table_providers()
        .into_iter()
        .find_map(|(name, provider)| (name == "orders").then_some(provider))
        .expect("orders query provider");
    let ctx = SessionContext::new();
    ctx.register_table("orders", provider)
        .expect("register query provider");
    let batches = ctx
        .sql("SELECT note FROM orders")
        .await
        .expect("query provider sql")
        .collect()
        .await
        .expect("collect query provider rows");
    let note = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("note column")
        .value(0);
    assert_eq!(note, "kept");
}

#[tokio::test]
async fn primary_key_cdc_delta_updates_filter_project_mv_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("amount", SourceDataType::Int64),
        ],
    )
    .expect("source definition")
    .with_property(SOURCE_PRIMARY_KEY_PROPERTY, "id");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 30])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_orders",
            "SELECT id, amount FROM orders WHERE amount >= 20",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_source_query_tables(),
    )
    .await
    .expect("runtime");

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let update_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 40, 30])),
        ],
    )
    .expect("cdc source rows");
    let weighted = weighted_batch_from_diffs(&update_rows, &weighted_schema, &[-1, 1, -1])
        .expect("weighted cdc rows");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply cdc delta");
    runtime.run_tick(2).await.expect("cdc tick");

    let handle = registry.get("mv_orders").expect("materialized view");
    let version = handle.latest_version().expect("mv version");
    let snapshot = handle.arrow_snapshot_for(version).expect("mv snapshot");
    assert_eq!(snapshot.len(), 1);
    assert_eq!(int64_values(&snapshot[0], 0), vec![1]);
    assert_eq!(int64_values(&snapshot[0], 1), vec![40]);

    let delta = handle.arrow_delta_for(2).expect("mv delta");
    let delta = delta
        .iter()
        .filter(|batch| batch.num_rows() > 0)
        .collect::<Vec<_>>();
    assert_eq!(delta.len(), 1);
    let weight_idx = delta[0]
        .schema()
        .index_of(WEIGHT_COLUMN_NAME)
        .expect("weight column");
    assert_eq!(int64_values(delta[0], 0), vec![1, 2]);
    assert_eq!(int64_values(delta[0], 1), vec![40, 30]);
    assert_eq!(int64_values(delta[0], weight_idx), vec![1, -1]);

    let provider = runtime
        .table_providers()
        .into_iter()
        .find_map(|(name, provider)| (name == "orders").then_some(provider))
        .expect("orders query provider");
    let ctx = SessionContext::new();
    ctx.register_table("orders", provider)
        .expect("register orders provider");
    let source_rows = ctx
        .sql("SELECT id, amount FROM orders ORDER BY id")
        .await
        .expect("source query")
        .collect()
        .await
        .expect("collect source rows");
    assert_eq!(source_rows.len(), 1);
    assert_eq!(int64_values(&source_rows[0], 0), vec![1]);
    assert_eq!(int64_values(&source_rows[0], 1), vec![40]);
}

#[tokio::test]
async fn filter_project_uses_slate_backed_columnar_stateless_operator_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("note", SourceDataType::Utf8),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 4])),
            Arc::new(StringArray::from(vec!["a", "b", "d"])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-stateless").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::clone(&schema);
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_orders",
            "SELECT id, note FROM orders WHERE id >= 2",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarStateless
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_orders").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(
        id_note_rows(&snapshot),
        vec![(2, "b".to_string()), (4, "d".to_string())]
    );

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let source_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 3])),
            Arc::new(StringArray::from(vec!["b", "c"])),
        ],
    )
    .expect("source delta rows");
    let weighted = weighted_batch_from_diffs(&source_rows, &weighted_schema, &[-1, 1])
        .expect("weighted source rows");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted delta");
    runtime.run_tick(2).await.expect("weighted tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(
        id_note_rows(&snapshot),
        vec![(3, "c".to_string()), (4, "d".to_string())]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_note_rows(&delta),
        vec![(2, "b".to_string(), -1), (3, "c".to_string(), 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_orders",
            "SELECT id, note FROM orders WHERE id >= 2",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarStateless
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_orders")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        id_note_rows(&recovered_snapshot),
        vec![(3, "c".to_string()), (4, "d".to_string())]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn count_group_by_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 1, 2]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-count").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
    ]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_order_counts",
            "SELECT id, COUNT(*) AS count FROM orders GROUP BY id",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_order_counts").expect("materialized view");
    let version = handle.latest_version().expect("mv version");
    let snapshot = handle.arrow_snapshot_for(version).expect("mv snapshot");
    assert_eq!(snapshot.len(), 1);
    assert_eq!(int64_values(&snapshot[0], 0), vec![1, 2]);
    assert_eq!(int64_values(&snapshot[0], 1), vec![2, 1]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let source_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .expect("source delta rows");
    let weighted = weighted_batch_from_diffs(&source_rows, &weighted_schema, &[-1, 1, 1])
        .expect("weighted source rows");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted delta");
    runtime.run_tick(2).await.expect("weighted tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(snapshot.len(), 1);
    assert_eq!(int64_values(&snapshot[0], 0), vec![1, 2, 3]);
    assert_eq!(int64_values(&snapshot[0], 1), vec![1, 2, 1]);

    let delta = handle.arrow_delta_for(2).expect("mv delta");
    let delta = delta
        .iter()
        .filter(|batch| batch.num_rows() > 0)
        .collect::<Vec<_>>();
    assert_eq!(delta.len(), 1);
    let weight_idx = delta[0]
        .schema()
        .index_of(WEIGHT_COLUMN_NAME)
        .expect("weight column");
    assert_eq!(int64_values(delta[0], 0), vec![1, 1, 2, 2, 3]);
    assert_eq!(int64_values(delta[0], 1), vec![2, 1, 1, 2, 1]);
    assert_eq!(int64_values(delta[0], weight_idx), vec![-1, 1, -1, 1, 1]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_order_counts",
            "SELECT id, COUNT(*) AS count FROM orders GROUP BY id",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_counts")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(recovered_snapshot.len(), 1);
    assert_eq!(int64_values(&recovered_snapshot[0], 0), vec![1, 2, 3]);
    assert_eq!(int64_values(&recovered_snapshot[0], 1), vec![1, 2, 1]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn grouped_count_with_hidden_key_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("ts", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 10])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-count-hidden").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
    ]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_order_counts",
            "SELECT id, COUNT(*) AS count FROM orders GROUP BY id, ts",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_order_counts").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 1), (1, 1), (2, 1)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 1), (1, 2), (2, 1)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 1, -1), (1, 2, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_order_counts",
            "SELECT id, COUNT(*) AS count FROM orders GROUP BY id, ts",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].execution_mode,
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_counts")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        id_count_rows(&recovered_snapshot),
        vec![(1, 1), (1, 2), (2, 1)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let hidden_key_insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![20])),
        ],
    )
    .expect("hidden key insert rows");
    recovered
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![hidden_key_insert.clone()],
            vec![hidden_key_insert],
        )
        .await
        .expect("append hidden key source rows");
    recovered.run_tick(4).await.expect("post-recovery tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-recovery snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 2), (1, 2), (2, 1)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-recovery delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 1, -1), (1, 2, 1)]);
}

#[tokio::test]
async fn count_group_by_requires_slate_backed_operator_state_table() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("source definition");
    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
    ]));

    let result = VectorizedExecutionRuntime::new(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_order_counts",
            "SELECT id, COUNT(*) AS count FROM orders GROUP BY id",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
    )
    .await;

    let err = match result {
        Ok(_) => panic!("count MV should require SlateDB-backed operator state"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("requires SlateDB-backed operator state"),
        "{err:#}"
    );
}

#[tokio::test]
async fn source_query_tables_are_not_maintained_by_default() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new("id", SourceDataType::Int64)],
    )
    .expect("source definition");
    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let runtime = VectorizedExecutionRuntime::new(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_orders",
            "SELECT id FROM orders",
            Arc::clone(&output_schema),
        )],
        registry,
    )
    .await
    .expect("runtime");

    assert!(runtime.table_providers().is_empty());
}

#[test]
fn weighted_batch_from_diffs_rejects_non_unit_weights() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .expect("source batch");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");

    let err = weighted_batch_from_diffs(&batch, &weighted_schema, &[2])
        .expect_err("non-unit diffs should be rejected");

    assert!(
        err.to_string().contains("diff must be -1, 0, or 1"),
        "{err:#}"
    );
}

#[tokio::test]
async fn non_incremental_mv_requires_explicit_full_refresh_policy() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("amount", SourceDataType::Int64),
        ],
    )
    .expect("source definition");
    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
    ]));

    let result = VectorizedExecutionRuntime::new(
        &sources,
        vec![VectorizedMaterializedViewPlan::new(
            "mv_order_totals",
            "SELECT id, SUM(amount) AS total FROM orders GROUP BY id",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
    )
    .await;

    let err = match result {
        Ok(_) => panic!("aggregate MV should require explicit full-refresh policy"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("requires full-refresh"), "{err:#}");
}

#[tokio::test]
async fn explicit_full_refresh_policy_allows_non_incremental_mv() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("amount", SourceDataType::Int64),
        ],
    )
    .expect("source definition");
    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
    ]));

    VectorizedExecutionRuntime::new(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::new(
                "mv_order_totals",
                "SELECT id, SUM(amount) AS total FROM orders GROUP BY id",
                Arc::clone(&output_schema),
            )
            .allow_full_refresh(),
        ],
        Arc::clone(&registry),
    )
    .await
    .expect("explicit full-refresh policy should allow aggregate MV planning");
}
