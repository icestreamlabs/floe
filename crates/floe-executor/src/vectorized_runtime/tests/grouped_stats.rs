#[tokio::test]
async fn grouped_stats_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 30, 100])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("total_bids", DataType::Int64, false),
        Field::new("cheap_bids", DataType::Int64, false),
        Field::new("min_price", DataType::Int64, true),
        Field::new("max_price", DataType::Int64, true),
        Field::new("avg_price", DataType::Float64, true),
        Field::new("sum_price", DataType::Int64, true),
    ]));
    let query = "SELECT auction, \
        COUNT(*) AS total_bids, \
        COUNT(*) FILTER (WHERE price < 50) AS cheap_bids, \
        MIN(price) AS min_price, \
        MAX(price) AS max_price, \
        AVG(price) AS avg_price, \
        SUM(price) AS sum_price \
        FROM bids GROUP BY auction";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_bid_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_bid_stats").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(
        grouped_stats_rows(&snapshot),
        vec![(1, 2, 2, 10, 30, 20.0, 40), (2, 1, 0, 100, 100, 100.0, 100),]
    );

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![50])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(
        grouped_stats_rows(&snapshot),
        vec![(1, 3, 2, 10, 50, 30.0, 90), (2, 1, 0, 100, 100, 100.0, 100),]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_bid_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_bid_stats")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        grouped_stats_rows(&recovered_snapshot),
        vec![(1, 3, 2, 10, 50, 30.0, 90), (2, 1, 0, 100, 100, 100.0, 100),]
    );

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![50])),
        ],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(
        grouped_stats_rows(&snapshot),
        vec![(1, 2, 2, 10, 30, 20.0, 40), (2, 1, 0, 100, 100, 100.0, 100),]
    );
}
#[tokio::test]
async fn grouped_stats_can_publish_columnar_versions_without_arrow_snapshots() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 30, 100])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-no-arrow").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("total_bids", DataType::Int64, false),
        Field::new("sum_price", DataType::Int64, true),
    ]));
    let query = "SELECT auction, COUNT(*) AS total_bids, SUM(price) AS sum_price \
        FROM bids GROUP BY auction";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_bid_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default()
            .with_operator_state_table(table)
            .without_grouped_stats_arrow_snapshots(),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_bid_stats").expect("materialized view");
    assert!(handle.arrow_snapshot_for(1).is_none());
    assert_eq!(handle.authoritative_row_count_for(1), Some(2));
    let snapshot = scan_materialized_view_table(
        Arc::clone(&registry),
        "mv_bid_stats",
        Arc::clone(&output_schema),
        "SELECT auction, total_bids, sum_price FROM mv_bid_stats",
    )
    .await;
    assert_eq!(id_count_sum_rows(&snapshot), vec![(1, 2, 40), (2, 1, 100)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![50])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    assert!(handle.arrow_snapshot_for(2).is_none());
    assert_eq!(handle.authoritative_row_count_for(2), Some(2));
    let snapshot = scan_materialized_view_table(
        Arc::clone(&registry),
        "mv_bid_stats",
        output_schema,
        "SELECT auction, total_bids, sum_price FROM mv_bid_stats",
    )
    .await;
    assert_eq!(id_count_sum_rows(&snapshot), vec![(1, 3, 90), (2, 1, 100)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 2, -1), (1, 3, 1)]);
}

#[tokio::test]
async fn append_only_grouped_stats_recovers_from_dense_compact_state_snapshot() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition")
    .with_property(SOURCE_APPEND_ONLY_PROPERTY, "true");
    let schema = definition.to_arrow_schema();
    let group_count = 1024_i64;
    let auctions = (0..group_count).collect::<Vec<_>>();
    let prices = auctions
        .iter()
        .map(|auction| auction * 10)
        .collect::<Vec<_>>();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(auctions.clone())),
            Arc::new(Int64Array::from(prices)),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-grouped-stats-dense-compact-snapshot")
            .await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("total_bids", DataType::Int64, false),
        Field::new("sum_price", DataType::Int64, true),
    ]));
    let query = "SELECT auction, COUNT(*) AS total_bids, SUM(price) AS sum_price \
        FROM bids GROUP BY auction";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_bid_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default()
            .with_operator_state_table(Arc::clone(&table))
            .without_grouped_stats_arrow_snapshots(),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_bid_stats").expect("materialized view");
    assert!(handle.arrow_snapshot_for(1).is_none());
    assert_eq!(
        handle.authoritative_row_count_for(1),
        Some(group_count as usize)
    );

    let logged_insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(Int64Array::from(vec![7000])),
        ],
    )
    .expect("logged source insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bids",
            vec![logged_insert.clone()],
            vec![logged_insert],
        )
        .await
        .expect("append logged source rows");
    runtime.run_tick(2).await.expect("logged tick");

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_bid_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default()
            .with_operator_state_table(table)
            .without_grouped_stats_arrow_snapshots(),
    )
    .await
    .expect("recovered runtime");

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(Int64Array::from(vec![9000])),
        ],
    )
    .expect("recovered source insert batch");
    recovered
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append recovered source rows");
    recovered.run_tick(3).await.expect("recovered tick");

    let snapshot = scan_materialized_view_table(
        Arc::clone(&recovery_registry),
        "mv_bid_stats",
        output_schema,
        "SELECT auction, total_bids, sum_price FROM mv_bid_stats WHERE auction = 7",
    )
    .await;
    assert_eq!(id_count_sum_rows(&snapshot), vec![(7, 3, 16070)]);
}

#[tokio::test]
async fn append_only_grouped_stats_recovers_distinct_presence_segments() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition")
    .with_property(SOURCE_APPEND_ONLY_PROPERTY, "true");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-grouped-stats-append-distinct-segments")
            .await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("total_bids", DataType::Int64, false),
        Field::new("distinct_bidders", DataType::Int64, false),
    ]));
    let query = "SELECT auction, COUNT(*) AS total_bids, \
        COUNT(DISTINCT bidder) AS distinct_bidders FROM bids GROUP BY auction";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_bid_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_bid_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(Int64Array::from(vec![20, 30])),
        ],
    )
    .expect("recovered source insert batch");
    recovered
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append recovered source rows");
    recovered.run_tick(2).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_bid_stats")
        .expect("recovered materialized view");
    let snapshot = recovered_handle
        .arrow_snapshot_for(2)
        .expect("recovered snapshot");
    assert_eq!(id_count_sum_rows(&snapshot), vec![(1, 4, 3)]);
}

#[tokio::test]
async fn append_only_grouped_stats_rejects_negative_source_delta() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition")
    .with_property(SOURCE_APPEND_ONLY_PROPERTY, "true");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-append-only").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("total_bids", DataType::Int64, false),
        Field::new("min_price", DataType::Int64, true),
    ]));
    let query = "SELECT auction, COUNT(*) AS total_bids, MIN(price) AS min_price \
        FROM bids GROUP BY auction";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_bid_stats",
            query,
            Arc::clone(&output_schema),
        )],
        registry,
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("runtime");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    runtime
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    let err = runtime
        .run_tick(2)
        .await
        .expect_err("append-only grouped-stats should reject retractions");
    let err = format!("{err:#}");
    assert!(
        err.contains("append-only grouped-stats"),
        "unexpected error: {err:#}"
    );
}

#[tokio::test]
async fn sum_group_by_uses_slate_backed_grouped_stats_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-sum").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
    ]));
    let query = "SELECT id, SUM(amount) AS total FROM orders GROUP BY id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_order_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
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

    let handle = registry.get("mv_order_totals").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 30), (2, 5)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![5])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 35), (2, 5)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 30, -1), (1, 35, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_order_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_totals")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 35), (2, 5)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![5])),
        ],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 30), (2, 5)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 30, 1), (1, 35, -1)]
    );
}

#[tokio::test]
async fn ordered_sum_group_by_uses_slate_backed_grouped_stats_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-ordered-sum").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
    ]));
    let query = "SELECT id, SUM(amount) AS total \
        FROM orders \
        GROUP BY id \
        ORDER BY id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_order_totals_ordered",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
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

    let handle = registry
        .get("mv_order_totals_ordered")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 30), (2, 5)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![5])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 35), (2, 5)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 30, -1), (1, 35, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_order_totals_ordered",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_totals_ordered")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 35), (2, 5)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![5])),
        ],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 30), (2, 5)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 30, 1), (1, 35, -1)]
    );
}

#[tokio::test]
async fn having_grouped_stats_uses_slate_backed_post_aggregate_filter_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 5, 25])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-having").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
    ]));
    let query = "SELECT id, SUM(amount) AS total \
        FROM orders GROUP BY id HAVING SUM(amount) >= 20";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_large_order_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
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

    let handle = registry
        .get("mv_large_order_totals")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(2, 25)]);

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
    assert_eq!(id_count_rows(&snapshot), vec![(1, 25), (2, 25)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 25, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_large_order_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_large_order_totals")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 25), (2, 25)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(2, 25)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 25, -1)]);
}

#[tokio::test]
async fn final_aggregate_projection_uses_slate_backed_grouped_stats_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-grouped-stats-final-projection").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("adjusted_total", DataType::Int64, true),
    ]));
    let query = "SELECT id, SUM(amount) + 1 AS adjusted_total FROM orders GROUP BY id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_adjusted_order_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
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

    let handle = registry
        .get("mv_adjusted_order_totals")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 31), (2, 6)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![5])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 36), (2, 6)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 31, -1), (1, 36, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_adjusted_order_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_adjusted_order_totals")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 36), (2, 6)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![5])),
        ],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 31), (2, 6)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 31, 1), (1, 36, -1)]
    );
}

#[tokio::test]
async fn aggregate_subquery_having_projection_uses_slate_backed_grouped_stats_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, true),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![Some(10), None, Some(20), None])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-grouped-stats-subquery-having").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("total", DataType::Int64, true),
    ]));
    let query = "SELECT id, total FROM (\
        SELECT id, SUM(amount) AS total, COUNT(amount) AS amount_count, AVG(amount) AS avg_amount \
        FROM orders GROUP BY id\
    ) a WHERE amount_count > 1";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_order_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
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

    let handle = registry.get("mv_order_totals").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 30)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 2])),
            Arc::new(Int64Array::from(vec![Some(5), Some(15)])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 30), (2, 20)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(2, 20, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_order_totals",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_totals")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 30), (2, 20)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![Some(20)])),
        ],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(2, 20)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 30, -1)]);
}

#[tokio::test]
async fn global_count_uses_slate_backed_grouped_stats_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "amount",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![10, 20, 5]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-global-count").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "total",
        DataType::Int64,
        false,
    )]));
    let query = "SELECT COUNT(*) AS total FROM orders";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_order_count",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
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

    let handle = registry.get("mv_order_count").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![3]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![7]))],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![4]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(3, -1), (4, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_order_count",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_count")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![4]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![7]))],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![3]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(3, 1), (4, -1)]);
}

#[tokio::test]
async fn global_sum_uses_slate_backed_grouped_stats_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "amount",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![10, 20, 5]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-global-sum").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "total",
        DataType::Int64,
        true,
    )]));
    let query = "SELECT SUM(amount) AS total FROM orders";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_order_sum",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
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

    let handle = registry.get("mv_order_sum").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![35]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![7]))],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![42]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(35, -1), (42, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_order_sum",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_sum")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![42]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![7]))],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![35]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(35, 1), (42, -1)]);
}

#[tokio::test]
async fn grouped_stats_supports_distinct_counts_and_string_max_incrementally() {
    let definition = SourceDefinition::new(
        "events",
        vec![
            SourceColumn::new_nullable("channel", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("day", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("minute", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["apple", "apple", "apple"])),
            Arc::new(StringArray::from(vec![
                "2026-06-08",
                "2026-06-08",
                "2026-06-08",
            ])),
            Arc::new(StringArray::from(vec!["10:00", "10:05", "09:55"])),
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![100, 101, 100])),
            Arc::new(Int64Array::from(vec![50, 150, 75])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-distinct").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Utf8, false),
        Field::new("minute", DataType::Utf8, true),
        Field::new("total_bids", DataType::Int64, false),
        Field::new("cheap_bids", DataType::Int64, false),
        Field::new("total_bidders", DataType::Int64, false),
        Field::new("cheap_bidders", DataType::Int64, false),
        Field::new("total_auctions", DataType::Int64, false),
    ]));
    let query = "SELECT channel, day, \
        MAX(minute) AS minute, \
        COUNT(*) AS total_bids, \
        COUNT(*) FILTER (WHERE price < 100) AS cheap_bids, \
        COUNT(DISTINCT bidder) AS total_bidders, \
        COUNT(DISTINCT bidder) FILTER (WHERE price < 100) AS cheap_bidders, \
        COUNT(DISTINCT auction) AS total_auctions \
        FROM events GROUP BY channel, day";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_event_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "events",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_event_stats").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(
        distinct_stats_rows(&snapshot),
        vec![(
            "apple".to_string(),
            "2026-06-08".to_string(),
            "10:05".to_string(),
            3,
            2,
            2,
            2,
            2,
        )]
    );

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["apple"])),
            Arc::new(StringArray::from(vec!["2026-06-08"])),
            Arc::new(StringArray::from(vec!["10:10"])),
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![102])),
            Arc::new(Int64Array::from(vec![80])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("events", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(
        distinct_stats_rows(&snapshot),
        vec![(
            "apple".to_string(),
            "2026-06-08".to_string(),
            "10:10".to_string(),
            4,
            3,
            3,
            3,
            3,
        )]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_event_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_event_stats")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        distinct_stats_rows(&recovered_snapshot),
        vec![(
            "apple".to_string(),
            "2026-06-08".to_string(),
            "10:10".to_string(),
            4,
            3,
            3,
            3,
            3,
        )]
    );

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["apple"])),
            Arc::new(StringArray::from(vec!["2026-06-08"])),
            Arc::new(StringArray::from(vec!["10:00"])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![100])),
            Arc::new(Int64Array::from(vec![50])),
        ],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("events", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(
        distinct_stats_rows(&snapshot),
        vec![(
            "apple".to_string(),
            "2026-06-08".to_string(),
            "10:10".to_string(),
            3,
            2,
            3,
            2,
            3,
        )]
    );
}

#[tokio::test]
async fn grouped_stats_supports_string_distinct_count_incrementally() {
    let definition = SourceDefinition::new(
        "events",
        vec![SourceColumn::new_nullable(
            "channel",
            SourceDataType::Utf8,
            true,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![
            Some("web"),
            Some("web"),
            Some("mobile"),
            None,
        ]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-grouped-stats-string-distinct").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "distinct_channels",
        DataType::Int64,
        false,
    )]));
    let query = "SELECT COUNT(DISTINCT channel) AS distinct_channels FROM events";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_distinct_channels",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "events",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_distinct_channels")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![
            Some("email"),
            Some("web"),
        ]))],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("events", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![3]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1), (3, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_distinct_channels",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_distinct_channels")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![3]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![Some("mobile")]))],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("events", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, 1), (3, -1)]);
}

#[tokio::test]
async fn grouped_stats_supports_timestamp_min_max_incrementally() {
    let definition = SourceDefinition::new(
        "events",
        vec![SourceColumn::new_nullable(
            "event_time",
            SourceDataType::TimestampMillis,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(TimestampMillisecondArray::from(vec![
            1000, 500, 750,
        ]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-grouped-stats-timestamp-minmax").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let ts_type = DataType::Timestamp(TimeUnit::Millisecond, None);
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("first_ts", ts_type.clone(), false),
        Field::new("last_ts", ts_type, false),
    ]));
    let query = "SELECT MIN(event_time) AS first_ts, MAX(event_time) AS last_ts FROM events";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_event_bounds",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "events",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_event_bounds").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(timestamp_pair_rows(&snapshot), vec![(500, 1000)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(TimestampMillisecondArray::from(vec![400, 1200]))],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("events", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(timestamp_pair_rows(&snapshot), vec![(400, 1200)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_timestamp_pair_rows(&delta),
        vec![(400, 1200, 1), (500, 1000, -1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_event_bounds",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_event_bounds")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(timestamp_pair_rows(&recovered_snapshot), vec![(400, 1200)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(TimestampMillisecondArray::from(vec![400]))],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("events", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(timestamp_pair_rows(&snapshot), vec![(500, 1200)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_timestamp_pair_rows(&delta),
        vec![(400, 1200, -1), (500, 1200, 1)]
    );
}

#[tokio::test]
async fn grouped_stats_supports_date_min_max_incrementally() {
    let definition = SourceDefinition::new(
        "events",
        vec![SourceColumn::new_nullable(
            "event_day",
            SourceDataType::DateDays,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Date32Array::from(vec![10, 5, 7]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-date-minmax").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("first_day", DataType::Date32, false),
        Field::new("last_day", DataType::Date32, false),
    ]));
    let query = "SELECT MIN(event_day) AS first_day, MAX(event_day) AS last_day FROM events";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_event_days",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "events",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_event_days").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(date_pair_rows(&snapshot), vec![(5, 10)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Date32Array::from(vec![4, 12]))],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("events", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(date_pair_rows(&snapshot), vec![(4, 12)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_date_pair_rows(&delta),
        vec![(4, 12, 1), (5, 10, -1)]
    );
}

#[tokio::test]
async fn grouped_stats_supports_timestamp_distinct_count_incrementally() {
    let definition = SourceDefinition::new(
        "events",
        vec![SourceColumn::new_nullable(
            "event_time",
            SourceDataType::TimestampMillis,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(TimestampMillisecondArray::from(vec![
            1000, 1000, 500,
        ]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-grouped-stats-timestamp-distinct").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "distinct_times",
        DataType::Int64,
        false,
    )]));
    let query = "SELECT COUNT(DISTINCT event_time) AS distinct_times FROM events";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_distinct_times",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "events",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_distinct_times")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(TimestampMillisecondArray::from(vec![750, 1000]))],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("events", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![3]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1), (3, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_distinct_times",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_distinct_times")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![3]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(TimestampMillisecondArray::from(vec![500]))],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("events", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, 1), (3, -1)]);
}

#[tokio::test]
async fn grouped_stats_supports_boolean_distinct_count_incrementally() {
    let definition = SourceDefinition::new(
        "events",
        vec![SourceColumn::new_nullable(
            "active",
            SourceDataType::Bool,
            true,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(BooleanArray::from(vec![Some(true), None]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-grouped-stats-boolean-distinct").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "distinct_flags",
        DataType::Int64,
        false,
    )]));
    let query = "SELECT COUNT(DISTINCT active) AS distinct_flags FROM events";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_distinct_flags",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "events",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_distinct_flags")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(BooleanArray::from(vec![Some(false)]))],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("events", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(1, -1), (2, 1)]);
}

#[tokio::test]
async fn grouped_stats_supports_decimal_stats_incrementally() {
    let definition = SourceDefinition::new(
        "payments",
        vec![SourceColumn::new_nullable(
            "amount",
            SourceDataType::Decimal128 {
                precision: 10,
                scale: 2,
            },
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let amount_type = DataType::Decimal128(10, 2);
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(
            Decimal128Array::from(vec![1000_i128, 2000, 1000]).with_data_type(amount_type.clone()),
        )],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-stats-decimal").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("total_amount", DataType::Decimal128(20, 2), false),
        Field::new("min_amount", amount_type.clone(), false),
        Field::new("max_amount", amount_type.clone(), false),
        Field::new("distinct_amounts", DataType::Int64, false),
    ]));
    let query = "SELECT SUM(amount) AS total_amount, MIN(amount) AS min_amount, MAX(amount) AS max_amount, COUNT(DISTINCT amount) AS distinct_amounts FROM payments";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_payment_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "payments",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_payment_stats").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(decimal_stats_rows(&snapshot), vec![(4000, 1000, 2000, 2)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(
            Decimal128Array::from(vec![500_i128, 3000]).with_data_type(amount_type.clone()),
        )],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query(
            "payments",
            vec![insert.clone()],
            vec![insert],
        )
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(decimal_stats_rows(&snapshot), vec![(7500, 500, 3000, 4)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_decimal_stats_rows(&delta),
        vec![(4000, 1000, 2000, 2, -1), (7500, 500, 3000, 4, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_payment_stats",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_payment_stats")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        decimal_stats_rows(&recovered_snapshot),
        vec![(7500, 500, 3000, 4)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(
            Decimal128Array::from(vec![500_i128]).with_data_type(amount_type),
        )],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("payments", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(decimal_stats_rows(&snapshot), vec![(7000, 1000, 3000, 3)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_decimal_stats_rows(&delta),
        vec![(7000, 1000, 3000, 3, 1), (7500, 500, 3000, 4, -1)]
    );
}
