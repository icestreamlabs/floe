#[tokio::test]
async fn global_topn_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            Arc::new(Int64Array::from(vec![10, 20, 15, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-global-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let query = "SELECT auction, bidder, price FROM bids ORDER BY price DESC LIMIT 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_bids",
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
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_top_bids").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(bid_topn_rows(&snapshot), vec![(1, 20, 20), (2, 30, 15)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4, 5])),
            Arc::new(Int64Array::from(vec![50, 60])),
            Arc::new(Int64Array::from(vec![25, 7])),
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
    assert_eq!(bid_topn_rows(&snapshot), vec![(1, 20, 20), (4, 50, 25)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_bid_topn_rows(&delta),
        vec![(2, 30, 15, -1), (4, 50, 25, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_bids",
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
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_top_bids")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        bid_topn_rows(&recovered_snapshot),
        vec![(1, 20, 20), (4, 50, 25)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(Int64Array::from(vec![50])),
            Arc::new(Int64Array::from(vec![25])),
        ],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(bid_topn_rows(&snapshot), vec![(1, 20, 20), (2, 30, 15)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_bid_topn_rows(&delta),
        vec![(2, 30, 15, 1), (4, 50, 25, -1)]
    );
}
#[tokio::test]
async fn hidden_sort_key_topn_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            Arc::new(Int64Array::from(vec![10, 20, 15, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-hidden-sort-key-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "auction",
        DataType::Int64,
        false,
    )]));
    let query = "SELECT auction FROM bids ORDER BY price DESC LIMIT 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_hidden_sort_top_bids",
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
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_hidden_sort_top_bids")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2, 3]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![5, 6])),
            Arc::new(Int64Array::from(vec![50, 60])),
            Arc::new(Int64Array::from(vec![25, 7])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2, 5]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(3, -1), (5, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_hidden_sort_top_bids",
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
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_hidden_sort_top_bids")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![2, 5]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![5])),
            Arc::new(Int64Array::from(vec![50])),
            Arc::new(Int64Array::from(vec![25])),
        ],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![2, 3]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(3, 1), (5, -1)]);
}

#[tokio::test]
async fn filtered_topn_wrappers_use_slate_backed_columnar_operator_incrementally() {
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
            Arc::new(Int64Array::from(vec![1, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-filtered-topn-wrappers").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let global_query = "SELECT auction, price \
        FROM (SELECT auction, price FROM bids ORDER BY price DESC LIMIT 3) t \
        WHERE price > 18";
    let partitioned_query = "SELECT auction, price \
        FROM (SELECT auction, price, \
            ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rn \
            FROM bids) ranked \
        WHERE rn <= 2 AND price > 18";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::from_sql(
                "mv_filtered_global_topn",
                global_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::from_sql(
                "mv_filtered_partitioned_topn",
                partitioned_query,
                Arc::clone(&output_schema),
            ),
        ],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    assert_eq!(
        runtime.materialized_views[1].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let global_handle = registry
        .get("mv_filtered_global_topn")
        .expect("global materialized view");
    let partitioned_handle = registry
        .get("mv_filtered_partitioned_topn")
        .expect("partitioned materialized view");
    assert_eq!(
        id_count_rows(&global_handle.arrow_snapshot_for(1).expect("snapshot")),
        vec![(1, 20), (1, 30)]
    );
    assert_eq!(
        id_count_rows(
            &materialized_view_snapshot_for(
                partitioned_handle.as_ref(),
                Arc::clone(&output_schema),
                1,
            )
            .await
        ),
        vec![(1, 20), (1, 30)]
    );

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![25, 40])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let expected_after_insert = vec![(1, 25), (1, 30), (2, 40)];
    assert_eq!(
        id_count_rows(&global_handle.arrow_snapshot_for(2).expect("snapshot")),
        expected_after_insert
    );
    assert_eq!(
        id_count_rows(
            &materialized_view_snapshot_for(
                partitioned_handle.as_ref(),
                Arc::clone(&output_schema),
                2,
            )
            .await
        ),
        expected_after_insert
    );
    let expected_insert_delta = vec![(1, 20, -1), (1, 25, 1), (2, 40, 1)];
    assert_eq!(
        weighted_id_count_rows(&global_handle.arrow_delta_for(2).expect("delta")),
        expected_insert_delta
    );
    assert_eq!(
        weighted_id_count_rows(&partitioned_handle.arrow_delta_for(2).expect("delta")),
        expected_insert_delta
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::from_sql(
                "mv_filtered_global_topn",
                global_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::from_sql(
                "mv_filtered_partitioned_topn",
                partitioned_query,
                Arc::clone(&output_schema),
            ),
        ],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    assert_eq!(
        recovered.materialized_views[1].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_global = recovery_registry
        .get("mv_filtered_global_topn")
        .expect("recovered global materialized view");
    let recovered_partitioned = recovery_registry
        .get("mv_filtered_partitioned_topn")
        .expect("recovered partitioned materialized view");
    assert_eq!(
        id_count_rows(&recovered_global.arrow_snapshot_for(3).expect("snapshot")),
        expected_after_insert
    );
    assert_eq!(
        id_count_rows(
            &materialized_view_snapshot_for(
                recovered_partitioned.as_ref(),
                Arc::clone(&output_schema),
                3,
            )
            .await
        ),
        expected_after_insert
    );
    assert!(
        recovered_global
            .arrow_delta_for(3)
            .expect("delta")
            .iter()
            .all(|batch| batch.num_rows() == 0)
    );
    assert!(
        recovered_partitioned
            .arrow_delta_for(3)
            .expect("delta")
            .iter()
            .all(|batch| batch.num_rows() == 0)
    );

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![25])),
        ],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let expected_after_retract = vec![(1, 20), (1, 30), (2, 40)];
    assert_eq!(
        id_count_rows(&recovered_global.arrow_snapshot_for(4).expect("snapshot")),
        expected_after_retract
    );
    assert_eq!(
        id_count_rows(
            &materialized_view_snapshot_for(
                recovered_partitioned.as_ref(),
                Arc::clone(&output_schema),
                4,
            )
            .await
        ),
        expected_after_retract
    );
    let expected_retract_delta = vec![(1, 20, 1), (1, 25, -1)];
    assert_eq!(
        weighted_id_count_rows(&recovered_global.arrow_delta_for(4).expect("delta")),
        expected_retract_delta
    );
    assert_eq!(
        weighted_id_count_rows(&recovered_partitioned.arrow_delta_for(4).expect("delta")),
        expected_retract_delta
    );
}

#[tokio::test]
async fn ordered_topn_wrappers_use_slate_backed_columnar_operator_semantics() {
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
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![10, 30, 20, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-ordered-topn-wrappers").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let global_query = "SELECT auction, price \
        FROM (SELECT auction, price FROM bids ORDER BY price DESC LIMIT 3) t \
        ORDER BY auction";
    let row_number_query = "SELECT auction, price \
        FROM (SELECT auction, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS rn FROM bids) ranked \
        WHERE rn <= 3 \
        ORDER BY auction";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::from_sql(
                "mv_ordered_global_topn",
                global_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::from_sql(
                "mv_ordered_row_number_topn",
                row_number_query,
                Arc::clone(&output_schema),
            ),
        ],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    assert_eq!(
        runtime.materialized_views[1].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let global_handle = registry
        .get("mv_ordered_global_topn")
        .expect("global materialized view");
    let row_number_handle = registry
        .get("mv_ordered_row_number_topn")
        .expect("row-number materialized view");
    let expected_initial = vec![(1, 10), (2, 30), (3, 20)];
    assert_eq!(
        id_count_rows(&global_handle.arrow_snapshot_for(1).expect("snapshot")),
        expected_initial
    );
    assert_eq!(
        id_count_rows(&row_number_handle.arrow_snapshot_for(1).expect("snapshot")),
        expected_initial
    );

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4, 5])),
            Arc::new(Int64Array::from(vec![40, 25])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let expected_after_insert = vec![(2, 30), (4, 40), (5, 25)];
    assert_eq!(
        id_count_rows(&global_handle.arrow_snapshot_for(2).expect("snapshot")),
        expected_after_insert
    );
    assert_eq!(
        id_count_rows(&row_number_handle.arrow_snapshot_for(2).expect("snapshot")),
        expected_after_insert
    );
    let expected_insert_delta = vec![(1, 10, -1), (3, 20, -1), (4, 40, 1), (5, 25, 1)];
    assert_eq!(
        weighted_id_count_rows(&global_handle.arrow_delta_for(2).expect("delta")),
        expected_insert_delta
    );
    assert_eq!(
        weighted_id_count_rows(&row_number_handle.arrow_delta_for(2).expect("delta")),
        expected_insert_delta
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::from_sql(
                "mv_ordered_global_topn",
                global_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::from_sql(
                "mv_ordered_row_number_topn",
                row_number_query,
                Arc::clone(&output_schema),
            ),
        ],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    assert_eq!(
        recovered.materialized_views[1].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_global = recovery_registry
        .get("mv_ordered_global_topn")
        .expect("recovered global materialized view");
    let recovered_row_number = recovery_registry
        .get("mv_ordered_row_number_topn")
        .expect("recovered row-number materialized view");
    assert_eq!(
        id_count_rows(&recovered_global.arrow_snapshot_for(3).expect("snapshot")),
        expected_after_insert
    );
    assert_eq!(
        id_count_rows(
            &recovered_row_number
                .arrow_snapshot_for(3)
                .expect("snapshot")
        ),
        expected_after_insert
    );
    assert!(
        recovered_global
            .arrow_delta_for(3)
            .expect("delta")
            .iter()
            .all(|batch| batch.num_rows() == 0)
    );
    assert!(
        recovered_row_number
            .arrow_delta_for(3)
            .expect("delta")
            .iter()
            .all(|batch| batch.num_rows() == 0)
    );
}

#[tokio::test]
async fn global_row_number_topn_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            Arc::new(Int64Array::from(vec![10, 20, 15, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-global-row-number-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let query = "SELECT auction, bidder, price \
        FROM (SELECT auction, bidder, price, \
            ROW_NUMBER() OVER (ORDER BY price DESC) AS rank_number \
            FROM bids) ranked \
        WHERE rank_number <= 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_global_ranked_bids",
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
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_global_ranked_bids")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(bid_topn_rows(&snapshot), vec![(1, 20, 20), (2, 30, 15)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4, 5])),
            Arc::new(Int64Array::from(vec![50, 60])),
            Arc::new(Int64Array::from(vec![25, 7])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(bid_topn_rows(&snapshot), vec![(1, 20, 20), (4, 50, 25)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_bid_topn_rows(&delta),
        vec![(2, 30, 15, -1), (4, 50, 25, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_global_ranked_bids",
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
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_global_ranked_bids")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        bid_topn_rows(&recovered_snapshot),
        vec![(1, 20, 20), (4, 50, 25)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(Int64Array::from(vec![50])),
            Arc::new(Int64Array::from(vec![25])),
        ],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(bid_topn_rows(&snapshot), vec![(1, 20, 20), (2, 30, 15)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_bid_topn_rows(&delta),
        vec![(2, 30, 15, 1), (4, 50, 25, -1)]
    );
}

#[tokio::test]
async fn row_number_predicate_variants_use_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 15, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table =
        build_operator_state_table("vectorized-columnar-row-number-predicate-variants").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let reversed_query = "SELECT auction, bidder, price \
        FROM (SELECT auction, bidder, price, \
            ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number \
            FROM bids) ranked \
        WHERE 2 >= rank_number";
    let equality_query = "SELECT auction, bidder, price \
        FROM (SELECT auction, bidder, price, \
            ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number \
            FROM bids) ranked \
        WHERE rank_number = 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![
            VectorizedMaterializedViewPlan::from_sql(
                "mv_reversed_ranked_bids",
                reversed_query,
                Arc::clone(&output_schema),
            ),
            VectorizedMaterializedViewPlan::from_sql(
                "mv_second_ranked_bids",
                equality_query,
                Arc::clone(&output_schema),
            ),
        ],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert!(
        runtime
            .materialized_views
            .iter()
            .all(|mv| mv.operator.mode() == MaterializedViewExecutionMode::ColumnarTopN)
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let reversed = registry
        .get("mv_reversed_ranked_bids")
        .expect("reversed materialized view");
    let equality = registry
        .get("mv_second_ranked_bids")
        .expect("equality materialized view");
    let reversed_snapshot =
        materialized_view_snapshot_for(reversed.as_ref(), Arc::clone(&output_schema), 1).await;
    let equality_snapshot =
        materialized_view_snapshot_for(equality.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(
        bid_topn_rows(&reversed_snapshot),
        vec![(1, 20, 20), (1, 30, 30), (2, 40, 15), (2, 50, 5)]
    );
    assert_eq!(
        bid_topn_rows(&equality_snapshot),
        vec![(1, 20, 20), (2, 50, 5)]
    );

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![99])),
            Arc::new(Int64Array::from(vec![25])),
        ],
    )
    .expect("source insert batch");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let expected_snapshot = vec![(1, 30, 30), (1, 99, 25), (2, 40, 15), (2, 50, 5)];
    let reversed_snapshot =
        materialized_view_snapshot_for(reversed.as_ref(), Arc::clone(&output_schema), 2).await;
    let equality_snapshot =
        materialized_view_snapshot_for(equality.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(bid_topn_rows(&reversed_snapshot), expected_snapshot);
    assert_eq!(
        bid_topn_rows(&equality_snapshot),
        vec![(1, 99, 25), (2, 50, 5)]
    );
    let expected_delta = vec![(1, 20, 20, -1), (1, 99, 25, 1)];
    assert_eq!(
        weighted_bid_topn_rows(&reversed.arrow_delta_for(2).expect("reversed delta")),
        expected_delta
    );
    assert_eq!(
        weighted_bid_topn_rows(&equality.arrow_delta_for(2).expect("equality delta")),
        expected_delta
    );
}

#[tokio::test]
async fn global_topn_offset_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            Arc::new(Int64Array::from(vec![30, 25, 20, 15])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-global-topn-offset").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let query = "SELECT auction, bidder, price FROM bids ORDER BY price DESC LIMIT 2 OFFSET 1";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_offset_top_bids",
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
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_offset_top_bids")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(bid_topn_rows(&snapshot), vec![(2, 20, 25), (3, 30, 20)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![5, 6])),
            Arc::new(Int64Array::from(vec![50, 60])),
            Arc::new(Int64Array::from(vec![40, 18])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("bids", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(bid_topn_rows(&snapshot), vec![(1, 10, 30), (2, 20, 25)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_bid_topn_rows(&delta),
        vec![(1, 10, 30, 1), (3, 30, 20, -1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_offset_top_bids",
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
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_offset_top_bids")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        bid_topn_rows(&recovered_snapshot),
        vec![(1, 10, 30), (2, 20, 25)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![5])),
            Arc::new(Int64Array::from(vec![50])),
            Arc::new(Int64Array::from(vec![40])),
        ],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(bid_topn_rows(&snapshot), vec![(2, 20, 25), (3, 30, 20)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_bid_topn_rows(&delta),
        vec![(1, 10, 30, -1), (3, 30, 20, 1)]
    );
}

#[tokio::test]
async fn topn_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 5])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let query = "SELECT auction, bidder, price FROM (\
        SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number \
        FROM bids) ranked WHERE rank_number <= 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_bids",
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
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_top_bids").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(
        bid_topn_rows(&snapshot),
        vec![(1, 20, 20), (1, 30, 30), (2, 40, 5)]
    );

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 3])),
            Arc::new(Int64Array::from(vec![25, 50])),
            Arc::new(Int64Array::from(vec![25, 7])),
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
        bid_topn_rows(&snapshot),
        vec![(1, 25, 25), (1, 30, 30), (2, 40, 5), (3, 50, 7)]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_bid_topn_rows(&delta),
        vec![(1, 20, 20, -1), (1, 25, 25, 1), (3, 50, 7, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_bids",
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
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_top_bids")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        bid_topn_rows(&recovered_snapshot),
        vec![(1, 25, 25), (1, 30, 30), (2, 40, 5), (3, 50, 7)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![30])),
            Arc::new(Int64Array::from(vec![30])),
        ],
    )
    .expect("source retract rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract, &weighted_schema, &[-1])
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
        bid_topn_rows(&snapshot),
        vec![(1, 20, 20), (1, 25, 25), (2, 40, 5), (3, 50, 7)]
    );
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_bid_topn_rows(&delta),
        vec![(1, 20, 20, 1), (1, 30, 30, -1)]
    );
}

#[tokio::test]
async fn under_limit_topn_projection_uses_weighted_source_delta_semantics() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("date_time", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![101, 102, 103])),
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(TimestampMillisecondArray::from(vec![1000, 1100, 1200])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-under-limit-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let query = "SELECT auction, bidder, price, \"dateTime\" FROM (\
        SELECT auction, bidder, price, date_time AS \"dateTime\", \
            ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC, date_time ASC) \
                AS rank_number \
        FROM bids) ranked WHERE rank_number <= 10";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_under_limit_top_bids",
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
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_under_limit_top_bids")
        .expect("materialized view");
    assert_eq!(
        bid_topn_timestamp_rows(
            &materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await
        ),
        vec![(1, 10, 10, 1000), (1, 20, 20, 1100), (2, 30, 30, 1200)]
    );

    let changes = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![102, 104])),
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(Int64Array::from(vec![20, 40])),
            Arc::new(Int64Array::from(vec![20, 40])),
            Arc::new(TimestampMillisecondArray::from(vec![1100, 900])),
        ],
    )
    .expect("source changes");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&changes, &weighted_schema, &[-1, 1]).expect("weighted changes");
    runtime
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted source delta");
    runtime.run_tick(2).await.expect("weighted delta tick");

    assert_eq!(
        bid_topn_timestamp_rows(
            &materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await
        ),
        vec![(1, 10, 10, 1000), (1, 40, 40, 900), (2, 30, 30, 1200)]
    );
    assert_eq!(
        weighted_bid_topn_timestamp_rows(&handle.arrow_delta_for(2).expect("mv delta")),
        vec![(1, 20, 20, 1100, -1), (1, 40, 40, 900, 1)]
    );
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
        vec![VectorizedMaterializedViewPlan::from_sql(
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
async fn filter_project_requires_slate_backed_operator_state_table() {
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
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let result = VectorizedExecutionRuntime::new(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_orders",
            "SELECT id FROM orders WHERE amount > 10",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
    )
    .await;

    let err = match result {
        Ok(_) => panic!("filter/project MV should require SlateDB-backed operator state"),
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
    let table = build_operator_state_table("vectorized-source-query-default").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_orders",
            "SELECT id FROM orders",
            Arc::clone(&output_schema),
        )],
        registry,
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("runtime");

    assert!(runtime.table_providers().is_empty());
}

#[tokio::test]
async fn source_query_tables_can_be_limited_by_name() {
    let orders = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new("id", SourceDataType::Int64)],
    )
    .expect("orders source definition");
    let raw_events = SourceDefinition::new(
        "raw_events",
        vec![SourceColumn::new("id", SourceDataType::Int64)],
    )
    .expect("raw_events source definition");
    let nexmark_bid = SourceDefinition::new(
        "nexmark_bid",
        vec![SourceColumn::new("id", SourceDataType::Int64)],
    )
    .expect("nexmark_bid source definition");
    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(raw_events);
    sources.register(nexmark_bid);

    let runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        Vec::new(),
        Arc::new(MaterializedViewRegistry::new()),
        VectorizedExecutionRuntimeOptions::default()
            .with_source_query_tables_for(["orders", "nexmark_bid"]),
    )
    .await
    .expect("runtime");

    let mut names = runtime
        .table_providers()
        .into_iter()
        .map(|(name, _)| name)
        .collect::<Vec<_>>();
    names.sort();

    assert_eq!(names, vec!["nexmark_bid".to_string(), "orders".to_string()]);
}

#[tokio::test]
async fn source_query_tables_include_explicit_aliases_when_unrestricted() {
    let nexmark_bid = SourceDefinition::new(
        "nexmark_bid",
        vec![SourceColumn::new("id", SourceDataType::Int64)],
    )
    .expect("nexmark_bid source definition")
    .with_property(SOURCE_QUERY_ALIAS_PROPERTY, "bid");
    let mut sources = SourceRegistry::new();
    sources.register(nexmark_bid);

    let runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        Vec::new(),
        Arc::new(MaterializedViewRegistry::new()),
        VectorizedExecutionRuntimeOptions::default().with_source_query_tables(),
    )
    .await
    .expect("runtime");

    let mut names = runtime
        .table_providers()
        .into_iter()
        .map(|(name, _)| name)
        .collect::<Vec<_>>();
    names.sort();

    assert_eq!(names, vec!["bid".to_string(), "nexmark_bid".to_string()]);
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
async fn sum_group_by_requires_slate_backed_operator_state_table() {
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
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_totals",
            "SELECT id, SUM(amount) AS total FROM orders GROUP BY id",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
    )
    .await;

    let err = match result {
        Ok(_) => panic!("sum group-by MV should require SlateDB-backed operator state"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("requires SlateDB-backed operator state"),
        "{err:#}"
    );
}
