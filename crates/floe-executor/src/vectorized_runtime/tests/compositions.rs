#[tokio::test]
async fn aggregate_topn_uses_slate_backed_columnar_topn_operator_semantics() {
    let bids = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("bids source definition");
    let bids_schema = bids.to_arrow_schema();
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 5, 20, 7])),
        ],
    )
    .expect("initial bids batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-aggregate-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("total", DataType::Int64, true),
    ]));
    let query = "SELECT auction, SUM(price) AS total \
        FROM bids \
        GROUP BY auction \
        ORDER BY total DESC \
        LIMIT 2";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_auction_totals",
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
        .append_source_batches_for_execution_and_query(
            "bids",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_top_auction_totals")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 15), (2, 20)]);

    let bid_insert = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![40])),
        ],
    )
    .expect("bid insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bids",
            vec![bid_insert.clone()],
            vec![bid_insert],
        )
        .await
        .expect("append bid insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(2, 20), (3, 47)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 15, -1), (3, 47, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_top_auction_totals",
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
        .get("mv_top_auction_totals")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(2, 20), (3, 47)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let bid_retract = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![20])),
        ],
    )
    .expect("bid retract batch");
    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&bids_schema)
        .expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&bid_retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply bid retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 15), (3, 47)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 15, 1), (2, 20, -1)]
    );
}

#[tokio::test]
async fn join_aggregate_uses_slate_backed_columnar_operator_semantics() {
    let auctions = SourceDefinition::new(
        "auctions",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
        ],
    )
    .expect("auctions source definition");
    let bids = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("bids source definition");
    let auctions_schema = auctions.to_arrow_schema();
    let bids_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 10])),
        ],
    )
    .expect("initial auctions batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![100, 110, 120])),
        ],
    )
    .expect("initial bids batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-join-aggregate").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Int64, false),
        Field::new("bid_count", DataType::Int64, false),
    ]));
    let query = "SELECT a.category, COUNT(*) AS bid_count \
        FROM auctions a JOIN bids b ON a.id = b.auction \
        GROUP BY a.category";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_category_bid_counts",
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
            "auctions",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime
        .append_source_batches_for_execution_and_query(
            "bids",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_category_bid_counts")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(10, 3)]);

    let bid_insert = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![130])),
        ],
    )
    .expect("bid insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bids",
            vec![bid_insert.clone()],
            vec![bid_insert],
        )
        .await
        .expect("append bid insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(10, 4)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(10, 3, -1), (10, 4, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_category_bid_counts",
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
        .get("mv_category_bid_counts")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(10, 4)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let bid_retract = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![100])),
        ],
    )
    .expect("bid retract batch");
    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&bids_schema)
        .expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&bid_retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(10, 3)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(10, 3, 1), (10, 4, -1)]
    );
}

#[tokio::test]
async fn q4_uses_incremental_grouped_stats_composition_semantics() {
    let auctions = SourceDefinition::new(
        "auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, true),
            SourceColumn::new_nullable("expires", SourceDataType::TimestampMillis, true),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
        ],
    )
    .expect("auction source definition");
    let bids = SourceDefinition::new(
        "bid",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, true),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let auction_batch = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(10),
                Some(10),
                Some(10),
                None,
            ])),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(100),
                Some(100),
                Some(100),
                Some(100),
            ])),
            Arc::new(Int64Array::from(vec![10, 10, 20, 10])),
        ],
    )
    .expect("auction batch");
    let bid_batch = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 2, 3, 3, 4])),
            Arc::new(Int64Array::from(vec![100, 200, 50, 500, 300, 400, 1000])),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(20),
                Some(15),
                Some(25),
                None,
                Some(30),
                Some(200),
                Some(40),
            ])),
        ],
    )
    .expect("bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-q4-generic-join-aggregate").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Int64, false),
        Field::new("avg_max", DataType::Float64, true),
    ]));
    let query = "SELECT category, AVG(max) AS avg_max \
        FROM (SELECT MAX(b.price) AS max, a.category \
        FROM auction a JOIN bid b ON a.id = b.auction \
        WHERE b.\"dateTime\" BETWEEN a.\"dateTime\" AND a.expires \
        GROUP BY a.id, a.category) per_auction GROUP BY category";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_q4_avg_price",
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
            "auction",
            vec![auction_batch.clone()],
            vec![auction_batch],
        )
        .await
        .expect("append auctions");
    runtime.run_tick(1).await.expect("auction-only tick");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![bid_batch.clone()],
            vec![bid_batch],
        )
        .await
        .expect("append bids");
    runtime.run_tick(2).await.expect("bid tick");

    let handle = registry.get("mv_q4_avg_price").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(category_avg_rows(&snapshot), vec![(10, 125.0), (20, 300.0)]);

    let better_bid = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![150])),
            Arc::new(TimestampMillisecondArray::from(vec![Some(40)])),
        ],
    )
    .expect("better bid batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![better_bid.clone()],
            vec![better_bid.clone()],
        )
        .await
        .expect("append better bid");
    runtime.run_tick(3).await.expect("better bid tick");

    let snapshot = handle.arrow_snapshot_for(3).expect("updated snapshot");
    assert_eq!(category_avg_rows(&snapshot), vec![(10, 175.0), (20, 300.0)]);
    let delta = handle.arrow_delta_for(3).expect("updated delta");
    assert_eq!(
        weighted_category_avg_rows(&delta),
        vec![(10, 125.0, -1), (10, 175.0, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_q4_avg_price",
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
    recovered.run_tick(4).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_q4_avg_price")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("recovered snapshot");
    assert_eq!(
        category_avg_rows(&recovered_snapshot),
        vec![(10, 175.0), (20, 300.0)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(4)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&bid_schema).expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&better_bid, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("bid", weighted)
        .await
        .expect("apply better bid retract");
    recovered.run_tick(5).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(5)
        .expect("post-retract snapshot");
    assert_eq!(category_avg_rows(&snapshot), vec![(10, 125.0), (20, 300.0)]);
    let delta = recovered_handle
        .arrow_delta_for(5)
        .expect("post-retract delta");
    assert_eq!(
        weighted_category_avg_rows(&delta),
        vec![(10, 175.0, -1), (10, 125.0, 1)]
    );
}

#[tokio::test]
async fn q4_nexmark_shape_uses_incremental_grouped_stats_composition_semantics() {
    let auctions = SourceDefinition::new(
        "nexmark_auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("item_name", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("description", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("initial_bid", SourceDataType::Int64, false),
            SourceColumn::new_nullable("reserve", SourceDataType::Int64, false),
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
            SourceColumn::new_nullable("expires", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("date_time", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("auction source definition");
    let bids = SourceDefinition::new(
        "nexmark_bid",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("channel", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("url", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("date_time", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let auction_batch = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["item_1", "item_2"])),
            Arc::new(StringArray::from(vec!["desc_1", "desc_2"])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(Int64Array::from(vec![1000, 1000])),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(TimestampMillisecondArray::from(vec![
                1_700_086_400_001_i64,
                1_700_086_400_002,
            ])),
            Arc::new(TimestampMillisecondArray::from(vec![
                1_700_000_000_001_i64,
                1_700_000_000_002,
            ])),
            Arc::new(StringArray::from(vec![
                "auction_extra_1",
                "auction_extra_2",
            ])),
        ],
    )
    .expect("auction batch");
    let bid_batch = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![11, 12, 13])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
            Arc::new(StringArray::from(vec!["web", "web", "web"])),
            Arc::new(StringArray::from(vec!["/a", "/b", "/c"])),
            Arc::new(TimestampMillisecondArray::from(vec![
                1_700_000_000_001_i64,
                1_700_000_000_002,
                1_700_000_000_003,
            ])),
            Arc::new(StringArray::from(vec![
                "bid_extra_1",
                "bid_extra_2",
                "bid_extra_3",
            ])),
        ],
    )
    .expect("bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-q4-nexmark-shape").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Int64, false),
        Field::new("avg_price", DataType::Int64, true),
    ]));
    let query = "SELECT category, CAST(AVG(max) AS BIGINT) AS avg_price \
        FROM (SELECT MAX(b.price) AS max, a.category \
        FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction \
        WHERE b.date_time BETWEEN a.date_time AND a.expires \
        GROUP BY a.id, a.category) per_auction GROUP BY category";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_q4_nexmark",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_bid",
            vec![bid_batch.clone()],
            vec![bid_batch],
        )
        .await
        .expect("append bids");
    runtime.run_tick(1).await.expect("bid-only q4 tick");
    let handle = registry.get("mv_q4_nexmark").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("bid-only snapshot");
    assert!(id_count_rows(&snapshot).is_empty());

    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_auction",
            vec![auction_batch.clone()],
            vec![auction_batch],
        )
        .await
        .expect("append auctions");
    runtime.run_tick(2).await.expect("auction q4 tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(10, 200), (20, 300)]);
}

#[tokio::test]
async fn q4_nexmark_generated_batches_use_incremental_grouped_stats_semantics() {
    const BASE_TS_MS: i64 = 1_700_000_000_000;

    fn nexmark_auction_definition() -> SourceDefinition {
        SourceDefinition::new(
            "nexmark_auction",
            vec![
                SourceColumn::new("id", SourceDataType::Int64),
                SourceColumn::new("item_name", SourceDataType::Utf8),
                SourceColumn::new("description", SourceDataType::Utf8),
                SourceColumn::new("initial_bid", SourceDataType::Int64),
                SourceColumn::new("reserve", SourceDataType::Int64),
                SourceColumn::new("seller", SourceDataType::Int64),
                SourceColumn::new("category", SourceDataType::Int64),
                SourceColumn::new("expires", SourceDataType::TimestampMillis),
                SourceColumn::new("date_time", SourceDataType::TimestampMillis),
                SourceColumn::new("extra", SourceDataType::Utf8),
            ],
        )
        .expect("auction source definition")
    }

    fn nexmark_bid_definition() -> SourceDefinition {
        SourceDefinition::new(
            "nexmark_bid",
            vec![
                SourceColumn::new("auction", SourceDataType::Int64),
                SourceColumn::new("bidder", SourceDataType::Int64),
                SourceColumn::new("price", SourceDataType::Int64),
                SourceColumn::new("channel", SourceDataType::Utf8),
                SourceColumn::new("url", SourceDataType::Utf8),
                SourceColumn::new("date_time", SourceDataType::TimestampMillis),
                SourceColumn::new("extra", SourceDataType::Utf8),
            ],
        )
        .expect("bid source definition")
    }

    fn generated_auction_batch(schema: SchemaRef, start: usize, rows: usize) -> RecordBatch {
        let mut ids = Vec::with_capacity(rows);
        let mut item_names = Vec::with_capacity(rows);
        let mut descriptions = Vec::with_capacity(rows);
        let mut initial_bids = Vec::with_capacity(rows);
        let mut reserves = Vec::with_capacity(rows);
        let mut sellers = Vec::with_capacity(rows);
        let mut categories = Vec::with_capacity(rows);
        let mut expires = Vec::with_capacity(rows);
        let mut date_times = Vec::with_capacity(rows);
        let mut extras = Vec::with_capacity(rows);
        for auction_idx in start..(start + rows) {
            let idx = i64::try_from(auction_idx).expect("auction idx");
            let initial_bid = 5_000 + (idx % 25_000);
            let date_time = BASE_TS_MS + idx;
            ids.push(idx);
            item_names.push(format!("item_{auction_idx}"));
            descriptions.push(format!("auction_description_{auction_idx}"));
            initial_bids.push(initial_bid);
            reserves.push(initial_bid + 500);
            sellers.push(50_000 + idx);
            categories.push(((idx - 1).rem_euclid(10)) + 1);
            expires.push(date_time + 86_400_000);
            date_times.push(date_time);
            extras.push(format!("auction_extra_{auction_idx}"));
        }
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(item_names)),
                Arc::new(StringArray::from(descriptions)),
                Arc::new(Int64Array::from(initial_bids)),
                Arc::new(Int64Array::from(reserves)),
                Arc::new(Int64Array::from(sellers)),
                Arc::new(Int64Array::from(categories)),
                Arc::new(TimestampMillisecondArray::from(expires)),
                Arc::new(TimestampMillisecondArray::from(date_times)),
                Arc::new(StringArray::from(extras)),
            ],
        )
        .expect("generated auction batch")
    }

    fn generated_bid_batch(schema: SchemaRef, start: usize, rows: usize) -> RecordBatch {
        let mut auctions = Vec::with_capacity(rows);
        let mut bidders = Vec::with_capacity(rows);
        let mut prices = Vec::with_capacity(rows);
        let mut channels = Vec::with_capacity(rows);
        let mut urls = Vec::with_capacity(rows);
        let mut date_times = Vec::with_capacity(rows);
        let mut extras = Vec::with_capacity(rows);
        for bid_idx in start..(start + rows) {
            let idx = i64::try_from(bid_idx).expect("bid idx");
            let auction = i64::try_from((bid_idx - 1) % 10_000 + 1).expect("auction id");
            let channel = match bid_idx % 5 {
                0 => "web",
                1 => "apple",
                2 => "google",
                3 => "facebook",
                _ => "baidu",
            };
            auctions.push(auction);
            bidders.push(10_000 + idx);
            prices.push(1_000 + (idx % 50_000));
            channels.push(channel.to_string());
            urls.push(format!("https://example.com/item/{bid_idx}"));
            date_times.push(BASE_TS_MS + idx);
            extras.push(format!("bid_extra_{bid_idx}"));
        }
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(auctions)),
                Arc::new(Int64Array::from(bidders)),
                Arc::new(Int64Array::from(prices)),
                Arc::new(StringArray::from(channels)),
                Arc::new(StringArray::from(urls)),
                Arc::new(TimestampMillisecondArray::from(date_times)),
                Arc::new(StringArray::from(extras)),
            ],
        )
        .expect("generated bid batch")
    }

    let auctions = nexmark_auction_definition();
    let bids = nexmark_bid_definition();
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-q4-generated-batches").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Int64, true),
        Field::new("avg_price", DataType::Int64, true),
    ]));
    let query = "SELECT category, CAST(AVG(max) AS BIGINT) AS avg_price \
        FROM (SELECT MAX(b.price) AS max, a.category \
        FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction \
        WHERE b.date_time BETWEEN a.date_time AND a.expires \
        GROUP BY a.id, a.category) per_auction GROUP BY category";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_q4_generated",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    for (version, (start, rows)) in [(1, 8192), (8193, 1808)].into_iter().enumerate() {
        let bid_batch = generated_bid_batch(Arc::clone(&bid_schema), start, rows);
        runtime
            .append_source_batches_for_execution_and_query(
                "nexmark_bid",
                vec![bid_batch.clone()],
                vec![bid_batch],
            )
            .await
            .expect("append generated bids");
        let auction_batch = generated_auction_batch(Arc::clone(&auction_schema), start, rows);
        runtime
            .append_source_batches_for_execution_and_query(
                "nexmark_auction",
                vec![auction_batch.clone()],
                vec![auction_batch],
            )
            .await
            .expect("append generated auctions");
        runtime
            .run_tick((version + 1) as i64)
            .await
            .expect("generated q4 tick");
    }

    let handle = registry.get("mv_q4_generated").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(
        id_count_rows(&snapshot),
        vec![
            (1, 5996),
            (2, 5997),
            (3, 5998),
            (4, 5999),
            (5, 6000),
            (6, 6001),
            (7, 6002),
            (8, 6003),
            (9, 6004),
            (10, 6005),
        ]
    );
}

#[tokio::test]
async fn union_aggregate_uses_slate_backed_columnar_operator_semantics() {
    let bids = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("bids source definition");
    let auctions = SourceDefinition::new(
        "auctions",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
        ],
    )
    .expect("auctions source definition");
    let bids_schema = bids.to_arrow_schema();
    let auctions_schema = auctions.to_arrow_schema();
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![100, 110, 120])),
        ],
    )
    .expect("initial bids batch");
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 3])),
            Arc::new(Int64Array::from(vec![10, 30])),
        ],
    )
    .expect("initial auctions batch");

    let mut sources = SourceRegistry::new();
    sources.register(bids);
    sources.register(auctions);
    let table = build_operator_state_table("vectorized-columnar-union-aggregate").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("row_count", DataType::Int64, false),
    ]));
    let query = "SELECT key, COUNT(*) AS row_count \
        FROM (SELECT auction AS key FROM bids UNION ALL SELECT id AS key FROM auctions) u \
        GROUP BY key";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_union_key_counts",
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
        MaterializedViewExecutionMode::ColumnarUnionGroupedCount
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "bids",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime
        .append_source_batches_for_execution_and_query(
            "auctions",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_union_key_counts")
        .expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(id_count_rows(&snapshot), vec![(1, 3), (2, 1), (3, 1)]);

    let auction_insert = RecordBatch::try_new(
        Arc::clone(&auctions_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![20])),
        ],
    )
    .expect("auction insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "auctions",
            vec![auction_insert.clone()],
            vec![auction_insert],
        )
        .await
        .expect("append auction insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(id_count_rows(&snapshot), vec![(1, 3), (2, 2), (3, 1)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(2, 1, -1), (2, 2, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_union_key_counts",
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
        MaterializedViewExecutionMode::ColumnarUnionGroupedCount
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_union_key_counts")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        id_count_rows(&recovered_snapshot),
        vec![(1, 3), (2, 2), (3, 1)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let bid_retract = RecordBatch::try_new(
        Arc::clone(&bids_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![100])),
        ],
    )
    .expect("bid retract batch");
    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&bids_schema)
        .expect("weighted schema");
    let weighted =
        weighted_batch_from_diffs(&bid_retract, &weighted_schema, &[-1]).expect("weighted retract");
    recovered
        .apply_weighted_source_delta("bids", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(id_count_rows(&snapshot), vec![(1, 2), (2, 2), (3, 1)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 2, 1), (1, 3, -1)]);
}

#[tokio::test]
async fn unsupported_incremental_plan_without_state_table_is_rejected() {
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
        Ok(_) => panic!("unsupported aggregate MV planned without operator state"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("requires SlateDB-backed operator state"),
        "{err:#}"
    );
}
