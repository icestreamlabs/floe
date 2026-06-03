use super::*;

#[tokio::test]
#[serial_test::serial]
async fn three_way_join_materializes_through_binary_composition() {
    let db = test_db("three-way-join").await;
    let view_name = "mv_three_way_join";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let person_schema = nexmark_person_schema();
        let auction_schema = nexmark_auction_schema();
        let bid_schema = nexmark_bid_schema();
        let auction = table_scan(Some("nexmark_auction"), &auction_schema, None)
            .expect("auction scan")
            .project(vec![col("id").alias("auction_id"), col("seller")])
            .expect("auction project")
            .build()
            .expect("auction plan");
        let bid = table_scan(Some("nexmark_bid"), &bid_schema, None)
            .expect("bid scan")
            .project(vec![col("auction").alias("bid_auction"), col("price")])
            .expect("bid project")
            .build()
            .expect("bid plan");
        let logical = table_scan(Some("nexmark_person"), &person_schema, None)
            .expect("person scan")
            .project(vec![col("id").alias("person_id")])
            .expect("person project")
            .join(
                auction,
                JoinType::Inner,
                (
                    vec![Column::from_name("person_id")],
                    vec![Column::from_name("seller")],
                ),
                None,
            )
            .expect("person auction join")
            .join(
                bid,
                JoinType::Inner,
                (
                    vec![Column::from_name("auction_id")],
                    vec![Column::from_name("bid_auction")],
                ),
                None,
            )
            .expect("auction bid join")
            .project(vec![col("person_id"), col("price")])
            .expect("project")
            .build()
            .expect("build logical");
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_person", "nexmark_auction", "nexmark_bid"]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
        .expect("validate plan")
        .required_sources;

    let mut registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await
            .expect("outer streams");

    registry
        .writer_mut("nexmark_person")
        .expect("person writer")
        .append_encoded(encoded_person_row(100, "alice"), 1)
        .expect("append person");
    registry
        .writer_mut("nexmark_person")
        .expect("person writer")
        .flush()
        .await
        .expect("flush person");

    registry
        .writer_mut("nexmark_auction")
        .expect("auction writer")
        .append_encoded(encoded_auction_row(10, 100), 1)
        .expect("append auction");
    registry
        .writer_mut("nexmark_auction")
        .expect("auction writer")
        .flush()
        .await
        .expect("flush auction");

    registry
        .writer_mut("nexmark_bid")
        .expect("bid writer")
        .append_encoded(encoded_bid_row_with_ts(10, 7, 99, 1_700_000_000_000), 1)
        .expect("append bid");
    registry
        .writer_mut("nexmark_bid")
        .expect("bid writer")
        .flush()
        .await
        .expect("flush bid");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("person_id", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = LegacyGraphHarness::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    let outputs = builder
        .build(LegacyGraphHarnessInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build three-way join graph");

    assert_eq!(outputs.required_sources, required_sources);
    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_row(&[100, 99])]);
}

#[tokio::test]
#[serial_test::serial]
async fn range_join_materializes_half_open_matches() {
    let db = test_db("range-join").await;
    let view_name = "mv_range_join";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let auction_schema = nexmark_auction_schema();
        let bid_schema = nexmark_bid_schema();
        let right = table_scan(Some("nexmark_bid"), &bid_schema, None)
            .expect("bid scan")
            .build()
            .expect("bid plan");
        let filter = col("price")
            .gt_eq(col("initial_bid"))
            .and(col("price").lt(col("reserve")));
        let logical = table_scan(Some("nexmark_auction"), &auction_schema, None)
            .expect("auction scan")
            .join(
                right,
                JoinType::Inner,
                (Vec::<Column>::new(), Vec::<Column>::new()),
                Some(filter),
            )
            .expect("range join")
            .project(vec![col("id"), col("price")])
            .expect("project")
            .build()
            .expect("build logical");
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_bid", "nexmark_auction"]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
        .expect("validate plan")
        .required_sources;

    let mut registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await
            .expect("outer streams");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(encoded_auction_row(10, 100), 1)
        .expect("append auction");
    auction_writer.flush().await.expect("flush auction");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 7, 15, 1_700_000_000_000), 1)
        .expect("append in-range bid");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 7, 20, 1_700_000_000_000), 1)
        .expect("append upper-bound bid");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 7, 9, 1_700_000_000_000), 1)
        .expect("append lower-bound miss bid");
    bid_writer.flush().await.expect("flush bid");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = LegacyGraphHarness::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    let outputs = builder
        .build(LegacyGraphHarnessInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build range join graph");

    assert_eq!(outputs.required_sources, required_sources);
    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_row(&[10, 15])]);
}

#[tokio::test]
#[serial_test::serial]
async fn asof_join_materializes_latest_prior_match() {
    let db = test_db("asof-join").await;
    let view_name = "mv_asof_join";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let auction_schema = nexmark_auction_schema();
        let bid_schema = nexmark_bid_schema();
        let right = table_scan(Some("nexmark_bid"), &bid_schema, None)
            .expect("bid scan")
            .build()
            .expect("bid plan");
        let logical = table_scan(Some("nexmark_auction"), &auction_schema, None)
            .expect("auction scan")
            .join(
                right,
                JoinType::Inner,
                (Vec::<Column>::new(), Vec::<Column>::new()),
                Some(col("price").lt_eq(col("reserve"))),
            )
            .expect("asof join")
            .project(vec![col("id"), col("price")])
            .expect("project")
            .build()
            .expect("build logical");
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_bid", "nexmark_auction"]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
        .expect("validate plan")
        .required_sources;

    let mut registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await
            .expect("outer streams");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(encoded_auction_row(10, 100), 1)
        .expect("append auction");
    auction_writer.flush().await.expect("flush auction");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 7, 15, 1_700_000_000_000), 1)
        .expect("append earlier bid");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 7, 18, 1_700_000_000_000), 1)
        .expect("append latest bid");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 7, 21, 1_700_000_000_000), 1)
        .expect("append future bid");
    bid_writer.flush().await.expect("flush bid");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = LegacyGraphHarness::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    let outputs = builder
        .build(LegacyGraphHarnessInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build ASOF join graph");

    assert_eq!(outputs.required_sources, required_sources);
    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_row(&[10, 18])]);
}

#[tokio::test]
#[serial_test::serial]
async fn sql_left_asof_join_null_extends_and_retracts_unmatched_rows() {
    let db = test_db("sql-left-asof-join").await;
    let view_name = "mv_sql_left_asof_join";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT a.id, b.price \
             FROM nexmark_auction a ASOF JOIN nexmark_bid b \
             MATCH_CONDITION (b.price <= a.reserve) \
             ON a.id = b.auction",
        )
        .await;
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_bid", "nexmark_auction"]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
        .expect("validate plan")
        .required_sources;

    let mut registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await
            .expect("outer streams");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("price", DataType::Int64, true),
        ]),
    );

    let (task_tx, mut task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = LegacyGraphHarness::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(LegacyGraphHarnessInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build SQL ASOF join graph");

    {
        let auction_writer = registry
            .writer_mut("nexmark_auction")
            .expect("auction writer");
        auction_writer
            .append_encoded(encoded_auction_row(10, 100), 1)
            .expect("append unmatched auction");
        auction_writer
            .append_encoded(encoded_auction_row(20, 200), 1)
            .expect("append matched auction");
    }
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick auctions");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 1, &mut task_rx).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;
    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![int_nullable_row(10, None), int_nullable_row(20, None)]
    );

    {
        let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        bid_writer
            .append_encoded(encoded_bid_row(20, 7, 15), 1)
            .expect("append earlier matching bid");
        bid_writer
            .append_encoded(encoded_bid_row(20, 8, 18), 1)
            .expect("append latest matching bid");
        bid_writer
            .append_encoded(encoded_bid_row(20, 9, 21), 1)
            .expect("append future nonmatching bid");
    }
    registry.tick_all_with_version(2).await.expect("tick bids");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 2, &mut task_rx).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;
    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![int_nullable_row(10, None), int_nullable_row(20, Some(18))]
    );

    {
        let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        bid_writer
            .append_encoded(encoded_bid_row(10, 10, 17), 1)
            .expect("append delayed match");
    }
    registry
        .tick_all_with_version(3)
        .await
        .expect("tick delayed match");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 3, &mut task_rx).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;
    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![
            int_nullable_row(10, Some(17)),
            int_nullable_row(20, Some(18))
        ]
    );
}

#[tokio::test]
#[serial_test::serial]
async fn sql_asof_join_applies_residual_with_precomputed_key_and_timestamp_expressions() {
    let db = test_db("sql-asof-residual-precompute").await;
    let view_name = "mv_sql_asof_residual_precompute";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT a.id, b.price \
             FROM nexmark_auction a ASOF JOIN nexmark_bid b \
             MATCH_CONDITION ((b.price + 1) <= (a.reserve + 1)) \
             ON (a.id + 0) = (b.auction + 0) AND b.bidder > a.seller",
        )
        .await;
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_bid", "nexmark_auction"]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
        .expect("validate plan")
        .required_sources;

    let mut registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await
            .expect("outer streams");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(encoded_auction_row(20, 7), 1)
        .expect("append auction");
    auction_writer.flush().await.expect("flush auction");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(20, 6, 18), 1)
        .expect("append residual-filtered bid");
    bid_writer
        .append_encoded(encoded_bid_row(20, 8, 15), 1)
        .expect("append matching bid");
    bid_writer
        .append_encoded(encoded_bid_row(20, 9, 21), 1)
        .expect("append future bid");
    bid_writer.flush().await.expect("flush bid");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("price", DataType::Int64, true),
        ]),
    );

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = LegacyGraphHarness::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(LegacyGraphHarnessInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build SQL ASOF residual graph");

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_nullable_row(20, Some(15))]);
}

#[tokio::test]
#[serial_test::serial]
async fn left_semi_join_materializes_retained_left_rows() {
    let db = test_db("left-semi-join").await;
    let view_name = "mv_left_semi_join";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let person_schema = nexmark_person_schema();
        let auction_schema = nexmark_auction_schema();
        let right = table_scan(Some("nexmark_auction"), &auction_schema, None)
            .expect("auction scan")
            .build()
            .expect("auction plan");
        let logical = table_scan(Some("nexmark_person"), &person_schema, None)
            .expect("person scan")
            .join(
                right,
                JoinType::LeftSemi,
                (
                    vec![Column::from_name("id")],
                    vec![Column::from_name("seller")],
                ),
                None,
            )
            .expect("left semi join")
            .project(vec![col("id")])
            .expect("project")
            .build()
            .expect("build logical");
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_person", "nexmark_auction"]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
        .expect("validate plan")
        .required_sources;

    let mut registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await
            .expect("outer streams");

    let person_writer = registry
        .writer_mut("nexmark_person")
        .expect("person writer");
    person_writer
        .append_encoded(encoded_person_row(100, "alice"), 1)
        .expect("append alice");
    person_writer
        .append_encoded(encoded_person_row(200, "bob"), 1)
        .expect("append bob");
    person_writer.flush().await.expect("flush person");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(encoded_auction_row(10, 100), 1)
        .expect("append matched auction");
    auction_writer
        .append_encoded(encoded_auction_row(11, 999), 1)
        .expect("append unmatched auction");
    auction_writer.flush().await.expect("flush auctions");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![Field::new("id", DataType::Int64, true)]),
    );

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = LegacyGraphHarness::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(LegacyGraphHarnessInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build left semi join graph");

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_row(&[100])]);
}
