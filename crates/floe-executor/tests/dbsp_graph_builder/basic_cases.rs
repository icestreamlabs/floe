use super::*;

#[tokio::test]
#[serial_test::serial]
async fn filter_and_projection_materializes_mv() {
    let db = test_db("filter-projection").await;
    let view_name = "mv_price";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .filter(col("bidder").eq(lit(42i64)))
            .expect("filter")
            .project(vec![col("price")])
            .expect("project")
            .build()
            .expect("build logical");
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };
    assert_eq!(
        source_batch_journal_root_sources(&plan).expect("source journal root sources"),
        Some(BTreeSet::from(["nexmark_bid".to_string()])),
        "source-batch-journal replay test requires a source-journal-eligible root"
    );

    let available_sources = ["nexmark_bid"]
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

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 99), 2)
        .expect("append duplicate bidder 42");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 99), -1)
        .expect("append bidder 42 retraction");
    bid_writer.flush().await.expect("flush first step");
    bid_writer
        .append_encoded(encoded_bid_row(2, 7, 50), 1)
        .expect("append bidder 7");
    bid_writer.flush().await.expect("flush second step");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![Field::new("price", DataType::Int64, true)]);
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
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    assert_eq!(outputs.required_sources, required_sources);

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_row(&[99])]);
}

#[tokio::test]
#[serial_test::serial]
async fn planned_mv_records_delta_work_for_retractions_and_consolidation() {
    let db = test_db("planned-mv-logical-work-retractions").await;
    let view_name = "mv_price_work";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .filter(col("bidder").eq(lit(42i64)))
            .expect("filter")
            .project(vec![col("price")])
            .expect("project")
            .build()
            .expect("build logical");
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_bid"]
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
        arrow_schema(vec![Field::new("price", DataType::Int64, true)]),
    );

    let (task_tx, mut task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    let mut builder = LegacyGraphHarness::new(db).await.expect("builder");
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
            mv_retention: StreamRetention::KeepLast { keep_last: 4 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 99), 1)
        .expect("append matching bid");
    bid_writer
        .append_encoded(encoded_bid_row(2, 7, 50), 1)
        .expect("append nonmatching bid");
    bid_writer.flush().await.expect("flush initial bids");
    wait_for_logical_version_or_task_error(&mv_registry, view_name, 1, &mut task_rx).await;

    let handle = mv_registry.get(view_name).expect("view handle");
    let work = handle
        .logical_work_for(1)
        .expect("logical work for version 1");
    assert_eq!(work.input_delta_rows, 1);
    assert_eq!(work.output_delta_rows, 1);
    assert_eq!(work.state_full_scan_count, 0);
    assert_eq!(work.cache_rebuild_rows, 0);

    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 99), -1)
        .expect("retract old matching bid");
    bid_writer
        .append_encoded(encoded_bid_row(3, 42, 100), 1)
        .expect("insert replacement matching bid");
    bid_writer
        .append_encoded(encoded_bid_row(4, 7, 70), 1)
        .expect("append canceling nonmatch");
    bid_writer
        .append_encoded(encoded_bid_row(4, 7, 70), -1)
        .expect("retract canceling nonmatch");
    bid_writer.flush().await.expect("flush update bids");
    wait_for_logical_version_or_task_error(&mv_registry, view_name, 2, &mut task_rx).await;

    let rows = visible_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_row(&[100])]);

    let work = handle
        .logical_work_for(2)
        .expect("logical work for version 2");
    assert_eq!(work.input_delta_rows, 2);
    assert_eq!(work.output_delta_rows, 2);
    assert_eq!(work.state_full_scan_count, 0);
    assert_eq!(work.cache_rebuild_rows, 0);
}

#[tokio::test]
#[serial_test::serial]
async fn unrelated_source_delta_only_advances_affected_materialized_view() {
    let db = test_db("multi-mv-source-boundary").await;
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let bid_view = "mv_bid_prices";
    let auction_view = "mv_auction_sellers";
    let planner = DbspPlanBuilder::new(nexmark_config());
    let bid_plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("bid scan")
            .project(vec![col("price")])
            .expect("bid project")
            .build()
            .expect("build bid logical");
        planner.build(&logical).expect("bid plan")
    };
    let auction_plan = {
        let schema = nexmark_auction_schema();
        let logical = table_scan(Some("nexmark_auction"), &schema, None)
            .expect("auction scan")
            .project(vec![col("seller")])
            .expect("auction project")
            .build()
            .expect("build auction logical");
        planner.build(&logical).expect("auction plan")
    };

    let available_sources = ["nexmark_bid", "nexmark_auction"]
        .into_iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let bid_required = validate_dbsp_plan(&bid_plan, &available_sources, bid_view)
        .expect("validate bid plan")
        .required_sources;
    let auction_required = validate_dbsp_plan(&auction_plan, &available_sources, auction_view)
        .expect("validate auction plan")
        .required_sources;
    let all_required = bid_required
        .union(&auction_required)
        .cloned()
        .collect::<BTreeSet<_>>();

    let mut registry =
        OuterStreamRegistry::from_validated_sources(&all_required, &mut ingestion_bridge)
            .await
            .expect("outer streams");
    let source_refs: Vec<&str> = all_required.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(bid_view);
    mv_registry.register(auction_view);
    let (task_tx, mut task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = LegacyGraphHarness::new(db).await.expect("builder");
    builder
        .build(LegacyGraphHarnessInputs {
            graph_id: bid_view,
            view_name: bid_view,
            plan: &bid_plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 4 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build bid graph");
    builder
        .build(LegacyGraphHarnessInputs {
            graph_id: auction_view,
            view_name: auction_view,
            plan: &auction_plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: false,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 4 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build auction graph");

    registry
        .writer_mut("nexmark_bid")
        .expect("bid writer")
        .append_encoded(encoded_bid_row(1, 42, 99), 1)
        .expect("append bid");
    registry
        .writer_mut("nexmark_bid")
        .expect("bid writer")
        .flush()
        .await
        .expect("flush bid source");
    wait_for_logical_version_or_task_error(&mv_registry, bid_view, 1, &mut task_rx).await;

    let bid_handle = mv_registry.get(bid_view).expect("bid view");
    let auction_handle = mv_registry.get(auction_view).expect("auction view");
    assert_eq!(bid_handle.latest_version(), Some(1));
    assert!(auction_handle.latest_version().unwrap_or(0) <= 0);
    assert!(auction_handle.logical_work_for(1).is_none());

    registry
        .writer_mut("nexmark_auction")
        .expect("auction writer")
        .append_encoded(encoded_auction_row(1, 42), 1)
        .expect("append auction");
    registry
        .writer_mut("nexmark_auction")
        .expect("auction writer")
        .flush()
        .await
        .expect("flush auction source");
    wait_for_logical_version_or_task_error(&mv_registry, auction_view, 1, &mut task_rx).await;

    assert_eq!(bid_handle.latest_version(), Some(1));
    assert_eq!(auction_handle.latest_version(), Some(1));
    assert_eq!(
        bid_handle
            .logical_work_for(1)
            .expect("bid logical work")
            .input_delta_rows,
        1
    );
    assert_eq!(
        auction_handle
            .logical_work_for(1)
            .expect("auction logical work")
            .input_delta_rows,
        1
    );
}

#[tokio::test]
#[serial_test::serial]
async fn inner_join_materializes_mv() {
    let db = test_db("inner-join").await;
    let view_name = "mv_join";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let person_schema = nexmark_person_schema();
        let auction_schema = nexmark_auction_schema();
        let right = table_scan(Some("nexmark_person"), &person_schema, None)
            .expect("person scan")
            .project(vec![col("id").alias("person_id"), col("name")])
            .expect("person project")
            .build()
            .expect("person plan");
        let logical = table_scan(Some("nexmark_auction"), &auction_schema, None)
            .expect("auction scan")
            .join(
                right,
                JoinType::Inner,
                (
                    vec![Column::from_name("seller")],
                    vec![Column::from_name("person_id")],
                ),
                None,
            )
            .expect("join")
            .project(vec![col("id"), col("name")])
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
    person_writer.flush().await.expect("flush person");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(encoded_auction_row(10, 100), 1)
        .expect("append auction");
    auction_writer.flush().await.expect("flush auction");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
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
        .expect("build join graph");

    assert_eq!(outputs.required_sources, required_sources);

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_utf8_row(10, Some("alice"))]);
}
