use super::*;

#[tokio::test]
#[serial_test::serial]
async fn distinct_materializes_unique_rows() {
    let db = test_db("distinct-single").await;
    let view_name = "mv_distinct_bidder";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .project(vec![col("bidder")])
            .expect("project")
            .distinct()
            .expect("distinct")
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
        arrow_schema(vec![Field::new("bidder", DataType::Int64, true)]),
    );

    let (task_tx, mut task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = LegacyGraphHarness::new(Arc::clone(&db))
        .await
        .expect("builder");
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
        .expect("build distinct graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), 1)
        .expect("append first bidder");
    bid_writer
        .append_encoded(encoded_bid_row(2, 42, 20), 1)
        .expect("append duplicate bidder");
    bid_writer
        .append_encoded(encoded_bid_row(3, 7, 30), 1)
        .expect("append second bidder");
    bid_writer.flush().await.expect("flush bids");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 1, &mut task_rx).await;

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    let expected = vec![int_row(&[7]), int_row(&[42])];
    assert_eq!(rows, expected);

    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), -1)
        .expect("retract duplicate bidder");
    bid_writer
        .flush()
        .await
        .expect("flush duplicate retraction");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 2, &mut task_rx).await;

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    let expected = vec![int_row(&[7]), int_row(&[42])];
    assert_eq!(
        rows, expected,
        "distinct output should keep bidder 42 while one duplicate remains"
    );

    bid_writer
        .append_encoded(encoded_bid_row(2, 42, 20), -1)
        .expect("retract final bidder duplicate");
    bid_writer.flush().await.expect("flush final retraction");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 3, &mut task_rx).await;

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![int_row(&[7])],
        "distinct output should retract bidder 42 after its final row is removed"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn count_distinct_aggregate_materializes_mv() {
    let db = test_db("count-distinct-aggregate").await;
    let view_name = "mv_count_distinct_aggregate";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .aggregate(
                vec![col("bidder")],
                vec![
                    count(col("price")).alias("cnt"),
                    count_distinct(col("auction")).alias("distinct_auctions"),
                ],
            )
            .expect("aggregate")
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
    let arrow_schema = arrow_schema(vec![
        Field::new("bidder", DataType::Int64, true),
        Field::new("cnt", DataType::Int64, true),
        Field::new("distinct_auctions", DataType::Int64, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, mut task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = LegacyGraphHarness::new(Arc::clone(&db))
        .await
        .expect("builder");
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
        .expect("build count-distinct aggregate graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 20), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 42, 30), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(3, 7, 5), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 1, &mut task_rx).await;

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(rows, vec![int_row(&[7, 1, 1]), int_row(&[42, 3, 2])]);

    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), -1)
        .expect("retract one duplicate distinct value");
    bid_writer
        .flush()
        .await
        .expect("flush duplicate distinct retraction");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 2, &mut task_rx).await;

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(rows, vec![int_row(&[7, 1, 1]), int_row(&[42, 2, 2])]);

    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 20), -1)
        .expect("retract final duplicate distinct value");
    bid_writer
        .flush()
        .await
        .expect("flush final distinct retraction");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 3, &mut task_rx).await;

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(rows, vec![int_row(&[7, 1, 1]), int_row(&[42, 1, 1])]);
}

#[tokio::test]
#[serial_test::serial]
async fn count_distinct_aggregate_materializes_from_transient_source_journal() {
    let db = test_db("count-distinct-aggregate-transient").await;
    let view_name = "mv_count_distinct_aggregate_transient";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .aggregate(
                vec![col("bidder")],
                vec![
                    count(col("price")).alias("cnt"),
                    count_distinct(col("auction")).alias("distinct_auctions"),
                ],
            )
            .expect("aggregate")
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
    let _view_handle = mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("bidder", DataType::Int64, true),
        Field::new("cnt", DataType::Int64, true),
        Field::new("distinct_auctions", DataType::Int64, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = LegacyGraphHarness::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(LegacyGraphHarnessInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build transient count-distinct aggregate graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 20), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 42, 30), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(3, 7, 5), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(rows, vec![int_row(&[7, 1, 1]), int_row(&[42, 3, 2])]);
}

#[tokio::test]
#[serial_test::serial]
async fn q16_style_aggregate_keeps_single_group_across_transient_ticks() {
    let db = test_db("q16-transient-aggregate-date-format").await;
    let view_name = "mv_q16_transient";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let logical = sql_plan(
        "SELECT channel, DATE_FORMAT(date_time, 'yyyy-MM-dd') AS day, \
                MAX(DATE_FORMAT(date_time, 'HH:mm')) AS minute, \
                COUNT(*) AS total_bids, \
                COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, \
                COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, \
                COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, \
                COUNT(DISTINCT bidder) AS total_bidders, \
                COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, \
                COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, \
                COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, \
                COUNT(DISTINCT auction) AS total_auctions, \
                COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, \
                COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, \
                COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions \
         FROM nexmark_bid \
         GROUP BY channel, DATE_FORMAT(date_time, 'yyyy-MM-dd')",
    )
    .await;
    let plan = DbspPlanBuilder::new(nexmark_config())
        .build(&logical)
        .expect("circuit plan");

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
    let _view_handle = mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("channel", DataType::Utf8, true),
            Field::new("day", DataType::Utf8, true),
            Field::new("minute", DataType::Utf8, true),
            Field::new("total_bids", DataType::Int64, true),
            Field::new("rank1_bids", DataType::Int64, true),
            Field::new("rank2_bids", DataType::Int64, true),
            Field::new("rank3_bids", DataType::Int64, true),
            Field::new("total_bidders", DataType::Int64, true),
            Field::new("rank1_bidders", DataType::Int64, true),
            Field::new("rank2_bidders", DataType::Int64, true),
            Field::new("rank3_bidders", DataType::Int64, true),
            Field::new("total_auctions", DataType::Int64, true),
            Field::new("rank1_auctions", DataType::Int64, true),
            Field::new("rank2_auctions", DataType::Int64, true),
            Field::new("rank3_auctions", DataType::Int64, true),
        ]),
    );

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = LegacyGraphHarness::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(LegacyGraphHarnessInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx.clone(),
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build q16 transient aggregate graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 42, 9_999, 1_700_000_036_211), 1)
        .expect("append tick 1");
    bid_writer.flush().await.expect("flush tick 1");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    assert_eq!(
        visible_rows(&mv_registry, view_name).await,
        vec![vec![
            Some(EncodedRowScalar::Utf8("channel".to_string())),
            Some(EncodedRowScalar::Utf8("2023-11-14".to_string())),
            Some(EncodedRowScalar::Utf8("22:13".to_string())),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(0)),
            Some(EncodedRowScalar::Int64(0)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(0)),
            Some(EncodedRowScalar::Int64(0)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(0)),
            Some(EncodedRowScalar::Int64(0)),
        ]]
    );

    bid_writer
        .append_encoded(encoded_bid_row_with_ts(2, 99, 15_000, 1_700_000_096_211), 1)
        .expect("append tick 2");
    bid_writer.flush().await.expect("flush tick 2");

    wait_for_logical_version(&mv_registry, view_name, 2).await;
    assert_eq!(
        visible_rows(&mv_registry, view_name).await,
        vec![vec![
            Some(EncodedRowScalar::Utf8("channel".to_string())),
            Some(EncodedRowScalar::Utf8("2023-11-14".to_string())),
            Some(EncodedRowScalar::Utf8("22:14".to_string())),
            Some(EncodedRowScalar::Int64(2)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(0)),
            Some(EncodedRowScalar::Int64(2)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(0)),
            Some(EncodedRowScalar::Int64(2)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(0)),
        ]]
    );

    bid_writer
        .append_encoded(
            encoded_bid_row_with_ts(3, 7, 1_200_000, 1_700_000_156_211),
            1,
        )
        .expect("append tick 3");
    bid_writer.flush().await.expect("flush tick 3");

    wait_for_logical_version(&mv_registry, view_name, 3).await;
    assert_eq!(
        visible_rows(&mv_registry, view_name).await,
        vec![vec![
            Some(EncodedRowScalar::Utf8("channel".to_string())),
            Some(EncodedRowScalar::Utf8("2023-11-14".to_string())),
            Some(EncodedRowScalar::Utf8("22:15".to_string())),
            Some(EncodedRowScalar::Int64(3)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(3)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(3)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
            Some(EncodedRowScalar::Int64(1)),
        ]]
    );
}
