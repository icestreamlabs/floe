use super::*;

#[tokio::test]
#[serial_test::serial]
async fn row_number_top1_join_q9_shape_preserves_order_and_bid_alias_projection() {
    let db = test_db("row-number-top1-join-q9-shape").await;
    let view_name = "mv_row_number_top1_join_q9_shape";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT id, bidder, price, \"bidExtra\" \
             FROM ( \
               SELECT a.id, b.bidder, b.price, b.extra AS \"bidExtra\", \
                 ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b.date_time ASC) AS rownum \
               FROM nexmark_auction a \
               JOIN nexmark_bid b ON a.id = b.auction \
               WHERE b.date_time BETWEEN a.date_time AND a.expires \
             ) ranked \
             WHERE rownum <= 1",
        )
        .await;
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_auction", "nexmark_bid"]
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
    registry.set_durable_enabled("nexmark_auction", false);
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("price", DataType::Int64, true),
            Field::new("bidExtra", DataType::Utf8, true),
        ]),
    );

    let (task_tx, mut task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build transient row-number top1 join graph");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(
            encoded_auction_row_with_ts_and_extra(
                1,
                500,
                9,
                1_700_000_100_000,
                1_700_000_000_000,
                "auction_extra",
            ),
            1,
        )
        .expect("append auction");
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick auctions");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(
            encoded_bid_row_with_ts_and_extra(1, 11, 10, 1_700_000_001_000, "bid_low"),
            1,
        )
        .expect("append low bid");
    bid_writer
        .append_encoded(
            encoded_bid_row_with_ts_and_extra(1, 22, 40, 1_700_000_002_000, "bid_high_late"),
            1,
        )
        .expect("append high late bid");
    bid_writer
        .append_encoded(
            encoded_bid_row_with_ts_and_extra(1, 33, 40, 1_700_000_001_500, "bid_high_early"),
            1,
        )
        .expect("append high early bid");
    registry.tick_all_with_version(2).await.expect("tick bids");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 2, &mut task_rx).await;
    wait_for_visible_row_count(&mv_registry, view_name, 1).await;

    let rows = visible_rows(&mv_registry, view_name).await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(scalar_i64(row.first()), 1);
    assert_eq!(scalar_i64(row.get(1)), 33);
    assert_eq!(scalar_i64(row.get(2)), 40);
    assert!(
        matches!(
            row.get(3),
            Some(Some(EncodedRowScalar::Utf8(value))) if value == "bid_high_early"
        ),
        "expected bidExtra=bid_high_early, got {:?}",
        row.get(3)
    );
}

#[tokio::test]
#[serial_test::serial]
async fn join_top1_aggregate_q6_shape_materializes_from_transient_source_journal() {
    let db = test_db("join-top1-aggregate-q6-transient-source").await;
    let view_name = "mv_join_top1_aggregate_q6_transient_source";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT seller, CAST(AVG(price) AS BIGINT) AS moving_avg_price \
             FROM (SELECT a.seller, b.price, \
                          ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC) AS rownum \
                   FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction \
                   WHERE b.date_time BETWEEN a.date_time AND a.expires) ranked \
             WHERE rownum <= 1 \
             GROUP BY seller",
        )
        .await;
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_auction", "nexmark_bid"]
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
    registry.set_durable_enabled("nexmark_auction", false);
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("seller", DataType::Int64, true),
            Field::new("moving_avg_price", DataType::Int64, true),
        ]),
    );

    let (task_tx, mut task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build transient q6 graph");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(
            encoded_auction_row_with_ts_and_extra(
                1,
                7,
                1,
                1_700_000_100_000,
                1_700_000_000_000,
                "a1",
            ),
            1,
        )
        .expect("append auction 1");
    auction_writer
        .append_encoded(
            encoded_auction_row_with_ts_and_extra(
                2,
                7,
                1,
                1_700_000_100_000,
                1_700_000_000_000,
                "a2",
            ),
            1,
        )
        .expect("append auction 2");
    auction_writer
        .append_encoded(
            encoded_auction_row_with_ts_and_extra(
                3,
                9,
                1,
                1_700_000_100_000,
                1_700_000_000_000,
                "a3",
            ),
            1,
        )
        .expect("append auction 3");
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick auctions");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 11, 30, 1_700_000_001_000), 1)
        .expect("append bid 1");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 12, 50, 1_700_000_002_000), 1)
        .expect("append bid 2");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(2, 21, 70, 1_700_000_001_500), 1)
        .expect("append bid 3");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(2, 22, 10, 1_700_000_001_250), 1)
        .expect("append bid 4");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(3, 31, 90, 1_700_000_001_750), 1)
        .expect("append bid 5");
    registry.tick_all_with_version(2).await.expect("tick bids");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 2, &mut task_rx).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(rows, vec![int_row(&[7, 60]), int_row(&[9, 90])]);

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 13, 90, 1_700_000_003_000), 1)
        .expect("append later winning bid");
    registry
        .tick_all_with_version(3)
        .await
        .expect("tick second bid batch");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 3, &mut task_rx).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(rows, vec![int_row(&[7, 80]), int_row(&[9, 90])]);
}

#[tokio::test]
#[serial_test::serial]
async fn join_aggregate_pipeline_recomputes_from_transient_source_journal_retraction() {
    let db = test_db("join-aggregate-retraction-transient-source").await;
    let view_name = "mv_join_aggregate_retraction_transient_source";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT a.seller, SUM(b.price) AS total_price \
             FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction \
             GROUP BY a.seller",
        )
        .await;
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_auction", "nexmark_bid"]
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
    registry.set_durable_enabled("nexmark_auction", false);
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("seller", DataType::Int64, true),
            Field::new("total_price", DataType::Int64, true),
        ]),
    );

    let (task_tx, mut task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build transient join aggregate graph");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(encoded_auction_row(1, 7), 1)
        .expect("append auction");
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick auction");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 10, 10), 1)
        .expect("append first bid");
    bid_writer
        .append_encoded(encoded_bid_row(1, 11, 30), 1)
        .expect("append second bid");
    registry.tick_all_with_version(2).await.expect("tick bids");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 2, &mut task_rx).await;
    wait_for_visible_row_count(&mv_registry, view_name, 1).await;

    let rows = visible_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_row(&[7, 40])]);

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 11, 30), -1)
        .expect("retract second bid");
    registry
        .tick_all_with_version(3)
        .await
        .expect("tick bid retraction");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 3, &mut task_rx).await;

    let rows = visible_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_row(&[7, 10])]);
}

#[tokio::test]
#[serial_test::serial]
async fn join_with_proctime_q13_shape_materializes_from_transient_source_journal() {
    let db = test_db("join-proctime-q13-transient-source").await;
    let view_name = "mv_join_proctime_q13_transient_source";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT b.auction, b.bidder, b.price, b.date_time AS \"dateTime\", a.seller AS value \
             FROM (SELECT *, PROCTIME() AS p_time FROM nexmark_bid) b \
             JOIN nexmark_auction AS a ON b.auction = a.id \
             WHERE b.auction % 10000 = a.id % 10000",
        )
        .await;
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let available_sources = ["nexmark_auction", "nexmark_bid"]
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
    registry.set_durable_enabled("nexmark_auction", false);
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("price", DataType::Int64, true),
            Field::new(
                "dateTime",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("value", DataType::Int64, true),
        ]),
    );

    let (task_tx, mut task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build transient q13 graph");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(
            encoded_auction_row_with_ts_and_extra(
                1,
                7,
                1,
                1_700_000_100_000,
                1_700_000_000_000,
                "a1",
            ),
            1,
        )
        .expect("append auction");
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick auctions");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 42, 10, 1_700_000_001_000), 1)
        .expect("append first bid");
    registry.tick_all_with_version(2).await.expect("tick bids");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 84, 20, 1_700_000_002_000), 1)
        .expect("append second bid");
    registry.tick_all_with_version(3).await.expect("tick bids");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 2, &mut task_rx).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    rows.sort_by_key(|row| match row.get(1) {
        Some(Some(EncodedRowScalar::Int64(value))) => *value,
        _ => i64::MIN,
    });
    assert_eq!(
        rows,
        vec![
            vec![
                Some(EncodedRowScalar::Int64(1)),
                Some(EncodedRowScalar::Int64(42)),
                Some(EncodedRowScalar::Int64(10)),
                Some(EncodedRowScalar::TimestampMillis(1_700_000_001_000)),
                Some(EncodedRowScalar::Int64(7)),
            ],
            vec![
                Some(EncodedRowScalar::Int64(1)),
                Some(EncodedRowScalar::Int64(84)),
                Some(EncodedRowScalar::Int64(20)),
                Some(EncodedRowScalar::TimestampMillis(1_700_000_002_000)),
                Some(EncodedRowScalar::Int64(7)),
            ]
        ]
    );
}

#[tokio::test]
#[serial_test::serial]
async fn row_number_top1_with_two_int64_partition_keys_and_timestamp_order_recomputes_from_transient_source_journal()
 {
    let db = test_db("row-number-top1-two-int64-partitions-transient-source").await;
    let view_name = "mv_row_number_top1_two_int64_partitions_transient_source";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            r#"SELECT auction, bidder, price, channel, url, "dateTime", extra
             FROM (SELECT auction, bidder, price, channel, url, date_time AS "dateTime", extra,
                   ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY date_time DESC) AS rank_number
                   FROM nexmark_bid) ranked
             WHERE rank_number <= 1"#,
        )
        .await;
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
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("price", DataType::Int64, true),
            Field::new("channel", DataType::Utf8, true),
            Field::new("url", DataType::Utf8, true),
            Field::new(
                "dateTime",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("extra", DataType::Utf8, true),
        ]),
    );

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
            outer_transient_streams: &transient_streams,
            enable_source_batch_journal: true,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build transient row-number top1 graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 10, 50, 1_700_000_000_000), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 10, 60, 1_700_000_100_000), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 11, 20, 1_700_000_050_000), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(2, 20, 5, 1_700_000_010_000), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(2, 20, 15, 1_700_000_005_000), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 3).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by(|left, right| {
        let left_key = (
            scalar_i64(left.get(1)),
            scalar_i64(left.first()),
            scalar_timestamp_millis(left.get(5)),
        );
        let right_key = (
            scalar_i64(right.get(1)),
            scalar_i64(right.first()),
            scalar_timestamp_millis(right.get(5)),
        );
        left_key.cmp(&right_key)
    });
    assert_eq!(
        rows,
        vec![
            bid_row_with_ts(1, 10, 60, 1_700_000_100_000),
            bid_row_with_ts(1, 11, 20, 1_700_000_050_000),
            bid_row_with_ts(2, 20, 5, 1_700_000_010_000),
        ]
    );

    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 10, 60, 1_700_000_100_000), -1)
        .expect("remove top row");
    bid_writer.flush().await.expect("flush removal");

    wait_for_logical_version(&mv_registry, view_name, 2).await;
    wait_for_visible_row_count(&mv_registry, view_name, 3).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by(|left, right| {
        let left_key = (
            scalar_i64(left.get(1)),
            scalar_i64(left.first()),
            scalar_timestamp_millis(left.get(5)),
        );
        let right_key = (
            scalar_i64(right.get(1)),
            scalar_i64(right.first()),
            scalar_timestamp_millis(right.get(5)),
        );
        left_key.cmp(&right_key)
    });
    assert_eq!(
        rows,
        vec![
            bid_row_with_ts(1, 10, 50, 1_700_000_000_000),
            bid_row_with_ts(1, 11, 20, 1_700_000_050_000),
            bid_row_with_ts(2, 20, 5, 1_700_000_010_000),
        ]
    );
}
