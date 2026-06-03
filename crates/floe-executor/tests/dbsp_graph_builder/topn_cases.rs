use super::*;

#[tokio::test]
#[serial_test::serial]
async fn aggregate_materializes_mv() {
    let db = test_db("aggregate").await;
    let view_name = "mv_aggregate";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .aggregate(
                vec![col("bidder")],
                vec![
                    count(col("price")).alias("cnt"),
                    sum(col("price")).alias("total"),
                    min(col("price")).alias("min_price"),
                    max(col("price")).alias("max_price"),
                    avg(col("price")).alias("avg_price"),
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
    let view_handle = mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("bidder", DataType::Int64, true),
        Field::new("cnt", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
        Field::new("min_price", DataType::Int64, true),
        Field::new("max_price", DataType::Int64, true),
        Field::new("avg_price", DataType::Int64, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build_legacy_for_harness(BuildInputs {
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
        .expect("build aggregate graph");

    let mut version_rx = view_handle.version_watch();
    version_rx.borrow_and_update();

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), 1)
        .expect("append bidder 42");
    bid_writer
        .append_encoded(encoded_bid_row(2, 42, 30), 1)
        .expect("append bidder 42");
    bid_writer
        .append_encoded(encoded_bid_row(3, 7, 5), 1)
        .expect("append bidder 7");
    bid_writer.flush().await.expect("flush bids");

    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("aggregate update timeout")
        .expect("aggregate update");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    let mut expected = vec![
        int_row(&[7, 1, 5, 5, 5, 5]),
        int_row(&[42, 2, 40, 10, 30, 20]),
    ];
    sort_rows_by_first_column(&mut expected);
    assert_eq!(rows, expected);

    bid_writer
        .append_encoded(encoded_bid_row(2, 42, 30), -1)
        .expect("remove bidder 42");
    bid_writer.flush().await.expect("flush removal");

    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("aggregate update timeout")
        .expect("aggregate update");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    let mut expected = vec![
        int_row(&[7, 1, 5, 5, 5, 5]),
        int_row(&[42, 1, 10, 10, 10, 10]),
    ];
    sort_rows_by_first_column(&mut expected);
    assert_eq!(rows, expected);
}

#[tokio::test]
#[serial_test::serial]
async fn topn_materializes_mv() {
    let db = test_db("topn").await;
    let view_name = "mv_topn";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .project(vec![col("price")])
            .expect("project")
            .sort(vec![col("price").sort(false, true)])
            .expect("sort")
            .limit(0, Some(2))
            .expect("limit")
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

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 7, 10), 1)
        .expect("append 10");
    bid_writer
        .append_encoded(encoded_bid_row(2, 8, 30), 1)
        .expect("append 30");
    bid_writer
        .append_encoded(encoded_bid_row(3, 9, 20), 1)
        .expect("append 20");
    bid_writer
        .append_encoded(encoded_bid_row(4, 10, 30), 1)
        .expect("append 30 again");
    bid_writer.flush().await.expect("flush bids");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![Field::new("price", DataType::Int64, true)]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build_legacy_for_harness(BuildInputs {
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
        .expect("build topn graph");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(rows, vec![int_row(&[30]), int_row(&[30])]);
}

#[tokio::test]
#[serial_test::serial]
async fn topn_materializes_mv_from_transient_source_journal() {
    let db = test_db("topn_transient_source").await;
    let view_name = "mv_topn_transient_source";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .project(vec![col("price")])
            .expect("project")
            .sort(vec![col("price").sort(false, true)])
            .expect("sort")
            .limit(0, Some(2))
            .expect("limit")
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
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![Field::new("price", DataType::Int64, true)]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build_legacy_for_harness(BuildInputs {
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
        .expect("build transient topn graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 7, 10), 1)
        .expect("append 10");
    bid_writer
        .append_encoded(encoded_bid_row(2, 8, 30), 1)
        .expect("append 30");
    bid_writer
        .append_encoded(encoded_bid_row(3, 9, 20), 1)
        .expect("append 20");
    bid_writer
        .append_encoded(encoded_bid_row(4, 10, 30), 1)
        .expect("append 30 again");
    bid_writer.flush().await.expect("flush bids");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by_key(|row| {
        let first = match row.first() {
            Some(Some(
                EncodedRowScalar::Int64(value) | EncodedRowScalar::TimestampMillis(value),
            )) => *value,
            _ => 0,
        };
        let second = match row.get(1) {
            Some(Some(
                EncodedRowScalar::Int64(value) | EncodedRowScalar::TimestampMillis(value),
            )) => *value,
            _ => 0,
        };
        (first, second)
    });
    assert_eq!(rows, vec![int_row(&[30]), int_row(&[30])]);
}

#[tokio::test]
#[serial_test::serial]
async fn row_number_topn_with_post_projection_materializes_from_transient_source_journal() {
    let db = test_db("row-number-topn-transient-source").await;
    let view_name = "mv_row_number_topn_transient_source";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT auction, bidder, price, channel, url, \"dateTime\", extra \
             FROM (SELECT auction, bidder, price, channel, url, date_time AS \"dateTime\", extra, \
                   ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number \
                   FROM nexmark_bid) ranked \
             WHERE rank_number <= 2",
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
        .build_legacy_for_harness(BuildInputs {
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
        .expect("build transient row-number topn graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 10, 50), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 11, 20), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 12, 40), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 20, 5), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 21, 15), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 22, 10), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 4).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by(|left, right| {
        let left_key = (
            scalar_i64(left.first()),
            scalar_i64(left.get(2)),
            scalar_i64(left.get(1)),
        );
        let right_key = (
            scalar_i64(right.first()),
            scalar_i64(right.get(2)),
            scalar_i64(right.get(1)),
        );
        left_key.cmp(&right_key)
    });
    assert_eq!(
        rows,
        vec![
            bid_row(1, 12, 40),
            bid_row(1, 10, 50),
            bid_row(2, 22, 10),
            bid_row(2, 21, 15),
        ]
    );
}

#[tokio::test]
#[serial_test::serial]
async fn row_number_topn_append_only_source_journal_updates_boundary_across_ticks() {
    let db = test_db("row-number-topn-boundary-updates").await;
    let view_name = "mv_row_number_topn_boundary_updates";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT auction, bidder, price, channel, url, \"dateTime\", extra \
             FROM (SELECT auction, bidder, price, channel, url, date_time AS \"dateTime\", extra, \
                   ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number \
                   FROM nexmark_bid) ranked \
             WHERE rank_number <= 2",
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
        .build_legacy_for_harness(BuildInputs {
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
        .expect("build transient row-number topn graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 10, 50), 1)
        .expect("append 50");
    bid_writer
        .append_encoded(encoded_bid_row(1, 11, 20), 1)
        .expect("append 20");
    bid_writer
        .append_encoded(encoded_bid_row(1, 12, 40), 1)
        .expect("append 40");
    bid_writer.flush().await.expect("flush first batch");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by(|left, right| {
        let left_key = (scalar_i64(left.first()), scalar_i64(left.get(2)));
        let right_key = (scalar_i64(right.first()), scalar_i64(right.get(2)));
        left_key.cmp(&right_key)
    });
    assert_eq!(rows, vec![bid_row(1, 12, 40), bid_row(1, 10, 50)]);

    bid_writer
        .append_encoded(encoded_bid_row(1, 13, 60), 1)
        .expect("append 60");
    bid_writer
        .append_encoded(encoded_bid_row(1, 14, 45), 1)
        .expect("append 45");
    bid_writer.flush().await.expect("flush second batch");

    wait_for_logical_version(&mv_registry, view_name, 2).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by(|left, right| {
        let left_key = (scalar_i64(left.first()), scalar_i64(left.get(2)));
        let right_key = (scalar_i64(right.first()), scalar_i64(right.get(2)));
        left_key.cmp(&right_key)
    });
    assert_eq!(rows, vec![bid_row(1, 10, 50), bid_row(1, 13, 60)]);
}

#[tokio::test]
#[serial_test::serial]
async fn row_number_top1_with_post_projection_recomputes_from_transient_source_journal() {
    let db = test_db("row-number-top1-transient-source").await;
    let view_name = "mv_row_number_top1_transient_source";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT auction, bidder, price, channel, url, \"dateTime\", extra \
             FROM (SELECT auction, bidder, price, channel, url, date_time AS \"dateTime\", extra, \
                   ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number \
                   FROM nexmark_bid) ranked \
             WHERE rank_number <= 1",
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
        .build_legacy_for_harness(BuildInputs {
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
        .append_encoded(encoded_bid_row(1, 10, 50), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 11, 20), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 12, 40), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 20, 5), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 21, 15), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 22, 10), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by(|left, right| {
        let left_key = (scalar_i64(left.first()), scalar_i64(left.get(2)));
        let right_key = (scalar_i64(right.first()), scalar_i64(right.get(2)));
        left_key.cmp(&right_key)
    });
    assert_eq!(rows, vec![bid_row(1, 10, 50), bid_row(2, 21, 15)]);

    bid_writer
        .append_encoded(encoded_bid_row(1, 10, 50), -1)
        .expect("remove top row");
    bid_writer.flush().await.expect("flush removal");

    wait_for_logical_version(&mv_registry, view_name, 2).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by(|left, right| {
        let left_key = (scalar_i64(left.first()), scalar_i64(left.get(2)));
        let right_key = (scalar_i64(right.first()), scalar_i64(right.get(2)));
        left_key.cmp(&right_key)
    });
    assert_eq!(rows, vec![bid_row(1, 12, 40), bid_row(2, 21, 15)]);
}

#[tokio::test]
#[serial_test::serial]
async fn row_number_top1_with_two_order_keys_prefers_descending_primary_key() {
    let db = test_db("row-number-top1-two-order-keys").await;
    let view_name = "mv_row_number_top1_two_order_keys";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT auction, bidder, price, channel, url, \"dateTime\", extra \
             FROM (SELECT auction, bidder, price, channel, url, date_time AS \"dateTime\", extra, \
                   ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC, date_time ASC) AS rank_number \
                   FROM nexmark_bid) ranked \
             WHERE rank_number <= 1",
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
        .build_legacy_for_harness(BuildInputs {
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
        .append_encoded(encoded_bid_row_with_ts(1, 10, 100, 1_700_000_001_000), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 11, 200, 1_700_000_002_000), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 12, 150, 1_700_000_001_500), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(2, 20, 50, 1_700_000_001_000), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(2, 21, 20, 1_700_000_000_500), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(2, 22, 60, 1_700_000_002_000), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by(|left, right| {
        let left_key = (scalar_i64(left.first()), scalar_i64(left.get(2)));
        let right_key = (scalar_i64(right.first()), scalar_i64(right.get(2)));
        left_key.cmp(&right_key)
    });
    assert_eq!(
        rows,
        vec![
            bid_row_with_ts(1, 11, 200, 1_700_000_002_000),
            bid_row_with_ts(2, 22, 60, 1_700_000_002_000),
        ]
    );
}
