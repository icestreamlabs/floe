use super::*;

#[tokio::test]
#[serial_test::serial]
async fn aggregate_with_post_projection_materializes_from_transient_source_journal() {
    let db = test_db("aggregate-transient-source").await;
    let view_name = "mv_aggregate_transient_source";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .aggregate(
                vec![col("bidder")],
                vec![
                    count(lit(1i64)).alias("bid_count"),
                    sum(col("price")).alias("total_price"),
                ],
            )
            .expect("aggregate")
            .project(vec![col("bidder"), col("bid_count"), col("total_price")])
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
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("bidder", DataType::Int64, true),
            Field::new("bid_count", DataType::Int64, true),
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
        .expect("build transient aggregate graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 10, 50), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 10, 25), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(3, 11, 40), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 1, &mut task_rx).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(rows, vec![int_row(&[10, 2, 75]), int_row(&[11, 1, 40])]);

    bid_writer
        .append_encoded(encoded_bid_row(2, 10, 25), -1)
        .expect("retract aggregate input");
    bid_writer
        .flush()
        .await
        .expect("flush aggregate retraction");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 2, &mut task_rx).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(rows, vec![int_row(&[10, 1, 50]), int_row(&[11, 1, 40])]);
}

#[tokio::test]
#[serial_test::serial]
async fn source_projection_with_proctime_materializes_mv() {
    let db = test_db("source-projection-proctime").await;
    let view_name = "mv_source_projection_proctime";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan("SELECT bidder, PROCTIME() AS p_time FROM nexmark_bid").await;
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
            Field::new("bidder", DataType::Int64, true),
            Field::new(
                "p_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ]),
    );

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
    builder
        .build(BuildInputs {
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
        .expect("build graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), 1)
        .expect("append");
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick bid batch");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 1).await;

    let rows = visible_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_and_null_timestamp_row(42)]);
}

#[tokio::test]
#[serial_test::serial]
async fn source_filter_projection_with_count_char_materializes_from_transient_source_journal() {
    let db = test_db("source-filter-projection-count-char").await;
    let view_name = "mv_source_filter_projection_count_char";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT auction, bidder, price * 908 / 1000 AS price, \
             CASE \
               WHEN HOUR(date_time) >= 8 AND HOUR(date_time) <= 18 THEN 'dayTime' \
               WHEN HOUR(date_time) <= 6 OR HOUR(date_time) >= 20 THEN 'nightTime' \
               ELSE 'otherTime' \
             END AS bid_time_type, \
             date_time AS \"dateTime\", \
             extra, \
             COUNT_CHAR(extra, 'c') AS c_counts \
             FROM nexmark_bid \
             WHERE price * 908 / 1000 > 1000000 AND price * 908 / 1000 < 50000000",
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
            Field::new("bid_time_type", DataType::Utf8, true),
            Field::new(
                "dateTime",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("extra", DataType::Utf8, true),
            Field::new("c_counts", DataType::Int64, true),
        ]),
    );

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
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
        .expect("build graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 2_000_000), 1)
        .expect("append matching bid");
    bid_writer
        .append_encoded(encoded_bid_row(2, 7, 100), 1)
        .expect("append filtered bid");
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick bid batch");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 1).await;

    let rows = visible_rows(&mv_registry, view_name).await;
    assert_eq!(
        rows,
        vec![count_char_projection_row(
            1,
            42,
            1_816_000,
            "nightTime",
            1_700_000_000_000,
            "extra",
            0,
        )]
    );
}

#[tokio::test]
#[serial_test::serial]
async fn source_projection_with_regexp_extract_materializes_from_transient_source_journal() {
    let db = test_db("source-projection-regexp-extract").await;
    let view_name = "mv_source_projection_regexp_extract";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT auction, bidder, price, channel, \
             CASE \
               WHEN lower(channel) = 'apple' THEN '0' \
               WHEN lower(channel) = 'google' THEN '1' \
               WHEN lower(channel) = 'facebook' THEN '2' \
               WHEN lower(channel) = 'baidu' THEN '3' \
               ELSE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) \
             END AS channel_id \
             FROM nexmark_bid \
             WHERE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) IS NOT NULL \
                OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')",
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
            Field::new("channel_id", DataType::Utf8, true),
        ]),
    );

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
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
        .expect("build graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(
            encoded_bid_row_with_channel_url(1, 42, 10, "APPLE", "https://example.com/no-channel"),
            1,
        )
        .expect("append apple bid");
    bid_writer
        .append_encoded(
            encoded_bid_row_with_channel_url(
                2,
                7,
                20,
                "web",
                "https://example.com/x/item/1?q=1&channel_id=abc123&foo=1",
            ),
            1,
        )
        .expect("append regexp bid");
    bid_writer
        .append_encoded(
            encoded_bid_row_with_channel_url(3, 8, 30, "web", "https://example.com/no-match"),
            1,
        )
        .expect("append filtered bid");
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick bid batch");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![
            channel_id_projection_row(1, 42, 10, "APPLE", "0"),
            channel_id_projection_row(2, 7, 20, "web", "abc123"),
        ]
    );
}

#[tokio::test]
#[serial_test::serial]
async fn source_projection_with_split_index_materializes_from_transient_source_journal() {
    let db = test_db("source-projection-split-index").await;
    let view_name = "mv_source_projection_split_index";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let logical = sql_plan(
            "SELECT auction, bidder, price, channel, \
             SPLIT_INDEX(url, '/', 3) AS dir1, \
             SPLIT_INDEX(url, '/', 4) AS dir2, \
             SPLIT_INDEX(url, '/', 5) AS dir3 \
             FROM nexmark_bid",
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
            Field::new("dir1", DataType::Utf8, true),
            Field::new("dir2", DataType::Utf8, true),
            Field::new("dir3", DataType::Utf8, true),
        ]),
    );

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("builder");
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
        .expect("build graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(
            encoded_bid_row_with_channel_url(
                1,
                42,
                10,
                "web",
                "https://example.com/dirA/item/123?q=1",
            ),
            1,
        )
        .expect("append full split bid");
    bid_writer
        .append_encoded(
            encoded_bid_row_with_channel_url(2, 7, 20, "web", "https://example.com/only"),
            1,
        )
        .expect("append short split bid");
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick bid batch");

    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![
            split_index_projection_row(
                1,
                42,
                10,
                "web",
                Some("dirA"),
                Some("item"),
                Some("123?q=1"),
            ),
            split_index_projection_row(2, 7, 20, "web", Some("only"), None, None),
        ]
    );
}
