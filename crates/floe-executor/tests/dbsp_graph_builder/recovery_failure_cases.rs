use super::*;

#[tokio::test]
#[serial_test::serial]
async fn filtered_count_distinct_aggregate_materializes_mv() {
    let db = test_db("filtered-count-distinct-aggregate").await;
    let view_name = "mv_filtered_count_distinct_aggregate";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .aggregate(
                vec![col("bidder")],
                vec![
                    count(col("price")).alias("cnt"),
                    count(col("price"))
                        .filter(col("price").lt(lit(20i64)))
                        .build()
                        .expect("filtered count")
                        .alias("lt20_cnt"),
                    count_distinct(col("auction")).alias("distinct_auctions"),
                    count_distinct(col("auction"))
                        .filter(col("price").lt(lit(20i64)))
                        .build()
                        .expect("filtered distinct count")
                        .alias("lt20_distinct_auctions"),
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
        Field::new("lt20_cnt", DataType::Int64, true),
        Field::new("distinct_auctions", DataType::Int64, true),
        Field::new("lt20_distinct_auctions", DataType::Int64, true),
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
        .build(BuildInputs {
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
        .expect("build filtered count-distinct aggregate graph");

    let mut version_rx = view_handle.version_watch();
    version_rx.borrow_and_update();

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 30), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 42, 15), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(3, 7, 25), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("filtered count-distinct aggregate update timeout")
        .expect("filtered count-distinct aggregate update");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![int_row(&[7, 1, 0, 1, 0]), int_row(&[42, 3, 2, 2, 2])]
    );
}

#[tokio::test]
#[serial_test::serial]
async fn filtered_count_distinct_aggregate_materializes_with_parallel_ingest_view() {
    let db = test_db("filtered-count-distinct-parallel").await;
    let ingest_view_name = "mv_parallel_ingest_count";
    let result_view_name = "mv_parallel_filtered_count_distinct";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let ingest_plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .aggregate(
                Vec::<Expr>::new(),
                vec![count(col("price")).alias("row_count")],
            )
            .expect("aggregate")
            .build()
            .expect("build logical");
        let planner = DbspPlanBuilder::new(nexmark_config());
        planner.build(&logical).expect("circuit plan")
    };

    let result_plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .aggregate(
                vec![col("bidder")],
                vec![
                    count(col("price")).alias("cnt"),
                    count(col("price"))
                        .filter(col("price").lt(lit(20i64)))
                        .build()
                        .expect("filtered count")
                        .alias("lt20_cnt"),
                    count_distinct(col("auction")).alias("distinct_auctions"),
                    count_distinct(col("auction"))
                        .filter(col("price").lt(lit(20i64)))
                        .build()
                        .expect("filtered distinct count")
                        .alias("lt20_distinct_auctions"),
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
    let mut required_sources =
        validate_dbsp_plan(&ingest_plan, &available_sources, ingest_view_name)
            .expect("validate ingest plan")
            .required_sources;
    required_sources.extend(
        validate_dbsp_plan(&result_plan, &available_sources, result_view_name)
            .expect("validate result plan")
            .required_sources,
    );

    let mut registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await
            .expect("outer streams");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(ingest_view_name);
    mv_registry.set_schema(
        ingest_view_name,
        arrow_schema(vec![Field::new("row_count", DataType::Int64, true)]),
    );
    let result_view_handle = mv_registry.register(result_view_name);
    mv_registry.set_schema(
        result_view_name,
        arrow_schema(vec![
            Field::new("bidder", DataType::Int64, true),
            Field::new("cnt", DataType::Int64, true),
            Field::new("lt20_cnt", DataType::Int64, true),
            Field::new("distinct_auctions", DataType::Int64, true),
            Field::new("lt20_distinct_auctions", DataType::Int64, true),
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
            graph_id: ingest_view_name,
            view_name: ingest_view_name,
            plan: &ingest_plan,
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
        .expect("build ingest graph");

    builder
        .build(BuildInputs {
            graph_id: result_view_name,
            view_name: result_view_name,
            plan: &result_plan,
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
        .expect("build result graph");

    let mut version_rx = result_view_handle.version_watch();
    version_rx.borrow_and_update();

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 30), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 42, 15), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(3, 7, 25), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("parallel filtered count-distinct aggregate update timeout")
        .expect("parallel filtered count-distinct aggregate update");

    let mut rows = materialized_rows(&mv_registry, result_view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![int_row(&[7, 1, 0, 1, 0]), int_row(&[42, 3, 2, 2, 2])]
    );
}

#[tokio::test]
#[serial_test::serial]
async fn distinct_subquery_aggregate_counts_unique_rows() {
    let db = test_db("distinct-aggregate").await;
    let view_name = "mv_distinct_count";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let distinct = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .project(vec![col("auction"), col("bidder")])
            .expect("project")
            .distinct()
            .expect("distinct")
            .build()
            .expect("build distinct");
        let logical = datafusion::logical_expr::LogicalPlanBuilder::from(distinct)
            .aggregate(Vec::<Expr>::new(), vec![count(col("auction"))])
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
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![Field::new("count", DataType::Int64, true)]),
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
            enable_source_batch_journal: false,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build distinct aggregate graph");

    let mut version_rx = view_handle.version_watch();
    version_rx.borrow_and_update();

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    // Unique (auction, bidder) pairs: (1,42), (1,7), (2,7) => count 3.
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 10), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 42, 20), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(1, 7, 30), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row(2, 7, 40), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("distinct aggregate update timeout")
        .expect("distinct aggregate update");

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_row(&[3])]);
}

#[tokio::test]
#[serial_test::serial]
async fn rebuild_recovers_materialized_view_without_reingest() {
    let db = test_db("rebuild").await;
    let view_name = "mv_rebuild";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .filter(col("bidder").eq(lit(42i64)))
            .expect("filter")
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

    let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    writer
        .append_encoded(encoded_bid_row(1, 42, 80), 1)
        .expect("append row");
    writer.flush().await.expect("flush one");
    writer
        .append_encoded(encoded_bid_row(2, 42, 81), 1)
        .expect("append second");
    writer.flush().await.expect("flush two");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![Field::new("auction", DataType::Int64, true)]);
    mv_registry.set_schema(view_name, arrow_schema);

    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let cancel = CancellationToken::new();
    {
        let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
            .await
            .expect("builder");
        let outputs = builder
            .build(BuildInputs {
                graph_id: view_name,
                view_name,
                plan: &plan,
                cancel: cancel.clone(),
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
            .expect("initial build");
        assert_eq!(outputs.required_sources, required_sources.clone());
    }

    materialized_rows(&mv_registry, view_name).await;
    cancel.cancel();
    tokio::task::yield_now().await;

    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let outputs = builder
        .build(BuildInputs {
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
        .expect("rebuild");

    assert_eq!(outputs.required_sources, required_sources);

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
#[serial_test::serial]
async fn cancel_stops_materialized_view_updates() {
    let db = test_db("cancel-updates").await;
    let view_name = "mv_cancel_updates";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
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

    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let cancel = CancellationToken::new();
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
            cancel: cancel.clone(),
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

    let mut version_rx = view_handle.version_watch();
    {
        let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        writer
            .append_encoded(encoded_bid_row(1, 42, 99), 1)
            .expect("append first");
        writer.flush().await.expect("flush first");
    }
    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("expected version update")
        .expect("version watch update");
    let first_version = view_handle.latest_version().expect("latest version");

    cancel.cancel();
    tokio::time::sleep(Duration::from_millis(20)).await;

    {
        let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        writer
            .append_encoded(encoded_bid_row(2, 42, 100), 1)
            .expect("append second");
        writer.flush().await.expect("flush second");
    }

    let update = timeout(Duration::from_millis(100), version_rx.changed()).await;
    assert!(update.is_err(), "expected no update after cancel");
    assert_eq!(view_handle.latest_version(), Some(first_version));
}

#[tokio::test]
#[serial_test::serial]
async fn graph_task_error_is_reported() {
    let db = test_db("graph-task-error").await;
    let view_name = "mv_error";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
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

    let registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await
            .expect("outer streams");
    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let (task_tx, mut task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);
    let cancel = CancellationToken::new();

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
            cancel: cancel.clone(),
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

    tokio::task::yield_now().await;

    let mut stream = handle_streams
        .get("nexmark_bid")
        .expect("bid stream")
        .clone();
    stream
        .send(ZSetHandle {
            ns: "missing_namespace".to_string(),
            version: 99,
        })
        .await
        .expect("send invalid handle");
    stream.flush().await.expect("flush invalid handle");

    let event = timeout(Duration::from_millis(200), task_rx.recv())
        .await
        .expect("graph task error timeout")
        .expect("graph task error");
    assert_eq!(event.graph_id, view_name);
    assert!(
        event.task.contains("map")
            || event.task.contains("attach-view")
            || event.task.contains("materialize-view"),
        "unexpected task label: {}",
        event.task
    );
    let message = event.error.to_string();
    assert!(!message.is_empty(), "expected error message");
    drop(cancel);
}
