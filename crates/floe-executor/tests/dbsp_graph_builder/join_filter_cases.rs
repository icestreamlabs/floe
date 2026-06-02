use super::*;

#[tokio::test]
#[serial_test::serial]
async fn right_anti_join_materializes_retained_right_rows() {
    let db = test_db("right-anti-join").await;
    let view_name = "mv_right_anti_join";
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
                JoinType::RightAnti,
                (
                    vec![Column::from_name("id")],
                    vec![Column::from_name("seller")],
                ),
                None,
            )
            .expect("right anti join")
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
            enable_source_batch_journal: false,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build right anti join graph");

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_row(&[11])]);
}

#[tokio::test]
#[serial_test::serial]
async fn pushed_join_filter_keeps_advancing_with_static_build_side() {
    let db = test_db("join-filter-pushdown-static-build").await;
    let view_name = "mv_join_filter_pushdown";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let bid_schema = nexmark_bid_schema();
        let auction_schema = nexmark_auction_schema();
        let logical = table_scan(Some("nexmark_bid"), &bid_schema, None)
            .expect("bid scan")
            .join(
                table_scan(Some("nexmark_auction"), &auction_schema, None)
                    .expect("auction scan")
                    .build()
                    .expect("auction plan"),
                JoinType::Inner,
                (
                    vec![Column::from_name("auction")],
                    vec![Column::from_name("id")],
                ),
                None,
            )
            .expect("join")
            .filter(col("category").eq(lit(10i64)))
            .expect("filter")
            .project(vec![
                col("auction"),
                col("bidder"),
                col("price").alias("projected_price"),
                col("seller"),
            ])
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

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("projected_price", DataType::Int64, true),
            Field::new("seller", DataType::Int64, true),
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
            enable_source_batch_journal: false,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(encoded_auction_row_with_category(1, 100, 10), 1)
        .expect("append matching auction");
    auction_writer
        .append_encoded(encoded_auction_row_with_category(2, 200, 5), 1)
        .expect("append filtered auction");
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick auction setup");

    {
        let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        bid_writer
            .append_encoded(encoded_bid_row(1, 42, 10), 1)
            .expect("append first matching bid");
        bid_writer
            .append_encoded(encoded_bid_row(2, 7, 20), 1)
            .expect("append filtered bid");
        bid_writer
            .append_encoded(encoded_bid_row(1, 8, 30), 1)
            .expect("append second matching bid");
    }
    registry
        .tick_all_with_version(2)
        .await
        .expect("tick first bid batch");

    wait_for_logical_version(&mv_registry, view_name, 2).await;
    wait_for_visible_row_count(&mv_registry, view_name, 2).await;

    {
        let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        bid_writer
            .append_encoded(encoded_bid_row(1, 9, 40), 1)
            .expect("append later matching bid");
        bid_writer
            .append_encoded(encoded_bid_row(2, 10, 50), 1)
            .expect("append later filtered bid");
    }
    registry
        .tick_all_with_version(3)
        .await
        .expect("tick second bid batch");

    wait_for_logical_version(&mv_registry, view_name, 3).await;
    wait_for_visible_row_count(&mv_registry, view_name, 3).await;

    {
        let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        bid_writer
            .append_encoded(encoded_bid_row(2, 11, 60), 1)
            .expect("append no-op filtered bid");
    }
    registry
        .tick_all_with_version(4)
        .await
        .expect("tick no-op bid batch");

    wait_for_logical_version(&mv_registry, view_name, 4).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by_key(|row| {
        (
            scalar_i64(row.first()),
            scalar_i64(row.get(1)),
            scalar_i64(row.get(2)),
            scalar_timestamp_millis(row.get(3)),
            scalar_i64(row.get(4)),
        )
    });
    rows.sort_by_key(|row| scalar_i64(row.get(1)));
    assert_eq!(
        rows,
        vec![
            int_row(&[1, 8, 30, 100]),
            int_row(&[1, 9, 40, 100]),
            int_row(&[1, 42, 10, 100])
        ]
    );
}

#[tokio::test]
#[serial_test::serial]
async fn pushed_join_filter_preserves_rows_with_source_journal_fast_path() {
    let db = test_db("join-filter-transient-join-inputs").await;
    let view_name = "mv_join_filter_transient_inputs";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let bid_schema = nexmark_bid_schema();
        let auction_schema = nexmark_auction_schema();
        let logical = table_scan(Some("nexmark_bid"), &bid_schema, None)
            .expect("bid scan")
            .join(
                table_scan(Some("nexmark_auction"), &auction_schema, None)
                    .expect("auction scan")
                    .build()
                    .expect("auction plan"),
                JoinType::Inner,
                (
                    vec![Column::from_name("auction")],
                    vec![Column::from_name("id")],
                ),
                None,
            )
            .expect("join")
            .filter(col("category").eq(lit(10i64)))
            .expect("filter")
            .project(vec![
                col("auction"),
                col("bidder"),
                col("price").alias("projected_price"),
                col("seller"),
            ])
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
    registry.set_durable_enabled("nexmark_bid", false);
    registry.set_durable_enabled("nexmark_auction", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("auction", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("projected_price", DataType::Int64, true),
            Field::new("seller", DataType::Int64, true),
        ]),
    );

    let (task_tx, mut task_rx) =
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

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(encoded_auction_row_with_category(1, 100, 10), 1)
        .expect("append matching auction");
    auction_writer
        .append_encoded(encoded_auction_row_with_category(2, 200, 5), 1)
        .expect("append filtered auction");
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick auction setup");

    let expected_rows = 64usize;
    for idx in 0..expected_rows {
        let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        bid_writer
            .append_encoded(encoded_bid_row(1, 1_000 + idx as i64, 10 + idx as i64), 1)
            .expect("append matching bid");
        bid_writer
            .append_encoded(encoded_bid_row(2, 2_000 + idx as i64, 20 + idx as i64), 1)
            .expect("append filtered bid");
        registry
            .tick_all_with_version(i64::try_from(idx + 2).expect("version"))
            .await
            .expect("tick bid batch");
    }

    wait_for_visible_row_count(&mv_registry, view_name, expected_rows).await;

    let mut rows = visible_rows(&mv_registry, view_name).await;
    rows.sort_by_key(|row| scalar_i64(row.get(1)));
    assert_eq!(rows.len(), expected_rows);
    for (idx, row) in rows.iter().enumerate() {
        assert_eq!(
            row,
            &int_row(&[1, 1_000 + idx as i64, 10 + idx as i64, 100])
        );
    }

    let retract_version = i64::try_from(expected_rows + 2).expect("retract version");
    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append_encoded(encoded_auction_row_with_category(1, 100, 10), -1)
        .expect("retract matching auction");
    registry
        .tick_all_with_version(retract_version)
        .await
        .expect("tick auction retraction");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, retract_version, &mut task_rx)
        .await;
    wait_for_exact_visible_row_count(&mv_registry, view_name, 0).await;
}

#[tokio::test]
#[serial_test::serial]
async fn inner_join_materializes_mv_with_transient_join_root_fast_path() {
    let db = test_db("inner-join-transient-root").await;
    let view_name = "mv_join_transient_root";
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
    let mut builder = DbspGraphBuilder::new(db).await.expect("builder");
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);
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
            enable_source_batch_journal: true,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build transient join graph");

    assert_eq!(outputs.required_sources, required_sources);
    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 1).await;

    let rows = visible_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![int_utf8_row(10, Some("alice"))]);
}

#[tokio::test]
#[serial_test::serial]
async fn left_outer_join_materializes_null_extended_rows() {
    let db = test_db("left-outer-join").await;
    let view_name = "mv_left_join";
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
                JoinType::Left,
                (
                    vec![Column::from_name("seller")],
                    vec![Column::from_name("person_id")],
                ),
                None,
            )
            .expect("left join")
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
        .expect("append matched auction");
    auction_writer
        .append_encoded(encoded_auction_row(11, 999), 1)
        .expect("append unmatched auction");
    auction_writer.flush().await.expect("flush auctions");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

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
        .expect("build left join graph");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![int_utf8_row(10, Some("alice")), int_utf8_row(11, None)]
    );
}

#[tokio::test]
#[serial_test::serial]
async fn left_outer_join_live_updates_preserve_logical_versions_on_noop_ticks() {
    let db = test_db("left-outer-join-live-noop").await;
    let view_name = "mv_left_join_live_noop";
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
                JoinType::Left,
                (
                    vec![Column::from_name("seller")],
                    vec![Column::from_name("person_id")],
                ),
                None,
            )
            .expect("left join")
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

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
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
            enable_source_batch_journal: false,
            restore_transient_helper_state: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build left join graph");

    {
        let auction_writer = registry
            .writer_mut("nexmark_auction")
            .expect("auction writer");
        auction_writer
            .append_encoded(encoded_auction_row(11, 999), 1)
            .expect("append unmatched auction");
    }
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick unmatched auction");
    wait_for_logical_version(&mv_registry, view_name, 1).await;
    assert_eq!(
        visible_rows(&mv_registry, view_name).await,
        vec![int_utf8_row(11, None)]
    );

    {
        let person_writer = registry
            .writer_mut("nexmark_person")
            .expect("person writer");
        person_writer
            .append_encoded(encoded_person_row(100, "alice"), 1)
            .expect("append unrelated person");
    }
    registry
        .tick_all_with_version(2)
        .await
        .expect("tick unrelated person");
    wait_for_logical_version(&mv_registry, view_name, 2).await;
    assert_eq!(
        visible_rows(&mv_registry, view_name).await,
        vec![int_utf8_row(11, None)]
    );

    {
        let person_writer = registry
            .writer_mut("nexmark_person")
            .expect("person writer");
        person_writer
            .append_encoded(encoded_person_row(999, "bob"), 1)
            .expect("append matching person");
    }
    registry
        .tick_all_with_version(3)
        .await
        .expect("tick matching person");
    wait_for_logical_version(&mv_registry, view_name, 3).await;
    assert_eq!(
        visible_rows(&mv_registry, view_name).await,
        vec![int_utf8_row(11, Some("bob"))]
    );
}
