use super::*;

#[tokio::test]
#[serial_test::serial]
async fn hopping_window_count_materializes_from_transient_source_journal() {
    let plan = hopping_window_count_plan();
    let rows = build_window_plan_rows(
        "hopping-window-transient",
        "mv_hopping_window_transient",
        &plan,
        &[
            (7, 42, 10, 1_700_000_000_000),
            (7, 77, 20, 1_700_000_001_000),
            (9, 11, 30, 1_700_000_002_000),
        ],
    )
    .await;

    assert!(
        rows.iter().any(|row| row_contains_i64(row, 7)),
        "expected hopping window output to include auction 7"
    );
    assert!(
        rows.iter().any(|row| row_contains_i64(row, 2)),
        "expected hopping window output to include count 2"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn tumbling_window_max_materializes_from_transient_source_journal() {
    let plan = tumbling_window_max_plan();
    let rows = build_window_plan_rows(
        "tumbling-max-transient",
        "mv_tumbling_max_transient",
        &plan,
        &[
            (1, 42, 10, 1_700_000_000_000),
            (2, 7, 20, 1_700_000_001_000),
            (3, 9, 99, 1_700_000_002_000),
        ],
    )
    .await;

    assert!(
        rows.iter().any(|row| row_contains_i64(row, 99)),
        "expected tumbling window output to include max price 99"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn tumbling_window_avg_materializes_from_transient_source_journal() {
    let plan = tumbling_window_avg_plan();
    let rows = build_window_plan_rows(
        "tumbling-avg-transient",
        "mv_tumbling_avg_transient",
        &plan,
        &[
            (1, 42, 10, 1_700_000_000_000),
            (2, 7, 20, 1_700_000_001_000),
            (3, 9, 30, 1_700_000_002_000),
        ],
    )
    .await;

    assert!(rows.iter().any(|row| row_contains_i64(row, 20)));
}

#[tokio::test]
#[serial_test::serial]
async fn tumbling_window_max_recomputes_from_transient_source_journal_retraction() {
    let plan = tumbling_window_max_plan();
    let db = test_db("tumbling-max-retraction-transient").await;
    let view_name = "mv_tumbling_max_retraction_transient";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

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
    mv_registry.set_schema(view_name, root_arrow_schema(&plan));

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
        .expect("build transient window max graph");

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(1, 42, 10, 1_700_000_000_000), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(2, 7, 20, 1_700_000_001_000), 1)
        .expect("append");
    bid_writer
        .append_encoded(encoded_bid_row_with_ts(3, 9, 99, 1_700_000_002_000), 1)
        .expect("append");
    bid_writer.flush().await.expect("flush bids");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 1, &mut task_rx).await;
    wait_for_visible_row_count(&mv_registry, view_name, 1).await;
    assert!(
        visible_rows(&mv_registry, view_name)
            .await
            .iter()
            .any(|row| row_contains_i64(row, 99))
    );

    bid_writer
        .append_encoded(encoded_bid_row_with_ts(3, 9, 99, 1_700_000_002_000), -1)
        .expect("retract max row");
    bid_writer.flush().await.expect("flush max retraction");

    wait_for_logical_version_or_task_error(&mv_registry, view_name, 2, &mut task_rx).await;
    wait_for_visible_row_count(&mv_registry, view_name, 1).await;
    assert!(
        visible_rows(&mv_registry, view_name)
            .await
            .iter()
            .any(|row| row_contains_i64(row, 20))
    );
}

#[tokio::test]
#[serial_test::serial]
async fn tumbling_window_max_materializes_from_durable_transient_source_journal() {
    let plan = tumbling_window_max_plan();
    let rows = build_window_plan_rows_with_durable_source(
        "tumbling-max-durable-transient",
        "mv_tumbling_max_durable_transient",
        &plan,
        &[
            (1, 42, 10, 1_700_000_000_000),
            (2, 7, 20, 1_700_000_001_000),
            (3, 9, 99, 1_700_000_002_000),
        ],
        true,
    )
    .await;

    assert!(
        rows.iter().any(|row| row_contains_i64(row, 99)),
        "expected durable tumbling window output to include max price 99"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn tumbling_window_count_by_bidder_materializes_from_transient_source_journal() {
    let plan = tumbling_window_count_by_bidder_plan();
    let mut rows = build_window_plan_rows(
        "tumbling-count-bidder-transient",
        "mv_tumbling_count_bidder_transient",
        &plan,
        &[
            (11, 42, 10, 1_700_000_000_000),
            (12, 42, 20, 1_700_000_001_000),
            (13, 7, 30, 1_700_000_001_500),
        ],
    )
    .await;

    assert!(
        rows.iter()
            .any(|row| row_contains_i64(row, 42) && row_contains_i64(row, 2)),
        "expected bidder 42 aggregate count of 2"
    );
    let mut expected = vec![
        timestamp_int_row(1_700_000_000_000, 1_700_000_010_000, &[7, 1]),
        timestamp_int_row(1_700_000_000_000, 1_700_000_010_000, &[42, 2]),
    ];
    rows.sort_by_key(|row| scalar_i64(row.get(2)));
    expected.sort_by_key(|row| scalar_i64(row.get(2)));
    assert_eq!(rows, expected);
}
