use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use arrow_schema::{DataType, Field, Schema};
use datafusion::common::Column;
use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion::logical_expr::{Expr, JoinType, col, lit, table_scan};
use datafusion::scalar::ScalarValue;
use dbsp::StreamRetention;
use dbsp::handles::{ZSetHandle, ZSetHandleView};
use dbsp::storage::SlateTable;
use floe_executor::GraphTaskError;
use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::dbsp_graph_builder::{BuildInputs, DbspGraphBuilder};
use floe_executor::dbsp_plan::{
    DbspPlanBuilder, nexmark_auction_table, nexmark_bid_table, nexmark_config,
    nexmark_person_table, validate_dbsp_plan,
};
use floe_executor::encoding::decode_projected_row_key;
use floe_executor::materialized_view::MaterializedViewRegistry;
use floe_executor::outer_stream::OuterStreamRegistry;
use floe_executor::source_journal::SourceBatchJournal;
use object_store::memory::InMemory;
use slatedb::Db;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

fn arrow_schema(fields: Vec<Field>) -> Arc<Schema> {
    Arc::new(Schema::new(fields))
}

#[tokio::test]
async fn filter_and_projection_materializes_mv() {
    let db = test_db("filter-projection").await;
    let view_name = "mv_price";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .project(vec![col("price")])
            .expect("project")
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

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append(&bid_row(1, 42, 99), 1)
        .expect("append bidder 42");
    bid_writer.flush().await.expect("flush first step");
    bid_writer
        .append(&bid_row(2, 7, 50), 1)
        .expect("append bidder 7");
    bid_writer.flush().await.expect("flush second step");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![Field::new("price", DataType::Int64, true)]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
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
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    assert_eq!(outputs.required_sources, required_sources);

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![vec![ScalarValue::Int64(Some(99))]]);
}

#[tokio::test]
async fn source_batch_journal_replay_recovers_overlay_view() {
    let db = test_db("source-batch-journal-replay").await;
    let view_name = "mv_source_batch_journal";
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

    let plan = {
        let schema = nexmark_bid_schema();
        let logical = table_scan(Some("nexmark_bid"), &schema, None)
            .expect("scan")
            .project(vec![col("price")])
            .expect("project")
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
    registry.set_durable_enabled("nexmark_bid", false);

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![Field::new("price", DataType::Int64, true)]);
    mv_registry.set_schema(view_name, arrow_schema.clone());

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
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
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    let journal = SourceBatchJournal::new(Arc::new(SlateTable::new(Arc::clone(&db))));
    {
        let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        writer
            .append(&bid_row(1, 42, 99), 1)
            .expect("append bidder 42");
        writer
            .append(&bid_row(2, 7, 50), 1)
            .expect("append bidder 7");
        let batch = writer
            .pending_transient_batch(1)
            .expect("pending transient batch");
        journal
            .append("nexmark_bid", 1, None, &batch.deltas)
            .await
            .expect("append source journal");
    }
    registry
        .tick_all_with_version(1)
        .await
        .expect("tick transient source root");
    wait_for_logical_version(&mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&mv_registry, view_name, 1).await;

    let rows = visible_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![vec![ScalarValue::Int64(Some(99))]]);

    let mut restarted_bridge = DbspBridge::new(Arc::clone(&db))
        .await
        .expect("restarted bridge");
    let mut restarted_registry =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut restarted_bridge)
            .await
            .expect("restarted outer streams");
    restarted_registry.set_durable_enabled("nexmark_bid", false);

    let restarted_mv_registry = Arc::new(MaterializedViewRegistry::new());
    restarted_mv_registry.register(view_name);
    restarted_mv_registry.set_schema(view_name, arrow_schema);

    let restarted_handle_streams = gather_handle_streams(&restarted_registry, &source_refs);
    let restarted_transient_streams = gather_transient_streams(&restarted_registry, &source_refs);
    let mut restarted_builder = DbspGraphBuilder::new(Arc::clone(&db))
        .await
        .expect("restarted builder");
    restarted_builder
        .build(BuildInputs {
            graph_id: view_name,
            view_name,
            plan: &plan,
            cancel: CancellationToken::new(),
            task_events: task_tx,
            mv_registry: Arc::clone(&restarted_mv_registry),
            outer_handle_streams: &restarted_handle_streams,
            outer_transient_streams: &restarted_transient_streams,
            enable_source_batch_journal: true,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("rebuild graph");

    journal
        .replay_committed_entries_up_to(&mut restarted_registry, 1, &required_sources)
        .await
        .expect("replay source journal");
    wait_for_logical_version(&restarted_mv_registry, view_name, 1).await;
    wait_for_visible_row_count(&restarted_mv_registry, view_name, 1).await;

    let restarted_rows = visible_rows(&restarted_mv_registry, view_name).await;
    assert_eq!(restarted_rows, rows);
}

#[tokio::test]
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
        .append(&person_row(100, "alice"), 1)
        .expect("append alice");
    person_writer.flush().await.expect("flush person");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append(&auction_row(10, 100), 1)
        .expect("append auction");
    auction_writer.flush().await.expect("flush auction");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
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
            enable_source_batch_journal: false,
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build join graph");

    assert_eq!(outputs.required_sources, required_sources);

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(
        rows,
        vec![vec![
            ScalarValue::Int64(Some(10)),
            ScalarValue::Utf8(Some("alice".to_string())),
        ]]
    );
}

#[tokio::test]
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
        .append(&person_row(100, "alice"), 1)
        .expect("append alice");
    person_writer.flush().await.expect("flush person");

    let auction_writer = registry
        .writer_mut("nexmark_auction")
        .expect("auction writer");
    auction_writer
        .append(&auction_row(10, 100), 1)
        .expect("append matched auction");
    auction_writer
        .append(&auction_row(11, 999), 1)
        .expect("append unmatched auction");
    auction_writer.flush().await.expect("flush auctions");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
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
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build left join graph");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![
            vec![
                ScalarValue::Int64(Some(10)),
                ScalarValue::Utf8(Some("alice".to_string())),
            ],
            vec![ScalarValue::Int64(Some(11)), ScalarValue::Utf8(None)],
        ]
    );
}

#[tokio::test]
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

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
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
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build aggregate graph");

    let mut version_rx = view_handle.version_watch();
    version_rx.borrow_and_update();

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append(&bid_row(1, 42, 10), 1)
        .expect("append bidder 42");
    bid_writer
        .append(&bid_row(2, 42, 30), 1)
        .expect("append bidder 42");
    bid_writer
        .append(&bid_row(3, 7, 5), 1)
        .expect("append bidder 7");
    bid_writer.flush().await.expect("flush bids");

    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("aggregate update timeout")
        .expect("aggregate update");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    let mut expected = vec![
        vec![
            ScalarValue::Int64(Some(7)),
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(5)),
            ScalarValue::Int64(Some(5)),
            ScalarValue::Int64(Some(5)),
            ScalarValue::Int64(Some(5)),
        ],
        vec![
            ScalarValue::Int64(Some(42)),
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(40)),
            ScalarValue::Int64(Some(10)),
            ScalarValue::Int64(Some(30)),
            ScalarValue::Int64(Some(20)),
        ],
    ];
    sort_rows_by_first_column(&mut expected);
    assert_eq!(rows, expected);

    bid_writer
        .append(&bid_row(2, 42, 30), -1)
        .expect("remove bidder 42");
    bid_writer.flush().await.expect("flush removal");

    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("aggregate update timeout")
        .expect("aggregate update");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    let mut expected = vec![
        vec![
            ScalarValue::Int64(Some(7)),
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(5)),
            ScalarValue::Int64(Some(5)),
            ScalarValue::Int64(Some(5)),
            ScalarValue::Int64(Some(5)),
        ],
        vec![
            ScalarValue::Int64(Some(42)),
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(10)),
            ScalarValue::Int64(Some(10)),
            ScalarValue::Int64(Some(10)),
            ScalarValue::Int64(Some(10)),
        ],
    ];
    sort_rows_by_first_column(&mut expected);
    assert_eq!(rows, expected);
}

#[tokio::test]
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
    bid_writer.append(&bid_row(1, 7, 10), 1).expect("append 10");
    bid_writer.append(&bid_row(2, 8, 30), 1).expect("append 30");
    bid_writer.append(&bid_row(3, 9, 20), 1).expect("append 20");
    bid_writer
        .append(&bid_row(4, 10, 30), 1)
        .expect("append 30 again");
    bid_writer.flush().await.expect("flush bids");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![Field::new("price", DataType::Int64, true)]);
    mv_registry.set_schema(view_name, arrow_schema);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
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
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build topn graph");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![
            vec![ScalarValue::Int64(Some(30))],
            vec![ScalarValue::Int64(Some(30))],
        ]
    );
}

#[tokio::test]
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
    let view_handle = mv_registry.register(view_name);
    mv_registry.set_schema(
        view_name,
        arrow_schema(vec![Field::new("bidder", DataType::Int64, true)]),
    );

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
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
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build distinct graph");

    let mut version_rx = view_handle.version_watch();
    version_rx.borrow_and_update();

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    bid_writer
        .append(&bid_row(1, 42, 10), 1)
        .expect("append first bidder");
    bid_writer
        .append(&bid_row(2, 42, 20), 1)
        .expect("append duplicate bidder");
    bid_writer
        .append(&bid_row(3, 7, 30), 1)
        .expect("append second bidder");
    bid_writer.flush().await.expect("flush bids");

    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("distinct update timeout")
        .expect("distinct update");

    let mut rows = materialized_rows(&mv_registry, view_name).await;
    sort_rows_by_first_column(&mut rows);
    assert_eq!(
        rows,
        vec![
            vec![ScalarValue::Int64(Some(7))],
            vec![ScalarValue::Int64(Some(42))]
        ]
    );
}

#[tokio::test]
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

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
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
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build distinct aggregate graph");

    let mut version_rx = view_handle.version_watch();
    version_rx.borrow_and_update();

    let bid_writer = registry.writer_mut("nexmark_bid").expect("bid writer");
    // Unique (auction, bidder) pairs: (1,42), (1,7), (2,7) => count 3.
    bid_writer.append(&bid_row(1, 42, 10), 1).expect("append");
    bid_writer.append(&bid_row(1, 42, 20), 1).expect("append");
    bid_writer.append(&bid_row(1, 7, 30), 1).expect("append");
    bid_writer.append(&bid_row(2, 7, 40), 1).expect("append");
    bid_writer.flush().await.expect("flush bids");

    timeout(Duration::from_millis(200), version_rx.changed())
        .await
        .expect("distinct aggregate update timeout")
        .expect("distinct aggregate update");

    let rows = materialized_rows(&mv_registry, view_name).await;
    assert_eq!(rows, vec![vec![ScalarValue::Int64(Some(3))]]);
}

#[tokio::test]
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
    writer.append(&bid_row(1, 42, 80), 1).expect("append row");
    writer.flush().await.expect("flush one");
    writer
        .append(&bid_row(2, 42, 81), 1)
        .expect("append second");
    writer.flush().await.expect("flush two");

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    mv_registry.register(view_name);
    let arrow_schema = arrow_schema(vec![Field::new("auction", DataType::Int64, true)]);
    mv_registry.set_schema(view_name, arrow_schema);

    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&registry, &source_refs);
    let transient_streams = gather_transient_streams(&registry, &source_refs);

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
    {
        let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
            .await
            .expect("builder");
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
                mv_retention: StreamRetention::KeepLast { keep_last: 1 },
                watermark: Arc::new(AtomicI64::new(-1)),
            })
            .await
            .expect("initial build");
        assert_eq!(outputs.required_sources, required_sources.clone());
    }

    materialized_rows(&mv_registry, view_name).await;

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

    let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
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
            mv_retention: StreamRetention::KeepLast { keep_last: 1 },
            watermark: Arc::new(AtomicI64::new(-1)),
        })
        .await
        .expect("build graph");

    let mut version_rx = view_handle.version_watch();
    {
        let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
        writer.append(&bid_row(1, 42, 99), 1).expect("append first");
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
            .append(&bid_row(2, 42, 100), 1)
            .expect("append second");
        writer.flush().await.expect("flush second");
    }

    let update = timeout(Duration::from_millis(100), version_rx.changed()).await;
    assert!(update.is_err(), "expected no update after cancel");
    assert_eq!(view_handle.latest_version(), Some(first_version));
}

#[tokio::test]
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
    let (task_tx, mut task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
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

fn gather_handle_streams(
    registry: &OuterStreamRegistry,
    sources: &[&str],
) -> HashMap<String, dbsp::DeltaHandleStream> {
    let mut map = HashMap::new();
    for source in sources {
        if let Some(stream) = registry.delta_handle_stream(source) {
            map.insert((*source).to_string(), stream);
        }
    }
    map
}

fn gather_transient_streams(
    registry: &OuterStreamRegistry,
    sources: &[&str],
) -> HashMap<String, floe_executor::outer_stream::TransientSourceHandleStream> {
    let mut map = HashMap::new();
    for source in sources {
        if let Some(stream) = registry.transient_stream(source) {
            map.insert((*source).to_string(), stream);
        }
    }
    map
}

async fn wait_for_logical_version(
    registry: &MaterializedViewRegistry,
    view_name: &str,
    target_version: i64,
) {
    let handle = registry.get(view_name).expect("view registered");
    if handle.latest_version().unwrap_or(-1) >= target_version {
        return;
    }
    let mut rx = handle.version_watch();
    timeout(Duration::from_secs(5), async {
        loop {
            if rx.borrow().unwrap_or(-1) >= target_version {
                break;
            }
            rx.changed().await.expect("version watch update");
        }
    })
    .await
    .expect("wait for logical version");
}

async fn wait_for_visible_row_count(
    registry: &MaterializedViewRegistry,
    view_name: &str,
    expected_rows: usize,
) {
    timeout(Duration::from_secs(5), async {
        loop {
            if visible_rows(registry, view_name).await.len() >= expected_rows {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("wait for visible rows");
}

fn sort_rows_by_first_column(rows: &mut [Vec<ScalarValue>]) {
    rows.sort_by_key(|row| match row.first() {
        Some(ScalarValue::Int64(Some(value))) => *value,
        Some(ScalarValue::TimestampMillisecond(Some(value), _)) => *value,
        _ => 0,
    });
}

fn bid_row(auction: i64, bidder: i64, price: i64) -> Vec<ScalarValue> {
    vec![
        ScalarValue::Int64(Some(auction)),
        ScalarValue::Int64(Some(bidder)),
        ScalarValue::Int64(Some(price)),
        ScalarValue::Utf8(Some("channel".to_string())),
        ScalarValue::Utf8(Some("url".to_string())),
        ScalarValue::TimestampMillisecond(Some(1_700_000_000_000), None),
        ScalarValue::Utf8(Some("extra".to_string())),
    ]
}

fn person_row(id: i64, name: &str) -> Vec<ScalarValue> {
    vec![
        ScalarValue::Int64(Some(id)),
        ScalarValue::Utf8(Some(name.to_string())),
        ScalarValue::Utf8(Some("email".to_string())),
        ScalarValue::Utf8(Some("card".to_string())),
        ScalarValue::Utf8(Some("city".to_string())),
        ScalarValue::Utf8(Some("state".to_string())),
        ScalarValue::TimestampMillisecond(Some(1_700_000_000_000), None),
        ScalarValue::Utf8(Some("extra".to_string())),
    ]
}

fn auction_row(id: i64, seller: i64) -> Vec<ScalarValue> {
    vec![
        ScalarValue::Int64(Some(id)),
        ScalarValue::Utf8(Some("item".to_string())),
        ScalarValue::Utf8(Some("desc".to_string())),
        ScalarValue::Int64(Some(10)),
        ScalarValue::Int64(Some(20)),
        ScalarValue::Int64(Some(seller)),
        ScalarValue::Int64(Some(5)),
        ScalarValue::TimestampMillisecond(Some(1_700_000_000_000), None),
        ScalarValue::TimestampMillisecond(Some(1_700_000_100_000), None),
        ScalarValue::Utf8(Some("extra".to_string())),
    ]
}

async fn test_db(name: &str) -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open(name, store).await.expect("open SlateDB"))
}

fn nexmark_bid_schema() -> Arc<Schema> {
    nexmark_bid_table().schema().to_arrow_schema()
}

fn nexmark_person_schema() -> Arc<Schema> {
    nexmark_person_table().schema().to_arrow_schema()
}

fn nexmark_auction_schema() -> Arc<Schema> {
    nexmark_auction_table().schema().to_arrow_schema()
}

async fn materialized_rows(
    registry: &MaterializedViewRegistry,
    view_name: &str,
) -> Vec<Vec<ScalarValue>> {
    let handle = registry.get(view_name).expect("view registered");
    let state = handle.dbsp_state().expect("mv state");
    let view = ZSetHandleView::new(
        state.dictionary(),
        state.table(),
        state.namespace().to_string(),
        state.version(),
    );
    let snapshot = view.materialize().await.expect("materialize view");
    let mut rows = Vec::new();
    for (key, diff) in snapshot {
        let decoded = decode_projected_row_key(&key).expect("decode row");
        if diff > 0 {
            for _ in 0..diff {
                rows.push(decoded.clone());
            }
        }
    }
    rows
}

async fn visible_rows(
    registry: &MaterializedViewRegistry,
    view_name: &str,
) -> Vec<Vec<ScalarValue>> {
    let handle = registry.get(view_name).expect("view registered");
    if handle.dbsp_state().is_some() {
        return materialized_rows(registry, view_name).await;
    }

    let mut rows = Vec::new();
    for (row, diff) in handle.snapshot() {
        if diff > 0 {
            for _ in 0..diff {
                rows.push(row.clone());
            }
        }
    }
    rows
}
