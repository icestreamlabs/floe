use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use anyhow::Result;
use dbsp::StreamRetention;
use floe_executor::{
    BuildInputs, DbspBridge, DbspGraphBuilder, GraphTaskError, MaterializedViewRegistry,
    OuterStreamRegistry, ValidatedPlan, validate_dbsp_plan,
};
use floe_node::executor::{available_sources_from_registry, build_dataflows};
use floe_node::generator;
use floe_node::planner::plan_materialized_views;
use floe_node::source::SourceRegistry;
use floe_sql_parser::parse_materialized_view;
use floe_storage::SlateCatalog;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::harness::MvTestHarness;
use crate::helpers::{
    append_bid, assert_manifest_exists, wait_for_materialized_row_count, wait_for_version,
};
use crate::rows::int_rows_n;

#[tokio::test]
#[serial_test::serial]
async fn wrapped_q15_style_aggregate_materializes_mv() -> Result<()> {
    let mut harness = MvTestHarness::new(
        "mv_q15_wrapped",
        "CREATE MATERIALIZED VIEW mv_q15_wrapped AS \
         WITH bid AS (SELECT auction, bidder, price, channel, url, date_time AS \"dateTime\", extra FROM nexmark_bid) \
         SELECT DATE_FORMAT(\"dateTime\", 'yyyy-MM-dd') AS day, \
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
         FROM bid GROUP BY DATE_FORMAT(\"dateTime\", 'yyyy-MM-dd')",
    )
    .await?;

    let handles = vec![
        append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 1, 42, 10).await?,
        append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 1, 42, 30).await?,
        append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 2, 42, 15).await?,
        append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 3, 7, 25).await?,
    ];
    for handle in &handles {
        assert_manifest_exists(
            harness.ingestion_bridge.table(),
            &handle.namespace,
            handle.version,
        )
        .await?;
    }
    let target_version = handles.last().expect("latest handle").version as i64;
    wait_for_version(&harness.mv_registry, &harness.view_name, target_version).await?;
    wait_for_materialized_row_count(&harness.mv_registry, &harness.view_name, 1).await?;

    let (session, _bridge) = harness.session_with_view().await?;
    let df = session
        .sql(
            "SELECT total_bidders, rank1_bidders, rank2_bidders, rank3_bidders, \
                    total_auctions, rank1_auctions, rank2_auctions, rank3_auctions \
             FROM mv_q15_wrapped",
        )
        .await?;
    let batches = df.collect().await?;
    let rows = int_rows_n(&batches, 8);

    assert_eq!(rows, vec![vec![2, 2, 0, 0, 3, 3, 0, 0]]);

    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn wrapped_q15_style_aggregate_materializes_with_parallel_ingest_view() -> Result<()> {
    let catalog = Arc::new(SlateCatalog::in_memory().await?);
    let db = catalog.db();

    let ingest_sql = "CREATE MATERIALIZED VIEW mv_parallel_ingest_bid AS \
        SELECT auction FROM nexmark_bid";
    let result_sql = "CREATE MATERIALIZED VIEW mv_parallel_q15_wrapped AS \
        WITH bid AS (SELECT auction, bidder, price, channel, url, date_time AS \"dateTime\", extra FROM nexmark_bid) \
        SELECT DATE_FORMAT(\"dateTime\", 'yyyy-MM-dd') AS day, \
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
        FROM bid GROUP BY DATE_FORMAT(\"dateTime\", 'yyyy-MM-dd')";

    let mut registry = SourceRegistry::new();
    registry.extend(generator::definitions()?);
    let available_sources = available_sources_from_registry(&registry);

    let definitions = vec![
        parse_materialized_view(ingest_sql)?,
        parse_materialized_view(result_sql)?,
    ];
    let planned = plan_materialized_views(&registry, &definitions).await?;
    let circuit_plans = build_dataflows(&planned, &available_sources, &registry)?;
    assert_eq!(circuit_plans.len(), 2);

    let mut required_sources = std::collections::BTreeSet::new();
    for (view_name, plan) in [
        ("mv_parallel_ingest_bid", &circuit_plans[0]),
        ("mv_parallel_q15_wrapped", &circuit_plans[1]),
    ] {
        let ValidatedPlan {
            required_sources: sources,
            ..
        } = validate_dbsp_plan(plan, &available_sources, view_name)?;
        required_sources.extend(sources);
    }

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let mut graph_builder = DbspGraphBuilder::new(Arc::clone(&db)).await?;
    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await?;
    let mut outer =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await?;
    let source_refs: Vec<&str> = required_sources.iter().map(String::as_str).collect();
    let handle_streams = gather_handle_streams(&outer, &source_refs);
    let transient_streams = gather_transient_streams(&outer, &source_refs);
    let (task_tx, _task_rx) =
        mpsc::channel::<GraphTaskError>(floe_executor::GRAPH_TASK_EVENT_CHANNEL_CAPACITY);

    graph_builder
        .build(BuildInputs {
            graph_id: "mv_parallel_ingest_bid",
            view_name: "mv_parallel_ingest_bid",
            plan: &circuit_plans[0],
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
        .await?;

    graph_builder
        .build(BuildInputs {
            graph_id: "mv_parallel_q15_wrapped",
            view_name: "mv_parallel_q15_wrapped",
            plan: &circuit_plans[1],
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
        .await?;

    let handles = [
        append_bid(&mut outer, &mut ingestion_bridge, 1, 42, 10).await?,
        append_bid(&mut outer, &mut ingestion_bridge, 1, 42, 30).await?,
        append_bid(&mut outer, &mut ingestion_bridge, 2, 42, 15).await?,
        append_bid(&mut outer, &mut ingestion_bridge, 3, 7, 25).await?,
    ];
    let target_version = handles.last().expect("latest handle").version as i64;
    wait_for_version(&mv_registry, "mv_parallel_q15_wrapped", target_version).await?;
    wait_for_materialized_row_count(&mv_registry, "mv_parallel_q15_wrapped", 1).await?;

    Ok(())
}

fn gather_handle_streams(
    outer: &OuterStreamRegistry,
    sources: &[&str],
) -> HashMap<String, dbsp::DeltaHandleStream> {
    let mut map = HashMap::new();
    for source in sources {
        if let Some(stream) = outer.delta_handle_stream(source) {
            map.insert((*source).to_string(), stream);
        }
    }
    map
}

fn gather_transient_streams(
    outer: &OuterStreamRegistry,
    sources: &[&str],
) -> HashMap<String, floe_executor::outer_stream::TransientSourceHandleStream> {
    let mut map = HashMap::new();
    for source in sources {
        if let Some(stream) = outer.transient_stream(source) {
            map.insert((*source).to_string(), stream);
        }
    }
    map
}
