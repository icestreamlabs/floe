use anyhow::Result;

use floe_node_core::generator::BID_SOURCE_NAME;

use crate::harness::MvTestHarness;
use crate::helpers::{
    append_auction, append_bid, append_row, wait_for_materialized_row_count, wait_for_version,
};
use crate::rows::{bid_row_nullable, int_rows, int_rows2};

#[tokio::test]
#[serial_test::serial]
async fn sql_filter_excludes_nulls() -> Result<()> {
    let mut harness = MvTestHarness::new(
        "mv_null_filter",
        "CREATE MATERIALIZED VIEW mv_null_filter AS \
         SELECT auction, bidder FROM nexmark_bid WHERE bidder = 42",
    )
    .await?;

    append_row(
        &mut harness.outer,
        &mut harness.ingestion_bridge,
        BID_SOURCE_NAME,
        bid_row_nullable(Some(1), None, 10),
    )
    .await?;
    let handle = append_row(
        &mut harness.outer,
        &mut harness.ingestion_bridge,
        BID_SOURCE_NAME,
        bid_row_nullable(Some(2), Some(42), 20),
    )
    .await?;

    wait_for_version(
        &harness.mv_registry,
        &harness.view_name,
        handle.version as i64,
    )
    .await?;

    let (session, _) = harness.session_with_view().await?;
    let df = session
        .sql("SELECT auction, bidder FROM mv_null_filter ORDER BY auction")
        .await?;
    let batches = df.collect().await?;
    let rows = int_rows2(&batches);
    assert_eq!(rows, vec![vec![2, 42]]);

    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn sql_join_skips_null_keys() -> Result<()> {
    let mut harness = MvTestHarness::new(
        "mv_null_join",
        "CREATE MATERIALIZED VIEW mv_null_join AS \
         SELECT b.auction, b.bidder, a.seller \
         FROM nexmark_bid AS b JOIN nexmark_auction AS a ON b.auction = a.id",
    )
    .await?;

    append_auction(
        &mut harness.outer,
        &mut harness.ingestion_bridge,
        1,
        100,
        5,
        1_600_010_000,
        "chair",
    )
    .await?;
    append_row(
        &mut harness.outer,
        &mut harness.ingestion_bridge,
        BID_SOURCE_NAME,
        bid_row_nullable(None, Some(7), 50),
    )
    .await?;
    append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 1, 8, 60).await?;
    wait_for_materialized_row_count(&harness.mv_registry, &harness.view_name, 1).await?;

    let (session, _) = harness.session_with_view().await?;
    let df = session
        .sql("SELECT auction, bidder, seller FROM mv_null_join ORDER BY bidder")
        .await?;
    let batches = df.collect().await?;
    let rows = int_rows(&batches);
    assert_eq!(rows, vec![vec![1, 8, 100]]);

    Ok(())
}
