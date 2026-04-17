use anyhow::Result;

use crate::harness::MvTestHarness;
use crate::helpers::{
    append_auction, append_bid, assert_manifest_exists, wait_for_materialized_row_count,
    wait_for_version,
};
use crate::rows::int_rows;

#[tokio::test]
#[serial_test::serial]
async fn mixed_workload_join_regression_handles_retractions() -> Result<()> {
    let mut harness = MvTestHarness::new(
        "mv_mixed",
        "CREATE MATERIALIZED VIEW mv_mixed AS \
         SELECT b.auction, b.bidder, a.seller \
         FROM nexmark_bid AS b \
         JOIN nexmark_auction AS a ON b.auction = a.id",
    )
    .await?;

    let handles = vec![
        append_auction(
            &mut harness.outer,
            &mut harness.ingestion_bridge,
            1,
            7,
            1,
            50_000,
            "item-1",
        )
        .await?,
        append_auction(
            &mut harness.outer,
            &mut harness.ingestion_bridge,
            2,
            8,
            1,
            50_000,
            "item-2",
        )
        .await?,
        append_bid(
            &mut harness.outer,
            &mut harness.ingestion_bridge,
            1,
            10,
            100,
        )
        .await?,
        append_bid(
            &mut harness.outer,
            &mut harness.ingestion_bridge,
            1,
            12,
            150,
        )
        .await?,
        append_bid(
            &mut harness.outer,
            &mut harness.ingestion_bridge,
            2,
            11,
            200,
        )
        .await?,
    ];

    for handle in &handles {
        assert_manifest_exists(
            harness.ingestion_bridge.table(),
            &handle.namespace,
            handle.version,
        )
        .await?;
    }

    let target_version = handles
        .iter()
        .map(|handle| handle.version as i64)
        .max()
        .unwrap_or(0);
    wait_for_version(&harness.mv_registry, &harness.view_name, target_version).await?;
    wait_for_materialized_row_count(&harness.mv_registry, &harness.view_name, 3).await?;
    let (session, _bridge) = harness.session_with_view().await?;

    let df = session
        .sql("SELECT auction, bidder, seller FROM mv_mixed ORDER BY auction, bidder")
        .await?;
    let batches = df.collect().await?;
    let rows = int_rows(&batches);

    assert_eq!(rows, vec![vec![1, 10, 7], vec![1, 12, 7], vec![2, 11, 8]]);

    Ok(())
}
