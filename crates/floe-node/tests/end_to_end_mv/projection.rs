use anyhow::Result;

use crate::harness::MvTestHarness;
use crate::helpers::{append_bid, wait_for_version};
use crate::rows::int_rows2;

#[tokio::test]
async fn sql_projection_applies_expressions() -> Result<()> {
    let mut harness = MvTestHarness::new(
        "mv_projection_expr",
        "CREATE MATERIALIZED VIEW mv_projection_expr AS \
         SELECT auction, price + 10 AS adjusted_price \
         FROM nexmark_bid WHERE price > 50",
    )
    .await?;

    append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 1, 7, 40).await?;
    append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 2, 8, 70).await?;

    wait_for_version(&harness.mv_registry, &harness.view_name, 1).await?;

    let (session, _) = harness.session_with_view().await?;
    let df = session
        .sql("SELECT auction, adjusted_price FROM mv_projection_expr ORDER BY auction")
        .await?;
    let batches = df.collect().await?;
    let rows = int_rows2(&batches);
    assert_eq!(rows, vec![vec![2, 80]]);

    Ok(())
}
