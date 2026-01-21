use anyhow::Result;
use dbsp::handles::ZSetHandleView;

use crate::harness::MvTestHarness;
use crate::helpers::{append_bid, assert_manifest_exists, wait_for_version};
use crate::rows::int_rows;

#[tokio::test]
async fn materialized_view_ingests_and_queries() -> Result<()> {
    let mut harness = MvTestHarness::new(
        "mv_q1",
        "CREATE MATERIALIZED VIEW mv_q1 AS \
         SELECT auction, bidder, price * 2 AS price \
         FROM nexmark_bid WHERE bidder = 42",
    )
    .await?;

    let handles = vec![
        append_bid(
            &mut harness.outer,
            &mut harness.ingestion_bridge,
            1,
            42,
            100,
        )
        .await?,
        append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 2, 10, 50).await?,
        append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 3, 42, 75).await?,
    ];
    for handle in &handles {
        assert_manifest_exists(
            harness.ingestion_bridge.table(),
            &handle.namespace,
            handle.version,
        )
        .await?;
    }
    wait_for_version(&harness.mv_registry, &harness.view_name, 2).await?;

    let (session, _bridge) = harness.session_with_view().await?;

    // Inspect the persisted state directly to ensure weights are correct.
    let persisted = harness
        .mv_registry
        .get(&harness.view_name)
        .expect("mv handle")
        .dbsp_state()
        .expect("persisted state");
    let handle_view = ZSetHandleView::new(
        persisted.dictionary(),
        persisted.table(),
        persisted.namespace().to_string(),
        persisted.version(),
    );
    let materialized = handle_view.materialize().await.unwrap();
    let total_weight: i64 = materialized.values().copied().sum();
    assert_eq!(total_weight, 2, "expected total weight of 2 rows");
    assert!(
        materialized.values().all(|w| *w == 1),
        "each row should have weight 1, got {:?}",
        materialized
    );

    let df = session
        .sql("SELECT auction, bidder, price FROM mv_q1 ORDER BY auction")
        .await?;
    let batches = df.collect().await?;
    let rows = int_rows(&batches);
    assert_eq!(rows, vec![vec![1, 42, 200], vec![3, 42, 150]]);

    Ok(())
}
