use std::sync::Arc;

use anyhow::Result;
use floe_executor::{DbspBridge, FloeQueryContext, MaterializedViewRegistry, load_or_register_mv};
use floe_storage::SlateCatalog;

use crate::harness::MvTestHarness;
use crate::helpers::{append_bid, wait_for_version};
use crate::rows::int_rows;

#[tokio::test]
async fn sql_restart_recovers_view_state() -> Result<()> {
    let catalog = Arc::new(SlateCatalog::in_memory().await?);
    let mut harness = MvTestHarness::new_with_catalog(
        Arc::clone(&catalog),
        "mv_restart",
        "CREATE MATERIALIZED VIEW mv_restart AS \
         SELECT auction, bidder, price FROM nexmark_bid WHERE price > 0",
    )
    .await?;

    append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 1, 7, 50).await?;
    append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 2, 8, 60).await?;
    wait_for_version(&harness.mv_registry, &harness.view_name, 2).await?;

    let (session, _) = harness.session_with_view().await?;
    let df = session
        .sql("SELECT auction, bidder, price FROM mv_restart ORDER BY auction")
        .await?;
    let batches = df.collect().await?;
    let rows = int_rows(&batches);
    assert_eq!(rows, vec![vec![1, 7, 50], vec![2, 8, 60]]);

    let query = FloeQueryContext::new(Arc::clone(&catalog));
    let session = query.session();
    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let mut bridge = DbspBridge::new(catalog.db()).await?;
    load_or_register_mv(
        &session,
        Arc::clone(&mv_registry),
        &mut bridge,
        "mv_restart",
    )
    .await?;
    let df = session
        .sql("SELECT auction, bidder, price FROM mv_restart ORDER BY auction")
        .await?;
    let batches = df.collect().await?;
    let recovered_rows = int_rows(&batches);
    assert_eq!(recovered_rows, rows);

    Ok(())
}
