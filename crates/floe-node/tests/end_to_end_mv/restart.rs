use std::sync::Arc;

use anyhow::Result;
use floe_executor::checkpoint::{CheckpointManager, recover_materialized_views};
use floe_executor::{
    DbspBridge, FloeQueryContext, MaterializedViewRegistry, OuterStreamRegistry,
    load_or_register_mv,
};
use floe_node::generator::BID_SOURCE_NAME;
use floe_storage::SlateCatalog;

use crate::harness::MvTestHarness;
use crate::helpers::{append_bid, rows_at_version, wait_for_version};
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

#[tokio::test]
async fn checkpoint_restart_recovers_exact_version() -> Result<()> {
    let catalog = Arc::new(SlateCatalog::in_memory().await?);
    let mut harness = MvTestHarness::new_with_catalog(
        Arc::clone(&catalog),
        "mv_checkpoint_restart",
        "CREATE MATERIALIZED VIEW mv_checkpoint_restart AS \
         SELECT auction, bidder, price FROM nexmark_bid WHERE price > 0",
    )
    .await?;

    append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 1, 7, 50).await?;
    append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 2, 8, 60).await?;
    wait_for_version(&harness.mv_registry, &harness.view_name, 2).await?;

    let mut expected_rows = rows_at_version(&harness.mv_registry, &harness.view_name, 2).await?;
    expected_rows.sort();

    let mut checkpoint_manager =
        CheckpointManager::new("mv_checkpoint_restart", harness.ingestion_bridge.table()).await?;
    let manifest = checkpoint_manager
        .persist_snapshot(0, &harness.mv_registry, &harness.outer)
        .await?;
    let mv_entry = manifest
        .materialized_views
        .iter()
        .find(|entry| entry.view == harness.view_name)
        .expect("checkpoint manifest entry for view");
    assert_eq!(mv_entry.version, 2);

    append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 3, 9, 70).await?;
    wait_for_version(&harness.mv_registry, &harness.view_name, 3).await?;

    let recovered_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovery_bridge = DbspBridge::new(harness.db.clone()).await?;
    recover_materialized_views(&manifest, &recovered_registry, &mut recovery_bridge).await?;

    let mut recovered_rows =
        rows_at_version(&recovered_registry, &harness.view_name, mv_entry.version).await?;
    recovered_rows.sort();
    assert_eq!(recovered_rows, expected_rows);

    let outer_entry = manifest
        .outer_streams
        .iter()
        .find(|entry| entry.source == BID_SOURCE_NAME)
        .expect("checkpoint manifest entry for source");
    let mut outer_bridge = DbspBridge::new(harness.db.clone()).await?;
    let outer_registry =
        OuterStreamRegistry::from_sources(vec![BID_SOURCE_NAME.to_string()], &mut outer_bridge)
            .await?;
    let mut handle_stream = outer_registry
        .handle_stream(BID_SOURCE_NAME)
        .expect("handle stream for bid source");
    let handle = handle_stream.get(outer_entry.frontier).await?;
    assert_eq!(handle.version, outer_entry.version);

    Ok(())
}
