use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use floe_executor::checkpoint::{CheckpointManager, recover_materialized_views};
use floe_executor::{
    DbspBridge, FloeQueryContext, MaterializedViewRegistry, OuterStreamRegistry,
    load_or_register_mv,
};
use floe_node::generator::BID_SOURCE_NAME;
use floe_storage::SlateCatalog;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;

use crate::harness::MvTestHarness;
use crate::helpers::{append_bid, wait_for_materialized_row_count, wait_for_version};
use crate::rows::int_rows;

static NEXT_RESTART_CATALOG_ID: AtomicU64 = AtomicU64::new(1);

async fn isolated_catalog() -> Result<Arc<SlateCatalog>> {
    let id = NEXT_RESTART_CATALOG_ID.fetch_add(1, Ordering::Relaxed);
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    Ok(Arc::new(
        SlateCatalog::with_object_store(format!("restart-test-{id}"), object_store).await?,
    ))
}

#[tokio::test]
#[serial_test::serial]
async fn sql_restart_reloads_overlay_only_view_without_persisted_rows() -> Result<()> {
    let catalog = isolated_catalog().await?;
    let mut harness = MvTestHarness::new_with_catalog(
        Arc::clone(&catalog),
        "mv_restart",
        "CREATE MATERIALIZED VIEW mv_restart AS \
         SELECT auction, bidder, SUM(price) AS price \
         FROM nexmark_bid WHERE price > 0 GROUP BY auction, bidder",
    )
    .await?;

    append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 1, 7, 50).await?;
    append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 2, 8, 60).await?;
    wait_for_version(&harness.mv_registry, &harness.view_name, 2).await?;
    wait_for_materialized_row_count(&harness.mv_registry, &harness.view_name, 2).await?;

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
    assert!(
        recovered_rows.is_empty(),
        "overlay-only materialized views are not reconstructed from persisted MV state alone"
    );

    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn checkpoint_snapshot_skips_overlay_only_materialized_view_state() -> Result<()> {
    let catalog = isolated_catalog().await?;
    let mut harness = MvTestHarness::new_with_catalog(
        Arc::clone(&catalog),
        "mv_checkpoint_restart",
        "CREATE MATERIALIZED VIEW mv_checkpoint_restart AS \
         SELECT auction, bidder, SUM(price) AS price \
         FROM nexmark_bid WHERE price > 0 GROUP BY auction, bidder",
    )
    .await?;

    append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 1, 7, 50).await?;
    append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 2, 8, 60).await?;
    wait_for_version(&harness.mv_registry, &harness.view_name, 2).await?;
    wait_for_materialized_row_count(&harness.mv_registry, &harness.view_name, 2).await?;

    let mut checkpoint_manager =
        CheckpointManager::new("mv_checkpoint_restart", harness.ingestion_bridge.table()).await?;
    let manifest = checkpoint_manager
        .persist_snapshot(0, &harness.mv_registry, &harness.outer)
        .await?;
    let mv_entry = manifest
        .materialized_views
        .iter()
        .find(|entry| entry.view == harness.view_name);
    assert!(
        mv_entry.is_none(),
        "overlay-only views should not emit persisted MV checkpoint entries"
    );

    append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 3, 9, 70).await?;
    wait_for_version(&harness.mv_registry, &harness.view_name, 3).await?;

    let recovered_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovery_bridge = DbspBridge::new(harness.db.clone()).await?;
    recover_materialized_views(&manifest, &recovered_registry, &mut recovery_bridge).await?;
    assert!(
        recovered_registry.get(&harness.view_name).is_none(),
        "no MV checkpoint entries means recovery should not hydrate MV registry directly"
    );

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

#[tokio::test]
#[serial_test::serial]
async fn start_run_shutdown_restart_processes_new_ticks() -> Result<()> {
    let catalog = isolated_catalog().await?;
    let view_sql = "CREATE MATERIALIZED VIEW mv_lifecycle AS \
         SELECT auction, bidder, SUM(price) AS price \
         FROM nexmark_bid WHERE price > 0 GROUP BY auction, bidder";

    {
        let mut first =
            MvTestHarness::new_with_catalog(Arc::clone(&catalog), "mv_lifecycle", view_sql).await?;
        append_bid(&mut first.outer, &mut first.ingestion_bridge, 1, 10, 100).await?;
        wait_for_version(&first.mv_registry, &first.view_name, 1).await?;
        wait_for_materialized_row_count(&first.mv_registry, &first.view_name, 1).await?;
    }

    let mut restarted =
        MvTestHarness::new_with_catalog(Arc::clone(&catalog), "mv_lifecycle", view_sql).await?;
    append_bid(
        &mut restarted.outer,
        &mut restarted.ingestion_bridge,
        2,
        11,
        200,
    )
    .await?;
    wait_for_version(&restarted.mv_registry, &restarted.view_name, 2).await?;

    let (session, _) = restarted.session_with_view().await?;
    let df = session
        .sql("SELECT auction, bidder, price FROM mv_lifecycle ORDER BY auction")
        .await?;
    let batches = df.collect().await?;
    let rows = int_rows(&batches);
    assert!(
        rows.iter().any(|row| row == &vec![2, 11, 200]),
        "expected restarted run to process new tick row, got {rows:?}"
    );

    Ok(())
}
