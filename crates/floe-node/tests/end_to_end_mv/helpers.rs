use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::scalar::ScalarValue;
use floe_executor::{DbspBridge, MaterializedViewRegistry, OuterStreamRegistry};
use floe_executor::outer_stream::OuterStreamHandle;
use floe_node::generator::{AUCTION_SOURCE_NAME, BID_SOURCE_NAME};
use tokio::time::{Duration, timeout};

use crate::rows::{auction_row, bid_row};

pub(crate) async fn assert_manifest_exists(
    table: Arc<dyn dbsp::storage::KeyValueTable>,
    namespace: &str,
    version: u64,
) -> Result<()> {
    let mut key = format!("zset/{namespace}/manifest/").into_bytes();
    key.extend_from_slice(&version.to_be_bytes());
    let exists = table
        .get(&key)
        .await
        .context("lookup manifest key")?
        .is_some();
    anyhow::ensure!(
        exists,
        "manifest {version} missing for namespace {namespace}"
    );
    Ok(())
}

pub(crate) async fn append_bid(
    outer: &mut OuterStreamRegistry,
    bridge: &mut DbspBridge,
    auction: i64,
    bidder: i64,
    price: i64,
) -> Result<OuterStreamHandle> {
    append_row(outer, bridge, BID_SOURCE_NAME, bid_row(auction, bidder, price)).await
}

pub(crate) async fn append_auction(
    outer: &mut OuterStreamRegistry,
    bridge: &mut DbspBridge,
    auction: i64,
    seller: i64,
    category: i64,
    expires_ms: i64,
    item_name: &str,
) -> Result<OuterStreamHandle> {
    append_row(
        outer,
        bridge,
        AUCTION_SOURCE_NAME,
        auction_row(auction, seller, category, expires_ms, item_name),
    )
    .await
}

pub(crate) async fn append_row(
    outer: &mut OuterStreamRegistry,
    bridge: &mut DbspBridge,
    source: &str,
    row: Vec<ScalarValue>,
) -> Result<OuterStreamHandle> {
    let writer = outer
        .writer_mut(source)
        .with_context(|| format!("{source} source writer must exist"))?;
    writer.append(&row, 1)?;
    let handles = outer.tick_all().await?;
    let handle = handles
        .into_iter()
        .find(|h| h.source == source)
        .with_context(|| format!("{source} handle present after tick"))?;
    // Ensure the manifest is readable to catch retention or persistence issues early.
    let _ = bridge
        .handle_view_for(&handle.namespace, handle.version)
        .await
        .with_context(|| format!("load handle view after {source} append"))?;
    Ok(handle)
}

pub(crate) async fn wait_for_version(
    registry: &MaterializedViewRegistry,
    view: &str,
    target_version: i64,
) -> Result<()> {
    let handle = registry
        .get(view)
        .with_context(|| format!("materialized view handle for '{view}'"))?;
    let mut rx = handle.version_watch();
    let mut observed = *rx.borrow();
    if observed.unwrap_or(-1) >= target_version {
        return Ok(());
    }
    timeout(Duration::from_secs(5), async {
        loop {
            rx.changed().await.context("version watch closed")?;
            observed = *rx.borrow();
            if observed.unwrap_or(-1) >= target_version {
                break;
            }
        }
        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("timeout waiting for mv version")??;
    Ok(())
}
