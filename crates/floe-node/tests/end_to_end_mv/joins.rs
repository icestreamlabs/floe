use anyhow::Result;

use crate::harness::MvTestHarness;
use crate::helpers::{append_auction, append_bid, assert_manifest_exists, wait_for_version};
use crate::rows::{BidAuctionRow, bid_auction_rows};

#[tokio::test]
async fn materialized_view_joins_auctions() -> Result<()> {
    let mut harness = MvTestHarness::new(
        "mv_nexmark_bid_auctions",
        "CREATE MATERIALIZED VIEW mv_nexmark_bid_auctions AS \
         SELECT b.bidder, b.price, b.auction, a.seller, a.category, a.expires, a.item_name \
         FROM nexmark_bid AS b JOIN nexmark_auction AS a ON b.auction = a.id WHERE b.price > 0",
    )
    .await?;

    let handles = vec![
        append_auction(
            &mut harness.outer,
            &mut harness.ingestion_bridge,
            1,
            100,
            5,
            1_600_010_000,
            "chair",
        )
        .await?,
        append_auction(
            &mut harness.outer,
            &mut harness.ingestion_bridge,
            2,
            101,
            6,
            1_600_020_000,
            "table",
        )
        .await?,
        append_auction(
            &mut harness.outer,
            &mut harness.ingestion_bridge,
            3,
            102,
            7,
            1_600_030_000,
            "lamp",
        )
        .await?,
        append_auction(
            &mut harness.outer,
            &mut harness.ingestion_bridge,
            4,
            103,
            8,
            1_600_040_000,
            "sofa",
        )
        .await?,
        append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 1, 7, 50).await?,
        append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 2, 8, 60).await?,
        append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 3, 9, 70).await?,
        append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 1, 10, 0).await?,
        append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 4, 11, 90).await?,
        append_bid(
            &mut harness.outer,
            &mut harness.ingestion_bridge,
            3,
            12,
            130,
        )
        .await?,
        append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 2, 13, 0).await?,
        append_bid(
            &mut harness.outer,
            &mut harness.ingestion_bridge,
            2,
            14,
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
    let expected_rows = vec![
        BidAuctionRow {
            bidder: 7,
            price: 50,
            auction: 1,
            seller: 100,
            category: 5,
            expires_ms: 1_600_010_000,
            item_name: "chair".to_string(),
        },
        BidAuctionRow {
            bidder: 8,
            price: 60,
            auction: 2,
            seller: 101,
            category: 6,
            expires_ms: 1_600_020_000,
            item_name: "table".to_string(),
        },
        BidAuctionRow {
            bidder: 14,
            price: 200,
            auction: 2,
            seller: 101,
            category: 6,
            expires_ms: 1_600_020_000,
            item_name: "table".to_string(),
        },
        BidAuctionRow {
            bidder: 9,
            price: 70,
            auction: 3,
            seller: 102,
            category: 7,
            expires_ms: 1_600_030_000,
            item_name: "lamp".to_string(),
        },
        BidAuctionRow {
            bidder: 12,
            price: 130,
            auction: 3,
            seller: 102,
            category: 7,
            expires_ms: 1_600_030_000,
            item_name: "lamp".to_string(),
        },
        BidAuctionRow {
            bidder: 11,
            price: 90,
            auction: 4,
            seller: 103,
            category: 8,
            expires_ms: 1_600_040_000,
            item_name: "sofa".to_string(),
        },
    ];
    let target_version = handles.last().expect("latest handle").version as i64;
    wait_for_version(&harness.mv_registry, &harness.view_name, target_version).await?;

    let (session, _bridge) = harness.session_with_view().await?;
    let df = session
        .sql(
            "SELECT bidder, price, auction, seller, category, expires, item_name \
             FROM mv_nexmark_bid_auctions ORDER BY auction, bidder",
        )
        .await?;
    let batches = df.collect().await?;
    let rows = bid_auction_rows(&batches);
    assert_eq!(rows, expected_rows);

    Ok(())
}
