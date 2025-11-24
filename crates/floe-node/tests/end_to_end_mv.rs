use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::arrow::array::{Int64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;
use dbsp::Stream;
use dbsp::handles::ZSetHandleView;
use dbsp::handles::ZSetHandle;
use floe_executor::{
    BuildInputs, DbspBridge, DbspGraphBuilder, FloeQueryContext, MaterializedViewRegistry,
    OuterStreamRegistry, ValidatedPlan, load_or_register_mv, validate_dbsp_plan,
};
use floe_executor::outer_stream::OuterStreamHandle;
use floe_node::executor::{available_sources_from_registry, build_dataflows};
use floe_node::generator::{self, AUCTION_SOURCE_NAME, BID_SOURCE_NAME};
use floe_node::planner::plan_materialized_views;
use floe_node::source::SourceRegistry;
use floe_sql_parser::parse_materialized_view;
use floe_storage::SlateCatalog;
use tokio::time::{Duration, timeout};

struct MvTestHarness {
    catalog: Arc<SlateCatalog>,
    db: Arc<slatedb::Db>,
    mv_registry: Arc<MaterializedViewRegistry>,
    outer: OuterStreamRegistry,
    ingestion_bridge: DbspBridge,
    view_name: String,
}

impl MvTestHarness {
    async fn new(view_name: &str, view_sql: &str) -> Result<Self> {
        let catalog = Arc::new(SlateCatalog::in_memory().await?);
        let db = catalog.db();

        let mut registry = SourceRegistry::new();
        registry.extend(generator::definitions()?);
        let available_sources = available_sources_from_registry(&registry);

        let definition = parse_materialized_view(view_sql)?;
        let planned = plan_materialized_views(&registry, &[definition]).await?;
        assert_eq!(
            planned.len(),
            1,
            "expected a single planned materialized view"
        );
        let circuit_plans = build_dataflows(&planned, &available_sources)?;
        assert_eq!(
            circuit_plans.len(),
            1,
            "expected a single circuit plan for the view"
        );

        let ValidatedPlan { required_sources, .. } =
            validate_dbsp_plan(&circuit_plans[0], &available_sources, view_name)?;

        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        let mut graph_builder = DbspGraphBuilder::new(Arc::clone(&db)).await?;
        let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let outer =
            OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
                .await?;
        let source_refs: Vec<&str> = required_sources.iter().map(String::as_str).collect();
        let handle_streams = gather_handle_streams(&outer, &source_refs);
        graph_builder
            .build(BuildInputs {
                graph_id: view_name,
                view_name,
                plan: &circuit_plans[0],
                mv_registry: Arc::clone(&mv_registry),
                outer_handle_streams: &handle_streams,
            })
            .await?;

        Ok(Self {
            catalog,
            db,
            mv_registry,
            outer,
            ingestion_bridge,
            view_name: view_name.to_string(),
        })
    }

    async fn session_with_view(
        &self,
    ) -> Result<(datafusion::execution::context::SessionContext, DbspBridge)> {
        let query = FloeQueryContext::new(Arc::clone(&self.catalog));
        let session = query.session();
        let mut bridge = DbspBridge::new(Arc::clone(&self.db)).await?;
        load_or_register_mv(
            &session,
            Arc::clone(&self.mv_registry),
            &mut bridge,
            &self.view_name,
        )
        .await?;
        Ok((session, bridge))
    }
}

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
        append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 1, 42, 100).await?,
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
        append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 3, 12, 130).await?,
        append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 2, 13, 0).await?,
        append_bid(&mut harness.outer, &mut harness.ingestion_bridge, 2, 14, 200).await?,
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
    wait_for_version(
        &harness.mv_registry,
        &harness.view_name,
        expected_rows.len() as i64,
    )
    .await?;

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

async fn assert_manifest_exists(
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
    anyhow::ensure!(exists, "manifest {version} missing for namespace {namespace}");
    Ok(())
}

async fn append_bid(
    outer: &mut OuterStreamRegistry,
    bridge: &mut DbspBridge,
    auction: i64,
    bidder: i64,
    price: i64,
) -> Result<OuterStreamHandle> {
    append_row(outer, bridge, BID_SOURCE_NAME, bid_row(auction, bidder, price)).await
}

async fn append_auction(
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

async fn append_row(
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

async fn wait_for_version(
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

fn bid_row(auction: i64, bidder: i64, price: i64) -> Vec<ScalarValue> {
    vec![
        ScalarValue::Int64(Some(auction)),
        ScalarValue::Int64(Some(bidder)),
        ScalarValue::Int64(Some(price)),
        ScalarValue::Utf8(Some("channel".to_string())),
        ScalarValue::Utf8(Some("http://example.com".to_string())),
        ScalarValue::TimestampMillisecond(Some(1_600_000_000), None),
        ScalarValue::Utf8(Some("extra".to_string())),
    ]
}

fn auction_row(
    auction: i64,
    seller: i64,
    category: i64,
    expires_ms: i64,
    item_name: &str,
) -> Vec<ScalarValue> {
    vec![
        ScalarValue::Int64(Some(auction)),
        ScalarValue::Utf8(Some(item_name.to_string())),
        ScalarValue::Utf8(Some("description".to_string())),
        ScalarValue::Int64(Some(10)),
        ScalarValue::Int64(Some(15)),
        ScalarValue::Int64(Some(seller)),
        ScalarValue::Int64(Some(category)),
        ScalarValue::TimestampMillisecond(Some(expires_ms), None),
        ScalarValue::TimestampMillisecond(Some(expires_ms - 1), None),
        ScalarValue::Utf8(Some("extra".to_string())),
    ]
}

fn gather_handle_streams(
    outer: &OuterStreamRegistry,
    sources: &[&str],
) -> std::collections::HashMap<String, Stream<ZSetHandle>> {
    let mut map = std::collections::HashMap::new();
    for source in sources {
        if let Some(stream) = outer.handle_stream(source) {
            map.insert((*source).to_string(), stream);
        }
    }
    map
}

fn int_rows(batches: &[RecordBatch]) -> Vec<Vec<i64>> {
    let mut rows = Vec::new();
    for batch in batches {
        let auctions = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("auction column");
        let bidders = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("bidder column");
        let prices = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("price column");
        for idx in 0..batch.num_rows() {
            rows.push(vec![
                auctions.value(idx),
                bidders.value(idx),
                prices.value(idx),
            ]);
        }
    }
    rows
}

#[derive(Debug, PartialEq, Eq)]
struct BidAuctionRow {
    bidder: i64,
    price: i64,
    auction: i64,
    seller: i64,
    category: i64,
    expires_ms: i64,
    item_name: String,
}

fn bid_auction_rows(batches: &[RecordBatch]) -> Vec<BidAuctionRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let bidder = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("bidder column");
        let price = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("price column");
        let auction = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("auction column");
        let seller = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("seller column");
        let category = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("category column");
        let expires = batch
            .column(5)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("expires column");
        let item_name = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("item name column");
        for idx in 0..batch.num_rows() {
            rows.push(BidAuctionRow {
                bidder: bidder.value(idx),
                price: price.value(idx),
                auction: auction.value(idx),
                seller: seller.value(idx),
                category: category.value(idx),
                expires_ms: expires.value(idx),
                item_name: item_name.value(idx).to_string(),
            });
        }
    }
    rows
}
