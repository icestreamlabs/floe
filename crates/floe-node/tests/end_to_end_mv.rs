use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;
use dbsp::Stream;
use dbsp::handles::ZSetHandle;
use floe_executor::{
    BuildInputs, DbspBridge, DbspGraphBuilder, FloeQueryContext, MaterializedViewRegistry,
    OuterStreamRegistry, ValidatedPlan, load_or_register_mv, validate_dbsp_plan,
};
use floe_node::executor::{available_sources_from_registry, build_dataflows};
use floe_node::generator::{self, BID_SOURCE_NAME};
use floe_node::planner::plan_materialized_views;
use floe_node::source::SourceRegistry;
use floe_sql_parser::parse_materialized_view;
use floe_storage::SlateCatalog;

#[tokio::test]
async fn materialized_view_ingests_and_queries() -> Result<()> {
    let catalog = Arc::new(SlateCatalog::in_memory().await?);
    let db = catalog.db();

    let mut registry = SourceRegistry::new();
    registry.extend(generator::definitions()?);
    let available_sources = available_sources_from_registry(&registry);

    let definition = parse_materialized_view(
        "CREATE MATERIALIZED VIEW mv_q1 AS \
         SELECT auction, bidder, price * 2 AS price \
         FROM nexmark_bid WHERE bidder = 42",
    )?;
    let planned = plan_materialized_views(&registry, &[definition]).await?;
    assert_eq!(planned.len(), 1);
    let circuit_plans = build_dataflows(&planned, &available_sources)?;
    assert_eq!(circuit_plans.len(), 1);

    let ValidatedPlan {
        required_sources, ..
    } = validate_dbsp_plan(&circuit_plans[0], &available_sources, "mv_q1")?;

    let mut ingestion_bridge = DbspBridge::new(Arc::clone(&db)).await?;
    let mut outer =
        OuterStreamRegistry::from_validated_sources(&required_sources, &mut ingestion_bridge)
            .await?;
    append_bid(&mut outer, 1, 42, 100).await?;
    append_bid(&mut outer, 2, 10, 50).await?;
    append_bid(&mut outer, 3, 42, 75).await?;

    let mv_registry = Arc::new(MaterializedViewRegistry::new());
    let mut graph_builder = DbspGraphBuilder::new(Arc::clone(&db)).await?;
    let source_refs: Vec<&str> = required_sources.iter().map(|s| s.as_str()).collect();
    let handle_streams = gather_handle_streams(&outer, &source_refs);
    let _outputs = graph_builder
        .build(BuildInputs {
            graph_id: "mv_q1",
            view_name: "mv_q1",
            plan: &circuit_plans[0],
            mv_registry: Arc::clone(&mv_registry),
            outer_handle_streams: &handle_streams,
        })
        .await?;

    let query = FloeQueryContext::new(Arc::clone(&catalog));
    let session = query.session();
    let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
    load_or_register_mv(&session, Arc::clone(&mv_registry), &mut bridge, "mv_q1").await?;

    let df = session
        .sql("SELECT auction, bidder, price FROM mv_q1 ORDER BY auction")
        .await?;
    let batches = df.collect().await?;
    let rows = int_rows(&batches);
    assert_eq!(rows, vec![vec![1, 42, 200], vec![3, 42, 150]]);

    Ok(())
}

async fn append_bid(
    outer: &mut OuterStreamRegistry,
    auction: i64,
    bidder: i64,
    price: i64,
) -> Result<()> {
    let writer = outer
        .writer_mut(BID_SOURCE_NAME)
        .expect("bid source writer must exist");
    writer.append(&bid_row(auction, bidder, price), 1)?;
    writer.flush().await?;
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
