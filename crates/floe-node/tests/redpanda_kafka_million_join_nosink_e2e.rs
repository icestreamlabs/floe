#[path = "support/kafka_million.rs"]
mod kafka_million;

use anyhow::Result;
use kafka_million::{
    AuctionInput, BID_ROW_COUNT, BidInput, ExpectedRow, FieldSpec, JOIN_AUCTION_ROW_COUNT,
    MillionDatasetKind, MillionQuerySpec, NoSinkVerifyMode, SampleSelection, int64,
    run_redpanda_kafka_million_no_sink_test,
    run_redpanda_kafka_million_no_sink_test_with_verify_mode,
};

const MV_SQL: &str = r#"
CREATE MATERIALIZED VIEW mv_kafka_redpanda_million_join_nosink AS
SELECT
  b.auction,
  b.bidder,
  b.price AS projected_price,
  a.seller
FROM nexmark_bid AS b
JOIN nexmark_auction AS a ON b.auction = a.id
WHERE a.category = 10
"#;

const OUTPUT_FIELDS: &[FieldSpec] = &[
    FieldSpec::int64("auction"),
    FieldSpec::int64("bidder"),
    FieldSpec::int64("projected_price"),
    FieldSpec::int64("seller"),
];

const SPEC: MillionQuerySpec = MillionQuerySpec {
    mv_name: "mv_kafka_redpanda_million_join_nosink",
    mv_sql: MV_SQL,
    output_fields: OUTPUT_FIELDS,
    input_row_count: BID_ROW_COUNT + JOIN_AUCTION_ROW_COUNT,
    dataset: MillionDatasetKind::BidAuctionJoin {
        auction_rows: JOIN_AUCTION_ROW_COUNT,
        project: project_join_row,
    },
    sample_selection: SampleSelection::FirstN(20),
    sample_match_field: "bidder",
};

#[tokio::test]
#[ignore = "requires Kafka/Redpanda and processes a >1,000,000-row join dataset"]
async fn redpanda_kafka_million_join_nosink_row_e2e() -> Result<()> {
    run_redpanda_kafka_million_no_sink_test(SPEC).await
}

fn project_join_row(bid: &BidInput, auction: &AuctionInput) -> Option<ExpectedRow> {
    if auction.category != 10 {
        return None;
    }

    Some(ExpectedRow::new(vec![
        int64(bid.auction),
        int64(bid.bidder),
        int64(bid.price),
        int64(auction.seller),
    ]))
}

#[tokio::test]
#[ignore = "requires Kafka/Redpanda and processes a >1,000,000-row join dataset"]
async fn redpanda_kafka_million_join_nosink_endverify_row_e2e() -> Result<()> {
    run_redpanda_kafka_million_no_sink_test_with_verify_mode(SPEC, NoSinkVerifyMode::CountAtEndOnly)
        .await
}
