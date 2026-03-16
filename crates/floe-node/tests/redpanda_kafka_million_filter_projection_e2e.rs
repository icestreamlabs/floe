#[path = "support/kafka_million.rs"]
mod kafka_million;

use anyhow::Result;
use kafka_million::{
    BID_ROW_COUNT, BidInput, ExpectedRow, FieldSpec, MillionDatasetKind, MillionQuerySpec,
    SampleSelection, int64, run_redpanda_kafka_million_test,
};

const MV_SQL: &str = r#"
CREATE MATERIALIZED VIEW mv_kafka_redpanda_million_filter_projection AS
SELECT
  auction,
  bidder,
  price AS projected_price
FROM nexmark_bid
WHERE auction <= 5000
"#;

const OUTPUT_FIELDS: &[FieldSpec] = &[
    FieldSpec::int64("auction"),
    FieldSpec::int64("bidder"),
    FieldSpec::int64("projected_price"),
];

const SPEC: MillionQuerySpec = MillionQuerySpec {
    mv_name: "mv_kafka_redpanda_million_filter_projection",
    mv_sql: MV_SQL,
    output_fields: OUTPUT_FIELDS,
    input_row_count: BID_ROW_COUNT,
    dataset: MillionDatasetKind::BidOnly {
        project: project_row,
    },
    sample_selection: SampleSelection::FirstN(20),
    sample_match_field: "bidder",
};

#[tokio::test]
#[ignore = "requires Kafka/Redpanda and processes a 1,000,000-row dataset"]
async fn redpanda_kafka_million_filter_projection_row_e2e() -> Result<()> {
    run_redpanda_kafka_million_test(SPEC).await
}

fn project_row(input: &BidInput) -> Option<ExpectedRow> {
    if input.auction > 5000 {
        return None;
    }

    Some(ExpectedRow::new(vec![
        int64(input.auction),
        int64(input.bidder),
        int64(input.price),
    ]))
}
