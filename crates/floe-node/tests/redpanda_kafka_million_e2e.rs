#[path = "support/kafka_million.rs"]
mod kafka_million;

use anyhow::Result;
use kafka_million::{
    BID_ROW_COUNT, BidInput, ExpectedRow, FieldSpec, MillionDatasetKind, MillionQuerySpec,
    SampleSelection, day_string, int64, run_redpanda_kafka_million_test, string,
};

const MV_SQL: &str = r#"
CREATE MATERIALIZED VIEW mv_kafka_redpanda_million AS
SELECT
  auction,
  bidder,
  price * 89 / 100 AS normalized_price,
  CASE
    WHEN lower(channel) = 'apple' THEN lower(channel)
    WHEN lower(channel) = 'google' THEN lower(channel)
    WHEN lower(channel) = 'facebook' THEN lower(channel)
    WHEN lower(channel) = 'baidu' THEN lower(channel)
    ELSE REGEXP_EXTRACT(channel, '(web)', 1)
  END AS channel_id,
  SPLIT_INDEX(url, '/', 3) AS dir1,
  DATE_FORMAT(date_time, 'yyyy-MM-dd') AS day
FROM nexmark_bid
WHERE price >= 0
"#;

const OUTPUT_FIELDS: &[FieldSpec] = &[
    FieldSpec::int64("auction"),
    FieldSpec::int64("bidder"),
    FieldSpec::int64("normalized_price"),
    FieldSpec::string("channel_id"),
    FieldSpec::string("dir1"),
    FieldSpec::string("day"),
];

const SPEC: MillionQuerySpec = MillionQuerySpec {
    mv_name: "mv_kafka_redpanda_million",
    mv_sql: MV_SQL,
    output_fields: OUTPUT_FIELDS,
    input_row_count: BID_ROW_COUNT,
    dataset: MillionDatasetKind::BidOnly {
        project: project_row,
    },
    sample_selection: SampleSelection::EvenlySpaced(20),
    sample_match_field: "bidder",
};

#[tokio::test]
#[ignore = "requires Kafka/Redpanda and processes a 1,000,000-row dataset"]
async fn redpanda_kafka_million_row_e2e() -> Result<()> {
    run_redpanda_kafka_million_test(SPEC).await
}

fn project_row(input: &BidInput) -> Option<ExpectedRow> {
    let channel_id = match input.channel {
        "apple" | "google" | "facebook" | "baidu" | "web" => input.channel.to_string(),
        _ => return None,
    };
    Some(ExpectedRow::new(vec![
        int64(input.auction),
        int64(input.bidder),
        int64(input.price * 89 / 100),
        string(channel_id),
        string(input.dir1.clone()),
        string(day_string()),
    ]))
}
