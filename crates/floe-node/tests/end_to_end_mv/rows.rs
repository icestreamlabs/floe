use datafusion::arrow::array::{Int64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;

pub(crate) fn bid_row(auction: i64, bidder: i64, price: i64) -> Vec<ScalarValue> {
    bid_row_nullable(Some(auction), Some(bidder), price)
}

pub(crate) fn bid_row_nullable(
    auction: Option<i64>,
    bidder: Option<i64>,
    price: i64,
) -> Vec<ScalarValue> {
    vec![
        ScalarValue::Int64(auction),
        ScalarValue::Int64(bidder),
        ScalarValue::Int64(Some(price)),
        ScalarValue::Utf8(Some("channel".to_string())),
        ScalarValue::Utf8(Some("http://example.com".to_string())),
        ScalarValue::TimestampMillisecond(Some(1_600_000_000), None),
        ScalarValue::Utf8(Some("extra".to_string())),
    ]
}

pub(crate) fn auction_row(
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

pub(crate) fn int_rows(batches: &[RecordBatch]) -> Vec<Vec<i64>> {
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

pub(crate) fn int_rows2(batches: &[RecordBatch]) -> Vec<Vec<i64>> {
    let mut rows = Vec::new();
    for batch in batches {
        let first = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("first column");
        let second = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("second column");
        for idx in 0..batch.num_rows() {
            rows.push(vec![first.value(idx), second.value(idx)]);
        }
    }
    rows
}

pub(crate) fn int_rows_n(batches: &[RecordBatch], columns: usize) -> Vec<Vec<i64>> {
    let mut rows = Vec::new();
    for batch in batches {
        let arrays = (0..columns)
            .map(|idx| {
                batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 column")
            })
            .collect::<Vec<_>>();
        for row_idx in 0..batch.num_rows() {
            rows.push(arrays.iter().map(|array| array.value(row_idx)).collect());
        }
    }
    rows
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BidAuctionRow {
    pub(crate) bidder: i64,
    pub(crate) price: i64,
    pub(crate) auction: i64,
    pub(crate) seller: i64,
    pub(crate) category: i64,
    pub(crate) expires_ms: i64,
    pub(crate) item_name: String,
}

pub(crate) fn bid_auction_rows(batches: &[RecordBatch]) -> Vec<BidAuctionRow> {
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
