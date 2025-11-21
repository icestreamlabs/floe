use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use datafusion::scalar::ScalarValue;
use dbsp::storage::dictionary::Dictionary;
use dbsp::storage::KeyValueTable;
use dbsp::{StreamRetention, ZSetStream};
use nexmark::event::{Auction, Bid, Person};
use crate::encoding::encode_projected_row_key;
use crate::namespaces;

pub async fn nexmark_person_stream(
    table: Arc<dyn KeyValueTable>,
) -> Result<ZSetStream<Vec<u8>>> {
    build_stream(table, namespaces::source("nexmark_person")?.as_str()).await
}

pub async fn nexmark_auction_stream(
    table: Arc<dyn KeyValueTable>,
) -> Result<ZSetStream<Vec<u8>>> {
    build_stream(table, namespaces::source("nexmark_auction")?.as_str()).await
}

pub async fn nexmark_bid_stream(table: Arc<dyn KeyValueTable>) -> Result<ZSetStream<Vec<u8>>> {
    build_stream(table, namespaces::source("nexmark_bid")?.as_str()).await
}

pub fn encode_person_key(person: &Person) -> Result<Vec<u8>> {
    let row = vec![
        ScalarValue::Int64(Some(person.id as i64)),
        ScalarValue::Utf8(Some(person.name.clone())),
        ScalarValue::Utf8(Some(person.email_address.clone())),
        ScalarValue::Utf8(Some(person.credit_card.clone())),
        ScalarValue::Utf8(Some(person.city.clone())),
        ScalarValue::Utf8(Some(person.state.clone())),
        ScalarValue::TimestampMillisecond(Some(ts_to_i64(person.date_time)?), None),
        ScalarValue::Utf8(Some(person.extra.clone())),
    ];
    encode_projected_row_key(&row)
}

pub fn encode_auction_key(auction: &Auction) -> Result<Vec<u8>> {
    let row = vec![
        ScalarValue::Int64(Some(auction.id as i64)),
        ScalarValue::Utf8(Some(auction.item_name.clone())),
        ScalarValue::Utf8(Some(auction.description.clone())),
        ScalarValue::Int64(Some(as_i64(auction.initial_bid, "initial_bid")?)),
        ScalarValue::Int64(Some(as_i64(auction.reserve, "reserve")?)),
        ScalarValue::Int64(Some(auction.seller as i64)),
        ScalarValue::Int64(Some(auction.category as i64)),
        ScalarValue::TimestampMillisecond(Some(ts_to_i64(auction.expires)?), None),
        ScalarValue::TimestampMillisecond(Some(ts_to_i64(auction.date_time)?), None),
        ScalarValue::Utf8(Some(auction.extra.clone())),
    ];
    encode_projected_row_key(&row)
}

pub fn encode_bid_key(bid: &Bid) -> Result<Vec<u8>> {
    let row = vec![
        ScalarValue::Int64(Some(bid.auction as i64)),
        ScalarValue::Int64(Some(bid.bidder as i64)),
        ScalarValue::Int64(Some(as_i64(bid.price, "price")?)),
        ScalarValue::Utf8(Some(bid.channel.clone())),
        ScalarValue::Utf8(Some(bid.url.clone())),
        ScalarValue::TimestampMillisecond(Some(ts_to_i64(bid.date_time)?), None),
        ScalarValue::Utf8(Some(bid.extra.clone())),
    ];
    encode_projected_row_key(&row)
}

fn as_i64(value: usize, label: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("{label} value {value} exceeds i64 range"))
}

fn ts_to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("timestamp {value} exceeds i64 range"))
}

async fn build_stream(table: Arc<dyn KeyValueTable>, namespace: &str) -> Result<ZSetStream<Vec<u8>>> {
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), namespace.to_string(), None)
            .await
            .with_context(|| anyhow!("build dictionary for namespace '{namespace}'"))?,
    );
    ZSetStream::new(
        dict,
        table,
        namespace.to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .with_context(|| anyhow!("create ZSetStream for namespace '{namespace}'"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbsp::storage::SlateTable;
    use dbsp::stream::util::materialize_zset_handle;
    use slatedb::Db;
    use std::collections::HashMap;

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        Arc::new(Db::open("nexmark_source", store).await.expect("open SlateDB"))
    }

    #[tokio::test]
    async fn bid_stream_inserts_key() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        let mut stream = nexmark_bid_stream(table.clone())
            .await
            .expect("build bid stream");

        let bid = Bid {
            auction: 1,
            bidder: 2,
            price: 3,
            channel: "chan".to_string(),
            url: "url".to_string(),
            date_time: 4,
            extra: "x".to_string(),
        };
        let key = encode_bid_key(&bid).expect("encode bid");
        stream.add_delta(key.clone(), 1);
        let handle = stream.flush().await.expect("flush bid stream");

        let mut cache = HashMap::new();
        let materialized =
            materialize_zset_handle::<Vec<u8>>(table.clone(), &mut cache, &handle)
                .await
                .expect("materialize bid handle");
        assert_eq!(materialized.get(&key), Some(&1));
    }
}
