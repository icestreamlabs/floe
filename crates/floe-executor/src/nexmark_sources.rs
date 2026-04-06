use std::sync::Arc;

use crate::namespaces;
use anyhow::{Context, Result, anyhow};
use dbsp::storage::KeyValueTable;
use dbsp::storage::dictionary::Dictionary;
use dbsp::{StreamRetention, ZSetStream};
use nexmark::event::{Auction, Bid, Person};

pub async fn nexmark_person_stream(table: Arc<dyn KeyValueTable>) -> Result<ZSetStream<Vec<u8>>> {
    build_stream(table, namespaces::source("nexmark_person")?.as_str()).await
}

pub async fn nexmark_auction_stream(table: Arc<dyn KeyValueTable>) -> Result<ZSetStream<Vec<u8>>> {
    build_stream(table, namespaces::source("nexmark_auction")?.as_str()).await
}

pub async fn nexmark_bid_stream(table: Arc<dyn KeyValueTable>) -> Result<ZSetStream<Vec<u8>>> {
    build_stream(table, namespaces::source("nexmark_bid")?.as_str()).await
}

pub fn encode_person_key(person: &Person) -> Result<Vec<u8>> {
    let mut encoded = begin_encoded_row(8)?;
    encode_i64_field(&mut encoded, person.id as i64);
    encode_utf8_field(&mut encoded, &person.name)?;
    encode_utf8_field(&mut encoded, &person.email_address)?;
    encode_utf8_field(&mut encoded, &person.credit_card)?;
    encode_utf8_field(&mut encoded, &person.city)?;
    encode_utf8_field(&mut encoded, &person.state)?;
    encode_timestamp_field(&mut encoded, ts_to_i64(person.date_time)?);
    encode_utf8_field(&mut encoded, &person.extra)?;
    Ok(encoded)
}

pub fn encode_auction_key(auction: &Auction) -> Result<Vec<u8>> {
    let mut encoded = begin_encoded_row(10)?;
    encode_i64_field(&mut encoded, auction.id as i64);
    encode_utf8_field(&mut encoded, &auction.item_name)?;
    encode_utf8_field(&mut encoded, &auction.description)?;
    encode_i64_field(&mut encoded, as_i64(auction.initial_bid, "initial_bid")?);
    encode_i64_field(&mut encoded, as_i64(auction.reserve, "reserve")?);
    encode_i64_field(&mut encoded, auction.seller as i64);
    encode_i64_field(&mut encoded, auction.category as i64);
    encode_timestamp_field(&mut encoded, ts_to_i64(auction.expires)?);
    encode_timestamp_field(&mut encoded, ts_to_i64(auction.date_time)?);
    encode_utf8_field(&mut encoded, &auction.extra)?;
    Ok(encoded)
}

pub fn encode_bid_key(bid: &Bid) -> Result<Vec<u8>> {
    let mut encoded = begin_encoded_row(7)?;
    encode_i64_field(&mut encoded, bid.auction as i64);
    encode_i64_field(&mut encoded, bid.bidder as i64);
    encode_i64_field(&mut encoded, as_i64(bid.price, "price")?);
    encode_utf8_field(&mut encoded, &bid.channel)?;
    encode_utf8_field(&mut encoded, &bid.url)?;
    encode_timestamp_field(&mut encoded, ts_to_i64(bid.date_time)?);
    encode_utf8_field(&mut encoded, &bid.extra)?;
    Ok(encoded)
}

fn as_i64(value: usize, label: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("{label} value {value} exceeds i64 range"))
}

fn ts_to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("timestamp {value} exceeds i64 range"))
}

fn begin_encoded_row(column_count: usize) -> Result<Vec<u8>> {
    let mut encoded = Vec::with_capacity(4 + column_count.saturating_mul(16));
    let count = u32::try_from(column_count).context("too many columns to encode")?;
    encoded.extend_from_slice(&count.to_le_bytes());
    Ok(encoded)
}

fn encode_i64_field(encoded: &mut Vec<u8>, value: i64) {
    encoded.push(0x01);
    encoded.extend_from_slice(&value.to_le_bytes());
}

fn encode_timestamp_field(encoded: &mut Vec<u8>, value: i64) {
    encoded.push(0x03);
    encoded.extend_from_slice(&value.to_le_bytes());
}

fn encode_utf8_field(encoded: &mut Vec<u8>, value: &str) -> Result<()> {
    encoded.push(0x02);
    let bytes = value.as_bytes();
    let len = u32::try_from(bytes.len()).context("utf8 value too large for encoded row")?;
    encoded.extend_from_slice(&len.to_le_bytes());
    encoded.extend_from_slice(bytes);
    Ok(())
}

async fn build_stream(
    table: Arc<dyn KeyValueTable>,
    namespace: &str,
) -> Result<ZSetStream<Vec<u8>>> {
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
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        Arc::new(
            Db::open("nexmark_source", store)
                .await
                .expect("open SlateDB"),
        )
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
        let materialized = materialize_zset_handle::<Vec<u8>>(table.clone(), &mut cache, &handle)
            .await
            .expect("materialize bid handle");
        assert_eq!(materialized.get(&key), Some(&1));
    }
}
