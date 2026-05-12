use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
#[cfg(test)]
use dbsp::circuit::tables::{
    nexmark_auction_alias_table, nexmark_auction_table, nexmark_bid_alias_table, nexmark_bid_table,
    nexmark_person_alias_table, nexmark_person_table,
};
use dbsp::storage::dictionary::Dictionary;
use dbsp::storage::{KeyValueTable, SlateTable};
use dbsp::stream::{DeltaHandleStream, SnapshotHandleStream};
use dbsp::{StreamRetention, ZSetStream};
use nexmark::event::{Auction, Bid, Event, Person};
use slatedb::Db;

use crate::namespaces;

/// Holds the base ZSet streams for Nexmark tables.
pub struct DbspTableEnvironment {
    /// Stream backing `nexmark_person`.
    pub person: ZSetStream<Vec<u8>>,
    /// Stream backing `nexmark_auction`.
    pub auction: ZSetStream<Vec<u8>>,
    /// Stream backing `nexmark_bid`.
    pub bid: ZSetStream<Vec<u8>>,
    table: Arc<dyn KeyValueTable>,
}

impl DbspTableEnvironment {
    /// Creates a table environment backed by the provided SlateDB instance.
    pub async fn new(db: Arc<Db>) -> Result<Self> {
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        Self::with_table(table).await
    }

    /// Creates a table environment backed by the supplied [`KeyValueTable`].
    pub async fn with_table(table: Arc<dyn KeyValueTable>) -> Result<Self> {
        let person = build_stream(
            table.clone(),
            namespaces::source("nexmark_person")?.as_str(),
        )
        .await
        .context("create ZSet stream for nexmark_person")?;
        let auction = build_stream(
            table.clone(),
            namespaces::source("nexmark_auction")?.as_str(),
        )
        .await
        .context("create ZSet stream for nexmark_auction")?;
        let bid = build_stream(table.clone(), namespaces::source("nexmark_bid")?.as_str())
            .await
            .context("create ZSet stream for nexmark_bid")?;

        Ok(Self {
            person,
            auction,
            bid,
            table,
        })
    }

    /// Applies a generated Nexmark event to the corresponding base stream.
    pub fn ingest_event(&mut self, event: &Event) -> Result<()> {
        match event {
            Event::Person(person) => self.ingest_person(person),
            Event::Auction(auction) => self.ingest_auction(auction),
            Event::Bid(bid) => self.ingest_bid(bid),
        }
    }

    pub fn ingest_person(&mut self, person: &Person) -> Result<()> {
        let key = encode_person_row(person)?;
        self.person.add_delta(key, 1);
        Ok(())
    }

    pub fn ingest_auction(&mut self, auction: &Auction) -> Result<()> {
        let key = encode_auction_row(auction)?;
        self.auction.add_delta(key, 1);
        Ok(())
    }

    pub fn ingest_bid(&mut self, bid: &Bid) -> Result<()> {
        let key = encode_bid_row(bid)?;
        self.bid.add_delta(key, 1);
        Ok(())
    }

    /// Flushes all base streams, persisting pending deltas.
    pub async fn flush_all(&mut self) -> Result<()> {
        self.person.flush().await?;
        self.auction.flush().await?;
        self.bid.flush().await?;
        Ok(())
    }

    /// Returns the handle stream backing the provided table descriptor.
    pub fn handle_stream_for(&self, table: &dbsp::TableDescriptor) -> Option<SnapshotHandleStream> {
        self.zset_for(table).map(|zset| zset.handle_stream())
    }

    pub fn delta_handle_stream_for(
        &self,
        table: &dbsp::TableDescriptor,
    ) -> Option<DeltaHandleStream> {
        self.zset_for(table).map(|zset| zset.delta_handle_stream())
    }

    /// Provides mutable access to the ZSet stream for a table descriptor.
    pub fn zset_mut_for(
        &mut self,
        table: &dbsp::TableDescriptor,
    ) -> Option<&mut ZSetStream<Vec<u8>>> {
        match table_kind(table)? {
            TableKind::Person => Some(&mut self.person),
            TableKind::Auction => Some(&mut self.auction),
            TableKind::Bid => Some(&mut self.bid),
        }
    }

    fn zset_for(&self, table: &dbsp::TableDescriptor) -> Option<&ZSetStream<Vec<u8>>> {
        match table_kind(table)? {
            TableKind::Person => Some(&self.person),
            TableKind::Auction => Some(&self.auction),
            TableKind::Bid => Some(&self.bid),
        }
    }

    pub fn table(&self) -> Arc<dyn KeyValueTable> {
        self.table.clone()
    }
}

#[derive(Clone, Copy, Debug)]
enum TableKind {
    Person,
    Auction,
    Bid,
}

fn table_kind(table: &dbsp::TableDescriptor) -> Option<TableKind> {
    match table.name {
        "nexmark_person" | "person" => Some(TableKind::Person),
        "nexmark_auction" | "auction" => Some(TableKind::Auction),
        "nexmark_bid" | "bid" => Some(TableKind::Bid),
        _ => None,
    }
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

fn encode_person_row(person: &Person) -> Result<Vec<u8>> {
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

pub fn encode_auction_row(auction: &Auction) -> Result<Vec<u8>> {
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

pub fn encode_bid_row(bid: &Bid) -> Result<Vec<u8>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::{EncodedRowScalar, decode_all_encoded_row_scalars};
    use object_store::memory::InMemory;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT_DB_ID: AtomicUsize = AtomicUsize::new(0);

    async fn test_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let id = NEXT_DB_ID.fetch_add(1, Ordering::Relaxed);
        Arc::new(
            Db::open(format!("dbsp-table-env-test-{id}"), store)
                .await
                .expect("open db"),
        )
    }

    fn sample_person() -> Person {
        Person {
            id: 1,
            name: "alice".to_string(),
            email_address: "alice@example.com".to_string(),
            credit_card: "1234".to_string(),
            city: "seattle".to_string(),
            state: "wa".to_string(),
            date_time: 1_700_000_000_000,
            extra: "extra".to_string(),
        }
    }

    fn sample_auction() -> Auction {
        Auction {
            id: 10,
            item_name: "item".to_string(),
            description: "desc".to_string(),
            initial_bid: 100,
            reserve: 200,
            date_time: 1_700_000_000_000,
            expires: 1_700_000_100_000,
            seller: 1,
            category: 5,
            extra: "auction-extra".to_string(),
        }
    }

    fn sample_bid() -> Bid {
        Bid {
            auction: 10,
            bidder: 2,
            price: 150,
            channel: "web".to_string(),
            url: "https://example".to_string(),
            date_time: 1_700_000_000_500,
            extra: "bid-extra".to_string(),
        }
    }

    #[test]
    fn table_kind_maps_nexmark_tables_and_aliases() {
        assert!(matches!(
            table_kind(nexmark_person_table()),
            Some(TableKind::Person)
        ));
        assert!(matches!(
            table_kind(nexmark_person_alias_table()),
            Some(TableKind::Person)
        ));
        assert!(matches!(
            table_kind(nexmark_auction_table()),
            Some(TableKind::Auction)
        ));
        assert!(matches!(
            table_kind(nexmark_auction_alias_table()),
            Some(TableKind::Auction)
        ));
        assert!(matches!(
            table_kind(nexmark_bid_table()),
            Some(TableKind::Bid)
        ));
        assert!(matches!(
            table_kind(nexmark_bid_alias_table()),
            Some(TableKind::Bid)
        ));
    }

    #[test]
    fn row_encoders_produce_expected_values() {
        let person_row = encode_person_row(&sample_person()).expect("encode person");
        let person_values = decode_all_encoded_row_scalars(&person_row).expect("decode person");
        assert_eq!(person_values.len(), 8);
        assert_eq!(person_values[0], Some(EncodedRowScalar::Int64(1)));

        let auction_row = encode_auction_row(&sample_auction()).expect("encode auction");
        let auction_values = decode_all_encoded_row_scalars(&auction_row).expect("decode auction");
        assert_eq!(auction_values.len(), 10);
        assert_eq!(auction_values[0], Some(EncodedRowScalar::Int64(10)));

        let bid_row = encode_bid_row(&sample_bid()).expect("encode bid");
        let bid_values = decode_all_encoded_row_scalars(&bid_row).expect("decode bid");
        assert_eq!(bid_values.len(), 7);
        assert_eq!(bid_values[0], Some(EncodedRowScalar::Int64(10)));
        assert_eq!(bid_values[1], Some(EncodedRowScalar::Int64(2)));
    }

    #[tokio::test]
    async fn environment_ingests_and_flushes_all_streams() {
        let db = test_db().await;
        let mut env = DbspTableEnvironment::new(db)
            .await
            .expect("build environment");

        env.ingest_event(&Event::Person(sample_person()))
            .expect("ingest person");
        env.ingest_event(&Event::Auction(sample_auction()))
            .expect("ingest auction");
        env.ingest_event(&Event::Bid(sample_bid()))
            .expect("ingest bid");
        env.flush_all().await.expect("flush all");

        let person_rows = env
            .person
            .latest_view()
            .materialize()
            .await
            .expect("person rows");
        let auction_rows = env
            .auction
            .latest_view()
            .materialize()
            .await
            .expect("auction rows");
        let bid_rows = env.bid.latest_view().materialize().await.expect("bid rows");

        assert_eq!(person_rows.len(), 1);
        assert_eq!(auction_rows.len(), 1);
        assert_eq!(bid_rows.len(), 1);
        assert!(env.handle_stream_for(nexmark_bid_table()).is_some());
        assert!(
            env.delta_handle_stream_for(nexmark_bid_alias_table())
                .is_some()
        );
        assert!(env.zset_mut_for(nexmark_person_alias_table()).is_some());
    }

    #[test]
    fn range_conversions_reject_overflow() {
        let i64_overflow = as_i64(usize::MAX, "value").unwrap_err();
        assert!(format!("{i64_overflow:#}").contains("exceeds i64 range"));

        let ts_overflow = ts_to_i64(u64::MAX).unwrap_err();
        assert!(format!("{ts_overflow:#}").contains("exceeds i64 range"));
    }
}
