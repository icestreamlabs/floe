use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use datafusion::scalar::ScalarValue;
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

use crate::encoding::encode_projected_row_key;
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

    pub fn ingest_row(&mut self, table_name: &str, row: &[ScalarValue]) -> Result<()> {
        let key = encode_projected_row_key(row)?;
        match table_name {
            "nexmark_person" | "person" => {
                self.person.add_delta(key, 1);
                Ok(())
            }
            "nexmark_auction" | "auction" => {
                self.auction.add_delta(key, 1);
                Ok(())
            }
            "nexmark_bid" | "bid" => {
                self.bid.add_delta(key, 1);
                Ok(())
            }
            other => Err(anyhow!("unknown table '{other}' for ingestion")),
        }
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
    let ptr = table as *const dbsp::TableDescriptor;
    if std::ptr::eq(ptr, nexmark_person_table()) || std::ptr::eq(ptr, nexmark_person_alias_table())
    {
        return Some(TableKind::Person);
    }
    if std::ptr::eq(ptr, nexmark_auction_table())
        || std::ptr::eq(ptr, nexmark_auction_alias_table())
    {
        return Some(TableKind::Auction);
    }
    if std::ptr::eq(ptr, nexmark_bid_table()) || std::ptr::eq(ptr, nexmark_bid_alias_table()) {
        return Some(TableKind::Bid);
    }
    None
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
