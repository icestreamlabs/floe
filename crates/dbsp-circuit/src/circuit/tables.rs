use std::sync::Arc;

use anyhow::anyhow;
use once_cell::sync::Lazy;

use crate::circuit::schema::{Field, PrimaryKey, RowSchema};
use crate::circuit::types::DbspScalarType;

#[derive(Debug, Clone)]
pub struct TableDescriptor {
    name: Arc<str>,
    source_name: Arc<str>,
    schema: Arc<RowSchema>,
    primary_key: PrimaryKey,
}

impl TableDescriptor {
    pub fn try_new(
        name: impl Into<String>,
        fields: Vec<Field>,
        primary_key_columns: &[&str],
    ) -> anyhow::Result<Self> {
        let name = name.into();
        Self::try_new_with_source(name.clone(), name, fields, primary_key_columns)
    }

    pub fn try_new_with_source(
        name: impl Into<String>,
        source_name: impl Into<String>,
        fields: Vec<Field>,
        primary_key_columns: &[&str],
    ) -> anyhow::Result<Self> {
        let name = Arc::<str>::from(name.into());
        let source_name = Arc::<str>::from(source_name.into());
        let schema = RowSchema::try_new(fields)?;
        let primary_key = PrimaryKey::new(schema.clone(), primary_key_columns)?;
        Ok(Self {
            source_name,
            name,
            schema,
            primary_key,
        })
    }

    pub fn try_new_dynamic(
        name: impl Into<String>,
        fields: Vec<Field>,
        primary_key_columns: &[String],
    ) -> anyhow::Result<Self> {
        let primary_key_columns = primary_key_columns
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        Self::try_new(name, fields, &primary_key_columns)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.schema.field_index(name)
    }

    pub fn schema(&self) -> &Arc<RowSchema> {
        &self.schema
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn primary_key(&self) -> &PrimaryKey {
        &self.primary_key
    }
}

static NEXMARK_PERSON_TABLE: Lazy<anyhow::Result<TableDescriptor>> = Lazy::new(|| {
    TableDescriptor::try_new(
        "nexmark_person",
        vec![
            Field::new("id", DbspScalarType::Int64, false),
            Field::new("name", DbspScalarType::Utf8, false),
            Field::new("email_address", DbspScalarType::Utf8, false),
            Field::new("credit_card", DbspScalarType::Utf8, false),
            Field::new("city", DbspScalarType::Utf8, false),
            Field::new("state", DbspScalarType::Utf8, false),
            Field::new("date_time", DbspScalarType::TimestampMillis, false),
            Field::new("extra", DbspScalarType::Utf8, false),
        ],
        &["id"],
    )
});

static NEXMARK_PERSON_ALIAS_TABLE: Lazy<anyhow::Result<TableDescriptor>> = Lazy::new(|| {
    TableDescriptor::try_new_with_source(
        "person",
        "nexmark_person",
        vec![
            Field::new("id", DbspScalarType::Int64, false),
            Field::new("name", DbspScalarType::Utf8, false),
            Field::new("emailAddress", DbspScalarType::Utf8, false),
            Field::new("creditCard", DbspScalarType::Utf8, false),
            Field::new("city", DbspScalarType::Utf8, false),
            Field::new("state", DbspScalarType::Utf8, false),
            Field::new("dateTime", DbspScalarType::TimestampMillis, false),
            Field::new("extra", DbspScalarType::Utf8, false),
        ],
        &["id"],
    )
});

static NEXMARK_AUCTION_TABLE: Lazy<anyhow::Result<TableDescriptor>> = Lazy::new(|| {
    TableDescriptor::try_new(
        "nexmark_auction",
        vec![
            Field::new("id", DbspScalarType::Int64, false),
            Field::new("item_name", DbspScalarType::Utf8, false),
            Field::new("description", DbspScalarType::Utf8, false),
            Field::new("initial_bid", DbspScalarType::Int64, false),
            Field::new("reserve", DbspScalarType::Int64, false),
            Field::new("seller", DbspScalarType::Int64, false),
            Field::new("category", DbspScalarType::Int64, false),
            Field::new("expires", DbspScalarType::TimestampMillis, false),
            Field::new("date_time", DbspScalarType::TimestampMillis, false),
            Field::new("extra", DbspScalarType::Utf8, false),
        ],
        &["id"],
    )
});

static NEXMARK_AUCTION_ALIAS_TABLE: Lazy<anyhow::Result<TableDescriptor>> = Lazy::new(|| {
    TableDescriptor::try_new_with_source(
        "auction",
        "nexmark_auction",
        vec![
            Field::new("id", DbspScalarType::Int64, false),
            Field::new("itemName", DbspScalarType::Utf8, false),
            Field::new("description", DbspScalarType::Utf8, false),
            Field::new("initialBid", DbspScalarType::Int64, false),
            Field::new("reserve", DbspScalarType::Int64, false),
            Field::new("seller", DbspScalarType::Int64, false),
            Field::new("category", DbspScalarType::Int64, false),
            Field::new("expires", DbspScalarType::TimestampMillis, false),
            Field::new("dateTime", DbspScalarType::TimestampMillis, false),
            Field::new("extra", DbspScalarType::Utf8, false),
        ],
        &["id"],
    )
});

static NEXMARK_BID_TABLE: Lazy<anyhow::Result<TableDescriptor>> = Lazy::new(|| {
    TableDescriptor::try_new(
        "nexmark_bid",
        vec![
            Field::new("auction", DbspScalarType::Int64, false),
            Field::new("bidder", DbspScalarType::Int64, false),
            Field::new("price", DbspScalarType::Int64, false),
            Field::new("channel", DbspScalarType::Utf8, false),
            Field::new("url", DbspScalarType::Utf8, false),
            Field::new("date_time", DbspScalarType::TimestampMillis, false),
            Field::new("extra", DbspScalarType::Utf8, false),
        ],
        &["auction", "bidder", "date_time", "price"],
    )
});

static NEXMARK_BID_ALIAS_TABLE: Lazy<anyhow::Result<TableDescriptor>> = Lazy::new(|| {
    TableDescriptor::try_new_with_source(
        "bid",
        "nexmark_bid",
        vec![
            Field::new("auction", DbspScalarType::Int64, false),
            Field::new("bidder", DbspScalarType::Int64, false),
            Field::new("price", DbspScalarType::Int64, false),
            Field::new("channel", DbspScalarType::Utf8, false),
            Field::new("url", DbspScalarType::Utf8, false),
            Field::new("dateTime", DbspScalarType::TimestampMillis, false),
            Field::new("extra", DbspScalarType::Utf8, false),
        ],
        &["auction", "bidder", "dateTime", "price"],
    )
});

fn table_ref(
    table: &'static Lazy<anyhow::Result<TableDescriptor>>,
    descriptor_name: &str,
) -> anyhow::Result<&'static TableDescriptor> {
    table
        .as_ref()
        .map_err(|error| anyhow!("failed to build {descriptor_name} descriptor: {error}"))
}

pub fn nexmark_person_table() -> anyhow::Result<&'static TableDescriptor> {
    table_ref(&NEXMARK_PERSON_TABLE, "nexmark_person")
}

pub fn nexmark_person_alias_table() -> anyhow::Result<&'static TableDescriptor> {
    table_ref(&NEXMARK_PERSON_ALIAS_TABLE, "person")
}

pub fn nexmark_auction_table() -> anyhow::Result<&'static TableDescriptor> {
    table_ref(&NEXMARK_AUCTION_TABLE, "nexmark_auction")
}

pub fn nexmark_auction_alias_table() -> anyhow::Result<&'static TableDescriptor> {
    table_ref(&NEXMARK_AUCTION_ALIAS_TABLE, "auction")
}

pub fn nexmark_bid_table() -> anyhow::Result<&'static TableDescriptor> {
    table_ref(&NEXMARK_BID_TABLE, "nexmark_bid")
}

pub fn nexmark_bid_alias_table() -> anyhow::Result<&'static TableDescriptor> {
    table_ref(&NEXMARK_BID_ALIAS_TABLE, "bid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_descriptors_are_available() -> anyhow::Result<()> {
        assert_eq!(nexmark_person_table()?.name(), "nexmark_person");
        assert_eq!(nexmark_person_alias_table()?.name(), "person");
        assert_eq!(
            nexmark_person_alias_table()?.source_name(),
            "nexmark_person"
        );
        assert_eq!(nexmark_auction_table()?.primary_key().columns(), &[0]);
        assert_eq!(nexmark_auction_alias_table()?.name(), "auction");
        assert_eq!(
            nexmark_auction_alias_table()?.source_name(),
            "nexmark_auction"
        );
        assert_eq!(nexmark_bid_table()?.primary_key().columns().len(), 4);
        assert_eq!(nexmark_bid_alias_table()?.name(), "bid");
        assert_eq!(nexmark_bid_alias_table()?.source_name(), "nexmark_bid");
        Ok(())
    }
}
