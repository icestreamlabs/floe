use std::time::Duration;

use anyhow::{Context, Result, anyhow, ensure};
use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition, SourceEvent};
use nexmark::EventGenerator;
use nexmark::config::NexmarkConfig;
use nexmark::event::Event;
use serde::Serialize;
use tokio::time::sleep;

use crate::source::SourceEventSender;

const CONNECTOR_NAME: &str = "nexmark";
const CONNECTOR_PROPERTY: &str = "connector";
const ENTITY_PROPERTY: &str = "entity";

pub const PERSON_SOURCE_NAME: &str = "nexmark_person";
pub const AUCTION_SOURCE_NAME: &str = "nexmark_auction";
pub const BID_SOURCE_NAME: &str = "nexmark_bid";

#[derive(Debug, Clone)]
pub struct Config {
    pub events_per_second: f64,
    pub max_events: Option<u64>,
}

pub fn definitions() -> Result<Vec<SourceDefinition>> {
    let person = SourceDefinition::new(PERSON_SOURCE_NAME, person_columns())?
        .with_property(CONNECTOR_PROPERTY, CONNECTOR_NAME)
        .with_property(ENTITY_PROPERTY, "person");
    let auction = SourceDefinition::new(AUCTION_SOURCE_NAME, auction_columns())?
        .with_property(CONNECTOR_PROPERTY, CONNECTOR_NAME)
        .with_property(ENTITY_PROPERTY, "auction");
    let bid = SourceDefinition::new(BID_SOURCE_NAME, bid_columns())?
        .with_property(CONNECTOR_PROPERTY, CONNECTOR_NAME)
        .with_property(ENTITY_PROPERTY, "bid");

    Ok(vec![person, auction, bid])
}

pub async fn run(config: Config, sender: SourceEventSender) -> Result<()> {
    ensure!(
        config.events_per_second.is_finite() && config.events_per_second > 0.0,
        "events-per-second must be a positive finite value"
    );

    if let Some(limit) = config.max_events
        && limit == 0
    {
        return Ok(());
    }

    let mut generator = EventGenerator::new(NexmarkConfig::default());
    let interval = Duration::from_secs_f64(1.0 / config.events_per_second);
    let mut emitted: u64 = 0;

    loop {
        let event = generator
            .next()
            .context("nexmark generator produced no event")?;
        forward_event(&sender, &event).await?;
        emitted = emitted.saturating_add(1);

        if let Some(limit) = config.max_events
            && emitted >= limit
        {
            break;
        }

        if !interval.is_zero() {
            sleep(interval).await;
        } else {
            tokio::task::yield_now().await;
        }
    }

    Ok(())
}

async fn forward_event(sender: &SourceEventSender, event: &Event) -> Result<()> {
    match event {
        Event::Person(person) => send_payload(sender, PERSON_SOURCE_NAME, person, "person").await,
        Event::Auction(auction) => {
            send_payload(sender, AUCTION_SOURCE_NAME, auction, "auction").await
        }
        Event::Bid(bid) => send_payload(sender, BID_SOURCE_NAME, bid, "bid").await,
    }
}

async fn send_payload<T>(
    sender: &SourceEventSender,
    source: &str,
    payload: &T,
    entity: &str,
) -> Result<()>
where
    T: Serialize,
{
    let json = serde_json::to_value(payload)
        .with_context(|| format!("failed to serialize {entity} event"))?;
    sender
        .send(SourceEvent::new(source, json))
        .await
        .map_err(|err| anyhow!("failed to enqueue event for source {source}: {err}"))?;
    Ok(())
}

fn person_columns() -> Vec<SourceColumn> {
    vec![
        SourceColumn::new("id", SourceDataType::Int64),
        SourceColumn::new("name", SourceDataType::Utf8),
        SourceColumn::new("email_address", SourceDataType::Utf8),
        SourceColumn::new("credit_card", SourceDataType::Utf8),
        SourceColumn::new("city", SourceDataType::Utf8),
        SourceColumn::new("state", SourceDataType::Utf8),
        SourceColumn::new("date_time", SourceDataType::TimestampMillis),
        SourceColumn::new("extra", SourceDataType::Utf8),
    ]
}

fn auction_columns() -> Vec<SourceColumn> {
    vec![
        SourceColumn::new("id", SourceDataType::Int64),
        SourceColumn::new("item_name", SourceDataType::Utf8),
        SourceColumn::new("description", SourceDataType::Utf8),
        SourceColumn::new("initial_bid", SourceDataType::Int64),
        SourceColumn::new("reserve", SourceDataType::Int64),
        SourceColumn::new("seller", SourceDataType::Int64),
        SourceColumn::new("category", SourceDataType::Int64),
        SourceColumn::new("expires", SourceDataType::TimestampMillis),
        SourceColumn::new("date_time", SourceDataType::TimestampMillis),
        SourceColumn::new("extra", SourceDataType::Utf8),
    ]
}

fn bid_columns() -> Vec<SourceColumn> {
    vec![
        SourceColumn::new("auction", SourceDataType::Int64),
        SourceColumn::new("bidder", SourceDataType::Int64),
        SourceColumn::new("price", SourceDataType::Int64),
        SourceColumn::new("channel", SourceDataType::Utf8),
        SourceColumn::new("url", SourceDataType::Utf8),
        SourceColumn::new("date_time", SourceDataType::TimestampMillis),
        SourceColumn::new("extra", SourceDataType::Utf8),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source;

    #[test]
    fn definitions_expose_three_sources() {
        let defs = definitions().expect("definitions");
        assert_eq!(defs.len(), 3);

        let names: std::collections::BTreeSet<_> = defs.iter().map(|def| def.name()).collect();
        let expected: std::collections::BTreeSet<_> =
            vec![AUCTION_SOURCE_NAME, BID_SOURCE_NAME, PERSON_SOURCE_NAME]
                .into_iter()
                .collect();
        assert_eq!(names, expected);

        for def in defs {
            assert_eq!(def.property(CONNECTOR_PROPERTY), Some(CONNECTOR_NAME));
            assert!(def.property(ENTITY_PROPERTY).is_some());
            assert!(!def.columns().is_empty());
        }
    }

    #[tokio::test]
    async fn run_produces_events() {
        let (tx, mut rx) = source::channel(16);
        let config = Config {
            events_per_second: 1000.0,
            max_events: Some(5),
        };

        run(config, tx).await.expect("generator run");

        let mut collected = Vec::new();
        while let Some(event) = rx.recv().await {
            collected.push(event.source().to_owned());
        }

        assert_eq!(collected.len(), 5);
        let names: std::collections::BTreeSet<_> = collected.into_iter().collect();
        let expected: std::collections::BTreeSet<_> = vec![
            AUCTION_SOURCE_NAME.to_string(),
            BID_SOURCE_NAME.to_string(),
            PERSON_SOURCE_NAME.to_string(),
        ]
        .into_iter()
        .collect();
        assert!(names.is_superset(&expected));
    }

    #[tokio::test]
    async fn run_rejects_non_positive_rate() {
        let (tx, _rx) = source::channel(1);
        let config = Config {
            events_per_second: 0.0,
            max_events: Some(1),
        };

        let result = run(config, tx).await;
        assert!(result.is_err());
    }
}
