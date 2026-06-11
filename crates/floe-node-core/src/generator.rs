use std::time::Duration;

use anyhow::{Context, Result, anyhow, ensure};
use floe_core::source::{
    AppendIngestEvent, AppendIngestResumeToken, SourceColumn, SourceDataType, SourceDefinition,
};
use nexmark::EventGenerator;
use nexmark::config::NexmarkConfig;
use nexmark::event::Event;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::connector::{Connector, ConnectorContext, ConnectorTick, run_connector};
use crate::source::AppendIngestEventSender;
use crate::source::send_event;

const CONNECTOR_NAME: &str = "nexmark";
const CONNECTOR_PROPERTY: &str = "connector";
const ENTITY_PROPERTY: &str = "entity";
const APPEND_ONLY_PROPERTY: &str = "append_only";

pub const PERSON_SOURCE_NAME: &str = "nexmark_person";
pub const AUCTION_SOURCE_NAME: &str = "nexmark_auction";
pub const BID_SOURCE_NAME: &str = "nexmark_bid";

#[derive(Debug, Clone)]
pub struct Config {
    pub events_per_second: f64,
    pub max_events: Option<u64>,
}

pub struct NexmarkConnector {
    config: Config,
    definitions: Vec<SourceDefinition>,
    generator: Option<EventGenerator>,
    emitted: u64,
    interval: Duration,
}

impl NexmarkConnector {
    pub fn new(config: Config) -> Result<Self> {
        ensure!(
            config.events_per_second.is_finite() && config.events_per_second > 0.0,
            "events-per-second must be a positive finite value"
        );
        let interval = Duration::from_secs_f64(1.0 / config.events_per_second);
        let definitions = definitions()?;
        Ok(Self {
            config,
            definitions,
            generator: None,
            emitted: 0,
            interval,
        })
    }
}

#[async_trait::async_trait]
impl Connector for NexmarkConnector {
    fn name(&self) -> &str {
        CONNECTOR_NAME
    }

    fn definitions(&self) -> &[SourceDefinition] {
        &self.definitions
    }

    fn tick_interval(&self) -> Duration {
        self.interval
    }

    async fn init(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        self.generator = Some(EventGenerator::new(NexmarkConfig::default()));
        self.emitted = 0;
        Ok(())
    }

    async fn tick(&mut self, ctx: &ConnectorContext) -> Result<ConnectorTick> {
        if let Some(limit) = self.config.max_events
            && self.emitted >= limit
        {
            return Ok(ConnectorTick::Finished);
        }

        let generator = self
            .generator
            .as_mut()
            .context("nexmark connector is not initialized")?;
        let event = generator
            .next()
            .context("nexmark generator produced no event")?;
        forward_event(ctx.sender(), &event, self.emitted).await?;
        self.emitted = self.emitted.saturating_add(1);

        if let Some(limit) = self.config.max_events
            && self.emitted >= limit
        {
            Ok(ConnectorTick::Finished)
        } else {
            Ok(ConnectorTick::Emitted(1))
        }
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.generator = None;
        Ok(())
    }
}

pub fn definitions() -> Result<Vec<SourceDefinition>> {
    let person = SourceDefinition::new(PERSON_SOURCE_NAME, person_columns())?
        .with_property(CONNECTOR_PROPERTY, CONNECTOR_NAME)
        .with_property(ENTITY_PROPERTY, "person")
        .with_property(APPEND_ONLY_PROPERTY, "true");
    let auction = SourceDefinition::new(AUCTION_SOURCE_NAME, auction_columns())?
        .with_property(CONNECTOR_PROPERTY, CONNECTOR_NAME)
        .with_property(ENTITY_PROPERTY, "auction")
        .with_property(APPEND_ONLY_PROPERTY, "true");
    let bid = SourceDefinition::new(BID_SOURCE_NAME, bid_columns())?
        .with_property(CONNECTOR_PROPERTY, CONNECTOR_NAME)
        .with_property(ENTITY_PROPERTY, "bid")
        .with_property(APPEND_ONLY_PROPERTY, "true");

    Ok(vec![person, auction, bid])
}

pub async fn run(config: Config, sender: AppendIngestEventSender) -> Result<()> {
    let mut connector = NexmarkConnector::new(config)?;
    let ctx = ConnectorContext::new(sender);
    run_connector(&mut connector, &ctx, CancellationToken::new()).await
}

async fn forward_event(
    sender: &AppendIngestEventSender,
    event: &Event,
    position: u64,
) -> Result<()> {
    match event {
        Event::Person(person) => {
            send_payload(sender, PERSON_SOURCE_NAME, person, "person", position).await
        }
        Event::Auction(auction) => {
            send_payload(sender, AUCTION_SOURCE_NAME, auction, "auction", position).await
        }
        Event::Bid(bid) => send_payload(sender, BID_SOURCE_NAME, bid, "bid", position).await,
    }
}

async fn send_payload<T>(
    sender: &AppendIngestEventSender,
    source: &str,
    payload: &T,
    entity: &str,
    position: u64,
) -> Result<()>
where
    T: Serialize,
{
    let json = serde_json::to_value(payload)
        .with_context(|| format!("failed to serialize {entity} event"))?;
    let event = AppendIngestEvent::new(source, json)
        .with_resume_token(AppendIngestResumeToken::Generator { position });
    send_event(sender, event)
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
            assert_eq!(def.property(APPEND_ONLY_PROPERTY), Some("true"));
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
        while let Some(batch) = rx.recv().await {
            collected.extend(batch.into_iter().map(|event| event.source().to_owned()));
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
