use std::time::Duration;

use anyhow::{Context, Result, ensure};
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::connector::{Connector, ConnectorContext, ConnectorTick, run_connector};
use crate::source::SourceEventSender;
use floe_core::source::{SourceDefinition, SourceEvent};

#[derive(Debug, Clone)]
pub struct KafkaConnectorConfig {
    pub brokers: String,
    pub topics: Vec<String>,
    pub group_id: String,
    pub default_source: Option<String>,
    pub poll_timeout: Duration,
    pub max_messages_per_tick: usize,
}

pub struct KafkaConnector {
    config: KafkaConnectorConfig,
    definitions: Vec<SourceDefinition>,
    consumer: Option<StreamConsumer>,
}

impl KafkaConnector {
    pub fn new(config: KafkaConnectorConfig, definitions: Vec<SourceDefinition>) -> Result<Self> {
        ensure!(
            !config.brokers.trim().is_empty(),
            "kafka brokers must not be empty"
        );
        ensure!(!config.topics.is_empty(), "kafka topics must not be empty");
        ensure!(
            config.max_messages_per_tick > 0,
            "kafka max messages per tick must be positive"
        );
        Ok(Self {
            config,
            definitions,
            consumer: None,
        })
    }

    pub async fn run(config: KafkaConnectorConfig, sender: SourceEventSender) -> Result<()> {
        let mut connector = KafkaConnector::new(config, Vec::new())?;
        let ctx = ConnectorContext::new(sender);
        run_connector(&mut connector, &ctx, CancellationToken::new()).await
    }

    async fn handle_message(
        &self,
        ctx: &ConnectorContext,
        message: &BorrowedMessage<'_>,
    ) -> Result<usize> {
        let payload = match message.payload() {
            Some(payload) => payload,
            None => {
                tracing::warn!(
                    topic = message.topic(),
                    partition = message.partition(),
                    offset = message.offset(),
                    "kafka message missing payload"
                );
                return Ok(0);
            }
        };
        let value: Value = match serde_json::from_slice(payload) {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(
                    topic = message.topic(),
                    partition = message.partition(),
                    offset = message.offset(),
                    error = %err,
                    "failed to decode kafka message json"
                );
                return Ok(0);
            }
        };

        let events = match parse_events(
            value,
            self.config.default_source.as_deref(),
            message.topic(),
        ) {
            Ok(events) => events,
            Err(err) => {
                tracing::warn!(
                    topic = message.topic(),
                    partition = message.partition(),
                    offset = message.offset(),
                    error = %err,
                    "failed to parse kafka message payload"
                );
                return Ok(0);
            }
        };

        for event in &events {
            ctx.sender().send(event.clone()).await.with_context(|| {
                format!(
                    "failed to enqueue kafka event for source {}",
                    event.source()
                )
            })?;
        }
        Ok(events.len())
    }
}

#[async_trait::async_trait]
impl Connector for KafkaConnector {
    fn name(&self) -> &str {
        "kafka"
    }

    fn definitions(&self) -> &[SourceDefinition] {
        &self.definitions
    }

    fn tick_interval(&self) -> Duration {
        self.config.poll_timeout
    }

    async fn init(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.config.brokers)
            .set("group.id", &self.config.group_id)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest");
        let consumer: StreamConsumer = client_config.create().context("create kafka consumer")?;
        let topics: Vec<&str> = self.config.topics.iter().map(String::as_str).collect();
        consumer
            .subscribe(&topics)
            .context("subscribe to kafka topics")?;
        self.consumer = Some(consumer);
        Ok(())
    }

    async fn tick(&mut self, ctx: &ConnectorContext) -> Result<ConnectorTick> {
        let consumer = self
            .consumer
            .as_ref()
            .context("kafka connector is not initialized")?;
        let mut emitted = 0usize;

        let first_timeout = if self.config.poll_timeout.is_zero() {
            Duration::from_millis(0)
        } else {
            self.config.poll_timeout
        };
        match tokio::time::timeout(first_timeout, consumer.recv()).await {
            Ok(Ok(message)) => {
                emitted = emitted.saturating_add(self.handle_message(ctx, &message).await?);
            }
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "failed to receive kafka message");
            }
            Err(_) => {}
        }

        while emitted < self.config.max_messages_per_tick {
            match tokio::time::timeout(Duration::from_millis(0), consumer.recv()).await {
                Ok(Ok(message)) => {
                    emitted = emitted.saturating_add(self.handle_message(ctx, &message).await?);
                }
                Ok(Err(err)) => {
                    tracing::warn!(error = %err, "failed to receive kafka message");
                }
                Err(_) => break,
            }
        }

        if emitted > 0 {
            Ok(ConnectorTick::Emitted(emitted))
        } else {
            Ok(ConnectorTick::Idle)
        }
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.consumer = None;
        Ok(())
    }
}

fn parse_events(
    value: Value,
    default_source: Option<&str>,
    topic: &str,
) -> Result<Vec<SourceEvent>> {
    match value {
        Value::Array(items) => {
            ensure!(!items.is_empty(), "event array must not be empty");
            let mut events = Vec::with_capacity(items.len());
            for item in items {
                events.push(parse_event(item, default_source, topic)?);
            }
            Ok(events)
        }
        other => Ok(vec![parse_event(other, default_source, topic)?]),
    }
}

fn parse_event(value: Value, default_source: Option<&str>, topic: &str) -> Result<SourceEvent> {
    let object = value
        .as_object()
        .context("event payload must be a JSON object")?;

    if let (Some(source), Some(payload)) = (object.get("source"), object.get("data")) {
        let source = source.as_str().context("event source must be a string")?;
        ensure!(payload.is_object(), "event payload must be an object");
        return Ok(SourceEvent::new(source, payload.clone()));
    }

    let source = default_source.unwrap_or(topic);
    Ok(SourceEvent::new(source, value))
}
