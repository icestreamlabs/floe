use std::time::Duration;

use anyhow::{Context, Result, ensure};
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Timestamp};
use rdkafka::{Offset, TopicPartitionList};
use serde_json::Value;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::connector::{Connector, ConnectorContext, ConnectorTick, run_connector};
use crate::source::SourceEventSender;
use floe_core::source::{SourceDefinition, SourceEvent, SourceResumeToken};

#[derive(Debug, Clone)]
pub struct KafkaConnectorConfig {
    pub brokers: String,
    pub topics: Vec<String>,
    pub group_id: String,
    pub default_source: Option<String>,
    pub poll_timeout: Duration,
    pub max_messages_per_tick: usize,
    pub commit_offsets_rx: Option<watch::Receiver<KafkaOffsetCommit>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaTopicPartitionOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KafkaOffsetCommit {
    pub tick_id: u64,
    pub offsets: Vec<KafkaTopicPartitionOffset>,
}

pub struct KafkaConnector {
    config: KafkaConnectorConfig,
    definitions: Vec<SourceDefinition>,
    consumer: Option<StreamConsumer>,
    last_committed_tick_id: u64,
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
            last_committed_tick_id: 0,
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
            let mut event = event.clone().with_resume_token(SourceResumeToken::Kafka {
                topic: message.topic().to_string(),
                partition: message.partition(),
                offset: message.offset(),
            });
            if let Some(event_time_ms) = kafka_message_timestamp_ms(message) {
                event = event.with_event_time_ms(event_time_ms);
            }
            let source_name = event.source().to_string();
            ctx.sender().send(event).await.with_context(|| {
                format!("failed to enqueue kafka event for source {}", source_name)
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
            .set("enable.auto.commit", "false")
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

        self.commit_offsets_if_requested().await?;

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

impl KafkaConnector {
    async fn commit_offsets_if_requested(&mut self) -> Result<()> {
        let Some(consumer) = self.consumer.as_ref() else {
            return Ok(());
        };
        let Some(receiver) = self.config.commit_offsets_rx.as_mut() else {
            return Ok(());
        };

        let mut latest_commit = None;
        while receiver.has_changed().unwrap_or(false) {
            latest_commit = Some(receiver.borrow_and_update().clone());
        }
        let Some(commit) = latest_commit else {
            return Ok(());
        };
        if commit.tick_id <= self.last_committed_tick_id {
            return Ok(());
        }

        let mut tpl = TopicPartitionList::new();
        let mut has_offsets = false;
        for entry in &commit.offsets {
            if !self.config.topics.iter().any(|topic| topic == &entry.topic) {
                continue;
            }
            let next_offset = entry.offset.saturating_add(1);
            tpl.add_partition_offset(&entry.topic, entry.partition, Offset::Offset(next_offset))
                .with_context(|| {
                    format!(
                        "set kafka commit offset for {}[{}] at tick {}",
                        entry.topic, entry.partition, commit.tick_id
                    )
                })?;
            has_offsets = true;
        }
        if !has_offsets {
            return Ok(());
        }

        consumer
            .commit(&tpl, CommitMode::Sync)
            .context("commit kafka offsets after tick commit")?;
        self.last_committed_tick_id = commit.tick_id;
        Ok(())
    }
}

fn kafka_message_timestamp_ms(message: &BorrowedMessage<'_>) -> Option<u64> {
    match message.timestamp() {
        Timestamp::NotAvailable => None,
        Timestamp::CreateTime(value) | Timestamp::LogAppendTime(value) => u64::try_from(value).ok(),
    }
}
