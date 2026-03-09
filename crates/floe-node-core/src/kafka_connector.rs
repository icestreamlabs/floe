use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Timestamp};
use rdkafka::{Offset, TopicPartitionList};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::connector::{Connector, ConnectorContext, ConnectorTick, run_connector};
use crate::source::SourceEventSender;
use floe_core::source::{SourceDefinition, SourceEvent, SourceResumeToken};

static KAFKA_CONNECTOR_TICK_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
const KAFKA_CONNECTOR_TICK_LOG_EVERY: u64 = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaMessageFormat {
    FloeJson,
    DebeziumJson,
}

impl KafkaMessageFormat {
    pub fn parse(value: Option<&str>) -> Result<Self> {
        match value.map(|format| format.to_ascii_lowercase()) {
            None => Ok(Self::FloeJson),
            Some(format) if format == "floe_json" => Ok(Self::FloeJson),
            Some(format) if format == "debezium_json" => Ok(Self::DebeziumJson),
            Some(other) => anyhow::bail!("unsupported kafka message format '{other}'"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct KafkaConnectorConfig {
    pub brokers: String,
    pub topics: Vec<String>,
    pub group_id: String,
    pub default_source: Option<String>,
    pub poll_timeout: Duration,
    pub max_messages_per_tick: usize,
    pub message_format: Option<String>,
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
    message_format: KafkaMessageFormat,
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
        let message_format = KafkaMessageFormat::parse(config.message_format.as_deref())?;
        Ok(Self {
            config,
            message_format,
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
        let events = match self.message_format {
            KafkaMessageFormat::FloeJson => parse_floe_json_events(
                payload,
                self.config.default_source.as_deref(),
                message.topic(),
            ),
            KafkaMessageFormat::DebeziumJson => {
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
                parse_debezium_events(
                    value,
                    self.config.default_source.as_deref(),
                    message.topic(),
                    message.key(),
                )
            }
        };

        let events = match events {
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

        if KAFKA_CONNECTOR_TICK_LOG_COUNTER
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(KAFKA_CONNECTOR_TICK_LOG_EVERY)
        {
            tracing::info!(
                emitted,
                max_messages_per_tick = self.config.max_messages_per_tick,
                poll_timeout_ms = self.config.poll_timeout.as_millis() as u64,
                "kafka connector tick metrics"
            );
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

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FloeJsonMessage {
    Single(FloeJsonEvent),
    Batch(Vec<FloeJsonEvent>),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FloeJsonEvent {
    Wrapped { source: String, data: Value },
    Payload(Value),
}

fn parse_floe_json_events(
    payload: &[u8],
    default_source: Option<&str>,
    topic: &str,
) -> Result<Vec<SourceEvent>> {
    let message: FloeJsonMessage =
        serde_json::from_slice(payload).context("floe json payload must be valid json")?;

    match message {
        FloeJsonMessage::Single(event) => {
            Ok(vec![parse_floe_json_event(event, default_source, topic)?])
        }
        FloeJsonMessage::Batch(events) => {
            ensure!(!events.is_empty(), "event array must not be empty");
            let mut parsed = Vec::with_capacity(events.len());
            for event in events {
                parsed.push(parse_floe_json_event(event, default_source, topic)?);
            }
            Ok(parsed)
        }
    }
}

fn parse_floe_json_event(
    event: FloeJsonEvent,
    default_source: Option<&str>,
    topic: &str,
) -> Result<SourceEvent> {
    match event {
        FloeJsonEvent::Wrapped { source, data } => {
            ensure!(data.is_object(), "event payload must be an object");
            Ok(SourceEvent::new(source, data))
        }
        FloeJsonEvent::Payload(payload) => {
            let source = default_source.unwrap_or(topic);
            Ok(SourceEvent::new(source, payload))
        }
    }
}

fn parse_debezium_events(
    value: Value,
    default_source: Option<&str>,
    topic: &str,
    message_key: Option<&[u8]>,
) -> Result<Vec<SourceEvent>> {
    let object = value
        .as_object()
        .context("debezium payload must be a JSON object")?;
    let payload = object
        .get("payload")
        .context("debezium payload missing 'payload' field")?;
    if payload.is_null() {
        return Ok(Vec::new());
    }
    let payload_obj = payload
        .as_object()
        .context("debezium 'payload' must be an object")?;
    let op = payload_obj
        .get("op")
        .and_then(Value::as_str)
        .context("debezium payload missing string 'op'")?;

    let source = default_source.unwrap_or(topic);
    let mut row = match op {
        "c" | "u" => payload_obj
            .get("after")
            .and_then(Value::as_object)
            .cloned()
            .context("debezium upsert payload missing object 'after'")?,
        "r" => payload_obj
            .get("after")
            .and_then(Value::as_object)
            .cloned()
            .context("debezium snapshot payload missing object 'after'")?,
        "d" => payload_obj
            .get("before")
            .and_then(Value::as_object)
            .cloned()
            .context("debezium delete payload missing object 'before'")?,
        "t" => return Ok(Vec::new()),
        other => anyhow::bail!("unsupported debezium op '{other}'"),
    };

    let floe_op = match op {
        "d" => "delete",
        "r" => "snapshot",
        _ => "upsert",
    };
    row.insert("__floe_op".to_string(), Value::from(floe_op));

    if let Some(key_bytes) = message_key
        && let Ok(key_value) = serde_json::from_slice::<Value>(key_bytes)
    {
        row.insert("__floe_key".to_string(), key_value);
    }

    Ok(vec![SourceEvent::new(source, Value::Object(row))])
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_debezium_upsert_maps_after_payload() {
        let payload = json!({
            "payload": {
                "op": "u",
                "before": {"id": 1, "name": "old"},
                "after": {"id": 1, "name": "new"}
            }
        });
        let events =
            parse_debezium_events(payload, Some("public.users"), "dbz", Some(br#"{"id":1}"#))
                .expect("parse debezium");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source(), "public.users");
        assert_eq!(
            events[0].payload().get("__floe_op").and_then(Value::as_str),
            Some("upsert")
        );
        assert_eq!(
            events[0].payload().get("name").and_then(Value::as_str),
            Some("new")
        );
    }

    #[test]
    fn parse_debezium_delete_maps_before_payload() {
        let payload = json!({
            "payload": {
                "op": "d",
                "before": {"id": 7, "name": "gone"},
                "after": null
            }
        });
        let events = parse_debezium_events(payload, Some("public.users"), "dbz", None)
            .expect("parse debezium");
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].payload().get("__floe_op").and_then(Value::as_str),
            Some("delete")
        );
        assert_eq!(
            events[0].payload().get("id").and_then(Value::as_i64),
            Some(7)
        );
    }

    #[test]
    fn parse_floe_json_still_supports_source_data_wrapper() {
        let payload = br#"{"source":"nexmark_bid","data":{"auction":1}}"#;
        let events = parse_floe_json_events(payload, None, "topic").expect("parse floe json");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source(), "nexmark_bid");
        assert_eq!(
            events[0].payload().get("auction").and_then(Value::as_i64),
            Some(1)
        );
    }
}
