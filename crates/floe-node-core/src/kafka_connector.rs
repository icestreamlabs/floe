use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::{BorrowedMessage, Timestamp};
use rdkafka::{Offset, TopicPartitionList};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::connector::{Connector, ConnectorContext, ConnectorTick, run_connector};
use crate::source::AppendIngestEventSender;
use floe_core::source::{AppendIngestEvent, SourceDefinition};

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
    pub resume_from_offsets: Vec<KafkaTopicPartitionOffset>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaReplayRange {
    pub source: String,
    pub tick_id: u64,
    pub max_event_time_ms: Option<i64>,
    pub topic: String,
    pub partition: i32,
    pub start_offset: i64,
    pub end_offset: i64,
}

#[derive(Debug, Clone)]
pub struct KafkaReplayBatch {
    pub source: String,
    pub tick_id: u64,
    pub max_event_time_ms: Option<i64>,
    pub events: Vec<AppendIngestEvent>,
}

pub struct KafkaConnector {
    config: KafkaConnectorConfig,
    message_format: KafkaMessageFormat,
    definitions: Vec<SourceDefinition>,
    topic_arcs: HashMap<String, Arc<str>>,
    consumer: Option<BaseConsumer>,
    last_committed_tick_id: u64,
    started_at: Option<Instant>,
    first_batch_logged: bool,
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
        let topic_arcs = config
            .topics
            .iter()
            .map(|topic| (topic.clone(), Arc::<str>::from(topic.as_str())))
            .collect();
        Ok(Self {
            config,
            message_format,
            definitions,
            topic_arcs,
            consumer: None,
            last_committed_tick_id: 0,
            started_at: None,
            first_batch_logged: false,
        })
    }

    pub async fn run(config: KafkaConnectorConfig, sender: AppendIngestEventSender) -> Result<()> {
        let mut connector = KafkaConnector::new(config, Vec::new())?;
        let ctx = ConnectorContext::new(sender);
        run_connector(&mut connector, &ctx, CancellationToken::new()).await
    }

    pub async fn replay_range(
        config: KafkaConnectorConfig,
        definitions: Vec<SourceDefinition>,
        range: KafkaReplayRange,
    ) -> Result<KafkaReplayBatch> {
        ensure!(
            range.start_offset >= 0 && range.start_offset <= range.end_offset,
            "invalid kafka replay range {}[{}] {}..{}",
            range.topic,
            range.partition,
            range.start_offset,
            range.end_offset
        );
        ensure!(
            config.topics.iter().any(|topic| topic == &range.topic),
            "kafka replay range topic '{}' is not owned by connector topics {:?}",
            range.topic,
            config.topics
        );
        let poll_timeout = config.poll_timeout.max(Duration::from_millis(100));
        let idle_timeout = Duration::from_secs(30);
        let connector = KafkaConnector::new(config.clone(), definitions)?;
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", format!("{}-replay", config.group_id))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "none")
            .set("enable.partition.eof", "true");
        Self::apply_latency_fetch_config(&mut client_config);
        let consumer: BaseConsumer = client_config
            .create()
            .context("create kafka replay consumer")?;
        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset(
                &range.topic,
                range.partition,
                Offset::Offset(range.start_offset),
            )
            .with_context(|| {
                format!(
                    "assign kafka replay offset for {}[{}]",
                    range.topic, range.partition
                )
            })?;
        consumer
            .assign(&assignment)
            .context("assign kafka replay partition")?;

        let mut events = Vec::new();
        let mut idle_since = Instant::now();
        loop {
            match consumer.poll(poll_timeout) {
                Some(Ok(message)) => {
                    idle_since = Instant::now();
                    if message.topic() != range.topic || message.partition() != range.partition {
                        continue;
                    }
                    let offset = message.offset();
                    if offset < range.start_offset {
                        continue;
                    }
                    if offset > range.end_offset {
                        break;
                    }
                    let mut parsed = connector.handle_message(&message)?;
                    events.append(&mut parsed);
                    if offset >= range.end_offset {
                        break;
                    }
                }
                Some(Err(err)) => {
                    tracing::debug!(
                        topic = %range.topic,
                        partition = range.partition,
                        error = %err,
                        "kafka replay poll returned an error"
                    );
                    if idle_since.elapsed() >= idle_timeout {
                        anyhow::bail!(
                            "timed out replaying kafka range {}[{}] {}..{} after poll error: {err}",
                            range.topic,
                            range.partition,
                            range.start_offset,
                            range.end_offset
                        );
                    }
                }
                None => {
                    if idle_since.elapsed() >= idle_timeout {
                        anyhow::bail!(
                            "timed out replaying kafka range {}[{}] {}..{}",
                            range.topic,
                            range.partition,
                            range.start_offset,
                            range.end_offset
                        );
                    }
                }
            }
        }

        Ok(KafkaReplayBatch {
            source: range.source,
            tick_id: range.tick_id,
            max_event_time_ms: range.max_event_time_ms,
            events,
        })
    }

    fn apply_latency_fetch_config(client_config: &mut ClientConfig) {
        client_config
            .set("fetch.wait.max.ms", "1")
            .set("fetch.queue.backoff.ms", "1")
            .set("fetch.min.bytes", "1")
            .set("enable.auto.offset.store", "false");
    }

    fn handle_message(&self, message: &BorrowedMessage<'_>) -> Result<Vec<AppendIngestEvent>> {
        let payload = match message.payload() {
            Some(payload) => payload,
            None => {
                tracing::warn!(
                    topic = message.topic(),
                    partition = message.partition(),
                    offset = message.offset(),
                    "kafka message missing payload"
                );
                return Ok(Vec::new());
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
                        return Ok(Vec::new());
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
                return Ok(Vec::new());
            }
        };

        let mut staged = Vec::with_capacity(events.len());
        let topic = self
            .topic_arcs
            .get(message.topic())
            .cloned()
            .unwrap_or_else(|| Arc::<str>::from(message.topic()));
        for event in events {
            let mut event = event.with_kafka_position(
                Arc::clone(&topic),
                message.partition(),
                message.offset(),
            );
            if event.event_time_ms().is_none()
                && let Some(event_time_ms) = kafka_message_timestamp_ms(message)
            {
                event = event.with_event_time_ms(event_time_ms);
            }
            staged.push(event);
        }
        Ok(staged)
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
        Duration::ZERO
    }

    async fn init(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.config.brokers)
            .set("group.id", &self.config.group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest");
        Self::apply_latency_fetch_config(&mut client_config);
        tracing::info!("kafka latency fetch config enabled by default");
        let consumer: BaseConsumer = client_config.create().context("create kafka consumer")?;
        if self.config.resume_from_offsets.is_empty() {
            let topics: Vec<&str> = self.config.topics.iter().map(String::as_str).collect();
            consumer
                .subscribe(&topics)
                .context("subscribe to kafka topics")?;
        } else {
            let mut assignment = TopicPartitionList::new();
            let mut assigned_partitions = 0usize;
            for offset in &self.config.resume_from_offsets {
                if !self
                    .config
                    .topics
                    .iter()
                    .any(|topic| topic == &offset.topic)
                {
                    continue;
                }
                let resume_offset = offset.offset.saturating_add(1);
                assignment
                    .add_partition_offset(
                        &offset.topic,
                        offset.partition,
                        Offset::Offset(resume_offset),
                    )
                    .with_context(|| {
                        format!(
                            "assign kafka resume offset for {}[{}]",
                            offset.topic, offset.partition
                        )
                    })?;
                assigned_partitions += 1;
            }
            if assigned_partitions == 0 {
                let topics: Vec<&str> = self.config.topics.iter().map(String::as_str).collect();
                consumer
                    .subscribe(&topics)
                    .context("subscribe to kafka topics")?;
            } else {
                consumer
                    .assign(&assignment)
                    .context("assign kafka resume partitions")?;
                tracing::info!(
                    resume_offsets = ?self.config.resume_from_offsets,
                    "kafka connector resuming assigned partitions after recovered offsets"
                );
            }
        }
        self.consumer = Some(consumer);
        self.started_at = Some(Instant::now());
        self.first_batch_logged = false;
        Ok(())
    }

    async fn tick(&mut self, ctx: &ConnectorContext) -> Result<ConnectorTick> {
        let consumer = self
            .consumer
            .as_ref()
            .context("kafka connector is not initialized")?;
        let mut emitted = 0usize;
        let mut staged = Vec::new();

        if let Some(message) = consumer.poll(self.config.poll_timeout) {
            match message {
                Ok(message) => {
                    let mut events = self.handle_message(&message)?;
                    emitted = emitted.saturating_add(events.len());
                    staged.append(&mut events);
                }
                Err(err) => {
                    tracing::warn!(error = %err, "failed to receive kafka message");
                }
            }
        }

        while emitted < self.config.max_messages_per_tick {
            match consumer.poll(Duration::ZERO) {
                Some(Ok(message)) => {
                    let mut events = self.handle_message(&message)?;
                    emitted = emitted.saturating_add(events.len());
                    staged.append(&mut events);
                }
                Some(Err(err)) => {
                    tracing::warn!(error = %err, "failed to receive kafka message");
                }
                None => break,
            }
        }

        if !staged.is_empty() {
            ctx.send_batch(staged)
                .await
                .context("failed to enqueue kafka event batch")?;
            if !self.first_batch_logged {
                self.first_batch_logged = true;
                tracing::info!(
                    emitted,
                    time_to_first_batch_ms = self
                        .started_at
                        .map(|started| started.elapsed().as_millis() as u64)
                        .unwrap_or_default(),
                    "kafka connector emitted first batch"
                );
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
) -> Result<Vec<AppendIngestEvent>> {
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
) -> Result<AppendIngestEvent> {
    match event {
        FloeJsonEvent::Wrapped { source, data } => {
            ensure!(data.is_object(), "event payload must be an object");
            Ok(AppendIngestEvent::new(source, data))
        }
        FloeJsonEvent::Payload(payload) => {
            let source = default_source.unwrap_or(topic);
            Ok(AppendIngestEvent::new(source, payload))
        }
    }
}

fn parse_debezium_events(
    value: Value,
    default_source: Option<&str>,
    topic: &str,
    message_key: Option<&[u8]>,
) -> Result<Vec<AppendIngestEvent>> {
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

    Ok(vec![AppendIngestEvent::new(source, Value::Object(row))])
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
            self.last_committed_tick_id = commit.tick_id;
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
            events[0]
                .payload()
                .and_then(|payload| payload.get("__floe_op"))
                .and_then(Value::as_str),
            Some("upsert")
        );
        assert_eq!(
            events[0]
                .payload()
                .and_then(|payload| payload.get("name"))
                .and_then(Value::as_str),
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
            events[0]
                .payload()
                .and_then(|payload| payload.get("__floe_op"))
                .and_then(Value::as_str),
            Some("delete")
        );
        assert_eq!(
            events[0]
                .payload()
                .and_then(|payload| payload.get("id"))
                .and_then(Value::as_i64),
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
            events[0]
                .payload()
                .and_then(|payload| payload.get("auction"))
                .and_then(Value::as_i64),
            Some(1)
        );
    }
}
