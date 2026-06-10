use std::collections::HashMap;
use std::sync::Arc;
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
use crate::source::{AppendIngestEventSender, KafkaRawIngestBatch, KafkaRawIngestRecord};
use floe_core::source::{AppendIngestEvent, SourceDefinition};

const KAFKA_CONNECTOR_TICK_LOG_EVERY: u64 = 32;

#[derive(Default)]
struct KafkaConnectorTickMetrics {
    messages: u64,
    events: u64,
    errors: u64,
    blocking_polls: u64,
    empty_blocking_polls: u64,
    drain_polls: u64,
    empty_drain_polls: u64,
    committed_offset_batches: u64,
    poll_blocking_us: u64,
    poll_drain_us: u64,
    parse_us: u64,
    message_us: u64,
    send_us: u64,
    commit_us: u64,
    tick_us: u64,
}

#[derive(Default)]
struct KafkaConnectorTickWindow {
    ticks: u64,
    idle_ticks: u64,
    messages: u64,
    events: u64,
    errors: u64,
    blocking_polls: u64,
    empty_blocking_polls: u64,
    drain_polls: u64,
    empty_drain_polls: u64,
    committed_offset_batches: u64,
    poll_blocking_us: u64,
    poll_drain_us: u64,
    parse_us: u64,
    message_us: u64,
    send_us: u64,
    commit_us: u64,
    tick_us: u64,
    max_tick_us: u64,
}

impl KafkaConnectorTickWindow {
    fn record(&mut self, metrics: &KafkaConnectorTickMetrics) {
        self.ticks = self.ticks.saturating_add(1);
        if metrics.events == 0 {
            self.idle_ticks = self.idle_ticks.saturating_add(1);
        }
        self.messages = self.messages.saturating_add(metrics.messages);
        self.events = self.events.saturating_add(metrics.events);
        self.errors = self.errors.saturating_add(metrics.errors);
        self.blocking_polls = self.blocking_polls.saturating_add(metrics.blocking_polls);
        self.empty_blocking_polls = self
            .empty_blocking_polls
            .saturating_add(metrics.empty_blocking_polls);
        self.drain_polls = self.drain_polls.saturating_add(metrics.drain_polls);
        self.empty_drain_polls = self
            .empty_drain_polls
            .saturating_add(metrics.empty_drain_polls);
        self.committed_offset_batches = self
            .committed_offset_batches
            .saturating_add(metrics.committed_offset_batches);
        self.poll_blocking_us = self
            .poll_blocking_us
            .saturating_add(metrics.poll_blocking_us);
        self.poll_drain_us = self.poll_drain_us.saturating_add(metrics.poll_drain_us);
        self.parse_us = self.parse_us.saturating_add(metrics.parse_us);
        self.message_us = self.message_us.saturating_add(metrics.message_us);
        self.send_us = self.send_us.saturating_add(metrics.send_us);
        self.commit_us = self.commit_us.saturating_add(metrics.commit_us);
        self.tick_us = self.tick_us.saturating_add(metrics.tick_us);
        self.max_tick_us = self.max_tick_us.max(metrics.tick_us);
    }
}

fn elapsed_us(start: Instant) -> u64 {
    start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}

fn avg_u64(total: u64, count: u64) -> u64 {
    if count == 0 { 0 } else { total / count }
}

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
    pub replay_idle_timeout: Duration,
    pub max_messages_per_tick: usize,
    pub message_format: Option<String>,
    pub commit_offsets_rx: Option<watch::Receiver<KafkaOffsetCommit>>,
    pub resume_from_offsets: Vec<KafkaTopicPartitionOffset>,
}

impl KafkaConnectorConfig {
    pub fn default_replay_idle_timeout(poll_timeout: Duration) -> Duration {
        let poll_timeout = poll_timeout.max(Duration::from_millis(1));
        poll_timeout
            .saturating_mul(10)
            .clamp(Duration::from_millis(50), Duration::from_secs(5))
    }
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
    pub raw_payloads: Vec<KafkaReplayRawPayload>,
}

#[derive(Debug, Clone)]
pub struct KafkaReplayRawPayload {
    pub topic: Arc<str>,
    pub partition: i32,
    pub offset: i64,
    pub payload: Vec<u8>,
}

pub struct KafkaConnector {
    config: KafkaConnectorConfig,
    message_format: KafkaMessageFormat,
    definitions: Arc<[SourceDefinition]>,
    topic_arcs: HashMap<String, Arc<str>>,
    consumer: Option<BaseConsumer>,
    last_committed_tick_id: u64,
    started_at: Option<Instant>,
    first_batch_logged: bool,
    tick_counter: u64,
    tick_window: KafkaConnectorTickWindow,
}

impl KafkaConnector {
    pub fn new(config: KafkaConnectorConfig, definitions: Vec<SourceDefinition>) -> Result<Self> {
        Self::new_with_shared_definitions(config, Arc::from(definitions))
    }

    pub fn new_with_shared_definitions(
        config: KafkaConnectorConfig,
        definitions: Arc<[SourceDefinition]>,
    ) -> Result<Self> {
        ensure!(
            !config.brokers.trim().is_empty(),
            "kafka brokers must not be empty"
        );
        ensure!(!config.topics.is_empty(), "kafka topics must not be empty");
        ensure!(
            config.max_messages_per_tick > 0,
            "kafka max messages per tick must be positive"
        );
        ensure!(
            !config.replay_idle_timeout.is_zero(),
            "kafka replay idle timeout must be positive"
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
            tick_counter: 0,
            tick_window: KafkaConnectorTickWindow::default(),
        })
    }

    pub async fn run(config: KafkaConnectorConfig, sender: AppendIngestEventSender) -> Result<()> {
        let mut connector = KafkaConnector::new(config, Vec::new())?;
        let ctx = ConnectorContext::new(sender);
        run_connector(&mut connector, &ctx, CancellationToken::new()).await
    }

    pub async fn replay_range(
        config: KafkaConnectorConfig,
        definitions: Arc<[SourceDefinition]>,
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
        let poll_timeout = config.poll_timeout;
        let idle_timeout = config.replay_idle_timeout;
        let connector = KafkaConnector::new_with_shared_definitions(config.clone(), definitions)?;
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
        let mut raw_payloads = Vec::new();
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
                    if let Some(payload) = message.payload() {
                        raw_payloads.push(KafkaReplayRawPayload {
                            topic: Arc::<str>::from(message.topic()),
                            partition: message.partition(),
                            offset,
                            payload: payload.to_vec(),
                        });
                    }
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
            raw_payloads,
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
        let metrics_enabled =
            tracing::enabled!(target: "floe_node_core::kafka_connector", tracing::Level::DEBUG);
        let tick_start = metrics_enabled.then(Instant::now);
        let consumer = self
            .consumer
            .as_ref()
            .context("kafka connector is not initialized")?;
        let raw_source = self.raw_default_source(ctx).map(str::to_string);
        let mut emitted = 0usize;
        let mut staged = Vec::new();
        let mut staged_raw = raw_source.as_ref().map(|source| KafkaRawIngestBatch {
            source: source.clone(),
            records: Vec::new(),
        });
        let mut metrics = KafkaConnectorTickMetrics::default();
        if metrics_enabled {
            metrics.blocking_polls = 1;
        }

        let poll_start = metrics_enabled.then(Instant::now);
        if let Some(message) = consumer.poll(self.config.poll_timeout) {
            if let Some(poll_start) = poll_start {
                metrics.poll_blocking_us = elapsed_us(poll_start);
            }
            match message {
                Ok(message) => {
                    let event_count = if let Some(raw_batch) = staged_raw.as_mut() {
                        self.stage_raw_or_event_message(
                            &message,
                            raw_batch,
                            &mut staged,
                            &mut metrics,
                            metrics_enabled,
                        )?
                    } else {
                        self.stage_message(&message, &mut staged, &mut metrics, metrics_enabled)?
                    };
                    emitted = emitted.saturating_add(event_count);
                }
                Err(err) => {
                    if metrics_enabled {
                        metrics.errors = metrics.errors.saturating_add(1);
                    }
                    tracing::warn!(error = %err, "failed to receive kafka message");
                }
            }
        } else {
            if let Some(poll_start) = poll_start {
                metrics.poll_blocking_us = elapsed_us(poll_start);
                metrics.empty_blocking_polls = metrics.empty_blocking_polls.saturating_add(1);
            }
        }

        while emitted < self.config.max_messages_per_tick {
            if metrics_enabled {
                metrics.drain_polls = metrics.drain_polls.saturating_add(1);
            }
            let poll_start = metrics_enabled.then(Instant::now);
            match consumer.poll(Duration::ZERO) {
                Some(Ok(message)) => {
                    if let Some(poll_start) = poll_start {
                        metrics.poll_drain_us =
                            metrics.poll_drain_us.saturating_add(elapsed_us(poll_start));
                    }
                    let event_count = if let Some(raw_batch) = staged_raw.as_mut() {
                        self.stage_raw_or_event_message(
                            &message,
                            raw_batch,
                            &mut staged,
                            &mut metrics,
                            metrics_enabled,
                        )?
                    } else {
                        self.stage_message(&message, &mut staged, &mut metrics, metrics_enabled)?
                    };
                    emitted = emitted.saturating_add(event_count);
                }
                Some(Err(err)) => {
                    if let Some(poll_start) = poll_start {
                        metrics.poll_drain_us =
                            metrics.poll_drain_us.saturating_add(elapsed_us(poll_start));
                        metrics.errors = metrics.errors.saturating_add(1);
                    }
                    tracing::warn!(error = %err, "failed to receive kafka message");
                }
                None => {
                    if let Some(poll_start) = poll_start {
                        metrics.poll_drain_us =
                            metrics.poll_drain_us.saturating_add(elapsed_us(poll_start));
                        metrics.empty_drain_polls = metrics.empty_drain_polls.saturating_add(1);
                    }
                    break;
                }
            }
        }

        if let Some(raw_batch) = staged_raw.take()
            && !raw_batch.is_empty()
        {
            let send_start = metrics_enabled.then(Instant::now);
            if let Err(err) = ctx.send_kafka_raw_batch(raw_batch).await {
                anyhow::bail!(
                    "failed to enqueue raw kafka event batch with {} records",
                    err.0.len()
                );
            }
            if let Some(send_start) = send_start {
                metrics.send_us = metrics.send_us.saturating_add(elapsed_us(send_start));
            }
        }

        if !staged.is_empty() {
            let send_start = metrics_enabled.then(Instant::now);
            ctx.send_batch(staged)
                .await
                .context("failed to enqueue kafka event batch")?;
            if let Some(send_start) = send_start {
                metrics.send_us = metrics.send_us.saturating_add(elapsed_us(send_start));
            }
        }

        if emitted > 0 && !self.first_batch_logged {
            self.first_batch_logged = true;
            let time_to_first_batch_ms = self
                .started_at
                .map(|started| started.elapsed().as_millis() as u64)
                .unwrap_or_default();
            if metrics_enabled {
                tracing::info!(
                    emitted,
                    kafka_messages = metrics.messages,
                    poll_blocking_us = metrics.poll_blocking_us,
                    poll_drain_us = metrics.poll_drain_us,
                    parse_us = metrics.parse_us,
                    message_us = metrics.message_us,
                    send_us = metrics.send_us,
                    raw_fast_path = raw_source.is_some(),
                    time_to_first_batch_ms,
                    "kafka connector emitted first batch"
                );
            } else {
                tracing::info!(
                    emitted,
                    raw_fast_path = raw_source.is_some(),
                    time_to_first_batch_ms,
                    "kafka connector emitted first batch"
                );
            }
        }

        let commit_start = metrics_enabled.then(Instant::now);
        if self.commit_offsets_if_requested().await? {
            metrics.committed_offset_batches = metrics.committed_offset_batches.saturating_add(1);
        }
        if let Some(commit_start) = commit_start {
            metrics.commit_us = elapsed_us(commit_start);
        }
        if let Some(tick_start) = tick_start {
            metrics.tick_us = elapsed_us(tick_start);
            self.record_tick_metrics(&metrics);
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
    fn raw_default_source<'a>(&'a self, ctx: &ConnectorContext) -> Option<&'a str> {
        if self.message_format != KafkaMessageFormat::FloeJson || !ctx.supports_kafka_raw_batches()
        {
            return None;
        }
        self.config.default_source.as_deref()
    }

    fn stage_raw_or_event_message(
        &self,
        message: &BorrowedMessage<'_>,
        raw_batch: &mut KafkaRawIngestBatch,
        staged_events: &mut Vec<AppendIngestEvent>,
        metrics: &mut KafkaConnectorTickMetrics,
        metrics_enabled: bool,
    ) -> Result<usize> {
        let Some(payload) = message.payload() else {
            tracing::warn!(
                topic = message.topic(),
                partition = message.partition(),
                offset = message.offset(),
                "kafka message missing payload"
            );
            return Ok(0);
        };
        if floe_json_payload_needs_event_parser(payload) {
            return self.stage_message(message, staged_events, metrics, metrics_enabled);
        }

        let message_start = metrics_enabled.then(Instant::now);
        if metrics_enabled {
            metrics.messages = metrics.messages.saturating_add(1);
        }
        let topic = self
            .topic_arcs
            .get(message.topic())
            .cloned()
            .unwrap_or_else(|| Arc::<str>::from(message.topic()));
        raw_batch.records.push(KafkaRawIngestRecord {
            payload: payload.to_vec(),
            topic,
            partition: message.partition(),
            offset: message.offset(),
            event_time_ms: kafka_message_timestamp_ms(message),
        });
        if let Some(message_start) = message_start {
            metrics.events = metrics.events.saturating_add(1);
            metrics.message_us = metrics.message_us.saturating_add(elapsed_us(message_start));
        }
        Ok(1)
    }

    fn stage_message(
        &self,
        message: &BorrowedMessage<'_>,
        staged: &mut Vec<AppendIngestEvent>,
        metrics: &mut KafkaConnectorTickMetrics,
        metrics_enabled: bool,
    ) -> Result<usize> {
        let message_start = metrics_enabled.then(Instant::now);
        if metrics_enabled {
            metrics.messages = metrics.messages.saturating_add(1);
        }

        let parse_start = metrics_enabled.then(Instant::now);
        let mut events = self.handle_message(message)?;
        if let Some(parse_start) = parse_start {
            metrics.parse_us = metrics.parse_us.saturating_add(elapsed_us(parse_start));
        }

        let event_count = events.len();
        staged.append(&mut events);
        if let Some(message_start) = message_start {
            metrics.events = metrics.events.saturating_add(event_count as u64);
            metrics.message_us = metrics.message_us.saturating_add(elapsed_us(message_start));
        }
        Ok(event_count)
    }

    fn record_tick_metrics(&mut self, metrics: &KafkaConnectorTickMetrics) {
        self.tick_counter = self.tick_counter.saturating_add(1);
        self.tick_window.record(metrics);
        if self.tick_counter == 1
            || self
                .tick_counter
                .is_multiple_of(KAFKA_CONNECTOR_TICK_LOG_EVERY)
        {
            let window = std::mem::take(&mut self.tick_window);
            tracing::debug!(
                tick = self.tick_counter,
                window_ticks = window.ticks,
                idle_ticks = window.idle_ticks,
                kafka_messages = window.messages,
                emitted_events = window.events,
                errors = window.errors,
                blocking_polls = window.blocking_polls,
                empty_blocking_polls = window.empty_blocking_polls,
                drain_polls = window.drain_polls,
                empty_drain_polls = window.empty_drain_polls,
                committed_offset_batches = window.committed_offset_batches,
                poll_blocking_us = window.poll_blocking_us,
                poll_drain_us = window.poll_drain_us,
                parse_us = window.parse_us,
                message_us = window.message_us,
                send_us = window.send_us,
                commit_us = window.commit_us,
                tick_us = window.tick_us,
                avg_tick_us = avg_u64(window.tick_us, window.ticks),
                max_tick_us = window.max_tick_us,
                avg_events_per_tick = avg_u64(window.events, window.ticks),
                max_messages_per_tick = self.config.max_messages_per_tick,
                poll_timeout_ms = self.config.poll_timeout.as_millis() as u64,
                "kafka connector tick window metrics"
            );
        }
    }

    async fn commit_offsets_if_requested(&mut self) -> Result<bool> {
        let Some(consumer) = self.consumer.as_ref() else {
            return Ok(false);
        };
        let Some(receiver) = self.config.commit_offsets_rx.as_mut() else {
            return Ok(false);
        };

        let mut latest_commit = None;
        while receiver.has_changed().unwrap_or(false) {
            latest_commit = Some(receiver.borrow_and_update().clone());
        }
        let Some(commit) = latest_commit else {
            return Ok(false);
        };
        if commit.tick_id <= self.last_committed_tick_id {
            return Ok(false);
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
            return Ok(false);
        }

        consumer
            .commit(&tpl, CommitMode::Sync)
            .context("commit kafka offsets after tick commit")?;
        self.last_committed_tick_id = commit.tick_id;
        Ok(true)
    }
}

fn floe_json_payload_needs_event_parser(payload: &[u8]) -> bool {
    let first = payload
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace());
    if first != Some(b'{') {
        return true;
    }
    contains_json_field_name(payload, b"source") && contains_json_field_name(payload, b"data")
}

fn contains_json_field_name(payload: &[u8], field: &[u8]) -> bool {
    let needle_len = field.len().saturating_add(2);
    if payload.len() < needle_len {
        return false;
    }
    payload.windows(needle_len).any(|window| {
        window.first() == Some(&b'"')
            && window.last() == Some(&b'"')
            && &window[1..window.len() - 1] == field
    })
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
