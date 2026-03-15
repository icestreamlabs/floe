use std::borrow::Cow;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::{BorrowedMessage, Timestamp};
use rdkafka::{Offset, TopicPartitionList};
use serde::Deserialize;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, Visitor};
use serde_json::Value;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::connector::{Connector, ConnectorContext, ConnectorTick, run_connector};
use crate::source::SourceEventSender;
use floe_core::source::{SourceDataType, SourceDefinition, SourceEvent, SourceResumeToken};

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
    consumer: Option<BaseConsumer>,
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

    fn apply_latency_fetch_config(client_config: &mut ClientConfig) {
        client_config
            .set("fetch.wait.max.ms", "1")
            .set("fetch.queue.backoff.ms", "1")
            .set("fetch.min.bytes", "1")
            .set("enable.auto.offset.store", "false");
    }

    fn handle_message(&self, message: &BorrowedMessage<'_>) -> Result<Vec<SourceEvent>> {
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
            KafkaMessageFormat::FloeJson => match parse_direct_floe_json_event(
                payload,
                self.config.default_source.as_deref(),
                message.topic(),
                &self.definitions,
            ) {
                Ok(Some(event)) => Ok(vec![event]),
                Ok(None) => parse_floe_json_events(
                    payload,
                    self.config.default_source.as_deref(),
                    message.topic(),
                ),
                Err(err) => {
                    tracing::debug!(
                        topic = message.topic(),
                        partition = message.partition(),
                        offset = message.offset(),
                        error = %err,
                        "direct kafka floe_json parse fell back to serde_json::Value path"
                    );
                    parse_floe_json_events(
                        payload,
                        self.config.default_source.as_deref(),
                        message.topic(),
                    )
                }
            },
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
        for event in events {
            let mut event = event.with_resume_token(SourceResumeToken::Kafka {
                topic: message.topic().to_string(),
                partition: message.partition(),
                offset: message.offset(),
            });
            if let Some(event_time_ms) = kafka_message_timestamp_ms(message) {
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

fn parse_direct_floe_json_event(
    payload: &[u8],
    default_source: Option<&str>,
    topic: &str,
    definitions: &[SourceDefinition],
) -> Result<Option<SourceEvent>> {
    let mut deserializer = serde_json::Deserializer::from_slice(payload);
    DirectFloeJsonEventSeed {
        default_source,
        topic,
        definitions,
    }
    .deserialize(&mut deserializer)
    .map_err(|err| anyhow::anyhow!("direct floe_json decode failed: {err}"))
}

struct DirectFloeJsonEventSeed<'a> {
    default_source: Option<&'a str>,
    topic: &'a str,
    definitions: &'a [SourceDefinition],
}

impl<'de> DeserializeSeed<'de> for DirectFloeJsonEventSeed<'_> {
    type Value = Option<SourceEvent>;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(DirectFloeJsonEventVisitor { seed: self })
    }
}

struct DirectFloeJsonEventVisitor<'a> {
    seed: DirectFloeJsonEventSeed<'a>,
}

impl<'de> Visitor<'de> for DirectFloeJsonEventVisitor<'_> {
    type Value = Option<SourceEvent>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a single floe_json source/data wrapper object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut source = self.seed.default_source.map(ToOwned::to_owned);
        let mut encoded_row = None;
        let mut event_ts = None;
        while let Some(key) = map.next_key::<Cow<'de, str>>()? {
            match key.as_ref() {
                "source" => {
                    source = Some(map.next_value::<String>()?);
                }
                "data" => {
                    let Some(source_name) = source
                        .as_deref()
                        .or(self.seed.default_source)
                        .or(Some(self.seed.topic))
                    else {
                        let _: IgnoredAny = map.next_value()?;
                        skip_remaining_map(&mut map)?;
                        return Ok(None);
                    };
                    let Some(definition) =
                        lookup_source_definition(self.seed.definitions, source_name)
                    else {
                        let _: IgnoredAny = map.next_value()?;
                        skip_remaining_map(&mut map)?;
                        return Ok(None);
                    };
                    let (encoded, parsed_event_ts) =
                        map.next_value_seed(DirectSourceDataSeed { definition })?;
                    encoded_row = Some(encoded);
                    if event_ts.is_none() {
                        event_ts = parsed_event_ts;
                    }
                }
                _ => {
                    let _: IgnoredAny = map.next_value()?;
                }
            }
        }
        let Some(encoded_row) = encoded_row else {
            return Ok(None);
        };
        let Some(source_name) = source
            .or_else(|| self.seed.default_source.map(str::to_string))
            .or_else(|| Some(self.seed.topic.to_string()))
        else {
            return Ok(None);
        };
        let mut event = SourceEvent::preencoded(source_name, encoded_row);
        if let Some(event_time_ms) = event_ts {
            event = event.with_event_time_ms(event_time_ms);
        }
        Ok(Some(event))
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(None)
    }
}

struct DirectSourceDataSeed<'a> {
    definition: &'a SourceDefinition,
}

impl<'de> DeserializeSeed<'de> for DirectSourceDataSeed<'_> {
    type Value = (Vec<u8>, Option<u64>);

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(DirectSourceDataVisitor {
            definition: self.definition,
        })
    }
}

struct DirectSourceDataVisitor<'a> {
    definition: &'a SourceDefinition,
}

impl<'de> Visitor<'de> for DirectSourceDataVisitor<'_> {
    type Value = (Vec<u8>, Option<u64>);

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a source data object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut encoded_columns = vec![None; self.definition.columns().len()];
        let mut event_ts = None;
        while let Some(key) = map.next_key::<Cow<'de, str>>()? {
            if let Some((idx, column)) = lookup_source_column(self.definition, key.as_ref()) {
                let (encoded, parsed_event_ts) = map.next_value_seed(DirectSourceColumnSeed {
                    data_type: column.data_type(),
                    nullable: column.nullable(),
                    field_name: column.name(),
                })?;
                encoded_columns[idx] = Some(encoded);
                if event_ts.is_none() {
                    event_ts = parsed_event_ts;
                }
            } else {
                let _: IgnoredAny = map.next_value()?;
            }
        }

        let mut row = Vec::with_capacity(64);
        let count = u32::try_from(self.definition.columns().len())
            .map_err(|_| de::Error::custom("too many source columns to encode"))?;
        row.extend_from_slice(&count.to_le_bytes());
        for (idx, column) in self.definition.columns().iter().enumerate() {
            if let Some(encoded) = encoded_columns[idx].take() {
                row.extend_from_slice(&encoded);
            } else if column.nullable() {
                encode_typed_null(&mut row, column.data_type());
            } else {
                return Err(de::Error::custom(format!(
                    "missing field '{}' in source payload",
                    column.name()
                )));
            }
        }
        Ok((row, event_ts))
    }
}

struct DirectSourceColumnSeed<'a> {
    data_type: &'a SourceDataType,
    nullable: bool,
    field_name: &'a str,
}

impl<'de> DeserializeSeed<'de> for DirectSourceColumnSeed<'_> {
    type Value = (Vec<u8>, Option<u64>);

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match self.data_type {
            SourceDataType::Int64 => {
                let value = Option::<i64>::deserialize(deserializer)?;
                encode_i64_column(value, self.nullable, self.field_name, false)
                    .map(|encoded| (encoded, None))
            }
            SourceDataType::TimestampMillis => {
                let value = Option::<i64>::deserialize(deserializer)?;
                let encoded = encode_i64_column(value, self.nullable, self.field_name, true)?;
                let event_ts = value.filter(|value| *value >= 0).map(|value| value as u64);
                Ok((encoded, event_ts))
            }
            SourceDataType::Utf8 => {
                let value = Option::<Cow<'de, str>>::deserialize(deserializer)?;
                match value {
                    Some(value) => {
                        let bytes = value.as_bytes();
                        let len = u32::try_from(bytes.len())
                            .map_err(|_| de::Error::custom("utf8 value too large for MV key"))?;
                        let mut encoded = Vec::with_capacity(1 + 4 + bytes.len());
                        encoded.push(0x02);
                        encoded.extend_from_slice(&len.to_le_bytes());
                        encoded.extend_from_slice(bytes);
                        Ok((encoded, None))
                    }
                    None if self.nullable => Ok((vec![0x06], None)),
                    None => Err(de::Error::custom(format!(
                        "null value violates non-nullable column '{}'",
                        self.field_name
                    ))),
                }
            }
            SourceDataType::Bool => {
                let value = Option::<bool>::deserialize(deserializer)?;
                match value {
                    Some(value) => Ok((vec![0x04, if value { 1 } else { 0 }], None)),
                    None if self.nullable => Ok((vec![0x08], None)),
                    None => Err(de::Error::custom(format!(
                        "null value violates non-nullable column '{}'",
                        self.field_name
                    ))),
                }
            }
        }
    }
}

fn encode_i64_column<E>(
    value: Option<i64>,
    nullable: bool,
    field_name: &str,
    timestamp: bool,
) -> std::result::Result<Vec<u8>, E>
where
    E: de::Error,
{
    match value {
        Some(value) => {
            let mut encoded = Vec::with_capacity(9);
            encoded.push(if timestamp { 0x03 } else { 0x01 });
            encoded.extend_from_slice(&value.to_le_bytes());
            Ok(encoded)
        }
        None if nullable => Ok(vec![if timestamp { 0x07 } else { 0x05 }]),
        None => Err(E::custom(format!(
            "null value violates non-nullable column '{}'",
            field_name
        ))),
    }
}

fn encode_typed_null(buf: &mut Vec<u8>, data_type: &SourceDataType) {
    match data_type {
        SourceDataType::Int64 => buf.push(0x05),
        SourceDataType::Utf8 => buf.push(0x06),
        SourceDataType::TimestampMillis => buf.push(0x07),
        SourceDataType::Bool => buf.push(0x08),
    }
}

fn lookup_source_definition<'a>(
    definitions: &'a [SourceDefinition],
    source: &str,
) -> Option<&'a SourceDefinition> {
    definitions
        .iter()
        .find(|definition| definition.name() == source)
}

fn lookup_source_column<'a>(
    definition: &'a SourceDefinition,
    field_name: &str,
) -> Option<(usize, &'a floe_core::source::SourceColumn)> {
    definition
        .columns()
        .iter()
        .enumerate()
        .find(|(_, column)| column.name() == field_name)
}

fn skip_remaining_map<'de, A>(map: &mut A) -> std::result::Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
    Ok(())
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
    use floe_core::source::{SourceColumn, SourceDataType};
    use floe_executor::SourceRowDecoder;
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

    #[test]
    fn direct_floe_json_parse_matches_source_decoder_encoding() {
        let definition = SourceDefinition::new(
            "nexmark_bid",
            vec![
                SourceColumn::new("auction", SourceDataType::Int64),
                SourceColumn::new("bidder", SourceDataType::Int64),
                SourceColumn::new("price", SourceDataType::Int64),
                SourceColumn::new("channel", SourceDataType::Utf8),
                SourceColumn::new("url", SourceDataType::Utf8),
                SourceColumn::new("date_time", SourceDataType::TimestampMillis),
                SourceColumn::new("extra", SourceDataType::Utf8),
            ],
        )
        .expect("definition");
        let payload = br#"{"source":"nexmark_bid","data":{"auction":100,"bidder":42,"price":99,"channel":"web","url":"http://example.com","date_time":1700000000000,"extra":"bid_extra"}}"#;

        let direct = parse_direct_floe_json_event(payload, None, "topic", &[definition.clone()])
            .expect("direct parse")
            .expect("direct event");
        let expected_event = SourceEvent::new(
            "nexmark_bid",
            json!({
                "auction": 100,
                "bidder": 42,
                "price": 99,
                "channel": "web",
                "url": "http://example.com",
                "date_time": 1700000000000_i64,
                "extra": "bid_extra"
            }),
        );
        let decoder = SourceRowDecoder::new(definition);
        let (expected_encoded, expected_ts) = decoder
            .encode_row_key(&expected_event)
            .expect("expected encoding");

        assert_eq!(
            direct.preencoded_row_key(),
            Some(expected_encoded.as_slice())
        );
        assert_eq!(direct.event_time_ms(), expected_ts);
        assert_eq!(direct.source(), "nexmark_bid");
    }
}
