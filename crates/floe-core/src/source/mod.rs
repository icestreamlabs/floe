use std::collections::{BTreeMap, HashMap};

use anyhow::{Result, ensure};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::catalog::ColumnType;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SourceDataType {
    Int64,
    Bool,
    Utf8,
    TimestampMillis,
    DateDays,
    Decimal128 { precision: u8, scale: i8 },
    Numeric,
}

impl SourceDataType {
    pub fn column_type(&self) -> ColumnType {
        match self {
            SourceDataType::Int64 => ColumnType::Int64,
            SourceDataType::Bool => ColumnType::Bool,
            SourceDataType::Utf8 => ColumnType::Utf8,
            SourceDataType::TimestampMillis => ColumnType::TimestampMillis,
            SourceDataType::DateDays => ColumnType::DateDays,
            SourceDataType::Decimal128 { precision, scale } => ColumnType::Decimal128 {
                precision: *precision,
                scale: *scale,
            },
            SourceDataType::Numeric => ColumnType::Numeric,
        }
    }

    pub fn arrow_type(&self) -> DataType {
        self.column_type().arrow_type()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceColumn {
    name: String,
    data_type: SourceDataType,
    #[serde(default = "default_nullable")]
    nullable: bool,
}

impl SourceColumn {
    pub fn new(name: impl Into<String>, data_type: SourceDataType) -> Self {
        Self::new_nullable(name, data_type, true)
    }

    pub fn new_nullable(
        name: impl Into<String>,
        data_type: SourceDataType,
        nullable: bool,
    ) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &SourceDataType {
        &self.data_type
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceDefinition {
    name: String,
    columns: Vec<SourceColumn>,
    properties: BTreeMap<String, String>,
}

impl SourceDefinition {
    pub fn new(name: impl Into<String>, columns: Vec<SourceColumn>) -> Result<Self> {
        let name = name.into();
        ensure!(!name.trim().is_empty(), "source name cannot be empty");
        ensure!(
            !columns.is_empty(),
            "source {} must declare at least one column",
            name
        );
        Ok(Self {
            name,
            columns,
            properties: BTreeMap::new(),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[SourceColumn] {
        &self.columns
    }

    pub fn properties(&self) -> &BTreeMap<String, String> {
        &self.properties
    }

    pub fn property(&self, key: &str) -> Option<&str> {
        self.properties.get(key).map(|value| value.as_str())
    }

    pub fn to_arrow_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|column| {
                Field::new(
                    column.name(),
                    column.data_type().arrow_type(),
                    column.nullable(),
                )
            })
            .collect();
        SchemaRef::new(Schema::new(fields))
    }

    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    pub fn set_property(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.properties.insert(key.into(), value.into());
    }
}

fn default_nullable() -> bool {
    true
}

#[derive(Default, Debug, Clone)]
pub struct SourceRegistry {
    definitions: Vec<SourceDefinition>,
    by_name: HashMap<String, usize>,
}

impl SourceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, definition: SourceDefinition) {
        let name = definition.name().to_string();
        if let Some(idx) = self.by_name.get(&name).copied() {
            self.definitions[idx] = definition;
        } else {
            self.by_name.insert(name, self.definitions.len());
            self.definitions.push(definition);
        }
    }

    pub fn extend<I>(&mut self, definitions: I)
    where
        I: IntoIterator<Item = SourceDefinition>,
    {
        for definition in definitions {
            self.register(definition);
        }
    }

    pub fn definitions(&self) -> &[SourceDefinition] {
        &self.definitions
    }

    pub fn get(&self, name: &str) -> Option<&SourceDefinition> {
        self.by_name
            .get(name)
            .and_then(|idx| self.definitions.get(*idx))
    }

    pub fn contains(&self, name: &str) -> bool {
        self.by_name.contains_key(name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AppendIngestEvent {
    source: String,
    payload: AppendIngestPayload,
    metadata: AppendIngestMetadata,
}

#[derive(Debug, Clone, PartialEq)]
enum AppendIngestPayload {
    Json(Value),
    Empty,
}

#[derive(Debug, Clone, PartialEq, Default)]
struct AppendIngestMetadata {
    resume_token: Option<AppendIngestResumeToken>,
    event_time_ms: Option<u64>,
    connector_position: Option<AppendIngestConnectorPosition>,
}

#[derive(Debug, Clone, PartialEq)]
enum AppendIngestConnectorPosition {
    Kafka {
        topic: Arc<str>,
        partition: i32,
        offset: i64,
    },
}

#[derive(Serialize, Deserialize)]
struct AppendIngestEventSerde {
    source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resume_token: Option<AppendIngestResumeToken>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_time_ms: Option<u64>,
}

impl Serialize for AppendIngestEvent {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        AppendIngestEventSerde {
            source: self.source.clone(),
            payload: match &self.payload {
                AppendIngestPayload::Json(payload) => Some(payload.clone()),
                AppendIngestPayload::Empty => None,
            },
            resume_token: self.metadata.resume_token.clone(),
            event_time_ms: self.metadata.event_time_ms,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AppendIngestEvent {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let decoded = AppendIngestEventSerde::deserialize(deserializer)?;
        Ok(Self {
            source: decoded.source,
            payload: decoded
                .payload
                .map(AppendIngestPayload::Json)
                .unwrap_or(AppendIngestPayload::Empty),
            metadata: AppendIngestMetadata {
                resume_token: decoded.resume_token,
                event_time_ms: decoded.event_time_ms,
                connector_position: None,
            },
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AppendIngestResumeToken {
    Kafka {
        topic: String,
        partition: i32,
        offset: i64,
    },
    File {
        cursor: u64,
    },
    Generator {
        position: u64,
    },
    ObjectStore {
        cursor: u64,
    },
}

/// Append-style row ingest envelope for file, object-store, generator, Kafka,
/// HTTP, and connector-SDK inputs. Native CDC paths use transaction/change
/// batches instead of this type.
impl AppendIngestEvent {
    pub fn new(source: impl Into<String>, payload: Value) -> Self {
        Self {
            source: source.into(),
            payload: AppendIngestPayload::Json(payload),
            metadata: AppendIngestMetadata::default(),
        }
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn payload(&self) -> Option<&Value> {
        match &self.payload {
            AppendIngestPayload::Json(payload) => Some(payload),
            AppendIngestPayload::Empty => None,
        }
    }

    pub fn resume_token(&self) -> Option<&AppendIngestResumeToken> {
        self.metadata.resume_token.as_ref()
    }

    pub fn with_resume_token(mut self, resume_token: AppendIngestResumeToken) -> Self {
        self.metadata.resume_token = Some(resume_token);
        self
    }

    pub fn with_kafka_position(mut self, topic: Arc<str>, partition: i32, offset: i64) -> Self {
        self.metadata.connector_position = Some(AppendIngestConnectorPosition::Kafka {
            topic,
            partition,
            offset,
        });
        self
    }

    pub fn kafka_position(&self) -> Option<(&Arc<str>, i32, i64)> {
        match self.metadata.connector_position.as_ref()? {
            AppendIngestConnectorPosition::Kafka {
                topic,
                partition,
                offset,
            } => Some((topic, *partition, *offset)),
        }
    }

    pub fn event_time_ms(&self) -> Option<u64> {
        self.metadata.event_time_ms
    }

    pub fn with_event_time_ms(mut self, event_time_ms: u64) -> Self {
        self.metadata.event_time_ms = Some(event_time_ms);
        self
    }

    pub fn into_payload(self) -> Option<Value> {
        match self.payload {
            AppendIngestPayload::Json(payload) => Some(payload),
            AppendIngestPayload::Empty => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn source_definition_builds_arrow_schema() {
        let definition = SourceDefinition::new(
            "nexmark_person",
            vec![
                SourceColumn::new("id", SourceDataType::Int64),
                SourceColumn::new("active", SourceDataType::Bool),
                SourceColumn::new("name", SourceDataType::Utf8),
                SourceColumn::new("date_time", SourceDataType::TimestampMillis),
            ],
        )
        .expect("valid definition");

        let schema = definition.to_arrow_schema();
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        assert_eq!(
            schema.field(3).data_type(),
            &DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
        );
    }

    #[test]
    fn properties_are_preserved() {
        let definition = SourceDefinition::new(
            "nexmark_person",
            vec![SourceColumn::new("id", SourceDataType::Int64)],
        )
        .unwrap()
        .with_property("connector", "nexmark")
        .with_property("entity", "person");

        assert_eq!(definition.property("connector"), Some("nexmark"));
        assert_eq!(definition.property("entity"), Some("person"));
        assert_eq!(definition.properties().len(), 2);
    }

    #[test]
    fn append_ingest_event_serializes_to_json() {
        let event = AppendIngestEvent::new("nexmark_person", json!({"id": 42}));
        let serialized = serde_json::to_string(&event).expect("json serialization");

        assert!(serialized.contains("nexmark_person"));
        assert!(serialized.contains("id"));
        assert!(serialized.contains("42"));
    }

    #[test]
    fn append_ingest_payload_separates_json_and_empty_events() {
        let json_event = AppendIngestEvent::new("orders", json!({"id": 7}));
        assert_eq!(json_event.payload(), Some(&json!({"id": 7})));
        assert_eq!(json_event.into_payload(), Some(json!({"id": 7})));

        let empty_event: AppendIngestEvent =
            serde_json::from_value(json!({"source": "orders"})).expect("empty append event");
        assert_eq!(empty_event.source(), "orders");
        assert!(empty_event.payload().is_none());
        assert!(empty_event.into_payload().is_none());
    }

    #[test]
    fn append_ingest_event_serde_keeps_external_shape() {
        let event = AppendIngestEvent::new("orders", json!({"id": 7}))
            .with_resume_token(AppendIngestResumeToken::File { cursor: 11 })
            .with_event_time_ms(99);

        let encoded = serde_json::to_value(&event).expect("serialize append ingest event");
        assert_eq!(encoded["source"], "orders");
        assert_eq!(encoded["payload"], json!({"id": 7}));
        assert_eq!(encoded["event_time_ms"], 99);

        let decoded: AppendIngestEvent =
            serde_json::from_value(encoded).expect("decode append ingest event");
        assert_eq!(decoded.source(), "orders");
        assert_eq!(decoded.payload(), Some(&json!({"id": 7})));
        assert_eq!(decoded.event_time_ms(), Some(99));
        assert!(matches!(
            decoded.resume_token(),
            Some(AppendIngestResumeToken::File { cursor: 11 })
        ));
    }

    #[test]
    fn invalid_source_definition_is_rejected() {
        let err = SourceDefinition::new("", Vec::new()).unwrap_err();
        assert!(err.to_string().contains("source name cannot be empty"));
    }
}
