use std::collections::{BTreeMap, HashMap};

use anyhow::{Result, ensure};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceEvent {
    source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resume_token: Option<SourceResumeToken>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_time_ms: Option<u64>,
    #[serde(skip)]
    preencoded_row_key: Option<Vec<u8>>,
    #[serde(skip)]
    source_id: Option<usize>,
    #[serde(skip)]
    kafka_topic: Option<Arc<str>>,
    #[serde(skip)]
    kafka_partition: Option<i32>,
    #[serde(skip)]
    kafka_offset: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceResumeToken {
    Kafka {
        topic: String,
        partition: i32,
        offset: i64,
    },
    PostgresCdc {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        slot: Option<String>,
        lsn: String,
        txid: Option<u64>,
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

impl SourceEvent {
    pub fn new(source: impl Into<String>, payload: Value) -> Self {
        Self {
            source: source.into(),
            payload: Some(payload),
            resume_token: None,
            event_time_ms: None,
            preencoded_row_key: None,
            source_id: None,
            kafka_topic: None,
            kafka_partition: None,
            kafka_offset: None,
        }
    }

    pub fn preencoded(source: impl Into<String>, preencoded_row_key: Vec<u8>) -> Self {
        Self {
            source: source.into(),
            payload: None,
            resume_token: None,
            event_time_ms: None,
            preencoded_row_key: Some(preencoded_row_key),
            source_id: None,
            kafka_topic: None,
            kafka_partition: None,
            kafka_offset: None,
        }
    }

    pub fn preencoded_for_source_id(source_id: usize, preencoded_row_key: Vec<u8>) -> Self {
        Self {
            source: String::new(),
            payload: None,
            resume_token: None,
            event_time_ms: None,
            preencoded_row_key: Some(preencoded_row_key),
            source_id: Some(source_id),
            kafka_topic: None,
            kafka_partition: None,
            kafka_offset: None,
        }
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn source_id(&self) -> Option<usize> {
        self.source_id
    }

    pub fn payload(&self) -> Option<&Value> {
        self.payload.as_ref()
    }

    pub fn resume_token(&self) -> Option<&SourceResumeToken> {
        self.resume_token.as_ref()
    }

    pub fn with_resume_token(mut self, resume_token: SourceResumeToken) -> Self {
        self.resume_token = Some(resume_token);
        self
    }

    pub fn with_kafka_position(mut self, topic: Arc<str>, partition: i32, offset: i64) -> Self {
        self.kafka_topic = Some(topic);
        self.kafka_partition = Some(partition);
        self.kafka_offset = Some(offset);
        self
    }

    pub fn kafka_position(&self) -> Option<(&Arc<str>, i32, i64)> {
        Some((
            self.kafka_topic.as_ref()?,
            self.kafka_partition?,
            self.kafka_offset?,
        ))
    }

    pub fn event_time_ms(&self) -> Option<u64> {
        self.event_time_ms
    }

    pub fn with_event_time_ms(mut self, event_time_ms: u64) -> Self {
        self.event_time_ms = Some(event_time_ms);
        self
    }

    pub fn preencoded_row_key(&self) -> Option<&[u8]> {
        self.preencoded_row_key.as_deref()
    }

    pub fn with_preencoded_row_key(mut self, preencoded_row_key: Vec<u8>) -> Self {
        self.payload = None;
        self.preencoded_row_key = Some(preencoded_row_key);
        self
    }

    pub fn take_preencoded_row_key(&mut self) -> Option<Vec<u8>> {
        self.preencoded_row_key.take()
    }

    pub fn into_payload(self) -> Option<Value> {
        self.payload
    }

    pub fn to_json_value(&self) -> Value {
        json!({
            "source": self.source,
            "data": self.payload.clone().unwrap_or(Value::Null),
        })
    }

    pub fn to_json_string(&self) -> serde_json::Result<String> {
        serde_json::to_string(&self.to_json_value())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn source_event_serializes_to_json() {
        let event = SourceEvent::new("nexmark_person", json!({"id": 42}));
        let serialized = event.to_json_string().expect("json serialization");

        assert!(serialized.contains("nexmark_person"));
        assert!(serialized.contains("id"));
        assert!(serialized.contains("42"));
    }

    #[test]
    fn invalid_source_definition_is_rejected() {
        let err = SourceDefinition::new("", Vec::new()).unwrap_err();
        assert!(err.to_string().contains("source name cannot be empty"));
    }
}
