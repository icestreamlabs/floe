use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder, Int64Builder, RecordBatch,
    StringBuilder, TimestampMillisecondBuilder,
};
use floe_core::decimal::parse_decimal_text_to_i128;
use floe_core::source::SourceColumn;
use floe_core::source::{AppendIngestEvent, SourceDataType, SourceDefinition};
use serde::Deserializer;
use serde::de::{Error as DeError, IgnoredAny, MapAccess, Visitor};
use serde_json::Value;

use crate::stream_types::Timestamp;

trait PayloadRefExt<'a> {
    fn require_payload(self, message: &'static str) -> Result<&'a Value>;
}

impl<'a> PayloadRefExt<'a> for &'a Value {
    fn require_payload(self, _message: &'static str) -> Result<&'a Value> {
        Ok(self)
    }
}

impl<'a> PayloadRefExt<'a> for Option<&'a Value> {
    fn require_payload(self, message: &'static str) -> Result<&'a Value> {
        self.ok_or_else(|| anyhow!(message))
    }
}

pub struct SourceArrowBatchBuilder {
    definition: SourceDefinition,
    builders: Vec<Option<SourceArrowColumnBuilder>>,
    column_index_by_name: HashMap<String, usize>,
    execution_required_columns: Option<Arc<[bool]>>,
    batch_mode: SourceArrowBatchMode,
    row_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceArrowBatchMode {
    ExecutionAndQuery,
    ExecutionOnly,
}

impl SourceArrowBatchMode {
    fn includes_query_batch(self) -> bool {
        matches!(self, Self::ExecutionAndQuery)
    }
}

#[derive(Debug, Clone)]
pub enum SourceArrowBatches {
    ExecutionAndQuery {
        execution: RecordBatch,
        query: RecordBatch,
    },
    ExecutionOnly {
        execution: RecordBatch,
    },
}

impl SourceArrowBatches {
    pub fn execution(&self) -> &RecordBatch {
        match self {
            Self::ExecutionAndQuery { execution, .. } | Self::ExecutionOnly { execution } => {
                execution
            }
        }
    }

    pub fn query(&self) -> Option<&RecordBatch> {
        match self {
            Self::ExecutionAndQuery { query, .. } => Some(query),
            Self::ExecutionOnly { .. } => None,
        }
    }

    pub fn into_parts(self) -> (RecordBatch, Option<RecordBatch>) {
        match self {
            Self::ExecutionAndQuery { execution, query } => (execution, Some(query)),
            Self::ExecutionOnly { execution } => (execution, None),
        }
    }
}

impl SourceArrowBatchBuilder {
    pub fn new(definition: SourceDefinition, capacity: usize) -> Self {
        Self::new_with_execution_required_columns(definition, capacity, None)
    }

    pub fn new_with_execution_required_columns(
        definition: SourceDefinition,
        capacity: usize,
        execution_required_columns: Option<Arc<[bool]>>,
    ) -> Self {
        Self::new_with_execution_required_columns_and_batch_mode(
            definition,
            capacity,
            execution_required_columns,
            SourceArrowBatchMode::ExecutionAndQuery,
        )
    }

    pub fn new_with_execution_required_columns_and_batch_mode(
        definition: SourceDefinition,
        capacity: usize,
        execution_required_columns: Option<Arc<[bool]>>,
        batch_mode: SourceArrowBatchMode,
    ) -> Self {
        let execution_required_columns = execution_required_columns
            .filter(|required_columns| !required_columns.iter().all(|required| *required));
        let includes_query_batch = batch_mode.includes_query_batch();
        let builders = definition
            .columns()
            .iter()
            .enumerate()
            .map(|(idx, column)| {
                let required_for_execution = execution_required_columns
                    .as_ref()
                    .and_then(|required_columns| required_columns.get(idx))
                    .copied()
                    .unwrap_or(true);
                (includes_query_batch || required_for_execution)
                    .then(|| SourceArrowColumnBuilder::new(column.data_type(), capacity))
            })
            .collect();
        let column_index_by_name = definition
            .columns()
            .iter()
            .enumerate()
            .map(|(idx, column)| (column.name().to_string(), idx))
            .collect();
        Self {
            definition,
            builders,
            column_index_by_name,
            execution_required_columns,
            batch_mode,
            row_count: 0,
        }
    }

    pub fn append_event(&mut self, event: &AppendIngestEvent) -> Result<Option<Timestamp>> {
        if event.source() != self.definition.name() {
            bail!(
                "event source {} does not match definition {}",
                event.source(),
                self.definition.name()
            );
        }
        let payload = AppendIngestEvent::payload(event)
            .require_payload("source payload must be present for vectorized events")?;
        let object = payload
            .as_object()
            .context("source payload must be a JSON object")?;
        let mut event_ts = None;
        for (builder, column) in self.builders.iter_mut().zip(self.definition.columns()) {
            let value = object.get(column.name());
            if let Some(builder) = builder.as_mut() {
                builder.append_json_value(column, value, &mut event_ts)?;
            } else {
                observe_skipped_event_timestamp(column, value, &mut event_ts);
            }
        }
        self.row_count += 1;
        Ok(event_ts)
    }

    pub fn append_json_payload(
        &mut self,
        source: &str,
        payload: &[u8],
    ) -> Result<Option<Timestamp>> {
        if source != self.definition.name() {
            bail!(
                "event source {} does not match definition {}",
                source,
                self.definition.name()
            );
        }
        let visitor = SourceJsonObjectVisitor {
            definition: &self.definition,
            builders: &mut self.builders,
            column_index_by_name: &self.column_index_by_name,
        };
        let mut deserializer = serde_json::Deserializer::from_slice(payload);
        let event_ts = deserializer
            .deserialize_any(visitor)
            .context("source payload must be a JSON object")?;
        deserializer
            .end()
            .context("source payload has trailing data after JSON object")?;
        self.row_count += 1;
        Ok(event_ts)
    }

    pub fn finish(&mut self) -> Result<Option<SourceArrowBatches>> {
        if self.batch_mode.includes_query_batch() {
            let Some(query) = self.finish_query_batch()? else {
                return Ok(None);
            };
            let execution = execution_batch_for_required_columns(
                &self.definition,
                &query,
                self.execution_required_columns.as_ref(),
            )?;
            return Ok(Some(SourceArrowBatches::ExecutionAndQuery {
                execution,
                query,
            }));
        }

        let Some(execution) = self.finish_execution_batch()? else {
            return Ok(None);
        };
        Ok(Some(SourceArrowBatches::ExecutionOnly { execution }))
    }

    pub fn finish_query_batch(&mut self) -> Result<Option<RecordBatch>> {
        if !self.batch_mode.includes_query_batch() {
            bail!(
                "source '{}' builder was configured without query batches",
                self.definition.name()
            );
        }
        self.finish_batch(FinishBatchMode::Query)
    }

    fn finish_execution_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.finish_batch(FinishBatchMode::Execution)
    }

    fn finish_batch(&mut self, mode: FinishBatchMode) -> Result<Option<RecordBatch>> {
        if self.row_count == 0 {
            return Ok(None);
        }
        let mut arrays = Vec::with_capacity(self.definition.columns().len());
        for (idx, (builder, column)) in self
            .builders
            .iter_mut()
            .zip(self.definition.columns())
            .enumerate()
        {
            let array = match builder.as_mut() {
                Some(builder) => builder.finish()?,
                None if mode == FinishBatchMode::Execution => {
                    skipped_arrow_column(column, self.row_count)?
                }
                None => bail!(
                    "source '{}' query batch is missing builder for column {}",
                    self.definition.name(),
                    idx
                ),
            };
            arrays.push(array);
        }
        let batch = RecordBatch::try_new(self.definition.to_arrow_schema(), arrays)?;
        self.row_count = 0;
        Ok(Some(batch))
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FinishBatchMode {
    Execution,
    Query,
}

fn execution_batch_for_required_columns(
    definition: &SourceDefinition,
    batch: &RecordBatch,
    required_columns: Option<&Arc<[bool]>>,
) -> Result<RecordBatch> {
    let Some(required_columns) = required_columns else {
        return Ok(batch.clone());
    };
    if required_columns.iter().all(|required| *required) {
        return Ok(batch.clone());
    }
    if batch.schema().as_ref() != definition.to_arrow_schema().as_ref() {
        bail!(
            "Arrow batch schema does not match definition '{}'",
            definition.name()
        );
    }

    let row_count = batch.num_rows();
    let mut columns = Vec::with_capacity(definition.columns().len());
    for (idx, column) in definition.columns().iter().enumerate() {
        if required_columns.get(idx).copied().unwrap_or(true) {
            columns.push(Arc::clone(batch.column(idx)));
            continue;
        }
        columns.push(skipped_arrow_column(column, row_count)?);
    }
    Ok(RecordBatch::try_new(definition.to_arrow_schema(), columns)?)
}

enum SourceArrowColumnBuilder {
    Int64(Int64Builder),
    Bool(BooleanBuilder),
    Utf8(StringBuilder),
    TimestampMillis(TimestampMillisecondBuilder),
    DateDays(Date32Builder),
    Decimal128(Decimal128Builder),
    Numeric(StringBuilder),
}

fn skipped_arrow_column(column: &SourceColumn, row_count: usize) -> Result<ArrayRef> {
    let mut builder = SourceArrowColumnBuilder::new(column.data_type(), row_count);
    for _ in 0..row_count {
        builder.append_skipped_value(column)?;
    }
    builder.finish()
}

fn observe_skipped_event_timestamp(
    column: &SourceColumn,
    value: Option<&Value>,
    event_ts: &mut Option<Timestamp>,
) {
    if event_ts.is_some() || !matches!(column.data_type(), SourceDataType::TimestampMillis) {
        return;
    }
    let Some(value) = value else {
        return;
    };
    let Some(number) = value.as_i64() else {
        return;
    };
    if number >= 0 {
        *event_ts = Some(number as u64);
    }
}

struct SourceJsonObjectVisitor<'a> {
    definition: &'a SourceDefinition,
    builders: &'a mut [Option<SourceArrowColumnBuilder>],
    column_index_by_name: &'a HashMap<String, usize>,
}

impl<'de> Visitor<'de> for SourceJsonObjectVisitor<'_> {
    type Value = Option<Timestamp>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<M>(self, mut map: M) -> std::result::Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut seen = vec![false; self.definition.columns().len()];
        let mut event_ts = None;
        while let Some(key) = map.next_key::<Cow<'de, str>>()? {
            let Some(&idx) = self.column_index_by_name.get(key.as_ref()) else {
                let _: IgnoredAny = map.next_value()?;
                continue;
            };
            seen[idx] = true;
            let column = &self.definition.columns()[idx];
            match self.builders[idx].as_mut() {
                Some(builder) => {
                    builder.append_deserialized_json_value(column, &mut map, &mut event_ts)?
                }
                None => observe_skipped_deserialized_json_value(column, &mut map, &mut event_ts)?,
            }
        }

        for (idx, column) in self.definition.columns().iter().enumerate() {
            if seen[idx] {
                continue;
            }
            if let Some(builder) = self.builders[idx].as_mut() {
                if column.nullable() {
                    builder.append_null().map_err(M::Error::custom)?;
                } else {
                    return Err(M::Error::custom(format!(
                        "missing field '{}' in source payload",
                        column.name()
                    )));
                }
            }
        }
        Ok(event_ts)
    }
}

fn observe_skipped_deserialized_json_value<'de, M>(
    column: &SourceColumn,
    map: &mut M,
    event_ts: &mut Option<Timestamp>,
) -> std::result::Result<(), M::Error>
where
    M: MapAccess<'de>,
{
    if event_ts.is_none() && matches!(column.data_type(), SourceDataType::TimestampMillis) {
        let value = map.next_value::<Option<i64>>()?;
        if let Some(number) = value
            && number >= 0
        {
            *event_ts = Some(number as u64);
        }
        return Ok(());
    }
    let _: IgnoredAny = map.next_value()?;
    Ok(())
}

fn non_nullable_null_error<'de, M>(column: &SourceColumn) -> M::Error
where
    M: MapAccess<'de>,
{
    M::Error::custom(format!(
        "null value violates non-nullable column '{}'",
        column.name()
    ))
}

impl SourceArrowColumnBuilder {
    fn new(data_type: &SourceDataType, capacity: usize) -> Self {
        match data_type {
            SourceDataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            SourceDataType::Bool => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            SourceDataType::Utf8 => Self::Utf8(StringBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(16),
            )),
            SourceDataType::TimestampMillis => {
                Self::TimestampMillis(TimestampMillisecondBuilder::with_capacity(capacity))
            }
            SourceDataType::DateDays => Self::DateDays(Date32Builder::with_capacity(capacity)),
            SourceDataType::Decimal128 { precision, scale } => {
                let data_type =
                    datafusion::arrow::datatypes::DataType::Decimal128(*precision, *scale);
                Self::Decimal128(
                    Decimal128Builder::with_capacity(capacity).with_data_type(data_type),
                )
            }
            SourceDataType::Numeric => Self::Numeric(StringBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(16),
            )),
        }
    }

    fn append_deserialized_json_value<'de, M>(
        &mut self,
        column: &SourceColumn,
        map: &mut M,
        event_ts: &mut Option<Timestamp>,
    ) -> std::result::Result<(), M::Error>
    where
        M: MapAccess<'de>,
    {
        match (column.data_type(), self) {
            (SourceDataType::Int64, Self::Int64(builder)) => {
                match map.next_value::<Option<i64>>()? {
                    Some(value) => builder.append_value(value),
                    None if column.nullable() => builder.append_null(),
                    None => return Err(non_nullable_null_error::<M>(column)),
                }
                Ok(())
            }
            (SourceDataType::Bool, Self::Bool(builder)) => {
                match map.next_value::<Option<bool>>()? {
                    Some(value) => builder.append_value(value),
                    None if column.nullable() => builder.append_null(),
                    None => return Err(non_nullable_null_error::<M>(column)),
                }
                Ok(())
            }
            (SourceDataType::Utf8, Self::Utf8(builder)) => {
                match map.next_value::<Option<String>>()? {
                    Some(value) => builder.append_value(value),
                    None if column.nullable() => builder.append_null(),
                    None => return Err(non_nullable_null_error::<M>(column)),
                }
                Ok(())
            }
            (SourceDataType::TimestampMillis, Self::TimestampMillis(builder)) => {
                match map.next_value::<Option<i64>>()? {
                    Some(value) => {
                        builder.append_value(value);
                        if event_ts.is_none() && value >= 0 {
                            *event_ts = Some(value as u64);
                        }
                    }
                    None if column.nullable() => builder.append_null(),
                    None => return Err(non_nullable_null_error::<M>(column)),
                }
                Ok(())
            }
            (SourceDataType::DateDays, Self::DateDays(builder)) => {
                match map.next_value::<Option<i64>>()? {
                    Some(value) => {
                        let value = i32::try_from(value).map_err(|_| {
                            M::Error::custom(format!(
                                "date days value out of range for '{}': {value}",
                                column.name()
                            ))
                        })?;
                        builder.append_value(value);
                    }
                    None if column.nullable() => builder.append_null(),
                    None => return Err(non_nullable_null_error::<M>(column)),
                }
                Ok(())
            }
            (SourceDataType::Decimal128 { scale, .. }, Self::Decimal128(builder)) => {
                let value = map.next_value::<Value>()?;
                if value.is_null() {
                    if column.nullable() {
                        builder.append_null();
                        return Ok(());
                    }
                    return Err(non_nullable_null_error::<M>(column));
                }
                let number = match &value {
                    Value::String(value) => parse_decimal_text_to_i128(value, *scale),
                    Value::Number(value) => parse_decimal_text_to_i128(&value.to_string(), *scale),
                    other => {
                        return Err(M::Error::custom(format!(
                            "expected decimal string or JSON number, found {other}"
                        )));
                    }
                }
                .map_err(M::Error::custom)?;
                builder.append_value(number);
                Ok(())
            }
            (SourceDataType::Numeric, Self::Numeric(builder)) => {
                let value = map.next_value::<Value>()?;
                if value.is_null() {
                    if column.nullable() {
                        builder.append_null();
                        return Ok(());
                    }
                    return Err(non_nullable_null_error::<M>(column));
                }
                match &value {
                    Value::String(value) => builder.append_value(value),
                    Value::Number(_) => builder.append_value(value.to_string()),
                    other => {
                        return Err(M::Error::custom(format!(
                            "expected numeric string or JSON number, found {other}"
                        )));
                    }
                }
                Ok(())
            }
            (data_type, _) => Err(M::Error::custom(format!(
                "source column '{}' does not match Arrow builder for {data_type:?}",
                column.name()
            ))),
        }
    }

    fn append_json_value(
        &mut self,
        column: &SourceColumn,
        value: Option<&Value>,
        event_ts: &mut Option<Timestamp>,
    ) -> Result<()> {
        match value {
            None if column.nullable() => self.append_null(),
            None => bail!("missing field '{}' in source payload", column.name()),
            Some(value) if value.is_null() => {
                if column.nullable() {
                    return self.append_null();
                }
                bail!(
                    "null value violates non-nullable column '{}'",
                    column.name()
                );
            }
            Some(value) => match (column.data_type(), self) {
                (SourceDataType::Int64, Self::Int64(builder)) => {
                    builder.append_value(value.as_i64().with_context(|| {
                        format!(
                            "expected integer value for '{}', found {value}",
                            column.name()
                        )
                    })?);
                    Ok(())
                }
                (SourceDataType::Bool, Self::Bool(builder)) => {
                    builder.append_value(value.as_bool().with_context(|| {
                        format!(
                            "expected boolean value for '{}', found {value}",
                            column.name()
                        )
                    })?);
                    Ok(())
                }
                (SourceDataType::Utf8, Self::Utf8(builder)) => {
                    builder.append_value(value.as_str().with_context(|| {
                        format!(
                            "expected string value for '{}', found {value}",
                            column.name()
                        )
                    })?);
                    Ok(())
                }
                (SourceDataType::TimestampMillis, Self::TimestampMillis(builder)) => {
                    let number = value.as_i64().with_context(|| {
                        format!(
                            "expected integer timestamp for '{}', found {value}",
                            column.name()
                        )
                    })?;
                    builder.append_value(number);
                    if event_ts.is_none() && number >= 0 {
                        *event_ts = Some(number as u64);
                    }
                    Ok(())
                }
                (SourceDataType::DateDays, Self::DateDays(builder)) => {
                    let days = value.as_i64().with_context(|| {
                        format!(
                            "expected integer date days for '{}', found {value}",
                            column.name()
                        )
                    })?;
                    builder.append_value(i32::try_from(days).with_context(|| {
                        format!(
                            "date days value out of range for '{}': {value}",
                            column.name()
                        )
                    })?);
                    Ok(())
                }
                (SourceDataType::Decimal128 { scale, .. }, Self::Decimal128(builder)) => {
                    let number = match value {
                        Value::String(value) => parse_decimal_text_to_i128(value, *scale)?,
                        Value::Number(value) => {
                            parse_decimal_text_to_i128(&value.to_string(), *scale)?
                        }
                        other => bail!("expected decimal string or JSON number, found {other}"),
                    };
                    builder.append_value(number);
                    Ok(())
                }
                (SourceDataType::Numeric, Self::Numeric(builder)) => {
                    let number = match value {
                        Value::String(value) => value.as_str(),
                        Value::Number(_) => {
                            builder.append_value(value.to_string());
                            return Ok(());
                        }
                        other => bail!("expected numeric string or JSON number, found {other}"),
                    };
                    builder.append_value(number);
                    Ok(())
                }
                (data_type, _) => bail!(
                    "source column '{}' does not match Arrow builder for {data_type:?}",
                    column.name()
                ),
            },
        }
    }

    fn append_null(&mut self) -> Result<()> {
        match self {
            Self::Int64(builder) => builder.append_null(),
            Self::Bool(builder) => builder.append_null(),
            Self::Utf8(builder) => builder.append_null(),
            Self::TimestampMillis(builder) => builder.append_null(),
            Self::DateDays(builder) => builder.append_null(),
            Self::Decimal128(builder) => builder.append_null(),
            Self::Numeric(builder) => builder.append_null(),
        }
        Ok(())
    }

    fn append_skipped_value(&mut self, column: &SourceColumn) -> Result<()> {
        if column.nullable() {
            return self.append_null();
        }
        match (column.data_type(), self) {
            (SourceDataType::Int64, Self::Int64(builder)) => builder.append_value(0),
            (SourceDataType::Bool, Self::Bool(builder)) => builder.append_value(false),
            (SourceDataType::Utf8, Self::Utf8(builder)) => builder.append_value(""),
            (SourceDataType::TimestampMillis, Self::TimestampMillis(builder)) => {
                builder.append_value(0)
            }
            (SourceDataType::DateDays, Self::DateDays(builder)) => builder.append_value(0),
            (SourceDataType::Decimal128 { .. }, Self::Decimal128(builder)) => {
                builder.append_value(0)
            }
            (SourceDataType::Numeric, Self::Numeric(builder)) => builder.append_value("0"),
            (data_type, _) => bail!(
                "source column '{}' does not match Arrow builder for {data_type:?}",
                column.name()
            ),
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef> {
        let array: ArrayRef = match self {
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::TimestampMillis(builder) => Arc::new(builder.finish()),
            Self::DateDays(builder) => Arc::new(builder.finish()),
            Self::Decimal128(builder) => Arc::new(builder.finish()),
            Self::Numeric(builder) => Arc::new(builder.finish()),
        };
        Ok(array)
    }
}

#[cfg(test)]
mod tests;
