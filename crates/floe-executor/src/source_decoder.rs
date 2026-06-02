use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail, ensure};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Date32Builder, Decimal128Array,
    Decimal128Builder, Int64Array, Int64Builder, RecordBatch, StringArray, StringBuilder,
    TimestampMillisecondArray, TimestampMillisecondBuilder,
};
use floe_core::source::{AppendIngestEvent, SourceDataType, SourceDefinition};
use floe_core::{RowValue, source::SourceColumn};
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

#[derive(Debug, Clone)]
pub struct SourceRowDecoder {
    definition: SourceDefinition,
    encoded_required_columns: Option<Arc<[bool]>>,
}

impl SourceRowDecoder {
    pub fn new(definition: SourceDefinition) -> Self {
        Self {
            definition,
            encoded_required_columns: None,
        }
    }

    pub fn new_with_encoded_required_columns(
        definition: SourceDefinition,
        encoded_required_columns: Option<Arc<[bool]>>,
    ) -> Self {
        Self {
            definition,
            encoded_required_columns,
        }
    }

    pub fn definition(&self) -> &SourceDefinition {
        &self.definition
    }

    pub fn encode_row_key(
        &self,
        event: &AppendIngestEvent,
    ) -> Result<(Vec<u8>, Option<Timestamp>)> {
        if event.source() != self.definition.name() {
            bail!(
                "event source {} does not match definition {}",
                event.source(),
                self.definition.name()
            );
        }
        let payload = AppendIngestEvent::payload(event)
            .require_payload("source payload must be present for encoded events")?;
        let object = payload
            .as_object()
            .context("source payload must be a JSON object")?;
        let mut buf = Vec::with_capacity(64);
        let count = u32::try_from(self.definition.columns().len())
            .context("too many source columns to encode")?;
        buf.extend_from_slice(&count.to_le_bytes());
        let mut event_ts = None;
        for (idx, column) in self.definition.columns().iter().enumerate() {
            if !self.column_required(idx) {
                encode_typed_null(&mut buf, column.data_type());
                continue;
            }
            let value = object.get(column.name());
            encode_value_direct(
                &mut buf,
                column.data_type(),
                value,
                column.nullable(),
                &mut event_ts,
            )?;
        }
        Ok((buf, event_ts))
    }

    pub fn encode_row_values(
        &self,
        values: &[Option<RowValue>],
    ) -> Result<(Vec<u8>, Option<Timestamp>)> {
        if values.len() != self.definition.columns().len() {
            bail!(
                "source row value count {} does not match definition '{}' column count {}",
                values.len(),
                self.definition.name(),
                self.definition.columns().len()
            );
        }

        let mut buf = Vec::with_capacity(64);
        let count = u32::try_from(self.definition.columns().len())
            .context("too many source columns to encode")?;
        buf.extend_from_slice(&count.to_le_bytes());
        let mut event_ts = None;
        for (idx, (column, value)) in self.definition.columns().iter().zip(values).enumerate() {
            if !self.column_required(idx) {
                encode_typed_null(&mut buf, column.data_type());
                continue;
            }
            encode_row_value_direct(
                &mut buf,
                column.name(),
                column.data_type(),
                value.as_ref(),
                column.nullable(),
                &mut event_ts,
            )?;
        }
        Ok((buf, event_ts))
    }

    pub fn encode_arrow_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<Vec<(Vec<u8>, Option<Timestamp>)>> {
        if batch.num_columns() != self.definition.columns().len() {
            bail!(
                "Arrow batch column count {} does not match definition '{}' column count {}",
                batch.num_columns(),
                self.definition.name(),
                self.definition.columns().len()
            );
        }
        let columns = self.prepare_arrow_columns(batch)?;
        let mut rows = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            rows.push(self.encode_prepared_arrow_row(&columns, row_idx)?);
        }
        Ok(rows)
    }

    fn prepare_arrow_columns<'a>(
        &self,
        batch: &'a RecordBatch,
    ) -> Result<Vec<PreparedArrowColumn<'a>>> {
        self.definition
            .columns()
            .iter()
            .enumerate()
            .map(|(idx, column)| {
                let values = if self.column_required(idx) {
                    Some(ArrowColumnValues::new(
                        column.name(),
                        column.data_type(),
                        batch.column(idx).as_ref(),
                    )?)
                } else {
                    None
                };
                Ok(PreparedArrowColumn {
                    name: column.name().to_string(),
                    data_type: column.data_type().clone(),
                    nullable: column.nullable(),
                    values,
                })
            })
            .collect()
    }

    fn encode_prepared_arrow_row(
        &self,
        columns: &[PreparedArrowColumn<'_>],
        row_idx: usize,
    ) -> Result<(Vec<u8>, Option<Timestamp>)> {
        let mut buf = Vec::with_capacity(64);
        let count = u32::try_from(columns.len()).context("too many source columns to encode")?;
        buf.extend_from_slice(&count.to_le_bytes());
        let mut event_ts = None;
        for column in columns {
            let Some(values) = column.values.as_ref() else {
                encode_typed_null(&mut buf, &column.data_type);
                continue;
            };
            encode_prepared_arrow_value_direct(
                &mut buf,
                column.name.as_str(),
                &column.data_type,
                values,
                row_idx,
                column.nullable,
                &mut event_ts,
            )?;
        }
        Ok((buf, event_ts))
    }

    fn column_required(&self, idx: usize) -> bool {
        self.encoded_required_columns
            .as_ref()
            .and_then(|columns| columns.get(idx))
            .copied()
            .unwrap_or(true)
    }
}

pub struct SourceArrowBatchBuilder {
    definition: SourceDefinition,
    builders: Vec<SourceArrowColumnBuilder>,
    row_count: usize,
}

impl SourceArrowBatchBuilder {
    pub fn new(definition: SourceDefinition, capacity: usize) -> Self {
        let builders = definition
            .columns()
            .iter()
            .map(|column| SourceArrowColumnBuilder::new(column.data_type(), capacity))
            .collect();
        Self {
            definition,
            builders,
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
            builder.append_json_value(column, value, &mut event_ts)?;
        }
        self.row_count += 1;
        Ok(event_ts)
    }

    pub fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.row_count == 0 {
            return Ok(None);
        }
        let arrays = self
            .builders
            .iter_mut()
            .map(SourceArrowColumnBuilder::finish)
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(self.definition.to_arrow_schema(), arrays)?;
        self.row_count = 0;
        Ok(Some(batch))
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }
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

struct PreparedArrowColumn<'a> {
    name: String,
    data_type: SourceDataType,
    nullable: bool,
    values: Option<ArrowColumnValues<'a>>,
}

enum ArrowColumnValues<'a> {
    Int64(&'a Int64Array),
    Bool(&'a BooleanArray),
    Utf8(&'a StringArray),
    TimestampMillis(&'a TimestampMillisecondArray),
    DateDays(&'a Date32Array),
    Decimal128(&'a Decimal128Array),
    Numeric(&'a StringArray),
}

impl<'a> ArrowColumnValues<'a> {
    fn new(column_name: &str, data_type: &SourceDataType, array: &'a dyn Array) -> Result<Self> {
        match data_type {
            SourceDataType::Int64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(Self::Int64)
                .with_context(|| format!("Arrow column '{column_name}' is not Int64")),
            SourceDataType::Utf8 => array
                .as_any()
                .downcast_ref::<StringArray>()
                .map(Self::Utf8)
                .with_context(|| format!("Arrow column '{column_name}' is not Utf8")),
            SourceDataType::TimestampMillis => array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .map(Self::TimestampMillis)
                .with_context(|| format!("Arrow column '{column_name}' is not TimestampMillis")),
            SourceDataType::DateDays => array
                .as_any()
                .downcast_ref::<Date32Array>()
                .map(Self::DateDays)
                .with_context(|| format!("Arrow column '{column_name}' is not Date32")),
            SourceDataType::Decimal128 { .. } => array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .map(Self::Decimal128)
                .with_context(|| format!("Arrow column '{column_name}' is not Decimal128")),
            SourceDataType::Numeric => array
                .as_any()
                .downcast_ref::<StringArray>()
                .map(Self::Numeric)
                .with_context(|| format!("Arrow column '{column_name}' is not Numeric/Utf8")),
            SourceDataType::Bool => array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(Self::Bool)
                .with_context(|| format!("Arrow column '{column_name}' is not Boolean")),
        }
    }

    fn is_null(&self, row_idx: usize) -> bool {
        match self {
            Self::Int64(array) => array.is_null(row_idx),
            Self::Bool(array) => array.is_null(row_idx),
            Self::Utf8(array) => array.is_null(row_idx),
            Self::TimestampMillis(array) => array.is_null(row_idx),
            Self::DateDays(array) => array.is_null(row_idx),
            Self::Decimal128(array) => array.is_null(row_idx),
            Self::Numeric(array) => array.is_null(row_idx),
        }
    }
}

fn encode_prepared_arrow_value_direct(
    buf: &mut Vec<u8>,
    column_name: &str,
    data_type: &SourceDataType,
    values: &ArrowColumnValues<'_>,
    row_idx: usize,
    nullable: bool,
    event_ts: &mut Option<Timestamp>,
) -> Result<()> {
    if values.is_null(row_idx) {
        if nullable {
            encode_typed_null(buf, data_type);
            return Ok(());
        }
        bail!("null value violates non-nullable column '{column_name}'");
    }

    match (data_type, values) {
        (SourceDataType::Int64, ArrowColumnValues::Int64(array)) => {
            let number = array.value(row_idx);
            buf.push(0x01);
            buf.extend_from_slice(&number.to_le_bytes());
            Ok(())
        }
        (SourceDataType::Utf8, ArrowColumnValues::Utf8(array)) => {
            let bytes = array.value(row_idx).as_bytes();
            buf.push(0x02);
            let len = u32::try_from(bytes.len()).context("utf8 value too large for MV key")?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(bytes);
            Ok(())
        }
        (SourceDataType::TimestampMillis, ArrowColumnValues::TimestampMillis(array)) => {
            let number = array.value(row_idx);
            buf.push(0x03);
            buf.extend_from_slice(&number.to_le_bytes());
            if event_ts.is_none() && number >= 0 {
                *event_ts = Some(number as u64);
            }
            Ok(())
        }
        (SourceDataType::Bool, ArrowColumnValues::Bool(array)) => {
            buf.push(0x04);
            buf.push(if array.value(row_idx) { 1 } else { 0 });
            Ok(())
        }
        (SourceDataType::DateDays, ArrowColumnValues::DateDays(array)) => {
            let days = array.value(row_idx);
            buf.push(0x09);
            buf.extend_from_slice(&days.to_le_bytes());
            Ok(())
        }
        (SourceDataType::Decimal128 { .. }, ArrowColumnValues::Decimal128(array)) => {
            let number = array.value(row_idx);
            buf.push(0x0B);
            buf.extend_from_slice(&number.to_le_bytes());
            Ok(())
        }
        (SourceDataType::Numeric, ArrowColumnValues::Numeric(array)) => {
            let bytes = array.value(row_idx).as_bytes();
            buf.push(0x02);
            let len = u32::try_from(bytes.len()).context("numeric value too large for MV key")?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(bytes);
            Ok(())
        }
        _ => bail!("prepared Arrow column '{column_name}' does not match type {data_type:?}"),
    }
}

fn encode_row_value_direct(
    buf: &mut Vec<u8>,
    column_name: &str,
    data_type: &SourceDataType,
    value: Option<&RowValue>,
    nullable: bool,
    event_ts: &mut Option<Timestamp>,
) -> Result<()> {
    match value {
        None if nullable => {
            encode_typed_null(buf, data_type);
            Ok(())
        }
        None => bail!("null value violates non-nullable column '{column_name}'"),
        Some(RowValue::Int64(number)) if matches!(data_type, SourceDataType::Int64) => {
            buf.push(0x01);
            buf.extend_from_slice(&number.to_le_bytes());
            Ok(())
        }
        Some(RowValue::Utf8(string)) if matches!(data_type, SourceDataType::Utf8) => {
            buf.push(0x02);
            let bytes = string.as_bytes();
            let len = u32::try_from(bytes.len()).context("utf8 value too large for MV key")?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(bytes);
            Ok(())
        }
        Some(RowValue::TimestampMillis(number))
            if matches!(data_type, SourceDataType::TimestampMillis) =>
        {
            buf.push(0x03);
            buf.extend_from_slice(&number.to_le_bytes());
            if event_ts.is_none() && *number >= 0 {
                *event_ts = Some(*number as u64);
            }
            Ok(())
        }
        Some(RowValue::Bool(flag)) if matches!(data_type, SourceDataType::Bool) => {
            buf.push(0x04);
            buf.push(if *flag { 1 } else { 0 });
            Ok(())
        }
        Some(RowValue::DateDays(days)) if matches!(data_type, SourceDataType::DateDays) => {
            buf.push(0x09);
            buf.extend_from_slice(&days.to_le_bytes());
            Ok(())
        }
        Some(RowValue::Decimal128(number))
            if matches!(data_type, SourceDataType::Decimal128 { .. }) =>
        {
            buf.push(0x0B);
            buf.extend_from_slice(&number.to_le_bytes());
            Ok(())
        }
        Some(RowValue::Numeric(number)) if matches!(data_type, SourceDataType::Numeric) => {
            buf.push(0x02);
            let bytes = number.as_bytes();
            let len = u32::try_from(bytes.len()).context("numeric value too large for MV key")?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(bytes);
            Ok(())
        }
        Some(value) => bail!(
            "source row value for column '{}' does not match type {:?}: {:?}",
            column_name,
            data_type,
            value
        ),
    }
}

fn encode_value_direct(
    buf: &mut Vec<u8>,
    data_type: &SourceDataType,
    value: Option<&Value>,
    nullable: bool,
    event_ts: &mut Option<Timestamp>,
) -> Result<()> {
    match value {
        None if nullable => {
            encode_typed_null(buf, data_type);
            Ok(())
        }
        None => bail!("missing field in source payload"),
        Some(value) if value.is_null() => {
            if nullable {
                encode_typed_null(buf, data_type);
                Ok(())
            } else {
                bail!("null value violates non-nullable column");
            }
        }
        Some(value) => match data_type {
            SourceDataType::Int64 => {
                let number = value
                    .as_i64()
                    .with_context(|| format!("expected integer value, found {value}"))?;
                buf.push(0x01);
                buf.extend_from_slice(&number.to_le_bytes());
                Ok(())
            }
            SourceDataType::Utf8 => {
                let string = value
                    .as_str()
                    .with_context(|| format!("expected string value, found {value}"))?;
                buf.push(0x02);
                let bytes = string.as_bytes();
                let len = u32::try_from(bytes.len()).context("utf8 value too large for MV key")?;
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(bytes);
                Ok(())
            }
            SourceDataType::TimestampMillis => {
                let number = value
                    .as_i64()
                    .with_context(|| format!("expected integer timestamp, found {value}"))?;
                buf.push(0x03);
                buf.extend_from_slice(&number.to_le_bytes());
                if event_ts.is_none() && number >= 0 {
                    *event_ts = Some(number as u64);
                }
                Ok(())
            }
            SourceDataType::Bool => {
                let flag = value
                    .as_bool()
                    .with_context(|| format!("expected boolean value, found {value}"))?;
                buf.push(0x04);
                buf.push(if flag { 1 } else { 0 });
                Ok(())
            }
            SourceDataType::DateDays => {
                let days = value
                    .as_i64()
                    .with_context(|| format!("expected integer date days, found {value}"))?;
                let days = i32::try_from(days)
                    .with_context(|| format!("date days value out of range: {value}"))?;
                buf.push(0x09);
                buf.extend_from_slice(&days.to_le_bytes());
                Ok(())
            }
            SourceDataType::Decimal128 { scale, .. } => {
                let number = match value {
                    Value::String(value) => parse_decimal_text_to_i128(value, *scale)?,
                    Value::Number(value) => parse_decimal_text_to_i128(&value.to_string(), *scale)?,
                    other => bail!("expected decimal string or JSON number, found {other}"),
                };
                buf.push(0x0B);
                buf.extend_from_slice(&number.to_le_bytes());
                Ok(())
            }
            SourceDataType::Numeric => {
                let number = match value {
                    Value::String(value) => value.clone(),
                    Value::Number(value) => value.to_string(),
                    other => bail!("expected numeric string or JSON number, found {other}"),
                };
                buf.push(0x02);
                let bytes = number.as_bytes();
                let len =
                    u32::try_from(bytes.len()).context("numeric value too large for MV key")?;
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(bytes);
                Ok(())
            }
        },
    }
}

fn encode_typed_null(buf: &mut Vec<u8>, data_type: &SourceDataType) {
    match data_type {
        SourceDataType::Int64 => buf.push(0x05),
        SourceDataType::Utf8 => buf.push(0x06),
        SourceDataType::TimestampMillis => buf.push(0x07),
        SourceDataType::Bool => buf.push(0x08),
        SourceDataType::DateDays => buf.push(0x0A),
        SourceDataType::Decimal128 { .. } => buf.push(0x0C),
        SourceDataType::Numeric => buf.push(0x06),
    }
}

fn parse_decimal_text_to_i128(value: &str, scale: i8) -> Result<i128> {
    let scale = u32::try_from(scale).context("Decimal128 scale cannot be negative")?;
    let value = value.trim();
    let (negative, unsigned) = value
        .strip_prefix('-')
        .map(|rest| (true, rest))
        .unwrap_or((false, value));
    let unsigned = unsigned.strip_prefix('+').unwrap_or(unsigned);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let mut digits = String::with_capacity(whole.len() + scale as usize);
    digits.push_str(whole);
    let scale_usize = usize::try_from(scale).expect("u32 scale fits usize");
    ensure!(
        fraction.len() <= scale_usize,
        "decimal value '{value}' has more fractional digits than scale {scale}"
    );
    digits.push_str(fraction);
    digits.extend(std::iter::repeat_n('0', scale_usize - fraction.len()));
    let parsed = digits
        .parse::<i128>()
        .with_context(|| format!("decode decimal value '{value}'"))?;
    Ok(if negative { -parsed } else { parsed })
}

#[cfg(test)]
mod tests;
