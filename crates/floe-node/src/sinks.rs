use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use floe_cdc_core::{
    CdcChange, CdcColumn, CdcPrimaryKey, CdcRow, CdcSourcePosition, CdcTableId, CdcTableSchema,
    CdcTransactionId, UpstreamTableRef,
};
use floe_core::{RowValue, catalog::ColumnType};
use floe_executor::FloeQueryContext;
use floe_executor::MaterializedViewRegistry;
use floe_executor::checkpoint::SinkCursor;
use floe_executor::tail::{TailBatch, TailParams, execute_tail, is_tail_canceled_error};
use floe_node_core::debezium_encoder::{
    DebeziumEncodeContext, DebeziumEnvelopeConfig, encode_debezium_change,
};
use futures::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{Message, Offset, TopicPartitionList};
use reqwest::Client;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::{SinkConfig, SinkSpec};
use crate::metrics;

const DEFAULT_SINK_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_BATCH_ROWS: usize = 1;
const DEFAULT_BATCH_BYTES: usize = usize::MAX;
const DEFAULT_RETRY_MAX_ATTEMPTS: usize = 5;
const DEFAULT_RETRY_BASE_MS: u64 = 100;
const DEFAULT_RETRY_MAX_BACKOFF_MS: u64 = 5_000;
const DEFAULT_KAFKA_CHECKPOINT_PARTITION: i32 = 0;
const DEFAULT_KAFKA_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(10);
const TAIL_BATCH_LOG_SAMPLE_EVERY: u64 = 256;
static TAIL_BATCH_LOG_COUNTER: AtomicUsize = AtomicUsize::new(0);

mod file_backend;
mod http_backend;
mod kafka_backend;

use file_backend::*;
use http_backend::*;
use kafka_backend::*;

#[derive(Clone, Copy)]
struct BatchPolicy {
    max_rows: usize,
    max_bytes: usize,
}

impl BatchPolicy {
    fn new(max_rows: usize, max_bytes: usize) -> Result<Self> {
        if max_rows == 0 {
            bail!("sink batch_rows must be greater than zero");
        }
        if max_bytes == 0 {
            bail!("sink batch_bytes must be greater than zero");
        }
        Ok(Self {
            max_rows,
            max_bytes,
        })
    }

    fn should_flush(&self, rows: usize, bytes: usize) -> bool {
        rows > 0 && (rows >= self.max_rows || bytes >= self.max_bytes)
    }
}

#[derive(Clone, Copy)]
struct RetryPolicy {
    max_attempts: usize,
    base_backoff: Duration,
    max_backoff: Duration,
}

impl RetryPolicy {
    fn new(max_attempts: usize, base_backoff: Duration, max_backoff: Duration) -> Result<Self> {
        if max_attempts == 0 {
            bail!("sink retry_max_attempts must be greater than zero");
        }
        if base_backoff.is_zero() || max_backoff.is_zero() {
            bail!("sink retry backoff durations must be greater than zero");
        }
        Ok(Self {
            max_attempts,
            base_backoff,
            max_backoff,
        })
    }

    fn backoff_for_failure(&self, failure_idx: usize) -> Duration {
        let base_ms = self.base_backoff.as_millis() as u64;
        let max_ms = self.max_backoff.as_millis() as u64;
        let factor = if failure_idx >= 63 {
            u64::MAX
        } else {
            1_u64 << failure_idx
        };
        Duration::from_millis(base_ms.saturating_mul(factor).min(max_ms))
    }
}

struct SinkQueueTracker {
    sink_name: String,
    queued: AtomicUsize,
    latest_enqueued_version: AtomicI64,
    latest_flushed_version: AtomicI64,
}

impl SinkQueueTracker {
    fn new(sink_name: impl Into<String>) -> Arc<Self> {
        let sink_name = sink_name.into();
        metrics::record_sink_queue_depth(&sink_name, 0);
        metrics::record_sink_version_lag(&sink_name, 0);
        Arc::new(Self {
            sink_name,
            queued: AtomicUsize::new(0),
            latest_enqueued_version: AtomicI64::new(-1),
            latest_flushed_version: AtomicI64::new(-1),
        })
    }

    fn on_enqueue(&self, version: i64) {
        self.on_enqueue_many(1, version);
    }

    fn on_enqueue_many(&self, count: usize, version: i64) {
        if count == 0 {
            return;
        }
        let depth = self.queued.fetch_add(count, Ordering::Relaxed) + count;
        metrics::record_sink_queue_depth(&self.sink_name, depth);
        self.latest_enqueued_version
            .fetch_max(version, Ordering::Relaxed);
        self.update_lag();
    }

    fn on_dequeue(&self) {
        self.on_dequeue_many(1);
    }

    fn on_dequeue_many(&self, count: usize) {
        if count == 0 {
            return;
        }
        let prev = self.queued.fetch_sub(count, Ordering::Relaxed);
        let depth = prev.saturating_sub(count);
        metrics::record_sink_queue_depth(&self.sink_name, depth);
        self.update_lag();
    }

    fn on_flushed(&self, version: i64) {
        self.latest_flushed_version
            .fetch_max(version, Ordering::Relaxed);
        self.update_lag();
    }

    fn update_lag(&self) {
        let enqueued = self.latest_enqueued_version.load(Ordering::Relaxed);
        let flushed = self.latest_flushed_version.load(Ordering::Relaxed);
        metrics::record_sink_version_lag(&self.sink_name, (enqueued - flushed).max(0));
    }
}

#[derive(Debug, Clone)]
enum SinkEncoding {
    Json,
    Debezium(DebeziumSinkEncoding),
}

#[derive(Debug, Clone)]
struct DebeziumSinkEncoding {
    source_name: String,
    database_name: String,
    schema_name: String,
    table_name: String,
    key_columns: Vec<String>,
}

struct SinkRecord {
    version: i64,
    row_idx: u64,
    key: Option<String>,
    json: serde_json::Value,
    payload: String,
    byte_len: usize,
}

enum SinkEvent {
    Rows(Vec<SinkRecord>),
    Flush { version: i64 },
}

pub fn spawn_sinks(
    sinks: Vec<SinkSpec>,
    query: FloeQueryContext,
    registry: Arc<MaterializedViewRegistry>,
    resume_cursors: HashMap<String, SinkCursor>,
    checkpoint_tx: Option<mpsc::UnboundedSender<SinkCursor>>,
    tail_cancel: CancellationToken,
    runtime_cancel: CancellationToken,
    runtime_failure: Arc<StdMutex<Option<String>>>,
) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();
    for sink in sinks {
        let resume_cursor = resume_cursors.get(&sink.name).cloned();
        let checkpoint_tx = checkpoint_tx.clone();
        let ctx = query.clone();
        let registry = registry.clone();
        let tail_cancel = tail_cancel.clone();
        let runtime_cancel = runtime_cancel.clone();
        let runtime_failure = Arc::clone(&runtime_failure);
        handles.push(tokio::spawn(async move {
            let name = sink.name.clone();
            if let Err(err) = run_sink(
                sink,
                ctx,
                registry,
                resume_cursor,
                checkpoint_tx,
                tail_cancel.clone(),
            )
            .await
            {
                if is_tail_canceled_error(&err) {
                    tracing::info!(sink = %name, "sink canceled");
                } else {
                    tracing::error!(sink = %name, error = %err, "sink failed");
                    record_runtime_failure(
                        &runtime_failure,
                        format!("sink '{name}' failed: {err}"),
                    );
                    runtime_cancel.cancel();
                }
            }
        }));
    }
    handles
}

fn record_runtime_failure(state: &Arc<StdMutex<Option<String>>>, message: String) {
    metrics::inc_runtime_error("sink");
    let mut guard = state.lock().expect("runtime failure lock poisoned");
    if guard.is_none() {
        *guard = Some(message);
    }
}

async fn run_sink(
    sink: SinkSpec,
    query: FloeQueryContext,
    registry: Arc<MaterializedViewRegistry>,
    resume_cursor: Option<SinkCursor>,
    checkpoint_tx: Option<mpsc::UnboundedSender<SinkCursor>>,
    cancel: CancellationToken,
) -> Result<()> {
    let resume_as_of = resume_cursor
        .as_ref()
        .map(|cursor| cursor.last_emitted_mv_version);
    let resume_with_snapshot = resume_cursor.is_none();

    match sink.config {
        SinkConfig::Kafka {
            brokers,
            topic,
            mv,
            format,
            key_columns,
            with_snapshot,
            as_of,
            batch_rows,
            batch_bytes,
            queue_capacity,
            retry_max_attempts,
            retry_base_ms,
            retry_max_backoff_ms,
            transactional_id,
            checkpoint_topic,
            checkpoint_partition,
            ..
        } => {
            let encoding =
                kafka_sink_encoding(&sink.name, &mv, format.as_deref(), key_columns.as_deref())?;
            let batch_policy = BatchPolicy::new(
                batch_rows.unwrap_or(DEFAULT_BATCH_ROWS),
                batch_bytes.unwrap_or(DEFAULT_BATCH_BYTES),
            )?;
            let queue_capacity = queue_capacity.unwrap_or(DEFAULT_SINK_QUEUE_CAPACITY);
            let retry_policy = RetryPolicy::new(
                retry_max_attempts.unwrap_or(DEFAULT_RETRY_MAX_ATTEMPTS),
                Duration::from_millis(retry_base_ms.unwrap_or(DEFAULT_RETRY_BASE_MS)),
                Duration::from_millis(retry_max_backoff_ms.unwrap_or(DEFAULT_RETRY_MAX_BACKOFF_MS)),
            )?;
            run_kafka_sink(
                &sink.name,
                &query,
                registry,
                cancel,
                &brokers,
                &topic,
                &mv,
                with_snapshot.unwrap_or(false) && resume_with_snapshot,
                as_of.or(resume_as_of),
                queue_capacity,
                batch_policy,
                retry_policy,
                checkpoint_tx,
                transactional_id,
                checkpoint_topic,
                checkpoint_partition,
                encoding,
            )
            .await
        }
        SinkConfig::File {
            path,
            mv,
            with_snapshot,
            as_of,
            append,
            effectively_once,
            batch_rows,
            batch_bytes,
            queue_capacity,
            ..
        } => {
            let batch_policy = BatchPolicy::new(
                batch_rows.unwrap_or(DEFAULT_BATCH_ROWS),
                batch_bytes.unwrap_or(DEFAULT_BATCH_BYTES),
            )?;
            let queue_capacity = queue_capacity.unwrap_or(DEFAULT_SINK_QUEUE_CAPACITY);
            run_file_sink(
                &sink.name,
                &query,
                registry,
                cancel,
                &path,
                &mv,
                with_snapshot.unwrap_or(false) && resume_with_snapshot,
                as_of.or(resume_as_of),
                append.unwrap_or(true),
                effectively_once.unwrap_or(false),
                queue_capacity,
                batch_policy,
                checkpoint_tx,
            )
            .await
        }
        SinkConfig::Http {
            url,
            mv,
            with_snapshot,
            as_of,
            batch_size,
            batch_rows,
            batch_bytes,
            queue_capacity,
            retry_max_attempts,
            retry_base_ms,
            retry_max_backoff_ms,
            ..
        } => {
            let rows_threshold = batch_rows.or(batch_size).unwrap_or(DEFAULT_BATCH_ROWS);
            let batch_policy =
                BatchPolicy::new(rows_threshold, batch_bytes.unwrap_or(DEFAULT_BATCH_BYTES))?;
            let queue_capacity = queue_capacity.unwrap_or(DEFAULT_SINK_QUEUE_CAPACITY);
            let retry_policy = RetryPolicy::new(
                retry_max_attempts.unwrap_or(DEFAULT_RETRY_MAX_ATTEMPTS),
                Duration::from_millis(retry_base_ms.unwrap_or(DEFAULT_RETRY_BASE_MS)),
                Duration::from_millis(retry_max_backoff_ms.unwrap_or(DEFAULT_RETRY_MAX_BACKOFF_MS)),
            )?;
            run_http_sink(
                &sink.name,
                &query,
                registry,
                cancel,
                &url,
                &mv,
                with_snapshot.unwrap_or(false) && resume_with_snapshot,
                as_of.or(resume_as_of),
                queue_capacity,
                batch_policy,
                retry_policy,
                checkpoint_tx,
            )
            .await
        }
    }
}
async fn stream_tail_into_queue(
    stream: impl futures::Stream<Item = Result<TailBatch>> + Unpin,
    sender: mpsc::Sender<SinkEvent>,
    tracker: Arc<SinkQueueTracker>,
) -> Result<()> {
    stream_tail_into_queue_with_encoding(stream, sender, tracker, SinkEncoding::Json).await
}

async fn stream_tail_into_queue_with_encoding(
    mut stream: impl futures::Stream<Item = Result<TailBatch>> + Unpin,
    sender: mpsc::Sender<SinkEvent>,
    tracker: Arc<SinkQueueTracker>,
    encoding: SinkEncoding,
) -> Result<()> {
    while let Some(batch) = stream.next().await {
        let batch_start = Instant::now();
        let batch = batch?;
        let schema = batch.batch.schema();
        let version = batch.version;
        let row_count = batch.batch.num_rows();
        let seq = TAIL_BATCH_LOG_COUNTER.fetch_add(1, Ordering::Relaxed) as u64;
        if seq < 16 || seq.is_multiple_of(TAIL_BATCH_LOG_SAMPLE_EVERY) {
            tracing::info!(
                batch_seq = seq,
                version,
                rows = row_count,
                "sink tail batch observed"
            );
        }
        let convert_start = Instant::now();
        let rows = encode_tail_batch_for_sink(&batch, &schema, &encoding)?;
        let convert_latency_ms = convert_start.elapsed().as_millis() as u64;
        let enqueue_start = Instant::now();
        if !rows.is_empty() {
            let row_count = rows.len();
            sender
                .send(SinkEvent::Rows(rows))
                .await
                .map_err(|_| anyhow!("sink queue consumer stopped"))?;
            tracker.on_enqueue_many(row_count, version);
        }

        sender
            .send(SinkEvent::Flush { version })
            .await
            .map_err(|_| anyhow!("sink queue consumer stopped"))?;
        tracker.on_enqueue(version);
        let enqueue_latency_ms = enqueue_start.elapsed().as_millis() as u64;
        let batch_latency_ms = batch_start.elapsed().as_millis() as u64;
        if seq < 16 || seq.is_multiple_of(TAIL_BATCH_LOG_SAMPLE_EVERY) {
            tracing::info!(
                batch_seq = seq,
                version,
                rows = row_count,
                convert_latency_ms,
                enqueue_latency_ms,
                batch_latency_ms,
                "sink tail batch conversion metrics"
            );
        }
    }
    Ok(())
}

fn kafka_sink_encoding(
    sink_name: &str,
    mv_name: &str,
    format: Option<&str>,
    key_columns: Option<&[String]>,
) -> Result<SinkEncoding> {
    let Some(format) = format else {
        return Ok(SinkEncoding::Json);
    };
    match format.to_ascii_lowercase().replace('-', "_").as_str() {
        "json" => Ok(SinkEncoding::Json),
        "debezium_json" => {
            let key_columns = key_columns
                .filter(|columns| !columns.is_empty())
                .ok_or_else(|| anyhow!("Kafka Debezium sink '{sink_name}' requires key_columns"))?
                .to_vec();
            Ok(SinkEncoding::Debezium(DebeziumSinkEncoding {
                source_name: sink_name.to_string(),
                database_name: "floe".to_string(),
                schema_name: "public".to_string(),
                table_name: mv_name.to_string(),
                key_columns,
            }))
        }
        other => bail!("unsupported Kafka sink format '{other}'"),
    }
}

fn encode_tail_batch_for_sink(
    batch: &TailBatch,
    schema: &SchemaRef,
    encoding: &SinkEncoding,
) -> Result<Vec<SinkRecord>> {
    match encoding {
        SinkEncoding::Json => encode_tail_batch_as_json(batch, schema),
        SinkEncoding::Debezium(config) => encode_tail_batch_as_debezium(batch, schema, config),
    }
}

fn encode_tail_batch_as_json(batch: &TailBatch, schema: &SchemaRef) -> Result<Vec<SinkRecord>> {
    let mut rows = Vec::with_capacity(batch.batch.num_rows());
    for row_idx in 0..batch.batch.num_rows() {
        let json = tail_row_to_json(batch, row_idx, schema)?;
        let payload = serde_json::to_string(&json).context("serialize sink row")?;
        rows.push(SinkRecord {
            version: batch.version,
            row_idx: u64::try_from(row_idx).unwrap_or(u64::MAX),
            key: None,
            json,
            byte_len: payload.len(),
            payload,
        });
    }
    Ok(rows)
}

fn encode_tail_batch_as_debezium(
    batch: &TailBatch,
    schema: &SchemaRef,
    config: &DebeziumSinkEncoding,
) -> Result<Vec<SinkRecord>> {
    let cdc_schema = cdc_schema_from_tail_schema(schema, config)?;
    let envelope_config =
        DebeziumEnvelopeConfig::new(&config.source_name)?.with_database_name(&config.database_name);
    let source_position =
        CdcSourcePosition::opaque(format!("mv/{}/{}", config.table_name, batch.version))?;
    let transaction_id =
        CdcTransactionId::new(format!("mv-{}-{}", config.table_name, batch.version))?;
    let mut rows = Vec::with_capacity(batch.batch.num_rows());
    for row_idx in 0..batch.batch.num_rows() {
        let row = cdc_row_from_tail_batch(batch, row_idx, &cdc_schema)?;
        let op = batch.ops.get(row_idx).copied().unwrap_or(1);
        let change = if op < 0 {
            CdcChange::Delete {
                key: None,
                before: Some(row),
            }
        } else {
            CdcChange::Insert { row }
        };
        let records = encode_debezium_change(
            &cdc_schema,
            &change,
            &envelope_config,
            DebeziumEncodeContext {
                source_position: Some(&source_position),
                transaction_id: Some(&transaction_id),
                sequence: Some(u64::try_from(row_idx).unwrap_or(u64::MAX)),
                ts_ms: tail_row_time_ms(batch, row_idx),
            },
        )?;
        for record in records {
            let Some(value_bytes) = record.value_json_bytes()? else {
                continue;
            };
            let key = record
                .key_json_bytes()?
                .map(String::from_utf8)
                .transpose()
                .context("Debezium Kafka key must be UTF-8 JSON")?;
            let payload = String::from_utf8(value_bytes)
                .context("Debezium Kafka value must be UTF-8 JSON")?;
            let byte_len = payload.len() + key.as_ref().map(String::len).unwrap_or(0);
            rows.push(SinkRecord {
                version: batch.version,
                row_idx: u64::try_from(row_idx).unwrap_or(u64::MAX),
                key,
                json: record.value().cloned().unwrap_or(serde_json::Value::Null),
                byte_len,
                payload,
            });
        }
    }
    Ok(rows)
}

fn publish_sink_cursor(
    checkpoint_tx: &Option<mpsc::UnboundedSender<SinkCursor>>,
    cursor: SinkCursor,
) {
    if let Some(sender) = checkpoint_tx {
        let _ = sender.send(cursor);
    }
}

fn current_unix_time_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().try_into().unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}

fn tail_row_time_ms(batch: &TailBatch, row_idx: usize) -> Option<i64> {
    batch
        .times
        .get(row_idx)
        .copied()
        .flatten()
        .map(|micros| micros / 1_000)
}

fn cdc_schema_from_tail_schema(
    schema: &SchemaRef,
    config: &DebeziumSinkEncoding,
) -> Result<CdcTableSchema> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let is_key = config
                .key_columns
                .iter()
                .any(|column| column == field.name());
            CdcColumn::new(
                field.name().clone(),
                column_type_from_arrow(field.data_type())?,
                field.is_nullable() && !is_key,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    CdcTableSchema::new(
        CdcTableId::new(&config.table_name)?,
        UpstreamTableRef::new(&config.schema_name, &config.table_name)?,
        columns,
        CdcPrimaryKey::new(config.key_columns.clone())?,
    )
}

fn column_type_from_arrow(data_type: &DataType) -> Result<ColumnType> {
    match data_type {
        DataType::Boolean => Ok(ColumnType::Bool),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Ok(ColumnType::Int64),
        DataType::Float32 | DataType::Float64 => Ok(ColumnType::Numeric),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(ColumnType::Utf8),
        DataType::Timestamp(_, _) => Ok(ColumnType::TimestampMillis),
        DataType::Date32 => Ok(ColumnType::DateDays),
        DataType::Decimal128(precision, scale) => ColumnType::decimal128(*precision, *scale),
        other => bail!("unsupported Debezium Kafka sink column type: {other:?}"),
    }
}

fn cdc_row_from_tail_batch(
    batch: &TailBatch,
    row_idx: usize,
    schema: &CdcTableSchema,
) -> Result<CdcRow> {
    let mut values = Vec::with_capacity(schema.columns().len());
    for (column_idx, column) in schema.columns().iter().enumerate() {
        values.push(arrow_value_to_row_value(
            batch.batch.column(column_idx).as_ref(),
            row_idx,
            column.data_type(),
        )?);
    }
    CdcRow::new(values)
}

fn arrow_value_to_row_value(
    array: &dyn Array,
    row_idx: usize,
    data_type: &ColumnType,
) -> Result<Option<RowValue>> {
    if array.is_null(row_idx) {
        return Ok(None);
    }
    match data_type {
        ColumnType::Int64 => Ok(Some(RowValue::Int64(arrow_i64_value(array, row_idx)?))),
        ColumnType::Bool => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("Debezium sink Boolean column has incompatible Arrow type")?;
            Ok(Some(RowValue::Bool(values.value(row_idx))))
        }
        ColumnType::Utf8 => Ok(Some(RowValue::Utf8(arrow_string_value(array, row_idx)?))),
        ColumnType::TimestampMillis => Ok(Some(RowValue::TimestampMillis(
            arrow_timestamp_millis_value(array, row_idx)?,
        ))),
        ColumnType::DateDays => {
            let values = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .context("Debezium sink DateDays column has incompatible Arrow type")?;
            Ok(Some(RowValue::DateDays(values.value(row_idx))))
        }
        ColumnType::Decimal128 { .. } => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .context("Debezium sink Decimal128 column has incompatible Arrow type")?;
            Ok(Some(RowValue::Decimal128(values.value(row_idx))))
        }
        ColumnType::Numeric => Ok(Some(RowValue::Numeric(arrow_numeric_string_value(
            array, row_idx,
        )?))),
    }
}

fn arrow_i64_value(array: &dyn Array, row_idx: usize) -> Result<i64> {
    if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
        return Ok(i64::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
        return Ok(i64::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(i64::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(values.value(row_idx));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
        return Ok(i64::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
        return Ok(i64::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(i64::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return i64::try_from(values.value(row_idx))
            .context("Debezium sink UInt64 value does not fit in Int64");
    }
    bail!(
        "Debezium sink Int64 column has incompatible Arrow type: {:?}",
        array.data_type()
    )
}

fn arrow_string_value(array: &dyn Array, row_idx: usize) -> Result<String> {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(values.value(row_idx).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(values.value(row_idx).to_string());
    }
    bail!(
        "Debezium sink Utf8 column has incompatible Arrow type: {:?}",
        array.data_type()
    )
}

fn arrow_timestamp_millis_value(array: &dyn Array, row_idx: usize) -> Result<i64> {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .context("Debezium sink timestamp column has incompatible Arrow type")?;
            Ok(values.value(row_idx).saturating_mul(1_000))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .context("Debezium sink timestamp column has incompatible Arrow type")?;
            Ok(values.value(row_idx))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .context("Debezium sink timestamp column has incompatible Arrow type")?;
            Ok(values.value(row_idx) / 1_000)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .context("Debezium sink timestamp column has incompatible Arrow type")?;
            Ok(values.value(row_idx) / 1_000_000)
        }
        other => bail!("Debezium sink timestamp column has incompatible Arrow type: {other:?}"),
    }
}

fn arrow_numeric_string_value(array: &dyn Array, row_idx: usize) -> Result<String> {
    if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(values.value(row_idx).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(values.value(row_idx).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(values.value(row_idx).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(values.value(row_idx).to_string());
    }
    bail!(
        "Debezium sink Numeric column has incompatible Arrow type: {:?}",
        array.data_type()
    )
}

fn tail_row_to_json(
    batch: &TailBatch,
    row_idx: usize,
    schema: &SchemaRef,
) -> Result<serde_json::Value> {
    let mut object = serde_json::Map::new();
    object.insert(
        "__mv_version".to_string(),
        serde_json::Value::from(batch.version),
    );
    object.insert(
        "__op".to_string(),
        serde_json::Value::from(batch.ops.get(row_idx).copied().unwrap_or(0)),
    );
    let time = batch.times.get(row_idx).copied().flatten();
    if let Some(time) = time {
        object.insert("__time".to_string(), serde_json::Value::from(time));
    } else {
        object.insert("__time".to_string(), serde_json::Value::Null);
    }

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array = batch.batch.column(col_idx);
        let value = array_value_to_json(array, row_idx)?;
        object.insert(field.name().clone(), value);
    }

    Ok(serde_json::Value::Object(object))
}

fn array_value_to_json(array: &ArrayRef, row_idx: usize) -> Result<serde_json::Value> {
    if array.is_null(row_idx) {
        return Ok(serde_json::Value::Null);
    }

    if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx) as f64));
    }
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(serde_json::Value::from(values.value(row_idx).to_string()));
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(serde_json::Value::from(values.value(row_idx).to_string()));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampSecondArray>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Date32Array>() {
        return Ok(serde_json::Value::from(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Decimal128Array>() {
        let scale = match values.data_type() {
            DataType::Decimal128(_, scale) => *scale,
            _ => 0,
        };
        return Ok(serde_json::Value::String(format_decimal128(
            values.value(row_idx),
            scale,
        )));
    }

    bail!(
        "unsupported sink column type for JSON conversion: {:?}",
        array.data_type()
    )
}

fn format_decimal128(value: i128, scale: i8) -> String {
    if scale <= 0 {
        return value.to_string();
    }
    let scale = scale as u32;
    let factor = 10_i128.pow(scale);
    let sign = if value < 0 { "-" } else { "" };
    let magnitude = value.abs();
    let whole = magnitude / factor;
    let fraction = magnitude % factor;
    format!("{sign}{whole}.{fraction:0width$}", width = scale as usize)
}
#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use axum::{Json, Router};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::fs;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;
    use tokio::net::TcpListener;

    #[test]
    fn batch_policy_flushes_on_row_or_byte_threshold() {
        let policy = BatchPolicy::new(3, 10).expect("batch policy");
        assert!(!policy.should_flush(1, 5));
        assert!(policy.should_flush(3, 5));
        assert!(policy.should_flush(2, 10));
    }

    #[test]
    fn retry_policy_backoff_is_bounded_exponential() {
        let policy = RetryPolicy::new(5, Duration::from_millis(100), Duration::from_millis(500))
            .expect("retry policy");
        assert_eq!(policy.backoff_for_failure(0), Duration::from_millis(100));
        assert_eq!(policy.backoff_for_failure(1), Duration::from_millis(200));
        assert_eq!(policy.backoff_for_failure(2), Duration::from_millis(400));
        assert_eq!(policy.backoff_for_failure(3), Duration::from_millis(500));
        assert_eq!(policy.backoff_for_failure(10), Duration::from_millis(500));
    }

    #[test]
    fn debezium_mv_sink_encoding_builds_kafka_key_and_envelope() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("open"), Some("closed")])),
            ],
        )
        .expect("record batch");
        let tail = TailBatch {
            version: 42,
            batch,
            ops: vec![1, -1],
            times: vec![Some(1_234_000), Some(1_235_000)],
        };
        let rows = encode_tail_batch_as_debezium(
            &tail,
            &schema,
            &DebeziumSinkEncoding {
                source_name: "orders_sink".to_string(),
                database_name: "floe".to_string(),
                schema_name: "public".to_string(),
                table_name: "mv_orders".to_string(),
                key_columns: vec!["id".to_string()],
            },
        )
        .expect("encode Debezium sink rows");

        assert_eq!(rows.len(), 2);
        let first_key: serde_json::Value =
            serde_json::from_str(rows[0].key.as_deref().expect("key")).expect("key JSON");
        let first_value: serde_json::Value =
            serde_json::from_str(&rows[0].payload).expect("value JSON");
        assert_eq!(first_key["payload"]["id"], 1);
        assert_eq!(first_value["payload"]["op"], "c");
        assert_eq!(first_value["payload"]["after"]["status"], "open");
        assert_eq!(first_value["payload"]["before"], serde_json::Value::Null);
        assert_eq!(first_value["payload"]["source"]["name"], "orders_sink");
        assert_eq!(first_value["payload"]["source"]["db"], "floe");
        assert_eq!(first_value["payload"]["source"]["table"], "mv_orders");
        assert_eq!(
            first_value["payload"]["source"]["position"],
            "mv/mv_orders/42"
        );
        assert_eq!(first_value["payload"]["ts_ms"], 1234);

        let second_key: serde_json::Value =
            serde_json::from_str(rows[1].key.as_deref().expect("key")).expect("key JSON");
        let second_value: serde_json::Value =
            serde_json::from_str(&rows[1].payload).expect("value JSON");
        assert_eq!(second_key["payload"]["id"], 2);
        assert_eq!(second_value["payload"]["op"], "d");
        assert_eq!(second_value["payload"]["before"]["status"], "closed");
        assert_eq!(second_value["payload"]["after"], serde_json::Value::Null);
        assert_eq!(second_value["payload"]["ts_ms"], 1235);
    }

    #[test]
    fn http_idempotency_keys_include_mv_version_and_row_index() {
        let rows = vec![
            SinkRecord {
                version: 7,
                row_idx: 0,
                key: None,
                json: serde_json::json!({"k": 1}),
                payload: "{\"k\":1}".to_string(),
                byte_len: 7,
            },
            SinkRecord {
                version: 7,
                row_idx: 1,
                key: None,
                json: serde_json::json!({"k": 2}),
                payload: "{\"k\":2}".to_string(),
                byte_len: 7,
            },
        ];
        let (batch_key, keys) = build_http_idempotency_keys(&rows);
        assert_eq!(batch_key, "batch:7:0..7:1");
        assert_eq!(keys, "7:0,7:1");
    }

    #[tokio::test]
    async fn versioned_file_sink_writes_atomic_files_and_markers() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path().join("sink.jsonl");
        let base_str = base.to_string_lossy().to_string();
        let rows = vec![SinkRecord {
            version: 5,
            row_idx: 0,
            key: None,
            json: serde_json::json!({"auction": 1}),
            payload: "{\"auction\":1}".to_string(),
            byte_len: 13,
        }];

        write_versioned_file_batch(&base_str, 5, &rows)
            .await
            .expect("write versioned file");
        write_versioned_file_batch(&base_str, 5, &rows)
            .await
            .expect("idempotent rewrite");

        let data_path = format!("{base_str}.v5.jsonl");
        let manifest_path = format!("{base_str}.manifest");
        let commit_path = format!("{base_str}.commit");
        assert!(std::path::Path::new(&data_path).exists());
        assert!(std::path::Path::new(&manifest_path).exists());
        assert!(std::path::Path::new(&commit_path).exists());

        let manifest = fs::read_to_string(&manifest_path).expect("read manifest");
        assert_eq!(manifest.lines().count(), 1);
        assert!(manifest.contains("\"version\":5"));
        let commit = fs::read_to_string(&commit_path).expect("read commit");
        assert_eq!(commit.trim(), "5");
    }

    #[tokio::test]
    async fn file_effectively_once_crash_mid_batch_leaves_no_commit_marker() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path().join("sink.jsonl");
        let base_str = base.to_string_lossy().to_string();
        let (tx, rx) = mpsc::channel(8);
        let tracker = SinkQueueTracker::new("sink_file");
        let worker_path = base_str.clone();
        let task = tokio::spawn(async move {
            run_file_worker_effectively_once("sink_file", "mv_bid", &worker_path, rx, tracker, None)
                .await
        });

        tx.send(SinkEvent::Rows(vec![SinkRecord {
            version: 9,
            row_idx: 0,
            key: None,
            json: serde_json::json!({"auction": 9}),
            payload: "{\"auction\":9}".to_string(),
            byte_len: 13,
        }]))
        .await
        .expect("send row");
        tokio::time::sleep(Duration::from_millis(25)).await;
        task.abort();
        let _ = task.await;

        assert!(!std::path::Path::new(&format!("{base_str}.commit")).exists());
        assert!(!std::path::Path::new(&format!("{base_str}.v9.jsonl")).exists());
    }

    #[tokio::test]
    async fn file_effectively_once_restart_does_not_duplicate_committed_version() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path().join("sink.jsonl");
        let base_str = base.to_string_lossy().to_string();

        let rows = vec![SinkRecord {
            version: 3,
            row_idx: 0,
            key: None,
            json: serde_json::json!({"auction": 3}),
            payload: "{\"auction\":3}".to_string(),
            byte_len: 13,
        }];
        write_versioned_file_batch(&base_str, 3, &rows)
            .await
            .expect("first commit");
        write_versioned_file_batch(&base_str, 3, &rows)
            .await
            .expect("replayed commit");

        let manifest = fs::read_to_string(format!("{base_str}.manifest")).expect("manifest");
        assert_eq!(manifest.lines().count(), 1);
        let commit = fs::read_to_string(format!("{base_str}.commit")).expect("commit marker");
        assert_eq!(commit.trim(), "3");
    }

    #[derive(Clone)]
    struct HttpRetryState {
        attempts: Arc<AtomicUsize>,
        keys: Arc<Mutex<Vec<String>>>,
    }

    #[tokio::test]
    async fn http_retry_reuses_same_idempotency_key() {
        async fn collect(
            State(state): State<HttpRetryState>,
            headers: HeaderMap,
            Json(_payload): Json<serde_json::Value>,
        ) -> StatusCode {
            if let Some(value) = headers.get("idempotency-key")
                && let Ok(key) = value.to_str()
            {
                state.keys.lock().expect("lock").push(key.to_string());
            }
            let attempt = state.attempts.fetch_add(1, Ordering::Relaxed);
            if attempt == 0 {
                StatusCode::INTERNAL_SERVER_ERROR
            } else {
                StatusCode::OK
            }
        }

        let state = HttpRetryState {
            attempts: Arc::new(AtomicUsize::new(0)),
            keys: Arc::new(Mutex::new(Vec::new())),
        };
        let app = Router::new()
            .route("/collect", post(collect))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let client = Client::new();
        let rows = vec![SinkRecord {
            version: 11,
            row_idx: 4,
            key: None,
            json: serde_json::json!({"auction": 11}),
            payload: "{\"auction\":11}".to_string(),
            byte_len: 14,
        }];
        post_http_batch_with_retry(
            "sink_http",
            &client,
            &format!("http://{addr}/collect"),
            &rows,
            RetryPolicy::new(3, Duration::from_millis(10), Duration::from_millis(50))
                .expect("retry policy"),
        )
        .await
        .expect("http sink retry");
        server.abort();
        let _ = server.await;

        let keys = state.keys.lock().expect("lock");
        assert!(keys.len() >= 2);
        assert!(keys.iter().all(|key| key == "11:4"));
    }

    #[tokio::test]
    async fn http_sink_crash_mid_batch_emits_no_request_before_flush() {
        #[derive(Clone)]
        struct RequestCount(Arc<AtomicUsize>);
        async fn count(
            State(state): State<RequestCount>,
            Json(_payload): Json<serde_json::Value>,
        ) -> StatusCode {
            state.0.fetch_add(1, Ordering::Relaxed);
            StatusCode::OK
        }

        let counter = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/collect", post(count))
            .with_state(RequestCount(Arc::clone(&counter)));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let (tx, rx) = mpsc::channel(8);
        let tracker = SinkQueueTracker::new("sink_http");
        let url = format!("http://{addr}/collect");
        let worker = tokio::spawn(async move {
            let client = Client::new();
            run_http_worker(
                "sink_http",
                "mv_bid",
                &client,
                &url,
                rx,
                tracker,
                BatchPolicy::new(1000, usize::MAX).expect("batch policy"),
                RetryPolicy::new(3, Duration::from_millis(10), Duration::from_millis(20))
                    .expect("retry policy"),
                None,
            )
            .await
        });

        tx.send(SinkEvent::Rows(vec![SinkRecord {
            version: 12,
            row_idx: 0,
            key: None,
            json: serde_json::json!({"auction": 12}),
            payload: "{\"auction\":12}".to_string(),
            byte_len: 14,
        }]))
        .await
        .expect("send row");
        tokio::time::sleep(Duration::from_millis(25)).await;
        worker.abort();
        let _ = worker.await;
        server.abort();
        let _ = server.await;

        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }
}
