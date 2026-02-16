use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::scalar::ScalarValue;
use floe_executor::FloeQueryContext;
use floe_executor::MaterializedViewRegistry;
use floe_executor::tail::{TailBatch, TailParams, execute_tail, is_tail_canceled_error};
use futures::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use reqwest::Client;
use tokio::fs::OpenOptions;
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
        let depth = self.queued.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::record_sink_queue_depth(&self.sink_name, depth);
        self.latest_enqueued_version
            .fetch_max(version, Ordering::Relaxed);
        self.update_lag();
    }

    fn on_dequeue(&self) {
        let prev = self.queued.fetch_sub(1, Ordering::Relaxed);
        let depth = prev.saturating_sub(1);
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

struct SinkRecord {
    version: i64,
    json: serde_json::Value,
    payload: String,
    byte_len: usize,
}

enum SinkEvent {
    Row(SinkRecord),
    Flush { version: i64 },
}

pub fn spawn_sinks(
    sinks: Vec<SinkSpec>,
    query: FloeQueryContext,
    registry: Arc<MaterializedViewRegistry>,
    cancel: CancellationToken,
) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();
    for sink in sinks {
        let ctx = query.clone();
        let registry = registry.clone();
        let cancel = cancel.clone();
        handles.push(tokio::spawn(async move {
            let name = sink.name.clone();
            if let Err(err) = run_sink(sink, ctx, registry, cancel).await {
                if is_tail_canceled_error(&err) {
                    tracing::info!(sink = %name, "sink canceled");
                } else {
                    tracing::error!(sink = %name, error = %err, "sink failed");
                }
            }
        }));
    }
    handles
}

async fn run_sink(
    sink: SinkSpec,
    query: FloeQueryContext,
    registry: Arc<MaterializedViewRegistry>,
    cancel: CancellationToken,
) -> Result<()> {
    match sink.config {
        SinkConfig::Kafka {
            brokers,
            topic,
            mv,
            with_snapshot,
            as_of,
            batch_rows,
            batch_bytes,
            queue_capacity,
            retry_max_attempts,
            retry_base_ms,
            retry_max_backoff_ms,
            ..
        } => {
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
                with_snapshot.unwrap_or(false),
                as_of,
                queue_capacity,
                batch_policy,
                retry_policy,
            )
            .await
        }
        SinkConfig::File {
            path,
            mv,
            with_snapshot,
            as_of,
            append,
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
                with_snapshot.unwrap_or(false),
                as_of,
                append.unwrap_or(true),
                queue_capacity,
                batch_policy,
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
                with_snapshot.unwrap_or(false),
                as_of,
                queue_capacity,
                batch_policy,
                retry_policy,
            )
            .await
        }
    }
}

async fn run_kafka_sink(
    sink_name: &str,
    query: &FloeQueryContext,
    registry: Arc<MaterializedViewRegistry>,
    cancel: CancellationToken,
    brokers: &str,
    topic: &str,
    mv: &str,
    with_snapshot: bool,
    as_of: Option<i64>,
    queue_capacity: usize,
    batch_policy: BatchPolicy,
    retry_policy: RetryPolicy,
) -> Result<()> {
    if queue_capacity == 0 {
        bail!("sink queue_capacity must be greater than zero");
    }

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .context("create kafka producer")?;

    let stream = execute_tail(
        &query.session(),
        registry.as_ref(),
        TailParams {
            mv_name: mv.to_string(),
            with_snapshot,
            as_of,
        },
        cancel,
    )
    .await?;

    let (tx, rx) = mpsc::channel(queue_capacity);
    let tracker = SinkQueueTracker::new(sink_name);
    let producer_task = tokio::spawn(stream_tail_into_queue(stream, tx, Arc::clone(&tracker)));
    let consumer_result = run_kafka_worker(
        sink_name,
        &producer,
        topic,
        rx,
        tracker,
        batch_policy,
        retry_policy,
    )
    .await;
    let producer_result = producer_task
        .await
        .context("join sink queue producer task")
        .and_then(|result| result);

    consumer_result?;
    producer_result?;
    Ok(())
}

async fn run_file_sink(
    sink_name: &str,
    query: &FloeQueryContext,
    registry: Arc<MaterializedViewRegistry>,
    cancel: CancellationToken,
    path: &str,
    mv: &str,
    with_snapshot: bool,
    as_of: Option<i64>,
    append: bool,
    queue_capacity: usize,
    batch_policy: BatchPolicy,
) -> Result<()> {
    if queue_capacity == 0 {
        bail!("sink queue_capacity must be greater than zero");
    }

    let stream = execute_tail(
        &query.session(),
        registry.as_ref(),
        TailParams {
            mv_name: mv.to_string(),
            with_snapshot,
            as_of,
        },
        cancel,
    )
    .await?;

    let (tx, rx) = mpsc::channel(queue_capacity);
    let tracker = SinkQueueTracker::new(sink_name);
    let producer_task = tokio::spawn(stream_tail_into_queue(stream, tx, Arc::clone(&tracker)));
    let consumer_result = run_file_worker(path, append, rx, tracker, batch_policy).await;
    let producer_result = producer_task
        .await
        .context("join sink queue producer task")
        .and_then(|result| result);

    consumer_result?;
    producer_result?;
    Ok(())
}

async fn run_http_sink(
    sink_name: &str,
    query: &FloeQueryContext,
    registry: Arc<MaterializedViewRegistry>,
    cancel: CancellationToken,
    url: &str,
    mv: &str,
    with_snapshot: bool,
    as_of: Option<i64>,
    queue_capacity: usize,
    batch_policy: BatchPolicy,
    retry_policy: RetryPolicy,
) -> Result<()> {
    if queue_capacity == 0 {
        bail!("sink queue_capacity must be greater than zero");
    }

    let client = Client::new();
    let stream = execute_tail(
        &query.session(),
        registry.as_ref(),
        TailParams {
            mv_name: mv.to_string(),
            with_snapshot,
            as_of,
        },
        cancel,
    )
    .await?;

    let (tx, rx) = mpsc::channel(queue_capacity);
    let tracker = SinkQueueTracker::new(sink_name);
    let producer_task = tokio::spawn(stream_tail_into_queue(stream, tx, Arc::clone(&tracker)));
    let consumer_result = run_http_worker(
        sink_name,
        &client,
        url,
        rx,
        tracker,
        batch_policy,
        retry_policy,
    )
    .await;
    let producer_result = producer_task
        .await
        .context("join sink queue producer task")
        .and_then(|result| result);

    consumer_result?;
    producer_result?;
    Ok(())
}

async fn stream_tail_into_queue(
    mut stream: impl futures::Stream<Item = Result<TailBatch>> + Unpin,
    sender: mpsc::Sender<SinkEvent>,
    tracker: Arc<SinkQueueTracker>,
) -> Result<()> {
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let schema = batch.batch.schema();
        let version = batch.version;
        for row_idx in 0..batch.batch.num_rows() {
            let json = tail_row_to_json(&batch, row_idx, &schema)?;
            let payload = serde_json::to_string(&json).context("serialize sink row")?;
            let event = SinkEvent::Row(SinkRecord {
                version,
                json,
                byte_len: payload.len(),
                payload,
            });
            sender
                .send(event)
                .await
                .map_err(|_| anyhow!("sink queue consumer stopped"))?;
            tracker.on_enqueue(version);
        }

        sender
            .send(SinkEvent::Flush { version })
            .await
            .map_err(|_| anyhow!("sink queue consumer stopped"))?;
        tracker.on_enqueue(version);
    }
    Ok(())
}

async fn run_kafka_worker(
    sink_name: &str,
    producer: &FutureProducer,
    topic: &str,
    mut rx: mpsc::Receiver<SinkEvent>,
    tracker: Arc<SinkQueueTracker>,
    batch_policy: BatchPolicy,
    retry_policy: RetryPolicy,
) -> Result<()> {
    let mut buffer = Vec::new();
    let mut buffer_bytes = 0usize;

    while let Some(event) = rx.recv().await {
        tracker.on_dequeue();
        match event {
            SinkEvent::Row(row) => {
                buffer_bytes += row.byte_len;
                buffer.push(row);
                if batch_policy.should_flush(buffer.len(), buffer_bytes) {
                    flush_kafka_buffer(
                        sink_name,
                        producer,
                        topic,
                        &mut buffer,
                        &mut buffer_bytes,
                        retry_policy,
                        &tracker,
                        None,
                    )
                    .await?;
                }
            }
            SinkEvent::Flush { version } => {
                flush_kafka_buffer(
                    sink_name,
                    producer,
                    topic,
                    &mut buffer,
                    &mut buffer_bytes,
                    retry_policy,
                    &tracker,
                    Some(version),
                )
                .await?;
            }
        }
    }

    flush_kafka_buffer(
        sink_name,
        producer,
        topic,
        &mut buffer,
        &mut buffer_bytes,
        retry_policy,
        &tracker,
        None,
    )
    .await?;
    Ok(())
}

async fn run_file_worker(
    path: &str,
    append: bool,
    mut rx: mpsc::Receiver<SinkEvent>,
    tracker: Arc<SinkQueueTracker>,
    batch_policy: BatchPolicy,
) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .truncate(!append)
        .open(path)
        .await
        .with_context(|| format!("open sink file {path}"))?;

    let mut buffer = Vec::new();
    let mut buffer_bytes = 0usize;

    while let Some(event) = rx.recv().await {
        tracker.on_dequeue();
        match event {
            SinkEvent::Row(row) => {
                buffer_bytes += row.byte_len;
                buffer.push(row);
                if batch_policy.should_flush(buffer.len(), buffer_bytes) {
                    flush_file_buffer(&mut file, &mut buffer, &mut buffer_bytes, &tracker, None)
                        .await?;
                }
            }
            SinkEvent::Flush { version } => {
                flush_file_buffer(
                    &mut file,
                    &mut buffer,
                    &mut buffer_bytes,
                    &tracker,
                    Some(version),
                )
                .await?;
            }
        }
    }

    flush_file_buffer(&mut file, &mut buffer, &mut buffer_bytes, &tracker, None).await?;
    Ok(())
}

async fn run_http_worker(
    sink_name: &str,
    client: &Client,
    url: &str,
    mut rx: mpsc::Receiver<SinkEvent>,
    tracker: Arc<SinkQueueTracker>,
    batch_policy: BatchPolicy,
    retry_policy: RetryPolicy,
) -> Result<()> {
    let mut buffer = Vec::new();
    let mut buffer_bytes = 0usize;

    while let Some(event) = rx.recv().await {
        tracker.on_dequeue();
        match event {
            SinkEvent::Row(row) => {
                buffer_bytes += row.byte_len;
                buffer.push(row);
                if batch_policy.should_flush(buffer.len(), buffer_bytes) {
                    flush_http_buffer(
                        sink_name,
                        client,
                        url,
                        &mut buffer,
                        &mut buffer_bytes,
                        retry_policy,
                        &tracker,
                        None,
                    )
                    .await?;
                }
            }
            SinkEvent::Flush { version } => {
                flush_http_buffer(
                    sink_name,
                    client,
                    url,
                    &mut buffer,
                    &mut buffer_bytes,
                    retry_policy,
                    &tracker,
                    Some(version),
                )
                .await?;
            }
        }
    }

    flush_http_buffer(
        sink_name,
        client,
        url,
        &mut buffer,
        &mut buffer_bytes,
        retry_policy,
        &tracker,
        None,
    )
    .await?;
    Ok(())
}

async fn flush_kafka_buffer(
    sink_name: &str,
    producer: &FutureProducer,
    topic: &str,
    buffer: &mut Vec<SinkRecord>,
    buffer_bytes: &mut usize,
    retry_policy: RetryPolicy,
    tracker: &SinkQueueTracker,
    flush_version: Option<i64>,
) -> Result<()> {
    let mut flushed_version = flush_version.unwrap_or(-1);
    for row in buffer.iter() {
        send_kafka_with_retry(sink_name, producer, topic, &row.payload, retry_policy).await?;
        flushed_version = flushed_version.max(row.version);
    }
    buffer.clear();
    *buffer_bytes = 0;
    if flushed_version >= 0 {
        tracker.on_flushed(flushed_version);
    }
    Ok(())
}

async fn flush_file_buffer(
    file: &mut tokio::fs::File,
    buffer: &mut Vec<SinkRecord>,
    buffer_bytes: &mut usize,
    tracker: &SinkQueueTracker,
    flush_version: Option<i64>,
) -> Result<()> {
    let mut flushed_version = flush_version.unwrap_or(-1);
    for row in buffer.iter() {
        file.write_all(row.payload.as_bytes()).await?;
        file.write_all(b"\n").await?;
        flushed_version = flushed_version.max(row.version);
    }
    file.flush().await?;
    buffer.clear();
    *buffer_bytes = 0;
    if flushed_version >= 0 {
        tracker.on_flushed(flushed_version);
    }
    Ok(())
}

async fn flush_http_buffer(
    sink_name: &str,
    client: &Client,
    url: &str,
    buffer: &mut Vec<SinkRecord>,
    buffer_bytes: &mut usize,
    retry_policy: RetryPolicy,
    tracker: &SinkQueueTracker,
    flush_version: Option<i64>,
) -> Result<()> {
    if buffer.is_empty() {
        if let Some(version) = flush_version {
            tracker.on_flushed(version);
        }
        return Ok(());
    }

    post_http_batch_with_retry(sink_name, client, url, buffer, retry_policy).await?;
    let mut flushed_version = flush_version.unwrap_or(-1);
    for row in buffer.iter() {
        flushed_version = flushed_version.max(row.version);
    }
    buffer.clear();
    *buffer_bytes = 0;
    if flushed_version >= 0 {
        tracker.on_flushed(flushed_version);
    }
    Ok(())
}

async fn send_kafka_with_retry(
    sink_name: &str,
    producer: &FutureProducer,
    topic: &str,
    payload: &str,
    retry_policy: RetryPolicy,
) -> Result<()> {
    for attempt in 0..retry_policy.max_attempts {
        let record = FutureRecord::<(), _>::to(topic).payload(payload);
        match producer.send(record, Duration::from_secs(0)).await {
            Ok(_) => return Ok(()),
            Err((err, _message)) => {
                if attempt + 1 == retry_policy.max_attempts {
                    metrics::inc_sink_failure(sink_name, "kafka");
                    return Err(anyhow!("kafka sink delivery failed after retries: {err}"));
                }
                tokio::time::sleep(retry_policy.backoff_for_failure(attempt)).await;
            }
        }
    }
    unreachable!("retry loop should return or fail");
}

async fn post_http_batch_with_retry(
    sink_name: &str,
    client: &Client,
    url: &str,
    batch: &[SinkRecord],
    retry_policy: RetryPolicy,
) -> Result<()> {
    let payload = if batch.len() == 1 {
        batch[0].json.clone()
    } else {
        serde_json::Value::Array(batch.iter().map(|row| row.json.clone()).collect())
    };

    for attempt in 0..retry_policy.max_attempts {
        let result = client
            .post(url)
            .json(&payload)
            .send()
            .await
            .context("post sink batch")
            .and_then(|response| response.error_for_status().context("sink http error"));

        match result {
            Ok(_) => return Ok(()),
            Err(err) => {
                if attempt + 1 == retry_policy.max_attempts {
                    metrics::inc_sink_failure(sink_name, "http");
                    return Err(anyhow!("http sink request failed after retries: {err}"));
                }
                tokio::time::sleep(retry_policy.backoff_for_failure(attempt)).await;
            }
        }
    }
    unreachable!("retry loop should return or fail");
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
    let scalar = ScalarValue::try_from_array(array, row_idx)?;
    Ok(scalar_to_json(&scalar))
}

fn scalar_to_json(value: &ScalarValue) -> serde_json::Value {
    match value {
        ScalarValue::Boolean(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::Int8(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::Int16(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::Int32(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::Int64(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::UInt8(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::UInt16(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::UInt32(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::UInt64(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::Float32(Some(v)) => serde_json::Value::from(*v as f64),
        ScalarValue::Float64(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
            serde_json::Value::from(v.clone())
        }
        ScalarValue::TimestampMicrosecond(Some(v), _)
        | ScalarValue::TimestampMillisecond(Some(v), _)
        | ScalarValue::TimestampNanosecond(Some(v), _)
        | ScalarValue::TimestampSecond(Some(v), _) => serde_json::Value::from(*v),
        ScalarValue::Null => serde_json::Value::Null,
        other => serde_json::Value::String(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
