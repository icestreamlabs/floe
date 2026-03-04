use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::scalar::ScalarValue;
use floe_executor::FloeQueryContext;
use floe_executor::MaterializedViewRegistry;
use floe_executor::checkpoint::SinkCursor;
use floe_executor::tail::{TailBatch, TailParams, execute_tail, is_tail_canceled_error};
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

#[derive(Clone)]
struct KafkaEosConfig {
    transactional_id: String,
    checkpoint_topic: String,
    checkpoint_partition: i32,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct KafkaSinkCheckpointRecord {
    sink: String,
    mv_name: String,
    last_emitted_mv_version: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    row_index: Option<u64>,
    committed_at_unix_ms: u64,
}

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
    row_idx: u64,
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

#[allow(clippy::too_many_arguments)]
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
    checkpoint_tx: Option<mpsc::UnboundedSender<SinkCursor>>,
    transactional_id: Option<String>,
    checkpoint_topic: Option<String>,
    checkpoint_partition: Option<i32>,
) -> Result<()> {
    if queue_capacity == 0 {
        bail!("sink queue_capacity must be greater than zero");
    }

    let kafka_eos = checkpoint_topic.map(|topic_name| KafkaEosConfig {
        transactional_id: transactional_id.unwrap_or_else(|| {
            format!(
                "floe-{}-{}",
                sink_name.replace(' ', "_"),
                current_unix_time_ms()
            )
        }),
        checkpoint_topic: topic_name,
        checkpoint_partition: checkpoint_partition.unwrap_or(DEFAULT_KAFKA_CHECKPOINT_PARTITION),
    });

    let mut producer_config = ClientConfig::new();
    producer_config.set("bootstrap.servers", brokers);
    if let Some(eos) = &kafka_eos {
        producer_config
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("transactional.id", &eos.transactional_id);
    }
    let producer: FutureProducer = producer_config.create().context("create kafka producer")?;
    if kafka_eos.is_some() {
        producer
            .init_transactions(DEFAULT_KAFKA_TRANSACTION_TIMEOUT)
            .context("initialize kafka transactions for sink")?;
    }

    let mut effective_as_of = as_of;
    let mut effective_with_snapshot = with_snapshot;
    if let Some(eos) = &kafka_eos
        && let Some(cursor) = load_latest_kafka_checkpoint(brokers, eos, sink_name, mv).await?
    {
        effective_as_of = Some(
            effective_as_of
                .map(|value| value.max(cursor.last_emitted_mv_version))
                .unwrap_or(cursor.last_emitted_mv_version),
        );
        effective_with_snapshot = false;
    }

    let stream = execute_tail(
        &query.session(),
        registry.as_ref(),
        TailParams {
            mv_name: mv.to_string(),
            with_snapshot: effective_with_snapshot,
            as_of: effective_as_of,
        },
        cancel,
    )
    .await?;

    let (tx, rx) = mpsc::channel(queue_capacity);
    let tracker = SinkQueueTracker::new(sink_name);
    let producer_task = tokio::spawn(stream_tail_into_queue(stream, tx, Arc::clone(&tracker)));
    let consumer_result = run_kafka_worker(
        sink_name,
        mv,
        &producer,
        topic,
        rx,
        tracker,
        batch_policy,
        retry_policy,
        checkpoint_tx,
        kafka_eos,
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

#[allow(clippy::too_many_arguments)]
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
    effectively_once: bool,
    queue_capacity: usize,
    batch_policy: BatchPolicy,
    checkpoint_tx: Option<mpsc::UnboundedSender<SinkCursor>>,
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
    let consumer_result = if effectively_once {
        run_file_worker_effectively_once(sink_name, mv, path, rx, tracker, checkpoint_tx).await
    } else {
        run_file_worker(
            sink_name,
            mv,
            path,
            append,
            rx,
            tracker,
            batch_policy,
            checkpoint_tx,
        )
        .await
    };
    let producer_result = producer_task
        .await
        .context("join sink queue producer task")
        .and_then(|result| result);

    consumer_result?;
    producer_result?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
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
    checkpoint_tx: Option<mpsc::UnboundedSender<SinkCursor>>,
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
        mv,
        &client,
        url,
        rx,
        tracker,
        batch_policy,
        retry_policy,
        checkpoint_tx,
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
                row_idx: u64::try_from(row_idx).unwrap_or(u64::MAX),
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
    mv_name: &str,
    producer: &FutureProducer,
    topic: &str,
    mut rx: mpsc::Receiver<SinkEvent>,
    tracker: Arc<SinkQueueTracker>,
    batch_policy: BatchPolicy,
    retry_policy: RetryPolicy,
    checkpoint_tx: Option<mpsc::UnboundedSender<SinkCursor>>,
    kafka_eos: Option<KafkaEosConfig>,
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
                        mv_name,
                        producer,
                        topic,
                        &mut buffer,
                        &mut buffer_bytes,
                        retry_policy,
                        &tracker,
                        None,
                        &checkpoint_tx,
                        kafka_eos.as_ref(),
                    )
                    .await?;
                }
            }
            SinkEvent::Flush { version } => {
                flush_kafka_buffer(
                    sink_name,
                    mv_name,
                    producer,
                    topic,
                    &mut buffer,
                    &mut buffer_bytes,
                    retry_policy,
                    &tracker,
                    Some(version),
                    &checkpoint_tx,
                    kafka_eos.as_ref(),
                )
                .await?;
            }
        }
    }

    flush_kafka_buffer(
        sink_name,
        mv_name,
        producer,
        topic,
        &mut buffer,
        &mut buffer_bytes,
        retry_policy,
        &tracker,
        None,
        &checkpoint_tx,
        kafka_eos.as_ref(),
    )
    .await?;
    Ok(())
}

async fn run_file_worker(
    sink_name: &str,
    mv_name: &str,
    path: &str,
    append: bool,
    mut rx: mpsc::Receiver<SinkEvent>,
    tracker: Arc<SinkQueueTracker>,
    batch_policy: BatchPolicy,
    checkpoint_tx: Option<mpsc::UnboundedSender<SinkCursor>>,
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
                    let flushed_version =
                        buffer.iter().map(|entry| entry.version).max().unwrap_or(-1);
                    flush_file_buffer(&mut file, &mut buffer, &mut buffer_bytes, &tracker, None)
                        .await?;
                    if flushed_version >= 0 {
                        publish_sink_cursor(
                            &checkpoint_tx,
                            SinkCursor {
                                sink: sink_name.to_string(),
                                mv_name: mv_name.to_string(),
                                last_emitted_mv_version: flushed_version,
                                row_index: None,
                            },
                        );
                    }
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
                publish_sink_cursor(
                    &checkpoint_tx,
                    SinkCursor {
                        sink: sink_name.to_string(),
                        mv_name: mv_name.to_string(),
                        last_emitted_mv_version: version,
                        row_index: None,
                    },
                );
            }
        }
    }

    let final_version = buffer.iter().map(|entry| entry.version).max().unwrap_or(-1);
    flush_file_buffer(&mut file, &mut buffer, &mut buffer_bytes, &tracker, None).await?;
    if final_version >= 0 {
        publish_sink_cursor(
            &checkpoint_tx,
            SinkCursor {
                sink: sink_name.to_string(),
                mv_name: mv_name.to_string(),
                last_emitted_mv_version: final_version,
                row_index: None,
            },
        );
    }
    Ok(())
}

async fn run_file_worker_effectively_once(
    sink_name: &str,
    mv_name: &str,
    path: &str,
    mut rx: mpsc::Receiver<SinkEvent>,
    tracker: Arc<SinkQueueTracker>,
    checkpoint_tx: Option<mpsc::UnboundedSender<SinkCursor>>,
) -> Result<()> {
    let mut pending = Vec::new();

    while let Some(event) = rx.recv().await {
        tracker.on_dequeue();
        match event {
            SinkEvent::Row(row) => pending.push(row),
            SinkEvent::Flush { version } => {
                let mut rows = Vec::new();
                let mut retained = Vec::new();
                for row in pending.drain(..) {
                    if row.version <= version {
                        rows.push(row);
                    } else {
                        retained.push(row);
                    }
                }
                pending = retained;
                write_versioned_file_batch(path, version, &rows).await?;
                tracker.on_flushed(version);
                publish_sink_cursor(
                    &checkpoint_tx,
                    SinkCursor {
                        sink: sink_name.to_string(),
                        mv_name: mv_name.to_string(),
                        last_emitted_mv_version: version,
                        row_index: None,
                    },
                );
            }
        }
    }

    if !pending.is_empty() {
        let final_version = pending.iter().map(|row| row.version).max().unwrap_or(-1);
        if final_version >= 0 {
            write_versioned_file_batch(path, final_version, &pending).await?;
            tracker.on_flushed(final_version);
            publish_sink_cursor(
                &checkpoint_tx,
                SinkCursor {
                    sink: sink_name.to_string(),
                    mv_name: mv_name.to_string(),
                    last_emitted_mv_version: final_version,
                    row_index: None,
                },
            );
        }
    }
    Ok(())
}

async fn write_versioned_file_batch(path: &str, version: i64, rows: &[SinkRecord]) -> Result<()> {
    let data_path = format!("{path}.v{version}.jsonl");
    if fs::try_exists(&data_path).await.unwrap_or(false) {
        return Ok(());
    }

    let tmp_path = format!("{data_path}.pending");
    let mut tmp = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)
        .await
        .with_context(|| format!("open pending sink file {}", tmp_path))?;
    for row in rows {
        tmp.write_all(row.payload.as_bytes()).await?;
        tmp.write_all(b"\n").await?;
    }
    tmp.flush().await?;
    tmp.sync_all().await?;
    drop(tmp);
    fs::rename(&tmp_path, &data_path)
        .await
        .with_context(|| format!("commit versioned sink file {}", data_path))?;

    let manifest_path = format!("{path}.manifest");
    let mut manifest = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&manifest_path)
        .await
        .with_context(|| format!("open sink manifest {}", manifest_path))?;
    let manifest_entry = serde_json::json!({
        "version": version,
        "file": data_path,
        "rows": rows.len(),
        "committed_at_unix_ms": current_unix_time_ms(),
    });
    let line = serde_json::to_string(&manifest_entry).context("serialize sink manifest entry")?;
    manifest.write_all(line.as_bytes()).await?;
    manifest.write_all(b"\n").await?;
    manifest.flush().await?;
    manifest.sync_all().await?;

    let commit_tmp_path = format!("{path}.commit.pending");
    let commit_path = format!("{path}.commit");
    fs::write(&commit_tmp_path, version.to_string())
        .await
        .with_context(|| format!("write sink commit marker {}", commit_tmp_path))?;
    fs::rename(&commit_tmp_path, &commit_path)
        .await
        .with_context(|| format!("commit sink marker {}", commit_path))?;
    Ok(())
}

async fn run_http_worker(
    sink_name: &str,
    mv_name: &str,
    client: &Client,
    url: &str,
    mut rx: mpsc::Receiver<SinkEvent>,
    tracker: Arc<SinkQueueTracker>,
    batch_policy: BatchPolicy,
    retry_policy: RetryPolicy,
    checkpoint_tx: Option<mpsc::UnboundedSender<SinkCursor>>,
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
                        mv_name,
                        client,
                        url,
                        &mut buffer,
                        &mut buffer_bytes,
                        retry_policy,
                        &tracker,
                        None,
                        &checkpoint_tx,
                    )
                    .await?;
                }
            }
            SinkEvent::Flush { version } => {
                flush_http_buffer(
                    sink_name,
                    mv_name,
                    client,
                    url,
                    &mut buffer,
                    &mut buffer_bytes,
                    retry_policy,
                    &tracker,
                    Some(version),
                    &checkpoint_tx,
                )
                .await?;
            }
        }
    }

    flush_http_buffer(
        sink_name,
        mv_name,
        client,
        url,
        &mut buffer,
        &mut buffer_bytes,
        retry_policy,
        &tracker,
        None,
        &checkpoint_tx,
    )
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn flush_kafka_buffer(
    sink_name: &str,
    mv_name: &str,
    producer: &FutureProducer,
    topic: &str,
    buffer: &mut Vec<SinkRecord>,
    buffer_bytes: &mut usize,
    retry_policy: RetryPolicy,
    tracker: &SinkQueueTracker,
    flush_version: Option<i64>,
    checkpoint_tx: &Option<mpsc::UnboundedSender<SinkCursor>>,
    kafka_eos: Option<&KafkaEosConfig>,
) -> Result<()> {
    let mut flushed_version = flush_version.unwrap_or(-1);
    for row in buffer.iter() {
        flushed_version = flushed_version.max(row.version);
    }
    if flushed_version < 0 {
        buffer.clear();
        *buffer_bytes = 0;
        return Ok(());
    }

    if let Some(eos) = kafka_eos {
        send_kafka_transactional_batch_with_retry(
            sink_name,
            mv_name,
            producer,
            topic,
            buffer,
            flushed_version,
            retry_policy,
            eos,
        )
        .await?;
    } else {
        for row in buffer.iter() {
            send_kafka_with_retry(sink_name, producer, topic, &row.payload, retry_policy).await?;
        }
    }
    buffer.clear();
    *buffer_bytes = 0;
    if flushed_version >= 0 {
        tracker.on_flushed(flushed_version);
        publish_sink_cursor(
            checkpoint_tx,
            SinkCursor {
                sink: sink_name.to_string(),
                mv_name: mv_name.to_string(),
                last_emitted_mv_version: flushed_version,
                row_index: None,
            },
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn send_kafka_transactional_batch_with_retry(
    sink_name: &str,
    mv_name: &str,
    producer: &FutureProducer,
    topic: &str,
    rows: &[SinkRecord],
    flushed_version: i64,
    retry_policy: RetryPolicy,
    eos: &KafkaEosConfig,
) -> Result<()> {
    for attempt in 0..retry_policy.max_attempts {
        if let Err(err) = producer.begin_transaction() {
            if attempt + 1 == retry_policy.max_attempts {
                return Err(anyhow!(
                    "kafka sink failed to begin transaction after retries: {err}"
                ));
            }
            tokio::time::sleep(retry_policy.backoff_for_failure(attempt)).await;
            continue;
        }

        let mut step_error: Option<anyhow::Error> = None;
        for row in rows {
            let record = FutureRecord::<(), _>::to(topic).payload(&row.payload);
            if let Err((err, _message)) = producer.send(record, Duration::from_secs(0)).await {
                step_error = Some(anyhow!(
                    "kafka sink transactional row publish failed: {err}"
                ));
                break;
            }
        }

        if step_error.is_none() {
            let checkpoint = KafkaSinkCheckpointRecord {
                sink: sink_name.to_string(),
                mv_name: mv_name.to_string(),
                last_emitted_mv_version: flushed_version,
                row_index: None,
                committed_at_unix_ms: current_unix_time_ms(),
            };
            let payload =
                serde_json::to_string(&checkpoint).context("serialize kafka checkpoint")?;
            let checkpoint_record = FutureRecord::<str, _>::to(&eos.checkpoint_topic)
                .partition(eos.checkpoint_partition)
                .key(sink_name)
                .payload(&payload);
            if let Err((err, _message)) = producer
                .send(checkpoint_record, Duration::from_secs(0))
                .await
            {
                step_error = Some(anyhow!(
                    "kafka sink transactional checkpoint publish failed: {err}"
                ));
            }
        }

        if let Some(err) = step_error {
            let _ = producer.abort_transaction(DEFAULT_KAFKA_TRANSACTION_TIMEOUT);
            if attempt + 1 == retry_policy.max_attempts {
                metrics::inc_sink_failure(sink_name, "kafka");
                return Err(err);
            }
            metrics::inc_sink_retry(sink_name, "kafka");
            tokio::time::sleep(retry_policy.backoff_for_failure(attempt)).await;
            continue;
        }

        match producer.commit_transaction(DEFAULT_KAFKA_TRANSACTION_TIMEOUT) {
            Ok(()) => return Ok(()),
            Err(err) => {
                let _ = producer.abort_transaction(DEFAULT_KAFKA_TRANSACTION_TIMEOUT);
                if attempt + 1 == retry_policy.max_attempts {
                    metrics::inc_sink_failure(sink_name, "kafka");
                    return Err(anyhow!(
                        "kafka sink transaction commit failed after retries: {err}"
                    ));
                }
                metrics::inc_sink_retry(sink_name, "kafka");
                tokio::time::sleep(retry_policy.backoff_for_failure(attempt)).await;
            }
        }
    }
    unreachable!("transaction retry loop should return or fail");
}

async fn load_latest_kafka_checkpoint(
    brokers: &str,
    eos: &KafkaEosConfig,
    sink_name: &str,
    mv_name: &str,
) -> Result<Option<SinkCursor>> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", brokers)
        .set(
            "group.id",
            &format!("floe-sink-cursor-reader-{}", current_unix_time_ms()),
        )
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest");
    let consumer: BaseConsumer = client_config
        .create()
        .context("create kafka checkpoint consumer")?;
    let (low, high) = consumer
        .fetch_watermarks(
            &eos.checkpoint_topic,
            eos.checkpoint_partition,
            DEFAULT_KAFKA_TRANSACTION_TIMEOUT,
        )
        .context("fetch kafka checkpoint watermarks")?;
    if high <= low {
        return Ok(None);
    }

    let start = if high - low > 2048 { high - 2048 } else { low };
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(
        &eos.checkpoint_topic,
        eos.checkpoint_partition,
        Offset::Offset(start),
    )
    .with_context(|| {
        format!(
            "assign kafka checkpoint topic {} partition {}",
            eos.checkpoint_topic, eos.checkpoint_partition
        )
    })?;
    consumer
        .assign(&tpl)
        .context("assign checkpoint consumer")?;

    let mut latest: Option<(i64, SinkCursor)> = None;
    let mut idle_polls = 0usize;
    let target_last_offset = high.saturating_sub(1);
    while idle_polls < 5 {
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(message)) => {
                idle_polls = 0;
                let payload = match message.payload() {
                    Some(payload) => payload,
                    None => continue,
                };
                let record: KafkaSinkCheckpointRecord = match serde_json::from_slice(payload) {
                    Ok(record) => record,
                    Err(_) => continue,
                };
                if record.sink != sink_name || record.mv_name != mv_name {
                    if message.offset() >= target_last_offset {
                        break;
                    }
                    continue;
                }
                let cursor = SinkCursor {
                    sink: record.sink,
                    mv_name: record.mv_name,
                    last_emitted_mv_version: record.last_emitted_mv_version,
                    row_index: record.row_index,
                };
                let replace = latest
                    .as_ref()
                    .map(|(offset, _)| message.offset() > *offset)
                    .unwrap_or(true);
                if replace {
                    latest = Some((message.offset(), cursor));
                }
                if message.offset() >= target_last_offset {
                    break;
                }
            }
            Some(Err(_)) => {}
            None => {
                idle_polls = idle_polls.saturating_add(1);
            }
        }
    }

    Ok(latest.map(|(_, cursor)| cursor))
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

#[allow(clippy::too_many_arguments)]
async fn flush_http_buffer(
    sink_name: &str,
    mv_name: &str,
    client: &Client,
    url: &str,
    buffer: &mut Vec<SinkRecord>,
    buffer_bytes: &mut usize,
    retry_policy: RetryPolicy,
    tracker: &SinkQueueTracker,
    flush_version: Option<i64>,
    checkpoint_tx: &Option<mpsc::UnboundedSender<SinkCursor>>,
) -> Result<()> {
    if buffer.is_empty() {
        if let Some(version) = flush_version {
            tracker.on_flushed(version);
            publish_sink_cursor(
                checkpoint_tx,
                SinkCursor {
                    sink: sink_name.to_string(),
                    mv_name: mv_name.to_string(),
                    last_emitted_mv_version: version,
                    row_index: None,
                },
            );
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
        publish_sink_cursor(
            checkpoint_tx,
            SinkCursor {
                sink: sink_name.to_string(),
                mv_name: mv_name.to_string(),
                last_emitted_mv_version: flushed_version,
                row_index: None,
            },
        );
    }
    Ok(())
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
                metrics::inc_sink_retry(sink_name, "kafka");
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
    let (idempotency_key, idempotency_key_list) = build_http_idempotency_keys(batch);

    for attempt in 0..retry_policy.max_attempts {
        let result = client
            .post(url)
            .header("Idempotency-Key", &idempotency_key)
            .header("X-Floe-Idempotency-Keys", &idempotency_key_list)
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
                metrics::inc_sink_retry(sink_name, "http");
                tokio::time::sleep(retry_policy.backoff_for_failure(attempt)).await;
            }
        }
    }
    unreachable!("retry loop should return or fail");
}

fn build_http_idempotency_keys(batch: &[SinkRecord]) -> (String, String) {
    let key_parts: Vec<String> = batch
        .iter()
        .map(|row| format!("{}:{}", row.version, row.row_idx))
        .collect();
    let first_key = key_parts.first().cloned().unwrap_or_default();
    let last_key = key_parts.last().cloned().unwrap_or_default();
    let idempotency_key = if key_parts.len() == 1 {
        first_key
    } else {
        format!("batch:{first_key}..{last_key}")
    };
    (idempotency_key, key_parts.join(","))
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
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use axum::{Json, Router};
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
    fn http_idempotency_keys_include_mv_version_and_row_index() {
        let rows = vec![
            SinkRecord {
                version: 7,
                row_idx: 0,
                json: serde_json::json!({"k": 1}),
                payload: "{\"k\":1}".to_string(),
                byte_len: 7,
            },
            SinkRecord {
                version: 7,
                row_idx: 1,
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

        tx.send(SinkEvent::Row(SinkRecord {
            version: 9,
            row_idx: 0,
            json: serde_json::json!({"auction": 9}),
            payload: "{\"auction\":9}".to_string(),
            byte_len: 13,
        }))
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

        tx.send(SinkEvent::Row(SinkRecord {
            version: 12,
            row_idx: 0,
            json: serde_json::json!({"auction": 12}),
            payload: "{\"auction\":12}".to_string(),
            byte_len: 14,
        }))
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
