use super::*;
use futures::future::join_all;
use std::sync::atomic::{AtomicU64, Ordering};

static KAFKA_SINK_FLUSH_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
const KAFKA_SINK_FLUSH_LOG_EVERY: u64 = 256;
const KAFKA_NON_TX_IN_FLIGHT_LIMIT: usize = 1024;
const KAFKA_CHECKPOINT_RECOVERY_BACKSCAN_RECORDS: i64 = 4096;

#[derive(Clone)]
pub(super) struct KafkaEosConfig {
    transactional_id: String,
    checkpoint_topic: String,
    checkpoint_partition: i32,
}

pub(super) struct KafkaSinkConfig<'a> {
    pub(super) sink_name: &'a str,
    pub(super) changelog: ChangelogSourceConfig<'a>,
    pub(super) brokers: &'a str,
    pub(super) topic: &'a str,
    pub(super) queue_capacity: usize,
    pub(super) batch_policy: BatchPolicy,
    pub(super) retry_policy: RetryPolicy,
    pub(super) checkpoint_tx: Option<SinkCheckpointSender>,
    pub(super) transactional_id: Option<String>,
    pub(super) checkpoint_topic: Option<String>,
    pub(super) checkpoint_partition: Option<i32>,
    pub(super) encoding: SinkEncoding,
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

pub(super) async fn run_kafka_sink(config: KafkaSinkConfig<'_>) -> Result<()> {
    if config.queue_capacity == 0 {
        bail!("sink queue_capacity must be greater than zero");
    }

    let kafka_eos = config.checkpoint_topic.map(|topic_name| KafkaEosConfig {
        transactional_id: config
            .transactional_id
            .unwrap_or_else(|| default_kafka_transactional_id(config.sink_name)),
        checkpoint_topic: topic_name,
        checkpoint_partition: config
            .checkpoint_partition
            .unwrap_or(DEFAULT_KAFKA_CHECKPOINT_PARTITION),
    });

    let mut producer_config = ClientConfig::new();
    producer_config.set("bootstrap.servers", config.brokers);
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

    let mut effective_as_of = config.changelog.as_of;
    let mut effective_with_snapshot = config.changelog.with_snapshot;
    if let Some(eos) = &kafka_eos
        && let Some(cursor) =
            load_latest_kafka_checkpoint(config.brokers, eos, config.sink_name, config.changelog.mv)
                .await?
    {
        effective_as_of = Some(
            effective_as_of
                .map(|value| value.max(cursor.last_emitted_mv_version))
                .unwrap_or(cursor.last_emitted_mv_version),
        );
        effective_with_snapshot = false;
    }

    let stream = execute_mv_changelog(
        config.changelog.registry.as_ref(),
        MvChangelogParams {
            mv_name: config.changelog.mv.to_string(),
            with_snapshot: effective_with_snapshot,
            as_of: effective_as_of,
        },
        config.changelog.cancel.clone(),
    )
    .await?;

    let (tx, rx) = mpsc::channel(config.queue_capacity);
    let tracker = SinkQueueTracker::new(config.sink_name);
    let producer_task = tokio::spawn(stream_changelog_into_queue_with_encoding(
        stream,
        tx,
        Arc::clone(&tracker),
        config.encoding,
    ));
    let consumer_result = run_kafka_worker(KafkaWorkerConfig {
        sink_name: config.sink_name,
        mv_name: config.changelog.mv,
        producer: &producer,
        topic: config.topic,
        rx,
        tracker,
        batch_policy: config.batch_policy,
        retry_policy: config.retry_policy,
        checkpoint_tx: config.checkpoint_tx,
        kafka_eos,
    })
    .await;
    let producer_result = producer_task
        .await
        .context("join sink queue producer task")
        .and_then(|result| result);

    consumer_result?;
    producer_result?;
    Ok(())
}

pub(super) struct KafkaWorkerConfig<'a> {
    pub(super) sink_name: &'a str,
    pub(super) mv_name: &'a str,
    pub(super) producer: &'a FutureProducer,
    pub(super) topic: &'a str,
    pub(super) rx: mpsc::Receiver<SinkEvent>,
    pub(super) tracker: Arc<SinkQueueTracker>,
    pub(super) batch_policy: BatchPolicy,
    pub(super) retry_policy: RetryPolicy,
    pub(super) checkpoint_tx: Option<SinkCheckpointSender>,
    pub(super) kafka_eos: Option<KafkaEosConfig>,
}

pub(super) async fn run_kafka_worker(config: KafkaWorkerConfig<'_>) -> Result<()> {
    let backend = KafkaSinkBackend {
        sink_name: config.sink_name,
        mv_name: config.mv_name,
        producer: config.producer,
        topic: config.topic,
        retry_policy: config.retry_policy,
        tracker: Arc::clone(&config.tracker),
        checkpoint_tx: config.checkpoint_tx,
        kafka_eos: config.kafka_eos,
    };
    run_buffered_sink_worker(config.rx, config.tracker, config.batch_policy, backend).await
}

struct KafkaSinkBackend<'a> {
    sink_name: &'a str,
    mv_name: &'a str,
    producer: &'a FutureProducer,
    topic: &'a str,
    retry_policy: RetryPolicy,
    tracker: Arc<SinkQueueTracker>,
    checkpoint_tx: Option<SinkCheckpointSender>,
    kafka_eos: Option<KafkaEosConfig>,
}

impl BufferedSinkBackend for KafkaSinkBackend<'_> {
    async fn flush(
        &mut self,
        buffer: &mut Vec<SinkRecord>,
        buffer_bytes: &mut usize,
        flush_version: Option<i64>,
    ) -> Result<()> {
        flush_kafka_buffer(
            KafkaFlushContext {
                sink_name: self.sink_name,
                mv_name: self.mv_name,
                producer: self.producer,
                topic: self.topic,
                retry_policy: self.retry_policy,
                tracker: &self.tracker,
                checkpoint_tx: &self.checkpoint_tx,
                kafka_eos: self.kafka_eos.as_ref(),
            },
            buffer,
            buffer_bytes,
            flush_version,
        )
        .await
    }
}

struct KafkaFlushContext<'a> {
    sink_name: &'a str,
    mv_name: &'a str,
    producer: &'a FutureProducer,
    topic: &'a str,
    retry_policy: RetryPolicy,
    tracker: &'a SinkQueueTracker,
    checkpoint_tx: &'a Option<SinkCheckpointSender>,
    kafka_eos: Option<&'a KafkaEosConfig>,
}

async fn flush_kafka_buffer(
    context: KafkaFlushContext<'_>,
    buffer: &mut Vec<SinkRecord>,
    buffer_bytes: &mut usize,
    flush_version: Option<i64>,
) -> Result<()> {
    let flush_start = std::time::Instant::now();
    let rows_in_flush = buffer.len();
    let bytes_in_flush = *buffer_bytes;
    let mut flushed_version = flush_version.unwrap_or(-1);
    for row in buffer.iter() {
        flushed_version = flushed_version.max(row.version);
    }
    if flushed_version < 0 {
        buffer.clear();
        *buffer_bytes = 0;
        return Ok(());
    }

    if let Some(eos) = context.kafka_eos {
        send_kafka_transactional_batch_with_retry(KafkaTransactionalBatch {
            sink_name: context.sink_name,
            mv_name: context.mv_name,
            producer: context.producer,
            topic: context.topic,
            rows: buffer,
            flushed_version,
            retry_policy: context.retry_policy,
            eos,
        })
        .await?;
    } else {
        send_kafka_batch_with_retry(
            context.sink_name,
            context.producer,
            context.topic,
            buffer,
            context.retry_policy,
        )
        .await?;
    }
    buffer.clear();
    *buffer_bytes = 0;
    if flushed_version >= 0 {
        context.tracker.on_flushed(flushed_version);
        publish_sink_cursor(
            context.checkpoint_tx,
            SinkCursor {
                sink: context.sink_name.to_string(),
                mv_name: context.mv_name.to_string(),
                last_emitted_mv_version: flushed_version,
                row_index: None,
            },
        )
        .await?;
    }
    let flush_seq = KAFKA_SINK_FLUSH_LOG_COUNTER.fetch_add(1, Ordering::Relaxed);
    if flush_seq < 16 || flush_seq.is_multiple_of(KAFKA_SINK_FLUSH_LOG_EVERY) {
        let flush_reason = if flush_version.is_some() {
            "version"
        } else {
            "batch_or_drain"
        };
        tracing::info!(
            sink = %context.sink_name,
            mv = %context.mv_name,
            flush_seq,
            flush_reason,
            rows = rows_in_flush,
            bytes = bytes_in_flush,
            flushed_version,
            transactional = context.kafka_eos.is_some(),
            latency_ms = flush_start.elapsed().as_millis() as u64,
            "kafka sink flush metrics"
        );
    }
    Ok(())
}

struct KafkaTransactionalBatch<'a> {
    sink_name: &'a str,
    mv_name: &'a str,
    producer: &'a FutureProducer,
    topic: &'a str,
    rows: &'a [SinkRecord],
    flushed_version: i64,
    retry_policy: RetryPolicy,
    eos: &'a KafkaEosConfig,
}

async fn send_kafka_transactional_batch_with_retry(
    batch: KafkaTransactionalBatch<'_>,
) -> Result<()> {
    for attempt in 0..batch.retry_policy.max_attempts {
        if let Err(err) = batch.producer.begin_transaction() {
            if attempt + 1 == batch.retry_policy.max_attempts {
                return Err(anyhow!(
                    "kafka sink failed to begin transaction after retries: {err}"
                ));
            }
            tokio::time::sleep(batch.retry_policy.backoff_for_failure(attempt)).await;
            continue;
        }

        let mut step_error = send_kafka_transactional_rows(batch.producer, batch.topic, batch.rows)
            .await
            .err();

        if step_error.is_none() {
            let checkpoint = KafkaSinkCheckpointRecord {
                sink: batch.sink_name.to_string(),
                mv_name: batch.mv_name.to_string(),
                last_emitted_mv_version: batch.flushed_version,
                row_index: None,
                committed_at_unix_ms: current_unix_time_ms(),
            };
            let payload =
                serde_json::to_string(&checkpoint).context("serialize kafka checkpoint")?;
            let checkpoint_key = kafka_checkpoint_key(batch.sink_name, batch.mv_name);
            let checkpoint_record = FutureRecord::<str, _>::to(&batch.eos.checkpoint_topic)
                .partition(batch.eos.checkpoint_partition)
                .key(&checkpoint_key)
                .payload(&payload);
            if let Err((err, _message)) = batch
                .producer
                .send(checkpoint_record, Duration::from_secs(0))
                .await
            {
                step_error = Some(anyhow!(
                    "kafka sink transactional checkpoint publish failed: {err}"
                ));
            }
        }

        if let Some(err) = step_error {
            let _ = batch
                .producer
                .abort_transaction(DEFAULT_KAFKA_TRANSACTION_TIMEOUT);
            if attempt + 1 == batch.retry_policy.max_attempts {
                metrics::inc_sink_failure(batch.sink_name, "kafka");
                return Err(err);
            }
            metrics::inc_sink_retry(batch.sink_name, "kafka");
            tokio::time::sleep(batch.retry_policy.backoff_for_failure(attempt)).await;
            continue;
        }

        match batch
            .producer
            .commit_transaction(DEFAULT_KAFKA_TRANSACTION_TIMEOUT)
        {
            Ok(()) => return Ok(()),
            Err(err) => {
                let _ = batch
                    .producer
                    .abort_transaction(DEFAULT_KAFKA_TRANSACTION_TIMEOUT);
                if attempt + 1 == batch.retry_policy.max_attempts {
                    metrics::inc_sink_failure(batch.sink_name, "kafka");
                    return Err(anyhow!(
                        "kafka sink transaction commit failed after retries: {err}"
                    ));
                }
                metrics::inc_sink_retry(batch.sink_name, "kafka");
                tokio::time::sleep(batch.retry_policy.backoff_for_failure(attempt)).await;
            }
        }
    }
    unreachable!("transaction retry loop should return or fail");
}

async fn send_kafka_transactional_rows(
    producer: &FutureProducer,
    topic: &str,
    rows: &[SinkRecord],
) -> Result<()> {
    for chunk in rows.chunks(KAFKA_NON_TX_IN_FLIGHT_LIMIT) {
        let mut deliveries = Vec::with_capacity(chunk.len());
        for row in chunk {
            let record = kafka_record(topic, row);
            match producer.send_result(record) {
                Ok(delivery) => deliveries.push(delivery),
                Err((err, _message)) => {
                    return Err(anyhow!(
                        "kafka sink transactional row enqueue failed: {err}"
                    ));
                }
            }
        }
        for delivery in join_all(deliveries).await {
            match delivery {
                Ok(Ok(_)) => {}
                Ok(Err((err, _message))) => {
                    return Err(anyhow!(
                        "kafka sink transactional row publish failed: {err}"
                    ));
                }
                Err(err) => {
                    return Err(anyhow!(
                        "kafka sink transactional row delivery future was canceled: {err}"
                    ));
                }
            }
        }
    }
    Ok(())
}

pub(super) async fn load_latest_kafka_checkpoint(
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
            format!("floe-sink-cursor-reader-{}", current_unix_time_ms()),
        )
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
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

    let checkpoint_key = kafka_checkpoint_key(sink_name, mv_name);
    let tail_start = high
        .saturating_sub(KAFKA_CHECKPOINT_RECOVERY_BACKSCAN_RECORDS)
        .max(low);
    if let Some(cursor) = scan_kafka_checkpoint_range(
        &consumer,
        eos,
        tail_start,
        high,
        &checkpoint_key,
        sink_name,
        mv_name,
    )? {
        return Ok(Some(cursor));
    }
    if tail_start <= low {
        return Ok(None);
    }
    scan_kafka_checkpoint_range(
        &consumer,
        eos,
        low,
        tail_start,
        &checkpoint_key,
        sink_name,
        mv_name,
    )
}

fn scan_kafka_checkpoint_range(
    consumer: &BaseConsumer,
    eos: &KafkaEosConfig,
    start_offset: i64,
    high_watermark: i64,
    checkpoint_key: &str,
    sink_name: &str,
    mv_name: &str,
) -> Result<Option<SinkCursor>> {
    if high_watermark <= start_offset {
        return Ok(None);
    }

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(
        &eos.checkpoint_topic,
        eos.checkpoint_partition,
        Offset::Offset(start_offset),
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
    let target_last_offset = high_watermark.saturating_sub(1);
    while idle_polls < 5 {
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(message)) => {
                idle_polls = 0;
                if let Some(key) = message.key()
                    && key != checkpoint_key.as_bytes()
                {
                    continue;
                }
                let payload = match message.payload() {
                    Some(payload) => payload,
                    None => continue,
                };
                consider_kafka_checkpoint_payload(
                    &mut latest,
                    message.offset(),
                    payload,
                    sink_name,
                    mv_name,
                );
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

pub(super) fn kafka_checkpoint_key(sink_name: &str, mv_name: &str) -> String {
    format!("{sink_name}\0{mv_name}")
}

pub(super) fn default_kafka_transactional_id(sink_name: &str) -> String {
    format!("floe-{}", sink_name.replace(' ', "_"))
}

pub(super) fn consider_kafka_checkpoint_payload(
    latest: &mut Option<(i64, SinkCursor)>,
    offset: i64,
    payload: &[u8],
    sink_name: &str,
    mv_name: &str,
) {
    let Ok(record) = serde_json::from_slice::<KafkaSinkCheckpointRecord>(payload) else {
        return;
    };
    if record.sink != sink_name || record.mv_name != mv_name {
        return;
    }
    let replace = latest
        .as_ref()
        .map(|(latest_offset, _)| offset > *latest_offset)
        .unwrap_or(true);
    if replace {
        *latest = Some((
            offset,
            SinkCursor {
                sink: record.sink,
                mv_name: record.mv_name,
                last_emitted_mv_version: record.last_emitted_mv_version,
                row_index: record.row_index,
            },
        ));
    }
}

pub(super) async fn send_kafka_batch_with_retry(
    sink_name: &str,
    producer: &FutureProducer,
    topic: &str,
    rows: &[SinkRecord],
    retry_policy: RetryPolicy,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut pending: Vec<usize> = (0..rows.len()).collect();
    let mut last_error = String::new();

    for attempt in 0..retry_policy.max_attempts {
        let mut retry_rows = Vec::new();
        for chunk in pending.chunks(KAFKA_NON_TX_IN_FLIGHT_LIMIT) {
            let mut deliveries = Vec::with_capacity(chunk.len());
            for row_idx in chunk {
                let row = &rows[*row_idx];
                let record = kafka_record(topic, row);
                match producer.send_result(record) {
                    Ok(delivery_future) => deliveries.push((*row_idx, delivery_future)),
                    Err((err, _message)) => {
                        last_error = err.to_string();
                        retry_rows.push(*row_idx);
                    }
                }
            }

            for (row_idx, delivery) in join_all(
                deliveries
                    .into_iter()
                    .map(|(row_idx, delivery)| async move { (row_idx, delivery.await) }),
            )
            .await
            {
                match delivery {
                    Ok(Ok(_)) => {}
                    Ok(Err((err, _message))) => {
                        last_error = err.to_string();
                        retry_rows.push(row_idx);
                    }
                    Err(err) => {
                        last_error = err.to_string();
                        retry_rows.push(row_idx);
                    }
                }
            }
        }
        if retry_rows.is_empty() {
            return Ok(());
        }
        if attempt + 1 == retry_policy.max_attempts {
            metrics::inc_sink_failure(sink_name, "kafka");
            return Err(anyhow!(
                "kafka sink delivery failed after retries (pending_rows={}): {}",
                retry_rows.len(),
                last_error
            ));
        }
        metrics::inc_sink_retry(sink_name, "kafka");
        pending = retry_rows;
        tokio::time::sleep(retry_policy.backoff_for_failure(attempt)).await;
    }
    unreachable!("retry loop should return or fail");
}

fn kafka_record<'a>(topic: &'a str, row: &'a SinkRecord) -> FutureRecord<'a, str, str> {
    let record = FutureRecord::<str, str>::to(topic).payload(row.payload.as_str());
    if let Some(key) = row.key.as_deref() {
        record.key(key)
    } else {
        record
    }
}
