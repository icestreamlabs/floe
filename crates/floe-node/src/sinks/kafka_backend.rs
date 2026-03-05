use super::*;

#[derive(Clone)]
pub(super) struct KafkaEosConfig {
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

pub(super) async fn run_kafka_sink(
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

pub(super) async fn run_kafka_worker(
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

pub(super) async fn flush_kafka_buffer(
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
pub(super) async fn send_kafka_transactional_batch_with_retry(
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

pub(super) async fn send_kafka_with_retry(
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
