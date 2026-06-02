use std::collections::HashSet;
use std::ffi::{CString, c_void};
use std::ptr;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use floe_cdc_core::{CdcColumn, CdcTableSchema};
use floe_config::ReplicationKafkaProducerConfig;
use floe_core::catalog::ColumnType;
use floe_storage::CdcBufferRecord;
use rdkafka::ClientConfig;
use rdkafka::bindings as rdsys;
use rdkafka::client::ClientContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Message, OwnedHeaders};
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use rdkafka::types::RDKafkaTopic;
use tokio_postgres::{Statement, types::ToSql};

use super::super::ReplicationPipelineRuntimeBufferMode;
use super::target_state::TargetStateBuilder;
use super::{
    FLOE_JSON_DELETED_FIELD, REPLICATION_KAFKA_MESSAGE_TIMEOUT_MS,
    REPLICATION_KAFKA_METADATA_WARMUP_TIMEOUT, REPLICATION_KAFKA_RETRY_ATTEMPTS,
    REPLICATION_KAFKA_RETRY_BASE_MS, REPLICATION_KAFKA_SEND_TIMEOUT, encoding,
    log_replication_kafka_send_perf,
};

pub(super) struct KafkaReplicationPipelineWriter {
    producer: ThreadedProducer<KafkaReplicationPipelineContext>,
    native_topic: KafkaNativeTopic,
    topic: String,
    partition_offsets: usize,
    perf_log: bool,
}

struct KafkaNativeTopic {
    ptr: *mut RDKafkaTopic,
}

unsafe impl Send for KafkaNativeTopic {}
unsafe impl Sync for KafkaNativeTopic {}

impl KafkaNativeTopic {
    fn new(
        producer: &ThreadedProducer<KafkaReplicationPipelineContext>,
        topic: &str,
    ) -> anyhow::Result<Self> {
        let topic_cstring =
            CString::new(topic).context("replication pipeline Kafka topic contains null byte")?;
        let ptr = unsafe {
            rdsys::rd_kafka_topic_new(
                producer.client().native_ptr(),
                topic_cstring.as_ptr(),
                ptr::null_mut(),
            )
        };
        if ptr.is_null() {
            return Err(anyhow!(
                "create replication pipeline Kafka native topic handle for {topic}"
            ));
        }
        Ok(Self { ptr })
    }
}

impl Drop for KafkaNativeTopic {
    fn drop(&mut self) {
        unsafe {
            rdsys::rd_kafka_topic_destroy(self.ptr);
        }
    }
}

#[path = "writers/postgres.rs"]
mod postgres;

pub(super) use self::postgres::*;
struct KafkaReplicationPipelineContext;

impl ClientContext for KafkaReplicationPipelineContext {}

impl ProducerContext for KafkaReplicationPipelineContext {
    type DeliveryOpaque = Arc<KafkaDeliveryBatchState>;

    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        delivery_state: Arc<KafkaDeliveryBatchState>,
    ) {
        delivery_state.record(delivery_result);
    }
}

struct KafkaDeliveryBatchState {
    expected: usize,
    completed: AtomicUsize,
    failed: AtomicUsize,
    partition_offsets: Vec<AtomicI64>,
    overflow_offsets_by_partition: Mutex<std::collections::BTreeMap<i32, i64>>,
    first_error: Mutex<Option<String>>,
    notify: tokio::sync::Notify,
}

impl KafkaDeliveryBatchState {
    fn new(expected: usize, partition_offsets: usize) -> Arc<Self> {
        Arc::new(Self {
            expected,
            completed: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
            partition_offsets: (0..partition_offsets).map(|_| AtomicI64::new(-1)).collect(),
            overflow_offsets_by_partition: Mutex::new(std::collections::BTreeMap::new()),
            first_error: Mutex::new(None),
            notify: tokio::sync::Notify::new(),
        })
    }

    fn record(&self, delivery_result: &DeliveryResult<'_>) {
        match delivery_result {
            Ok(message) => {
                let partition = message.partition();
                if let Ok(partition_idx) = usize::try_from(partition)
                    && let Some(offset) = self.partition_offsets.get(partition_idx)
                {
                    offset.fetch_max(message.offset(), Ordering::AcqRel);
                } else if let Ok(mut offsets_by_partition) =
                    self.overflow_offsets_by_partition.lock()
                {
                    offsets_by_partition
                        .entry(partition)
                        .and_modify(|current| *current = (*current).max(message.offset()))
                        .or_insert(message.offset());
                }
            }
            Err((err, _message)) => {
                self.failed.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut first_error) = self.first_error.lock()
                    && first_error.is_none()
                {
                    *first_error = Some(err.to_string());
                }
            }
        }
        let completed = self.completed.fetch_add(1, Ordering::AcqRel) + 1;
        if completed >= self.expected {
            self.notify.notify_waiters();
        }
    }

    async fn wait(
        &self,
        timeout: Duration,
    ) -> anyhow::Result<std::collections::BTreeMap<i32, i64>> {
        let started_at = Instant::now();
        while self.completed.load(Ordering::Acquire) < self.expected {
            let elapsed = started_at.elapsed();
            if elapsed >= timeout {
                return Err(anyhow!(
                    "replication pipeline Kafka send timed out after {} delivered of {} records",
                    self.completed.load(Ordering::Acquire),
                    self.expected
                ));
            }
            tokio::time::timeout(timeout - elapsed, self.notify.notified())
                .await
                .map_err(|_| {
                    anyhow!(
                        "replication pipeline Kafka send timed out after {} delivered of {} records",
                        self.completed.load(Ordering::Acquire),
                        self.expected
                    )
                })?;
        }
        if self.failed.load(Ordering::Acquire) > 0 {
            let first_error = self
                .first_error
                .lock()
                .ok()
                .and_then(|first_error| first_error.clone())
                .unwrap_or_else(|| "unknown Kafka delivery error".to_string());
            return Err(anyhow!(
                "replication pipeline Kafka send failed for {} of {} records: {first_error}",
                self.failed.load(Ordering::Acquire),
                self.expected
            ));
        }
        let mut offsets_by_partition = std::collections::BTreeMap::new();
        for (partition, offset) in self.partition_offsets.iter().enumerate() {
            let offset = offset.load(Ordering::Acquire);
            if offset >= 0 {
                offsets_by_partition.insert(partition as i32, offset);
            }
        }
        let overflow_offsets = self
            .overflow_offsets_by_partition
            .lock()
            .map_err(|_| anyhow!("replication pipeline Kafka delivery offsets lock poisoned"))?;
        for (partition, offset) in overflow_offsets.iter() {
            offsets_by_partition
                .entry(*partition)
                .and_modify(|current| *current = (*current).max(*offset))
                .or_insert(*offset);
        }
        Ok(offsets_by_partition)
    }
}

impl KafkaReplicationPipelineWriter {
    pub(super) fn new(
        brokers: &str,
        topic: &str,
        _buffer_mode: ReplicationPipelineRuntimeBufferMode,
        settings: ReplicationKafkaProducerConfig,
        perf_log: bool,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !brokers.trim().is_empty(),
            "replication Kafka brokers cannot be empty"
        );
        anyhow::ensure!(
            !topic.trim().is_empty(),
            "replication Kafka topic cannot be empty"
        );
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", REPLICATION_KAFKA_MESSAGE_TIMEOUT_MS)
            .set("message.max.bytes", settings.message_max_bytes.to_string())
            .set("acks", settings.acks.as_str())
            .set(
                "enable.idempotence",
                settings.enable_idempotence.to_string(),
            )
            .set("compression.type", "none")
            .set("linger.ms", settings.linger_ms.to_string())
            .set("batch.size", settings.batch_size.to_string())
            .set(
                "batch.num.messages",
                settings.batch_num_messages.to_string(),
            )
            .set(
                "queue.buffering.max.messages",
                settings.queue_max_messages.to_string(),
            )
            .set(
                "queue.buffering.max.kbytes",
                settings.queue_max_kbytes.to_string(),
            )
            .set(
                "message.send.max.retries",
                settings.message_send_max_retries.to_string(),
            );
        let producer: ThreadedProducer<KafkaReplicationPipelineContext> = config
            .create_with_context(KafkaReplicationPipelineContext)
            .context("create replication pipeline Kafka producer")?;
        let native_topic = KafkaNativeTopic::new(&producer, topic)?;
        let partition_offsets = match producer
            .client()
            .fetch_metadata(Some(topic), REPLICATION_KAFKA_METADATA_WARMUP_TIMEOUT)
        {
            Ok(metadata) => metadata
                .topics()
                .iter()
                .find(|metadata_topic| metadata_topic.name() == topic)
                .map(|metadata_topic| metadata_topic.partitions().len())
                .unwrap_or(0),
            Err(err) => {
                tracing::debug!(
                    topic,
                    error = %err,
                    "replication pipeline Kafka metadata warm-up failed; first send will retry metadata"
                );
                0
            }
        };
        Ok(Self {
            producer,
            native_topic,
            topic: topic.to_string(),
            partition_offsets,
            perf_log,
        })
    }

    pub(super) async fn send_records(
        &self,
        records: &[CdcBufferRecord],
    ) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
        let perf_enabled = self.perf_log;
        let perf_started_at = perf_enabled.then(Instant::now);
        let enqueue_started_at = perf_enabled.then(Instant::now);
        let delivery_state = KafkaDeliveryBatchState::new(records.len(), self.partition_offsets);
        for record in records {
            self.enqueue_record_with_retry(record, Arc::clone(&delivery_state))
                .await?;
        }
        let enqueue_elapsed = enqueue_started_at
            .map(|started_at| started_at.elapsed())
            .unwrap_or(Duration::ZERO);
        let delivery_wait_started_at = perf_enabled.then(Instant::now);
        let offsets_by_partition = match delivery_state.wait(REPLICATION_KAFKA_SEND_TIMEOUT).await {
            Ok(offsets) => offsets,
            Err(err) => {
                tracing::warn!(
                    topic = %self.topic,
                    records = records.len(),
                    timeout_ms = REPLICATION_KAFKA_SEND_TIMEOUT.as_millis() as u64,
                    error = %err,
                    "replication pipeline Kafka delivery wait failed"
                );
                return Err(err);
            }
        };
        let delivery_wait_elapsed = delivery_wait_started_at
            .map(|started_at| started_at.elapsed())
            .unwrap_or(Duration::ZERO);
        log_replication_kafka_send_perf(
            perf_enabled,
            &self.topic,
            records,
            self.partition_offsets,
            enqueue_elapsed,
            delivery_wait_elapsed,
            perf_started_at
                .map(|started_at| started_at.elapsed())
                .unwrap_or(Duration::ZERO),
        );

        let mut target_state = TargetStateBuilder::new();
        target_state.target_topic(&self.topic);
        for (partition, offset) in offsets_by_partition {
            target_state.target_partition_offset(partition, offset);
        }
        Ok(target_state.build())
    }

    async fn enqueue_record_with_retry(
        &self,
        record: &CdcBufferRecord,
        delivery_state: Arc<KafkaDeliveryBatchState>,
    ) -> anyhow::Result<()> {
        if record.headers().is_empty() {
            return self
                .enqueue_record_direct_with_retry(record, delivery_state)
                .await;
        }

        for attempt in 0..REPLICATION_KAFKA_RETRY_ATTEMPTS {
            let attempt_number = attempt + 1;
            let mut kafka_record =
                BaseRecord::<[u8], [u8], Arc<KafkaDeliveryBatchState>>::with_opaque_to(
                    &self.topic,
                    Arc::clone(&delivery_state),
                );
            if let Some(key) = record.key() {
                kafka_record = kafka_record.key(key);
            }
            if let Some(value) = record.value() {
                kafka_record = kafka_record.payload(value);
            }
            if !record.headers().is_empty() {
                let headers =
                    record
                        .headers()
                        .iter()
                        .fold(OwnedHeaders::new(), |headers, header| {
                            headers.insert(Header {
                                key: header.key(),
                                value: Some(header.value()),
                            })
                        });
                kafka_record = kafka_record.headers(headers);
            }

            match self.producer.send(kafka_record) {
                Ok(()) => return Ok(()),
                Err((err, _record))
                    if is_kafka_queue_full(&err)
                        && attempt_number < REPLICATION_KAFKA_RETRY_ATTEMPTS =>
                {
                    let delay_ms = REPLICATION_KAFKA_RETRY_BASE_MS.saturating_mul(
                        1_u64 << u32::try_from(attempt).unwrap_or(u32::MAX).min(16),
                    );
                    tracing::warn!(
                        topic = %self.topic,
                        attempt = attempt_number,
                        max_attempts = REPLICATION_KAFKA_RETRY_ATTEMPTS,
                        retry_delay_ms = delay_ms,
                        record_bytes = record.byte_len(),
                        key_bytes = record.key().map(|key| key.len()),
                        value_bytes = record.value().map(|value| value.len()),
                        error = %err,
                        "replication pipeline Kafka producer queue is full; retrying"
                    );
                    self.producer.poll(Duration::from_millis(0));
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                Err((err, _record)) if is_kafka_queue_full(&err) => {
                    tracing::warn!(
                        topic = %self.topic,
                        attempt = attempt_number,
                        max_attempts = REPLICATION_KAFKA_RETRY_ATTEMPTS,
                        record_bytes = record.byte_len(),
                        key_bytes = record.key().map(|key| key.len()),
                        value_bytes = record.value().map(|value| value.len()),
                        error = %err,
                        "replication pipeline Kafka producer queue remained full after retries"
                    );
                    return Err(anyhow!(
                        "replication pipeline Kafka enqueue failed after retries: {err}"
                    ));
                }
                Err((err, _record)) => {
                    tracing::warn!(
                        topic = %self.topic,
                        attempt = attempt_number,
                        max_attempts = REPLICATION_KAFKA_RETRY_ATTEMPTS,
                        record_bytes = record.byte_len(),
                        key_bytes = record.key().map(|key| key.len()),
                        value_bytes = record.value().map(|value| value.len()),
                        error = %err,
                        "replication pipeline Kafka enqueue failed without retry"
                    );
                    return Err(anyhow!(
                        "replication pipeline Kafka enqueue failed after retries: {err}"
                    ));
                }
            }
        }
        Err(anyhow!(
            "replication pipeline Kafka enqueue failed after retries"
        ))
    }

    async fn enqueue_record_direct_with_retry(
        &self,
        record: &CdcBufferRecord,
        delivery_state: Arc<KafkaDeliveryBatchState>,
    ) -> anyhow::Result<()> {
        for attempt in 0..REPLICATION_KAFKA_RETRY_ATTEMPTS {
            let attempt_number = attempt + 1;
            match self.enqueue_record_direct(record, Arc::clone(&delivery_state)) {
                Ok(()) => return Ok(()),
                Err(err)
                    if is_kafka_queue_full(&err)
                        && attempt_number < REPLICATION_KAFKA_RETRY_ATTEMPTS =>
                {
                    let delay_ms = REPLICATION_KAFKA_RETRY_BASE_MS.saturating_mul(
                        1_u64 << u32::try_from(attempt).unwrap_or(u32::MAX).min(16),
                    );
                    tracing::warn!(
                        topic = %self.topic,
                        attempt = attempt_number,
                        max_attempts = REPLICATION_KAFKA_RETRY_ATTEMPTS,
                        retry_delay_ms = delay_ms,
                        record_bytes = record.byte_len(),
                        key_bytes = record.key().map(|key| key.len()),
                        value_bytes = record.value().map(|value| value.len()),
                        error = %err,
                        "replication pipeline Kafka producer queue is full; retrying"
                    );
                    self.producer.poll(Duration::from_millis(0));
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                Err(err) if is_kafka_queue_full(&err) => {
                    tracing::warn!(
                        topic = %self.topic,
                        attempt = attempt_number,
                        max_attempts = REPLICATION_KAFKA_RETRY_ATTEMPTS,
                        record_bytes = record.byte_len(),
                        key_bytes = record.key().map(|key| key.len()),
                        value_bytes = record.value().map(|value| value.len()),
                        error = %err,
                        "replication pipeline Kafka producer queue remained full after retries"
                    );
                    return Err(anyhow!(
                        "replication pipeline Kafka enqueue failed after retries: {err}"
                    ));
                }
                Err(err) => {
                    tracing::warn!(
                        topic = %self.topic,
                        attempt = attempt_number,
                        max_attempts = REPLICATION_KAFKA_RETRY_ATTEMPTS,
                        record_bytes = record.byte_len(),
                        key_bytes = record.key().map(|key| key.len()),
                        value_bytes = record.value().map(|value| value.len()),
                        error = %err,
                        "replication pipeline Kafka enqueue failed without retry"
                    );
                    return Err(anyhow!(
                        "replication pipeline Kafka enqueue failed after retries: {err}"
                    ));
                }
            }
        }
        Err(anyhow!(
            "replication pipeline Kafka enqueue failed after retries"
        ))
    }

    fn enqueue_record_direct(
        &self,
        record: &CdcBufferRecord,
        delivery_state: Arc<KafkaDeliveryBatchState>,
    ) -> Result<(), KafkaError> {
        let payload = record.value();
        let payload_ptr = payload.map_or(ptr::null_mut(), |payload| {
            payload.as_ptr().cast::<c_void>().cast_mut()
        });
        let payload_len = payload.map_or(0, <[u8]>::len);
        let key = record.key();
        let key_ptr = key.map_or(ptr::null(), |key| key.as_ptr().cast::<c_void>());
        let key_len = key.map_or(0, <[u8]>::len);
        let opaque_ptr = Arc::into_raw(delivery_state).cast::<c_void>().cast_mut();
        let produce_result = unsafe {
            rdsys::rd_kafka_produce(
                self.native_topic.ptr,
                -1,
                rdsys::RD_KAFKA_MSG_F_COPY,
                payload_ptr,
                payload_len,
                key_ptr,
                key_len,
                opaque_ptr,
            )
        };
        if produce_result == 0 {
            Ok(())
        } else {
            unsafe {
                drop(Arc::from_raw(
                    opaque_ptr.cast::<KafkaDeliveryBatchState>().cast_const(),
                ));
            }
            Err(KafkaError::MessageProduction(RDKafkaErrorCode::from(
                unsafe { rdsys::rd_kafka_last_error() },
            )))
        }
    }
}

fn is_kafka_queue_full(err: &KafkaError) -> bool {
    matches!(
        err,
        KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
    )
}
