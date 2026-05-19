use std::collections::HashSet;
use std::ffi::{CString, c_void};
use std::ptr;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use floe_cdc_core::{CdcColumn, CdcTableSchema};
use floe_core::catalog::ColumnType;
use floe_storage::CdcBufferRecord;
use rdkafka::ClientConfig;
use rdkafka::bindings as rdsys;
use rdkafka::client::ClientContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Message, OwnedHeaders};
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use rdkafka::types::RDKafkaTopic;
use tokio_postgres::types::ToSql;

use super::super::ReplicationPipelineRuntimeBufferMode;
use super::{
    CDC_PERF_LOGGING_ENABLED, FLOE_JSON_DELETED_FIELD, REPLICATION_KAFKA_ACKS,
    REPLICATION_KAFKA_BATCH_NUM_MESSAGES, REPLICATION_KAFKA_BATCH_SIZE,
    REPLICATION_KAFKA_ENABLE_IDEMPOTENCE, REPLICATION_KAFKA_LINGER_MS,
    REPLICATION_KAFKA_MESSAGE_MAX_BYTES, REPLICATION_KAFKA_MESSAGE_SEND_MAX_RETRIES,
    REPLICATION_KAFKA_MESSAGE_TIMEOUT_MS, REPLICATION_KAFKA_METADATA_WARMUP_TIMEOUT,
    REPLICATION_KAFKA_QUEUE_MAX_KBYTES, REPLICATION_KAFKA_QUEUE_MAX_MESSAGES,
    REPLICATION_KAFKA_RETRY_ATTEMPTS, REPLICATION_KAFKA_RETRY_BASE_MS,
    REPLICATION_KAFKA_SEND_TIMEOUT, encoding, log_replication_kafka_send_perf,
};

pub(super) struct KafkaReplicationPipelineWriter {
    producer: ThreadedProducer<KafkaReplicationPipelineContext>,
    native_topic: KafkaNativeTopic,
    topic: String,
    partition_offsets: usize,
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

pub(super) struct PostgresReplicationPipelineWriter {
    connection: String,
    target_table: String,
    pub(super) insert_sql: String,
    pub(super) delete_sql: String,
    schema: CdcTableSchema,
}

#[derive(Clone)]
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
            .set(
                "message.max.bytes",
                REPLICATION_KAFKA_MESSAGE_MAX_BYTES.as_str(),
            )
            .set("acks", REPLICATION_KAFKA_ACKS.as_str())
            .set(
                "enable.idempotence",
                REPLICATION_KAFKA_ENABLE_IDEMPOTENCE.as_str(),
            )
            .set("compression.type", "none")
            .set("linger.ms", REPLICATION_KAFKA_LINGER_MS.as_str())
            .set("batch.size", REPLICATION_KAFKA_BATCH_SIZE.as_str())
            .set(
                "batch.num.messages",
                REPLICATION_KAFKA_BATCH_NUM_MESSAGES.as_str(),
            )
            .set(
                "queue.buffering.max.messages",
                REPLICATION_KAFKA_QUEUE_MAX_MESSAGES.as_str(),
            )
            .set(
                "queue.buffering.max.kbytes",
                REPLICATION_KAFKA_QUEUE_MAX_KBYTES.as_str(),
            )
            .set(
                "message.send.max.retries",
                REPLICATION_KAFKA_MESSAGE_SEND_MAX_RETRIES.as_str(),
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
        })
    }

    pub(super) async fn send_records(
        &self,
        records: &[CdcBufferRecord],
    ) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
        let perf_enabled = *CDC_PERF_LOGGING_ENABLED;
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
            &self.topic,
            records,
            self.partition_offsets,
            enqueue_elapsed,
            delivery_wait_elapsed,
            perf_started_at
                .map(|started_at| started_at.elapsed())
                .unwrap_or(Duration::ZERO),
        );

        let mut target_state = std::collections::BTreeMap::new();
        target_state.insert("kafka.topic".to_string(), self.topic.clone());
        for (partition, offset) in offsets_by_partition {
            target_state.insert(
                format!("kafka.partition.{partition}.offset"),
                offset.to_string(),
            );
        }
        Ok(target_state)
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

#[derive(Debug, PartialEq)]
pub(super) enum PostgresParamValue {
    Int64(Option<i64>),
    Bool(Option<bool>),
    Text(Option<String>),
    Float64(Option<f64>),
    Int32(Option<i32>),
}

impl PostgresParamValue {
    fn null(data_type: &ColumnType) -> Self {
        match data_type {
            ColumnType::Int64 => Self::Int64(None),
            ColumnType::Bool => Self::Bool(None),
            ColumnType::Utf8 => Self::Text(None),
            ColumnType::TimestampMillis => Self::Float64(None),
            ColumnType::DateDays => Self::Int32(None),
            ColumnType::Decimal128 { .. } | ColumnType::Numeric => Self::Text(None),
        }
    }

    fn as_tosql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Int64(value) => value,
            Self::Bool(value) => value,
            Self::Text(value) => value,
            Self::Float64(value) => value,
            Self::Int32(value) => value,
        }
    }
}

impl PostgresReplicationPipelineWriter {
    pub(super) fn new(
        connection: &str,
        table: &str,
        schema: CdcTableSchema,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !connection.trim().is_empty(),
            "replication Postgres connection cannot be empty"
        );
        let target_table = quote_postgres_qualified_name(table)?;
        encoding::validate_floe_json_schema(&schema)?;
        Ok(Self {
            connection: connection.to_string(),
            target_table,
            insert_sql: postgres_upsert_sql(&schema, table)?,
            delete_sql: postgres_delete_sql(&schema, table)?,
            schema,
        })
    }

    pub(super) async fn send_records(
        &self,
        records: &[CdcBufferRecord],
    ) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
        if records.is_empty() {
            return Ok(self.target_state(0));
        }

        let (mut client, connection) =
            tokio_postgres::connect(self.connection.as_str(), tokio_postgres::NoTls)
                .await
                .context("connect replication pipeline Postgres target")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(
                    error = %err,
                    "replication pipeline Postgres target connection failed"
                );
            }
        });

        let transaction = client
            .transaction()
            .await
            .context("start replication pipeline Postgres target transaction")?;
        for record in records {
            self.apply_record(&transaction, record).await?;
        }
        transaction
            .commit()
            .await
            .context("commit replication pipeline Postgres target transaction")?;
        Ok(self.target_state(records.len()))
    }

    async fn apply_record(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        record: &CdcBufferRecord,
    ) -> anyhow::Result<()> {
        let value = parse_floe_json_record_value(record)?;
        let deleted = value
            .get(FLOE_JSON_DELETED_FIELD)
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if deleted {
            let key = parse_floe_json_record_key(record).unwrap_or_else(|_| value.clone());
            let params = postgres_key_params_from_json(&self.schema, &key)?;
            let refs = params
                .iter()
                .map(PostgresParamValue::as_tosql)
                .collect::<Vec<_>>();
            transaction
                .execute(&self.delete_sql, &refs)
                .await
                .with_context(|| {
                    format!(
                        "delete CDC row from replication pipeline Postgres target {}",
                        self.target_table
                    )
                })?;
            return Ok(());
        }

        let params = postgres_row_params_from_json(&self.schema, &value)?;
        let refs = params
            .iter()
            .map(PostgresParamValue::as_tosql)
            .collect::<Vec<_>>();
        transaction
            .execute(&self.insert_sql, &refs)
            .await
            .with_context(|| {
                format!(
                    "upsert CDC row into replication pipeline Postgres target {}",
                    self.target_table
                )
            })?;
        Ok(())
    }

    fn target_state(&self, records: usize) -> std::collections::BTreeMap<String, String> {
        std::collections::BTreeMap::from([
            ("postgres.table".to_string(), self.target_table.clone()),
            ("postgres.records_applied".to_string(), records.to_string()),
        ])
    }
}

fn is_kafka_queue_full(err: &KafkaError) -> bool {
    matches!(
        err,
        KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
    )
}

pub(super) fn parse_floe_json_record_value(
    record: &CdcBufferRecord,
) -> anyhow::Result<serde_json::Map<String, serde_json::Value>> {
    let value = record
        .value()
        .ok_or_else(|| anyhow!("Floe JSON Postgres target record is missing a value"))?;
    let value = serde_json::from_slice::<serde_json::Value>(value)
        .context("parse Floe JSON Postgres target record value")?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow!("Floe JSON Postgres target record value must be an object"))
}

pub(super) fn parse_floe_json_record_key(
    record: &CdcBufferRecord,
) -> anyhow::Result<serde_json::Map<String, serde_json::Value>> {
    let key = record
        .key()
        .ok_or_else(|| anyhow!("Floe JSON Postgres target record is missing a key"))?;
    let key = serde_json::from_slice::<serde_json::Value>(key)
        .context("parse Floe JSON Postgres target record key")?;
    key.as_object()
        .cloned()
        .ok_or_else(|| anyhow!("Floe JSON Postgres target record key must be an object"))
}

pub(super) fn postgres_row_params_from_json(
    schema: &CdcTableSchema,
    object: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<Vec<PostgresParamValue>> {
    schema
        .columns()
        .iter()
        .map(|column| postgres_param_from_json(column, object.get(column.name())))
        .collect()
}

pub(super) fn postgres_key_params_from_json(
    schema: &CdcTableSchema,
    object: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<Vec<PostgresParamValue>> {
    schema
        .primary_key()
        .columns()
        .iter()
        .map(|column_name| {
            let column = schema
                .columns()
                .iter()
                .find(|column| column.name() == column_name)
                .ok_or_else(|| {
                    anyhow!("CDC primary-key column '{column_name}' missing from schema")
                })?;
            postgres_param_from_json(column, object.get(column.name()))
        })
        .collect()
}

fn postgres_param_from_json(
    column: &CdcColumn,
    value: Option<&serde_json::Value>,
) -> anyhow::Result<PostgresParamValue> {
    let Some(value) = value else {
        anyhow::ensure!(
            column.nullable(),
            "CDC column '{}' is required for Postgres target",
            column.name()
        );
        return Ok(PostgresParamValue::null(column.data_type()));
    };
    if value.is_null() {
        anyhow::ensure!(
            column.nullable(),
            "CDC column '{}' cannot be NULL for Postgres target",
            column.name()
        );
        return Ok(PostgresParamValue::null(column.data_type()));
    }
    match column.data_type() {
        ColumnType::Int64 => Ok(PostgresParamValue::Int64(Some(json_i64(
            column.name(),
            value,
        )?))),
        ColumnType::Bool => Ok(PostgresParamValue::Bool(Some(json_bool(
            column.name(),
            value,
        )?))),
        ColumnType::Utf8 => Ok(PostgresParamValue::Text(Some(json_string(
            column.name(),
            value,
        )?))),
        ColumnType::TimestampMillis => Ok(PostgresParamValue::Float64(Some(json_i64(
            column.name(),
            value,
        )? as f64))),
        ColumnType::DateDays => Ok(PostgresParamValue::Int32(Some(json_i32(
            column.name(),
            value,
        )?))),
        ColumnType::Decimal128 { scale, .. } => {
            let text = if let Some(value) = value.as_str() {
                value.to_string()
            } else {
                encoding::format_decimal128_for_json(
                    i128::from(json_i64(column.name(), value)?),
                    *scale,
                )
            };
            Ok(PostgresParamValue::Text(Some(text)))
        }
        ColumnType::Numeric => Ok(PostgresParamValue::Text(Some(json_scalar_string(
            column.name(),
            value,
        )?))),
    }
}

fn json_i64(column: &str, value: &serde_json::Value) -> anyhow::Result<i64> {
    if let Some(value) = value.as_i64() {
        return Ok(value);
    }
    if let Some(value) = value.as_str() {
        return value
            .parse::<i64>()
            .with_context(|| format!("parse CDC column '{column}' as i64"));
    }
    Err(anyhow!("CDC column '{column}' must be an integer"))
}

fn json_i32(column: &str, value: &serde_json::Value) -> anyhow::Result<i32> {
    let value = json_i64(column, value)?;
    i32::try_from(value).with_context(|| format!("CDC column '{column}' exceeds i32 range"))
}

fn json_bool(column: &str, value: &serde_json::Value) -> anyhow::Result<bool> {
    if let Some(value) = value.as_bool() {
        return Ok(value);
    }
    if let Some(value) = value.as_str() {
        return value
            .parse::<bool>()
            .with_context(|| format!("parse CDC column '{column}' as bool"));
    }
    Err(anyhow!("CDC column '{column}' must be a boolean"))
}

fn json_string(column: &str, value: &serde_json::Value) -> anyhow::Result<String> {
    value
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("CDC column '{column}' must be a string"))
}

fn json_scalar_string(column: &str, value: &serde_json::Value) -> anyhow::Result<String> {
    match value {
        serde_json::Value::String(value) => Ok(value.clone()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        _ => Err(anyhow!("CDC column '{column}' must be a scalar value")),
    }
}

fn postgres_upsert_sql(schema: &CdcTableSchema, table: &str) -> anyhow::Result<String> {
    let table = quote_postgres_qualified_name(table)?;
    let columns = schema
        .columns()
        .iter()
        .map(|column| quote_postgres_ident(column.name()))
        .collect::<Vec<_>>();
    let values = schema
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, column)| postgres_value_expr(idx + 1, column.data_type()))
        .collect::<Vec<_>>();
    let primary_keys = schema
        .primary_key()
        .columns()
        .iter()
        .map(|column| quote_postgres_ident(column))
        .collect::<Vec<_>>();
    let primary_key_names = schema
        .primary_key()
        .columns()
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let updates = schema
        .columns()
        .iter()
        .filter(|column| !primary_key_names.contains(column.name()))
        .map(|column| {
            let quoted = quote_postgres_ident(column.name());
            format!("{quoted} = EXCLUDED.{quoted}")
        })
        .collect::<Vec<_>>();
    let conflict_action = if updates.is_empty() {
        "DO NOTHING".to_string()
    } else {
        format!("DO UPDATE SET {}", updates.join(", "))
    };
    Ok(format!(
        "INSERT INTO {table} ({}) VALUES ({}) ON CONFLICT ({}) {conflict_action}",
        columns.join(", "),
        values.join(", "),
        primary_keys.join(", ")
    ))
}

fn postgres_delete_sql(schema: &CdcTableSchema, table: &str) -> anyhow::Result<String> {
    let table = quote_postgres_qualified_name(table)?;
    let predicates = schema
        .primary_key()
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, column_name)| {
            let column = schema
                .columns()
                .iter()
                .find(|column| column.name() == column_name)
                .ok_or_else(|| {
                    anyhow!("CDC primary-key column '{column_name}' missing from schema")
                })?;
            Ok(format!(
                "{} = {}",
                quote_postgres_ident(column.name()),
                postgres_value_expr(idx + 1, column.data_type())
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(format!(
        "DELETE FROM {table} WHERE {}",
        predicates.join(" AND ")
    ))
}

fn postgres_value_expr(param_idx: usize, data_type: &ColumnType) -> String {
    match data_type {
        ColumnType::TimestampMillis => {
            format!("to_timestamp(${param_idx}::double precision / 1000.0)")
        }
        ColumnType::DateDays => format!("DATE '1970-01-01' + ${param_idx}::integer"),
        ColumnType::Decimal128 { .. } | ColumnType::Numeric => {
            format!("${param_idx}::numeric")
        }
        ColumnType::Int64 | ColumnType::Bool | ColumnType::Utf8 => format!("${param_idx}"),
    }
}

fn quote_postgres_qualified_name(name: &str) -> anyhow::Result<String> {
    let parts = name
        .split('.')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(quote_postgres_ident)
        .collect::<Vec<_>>();
    anyhow::ensure!(!parts.is_empty(), "Postgres target table cannot be empty");
    Ok(parts.join("."))
}

fn quote_postgres_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
