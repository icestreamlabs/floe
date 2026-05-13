use super::*;

use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::builder::{
    BooleanBuilder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use floe_cdc_core::{CdcColumnarColumn, CdcColumnarRowBatch, CdcRow, CdcRowKey, CdcSourcePosition};
use floe_core::RowValue;
use floe_node_core::debezium_encoder::{
    DebeziumEncodeContext, DebeziumEncodedRecord, DebeziumEnvelopeConfig, encode_debezium_change,
    encode_debezium_snapshot_row,
};
use floe_storage::{
    CdcBufferAppend, CdcBufferCleanupPolicy, CdcBufferRecord, CdcBufferStore,
    CdcBufferedTransactionManifest, ReplicationPipelineCheckpoint, SlateCatalog,
};
use futures::future::join_all;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

const REPLICATION_KAFKA_RETRY_ATTEMPTS: usize = 5;
const REPLICATION_KAFKA_RETRY_BASE_MS: u64 = 50;
const REPLICATION_KAFKA_MESSAGE_TIMEOUT_MS: &str = "1000";
const REPLICATION_KAFKA_SEND_TIMEOUT: Duration = Duration::from_secs(2);
const REPLICATION_BUFFER_REPLAY_LIMIT: usize = 1024;
const REPLICATION_BUFFER_DELIVERED_RETENTION_MS: u64 = 0;
const DEFAULT_REPLICATION_ARROW_IPC_ROWS_PER_RECORD: usize = 8192;
static REPLICATION_ARROW_IPC_ROWS_PER_RECORD: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("FLOE_REPLICATION_ARROW_IPC_ROWS_PER_RECORD")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_REPLICATION_ARROW_IPC_ROWS_PER_RECORD)
});

pub(super) struct ReplicationPipelineRuntime {
    pipelines_by_source: HashMap<CdcSourceId, Vec<ReplicationPipelineRuntimePlan>>,
    kafka_writers_by_pipeline: HashMap<String, Arc<KafkaReplicationPipelineWriter>>,
}

struct KafkaReplicationPipelineWriter {
    producer: FutureProducer,
    topic: String,
}

impl ReplicationPipelineRuntime {
    pub(super) fn new(
        plans: impl IntoIterator<Item = ReplicationPipelineRuntimePlan>,
    ) -> anyhow::Result<Self> {
        let mut pipelines_by_source: HashMap<CdcSourceId, Vec<ReplicationPipelineRuntimePlan>> =
            HashMap::new();
        let mut kafka_writers_by_pipeline = HashMap::new();

        for plan in plans {
            match &plan.target {
                ReplicationPipelineRuntimeTarget::Kafka { brokers, topic } => {
                    kafka_writers_by_pipeline.insert(
                        plan.name.clone(),
                        Arc::new(KafkaReplicationPipelineWriter::new(brokers, topic)?),
                    );
                }
            }
            pipelines_by_source
                .entry(CdcSourceId::new(plan.source_name.clone())?)
                .or_default()
                .push(plan);
        }

        Ok(Self {
            pipelines_by_source,
            kafka_writers_by_pipeline,
        })
    }

    pub(super) fn has_pipelines_for_source(&self, source_id: &CdcSourceId) -> bool {
        self.pipelines_by_source
            .get(source_id)
            .is_some_and(|plans| !plans.is_empty())
    }

    pub(super) async fn replay_buffered(&self, storage: &SlateCatalog) -> anyhow::Result<usize> {
        let buffer_store = CdcBufferStore::new(storage.db());
        let mut delivered = 0usize;
        for plans in self.pipelines_by_source.values() {
            for plan in plans {
                delivered = delivered.saturating_add(
                    self.replay_pending_for_plan(plan, &buffer_store, storage)
                        .await?,
                );
            }
        }
        Ok(delivered)
    }

    pub(super) async fn run_transaction(
        &self,
        source_id: &CdcSourceId,
        schemas: &HashMap<CdcTableId, CdcTableSchema>,
        transaction: &TransactionBatch,
        storage: Option<&SlateCatalog>,
    ) -> anyhow::Result<usize> {
        let Some(plans) = self.pipelines_by_source.get(source_id) else {
            return Ok(0);
        };

        let mut written = 0usize;
        for plan in plans {
            let buffered_records = encode_pipeline_transaction_records(plan, schemas, transaction)?;
            if buffered_records.is_empty() {
                continue;
            }
            if let Some(storage) = storage {
                let buffer_store = CdcBufferStore::new(storage.db());
                let manifest = buffer_store
                    .append_transaction(CdcBufferAppend::new(
                        &plan.name,
                        &plan.source_name,
                        plan.table_id.as_str(),
                        transaction.commit_position().clone(),
                        transaction.transaction_id().cloned(),
                        buffered_records,
                        current_unix_time_ms(),
                    )?)
                    .await
                    .with_context(|| {
                        format!(
                            "append replication pipeline '{}' transaction buffer",
                            plan.name
                        )
                    })?;
                storage
                    .put_replication_pipeline_checkpoint(ReplicationPipelineCheckpoint::new(
                        &plan.name,
                        &plan.source_name,
                        transaction.commit_position().clone(),
                        transaction.transaction_id().cloned(),
                        pending_target_state(plan, &manifest),
                        current_unix_time_ms(),
                    )?)
                    .await
                    .with_context(|| {
                        format!("persist replication pipeline '{}' checkpoint", plan.name)
                    })?;
                written = written.saturating_add(manifest.record_count());
                self.replay_pending_for_plan(plan, &buffer_store, storage)
                    .await?;
                record_buffer_stats(&buffer_store, &plan.name).await?;
            } else {
                match &plan.target {
                    ReplicationPipelineRuntimeTarget::Kafka { .. } => {
                        let writer =
                            self.kafka_writers_by_pipeline
                                .get(&plan.name)
                                .ok_or_else(|| {
                                    anyhow!(
                                        "replication pipeline '{}' has no Kafka writer",
                                        plan.name
                                    )
                                })?;
                        writer.send_records(&buffered_records).await?;
                    }
                }
                written = written.saturating_add(buffered_records.len());
            }
        }

        Ok(written)
    }

    async fn replay_pending_for_plan(
        &self,
        plan: &ReplicationPipelineRuntimePlan,
        buffer_store: &CdcBufferStore,
        storage: &SlateCatalog,
    ) -> anyhow::Result<usize> {
        let mut delivered_records = 0usize;
        let pending = buffer_store
            .pending_transactions(&plan.name, REPLICATION_BUFFER_REPLAY_LIMIT)
            .await
            .with_context(|| {
                format!(
                    "load pending replication pipeline '{}' buffer transactions",
                    plan.name
                )
            })?;
        for manifest in pending {
            let records = buffer_store.records(&manifest).await.with_context(|| {
                format!(
                    "load replication pipeline '{}' buffered payloads",
                    plan.name
                )
            })?;
            let target_state = match &plan.target {
                ReplicationPipelineRuntimeTarget::Kafka { .. } => {
                    let writer =
                        self.kafka_writers_by_pipeline
                            .get(&plan.name)
                            .ok_or_else(|| {
                                anyhow!("replication pipeline '{}' has no Kafka writer", plan.name)
                            })?;
                    match writer.send_records(&records).await {
                        Ok(mut target_state) => {
                            target_state
                                .insert("source.table".to_string(), plan.upstream_table.clone());
                            target_state
                        }
                        Err(err) => {
                            crate::metrics::inc_sink_failure(&plan.name, "kafka_replication");
                            tracing::warn!(
                                pipeline = %plan.name,
                                error = %err,
                                "replication pipeline target write failed; buffered transaction remains pending"
                            );
                            break;
                        }
                    }
                }
            };
            let delivered_at = current_unix_time_ms();
            buffer_store
                .mark_delivered(&manifest, delivered_at)
                .await
                .with_context(|| {
                    format!(
                        "mark replication pipeline '{}' buffered transaction delivered",
                        plan.name
                    )
                })?;
            storage
                .put_replication_pipeline_checkpoint(ReplicationPipelineCheckpoint::new(
                    &plan.name,
                    &plan.source_name,
                    buffer_store
                        .source_frontier(&plan.name)
                        .await?
                        .map(|frontier| frontier.source_position().clone())
                        .unwrap_or_else(|| manifest.source_position().clone()),
                    manifest.transaction_id().cloned(),
                    delivered_target_state(&manifest, target_state),
                    delivered_at,
                )?)
                .await
                .with_context(|| {
                    format!(
                        "persist replication pipeline '{}' delivery checkpoint",
                        plan.name
                    )
                })?;
            delivered_records = delivered_records.saturating_add(records.len());
            buffer_store
                .cleanup_delivered(
                    &plan.name,
                    CdcBufferCleanupPolicy::new(REPLICATION_BUFFER_DELIVERED_RETENTION_MS),
                    current_unix_time_ms(),
                )
                .await
                .with_context(|| {
                    format!(
                        "cleanup replication pipeline '{}' delivered buffer",
                        plan.name
                    )
                })?;
        }
        record_buffer_stats(buffer_store, &plan.name).await?;
        Ok(delivered_records)
    }
}

impl KafkaReplicationPipelineWriter {
    fn new(brokers: &str, topic: &str) -> anyhow::Result<Self> {
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
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", REPLICATION_KAFKA_MESSAGE_TIMEOUT_MS);
        let producer: FutureProducer = config
            .create()
            .context("create replication pipeline Kafka producer")?;
        Ok(Self {
            producer,
            topic: topic.to_string(),
        })
    }

    async fn send_records(
        &self,
        records: &[CdcBufferRecord],
    ) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
        let deliveries = join_all(
            records
                .iter()
                .map(|record| self.send_record_with_retry(record)),
        )
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;

        let mut target_state = std::collections::BTreeMap::new();
        target_state.insert("kafka.topic".to_string(), self.topic.clone());
        for (partition, offset) in deliveries {
            let key = format!("kafka.partition.{partition}.offset");
            let entry = target_state
                .entry(key)
                .or_insert_with(|| offset.to_string());
            if offset > entry.parse::<i64>().unwrap_or(i64::MIN) {
                *entry = offset.to_string();
            }
        }
        Ok(target_state)
    }

    async fn send_record_with_retry(&self, record: &CdcBufferRecord) -> anyhow::Result<(i32, i64)> {
        for attempt in 0..REPLICATION_KAFKA_RETRY_ATTEMPTS {
            let mut kafka_record = FutureRecord::<[u8], [u8]>::to(&self.topic);
            if let Some(key) = record.key() {
                kafka_record = kafka_record.key(key);
            }
            if let Some(value) = record.value() {
                kafka_record = kafka_record.payload(value);
            }

            let send_result = tokio::time::timeout(
                REPLICATION_KAFKA_SEND_TIMEOUT,
                self.producer.send(kafka_record, Duration::from_secs(0)),
            )
            .await;
            match send_result {
                Ok(Ok((partition, offset))) => return Ok((partition, offset)),
                Ok(Err((err, _message))) if attempt + 1 < REPLICATION_KAFKA_RETRY_ATTEMPTS => {
                    let delay_ms = REPLICATION_KAFKA_RETRY_BASE_MS.saturating_mul(
                        1_u64 << u32::try_from(attempt).unwrap_or(u32::MAX).min(16),
                    );
                    tracing::warn!(
                        topic = %self.topic,
                        attempt = attempt + 1,
                        error = %err,
                        "replication pipeline Kafka send failed; retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                Ok(Err((err, _message))) => {
                    return Err(anyhow!(
                        "replication pipeline Kafka send failed after retries: {err}"
                    ));
                }
                Err(_) if attempt + 1 < REPLICATION_KAFKA_RETRY_ATTEMPTS => {
                    tracing::warn!(
                        topic = %self.topic,
                        attempt = attempt + 1,
                        timeout_ms = REPLICATION_KAFKA_SEND_TIMEOUT.as_millis() as u64,
                        "replication pipeline Kafka send timed out; retrying"
                    );
                }
                Err(_) => {
                    return Err(anyhow!(
                        "replication pipeline Kafka send timed out after retries"
                    ));
                }
            }
        }
        unreachable!("Kafka retry loop should return");
    }
}

fn encode_pipeline_transaction_records(
    plan: &ReplicationPipelineRuntimePlan,
    schemas: &HashMap<CdcTableId, CdcTableSchema>,
    transaction: &TransactionBatch,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    let mut matching_batches = transaction
        .change_batches()
        .iter()
        .filter(|batch| batch.table_id() == &plan.table_id)
        .peekable();
    if matching_batches.peek().is_none() {
        return Ok(Vec::new());
    }

    let schema = schemas.get(&plan.table_id).ok_or_else(|| {
        anyhow!(
            "replication pipeline '{}' references missing CDC schema '{}'",
            plan.name,
            plan.table_id.as_str()
        )
    })?;
    let mut records = Vec::new();
    for change_batch in matching_batches {
        records.extend(encode_pipeline_buffer_records(
            plan,
            schema,
            change_batch,
            transaction,
        )?);
    }
    Ok(records)
}

fn encode_pipeline_buffer_records(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    batch: &ChangeBatch,
    transaction: &TransactionBatch,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    if let Some(rows) = batch.snapshot_insert_rows() {
        return match plan.format {
            ReplicationPipelineRuntimeFormat::DebeziumJson => {
                let records =
                    encode_debezium_snapshot_pipeline_records(plan, schema, rows, transaction)?;
                debezium_records_to_buffer_records(&records)
            }
            ReplicationPipelineRuntimeFormat::ArrowIpc => {
                encode_arrow_ipc_snapshot_pipeline_records(plan, schema, rows, transaction)
            }
        };
    }

    match plan.format {
        ReplicationPipelineRuntimeFormat::DebeziumJson => {
            let records = encode_debezium_pipeline_records(plan, schema, batch, transaction)?;
            debezium_records_to_buffer_records(&records)
        }
        ReplicationPipelineRuntimeFormat::ArrowIpc => {
            encode_arrow_ipc_pipeline_records(plan, schema, batch, transaction)
        }
    }
}

fn encode_debezium_pipeline_records(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    batch: &ChangeBatch,
    transaction: &TransactionBatch,
) -> anyhow::Result<Vec<DebeziumEncodedRecord>> {
    let config = DebeziumEnvelopeConfig::new(&plan.source_name)?
        .with_emit_tombstones(plan.emit_tombstones)
        .with_transaction_metadata(plan.include_transaction_metadata);
    let is_snapshot = transaction
        .transaction_id()
        .is_some_and(|tx| tx.as_str().starts_with("snapshot:"));
    let mut records = Vec::new();
    for (idx, change) in batch.changes().iter().enumerate() {
        let context = DebeziumEncodeContext {
            source_position: Some(transaction.commit_position()),
            transaction_id: transaction.transaction_id(),
            sequence: Some(u64::try_from(idx).unwrap_or(u64::MAX)),
            ts_ms: None,
        };
        if is_snapshot && let CdcChange::Insert { row } = change {
            records.push(encode_debezium_snapshot_row(schema, row, &config, context)?);
            continue;
        }
        records.extend(encode_debezium_change(schema, change, &config, context)?);
    }
    Ok(records)
}

fn encode_debezium_snapshot_pipeline_records(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    rows: &CdcColumnarRowBatch,
    transaction: &TransactionBatch,
) -> anyhow::Result<Vec<DebeziumEncodedRecord>> {
    let config = DebeziumEnvelopeConfig::new(&plan.source_name)?
        .with_emit_tombstones(plan.emit_tombstones)
        .with_transaction_metadata(plan.include_transaction_metadata);
    let mut records = Vec::with_capacity(rows.row_count());
    for row_idx in 0..rows.row_count() {
        let row = rows.row(row_idx)?;
        let context = DebeziumEncodeContext {
            source_position: Some(transaction.commit_position()),
            transaction_id: transaction.transaction_id(),
            sequence: Some(u64::try_from(row_idx).unwrap_or(u64::MAX)),
            ts_ms: None,
        };
        records.push(encode_debezium_snapshot_row(
            schema, &row, &config, context,
        )?);
    }
    Ok(records)
}

fn debezium_records_to_buffer_records(
    records: &[DebeziumEncodedRecord],
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    records
        .iter()
        .map(|record| {
            Ok(CdcBufferRecord::new(
                record.key_json_bytes()?,
                record.value_json_bytes()?,
            ))
        })
        .collect()
}

fn encode_arrow_ipc_pipeline_records(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    batch: &ChangeBatch,
    transaction: &TransactionBatch,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    let mut records = Vec::new();
    let rows_per_record = *REPLICATION_ARROW_IPC_ROWS_PER_RECORD;
    let mut builder = ArrowIpcChangeBatchBuilder::new(schema, rows_per_record);
    let is_snapshot = transaction
        .transaction_id()
        .is_some_and(|tx| tx.as_str().starts_with("snapshot:"));
    for (idx, change) in batch.changes().iter().enumerate() {
        let sequence = u64::try_from(idx).unwrap_or(u64::MAX);
        match change {
            CdcChange::Insert { row } => {
                builder.append_row(row, if is_snapshot { "r" } else { "c" }, 1, sequence)?;
            }
            CdcChange::Update { before, after, .. } => {
                if let Some(before) = before {
                    builder.append_row(before, "u_before", -1, sequence)?;
                    flush_arrow_ipc_record_if_full(plan, transaction, &mut builder, &mut records)?;
                }
                builder.append_row(after, "u", 1, sequence)?;
            }
            CdcChange::Delete { key, before } => match before {
                Some(row) => builder.append_row(row, "d", -1, sequence)?,
                None => {
                    let key = key.as_ref().ok_or_else(|| {
                        anyhow!(
                            "CDC Arrow IPC delete for table '{}' requires a key or before row",
                            schema.table_id().as_str()
                        )
                    })?;
                    let key_row = key_only_row(schema, key)?;
                    builder.append_values(&key_row, "d", -1, sequence)?;
                }
            },
            CdcChange::Truncate => {
                return Err(anyhow!(
                    "CDC Arrow IPC truncate for table '{}' is not supported",
                    schema.table_id().as_str()
                ));
            }
        }
        flush_arrow_ipc_record_if_full(plan, transaction, &mut builder, &mut records)?;
    }
    if !builder.is_empty() {
        records.push(finish_arrow_ipc_record(plan, transaction, &mut builder)?);
    }
    Ok(records)
}

fn encode_arrow_ipc_snapshot_pipeline_records(
    plan: &ReplicationPipelineRuntimePlan,
    schema: &CdcTableSchema,
    rows: &CdcColumnarRowBatch,
    transaction: &TransactionBatch,
) -> anyhow::Result<Vec<CdcBufferRecord>> {
    schema.validate_columnar_rows(rows)?;
    let mut records = Vec::new();
    let rows_per_record = *REPLICATION_ARROW_IPC_ROWS_PER_RECORD;
    for start in (0..rows.row_count()).step_by(rows_per_record) {
        let len = rows.row_count().saturating_sub(start).min(rows_per_record);
        let batch = arrow_ipc_snapshot_record_batch(schema, rows, start, len)?;
        records.push(arrow_ipc_record_from_batch(
            plan,
            transaction,
            start / rows_per_record,
            batch,
        )?);
    }
    Ok(records)
}

fn flush_arrow_ipc_record_if_full(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    builder: &mut ArrowIpcChangeBatchBuilder,
    records: &mut Vec<CdcBufferRecord>,
) -> anyhow::Result<()> {
    if builder.is_full() {
        records.push(finish_arrow_ipc_record(plan, transaction, builder)?);
    }
    Ok(())
}

fn finish_arrow_ipc_record(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    builder: &mut ArrowIpcChangeBatchBuilder,
) -> anyhow::Result<CdcBufferRecord> {
    let chunk_idx = builder.chunk_idx();
    let batch = builder.finish()?;
    arrow_ipc_record_from_batch(plan, transaction, chunk_idx, batch)
}

fn arrow_ipc_record_from_batch(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    chunk_idx: usize,
    batch: RecordBatch,
) -> anyhow::Result<CdcBufferRecord> {
    let mut value = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut value, batch.schema().as_ref())
            .context("create replication Arrow IPC writer")?;
        writer
            .write(&batch)
            .context("write replication Arrow IPC batch")?;
        writer
            .finish()
            .context("finish replication Arrow IPC batch")?;
    }
    let key = format!(
        "{}/{}/{chunk_idx:020}",
        plan.upstream_table,
        source_position_key(transaction.commit_position())
    )
    .into_bytes();
    Ok(CdcBufferRecord::new(Some(key), Some(value)))
}

fn arrow_ipc_snapshot_record_batch(
    schema: &CdcTableSchema,
    rows: &CdcColumnarRowBatch,
    start: usize,
    len: usize,
) -> anyhow::Result<RecordBatch> {
    let end = start.saturating_add(len);
    anyhow::ensure!(
        end <= rows.row_count(),
        "CDC Arrow IPC snapshot range {start}..{end} exceeds {} rows",
        rows.row_count()
    );
    let mut arrays = rows
        .columns()
        .iter()
        .zip(schema.columns())
        .map(|(values, column)| arrow_ipc_columnar_array(values, column, start, end))
        .collect::<anyhow::Result<Vec<_>>>()?;

    let mut operations = StringBuilder::with_capacity(len, len);
    let mut diffs = Int64Builder::with_capacity(len);
    let mut sequences = Int64Builder::with_capacity(len);
    for sequence in start..end {
        operations.append_value("r");
        diffs.append_value(1);
        sequences.append_value(i64::try_from(sequence).unwrap_or(i64::MAX));
    }
    arrays.push(Arc::new(operations.finish()));
    arrays.push(Arc::new(diffs.finish()));
    arrays.push(Arc::new(sequences.finish()));

    RecordBatch::try_new(arrow_ipc_schema(schema), arrays)
        .context("build replication Arrow IPC snapshot batch")
}

fn arrow_ipc_columnar_array(
    values: &CdcColumnarColumn,
    column: &CdcColumn,
    start: usize,
    end: usize,
) -> anyhow::Result<ArrayRef> {
    anyhow::ensure!(
        values.data_type() == column.data_type().clone(),
        "CDC Arrow IPC snapshot column '{}' type {:?} does not match {:?}",
        column.name(),
        values.data_type(),
        column.data_type()
    );
    let array: ArrayRef = match values {
        CdcColumnarColumn::Int64(values) => {
            Arc::new(arrow_array::Int64Array::from(values[start..end].to_vec()))
        }
        CdcColumnarColumn::Bool(values) => {
            Arc::new(arrow_array::BooleanArray::from(values[start..end].to_vec()))
        }
        CdcColumnarColumn::Utf8(values) => {
            let mut builder = StringBuilder::with_capacity(end - start, (end - start) * 16);
            for value in &values[start..end] {
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        CdcColumnarColumn::TimestampMillis(values) => Arc::new(
            arrow_array::TimestampMillisecondArray::from(values[start..end].to_vec()),
        ),
    };
    Ok(array)
}

fn arrow_ipc_schema(schema: &CdcTableSchema) -> Arc<ArrowSchema> {
    let mut fields = schema
        .columns()
        .iter()
        .map(|column| {
            ArrowField::new(
                column.name(),
                match column.data_type() {
                    ColumnType::Int64 => DataType::Int64,
                    ColumnType::Bool => DataType::Boolean,
                    ColumnType::Utf8 => DataType::Utf8,
                    ColumnType::TimestampMillis => {
                        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
                    }
                },
                true,
            )
        })
        .collect::<Vec<_>>();
    fields.push(ArrowField::new("__op", DataType::Utf8, false));
    fields.push(ArrowField::new("__diff", DataType::Int64, false));
    fields.push(ArrowField::new("__sequence", DataType::Int64, false));
    Arc::new(ArrowSchema::new(fields))
}

fn key_only_row(schema: &CdcTableSchema, key: &CdcRowKey) -> anyhow::Result<Vec<Option<RowValue>>> {
    key.validate_against_schema(schema)?;
    let mut values = vec![None; schema.columns().len()];
    for (value, column_idx) in key.values().iter().zip(schema.primary_key_indices()) {
        values[column_idx] = Some(value.clone());
    }
    Ok(values)
}

fn source_position_key(position: &CdcSourcePosition) -> String {
    match position {
        CdcSourcePosition::Postgres {
            commit_lsn,
            event_lsn,
        } => match event_lsn {
            Some(event_lsn) => format!("pg/{commit_lsn}/{event_lsn}"),
            None => format!("pg/{commit_lsn}"),
        },
        CdcSourcePosition::Opaque { value } => format!("opaque/{value}"),
    }
}

enum ArrowIpcColumnBuilder {
    Int64(Int64Builder),
    Bool(BooleanBuilder),
    Utf8(StringBuilder),
    TimestampMillis(TimestampMillisecondBuilder),
}

impl ArrowIpcColumnBuilder {
    fn new(data_type: &ColumnType, capacity: usize) -> Self {
        match data_type {
            ColumnType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            ColumnType::Bool => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            ColumnType::Utf8 => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 16)),
            ColumnType::TimestampMillis => {
                Self::TimestampMillis(TimestampMillisecondBuilder::with_capacity(capacity))
            }
        }
    }

    fn append(&mut self, column: &CdcColumn, value: Option<&RowValue>) -> anyhow::Result<()> {
        match (self, column.data_type(), value) {
            (Self::Int64(builder), ColumnType::Int64, Some(RowValue::Int64(value))) => {
                builder.append_value(*value);
            }
            (Self::Bool(builder), ColumnType::Bool, Some(RowValue::Bool(value))) => {
                builder.append_value(*value);
            }
            (Self::Utf8(builder), ColumnType::Utf8, Some(RowValue::Utf8(value))) => {
                builder.append_value(value);
            }
            (
                Self::TimestampMillis(builder),
                ColumnType::TimestampMillis,
                Some(RowValue::TimestampMillis(value)),
            ) => {
                builder.append_value(*value);
            }
            (Self::Int64(builder), ColumnType::Int64, None) => builder.append_null(),
            (Self::Bool(builder), ColumnType::Bool, None) => builder.append_null(),
            (Self::Utf8(builder), ColumnType::Utf8, None) => builder.append_null(),
            (Self::TimestampMillis(builder), ColumnType::TimestampMillis, None) => {
                builder.append_null();
            }
            (_, _, Some(value)) => {
                return Err(anyhow!(
                    "CDC Arrow IPC value for column '{}' does not match type {:?}: {:?}",
                    column.name(),
                    column.data_type(),
                    value
                ));
            }
            _ => {
                return Err(anyhow!(
                    "CDC Arrow IPC builder for column '{}' does not match type {:?}",
                    column.name(),
                    column.data_type()
                ));
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::TimestampMillis(builder) => Arc::new(builder.finish()),
        }
    }
}

struct ArrowIpcChangeBatchBuilder {
    schema: CdcTableSchema,
    arrow_schema: Arc<ArrowSchema>,
    columns: Vec<ArrowIpcColumnBuilder>,
    operations: StringBuilder,
    diffs: Int64Builder,
    sequences: Int64Builder,
    len: usize,
    capacity: usize,
    chunk_idx: usize,
}

impl ArrowIpcChangeBatchBuilder {
    fn new(schema: &CdcTableSchema, capacity: usize) -> Self {
        Self {
            schema: schema.clone(),
            arrow_schema: arrow_ipc_schema(schema),
            columns: schema
                .columns()
                .iter()
                .map(|column| ArrowIpcColumnBuilder::new(column.data_type(), capacity))
                .collect(),
            operations: StringBuilder::with_capacity(capacity, capacity * 2),
            diffs: Int64Builder::with_capacity(capacity),
            sequences: Int64Builder::with_capacity(capacity),
            len: 0,
            capacity,
            chunk_idx: 0,
        }
    }

    fn append_row(
        &mut self,
        row: &CdcRow,
        operation: &str,
        diff: i64,
        sequence: u64,
    ) -> anyhow::Result<()> {
        self.append_values(row.values(), operation, diff, sequence)
    }

    fn append_values(
        &mut self,
        values: &[Option<RowValue>],
        operation: &str,
        diff: i64,
        sequence: u64,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            values.len() == self.schema.columns().len(),
            "CDC Arrow IPC row has {} values, expected {}",
            values.len(),
            self.schema.columns().len()
        );
        for ((builder, column), value) in self
            .columns
            .iter_mut()
            .zip(self.schema.columns())
            .zip(values)
        {
            builder.append(column, value.as_ref())?;
        }
        self.operations.append_value(operation);
        self.diffs.append_value(diff);
        self.sequences
            .append_value(i64::try_from(sequence).unwrap_or(i64::MAX));
        self.len += 1;
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn is_full(&self) -> bool {
        self.len >= self.capacity
    }

    fn chunk_idx(&self) -> usize {
        self.chunk_idx
    }

    fn finish(&mut self) -> anyhow::Result<RecordBatch> {
        let mut arrays = self
            .columns
            .iter_mut()
            .map(ArrowIpcColumnBuilder::finish)
            .collect::<Vec<_>>();
        arrays.push(Arc::new(self.operations.finish()));
        arrays.push(Arc::new(self.diffs.finish()));
        arrays.push(Arc::new(self.sequences.finish()));
        let batch = RecordBatch::try_new(Arc::clone(&self.arrow_schema), arrays)
            .context("build replication Arrow IPC batch")?;
        self.columns = self
            .schema
            .columns()
            .iter()
            .map(|column| ArrowIpcColumnBuilder::new(column.data_type(), self.capacity))
            .collect();
        self.operations = StringBuilder::with_capacity(self.capacity, self.capacity * 2);
        self.diffs = Int64Builder::with_capacity(self.capacity);
        self.sequences = Int64Builder::with_capacity(self.capacity);
        self.len = 0;
        self.chunk_idx += 1;
        Ok(batch)
    }
}

fn pending_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
) -> std::collections::BTreeMap<String, String> {
    let mut state = std::collections::BTreeMap::new();
    state.insert("source.table".to_string(), plan.upstream_table.clone());
    state.insert("buffer.status".to_string(), "pending".to_string());
    state.insert(
        "buffer.transaction_key".to_string(),
        manifest.transaction_key().to_string(),
    );
    state.insert(
        "buffer.record_count".to_string(),
        manifest.record_count().to_string(),
    );
    state
}

fn delivered_target_state(
    manifest: &CdcBufferedTransactionManifest,
    mut target_state: std::collections::BTreeMap<String, String>,
) -> std::collections::BTreeMap<String, String> {
    target_state.insert("buffer.status".to_string(), "delivered".to_string());
    target_state.insert(
        "buffer.transaction_key".to_string(),
        manifest.transaction_key().to_string(),
    );
    target_state.insert(
        "buffer.record_count".to_string(),
        manifest.record_count().to_string(),
    );
    target_state
}

async fn record_buffer_stats(
    buffer_store: &CdcBufferStore,
    pipeline_name: &str,
) -> anyhow::Result<()> {
    let stats = buffer_store
        .stats(pipeline_name, current_unix_time_ms())
        .await
        .with_context(|| format!("load CDC buffer stats for pipeline '{pipeline_name}'"))?;
    crate::metrics::record_cdc_buffer_pending(
        pipeline_name,
        stats.pending_transactions(),
        stats.pending_records(),
        stats.pending_bytes(),
        stats.oldest_pending_age_ms(),
    );
    Ok(())
}

pub(super) fn replication_pipeline_table_id(
    source_name: &str,
    upstream_table: &str,
) -> anyhow::Result<CdcTableId> {
    CdcTableId::new(format!("{source_name}:{upstream_table}"))
}

pub(super) fn materialized_transaction(
    source_id: &CdcSourceId,
    materialized_table_ids: &HashSet<CdcTableId>,
    transaction: &TransactionBatch,
) -> anyhow::Result<Option<TransactionBatch>> {
    let change_batches = transaction
        .change_batches()
        .iter()
        .filter(|batch| materialized_table_ids.contains(batch.table_id()))
        .cloned()
        .collect::<Vec<_>>();
    if change_batches.is_empty() {
        return Ok(None);
    }
    Ok(Some(TransactionBatch::new(
        source_id.clone(),
        transaction.transaction_id().cloned(),
        transaction.start_position().cloned(),
        transaction.commit_position().clone(),
        change_batches,
    )?))
}

pub(super) fn pipeline_checkpoint_from_transaction(
    transaction: &TransactionBatch,
) -> CdcCheckpoint {
    CdcCheckpoint::new(
        transaction.source_id().clone(),
        transaction.commit_position().clone(),
        transaction.transaction_id().cloned(),
    )
}

fn current_unix_time_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use floe_cdc_core::{CdcColumn, CdcPrimaryKey, CdcRow, CdcTransactionId, UpstreamTableRef};
    use floe_core::RowValue;
    use floe_core::catalog::ColumnType;

    #[test]
    fn materialized_transaction_filters_non_materialized_batches() {
        let source_id = CdcSourceId::new("pg_main").unwrap();
        let materialized = CdcTableId::new("orders").unwrap();
        let passthrough = CdcTableId::new("pg_main:public.customers").unwrap();
        let transaction = TransactionBatch::new(
            source_id.clone(),
            None,
            None,
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
            vec![
                ChangeBatch::new(
                    materialized.clone(),
                    vec![CdcChange::Insert {
                        row: row(1, "open"),
                    }],
                )
                .unwrap(),
                ChangeBatch::new(passthrough, vec![CdcChange::Insert { row: row(2, "new") }])
                    .unwrap(),
            ],
        )
        .unwrap();

        let filtered = materialized_transaction(
            &source_id,
            &HashSet::from([materialized.clone()]),
            &transaction,
        )
        .unwrap()
        .unwrap();

        assert_eq!(filtered.change_batches().len(), 1);
        assert_eq!(filtered.change_batches()[0].table_id(), &materialized);
    }

    #[test]
    fn pipeline_snapshot_records_use_read_operation() {
        let plan = ReplicationPipelineRuntimePlan {
            name: "p".to_string(),
            source_name: "pg_main".to_string(),
            upstream_table: "public.orders".to_string(),
            table_id: CdcTableId::new("orders").unwrap(),
            target: ReplicationPipelineRuntimeTarget::Kafka {
                brokers: "localhost:9092".to_string(),
                topic: "orders".to_string(),
            },
            format: ReplicationPipelineRuntimeFormat::DebeziumJson,
            emit_tombstones: false,
            include_transaction_metadata: false,
        };
        let schema = schema(plan.table_id.clone());
        let batch = ChangeBatch::new(
            plan.table_id.clone(),
            vec![CdcChange::Insert {
                row: row(1, "open"),
            }],
        )
        .unwrap();
        let transaction = TransactionBatch::new(
            CdcSourceId::new("pg_main").unwrap(),
            Some(CdcTransactionId::new("snapshot:0/16B6C50").unwrap()),
            None,
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
            vec![batch.clone()],
        )
        .unwrap();

        let records =
            encode_debezium_pipeline_records(&plan, &schema, &batch, &transaction).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value().unwrap()["op"], "r");
        assert_eq!(records[0].value().unwrap()["source"]["snapshot"], "true");
    }

    #[test]
    fn pipeline_arrow_ipc_records_encode_batches_without_json() {
        let plan = ReplicationPipelineRuntimePlan {
            name: "p".to_string(),
            source_name: "pg_main".to_string(),
            upstream_table: "public.orders".to_string(),
            table_id: CdcTableId::new("orders").unwrap(),
            target: ReplicationPipelineRuntimeTarget::Kafka {
                brokers: "localhost:9092".to_string(),
                topic: "orders".to_string(),
            },
            format: ReplicationPipelineRuntimeFormat::ArrowIpc,
            emit_tombstones: false,
            include_transaction_metadata: false,
        };
        let schema = schema(plan.table_id.clone());
        let batch = ChangeBatch::new(
            plan.table_id.clone(),
            vec![
                CdcChange::Insert {
                    row: row(1, "open"),
                },
                CdcChange::Update {
                    key: None,
                    before: Some(row(1, "open")),
                    after: row(1, "paid"),
                },
            ],
        )
        .unwrap();
        let transaction = TransactionBatch::new(
            CdcSourceId::new("pg_main").unwrap(),
            Some(CdcTransactionId::new("tx-1").unwrap()),
            None,
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
            vec![batch.clone()],
        )
        .unwrap();

        let records = encode_pipeline_buffer_records(&plan, &schema, &batch, &transaction)
            .expect("encode arrow records");
        assert_eq!(records.len(), 1);
        let payload = records[0].value().expect("payload");
        assert!(!payload.starts_with(b"{"));

        let mut reader =
            arrow_ipc::reader::StreamReader::try_new(payload, None).expect("arrow reader");
        let batch = reader
            .next()
            .expect("one record batch")
            .expect("decode batch");
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(2).name(), "__op");
    }

    #[test]
    fn pipeline_arrow_ipc_records_encode_columnar_snapshot_without_json() {
        let plan = ReplicationPipelineRuntimePlan {
            name: "p".to_string(),
            source_name: "pg_main".to_string(),
            upstream_table: "public.orders".to_string(),
            table_id: CdcTableId::new("orders").unwrap(),
            target: ReplicationPipelineRuntimeTarget::Kafka {
                brokers: "localhost:9092".to_string(),
                topic: "orders".to_string(),
            },
            format: ReplicationPipelineRuntimeFormat::ArrowIpc,
            emit_tombstones: false,
            include_transaction_metadata: false,
        };
        let schema = schema(plan.table_id.clone());
        let rows = CdcColumnarRowBatch::new(vec![
            CdcColumnarColumn::Int64(vec![Some(1), Some(2)]),
            CdcColumnarColumn::Utf8(vec![Some("open".to_string()), Some("paid".to_string())]),
        ])
        .unwrap();
        let batch =
            ChangeBatch::new_snapshot_insert(plan.table_id.clone(), rows).expect("snapshot batch");
        let transaction = TransactionBatch::new(
            CdcSourceId::new("pg_main").unwrap(),
            Some(CdcTransactionId::new("snapshot:0/16B6C50").unwrap()),
            None,
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
            vec![batch.clone()],
        )
        .unwrap();

        let records = encode_pipeline_buffer_records(&plan, &schema, &batch, &transaction)
            .expect("encode arrow snapshot records");
        assert_eq!(records.len(), 1);
        let payload = records[0].value().expect("payload");
        assert!(!payload.starts_with(b"{"));

        let mut reader =
            arrow_ipc::reader::StreamReader::try_new(payload, None).expect("arrow reader");
        let batch = reader
            .next()
            .expect("one record batch")
            .expect("decode batch");
        assert_eq!(batch.num_rows(), 2);
        let ops = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("op column");
        assert_eq!(ops.value(0), "r");
        assert_eq!(ops.value(1), "r");
    }

    #[test]
    fn pipeline_transaction_records_include_all_matching_snapshot_chunks() {
        let plan = ReplicationPipelineRuntimePlan {
            name: "p".to_string(),
            source_name: "pg_main".to_string(),
            upstream_table: "public.orders".to_string(),
            table_id: CdcTableId::new("orders").unwrap(),
            target: ReplicationPipelineRuntimeTarget::Kafka {
                brokers: "localhost:9092".to_string(),
                topic: "orders".to_string(),
            },
            format: ReplicationPipelineRuntimeFormat::ArrowIpc,
            emit_tombstones: false,
            include_transaction_metadata: false,
        };
        let schema = schema(plan.table_id.clone());
        let first_rows = CdcColumnarRowBatch::new(vec![
            CdcColumnarColumn::Int64(vec![Some(1)]),
            CdcColumnarColumn::Utf8(vec![Some("open".to_string())]),
        ])
        .unwrap();
        let second_rows = CdcColumnarRowBatch::new(vec![
            CdcColumnarColumn::Int64(vec![Some(2), Some(3)]),
            CdcColumnarColumn::Utf8(vec![Some("paid".to_string()), Some("void".to_string())]),
        ])
        .unwrap();
        let transaction = TransactionBatch::new(
            CdcSourceId::new("pg_main").unwrap(),
            Some(CdcTransactionId::new("snapshot:0/16B6C50").unwrap()),
            None,
            floe_cdc_core::CdcSourcePosition::postgres("0/16B6C50", None).unwrap(),
            vec![
                ChangeBatch::new_snapshot_insert(plan.table_id.clone(), first_rows)
                    .expect("first snapshot batch"),
                ChangeBatch::new_snapshot_insert(plan.table_id.clone(), second_rows)
                    .expect("second snapshot batch"),
            ],
        )
        .unwrap();
        let schemas = HashMap::from([(plan.table_id.clone(), schema)]);

        let records = encode_pipeline_transaction_records(&plan, &schemas, &transaction)
            .expect("encode transaction records");

        assert_eq!(records.len(), 2);
        let decoded_rows = records
            .iter()
            .map(|record| {
                let payload = record.value().expect("payload");
                let mut reader =
                    arrow_ipc::reader::StreamReader::try_new(payload, None).expect("arrow reader");
                reader
                    .next()
                    .expect("one record batch")
                    .expect("decode batch")
                    .num_rows()
            })
            .collect::<Vec<_>>();
        assert_eq!(decoded_rows, vec![1, 2]);
    }

    fn schema(table_id: CdcTableId) -> CdcTableSchema {
        CdcTableSchema::new(
            table_id,
            UpstreamTableRef::new("public", "orders").unwrap(),
            vec![
                CdcColumn::new("id", ColumnType::Int64, false).unwrap(),
                CdcColumn::new("status", ColumnType::Utf8, true).unwrap(),
            ],
            CdcPrimaryKey::new(["id"]).unwrap(),
        )
        .unwrap()
    }

    fn row(id: i64, status: &str) -> CdcRow {
        CdcRow::new([
            Some(RowValue::Int64(id)),
            Some(RowValue::Utf8(status.to_string())),
        ])
        .unwrap()
    }
}
