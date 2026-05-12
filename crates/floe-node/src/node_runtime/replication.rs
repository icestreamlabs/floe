use super::*;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
            let Some(change_batch) = transaction
                .change_batches()
                .iter()
                .find(|batch| batch.table_id() == &plan.table_id)
            else {
                continue;
            };
            let schema = schemas.get(&plan.table_id).ok_or_else(|| {
                anyhow!(
                    "replication pipeline '{}' references missing CDC schema '{}'",
                    plan.name,
                    plan.table_id.as_str()
                )
            })?;
            let records = encode_pipeline_records(plan, schema, change_batch, transaction)?;
            if records.is_empty() {
                continue;
            }
            let buffered_records = debezium_records_to_buffer_records(&records)?;
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
                written = written.saturating_add(records.len());
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

fn encode_pipeline_records(
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

        let records = encode_pipeline_records(&plan, &schema, &batch, &transaction).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value().unwrap()["op"], "r");
        assert_eq!(records[0].value().unwrap()["source"]["snapshot"], "true");
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
