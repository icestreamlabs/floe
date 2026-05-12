use super::*;

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use floe_node_core::debezium_encoder::{
    DebeziumEncodeContext, DebeziumEncodedRecord, DebeziumEnvelopeConfig, encode_debezium_change,
    encode_debezium_snapshot_row,
};
use floe_storage::{ReplicationPipelineCheckpoint, SlateCatalog};
use futures::future::join_all;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

const REPLICATION_KAFKA_RETRY_ATTEMPTS: usize = 5;
const REPLICATION_KAFKA_RETRY_BASE_MS: u64 = 50;

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
            let mut target_state = match &plan.target {
                ReplicationPipelineRuntimeTarget::Kafka { .. } => {
                    let writer =
                        self.kafka_writers_by_pipeline
                            .get(&plan.name)
                            .ok_or_else(|| {
                                anyhow!("replication pipeline '{}' has no Kafka writer", plan.name)
                            })?;
                    writer.send_records(&records).await?
                }
            };
            target_state.insert("source.table".to_string(), plan.upstream_table.clone());
            if let Some(storage) = storage {
                storage
                    .put_replication_pipeline_checkpoint(ReplicationPipelineCheckpoint::new(
                        &plan.name,
                        &plan.source_name,
                        transaction.commit_position().clone(),
                        transaction.transaction_id().cloned(),
                        target_state,
                        current_unix_time_ms(),
                    )?)
                    .await
                    .with_context(|| {
                        format!("persist replication pipeline '{}' checkpoint", plan.name)
                    })?;
            }
            written = written.saturating_add(records.len());
        }

        Ok(written)
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
            .set("enable.idempotence", "true");
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
        records: &[DebeziumEncodedRecord],
    ) -> anyhow::Result<BTreeMap<String, String>> {
        let deliveries = join_all(
            records
                .iter()
                .map(|record| self.send_record_with_retry(record)),
        )
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;

        let mut target_state = BTreeMap::new();
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

    async fn send_record_with_retry(
        &self,
        record: &DebeziumEncodedRecord,
    ) -> anyhow::Result<(i32, i64)> {
        let key = record.key_json_bytes()?;
        let value = record.value_json_bytes()?;
        for attempt in 0..REPLICATION_KAFKA_RETRY_ATTEMPTS {
            let mut kafka_record = FutureRecord::<[u8], [u8]>::to(&self.topic);
            if let Some(key) = key.as_deref() {
                kafka_record = kafka_record.key(key);
            }
            if let Some(value) = value.as_deref() {
                kafka_record = kafka_record.payload(value);
            }

            match self
                .producer
                .send(kafka_record, Duration::from_secs(0))
                .await
            {
                Ok((partition, offset)) => return Ok((partition, offset)),
                Err((err, _message)) if attempt + 1 < REPLICATION_KAFKA_RETRY_ATTEMPTS => {
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
                Err((err, _message)) => {
                    return Err(anyhow!(
                        "replication pipeline Kafka send failed after retries: {err}"
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
