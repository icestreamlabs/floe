use std::collections::BTreeMap;

use floe_cdc_core::{CdcSourcePosition, CdcTransactionId, TransactionBatch};
use floe_core::catalog::ReplicationErrorPolicyMode as CatalogReplicationErrorPolicyMode;
use floe_storage::{
    CdcBufferPayloadFormat, CdcBufferedTransactionManifest, ReplicationPipelineDlqEntry,
};

use super::super::{ReplicationPipelineRuntimePlan, ReplicationPipelineRuntimeTarget};
use super::{current_unix_time_ms, encoding};

#[derive(Debug, Clone, Copy)]
pub(super) enum BufferStatus {
    Durable,
    Delivered,
    DeadLettered,
    NotBuffered,
}

impl BufferStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::Delivered => "delivered",
            Self::DeadLettered => "dead_lettered",
            Self::NotBuffered => "not_buffered",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum DeliveryStatus {
    Pending,
    Delivered,
    Failed,
    DeadLettered,
}

impl DeliveryStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Delivered => "delivered",
            Self::Failed => "failed",
            Self::DeadLettered => "dead_lettered",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TargetFailureClass {
    Retryable,
    Permanent,
}

impl TargetFailureClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::Permanent => "permanent",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct TargetStateBuilder {
    state: BTreeMap<String, String>,
}

impl TargetStateBuilder {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn from_state(state: BTreeMap<String, String>) -> Self {
        Self { state }
    }

    pub(super) fn for_buffered_manifest(
        plan: &ReplicationPipelineRuntimePlan,
        manifest: &CdcBufferedTransactionManifest,
    ) -> Self {
        let mut builder = Self::new();
        builder
            .source_table(&plan.upstream_table)
            .target_kind(target_kind(plan))
            .buffer_transaction_key(manifest.transaction_key())
            .buffer_record_count(manifest.record_count())
            .buffer_payload_format(manifest.payload_format());
        if let Some(transaction_id) = manifest.transaction_id() {
            builder.source_transaction_id(transaction_id);
        }
        builder.source_position(manifest.source_position());
        builder
    }

    pub(super) fn for_direct_transaction(
        plan: &ReplicationPipelineRuntimePlan,
        transaction: &TransactionBatch,
    ) -> Self {
        let mut builder = Self::new();
        builder
            .source_table(&plan.upstream_table)
            .target_kind(target_kind(plan));
        if let Some(transaction_id) = transaction.transaction_id() {
            builder.source_transaction_id(transaction_id);
        }
        builder.source_position(transaction.commit_position());
        builder
    }

    pub(super) fn source_table(&mut self, table: impl Into<String>) -> &mut Self {
        self.state.insert("source.table".to_string(), table.into());
        self
    }

    pub(super) fn source_transaction_id(&mut self, transaction_id: &CdcTransactionId) -> &mut Self {
        self.state.insert(
            "source.transaction_id".to_string(),
            transaction_id.as_str().to_string(),
        );
        self
    }

    pub(super) fn source_position(&mut self, position: &CdcSourcePosition) -> &mut Self {
        match position {
            CdcSourcePosition::Postgres {
                commit_lsn,
                event_lsn,
            } => {
                self.state.insert(
                    "source.position.postgres.commit_lsn".to_string(),
                    commit_lsn.clone(),
                );
                if let Some(event_lsn) = event_lsn {
                    self.state.insert(
                        "source.position.postgres.event_lsn".to_string(),
                        event_lsn.clone(),
                    );
                }
            }
            CdcSourcePosition::Opaque { value } => {
                self.state
                    .insert("source.position".to_string(), value.clone());
            }
        }
        self
    }

    pub(super) fn target_kind(&mut self, kind: impl Into<String>) -> &mut Self {
        self.state.insert("target.kind".to_string(), kind.into());
        self
    }

    pub(super) fn target_topic(&mut self, topic: impl Into<String>) -> &mut Self {
        self.state.insert("kafka.topic".to_string(), topic.into());
        self
    }

    pub(super) fn target_partition_offset(&mut self, partition: i32, offset: i64) -> &mut Self {
        self.state.insert(
            format!("kafka.partition.{partition}.offset"),
            offset.to_string(),
        );
        self
    }

    pub(super) fn postgres_table(&mut self, table: impl Into<String>) -> &mut Self {
        self.state
            .insert("postgres.table".to_string(), table.into());
        self
    }

    pub(super) fn postgres_records_applied(&mut self, records: usize) -> &mut Self {
        self.state
            .insert("postgres.records_applied".to_string(), records.to_string());
        self
    }

    pub(super) fn buffer_status(&mut self, status: BufferStatus) -> &mut Self {
        self.state
            .insert("buffer.status".to_string(), status.as_str().to_string());
        self
    }

    pub(super) fn buffer_transaction_key(&mut self, transaction_key: &str) -> &mut Self {
        self.state.insert(
            "buffer.transaction_key".to_string(),
            transaction_key.to_string(),
        );
        self
    }

    pub(super) fn buffer_record_count(&mut self, record_count: usize) -> &mut Self {
        self.state
            .insert("buffer.record_count".to_string(), record_count.to_string());
        self
    }

    pub(super) fn buffer_payload_format(
        &mut self,
        payload_format: CdcBufferPayloadFormat,
    ) -> &mut Self {
        self.state.insert(
            "buffer.payload_format".to_string(),
            format!("{payload_format:?}"),
        );
        self
    }

    pub(super) fn delivery_status(&mut self, status: DeliveryStatus) -> &mut Self {
        self.state.insert(
            "target.delivery.status".to_string(),
            status.as_str().to_string(),
        );
        self
    }

    pub(super) fn replay_may_duplicate(&mut self, may_duplicate: bool) -> &mut Self {
        self.state.insert(
            "target.delivery.replay_may_duplicate".to_string(),
            may_duplicate.to_string(),
        );
        self
    }

    pub(super) fn dlq_entry(&mut self, dlq_entry: &ReplicationPipelineDlqEntry) -> &mut Self {
        self.state
            .insert("target.dlq.id".to_string(), dlq_entry.dlq_id().to_string());
        self.state.insert(
            "target.dlq.status".to_string(),
            dlq_entry.status().as_str().to_string(),
        );
        if let Some(payload_object_key) = dlq_entry.payload_object_key() {
            self.state.insert(
                "target.dlq.payload_object_key".to_string(),
                payload_object_key.to_string(),
            );
        }
        self
    }

    pub(super) fn last_error(&mut self, err: &anyhow::Error) -> &mut Self {
        self.state.insert(
            "target.last_error".to_string(),
            truncate_target_error(&format!("{err:#}")),
        );
        self
    }

    pub(super) fn failure_class(&mut self, class: TargetFailureClass) -> &mut Self {
        self.state.insert(
            "target.failure.class".to_string(),
            class.as_str().to_string(),
        );
        self
    }

    pub(super) fn build(self) -> BTreeMap<String, String> {
        self.state
    }
}

pub(super) fn pending_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
) -> BTreeMap<String, String> {
    let mut builder = TargetStateBuilder::for_buffered_manifest(plan, manifest);
    builder
        .buffer_status(BufferStatus::Durable)
        .delivery_status(DeliveryStatus::Pending)
        .replay_may_duplicate(true);
    builder.build()
}

pub(super) fn delivered_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    target_state: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut base = TargetStateBuilder::for_buffered_manifest(plan, manifest).build();
    base.extend(target_state);
    let mut builder = TargetStateBuilder::from_state(base);
    builder
        .buffer_status(BufferStatus::Delivered)
        .delivery_status(DeliveryStatus::Delivered)
        .replay_may_duplicate(false);
    builder.build()
}

pub(super) fn failed_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    err: &anyhow::Error,
) -> BTreeMap<String, String> {
    let class = classify_target_write_failure(plan, err);
    let mut builder = TargetStateBuilder::for_buffered_manifest(plan, manifest);
    builder
        .buffer_status(BufferStatus::Durable)
        .delivery_status(DeliveryStatus::Failed)
        .replay_may_duplicate(true)
        .failure_class(class)
        .last_error(err);
    builder.build()
}

pub(super) fn dead_lettered_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    manifest: &CdcBufferedTransactionManifest,
    dlq_entry: &ReplicationPipelineDlqEntry,
    err: &anyhow::Error,
) -> BTreeMap<String, String> {
    let mut builder = TargetStateBuilder::for_buffered_manifest(plan, manifest);
    builder.buffer_status(BufferStatus::DeadLettered);
    add_dead_letter_state(&mut builder, plan, dlq_entry, err);
    builder.build()
}

pub(super) fn direct_delivered_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    record_count: usize,
    payload_format: CdcBufferPayloadFormat,
    target_state: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut base = TargetStateBuilder::for_direct_transaction(plan, transaction).build();
    base.extend(target_state);
    let mut builder = TargetStateBuilder::from_state(base);
    builder
        .buffer_status(BufferStatus::NotBuffered)
        .buffer_record_count(record_count)
        .buffer_payload_format(payload_format)
        .delivery_status(DeliveryStatus::Delivered)
        .replay_may_duplicate(false);
    builder.build()
}

pub(super) fn direct_dead_lettered_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    record_count: usize,
    payload_format: CdcBufferPayloadFormat,
    dlq_entry: &ReplicationPipelineDlqEntry,
    err: &anyhow::Error,
) -> BTreeMap<String, String> {
    let state = direct_delivered_target_state(
        plan,
        transaction,
        record_count,
        payload_format,
        BTreeMap::new(),
    );
    let mut builder = TargetStateBuilder::from_state(state);
    add_dead_letter_state(&mut builder, plan, dlq_entry, err);
    builder.build()
}

fn add_dead_letter_state(
    builder: &mut TargetStateBuilder,
    plan: &ReplicationPipelineRuntimePlan,
    dlq_entry: &ReplicationPipelineDlqEntry,
    err: &anyhow::Error,
) {
    let class = classify_target_write_failure(plan, err);
    builder
        .delivery_status(DeliveryStatus::DeadLettered)
        .replay_may_duplicate(false)
        .failure_class(class)
        .dlq_entry(dlq_entry)
        .last_error(err);
}

pub(super) fn dead_letter_target_state(
    plan: &ReplicationPipelineRuntimePlan,
    err: &anyhow::Error,
) -> BTreeMap<String, String> {
    let class = classify_target_write_failure(plan, err);
    let mut builder = TargetStateBuilder::new();
    builder
        .target_kind(target_kind(plan))
        .delivery_status(DeliveryStatus::DeadLettered)
        .failure_class(class)
        .last_error(err);
    builder.build()
}

pub(super) fn classify_target_write_failure(
    plan: &ReplicationPipelineRuntimePlan,
    err: &anyhow::Error,
) -> TargetFailureClass {
    match &plan.target {
        ReplicationPipelineRuntimeTarget::Kafka { .. } => classify_kafka_target_write_failure(err),
        ReplicationPipelineRuntimeTarget::Postgres { .. } => {
            classify_postgres_target_write_failure(err)
        }
    }
}

fn classify_kafka_target_write_failure(err: &anyhow::Error) -> TargetFailureClass {
    let message = format!("{err:#}").to_ascii_lowercase();
    if message.contains("has no kafka writer")
        || message.contains("message size too large")
        || message.contains("invalid topic")
        || message.contains("unknown topic or partition")
    {
        TargetFailureClass::Permanent
    } else {
        TargetFailureClass::Retryable
    }
}

fn classify_postgres_target_write_failure(err: &anyhow::Error) -> TargetFailureClass {
    if let Some(class) = postgres_sqlstate_failure_class(err) {
        return class;
    }
    let message = format!("{err:#}").to_ascii_lowercase();
    if message.contains("has no postgres writer")
        || message.contains("permission denied")
        || message.contains("does not exist")
        || message.contains("duplicate key")
        || message.contains("violates")
        || message.contains("invalid input syntax")
        || message.contains("type mismatch")
        || message.contains("cannot be null")
    {
        TargetFailureClass::Permanent
    } else {
        TargetFailureClass::Retryable
    }
}

fn postgres_sqlstate_failure_class(err: &anyhow::Error) -> Option<TargetFailureClass> {
    err.chain()
        .find_map(|source| source.downcast_ref::<tokio_postgres::Error>())
        .and_then(|err| err.code())
        .map(|code| match code.code() {
            "08000" | "08001" | "08003" | "08004" | "08006" | "08007" | "40001" | "40P01"
            | "53300" | "55P03" | "57014" | "57P01" | "57P02" | "57P03" => {
                TargetFailureClass::Retryable
            }
            sqlstate
                if sqlstate.starts_with("22")
                    || sqlstate.starts_with("23")
                    || sqlstate.starts_with("28")
                    || sqlstate.starts_with("3D")
                    || sqlstate.starts_with("42") =>
            {
                TargetFailureClass::Permanent
            }
            _ => TargetFailureClass::Retryable,
        })
}

pub(super) fn target_kind(plan: &ReplicationPipelineRuntimePlan) -> &'static str {
    match &plan.target {
        ReplicationPipelineRuntimeTarget::Kafka { .. } => "kafka",
        ReplicationPipelineRuntimeTarget::Postgres { .. } => "postgres",
    }
}

pub(super) fn replication_pipeline_uses_dlq(plan: &ReplicationPipelineRuntimePlan) -> bool {
    plan.error_policy.mode() == CatalogReplicationErrorPolicyMode::DeadLetterAndContinue
}

pub(super) fn replication_pipeline_dlq_id(
    source_position: &CdcSourcePosition,
    transaction_id: Option<&CdcTransactionId>,
) -> String {
    let position = encoding::source_position_key(source_position);
    let transaction = transaction_id.map_or("none", CdcTransactionId::as_str);
    format!(
        "{}-{}-{}",
        hex_component(position.as_bytes()),
        hex_component(transaction.as_bytes()),
        current_unix_time_ms()
    )
}

fn hex_component(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0F) as usize] as char);
    }
    out
}

pub(super) fn truncate_target_error(message: &str) -> String {
    const MAX_ERROR_LEN: usize = 512;
    if message.len() <= MAX_ERROR_LEN {
        return message.to_string();
    }
    let mut truncated = message
        .chars()
        .take(MAX_ERROR_LEN.saturating_sub(3))
        .collect::<String>();
    truncated.push_str("...");
    truncated
}
