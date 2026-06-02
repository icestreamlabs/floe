use super::*;

use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use floe_cdc_core::{CdcSourceId, CdcTableId, CdcTableSchema, TransactionBatch};
use floe_config::ReplicationConfig as FloeReplicationConfig;
#[cfg(test)]
use floe_storage::CdcBufferRecord;
use floe_storage::{
    CdcBufferPayloadFormat, CdcBufferStore, CdcBufferedTransactionManifest,
    ReplicationPipelineCheckpoint, ReplicationPipelineDlqEntry, ReplicationPipelineDlqStatus,
    SlateCatalog, decode_cdc_buffer_records_payload,
};
use futures::future::join_all;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReplicationPipelineDlqRetryBatchOutcome {
    pub(crate) pipeline: String,
    pub(crate) requested_limit: usize,
    pub(crate) attempted: usize,
    pub(crate) replayed: Vec<ReplicationPipelineDlqEntry>,
    pub(crate) failed: Vec<ReplicationPipelineDlqRetryFailure>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReplicationPipelineDlqRetryFailure {
    pub(crate) dlq_id: String,
    pub(crate) error: String,
    pub(crate) entry: Option<ReplicationPipelineDlqEntry>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ReplicationPipelineReconciliationOptions {
    pub(crate) max_rows: usize,
    pub(crate) full_scan: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReplicationPipelineReconciliationReport {
    pub(crate) pipeline: String,
    pub(crate) source: String,
    pub(crate) upstream_table: String,
    pub(crate) target_kind: String,
    pub(crate) target_table: Option<String>,
    pub(crate) checkpoint_position: Option<String>,
    pub(crate) checkpoint_lsn_bytes: Option<u64>,
    pub(crate) pending_transactions: usize,
    pub(crate) pending_records: usize,
    pub(crate) max_rows: usize,
    pub(crate) full_scan: bool,
    pub(crate) status: String,
    pub(crate) source_observation: Option<ReplicationPipelineReconciliationObservation>,
    pub(crate) target_observation: Option<ReplicationPipelineReconciliationObservation>,
    pub(crate) drift: Vec<ReplicationPipelineReconciliationDrift>,
    pub(crate) next_steps: Vec<String>,
    pub(crate) observed_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReplicationPipelineReconciliationObservation {
    pub(crate) table: String,
    pub(crate) row_count: Option<u64>,
    pub(crate) row_count_lower_bound: Option<u64>,
    pub(crate) exact: bool,
    pub(crate) observed_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ReplicationPipelineReconciliationDrift {
    pub(crate) kind: String,
    pub(crate) source_table: String,
    pub(crate) target_table: String,
    pub(crate) source_count: Option<u64>,
    pub(crate) target_count: Option<u64>,
    pub(crate) detail: String,
}

pub(crate) struct ReplicationPipelineRuntime {
    pipelines_by_source: HashMap<CdcSourceId, Vec<ReplicationPipelineRuntimePlan>>,
    kafka_writers_by_pipeline: HashMap<String, Arc<writers::KafkaReplicationPipelineWriter>>,
    postgres_writers_by_pipeline: HashMap<String, Arc<writers::PostgresReplicationPipelineWriter>>,
    buffer_cleanup_last_by_pipeline: Mutex<HashMap<String, u64>>,
    replay_state_by_pipeline: Mutex<HashMap<String, bool>>,
    backpressure_state_by_pipeline: Mutex<HashMap<String, bool>>,
    last_target_error_by_pipeline: Mutex<HashMap<String, String>>,
    settings: FloeReplicationConfig,
}

fn current_unix_time_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

mod buffer;
mod buffer_cleanup;
mod config;
mod dead_letter;
mod delivery;
mod encoding;
mod perf;
mod plan_helpers;
mod replay;
mod runtime_state;
mod status;
mod target_state;
mod writers;

mod reconciliation;
mod runtime_admin;
mod runtime_delivery;
mod runtime_status;

#[cfg(test)]
mod tests;

#[cfg(test)]
use buffer::{ReplicationBufferLimitViolation, effective_u64_limit, effective_usize_limit};
use buffer::{
    ReplicationBufferLimits, append_buffer_transaction, buffer_limit_violation,
    effective_replication_buffer_limits, estimated_buffer_payload_bytes,
    log_replication_buffer_backpressure, prepare_replication_buffer_append,
    record_buffer_cap_utilization, record_buffer_stats,
};
use config::{
    FLOE_HEADER_IDEMPOTENCY_KEY, FLOE_HEADER_PIPELINE, FLOE_HEADER_RECORD_SEQUENCE,
    FLOE_HEADER_SOURCE, FLOE_HEADER_SOURCE_POSITION, FLOE_HEADER_SOURCE_TABLE,
    FLOE_HEADER_TRANSACTION_ID, FLOE_JSON_DELETED_FIELD, FLOE_JSON_PARALLEL_RECORD_THRESHOLD,
    FLOE_JSON_VERSION, FLOE_JSON_VERSION_FIELD, REPLICATION_KAFKA_MESSAGE_TIMEOUT_MS,
    REPLICATION_KAFKA_METADATA_WARMUP_TIMEOUT, REPLICATION_KAFKA_RETRY_ATTEMPTS,
    REPLICATION_KAFKA_RETRY_BASE_MS, REPLICATION_KAFKA_SEND_TIMEOUT,
};
use dead_letter::persist_dead_letter_records;
use perf::{
    log_replication_buffer_append_perf, log_replication_direct_delivery_perf,
    log_replication_kafka_send_perf, log_replication_pipeline_perf,
};
pub(super) use plan_helpers::{
    materialized_transaction, pipeline_checkpoint_from_transaction, replication_pipeline_table_id,
};
use plan_helpers::{
    ordered_replication_plans_for_transaction, replication_pipeline_targets_are_distinct,
};
use status::{
    ReplicationPipelineStatusSnapshot, cdc_replication_debug_state_from_snapshots,
    enrich_pipeline_checkpoint_lag, postgres_position_lsn_bytes,
};
use target_state::{
    direct_dead_lettered_target_state, direct_delivered_target_state, pending_target_state,
    replication_pipeline_uses_dlq, target_kind,
};
