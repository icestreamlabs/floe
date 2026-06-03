use std::fmt;

use anyhow::Context;
use floe_cdc_core::TransactionBatch;
use floe_config::ReplicationBufferLimitsConfig;
use floe_storage::{
    CdcBufferAppend, CdcBufferRecord, CdcBufferStats, CdcBufferStore,
    CdcBufferedTransactionManifest,
};

use super::super::ReplicationPipelineRuntimePlan;
use super::current_unix_time_ms;
use super::target_state::target_kind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReplicationBufferLimits {
    pub(super) max_pending_bytes: Option<usize>,
    pub(super) max_pending_records: Option<usize>,
    pub(super) max_pending_transactions: Option<usize>,
    pub(super) max_pending_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReplicationBufferLimitViolation {
    Bytes {
        pending_bytes: usize,
        incoming_bytes: usize,
        max_pending_bytes: usize,
    },
    Records {
        pending_records: usize,
        incoming_records: usize,
        max_pending_records: usize,
    },
    Objects {
        pending_transactions: usize,
        incoming_transactions: usize,
        max_pending_transactions: usize,
    },
    Age {
        oldest_pending_age_ms: u64,
        max_pending_age_ms: u64,
    },
}

pub(super) struct PreparedReplicationBufferAppend {
    pub(super) append: CdcBufferAppend,
    pub(super) target_records: Option<Vec<CdcBufferRecord>>,
}

impl PreparedReplicationBufferAppend {
    pub(super) fn target_records(&self) -> &[CdcBufferRecord] {
        self.target_records
            .as_deref()
            .unwrap_or_else(|| self.append.records())
    }
}

impl ReplicationBufferLimits {
    pub(super) fn from_config(config: ReplicationBufferLimitsConfig) -> Self {
        Self {
            max_pending_bytes: nonzero_usize_limit(config.max_pending_bytes),
            max_pending_records: nonzero_usize_limit(config.max_pending_records),
            max_pending_transactions: nonzero_usize_limit(config.max_pending_transactions),
            max_pending_age_ms: nonzero_u64_limit(config.max_pending_age_ms),
        }
    }

    pub(super) fn enabled(self) -> bool {
        self.max_pending_bytes.is_some()
            || self.max_pending_records.is_some()
            || self.max_pending_transactions.is_some()
            || self.max_pending_age_ms.is_some()
    }
}

impl fmt::Display for ReplicationBufferLimitViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes {
                pending_bytes,
                incoming_bytes,
                max_pending_bytes,
            } => write!(
                f,
                "pending buffer bytes would be {} with incoming {} bytes, above max {} bytes",
                pending_bytes.saturating_add(*incoming_bytes),
                incoming_bytes,
                max_pending_bytes
            ),
            Self::Records {
                pending_records,
                incoming_records,
                max_pending_records,
            } => write!(
                f,
                "pending buffer records would be {} with incoming {} records, above max {} records",
                pending_records.saturating_add(*incoming_records),
                incoming_records,
                max_pending_records
            ),
            Self::Objects {
                pending_transactions,
                incoming_transactions,
                max_pending_transactions,
            } => write!(
                f,
                "pending buffer objects would be {} with incoming {} object, above max {} objects",
                pending_transactions.saturating_add(*incoming_transactions),
                incoming_transactions,
                max_pending_transactions
            ),
            Self::Age {
                oldest_pending_age_ms,
                max_pending_age_ms,
            } => write!(
                f,
                "oldest pending transaction age is {oldest_pending_age_ms} ms, above max {max_pending_age_ms} ms"
            ),
        }
    }
}

pub(super) async fn append_buffer_transaction(
    buffer_store: &CdcBufferStore,
    append: &CdcBufferAppend,
    await_durable: bool,
) -> anyhow::Result<CdcBufferedTransactionManifest> {
    let manifest = if await_durable {
        buffer_store.append_transaction(append).await
    } else {
        buffer_store
            .append_transaction_without_durable_wait(append)
            .await
    }?;
    crate::metrics::inc_cdc_buffer_object_op(append.pipeline_name(), "create", 1);
    Ok(manifest)
}

pub(super) fn prepare_replication_buffer_append(
    plan: &ReplicationPipelineRuntimePlan,
    transaction: &TransactionBatch,
    target_records: Vec<CdcBufferRecord>,
) -> anyhow::Result<PreparedReplicationBufferAppend> {
    let buffered_at_unix_ms = current_unix_time_ms();
    Ok(PreparedReplicationBufferAppend {
        append: CdcBufferAppend::new(
            &plan.name,
            &plan.source_name,
            plan.table_id.as_str(),
            transaction.commit_position().clone(),
            transaction.transaction_id().cloned(),
            target_records,
            buffered_at_unix_ms,
        )?
        .with_schema_versions(transaction.schema_versions().clone()),
        target_records: None,
    })
}

pub(super) async fn record_buffer_stats(
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

pub(super) fn record_buffer_cap_utilization(
    pipeline_name: &str,
    stats: &CdcBufferStats,
    limits: ReplicationBufferLimits,
) {
    if let Some(max_pending_bytes) = limits.max_pending_bytes {
        crate::metrics::record_cdc_buffer_cap_utilization(
            pipeline_name,
            "pending_bytes",
            stats.pending_bytes(),
            max_pending_bytes,
        );
    }
    if let Some(max_pending_records) = limits.max_pending_records {
        crate::metrics::record_cdc_buffer_cap_utilization(
            pipeline_name,
            "pending_records",
            stats.pending_records(),
            max_pending_records,
        );
    }
    if let Some(max_pending_transactions) = limits.max_pending_transactions {
        crate::metrics::record_cdc_buffer_cap_utilization(
            pipeline_name,
            "pending_objects",
            stats.pending_objects(),
            max_pending_transactions,
        );
    }
    if let Some(max_pending_age_ms) = limits.max_pending_age_ms {
        crate::metrics::record_cdc_buffer_cap_utilization_u64(
            pipeline_name,
            "pending_age",
            stats.oldest_pending_age_ms().unwrap_or(0),
            max_pending_age_ms,
        );
    }
}

pub(super) fn effective_replication_buffer_limits(
    plan: &ReplicationPipelineRuntimePlan,
    defaults: ReplicationBufferLimits,
) -> ReplicationBufferLimits {
    ReplicationBufferLimits {
        max_pending_bytes: effective_usize_limit(
            plan.buffer_policy.max_pending_bytes(),
            defaults.max_pending_bytes,
        ),
        max_pending_records: effective_usize_limit(
            plan.buffer_policy.max_pending_records(),
            defaults.max_pending_records,
        ),
        max_pending_transactions: effective_usize_limit(
            plan.buffer_policy.max_pending_transactions(),
            defaults.max_pending_transactions,
        ),
        max_pending_age_ms: effective_u64_limit(
            plan.buffer_policy.max_pending_age_ms(),
            defaults.max_pending_age_ms,
        ),
    }
}

fn nonzero_usize_limit(value: usize) -> Option<usize> {
    (value > 0).then_some(value)
}

fn nonzero_u64_limit(value: u64) -> Option<u64> {
    (value > 0).then_some(value)
}

pub(super) fn effective_usize_limit(
    override_value: Option<usize>,
    default_value: Option<usize>,
) -> Option<usize> {
    match override_value {
        Some(0) => None,
        Some(value) => Some(value),
        None => default_value,
    }
}

pub(super) fn effective_u64_limit(
    override_value: Option<u64>,
    default_value: Option<u64>,
) -> Option<u64> {
    match override_value {
        Some(0) => None,
        Some(value) => Some(value),
        None => default_value,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn log_replication_buffer_backpressure(
    plan: &ReplicationPipelineRuntimePlan,
    phase: &str,
    stats: Option<&CdcBufferStats>,
    incoming_bytes: usize,
    incoming_records: usize,
    limits: ReplicationBufferLimits,
    violation: ReplicationBufferLimitViolation,
    delivered_records: Option<usize>,
) {
    tracing::warn!(
        pipeline = %plan.name,
        source = %plan.source_name,
        target_kind = target_kind(plan),
        phase,
        violation_kind = buffer_limit_violation_kind(violation),
        violation = %violation,
        pending_transactions = stats.map(CdcBufferStats::pending_transactions).unwrap_or(0),
        pending_records = stats.map(CdcBufferStats::pending_records).unwrap_or(0),
        pending_bytes = stats.map(CdcBufferStats::pending_bytes).unwrap_or(0),
        oldest_pending_age_ms = stats.and_then(CdcBufferStats::oldest_pending_age_ms),
        incoming_bytes,
        incoming_records,
        delivered_records,
        max_pending_bytes = limits.max_pending_bytes,
        max_pending_records = limits.max_pending_records,
        max_pending_transactions = limits.max_pending_transactions,
        max_pending_age_ms = limits.max_pending_age_ms,
        "replication pipeline durable buffer guardrail applying CDC source backpressure"
    );
}

fn buffer_limit_violation_kind(violation: ReplicationBufferLimitViolation) -> &'static str {
    match violation {
        ReplicationBufferLimitViolation::Bytes { .. } => "pending_bytes",
        ReplicationBufferLimitViolation::Records { .. } => "pending_records",
        ReplicationBufferLimitViolation::Objects { .. } => "pending_objects",
        ReplicationBufferLimitViolation::Age { .. } => "pending_age",
    }
}

pub(super) fn estimated_buffer_payload_bytes(records: &[CdcBufferRecord]) -> usize {
    records.iter().fold(16usize, |bytes, record| {
        bytes
            .saturating_add(24)
            .saturating_add(record.byte_len())
            .saturating_add(record.headers().len().saturating_mul(16))
    })
}

pub(super) fn buffer_limit_violation(
    pending_transactions: usize,
    pending_records: usize,
    pending_bytes: usize,
    oldest_pending_age_ms: Option<u64>,
    incoming_bytes: usize,
    incoming_records: usize,
    limits: ReplicationBufferLimits,
) -> Option<ReplicationBufferLimitViolation> {
    if let Some(max_pending_bytes) = limits.max_pending_bytes
        && pending_bytes.saturating_add(incoming_bytes) > max_pending_bytes
    {
        return Some(ReplicationBufferLimitViolation::Bytes {
            pending_bytes,
            incoming_bytes,
            max_pending_bytes,
        });
    }
    if let Some(max_pending_records) = limits.max_pending_records
        && pending_records.saturating_add(incoming_records) > max_pending_records
    {
        return Some(ReplicationBufferLimitViolation::Records {
            pending_records,
            incoming_records,
            max_pending_records,
        });
    }
    if let Some(max_pending_transactions) = limits.max_pending_transactions
        && pending_transactions.saturating_add(1) > max_pending_transactions
    {
        return Some(ReplicationBufferLimitViolation::Objects {
            pending_transactions,
            incoming_transactions: 1,
            max_pending_transactions,
        });
    }
    if let Some(max_pending_age_ms) = limits.max_pending_age_ms
        && let Some(oldest_pending_age_ms) = oldest_pending_age_ms
        && oldest_pending_age_ms > max_pending_age_ms
    {
        return Some(ReplicationBufferLimitViolation::Age {
            oldest_pending_age_ms,
            max_pending_age_ms,
        });
    }
    None
}
