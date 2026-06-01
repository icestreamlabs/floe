use std::collections::{BTreeMap, HashMap};

use floe_cdc_core::{CdcSourcePosition, CdcTransactionId};
use floe_cdc_pg::PostgresLsn;

use crate::http_ingest;

use super::{current_unix_time_ms, encoding};

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::node_runtime) struct ReplicationPipelineStatusSnapshot {
    pub(super) pipeline_name: String,
    pub(super) source_name: String,
    pub(super) schema_evolution_policy: String,
    pub(super) error_policy: String,
    pub(super) target_kind: String,
    pub(super) checkpoint_position: Option<CdcSourcePosition>,
    pub(super) checkpoint_lsn_bytes: Option<u64>,
    pub(super) checkpoint_transaction_id: Option<CdcTransactionId>,
    pub(super) target_state: BTreeMap<String, String>,
    pub(super) pending_transactions: usize,
    pub(super) pending_objects: usize,
    pub(super) pending_records: usize,
    pub(super) pending_bytes: usize,
    pub(super) oldest_pending_age_ms: Option<u64>,
    pub(super) dlq_pending_entries: usize,
    pub(super) dlq_replayed_entries: usize,
    pub(super) dlq_discarded_entries: usize,
    pub(super) oldest_dlq_pending_age_ms: Option<u64>,
    pub(super) missing_payload_objects: usize,
    pub(super) orphan_payload_objects: usize,
    pub(super) orphan_payload_bytes: usize,
    pub(super) replaying: bool,
    pub(super) source_backpressure_active: bool,
    pub(super) last_error: Option<String>,
}

#[allow(dead_code)]
impl ReplicationPipelineStatusSnapshot {
    pub(super) fn pipeline_name(&self) -> &str {
        &self.pipeline_name
    }

    pub(super) fn source_name(&self) -> &str {
        &self.source_name
    }

    pub(super) fn schema_evolution_policy(&self) -> &str {
        &self.schema_evolution_policy
    }

    pub(super) fn error_policy(&self) -> &str {
        &self.error_policy
    }

    pub(super) fn target_kind(&self) -> &str {
        &self.target_kind
    }

    pub(super) fn checkpoint_position(&self) -> Option<&CdcSourcePosition> {
        self.checkpoint_position.as_ref()
    }

    pub(super) fn checkpoint_lsn_bytes(&self) -> Option<u64> {
        self.checkpoint_lsn_bytes
    }

    pub(super) fn checkpoint_transaction_id(&self) -> Option<&CdcTransactionId> {
        self.checkpoint_transaction_id.as_ref()
    }

    pub(super) fn target_state(&self) -> &BTreeMap<String, String> {
        &self.target_state
    }

    pub(super) fn pending_transactions(&self) -> usize {
        self.pending_transactions
    }

    pub(super) fn pending_objects(&self) -> usize {
        self.pending_objects
    }

    pub(super) fn pending_records(&self) -> usize {
        self.pending_records
    }

    pub(super) fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    pub(super) fn oldest_pending_age_ms(&self) -> Option<u64> {
        self.oldest_pending_age_ms
    }

    pub(super) fn dlq_pending_entries(&self) -> usize {
        self.dlq_pending_entries
    }

    pub(super) fn dlq_replayed_entries(&self) -> usize {
        self.dlq_replayed_entries
    }

    pub(super) fn dlq_discarded_entries(&self) -> usize {
        self.dlq_discarded_entries
    }

    pub(super) fn oldest_dlq_pending_age_ms(&self) -> Option<u64> {
        self.oldest_dlq_pending_age_ms
    }

    pub(super) fn missing_payload_objects(&self) -> usize {
        self.missing_payload_objects
    }

    pub(super) fn orphan_payload_objects(&self) -> usize {
        self.orphan_payload_objects
    }

    pub(super) fn orphan_payload_bytes(&self) -> usize {
        self.orphan_payload_bytes
    }

    pub(super) fn replaying(&self) -> bool {
        self.replaying
    }

    pub(super) fn source_backpressure_active(&self) -> bool {
        self.source_backpressure_active
    }

    pub(super) fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}

pub(super) fn cdc_replication_debug_state_from_snapshots(
    snapshots: Vec<ReplicationPipelineStatusSnapshot>,
) -> http_ingest::CdcReplicationDebugState {
    http_ingest::CdcReplicationDebugState {
        updated_at_unix_ms: current_unix_time_ms(),
        refresh_error: None,
        postgres_sources: Vec::new(),
        pipelines: snapshots
            .into_iter()
            .map(|snapshot| http_ingest::CdcReplicationDebugPipelineState {
                pipeline: snapshot.pipeline_name().to_string(),
                source: snapshot.source_name().to_string(),
                schema_evolution_policy: snapshot.schema_evolution_policy().to_string(),
                error_policy: snapshot.error_policy().to_string(),
                target_kind: snapshot.target_kind().to_string(),
                checkpoint_position: snapshot
                    .checkpoint_position()
                    .map(encoding::source_position_key),
                checkpoint_lsn_bytes: snapshot.checkpoint_lsn_bytes(),
                checkpoint_lag_bytes: None,
                checkpoint_transaction_id: snapshot
                    .checkpoint_transaction_id()
                    .map(|transaction_id| transaction_id.as_str().to_string()),
                target_state: snapshot.target_state().clone(),
                pending_transactions: snapshot.pending_transactions(),
                pending_objects: snapshot.pending_objects(),
                pending_records: snapshot.pending_records(),
                pending_bytes: snapshot.pending_bytes(),
                oldest_pending_age_ms: snapshot.oldest_pending_age_ms(),
                dlq_pending_entries: snapshot.dlq_pending_entries(),
                dlq_replayed_entries: snapshot.dlq_replayed_entries(),
                dlq_discarded_entries: snapshot.dlq_discarded_entries(),
                oldest_dlq_pending_age_ms: snapshot.oldest_dlq_pending_age_ms(),
                missing_payload_objects: snapshot.missing_payload_objects(),
                orphan_payload_objects: snapshot.orphan_payload_objects(),
                orphan_payload_bytes: snapshot.orphan_payload_bytes(),
                replaying: snapshot.replaying(),
                source_backpressure_active: snapshot.source_backpressure_active(),
                last_error: snapshot.last_error().map(str::to_string),
            })
            .collect(),
    }
}

pub(super) fn postgres_position_lsn_bytes(position: &CdcSourcePosition) -> Option<u64> {
    let CdcSourcePosition::Postgres { commit_lsn, .. } = position else {
        return None;
    };
    PostgresLsn::parse(commit_lsn).ok().map(|lsn| lsn.as_u64())
}

pub(super) fn enrich_pipeline_checkpoint_lag(state: &mut http_ingest::CdcReplicationDebugState) {
    let upstream_lsn_by_source = state
        .postgres_sources
        .iter()
        .filter_map(|source| {
            source
                .upstream_lsn_bytes
                .map(|upstream_lsn| (source.source.as_str(), upstream_lsn))
        })
        .collect::<HashMap<_, _>>();
    for pipeline in &mut state.pipelines {
        pipeline.checkpoint_lag_bytes = pipeline.checkpoint_lsn_bytes.and_then(|checkpoint_lsn| {
            upstream_lsn_by_source
                .get(pipeline.source.as_str())
                .map(|upstream_lsn| upstream_lsn.saturating_sub(checkpoint_lsn))
        });
    }
}
