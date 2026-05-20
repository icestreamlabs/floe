use std::collections::HashMap;

use anyhow::Result;
use floe_cdc::{CdcApplyResult, CdcTableStore};
use floe_cdc_core::{CdcSourceId, CdcTableId, CdcTableSchema};

use crate::{PostgresLsn, PostgresReplicationEvent};

use super::assembler::PostgresTransactionAssembler;
use super::router::PostgresTableRouter;
use super::schema_evolution::PostgresSchemaEvolutionPolicy;

pub struct PostgresCdcEventApplier {
    source_id: CdcSourceId,
    table_store: CdcTableStore,
    schemas: HashMap<CdcTableId, CdcTableSchema>,
    assembler: PostgresTransactionAssembler,
    upstream_wal_end: Option<PostgresLsn>,
    durable_lsn: Option<PostgresLsn>,
    table_applied_lsns: HashMap<CdcTableId, PostgresLsn>,
}

impl PostgresCdcEventApplier {
    pub fn new(
        source_id: CdcSourceId,
        table_store: CdcTableStore,
        schemas: HashMap<CdcTableId, CdcTableSchema>,
    ) -> Self {
        let router = PostgresTableRouter::from_schemas(schemas.values());
        Self::with_router_and_schema_policy(
            source_id,
            table_store,
            schemas,
            router,
            PostgresSchemaEvolutionPolicy::FailFast,
        )
    }

    pub fn with_schema_policy(
        source_id: CdcSourceId,
        table_store: CdcTableStore,
        schemas: HashMap<CdcTableId, CdcTableSchema>,
        schema_policy: PostgresSchemaEvolutionPolicy,
    ) -> Self {
        let router = PostgresTableRouter::from_schemas(schemas.values());
        Self::with_router_and_schema_policy(source_id, table_store, schemas, router, schema_policy)
    }

    pub fn with_router(
        source_id: CdcSourceId,
        table_store: CdcTableStore,
        schemas: HashMap<CdcTableId, CdcTableSchema>,
        router: PostgresTableRouter,
    ) -> Self {
        Self::with_router_and_schema_policy(
            source_id,
            table_store,
            schemas,
            router,
            PostgresSchemaEvolutionPolicy::FailFast,
        )
    }

    pub fn with_router_and_schema_policy(
        source_id: CdcSourceId,
        table_store: CdcTableStore,
        schemas: HashMap<CdcTableId, CdcTableSchema>,
        router: PostgresTableRouter,
        schema_policy: PostgresSchemaEvolutionPolicy,
    ) -> Self {
        Self {
            source_id: source_id.clone(),
            table_store,
            schemas: schemas.clone(),
            assembler: PostgresTransactionAssembler::with_schemas(
                source_id,
                router,
                schemas,
                schema_policy,
            ),
            upstream_wal_end: None,
            durable_lsn: None,
            table_applied_lsns: HashMap::new(),
        }
    }

    pub async fn accept_event(
        &mut self,
        event: PostgresReplicationEvent,
    ) -> Result<PostgresCdcApplyOutcome> {
        self.observe_event_frontier(&event);
        let Some(transaction) = self.assembler.accept_event(event)? else {
            return Ok(PostgresCdcApplyOutcome::idle(self.lag_snapshot()));
        };
        let commit_lsn = PostgresLsn::from_source_position(transaction.commit_position())?;
        let changed_table_ids = transaction
            .change_batches()
            .iter()
            .map(|batch| batch.table_id().clone())
            .collect::<Vec<_>>();
        let apply_result = self
            .table_store
            .apply_transaction(&self.schemas, &transaction)
            .await?;
        let feedback_lsn = PostgresLsn::from_source_position(apply_result.checkpoint().position())?;
        self.record_committed_apply(
            feedback_lsn,
            commit_lsn,
            apply_result.already_committed(),
            &changed_table_ids,
        );
        Ok(PostgresCdcApplyOutcome::applied(
            apply_result,
            feedback_lsn,
            self.lag_snapshot(),
        ))
    }

    pub fn table_store(&self) -> &CdcTableStore {
        &self.table_store
    }

    pub fn schemas(&self) -> &HashMap<CdcTableId, CdcTableSchema> {
        &self.schemas
    }

    pub fn lag_snapshot(&self) -> PostgresCdcLagSnapshot {
        let mut table_ids = self.schemas.keys().collect::<Vec<_>>();
        table_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let table_lags = table_ids
            .into_iter()
            .map(|table_id| {
                let last_applied_lsn = self.table_applied_lsns.get(table_id).copied();
                PostgresCdcTableLag::new(table_id.clone(), last_applied_lsn, self.upstream_wal_end)
            })
            .collect();

        PostgresCdcLagSnapshot::new(
            self.source_id.clone(),
            self.upstream_wal_end,
            self.durable_lsn,
            table_lags,
        )
    }

    pub fn reset_stream_state(&mut self) {
        self.assembler.reset_stream_state();
    }

    fn observe_event_frontier(&mut self, event: &PostgresReplicationEvent) {
        let event_lsn = match event {
            PostgresReplicationEvent::KeepAlive { wal_end, .. } => Some(*wal_end),
            PostgresReplicationEvent::Begin { final_lsn, .. } => Some(*final_lsn),
            PostgresReplicationEvent::XLogData { wal_end, .. } => Some(*wal_end),
            PostgresReplicationEvent::Commit { end_lsn, .. } => Some(*end_lsn),
            PostgresReplicationEvent::Message { lsn, .. } => Some(*lsn),
            PostgresReplicationEvent::StoppedAt { reached } => Some(*reached),
        };
        if let Some(event_lsn) = event_lsn {
            record_max_lsn(&mut self.upstream_wal_end, event_lsn);
        }
    }

    fn record_committed_apply(
        &mut self,
        feedback_lsn: PostgresLsn,
        transaction_commit_lsn: PostgresLsn,
        already_committed: bool,
        changed_table_ids: &[CdcTableId],
    ) {
        record_max_lsn(&mut self.durable_lsn, feedback_lsn);
        if already_committed && transaction_commit_lsn != feedback_lsn {
            return;
        }

        for table_id in changed_table_ids {
            let table_lsn = self
                .table_applied_lsns
                .entry(table_id.clone())
                .or_insert(feedback_lsn);
            if *table_lsn < feedback_lsn {
                *table_lsn = feedback_lsn;
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresCdcApplyOutcome {
    apply_result: Option<CdcApplyResult>,
    feedback_lsn: Option<PostgresLsn>,
    lag_snapshot: PostgresCdcLagSnapshot,
}

impl PostgresCdcApplyOutcome {
    pub fn idle(lag_snapshot: PostgresCdcLagSnapshot) -> Self {
        Self {
            apply_result: None,
            feedback_lsn: None,
            lag_snapshot,
        }
    }

    pub fn applied(
        apply_result: CdcApplyResult,
        feedback_lsn: PostgresLsn,
        lag_snapshot: PostgresCdcLagSnapshot,
    ) -> Self {
        Self {
            apply_result: Some(apply_result),
            feedback_lsn: Some(feedback_lsn),
            lag_snapshot,
        }
    }

    pub fn apply_result(&self) -> Option<&CdcApplyResult> {
        self.apply_result.as_ref()
    }

    pub fn feedback_lsn(&self) -> Option<PostgresLsn> {
        self.feedback_lsn
    }

    pub fn lag_snapshot(&self) -> &PostgresCdcLagSnapshot {
        &self.lag_snapshot
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresCdcLagSnapshot {
    source_id: CdcSourceId,
    upstream_wal_end: Option<PostgresLsn>,
    durable_lsn: Option<PostgresLsn>,
    source_lag_bytes: Option<u64>,
    table_lags: Vec<PostgresCdcTableLag>,
}

impl PostgresCdcLagSnapshot {
    fn new(
        source_id: CdcSourceId,
        upstream_wal_end: Option<PostgresLsn>,
        durable_lsn: Option<PostgresLsn>,
        table_lags: Vec<PostgresCdcTableLag>,
    ) -> Self {
        Self {
            source_id,
            upstream_wal_end,
            durable_lsn,
            source_lag_bytes: lsn_lag_bytes(upstream_wal_end, durable_lsn),
            table_lags,
        }
    }

    pub fn source_id(&self) -> &CdcSourceId {
        &self.source_id
    }

    pub fn upstream_wal_end(&self) -> Option<PostgresLsn> {
        self.upstream_wal_end
    }

    pub fn durable_lsn(&self) -> Option<PostgresLsn> {
        self.durable_lsn
    }

    pub fn source_lag_bytes(&self) -> Option<u64> {
        self.source_lag_bytes
    }

    pub fn table_lags(&self) -> &[PostgresCdcTableLag] {
        &self.table_lags
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresCdcTableLag {
    table_id: CdcTableId,
    last_applied_lsn: Option<PostgresLsn>,
    table_lag_bytes: Option<u64>,
}

impl PostgresCdcTableLag {
    fn new(
        table_id: CdcTableId,
        last_applied_lsn: Option<PostgresLsn>,
        upstream_wal_end: Option<PostgresLsn>,
    ) -> Self {
        Self {
            table_id,
            last_applied_lsn,
            table_lag_bytes: lsn_lag_bytes(upstream_wal_end, last_applied_lsn),
        }
    }

    pub fn table_id(&self) -> &CdcTableId {
        &self.table_id
    }

    pub fn last_applied_lsn(&self) -> Option<PostgresLsn> {
        self.last_applied_lsn
    }

    pub fn table_lag_bytes(&self) -> Option<u64> {
        self.table_lag_bytes
    }
}

fn record_max_lsn(slot: &mut Option<PostgresLsn>, lsn: PostgresLsn) {
    if slot.is_none_or(|current| lsn > current) {
        *slot = Some(lsn);
    }
}

fn lsn_lag_bytes(upstream: Option<PostgresLsn>, durable: Option<PostgresLsn>) -> Option<u64> {
    Some(upstream?.as_u64().saturating_sub(durable?.as_u64()))
}
