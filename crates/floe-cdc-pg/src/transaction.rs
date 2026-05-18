use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use floe_cdc::{CdcApplyResult, CdcTableStore};
use floe_cdc_core::{
    CdcChange, CdcRow, CdcSchemaVersionMap, CdcSourceId, CdcSourcePosition, CdcTableId,
    CdcTableSchema, CdcTransactionId, ChangeBatch, TransactionBatch, UpstreamTableRef,
};

use crate::{
    PgOutputCdcChange, PgOutputDecoder, PostgresCdcConfig, PostgresLsn, PostgresReplicationClient,
    PostgresReplicationEvent, config_with_stored_cdc_checkpoint,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresSchemaEvolutionPolicy {
    FailFast,
    IgnoreCompatible,
    ApplyCompatibleAdditions,
}

impl PostgresSchemaEvolutionPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FailFast => "fail_fast",
            Self::IgnoreCompatible => "ignore_compatible",
            Self::ApplyCompatibleAdditions => "apply_compatible_additions",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PostgresTableRouter {
    by_upstream_table: HashMap<UpstreamTableRef, CdcTableId>,
}

impl PostgresTableRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, upstream_table: UpstreamTableRef, table_id: CdcTableId) {
        self.by_upstream_table.insert(upstream_table, table_id);
    }

    pub fn get(&self, upstream_table: &UpstreamTableRef) -> Option<&CdcTableId> {
        self.by_upstream_table.get(upstream_table)
    }

    pub fn from_schemas<'a>(schemas: impl IntoIterator<Item = &'a CdcTableSchema>) -> Self {
        let mut router = Self::new();
        for schema in schemas {
            router.insert(schema.upstream_table().clone(), schema.table_id().clone());
        }
        router
    }
}

pub struct PostgresTransactionAssembler {
    source_id: CdcSourceId,
    router: PostgresTableRouter,
    decoder: PgOutputDecoder,
    current: Option<InFlightTransaction>,
    schemas: HashMap<CdcTableId, CdcTableSchema>,
    schema_policy: PostgresSchemaEvolutionPolicy,
    schema_versions: CdcSchemaVersionMap,
}

impl PostgresTransactionAssembler {
    pub fn new(source_id: CdcSourceId, router: PostgresTableRouter) -> Self {
        Self {
            source_id,
            router,
            decoder: PgOutputDecoder::new(),
            current: None,
            schemas: HashMap::new(),
            schema_policy: PostgresSchemaEvolutionPolicy::FailFast,
            schema_versions: CdcSchemaVersionMap::new(),
        }
    }

    pub fn with_schemas(
        source_id: CdcSourceId,
        router: PostgresTableRouter,
        schemas: HashMap<CdcTableId, CdcTableSchema>,
        schema_policy: PostgresSchemaEvolutionPolicy,
    ) -> Self {
        let schema_versions = schema_versions_for_schemas(&schemas);
        Self {
            source_id,
            router,
            decoder: PgOutputDecoder::new(),
            current: None,
            schemas,
            schema_policy,
            schema_versions,
        }
    }

    pub fn accept_event(
        &mut self,
        event: PostgresReplicationEvent,
    ) -> Result<Option<TransactionBatch>> {
        match event {
            PostgresReplicationEvent::Begin { xid, .. } => {
                self.begin(xid)?;
                Ok(None)
            }
            PostgresReplicationEvent::XLogData { data, .. } => {
                self.accept_xlog_data(data)?;
                Ok(None)
            }
            PostgresReplicationEvent::Commit { end_lsn, .. } => self.commit(end_lsn),
            PostgresReplicationEvent::KeepAlive { .. }
            | PostgresReplicationEvent::Message { .. }
            | PostgresReplicationEvent::StoppedAt { .. } => Ok(None),
        }
    }

    pub fn decoder(&self) -> &PgOutputDecoder {
        &self.decoder
    }

    pub fn reset_stream_state(&mut self) {
        self.decoder = PgOutputDecoder::new();
        self.current = None;
    }

    fn begin(&mut self, xid: u32) -> Result<()> {
        if self.current.is_some() {
            bail!("Postgres CDC transaction began before previous transaction committed");
        }
        self.current = Some(InFlightTransaction::new(xid)?);
        Ok(())
    }

    fn accept_xlog_data(&mut self, data: bytes::Bytes) -> Result<()> {
        let decoded = self.decoder.decode_cdc_changes_with_metadata(data)?;
        if let Some(relation) = decoded.relation() {
            self.accept_relation(relation)?;
        }
        let routed_changes = decoded
            .changes()
            .iter()
            .map(|change| self.route_change(change))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        if routed_changes.is_empty() {
            return Ok(());
        }
        let current = self
            .current
            .as_mut()
            .ok_or_else(|| anyhow!("Postgres CDC change arrived outside a transaction boundary"))?;
        for (table_id, change) in routed_changes {
            current.push(
                table_id.clone(),
                change,
                self.schema_versions
                    .get(table_id.as_str())
                    .copied()
                    .unwrap_or_default(),
            );
        }
        Ok(())
    }

    fn accept_relation(&mut self, relation: &crate::PgOutputRelation) -> Result<()> {
        let upstream_table = relation.upstream_table_ref()?;
        let Some(table_id) = self.router.get(&upstream_table).cloned() else {
            return Ok(());
        };
        let Some(catalog_schema) = self.schemas.get(&table_id) else {
            return Ok(());
        };
        let observed_schema = relation.to_cdc_schema(table_id.clone())?;
        match classify_schema_evolution(catalog_schema, &observed_schema) {
            SchemaEvolution::Unchanged => {
                self.schema_versions.insert(
                    table_id.as_str().to_string(),
                    catalog_schema.stable_fingerprint(),
                );
                Ok(())
            }
            SchemaEvolution::CompatibleAddition { added_columns } => match self.schema_policy {
                PostgresSchemaEvolutionPolicy::FailFast => {
                    tracing::warn!(
                        source = %self.source_id.as_str(),
                        table = %table_id.as_str(),
                        upstream_table = %format!("{}.{}", upstream_table.schema(), upstream_table.table()),
                        policy = ?self.schema_policy,
                        added_column_count = added_columns.len(),
                        added_columns = ?added_columns,
                        catalog_schema_version = catalog_schema.stable_fingerprint(),
                        observed_schema_version = observed_schema.stable_fingerprint(),
                        "Postgres CDC relation schema has compatible additions but fail-fast policy rejects schema evolution"
                    );
                    bail!(
                        "Postgres CDC schema for table '{}' has compatible column additions but policy is fail-fast",
                        table_id.as_str()
                    )
                }
                PostgresSchemaEvolutionPolicy::IgnoreCompatible
                | PostgresSchemaEvolutionPolicy::ApplyCompatibleAdditions => {
                    tracing::info!(
                        source = %self.source_id.as_str(),
                        table = %table_id.as_str(),
                        upstream_table = %format!("{}.{}", upstream_table.schema(), upstream_table.table()),
                        policy = ?self.schema_policy,
                        added_column_count = added_columns.len(),
                        added_columns = ?added_columns,
                        catalog_schema_version = catalog_schema.stable_fingerprint(),
                        observed_schema_version = observed_schema.stable_fingerprint(),
                        "Postgres CDC relation schema has compatible additions; projecting to catalog schema"
                    );
                    self.schema_versions.insert(
                        table_id.as_str().to_string(),
                        observed_schema.stable_fingerprint(),
                    );
                    Ok(())
                }
            },
            SchemaEvolution::Incompatible(reason) => {
                tracing::warn!(
                    source = %self.source_id.as_str(),
                    table = %table_id.as_str(),
                    upstream_table = %format!("{}.{}", upstream_table.schema(), upstream_table.table()),
                    policy = ?self.schema_policy,
                    reason = %reason,
                    catalog_column_count = catalog_schema.columns().len(),
                    observed_column_count = observed_schema.columns().len(),
                    catalog_schema_version = catalog_schema.stable_fingerprint(),
                    observed_schema_version = observed_schema.stable_fingerprint(),
                    "Postgres CDC relation schema is incompatible with catalog schema"
                );
                bail!(
                    "Postgres CDC schema for table '{}' is incompatible with catalog schema: {reason}",
                    table_id.as_str()
                )
            }
        }
    }

    fn route_change(&self, change: &PgOutputCdcChange) -> Result<Option<(CdcTableId, CdcChange)>> {
        let upstream_table = change.relation().upstream_table_ref()?;
        let Some(table_id) = self.router.get(&upstream_table).cloned() else {
            return Ok(None);
        };
        let change = if let Some(schema) = self.schemas.get(&table_id) {
            project_change_to_schema(change, schema)?
        } else {
            change.change().clone()
        };
        Ok(Some((table_id, change)))
    }

    fn commit(&mut self, end_lsn: PostgresLsn) -> Result<Option<TransactionBatch>> {
        let current = self
            .current
            .take()
            .ok_or_else(|| anyhow!("Postgres CDC commit arrived without a begin boundary"))?;
        if current.table_changes.is_empty() {
            return Ok(None);
        }
        let change_batches = current
            .table_changes
            .into_iter()
            .map(|table_changes| ChangeBatch::new(table_changes.table_id, table_changes.changes))
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(
            TransactionBatch::new(
                self.source_id.clone(),
                Some(current.transaction_id),
                None,
                CdcSourcePosition::postgres(end_lsn.to_pg_string(), None)?,
                change_batches,
            )?
            .with_schema_versions(current.schema_versions),
        ))
    }
}

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

#[async_trait]
pub trait PostgresReplicationStream {
    async fn recv_event(&mut self) -> Result<Option<PostgresReplicationEvent>>;
    fn update_applied_lsn(&mut self, lsn: PostgresLsn);
}

#[async_trait]
impl PostgresReplicationStream for PostgresReplicationClient {
    async fn recv_event(&mut self) -> Result<Option<PostgresReplicationEvent>> {
        self.recv().await
    }

    fn update_applied_lsn(&mut self, lsn: PostgresLsn) {
        PostgresReplicationClient::update_applied_lsn(self, lsn);
    }
}

pub async fn run_postgres_cdc_apply_loop<C>(
    client: &mut C,
    applier: &mut PostgresCdcEventApplier,
) -> Result<()>
where
    C: PostgresReplicationStream + Send,
{
    while let Some(event) = client.recv_event().await? {
        let outcome = applier.accept_event(event).await?;
        if let Some(feedback_lsn) = outcome.feedback_lsn() {
            client.update_applied_lsn(feedback_lsn);
        }
    }
    Ok(())
}

#[async_trait]
pub trait PostgresReplicationClientFactory {
    type Stream: PostgresReplicationStream + Send;

    async fn connect(&self, config: &PostgresCdcConfig) -> Result<Self::Stream>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PgWireReplicationClientFactory;

#[async_trait]
impl PostgresReplicationClientFactory for PgWireReplicationClientFactory {
    type Stream = PostgresReplicationClient;

    async fn connect(&self, config: &PostgresCdcConfig) -> Result<Self::Stream> {
        PostgresReplicationClient::connect(config).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostgresCdcReconnectPolicy {
    max_reconnects: usize,
    retry_delay: Duration,
}

impl PostgresCdcReconnectPolicy {
    pub fn new(max_reconnects: usize, retry_delay: Duration) -> Self {
        Self {
            max_reconnects,
            retry_delay,
        }
    }

    pub fn max_reconnects(&self) -> usize {
        self.max_reconnects
    }

    pub fn retry_delay(&self) -> Duration {
        self.retry_delay
    }
}

impl Default for PostgresCdcReconnectPolicy {
    fn default() -> Self {
        Self {
            max_reconnects: 10,
            retry_delay: Duration::from_secs(1),
        }
    }
}

pub async fn run_postgres_cdc_apply_loop_with_reconnect<F>(
    base_config: PostgresCdcConfig,
    source_id: &CdcSourceId,
    table_store: &CdcTableStore,
    applier: &mut PostgresCdcEventApplier,
    factory: &F,
    policy: PostgresCdcReconnectPolicy,
) -> Result<()>
where
    F: PostgresReplicationClientFactory + Sync,
{
    let mut reconnects = 0usize;
    loop {
        let config =
            config_with_stored_cdc_checkpoint(base_config.clone(), table_store, source_id).await?;
        let mut client = factory.connect(&config).await.with_context(|| {
            format!(
                "connect Postgres CDC replication stream from LSN {:?}",
                config.start_lsn()
            )
        })?;

        match run_postgres_cdc_apply_loop(&mut client, applier).await {
            Ok(()) => return Ok(()),
            Err(err) if reconnects < policy.max_reconnects => {
                reconnects += 1;
                applier.reset_stream_state();
                tracing::warn!(
                    error = %err,
                    reconnects,
                    max_reconnects = policy.max_reconnects,
                    retry_delay_ms = policy.retry_delay.as_millis() as u64,
                    start_lsn = ?config.start_lsn(),
                    "Postgres CDC stream failed; reconnecting from durable checkpoint"
                );
                if !policy.retry_delay.is_zero() {
                    tokio::time::sleep(policy.retry_delay).await;
                }
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "Postgres CDC stream failed after {} reconnect attempt(s)",
                        reconnects
                    )
                });
            }
        }
    }
}

enum SchemaEvolution {
    Unchanged,
    CompatibleAddition { added_columns: Vec<String> },
    Incompatible(String),
}

fn classify_schema_evolution(
    catalog_schema: &CdcTableSchema,
    observed_schema: &CdcTableSchema,
) -> SchemaEvolution {
    if catalog_schema.primary_key().columns() != observed_schema.primary_key().columns() {
        return SchemaEvolution::Incompatible(format!(
            "primary key changed from {:?} to {:?}",
            catalog_schema.primary_key().columns(),
            observed_schema.primary_key().columns()
        ));
    }

    if observed_schema.columns().len() < catalog_schema.columns().len() {
        return SchemaEvolution::Incompatible(format!(
            "column count decreased from {} to {}",
            catalog_schema.columns().len(),
            observed_schema.columns().len()
        ));
    }

    for (idx, catalog_column) in catalog_schema.columns().iter().enumerate() {
        let Some(observed_column) = observed_schema.columns().get(idx) else {
            return SchemaEvolution::Incompatible(format!(
                "column '{}' is missing from observed schema",
                catalog_column.name()
            ));
        };
        if catalog_column.name() != observed_column.name() {
            return SchemaEvolution::Incompatible(format!(
                "column {} changed from '{}' to '{}'",
                idx,
                catalog_column.name(),
                observed_column.name()
            ));
        }
        if catalog_column.data_type() != observed_column.data_type() {
            return SchemaEvolution::Incompatible(format!(
                "column '{}' type changed from {:?} to {:?}",
                catalog_column.name(),
                catalog_column.data_type(),
                observed_column.data_type()
            ));
        }
    }

    if observed_schema.columns().len() == catalog_schema.columns().len() {
        SchemaEvolution::Unchanged
    } else {
        SchemaEvolution::CompatibleAddition {
            added_columns: observed_schema.columns()[catalog_schema.columns().len()..]
                .iter()
                .map(|column| column.name().to_string())
                .collect(),
        }
    }
}

fn project_change_to_schema(
    change: &PgOutputCdcChange,
    schema: &CdcTableSchema,
) -> Result<CdcChange> {
    match change.change() {
        CdcChange::Insert { row } => Ok(CdcChange::Insert {
            row: project_row_to_schema(change.relation(), schema, row)?,
        }),
        CdcChange::Update { key, before, after } => Ok(CdcChange::Update {
            key: key.clone(),
            before: before
                .as_ref()
                .map(|row| project_row_to_schema(change.relation(), schema, row))
                .transpose()?,
            after: project_row_to_schema(change.relation(), schema, after)?,
        }),
        CdcChange::Delete { key, before } => Ok(CdcChange::Delete {
            key: key.clone(),
            before: before
                .as_ref()
                .map(|row| project_row_to_schema(change.relation(), schema, row))
                .transpose()?,
        }),
        CdcChange::Truncate => Ok(CdcChange::Truncate),
    }
}

fn project_row_to_schema(
    relation: &crate::PgOutputRelation,
    schema: &CdcTableSchema,
    row: &CdcRow,
) -> Result<CdcRow> {
    if relation.columns().len() == schema.columns().len()
        && relation
            .columns()
            .iter()
            .zip(schema.columns())
            .all(|(relation_column, schema_column)| relation_column.name() == schema_column.name())
    {
        return Ok(row.clone());
    }
    anyhow::ensure!(
        row.values().len() == relation.columns().len(),
        "Postgres CDC row for relation '{}.{}' has {} values but relation metadata has {} columns",
        relation.namespace(),
        relation.name(),
        row.values().len(),
        relation.columns().len()
    );
    let mut values = Vec::with_capacity(schema.columns().len());
    for schema_column in schema.columns() {
        let index = relation
            .columns()
            .iter()
            .position(|relation_column| relation_column.name() == schema_column.name())
            .ok_or_else(|| {
                anyhow!(
                    "Postgres CDC relation '{}.{}' no longer contains catalog column '{}'",
                    relation.namespace(),
                    relation.name(),
                    schema_column.name()
                )
            })?;
        values.push(row.values()[index].clone());
    }
    CdcRow::new(values)
}

fn schema_versions_for_schemas(
    schemas: &HashMap<CdcTableId, CdcTableSchema>,
) -> CdcSchemaVersionMap {
    schemas
        .iter()
        .map(|(table_id, schema)| (table_id.as_str().to_string(), schema.stable_fingerprint()))
        .collect()
}

struct InFlightTransaction {
    transaction_id: CdcTransactionId,
    table_changes: Vec<TableChanges>,
    schema_versions: CdcSchemaVersionMap,
}

impl InFlightTransaction {
    fn new(xid: u32) -> Result<Self> {
        Ok(Self {
            transaction_id: CdcTransactionId::new(format!("pg-xid-{xid}"))?,
            table_changes: Vec::new(),
            schema_versions: CdcSchemaVersionMap::new(),
        })
    }

    fn push(&mut self, table_id: CdcTableId, change: CdcChange, schema_version: u64) {
        self.schema_versions
            .insert(table_id.as_str().to_string(), schema_version);
        if let Some(existing) = self
            .table_changes
            .iter_mut()
            .find(|existing| existing.table_id == table_id)
        {
            existing.changes.push(change);
        } else {
            self.table_changes.push(TableChanges {
                table_id,
                changes: vec![change],
            });
        }
    }
}

struct TableChanges {
    table_id: CdcTableId,
    changes: Vec<CdcChange>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use bytes::Bytes;
    use dbsp_storage::storage::{KeyValueTable, SlateTable};
    use floe_cdc_core::{CdcRow, CdcRowKey, UpstreamTableRef};
    use floe_core::RowValue;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;

    use crate::{PgOutputMessage, decode_pgoutput_message};

    const RELATION_ID: u32 = 42;
    const OTHER_RELATION_ID: u32 = 43;
    const PG_INT8_OID: u32 = 20;
    const PG_TEXT_OID: u32 = 25;

    fn put_u8(out: &mut Vec<u8>, value: u8) {
        out.push(value);
    }

    fn put_u16(out: &mut Vec<u8>, value: u16) {
        out.extend_from_slice(&value.to_be_bytes());
    }

    fn put_u32(out: &mut Vec<u8>, value: u32) {
        out.extend_from_slice(&value.to_be_bytes());
    }

    fn put_i32(out: &mut Vec<u8>, value: i32) {
        out.extend_from_slice(&value.to_be_bytes());
    }

    fn put_cstring(out: &mut Vec<u8>, value: &str) {
        out.extend_from_slice(value.as_bytes());
        out.push(0);
    }

    fn put_text_value(out: &mut Vec<u8>, value: &str) {
        put_u8(out, b't');
        put_i32(out, value.len() as i32);
        out.extend_from_slice(value.as_bytes());
    }

    fn relation_message(relation_id: u32, table: &str) -> Bytes {
        relation_message_with_columns(
            relation_id,
            table,
            &[("id", PG_INT8_OID, true), ("status", PG_TEXT_OID, false)],
        )
    }

    fn relation_message_with_columns(
        relation_id: u32,
        table: &str,
        columns: &[(&str, u32, bool)],
    ) -> Bytes {
        let mut out = Vec::new();
        put_u8(&mut out, b'R');
        put_u32(&mut out, relation_id);
        put_cstring(&mut out, "public");
        put_cstring(&mut out, table);
        put_u8(&mut out, b'd');
        put_u16(&mut out, columns.len() as u16);

        for (name, oid, is_key) in columns {
            put_u8(&mut out, u8::from(*is_key));
            put_cstring(&mut out, name);
            put_u32(&mut out, *oid);
            put_i32(&mut out, -1);
        }

        Bytes::from(out)
    }

    fn insert_message(relation_id: u32, id: i64, status: &str) -> Bytes {
        insert_message_with_values(relation_id, &[id.to_string(), status.to_string()])
    }

    fn insert_message_with_values(relation_id: u32, values: &[String]) -> Bytes {
        let mut out = Vec::new();
        put_u8(&mut out, b'I');
        put_u32(&mut out, relation_id);
        put_u8(&mut out, b'N');
        put_u16(&mut out, values.len() as u16);
        for value in values {
            put_text_value(&mut out, value);
        }
        Bytes::from(out)
    }

    fn truncate_message(relation_ids: impl IntoIterator<Item = u32>) -> Bytes {
        let relation_ids: Vec<u32> = relation_ids.into_iter().collect();
        let mut out = Vec::new();
        put_u8(&mut out, b'T');
        put_u32(&mut out, relation_ids.len() as u32);
        put_u8(&mut out, 0);
        for relation_id in relation_ids {
            put_u32(&mut out, relation_id);
        }
        Bytes::from(out)
    }

    fn begin(xid: u32) -> PostgresReplicationEvent {
        PostgresReplicationEvent::Begin {
            final_lsn: PostgresLsn::from_u64(10),
            xid,
            commit_time_micros: 100,
        }
    }

    fn xlog(data: Bytes) -> PostgresReplicationEvent {
        PostgresReplicationEvent::XLogData {
            wal_start: PostgresLsn::from_u64(11),
            wal_end: PostgresLsn::from_u64(12),
            server_time_micros: 101,
            data,
        }
    }

    fn commit(end_lsn: u64) -> PostgresReplicationEvent {
        PostgresReplicationEvent::Commit {
            lsn: PostgresLsn::from_u64(end_lsn - 1),
            end_lsn: PostgresLsn::from_u64(end_lsn),
            commit_time_micros: 102,
        }
    }

    fn router() -> PostgresTableRouter {
        let mut router = PostgresTableRouter::new();
        router.insert(
            UpstreamTableRef::new("public", "orders").expect("upstream"),
            CdcTableId::new("orders").expect("table id"),
        );
        router
    }

    fn orders_schema() -> CdcTableSchema {
        schema_for(RELATION_ID, "orders", "orders")
    }

    fn schema_for(relation_id: u32, upstream_table: &str, table_id: &str) -> CdcTableSchema {
        let PgOutputMessage::Relation(relation) =
            decode_pgoutput_message(relation_message(relation_id, upstream_table))
                .expect("decode relation")
        else {
            panic!("expected relation");
        };
        relation
            .to_cdc_schema(CdcTableId::new(table_id).expect("table id"))
            .expect("schema")
    }

    fn orders_schemas() -> HashMap<CdcTableId, CdcTableSchema> {
        let schema = orders_schema();
        HashMap::from([(schema.table_id().clone(), schema)])
    }

    async fn test_store(name: &str) -> CdcTableStore {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, object_store).await.expect("open SlateDB"));
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        CdcTableStore::new(table)
    }

    enum FakeStep {
        Event(PostgresReplicationEvent),
        End,
        Error(&'static str),
    }

    struct FakeStream {
        steps: VecDeque<FakeStep>,
        feedbacks: Arc<Mutex<Vec<PostgresLsn>>>,
    }

    impl FakeStream {
        fn new(
            steps: impl IntoIterator<Item = FakeStep>,
            feedbacks: Arc<Mutex<Vec<PostgresLsn>>>,
        ) -> Self {
            Self {
                steps: steps.into_iter().collect(),
                feedbacks,
            }
        }
    }

    #[async_trait]
    impl PostgresReplicationStream for FakeStream {
        async fn recv_event(&mut self) -> Result<Option<PostgresReplicationEvent>> {
            match self.steps.pop_front().unwrap_or(FakeStep::End) {
                FakeStep::Event(event) => Ok(Some(event)),
                FakeStep::End => Ok(None),
                FakeStep::Error(message) => Err(anyhow!(message)),
            }
        }

        fn update_applied_lsn(&mut self, lsn: PostgresLsn) {
            self.feedbacks.lock().expect("feedback lock").push(lsn);
        }
    }

    #[derive(Clone)]
    struct FakeFactory {
        streams: Arc<Mutex<VecDeque<FakeStream>>>,
        configs: Arc<Mutex<Vec<PostgresCdcConfig>>>,
    }

    impl FakeFactory {
        fn new(streams: impl IntoIterator<Item = FakeStream>) -> Self {
            Self {
                streams: Arc::new(Mutex::new(streams.into_iter().collect())),
                configs: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn configs(&self) -> Vec<PostgresCdcConfig> {
            self.configs.lock().expect("configs lock").clone()
        }
    }

    #[async_trait]
    impl PostgresReplicationClientFactory for FakeFactory {
        type Stream = FakeStream;

        async fn connect(&self, config: &PostgresCdcConfig) -> Result<Self::Stream> {
            self.configs
                .lock()
                .expect("configs lock")
                .push(config.clone());
            self.streams
                .lock()
                .expect("streams lock")
                .pop_front()
                .ok_or_else(|| anyhow!("no fake stream configured"))
        }
    }

    #[test]
    fn assembles_decoded_changes_into_transaction_batch() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let mut assembler = PostgresTransactionAssembler::new(source_id.clone(), router());

        assert!(
            assembler
                .accept_event(xlog(relation_message(RELATION_ID, "orders")))
                .expect("relation metadata")
                .is_none()
        );
        assembler.accept_event(begin(55)).expect("begin");
        assembler
            .accept_event(xlog(insert_message(RELATION_ID, 7, "open")))
            .expect("insert");
        let transaction = assembler
            .accept_event(commit(30))
            .expect("commit")
            .expect("transaction");

        assert_eq!(transaction.source_id(), &source_id);
        assert_eq!(
            transaction.transaction_id().expect("txid").as_str(),
            "pg-xid-55"
        );
        assert_eq!(
            transaction.commit_position(),
            &CdcSourcePosition::Postgres {
                commit_lsn: "0/1E".to_string(),
                event_lsn: None
            }
        );
        assert_eq!(transaction.change_batches().len(), 1);
        assert_eq!(
            transaction.change_batches()[0].table_id(),
            &CdcTableId::new("orders").expect("table id")
        );
        assert_eq!(
            transaction.change_batches()[0].changes(),
            &[CdcChange::Insert {
                row: CdcRow::new([
                    Some(RowValue::Int64(7)),
                    Some(RowValue::Utf8("open".to_string())),
                ])
                .expect("row")
            }]
        );
    }

    #[test]
    fn compatible_column_additions_can_be_projected_to_catalog_schema() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let mut assembler = PostgresTransactionAssembler::with_schemas(
            source_id,
            router(),
            orders_schemas(),
            PostgresSchemaEvolutionPolicy::IgnoreCompatible,
        );
        let relation = relation_message_with_columns(
            RELATION_ID,
            "orders",
            &[
                ("id", PG_INT8_OID, true),
                ("status", PG_TEXT_OID, false),
                ("note", PG_TEXT_OID, false),
            ],
        );

        assembler
            .accept_event(xlog(relation.clone()))
            .expect("compatible relation");
        assembler.accept_event(begin(57)).expect("begin");
        assembler
            .accept_event(xlog(insert_message_with_values(
                RELATION_ID,
                &["7".to_string(), "open".to_string(), "ignored".to_string()],
            )))
            .expect("insert");
        let transaction = assembler
            .accept_event(commit(41))
            .expect("commit")
            .expect("transaction");

        assert_eq!(
            transaction.change_batches()[0].changes(),
            &[CdcChange::Insert {
                row: CdcRow::new([
                    Some(RowValue::Int64(7)),
                    Some(RowValue::Utf8("open".to_string())),
                ])
                .expect("row")
            }]
        );
        let PgOutputMessage::Relation(observed_relation) =
            decode_pgoutput_message(relation).expect("decode relation")
        else {
            panic!("expected relation");
        };
        let observed_schema = observed_relation
            .to_cdc_schema(CdcTableId::new("orders").expect("table id"))
            .expect("observed schema");
        assert_eq!(
            transaction.schema_versions().get("orders").copied(),
            Some(observed_schema.stable_fingerprint())
        );
    }

    #[test]
    fn fail_fast_schema_policy_rejects_compatible_additions() {
        let mut assembler = PostgresTransactionAssembler::with_schemas(
            CdcSourceId::new("pg_main").expect("source id"),
            router(),
            orders_schemas(),
            PostgresSchemaEvolutionPolicy::FailFast,
        );
        let err = assembler
            .accept_event(xlog(relation_message_with_columns(
                RELATION_ID,
                "orders",
                &[
                    ("id", PG_INT8_OID, true),
                    ("status", PG_TEXT_OID, false),
                    ("note", PG_TEXT_OID, false),
                ],
            )))
            .expect_err("compatible addition should fail under fail-fast");

        assert!(format!("{err:#}").contains("compatible column additions"));
    }

    #[test]
    fn schema_policy_rejects_incompatible_type_changes() {
        let mut assembler = PostgresTransactionAssembler::with_schemas(
            CdcSourceId::new("pg_main").expect("source id"),
            router(),
            orders_schemas(),
            PostgresSchemaEvolutionPolicy::IgnoreCompatible,
        );
        let err = assembler
            .accept_event(xlog(relation_message_with_columns(
                RELATION_ID,
                "orders",
                &[("id", PG_INT8_OID, true), ("status", PG_INT8_OID, false)],
            )))
            .expect_err("type change should fail");

        assert!(format!("{err:#}").contains("type changed"));
    }

    #[test]
    fn schema_policy_rejects_dropped_columns() {
        let mut assembler = PostgresTransactionAssembler::with_schemas(
            CdcSourceId::new("pg_main").expect("source id"),
            router(),
            orders_schemas(),
            PostgresSchemaEvolutionPolicy::IgnoreCompatible,
        );
        let err = assembler
            .accept_event(xlog(relation_message_with_columns(
                RELATION_ID,
                "orders",
                &[("id", PG_INT8_OID, true)],
            )))
            .expect_err("dropped column should fail");

        assert!(format!("{err:#}").contains("column count decreased"));
    }

    #[test]
    fn schema_policy_rejects_reordered_columns() {
        let mut assembler = PostgresTransactionAssembler::with_schemas(
            CdcSourceId::new("pg_main").expect("source id"),
            router(),
            orders_schemas(),
            PostgresSchemaEvolutionPolicy::IgnoreCompatible,
        );
        let err = assembler
            .accept_event(xlog(relation_message_with_columns(
                RELATION_ID,
                "orders",
                &[("status", PG_TEXT_OID, false), ("id", PG_INT8_OID, true)],
            )))
            .expect_err("reordered columns should fail");

        assert!(format!("{err:#}").contains("column 0 changed"));
    }

    #[test]
    fn groups_multiple_tables_in_one_source_transaction() {
        let mut router = router();
        router.insert(
            UpstreamTableRef::new("public", "customers").expect("upstream"),
            CdcTableId::new("customers").expect("table id"),
        );
        let mut assembler =
            PostgresTransactionAssembler::new(CdcSourceId::new("pg_main").expect("source"), router);

        assembler
            .accept_event(xlog(relation_message(RELATION_ID, "orders")))
            .expect("orders relation");
        assembler
            .accept_event(xlog(relation_message(OTHER_RELATION_ID, "customers")))
            .expect("customers relation");
        assembler.accept_event(begin(56)).expect("begin");
        assembler
            .accept_event(xlog(insert_message(RELATION_ID, 7, "open")))
            .expect("orders insert");
        assembler
            .accept_event(xlog(insert_message(OTHER_RELATION_ID, 8, "active")))
            .expect("customers insert");

        let transaction = assembler
            .accept_event(commit(40))
            .expect("commit")
            .expect("transaction");
        let tables: Vec<&str> = transaction
            .change_batches()
            .iter()
            .map(|batch| batch.table_id().as_str())
            .collect();
        assert_eq!(tables, vec!["orders", "customers"]);
    }

    #[test]
    fn groups_multi_relation_truncate_in_one_source_transaction() {
        let mut router = router();
        router.insert(
            UpstreamTableRef::new("public", "customers").expect("upstream"),
            CdcTableId::new("customers").expect("table id"),
        );
        let mut assembler =
            PostgresTransactionAssembler::new(CdcSourceId::new("pg_main").expect("source"), router);

        assembler
            .accept_event(xlog(relation_message(RELATION_ID, "orders")))
            .expect("orders relation");
        assembler
            .accept_event(xlog(relation_message(OTHER_RELATION_ID, "customers")))
            .expect("customers relation");
        assembler.accept_event(begin(62)).expect("begin");
        assembler
            .accept_event(xlog(truncate_message([RELATION_ID, OTHER_RELATION_ID])))
            .expect("truncate");

        let transaction = assembler
            .accept_event(commit(45))
            .expect("commit")
            .expect("transaction");
        assert_eq!(transaction.change_batches().len(), 2);
        assert_eq!(
            transaction.change_batches()[0].changes(),
            &[CdcChange::Truncate]
        );
        assert_eq!(
            transaction.change_batches()[1].changes(),
            &[CdcChange::Truncate]
        );
    }

    #[test]
    fn ignores_unrouted_tables_and_empty_transactions() {
        let mut assembler = PostgresTransactionAssembler::new(
            CdcSourceId::new("pg_main").expect("source"),
            router(),
        );
        assembler
            .accept_event(xlog(relation_message(OTHER_RELATION_ID, "unmapped")))
            .expect("relation");
        assembler.accept_event(begin(57)).expect("begin");
        assembler
            .accept_event(xlog(insert_message(OTHER_RELATION_ID, 7, "ignored")))
            .expect("ignored insert");
        assert!(
            assembler
                .accept_event(commit(50))
                .expect("commit")
                .is_none()
        );
    }

    #[test]
    fn rejects_dml_outside_transaction_boundary() {
        let mut assembler = PostgresTransactionAssembler::new(
            CdcSourceId::new("pg_main").expect("source"),
            router(),
        );
        assembler
            .accept_event(xlog(relation_message(RELATION_ID, "orders")))
            .expect("relation");
        let err = assembler
            .accept_event(xlog(insert_message(RELATION_ID, 7, "open")))
            .expect_err("dml outside transaction should fail");
        assert!(format!("{err:#}").contains("outside a transaction"));
    }

    #[tokio::test]
    async fn applier_returns_feedback_lsn_only_after_table_apply() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let table_store = test_store("pg-cdc-applier-apply").await;
        let mut applier =
            PostgresCdcEventApplier::new(source_id.clone(), table_store.clone(), orders_schemas());

        let relation_outcome = applier
            .accept_event(xlog(relation_message(RELATION_ID, "orders")))
            .await
            .expect("relation");
        assert!(relation_outcome.apply_result().is_none());
        assert_eq!(relation_outcome.feedback_lsn(), None);

        applier.accept_event(begin(58)).await.expect("begin");
        applier
            .accept_event(xlog(insert_message(RELATION_ID, 9, "open")))
            .await
            .expect("insert");
        let outcome = applier
            .accept_event(commit(60))
            .await
            .expect("commit apply");

        assert_eq!(outcome.feedback_lsn(), Some(PostgresLsn::from_u64(60)));
        let apply_result = outcome.apply_result().expect("apply result");
        assert!(!apply_result.already_committed());
        assert_eq!(
            table_store
                .load_checkpoint(&source_id)
                .await
                .expect("load checkpoint"),
            Some(apply_result.checkpoint().clone())
        );
        assert_eq!(
            table_store
                .load_row(
                    &CdcTableId::new("orders").expect("table id"),
                    &CdcRowKey::new([RowValue::Int64(9)]).expect("key")
                )
                .await
                .expect("load row"),
            Some(
                CdcRow::new([
                    Some(RowValue::Int64(9)),
                    Some(RowValue::Utf8("open".to_string()))
                ])
                .expect("row")
            )
        );

        let lag = outcome.lag_snapshot();
        assert_eq!(lag.source_id(), &source_id);
        assert_eq!(lag.upstream_wal_end(), Some(PostgresLsn::from_u64(60)));
        assert_eq!(lag.durable_lsn(), Some(PostgresLsn::from_u64(60)));
        assert_eq!(lag.source_lag_bytes(), Some(0));
        assert_eq!(lag.table_lags().len(), 1);
        assert_eq!(
            lag.table_lags()[0].table_id(),
            &CdcTableId::new("orders").expect("table id")
        );
        assert_eq!(
            lag.table_lags()[0].last_applied_lsn(),
            Some(PostgresLsn::from_u64(60))
        );
        assert_eq!(lag.table_lags()[0].table_lag_bytes(), Some(0));
    }

    #[tokio::test]
    async fn applier_does_not_persist_or_feedback_when_table_apply_fails() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let table_store = test_store("pg-cdc-applier-apply-fails").await;
        let mut applier = PostgresCdcEventApplier::with_router(
            source_id.clone(),
            table_store.clone(),
            HashMap::new(),
            router(),
        );

        applier
            .accept_event(xlog(relation_message(RELATION_ID, "orders")))
            .await
            .expect("relation");
        applier.accept_event(begin(59)).await.expect("begin");
        applier
            .accept_event(xlog(insert_message(RELATION_ID, 10, "open")))
            .await
            .expect("insert");
        let err = applier
            .accept_event(commit(70))
            .await
            .expect_err("missing schema should fail apply");
        assert!(format!("{err:#}").contains("unknown table"));
        assert_eq!(
            table_store
                .load_checkpoint(&source_id)
                .await
                .expect("load checkpoint"),
            None
        );
    }

    #[tokio::test]
    async fn apply_loop_ignores_idle_events_and_feedbacks_after_commit() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let table_store = test_store("pg-cdc-loop-feedback").await;
        let mut applier = PostgresCdcEventApplier::new(source_id, table_store, orders_schemas());
        let feedbacks = Arc::new(Mutex::new(Vec::new()));
        let mut stream = FakeStream::new(
            [
                FakeStep::Event(PostgresReplicationEvent::KeepAlive {
                    wal_end: PostgresLsn::from_u64(11),
                    reply_requested: true,
                    server_time_micros: 1,
                }),
                FakeStep::Event(xlog(relation_message(RELATION_ID, "orders"))),
                FakeStep::Event(begin(60)),
                FakeStep::Event(xlog(insert_message(RELATION_ID, 11, "open"))),
                FakeStep::Event(PostgresReplicationEvent::Message {
                    transactional: false,
                    lsn: PostgresLsn::from_u64(12),
                    prefix: "noop".to_string(),
                    content: Bytes::new(),
                }),
                FakeStep::Event(commit(80)),
                FakeStep::End,
            ],
            Arc::clone(&feedbacks),
        );

        run_postgres_cdc_apply_loop(&mut stream, &mut applier)
            .await
            .expect("run apply loop");
        assert_eq!(
            *feedbacks.lock().expect("feedback lock"),
            vec![PostgresLsn::from_u64(80)]
        );
    }

    #[tokio::test]
    async fn applier_exposes_shared_source_and_per_table_lag() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let table_store = test_store("pg-cdc-lag-snapshot").await;
        let orders = schema_for(RELATION_ID, "orders", "orders");
        let customers = schema_for(OTHER_RELATION_ID, "customers", "customers");
        let schemas = HashMap::from([
            (orders.table_id().clone(), orders),
            (customers.table_id().clone(), customers),
        ]);
        let mut router = PostgresTableRouter::new();
        router.insert(
            UpstreamTableRef::new("public", "orders").expect("orders upstream"),
            CdcTableId::new("orders").expect("orders id"),
        );
        router.insert(
            UpstreamTableRef::new("public", "customers").expect("customers upstream"),
            CdcTableId::new("customers").expect("customers id"),
        );
        let mut applier =
            PostgresCdcEventApplier::with_router(source_id.clone(), table_store, schemas, router);

        applier
            .accept_event(xlog(relation_message(RELATION_ID, "orders")))
            .await
            .expect("orders relation");
        applier
            .accept_event(xlog(relation_message(OTHER_RELATION_ID, "customers")))
            .await
            .expect("customers relation");
        applier.accept_event(begin(63)).await.expect("begin");
        applier
            .accept_event(xlog(insert_message(RELATION_ID, 20, "open")))
            .await
            .expect("orders insert");
        let applied = applier
            .accept_event(commit(100))
            .await
            .expect("commit apply");

        assert_eq!(applied.lag_snapshot().source_lag_bytes(), Some(0));
        let idle = applier
            .accept_event(PostgresReplicationEvent::KeepAlive {
                wal_end: PostgresLsn::from_u64(150),
                reply_requested: false,
                server_time_micros: 200,
            })
            .await
            .expect("keepalive");
        let lag = idle.lag_snapshot();
        assert_eq!(lag.source_id(), &source_id);
        assert_eq!(lag.upstream_wal_end(), Some(PostgresLsn::from_u64(150)));
        assert_eq!(lag.durable_lsn(), Some(PostgresLsn::from_u64(100)));
        assert_eq!(lag.source_lag_bytes(), Some(50));

        let table_lags = lag.table_lags();
        assert_eq!(table_lags.len(), 2);
        assert_eq!(
            table_lags[0].table_id(),
            &CdcTableId::new("customers").expect("customers id")
        );
        assert_eq!(table_lags[0].last_applied_lsn(), None);
        assert_eq!(table_lags[0].table_lag_bytes(), None);
        assert_eq!(
            table_lags[1].table_id(),
            &CdcTableId::new("orders").expect("orders id")
        );
        assert_eq!(
            table_lags[1].last_applied_lsn(),
            Some(PostgresLsn::from_u64(100))
        );
        assert_eq!(table_lags[1].table_lag_bytes(), Some(50));
    }

    #[tokio::test]
    async fn reconnect_loop_reloads_checkpoint_as_next_start_lsn() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let table_store = test_store("pg-cdc-loop-reconnect").await;
        let mut applier =
            PostgresCdcEventApplier::new(source_id.clone(), table_store.clone(), orders_schemas());
        let feedbacks = Arc::new(Mutex::new(Vec::new()));
        let first_stream = FakeStream::new(
            [
                FakeStep::Event(xlog(relation_message(RELATION_ID, "orders"))),
                FakeStep::Event(begin(61)),
                FakeStep::Event(xlog(insert_message(RELATION_ID, 12, "open"))),
                FakeStep::Event(commit(90)),
                FakeStep::Error("disconnect"),
            ],
            Arc::clone(&feedbacks),
        );
        let second_stream = FakeStream::new([FakeStep::End], Arc::clone(&feedbacks));
        let factory = FakeFactory::new([first_stream, second_stream]);

        run_postgres_cdc_apply_loop_with_reconnect(
            PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
                .expect("config"),
            &source_id,
            &table_store,
            &mut applier,
            &factory,
            PostgresCdcReconnectPolicy::new(1, Duration::ZERO),
        )
        .await
        .expect("run reconnect loop");

        assert_eq!(
            *feedbacks.lock().expect("feedback lock"),
            vec![PostgresLsn::from_u64(90)]
        );
        let configs = factory.configs();
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].start_lsn(), None);
        assert_eq!(configs[1].start_lsn(), Some(PostgresLsn::from_u64(90)));
    }

    #[tokio::test]
    async fn reconnect_loop_replays_inflight_wal_transaction_from_durable_checkpoint() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let table_store = test_store("pg-cdc-loop-inflight-reconnect").await;
        let mut applier =
            PostgresCdcEventApplier::new(source_id.clone(), table_store.clone(), orders_schemas());
        let feedbacks = Arc::new(Mutex::new(Vec::new()));
        let first_stream = FakeStream::new(
            [
                FakeStep::Event(xlog(relation_message(RELATION_ID, "orders"))),
                FakeStep::Event(begin(61)),
                FakeStep::Event(xlog(insert_message(RELATION_ID, 12, "committed"))),
                FakeStep::Event(commit(90)),
                FakeStep::Event(begin(63)),
                FakeStep::Event(xlog(insert_message(RELATION_ID, 14, "inflight"))),
                FakeStep::Error("disconnect before commit"),
            ],
            Arc::clone(&feedbacks),
        );
        let second_stream = FakeStream::new(
            [
                FakeStep::Event(xlog(relation_message(RELATION_ID, "orders"))),
                FakeStep::Event(begin(63)),
                FakeStep::Event(xlog(insert_message(RELATION_ID, 14, "replayed"))),
                FakeStep::Event(commit(120)),
                FakeStep::End,
            ],
            Arc::clone(&feedbacks),
        );
        let factory = FakeFactory::new([first_stream, second_stream]);

        run_postgres_cdc_apply_loop_with_reconnect(
            PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
                .expect("config"),
            &source_id,
            &table_store,
            &mut applier,
            &factory,
            PostgresCdcReconnectPolicy::new(1, Duration::ZERO),
        )
        .await
        .expect("run reconnect loop");

        assert_eq!(
            *feedbacks.lock().expect("feedback lock"),
            vec![PostgresLsn::from_u64(90), PostgresLsn::from_u64(120)]
        );
        let configs = factory.configs();
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].start_lsn(), None);
        assert_eq!(configs[1].start_lsn(), Some(PostgresLsn::from_u64(90)));
        let checkpoint = table_store
            .load_checkpoint(&source_id)
            .await
            .expect("load checkpoint")
            .expect("checkpoint");
        assert_eq!(
            PostgresLsn::from_source_position(checkpoint.position()).expect("checkpoint lsn"),
            PostgresLsn::from_u64(120)
        );
        assert_eq!(
            table_store
                .load_row(
                    &CdcTableId::new("orders").expect("table id"),
                    &CdcRowKey::new([RowValue::Int64(12)]).expect("key")
                )
                .await
                .expect("load committed row"),
            Some(
                CdcRow::new([
                    Some(RowValue::Int64(12)),
                    Some(RowValue::Utf8("committed".to_string())),
                ])
                .expect("row")
            )
        );
        assert_eq!(
            table_store
                .load_row(
                    &CdcTableId::new("orders").expect("table id"),
                    &CdcRowKey::new([RowValue::Int64(14)]).expect("key")
                )
                .await
                .expect("load replayed row"),
            Some(
                CdcRow::new([
                    Some(RowValue::Int64(14)),
                    Some(RowValue::Utf8("replayed".to_string())),
                ])
                .expect("row")
            )
        );
    }

    #[tokio::test]
    async fn reconnect_loop_resumes_after_compatible_schema_change() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let table_store = test_store("pg-cdc-loop-schema-reconnect").await;
        let relation = relation_message_with_columns(
            RELATION_ID,
            "orders",
            &[
                ("id", PG_INT8_OID, true),
                ("status", PG_TEXT_OID, false),
                ("note", PG_TEXT_OID, false),
            ],
        );
        let PgOutputMessage::Relation(observed_relation) =
            decode_pgoutput_message(relation.clone()).expect("decode relation")
        else {
            panic!("expected relation");
        };
        let observed_schema = observed_relation
            .to_cdc_schema(CdcTableId::new("orders").expect("table id"))
            .expect("observed schema");
        let mut applier = PostgresCdcEventApplier::with_schema_policy(
            source_id.clone(),
            table_store.clone(),
            orders_schemas(),
            PostgresSchemaEvolutionPolicy::IgnoreCompatible,
        );
        let feedbacks = Arc::new(Mutex::new(Vec::new()));
        let first_stream = FakeStream::new(
            [
                FakeStep::Event(xlog(relation)),
                FakeStep::Event(begin(62)),
                FakeStep::Event(xlog(insert_message_with_values(
                    RELATION_ID,
                    &["13".to_string(), "open".to_string(), "ignored".to_string()],
                ))),
                FakeStep::Event(commit(120)),
                FakeStep::Error("disconnect after schema change"),
            ],
            Arc::clone(&feedbacks),
        );
        let second_stream = FakeStream::new([FakeStep::End], Arc::clone(&feedbacks));
        let factory = FakeFactory::new([first_stream, second_stream]);

        run_postgres_cdc_apply_loop_with_reconnect(
            PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
                .expect("config"),
            &source_id,
            &table_store,
            &mut applier,
            &factory,
            PostgresCdcReconnectPolicy::new(1, Duration::ZERO),
        )
        .await
        .expect("run reconnect loop");

        assert_eq!(
            *feedbacks.lock().expect("feedback lock"),
            vec![PostgresLsn::from_u64(120)]
        );
        let configs = factory.configs();
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].start_lsn(), None);
        assert_eq!(configs[1].start_lsn(), Some(PostgresLsn::from_u64(120)));
        let checkpoint = table_store
            .load_checkpoint(&source_id)
            .await
            .expect("load checkpoint")
            .expect("checkpoint");
        assert_eq!(
            checkpoint.schema_versions().get("orders").copied(),
            Some(observed_schema.stable_fingerprint())
        );
        assert_eq!(
            table_store
                .load_row(
                    &CdcTableId::new("orders").expect("table id"),
                    &CdcRowKey::new([RowValue::Int64(13)]).expect("key")
                )
                .await
                .expect("load row"),
            Some(
                CdcRow::new([
                    Some(RowValue::Int64(13)),
                    Some(RowValue::Utf8("open".to_string())),
                ])
                .expect("row")
            )
        );
    }

    #[tokio::test]
    async fn reconnect_loop_errors_after_max_reconnects() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let table_store = test_store("pg-cdc-loop-reconnect-exhausted").await;
        let mut applier =
            PostgresCdcEventApplier::new(source_id.clone(), table_store.clone(), orders_schemas());
        let feedbacks = Arc::new(Mutex::new(Vec::new()));
        let factory = FakeFactory::new([
            FakeStream::new([FakeStep::Error("disconnect 1")], Arc::clone(&feedbacks)),
            FakeStream::new([FakeStep::Error("disconnect 2")], feedbacks),
        ]);

        let err = run_postgres_cdc_apply_loop_with_reconnect(
            PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
                .expect("config"),
            &source_id,
            &table_store,
            &mut applier,
            &factory,
            PostgresCdcReconnectPolicy::new(1, Duration::ZERO),
        )
        .await
        .expect_err("reconnects should be exhausted");
        assert!(format!("{err:#}").contains("failed after 1 reconnect"));
    }
}
