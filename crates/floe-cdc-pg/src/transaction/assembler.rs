use std::collections::{HashMap, VecDeque};

use anyhow::{Result, anyhow, bail};
use floe_cdc_core::{
    CdcChange, CdcSchemaVersionMap, CdcSourceId, CdcSourcePosition, CdcTableId, CdcTableSchema,
    CdcTransactionId, ChangeBatch, TransactionBatch,
};

use crate::{
    PgOutputCdcChange, PgOutputDecoder, PostgresLsn, PostgresReplicaIdentity,
    PostgresReplicationEvent,
};

use super::router::PostgresTableRouter;
use super::schema_evolution::{
    PostgresObservedSchemaVersion, PostgresSchemaEvolutionObservation,
    PostgresSchemaEvolutionObservationParts, PostgresSchemaEvolutionOutcome,
    PostgresSchemaEvolutionPolicy, SchemaEvolution, classify_schema_evolution,
    project_change_to_schema, push_schema_history, schema_versions_for_schemas,
};

pub struct PostgresTransactionAssembler {
    source_id: CdcSourceId,
    router: PostgresTableRouter,
    decoder: PgOutputDecoder,
    current: Option<InFlightTransaction>,
    schemas: HashMap<CdcTableId, CdcTableSchema>,
    schema_policy: PostgresSchemaEvolutionPolicy,
    schema_versions: CdcSchemaVersionMap,
    schema_history: HashMap<CdcTableId, VecDeque<PostgresObservedSchemaVersion>>,
    replica_identity_by_table: HashMap<CdcTableId, PostgresReplicaIdentity>,
    schema_observations: Vec<PostgresSchemaEvolutionObservation>,
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
            schema_history: HashMap::new(),
            replica_identity_by_table: HashMap::new(),
            schema_observations: Vec::new(),
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
            schema_history: HashMap::new(),
            replica_identity_by_table: HashMap::new(),
            schema_observations: Vec::new(),
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

    pub fn drain_schema_evolution_observations(
        &mut self,
    ) -> Vec<PostgresSchemaEvolutionObservation> {
        self.schema_observations.drain(..).collect()
    }

    pub fn reset_stream_state(&mut self) {
        self.decoder = PgOutputDecoder::new();
        self.current = None;
    }

    #[cfg(test)]
    pub(crate) fn schema_history_len_for_test(&self, table_id: &CdcTableId) -> usize {
        self.schema_history
            .get(table_id)
            .map(VecDeque::len)
            .unwrap_or_default()
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
        let Some(catalog_schema) = self.schemas.get(&table_id).cloned() else {
            return Ok(());
        };
        let catalog_schema_version = catalog_schema.stable_fingerprint();
        let observed_schema = match relation.to_cdc_schema(table_id.clone()) {
            Ok(schema) => schema,
            Err(err) => {
                let reason = format!(
                    "replica identity {:?} cannot provide a CDC key: {err:#}",
                    relation.replica_identity()
                );
                self.record_schema_evolution_observation(PostgresSchemaEvolutionObservation::new(
                    PostgresSchemaEvolutionObservationParts {
                        table_id: table_id.clone(),
                        upstream_table: upstream_table.clone(),
                        policy: self.schema_policy,
                        outcome: PostgresSchemaEvolutionOutcome::Incompatible,
                        added_columns: Vec::new(),
                        reason: Some(reason.clone()),
                        catalog_schema_version,
                        observed_schema_version: catalog_schema_version,
                    },
                ));
                bail!(
                    "Postgres CDC schema for table '{}' is incompatible with catalog schema: {reason}",
                    table_id.as_str()
                );
            }
        };
        let observed_schema_version = observed_schema.stable_fingerprint();
        if let Err(err) = self.accept_replica_identity(&table_id, relation.replica_identity()) {
            let reason = err.to_string();
            self.record_schema_evolution_observation(PostgresSchemaEvolutionObservation::new(
                PostgresSchemaEvolutionObservationParts {
                    table_id: table_id.clone(),
                    upstream_table: upstream_table.clone(),
                    policy: self.schema_policy,
                    outcome: PostgresSchemaEvolutionOutcome::Incompatible,
                    added_columns: Vec::new(),
                    reason: Some(reason.clone()),
                    catalog_schema_version,
                    observed_schema_version,
                },
            ));
            bail!(
                "Postgres CDC schema for table '{}' is incompatible with catalog schema: {reason}",
                table_id.as_str()
            );
        }
        let catalog_column_count = catalog_schema.columns().len();
        let observed_column_count = observed_schema.columns().len();
        match classify_schema_evolution(&catalog_schema, &observed_schema) {
            SchemaEvolution::Unchanged => {
                self.schema_versions
                    .insert(table_id.as_str().to_string(), catalog_schema_version);
                self.record_schema_history(table_id, &observed_schema);
                Ok(())
            }
            SchemaEvolution::CompatibleAddition { added_columns } => match self.schema_policy {
                PostgresSchemaEvolutionPolicy::FailFast => {
                    self.record_schema_evolution_observation(
                        PostgresSchemaEvolutionObservation::new(
                            PostgresSchemaEvolutionObservationParts {
                                table_id: table_id.clone(),
                                upstream_table: upstream_table.clone(),
                                policy: self.schema_policy,
                                outcome: PostgresSchemaEvolutionOutcome::RejectedCompatibleAddition,
                                added_columns: added_columns.clone(),
                                reason: Some(
                                    "compatible column additions rejected by fail-fast policy"
                                        .to_string(),
                                ),
                                catalog_schema_version,
                                observed_schema_version,
                            },
                        ),
                    );
                    tracing::warn!(
                        source = %self.source_id.as_str(),
                        table = %table_id.as_str(),
                        upstream_table = %format!("{}.{}", upstream_table.schema(), upstream_table.table()),
                        policy = ?self.schema_policy,
                        added_column_count = added_columns.len(),
                        added_columns = ?added_columns,
                        catalog_schema_version,
                        observed_schema_version,
                        "Postgres CDC relation schema has compatible additions but fail-fast policy rejects schema evolution"
                    );
                    bail!(
                        "Postgres CDC schema for table '{}' has compatible column additions but policy is fail-fast",
                        table_id.as_str()
                    )
                }
                PostgresSchemaEvolutionPolicy::IgnoreCompatible
                | PostgresSchemaEvolutionPolicy::ApplyCompatibleAdditions => {
                    self.record_schema_evolution_observation(
                        PostgresSchemaEvolutionObservation::new(
                            PostgresSchemaEvolutionObservationParts {
                                table_id: table_id.clone(),
                                upstream_table: upstream_table.clone(),
                                policy: self.schema_policy,
                                outcome: PostgresSchemaEvolutionOutcome::CompatibleAddition,
                                added_columns: added_columns.clone(),
                                reason: None,
                                catalog_schema_version,
                                observed_schema_version,
                            },
                        ),
                    );
                    tracing::info!(
                        source = %self.source_id.as_str(),
                        table = %table_id.as_str(),
                        upstream_table = %format!("{}.{}", upstream_table.schema(), upstream_table.table()),
                        policy = ?self.schema_policy,
                        added_column_count = added_columns.len(),
                        added_columns = ?added_columns,
                        catalog_schema_version,
                        observed_schema_version,
                        "Postgres CDC relation schema has compatible additions; projecting to catalog schema"
                    );
                    self.schema_versions
                        .insert(table_id.as_str().to_string(), observed_schema_version);
                    self.record_schema_history(table_id, &observed_schema);
                    Ok(())
                }
            },
            SchemaEvolution::Incompatible(reason) => {
                self.record_schema_evolution_observation(PostgresSchemaEvolutionObservation::new(
                    PostgresSchemaEvolutionObservationParts {
                        table_id: table_id.clone(),
                        upstream_table: upstream_table.clone(),
                        policy: self.schema_policy,
                        outcome: PostgresSchemaEvolutionOutcome::Incompatible,
                        added_columns: Vec::new(),
                        reason: Some(reason.clone()),
                        catalog_schema_version,
                        observed_schema_version,
                    },
                ));
                tracing::warn!(
                    source = %self.source_id.as_str(),
                    table = %table_id.as_str(),
                    upstream_table = %format!("{}.{}", upstream_table.schema(), upstream_table.table()),
                    policy = ?self.schema_policy,
                    reason = %reason,
                    catalog_column_count,
                    observed_column_count,
                    catalog_schema_version,
                    observed_schema_version,
                    "Postgres CDC relation schema is incompatible with catalog schema"
                );
                bail!(
                    "Postgres CDC schema for table '{}' is incompatible with catalog schema: {reason}",
                    table_id.as_str()
                )
            }
        }
    }

    fn accept_replica_identity(
        &mut self,
        table_id: &CdcTableId,
        observed: PostgresReplicaIdentity,
    ) -> Result<()> {
        match observed {
            PostgresReplicaIdentity::Default
            | PostgresReplicaIdentity::Index
            | PostgresReplicaIdentity::Full => {}
            PostgresReplicaIdentity::Nothing => {
                bail!("replica identity NOTHING cannot safely decode updates or deletes")
            }
            PostgresReplicaIdentity::Unknown(value) => {
                bail!("unsupported replica identity mode 0x{value:02x}")
            }
        }
        if let Some(previous) = self.replica_identity_by_table.get(table_id)
            && *previous != observed
        {
            bail!("replica identity changed from {previous:?} to {observed:?}");
        }
        self.replica_identity_by_table
            .insert(table_id.clone(), observed);
        Ok(())
    }

    fn record_schema_history(&mut self, table_id: CdcTableId, schema: &CdcTableSchema) {
        push_schema_history(
            self.schema_history.entry(table_id).or_default(),
            PostgresObservedSchemaVersion::from_schema(schema),
        );
    }

    fn record_schema_evolution_observation(
        &mut self,
        observation: PostgresSchemaEvolutionObservation,
    ) {
        self.schema_observations.push(observation);
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
