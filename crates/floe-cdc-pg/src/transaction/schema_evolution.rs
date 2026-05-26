use std::collections::{HashMap, VecDeque};

use anyhow::{Result, anyhow};
use floe_cdc_core::{
    CdcChange, CdcRow, CdcSchemaVersionMap, CdcTableId, CdcTableSchema, UpstreamTableRef,
};

use crate::PgOutputCdcChange;

pub(super) const POSTGRES_SCHEMA_HISTORY_LIMIT: usize = 64;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresSchemaEvolutionOutcome {
    CompatibleAddition,
    RejectedCompatibleAddition,
    Incompatible,
}

impl PostgresSchemaEvolutionOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CompatibleAddition => "compatible_addition",
            Self::RejectedCompatibleAddition => "rejected_compatible_addition",
            Self::Incompatible => "incompatible",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresSchemaEvolutionObservation {
    table_id: CdcTableId,
    upstream_table: UpstreamTableRef,
    policy: PostgresSchemaEvolutionPolicy,
    outcome: PostgresSchemaEvolutionOutcome,
    added_columns: Vec<String>,
    reason: Option<String>,
    catalog_schema_version: u64,
    observed_schema_version: u64,
}

impl PostgresSchemaEvolutionObservation {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        table_id: CdcTableId,
        upstream_table: UpstreamTableRef,
        policy: PostgresSchemaEvolutionPolicy,
        outcome: PostgresSchemaEvolutionOutcome,
        added_columns: Vec<String>,
        reason: Option<String>,
        catalog_schema_version: u64,
        observed_schema_version: u64,
    ) -> Self {
        Self {
            table_id,
            upstream_table,
            policy,
            outcome,
            added_columns,
            reason,
            catalog_schema_version,
            observed_schema_version,
        }
    }

    pub fn table_id(&self) -> &CdcTableId {
        &self.table_id
    }

    pub fn upstream_table(&self) -> &UpstreamTableRef {
        &self.upstream_table
    }

    pub fn policy(&self) -> PostgresSchemaEvolutionPolicy {
        self.policy
    }

    pub fn outcome(&self) -> PostgresSchemaEvolutionOutcome {
        self.outcome
    }

    pub fn added_columns(&self) -> &[String] {
        &self.added_columns
    }

    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    pub fn catalog_schema_version(&self) -> u64 {
        self.catalog_schema_version
    }

    pub fn observed_schema_version(&self) -> u64 {
        self.observed_schema_version
    }
}

pub(super) enum SchemaEvolution {
    Unchanged,
    CompatibleAddition { added_columns: Vec<String> },
    Incompatible(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PostgresObservedSchemaVersion {
    version: u64,
    column_count: usize,
    primary_key_columns: Vec<String>,
}

impl PostgresObservedSchemaVersion {
    pub(super) fn from_schema(schema: &CdcTableSchema) -> Self {
        Self {
            version: schema.stable_fingerprint(),
            column_count: schema.columns().len(),
            primary_key_columns: schema.primary_key().columns().to_vec(),
        }
    }

    pub(super) fn version(&self) -> u64 {
        self.version
    }
}

pub(super) fn push_schema_history(
    history: &mut VecDeque<PostgresObservedSchemaVersion>,
    observed: PostgresObservedSchemaVersion,
) {
    if history
        .back()
        .is_some_and(|existing| existing.version() == observed.version())
    {
        return;
    }
    history.push_back(observed);
    while history.len() > POSTGRES_SCHEMA_HISTORY_LIMIT {
        history.pop_front();
    }
}

pub(super) fn classify_schema_evolution(
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

pub(super) fn project_change_to_schema(
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

pub(super) fn schema_versions_for_schemas(
    schemas: &HashMap<CdcTableId, CdcTableSchema>,
) -> CdcSchemaVersionMap {
    schemas
        .iter()
        .map(|(table_id, schema)| (table_id.as_str().to_string(), schema.stable_fingerprint()))
        .collect()
}
