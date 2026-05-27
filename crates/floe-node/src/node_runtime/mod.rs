use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use clap::Parser;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::DFSchemaRef;
use dbsp::collections::CompactionPolicy;
use dbsp::storage::gc::{GcPolicy, GcService};
use dbsp::storage::{KeyValueTable, SlateTable};
use dbsp::{CompactionSchedulerConfig, StreamRetention};
use floe_cdc::CdcTableStore;
use floe_cdc_core::{
    CdcColumn, CdcPrimaryKey, CdcSourceId, CdcTableId, CdcTableSchema, ChangeBatch,
    TransactionBatch, UpstreamTableRef,
};
use floe_cdc_pg::{
    PostgresCdcConfig, PostgresLsn, PostgresReplicationClient, PostgresReplicationEvent,
    PostgresSchemaEvolutionObservation, PostgresSchemaEvolutionPolicy, PostgresTableRouter,
    PostgresTransactionAssembler, config_with_stored_cdc_checkpoint,
    create_pgoutput_slot_with_exported_snapshot,
};
#[cfg(test)]
use floe_core::catalog::ReplicationErrorPolicyMode as CatalogReplicationErrorPolicyMode;
use floe_core::catalog::{
    CatalogSourceConnector, CatalogSourceDefinition, ColumnDefinition, ColumnType,
    PostgresCdcSchemaEvolutionPolicy as CatalogPostgresCdcSchemaEvolutionPolicy,
    PostgresCdcSourceDefinition, ReplicationBufferMode as CatalogReplicationBufferMode,
    ReplicationBufferPolicy as CatalogReplicationBufferPolicy,
    ReplicationErrorPolicy as CatalogReplicationErrorPolicy,
    ReplicationPipelineDefinition as CatalogReplicationPipelineDefinition,
    ReplicationPipelineFormat as CatalogReplicationPipelineFormat,
    ReplicationPipelineTarget as CatalogReplicationPipelineTarget, SourceBackedTableDefinition,
    TableDefinition,
};
use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};
use floe_executor::checkpoint::{
    CheckpointManager, KafkaCheckpointOffset, MaterializedViewTickVersion, SinkCursor, TickCommit,
};
use floe_executor::source_journal::{
    KafkaSourceJournal, KafkaSourceJournalRange, SourceBatchJournal,
    kafka_source_journal_initial_checksum, update_kafka_source_journal_checksum,
};
use floe_executor::{
    BuildInputs, ConsolidationMode, DbspBridge, DbspGraphBuilder, FloeQueryContext, GraphTaskError,
    MaterializedViewRegistry, MaterializedViewTableProvider, MvFlushCoalescingConfig,
    OuterStreamRegistry, OverlaySnapshotConfig, PersistencePolicyConfig, SourceRowDecoder,
    SourceTableProvider, TailExecutionConfig, ValidatedPlan, plan_source_requirements,
    source_batch_journal_root_sources_with_config, validate_dbsp_plan,
};
use floe_node_core::cdc_delta_encoder::encode_cdc_table_deltas;
use floe_node_core::connector::{ConnectorContext, run_connector};
use floe_node_core::file_connector::{FileConnector, FileConnectorConfig};
#[cfg(test)]
use floe_node_core::generator;
use floe_node_core::kafka_connector::{
    KafkaConnector, KafkaConnectorConfig, KafkaOffsetCommit, KafkaReplayRange,
    KafkaTopicPartitionOffset,
};
use floe_node_core::object_store_connector::{ObjectStoreConnector, ObjectStoreConnectorConfig};
use floe_node_core::planner::{
    PlannedMaterializedView, camel_case_schema, plan_materialized_views,
};
use floe_node_core::postgres_cdc::{
    PostgresCdcCommit, PostgresCdcSourceConfig, PostgresSlotCommit, default_postgres_publication,
    replication_config_from_connection_string, stored_slot_start_lsn,
};
use floe_node_core::tail_client;
use floe_server as server;
use floe_sql_parser::{
    CreateSourceDefinition, CreateTableDefinition, FloeStatement, MaterializedViewDefinition,
    ReplicationPipelineDefinition as SqlReplicationPipelineDefinition, SourceConnector,
    parse_floe_program,
};
use floe_storage::MaterializedViewMetadata;
use slatedb::WriteBatch;
use slatedb::config::{CompactorOptions, Settings};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{cli, http_ingest, metrics, sinks};
use floe_config as config;
use floe_config::{
    ConnectorConfig, MvFlushConfig, MvSnapshotConfig, NodeConfig, OutputConsolidationModeConfig,
    PostgresCdcReconnectConfig, PostgresCdcSnapshotConfig, SinkConfig, SinkSpec,
    SourceJournalConfig, apply_connector_properties, load_config,
    materialized_view_definitions_from_config, normalize_connectors, normalize_sinks,
    sink_spec_from_sql,
};

static TICK_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
static INGEST_METRICS_COUNTER: AtomicU64 = AtomicU64::new(0);
const TICK_LOG_SAMPLE_EVERY: u64 = 128;
const INGEST_METRICS_SAMPLE_EVERY: u64 = 128;
const DEFAULT_EVENTS_PER_SECOND: f64 = 10.0;
const DEFAULT_MV_RETAIN_LAST: usize = 1;
const DEFAULT_ZSET_COMPACTION_MAX_CHAIN_LEN: usize = 512;
const DEFAULT_ZSET_COMPACTION_MAX_SEGMENTS: usize = 4096;
const DEFAULT_ZSET_COMPACTION_BACKOFF_TICKS: u64 = 1;
const DEFAULT_ZSET_COMPACTION_MAX_CONCURRENT_JOBS: usize = 1;
const DEFAULT_ZSET_GC_GRACE_PERIOD_MS: u64 = 30_000;
const DEFAULT_HTTP_HOST: &str = "127.0.0.1";
const DEFAULT_KAFKA_GROUP_ID: &str = "floe";
const DEFAULT_KAFKA_POLL_MS: u64 = 100;
const DEFAULT_KAFKA_MAX_MESSAGES: usize = 256;
const DEFAULT_INGEST_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_INGEST_BATCH_SIZE: usize = 256;
const DEFAULT_INGEST_BATCH_PER_SOURCE: usize = 64;
const DEFAULT_INGEST_BATCH_PER_CONNECTOR: usize = 64;
const DEFAULT_PGWIRE_ADDR: &str = "127.0.0.1:6432";
const DEFAULT_ADMIN_PORT: u16 = 8081;
const CHECKPOINT_GRAPH_ID: &str = "floe_runtime";
const SOURCE_PRIMARY_KEY_PROPERTY: &str = "primary_key";
const DEFAULT_SLATEDB_CLOSE_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_WATERMARK_IDLE_SOURCE_MS: u64 = 30_000;

#[derive(Debug)]
struct ReconnectablePostgresCdcError(anyhow::Error);

impl fmt::Display for ReconnectablePostgresCdcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:#}", self.0)
    }
}

impl std::error::Error for ReconnectablePostgresCdcError {}

fn reconnectable_postgres_cdc_error(err: anyhow::Error) -> anyhow::Error {
    anyhow!(ReconnectablePostgresCdcError(err))
}

fn is_reconnectable_postgres_cdc_error(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|source| source.is::<ReconnectablePostgresCdcError>())
}

struct ConnectorQueue {
    id: usize,
    name: String,
    pending: VecDeque<QueuedAppendIngestEvent>,
}

struct QueuedAppendIngestEvent {
    event: core_source::AppendIngestEvent,
    commit_ack: Option<core_source::CommitAck>,
}

struct SelectedAppendIngestEvent {
    source_id: Option<usize>,
    event: core_source::AppendIngestEvent,
    commit_ack: Option<core_source::CommitAck>,
}

struct BatchSelection {
    batch: Vec<SelectedAppendIngestEvent>,
    per_connector_counts: Vec<usize>,
}

struct QueuedCdcTransaction {
    slot: String,
    source_id: CdcSourceId,
    transaction: TransactionBatch,
}

struct BufferedPostgresWalStream {
    slot: String,
    snapshot_lsn: PostgresLsn,
    release_feedback_tx: watch::Sender<bool>,
    receiver: mpsc::Receiver<QueuedCdcTransaction>,
    task: JoinHandle<anyhow::Result<()>>,
}

struct InitialPostgresSnapshot {
    lsn: Option<PostgresLsn>,
    wal_stream: Option<BufferedPostgresWalStream>,
}

impl std::fmt::Debug for InitialPostgresSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InitialPostgresSnapshot")
            .field("lsn", &self.lsn)
            .field("has_wal_stream", &self.wal_stream.is_some())
            .finish()
    }
}

#[derive(Clone)]
struct ReplicationPipelineRuntimePlan {
    name: String,
    source_name: String,
    source_connection: String,
    database_name: String,
    upstream_table: String,
    table_id: CdcTableId,
    schema: CdcTableSchema,
    schema_evolution_policy: PostgresSchemaEvolutionPolicy,
    target: ReplicationPipelineRuntimeTarget,
    format: ReplicationPipelineRuntimeFormat,
    buffer_mode: ReplicationPipelineRuntimeBufferMode,
    buffer_policy: CatalogReplicationBufferPolicy,
    error_policy: CatalogReplicationErrorPolicy,
    emit_tombstones: bool,
    include_transaction_metadata: bool,
}

#[derive(Clone)]
enum ReplicationPipelineRuntimeTarget {
    Kafka { brokers: String, topic: String },
    Postgres { connection: String, table: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReplicationPipelineRuntimeFormat {
    FloeJson,
    DebeziumJson,
    ArrowIpc,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReplicationPipelineRuntimeBufferMode {
    Durable,
    NoBuffer,
}

#[derive(Clone)]
struct PostgresCdcRuntimePlan {
    source_id: CdcSourceId,
    schemas: HashMap<CdcTableId, CdcTableSchema>,
    schema_evolution_policy: PostgresSchemaEvolutionPolicy,
    replication_pipelines: Vec<ReplicationPipelineRuntimePlan>,
}

impl ConnectorQueue {
    fn new(id: usize, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            pending: VecDeque::new(),
        }
    }
}

use crate::http_ingest::{HttpAdminConfig, HttpIngestConfig, HttpIngestHealth};
use floe_node_core::executor::{
    StreamCompactionConfig, StreamGcConfig, available_sources_from_registry, build_dataflows,
};
use floe_node_core::source as core_source;
use floe_node_core::source::SourceRegistry;

mod catalog;
mod command;
mod ingest;
mod orchestration;
mod postgres_snapshot;
mod replication;
mod shutdown;
mod startup;

#[cfg(test)]
mod tests;

use catalog::*;
use command::*;
use ingest::*;
pub(crate) use orchestration::run;
use replication::*;
pub(crate) use replication::{
    ReplicationPipelineReconciliationOptions, ReplicationPipelineRuntime,
};
use shutdown::*;
use startup::*;

async fn initialize_postgres_cdc_debug_sources<'a>(
    shared: &Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    plans: impl IntoIterator<Item = &'a PostgresCdcRuntimePlan>,
) {
    let mut next_sources = plans
        .into_iter()
        .map(|plan| http_ingest::PostgresCdcDebugSourceState {
            source: plan.source_id.as_str().to_string(),
            schema_evolution_policy: plan.schema_evolution_policy.as_str().to_string(),
            ..http_ingest::PostgresCdcDebugSourceState::default()
        })
        .collect::<Vec<_>>();
    next_sources.sort_by(|left, right| left.source.cmp(&right.source));
    next_sources.dedup_by(|left, right| left.source == right.source);

    let mut state = shared.write().await;
    for next_source in &mut next_sources {
        if let Some(existing_source) = state
            .postgres_sources
            .iter()
            .find(|source| source.source == next_source.source)
        {
            next_source.latest_schema_evolution = existing_source.latest_schema_evolution.clone();
            next_source.slot = existing_source.slot.clone();
            next_source.upstream_lsn = existing_source.upstream_lsn.clone();
            next_source.upstream_lsn_bytes = existing_source.upstream_lsn_bytes;
            next_source.durable_lsn = existing_source.durable_lsn.clone();
            next_source.durable_lsn_bytes = existing_source.durable_lsn_bytes;
            next_source.source_lag_bytes = existing_source.source_lag_bytes;
            next_source.connected = existing_source.connected;
            next_source.reconnect_attempts = existing_source.reconnect_attempts;
            next_source.last_error = existing_source.last_error.clone();
        }
    }
    state.postgres_sources = next_sources;
    state.updated_at_unix_ms = current_unix_time_ms();
}

fn record_postgres_cdc_debug_connection_state(
    shared: &Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    source: &str,
    slot: &str,
    connected: bool,
    reconnect_attempts: u64,
    last_error: Option<String>,
) {
    let Ok(mut state) = shared.try_write() else {
        return;
    };
    let source_idx = match state
        .postgres_sources
        .iter()
        .position(|source_state| source_state.source == source)
    {
        Some(source_idx) => source_idx,
        None => {
            state
                .postgres_sources
                .push(http_ingest::PostgresCdcDebugSourceState {
                    source: source.to_string(),
                    slot: Some(slot.to_string()),
                    ..http_ingest::PostgresCdcDebugSourceState::default()
                });
            state.postgres_sources.len() - 1
        }
    };
    let source_state = state
        .postgres_sources
        .get_mut(source_idx)
        .expect("Postgres CDC debug source index is valid");
    source_state.slot = Some(slot.to_string());
    source_state.connected = connected;
    source_state.reconnect_attempts = reconnect_attempts;
    source_state.last_error = last_error;
    state.updated_at_unix_ms = current_unix_time_ms();
}

async fn record_postgres_schema_evolution_observations(
    shared: &Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    source_id: &CdcSourceId,
    observations: Vec<PostgresSchemaEvolutionObservation>,
) {
    if observations.is_empty() {
        return;
    }

    let observed_at_unix_ms = current_unix_time_ms();
    for observation in &observations {
        metrics::record_postgres_cdc_schema_evolution_observation(
            source_id.as_str(),
            observation.table_id().as_str(),
            observation.outcome().as_str(),
            observation.policy().as_str(),
            observed_at_unix_ms,
        );
    }

    let mut state = shared.write().await;
    for observation in observations {
        let source_idx = match state
            .postgres_sources
            .iter()
            .position(|source| source.source == source_id.as_str())
        {
            Some(source_idx) => source_idx,
            None => {
                state
                    .postgres_sources
                    .push(http_ingest::PostgresCdcDebugSourceState {
                        source: source_id.as_str().to_string(),
                        schema_evolution_policy: observation.policy().as_str().to_string(),
                        ..http_ingest::PostgresCdcDebugSourceState::default()
                    });
                state.postgres_sources.len() - 1
            }
        };
        let source_state = state
            .postgres_sources
            .get_mut(source_idx)
            .expect("Postgres CDC debug source index is valid");
        source_state.schema_evolution_policy = observation.policy().as_str().to_string();
        source_state.latest_schema_evolution = Some(postgres_schema_evolution_debug_state(
            &observation,
            observed_at_unix_ms,
        ));
    }
    state
        .postgres_sources
        .sort_by(|left, right| left.source.cmp(&right.source));
    state.updated_at_unix_ms = observed_at_unix_ms;
}

fn record_postgres_cdc_debug_lsn(
    shared: &Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    source: &str,
    slot: &str,
    upstream_lsn: Option<u64>,
    durable_lsn: Option<u64>,
) {
    let Ok(mut state) = shared.try_write() else {
        return;
    };
    let source_idx = match state
        .postgres_sources
        .iter()
        .position(|source_state| source_state.source == source)
    {
        Some(source_idx) => source_idx,
        None => {
            state
                .postgres_sources
                .push(http_ingest::PostgresCdcDebugSourceState {
                    source: source.to_string(),
                    slot: Some(slot.to_string()),
                    ..http_ingest::PostgresCdcDebugSourceState::default()
                });
            state.postgres_sources.len() - 1
        }
    };
    let source_state = state
        .postgres_sources
        .get_mut(source_idx)
        .expect("Postgres CDC debug source index is valid");
    source_state.slot = Some(slot.to_string());
    if let Some(upstream_lsn) = upstream_lsn {
        let upstream_lsn = source_state
            .upstream_lsn_bytes
            .unwrap_or(0)
            .max(upstream_lsn);
        source_state.upstream_lsn_bytes = Some(upstream_lsn);
        source_state.upstream_lsn = Some(PostgresLsn::from_u64(upstream_lsn).to_pg_string());
    }
    if let Some(durable_lsn) = durable_lsn {
        let durable_lsn = source_state.durable_lsn_bytes.unwrap_or(0).max(durable_lsn);
        source_state.durable_lsn_bytes = Some(durable_lsn);
        source_state.durable_lsn = Some(PostgresLsn::from_u64(durable_lsn).to_pg_string());
    }
    source_state.source_lag_bytes = match (
        source_state.upstream_lsn_bytes,
        source_state.durable_lsn_bytes,
    ) {
        (Some(upstream_lsn), Some(durable_lsn)) => Some(upstream_lsn.saturating_sub(durable_lsn)),
        _ => None,
    };
    state.updated_at_unix_ms = current_unix_time_ms();
}

fn postgres_schema_evolution_debug_state(
    observation: &PostgresSchemaEvolutionObservation,
    observed_at_unix_ms: u64,
) -> http_ingest::PostgresCdcSchemaEvolutionDebugState {
    http_ingest::PostgresCdcSchemaEvolutionDebugState {
        table: observation.table_id().as_str().to_string(),
        upstream_table: format!(
            "{}.{}",
            observation.upstream_table().schema(),
            observation.upstream_table().table()
        ),
        policy: observation.policy().as_str().to_string(),
        outcome: observation.outcome().as_str().to_string(),
        added_columns: observation.added_columns().to_vec(),
        reason: observation.reason().map(str::to_string),
        catalog_schema_version: observation.catalog_schema_version(),
        observed_schema_version: observation.observed_schema_version(),
        observed_at_unix_ms,
    }
}
