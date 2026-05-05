use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use clap::Parser;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::DFSchemaRef;
use dbsp::collections::CompactionPolicy;
use dbsp::storage::gc::{GcPolicy, GcService};
use dbsp::storage::{KeyValueTable, SlateTable};
use dbsp::{CompactionSchedulerConfig, StreamRetention};
use floe_core::catalog::{ColumnDefinition, ColumnType, TableDefinition};
use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};
use floe_executor::checkpoint::{
    CheckpointManager, KafkaCheckpointOffset, MaterializedViewTickVersion, SinkCursor, TickCommit,
};
use floe_executor::source_journal::SourceBatchJournal;
use floe_executor::{
    BuildInputs, ConsolidationMode, DbspBridge, DbspGraphBuilder, FloeQueryContext, GraphTaskError,
    MaterializedViewRegistry, MaterializedViewTableProvider, MvFlushCoalescingConfig,
    OuterStreamRegistry, OverlaySnapshotConfig, SourceRowDecoder, SourceTableProvider,
    ValidatedPlan, plan_source_requirements, source_batch_journal_root_sources, validate_dbsp_plan,
};
use floe_node_core::connector::{ConnectorContext, run_connector};
use floe_node_core::file_connector::{FileConnector, FileConnectorConfig};
#[cfg(test)]
use floe_node_core::generator;
use floe_node_core::kafka_connector::{
    KafkaConnector, KafkaConnectorConfig, KafkaOffsetCommit, KafkaTopicPartitionOffset,
};
use floe_node_core::object_store_connector::{ObjectStoreConnector, ObjectStoreConnectorConfig};
use floe_node_core::planner::{
    PlannedMaterializedView, camel_case_schema, plan_materialized_views,
};
use floe_node_core::postgres_cdc_connector::{
    PostgresCdcCommit, PostgresCdcConnector, PostgresCdcConnectorConfig, PostgresSlotCommit,
};
use floe_node_core::tail_client;
use floe_server as server;
use floe_sql_parser::{
    CreateTableDefinition, FloeStatement, MaterializedViewDefinition, SqlColumnType,
    parse_floe_program,
};
use floe_storage::MaterializedViewMetadata;
use slatedb::config::{CompactorOptions, Settings};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::{
    ConnectorConfig, MvFlushConfig, MvSnapshotConfig, NodeConfig, OutputConsolidationModeConfig,
    SinkConfig, SinkSpec, SourceJournalConfig, apply_connector_properties, load_config,
    materialized_view_definitions_from_config, normalize_connectors, normalize_sinks,
    sink_spec_from_sql,
};
use crate::{cli, config, http_ingest, metrics, sinks};

static TICK_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
static INGEST_METRICS_COUNTER: AtomicU64 = AtomicU64::new(0);
const TICK_LOG_SAMPLE_EVERY: u64 = 128;
const INGEST_METRICS_SAMPLE_EVERY: u64 = 128;
const SLATEDB_CONFIG_ENV: &str = "FLOE_SLATEDB_CONFIG";
const SLATEDB_ENV_PREFIX_ENV: &str = "FLOE_SLATEDB_ENV_PREFIX";
const DEFAULT_SLATEDB_ENV_PREFIX: &str = "SLATEDB_";
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
const DEFAULT_ADMIN_PORT: u16 = 8081;
const CHECKPOINT_GRAPH_ID: &str = "floe_runtime";
const SOURCE_PRIMARY_KEY_PROPERTY: &str = "primary_key";
const ADMIN_PORT_ENV: &str = "FLOE_ADMIN_PORT";
const SLATEDB_CLOSE_TIMEOUT_MS_ENV: &str = "FLOE_SLATEDB_CLOSE_TIMEOUT_MS";
const DEFAULT_SLATEDB_CLOSE_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_WATERMARK_IDLE_SOURCE_MS: u64 = 30_000;

struct ConnectorQueue {
    id: usize,
    name: String,
    pending: VecDeque<core_source::SourceEvent>,
}

struct SelectedSourceEvent {
    source_id: Option<usize>,
    event: core_source::SourceEvent,
}

struct BatchSelection {
    batch: Vec<SelectedSourceEvent>,
    per_connector_counts: Vec<usize>,
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
mod shutdown;
mod startup;

#[cfg(test)]
mod tests;

use catalog::*;
use command::*;
use ingest::*;
pub(crate) use orchestration::run;
use shutdown::*;
use startup::*;
