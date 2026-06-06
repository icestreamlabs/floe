use super::*;

pub(in crate::node_runtime::orchestration) struct ExecutorTaskContext {
    pub(in crate::node_runtime::orchestration) runtime: ExecutorRuntimeContext,
    pub(in crate::node_runtime::orchestration) sources: ExecutorSourceContext,
    pub(in crate::node_runtime::orchestration) cdc: ExecutorCdcContext,
    pub(in crate::node_runtime::orchestration) ingest: ExecutorIngestContext,
    pub(in crate::node_runtime::orchestration) checkpoint: ExecutorCheckpointContext,
    pub(in crate::node_runtime::orchestration) limits: ExecutorBatchLimits,
}

pub(in crate::node_runtime::orchestration) struct ExecutorRuntimeContext {
    pub(in crate::node_runtime::orchestration) event_watermark: Arc<AtomicI64>,
    pub(in crate::node_runtime::orchestration) mv_registry: Arc<MaterializedViewRegistry>,
    pub(in crate::node_runtime::orchestration) vectorized_runtime: VectorizedExecutionRuntime,
    pub(in crate::node_runtime::orchestration) runtime_cancel: CancellationToken,
    pub(in crate::node_runtime::orchestration) executor_running: Arc<AtomicBool>,
    pub(in crate::node_runtime::orchestration) runtime_failure: Arc<StdMutex<Option<String>>>,
}

pub(in crate::node_runtime::orchestration) struct ExecutorSourceContext {
    pub(in crate::node_runtime::orchestration) active_source_definitions_by_id:
        Arc<Vec<Option<SourceDefinition>>>,
    pub(in crate::node_runtime::orchestration) required_columns_by_source_id:
        Arc<Vec<Option<Arc<[bool]>>>>,
    pub(in crate::node_runtime::orchestration) query_batches_by_source_id: Arc<Vec<bool>>,
    pub(in crate::node_runtime::orchestration) materialized_source_ids: Arc<Vec<bool>>,
    pub(in crate::node_runtime::orchestration) source_names_by_id: Arc<Vec<String>>,
    pub(in crate::node_runtime::orchestration) source_id_by_name: HashMap<String, usize>,
    pub(in crate::node_runtime::orchestration) definitions: Vec<SourceDefinition>,
    pub(in crate::node_runtime::orchestration) kafka_metadata_journal_source_ids: Arc<Vec<usize>>,
    pub(in crate::node_runtime::orchestration) source_journal_required_sources:
        Arc<BTreeSet<String>>,
}

pub(in crate::node_runtime::orchestration) struct ExecutorCdcContext {
    pub(in crate::node_runtime::orchestration) cdc_table_store: CdcTableStore,
    pub(in crate::node_runtime::orchestration) cdc_schemas_by_source_id:
        Arc<HashMap<CdcSourceId, HashMap<CdcTableId, CdcTableSchema>>>,
    pub(in crate::node_runtime::orchestration) cdc_stateful_table_ids_by_source_id:
        Arc<HashMap<CdcSourceId, HashSet<CdcTableId>>>,
    pub(in crate::node_runtime::orchestration) cdc_transaction_receiver:
        mpsc::Receiver<QueuedCdcTransaction>,
    pub(in crate::node_runtime::orchestration) cdc_replication_debug:
        Arc<tokio::sync::RwLock<http_ingest::CdcReplicationDebugState>>,
    pub(in crate::node_runtime::orchestration) postgres_cdc_commit_senders:
        Vec<watch::Sender<PostgresCdcCommit>>,
    pub(in crate::node_runtime::orchestration) storage: Arc<floe_storage::SlateCatalog>,
    pub(in crate::node_runtime::orchestration) replication_pipeline_runtime:
        Arc<ReplicationPipelineRuntime>,
}

pub(in crate::node_runtime::orchestration) struct ExecutorIngestContext {
    pub(in crate::node_runtime::orchestration) connector_receiver:
        core_source::RoutedAppendIngestEventReceiver,
    pub(in crate::node_runtime::orchestration) connector_queues: Vec<ConnectorQueue>,
    pub(in crate::node_runtime::orchestration) kafka_commit_senders:
        Vec<watch::Sender<KafkaOffsetCommit>>,
    pub(in crate::node_runtime::orchestration) pending_event_counter:
        core_source::PendingAppendIngestEventCounter,
}

pub(in crate::node_runtime::orchestration) struct ExecutorCheckpointContext {
    pub(in crate::node_runtime::orchestration) sink_checkpoint_rx: mpsc::Receiver<SinkCursor>,
    pub(in crate::node_runtime::orchestration) checkpoint_manager: CheckpointManager,
    pub(in crate::node_runtime::orchestration) tracked_mv_names: Vec<String>,
    pub(in crate::node_runtime::orchestration) watermark_debug:
        Arc<tokio::sync::RwLock<http_ingest::WatermarkDebugState>>,
    pub(in crate::node_runtime::orchestration) watermark_idle_source_ms: u64,
}

pub(in crate::node_runtime::orchestration) struct ExecutorBatchLimits {
    pub(in crate::node_runtime::orchestration) max_batch: usize,
    pub(in crate::node_runtime::orchestration) max_batch_per_source: usize,
    pub(in crate::node_runtime::orchestration) max_batch_per_connector: usize,
}
