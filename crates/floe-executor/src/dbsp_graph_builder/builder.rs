use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use async_recursion::async_recursion;
use datafusion::common::Column;
use datafusion::logical_expr::Expr;
use dbsp::circuit::plan::DbspProjectExpr;
use dbsp::collections::CompactionPolicy;
use dbsp::handles::ZSetHandle;
use dbsp::storage::KeyValueTable;
use dbsp::storage::gc::{GcPolicy, SweepStats};
use dbsp::stream::DeltaHandleStream;
use dbsp::{
    CircuitNode, CircuitPlan, CompactionSchedulerConfig, DbspAggregateNode, DbspExpression,
    DbspNodeKind, DbspScalarType, DbspTopNNode, RowSchema, StreamRetention,
};
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::dbsp_bridge::{DbspBridge, NamespaceStorageSummary};
use crate::dbsp_plan::{
    DbspProjectNode, DbspSelectNode, DbspSourceNode, ValidatedPlan, validate_dbsp_plan,
};
use crate::delta_consolidation::ConsolidationMode;
use crate::encoding::{
    EncodedRowProjectionColumn, EncodedRowProjectionSource, EncodedRowScalar, concat_encoded_rows,
    extract_encoded_row_columns, extract_encoded_row_columns_and_i64_like_column,
    extract_encoded_row_scalar,
};
use crate::materialized_view::MaterializedViewRegistry;
use crate::outer_stream::TransientSourceHandleStream;
use crate::task_events::{GraphTaskSender, report_graph_task_error};

use super::compile::{
    build_count_aggregate_slot_kinds, build_count_row_evaluator,
    build_incremental_aggregate_row_evaluator, build_incremental_aggregate_slot_kinds,
};
use super::materialize::{DeltaTransformFn, TransientMaterializeBatch};
use super::persistence_policy::{PersistencePolicy, TransientSegmentSpec, TransientSegmentStep};
use super::vectorized_filter_project::{
    VectorizedFilterProjectEvaluator, required_encoded_input_columns,
};

type ClosedJoinKeyTransformFn =
    dyn Fn(&[(Vec<u8>, i64)]) -> Result<Vec<(Vec<u8>, i64)>> + Send + Sync + 'static;

/// Orchestrates compilation of a [`CircuitPlan`] into DBSP streams backed by SlateDB.
pub struct DbspGraphBuilder {
    pub(super) bridge: Arc<Mutex<DbspBridge>>,
    ns: GraphNamespace,
    pub(super) watermark: Arc<AtomicI64>,
    output_consolidation_mode: ConsolidationMode,
    pub(super) mv_flush_coalescing: MvFlushCoalescingConfig,
    pub(super) mv_overlay_snapshot: OverlaySnapshotConfig,
}

#[derive(Debug, Clone, Copy)]
pub struct MvFlushCoalescingConfig {
    pub enabled: bool,
    pub max_pending_deltas: usize,
    pub max_pending_versions: Option<usize>,
    pub max_pending_rows: Option<usize>,
    pub max_pending_bytes: Option<usize>,
    pub max_delay_ms: Option<u64>,
    pub flush_on_catchup_boundary: bool,
    pub flush_on_shutdown: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct OverlaySnapshotConfig {
    pub max_pending_batches: usize,
    pub max_pending_rows: usize,
    pub max_delay_ms: u64,
}

impl Default for OverlaySnapshotConfig {
    fn default() -> Self {
        Self {
            max_pending_batches: 16_384,
            max_pending_rows: 1_000_000,
            max_delay_ms: 10_000,
        }
    }
}

impl Default for MvFlushCoalescingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_pending_deltas: 1,
            max_pending_versions: None,
            max_pending_rows: None,
            max_pending_bytes: None,
            max_delay_ms: None,
            flush_on_catchup_boundary: true,
            flush_on_shutdown: true,
        }
    }
}

impl DbspGraphBuilder {
    pub async fn new(db: Arc<slatedb::Db>) -> Result<Self> {
        crate::metrics::init();
        let bridge = DbspBridge::new(db).await?;
        Ok(Self {
            bridge: Arc::new(Mutex::new(bridge)),
            ns: GraphNamespace::default(),
            watermark: Arc::new(AtomicI64::new(-1)),
            output_consolidation_mode: ConsolidationMode::ByAllColumns,
            mv_flush_coalescing: MvFlushCoalescingConfig::default(),
            mv_overlay_snapshot: OverlaySnapshotConfig::default(),
        })
    }

    pub fn set_output_consolidation_mode(&mut self, mode: ConsolidationMode) {
        self.output_consolidation_mode = mode;
    }

    pub fn set_mv_flush_coalescing(&mut self, config: MvFlushCoalescingConfig) {
        let mut sanitized = config;
        if sanitized.max_pending_deltas == 0 {
            sanitized.max_pending_deltas = 1;
        }
        self.mv_flush_coalescing = sanitized;
    }

    pub fn set_mv_overlay_snapshot(&mut self, config: OverlaySnapshotConfig) {
        let mut sanitized = config;
        if sanitized.max_pending_batches == 0 {
            sanitized.max_pending_batches = 1;
        }
        if sanitized.max_pending_rows == 0 {
            sanitized.max_pending_rows = 1;
        }
        if sanitized.max_delay_ms == 0 {
            sanitized.max_delay_ms = 1;
        }
        self.mv_overlay_snapshot = sanitized;
    }

    pub async fn set_stream_compaction(
        &mut self,
        policy: CompactionPolicy,
        scheduler: CompactionSchedulerConfig,
    ) {
        let mut bridge = self.bridge.lock().await;
        bridge.set_stream_compaction_policy(policy);
        bridge.set_stream_compaction_scheduler_config(scheduler);
    }

    pub async fn pause_maintenance(&mut self) {
        let mut bridge = self.bridge.lock().await;
        bridge.pause_maintenance();
    }

    pub async fn resume_maintenance(&mut self) {
        let mut bridge = self.bridge.lock().await;
        bridge.resume_maintenance();
    }

    pub async fn maintenance_paused(&self) -> bool {
        let bridge = self.bridge.lock().await;
        bridge.maintenance_paused()
    }

    pub async fn inspect_namespace_storage(
        &self,
        namespace: &str,
    ) -> Result<NamespaceStorageSummary> {
        let bridge = self.bridge.lock().await;
        bridge.inspect_namespace_storage(namespace).await
    }

    pub async fn run_namespace_compaction_once(&mut self, namespace: &str) -> Result<Option<u64>> {
        let mut bridge = self.bridge.lock().await;
        bridge.compact_namespace_once(namespace).await
    }

    pub async fn run_namespace_gc_once(
        &self,
        namespace: &str,
        policy: GcPolicy,
    ) -> Result<SweepStats> {
        let bridge = self.bridge.lock().await;
        bridge.run_namespace_gc_once(namespace, policy).await
    }

    pub async fn build(&mut self, inputs: BuildInputs<'_>) -> Result<BuildOutputs> {
        self.ns.set_graph_id(inputs.graph_id);
        self.watermark = Arc::clone(&inputs.watermark);
        let available_sources: BTreeSet<String> =
            inputs.outer_handle_streams.keys().cloned().collect();
        let ValidatedPlan {
            required_sources, ..
        } = validate_dbsp_plan(inputs.plan, &available_sources, inputs.view_name)
            .context("validating query plan before DBSP graph build")?;
        let mut built = HashMap::new();
        let mut mv_latest = HashMap::new();
        let persistence_policy = PersistencePolicy::for_plan(inputs.plan);
        tracing::info!(
            graph_id = %self.graph_id(),
            transient_max_segment_nodes = persistence_policy.max_transient_segment_nodes(),
            transient_min_segment_score = persistence_policy.min_transient_segment_score(),
            "persistence policy configured"
        );
        let root_node = inputs
            .plan
            .node(inputs.plan.root)
            .with_context(|| anyhow!("root node {} missing from circuit plan", inputs.plan.root))?;
        let transient_state_table = if inputs.restore_transient_helper_state {
            let bridge = self.bridge.lock().await;
            Some(bridge.table())
        } else {
            None
        };

        if !matches!(root_node.kind, DbspNodeKind::Sink(_)) && inputs.enable_source_batch_journal {
            if let Some(transient_window_root) =
                try_build_transient_source_window_count_star_root_materialization(
                    inputs.plan,
                    inputs.plan.root,
                    inputs.outer_transient_streams,
                    Arc::clone(&self.watermark),
                    &inputs.cancel,
                    &inputs.task_events,
                    self.graph_id(),
                    transient_state_table.clone(),
                )
                .await?
            {
                tracing::info!(
                    graph_id = %self.graph_id(),
                    view = %inputs.view_name,
                    source = %transient_window_root.source_name,
                    optimized_nodes = ?transient_window_root.optimized_nodes,
                    "using transient window count-star root materialization with source batch journal"
                );
                self.materialize_view_from_transient_overlay_receiver(
                    inputs.view_name,
                    Arc::clone(&root_node.output_schema),
                    transient_window_root.receiver,
                    None,
                    &inputs.cancel,
                    &inputs.task_events,
                    &inputs.mv_registry,
                )
                .await?;
                return Ok(BuildOutputs {
                    node_streams: built,
                    mv_latest,
                    required_sources,
                });
            }
            if let Some(transient_window_root) =
                try_build_transient_source_window_aggregate_root_materialization(
                    inputs.plan,
                    inputs.plan.root,
                    inputs.outer_transient_streams,
                    Arc::clone(&self.watermark),
                    &inputs.cancel,
                    &inputs.task_events,
                    self.graph_id(),
                    transient_state_table.clone(),
                )
                .await?
            {
                tracing::info!(
                    graph_id = %self.graph_id(),
                    view = %inputs.view_name,
                    source = %transient_window_root.source_name,
                    optimized_nodes = ?transient_window_root.optimized_nodes,
                    "using transient window aggregate root materialization with source batch journal"
                );
                self.materialize_view_from_transient_overlay_receiver(
                    inputs.view_name,
                    Arc::clone(&root_node.output_schema),
                    transient_window_root.receiver,
                    None,
                    &inputs.cancel,
                    &inputs.task_events,
                    &inputs.mv_registry,
                )
                .await?;
                return Ok(BuildOutputs {
                    node_streams: built,
                    mv_latest,
                    required_sources,
                });
            }
            if let Some(transient_aggregate_root) =
                try_build_transient_source_aggregate_root_materialization(
                    inputs.plan,
                    inputs.plan.root,
                    inputs.outer_transient_streams,
                    &inputs.cancel,
                    &inputs.task_events,
                    self.graph_id(),
                    transient_state_table.clone(),
                )
                .await?
            {
                tracing::info!(
                    graph_id = %self.graph_id(),
                    view = %inputs.view_name,
                    source = %transient_aggregate_root.source_name,
                    optimized_nodes = ?transient_aggregate_root.optimized_nodes,
                    "using transient aggregate root materialization with source batch journal"
                );
                self.materialize_view_from_transient_overlay_receiver(
                    inputs.view_name,
                    Arc::clone(&root_node.output_schema),
                    transient_aggregate_root.receiver,
                    None,
                    &inputs.cancel,
                    &inputs.task_events,
                    &inputs.mv_registry,
                )
                .await?;
                return Ok(BuildOutputs {
                    node_streams: built,
                    mv_latest,
                    required_sources,
                });
            }
            if let Some(transient_topn_root) = try_build_transient_source_topn_root_materialization(
                inputs.plan,
                inputs.plan.root,
                inputs.outer_transient_streams,
                &inputs.cancel,
                &inputs.task_events,
                self.graph_id(),
                transient_state_table.clone(),
            )? {
                tracing::info!(
                    graph_id = %self.graph_id(),
                    view = %inputs.view_name,
                    source = %transient_topn_root.source_name,
                    optimized_nodes = ?transient_topn_root.optimized_nodes,
                    "using transient topn root materialization with source batch journal"
                );
                self.materialize_view_from_transient_overlay_receiver(
                    inputs.view_name,
                    Arc::clone(&root_node.output_schema),
                    transient_topn_root.receiver,
                    transient_topn_root.transform.clone(),
                    &inputs.cancel,
                    &inputs.task_events,
                    &inputs.mv_registry,
                )
                .await?;
                return Ok(BuildOutputs {
                    node_streams: built,
                    mv_latest,
                    required_sources,
                });
            }
            if let Some(transient_root) =
                try_build_transient_source_root_materialization(inputs.plan, inputs.plan.root)?
            {
                if let Some(upstream) = inputs
                    .outer_transient_streams
                    .get(&transient_root.source_name)
                    .cloned()
                {
                    tracing::info!(
                        graph_id = %self.graph_id(),
                        view = %inputs.view_name,
                        source = %transient_root.source_name,
                        optimized_nodes = ?transient_root.optimized_nodes,
                        "using transient source root materialization with source batch journal"
                    );
                    self.materialize_view_from_transient_source_overlay(
                        inputs.view_name,
                        Arc::clone(&root_node.output_schema),
                        upstream,
                        transient_root.transform,
                        &inputs.cancel,
                        &inputs.task_events,
                        &inputs.mv_registry,
                    )
                    .await?;
                    return Ok(BuildOutputs {
                        node_streams: built,
                        mv_latest,
                        required_sources,
                    });
                }
            }
            if let Some(transient_join_pipeline_root) =
                try_build_transient_join_pipeline_root_materialization(
                    inputs.plan,
                    inputs.plan.root,
                )?
                && transient_join_pipeline_root
                    .steps
                    .iter()
                    .any(|step| !matches!(step, TransientJoinPipelineStep::Transform(_)))
            {
                tracing::info!(
                    graph_id = %self.graph_id(),
                    view = %inputs.view_name,
                    left_source = %transient_join_pipeline_root.left_source_root.source_name,
                    right_source = %transient_join_pipeline_root.right_source_root.source_name,
                    optimized_nodes = ?transient_join_pipeline_root.optimized_nodes,
                    "using transient join pipeline root materialization with source batch journal"
                );
                let receiver = self
                    .build_transient_join_pipeline_root_receiver(
                        inputs.plan,
                        &transient_join_pipeline_root,
                        inputs.outer_handle_streams,
                        inputs.outer_transient_streams,
                        &inputs.cancel,
                        &inputs.task_events,
                        &mut built,
                        &inputs.mv_registry,
                        &mut mv_latest,
                        inputs.mv_retention,
                        &persistence_policy,
                        transient_state_table.clone(),
                    )
                    .await?;
                self.materialize_view_from_transient_overlay_receiver(
                    inputs.view_name,
                    Arc::clone(&root_node.output_schema),
                    receiver,
                    None,
                    &inputs.cancel,
                    &inputs.task_events,
                    &inputs.mv_registry,
                )
                .await?;
                return Ok(BuildOutputs {
                    node_streams: built,
                    mv_latest,
                    required_sources,
                });
            }
        }

        let root_stream = if !matches!(root_node.kind, DbspNodeKind::Sink(_)) {
            if let Some(transient_opt) = try_build_transient_segment_optimization(
                inputs.plan,
                inputs.plan.root,
                &built,
                self.graph_id(),
                true,
                &persistence_policy,
            )? {
                if inputs.enable_source_batch_journal
                    && let Some(join_node) = inputs.plan.node(transient_opt.durable_input_idx)
                    && let DbspNodeKind::Join(join) = &join_node.kind
                    && matches!(join.join_type, dbsp::DbspJoinType::Inner)
                    && has_single_consumer(inputs.plan, transient_opt.durable_input_idx)
                {
                    let (left_idx, right_idx) = join_inputs(join_node)?;
                    let left = self
                        .compile_node(
                            inputs.plan,
                            left_idx,
                            inputs.outer_handle_streams,
                            &inputs.cancel,
                            &inputs.task_events,
                            &mut built,
                            &inputs.mv_registry,
                            &mut mv_latest,
                            inputs.mv_retention,
                            &persistence_policy,
                        )
                        .await?;
                    let right = self
                        .compile_node(
                            inputs.plan,
                            right_idx,
                            inputs.outer_handle_streams,
                            &inputs.cancel,
                            &inputs.task_events,
                            &mut built,
                            &inputs.mv_registry,
                            &mut mv_latest,
                            inputs.mv_retention,
                            &persistence_policy,
                        )
                        .await?;
                    let mut left_transient_input = try_build_transient_join_input_optimization(
                        self.graph_id(),
                        inputs.plan,
                        left_idx,
                        inputs.outer_transient_streams,
                        None,
                        &inputs.cancel,
                    )?;
                    let mut right_transient_input = try_build_transient_join_input_optimization(
                        self.graph_id(),
                        inputs.plan,
                        right_idx,
                        inputs.outer_transient_streams,
                        None,
                        &inputs.cancel,
                    )?;
                    if left_transient_input.is_some() ^ right_transient_input.is_some() {
                        tracing::info!(
                            graph_id = %self.graph_id(),
                            view = %inputs.view_name,
                            root = inputs.plan.root,
                            "disabling asymmetric transient join inputs for correctness fallback"
                        );
                        left_transient_input = None;
                        right_transient_input = None;
                    }
                    let left_transient_source = left_transient_input
                        .as_ref()
                        .map(|input| input.source_name.clone());
                    let left_transient_nodes = left_transient_input
                        .as_ref()
                        .map(|input| input.optimized_nodes.clone());
                    let right_transient_source = right_transient_input
                        .as_ref()
                        .map(|input| input.source_name.clone());
                    let right_transient_nodes = right_transient_input
                        .as_ref()
                        .map(|input| input.optimized_nodes.clone());
                    let output_projection =
                        try_build_direct_join_output_projection(join, &transient_opt.steps);
                    let direct_output_projection = output_projection.is_some();
                    // CDC-capable sources can retract previously matched rows, so transient
                    // join state must retain both sides unless an explicit append-only source
                    // contract proves that dropping matched rows is safe.
                    let left_retention = dbsp::JoinInputRetention::RetainAll;
                    let right_retention = dbsp::JoinInputRetention::RetainAll;
                    let delta_transform = if direct_output_projection {
                        None
                    } else {
                        Some(Arc::clone(&transient_opt.transform))
                    };
                    let (tx, rx) =
                        tokio::sync::mpsc::unbounded_channel::<TransientMaterializeBatch>();
                    tracing::info!(
                        graph_id = %self.graph_id(),
                        view = %inputs.view_name,
                        root = inputs.plan.root,
                        durable_input_idx = transient_opt.durable_input_idx,
                        optimized_nodes = ?transient_opt.optimized_nodes,
                        segment_score = transient_opt.score,
                        left_transient_source = ?left_transient_source,
                        left_transient_nodes = ?left_transient_nodes,
                        right_transient_source = ?right_transient_source,
                        right_transient_nodes = ?right_transient_nodes,
                        direct_output_projection,
                        left_retention = ?left_retention,
                        right_retention = ?right_retention,
                        "using transient join-to-mv root materialization"
                    );
                    self.materialize_view_from_transient_overlay_receiver(
                        inputs.view_name,
                        Arc::clone(&root_node.output_schema),
                        rx,
                        delta_transform,
                        &inputs.cancel,
                        &inputs.task_events,
                        &inputs.mv_registry,
                    )
                    .await?;
                    self.compile_transient_join_root_materialization(
                        join,
                        left,
                        right,
                        left_transient_input.map(|input| input.receiver),
                        right_transient_input.map(|input| input.receiver),
                        left_retention,
                        right_retention,
                        output_projection,
                        tx,
                        &inputs.task_events,
                        inputs.restore_transient_helper_state,
                    )
                    .await?;
                    return Ok(BuildOutputs {
                        node_streams: built,
                        mv_latest,
                        required_sources,
                    });
                }
                let upstream = self
                    .compile_node(
                        inputs.plan,
                        transient_opt.durable_input_idx,
                        inputs.outer_handle_streams,
                        &inputs.cancel,
                        &inputs.task_events,
                        &mut built,
                        &inputs.mv_registry,
                        &mut mv_latest,
                        inputs.mv_retention,
                        &persistence_policy,
                    )
                    .await?;
                tracing::info!(
                    graph_id = %self.graph_id(),
                    view = %inputs.view_name,
                    root = inputs.plan.root,
                    durable_input_idx = transient_opt.durable_input_idx,
                    optimized_nodes = ?transient_opt.optimized_nodes,
                    segment_score = transient_opt.score,
                    "using transient segment for root materialization"
                );
                self.materialize_view_from_delta_overlay(
                    inputs.view_name,
                    Arc::clone(&root_node.output_schema),
                    upstream,
                    Some(transient_opt.transform),
                    &inputs.cancel,
                    &inputs.task_events,
                    &inputs.mv_registry,
                )
                .await?;
                return Ok(BuildOutputs {
                    node_streams: built,
                    mv_latest,
                    required_sources,
                });
            } else {
                self.compile_node(
                    inputs.plan,
                    inputs.plan.root,
                    inputs.outer_handle_streams,
                    &inputs.cancel,
                    &inputs.task_events,
                    &mut built,
                    &inputs.mv_registry,
                    &mut mv_latest,
                    inputs.mv_retention,
                    &persistence_policy,
                )
                .await?
            }
        } else {
            self.compile_node(
                inputs.plan,
                inputs.plan.root,
                inputs.outer_handle_streams,
                &inputs.cancel,
                &inputs.task_events,
                &mut built,
                &inputs.mv_registry,
                &mut mv_latest,
                inputs.mv_retention,
                &persistence_policy,
            )
            .await?
        };

        if !mv_latest.contains_key(inputs.view_name) {
            self.materialize_view(
                inputs.view_name,
                Arc::clone(&root_node.output_schema),
                root_stream,
                None,
                &inputs.cancel,
                &inputs.task_events,
                &inputs.mv_registry,
                &mut mv_latest,
                inputs.mv_retention,
                self.output_consolidation_mode,
            )
            .await?;
        }

        Ok(BuildOutputs {
            node_streams: built,
            mv_latest,
            required_sources,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_transient_join_pipeline_root_receiver(
        &mut self,
        plan: &CircuitPlan,
        root: &TransientJoinPipelineRootMaterialization,
        outer_handle_streams: &HashMap<String, DeltaHandleStream>,
        outer_transient_streams: &HashMap<String, TransientSourceHandleStream>,
        cancel: &CancellationToken,
        task_events: &GraphTaskSender,
        built: &mut HashMap<usize, DeltaHandleStream>,
        mv_registry: &Arc<MaterializedViewRegistry>,
        mv_latest: &mut HashMap<String, (i64, ZSetHandle)>,
        mv_retention: StreamRetention,
        persistence_policy: &PersistencePolicy,
        state_table: Option<Arc<dyn KeyValueTable>>,
    ) -> Result<mpsc::UnboundedReceiver<TransientMaterializeBatch>> {
        let left = self
            .compile_node(
                plan,
                root.left_input_idx,
                outer_handle_streams,
                cancel,
                task_events,
                built,
                mv_registry,
                mv_latest,
                mv_retention,
                persistence_policy,
            )
            .await?;
        let right = self
            .compile_node(
                plan,
                root.right_input_idx,
                outer_handle_streams,
                cancel,
                task_events,
                built,
                mv_registry,
                mv_latest,
                mv_retention,
                persistence_policy,
            )
            .await?;
        let left_transient_input = try_build_transient_join_input_optimization(
            self.graph_id(),
            plan,
            root.left_input_idx,
            outer_transient_streams,
            None,
            cancel,
        )?
        .ok_or_else(|| {
            anyhow!(
                "missing transient join input for left source-journal input {}",
                root.left_input_idx
            )
        })?;
        let right_transient_input = try_build_transient_join_input_optimization(
            self.graph_id(),
            plan,
            root.right_input_idx,
            outer_transient_streams,
            None,
            cancel,
        )?
        .ok_or_else(|| {
            anyhow!(
                "missing transient join input for right source-journal input {}",
                root.right_input_idx
            )
        })?;

        let (tx, mut receiver) = mpsc::unbounded_channel::<TransientMaterializeBatch>();
        // Join pipeline outputs are general ZSets under CDC. Matched input rows
        // must remain available for future retractions and replacement joins.
        let left_retention = dbsp::JoinInputRetention::RetainAll;
        let right_retention = dbsp::JoinInputRetention::RetainAll;
        tracing::info!(
            graph_id = %self.graph_id(),
            left_retention = ?left_retention,
            right_retention = ?right_retention,
            "using transient join pipeline state retention"
        );
        self.compile_transient_join_root_materialization(
            &root.join,
            left,
            right,
            Some(left_transient_input.receiver),
            Some(right_transient_input.receiver),
            left_retention,
            right_retention,
            None,
            tx,
            task_events,
            state_table.is_some(),
        )
        .await?;

        let identity_transform: Arc<DeltaTransformFn> =
            Arc::new(|deltas: &[(Vec<u8>, i64)]| Ok(deltas.to_vec()));
        let mut current_output_append_only = false;
        for (step_idx, step) in root.steps.iter().enumerate() {
            receiver = match step {
                TransientJoinPipelineStep::Transform(transform) => {
                    build_transient_transform_receiver(
                        self.graph_id(),
                        format!("transient-join-pipeline-transform:{step_idx}"),
                        receiver,
                        Arc::clone(transform),
                        cancel,
                        task_events,
                    )
                }
                TransientJoinPipelineStep::TopN(topn) => {
                    let next = build_transient_topn_receiver_from_batches(
                        self.graph_id(),
                        topn,
                        receiver,
                        current_output_append_only,
                        false,
                        None,
                        cancel,
                        task_events,
                        state_table.clone(),
                        format!("join_pipeline_topn_{step_idx}"),
                    );
                    current_output_append_only = false;
                    next
                }
                TransientJoinPipelineStep::Aggregate(aggregate) => {
                    let next = build_transient_aggregate_receiver_from_batches(
                        self.graph_id(),
                        aggregate,
                        receiver,
                        Arc::clone(&identity_transform),
                        current_output_append_only,
                        false,
                        cancel,
                        task_events,
                        state_table.clone(),
                        format!("join_pipeline_aggregate_{step_idx}"),
                    )
                    .await?;
                    current_output_append_only = false;
                    next
                }
            };
        }

        Ok(receiver)
    }

    pub(super) fn graph_id(&self) -> &str {
        &self.ns.graph_id
    }

    #[allow(clippy::too_many_arguments)]
    #[async_recursion]
    async fn compile_node(
        &mut self,
        plan: &CircuitPlan,
        node_idx: usize,
        outer_streams: &HashMap<String, DeltaHandleStream>,
        cancel: &CancellationToken,
        task_events: &GraphTaskSender,
        built: &mut HashMap<usize, DeltaHandleStream>,
        mv_registry: &Arc<MaterializedViewRegistry>,
        mv_latest: &mut HashMap<String, (i64, ZSetHandle)>,
        mv_retention: StreamRetention,
        persistence_policy: &PersistencePolicy,
    ) -> Result<DeltaHandleStream> {
        if let Some(stream) = built.get(&node_idx) {
            return Ok(stream.clone());
        }
        let node = plan
            .node(node_idx)
            .with_context(|| anyhow!("node {node_idx} missing from circuit plan"))?;

        let stream = match &node.kind {
            DbspNodeKind::Source(source) => self
                .compile_source(source, outer_streams)
                .await
                .with_context(|| anyhow!("source {}", source.table.name))?,
            DbspNodeKind::Select(select) => {
                let input_idx = first_input(node, "select")?;
                let upstream = self
                    .compile_node(
                        plan,
                        input_idx,
                        outer_streams,
                        cancel,
                        task_events,
                        built,
                        mv_registry,
                        mv_latest,
                        mv_retention,
                        persistence_policy,
                    )
                    .await?;
                self.compile_filter(select, upstream, task_events).await?
            }
            DbspNodeKind::Project(project) => {
                let input_idx = first_input(node, "project")?;
                if let Some(select_input_idx) = fuseable_select_input(plan, node_idx, input_idx)? {
                    let select = match &plan
                        .node(input_idx)
                        .with_context(|| {
                            anyhow!("select node {input_idx} missing from circuit plan")
                        })?
                        .kind
                    {
                        DbspNodeKind::Select(select) => select.clone(),
                        _ => unreachable!("fuseable_select_input guarantees select node"),
                    };
                    let upstream = self
                        .compile_node(
                            plan,
                            select_input_idx,
                            outer_streams,
                            cancel,
                            task_events,
                            built,
                            mv_registry,
                            mv_latest,
                            mv_retention,
                            persistence_policy,
                        )
                        .await?;
                    self.compile_filter_map(&select, project, upstream, task_events)
                        .await?
                } else {
                    let upstream = self
                        .compile_node(
                            plan,
                            input_idx,
                            outer_streams,
                            cancel,
                            task_events,
                            built,
                            mv_registry,
                            mv_latest,
                            mv_retention,
                            persistence_policy,
                        )
                        .await?;
                    self.compile_map(project, upstream, task_events).await?
                }
            }
            DbspNodeKind::Join(join) => {
                let (left_idx, right_idx) = join_inputs(node)?;
                let left = self
                    .compile_node(
                        plan,
                        left_idx,
                        outer_streams,
                        cancel,
                        task_events,
                        built,
                        mv_registry,
                        mv_latest,
                        mv_retention,
                        persistence_policy,
                    )
                    .await?;
                let right = self
                    .compile_node(
                        plan,
                        right_idx,
                        outer_streams,
                        cancel,
                        task_events,
                        built,
                        mv_registry,
                        mv_latest,
                        mv_retention,
                        persistence_policy,
                    )
                    .await?;
                self.compile_join(node_idx, join, left, right, cancel, task_events)
                    .await?
            }
            DbspNodeKind::Aggregate(aggregate) => {
                let input_idx = first_input(node, "aggregate")?;
                let append_only_input = plan_node_output_append_only(plan, input_idx)?;
                let upstream = self
                    .compile_node(
                        plan,
                        input_idx,
                        outer_streams,
                        cancel,
                        task_events,
                        built,
                        mv_registry,
                        mv_latest,
                        mv_retention,
                        persistence_policy,
                    )
                    .await?;
                self.compile_aggregate(aggregate, upstream, append_only_input, task_events)
                    .await?
            }
            DbspNodeKind::TopN(topn) => {
                let input_idx = first_input(node, "topn")?;
                let upstream = self
                    .compile_node(
                        plan,
                        input_idx,
                        outer_streams,
                        cancel,
                        task_events,
                        built,
                        mv_registry,
                        mv_latest,
                        mv_retention,
                        persistence_policy,
                    )
                    .await?;
                self.compile_topn(topn, upstream, task_events).await?
            }
            DbspNodeKind::Distinct(distinct) => {
                let input_idx = first_input(node, "distinct")?;
                let append_only_input = plan_node_output_append_only(plan, input_idx)?;
                let upstream = self
                    .compile_node(
                        plan,
                        input_idx,
                        outer_streams,
                        cancel,
                        task_events,
                        built,
                        mv_registry,
                        mv_latest,
                        mv_retention,
                        persistence_policy,
                    )
                    .await?;
                self.compile_distinct(distinct, upstream, append_only_input, task_events)
                    .await?
            }
            DbspNodeKind::WindowAggregate(window) => {
                let input_idx = first_input(node, "window aggregate")?;
                let upstream = self
                    .compile_node(
                        plan,
                        input_idx,
                        outer_streams,
                        cancel,
                        task_events,
                        built,
                        mv_registry,
                        mv_latest,
                        mv_retention,
                        persistence_policy,
                    )
                    .await?;
                self.compile_window_aggregate(window, upstream, task_events)
                    .await?
            }
            DbspNodeKind::Union(union) => {
                let mut inputs = Vec::with_capacity(node.inputs.len());
                for &input_idx in &node.inputs {
                    let upstream = self
                        .compile_node(
                            plan,
                            input_idx,
                            outer_streams,
                            cancel,
                            task_events,
                            built,
                            mv_registry,
                            mv_latest,
                            mv_retention,
                            persistence_policy,
                        )
                        .await?;
                    inputs.push(upstream);
                }
                self.compile_union(union, inputs, task_events).await?
            }
            DbspNodeKind::Passthrough => {
                let input_idx = first_input(node, "passthrough")?;
                self.compile_node(
                    plan,
                    input_idx,
                    outer_streams,
                    cancel,
                    task_events,
                    built,
                    mv_registry,
                    mv_latest,
                    mv_retention,
                    persistence_policy,
                )
                .await?
            }
            DbspNodeKind::Sink(sink) => {
                let input_idx = first_input(node, "sink")?;
                if let Some(transient_opt) = try_build_transient_segment_optimization(
                    plan,
                    input_idx,
                    built,
                    self.graph_id(),
                    false,
                    persistence_policy,
                )? {
                    let upstream = self
                        .compile_node(
                            plan,
                            transient_opt.durable_input_idx,
                            outer_streams,
                            cancel,
                            task_events,
                            built,
                            mv_registry,
                            mv_latest,
                            mv_retention,
                            persistence_policy,
                        )
                        .await?;
                    tracing::info!(
                        graph_id = %self.graph_id(),
                        sink = %sink.name,
                        durable_input_idx = transient_opt.durable_input_idx,
                        optimized_nodes = ?transient_opt.optimized_nodes,
                        segment_score = transient_opt.score,
                        "using transient segment for sink materialization"
                    );
                    self.materialize_view(
                        &sink.name,
                        Arc::clone(sink.input_schema()),
                        upstream,
                        Some(transient_opt.transform),
                        cancel,
                        task_events,
                        mv_registry,
                        mv_latest,
                        mv_retention,
                        self.output_consolidation_mode,
                    )
                    .await?
                } else {
                    let upstream = self
                        .compile_node(
                            plan,
                            input_idx,
                            outer_streams,
                            cancel,
                            task_events,
                            built,
                            mv_registry,
                            mv_latest,
                            mv_retention,
                            persistence_policy,
                        )
                        .await?;
                    self.materialize_view(
                        &sink.name,
                        Arc::clone(sink.input_schema()),
                        upstream,
                        None,
                        cancel,
                        task_events,
                        mv_registry,
                        mv_latest,
                        mv_retention,
                        self.output_consolidation_mode,
                    )
                    .await?
                }
            }
        };

        built.insert(node_idx, stream.clone());
        Ok(stream)
    }
}

#[derive(Default, Clone)]
struct GraphNamespace {
    graph_id: String,
}

impl GraphNamespace {
    fn set_graph_id(&mut self, graph_id: impl Into<String>) {
        self.graph_id = graph_id.into();
    }
}

pub struct BuildInputs<'a> {
    pub graph_id: &'a str,
    pub view_name: &'a str,
    pub plan: &'a CircuitPlan,
    pub cancel: CancellationToken,
    pub task_events: GraphTaskSender,
    pub mv_registry: Arc<MaterializedViewRegistry>,
    pub outer_handle_streams: &'a HashMap<String, DeltaHandleStream>,
    pub outer_transient_streams: &'a HashMap<String, TransientSourceHandleStream>,
    pub enable_source_batch_journal: bool,
    pub restore_transient_helper_state: bool,
    pub mv_retention: StreamRetention,
    pub watermark: Arc<AtomicI64>,
}

pub struct BuildOutputs {
    pub node_streams: HashMap<usize, DeltaHandleStream>,
    pub mv_latest: HashMap<String, (i64, ZSetHandle)>,
    pub required_sources: BTreeSet<String>,
}

fn first_input(node: &CircuitNode, label: &str) -> Result<usize> {
    node.inputs
        .first()
        .copied()
        .with_context(|| anyhow!("{label} node missing required input"))
}

fn join_inputs(node: &CircuitNode) -> Result<(usize, usize)> {
    if node.inputs.len() < 2 {
        bail!("join node requires two inputs");
    }
    Ok((node.inputs[0], node.inputs[1]))
}

fn plan_node_output_append_only(plan: &CircuitPlan, node_idx: usize) -> Result<bool> {
    let node = plan
        .node(node_idx)
        .with_context(|| anyhow!("append-only analysis missing node {node_idx}"))?;
    let input_append_only = |input_idx| plan_node_output_append_only(plan, input_idx);

    match &node.kind {
        DbspNodeKind::Source(_) => {
            // A source root is a ZSet input, not an append-only guarantee. CDC
            // sources can emit deletes and before-image retractions.
            Ok(false)
        }
        DbspNodeKind::Select(_) | DbspNodeKind::Project(_) | DbspNodeKind::Passthrough => {
            input_append_only(first_input(node, "append-only passthrough")?)
        }
        DbspNodeKind::Join(_) | DbspNodeKind::Union(_) => {
            for &input_idx in &node.inputs {
                if !input_append_only(input_idx)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        DbspNodeKind::Distinct(_) => input_append_only(first_input(node, "append-only distinct")?),
        DbspNodeKind::Aggregate(_) | DbspNodeKind::WindowAggregate(_) | DbspNodeKind::TopN(_) => {
            Ok(false)
        }
        DbspNodeKind::Sink(_) => input_append_only(first_input(node, "append-only sink")?),
    }
}

fn fuseable_select_input(
    plan: &CircuitPlan,
    project_node_idx: usize,
    input_idx: usize,
) -> Result<Option<usize>> {
    let select_node = match plan.node(input_idx) {
        Some(node) => node,
        None => return Ok(None),
    };
    let DbspNodeKind::Select(_) = &select_node.kind else {
        return Ok(None);
    };
    if !has_single_consumer(plan, input_idx) {
        return Ok(None);
    }
    first_input(select_node, "select")
        .map(Some)
        .with_context(|| {
            anyhow!(
                "project node {project_node_idx} has fuseable select input {input_idx} without an upstream source"
            )
        })
}

fn has_single_consumer(plan: &CircuitPlan, node_idx: usize) -> bool {
    plan.nodes()
        .iter()
        .flat_map(|node| node.inputs.iter())
        .filter(|&&input| input == node_idx)
        .count()
        == 1
}

struct TransientSegmentOptimization {
    durable_input_idx: usize,
    optimized_nodes: Vec<usize>,
    score: i32,
    steps: Vec<TransientSegmentStep>,
    transform: Arc<DeltaTransformFn>,
}

fn try_build_transient_segment_optimization(
    plan: &CircuitPlan,
    terminal_input_idx: usize,
    built: &HashMap<usize, DeltaHandleStream>,
    graph_id: &str,
    allow_terminal_without_consumer: bool,
    persistence_policy: &PersistencePolicy,
) -> Result<Option<TransientSegmentOptimization>> {
    let Some(segment) = persistence_policy.build_transient_segment(
        plan,
        terminal_input_idx,
        built,
        allow_terminal_without_consumer,
    )?
    else {
        return Ok(None);
    };
    build_transient_segment_optimization_from_spec(graph_id, segment).map(Some)
}

fn build_transient_segment_optimization_from_spec(
    graph_id: &str,
    segment: TransientSegmentSpec,
) -> Result<TransientSegmentOptimization> {
    let steps = segment.steps.clone();
    let mut evaluators = Vec::new();
    for step in &steps {
        match step {
            TransientSegmentStep::Passthrough => {}
            TransientSegmentStep::Select { predicate, schema } => {
                evaluators.push(VectorizedFilterProjectEvaluator::for_filter(
                    predicate,
                    Arc::clone(schema),
                )?);
            }
            TransientSegmentStep::Project {
                expressions,
                schema,
            } => {
                evaluators.push(VectorizedFilterProjectEvaluator::for_map(
                    expressions.as_ref(),
                    Arc::clone(schema),
                )?);
            }
        }
    }

    let evaluators = Arc::new(evaluators);
    let graph_id = graph_id.to_string();
    let transform: Arc<DeltaTransformFn> = Arc::new(move |deltas| {
        apply_transient_segment_vectorized(&graph_id, evaluators.as_ref(), deltas)
    });

    Ok(TransientSegmentOptimization {
        durable_input_idx: segment.durable_input_idx,
        optimized_nodes: segment.segment_nodes,
        score: segment.score,
        steps,
        transform,
    })
}

fn apply_transient_segment_vectorized(
    graph_id: &str,
    evaluators: &[VectorizedFilterProjectEvaluator],
    deltas: &[(Vec<u8>, i64)],
) -> Result<Vec<(Vec<u8>, i64)>> {
    if evaluators.is_empty() {
        return Ok(deltas.to_vec());
    }
    let mut deltas = evaluators[0].transform_delta(graph_id, deltas)?;
    for evaluator in &evaluators[1..] {
        if deltas.is_empty() {
            break;
        }
        deltas = evaluator.transform_delta(graph_id, &deltas)?;
    }
    Ok(deltas)
}

#[derive(Clone)]
struct TransientSourceRootMaterialization {
    source_name: String,
    optimized_nodes: Vec<usize>,
    transform: Arc<DeltaTransformFn>,
}

struct TransientSourceTopNRootMaterialization {
    source_name: String,
    optimized_nodes: Vec<usize>,
    receiver: mpsc::UnboundedReceiver<TransientMaterializeBatch>,
    transform: Option<Arc<DeltaTransformFn>>,
}

struct TransientSourceAggregateRootMaterialization {
    source_name: String,
    optimized_nodes: Vec<usize>,
    receiver: mpsc::UnboundedReceiver<TransientMaterializeBatch>,
}

struct TransientSourceWindowCountStarRootMaterialization {
    source_name: String,
    optimized_nodes: Vec<usize>,
    receiver: mpsc::UnboundedReceiver<TransientMaterializeBatch>,
}

struct TransientSourceWindowAggregateRootMaterialization {
    source_name: String,
    optimized_nodes: Vec<usize>,
    receiver: mpsc::UnboundedReceiver<TransientMaterializeBatch>,
}

struct TransientJoinInputOptimization {
    source_name: String,
    optimized_nodes: Vec<usize>,
    receiver:
        tokio::sync::mpsc::UnboundedReceiver<dbsp::join::TransientJoinInputBatch<Vec<u8>, Vec<u8>>>,
}

struct PersistentTransientInputState {
    table: Option<Arc<dyn KeyValueTable>>,
    prefix: Vec<u8>,
    rows: HashMap<Vec<u8>, i64>,
}

impl PersistentTransientInputState {
    async fn load(
        table: Option<Arc<dyn KeyValueTable>>,
        graph_id: &str,
        label: impl AsRef<str>,
    ) -> Result<Self> {
        let prefix = transient_helper_state_prefix(graph_id, label.as_ref());
        let entries = match table.as_ref() {
            Some(table) => table
                .scan_prefix(&prefix, &ScanOptions::default())
                .await
                .with_context(|| {
                    format!(
                        "load transient helper input state for graph '{graph_id}' label '{}'",
                        label.as_ref()
                    )
                })?,
            None => Vec::new(),
        };
        let mut rows = HashMap::with_capacity(entries.len());
        for (key, value) in entries {
            if value.len() != std::mem::size_of::<i64>() {
                tracing::warn!(
                    graph_id,
                    label = label.as_ref(),
                    key_len = key.len(),
                    value_len = value.len(),
                    "skipping malformed transient helper state row"
                );
                continue;
            }
            let row = key[prefix.len()..].to_vec();
            let mut weight = [0_u8; 8];
            weight.copy_from_slice(&value);
            let weight = i64::from_le_bytes(weight);
            if weight != 0 {
                rows.insert(row, weight);
            }
        }
        Ok(Self {
            table,
            prefix,
            rows,
        })
    }

    fn snapshot_deltas(&self) -> Vec<(Vec<u8>, i64)> {
        self.rows
            .iter()
            .map(|(row, weight)| (row.clone(), *weight))
            .collect()
    }

    async fn apply_deltas(&mut self, deltas: &[(Vec<u8>, i64)]) -> Result<()> {
        if deltas.is_empty() {
            return Ok(());
        }
        let mut batch = WriteBatch::new();
        let mut dirty = false;
        for (row, diff) in deltas {
            if *diff == 0 {
                continue;
            }
            let previous = self.rows.get(row).copied().unwrap_or(0);
            let next = previous.saturating_add(*diff);
            let mut key = self.prefix.clone();
            key.extend_from_slice(row);
            if next == 0 {
                self.rows.remove(row);
                batch.delete(key);
            } else {
                self.rows.insert(row.clone(), next);
                batch.put(key, next.to_le_bytes());
            }
            dirty = true;
        }
        if dirty && let Some(table) = self.table.as_ref() {
            table.write_batch(batch).await?;
        }
        Ok(())
    }

    async fn replace_with_snapshot(&mut self, rows: Vec<(Vec<u8>, i64)>) -> Result<()> {
        let next_rows = rows
            .into_iter()
            .filter(|(_, weight)| *weight != 0)
            .collect::<HashMap<_, _>>();
        if self.rows == next_rows {
            return Ok(());
        }

        let mut batch = WriteBatch::new();
        for row in self.rows.keys() {
            if !next_rows.contains_key(row) {
                let mut key = self.prefix.clone();
                key.extend_from_slice(row);
                batch.delete(key);
            }
        }
        for (row, weight) in &next_rows {
            if self.rows.get(row).copied() != Some(*weight) {
                let mut key = self.prefix.clone();
                key.extend_from_slice(row);
                batch.put(key, weight.to_le_bytes());
            }
        }
        if let Some(table) = self.table.as_ref() {
            table.write_batch(batch).await?;
        }
        self.rows = next_rows;
        Ok(())
    }
}

fn transient_helper_state_prefix(graph_id: &str, label: &str) -> Vec<u8> {
    let mut prefix = b"floe/transient_helper_state/".to_vec();
    prefix.extend_from_slice(graph_id.as_bytes());
    prefix.push(b'/');
    prefix.extend_from_slice(label.as_bytes());
    prefix.push(b'/');
    prefix
}

#[derive(Clone)]
struct TransientJoinPipelineRootMaterialization {
    left_input_idx: usize,
    right_input_idx: usize,
    left_source_root: TransientSourceRootMaterialization,
    right_source_root: TransientSourceRootMaterialization,
    join: dbsp::DbspJoinNode,
    optimized_nodes: Vec<usize>,
    steps: Vec<TransientJoinPipelineStep>,
}

#[derive(Clone)]
struct TransientSourceTopNRootShape {
    source_root: TransientSourceRootMaterialization,
    topn: DbspTopNNode,
    optimized_nodes: Vec<usize>,
    transform: Option<Arc<DeltaTransformFn>>,
    output_projection: Option<Arc<Vec<usize>>>,
}

#[derive(Clone)]
struct TransientSourceAggregateRootShape {
    source_root: TransientSourceRootMaterialization,
    aggregate: DbspAggregateNode,
    optimized_nodes: Vec<usize>,
    transform: Arc<DeltaTransformFn>,
}

#[derive(Clone)]
struct TransientSourceWindowCountStarRootShape {
    source_root: TransientSourceRootMaterialization,
    window: dbsp::DbspWindowAggregateNode,
    optimized_nodes: Vec<usize>,
    transform: Arc<DeltaTransformFn>,
}

#[derive(Clone)]
struct TransientSourceWindowAggregateRootShape {
    source_root: TransientSourceRootMaterialization,
    window: dbsp::DbspWindowAggregateNode,
    optimized_nodes: Vec<usize>,
    transform: Arc<DeltaTransformFn>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TransientWindowCountKey {
    start: i64,
    end: i64,
    key: Vec<u8>,
}

#[derive(Clone)]
enum TransientJoinPipelineStep {
    Transform(Arc<DeltaTransformFn>),
    Aggregate(DbspAggregateNode),
    TopN(DbspTopNNode),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TransientTopNSortSpec {
    ascending: bool,
    nulls_first: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TransientTopNValue {
    Null,
    Int64(i64),
    Timestamp(i64),
    Utf8(String),
    Bool(bool),
}

impl TransientTopNValue {
    fn from_encoded_scalar(
        scalar: Option<EncodedRowScalar>,
        expected_type: &DbspScalarType,
    ) -> Result<Self> {
        match (scalar, expected_type) {
            (None, _) => Ok(Self::Null),
            (Some(EncodedRowScalar::Int64(value)), DbspScalarType::Int64) => Ok(Self::Int64(value)),
            (Some(EncodedRowScalar::TimestampMillis(value)), DbspScalarType::TimestampMillis) => {
                Ok(Self::Timestamp(value))
            }
            (Some(EncodedRowScalar::Utf8(value)), DbspScalarType::Utf8) => Ok(Self::Utf8(value)),
            (Some(EncodedRowScalar::Bool(value)), DbspScalarType::Bool) => Ok(Self::Bool(value)),
            (Some(other), expected) => Err(anyhow!(
                "transient topn order key type mismatch: expected {expected:?}, decoded {other:?}"
            )),
        }
    }
}

impl Ord for TransientTopNValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use TransientTopNValue::*;
        let rank = |value: &TransientTopNValue| -> u8 {
            match value {
                Null => 0,
                Int64(_) => 1,
                Timestamp(_) => 2,
                Utf8(_) => 3,
                Bool(_) => 4,
            }
        };

        let left_rank = rank(self);
        let right_rank = rank(other);
        if left_rank != right_rank {
            return left_rank.cmp(&right_rank);
        }

        match (self, other) {
            (Null, Null) => std::cmp::Ordering::Equal,
            (Int64(a), Int64(b)) => a.cmp(b),
            (Timestamp(a), Timestamp(b)) => a.cmp(b),
            (Utf8(a), Utf8(b)) => a.cmp(b),
            (Bool(a), Bool(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        }
    }
}

impl PartialOrd for TransientTopNValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TransientTopNKey {
    specs: Arc<Vec<TransientTopNSortSpec>>,
    values: Vec<TransientTopNValue>,
    tie_breaker: Vec<u8>,
}

impl TransientTopNKey {
    fn new(
        specs: Arc<Vec<TransientTopNSortSpec>>,
        values: Vec<TransientTopNValue>,
        tie_breaker: Vec<u8>,
    ) -> Self {
        Self {
            specs,
            values,
            tie_breaker,
        }
    }
}

impl Ord for TransientTopNKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for (idx, spec) in self.specs.iter().enumerate() {
            let left = self.values.get(idx);
            let right = other.values.get(idx);
            let (left, right) = match (left, right) {
                (Some(left), Some(right)) => (left, right),
                _ => continue,
            };

            let cmp = match (left, right) {
                (TransientTopNValue::Null, TransientTopNValue::Null) => std::cmp::Ordering::Equal,
                (TransientTopNValue::Null, _) => {
                    if spec.nulls_first {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    }
                }
                (_, TransientTopNValue::Null) => {
                    if spec.nulls_first {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                }
                _ => {
                    let cmp = left.cmp(right);
                    if spec.ascending { cmp } else { cmp.reverse() }
                }
            };

            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }

        self.tie_breaker.cmp(&other.tie_breaker)
    }
}

impl PartialOrd for TransientTopNKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone)]
struct TransientTopNKeyLayout {
    partition_columns: Arc<Vec<usize>>,
    order_columns: Arc<Vec<usize>>,
    order_types: Arc<Vec<DbspScalarType>>,
    precompute_evaluator: Option<Arc<VectorizedFilterProjectEvaluator>>,
}

struct TransientTopNProcessor {
    graph_id: String,
    partition_key_columns: Arc<Vec<usize>>,
    order_key_columns: Arc<Vec<usize>>,
    order_value_types: Arc<Vec<DbspScalarType>>,
    order_specs: Arc<Vec<TransientTopNSortSpec>>,
    limit: usize,
    offset: usize,
    row_key_cache: Option<HashMap<Vec<u8>, (Option<Vec<u8>>, Option<TransientTopNKey>)>>,
    order_index: BTreeMap<Vec<u8>, BTreeMap<TransientTopNKey, i64>>,
    partition_output_cache: BTreeMap<Vec<u8>, HashMap<Vec<u8>, i64>>,
    profile_enabled: bool,
    profiled_batches: usize,
}

struct TransientTop1Processor {
    graph_id: String,
    partition_key_columns: Arc<Vec<usize>>,
    order_key_columns: Arc<Vec<usize>>,
    order_value_types: Arc<Vec<DbspScalarType>>,
    order_specs: Arc<Vec<TransientTopNSortSpec>>,
    row_key_cache: HashMap<Vec<u8>, (Option<Vec<u8>>, Option<TransientTopNKey>)>,
    order_index: HashMap<Vec<u8>, BTreeMap<(TransientTopNKey, Vec<u8>), i64>>,
    partition_output_cache: HashMap<Vec<u8>, Vec<u8>>,
}

impl TransientTopNProcessor {
    fn new(
        graph_id: impl Into<String>,
        topn: &DbspTopNNode,
        key_layout: &TransientTopNKeyLayout,
        append_only_input: bool,
    ) -> Self {
        let order_specs = Arc::new(
            topn.order_by()
                .iter()
                .map(|expr| TransientTopNSortSpec {
                    ascending: expr.ascending(),
                    nulls_first: expr.nulls_first(),
                })
                .collect(),
        );
        Self {
            graph_id: graph_id.into(),
            partition_key_columns: Arc::clone(&key_layout.partition_columns),
            order_key_columns: Arc::clone(&key_layout.order_columns),
            order_value_types: Arc::clone(&key_layout.order_types),
            order_specs,
            limit: topn.limit(),
            offset: topn.offset(),
            row_key_cache: (!append_only_input).then(HashMap::new),
            order_index: BTreeMap::new(),
            partition_output_cache: BTreeMap::new(),
            profile_enabled: std::env::var_os("FLOE_PROFILE_TRANSIENT_TOPN").is_some(),
            profiled_batches: 0,
        }
    }

    fn apply_deltas(&mut self, deltas: Vec<(Vec<u8>, i64)>) -> Result<Vec<(Vec<u8>, i64)>> {
        let input_delta_count = deltas.len();
        let profile_this_batch = self.profile_enabled && self.profiled_batches < 16;
        let total_start = profile_this_batch.then(Instant::now);
        let mut key_eval_us = 0u128;
        let mut mutation_us = 0u128;

        let mut affected_partitions = BTreeSet::new();
        for (row_key, diff) in deltas {
            if diff == 0 {
                continue;
            }
            let key_start = profile_this_batch.then(Instant::now);
            let (partition_key, order_key) = self.keys_for(&row_key);
            if let Some(key_start) = key_start {
                key_eval_us += key_start.elapsed().as_micros();
            }
            let (Some(partition_key), Some(order_key)) = (partition_key, order_key) else {
                continue;
            };
            affected_partitions.insert(partition_key.clone());

            let mutation_start = profile_this_batch.then(Instant::now);
            let partition_index = self.order_index.entry(partition_key.clone()).or_default();
            let previous_weight = partition_index.get(&order_key).copied().unwrap_or(0);
            let next_weight = previous_weight.saturating_add(diff);
            if next_weight <= 0 {
                partition_index.remove(&order_key);
                if partition_index.is_empty() {
                    self.order_index.remove(&partition_key);
                }
            } else {
                partition_index.insert(order_key, next_weight);
            }
            if let Some(mutation_start) = mutation_start {
                mutation_us += mutation_start.elapsed().as_micros();
            }
        }

        let recompute_start = profile_this_batch.then(Instant::now);
        let mut recompute_rows_scanned = 0usize;
        let mut affected_partition_count = 0usize;
        let mut output_deltas = HashMap::new();
        for partition_key in affected_partitions {
            affected_partition_count += 1;
            let previous_output = self
                .partition_output_cache
                .remove(&partition_key)
                .unwrap_or_default();
            let next_output = self
                .order_index
                .get(&partition_key)
                .map(|partition_index| {
                    if profile_this_batch {
                        recompute_rows_scanned += partition_index.len();
                    }
                    self.compute_partition_topn(partition_index)
                })
                .unwrap_or_default();
            accumulate_weight_deltas(&mut output_deltas, &previous_output, &next_output);
            if !next_output.is_empty() {
                self.partition_output_cache
                    .insert(partition_key, next_output);
            }
        }

        let output_deltas = output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect::<Vec<_>>();

        if profile_this_batch {
            self.profiled_batches += 1;
            let recompute_us = recompute_start
                .expect("recompute start present")
                .elapsed()
                .as_micros();
            let total_us = total_start
                .expect("total start present")
                .elapsed()
                .as_micros();
            tracing::info!(
                graph_id = %self.graph_id,
                input_delta_count,
                affected_partition_count,
                retained_partitions = self.partition_output_cache.len(),
                recompute_rows_scanned,
                output_delta_count = output_deltas.len(),
                key_eval_us,
                mutation_us,
                recompute_us,
                total_us,
                "transient topn batch profile"
            );
        }

        Ok(output_deltas)
    }

    fn compute_partition_topn(
        &self,
        partition_index: &BTreeMap<TransientTopNKey, i64>,
    ) -> HashMap<Vec<u8>, i64> {
        if self.limit == 0 {
            return HashMap::new();
        }

        let mut remaining_skip = self.offset;
        let mut remaining_take = self.limit;
        let mut output = HashMap::new();

        for (order_key, weight) in partition_index {
            if remaining_take == 0 {
                break;
            }

            let mut remaining_weight = *weight;
            if remaining_skip > 0 {
                let available = usize::try_from(remaining_weight).unwrap_or(usize::MAX);
                let skip = remaining_skip.min(available);
                remaining_skip -= skip;
                remaining_weight -= skip as i64;
            }

            if remaining_weight <= 0 {
                continue;
            }

            let available = usize::try_from(remaining_weight).unwrap_or(usize::MAX);
            let take = remaining_take.min(available);
            if take > 0 {
                output.insert(order_key.tie_breaker.clone(), take as i64);
                remaining_take -= take;
            }
        }

        output
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn snapshot_deltas(&self) -> Vec<(Vec<u8>, i64)> {
        let retain_count = self.offset.saturating_add(self.limit);
        if retain_count == 0 {
            return Vec::new();
        }

        self.order_index
            .values()
            .flat_map(|partition_index| {
                let mut remaining = retain_count;
                partition_index
                    .iter()
                    .filter_map(move |(order_key, weight)| {
                        if remaining == 0 || *weight <= 0 {
                            return None;
                        }
                        let retained = usize::try_from(*weight)
                            .unwrap_or(usize::MAX)
                            .min(remaining);
                        remaining -= retained;
                        Some((order_key.tie_breaker.clone(), retained as i64))
                    })
            })
            .collect()
    }

    fn keys_for(&mut self, row_key: &Vec<u8>) -> (Option<Vec<u8>>, Option<TransientTopNKey>) {
        if let Some(cache) = self.row_key_cache.as_ref()
            && let Some(cached) = cache.get(row_key)
        {
            return cached.clone();
        }
        let computed = self.compute_key_parts(row_key);
        if let Some(cache) = self.row_key_cache.as_mut() {
            cache.insert(row_key.clone(), computed.clone());
        }
        computed
    }

    fn compute_key_parts(&self, row_key: &Vec<u8>) -> (Option<Vec<u8>>, Option<TransientTopNKey>) {
        compute_transient_topn_key_parts(
            &self.graph_id,
            Arc::clone(&self.order_specs),
            self.partition_key_columns.as_ref(),
            self.order_key_columns.as_ref(),
            self.order_value_types.as_ref(),
            row_key,
        )
    }
}

#[derive(Default)]
struct TransientAppendOnlyTopNPartitionState {
    visible_rows: BTreeMap<TransientTopNKey, i64>,
    visible_count: usize,
}

struct TransientAppendOnlyTopNProcessor {
    graph_id: String,
    partition_key_columns: Arc<Vec<usize>>,
    order_key_columns: Arc<Vec<usize>>,
    order_value_types: Arc<Vec<DbspScalarType>>,
    order_specs: Arc<Vec<TransientTopNSortSpec>>,
    limit: usize,
    profile_enabled: bool,
    profiled_batches: usize,
    partitions: HashMap<Vec<u8>, TransientAppendOnlyTopNPartitionState>,
}

impl TransientAppendOnlyTopNProcessor {
    fn new(
        graph_id: impl Into<String>,
        topn: &DbspTopNNode,
        key_layout: &TransientTopNKeyLayout,
    ) -> Self {
        let order_specs = Arc::new(
            topn.order_by()
                .iter()
                .map(|expr| TransientTopNSortSpec {
                    ascending: expr.ascending(),
                    nulls_first: expr.nulls_first(),
                })
                .collect(),
        );
        Self {
            graph_id: graph_id.into(),
            partition_key_columns: Arc::clone(&key_layout.partition_columns),
            order_key_columns: Arc::clone(&key_layout.order_columns),
            order_value_types: Arc::clone(&key_layout.order_types),
            order_specs,
            limit: topn.limit(),
            profile_enabled: std::env::var_os("FLOE_PROFILE_TRANSIENT_TOPN").is_some(),
            profiled_batches: 0,
            partitions: HashMap::new(),
        }
    }

    fn apply_deltas(&mut self, deltas: Vec<(Vec<u8>, i64)>) -> Result<Vec<(Vec<u8>, i64)>> {
        let input_delta_count = deltas.len();
        let profile_this_batch = self.profile_enabled && self.profiled_batches < 16;
        let total_start = profile_this_batch.then(Instant::now);
        let mut key_eval_us = 0u128;
        let mut partition_apply_us = 0u128;
        let mut trimmed_rows = 0usize;
        let mut skipped_rows = 0usize;
        let mut affected_partitions = HashSet::new();
        let mut output_deltas = HashMap::new();

        for (row_key, diff) in deltas {
            if diff == 0 {
                continue;
            }
            if diff < 0 {
                bail!(
                    "append-only transient topn received negative diff for graph {}",
                    self.graph_id
                );
            }

            let key_start = profile_this_batch.then(Instant::now);
            let (partition_key, order_key) = self.compute_key_parts(&row_key);
            if let Some(key_start) = key_start {
                key_eval_us += key_start.elapsed().as_micros();
            }
            let (Some(partition_key), Some(order_key)) = (partition_key, order_key) else {
                continue;
            };

            affected_partitions.insert(partition_key.clone());
            let apply_start = profile_this_batch.then(Instant::now);
            let state = self.partitions.entry(partition_key).or_default();
            Self::apply_positive_delta(
                state,
                order_key,
                diff,
                self.limit,
                &mut output_deltas,
                &mut trimmed_rows,
                &mut skipped_rows,
            );
            if let Some(apply_start) = apply_start {
                partition_apply_us += apply_start.elapsed().as_micros();
            }
        }

        let output_deltas = output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect::<Vec<_>>();

        if profile_this_batch {
            self.profiled_batches += 1;
            let total_us = total_start
                .expect("total start present")
                .elapsed()
                .as_micros();
            tracing::info!(
                graph_id = %self.graph_id,
                input_delta_count,
                affected_partition_count = affected_partitions.len(),
                retained_partitions = self.partitions.len(),
                trimmed_rows,
                skipped_rows,
                output_delta_count = output_deltas.len(),
                key_eval_us,
                partition_apply_us,
                total_us,
                "transient append-only topn profile"
            );
        }

        Ok(output_deltas)
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn snapshot_deltas(&self) -> Vec<(Vec<u8>, i64)> {
        self.partitions
            .values()
            .flat_map(|state| {
                state.visible_rows.iter().filter_map(|(order_key, weight)| {
                    (*weight > 0).then_some((order_key.tie_breaker.clone(), *weight))
                })
            })
            .collect()
    }

    fn apply_positive_delta(
        state: &mut TransientAppendOnlyTopNPartitionState,
        order_key: TransientTopNKey,
        diff: i64,
        limit: usize,
        output_deltas: &mut HashMap<Vec<u8>, i64>,
        trimmed_rows: &mut usize,
        skipped_rows: &mut usize,
    ) {
        if limit == 0 {
            return;
        }

        if state.visible_count >= limit
            && let Some((worst_key, _)) = state.visible_rows.last_key_value()
            && order_key > *worst_key
        {
            *skipped_rows = skipped_rows.saturating_add(diff as usize);
            return;
        }

        let row_key = order_key.tie_breaker.clone();
        let entry = state.visible_rows.entry(order_key).or_insert(0);
        *entry = entry.saturating_add(diff);
        state.visible_count = state.visible_count.saturating_add(diff as usize);
        accumulate_single_weight_delta(output_deltas, row_key, diff);

        while state.visible_count > limit {
            let overflow = state.visible_count - limit;
            let Some((worst_key, worst_weight)) = state
                .visible_rows
                .last_key_value()
                .map(|(key, weight)| (key.clone(), *weight))
            else {
                break;
            };
            let removable = usize::try_from(worst_weight)
                .unwrap_or(usize::MAX)
                .min(overflow) as i64;
            if removable <= 0 {
                break;
            }
            if let Some(weight) = state.visible_rows.get_mut(&worst_key) {
                *weight -= removable;
                if *weight <= 0 {
                    state.visible_rows.remove(&worst_key);
                }
            }
            state.visible_count -= removable as usize;
            *trimmed_rows = trimmed_rows.saturating_add(removable as usize);
            accumulate_single_weight_delta(
                output_deltas,
                worst_key.tie_breaker.clone(),
                -removable,
            );
        }
    }

    fn compute_key_parts(&self, row_key: &Vec<u8>) -> (Option<Vec<u8>>, Option<TransientTopNKey>) {
        compute_transient_topn_key_parts(
            &self.graph_id,
            Arc::clone(&self.order_specs),
            self.partition_key_columns.as_ref(),
            self.order_key_columns.as_ref(),
            self.order_value_types.as_ref(),
            row_key,
        )
    }
}

impl TransientTop1Processor {
    fn new(
        graph_id: impl Into<String>,
        topn: &DbspTopNNode,
        key_layout: &TransientTopNKeyLayout,
    ) -> Self {
        let order_specs = Arc::new(
            topn.order_by()
                .iter()
                .map(|expr| TransientTopNSortSpec {
                    ascending: expr.ascending(),
                    nulls_first: expr.nulls_first(),
                })
                .collect(),
        );
        Self {
            graph_id: graph_id.into(),
            partition_key_columns: Arc::clone(&key_layout.partition_columns),
            order_key_columns: Arc::clone(&key_layout.order_columns),
            order_value_types: Arc::clone(&key_layout.order_types),
            order_specs,
            row_key_cache: HashMap::new(),
            order_index: HashMap::new(),
            partition_output_cache: HashMap::new(),
        }
    }

    fn apply_deltas(&mut self, deltas: Vec<(Vec<u8>, i64)>) -> Result<Vec<(Vec<u8>, i64)>> {
        let mut output_deltas = HashMap::new();
        for (row_key, diff) in deltas {
            if diff == 0 {
                continue;
            }
            let (partition_key, order_key) = self.keys_for(&row_key);
            let (Some(partition_key), Some(order_key)) = (partition_key, order_key) else {
                continue;
            };

            let previous_top = self.partition_output_cache.get(&partition_key).cloned();
            let partition_now_empty = {
                let partition_index = self.order_index.entry(partition_key.clone()).or_default();
                let index_key = (order_key, row_key.clone());
                let previous_weight = partition_index.get(&index_key).copied().unwrap_or(0);
                let next_weight = previous_weight.saturating_add(diff);
                if next_weight <= 0 {
                    partition_index.remove(&index_key);
                } else {
                    partition_index.insert(index_key, next_weight);
                }
                partition_index.is_empty()
            };

            let next_top = if partition_now_empty {
                self.order_index.remove(&partition_key);
                None
            } else {
                self.order_index
                    .get(&partition_key)
                    .and_then(|partition_index| {
                        partition_index
                            .first_key_value()
                            .map(|((_order_key, row_key), _)| row_key.clone())
                    })
            };

            if previous_top == next_top {
                continue;
            }
            if let Some(previous_top) = previous_top {
                let entry = output_deltas.entry(previous_top).or_insert(0);
                *entry -= 1;
            }
            match next_top {
                Some(next_top) => {
                    let entry = output_deltas.entry(next_top.clone()).or_insert(0);
                    *entry += 1;
                    self.partition_output_cache.insert(partition_key, next_top);
                }
                None => {
                    self.partition_output_cache.remove(&partition_key);
                }
            }
        }

        Ok(output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect())
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn snapshot_deltas(&self) -> Vec<(Vec<u8>, i64)> {
        self.partition_output_cache
            .values()
            .filter_map(|row_key| {
                let (partition_key, order_key) = self.compute_key_parts(row_key);
                let (Some(partition_key), Some(order_key)) = (partition_key, order_key) else {
                    return None;
                };
                let weight = self
                    .order_index
                    .get(&partition_key)?
                    .get(&(order_key, row_key.clone()))?;
                (*weight > 0).then_some((row_key.clone(), 1))
            })
            .collect()
    }

    fn keys_for(&mut self, row_key: &Vec<u8>) -> (Option<Vec<u8>>, Option<TransientTopNKey>) {
        if let Some(cached) = self.row_key_cache.get(row_key) {
            return cached.clone();
        }
        let computed = self.compute_key_parts(row_key);
        self.row_key_cache.insert(row_key.clone(), computed.clone());
        computed
    }

    fn compute_key_parts(&self, row_key: &Vec<u8>) -> (Option<Vec<u8>>, Option<TransientTopNKey>) {
        compute_transient_topn_key_parts(
            &self.graph_id,
            Arc::clone(&self.order_specs),
            self.partition_key_columns.as_ref(),
            self.order_key_columns.as_ref(),
            self.order_value_types.as_ref(),
            row_key,
        )
    }
}

#[derive(Clone)]
struct TransientBatchTopNPartitionUpdate {
    row_key: Vec<u8>,
    order_key: TransientTopNKey,
    diff: i64,
}

#[derive(Clone)]
struct TransientBatchTopNLiveRow {
    order_key: TransientTopNKey,
    weight: i64,
}

#[derive(Default)]
struct TransientBatchTopNPartitionState {
    live_rows: HashMap<Vec<u8>, TransientBatchTopNLiveRow>,
    output_rows: Vec<(Vec<u8>, i64)>,
}

#[derive(Clone, Copy)]
struct TransientDirectInt64TopNConfig {
    partition_idx: usize,
    order_idx: usize,
    ascending: bool,
}

#[derive(Clone, Copy)]
enum TransientDirectTop1PartitionLayout {
    One(usize),
    Two([usize; 2]),
}

#[derive(Clone)]
struct TransientDirectTop1Config {
    partition_layout: TransientDirectTop1PartitionLayout,
    order_idx: usize,
    ascending: bool,
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum TransientDirectTop1PartitionKey {
    One(i64),
    Two(i64, i64),
}

#[derive(Clone)]
struct TransientDirectTop1PartitionUpdate {
    row_key: Vec<u8>,
    order_value: i64,
    diff: i64,
}

#[derive(Clone)]
struct TransientDirectTop1LiveRow {
    order_value: i64,
    weight: i64,
}

#[derive(Default)]
struct TransientDirectTop1PartitionState {
    live_rows: HashMap<Vec<u8>, TransientDirectTop1LiveRow>,
    top_row: Option<Vec<u8>>,
}

#[derive(Clone)]
struct TransientDirectInt64TopNPartitionUpdate {
    row_key: Vec<u8>,
    order_value: i64,
    diff: i64,
}

#[derive(Clone)]
struct TransientDirectInt64TopNLiveRow {
    order_value: i64,
    weight: i64,
}

#[derive(Default)]
struct TransientDirectInt64TopNPartitionState {
    live_rows: HashMap<Vec<u8>, TransientDirectInt64TopNLiveRow>,
    output_rows: Vec<(Vec<u8>, i64)>,
}

struct TransientDirectInt64TopNProcessor {
    graph_id: String,
    partition_idx: usize,
    order_idx: usize,
    ascending: bool,
    limit: usize,
    row_key_cache: HashMap<Vec<u8>, Option<(i64, i64)>>,
    partitions: HashMap<i64, TransientDirectInt64TopNPartitionState>,
    profile_enabled: bool,
    profiled_batches: usize,
}

struct TransientDirectTop1Processor {
    graph_id: String,
    partition_layout: TransientDirectTop1PartitionLayout,
    order_idx: usize,
    ascending: bool,
    compact_append_only_state: bool,
    row_key_cache: HashMap<Vec<u8>, Option<(TransientDirectTop1PartitionKey, i64)>>,
    partitions: HashMap<TransientDirectTop1PartitionKey, TransientDirectTop1PartitionState>,
    profile_enabled: bool,
    profiled_batches: usize,
}

struct TransientBatchTopNProcessor {
    graph_id: String,
    partition_key_columns: Arc<Vec<usize>>,
    order_key_columns: Arc<Vec<usize>>,
    order_value_types: Arc<Vec<DbspScalarType>>,
    order_specs: Arc<Vec<TransientTopNSortSpec>>,
    limit: usize,
    row_key_cache: Option<HashMap<Vec<u8>, (Option<Vec<u8>>, Option<TransientTopNKey>)>>,
    partitions: HashMap<Vec<u8>, TransientBatchTopNPartitionState>,
    profile_enabled: bool,
    profiled_batches: usize,
}

impl TransientBatchTopNProcessor {
    fn new(
        graph_id: impl Into<String>,
        topn: &DbspTopNNode,
        key_layout: &TransientTopNKeyLayout,
        append_only_input: bool,
    ) -> Self {
        let order_specs = Arc::new(
            topn.order_by()
                .iter()
                .map(|expr| TransientTopNSortSpec {
                    ascending: expr.ascending(),
                    nulls_first: expr.nulls_first(),
                })
                .collect(),
        );
        Self {
            graph_id: graph_id.into(),
            partition_key_columns: Arc::clone(&key_layout.partition_columns),
            order_key_columns: Arc::clone(&key_layout.order_columns),
            order_value_types: Arc::clone(&key_layout.order_types),
            order_specs,
            limit: topn.limit(),
            row_key_cache: (!append_only_input).then(HashMap::new),
            partitions: HashMap::new(),
            profile_enabled: std::env::var_os("FLOE_PROFILE_TRANSIENT_TOPN").is_some(),
            profiled_batches: 0,
        }
    }

    fn apply_deltas(&mut self, deltas: Vec<(Vec<u8>, i64)>) -> Result<Vec<(Vec<u8>, i64)>> {
        let input_delta_count = deltas.len();
        let profile_this_batch = self.profile_enabled && self.profiled_batches < 16;
        let total_start = profile_this_batch.then(Instant::now);
        let mut key_eval_us = 0u128;

        let grouping_start = profile_this_batch.then(Instant::now);
        let mut partition_updates =
            HashMap::<Vec<u8>, Vec<TransientBatchTopNPartitionUpdate>>::new();
        for (row_key, diff) in deltas {
            if diff == 0 {
                continue;
            }
            let key_start = profile_this_batch.then(Instant::now);
            let (partition_key, order_key) = self.keys_for(&row_key);
            if let Some(key_start) = key_start {
                key_eval_us += key_start.elapsed().as_micros();
            }
            let (Some(partition_key), Some(order_key)) = (partition_key, order_key) else {
                continue;
            };
            partition_updates.entry(partition_key).or_default().push(
                TransientBatchTopNPartitionUpdate {
                    row_key,
                    order_key,
                    diff,
                },
            );
        }
        let grouping_us = grouping_start
            .map(|start| start.elapsed().as_micros())
            .unwrap_or(0);

        let partition_apply_start = profile_this_batch.then(Instant::now);
        let mut output_deltas = HashMap::new();
        let mut affected_partition_count = 0usize;
        let mut candidate_rows_considered = 0usize;
        let mut exact_rows_sorted = 0usize;
        for (partition_key, updates) in partition_updates {
            affected_partition_count += 1;
            let mut state = self.partitions.remove(&partition_key).unwrap_or_default();
            let previous_output = std::mem::take(&mut state.output_rows);
            let next_output = self.apply_partition_updates(
                &mut state,
                &previous_output,
                &updates,
                &mut candidate_rows_considered,
                &mut exact_rows_sorted,
            );
            Self::accumulate_output_row_deltas(&mut output_deltas, &previous_output, &next_output);
            state.output_rows = next_output;
            if !state.live_rows.is_empty() {
                self.partitions.insert(partition_key, state);
            }
        }
        let partition_apply_us = partition_apply_start
            .map(|start| start.elapsed().as_micros())
            .unwrap_or(0);

        let output_deltas = output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect::<Vec<_>>();

        if profile_this_batch {
            self.profiled_batches += 1;
            let total_us = total_start
                .expect("total start present")
                .elapsed()
                .as_micros();
            tracing::info!(
                graph_id = %self.graph_id,
                input_delta_count,
                affected_partition_count,
                retained_partitions = self.partitions.len(),
                candidate_rows_considered,
                exact_rows_sorted,
                output_delta_count = output_deltas.len(),
                key_eval_us,
                grouping_us,
                partition_apply_us,
                total_us,
                "transient batch topn profile"
            );
        }

        Ok(output_deltas)
    }

    fn apply_partition_updates(
        &self,
        state: &mut TransientBatchTopNPartitionState,
        previous_output: &[(Vec<u8>, i64)],
        updates: &[TransientBatchTopNPartitionUpdate],
        candidate_rows_considered: &mut usize,
        exact_rows_sorted: &mut usize,
    ) -> Vec<(Vec<u8>, i64)> {
        if updates.iter().all(|update| update.diff > 0) {
            self.apply_partition_updates_append_only(
                state,
                previous_output,
                updates,
                candidate_rows_considered,
            )
        } else {
            self.apply_partition_updates_exact(state, updates, exact_rows_sorted)
        }
    }

    fn apply_partition_updates_append_only(
        &self,
        state: &mut TransientBatchTopNPartitionState,
        previous_output: &[(Vec<u8>, i64)],
        updates: &[TransientBatchTopNPartitionUpdate],
        candidate_rows_considered: &mut usize,
    ) -> Vec<(Vec<u8>, i64)> {
        let mut updated_rows = Vec::with_capacity(updates.len());
        for update in updates {
            let next_weight = Self::apply_live_row_update(state, update);
            if next_weight > 0 {
                updated_rows.push(update.row_key.clone());
            }
        }

        updated_rows.sort_by(|left, right| {
            let left_key = &state
                .live_rows
                .get(left)
                .expect("updated row must exist after append-only update")
                .order_key;
            let right_key = &state
                .live_rows
                .get(right)
                .expect("updated row must exist after append-only update")
                .order_key;
            left_key.cmp(right_key)
        });
        updated_rows.dedup();

        *candidate_rows_considered += previous_output.len() + updated_rows.len();
        self.merge_output_rows(state, previous_output, &updated_rows)
    }

    fn apply_partition_updates_exact(
        &self,
        state: &mut TransientBatchTopNPartitionState,
        updates: &[TransientBatchTopNPartitionUpdate],
        exact_rows_sorted: &mut usize,
    ) -> Vec<(Vec<u8>, i64)> {
        for update in updates {
            Self::apply_live_row_update(state, update);
        }

        let mut rows = state
            .live_rows
            .iter()
            .filter_map(|(row_key, live_row)| {
                (live_row.weight > 0).then_some((
                    row_key.clone(),
                    live_row.order_key.clone(),
                    live_row.weight,
                ))
            })
            .collect::<Vec<_>>();
        *exact_rows_sorted += rows.len();
        rows.sort_by(|left, right| left.1.cmp(&right.1));
        self.build_output_from_sorted_rows(
            rows.into_iter()
                .map(|(row_key, _order_key, weight)| (row_key, weight)),
        )
    }

    fn apply_live_row_update(
        state: &mut TransientBatchTopNPartitionState,
        update: &TransientBatchTopNPartitionUpdate,
    ) -> i64 {
        let next_weight = match state.live_rows.get(&update.row_key) {
            Some(live_row) => live_row.weight.saturating_add(update.diff),
            None => update.diff,
        };
        if next_weight <= 0 {
            state.live_rows.remove(&update.row_key);
            return 0;
        }
        state.live_rows.insert(
            update.row_key.clone(),
            TransientBatchTopNLiveRow {
                order_key: update.order_key.clone(),
                weight: next_weight,
            },
        );
        next_weight
    }

    fn merge_output_rows(
        &self,
        state: &TransientBatchTopNPartitionState,
        previous_output: &[(Vec<u8>, i64)],
        updated_rows: &[Vec<u8>],
    ) -> Vec<(Vec<u8>, i64)> {
        if self.limit == 0 {
            return Vec::new();
        }

        let mut output = Vec::new();
        let mut previous_idx = 0usize;
        let mut updated_idx = 0usize;
        let mut remaining_take = self.limit;

        while remaining_take > 0
            && (previous_idx < previous_output.len() || updated_idx < updated_rows.len())
        {
            while previous_idx < previous_output.len() {
                let row_key = &previous_output[previous_idx].0;
                match state.live_rows.get(row_key) {
                    Some(live_row) if live_row.weight > 0 => break,
                    _ => previous_idx += 1,
                }
            }
            while updated_idx < updated_rows.len() {
                let row_key = &updated_rows[updated_idx];
                match state.live_rows.get(row_key) {
                    Some(live_row) if live_row.weight > 0 => break,
                    _ => updated_idx += 1,
                }
            }

            let choice = match (
                previous_output.get(previous_idx),
                updated_rows.get(updated_idx),
            ) {
                (Some((previous_row_key, _)), Some(updated_row_key)) => {
                    let previous_key = &state
                        .live_rows
                        .get(previous_row_key)
                        .expect("previous output row must still exist")
                        .order_key;
                    let updated_key = &state
                        .live_rows
                        .get(updated_row_key)
                        .expect("updated row must still exist")
                        .order_key;
                    match previous_key.cmp(updated_key) {
                        std::cmp::Ordering::Less => {
                            let row_key = previous_row_key.clone();
                            previous_idx += 1;
                            Some(row_key)
                        }
                        std::cmp::Ordering::Greater => {
                            let row_key = updated_row_key.clone();
                            updated_idx += 1;
                            Some(row_key)
                        }
                        std::cmp::Ordering::Equal => {
                            let row_key = previous_row_key.clone();
                            previous_idx += 1;
                            updated_idx += 1;
                            Some(row_key)
                        }
                    }
                }
                (Some((previous_row_key, _)), None) => {
                    let row_key = previous_row_key.clone();
                    previous_idx += 1;
                    Some(row_key)
                }
                (None, Some(updated_row_key)) => {
                    let row_key = updated_row_key.clone();
                    updated_idx += 1;
                    Some(row_key)
                }
                (None, None) => None,
            };

            let Some(row_key) = choice else {
                break;
            };
            let Some(live_row) = state.live_rows.get(&row_key) else {
                continue;
            };
            if live_row.weight <= 0 {
                continue;
            }
            let available = usize::try_from(live_row.weight).unwrap_or(usize::MAX);
            let take = remaining_take.min(available);
            if take == 0 {
                continue;
            }
            output.push((row_key, take as i64));
            remaining_take -= take;
        }

        output
    }

    fn build_output_from_sorted_rows(
        &self,
        rows: impl IntoIterator<Item = (Vec<u8>, i64)>,
    ) -> Vec<(Vec<u8>, i64)> {
        if self.limit == 0 {
            return Vec::new();
        }

        let mut remaining_take = self.limit;
        let mut output = Vec::new();
        for (row_key, weight) in rows {
            if remaining_take == 0 {
                break;
            }
            if weight <= 0 {
                continue;
            }
            let available = usize::try_from(weight).unwrap_or(usize::MAX);
            let take = remaining_take.min(available);
            if take == 0 {
                continue;
            }
            output.push((row_key, take as i64));
            remaining_take -= take;
        }
        output
    }

    fn accumulate_output_row_deltas(
        output_deltas: &mut HashMap<Vec<u8>, i64>,
        previous_output: &[(Vec<u8>, i64)],
        next_output: &[(Vec<u8>, i64)],
    ) {
        for (row_key, previous_weight) in previous_output {
            let next_weight = next_output
                .iter()
                .find_map(|(next_row_key, next_weight)| {
                    (next_row_key == row_key).then_some(*next_weight)
                })
                .unwrap_or(0);
            let delta = next_weight.saturating_sub(*previous_weight);
            if delta != 0 {
                let entry = output_deltas.entry(row_key.clone()).or_insert(0);
                *entry = entry.saturating_add(delta);
                if *entry == 0 {
                    output_deltas.remove(row_key);
                }
            }
        }
        for (row_key, next_weight) in next_output {
            if previous_output
                .iter()
                .any(|(previous_row_key, _)| previous_row_key == row_key)
            {
                continue;
            }
            if *next_weight != 0 {
                let entry = output_deltas.entry(row_key.clone()).or_insert(0);
                *entry = entry.saturating_add(*next_weight);
                if *entry == 0 {
                    output_deltas.remove(row_key);
                }
            }
        }
    }

    fn keys_for(&mut self, row_key: &Vec<u8>) -> (Option<Vec<u8>>, Option<TransientTopNKey>) {
        if let Some(cache) = self.row_key_cache.as_ref()
            && let Some(cached) = cache.get(row_key)
        {
            return cached.clone();
        }
        let computed = self.compute_key_parts(row_key);
        if let Some(cache) = self.row_key_cache.as_mut() {
            cache.insert(row_key.clone(), computed.clone());
        }
        computed
    }

    fn compute_key_parts(&self, row_key: &Vec<u8>) -> (Option<Vec<u8>>, Option<TransientTopNKey>) {
        compute_transient_topn_key_parts(
            &self.graph_id,
            Arc::clone(&self.order_specs),
            self.partition_key_columns.as_ref(),
            self.order_key_columns.as_ref(),
            self.order_value_types.as_ref(),
            row_key,
        )
    }
}

impl TransientDirectInt64TopNProcessor {
    fn new(
        graph_id: impl Into<String>,
        config: TransientDirectInt64TopNConfig,
        topn: &DbspTopNNode,
    ) -> Self {
        Self {
            graph_id: graph_id.into(),
            partition_idx: config.partition_idx,
            order_idx: config.order_idx,
            ascending: config.ascending,
            limit: topn.limit(),
            row_key_cache: HashMap::new(),
            partitions: HashMap::new(),
            profile_enabled: std::env::var_os("FLOE_PROFILE_TRANSIENT_TOPN").is_some(),
            profiled_batches: 0,
        }
    }

    fn apply_deltas(&mut self, deltas: Vec<(Vec<u8>, i64)>) -> Result<Vec<(Vec<u8>, i64)>> {
        let input_delta_count = deltas.len();
        let profile_this_batch = self.profile_enabled && self.profiled_batches < 16;
        let total_start = profile_this_batch.then(Instant::now);
        let mut key_eval_us = 0u128;

        let grouping_start = profile_this_batch.then(Instant::now);
        let mut partition_updates =
            HashMap::<i64, Vec<TransientDirectInt64TopNPartitionUpdate>>::new();
        for (row_key, diff) in deltas {
            if diff == 0 {
                continue;
            }
            let key_start = profile_this_batch.then(Instant::now);
            let maybe_keys = self.keys_for(&row_key)?;
            if let Some(key_start) = key_start {
                key_eval_us += key_start.elapsed().as_micros();
            }
            let Some((partition_value, order_value)) = maybe_keys else {
                continue;
            };
            partition_updates.entry(partition_value).or_default().push(
                TransientDirectInt64TopNPartitionUpdate {
                    row_key,
                    order_value,
                    diff,
                },
            );
        }
        let grouping_us = grouping_start
            .map(|start| start.elapsed().as_micros())
            .unwrap_or(0);

        let partition_apply_start = profile_this_batch.then(Instant::now);
        let mut output_deltas = HashMap::new();
        let mut affected_partition_count = 0usize;
        let mut candidate_rows_considered = 0usize;
        let mut exact_rows_sorted = 0usize;
        for (partition_value, updates) in partition_updates {
            affected_partition_count += 1;
            let mut state = self.partitions.remove(&partition_value).unwrap_or_default();
            let previous_output = std::mem::take(&mut state.output_rows);
            let next_output = self.apply_partition_updates(
                &mut state,
                &previous_output,
                &updates,
                &mut candidate_rows_considered,
                &mut exact_rows_sorted,
            );
            TransientBatchTopNProcessor::accumulate_output_row_deltas(
                &mut output_deltas,
                &previous_output,
                &next_output,
            );
            state.output_rows = next_output;
            if !state.live_rows.is_empty() {
                self.partitions.insert(partition_value, state);
            }
        }
        let partition_apply_us = partition_apply_start
            .map(|start| start.elapsed().as_micros())
            .unwrap_or(0);

        let output_deltas = output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect::<Vec<_>>();

        if profile_this_batch {
            self.profiled_batches += 1;
            let total_us = total_start
                .expect("total start present")
                .elapsed()
                .as_micros();
            tracing::info!(
                graph_id = %self.graph_id,
                input_delta_count,
                affected_partition_count,
                retained_partitions = self.partitions.len(),
                candidate_rows_considered,
                exact_rows_sorted,
                output_delta_count = output_deltas.len(),
                key_eval_us,
                grouping_us,
                partition_apply_us,
                total_us,
                "transient direct int64 batch topn profile"
            );
        }

        Ok(output_deltas)
    }

    fn apply_partition_updates(
        &self,
        state: &mut TransientDirectInt64TopNPartitionState,
        previous_output: &[(Vec<u8>, i64)],
        updates: &[TransientDirectInt64TopNPartitionUpdate],
        candidate_rows_considered: &mut usize,
        exact_rows_sorted: &mut usize,
    ) -> Vec<(Vec<u8>, i64)> {
        if previous_output.is_empty() && updates.iter().all(|update| update.diff > 0) {
            self.apply_partition_updates_append_only(
                state,
                previous_output,
                updates,
                candidate_rows_considered,
            )
        } else {
            self.apply_partition_updates_exact(state, updates, exact_rows_sorted)
        }
    }

    fn apply_partition_updates_append_only(
        &self,
        state: &mut TransientDirectInt64TopNPartitionState,
        previous_output: &[(Vec<u8>, i64)],
        updates: &[TransientDirectInt64TopNPartitionUpdate],
        candidate_rows_considered: &mut usize,
    ) -> Vec<(Vec<u8>, i64)> {
        let mut updated_rows = Vec::with_capacity(updates.len());
        for update in updates {
            let next_weight = Self::apply_live_row_update(state, update);
            if next_weight > 0 {
                updated_rows.push(update.row_key.clone());
            }
        }

        updated_rows.sort_by(|left, right| self.compare_live_rows(state, left, right));
        updated_rows.dedup();

        *candidate_rows_considered += previous_output.len() + updated_rows.len();
        self.merge_output_rows(state, previous_output, &updated_rows)
    }

    fn apply_partition_updates_exact(
        &self,
        state: &mut TransientDirectInt64TopNPartitionState,
        updates: &[TransientDirectInt64TopNPartitionUpdate],
        exact_rows_sorted: &mut usize,
    ) -> Vec<(Vec<u8>, i64)> {
        for update in updates {
            Self::apply_live_row_update(state, update);
        }

        let mut rows = state
            .live_rows
            .iter()
            .filter_map(|(row_key, live_row)| {
                (live_row.weight > 0).then_some((
                    row_key.clone(),
                    live_row.order_value,
                    live_row.weight,
                ))
            })
            .collect::<Vec<_>>();
        *exact_rows_sorted += rows.len();
        rows.sort_by(|left, right| {
            self.compare_order_and_tie_breaker(left.1, &left.0, right.1, &right.0)
        });
        self.build_output_from_sorted_rows(
            rows.into_iter()
                .map(|(row_key, _order_value, weight)| (row_key, weight)),
        )
    }

    fn apply_live_row_update(
        state: &mut TransientDirectInt64TopNPartitionState,
        update: &TransientDirectInt64TopNPartitionUpdate,
    ) -> i64 {
        let next_weight = match state.live_rows.get(&update.row_key) {
            Some(live_row) => live_row.weight.saturating_add(update.diff),
            None => update.diff,
        };
        if next_weight <= 0 {
            state.live_rows.remove(&update.row_key);
            return 0;
        }
        state.live_rows.insert(
            update.row_key.clone(),
            TransientDirectInt64TopNLiveRow {
                order_value: update.order_value,
                weight: next_weight,
            },
        );
        next_weight
    }

    fn merge_output_rows(
        &self,
        state: &TransientDirectInt64TopNPartitionState,
        previous_output: &[(Vec<u8>, i64)],
        updated_rows: &[Vec<u8>],
    ) -> Vec<(Vec<u8>, i64)> {
        if self.limit == 0 {
            return Vec::new();
        }

        let mut output = Vec::new();
        let mut previous_idx = 0usize;
        let mut updated_idx = 0usize;
        let mut remaining_take = self.limit;

        while remaining_take > 0
            && (previous_idx < previous_output.len() || updated_idx < updated_rows.len())
        {
            while previous_idx < previous_output.len() {
                let row_key = &previous_output[previous_idx].0;
                match state.live_rows.get(row_key) {
                    Some(live_row) if live_row.weight > 0 => break,
                    _ => previous_idx += 1,
                }
            }
            while updated_idx < updated_rows.len() {
                let row_key = &updated_rows[updated_idx];
                match state.live_rows.get(row_key) {
                    Some(live_row) if live_row.weight > 0 => break,
                    _ => updated_idx += 1,
                }
            }

            let choice = match (
                previous_output.get(previous_idx),
                updated_rows.get(updated_idx),
            ) {
                (Some((previous_row_key, _)), Some(updated_row_key)) => {
                    let previous_live_row = state
                        .live_rows
                        .get(previous_row_key)
                        .expect("previous output row must still exist");
                    let updated_live_row = state
                        .live_rows
                        .get(updated_row_key)
                        .expect("updated row must still exist");
                    match self.compare_order_and_tie_breaker(
                        previous_live_row.order_value,
                        previous_row_key,
                        updated_live_row.order_value,
                        updated_row_key,
                    ) {
                        std::cmp::Ordering::Less => {
                            let row_key = previous_row_key.clone();
                            previous_idx += 1;
                            Some(row_key)
                        }
                        std::cmp::Ordering::Greater => {
                            let row_key = updated_row_key.clone();
                            updated_idx += 1;
                            Some(row_key)
                        }
                        std::cmp::Ordering::Equal => {
                            let row_key = previous_row_key.clone();
                            previous_idx += 1;
                            updated_idx += 1;
                            Some(row_key)
                        }
                    }
                }
                (Some((previous_row_key, _)), None) => {
                    let row_key = previous_row_key.clone();
                    previous_idx += 1;
                    Some(row_key)
                }
                (None, Some(updated_row_key)) => {
                    let row_key = updated_row_key.clone();
                    updated_idx += 1;
                    Some(row_key)
                }
                (None, None) => None,
            };

            let Some(row_key) = choice else {
                break;
            };
            let Some(live_row) = state.live_rows.get(&row_key) else {
                continue;
            };
            if live_row.weight <= 0 {
                continue;
            }
            let available = usize::try_from(live_row.weight).unwrap_or(usize::MAX);
            let take = remaining_take.min(available);
            if take == 0 {
                continue;
            }
            output.push((row_key, take as i64));
            remaining_take -= take;
        }

        output
    }

    fn build_output_from_sorted_rows(
        &self,
        rows: impl IntoIterator<Item = (Vec<u8>, i64)>,
    ) -> Vec<(Vec<u8>, i64)> {
        if self.limit == 0 {
            return Vec::new();
        }

        let mut remaining_take = self.limit;
        let mut output = Vec::new();
        for (row_key, weight) in rows {
            if remaining_take == 0 {
                break;
            }
            if weight <= 0 {
                continue;
            }
            let available = usize::try_from(weight).unwrap_or(usize::MAX);
            let take = remaining_take.min(available);
            if take == 0 {
                continue;
            }
            output.push((row_key, take as i64));
            remaining_take -= take;
        }
        output
    }

    fn compare_live_rows(
        &self,
        state: &TransientDirectInt64TopNPartitionState,
        left: &Vec<u8>,
        right: &Vec<u8>,
    ) -> std::cmp::Ordering {
        let left_live_row = state
            .live_rows
            .get(left)
            .expect("live row must exist for left comparison");
        let right_live_row = state
            .live_rows
            .get(right)
            .expect("live row must exist for right comparison");
        self.compare_order_and_tie_breaker(
            left_live_row.order_value,
            left,
            right_live_row.order_value,
            right,
        )
    }

    fn compare_order_and_tie_breaker(
        &self,
        left_order: i64,
        left_row_key: &Vec<u8>,
        right_order: i64,
        right_row_key: &Vec<u8>,
    ) -> std::cmp::Ordering {
        let order_cmp = if self.ascending {
            left_order.cmp(&right_order)
        } else {
            right_order.cmp(&left_order)
        };
        if order_cmp != std::cmp::Ordering::Equal {
            return order_cmp;
        }
        left_row_key.cmp(right_row_key)
    }

    fn keys_for(&mut self, row_key: &Vec<u8>) -> Result<Option<(i64, i64)>> {
        if let Some(cached) = self.row_key_cache.get(row_key) {
            return Ok(*cached);
        }
        let computed = self.compute_key_parts(row_key)?;
        self.row_key_cache.insert(row_key.clone(), computed);
        Ok(computed)
    }

    fn compute_key_parts(&self, row_key: &Vec<u8>) -> Result<Option<(i64, i64)>> {
        let Some(partition_value) = extract_encoded_row_int64_column(row_key, self.partition_idx)?
        else {
            return Ok(None);
        };
        let Some(order_value) = extract_encoded_row_int64_column(row_key, self.order_idx)? else {
            return Ok(None);
        };
        Ok(Some((partition_value, order_value)))
    }
}
impl TransientDirectTop1Processor {
    fn new(
        graph_id: impl Into<String>,
        config: TransientDirectTop1Config,
        compact_append_only_state: bool,
    ) -> Self {
        Self {
            graph_id: graph_id.into(),
            partition_layout: config.partition_layout,
            order_idx: config.order_idx,
            ascending: config.ascending,
            compact_append_only_state,
            row_key_cache: HashMap::new(),
            partitions: HashMap::new(),
            profile_enabled: std::env::var_os("FLOE_PROFILE_TRANSIENT_TOPN").is_some(),
            profiled_batches: 0,
        }
    }

    fn apply_deltas(&mut self, deltas: Vec<(Vec<u8>, i64)>) -> Result<Vec<(Vec<u8>, i64)>> {
        let input_delta_count = deltas.len();
        let profile_this_batch = self.profile_enabled && self.profiled_batches < 16;
        let total_start = profile_this_batch.then(Instant::now);
        let mut key_eval_us = 0u128;

        let grouping_start = profile_this_batch.then(Instant::now);
        let mut partition_updates = HashMap::<
            TransientDirectTop1PartitionKey,
            Vec<TransientDirectTop1PartitionUpdate>,
        >::new();
        for (row_key, diff) in deltas {
            if diff == 0 {
                continue;
            }
            let key_start = profile_this_batch.then(Instant::now);
            let maybe_keys = self.keys_for(&row_key)?;
            if let Some(key_start) = key_start {
                key_eval_us += key_start.elapsed().as_micros();
            }
            let Some((partition_key, order_value)) = maybe_keys else {
                continue;
            };
            partition_updates.entry(partition_key).or_default().push(
                TransientDirectTop1PartitionUpdate {
                    row_key,
                    order_value,
                    diff,
                },
            );
        }
        let grouping_us = grouping_start
            .map(|start| start.elapsed().as_micros())
            .unwrap_or(0);

        let partition_apply_start = profile_this_batch.then(Instant::now);
        let mut output_deltas = HashMap::new();
        let mut affected_partition_count = 0usize;
        let mut exact_rows_scanned = 0usize;
        for (partition_key, updates) in partition_updates {
            affected_partition_count += 1;
            let mut state = self.partitions.remove(&partition_key).unwrap_or_default();
            let previous_top = state.top_row.clone();
            let next_top = if updates.iter().all(|update| update.diff > 0) {
                self.apply_partition_updates_append_only(&mut state, &updates)
            } else {
                self.apply_partition_updates_exact(&mut state, &updates, &mut exact_rows_scanned)
            };

            if previous_top != next_top {
                if let Some(previous_top) = previous_top {
                    let entry = output_deltas.entry(previous_top).or_insert(0);
                    *entry -= 1;
                }
                if let Some(next_top_row) = next_top.clone() {
                    let entry = output_deltas.entry(next_top_row).or_insert(0);
                    *entry += 1;
                }
            }

            state.top_row = next_top;
            if !state.live_rows.is_empty() {
                self.partitions.insert(partition_key, state);
            }
        }
        let partition_apply_us = partition_apply_start
            .map(|start| start.elapsed().as_micros())
            .unwrap_or(0);

        let output_deltas = output_deltas
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect::<Vec<_>>();

        if profile_this_batch {
            self.profiled_batches += 1;
            let total_us = total_start
                .expect("total start present")
                .elapsed()
                .as_micros();
            tracing::info!(
                graph_id = %self.graph_id,
                input_delta_count,
                affected_partition_count,
                retained_partitions = self.partitions.len(),
                exact_rows_scanned,
                output_delta_count = output_deltas.len(),
                key_eval_us,
                grouping_us,
                partition_apply_us,
                total_us,
                "transient direct top1 profile"
            );
        }

        Ok(output_deltas)
    }

    #[cfg(test)]
    fn snapshot_deltas(&self) -> Vec<(Vec<u8>, i64)> {
        self.partitions
            .values()
            .filter_map(|state| {
                let row_key = state.top_row.as_ref()?;
                let weight = state.live_rows.get(row_key)?.weight;
                (weight > 0).then_some((row_key.clone(), weight))
            })
            .collect()
    }

    fn apply_partition_updates_append_only(
        &self,
        state: &mut TransientDirectTop1PartitionState,
        updates: &[TransientDirectTop1PartitionUpdate],
    ) -> Option<Vec<u8>> {
        let mut next_top = state.top_row.clone();
        for update in updates {
            let next_weight = Self::apply_live_row_update(state, update);
            if next_weight <= 0 {
                continue;
            }
            let previous_top = next_top.clone();
            match next_top.as_ref() {
                Some(current_top) => {
                    if self.compare_live_rows(state, &update.row_key, current_top)
                        == std::cmp::Ordering::Less
                    {
                        next_top = Some(update.row_key.clone());
                    }
                }
                None => {
                    next_top = Some(update.row_key.clone());
                }
            }
            if next_top.as_ref() == Some(&update.row_key)
                && previous_top.as_ref() != Some(&update.row_key)
                && self.compact_append_only_state
            {
                let retained = state
                    .live_rows
                    .get(&update.row_key)
                    .cloned()
                    .expect("winning append-only top1 row must be live");
                state.live_rows.clear();
                state.live_rows.insert(update.row_key.clone(), retained);
            } else if previous_top.as_ref() != Some(&update.row_key)
                && self.compact_append_only_state
            {
                state.live_rows.remove(&update.row_key);
            }
        }
        next_top
    }

    fn apply_partition_updates_exact(
        &self,
        state: &mut TransientDirectTop1PartitionState,
        updates: &[TransientDirectTop1PartitionUpdate],
        exact_rows_scanned: &mut usize,
    ) -> Option<Vec<u8>> {
        for update in updates {
            Self::apply_live_row_update(state, update);
        }

        *exact_rows_scanned += state.live_rows.len();
        let mut best_row_key: Option<&Vec<u8>> = None;
        let mut best_order_value = 0i64;
        for (row_key, live_row) in &state.live_rows {
            if live_row.weight <= 0 {
                continue;
            }
            match best_row_key {
                Some(current_best) => {
                    if self.compare_order_and_tie_breaker(
                        live_row.order_value,
                        row_key,
                        best_order_value,
                        current_best,
                    ) == std::cmp::Ordering::Less
                    {
                        best_row_key = Some(row_key);
                        best_order_value = live_row.order_value;
                    }
                }
                None => {
                    best_row_key = Some(row_key);
                    best_order_value = live_row.order_value;
                }
            }
        }
        best_row_key.cloned()
    }

    fn apply_live_row_update(
        state: &mut TransientDirectTop1PartitionState,
        update: &TransientDirectTop1PartitionUpdate,
    ) -> i64 {
        let next_weight = match state.live_rows.get(&update.row_key) {
            Some(live_row) => live_row.weight.saturating_add(update.diff),
            None => update.diff,
        };
        if next_weight <= 0 {
            state.live_rows.remove(&update.row_key);
            return 0;
        }
        state.live_rows.insert(
            update.row_key.clone(),
            TransientDirectTop1LiveRow {
                order_value: update.order_value,
                weight: next_weight,
            },
        );
        next_weight
    }

    fn compare_live_rows(
        &self,
        state: &TransientDirectTop1PartitionState,
        left: &Vec<u8>,
        right: &Vec<u8>,
    ) -> std::cmp::Ordering {
        let left_live_row = state
            .live_rows
            .get(left)
            .expect("live row must exist for left comparison");
        let right_live_row = state
            .live_rows
            .get(right)
            .expect("live row must exist for right comparison");
        self.compare_order_and_tie_breaker(
            left_live_row.order_value,
            left,
            right_live_row.order_value,
            right,
        )
    }

    fn compare_order_and_tie_breaker(
        &self,
        left_order: i64,
        left_row_key: &Vec<u8>,
        right_order: i64,
        right_row_key: &Vec<u8>,
    ) -> std::cmp::Ordering {
        let order_cmp = if self.ascending {
            left_order.cmp(&right_order)
        } else {
            right_order.cmp(&left_order)
        };
        if order_cmp != std::cmp::Ordering::Equal {
            return order_cmp;
        }
        left_row_key.cmp(right_row_key)
    }

    fn keys_for(
        &mut self,
        row_key: &Vec<u8>,
    ) -> Result<Option<(TransientDirectTop1PartitionKey, i64)>> {
        if let Some(cached) = self.row_key_cache.get(row_key) {
            return Ok(cached.clone());
        }
        let computed = self.compute_key_parts(row_key)?;
        self.row_key_cache.insert(row_key.clone(), computed.clone());
        Ok(computed)
    }

    fn compute_key_parts(
        &self,
        row_key: &Vec<u8>,
    ) -> Result<Option<(TransientDirectTop1PartitionKey, i64)>> {
        let Some(partition_key) =
            extract_direct_top1_partition_key(row_key, self.partition_layout)?
        else {
            return Ok(None);
        };
        let Some(order_value) = extract_encoded_row_i64_like_column(row_key, self.order_idx)?
        else {
            return Ok(None);
        };
        Ok(Some((partition_key, order_value)))
    }
}

fn compute_transient_topn_key_parts(
    graph_id: &str,
    order_specs: Arc<Vec<TransientTopNSortSpec>>,
    partition_key_columns: &[usize],
    order_key_columns: &[usize],
    order_value_types: &[DbspScalarType],
    row_key: &Vec<u8>,
) -> (Option<Vec<u8>>, Option<TransientTopNKey>) {
    let partition_key = if partition_key_columns.is_empty() {
        Some(Vec::new())
    } else {
        match extract_encoded_row_columns(row_key, partition_key_columns, false) {
            Ok(selected) => selected,
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to extract transient topn partition key columns"
                );
                return (None, None);
            }
        }
    };

    let order_key = compute_transient_topn_order_key(
        graph_id,
        order_specs,
        order_key_columns,
        order_value_types,
        row_key,
    );

    (partition_key, order_key)
}

fn compute_transient_topn_order_key(
    graph_id: &str,
    order_specs: Arc<Vec<TransientTopNSortSpec>>,
    order_key_columns: &[usize],
    order_value_types: &[DbspScalarType],
    row_key: &Vec<u8>,
) -> Option<TransientTopNKey> {
    let mut values = Vec::with_capacity(order_key_columns.len());
    for (column_idx, expected_type) in order_key_columns.iter().zip(order_value_types.iter()) {
        let scalar = match extract_encoded_row_scalar(row_key, *column_idx) {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to extract transient topn order key column"
                );
                return None;
            }
        };
        match TransientTopNValue::from_encoded_scalar(scalar, expected_type) {
            Ok(value) => values.push(value),
            Err(err) => {
                tracing::warn!(
                    graph_id = %graph_id,
                    error = %err,
                    "failed to map transient topn order value"
                );
                return None;
            }
        }
    }

    Some(TransientTopNKey::new(order_specs, values, row_key.clone()))
}

fn build_transient_topn_key_layout(topn: &DbspTopNNode) -> Result<TransientTopNKeyLayout> {
    let input_schema = Arc::clone(topn.output_schema());
    let direct_partition_columns = topn
        .partition_by()
        .iter()
        .map(|expr| projection_direct_column_index_expression(expr.expr(), input_schema.as_ref()))
        .collect::<Vec<_>>();
    let direct_order_columns = topn
        .order_by()
        .iter()
        .map(|expr| {
            projection_direct_column_index_expression(
                expr.expression().expr(),
                input_schema.as_ref(),
            )
        })
        .collect::<Vec<_>>();

    if direct_partition_columns.iter().all(Option::is_some)
        && direct_order_columns.iter().all(Option::is_some)
    {
        return Ok(TransientTopNKeyLayout {
            partition_columns: Arc::new(
                direct_partition_columns
                    .into_iter()
                    .collect::<Option<Vec<_>>>()
                    .expect("all direct transient topn partition columns should be present"),
            ),
            order_columns: Arc::new(
                direct_order_columns
                    .iter()
                    .copied()
                    .collect::<Option<Vec<_>>>()
                    .expect("all direct transient topn order columns should be present"),
            ),
            order_types: Arc::new(
                direct_order_columns
                    .iter()
                    .copied()
                    .collect::<Option<Vec<_>>>()
                    .expect("all direct transient topn order columns should be present")
                    .into_iter()
                    .map(|column_idx| {
                        input_schema
                            .field(column_idx)
                            .map(|field| field.data_type.clone())
                            .expect("transient topn order key column index should be in bounds")
                    })
                    .collect(),
            ),
            precompute_evaluator: None,
        });
    }

    let mut items =
        Vec::with_capacity(input_schema.len() + topn.partition_by().len() + topn.order_by().len());
    for field in input_schema.fields() {
        items.push(dbsp::circuit::plan::ProjectItem {
            expr: Expr::Column(Column::new_unqualified(field.name.clone())),
            alias: Some(field.name.clone()),
        });
    }

    let mut expression_columns = HashMap::new();
    let mut seen = HashSet::new();
    let mut next_index = input_schema.len();
    let mut partition_columns = Vec::with_capacity(topn.partition_by().len());
    for (index, expr) in topn.partition_by().iter().enumerate() {
        if let Some(column_idx) = direct_partition_columns[index] {
            partition_columns.push(column_idx);
            continue;
        }
        let key = transient_topn_expression_lookup_key(expr.expr());
        if seen.insert(key.clone()) {
            let alias = format!("__floe_transient_topn_partition_expr_{index}");
            items.push(dbsp::circuit::plan::ProjectItem {
                expr: expr.expr().clone(),
                alias: Some(alias),
            });
            expression_columns.insert(key.clone(), next_index);
            next_index += 1;
        }
        partition_columns.push(
            *expression_columns
                .get(&key)
                .expect("transient topn partition expression column should be registered"),
        );
    }

    let mut order_columns = Vec::with_capacity(topn.order_by().len());
    for (index, expr) in topn.order_by().iter().enumerate() {
        if let Some(column_idx) = direct_order_columns[index] {
            order_columns.push(column_idx);
            continue;
        }
        let key = transient_topn_expression_lookup_key(expr.expression().expr());
        if seen.insert(key.clone()) {
            let alias = format!("__floe_transient_topn_order_expr_{index}");
            items.push(dbsp::circuit::plan::ProjectItem {
                expr: expr.expression().expr().clone(),
                alias: Some(alias),
            });
            expression_columns.insert(key.clone(), next_index);
            next_index += 1;
        }
        order_columns.push(
            *expression_columns
                .get(&key)
                .expect("transient topn order expression column should be registered"),
        );
    }

    let project_node = DbspProjectNode::try_new(Arc::clone(&input_schema), items)
        .context("build transient topn expression precompute projection")?;
    let evaluator = VectorizedFilterProjectEvaluator::for_map(
        project_node.expressions(),
        Arc::clone(&input_schema),
    )
    .context("initialize transient topn precompute evaluator")?;
    let projected_schema = project_node.output_schema();
    let order_types = order_columns
        .iter()
        .map(|column_idx| {
            projected_schema
                .field(*column_idx)
                .map(|field| field.data_type.clone())
                .ok_or_else(|| {
                    anyhow!("transient topn order key column index {column_idx} out of bounds")
                })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(TransientTopNKeyLayout {
        partition_columns: Arc::new(partition_columns),
        order_columns: Arc::new(order_columns),
        order_types: Arc::new(order_types),
        precompute_evaluator: Some(Arc::new(evaluator)),
    })
}

fn transient_topn_expression_lookup_key(expr: &Expr) -> String {
    match expr {
        Expr::Alias(alias) => transient_topn_expression_lookup_key(alias.expr.as_ref()),
        other => other.to_string(),
    }
}

fn accumulate_weight_deltas(
    output_deltas: &mut HashMap<Vec<u8>, i64>,
    previous_output: &HashMap<Vec<u8>, i64>,
    next_output: &HashMap<Vec<u8>, i64>,
) {
    for (row_key, previous_weight) in previous_output {
        let next_weight = next_output.get(row_key).copied().unwrap_or(0);
        let delta = next_weight.saturating_sub(*previous_weight);
        if delta != 0 {
            let entry = output_deltas.entry(row_key.clone()).or_insert(0);
            *entry = entry.saturating_add(delta);
            if *entry == 0 {
                output_deltas.remove(row_key);
            }
        }
    }
    for (row_key, next_weight) in next_output {
        if previous_output.contains_key(row_key) {
            continue;
        }
        if *next_weight != 0 {
            let entry = output_deltas.entry(row_key.clone()).or_insert(0);
            *entry = entry.saturating_add(*next_weight);
            if *entry == 0 {
                output_deltas.remove(row_key);
            }
        }
    }
}

fn accumulate_single_weight_delta(
    output_deltas: &mut HashMap<Vec<u8>, i64>,
    row_key: Vec<u8>,
    diff: i64,
) {
    if diff == 0 {
        return;
    }
    let entry = output_deltas.entry(row_key.clone()).or_insert(0);
    *entry = entry.saturating_add(diff);
    if *entry == 0 {
        output_deltas.remove(&row_key);
    }
}

enum TransientSourceRootShape {
    Source {
        source: DbspSourceNode,
        optimized_nodes: Vec<usize>,
    },
    Select {
        source: DbspSourceNode,
        select: DbspSelectNode,
        optimized_nodes: Vec<usize>,
    },
    Project {
        source: DbspSourceNode,
        project: DbspProjectNode,
        optimized_nodes: Vec<usize>,
    },
    FilterMap {
        source: DbspSourceNode,
        select: DbspSelectNode,
        project: DbspProjectNode,
        optimized_nodes: Vec<usize>,
    },
}

impl TransientSourceRootShape {
    fn source_name(&self) -> &str {
        match self {
            Self::Source { source, .. }
            | Self::Select { source, .. }
            | Self::Project { source, .. }
            | Self::FilterMap { source, .. } => &source.table.name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransientSourceRootRequirements {
    pub source_name: String,
    pub required_columns: Vec<usize>,
}

pub use source_requirements::{
    PlanSourceRequirements, plan_source_requirements, source_batch_journal_root_source_name,
    source_batch_journal_root_sources, transient_source_root_requirements,
};

mod source_requirements;

#[cfg(test)]
fn join_input_unique_on_direct_source_primary_key<'a>(
    plan: &CircuitPlan,
    input_idx: usize,
    key_expressions: impl IntoIterator<Item = &'a DbspExpression>,
    input_schema: &RowSchema,
) -> Result<bool> {
    Ok(join_input_direct_source_primary_key_columns(
        plan,
        input_idx,
        key_expressions,
        input_schema,
    )?
    .is_some())
}

#[cfg(test)]
fn join_input_direct_source_primary_key_columns<'a>(
    plan: &CircuitPlan,
    input_idx: usize,
    key_expressions: impl IntoIterator<Item = &'a DbspExpression>,
    input_schema: &RowSchema,
) -> Result<Option<Arc<Vec<usize>>>> {
    let Some(shape) = find_transient_source_root_shape(plan, input_idx)? else {
        return Ok(None);
    };
    let (source, project) = match shape {
        TransientSourceRootShape::Source { source, .. }
        | TransientSourceRootShape::Select { source, .. } => (source, None),
        TransientSourceRootShape::Project {
            source, project, ..
        }
        | TransientSourceRootShape::FilterMap {
            source, project, ..
        } => (source, Some(project)),
    };

    let Some(key_columns) = key_expressions
        .into_iter()
        .map(|expr| projection_direct_column_index_expression(expr.expr(), input_schema))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let key_columns = if let Some(project) = project.as_ref() {
        key_columns
            .into_iter()
            .map(|column_idx| {
                project
                    .expressions()
                    .get(column_idx)
                    .and_then(|expr| projection_direct_column_index(expr, project.input_schema()))
            })
            .collect::<Option<BTreeSet<_>>>()
    } else {
        Some(key_columns.into_iter().collect::<BTreeSet<_>>())
    };
    let Some(key_columns) = key_columns else {
        return Ok(None);
    };
    let primary_key_columns = source
        .table
        .primary_key()
        .columns()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();

    if key_columns == primary_key_columns {
        Ok(Some(Arc::new(primary_key_columns.into_iter().collect())))
    } else {
        Ok(None)
    }
}

fn try_build_direct_join_output_projection(
    join: &dbsp::DbspJoinNode,
    steps: &[TransientSegmentStep],
) -> Option<Arc<Vec<EncodedRowProjectionColumn>>> {
    let mut project_expressions: Option<Arc<Vec<DbspProjectExpr>>> = None;
    for step in steps {
        match step {
            TransientSegmentStep::Passthrough => {}
            TransientSegmentStep::Select { .. } => return None,
            TransientSegmentStep::Project { expressions, .. } => {
                if project_expressions.is_some() {
                    return None;
                }
                project_expressions = Some(Arc::clone(expressions));
            }
        }
    }

    let expressions = project_expressions?;
    let left_width = join.left_schema.len();
    let columns = expressions
        .iter()
        .map(|expr| {
            let column_idx = projection_direct_column_index(expr, join.output_schema.as_ref())?;
            if column_idx < left_width {
                Some(EncodedRowProjectionColumn {
                    source: EncodedRowProjectionSource::Left,
                    index: column_idx,
                })
            } else {
                Some(EncodedRowProjectionColumn {
                    source: EncodedRowProjectionSource::Right,
                    index: column_idx - left_width,
                })
            }
        })
        .collect::<Option<Vec<_>>>()?;
    Some(Arc::new(columns))
}

fn projection_direct_column_index(expr: &DbspProjectExpr, schema: &RowSchema) -> Option<usize> {
    match expr.expression().expr() {
        Expr::Alias(alias) => {
            projection_direct_column_index_expression(alias.expr.as_ref(), schema)
        }
        other => projection_direct_column_index_expression(other, schema),
    }
}

fn projection_direct_column_index_expression(expr: &Expr, schema: &RowSchema) -> Option<usize> {
    match expr {
        Expr::Column(column) => projection_resolve_direct_column(schema, column),
        Expr::Alias(alias) => {
            projection_direct_column_index_expression(alias.expr.as_ref(), schema)
        }
        _ => None,
    }
}

fn projection_resolve_direct_column(schema: &RowSchema, column: &Column) -> Option<usize> {
    let qualified = column.flat_name();
    schema
        .field_index(&qualified)
        .or_else(|| schema.field_index(&column.name))
}

fn try_build_direct_partitioned_top1_config(
    topn: &DbspTopNNode,
) -> Option<TransientDirectTop1Config> {
    if topn.offset() != 0 || topn.limit() != 1 {
        return None;
    }
    if topn.partition_by().is_empty() || topn.partition_by().len() > 2 || topn.order_by().len() != 1
    {
        return None;
    }

    let schema = topn.output_schema();
    let partition_indices = topn
        .partition_by()
        .iter()
        .map(|expr| projection_direct_column_index_expression(expr.expr(), schema.as_ref()))
        .collect::<Option<Vec<_>>>()?;

    for partition_idx in &partition_indices {
        let partition_field = schema.field(*partition_idx)?;
        if partition_field.data_type != dbsp::circuit::types::DbspScalarType::Int64
            || partition_field.nullable
        {
            return None;
        }
    }

    let order_idx = projection_direct_column_index_expression(
        topn.order_by()[0].expression().expr(),
        schema.as_ref(),
    )?;
    let order_field = schema.field(order_idx)?;
    if !matches!(
        order_field.data_type,
        dbsp::circuit::types::DbspScalarType::Int64
            | dbsp::circuit::types::DbspScalarType::TimestampMillis
    ) || order_field.nullable
    {
        return None;
    }

    let partition_layout = match partition_indices.as_slice() {
        [partition_idx] => TransientDirectTop1PartitionLayout::One(*partition_idx),
        [first_partition_idx, second_partition_idx] => {
            TransientDirectTop1PartitionLayout::Two([*first_partition_idx, *second_partition_idx])
        }
        _ => return None,
    };

    Some(TransientDirectTop1Config {
        partition_layout,
        order_idx,
        ascending: topn.order_by()[0].ascending(),
    })
}

#[allow(dead_code)]
fn try_build_direct_int64_partitioned_topn_config(
    topn: &DbspTopNNode,
) -> Option<TransientDirectInt64TopNConfig> {
    if topn.offset() != 0 || topn.limit() == 0 || topn.limit() > 64 {
        return None;
    }
    if topn.partition_by().len() != 1 || topn.order_by().len() != 1 {
        return None;
    }

    let schema = topn.output_schema();
    let partition_idx =
        projection_direct_column_index_expression(topn.partition_by()[0].expr(), schema.as_ref())?;
    let order_idx = projection_direct_column_index_expression(
        topn.order_by()[0].expression().expr(),
        schema.as_ref(),
    )?;

    let partition_field = schema.field(partition_idx)?;
    let order_field = schema.field(order_idx)?;
    if partition_field.data_type != dbsp::circuit::types::DbspScalarType::Int64
        || partition_field.nullable
    {
        return None;
    }
    if order_field.data_type != dbsp::circuit::types::DbspScalarType::Int64 || order_field.nullable
    {
        return None;
    }

    Some(TransientDirectInt64TopNConfig {
        partition_idx,
        order_idx,
        ascending: topn.order_by()[0].ascending(),
    })
}

fn extract_encoded_row_int64_column(bytes: &[u8], target_index: usize) -> Result<Option<i64>> {
    if bytes.len() < 4 {
        bail!("encoded key too short");
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if target_index >= count {
        bail!("encoded row missing int64 column at index {target_index}");
    }

    let mut cursor = 4usize;
    for column_idx in 0..count {
        let tag = *bytes
            .get(cursor)
            .ok_or_else(|| anyhow!("unexpected end of key while decoding tag"))?;
        cursor += 1;
        if column_idx == target_index {
            return match tag {
                0x01 => {
                    let end = cursor + 8;
                    let chunk = bytes
                        .get(cursor..end)
                        .ok_or_else(|| anyhow!("truncated int64"))?;
                    Ok(Some(i64::from_le_bytes(chunk.try_into().unwrap())))
                }
                0x05 | 0x00 => Ok(None),
                other => Err(anyhow!(
                    "expected int64 encoded field at index {target_index}, found tag {other:#x}"
                )),
            };
        }
        cursor = skip_encoded_row_field(bytes, cursor, tag)?;
    }

    bail!("encoded row missing int64 column at index {target_index}")
}

fn extract_direct_top1_partition_key(
    row_key: &[u8],
    partition_layout: TransientDirectTop1PartitionLayout,
) -> Result<Option<TransientDirectTop1PartitionKey>> {
    let partition_key = match partition_layout {
        TransientDirectTop1PartitionLayout::One(partition_idx) => {
            let Some(partition_value) = extract_encoded_row_int64_column(row_key, partition_idx)?
            else {
                return Ok(None);
            };
            TransientDirectTop1PartitionKey::One(partition_value)
        }
        TransientDirectTop1PartitionLayout::Two(partition_indices) => {
            let Some(first_partition_value) =
                extract_encoded_row_int64_column(row_key, partition_indices[0])?
            else {
                return Ok(None);
            };
            let Some(second_partition_value) =
                extract_encoded_row_int64_column(row_key, partition_indices[1])?
            else {
                return Ok(None);
            };
            TransientDirectTop1PartitionKey::Two(first_partition_value, second_partition_value)
        }
    };
    Ok(Some(partition_key))
}
fn extract_encoded_row_i64_like_column(bytes: &[u8], target_index: usize) -> Result<Option<i64>> {
    if bytes.len() < 4 {
        bail!("encoded key too short");
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if target_index >= count {
        bail!("encoded row missing i64-like column at index {target_index}");
    }

    let mut cursor = 4usize;
    for column_idx in 0..count {
        let tag = *bytes
            .get(cursor)
            .ok_or_else(|| anyhow!("unexpected end of key while decoding tag"))?;
        cursor += 1;
        if column_idx == target_index {
            return match tag {
                0x01 | 0x03 => {
                    let end = cursor + 8;
                    let chunk = bytes
                        .get(cursor..end)
                        .ok_or_else(|| anyhow!("truncated fixed-width i64-like value"))?;
                    Ok(Some(i64::from_le_bytes(chunk.try_into().unwrap())))
                }
                0x05 | 0x07 | 0x00 => Ok(None),
                other => Err(anyhow!(
                    "expected i64-like encoded field at index {target_index}, found tag {other:#x}"
                )),
            };
        }
        cursor = skip_encoded_row_field(bytes, cursor, tag)?;
    }

    bail!("encoded row missing i64-like column at index {target_index}")
}

fn skip_encoded_row_field(bytes: &[u8], cursor: usize, tag: u8) -> Result<usize> {
    match tag {
        0x00 | 0x05 | 0x06 | 0x07 | 0x08 => Ok(cursor),
        0x01 | 0x03 => {
            let end = cursor + 8;
            bytes
                .get(cursor..end)
                .ok_or_else(|| anyhow!("truncated fixed-width value"))?;
            Ok(end)
        }
        0x02 => {
            let len_bytes = bytes
                .get(cursor..cursor + 4)
                .ok_or_else(|| anyhow!("truncated string length"))?;
            let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            let end = cursor + 4 + len;
            bytes
                .get(cursor + 4..end)
                .ok_or_else(|| anyhow!("truncated string payload"))?;
            Ok(end)
        }
        0x04 => {
            bytes
                .get(cursor)
                .ok_or_else(|| anyhow!("missing boolean payload"))?;
            Ok(cursor + 1)
        }
        _ => Err(anyhow!("unknown column tag {tag:#x} in MV key")),
    }
}

fn try_build_transient_join_input_optimization(
    graph_id: &str,
    plan: &CircuitPlan,
    input_idx: usize,
    outer_transient_streams: &HashMap<String, TransientSourceHandleStream>,
    closed_key_columns: Option<Arc<Vec<usize>>>,
    cancel: &CancellationToken,
) -> Result<Option<TransientJoinInputOptimization>> {
    let Some(source_root) = try_build_transient_source_root_materialization(plan, input_idx)?
    else {
        return Ok(None);
    };
    let Some(upstream) = outer_transient_streams
        .get(&source_root.source_name)
        .cloned()
    else {
        return Ok(None);
    };

    let mut upstream_rx = upstream.subscribe();
    let (tx, receiver) = tokio::sync::mpsc::unbounded_channel();
    let graph_id = graph_id.to_string();
    let input_label = format!("join_input:{input_idx}");
    let source_name = source_root.source_name.clone();
    let optimized_nodes = source_root.optimized_nodes.clone();
    let transform = Arc::clone(&source_root.transform);
    let closed_key_transform =
        try_build_transient_join_closed_key_transform(plan, input_idx, closed_key_columns)?;
    let cancel = cancel.clone();
    let debug_transient_join = std::env::var_os("FLOE_DEBUG_TRANSIENT_JOIN").is_some();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                maybe_batch = upstream_rx.recv() => {
                    let Some(batch) = maybe_batch else {
                        break;
                    };
                    let transformed = match transform(batch.deltas.as_ref()) {
                        Ok(transformed) => transformed,
                        Err(err) => {
                            tracing::warn!(
                                graph_id = %graph_id,
                                input_idx,
                                source = %batch.source,
                                version = batch.version,
                                error = %err,
                                "dropping transient join input batch after transform failure"
                            );
                            continue;
                        }
                    };
                        let join_ts = batch.version.saturating_add(1);
                        let closed_keys = match closed_key_transform.as_ref() {
                            Some(transform) => match transform(batch.deltas.as_ref()) {
                                Ok(closed_keys) => closed_keys,
                                Err(err) => {
                                    tracing::warn!(
                                        graph_id = %graph_id,
                                        input_idx,
                                        source = %batch.source,
                                        version = batch.version,
                                        error = %err,
                                        "dropping transient join closed-key batch after transform failure"
                                    );
                                    Vec::new()
                                }
                            },
                            None => Vec::new(),
                        };
                        if debug_transient_join {
                            eprintln!(
                                "transient-join-input graph_id={} input_idx={} source={} version={} join_ts={} rows={} closed_keys={}",
                                graph_id,
                                input_idx,
                                batch.source,
                                batch.version,
                                join_ts,
                                transformed.len(),
                                closed_keys.len()
                            );
                        }
                        if tx.send(dbsp::join::TransientJoinInputBatch {
                            ts: join_ts,
                            deltas: Arc::new(transformed),
                            closed_keys: Arc::new(closed_keys),
                        }).is_err() {
                        tracing::debug!(
                            graph_id = %graph_id,
                            input_idx,
                            source = %batch.source,
                            "transient join input receiver closed"
                        );
                        break;
                    }
                }
            }
        }
        tracing::debug!(
            graph_id = %graph_id,
            input_idx,
            source = %source_name,
            optimized_nodes = ?optimized_nodes,
            label = %input_label,
            "transient join input optimization stopped"
        );
    });

    Ok(Some(TransientJoinInputOptimization {
        source_name: source_root.source_name,
        optimized_nodes: source_root.optimized_nodes,
        receiver,
    }))
}

fn try_build_transient_join_closed_key_transform(
    plan: &CircuitPlan,
    input_idx: usize,
    closed_key_columns: Option<Arc<Vec<usize>>>,
) -> Result<Option<Arc<ClosedJoinKeyTransformFn>>> {
    let Some(closed_key_columns) = closed_key_columns else {
        return Ok(None);
    };
    let Some(shape) = find_transient_source_root_shape(plan, input_idx)? else {
        return Ok(None);
    };
    let select = match shape {
        TransientSourceRootShape::Select { select, .. }
        | TransientSourceRootShape::FilterMap { select, .. } => select,
        TransientSourceRootShape::Source { .. } | TransientSourceRootShape::Project { .. } => {
            return Ok(None);
        }
    };
    let filter_transform = build_filter_transform(&select)?;
    Ok(Some(Arc::new(move |delta_values: &[(Vec<u8>, i64)]| {
        let selected = filter_transform(delta_values)?;
        let mut selected_keys = BTreeSet::new();
        for (row, weight) in selected.iter() {
            if *weight <= 0 {
                continue;
            }
            if let Some(key) = extract_encoded_row_columns(row, closed_key_columns.as_ref(), true)?
            {
                selected_keys.insert(key);
            }
        }

        let mut closed = BTreeMap::new();
        for (row, weight) in delta_values {
            if *weight <= 0 {
                continue;
            }
            let Some(key) = extract_encoded_row_columns(row, closed_key_columns.as_ref(), true)?
            else {
                continue;
            };
            if selected_keys.contains(&key) {
                continue;
            }
            *closed.entry(key).or_insert(0_i64) += *weight;
        }
        Ok(closed.into_iter().collect())
    })))
}

fn try_build_transient_source_root_materialization(
    plan: &CircuitPlan,
    root_idx: usize,
) -> Result<Option<TransientSourceRootMaterialization>> {
    if let Some(shape) = find_transient_source_root_shape(plan, root_idx)? {
        let source_name = canonical_source_name(shape.source_name());
        let optimized_nodes = match &shape {
            TransientSourceRootShape::Source {
                optimized_nodes, ..
            }
            | TransientSourceRootShape::Select {
                optimized_nodes, ..
            }
            | TransientSourceRootShape::Project {
                optimized_nodes, ..
            }
            | TransientSourceRootShape::FilterMap {
                optimized_nodes, ..
            } => optimized_nodes.clone(),
        };
        let transform = match match shape {
            TransientSourceRootShape::Source { .. } => {
                Ok(Arc::new(|deltas: &[(Vec<u8>, i64)]| Ok(deltas.to_vec()))
                    as Arc<DeltaTransformFn>)
            }
            TransientSourceRootShape::Select { select, .. } => build_filter_transform(&select),
            TransientSourceRootShape::Project { project, .. } => build_map_transform(&project),
            TransientSourceRootShape::FilterMap {
                select, project, ..
            } => build_filter_map_transform(&select, &project),
        } {
            Ok(transform) => transform,
            Err(err) => {
                tracing::debug!(
                    root_idx,
                    error = %err,
                    "transient source root materialization declined"
                );
                return Ok(None);
            }
        };
        return Ok(Some(TransientSourceRootMaterialization {
            source_name,
            optimized_nodes,
            transform,
        }));
    }

    let Some(root) = plan.node(root_idx) else {
        return Ok(None);
    };
    match &root.kind {
        DbspNodeKind::Passthrough => {
            let input_idx = first_input(root, "passthrough")?;
            let Some(mut shape) = try_build_transient_source_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Select(select) => {
            let input_idx = first_input(root, "select")?;
            let Some(mut shape) = try_build_transient_source_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.transform = compose_delta_transforms(
                Arc::clone(&shape.transform),
                build_filter_transform(select)?,
            );
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Project(project) => {
            let input_idx = first_input(root, "project")?;
            if let Some(select_input_idx) = fuseable_select_input(plan, root_idx, input_idx)? {
                let Some(select_node) = plan.node(input_idx) else {
                    return Ok(None);
                };
                let DbspNodeKind::Select(select) = &select_node.kind else {
                    return Ok(None);
                };
                let Some(mut shape) =
                    try_build_transient_source_root_materialization(plan, select_input_idx)?
                else {
                    return Ok(None);
                };
                shape.transform = compose_delta_transforms(
                    Arc::clone(&shape.transform),
                    build_filter_map_transform(select, project)?,
                );
                shape.optimized_nodes.push(input_idx);
                shape.optimized_nodes.push(root_idx);
                return Ok(Some(shape));
            }

            let Some(mut shape) = try_build_transient_source_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.transform = compose_delta_transforms(
                Arc::clone(&shape.transform),
                build_map_transform(project)?,
            );
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        _ => Ok(None),
    }
}

fn canonical_source_name(source_name: &str) -> String {
    match source_name {
        "person" => "nexmark_person".to_string(),
        "auction" => "nexmark_auction".to_string(),
        "bid" => "nexmark_bid".to_string(),
        _ => source_name.to_string(),
    }
}

fn try_build_transient_source_topn_root_shape(
    plan: &CircuitPlan,
    root_idx: usize,
) -> Result<Option<TransientSourceTopNRootShape>> {
    let Some(root) = plan.node(root_idx) else {
        return Ok(None);
    };
    match &root.kind {
        DbspNodeKind::TopN(topn) => {
            let input_idx = first_input(root, "topn")?;
            let Some(source_root) =
                try_build_transient_source_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            let mut optimized_nodes = source_root.optimized_nodes.clone();
            optimized_nodes.push(root_idx);
            Ok(Some(TransientSourceTopNRootShape {
                source_root,
                topn: topn.clone(),
                optimized_nodes,
                transform: None,
                output_projection: None,
            }))
        }
        DbspNodeKind::Passthrough => {
            let input_idx = first_input(root, "passthrough")?;
            let Some(mut shape) = try_build_transient_source_topn_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Select(select) => {
            let input_idx = first_input(root, "select")?;
            let Some(mut shape) = try_build_transient_source_topn_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            fold_topn_root_output_projection(&mut shape);
            shape.transform = compose_optional_delta_transform(
                shape.transform.take(),
                build_filter_transform(select)?,
            );
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Project(project) => {
            let input_idx = first_input(root, "project")?;
            if let Some(select_input_idx) = fuseable_select_input(plan, root_idx, input_idx)? {
                let Some(select_node) = plan.node(input_idx) else {
                    return Ok(None);
                };
                let DbspNodeKind::Select(select) = &select_node.kind else {
                    return Ok(None);
                };
                let Some(mut shape) =
                    try_build_transient_source_topn_root_shape(plan, select_input_idx)?
                else {
                    return Ok(None);
                };
                fold_topn_root_output_projection(&mut shape);
                shape.transform = compose_optional_delta_transform(
                    shape.transform.take(),
                    build_filter_map_transform(select, project)?,
                );
                shape.optimized_nodes.push(input_idx);
                shape.optimized_nodes.push(root_idx);
                return Ok(Some(shape));
            }

            let Some(mut shape) = try_build_transient_source_topn_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            if let Some(columns) = try_build_direct_row_projection(project) {
                if shape.transform.is_none() {
                    shape.output_projection = Some(compose_direct_row_projection(
                        shape.output_projection.take(),
                        columns,
                    )?);
                } else {
                    shape.transform = compose_optional_delta_transform(
                        shape.transform.take(),
                        build_direct_projection_transform(columns),
                    );
                }
            } else {
                fold_topn_root_output_projection(&mut shape);
                shape.transform = compose_optional_delta_transform(
                    shape.transform.take(),
                    build_map_transform(project)?,
                );
            }
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        _ => Ok(None),
    }
}

fn try_build_transient_source_aggregate_root_shape(
    plan: &CircuitPlan,
    root_idx: usize,
) -> Result<Option<TransientSourceAggregateRootShape>> {
    let Some(root) = plan.node(root_idx) else {
        return Ok(None);
    };
    match &root.kind {
        DbspNodeKind::Aggregate(aggregate) => {
            let input_idx = first_input(root, "aggregate")?;
            let Some(source_root) =
                try_build_transient_source_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            if build_incremental_aggregate_slot_kinds(aggregate.aggregates()).is_none() {
                return Ok(None);
            }
            let mut optimized_nodes = source_root.optimized_nodes.clone();
            optimized_nodes.push(root_idx);
            Ok(Some(TransientSourceAggregateRootShape {
                source_root,
                aggregate: aggregate.clone(),
                optimized_nodes,
                transform: Arc::new(|deltas: &[(Vec<u8>, i64)]| Ok(deltas.to_vec())),
            }))
        }
        DbspNodeKind::Passthrough => {
            let input_idx = first_input(root, "passthrough")?;
            let Some(mut shape) = try_build_transient_source_aggregate_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Select(select) => {
            let input_idx = first_input(root, "select")?;
            let Some(mut shape) = try_build_transient_source_aggregate_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.transform = compose_delta_transforms(
                Arc::clone(&shape.transform),
                build_filter_transform(select)?,
            );
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Project(project) => {
            let input_idx = first_input(root, "project")?;
            if let Some(select_input_idx) = fuseable_select_input(plan, root_idx, input_idx)? {
                let Some(select_node) = plan.node(input_idx) else {
                    return Ok(None);
                };
                let DbspNodeKind::Select(select) = &select_node.kind else {
                    return Ok(None);
                };
                let Some(mut shape) =
                    try_build_transient_source_aggregate_root_shape(plan, select_input_idx)?
                else {
                    return Ok(None);
                };
                shape.transform = compose_delta_transforms(
                    Arc::clone(&shape.transform),
                    build_filter_map_transform(select, project)?,
                );
                shape.optimized_nodes.push(input_idx);
                shape.optimized_nodes.push(root_idx);
                return Ok(Some(shape));
            }

            let Some(mut shape) = try_build_transient_source_aggregate_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.transform = compose_delta_transforms(
                Arc::clone(&shape.transform),
                build_map_transform(project)?,
            );
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        _ => Ok(None),
    }
}

fn try_build_transient_source_window_count_star_root_shape(
    plan: &CircuitPlan,
    root_idx: usize,
) -> Result<Option<TransientSourceWindowCountStarRootShape>> {
    let Some(root) = plan.node(root_idx) else {
        return Ok(None);
    };
    match &root.kind {
        DbspNodeKind::WindowAggregate(window) => {
            if !is_transient_window_count_star_root(window) {
                return Ok(None);
            }
            let input_idx = first_input(root, "window aggregate")?;
            let Some(source_root) =
                try_build_transient_source_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            let mut optimized_nodes = source_root.optimized_nodes.clone();
            optimized_nodes.push(root_idx);
            Ok(Some(TransientSourceWindowCountStarRootShape {
                source_root,
                window: window.clone(),
                optimized_nodes,
                transform: Arc::new(|deltas: &[(Vec<u8>, i64)]| Ok(deltas.to_vec())),
            }))
        }
        DbspNodeKind::Passthrough => {
            let input_idx = first_input(root, "passthrough")?;
            let Some(mut shape) =
                try_build_transient_source_window_count_star_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Select(select) => {
            let input_idx = first_input(root, "select")?;
            let Some(mut shape) =
                try_build_transient_source_window_count_star_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.transform = compose_delta_transforms(
                Arc::clone(&shape.transform),
                build_filter_transform(select)?,
            );
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Project(project) => {
            let input_idx = first_input(root, "project")?;
            if let Some(select_input_idx) = fuseable_select_input(plan, root_idx, input_idx)? {
                let Some(select_node) = plan.node(input_idx) else {
                    return Ok(None);
                };
                let DbspNodeKind::Select(select) = &select_node.kind else {
                    return Ok(None);
                };
                let Some(mut shape) = try_build_transient_source_window_count_star_root_shape(
                    plan,
                    select_input_idx,
                )?
                else {
                    return Ok(None);
                };
                shape.transform = compose_delta_transforms(
                    Arc::clone(&shape.transform),
                    build_filter_map_transform(select, project)?,
                );
                shape.optimized_nodes.push(input_idx);
                shape.optimized_nodes.push(root_idx);
                return Ok(Some(shape));
            }

            let Some(mut shape) =
                try_build_transient_source_window_count_star_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.transform = compose_delta_transforms(
                Arc::clone(&shape.transform),
                build_map_transform(project)?,
            );
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        _ => Ok(None),
    }
}

async fn try_build_transient_source_window_count_star_root_materialization(
    plan: &CircuitPlan,
    root_idx: usize,
    outer_transient_streams: &HashMap<String, TransientSourceHandleStream>,
    watermark: Arc<AtomicI64>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    graph_id: &str,
    state_table: Option<Arc<dyn KeyValueTable>>,
) -> Result<Option<TransientSourceWindowCountStarRootMaterialization>> {
    let Some(shape) = try_build_transient_source_window_count_star_root_shape(plan, root_idx)?
    else {
        return Ok(None);
    };
    let Some(upstream) = outer_transient_streams
        .get(&shape.source_root.source_name)
        .cloned()
    else {
        return Ok(None);
    };
    let receiver = build_transient_window_count_star_receiver(
        graph_id,
        &shape.window,
        upstream,
        Arc::clone(&shape.source_root.transform),
        Arc::clone(&shape.transform),
        watermark,
        cancel,
        task_events,
        state_table,
        "source_window_count_star",
    )
    .await?;
    Ok(Some(TransientSourceWindowCountStarRootMaterialization {
        source_name: shape.source_root.source_name,
        optimized_nodes: shape.optimized_nodes,
        receiver,
    }))
}

fn try_build_transient_source_window_aggregate_root_shape(
    plan: &CircuitPlan,
    root_idx: usize,
) -> Result<Option<TransientSourceWindowAggregateRootShape>> {
    let Some(root) = plan.node(root_idx) else {
        return Ok(None);
    };
    match &root.kind {
        DbspNodeKind::WindowAggregate(window) => {
            if !is_transient_window_incremental_root(window) {
                return Ok(None);
            }
            let input_idx = first_input(root, "window aggregate")?;
            let Some(source_root) =
                try_build_transient_source_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            let mut optimized_nodes = source_root.optimized_nodes.clone();
            optimized_nodes.push(root_idx);
            Ok(Some(TransientSourceWindowAggregateRootShape {
                source_root,
                window: window.clone(),
                optimized_nodes,
                transform: Arc::new(|deltas: &[(Vec<u8>, i64)]| Ok(deltas.to_vec())),
            }))
        }
        DbspNodeKind::Passthrough => {
            let input_idx = first_input(root, "passthrough")?;
            let Some(mut shape) =
                try_build_transient_source_window_aggregate_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Select(select) => {
            let input_idx = first_input(root, "select")?;
            let Some(mut shape) =
                try_build_transient_source_window_aggregate_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.transform = compose_delta_transforms(
                Arc::clone(&shape.transform),
                build_filter_transform(select)?,
            );
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Project(project) => {
            let input_idx = first_input(root, "project")?;
            if let Some(select_input_idx) = fuseable_select_input(plan, root_idx, input_idx)? {
                let Some(select_node) = plan.node(input_idx) else {
                    return Ok(None);
                };
                let DbspNodeKind::Select(select) = &select_node.kind else {
                    return Ok(None);
                };
                let Some(mut shape) =
                    try_build_transient_source_window_aggregate_root_shape(plan, select_input_idx)?
                else {
                    return Ok(None);
                };
                shape.transform = compose_delta_transforms(
                    Arc::clone(&shape.transform),
                    build_filter_map_transform(select, project)?,
                );
                shape.optimized_nodes.push(input_idx);
                shape.optimized_nodes.push(root_idx);
                return Ok(Some(shape));
            }

            let Some(mut shape) =
                try_build_transient_source_window_aggregate_root_shape(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.transform = compose_delta_transforms(
                Arc::clone(&shape.transform),
                build_map_transform(project)?,
            );
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        _ => Ok(None),
    }
}

async fn try_build_transient_source_window_aggregate_root_materialization(
    plan: &CircuitPlan,
    root_idx: usize,
    outer_transient_streams: &HashMap<String, TransientSourceHandleStream>,
    watermark: Arc<AtomicI64>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    graph_id: &str,
    state_table: Option<Arc<dyn KeyValueTable>>,
) -> Result<Option<TransientSourceWindowAggregateRootMaterialization>> {
    let Some(shape) = try_build_transient_source_window_aggregate_root_shape(plan, root_idx)?
    else {
        return Ok(None);
    };
    let Some(upstream) = outer_transient_streams
        .get(&shape.source_root.source_name)
        .cloned()
    else {
        return Ok(None);
    };
    let receiver = build_transient_window_incremental_receiver(
        graph_id,
        &shape.window,
        upstream,
        Arc::clone(&shape.source_root.transform),
        Arc::clone(&shape.transform),
        watermark,
        cancel,
        task_events,
        state_table,
        "source_window_aggregate",
    )
    .await?;
    Ok(Some(TransientSourceWindowAggregateRootMaterialization {
        source_name: shape.source_root.source_name,
        optimized_nodes: shape.optimized_nodes,
        receiver,
    }))
}

async fn try_build_transient_source_aggregate_root_materialization(
    plan: &CircuitPlan,
    root_idx: usize,
    outer_transient_streams: &HashMap<String, TransientSourceHandleStream>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    graph_id: &str,
    state_table: Option<Arc<dyn KeyValueTable>>,
) -> Result<Option<TransientSourceAggregateRootMaterialization>> {
    let Some(shape) = try_build_transient_source_aggregate_root_shape(plan, root_idx)? else {
        return Ok(None);
    };
    let Some(upstream) = outer_transient_streams
        .get(&shape.source_root.source_name)
        .cloned()
    else {
        return Ok(None);
    };
    let receiver = build_transient_aggregate_receiver(
        graph_id,
        &shape.aggregate,
        upstream,
        Arc::clone(&shape.source_root.transform),
        Arc::clone(&shape.transform),
        cancel,
        task_events,
        state_table,
        "source_aggregate",
    )
    .await?;
    Ok(Some(TransientSourceAggregateRootMaterialization {
        source_name: shape.source_root.source_name,
        optimized_nodes: shape.optimized_nodes,
        receiver,
    }))
}

fn build_transient_source_receiver(
    graph_id: &str,
    task_label: impl Into<String>,
    upstream: TransientSourceHandleStream,
    input_transform: Arc<DeltaTransformFn>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
) -> mpsc::UnboundedReceiver<TransientMaterializeBatch> {
    let mut upstream_rx = upstream.subscribe();
    let (tx, rx) = mpsc::unbounded_channel::<TransientMaterializeBatch>();
    let graph_id = graph_id.to_string();
    let task_label = task_label.into();
    let task_events = task_events.clone();
    let cancel = cancel.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                maybe_batch = upstream_rx.recv() => {
                    let Some(batch) = maybe_batch else {
                        break;
                    };
                    let input_deltas = match input_transform(batch.deltas.as_ref()) {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if tx.send(TransientMaterializeBatch {
                        version: batch.version,
                        deltas: Arc::new(input_deltas),
                    }).is_err() {
                        break;
                    }
                }
            }
        }
    });
    rx
}

fn build_transient_transform_receiver(
    graph_id: &str,
    task_label: impl Into<String>,
    mut upstream: mpsc::UnboundedReceiver<TransientMaterializeBatch>,
    transform: Arc<DeltaTransformFn>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
) -> mpsc::UnboundedReceiver<TransientMaterializeBatch> {
    let (tx, rx) = mpsc::unbounded_channel::<TransientMaterializeBatch>();
    let graph_id = graph_id.to_string();
    let task_label = task_label.into();
    let task_events = task_events.clone();
    let cancel = cancel.clone();
    let debug_transient_join = std::env::var_os("FLOE_DEBUG_TRANSIENT_JOIN").is_some();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                maybe_batch = upstream.recv() => {
                    let Some(batch) = maybe_batch else {
                        break;
                    };
                    let output_deltas = match transform(batch.deltas.as_ref()) {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if debug_transient_join {
                        eprintln!(
                            "transient-transform-output graph_id={} task={} version={} rows={}",
                            graph_id,
                            task_label,
                            batch.version,
                            output_deltas.len()
                        );
                    }
                    if tx.send(TransientMaterializeBatch {
                        version: batch.version,
                        deltas: Arc::new(output_deltas),
                    }).is_err() {
                        break;
                    }
                }
            }
        }
    });
    rx
}

async fn build_transient_aggregate_receiver(
    graph_id: &str,
    aggregate: &DbspAggregateNode,
    upstream: TransientSourceHandleStream,
    input_transform: Arc<DeltaTransformFn>,
    output_transform: Arc<DeltaTransformFn>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> Result<mpsc::UnboundedReceiver<TransientMaterializeBatch>> {
    let compact_source_state = upstream.recoverable();
    let upstream_rx = build_transient_source_receiver(
        graph_id,
        format!("transient-aggregate-source:{graph_id}"),
        upstream,
        input_transform,
        cancel,
        task_events,
    );
    build_transient_aggregate_receiver_from_batches(
        graph_id,
        aggregate,
        upstream_rx,
        output_transform,
        // Source-journal deltas are signed ZSet updates. Do not enable
        // append-only aggregate shortcuts without explicit source metadata.
        false,
        compact_source_state,
        cancel,
        task_events,
        state_table,
        state_label,
    )
    .await
}

async fn build_transient_aggregate_receiver_from_batches(
    graph_id: &str,
    aggregate: &DbspAggregateNode,
    mut upstream_rx: mpsc::UnboundedReceiver<TransientMaterializeBatch>,
    output_transform: Arc<DeltaTransformFn>,
    append_only_input: bool,
    compact_source_state: bool,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> Result<mpsc::UnboundedReceiver<TransientMaterializeBatch>> {
    let (tx, rx) = mpsc::unbounded_channel::<TransientMaterializeBatch>();
    let (precompute_evaluator, aggregate_input_schema, aggregate_expression_columns) =
        build_transient_aggregate_precompute(aggregate)?;
    let graph_id = graph_id.to_string();
    let task_label = format!("transient-aggregate:{graph_id}");
    let task_events = task_events.clone();
    let cancel = cancel.clone();
    let state_label = state_label.into();
    let debug_transient_join = std::env::var_os("FLOE_DEBUG_TRANSIENT_JOIN").is_some();
    if aggregate
        .aggregates()
        .iter()
        .all(|agg| agg.function() == &dbsp::DbspAggregateFunction::Count)
    {
        let slot_kinds = build_count_aggregate_slot_kinds(aggregate.aggregates());
        let row_evaluator = build_count_row_evaluator(
            Arc::clone(&aggregate_input_schema),
            aggregate.group_keys().to_vec(),
            aggregate.aggregates().to_vec(),
            Arc::clone(&aggregate_expression_columns),
            graph_id.clone(),
            "transient_count_aggregate",
        );
        let aggregate_processor = Arc::new(
            dbsp::DbspTransientCountAggregate::<Vec<u8>, Vec<u8>, Vec<u8>>::new(
                row_evaluator,
                slot_kinds,
            )
            .await
            .context("initialize transient count aggregate")?,
        );
        let count_state_label = if compact_source_state {
            format!("{state_label}_count_state")
        } else {
            state_label.clone()
        };
        let mut persistent_state =
            PersistentTransientInputState::load(state_table.clone(), &graph_id, count_state_label)
                .await?;
        let restored_deltas = persistent_state.snapshot_deltas();
        if !restored_deltas.is_empty() {
            if compact_source_state {
                let snapshot = decode_transient_count_aggregate_snapshot(restored_deltas)
                    .context("decode transient count aggregate state snapshot")?;
                aggregate_processor.restore_state(snapshot).await;
            } else {
                aggregate_processor
                    .apply_deltas(restored_deltas)
                    .await
                    .context("restore transient count aggregate input state")?;
            }
        }
        let precompute_evaluator = precompute_evaluator.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                            match evaluator.transform_delta(&graph_id, &input_deltas) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            }
                        } else {
                            input_deltas
                        };
                        if !compact_source_state {
                            if let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let aggregate_deltas = match aggregate_processor.apply_deltas(input_deltas).await {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if compact_source_state {
                            let snapshot = aggregate_processor.snapshot_state().await;
                            let encoded_snapshot = match encode_transient_count_aggregate_snapshot(snapshot) {
                                Ok(snapshot) => snapshot,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            };
                            if let Err(err) = persistent_state.replace_with_snapshot(encoded_snapshot).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let encoded_output = match encode_count_aggregate_output_deltas(aggregate_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        let final_deltas = match output_transform(&encoded_output) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if debug_transient_join {
                            eprintln!(
                                "transient-aggregate-output graph_id={} version={} rows={}",
                                graph_id,
                                batch.version,
                                final_deltas.len()
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(final_deltas),
                        }).is_err() {
                            break;
                        }
                    }
                }
            }
        });
    } else {
        let slot_kinds = build_incremental_aggregate_slot_kinds(aggregate.aggregates())
            .ok_or_else(|| {
                anyhow!("aggregate is not eligible for transient incremental aggregation")
            })?;
        let row_evaluator = build_incremental_aggregate_row_evaluator(
            Arc::clone(&aggregate_input_schema),
            aggregate.group_keys().to_vec(),
            aggregate.aggregates().to_vec(),
            Arc::clone(&aggregate_expression_columns),
            graph_id.clone(),
            "transient_aggregate",
        );
        let aggregate_processor = Arc::new(
            dbsp::DbspTransientIncrementalAggregate::<Vec<u8>, Vec<u8>>::new(
                row_evaluator,
                slot_kinds,
            )
            .await
            .context("initialize transient incremental aggregate")?,
        );
        if append_only_input {
            aggregate_processor.enable_append_only_input().await;
        }
        let incremental_state_label = if compact_source_state {
            format!("{state_label}_incremental_state")
        } else {
            state_label.clone()
        };
        let mut persistent_state = PersistentTransientInputState::load(
            state_table.clone(),
            &graph_id,
            incremental_state_label,
        )
        .await?;
        let restored_deltas = persistent_state.snapshot_deltas();
        if !restored_deltas.is_empty() {
            if compact_source_state {
                let snapshot = decode_transient_incremental_aggregate_snapshot(restored_deltas)
                    .context("decode transient incremental aggregate state snapshot")?;
                aggregate_processor
                    .restore_state(snapshot)
                    .await
                    .context("restore transient incremental aggregate state snapshot")?;
            } else {
                aggregate_processor
                    .apply_deltas(restored_deltas)
                    .await
                    .context("restore transient incremental aggregate input state")?;
            }
        }
        let precompute_evaluator = precompute_evaluator.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                            match evaluator.transform_delta(&graph_id, &input_deltas) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            }
                        } else {
                            input_deltas
                        };
                        if !compact_source_state && let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                        let aggregate_deltas = match aggregate_processor.apply_deltas(input_deltas).await {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if compact_source_state {
                            let snapshot = match aggregate_processor.snapshot_state().await {
                                Ok(snapshot) => snapshot,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            };
                            let encoded_snapshot = match encode_transient_incremental_aggregate_snapshot(snapshot) {
                                Ok(snapshot) => snapshot,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            };
                            if let Err(err) = persistent_state.replace_with_snapshot(encoded_snapshot).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let encoded_output = match encode_incremental_aggregate_output_deltas(aggregate_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        let final_deltas = match output_transform(&encoded_output) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if debug_transient_join {
                            eprintln!(
                                "transient-aggregate-output graph_id={} version={} rows={}",
                                graph_id,
                                batch.version,
                                final_deltas.len()
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(final_deltas),
                        }).is_err() {
                            break;
                        }
                    }
                }
            }
        });
    }

    Ok(rx)
}

async fn build_transient_window_count_star_receiver(
    graph_id: &str,
    window: &dbsp::DbspWindowAggregateNode,
    upstream: TransientSourceHandleStream,
    input_transform: Arc<DeltaTransformFn>,
    output_transform: Arc<DeltaTransformFn>,
    watermark: Arc<AtomicI64>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> Result<mpsc::UnboundedReceiver<TransientMaterializeBatch>> {
    let compact_count_state = upstream.recoverable();
    let upstream_rx = build_transient_source_receiver(
        graph_id,
        format!("transient-window-count-star-source:{graph_id}"),
        upstream,
        input_transform,
        cancel,
        task_events,
    );
    build_transient_window_count_star_receiver_from_batches(
        graph_id,
        window,
        upstream_rx,
        output_transform,
        watermark,
        compact_count_state,
        cancel,
        task_events,
        state_table,
        state_label,
    )
    .await
}

async fn build_transient_window_count_star_receiver_from_batches(
    graph_id: &str,
    window: &dbsp::DbspWindowAggregateNode,
    mut upstream_rx: mpsc::UnboundedReceiver<TransientMaterializeBatch>,
    output_transform: Arc<DeltaTransformFn>,
    watermark: Arc<AtomicI64>,
    compact_count_state: bool,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> Result<mpsc::UnboundedReceiver<TransientMaterializeBatch>> {
    let (tx, rx) = mpsc::unbounded_channel::<TransientMaterializeBatch>();
    let (precompute_evaluator, eval_schema, expression_columns) =
        build_transient_window_count_star_precompute(window)?;
    let group_key_columns = transient_window_direct_group_key_columns(
        window.aggregate.group_keys(),
        eval_schema.as_ref(),
        expression_columns.as_ref(),
    )
    .ok_or_else(|| anyhow!("failed to resolve transient window count-star group key columns"))?;
    let time_column = transient_window_resolved_expression_column_index(
        &window.window.time_expression,
        eval_schema.as_ref(),
        expression_columns.as_ref(),
    )
    .ok_or_else(|| anyhow!("failed to resolve transient window count-star time column"))?;
    let (window_size, window_slide) = match &window.window.policy {
        dbsp::DbspWindowPolicy::Tumbling { size_ms } => (*size_ms, *size_ms),
        dbsp::DbspWindowPolicy::Hopping { size_ms, slide_ms } => (*size_ms, *slide_ms),
        dbsp::DbspWindowPolicy::Session { gap_ms } => (*gap_ms, *gap_ms),
    };
    let allowed_lateness_ms = window.window.allowed_lateness_ms;
    let group_key_columns = Arc::new(group_key_columns);
    let graph_id = graph_id.to_string();
    let task_label = format!("transient-window-count-star:{graph_id}");
    let task_events = task_events.clone();
    let cancel = cancel.clone();
    let state_label = state_label.into();
    let state_label = if compact_count_state {
        format!("{state_label}_counts")
    } else {
        state_label
    };
    let mut persistent_state =
        PersistentTransientInputState::load(state_table, &graph_id, state_label).await?;
    let restored_deltas = persistent_state.snapshot_deltas();
    tokio::spawn(async move {
        let mut counts: HashMap<TransientWindowCountKey, i64> = HashMap::new();
        let mut eviction_schedule: BTreeMap<i64, Vec<TransientWindowCountKey>> = BTreeMap::new();
        let restore_result = if compact_count_state {
            restore_transient_window_count_state(
                restored_deltas,
                &mut counts,
                &mut eviction_schedule,
            )
        } else {
            apply_transient_window_count_star_deltas(
                restored_deltas,
                group_key_columns.as_ref(),
                time_column,
                window_size,
                window_slide,
                transient_window_watermark_cutoff(&watermark, allowed_lateness_ms),
                &mut counts,
                &mut eviction_schedule,
            )
            .map(|_| ())
        };
        if let Err(err) = restore_result {
            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
            return;
        }
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                maybe_batch = upstream_rx.recv() => {
                    let Some(batch) = maybe_batch else {
                        break;
                    };
                    let input_deltas = batch.deltas.as_ref().clone();
                    let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                        match evaluator.transform_delta(&graph_id, &input_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                    } else {
                        input_deltas
                    };
                    if !compact_count_state {
                        if let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    }
                    let updates = match apply_transient_window_count_star_deltas(
                        input_deltas,
                        group_key_columns.as_ref(),
                        time_column,
                        window_size,
                        window_slide,
                        transient_window_watermark_cutoff(&watermark, allowed_lateness_ms),
                        &mut counts,
                        &mut eviction_schedule,
                    ) {
                        Ok(updates) => updates,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if compact_count_state {
                        let snapshot = match encode_transient_window_count_state(&counts) {
                            Ok(snapshot) => snapshot,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if let Err(err) = persistent_state.replace_with_snapshot(snapshot).await {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    }

                    let encoded_output = match encode_transient_window_count_output_deltas(updates) {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    let final_deltas = match output_transform(&encoded_output) {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if tx.send(TransientMaterializeBatch {
                        version: batch.version,
                        deltas: Arc::new(final_deltas),
                    }).is_err() {
                        break;
                    }
                }
            }
        }
    });
    Ok(rx)
}

async fn build_transient_window_incremental_receiver(
    graph_id: &str,
    window: &dbsp::DbspWindowAggregateNode,
    upstream: TransientSourceHandleStream,
    input_transform: Arc<DeltaTransformFn>,
    output_transform: Arc<DeltaTransformFn>,
    watermark: Arc<AtomicI64>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> Result<mpsc::UnboundedReceiver<TransientMaterializeBatch>> {
    let compact_source_state = upstream.recoverable();
    let upstream_rx = build_transient_source_receiver(
        graph_id,
        format!("transient-window-aggregate-source:{graph_id}"),
        upstream,
        input_transform,
        cancel,
        task_events,
    );
    build_transient_window_incremental_receiver_from_batches(
        graph_id,
        window,
        upstream_rx,
        output_transform,
        watermark,
        compact_source_state,
        cancel,
        task_events,
        state_table,
        state_label,
    )
    .await
}

async fn build_transient_window_incremental_receiver_from_batches(
    graph_id: &str,
    window: &dbsp::DbspWindowAggregateNode,
    mut upstream_rx: mpsc::UnboundedReceiver<TransientMaterializeBatch>,
    output_transform: Arc<DeltaTransformFn>,
    watermark: Arc<AtomicI64>,
    compact_source_state: bool,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> Result<mpsc::UnboundedReceiver<TransientMaterializeBatch>> {
    let (tx, rx) = mpsc::unbounded_channel::<TransientMaterializeBatch>();
    let (precompute_evaluator, eval_schema, expression_columns) =
        build_transient_window_aggregate_precompute(window)?;
    let group_key_columns = transient_window_direct_group_key_columns(
        window.aggregate.group_keys(),
        eval_schema.as_ref(),
        expression_columns.as_ref(),
    )
    .ok_or_else(|| anyhow!("failed to resolve transient window aggregate group key columns"))?;
    let time_column = transient_window_resolved_expression_column_index(
        &window.window.time_expression,
        eval_schema.as_ref(),
        expression_columns.as_ref(),
    )
    .ok_or_else(|| anyhow!("failed to resolve transient window aggregate time column"))?;
    let (window_size, window_slide) = match &window.window.policy {
        dbsp::DbspWindowPolicy::Tumbling { size_ms } => (*size_ms, *size_ms),
        dbsp::DbspWindowPolicy::Hopping { size_ms, slide_ms } => (*size_ms, *slide_ms),
        dbsp::DbspWindowPolicy::Session { gap_ms } => (*gap_ms, *gap_ms),
    };
    let allowed_lateness_ms = window.window.allowed_lateness_ms;
    let slot_kinds = build_incremental_aggregate_slot_kinds(window.aggregate.aggregates())
        .ok_or_else(|| {
            anyhow!("window aggregate is not eligible for transient incremental aggregation")
        })?;
    let row_evaluator = build_incremental_aggregate_row_evaluator(
        Arc::clone(&eval_schema),
        window.aggregate.group_keys().to_vec(),
        window.aggregate.aggregates().to_vec(),
        Arc::clone(&expression_columns),
        graph_id.to_string(),
        "transient_window_aggregate",
    );
    let row_evaluator = Arc::new(row_evaluator);
    let aggregate_processor = Arc::new(
        dbsp::DbspTransientIncrementalAggregate::<Vec<u8>, (Vec<u8>, Vec<u8>)>::new(
            {
                let row_evaluator = Arc::clone(&row_evaluator);
                move |pair: &(Vec<u8>, Vec<u8>)| {
                    row_evaluator(&pair.1).map(|mut row| {
                        row.key = pair.0.clone();
                        row
                    })
                }
            },
            slot_kinds,
        )
        .await
        .context("initialize transient window incremental aggregate")?,
    );
    let group_key_columns = Arc::new(group_key_columns);
    let graph_id = graph_id.to_string();
    let task_label = format!("transient-window-aggregate:{graph_id}");
    let task_events = task_events.clone();
    let cancel = cancel.clone();
    let state_label = state_label.into();
    let state_label = if compact_source_state {
        format!("{state_label}_incremental_state")
    } else {
        state_label
    };
    let mut persistent_state =
        PersistentTransientInputState::load(state_table, &graph_id, state_label).await?;
    let restored_state = persistent_state.snapshot_deltas();
    if !restored_state.is_empty() {
        if compact_source_state {
            let snapshot = decode_transient_window_incremental_aggregate_snapshot(restored_state)
                .context("decode transient window aggregate state snapshot")?;
            aggregate_processor
                .restore_state(snapshot)
                .await
                .context("restore transient window aggregate state snapshot")?;
        } else {
            let restored_deltas = restored_state
                .into_iter()
                .filter_map(|(row, weight)| {
                    match decode_transient_window_aggregate_input_pair(&row) {
                        Ok(pair) => Some((pair, weight)),
                        Err(err) => {
                            tracing::warn!(
                                graph_id = %graph_id,
                                error = %err,
                                "skipping malformed transient window aggregate input state row"
                            );
                            None
                        }
                    }
                })
                .collect::<Vec<_>>();
            aggregate_processor
                .apply_deltas(restored_deltas)
                .await
                .context("restore transient window aggregate input state")?;
        }
    }
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                maybe_batch = upstream_rx.recv() => {
                    let Some(batch) = maybe_batch else {
                        break;
                    };
                    let input_deltas = batch.deltas.as_ref().clone();
                    let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                        match evaluator.transform_delta(&graph_id, &input_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                    } else {
                        input_deltas
                    };
                    let cutoff = transient_window_watermark_cutoff(&watermark, allowed_lateness_ms);
                    let mut windowed_deltas = Vec::new();
                    let mut encoded_window_cache: HashMap<(i64, i64), Vec<u8>> = HashMap::new();
                    for (row, weight) in input_deltas {
                        if weight == 0 {
                            continue;
                        }
                        let (group_key, event_ts) = if group_key_columns.is_empty() {
                            match extract_encoded_row_i64_like_column(&row, time_column) {
                                Ok(Some(event_ts)) => (None, event_ts),
                                Ok(None) => continue,
                                Err(err) => {
                                    report_graph_task_error(
                                        &task_events,
                                        &graph_id,
                                        task_label.clone(),
                                        err.context("extract transient window aggregate timestamp"),
                                    );
                                    return;
                                }
                            }
                        } else {
                            match extract_encoded_row_columns_and_i64_like_column(
                                &row,
                                group_key_columns.as_ref(),
                                time_column,
                                false,
                            ) {
                                Ok(Some((group_key, event_ts))) => (Some(group_key), event_ts),
                                Ok(None) => continue,
                                Err(err) => {
                                    report_graph_task_error(
                                        &task_events,
                                        &graph_id,
                                        task_label.clone(),
                                        err.context("extract transient window aggregate row"),
                                    );
                                    return;
                                }
                            }
                        };
                        if event_ts < 0 {
                            continue;
                        }
                        if let Some(cutoff) = cutoff
                            && event_ts < cutoff
                        {
                            continue;
                        }
                        let mut build_window_key = |window_start: i64, window_end: i64| {
                            let encoded_window = if let Some(encoded) =
                                encoded_window_cache.get(&(window_start, window_end)).cloned()
                            {
                                encoded
                            } else {
                                match encode_transient_window_bounds(window_start, window_end) {
                                    Ok(encoded) => {
                                        encoded_window_cache
                                            .insert((window_start, window_end), encoded.clone());
                                        encoded
                                    }
                                    Err(err) => {
                                        report_graph_task_error(
                                            &task_events,
                                            &graph_id,
                                            task_label.clone(),
                                            err,
                                        );
                                        return None;
                                    }
                                }
                            };
                            if let Some(group_key) = group_key.as_ref() {
                                match concat_encoded_rows(&encoded_window, group_key) {
                                    Ok(encoded) => Some(encoded),
                                    Err(err) => {
                                        report_graph_task_error(
                                            &task_events,
                                            &graph_id,
                                            task_label.clone(),
                                            err,
                                        );
                                        None
                                    }
                                }
                            } else {
                                Some(encoded_window)
                            }
                        };
                        if window_size == window_slide {
                            let mut encoded_key = None;
                            transient_window_for_each_window(
                                event_ts,
                                window_size,
                                window_slide,
                                |window_start, window_end| {
                                    encoded_key = build_window_key(window_start, window_end);
                                },
                            );
                            if let Some(encoded_key) = encoded_key {
                                windowed_deltas.push(((encoded_key, row), weight));
                            }
                            continue;
                        }
                        let mut encoded_keys = Vec::new();
                        transient_window_for_each_window(
                            event_ts,
                            window_size,
                            window_slide,
                            |window_start, window_end| {
                                if let Some(encoded_key) = build_window_key(window_start, window_end)
                                {
                                    encoded_keys.push(encoded_key);
                                }
                            },
                        );
                        if encoded_keys.is_empty() {
                            continue;
                        }
                        let last_idx = encoded_keys.len() - 1;
                        let mut row = Some(row);
                        for (idx, encoded_key) in encoded_keys.into_iter().enumerate() {
                            let row_value = if idx == last_idx {
                                row.take().expect("transient window row already moved")
                            } else {
                                row.as_ref()
                                    .expect("transient window row missing")
                                    .clone()
                            };
                            windowed_deltas.push(((encoded_key, row_value), weight));
                        }
                    }
                    if !compact_source_state {
                        let persisted_window_rows = windowed_deltas
                            .iter()
                            .map(|((window_key, row), weight)| {
                                encode_transient_window_aggregate_input_pair(window_key, row)
                                    .map(|encoded| (encoded, *weight))
                            })
                            .collect::<Result<Vec<_>>>();
                        let persisted_window_rows = match persisted_window_rows {
                            Ok(rows) => rows,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if let Err(err) = persistent_state.apply_deltas(&persisted_window_rows).await {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    }
                    let aggregate_deltas = match aggregate_processor.apply_deltas(windowed_deltas).await {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if compact_source_state {
                        let snapshot = match aggregate_processor.snapshot_state().await {
                            Ok(snapshot) => snapshot,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        let encoded_snapshot = match encode_transient_window_incremental_aggregate_snapshot(snapshot) {
                            Ok(snapshot) => snapshot,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if let Err(err) = persistent_state.replace_with_snapshot(encoded_snapshot).await {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    }
                    let encoded_output = match encode_incremental_aggregate_output_deltas(aggregate_deltas) {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    let final_deltas = match output_transform(&encoded_output) {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if tx.send(TransientMaterializeBatch {
                        version: batch.version,
                        deltas: Arc::new(final_deltas),
                    }).is_err() {
                        break;
                    }
                }
            }
        }
    });
    Ok(rx)
}

fn build_transient_aggregate_precompute(
    aggregate: &DbspAggregateNode,
) -> Result<(
    Option<Arc<VectorizedFilterProjectEvaluator>>,
    Arc<RowSchema>,
    Arc<HashMap<String, usize>>,
)> {
    let input_schema = Arc::clone(aggregate.input_schema());
    let mut expressions = Vec::new();
    expressions.extend(
        aggregate
            .group_keys()
            .iter()
            .map(|group_key| group_key.expression().clone()),
    );
    for agg in aggregate.aggregates() {
        if let Some(filter) = agg.filter() {
            expressions.push(filter.clone());
        }
        if let Some(expr) = agg.expression() {
            expressions.push(expr.clone());
        }
    }

    let mut direct_input_columns = BTreeSet::new();
    let mut seen = HashSet::new();
    let mut non_direct_expressions = Vec::new();
    for expr in &expressions {
        if let Some(column_idx) =
            transient_aggregate_direct_column_index(expr, input_schema.as_ref())
        {
            direct_input_columns.insert(column_idx);
            continue;
        }
        let key = transient_aggregate_expression_lookup_key(expr.expr());
        if seen.insert(key.clone()) {
            non_direct_expressions.push((key, expr.expr().clone()));
        }
    }
    if non_direct_expressions.is_empty() {
        return Ok((None, input_schema, Arc::new(HashMap::new())));
    }

    let mut items = Vec::with_capacity(direct_input_columns.len() + non_direct_expressions.len());
    for column_idx in direct_input_columns {
        let field = input_schema
            .field(column_idx)
            .ok_or_else(|| anyhow!("transient aggregate input column {column_idx} missing"))?;
        items.push(dbsp::circuit::plan::ProjectItem {
            expr: Expr::Column(Column::new_unqualified(field.name.clone())),
            alias: Some(field.name.clone()),
        });
    }

    let mut expression_columns = HashMap::with_capacity(non_direct_expressions.len());
    let mut next_index = items.len();
    for (index, (key, expr)) in non_direct_expressions.into_iter().enumerate() {
        let alias = format!("__floe_transient_aggregate_expr_{index}");
        items.push(dbsp::circuit::plan::ProjectItem {
            expr,
            alias: Some(alias),
        });
        expression_columns.insert(key, next_index);
        next_index += 1;
    }

    let project_node = DbspProjectNode::try_new(Arc::clone(&input_schema), items)
        .context("build transient aggregate expression precompute projection")?;
    let evaluator = VectorizedFilterProjectEvaluator::for_map(
        project_node.expressions(),
        Arc::clone(&input_schema),
    )
    .context("initialize transient aggregate precompute evaluator")?;
    Ok((
        Some(Arc::new(evaluator)),
        Arc::clone(project_node.output_schema()),
        Arc::new(expression_columns),
    ))
}

fn build_transient_window_count_star_precompute(
    window: &dbsp::DbspWindowAggregateNode,
) -> Result<(
    Option<Arc<VectorizedFilterProjectEvaluator>>,
    Arc<RowSchema>,
    Arc<HashMap<String, usize>>,
)> {
    let input_schema = Arc::clone(window.aggregate.input_schema());
    let mut expressions = Vec::new();
    expressions.extend(
        window
            .aggregate
            .group_keys()
            .iter()
            .map(|group_key| group_key.expression().clone()),
    );
    expressions.push(window.window.time_expression.clone());
    build_transient_expression_precompute(input_schema, expressions, "__floe_transient_window_expr")
}

fn build_transient_window_aggregate_precompute(
    window: &dbsp::DbspWindowAggregateNode,
) -> Result<(
    Option<Arc<VectorizedFilterProjectEvaluator>>,
    Arc<RowSchema>,
    Arc<HashMap<String, usize>>,
)> {
    let input_schema = Arc::clone(window.aggregate.input_schema());
    let mut expressions = Vec::new();
    expressions.extend(
        window
            .aggregate
            .group_keys()
            .iter()
            .map(|group_key| group_key.expression().clone()),
    );
    expressions.push(window.window.time_expression.clone());
    for agg in window.aggregate.aggregates() {
        if let Some(filter) = agg.filter() {
            expressions.push(filter.clone());
        }
        if let Some(expr) = agg.expression() {
            expressions.push(expr.clone());
        }
    }
    build_transient_expression_precompute(
        input_schema,
        expressions,
        "__floe_transient_window_aggregate_expr",
    )
}

fn build_transient_expression_precompute(
    input_schema: Arc<RowSchema>,
    expressions: Vec<DbspExpression>,
    alias_prefix: &str,
) -> Result<(
    Option<Arc<VectorizedFilterProjectEvaluator>>,
    Arc<RowSchema>,
    Arc<HashMap<String, usize>>,
)> {
    let mut direct_input_columns = BTreeSet::new();
    let mut seen = HashSet::new();
    let mut non_direct_expressions = Vec::new();
    for expr in &expressions {
        if let Some(column_idx) =
            transient_aggregate_direct_column_index(expr, input_schema.as_ref())
        {
            direct_input_columns.insert(column_idx);
            continue;
        }
        let key = transient_aggregate_expression_lookup_key(expr.expr());
        if seen.insert(key.clone()) {
            non_direct_expressions.push((key, expr.expr().clone()));
        }
    }
    if non_direct_expressions.is_empty() {
        return Ok((None, input_schema, Arc::new(HashMap::new())));
    }

    let mut items = Vec::with_capacity(direct_input_columns.len() + non_direct_expressions.len());
    for column_idx in direct_input_columns {
        let field = input_schema
            .field(column_idx)
            .ok_or_else(|| anyhow!("transient expression input column {column_idx} missing"))?;
        items.push(dbsp::circuit::plan::ProjectItem {
            expr: Expr::Column(Column::new_unqualified(field.name.clone())),
            alias: Some(field.name.clone()),
        });
    }

    let mut expression_columns = HashMap::with_capacity(non_direct_expressions.len());
    let mut next_index = items.len();
    for (index, (key, expr)) in non_direct_expressions.into_iter().enumerate() {
        let alias = format!("{alias_prefix}_{index}");
        items.push(dbsp::circuit::plan::ProjectItem {
            expr,
            alias: Some(alias),
        });
        expression_columns.insert(key, next_index);
        next_index += 1;
    }

    let project_node = DbspProjectNode::try_new(Arc::clone(&input_schema), items)
        .context("build transient expression precompute projection")?;
    let evaluator = VectorizedFilterProjectEvaluator::for_map(
        project_node.expressions(),
        Arc::clone(&input_schema),
    )
    .context("initialize transient expression precompute evaluator")?;
    Ok((
        Some(Arc::new(evaluator)),
        Arc::clone(project_node.output_schema()),
        Arc::new(expression_columns),
    ))
}

fn transient_aggregate_direct_column_index(
    expression: &DbspExpression,
    schema: &RowSchema,
) -> Option<usize> {
    match expression.expr() {
        Expr::Alias(alias) => {
            transient_aggregate_direct_column_index_expression(alias.expr.as_ref(), schema)
        }
        other => transient_aggregate_direct_column_index_expression(other, schema),
    }
}

fn transient_aggregate_direct_column_index_expression(
    expr: &Expr,
    schema: &RowSchema,
) -> Option<usize> {
    match expr {
        Expr::Column(column) => projection_resolve_direct_column(schema, column),
        Expr::Alias(alias) => {
            transient_aggregate_direct_column_index_expression(alias.expr.as_ref(), schema)
        }
        _ => None,
    }
}

fn transient_aggregate_expression_lookup_key(expr: &Expr) -> String {
    match expr {
        Expr::Alias(alias) => transient_aggregate_expression_lookup_key(alias.expr.as_ref()),
        other => other.to_string(),
    }
}

fn transient_window_direct_group_key_columns(
    group_keys: &[dbsp::circuit::plan::GroupKeyExpr],
    schema: &RowSchema,
    expression_columns: &HashMap<String, usize>,
) -> Option<Vec<usize>> {
    group_keys
        .iter()
        .map(|key_expr| {
            transient_window_resolved_expression_column_index(
                key_expr.expression(),
                schema,
                expression_columns,
            )
        })
        .collect()
}

fn transient_window_resolved_expression_column_index(
    expression: &DbspExpression,
    schema: &RowSchema,
    expression_columns: &HashMap<String, usize>,
) -> Option<usize> {
    transient_aggregate_direct_column_index(expression, schema).or_else(|| {
        expression_columns
            .get(&transient_aggregate_expression_lookup_key(
                expression.expr(),
            ))
            .copied()
    })
}

fn is_transient_window_count_star_root(window: &dbsp::DbspWindowAggregateNode) -> bool {
    let aggregates = window.aggregate.aggregates();
    aggregates.len() == 1
        && aggregates.iter().all(|agg| {
            agg.function() == &dbsp::DbspAggregateFunction::Count
                && !agg.distinct()
                && agg.filter().is_none()
                && agg.expression().is_none_or(|expr| match expr.expr() {
                    Expr::Literal(value, _) => !value.is_null(),
                    _ => false,
                })
        })
}

fn is_transient_window_incremental_root(window: &dbsp::DbspWindowAggregateNode) -> bool {
    build_incremental_aggregate_slot_kinds(window.aggregate.aggregates()).is_some()
}

fn transient_window_for_each_window<F>(ts: i64, window_size: i64, window_slide: i64, mut visit: F)
where
    F: FnMut(i64, i64),
{
    if window_size == window_slide {
        let start = ts.div_euclid(window_slide) * window_slide;
        visit(start, start + window_size);
        return;
    }

    let latest_start = ts.div_euclid(window_slide) * window_slide;
    let count = (window_size / window_slide).max(1);
    let first_start = latest_start - (count - 1) * window_slide;
    for i in 0..count {
        let start = first_start + i * window_slide;
        visit(start, start + window_size);
    }
}

fn transient_window_watermark_cutoff(
    watermark: &AtomicI64,
    allowed_lateness_ms: i64,
) -> Option<i64> {
    let watermark = watermark.load(Ordering::Relaxed);
    if watermark < 0 {
        return None;
    }
    Some(watermark.saturating_sub(allowed_lateness_ms.max(0)))
}

fn merge_i64_delta(
    map: &mut HashMap<TransientWindowCountKey, i64>,
    key: TransientWindowCountKey,
    delta: i64,
) {
    if delta == 0 {
        return;
    }
    let entry = map.entry(key.clone()).or_insert(0);
    *entry += delta;
    if *entry == 0 {
        map.remove(&key);
    }
}

fn merge_count_delta(
    updates: &mut HashMap<(TransientWindowCountKey, i64), i64>,
    key: TransientWindowCountKey,
    count: i64,
    diff: i64,
) {
    if diff == 0 {
        return;
    }
    let pair = (key, count);
    let entry = updates.entry(pair.clone()).or_insert(0);
    *entry += diff;
    if *entry == 0 {
        updates.remove(&pair);
    }
}

fn transient_window_evict_expired_counts(
    cutoff: Option<i64>,
    counts: &mut HashMap<TransientWindowCountKey, i64>,
    eviction_schedule: &mut BTreeMap<i64, Vec<TransientWindowCountKey>>,
    updates: &mut HashMap<(TransientWindowCountKey, i64), i64>,
) {
    let Some(cutoff) = cutoff else {
        return;
    };
    let retained = eviction_schedule.split_off(&(cutoff + 1));
    let expired = std::mem::replace(eviction_schedule, retained);
    for (_, keys) in expired {
        for key in keys {
            let Some(old_count) = counts.remove(&key) else {
                continue;
            };
            merge_count_delta(updates, key, old_count, -1);
        }
    }
}

fn apply_transient_window_count_star_deltas(
    input_deltas: Vec<(Vec<u8>, i64)>,
    group_key_columns: &[usize],
    time_column: usize,
    window_size: i64,
    window_slide: i64,
    cutoff: Option<i64>,
    counts: &mut HashMap<TransientWindowCountKey, i64>,
    eviction_schedule: &mut BTreeMap<i64, Vec<TransientWindowCountKey>>,
) -> Result<HashMap<(TransientWindowCountKey, i64), i64>> {
    let mut grouped_deltas: HashMap<TransientWindowCountKey, i64> = HashMap::new();
    for (row, weight) in input_deltas {
        if weight == 0 {
            continue;
        }
        let Some((key, event_ts)) = extract_encoded_row_columns_and_i64_like_column(
            &row,
            group_key_columns,
            time_column,
            false,
        )
        .context("extract transient window count-star row")?
        else {
            continue;
        };
        if event_ts < 0 {
            continue;
        }
        if let Some(cutoff) = cutoff
            && event_ts < cutoff
        {
            continue;
        }
        transient_window_for_each_window(event_ts, window_size, window_slide, |start, end| {
            merge_i64_delta(
                &mut grouped_deltas,
                TransientWindowCountKey {
                    start,
                    end,
                    key: key.clone(),
                },
                weight,
            );
        });
    }

    let mut updates: HashMap<(TransientWindowCountKey, i64), i64> = HashMap::new();
    for (key, delta) in grouped_deltas {
        if delta == 0 {
            continue;
        }
        let old_count = counts.get(&key).copied().unwrap_or(0);
        let new_count = old_count.saturating_add(delta);
        if old_count == new_count {
            continue;
        }
        if old_count != 0 {
            merge_count_delta(&mut updates, key.clone(), old_count, -1);
        }
        if new_count != 0 {
            merge_count_delta(&mut updates, key.clone(), new_count, 1);
            if old_count == 0 {
                eviction_schedule
                    .entry(key.end)
                    .or_default()
                    .push(key.clone());
            }
            counts.insert(key, new_count);
        } else {
            counts.remove(&key);
        }
    }

    transient_window_evict_expired_counts(cutoff, counts, eviction_schedule, &mut updates);
    Ok(updates)
}

fn encode_transient_window_count_state(
    counts: &HashMap<TransientWindowCountKey, i64>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    counts
        .iter()
        .filter(|(_, count)| **count != 0)
        .map(|(key, count)| {
            let encoded_window = encode_transient_window_bounds(key.start, key.end)?;
            let row = concat_encoded_rows(&encoded_window, &key.key)?;
            Ok((row, *count))
        })
        .collect()
}

fn restore_transient_window_count_state(
    rows: Vec<(Vec<u8>, i64)>,
    counts: &mut HashMap<TransientWindowCountKey, i64>,
    eviction_schedule: &mut BTreeMap<i64, Vec<TransientWindowCountKey>>,
) -> Result<()> {
    for (row, count) in rows {
        if count == 0 {
            continue;
        }
        let key = decode_transient_window_count_state_key(&row)?;
        counts.insert(key.clone(), count);
        eviction_schedule.entry(key.end).or_default().push(key);
    }
    Ok(())
}

fn decode_transient_window_count_state_key(row: &[u8]) -> Result<TransientWindowCountKey> {
    if row.len() < 4 {
        bail!("encoded window count state row too short");
    }
    let column_count = u32::from_le_bytes(row[0..4].try_into().unwrap()) as usize;
    if column_count < 2 {
        bail!("encoded window count state row has fewer than two window columns");
    }
    let start = extract_encoded_row_i64_like_column(row, 0)?
        .ok_or_else(|| anyhow!("encoded window count state start is null"))?;
    let end = extract_encoded_row_i64_like_column(row, 1)?
        .ok_or_else(|| anyhow!("encoded window count state end is null"))?;
    let key_columns = (2..column_count).collect::<Vec<_>>();
    let key = extract_encoded_row_columns(row, &key_columns, false)?
        .ok_or_else(|| anyhow!("encoded window count state key unexpectedly null"))?;
    Ok(TransientWindowCountKey { start, end, key })
}

fn encode_transient_window_count_output_deltas(
    deltas: HashMap<(TransientWindowCountKey, i64), i64>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    let mut encoded = Vec::with_capacity(deltas.len());
    for ((key, count), diff) in deltas {
        if diff == 0 {
            continue;
        }
        let encoded_window = encode_transient_window_bounds(key.start, key.end)?;
        let with_key = concat_encoded_rows(&encoded_window, &key.key)?;
        let encoded_count = encode_i64_values(std::slice::from_ref(&count))?;
        let row = concat_encoded_rows(&with_key, &encoded_count)?;
        encoded.push((row, diff));
    }
    Ok(encoded)
}

fn encode_transient_window_bounds(start: i64, end: i64) -> Result<Vec<u8>> {
    let mut encoded = Vec::with_capacity(4 + 18);
    encoded.extend_from_slice(&2_u32.to_le_bytes());
    encoded.push(0x03);
    encoded.extend_from_slice(&start.to_le_bytes());
    encoded.push(0x03);
    encoded.extend_from_slice(&end.to_le_bytes());
    Ok(encoded)
}

fn encode_transient_window_aggregate_input_pair(window_key: &[u8], row: &[u8]) -> Result<Vec<u8>> {
    let key_len =
        u32::try_from(window_key.len()).context("transient window aggregate key too large")?;
    let mut encoded = Vec::with_capacity(4 + window_key.len() + row.len());
    encoded.extend_from_slice(&key_len.to_le_bytes());
    encoded.extend_from_slice(window_key);
    encoded.extend_from_slice(row);
    Ok(encoded)
}

fn decode_transient_window_aggregate_input_pair(encoded: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    if encoded.len() < 4 {
        bail!("transient window aggregate input pair missing key length");
    }
    let mut key_len = [0_u8; 4];
    key_len.copy_from_slice(&encoded[..4]);
    let key_len = u32::from_le_bytes(key_len) as usize;
    if encoded.len() < 4 + key_len {
        bail!("transient window aggregate input pair truncated");
    }
    Ok((
        encoded[4..4 + key_len].to_vec(),
        encoded[4 + key_len..].to_vec(),
    ))
}

const TRANSIENT_COUNT_AGGREGATE_GROUP_TAG: u8 = 1;
const TRANSIENT_COUNT_AGGREGATE_DISTINCT_TAG: u8 = 2;
const TRANSIENT_INCREMENTAL_AGGREGATE_GROUP_TAG: u8 = 11;
const TRANSIENT_INCREMENTAL_AGGREGATE_DISTINCT_TAG: u8 = 12;
const TRANSIENT_INCREMENTAL_AGGREGATE_INPUT_TAG: u8 = 13;
const AGGREGATE_VALUE_NULL_INT64_TAG: u8 = 1;
const AGGREGATE_VALUE_NULL_TIMESTAMP_MILLIS_TAG: u8 = 2;
const AGGREGATE_VALUE_NULL_UTF8_TAG: u8 = 3;
const AGGREGATE_VALUE_INT64_TAG: u8 = 4;
const AGGREGATE_VALUE_TIMESTAMP_MILLIS_TAG: u8 = 5;
const AGGREGATE_VALUE_UTF8_TAG: u8 = 6;
const INCREMENTAL_AGGREGATE_SLOT_COUNT_TAG: u8 = 1;
const INCREMENTAL_AGGREGATE_SLOT_COUNT_DISTINCT_TAG: u8 = 2;
const INCREMENTAL_AGGREGATE_SLOT_SUM_TAG: u8 = 3;
const INCREMENTAL_AGGREGATE_SLOT_AVG_TAG: u8 = 4;
const INCREMENTAL_AGGREGATE_SLOT_MIN_TAG: u8 = 5;
const INCREMENTAL_AGGREGATE_SLOT_MAX_TAG: u8 = 6;

fn encode_transient_count_aggregate_snapshot(
    snapshot: dbsp::TransientCountAggregateSnapshot<Vec<u8>, Vec<u8>>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    let mut rows = Vec::with_capacity(snapshot.grouped.len() + snapshot.distinct.len());
    for group in snapshot.grouped {
        if group.total_rows == 0 {
            continue;
        }
        let mut row = Vec::new();
        row.push(TRANSIENT_COUNT_AGGREGATE_GROUP_TAG);
        write_len_prefixed_bytes(&mut row, &group.key)?;
        row.extend_from_slice(&group.total_rows.to_le_bytes());
        let count_len = u32::try_from(group.counts.len())
            .map_err(|_| anyhow!("too many transient count aggregate slots"))?;
        row.extend_from_slice(&count_len.to_le_bytes());
        for count in group.counts {
            row.extend_from_slice(&count.to_le_bytes());
        }
        rows.push((row, 1));
    }
    for distinct in snapshot.distinct {
        if distinct.weight == 0 {
            continue;
        }
        let mut row = Vec::new();
        row.push(TRANSIENT_COUNT_AGGREGATE_DISTINCT_TAG);
        write_len_prefixed_bytes(&mut row, &distinct.group_key)?;
        row.extend_from_slice(&distinct.slot.to_le_bytes());
        write_len_prefixed_bytes(&mut row, &distinct.value)?;
        rows.push((row, distinct.weight));
    }
    Ok(rows)
}

fn decode_transient_count_aggregate_snapshot(
    rows: Vec<(Vec<u8>, i64)>,
) -> Result<dbsp::TransientCountAggregateSnapshot<Vec<u8>, Vec<u8>>> {
    let mut snapshot = dbsp::TransientCountAggregateSnapshot::default();
    for (row, weight) in rows {
        if row.is_empty() || weight == 0 {
            continue;
        }
        let mut cursor = 1usize;
        match row[0] {
            TRANSIENT_COUNT_AGGREGATE_GROUP_TAG => {
                let key = read_len_prefixed_bytes(&row, &mut cursor)?;
                let total_rows = read_i64_le(&row, &mut cursor)?;
                let count_len = read_u32_le(&row, &mut cursor)? as usize;
                let mut counts = Vec::with_capacity(count_len);
                for _ in 0..count_len {
                    counts.push(read_i64_le(&row, &mut cursor)?);
                }
                if cursor != row.len() {
                    bail!("trailing bytes in transient count aggregate group state row");
                }
                snapshot
                    .grouped
                    .push(dbsp::TransientCountAggregateGroupedState {
                        key,
                        total_rows,
                        counts,
                    });
            }
            TRANSIENT_COUNT_AGGREGATE_DISTINCT_TAG => {
                let group_key = read_len_prefixed_bytes(&row, &mut cursor)?;
                let slot = read_u32_le(&row, &mut cursor)?;
                let value = read_len_prefixed_bytes(&row, &mut cursor)?;
                if cursor != row.len() {
                    bail!("trailing bytes in transient count aggregate distinct state row");
                }
                snapshot
                    .distinct
                    .push(dbsp::TransientCountAggregateDistinctWeight {
                        group_key,
                        slot,
                        value,
                        weight,
                    });
            }
            other => bail!("unknown transient count aggregate state row tag {other}"),
        }
    }
    Ok(snapshot)
}

fn encode_transient_incremental_aggregate_snapshot(
    snapshot: dbsp::TransientIncrementalAggregateSnapshot<Vec<u8>, Vec<u8>>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    let mut rows =
        Vec::with_capacity(snapshot.grouped.len() + snapshot.distinct.len() + snapshot.input.len());
    for group in snapshot.grouped {
        if group.total_rows == 0 {
            continue;
        }
        let mut row = Vec::new();
        row.push(TRANSIENT_INCREMENTAL_AGGREGATE_GROUP_TAG);
        write_len_prefixed_bytes(&mut row, &group.key)?;
        row.extend_from_slice(&group.total_rows.to_le_bytes());
        let slot_len = u32::try_from(group.slots.len())
            .map_err(|_| anyhow!("too many transient incremental aggregate slots"))?;
        row.extend_from_slice(&slot_len.to_le_bytes());
        for slot in group.slots {
            encode_incremental_aggregate_slot_state(&mut row, slot)?;
        }
        rows.push((row, 1));
    }
    for distinct in snapshot.distinct {
        if distinct.weight == 0 {
            continue;
        }
        let mut row = Vec::new();
        row.push(TRANSIENT_INCREMENTAL_AGGREGATE_DISTINCT_TAG);
        write_len_prefixed_bytes(&mut row, &distinct.group_key)?;
        row.extend_from_slice(&distinct.slot.to_le_bytes());
        encode_aggregate_value(&mut row, distinct.value)?;
        rows.push((row, distinct.weight));
    }
    for input in snapshot.input {
        if input.weight == 0 {
            continue;
        }
        let mut row = Vec::new();
        row.push(TRANSIENT_INCREMENTAL_AGGREGATE_INPUT_TAG);
        write_len_prefixed_bytes(&mut row, &input.group_key)?;
        write_len_prefixed_bytes(&mut row, &input.value)?;
        rows.push((row, input.weight));
    }
    Ok(rows)
}

fn decode_transient_incremental_aggregate_snapshot(
    rows: Vec<(Vec<u8>, i64)>,
) -> Result<dbsp::TransientIncrementalAggregateSnapshot<Vec<u8>, Vec<u8>>> {
    let mut snapshot = dbsp::TransientIncrementalAggregateSnapshot::default();
    for (row, weight) in rows {
        if row.is_empty() || weight == 0 {
            continue;
        }
        let mut cursor = 1usize;
        match row[0] {
            TRANSIENT_INCREMENTAL_AGGREGATE_GROUP_TAG => {
                let key = read_len_prefixed_bytes(&row, &mut cursor)?;
                let total_rows = read_i64_le(&row, &mut cursor)?;
                let slot_len = read_u32_le(&row, &mut cursor)? as usize;
                let mut slots = Vec::with_capacity(slot_len);
                for _ in 0..slot_len {
                    slots.push(decode_incremental_aggregate_slot_state(&row, &mut cursor)?);
                }
                if cursor != row.len() {
                    bail!("trailing bytes in transient incremental aggregate group state row");
                }
                snapshot
                    .grouped
                    .push(dbsp::TransientIncrementalAggregateGroupedState {
                        key,
                        total_rows,
                        slots,
                    });
            }
            TRANSIENT_INCREMENTAL_AGGREGATE_DISTINCT_TAG => {
                let group_key = read_len_prefixed_bytes(&row, &mut cursor)?;
                let slot = read_u32_le(&row, &mut cursor)?;
                let value = decode_aggregate_value(&row, &mut cursor)?;
                if cursor != row.len() {
                    bail!("trailing bytes in transient incremental aggregate distinct state row");
                }
                snapshot
                    .distinct
                    .push(dbsp::TransientIncrementalAggregateDistinctWeight {
                        group_key,
                        slot,
                        value,
                        weight,
                    });
            }
            TRANSIENT_INCREMENTAL_AGGREGATE_INPUT_TAG => {
                let group_key = read_len_prefixed_bytes(&row, &mut cursor)?;
                let value = read_len_prefixed_bytes(&row, &mut cursor)?;
                if cursor != row.len() {
                    bail!("trailing bytes in transient incremental aggregate input state row");
                }
                snapshot
                    .input
                    .push(dbsp::TransientIncrementalAggregateInputWeight {
                        group_key,
                        value,
                        weight,
                    });
            }
            other => bail!("unknown transient incremental aggregate state row tag {other}"),
        }
    }
    Ok(snapshot)
}

fn encode_transient_window_incremental_aggregate_snapshot(
    snapshot: dbsp::TransientIncrementalAggregateSnapshot<Vec<u8>, (Vec<u8>, Vec<u8>)>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    let input = snapshot
        .input
        .into_iter()
        .map(|entry| {
            let value =
                encode_transient_window_aggregate_input_pair(&entry.value.0, &entry.value.1)?;
            Ok(dbsp::TransientIncrementalAggregateInputWeight {
                group_key: entry.group_key,
                value,
                weight: entry.weight,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    encode_transient_incremental_aggregate_snapshot(dbsp::TransientIncrementalAggregateSnapshot {
        grouped: snapshot.grouped,
        distinct: snapshot.distinct,
        input,
    })
}

fn decode_transient_window_incremental_aggregate_snapshot(
    rows: Vec<(Vec<u8>, i64)>,
) -> Result<dbsp::TransientIncrementalAggregateSnapshot<Vec<u8>, (Vec<u8>, Vec<u8>)>> {
    let snapshot = decode_transient_incremental_aggregate_snapshot(rows)?;
    let input = snapshot
        .input
        .into_iter()
        .map(|entry| {
            let value = decode_transient_window_aggregate_input_pair(&entry.value)?;
            Ok(dbsp::TransientIncrementalAggregateInputWeight {
                group_key: entry.group_key,
                value,
                weight: entry.weight,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(dbsp::TransientIncrementalAggregateSnapshot {
        grouped: snapshot.grouped,
        distinct: snapshot.distinct,
        input,
    })
}

fn encode_incremental_aggregate_slot_state(
    dst: &mut Vec<u8>,
    slot: dbsp::IncrementalAggregateSlotState,
) -> Result<()> {
    match slot {
        dbsp::IncrementalAggregateSlotState::Count { count } => {
            dst.push(INCREMENTAL_AGGREGATE_SLOT_COUNT_TAG);
            dst.extend_from_slice(&count.to_le_bytes());
        }
        dbsp::IncrementalAggregateSlotState::CountDistinct { count } => {
            dst.push(INCREMENTAL_AGGREGATE_SLOT_COUNT_DISTINCT_TAG);
            dst.extend_from_slice(&count.to_le_bytes());
        }
        dbsp::IncrementalAggregateSlotState::Sum {
            sum,
            non_null_count,
        } => {
            dst.push(INCREMENTAL_AGGREGATE_SLOT_SUM_TAG);
            dst.extend_from_slice(&sum.to_le_bytes());
            dst.extend_from_slice(&non_null_count.to_le_bytes());
        }
        dbsp::IncrementalAggregateSlotState::Avg { sum, count } => {
            dst.push(INCREMENTAL_AGGREGATE_SLOT_AVG_TAG);
            dst.extend_from_slice(&sum.to_le_bytes());
            dst.extend_from_slice(&count.to_le_bytes());
        }
        dbsp::IncrementalAggregateSlotState::Min { current } => {
            dst.push(INCREMENTAL_AGGREGATE_SLOT_MIN_TAG);
            encode_optional_aggregate_value(dst, current)?;
        }
        dbsp::IncrementalAggregateSlotState::Max { current } => {
            dst.push(INCREMENTAL_AGGREGATE_SLOT_MAX_TAG);
            encode_optional_aggregate_value(dst, current)?;
        }
    }
    Ok(())
}

fn decode_incremental_aggregate_slot_state(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<dbsp::IncrementalAggregateSlotState> {
    let tag = read_u8(bytes, cursor)?;
    match tag {
        INCREMENTAL_AGGREGATE_SLOT_COUNT_TAG => Ok(dbsp::IncrementalAggregateSlotState::Count {
            count: read_i64_le(bytes, cursor)?,
        }),
        INCREMENTAL_AGGREGATE_SLOT_COUNT_DISTINCT_TAG => {
            Ok(dbsp::IncrementalAggregateSlotState::CountDistinct {
                count: read_i64_le(bytes, cursor)?,
            })
        }
        INCREMENTAL_AGGREGATE_SLOT_SUM_TAG => Ok(dbsp::IncrementalAggregateSlotState::Sum {
            sum: read_i64_le(bytes, cursor)?,
            non_null_count: read_i64_le(bytes, cursor)?,
        }),
        INCREMENTAL_AGGREGATE_SLOT_AVG_TAG => Ok(dbsp::IncrementalAggregateSlotState::Avg {
            sum: read_i64_le(bytes, cursor)?,
            count: read_i64_le(bytes, cursor)?,
        }),
        INCREMENTAL_AGGREGATE_SLOT_MIN_TAG => Ok(dbsp::IncrementalAggregateSlotState::Min {
            current: decode_optional_aggregate_value(bytes, cursor)?,
        }),
        INCREMENTAL_AGGREGATE_SLOT_MAX_TAG => Ok(dbsp::IncrementalAggregateSlotState::Max {
            current: decode_optional_aggregate_value(bytes, cursor)?,
        }),
        other => bail!("unknown incremental aggregate slot state tag {other}"),
    }
}

fn encode_optional_aggregate_value(
    dst: &mut Vec<u8>,
    value: Option<dbsp::AggregateValue>,
) -> Result<()> {
    match value {
        Some(value) => {
            dst.push(1);
            encode_aggregate_value(dst, value)?;
        }
        None => dst.push(0),
    }
    Ok(())
}

fn decode_optional_aggregate_value(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<Option<dbsp::AggregateValue>> {
    match read_u8(bytes, cursor)? {
        0 => Ok(None),
        1 => Ok(Some(decode_aggregate_value(bytes, cursor)?)),
        other => bail!("invalid optional aggregate value tag {other}"),
    }
}

fn encode_aggregate_value(dst: &mut Vec<u8>, value: dbsp::AggregateValue) -> Result<()> {
    match value {
        dbsp::AggregateValue::Null(dbsp::AggregateValueType::Int64) => {
            dst.push(AGGREGATE_VALUE_NULL_INT64_TAG);
        }
        dbsp::AggregateValue::Null(dbsp::AggregateValueType::TimestampMillis) => {
            dst.push(AGGREGATE_VALUE_NULL_TIMESTAMP_MILLIS_TAG);
        }
        dbsp::AggregateValue::Null(dbsp::AggregateValueType::Utf8) => {
            dst.push(AGGREGATE_VALUE_NULL_UTF8_TAG);
        }
        dbsp::AggregateValue::Int64(value) => {
            dst.push(AGGREGATE_VALUE_INT64_TAG);
            dst.extend_from_slice(&value.to_le_bytes());
        }
        dbsp::AggregateValue::TimestampMillis(value) => {
            dst.push(AGGREGATE_VALUE_TIMESTAMP_MILLIS_TAG);
            dst.extend_from_slice(&value.to_le_bytes());
        }
        dbsp::AggregateValue::Utf8(value) => {
            dst.push(AGGREGATE_VALUE_UTF8_TAG);
            write_len_prefixed_bytes(dst, value.as_bytes())?;
        }
    }
    Ok(())
}

fn decode_aggregate_value(bytes: &[u8], cursor: &mut usize) -> Result<dbsp::AggregateValue> {
    match read_u8(bytes, cursor)? {
        AGGREGATE_VALUE_NULL_INT64_TAG => {
            Ok(dbsp::AggregateValue::Null(dbsp::AggregateValueType::Int64))
        }
        AGGREGATE_VALUE_NULL_TIMESTAMP_MILLIS_TAG => Ok(dbsp::AggregateValue::Null(
            dbsp::AggregateValueType::TimestampMillis,
        )),
        AGGREGATE_VALUE_NULL_UTF8_TAG => {
            Ok(dbsp::AggregateValue::Null(dbsp::AggregateValueType::Utf8))
        }
        AGGREGATE_VALUE_INT64_TAG => Ok(dbsp::AggregateValue::Int64(read_i64_le(bytes, cursor)?)),
        AGGREGATE_VALUE_TIMESTAMP_MILLIS_TAG => Ok(dbsp::AggregateValue::TimestampMillis(
            read_i64_le(bytes, cursor)?,
        )),
        AGGREGATE_VALUE_UTF8_TAG => {
            let value = read_len_prefixed_bytes(bytes, cursor)?;
            Ok(dbsp::AggregateValue::Utf8(
                String::from_utf8(value).context("decode aggregate UTF-8 value")?,
            ))
        }
        other => bail!("unknown aggregate value tag {other}"),
    }
}

fn write_len_prefixed_bytes(dst: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| anyhow!("byte field too large"))?;
    dst.extend_from_slice(&len.to_le_bytes());
    dst.extend_from_slice(bytes);
    Ok(())
}

fn read_len_prefixed_bytes(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let len = read_u32_le(bytes, cursor)? as usize;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| anyhow!("length-prefixed byte field overflow"))?;
    if end > bytes.len() {
        bail!("truncated length-prefixed byte field");
    }
    let value = bytes[*cursor..end].to_vec();
    *cursor = end;
    Ok(value)
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8> {
    let value = *bytes.get(*cursor).ok_or_else(|| anyhow!("truncated u8"))?;
    *cursor = cursor
        .checked_add(1)
        .ok_or_else(|| anyhow!("u8 cursor overflow"))?;
    Ok(value)
}

fn read_u32_le(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| anyhow!("u32 cursor overflow"))?;
    let chunk = bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow!("truncated u32"))?;
    *cursor = end;
    Ok(u32::from_le_bytes(chunk.try_into().unwrap()))
}

fn read_i64_le(bytes: &[u8], cursor: &mut usize) -> Result<i64> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| anyhow!("i64 cursor overflow"))?;
    let chunk = bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow!("truncated i64"))?;
    *cursor = end;
    Ok(i64::from_le_bytes(chunk.try_into().unwrap()))
}

fn encode_count_aggregate_output_deltas(
    deltas: Vec<((Vec<u8>, Vec<i64>), i64)>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    let mut encoded = Vec::with_capacity(deltas.len());
    for ((key, values), diff) in deltas {
        if diff == 0 {
            continue;
        }
        let encoded_aggregate_values = encode_i64_values(&values)?;
        let row = concat_encoded_rows(&key, &encoded_aggregate_values)?;
        encoded.push((row, diff));
    }
    Ok(encoded)
}

fn encode_incremental_aggregate_output_deltas(
    deltas: Vec<((Vec<u8>, Vec<dbsp::AggregateValue>), i64)>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    let mut encoded = Vec::with_capacity(deltas.len());
    for ((key, values), diff) in deltas {
        if diff == 0 {
            continue;
        }
        let encoded_aggregate_values = encode_incremental_aggregate_values(&values)?;
        let row = concat_encoded_rows(&key, &encoded_aggregate_values)?;
        encoded.push((row, diff));
    }
    Ok(encoded)
}

fn encode_i64_values(values: &[i64]) -> Result<Vec<u8>> {
    let count = u32::try_from(values.len()).map_err(|_| anyhow!("too many columns in MV key"))?;
    let mut encoded = Vec::with_capacity(4 + (values.len() * 9));
    encoded.extend_from_slice(&count.to_le_bytes());
    for value in values {
        encoded.push(0x01);
        encoded.extend_from_slice(&value.to_le_bytes());
    }
    Ok(encoded)
}

fn encode_incremental_aggregate_values(values: &[dbsp::AggregateValue]) -> Result<Vec<u8>> {
    let count = u32::try_from(values.len()).map_err(|_| anyhow!("too many columns in MV key"))?;
    let mut encoded = Vec::with_capacity(4 + (values.len() * 9));
    encoded.extend_from_slice(&count.to_le_bytes());
    for value in values {
        match value {
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::Int64) => encoded.push(0x05),
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::TimestampMillis) => {
                encoded.push(0x07);
            }
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::Utf8) => encoded.push(0x06),
            dbsp::AggregateValue::Int64(value) => {
                encoded.push(0x01);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            dbsp::AggregateValue::TimestampMillis(value) => {
                encoded.push(0x03);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            dbsp::AggregateValue::Utf8(value) => {
                encoded.push(0x02);
                let bytes = value.as_bytes();
                let len = u32::try_from(bytes.len())
                    .map_err(|_| anyhow!("utf8 value too large for MV key"))?;
                encoded.extend_from_slice(&len.to_le_bytes());
                encoded.extend_from_slice(bytes);
            }
        }
    }
    Ok(encoded)
}

fn try_build_transient_join_pipeline_root_materialization(
    plan: &CircuitPlan,
    root_idx: usize,
) -> Result<Option<TransientJoinPipelineRootMaterialization>> {
    let Some(root) = plan.node(root_idx) else {
        return Ok(None);
    };
    match &root.kind {
        DbspNodeKind::Join(join) => {
            if !matches!(join.join_type, dbsp::DbspJoinType::Inner)
                || !has_single_consumer(plan, root_idx)
            {
                return Ok(None);
            }
            let (left_input_idx, right_input_idx) = join_inputs(root)?;
            let Some(left_source_root) =
                try_build_transient_source_root_materialization(plan, left_input_idx)?
            else {
                return Ok(None);
            };
            let Some(right_source_root) =
                try_build_transient_source_root_materialization(plan, right_input_idx)?
            else {
                return Ok(None);
            };
            Ok(Some(TransientJoinPipelineRootMaterialization {
                left_input_idx,
                right_input_idx,
                left_source_root,
                right_source_root,
                join: join.clone(),
                optimized_nodes: vec![root_idx],
                steps: Vec::new(),
            }))
        }
        DbspNodeKind::Aggregate(aggregate) => {
            if build_incremental_aggregate_slot_kinds(aggregate.aggregates()).is_none() {
                return Ok(None);
            }
            let input_idx = first_input(root, "aggregate")?;
            let Some(mut shape) =
                try_build_transient_join_pipeline_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape
                .steps
                .push(TransientJoinPipelineStep::Aggregate(aggregate.clone()));
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::TopN(topn) => {
            let input_idx = first_input(root, "topn")?;
            let Some(mut shape) =
                try_build_transient_join_pipeline_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape
                .steps
                .push(TransientJoinPipelineStep::TopN(topn.clone()));
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Passthrough => {
            let input_idx = first_input(root, "passthrough")?;
            let Some(mut shape) =
                try_build_transient_join_pipeline_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Select(select) => {
            let input_idx = first_input(root, "select")?;
            let Some(mut shape) =
                try_build_transient_join_pipeline_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape.steps.push(TransientJoinPipelineStep::Transform(
                build_filter_transform(select)?,
            ));
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        DbspNodeKind::Project(project) => {
            let input_idx = first_input(root, "project")?;
            if let Some(select_input_idx) = fuseable_select_input(plan, root_idx, input_idx)? {
                let Some(select_node) = plan.node(input_idx) else {
                    return Ok(None);
                };
                let DbspNodeKind::Select(select) = &select_node.kind else {
                    return Ok(None);
                };
                let Some(mut shape) =
                    try_build_transient_join_pipeline_root_materialization(plan, select_input_idx)?
                else {
                    return Ok(None);
                };
                shape.steps.push(TransientJoinPipelineStep::Transform(
                    build_filter_map_transform(select, project)?,
                ));
                shape.optimized_nodes.push(input_idx);
                shape.optimized_nodes.push(root_idx);
                return Ok(Some(shape));
            }

            let Some(mut shape) =
                try_build_transient_join_pipeline_root_materialization(plan, input_idx)?
            else {
                return Ok(None);
            };
            shape
                .steps
                .push(TransientJoinPipelineStep::Transform(build_map_transform(
                    project,
                )?));
            shape.optimized_nodes.push(root_idx);
            Ok(Some(shape))
        }
        _ => Ok(None),
    }
}

fn try_build_transient_source_topn_root_materialization(
    plan: &CircuitPlan,
    root_idx: usize,
    outer_transient_streams: &HashMap<String, TransientSourceHandleStream>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    graph_id: &str,
    state_table: Option<Arc<dyn KeyValueTable>>,
) -> Result<Option<TransientSourceTopNRootMaterialization>> {
    let Some(shape) = try_build_transient_source_topn_root_shape(plan, root_idx)? else {
        return Ok(None);
    };
    let Some(upstream) = outer_transient_streams
        .get(&shape.source_root.source_name)
        .cloned()
    else {
        return Ok(None);
    };
    let receiver = build_transient_topn_receiver(
        graph_id,
        &shape.topn,
        upstream,
        Arc::clone(&shape.source_root.transform),
        shape.output_projection.clone(),
        cancel,
        task_events,
        state_table,
        "source_topn",
    );
    Ok(Some(TransientSourceTopNRootMaterialization {
        source_name: shape.source_root.source_name,
        optimized_nodes: shape.optimized_nodes,
        receiver,
        transform: shape.transform,
    }))
}

fn compose_delta_transforms(
    first: Arc<DeltaTransformFn>,
    second: Arc<DeltaTransformFn>,
) -> Arc<DeltaTransformFn> {
    Arc::new(move |deltas| {
        let deltas = first(deltas)?;
        second(&deltas)
    })
}

fn compose_optional_delta_transform(
    first: Option<Arc<DeltaTransformFn>>,
    second: Arc<DeltaTransformFn>,
) -> Option<Arc<DeltaTransformFn>> {
    Some(match first {
        Some(first) => compose_delta_transforms(first, second),
        None => second,
    })
}

fn try_build_direct_row_projection(project: &DbspProjectNode) -> Option<Arc<Vec<usize>>> {
    let columns = project
        .expressions()
        .iter()
        .map(|expr| projection_direct_column_index(expr, project.input_schema().as_ref()))
        .collect::<Option<Vec<_>>>()?;
    Some(Arc::new(columns))
}

fn compose_direct_row_projection(
    first: Option<Arc<Vec<usize>>>,
    second: Arc<Vec<usize>>,
) -> Result<Arc<Vec<usize>>> {
    let Some(first) = first else {
        return Ok(second);
    };
    let mut composed = Vec::with_capacity(second.len());
    for projected_idx in second.iter().copied() {
        let Some(&source_idx) = first.get(projected_idx) else {
            bail!(
                "direct projection index {projected_idx} out of bounds for prior width {}",
                first.len()
            );
        };
        composed.push(source_idx);
    }
    Ok(Arc::new(composed))
}

fn build_direct_projection_transform(columns: Arc<Vec<usize>>) -> Arc<DeltaTransformFn> {
    Arc::new(move |deltas: &[(Vec<u8>, i64)]| project_encoded_deltas(deltas, columns.as_ref()))
}

fn fold_topn_root_output_projection(shape: &mut TransientSourceTopNRootShape) {
    if let Some(output_projection) = shape.output_projection.take() {
        shape.transform = compose_optional_delta_transform(
            shape.transform.take(),
            build_direct_projection_transform(output_projection),
        );
    }
}

fn project_encoded_deltas(
    deltas: &[(Vec<u8>, i64)],
    columns: &[usize],
) -> Result<Vec<(Vec<u8>, i64)>> {
    deltas
        .iter()
        .map(|(encoded, weight)| {
            let projected = extract_encoded_row_columns(encoded, columns, false)?
                .ok_or_else(|| anyhow!("direct encoded projection unexpectedly returned null"))?;
            Ok((projected, *weight))
        })
        .collect()
}

fn build_transient_topn_receiver(
    graph_id: &str,
    topn: &DbspTopNNode,
    upstream: TransientSourceHandleStream,
    input_transform: Arc<DeltaTransformFn>,
    output_projection: Option<Arc<Vec<usize>>>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> mpsc::UnboundedReceiver<TransientMaterializeBatch> {
    // Source roots are ZSet inputs, not a proven append-only contract. Keeping
    // full TopN input state is required to recompute replacement winners after
    // retractions; winner-only compact state is only correct for strictly
    // append-only streams.
    let append_only_input = false;
    let compact_append_only_state = false;
    let upstream_rx = build_transient_source_receiver(
        graph_id,
        format!("transient-topn-source:{graph_id}"),
        upstream,
        input_transform,
        cancel,
        task_events,
    );
    build_transient_topn_receiver_from_batches(
        graph_id,
        topn,
        upstream_rx,
        append_only_input,
        compact_append_only_state,
        output_projection,
        cancel,
        task_events,
        state_table,
        state_label,
    )
}

fn build_transient_topn_receiver_from_batches(
    graph_id: &str,
    topn: &DbspTopNNode,
    mut upstream_rx: mpsc::UnboundedReceiver<TransientMaterializeBatch>,
    append_only_input: bool,
    compact_append_only_state: bool,
    output_projection: Option<Arc<Vec<usize>>>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> mpsc::UnboundedReceiver<TransientMaterializeBatch> {
    let (tx, rx) = mpsc::unbounded_channel::<TransientMaterializeBatch>();
    let graph_id = graph_id.to_string();
    let task_label = format!("transient-topn:{graph_id}");
    let task_events = task_events.clone();
    let cancel = cancel.clone();
    let state_label = state_label.into();
    let debug_transient_join = std::env::var_os("FLOE_DEBUG_TRANSIENT_JOIN").is_some();
    if let Some(config) = try_build_direct_partitioned_top1_config(topn) {
        let mut processor =
            TransientDirectTop1Processor::new(graph_id.clone(), config, compact_append_only_state);
        let output_projection = output_projection.clone();
        let state_table = state_table.clone();
        let state_label = state_label.clone();
        tokio::spawn(async move {
            let mut persistent_state =
                match PersistentTransientInputState::load(state_table, &graph_id, &state_label)
                    .await
                {
                    Ok(state) => state,
                    Err(err) => {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        return;
                    }
                };
            if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                return;
            }
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        if !compact_append_only_state {
                            if let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let output_deltas = match processor.apply_deltas(input_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if compact_append_only_state {
                            if let Err(err) = persistent_state.apply_deltas(&output_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let output_deltas = match output_projection.as_ref() {
                            Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref()) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            },
                            None => output_deltas,
                        };
                        if debug_transient_join {
                            eprintln!(
                                "transient-topn-output graph_id={} version={} rows={}",
                                graph_id,
                                batch.version,
                                output_deltas.len()
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(output_deltas),
                        }).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        return rx;
    }

    let use_direct_int64_partitioned_topn = false;
    if use_direct_int64_partitioned_topn {
        if let Some(config) = try_build_direct_int64_partitioned_topn_config(topn) {
            let mut processor =
                TransientDirectInt64TopNProcessor::new(graph_id.clone(), config, topn);
            let output_projection = output_projection.clone();
            let state_table = state_table.clone();
            let state_label = state_label.clone();
            tokio::spawn(async move {
                let mut persistent_state =
                    match PersistentTransientInputState::load(state_table, &graph_id, &state_label)
                        .await
                    {
                        Ok(state) => state,
                        Err(err) => {
                            report_graph_task_error(
                                &task_events,
                                &graph_id,
                                task_label.clone(),
                                err,
                            );
                            return;
                        }
                    };
                if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                    return;
                }
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        maybe_batch = upstream_rx.recv() => {
                            let Some(batch) = maybe_batch else {
                                break;
                            };
                            let input_deltas = batch.deltas.as_ref().clone();
                            if let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                            let output_deltas = match processor.apply_deltas(input_deltas) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            };
                            let output_deltas = match output_projection.as_ref() {
                                Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref()) {
                                    Ok(deltas) => deltas,
                                    Err(err) => {
                                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                        break;
                                    }
                                },
                                None => output_deltas,
                            };
                            if debug_transient_join {
                                eprintln!(
                                    "transient-topn-output graph_id={} version={} rows={}",
                                    graph_id,
                                    batch.version,
                                    output_deltas.len()
                                );
                            }
                            if tx.send(TransientMaterializeBatch {
                                version: batch.version,
                                deltas: Arc::new(output_deltas),
                            }).is_err() {
                                break;
                            }
                        }
                    }
                }
            });
            return rx;
        }
    }

    let use_partitioned_top1 =
        topn.limit() == 1 && topn.offset() == 0 && !topn.partition_by().is_empty();
    let key_layout = match build_transient_topn_key_layout(topn) {
        Ok(layout) => layout,
        Err(err) => {
            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
            return rx;
        }
    };

    if use_partitioned_top1 {
        let mut processor = TransientTop1Processor::new(graph_id.clone(), topn, &key_layout);
        let precompute_evaluator = key_layout.precompute_evaluator.clone();
        let output_projection = output_projection.clone();
        let state_table = state_table.clone();
        let state_label = state_label.clone();
        tokio::spawn(async move {
            let mut persistent_state =
                match PersistentTransientInputState::load(state_table, &graph_id, &state_label)
                    .await
                {
                    Ok(state) => state,
                    Err(err) => {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        return;
                    }
                };
            if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                return;
            }
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                            match evaluator.transform_delta(&graph_id, &input_deltas) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            }
                        } else {
                            input_deltas
                        };
                        if !compact_append_only_state
                            && let Err(err) = persistent_state.apply_deltas(&input_deltas).await
                        {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                        let output_deltas = match processor.apply_deltas(input_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if compact_append_only_state
                            && let Err(err) = persistent_state.apply_deltas(&output_deltas).await
                        {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                        let output_deltas = match output_projection.as_ref() {
                            Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref()) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            },
                            None => output_deltas,
                        };
                        if debug_transient_join {
                            eprintln!(
                                "transient-topn-output graph_id={} version={} rows={}",
                                graph_id,
                                batch.version,
                                output_deltas.len()
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(output_deltas),
                        }).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        return rx;
    }

    let use_append_only_partitioned_topn = append_only_input
        && topn.offset() == 0
        && topn.limit() > 1
        && !topn.partition_by().is_empty();

    if use_append_only_partitioned_topn {
        let mut processor =
            TransientAppendOnlyTopNProcessor::new(graph_id.clone(), topn, &key_layout);
        let precompute_evaluator = key_layout.precompute_evaluator.clone();
        let output_projection = output_projection.clone();
        let state_table = state_table.clone();
        let state_label = state_label.clone();
        tokio::spawn(async move {
            let mut persistent_state =
                match PersistentTransientInputState::load(state_table, &graph_id, &state_label)
                    .await
                {
                    Ok(state) => state,
                    Err(err) => {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        return;
                    }
                };
            if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                return;
            }
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                            match evaluator.transform_delta(&graph_id, &input_deltas) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            }
                        } else {
                            input_deltas
                        };
                        if !compact_append_only_state {
                            if let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let output_deltas = match processor.apply_deltas(input_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        if compact_append_only_state {
                            if let Err(err) = persistent_state.apply_deltas(&output_deltas).await {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                        let output_deltas = match output_projection.as_ref() {
                            Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref()) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            },
                            None => output_deltas,
                        };
                        if debug_transient_join {
                            eprintln!(
                                "transient-topn-output graph_id={} version={} rows={}",
                                graph_id,
                                batch.version,
                                output_deltas.len()
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(output_deltas),
                        }).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        return rx;
    }

    let use_vectorized_partitioned_topn = false
        && topn.offset() == 0
        && topn.limit() > 1
        && topn.limit() <= 64
        && !topn.partition_by().is_empty();

    if use_vectorized_partitioned_topn {
        let mut processor = TransientBatchTopNProcessor::new(
            graph_id.clone(),
            topn,
            &key_layout,
            append_only_input,
        );
        let precompute_evaluator = key_layout.precompute_evaluator.clone();
        let output_projection = output_projection.clone();
        let state_table = state_table.clone();
        let state_label = state_label.clone();
        tokio::spawn(async move {
            let mut persistent_state =
                match PersistentTransientInputState::load(state_table, &graph_id, &state_label)
                    .await
                {
                    Ok(state) => state,
                    Err(err) => {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        return;
                    }
                };
            if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                return;
            }
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_batch = upstream_rx.recv() => {
                        let Some(batch) = maybe_batch else {
                            break;
                        };
                        let input_deltas = batch.deltas.as_ref().clone();
                        let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                            match evaluator.transform_delta(&graph_id, &input_deltas) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            }
                        } else {
                            input_deltas
                        };
                        if let Err(err) = persistent_state.apply_deltas(&input_deltas).await {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                        let output_deltas = match processor.apply_deltas(input_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        let output_deltas = match output_projection.as_ref() {
                            Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref()) {
                                Ok(deltas) => deltas,
                                Err(err) => {
                                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                    break;
                                }
                            },
                            None => output_deltas,
                        };
                        if debug_transient_join {
                            eprintln!(
                                "transient-topn-output graph_id={} version={} rows={}",
                                graph_id,
                                batch.version,
                                output_deltas.len()
                            );
                        }
                        if tx.send(TransientMaterializeBatch {
                            version: batch.version,
                            deltas: Arc::new(output_deltas),
                        }).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        return rx;
    }

    let mut processor =
        TransientTopNProcessor::new(graph_id.clone(), topn, &key_layout, append_only_input);
    let precompute_evaluator = key_layout.precompute_evaluator.clone();
    let output_projection = output_projection.clone();
    let state_table = state_table.clone();
    let state_label = state_label.clone();

    tokio::spawn(async move {
        let mut persistent_state =
            match PersistentTransientInputState::load(state_table, &graph_id, &state_label).await {
                Ok(state) => state,
                Err(err) => {
                    report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                    return;
                }
            };
        if let Err(err) = processor.apply_deltas(persistent_state.snapshot_deltas()) {
            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
            return;
        }
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                maybe_batch = upstream_rx.recv() => {
                    let Some(batch) = maybe_batch else {
                        break;
                    };
                    let input_deltas = batch.deltas.as_ref().clone();
                    let input_deltas = if let Some(evaluator) = precompute_evaluator.as_ref() {
                        match evaluator.transform_delta(&graph_id, &input_deltas) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                    } else {
                        input_deltas
                    };
                    if !compact_append_only_state
                        && let Err(err) = persistent_state.apply_deltas(&input_deltas).await
                    {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        break;
                    }
                    let output_deltas = match processor.apply_deltas(input_deltas) {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if compact_append_only_state
                        && let Err(err) = persistent_state.apply_deltas(&output_deltas).await
                    {
                        report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                        break;
                    }
                    let output_deltas = match output_projection.as_ref() {
                        Some(columns) => match project_encoded_deltas(&output_deltas, columns.as_ref()) {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        },
                        None => output_deltas,
                    };
                    if debug_transient_join {
                        eprintln!(
                            "transient-topn-output graph_id={} version={} rows={}",
                            graph_id,
                            batch.version,
                            output_deltas.len()
                        );
                    }
                    if tx.send(TransientMaterializeBatch {
                        version: batch.version,
                        deltas: Arc::new(output_deltas),
                    }).is_err() {
                        break;
                    }
                }
            }
        }
    });

    rx
}

fn find_transient_source_root_shape(
    plan: &CircuitPlan,
    root_idx: usize,
) -> Result<Option<TransientSourceRootShape>> {
    let Some(root) = plan.node(root_idx) else {
        return Ok(None);
    };
    match &root.kind {
        DbspNodeKind::Source(source) => Ok(Some(TransientSourceRootShape::Source {
            source: source.clone(),
            optimized_nodes: vec![root_idx],
        })),
        DbspNodeKind::Select(select) => {
            let input_idx = first_input(root, "select")?;
            let Some(input) = plan.node(input_idx) else {
                return Ok(None);
            };
            let DbspNodeKind::Source(source) = &input.kind else {
                return Ok(None);
            };
            Ok(Some(TransientSourceRootShape::Select {
                source: source.clone(),
                select: select.clone(),
                optimized_nodes: vec![root_idx],
            }))
        }
        DbspNodeKind::Project(project) => {
            let input_idx = first_input(root, "project")?;
            if let Some(select_input_idx) = fuseable_select_input(plan, root_idx, input_idx)? {
                let Some(select_node) = plan.node(input_idx) else {
                    return Ok(None);
                };
                let Some(source_node) = plan.node(select_input_idx) else {
                    return Ok(None);
                };
                let DbspNodeKind::Select(select) = &select_node.kind else {
                    return Ok(None);
                };
                let DbspNodeKind::Source(source) = &source_node.kind else {
                    return Ok(None);
                };
                return Ok(Some(TransientSourceRootShape::FilterMap {
                    source: source.clone(),
                    select: select.clone(),
                    project: project.clone(),
                    optimized_nodes: vec![root_idx, input_idx],
                }));
            }
            let Some(input) = plan.node(input_idx) else {
                return Ok(None);
            };
            let DbspNodeKind::Source(source) = &input.kind else {
                return Ok(None);
            };
            Ok(Some(TransientSourceRootShape::Project {
                source: source.clone(),
                project: project.clone(),
                optimized_nodes: vec![root_idx],
            }))
        }
        _ => Ok(None),
    }
}

fn build_filter_transform(node: &DbspSelectNode) -> Result<Arc<DeltaTransformFn>> {
    let predicate = node.predicate().clone();
    let schema = Arc::clone(node.output_schema());
    let evaluator = Arc::new(
        VectorizedFilterProjectEvaluator::for_filter(&predicate, Arc::clone(&schema))
            .context("build vectorized transient source filter evaluator")?,
    );
    Ok(Arc::new(move |delta_values| {
        evaluator.transform_delta("source_batch_journal", &delta_values)
    }))
}

fn build_map_transform(node: &DbspProjectNode) -> Result<Arc<DeltaTransformFn>> {
    let expressions: Arc<Vec<DbspProjectExpr>> = Arc::new(node.expressions().to_vec());
    let schema = Arc::clone(node.input_schema());
    let evaluator = Arc::new(
        VectorizedFilterProjectEvaluator::for_map(expressions.as_ref(), Arc::clone(&schema))
            .context("build vectorized transient source map evaluator")?,
    );
    Ok(Arc::new(move |delta_values| {
        evaluator.transform_delta("source_batch_journal", &delta_values)
    }))
}

fn build_filter_map_transform(
    select: &DbspSelectNode,
    project: &DbspProjectNode,
) -> Result<Arc<DeltaTransformFn>> {
    let predicate = select.predicate().clone();
    let expressions: Arc<Vec<DbspProjectExpr>> = Arc::new(project.expressions().to_vec());
    let project_schema = Arc::clone(select.output_schema());
    let evaluator = Arc::new(
        VectorizedFilterProjectEvaluator::for_filter_map(
            &predicate,
            expressions.as_ref(),
            Arc::clone(&project_schema),
        )
        .context("build vectorized transient source filter_map evaluator")?,
    );
    Ok(Arc::new(move |delta_values| {
        evaluator.transform_delta("source_batch_journal", &delta_values)
    }))
}

#[cfg(test)]
mod tests;
